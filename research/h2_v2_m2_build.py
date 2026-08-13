from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import dotenv_values


REPO = Path(__file__).resolve().parents[1]

CONTRACT = (
    REPO
    / "research"
    / "h2_v2_m2_contract.json"
)

TAPE = (
    REPO
    / "research"
    / "h2_v2_m2_tape.jsonl"
)

VALIDATION = (
    REPO
    / "research"
    / "h2_v2_m2_validation.json"
)

EXPECTED_CONTRACT_SHA = (
    "4a09e3124bd0e05624a577916b10a712"
    "802bcc1096f6218a765c3830cf239cc4"
)

RUN_ID = 11691
EXPECTED_ANCHORS = 1080
EXPECTED_GAMES = 9


def sha256_file(path: Path) -> str:
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def clean(x):
    if isinstance(x, Decimal):
        return str(x)

    if isinstance(x, datetime):
        return x.isoformat()

    if isinstance(x, dict):
        return {
            str(k): clean(v)
            for k, v in x.items()
        }

    if isinstance(x, (list, tuple)):
        return [
            clean(v)
            for v in x
        ]

    return x


def norm_team(x: str | None) -> str:
    return (
        (x or "")
        .strip()
        .lower()
    )


def connect():
    env = dotenv_values(
        REPO / ".env"
    )

    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="quantlab",
        user="quantlab",
        password=env["PG_PASSWORD"],
        cursor_factory=(
            psycopg2.extras.RealDictCursor
        ),
    )

    # M2 must never write to the DB.
    conn.set_session(
        readonly=True,
        autocommit=False,
    )

    return conn


def load_levels(raw):
    levels = {}

    for level in raw or []:
        if (
            not isinstance(level, (list, tuple))
            or len(level) < 2
        ):
            raise RuntimeError(
                f"bad book level: {level!r}"
            )

        price = int(level[0])
        size = Decimal(
            str(level[1])
        )

        if not 0 <= price <= 1000:
            raise RuntimeError(
                f"bad price_mc: {price}"
            )

        if size < 0:
            raise RuntimeError(
                f"negative book size: {size}"
            )

        if size > 0:
            levels[price] = size

    return levels


def derive_bbo(bids, asks):
    if not bids or not asks:
        return {
            "best_bid_mc": None,
            "best_bid_size": None,
            "best_ask_mc": None,
            "best_ask_size": None,
            "two_sided": False,
            "crossed": False,
        }

    best_bid = max(bids)
    best_ask = min(asks)

    return {
        "best_bid_mc":
            best_bid,

        "best_bid_size":
            bids[best_bid],

        "best_ask_mc":
            best_ask,

        "best_ask_size":
            asks[best_ask],

        "two_sided":
            True,

        "crossed":
            best_bid >= best_ask,
    }


def anchor_rows(cur):
    cur.execute(
        """
        SELECT
            c.id AS sharp_consensus_id,
            c.poll_id,
            c.odds_game_id,
            c.outcome_team,
            c.capture_time AS decision_time,
            c.consensus_prob,
            c.families_used,
            g.commence_time

        FROM h2_v2_sharp_consensus c

        JOIN h2_v2_sharp_polls p
          ON p.id = c.poll_id

        JOIN odds_games g
          ON g.id = c.odds_game_id

        WHERE
            p.collector_run_id = %s
            AND c.eligible
            AND g.commence_time
                > c.capture_time
            AND g.commence_time
                <= c.capture_time
                   + interval '23 hours'

        ORDER BY
            c.id
        """,
        (RUN_ID,),
    )

    return [
        dict(x)
        for x in cur.fetchall()
    ]


def market_identity(
    cur,
    odds_game_id,
    outcome_team,
    market_cache,
    token_cache,
):
    if odds_game_id not in market_cache:
        cur.execute(
            """
            SELECT
                polymarket_market_id
            FROM h2_v2_market_matches
            WHERE odds_game_id = %s
            ORDER BY id
            """,
            (odds_game_id,),
        )

        market_rows = [
            dict(x)
            for x in cur.fetchall()
        ]

        market_cache[odds_game_id] = (
            market_rows
        )

    market_rows = (
        market_cache[odds_game_id]
    )

    market_unique = (
        len(market_rows) == 1
    )

    if not market_unique:
        return {
            "market_unique": False,
            "token_unique": False,
            "polymarket_market_id": None,
            "token_id": None,
            "pm_outcome": None,
        }

    market_id = (
        market_rows[0][
            "polymarket_market_id"
        ]
    )

    if market_id not in token_cache:
        cur.execute(
            """
            SELECT
                id,
                outcome
            FROM tokens
            WHERE market_id = %s
            ORDER BY id
            """,
            (market_id,),
        )

        token_cache[market_id] = [
            dict(x)
            for x in cur.fetchall()
        ]

    tokens = token_cache[market_id]

    # Binary moneyline market must have
    # exactly two local Polymarket tokens.
    token_count_ok = (
        len(tokens) == 2
    )

    matches = [
        x
        for x in tokens
        if norm_team(x["outcome"])
        == norm_team(outcome_team)
    ]

    token_unique = (
        token_count_ok
        and len(matches) == 1
    )

    if not token_unique:
        return {
            "market_unique":
                True,

            "token_unique":
                False,

            "polymarket_market_id":
                market_id,

            "token_id":
                None,

            "pm_outcome":
                None,
        }

    chosen = matches[0]

    return {
        "market_unique":
            True,

        "token_unique":
            True,

        "polymarket_market_id":
            market_id,

        "token_id":
            chosen["id"],

        "pm_outcome":
            chosen["outcome"],
    }


def config_at(
    cur,
    market_id,
    decision_time,
):
    if market_id is None:
        return None

    cur.execute(
        """
        SELECT
            observed_at,
            minimum_order_size,
            minimum_tick_size,
            fee_rate,
            fee_exponent,
            taker_only

        FROM h2_v2_pm_market_config

        WHERE
            polymarket_market_id = %s
            AND observed_at <= %s

        ORDER BY
            observed_at DESC

        LIMIT 1
        """,
        (
            market_id,
            decision_time,
        ),
    )

    row = cur.fetchone()

    return (
        dict(row)
        if row
        else None
    )


def reference_tob(
    cur,
    token_id,
    target_time,
):
    cur.execute(
        """
        SELECT
            capture_time,
            best_bid_mc,
            best_bid_size,
            best_ask_mc,
            best_ask_size,
            source

        FROM tob_snapshots

        WHERE
            token_id = %s
            AND capture_time <= %s
            AND watchlist_rule = 'h2_v2'

        ORDER BY
            capture_time DESC

        LIMIT 1
        """,
        (
            token_id,
            target_time,
        ),
    )

    row = cur.fetchone()

    return (
        dict(row)
        if row
        else None
    )


def reconstruct(
    cur,
    token_id,
    target_time,
):
    result = {
        "reconstructed": False,
        "base_time": None,
        "base_source": None,
        "base_generation": None,
        "base_connection_id": None,
        "replayed_deltas": 0,
        "generation_mixing": False,
        "connection_mixing": False,
        "lookahead": False,
        "best_bid_mc": None,
        "best_bid_size": None,
        "best_ask_mc": None,
        "best_ask_size": None,
        "two_sided": False,
        "crossed": False,
        "reference_time": None,
        "reference_source": None,
        "reference_best_bid_mc": None,
        "reference_best_bid_size": None,
        "reference_best_ask_mc": None,
        "reference_best_ask_size": None,
        "reference_covered": False,
        "reference_price_match": False,
        "reference_size_match": True,
        "reference_size_comparisons": 0,
    }

    if token_id is None:
        return result

    # -------------------------------------------------
    # Select the latest compatible full-book base.
    # -------------------------------------------------
    cur.execute(
        """
        SELECT
            capture_time,
            bids,
            asks,
            source,
            connection_id,
            ingest_sequence,
            book_generation

        FROM book_snapshots

        WHERE
            token_id = %s
            AND capture_time <= %s
            AND source IN (
                'ws_book',
                'rest_book'
            )
            AND watchlist_rule = 'h2_v2'
            AND book_generation IS NOT NULL

        ORDER BY
            capture_time DESC,
            ingest_sequence DESC NULLS LAST

        LIMIT 1
        """,
        (
            token_id,
            target_time,
        ),
    )

    base = cur.fetchone()

    if not base:
        return result

    base = dict(base)

    result["base_time"] = (
        base["capture_time"]
    )

    result["base_source"] = (
        base["source"]
    )

    result["base_generation"] = (
        base["book_generation"]
    )

    result["base_connection_id"] = (
        str(base["connection_id"])
        if base["connection_id"]
        is not None
        else None
    )

    if base["capture_time"] > target_time:
        result["lookahead"] = True

    bids = load_levels(
        base["bids"]
    )

    asks = load_levels(
        base["asks"]
    )

    # -------------------------------------------------
    # Fetch ALL post-base deltas first.
    #
    # We intentionally do not pre-filter generation or
    # connection in SQL. That lets the validator detect
    # contamination instead of silently discarding it.
    # -------------------------------------------------
    cur.execute(
        """
        SELECT
            capture_time,
            price_mc,
            size,
            side,
            connection_id,
            ingest_sequence,
            change_index,
            book_generation

        FROM book_deltas

        WHERE
            token_id = %s
            AND capture_time > %s
            AND capture_time <= %s
            AND watchlist_rule = 'h2_v2'

        ORDER BY
            capture_time ASC,
            ingest_sequence ASC,
            change_index ASC
        """,
        (
            token_id,
            base["capture_time"],
            target_time,
        ),
    )

    deltas = [
        dict(x)
        for x in cur.fetchall()
    ]

    matching_generation = [
        d
        for d in deltas
        if d["book_generation"]
        == base["book_generation"]
    ]

    if len(matching_generation) != len(deltas):
        result[
            "generation_mixing"
        ] = True

    # WS bases must remain on their exact
    # originating connection.
    if base["source"] == "ws_book":
        for d in matching_generation:
            if (
                d["connection_id"]
                != base["connection_id"]
            ):
                result[
                    "connection_mixing"
                ] = True

    # A REST reset has no connection_id itself.
    # Its same-generation continuation must be
    # attributable to at most one WS connection.
    elif base["source"] == "rest_book":
        rest_connections = {
            d["connection_id"]
            for d in matching_generation
            if d["connection_id"]
            is not None
        }

        if len(rest_connections) > 1:
            result[
                "connection_mixing"
            ] = True

    else:
        raise RuntimeError(
            "unexpected base source"
        )

    for d in matching_generation:
        # Do not allow a wrong WS connection
        # to influence reconstructed state.
        if (
            base["source"] == "ws_book"
            and d["connection_id"]
            != base["connection_id"]
        ):
            continue

        if d["capture_time"] > target_time:
            result["lookahead"] = True
            continue

        price = int(
            d["price_mc"]
        )

        size = Decimal(
            str(d["size"])
        )

        side = d["side"]

        book = (
            bids
            if side == "B"
            else asks
            if side == "S"
            else None
        )

        if book is None:
            raise RuntimeError(
                f"unexpected delta side: {side!r}"
            )

        if size == 0:
            book.pop(
                price,
                None,
            )
        else:
            book[price] = size

        result[
            "replayed_deltas"
        ] += 1

    bbo = derive_bbo(
        bids,
        asks,
    )

    result.update(bbo)

    result["reconstructed"] = (
        bbo["two_sided"]
    )

    # -------------------------------------------------
    # Independent recorded TOB cross-check.
    # -------------------------------------------------
    ref = reference_tob(
        cur,
        token_id,
        target_time,
    )

    if ref:
        result[
            "reference_covered"
        ] = True

        result[
            "reference_time"
        ] = ref["capture_time"]

        result[
            "reference_source"
        ] = ref["source"]

        result[
            "reference_best_bid_mc"
        ] = ref["best_bid_mc"]

        result[
            "reference_best_bid_size"
        ] = ref["best_bid_size"]

        result[
            "reference_best_ask_mc"
        ] = ref["best_ask_mc"]

        result[
            "reference_best_ask_size"
        ] = ref["best_ask_size"]

        result[
            "reference_price_match"
        ] = (
            ref["best_bid_mc"]
            == result["best_bid_mc"]
            and
            ref["best_ask_mc"]
            == result["best_ask_mc"]
        )

        for (
            ref_size,
            reconstructed_size,
        ) in (
            (
                ref["best_bid_size"],
                result["best_bid_size"],
            ),
            (
                ref["best_ask_size"],
                result["best_ask_size"],
            ),
        ):
            if ref_size is None:
                continue

            result[
                "reference_size_comparisons"
            ] += 1

            if (
                reconstructed_size
                is None
                or Decimal(str(ref_size))
                != Decimal(
                    str(reconstructed_size)
                )
            ):
                result[
                    "reference_size_match"
                ] = False

    return result


def endpoint_metrics(states):
    bases = Counter(
        s["base_source"]
        for s in states
        if s["base_source"]
        is not None
    )

    return {
        "anchors":
            len(states),

        "reconstructed":
            sum(
                s["reconstructed"]
                for s in states
            ),

        "two_sided":
            sum(
                s["two_sided"]
                for s in states
            ),

        "crossed":
            sum(
                s["crossed"]
                for s in states
            ),

        "generation_mixing":
            sum(
                s["generation_mixing"]
                for s in states
            ),

        "connection_mixing":
            sum(
                s["connection_mixing"]
                for s in states
            ),

        "lookahead":
            sum(
                s["lookahead"]
                for s in states
            ),

        "reference_covered":
            sum(
                s["reference_covered"]
                for s in states
            ),

        "reference_price_matches":
            sum(
                s["reference_price_match"]
                for s in states
            ),

        "reference_price_mismatches":
            sum(
                (
                    s["reference_covered"]
                    and not s[
                        "reference_price_match"
                    ]
                )
                for s in states
            ),

        "reference_size_comparisons":
            sum(
                s[
                    "reference_size_comparisons"
                ]
                for s in states
            ),

        "reference_size_mismatch_anchors":
            sum(
                not s[
                    "reference_size_match"
                ]
                for s in states
            ),

        "replayed_deltas":
            sum(
                s["replayed_deltas"]
                for s in states
            ),

        "base_sources":
            dict(
                sorted(
                    bases.items()
                )
            ),
    }


def main():
    contract_sha = sha256_file(
        CONTRACT
    )

    if (
        contract_sha
        != EXPECTED_CONTRACT_SHA
    ):
        raise SystemExit(
            "M2 contract SHA mismatch"
        )

    contract = json.loads(
        CONTRACT.read_text(
            encoding="utf-8"
        )
    )

    # Defensive frozen-design checks.
    assert (
        contract["version"]
        == "H2-v2-M2-CONTRACT-1"
    )

    assert (
        contract[
            "engineering_dataset"
        ][
            "sharp_collector_run_id"
        ]
        == RUN_ID
    )

    assert (
        contract[
            "engineering_dataset"
        ][
            "performance_use"
        ]
        is False
    )

    assert (
        contract[
            "endpoints"
        ][
            "execution_500ms"
        ][
            "offset_ms"
        ]
        == 500
    )

    assert (
        contract[
            "tape"
        ][
            "edge_column_allowed"
        ]
        is False
    )

    assert (
        contract[
            "tape"
        ][
            "trade_column_allowed"
        ]
        is False
    )

    assert (
        contract[
            "tape"
        ][
            "pnl_column_allowed"
        ]
        is False
    )

    assert (
        contract[
            "tape"
        ][
            "resolution_column_allowed"
        ]
        is False
    )

    conn = connect()

    rows = []
    decision_states = []
    execution_states = []

    market_cache = {}
    token_cache = {}

    market_unique_count = 0
    token_unique_count = 0
    config_covered = 0

    try:
        with conn.cursor() as cur:
            anchors = anchor_rows(
                cur
            )

            distinct_games = len({
                x["odds_game_id"]
                for x in anchors
            })

            for i, anchor in enumerate(
                anchors,
                start=1,
            ):
                identity = market_identity(
                    cur,
                    anchor[
                        "odds_game_id"
                    ],
                    anchor[
                        "outcome_team"
                    ],
                    market_cache,
                    token_cache,
                )

                if identity[
                    "market_unique"
                ]:
                    market_unique_count += 1

                if identity[
                    "token_unique"
                ]:
                    token_unique_count += 1

                cfg = config_at(
                    cur,
                    identity[
                        "polymarket_market_id"
                    ],
                    anchor[
                        "decision_time"
                    ],
                )

                config_ok = bool(
                    cfg
                    and cfg[
                        "minimum_order_size"
                    ] is not None
                    and cfg[
                        "minimum_tick_size"
                    ] is not None
                    and cfg[
                        "fee_rate"
                    ] is not None
                    and cfg[
                        "fee_exponent"
                    ] is not None
                    and cfg[
                        "taker_only"
                    ] is not None
                )

                if config_ok:
                    config_covered += 1

                decision_time = (
                    anchor[
                        "decision_time"
                    ]
                )

                execution_time = (
                    decision_time
                    + timedelta(
                        milliseconds=500
                    )
                )

                decision = reconstruct(
                    cur,
                    identity["token_id"],
                    decision_time,
                )

                execution = reconstruct(
                    cur,
                    identity["token_id"],
                    execution_time,
                )

                decision_states.append(
                    decision
                )

                execution_states.append(
                    execution
                )

                row = {
                    "sharp_consensus_id":
                        anchor[
                            "sharp_consensus_id"
                        ],

                    "poll_id":
                        anchor["poll_id"],

                    "odds_game_id":
                        anchor[
                            "odds_game_id"
                        ],

                    "outcome_team":
                        anchor[
                            "outcome_team"
                        ],

                    "decision_time":
                        decision_time,

                    "consensus_prob":
                        anchor[
                            "consensus_prob"
                        ],

                    "families_used":
                        anchor[
                            "families_used"
                        ],

                    "polymarket_market_id":
                        identity[
                            "polymarket_market_id"
                        ],

                    "token_id":
                        identity[
                            "token_id"
                        ],

                    "pm_outcome":
                        identity[
                            "pm_outcome"
                        ],

                    "decision_best_bid_mc":
                        decision[
                            "best_bid_mc"
                        ],

                    "decision_best_bid_size":
                        decision[
                            "best_bid_size"
                        ],

                    "decision_best_ask_mc":
                        decision[
                            "best_ask_mc"
                        ],

                    "decision_best_ask_size":
                        decision[
                            "best_ask_size"
                        ],

                    "execution_time":
                        execution_time,

                    "execution_best_bid_mc":
                        execution[
                            "best_bid_mc"
                        ],

                    "execution_best_bid_size":
                        execution[
                            "best_bid_size"
                        ],

                    "execution_best_ask_mc":
                        execution[
                            "best_ask_mc"
                        ],

                    "execution_best_ask_size":
                        execution[
                            "best_ask_size"
                        ],

                    "decision_base_source":
                        decision[
                            "base_source"
                        ],

                    "decision_base_time":
                        decision[
                            "base_time"
                        ],

                    "decision_base_generation":
                        decision[
                            "base_generation"
                        ],

                    "decision_replayed_deltas":
                        decision[
                            "replayed_deltas"
                        ],

                    "execution_base_source":
                        execution[
                            "base_source"
                        ],

                    "execution_base_time":
                        execution[
                            "base_time"
                        ],

                    "execution_base_generation":
                        execution[
                            "base_generation"
                        ],

                    "execution_replayed_deltas":
                        execution[
                            "replayed_deltas"
                        ],

                    "config_observed_at":
                        (
                            cfg[
                                "observed_at"
                            ]
                            if cfg
                            else None
                        ),

                    "minimum_order_size":
                        (
                            cfg[
                                "minimum_order_size"
                            ]
                            if cfg
                            else None
                        ),

                    "minimum_tick_size":
                        (
                            cfg[
                                "minimum_tick_size"
                            ]
                            if cfg
                            else None
                        ),

                    "fee_rate":
                        (
                            cfg[
                                "fee_rate"
                            ]
                            if cfg
                            else None
                        ),

                    "fee_exponent":
                        (
                            cfg[
                                "fee_exponent"
                            ]
                            if cfg
                            else None
                        ),

                    "taker_only":
                        (
                            cfg[
                                "taker_only"
                            ]
                            if cfg
                            else None
                        ),

                    # Cross-check provenance only.
                    "decision_reference_tob_time":
                        decision[
                            "reference_time"
                        ],

                    "decision_reference_tob_source":
                        decision[
                            "reference_source"
                        ],

                    "execution_reference_tob_time":
                        execution[
                            "reference_time"
                        ],

                    "execution_reference_tob_source":
                        execution[
                            "reference_source"
                        ],
                }

                rows.append(
                    clean(row)
                )

                if (
                    i % 100 == 0
                    or i == len(anchors)
                ):
                    print(
                        f"replayed "
                        f"{i}/{len(anchors)} "
                        f"anchors"
                    )

        # Read-only transaction ends without commit.
        conn.rollback()

    finally:
        conn.close()

    # -------------------------------------------------
    # Deterministic tape.
    # -------------------------------------------------
    tape_text = "".join(
        json.dumps(
            row,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
        for row in rows
    )

    TAPE.write_text(
        tape_text,
        encoding="utf-8",
    )

    tape_sha = sha256_file(
        TAPE
    )

    builder_sha = sha256_file(
        Path(__file__)
    )

    dm = endpoint_metrics(
        decision_states
    )

    em = endpoint_metrics(
        execution_states
    )

    anchor_count = len(rows)

    distinct_games = len({
        x["odds_game_id"]
        for x in rows
    })

    overall = (
        anchor_count
            == EXPECTED_ANCHORS

        and distinct_games
            == EXPECTED_GAMES

        and market_unique_count
            == EXPECTED_ANCHORS

        and token_unique_count
            == EXPECTED_ANCHORS

        and config_covered
            == EXPECTED_ANCHORS

        and dm["reconstructed"]
            == EXPECTED_ANCHORS

        and em["reconstructed"]
            == EXPECTED_ANCHORS

        and dm["two_sided"]
            == EXPECTED_ANCHORS

        and em["two_sided"]
            == EXPECTED_ANCHORS

        and dm[
            "generation_mixing"
        ] == 0

        and em[
            "generation_mixing"
        ] == 0

        and dm[
            "connection_mixing"
        ] == 0

        and em[
            "connection_mixing"
        ] == 0

        and dm["crossed"] == 0
        and em["crossed"] == 0

        and dm["lookahead"] == 0
        and em["lookahead"] == 0

        and dm[
            "reference_covered"
        ] == EXPECTED_ANCHORS

        and em[
            "reference_covered"
        ] == EXPECTED_ANCHORS

        and dm[
            "reference_price_matches"
        ] == EXPECTED_ANCHORS

        and em[
            "reference_price_matches"
        ] == EXPECTED_ANCHORS

        and dm[
            "reference_size_mismatch_anchors"
        ] == 0

        and em[
            "reference_size_mismatch_anchors"
        ] == 0
    )

    validation = {
        "version":
            "H2-v2-M2-VALIDATION-1",

        "contract_sha256":
            contract_sha,

        "builder_sha256":
            builder_sha,

        "tape_sha256":
            tape_sha,

        "engineering_dataset": {
            "sharp_collector_run_id":
                RUN_ID,

            "tape_rows":
                anchor_count,

            "distinct_games":
                distinct_games,

            "unique_market_mapping":
                market_unique_count,

            "unique_outcome_token_mapping":
                token_unique_count,

            "configuration_covered":
                config_covered,
        },

        "decision_endpoint":
            dm,

        "execution_500ms_endpoint":
            em,

        "decisions": {
            "m2_overall_pass":
                overall,
        },

        "interpretation": (
            "M2 validates deterministic "
            "synchronization and book replay only. "
            "No edge, threshold, trade, fill, PnL, "
            "resolution, calibration, or OOS "
            "strategy result is computed."
        ),
    }

    VALIDATION.write_text(
        json.dumps(
            clean(validation),
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    print()
    print(
        "H2-v2 M2 REPLAY VALIDATION COMPLETE"
    )

    print(
        "contract SHA:",
        contract_sha,
    )

    print(
        "tape rows:",
        anchor_count,
    )

    print(
        "games:",
        distinct_games,
    )

    print(
        "market mapping:",
        f"{market_unique_count}/{EXPECTED_ANCHORS}",
    )

    print(
        "token mapping:",
        f"{token_unique_count}/{EXPECTED_ANCHORS}",
    )

    print(
        "config:",
        f"{config_covered}/{EXPECTED_ANCHORS}",
    )

    print()

    print(
        "decision reconstructed:",
        f"{dm['reconstructed']}/{EXPECTED_ANCHORS}",
    )

    print(
        "decision price agreement:",
        f"{dm['reference_price_matches']}/"
        f"{EXPECTED_ANCHORS}",
    )

    print(
        "decision generation mixing:",
        dm["generation_mixing"],
    )

    print(
        "decision connection mixing:",
        dm["connection_mixing"],
    )

    print(
        "decision crossed:",
        dm["crossed"],
    )

    print()

    print(
        "execution reconstructed:",
        f"{em['reconstructed']}/{EXPECTED_ANCHORS}",
    )

    print(
        "execution price agreement:",
        f"{em['reference_price_matches']}/"
        f"{EXPECTED_ANCHORS}",
    )

    print(
        "execution generation mixing:",
        em["generation_mixing"],
    )

    print(
        "execution connection mixing:",
        em["connection_mixing"],
    )

    print(
        "execution crossed:",
        em["crossed"],
    )

    print()
    print(
        "M2 overall:",
        overall,
    )

    print(
        "tape:",
        TAPE.relative_to(REPO),
    )

    print(
        "validation:",
        VALIDATION.relative_to(REPO),
    )

    print(
        "NO EDGE / NO TRADES / NO PNL"
    )


if __name__ == "__main__":
    main()
