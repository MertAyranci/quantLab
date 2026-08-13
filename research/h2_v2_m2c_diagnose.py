from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import dotenv_values


REPO = Path(__file__).resolve().parents[1]
R = REPO / "research"

CONTRACT = R / "h2_v2_m2c_contract.json"

M2B_RESULTS = (
    R / "h2_v2_m2b_checkpoint_results.jsonl"
)

PAIR_OUT = (
    R / "h2_v2_m2c_pair_diagnostics.jsonl"
)

MISMATCH_OUT = (
    R / "h2_v2_m2c_mismatch_diagnostics.jsonl"
)

SUMMARY_OUT = (
    R / "h2_v2_m2c_summary.json"
)

EXPECTED_CONTRACT_SHA = (
    "db502a88e241071c1faabeef90a97e84"
    "cdab1c46fcea850964fd6e2b5d3cb19c"
)

EXPECTED_M2B_RESULTS_SHA = (
    "c058cd37fbdfed83f7c2909577ecd270f"
    "6608ce0d738af84cb486baab58dc66c"
)

EXPECTED_PAIRS = 142
EXPECTED_FAILED = 58
EXPECTED_SIZE_MISMATCHES = 114


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


def dt(x):
    if isinstance(x, datetime):
        return x

    return datetime.fromisoformat(
        str(x).replace(
            "Z",
            "+00:00",
        )
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

    conn.set_session(
        readonly=True,
        autocommit=False,
    )

    return conn


def normalized_book(raw):
    out = {}

    for level in raw or []:
        price = int(level[0])
        size = Decimal(
            str(level[1])
        )

        if size > 0:
            out[price] = size

    return out


def fetch_snapshot(
    cur,
    token_id,
    capture_time,
    source,
    generation,
):
    cur.execute(
        """
        SELECT
            token_id,
            event_time,
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
            AND capture_time = %s
            AND source = %s
            AND book_generation = %s
            AND watchlist_rule = 'h2_v2'
        """,
        (
            token_id,
            capture_time,
            source,
            generation,
        ),
    )

    rows = [
        dict(x)
        for x in cur.fetchall()
    ]

    if len(rows) != 1:
        raise RuntimeError(
            "snapshot identity not unique: "
            f"token={token_id} "
            f"time={capture_time} "
            f"source={source} "
            f"generation={generation} "
            f"rows={len(rows)}"
        )

    return rows[0]


def fetch_interval_deltas(
    cur,
    token_id,
    base,
    checkpoint,
):
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
            AND capture_time < %s
            AND watchlist_rule = 'h2_v2'

        ORDER BY
            ingest_sequence ASC,
            change_index ASC,
            capture_time ASC
        """,
        (
            token_id,
            base["capture_time"],
            checkpoint["capture_time"],
        ),
    )

    return [
        dict(x)
        for x in cur.fetchall()
    ]


def replay_pair(
    cur,
    frozen,
    base,
    checkpoint,
):
    deltas = fetch_interval_deltas(
        cur,
        frozen["token_id"],
        base,
        checkpoint,
    )

    same_gen = [
        d
        for d in deltas
        if d["book_generation"]
        == base["book_generation"]
    ]

    replay = []

    checkpoint_conn = checkpoint[
        "connection_id"
    ]

    checkpoint_seq = checkpoint[
        "ingest_sequence"
    ]

    if base["source"] == "ws_book":
        for d in same_gen:
            if (
                d["connection_id"]
                == checkpoint_conn
                and
                base["ingest_sequence"]
                < d["ingest_sequence"]
                < checkpoint_seq
            ):
                replay.append(d)

    elif base["source"] == "rest_book":
        for d in same_gen:
            if (
                d["connection_id"]
                == checkpoint_conn
                and
                d["capture_time"]
                > base["capture_time"]
                and
                d["ingest_sequence"]
                < checkpoint_seq
            ):
                replay.append(d)

    else:
        raise RuntimeError(
            f"unexpected base source "
            f"{base['source']}"
        )

    bids = normalized_book(
        base["bids"]
    )

    asks = normalized_book(
        base["asks"]
    )

    for d in replay:
        price = int(
            d["price_mc"]
        )

        size = Decimal(
            str(d["size"])
        )

        if d["side"] == "B":
            book = bids

        elif d["side"] == "S":
            book = asks

        else:
            raise RuntimeError(
                f"bad side {d['side']!r}"
            )

        if size == 0:
            book.pop(
                price,
                None,
            )
        else:
            book[price] = size

    target_bids = normalized_book(
        checkpoint["bids"]
    )

    target_asks = normalized_book(
        checkpoint["asks"]
    )

    mismatches = []

    for side, replayed, target in (
        (
            "B",
            bids,
            target_bids,
        ),
        (
            "S",
            asks,
            target_asks,
        ),
    ):
        all_prices = sorted(
            set(replayed)
            | set(target)
        )

        for price in all_prices:
            r = replayed.get(price)
            t = target.get(price)

            if r == t:
                continue

            mismatch_type = (
                "presence"
                if (
                    r is None
                    or t is None
                )
                else "size"
            )

            delta = (
                None
                if (
                    r is None
                    or t is None
                )
                else t - r
            )

            if delta is None:
                direction = (
                    "checkpoint_added_level"
                    if r is None
                    else "checkpoint_removed_level"
                )

            elif delta < 0:
                direction = (
                    "checkpoint_lower"
                )

            elif delta > 0:
                direction = (
                    "checkpoint_higher"
                )

            else:
                direction = "equal"

            mismatches.append({
                "side":
                    side,

                "price_mc":
                    price,

                "replayed_size":
                    r,

                "checkpoint_size":
                    t,

                "checkpoint_minus_replay":
                    delta,

                "mismatch_type":
                    mismatch_type,

                "direction":
                    direction,
            })

    exact = (
        bids == target_bids
        and asks == target_asks
    )

    # Reproduce frozen M2b result exactly.
    expected_exact = frozen[
        "exact_full_book_match"
    ]

    expected_size = frozen[
        "size_mismatches"
    ]

    presence_count = sum(
        x["mismatch_type"]
        == "presence"
        for x in mismatches
    )

    size_count = sum(
        x["mismatch_type"]
        == "size"
        for x in mismatches
    )

    if exact != expected_exact:
        raise RuntimeError(
            "M2b exact-match reproduction failed"
        )

    if presence_count != (
        frozen[
            "bid_presence_mismatches"
        ]
        +
        frozen[
            "ask_presence_mismatches"
        ]
    ):
        raise RuntimeError(
            "M2b presence reproduction failed"
        )

    if size_count != expected_size:
        raise RuntimeError(
            "M2b size reproduction failed"
        )

    return replay, mismatches


def fetch_trades(
    cur,
    token_id,
    start,
    end,
):
    cur.execute(
        """
        SELECT
            token_id,
            event_time,
            capture_time,
            price_mc,
            size,
            side,
            connection_id,
            ingest_sequence

        FROM last_trade_events

        WHERE
            token_id = %s
            AND capture_time >= %s
            AND capture_time <= %s
            AND source =
                'ws_last_trade_price'

        ORDER BY
            capture_time,
            ingest_sequence
        """,
        (
            token_id,
            start,
            end,
        ),
    )

    return [
        dict(x)
        for x in cur.fetchall()
    ]


def abs_ms(a, b):
    return abs(
        (a - b).total_seconds()
        * 1000
    )


def trade_diagnostics(
    checkpoint,
    trades,
):
    event_time = checkpoint[
        "event_time"
    ]

    conn = checkpoint[
        "connection_id"
    ]

    seq = checkpoint[
        "ingest_sequence"
    ]

    out = {
        "trade_rows_for_token":
            len(trades),

        "nearest_event_trade":
            None,

        "nearest_same_connection_trade":
            None,

        "exact_event_time_trade_count":
            0,

        "exact_event_time_same_connection_count":
            0,

        "adjacent_sequence_trade_count":
            0,

        "associated_trade":
            None,

        "association_rule":
            None,
    }

    if not trades:
        return out

    event_candidates = [
        t
        for t in trades
        if (
            t["event_time"]
            is not None
            and event_time
            is not None
        )
    ]

    if event_candidates:
        nearest = min(
            event_candidates,
            key=lambda t: (
                abs_ms(
                    t["event_time"],
                    event_time,
                ),
                abs(
                    t["ingest_sequence"]
                    - seq
                )
                if (
                    t["ingest_sequence"]
                    is not None
                    and seq is not None
                )
                else 10**18,
            ),
        )

        out[
            "nearest_event_trade"
        ] = {
            "event_time":
                nearest["event_time"],

            "capture_time":
                nearest["capture_time"],

            "event_abs_distance_ms":
                abs_ms(
                    nearest["event_time"],
                    event_time,
                ),

            "price_mc":
                nearest["price_mc"],

            "size":
                nearest["size"],

            "side":
                nearest["side"],

            "connection_id":
                str(
                    nearest[
                        "connection_id"
                    ]
                ),

            "ingest_sequence":
                nearest[
                    "ingest_sequence"
                ],
        }

    exact = [
        t
        for t in event_candidates
        if t["event_time"]
        == event_time
    ]

    out[
        "exact_event_time_trade_count"
    ] = len(exact)

    exact_same_conn = [
        t
        for t in exact
        if t["connection_id"] == conn
    ]

    out[
        "exact_event_time_same_connection_count"
    ] = len(
        exact_same_conn
    )

    same_conn = [
        t
        for t in trades
        if (
            t["connection_id"]
            == conn
            and t["ingest_sequence"]
            is not None
            and seq is not None
        )
    ]

    nearest_seq = None

    if same_conn:
        nearest_seq = min(
            same_conn,
            key=lambda t: (
                abs(
                    seq
                    - t["ingest_sequence"]
                ),
                abs_ms(
                    t["capture_time"],
                    checkpoint[
                        "capture_time"
                    ],
                ),
            ),
        )

        signed_gap = (
            seq
            - nearest_seq[
                "ingest_sequence"
            ]
        )

        out[
            "nearest_same_connection_trade"
        ] = {
            "event_time":
                nearest_seq["event_time"],

            "capture_time":
                nearest_seq[
                    "capture_time"
                ],

            "checkpoint_minus_trade_sequence":
                signed_gap,

            "absolute_sequence_gap":
                abs(signed_gap),

            "capture_abs_distance_ms":
                abs_ms(
                    nearest_seq[
                        "capture_time"
                    ],
                    checkpoint[
                        "capture_time"
                    ],
                ),

            "price_mc":
                nearest_seq["price_mc"],

            "size":
                nearest_seq["size"],

            "side":
                nearest_seq["side"],

            "ingest_sequence":
                nearest_seq[
                    "ingest_sequence"
                ],
        }

    adjacent = [
        t
        for t in same_conn
        if abs(
            seq
            - t["ingest_sequence"]
        ) == 1
    ]

    out[
        "adjacent_sequence_trade_count"
    ] = len(adjacent)

    # Frozen association rule:
    # 1. exact server event_time on same connection
    # 2. otherwise adjacent sequence on same connection
    associated = None
    rule = None

    if exact_same_conn:
        associated = min(
            exact_same_conn,
            key=lambda t: abs(
                seq
                - t["ingest_sequence"]
            ),
        )
        rule = (
            "exact_event_time_same_connection"
        )

    elif adjacent:
        associated = min(
            adjacent,
            key=lambda t: abs_ms(
                t["capture_time"],
                checkpoint[
                    "capture_time"
                ],
            ),
        )
        rule = (
            "adjacent_sequence_same_connection"
        )

    if associated is not None:
        out[
            "associated_trade"
        ] = {
            "event_time":
                associated[
                    "event_time"
                ],

            "capture_time":
                associated[
                    "capture_time"
                ],

            "price_mc":
                associated["price_mc"],

            "size":
                associated["size"],

            "side":
                associated["side"],

            "connection_id":
                str(
                    associated[
                        "connection_id"
                    ]
                ),

            "ingest_sequence":
                associated[
                    "ingest_sequence"
                ],

            "checkpoint_minus_trade_sequence":
                (
                    seq
                    - associated[
                        "ingest_sequence"
                    ]
                ),
        }

        out[
            "association_rule"
        ] = rule

    return out


def percentile(values, q):
    if not values:
        return None

    xs = sorted(
        Decimal(str(x))
        for x in values
    )

    if len(xs) == 1:
        return xs[0]

    pos = (
        Decimal(len(xs) - 1)
        * Decimal(str(q))
    )

    lo = int(pos)
    hi = min(
        lo + 1,
        len(xs) - 1,
    )

    frac = (
        pos
        - Decimal(lo)
    )

    return (
        xs[lo] * (
            Decimal("1")
            - frac
        )
        +
        xs[hi] * frac
    )


def distribution(values):
    if not values:
        return {
            "n": 0,
            "p50": None,
            "p90": None,
            "max": None,
        }

    return {
        "n":
            len(values),

        "p50":
            percentile(
                values,
                "0.50",
            ),

        "p90":
            percentile(
                values,
                "0.90",
            ),

        "max":
            max(
                Decimal(str(x))
                for x in values
            ),
    }


def main():
    if (
        sha256_file(CONTRACT)
        != EXPECTED_CONTRACT_SHA
    ):
        raise SystemExit(
            "M2c contract SHA mismatch"
        )

    if (
        sha256_file(M2B_RESULTS)
        != EXPECTED_M2B_RESULTS_SHA
    ):
        raise SystemExit(
            "M2b results SHA mismatch"
        )

    frozen_rows = [
        json.loads(line)
        for line
        in M2B_RESULTS.read_text(
            encoding="utf-8"
        ).splitlines()
        if line.strip()
    ]

    if len(frozen_rows) != EXPECTED_PAIRS:
        raise SystemExit(
            "unexpected M2b pair count"
        )

    all_times = []

    for row in frozen_rows:
        all_times.extend([
            dt(row["base_time"]),
            dt(row["checkpoint_time"]),
        ])

    trade_start = (
        min(all_times)
        - timedelta(seconds=5)
    )

    trade_end = (
        max(all_times)
        + timedelta(seconds=5)
    )

    conn = connect()

    pair_rows = []
    mismatch_rows = []
    trade_cache = {}

    try:
        with conn.cursor() as cur:
            for n, frozen in enumerate(
                frozen_rows,
                start=1,
            ):
                token_id = frozen[
                    "token_id"
                ]

                base = fetch_snapshot(
                    cur,
                    token_id,
                    dt(
                        frozen[
                            "base_time"
                        ]
                    ),
                    frozen[
                        "base_source"
                    ],
                    frozen[
                        "book_generation"
                    ],
                )

                checkpoint = (
                    fetch_snapshot(
                        cur,
                        token_id,
                        dt(
                            frozen[
                                "checkpoint_time"
                            ]
                        ),
                        "ws_book",
                        frozen[
                            "book_generation"
                        ],
                    )
                )

                _, mismatches = (
                    replay_pair(
                        cur,
                        frozen,
                        base,
                        checkpoint,
                    )
                )

                if token_id not in trade_cache:
                    trade_cache[
                        token_id
                    ] = fetch_trades(
                        cur,
                        token_id,
                        trade_start,
                        trade_end,
                    )

                td = trade_diagnostics(
                    checkpoint,
                    trade_cache[
                        token_id
                    ],
                )

                failed = not frozen[
                    "exact_full_book_match"
                ]

                associated = td[
                    "associated_trade"
                ]

                pair = {
                    "token_id":
                        token_id,

                    "pair_index":
                        frozen[
                            "pair_index"
                        ],

                    "pair_status":
                        (
                            "failed"
                            if failed
                            else "passed"
                        ),

                    "base_source":
                        base["source"],

                    "book_generation":
                        base[
                            "book_generation"
                        ],

                    "base_time":
                        base[
                            "capture_time"
                        ],

                    "checkpoint_time":
                        checkpoint[
                            "capture_time"
                        ],

                    "checkpoint_event_time":
                        checkpoint[
                            "event_time"
                        ],

                    "checkpoint_connection_id":
                        str(
                            checkpoint[
                                "connection_id"
                            ]
                        ),

                    "checkpoint_ingest_sequence":
                        checkpoint[
                            "ingest_sequence"
                        ],

                    "size_mismatches":
                        len([
                            x
                            for x in mismatches
                            if x[
                                "mismatch_type"
                            ]
                            == "size"
                        ]),

                    "presence_mismatches":
                        len([
                            x
                            for x in mismatches
                            if x[
                                "mismatch_type"
                            ]
                            == "presence"
                        ]),

                    **td,
                }

                pair_rows.append(
                    pair
                )

                for i, mismatch in enumerate(
                    mismatches
                ):
                    associated_side = (
                        associated["side"]
                        if associated
                        else None
                    )

                    reduction = (
                        mismatch[
                            "direction"
                        ]
                        == "checkpoint_lower"
                    )

                    side_consistent = None

                    if associated is not None:
                        side_consistent = (
                            reduction
                            and (
                                (
                                    associated_side
                                    == "BUY"
                                    and mismatch[
                                        "side"
                                    ]
                                    == "S"
                                )
                                or
                                (
                                    associated_side
                                    == "SELL"
                                    and mismatch[
                                        "side"
                                    ]
                                    == "B"
                                )
                            )
                        )

                    mismatch_rows.append({
                        "token_id":
                            token_id,

                        "pair_index":
                            frozen[
                                "pair_index"
                            ],

                        "base_source":
                            base["source"],

                        "checkpoint_time":
                            checkpoint[
                                "capture_time"
                            ],

                        "mismatch_index":
                            i,

                        **mismatch,

                        "associated_trade":
                            associated,

                        "association_rule":
                            td[
                                "association_rule"
                            ],

                        "trade_side_consistent":
                            side_consistent,

                        "associated_trade_price_matches_level":
                            (
                                (
                                    associated[
                                        "price_mc"
                                    ]
                                    == mismatch[
                                        "price_mc"
                                    ]
                                )
                                if associated
                                else None
                            ),
                    })

                if (
                    n % 25 == 0
                    or n
                    == len(frozen_rows)
                ):
                    print(
                        f"diagnosed "
                        f"{n}/"
                        f"{len(frozen_rows)} "
                        f"pairs"
                    )

        conn.rollback()

    finally:
        conn.close()

    failed_pairs = [
        x
        for x in pair_rows
        if x["pair_status"]
        == "failed"
    ]

    passed_pairs = [
        x
        for x in pair_rows
        if x["pair_status"]
        == "passed"
    ]

    if len(failed_pairs) != EXPECTED_FAILED:
        raise RuntimeError(
            "failed pair count no longer "
            "matches frozen M2b"
        )

    if len(mismatch_rows) != (
        EXPECTED_SIZE_MISMATCHES
    ):
        raise RuntimeError(
            "mismatch count no longer "
            "matches frozen M2b"
        )

    def pair_summary(rows):
        nearest_ms = []

        for row in rows:
            nearest = row[
                "nearest_event_trade"
            ]

            if nearest is not None:
                nearest_ms.append(
                    nearest[
                        "event_abs_distance_ms"
                    ]
                )

        return {
            "pairs":
                len(rows),

            "tokens":
                len({
                    x["token_id"]
                    for x in rows
                }),

            "base_sources":
                dict(
                    sorted(
                        Counter(
                            x["base_source"]
                            for x in rows
                        ).items()
                    )
                ),

            "pairs_with_any_token_trade":
                sum(
                    x[
                        "trade_rows_for_token"
                    ] > 0
                    for x in rows
                ),

            "pairs_with_exact_event_time_trade":
                sum(
                    x[
                        "exact_event_time_trade_count"
                    ] > 0
                    for x in rows
                ),

            "pairs_with_exact_event_time_same_connection_trade":
                sum(
                    x[
                        "exact_event_time_same_connection_count"
                    ] > 0
                    for x in rows
                ),

            "pairs_with_adjacent_sequence_trade":
                sum(
                    x[
                        "adjacent_sequence_trade_count"
                    ] > 0
                    for x in rows
                ),

            "pairs_with_associated_trade":
                sum(
                    x[
                        "associated_trade"
                    ] is not None
                    for x in rows
                ),

            "nearest_trade_event_abs_distance_ms":
                distribution(
                    nearest_ms
                ),
        }

    side_counts = Counter(
        x["side"]
        for x in mismatch_rows
    )

    direction_counts = Counter(
        x["direction"]
        for x in mismatch_rows
    )

    base_counts = Counter(
        x["base_source"]
        for x in mismatch_rows
    )

    associated_mismatches = [
        x
        for x in mismatch_rows
        if x["associated_trade"]
        is not None
    ]

    side_consistent = sum(
        x["trade_side_consistent"]
        is True
        for x in associated_mismatches
    )

    side_inconsistent = sum(
        x["trade_side_consistent"]
        is False
        for x in associated_mismatches
    )

    price_match = sum(
        x[
            "associated_trade_price_matches_level"
        ]
        is True
        for x in associated_mismatches
    )

    summary = {
        "version":
            "H2-v2-M2c-SUMMARY-1",

        "contract_sha256":
            EXPECTED_CONTRACT_SHA,

        "m2b_checkpoint_results_sha256":
            EXPECTED_M2B_RESULTS_SHA,

        "diagnostic_sha256":
            sha256_file(
                Path(__file__)
            ),

        "frozen_parent_reproduction": {
            "pairs":
                len(pair_rows),

            "passed_pairs":
                len(passed_pairs),

            "failed_pairs":
                len(failed_pairs),

            "size_mismatches":
                len(mismatch_rows),

            "presence_mismatches":
                sum(
                    x[
                        "presence_mismatches"
                    ]
                    for x in pair_rows
                ),
        },

        "failed_pair_trade_diagnostics":
            pair_summary(
                failed_pairs
            ),

        "passed_pair_control":
            pair_summary(
                passed_pairs
            ),

        "mismatch_diagnostics": {
            "mismatches":
                len(mismatch_rows),

            "by_side":
                dict(
                    sorted(
                        side_counts.items()
                    )
                ),

            "by_direction":
                dict(
                    sorted(
                        direction_counts.items()
                    )
                ),

            "by_base_source":
                dict(
                    sorted(
                        base_counts.items()
                    )
                ),

            "mismatches_with_associated_trade":
                len(
                    associated_mismatches
                ),

            "trade_side_consistent":
                side_consistent,

            "trade_side_inconsistent":
                side_inconsistent,

            "associated_trade_price_matches_level":
                price_match,
        },

        "association_rule": {
            "primary":
                (
                    "same-token exact server "
                    "event_time on same connection"
                ),

            "fallback":
                (
                    "same-token adjacent WS sequence "
                    "(absolute sequence gap = 1) "
                    "on same connection"
                ),
        },

        "interpretation":
            (
                "Diagnostic only. M2 and M2b remain "
                "frozen FAIL regardless of M2c "
                "findings. No strategy performance "
                "information was inspected."
            ),
    }

    PAIR_OUT.write_text(
        "".join(
            json.dumps(
                clean(x),
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
            for x in pair_rows
        ),
        encoding="utf-8",
    )

    MISMATCH_OUT.write_text(
        "".join(
            json.dumps(
                clean(x),
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
            for x in mismatch_rows
        ),
        encoding="utf-8",
    )

    summary[
        "pair_diagnostics_sha256"
    ] = sha256_file(
        PAIR_OUT
    )

    summary[
        "mismatch_diagnostics_sha256"
    ] = sha256_file(
        MISMATCH_OUT
    )

    SUMMARY_OUT.write_text(
        json.dumps(
            clean(summary),
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    print()
    print(
        "H2-v2 M2c DIAGNOSTIC COMPLETE"
    )

    print(
        "pairs:",
        len(pair_rows),
    )

    print(
        "passed controls:",
        len(passed_pairs),
    )

    print(
        "failed pairs:",
        len(failed_pairs),
    )

    print(
        "size mismatches:",
        len(mismatch_rows),
    )

    print()

    fs = summary[
        "failed_pair_trade_diagnostics"
    ]

    ps = summary[
        "passed_pair_control"
    ]

    print(
        "FAILED exact-event trade:",
        fs[
            "pairs_with_exact_event_time_trade"
        ],
        "/",
        fs["pairs"],
    )

    print(
        "FAILED adjacent-seq trade:",
        fs[
            "pairs_with_adjacent_sequence_trade"
        ],
        "/",
        fs["pairs"],
    )

    print(
        "FAILED associated trade:",
        fs[
            "pairs_with_associated_trade"
        ],
        "/",
        fs["pairs"],
    )

    print()

    print(
        "PASSED exact-event trade:",
        ps[
            "pairs_with_exact_event_time_trade"
        ],
        "/",
        ps["pairs"],
    )

    print(
        "PASSED adjacent-seq trade:",
        ps[
            "pairs_with_adjacent_sequence_trade"
        ],
        "/",
        ps["pairs"],
    )

    print(
        "PASSED associated trade:",
        ps[
            "pairs_with_associated_trade"
        ],
        "/",
        ps["pairs"],
    )

    print()

    md = summary[
        "mismatch_diagnostics"
    ]

    print(
        "mismatch side counts:",
        md["by_side"],
    )

    print(
        "mismatch directions:",
        md["by_direction"],
    )

    print(
        "mismatches with associated trade:",
        md[
            "mismatches_with_associated_trade"
        ],
    )

    print(
        "trade-side consistent:",
        md[
            "trade_side_consistent"
        ],
    )

    print(
        "trade-side inconsistent:",
        md[
            "trade_side_inconsistent"
        ],
    )

    print()
    print(
        "NO EDGE / NO TRADES / NO PNL"
    )


if __name__ == "__main__":
    main()
