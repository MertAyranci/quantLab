from __future__ import annotations

import hashlib
import json
import statistics
import subprocess
from collections import defaultdict
from decimal import Decimal, getcontext
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import dotenv_values


getcontext().prec = 40

REPO = Path(__file__).resolve().parents[1]
R = REPO / "research"

CONTRACT = R / "h2_v2_m3a_contract.json"
SUPERSEDED = R / "h2_v2_m3_contract_superseded_30m.json"
SUPERSESSION = R / "h2_v2_m3_contract_supersession.json"

TAPE = R / "h2_v2_m2h_tape.jsonl"
M2H_VALIDATION = R / "h2_v2_m2h_validation.json"
M2H_CONCLUSION = R / "h2_v2_m2h_conclusion.json"

ROW_OUT = R / "h2_v2_m3a_row_diagnostics.jsonl"
THRESHOLD_OUT = R / "h2_v2_m3a_threshold_summary.json"
SUMMARY_OUT = R / "h2_v2_m3a_summary.json"

EXPECTED_HEAD = (
    "935b44af95e8f22ea89cfd69d4263272c86fe74b"
)

EXPECTED_HASHES = {
    CONTRACT:
        "9a63e51ae695009330ab90f542f8cb7fe6e1ae1ede7759f721f4bb5954dfd3bc",

    SUPERSEDED:
        "f79a4141c9844c84d63d91258c379de4ca81caf54c2cf0b85615cf769212fdf1",

    SUPERSESSION:
        "74a90bbb56b9fbdca490c2f703444506b7d83e64d034eb7905bd8c63eb7a3f76",

    TAPE:
        "c234267e3342601c9706c39d7d4c20c93eaeea1df5b0ca0a5513cf67221be75f",

    M2H_VALIDATION:
        "c6883a4994cee12e0da9a5c7621c27a5046b76e43363e9679e2b2fc6bfa0d1ed",

    M2H_CONCLUSION:
        "c992fb70d32247e3e19102a5c708a4084385c16f7407b00ae8690adf851092da",
}

EXPECTED_ROWS = 1080
EXPECTED_GAMES = 9

THRESHOLDS = [
    Decimal("0.005"),
    Decimal("0.01"),
    Decimal("0.015"),
    Decimal("0.02"),
    Decimal("0.025"),
    Decimal("0.03"),
    Decimal("0.04"),
    Decimal("0.05"),
]

POSITION_SHARES = Decimal("5")
STRUCTURAL_THRESHOLD = Decimal("0.01")
STRUCTURAL_MARKETS_REQUIRED = 3


def sha256_file(path: Path) -> str:
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def dec(x):
    if x is None:
        return None
    return Decimal(str(x))


def dstr(x):
    if x is None:
        return None
    return format(
        Decimal(x),
        "f",
    )


def clean(x):
    if isinstance(x, Decimal):
        return dstr(x)

    if hasattr(x, "isoformat"):
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


def q(values, frac):
    if not values:
        return None

    xs = sorted(values)

    if len(xs) == 1:
        return xs[0]

    pos = (
        Decimal(str(frac))
        * Decimal(len(xs) - 1)
    )

    lo = int(pos)
    hi = min(
        lo + 1,
        len(xs) - 1,
    )

    w = pos - Decimal(lo)

    return (
        xs[lo] * (Decimal("1") - w)
        + xs[hi] * w
    )


def distribution(values):
    if not values:
        return {
            "count": 0,
            "min": None,
            "p10": None,
            "p25": None,
            "median": None,
            "p75": None,
            "p90": None,
            "p95": None,
            "p99": None,
            "max": None,
            "mean": None,
        }

    return {
        "count": len(values),
        "min": min(values),
        "p10": q(values, "0.10"),
        "p25": q(values, "0.25"),
        "median": q(values, "0.50"),
        "p75": q(values, "0.75"),
        "p90": q(values, "0.90"),
        "p95": q(values, "0.95"),
        "p99": q(values, "0.99"),
        "max": max(values),
        "mean": (
            sum(values, Decimal("0"))
            / Decimal(len(values))
        ),
    }


def fee_per_share(
    fee_rate: Decimal,
    price: Decimal,
) -> Decimal:
    return (
        fee_rate
        * price
        * (Decimal("1") - price)
    )


def load_tape():
    rows = []

    with TAPE.open(
        encoding="utf-8"
    ) as f:
        for lineno, line in enumerate(
            f,
            start=1,
        ):
            if not line.strip():
                continue

            row = json.loads(line)

            row["_line"] = lineno

            rows.append(row)

    return rows


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
        isolation_level="REPEATABLE READ",
        readonly=True,
        autocommit=False,
    )

    return conn


def load_resolutions(
    conn,
    market_ids,
    market_tokens,
):
    result = {}

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                id,
                market_id,
                event_time,
                capture_time,
                winning_token_id,
                winning_outcome,
                learned_via,
                status

            FROM resolutions

            WHERE
                market_id = ANY(%s)
                AND status IN (
                    'final',
                    'corrected'
                )

            ORDER BY
                market_id ASC,
                capture_time ASC,
                id ASC
            """,
            (
                list(
                    sorted(
                        market_ids
                    )
                ),
            ),
        )

        grouped = defaultdict(list)

        for r in cur.fetchall():
            grouped[
                int(r["market_id"])
            ].append(
                dict(r)
            )

    for market_id in sorted(
        market_ids
    ):
        rows = grouped.get(
            market_id,
            [],
        )

        if not rows:
            result[market_id] = {
                "resolution_complete": False,
                "resolution_ambiguous": False,
                "winning_token_id": None,
                "winning_outcome": None,
                "status": None,
                "capture_time": None,
                "learned_via": None,
                "reason": "NO_FINAL_OR_CORRECTED_RESOLUTION",
            }
            continue

        latest_time = max(
            r["capture_time"]
            for r in rows
        )

        latest_rows = [
            r
            for r in rows
            if r["capture_time"]
            == latest_time
        ]

        winners = {
            int(r["winning_token_id"])
            if r["winning_token_id"]
            is not None
            else None
            for r in latest_rows
        }

        if (
            len(winners) != 1
            or None in winners
        ):
            result[market_id] = {
                "resolution_complete": False,
                "resolution_ambiguous": True,
                "winning_token_id": None,
                "winning_outcome": None,
                "status": None,
                "capture_time": latest_time,
                "learned_via": None,
                "reason": "AMBIGUOUS_LATEST_RESOLUTION",
            }
            continue

        winning_token_id = next(
            iter(winners)
        )

        expected_tokens = (
            market_tokens[
                market_id
            ]
        )

        if (
            winning_token_id
            not in expected_tokens
        ):
            result[market_id] = {
                "resolution_complete": False,
                "resolution_ambiguous": True,
                "winning_token_id": winning_token_id,
                "winning_outcome": None,
                "status": None,
                "capture_time": latest_time,
                "learned_via": None,
                "reason": "WINNING_TOKEN_NOT_IN_DEVELOPMENT_MARKET",
            }
            continue

        representative = latest_rows[-1]

        result[market_id] = {
            "resolution_complete": True,
            "resolution_ambiguous": False,
            "winning_token_id":
                winning_token_id,
            "winning_outcome":
                representative[
                    "winning_outcome"
                ],
            "status":
                representative["status"],
            "capture_time":
                representative[
                    "capture_time"
                ],
            "learned_via":
                representative[
                    "learned_via"
                ],
            "reason": "OK",
        }

    return result


def main():
    # --------------------------------------------------
    # Immutable input checks before opening economics.
    # --------------------------------------------------
    for path, expected in (
        EXPECTED_HASHES.items()
    ):
        actual = sha256_file(
            path
        )

        if actual != expected:
            raise SystemExit(
                "REFUSING M3a: frozen hash changed\n"
                f"path={path}\n"
                f"expected={expected}\n"
                f"actual={actual}"
            )

    head = subprocess.check_output(
        [
            "git",
            "-C",
            str(REPO),
            "rev-parse",
            "HEAD",
        ],
        text=True,
    ).strip()

    if head != EXPECTED_HEAD:
        raise SystemExit(
            "REFUSING M3a: HEAD changed\n"
            f"expected={EXPECTED_HEAD}\n"
            f"actual={head}"
        )

    contract = json.loads(
        CONTRACT.read_text(
            encoding="utf-8"
        )
    )

    assert (
        contract["version"]
        == "H2-v2-M3a-CONTRACT-1"
    )

    assert (
        contract["cost"][
            "new_odds_api_calls"
        ]
        == 0
    )

    assert (
        contract["cost"][
            "new_live_collection"
        ]
        is False
    )

    assert (
        contract[
            "formal_h2_v2_design_unchanged"
        ][
            "formal_threshold_may_be_selected_in_m3a"
        ]
        is False
    )

    frozen_thresholds = [
        Decimal(str(x))
        for x in contract[
            "candidate_thresholds"
        ]
    ]

    if frozen_thresholds != THRESHOLDS:
        raise SystemExit(
            "M3a threshold grid mismatch"
        )

    gate = contract[
        "m3a_progression_gate"
    ]

    if (
        Decimal(
            str(
                gate["threshold"]
            )
        )
        != STRUCTURAL_THRESHOLD
        or
        int(
            gate[
                "required_distinct_markets"
            ]
        )
        != STRUCTURAL_MARKETS_REQUIRED
    ):
        raise SystemExit(
            "M3a structural gate mismatch"
        )

    rows = load_tape()

    if len(rows) != EXPECTED_ROWS:
        raise SystemExit(
            "unexpected M2h tape rows: "
            f"{len(rows)}"
        )

    games = {
        int(r["odds_game_id"])
        for r in rows
    }

    if len(games) != EXPECTED_GAMES:
        raise SystemExit(
            "unexpected development games: "
            f"{len(games)}"
        )

    markets = {
        int(
            r[
                "polymarket_market_id"
            ]
        )
        for r in rows
    }

    if len(markets) != EXPECTED_GAMES:
        raise SystemExit(
            "expected exactly one canonical "
            "market per development game"
        )

    game_to_markets = defaultdict(set)

    market_tokens = defaultdict(set)

    for r in rows:
        game_to_markets[
            int(r["odds_game_id"])
        ].add(
            int(
                r[
                    "polymarket_market_id"
                ]
            )
        )

        market_tokens[
            int(
                r[
                    "polymarket_market_id"
                ]
            )
        ].add(
            int(r["token_id"])
        )

    if any(
        len(x) != 1
        for x in game_to_markets.values()
    ):
        raise SystemExit(
            "development game maps to "
            "multiple markets"
        )

    if any(
        len(x) != 2
        for x in market_tokens.values()
    ):
        raise SystemExit(
            "development market does not "
            "have exactly two tape tokens"
        )

    # --------------------------------------------------
    # Fee/config pre-analysis gate.
    # No output is written before this passes.
    # --------------------------------------------------
    market_config = {}

    for r in rows:
        market_id = int(
            r[
                "polymarket_market_id"
            ]
        )

        fee_rate = dec(
            r.get("fee_rate")
        )

        fee_exponent = dec(
            r.get("fee_exponent")
        )

        taker_only = r.get(
            "taker_only"
        )

        minimum_order_size = dec(
            r.get(
                "minimum_order_size"
            )
        )

        if fee_rate is None:
            raise SystemExit(
                "REFUSING ECONOMIC OUTPUT: "
                "missing fee_rate"
            )

        if fee_exponent is None:
            raise SystemExit(
                "REFUSING ECONOMIC OUTPUT: "
                "missing fee_exponent"
            )

        if taker_only is None:
            raise SystemExit(
                "REFUSING ECONOMIC OUTPUT: "
                "missing taker_only"
            )

        if minimum_order_size is None:
            raise SystemExit(
                "REFUSING ECONOMIC OUTPUT: "
                "missing minimum_order_size"
            )

        if fee_rate < 0:
            raise SystemExit(
                "REFUSING ECONOMIC OUTPUT: "
                "negative fee_rate"
            )

        # Frozen M3a fee formula is explicitly
        # taker-only. If captured config says
        # otherwise, stop rather than improvise.
        if taker_only is not True:
            raise SystemExit(
                "REFUSING ECONOMIC OUTPUT: "
                "captured config is not taker-only"
            )

        cfg = (
            fee_rate,
            fee_exponent,
            taker_only,
            minimum_order_size,
        )

        if market_id not in market_config:
            market_config[
                market_id
            ] = cfg

        elif (
            market_config[
                market_id
            ]
            != cfg
        ):
            raise SystemExit(
                "REFUSING ECONOMIC OUTPUT: "
                "inconsistent frozen config "
                f"within market {market_id}"
            )

    # --------------------------------------------------
    # Existing resolution evidence only.
    # --------------------------------------------------
    conn = connect()

    try:
        resolutions = load_resolutions(
            conn,
            markets,
            market_tokens,
        )

        conn.rollback()

    finally:
        conn.close()

    # --------------------------------------------------
    # Row-level edge and liquidity diagnostics.
    # --------------------------------------------------
    diagnostics = []

    for r in rows:
        fair = dec(
            r["consensus_prob"]
        )

        decision_bid = (
            dec(
                r[
                    "decision_best_bid_mc"
                ]
            )
            / Decimal("1000")
        )

        decision_ask = (
            dec(
                r[
                    "decision_best_ask_mc"
                ]
            )
            / Decimal("1000")
        )

        execution_bid = (
            dec(
                r[
                    "execution_best_bid_mc"
                ]
            )
            / Decimal("1000")
        )

        execution_ask = (
            dec(
                r[
                    "execution_best_ask_mc"
                ]
            )
            / Decimal("1000")
        )

        decision_ask_size = dec(
            r[
                "decision_best_ask_size"
            ]
        )

        execution_ask_size = dec(
            r[
                "execution_best_ask_size"
            ]
        )

        fee_rate = dec(
            r["fee_rate"]
        )

        min_order = dec(
            r[
                "minimum_order_size"
            ]
        )

        for name, value in (
            ("fair", fair),
            ("decision_ask", decision_ask),
            ("execution_ask", execution_ask),
        ):
            if value is None:
                raise SystemExit(
                    f"missing {name} "
                    f"at tape line "
                    f"{r['_line']}"
                )

        if not (
            Decimal("0")
            <= fair
            <= Decimal("1")
        ):
            raise SystemExit(
                "fair probability outside [0,1]"
            )

        if not (
            Decimal("0")
            < decision_ask
            < Decimal("1")
        ):
            raise SystemExit(
                "decision ask outside (0,1)"
            )

        if not (
            Decimal("0")
            < execution_ask
            < Decimal("1")
        ):
            raise SystemExit(
                "execution ask outside (0,1)"
            )

        decision_fee = fee_per_share(
            fee_rate,
            decision_ask,
        )

        execution_fee = fee_per_share(
            fee_rate,
            execution_ask,
        )

        decision_edge = (
            fair
            - decision_ask
            - decision_fee
        )

        execution_edge = (
            fair
            - execution_ask
            - execution_fee
        )

        latency_decay = (
            decision_edge
            - execution_edge
        )

        liquidity_valid = (
            execution_ask_size
            is not None
            and
            execution_ask_size
            >= POSITION_SHARES
            and
            min_order
            <= POSITION_SHARES
        )

        market_id = int(
            r[
                "polymarket_market_id"
            ]
        )

        token_id = int(
            r["token_id"]
        )

        resolution = resolutions[
            market_id
        ]

        payout = None
        pnl_per_share = None
        five_share_pnl = None
        won = None

        if resolution[
            "resolution_complete"
        ]:
            won = (
                token_id
                == resolution[
                    "winning_token_id"
                ]
            )

            payout = (
                Decimal("1")
                if won
                else Decimal("0")
            )

            pnl_per_share = (
                payout
                - execution_ask
                - execution_fee
            )

            five_share_pnl = (
                POSITION_SHARES
                * pnl_per_share
            )

        threshold_flags = {
            dstr(t):
                bool(
                    liquidity_valid
                    and execution_edge
                    >= t
                )
            for t in THRESHOLDS
        }

        diagnostics.append({
            "tape_line":
                r["_line"],

            "sharp_consensus_id":
                r[
                    "sharp_consensus_id"
                ],

            "poll_id":
                r["poll_id"],

            "odds_game_id":
                int(
                    r[
                        "odds_game_id"
                    ]
                ),

            "polymarket_market_id":
                market_id,

            "token_id":
                token_id,

            "outcome_team":
                r[
                    "outcome_team"
                ],

            "pm_outcome":
                r[
                    "pm_outcome"
                ],

            "decision_time":
                r[
                    "decision_time"
                ],

            "execution_time":
                r[
                    "execution_time"
                ],

            "fair_probability":
                fair,

            "fee_rate":
                fee_rate,

            "fee_exponent":
                dec(
                    r[
                        "fee_exponent"
                    ]
                ),

            "taker_only":
                r[
                    "taker_only"
                ],

            "minimum_order_size":
                min_order,

            "decision_best_bid":
                decision_bid,

            "decision_best_ask":
                decision_ask,

            "decision_best_ask_size":
                decision_ask_size,

            "execution_best_bid":
                execution_bid,

            "execution_best_ask":
                execution_ask,

            "execution_best_ask_size":
                execution_ask_size,

            "decision_fee_per_share":
                decision_fee,

            "execution_fee_per_share":
                execution_fee,

            "decision_edge":
                decision_edge,

            "execution_edge":
                execution_edge,

            "latency_edge_decay":
                latency_decay,

            "liquidity_valid":
                liquidity_valid,

            "threshold_flags":
                threshold_flags,

            "resolution_complete":
                resolution[
                    "resolution_complete"
                ],

            "resolution_ambiguous":
                resolution[
                    "resolution_ambiguous"
                ],

            "resolution_status":
                resolution[
                    "status"
                ],

            "resolution_capture_time":
                resolution[
                    "capture_time"
                ],

            "resolution_learned_via":
                resolution[
                    "learned_via"
                ],

            "winning_token_id":
                resolution[
                    "winning_token_id"
                ],

            "selected_token_won":
                won,

            "resolution_payout":
                payout,

            "exploratory_pnl_per_share":
                pnl_per_share,

            "exploratory_five_share_pnl":
                five_share_pnl,
        })

    # --------------------------------------------------
    # Structural incidence.
    # --------------------------------------------------
    by_market = defaultdict(list)

    for d in diagnostics:
        by_market[
            d[
                "polymarket_market_id"
            ]
        ].append(d)

    market_incidence = {}

    for threshold in THRESHOLDS:
        qualifying_markets = []

        for market_id, ds in (
            by_market.items()
        ):
            if any(
                d[
                    "liquidity_valid"
                ]
                and
                d[
                    "execution_edge"
                ]
                >= threshold
                for d in ds
            ):
                qualifying_markets.append(
                    market_id
                )

        market_incidence[
            dstr(threshold)
        ] = {
            "market_count":
                len(
                    qualifying_markets
                ),

            "market_ids":
                sorted(
                    qualifying_markets
                ),

            "fraction_of_9":
                (
                    Decimal(
                        len(
                            qualifying_markets
                        )
                    )
                    / Decimal(
                        EXPECTED_GAMES
                    )
                ),
        }

    gate_market_count = (
        market_incidence[
            dstr(
                STRUCTURAL_THRESHOLD
            )
        ][
            "market_count"
        ]
    )

    progression = (
        "PROMISING_FOR_M3B"
        if (
            gate_market_count
            >= STRUCTURAL_MARKETS_REQUIRED
        )
        else
        "NOT_PROMISING_FOR_M3B"
    )

    # --------------------------------------------------
    # Latency diagnostics.
    # --------------------------------------------------
    liquidity_rows = [
        d
        for d in diagnostics
        if d[
            "liquidity_valid"
        ]
    ]

    decision_edges = [
        d["decision_edge"]
        for d in liquidity_rows
    ]

    execution_edges = [
        d["execution_edge"]
        for d in liquidity_rows
    ]

    decay_values = [
        d["latency_edge_decay"]
        for d in liquidity_rows
    ]

    positive_decision = [
        d
        for d in liquidity_rows
        if d[
            "decision_edge"
        ] > 0
    ]

    positive_decision_survives = [
        d
        for d in positive_decision
        if d[
            "execution_edge"
        ] > 0
    ]

    threshold_survival = {}

    for threshold in THRESHOLDS:
        decision_crossings = [
            d
            for d in liquidity_rows
            if d[
                "decision_edge"
            ] >= threshold
        ]

        survived = [
            d
            for d in decision_crossings
            if d[
                "execution_edge"
            ] >= threshold
        ]

        threshold_survival[
            dstr(threshold)
        ] = {
            "decision_crossing_rows":
                len(
                    decision_crossings
                ),

            "surviving_execution_rows":
                len(
                    survived
                ),

            "survival_fraction":
                (
                    Decimal(
                        len(
                            survived
                        )
                    )
                    / Decimal(
                        len(
                            decision_crossings
                        )
                    )
                    if decision_crossings
                    else None
                ),
        }

    # --------------------------------------------------
    # Counterfactual threshold strategies.
    # Earliest signal per market independently.
    # --------------------------------------------------
    threshold_summary = {}

    for threshold in THRESHOLDS:
        selected = []

        for market_id in sorted(
            by_market
        ):
            candidates = [
                d
                for d in (
                    by_market[
                        market_id
                    ]
                )
                if (
                    d[
                        "liquidity_valid"
                    ]
                    and
                    d[
                        "execution_edge"
                    ]
                    >= threshold
                )
            ]

            if not candidates:
                continue

            # First chronological observation.
            first_time = min(
                d[
                    "decision_time"
                ]
                for d in candidates
            )

            same_time = [
                d
                for d in candidates
                if d[
                    "decision_time"
                ]
                == first_time
            ]

            # Larger execution edge first,
            # then lower token_id deterministic tie.
            same_time.sort(
                key=lambda d: (
                    -d[
                        "execution_edge"
                    ],
                    d["token_id"],
                )
            )

            selected.append(
                same_time[0]
            )

        resolved = [
            d
            for d in selected
            if d[
                "resolution_complete"
            ]
        ]

        pnls = [
            d[
                "exploratory_pnl_per_share"
            ]
            for d in resolved
        ]

        five_pnls = [
            d[
                "exploratory_five_share_pnl"
            ]
            for d in resolved
        ]

        wins = [
            d[
                "selected_token_won"
            ]
            for d in resolved
        ]

        predicted_edges = [
            d[
                "execution_edge"
            ]
            for d in selected
        ]

        threshold_summary[
            dstr(threshold)
        ] = {
            "threshold":
                threshold,

            "development_trade_count":
                len(selected),

            "distinct_markets_traded":
                len({
                    d[
                        "polymarket_market_id"
                    ]
                    for d in selected
                }),

            "selected_trades": [
                {
                    "odds_game_id":
                        d[
                            "odds_game_id"
                        ],

                    "polymarket_market_id":
                        d[
                            "polymarket_market_id"
                        ],

                    "token_id":
                        d[
                            "token_id"
                        ],

                    "outcome_team":
                        d[
                            "outcome_team"
                        ],

                    "decision_time":
                        d[
                            "decision_time"
                        ],

                    "execution_time":
                        d[
                            "execution_time"
                        ],

                    "execution_price":
                        d[
                            "execution_best_ask"
                        ],

                    "execution_fee_per_share":
                        d[
                            "execution_fee_per_share"
                        ],

                    "predicted_execution_edge":
                        d[
                            "execution_edge"
                        ],

                    "resolution_complete":
                        d[
                            "resolution_complete"
                        ],

                    "won":
                        d[
                            "selected_token_won"
                        ],

                    "net_pnl_per_share":
                        d[
                            "exploratory_pnl_per_share"
                        ],

                    "five_share_net_pnl":
                        d[
                            "exploratory_five_share_pnl"
                        ],
                }
                for d in selected
            ],

            "resolved_trade_count":
                len(resolved),

            "unresolved_trade_count":
                (
                    len(selected)
                    - len(resolved)
                ),

            "market_equal_mean_net_pnl_per_share":
                (
                    sum(
                        pnls,
                        Decimal("0")
                    )
                    / Decimal(
                        len(pnls)
                    )
                    if pnls
                    else None
                ),

            "median_net_pnl_per_trade":
                (
                    Decimal(
                        str(
                            statistics.median(
                                pnls
                            )
                        )
                    )
                    if pnls
                    else None
                ),

            "total_fixed_five_share_net_pnl":
                (
                    sum(
                        five_pnls,
                        Decimal("0")
                    )
                    if five_pnls
                    else None
                ),

            "win_rate":
                (
                    Decimal(
                        sum(
                            1
                            for x in wins
                            if x
                        )
                    )
                    / Decimal(
                        len(wins)
                    )
                    if wins
                    else None
                ),

            "mean_predicted_execution_edge":
                (
                    sum(
                        predicted_edges,
                        Decimal("0")
                    )
                    / Decimal(
                        len(
                            predicted_edges
                        )
                    )
                    if predicted_edges
                    else None
                ),
        }

    # --------------------------------------------------
    # Resolution coverage of the full 9-market set.
    # --------------------------------------------------
    resolved_markets = [
        mid
        for mid, x in resolutions.items()
        if x[
            "resolution_complete"
        ]
    ]

    ambiguous_markets = [
        mid
        for mid, x in resolutions.items()
        if x[
            "resolution_ambiguous"
        ]
    ]

    unresolved_markets = [
        mid
        for mid, x in resolutions.items()
        if (
            not x[
                "resolution_complete"
            ]
            and
            not x[
                "resolution_ambiguous"
            ]
        )
    ]

    summary = {
        "version":
            "H2-v2-M3a-SUMMARY-1",

        "contract_sha256":
            EXPECTED_HASHES[
                CONTRACT
            ],

        "analyzer_sha256":
            sha256_file(
                Path(__file__)
            ),

        "development_set": {
            "tape_rows":
                len(rows),

            "distinct_games":
                len(games),

            "distinct_markets":
                len(markets),

            "market_ids":
                sorted(
                    markets
                ),

            "formal_calibration_eligible":
                False,

            "formal_oos_eligible":
                False,
        },

        "configuration_gate": {
            "markets_checked":
                len(
                    market_config
                ),

            "passed":
                True,

            "taker_only_required":
                True,

            "position_size_shares":
                POSITION_SHARES,
        },

        "liquidity": {
            "total_rows":
                len(diagnostics),

            "liquidity_valid_rows":
                len(
                    liquidity_rows
                ),

            "liquidity_invalid_rows":
                (
                    len(diagnostics)
                    - len(
                        liquidity_rows
                    )
                ),
        },

        "market_level_execution_edge_incidence":
            market_incidence,

        "latency": {
            "decision_edge_distribution":
                distribution(
                    decision_edges
                ),

            "execution_edge_distribution":
                distribution(
                    execution_edges
                ),

            "edge_decay_distribution":
                distribution(
                    decay_values
                ),

            "positive_decision_edge_rows":
                len(
                    positive_decision
                ),

            "positive_decision_edge_rows_remaining_positive_after_500ms":
                len(
                    positive_decision_survives
                ),

            "positive_decision_edge_survival_fraction":
                (
                    Decimal(
                        len(
                            positive_decision_survives
                        )
                    )
                    / Decimal(
                        len(
                            positive_decision
                        )
                    )
                    if positive_decision
                    else None
                ),

            "threshold_crossing_survival":
                threshold_survival,
        },

        "resolutions": {
            "resolved_markets":
                len(
                    resolved_markets
                ),

            "resolved_market_ids":
                sorted(
                    resolved_markets
                ),

            "ambiguous_markets":
                len(
                    ambiguous_markets
                ),

            "ambiguous_market_ids":
                sorted(
                    ambiguous_markets
                ),

            "unresolved_markets":
                len(
                    unresolved_markets
                ),

            "unresolved_market_ids":
                sorted(
                    unresolved_markets
                ),

            "winner_inference_used":
                False,
        },

        "progression_gate": {
            "name":
                "STRUCTURAL_EXECUTABLE_SIGNAL",

            "threshold":
                STRUCTURAL_THRESHOLD,

            "required_distinct_markets":
                STRUCTURAL_MARKETS_REQUIRED,

            "observed_distinct_markets":
                gate_market_count,

            "uses_resolution":
                False,

            "uses_realized_pnl":
                False,

            "result":
                progression,
        },

        "formal_threshold_selected":
            False,

        "interpretation_boundary": (
            "M3a is exploratory development evidence "
            "only. Its nine markets are permanently "
            "excluded from formal H2-v2 calibration "
            "and confirmatory OOS."
        ),
    }

    # --------------------------------------------------
    # Write only after all frozen/config checks pass.
    # --------------------------------------------------
    ROW_OUT.write_text(
        "".join(
            json.dumps(
                clean(d),
                sort_keys=True,
                separators=(
                    ",",
                    ":",
                ),
            )
            + "\n"
            for d in diagnostics
        ),
        encoding="utf-8",
    )

    THRESHOLD_OUT.write_text(
        json.dumps(
            clean(
                threshold_summary
            ),
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
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
        "=== H2-v2 M3a DEVELOPMENT ECONOMIC RESULT ==="
    )

    print(
        "development games:",
        len(games),
    )

    print(
        "development markets:",
        len(markets),
    )

    print(
        "tape rows:",
        len(rows),
    )

    print(
        "liquidity-valid rows:",
        len(
            liquidity_rows
        ),
    )

    print()
    print(
        "EXECUTABLE EDGE MARKET INCIDENCE"
    )

    for t in THRESHOLDS:
        x = market_incidence[
            dstr(t)
        ]

        print(
            f">= {dstr(t)}:",
            f"{x['market_count']}/"
            f"{EXPECTED_GAMES}",
        )

    print()
    print(
        "LATENCY"
    )

    print(
        "positive decision-edge rows:",
        len(
            positive_decision
        ),
    )

    print(
        "positive after +500ms:",
        len(
            positive_decision_survives
        ),
    )

    print(
        "decision edge median:",
        dstr(
            distribution(
                decision_edges
            )["median"]
        ),
    )

    print(
        "execution edge median:",
        dstr(
            distribution(
                execution_edges
            )["median"]
        ),
    )

    print(
        "execution edge p90:",
        dstr(
            distribution(
                execution_edges
            )["p90"]
        ),
    )

    print(
        "execution edge p95:",
        dstr(
            distribution(
                execution_edges
            )["p95"]
        ),
    )

    print(
        "execution edge max:",
        dstr(
            distribution(
                execution_edges
            )["max"]
        ),
    )

    print()
    print(
        "RESOLUTION COVERAGE"
    )

    print(
        "resolved markets:",
        len(
            resolved_markets
        ),
        "/",
        EXPECTED_GAMES,
    )

    print(
        "ambiguous markets:",
        len(
            ambiguous_markets
        ),
    )

    print(
        "unresolved markets:",
        len(
            unresolved_markets
        ),
    )

    print()
    print(
        "COUNTERFACTUAL THRESHOLD ECONOMICS"
    )

    for t in THRESHOLDS:
        x = threshold_summary[
            dstr(t)
        ]

        print()
        print(
            "threshold:",
            dstr(t),
        )

        print(
            "  trades:",
            x[
                "development_trade_count"
            ],
        )

        print(
            "  resolved:",
            x[
                "resolved_trade_count"
            ],
        )

        print(
            "  mean net/share:",
            dstr(
                x[
                    "market_equal_mean_net_pnl_per_share"
                ]
            ),
        )

        print(
            "  total 5-share PnL:",
            dstr(
                x[
                    "total_fixed_five_share_net_pnl"
                ]
            ),
        )

        print(
            "  win rate:",
            dstr(
                x[
                    "win_rate"
                ]
            ),
        )

        print(
            "  mean predicted edge:",
            dstr(
                x[
                    "mean_predicted_execution_edge"
                ]
            ),
        )

    print()
    print(
        "=== M3a PROGRESSION GATE ==="
    )

    print(
        "required:",
        (
            f">= {STRUCTURAL_MARKETS_REQUIRED} "
            "markets with >= "
            f"{dstr(STRUCTURAL_THRESHOLD)} "
            "liquidity-valid execution edge"
        ),
    )

    print(
        "observed:",
        gate_market_count,
    )

    print(
        "M3a result:",
        progression,
    )

    print()
    print(
        "FORMAL THRESHOLD SELECTED: NO"
    )

    print(
        "DEVELOPMENT MARKETS COUNT "
        "TOWARD CALIBRATION/OOS: NO"
    )

    print()
    print(
        "row_output_sha256:",
        sha256_file(
            ROW_OUT
        ),
    )

    print(
        "threshold_output_sha256:",
        sha256_file(
            THRESHOLD_OUT
        ),
    )

    print(
        "summary_sha256:",
        sha256_file(
            SUMMARY_OUT
        ),
    )


if __name__ == "__main__":
    main()
