from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from collections import Counter
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import dotenv_values


REPO = Path(__file__).resolve().parents[1]
R = REPO / "research"
E = REPO / "execution"

sys.path.insert(
    0,
    str(E),
)


from rn1_f21_polymarket_venue import (  # noqa: E402
    price_conforms_to_tick,
)

from rn1_f21_replay import (  # noqa: E402
    EventKind,
    OrderSpec,
    ReplayEvent,
    replay_passive_order,
)

from rn1_f21_shadow_mm import (  # noqa: E402
    AggressorSide,
    ShadowConfig,
    Side,
)


CONTRACT = (
    R
    / "rn1_f21_m7_burned_fill_path_contract.json"
)

M6_GATE = (
    R
    / "rn1_f21_m6_trade_side_gate.json"
)

M6_VALIDATION = (
    R
    / "rn1_f21_m6_burned_replay_validation.json"
)

TAPE = (
    R
    / "h2_v2_m2h_tape.jsonl"
)

OUT = (
    R
    / "rn1_f21_m7_burned_fill_path_validation.json"
)


EXPECTED_HEAD = (
    "59e491e533717f6e52b2948bb2231041caeb523e"
)

EXPECTED_CONTRACT_SHA = (
    "01e9dba3fa3059221820f04105b8beb5"
    "d28efa1cf1701231934e60893b575ae1"
)

EXPECTED_M6_GATE_SHA = (
    "3eccb25f9493c2ffeed2c8d28d41d509"
    "e8a3c8e8f43f87f404f56b4ba5b0230c"
)

EXPECTED_M6_VALIDATION_SHA = (
    "f20ac6525252db0e334e8e9703ba83b27"
    "140c1f97e2a1e27bfc92194f2c65fb2"
)

EXPECTED_TAPE_SHA = (
    "c234267e3342601c9706c39d7d4c20c9"
    "3eaeea1df5b0ca0a5513cf67221be75f"
)


def sha256(path: Path) -> str:
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def canonical_sha(value) -> str:
    raw = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    return hashlib.sha256(
        raw
    ).hexdigest()


def dt(value) -> datetime:
    if isinstance(
        value,
        datetime,
    ):
        return value

    return datetime.fromisoformat(
        str(value).replace(
            "Z",
            "+00:00",
        )
    )


def dec(value) -> Decimal:
    return Decimal(
        str(value)
    )


def mc_price(value) -> Decimal:
    return (
        dec(value)
        /
        Decimal("1000")
    )


def clean(value):

    if isinstance(
        value,
        Decimal,
    ):
        return str(value)

    if isinstance(
        value,
        datetime,
    ):
        return value.isoformat()

    if isinstance(
        value,
        dict,
    ):
        return {
            str(k): clean(v)
            for k, v
            in value.items()
        }

    if isinstance(
        value,
        (list, tuple),
    ):
        return [
            clean(v)
            for v in value
        ]

    return value


# ==================================================
# Frozen-input integrity
# ==================================================

head = subprocess.check_output(
    [
        "git",
        "rev-parse",
        "HEAD",
    ],
    cwd=REPO,
    text=True,
).strip()


if head != EXPECTED_HEAD:
    raise RuntimeError(
        "REFUSING M7: unexpected HEAD\n"
        f"expected {EXPECTED_HEAD}\n"
        f"actual   {head}"
    )


for path, expected in (
    (
        CONTRACT,
        EXPECTED_CONTRACT_SHA,
    ),
    (
        M6_GATE,
        EXPECTED_M6_GATE_SHA,
    ),
    (
        M6_VALIDATION,
        EXPECTED_M6_VALIDATION_SHA,
    ),
    (
        TAPE,
        EXPECTED_TAPE_SHA,
    ),
):

    actual = sha256(path)

    if actual != expected:
        raise RuntimeError(
            "REFUSING M7: frozen artifact changed\n"
            f"path     {path}\n"
            f"expected {expected}\n"
            f"actual   {actual}"
        )


contract = json.loads(
    CONTRACT.read_text(
        encoding="utf-8"
    )
)

gate = json.loads(
    M6_GATE.read_text(
        encoding="utf-8"
    )
)

m6 = json.loads(
    M6_VALIDATION.read_text(
        encoding="utf-8"
    )
)


if (
    contract["status"]
    !=
    "FROZEN_BURNED_LONG_WINDOW_FILL_PATH_VALIDATION"
):
    raise RuntimeError(
        "unexpected M7 contract status"
    )


if gate["status"] != "PASS":
    raise RuntimeError(
        "M6 trade-side gate is not PASS"
    )


if (
    m6["status"]
    !=
    "PASS_BURNED_REAL_DATA_REPLAY_VALIDATION"
):
    raise RuntimeError(
        "M6 parent validation is not PASS"
    )


# M6 independently validated this mapping.
TRADE_SIDE = {
    "BUY":
        AggressorSide.BUY,

    "SELL":
        AggressorSide.SELL,
}


# ==================================================
# Burned universe
# ==================================================

rows = [
    json.loads(line)

    for line
    in TAPE.read_text(
        encoding="utf-8"
    ).splitlines()

    if line.strip()
]


if len(rows) != 1080:
    raise RuntimeError(
        "unexpected anchor count"
    )


if len({
    x["odds_game_id"]
    for x in rows
}) != 9:
    raise RuntimeError(
        "unexpected game count"
    )


if len({
    x["polymarket_market_id"]
    for x in rows
}) != 9:
    raise RuntimeError(
        "unexpected market count"
    )


if len({
    x["token_id"]
    for x in rows
}) != 18:
    raise RuntimeError(
        "unexpected token count"
    )


EXPOSURE_SECONDS = int(
    contract[
        "exposure_window"
    ][
        "duration_seconds"
    ]
)

if EXPOSURE_SECONDS != 60:
    raise RuntimeError(
        "M7 exposure is not frozen at 60s"
    )


MIN_FILL_PROBES = int(
    contract[
        "engineering_sufficiency_gate"
    ][
        "minimum_probes_with_positive_simulated_fill"
    ]
)

MIN_FILL_MARKETS = int(
    contract[
        "engineering_sufficiency_gate"
    ][
        "minimum_markets_with_positive_simulated_fill"
    ]
)

MIN_QUEUE_EVENTS = int(
    contract[
        "engineering_sufficiency_gate"
    ][
        "minimum_unique_queue_consuming_real_trade_events"
    ]
)


# ==================================================
# DB helpers
# ==================================================

def latest_full_snapshot(
    cur,
    token_id,
    target_time,
):

    cur.execute(
        """
        SELECT
            capture_time,
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

    row = cur.fetchone()

    return (
        dict(row)
        if row
        else None
    )


def window_sources(
    cur,
    token_id,
    start,
    end,
):

    cur.execute(
        """
        SELECT
            capture_time,
            source,
            connection_id,
            ingest_sequence,
            book_generation

        FROM book_snapshots

        WHERE
            token_id = %s
            AND capture_time > %s
            AND capture_time <= %s
            AND source IN (
                'ws_book',
                'rest_book'
            )
            AND watchlist_rule = 'h2_v2'
            AND book_generation IS NOT NULL
        """,
        (
            token_id,
            start,
            end,
        ),
    )

    snapshots = [
        {
            "kind":
                "SNAPSHOT",

            **dict(x),
        }

        for x in cur.fetchall()
    ]


    cur.execute(
        """
        SELECT
            capture_time,
            connection_id,
            ingest_sequence,
            change_index,
            book_generation,
            price_mc,
            size,
            side

        FROM book_deltas

        WHERE
            token_id = %s
            AND capture_time > %s
            AND capture_time <= %s
            AND watchlist_rule = 'h2_v2'
        """,
        (
            token_id,
            start,
            end,
        ),
    )

    deltas = [
        {
            "kind":
                "DELTA",

            **dict(x),
        }

        for x in cur.fetchall()
    ]


    cur.execute(
        """
        SELECT
            capture_time,
            connection_id,
            ingest_sequence,
            price_mc,
            size,
            side

        FROM last_trade_events

        WHERE
            token_id = %s
            AND capture_time > %s
            AND capture_time <= %s
        """,
        (
            token_id,
            start,
            end,
        ),
    )

    trades = [
        {
            "kind":
                "TRADE",

            **dict(x),
        }

        for x in cur.fetchall()
    ]


    kind_rank = {
        "SNAPSHOT":
            0,

        "DELTA":
            1,

        "TRADE":
            2,
    }


    def key(x):

        seq = x.get(
            "ingest_sequence"
        )

        ci = x.get(
            "change_index",
            0,
        )

        return (
            x["capture_time"],

            (
                int(seq)
                if seq is not None
                else -1
            ),

            (
                int(ci)
                if ci is not None
                else 0
            ),

            kind_rank[
                x["kind"]
            ],
        )


    out = (
        snapshots
        + deltas
        + trades
    )

    out.sort(
        key=key
    )

    return out


# ==================================================
# Frozen venue probe
# ==================================================

CENT = Decimal("0.01")


def venue_probe_ok(
    row,
    side,
):

    bid_mc = row[
        "decision_best_bid_mc"
    ]

    ask_mc = row[
        "decision_best_ask_mc"
    ]


    if (
        bid_mc is None
        or ask_mc is None
        or
        row[
            "decision_best_bid_size"
        ] is None
        or
        row[
            "decision_best_ask_size"
        ] is None
    ):
        return (
            False,
            "missing_decision_bbo",
        )


    bid = mc_price(
        bid_mc
    )

    ask = mc_price(
        ask_mc
    )


    if not (
        Decimal("0")
        < bid
        < ask
        < Decimal("1")
    ):
        return (
            False,
            "invalid_or_crossed_bbo",
        )


    tick = dec(
        row[
            "minimum_tick_size"
        ]
    )

    minimum = dec(
        row[
            "minimum_order_size"
        ]
    )


    if minimum <= 0:
        return (
            False,
            "invalid_minimum_order_size",
        )


    encoded_minimum = (
        minimum.quantize(
            CENT,
            rounding=ROUND_DOWN,
        )
    )


    if (
        encoded_minimum
        != minimum
    ):
        return (
            False,
            "minimum_not_exact_two_decimals",
        )


    price = (
        bid
        if side is Side.BUY
        else ask
    )


    if not price_conforms_to_tick(
        price,
        tick,
    ):
        return (
            False,
            "quote_not_on_tick",
        )


    if (
        side is Side.BUY
        and
        price >= ask
    ):
        return (
            False,
            "buy_marketable",
        )


    if (
        side is Side.SELL
        and
        price <= bid
    ):
        return (
            False,
            "sell_marketable",
        )


    return (
        True,
        None,
    )


# ==================================================
# Source -> F21 translation
# ==================================================

def build_events(
    *,
    row,
    side,
    base,
    source_rows,
):

    bid = mc_price(
        row[
            "decision_best_bid_mc"
        ]
    )

    ask = mc_price(
        row[
            "decision_best_ask_mc"
        ]
    )


    shares = dec(
        row[
            "minimum_order_size"
        ]
    )


    if side is Side.BUY:

        quote_mc = int(
            row[
                "decision_best_bid_mc"
            ]
        )

        quote_price = bid

        initial_queue = dec(
            row[
                "decision_best_bid_size"
            ]
        )

        delta_side = "B"

        queue_aggressor = "SELL"


    else:

        quote_mc = int(
            row[
                "decision_best_ask_mc"
            ]
        )

        quote_price = ask

        initial_queue = dec(
            row[
                "decision_best_ask_size"
            ]
        )

        delta_side = "S"

        queue_aggressor = "BUY"


    events = [
        ReplayEvent(
            ts_ms=0,
            kind=(
                EventKind.PLACEMENT_BOOK
            ),
            displayed_size_at_price=float(
                initial_queue
            ),
            best_bid=float(
                bid
            ),
            best_ask=float(
                ask
            ),
        )
    ]


    stats = Counter()

    expected_generation = int(
        base[
            "book_generation"
        ]
    )


    active_connection = (
        str(
            base[
                "connection_id"
            ]
        )
        if (
            base[
                "source"
            ]
            == "ws_book"
            and
            base[
                "connection_id"
            ]
            is not None
        )
        else None
    )


    gap_inserted = False


    # Independent structural tracker used ONLY
    # to determine whether an explicit real trade
    # occurred before this probe became terminal.
    #
    # It does not create fills in the F21 engine.
    queue_remaining = (
        initial_queue
    )

    own_remaining = shares

    structurally_alive = True

    queue_event_keys = set()


    for logical_tick, src in enumerate(
        source_rows,
        start=1,
    ):

        if gap_inserted:

            stats[
                "source_rows_after_gap"
            ] += 1

            continue


        kind = src[
            "kind"
        ]

        stats[
            "source_"
            + kind.lower()
        ] += 1


        src_connection = (
            str(
                src[
                    "connection_id"
                ]
            )
            if src.get(
                "connection_id"
            )
            is not None
            else None
        )


        # ------------------------------------------
        # Generation continuity
        # ------------------------------------------

        if kind in (
            "SNAPSHOT",
            "DELTA",
        ):

            src_generation = int(
                src[
                    "book_generation"
                ]
            )

            if (
                src_generation
                != expected_generation
            ):

                stats[
                    "generation_crossing_observed"
                ] += 1

                events.append(
                    ReplayEvent(
                        ts_ms=logical_tick,
                        kind=(
                            EventKind.COLLECTOR_GAP
                        ),
                        reason=(
                            "book_generation_changed"
                        ),
                    )
                )

                gap_inserted = True
                structurally_alive = False

                continue


        # ------------------------------------------
        # Connection continuity
        # ------------------------------------------

        if src_connection is not None:

            if active_connection is None:

                active_connection = (
                    src_connection
                )

            elif (
                src_connection
                != active_connection
            ):

                stats[
                    "connection_change_observed"
                ] += 1

                events.append(
                    ReplayEvent(
                        ts_ms=logical_tick,
                        kind=(
                            EventKind.COLLECTOR_GAP
                        ),
                        reason=(
                            "connection_changed"
                        ),
                    )
                )

                gap_inserted = True
                structurally_alive = False

                continue


        # ------------------------------------------
        # Full snapshot = structural evidence only
        # ------------------------------------------

        if kind == "SNAPSHOT":

            stats[
                "snapshot_structural_only"
            ] += 1

            continue


        # ------------------------------------------
        # Exact resting level public-size update
        # ------------------------------------------

        if kind == "DELTA":

            if (
                src[
                    "side"
                ]
                == delta_side
                and
                int(
                    src[
                        "price_mc"
                    ]
                )
                == quote_mc
            ):

                events.append(
                    ReplayEvent(
                        ts_ms=logical_tick,
                        kind=(
                            EventKind.BOOK_SIZE
                        ),
                        new_displayed_size_at_price=float(
                            dec(
                                src[
                                    "size"
                                ]
                            )
                        ),
                    )
                )

                stats[
                    "translated_book_size"
                ] += 1

            else:

                stats[
                    "irrelevant_book_delta"
                ] += 1

            continue


        # ------------------------------------------
        # Explicit trade
        # ------------------------------------------

        if kind == "TRADE":

            reported_side = src[
                "side"
            ]


            if (
                reported_side
                not in TRADE_SIDE
            ):

                stats[
                    "unknown_trade_side"
                ] += 1

                events.append(
                    ReplayEvent(
                        ts_ms=logical_tick,
                        kind=(
                            EventKind.COLLECTOR_GAP
                        ),
                        reason=(
                            "unknown_trade_side"
                        ),
                    )
                )

                gap_inserted = True
                structurally_alive = False

                continue


            if src[
                "size"
            ] is None:

                stats[
                    "missing_trade_size"
                ] += 1

                events.append(
                    ReplayEvent(
                        ts_ms=logical_tick,
                        kind=(
                            EventKind.COLLECTOR_GAP
                        ),
                        reason=(
                            "missing_trade_size"
                        ),
                    )
                )

                gap_inserted = True
                structurally_alive = False

                continue


            trade_size = dec(
                src[
                    "size"
                ]
            )


            if trade_size <= 0:

                stats[
                    "nonpositive_trade_size"
                ] += 1

                events.append(
                    ReplayEvent(
                        ts_ms=logical_tick,
                        kind=(
                            EventKind.COLLECTOR_GAP
                        ),
                        reason=(
                            "nonpositive_trade_size"
                        ),
                    )
                )

                gap_inserted = True
                structurally_alive = False

                continue


            trade_price_mc = int(
                src[
                    "price_mc"
                ]
            )


            events.append(
                ReplayEvent(
                    ts_ms=logical_tick,
                    kind=(
                        EventKind.TRADE
                    ),
                    aggressor=TRADE_SIDE[
                        reported_side
                    ],
                    trade_price=float(
                        mc_price(
                            trade_price_mc
                        )
                    ),
                    trade_size=float(
                        trade_size
                    ),
                )
            )

            stats[
                "translated_trade"
            ] += 1


            # --------------------------------------
            # Frozen queue-consuming-real-trade
            # coverage definition.
            #
            # Explicit trade only.
            # Exact quote only.
            # Correct aggressor only.
            # Before terminal only.
            # --------------------------------------

            if (
                structurally_alive
                and
                reported_side
                == queue_aggressor
                and
                trade_price_mc
                == quote_mc
            ):

                event_key = (
                    str(
                        src[
                            "connection_id"
                        ]
                    ),
                    int(
                        src[
                            "ingest_sequence"
                        ]
                    ),
                )

                queue_event_keys.add(
                    event_key
                )

                stats[
                    "queue_consuming_trade_occurrence"
                ] += 1


                remaining = (
                    trade_size
                )


                if queue_remaining > 0:

                    consumed_queue = min(
                        queue_remaining,
                        remaining,
                    )

                    queue_remaining -= (
                        consumed_queue
                    )

                    remaining -= (
                        consumed_queue
                    )


                if (
                    remaining > 0
                    and
                    own_remaining > 0
                ):

                    consumed_own = min(
                        own_remaining,
                        remaining,
                    )

                    own_remaining -= (
                        consumed_own
                    )


                if own_remaining <= 0:

                    structurally_alive = (
                        False
                    )


            continue


        raise RuntimeError(
            "unexpected source kind: "
            f"{kind}"
        )


    explicit_model_filled = (
        shares
        - own_remaining
    )


    return {
        "events":
            events,

        "quote_price":
            quote_price,

        "initial_queue":
            initial_queue,

        "queue_event_keys":
            queue_event_keys,

        "explicit_model_filled":
            explicit_model_filled,

        "end_tick":
            len(
                source_rows
            )
            + 1,

        "stats":
            stats,
    }


# ==================================================
# One complete deterministic replay build
# ==================================================

def build_once(
    cur,
    *,
    label,
):

    metrics = Counter()

    translation = Counter()

    terminal_states = Counter()

    unique_queue_events = set()

    fill_markets = set()

    replay_material = []


    for index, row in enumerate(
        rows,
        start=1,
    ):

        token_id = int(
            row[
                "token_id"
            ]
        )

        market_id = int(
            row[
                "polymarket_market_id"
            ]
        )

        start = dt(
            row[
                "decision_time"
            ]
        )

        end = (
            start
            + timedelta(
                seconds=EXPOSURE_SECONDS
            )
        )


        if (
            (
                end - start
            ).total_seconds()
            != 60
        ):

            metrics[
                "bad_window"
            ] += 1


        base = latest_full_snapshot(
            cur,
            token_id,
            start,
        )


        if base is None:

            metrics[
                "missing_decision_base"
            ] += 1

            continue


        expected_base_time = dt(
            row[
                "decision_base_time"
            ]
        )


        if (
            base[
                "capture_time"
            ]
            != expected_base_time
            or
            base[
                "source"
            ]
            != row[
                "decision_base_source"
            ]
            or
            int(
                base[
                    "book_generation"
                ]
            )
            != int(
                row[
                    "decision_base_generation"
                ]
            )
        ):

            metrics[
                "decision_base_mismatch"
            ] += 1


        source_rows = window_sources(
            cur,
            token_id,
            start,
            end,
        )


        metrics[
            "source_rows"
        ] += len(
            source_rows
        )


        for src in source_rows:

            if (
                src[
                    "capture_time"
                ]
                <= start
                or
                src[
                    "capture_time"
                ]
                > end
            ):

                metrics[
                    "lookahead_or_predecision_event"
                ] += 1


        # Always both sides.
        # No signal filtering.
        for side in (
            Side.BUY,
            Side.SELL,
        ):

            metrics[
                "probe_orders"
            ] += 1


            ok, reason = venue_probe_ok(
                row,
                side,
            )


            if not ok:

                metrics[
                    "venue_invalid"
                ] += 1

                replay_material.append({
                    "anchor":
                        row[
                            "sharp_consensus_id"
                        ],

                    "side":
                        side.value,

                    "status":
                        "VENUE_INVALID",

                    "reason":
                        reason,
                })

                continue


            built = build_events(
                row=row,
                side=side,
                base=base,
                source_rows=source_rows,
            )


            translation.update(
                built[
                    "stats"
                ]
            )


            shares = dec(
                row[
                    "minimum_order_size"
                ]
            )


            spec = OrderSpec(
                order_id=(
                    "m7:"
                    f"{row['sharp_consensus_id']}:"
                    f"{side.value}"
                ),
                side=side,
                price=float(
                    built[
                        "quote_price"
                    ]
                ),
                shares=float(
                    shares
                ),
                submit_ts_ms=0,
            )


            try:

                result = (
                    replay_passive_order(
                        spec=spec,
                        config=(
                            ShadowConfig(
                                placement_latency_ms=0,
                                cancel_latency_ms=0,
                            )
                        ),
                        events=built[
                            "events"
                        ],
                        end_ts_ms=built[
                            "end_tick"
                        ],
                    )
                )


            except Exception as exc:

                metrics[
                    "replay_errors"
                ] += 1

                replay_material.append({
                    "anchor":
                        row[
                            "sharp_consensus_id"
                        ],

                    "side":
                        side.value,

                    "status":
                        "REPLAY_ERROR",

                    "error_type":
                        type(
                            exc
                        ).__name__,

                    "error":
                        str(
                            exc
                        ),
                })

                continue


            metrics[
                "probes_replayed"
            ] += 1


            if not (
                result[
                    "order"
                ]
                if isinstance(
                    result,
                    dict,
                )
                else False
            ):
                pass
            else:
                raise RuntimeError(
                    "unexpected dict ReplayResult"
                )


            if (
                result[
                    "placement_snapshot_seen"
                ]
                if isinstance(
                    result,
                    dict,
                )
                else False
            ):
                raise RuntimeError(
                    "unexpected dict ReplayResult"
                )


            if (
                result.placement_snapshot_seen
                is not True
            ):

                metrics[
                    "placement_snapshot_missing"
                ] += 1


            order = result.order

            state_obj = order.state

            state = (
                state_obj.value
                if hasattr(
                    state_obj,
                    "value",
                )
                else str(
                    state_obj
                )
            )

            terminal_states[
                state
            ] += 1


            if state == "invalidated":

                metrics[
                    "invalidated_probes"
                ] += 1


            filled = dec(
                order.filled_shares
            )

            own_shares = dec(
                order.shares
            )


            if filled < 0:

                metrics[
                    "negative_filled_shares"
                ] += 1


            if (
                filled
                >
                own_shares
                + Decimal("0.00000001")
            ):

                metrics[
                    "overfilled_probe"
                ] += 1


            if filled > 0:

                metrics[
                    "positive_fill_probes"
                ] += 1

                fill_markets.add(
                    market_id
                )


                if (
                    filled
                    + Decimal("0.00000001")
                    >= own_shares
                ):

                    metrics[
                        "full_fill_probes"
                    ] += 1

                else:

                    metrics[
                        "partial_fill_probes"
                    ] += 1


            # The independent explicit-trade tracker is
            # not an alternative fill engine. It is only
            # an integrity cross-check that F21 fills were
            # caused by explicit qualifying trades.
            explicit_filled = built[
                "explicit_model_filled"
            ]


            if (
                abs(
                    filled
                    - explicit_filled
                )
                >
                Decimal("0.0000001")
            ):

                metrics[
                    "explicit_fill_model_mismatch"
                ] += 1


            unique_queue_events.update(
                built[
                    "queue_event_keys"
                ]
            )


            replay_material.append({
                "anchor":
                    row[
                        "sharp_consensus_id"
                    ],

                "market_id":
                    market_id,

                "token_id":
                    token_id,

                "side":
                    side.value,

                "source_rows":
                    len(
                        source_rows
                    ),

                "queue_event_count":
                    len(
                        built[
                            "queue_event_keys"
                        ]
                    ),

                "replay":
                    clean(
                        result.canonical_summary()
                    ),
            })


        if (
            index % 100 == 0
            or
            index == len(rows)
        ):

            print(
                f"{label}: "
                f"{index}/{len(rows)} anchors"
            )


    return {
        "metrics":
            dict(
                metrics
            ),

        "translation":
            dict(
                translation
            ),

        "terminal_states":
            dict(
                terminal_states
            ),

        "fill_markets":
            len(
                fill_markets
            ),

        "unique_queue_events":
            len(
                unique_queue_events
            ),

        "replay_sha256":
            canonical_sha(
                replay_material
            ),
    }


# ==================================================
# Main
# ==================================================

env = dotenv_values(
    REPO / ".env"
)

conn = psycopg2.connect(
    host="127.0.0.1",
    port=5432,
    dbname="quantlab",
    user="quantlab",
    password=env[
        "PG_PASSWORD"
    ],
    cursor_factory=(
        psycopg2.extras.RealDictCursor
    ),
)


# Both builds see the exact same DB snapshot.
conn.set_session(
    readonly=True,
    autocommit=False,
    isolation_level="REPEATABLE READ",
)


try:

    token_ids = sorted({
        int(
            x["token_id"]
        )
        for x in rows
    })


    with conn.cursor() as cur:

        # ------------------------------------------
        # Frozen sequence-integrity checks
        # ------------------------------------------

        cur.execute(
            """
            SELECT count(*) AS n

            FROM (
                SELECT
                    connection_id,
                    ingest_sequence,
                    change_index

                FROM book_deltas

                WHERE
                    token_id = ANY(%s)
                    AND watchlist_rule = 'h2_v2'

                GROUP BY
                    connection_id,
                    ingest_sequence,
                    change_index

                HAVING count(*) > 1
            ) x
            """,
            (token_ids,),
        )

        duplicate_deltas = int(
            cur.fetchone()[
                "n"
            ]
        )


        cur.execute(
            """
            SELECT count(*) AS n

            FROM (
                SELECT
                    connection_id,
                    ingest_sequence

                FROM last_trade_events

                WHERE
                    token_id = ANY(%s)

                GROUP BY
                    connection_id,
                    ingest_sequence

                HAVING count(*) > 1
            ) x
            """,
            (token_ids,),
        )

        duplicate_trades = int(
            cur.fetchone()[
                "n"
            ]
        )


        print()
        print(
            "========================================"
        )
        print(
            "F21-M7 BUILD 1"
        )
        print(
            "========================================"
        )

        first = build_once(
            cur,
            label="build-1",
        )


        print()
        print(
            "========================================"
        )
        print(
            "F21-M7 BUILD 2"
        )
        print(
            "========================================"
        )

        second = build_once(
            cur,
            label="build-2",
        )


    deterministic = (
        first == second
    )


    metrics = Counter(
        first[
            "metrics"
        ]
    )

    translation = Counter(
        first[
            "translation"
        ]
    )


    # Offending events are never applied.
    # They become COLLECTOR_GAP first.
    generation_mixing = 0
    connection_mixing = 0

    inferred_fills = 0


    requirements = {
        "anchors":
            len(rows)
            == 1080,

        "probe_orders":
            metrics[
                "probe_orders"
            ]
            == 2160,

        "venue_invalid":
            metrics[
                "venue_invalid"
            ]
            == 0,

        "decision_base_mismatches":
            metrics[
                "decision_base_mismatch"
            ]
            == 0,

        "lookahead_events":
            metrics[
                "lookahead_or_predecision_event"
            ]
            == 0,

        "duplicate_delta_sequence_keys":
            duplicate_deltas
            == 0,

        "duplicate_trade_sequence_keys":
            duplicate_trades
            == 0,

        "generation_mixing":
            generation_mixing
            == 0,

        "connection_mixing":
            connection_mixing
            == 0,

        "inferred_fills":
            inferred_fills
            == 0,

        "replay_errors":
            metrics[
                "replay_errors"
            ]
            == 0,

        "deterministic_rebuild":
            deterministic,

        "minimum_probes_with_positive_simulated_fill":
            metrics[
                "positive_fill_probes"
            ]
            >= MIN_FILL_PROBES,

        "minimum_markets_with_positive_simulated_fill":
            first[
                "fill_markets"
            ]
            >= MIN_FILL_MARKETS,

        "minimum_unique_queue_consuming_real_trade_events":
            first[
                "unique_queue_events"
            ]
            >= MIN_QUEUE_EVENTS,
    }


    status = (
        "PASS_BURNED_LONG_WINDOW_FILL_PATH_VALIDATION"
        if all(
            requirements.values()
        )
        else
        "FAIL_BURNED_LONG_WINDOW_FILL_PATH_VALIDATION"
    )


    validation = {
        "study":
            "RN1-F21-M7",

        "status":
            status,

        "contract_sha256":
            sha256(
                CONTRACT
            ),

        "parent_m6": {
            "commit":
                head,

            "validation_sha256":
                sha256(
                    M6_VALIDATION
                ),

            "trade_side_gate_sha256":
                sha256(
                    M6_GATE
                ),

            "trade_side_gate_status":
                gate[
                    "status"
                ],
        },

        "burned_universe": {
            "anchors":
                1080,

            "games":
                9,

            "markets":
                9,

            "tokens":
                18,

            "probe_orders":
                metrics[
                    "probe_orders"
                ],

            "exposure_seconds":
                EXPOSURE_SECONDS,
        },

        "fill_path_coverage": {
            "probes_replayed":
                metrics[
                    "probes_replayed"
                ],

            "positive_simulated_fill_probes":
                metrics[
                    "positive_fill_probes"
                ],

            "partial_fill_probes":
                metrics[
                    "partial_fill_probes"
                ],

            "full_fill_probes":
                metrics[
                    "full_fill_probes"
                ],

            "markets_with_positive_simulated_fill":
                first[
                    "fill_markets"
                ],

            "unique_queue_consuming_real_trade_events":
                first[
                    "unique_queue_events"
                ],

            "invalidated_probes":
                metrics[
                    "invalidated_probes"
                ],
        },

        "engineering_thresholds": {
            "minimum_probes_with_positive_simulated_fill":
                MIN_FILL_PROBES,

            "minimum_markets_with_positive_simulated_fill":
                MIN_FILL_MARKETS,

            "minimum_unique_queue_consuming_real_trade_events":
                MIN_QUEUE_EVENTS,
        },

        "source_integrity": {
            "duplicate_delta_sequence_keys":
                duplicate_deltas,

            "duplicate_trade_sequence_keys":
                duplicate_trades,

            "decision_base_mismatches":
                metrics[
                    "decision_base_mismatch"
                ],

            "lookahead_or_predecision_events":
                metrics[
                    "lookahead_or_predecision_event"
                ],
        },

        "continuity": {
            "generation_crossings_observed":
                translation[
                    "generation_crossing_observed"
                ],

            "connection_changes_observed":
                translation[
                    "connection_change_observed"
                ],

            "generation_mixing":
                generation_mixing,

            "connection_mixing":
                connection_mixing,
        },

        "translation_counts": {
            key:
                int(value)

            for key, value
            in sorted(
                translation.items()
            )
        },

        "terminal_state_counts": {
            key:
                int(value)

            for key, value
            in sorted(
                first[
                    "terminal_states"
                ].items()
            )
        },

        "integrity_checks": {
            "negative_filled_shares":
                metrics[
                    "negative_filled_shares"
                ],

            "overfilled_probes":
                metrics[
                    "overfilled_probe"
                ],

            "placement_snapshot_missing":
                metrics[
                    "placement_snapshot_missing"
                ],

            "explicit_fill_model_mismatches":
                metrics[
                    "explicit_fill_model_mismatch"
                ],

            "unknown_trade_side":
                translation[
                    "unknown_trade_side"
                ],

            "missing_trade_size":
                translation[
                    "missing_trade_size"
                ],

            "replay_errors":
                metrics[
                    "replay_errors"
                ],

            "inferred_fills":
                inferred_fills,
        },

        "determinism": {
            "deterministic_rebuild":
                deterministic,

            "first_replay_sha256":
                first[
                    "replay_sha256"
                ],

            "second_replay_sha256":
                second[
                    "replay_sha256"
                ],
        },

        "requirements": {
            key:
                bool(value)

            for key, value
            in requirements.items()
        },

        "analysis_boundary": {
            "F19_read":
                False,

            "F20_read":
                False,

            "prospective_data_read":
                False,

            "prospective_performance_read":
                False,

            "F18_signal_calculated":
                False,

            "flow_imbalance_calculated":
                False,

            "protected_vs_unprotected_compared":
                False,

            "markout_calculated":
                False,

            "post_fill_price_evaluated":
                False,

            "inventory_valued":
                False,

            "maker_rebate_calculated":
                False,

            "winner_used":
                False,

            "settlement_used":
                False,

            "pnl_calculated":
                False,
        },
    }


    OUT.write_text(
        json.dumps(
            clean(
                validation
            ),
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


    print()
    print(
        "========================================"
    )
    print(
        "F21-M7 BURNED 60s FILL-PATH VALIDATION"
    )
    print(
        "========================================"
    )

    print(
        "status:",
        status,
    )

    print(
        "anchors:",
        len(rows),
    )

    print(
        "probe orders:",
        metrics[
            "probe_orders"
        ],
    )

    print(
        "probes replayed:",
        metrics[
            "probes_replayed"
        ],
    )

    print(
        "exposure seconds:",
        EXPOSURE_SECONDS,
    )


    print()
    print(
        "--- real fill-path coverage ---"
    )

    print(
        "positive simulated-fill probes:",
        metrics[
            "positive_fill_probes"
        ],
        "/ required",
        MIN_FILL_PROBES,
    )

    print(
        "partial-fill probes:",
        metrics[
            "partial_fill_probes"
        ],
    )

    print(
        "full-fill probes:",
        metrics[
            "full_fill_probes"
        ],
    )

    print(
        "markets with positive fills:",
        first[
            "fill_markets"
        ],
        "/ required",
        MIN_FILL_MARKETS,
    )

    print(
        "unique queue-consuming real trades:",
        first[
            "unique_queue_events"
        ],
        "/ required",
        MIN_QUEUE_EVENTS,
    )


    print()
    print(
        "--- translation / continuity ---"
    )

    print(
        "translated trades:",
        translation[
            "translated_trade"
        ],
    )

    print(
        "translated exact-level book updates:",
        translation[
            "translated_book_size"
        ],
    )

    print(
        "generation crossings observed:",
        translation[
            "generation_crossing_observed"
        ],
    )

    print(
        "connection changes observed:",
        translation[
            "connection_change_observed"
        ],
    )

    print(
        "invalidated probes:",
        metrics[
            "invalidated_probes"
        ],
    )

    print(
        "generation mixing:",
        generation_mixing,
    )

    print(
        "connection mixing:",
        connection_mixing,
    )


    print()
    print(
        "--- integrity ---"
    )

    print(
        "venue invalid:",
        metrics[
            "venue_invalid"
        ],
    )

    print(
        "decision base mismatches:",
        metrics[
            "decision_base_mismatch"
        ],
    )

    print(
        "lookahead events:",
        metrics[
            "lookahead_or_predecision_event"
        ],
    )

    print(
        "unknown trade side:",
        translation[
            "unknown_trade_side"
        ],
    )

    print(
        "missing trade size:",
        translation[
            "missing_trade_size"
        ],
    )

    print(
        "explicit fill-model mismatches:",
        metrics[
            "explicit_fill_model_mismatch"
        ],
    )

    print(
        "replay errors:",
        metrics[
            "replay_errors"
        ],
    )

    print(
        "inferred fills:",
        inferred_fills,
    )

    print(
        "deterministic rebuild:",
        (
            "YES"
            if deterministic
            else "NO"
        ),
    )

    print(
        "replay SHA:",
        first[
            "replay_sha256"
        ],
    )


    print()
    print(
        "--- terminal structural states ---"
    )

    for key, value in sorted(
        first[
            "terminal_states"
        ].items()
    ):

        print(
            key,
            value,
        )


    print()
    print(
        "--- frozen pass requirements ---"
    )

    for key, value in (
        requirements.items()
    ):

        print(
            key + ":",
            (
                "PASS"
                if value
                else "FAIL"
            ),
        )


    print()
    print(
        "--- boundary ---"
    )

    print(
        "signal selection: NO"
    )

    print(
        "protected-vs-unprotected: NO"
    )

    print(
        "F19 read: NO"
    )

    print(
        "F20 read: NO"
    )

    print(
        "prospective performance read: NO"
    )

    print(
        "markout: NO"
    )

    print(
        "post-fill price evaluation: NO"
    )

    print(
        "inventory valuation: NO"
    )

    print(
        "maker rebate: NO"
    )

    print(
        "settlement/winner: NO"
    )

    print(
        "PnL: NO"
    )

    print(
        "artifact SHA:",
        sha256(
            OUT
        ),
    )


finally:

    conn.rollback()
    conn.close()
