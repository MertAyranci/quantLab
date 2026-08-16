from __future__ import annotations

import hashlib
import json
import sys
from collections import Counter
from datetime import datetime
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
    / "rn1_f21_m6_burned_replay_contract.json"
)

SIDE_GATE = (
    R
    / "rn1_f21_m6_trade_side_gate.json"
)

TAPE = (
    R
    / "h2_v2_m2h_tape.jsonl"
)

OUT = (
    R
    / "rn1_f21_m6_burned_replay_validation.json"
)


EXPECTED_CONTRACT_SHA = (
    "c653dafcab0cc1a35695ac3bb3863d16"
    "b089efdd20514eb0ec949ec1e03aecbc"
)

EXPECTED_SIDE_GATE_SHA = (
    "3eccb25f9493c2ffeed2c8d28d41d509"
    "e8a3c8e8f43f87f404f56b4ba5b0230c"
)

EXPECTED_TAPE_SHA = (
    "c234267e3342601c9706c39d7d4c20c9"
    "3eaeea1df5b0ca0a5513cf67221be75f"
)


def sha256(path: Path) -> str:
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def canonical_sha(obj) -> str:
    raw = json.dumps(
        obj,
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


def mc_price(value) -> Decimal:
    return (
        Decimal(
            str(value)
        )
        /
        Decimal("1000")
    )


def dec(value) -> Decimal:
    return Decimal(
        str(value)
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
            str(k):
                clean(v)

            for k, v
            in value.items()
        }

    if isinstance(
        value,
        (list, tuple),
    ):
        return [
            clean(x)
            for x in value
        ]

    return value


# --------------------------------------------------
# Frozen input integrity
# --------------------------------------------------

if (
    sha256(CONTRACT)
    != EXPECTED_CONTRACT_SHA
):
    raise RuntimeError(
        "REFUSING M6: contract hash changed"
    )


if (
    sha256(SIDE_GATE)
    != EXPECTED_SIDE_GATE_SHA
):
    raise RuntimeError(
        "REFUSING M6: side-gate hash changed"
    )


if (
    sha256(TAPE)
    != EXPECTED_TAPE_SHA
):
    raise RuntimeError(
        "REFUSING M6: burned tape hash changed"
    )


contract = json.loads(
    CONTRACT.read_text(
        encoding="utf-8"
    )
)

gate = json.loads(
    SIDE_GATE.read_text(
        encoding="utf-8"
    )
)


if (
    contract["status"]
    !=
    "FROZEN_BURNED_REAL_DATA_REPLAY_VALIDATION"
):
    raise RuntimeError(
        "unexpected M6 contract status"
    )


if gate["status"] != "PASS":
    raise RuntimeError(
        "trade-side gate did not PASS"
    )


if (
    gate["contract_sha256"]
    != EXPECTED_CONTRACT_SHA
):
    raise RuntimeError(
        "side gate bound to wrong contract"
    )


# Freeze direct side mapping only after gate PASS.
TRADE_SIDE = {
    "BUY":
        AggressorSide.BUY,

    "SELL":
        AggressorSide.SELL,
}


# --------------------------------------------------
# Burned structural tape
# --------------------------------------------------

tape_rows = [
    json.loads(line)

    for line
    in TAPE.read_text(
        encoding="utf-8"
    ).splitlines()

    if line.strip()
]


if len(tape_rows) != 1080:
    raise RuntimeError(
        "unexpected burned anchor count"
    )


if (
    len({
        x["odds_game_id"]
        for x in tape_rows
    })
    != 9
):
    raise RuntimeError(
        "unexpected burned game count"
    )


if (
    len({
        x["polymarket_market_id"]
        for x in tape_rows
    })
    != 9
):
    raise RuntimeError(
        "unexpected burned market count"
    )


if (
    len({
        x["token_id"]
        for x in tape_rows
    })
    != 18
):
    raise RuntimeError(
        "unexpected burned token count"
    )


# --------------------------------------------------
# Database helpers
# --------------------------------------------------

def latest_full_snapshot(
    cur,
    token_id,
    target,
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
            target,
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

    # Authoritative snapshots are structural continuity
    # evidence only. They never directly create a fill.
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


    def source_key(x):

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


    combined = (
        snapshots
        + deltas
        + trades
    )

    combined.sort(
        key=source_key
    )

    return combined


# --------------------------------------------------
# Venue structural validation
# --------------------------------------------------

CENT = Decimal("0.01")


def venue_probe_ok(
    row,
    side: Side,
):

    bid_mc = row[
        "decision_best_bid_mc"
    ]

    ask_mc = row[
        "decision_best_ask_mc"
    ]

    bid_size = row[
        "decision_best_bid_size"
    ]

    ask_size = row[
        "decision_best_ask_size"
    ]


    if (
        bid_mc is None
        or ask_mc is None
        or bid_size is None
        or ask_size is None
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
            "non_passive_or_invalid_bbo",
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


    # M5 requires size to encode at 2 decimals.
    encoded_minimum = minimum.quantize(
        CENT,
        rounding=ROUND_DOWN,
    )

    if (
        encoded_minimum
        != minimum
    ):
        return (
            False,
            "minimum_not_exact_at_two_decimals",
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


    # Frozen post-only condition.
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


# --------------------------------------------------
# Source -> frozen F21 event translation
# --------------------------------------------------

def build_replay_events(
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


    if side is Side.BUY:

        quote_mc = int(
            row[
                "decision_best_bid_mc"
            ]
        )

        quote_price = bid

        queue_ahead = dec(
            row[
                "decision_best_bid_size"
            ]
        )

        book_side = "B"


    else:

        quote_mc = int(
            row[
                "decision_best_ask_mc"
            ]
        )

        quote_price = ask

        queue_ahead = dec(
            row[
                "decision_best_ask_size"
            ]
        )

        book_side = "S"


    events = [
        ReplayEvent(
            ts_ms=0,
            kind=EventKind.PLACEMENT_BOOK,
            displayed_size_at_price=float(
                queue_ahead
            ),
            best_bid=float(
                bid
            ),
            best_ask=float(
                ask
            ),
        )
    ]


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


    stats = Counter()

    gap_inserted = False


    # Every real source record receives a distinct
    # logical replay tick. Real microsecond ordering
    # therefore cannot collapse into a millisecond tie.
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
            f"source_{kind.lower()}"
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
        # Generation continuity where observable.
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
                        kind=EventKind.COLLECTOR_GAP,
                        reason=(
                            "book_generation_changed"
                        ),
                    )
                )

                gap_inserted = True

                continue


        # ------------------------------------------
        # Continuation WS connection.
        #
        # For a REST base, the first subsequent WS
        # record establishes the one allowed
        # continuation connection.
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
                        kind=EventKind.COLLECTOR_GAP,
                        reason=(
                            "connection_changed"
                        ),
                    )
                )

                gap_inserted = True

                continue


        # ------------------------------------------
        # Full snapshots are continuity evidence.
        # Never infer a fill from them.
        # ------------------------------------------

        if kind == "SNAPSHOT":

            stats[
                "snapshot_structural_only"
            ] += 1

            continue


        # ------------------------------------------
        # price_change -> BOOK_SIZE only when it is
        # our exact resting level and side.
        # ------------------------------------------

        if kind == "DELTA":

            if (
                src[
                    "side"
                ]
                == book_side
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
                        kind=EventKind.BOOK_SIZE,
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
        # last_trade_price -> TRADE
        #
        # Direct mapping has already passed the
        # frozen independent M6 gate.
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
                        kind=EventKind.COLLECTOR_GAP,
                        reason=(
                            "unknown_trade_side"
                        ),
                    )
                )

                gap_inserted = True

                continue


            if (
                src[
                    "size"
                ]
                is None
            ):

                stats[
                    "missing_trade_size"
                ] += 1

                events.append(
                    ReplayEvent(
                        ts_ms=logical_tick,
                        kind=EventKind.COLLECTOR_GAP,
                        reason=(
                            "missing_trade_size"
                        ),
                    )
                )

                gap_inserted = True

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
                        kind=EventKind.COLLECTOR_GAP,
                        reason=(
                            "nonpositive_trade_size"
                        ),
                    )
                )

                gap_inserted = True

                continue


            events.append(
                ReplayEvent(
                    ts_ms=logical_tick,
                    kind=EventKind.TRADE,
                    aggressor=TRADE_SIDE[
                        reported_side
                    ],
                    trade_price=float(
                        mc_price(
                            src[
                                "price_mc"
                            ]
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

            continue


        raise RuntimeError(
            f"unexpected source kind: {kind}"
        )


    return {
        "events":
            events,

        "quote_price":
            quote_price,

        "queue_ahead":
            queue_ahead,

        "end_tick":
            len(
                source_rows
            )
            + 1,

        "stats":
            stats,
    }


# --------------------------------------------------
# One deterministic complete build
# --------------------------------------------------

def build_once(
    cur,
):

    metrics = Counter()

    terminal_states = Counter()

    translation = Counter()

    replay_material = []


    for i, row in enumerate(
        tape_rows,
        start=1,
    ):

        token_id = int(
            row[
                "token_id"
            ]
        )

        start = dt(
            row[
                "decision_time"
            ]
        )

        end = dt(
            row[
                "execution_time"
            ]
        )


        if (
            (
                end - start
            ).total_seconds()
            != 0.5
        ):

            metrics[
                "bad_real_window"
            ] += 1


        # ------------------------------------------
        # Independently recover frozen decision base.
        # ------------------------------------------

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


        expected_decision_time = dt(
            row[
                "decision_base_time"
            ]
        )


        if (
            base[
                "capture_time"
            ]
            != expected_decision_time
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


        # ------------------------------------------
        # Independently recover frozen execution base.
        # ------------------------------------------

        execution_base = (
            latest_full_snapshot(
                cur,
                token_id,
                end,
            )
        )


        if execution_base is None:

            metrics[
                "missing_execution_base"
            ] += 1


        else:

            expected_execution_time = dt(
                row[
                    "execution_base_time"
                ]
            )


            if (
                execution_base[
                    "capture_time"
                ]
                != expected_execution_time
                or
                execution_base[
                    "source"
                ]
                != row[
                    "execution_base_source"
                ]
                or
                int(
                    execution_base[
                        "book_generation"
                    ]
                )
                != int(
                    row[
                        "execution_base_generation"
                    ]
                )
            ):

                metrics[
                    "execution_base_mismatch"
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


        # Query bounds themselves enforce no lookahead.
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


        # ------------------------------------------
        # Always probe BOTH sides.
        # No signal selects either side.
        # ------------------------------------------

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


            translated = build_replay_events(
                row=row,
                side=side,
                base=base,
                source_rows=source_rows,
            )


            translation.update(
                translated[
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
                    "m6:"
                    f"{row['sharp_consensus_id']}:"
                    f"{side.value}"
                ),
                side=side,
                price=float(
                    translated[
                        "quote_price"
                    ]
                ),
                shares=float(
                    shares
                ),
                submit_ts_ms=0,
            )


            try:

                result = replay_passive_order(
                    spec=spec,
                    config=ShadowConfig(
                        placement_latency_ms=0,
                        cancel_latency_ms=0,
                    ),
                    events=translated[
                        "events"
                    ],
                    end_ts_ms=translated[
                        "end_tick"
                    ],
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


            if (
                result[
                    "order"
                ]
                if isinstance(
                    result,
                    dict,
                )
                else None
            ):
                # Defensive only; frozen ReplayResult
                # is a dataclass, not a dict.
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


            state_obj = (
                result.order.state
            )

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
                str(state)
            ] += 1


            # Full replay summaries are hashed for
            # determinism but are NOT written out as
            # per-probe performance observations.
            replay_material.append({
                "anchor":
                    row[
                        "sharp_consensus_id"
                    ],

                "token_id":
                    token_id,

                "side":
                    side.value,

                "source_rows":
                    len(
                        source_rows
                    ),

                "replay":
                    clean(
                        result.canonical_summary()
                    ),
            })


        if (
            i % 100 == 0
            or i
            == len(
                tape_rows
            )
        ):

            print(
                f"build: {i}/{len(tape_rows)} anchors"
            )


    return {
        "metrics":
            dict(
                metrics
            ),

        "terminal_states":
            dict(
                terminal_states
            ),

        "translation":
            dict(
                translation
            ),

        "replay_sha256":
            canonical_sha(
                replay_material
            ),
    }


# --------------------------------------------------
# Main
# --------------------------------------------------

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


# One immutable DB snapshot for both deterministic
# rebuilds.
conn.set_session(
    readonly=True,
    autocommit=False,
    isolation_level=(
        "REPEATABLE READ"
    ),
)


try:

    with conn.cursor() as cur:

        token_ids = sorted({
            int(
                x["token_id"]
            )
            for x in tape_rows
        })


        # ------------------------------------------
        # Reconfirm frozen sequencing integrity.
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
            "F21-M6 BUILD 1"
        )
        print(
            "========================================"
        )

        first = build_once(
            cur
        )


        print()
        print(
            "========================================"
        )
        print(
            "F21-M6 BUILD 2"
        )
        print(
            "========================================"
        )

        second = build_once(
            cur
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


    # Adapter never applies an offending event
    # across a continuity boundary. It emits
    # COLLECTOR_GAP first instead.
    generation_mixing = 0
    connection_mixing = 0


    pass_requirements = {
        "side_gate_pass":
            gate["status"]
            == "PASS",

        "anchors":
            len(
                tape_rows
            )
            == 1080,

        "probe_orders":
            metrics[
                "probe_orders"
            ]
            == 2160,

        "probes_replayed":
            metrics[
                "probes_replayed"
            ]
            == 2160,

        "venue_invalid":
            metrics[
                "venue_invalid"
            ]
            == 0,

        "decision_base_mismatch":
            metrics[
                "decision_base_mismatch"
            ]
            == 0,

        "execution_base_mismatch":
            metrics[
                "execution_base_mismatch"
            ]
            == 0,

        "missing_decision_base":
            metrics[
                "missing_decision_base"
            ]
            == 0,

        "missing_execution_base":
            metrics[
                "missing_execution_base"
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

        "unknown_trade_side":
            translation[
                "unknown_trade_side"
            ]
            == 0,

        "missing_trade_size":
            translation[
                "missing_trade_size"
            ]
            == 0,

        "replay_errors":
            metrics[
                "replay_errors"
            ]
            == 0,

        "placement_snapshot_seen":
            metrics[
                "placement_snapshot_missing"
            ]
            == 0,

        "generation_mixing":
            generation_mixing
            == 0,

        "connection_mixing":
            connection_mixing
            == 0,

        "deterministic_rebuild":
            deterministic,
    }


    status = (
        "PASS_BURNED_REAL_DATA_REPLAY_VALIDATION"
        if all(
            pass_requirements.values()
        )
        else
        "FAIL_BURNED_REAL_DATA_REPLAY_VALIDATION"
    )


    artifact = {
        "study":
            "RN1-F21-M6",

        "status":
            status,

        "contract_sha256":
            sha256(
                CONTRACT
            ),

        "trade_side_gate": {
            "artifact_sha256":
                sha256(
                    SIDE_GATE
                ),

            "status":
                gate[
                    "status"
                ],

            "direct":
                gate[
                    "classification"
                ][
                    "direct"
                ],

            "inverse":
                gate[
                    "classification"
                ][
                    "inverse"
                ],

            "comparable":
                gate[
                    "classification"
                ][
                    "comparable"
                ],

            "direct_support_fraction":
                gate[
                    "classification"
                ][
                    "direct_support_fraction"
                ],

            "frozen_mapping": {
                "BUY":
                    (
                        "aggressive BUY "
                        "consumes resting asks"
                    ),

                "SELL":
                    (
                        "aggressive SELL "
                        "consumes resting bids"
                    ),
            },
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
        },

        "replay_semantics": {
            "real_window_ms":
                500,

            "logical_replay_ticks":
                True,

            "logical_ticks_are_latency":
                False,

            "placement_latency_ms":
                0,

            "cancel_latency_ms":
                0,

            "signal_used":
                False,

            "both_sides_probed":
                True,

            "probe_size":
                "captured minimum_order_size",

            "trade_inference":
                False,

            "book_snapshot_fill_inference":
                False,

            "continuity_break":
                "COLLECTOR_GAP before offending event",
        },

        "source_integrity": {
            "duplicate_delta_sequence_keys":
                duplicate_deltas,

            "duplicate_trade_sequence_keys":
                duplicate_trades,

            "lookahead_or_predecision_events":
                metrics[
                    "lookahead_or_predecision_event"
                ],

            "decision_base_mismatches":
                metrics[
                    "decision_base_mismatch"
                ],

            "execution_base_mismatches":
                metrics[
                    "execution_base_mismatch"
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

            "important":
                (
                    "Observed continuity breaks invalidate "
                    "the affected probe before the offending "
                    "source event; they are not mixed into replay."
                ),
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

        "validation": {
            "probes_replayed":
                metrics[
                    "probes_replayed"
                ],

            "venue_invalid":
                metrics[
                    "venue_invalid"
                ],

            "replay_errors":
                metrics[
                    "replay_errors"
                ],

            "placement_snapshot_missing":
                metrics[
                    "placement_snapshot_missing"
                ],

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

            "requirements":
                pass_requirements,
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

            "markout_calculated":
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
                artifact
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
        "F21-M6 BURNED REAL-DATA REPLAY"
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
        len(
            tape_rows
        ),
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
        "execution base mismatches:",
        metrics[
            "execution_base_mismatch"
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
        "generation mixing:",
        generation_mixing,
    )

    print(
        "connection mixing:",
        connection_mixing,
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
        "unknown trade side:",
        translation[
            "unknown_trade_side"
        ],
    )

    print(
        "replay errors:",
        metrics[
            "replay_errors"
        ],
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
        "--- pass requirements ---"
    )

    for key, value in (
        pass_requirements.items()
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
        "protected-vs-unprotected comparison: NO"
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
