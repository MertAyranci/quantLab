from __future__ import annotations

import hashlib
import json
import subprocess
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import dotenv_values


REPO = Path(__file__).resolve().parents[1]
R = REPO / "research"

CONTRACT = (
    R / "rn1_f21_m8_queue_diagnosis_contract.json"
)

M7_VALIDATION = (
    R / "rn1_f21_m7_burned_fill_path_validation.json"
)

M7_RUNNER = (
    R / "rn1_f21_m7_burned_fill_path.py"
)

TAPE = (
    R / "h2_v2_m2h_tape.jsonl"
)

OUT = (
    R / "rn1_f21_m8_queue_diagnosis_validation.json"
)


EXPECTED_HEAD = (
    "e52ee344165716b9efa176e9bbf68d58250ddbf1"
)

EXPECTED_CONTRACT_SHA = (
    "6398bdbe3bc0501b260202df84812b2c"
    "bcc054a26ce20fe34ae39f3a8dd5b169"
)

EXPECTED_M7_VALIDATION_SHA = (
    "bae57113937b96277000f1ddc68642b1"
    "b4e9bf2b8b925ab9405ead4567c8c3d7"
)

EXPECTED_M7_RUNNER_SHA = (
    "6bb7f3afed358300cff2081a148212bfc"
    "9d65954a35f4f5a76fa35bd10046684"
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
    if isinstance(value, datetime):
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


def clean(value):

    if isinstance(value, Decimal):
        return str(value)

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, dict):
        return {
            str(k): clean(v)
            for k, v in value.items()
        }

    if isinstance(value, (list, tuple)):
        return [
            clean(v)
            for v in value
        ]

    return value


def empirical_quantile(
    values,
    fraction: Decimal,
):
    """
    Deterministic empirical-lower quantile.

    index = floor((n - 1) * q)

    No statistical modeling is performed.
    """

    if not values:
        return None

    ordered = sorted(
        dec(v)
        for v in values
    )

    if fraction <= 0:
        return ordered[0]

    if fraction >= 1:
        return ordered[-1]

    idx = int(
        Decimal(
            len(ordered) - 1
        )
        * fraction
    )

    return ordered[
        idx
    ]


def summary(values):

    vals = [
        dec(v)
        for v in values
    ]

    if not vals:
        return {
            "n": 0,
            "min": None,
            "p10": None,
            "p25": None,
            "p50": None,
            "p75": None,
            "p90": None,
            "p95": None,
            "p99": None,
            "max": None,
        }

    return {
        "n":
            len(vals),

        "min":
            min(vals),

        "p10":
            empirical_quantile(
                vals,
                Decimal("0.10"),
            ),

        "p25":
            empirical_quantile(
                vals,
                Decimal("0.25"),
            ),

        "p50":
            empirical_quantile(
                vals,
                Decimal("0.50"),
            ),

        "p75":
            empirical_quantile(
                vals,
                Decimal("0.75"),
            ),

        "p90":
            empirical_quantile(
                vals,
                Decimal("0.90"),
            ),

        "p95":
            empirical_quantile(
                vals,
                Decimal("0.95"),
            ),

        "p99":
            empirical_quantile(
                vals,
                Decimal("0.99"),
            ),

        "max":
            max(vals),
    }


# ==================================================
# Frozen input checks
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
        "REFUSING M8: unexpected HEAD\n"
        f"expected {EXPECTED_HEAD}\n"
        f"actual   {head}"
    )


for path, expected in (
    (
        CONTRACT,
        EXPECTED_CONTRACT_SHA,
    ),
    (
        M7_VALIDATION,
        EXPECTED_M7_VALIDATION_SHA,
    ),
    (
        M7_RUNNER,
        EXPECTED_M7_RUNNER_SHA,
    ),
    (
        TAPE,
        EXPECTED_TAPE_SHA,
    ),
):

    actual = sha256(path)

    if actual != expected:
        raise RuntimeError(
            "REFUSING M8: frozen artifact changed\n"
            f"path     {path}\n"
            f"expected {expected}\n"
            f"actual   {actual}"
        )


contract = json.loads(
    CONTRACT.read_text(
        encoding="utf-8"
    )
)

m7 = json.loads(
    M7_VALIDATION.read_text(
        encoding="utf-8"
    )
)


if (
    contract["status"]
    !=
    "FROZEN_BURNED_QUEUE_DEPLETION_DIAGNOSIS"
):
    raise RuntimeError(
        "unexpected M8 contract status"
    )


if (
    m7["status"]
    !=
    "FAIL_BURNED_LONG_WINDOW_FILL_PATH_VALIDATION"
):
    raise RuntimeError(
        "unexpected M7 parent status"
    )


if (
    m7[
        "fill_path_coverage"
    ][
        "positive_simulated_fill_probes"
    ]
    != 0
):
    raise RuntimeError(
        "M7 positive fill count changed"
    )


if (
    m7[
        "fill_path_coverage"
    ][
        "unique_queue_consuming_real_trade_events"
    ]
    != 64
):
    raise RuntimeError(
        "M7 queue event count changed"
    )


if (
    m7[
        "fill_path_coverage"
    ][
        "invalidated_probes"
    ]
    != 504
):
    raise RuntimeError(
        "M7 invalidation count changed"
    )


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
        "unexpected burned anchor count"
    )


if len({
    int(x["token_id"])
    for x in rows
}) != 18:
    raise RuntimeError(
        "unexpected burned token count"
    )


if len({
    int(x["polymarket_market_id"])
    for x in rows
}) != 9:
    raise RuntimeError(
        "unexpected burned market count"
    )


# ==================================================
# DB helpers — same data boundary as M7
# ==================================================

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


    result = (
        snapshots
        + deltas
        + trades
    )

    result.sort(
        key=source_key
    )

    return result


# ==================================================
# One probe diagnosis
# ==================================================

def diagnose_probe(
    *,
    row,
    side,
    base,
    source_rows,
    start,
):

    if side == "BUY":

        quote_mc = int(
            row[
                "decision_best_bid_mc"
            ]
        )

        initial_queue = dec(
            row[
                "decision_best_bid_size"
            ]
        )

        order_size = dec(
            row[
                "minimum_order_size"
            ]
        )

        qualifying_aggressor = (
            "SELL"
        )

    elif side == "SELL":

        quote_mc = int(
            row[
                "decision_best_ask_mc"
            ]
        )

        initial_queue = dec(
            row[
                "decision_best_ask_size"
            ]
        )

        order_size = dec(
            row[
                "minimum_order_size"
            ]
        )

        qualifying_aggressor = (
            "BUY"
        )

    else:

        raise RuntimeError(
            f"unexpected probe side {side}"
        )


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


    cumulative_volume = (
        Decimal("0")
    )

    queue_remaining = (
        initial_queue
    )

    own_remaining = (
        order_size
    )


    first_qualifying_time = (
        None
    )

    invalidation_time = (
        None
    )

    invalidation_reason = (
        None
    )


    qualifying_event_keys = set()

    qualifying_occurrences = 0

    translated_trades = 0

    exact_level_updates = 0

    generation_crossings = 0

    connection_changes = 0

    unknown_trade_side = 0

    missing_trade_size = 0

    nonpositive_trade_size = 0


    terminal = False


    for src in source_rows:

        if terminal:
            break


        kind = src[
            "kind"
        ]


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
        # Same frozen continuity semantics as M7.
        # ------------------------------------------

        if kind in (
            "SNAPSHOT",
            "DELTA",
        ):

            source_generation = int(
                src[
                    "book_generation"
                ]
            )

            if (
                source_generation
                != expected_generation
            ):

                generation_crossings += 1

                invalidation_time = (
                    src[
                        "capture_time"
                    ]
                )

                invalidation_reason = (
                    "book_generation_changed"
                )

                terminal = True

                break


        if src_connection is not None:

            if active_connection is None:

                active_connection = (
                    src_connection
                )

            elif (
                src_connection
                != active_connection
            ):

                connection_changes += 1

                invalidation_time = (
                    src[
                        "capture_time"
                    ]
                )

                invalidation_reason = (
                    "connection_changed"
                )

                terminal = True

                break


        # ------------------------------------------
        # Snapshot never changes queue position.
        # ------------------------------------------

        if kind == "SNAPSHOT":
            continue


        # ------------------------------------------
        # Book deltas are descriptive only.
        #
        # Decreases grant no queue credit.
        # Increases are behind us.
        # ------------------------------------------

        if kind == "DELTA":

            expected_delta_side = (
                "B"
                if side == "BUY"
                else "S"
            )

            if (
                src[
                    "side"
                ]
                == expected_delta_side
                and
                int(
                    src[
                        "price_mc"
                    ]
                )
                == quote_mc
            ):

                exact_level_updates += 1

            continue


        # ------------------------------------------
        # Explicit real trade only.
        # ------------------------------------------

        if kind == "TRADE":

            reported_side = (
                src[
                    "side"
                ]
            )


            if reported_side not in (
                "BUY",
                "SELL",
            ):

                unknown_trade_side += 1

                invalidation_time = (
                    src[
                        "capture_time"
                    ]
                )

                invalidation_reason = (
                    "unknown_trade_side"
                )

                terminal = True

                break


            if (
                src[
                    "size"
                ]
                is None
            ):

                missing_trade_size += 1

                invalidation_time = (
                    src[
                        "capture_time"
                    ]
                )

                invalidation_reason = (
                    "missing_trade_size"
                )

                terminal = True

                break


            size = dec(
                src[
                    "size"
                ]
            )


            if size <= 0:

                nonpositive_trade_size += 1

                invalidation_time = (
                    src[
                        "capture_time"
                    ]
                )

                invalidation_reason = (
                    "nonpositive_trade_size"
                )

                terminal = True

                break


            translated_trades += 1


            if not (
                reported_side
                == qualifying_aggressor
                and
                int(
                    src[
                        "price_mc"
                    ]
                )
                == quote_mc
            ):
                continue


            # This is an explicit queue-consuming
            # trade under the already-frozen M6 side
            # mapping and M7 exact-price rule.

            qualifying_occurrences += 1

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

            qualifying_event_keys.add(
                event_key
            )


            if (
                first_qualifying_time
                is None
            ):

                first_qualifying_time = (
                    src[
                        "capture_time"
                    ]
                )


            cumulative_volume += (
                size
            )


            remaining_trade = (
                size
            )


            if queue_remaining > 0:

                consumed_queue = min(
                    queue_remaining,
                    remaining_trade,
                )

                queue_remaining -= (
                    consumed_queue
                )

                remaining_trade -= (
                    consumed_queue
                )


            # This is the same mechanical fill
            # threshold as the frozen F21 queue
            # model. We do NOT alter the model.
            if (
                remaining_trade > 0
                and
                own_remaining > 0
            ):

                consumed_own = min(
                    own_remaining,
                    remaining_trade,
                )

                own_remaining -= (
                    consumed_own
                )


            if own_remaining <= 0:

                terminal = True

                break


            continue


        raise RuntimeError(
            f"unexpected source kind {kind}"
        )


    expected_filled = (
        order_size
        - own_remaining
    )


    residual_queue = max(
        initial_queue
        - cumulative_volume,
        Decimal("0"),
    )


    volume_beyond_queue = max(
        cumulative_volume
        - initial_queue,
        Decimal("0"),
    )


    if initial_queue > 0:

        depletion_ratio = (
            cumulative_volume
            /
            initial_queue
        )

        queue_to_order_ratio = (
            initial_queue
            /
            order_size
        )

    else:

        depletion_ratio = (
            None
        )

        queue_to_order_ratio = (
            Decimal("0")
        )


    first_trade_seconds = (
        (
            first_qualifying_time
            - start
        ).total_seconds()

        if first_qualifying_time
        is not None

        else None
    )


    invalidation_seconds = (
        (
            invalidation_time
            - start
        ).total_seconds()

        if invalidation_time
        is not None

        else None
    )


    return {
        "initial_queue":
            initial_queue,

        "order_size":
            order_size,

        "queue_to_order_ratio":
            queue_to_order_ratio,

        "qualifying_occurrences":
            qualifying_occurrences,

        "qualifying_event_keys":
            qualifying_event_keys,

        "cumulative_volume":
            cumulative_volume,

        "depletion_ratio":
            depletion_ratio,

        "residual_queue":
            residual_queue,

        "volume_beyond_queue":
            volume_beyond_queue,

        "expected_filled":
            expected_filled,

        "first_trade_seconds":
            first_trade_seconds,

        "invalidation_seconds":
            invalidation_seconds,

        "invalidation_reason":
            invalidation_reason,

        "translated_trades":
            translated_trades,

        "exact_level_updates":
            exact_level_updates,

        "generation_crossings":
            generation_crossings,

        "connection_changes":
            connection_changes,

        "unknown_trade_side":
            unknown_trade_side,

        "missing_trade_size":
            missing_trade_size,

        "nonpositive_trade_size":
            nonpositive_trade_size,
    }


# ==================================================
# Complete deterministic build
# ==================================================

def build_once(
    cur,
    *,
    label,
):

    counters = Counter()

    unique_queue_events = set()

    initial_queues = []

    queue_to_order_ratios = []

    cumulative_volumes = []

    depletion_ratios_all = []

    depletion_ratios_active = []

    residual_queues = []

    first_trade_times = []

    invalidation_times = []


    side_stats = {
        "BUY":
            Counter(),

        "SELL":
            Counter(),
    }


    market_stats = defaultdict(
        Counter
    )

    market_unique_events = defaultdict(
        set
    )


    probe_material = []


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
                seconds=60
            )
        )


        base = latest_full_snapshot(
            cur,
            token_id,
            start,
        )


        if base is None:

            counters[
                "missing_decision_base"
            ] += 2

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

            counters[
                "decision_base_mismatch"
            ] += 2


        source_rows = window_sources(
            cur,
            token_id,
            start,
            end,
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

                counters[
                    "lookahead_or_predecision_event"
                ] += 1


        for side in (
            "BUY",
            "SELL",
        ):

            counters[
                "probe_orders"
            ] += 1

            side_stats[
                side
            ][
                "probes"
            ] += 1

            market_stats[
                market_id
            ][
                "probes"
            ] += 1


            result = diagnose_probe(
                row=row,
                side=side,
                base=base,
                source_rows=source_rows,
                start=start,
            )


            initial_queue = result[
                "initial_queue"
            ]

            cumulative = result[
                "cumulative_volume"
            ]

            ratio = result[
                "depletion_ratio"
            ]

            expected_filled = result[
                "expected_filled"
            ]


            initial_queues.append(
                initial_queue
            )

            queue_to_order_ratios.append(
                result[
                    "queue_to_order_ratio"
                ]
            )

            cumulative_volumes.append(
                cumulative
            )

            residual_queues.append(
                result[
                    "residual_queue"
                ]
            )


            if ratio is not None:

                depletion_ratios_all.append(
                    ratio
                )


            if (
                result[
                    "qualifying_occurrences"
                ]
                > 0
            ):

                counters[
                    "probes_with_qualifying_trade"
                ] += 1

                side_stats[
                    side
                ][
                    "probes_with_qualifying_trade"
                ] += 1

                market_stats[
                    market_id
                ][
                    "probes_with_qualifying_trade"
                ] += 1


                if ratio is not None:

                    depletion_ratios_active.append(
                        ratio
                    )


            if (
                cumulative
                >= initial_queue
                and
                cumulative > 0
            ):

                counters[
                    "probes_reaching_queue"
                ] += 1

                side_stats[
                    side
                ][
                    "probes_reaching_queue"
                ] += 1

                market_stats[
                    market_id
                ][
                    "probes_reaching_queue"
                ] += 1


            if (
                cumulative
                == initial_queue
                and
                cumulative > 0
            ):

                counters[
                    "probes_exactly_exhausting_queue"
                ] += 1


            if (
                cumulative
                > initial_queue
            ):

                counters[
                    "probes_exceeding_queue"
                ] += 1

                side_stats[
                    side
                ][
                    "probes_exceeding_queue"
                ] += 1

                market_stats[
                    market_id
                ][
                    "probes_exceeding_queue"
                ] += 1


            if expected_filled > 0:

                counters[
                    "mechanically_expected_positive_fills"
                ] += 1


            for threshold in (
                Decimal("0.25"),
                Decimal("0.50"),
                Decimal("0.75"),
                Decimal("0.90"),
                Decimal("0.95"),
                Decimal("0.99"),
            ):

                if (
                    ratio is not None
                    and
                    ratio >= threshold
                ):

                    key = (
                        "depletion_ge_"
                        + str(
                            int(
                                threshold
                                * 100
                            )
                        )
                        + "pct"
                    )

                    counters[
                        key
                    ] += 1


            if (
                result[
                    "first_trade_seconds"
                ]
                is not None
            ):

                first_trade_times.append(
                    dec(
                        result[
                            "first_trade_seconds"
                        ]
                    )
                )


            if (
                result[
                    "invalidation_seconds"
                ]
                is not None
            ):

                invalidation_times.append(
                    dec(
                        result[
                            "invalidation_seconds"
                        ]
                    )
                )

                counters[
                    "invalidated_probes"
                ] += 1

                side_stats[
                    side
                ][
                    "invalidated_probes"
                ] += 1

                market_stats[
                    market_id
                ][
                    "invalidated_probes"
                ] += 1


                if (
                    result[
                        "first_trade_seconds"
                    ]
                    is None
                ):

                    counters[
                        "invalidated_before_any_qualifying_trade"
                    ] += 1

                else:

                    counters[
                        "invalidated_after_qualifying_trade"
                    ] += 1


            counters[
                "qualifying_trade_occurrences"
            ] += result[
                "qualifying_occurrences"
            ]

            counters[
                "translated_trades"
            ] += result[
                "translated_trades"
            ]

            counters[
                "exact_level_updates"
            ] += result[
                "exact_level_updates"
            ]

            counters[
                "generation_crossings"
            ] += result[
                "generation_crossings"
            ]

            counters[
                "connection_changes"
            ] += result[
                "connection_changes"
            ]

            counters[
                "unknown_trade_side"
            ] += result[
                "unknown_trade_side"
            ]

            counters[
                "missing_trade_size"
            ] += result[
                "missing_trade_size"
            ]

            counters[
                "nonpositive_trade_size"
            ] += result[
                "nonpositive_trade_size"
            ]


            unique_queue_events.update(
                result[
                    "qualifying_event_keys"
                ]
            )

            market_unique_events[
                market_id
            ].update(
                result[
                    "qualifying_event_keys"
                ]
            )


            side_stats[
                side
            ][
                "qualifying_occurrences"
            ] += result[
                "qualifying_occurrences"
            ]


            market_stats[
                market_id
            ][
                "qualifying_occurrences"
            ] += result[
                "qualifying_occurrences"
            ]


            probe_material.append({
                "anchor":
                    row[
                        "sharp_consensus_id"
                    ],

                "market_id":
                    market_id,

                "token_id":
                    token_id,

                "side":
                    side,

                "initial_queue":
                    str(
                        initial_queue
                    ),

                "order_size":
                    str(
                        result[
                            "order_size"
                        ]
                    ),

                "qualifying_occurrences":
                    result[
                        "qualifying_occurrences"
                    ],

                "cumulative_volume":
                    str(
                        cumulative
                    ),

                "depletion_ratio":
                    (
                        str(ratio)
                        if ratio is not None
                        else None
                    ),

                "residual_queue":
                    str(
                        result[
                            "residual_queue"
                        ]
                    ),

                "volume_beyond_queue":
                    str(
                        result[
                            "volume_beyond_queue"
                        ]
                    ),

                "expected_filled":
                    str(
                        expected_filled
                    ),

                "first_trade_seconds":
                    result[
                        "first_trade_seconds"
                    ],

                "invalidation_seconds":
                    result[
                        "invalidation_seconds"
                    ],

                "invalidation_reason":
                    result[
                        "invalidation_reason"
                    ],
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


    by_market = {}

    for market_id in sorted(
        market_stats
    ):

        counts = market_stats[
            market_id
        ]

        by_market[
            str(market_id)
        ] = {
            "probes":
                int(
                    counts[
                        "probes"
                    ]
                ),

            "probes_with_qualifying_trade":
                int(
                    counts[
                        "probes_with_qualifying_trade"
                    ]
                ),

            "qualifying_trade_occurrences":
                int(
                    counts[
                        "qualifying_occurrences"
                    ]
                ),

            "unique_qualifying_trade_events":
                len(
                    market_unique_events[
                        market_id
                    ]
                ),

            "probes_reaching_queue":
                int(
                    counts[
                        "probes_reaching_queue"
                    ]
                ),

            "probes_exceeding_queue":
                int(
                    counts[
                        "probes_exceeding_queue"
                    ]
                ),

            "invalidated_probes":
                int(
                    counts[
                        "invalidated_probes"
                    ]
                ),
        }


    by_side = {}

    for side in (
        "BUY",
        "SELL",
    ):

        counts = side_stats[
            side
        ]

        by_side[
            side
        ] = {
            key:
                int(value)

            for key, value
            in sorted(
                counts.items()
            )
        }


    return {
        "counters":
            dict(
                counters
            ),

        "unique_queue_events":
            len(
                unique_queue_events
            ),

        "summaries": {
            "initial_queue_ahead":
                summary(
                    initial_queues
                ),

            "initial_queue_to_order_size_ratio":
                summary(
                    queue_to_order_ratios
                ),

            "cumulative_qualifying_trade_volume":
                summary(
                    cumulative_volumes
                ),

            "queue_depletion_ratio_all_probes":
                summary(
                    depletion_ratios_all
                ),

            "queue_depletion_ratio_probes_with_trade":
                summary(
                    depletion_ratios_active
                ),

            "residual_queue_ahead":
                summary(
                    residual_queues
                ),

            "time_to_first_qualifying_trade_seconds":
                summary(
                    first_trade_times
                ),

            "time_to_invalidation_seconds":
                summary(
                    invalidation_times
                ),
        },

        "by_side":
            by_side,

        "by_market":
            by_market,

        "diagnostic_sha256":
            canonical_sha(
                probe_material
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


conn.set_session(
    readonly=True,
    autocommit=False,
    isolation_level="REPEATABLE READ",
)


try:

    token_ids = sorted({
        int(
            x[
                "token_id"
            ]
        )
        for x in rows
    })


    with conn.cursor() as cur:

        # ------------------------------------------
        # Reconfirm sequence integrity.
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
            "F21-M8 BUILD 1"
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
            "F21-M8 BUILD 2"
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


    c = Counter(
        first[
            "counters"
        ]
    )


    m7_unique_events = int(
        m7[
            "fill_path_coverage"
        ][
            "unique_queue_consuming_real_trade_events"
        ]
    )

    m7_invalidated = int(
        m7[
            "fill_path_coverage"
        ][
            "invalidated_probes"
        ]
    )

    m7_positive_fills = int(
        m7[
            "fill_path_coverage"
        ][
            "positive_simulated_fill_probes"
        ]
    )


    # A trade volume equal to queue ahead exactly
    # exhausts queue but does not yet fill our order.
    #
    # Positive own fill requires STRICTLY more
    # qualifying volume than initial queue ahead.
    expected_positive = int(
        c[
            "mechanically_expected_positive_fills"
        ]
    )


    discrepancy = (
        expected_positive
        != m7_positive_fills
    )


    integrity = {
        "probe_orders":
            c[
                "probe_orders"
            ]
            == 2160,

        "missing_decision_base":
            c[
                "missing_decision_base"
            ]
            == 0,

        "decision_base_mismatch":
            c[
                "decision_base_mismatch"
            ]
            == 0,

        "lookahead_or_predecision_event":
            c[
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
            c[
                "unknown_trade_side"
            ]
            == 0,

        "missing_trade_size":
            c[
                "missing_trade_size"
            ]
            == 0,

        "nonpositive_trade_size":
            c[
                "nonpositive_trade_size"
            ]
            == 0,

        "m7_unique_queue_event_reproduction":
            first[
                "unique_queue_events"
            ]
            == m7_unique_events,

        "m7_invalidation_reproduction":
            c[
                "invalidated_probes"
            ]
            == m7_invalidated,

        "m7_positive_fill_mechanical_reproduction":
            expected_positive
            == m7_positive_fills,

        "deterministic_rebuild":
            deterministic,
    }


    mechanically_explained = (
        expected_positive == 0
        and
        m7_positive_fills == 0
        and
        not discrepancy
    )


    status = (
        "PASS_BURNED_QUEUE_DEPLETION_DIAGNOSIS"
        if (
            all(
                integrity.values()
            )
            and
            mechanically_explained
        )
        else
        "FAIL_BURNED_QUEUE_DEPLETION_DIAGNOSIS"
    )


    interpretation = (
        (
            "M7 zero fills are mechanically consistent "
            "with the frozen F21 queue model: explicit "
            "qualifying trade volume never exceeded "
            "initial queue ahead sufficiently to reach "
            "the probe order."
        )
        if mechanically_explained
        else
        (
            "An unresolved discrepancy exists between "
            "explicit qualifying trade volume and the "
            "frozen M7/F21 fill result."
        )
    )


    validation = {
        "study":
            "RN1-F21-M8",

        "status":
            status,

        "contract_sha256":
            sha256(
                CONTRACT
            ),

        "parent_m7": {
            "commit":
                head,

            "runner_sha256":
                sha256(
                    M7_RUNNER
                ),

            "validation_sha256":
                sha256(
                    M7_VALIDATION
                ),

            "positive_fill_probes":
                m7_positive_fills,

            "unique_queue_consuming_real_trade_events":
                m7_unique_events,

            "invalidated_probes":
                m7_invalidated,
        },

        "diagnosis": {
            "mechanically_explained":
                mechanically_explained,

            "queue_model_discrepancy":
                discrepancy,

            "probe_orders":
                int(
                    c[
                        "probe_orders"
                    ]
                ),

            "probes_with_any_qualifying_trade":
                int(
                    c[
                        "probes_with_qualifying_trade"
                    ]
                ),

            "qualifying_trade_occurrences":
                int(
                    c[
                        "qualifying_trade_occurrences"
                    ]
                ),

            "unique_qualifying_trade_events":
                int(
                    first[
                        "unique_queue_events"
                    ]
                ),

            "probes_reaching_initial_queue":
                int(
                    c[
                        "probes_reaching_queue"
                    ]
                ),

            "probes_exactly_exhausting_initial_queue":
                int(
                    c[
                        "probes_exactly_exhausting_queue"
                    ]
                ),

            "probes_exceeding_initial_queue":
                int(
                    c[
                        "probes_exceeding_queue"
                    ]
                ),

            "mechanically_expected_positive_fill_probes":
                expected_positive,

            "m7_actual_positive_simulated_fill_probes":
                m7_positive_fills,

            "depletion_ge_25pct":
                int(
                    c[
                        "depletion_ge_25pct"
                    ]
                ),

            "depletion_ge_50pct":
                int(
                    c[
                        "depletion_ge_50pct"
                    ]
                ),

            "depletion_ge_75pct":
                int(
                    c[
                        "depletion_ge_75pct"
                    ]
                ),

            "depletion_ge_90pct":
                int(
                    c[
                        "depletion_ge_90pct"
                    ]
                ),

            "depletion_ge_95pct":
                int(
                    c[
                        "depletion_ge_95pct"
                    ]
                ),

            "depletion_ge_99pct":
                int(
                    c[
                        "depletion_ge_99pct"
                    ]
                ),

            "invalidated_probes":
                int(
                    c[
                        "invalidated_probes"
                    ]
                ),

            "invalidated_before_any_qualifying_trade":
                int(
                    c[
                        "invalidated_before_any_qualifying_trade"
                    ]
                ),

            "invalidated_after_qualifying_trade":
                int(
                    c[
                        "invalidated_after_qualifying_trade"
                    ]
                ),

            "interpretation":
                interpretation,
        },

        "summaries":
            first[
                "summaries"
            ],

        "by_side":
            first[
                "by_side"
            ],

        "by_market":
            first[
                "by_market"
            ],

        "continuity": {
            "generation_crossings_observed":
                int(
                    c[
                        "generation_crossings"
                    ]
                ),

            "connection_changes_observed":
                int(
                    c[
                        "connection_changes"
                    ]
                ),

            "generation_mixing":
                0,

            "connection_mixing":
                0,
        },

        "source_counts": {
            "translated_trades":
                int(
                    c[
                        "translated_trades"
                    ]
                ),

            "exact_level_updates":
                int(
                    c[
                        "exact_level_updates"
                    ]
                ),
        },

        "integrity": {
            "duplicate_delta_sequence_keys":
                duplicate_deltas,

            "duplicate_trade_sequence_keys":
                duplicate_trades,

            "checks": {
                key:
                    bool(value)

                for key, value
                in integrity.items()
            },
        },

        "determinism": {
            "deterministic_rebuild":
                deterministic,

            "first_diagnostic_sha256":
                first[
                    "diagnostic_sha256"
                ],

            "second_diagnostic_sha256":
                second[
                    "diagnostic_sha256"
                ],
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

            "alternate_horizon_tested":
                False,

            "queue_semantics_changed":
                False,

            "cancel_ahead_inferred":
                False,

            "hidden_liquidity_inferred":
                False,

            "trade_through_fill_inferred":
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


    s = first[
        "summaries"
    ]


    print()
    print(
        "========================================"
    )
    print(
        "F21-M8 BURNED QUEUE-DEPLETION DIAGNOSIS"
    )
    print(
        "========================================"
    )

    print(
        "status:",
        status,
    )

    print(
        "probes:",
        int(
            c[
                "probe_orders"
            ]
        ),
    )

    print(
        "probes with qualifying trade:",
        int(
            c[
                "probes_with_qualifying_trade"
            ]
        ),
    )

    print(
        "qualifying trade occurrences:",
        int(
            c[
                "qualifying_trade_occurrences"
            ]
        ),
    )

    print(
        "unique qualifying real trades:",
        first[
            "unique_queue_events"
        ],
    )


    print()
    print(
        "--- queue exhaustion ---"
    )

    print(
        "probes reaching queue:",
        int(
            c[
                "probes_reaching_queue"
            ]
        ),
    )

    print(
        "probes exactly exhausting queue:",
        int(
            c[
                "probes_exactly_exhausting_queue"
            ]
        ),
    )

    print(
        "probes exceeding queue:",
        int(
            c[
                "probes_exceeding_queue"
            ]
        ),
    )

    print(
        "mechanically expected positive fills:",
        expected_positive,
    )

    print(
        "M7 actual positive fills:",
        m7_positive_fills,
    )

    print(
        "queue-model discrepancy:",
        (
            "YES"
            if discrepancy
            else "NO"
        ),
    )


    print()
    print(
        "--- depletion thresholds ---"
    )

    for pct in (
        25,
        50,
        75,
        90,
        95,
        99,
    ):

        print(
            f">= {pct}%:",
            int(
                c[
                    f"depletion_ge_{pct}pct"
                ]
            ),
        )


    print()
    print(
        "--- most relevant distributions ---"
    )

    q = s[
        "initial_queue_to_order_size_ratio"
    ]

    print(
        "initial queue / order size:",
        "p50",
        q["p50"],
        "| p90",
        q["p90"],
        "| p99",
        q["p99"],
        "| max",
        q["max"],
    )


    d = s[
        "queue_depletion_ratio_probes_with_trade"
    ]

    print(
        "depletion ratio among probes with trade:",
        "p50",
        d["p50"],
        "| p90",
        d["p90"],
        "| p99",
        d["p99"],
        "| max",
        d["max"],
    )


    tv = s[
        "cumulative_qualifying_trade_volume"
    ]

    print(
        "cumulative qualifying volume:",
        "p50",
        tv["p50"],
        "| p90",
        tv["p90"],
        "| p99",
        tv["p99"],
        "| max",
        tv["max"],
    )


    print()
    print(
        "--- continuity truncation ---"
    )

    print(
        "invalidated probes:",
        int(
            c[
                "invalidated_probes"
            ]
        ),
    )

    print(
        "invalidated before any qualifying trade:",
        int(
            c[
                "invalidated_before_any_qualifying_trade"
            ]
        ),
    )

    print(
        "invalidated after qualifying trade:",
        int(
            c[
                "invalidated_after_qualifying_trade"
            ]
        ),
    )

    print(
        "generation crossings:",
        int(
            c[
                "generation_crossings"
            ]
        ),
    )

    print(
        "connection changes:",
        int(
            c[
                "connection_changes"
            ]
        ),
    )


    print()
    print(
        "--- integrity ---"
    )

    print(
        "M7 unique-event reproduction:",
        (
            "PASS"
            if integrity[
                "m7_unique_queue_event_reproduction"
            ]
            else "FAIL"
        ),
    )

    print(
        "M7 invalidation reproduction:",
        (
            "PASS"
            if integrity[
                "m7_invalidation_reproduction"
            ]
            else "FAIL"
        ),
    )

    print(
        "M7 fill reproduction:",
        (
            "PASS"
            if integrity[
                "m7_positive_fill_mechanical_reproduction"
            ]
            else "FAIL"
        ),
    )

    print(
        "duplicate delta keys:",
        duplicate_deltas,
    )

    print(
        "duplicate trade keys:",
        duplicate_trades,
    )

    print(
        "lookahead events:",
        int(
            c[
                "lookahead_or_predecision_event"
            ]
        ),
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
        "diagnostic SHA:",
        first[
            "diagnostic_sha256"
        ],
    )


    print()
    print(
        "--- conclusion ---"
    )

    print(
        interpretation
    )


    print()
    print(
        "--- boundary ---"
    )

    print(
        "60s horizon changed: NO"
    )

    print(
        "second horizon tested: NO"
    )

    print(
        "queue semantics changed: NO"
    )

    print(
        "cancel-ahead inference: NO"
    )

    print(
        "trade-through inference: NO"
    )

    print(
        "F19 read: NO"
    )

    print(
        "F20 read: NO"
    )

    print(
        "markout: NO"
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
