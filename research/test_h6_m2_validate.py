from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

from research import h6_m2_validate as v


def dt(
    text: str,
):
    return datetime.fromisoformat(
        text
    ).astimezone(
        timezone.utc
    )


def registry():
    return [
        {
            "condition_id":
                "c1",

            "p1_token_id":
                "p1",

            "p0_token_id":
                "p0",

            "game_start_time_utc":
                "2026-01-01T00:30:00+00:00",
        }
    ]


def book(
    token: str,
    bid_size="10",
    ask_size="10",
    ts="1000",
):
    return {
        "asset_id":
            token,

        "bids": [
            {
                "price": "0.40",
                "size": bid_size,
            }
        ],

        "asks": [
            {
                "price": "0.60",
                "size": ask_size,
            }
        ],

        "tick_size":
            "0.01",

        "timestamp":
            ts,
    }


def rest_row(
    token: str,
):
    return {
        "connection_id":
            "conn",

        "reason":
            "connect",

        "request_started_at_utc":
            "2025-12-31T23:58:59+00:00",

        "response_received_at_utc":
            "2025-12-31T23:59:00+00:00",

        "token":
            token,

        "book_generation":
            1,

        "book":
            book(token),
    }


def raw_row(
    payload,
    when="2026-01-01T00:10:00+00:00",
    seq=1,
):
    raw = json.dumps(
        payload,
        separators=(",", ":"),
    )

    return {
        "capture_time":
            when,

        "connection_id":
            "conn",

        "frame_sequence":
            seq,

        "raw_frame_sha256":
            hashlib.sha256(
                raw.encode()
            ).hexdigest(),

        "raw":
            raw,
    }


def passing_replay():
    return {
        "distinct_markets":
            30,

        "initial_full_p1_snapshot_count":
            30,

        "generation_or_connection_mixing":
            0,

        "future_backfill":
            0,

        "in_play_primary_transitions":
            0,

        "reconstruction_integrity_errors":
            [],

        "state_errors":
            0,

        "rest_flag_crosscheck_failures":
            0,

        "rest_generation_crosscheck_failures":
            0,

        "post_frame_crosscheck_failures":
            0,

        "aggregate_coverage_fraction":
            0.99,

        "transition_count_total":
            5000,

        "transition_distinct_markets":
            20,

        "markets_with_at_least_25_transitions":
            20,
    }


def passing_lineage():
    return {
        "raw_sha_failures":
            0,

        "duplicate_raw_keys":
            0,

        "normalized_lineage_failures":
            0,
    }


def passing_reconciliation():
    return {
        "errors":
            [],

        "max_interval_seconds":
            30.0,
    }


def test_m2_uses_pinned_m1_replay_core():
    assert (
        v.sha256_file(
            v.REPO
            / "research"
            / "h6_m1_validate.py"
        )
        ==
        v.EXPECTED_M1_REPLAY_CORE_SHA
    )


def test_core_replay_stationary_size_transition():
    payload = {
        "event_type":
            "price_change",

        "timestamp":
            "1001",

        "price_changes": [
            {
                "asset_id":
                    "p1",

                "side":
                    "BUY",

                "price":
                    "0.40",

                "size":
                    "20",

                "best_bid":
                    "0.40",

                "best_ask":
                    "0.60",
            }
        ],
    }

    out = v.core.Replay(
        registry(),
        [
            raw_row(
                payload
            )
        ],
        [
            rest_row("p1"),
            rest_row("p0"),
        ],
    ).replay()

    assert (
        out[
            "transition_count_total"
        ]
        == 1
    )


def test_active_reconciliation_ignores_idle_gap():
    control = [
        {
            "kind":
                "connection_start",

            "capture_time":
                "2026-01-01T00:00:00+00:00",

            "connection_id":
                "c1",
        },
        {
            "kind":
                "capture_initialized",

            "capture_time":
                "2026-01-01T00:00:01+00:00",

            "connection_id":
                "c1",
        },
        {
            "kind":
                "rest_reconcile_complete",

            "capture_time":
                "2026-01-01T00:00:31+00:00",

            "connection_id":
                "c1",
        },
        {
            "kind":
                "active_token_set_change",

            "capture_time":
                "2026-01-01T00:00:50+00:00",

            "connection_id":
                "c1",
        },

        # Large inactive gap is intentionally
        # irrelevant to reconciliation cadence.
        {
            "kind":
                "connection_start",

            "capture_time":
                "2026-01-01T01:00:00+00:00",

            "connection_id":
                "c2",
        },
        {
            "kind":
                "capture_initialized",

            "capture_time":
                "2026-01-01T01:00:01+00:00",

            "connection_id":
                "c2",
        },
        {
            "kind":
                "rest_reconcile_complete",

            "capture_time":
                "2026-01-01T01:00:31+00:00",

            "connection_id":
                "c2",
        },
        {
            "kind":
                "active_token_set_change",

            "capture_time":
                "2026-01-01T01:00:50+00:00",

            "connection_id":
                "c2",
        },
    ]

    out = (
        v.active_reconciliation_metrics(
            control,
            dt(
                "2026-01-01T02:00:00+00:00"
            ),
        )
    )

    assert out["errors"] == []

    assert (
        out[
            "initialized_connection_count"
        ]
        == 2
    )

    assert (
        out[
            "max_interval_seconds"
        ]
        == 30.0
    )


def test_short_connection_needs_no_reconcile():
    control = [
        {
            "kind":
                "capture_initialized",

            "capture_time":
                "2026-01-01T00:00:00+00:00",

            "connection_id":
                "c1",
        },
        {
            "kind":
                "active_token_set_change",

            "capture_time":
                "2026-01-01T00:00:20+00:00",

            "connection_id":
                "c1",
        },
    ]

    out = (
        v.active_reconciliation_metrics(
            control,
            dt(
                "2026-01-01T01:00:00+00:00"
            ),
        )
    )

    assert out["errors"] == []

    assert (
        out[
            "max_interval_seconds"
        ]
        == 20.0
    )


def test_reconciliation_over_60_detected():
    control = [
        {
            "kind":
                "capture_initialized",

            "capture_time":
                "2026-01-01T00:00:00+00:00",

            "connection_id":
                "c1",
        },
        {
            "kind":
                "rest_reconcile_complete",

            "capture_time":
                "2026-01-01T00:01:01+00:00",

            "connection_id":
                "c1",
        },
        {
            "kind":
                "active_token_set_change",

            "capture_time":
                "2026-01-01T00:01:10+00:00",

            "connection_id":
                "c1",
        },
    ]

    out = (
        v.active_reconciliation_metrics(
            control,
            dt(
                "2026-01-01T01:00:00+00:00"
            ),
        )
    )

    assert (
        out[
            "max_interval_seconds"
        ]
        == 61.0
    )


def test_verdict_pass():
    verdict, gates = (
        v.verdict_from_metrics(
            contract={},
            replay=passing_replay(),
            lineage=passing_lineage(),
            reconciliation=(
                passing_reconciliation()
            ),
            deterministic_replay_pass=True,
            capture_stop_reason=(
                v.NORMAL_STOP_REASON
            ),
        )
    )

    assert (
        verdict
        ==
        "PASS_ENGINEERING_AND_DATA_SUFFICIENCY"
    )

    assert all(
        gates[
            "integrity"
        ].values()
    )

    assert all(
        gates[
            "sufficiency"
        ].values()
    )


def test_verdict_inconclusive_on_density():
    replay = passing_replay()

    replay[
        "transition_count_total"
    ] = 4999

    verdict, gates = (
        v.verdict_from_metrics(
            contract={},
            replay=replay,
            lineage=passing_lineage(),
            reconciliation=(
                passing_reconciliation()
            ),
            deterministic_replay_pass=True,
            capture_stop_reason=(
                v.NORMAL_STOP_REASON
            ),
        )
    )

    assert (
        verdict
        ==
        "INCONCLUSIVE_DEVELOPMENT_DATA"
    )

    assert (
        gates[
            "integrity"
        ]["capture_completed_normally"]
        is True
    )


def test_verdict_fail_integrity_on_lineage():
    lineage = passing_lineage()

    lineage[
        "raw_sha_failures"
    ] = 1

    verdict, _ = (
        v.verdict_from_metrics(
            contract={},
            replay=passing_replay(),
            lineage=lineage,
            reconciliation=(
                passing_reconciliation()
            ),
            deterministic_replay_pass=True,
            capture_stop_reason=(
                v.NORMAL_STOP_REASON
            ),
        )
    )

    assert (
        verdict
        ==
        "FAIL_INTEGRITY"
    )


def test_abnormal_capture_stop_fails_integrity():
    verdict, _ = (
        v.verdict_from_metrics(
            contract={},
            replay=passing_replay(),
            lineage=passing_lineage(),
            reconciliation=(
                passing_reconciliation()
            ),
            deterministic_replay_pass=True,
            capture_stop_reason=(
                "DISK_SAFETY"
            ),
        )
    )

    assert (
        verdict
        ==
        "FAIL_INTEGRITY"
    )


def test_validator_boundary_contains_no_scientific_readout():
    source = (
        v.REPO
        / "research"
        / "h6_m2_validate.py"
    ).read_text(
        encoding="utf-8"
    )

    assert (
        "numpy.random"
        not in source
    )

    assert (
        "statsmodels"
        not in source
    )
