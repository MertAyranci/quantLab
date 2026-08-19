from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

from research import h6_m1_validate as v


def dt(text: str):
    return datetime.fromisoformat(text).astimezone(timezone.utc)


def registry():
    return [{
        "condition_id": "c1",
        "p1_token_id": "p1",
        "p0_token_id": "p0",
        "game_start_time_utc": "2026-01-01T00:30:00+00:00",
    }]


def book(token: str, bid_size="10", ask_size="10", ts="1000"):
    return {
        "asset_id": token,
        "bids": [{"price": "0.40", "size": bid_size}],
        "asks": [{"price": "0.60", "size": ask_size}],
        "tick_size": "0.01",
        "timestamp": ts,
    }


def rest_row(token: str, reason="connect", matched=None, generation=1):
    row = {
        "connection_id": "conn",
        "reason": reason,
        "request_started_at_utc": "2026-01-01T00:00:00+00:00",
        "response_received_at_utc": "2026-01-01T00:00:01+00:00",
        "token": token,
        "book_generation": generation,
        "book": book(token),
    }
    if matched is not None:
        row["matched_local_state_before"] = matched
    return row


def raw_row(payload, when="2026-01-01T00:10:00+00:00", seq=1):
    raw = json.dumps(payload, separators=(",", ":"))
    return {
        "capture_time": when,
        "connection_id": "conn",
        "frame_sequence": seq,
        "raw_frame_sha256": hashlib.sha256(raw.encode()).hexdigest(),
        "raw": raw,
    }


def base_rest():
    return [rest_row("p1"), rest_row("p0")]


def test_valid_stationary_size_transition_counts_once():
    payload = {
        "event_type": "price_change",
        "timestamp": "1001",
        "price_changes": [
            {
                "asset_id": "p1",
                "side": "BUY",
                "price": "0.40",
                "size": "20",
                "best_bid": "0.40",
                "best_ask": "0.60",
            },
            {
                "asset_id": "p1",
                "side": "BUY",
                "price": "0.40",
                "size": "30",
                "best_bid": "0.40",
                "best_ask": "0.60",
            },
        ],
    }
    out = v.Replay(registry(), [raw_row(payload)], base_rest()).replay()
    assert out["transition_count_total"] == 1
    assert out["transition_count_by_market"]["c1"] == 1


def test_price_move_is_not_primary_transition():
    payload = {
        "event_type": "price_change",
        "timestamp": "1001",
        "price_changes": [{
            "asset_id": "p1",
            "side": "BUY",
            "price": "0.50",
            "size": "20",
            "best_bid": "0.50",
            "best_ask": "0.60",
        }],
    }
    out = v.Replay(registry(), [raw_row(payload)], base_rest()).replay()
    assert out["transition_count_total"] == 0


def test_venue_tob_mismatch_frame_is_excluded():
    payload = {
        "event_type": "price_change",
        "timestamp": "1001",
        "price_changes": [{
            "asset_id": "p1",
            "side": "BUY",
            "price": "0.40",
            "size": "20",
            "best_bid": "0.41",
            "best_ask": "0.60",
        }],
    }
    out = v.Replay(registry(), [raw_row(payload)], base_rest()).replay()
    assert out["transition_count_total"] == 0
    assert out["venue_tob_mismatch_tokens"] == 1


def test_stale_ws_mutation_is_ignored():
    payload = {
        "event_type": "price_change",
        "timestamp": "999",
        "price_changes": [{
            "asset_id": "p1",
            "side": "BUY",
            "price": "0.40",
            "size": "20",
            "best_bid": "0.40",
            "best_ask": "0.60",
        }],
    }
    out = v.Replay(registry(), [raw_row(payload)], base_rest()).replay()
    assert out["transition_count_total"] == 0


def test_reconciliation_mismatch_increments_generation_not_transition():
    rows = base_rest()
    mismatch = rest_row(
        "p1",
        reason="reconciliation_check",
        matched=False,
        generation=2,
    )
    mismatch["response_received_at_utc"] = "2026-01-01T00:10:00+00:00"
    mismatch["request_started_at_utc"] = "2026-01-01T00:09:59+00:00"
    mismatch["book"] = book("p1", bid_size="20", ts="1001")
    rows.append(mismatch)
    out = v.Replay(registry(), [], rows).replay()
    assert out["rest_reconciliation_mismatches"] == 1
    assert out["transition_count_total"] == 0


def test_in_play_frame_never_enters_primary_count():
    payload = {
        "event_type": "price_change",
        "timestamp": "1001",
        "price_changes": [{
            "asset_id": "p1",
            "side": "BUY",
            "price": "0.40",
            "size": "20",
            "best_bid": "0.40",
            "best_ask": "0.60",
        }],
    }
    out = v.Replay(
        registry(),
        [raw_row(payload, when="2026-01-01T00:30:01+00:00")],
        base_rest(),
    ).replay()
    assert out["transition_count_total"] == 0


def test_coverage_is_wall_clock_and_capped_at_game_start():
    out = v.Replay(registry(), [], base_rest()).replay()
    assert out["aggregate_coverage_fraction"] > 0.999
    assert out["coverage_fraction_by_market"]["c1"] > 0.999


def test_raw_sha_and_normalized_lineage_checks():
    r = raw_row({"event_type": "best_bid_ask"})
    n = {
        "connection_id": r["connection_id"],
        "frame_sequence": r["frame_sequence"],
        "raw_frame_sha256": r["raw_frame_sha256"],
    }
    out = v.verify_lineage([r], [n])
    assert out == {
        "raw_sha_failures": 0,
        "duplicate_raw_keys": 0,
        "normalized_lineage_failures": 0,
    }


def test_reconciliation_interval_gate_detects_over_60_seconds():
    control = [
        {
            "kind": "capture_initialized",
            "capture_time": "2026-01-01T00:00:00+00:00",
        },
        {
            "kind": "rest_reconcile_complete",
            "capture_time": "2026-01-01T00:01:01+00:00",
        },
    ]
    out = v.reconciliation_interval_metrics(
        control,
        dt("2026-01-01T00:01:30+00:00"),
    )
    assert out["max_interval_seconds"] == 61.0


def test_verdict_inconclusive_when_integrity_passes_but_density_fails():
    contract = {
        "success_gates": {
            "distinct_markets_minimum": 5,
            "generation_or_connection_mixing": 0,
            "future_backfill": 0,
            "in_play_primary_transitions": 0,
            "reconciliation_interval_seconds_max": 60,
            "valid_two_sided_l1_wall_clock_coverage_min_fraction": 0.95,
            "valid_price_stationary_nonzero_imbalance_transitions_minimum": 500,
            "transition_distinct_markets_minimum": 5,
            "markets_with_at_least_25_valid_transitions_minimum": 4,
        }
    }
    replay = {
        "distinct_markets": 5,
        "initial_full_p1_snapshot_count": 5,
        "generation_or_connection_mixing": 0,
        "future_backfill": 0,
        "in_play_primary_transitions": 0,
        "reconstruction_integrity_errors": [],
        "state_errors": 0,
        "rest_flag_crosscheck_failures": 0,
        "rest_generation_crosscheck_failures": 0,
        "post_frame_crosscheck_failures": 0,
        "aggregate_coverage_fraction": 0.99,
        "transition_count_total": 499,
        "transition_distinct_markets": 5,
        "markets_with_at_least_25_transitions": 4,
    }
    lineage = {
        "raw_sha_failures": 0,
        "duplicate_raw_keys": 0,
        "normalized_lineage_failures": 0,
    }
    reconciliation = {
        "errors": [],
        "max_interval_seconds": 30,
    }
    verdict, _ = v.verdict_from_metrics(
        contract=contract,
        replay=replay,
        lineage=lineage,
        reconciliation=reconciliation,
        deterministic_replay_pass=True,
    )
    assert verdict == "INCONCLUSIVE_ENGINEERING"
