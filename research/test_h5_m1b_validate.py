from __future__ import annotations

import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "research"
    / "h5_m1b_validate.py"
)

spec = importlib.util.spec_from_file_location(
    "h5_m1b_validate",
    MODULE_PATH,
)
m = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(m)


def test_frozen_constants():
    assert m.MIN_MARKETS == 5
    assert m.MIN_ACCEPTED_EXTERNAL_STATES == 100
    assert m.MIN_FRESH_COVERAGE == 0.80
    assert m.MIN_PM_COVERAGE == 0.95
    assert m.CANDIDATE_HORIZONS_SECONDS == [1, 3, 5, 10, 30, 60]
    assert m.EXPECTED_REGISTRAR_SHA == (
        "fe6bafa6c21aa6dbc7490d6b82109a1"
        "90f0f563b8e42e93557ce9e245b6e5719"
    )
    assert m.EXPECTED_M0_EXCLUSIONS_SHA == (
        "fb4a268f3148e746e31232c658cdc879"
        "3c2e082a73c5a8260af6779d89b1949a"
    )


def test_parse_dt_requires_timezone():
    assert m.parse_dt("2026-08-17T00:00:00Z") is not None
    assert m.parse_dt("2026-08-17T00:00:00") is None
    assert m.parse_dt("garbage") is None


def test_percentile_empty():
    assert m.percentile([], 0.95) is None


def test_summarize_numeric():
    out = m.summarize_numeric([1, 2, 3, 4])
    assert out["n"] == 4
    assert out["min"] == 1
    assert out["median"] == 2.5
    assert out["max"] == 4


def test_tob_snapshot_presence():
    bid, ask = m.tob_presence_from_payload(
        "rest_resync_book",
        {
            "bids": [{"price": "0.4"}],
            "asks": [{"price": "0.6"}],
        },
        None,
    )
    assert bid is True
    assert ask is True


def test_tob_snapshot_one_sided():
    bid, ask = m.tob_presence_from_payload(
        "book",
        {"bids": [], "asks": [{"price": "0.6"}]},
        None,
    )
    assert bid is False
    assert ask is True


def test_tob_best_bid_ask_presence_only():
    bid, ask = m.tob_presence_from_payload(
        "best_bid_ask",
        {"best_bid": "0.4", "best_ask": "0.6"},
        None,
    )
    assert (bid, ask) == (True, True)


def test_tob_price_change_preserves_other_side():
    bid, ask = m.tob_presence_from_payload(
        "price_change",
        {"best_bid": "0.41"},
        (True, True),
    )
    assert (bid, ask) == (True, True)


def test_boundary_rejects_true():
    boundary = {key: False for key in m.BOUNDARY_FALSE_KEYS}
    boundary["pnl_calculated"] = True
    with pytest.raises(RuntimeError, match="pnl_calculated"):
        m.validate_boundary(boundary, where="test")


def test_boundary_accepts_all_false():
    boundary = {key: False for key in m.BOUNDARY_FALSE_KEYS}
    m.validate_boundary(boundary, where="test")


def test_source_update_intervals_deduplicates_same_source_timestamp():
    rows = [
        {
            "odds_game_id": "g1",
            "exchange_family": "betfair_ex_uk",
            "family_source_timestamp_utc": "2026-08-17T00:00:00Z",
        },
        {
            "odds_game_id": "g1",
            "exchange_family": "betfair_ex_uk",
            "family_source_timestamp_utc": "2026-08-17T00:00:00Z",
        },
        {
            "odds_game_id": "g1",
            "exchange_family": "betfair_ex_uk",
            "family_source_timestamp_utc": "2026-08-17T00:00:20Z",
        },
    ]
    out = m.source_update_intervals(rows)
    assert out["betfair_ex_uk"]["n"] == 1
    assert out["betfair_ex_uk"]["median"] == 20


def test_pm_checkpoint_valid_after_snapshot():
    consensus = [
        {
            "condition_id": "c1",
            "response_received_at_utc": "2026-08-17T00:00:10Z",
        }
    ]
    pm = [
        {
            "capture_time": "2026-08-17T00:00:01Z",
            "token": "p1",
            "kind": "rest_resync_book",
            "connection_id": "a",
            "book_generation": 1,
            "payload": {
                "bids": [{"price": "0.4"}],
                "asks": [{"price": "0.6"}],
            },
        }
    ]
    control = [
        {
            "capture_time": "2026-08-17T00:00:00Z",
            "kind": "connection_start",
            "connection_id": "a",
        }
    ]
    out = m.pm_checkpoint_coverage(
        consensus_rows=consensus,
        pm_rows=pm,
        pm_control=control,
        p1_by_condition={"c1": "p1"},
    )
    assert out["eligible_checkpoints"] == 1
    assert out["valid_checkpoints"] == 1
    assert out["coverage"] == 1


def test_pm_checkpoint_invalid_after_connection_error():
    consensus = [
        {
            "condition_id": "c1",
            "response_received_at_utc": "2026-08-17T00:00:10Z",
        }
    ]
    pm = [
        {
            "capture_time": "2026-08-17T00:00:01Z",
            "token": "p1",
            "kind": "rest_resync_book",
            "connection_id": "a",
            "book_generation": 1,
            "payload": {
                "bids": [{"price": "0.4"}],
                "asks": [{"price": "0.6"}],
            },
        }
    ]
    control = [
        {
            "capture_time": "2026-08-17T00:00:00Z",
            "kind": "connection_start",
            "connection_id": "a",
        },
        {
            "capture_time": "2026-08-17T00:00:05Z",
            "kind": "connection_error",
            "connection_id": "a",
        },
    ]
    out = m.pm_checkpoint_coverage(
        consensus_rows=consensus,
        pm_rows=pm,
        pm_control=control,
        p1_by_condition={"c1": "p1"},
    )
    assert out["valid_checkpoints"] == 0
    assert out["invalid_by_reason"]["not_initialized_or_gap"] == 1


def test_pm_checkpoint_future_snapshot_does_not_backfill():
    consensus = [
        {
            "condition_id": "c1",
            "response_received_at_utc": "2026-08-17T00:00:10Z",
        }
    ]
    pm = [
        {
            "capture_time": "2026-08-17T00:00:11Z",
            "token": "p1",
            "kind": "rest_resync_book",
            "connection_id": "a",
            "book_generation": 1,
            "payload": {
                "bids": [{"price": "0.4"}],
                "asks": [{"price": "0.6"}],
            },
        }
    ]
    out = m.pm_checkpoint_coverage(
        consensus_rows=consensus,
        pm_rows=pm,
        pm_control=[],
        p1_by_condition={"c1": "p1"},
    )
    assert out["valid_checkpoints"] == 0


def test_pm_checkpoint_one_sided_not_valid():
    consensus = [
        {
            "condition_id": "c1",
            "response_received_at_utc": "2026-08-17T00:00:10Z",
        }
    ]
    pm = [
        {
            "capture_time": "2026-08-17T00:00:01Z",
            "token": "p1",
            "kind": "rest_resync_book",
            "connection_id": "a",
            "book_generation": 1,
            "payload": {
                "bids": [{"price": "0.4"}],
                "asks": [],
            },
        }
    ]
    out = m.pm_checkpoint_coverage(
        consensus_rows=consensus,
        pm_rows=pm,
        pm_control=[],
        p1_by_condition={"c1": "p1"},
    )
    assert out["valid_checkpoints"] == 0
    assert out["invalid_by_reason"]["not_two_sided"] == 1


def test_pm_message_diagnostics_counts_only_timing_and_kinds():
    pm = [
        {
            "capture_time": "2026-08-17T00:00:00Z",
            "kind": "book",
        },
        {
            "capture_time": "2026-08-17T00:00:02Z",
            "kind": "price_change",
        },
    ]
    control = [
        {
            "capture_time": "2026-08-17T00:00:00Z",
            "kind": "connection_start",
        }
    ]
    out = m.pm_message_diagnostics(pm, control)
    assert out["normalized_events"] == 2
    assert out["connection_starts"] == 1
    assert out["inter_event_gap_seconds"]["median"] == 2


def test_write_report_exclusive_refuses_overwrite(tmp_path):
    path = tmp_path / "report.json"
    m.write_report_exclusive({"a": 1}, output=path)
    with pytest.raises(FileExistsError):
        m.write_report_exclusive({"a": 2}, output=path)


def test_no_network_libraries_imported():
    source = MODULE_PATH.read_text(encoding="utf-8")
    assert "import httpx" not in source
    assert "import requests" not in source
    assert "import websockets" not in source


def test_prohibited_metric_words_not_used_as_computation_functions():
    source = MODULE_PATH.read_text(encoding="utf-8")
    assert "def calculate_innovation" not in source
    assert "def calculate_response" not in source
    assert "def calculate_pnl" not in source
    assert "consensus_p1_probability" not in source


def test_config_validation_does_not_require_registry(monkeypatch):
    monkeypatch.setattr(m, "sha256_file", lambda path: {
        m.M1A_CONTRACT: m.EXPECTED_M1A_SHA,
        m.COLLECTOR: m.EXPECTED_COLLECTOR_SHA,
    }.get(path, "x"))
    monkeypatch.setattr(Path, "is_file", lambda self: True)
    monkeypatch.setattr(
        m,
        "read_json",
        lambda path: {
            "m1_success_requirements": {
                "minimum_distinct_markets": 5,
                "minimum_accepted_external_states": 100,
            },
            "candidate_horizons_seconds": [1, 3, 5, 10, 30, 60],
        },
    )
    out = m.load_contract(require_committed_self=False)
    assert out["candidate_horizons_seconds"] == [1, 3, 5, 10, 30, 60]


def test_all_frozen_m1_gate_names_present():
    source = MODULE_PATH.read_text(encoding="utf-8")
    for gate in (
        "minimum_distinct_markets",
        "minimum_accepted_external_states",
        "fresh_external_family_coverage",
        "pm_book_coverage",
        "team_mapping_failures",
        "f19_overlap",
        "h2_v2_burned_overlap",
        "clock_fields_missing",
        "in_play_records_admitted",
    ):
        assert f'"{gate}"' in source
