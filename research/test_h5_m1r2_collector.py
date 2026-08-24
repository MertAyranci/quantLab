from __future__ import annotations

import copy
import importlib.util
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest


COLLECTOR_PATH = (
    Path(__file__).resolve().parents[1]
    / "collectors"
    / "odds"
    / "h5_m1r2_collector.py"
)

spec = importlib.util.spec_from_file_location(
    "h5_m1r2_collector",
    COLLECTOR_PATH,
)
assert spec is not None and spec.loader is not None
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def reg_row(i: int = 1, *, start=None):
    start = start or datetime(2026, 8, 18, 1, tzinfo=timezone.utc)
    return {
        "study": "H5",
        "milestone": "H5-M1R2",
        "registration_index": i,
        "odds_game_id": f"event-{i}",
        "condition_id": f"0x{i:064x}",
        "canonical_commence_time": start.isoformat(),
        "home_team": f"Home {i}",
        "away_team": f"Away {i}",
        "p1_team": f"Away {i}",
        "p0_team": f"Home {i}",
        "p1_token_id": f"p1-{i}",
        "p0_token_id": f"p0-{i}",
    }



def runtime_bundle(rows):
    return {
        "rows":
            rows,

        "stage1_registry_sha256":
            "r" * 64,

        "stage1_receipt_sha256":
            "s" * 64,

        "stage2_map_sha256":
            "m" * 64,

        "stage2_receipt_sha256":
            "t" * 64,

        "r2_preregistration_sha256":
            "p" * 64,

        "stage2_execution_sha256":
            "e" * 64,

        "stage2_attempt_sha256":
            "a" * 64,

        "h2_identity_sha256":
            "h" * 64,

        "stage1_registered_market_count":
            10,

        "acquisition_market_count":
            len(rows),
    }


def baseball_reg():
    return {
        "study": "H5",
        "milestone": "H5-M1R2",
        "registration_index": 1,
        "odds_game_id": "event-123",
        "condition_id": "0xabc",
        "canonical_commence_time": "2026-08-18T01:00:00+00:00",
        "home_team": "New York Yankees",
        "away_team": "Boston Red Sox",
        "p1_team": "Boston Red Sox",
        "p0_team": "New York Yankees",
        "p1_token_id": "p1",
        "p0_token_id": "p0",
    }


def family_book(key: str, last_update: datetime, *, missing_p0_lay=False):
    back = [
        {"name": "Boston Red Sox", "price": 2.0},
        {"name": "New York Yankees", "price": 1.9},
    ]
    lay = [
        {"name": "Boston Red Sox", "price": 2.04},
        {"name": "New York Yankees", "price": 1.94},
    ]
    if missing_p0_lay:
        lay = lay[:1]
    return {
        "key": key,
        "last_update": last_update.isoformat().replace("+00:00", "Z"),
        "markets": [
            {"key": "h2h", "outcomes": back},
            {"key": "h2h_lay", "outcomes": lay},
        ],
    }


def game(received: datetime, *, ages=(10, 20, 30)):
    return {
        "id": "event-123",
        "home_team": "New York Yankees",
        "away_team": "Boston Red Sox",
        "bookmakers": [
            family_book(name, received - timedelta(seconds=age))
            for name, age in zip(m.BOOKMAKERS, ages)
        ],
    }


def test_frozen_external_constants():
    assert m.BOOKMAKERS == ["betfair_ex_uk", "matchbook", "smarkets"]
    assert m.MAX_SOURCE_AGE_SECONDS == Decimal("25")
    assert m.MIN_FRESH_FAMILIES == 2
    assert m.POLL_INTERVAL_SECONDS == 10
    assert m.MAX_POLLS == 60
    assert m.INITIAL_RUN_SECONDS == 600
    assert m.MIN_REMAINING_CREDITS == 50
    assert m.MIN_FREE_DISK_BYTES == 4 * 1024**3


def test_safe_odds_params_excludes_secret_and_filters_registry_ids():
    p = m.safe_odds_params(["a", "b"])
    assert "apiKey" not in p
    assert p["eventIds"] == "a,b"
    assert p["markets"] == "h2h"
    assert p["bookmakers"] == "betfair_ex_uk,matchbook,smarkets"


def test_decimal_price_and_probability():
    assert m.decimal_price({"price": 2}) == Decimal("2")
    assert m.decimal_price({"price": 1}) is None
    assert m.probability(Decimal("2")) == Decimal("0.5")


def test_external_parser_requires_two_fresh_complete_families():
    received = datetime(2026, 8, 17, 23, 0, tzinfo=timezone.utc)
    families, consensus = m.parse_external_observation(
        capture_id="c",
        poll_sequence=1,
        reg=baseball_reg(),
        game=game(received, ages=(10, 20, 30)),
        requested_at=received - timedelta(milliseconds=100),
        received_at=received,
        latency_ms=Decimal("100"),
    )
    assert len(families) == 3
    assert [x["family_fresh"] for x in families] == [True, True, False]
    assert consensus["valid_fresh_family_count"] == 2
    assert consensus["families_used"] == m.BOOKMAKERS[:2]
    assert consensus["accepted_external_state"] is True
    assert consensus["consensus_p1_probability"] is not None


def test_source_age_25_is_fresh_and_negative_is_not():
    received = datetime(2026, 8, 17, 23, 0, tzinfo=timezone.utc)
    g = game(received, ages=(25, -1, 40))
    families, consensus = m.parse_external_observation(
        capture_id="c",
        poll_sequence=1,
        reg=baseball_reg(),
        game=g,
        requested_at=received,
        received_at=received,
        latency_ms=Decimal("0"),
    )
    assert families[0]["family_fresh"] is True
    assert families[1]["family_fresh"] is False
    assert consensus["accepted_external_state"] is False


def test_family_complete_requires_both_teams_back_and_lay():
    received = datetime(2026, 8, 17, 23, 0, tzinfo=timezone.utc)
    g = game(received, ages=(10, 40, 40))
    g["bookmakers"][0] = family_book(
        m.BOOKMAKERS[0], received - timedelta(seconds=10), missing_p0_lay=True
    )
    families, _ = m.parse_external_observation(
        capture_id="c",
        poll_sequence=1,
        reg=baseball_reg(),
        game=g,
        requested_at=received,
        received_at=received,
        latency_ms=Decimal("0"),
    )
    assert families[0]["p1_back_decimal"] is not None
    assert families[0]["p1_lay_decimal"] is not None
    assert families[0]["family_pair_complete"] is False
    assert families[0]["family_fresh"] is False


def test_external_parser_rejects_team_mismatch():
    received = datetime(2026, 8, 17, 23, 0, tzinfo=timezone.utc)
    g = game(received)
    g["away_team"] = "Chicago Cubs"
    with pytest.raises(m.MappingError):
        m.parse_external_observation(
            capture_id="c",
            poll_sequence=1,
            reg=baseball_reg(),
            game=g,
            requested_at=received,
            received_at=received,
            latency_ms=Decimal("0"),
        )


def test_external_parser_rejects_in_play_observation():
    received = datetime(2026, 8, 18, 1, 0, tzinfo=timezone.utc)
    with pytest.raises(m.MappingError):
        m.parse_external_observation(
            capture_id="c",
            poll_sequence=1,
            reg=baseball_reg(),
            game=game(received),
            requested_at=received,
            received_at=received,
            latency_ms=Decimal("0"),
        )


def test_duplicate_normalized_outcome_fails_closed():
    market = {
        "outcomes": [
            {"name": "St. Louis Cardinals", "price": 2},
            {"name": "St Louis Cardinals", "price": 2.1},
        ]
    }
    with pytest.raises(m.MappingError):
        m.outcomes_by_norm_name(market)


def test_registry_validation_accepts_5_and_rejects_4():
    rows = [reg_row(i) for i in range(1, 6)]
    m.validate_registry_rows(rows)
    with pytest.raises(RuntimeError):
        m.validate_registry_rows(rows[:4])


def test_registry_validation_rejects_duplicate_condition():
    rows = [reg_row(i) for i in range(1, 6)]
    rows[1]["condition_id"] = rows[0]["condition_id"]
    with pytest.raises(RuntimeError):
        m.validate_registry_rows(rows)


def test_boundary_flags_are_all_false():
    flags = m.boundary_flags()
    assert flags
    assert set(flags.values()) == {False}


def test_capture_writer_is_exclusive_and_raw_bytes_exact(tmp_path, monkeypatch):
    monkeypatch.setattr(m, "REPO", tmp_path)
    monkeypatch.setattr(m, "DATA_ROOT", tmp_path / "data" / "research" / "h5_m1r2" / "capture")
    monkeypatch.setattr(m, "git_head", lambda: "test-head")
    bundle = runtime_bundle(
        [
            reg_row(i)
            for i in range(1, 6)
        ]
    )
    w = m.CaptureWriter("cap", bundle)
    raw = b'{"exact":true}\n'
    meta = w.save_external_raw(1, raw)
    assert meta["sha256"] == m.sha256_bytes(raw)
    assert (w.raw_dir / "poll_001.json").read_bytes() == raw
    w.finalize(stop_reason="TEST")
    with pytest.raises(FileExistsError):
        m.CaptureWriter("cap", bundle)


def _pm_bundle(start: datetime):
    rows = [
        reg_row(
            i,
            start=start,
        )
        for i in range(1, 6)
    ]

    return runtime_bundle(
        rows
    )


def test_pm_raw_frame_exact_and_required_event_normalized(tmp_path, monkeypatch):
    monkeypatch.setattr(m, "REPO", tmp_path)
    monkeypatch.setattr(m, "DATA_ROOT", tmp_path / "data" / "research" / "h5_m1r2" / "capture")
    monkeypatch.setattr(m, "git_head", lambda: "test-head")
    start = datetime(2026, 8, 18, 1, tzinfo=timezone.utc)
    bundle = _pm_bundle(start)
    w = m.CaptureWriter("pmcap", bundle)
    pm = m.H5PMCapture(bundle=bundle, writer=w)
    pm.conn_id = "conn"
    raw = json.dumps(
        {
            "event_type": "best_bid_ask",
            "asset_id": "p1-1",
            "best_bid": "0.49",
            "best_ask": "0.51",
            "timestamp": "123",
        },
        separators=(",", ":"),
    )
    pm.handle_raw(raw, capture_time=start - timedelta(seconds=10))
    w.flush()
    raw_lines = w.paths["pm_raw_frames"].read_text().splitlines()
    normalized = w.paths["pm_normalized"].read_text().splitlines()
    assert len(raw_lines) == 1
    assert json.loads(raw_lines[0])["raw"] == raw
    assert len(normalized) == 1
    rec = json.loads(normalized[0])
    assert rec["kind"] == "best_bid_ask"
    assert rec["token"] == "p1-1"
    assert rec["book_generation"] == 0
    w.finalize(stop_reason="TEST")


def test_pm_post_start_event_is_raw_but_not_admitted_normalized(tmp_path, monkeypatch):
    monkeypatch.setattr(m, "REPO", tmp_path)
    monkeypatch.setattr(m, "DATA_ROOT", tmp_path / "data" / "research" / "h5_m1r2" / "capture")
    monkeypatch.setattr(m, "git_head", lambda: "test-head")
    start = datetime(2026, 8, 18, 1, tzinfo=timezone.utc)
    bundle = _pm_bundle(start)
    w = m.CaptureWriter("post", bundle)
    pm = m.H5PMCapture(bundle=bundle, writer=w)
    pm.conn_id = "conn"
    raw = json.dumps(
        {
            "event_type": "book",
            "asset_id": "p1-1",
            "bids": [],
            "asks": [],
        }
    )
    pm.handle_raw(raw, capture_time=start + timedelta(seconds=1))
    w.flush()
    assert len(w.paths["pm_raw_frames"].read_text().splitlines()) == 1
    assert w.paths["pm_normalized"].read_text() == ""
    w.finalize(stop_reason="TEST")


def test_resolution_event_not_normalized_with_payload(tmp_path, monkeypatch):
    monkeypatch.setattr(m, "REPO", tmp_path)
    monkeypatch.setattr(m, "DATA_ROOT", tmp_path / "data" / "research" / "h5_m1r2" / "capture")
    monkeypatch.setattr(m, "git_head", lambda: "test-head")
    start = datetime(2026, 8, 18, 1, tzinfo=timezone.utc)
    bundle = _pm_bundle(start)
    w = m.CaptureWriter("resolved", bundle)
    pm = m.H5PMCapture(bundle=bundle, writer=w)
    pm.conn_id = "conn"
    raw = json.dumps(
        {
            "event_type": "market_resolved",
            "asset_id": "p1-1",
            "winner": "secret",
        }
    )
    pm.handle_raw(raw, capture_time=start - timedelta(seconds=1))
    w.flush()
    assert w.paths["pm_normalized"].read_text() == ""
    control = w.paths["pm_control"].read_text()
    assert "secret" not in control
    assert "forbidden_resolution_event_ignored" in control
    w.finalize(stop_reason="TEST")


def test_collector_has_no_database_or_f19_f20_data_paths():
    source = COLLECTOR_PATH.read_text(encoding="utf-8")
    assert "psycopg" not in source
    assert "data/prospective/rn1_f19" not in source
    assert "data/research/f20" not in source
    assert "data/prospective/rn1_f20" not in source


def test_consensus_record_contains_no_change_or_response_fields():
    received = datetime(2026, 8, 17, 23, 0, tzinfo=timezone.utc)
    _, consensus = m.parse_external_observation(
        capture_id="c",
        poll_sequence=1,
        reg=baseball_reg(),
        game=game(received, ages=(10, 20, 30)),
        requested_at=received,
        received_at=received,
        latency_ms=Decimal("0"),
    )
    keys = " ".join(consensus).lower()
    assert "innovation" not in keys
    assert "pm_response" not in keys
    assert "change" not in keys
    assert "future" not in keys



def test_collector_uses_frozen_r2_runtime_loader():
    source = COLLECTOR_PATH.read_text(
        encoding="utf-8"
    )

    assert (
        "import h5_m1r2_runtime as compat"
        in source
    )

    assert (
        "import h5_m1_collector_compat"
        not in source
    )

    assert (
        "EXPECTED_R2_RUNTIME_SHA"
        in source
    )

    assert (
        "EXPECTED_R2_RUNTIME_COMMIT"
        in source
    )


def test_registry_bundle_delegates_to_compat(
    monkeypatch,
):
    sentinel = {
        "rows":
            ["sentinel"]
    }

    monkeypatch.setattr(
        m,
        "validate_frozen_engineering_inputs",
        lambda **kwargs: None,
    )

    monkeypatch.setattr(
        m.compat,
        "load_runtime_bundle",
        lambda: sentinel,
    )

    result = (
        m.load_registry_bundle()
    )

    assert result is sentinel


def test_capture_start_records_split_provenance(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(
        m,
        "REPO",
        tmp_path,
    )

    monkeypatch.setattr(
        m,
        "DATA_ROOT",
        (
            tmp_path
            / "data"
            / "research"
            / "h5_m1r2"
            / "capture"
        ),
    )

    monkeypatch.setattr(
        m,
        "git_head",
        lambda: "test-head",
    )

    bundle = runtime_bundle(
        [
            reg_row(i)
            for i in range(1, 6)
        ]
    )

    writer = m.CaptureWriter(
        "prov",
        bundle,
    )

    start = json.loads(
        (
            writer.capture_dir
            / "capture_start.json"
        ).read_text(
            encoding="utf-8"
        )
    )

    assert (
        start[
            "r2_runtime_sha256"
        ]
        ==
        m.EXPECTED_R2_RUNTIME_SHA
    )

    assert (
        start[
            "r2_runtime_commit"
        ]
        ==
        m.EXPECTED_R2_RUNTIME_COMMIT
    )

    assert (
        start[
            "stage1_registry_sha256"
        ]
        ==
        "r" * 64
    )

    assert (
        start[
            "stage1_receipt_sha256"
        ]
        ==
        "s" * 64
    )

    assert (
        start[
            "stage2_map_sha256"
        ]
        ==
        "m" * 64
    )

    assert (
        start[
            "stage2_receipt_sha256"
        ]
        ==
        "t" * 64
    )

    assert (
        start[
            "stage1_registered_market_count"
        ]
        == 10
    )

    assert (
        start[
            "acquisition_market_count"
        ]
        == 5
    )

    writer.finalize(
        stop_reason="TEST"
    )


def test_r2_data_root_is_dedicated_capture_namespace():
    assert (
        m.DATA_ROOT
        ==
        (
            Path(__file__).resolve().parents[1]
            / "data"
            / "research"
            / "h5_m1r2"
            / "capture"
        )
    )


def test_actual_r2_runtime_binding_is_exact():
    bundle = m.compat.load_runtime_bundle()

    assert (
        bundle[
            "acquisition_market_count"
        ]
        == 7
    )

    assert (
        bundle[
            "runtime_registration_indexes"
        ]
        == [
            2, 4, 5, 6, 7, 8, 10
        ]
    )


def test_r2_collector_has_no_analysis_logic():
    source = COLLECTOR_PATH.read_text(
        encoding="utf-8"
    ).lower()

    forbidden = [
        "calculate_innovation",
        "calculate_pm_response",
        "calculate_markout",
        "calculate_edge",
        "calculate_pnl",
        "winner_used = true",
        "settlement_used = true",
    ]

    for token in forbidden:
        assert token not in source
