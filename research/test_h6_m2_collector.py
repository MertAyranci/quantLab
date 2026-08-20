from __future__ import annotations

import asyncio
import hashlib
import importlib.util
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest


HERE = Path(__file__).resolve().parent
MODULE_PATH = HERE.parent / "collectors" / "polymarket" / "h6_m2_collector.py"

spec = importlib.util.spec_from_file_location("h6_m2_collector", MODULE_PATH)
m = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(m)


def make_book(token="t", bid=("0.40", "10"), ask=("0.60", "20"), tick="0.01"):
    return {
        "asset_id": token,
        "bids": [{"price": bid[0], "size": bid[1]}],
        "asks": [{"price": ask[0], "size": ask[1]}],
        "tick_size": tick,
    }


def test_frozen_constants():
    assert m.MARKET_COUNT == 30
    assert m.TOKEN_COUNT == 60
    assert m.PRIMARY_WINDOW_SECONDS == 1800
    assert m.PREARM_SECONDS == 120
    assert m.RECONCILE_SECONDS == 30
    assert m.PING_SECONDS == 10
    assert m.SILENCE_SECONDS == 30


def test_book_replace_and_best():
    s = m.BookState("t")
    s.replace_full(make_book(), connection_id="c", increment_generation=True)
    assert s.generation == 1
    assert s.structurally_valid()
    assert s.best() == (
        Decimal("0.40"), Decimal("10"),
        Decimal("0.60"), Decimal("20"),
    )


def test_price_change_absolute_size():
    s = m.BookState("t")
    s.replace_full(make_book(), connection_id="c", increment_generation=True)
    s.apply_level(side="BUY", price="0.40", size="17")
    assert s.bids[Decimal("0.40")] == Decimal("17")
    # Absolute, not additive: must be 17, not 27.
    assert s.bids[Decimal("0.40")] != Decimal("27")


def test_zero_removes_level():
    s = m.BookState("t")
    s.replace_full(make_book(), connection_id="c", increment_generation=True)
    s.apply_level(side="BUY", price="0.40", size="0")
    assert Decimal("0.40") not in s.bids


def test_sell_updates_ask():
    s = m.BookState("t")
    s.replace_full(make_book(), connection_id="c", increment_generation=True)
    s.apply_level(side="SELL", price="0.55", size="7")
    assert s.asks[Decimal("0.55")] == Decimal("7")
    assert s.best()[2] == Decimal("0.55")


@pytest.mark.parametrize("side", ["", "X", None])
def test_unknown_side_rejected(side):
    s = m.BookState("t")
    s.replace_full(make_book(), connection_id="c", increment_generation=True)
    with pytest.raises(ValueError):
        s.apply_level(side=side, price="0.40", size="5")


@pytest.mark.parametrize(
    "price,size",
    [
        ("0", "1"),
        ("1", "1"),
        ("1.1", "1"),
        ("0.5", "-1"),
        ("nan", "1"),
    ],
)
def test_invalid_level_rejected(price, size):
    s = m.BookState("t")
    s.replace_full(make_book(), connection_id="c", increment_generation=True)
    with pytest.raises(ValueError):
        s.apply_level(side="BUY", price=price, size=size)


def test_full_book_compare():
    s = m.BookState("t")
    book = make_book()
    s.replace_full(book, connection_id="c", increment_generation=True)
    assert s.same_full_book(book)
    changed = make_book(bid=("0.40", "11"))
    assert not s.same_full_book(changed)


def test_matching_replace_can_keep_generation():
    s = m.BookState("t")
    s.replace_full(make_book(), connection_id="c", increment_generation=True)
    g = s.generation
    s.replace_full(make_book(bid=("0.41", "1")), connection_id="c", increment_generation=False)
    assert s.generation == g


def test_snapshot_contains_no_imbalance():
    s = m.BookState("t")
    s.replace_full(make_book(), connection_id="c", increment_generation=True)
    snap = s.snapshot()
    assert "imbalance" not in json.dumps(m.json_clean(snap)).lower()


def fake_bundle():
    base = datetime(2026, 8, 18, 12, 0, tzinfo=timezone.utc)
    rows = []
    for idx in range(30):
        rows.append({
            "study": "H6",
            "milestone": "H6-M2",
            "registration_index": idx + 1,
            "registered_at_utc": (base - timedelta(hours=2)).isoformat(),
            "gamma_market_id": str(idx),
            "condition_id": f"0x{idx}",
            "game_start_time_utc": (base + timedelta(minutes=idx * 5)).isoformat(),
            "p1_token_id": f"p1-{idx}",
            "p0_token_id": f"p0-{idx}",
            "p1_outcome": f"A{idx}",
            "p0_outcome": f"B{idx}",
        })
    return {"rows": rows, "registry_sha256": "a"*64, "receipt_sha256": "b"*64}


class MemoryWriter:
    def __init__(self):
        self.capture_id = "test"
        self.rows = []

    def write(self, stream, record):
        self.rows.append((stream, record))


def build_capture():
    w = MemoryWriter()
    c = m.H6Capture(bundle=fake_bundle(), writer=w)
    c.conn_id = "conn"
    for token, state in c.states.items():
        state.replace_full(
            make_book(token=token),
            connection_id="conn",
            increment_generation=True,
        )
    return c, w


def test_primary_window_boundaries():
    c, _ = build_capture()
    token = "p1-0"
    start = c.token_meta[token]["game_start"]
    assert not c.in_primary_window(token, start - timedelta(seconds=1801))
    assert c.in_primary_window(token, start - timedelta(seconds=1800))
    assert c.in_primary_window(token, start - timedelta(microseconds=1))
    assert not c.in_primary_window(token, start)


def test_frame_atomicity_same_token_multiple_changes():
    c, w = build_capture()
    t = c.token_meta["p1-0"]["window_start"] + timedelta(seconds=1)
    raw = json.dumps({
        "event_type": "price_change",
        "market": "0x0",
        "timestamp": "100",
        "price_changes": [
            {
                "asset_id": "p1-0",
                "price": "0.40",
                "size": "30",
                "side": "BUY",
                "best_bid": "0.40",
                "best_ask": "0.60",
            },
            {
                "asset_id": "p1-0",
                "price": "0.60",
                "size": "5",
                "side": "SELL",
                "best_bid": "0.40",
                "best_ask": "0.60",
            },
        ],
    })
    mismatches = c.handle_raw(raw, capture_time=t)
    assert mismatches == set()
    s = c.states["p1-0"]
    assert s.bids[Decimal("0.40")] == Decimal("30")
    assert s.asks[Decimal("0.60")] == Decimal("5")
    post = [
        r for stream, r in w.rows
        if stream == "control"
        and r.get("kind") == "post_frame_state"
        and r.get("token") == "p1-0"
    ]
    assert len(post) == 1


def test_all_normalized_rows_inherit_frame_time_and_hash():
    c, w = build_capture()
    t = c.token_meta["p1-0"]["window_start"] + timedelta(seconds=1)
    raw = json.dumps({
        "event_type": "price_change",
        "market": "0x0",
        "timestamp": "100",
        "price_changes": [{
            "asset_id": "p1-0",
            "price": "0.40",
            "size": "30",
            "side": "BUY",
            "best_bid": "0.40",
            "best_ask": "0.60",
        }],
    })
    c.handle_raw(raw, capture_time=t)
    raws = [r for stream, r in w.rows if stream == "raw_frames"]
    normalized = [r for stream, r in w.rows if stream == "normalized"]
    assert len(raws) == 1
    assert normalized
    for row in normalized:
        assert row["capture_time"] == t
        assert row["raw_frame_sha256"] == raws[0]["raw_frame_sha256"]


def test_venue_tob_mismatch_detected():
    c, _ = build_capture()
    t = c.token_meta["p1-0"]["window_start"] + timedelta(seconds=1)
    raw = json.dumps({
        "event_type": "best_bid_ask",
        "asset_id": "p1-0",
        "best_bid": "0.41",
        "best_ask": "0.60",
        "timestamp": "100",
    })
    assert c.handle_raw(raw, capture_time=t) == {"p1-0"}


def test_ws_book_does_not_increment_generation_when_valid():
    c, _ = build_capture()
    token = "p1-0"
    g = c.states[token].generation
    t = c.token_meta[token]["window_start"] + timedelta(seconds=1)
    raw = json.dumps({
        "event_type": "book",
        "asset_id": token,
        "bids": [{"price": "0.41", "size": "8"}],
        "asks": [{"price": "0.61", "size": "9"}],
        "timestamp": "100",
    })
    c.handle_raw(raw, capture_time=t)
    assert c.states[token].generation == g


def test_ws_book_increments_generation_when_restoring_invalid_state():
    c, _ = build_capture()
    token = "p1-0"
    c.states[token].valid = False
    g = c.states[token].generation
    t = c.token_meta[token]["window_start"] + timedelta(seconds=1)
    raw = json.dumps({
        "event_type": "book",
        "asset_id": token,
        "bids": [{"price": "0.41", "size": "8"}],
        "asks": [{"price": "0.61", "size": "9"}],
        "timestamp": "100",
    })
    c.handle_raw(raw, capture_time=t)
    assert c.states[token].generation == g + 1
    assert c.states[token].valid is True


def test_process_text_frame_records_boundary_before_raw_frame():
    c, w = build_capture()

    token = "p1-0"
    t = c.token_meta[token]["window_start"]

    raw = json.dumps({
        "event_type": "price_change",
        "market": "0x0",
        "timestamp": "100",
        "price_changes": [{
            "asset_id": token,
            "price": "0.40",
            "size": "30",
            "side": "BUY",
            "best_bid": "0.40",
            "best_ask": "0.60",
        }],
    })

    c.process_text_frame(
        raw,
        capture_time=t,
    )

    streams = [
        stream
        for stream, _ in w.rows
    ]

    first_raw = streams.index(
        "raw_frames"
    )

    boundary_positions = [
        i
        for i, (stream, row)
        in enumerate(w.rows)
        if (
            stream == "boundary_states"
            and row.get("condition_id") == "0x0"
            and row.get("boundary")
            == "primary_window_open"
        )
    ]

    assert boundary_positions
    assert max(boundary_positions) < first_raw


def test_ingest_sequence_reserved_once_per_top_level_message():
    c, w = build_capture()

    token = "p1-0"

    moment = (
        c.token_meta[token]["window_start"]
        + timedelta(seconds=1)
    )

    raw = json.dumps({
        "event_type": "price_change",
        "market": "0x0",
        "timestamp": "100",
        "price_changes": [
            {
                "asset_id": token,
                "price": "0.40",
                "size": "30",
                "side": "BUY",
                "best_bid": "0.40",
                "best_ask": "0.60",
            },
            {
                "asset_id": token,
                "price": "0.60",
                "size": "5",
                "side": "SELL",
                "best_bid": "0.40",
                "best_ask": "0.60",
            },
        ],
    })

    before = c.ingest_sequence

    c.process_text_frame(
        raw,
        capture_time=moment,
    )

    rows = [
        row
        for stream, row in w.rows
        if (
            stream == "normalized"
            and row.get("token") == token
            and row.get("kind") == "price_change"
        )
    ]

    assert len(rows) == 2

    # Both changes came from ONE top-level
    # price_change message.
    assert {
        row["ingest_sequence"]
        for row in rows
    } == {before + 1}

    assert {
        row["change_index"]
        for row in rows
    } == {0, 1}

    # Exactly one sequence number was consumed.
    assert c.ingest_sequence == before + 1


def test_reconcile_restores_invalid_state_even_when_book_identical():
    c, w = build_capture()

    token = "p1-0"
    state = c.states[token]

    before_generation = state.generation
    snapshot = state.snapshot()

    # Simulate a previous REST observation
    # where this token was absent.
    state.valid = False

    async def fake_rest_books(http, tokens):
        assert tokens == [token]

        return [{
            "asset_id": token,
            "bids": snapshot["bids"],
            "asks": snapshot["asks"],
            "tick_size": str(
                snapshot["tick_size"]
            ),
        }]

    c.rest_books = fake_rest_books

    asyncio.run(
        c.reconcile(
            object(),
            tokens=[token],
        )
    )

    # Even though levels were identical to the
    # last known book, invalidity forces resync.
    assert state.valid is True

    assert (
        state.generation
        == before_generation + 1
    )

    snapshots = [
        row
        for stream, row in w.rows
        if (
            stream == "rest_snapshots"
            and row.get("token") == token
        )
    ]

    assert len(snapshots) == 1

    assert (
        snapshots[0]["matched_local_state_before"]
        is False
    )


def test_boundary_flags_all_false():
    assert all(value is False for value in m.boundary_flags().values())


def test_source_contains_no_f19_f20_data_paths():
    source = MODULE_PATH.read_text(encoding="utf-8")
    assert "data/prospective/rn1_f19" not in source
    assert "clob_f20" not in source
    assert "data-api.polymarket.com" not in source
    assert "ODDS_API" not in source


def test_source_has_no_response_or_pnl_formula():
    source = MODULE_PATH.read_text(encoding="utf-8").lower()
    forbidden = [
        "directional_hit_rate(",
        "imbalance_change =",
        "l1_imbalance =",
        "calculate_pnl",
        "markout =",
    ]
    for token in forbidden:
        assert token not in source



def test_stale_venue_timestamp_guard():
    s = m.BookState("t")
    book = make_book()
    book["timestamp"] = "200"
    s.replace_full(book, connection_id="c", increment_generation=True)
    assert s.is_stale_venue_timestamp("199")
    assert not s.is_stale_venue_timestamp("200")
    assert not s.is_stale_venue_timestamp("201")


def test_stale_price_change_preserved_but_not_applied():
    c, w = build_capture()
    token = "p1-0"
    c.states[token].last_venue_timestamp_ms = 200
    before = c.states[token].bids[Decimal("0.40")]
    t0 = c.token_meta[token]["window_start"] + timedelta(seconds=1)
    raw = json.dumps({
        "event_type": "price_change",
        "market": "0x0",
        "timestamp": "199",
        "price_changes": [{
            "asset_id": token,
            "price": "0.40",
            "size": "99",
            "side": "BUY",
            "best_bid": "0.40",
            "best_ask": "0.60",
        }],
    })
    c.handle_raw(raw, capture_time=t0)
    assert c.states[token].bids[Decimal("0.40")] == before
    rows = [r for stream, r in w.rows if stream == "normalized"]
    assert len(rows) == 1
    assert rows[0]["payload"]["state_applied"] is False


def test_contract_sha_matches_embedded():
    contract = HERE / "h6_m2_capture_contract.json"
    assert hashlib.sha256(contract.read_bytes()).hexdigest() == m.EXPECTED_CAPTURE_CONTRACT_SHA



def test_m2_registry_validation_accepts_exact_30():
    rows = fake_bundle()["rows"]
    m.validate_registry_rows(rows)


def test_m2_registry_validation_rejects_29():
    rows = fake_bundle()["rows"][:-1]

    with pytest.raises(
        RuntimeError,
        match="exactly 30",
    ):
        m.validate_registry_rows(rows)


def test_m2_registry_validation_rejects_wrong_milestone():
    rows = fake_bundle()["rows"]

    rows[0] = {
        **rows[0],
        "milestone": "H6-M1",
    }

    with pytest.raises(
        RuntimeError,
        match="study/milestone",
    ):
        m.validate_registry_rows(rows)


def test_m2_frozen_config_without_live_requirement():
    c = m.validate_frozen_inputs(
        require_committed_self=False
    )

    assert (
        c["milestone"]
        == "H6-M2-CAPTURE"
    )

    assert (
        c["registry_requirements"][
            "exact_market_count"
        ]
        == 30
    )

    assert (
        c["registry_requirements"][
            "exact_token_count"
        ]
        == 60
    )

    assert (
        c["capture"][
            "rest_reconciliation_seconds"
        ]
        == 30
    )


def test_m2_source_uses_only_m2_capture_namespace():
    source = MODULE_PATH.read_text(
        encoding="utf-8"
    )

    assert (
        "data/research/h6/m1/registry.jsonl"
        not in source
    )

    assert (
        '"h6" / "m1" / "capture"'
        not in source
    )

    assert (
        "h6_m1_registry_freeze.json"
        not in source
    )


def test_m2_source_has_no_external_or_other_hypothesis_data_reads():
    source = MODULE_PATH.read_text(
        encoding="utf-8"
    )

    forbidden = [
        "data/prospective/rn1_f19",
        "data-api.polymarket.com",
        "ODDS_API_KEY",
        "external_id_map.jsonl",
        "h5_m1_collector",
    ]

    for token in forbidden:
        assert token not in source
