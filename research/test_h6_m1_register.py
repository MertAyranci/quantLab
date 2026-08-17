from __future__ import annotations

import hashlib
import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest


HERE = Path(__file__).resolve().parent
MODULE_PATH = HERE / "h6_m1_register.py"

spec = importlib.util.spec_from_file_location("h6_m1_register", MODULE_PATH)
m = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(m)


class FakeResponse:
    def __init__(self, payload, status_code=200, text=""):
        self._payload = payload
        self.status_code = status_code
        self.text = text

    def json(self):
        return self._payload


class FakeClient:
    def __init__(self, routes):
        self.routes = list(routes)
        self.calls = []

    def get(self, url, params=None):
        self.calls.append((url, params))
        if not self.routes:
            raise AssertionError("unexpected GET")
        expected_url, payload = self.routes.pop(0)
        assert url == expected_url
        return FakeResponse(payload)


def market(
    *,
    condition,
    start,
    market_id="100",
    question="A Team vs B Team",
    outcomes=("A Team", "B Team"),
    tokens=("t1", "t2"),
    active=True,
    closed=False,
    accepting=True,
    market_type="moneyline",
):
    return {
        "id": market_id,
        "question": question,
        "sportsMarketType": market_type,
        "active": active,
        "closed": closed,
        "acceptingOrders": accepting,
        "gameStartTime": start,
        "conditionId": condition,
        "slug": f"slug-{market_id}",
        "outcomes": json.dumps(list(outcomes)),
        "clobTokenIds": json.dumps(list(tokens)),
        # Forbidden fields can exist upstream; canonicalization must omit them.
        "liquidity": 999,
        "volume": 888,
        "outcomePrices": "[0.5,0.5]",
    }


def event(event_id, market_obj):
    return {
        "id": event_id,
        "slug": f"event-{event_id}",
        "markets": [market_obj],
        "volume": 99999,
        "liquidity": 99999,
    }


def test_constants():
    assert m.TARGET_MARKETS == 5
    assert m.MIN_MARKETS == 5
    assert m.M0_MIN_LEAD_SECONDS == 1800
    assert m.REGISTRATION_LEAD_SECONDS == 3600
    assert m.SPORT == "mlb"


def test_parse_dt_requires_timezone():
    assert m.parse_dt("2026-08-18T12:00:00Z") is not None
    assert m.parse_dt("2026-08-18T12:00:00") is None


def test_matchup_normalization():
    assert m.parse_matchup("Athletics vs. Boston Red Sox") == {
        "athletics",
        "boston red sox",
    }


def test_resolve_mlb_tag_unique():
    c = FakeClient([
        (m.SPORTS_URL, [
            {"sport": "nba", "primaryTagId": 1},
            {"sport": "mlb", "primaryTagId": 100381},
        ]),
    ])
    assert m.resolve_mlb_tag(c) == "100381"


def test_resolve_mlb_tag_rejects_missing():
    c = FakeClient([(m.SPORTS_URL, [{"sport": "nba", "primaryTagId": 1}])])
    with pytest.raises(RuntimeError):
        m.resolve_mlb_tag(c)


def test_canonical_candidate_accepts_valid_market():
    now = datetime(2026, 8, 18, 0, 0, tzinfo=timezone.utc)
    row = m.canonical_candidate(
        gamma_event={"id": "e1", "slug": "event-e1"},
        market=market(
            condition="0xabc",
            start="2026-08-18T01:00:00Z",
        ),
        now=now,
    )
    assert row is not None
    assert row["condition_id"] == "0xabc"
    assert row["p1_token_id"] == "t1"
    assert "volume" not in row
    assert "liquidity" not in row
    assert "outcomePrices" not in row


@pytest.mark.parametrize(
    "patch",
    [
        {"market_type": "spread"},
        {"active": False},
        {"closed": True},
        {"accepting": False},
        {"tokens": ("t1",)},
        {"outcomes": ("A Team",)},
        {"question": "A Team vs C Team"},
        {"start": "2026-08-18T00:29:59Z"},
    ],
)
def test_canonical_candidate_rejects_ineligible(patch):
    now = datetime(2026, 8, 18, 0, 0, tzinfo=timezone.utc)
    kwargs = {
        "condition": "0xabc",
        "start": "2026-08-18T01:00:00Z",
    }
    kwargs.update(patch)
    row = m.canonical_candidate(
        gamma_event={"id": "e1", "slug": "event-e1"},
        market=market(**kwargs),
        now=now,
    )
    assert row is None


def test_select_exactly_five_sorted():
    now = datetime(2026, 8, 18, 0, 0, tzinfo=timezone.utc)
    rows = []
    for idx in range(7):
        hour = idx + 1
        rows.append(
            event(
                f"e{idx}",
                market(
                    condition=f"0x{idx:02x}",
                    market_id=str(idx),
                    start=f"2026-08-18T{hour:02d}:00:00Z",
                    tokens=(f"p1-{idx}", f"p0-{idx}"),
                ),
            )
        )
    selected = m.select_candidates(rows[::-1], now=now)
    assert len(selected) == 5
    assert [x["condition_id"] for x in selected] == [
        "0x00", "0x01", "0x02", "0x03", "0x04"
    ]


def test_select_rejects_fewer_than_five():
    now = datetime(2026, 8, 18, 0, 0, tzinfo=timezone.utc)
    rows = [
        event(
            f"e{idx}",
            market(
                condition=f"0x{idx}",
                market_id=str(idx),
                start="2026-08-18T02:00:00Z",
                tokens=(f"a{idx}", f"b{idx}"),
            ),
        )
        for idx in range(4)
    ]
    with pytest.raises(RuntimeError):
        m.select_candidates(rows, now=now)


def test_select_rejects_duplicate_tokens():
    now = datetime(2026, 8, 18, 0, 0, tzinfo=timezone.utc)
    rows = []
    for idx in range(5):
        toks = ("same", f"b{idx}") if idx < 2 else (f"a{idx}", f"b{idx}")
        rows.append(
            event(
                f"e{idx}",
                market(
                    condition=f"0x{idx}",
                    market_id=str(idx),
                    start="2026-08-18T02:00:00Z",
                    tokens=toks,
                ),
            )
        )
    with pytest.raises(RuntimeError):
        m.select_candidates(rows, now=now)


def test_registry_allowlist_and_determinism():
    now = datetime(2026, 8, 18, 0, 0, tzinfo=timezone.utc)
    row = {
        "gamma_event_id": "e",
        "gamma_event_slug": "es",
        "gamma_market_id": "m",
        "condition_id": "0xc",
        "gamma_market_slug": "ms",
        "gamma_question": "A Team vs B Team",
        "game_start_time_utc": "2026-08-18T01:00:00+00:00",
        "p1_outcome": "A Team",
        "p0_outcome": "B Team",
        "p1_token_id": "a",
        "p0_token_id": "b",
        "mapping_rule": m.MAPPING_RULE,
    }
    a = m.serialize_registry([row], registered_at=now)
    b = m.serialize_registry([row], registered_at=now)
    assert a == b
    parsed = json.loads(a)
    assert set(parsed) == {
        "study", "milestone", "registration_index", "registered_at_utc",
        "gamma_event_id", "gamma_event_slug", "gamma_market_id",
        "condition_id", "gamma_market_slug", "gamma_question",
        "game_start_time_utc", "p1_outcome", "p0_outcome",
        "p1_token_id", "p0_token_id", "mapping_rule",
    }


def test_safe_get_json_does_not_raise_httpx_url_exception():
    c = FakeClient([(m.SPORTS_URL, {"error": "x"})])
    # Fake response is 200, payload shape failure happens later.
    with pytest.raises(RuntimeError):
        m.resolve_mlb_tag(c)


def test_source_has_no_f19_selection_path():
    source = MODULE_PATH.read_text(encoding="utf-8")
    assert "rn1_f19" not in source
    assert "data/prospective" not in source
    assert "data-api.polymarket.com" not in source
    assert "ODDS_API" not in source


def test_source_has_no_response_economics_logic():
    source = MODULE_PATH.read_text(encoding="utf-8").lower()
    forbidden = [
        "future_midpoint",
        "directional_hit",
        "markout(",
        "calculate_pnl",
        "winner_used = true",
        "settlement_used = true",
    ]
    for token in forbidden:
        assert token not in source


def test_contract_sha_matches_embedded_constant():
    contract = HERE / "h6_m1_registration_contract.json"
    assert hashlib.sha256(contract.read_bytes()).hexdigest() == m.EXPECTED_CONTRACT_SHA
