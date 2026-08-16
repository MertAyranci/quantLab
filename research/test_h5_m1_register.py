from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sys

import pytest

sys.path.insert(
    0,
    str(Path(__file__).resolve().parent),
)

import h5_m1_register as r


def event():
    return {
        "odds_game_id":
            "event-123",

        "canonical_commence_time":
            datetime(
                2026,
                8,
                18,
                1,
                tzinfo=timezone.utc,
            ),

        "away_team":
            "Boston Red Sox",

        "home_team":
            "New York Yankees",
    }


def market(**changes):
    base = {
        "id":
            "gamma-1",

        "question":
            "Boston Red Sox vs. New York Yankees",

        "sportsMarketType":
            "moneyline",

        "clobTokenIds":
            ["tok0", "tok1"],

        "outcomes":
            [
                "Boston Red Sox",
                "New York Yankees",
            ],

        "active":
            True,

        "closed":
            False,

        "acceptingOrders":
            True,

        "gameStartTime":
            "2026-08-18T01:00:00Z",

        "conditionId":
            "0xabc",

        "slug":
            "bos-nyy",

        # Deliberate forbidden-looking
        # metadata: mapper must ignore it.
        "outcomePrices":
            ["0.45", "0.55"],

        "volume":
            "999999",
    }

    base.update(changes)
    return base


class FakeResponse:

    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeHTTP:

    def __init__(self, payload):
        self.payload = payload

    def get(self, *args, **kwargs):
        return FakeResponse(
            self.payload
        )


def test_norm_team():
    assert (
        r.norm_team(
            "St. Louis Cardinals"
        )
        == "st louis cardinals"
    )


def test_parse_matchup():
    assert r.parse_matchup(
        "Boston Red Sox vs. New York Yankees"
    ) == {
        "boston red sox",
        "new york yankees",
    }


def test_unique_gamma_mapping():
    payload = {
        "events": [
            {
                "markets": [
                    market()
                ]
            }
        ]
    }

    got = r.gamma_mapping(
        event=event(),
        http=FakeHTTP(payload),
    )

    assert got is not None
    assert got[
        "condition_id"
    ] == "0xabc"

    assert got[
        "p1_team"
    ] == "Boston Red Sox"

    assert got[
        "p1_token_id"
    ] == "tok0"


def test_price_and_volume_never_enter_mapping():
    payload = {
        "events": [
            {
                "markets": [
                    market()
                ]
            }
        ]
    }

    got = r.gamma_mapping(
        event=event(),
        http=FakeHTTP(payload),
    )

    text = str(got).lower()

    assert "outcomeprices" not in text
    assert "volume" not in text
    assert "0.45" not in text
    assert "0.55" not in text


def test_non_moneyline_fails():
    payload = {
        "events": [
            {
                "markets": [
                    market(
                        sportsMarketType="spread"
                    )
                ]
            }
        ]
    }

    assert r.gamma_mapping(
        event=event(),
        http=FakeHTTP(payload),
    ) is None


def test_three_tokens_fails():
    payload = {
        "events": [
            {
                "markets": [
                    market(
                        clobTokenIds=[
                            "a",
                            "b",
                            "c",
                        ]
                    )
                ]
            }
        ]
    }

    assert r.gamma_mapping(
        event=event(),
        http=FakeHTTP(payload),
    ) is None


def test_closed_fails():
    payload = {
        "events": [
            {
                "markets": [
                    market(
                        closed=True
                    )
                ]
            }
        ]
    }

    assert r.gamma_mapping(
        event=event(),
        http=FakeHTTP(payload),
    ) is None


def test_not_accepting_fails():
    payload = {
        "events": [
            {
                "markets": [
                    market(
                        acceptingOrders=False
                    )
                ]
            }
        ]
    }

    assert r.gamma_mapping(
        event=event(),
        http=FakeHTTP(payload),
    ) is None


def test_start_delta_over_600_fails():
    payload = {
        "events": [
            {
                "markets": [
                    market(
                        gameStartTime=(
                            "2026-08-18T01:11:00Z"
                        )
                    )
                ]
            }
        ]
    }

    assert r.gamma_mapping(
        event=event(),
        http=FakeHTTP(payload),
    ) is None


def test_ambiguous_two_candidates_fails():
    m2 = market(
        id="gamma-2"
    )

    payload = {
        "events": [
            {
                "markets": [
                    market(),
                    m2,
                ]
            }
        ]
    }

    assert r.gamma_mapping(
        event=event(),
        http=FakeHTTP(payload),
    ) is None


def test_outcomes_must_match_teams():
    payload = {
        "events": [
            {
                "markets": [
                    market(
                        outcomes=[
                            "Boston Red Sox",
                            "Chicago Cubs",
                        ]
                    )
                ]
            }
        ]
    }

    assert r.gamma_mapping(
        event=event(),
        http=FakeHTTP(payload),
    ) is None


def test_canonical_row_is_allowlisted():
    mapping = {
        "gamma_market_id":
            "g1",
        "condition_id":
            "0xabc",
        "gamma_slug":
            "slug",
        "gamma_question":
            "Boston Red Sox vs. New York Yankees",
        "pm_game_start_time":
            datetime(
                2026,
                8,
                18,
                1,
                tzinfo=timezone.utc,
            ),
        "start_delta_seconds":
            0,
        "p1_team":
            "Boston Red Sox",
        "p0_team":
            "New York Yankees",
        "p1_token_id":
            "t0",
        "p0_token_id":
            "t1",
        "exact_team_candidates":
            1,
        "in_window_candidates":
            1,
        "mapping_rule":
            r.MAPPING_RULE,

        # Must not leak:
        "price":
            0.5,
        "volume":
            1000,
    }

    row = r.canonical_row(
        index=1,
        event=event(),
        mapping=mapping,
        registered_at=datetime(
            2026,
            8,
            17,
            tzinfo=timezone.utc,
        ),
    )

    assert "price" not in row
    assert "volume" not in row


def test_registry_serialization_deterministic():
    mapping = {
        "gamma_market_id":
            "g1",
        "condition_id":
            "0xabc",
        "gamma_slug":
            "slug",
        "gamma_question":
            "Boston Red Sox vs. New York Yankees",
        "pm_game_start_time":
            datetime(
                2026,
                8,
                18,
                1,
                tzinfo=timezone.utc,
            ),
        "start_delta_seconds":
            0,
        "p1_team":
            "Boston Red Sox",
        "p0_team":
            "New York Yankees",
        "p1_token_id":
            "t0",
        "p0_token_id":
            "t1",
        "exact_team_candidates":
            1,
        "in_window_candidates":
            1,
        "mapping_rule":
            r.MAPPING_RULE,
    }

    row = r.canonical_row(
        index=1,
        event=event(),
        mapping=mapping,
        registered_at=datetime(
            2026,
            8,
            17,
            tzinfo=timezone.utc,
        ),
    )

    assert (
        r.serialize_registry([row])
        == r.serialize_registry([row])
    )


def test_events_endpoint_is_non_odds_path():
    assert (
        r.ODDS_EVENTS
        == (
            "https://api.the-odds-api.com/"
            "v4/sports/baseball_mlb/events"
        )
    )

    assert "/events/" not in (
        r.ODDS_EVENTS
    )

    assert not r.ODDS_EVENTS.endswith(
        "/odds"
    )
