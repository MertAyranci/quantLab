from __future__ import annotations

from datetime import (
    datetime,
    timedelta,
    timezone,
)

from pathlib import Path

import pytest

from research import (
    h5_m1r2_register as m,
)


ANCHOR = datetime(
    2026,
    8,
    24,
    20,
    0,
    tzinfo=timezone.utc,
)


def market(
    *,
    condition="0xabc",
    market_id="123",
    start=None,
    active=True,
    closed=False,
    accepting=True,
    tokens=None,
    outcomes=None,
):
    if start is None:
        start = (
            ANCHOR
            + timedelta(
                hours=9
            )
        )

    if tokens is None:
        tokens = [
            "1",
            "2",
        ]

    if outcomes is None:
        outcomes = [
            "Team A",
            "Team B",
        ]

    return {
        "id":
            market_id,

        "conditionId":
            condition,

        "slug":
            "mlb-a-b-2026-08-25",

        "question":
            "Team A vs. Team B",

        "gameStartTime":
            start.isoformat(),

        "sportsMarketType":
            "moneyline",

        "active":
            active,

        "closed":
            closed,

        "acceptingOrders":
            accepting,

        "clobTokenIds":
            tokens,

        "outcomes":
            outcomes,

        # Forbidden scientific/economic fields
        # intentionally included in the synthetic
        # source to prove projection strips them.
        "lastTradePrice":
            0.51,

        "volume":
            12345,

        "liquidity":
            999,
    }


def candidate(
    *,
    condition="0xabc",
    start=None,
    excluded=None,
):
    if excluded is None:
        excluded = set()

    return m.eligible_candidate(
        market(
            condition=condition,
            start=start,
        ),
        anchor=ANCHOR,
        excluded_conditions=excluded,
    )


def test_lower_bound_is_inclusive():
    row = candidate(
        start=(
            ANCHOR
            + timedelta(
                hours=8
            )
        )
    )

    assert row is not None

    assert (
        row[
            "registration_lead_seconds"
        ]
        == 8 * 3600
    )


def test_before_lower_bound_rejected():
    row = candidate(
        start=(
            ANCHOR
            + timedelta(
                hours=8,
                microseconds=-1,
            )
        )
    )

    assert row is None


def test_upper_bound_is_exclusive():
    before = candidate(
        start=(
            ANCHOR
            + timedelta(
                hours=32,
                microseconds=-1,
            )
        )
    )

    at_upper = candidate(
        start=(
            ANCHOR
            + timedelta(
                hours=32
            )
        )
    )

    assert before is not None
    assert at_upper is None


def test_any_frozen_condition_is_rejected():
    assert (
        candidate(
            condition="0xF19",
            excluded={
                "0xf19",
            },
        )
        is None
    )

    assert (
        candidate(
            condition="0xH2",
            excluded={
                "0xh2",
            },
        )
        is None
    )

    assert (
        candidate(
            condition="0xOLDH5",
            excluded={
                "0xoldh5",
            },
        )
        is None
    )


def test_wrong_market_state_rejected():
    assert (
        m.eligible_candidate(
            market(
                active=False
            ),
            anchor=ANCHOR,
            excluded_conditions=set(),
        )
        is None
    )

    assert (
        m.eligible_candidate(
            market(
                closed=True
            ),
            anchor=ANCHOR,
            excluded_conditions=set(),
        )
        is None
    )

    assert (
        m.eligible_candidate(
            market(
                accepting=False
            ),
            anchor=ANCHOR,
            excluded_conditions=set(),
        )
        is None
    )


def test_binary_two_token_required():
    assert (
        m.eligible_candidate(
            market(
                tokens=["1"]
            ),
            anchor=ANCHOR,
            excluded_conditions=set(),
        )
        is None
    )

    assert (
        m.eligible_candidate(
            market(
                outcomes=["A"]
            ),
            anchor=ANCHOR,
            excluded_conditions=set(),
        )
        is None
    )


def test_candidate_enumeration_is_sorted_and_complete():
    events = [
        {
            "markets": [
                market(
                    condition=f"0x{i:02d}",
                    market_id=str(i),
                    start=(
                        ANCHOR
                        + timedelta(
                            hours=9,
                            minutes=(
                                10 - i
                            ),
                        )
                    ),
                )
                for i in range(11)
            ]
        }
    ]

    selected = m.select_candidates(
        events,
        anchor=ANCHOR,
        excluded_conditions=set(),
    )

    # Candidate enumeration remains complete.
    # R2 truncation occurs only in select_for_freeze().
    assert len(selected) == 11

    ordering = [
        (
            row[
                "pm_game_start_time"
            ],
            row[
                "condition_id"
            ],
        )
        for row in selected
    ]

    assert ordering == sorted(
        ordering
    )


def test_freeze_gate_requires_at_least_ten():
    m.validate_freeze_gate(
        list(range(10))
    )

    m.validate_freeze_gate(
        list(range(11))
    )

    m.validate_freeze_gate(
        list(range(15))
    )

    with pytest.raises(
        RuntimeError
    ):
        m.validate_freeze_gate(
            list(range(9))
        )


def test_select_for_freeze_uses_deterministic_first_ten():
    eligible = [
        {"condition_id": f"0x{i:02d}"}
        for i in range(15)
    ]

    selected = m.select_for_freeze(
        eligible
    )

    assert len(selected) == 10

    assert [
        row["condition_id"]
        for row in selected
    ] == [
        f"0x{i:02d}"
        for i in range(10)
    ]

    assert len(eligible) == 15


def test_conflicting_duplicate_condition_fails_closed():
    events = [
        {
            "markets": [
                market(
                    condition="0xabc",
                    market_id="1",
                ),
                market(
                    condition="0xabc",
                    market_id="2",
                ),
            ]
        }
    ]

    with pytest.raises(
        RuntimeError
    ):
        m.select_candidates(
            events,
            anchor=ANCHOR,
            excluded_conditions=set(),
        )


def test_metadata_projection_strips_forbidden_fields():
    projected = m.market_projection(
        market()
    )

    assert set(
        projected
    ) == {
        "id",
        "conditionId",
        "slug",
        "question",
        "gameStartTime",
        "sportsMarketType",
        "active",
        "closed",
        "acceptingOrders",
        "clobTokenIds",
        "outcomes",
    }

    assert (
        "lastTradePrice"
        not in projected
    )

    assert (
        "volume"
        not in projected
    )

    assert (
        "liquidity"
        not in projected
    )


def test_projection_is_deterministic():
    events_a = [
        {
            "markets": [
                market(
                    condition="0xb",
                    market_id="2",
                ),
                market(
                    condition="0xa",
                    market_id="1",
                ),
            ]
        }
    ]

    events_b = [
        {
            "markets": [
                market(
                    condition="0xa",
                    market_id="1",
                ),
                market(
                    condition="0xb",
                    market_id="2",
                ),
            ]
        }
    ]

    a = m.projection_bytes(
        m.gamma_selection_projection(
            events_a
        )
    )

    b = m.projection_bytes(
        m.gamma_selection_projection(
            events_b
        )
    )

    assert a == b

    assert (
        m.sha256_bytes(a)
        == m.sha256_bytes(b)
    )


def test_rows_contain_no_observed_odds():
    row = candidate()

    rows = m.canonical_rows(
        [row],
        anchor=ANCHOR,
        registrar_sha="registrar",
        gamma_tag_id="42",
        projection_sha="projection",
        projection_market_count=10,
    )

    out = rows[0]

    assert (
        out[
            "milestone"
        ]
        == "H5-M1R2"
    )

    assert (
        out[
            "external_identity_status"
        ]
        ==
        "PENDING_PROVIDER_VISIBILITY"
    )

    assert (
        out[
            "odds_game_id"
        ]
        is None
    )

    forbidden = {
        "price",
        "probability",
        "volume",
        "liquidity",
        "consensus",
        "response",
        "markout",
        "edge",
        "pnl",
        "winner",
        "settlement",
    }

    assert not (
        forbidden
        & set(out)
    )


def test_exclusive_create_refuses_overwrite(
    tmp_path,
):
    path = (
        tmp_path
        / "registry.jsonl"
    )

    m.create_exclusive(
        path,
        b"first\n",
    )

    with pytest.raises(
        FileExistsError
    ):
        m.create_exclusive(
            path,
            b"second\n",
        )

    assert (
        path.read_bytes()
        == b"first\n"
    )


def test_source_contains_no_odds_api_host():
    source = (
        Path(
            m.__file__
        )
        .read_text(
            encoding="utf-8"
        )
        .lower()
    )

    assert (
        "the-odds-api.com"
        not in source
    )

    assert (
        "api.the-odds-api"
        not in source
    )


def test_r2_source_exposes_no_real_gamma_preview():
    source = (
        Path(
            m.__file__
        )
        .read_text(
            encoding="utf-8"
        )
    )

    assert "--preview" not in source
