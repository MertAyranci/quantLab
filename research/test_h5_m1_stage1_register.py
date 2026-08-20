from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

import h5_m1_stage1_register as m


NOW = datetime(
    2026,
    8,
    20,
    17,
    30,
    tzinfo=timezone.utc,
)


def market(
    *,
    condition="0xabc",
    start=None,
    active=True,
    closed=False,
    accepting=True,
    tokens=None,
    outcomes=None,
):
    if start is None:
        start = (
            NOW
            + timedelta(hours=2)
        )

    if tokens is None:
        tokens = ["1", "2"]

    if outcomes is None:
        outcomes = [
            "Team A",
            "Team B",
        ]

    return {
        "id": "123",
        "conditionId":
            condition,
        "slug":
            "mlb-a-b-2026-08-20",
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
    }


def test_valid_non_f19_market():
    row = m.eligible_candidate(
        market(),
        registered_at=NOW,
        f19_conditions=set(),
    )

    assert row is not None
    assert row[
        "condition_id"
    ] == "0xabc"
    assert (
        row[
            "registration_lead_seconds"
        ]
        == 7200
    )


def test_f19_condition_rejected():
    row = m.eligible_candidate(
        market(
            condition="0xF19"
        ),
        registered_at=NOW,
        f19_conditions={
            "0xf19"
        },
    )

    assert row is None


def test_minimum_lead_enforced():
    row = m.eligible_candidate(
        market(
            start=(
                NOW
                + timedelta(
                    seconds=1799
                )
            )
        ),
        registered_at=NOW,
        f19_conditions=set(),
    )

    assert row is None


def test_wrong_market_state_rejected():
    assert (
        m.eligible_candidate(
            market(active=False),
            registered_at=NOW,
            f19_conditions=set(),
        )
        is None
    )

    assert (
        m.eligible_candidate(
            market(closed=True),
            registered_at=NOW,
            f19_conditions=set(),
        )
        is None
    )

    assert (
        m.eligible_candidate(
            market(
                accepting=False
            ),
            registered_at=NOW,
            f19_conditions=set(),
        )
        is None
    )


def test_binary_two_token_required():
    assert (
        m.eligible_candidate(
            market(
                tokens=["1"]
            ),
            registered_at=NOW,
            f19_conditions=set(),
        )
        is None
    )

    assert (
        m.eligible_candidate(
            market(
                outcomes=["A"]
            ),
            registered_at=NOW,
            f19_conditions=set(),
        )
        is None
    )


def test_selection_order_and_target():
    events = [
        {
            "markets": [
                market(
                    condition=f"0x{i:02d}",
                    start=(
                        NOW
                        + timedelta(
                            hours=2,
                            minutes=i,
                        )
                    ),
                )
                for i in range(12)
            ]
        }
    ]

    selected = m.select_candidates(
        events,
        registered_at=NOW,
        f19_conditions=set(),
    )

    assert len(selected) == 10

    conditions = [
        x["condition_id"]
        for x in selected
    ]

    assert conditions == [
        f"0x{i:02d}"
        for i in range(10)
    ]


def test_rows_have_no_observed_odds():
    candidate = m.eligible_candidate(
        market(),
        registered_at=NOW,
        f19_conditions=set(),
    )

    rows = m.canonical_rows(
        [candidate],
        registered_at=NOW,
        registrar_sha="abc123",
    )

    row = rows[0]

    assert row[
        "odds_game_id"
    ] is None

    assert (
        row[
            "external_identity_status"
        ]
        ==
        "PENDING_PROVIDER_VISIBILITY"
    )

    forbidden = {
        "price",
        "volume",
        "liquidity",
        "consensus",
        "pnl",
        "markout",
    }

    assert not (
        forbidden
        &
        set(row)
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
        ==
        b"first\n"
    )
