from __future__ import annotations

import hashlib
import json
from datetime import (
    datetime,
    timedelta,
    timezone,
)

import pytest

import h6_m2_register as m


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
    outcomes=None,
    tokens=None,
    question="Team A vs. Team B",
):
    if start is None:
        start = (
            NOW
            + timedelta(hours=2)
        )

    if outcomes is None:
        outcomes = [
            "Team A",
            "Team B",
        ]

    if tokens is None:
        tokens = [
            "token-a",
            "token-b",
        ]

    return {
        "id": "market-1",
        "conditionId":
            condition,
        "slug":
            "mlb-a-b-2026-08-20",
        "question":
            question,
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
        "outcomes":
            outcomes,
        "clobTokenIds":
            tokens,
    }


def event(markets):
    return {
        "id": "event-1",
        "slug": "event-slug",
        "markets": markets,
    }


def test_valid_candidate():
    row = m.canonical_candidate(
        gamma_event=event([]),
        market=market(),
        now=NOW,
    )

    assert row is not None
    assert row[
        "condition_id"
    ] == "0xabc"


def test_operational_lead_enforced():
    row = m.canonical_candidate(
        gamma_event=event([]),
        market=market(
            start=(
                NOW
                + timedelta(
                    seconds=3599
                )
            )
        ),
        now=NOW,
    )

    assert row is None


def test_market_state_enforced():
    for kwargs in (
        {"active": False},
        {"closed": True},
        {"accepting": False},
    ):
        assert (
            m.canonical_candidate(
                gamma_event=
                    event([]),
                market=market(
                    **kwargs
                ),
                now=NOW,
            )
            is None
        )


def test_binary_tokens_required():
    assert (
        m.canonical_candidate(
            gamma_event=event([]),
            market=market(
                outcomes=["Team A"]
            ),
            now=NOW,
        )
        is None
    )

    assert (
        m.canonical_candidate(
            gamma_event=event([]),
            market=market(
                tokens=["one"]
            ),
            now=NOW,
        )
        is None
    )


def test_question_outcome_identity_required():
    assert (
        m.canonical_candidate(
            gamma_event=event([]),
            market=market(
                question=
                    "Other A vs. Other B"
            ),
            now=NOW,
        )
        is None
    )


def make_candidate_market(i):
    return market(
        condition=f"0x{i:04d}",
        start=(
            NOW
            + timedelta(
                hours=2,
                minutes=i,
            )
        ),
        tokens=[
            f"p1-{i}",
            f"p0-{i}",
        ],
    )


def test_frozen_exclusion_is_removed():
    events = [
        event([
            make_candidate_market(i)
            for i in range(31)
        ])
    ]

    result = m.select_candidates(
        events,
        now=NOW,
        exclusion_sets={
            "F19": {"0x0000"},
            "H6_M1": set(),
            "H5": set(),
            "H2_V2": set(),
        },
    )

    conditions = {
        row["condition_id"]
        for row in result[
            "selected"
        ]
    }

    assert "0x0000" not in conditions
    assert len(conditions) == 30


def test_first_30_frozen_order():
    events = [
        event([
            make_candidate_market(i)
            for i in reversed(
                range(35)
            )
        ])
    ]

    result = m.select_candidates(
        events,
        now=NOW,
        exclusion_sets={
            "F19": set(),
            "H6_M1": set(),
            "H5": set(),
            "H2_V2": set(),
        },
    )

    assert [
        row["condition_id"]
        for row in result[
            "selected"
        ]
    ] == [
        f"0x{i:04d}"
        for i in range(30)
    ]


def test_fewer_than_30_fails_closed():
    events = [
        event([
            make_candidate_market(i)
            for i in range(29)
        ])
    ]

    with pytest.raises(
        RuntimeError,
        match="insufficient",
    ):
        m.select_candidates(
            events,
            now=NOW,
            exclusion_sets={
                "F19": set(),
                "H6_M1": set(),
                "H5": set(),
                "H2_V2": set(),
            },
        )


def test_duplicate_condition_fails():
    duplicate = (
        make_candidate_market(0)
    )

    second = dict(duplicate)
    second["id"] = "market-2"
    second["clobTokenIds"] = [
        "x",
        "y",
    ]

    with pytest.raises(
        RuntimeError,
        match="duplicate condition_id",
    ):
        m.select_candidates(
            [
                event([
                    duplicate,
                    second,
                ])
            ]
            + [
                event([
                    make_candidate_market(i)
                ])
                for i in range(
                    1,
                    31,
                )
            ],
            now=NOW,
            exclusion_sets={
                "F19": set(),
                "H6_M1": set(),
                "H5": set(),
                "H2_V2": set(),
            },
        )


def test_duplicate_selected_token_fails():
    markets = [
        make_candidate_market(i)
        for i in range(30)
    ]

    markets[1][
        "clobTokenIds"
    ] = [
        "p1-0",
        "other",
    ]

    with pytest.raises(
        RuntimeError,
        match="duplicate token",
    ):
        m.select_candidates(
            [event(markets)],
            now=NOW,
            exclusion_sets={
                "F19": set(),
                "H6_M1": set(),
                "H5": set(),
                "H2_V2": set(),
            },
        )


def test_load_jsonl_conditions(
    tmp_path,
):
    path = (
        tmp_path
        / "registry.jsonl"
    )

    rows = [
        {"condition_id": "0xA"},
        {"condition_id": "0xB"},
    ]

    raw = "".join(
        json.dumps(row)
        + "\n"
        for row in rows
    )

    path.write_text(
        raw,
        encoding="utf-8",
    )

    expected_sha = (
        hashlib.sha256(
            path.read_bytes()
        ).hexdigest()
    )

    values = (
        m.load_jsonl_conditions(
            path,
            expected_sha=
                expected_sha,
            expected_rows=2,
        )
    )

    assert values == {
        "0xa",
        "0xb",
    }


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
