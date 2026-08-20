from __future__ import annotations

from datetime import (
    datetime,
    timedelta,
    timezone,
)

import pytest

import h5_m1_stage2_resolve as r


NOW = datetime(
    2026,
    8,
    20,
    21,
    0,
    tzinfo=timezone.utc,
)


def stage1(
    index=1,
    condition="0xabc",
    p1="Team A",
    p0="Team B",
    start=None,
):
    if start is None:
        start = (
            NOW
            + timedelta(hours=8)
        )

    return {
        "registration_index":
            index,

        "condition_id":
            condition,

        "gamma_market_id":
            str(index),

        "gamma_slug":
            f"market-{index}",

        "p1_team":
            p1,

        "p0_team":
            p0,

        "pm_game_start_time":
            start.isoformat(),
    }


def provider(
    event_id="event-1",
    home="Team B",
    away="Team A",
    start=None,
):
    if start is None:
        start = (
            NOW
            + timedelta(hours=8)
        )

    return {
        "odds_game_id":
            event_id,

        "canonical_commence_time":
            start,

        "home_team":
            home,

        "away_team":
            away,

        "team_set": {
            r.norm_team(home),
            r.norm_team(away),
        },
    }


def test_unique_exact_team_mapping():
    resolved, status = (
        r.resolve_rows(
            [stage1()],
            [provider()],
            set(),
            resolved_at=NOW,
        )
    )

    assert len(resolved) == 1
    assert (
        resolved[0][
            "resolution_status"
        ]
        ==
        "RESOLVED_NONBURNED"
    )

    assert (
        status[0]["status"]
        ==
        "RESOLVED_NONBURNED"
    )


def test_delta_600_inclusive():
    s = stage1()

    start = (
        r.parse_dt(
            s["pm_game_start_time"]
        )
        + timedelta(seconds=600)
    )

    resolved, _ = (
        r.resolve_rows(
            [s],
            [
                provider(
                    start=start
                )
            ],
            set(),
            resolved_at=NOW,
        )
    )

    assert len(resolved) == 1


def test_delta_over_600_pending():
    s = stage1()

    start = (
        r.parse_dt(
            s["pm_game_start_time"]
        )
        + timedelta(seconds=601)
    )

    resolved, status = (
        r.resolve_rows(
            [s],
            [
                provider(
                    start=start
                )
            ],
            set(),
            resolved_at=NOW,
        )
    )

    assert resolved == []

    assert (
        status[0]["status"]
        ==
        "PENDING_PROVIDER_VISIBILITY"
    )


def test_wrong_team_pending():
    resolved, status = (
        r.resolve_rows(
            [stage1()],
            [
                provider(
                    home="Other B",
                    away="Other A",
                )
            ],
            set(),
            resolved_at=NOW,
        )
    )

    assert resolved == []

    assert (
        status[0]["status"]
        ==
        "PENDING_PROVIDER_VISIBILITY"
    )


def test_ambiguous_fails_closed_status():
    event1 = provider(
        event_id="one"
    )

    event2 = provider(
        event_id="two"
    )

    resolved, status = (
        r.resolve_rows(
            [stage1()],
            [
                event1,
                event2,
            ],
            set(),
            resolved_at=NOW,
        )
    )

    assert resolved == []

    assert (
        status[0]["status"]
        ==
        "AMBIGUOUS_FAIL_CLOSED"
    )


def test_burned_classification():
    resolved, status = (
        r.resolve_rows(
            [stage1()],
            [provider()],
            {"event-1"},
            resolved_at=NOW,
        )
    )

    assert len(resolved) == 1

    assert (
        resolved[0][
            "resolution_status"
        ]
        ==
        "RESOLVED_H2_BURNED_PROHIBITED"
    )

    assert (
        status[0]["status"]
        ==
        "RESOLVED_H2_BURNED_PROHIBITED"
    )


def test_stage1_order_preserved_in_serialization():
    rows = [
        {
            "registration_index":
                2,
            "condition_id":
                "b",
        },
        {
            "registration_index":
                1,
            "condition_id":
                "a",
        },
    ]

    data = (
        r.serialize_rows(
            rows
        ).decode()
    )

    lines = (
        data.splitlines()
    )

    assert (
        '"registration_index":1'
        in lines[0]
    )

    assert (
        '"registration_index":2'
        in lines[1]
    )


def test_exclusive_create_refuses_overwrite(
    tmp_path,
):
    path = (
        tmp_path
        / "map.jsonl"
    )

    r.create_exclusive(
        path,
        b"first\n",
    )

    with pytest.raises(
        FileExistsError
    ):
        r.create_exclusive(
            path,
            b"second\n",
        )

    assert (
        path.read_bytes()
        ==
        b"first\n"
    )
