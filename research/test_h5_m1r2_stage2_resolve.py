from __future__ import annotations

from datetime import (
    datetime,
    timedelta,
    timezone,
)
from pathlib import Path

import pytest

from research import (
    h5_m1r2_stage2_resolve as r,
)


NOW = datetime(
    2026,
    8,
    24,
    22,
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
            + timedelta(hours=24)
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
            + timedelta(hours=24)
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


def resolve(
    stage_rows,
    provider_rows,
    burned=None,
):
    if burned is None:
        burned = set()

    return r.resolve_rows(
        stage_rows,
        provider_rows,
        burned,
        resolved_at=NOW,
        resolver_sha="resolver",
    )


def test_unique_exact_team_mapping():
    resolved, status = resolve(
        [stage1()],
        [provider()],
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


def test_home_away_order_does_not_matter():
    resolved, _ = resolve(
        [stage1()],
        [
            provider(
                home="Team A",
                away="Team B",
            )
        ],
    )

    assert len(resolved) == 1


def test_delta_600_inclusive():
    s = stage1()

    start = (
        r.parse_dt(
            s[
                "pm_game_start_time"
            ]
        )
        + timedelta(seconds=600)
    )

    resolved, _ = resolve(
        [s],
        [
            provider(
                start=start
            )
        ],
    )

    assert len(resolved) == 1


def test_delta_601_pending():
    s = stage1()

    start = (
        r.parse_dt(
            s[
                "pm_game_start_time"
            ]
        )
        + timedelta(seconds=601)
    )

    resolved, statuses = resolve(
        [s],
        [
            provider(
                start=start
            )
        ],
    )

    assert resolved == []

    assert (
        statuses[0]["status"]
        ==
        "PENDING_PROVIDER_VISIBILITY"
    )


def test_wrong_team_pending():
    resolved, statuses = resolve(
        [stage1()],
        [
            provider(
                home="Other A",
                away="Other B",
            )
        ],
    )

    assert resolved == []

    assert (
        statuses[0]["status"]
        ==
        "PENDING_PROVIDER_VISIBILITY"
    )


def test_ambiguous_fails_closed():
    resolved, statuses = resolve(
        [stage1()],
        [
            provider(
                event_id="one"
            ),
            provider(
                event_id="two"
            ),
        ],
    )

    assert resolved == []

    assert (
        statuses[0]["status"]
        ==
        "AMBIGUOUS_FAIL_CLOSED"
    )


def test_h2_burned_classification():
    resolved, statuses = resolve(
        [stage1()],
        [provider()],
        {"event-1"},
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
        statuses[0]["status"]
        ==
        "RESOLVED_H2_BURNED_PROHIBITED"
    )


def test_athletics_alias():
    resolved, _ = resolve(
        [
            stage1(
                p1="Oakland Athletics",
                p0="Team B",
            )
        ],
        [
            provider(
                home="Team B",
                away="Athletics",
            )
        ],
    )

    assert len(resolved) == 1


def test_provider_projection_strips_extra_fields():
    payload = [
        {
            "id":
                "event-1",

            "home_team":
                "Team A",

            "away_team":
                "Team B",

            "commence_time":
                (
                    NOW
                    + timedelta(hours=24)
                ).isoformat(),

            "bookmakers": [
                {"secret": "not allowed"}
            ],

            "price":
                0.5,
        }
    ]

    events, projection = (
        r.parse_provider_payload(
            payload
        )
    )

    assert len(events) == 1

    assert set(
        projection[0]
    ) == {
        "id",
        "home_team",
        "away_team",
        "commence_time",
    }

    assert (
        "bookmakers"
        not in projection[0]
    )

    assert (
        "price"
        not in projection[0]
    )


def test_provider_projection_deterministic():
    t = (
        NOW
        + timedelta(hours=24)
    ).isoformat()

    a = [
        {
            "id": "b",
            "home_team": "B",
            "away_team": "A",
            "commence_time": t,
        },
        {
            "id": "a",
            "home_team": "D",
            "away_team": "C",
            "commence_time": t,
        },
    ]

    b = list(
        reversed(a)
    )

    _, p1 = r.parse_provider_payload(a)
    _, p2 = r.parse_provider_payload(b)

    assert (
        r.provider_projection_bytes(p1)
        ==
        r.provider_projection_bytes(p2)
    )


def test_conflicting_duplicate_provider_id_fails():
    t = (
        NOW
        + timedelta(hours=24)
    ).isoformat()

    payload = [
        {
            "id": "same",
            "home_team": "A",
            "away_team": "B",
            "commence_time": t,
        },
        {
            "id": "same",
            "home_team": "C",
            "away_team": "D",
            "commence_time": t,
        },
    ]

    with pytest.raises(
        RuntimeError
    ):
        r.parse_provider_payload(
            payload
        )


def statuses(
    nonburned=0,
    pending=0,
    burned=0,
    ambiguous=0,
):
    out = []

    out += [
        {"status": "RESOLVED_NONBURNED"}
        for _ in range(nonburned)
    ]

    out += [
        {
            "status":
                "PENDING_PROVIDER_VISIBILITY"
        }
        for _ in range(pending)
    ]

    out += [
        {
            "status":
                "RESOLVED_H2_BURNED_PROHIBITED"
        }
        for _ in range(burned)
    ]

    out += [
        {
            "status":
                "AMBIGUOUS_FAIL_CLOSED"
        }
        for _ in range(ambiguous)
    ]

    return out


def test_gate_passes_with_five_nonburned():
    counts = r.validate_gate(
        statuses(
            nonburned=5,
            pending=5,
        )
    )

    assert (
        counts[
            "RESOLVED_NONBURNED"
        ]
        == 5
    )


def test_gate_fails_below_five():
    with pytest.raises(
        RuntimeError
    ):
        r.validate_gate(
            statuses(
                nonburned=4,
                pending=6,
            )
        )


def test_gate_fails_any_ambiguity():
    with pytest.raises(
        RuntimeError
    ):
        r.validate_gate(
            statuses(
                nonburned=9,
                ambiguous=1,
            )
        )


def test_deadline_exactly_allowed():
    rows = [
        stage1(
            start=(
                NOW
                + timedelta(hours=24)
            )
        )
    ]

    deadline = (
        r.stage2_deadline(
            rows
        )
    )

    assert (
        r.validate_deadline(
            rows,
            now=deadline,
        )
        == deadline
    )


def test_deadline_after_is_rejected():
    rows = [
        stage1(
            start=(
                NOW
                + timedelta(hours=24)
            )
        )
    ]

    deadline = (
        r.stage2_deadline(
            rows
        )
    )

    with pytest.raises(
        RuntimeError
    ):
        r.validate_deadline(
            rows,
            now=(
                deadline
                + timedelta(
                    microseconds=1
                )
            ),
        )


def test_serialization_preserves_registration_order():
    data = r.serialize_rows([
        {
            "registration_index": 2,
            "condition_id": "b",
        },
        {
            "registration_index": 1,
            "condition_id": "a",
        },
    ]).decode()

    lines = data.splitlines()

    assert (
        '"registration_index":1'
        in lines[0]
    )

    assert (
        '"registration_index":2'
        in lines[1]
    )


def test_create_exclusive_refuses_overwrite(
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
        == b"first\n"
    )


def test_source_has_no_preview_or_database_dependency():
    source = (
        Path(
            r.__file__
        )
        .read_text(
            encoding="utf-8"
        )
        .lower()
    )

    assert "--preview" not in source
    assert "psycopg2" not in source
    assert "postgres" not in source
    assert "/odds" not in source

    assert (
        r.EVENTS_URL.endswith(
            "/events"
        )
    )
