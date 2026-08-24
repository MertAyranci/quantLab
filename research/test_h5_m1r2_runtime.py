from __future__ import annotations

from copy import deepcopy

import pytest

from research import (
    h5_m1r2_runtime as r,
)


def s1(
    index,
    condition=None,
):
    if condition is None:
        condition = f"0x{index}"

    return {
        "study":
            "H5",

        "milestone":
            "H5-M1R2",

        "registration_stage":
            "STAGE1_CONDITION_FREEZE",

        "registration_index":
            index,

        "condition_id":
            condition,

        "gamma_market_id":
            str(1000 + index),

        "gamma_slug":
            f"market-{index}",

        "pm_game_start_time":
            "2026-08-25T22:40:00+00:00",

        "p1_team":
            f"Team A{index}",

        "p0_team":
            f"Team B{index}",

        "p1_token_id":
            f"p1-{index}",

        "p0_token_id":
            f"p0-{index}",

        "external_identity_status":
            "PENDING_PROVIDER_VISIBILITY",

        "odds_game_id":
            None,
    }


def s2(
    index,
    condition=None,
):
    if condition is None:
        condition = f"0x{index}"

    return {
        "study":
            "H5",

        "milestone":
            "H5-M1R2",

        "registration_stage":
            "STAGE2_EXTERNAL_IDENTITY",

        "registration_index":
            index,

        "condition_id":
            condition,

        "gamma_market_id":
            str(1000 + index),

        "gamma_slug":
            f"market-{index}",

        "p1_team":
            f"Team A{index}",

        "p0_team":
            f"Team B{index}",

        "pm_game_start_time":
            "2026-08-25T22:40:00+00:00",

        "odds_game_id":
            f"event-{index}",

        "provider_home_team":
            f"Team B{index}",

        "provider_away_team":
            f"Team A{index}",

        "canonical_commence_time":
            "2026-08-25T22:40:00+00:00",

        "start_delta_seconds":
            0,

        "resolution_status":
            "RESOLVED_NONBURNED",

        "resolver_sha256":
            r.EXPECTED_STAGE2_RESOLVER_SHA,
    }


def full_stage1():
    return [
        s1(i)
        for i in range(
            1,
            11,
        )
    ]


def full_stage2():
    return [
        s2(i)
        for i in (
            2, 4, 5, 6, 7, 8, 10
        )
    ]


def test_norm_team_athletics_alias():
    assert (
        r.norm_team(
            "Oakland Athletics"
        )
        ==
        r.norm_team(
            "Athletics"
        )
    )


def test_join_exact_seven():
    rows = r.join_stage1_stage2(
        full_stage1(),
        full_stage2(),
    )

    assert len(rows) == 7


def test_join_preserves_registration_indexes():
    rows = r.join_stage1_stage2(
        full_stage1(),
        full_stage2(),
    )

    assert [
        row[
            "registration_index"
        ]
        for row in rows
    ] == [
        2, 4, 5, 6, 7, 8, 10
    ]


def test_join_preserves_stage1_tokens():
    rows = r.join_stage1_stage2(
        full_stage1(),
        full_stage2(),
    )

    assert (
        rows[0][
            "p1_token_id"
        ]
        == "p1-2"
    )

    assert (
        rows[0][
            "p0_token_id"
        ]
        == "p0-2"
    )


def test_join_adds_provider_identity():
    rows = r.join_stage1_stage2(
        full_stage1(),
        full_stage2(),
    )

    assert (
        rows[0][
            "odds_game_id"
        ]
        == "event-2"
    )

    assert (
        rows[0][
            "external_identity_status"
        ]
        ==
        "RESOLVED_NONBURNED"
    )


def test_join_detects_condition_mismatch():
    stage2 = full_stage2()

    stage2[0][
        "condition_id"
    ] = "wrong"

    with pytest.raises(
        RuntimeError
    ):
        r.join_stage1_stage2(
            full_stage1(),
            stage2,
        )


def test_join_detects_team_mismatch():
    stage2 = full_stage2()

    stage2[0][
        "p1_team"
    ] = "Wrong Team"

    with pytest.raises(
        RuntimeError
    ):
        r.join_stage1_stage2(
            full_stage1(),
            stage2,
        )


def test_runtime_rejects_renumbering():
    rows = r.join_stage1_stage2(
        full_stage1(),
        full_stage2(),
    )

    bad = deepcopy(
        rows
    )

    bad[0][
        "registration_index"
    ] = 1

    with pytest.raises(
        RuntimeError
    ):
        r.validate_runtime_rows(
            bad
        )


def test_runtime_rejects_duplicate_odds_id():
    rows = r.join_stage1_stage2(
        full_stage1(),
        full_stage2(),
    )

    bad = deepcopy(
        rows
    )

    bad[1][
        "odds_game_id"
    ] = bad[0][
        "odds_game_id"
    ]

    with pytest.raises(
        RuntimeError
    ):
        r.validate_runtime_rows(
            bad
        )


def test_runtime_rejects_duplicate_token():
    rows = r.join_stage1_stage2(
        full_stage1(),
        full_stage2(),
    )

    bad = deepcopy(
        rows
    )

    bad[1][
        "p1_token_id"
    ] = bad[0][
        "p1_token_id"
    ]

    with pytest.raises(
        RuntimeError
    ):
        r.validate_runtime_rows(
            bad
        )


def test_runtime_rejects_large_start_delta():
    rows = r.join_stage1_stage2(
        full_stage1(),
        full_stage2(),
    )

    bad = deepcopy(
        rows
    )

    bad[0][
        "canonical_commence_time"
    ] = (
        "2026-08-25T23:00:00+00:00"
    )

    with pytest.raises(
        RuntimeError
    ):
        r.validate_runtime_rows(
            bad
        )


def test_actual_frozen_bundle_loads():
    bundle = (
        r.load_runtime_bundle()
    )

    assert (
        bundle[
            "stage1_registered_market_count"
        ]
        == 10
    )

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


def test_actual_runtime_all_nonburned():
    bundle = (
        r.load_runtime_bundle()
    )

    assert all(
        row[
            "external_identity_status"
        ]
        ==
        "RESOLVED_NONBURNED"
        for row in bundle[
            "rows"
        ]
    )


def test_actual_runtime_unique_tokens():
    bundle = (
        r.load_runtime_bundle()
    )

    tokens = []

    for row in bundle[
        "rows"
    ]:
        tokens.extend([
            str(
                row[
                    "p1_token_id"
                ]
            ),
            str(
                row[
                    "p0_token_id"
                ]
            ),
        ])

    assert len(tokens) == 14
    assert len(set(tokens)) == 14


def test_loader_has_no_network_dependency():
    source = (
        r.Path(
            r.__file__
        )
        .read_text(
            encoding="utf-8"
        )
        .lower()
    )

    assert "httpx" not in source
    assert "requests" not in source
    assert "websocket" not in source
    assert "api.the-odds-api.com" not in source
    assert "clob.polymarket.com" not in source
