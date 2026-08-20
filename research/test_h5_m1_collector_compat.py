from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path

import pytest


MODULE_PATH = (
    Path(__file__).resolve().parent
    / "h5_m1_collector_compat.py"
)

spec = importlib.util.spec_from_file_location(
    "h5_m1_collector_compat",
    MODULE_PATH,
)

assert (
    spec is not None
    and
    spec.loader is not None
)

m = importlib.util.module_from_spec(
    spec
)

spec.loader.exec_module(
    m
)


def stage1_row(i: int):
    return {
        "study":
            "H5",

        "milestone":
            "H5-M1",

        "registration_stage":
            "STAGE1_CONDITION_FREEZE",

        "registration_index":
            i,

        "condition_id":
            f"0x{i:064x}",

        "gamma_market_id":
            f"gamma-{i}",

        "gamma_slug":
            f"game-{i}",

        "pm_game_start_time":
            f"2026-08-22T"
            f"{10 + i:02d}:00:00+00:00",

        "p1_team":
            f"Away {i}",

        "p0_team":
            f"Home {i}",

        "p1_token_id":
            f"p1-{i}",

        "p0_token_id":
            f"p0-{i}",

        "external_identity_status":
            "PENDING_PROVIDER_VISIBILITY",

        "odds_game_id":
            None,
    }


def stage1_rows():
    return [
        stage1_row(i)
        for i in range(1, 11)
    ]


def stage2_row(
    i: int,
    *,
    status="RESOLVED_NONBURNED",
):
    s1 = stage1_row(i)

    return {
        "study":
            "H5",

        "milestone":
            "H5-M1",

        "registration_stage":
            "STAGE2_EXTERNAL_IDENTITY",

        "registration_index":
            i,

        "condition_id":
            s1["condition_id"],

        "gamma_market_id":
            s1["gamma_market_id"],

        "gamma_slug":
            s1["gamma_slug"],

        "p1_team":
            s1["p1_team"],

        "p0_team":
            s1["p0_team"],

        "pm_game_start_time":
            s1[
                "pm_game_start_time"
            ],

        "odds_game_id":
            f"event-{i}",

        "provider_home_team":
            s1["p0_team"],

        "provider_away_team":
            s1["p1_team"],

        "canonical_commence_time":
            s1[
                "pm_game_start_time"
            ],

        "start_delta_seconds":
            0.0,

        "resolution_status":
            status,
    }


def test_static_frozen_constants():
    assert (
        m.MIN_RUNTIME_MARKETS
        == 5
    )

    assert (
        m.MAX_RUNTIME_MARKETS
        == 10
    )

    assert (
        m.MAX_START_DELTA_SECONDS
        == 600
    )

    assert (
        m.EXPECTED_COMPATIBILITY_CONTRACT_SHA
        ==
        "f7e69a2f4fc0734e62fea7abd5615b99"
        "25c86cf23438698ea014fa1a164eaecf"
    )


def test_historical_team_normalization():
    assert (
        m.norm_team(
            "Oakland Athletics"
        )
        ==
        "athletics"
    )

    assert (
        m.norm_team(
            "ATHLETICS"
        )
        ==
        "athletics"
    )

    assert (
        m.norm_team(
            "St. Louis Cardinals"
        )
        ==
        "st louis cardinals"
    )

    assert (
        m.norm_team(
            "St-Louis   Cardinals"
        )
        ==
        "st louis cardinals"
    )


def test_stage1_requires_exactly_ten_rows():
    rows = stage1_rows()

    m.validate_stage1_rows(
        rows
    )

    with pytest.raises(
        RuntimeError
    ):
        m.validate_stage1_rows(
            rows[:9]
        )


def test_stage1_requires_null_external_id():
    rows = stage1_rows()

    rows[0][
        "odds_game_id"
    ] = "should-not-exist"

    with pytest.raises(
        RuntimeError
    ):
        m.validate_stage1_rows(
            rows
        )


def test_noncontiguous_stage1_indexes_preserved():
    s1 = stage1_rows()

    s2 = [
        stage2_row(i)
        for i in (
            1,
            3,
            5,
            7,
            9,
        )
    ]

    runtime = (
        m.join_stage1_stage2(
            s1,
            s2,
        )
    )

    assert [
        row["registration_index"]
        for row in runtime
    ] == [
        1,
        3,
        5,
        7,
        9,
    ]


def test_pending_stage1_is_not_replaced():
    s1 = stage1_rows()

    s2 = [
        stage2_row(i)
        for i in (
            2,
            4,
            6,
            8,
            10,
        )
    ]

    runtime = (
        m.join_stage1_stage2(
            s1,
            s2,
        )
    )

    assert [
        row["registration_index"]
        for row in runtime
    ] == [
        2,
        4,
        6,
        8,
        10,
    ]


def test_burned_stage2_row_is_excluded_not_replaced():
    s1 = stage1_rows()

    s2 = [
        stage2_row(1),
        stage2_row(
            2,
            status=(
                "RESOLVED_H2_BURNED_PROHIBITED"
            ),
        ),
        stage2_row(3),
        stage2_row(4),
        stage2_row(5),
        stage2_row(6),
    ]

    runtime = (
        m.join_stage1_stage2(
            s1,
            s2,
        )
    )

    assert [
        row["registration_index"]
        for row in runtime
    ] == [
        1,
        3,
        4,
        5,
        6,
    ]


def test_join_requires_at_least_five_nonburned():
    s1 = stage1_rows()

    s2 = [
        stage2_row(i)
        for i in (
            1,
            3,
            5,
            7,
        )
    ]

    with pytest.raises(
        RuntimeError
    ):
        m.join_stage1_stage2(
            s1,
            s2,
        )


def test_condition_mismatch_fails_closed():
    s1 = stage1_rows()

    s2 = [
        stage2_row(i)
        for i in (
            1,
            2,
            3,
            4,
            5,
        )
    ]

    s2[2][
        "condition_id"
    ] = "0xbad"

    with pytest.raises(
        RuntimeError
    ):
        m.join_stage1_stage2(
            s1,
            s2,
        )


def test_gamma_identity_mismatch_fails_closed():
    s1 = stage1_rows()

    s2 = [
        stage2_row(i)
        for i in (
            1,
            2,
            3,
            4,
            5,
        )
    ]

    s2[0][
        "gamma_market_id"
    ] = "wrong"

    with pytest.raises(
        RuntimeError
    ):
        m.join_stage1_stage2(
            s1,
            s2,
        )


def test_team_equality_mismatch_fails_closed():
    s1 = stage1_rows()

    s2 = [
        stage2_row(i)
        for i in (
            1,
            2,
            3,
            4,
            5,
        )
    ]

    s2[0][
        "p1_team"
    ] = "Different"

    with pytest.raises(
        RuntimeError
    ):
        m.join_stage1_stage2(
            s1,
            s2,
        )


def test_duplicate_external_id_fails_closed():
    rows = [
        stage2_row(i)
        for i in (
            1,
            2,
            3,
            4,
            5,
        )
    ]

    rows[1][
        "odds_game_id"
    ] = rows[0][
        "odds_game_id"
    ]

    with pytest.raises(
        RuntimeError
    ):
        m.validate_stage2_rows(
            rows
        )


def test_stage2_rows_must_remain_in_stage1_order():
    rows = [
        stage2_row(i)
        for i in (
            1,
            5,
            3,
            7,
            9,
        )
    ]

    with pytest.raises(
        RuntimeError
    ):
        m.validate_stage2_rows(
            rows
        )


def test_runtime_row_uses_stage1_pm_and_stage2_external_fields():
    s1 = stage1_rows()

    s2 = [
        stage2_row(i)
        for i in (
            1,
            2,
            3,
            4,
            5,
        )
    ]

    runtime = (
        m.join_stage1_stage2(
            s1,
            s2,
        )
    )

    row = runtime[0]

    assert (
        row["condition_id"]
        ==
        s1[0]["condition_id"]
    )

    assert (
        row["p1_token_id"]
        ==
        s1[0]["p1_token_id"]
    )

    assert (
        row["odds_game_id"]
        ==
        s2[0]["odds_game_id"]
    )

    assert (
        row["home_team"]
        ==
        s2[0][
            "provider_home_team"
        ]
    )

    assert (
        row[
            "canonical_commence_time"
        ]
        ==
        s2[0][
            "canonical_commence_time"
        ]
    )


def test_join_does_not_mutate_stage1():
    s1 = stage1_rows()

    original = copy.deepcopy(
        s1
    )

    s2 = [
        stage2_row(i)
        for i in (
            1,
            2,
            3,
            4,
            5,
        )
    ]

    m.join_stage1_stage2(
        s1,
        s2,
    )

    assert s1 == original


def test_module_has_no_network_clients():
    source = MODULE_PATH.read_text(
        encoding="utf-8"
    )

    assert "import httpx" not in source
    assert "requests." not in source
    assert "websockets" not in source
    assert "ODDS_URL" not in source
    assert "CLOB" not in source


def test_module_has_no_response_or_pnl_logic():
    source = MODULE_PATH.read_text(
        encoding="utf-8"
    ).lower()

    forbidden = (
        "consensus_p1_probability",
        "directional_response",
        "future_midpoint",
        "markout =",
        "pnl =",
        "regression",
        "correlation",
    )

    for token in forbidden:
        assert token not in source



def synthetic_stage2_receipt(
    rows,
    *,
    map_sha,
    ambiguous_count=0,
):
    nonburned = [
        row
        for row in rows
        if (
            row["resolution_status"]
            ==
            "RESOLVED_NONBURNED"
        )
    ]

    burned = [
        row
        for row in rows
        if (
            row["resolution_status"]
            ==
            "RESOLVED_H2_BURNED_PROHIBITED"
        )
    ]

    return {
        "study":
            "H5",

        "milestone":
            "H5-M1",

        "status":
            "STAGE2_EXTERNAL_ID_MAP_FROZEN",

        "resolver": {
            "path":
                "research/h5_m1_stage2_resolve.py",

            "sha256":
                m.EXPECTED_STAGE2_RESOLVER_SHA,
        },

        "amendment_sha256":
            m.EXPECTED_VISIBILITY_AMENDMENT_SHA,

        "stage1_registry": {
            "path":
                "data/research/h5_m1/registry.jsonl",

            "sha256":
                m.EXPECTED_STAGE1_REGISTRY_SHA,

            "row_count":
                10,
        },

        "stage1_receipt_sha256":
            m.EXPECTED_STAGE1_RECEIPT_SHA,

        "h2_exclusions_sha256":
            m.EXPECTED_EXCLUSIONS_SHA,

        "external_id_map": {
            "path":
                "data/research/h5_m1/external_id_map.jsonl",

            "sha256":
                map_sha,

            "row_count":
                len(rows),

            "resolved_nonburned_count":
                len(nonburned),

            "resolved_h2_burned_count":
                len(burned),

            "pending_stage1_count":
                10 - len(rows),

            "ambiguous_count":
                ambiguous_count,

            "mutable":
                False,
        },

        "nonburned_condition_ids": [
            row["condition_id"]
            for row in nonburned
        ],

        "prohibited_h2_burned_condition_ids": [
            row["condition_id"]
            for row in burned
        ],

        "analysis_boundary": {
            "odds_bearing_endpoint_used":
                False,

            "external_odds_read":
                False,

            "external_consensus_calculated":
                False,

            "pm_prices_read":
                False,

            "h5_innovation_calculated":
                False,

            "pm_response_calculated":
                False,

            "f19_trade_content_read":
                False,

            "f20_read":
                False,

            "winner_used":
                False,

            "settlement_used":
                False,

            "pnl_calculated":
                False,
        },
    }


def write_synthetic_stage2(
    tmp_path,
    rows,
    *,
    ambiguous_count=0,
):
    map_path = (
        tmp_path
        / "external_id_map.jsonl"
    )

    receipt_path = (
        tmp_path
        / "external_id_map_receipt.json"
    )

    map_path.write_text(
        "".join(
            json.dumps(
                row,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
            for row in rows
        ),
        encoding="utf-8",
    )

    map_sha = m.sha256_file(
        map_path
    )

    receipt = (
        synthetic_stage2_receipt(
            rows,
            map_sha=map_sha,
            ambiguous_count=(
                ambiguous_count
            ),
        )
    )

    receipt_path.write_text(
        json.dumps(
            receipt,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    return (
        map_path,
        receipt_path,
    )


def test_runtime_bundle_binds_map_to_receipt(
    tmp_path,
    monkeypatch,
):
    s1 = stage1_rows()

    s2 = [
        stage2_row(i)
        for i in (
            1,
            3,
            5,
            7,
            9,
        )
    ]

    map_path, receipt_path = (
        write_synthetic_stage2(
            tmp_path,
            s2,
        )
    )

    monkeypatch.setattr(
        m,
        "STAGE2_MAP",
        map_path,
    )

    monkeypatch.setattr(
        m,
        "STAGE2_RECEIPT",
        receipt_path,
    )

    monkeypatch.setattr(
        m,
        "load_static_inputs",
        lambda: {
            "compatibility":
                {},

            "stage1_rows":
                s1,

            "stage1_receipt":
                {},

            "stage1_registry_sha256":
                m.EXPECTED_STAGE1_REGISTRY_SHA,

            "stage1_receipt_sha256":
                m.EXPECTED_STAGE1_RECEIPT_SHA,
        },
    )

    bundle = (
        m.load_runtime_bundle()
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
        == 5
    )

    assert [
        row["registration_index"]
        for row in bundle["rows"]
    ] == [
        1,
        3,
        5,
        7,
        9,
    ]

    assert (
        bundle[
            "stage2_map_sha256"
        ]
        ==
        m.sha256_file(map_path)
    )

    assert (
        bundle[
            "stage2_receipt_sha256"
        ]
        ==
        m.sha256_file(receipt_path)
    )


def test_runtime_bundle_rejects_map_sha_mismatch(
    tmp_path,
    monkeypatch,
):
    s1 = stage1_rows()

    s2 = [
        stage2_row(i)
        for i in (
            1,
            2,
            3,
            4,
            5,
        )
    ]

    map_path, receipt_path = (
        write_synthetic_stage2(
            tmp_path,
            s2,
        )
    )

    receipt = json.loads(
        receipt_path.read_text(
            encoding="utf-8"
        )
    )

    receipt[
        "external_id_map"
    ][
        "sha256"
    ] = "0" * 64

    receipt_path.write_text(
        json.dumps(
            receipt,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        m,
        "STAGE2_MAP",
        map_path,
    )

    monkeypatch.setattr(
        m,
        "STAGE2_RECEIPT",
        receipt_path,
    )

    monkeypatch.setattr(
        m,
        "load_static_inputs",
        lambda: {
            "compatibility":
                {},

            "stage1_rows":
                s1,

            "stage1_receipt":
                {},

            "stage1_registry_sha256":
                m.EXPECTED_STAGE1_REGISTRY_SHA,

            "stage1_receipt_sha256":
                m.EXPECTED_STAGE1_RECEIPT_SHA,
        },
    )

    with pytest.raises(
        RuntimeError,
        match=(
            "Stage-2 map SHA does not "
            "match freeze receipt"
        ),
    ):
        m.load_runtime_bundle()


def test_stage2_receipt_requires_zero_ambiguity(
    tmp_path,
):
    rows = [
        stage2_row(i)
        for i in (
            1,
            2,
            3,
            4,
            5,
        )
    ]

    map_path, receipt_path = (
        write_synthetic_stage2(
            tmp_path,
            rows,
            ambiguous_count=1,
        )
    )

    receipt = json.loads(
        receipt_path.read_text(
            encoding="utf-8"
        )
    )

    with pytest.raises(
        RuntimeError,
        match="ambiguity gate",
    ):
        m.validate_stage2_receipt(
            receipt,
            rows,
        )

    assert map_path.is_file()
