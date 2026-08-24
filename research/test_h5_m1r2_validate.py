from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "research"
    / "h5_m1r2_validate.py"
)

spec = importlib.util.spec_from_file_location(
    "h5_m1r2_validate",
    MODULE_PATH,
)

assert spec is not None
assert spec.loader is not None

m = importlib.util.module_from_spec(
    spec
)

spec.loader.exec_module(m)


def test_frozen_gate_constants():
    assert m.MIN_MARKETS == 5
    assert m.MIN_ACCEPTED == 100
    assert m.MIN_FRESH_COVERAGE == 0.80
    assert m.MIN_PM_COVERAGE == 0.95

    assert (
        m.EXPECTED_RUNTIME_INDEXES
        == [2, 4, 5, 6, 7, 8, 10]
    )


def test_authoritative_capture_is_fixed():
    assert (
        m.CAPTURE_ID
        == "h5m1r2_eng_20260824_a"
    )

    assert (
        m.RESULT
        ==
        (
            m.REPO
            / "data/research/h5_m1r2/"
              "validation/result.json"
        )
    )


def test_taxonomy_integrity_failure_wins():
    assert (
        m.classify_verdict(
            hard_integrity_pass=False,
            sufficiency_pass=True,
        )
        == "FAIL_INTEGRITY"
    )


def test_taxonomy_clean_but_insufficient():
    assert (
        m.classify_verdict(
            hard_integrity_pass=True,
            sufficiency_pass=False,
        )
        == "INCONCLUSIVE_ENGINEERING"
    )


def test_taxonomy_all_pass():
    assert (
        m.classify_verdict(
            hard_integrity_pass=True,
            sufficiency_pass=True,
        )
        ==
        "PASS_ENGINEERING_FEASIBILITY"
    )


def test_all_false_requires_nonempty_dict():
    assert m.all_false({"a": False})
    assert not m.all_false({})
    assert not m.all_false(
        {"a": False, "b": True}
    )


def test_write_result_is_exclusive(
    tmp_path,
    monkeypatch,
):
    out = tmp_path / "result.json"

    monkeypatch.setattr(
        m,
        "RESULT",
        out,
    )

    m.write_report_exclusive(
        {"status": "TEST"}
    )

    with pytest.raises(
        FileExistsError
    ):
        m.write_report_exclusive(
            {"status": "TEST2"}
        )


def test_no_network_imports():
    source = MODULE_PATH.read_text(
        encoding="utf-8"
    )

    assert "import httpx" not in source
    assert "import requests" not in source
    assert "import websockets" not in source


def test_no_response_computation_functions():
    source = MODULE_PATH.read_text(
        encoding="utf-8"
    )

    for token in (
        "def calculate_innovation",
        "def calculate_response",
        "def calculate_markout",
        "def calculate_edge",
        "def calculate_pnl",
    ):
        assert token not in source


def test_consensus_probability_not_read():
    source = MODULE_PATH.read_text(
        encoding="utf-8"
    )

    assert (
        "consensus_p1_probability"
        not in source
    )


def test_cli_has_no_arbitrary_capture_selection():
    source = MODULE_PATH.read_text(
        encoding="utf-8"
    )

    assert (
        'add_argument(\n        "--capture-id"'
        not in source
    )

    assert (
        "--run-authoritative"
        in source
    )


def test_validator_binds_capture_receipt():
    assert (
        m.EXPECTED_CAPTURE_RECEIPT_SHA
        ==
        "ff70c5d8df4aa2966b3d565d7133a939"
        "410b0ab686d5cd7089fa1f2e15cf93f0"
    )


def test_validator_binds_validation_contract():
    assert (
        m.EXPECTED_VALIDATION_CONTRACT_SHA
        ==
        "4462598b64077f5fadbbe112a4eae6f5"
        "943d952b2fd7d1d1433a8f6430ecf8dc"
    )


def test_authoritative_read_requires_committed_self(
    monkeypatch,
):
    monkeypatch.setattr(
        m,
        "verify_sha",
        lambda *args, **kwargs: None,
    )

    monkeypatch.setattr(
        m,
        "read_json",
        lambda path: (
            {
                "study": "H5",
                "milestone": "H5-M1R2",
                "stage":
                    "ENGINEERING_VALIDATION",
                "engineering_sufficiency_gates": {
                    "minimum_distinct_acquisition_markets": 5,
                    "minimum_accepted_external_states": 100,
                    "fresh_external_family_coverage_minimum": 0.80,
                    "pm_book_coverage_minimum": 0.95,
                },
                "analysis_boundary": {
                    "a": False
                },
            }
            if path == m.VALIDATION_CONTRACT
            else
            {
                "study": "H5",
                "milestone": "H5-M1R2",
                "status":
                    "ENGINEERING_CAPTURE_FROZEN",
                "capture_id":
                    m.CAPTURE_ID,
                "additional_acquisition_permitted":
                    False,
                "capture_start_sha256":
                    m.EXPECTED_CAPTURE_START_SHA,
                "capture_complete_sha256":
                    m.EXPECTED_CAPTURE_COMPLETE_SHA,
                "collector_sha256":
                    m.EXPECTED_COLLECTOR_SHA,
                "analysis_boundary": {
                    "a": False
                },
            }
        ),
    )

    class Runtime:
        @staticmethod
        def load_runtime_bundle():
            return {
                "acquisition_market_count": 7,
                "runtime_registration_indexes":
                    [2, 4, 5, 6, 7, 8, 10],
            }

    monkeypatch.setattr(
        m,
        "load_module",
        lambda path, name: (
            Runtime
            if path == m.RUNTIME_MODULE
            else object()
        ),
    )

    monkeypatch.setattr(
        m,
        "git_is_ancestor",
        lambda commit: True,
    )

    monkeypatch.setattr(
        m,
        "committed_self_matches_head",
        lambda: False,
    )

    with pytest.raises(
        RuntimeError,
        match="validator differs",
    ):
        m.load_static_inputs(
            require_committed_self=True
        )
