from __future__ import annotations

import hashlib
import json
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
R = REPO / "research"

PARENT_FILES = {
    "m2_contract":
        R / "h2_v2_m2_contract.json",

    "m2_builder":
        R / "h2_v2_m2_build.py",

    "m2_tape":
        R / "h2_v2_m2_tape.jsonl",

    "m2_validation":
        R / "h2_v2_m2_validation.json",

    "m2_failure_diagnosis":
        R / "h2_v2_m2_failure_diagnosis.json",
}

EXPECTED_PARENT_HASHES = {
    "m2_contract":
        "4a09e3124bd0e05624a577916b10a712802bcc1096f6218a765c3830cf239cc4",

    "m2_builder":
        "324ccba8cc74d0853de640d5758c3cc77c7fbba30cad81022adac138e5022ab8",

    "m2_tape":
        "88a1bd4cbaf3254850c0dc1c4c2c305e18db68700a5e90d7e86f64a74706d312",

    "m2_validation":
        "06983e20c4fcb1d09254221e99213a2e8497088263fb20d95955a884e7005f5e",

    "m2_failure_diagnosis":
        "481ef1eb7116da2bf83490253080e400cd10265fb03d6e465fca5bc1a92bb8b7",
}

PARENT_COMMIT = (
    "1eb30e3ac345341aa2dd420fc407e44f3733e573"
)

OUT = R / "h2_v2_m2b_contract.json"


def sha256_file(path: Path) -> str:
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def main():
    actual = {
        name: sha256_file(path)
        for name, path
        in PARENT_FILES.items()
    }

    if actual != EXPECTED_PARENT_HASHES:
        raise SystemExit(
            "M2 parent artifact SHA mismatch"
        )

    diagnosis = json.loads(
        PARENT_FILES[
            "m2_failure_diagnosis"
        ].read_text(
            encoding="utf-8"
        )
    )

    assert (
        diagnosis["m2_result"]
        == "FAIL"
    )

    assert (
        diagnosis[
            "diagnosis"
        ][
            "reconstruction_failure_demonstrated"
        ]
        is False
    )

    assert (
        diagnosis[
            "diagnosis"
        ][
            "reference_size_staleness_demonstrated"
        ]
        is True
    )

    contract = {
        "version":
            "H2-v2-M2b-CONTRACT-1",

        "milestone":
            (
                "H2-v2 M2b — replay-validation "
                "remediation"
            ),

        "parent": {
            "m2_commit":
                PARENT_COMMIT,

            "artifact_sha256":
                EXPECTED_PARENT_HASHES,

            "m2_result":
                "FAIL",

            "m2_failure_cause":
                (
                    "Latest size-bearing TOB was "
                    "an invalid endpoint size "
                    "ground truth after later "
                    "same-level deltas."
                ),

            "retroactively_change_m2":
                False,
        },

        "engineering_dataset": {
            "sharp_collector_run_id":
                11691,

            "expected_anchor_rows":
                1080,

            "expected_games":
                9,

            "target_tokens":
                18,

            "performance_use":
                False,
        },

        "window": {
            "start":
                (
                    "minimum decision_time among "
                    "eligible run-11691 anchors"
                ),

            "end":
                (
                    "maximum decision_time among "
                    "eligible run-11691 anchors "
                    "+ 500ms"
                ),
        },

        "primary_checkpoint_test": {
            "purpose":
                (
                    "Independently validate exact "
                    "price-level and size replay "
                    "semantics."
                ),

            "pair_definition":
                (
                    "Consecutive H2-v2 full-book "
                    "snapshots for the same target "
                    "token within the engineering "
                    "window."
                ),

            "base_sources": [
                "ws_book",
                "rest_book",
            ],

            "checkpoint_source":
                "ws_book",

            "same_generation_required":
                True,

            "delta_bearing_required":
                True,

            "capacity_observed_before_validation": {
                "consecutive_pairs":
                    268,

                "same_generation_pairs":
                    160,

                "same_generation_delta_pairs":
                    142,

                "generation_boundary_pairs":
                    108,

                "clean_boundary_delta_pairs":
                    102,

                "intermediate_generation_contamination":
                    0,
            },

            "outcome_match_information_inspected_before_freeze":
                False,
        },

        "replay_rule": {
            "base":
                (
                    "Start from every price/size "
                    "level in the earlier full-book "
                    "snapshot."
                ),

            "generation":
                (
                    "Replay only deltas with the "
                    "same book_generation as base "
                    "and checkpoint."
                ),

            "ws_base":
                (
                    "Base and checkpoint must share "
                    "connection_id. Replay deltas on "
                    "that connection with "
                    "base.ingest_sequence < "
                    "delta.ingest_sequence < "
                    "checkpoint.ingest_sequence."
                ),

            "rest_base":
                (
                    "Checkpoint connection_id defines "
                    "the WS continuation. Replay "
                    "same-generation deltas on that "
                    "connection with capture_time "
                    "> base.capture_time and "
                    "ingest_sequence < "
                    "checkpoint.ingest_sequence."
                ),

            "order": [
                "ingest_sequence ASC",
                "change_index ASC",
                "capture_time ASC",
            ],

            "delta_semantics": {
                "B":
                    (
                        "size > 0 sets/replaces bid "
                        "level; size = 0 removes it"
                    ),

                "S":
                    (
                        "size > 0 sets/replaces ask "
                        "level; size = 0 removes it"
                    ),
            },

            "comparison":
                (
                    "After replay, normalized complete "
                    "bid and ask maps must exactly "
                    "equal the checkpoint ws_book "
                    "maps, including every non-zero "
                    "price level and exact Decimal "
                    "size."
                ),

            "price_tolerance_mc":
                0,

            "size_tolerance":
                "0",

            "no_lookahead":
                True,
        },

        "primary_success_rule": {
            "use_all_eligible_pairs":
                True,

            "minimum_checkpoint_pairs":
                100,

            "minimum_distinct_tokens":
                9,

            "require_both_base_sources":
                True,

            "full_book_exact_match_rate":
                "100%",

            "bid_level_mismatches":
                0,

            "ask_level_mismatches":
                0,

            "size_mismatches":
                0,

            "generation_contamination":
                0,

            "connection_contamination":
                0,

            "sequence_violations":
                0,

            "lookahead":
                0,
        },

        "endpoint_requirements_inherited_from_m2": {
            "tape_rows":
                1080,

            "games":
                9,

            "market_mapping":
                "1080/1080",

            "outcome_token_mapping":
                "1080/1080",

            "configuration":
                "1080/1080",

            "decision_reconstruction":
                "1080/1080",

            "execution_500ms_reconstruction":
                "1080/1080",

            "decision_reference_price_agreement":
                "1080/1080",

            "execution_reference_price_agreement":
                "1080/1080",

            "generation_mixing":
                0,

            "connection_mixing":
                0,

            "crossed_books":
                0,

            "lookahead":
                0,
        },

        "m2b_interpretation": {
            "pass_means":
                (
                    "The synchronized M2 decision "
                    "tape is engineering-valid under "
                    "the corrected independently "
                    "preregistered replay-size "
                    "validation method."
                ),

            "does_not_change_m2_result":
                True,

            "does_not_establish_trading_edge":
                True,
        },

        "secondary_diagnostic_only": {
            "generation_boundary_pairs":
                (
                    "May be reported separately but "
                    "cannot determine M2b PASS/FAIL "
                    "because REST checkpoint timing "
                    "is asynchronous to WS ordering."
                ),
        },

        "forbidden_in_m2b": [
            "edge calculation",
            "edge distribution inspection",
            "candidate threshold evaluation",
            "trade selection",
            "paper fills",
            "PnL calculation",
            "resolution outcome use",
            "strategy calibration",
            "OOS strategy testing",
        ],
    }

    OUT.write_text(
        json.dumps(
            contract,
            indent=2,
            sort_keys=True,
        ) + "\n",
        encoding="utf-8",
    )

    print(
        "H2-v2 M2b CONTRACT VALIDATION PASSED"
    )

    print(
        "parent M2 commit:",
        PARENT_COMMIT,
    )

    print(
        "primary capacity:",
        "142 same-generation delta pairs",
    )

    print(
        "minimum evidence:",
        ">=100 pairs / >=9 tokens",
    )

    print(
        "required full-book match:",
        "100%",
    )

    print(
        "NO EDGE / NO TRADES / NO PNL"
    )


if __name__ == "__main__":
    main()
