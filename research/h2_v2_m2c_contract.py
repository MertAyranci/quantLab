from __future__ import annotations

import hashlib
import json
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
R = REPO / "research"

PARENT_COMMIT = (
    "2a40bf8cb220babf2a7364806bfc6d1abce68627"
)

PARENT_FILES = {
    "m2b_contract":
        R / "h2_v2_m2b_contract.json",

    "m2b_validator":
        R / "h2_v2_m2b_validate.py",

    "m2b_results":
        R / "h2_v2_m2b_checkpoint_results.jsonl",

    "m2b_validation":
        R / "h2_v2_m2b_validation.json",

    "m2b_failure_diagnosis":
        R / "h2_v2_m2b_failure_diagnosis.json",
}

EXPECTED_PARENT_HASHES = {
    "m2b_contract":
        "86babc92e1a32bbe95361bd66a24d09b0c729082589ea8cd7a5ea18e8bbfdb4c",

    "m2b_validator":
        "95e1faf114bb6d05297be93b909bcbf072a8e7718a47260bc7fb69acaa387506",

    "m2b_results":
        "c058cd37fbdfed83f7c2909577ecd270f6608ce0d738af84cb486baab58dc66c",

    "m2b_validation":
        "72af20049ef04a0f2ffbc2aeb9cc567b454a10ed7724700581c23f197864444d",

    "m2b_failure_diagnosis":
        "323502b7eda28aeeafd89e3ebab1803b750b4629550cc296a209d0faaefe2f52",
}

OUT = (
    R / "h2_v2_m2c_contract.json"
)


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
            "M2b parent artifact SHA mismatch"
        )

    diagnosis = json.loads(
        PARENT_FILES[
            "m2b_failure_diagnosis"
        ].read_text(
            encoding="utf-8"
        )
    )

    assert (
        diagnosis["m2b_result"]
        == "FAIL"
    )

    assert (
        diagnosis[
            "result"
        ][
            "failed_pairs"
        ]
        == 58
    )

    assert (
        diagnosis[
            "result"
        ][
            "size_mismatches"
        ]
        == 114
    )

    assert (
        diagnosis[
            "result"
        ][
            "bid_level_mismatches"
        ]
        == 0
    )

    assert (
        diagnosis[
            "result"
        ][
            "ask_level_mismatches"
        ]
        == 0
    )

    contract = {
        "version":
            "H2-v2-M2c-CONTRACT-1",

        "milestone":
            (
                "H2-v2 M2c — exact size "
                "divergence root-cause investigation"
            ),

        "parent": {
            "m2b_commit":
                PARENT_COMMIT,

            "artifact_sha256":
                EXPECTED_PARENT_HASHES,

            "m2b_result":
                "FAIL",

            "m2b_failed_pairs":
                58,

            "m2b_size_mismatches":
                114,

            "m2b_price_level_mismatches":
                0,
        },

        "scope": {
            "engineering_only":
                True,

            "modify_m2":
                False,

            "modify_m2b":
                False,

            "modify_collector":
                False,

            "modify_database":
                False,

            "new_external_data":
                False,

            "performance_data":
                False,
        },

        "hypothesis": {
            "id":
                "trade_triggered_ws_book",

            "statement":
                (
                    "Some or all M2b exact-size "
                    "divergences arise because the "
                    "terminal ws_book is itself a "
                    "trade-triggered state update. "
                    "The trade-induced size mutation "
                    "therefore need not appear in "
                    "price_change rows preceding that "
                    "ws_book."
                ),

            "status_before_investigation":
                "UNTESTED",

            "motivation":
                (
                    "Official Polymarket market-channel "
                    "documentation states that book "
                    "events are emitted on subscription "
                    "and when a trade affects the book, "
                    "while price_change events are "
                    "associated with new orders and "
                    "cancellations."
                ),
        },

        "investigation_dataset": {
            "source":
                "frozen M2b checkpoint results",

            "eligible_pairs":
                142,

            "failed_pairs":
                58,

            "passed_pairs":
                84,

            "sharp_collector_run_id":
                11691,
        },

        "required_diagnostics": {
            "pair_level_reconstruction":
                (
                    "Reconstruct every M2b pair again "
                    "only to recover the exact levels "
                    "and size differences already "
                    "tested in M2b."
                ),

            "size_direction":
                (
                    "For each mismatched level record "
                    "replayed size, checkpoint size, "
                    "and checkpoint-minus-replay size."
                ),

            "server_time_trade_proximity":
                (
                    "For each checkpoint locate the "
                    "nearest same-token last_trade_event "
                    "by absolute event_time distance."
                ),

            "sequence_trade_proximity":
                (
                    "Where connection provenance exists, "
                    "record checkpoint ingest_sequence "
                    "minus nearest same-connection "
                    "last_trade ingest_sequence."
                ),

            "exact_event_time_trade":
                (
                    "Record whether a same-token "
                    "last_trade_event has exactly the "
                    "same server event_time as the "
                    "checkpoint ws_book."
                ),

            "trade_side_consistency":
                (
                    "For associated last trades, test "
                    "whether BUY aligns with ask-side "
                    "liquidity reduction and SELL "
                    "aligns with bid-side liquidity "
                    "reduction."
                ),

            "pass_pair_control":
                (
                    "Run the same trade-proximity "
                    "diagnostics for the 84 exact-match "
                    "pairs as a control."
                ),
        },

        "predeclared_summary": {
            "report": [
                "failed vs passed pair counts",
                "mismatch count by bid/ask side",
                "checkpoint size lower/equal/higher than replay",
                "trade proximity distributions for failed and passed pairs",
                "exact same-event-time trade counts",
                "adjacent-sequence trade counts",
                "trade-side consistency counts",
                "results by ws_book vs rest_book base"
            ],

            "no_posthoc_exclusions":
                True,
        },

        "decision_categories": {
            "TRADE_TRIGGER_SUPPORTED":
                (
                    "Size divergence is systematically "
                    "associated with trade-triggered "
                    "checkpoint mutations and the "
                    "direction/side evidence is "
                    "consistent with liquidity being "
                    "consumed."
                ),

            "TRADE_TRIGGER_PARTIAL":
                (
                    "Trade-triggered checkpoints explain "
                    "a material subset but not all size "
                    "divergences."
                ),

            "TRADE_TRIGGER_NOT_SUPPORTED":
                (
                    "Failed pairs are not systematically "
                    "associated with trade-triggered "
                    "checkpoint mutations."
                ),

            "INCONCLUSIVE":
                (
                    "Stored provenance is insufficient "
                    "to distinguish the mechanisms."
                ),
        },

        "important_methodology": {
            "m2c_is_diagnostic":
                True,

            "no_pass_rate_threshold":
                True,

            "reason":
                (
                    "M2c is a root-cause investigation, "
                    "not a strategy test or an attempt "
                    "to rescue M2b."
                ),

            "next_milestone_if_cause_identified":
                (
                    "Any remediation or new replay "
                    "acceptance test must be frozen in "
                    "a separate milestone after M2c "
                    "is committed and stopped."
                ),
        },

        "forbidden_in_m2c": [
            "change M2 result",
            "change M2b result",
            "relax M2b 100% criterion",
            "edge calculation",
            "edge distribution inspection",
            "candidate threshold evaluation",
            "trade selection",
            "paper fills",
            "PnL calculation",
            "resolution outcome use",
            "strategy calibration",
            "OOS strategy testing"
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
        "H2-v2 M2c CONTRACT VALIDATION PASSED"
    )

    print(
        "parent M2b commit:",
        PARENT_COMMIT,
    )

    print(
        "frozen failed pairs:",
        58,
    )

    print(
        "frozen size mismatches:",
        114,
    )

    print(
        "hypothesis:",
        "trade_triggered_ws_book",
    )

    print(
        "M2c IS DIAGNOSTIC — NO RESCUE CRITERION"
    )

    print(
        "NO EDGE / NO TRADES / NO PNL"
    )


if __name__ == "__main__":
    main()
