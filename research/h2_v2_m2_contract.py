from __future__ import annotations

import hashlib
import json
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
R = REPO / "research"

PARENT_VALIDATION = (
    R / "h2_v2_m1b_validation.json"
)

OUT = (
    R / "h2_v2_m2_contract.json"
)

EXPECTED_PARENT_SHA = (
    "b27911859231d3a1d80202b3022b9f27"
    "33b653487ef2caefe4d31d4da175400b"
)

PARENT_COMMIT = (
    "9e6fc8338dbd0fb86ea5b4ac6a29a69af4e1e311"
)


def sha256_file(path: Path) -> str:
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def main():

    parent_sha = sha256_file(
        PARENT_VALIDATION
    )

    if parent_sha != EXPECTED_PARENT_SHA:
        raise SystemExit(
            "M1b validation SHA mismatch"
        )

    contract = {
        "version":
            "H2-v2-M2-CONTRACT-1",

        "milestone":
            (
                "H2-v2 M2 — synchronized sharp "
                "and Polymarket decision tape"
            ),

        "parent": {
            "m1b_commit":
                PARENT_COMMIT,

            "m1b_validation_sha256":
                EXPECTED_PARENT_SHA,

            "m1b_result":
                "PASS",

            "operational_universe":
                "0 < time_to_game <= 23h",

            "sharp_max_source_age_seconds":
                25,

            "required_fresh_families":
                2,
        },

        "engineering_dataset": {
            "sharp_collector_run_id":
                11691,

            "expected_games":
                9,

            "expected_game_snapshots":
                540,

            "expected_outcome_anchors":
                1080,

            "use":
                (
                    "Engineering synchronization and "
                    "replay validation only."
                ),

            "performance_use":
                False,
        },

        "anchor_definition": {
            "source":
                "h2_v2_sharp_consensus",

            "requirements": [
                "collector_run_id = 11691",
                "eligible = true",
                "strictly pregame",
                "0 < time_to_game <= 23h",
            ],

            "one_anchor_per":
                (
                    "sharp poll x game x outcome"
                ),

            "decision_time":
                "sharp consensus capture_time",
        },

        "identity": {
            "market_mapping":
                "h2_v2_market_matches",

            "outcome_mapping":
                (
                    "exact normalized equality between "
                    "sharp outcome_team and Polymarket "
                    "token outcome"
                ),

            "required_tokens_per_market":
                2,

            "mapping_must_be_unique":
                True,
        },

        "endpoints": {
            "decision": {
                "offset_ms":
                    0,
            },

            "execution_500ms": {
                "offset_ms":
                    500,

                "reason":
                    (
                        "Frozen in H2-v1 M8 as the "
                        "primary paper-execution "
                        "latency allowance."
                    ),
            },
        },

        "book_reconstruction": {
            "allowed_base_sources": [
                "ws_book",
                "rest_book",
            ],

            "base_selection": (
                "Latest H2-v2 full book snapshot with "
                "capture_time <= target_time and "
                "non-null book_generation."
            ),

            "base_must_be_two_sided":
                True,

            "delta_source":
                "ws_price_change",

            "delta_window": (
                "base_time < delta.capture_time "
                "<= target_time"
            ),

            "delta_watchlist_rule":
                "h2_v2",

            "generation_rule": (
                "Replay only deltas whose "
                "book_generation exactly equals "
                "the selected base generation."
            ),

            "ws_base_connection_rule": (
                "For a ws_book base, replay deltas "
                "must have the same connection_id "
                "as the base."
            ),

            "rest_base_connection_rule": (
                "For a rest_book base, replay deltas "
                "within the selected generation must "
                "belong to at most one distinct "
                "connection_id."
            ),

            "delta_order": [
                "capture_time ASC",
                "ingest_sequence ASC",
                "change_index ASC",
            ],

            "delta_semantics": {
                "B":
                    (
                        "Set bid price level to size; "
                        "size=0 removes level."
                    ),

                "S":
                    (
                        "Set ask price level to size; "
                        "size=0 removes level."
                    ),
            },

            "derived_state": {
                "best_bid":
                    "maximum positive bid price",

                "best_ask":
                    "minimum positive ask price",

                "best_bid_size":
                    "size at derived best bid",

                "best_ask_size":
                    "size at derived best ask",
            },

            "no_lookahead":
                True,
        },

        "independent_crosscheck": {
            "reference":
                (
                    "Latest H2-v2 tob_snapshot with "
                    "capture_time <= endpoint target."
                ),

            "required_reference_coverage":
                "100%",

            "price_match":
                (
                    "Reconstructed best_bid_mc and "
                    "best_ask_mc must exactly equal "
                    "the reference TOB prices."
                ),

            "size_match":
                (
                    "If reference TOB contains a "
                    "non-null size, reconstructed size "
                    "must match it."
                ),

            "tolerance_price_mc":
                0,

            "base_age_is_gate":
                False,

            "note":
                (
                    "Base age is diagnostic only "
                    "because subsequent same-generation "
                    "deltas are replayed."
                ),
        },

        "polymarket_configuration": {
            "source":
                "h2_v2_pm_market_config",

            "selection":
                (
                    "Latest configuration observed "
                    "at or before decision_time."
                ),

            "required_fields": [
                "minimum_order_size",
                "minimum_tick_size",
                "fee_rate",
                "fee_exponent",
                "taker_only",
            ],

            "fee_is_computed_in_m2":
                False,
        },

        "tape": {
            "one_row_per":
                "sharp outcome anchor",

            "expected_rows":
                1080,

            "required_fields": [
                "sharp_consensus_id",
                "poll_id",
                "odds_game_id",
                "outcome_team",
                "decision_time",
                "consensus_prob",
                "families_used",
                "polymarket_market_id",
                "token_id",
                "pm_outcome",
                "decision_best_bid_mc",
                "decision_best_bid_size",
                "decision_best_ask_mc",
                "decision_best_ask_size",
                "execution_time",
                "execution_best_bid_mc",
                "execution_best_bid_size",
                "execution_best_ask_mc",
                "execution_best_ask_size",
                "decision_base_source",
                "decision_base_time",
                "decision_base_generation",
                "decision_replayed_deltas",
                "execution_base_source",
                "execution_base_time",
                "execution_base_generation",
                "execution_replayed_deltas",
                "config_observed_at",
                "minimum_order_size",
                "minimum_tick_size",
                "fee_rate",
                "fee_exponent",
                "taker_only",
            ],

            "edge_column_allowed":
                False,

            "trade_column_allowed":
                False,

            "pnl_column_allowed":
                False,

            "resolution_column_allowed":
                False,
        },

        "prebuild_audit": {
            "outcome_anchors":
                1080,

            "decision_reconstructable":
                1080,

            "execution_reconstructable":
                1080,

            "decision_generation_conflicts":
                0,

            "execution_generation_conflicts":
                0,

            "decision_connection_conflicts":
                0,

            "execution_connection_conflicts":
                0,

            "decision_bases": {
                "rest_book":
                    728,

                "ws_book":
                    352,
            },

            "execution_bases": {
                "rest_book":
                    728,

                "ws_book":
                    352,
            },

            "performance_data_inspected":
                False,
        },

        "success_rule": {
            "required": [
                "exactly 1080 tape rows",
                "exactly 9 distinct games",
                "100% unique market mapping",
                "100% unique outcome-token mapping",
                "100% decision endpoint reconstruction",
                "100% execution +500ms reconstruction",
                "zero generation mixing",
                "zero connection mixing",
                "100% two-sided reconstructed books",
                "100% PM configuration coverage",
                "100% independent TOB price agreement",
                "zero crossed reconstructed books",
                "zero lookahead observations",
            ],
        },

        "forbidden_in_m2": [
            "edge calculation",
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
        "H2-v2 M2 CONTRACT VALIDATION PASSED"
    )

    print(
        "parent M1b SHA:",
        parent_sha,
    )

    print(
        "engineering run:",
        11691,
    )

    print(
        "expected anchors:",
        1080,
    )

    print(
        "endpoints:",
        "decision / decision+500ms",
    )

    print(
        "base sources:",
        "ws_book + rest_book",
    )

    print(
        "NO EDGE / NO TRADES / NO PNL"
    )


if __name__ == "__main__":
    main()
