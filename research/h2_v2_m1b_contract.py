from __future__ import annotations

import hashlib
import json
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
R = REPO / "research"

PARENT_VALIDATION = (
    R / "h2_v2_m1_validation.json"
)

OUT = (
    R / "h2_v2_m1b_contract.json"
)

EXPECTED_PARENT_SHA = (
    "bcb7d8aeaba14e83813ca56a2b875237"
    "39b762a1b8a11594f0d4a51e74de6cec"
)

PARENT_COMMIT = (
    "7624d23ddc4191daaaa0e5a3a04801dbfcf66598"
)


def sha256_file(path: Path) -> str:
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def main():

    actual_parent_sha = (
        sha256_file(
            PARENT_VALIDATION
        )
    )

    if actual_parent_sha != EXPECTED_PARENT_SHA:
        raise SystemExit(
            "Parent M1 validation SHA mismatch"
        )

    contract = {
        "version":
            "H2-v2-M1b-CONTRACT-1",

        "milestone":
            "H2-v2 M1b — sharp feed remediation",

        "parent": {
            "m1_validation_sha256":
                EXPECTED_PARENT_SHA,

            "m1_commit":
                PARENT_COMMIT,

            "m1_result":
                "FAIL",

            "m1_sharp_integrity_pass":
                True,

            "m1_sharp_coverage_pass":
                False,

            "m1_polymarket_data_plane_pass":
                True,

            "m1_game_snapshots":
                700,

            "m1_eligible_game_snapshots":
                551,

            "m1_eligibility_rate":
                "0.787142857143",

            "m1_required_rate":
                "0.80",
        },

        "diagnosis": {
            "performance_data_inspected":
                False,

            "signal_data_inspected":
                False,

            "thresholds_inspected":
                False,

            "pnl_inspected":
                False,

            "failure_class":
                "external_source_operability",

            "smarkets": {
                "fresh_game_snapshots":
                    700,
                "total_game_snapshots":
                    700,
                "coverage":
                    "1.0",
            },

            "matchbook": {
                "le_22h_fresh":
                    520,
                "le_22h_total":
                    520,

                "le_23h_fresh":
                    520,
                "le_23h_total":
                    520,

                "23_24h_fresh":
                    31,
                "23_24h_total":
                    40,

                "gt_24h_observed_fresh":
                    0,

                "observed_transition": {
                    "game":
                        (
                            "Philadelphia Phillies "
                            "@ Minnesota Twins"
                        ),

                    "closest_not_fresh_hours":
                        "23.928",

                    "furthest_fresh_hours":
                        "23.920",
                },
            },

            "interpretation":
                (
                    "M1 sharp coverage failure was "
                    "concentrated in game horizons "
                    "where Matchbook had not yet "
                    "become available through the "
                    "provider. Paired observations "
                    "were fresh whenever present."
                ),
        },

        "remediation": {
            "type":
                "operational_universe",

            "time_to_game": {
                "lower_bound_hours":
                    0,

                "lower_bound_inclusive":
                    False,

                "upper_bound_hours":
                    23,

                "upper_bound_inclusive":
                    True,

                "reason":
                    (
                        "Observed Matchbook publication "
                        "transition occurred around "
                        "23.92 hours. A 23-hour upper "
                        "bound provides approximately "
                        "55 minutes of safety margin "
                        "from that observed boundary."
                    ),
            },

            "collector_behavior":
                (
                    "Collector continues storing all "
                    "strictly pregame observations. "
                    "The operational horizon is applied "
                    "only by the M1b validator."
                ),
        },

        "sharp_data_contract": {
            "provider":
                "The Odds API",

            "sport":
                "baseball_mlb",

            "market":
                "h2h",

            "bookmakers": [
                "betfair_ex_uk",
                "matchbook",
                "smarkets",
            ],

            "required_components": [
                "h2h",
                "h2h_lay",
            ],

            "required_fresh_families":
                2,

            "max_source_age_seconds":
                25,

            "stale_carry_forward":
                False,

            "family_probability_formula":
                (
                    "(1/back_decimal "
                    "+ 1/lay_decimal) / 2"
                ),

            "consensus":
                "median",

            "pregame_only":
                True,
        },

        "fresh_validation": {
            "polls":
                60,

            "poll_interval_seconds":
                30,

            "required_http_successes":
                60,

            "minimum_operational_game_snapshots":
                100,

            "minimum_distinct_operational_games":
                3,

            "minimum_eligibility_rate":
                "0.80",

            "eligibility_definition":
                (
                    "A game-poll snapshot is eligible "
                    "iff both outcome consensus rows "
                    "are eligible, each based on at "
                    "least two complete fresh exchange "
                    "families."
                ),

            "integrity_requirements": [
                "zero in-play quote rows",
                "zero incomplete paired rows",
                "zero bad source-age rows",
                "zero bad midpoint rows",
                "zero bad freshness rows",
                "zero bad consensus eligibility rows",
                (
                    "zero asymmetric outcome "
                    "eligibility within a game-poll"
                ),
            ],
        },

        "decision_rule": {
            "pass_if_all": [
                "60 successful polls",
                (
                    "at least 100 operational "
                    "game-poll snapshots"
                ),
                (
                    "at least 3 distinct operational "
                    "games"
                ),
                (
                    "operational eligibility rate "
                    ">= 0.80"
                ),
                "all integrity requirements pass",
            ],

            "failure_action":
                (
                    "Do not proceed to calibration. "
                    "Investigate replacement/additional "
                    "sharp source without modifying "
                    "this M1b result."
                ),
        },

        "forbidden_in_m1b": [
            "signal construction",
            "edge thresholds",
            "trade simulation",
            "PnL calculation",
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
        "H2-v2 M1b CONTRACT VALIDATION PASSED"
    )

    print(
        "parent M1 SHA:",
        actual_parent_sha,
    )

    print(
        "operational horizon:",
        "0 < time_to_game <= 23h",
    )

    print(
        "freshness:",
        "<=25s",
    )

    print(
        "coverage gate:",
        ">=80%",
    )

    print(
        "minimum evidence:",
        ">=100 snapshots / >=3 games",
    )

    print(
        "NO API CALLS"
    )


if __name__ == "__main__":
    main()
