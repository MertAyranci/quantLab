from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO = Path(__file__).resolve().parents[1]
RESEARCH = REPO / "research"

M1A = RESEARCH / "h5_m1a_acquisition_contract.json"

R2_PREREG = (
    RESEARCH
    / "h5_m1r2_rerun_amendment.json"
)

STAGE2_EXECUTION = (
    RESEARCH
    / "h5_m1r2_stage2_execution_addendum.json"
)

STAGE1_REGISTRY = (
    REPO
    / "data/research/h5_m1r2/registry.jsonl"
)

STAGE1_RECEIPT = (
    RESEARCH
    / "h5_m1r2_registry_freeze.json"
)

STAGE2_ATTEMPT = (
    REPO
    / "data/research/h5_m1r2/"
    / "stage2_authoritative_attempt.json"
)

STAGE2_MAP = (
    REPO
    / "data/research/h5_m1r2/external_id_map.jsonl"
)

STAGE2_RECEIPT = (
    RESEARCH
    / "h5_m1r2_external_id_map_freeze.json"
)

H2_IDENTITIES = (
    RESEARCH
    / "h6_m2_h2_identity_exclusions.json"
)


EXPECTED_M1A_SHA = (
    "6da26ac94413b5a79566764dcc43d4584"
    "bff94c3b88531225dec09a4db99b1be"
)

EXPECTED_R2_PREREG_SHA = (
    "b805cc2fc9a01e1cd4b4d2d5c7966fde"
    "d4e19840bcbefcb72f102d50a0c380ed"
)

EXPECTED_STAGE2_EXECUTION_SHA = (
    "f49da0a5d9083c63740f1939ca88f5ed"
    "7e6e77a668e7f01aa325da0723a190b5"
)

EXPECTED_STAGE1_REGISTRY_SHA = (
    "1084436712172bc00126ba04cb4b7eb0"
    "840f28730ca75cc209a528bc88c6a078"
)

EXPECTED_STAGE1_RECEIPT_SHA = (
    "848970dd4cb40cdb499c6c0c29d00ffe"
    "eb61acdc824d1fa528d16fdfef7a9137"
)

EXPECTED_STAGE2_ATTEMPT_SHA = (
    "5bed76b90b7f502e7c344994d6d75340"
    "22e85d1be9eaa0af6318196d311a77e8"
)

EXPECTED_STAGE2_MAP_SHA = (
    "d00f1195e6261cf062b340645103cd24"
    "afebe4807460a590b8ab72b6e50f3a4b"
)

EXPECTED_STAGE2_RECEIPT_SHA = (
    "5f857a14c0c671a3c0f3d495fc542ad"
    "43fb9fa414b8ec24d1a7eae560ea19415"
)

EXPECTED_STAGE2_RESOLVER_SHA = (
    "ef80268b3206a1389fa4eccd5a4dd8d6"
    "9a91ea8f08cbcc54b57367d160bbff75"
)

EXPECTED_H2_IDENTITY_SHA = (
    "0907e1bd4ccc2aaaf7cef332e7fe96b8"
    "fb05355afdfc2e912d412e2f10c78d73"
)

EXPECTED_RUNTIME_INDEXES = [
    2, 4, 5, 6, 7, 8, 10
]

BOOKMAKERS = [
    "betfair_ex_uk",
    "matchbook",
    "smarkets",
]

MAX_SOURCE_AGE_SECONDS = 25
MIN_FRESH_FAMILIES = 2
POLL_INTERVAL_SECONDS = 10
MAX_POLLS = 60
MAX_RUN_SECONDS = 600
MAX_START_DELTA_SECONDS = 600


def sha256_file(
    path: Path,
) -> str:
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def verify_sha(
    path: Path,
    expected: str,
):
    if not path.is_file():
        raise RuntimeError(
            f"missing frozen input: {path}"
        )

    actual = sha256_file(
        path
    )

    if actual != expected:
        raise RuntimeError(
            f"SHA mismatch: {path}\n"
            f"expected={expected}\n"
            f"actual={actual}"
        )


def read_json(
    path: Path,
) -> dict[str, Any]:
    data = json.loads(
        path.read_text(
            encoding="utf-8"
        )
    )

    if not isinstance(
        data,
        dict,
    ):
        raise RuntimeError(
            f"expected JSON object: {path}"
        )

    return data


def read_jsonl(
    path: Path,
) -> list[dict[str, Any]]:
    rows = []

    for lineno, line in enumerate(
        path.read_text(
            encoding="utf-8"
        ).splitlines(),
        start=1,
    ):
        if not line.strip():
            continue

        try:
            row = json.loads(
                line
            )
        except json.JSONDecodeError:
            raise RuntimeError(
                f"invalid JSONL {path}:{lineno}"
            ) from None

        if not isinstance(
            row,
            dict,
        ):
            raise RuntimeError(
                f"non-object JSONL row "
                f"{path}:{lineno}"
            )

        rows.append(
            row
        )

    return rows


def parse_dt(
    value: Any,
) -> datetime | None:
    if not value:
        return None

    try:
        dt = datetime.fromisoformat(
            str(value).replace(
                "Z",
                "+00:00",
            )
        )
    except (
        TypeError,
        ValueError,
    ):
        return None

    if dt.tzinfo is None:
        return None

    return dt.astimezone(
        timezone.utc
    )


def norm_team(
    value: Any,
) -> str:
    text = str(
        value or ""
    ).strip().lower()

    text = (
        text.replace(".", "")
        .replace("-", " ")
    )

    text = re.sub(
        r"\s+",
        " ",
        text,
    ).strip()

    aliases = {
        "oakland athletics":
            "athletics",

        "athletics":
            "athletics",
    }

    return aliases.get(
        text,
        text,
    )


def validate_acquisition_contract():
    verify_sha(
        M1A,
        EXPECTED_M1A_SHA,
    )

    contract = read_json(
        M1A
    )

    source = contract[
        "external_source"
    ]

    polling = contract[
        "diagnostic_external_polling"
    ]

    capture = contract[
        "capture_window"
    ]

    if (
        source[
            "exchange_families"
        ]
        != BOOKMAKERS
    ):
        raise RuntimeError(
            "exchange-family mismatch"
        )

    if (
        source[
            "maximum_source_age_seconds"
        ]
        != MAX_SOURCE_AGE_SECONDS
    ):
        raise RuntimeError(
            "source-age mismatch"
        )

    if (
        source[
            "minimum_fresh_complete_families"
        ]
        != MIN_FRESH_FAMILIES
    ):
        raise RuntimeError(
            "fresh-family gate mismatch"
        )

    if (
        source["consensus"]
        !=
        "median of all valid fresh family probabilities"
    ):
        raise RuntimeError(
            "consensus semantics mismatch"
        )

    if (
        polling[
            "interval_seconds"
        ]
        != POLL_INTERVAL_SECONDS
        or
        polling[
            "maximum_polls_initial_run"
        ]
        != MAX_POLLS
        or
        polling[
            "maximum_initial_run_seconds"
        ]
        != MAX_RUN_SECONDS
    ):
        raise RuntimeError(
            "polling semantics mismatch"
        )

    if (
        capture[
            "pregame_only"
        ]
        is not True
        or
        capture[
            "hard_stop_per_market"
        ]
        != "canonical commence_time"
    ):
        raise RuntimeError(
            "pregame capture boundary mismatch"
        )

    prohibited = contract[
        "m1b_prohibited_metrics"
    ]

    required_prohibited = {
        "external consensus change",
        "innovation magnitude",
        "innovation sign",
        "PM price change after an external observation",
        "same-direction response",
        "hit rate",
        "correlation",
        "regression",
        "markout",
        "edge",
        "PnL",
    }

    if not required_prohibited.issubset(
        set(prohibited)
    ):
        raise RuntimeError(
            "analysis boundary mismatch"
        )

    return contract


def validate_r2_contracts():
    verify_sha(
        R2_PREREG,
        EXPECTED_R2_PREREG_SHA,
    )

    verify_sha(
        STAGE2_EXECUTION,
        EXPECTED_STAGE2_EXECUTION_SHA,
    )

    prereg = read_json(
        R2_PREREG
    )

    if (
        prereg.get("milestone")
        != "H5-M1R2"
        or
        prereg.get("status")
        !=
        "PROSPECTIVE_ENGINEERING_RERUN_PREREGISTRATION"
    ):
        raise RuntimeError(
            "R2 prereg mismatch"
        )

    if (
        prereg[
            "scientific_hypothesis"
        ][
            "m1_role"
        ]
        != "ENGINEERING_FEASIBILITY_ONLY"
    ):
        raise RuntimeError(
            "R2 M1 role mismatch"
        )

    acquisition = prereg[
        "unchanged_acquisition_semantics"
    ]

    if (
        acquisition[
            "exchanges"
        ]
        != BOOKMAKERS
        or
        acquisition[
            "external_family_freshness_seconds_max"
        ]
        != MAX_SOURCE_AGE_SECONDS
        or
        acquisition[
            "minimum_fresh_complete_exchange_families"
        ]
        != MIN_FRESH_FAMILIES
        or
        acquisition[
            "poll_interval_seconds"
        ]
        != POLL_INTERVAL_SECONDS
        or
        acquisition[
            "maximum_poll_count"
        ]
        != MAX_POLLS
        or
        acquisition[
            "maximum_acquisition_duration_seconds"
        ]
        != MAX_RUN_SECONDS
    ):
        raise RuntimeError(
            "R2 acquisition semantics mismatch"
        )

    if any(
        value is not False
        for value
        in prereg[
            "analysis_boundary"
        ].values()
    ):
        raise RuntimeError(
            "R2 analysis boundary violation"
        )


def validate_stage1(
    rows: list[dict[str, Any]],
    receipt: dict[str, Any],
):
    if len(rows) != 10:
        raise RuntimeError(
            "Stage1 must contain 10 rows"
        )

    conditions = set()
    gamma_ids = set()
    tokens = set()

    for expected_index, row in enumerate(
        rows,
        start=1,
    ):
        if (
            row.get("study")
            != "H5"
            or
            row.get("milestone")
            != "H5-M1R2"
            or
            row.get(
                "registration_stage"
            )
            != "STAGE1_CONDITION_FREEZE"
        ):
            raise RuntimeError(
                "Stage1 identity mismatch"
            )

        if (
            row.get(
                "registration_index"
            )
            != expected_index
        ):
            raise RuntimeError(
                "Stage1 index mismatch"
            )

        if (
            row.get(
                "external_identity_status"
            )
            != "PENDING_PROVIDER_VISIBILITY"
            or
            row.get(
                "odds_game_id"
            )
            is not None
        ):
            raise RuntimeError(
                "Stage1 external identity "
                "unexpectedly populated"
            )

        condition = str(
            row.get(
                "condition_id"
            )
            or ""
        ).lower()

        gamma_id = str(
            row.get(
                "gamma_market_id"
            )
            or ""
        )

        p1 = str(
            row.get(
                "p1_token_id"
            )
            or ""
        )

        p0 = str(
            row.get(
                "p0_token_id"
            )
            or ""
        )

        if (
            not condition
            or
            condition in conditions
            or
            not gamma_id
            or
            gamma_id in gamma_ids
            or
            not p1
            or
            not p0
            or
            p1 == p0
            or
            p1 in tokens
            or
            p0 in tokens
        ):
            raise RuntimeError(
                "Stage1 duplicate/empty identity"
            )

        if (
            parse_dt(
                row.get(
                    "pm_game_start_time"
                )
            )
            is None
        ):
            raise RuntimeError(
                "Stage1 invalid start time"
            )

        conditions.add(
            condition
        )

        gamma_ids.add(
            gamma_id
        )

        tokens.update(
            (p1, p0)
        )

    if (
        receipt.get("status")
        !=
        "STAGE1_CONDITION_REGISTRY_FROZEN"
        or
        receipt.get("milestone")
        != "H5-M1R2"
        or
        receipt.get(
            "registry_sha256"
        )
        != EXPECTED_STAGE1_REGISTRY_SHA
        or
        receipt.get(
            "row_count"
        )
        != 10
    ):
        raise RuntimeError(
            "Stage1 receipt mismatch"
        )

    expected_conditions = [
        row[
            "condition_id"
        ].lower()
        for row in rows
    ]

    if (
        receipt.get(
            "condition_ids"
        )
        != expected_conditions
    ):
        raise RuntimeError(
            "Stage1 receipt condition order mismatch"
        )

    if any(
        value is not False
        for value
        in receipt[
            "analysis_boundary"
        ].values()
    ):
        raise RuntimeError(
            "Stage1 analysis boundary violation"
        )


def validate_stage2(
    rows: list[dict[str, Any]],
    receipt: dict[str, Any],
    attempt: dict[str, Any],
):
    if len(rows) != 7:
        raise RuntimeError(
            "Stage2 map must contain 7 rows"
        )

    indexes = [
        int(
            row[
                "registration_index"
            ]
        )
        for row in rows
    ]

    if (
        indexes
        != EXPECTED_RUNTIME_INDEXES
    ):
        raise RuntimeError(
            "unexpected Stage2 indexes"
        )

    conditions = set()
    odds_ids = set()

    for row in rows:
        if (
            row.get("study")
            != "H5"
            or
            row.get("milestone")
            != "H5-M1R2"
            or
            row.get(
                "registration_stage"
            )
            != "STAGE2_EXTERNAL_IDENTITY"
            or
            row.get(
                "resolution_status"
            )
            != "RESOLVED_NONBURNED"
        ):
            raise RuntimeError(
                "Stage2 row status mismatch"
            )

        if (
            row.get(
                "resolver_sha256"
            )
            != EXPECTED_STAGE2_RESOLVER_SHA
        ):
            raise RuntimeError(
                "Stage2 resolver binding mismatch"
            )

        condition = str(
            row[
                "condition_id"
            ]
        ).lower()

        odds_id = str(
            row[
                "odds_game_id"
            ]
        )

        if (
            condition in conditions
            or
            odds_id in odds_ids
        ):
            raise RuntimeError(
                "duplicate Stage2 identity"
            )

        canonical = parse_dt(
            row[
                "canonical_commence_time"
            ]
        )

        pm_start = parse_dt(
            row[
                "pm_game_start_time"
            ]
        )

        if (
            canonical is None
            or
            pm_start is None
        ):
            raise RuntimeError(
                "invalid Stage2 time"
            )

        delta = (
            canonical
            - pm_start
        ).total_seconds()

        if (
            abs(delta)
            > MAX_START_DELTA_SECONDS
        ):
            raise RuntimeError(
                "Stage2 start delta exceeds 600s"
            )

        if (
            abs(
                delta
                - float(
                    row[
                        "start_delta_seconds"
                    ]
                )
            )
            > 1e-9
        ):
            raise RuntimeError(
                "Stage2 recorded delta mismatch"
            )

        conditions.add(
            condition
        )

        odds_ids.add(
            odds_id
        )

    if (
        receipt.get("status")
        !=
        "STAGE2_EXTERNAL_ID_MAP_FROZEN"
        or
        receipt.get("milestone")
        != "H5-M1R2"
        or
        receipt.get(
            "map_sha256"
        )
        != EXPECTED_STAGE2_MAP_SHA
        or
        receipt.get(
            "map_row_count"
        )
        != 7
        or
        receipt.get(
            "retry_permitted"
        )
        is not False
    ):
        raise RuntimeError(
            "Stage2 receipt mismatch"
        )

    gate = receipt[
        "gate"
    ]

    if (
        gate.get("passed")
        is not True
        or
        gate.get(
            "minimum_resolved_nonburned"
        )
        != 5
        or
        gate.get(
            "maximum_ambiguous"
        )
        != 0
        or
        gate.get(
            "counts"
        )
        != {
            "AMBIGUOUS_FAIL_CLOSED": 0,
            "PENDING_PROVIDER_VISIBILITY": 3,
            "RESOLVED_H2_BURNED_PROHIBITED": 0,
            "RESOLVED_NONBURNED": 7,
        }
    ):
        raise RuntimeError(
            "Stage2 gate mismatch"
        )

    if (
        receipt.get(
            "attempt_marker_sha256"
        )
        != EXPECTED_STAGE2_ATTEMPT_SHA
    ):
        raise RuntimeError(
            "Stage2 attempt binding mismatch"
        )

    if any(
        value is not False
        for value
        in receipt[
            "analysis_boundary"
        ].values()
    ):
        raise RuntimeError(
            "Stage2 analysis boundary violation"
        )

    if (
        attempt.get("status")
        !=
        "STAGE2_AUTHORITATIVE_ATTEMPT_STARTED"
        or
        attempt.get(
            "attempt_number"
        )
        != 1
        or
        attempt.get(
            "provider_identity_only"
        )
        is not True
        or
        attempt.get(
            "odds_read"
        )
        is not False
        or
        attempt.get(
            "resolver_sha256"
        )
        != EXPECTED_STAGE2_RESOLVER_SHA
    ):
        raise RuntimeError(
            "Stage2 attempt marker mismatch"
        )


def join_stage1_stage2(
    stage1_rows: list[dict[str, Any]],
    stage2_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    stage1_by_index = {
        int(
            row[
                "registration_index"
            ]
        ):
            row
        for row in stage1_rows
    }

    runtime = []

    for stage2 in stage2_rows:
        index = int(
            stage2[
                "registration_index"
            ]
        )

        stage1 = stage1_by_index.get(
            index
        )

        if stage1 is None:
            raise RuntimeError(
                "Stage2 row missing Stage1 parent"
            )

        for key in (
            "condition_id",
            "gamma_market_id",
            "gamma_slug",
            "p1_team",
            "p0_team",
            "pm_game_start_time",
        ):
            left = str(
                stage1[
                    key
                ]
            ).strip()

            right = str(
                stage2[
                    key
                ]
            ).strip()

            if key == "condition_id":
                left = left.lower()
                right = right.lower()

            if left != right:
                raise RuntimeError(
                    "Stage1/Stage2 join mismatch: "
                    f"{key}"
                )

        merged = dict(
            stage1
        )

        merged.update({
            "odds_game_id":
                str(
                    stage2[
                        "odds_game_id"
                    ]
                ),

            "canonical_commence_time":
                stage2[
                    "canonical_commence_time"
                ],

            "home_team":
                stage2[
                    "provider_home_team"
                ],

            "away_team":
                stage2[
                    "provider_away_team"
                ],

            "external_identity_status":
                "RESOLVED_NONBURNED",

            "stage2_resolution_status":
                "RESOLVED_NONBURNED",

            "stage2_start_delta_seconds":
                stage2[
                    "start_delta_seconds"
                ],
        })

        runtime.append(
            merged
        )

    validate_runtime_rows(
        runtime
    )

    return runtime


def validate_runtime_rows(
    rows: list[dict[str, Any]],
):
    if len(rows) != 7:
        raise RuntimeError(
            "runtime must contain 7 markets"
        )

    indexes = [
        int(
            row[
                "registration_index"
            ]
        )
        for row in rows
    ]

    if (
        indexes
        != EXPECTED_RUNTIME_INDEXES
    ):
        raise RuntimeError(
            "runtime indexes changed/renumbered"
        )

    condition_ids = set()
    odds_ids = set()
    token_ids = set()

    for row in rows:
        condition = str(
            row[
                "condition_id"
            ]
        ).lower()

        odds_id = str(
            row[
                "odds_game_id"
            ]
        )

        p1 = str(
            row[
                "p1_token_id"
            ]
        )

        p0 = str(
            row[
                "p0_token_id"
            ]
        )

        if (
            condition in condition_ids
            or
            odds_id in odds_ids
            or
            p1 == p0
            or
            p1 in token_ids
            or
            p0 in token_ids
        ):
            raise RuntimeError(
                "runtime duplicate identity"
            )

        canonical = parse_dt(
            row[
                "canonical_commence_time"
            ]
        )

        pm_start = parse_dt(
            row[
                "pm_game_start_time"
            ]
        )

        if (
            canonical is None
            or
            pm_start is None
            or
            abs(
                (
                    canonical
                    - pm_start
                ).total_seconds()
            )
            > MAX_START_DELTA_SECONDS
        ):
            raise RuntimeError(
                "runtime time mismatch"
            )

        provider_set = {
            norm_team(
                row[
                    "home_team"
                ]
            ),
            norm_team(
                row[
                    "away_team"
                ]
            ),
        }

        pm_set = {
            norm_team(
                row[
                    "p1_team"
                ]
            ),
            norm_team(
                row[
                    "p0_team"
                ]
            ),
        }

        if provider_set != pm_set:
            raise RuntimeError(
                "runtime team-set mismatch"
            )

        condition_ids.add(
            condition
        )

        odds_ids.add(
            odds_id
        )

        token_ids.update(
            (p1, p0)
        )


def load_runtime_bundle():
    validate_acquisition_contract()
    validate_r2_contracts()

    identities = {
        STAGE1_REGISTRY:
            EXPECTED_STAGE1_REGISTRY_SHA,

        STAGE1_RECEIPT:
            EXPECTED_STAGE1_RECEIPT_SHA,

        STAGE2_ATTEMPT:
            EXPECTED_STAGE2_ATTEMPT_SHA,

        STAGE2_MAP:
            EXPECTED_STAGE2_MAP_SHA,

        STAGE2_RECEIPT:
            EXPECTED_STAGE2_RECEIPT_SHA,

        H2_IDENTITIES:
            EXPECTED_H2_IDENTITY_SHA,
    }

    for path, expected in identities.items():
        verify_sha(
            path,
            expected,
        )

    stage1_rows = read_jsonl(
        STAGE1_REGISTRY
    )

    stage1_receipt = read_json(
        STAGE1_RECEIPT
    )

    validate_stage1(
        stage1_rows,
        stage1_receipt,
    )

    stage2_rows = read_jsonl(
        STAGE2_MAP
    )

    stage2_receipt = read_json(
        STAGE2_RECEIPT
    )

    attempt = read_json(
        STAGE2_ATTEMPT
    )

    validate_stage2(
        stage2_rows,
        stage2_receipt,
        attempt,
    )

    runtime = join_stage1_stage2(
        stage1_rows,
        stage2_rows,
    )

    return {
        "rows":
            runtime,

        "stage1_rows":
            stage1_rows,

        "stage2_rows":
            stage2_rows,

        "stage1_registered_market_count":
            10,

        "acquisition_market_count":
            7,

        "runtime_registration_indexes":
            EXPECTED_RUNTIME_INDEXES,

        "stage1_registry_sha256":
            EXPECTED_STAGE1_REGISTRY_SHA,

        "stage1_receipt_sha256":
            EXPECTED_STAGE1_RECEIPT_SHA,

        "stage2_attempt_sha256":
            EXPECTED_STAGE2_ATTEMPT_SHA,

        "stage2_map_sha256":
            EXPECTED_STAGE2_MAP_SHA,

        "stage2_receipt_sha256":
            EXPECTED_STAGE2_RECEIPT_SHA,

        "r2_preregistration_sha256":
            EXPECTED_R2_PREREG_SHA,

        "stage2_execution_sha256":
            EXPECTED_STAGE2_EXECUTION_SHA,

        "h2_identity_sha256":
            EXPECTED_H2_IDENTITY_SHA,

        "m1a_sha256":
            EXPECTED_M1A_SHA,
    }


def validate_output():
    bundle = load_runtime_bundle()

    print(
        "H5-M1R2 RUNTIME BUNDLE: PASS"
    )

    print(
        "Stage1 registered markets:",
        bundle[
            "stage1_registered_market_count"
        ],
    )

    print(
        "Stage2 acquisition markets:",
        bundle[
            "acquisition_market_count"
        ],
    )

    print(
        "runtime registration indexes:",
        ",".join(
            str(x)
            for x in bundle[
                "runtime_registration_indexes"
            ]
        ),
    )

    print(
        "Stage1 registry SHA:",
        bundle[
            "stage1_registry_sha256"
        ],
    )

    print(
        "Stage2 map SHA:",
        bundle[
            "stage2_map_sha256"
        ],
    )

    print(
        "M1A contract SHA:",
        bundle[
            "m1a_sha256"
        ],
    )

    print(
        "network calls made: NO"
    )

    print(
        "external odds read: NO"
    )

    print(
        "Polymarket prices read: NO"
    )

    print(
        "PM response calculated: NO"
    )

    print(
        "PnL calculated: NO"
    )


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--validate",
        action="store_true",
    )

    args = parser.parse_args()

    if not args.validate:
        parser.error(
            "--validate required"
        )

    validate_output()


if __name__ == "__main__":
    main()
