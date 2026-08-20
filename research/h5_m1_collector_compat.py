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

VISIBILITY_AMENDMENT = (
    RESEARCH
    / "h5_m1_registration_visibility_amendment.json"
)

COMPATIBILITY_CONTRACT = (
    RESEARCH
    / "h5_m1_collector_compatibility_contract.json"
)

STAGE1_REGISTRAR = (
    RESEARCH
    / "h5_m1_stage1_register.py"
)

STAGE2_RESOLVER = (
    RESEARCH
    / "h5_m1_stage2_resolve.py"
)

STAGE1_REGISTRY = (
    REPO
    / "data"
    / "research"
    / "h5_m1"
    / "registry.jsonl"
)

STAGE1_RECEIPT = (
    RESEARCH
    / "h5_m1_registry_freeze.json"
)

STAGE2_MAP = (
    REPO
    / "data"
    / "research"
    / "h5_m1"
    / "external_id_map.jsonl"
)

STAGE2_RECEIPT = (
    RESEARCH
    / "h5_m1_external_id_map_freeze.json"
)


EXPECTED_M1A_SHA = (
    "6da26ac94413b5a79566764dcc43d4584"
    "bff94c3b88531225dec09a4db99b1be"
)

EXPECTED_VISIBILITY_AMENDMENT_SHA = (
    "5cd23a68e1fea185acff187bd9a9556f"
    "c0e86c53d9a5b649a01ca0f74ce27dda"
)

EXPECTED_COMPATIBILITY_CONTRACT_SHA = (
    "f7e69a2f4fc0734e62fea7abd5615b99"
    "25c86cf23438698ea014fa1a164eaecf"
)

EXPECTED_STAGE1_REGISTRAR_SHA = (
    "c8453b9394bd476677e9eaa986a56c0"
    "d2f57164c581efe510bfa95f2bb735b4f"
)

EXPECTED_STAGE2_RESOLVER_SHA = (
    "e2ff82690ce0428c9e7785a8b1fa9209"
    "2da30b08c93c8f7cecde2ef3b54d7ac1"
)

EXPECTED_STAGE1_REGISTRY_SHA = (
    "c73e65dea1ba76eea40f1d724aa7d963"
    "81c06db4cb77e411df2213ce8f7cf95b"
)

EXPECTED_STAGE1_RECEIPT_SHA = (
    "61c96d789b4c3cc39984889be9209cb9"
    "90022ee20f9e3c985551dd6cf52874c2"
)

EXPECTED_EXCLUSIONS_SHA = (
    "fb4a268f3148e746e31232c658cdc879"
    "3c2e082a73c5a8260af6779d89b1949a"
)

MIN_RUNTIME_MARKETS = 5
MAX_RUNTIME_MARKETS = 10
MAX_START_DELTA_SECONDS = 600

ALLOWED_STAGE2_STATUSES = {
    "RESOLVED_NONBURNED",
    "RESOLVED_H2_BURNED_PROHIBITED",
}


def sha256_file(path: Path) -> str:
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def parse_dt(value: Any) -> datetime | None:
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


def norm_team(value: Any) -> str:
    text = str(
        value or ""
    ).strip().lower()

    text = text.replace(
        ".",
        "",
    )

    text = text.replace(
        "-",
        " ",
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

    with path.open(
        "r",
        encoding="utf-8",
    ) as fh:
        for lineno, line in enumerate(
            fh,
            start=1,
        ):
            if not line.strip():
                continue

            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise RuntimeError(
                    f"invalid JSONL "
                    f"{path}:{lineno}: "
                    f"{exc.msg}"
                ) from None

            if not isinstance(
                row,
                dict,
            ):
                raise RuntimeError(
                    "non-object JSONL row: "
                    f"{path}:{lineno}"
                )

            rows.append(row)

    return rows


def verify_sha(
    path: Path,
    expected: str,
):
    if not path.is_file():
        raise RuntimeError(
            f"missing frozen input: {path}"
        )

    actual = sha256_file(path)

    if actual != expected:
        raise RuntimeError(
            "frozen input SHA mismatch: "
            f"{path}\n"
            f"expected={expected}\n"
            f"actual={actual}"
        )


def validate_compatibility_contract(
    contract: dict[str, Any],
):
    if (
        contract.get("status")
        !=
        "FROZEN_BEFORE_H5_M1_ACQUISITION"
    ):
        raise RuntimeError(
            "compatibility contract status "
            "mismatch"
        )

    if (
        contract.get("verdict")
        !=
        "ENGINEERING COMPATIBILITY ONLY; "
        "H5 SCIENTIFIC SEMANTICS UNCHANGED"
    ):
        raise RuntimeError(
            "compatibility contract verdict "
            "mismatch"
        )

    binding = contract[
        "stage2_runtime_binding"
    ]

    if (
        binding[
            "minimum_resolved_nonburned_markets"
        ]
        != MIN_RUNTIME_MARKETS
    ):
        raise RuntimeError(
            "minimum runtime-market gate "
            "mismatch"
        )

    if (
        binding[
            "ambiguous_count_required"
        ]
        != 0
    ):
        raise RuntimeError(
            "ambiguity gate mismatch"
        )

    join = contract[
        "join_contract"
    ]

    if (
        join[
            "exact_join_keys"
        ]
        != [
            "registration_index",
            "condition_id",
            "gamma_market_id",
        ]
    ):
        raise RuntimeError(
            "join keys mismatch"
        )

    if (
        join[
            "additional_equality_checks"
        ]
        != [
            "gamma_slug",
            "p1_team",
            "p0_team",
            "pm_game_start_time",
        ]
    ):
        raise RuntimeError(
            "join equality checks mismatch"
        )

    if (
        join[
            "registration_indexes_are_never_renumbered"
        ]
        is not True
    ):
        raise RuntimeError(
            "registration-index policy mismatch"
        )

    if (
        join[
            "stage1_is_never_modified"
        ]
        is not True
    ):
        raise RuntimeError(
            "Stage-1 mutability policy mismatch"
        )

    if (
        join[
            "eligible_stage2_status"
        ]
        != "RESOLVED_NONBURNED"
    ):
        raise RuntimeError(
            "eligible Stage-2 status mismatch"
        )

    acquisition = contract[
        "acquisition_semantics"
    ]

    if (
        acquisition[
            "exchange_families"
        ]
        != [
            "betfair_ex_uk",
            "matchbook",
            "smarkets",
        ]
    ):
        raise RuntimeError(
            "exchange-family freeze mismatch"
        )

    if (
        acquisition[
            "maximum_source_age_seconds"
        ]
        != 25
    ):
        raise RuntimeError(
            "source-age freeze mismatch"
        )

    if (
        acquisition[
            "minimum_fresh_complete_families"
        ]
        != 2
    ):
        raise RuntimeError(
            "fresh-family freeze mismatch"
        )

    if (
        acquisition[
            "poll_interval_seconds"
        ]
        != 10
    ):
        raise RuntimeError(
            "poll interval mismatch"
        )

    if (
        acquisition[
            "maximum_polls"
        ]
        != 60
    ):
        raise RuntimeError(
            "maximum polls mismatch"
        )

    if (
        acquisition[
            "maximum_initial_run_seconds"
        ]
        != 600
    ):
        raise RuntimeError(
            "initial-run duration mismatch"
        )

    if (
        acquisition[
            "maximum_start_delta_seconds"
        ]
        != MAX_START_DELTA_SECONDS
    ):
        raise RuntimeError(
            "start-delta freeze mismatch"
        )

    boundary = contract[
        "analysis_boundary"
    ]

    if not boundary:
        raise RuntimeError(
            "missing compatibility "
            "analysis boundary"
        )

    if any(
        value is not False
        for value in boundary.values()
    ):
        raise RuntimeError(
            "compatibility analysis boundary "
            "violation"
        )


def validate_stage1_rows(
    rows: list[dict[str, Any]],
):
    if len(rows) != 10:
        raise RuntimeError(
            "Stage-1 registry must contain "
            "exactly 10 rows"
        )

    conditions: set[str] = set()
    gamma_ids: set[str] = set()
    tokens: set[str] = set()

    required = {
        "study",
        "milestone",
        "registration_stage",
        "registration_index",
        "condition_id",
        "gamma_market_id",
        "gamma_slug",
        "pm_game_start_time",
        "p1_team",
        "p0_team",
        "p1_token_id",
        "p0_token_id",
        "external_identity_status",
        "odds_game_id",
    }

    for expected_index, row in enumerate(
        rows,
        start=1,
    ):
        missing = (
            required
            - set(row)
        )

        if missing:
            raise RuntimeError(
                "Stage-1 row missing fields: "
                f"{sorted(missing)}"
            )

        if (
            row["study"] != "H5"
            or
            row["milestone"] != "H5-M1"
        ):
            raise RuntimeError(
                "Stage-1 study/milestone "
                "mismatch"
            )

        if (
            row["registration_stage"]
            !=
            "STAGE1_CONDITION_FREEZE"
        ):
            raise RuntimeError(
                "Stage-1 registration-stage "
                "mismatch"
            )

        if (
            row["registration_index"]
            != expected_index
        ):
            raise RuntimeError(
                "Stage-1 order/index mismatch"
            )

        if row["odds_game_id"] is not None:
            raise RuntimeError(
                "Stage-1 unexpectedly contains "
                "external event ID"
            )

        if (
            row["external_identity_status"]
            !=
            "PENDING_PROVIDER_VISIBILITY"
        ):
            raise RuntimeError(
                "unexpected Stage-1 external "
                "identity state"
            )

        if (
            parse_dt(
                row["pm_game_start_time"]
            )
            is None
        ):
            raise RuntimeError(
                "invalid Stage-1 start time"
            )

        condition = str(
            row["condition_id"]
        ).strip().lower()

        gamma_id = str(
            row["gamma_market_id"]
        ).strip()

        p1 = str(
            row["p1_token_id"]
        )

        p0 = str(
            row["p0_token_id"]
        )

        if (
            not condition
            or
            condition in conditions
        ):
            raise RuntimeError(
                "duplicate/empty Stage-1 "
                "condition ID"
            )

        if (
            not gamma_id
            or
            gamma_id in gamma_ids
        ):
            raise RuntimeError(
                "duplicate/empty Stage-1 "
                "Gamma market ID"
            )

        if (
            p1 == p0
            or
            p1 in tokens
            or
            p0 in tokens
        ):
            raise RuntimeError(
                "duplicate Stage-1 token "
                "identity"
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


def validate_stage1_receipt(
    receipt: dict[str, Any],
    rows: list[dict[str, Any]],
):
    if (
        receipt.get("status")
        !=
        "STAGE1_CONDITION_REGISTRY_FROZEN"
    ):
        raise RuntimeError(
            "Stage-1 receipt status mismatch"
        )

    if (
        receipt.get("registry_mutable")
        is not False
    ):
        raise RuntimeError(
            "Stage-1 registry not immutable"
        )

    if (
        receipt.get("row_count")
        != 10
    ):
        raise RuntimeError(
            "Stage-1 receipt row count mismatch"
        )

    if (
        receipt.get("m1a_sha256")
        != EXPECTED_M1A_SHA
    ):
        raise RuntimeError(
            "Stage-1 receipt M1A SHA mismatch"
        )

    if (
        receipt.get("amendment_sha256")
        != EXPECTED_VISIBILITY_AMENDMENT_SHA
    ):
        raise RuntimeError(
            "Stage-1 receipt amendment SHA "
            "mismatch"
        )

    if (
        receipt.get("registrar_sha256")
        != EXPECTED_STAGE1_REGISTRAR_SHA
    ):
        raise RuntimeError(
            "Stage-1 receipt registrar SHA "
            "mismatch"
        )

    if (
        receipt.get("registry_sha256")
        != EXPECTED_STAGE1_REGISTRY_SHA
    ):
        raise RuntimeError(
            "Stage-1 receipt registry SHA "
            "mismatch"
        )

    if (
        receipt.get("exclusions_sha256")
        != EXPECTED_EXCLUSIONS_SHA
    ):
        raise RuntimeError(
            "Stage-1 exclusions SHA mismatch"
        )

    expected_conditions = [
        str(
            row["condition_id"]
        ).strip().lower()
        for row in rows
    ]

    receipt_conditions = [
        str(x).strip().lower()
        for x in receipt.get(
            "condition_ids",
            [],
        )
    ]

    if (
        receipt_conditions
        != expected_conditions
    ):
        raise RuntimeError(
            "Stage-1 receipt condition order "
            "mismatch"
        )

    boundary = receipt.get(
        "analysis_boundary",
        {},
    )

    for key in (
        "external_consensus_calculated",
        "external_odds_read",
        "f19_trade_content_read",
        "f20_read",
        "h5_innovation_calculated",
        "markout_calculated",
        "pm_response_calculated",
        "pnl_calculated",
        "settlement_used",
        "winner_used",
    ):
        if boundary.get(key) is not False:
            raise RuntimeError(
                "Stage-1 boundary violation: "
                f"{key}"
            )


def load_static_inputs():
    identities = {
        M1A:
            EXPECTED_M1A_SHA,

        VISIBILITY_AMENDMENT:
            EXPECTED_VISIBILITY_AMENDMENT_SHA,

        COMPATIBILITY_CONTRACT:
            EXPECTED_COMPATIBILITY_CONTRACT_SHA,

        STAGE1_REGISTRAR:
            EXPECTED_STAGE1_REGISTRAR_SHA,

        STAGE2_RESOLVER:
            EXPECTED_STAGE2_RESOLVER_SHA,

        STAGE1_REGISTRY:
            EXPECTED_STAGE1_REGISTRY_SHA,

        STAGE1_RECEIPT:
            EXPECTED_STAGE1_RECEIPT_SHA,
    }

    for path, expected in identities.items():
        verify_sha(
            path,
            expected,
        )

    compatibility = read_json(
        COMPATIBILITY_CONTRACT
    )

    validate_compatibility_contract(
        compatibility
    )

    rows = read_jsonl(
        STAGE1_REGISTRY
    )

    validate_stage1_rows(
        rows
    )

    receipt = read_json(
        STAGE1_RECEIPT
    )

    validate_stage1_receipt(
        receipt,
        rows,
    )

    return {
        "compatibility":
            compatibility,

        "stage1_rows":
            rows,

        "stage1_receipt":
            receipt,

        "stage1_registry_sha256":
            EXPECTED_STAGE1_REGISTRY_SHA,

        "stage1_receipt_sha256":
            EXPECTED_STAGE1_RECEIPT_SHA,
    }


def validate_stage2_rows(
    rows: list[dict[str, Any]],
):
    if not 1 <= len(rows) <= 10:
        raise RuntimeError(
            "invalid Stage-2 map row count"
        )

    indexes: set[int] = set()
    conditions: set[str] = set()
    odds_ids: set[str] = set()

    last_index = 0

    required = {
        "study",
        "milestone",
        "registration_stage",
        "registration_index",
        "condition_id",
        "gamma_market_id",
        "gamma_slug",
        "p1_team",
        "p0_team",
        "pm_game_start_time",
        "odds_game_id",
        "provider_home_team",
        "provider_away_team",
        "canonical_commence_time",
        "start_delta_seconds",
        "resolution_status",
    }

    for row in rows:
        missing = (
            required
            - set(row)
        )

        if missing:
            raise RuntimeError(
                "Stage-2 row missing fields: "
                f"{sorted(missing)}"
            )

        if (
            row["study"] != "H5"
            or
            row["milestone"] != "H5-M1"
        ):
            raise RuntimeError(
                "Stage-2 study/milestone "
                "mismatch"
            )

        if (
            row["registration_stage"]
            !=
            "STAGE2_EXTERNAL_IDENTITY"
        ):
            raise RuntimeError(
                "Stage-2 registration-stage "
                "mismatch"
            )

        index = int(
            row["registration_index"]
        )

        if not 1 <= index <= 10:
            raise RuntimeError(
                "Stage-2 registration index "
                "outside 1..10"
            )

        if index <= last_index:
            raise RuntimeError(
                "Stage-2 rows not in strict "
                "Stage-1 order"
            )

        last_index = index

        if index in indexes:
            raise RuntimeError(
                "duplicate Stage-2 "
                "registration index"
            )

        condition = str(
            row["condition_id"]
        ).strip().lower()

        odds_id = str(
            row["odds_game_id"]
        ).strip()

        if (
            not condition
            or
            condition in conditions
        ):
            raise RuntimeError(
                "duplicate/empty Stage-2 "
                "condition ID"
            )

        if (
            not odds_id
            or
            odds_id in odds_ids
        ):
            raise RuntimeError(
                "duplicate/empty Stage-2 "
                "external event ID"
            )

        if (
            row["resolution_status"]
            not in ALLOWED_STAGE2_STATUSES
        ):
            raise RuntimeError(
                "unexpected Stage-2 status"
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
                "invalid Stage-2 start time"
            )

        delta = (
            canonical
            - pm_start
        ).total_seconds()

        recorded_delta = float(
            row[
                "start_delta_seconds"
            ]
        )

        if (
            abs(delta)
            > MAX_START_DELTA_SECONDS
        ):
            raise RuntimeError(
                "Stage-2 start delta exceeds "
                "frozen limit"
            )

        if (
            abs(
                delta
                - recorded_delta
            )
            > 1e-9
        ):
            raise RuntimeError(
                "Stage-2 recorded start delta "
                "mismatch"
            )

        indexes.add(
            index
        )

        conditions.add(
            condition
        )

        odds_ids.add(
            odds_id
        )


def validate_stage2_receipt(
    receipt: dict[str, Any],
    rows: list[dict[str, Any]],
):
    if (
        receipt.get("status")
        !=
        "STAGE2_EXTERNAL_ID_MAP_FROZEN"
    ):
        raise RuntimeError(
            "Stage-2 receipt status mismatch"
        )

    resolver = receipt.get(
        "resolver",
        {},
    )

    if (
        resolver.get("sha256")
        != EXPECTED_STAGE2_RESOLVER_SHA
    ):
        raise RuntimeError(
            "Stage-2 resolver SHA mismatch"
        )

    if (
        receipt.get("amendment_sha256")
        != EXPECTED_VISIBILITY_AMENDMENT_SHA
    ):
        raise RuntimeError(
            "Stage-2 amendment SHA mismatch"
        )

    stage1_meta = receipt.get(
        "stage1_registry",
        {},
    )

    if (
        stage1_meta.get("sha256")
        != EXPECTED_STAGE1_REGISTRY_SHA
    ):
        raise RuntimeError(
            "Stage-2 Stage-1 registry SHA "
            "mismatch"
        )

    if (
        int(
            stage1_meta.get(
                "row_count",
                -1,
            )
        )
        != 10
    ):
        raise RuntimeError(
            "Stage-2 Stage-1 row count "
            "mismatch"
        )

    if (
        receipt.get(
            "stage1_receipt_sha256"
        )
        != EXPECTED_STAGE1_RECEIPT_SHA
    ):
        raise RuntimeError(
            "Stage-2 Stage-1 receipt SHA "
            "mismatch"
        )

    if (
        receipt.get(
            "h2_exclusions_sha256"
        )
        != EXPECTED_EXCLUSIONS_SHA
    ):
        raise RuntimeError(
            "Stage-2 H2 exclusions SHA "
            "mismatch"
        )

    meta = receipt.get(
        "external_id_map",
        {},
    )

    if (
        meta.get("mutable")
        is not False
    ):
        raise RuntimeError(
            "Stage-2 map is not immutable"
        )

    if (
        int(
            meta.get(
                "row_count",
                -1,
            )
        )
        != len(rows)
    ):
        raise RuntimeError(
            "Stage-2 map row-count mismatch"
        )

    if (
        int(
            meta.get(
                "ambiguous_count",
                -1,
            )
        )
        != 0
    ):
        raise RuntimeError(
            "Stage-2 ambiguity gate failed"
        )

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

    if (
        len(nonburned)
        <
        MIN_RUNTIME_MARKETS
    ):
        raise RuntimeError(
            "fewer than five resolved "
            "nonburned Stage-2 markets"
        )

    if (
        int(
            meta.get(
                "resolved_nonburned_count",
                -1,
            )
        )
        != len(nonburned)
    ):
        raise RuntimeError(
            "Stage-2 nonburned count "
            "mismatch"
        )

    if (
        int(
            meta.get(
                "resolved_h2_burned_count",
                -1,
            )
        )
        != len(burned)
    ):
        raise RuntimeError(
            "Stage-2 burned count mismatch"
        )

    receipt_nonburned = [
        str(x).lower()
        for x in receipt.get(
            "nonburned_condition_ids",
            [],
        )
    ]

    actual_nonburned = [
        str(
            row["condition_id"]
        ).lower()
        for row in nonburned
    ]

    if (
        receipt_nonburned
        != actual_nonburned
    ):
        raise RuntimeError(
            "Stage-2 nonburned condition "
            "order mismatch"
        )

    receipt_burned = [
        str(x).lower()
        for x in receipt.get(
            "prohibited_h2_burned_condition_ids",
            [],
        )
    ]

    actual_burned = [
        str(
            row["condition_id"]
        ).lower()
        for row in burned
    ]

    if (
        receipt_burned
        != actual_burned
    ):
        raise RuntimeError(
            "Stage-2 burned condition "
            "order mismatch"
        )

    boundary = receipt.get(
        "analysis_boundary",
        {},
    )

    if not boundary:
        raise RuntimeError(
            "missing Stage-2 analysis "
            "boundary"
        )

    if any(
        value is not False
        for value in boundary.values()
    ):
        raise RuntimeError(
            "Stage-2 analysis boundary "
            "violation"
        )


def validate_runtime_rows(
    rows: list[dict[str, Any]],
):
    if not (
        MIN_RUNTIME_MARKETS
        <= len(rows)
        <= MAX_RUNTIME_MARKETS
    ):
        raise RuntimeError(
            "invalid H5 acquisition-market "
            "count"
        )

    indexes = [
        int(
            row["registration_index"]
        )
        for row in rows
    ]

    if indexes != sorted(
        indexes
    ):
        raise RuntimeError(
            "runtime registration indexes "
            "not increasing"
        )

    if (
        len(indexes)
        != len(set(indexes))
    ):
        raise RuntimeError(
            "duplicate runtime registration "
            "index"
        )

    conditions: set[str] = set()
    odds_ids: set[str] = set()
    tokens: set[str] = set()

    for row in rows:
        index = int(
            row["registration_index"]
        )

        if not 1 <= index <= 10:
            raise RuntimeError(
                "runtime registration index "
                "outside 1..10"
            )

        condition = str(
            row["condition_id"]
        ).lower()

        odds_id = str(
            row["odds_game_id"]
        )

        p1_token = str(
            row["p1_token_id"]
        )

        p0_token = str(
            row["p0_token_id"]
        )

        if (
            condition in conditions
            or
            odds_id in odds_ids
        ):
            raise RuntimeError(
                "duplicate runtime market "
                "identity"
            )

        if (
            p1_token == p0_token
            or
            p1_token in tokens
            or
            p0_token in tokens
        ):
            raise RuntimeError(
                "duplicate runtime token "
                "identity"
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
                "invalid runtime start time"
            )

        if (
            abs(
                (
                    canonical
                    - pm_start
                ).total_seconds()
            )
            >
            MAX_START_DELTA_SECONDS
        ):
            raise RuntimeError(
                "runtime start delta exceeds "
                "frozen limit"
            )

        provider_set = {
            norm_team(
                row["home_team"]
            ),
            norm_team(
                row["away_team"]
            ),
        }

        pm_set = {
            norm_team(
                row["p1_team"]
            ),
            norm_team(
                row["p0_team"]
            ),
        }

        if provider_set != pm_set:
            raise RuntimeError(
                "runtime team-set mismatch"
            )

        conditions.add(
            condition
        )

        odds_ids.add(
            odds_id
        )

        tokens.update(
            (
                p1_token,
                p0_token,
            )
        )


def join_stage1_stage2(
    stage1_rows: list[dict[str, Any]],
    stage2_rows: list[dict[str, Any]],
):
    validate_stage1_rows(
        stage1_rows
    )

    validate_stage2_rows(
        stage2_rows
    )

    stage1_by_index = {
        int(row["registration_index"]):
            row
        for row in stage1_rows
    }

    stage2_by_index = {
        int(row["registration_index"]):
            row
        for row in stage2_rows
    }

    if (
        len(stage2_by_index)
        != len(stage2_rows)
    ):
        raise RuntimeError(
            "duplicate Stage-2 index"
        )

    for index in stage2_by_index:
        if index not in stage1_by_index:
            raise RuntimeError(
                "Stage-2 row has no Stage-1 "
                "identity"
            )

    runtime_rows = []

    for stage1 in stage1_rows:
        index = int(
            stage1["registration_index"]
        )

        stage2 = stage2_by_index.get(
            index
        )

        if stage2 is None:
            continue

        exact_checks = (
            "condition_id",
            "gamma_market_id",
        )

        for key in exact_checks:
            left = str(
                stage1[key]
            ).strip()

            right = str(
                stage2[key]
            ).strip()

            if key == "condition_id":
                left = left.lower()
                right = right.lower()

            if left != right:
                raise RuntimeError(
                    "Stage1/Stage2 exact join "
                    f"mismatch: {key}"
                )

        for key in (
            "gamma_slug",
            "p1_team",
            "p0_team",
            "pm_game_start_time",
        ):
            if stage1[key] != stage2[key]:
                raise RuntimeError(
                    "Stage1/Stage2 equality "
                    f"mismatch: {key}"
                )

        status = stage2[
            "resolution_status"
        ]

        if (
            status
            ==
            "RESOLVED_H2_BURNED_PROHIBITED"
        ):
            continue

        if (
            status
            !=
            "RESOLVED_NONBURNED"
        ):
            raise RuntimeError(
                "unexpected Stage-2 status "
                "during join"
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
                status,
        })

        runtime_rows.append(
            merged
        )

    validate_runtime_rows(
        runtime_rows
    )

    return runtime_rows


def load_runtime_bundle():
    static = load_static_inputs()

    if (
        not STAGE2_MAP.is_file()
        or
        not STAGE2_RECEIPT.is_file()
    ):
        raise RuntimeError(
            "H5 Stage-2 map/receipt not frozen; "
            "live acquisition prohibited"
        )

    stage2_map_sha = sha256_file(
        STAGE2_MAP
    )

    stage2_receipt_sha = sha256_file(
        STAGE2_RECEIPT
    )

    stage2_rows = read_jsonl(
        STAGE2_MAP
    )

    validate_stage2_rows(
        stage2_rows
    )

    stage2_receipt = read_json(
        STAGE2_RECEIPT
    )

    validate_stage2_receipt(
        stage2_receipt,
        stage2_rows,
    )

    map_meta = stage2_receipt[
        "external_id_map"
    ]

    if (
        map_meta.get("sha256")
        != stage2_map_sha
    ):
        raise RuntimeError(
            "Stage-2 map SHA does not match "
            "freeze receipt"
        )

    runtime_rows = (
        join_stage1_stage2(
            static["stage1_rows"],
            stage2_rows,
        )
    )

    return {
        "rows":
            runtime_rows,

        "stage1_rows":
            static["stage1_rows"],

        "stage2_rows":
            stage2_rows,

        "stage1_registry_sha256":
            EXPECTED_STAGE1_REGISTRY_SHA,

        "stage1_receipt_sha256":
            EXPECTED_STAGE1_RECEIPT_SHA,

        "stage2_map_sha256":
            stage2_map_sha,

        "stage2_receipt_sha256":
            stage2_receipt_sha,

        "compatibility_contract_sha256":
            EXPECTED_COMPATIBILITY_CONTRACT_SHA,

        "stage1_registered_market_count":
            10,

        "acquisition_market_count":
            len(runtime_rows),
    }


def validate_static_output():
    static = load_static_inputs()

    print(
        "H5-M1 COLLECTOR COMPAT STATIC: PASS"
    )

    print(
        "compatibility contract SHA:",
        EXPECTED_COMPATIBILITY_CONTRACT_SHA,
    )

    print(
        "Stage-1 registry SHA:",
        static[
            "stage1_registry_sha256"
        ],
    )

    print(
        "Stage-1 receipt SHA:",
        static[
            "stage1_receipt_sha256"
        ],
    )

    print(
        "Stage-1 registered markets:",
        len(
            static["stage1_rows"]
        ),
    )

    print(
        "Stage-2 map frozen:",
        (
            "YES"
            if (
                STAGE2_MAP.is_file()
                and
                STAGE2_RECEIPT.is_file()
            )
            else
            "NO"
        ),
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
        "H5 innovation calculated: NO"
    )

    print(
        "PM response calculated: NO"
    )

    print(
        "PnL calculated: NO"
    )


def validate_map_output():
    bundle = load_runtime_bundle()

    print(
        "H5-M1 COLLECTOR COMPAT MAP: PASS"
    )

    print(
        "Stage-1 registered markets:",
        bundle[
            "stage1_registered_market_count"
        ],
    )

    print(
        "acquisition markets:",
        bundle[
            "acquisition_market_count"
        ],
    )

    print(
        "runtime registration indexes:",
        ",".join(
            str(
                row["registration_index"]
            )
            for row in bundle["rows"]
        ),
    )

    print(
        "Stage-2 map SHA:",
        bundle[
            "stage2_map_sha256"
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
        "H5 innovation calculated: NO"
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
        "--validate-static",
        action="store_true",
    )

    parser.add_argument(
        "--validate-map",
        action="store_true",
    )

    args = parser.parse_args()

    if (
        args.validate_static
        ==
        args.validate_map
    ):
        parser.error(
            "choose exactly one of "
            "--validate-static or "
            "--validate-map"
        )

    if args.validate_static:
        validate_static_output()
        return

    validate_map_output()


if __name__ == "__main__":
    main()
