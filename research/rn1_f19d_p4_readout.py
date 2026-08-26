from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path, PurePosixPath
import subprocess
from typing import Any, Sequence

import rn1_f19d_p2_core as core
import rn1_f19d_p3_adapter as adapter
import rn1_f19d_p3_readiness as p3


REPO = Path(__file__).resolve().parents[1]

CONTRACT = (
    REPO
    / "research"
    / "rn1_f19d_p4_readout_contract.json"
)

ACTIVATION_RECEIPT = (
    REPO
    / "research"
    / "rn1_f19d_p4_activation_receipt.json"
)

RESULT = (
    REPO
    / "data"
    / "prospective"
    / "rn1_f19"
    / "readout"
    / "rn1_f19d_p4_confirmatory.json"
)

REGISTRY = (
    REPO
    / "data"
    / "prospective"
    / "rn1_f19"
    / "registry.jsonl"
)

ACQUISITION = (
    REPO
    / "data"
    / "prospective"
    / "rn1_f19"
    / "acquisition"
)

WALLETS = (
    REPO
    / "config"
    / "research_wallets.json"
)

P4_TEST = (
    REPO
    / "research"
    / "test_rn1_f19d_p4_readout.py"
)

EXPECTED_P4_CONTRACT_SHA256 = (
    "88a8d08ddbe36e42a096e8dfb7868f29"
    "ecf5bd5830cee6ac4c4a94331c7fffef"
)

CORE_FIELDS = (
    "proxyWallet",
    "side",
    "asset",
    "conditionId",
    "size",
    "price",
    "timestamp",
    "transactionHash",
    "outcome",
    "outcomeIndex",
)


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def git_head(repo: Path = REPO) -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        text=True,
    ).strip()


def committed_bytes(
    repo: Path,
    relative_path: str,
) -> bytes:
    try:
        return subprocess.check_output(
            [
                "git",
                "show",
                f"HEAD:{relative_path}",
            ],
            cwd=repo,
        )
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            f"{relative_path} is not committed in HEAD"
        ) from exc


def require_worktree_matches_head(
    repo: Path,
    relative_path: str,
) -> None:
    path = repo / relative_path

    if not path.is_file():
        raise RuntimeError(
            f"missing required file: {relative_path}"
        )

    if path.read_bytes() != committed_bytes(
        repo,
        relative_path,
    ):
        raise RuntimeError(
            f"worktree differs from HEAD: {relative_path}"
        )


def read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(
        path.read_text(encoding="utf-8")
    )

    if not isinstance(payload, dict):
        raise RuntimeError(
            f"JSON object required: {path}"
        )

    return payload


def validate_config(
    repo: Path = REPO,
) -> dict[str, Any]:
    contract_path = (
        repo
        / "research"
        / "rn1_f19d_p4_readout_contract.json"
    )

    actual_contract_sha = sha256_file(
        contract_path
    )

    if (
        actual_contract_sha
        != EXPECTED_P4_CONTRACT_SHA256
    ):
        raise RuntimeError(
            "P4 contract SHA mismatch"
        )

    contract = read_json(contract_path)

    if contract.get("study") != "RN1-F19d-P4":
        raise RuntimeError(
            "unexpected P4 study"
        )

    if (
        contract.get("status")
        != "ONE_TIME_CONFIRMATORY_READOUT_ORCHESTRATION_FREEZE"
    ):
        raise RuntimeError(
            "unexpected P4 contract status"
        )

    parent_checks = {
        "p1_contract_sha256":
            "research/"
            "rn1_f19d_p1_implementation_completion_contract.json",

        "p2_core_contract_sha256":
            "research/"
            "rn1_f19d_p2_core_contract.json",

        "p2_core_sha256":
            "research/"
            "rn1_f19d_p2_core.py",

        "p2_validation_sha256":
            "research/"
            "rn1_f19d_p2_validation.json",

        "p3_contract_sha256":
            "research/"
            "rn1_f19d_p3_contract.json",

        "p3_adapter_sha256":
            "research/"
            "rn1_f19d_p3_adapter.py",

        "p3_readiness_sha256":
            "research/"
            "rn1_f19d_p3_readiness.py",

        "p3_validation_sha256":
            "research/"
            "rn1_f19d_p3_validation.json",
    }

    for key, rel in parent_checks.items():
        expected = contract["parents"][key]
        actual = sha256_file(repo / rel)

        if actual != expected:
            raise RuntimeError(
                f"P4 parent hash mismatch: {rel}"
            )

    frozen = contract["frozen_acquisition"]

    acquisition_checks = {
        "registry_sha256":
            "data/prospective/"
            "rn1_f19/registry.jsonl",

        "f19b_contract_sha256":
            "research/"
            "rn1_f19b_acquisition_contract.json",

        "f19b_collector_sha256":
            "collectors/polymarket/"
            "rn1_f19b_acquire.py",

        "f19c_contract_sha256":
            "research/"
            "rn1_f19c_operations_contract.json",

        "f19c_operator_sha256":
            "collectors/polymarket/"
            "rn1_f19c_operate.py",
    }

    for key, rel in acquisition_checks.items():
        expected = frozen[key]
        actual = sha256_file(repo / rel)

        if actual != expected:
            raise RuntimeError(
                f"P4 acquisition hash mismatch: {rel}"
            )

    wallet_binding = contract[
        "rn1_identity_binding"
    ]

    if (
        sha256_file(repo / wallet_binding["path"])
        != wallet_binding["sha256"]
    ):
        raise RuntimeError(
            "RN1 wallet config SHA mismatch"
        )

    if (
        contract["activation_gate"][
            "required_p3_status"
        ]
        != p3.READY
    ):
        raise RuntimeError(
            "P4/P3 READY status mismatch"
        )

    if (
        contract["authoritative_run"][
            "maximum_scientific_invocations"
        ]
        != 1
    ):
        raise RuntimeError(
            "P4 authoritative-run count mismatch"
        )

    if (
        contract["authoritative_run"][
            "result_create_only"
        ]
        is not True
    ):
        raise RuntimeError(
            "P4 result must be create-only"
        )

    return contract


def load_registry(
    repo: Path = REPO,
) -> list[dict[str, Any]]:
    path = (
        repo
        / "data"
        / "prospective"
        / "rn1_f19"
        / "registry.jsonl"
    )

    rows: list[dict[str, Any]] = []

    for line in path.read_text(
        encoding="utf-8"
    ).splitlines():
        if not line.strip():
            continue

        row = json.loads(line)

        if not isinstance(row, dict):
            raise RuntimeError(
                "registry row is not an object"
            )

        rows.append(row)

    if len(rows) != 100:
        raise RuntimeError(
            "P4 requires exactly 100 registry rows"
        )

    conditions = [
        str(row["condition_id"]).lower()
        for row in rows
    ]

    if len(set(conditions)) != 100:
        raise RuntimeError(
            "P4 registry condition IDs are not unique"
        )

    return rows


def load_rn1_wallet(
    repo: Path = REPO,
) -> str:
    payload = read_json(
        repo
        / "config"
        / "research_wallets.json"
    )

    candidates = [
        row
        for row in payload.get("wallets", [])
        if (
            isinstance(row, dict)
            and row.get("name") == "RN1"
        )
    ]

    if len(candidates) != 1:
        raise RuntimeError(
            "expected exactly one RN1 wallet"
        )

    wallet = str(
        candidates[0].get("address") or ""
    ).strip().lower()

    if not wallet:
        raise RuntimeError(
            "RN1 wallet address missing"
        )

    return wallet


def canonical_core(
    row: dict[str, Any],
) -> dict[str, Any]:
    return {
        field: row.get(field)
        for field in CORE_FIELDS
    }


def fingerprint_rows(
    rows: Sequence[dict[str, Any]],
) -> str:
    canonical = [
        json.dumps(
            canonical_core(row),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        for row in rows
    ]

    canonical.sort()

    joined = "\n".join(
        canonical
    ).encode("utf-8")

    return sha256_bytes(joined)


def flatten_pass_rows(
    payload: dict[str, Any],
) -> list[dict[str, Any]]:
    pages = payload.get("pages")

    if not isinstance(pages, list):
        raise RuntimeError(
            "pass pages must be a list"
        )

    rows: list[dict[str, Any]] = []

    for page in pages:
        if not isinstance(page, dict):
            raise RuntimeError(
                "pass page must be an object"
            )

        page_rows = page.get("rows")

        if not isinstance(page_rows, list):
            raise RuntimeError(
                "pass page rows must be a list"
            )

        for row in page_rows:
            if not isinstance(row, dict):
                raise RuntimeError(
                    "trade row must be an object"
                )

        rows.extend(page_rows)

    return rows


def safe_manifest_pass_path(
    market_dir: Path,
    *,
    stream_name: str,
    value: Any,
) -> Path:
    text = str(
        value if value is not None else ""
    ).strip()

    rel = PurePosixPath(text)

    if (
        not text
        or rel.is_absolute()
        or ".." in rel.parts
    ):
        raise RuntimeError(
            "unsafe acquisition pass path"
        )

    if (
        len(rel.parts) != 2
        or rel.parts[0] != stream_name
        or rel.name not in {
            "pass_1.json",
            "pass_2.json",
        }
    ):
        raise RuntimeError(
            "unexpected acquisition pass path"
        )

    return market_dir.joinpath(*rel.parts)


def selected_manifest_pass(
    stream_summary: dict[str, Any],
    *,
    pass_number: int,
) -> dict[str, Any]:
    passes = stream_summary.get("passes")

    if not isinstance(passes, list):
        raise RuntimeError(
            "manifest stream passes invalid"
        )

    selected = [
        row
        for row in passes
        if (
            isinstance(row, dict)
            and row.get("pass") == pass_number
        )
    ]

    if len(selected) != 1:
        raise RuntimeError(
            "expected exactly one selected pass"
        )

    return selected[0]


def parse_game_start(
    value: Any,
) -> datetime:
    dt = datetime.fromisoformat(
        str(value).replace("Z", "+00:00")
    )

    if dt.tzinfo is None:
        raise RuntimeError(
            "game start must be timezone-aware"
        )

    return dt.astimezone(timezone.utc)


def create_activation_receipt(
    repo: Path = REPO,
) -> Path:
    contract = validate_config(repo)

    receipt_path = (
        repo
        / "research"
        / "rn1_f19d_p4_activation_receipt.json"
    )

    result_path = (
        repo
        / "data"
        / "prospective"
        / "rn1_f19"
        / "readout"
        / "rn1_f19d_p4_confirmatory.json"
    )

    if receipt_path.exists():
        raise RuntimeError(
            "activation receipt already exists"
        )

    if result_path.exists():
        raise RuntimeError(
            "scientific result already exists"
        )

    require_worktree_matches_head(
        repo,
        "research/rn1_f19d_p4_readout.py",
    )

    require_worktree_matches_head(
        repo,
        "research/test_rn1_f19d_p4_readout.py",
    )

    snapshot = p3.scan_readiness(repo)

    if snapshot.status != p3.READY:
        raise RuntimeError(
            "P3 is not READY; activation prohibited"
        )

    registry = load_registry(repo)
    acquisition_root = (
        repo
        / "data"
        / "prospective"
        / "rn1_f19"
        / "acquisition"
    )

    markets: list[dict[str, Any]] = []

    for registry_row in sorted(
        registry,
        key=lambda row: (
            str(row["game_start_time_utc"]),
            str(row["condition_id"]).lower(),
        ),
    ):
        condition = str(
            registry_row["condition_id"]
        ).lower()

        market_dir = (
            acquisition_root
            / condition
        )

        manifest_path = (
            market_dir
            / "manifest.json"
        )

        manifest = read_json(
            manifest_path
        )

        stream_receipts: dict[
            str,
            Any,
        ] = {}

        for stream_name in (
            "market_taker",
            "rn1_all",
        ):
            stream_summary = manifest[
                "streams"
            ][stream_name]

            pass_receipts: dict[
                str,
                Any,
            ] = {}

            for pass_number in (1, 2):
                pass_summary = (
                    selected_manifest_pass(
                        stream_summary,
                        pass_number=pass_number,
                    )
                )

                pass_path = (
                    safe_manifest_pass_path(
                        market_dir,
                        stream_name=stream_name,
                        value=pass_summary[
                            "path"
                        ],
                    )
                )

                pass_receipts[
                    str(pass_number)
                ] = {
                    "path":
                        pass_path.relative_to(
                            repo
                        ).as_posix(),

                    "sha256":
                        sha256_file(
                            pass_path
                        ),

                    "row_count":
                        pass_summary[
                            "row_count"
                        ],

                    "canonical_core_sha256":
                        pass_summary[
                            "canonical_core_sha256"
                        ],
                }

            stream_receipts[
                stream_name
            ] = {
                "row_count":
                    stream_summary[
                        "row_count"
                    ],

                "canonical_core_sha256":
                    stream_summary[
                        "canonical_core_sha256"
                    ],

                "passes":
                    pass_receipts,
            }

        markets.append(
            {
                "condition_id":
                    condition,

                "manifest_path":
                    manifest_path.relative_to(
                        repo
                    ).as_posix(),

                "manifest_sha256":
                    sha256_file(
                        manifest_path
                    ),

                "streams":
                    stream_receipts,
            }
        )

    if len(markets) != 100:
        raise RuntimeError(
            "activation receipt did not bind 100 markets"
        )

    receipt = {
        "study":
            "RN1-F19d-P4",

        "status":
            "READY_COHORT_ACTIVATION_RECEIPT",

        "p3_snapshot":
            asdict(snapshot),

        "p4_contract_sha256":
            EXPECTED_P4_CONTRACT_SHA256,

        "p4_implementation_sha256":
            sha256_file(
                repo
                / "research"
                / "rn1_f19d_p4_readout.py"
            ),

        "p4_test_sha256":
            sha256_file(
                repo
                / "research"
                / "test_rn1_f19d_p4_readout.py"
            ),

        "registry_sha256":
            contract[
                "frozen_acquisition"
            ][
                "registry_sha256"
            ],

        "git_head_before_receipt":
            git_head(repo),

        "scientific_input_pass":
            1,

        "markets":
            markets,

        "analysis_boundary": {
            "trade_json_parsed":
                False,

            "signal_calculated":
                False,

            "markout_calculated":
                False,

            "f20_read":
                False,

            "winner_used":
                False,

            "settlement_used":
                False,

            "execution_economics_calculated":
                False,

            "pnl_calculated":
                False,
        },
    }

    receipt_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with receipt_path.open(
        "x",
        encoding="utf-8",
    ) as f:
        json.dump(
            receipt,
            f,
            indent=2,
            sort_keys=True,
        )
        f.write("\n")

    return receipt_path


def validate_activation_receipt(
    repo: Path = REPO,
) -> dict[str, Any]:
    receipt_path = (
        repo
        / "research"
        / "rn1_f19d_p4_activation_receipt.json"
    )

    require_worktree_matches_head(
        repo,
        "research/rn1_f19d_p4_readout.py",
    )

    require_worktree_matches_head(
        repo,
        "research/test_rn1_f19d_p4_readout.py",
    )

    require_worktree_matches_head(
        repo,
        "research/rn1_f19d_p4_activation_receipt.json",
    )

    receipt = read_json(
        receipt_path
    )

    if (
        receipt.get("status")
        != "READY_COHORT_ACTIVATION_RECEIPT"
    ):
        raise RuntimeError(
            "invalid activation receipt status"
        )

    if (
        receipt.get("p4_contract_sha256")
        != EXPECTED_P4_CONTRACT_SHA256
    ):
        raise RuntimeError(
            "activation P4 contract mismatch"
        )

    implementation_sha = sha256_file(
        repo
        / "research"
        / "rn1_f19d_p4_readout.py"
    )

    test_sha = sha256_file(
        repo
        / "research"
        / "test_rn1_f19d_p4_readout.py"
    )

    if (
        receipt.get(
            "p4_implementation_sha256"
        )
        != implementation_sha
    ):
        raise RuntimeError(
            "activation implementation mismatch"
        )

    if (
        receipt.get(
            "p4_test_sha256"
        )
        != test_sha
    ):
        raise RuntimeError(
            "activation test mismatch"
        )

    if (
        receipt.get("registry_sha256")
        != sha256_file(
            repo
            / "data"
            / "prospective"
            / "rn1_f19"
            / "registry.jsonl"
        )
    ):
        raise RuntimeError(
            "activation registry mismatch"
        )

    markets = receipt.get("markets")

    if (
        not isinstance(markets, list)
        or len(markets) != 100
    ):
        raise RuntimeError(
            "activation receipt market count invalid"
        )

    for market in markets:
        if not isinstance(market, dict):
            raise RuntimeError(
                "activation market entry invalid"
            )

        manifest_path = (
            repo
            / str(
                market["manifest_path"]
            )
        )

        if (
            sha256_file(manifest_path)
            != market["manifest_sha256"]
        ):
            raise RuntimeError(
                "activation manifest SHA mismatch"
            )

        streams = market["streams"]

        for stream_name in (
            "market_taker",
            "rn1_all",
        ):
            stream = streams[
                stream_name
            ]

            for pass_number in (
                "1",
                "2",
            ):
                entry = stream[
                    "passes"
                ][pass_number]

                pass_path = (
                    repo
                    / entry["path"]
                )

                if (
                    sha256_file(pass_path)
                    != entry["sha256"]
                ):
                    raise RuntimeError(
                        "activation pass SHA mismatch"
                    )

    return receipt


def claim_authoritative_invocation(
    repo: Path = REPO,
) -> Path:
    """
    Create the one-time scientific invocation sentinel.

    This MUST happen before any prospective trade JSON is parsed.
    If the process later fails, the sentinel remains and prevents
    a second scientific invocation.
    """
    sentinel = (
        repo
        / "research"
        / "rn1_f19d_p4_authoritative_invocation.json"
    )

    result_path = (
        repo
        / "data"
        / "prospective"
        / "rn1_f19"
        / "readout"
        / "rn1_f19d_p4_confirmatory.json"
    )

    receipt_path = (
        repo
        / "research"
        / "rn1_f19d_p4_activation_receipt.json"
    )

    if result_path.exists():
        raise RuntimeError(
            "confirmatory result already exists"
        )

    if sentinel.exists():
        raise RuntimeError(
            "authoritative scientific invocation "
            "already claimed; rerun prohibited"
        )

    if not receipt_path.is_file():
        raise RuntimeError(
            "activation receipt missing"
        )

    payload = {
        "study":
            "RN1-F19d-P4",

        "status":
            "AUTHORITATIVE_INVOCATION_CLAIMED",

        "claimed_at_utc":
            datetime.now(
                timezone.utc
            ).isoformat(),

        "git_head":
            git_head(repo),

        "p4_contract_sha256":
            EXPECTED_P4_CONTRACT_SHA256,

        "activation_receipt_sha256":
            sha256_file(
                receipt_path
            ),

        "analysis_boundary_at_claim": {
            "trade_json_parsed":
                False,

            "signal_calculated":
                False,

            "markout_calculated":
                False,

            "f20_read":
                False,

            "winner_used":
                False,

            "settlement_used":
                False,

            "execution_economics_calculated":
                False,

            "pnl_calculated":
                False,
        },

        "failure_governance":
            "If this invocation terminates without the "
            "create-only confirmatory result, do not invoke "
            "the scientific readout again. Freeze the "
            "operational outcome before any recovery decision.",
    }

    with sentinel.open(
        "x",
        encoding="utf-8",
    ) as f:
        json.dump(
            payload,
            f,
            indent=2,
            sort_keys=True,
        )
        f.write("\n")

    return sentinel



def load_verified_stream(
    *,
    repo: Path,
    condition_id: str,
    market_dir: Path,
    stream_name: str,
    stream_summary: dict[str, Any],
    receipt_stream: dict[str, Any],
) -> list[dict[str, Any]]:
    pass_summary = selected_manifest_pass(
        stream_summary,
        pass_number=1,
    )

    pass_path = safe_manifest_pass_path(
        market_dir,
        stream_name=stream_name,
        value=pass_summary["path"],
    )

    receipt_pass = receipt_stream[
        "passes"
    ]["1"]

    expected_repo_path = (
        pass_path.relative_to(
            repo
        ).as_posix()
    )

    if (
        receipt_pass["path"]
        != expected_repo_path
    ):
        raise RuntimeError(
            "activation/pass path mismatch"
        )

    if (
        sha256_file(pass_path)
        != receipt_pass["sha256"]
    ):
        raise RuntimeError(
            "scientific input pass SHA mismatch"
        )

    payload = read_json(
        pass_path
    )

    if payload.get("stream") != stream_name:
        raise RuntimeError(
            "pass stream mismatch"
        )

    if payload.get("pass") != 1:
        raise RuntimeError(
            "scientific pass number mismatch"
        )

    if (
        str(
            payload.get("condition_id")
            or ""
        ).lower()
        != condition_id
    ):
        raise RuntimeError(
            "pass condition mismatch"
        )

    rows = flatten_pass_rows(
        payload
    )

    expected_count = stream_summary[
        "row_count"
    ]

    expected_core_sha = stream_summary[
        "canonical_core_sha256"
    ]

    if (
        len(rows)
        != expected_count
        or payload.get("row_count")
        != expected_count
        or pass_summary.get("row_count")
        != expected_count
        or receipt_stream.get(
            "row_count"
        )
        != expected_count
    ):
        raise RuntimeError(
            "scientific pass row_count mismatch"
        )

    actual_core_sha = fingerprint_rows(
        rows
    )

    if (
        actual_core_sha
        != expected_core_sha
        or payload.get(
            "canonical_core_sha256"
        )
        != expected_core_sha
        or pass_summary.get(
            "canonical_core_sha256"
        )
        != expected_core_sha
        or receipt_stream.get(
            "canonical_core_sha256"
        )
        != expected_core_sha
    ):
        raise RuntimeError(
            "scientific pass canonical SHA mismatch"
        )

    return rows


def decimal_text(
    value: Decimal | None,
) -> str | None:
    if value is None:
        return None
    return str(value)


def decimal_map(
    values: dict[str, Decimal],
) -> dict[str, str]:
    return {
        key: str(value)
        for key, value in values.items()
    }


def summarize_results(
    results: Sequence[
        core.MatchedResult
    ],
) -> dict[str, Any]:
    matched_count = sum(
        1
        for result in results
        if result.matched
    )

    usable_results = [
        result
        for result in results
        if result.usable
    ]

    usable_markets = sorted(
        {
            result.flagged.trade.condition_id
            for result in usable_results
        }
    )

    primary = (
        core.market_equal_difference(
            results
        )
    )

    declustered_results = (
        core.decluster_usable(
            results
        )
    )

    declustered = (
        core.market_equal_difference(
            declustered_results
        )
    )

    per_market = (
        core.per_market_primary_means(
            results
        )
    )

    bootstrap = (
        core.bootstrap_market_mean_ci(
            per_market,
            draws=10_000,
            seed=19018,
        )
    )

    decision = (
        core.confirmation_decision(
            matched_flagged_events=(
                matched_count
            ),
            usable_market_count=(
                len(usable_markets)
            ),
            primary_market_equal=(
                primary
            ),
            declustered_market_equal=(
                declustered
            ),
            bootstrap_ci=(
                bootstrap
            ),
        )
    )

    medians = core.market_medians(
        results
    )

    lomo = (
        core.leave_one_market_out(
            per_market
        )
    )

    sign_count = (
        core.market_sign_count(
            per_market
        )
    )

    return {
        "status":
            decision.status,

        "matched_flagged_events":
            matched_count,

        "usable_matched_flagged_events":
            len(usable_results),

        "usable_markets":
            len(usable_markets),

        "declustered_usable_events":
            len(declustered_results),

        "primary_market_equal":
            decimal_text(
                primary
            ),

        "declustered_market_equal":
            decimal_text(
                declustered
            ),

        "event_equal_secondary":
            decimal_text(
                core.event_equal_difference(
                    results
                )
            ),

        "bootstrap_95_ci":
            (
                None
                if bootstrap is None
                else {
                    "lower":
                        str(bootstrap[0]),
                    "upper":
                        str(bootstrap[1]),
                }
            ),

        "per_market_primary_means":
            decimal_map(
                per_market
            ),

        "market_medians":
            decimal_map(
                medians
            ),

        "market_sign_count":
            sign_count,

        "leave_one_market_out":
            decimal_map(
                lomo
            ),
    }


def analyze_cohort_rows(
    *,
    registry_rows: Sequence[
        dict[str, Any]
    ],
    rows_by_condition: dict[
        str,
        dict[str, list[dict[str, Any]]],
    ],
    rn1_wallet: str,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
]:
    opportunities: list[
        core.MakerOpportunity
    ] = []

    market_taker_by_condition: dict[
        str,
        list[core.Trade],
    ] = {}

    diagnostics: dict[
        str,
        dict[str, Any],
    ] = {}

    ordered_registry = sorted(
        registry_rows,
        key=lambda row: (
            str(
                row[
                    "game_start_time_utc"
                ]
            ),
            str(
                row[
                    "condition_id"
                ]
            ).lower(),
        ),
    )

    for registry_row in ordered_registry:
        condition = str(
            registry_row["condition_id"]
        ).lower()

        if condition not in rows_by_condition:
            raise RuntimeError(
                "missing condition rows"
            )

        raw = rows_by_condition[
            condition
        ]

        market_taker = adapter.adapt_rows(
            raw["market_taker"],
            expected_condition_id=condition,
        )

        rn1_all = adapter.adapt_rows(
            raw["rn1_all"],
            expected_condition_id=condition,
            rn1_wallet=rn1_wallet,
        )

        rn1_taker, rn1_maker = (
            core.classify_rn1_roles(
                rn1_all=rn1_all,
                market_taker=market_taker,
                rn1_wallet=rn1_wallet,
            )
        )

        game_start = parse_game_start(
            registry_row[
                "game_start_time_utc"
            ]
        )

        market_taker_by_condition[
            condition
        ] = market_taker

        market_opportunities: list[
            core.MakerOpportunity
        ] = []

        for maker in rn1_maker:
            imbalance = core.flow_imbalance(
                maker_inventory_direction=(
                    maker.p1_direction
                ),
                maker_fill_time=(
                    maker.timestamp
                ),
                market_taker=(
                    market_taker
                ),
                rn1_wallet=(
                    rn1_wallet
                ),
            )

            market_opportunities.append(
                core.MakerOpportunity(
                    trade=maker,
                    game_start_time=game_start,
                    flagged=core.is_flagged(
                        imbalance
                    ),
                )
            )

        opportunities.extend(
            market_opportunities
        )

        diagnostics[
            condition
        ] = {
            "market_taker_rows":
                len(market_taker),

            "rn1_all_rows":
                len(rn1_all),

            "rn1_taker_rows":
                len(rn1_taker),

            "rn1_maker_rows":
                len(rn1_maker),

            "flagged_maker_events":
                sum(
                    1
                    for event
                    in market_opportunities
                    if event.flagged
                ),

            "unflagged_maker_events":
                sum(
                    1
                    for event
                    in market_opportunities
                    if not event.flagged
                ),

            "matched_flagged_events":
                0,

            "usable_matched_flagged_events":
                0,
        }

    flagged_events = [
        event
        for event in opportunities
        if event.flagged
    ]

    control_pool = [
        event
        for event in opportunities
        if not event.flagged
    ]

    selected = core.select_controls(
        flagged_events=flagged_events,
        control_pool=control_pool,
    )

    results: list[
        core.MatchedResult
    ] = []

    for flagged in sorted(
        flagged_events,
        key=core.flagged_processing_key,
    ):
        condition = (
            flagged.trade.condition_id
        )

        result = core.build_matched_result(
            flagged=flagged,
            selected_controls=selected[
                flagged.stable_event_key
            ],
            market_taker=(
                market_taker_by_condition[
                    condition
                ]
            ),
        )

        results.append(
            result
        )

        if result.matched:
            diagnostics[
                condition
            ][
                "matched_flagged_events"
            ] += 1

        if result.usable:
            diagnostics[
                condition
            ][
                "usable_matched_flagged_events"
            ] += 1

    return (
        summarize_results(
            results
        ),
        diagnostics,
    )


def run_live(
    repo: Path = REPO,
) -> Path:
    contract = validate_config(
        repo
    )

    result_path = (
        repo
        / "data"
        / "prospective"
        / "rn1_f19"
        / "readout"
        / "rn1_f19d_p4_confirmatory.json"
    )

    if result_path.exists():
        raise RuntimeError(
            "confirmatory result already exists"
        )

    require_worktree_matches_head(
        repo,
        "research/rn1_f19d_p4_readout.py",
    )

    require_worktree_matches_head(
        repo,
        "research/test_rn1_f19d_p4_readout.py",
    )

    readiness = p3.scan_readiness(
        repo
    )

    if readiness.status != p3.READY:
        raise RuntimeError(
            "P3 is not READY; scientific readout prohibited"
        )

    receipt = (
        validate_activation_receipt(
            repo
        )
    )

    # Final fail-closed boundary:
    # claim the sole scientific invocation BEFORE
    # any prospective trade JSON is parsed.
    claim_authoritative_invocation(
        repo
    )

    registry = load_registry(
        repo
    )

    rn1_wallet = load_rn1_wallet(
        repo
    )

    receipt_by_condition = {
        str(
            row["condition_id"]
        ).lower():
            row
        for row in receipt[
            "markets"
        ]
    }

    rows_by_condition: dict[
        str,
        dict[str, list[dict[str, Any]]],
    ] = {}

    for registry_row in registry:
        condition = str(
            registry_row["condition_id"]
        ).lower()

        market_dir = (
            repo
            / "data"
            / "prospective"
            / "rn1_f19"
            / "acquisition"
            / condition
        )

        manifest = read_json(
            market_dir
            / "manifest.json"
        )

        receipt_market = (
            receipt_by_condition[
                condition
            ]
        )

        rows_by_condition[
            condition
        ] = {}

        for stream_name in (
            "market_taker",
            "rn1_all",
        ):
            rows_by_condition[
                condition
            ][stream_name] = (
                load_verified_stream(
                    repo=repo,
                    condition_id=condition,
                    market_dir=market_dir,
                    stream_name=stream_name,
                    stream_summary=(
                        manifest[
                            "streams"
                        ][
                            stream_name
                        ]
                    ),
                    receipt_stream=(
                        receipt_market[
                            "streams"
                        ][
                            stream_name
                        ]
                    ),
                )
            )

    summary, diagnostics = (
        analyze_cohort_rows(
            registry_rows=registry,
            rows_by_condition=(
                rows_by_condition
            ),
            rn1_wallet=rn1_wallet,
        )
    )

    result = {
        "study":
            "RN1-F19",

        "milestone":
            "RN1-F19d-P4-CONFIRMATORY",

        "status":
            summary["status"],

        "created_at_utc":
            datetime.now(
                timezone.utc
            ).isoformat(),

        "provenance": {
            "git_head":
                git_head(repo),

            "p4_contract_sha256":
                EXPECTED_P4_CONTRACT_SHA256,

            "p4_implementation_sha256":
                sha256_file(
                    repo
                    / "research"
                    / "rn1_f19d_p4_readout.py"
                ),

            "p4_test_sha256":
                sha256_file(
                    repo
                    / "research"
                    / "test_rn1_f19d_p4_readout.py"
                ),

            "activation_receipt_sha256":
                sha256_file(
                    repo
                    / "research"
                    / "rn1_f19d_p4_activation_receipt.json"
                ),

            "registry_sha256":
                contract[
                    "frozen_acquisition"
                ][
                    "registry_sha256"
                ],
        },

        "readiness_snapshot":
            asdict(readiness),

        "confirmation":
            summary,

        "market_diagnostics":
            diagnostics,

        "analysis_boundary": {
            "winner_used":
                False,

            "settlement_used":
                False,

            "f20_read":
                False,

            "execution_economics_calculated":
                False,

            "pnl_calculated":
                False,

            "parameter_optimization":
                False,

            "prospective_confirmation_claim_only":
                True,
        },
    }

    result_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with result_path.open(
        "x",
        encoding="utf-8",
    ) as f:
        json.dump(
            result,
            f,
            indent=2,
            sort_keys=True,
        )
        f.write("\n")

    return result_path


def print_config() -> None:
    contract = validate_config(
        REPO
    )

    print(
        "========================================"
    )
    print(
        "RN1-F19d-P4 CONFIG VALIDATION"
    )
    print(
        "========================================"
    )
    print("status: PASS")
    print(
        "P4 contract SHA:",
        EXPECTED_P4_CONTRACT_SHA256,
    )
    print(
        "required P3 status:",
        contract[
            "activation_gate"
        ][
            "required_p3_status"
        ],
    )
    print(
        "authoritative runs:",
        contract[
            "authoritative_run"
        ][
            "maximum_scientific_invocations"
        ],
    )
    print(
        "bootstrap draws:",
        contract[
            "frozen_scientific_semantics"
        ][
            "bootstrap_draws"
        ],
    )
    print(
        "bootstrap seed:",
        contract[
            "frozen_scientific_semantics"
        ][
            "bootstrap_seed"
        ],
    )
    print(
        "real prospective trade JSON parsed: NO"
    )
    print(
        "signal calculated: NO"
    )
    print(
        "markout calculated: NO"
    )
    print(
        "F20 read: NO"
    )
    print(
        "PnL calculated: NO"
    )


def main() -> None:
    parser = argparse.ArgumentParser()

    group = parser.add_mutually_exclusive_group(
        required=True
    )

    group.add_argument(
        "--validate-config",
        action="store_true",
    )

    group.add_argument(
        "--create-activation-receipt",
        action="store_true",
    )

    group.add_argument(
        "--run-live",
        action="store_true",
    )

    args = parser.parse_args()

    if args.validate_config:
        print_config()
        return

    if args.create_activation_receipt:
        path = create_activation_receipt(
            REPO
        )

        print(
            "========================================"
        )
        print(
            "RN1-F19d-P4 ACTIVATION RECEIPT"
        )
        print(
            "========================================"
        )
        print(
            "path:",
            path.relative_to(
                REPO
            ),
        )
        print(
            "SHA256:",
            sha256_file(
                path
            ),
        )
        print(
            "trade JSON parsed: NO"
        )
        print(
            "signal calculated: NO"
        )
        print(
            "markout calculated: NO"
        )
        print(
            "PnL calculated: NO"
        )
        return

    path = run_live(
        REPO
    )

    result = read_json(
        path
    )

    confirmation = result[
        "confirmation"
    ]

    print(
        "========================================"
    )
    print(
        "RN1-F19 CONFIRMATORY READOUT"
    )
    print(
        "========================================"
    )
    print(
        "status:",
        result["status"],
    )
    print(
        "matched flagged events:",
        confirmation[
            "matched_flagged_events"
        ],
    )
    print(
        "usable markets:",
        confirmation[
            "usable_markets"
        ],
    )
    print(
        "primary market-equal:",
        confirmation[
            "primary_market_equal"
        ],
    )
    print(
        "declustered market-equal:",
        confirmation[
            "declustered_market_equal"
        ],
    )
    print(
        "bootstrap 95% CI:",
        confirmation[
            "bootstrap_95_ci"
        ],
    )
    print(
        "F20 read: NO"
    )
    print(
        "execution economics calculated: NO"
    )
    print(
        "PnL calculated: NO"
    )


if __name__ == "__main__":
    main()
