from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path, PurePosixPath
from typing import Any


EXPECTED_REGISTRY_SHA256 = (
    "e284e82699a60cae080ef100f095e1cb358a7dd85c2bc7c1fcbf06a25c68405b"
)
EXPECTED_F19B_CONTRACT_SHA256 = (
    "2e43aff44ec9bae5813fa2b1fd154c7b7b04872b02170542bd2e99bc55b2625f"
)
EXPECTED_F19B_COLLECTOR_SHA256 = (
    "5d9970659dc0d8d0d7f5ae9b1122c1d70a4060e4871b889eb135467ad0c10760"
)
EXPECTED_F19C_CONTRACT_SHA256 = (
    "69a5274d7f1e42344c374fef0b24083a61083beaacd5d613b1afd738a3c4f569"
)

REQUIRED_REGISTRY_ROWS = 100

READY = "READY_FOR_ONE_TIME_CONFIRMATORY_READOUT"
NOT_READY = "NOT_READY_FOR_CONFIRMATORY_READOUT"
FAIL = "FAIL_READINESS_INTEGRITY"


@dataclass(frozen=True)
class ReadinessSnapshot:
    status: str
    registry_rows: int
    unique_conditions: int
    valid_finalized_markets: int
    remaining_markets: int
    invalid_manifests: int
    markets_with_missing_referenced_raw_files: int
    orphan_condition_directories: int
    temporary_directories: int
    extra_acquisition_directories: int
    registry_sha256: str
    f19b_contract_sha256: str
    f19b_collector_sha256: str
    integrity_errors: tuple[str, ...]
    f19_trade_content_read: bool = False
    f20_read: bool = False
    signal_calculated: bool = False
    markout_calculated: bool = False
    pnl_calculated: bool = False


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_registry(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        raise ValueError(f"missing frozen registry: {path}")

    rows: list[dict[str, Any]] = []

    for lineno, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(),
        start=1,
    ):
        if not line.strip():
            continue

        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"registry line {lineno} is invalid JSON"
            ) from exc

        if not isinstance(row, dict):
            raise ValueError(
                f"registry line {lineno} is not an object"
            )

        rows.append(row)

    return rows


def _condition(value: Any) -> str:
    text = str(value if value is not None else "").strip().lower()

    if not text:
        raise ValueError("empty condition_id")

    return text


def _is_sha256(value: Any) -> bool:
    text = str(value if value is not None else "")

    return (
        len(text) == 64
        and all(
            ch in "0123456789abcdef"
            for ch in text.lower()
        )
    )


def _safe_relative_pass_path(
    market_dir: Path,
    stream_name: str,
    value: Any,
) -> Path:
    text = str(value if value is not None else "").strip()

    if not text:
        raise ValueError("empty pass path")

    rel = PurePosixPath(text)

    if rel.is_absolute() or ".." in rel.parts:
        raise ValueError(f"unsafe pass path: {text!r}")

    if len(rel.parts) != 2 or rel.parts[0] != stream_name:
        raise ValueError(
            f"unexpected {stream_name} pass path: {text!r}"
        )

    if rel.name not in {"pass_1.json", "pass_2.json"}:
        raise ValueError(
            f"unexpected pass filename: {rel.name!r}"
        )

    return market_dir.joinpath(*rel.parts)


def _validate_stream(
    *,
    market_dir: Path,
    name: str,
    payload: Any,
) -> tuple[list[str], bool]:
    errors: list[str] = []
    missing_raw = False

    if not isinstance(payload, dict):
        return [
            f"{name}: stream summary is not an object"
        ], False

    if payload.get("stream") != name:
        errors.append(f"{name}: stream label mismatch")

    if payload.get("status") != "PASS":
        errors.append(f"{name}: stream status is not PASS")

    row_count = payload.get("row_count")

    if (
        isinstance(row_count, bool)
        or not isinstance(row_count, int)
        or row_count < 0
    ):
        errors.append(f"{name}: invalid row_count")

    core_sha = payload.get("canonical_core_sha256")

    if not _is_sha256(core_sha):
        errors.append(
            f"{name}: invalid canonical core SHA"
        )

    passes = payload.get("passes")

    if not isinstance(passes, list) or len(passes) != 2:
        errors.append(
            f"{name}: expected exactly two pass summaries"
        )
        return errors, missing_raw

    seen_pass_numbers: set[int] = set()

    for entry in passes:
        if not isinstance(entry, dict):
            errors.append(
                f"{name}: pass summary is not an object"
            )
            continue

        pass_no = entry.get("pass")

        if pass_no not in {1, 2}:
            errors.append(
                f"{name}: invalid pass number {pass_no!r}"
            )
        elif pass_no in seen_pass_numbers:
            errors.append(
                f"{name}: duplicate pass number {pass_no}"
            )
        else:
            seen_pass_numbers.add(pass_no)

        if entry.get("row_count") != row_count:
            errors.append(
                f"{name}: pass row_count mismatch"
            )

        if entry.get("canonical_core_sha256") != core_sha:
            errors.append(
                f"{name}: pass canonical SHA mismatch"
            )

        try:
            pass_path = _safe_relative_pass_path(
                market_dir,
                name,
                entry.get("path"),
            )
        except ValueError as exc:
            errors.append(f"{name}: {exc}")
            continue

        # P3 boundary: existence only.
        # Raw prospective trade content is never opened.
        if not pass_path.is_file():
            missing_raw = True
            errors.append(
                f"{name}: referenced raw file missing: "
                f"{pass_path.relative_to(market_dir)}"
            )

    if seen_pass_numbers != {1, 2}:
        errors.append(
            f"{name}: pass numbers are not exactly {{1, 2}}"
        )

    return errors, missing_raw


def _validate_manifest(
    *,
    market_dir: Path,
    registry_row: dict[str, Any],
    expected_f19b_contract_sha256: str,
    expected_f19c_contract_sha256: str,
) -> tuple[list[str], bool]:
    errors: list[str] = []
    missing_raw = False

    manifest_path = market_dir / "manifest.json"

    if not manifest_path.is_file():
        return ["manifest.json missing"], False

    try:
        manifest = json.loads(
            manifest_path.read_text(encoding="utf-8")
        )
    except (OSError, json.JSONDecodeError) as exc:
        return [
            f"manifest unreadable/invalid: {exc}"
        ], False

    if not isinstance(manifest, dict):
        return ["manifest is not an object"], False

    expected_condition = _condition(
        registry_row.get("condition_id")
    )

    study = manifest.get("study")

    if study not in {
        "RN1-F19b",
        "RN1-F19c",
    }:
        errors.append(
            "study is neither RN1-F19b nor RN1-F19c"
        )

    if manifest.get("status") != "PASS":
        errors.append("status is not PASS")

    try:
        manifest_condition = _condition(
            manifest.get("condition_id")
        )
    except ValueError:
        manifest_condition = ""
        errors.append("manifest condition_id missing")

    if (
        manifest_condition
        and manifest_condition != expected_condition
    ):
        errors.append(
            "manifest condition_id differs from registry"
        )

    if market_dir.name.lower() != expected_condition:
        errors.append(
            "directory name differs from registry condition_id"
        )

    if study == "RN1-F19b":

        if (
            manifest.get("contract_sha256")
            != expected_f19b_contract_sha256
        ):
            errors.append(
                "direct F19b contract SHA mismatch"
            )

    elif study == "RN1-F19c":

        if (
            manifest.get("acquisition_method")
            != "frozen_RN1-F19b"
        ):
            errors.append(
                "F19c acquisition_method mismatch"
            )

        if (
            manifest.get("f19b_contract_sha256")
            != expected_f19b_contract_sha256
        ):
            errors.append(
                "F19c parent F19b contract SHA mismatch"
            )

        if (
            manifest.get("f19c_contract_sha256")
            != expected_f19c_contract_sha256
        ):
            errors.append(
                "F19c operations contract SHA mismatch"
            )

    for field in (
        "slug",
        "gamma_market_id",
        "registered_at_utc",
        "game_start_time_utc",
    ):
        if field in registry_row:
            if manifest.get(field) != registry_row.get(field):
                errors.append(
                    f"{field} differs from frozen registry"
                )

    boundary = manifest.get("analysis_boundary")

    if not isinstance(boundary, dict):
        errors.append(
            "analysis_boundary missing or invalid"
        )
    else:
        for key in (
            "signal_calculated",
            "markout_calculated",
            "settlement_used",
            "pnl_calculated",
            "winner_used",
        ):
            if boundary.get(key) is not False:
                errors.append(
                    "analysis boundary does not report "
                    f"{key}=false"
                )

    streams = manifest.get("streams")

    if not isinstance(streams, dict):
        errors.append("streams missing or invalid")
        return errors, missing_raw

    if set(streams) != {"market_taker", "rn1_all"}:
        errors.append(
            "stream set is not exactly "
            "market_taker and rn1_all"
        )

    for name in ("market_taker", "rn1_all"):
        stream_errors, stream_missing = (
            _validate_stream(
                market_dir=market_dir,
                name=name,
                payload=streams.get(name),
            )
        )

        errors.extend(stream_errors)
        missing_raw = missing_raw or stream_missing

    return errors, missing_raw


def scan_readiness(
    repo_root: Path,
    *,
    expected_registry_sha256: str = (
        EXPECTED_REGISTRY_SHA256
    ),
    expected_f19b_contract_sha256: str = (
        EXPECTED_F19B_CONTRACT_SHA256
    ),
    expected_f19b_collector_sha256: str = (
        EXPECTED_F19B_COLLECTOR_SHA256
    ),
    expected_f19c_contract_sha256: str = (
        EXPECTED_F19C_CONTRACT_SHA256
    ),
    required_registry_rows: int = REQUIRED_REGISTRY_ROWS,
) -> ReadinessSnapshot:
    """
    Inspect registry/code identities and manifest metadata only.

    F19 pass_*.json trade contents are never opened.
    F20 is never touched.
    """
    repo_root = Path(repo_root)

    registry_path = (
        repo_root
        / "data"
        / "prospective"
        / "rn1_f19"
        / "registry.jsonl"
    )

    acquisition_root = (
        repo_root
        / "data"
        / "prospective"
        / "rn1_f19"
        / "acquisition"
    )

    f19b_contract_path = (
        repo_root
        / "research"
        / "rn1_f19b_acquisition_contract.json"
    )

    f19b_collector_path = (
        repo_root
        / "collectors"
        / "polymarket"
        / "rn1_f19b_acquire.py"
    )

    errors: list[str] = []

    registry_sha = (
        sha256_file(registry_path)
        if registry_path.is_file()
        else ""
    )

    contract_sha = (
        sha256_file(f19b_contract_path)
        if f19b_contract_path.is_file()
        else ""
    )

    collector_sha = (
        sha256_file(f19b_collector_path)
        if f19b_collector_path.is_file()
        else ""
    )

    if registry_sha != expected_registry_sha256:
        errors.append(
            "frozen F19 registry SHA mismatch"
        )

    if contract_sha != expected_f19b_contract_sha256:
        errors.append(
            "frozen F19b contract SHA mismatch"
        )

    if collector_sha != expected_f19b_collector_sha256:
        errors.append(
            "frozen F19b collector SHA mismatch"
        )

    try:
        registry_rows = _load_registry(
            registry_path
        )
    except ValueError as exc:
        registry_rows = []
        errors.append(str(exc))

    registry_conditions: list[str] = []
    row_by_condition: dict[
        str,
        dict[str, Any],
    ] = {}

    for index, row in enumerate(registry_rows):
        try:
            condition = _condition(
                row.get("condition_id")
            )
        except ValueError:
            errors.append(
                f"registry row {index}: "
                "invalid condition_id"
            )
            continue

        registry_conditions.append(condition)

        if condition in row_by_condition:
            errors.append(
                "duplicate condition_id in registry: "
                f"{condition}"
            )
        else:
            row_by_condition[condition] = row

    unique_conditions = len(
        set(registry_conditions)
    )

    if len(registry_rows) != required_registry_rows:
        errors.append(
            f"registry rows {len(registry_rows)} "
            f"!= {required_registry_rows}"
        )

    if unique_conditions != required_registry_rows:
        errors.append(
            f"unique conditions {unique_conditions} "
            f"!= {required_registry_rows}"
        )

    condition_dirs: dict[str, Path] = {}
    temporary_dirs = 0
    extra_dirs = 0

    if acquisition_root.exists():

        if not acquisition_root.is_dir():
            errors.append(
                "acquisition root is not a directory"
            )

        else:

            for child in acquisition_root.iterdir():

                if child.name.startswith(".tmp_"):
                    temporary_dirs += 1
                    continue

                if not child.is_dir():
                    extra_dirs += 1
                    continue

                name = child.name.lower()

                if name in row_by_condition:

                    if name in condition_dirs:
                        errors.append(
                            "duplicate acquisition directory: "
                            f"{name}"
                        )

                    condition_dirs[name] = child

                elif name.startswith("0x"):
                    condition_dirs[name] = child

                else:
                    extra_dirs += 1

    orphan_names = {
        name
        for name in condition_dirs
        if name not in row_by_condition
    }

    orphan_dirs = len(orphan_names)

    invalid_manifests = 0
    missing_raw_markets = 0
    valid_finalized = 0

    for condition, market_dir in sorted(
        condition_dirs.items()
    ):

        if condition in orphan_names:
            continue

        row = row_by_condition[condition]

        manifest_errors, missing_raw = (
            _validate_manifest(
                market_dir=market_dir,
                registry_row=row,
                expected_f19b_contract_sha256=(
                    expected_f19b_contract_sha256
                ),
                expected_f19c_contract_sha256=(
                    expected_f19c_contract_sha256
                ),
            )
        )

        if missing_raw:
            missing_raw_markets += 1

        if manifest_errors:
            invalid_manifests += 1

            errors.extend(
                f"{condition}: {message}"
                for message in manifest_errors
            )

        else:
            valid_finalized += 1

    if temporary_dirs:
        errors.append(
            f"{temporary_dirs} temporary "
            "acquisition directories present"
        )

    if extra_dirs:
        errors.append(
            f"{extra_dirs} extra acquisition "
            "entries/directories present"
        )

    if orphan_dirs:
        errors.append(
            f"{orphan_dirs} orphan condition "
            "directories present"
        )

    if missing_raw_markets:
        errors.append(
            f"{missing_raw_markets} markets have "
            "missing referenced raw files"
        )

    remaining = max(
        required_registry_rows - valid_finalized,
        0,
    )

    if errors:
        status = FAIL

    elif valid_finalized == required_registry_rows:
        status = READY

    else:
        status = NOT_READY

    return ReadinessSnapshot(
        status=status,
        registry_rows=len(registry_rows),
        unique_conditions=unique_conditions,
        valid_finalized_markets=valid_finalized,
        remaining_markets=remaining,
        invalid_manifests=invalid_manifests,
        markets_with_missing_referenced_raw_files=(
            missing_raw_markets
        ),
        orphan_condition_directories=orphan_dirs,
        temporary_directories=temporary_dirs,
        extra_acquisition_directories=extra_dirs,
        registry_sha256=registry_sha,
        f19b_contract_sha256=contract_sha,
        f19b_collector_sha256=collector_sha,
        integrity_errors=tuple(errors),
    )


def main() -> None:
    repo = Path(__file__).resolve().parents[1]

    snapshot = scan_readiness(repo)

    print(
        "========================================"
    )
    print(
        "RN1-F19d-P3 MANIFEST-ONLY READINESS"
    )
    print(
        "========================================"
    )

    print("status:", snapshot.status)
    print(
        "registry rows:",
        snapshot.registry_rows,
    )
    print(
        "unique conditions:",
        snapshot.unique_conditions,
    )
    print(
        "valid finalized markets:",
        snapshot.valid_finalized_markets,
    )
    print(
        "remaining markets:",
        snapshot.remaining_markets,
    )
    print(
        "invalid manifests:",
        snapshot.invalid_manifests,
    )
    print(
        "markets with missing referenced raw files:",
        snapshot.markets_with_missing_referenced_raw_files,
    )
    print(
        "temporary directories:",
        snapshot.temporary_directories,
    )
    print(
        "extra acquisition directories:",
        snapshot.extra_acquisition_directories,
    )

    if snapshot.integrity_errors:
        print()
        print("integrity errors:")

        for error in snapshot.integrity_errors:
            print(" -", error)

    print()
    print("raw trade content read: NO")
    print("F20 read: NO")
    print("signal calculated: NO")
    print("markout calculated: NO")
    print("PnL calculated: NO")

    if snapshot.status == FAIL:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
