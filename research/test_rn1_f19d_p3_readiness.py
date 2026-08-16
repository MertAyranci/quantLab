from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(
    0,
    str(Path(__file__).resolve().parent),
)

from rn1_f19d_p3_readiness import (
    FAIL,
    NOT_READY,
    READY,
    scan_readiness,
)


def sha(path: Path) -> str:
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def condition(i: int) -> str:
    return "0x" + f"{i:064x}"


def registry_row(i: int) -> dict:
    return {
        "condition_id": condition(i),
        "slug": f"mlb-test-{i}",
        "gamma_market_id": str(1000 + i),
        "registered_at_utc":
            "2026-08-15T00:00:00+00:00",
        "game_start_time_utc":
            f"2026-08-{15+i:02d}T01:00:00+00:00",
    }


def write_registry(
    root: Path,
    rows: list[dict],
) -> Path:
    path = (
        root
        / "data"
        / "prospective"
        / "rn1_f19"
        / "registry.jsonl"
    )

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    path.write_text(
        "".join(
            json.dumps(
                row,
                sort_keys=True,
            )
            + "\n"
            for row in rows
        ),
        encoding="utf-8",
    )

    return path


def write_parent_files(
    root: Path,
) -> tuple[Path, Path]:
    contract = (
        root
        / "research"
        / "rn1_f19b_acquisition_contract.json"
    )

    collector = (
        root
        / "collectors"
        / "polymarket"
        / "rn1_f19b_acquire.py"
    )

    contract.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    collector.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    contract.write_text(
        '{"synthetic": true}\n',
        encoding="utf-8",
    )

    collector.write_text(
        "# synthetic collector\n",
        encoding="utf-8",
    )

    return contract, collector


def make_stream(
    market_dir: Path,
    name: str,
) -> dict:
    stream_sha = (
        "a" * 64
        if name == "market_taker"
        else "b" * 64
    )

    stream_dir = market_dir / name

    stream_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    passes = []

    for n in (1, 2):
        path = (
            stream_dir
            / f"pass_{n}.json"
        )

        # Deliberately invalid JSON.
        # Readiness must only check existence.
        path.write_bytes(
            b"\xff\x00NOT JSON "
            b"AND MUST NOT BE OPENED"
        )

        passes.append(
            {
                "pass": n,
                "row_count": 7,
                "page_count": 1,
                "canonical_core_sha256":
                    stream_sha,
                "path":
                    f"{name}/pass_{n}.json",
            }
        )

    return {
        "stream": name,
        "status": "PASS",
        "row_count": 7,
        "canonical_core_sha256":
            stream_sha,
        "passes": passes,
    }


def write_manifest(
    root: Path,
    row: dict,
    contract_sha: str,
) -> Path:
    market_dir = (
        root
        / "data"
        / "prospective"
        / "rn1_f19"
        / "acquisition"
        / row["condition_id"]
    )

    market_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    payload = {
        "study": "RN1-F19b",
        "status": "PASS",
        "condition_id":
            row["condition_id"],
        "slug":
            row["slug"],
        "gamma_market_id":
            row["gamma_market_id"],
        "registered_at_utc":
            row["registered_at_utc"],
        "game_start_time_utc":
            row["game_start_time_utc"],
        "contract_sha256":
            contract_sha,
        "streams": {
            "market_taker":
                make_stream(
                    market_dir,
                    "market_taker",
                ),
            "rn1_all":
                make_stream(
                    market_dir,
                    "rn1_all",
                ),
        },
        "analysis_boundary": {
            "signal_calculated": False,
            "markout_calculated": False,
            "settlement_used": False,
            "pnl_calculated": False,
            "winner_used": False,
        },
    }

    path = market_dir / "manifest.json"

    path.write_text(
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    return path


def setup_repo(
    tmp_path: Path,
    *,
    finalized: int = 0,
):
    rows = [
        registry_row(1),
        registry_row(2),
    ]

    registry = write_registry(
        tmp_path,
        rows,
    )

    contract, collector = (
        write_parent_files(tmp_path)
    )

    for row in rows[:finalized]:
        write_manifest(
            tmp_path,
            row,
            sha(contract),
        )

    kwargs = {
        "expected_registry_sha256":
            sha(registry),
        "expected_f19b_contract_sha256":
            sha(contract),
        "expected_f19b_collector_sha256":
            sha(collector),
        "required_registry_rows":
            2,
    }

    return (
        rows,
        registry,
        contract,
        collector,
        kwargs,
    )


def test_zero_finalized_is_clean_not_ready(
    tmp_path,
):
    _, _, _, _, kwargs = setup_repo(
        tmp_path,
        finalized=0,
    )

    snap = scan_readiness(
        tmp_path,
        **kwargs,
    )

    assert snap.status == NOT_READY
    assert snap.valid_finalized_markets == 0
    assert snap.remaining_markets == 2
    assert snap.integrity_errors == ()


def test_one_finalized_is_clean_not_ready(
    tmp_path,
):
    _, _, _, _, kwargs = setup_repo(
        tmp_path,
        finalized=1,
    )

    snap = scan_readiness(
        tmp_path,
        **kwargs,
    )

    assert snap.status == NOT_READY
    assert snap.valid_finalized_markets == 1
    assert snap.remaining_markets == 1


def test_all_finalized_is_ready(tmp_path):
    _, _, _, _, kwargs = setup_repo(
        tmp_path,
        finalized=2,
    )

    snap = scan_readiness(
        tmp_path,
        **kwargs,
    )

    assert snap.status == READY
    assert snap.valid_finalized_markets == 2
    assert snap.remaining_markets == 0


def test_raw_pass_content_is_not_parsed(
    tmp_path,
):
    _, _, _, _, kwargs = setup_repo(
        tmp_path,
        finalized=2,
    )

    snap = scan_readiness(
        tmp_path,
        **kwargs,
    )

    assert snap.status == READY
    assert (
        snap.f19_trade_content_read
        is False
    )


def test_missing_referenced_raw_file_fails(
    tmp_path,
):
    rows, _, _, _, kwargs = setup_repo(
        tmp_path,
        finalized=2,
    )

    victim = (
        tmp_path
        / "data"
        / "prospective"
        / "rn1_f19"
        / "acquisition"
        / rows[0]["condition_id"]
        / "market_taker"
        / "pass_1.json"
    )

    victim.unlink()

    snap = scan_readiness(
        tmp_path,
        **kwargs,
    )

    assert snap.status == FAIL

    assert (
        snap
        .markets_with_missing_referenced_raw_files
        == 1
    )


def test_bad_manifest_contract_sha_fails(
    tmp_path,
):
    rows, _, _, _, kwargs = setup_repo(
        tmp_path,
        finalized=1,
    )

    manifest = (
        tmp_path
        / "data"
        / "prospective"
        / "rn1_f19"
        / "acquisition"
        / rows[0]["condition_id"]
        / "manifest.json"
    )

    payload = json.loads(
        manifest.read_text()
    )

    payload["contract_sha256"] = (
        "0" * 64
    )

    manifest.write_text(
        json.dumps(payload),
        encoding="utf-8",
    )

    snap = scan_readiness(
        tmp_path,
        **kwargs,
    )

    assert snap.status == FAIL
    assert snap.invalid_manifests == 1


def test_manifest_boundary_true_fails(
    tmp_path,
):
    rows, _, _, _, kwargs = setup_repo(
        tmp_path,
        finalized=1,
    )

    manifest = (
        tmp_path
        / "data"
        / "prospective"
        / "rn1_f19"
        / "acquisition"
        / rows[0]["condition_id"]
        / "manifest.json"
    )

    payload = json.loads(
        manifest.read_text()
    )

    payload[
        "analysis_boundary"
    ][
        "markout_calculated"
    ] = True

    manifest.write_text(
        json.dumps(payload),
        encoding="utf-8",
    )

    snap = scan_readiness(
        tmp_path,
        **kwargs,
    )

    assert snap.status == FAIL


def test_orphan_condition_directory_fails(
    tmp_path,
):
    _, _, _, _, kwargs = setup_repo(
        tmp_path,
        finalized=0,
    )

    orphan = (
        tmp_path
        / "data"
        / "prospective"
        / "rn1_f19"
        / "acquisition"
        / condition(999)
    )

    orphan.mkdir(parents=True)

    snap = scan_readiness(
        tmp_path,
        **kwargs,
    )

    assert snap.status == FAIL

    assert (
        snap.orphan_condition_directories
        == 1
    )


def test_temporary_directory_fails(
    tmp_path,
):
    _, _, _, _, kwargs = setup_repo(
        tmp_path,
        finalized=0,
    )

    temp = (
        tmp_path
        / "data"
        / "prospective"
        / "rn1_f19"
        / "acquisition"
        / ".tmp_synthetic"
    )

    temp.mkdir(parents=True)

    snap = scan_readiness(
        tmp_path,
        **kwargs,
    )

    assert snap.status == FAIL
    assert snap.temporary_directories == 1


def test_extra_file_in_acquisition_root_fails(
    tmp_path,
):
    _, _, _, _, kwargs = setup_repo(
        tmp_path,
        finalized=0,
    )

    root = (
        tmp_path
        / "data"
        / "prospective"
        / "rn1_f19"
        / "acquisition"
    )

    root.mkdir(
        parents=True,
        exist_ok=True,
    )

    (root / "junk.txt").write_text(
        "junk"
    )

    snap = scan_readiness(
        tmp_path,
        **kwargs,
    )

    assert snap.status == FAIL

    assert (
        snap.extra_acquisition_directories
        == 1
    )


def test_registry_hash_mismatch_fails(
    tmp_path,
):
    _, _, _, _, kwargs = setup_repo(
        tmp_path,
        finalized=0,
    )

    kwargs[
        "expected_registry_sha256"
    ] = "0" * 64

    snap = scan_readiness(
        tmp_path,
        **kwargs,
    )

    assert snap.status == FAIL


def test_collector_hash_mismatch_fails(
    tmp_path,
):
    _, _, _, _, kwargs = setup_repo(
        tmp_path,
        finalized=0,
    )

    kwargs[
        "expected_f19b_collector_sha256"
    ] = "0" * 64

    snap = scan_readiness(
        tmp_path,
        **kwargs,
    )

    assert snap.status == FAIL


def test_duplicate_registry_condition_fails(
    tmp_path,
):
    rows = [
        registry_row(1),
        registry_row(1),
    ]

    registry = write_registry(
        tmp_path,
        rows,
    )

    contract, collector = (
        write_parent_files(tmp_path)
    )

    snap = scan_readiness(
        tmp_path,
        expected_registry_sha256=
            sha(registry),
        expected_f19b_contract_sha256=
            sha(contract),
        expected_f19b_collector_sha256=
            sha(collector),
        required_registry_rows=2,
    )

    assert snap.status == FAIL
    assert snap.unique_conditions == 1


def test_stream_pass_hash_mismatch_fails(
    tmp_path,
):
    rows, _, _, _, kwargs = setup_repo(
        tmp_path,
        finalized=1,
    )

    manifest = (
        tmp_path
        / "data"
        / "prospective"
        / "rn1_f19"
        / "acquisition"
        / rows[0]["condition_id"]
        / "manifest.json"
    )

    payload = json.loads(
        manifest.read_text()
    )

    payload[
        "streams"
    ][
        "market_taker"
    ][
        "passes"
    ][0][
        "canonical_core_sha256"
    ] = "c" * 64

    manifest.write_text(
        json.dumps(payload),
        encoding="utf-8",
    )

    snap = scan_readiness(
        tmp_path,
        **kwargs,
    )

    assert snap.status == FAIL


def test_manifest_registry_identity_mismatch_fails(
    tmp_path,
):
    rows, _, _, _, kwargs = setup_repo(
        tmp_path,
        finalized=1,
    )

    manifest = (
        tmp_path
        / "data"
        / "prospective"
        / "rn1_f19"
        / "acquisition"
        / rows[0]["condition_id"]
        / "manifest.json"
    )

    payload = json.loads(
        manifest.read_text()
    )

    payload["slug"] = "wrong-slug"

    manifest.write_text(
        json.dumps(payload),
        encoding="utf-8",
    )

    snap = scan_readiness(
        tmp_path,
        **kwargs,
    )

    assert snap.status == FAIL


def test_manifest_missing_fails(
    tmp_path,
):
    rows, _, _, _, kwargs = setup_repo(
        tmp_path,
        finalized=1,
    )

    manifest = (
        tmp_path
        / "data"
        / "prospective"
        / "rn1_f19"
        / "acquisition"
        / rows[0]["condition_id"]
        / "manifest.json"
    )

    manifest.unlink()

    snap = scan_readiness(
        tmp_path,
        **kwargs,
    )

    assert snap.status == FAIL
    assert snap.invalid_manifests == 1


def test_f19c_wrapper_manifest_is_valid(
    tmp_path,
):
    rows, _, contract, _, kwargs = setup_repo(
        tmp_path,
        finalized=1,
    )

    f19c_sha = "c" * 64

    kwargs[
        "expected_f19c_contract_sha256"
    ] = f19c_sha

    manifest = (
        tmp_path
        / "data"
        / "prospective"
        / "rn1_f19"
        / "acquisition"
        / rows[0]["condition_id"]
        / "manifest.json"
    )

    payload = json.loads(
        manifest.read_text()
    )

    payload["study"] = "RN1-F19c"
    payload.pop(
        "contract_sha256",
        None,
    )

    payload[
        "acquisition_method"
    ] = "frozen_RN1-F19b"

    payload[
        "f19b_contract_sha256"
    ] = sha(contract)

    payload[
        "f19c_contract_sha256"
    ] = f19c_sha

    manifest.write_text(
        json.dumps(payload),
        encoding="utf-8",
    )

    snap = scan_readiness(
        tmp_path,
        **kwargs,
    )

    assert snap.status == NOT_READY
    assert snap.valid_finalized_markets == 1
    assert snap.invalid_manifests == 0


def test_f19c_wrapper_wrong_parent_hash_fails(
    tmp_path,
):
    rows, _, _, _, kwargs = setup_repo(
        tmp_path,
        finalized=1,
    )

    f19c_sha = "c" * 64

    kwargs[
        "expected_f19c_contract_sha256"
    ] = f19c_sha

    manifest = (
        tmp_path
        / "data"
        / "prospective"
        / "rn1_f19"
        / "acquisition"
        / rows[0]["condition_id"]
        / "manifest.json"
    )

    payload = json.loads(
        manifest.read_text()
    )

    payload["study"] = "RN1-F19c"
    payload.pop(
        "contract_sha256",
        None,
    )

    payload[
        "acquisition_method"
    ] = "frozen_RN1-F19b"

    payload[
        "f19b_contract_sha256"
    ] = "0" * 64

    payload[
        "f19c_contract_sha256"
    ] = f19c_sha

    manifest.write_text(
        json.dumps(payload),
        encoding="utf-8",
    )

    snap = scan_readiness(
        tmp_path,
        **kwargs,
    )

    assert snap.status == FAIL
    assert snap.invalid_manifests == 1
