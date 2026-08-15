from __future__ import annotations

import argparse
import fcntl
import gzip
import hashlib
import json
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

from dotenv import dotenv_values


REPO = Path(__file__).resolve().parents[1]

CONTRACT_PATH = (
    REPO
    / "research"
    / "rn1_f20_m3_archive_contract.json"
)

ROOT = (
    REPO
    / "data"
    / "prospective"
    / "rn1_f19"
    / "clob_f20_m1"
)

RAW = ROOT / "raw"
NORMALIZED = ROOT / "normalized"

RUN_ROOT = (
    ROOT
    / "archive"
    / "runs"
)

BACKUP_DIR = (
    REPO
    / "backups"
    / "f20"
)

ENV = dotenv_values(
    REPO / ".env"
)

CONNECTION_STRING = ENV.get(
    "AZURE_STORAGE_CONNECTION_STRING"
)

CONTAINER = "quantlab-backups"
PREFIX = "rn1_f20_m1"

RESERVE_BYTES = 4 * 1024**3

DAY_RE = re.compile(
    r"^\d{8}$"
)


def utcnow():
    return datetime.now(timezone.utc)


def sha256_file(path: Path) -> str:

    h = hashlib.sha256()

    with path.open("rb") as fh:

        while True:
            chunk = fh.read(
                1024 * 1024
            )

            if not chunk:
                break

            h.update(chunk)

    return h.hexdigest()


def tree_bytes(path: Path) -> int:

    return sum(
        p.stat().st_size
        for p in path.rglob("*")
        if p.is_file()
    )


def az_base() -> list[str]:

    if not CONNECTION_STRING:
        raise RuntimeError(
            "AZURE_STORAGE_CONNECTION_STRING "
            "missing from .env"
        )

    return [
        "az",
        "storage",
        "blob",
    ]


def blob_exists(blob: str) -> bool:

    command = (
        az_base()
        + [
            "exists",
            "--connection-string",
            CONNECTION_STRING,
            "--container-name",
            CONTAINER,
            "--name",
            blob,
            "--query",
            "exists",
            "-o",
            "tsv",
            "--only-show-errors",
        ]
    )

    value = subprocess.check_output(
        command,
        text=True,
    ).strip()

    return value.lower() == "true"


def blob_size(blob: str) -> int:

    command = (
        az_base()
        + [
            "show",
            "--connection-string",
            CONNECTION_STRING,
            "--container-name",
            CONTAINER,
            "--name",
            blob,
            "--query",
            "properties.contentLength",
            "-o",
            "tsv",
            "--only-show-errors",
        ]
    )

    return int(
        subprocess.check_output(
            command,
            text=True,
        ).strip()
    )


def upload(
    path: Path,
    blob: str,
) -> None:

    command = (
        az_base()
        + [
            "upload",
            "--connection-string",
            CONNECTION_STRING,
            "--container-name",
            CONTAINER,
            "--name",
            blob,
            "--file",
            str(path),
            "--overwrite",
            "false",
            "--no-progress",
            "--only-show-errors",
            "-o",
            "none",
        ]
    )

    subprocess.run(
        command,
        check=True,
    )


def remote_stream(blob: str):

    # Azure CLI supports binary blob download to stdout
    # when --file is omitted. Do NOT use /dev/stdout as
    # --file because the CLI file-download path requires
    # a seekable destination handle.
    command = (
        az_base()
        + [
            "download",
            "--connection-string",
            CONNECTION_STRING,
            "--container-name",
            CONTAINER,
            "--name",
            blob,
            "--no-progress",
            "--only-show-errors",
        ]
    )

    return subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def remote_sha256(
    blob: str,
) -> str:

    proc = remote_stream(blob)

    if proc.stdout is None:
        raise RuntimeError(
            "Azure stdout pipe unavailable"
        )

    h = hashlib.sha256()

    while True:

        chunk = proc.stdout.read(
            1024 * 1024
        )

        if not chunk:
            break

        h.update(chunk)

    stderr = (
        proc.stderr.read()
        if proc.stderr is not None
        else b""
    )

    rc = proc.wait()

    if rc != 0:
        raise RuntimeError(
            "remote SHA download failed: "
            + stderr.decode(
                "utf-8",
                errors="replace",
            )[:500]
        )

    return h.hexdigest()


def local_members(
    archive: Path,
) -> list[str]:

    members = []

    with tarfile.open(
        archive,
        "r:gz",
    ) as tf:

        for member in tf:

            name = member.name

            if (
                name.startswith("/")
                or ".." in Path(name).parts
            ):
                raise RuntimeError(
                    "unsafe archive member"
                )

            members.append(name)

    if not members:
        raise RuntimeError(
            "empty archive"
        )

    return sorted(members)


def remote_members(
    blob: str,
) -> list[str]:

    proc = remote_stream(blob)

    if proc.stdout is None:
        raise RuntimeError(
            "Azure stdout pipe unavailable"
        )

    members = []

    try:

        with tarfile.open(
            fileobj=proc.stdout,
            mode="r|gz",
        ) as tf:

            for member in tf:

                name = member.name

                if (
                    name.startswith("/")
                    or ".." in Path(name).parts
                ):
                    raise RuntimeError(
                        "unsafe remote archive member"
                    )

                members.append(name)

    finally:

        try:
            proc.stdout.close()
        except Exception:
            pass

    stderr = (
        proc.stderr.read()
        if proc.stderr is not None
        else b""
    )

    rc = proc.wait()

    if rc != 0:
        raise RuntimeError(
            "remote tar verification failed: "
            + stderr.decode(
                "utf-8",
                errors="replace",
            )[:500]
        )

    if not members:
        raise RuntimeError(
            "remote archive empty"
        )

    return sorted(members)


def create_archive(
    day: str,
    output: Path,
) -> None:

    raw_rel = (
        f"raw/{day}"
    )

    norm_rel = (
        f"normalized/{day}"
    )

    # Deterministic tar metadata + deterministic
    # gzip header, making reruns byte-identical.
    command = (
        "set -o pipefail; "
        "tar "
        "--sort=name "
        "--mtime='@0' "
        "--owner=0 "
        "--group=0 "
        "--numeric-owner "
        f"-C '{ROOT}' "
        "-cf - "
        f"'{raw_rel}' "
        f"'{norm_rel}' "
        "| gzip -n -3 "
        f"> '{output}'"
    )

    subprocess.run(
        [
            "bash",
            "-lc",
            command,
        ],
        check=True,
    )


def valid_day_dirs(
    root: Path,
) -> set[str]:

    if not root.exists():
        return set()

    return {
        p.name
        for p in root.iterdir()
        if (
            p.is_dir()
            and DAY_RE.fullmatch(
                p.name
            )
        )
    }


def eligible_days() -> list[str]:

    today = utcnow().strftime(
        "%Y%m%d"
    )

    raw_days = valid_day_dirs(
        RAW
    )

    normalized_days = valid_day_dirs(
        NORMALIZED
    )

    all_days = sorted(
        raw_days
        | normalized_days
    )

    eligible = []

    for day in all_days:

        if day >= today:
            continue

        if (
            day not in raw_days
            or day not in normalized_days
        ):
            raise RuntimeError(
                f"incomplete F20 day {day}: "
                f"raw={day in raw_days} "
                f"normalized="
                f"{day in normalized_days}"
            )

        eligible.append(day)

    return eligible


def verify_remote(
    archive: Path,
    blob: str,
) -> dict:

    local_size = (
        archive.stat().st_size
    )

    local_sha = sha256_file(
        archive
    )

    local_list = local_members(
        archive
    )

    if not blob_exists(blob):
        raise RuntimeError(
            f"remote blob missing: {blob}"
        )

    remote_size_value = (
        blob_size(blob)
    )

    if remote_size_value != local_size:
        raise RuntimeError(
            "remote/local size mismatch: "
            f"local={local_size} "
            f"remote={remote_size_value}"
        )

    remote_sha = (
        remote_sha256(blob)
    )

    if remote_sha != local_sha:
        raise RuntimeError(
            "remote/local SHA256 mismatch"
        )

    remote_list = (
        remote_members(blob)
    )

    if remote_list != local_list:
        raise RuntimeError(
            "remote/local tar member "
            "listing mismatch"
        )

    return {
        "local_bytes":
            local_size,

        "remote_bytes":
            remote_size_value,

        "sha256":
            local_sha,

        "member_count":
            len(local_list),

        "remote_tar_integrity":
            True,

        "remote_member_match":
            True,
    }


def archive_day(day: str) -> None:

    today = utcnow().strftime(
        "%Y%m%d"
    )

    if day >= today:
        raise RuntimeError(
            "refusing to archive current "
            "or future UTC day"
        )

    raw_dir = RAW / day
    norm_dir = NORMALIZED / day

    if (
        not raw_dir.is_dir()
        or not norm_dir.is_dir()
    ):
        raise RuntimeError(
            f"F20 day incomplete: {day}"
        )

    source_bytes = (
        tree_bytes(raw_dir)
        + tree_bytes(norm_dir)
    )

    free = shutil.disk_usage(
        REPO
    ).free

    if (
        free - source_bytes
        < RESERVE_BYTES
    ):
        raise RuntimeError(
            "insufficient free disk for "
            f"safe archive build: "
            f"free={free} "
            f"source={source_bytes} "
            f"reserve={RESERVE_BYTES}"
        )

    BACKUP_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    RUN_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    blob = (
        f"{PREFIX}/"
        f"clob_{day}.tar.gz"
    )

    tmp = (
        BACKUP_DIR
        / f"clob_{day}.tar.gz.part"
    )

    if tmp.exists():
        tmp.unlink()

    receipt = (
        RUN_ROOT
        / f"archive_{day}.json"
    )

    print()
    print(
        "ARCHIVE DAY:",
        day,
    )

    create_archive(
        day,
        tmp,
    )

    # Local tar must be readable before upload.
    local_members(tmp)

    print(
        "local archive bytes:",
        tmp.stat().st_size,
    )

    print(
        "local SHA256:",
        sha256_file(tmp),
    )

    if blob_exists(blob):

        print(
            "remote blob already exists; "
            "will verify, not overwrite"
        )

    else:

        print(
            "uploading:",
            blob,
        )

        upload(
            tmp,
            blob,
        )

    verification = (
        verify_remote(
            tmp,
            blob,
        )
    )

    pre_delete = {
        "study":
            "RN1-F20-M3",

        "day":
            day,

        "status":
            "REMOTE_VERIFIED",

        "blob":
            blob,

        "source_bytes":
            source_bytes,

        "verification":
            verification,

        "local_deleted":
            False,

        "analysis_boundary": {
            "performance_read":
                False,

            "prices_inspected":
                False,

            "f18_signal_calculated":
                False,

            "markout_calculated":
                False,

            "winner_inspected":
                False,

            "settlement_used":
                False,

            "pnl_calculated":
                False,
        },
    }

    receipt.write_text(
        json.dumps(
            pre_delete,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    # Only now may local source data be removed.
    shutil.rmtree(
        raw_dir
    )

    shutil.rmtree(
        norm_dir
    )

    tmp.unlink()

    final = dict(
        pre_delete
    )

    final["status"] = "PASS"
    final["local_deleted"] = True

    receipt.write_text(
        json.dumps(
            final,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    print(
        "REMOTE VERIFIED: PASS"
    )

    print(
        "LOCAL SOURCE DELETE: PASS"
    )


def delete_blob(
    blob: str,
) -> None:

    subprocess.run(
        az_base()
        + [
            "delete",
            "--connection-string",
            CONNECTION_STRING,
            "--container-name",
            CONTAINER,
            "--name",
            blob,
            "--only-show-errors",
            "-o",
            "none",
        ],
        check=True,
    )


def self_test() -> None:

    print()
    print(
        "========================================"
    )
    print(
        "F20-M3 AZURE SELF-TEST"
    )
    print(
        "========================================"
    )

    with tempfile.TemporaryDirectory(
        prefix="f20_m3_"
    ) as td:

        root = Path(td)

        payload_dir = (
            root
            / "fixture"
        )

        payload_dir.mkdir()

        (
            payload_dir
            / "hello.txt"
        ).write_text(
            "RN1-F20-M3 archival self-test\n",
            encoding="utf-8",
        )

        archive = (
            root
            / "fixture.tar.gz"
        )

        with archive.open(
            "wb"
        ) as out:

            with gzip.GzipFile(
                fileobj=out,
                mode="wb",
                compresslevel=3,
                mtime=0,
            ) as gz:

                with tarfile.open(
                    fileobj=gz,
                    mode="w",
                ) as tf:

                    tf.add(
                        payload_dir,
                        arcname="fixture",
                    )

        local_members(
            archive
        )

        blob = (
            f"{PREFIX}/_selftest/"
            f"{uuid.uuid4().hex}.tar.gz"
        )

        try:

            upload(
                archive,
                blob,
            )

            v = verify_remote(
                archive,
                blob,
            )

            print(
                "remote bytes:",
                v["remote_bytes"],
            )

            print(
                "SHA256 match: PASS"
            )

            print(
                "remote tar integrity: PASS"
            )

            print(
                "member listing match: PASS"
            )

        finally:

            if blob_exists(blob):
                delete_blob(blob)

    print(
        "self-test blob cleanup: PASS"
    )

    print(
        "performance read: NO"
    )


def main() -> None:

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--dry-run",
        action="store_true",
    )

    parser.add_argument(
        "--self-test",
        action="store_true",
    )

    parser.add_argument(
        "--day",
        default=None,
    )

    args = parser.parse_args()

    contract = json.loads(
        CONTRACT_PATH.read_text(
            encoding="utf-8"
        )
    )

    if (
        contract["azure"]["container"]
        != CONTAINER
    ):
        raise RuntimeError(
            "contract/container mismatch"
        )

    lock_path = (
        "/tmp/rn1-f20-m3-archive.lock"
    )

    with open(
        lock_path,
        "w",
        encoding="utf-8",
    ) as lock:

        try:

            fcntl.flock(
                lock,
                fcntl.LOCK_EX
                | fcntl.LOCK_NB,
            )

        except BlockingIOError:

            print(
                "F20-M3 archive already running"
            )

            return

        if args.self_test:

            self_test()
            return

        days = eligible_days()

        if args.day is not None:

            if not DAY_RE.fullmatch(
                args.day
            ):
                raise RuntimeError(
                    "--day must be YYYYMMDD"
                )

            today = utcnow().strftime(
                "%Y%m%d"
            )

            if args.day >= today:
                raise RuntimeError(
                    "refusing current/future UTC day"
                )

            if args.day not in days:
                raise RuntimeError(
                    f"day not eligible: {args.day}"
                )

            days = [
                args.day
            ]

        print()
        print(
            "========================================"
        )
        print(
            "F20-M3 ARCHIVAL"
        )
        print(
            "========================================"
        )

        print(
            "current UTC day:",
            utcnow().strftime(
                "%Y%m%d"
            ),
        )

        print(
            "eligible completed days:",
            len(days),
        )

        for day in days:
            print(
                "ELIGIBLE",
                day,
            )

        if args.dry_run:

            print(
                "DRY RUN — no upload "
                "or deletion performed"
            )

            return

        for day in days:
            archive_day(day)

        print()
        print(
            "archived days:",
            len(days),
        )

        print(
            "performance read: NO"
        )


if __name__ == "__main__":
    main()
