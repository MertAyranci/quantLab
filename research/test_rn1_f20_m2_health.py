from datetime import datetime, timezone
from pathlib import Path

from collectors.polymarket import rn1_f20_m2_health as h


def make_gzip_placeholder(root: Path, minute: str) -> Path:
    day = minute[:8]
    path = (
        root
        / "normalized"
        / day
        / f"{minute}.jsonl.gz"
    )
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    path.write_bytes(b"placeholder")
    return path


def test_previous_minute_open_file_is_skipped(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(
        h,
        "ROOT",
        tmp_path,
    )

    now = datetime(
        2026, 8, 19,
        0, 27, 30,
        tzinfo=timezone.utc,
    )

    closed_previous = make_gzip_placeholder(
        tmp_path,
        "20260819_0025",
    )

    open_previous = make_gzip_placeholder(
        tmp_path,
        "20260819_0026",
    )

    selected = h.completed_recent_files(
        now=now,
        horizon_seconds=120,
        open_paths={open_previous},
    )

    assert closed_previous in selected
    assert open_previous not in selected


def test_closed_previous_minute_is_readable_candidate(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(
        h,
        "ROOT",
        tmp_path,
    )

    now = datetime(
        2026, 8, 19,
        0, 27, 30,
        tzinfo=timezone.utc,
    )

    previous = make_gzip_placeholder(
        tmp_path,
        "20260819_0026",
    )

    selected = h.completed_recent_files(
        now=now,
        horizon_seconds=120,
        open_paths=set(),
    )

    assert previous in selected


def test_current_minute_remains_excluded(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(
        h,
        "ROOT",
        tmp_path,
    )

    now = datetime(
        2026, 8, 19,
        0, 27, 30,
        tzinfo=timezone.utc,
    )

    current = make_gzip_placeholder(
        tmp_path,
        "20260819_0027",
    )

    selected = h.completed_recent_files(
        now=now,
        horizon_seconds=120,
        open_paths=set(),
    )

    assert current not in selected


def test_proc_fd_helper_finds_only_f20_gzip(
    tmp_path,
):
    root = tmp_path / "f20"
    gzip_path = (
        root
        / "normalized"
        / "20260819"
        / "20260819_0026.jsonl.gz"
    )

    gzip_path.parent.mkdir(
        parents=True,
    )
    gzip_path.write_bytes(b"x")

    unrelated = tmp_path / "other.txt"
    unrelated.write_text("x")

    proc = tmp_path / "proc"
    fd_root = proc / "1234" / "fd"
    fd_root.mkdir(parents=True)

    (fd_root / "7").symlink_to(
        gzip_path
    )
    (fd_root / "8").symlink_to(
        unrelated
    )

    result = h.open_gzip_paths_for_pid(
        1234,
        proc_root=proc,
        root=root,
    )

    assert result == {
        gzip_path.resolve()
    }
