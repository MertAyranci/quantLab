from __future__ import annotations

import gzip
import hashlib
import json
import subprocess
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx


REPO = Path(__file__).resolve().parents[2]

CONTRACT = (
    REPO
    / "research"
    / "rn1_f20_m2_health_contract.json"
)

M1_CONTRACT = (
    REPO
    / "research"
    / "rn1_f20_m1_clob_capture_contract.json"
)

REGISTRY = (
    REPO
    / "data"
    / "prospective"
    / "rn1_f19"
    / "registry.jsonl"
)

ROOT = (
    REPO
    / "data"
    / "prospective"
    / "rn1_f19"
    / "clob_f20_m1"
)

RUN_ROOT = (
    ROOT
    / "health"
    / "runs"
)

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"

UA = {
    "User-Agent":
        "quant-lab-rn1-f20-m2-health/1.0"
}


def sha256(path: Path) -> str:
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(
        value.replace(
            "Z",
            "+00:00",
        )
    )


def service_active() -> bool:
    result = subprocess.run(
        [
            "systemctl",
            "is-active",
            "--quiet",
            "rn1-f20-m1-clob.service",
        ],
        check=False,
    )

    return result.returncode == 0


def service_main_pid() -> int | None:
    result = subprocess.run(
        [
            "systemctl",
            "show",
            "rn1-f20-m1-clob.service",
            "--property=MainPID",
            "--value",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        return None

    try:
        pid = int(result.stdout.strip())
    except ValueError:
        return None

    return pid if pid > 0 else None


def open_gzip_paths_for_pid(
    pid: int,
    *,
    proc_root: Path = Path("/proc"),
    root: Path | None = None,
) -> set[Path]:
    """
    Return F20 gzip paths actually held open by the collector process.

    A MinuteGzipWriter rotates only when that stream next writes.
    Therefore a previous wall-clock minute can legitimately remain
    open during stream-specific silence.
    """
    root = (root or ROOT).resolve(strict=False)
    fd_root = proc_root / str(pid) / "fd"

    try:
        descriptors = list(fd_root.iterdir())
    except OSError:
        return set()

    selected: set[Path] = set()

    for fd in descriptors:
        try:
            target_text = str(fd.readlink())
        except OSError:
            continue

        if target_text.endswith(" (deleted)"):
            target_text = target_text.removesuffix(
                " (deleted)"
            )

        target = Path(target_text)

        if not target.is_absolute():
            continue

        target = target.resolve(strict=False)

        if not target.name.endswith(".jsonl.gz"):
            continue

        if root not in target.parents:
            continue

        selected.add(target)

    return selected


def live_open_gzip_paths() -> set[Path]:
    pid = service_main_pid()

    if pid is None:
        return set()

    return open_gzip_paths_for_pid(pid)


def completed_recent_files(
    *,
    now: datetime,
    horizon_seconds: int,
    open_paths: set[Path] | None = None,
) -> list[Path]:

    # Never read the gzip file for the minute that
    # the live collector is currently writing.
    current_minute = now.replace(
        second=0,
        microsecond=0,
    )

    earliest = (
        current_minute
        - timedelta(
            seconds=horizon_seconds + 120
        )
    )

    selected = []

    open_resolved = {
        path.resolve(strict=False)
        for path in (open_paths or set())
    }

    for path in (
        ROOT
        / "normalized"
    ).glob("*/*.jsonl.gz"):

        if path.resolve(strict=False) in open_resolved:
            continue

        name = path.name.removesuffix(
            ".jsonl.gz"
        )

        try:
            minute = datetime.strptime(
                name,
                "%Y%m%d_%H%M",
            ).replace(
                tzinfo=timezone.utc
            )

        except ValueError:
            continue

        if (
            earliest
            <= minute
            < current_minute
        ):
            selected.append(path)

    return sorted(selected)


def main() -> None:

    now = utcnow()

    contract = json.loads(
        CONTRACT.read_text(
            encoding="utf-8"
        )
    )

    m1 = json.loads(
        M1_CONTRACT.read_text(
            encoding="utf-8"
        )
    )

    if (
        sha256(M1_CONTRACT)
        !=
        contract[
            "m1_contract_sha256"
        ]
    ):
        raise RuntimeError(
            "F20-M1 contract changed "
            "after M2 health freeze"
        )

    if (
        sha256(REGISTRY)
        !=
        contract[
            "registry_sha256"
        ]
    ):
        raise RuntimeError(
            "F19 registry changed "
            "after M2 health freeze"
        )

    horizon = int(
        contract[
            "recent_capture_seconds"
        ]
    )

    window = m1[
        "capture"
    ]["subscription_window"]

    before = timedelta(
        seconds=int(
            window[
                "start_seconds_before_registered_game_start"
            ]
        )
    )

    after = timedelta(
        seconds=int(
            window[
                "end_seconds_after_registered_game_start"
            ]
        )
    )

    active_markets = []

    for market in m1[
        "capture_universe"
    ]["markets"]:

        start = parse_dt(
            market[
                "game_start_time_utc"
            ]
        )

        if (
            start - before
            <= now
            <= start + after
        ):
            active_markets.append(
                (
                    market,
                    start,
                )
            )

    cutoff = (
        now
        - timedelta(
            seconds=horizon
        )
    )

    recent_tokens = set()

    open_gzip_paths = live_open_gzip_paths()

    files = completed_recent_files(
        now=now,
        horizon_seconds=horizon,
        open_paths=open_gzip_paths,
    )

    for path in files:

        with gzip.open(
            path,
            "rt",
            encoding="utf-8",
        ) as fh:

            for line in fh:

                row = json.loads(line)

                token = row.get("token")

                if not token:
                    continue

                captured = parse_dt(
                    row["capture_time"]
                )

                if captured >= cutoff:
                    recent_tokens.add(
                        str(token)
                    )

    counts = Counter()

    details = []

    svc_active = service_active()

    with httpx.Client(
        timeout=20,
        headers=UA,
    ) as client:

        for market, start in active_markets:

            tokens = [
                str(x)
                for x in market[
                    "token_ids"
                ]
            ]

            missing = [
                token
                for token in tokens
                if token
                not in recent_tokens
            ]

            if not missing:

                counts[
                    "HEALTHY_RECENT"
                ] += len(tokens)

                continue

            try:
                gamma_response = (
                    client.get(
                        f"{GAMMA}/markets/"
                        f"{market['gamma_market_id']}"
                    )
                )

            except httpx.HTTPError:

                counts[
                    "STATUS_LOOKUP_FAILURE"
                ] += len(missing)

                continue

            if gamma_response.status_code != 200:

                counts[
                    "STATUS_LOOKUP_FAILURE"
                ] += len(missing)

                continue

            gamma = (
                gamma_response.json()
            )

            closed = (
                gamma.get("closed")
                is True
            )

            accepting = gamma.get(
                "acceptingOrders"
            )

            book_statuses = []

            lookup_failed = False

            for token in missing:

                try:
                    response = client.get(
                        f"{CLOB}/book",
                        params={
                            "token_id":
                                token
                        },
                    )

                    book_statuses.append(
                        response.status_code
                    )

                except httpx.HTTPError:

                    lookup_failed = True
                    break

            if lookup_failed:

                counts[
                    "STATUS_LOOKUP_FAILURE"
                ] += len(missing)

                continue

            all_404 = (
                bool(book_statuses)
                and
                all(
                    status == 404
                    for status
                    in book_statuses
                )
            )

            any_200 = any(
                status == 200
                for status
                in book_statuses
            )

            if (
                closed
                and all_404
            ):

                classification = (
                    "EXPECTED_CLOSED_SILENCE"
                )

            elif (
                now < start
                and not closed
                and accepting is False
                and all_404
            ):

                classification = (
                    "EXPECTED_PREOPEN_SILENCE"
                )

            elif any_200:

                classification = (
                    "CAPTURE_GAP"
                )

            else:

                classification = (
                    "UNEXPLAINED_VENUE_UNAVAILABLE"
                )

            counts[
                classification
            ] += len(missing)

            details.append(
                {
                    "condition_id":
                        market[
                            "condition_id"
                        ],

                    "slug":
                        market["slug"],

                    "missing_token_count":
                        len(missing),

                    "classification":
                        classification,

                    "gamma_closed":
                        closed,

                    "gamma_accepting_orders":
                        accepting,

                    "clob_http_statuses":
                        book_statuses,
                }
            )

    total_active_tokens = sum(
        len(
            market["token_ids"]
        )
        for market, _
        in active_markets
    )

    problematic = (
        counts[
            "CAPTURE_GAP"
        ]
        +
        counts[
            "UNEXPLAINED_VENUE_UNAVAILABLE"
        ]
        +
        counts[
            "STATUS_LOOKUP_FAILURE"
        ]
    )

    status = (
        "PASS"
        if (
            svc_active
            and problematic == 0
        )
        else
        "FAIL"
    )

    RUN_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    receipt = (
        RUN_ROOT
        / (
            "health_"
            + now.strftime(
                "%Y%m%dT%H%M%S.%fZ"
            )
            + ".json"
        )
    )

    payload = {
        "study":
            "RN1-F20-M2",

        "status":
            status,

        "captured_at_utc":
            now.isoformat(),

        "service_active":
            svc_active,

        "active_markets":
            len(active_markets),

        "active_tokens":
            total_active_tokens,

        "recent_tokens":
            len(
                recent_tokens
            ),

        "classifications":
            dict(counts),

        "details":
            details,

        "analysis_boundary": {
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
            payload,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    print()
    print(
        "========================================"
    )
    print(
        "F20-M2 VENUE-AWARE HEALTH"
    )
    print(
        "========================================"
    )

    print("status:", status)
    print(
        "service active:",
        svc_active,
    )
    print(
        "active markets:",
        len(active_markets),
    )
    print(
        "active tokens:",
        total_active_tokens,
    )
    print(
        "recent structural tokens:",
        len(
            recent_tokens
        ),
    )

    print()
    print("CLASSIFICATIONS")

    for name in (
        "HEALTHY_RECENT",
        "EXPECTED_CLOSED_SILENCE",
        "EXPECTED_PREOPEN_SILENCE",
        "CAPTURE_GAP",
        "UNEXPLAINED_VENUE_UNAVAILABLE",
        "STATUS_LOOKUP_FAILURE",
    ):
        print(
            name,
            counts[name],
        )

    print()
    print(
        "performance read: NO"
    )
    print(
        "receipt:",
        receipt,
    )

    if status != "PASS":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
