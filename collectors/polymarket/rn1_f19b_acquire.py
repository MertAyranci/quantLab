from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx


REPO = Path(__file__).resolve().parents[2]

REGISTRY = (
    REPO
    / "data"
    / "prospective"
    / "rn1_f19"
    / "registry.jsonl"
)

OUT_ROOT = (
    REPO
    / "data"
    / "prospective"
    / "rn1_f19"
    / "acquisition"
)

CONTRACT = (
    REPO
    / "research"
    / "rn1_f19b_acquisition_contract.json"
)

WALLETS = (
    REPO
    / "config"
    / "research_wallets.json"
)

GAMMA = "https://gamma-api.polymarket.com"
DATA_API = "https://data-api.polymarket.com"

PAGE_LIMIT = 500
MAX_OFFSET = 10_000
WAIT_HOURS = 12

UA = {
    "User-Agent":
        "quant-lab-rn1-f19b-acquisition/1.0"
}

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


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.isoformat()


def parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(
        value.replace("Z", "+00:00")
    )


def sha256_file(path: Path) -> str:
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def git_head() -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO,
        text=True,
    ).strip()


def write_json(
    path: Path,
    payload: Any,
) -> None:

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    path.write_text(
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def load_registry() -> list[dict[str, Any]]:

    if not REGISTRY.exists():
        raise RuntimeError(
            f"missing registry: {REGISTRY}"
        )

    rows = [
        json.loads(line)
        for line in REGISTRY.read_text(
            encoding="utf-8"
        ).splitlines()
        if line.strip()
    ]

    conditions = [
        row["condition_id"]
        for row in rows
    ]

    if len(conditions) != len(set(conditions)):
        raise RuntimeError(
            "duplicate condition_id in F19 registry"
        )

    rows.sort(
        key=lambda r: (
            r["game_start_time_utc"],
            r["condition_id"],
        )
    )

    return rows


def load_rn1() -> str:

    payload = json.loads(
        WALLETS.read_text(
            encoding="utf-8"
        )
    )

    matches = [
        w
        for w in payload["wallets"]
        if str(
            w.get("name")
            or ""
        ).upper()
        == "RN1"
    ]

    if len(matches) != 1:
        raise RuntimeError(
            "expected exactly one RN1 wallet"
        )

    address = str(
        matches[0]["address"]
    ).lower()

    if (
        not address.startswith("0x")
        or len(address) != 42
    ):
        raise RuntimeError(
            "invalid RN1 address"
        )

    return address


def request_json(
    client: httpx.Client,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    attempts: int = 5,
) -> Any:

    last_error = None

    for attempt in range(
        1,
        attempts + 1,
    ):
        try:
            response = client.get(
                url,
                params=params,
            )

            if response.status_code == 200:
                return response.json()

            last_error = (
                f"HTTP {response.status_code}: "
                f"{response.text[:300]}"
            )

        except (
            httpx.HTTPError,
            json.JSONDecodeError,
        ) as exc:
            last_error = str(exc)

        if attempt < attempts:
            time.sleep(
                1.5 * attempt
            )

    raise RuntimeError(
        f"request failed: {url}: {last_error}"
    )


def gamma_state(
    client: httpx.Client,
    row: dict[str, Any],
) -> dict[str, Any]:

    payload = request_json(
        client,
        f"{GAMMA}/markets/"
        f"{row['gamma_market_id']}",
    )

    if not isinstance(
        payload,
        dict,
    ):
        raise RuntimeError(
            "Gamma market response "
            "is not an object"
        )

    gamma_condition = str(
        payload.get("conditionId")
        or ""
    ).lower()

    if gamma_condition != str(
        row["condition_id"]
    ).lower():
        raise RuntimeError(
            "Gamma condition_id mismatch"
        )

    if payload.get("closed") is not True:
        raise RuntimeError(
            "Gamma market is not closed"
        )

    return payload


def canonical_core(
    trade: dict[str, Any],
) -> dict[str, Any]:

    return {
        field: trade.get(field)
        for field in CORE_FIELDS
    }


def fingerprint(
    rows: list[dict[str, Any]],
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

    return hashlib.sha256(
        joined
    ).hexdigest()


def exact_duplicate_count(
    rows: list[dict[str, Any]],
) -> int:

    canonical = [
        json.dumps(
            canonical_core(row),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        for row in rows
    ]

    return (
        len(canonical)
        - len(set(canonical))
    )


def validate_rows(
    rows: list[dict[str, Any]],
    *,
    condition_id: str,
    rn1: str | None,
) -> None:

    expected = condition_id.lower()

    wrong_market = [
        row
        for row in rows
        if str(
            row.get("conditionId")
            or ""
        ).lower()
        != expected
    ]

    if wrong_market:
        raise RuntimeError(
            "Data API returned "
            "wrong-market trade rows"
        )

    if rn1 is not None:

        wrong_wallet = [
            row
            for row in rows
            if str(
                row.get("proxyWallet")
                or ""
            ).lower()
            != rn1
        ]

        if wrong_wallet:
            raise RuntimeError(
                "RN1 stream returned "
                "non-RN1 rows"
            )


def fetch_trade_stream(
    client: httpx.Client,
    *,
    condition_id: str,
    taker_only: bool,
    user: str | None,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
]:

    rows: list[
        dict[str, Any]
    ] = []

    pages: list[
        dict[str, Any]
    ] = []

    offset = 0

    while True:

        params: dict[str, Any] = {
            "market":
                condition_id,

            "takerOnly":
                (
                    "true"
                    if taker_only
                    else "false"
                ),

            "limit":
                PAGE_LIMIT,

            "offset":
                offset,
        }

        if user is not None:
            params["user"] = user

        captured_at = utcnow()

        payload = request_json(
            client,
            f"{DATA_API}/trades",
            params=params,
        )

        if not isinstance(
            payload,
            list,
        ):
            raise RuntimeError(
                "Data API /trades "
                "returned non-list"
            )

        pages.append(
            {
                "captured_at_utc":
                    iso(captured_at),

                "request_params":
                    params,

                "rows":
                    payload,
            }
        )

        rows.extend(
            payload
        )

        if len(payload) < PAGE_LIMIT:
            break

        if offset >= MAX_OFFSET:
            raise RuntimeError(
                "trade stream reached "
                "maximum offset without "
                "a short terminal page"
            )

        next_offset = (
            offset
            + PAGE_LIMIT
        )

        if next_offset > MAX_OFFSET:
            raise RuntimeError(
                "trade stream would exceed "
                "maximum API offset"
            )

        offset = next_offset

    return rows, pages


def acquire_stream_twice(
    client: httpx.Client,
    *,
    condition_id: str,
    name: str,
    taker_only: bool,
    user: str | None,
    tmp_dir: Path,
) -> dict[str, Any]:

    passes = []

    for pass_no in (1, 2):

        rows, pages = (
            fetch_trade_stream(
                client,
                condition_id=condition_id,
                taker_only=taker_only,
                user=user,
            )
        )

        validate_rows(
            rows,
            condition_id=condition_id,
            rn1=user,
        )

        fp = fingerprint(
            rows
        )

        pass_path = (
            tmp_dir
            / name
            / f"pass_{pass_no}.json"
        )

        write_json(
            pass_path,
            {
                "stream":
                    name,

                "pass":
                    pass_no,

                "condition_id":
                    condition_id,

                "row_count":
                    len(rows),

                "page_count":
                    len(pages),

                "canonical_core_sha256":
                    fp,

                "exact_duplicate_core_rows":
                    exact_duplicate_count(
                        rows
                    ),

                "pages":
                    pages,
            },
        )

        passes.append(
            {
                "pass":
                    pass_no,

                "row_count":
                    len(rows),

                "page_count":
                    len(pages),

                "canonical_core_sha256":
                    fp,

                "path":
                    str(
                        pass_path.relative_to(
                            tmp_dir
                        )
                    ),
            }
        )

        time.sleep(0.5)

    p1, p2 = passes

    stable = (
        p1["row_count"]
        == p2["row_count"]
        and
        p1[
            "canonical_core_sha256"
        ]
        ==
        p2[
            "canonical_core_sha256"
        ]
    )

    if not stable:
        raise RuntimeError(
            f"{name} failed "
            "two-pass stability check"
        )

    return {
        "stream":
            name,

        "status":
            "PASS",

        "row_count":
            p1["row_count"],

        "canonical_core_sha256":
            p1[
                "canonical_core_sha256"
            ],

        "passes":
            passes,
    }


def existing_finalized(
    final_dir: Path,
) -> bool:

    manifest = (
        final_dir
        / "manifest.json"
    )

    if not manifest.exists():
        return False

    payload = json.loads(
        manifest.read_text(
            encoding="utf-8"
        )
    )

    return (
        payload.get("status")
        == "PASS"
    )


def eligible_rows(
    rows: list[dict[str, Any]],
    *,
    now: datetime,
) -> list[dict[str, Any]]:

    wait = timedelta(
        hours=WAIT_HOURS
    )

    return [
        row
        for row in rows
        if now >= (
            parse_dt(
                row[
                    "game_start_time_utc"
                ]
            )
            + wait
        )
    ]


def main() -> None:

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--dry-run",
        action="store_true",
    )

    parser.add_argument(
        "--limit-markets",
        type=int,
        default=None,
    )

    args = parser.parse_args()

    contract = json.loads(
        CONTRACT.read_text(
            encoding="utf-8"
        )
    )

    if (
        contract["pagination"]["limit"]
        != PAGE_LIMIT
        or
        contract["pagination"][
            "maximum_offset"
        ]
        != MAX_OFFSET
    ):
        raise RuntimeError(
            "code/contract pagination mismatch"
        )

    if (
        contract["eligibility_gate"][
            "minimum_hours_after_registered_game_start"
        ]
        != WAIT_HOURS
    ):
        raise RuntimeError(
            "code/contract maturity gate mismatch"
        )

    rows = load_registry()
    rn1 = load_rn1()

    now = utcnow()

    matured = eligible_rows(
        rows,
        now=now,
    )

    if args.limit_markets is not None:
        if args.limit_markets < 1:
            raise RuntimeError(
                "--limit-markets must be >= 1"
            )

        matured = matured[
            :args.limit_markets
        ]

    OUT_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    print()
    print(
        "========================================"
    )
    print(
        "RN1-F19b RAW ACQUISITION"
    )
    print(
        "========================================"
    )
    print(
        "captured_at:",
        iso(now),
    )
    print(
        "registry rows:",
        len(rows),
    )
    print(
        "12h-matured selected:",
        len(matured),
    )
    print(
        "contract sha256:",
        sha256_file(CONTRACT),
    )
    print(
        "git head:",
        git_head(),
    )

    if args.dry_run:
        for row in matured:
            print(
                "DRY",
                row["game_start_time_utc"],
                row["slug"],
                row["condition_id"],
            )

        print()
        print(
            "DRY RUN — no trade data acquired"
        )
        return

    results = []

    with httpx.Client(
        timeout=45,
        headers=UA,
    ) as client:

        for row in matured:

            condition = str(
                row["condition_id"]
            )

            final_dir = (
                OUT_ROOT
                / condition
            )

            if existing_finalized(
                final_dir
            ):
                print(
                    "SKIP finalized:",
                    row["slug"],
                )
                continue

            if final_dir.exists():
                raise RuntimeError(
                    "non-finalized output "
                    f"already exists: {final_dir}"
                )

            tmp_dir = (
                OUT_ROOT
                / (
                    ".tmp_"
                    + condition
                    + "_"
                    + uuid.uuid4().hex
                )
            )

            tmp_dir.mkdir(
                parents=True
            )

            try:

                captured_at = utcnow()

                start = parse_dt(
                    row[
                        "game_start_time_utc"
                    ]
                )

                if captured_at < (
                    start
                    + timedelta(
                        hours=WAIT_HOURS
                    )
                ):
                    raise RuntimeError(
                        "market no longer passes "
                        "12h acquisition gate"
                    )

                gamma = gamma_state(
                    client,
                    row,
                )

                write_json(
                    tmp_dir
                    / "gamma_closed_snapshot.json",
                    {
                        "captured_at_utc":
                            iso(captured_at),

                        "registry":
                            row,

                        "gamma":
                            gamma,
                    },
                )

                print()
                print(
                    "ACQUIRE:",
                    row["slug"],
                )

                market_taker = (
                    acquire_stream_twice(
                        client,
                        condition_id=condition,
                        name="market_taker",
                        taker_only=True,
                        user=None,
                        tmp_dir=tmp_dir,
                    )
                )

                rn1_all = (
                    acquire_stream_twice(
                        client,
                        condition_id=condition,
                        name="rn1_all",
                        taker_only=False,
                        user=rn1,
                        tmp_dir=tmp_dir,
                    )
                )

                manifest = {
                    "study":
                        "RN1-F19b",

                    "status":
                        "PASS",

                    "purpose":
                        "Raw OOS acquisition only; no signal or performance evaluation.",

                    "condition_id":
                        condition,

                    "slug":
                        row["slug"],

                    "gamma_market_id":
                        row[
                            "gamma_market_id"
                        ],

                    "registered_at_utc":
                        row[
                            "registered_at_utc"
                        ],

                    "game_start_time_utc":
                        row[
                            "game_start_time_utc"
                        ],

                    "acquired_at_utc":
                        iso(
                            utcnow()
                        ),

                    "contract_sha256":
                        sha256_file(
                            CONTRACT
                        ),

                    "git_head":
                        git_head(),

                    "eligibility": {
                        "minimum_hours_after_start":
                            WAIT_HOURS,

                        "gamma_closed":
                            True,
                    },

                    "streams": {
                        "market_taker":
                            market_taker,

                        "rn1_all":
                            rn1_all,
                    },

                    "analysis_boundary": {
                        "signal_calculated":
                            False,

                        "markout_calculated":
                            False,

                        "settlement_used":
                            False,

                        "pnl_calculated":
                            False,

                        "winner_used":
                            False,
                    },
                }

                write_json(
                    tmp_dir
                    / "manifest.json",
                    manifest,
                )

                tmp_dir.rename(
                    final_dir
                )

                results.append(
                    manifest
                )

                print(
                    "PASS",
                    row["slug"],
                    "| market_taker rows",
                    market_taker[
                        "row_count"
                    ],
                    "| rn1 rows",
                    rn1_all[
                        "row_count"
                    ],
                )

            except Exception:
                shutil.rmtree(
                    tmp_dir,
                    ignore_errors=True,
                )
                raise

    print()
    print(
        "========================================"
    )
    print(
        "F19b ACQUISITION SUMMARY"
    )
    print(
        "========================================"
    )
    print(
        "newly finalized:",
        len(results),
    )
    print(
        "analysis performed: NO"
    )
    print(
        "signal calculated: NO"
    )
    print(
        "markout calculated: NO"
    )
    print(
        "settlement used: NO"
    )
    print(
        "PnL calculated: NO"
    )


if __name__ == "__main__":
    main()
