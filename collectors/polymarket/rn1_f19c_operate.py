from __future__ import annotations

import argparse
import json
import shutil
import sys
import uuid
from collections import Counter
from datetime import timedelta
from pathlib import Path
from typing import Any


REPO = Path(__file__).resolve().parents[2]

if str(REPO) not in sys.path:
    sys.path.insert(
        0,
        str(REPO),
    )

from collectors.polymarket import rn1_f19b_acquire as f19b


CONTRACT = (
    REPO
    / "research"
    / "rn1_f19c_operations_contract.json"
)

F19B_VALIDATION = (
    REPO
    / "research"
    / "rn1_f19b_acquisition_validation.json"
)

OPS_ROOT = (
    REPO
    / "data"
    / "prospective"
    / "rn1_f19"
    / "ops"
)

RUN_ROOT = (
    OPS_ROOT
    / "runs"
)

EXPECTED_REGISTRY_ROWS = 100
WAIT_HOURS = 12


def validate_frozen_parent() -> None:

    validation = json.loads(
        F19B_VALIDATION.read_text(
            encoding="utf-8"
        )
    )

    current_contract_sha = (
        f19b.sha256_file(
            f19b.CONTRACT
        )
    )

    if (
        current_contract_sha
        != validation[
            "contract_sha256"
        ]
    ):
        raise RuntimeError(
            "F19b contract changed after freeze"
        )

    current_collector_sha = (
        f19b.sha256_file(
            Path(f19b.__file__)
        )
    )

    if (
        current_collector_sha
        != validation[
            "collector_sha256"
        ]
    ):
        raise RuntimeError(
            "F19b collector changed after freeze"
        )


def validate_contract() -> dict[str, Any]:

    payload = json.loads(
        CONTRACT.read_text(
            encoding="utf-8"
        )
    )

    if (
        payload["registry"][
            "required_rows"
        ]
        != EXPECTED_REGISTRY_ROWS
    ):
        raise RuntimeError(
            "F19c contract registry-size mismatch"
        )

    if (
        payload["acquisition"][
            "minimum_hours_after_registered_game_start"
        ]
        != WAIT_HOURS
    ):
        raise RuntimeError(
            "F19c contract maturity-gate mismatch"
        )

    return payload


def validate_registry(
    rows: list[dict[str, Any]],
) -> None:

    if len(rows) != EXPECTED_REGISTRY_ROWS:
        raise RuntimeError(
            f"F19 registry must contain exactly "
            f"{EXPECTED_REGISTRY_ROWS} rows; "
            f"got {len(rows)}"
        )

    conditions = [
        row["condition_id"]
        for row in rows
    ]

    if (
        len(set(conditions))
        != EXPECTED_REGISTRY_ROWS
    ):
        raise RuntimeError(
            "F19 registry does not contain "
            "100 unique conditions"
        )


def gamma_preflight(
    client,
    row: dict[str, Any],
) -> dict[str, Any]:

    payload = f19b.request_json(
        client,
        f"{f19b.GAMMA}/markets/"
        f"{row['gamma_market_id']}",
    )

    if not isinstance(
        payload,
        dict,
    ):
        raise RuntimeError(
            "Gamma market response is not object"
        )

    expected = str(
        row["condition_id"]
    ).lower()

    actual = str(
        payload.get("conditionId")
        or ""
    ).lower()

    if actual != expected:
        raise RuntimeError(
            "Gamma condition_id mismatch"
        )

    return payload


def acquire_one(
    client,
    *,
    row: dict[str, Any],
    rn1: str,
    gamma: dict[str, Any],
) -> dict[str, Any]:

    condition = str(
        row["condition_id"]
    )

    final_dir = (
        f19b.OUT_ROOT
        / condition
    )

    if final_dir.exists():
        raise RuntimeError(
            "non-finalized acquisition directory "
            f"already exists: {final_dir}"
        )

    tmp_dir = (
        f19b.OUT_ROOT
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

        captured_at = (
            f19b.utcnow()
        )

        f19b.write_json(
            tmp_dir
            / "gamma_closed_snapshot.json",
            {
                "captured_at_utc":
                    f19b.iso(
                        captured_at
                    ),

                "registry":
                    row,

                "gamma":
                    gamma,
            },
        )

        market_taker = (
            f19b.acquire_stream_twice(
                client,
                condition_id=condition,
                name="market_taker",
                taker_only=True,
                user=None,
                tmp_dir=tmp_dir,
            )
        )

        rn1_all = (
            f19b.acquire_stream_twice(
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
                "RN1-F19c",

            "acquisition_method":
                "frozen_RN1-F19b",

            "status":
                "PASS",

            "purpose":
                "Blind raw prospective OOS acquisition only.",

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
                f19b.iso(
                    f19b.utcnow()
                ),

            "f19b_contract_sha256":
                f19b.sha256_file(
                    f19b.CONTRACT
                ),

            "f19c_contract_sha256":
                f19b.sha256_file(
                    CONTRACT
                ),

            "git_head":
                f19b.git_head(),

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

        f19b.write_json(
            tmp_dir
            / "manifest.json",
            manifest,
        )

        tmp_dir.rename(
            final_dir
        )

        return manifest

    except Exception:

        shutil.rmtree(
            tmp_dir,
            ignore_errors=True,
        )

        raise


def main() -> None:

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--dry-run",
        action="store_true",
    )

    args = parser.parse_args()

    contract = validate_contract()

    validate_frozen_parent()

    current_registry_sha = (
        f19b.sha256_file(
            f19b.REGISTRY
        )
    )

    frozen_registry_sha = str(
        contract["registry"].get(
            "sha256"
        )
        or ""
    )

    if (
        current_registry_sha
        != frozen_registry_sha
    ):
        raise RuntimeError(
            "F19 registry hash changed "
            "after 100-market cohort freeze"
        )

    rows = f19b.load_registry()

    validate_registry(
        rows
    )

    rn1 = f19b.load_rn1()

    now = f19b.utcnow()

    wait = timedelta(
        hours=WAIT_HOURS
    )

    f19b.OUT_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    counters = Counter()

    results = []

    pending_matured = []

    for row in rows:

        condition = str(
            row["condition_id"]
        )

        final_dir = (
            f19b.OUT_ROOT
            / condition
        )

        if f19b.existing_finalized(
            final_dir
        ):
            counters[
                "already_finalized"
            ] += 1
            continue

        if final_dir.exists():
            counters[
                "invalid_existing_directory"
            ] += 1

            results.append(
                {
                    "condition_id":
                        condition,

                    "slug":
                        row["slug"],

                    "status":
                        "INVALID_EXISTING_DIRECTORY",
                }
            )

            continue

        start = f19b.parse_dt(
            row[
                "game_start_time_utc"
            ]
        )

        if now < (
            start + wait
        ):
            counters["future"] += 1
            continue

        counters[
            "matured_pending"
        ] += 1

        pending_matured.append(
            row
        )

    print()
    print(
        "========================================"
    )
    print(
        "RN1-F19c PROSPECTIVE OPERATIONS"
    )
    print(
        "========================================"
    )

    print(
        "captured_at:",
        f19b.iso(now),
    )

    print(
        "registry rows:",
        len(rows),
    )

    print(
        "already finalized:",
        counters[
            "already_finalized"
        ],
    )

    print(
        "future:",
        counters["future"],
    )

    print(
        "matured pending:",
        counters[
            "matured_pending"
        ],
    )

    print(
        "invalid existing dirs:",
        counters[
            "invalid_existing_directory"
        ],
    )

    if args.dry_run:

        print()
        print(
            "DRY RUN — Gamma check only "
            "for matured pending markets"
        )

    with f19b.httpx.Client(
        timeout=45,
        headers=f19b.UA,
    ) as client:

        for row in pending_matured:

            condition = str(
                row["condition_id"]
            )

            try:

                gamma = gamma_preflight(
                    client,
                    row,
                )

                if (
                    gamma.get("closed")
                    is not True
                ):

                    counters[
                        "deferred_open"
                    ] += 1

                    results.append(
                        {
                            "condition_id":
                                condition,

                            "slug":
                                row["slug"],

                            "status":
                                "DEFER_OPEN",

                            "gamma_closed":
                                gamma.get(
                                    "closed"
                                ),

                            "accepting_orders":
                                gamma.get(
                                    "acceptingOrders"
                                ),
                        }
                    )

                    print(
                        "DEFER",
                        row["slug"],
                        "| Gamma closed=false",
                    )

                    continue

                counters[
                    "closed_ready"
                ] += 1

                if args.dry_run:

                    results.append(
                        {
                            "condition_id":
                                condition,

                            "slug":
                                row["slug"],

                            "status":
                                "READY_CLOSED",
                        }
                    )

                    print(
                        "READY",
                        row["slug"],
                    )

                    continue

                print()
                print(
                    "ACQUIRE",
                    row["slug"],
                )

                manifest = acquire_one(
                    client,
                    row=row,
                    rn1=rn1,
                    gamma=gamma,
                )

                counters[
                    "newly_finalized"
                ] += 1

                results.append(
                    {
                        "condition_id":
                            condition,

                        "slug":
                            row["slug"],

                        "status":
                            "FINALIZED",

                        "market_taker_rows":
                            manifest[
                                "streams"
                            ][
                                "market_taker"
                            ][
                                "row_count"
                            ],

                        "rn1_rows":
                            manifest[
                                "streams"
                            ][
                                "rn1_all"
                            ][
                                "row_count"
                            ],
                    }
                )

                print(
                    "PASS",
                    row["slug"],
                )

            except Exception as exc:

                counters[
                    "failed"
                ] += 1

                results.append(
                    {
                        "condition_id":
                            condition,

                        "slug":
                            row["slug"],

                        "status":
                            "FAILED",

                        "error":
                            str(exc),
                    }
                )

                print(
                    "FAILED",
                    row["slug"],
                    "|",
                    str(exc),
                )

    print()
    print(
        "========================================"
    )
    print(
        "F19c OPERATIONS SUMMARY"
    )
    print(
        "========================================"
    )

    for key in (
        "already_finalized",
        "future",
        "matured_pending",
        "closed_ready",
        "deferred_open",
        "newly_finalized",
        "failed",
        "invalid_existing_directory",
    ):
        print(
            f"{key}:",
            counters[key],
        )

    print(
        "performance read: NO"
    )

    if args.dry_run:

        print(
            "DRY RUN — no acquisition "
            "directories written"
        )

        return

    RUN_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    stamp = (
        f19b.utcnow()
        .strftime(
            "%Y%m%dT%H%M%S.%fZ"
        )
    )

    receipt_path = (
        RUN_ROOT
        / f"run_{stamp}.json"
    )

    f19b.write_json(
        receipt_path,
        {
            "study":
                "RN1-F19c",

            "status":
                (
                    "PASS"
                    if (
                        counters["failed"]
                        == 0
                        and
                        counters[
                            "invalid_existing_directory"
                        ]
                        == 0
                    )
                    else
                    "PARTIAL_FAILURE"
                ),

            "captured_at_utc":
                f19b.iso(now),

            "completed_at_utc":
                f19b.iso(
                    f19b.utcnow()
                ),

            "registry_rows":
                len(rows),

            "contract_sha256":
                f19b.sha256_file(
                    CONTRACT
                ),

            "f19b_contract_sha256":
                f19b.sha256_file(
                    f19b.CONTRACT
                ),

            "counters":
                dict(counters),

            "results":
                results,

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
        },
    )

    print(
        "receipt:",
        receipt_path,
    )

    if (
        counters["failed"]
        or
        counters[
            "invalid_existing_directory"
        ]
    ):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
