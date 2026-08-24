from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
from dotenv import dotenv_values


REPO = Path(__file__).resolve().parents[1]

PREREG = REPO / "research/h5_m1r2_rerun_amendment.json"
EXECUTION = REPO / "research/h5_m1r2_stage2_execution_addendum.json"

STAGE1_REGISTRY = REPO / "data/research/h5_m1r2/registry.jsonl"
STAGE1_RECEIPT = REPO / "research/h5_m1r2_registry_freeze.json"

H2_IDENTITIES = REPO / "research/h6_m2_h2_identity_exclusions.json"

ATTEMPT = REPO / "data/research/h5_m1r2/stage2_authoritative_attempt.json"
MAP = REPO / "data/research/h5_m1r2/external_id_map.jsonl"
MAP_RECEIPT = REPO / "research/h5_m1r2_external_id_map_freeze.json"


EXPECTED_PREREG_SHA = (
    "b805cc2fc9a01e1cd4b4d2d5c7966fde"
    "d4e19840bcbefcb72f102d50a0c380ed"
)

EXPECTED_EXECUTION_SHA = (
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

EXPECTED_H2_IDENTITY_SHA = (
    "0907e1bd4ccc2aaaf7cef332e7fe96b8"
    "fb05355afdfc2e912d412e2f10c78d73"
)

EVENTS_URL = (
    "https://api.the-odds-api.com/"
    "v4/sports/baseball_mlb/events"
)

MAX_START_DELTA_SECONDS = 600
MIN_NONBURNED_TO_FREEZE = 5

UA = {
    "User-Agent":
        "quant-lab-h5-m1r2-stage2-identity/1.0"
}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def verify_sha(
    path: Path,
    expected: str,
) -> None:
    if not path.is_file():
        raise RuntimeError(
            f"missing frozen input: {path}"
        )

    actual = sha256_file(path)

    if actual != expected:
        raise RuntimeError(
            f"SHA mismatch: {path}\n"
            f"expected={expected}\n"
            f"actual={actual}"
        )


def read_json(path: Path) -> dict[str, Any]:
    x = json.loads(
        path.read_text(encoding="utf-8")
    )

    if not isinstance(x, dict):
        raise RuntimeError(
            f"expected JSON object: {path}"
        )

    return x


def git_head() -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO,
        text=True,
    ).strip()


def committed_self_matches_head() -> bool:
    path = Path(__file__).resolve()
    rel = path.relative_to(REPO).as_posix()

    try:
        committed = subprocess.check_output(
            ["git", "show", f"HEAD:{rel}"],
            cwd=REPO,
        )
    except subprocess.CalledProcessError:
        return False

    return (
        sha256_bytes(committed)
        == sha256_file(path)
    )


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

    return dt.astimezone(timezone.utc)


def norm_team(value: Any) -> str:
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


def load_frozen_inputs():
    verify_sha(
        PREREG,
        EXPECTED_PREREG_SHA,
    )

    verify_sha(
        EXECUTION,
        EXPECTED_EXECUTION_SHA,
    )

    verify_sha(
        STAGE1_REGISTRY,
        EXPECTED_STAGE1_REGISTRY_SHA,
    )

    verify_sha(
        STAGE1_RECEIPT,
        EXPECTED_STAGE1_RECEIPT_SHA,
    )

    verify_sha(
        H2_IDENTITIES,
        EXPECTED_H2_IDENTITY_SHA,
    )

    prereg = read_json(
        PREREG
    )

    if (
        prereg.get("status")
        !=
        "PROSPECTIVE_ENGINEERING_RERUN_PREREGISTRATION"
    ):
        raise RuntimeError(
            "unexpected R2 prereg status"
        )

    stage2 = prereg[
        "stage2_identity_resolution"
    ]

    if (
        stage2[
            "map_freeze_minimum_resolved_nonburned"
        ]
        != MIN_NONBURNED_TO_FREEZE
        or
        stage2[
            "map_freeze_requires_ambiguity_count"
        ]
        != 0
        or
        stage2[
            "matching"
        ][
            "maximum_absolute_commence_time_delta_seconds"
        ]
        != MAX_START_DELTA_SECONDS
        or
        stage2[
            "matching"
        ][
            "unique_match_required"
        ]
        is not True
        or
        stage2["odds_read"]
        is not False
    ):
        raise RuntimeError(
            "R2 Stage2 prereg mismatch"
        )

    execution = read_json(
        EXECUTION
    )

    if (
        execution.get("status")
        !=
        "PREREGISTERED_STAGE2_EXECUTION_RULE"
    ):
        raise RuntimeError(
            "unexpected Stage2 execution status"
        )

    rule = execution[
        "stage2_execution"
    ]

    if (
        rule[
            "authoritative_provider_attempt_count"
        ]
        != 1
        or
        rule[
            "preview_provider_calls"
        ]
        != 0
        or
        rule[
            "retry_after_failed_attempt"
        ]
        is not False
        or
        rule[
            "endpoint_contains_odds"
        ]
        is not False
        or
        rule["odds_read"]
        is not False
        or
        rule[
            "success_gate"
        ][
            "minimum_resolved_nonburned"
        ]
        != MIN_NONBURNED_TO_FREEZE
        or
        rule[
            "success_gate"
        ][
            "maximum_ambiguous"
        ]
        != 0
    ):
        raise RuntimeError(
            "Stage2 execution rule mismatch"
        )

    receipt = read_json(
        STAGE1_RECEIPT
    )

    if (
        receipt.get("status")
        !=
        "STAGE1_CONDITION_REGISTRY_FROZEN"
        or
        receipt.get("milestone")
        != "H5-M1R2"
        or
        receipt.get("registry_sha256")
        != EXPECTED_STAGE1_REGISTRY_SHA
    ):
        raise RuntimeError(
            "Stage1 receipt binding mismatch"
        )

    rows = [
        json.loads(line)
        for line
        in STAGE1_REGISTRY.read_text(
            encoding="utf-8"
        ).splitlines()
        if line.strip()
    ]

    if len(rows) != 10:
        raise RuntimeError(
            f"expected 10 Stage1 rows; "
            f"got {len(rows)}"
        )

    conditions = []

    for i, row in enumerate(
        rows,
        start=1,
    ):
        if (
            row.get("milestone")
            != "H5-M1R2"
            or
            row.get(
                "registration_index"
            )
            != i
        ):
            raise RuntimeError(
                "Stage1 row/order mismatch"
            )

        cid = str(
            row.get("condition_id")
            or ""
        ).strip().lower()

        if not cid:
            raise RuntimeError(
                "empty Stage1 condition"
            )

        if (
            row.get("odds_game_id")
            is not None
        ):
            raise RuntimeError(
                "Stage1 contains provider identity"
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
                "invalid Stage1 game start"
            )

        conditions.append(cid)

    if len(set(conditions)) != 10:
        raise RuntimeError(
            "duplicate Stage1 conditions"
        )

    if (
        receipt.get("condition_ids")
        != conditions
    ):
        raise RuntimeError(
            "Stage1 registry/receipt mismatch"
        )

    h2 = read_json(
        H2_IDENTITIES
    )

    if (
        h2.get("artifact")
        !=
        "H2_V2_BURNED_IDENTITY_EXCLUSIONS"
        or
        h2.get("resolved_count")
        != 9
    ):
        raise RuntimeError(
            "unexpected H2 identity artifact"
        )

    mappings = h2.get(
        "mappings"
    )

    if (
        not isinstance(mappings, list)
        or len(mappings) != 9
    ):
        raise RuntimeError(
            "expected 9 H2 identity mappings"
        )

    burned_ids = {
        str(
            row.get(
                "oddsapi_event_id"
            )
            or ""
        ).strip()
        for row in mappings
    }

    if (
        "" in burned_ids
        or len(burned_ids) != 9
    ):
        raise RuntimeError(
            "H2 Odds API identities incomplete"
        )

    required_false = (
        "external_odds_read",
        "h2_edge_read",
        "h2_outcome_read",
        "h2_performance_read",
        "h2_prices_read",
        "h5_response_read",
        "pnl_calculated",
    )

    for key in required_false:
        if (
            h2[
                "analysis_boundary"
            ].get(key)
            is not False
        ):
            raise RuntimeError(
                "H2 identity analysis "
                f"boundary violation: {key}"
            )

    return (
        rows,
        burned_ids,
        execution,
    )


def provider_projection_bytes(
    projection,
) -> bytes:
    return (
        json.dumps(
            projection,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        + "\n"
    ).encode("utf-8")


def parse_provider_payload(
    payload,
):
    if not isinstance(
        payload,
        list,
    ):
        raise RuntimeError(
            "Odds /events response "
            "is not a list"
        )

    by_id = {}

    for raw in payload:
        if not isinstance(
            raw,
            dict,
        ):
            continue

        event_id = str(
            raw.get("id")
            or ""
        ).strip()

        home = str(
            raw.get("home_team")
            or ""
        ).strip()

        away = str(
            raw.get("away_team")
            or ""
        ).strip()

        commence = parse_dt(
            raw.get(
                "commence_time"
            )
        )

        if (
            not event_id
            or not home
            or not away
            or commence is None
        ):
            continue

        projection = {
            "id":
                event_id,
            "home_team":
                home,
            "away_team":
                away,
            "commence_time":
                commence.isoformat(),
        }

        if event_id in by_id:
            if (
                by_id[event_id]
                != projection
            ):
                raise RuntimeError(
                    "conflicting duplicate "
                    "provider event ID: "
                    f"{event_id}"
                )

            continue

        by_id[
            event_id
        ] = projection

    projection = sorted(
        by_id.values(),
        key=lambda row: (
            row["commence_time"],
            row["id"],
        ),
    )

    events = []

    for row in projection:
        events.append({
            "odds_game_id":
                row["id"],

            "canonical_commence_time":
                parse_dt(
                    row[
                        "commence_time"
                    ]
                ),

            "home_team":
                row[
                    "home_team"
                ],

            "away_team":
                row[
                    "away_team"
                ],

            "team_set": {
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
            },
        })

    return (
        events,
        projection,
    )


def fetch_provider_events(
    *,
    api_key: str,
    http: httpx.Client,
):
    try:
        response = http.get(
            EVENTS_URL,
            params={
                "apiKey":
                    api_key,
                "dateFormat":
                    "iso",
            },
        )
    except httpx.HTTPError:
        raise RuntimeError(
            "Odds /events request failed"
        ) from None

    if response.status_code != 200:
        raise RuntimeError(
            "Odds /events request failed: "
            f"HTTP {response.status_code}"
        )

    return parse_provider_payload(
        response.json()
    )


def resolve_rows(
    stage1_rows,
    provider_events,
    burned_ids,
    *,
    resolved_at: datetime,
    resolver_sha: str,
):
    resolved = []
    statuses = []

    for row in stage1_rows:
        wanted = {
            norm_team(
                row["p1_team"]
            ),
            norm_team(
                row["p0_team"]
            ),
        }

        pm_start = parse_dt(
            row[
                "pm_game_start_time"
            ]
        )

        assert pm_start is not None

        candidates = []

        for event in provider_events:
            if (
                event["team_set"]
                != wanted
            ):
                continue

            delta = (
                event[
                    "canonical_commence_time"
                ]
                - pm_start
            ).total_seconds()

            if (
                abs(delta)
                >
                MAX_START_DELTA_SECONDS
            ):
                continue

            candidates.append(
                (
                    event,
                    delta,
                )
            )

        base_status = {
            "registration_index":
                row[
                    "registration_index"
                ],

            "condition_id":
                row[
                    "condition_id"
                ],

            "gamma_slug":
                row[
                    "gamma_slug"
                ],
        }

        if not candidates:
            statuses.append({
                **base_status,

                "status":
                    "PENDING_PROVIDER_VISIBILITY",
            })
            continue

        if len(candidates) > 1:
            statuses.append({
                **base_status,

                "status":
                    "AMBIGUOUS_FAIL_CLOSED",

                "candidate_count":
                    len(candidates),
            })
            continue

        event, delta = candidates[0]

        burned = (
            event[
                "odds_game_id"
            ]
            in burned_ids
        )

        status = (
            "RESOLVED_H2_BURNED_PROHIBITED"
            if burned
            else
            "RESOLVED_NONBURNED"
        )

        result = {
            "study":
                "H5",

            "milestone":
                "H5-M1R2",

            "registration_stage":
                "STAGE2_EXTERNAL_IDENTITY",

            "registration_index":
                row[
                    "registration_index"
                ],

            "condition_id":
                str(
                    row[
                        "condition_id"
                    ]
                ).lower(),

            "gamma_market_id":
                str(
                    row[
                        "gamma_market_id"
                    ]
                ),

            "gamma_slug":
                row[
                    "gamma_slug"
                ],

            "p1_team":
                row[
                    "p1_team"
                ],

            "p0_team":
                row[
                    "p0_team"
                ],

            "pm_game_start_time":
                row[
                    "pm_game_start_time"
                ],

            "odds_game_id":
                event[
                    "odds_game_id"
                ],

            "provider_home_team":
                event[
                    "home_team"
                ],

            "provider_away_team":
                event[
                    "away_team"
                ],

            "canonical_commence_time":
                event[
                    "canonical_commence_time"
                ].isoformat(),

            "start_delta_seconds":
                delta,

            "resolution_status":
                status,

            "resolved_at_utc":
                resolved_at.isoformat(),

            "mapping_rule":
                (
                    "exact_normalized_MLB_team_set"
                    "+unique_provider_event"
                    "+abs_start_delta_le_600s"
                ),

            "r2_preregistration_sha256":
                EXPECTED_PREREG_SHA,

            "stage2_execution_addendum_sha256":
                EXPECTED_EXECUTION_SHA,

            "stage1_registry_sha256":
                EXPECTED_STAGE1_REGISTRY_SHA,

            "stage1_receipt_sha256":
                EXPECTED_STAGE1_RECEIPT_SHA,

            "h2_identity_exclusions_sha256":
                EXPECTED_H2_IDENTITY_SHA,

            "resolver_sha256":
                resolver_sha,
        }

        resolved.append(
            result
        )

        statuses.append({
            **base_status,

            "status":
                status,

            "odds_game_id":
                event[
                    "odds_game_id"
                ],

            "start_delta_seconds":
                delta,
        })

    return (
        resolved,
        statuses,
    )


def gate_counts(statuses):
    counts = {
        "RESOLVED_NONBURNED": 0,
        "RESOLVED_H2_BURNED_PROHIBITED": 0,
        "PENDING_PROVIDER_VISIBILITY": 0,
        "AMBIGUOUS_FAIL_CLOSED": 0,
    }

    for row in statuses:
        status = row[
            "status"
        ]

        if status not in counts:
            raise RuntimeError(
                f"unknown status: {status}"
            )

        counts[status] += 1

    return counts


def validate_gate(statuses):
    counts = gate_counts(
        statuses
    )

    if (
        counts[
            "AMBIGUOUS_FAIL_CLOSED"
        ]
        != 0
    ):
        raise RuntimeError(
            "Stage2 gate FAIL: "
            "ambiguous mapping present"
        )

    if (
        counts[
            "RESOLVED_NONBURNED"
        ]
        <
        MIN_NONBURNED_TO_FREEZE
    ):
        raise RuntimeError(
            "Stage2 gate FAIL: "
            "fewer than 5 resolved "
            "nonburned markets"
        )

    return counts


def stage2_deadline(
    stage1_rows,
) -> datetime:
    starts = [
        parse_dt(
            row[
                "pm_game_start_time"
            ]
        )
        for row in stage1_rows
    ]

    if any(
        x is None
        for x in starts
    ):
        raise RuntimeError(
            "invalid Stage1 start time"
        )

    return (
        min(starts)
        - timedelta(minutes=30)
    )


def validate_deadline(
    stage1_rows,
    *,
    now: datetime,
) -> datetime:
    deadline = stage2_deadline(
        stage1_rows
    )

    if now > deadline:
        raise RuntimeError(
            "Stage2 deadline passed; "
            "provider call prohibited"
        )

    return deadline


def serialize_rows(rows) -> bytes:
    ordered = sorted(
        rows,
        key=lambda row: (
            row[
                "registration_index"
            ],
            row[
                "condition_id"
            ],
        ),
    )

    return "".join(
        json.dumps(
            row,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        + "\n"
        for row in ordered
    ).encode("utf-8")


def create_exclusive(
    path: Path,
    data: bytes,
):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    fd = os.open(
        path,
        (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
        ),
        0o644,
    )

    with os.fdopen(
        fd,
        "wb",
    ) as fh:
        fh.write(data)
        fh.flush()
        os.fsync(
            fh.fileno()
        )


def validate_config():
    rows, burned, _ = (
        load_frozen_inputs()
    )

    deadline = stage2_deadline(
        rows
    )

    print(
        "H5-M1R2 STAGE2 CONFIG: PASS"
    )

    print(
        "Stage1 registry SHA:",
        EXPECTED_STAGE1_REGISTRY_SHA,
    )

    print(
        "Stage1 receipt SHA:",
        EXPECTED_STAGE1_RECEIPT_SHA,
    )

    print(
        "R2 prereg SHA:",
        EXPECTED_PREREG_SHA,
    )

    print(
        "Stage2 execution SHA:",
        EXPECTED_EXECUTION_SHA,
    )

    print(
        "H2 identity SHA:",
        EXPECTED_H2_IDENTITY_SHA,
    )

    print(
        "Stage1 markets:",
        len(rows),
    )

    print(
        "H2 burned provider IDs:",
        len(burned),
    )

    print(
        "Stage2 deadline UTC:",
        deadline.isoformat(),
    )

    print(
        "authoritative provider attempts:",
        1,
    )

    print(
        "preview provider calls:",
        0,
    )

    print(
        "database lookup required: NO"
    )

    print(
        "provider called: NO"
    )

    print(
        "odds read: NO"
    )

    print(
        "map written: NO"
    )


def main():
    parser = argparse.ArgumentParser()

    modes = (
        parser.add_mutually_exclusive_group(
            required=True
        )
    )

    modes.add_argument(
        "--validate-config",
        action="store_true",
    )

    modes.add_argument(
        "--freeze",
        action="store_true",
    )

    args = parser.parse_args()

    if args.validate_config:
        validate_config()
        return

    if ATTEMPT.exists():
        raise RuntimeError(
            "Stage2 authoritative attempt "
            "already occurred; retry prohibited"
        )

    if MAP.exists():
        raise RuntimeError(
            "Stage2 map already exists"
        )

    if MAP_RECEIPT.exists():
        raise RuntimeError(
            "Stage2 map receipt already exists"
        )

    rows, burned_ids, _ = (
        load_frozen_inputs()
    )

    if not committed_self_matches_head():
        raise RuntimeError(
            "refusing provider access: "
            "resolver is not byte-identical "
            "to committed HEAD"
        )

    env = dotenv_values(
        REPO / ".env"
    )

    api_key = env.get(
        "ODDS_API_KEY"
    )

    if not api_key:
        raise RuntimeError(
            "ODDS_API_KEY missing"
        )

    resolver_sha = sha256_file(
        Path(__file__).resolve()
    )

    attempt_at = utcnow()

    deadline = validate_deadline(
        rows,
        now=attempt_at,
    )

    attempt = {
        "study":
            "H5",

        "milestone":
            "H5-M1R2",

        "status":
            "STAGE2_AUTHORITATIVE_ATTEMPT_STARTED",

        "attempt_number":
            1,

        "started_at_utc":
            attempt_at.isoformat(),

        "latest_permitted_freeze_utc":
            deadline.isoformat(),

        "resolver_sha256":
            resolver_sha,

        "git_head":
            git_head(),

        "stage1_registry_sha256":
            EXPECTED_STAGE1_REGISTRY_SHA,

        "stage1_receipt_sha256":
            EXPECTED_STAGE1_RECEIPT_SHA,

        "stage2_execution_addendum_sha256":
            EXPECTED_EXECUTION_SHA,

        "provider_identity_only":
            True,

        "odds_read":
            False,
    }

    attempt_bytes = (
        json.dumps(
            attempt,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")

    # This create-only marker mechanically burns
    # the single preregistered provider attempt.
    create_exclusive(
        ATTEMPT,
        attempt_bytes,
    )

    attempt_sha = sha256_bytes(
        attempt_bytes
    )

    with httpx.Client(
        headers=UA,
        timeout=30,
    ) as http:
        (
            provider_events,
            projection,
        ) = fetch_provider_events(
            api_key=api_key,
            http=http,
        )

    projection_bytes = (
        provider_projection_bytes(
            projection
        )
    )

    projection_sha = (
        sha256_bytes(
            projection_bytes
        )
    )

    resolved, statuses = (
        resolve_rows(
            rows,
            provider_events,
            burned_ids,
            resolved_at=attempt_at,
            resolver_sha=resolver_sha,
        )
    )

    counts = gate_counts(
        statuses
    )

    print("=" * 72)
    print(
        "H5-M1R2 STAGE2 AUTHORITATIVE IDENTITY RESOLUTION"
    )
    print(
        "THE ODDS API /events — IDENTITY ONLY"
    )
    print("=" * 72)

    print(
        "attempt_started_at_utc:",
        attempt_at.isoformat(),
    )

    print(
        "deadline_utc:",
        deadline.isoformat(),
    )

    print(
        "provider_identity_events:",
        len(provider_events),
    )

    print(
        "provider_projection_sha256:",
        projection_sha,
    )

    print()

    for row in statuses:
        print(
            row[
                "registration_index"
            ],
            "|",
            row[
                "gamma_slug"
            ],
            "|",
            row[
                "status"
            ],
            "|",
            row.get(
                "odds_game_id",
                "-",
            ),
        )

    print()
    print(
        "resolved_nonburned:",
        counts[
            "RESOLVED_NONBURNED"
        ],
    )

    print(
        "resolved_h2_burned:",
        counts[
            "RESOLVED_H2_BURNED_PROHIBITED"
        ],
    )

    print(
        "pending:",
        counts[
            "PENDING_PROVIDER_VISIBILITY"
        ],
    )

    print(
        "ambiguous:",
        counts[
            "AMBIGUOUS_FAIL_CLOSED"
        ],
    )

    print(
        "odds read: NO"
    )

    print(
        "PM prices read: NO"
    )

    print(
        "response calculated: NO"
    )

    print(
        "PnL calculated: NO"
    )

    # IMPORTANT:
    # on failure this raises BEFORE map/receipt
    # writes. The attempt marker remains, so
    # retry is mechanically prohibited.
    counts = validate_gate(
        statuses
    )

    map_bytes = serialize_rows(
        resolved
    )

    map_sha = sha256_bytes(
        map_bytes
    )

    receipt = {
        "study":
            "H5",

        "milestone":
            "H5-M1R2",

        "status":
            "STAGE2_EXTERNAL_ID_MAP_FROZEN",

        "frozen_at_utc":
            attempt_at.isoformat(),

        "authoritative_provider_attempt":
            1,

        "retry_permitted":
            False,

        "attempt_marker_path":
            str(
                ATTEMPT.relative_to(
                    REPO
                )
            ),

        "attempt_marker_sha256":
            attempt_sha,

        "map_path":
            str(
                MAP.relative_to(
                    REPO
                )
            ),

        "map_sha256":
            map_sha,

        "map_row_count":
            len(resolved),

        "resolver_path":
            str(
                Path(__file__)
                .resolve()
                .relative_to(REPO)
            ),

        "resolver_sha256":
            resolver_sha,

        "git_head":
            git_head(),

        "r2_preregistration_sha256":
            EXPECTED_PREREG_SHA,

        "stage2_execution_addendum_sha256":
            EXPECTED_EXECUTION_SHA,

        "stage1_registry_sha256":
            EXPECTED_STAGE1_REGISTRY_SHA,

        "stage1_receipt_sha256":
            EXPECTED_STAGE1_RECEIPT_SHA,

        "h2_identity_exclusions_sha256":
            EXPECTED_H2_IDENTITY_SHA,

        "latest_permitted_freeze_utc":
            deadline.isoformat(),

        "mapping_rule": {
            "team_set":
                "exact_normalized_MLB_team_set",

            "maximum_absolute_commence_time_delta_seconds":
                MAX_START_DELTA_SECONDS,

            "unique_provider_event_required":
                True,
        },

        "gate": {
            "minimum_resolved_nonburned":
                MIN_NONBURNED_TO_FREEZE,

            "maximum_ambiguous":
                0,

            "passed":
                True,

            "counts":
                counts,
        },

        "resolution_statuses":
            statuses,

        "provider_identity_projection": {
            "allowed_fields": [
                "id",
                "home_team",
                "away_team",
                "commence_time",
            ],

            "event_count":
                len(projection),

            "sha256":
                projection_sha,

            "events":
                projection,
        },

        "network_boundary": {
            "endpoint":
                "/v4/sports/baseball_mlb/events",

            "identity_only":
                True,

            "odds_bearing_endpoint_used":
                False,

            "odds_read":
                False,

            "api_key_persisted":
                False,
        },

        "acquisition_started":
            False,

        "analysis_boundary": {
            "external_odds_read":
                False,

            "external_consensus_calculated":
                False,

            "external_state_difference_calculated":
                False,

            "pm_response_calculated":
                False,

            "correlation_calculated":
                False,

            "regression_calculated":
                False,

            "markout_calculated":
                False,

            "edge_calculated":
                False,

            "pnl_calculated":
                False,

            "winner_used":
                False,

            "settlement_used":
                False,

            "f19_trade_content_read":
                False,

            "f20_read":
                False,

            "h2_performance_read":
                False,

            "h6_response_read":
                False,
        },
    }

    receipt_bytes = (
        json.dumps(
            receipt,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")

    create_exclusive(
        MAP,
        map_bytes,
    )

    create_exclusive(
        MAP_RECEIPT,
        receipt_bytes,
    )

    print()
    print(
        "STAGE2 MAP FROZEN"
    )

    print(
        "map_sha256:",
        map_sha,
    )

    print(
        "map_rows:",
        len(resolved),
    )

    print(
        "IMPORTANT: Stage2 resolver "
        "MUST NEVER RUN AGAIN"
    )


if __name__ == "__main__":
    main()
