from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import psycopg2
from dotenv import dotenv_values


REPO = Path(__file__).resolve().parents[1]

AMENDMENT = (
    REPO
    / "research"
    / "h5_m1_registration_visibility_amendment.json"
)

STAGE1_REGISTRY = (
    REPO
    / "data"
    / "research"
    / "h5_m1"
    / "registry.jsonl"
)

STAGE1_RECEIPT = (
    REPO
    / "research"
    / "h5_m1_registry_freeze.json"
)

EXCLUSIONS = (
    REPO
    / "research"
    / "h5_m0_exclusions.json"
)

MAP = (
    REPO
    / "data"
    / "research"
    / "h5_m1"
    / "external_id_map.jsonl"
)

MAP_RECEIPT = (
    REPO
    / "research"
    / "h5_m1_external_id_map_freeze.json"
)

EXPECTED_AMENDMENT_SHA = (
    "5cd23a68e1fea185acff187bd9a9556f"
    "c0e86c53d9a5b649a01ca0f74ce27dda"
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

EVENTS_URL = (
    "https://api.the-odds-api.com/"
    "v4/sports/baseball_mlb/events"
)

MAX_START_DELTA_SECONDS = 600
MIN_NONBURNED_TO_FREEZE = 5

UA = {
    "User-Agent":
        "quant-lab-h5-stage2-resolution/1.0"
}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def sha256_file(path: Path) -> str:
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(
        data
    ).hexdigest()


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


def committed_self_matches_head() -> bool:
    path = Path(__file__).resolve()

    relative = (
        path.relative_to(REPO)
        .as_posix()
    )

    try:
        committed = subprocess.check_output(
            [
                "git",
                "show",
                f"HEAD:{relative}",
            ],
            cwd=REPO,
        )
    except subprocess.CalledProcessError:
        return False

    return (
        sha256_bytes(committed)
        ==
        sha256_file(path)
    )


def git_head() -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO,
        text=True,
    ).strip()


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


def load_stage1():
    verify_sha(
        AMENDMENT,
        EXPECTED_AMENDMENT_SHA,
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
        EXCLUSIONS,
        EXPECTED_EXCLUSIONS_SHA,
    )

    amendment = json.loads(
        AMENDMENT.read_text(
            encoding="utf-8"
        )
    )

    if (
        amendment.get("status")
        !=
        "PREREGISTERED_ENGINEERING_AMENDMENT"
    ):
        raise RuntimeError(
            "unexpected amendment status"
        )

    receipt = json.loads(
        STAGE1_RECEIPT.read_text(
            encoding="utf-8"
        )
    )

    if (
        receipt.get("status")
        !=
        "STAGE1_CONDITION_REGISTRY_FROZEN"
    ):
        raise RuntimeError(
            "Stage-1 registry not frozen"
        )

    rows = [
        json.loads(line)
        for line in STAGE1_REGISTRY.read_text(
            encoding="utf-8"
        ).splitlines()
        if line.strip()
    ]

    if len(rows) != 10:
        raise RuntimeError(
            f"expected 10 Stage-1 rows, got {len(rows)}"
        )

    conditions = []

    for i, row in enumerate(
        rows,
        start=1,
    ):
        if (
            row.get("registration_index")
            != i
        ):
            raise RuntimeError(
                "Stage-1 registration "
                "index/order mismatch"
            )

        condition = str(
            row.get("condition_id")
            or ""
        ).strip().lower()

        if not condition:
            raise RuntimeError(
                "empty Stage-1 condition_id"
            )

        if (
            row.get("odds_game_id")
            is not None
        ):
            raise RuntimeError(
                "Stage-1 registry unexpectedly "
                "contains odds_game_id"
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
                "invalid Stage-1 start time"
            )

        conditions.append(
            condition
        )

    if len(set(conditions)) != 10:
        raise RuntimeError(
            "Stage-1 condition IDs "
            "are not unique"
        )

    receipt_conditions = {
        str(x).strip().lower()
        for x in receipt.get(
            "condition_ids",
            [],
        )
    }

    if (
        receipt_conditions
        != set(conditions)
    ):
        raise RuntimeError(
            "Stage-1 receipt/registry "
            "condition mismatch"
        )

    return rows


def connect_identity_db():

    env = dotenv_values(
        REPO / ".env"
    )

    password = env.get(
        "PG_PASSWORD"
    )

    if not password:
        raise RuntimeError(
            "PG_PASSWORD missing"
        )

    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="quantlab",
        user="quantlab",
        password=password,
    )

    conn.set_session(
        readonly=True,
        autocommit=False,
    )

    return conn


def burned_external_ids() -> set[str]:

    exclusions = json.loads(
        EXCLUSIONS.read_text(
            encoding="utf-8"
        )
    )

    legacy_ids = [
        int(x)
        for x in exclusions[
            "h2_v2_burned_development"
        ]["odds_game_ids"]
    ]

    if (
        len(legacy_ids) != 9
        or len(set(legacy_ids)) != 9
    ):
        raise RuntimeError(
            "expected exactly 9 "
            "H2 burned legacy IDs"
        )

    conn = connect_identity_db()

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    oddsapi_id
                FROM odds_games
                WHERE id = ANY(%s)
                ORDER BY id
                """,
                (legacy_ids,),
            )

            rows = cur.fetchall()

    finally:
        conn.rollback()
        conn.close()

    if len(rows) != 9:
        raise RuntimeError(
            "could not resolve all "
            "9 H2 external identities"
        )

    ids = {
        str(row[1])
        for row in rows
        if row[1]
    }

    if len(ids) != 9:
        raise RuntimeError(
            "H2 external identities "
            "not complete/unique"
        )

    return ids


def fetch_provider_events(
    *,
    api_key: str,
    http: httpx.Client,
):

    response = http.get(
        EVENTS_URL,
        params={
            "apiKey":
                api_key,

            "dateFormat":
                "iso",
        },
    )

    # Do not raise_for_status:
    # avoid a secret-bearing URL in an exception.
    if response.status_code != 200:
        raise RuntimeError(
            "Odds /events request failed: "
            f"HTTP {response.status_code}; "
            f"body={response.text[:300]!r}"
        )

    payload = response.json()

    if not isinstance(
        payload,
        list,
    ):
        raise RuntimeError(
            "Odds /events response "
            "is not a list"
        )

    events = []

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

        events.append({
            "odds_game_id":
                event_id,

            "canonical_commence_time":
                commence,

            "home_team":
                home,

            "away_team":
                away,

            "team_set": {
                norm_team(home),
                norm_team(away),
            },
        })

    return events


def resolve_rows(
    stage1_rows,
    provider_events,
    burned_ids,
    *,
    resolved_at: datetime,
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

        if not candidates:
            statuses.append({
                "registration_index":
                    row[
                        "registration_index"
                    ],

                "condition_id":
                    row["condition_id"],

                "gamma_slug":
                    row["gamma_slug"],

                "status":
                    "PENDING_PROVIDER_VISIBILITY",
            })

            continue

        if len(candidates) > 1:
            statuses.append({
                "registration_index":
                    row[
                        "registration_index"
                    ],

                "condition_id":
                    row["condition_id"],

                "gamma_slug":
                    row["gamma_slug"],

                "status":
                    "AMBIGUOUS_FAIL_CLOSED",

                "candidate_count":
                    len(candidates),
            })

            continue

        event, delta = candidates[0]

        burned = (
            event["odds_game_id"]
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
                "H5-M1",

            "registration_stage":
                "STAGE2_EXTERNAL_IDENTITY",

            "registration_index":
                row[
                    "registration_index"
                ],

            "condition_id":
                str(
                    row["condition_id"]
                ).lower(),

            "gamma_market_id":
                str(
                    row[
                        "gamma_market_id"
                    ]
                ),

            "gamma_slug":
                row["gamma_slug"],

            "p1_team":
                row["p1_team"],

            "p0_team":
                row["p0_team"],

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

            "mapping_rule": (
                "exact_normalized_MLB_team_set"
                "+unique_provider_event"
                "+abs_start_delta_le_600s"
            ),
        }

        resolved.append(
            result
        )

        statuses.append({
            "registration_index":
                row[
                    "registration_index"
                ],

            "condition_id":
                row["condition_id"],

            "gamma_slug":
                row["gamma_slug"],

            "status":
                status,

            "odds_game_id":
                event[
                    "odds_game_id"
                ],

            "start_delta_seconds":
                delta,
        })

    return resolved, statuses


def serialize_rows(rows) -> bytes:

    ordered = sorted(
        rows,
        key=lambda x:
            x["registration_index"],
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
    ).encode(
        "utf-8"
    )


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


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--freeze",
        action="store_true",
        help=(
            "Freeze the current resolved "
            "Stage-2 external identity map. "
            "Irreversible."
        ),
    )

    args = parser.parse_args()

    if MAP.exists():
        raise RuntimeError(
            "H5 Stage-2 map already exists; "
            "resolver MUST NOT be re-run"
        )

    if MAP_RECEIPT.exists():
        raise RuntimeError(
            "H5 Stage-2 map receipt "
            "already exists"
        )

    stage1 = load_stage1()

    burned = (
        burned_external_ids()
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

    now = utcnow()

    with httpx.Client(
        timeout=20,
        headers=UA,
    ) as http:

        provider_events = (
            fetch_provider_events(
                api_key=api_key,
                http=http,
            )
        )

    resolved, statuses = (
        resolve_rows(
            stage1,
            provider_events,
            burned,
            resolved_at=now,
        )
    )

    nonburned = [
        x
        for x in resolved
        if (
            x["resolution_status"]
            ==
            "RESOLVED_NONBURNED"
        )
    ]

    burned_rows = [
        x
        for x in resolved
        if (
            x["resolution_status"]
            ==
            "RESOLVED_H2_BURNED_PROHIBITED"
        )
    ]

    ambiguous = [
        x
        for x in statuses
        if (
            x["status"]
            ==
            "AMBIGUOUS_FAIL_CLOSED"
        )
    ]

    pending = [
        x
        for x in statuses
        if (
            x["status"]
            ==
            "PENDING_PROVIDER_VISIBILITY"
        )
    ]

    print("=" * 78)
    print(
        "H5-M1 STAGE-2 "
        "EXTERNAL IDENTITY RESOLUTION"
    )
    print(
        "NON-ODDS PROVIDER EVENTS ONLY"
    )
    print("=" * 78)

    print(
        "resolved_at_utc:",
        now.isoformat(),
    )

    print(
        "provider events visible:",
        len(provider_events),
    )

    print()

    for item in statuses:

        line = [
            str(
                item[
                    "registration_index"
                ]
            ),
            "|",
            str(
                item[
                    "gamma_slug"
                ]
            ),
            "|",
            item["status"],
        ]

        if "odds_game_id" in item:
            line += [
                "|",
                item["odds_game_id"],
                "| delta_s:",
                str(
                    item[
                        "start_delta_seconds"
                    ]
                ),
            ]

        print(
            " ".join(line)
        )

    print()
    print("SUMMARY")

    print(
        "resolved nonburned:",
        len(nonburned),
    )

    print(
        "resolved H2-burned:",
        len(burned_rows),
    )

    print(
        "pending visibility:",
        len(pending),
    )

    print(
        "ambiguous:",
        len(ambiguous),
    )

    gate = (
        len(nonburned)
        >=
        MIN_NONBURNED_TO_FREEZE
        and
        len(ambiguous) == 0
    )

    print(
        "minimum acquisition gate >=5:",
        gate,
    )

    print()
    print("ANALYSIS BOUNDARY")
    print(
        "odds-bearing endpoint used: NO"
    )
    print(
        "external odds read: NO"
    )
    print(
        "external consensus calculated: NO"
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
        "F19 trade content read: NO"
    )
    print(
        "F20 read: NO"
    )
    print(
        "winner/settlement used: NO"
    )
    print(
        "PnL calculated: NO"
    )

    if not args.freeze:
        print()
        print(
            "PREVIEW ONLY — "
            "external ID map written: NO"
        )
        return

    if not committed_self_matches_head():
        raise RuntimeError(
            "refusing Stage-2 freeze: "
            "resolver differs from "
            "committed HEAD"
        )

    if ambiguous:
        raise RuntimeError(
            "refusing Stage-2 freeze: "
            "ambiguous mapping exists"
        )

    if (
        len(nonburned)
        <
        MIN_NONBURNED_TO_FREEZE
    ):
        raise RuntimeError(
            "refusing Stage-2 freeze: "
            f"only {len(nonburned)} "
            "resolved nonburned markets"
        )

    # Freeze only identities that are actually
    # resolved at this boundary.
    # Stage-1 registry is never modified.
    map_bytes = serialize_rows(
        resolved
    )

    map_sha = (
        sha256_bytes(
            map_bytes
        )
    )

    create_exclusive(
        MAP,
        map_bytes,
    )

    receipt = {
        "study":
            "H5",

        "milestone":
            "H5-M1",

        "status":
            "STAGE2_EXTERNAL_ID_MAP_FROZEN",

        "frozen_at_utc":
            now.isoformat(),

        "git_commit":
            git_head(),

        "resolver": {
            "path":
                "research/h5_m1_stage2_resolve.py",

            "sha256":
                sha256_file(
                    Path(__file__).resolve()
                ),
        },

        "amendment_sha256":
            EXPECTED_AMENDMENT_SHA,

        "stage1_registry": {
            "path":
                "data/research/h5_m1/registry.jsonl",

            "sha256":
                EXPECTED_STAGE1_REGISTRY_SHA,

            "row_count":
                10,
        },

        "stage1_receipt_sha256":
            EXPECTED_STAGE1_RECEIPT_SHA,

        "h2_exclusions_sha256":
            EXPECTED_EXCLUSIONS_SHA,

        "external_id_map": {
            "path":
                "data/research/h5_m1/external_id_map.jsonl",

            "sha256":
                map_sha,

            "row_count":
                len(resolved),

            "resolved_nonburned_count":
                len(nonburned),

            "resolved_h2_burned_count":
                len(burned_rows),

            "pending_stage1_count":
                len(pending),

            "ambiguous_count":
                len(ambiguous),

            "mutable":
                False,
        },

        "nonburned_condition_ids": [
            x["condition_id"]
            for x in nonburned
        ],

        "prohibited_h2_burned_condition_ids": [
            x["condition_id"]
            for x in burned_rows
        ],

        "analysis_boundary": {
            "odds_bearing_endpoint_used":
                False,

            "external_odds_read":
                False,

            "external_consensus_calculated":
                False,

            "pm_prices_read":
                False,

            "h5_innovation_calculated":
                False,

            "pm_response_calculated":
                False,

            "f19_trade_content_read":
                False,

            "f20_read":
                False,

            "winner_used":
                False,

            "settlement_used":
                False,

            "pnl_calculated":
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
    ).encode(
        "utf-8"
    )

    create_exclusive(
        MAP_RECEIPT,
        receipt_bytes,
    )

    print()
    print(
        "H5 STAGE-2 EXTERNAL "
        "IDENTITY MAP FROZEN"
    )

    print(
        "map_sha256:",
        map_sha,
    )


if __name__ == "__main__":
    main()
