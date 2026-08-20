from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx


REPO = Path(__file__).resolve().parents[1]

AMENDMENT = (
    REPO
    / "research"
    / "h5_m1_registration_visibility_amendment.json"
)

M0 = (
    REPO
    / "research"
    / "h5_m0_mechanism_data_design.json"
)

EXCLUSIONS = (
    REPO
    / "research"
    / "h5_m0_exclusions.json"
)

M1A = (
    REPO
    / "research"
    / "h5_m1a_acquisition_contract.json"
)

REGISTRY = (
    REPO
    / "data"
    / "research"
    / "h5_m1"
    / "registry.jsonl"
)

RECEIPT = (
    REPO
    / "research"
    / "h5_m1_registry_freeze.json"
)

EXPECTED_AMENDMENT_SHA = (
    "5cd23a68e1fea185acff187bd9a9556f"
    "c0e86c53d9a5b649a01ca0f74ce27dda"
)

EXPECTED_M0_SHA = (
    "073730512baa587e5be730f534daf1e3"
    "89c8934d301769a77f387e1fff0a553c"
)

EXPECTED_EXCLUSIONS_SHA = (
    "fb4a268f3148e746e31232c658cdc879"
    "3c2e082a73c5a8260af6779d89b1949a"
)

EXPECTED_M1A_SHA = (
    "6da26ac94413b5a79566764dcc43d4584"
    "bff94c3b88531225dec09a4db99b1be"
)

GAMMA = "https://gamma-api.polymarket.com"

TARGET = 10
MINIMUM = 5
MIN_LEAD_SECONDS = 1800

UA = {
    "User-Agent":
        "quant-lab-h5-stage1-registration/1.0"
}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def git_head() -> str:
    return subprocess.check_output(
        [
            "git",
            "rev-parse",
            "HEAD",
        ],
        cwd=REPO,
        text=True,
    ).strip()


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
        hashlib.sha256(
            committed
        ).hexdigest()
        ==
        sha256_file(path)
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
    except ValueError:
        return None

    if dt.tzinfo is None:
        return None

    return dt.astimezone(
        timezone.utc
    )


def jloads_maybe(value: Any):
    if value is None:
        return None

    if isinstance(
        value,
        (list, dict),
    ):
        return value

    try:
        return json.loads(value)
    except (
        json.JSONDecodeError,
        TypeError,
    ):
        return None


def token_list(
    market: dict[str, Any],
) -> list[str]:
    value = jloads_maybe(
        market.get("clobTokenIds")
    )

    if not isinstance(
        value,
        list,
    ):
        return []

    return [
        str(x)
        for x in value
    ]


def outcome_list(
    market: dict[str, Any],
) -> list[str]:
    value = jloads_maybe(
        market.get("outcomes")
    )

    if not isinstance(
        value,
        list,
    ):
        return []

    return [
        str(x)
        for x in value
    ]


def load_frozen_inputs() -> set[str]:
    identities = {
        AMENDMENT:
            EXPECTED_AMENDMENT_SHA,
        M0:
            EXPECTED_M0_SHA,
        EXCLUSIONS:
            EXPECTED_EXCLUSIONS_SHA,
        M1A:
            EXPECTED_M1A_SHA,
    }

    for path, expected in identities.items():
        if not path.is_file():
            raise RuntimeError(
                f"missing frozen input: "
                f"{path}"
            )

        actual = sha256_file(path)

        if actual != expected:
            raise RuntimeError(
                "frozen input SHA mismatch: "
                f"{path}\n"
                f"expected={expected}\n"
                f"actual={actual}"
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

    exclusions = json.loads(
        EXCLUSIONS.read_text(
            encoding="utf-8"
        )
    )

    condition_ids = exclusions[
        "rn1_f19_prospective"
    ]["condition_ids"]

    out = {
        str(x).strip().lower()
        for x in condition_ids
        if str(x).strip()
    }

    if len(out) != 100:
        raise RuntimeError(
            "expected exactly 100 frozen "
            f"F19 conditions, got {len(out)}"
        )

    return out


def mlb_tag_id(
    client: httpx.Client,
) -> str:
    response = client.get(
        f"{GAMMA}/sports"
    )

    response.raise_for_status()

    payload = response.json()

    matches = [
        row
        for row in payload
        if str(
            row.get("sport") or ""
        ).strip().lower()
        == "mlb"
    ]

    if len(matches) != 1:
        raise RuntimeError(
            "expected exactly one MLB "
            "sports metadata record"
        )

    value = matches[0].get(
        "primaryTagId"
    )

    if value is None:
        raise RuntimeError(
            "MLB metadata missing "
            "primaryTagId"
        )

    return str(value)


def fetch_gamma_events(
    client: httpx.Client,
    tag_id: str,
) -> list[dict[str, Any]]:
    out = []

    after_cursor = None
    seen = set()

    for _ in range(100):
        params = {
            "tag_id":
                int(tag_id),
            "closed":
                "false",
            "limit":
                500,
        }

        if after_cursor:
            params[
                "after_cursor"
            ] = after_cursor

        response = client.get(
            f"{GAMMA}/events/keyset",
            params=params,
        )

        response.raise_for_status()

        payload = response.json()

        if not isinstance(
            payload,
            dict,
        ):
            raise RuntimeError(
                "Gamma keyset response "
                "is not an object"
            )

        batch = payload.get(
            "events"
        )

        if not isinstance(
            batch,
            list,
        ):
            raise RuntimeError(
                "Gamma keyset response "
                "missing events"
            )

        out.extend(batch)

        cursor = payload.get(
            "next_cursor"
        )

        if not cursor:
            return out

        cursor = str(cursor)

        if cursor in seen:
            raise RuntimeError(
                "Gamma cursor repeated"
            )

        seen.add(cursor)
        after_cursor = cursor

    raise RuntimeError(
        "Gamma pagination exceeded "
        "100 pages"
    )


def eligible_candidate(
    market: dict[str, Any],
    *,
    registered_at: datetime,
    f19_conditions: set[str],
):
    if (
        market.get(
            "sportsMarketType"
        )
        != "moneyline"
    ):
        return None

    if market.get(
        "active"
    ) is not True:
        return None

    if market.get(
        "closed"
    ) is not False:
        return None

    if market.get(
        "acceptingOrders"
    ) is not True:
        return None

    condition_id = str(
        market.get(
            "conditionId"
        )
        or ""
    ).strip().lower()

    if not condition_id:
        return None

    if condition_id in f19_conditions:
        return None

    tokens = token_list(
        market
    )

    outcomes = outcome_list(
        market
    )

    if (
        len(tokens) != 2
        or
        len(outcomes) != 2
    ):
        return None

    start = parse_dt(
        market.get(
            "gameStartTime"
        )
    )

    if start is None:
        return None

    lead = (
        start
        -
        registered_at
    ).total_seconds()

    if lead < MIN_LEAD_SECONDS:
        return None

    gamma_market_id = str(
        market.get("id")
        or ""
    ).strip()

    if not gamma_market_id:
        return None

    return {
        "condition_id":
            condition_id,

        "gamma_market_id":
            gamma_market_id,

        "gamma_slug":
            (
                str(
                    market.get("slug")
                )
                if market.get("slug")
                else None
            ),

        "gamma_question":
            str(
                market.get("question")
                or ""
            ),

        "pm_game_start_time":
            start,

        "registration_lead_seconds":
            int(lead),

        "p1_team":
            outcomes[0],

        "p0_team":
            outcomes[1],

        "p1_token_id":
            tokens[0],

        "p0_token_id":
            tokens[1],
    }


def select_candidates(
    events: list[dict[str, Any]],
    *,
    registered_at: datetime,
    f19_conditions: set[str],
):
    candidates = {}

    for event in events:
        if not isinstance(
            event,
            dict,
        ):
            continue

        for market in (
            event.get("markets")
            or []
        ):
            if not isinstance(
                market,
                dict,
            ):
                continue

            candidate = eligible_candidate(
                market,
                registered_at=registered_at,
                f19_conditions=f19_conditions,
            )

            if candidate is None:
                continue

            candidates[
                candidate[
                    "condition_id"
                ]
            ] = candidate

    ordered = sorted(
        candidates.values(),
        key=lambda x: (
            x[
                "pm_game_start_time"
            ],
            x[
                "condition_id"
            ],
        ),
    )

    return ordered[:TARGET]


def canonical_rows(
    selected,
    *,
    registered_at: datetime,
    registrar_sha: str,
):
    rows = []

    for index, market in enumerate(
        selected,
        start=1,
    ):
        rows.append({
            "study":
                "H5",

            "milestone":
                "H5-M1",

            "registration_stage":
                "STAGE1_CONDITION_FREEZE",

            "registration_index":
                index,

            "registered_at_utc":
                registered_at.isoformat(),

            "condition_id":
                market[
                    "condition_id"
                ],

            "gamma_market_id":
                market[
                    "gamma_market_id"
                ],

            "gamma_slug":
                market[
                    "gamma_slug"
                ],

            "gamma_question":
                market[
                    "gamma_question"
                ],

            "pm_game_start_time":
                market[
                    "pm_game_start_time"
                ].isoformat(),

            "registration_lead_seconds":
                market[
                    "registration_lead_seconds"
                ],

            "p1_team":
                market["p1_team"],

            "p0_team":
                market["p0_team"],

            "p1_token_id":
                market[
                    "p1_token_id"
                ],

            "p0_token_id":
                market[
                    "p0_token_id"
                ],

            "sports_market_type":
                "moneyline",

            "selection_basis":
                (
                    "first_10_gamma_metadata_only_"
                    "eligible_non_f19"
                ),

            "external_identity_status":
                "PENDING_PROVIDER_VISIBILITY",

            "odds_game_id":
                None,

            "amendment_sha256":
                EXPECTED_AMENDMENT_SHA,

            "registrar_sha256":
                registrar_sha,
        })

    return rows


def serialize_rows(rows) -> bytes:
    return "".join(
        json.dumps(
            row,
            sort_keys=True,
            separators=(
                ",",
                ":",
            ),
            ensure_ascii=False,
        )
        + "\n"
        for row in rows
    ).encode("utf-8")


def create_exclusive(
    path: Path,
    data: bytes,
):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
    )

    fd = os.open(
        path,
        flags,
        0o644,
    )

    try:
        with os.fdopen(
            fd,
            "wb",
        ) as fh:
            fh.write(data)
            fh.flush()
            os.fsync(
                fh.fileno()
            )
    except Exception:
        raise


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--freeze",
        action="store_true",
        help=(
            "Create the immutable Stage-1 "
            "registry and freeze receipt."
        ),
    )

    args = parser.parse_args()

    if REGISTRY.exists():
        raise RuntimeError(
            "H5 registry already exists; "
            "registrar MUST NOT be re-run"
        )

    if RECEIPT.exists():
        raise RuntimeError(
            "H5 registry freeze receipt "
            "already exists"
        )

    f19_conditions = (
        load_frozen_inputs()
    )

    registrar_sha = sha256_file(
        Path(__file__).resolve()
    )

    if (
        args.freeze
        and not
        committed_self_matches_head()
    ):
        raise RuntimeError(
            "refusing live freeze: "
            "registrar is not byte-identical "
            "to version committed at HEAD"
        )

    registered_at = utcnow()

    with httpx.Client(
        headers=UA,
        timeout=30,
    ) as client:
        tag_id = mlb_tag_id(
            client
        )

        events = fetch_gamma_events(
            client,
            tag_id,
        )

    selected = select_candidates(
        events,
        registered_at=registered_at,
        f19_conditions=f19_conditions,
    )

    print("=" * 72)
    print(
        "H5-M1 STAGE-1 CONDITION REGISTRATION"
    )
    print(
        "GAMMA METADATA ONLY"
    )
    print("=" * 72)

    print(
        "registered_at_utc:",
        registered_at.isoformat(),
    )

    print(
        "registrar_sha256:",
        registrar_sha,
    )

    print(
        "amendment_sha256:",
        EXPECTED_AMENDMENT_SHA,
    )

    print(
        "eligible selected:",
        len(selected),
    )

    print()

    for index, market in enumerate(
        selected,
        start=1,
    ):
        print(
            index,
            "|",
            market[
                "pm_game_start_time"
            ].isoformat(),
            "|",
            market[
                "gamma_slug"
            ],
            "|",
            market[
                "condition_id"
            ],
        )

    print()
    print(
        "odds-bearing endpoint used: NO"
    )
    print(
        "external consensus calculated: NO"
    )
    print(
        "Polymarket prices inspected: NO"
    )
    print(
        "F19 trade content read: NO"
    )
    print(
        "F20 read: NO"
    )
    print(
        "H5 innovation calculated: NO"
    )
    print(
        "PM response calculated: NO"
    )
    print(
        "PnL calculated: NO"
    )

    if len(selected) < MINIMUM:
        raise RuntimeError(
            "fewer than 5 eligible "
            "Stage-1 H5 markets"
        )

    if not args.freeze:
        print()
        print(
            "PREVIEW ONLY — "
            "registry written: NO"
        )
        return

    rows = canonical_rows(
        selected,
        registered_at=registered_at,
        registrar_sha=registrar_sha,
    )

    registry_bytes = (
        serialize_rows(rows)
    )

    registry_sha = (
        sha256_bytes(
            registry_bytes
        )
    )

    condition_ids = [
        row["condition_id"]
        for row in rows
    ]

    if (
        len(condition_ids)
        !=
        len(set(condition_ids))
    ):
        raise RuntimeError(
            "duplicate condition_id "
            "in selected registry"
        )

    create_exclusive(
        REGISTRY,
        registry_bytes,
    )

    receipt = {
        "study":
            "H5",

        "milestone":
            "H5-M1",

        "status":
            "STAGE1_CONDITION_REGISTRY_FROZEN",

        "frozen_at_utc":
            registered_at.isoformat(),

        "registry_path":
            str(
                REGISTRY.relative_to(
                    REPO
                )
            ),

        "registry_sha256":
            registry_sha,

        "row_count":
            len(rows),

        "distinct_condition_ids":
            len(set(condition_ids)),

        "condition_ids":
            condition_ids,

        "registrar_path":
            str(
                Path(__file__)
                .resolve()
                .relative_to(REPO)
            ),

        "registrar_sha256":
            registrar_sha,

        "git_head":
            git_head(),

        "amendment_sha256":
            EXPECTED_AMENDMENT_SHA,

        "m0_sha256":
            EXPECTED_M0_SHA,

        "exclusions_sha256":
            EXPECTED_EXCLUSIONS_SHA,

        "m1a_sha256":
            EXPECTED_M1A_SHA,

        "external_identity_status":
            "PENDING_PROVIDER_VISIBILITY",

        "registry_mutable":
            False,

        "h6_exclusion_condition_set_final":
            True,

        "analysis_boundary": {
            "external_odds_read":
                False,
            "external_consensus_calculated":
                False,
            "h5_innovation_calculated":
                False,
            "pm_response_calculated":
                False,
            "f19_trade_content_read":
                False,
            "f20_read":
                False,
            "markout_calculated":
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
    ).encode("utf-8")

    create_exclusive(
        RECEIPT,
        receipt_bytes,
    )

    print()
    print(
        "STAGE-1 REGISTRY FROZEN"
    )

    print(
        "registry_sha256:",
        registry_sha,
    )

    print(
        "registry rows:",
        len(rows),
    )

    print(
        "receipt:",
        RECEIPT.relative_to(
            REPO
        ),
    )


if __name__ == "__main__":
    main()
