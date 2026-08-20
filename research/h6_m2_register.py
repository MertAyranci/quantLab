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


REPO = Path(__file__).resolve().parents[1]

CONTRACT = REPO / "research" / "h6_m2_development_contract.json"
H2_EXCLUSIONS = REPO / "research" / "h6_m2_h2_identity_exclusions.json"
H5_RECEIPT = REPO / "research" / "h5_m1_registry_freeze.json"

F19_REGISTRY = REPO / "data" / "prospective" / "rn1_f19" / "registry.jsonl"
H6_M1_REGISTRY = REPO / "data" / "research" / "h6" / "m1" / "registry.jsonl"
H5_REGISTRY = REPO / "data" / "research" / "h5_m1" / "registry.jsonl"

REGISTRY = REPO / "data" / "research" / "h6" / "m2" / "registry.jsonl"
RECEIPT = REPO / "research" / "h6_m2_registry_freeze.json"

EXPECTED_CONTRACT_SHA = (
    "b33934981bc945d36247b013ba68f85a"
    "909a8185f2e55f179ab8ae025adbeb38"
)

EXPECTED_H2_EXCLUSIONS_SHA = (
    "0907e1bd4ccc2aaaf7cef332e7fe96b8"
    "fb05355afdfc2e912d412e2f10c78d73"
)

EXPECTED_F19_SHA = (
    "e284e82699a60cae080ef100f095e1cb"
    "358a7dd85c2bc7c1fcbf06a25c68405b"
)

EXPECTED_H6_M1_SHA = (
    "8484176202fff2f81974b698711145d10"
    "0fda156c6598d5e734b3c6789850217"
)

EXPECTED_H5_REGISTRY_SHA = (
    "c73e65dea1ba76eea40f1d724aa7d963"
    "81c06db4cb77e411df2213ce8f7cf95b"
)

EXPECTED_H5_RECEIPT_SHA = (
    "61c96d789b4c3cc39984889be9209cb9"
    "90022ee20f9e3c985551dd6cf52874c2"
)

SPORTS_URL = "https://gamma-api.polymarket.com/sports"
EVENTS_URL = "https://gamma-api.polymarket.com/events"

SPORT = "mlb"
TARGET_MARKETS = 30
REGISTRATION_LEAD_SECONDS = 3600
PAGE_LIMIT = 100
MAX_EVENT_PAGES = 20

MAPPING_RULE = (
    "gamma_mlb_primary_tag+moneyline+2_outcomes+2_tokens+"
    "active_open_accepting+question_outcome_team_set_match"
)

UA = {
    "User-Agent":
        "quant-lab-h6-m2-registration/1.0"
}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def sha256_file(path: Path) -> str:
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


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
            f"frozen input SHA mismatch: {path}\n"
            f"expected={expected}\n"
            f"actual={actual}"
        )


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
            [
                "git",
                "show",
                f"HEAD:{rel}",
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
    except (TypeError, ValueError):
        return None

    if dt.tzinfo is None:
        return None

    return dt.astimezone(timezone.utc)


def jloads_maybe(value: Any):
    if isinstance(value, (list, dict)):
        return value

    if value is None:
        return None

    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return None


def norm_team(name: Any) -> str:
    text = str(name or "").strip().lower()

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
        "oakland athletics": "athletics",
        "athletics": "athletics",
    }

    return aliases.get(text, text)


def parse_matchup(
    question: Any,
) -> set[str] | None:

    if not question:
        return None

    match = re.match(
        r"^\s*(.+?)\s+vs\.?\s+(.+?)\s*$",
        str(question),
        flags=re.I,
    )

    if not match:
        return None

    return {
        norm_team(match.group(1)),
        norm_team(match.group(2)),
    }


def tokens(
    market: dict[str, Any],
) -> list[str]:

    raw = jloads_maybe(
        market.get("clobTokenIds")
    )

    if not isinstance(raw, list):
        return []

    return [
        str(x)
        for x in raw
    ]


def outcomes(
    market: dict[str, Any],
) -> list[str]:

    raw = jloads_maybe(
        market.get("outcomes")
    )

    if not isinstance(raw, list):
        return []

    return [
        str(x)
        for x in raw
    ]


def load_jsonl_conditions(
    path: Path,
    *,
    expected_sha: str,
    expected_rows: int,
) -> set[str]:

    verify_sha(
        path,
        expected_sha,
    )

    rows = [
        json.loads(line)
        for line in path.read_text(
            encoding="utf-8"
        ).splitlines()
        if line.strip()
    ]

    if len(rows) != expected_rows:
        raise RuntimeError(
            f"{path}: expected "
            f"{expected_rows} rows, "
            f"got {len(rows)}"
        )

    conditions = []

    for row in rows:
        condition = str(
            row.get("condition_id")
            or ""
        ).strip().lower()

        if not condition:
            raise RuntimeError(
                f"{path}: null/empty condition_id"
            )

        conditions.append(condition)

    if len(set(conditions)) != expected_rows:
        raise RuntimeError(
            f"{path}: condition IDs "
            "are not unique"
        )

    return set(conditions)


def load_frozen_exclusions():
    verify_sha(
        CONTRACT,
        EXPECTED_CONTRACT_SHA,
    )

    verify_sha(
        H2_EXCLUSIONS,
        EXPECTED_H2_EXCLUSIONS_SHA,
    )

    verify_sha(
        H5_RECEIPT,
        EXPECTED_H5_RECEIPT_SHA,
    )

    contract = json.loads(
        CONTRACT.read_text(
            encoding="utf-8"
        )
    )

    if (
        contract.get("status")
        != "DEVELOPMENT_CONTRACT_FREEZE"
    ):
        raise RuntimeError(
            "unexpected H6-M2 contract status"
        )

    if (
        contract["registration"][
            "target_markets"
        ]
        != TARGET_MARKETS
    ):
        raise RuntimeError(
            "H6-M2 target market mismatch"
        )

    if (
        contract["registration"][
            "registrar_operational_lead_seconds"
        ]
        != REGISTRATION_LEAD_SECONDS
    ):
        raise RuntimeError(
            "registration lead mismatch"
        )

    f19 = load_jsonl_conditions(
        F19_REGISTRY,
        expected_sha=EXPECTED_F19_SHA,
        expected_rows=100,
    )

    h6_m1 = load_jsonl_conditions(
        H6_M1_REGISTRY,
        expected_sha=EXPECTED_H6_M1_SHA,
        expected_rows=5,
    )

    h5 = load_jsonl_conditions(
        H5_REGISTRY,
        expected_sha=EXPECTED_H5_REGISTRY_SHA,
        expected_rows=10,
    )

    h5_receipt = json.loads(
        H5_RECEIPT.read_text(
            encoding="utf-8"
        )
    )

    if (
        h5_receipt.get("status")
        != "STAGE1_CONDITION_REGISTRY_FROZEN"
    ):
        raise RuntimeError(
            "H5 registry is not frozen"
        )

    receipt_h5 = {
        str(x).strip().lower()
        for x in h5_receipt.get(
            "condition_ids",
            [],
        )
    }

    if receipt_h5 != h5:
        raise RuntimeError(
            "H5 receipt/registry "
            "condition set mismatch"
        )

    h2_payload = json.loads(
        H2_EXCLUSIONS.read_text(
            encoding="utf-8"
        )
    )

    if (
        h2_payload.get("status")
        != "IDENTITY_EXCLUSION_FREEZE"
    ):
        raise RuntimeError(
            "H2 identity exclusion "
            "artifact not frozen"
        )

    h2 = {
        str(x).strip().lower()
        for x in h2_payload.get(
            "condition_ids",
            [],
        )
    }

    if len(h2) != 9:
        raise RuntimeError(
            f"expected 9 H2 exclusions, "
            f"got {len(h2)}"
        )

    sets = {
        "F19": f19,
        "H6_M1": h6_m1,
        "H5": h5,
        "H2_V2": h2,
    }

    union = set().union(
        *sets.values()
    )

    overlaps = {}

    names = list(sets)

    for i, left in enumerate(names):
        for right in names[i + 1:]:
            overlap = (
                sets[left]
                &
                sets[right]
            )

            overlaps[
                f"{left}__{right}"
            ] = sorted(overlap)

    return {
        "sets": sets,
        "union": union,
        "overlaps": overlaps,
    }


def safe_get_json(
    client: httpx.Client,
    url: str,
    *,
    params: dict[str, Any] | None = None,
):
    response = client.get(
        url,
        params=params,
    )

    if response.status_code != 200:
        raise RuntimeError(
            "Gamma metadata request failed: "
            f"HTTP {response.status_code}; "
            f"body={response.text[:300]!r}"
        )

    return response.json()


def resolve_mlb_tag(
    client: httpx.Client,
) -> str:

    payload = safe_get_json(
        client,
        SPORTS_URL,
    )

    if not isinstance(payload, list):
        raise RuntimeError(
            "/sports response is not a list"
        )

    matches = [
        row
        for row in payload
        if isinstance(row, dict)
        and str(
            row.get("sport")
            or ""
        ).strip().lower()
        == SPORT
    ]

    if len(matches) != 1:
        raise RuntimeError(
            "expected exactly one MLB "
            f"sports row, got {len(matches)}"
        )

    tag = str(
        matches[0].get(
            "primaryTagId"
        )
        or ""
    ).strip()

    if not tag.isdigit():
        raise RuntimeError(
            "MLB primaryTagId invalid"
        )

    return tag


def fetch_active_mlb_events(
    client: httpx.Client,
    *,
    tag_id: str,
) -> list[dict[str, Any]]:

    all_events = []

    for page in range(
        MAX_EVENT_PAGES
    ):
        payload = safe_get_json(
            client,
            EVENTS_URL,
            params={
                "tag_id": tag_id,
                "active": "true",
                "closed": "false",
                "limit": PAGE_LIMIT,
                "offset":
                    page * PAGE_LIMIT,
            },
        )

        if not isinstance(
            payload,
            list,
        ):
            raise RuntimeError(
                "/events response "
                "is not a list"
            )

        all_events.extend(
            row
            for row in payload
            if isinstance(row, dict)
        )

        if len(payload) < PAGE_LIMIT:
            break

    else:
        raise RuntimeError(
            "MLB pagination exceeded "
            "frozen safety bound"
        )

    return all_events


def canonical_candidate(
    *,
    gamma_event: dict[str, Any],
    market: dict[str, Any],
    now: datetime,
) -> dict[str, Any] | None:

    if (
        market.get(
            "sportsMarketType"
        )
        != "moneyline"
    ):
        return None

    if market.get("active") is not True:
        return None

    if market.get("closed") is not False:
        return None

    if (
        market.get("acceptingOrders")
        is not True
    ):
        return None

    toks = tokens(market)
    outs = outcomes(market)

    if (
        len(toks) != 2
        or len(outs) != 2
    ):
        return None

    if len(set(toks)) != 2:
        return None

    question_set = parse_matchup(
        market.get("question")
    )

    outcome_set = {
        norm_team(outs[0]),
        norm_team(outs[1]),
    }

    if (
        question_set is None
        or question_set != outcome_set
    ):
        return None

    start = parse_dt(
        market.get("gameStartTime")
    )

    if start is None:
        return None

    if (
        start
        <
        now
        + timedelta(
            seconds=
                REGISTRATION_LEAD_SECONDS
        )
    ):
        return None

    condition = str(
        market.get("conditionId")
        or ""
    ).strip().lower()

    market_id = str(
        market.get("id")
        or ""
    ).strip()

    event_id = str(
        gamma_event.get("id")
        or ""
    ).strip()

    if (
        not condition
        or not market_id
        or not event_id
    ):
        return None

    return {
        "gamma_event_id":
            event_id,

        "gamma_event_slug":
            (
                str(
                    gamma_event.get(
                        "slug"
                    )
                )
                if gamma_event.get(
                    "slug"
                )
                else None
            ),

        "gamma_market_id":
            market_id,

        "condition_id":
            condition,

        "gamma_market_slug":
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

        "game_start_time_utc":
            start.isoformat(),

        "p1_outcome":
            outs[0],

        "p0_outcome":
            outs[1],

        "p1_token_id":
            toks[0],

        "p0_token_id":
            toks[1],

        "mapping_rule":
            MAPPING_RULE,
    }


def select_candidates(
    events: list[dict[str, Any]],
    *,
    now: datetime,
    exclusion_sets: dict[
        str,
        set[str],
    ],
):
    eligible = {}

    excluded_hits = {
        name: 0
        for name in exclusion_sets
    }

    any_excluded = set()

    for event in events:
        markets = (
            event.get("markets")
            or []
        )

        if not isinstance(
            markets,
            list,
        ):
            continue

        for market in markets:
            if not isinstance(
                market,
                dict,
            ):
                continue

            row = canonical_candidate(
                gamma_event=event,
                market=market,
                now=now,
            )

            if row is None:
                continue

            condition = row[
                "condition_id"
            ]

            hits = [
                name
                for name, values
                in exclusion_sets.items()
                if condition in values
            ]

            if hits:
                any_excluded.add(
                    condition
                )

                for name in hits:
                    excluded_hits[
                        name
                    ] += 1

                continue

            if condition in eligible:
                raise RuntimeError(
                    "duplicate condition_id "
                    "in Gamma discovery: "
                    f"{condition}"
                )

            eligible[
                condition
            ] = row

    ordered = sorted(
        eligible.values(),
        key=lambda row: (
            row[
                "game_start_time_utc"
            ],
            row[
                "condition_id"
            ],
        ),
    )

    selected = ordered[
        :TARGET_MARKETS
    ]

    if len(selected) < TARGET_MARKETS:
        raise RuntimeError(
            "insufficient eligible "
            "H6-M2 markets: "
            f"{len(selected)} "
            f"< {TARGET_MARKETS}"
        )

    token_ids = [
        token
        for row in selected
        for token in (
            row["p1_token_id"],
            row["p0_token_id"],
        )
    ]

    if (
        len(token_ids)
        != len(set(token_ids))
    ):
        raise RuntimeError(
            "duplicate token identity "
            "in selected H6-M2 registry"
        )

    selected_conditions = {
        row["condition_id"]
        for row in selected
    }

    exclusion_union = (
        set().union(
            *exclusion_sets.values()
        )
    )

    overlap = (
        selected_conditions
        &
        exclusion_union
    )

    if overlap:
        raise RuntimeError(
            "selected H6-M2 cohort "
            "overlaps frozen exclusions: "
            f"{sorted(overlap)}"
        )

    return {
        "selected": selected,
        "eligible_after_exclusions":
            len(ordered),
        "excluded_unique_conditions":
            len(any_excluded),
        "excluded_hits":
            excluded_hits,
    }


def serialize_registry(
    rows,
    *,
    registered_at: datetime,
) -> bytes:

    payload = []

    for index, row in enumerate(
        rows,
        start=1,
    ):
        payload.append({
            "study":
                "H6",

            "milestone":
                "H6-M2",

            "registration_index":
                index,

            "registered_at_utc":
                registered_at.isoformat(),

            **row,
        })

    text = "".join(
        json.dumps(
            row,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        + "\n"
        for row in payload
    )

    return text.encode(
        "utf-8"
    )


def create_exclusive(
    path: Path,
    data: bytes,
) -> None:

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
            "Irreversibly create "
            "the H6-M2 registry."
        ),
    )

    args = parser.parse_args()

    if REGISTRY.exists():
        raise RuntimeError(
            "H6-M2 registry already exists; "
            "registrar MUST NOT be re-run"
        )

    if RECEIPT.exists():
        raise RuntimeError(
            "H6-M2 registry receipt "
            "already exists"
        )

    frozen = (
        load_frozen_exclusions()
    )

    if (
        args.freeze
        and not
        committed_self_matches_head()
    ):
        raise RuntimeError(
            "refusing live H6-M2 freeze: "
            "registrar differs from "
            "committed HEAD"
        )

    registered_at = utcnow()

    with httpx.Client(
        headers=UA,
        timeout=30,
    ) as client:

        tag_id = resolve_mlb_tag(
            client
        )

        events = (
            fetch_active_mlb_events(
                client,
                tag_id=tag_id,
            )
        )

    selection = select_candidates(
        events,
        now=registered_at,
        exclusion_sets=
            frozen["sets"],
    )

    selected = selection[
        "selected"
    ]

    print("=" * 78)
    print(
        "H6-M2 DEVELOPMENT REGISTRATION"
    )
    print(
        "GAMMA METADATA ONLY"
    )
    print("=" * 78)

    print(
        "registered_at_utc:",
        registered_at.isoformat(),
    )

    print(
        "registrar_sha256:",
        sha256_file(
            Path(__file__).resolve()
        ),
    )

    print(
        "contract_sha256:",
        EXPECTED_CONTRACT_SHA,
    )

    print()
    print("EXCLUSION SETS")

    for name, values in (
        frozen["sets"].items()
    ):
        print(
            name,
            ":",
            len(values),
        )

    print(
        "distinct exclusion union:",
        len(frozen["union"]),
    )

    print()
    print(
        "CROSS-FAMILY EXCLUSION OVERLAPS"
    )

    for name, values in sorted(
        frozen["overlaps"].items()
    ):
        print(
            name,
            ":",
            len(values),
        )

    print()
    print(
        "eligible after exclusions:",
        selection[
            "eligible_after_exclusions"
        ],
    )

    print(
        "unique eligible conditions "
        "removed by exclusions:",
        selection[
            "excluded_unique_conditions"
        ],
    )

    print(
        "selected:",
        len(selected),
    )

    print()
    print("SELECTED COHORT")

    for index, row in enumerate(
        selected,
        start=1,
    ):
        print(
            index,
            "|",
            row[
                "game_start_time_utc"
            ],
            "|",
            row[
                "gamma_market_slug"
            ],
            "|",
            row[
                "condition_id"
            ],
        )

    print()
    print("ANALYSIS BOUNDARY")
    print("Gamma prices read: NO")
    print("Gamma depth read: NO")
    print("F19 trade content read: NO")
    print("F20 read: NO")
    print("H5 response read: NO")
    print("H2 performance read: NO")
    print("H6 transitions calculated: NO")
    print("H6 response calculated: NO")
    print("winner/settlement used: NO")
    print("PnL calculated: NO")

    if not args.freeze:
        print()
        print(
            "PREVIEW ONLY — "
            "registry written: NO"
        )
        return

    registry_bytes = (
        serialize_registry(
            selected,
            registered_at=
                registered_at,
        )
    )

    registry_sha = (
        sha256_bytes(
            registry_bytes
        )
    )

    create_exclusive(
        REGISTRY,
        registry_bytes,
    )

    receipt = {
        "study":
            "H6",

        "milestone":
            "H6-M2",

        "status":
            "DEVELOPMENT_REGISTRY_FROZEN",

        "registered_at_utc":
            registered_at.isoformat(),

        "git_commit":
            git_head(),

        "registrar": {
            "path":
                "research/h6_m2_register.py",

            "sha256":
                sha256_file(
                    Path(__file__).resolve()
                ),
        },

        "development_contract": {
            "path":
                "research/h6_m2_development_contract.json",

            "sha256":
                EXPECTED_CONTRACT_SHA,
        },

        "registry": {
            "path":
                "data/research/h6/m2/registry.jsonl",

            "sha256":
                registry_sha,

            "row_count":
                len(selected),

            "distinct_condition_ids":
                len({
                    row["condition_id"]
                    for row in selected
                }),

            "distinct_token_ids":
                len({
                    token
                    for row in selected
                    for token in (
                        row[
                            "p1_token_id"
                        ],
                        row[
                            "p0_token_id"
                        ],
                    )
                }),

            "mutable":
                False,
        },

        "frozen_exclusions": {
            "f19": {
                "count":
                    len(
                        frozen["sets"][
                            "F19"
                        ]
                    ),
                "sha256":
                    EXPECTED_F19_SHA,
            },

            "h6_m1": {
                "count":
                    len(
                        frozen["sets"][
                            "H6_M1"
                        ]
                    ),
                "sha256":
                    EXPECTED_H6_M1_SHA,
            },

            "h5": {
                "count":
                    len(
                        frozen["sets"][
                            "H5"
                        ]
                    ),
                "registry_sha256":
                    EXPECTED_H5_REGISTRY_SHA,
                "receipt_sha256":
                    EXPECTED_H5_RECEIPT_SHA,
            },

            "h2_v2": {
                "count":
                    len(
                        frozen["sets"][
                            "H2_V2"
                        ]
                    ),
                "identity_artifact_sha256":
                    EXPECTED_H2_EXCLUSIONS_SHA,
            },

            "distinct_union_count":
                len(
                    frozen["union"]
                ),

            "cross_family_overlaps":
                frozen["overlaps"],
        },

        "selection": {
            "source":
                "Polymarket Gamma metadata only",

            "sport":
                "mlb",

            "target_markets":
                TARGET_MARKETS,

            "operational_lead_seconds":
                REGISTRATION_LEAD_SECONDS,

            "ordering": [
                "game_start_time_utc ascending",
                "condition_id ascending",
            ],

            "selected_exclusion_overlap":
                0,
        },

        "burn_policy": {
            "all_m2_markets_burned_for_m4":
                True,

            "additions_after_freeze":
                False,

            "deletions_after_freeze":
                False,

            "replacements_after_freeze":
                False,
        },

        "analysis_boundary": {
            "book_price_read":
                False,

            "book_depth_read":
                False,

            "f19_trade_content_read":
                False,

            "f20_read":
                False,

            "h5_response_read":
                False,

            "h2_performance_read":
                False,

            "h6_transition_calculated":
                False,

            "h6_response_calculated":
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
        "H6-M2 DEVELOPMENT "
        "REGISTRY FROZEN"
    )

    print(
        "registry_sha256:",
        registry_sha,
    )

    print(
        "registry rows:",
        len(selected),
    )


if __name__ == "__main__":
    main()
