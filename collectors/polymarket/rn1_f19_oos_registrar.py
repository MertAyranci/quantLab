from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path

import httpx


REPO = Path(__file__).resolve().parents[2]

GAMMA = "https://gamma-api.polymarket.com"

CONTRACT = (
    REPO
    / "research"
    / "rn1_f19_oos_registration_contract.json"
)

DISCOVERY_COHORT = (
    REPO
    / "research"
    / "h2_v1_cohort.csv"
)

ROOT = (
    REPO
    / "data"
    / "prospective"
    / "rn1_f19"
)

RAW_ROOT = ROOT / "raw"
REGISTRY = ROOT / "registry.jsonl"

TARGET = 100

UA = {
    "User-Agent":
        "quant-lab-rn1-f19/1.0"
}


def utcnow():
    return datetime.now(
        timezone.utc
    )


def parse_dt(value):
    if not value:
        return None

    try:
        return datetime.fromisoformat(
            str(value).replace(
                "Z",
                "+00:00",
            )
        )
    except ValueError:
        return None


def jloads_maybe(value):
    if value is None:
        return None

    if isinstance(
        value,
        (list, dict),
    ):
        return value

    try:
        return json.loads(
            value
        )
    except (
        json.JSONDecodeError,
        TypeError,
    ):
        return None


def sha256(path):
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def git_head():
    try:
        return subprocess.check_output(
            [
                "git",
                "rev-parse",
                "HEAD",
            ],
            cwd=REPO,
            text=True,
        ).strip()

    except Exception:
        return "unknown"


def discovery_conditions():
    import csv

    out = set()

    if not DISCOVERY_COHORT.exists():
        raise RuntimeError(
            f"missing discovery cohort: "
            f"{DISCOVERY_COHORT}"
        )

    with DISCOVERY_COHORT.open(
        newline="",
        encoding="utf-8",
    ) as f:

        for row in csv.DictReader(f):

            condition = (
                row.get(
                    "condition_id"
                )
                or ""
            ).strip()

            if condition:
                out.add(
                    condition
                )

    if len(out) != 42:
        raise RuntimeError(
            f"expected 42 discovery "
            f"conditions, got {len(out)}"
        )

    return out


def load_registry():
    rows = []

    if not REGISTRY.exists():
        return rows

    for line in (
        REGISTRY
        .read_text(
            encoding="utf-8"
        )
        .splitlines()
    ):

        if line.strip():
            rows.append(
                json.loads(line)
            )

    return rows


def append_registry(row):
    ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    with REGISTRY.open(
        "a",
        encoding="utf-8",
    ) as f:

        f.write(
            json.dumps(
                row,
                sort_keys=True,
                separators=(
                    ",",
                    ":",
                ),
            )
        )

        f.write("\n")


def save_raw(
    name,
    payload,
    captured_at,
):

    day = captured_at.strftime(
        "%Y%m%d"
    )

    directory = (
        RAW_ROOT
        /
        day
    )

    directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    stamp = captured_at.strftime(
        "%Y%m%dT%H%M%S.%fZ"
    )

    path = (
        directory
        /
        (
            f"{name}_"
            f"{stamp}_"
            f"{uuid.uuid4().hex[:8]}"
            f".json"
        )
    )

    envelope = {
        "captured_at_utc":
            captured_at.isoformat(),

        "payload":
            payload,
    }

    path.write_text(
        json.dumps(
            envelope,
            separators=(
                ",",
                ":",
            ),
        ),
        encoding="utf-8",
    )

    return str(
        path.relative_to(
            REPO
        )
    )


def token_list(market):
    value = jloads_maybe(
        market.get(
            "clobTokenIds"
        )
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


def outcome_list(market):
    value = jloads_maybe(
        market.get(
            "outcomes"
        )
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


def mlb_tag_ids(client):

    response = client.get(
        f"{GAMMA}/sports"
    )

    response.raise_for_status()

    sports = response.json()

    matches = [
        row
        for row in sports
        if str(
            row.get("sport")
            or ""
        ).strip().lower()
        == "mlb"
    ]

    if len(matches) != 1:

        print(
            "MLB sports metadata matches:",
            len(matches),
        )

        for row in matches:
            print(
                json.dumps(
                    row,
                    sort_keys=True,
                )
            )

        raise RuntimeError(
            "expected exactly one MLB "
            "record in Gamma /sports"
        )

    mlb = matches[0]

    primary = mlb.get(
        "primaryTagId"
    )

    if primary is None:

        print(
            "MLB sports metadata:",
            json.dumps(
                mlb,
                sort_keys=True,
            ),
        )

        raise RuntimeError(
            "MLB sports metadata has "
            "no primaryTagId"
        )

    primary = str(
        primary
    ).strip()

    if not primary:

        raise RuntimeError(
            "MLB primaryTagId is empty"
        )

    return (
        [primary],
        sports,
    )


def fetch_events_for_tag(
    client,
    tag_id,
):

    out = []

    after_cursor = None

    pages = 0

    seen_cursors = set()

    while True:

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

        if response.status_code != 200:

            raise RuntimeError(
                "Gamma keyset events request "
                f"failed: status="
                f"{response.status_code} "
                f"url={response.request.url} "
                f"body={response.text[:1000]}"
            )

        payload = response.json()

        if not isinstance(
            payload,
            dict,
        ):

            raise RuntimeError(
                "Gamma /events/keyset "
                "returned non-object payload"
            )

        batch = payload.get(
            "events"
        )

        if not isinstance(
            batch,
            list,
        ):

            raise RuntimeError(
                "Gamma /events/keyset "
                "payload missing events list"
            )

        out.extend(
            batch
        )

        pages += 1

        next_cursor = payload.get(
            "next_cursor"
        )

        if not next_cursor:

            break

        next_cursor = str(
            next_cursor
        )

        if next_cursor in seen_cursors:

            raise RuntimeError(
                "Gamma keyset cursor repeated"
            )

        seen_cursors.add(
            next_cursor
        )

        after_cursor = (
            next_cursor
        )

        if pages > 100:

            raise RuntimeError(
                "unexpectedly exceeded "
                "100 Gamma keyset pages"
            )

    return out


def eligible_market(
    market,
    *,
    captured_at,
):

    if (
        market.get(
            "sportsMarketType"
        )
        != "moneyline"
    ):
        return None

    if (
        market.get("active")
        is not True
    ):
        return None

    if (
        market.get("closed")
        is not False
    ):
        return None

    if (
        market.get(
            "acceptingOrders"
        )
        is not True
    ):
        return None

    condition = str(
        market.get(
            "conditionId"
        )
        or ""
    ).strip()

    if not condition:
        return None

    tokens = token_list(
        market
    )

    if len(tokens) != 2:
        return None

    outcomes = outcome_list(
        market
    )

    if (
        outcomes
        and
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

    # Prospective boundary:
    # registration must happen BEFORE
    # the scheduled game start.
    if start <= captured_at:
        return None

    return {
        "condition_id":
            condition,

        "gamma_market_id":
            str(
                market.get("id")
                or ""
            ),

        "slug":
            str(
                market.get("slug")
                or ""
            ),

        "question":
            str(
                market.get("question")
                or ""
            ),

        "game_start_time":
            start,

        "token_ids":
            tokens,

        "outcomes":
            outcomes,
    }


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--dry-run",
        action="store_true",
    )

    args = parser.parse_args()

    if not CONTRACT.exists():
        raise RuntimeError(
            f"missing contract: "
            f"{CONTRACT}"
        )

    contract = json.loads(
        CONTRACT.read_text(
            encoding="utf-8"
        )
    )

    if (
        contract.get(
            "target_games"
        )
        != TARGET
    ):
        raise RuntimeError(
            "contract target mismatch"
        )

    discovery = (
        discovery_conditions()
    )

    existing_rows = (
        load_registry()
    )

    existing_conditions = {
        row["condition_id"]
        for row in existing_rows
    }

    duplicate_registry = (
        len(existing_conditions)
        !=
        len(existing_rows)
    )

    if duplicate_registry:
        raise RuntimeError(
            "duplicate condition_id "
            "already present in registry"
        )

    if (
        existing_conditions
        &
        discovery
    ):
        raise RuntimeError(
            "registry contains discovery "
            "market condition"
        )

    captured_at = utcnow()

    with httpx.Client(
        headers=UA,
        timeout=30,
    ) as client:

        (
            tag_ids,
            sports_payload,
        ) = mlb_tag_ids(
            client
        )

        sports_raw_ref = save_raw(
            "sports",
            sports_payload,
            captured_at,
        )

        all_events = []

        event_raw_refs = []

        for tag_id in tag_ids:

            events = (
                fetch_events_for_tag(
                    client,
                    tag_id,
                )
            )

            all_events.extend(
                events
            )

            event_raw_refs.append(
                save_raw(
                    f"events_tag_{tag_id}",
                    events,
                    captured_at,
                )
            )

    candidates = {}

    for event in all_events:

        event_id = str(
            event.get("id")
            or ""
        )

        event_slug = str(
            event.get("slug")
            or ""
        )

        for market in (
            event.get("markets")
            or []
        ):

            e = eligible_market(
                market,
                captured_at=captured_at,
            )

            if e is None:
                continue

            condition = (
                e["condition_id"]
            )

            if condition in discovery:
                continue

            if condition in (
                existing_conditions
            ):
                continue

            # Deduplicate if multiple MLB tags
            # return the same market.
            candidates[
                condition
            ] = {
                **e,

                "event_id":
                    event_id,

                "event_slug":
                    event_slug,
            }

    ordered = sorted(
        candidates.values(),
        key=lambda x: (
            x["game_start_time"],
            x["condition_id"],
        ),
    )

    slots = max(
        0,
        TARGET
        -
        len(existing_rows),
    )

    selected = (
        ordered[:slots]
    )

    contract_sha = sha256(
        CONTRACT
    )

    print()
    print(
        "========================================"
    )
    print(
        "A — F19 PROSPECTIVE REGISTRATION"
    )
    print(
        "========================================"
    )

    print(
        "captured_at:",
        captured_at.isoformat(),
    )

    print(
        "contract_sha256:",
        contract_sha,
    )

    print(
        "git_head:",
        git_head(),
    )

    print(
        "MLB tag IDs:",
        tag_ids,
    )

    print(
        "discovery exclusions:",
        len(discovery),
    )

    print(
        "already registered:",
        len(existing_rows),
    )

    print(
        "eligible unseen candidates:",
        len(ordered),
    )

    print(
        "remaining target slots:",
        slots,
    )

    print(
        "selected this run:",
        len(selected),
    )

    print()
    print(
        "========================================"
    )
    print(
        "B — SELECTED MARKETS"
    )
    print(
        "========================================"
    )

    for x in selected:

        seconds_before_start = int(
            (
                x["game_start_time"]
                -
                captured_at
            ).total_seconds()
        )

        print(
            x["game_start_time"]
            .isoformat(),
            "|",
            x["slug"],
            "| condition",
            x["condition_id"][:18] + "...",
            "| lead_sec",
            seconds_before_start,
        )

    if args.dry_run:

        print()
        print(
            "DRY RUN — registry unchanged"
        )

        print(
            "raw sports:",
            sports_raw_ref,
        )

        print(
            "raw events:",
            len(event_raw_refs),
        )

        return

    for x in selected:

        row = {
            "study":
                "RN1-F19",

            "registered_at_utc":
                captured_at.isoformat(),

            "game_start_time_utc":
                x[
                    "game_start_time"
                ].isoformat(),

            "registration_lead_seconds":
                int(
                    (
                        x[
                            "game_start_time"
                        ]
                        -
                        captured_at
                    ).total_seconds()
                ),

            "condition_id":
                x["condition_id"],

            "gamma_market_id":
                x["gamma_market_id"],

            "event_id":
                x["event_id"],

            "event_slug":
                x["event_slug"],

            "slug":
                x["slug"],

            "question":
                x["question"],

            "token_ids":
                x["token_ids"],

            "outcomes":
                x["outcomes"],

            "sports_market_type":
                "moneyline",

            "selection_basis":
                "all_eligible_upcoming_mlb_moneyline",

            "contract_sha256":
                contract_sha,

            "git_head":
                git_head(),

            "raw_sports_ref":
                sports_raw_ref,

            "raw_event_refs":
                event_raw_refs,
        }

        append_registry(
            row
        )

    final_rows = (
        load_registry()
    )

    final_conditions = [
        x["condition_id"]
        for x in final_rows
    ]

    if (
        len(final_conditions)
        !=
        len(
            set(
                final_conditions
            )
        )
    ):
        raise RuntimeError(
            "registry duplicate after append"
        )

    if (
        set(
            final_conditions
        )
        &
        discovery
    ):
        raise RuntimeError(
            "discovery leakage after append"
        )

    print()
    print(
        "========================================"
    )
    print(
        "C — REGISTRY VALIDATION"
    )
    print(
        "========================================"
    )

    print(
        "registry rows:",
        len(final_rows),
    )

    print(
        "unique conditions:",
        len(
            set(
                final_conditions
            )
        ),
    )

    print(
        "discovery overlap:",
        len(
            set(
                final_conditions
            )
            &
            discovery
        ),
    )

    print(
        "remaining to 100:",
        max(
            0,
            TARGET
            -
            len(
                final_rows
            ),
        ),
    )

    print()
    print(
        "registry:",
        REGISTRY,
    )

    print(
        "registry_sha256:",
        sha256(
            REGISTRY
        ),
    )

    print()
    print(
        "========================================"
    )
    print(
        "INTERPRETATION BOUNDARY"
    )
    print(
        "========================================"
    )

    print(
        "PROSPECTIVE REGISTRATION ONLY"
    )

    print(
        "NO RN1 WALLET DATA USED"
    )

    print(
        "NO MARKET TRADE DATA USED"
    )

    print(
        "NO SIGNAL / MARKOUT / PNL"
    )

    print(
        "NO SETTLEMENT DATA"
    )

    print(
        "DISCOVERY 42 CONDITIONS EXCLUDED"
    )

    print(
        "REGISTRATION MUST PRECEDE GAME START"
    )

    print(
        "REGISTRY ENTRIES APPEND-ONLY"
    )


if __name__ == "__main__":
    main()
