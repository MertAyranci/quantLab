from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import httpx
from dotenv import dotenv_values


REPO = Path(__file__).resolve().parents[1]

sys.path.insert(
    0,
    str(REPO / "research"),
)

import h5_m1_register as r


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

EXPECTED_REGISTRAR_SHA = (
    "fe6bafa6c21aa6dbc7490d6b82109a1"
    "90f0f563b8e42e93557ce9e245b6e5719"
)

MIN_READY = 5


def sha256_file(path: Path) -> str:
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def safe_events_request(
    http: httpx.Client,
    *,
    api_key: str,
):
    response = http.get(
        r.ODDS_EVENTS,
        params={
            "apiKey":
                api_key,
            "dateFormat":
                "iso",
        },
    )

    # Do not call raise_for_status() here:
    # exceptions may expose a secret-bearing URL.
    if response.status_code != 200:
        raise RuntimeError(
            "Odds events request failed: "
            f"HTTP {response.status_code}; "
            f"body={response.text[:500]!r}"
        )

    data = response.json()

    if not isinstance(data, list):
        raise RuntimeError(
            "Odds events response is not a list"
        )

    return data


def normalize_events(
    raw_events,
    *,
    now,
):
    cutoff = (
        now
        + r.timedelta(
            seconds=r.MIN_LEAD_SECONDS
        )
    )

    events = []

    for raw in raw_events:
        if not isinstance(raw, dict):
            continue

        event_id = str(
            raw.get("id")
            or ""
        ).strip()

        commence = r.parse_dt(
            raw.get(
                "commence_time"
            )
        )

        home = str(
            raw.get(
                "home_team"
            )
            or ""
        ).strip()

        away = str(
            raw.get(
                "away_team"
            )
            or ""
        ).strip()

        if (
            not event_id
            or commence is None
            or not home
            or not away
        ):
            continue

        if commence < cutoff:
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
        })

    events.sort(
        key=lambda x: (
            x[
                "canonical_commence_time"
            ],
            x[
                "odds_game_id"
            ],
        )
    )

    return events


def main():
    registrar = (
        REPO
        / "research"
        / "h5_m1_register.py"
    )

    actual = sha256_file(
        registrar
    )

    if actual != EXPECTED_REGISTRAR_SHA:
        raise RuntimeError(
            "frozen registrar SHA mismatch"
        )

    if REGISTRY.exists():
        print(
            "status: REGISTRY_ALREADY_FROZEN"
        )
        return

    if RECEIPT.exists():
        raise RuntimeError(
            "receipt exists without expected "
            "registry state"
        )

    exclusions = (
        r.load_frozen_inputs(
            require_committed_self=True,
        )
    )

    burned = (
        r.burned_external_event_ids(
            exclusions
        )
    )

    f19 = {
        str(x).lower()
        for x in exclusions[
            "rn1_f19_prospective"
        ]["condition_ids"]
    }

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

    now = r.utcnow()

    counts = {
        "events_after_lead":
            0,
        "h2_burned":
            0,
        "unmapped_or_ambiguous":
            0,
        "f19_excluded":
            0,
        "independent_eligible":
            0,
    }

    independent = []

    with httpx.Client(
        timeout=20,
        headers=r.UA,
    ) as http:

        raw = safe_events_request(
            http,
            api_key=api_key,
        )

        events = normalize_events(
            raw,
            now=now,
        )

        counts[
            "events_after_lead"
        ] = len(events)

        for event in events:

            if (
                event["odds_game_id"]
                in burned
            ):
                counts[
                    "h2_burned"
                ] += 1
                continue

            mapping = r.gamma_mapping(
                event=event,
                http=http,
            )

            if mapping is None:
                counts[
                    "unmapped_or_ambiguous"
                ] += 1
                continue

            condition = str(
                mapping[
                    "condition_id"
                ]
            ).lower()

            if condition in f19:
                counts[
                    "f19_excluded"
                ] += 1
                continue

            counts[
                "independent_eligible"
            ] += 1

            independent.append({
                "odds_game_id":
                    event[
                        "odds_game_id"
                    ],
                "condition_id":
                    condition,
                "commence_time":
                    event[
                        "canonical_commence_time"
                    ].isoformat(),
                "matchup":
                    (
                        f"{event['away_team']} @ "
                        f"{event['home_team']}"
                    ),
            })

    ready = (
        counts[
            "independent_eligible"
        ]
        >= MIN_READY
    )

    status = (
        "READY_FOR_H5_REGISTRATION"
        if ready
        else
        "NOT_READY_FOR_H5_REGISTRATION"
    )

    print(
        "========================================"
    )
    print(
        "H5-M1 REGISTRATION READINESS"
    )
    print(
        "========================================"
    )

    print(
        "status:",
        status,
    )

    print(
        "checked_at_utc:",
        now.isoformat(),
    )

    for key, value in (
        counts.items()
    ):
        print(
            f"{key}:",
            value,
        )

    if independent:
        print()
        print(
            "independent candidates:"
        )

        for x in independent:
            print(
                " -",
                x["commence_time"],
                x["matchup"],
                x["condition_id"],
            )

    print()
    print(
        "registry written: NO"
    )
    print(
        "odds-bearing endpoint used: NO"
    )
    print(
        "external consensus calculated: NO"
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
    print(
        "F19 trade content read: NO"
    )
    print(
        "F20 read: NO"
    )


if __name__ == "__main__":
    main()
