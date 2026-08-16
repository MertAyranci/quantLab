from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
import psycopg2
from dotenv import dotenv_values


REPO = Path(__file__).resolve().parents[1]

M1A_CONTRACT = (
    REPO
    / "research"
    / "h5_m1a_acquisition_contract.json"
)

M0_CONTRACT = (
    REPO
    / "research"
    / "h5_m0_mechanism_data_design.json"
)

EXCLUSIONS = (
    REPO
    / "research"
    / "h5_m0_exclusions.json"
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

EXPECTED_PARENT_HEAD = (
    "de347d585687a3aaf33e704f37b43a75ba7816a9"
)

EXPECTED_M1A_SHA = (
    "6da26ac94413b5a79566764dcc43d4584"
    "bff94c3b88531225dec09a4db99b1be"
)

EXPECTED_M0_SHA = (
    "073730512baa587e5be730f534daf1e3"
    "89c8934d301769a77f387e1fff0a553c"
)

EXPECTED_EXCLUSIONS_SHA = (
    "fb4a268f3148e746e31232c658cdc879"
    "3c2e082a73c5a8260af6779d89b1949a"
)

SPORT = "baseball_mlb"

ODDS_EVENTS = (
    "https://api.the-odds-api.com/"
    "v4/sports/baseball_mlb/events"
)

GAMMA_SEARCH = (
    "https://gamma-api.polymarket.com/public-search"
)

MIN_LEAD_SECONDS = 1800
MAX_START_DELTA_SECONDS = 600
TARGET_MARKETS = 10
MIN_MARKETS = 5

MAPPING_RULE = (
    "exact_team_set+moneyline+2_tokens+"
    "active_open_accepting+"
    "unique_abs_start_delta_le_600s"
)

UA = {
    "User-Agent":
        "quant-lab-h5-registration/1.0"
}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


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


def norm_team(name: Any) -> str:
    n = str(
        name or ""
    ).strip().lower()

    n = n.replace(".", "")
    n = n.replace("-", " ")

    n = re.sub(
        r"\s+",
        " ",
        n,
    ).strip()

    aliases = {
        "oakland athletics":
            "athletics",
        "athletics":
            "athletics",
        "st louis cardinals":
            "st louis cardinals",
    }

    return aliases.get(
        n,
        n,
    )


def parse_matchup(question: Any):
    if not question:
        return None

    m = re.match(
        r"^\s*(.+?)\s+vs\.?\s+(.+?)\s*$",
        str(question),
        flags=re.I,
    )

    if not m:
        return None

    return {
        norm_team(m.group(1)),
        norm_team(m.group(2)),
    }


def token_list(market: dict[str, Any]) -> list[str]:
    raw = jloads_maybe(
        market.get("clobTokenIds")
    )

    if not isinstance(raw, list):
        return []

    return [
        str(x)
        for x in raw
    ]


def outcomes_list(
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


def git_is_ancestor(
    ancestor: str,
    descendant: str = "HEAD",
) -> bool:
    result = subprocess.run(
        [
            "git",
            "merge-base",
            "--is-ancestor",
            ancestor,
            descendant,
        ],
        cwd=REPO,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    return result.returncode == 0


def committed_self_matches_head() -> bool:
    path = Path(__file__).resolve()

    try:
        relative = path.relative_to(
            REPO
        ).as_posix()
    except ValueError:
        return False

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

    committed_sha = hashlib.sha256(
        committed
    ).hexdigest()

    local_sha = sha256_file(
        path
    )

    return committed_sha == local_sha


def load_frozen_inputs(
    *,
    require_committed_self: bool,
):
    if not git_is_ancestor(
        EXPECTED_PARENT_HEAD
    ):
        raise RuntimeError(
            "frozen H5-M1A parent commit is "
            "not an ancestor of current HEAD"
        )

    if (
        require_committed_self
        and not committed_self_matches_head()
    ):
        raise RuntimeError(
            "refusing live H5 registration: "
            "registrar is not byte-identical to "
            "the version committed at HEAD"
        )

    identities = {
        M1A_CONTRACT:
            EXPECTED_M1A_SHA,
        M0_CONTRACT:
            EXPECTED_M0_SHA,
        EXCLUSIONS:
            EXPECTED_EXCLUSIONS_SHA,
    }

    for path, expected in identities.items():
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

    return json.loads(
        EXCLUSIONS.read_text(
            encoding="utf-8"
        )
    )


def connect_db():
    env = dotenv_values(
        REPO / ".env"
    )

    password = env.get(
        "PG_PASSWORD"
    )

    if not password:
        raise RuntimeError(
            "PG_PASSWORD missing from .env"
        )

    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="quantlab",
        user="quantlab",
        password=password,
    )

    # Registration may use legacy identity
    # translation only. Never write.
    conn.set_session(
        readonly=True,
        autocommit=False,
    )

    return conn


def burned_external_event_ids(
    exclusions: dict[str, Any],
) -> set[str]:
    legacy_ids = exclusions[
        "h2_v2_burned_development"
    ]["odds_game_ids"]

    legacy_ids = [
        int(x)
        for x in legacy_ids
    ]

    if len(set(legacy_ids)) != 9:
        raise RuntimeError(
            "expected exactly 9 legacy "
            "H2 burned odds_games.id values"
        )

    conn = connect_db()

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

    mapping = {
        int(row[0]): str(row[1])
        for row in rows
    }

    if set(mapping) != set(legacy_ids):
        missing = sorted(
            set(legacy_ids)
            - set(mapping)
        )

        raise RuntimeError(
            "could not translate all burned "
            f"H2 internal IDs: {missing}"
        )

    return set(
        mapping.values()
    )


def fetch_upcoming_events(
    *,
    api_key: str,
    now: datetime,
    http: httpx.Client,
) -> list[dict[str, Any]]:
    response = http.get(
        ODDS_EVENTS,
        params={
            "apiKey": api_key,
            "dateFormat": "iso",
        },
    )

    response.raise_for_status()

    data = response.json()

    if not isinstance(data, list):
        raise RuntimeError(
            "Odds events response is not a list"
        )

    cutoff = (
        now
        + timedelta(
            seconds=MIN_LEAD_SECONDS
        )
    )

    out = []

    for raw in data:
        if not isinstance(raw, dict):
            continue

        event_id = str(
            raw.get("id") or ""
        ).strip()

        commence = parse_dt(
            raw.get("commence_time")
        )

        home = str(
            raw.get("home_team") or ""
        ).strip()

        away = str(
            raw.get("away_team") or ""
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

        out.append({
            "odds_game_id":
                event_id,

            "canonical_commence_time":
                commence,

            "home_team":
                home,

            "away_team":
                away,
        })

    out.sort(
        key=lambda x: (
            x[
                "canonical_commence_time"
            ],
            x["odds_game_id"],
        )
    )

    return out


def gamma_mapping(
    *,
    event: dict[str, Any],
    http: httpx.Client,
):
    away = event["away_team"]
    home = event["home_team"]

    wanted = {
        norm_team(away),
        norm_team(home),
    }

    response = http.get(
        GAMMA_SEARCH,
        params={
            "q":
                f"{away} {home}",

            "limit_per_type":
                20,

            "keep_closed_markets":
                1,
        },
    )

    response.raise_for_status()

    payload = response.json()

    if not isinstance(
        payload,
        dict,
    ):
        return None

    exact_team_candidates = {}
    eligible = {}

    commence = event[
        "canonical_commence_time"
    ]

    for gamma_event in (
        payload.get("events")
        or []
    ):
        for market in (
            gamma_event.get("markets")
            or []
        ):
            if not isinstance(
                market,
                dict,
            ):
                continue

            if (
                parse_matchup(
                    market.get(
                        "question"
                    )
                )
                != wanted
            ):
                continue

            gamma_id = str(
                market.get("id")
                or ""
            ).strip()

            if not gamma_id:
                continue

            exact_team_candidates[
                gamma_id
            ] = market

            if (
                market.get(
                    "sportsMarketType"
                )
                != "moneyline"
            ):
                continue

            tokens = token_list(
                market
            )

            outcomes = outcomes_list(
                market
            )

            if (
                len(tokens) != 2
                or len(outcomes) != 2
            ):
                continue

            if {
                norm_team(outcomes[0]),
                norm_team(outcomes[1]),
            } != wanted:
                continue

            if (
                market.get("active")
                is not True
            ):
                continue

            if (
                market.get("closed")
                is not False
            ):
                continue

            if (
                market.get(
                    "acceptingOrders"
                )
                is not True
            ):
                continue

            start = parse_dt(
                market.get(
                    "gameStartTime"
                )
            )

            if start is None:
                continue

            delta = (
                start
                - commence
            ).total_seconds()

            if (
                abs(delta)
                > MAX_START_DELTA_SECONDS
            ):
                continue

            condition_id = str(
                market.get(
                    "conditionId"
                )
                or ""
            ).strip().lower()

            if not condition_id:
                continue

            eligible[
                gamma_id
            ] = {
                "gamma_market_id":
                    gamma_id,

                "condition_id":
                    condition_id,

                "gamma_slug":
                    (
                        str(
                            market.get(
                                "slug"
                            )
                        )
                        if market.get(
                            "slug"
                        )
                        else None
                    ),

                "gamma_question":
                    str(
                        market.get(
                            "question"
                        )
                    ),

                "pm_game_start_time":
                    start,

                "start_delta_seconds":
                    delta,

                "p1_team":
                    outcomes[0],

                "p0_team":
                    outcomes[1],

                "p1_token_id":
                    tokens[0],

                "p0_token_id":
                    tokens[1],

                "exact_team_candidates":
                    len(
                        exact_team_candidates
                    ),

                "mapping_rule":
                    MAPPING_RULE,
            }

    if len(eligible) != 1:
        return None

    result = next(
        iter(
            eligible.values()
        )
    )

    result[
        "exact_team_candidates"
    ] = len(
        exact_team_candidates
    )

    result[
        "in_window_candidates"
    ] = len(eligible)

    return result


def canonical_row(
    *,
    index: int,
    event: dict[str, Any],
    mapping: dict[str, Any],
    registered_at: datetime,
):
    # Explicit allowlist.
    # Gamma price/volume/liquidity fields
    # cannot enter the registry.
    return {
        "study":
            "H5",

        "milestone":
            "H5-M1",

        "registration_index":
            index,

        "registered_at_utc":
            registered_at.isoformat(),

        "odds_game_id":
            event[
                "odds_game_id"
            ],

        "canonical_commence_time":
            event[
                "canonical_commence_time"
            ].isoformat(),

        "away_team":
            event[
                "away_team"
            ],

        "home_team":
            event[
                "home_team"
            ],

        "gamma_market_id":
            mapping[
                "gamma_market_id"
            ],

        "condition_id":
            mapping[
                "condition_id"
            ],

        "gamma_slug":
            mapping[
                "gamma_slug"
            ],

        "gamma_question":
            mapping[
                "gamma_question"
            ],

        "pm_game_start_time":
            mapping[
                "pm_game_start_time"
            ].isoformat(),

        "start_delta_seconds":
            mapping[
                "start_delta_seconds"
            ],

        "p1_team":
            mapping[
                "p1_team"
            ],

        "p0_team":
            mapping[
                "p0_team"
            ],

        "p1_token_id":
            mapping[
                "p1_token_id"
            ],

        "p0_token_id":
            mapping[
                "p0_token_id"
            ],

        "exact_team_candidates":
            mapping[
                "exact_team_candidates"
            ],

        "in_window_candidates":
            mapping[
                "in_window_candidates"
            ],

        "mapping_rule":
            mapping[
                "mapping_rule"
            ],
    }


def serialize_registry(
    rows: list[dict[str, Any]],
) -> bytes:
    text = "".join(
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
    )

    return text.encode(
        "utf-8"
    )


def atomic_create(
    path: Path,
    content: bytes,
):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    if path.exists():
        raise RuntimeError(
            f"refusing to overwrite: {path}"
        )

    fd, tmp_name = tempfile.mkstemp(
        prefix=".tmp_h5_registry_",
        dir=path.parent,
    )

    tmp = Path(tmp_name)

    try:
        with os.fdopen(
            fd,
            "wb",
        ) as fh:
            fh.write(content)
            fh.flush()
            os.fsync(
                fh.fileno()
            )

        os.replace(
            tmp,
            path,
        )

    except Exception:
        tmp.unlink(
            missing_ok=True
        )
        raise


def register(
    *,
    http: httpx.Client,
    api_key: str,
    now: datetime,
):
    exclusions = (
        load_frozen_inputs(
            require_committed_self=True,
        )
    )

    if REGISTRY.exists():
        raise RuntimeError(
            "H5 registry already exists"
        )

    if RECEIPT.exists():
        raise RuntimeError(
            "H5 registry receipt already exists"
        )

    burned_external = (
        burned_external_event_ids(
            exclusions
        )
    )

    f19_conditions = {
        str(x).lower()
        for x in exclusions[
            "rn1_f19_prospective"
        ]["condition_ids"]
    }

    if len(
        burned_external
    ) != 9:
        raise RuntimeError(
            "burned external set != 9"
        )

    if len(
        f19_conditions
    ) != 100:
        raise RuntimeError(
            "F19 exclusion set != 100"
        )

    events = fetch_upcoming_events(
        api_key=api_key,
        now=now,
        http=http,
    )

    selected = []

    exclusion_counts = {
        "h2_v2_burned":
            0,

        "f19":
            0,

        "unmapped_or_ambiguous":
            0,
    }

    for event in events:

        if (
            event["odds_game_id"]
            in burned_external
        ):
            exclusion_counts[
                "h2_v2_burned"
            ] += 1
            continue

        mapping = gamma_mapping(
            event=event,
            http=http,
        )

        if mapping is None:
            exclusion_counts[
                "unmapped_or_ambiguous"
            ] += 1
            continue

        if (
            mapping["condition_id"]
            in f19_conditions
        ):
            exclusion_counts[
                "f19"
            ] += 1
            continue

        selected.append(
            canonical_row(
                index=(
                    len(selected)
                    + 1
                ),
                event=event,
                mapping=mapping,
                registered_at=now,
            )
        )

        if (
            len(selected)
            == TARGET_MARKETS
        ):
            break

    if len(selected) < MIN_MARKETS:
        raise RuntimeError(
            "insufficient eligible markets "
            f"for H5-M1 registration: "
            f"{len(selected)} < {MIN_MARKETS}"
        )

    condition_ids = {
        x["condition_id"]
        for x in selected
    }

    odds_game_ids = {
        x["odds_game_id"]
        for x in selected
    }

    if (
        len(condition_ids)
        != len(selected)
        or len(odds_game_ids)
        != len(selected)
    ):
        raise RuntimeError(
            "duplicate market identity "
            "in selected registry"
        )

    registry_bytes = (
        serialize_registry(
            selected
        )
    )

    registry_sha = (
        hashlib.sha256(
            registry_bytes
        ).hexdigest()
    )

    code_sha = sha256_file(
        Path(__file__).resolve()
    )

    atomic_create(
        REGISTRY,
        registry_bytes,
    )

    receipt = {
        "study":
            "H5",

        "milestone":
            "H5-M1",

        "status":
            "REGISTRY_FROZEN",

        "registered_at_utc":
            now.isoformat(),

        "git_commit":
            git_head(),

        "registration_code": {
            "path":
                "research/h5_m1_register.py",

            "sha256":
                code_sha,
        },

        "m1a_contract_sha256":
            sha256_file(
                M1A_CONTRACT
            ),

        "m0_contract_sha256":
            sha256_file(
                M0_CONTRACT
            ),

        "m0_exclusions_sha256":
            sha256_file(
                EXCLUSIONS
            ),

        "registry": {
            "path":
                "data/research/h5_m1/registry.jsonl",

            "sha256":
                registry_sha,

            "row_count":
                len(selected),

            "distinct_odds_game_id_count":
                len(
                    odds_game_ids
                ),

            "distinct_condition_id_count":
                len(
                    condition_ids
                ),
        },

        "selection": {
            "target_markets":
                TARGET_MARKETS,

            "minimum_markets":
                MIN_MARKETS,

            "ordering": [
                "canonical commence_time ascending",
                "odds_game_id ascending",
            ],

            "minimum_lead_seconds":
                MIN_LEAD_SECONDS,

            "excluded_h2_v2_external_ids":
                len(
                    burned_external
                ),

            "excluded_f19_condition_ids":
                len(
                    f19_conditions
                ),

            "observed_exclusion_counts":
                exclusion_counts,
        },

        "network_boundary": {
            "odds_events_endpoint_used":
                True,

            "odds_bearing_endpoint_used":
                False,

            "gamma_metadata_search_used":
                True,
        },

        "analysis_boundary": {
            "external_odds_read":
                False,

            "external_consensus_calculated":
                False,

            "h5_innovation_calculated":
                False,

            "pm_price_used_for_selection":
                False,

            "pm_response_calculated":
                False,

            "future_pm_state_read":
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
        },
    }

    RECEIPT.write_text(
        json.dumps(
            receipt,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    return receipt


def main():
    ap = argparse.ArgumentParser()

    ap.add_argument(
        "--validate-config",
        action="store_true",
    )

    args = ap.parse_args()

    exclusions = (
        load_frozen_inputs(
            require_committed_self=False,
        )
    )

    if args.validate_config:
        burned = (
            burned_external_event_ids(
                exclusions
            )
        )

        print(
            "H5-M1 REGISTRATION CONFIG: PASS"
        )

        print(
            "parent HEAD:",
            git_head(),
        )

        print(
            "registration code SHA:",
            sha256_file(
                Path(
                    __file__
                ).resolve()
            ),
        )

        print(
            "translated H2 burned events:",
            len(burned),
        )

        print(
            "F19 excluded conditions:",
            len(
                exclusions[
                    "rn1_f19_prospective"
                ][
                    "condition_ids"
                ]
            ),
        )

        print(
            "network calls made: NO"
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

        return

    env = dotenv_values(
        REPO / ".env"
    )

    api_key = env.get(
        "ODDS_API_KEY"
    )

    if not api_key:
        raise SystemExit(
            "ODDS_API_KEY missing from .env"
        )

    with httpx.Client(
        timeout=20,
        headers=UA,
    ) as http:

        receipt = register(
            http=http,
            api_key=api_key,
            now=utcnow(),
        )

    print(
        "========================================"
    )

    print(
        "H5-M1 REGISTRY FROZEN"
    )

    print(
        "========================================"
    )

    print(
        "markets:",
        receipt[
            "registry"
        ][
            "row_count"
        ],
    )

    print(
        "registry SHA:",
        receipt[
            "registry"
        ][
            "sha256"
        ],
    )

    print(
        "registration code SHA:",
        receipt[
            "registration_code"
        ][
            "sha256"
        ],
    )

    print(
        "odds-bearing endpoint used: NO"
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


if __name__ == "__main__":
    main()
