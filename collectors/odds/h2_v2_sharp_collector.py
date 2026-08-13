"""H2-v2 M1 sharp fair-value collector.

Collects MLB pregame exchange back/lay observations from The Odds API.

Frozen contract:
    research/h2_v2_m1_data_contract.json

Stores:
    h2_v2_sharp_polls
    h2_v2_exchange_quotes
    h2_v2_sharp_consensus

Important:
    - no signals
    - no thresholds
    - no PnL
    - no trading
    - no stale carry-forward
    - only pregame observations
    - consensus requires >=2 fresh complete exchange families

Usage:
    # zero API credits
    .venv/bin/python collectors/odds/h2_v2_sharp_collector.py \
        --validate-config

    # one API credit
    .venv/bin/python collectors/odds/h2_v2_sharp_collector.py \
        --polls 1

    # later M1 controlled run
    .venv/bin/python collectors/odds/h2_v2_sharp_collector.py \
        --polls 60 --interval-seconds 30
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from statistics import median

import httpx
import psycopg2
from psycopg2.extras import Json
from dotenv import dotenv_values


REPO = Path(__file__).resolve().parents[2]

ENV = dotenv_values(
    REPO / ".env"
)

CONTRACT_PATH = (
    REPO
    / "research"
    / "h2_v2_m1_data_contract.json"
)

EXPECTED_CONTRACT_SHA = (
    "c539abbf046e173d3f50f57b071d483b"
    "cb549b034cf9e7716151fe1752d1143b"
)

SPORT = "baseball_mlb"

URL = (
    "https://api.the-odds-api.com/"
    "v4/sports/baseball_mlb/odds"
)

BOOKMAKERS = [
    "betfair_ex_uk",
    "matchbook",
    "smarkets",
]

MAX_SOURCE_AGE_SECONDS = Decimal("25")

COLLECTOR_VERSION = "H2-v2-M1-sharp-1"

MAX_POLLS_PER_PROCESS = 60

DEFAULT_INTERVAL_SECONDS = 30

MIN_REMAINING_CREDITS = int(
    ENV.get(
        "H2_V2_ODDS_MIN_REMAINING",
        "50",
    )
)

RAW_ROOT = (
    REPO
    / "data"
    / "raw"
    / "h2_v2_sharp"
)

UA = {
    "User-Agent":
        "quant-lab-h2-v2-sharp/1.0"
}


def utcnow():
    return datetime.now(
        timezone.utc
    )


def sha256(path: Path):
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


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


def header_int(headers, key):
    value = headers.get(key)

    if value is None:
        return None

    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def decimal_price(outcome):
    if not outcome:
        return None

    value = outcome.get("price")

    if value is None:
        return None

    try:
        x = Decimal(
            str(value)
        )
    except (
        InvalidOperation,
        ValueError,
        TypeError,
    ):
        return None

    if x <= Decimal("1"):
        return None

    return x


def probability(decimal_odds):
    if decimal_odds is None:
        return None

    return (
        Decimal("1")
        / decimal_odds
    )


def outcomes_by_name(market):
    if not market:
        return {}

    return {
        str(o.get("name")): o
        for o in market.get(
            "outcomes",
            []
        )
        if o.get("name")
    }


def safe_request_params():
    # Deliberately excludes API key.
    return {
        "bookmakers":
            ",".join(BOOKMAKERS),

        "markets":
            "h2h",

        "oddsFormat":
            "decimal",

        "dateFormat":
            "iso",
    }


def api_request_params(api_key):
    return {
        "apiKey":
            api_key,

        **safe_request_params(),
    }


def save_raw_response(
    response: httpx.Response,
    received_at: datetime,
):
    day = received_at.strftime(
        "%Y%m%d"
    )

    outdir = RAW_ROOT / day

    outdir.mkdir(
        parents=True,
        exist_ok=True,
    )

    stamp = received_at.strftime(
        "%Y%m%dT%H%M%S.%fZ"
    )

    path = (
        outdir
        / (
            f"{stamp}_"
            f"{uuid.uuid4().hex[:8]}.json"
        )
    )

    path.write_bytes(
        response.content
    )

    return str(
        path.relative_to(REPO)
    )


def connect_db():
    password = ENV.get(
        "PG_PASSWORD"
    )

    if not password:
        raise RuntimeError(
            "PG_PASSWORD missing from .env"
        )

    return psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="quantlab",
        user="quantlab",
        password=password,
    )


def git_head():
    try:
        return subprocess.check_output(
            [
                "git",
                "rev-parse",
                "--short",
                "HEAD",
            ],
            cwd=REPO,
            text=True,
        ).strip()
    except Exception:
        return "unknown"


def load_and_validate_contract():
    if not CONTRACT_PATH.exists():
        raise RuntimeError(
            f"missing contract: "
            f"{CONTRACT_PATH}"
        )

    actual = sha256(
        CONTRACT_PATH
    )

    if actual != EXPECTED_CONTRACT_SHA:
        raise RuntimeError(
            "H2-v2 M1 contract SHA mismatch\n"
            f"expected={EXPECTED_CONTRACT_SHA}\n"
            f"actual={actual}"
        )

    contract = json.loads(
        CONTRACT_PATH.read_text(
            encoding="utf-8"
        )
    )

    sharp = contract[
        "sharp_source"
    ]

    if (
        sharp[
            "maximum_source_age_seconds"
        ]
        != 25
    ):
        raise RuntimeError(
            "contract max source age "
            "is not 25 seconds"
        )

    if (
        sharp[
            "required_fresh_families"
        ]
        != 2
    ):
        raise RuntimeError(
            "contract does not require "
            "exactly >=2 fresh families"
        )

    if set(
        sharp["bookmakers"]
    ) != set(BOOKMAKERS):
        raise RuntimeError(
            "collector bookmaker set "
            "does not match contract"
        )

    return contract


def validate_db_schema(conn):
    required = {
        "h2_v2_sharp_polls",
        "h2_v2_exchange_quotes",
        "h2_v2_sharp_consensus",
        "odds_games",
        "collector_runs",
    }

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='public'
            """
        )

        existing = {
            row[0]
            for row in cur.fetchall()
        }

    missing = (
        required
        - existing
    )

    if missing:
        raise RuntimeError(
            "missing DB tables: "
            + ", ".join(
                sorted(missing)
            )
        )


def create_collector_run(
    conn,
    requested_polls,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO collector_runs (
                component,
                started_at,
                version_sha,
                notes
            )
            VALUES (
                %s,
                %s,
                %s,
                %s
            )
            RETURNING id
            """,
            (
                "h2_v2_sharp_collector",
                utcnow(),
                git_head(),
                (
                    f"collector={COLLECTOR_VERSION}; "
                    f"contract={EXPECTED_CONTRACT_SHA}; "
                    f"requested_polls={requested_polls}"
                ),
            ),
        )

        run_id = cur.fetchone()[0]

    conn.commit()

    return run_id


def upsert_game(
    cur,
    game,
):
    oddsapi_id = str(
        game.get("id")
    )

    commence = parse_dt(
        game.get(
            "commence_time"
        )
    )

    home = game.get(
        "home_team"
    )

    away = game.get(
        "away_team"
    )

    if (
        not oddsapi_id
        or oddsapi_id == "None"
        or commence is None
        or not home
        or not away
    ):
        return None

    cur.execute(
        """
        INSERT INTO odds_games (
            sport_key,
            oddsapi_id,
            commence_time,
            home_team,
            away_team
        )
        VALUES (
            %s,%s,%s,%s,%s
        )
        ON CONFLICT (
            sport_key,
            oddsapi_id
        )
        DO UPDATE SET
            commence_time =
                EXCLUDED.commence_time,
            home_team =
                EXCLUDED.home_team,
            away_team =
                EXCLUDED.away_team
        RETURNING id
        """,
        (
            SPORT,
            oddsapi_id,
            commence,
            home,
            away,
        ),
    )

    game_id = cur.fetchone()[0]

    return {
        "id": game_id,
        "commence": commence,
        "home": str(home),
        "away": str(away),
    }


def insert_poll_row(
    cur,
    *,
    requested_at,
    received_at,
    latency_ms,
    response,
    games_returned,
    run_id,
    raw_ref,
):
    cur.execute(
        """
        INSERT INTO h2_v2_sharp_polls (
            requested_at,
            received_at,
            request_latency_ms,
            sport_key,
            requested_market,
            requested_bookmakers,
            request_params,
            http_status,
            games_returned,
            credits_last,
            credits_used,
            credits_remaining,
            contract_sha256,
            collector_version,
            collector_run_id,
            raw_ref
        )
        VALUES (
            %s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s
        )
        RETURNING id
        """,
        (
            requested_at,
            received_at,
            latency_ms,
            SPORT,
            "h2h",
            BOOKMAKERS,
            Json(
                safe_request_params()
            ),
            response.status_code,
            games_returned,
            header_int(
                response.headers,
                "x-requests-last",
            ),
            header_int(
                response.headers,
                "x-requests-used",
            ),
            header_int(
                response.headers,
                "x-requests-remaining",
            ),
            EXPECTED_CONTRACT_SHA,
            COLLECTOR_VERSION,
            run_id,
            raw_ref,
        ),
    )

    return cur.fetchone()[0]


def insert_quote(
    cur,
    *,
    poll_id,
    game_id,
    bookmaker,
    team,
    is_home,
    capture_time,
    source_update,
    source_age,
    back_outcome,
    lay_outcome,
    family_pair_complete,
    run_id,
):
    back = decimal_price(
        back_outcome
    )

    lay = decimal_price(
        lay_outcome
    )

    p_back = probability(
        back
    )

    p_lay = probability(
        lay
    )

    midpoint = None

    if (
        p_back is not None
        and p_lay is not None
    ):
        midpoint = (
            p_back
            + p_lay
        ) / Decimal("2")

    fresh = (
        family_pair_complete
        and source_age is not None
        and Decimal("0")
        <= source_age
        <= MAX_SOURCE_AGE_SECONDS
    )

    cur.execute(
        """
        INSERT INTO h2_v2_exchange_quotes (
            poll_id,
            odds_game_id,
            bookmaker,
            outcome_team,
            is_home,
            capture_time,
            source_update_time,
            source_age_seconds,
            back_decimal,
            lay_decimal,
            p_back,
            p_lay,
            family_midpoint,
            pair_complete,
            is_fresh,
            raw_back_outcome,
            raw_lay_outcome,
            collector_run_id
        )
        VALUES (
            %s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s
        )
        """,
        (
            poll_id,
            game_id,
            bookmaker,
            team,
            is_home,
            capture_time,
            source_update,
            source_age,
            back,
            lay,
            p_back,
            p_lay,
            midpoint,
            family_pair_complete,
            fresh,
            (
                Json(back_outcome)
                if back_outcome
                else None
            ),
            (
                Json(lay_outcome)
                if lay_outcome
                else None
            ),
            run_id,
        ),
    )

    return {
        "bookmaker":
            bookmaker,

        "pair_complete":
            family_pair_complete,

        "fresh":
            fresh,

        "midpoint":
            midpoint,

        "age":
            source_age,
    }


def insert_consensus(
    cur,
    *,
    poll_id,
    game_id,
    team,
    is_home,
    capture_time,
    observations,
    run_id,
):
    paired = [
        x
        for x in observations
        if (
            x["pair_complete"]
            and x["midpoint"]
            is not None
        )
    ]

    fresh = [
        x
        for x in paired
        if x["fresh"]
    ]

    paired_count = len(
        paired
    )

    fresh_count = len(
        fresh
    )

    eligible = (
        fresh_count >= 2
    )

    used = [
        x["bookmaker"]
        for x in fresh
    ]

    midpoint_map = {
        x["bookmaker"]:
            float(x["midpoint"])
        for x in paired
    }

    consensus = None

    if eligible:
        consensus = median(
            [
                x["midpoint"]
                for x in fresh
            ]
        )

    fresh_ages = [
        x["age"]
        for x in fresh
        if x["age"] is not None
    ]

    min_age = (
        min(fresh_ages)
        if fresh_ages
        else None
    )

    max_age = (
        max(fresh_ages)
        if fresh_ages
        else None
    )

    cur.execute(
        """
        INSERT INTO h2_v2_sharp_consensus (
            poll_id,
            odds_game_id,
            outcome_team,
            is_home,
            capture_time,
            paired_family_count,
            fresh_family_count,
            families_used,
            family_midpoints,
            min_family_age_seconds,
            max_family_age_seconds,
            consensus_prob,
            eligible,
            collector_run_id
        )
        VALUES (
            %s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s
        )
        """,
        (
            poll_id,
            game_id,
            team,
            is_home,
            capture_time,
            paired_count,
            fresh_count,
            used,
            Json(
                midpoint_map
            ),
            min_age,
            max_age,
            consensus,
            eligible,
            run_id,
        ),
    )

    return eligible


def process_game(
    cur,
    *,
    poll_id,
    game,
    capture_time,
    run_id,
):
    identity = upsert_game(
        cur,
        game,
    )

    if identity is None:
        return {
            "processed": False,
        }

    # Strict pregame gate.
    if not (
        capture_time
        < identity["commence"]
    ):
        return {
            "processed": False,
            "in_play": True,
        }

    game_id = identity["id"]

    home = identity["home"]
    away = identity["away"]

    expected_teams = [
        home,
        away,
    ]

    books = {
        str(b.get("key")): b
        for b in game.get(
            "bookmakers",
            []
        )
        if b.get("key")
    }

    by_team = {
        home: [],
        away: [],
    }

    quote_rows = 0

    for bookmaker in BOOKMAKERS:
        book = books.get(
            bookmaker
        )

        source_update = (
            parse_dt(
                book.get(
                    "last_update"
                )
            )
            if book
            else None
        )

        source_age = None

        if source_update is not None:
            source_age = Decimal(
                str(
                    (
                        capture_time
                        - source_update
                    ).total_seconds()
                )
            )

        markets = {
            str(m.get("key")): m
            for m in (
                book.get(
                    "markets",
                    []
                )
                if book
                else []
            )
            if m.get("key")
        }

        backs = outcomes_by_name(
            markets.get("h2h")
        )

        lays = outcomes_by_name(
            markets.get(
                "h2h_lay"
            )
        )

        # Valid family requires valid back and lay
        # for BOTH expected MLB teams.
        family_pair_complete = all(
            (
                decimal_price(
                    backs.get(team)
                )
                is not None
                and
                decimal_price(
                    lays.get(team)
                )
                is not None
            )
            for team in expected_teams
        )

        for team in expected_teams:
            obs = insert_quote(
                cur,
                poll_id=poll_id,
                game_id=game_id,
                bookmaker=bookmaker,
                team=team,
                is_home=(
                    team == home
                ),
                capture_time=capture_time,
                source_update=source_update,
                source_age=source_age,
                back_outcome=backs.get(
                    team
                ),
                lay_outcome=lays.get(
                    team
                ),
                family_pair_complete=(
                    family_pair_complete
                ),
                run_id=run_id,
            )

            by_team[
                team
            ].append(obs)

            quote_rows += 1

    eligible_outcomes = 0

    for team in expected_teams:
        eligible = insert_consensus(
            cur,
            poll_id=poll_id,
            game_id=game_id,
            team=team,
            is_home=(
                team == home
            ),
            capture_time=capture_time,
            observations=(
                by_team[team]
            ),
            run_id=run_id,
        )

        eligible_outcomes += int(
            eligible
        )

    return {
        "processed": True,
        "quote_rows": quote_rows,
        "consensus_rows": 2,
        "eligible_outcomes":
            eligible_outcomes,
        "eligible_game":
            eligible_outcomes == 2,
    }


def collect_one(
    conn,
    http,
    api_key,
    run_id,
    poll_no,
):
    requested_at = utcnow()

    t0 = time.monotonic()

    response = http.get(
        URL,
        params=api_request_params(
            api_key
        ),
    )

    received_at = utcnow()

    latency_ms = Decimal(
        str(
            (
                time.monotonic()
                - t0
            )
            * 1000
        )
    )

    raw_ref = save_raw_response(
        response,
        received_at,
    )

    games = None

    if response.status_code == 200:
        games = response.json()

        if not isinstance(
            games,
            list,
        ):
            raise RuntimeError(
                "Odds API response "
                "is not a list"
            )

    with conn.cursor() as cur:
        poll_id = insert_poll_row(
            cur,
            requested_at=requested_at,
            received_at=received_at,
            latency_ms=latency_ms,
            response=response,
            games_returned=(
                len(games)
                if games is not None
                else None
            ),
            run_id=run_id,
            raw_ref=raw_ref,
        )

        if response.status_code != 200:
            conn.commit()

            response.raise_for_status()

        processed = 0
        skipped_started = 0
        quote_rows = 0
        consensus_rows = 0
        eligible_outcomes = 0
        eligible_games = 0

        for game in games:
            result = process_game(
                cur,
                poll_id=poll_id,
                game=game,
                capture_time=received_at,
                run_id=run_id,
            )

            if result.get(
                "in_play"
            ):
                skipped_started += 1
                continue

            if not result.get(
                "processed"
            ):
                continue

            processed += 1

            quote_rows += result[
                "quote_rows"
            ]

            consensus_rows += result[
                "consensus_rows"
            ]

            eligible_outcomes += result[
                "eligible_outcomes"
            ]

            eligible_games += int(
                result[
                    "eligible_game"
                ]
            )

    conn.commit()

    remaining = header_int(
        response.headers,
        "x-requests-remaining",
    )

    used = header_int(
        response.headers,
        "x-requests-used",
    )

    last = header_int(
        response.headers,
        "x-requests-last",
    )

    print()
    print(
        f"POLL {poll_no}"
    )

    print(
        "  poll_id:",
        poll_id,
    )

    print(
        "  HTTP:",
        response.status_code,
    )

    print(
        "  request latency:",
        f"{latency_ms:.1f} ms",
    )

    print(
        "  games returned:",
        len(games),
    )

    print(
        "  pregame games stored:",
        processed,
    )

    print(
        "  already-started skipped:",
        skipped_started,
    )

    print(
        "  quote rows:",
        quote_rows,
    )

    print(
        "  consensus rows:",
        consensus_rows,
    )

    print(
        "  eligible outcomes:",
        eligible_outcomes,
    )

    print(
        "  eligible games:",
        eligible_games,
        "/",
        processed,
    )

    print(
        "  credits last:",
        last,
    )

    print(
        "  credits used:",
        used,
    )

    print(
        "  credits remaining:",
        remaining,
    )

    print(
        "  raw:",
        raw_ref,
    )

    return {
        "poll_id":
            poll_id,

        "remaining":
            remaining,

        "processed":
            processed,

        "eligible_games":
            eligible_games,
    }


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--validate-config",
        action="store_true",
    )

    parser.add_argument(
        "--polls",
        type=int,
        default=1,
    )

    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=(
            DEFAULT_INTERVAL_SECONDS
        ),
    )

    args = parser.parse_args()

    load_and_validate_contract()

    conn = connect_db()

    try:
        validate_db_schema(
            conn
        )

        if args.validate_config:
            print(
                "H2-v2 M1 COLLECTOR "
                "CONFIG VALIDATION PASSED"
            )

            print(
                "contract SHA:",
                sha256(
                    CONTRACT_PATH
                ),
            )

            print(
                "bookmakers:",
                ", ".join(
                    BOOKMAKERS
                ),
            )

            print(
                "max source age:",
                (
                    f"{MAX_SOURCE_AGE_SECONDS}"
                    "s"
                ),
            )

            print(
                "API CALLED: NO"
            )

            return

        if not (
            1
            <= args.polls
            <= MAX_POLLS_PER_PROCESS
        ):
            raise SystemExit(
                "polls must be between "
                "1 and 60"
            )

        if (
            args.polls > 1
            and args.interval_seconds <= 0
        ):
            raise SystemExit(
                "interval must be >0"
            )

        api_key = ENV.get(
            "ODDS_API_KEY"
        )

        if not api_key:
            raise SystemExit(
                "ODDS_API_KEY missing "
                "from .env"
            )

        run_id = create_collector_run(
            conn,
            args.polls,
        )

        print(
            "collector_run_id:",
            run_id,
        )

        print(
            "contract:",
            EXPECTED_CONTRACT_SHA,
        )

        print(
            "polls requested:",
            args.polls,
        )

        print(
            "interval:",
            args.interval_seconds,
        )

        print(
            "quota safety floor:",
            MIN_REMAINING_CREDITS,
        )

        with httpx.Client(
            headers=UA,
            timeout=30,
        ) as http:

            for i in range(
                1,
                args.polls + 1,
            ):
                result = collect_one(
                    conn,
                    http,
                    api_key,
                    run_id,
                    i,
                )

                remaining = result[
                    "remaining"
                ]

                if (
                    remaining is not None
                    and remaining
                    <= MIN_REMAINING_CREDITS
                ):
                    print(
                        "STOPPING: quota "
                        "safety floor reached"
                    )

                    break

                if i < args.polls:
                    time.sleep(
                        args.interval_seconds
                    )

        print()
        print(
            "H2-v2 M1 SHARP "
            "COLLECTION COMPLETE"
        )

    finally:
        conn.close()


if __name__ == "__main__":
    main()
