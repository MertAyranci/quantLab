"""H2-v2 collision-safe Odds API -> Polymarket mapper.

Mapping rule:
  1. exact normalized MLB team set
  2. sportsMarketType == moneyline
  3. exactly two CLOB tokens
  4. active == True
  5. closed == False
  6. acceptingOrders == True
  7. |Gamma gameStartTime - Odds commence_time| <= 10 minutes
  8. EXACTLY ONE candidate satisfies 1-7

Anything else is left unmapped.

Slug date is deliberately NOT used.

The script:
  - searches live Gamma
  - stores raw search responses
  - inserts missing PM market/token metadata
  - records H2-v2 mapping provenance
  - updates odds_games.polymarket_market_id only after unique certification

It does NOT:
  - call The Odds API
  - calculate signals
  - calculate PnL
  - trade

Usage:
    .venv/bin/python collectors/odds/h2_v2_event_mapper.py --dry-run
    .venv/bin/python collectors/odds/h2_v2_event_mapper.py
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path

import httpx
import psycopg2
from dotenv import dotenv_values


REPO = Path(__file__).resolve().parents[2]
ENV = dotenv_values(REPO / ".env")

GAMMA_SEARCH = (
    "https://gamma-api.polymarket.com/public-search"
)

CONTRACT = (
    REPO
    / "research"
    / "h2_v2_m1_data_contract.json"
)

CONTRACT_SHA = (
    "c539abbf046e173d3f50f57b071d483b"
    "cb549b034cf9e7716151fe1752d1143b"
)

RAW_ROOT = (
    REPO
    / "data"
    / "raw"
    / "h2_v2_mapping"
)

MAX_DELTA_SECONDS = 600

MAPPING_RULE = (
    "exact_team_set+moneyline+2_tokens+"
    "active_open_accepting+"
    "unique_abs_start_delta_le_600s"
)

UA = {
    "User-Agent":
        "quant-lab-h2-v2-mapper/1.0"
}


def utcnow():
    return datetime.now(
        timezone.utc
    )


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
                "--short",
                "HEAD",
            ],
            cwd=REPO,
            text=True,
        ).strip()
    except Exception:
        return "unknown"


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


def norm_team(name):
    n = (
        name
        or ""
    ).strip().lower()

    n = n.replace(
        ".",
        "",
    )

    n = n.replace(
        "-",
        " ",
    )

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


def parse_matchup(question):
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
        norm_team(
            m.group(1)
        ),
        norm_team(
            m.group(2)
        ),
    }


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


def outcomes_list(market):
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


def save_raw(
    payload,
    *,
    odds_game_id,
    captured_at,
):
    day = captured_at.strftime(
        "%Y%m%d"
    )

    outdir = (
        RAW_ROOT
        / day
    )

    outdir.mkdir(
        parents=True,
        exist_ok=True,
    )

    stamp = captured_at.strftime(
        "%Y%m%dT%H%M%S.%fZ"
    )

    path = (
        outdir
        / (
            f"game_{odds_game_id}_"
            f"{stamp}_"
            f"{uuid.uuid4().hex[:8]}.json"
        )
    )

    envelope = {
        "captured_at_utc":
            captured_at.isoformat(),

        "odds_game_id":
            odds_game_id,

        "payload":
            payload,
    }

    path.write_text(
        json.dumps(
            envelope,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )

    return str(
        path.relative_to(
            REPO
        )
    )


def connect():
    return psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="quantlab",
        user="quantlab",
        password=ENV[
            "PG_PASSWORD"
        ],
    )


def latest_h2_games(cur):
    cur.execute(
        """
        WITH latest AS (
            SELECT max(id) AS poll_id
            FROM h2_v2_sharp_polls
        ),
        games AS (
            SELECT DISTINCT q.odds_game_id
            FROM h2_v2_exchange_quotes q
            JOIN latest l
              ON l.poll_id = q.poll_id
        )
        SELECT
            g.id,
            g.commence_time,
            g.away_team,
            g.home_team,
            g.polymarket_market_id
        FROM games x
        JOIN odds_games g
          ON g.id = x.odds_game_id
        ORDER BY
            g.commence_time,
            g.id
        """
    )

    return cur.fetchall()


def valid_candidates(
    data,
    *,
    away,
    home,
    commence,
):
    wanted = {
        norm_team(away),
        norm_team(home),
    }

    exact_team_candidates = {}

    eligible = {}

    for event in (
        data.get("events")
        or []
    ):
        for market in (
            event.get("markets")
            or []
        ):
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
            )

            if not gamma_id:
                continue

            exact_team_candidates[
                gamma_id
            ] = market

            start = parse_dt(
                market.get(
                    "gameStartTime"
                )
            )

            tokens = token_list(
                market
            )

            if (
                market.get(
                    "sportsMarketType"
                )
                != "moneyline"
            ):
                continue

            if len(tokens) != 2:
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

            if start is None:
                continue

            delta = (
                start
                - commence
            ).total_seconds()

            if (
                abs(delta)
                > MAX_DELTA_SECONDS
            ):
                continue

            eligible[
                gamma_id
            ] = {
                "market":
                    market,

                "start":
                    start,

                "delta":
                    delta,
            }

    return (
        list(
            exact_team_candidates.values()
        ),
        list(
            eligible.values()
        ),
    )


def upsert_market(
    cur,
    market,
    *,
    capture,
    raw_ref,
    run_id,
):
    gamma_id = str(
        market["id"]
    )

    cur.execute(
        """
        INSERT INTO markets (
            venue_id,
            venue_market_id,
            condition_id,
            slug,
            question,
            resolution_source_text,
            end_date,
            first_seen_at,
            source,
            raw_ref,
            collector_run_id
        )
        VALUES (
            1,%s,%s,%s,%s,%s,%s,%s,
            'h2_v2_gamma_map',%s,%s
        )
        ON CONFLICT (
            venue_id,
            venue_market_id
        )
        DO UPDATE SET
            condition_id =
                COALESCE(
                    markets.condition_id,
                    EXCLUDED.condition_id
                ),
            slug =
                COALESCE(
                    markets.slug,
                    EXCLUDED.slug
                ),
            question =
                COALESCE(
                    markets.question,
                    EXCLUDED.question
                )
        RETURNING id
        """,
        (
            gamma_id,
            market.get(
                "conditionId"
            ),
            market.get(
                "slug"
            ),
            market.get(
                "question"
            ),
            market.get(
                "resolutionSource"
            ),
            parse_dt(
                market.get(
                    "endDate"
                )
            ),
            capture,
            raw_ref,
            run_id,
        ),
    )

    return cur.fetchone()[0]


def ensure_tokens(
    cur,
    *,
    market_id,
    market,
    raw_ref,
    run_id,
):
    tokens = token_list(
        market
    )

    outcomes = outcomes_list(
        market
    )

    if len(tokens) != 2:
        raise RuntimeError(
            "matched market does not "
            "have exactly 2 tokens"
        )

    result = []

    for index, venue_token in enumerate(
        tokens
    ):
        outcome = (
            outcomes[index]
            if index < len(outcomes)
            else None
        )

        cur.execute(
            """
            SELECT
                id,
                market_id,
                outcome_index
            FROM tokens
            WHERE venue_id = 1
              AND venue_token_id = %s
            """,
            (
                venue_token,
            ),
        )

        row = cur.fetchone()

        if row:
            internal_id, existing_mid, existing_index = row

            if existing_mid != market_id:
                raise RuntimeError(
                    "token already belongs "
                    "to another market"
                )

            if (
                existing_index is not None
                and existing_index
                != index
            ):
                raise RuntimeError(
                    "token outcome index "
                    "conflict"
                )

            cur.execute(
                """
                UPDATE tokens
                SET
                    outcome = COALESCE(
                        outcome,
                        %s
                    ),
                    outcome_index = COALESCE(
                        outcome_index,
                        %s
                    )
                WHERE id = %s
                """,
                (
                    outcome,
                    index,
                    internal_id,
                ),
            )

            result.append(
                internal_id
            )

            continue

        cur.execute(
            """
            INSERT INTO tokens (
                venue_id,
                market_id,
                venue_token_id,
                outcome,
                outcome_index,
                source,
                raw_ref,
                collector_run_id
            )
            VALUES (
                1,%s,%s,%s,%s,
                'h2_v2_gamma_map',
                %s,%s
            )
            RETURNING id
            """,
            (
                market_id,
                venue_token,
                outcome,
                index,
                raw_ref,
                run_id,
            ),
        )

        result.append(
            cur.fetchone()[0]
        )

    return result


def record_status(
    cur,
    *,
    market_id,
    market,
    capture,
    raw_ref,
    run_id,
):
    status = (
        "closed"
        if market.get("closed")
        else "active"
    )

    cur.execute(
        """
        INSERT INTO market_status (
            market_id,
            observed_at,
            status,
            source,
            raw_ref,
            collector_run_id
        )
        VALUES (
            %s,%s,%s,
            'h2_v2_gamma_map',
            %s,%s
        )
        ON CONFLICT DO NOTHING
        """,
        (
            market_id,
            capture,
            status,
            raw_ref,
            run_id,
        ),
    )


def record_tick(
    cur,
    *,
    token_ids,
    market,
    capture,
    raw_ref,
    run_id,
):
    tick = market.get(
        "orderPriceMinTickSize"
    )

    if tick is None:
        return

    tick_mc = int(
        float(tick)
        * 1000
    )

    if tick_mc <= 0:
        return

    for token_id in token_ids:
        cur.execute(
            """
            SELECT tick_mc
            FROM tick_sizes
            WHERE token_id = %s
            ORDER BY capture_time DESC
            LIMIT 1
            """,
            (
                token_id,
            ),
        )

        row = cur.fetchone()

        if (
            row
            and row[0] == tick_mc
        ):
            continue

        cur.execute(
            """
            INSERT INTO tick_sizes (
                token_id,
                capture_time,
                tick_mc,
                source,
                raw_ref,
                collector_run_id
            )
            VALUES (
                %s,%s,%s,
                'h2_v2_gamma_map',
                %s,%s
            )
            ON CONFLICT DO NOTHING
            """,
            (
                token_id,
                capture,
                tick_mc,
                raw_ref,
                run_id,
            ),
        )


def create_run(cur, dry_run):
    cur.execute(
        """
        INSERT INTO collector_runs (
            component,
            started_at,
            version_sha,
            notes
        )
        VALUES (
            'h2_v2_event_mapper',
            now(),
            %s,
            %s
        )
        RETURNING id
        """,
        (
            git_head(),
            (
                "dry_run="
                f"{dry_run}; "
                "rule="
                f"{MAPPING_RULE}; "
                "contract="
                f"{CONTRACT_SHA}"
            ),
        ),
    )

    return cur.fetchone()[0]


def main():
    ap = argparse.ArgumentParser()

    ap.add_argument(
        "--dry-run",
        action="store_true",
    )

    args = ap.parse_args()

    if sha256(CONTRACT) != CONTRACT_SHA:
        raise SystemExit(
            "H2-v2 contract SHA mismatch"
        )

    conn = connect()
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            games = latest_h2_games(
                cur
            )

            run_id = create_run(
                cur,
                args.dry_run,
            )

        if args.dry_run:
            # A dry run must leave absolutely
            # no DB changes, including collector_runs.
            conn.rollback()
            run_id = None
        else:
            conn.commit()

        counts = {
            "games": 0,
            "matched": 0,
            "no_candidate": 0,
            "ambiguous": 0,
            "already_same": 0,
        }

        with httpx.Client(
            timeout=30,
            headers=UA,
        ) as http:

            for (
                odds_game_id,
                commence,
                away,
                home,
                existing_pm,
            ) in games:

                counts[
                    "games"
                ] += 1

                capture = utcnow()

                response = http.get(
                    GAMMA_SEARCH,
                    params={
                        "q":
                            f"{away} {home}",

                        "limit_per_type":
                            20,

                        "keep_closed_markets":
                            1,

                        "search_profiles":
                            "false",

                        "search_tags":
                            "false",
                    },
                )

                response.raise_for_status()

                data = response.json()

                raw_ref = save_raw(
                    data,
                    odds_game_id=(
                        odds_game_id
                    ),
                    captured_at=capture,
                )

                exact, in_window = (
                    valid_candidates(
                        data,
                        away=away,
                        home=home,
                        commence=commence,
                    )
                )

                print()
                print(
                    f"{away} @ {home}"
                )

                print(
                    "  Odds start:",
                    commence.isoformat(),
                )

                print(
                    "  exact team candidates:",
                    len(exact),
                )

                print(
                    "  valid <=10m candidates:",
                    len(in_window),
                )

                if len(
                    in_window
                ) == 0:
                    counts[
                        "no_candidate"
                    ] += 1

                    print(
                        "  RESULT: NO MATCH"
                    )

                    continue

                if len(
                    in_window
                ) != 1:
                    counts[
                        "ambiguous"
                    ] += 1

                    print(
                        "  RESULT: AMBIGUOUS "
                        "-- NOT WRITTEN"
                    )

                    for x in in_window:
                        print(
                            "   ",
                            x["market"].get(
                                "id"
                            ),
                            x["market"].get(
                                "slug"
                            ),
                            (
                                f"delta="
                                f"{x['delta']:+.0f}s"
                            ),
                        )

                    continue

                chosen = (
                    in_window[0]
                )

                market = chosen[
                    "market"
                ]

                pm_start = chosen[
                    "start"
                ]

                delta = chosen[
                    "delta"
                ]

                print(
                    "  selected Gamma id:",
                    market.get("id"),
                )

                print(
                    "  slug:",
                    market.get("slug"),
                )

                print(
                    "  PM start:",
                    pm_start.isoformat(),
                )

                print(
                    "  delta:",
                    f"{delta:+.0f}s",
                )

                if args.dry_run:
                    print(
                        "  RESULT: MATCH "
                        "[dry-run]"
                    )

                    counts[
                        "matched"
                    ] += 1

                    continue

                with conn.cursor() as cur:
                    local_mid = upsert_market(
                        cur,
                        market,
                        capture=capture,
                        raw_ref=raw_ref,
                        run_id=run_id,
                    )

                    token_ids = ensure_tokens(
                        cur,
                        market_id=local_mid,
                        market=market,
                        raw_ref=raw_ref,
                        run_id=run_id,
                    )

                    if len(
                        token_ids
                    ) != 2:
                        raise RuntimeError(
                            "expected exactly "
                            "two local tokens"
                        )

                    # Protect any prior H2-v2
                    # provenance from remapping.
                    cur.execute(
                        """
                        SELECT
                            polymarket_market_id
                        FROM h2_v2_market_matches
                        WHERE odds_game_id = %s
                        """,
                        (
                            odds_game_id,
                        ),
                    )

                    prior = cur.fetchone()

                    if (
                        prior
                        and prior[0]
                        != local_mid
                    ):
                        raise RuntimeError(
                            "REFUSING REMAP: "
                            f"odds_game_id="
                            f"{odds_game_id}"
                        )

                    if (
                        existing_pm is not None
                        and existing_pm
                        != local_mid
                    ):
                        raise RuntimeError(
                            "REFUSING TO REPLACE "
                            "existing odds_games "
                            "Polymarket mapping"
                        )

                    record_status(
                        cur,
                        market_id=local_mid,
                        market=market,
                        capture=capture,
                        raw_ref=raw_ref,
                        run_id=run_id,
                    )

                    record_tick(
                        cur,
                        token_ids=token_ids,
                        market=market,
                        capture=capture,
                        raw_ref=raw_ref,
                        run_id=run_id,
                    )

                    cur.execute(
                        """
                        INSERT INTO
                        h2_v2_market_matches (
                            odds_game_id,
                            polymarket_market_id,
                            matched_at,
                            odds_commence_time,
                            pm_game_start_time,
                            start_delta_seconds,
                            exact_team_candidates,
                            in_window_candidates,
                            mapping_rule,
                            gamma_market_id,
                            gamma_slug,
                            gamma_question,
                            token_count,
                            active,
                            closed,
                            accepting_orders,
                            contract_sha256,
                            raw_ref,
                            collector_run_id
                        )
                        VALUES (
                            %s,%s,%s,%s,%s,%s,
                            %s,%s,%s,%s,%s,%s,
                            2,%s,%s,%s,%s,%s,%s
                        )
                        ON CONFLICT (
                            odds_game_id
                        )
                        DO NOTHING
                        """,
                        (
                            odds_game_id,
                            local_mid,
                            capture,
                            commence,
                            pm_start,
                            delta,
                            len(exact),
                            len(in_window),
                            MAPPING_RULE,
                            str(
                                market.get("id")
                            ),
                            market.get("slug"),
                            market.get(
                                "question"
                            ),
                            market.get(
                                "active"
                            ),
                            market.get(
                                "closed"
                            ),
                            market.get(
                                "acceptingOrders"
                            ),
                            CONTRACT_SHA,
                            raw_ref,
                            run_id,
                        ),
                    )

                    cur.execute(
                        """
                        UPDATE odds_games
                        SET
                            polymarket_market_id = %s,
                            match_confidence =
                                'h2_v2_time_exact'
                        WHERE id = %s
                          AND (
                              polymarket_market_id
                              IS NULL
                              OR
                              polymarket_market_id
                              = %s
                          )
                        """,
                        (
                            local_mid,
                            odds_game_id,
                            local_mid,
                        ),
                    )

                    if cur.rowcount != 1:
                        raise RuntimeError(
                            "odds_games mapping "
                            "update refused"
                        )

                conn.commit()

                counts[
                    "matched"
                ] += 1

                if (
                    existing_pm
                    == local_mid
                ):
                    counts[
                        "already_same"
                    ] += 1

                print(
                    "  RESULT: MATCH WRITTEN"
                )

        print()
        print("=" * 72)
        print("H2-v2 MAPPER SUMMARY")
        print("=" * 72)

        for k, v in counts.items():
            print(
                f"{k}: {v}"
            )

        print(
            "ODDS API CALLED: NO"
        )

        print(
            "DATABASE WRITES:",
            "NO"
            if args.dry_run
            else "YES",
        )

    finally:
        conn.close()


if __name__ == "__main__":
    main()
