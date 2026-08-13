"""Capture H2-v2 Polymarket CLOB market configuration.

Reads the 17 uniquely matched H2-v2 markets and queries:

    GET /clob-markets/{condition_id}

Captures:
    - game start time
    - minimum order size
    - minimum tick size
    - maker/taker base fee
    - platform fee curve
    - minimum order age
    - token identities

No Odds API calls.
No signals.
No PnL.
No trading.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import httpx
import psycopg2
from psycopg2.extras import Json
from dotenv import dotenv_values


REPO = Path(__file__).resolve().parents[2]

ENV = dotenv_values(
    REPO / ".env"
)

CLOB = (
    "https://clob.polymarket.com"
)

CONTRACT_PATH = (
    REPO
    / "research"
    / "h2_v2_m1_data_contract.json"
)

CONTRACT_SHA = (
    "c539abbf046e173d3f50f57b071d483b"
    "cb549b034cf9e7716151fe1752d1143b"
)

UA = {
    "User-Agent":
        "quant-lab-h2-v2-pm-config/1.0"
}


def utcnow():
    return datetime.now(
        timezone.utc
    )


def sha256_bytes(b):
    return hashlib.sha256(
        b
    ).hexdigest()


def sha256_file(path):
    return sha256_bytes(
        path.read_bytes()
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


def main():

    actual_contract_sha = (
        sha256_file(
            CONTRACT_PATH
        )
    )

    if (
        actual_contract_sha
        != CONTRACT_SHA
    ):
        raise SystemExit(
            "H2-v2 contract SHA mismatch"
        )

    conn = connect()
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    mm.polymarket_market_id,
                    m.condition_id,
                    g.away_team,
                    g.home_team,
                    g.commence_time
                FROM h2_v2_market_matches mm
                JOIN markets m
                  ON m.id =
                     mm.polymarket_market_id
                JOIN odds_games g
                  ON g.id =
                     mm.odds_game_id
                ORDER BY
                    g.commence_time,
                    g.id
                """
            )

            markets = cur.fetchall()

            if len(markets) != 17:
                raise RuntimeError(
                    "expected 17 certified "
                    f"H2-v2 markets, got "
                    f"{len(markets)}"
                )

            missing_condition = [
                row
                for row in markets
                if not row[1]
            ]

            if missing_condition:
                raise RuntimeError(
                    "matched market missing "
                    "condition_id"
                )

            cur.execute(
                """
                INSERT INTO collector_runs (
                    component,
                    started_at,
                    version_sha,
                    notes
                )
                VALUES (
                    'h2_v2_pm_config',
                    now(),
                    %s,
                    %s
                )
                RETURNING id
                """,
                (
                    git_head(),
                    (
                        "H2-v2 M1 live "
                        "CLOB market config; "
                        f"contract="
                        f"{CONTRACT_SHA}"
                    ),
                ),
            )

            run_id = (
                cur.fetchone()[0]
            )

        conn.commit()

        print(
            "collector_run_id:",
            run_id,
        )

        inserted = 0

        with httpx.Client(
            headers=UA,
            timeout=30,
        ) as http:

            for (
                market_id,
                condition_id,
                away,
                home,
                odds_start,
            ) in markets:

                observed = utcnow()

                r = http.get(
                    (
                        f"{CLOB}/"
                        f"clob-markets/"
                        f"{condition_id}"
                    )
                )

                r.raise_for_status()

                payload = r.json()

                raw_canonical = (
                    json.dumps(
                        payload,
                        sort_keys=True,
                        separators=(
                            ",",
                            ":",
                        ),
                    ).encode("utf-8")
                )

                raw_sha = (
                    sha256_bytes(
                        raw_canonical
                    )
                )

                tokens = (
                    payload.get("t")
                    or []
                )

                fd = (
                    payload.get("fd")
                    or {}
                )

                mos = payload.get(
                    "mos"
                )

                mts = payload.get(
                    "mts"
                )

                gst = parse_dt(
                    payload.get(
                        "gst"
                    )
                )

                if mos is None:
                    raise RuntimeError(
                        f"missing mos for "
                        f"market {market_id}"
                    )

                if mts is None:
                    raise RuntimeError(
                        f"missing mts for "
                        f"market {market_id}"
                    )

                if len(tokens) != 2:
                    raise RuntimeError(
                        f"expected 2 CLOB "
                        f"tokens for market "
                        f"{market_id}; "
                        f"got {len(tokens)}"
                    )

                # Strong identity check:
                # the two CLOB market-info
                # tokens must exactly equal
                # the two mapped DB tokens.
                clob_tokens = {
                    str(x.get("t"))
                    for x in tokens
                    if x.get("t")
                }

                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            venue_token_id
                        FROM tokens
                        WHERE market_id = %s
                        """,
                        (
                            market_id,
                        ),
                    )

                    db_tokens = {
                        str(x[0])
                        for x in cur.fetchall()
                    }

                if (
                    clob_tokens
                    != db_tokens
                ):
                    raise RuntimeError(
                        "CLOB/DB token "
                        "identity mismatch "
                        f"for market "
                        f"{market_id}"
                    )

                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO
                        h2_v2_pm_market_config (
                            polymarket_market_id,
                            observed_at,
                            condition_id,
                            game_start_time,
                            minimum_order_size,
                            minimum_tick_size,
                            maker_base_fee_bps,
                            taker_base_fee_bps,
                            fee_rate,
                            fee_exponent,
                            taker_only,
                            minimum_order_age_s,
                            token_count,
                            contract_sha256,
                            raw_response,
                            raw_sha256,
                            collector_run_id
                        )
                        VALUES (
                            %s,%s,%s,%s,%s,%s,
                            %s,%s,%s,%s,%s,%s,
                            %s,%s,%s,%s,%s
                        )
                        """,
                        (
                            market_id,
                            observed,
                            condition_id,
                            gst,
                            mos,
                            mts,
                            payload.get(
                                "mbf"
                            ),
                            payload.get(
                                "tbf"
                            ),
                            fd.get("r"),
                            fd.get("e"),
                            fd.get("to"),
                            payload.get(
                                "oas"
                            ),
                            len(tokens),
                            CONTRACT_SHA,
                            Json(payload),
                            raw_sha,
                            run_id,
                        ),
                    )

                conn.commit()

                inserted += 1

                delta = None

                if gst is not None:
                    delta = (
                        gst
                        - odds_start
                    ).total_seconds()

                print()
                print(
                    f"{away} @ {home}"
                )

                print(
                    "  market_id:",
                    market_id,
                )

                print(
                    "  mos:",
                    mos,
                )

                print(
                    "  mts:",
                    mts,
                )

                print(
                    "  fee:",
                    fd,
                )

                print(
                    "  maker_base_fee_bps:",
                    payload.get("mbf"),
                )

                print(
                    "  taker_base_fee_bps:",
                    payload.get("tbf"),
                )

                print(
                    "  start_delta_s:",
                    delta,
                )

        print()
        print(
            "H2-v2 PM CONFIG "
            "CAPTURE COMPLETE"
        )

        print(
            "markets inserted:",
            inserted,
        )

        print(
            "ODDS API CALLED: NO"
        )

    finally:
        conn.close()


if __name__ == "__main__":
    main()
