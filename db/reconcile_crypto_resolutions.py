#!/usr/bin/env python3
"""Reconcile missing resolutions for 5-minute crypto markets from Gamma.

Why this exists:
- WS market_resolved events are useful but ephemeral.
- If the WS collector is disconnected as a slow consumer, resolution events can
  be missed permanently.
- This script finds ended crypto_harvester markets without a resolution, fetches
  each market by slug from Gamma, validates a final 1/0 outcome, and inserts the
  winner idempotently.

Examples:
  .venv/bin/python db/reconcile_crypto_resolutions.py --dry-run
  .venv/bin/python db/reconcile_crypto_resolutions.py --lookback-hours 24
  .venv/bin/python db/reconcile_crypto_resolutions.py --lookback-hours 48 --limit 1000
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
import psycopg2
from dotenv import dotenv_values

REPO = Path(__file__).resolve().parents[1]
ENV = dotenv_values(REPO / ".env")

GAMMA_BASE = "https://gamma-api.polymarket.com"
LOG = logging.getLogger("crypto_resolution_reconciler")


def connect():
    password = ENV.get("PG_PASSWORD")
    if not password:
        raise RuntimeError("PG_PASSWORD is missing from .env")
    return psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="quantlab",
        user="quantlab",
        password=password,
    )


def git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=REPO,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return "unknown"


def jloads_maybe(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def parse_iso(value: Any) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def exact_binary_winner(market: dict[str, Any]) -> tuple[int, str, str | None] | None:
    """Return (winner index, winning outcome label, winning venue token id).

    Accept only a closed binary market with exactly one price at 1 and one at 0.
    """
    if market.get("closed") is not True:
        return None

    outcomes = jloads_maybe(market.get("outcomes"))
    prices_raw = jloads_maybe(market.get("outcomePrices"))
    venue_tokens = jloads_maybe(market.get("clobTokenIds"))

    if len(outcomes) != 2 or len(prices_raw) != 2:
        return None

    try:
        prices = [Decimal(str(x)) for x in prices_raw]
    except (InvalidOperation, ValueError):
        return None

    winners = [i for i, p in enumerate(prices) if p == Decimal("1")]
    losers = [i for i, p in enumerate(prices) if p == Decimal("0")]
    if len(winners) != 1 or len(losers) != 1:
        return None

    idx = winners[0]
    venue_token = str(venue_tokens[idx]) if len(venue_tokens) == 2 else None
    return idx, str(outcomes[idx]), venue_token


def fetch_gamma_market(client: httpx.Client, slug: str) -> dict[str, Any] | None:
    url = f"{GAMMA_BASE}/markets/slug/{quote(slug, safe='')}"
    response = client.get(url)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    payload = response.json()
    return payload if isinstance(payload, dict) else None


def unresolved_markets(cur, lookback_hours: int, grace_sec: int, limit: int):
    cur.execute(
        """
        SELECT m.id, m.slug, m.end_date
        FROM markets m
        LEFT JOIN resolutions r ON r.market_id = m.id
        WHERE m.source = 'crypto_harvester'
          AND m.slug IS NOT NULL
          AND m.end_date < now() - (%s * interval '1 second')
          AND m.end_date >= now() - (%s * interval '1 hour')
          AND r.market_id IS NULL
        ORDER BY m.end_date ASC, m.id ASC
        LIMIT %s
        """,
        (grace_sec, lookback_hours, limit),
    )
    return cur.fetchall()


def internal_winning_token(cur, market_id: int, winner_idx: int):
    cur.execute(
        """
        SELECT id, venue_token_id
        FROM tokens
        WHERE market_id = %s AND outcome_index = %s
        ORDER BY id
        LIMIT 1
        """,
        (market_id, winner_idx),
    )
    return cur.fetchone()


def insert_resolution(
    cur,
    *,
    market_id: int,
    event_time: datetime,
    capture_time: datetime,
    winning_token_id: int,
    winning_outcome: str,
    raw_ref: str,
    run_id: int,
) -> bool:
    cur.execute(
        """
        INSERT INTO resolutions (
            market_id, event_time, capture_time, winning_token_id,
            winning_outcome, learned_via, status, raw_ref, collector_run_id
        )
        SELECT %s, %s, %s, %s, %s,
               'gamma_crypto_reconcile', 'final', %s, %s
        WHERE NOT EXISTS (
            SELECT 1 FROM resolutions WHERE market_id = %s
        )
        ON CONFLICT DO NOTHING
        RETURNING market_id
        """,
        (
            market_id,
            event_time,
            capture_time,
            winning_token_id,
            winning_outcome,
            raw_ref,
            run_id,
            market_id,
        ),
    )
    inserted = cur.fetchone() is not None

    cur.execute(
        """
        INSERT INTO market_status (
            market_id, observed_at, status, source, raw_ref, collector_run_id
        )
        VALUES (%s, %s, 'resolved', 'gamma_crypto_reconcile', %s, %s)
        ON CONFLICT DO NOTHING
        """,
        (market_id, capture_time, raw_ref, run_id),
    )
    return inserted


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lookback-hours", type=int, default=24)
    parser.add_argument("--grace-sec", type=int, default=120)
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--sleep-sec", type=float, default=0.05)
    parser.add_argument("--timeout-sec", type=float, default=10.0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.lookback_hours <= 0 or args.grace_sec < 0 or args.limit <= 0:
        parser.error("lookback-hours and limit must be positive; grace-sec cannot be negative")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)sZ %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    conn = connect()
    conn.autocommit = False

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO collector_runs (
                component, started_at, version_sha, notes
            )
            VALUES ('crypto_resolution_reconciler', now(), %s, %s)
            RETURNING id
            """,
            (git_sha(), f"args={vars(args)}"),
        )
        run_id = cur.fetchone()[0]
    conn.commit()

    stats = {
        "candidates": 0,
        "inserted": 0,
        "already_present": 0,
        "gamma_not_found": 0,
        "not_final": 0,
        "missing_internal_token": 0,
        "token_mismatch": 0,
        "http_error": 0,
        "other_error": 0,
    }

    headers = {"User-Agent": "quantLab-crypto-resolution-reconciler/1.0"}
    with httpx.Client(
        timeout=args.timeout_sec,
        follow_redirects=True,
        headers=headers,
    ) as client:
        with conn.cursor() as cur:
            rows = unresolved_markets(
                cur,
                args.lookback_hours,
                args.grace_sec,
                args.limit,
            )

        stats["candidates"] = len(rows)
        LOG.info("found %d ended unresolved crypto markets", len(rows))

        for market_id, slug, end_date in rows:
            capture_time = datetime.now(timezone.utc)
            raw_ref = f"gamma:markets/slug:{slug}"

            try:
                market = fetch_gamma_market(client, slug)
                if market is None:
                    stats["gamma_not_found"] += 1
                    LOG.warning("Gamma market not found: %s", slug)
                    continue

                winner = exact_binary_winner(market)
                if winner is None:
                    stats["not_final"] += 1
                    continue

                winner_idx, winner_label, gamma_winning_venue_token = winner

                with conn.cursor() as cur:
                    token_row = internal_winning_token(cur, market_id, winner_idx)
                    if token_row is None:
                        stats["missing_internal_token"] += 1
                        LOG.error(
                            "missing internal token market_id=%s winner_idx=%s slug=%s",
                            market_id,
                            winner_idx,
                            slug,
                        )
                        conn.rollback()
                        continue

                    winning_token_id, internal_venue_token = token_row
                    if (
                        gamma_winning_venue_token
                        and internal_venue_token
                        and str(internal_venue_token) != gamma_winning_venue_token
                    ):
                        stats["token_mismatch"] += 1
                        LOG.error(
                            "token mismatch slug=%s idx=%s gamma=%s internal=%s",
                            slug,
                            winner_idx,
                            gamma_winning_venue_token,
                            internal_venue_token,
                        )
                        conn.rollback()
                        continue

                    closed_time = (
                        parse_iso(market.get("closedTime"))
                        or end_date
                        or capture_time
                    )

                    if args.dry_run:
                        LOG.info(
                            "DRY RUN resolve slug=%s idx=%s outcome=%s token_id=%s",
                            slug,
                            winner_idx,
                            winner_label,
                            winning_token_id,
                        )
                        conn.rollback()
                        continue

                    inserted = insert_resolution(
                        cur,
                        market_id=market_id,
                        event_time=closed_time,
                        capture_time=capture_time,
                        winning_token_id=winning_token_id,
                        winning_outcome=winner_label,
                        raw_ref=raw_ref,
                        run_id=run_id,
                    )
                    conn.commit()

                if inserted:
                    stats["inserted"] += 1
                    LOG.info(
                        "resolved slug=%s idx=%s outcome=%s token_id=%s",
                        slug,
                        winner_idx,
                        winner_label,
                        winning_token_id,
                    )
                else:
                    stats["already_present"] += 1

            except httpx.HTTPError as exc:
                conn.rollback()
                stats["http_error"] += 1
                LOG.error("HTTP failure slug=%s: %s", slug, exc)
            except Exception:
                conn.rollback()
                stats["other_error"] += 1
                LOG.exception("failed slug=%s market_id=%s", slug, market_id)

            if args.sleep_sec:
                time.sleep(args.sleep_sec)

    conn.close()
    LOG.info("summary %s", json.dumps(stats, sort_keys=True))
    print(json.dumps(stats, indent=2, sort_keys=True))
    return 0 if stats["other_error"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())