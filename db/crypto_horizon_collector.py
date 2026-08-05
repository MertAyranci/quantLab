#!/usr/bin/env python3
"""Deterministic full-book checkpoints for frozen crypto H4/H4b rules.

Captures both tokens for every retained 5-minute crypto cohort around the exact
backtest decision times:

    H4b signal      T-60
    H4b execution   T-59   (one-second latency)
    H4 signal       T-5
    H4 execution    T-4    (one-second latency)

Each target is probed twice shortly BEFORE the target. The backtester remains
strictly causal: it reads the latest fresh snapshot whose capture_time is <= the
target. The two probes improve the chance that at least one complete batch is
received before the target while still remaining within the reader's 3-second
freshness gate.

The collector uses the public CLOB POST /books batch endpoint, so all token books
for a cohort arrive in one HTTP response and share one local capture timestamp.
It writes BOTH:

  * book_snapshots  -- full depth, used for realistic fills
  * tob_snapshots   -- matching BBO, preventing a newer/contradictory source
                       selection artefact in crypto_book_reader.py

This process does not discover markets, alter watchlists, or write resolutions.
It is complementary to crypto_harvester.py and
reconcile_crypto_resolutions.py.

Usage:
  .venv/bin/python db/crypto_horizon_collector.py --dry-run
  .venv/bin/python db/crypto_horizon_collector.py

The default service configuration is intentionally frozen to:
  offsets:     60,59,5,4 seconds before end_date
  probe leads: 0.90,0.35 seconds before each target
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any

import httpx
import psycopg2
from dotenv import dotenv_values
from psycopg2.extras import Json

REPO = Path(__file__).resolve().parents[1]
ENV = dotenv_values(REPO / ".env")

CLOB_BOOKS_URL = "https://clob.polymarket.com/books"
SOURCE = "crypto_horizon_checkpoint"
WATCHLIST_RULE = "h4_frozen_horizons_v1"
USER_AGENT = {"User-Agent": "quantLab-crypto-horizon/1.0"}

LOG = logging.getLogger("crypto_horizon_collector")
STOP = False


@dataclass(frozen=True, order=True)
class CaptureEvent:
    trigger_time: datetime
    end_date: datetime
    offset_sec: int
    probe_index: int
    lead_sec: float

    @property
    def target_time(self) -> datetime:
        return self.end_date - timedelta(seconds=self.offset_sec)

    @property
    def label(self) -> str:
        return f"T-{self.offset_sec}:p{self.probe_index}"

    @property
    def raw_ref(self) -> str:
        epoch = int(self.end_date.timestamp())
        lead_ms = int(round(self.lead_sec * 1000))
        return (
            f"crypto_horizon:{epoch}:T-{self.offset_sec}:"
            f"p{self.probe_index}:lead{lead_ms}ms"
        )


@dataclass(frozen=True)
class TokenRow:
    market_id: int
    slug: str
    end_date: datetime
    token_id: int
    venue_token_id: str
    outcome_index: int


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_csv_ints(value: str) -> tuple[int, ...]:
    try:
        parsed = tuple(sorted({int(part.strip()) for part in value.split(",")}))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected comma-separated integers") from exc
    if not parsed or any(item <= 0 for item in parsed):
        raise argparse.ArgumentTypeError("all offsets must be positive")
    return parsed


def parse_csv_floats(value: str) -> tuple[float, ...]:
    try:
        parsed = tuple(sorted({float(part.strip()) for part in value.split(",")}, reverse=True))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected comma-separated numbers") from exc
    if not parsed or any(item <= 0 or item >= 1 for item in parsed):
        raise argparse.ArgumentTypeError("probe leads must be strictly between 0 and 1 second")
    return parsed


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


def create_run(conn, args) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO collector_runs (component, started_at, version_sha, notes)
            VALUES ('crypto_horizon_collector', now(), %s, %s)
            RETURNING id
            """,
            (git_sha(), f"args={vars(args)}"),
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def price_mc(value: Any) -> int | None:
    try:
        price = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    if price < 0 or price > 1:
        return None
    return int((price * 1000).to_integral_value(rounding=ROUND_HALF_UP))


def parse_levels(raw: Any, *, bids: bool) -> list[list[Any]]:
    levels: list[tuple[int, Decimal]] = []
    if not isinstance(raw, list):
        return []

    for level in raw:
        if not isinstance(level, dict):
            continue
        mc = price_mc(level.get("price"))
        try:
            size = Decimal(str(level.get("size")))
        except (InvalidOperation, ValueError, TypeError):
            continue
        if mc is None or size <= 0:
            continue
        levels.append((mc, size))

    levels.sort(key=lambda row: row[0], reverse=bids)
    return [[mc, str(size)] for mc, size in levels]


def parse_server_timestamp(value: Any, fallback: datetime) -> datetime:
    if value is None:
        return fallback

    text = str(value).strip()
    if not text:
        return fallback

    try:
        numeric = Decimal(text)
        # CLOB timestamps can be seconds or milliseconds.
        if numeric > Decimal("100000000000"):
            numeric /= Decimal(1000)
        return datetime.fromtimestamp(float(numeric), timezone.utc)
    except (InvalidOperation, ValueError, OSError, OverflowError):
        pass

    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return fallback


def best_level(levels: list[list[Any]]) -> tuple[int | None, float | None]:
    if not levels:
        return None, None
    return int(levels[0][0]), float(levels[0][1])


def upcoming_boundaries(conn, lookahead_sec: int) -> list[datetime]:
    now = utcnow()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT m.end_date
            FROM crypto_watch cw
            JOIN markets m ON m.id = cw.market_id
            WHERE m.end_date IS NOT NULL
              AND m.end_date >= %s
              AND m.end_date <= %s
            ORDER BY m.end_date
            """,
            (now - timedelta(seconds=10), now + timedelta(seconds=lookahead_sec)),
        )
        return [row[0] for row in cur.fetchall()]


def tokens_for_boundary(conn, end_date: datetime) -> list[TokenRow]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT m.id,
                   m.slug,
                   m.end_date,
                   tk.id,
                   tk.venue_token_id,
                   tk.outcome_index
            FROM crypto_watch cw
            JOIN markets m ON m.id = cw.market_id
            JOIN tokens tk ON tk.market_id = m.id
            WHERE m.end_date = %s
              AND tk.outcome_index IN (0, 1)
              AND tk.venue_token_id IS NOT NULL
            ORDER BY m.slug, tk.outcome_index
            """,
            (end_date,),
        )
        return [TokenRow(*row) for row in cur.fetchall()]


def existing_token_ids(conn, raw_ref: str) -> set[int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT token_id
            FROM book_snapshots
            WHERE source = %s AND raw_ref = %s
            """,
            (SOURCE, raw_ref),
        )
        return {row[0] for row in cur.fetchall()}


def insert_snapshot_pair(
    cur,
    *,
    token: TokenRow,
    book: dict[str, Any],
    capture_time: datetime,
    event: CaptureEvent,
    run_id: int,
) -> bool:
    bids = parse_levels(book.get("bids"), bids=True)
    asks = parse_levels(book.get("asks"), bids=False)
    if not bids and not asks:
        return False

    best_bid_mc, best_bid_size = best_level(bids)
    best_ask_mc, best_ask_size = best_level(asks)
    event_time = parse_server_timestamp(book.get("timestamp"), capture_time)
    venue_hash = book.get("hash")

    # Idempotency is keyed by token_id + raw_ref. One systemd instance is
    # expected, but this also makes restarts harmless.
    cur.execute(
        """
        INSERT INTO book_snapshots (
            token_id,
            event_time,
            capture_time,
            bids,
            asks,
            venue_hash,
            source,
            watchlist_rule,
            raw_ref,
            collector_run_id
        )
        SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        WHERE NOT EXISTS (
            SELECT 1
            FROM book_snapshots
            WHERE token_id = %s AND source = %s AND raw_ref = %s
        )
        """,
        (
            token.token_id,
            event_time,
            capture_time,
            Json(bids),
            Json(asks),
            venue_hash,
            SOURCE,
            WATCHLIST_RULE,
            event.raw_ref,
            run_id,
            token.token_id,
            SOURCE,
            event.raw_ref,
        ),
    )
    inserted = cur.rowcount > 0

    cur.execute(
        """
        INSERT INTO tob_snapshots (
            token_id,
            event_time,
            capture_time,
            best_bid_mc,
            best_bid_size,
            best_ask_mc,
            best_ask_size,
            source,
            watchlist_rule,
            raw_ref,
            collector_run_id
        )
        SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        WHERE NOT EXISTS (
            SELECT 1
            FROM tob_snapshots
            WHERE token_id = %s AND source = %s AND raw_ref = %s
        )
        """,
        (
            token.token_id,
            event_time,
            capture_time,
            best_bid_mc,
            best_bid_size,
            best_ask_mc,
            best_ask_size,
            SOURCE,
            WATCHLIST_RULE,
            event.raw_ref,
            run_id,
            token.token_id,
            SOURCE,
            event.raw_ref,
        ),
    )
    return inserted


def capture_event(
    conn,
    client: httpx.Client,
    event: CaptureEvent,
    run_id: int,
    dry_run: bool,
) -> dict[str, Any]:
    rows = tokens_for_boundary(conn, event.end_date)
    existing = existing_token_ids(conn, event.raw_ref)
    pending = [row for row in rows if row.token_id not in existing]

    summary = {
        "event": event.label,
        "end_date": event.end_date.isoformat(),
        "tokens_expected": len(rows),
        "tokens_existing": len(existing),
        "tokens_requested": len(pending),
        "books_returned": 0,
        "rows_written": 0,
        "capture_lag_ms": None,
    }

    if not rows:
        LOG.warning("%s boundary=%s has no tokens", event.label, event.end_date)
        return summary
    if not pending:
        LOG.info("%s boundary=%s already complete", event.label, event.end_date)
        return summary
    if dry_run:
        LOG.info(
            "DRY RUN %s boundary=%s tokens=%d trigger=%s target=%s",
            event.label,
            event.end_date,
            len(pending),
            event.trigger_time.isoformat(),
            event.target_time.isoformat(),
        )
        return summary

    payload = [{"token_id": row.venue_token_id} for row in pending]
    request_started = utcnow()
    response = client.post(CLOB_BOOKS_URL, json=payload)
    capture_time = utcnow()
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, list):
        raise ValueError(f"unexpected /books response type: {type(data).__name__}")

    by_venue_token = {
        str(book.get("asset_id")): book
        for book in data
        if isinstance(book, dict) and book.get("asset_id") is not None
    }
    summary["books_returned"] = len(by_venue_token)
    summary["capture_lag_ms"] = round(
        (capture_time - event.target_time).total_seconds() * 1000,
        1,
    )

    written = 0
    with conn.cursor() as cur:
        for token in pending:
            book = by_venue_token.get(token.venue_token_id)
            if book is None:
                LOG.warning(
                    "%s missing token response slug=%s outcome=%s token=%s",
                    event.label,
                    token.slug,
                    token.outcome_index,
                    token.venue_token_id,
                )
                continue
            if insert_snapshot_pair(
                cur,
                token=token,
                book=book,
                capture_time=capture_time,
                event=event,
                run_id=run_id,
            ):
                written += 1
    conn.commit()
    summary["rows_written"] = written

    duration_ms = (capture_time - request_started).total_seconds() * 1000
    timing = "CAUSAL" if capture_time <= event.target_time else "LATE"
    LOG.info(
        "%s boundary=%s requested=%d returned=%d written=%d "
        "http=%.1fms target_lag=%+.1fms %s raw_ref=%s",
        event.label,
        event.end_date,
        len(pending),
        len(by_venue_token),
        written,
        duration_ms,
        summary["capture_lag_ms"],
        timing,
        event.raw_ref,
    )
    return summary


def build_events(
    boundaries: list[datetime],
    offsets: tuple[int, ...],
    leads: tuple[float, ...],
) -> list[CaptureEvent]:
    events: list[CaptureEvent] = []
    for end_date in boundaries:
        for offset in offsets:
            target = end_date - timedelta(seconds=offset)
            for probe_index, lead in enumerate(leads, start=1):
                events.append(
                    CaptureEvent(
                        trigger_time=target - timedelta(seconds=lead),
                        end_date=end_date,
                        offset_sec=offset,
                        probe_index=probe_index,
                        lead_sec=lead,
                    )
                )
    return sorted(events)


def stop_handler(signum, _frame):
    global STOP
    STOP = True
    LOG.info("received signal %s; stopping cleanly", signum)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--offsets", type=parse_csv_ints, default=(4, 5, 59, 60))
    parser.add_argument(
        "--probe-leads",
        type=parse_csv_floats,
        default=(0.90, 0.35),
        help="Comma-separated seconds before each target; each must be between 0 and 1.",
    )
    parser.add_argument("--lookahead-sec", type=int, default=12 * 60)
    parser.add_argument("--refresh-sec", type=float, default=10.0)
    parser.add_argument("--max-lateness-sec", type=float, default=1.5)
    parser.add_argument("--poll-sec", type=float, default=0.03)
    parser.add_argument("--timeout-sec", type=float, default=5.0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--exit-after-sec",
        type=float,
        help="Testing only: exit after this many seconds.",
    )
    args = parser.parse_args()

    if args.lookahead_sec < max(args.offsets) + 30:
        parser.error("--lookahead-sec is too short for the configured offsets")
    if args.refresh_sec <= 0 or args.poll_sec <= 0 or args.timeout_sec <= 0:
        parser.error("refresh, poll, and timeout values must be positive")
    if args.max_lateness_sec < 0:
        parser.error("--max-lateness-sec cannot be negative")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)sZ %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    signal.signal(signal.SIGTERM, stop_handler)
    signal.signal(signal.SIGINT, stop_handler)

    conn = connect()
    conn.autocommit = False
    run_id = create_run(conn, args)
    LOG.info(
        "started run_id=%s offsets=%s leads=%s source=%s",
        run_id,
        args.offsets,
        args.probe_leads,
        SOURCE,
    )

    attempted: set[tuple[datetime, int, int]] = set()
    events: list[CaptureEvent] = []
    next_refresh = datetime.min.replace(tzinfo=timezone.utc)
    started = utcnow()

    limits = httpx.Limits(max_connections=4, max_keepalive_connections=2)
    with httpx.Client(
        headers=USER_AGENT,
        timeout=args.timeout_sec,
        follow_redirects=True,
        limits=limits,
    ) as client:
        while not STOP:
            now = utcnow()
            if args.exit_after_sec and (now - started).total_seconds() >= args.exit_after_sec:
                break

            if now >= next_refresh:
                try:
                    boundaries = upcoming_boundaries(conn, args.lookahead_sec)
                    events = build_events(boundaries, args.offsets, args.probe_leads)
                    next_refresh = now + timedelta(seconds=args.refresh_sec)
                    LOG.debug("schedule refreshed boundaries=%d events=%d", len(boundaries), len(events))
                except Exception:
                    conn.rollback()
                    LOG.exception("failed refreshing schedule")
                    time.sleep(1.0)
                    continue

            due = None
            for event in events:
                key = (event.end_date, event.offset_sec, event.probe_index)
                if key in attempted:
                    continue
                if event.trigger_time <= now:
                    due = event
                    break

            if due is not None:
                key = (due.end_date, due.offset_sec, due.probe_index)
                attempted.add(key)
                lateness = (now - due.trigger_time).total_seconds()
                if lateness > args.max_lateness_sec:
                    LOG.warning(
                        "MISSED %s boundary=%s trigger_late=%.3fs",
                        due.label,
                        due.end_date,
                        lateness,
                    )
                    continue
                try:
                    capture_event(conn, client, due, run_id, args.dry_run)
                except Exception:
                    conn.rollback()
                    LOG.exception("capture failed %s boundary=%s", due.label, due.end_date)
                continue

            # Remove old attempted keys so the set remains bounded.
            attempted = {
                key
                for key in attempted
                if key[0] >= now - timedelta(minutes=10)
            }

            future_times = [
                event.trigger_time
                for event in events
                if (event.end_date, event.offset_sec, event.probe_index) not in attempted
                and event.trigger_time > now
            ]
            if future_times:
                sleep_for = min(args.poll_sec, max(0.001, (min(future_times) - now).total_seconds()))
            else:
                sleep_for = args.poll_sec
            time.sleep(sleep_for)

    conn.close()
    LOG.info("stopped run_id=%s", run_id)
    return 0


if __name__ == "__main__":
    sys.exit(main())