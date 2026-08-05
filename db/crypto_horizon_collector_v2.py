#!/usr/bin/env python3
"""Deterministic full-book checkpoints for frozen crypto H4/H4b rules (v2).

The scheduler's critical path performs only:
    clock check -> cached token lookup -> POST /books -> timestamp response

Database discovery and database writes run in separate background threads. This
prevents PostgreSQL contention from making the HTTP capture several seconds late.

Frozen checkpoints:
    H4b signal      T-60
    H4b execution   T-59
    H4 signal       T-5
    H4 execution    T-4

Two causal probes are scheduled before every target. The backtester still uses
only snapshots with capture_time <= target_time and a maximum age of 3 seconds.

This service complements:
    db/crypto_harvester.py
    db/reconcile_crypto_resolutions.py
"""

from __future__ import annotations

import argparse
import json
import logging
import queue
import signal
import subprocess
import sys
import threading
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
WATCHLIST_RULE = "h4_frozen_horizons_v2"
USER_AGENT = {"User-Agent": "quantLab-crypto-horizon/2.0"}

LOG = logging.getLogger("crypto_horizon_collector")
STOP = threading.Event()


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
            f"crypto_horizon_v2:{epoch}:T-{self.offset_sec}:"
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


@dataclass(frozen=True)
class WriteJob:
    event: CaptureEvent
    rows: tuple[TokenRow, ...]
    books_by_token: dict[str, dict[str, Any]]
    capture_time: datetime


class CohortCache:
    def __init__(self):
        self._lock = threading.Lock()
        self._by_boundary: dict[datetime, tuple[TokenRow, ...]] = {}
        self.last_refresh: datetime | None = None

    def replace(self, grouped: dict[datetime, list[TokenRow]]) -> None:
        frozen = {
            boundary: tuple(rows)
            for boundary, rows in grouped.items()
        }
        with self._lock:
            self._by_boundary = frozen
            self.last_refresh = utcnow()

    def boundaries(self) -> tuple[datetime, ...]:
        with self._lock:
            return tuple(sorted(self._by_boundary))

    def rows(self, boundary: datetime) -> tuple[TokenRow, ...]:
        with self._lock:
            return self._by_boundary.get(boundary, ())


CACHE = CohortCache()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


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


def create_run(args) -> int:
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO collector_runs (
                    component, started_at, version_sha, notes
                )
                VALUES ('crypto_horizon_collector_v2', now(), %s, %s)
                RETURNING id
                """,
                (git_sha(), f"args={vars(args)}"),
            )
            run_id = cur.fetchone()[0]
        conn.commit()
        return run_id
    finally:
        conn.close()


def parse_csv_ints(value: str) -> tuple[int, ...]:
    try:
        values = tuple(sorted({int(x.strip()) for x in value.split(",")}))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "expected comma-separated integers"
        ) from exc
    if not values or any(x <= 0 for x in values):
        raise argparse.ArgumentTypeError("offsets must be positive")
    return values


def parse_csv_floats(value: str) -> tuple[float, ...]:
    try:
        values = tuple(
            sorted({float(x.strip()) for x in value.split(",")}, reverse=True)
        )
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "expected comma-separated numbers"
        ) from exc
    # The book reader accepts quotes up to 3 seconds old. Keep a safety margin.
    if not values or any(x <= 0 or x > 2.7 for x in values):
        raise argparse.ArgumentTypeError(
            "probe leads must be greater than 0 and at most 2.7 seconds"
        )
    return values


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


def discover_cohorts(lookahead_sec: int) -> dict[datetime, list[TokenRow]]:
    conn = connect()
    try:
        now = utcnow()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    m.id,
                    m.slug,
                    m.end_date,
                    tk.id,
                    tk.venue_token_id,
                    tk.outcome_index
                FROM crypto_watch cw
                JOIN markets m ON m.id = cw.market_id
                JOIN tokens tk ON tk.market_id = m.id
                WHERE m.end_date IS NOT NULL
                  AND m.end_date >= %s
                  AND m.end_date <= %s
                  AND tk.outcome_index IN (0, 1)
                  AND tk.venue_token_id IS NOT NULL
                ORDER BY m.end_date, m.slug, tk.outcome_index
                """,
                (
                    now - timedelta(seconds=10),
                    now + timedelta(seconds=lookahead_sec),
                ),
            )
            rows = [TokenRow(*row) for row in cur.fetchall()]
    finally:
        conn.close()

    grouped: dict[datetime, list[TokenRow]] = {}
    for row in rows:
        grouped.setdefault(row.end_date, []).append(row)
    return grouped


def discovery_worker(lookahead_sec: int, refresh_sec: float) -> None:
    while not STOP.is_set():
        started = time.monotonic()
        try:
            grouped = discover_cohorts(lookahead_sec)
            CACHE.replace(grouped)
            counts = [len(rows) for rows in grouped.values()]
            LOG.debug(
                "cache refreshed boundaries=%d token_range=%s",
                len(grouped),
                (min(counts), max(counts)) if counts else None,
            )
        except Exception:
            LOG.exception("cohort-cache refresh failed")

        elapsed = time.monotonic() - started
        STOP.wait(max(0.2, refresh_sec - elapsed))


def build_events(
    boundaries: tuple[datetime, ...],
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

    cur.execute(
        """
        INSERT INTO book_snapshots (
            token_id, event_time, capture_time, bids, asks, venue_hash,
            source, watchlist_rule, raw_ref, collector_run_id
        )
        SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        WHERE NOT EXISTS (
            SELECT 1 FROM book_snapshots
            WHERE token_id=%s AND source=%s AND raw_ref=%s
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
            token_id, event_time, capture_time,
            best_bid_mc, best_bid_size, best_ask_mc, best_ask_size,
            source, watchlist_rule, raw_ref, collector_run_id
        )
        SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        WHERE NOT EXISTS (
            SELECT 1 FROM tob_snapshots
            WHERE token_id=%s AND source=%s AND raw_ref=%s
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


def writer_worker(write_queue: queue.Queue[WriteJob | None], run_id: int) -> None:
    conn = connect()
    conn.autocommit = False
    try:
        while True:
            try:
                job = write_queue.get(timeout=0.5)
            except queue.Empty:
                if STOP.is_set() and write_queue.empty():
                    break
                continue

            if job is None:
                write_queue.task_done()
                break

            written = 0
            try:
                with conn.cursor() as cur:
                    for token in job.rows:
                        book = job.books_by_token.get(token.venue_token_id)
                        if book is None:
                            continue
                        if insert_snapshot_pair(
                            cur,
                            token=token,
                            book=book,
                            capture_time=job.capture_time,
                            event=job.event,
                            run_id=run_id,
                        ):
                            written += 1
                conn.commit()
                LOG.info(
                    "DB-WRITE %s boundary=%s written=%d/%d",
                    job.event.label,
                    job.event.end_date,
                    written,
                    len(job.rows),
                )
            except Exception:
                conn.rollback()
                LOG.exception(
                    "database write failed %s boundary=%s",
                    job.event.label,
                    job.event.end_date,
                )
            finally:
                write_queue.task_done()
    finally:
        conn.close()


def capture_event(
    client: httpx.Client,
    event: CaptureEvent,
    rows: tuple[TokenRow, ...],
    write_queue: queue.Queue[WriteJob | None],
    expected_min_tokens: int,
    dry_run: bool,
) -> None:
    if not rows:
        LOG.warning(
            "%s boundary=%s NO_TOKENS",
            event.label,
            event.end_date,
        )
        return

    cohort_state = (
        "COMPLETE"
        if len(rows) >= expected_min_tokens
        else f"PARTIAL({len(rows)}<{expected_min_tokens})"
    )

    if dry_run:
        LOG.info(
            "DRY RUN %s boundary=%s tokens=%d %s target=%s",
            event.label,
            event.end_date,
            len(rows),
            cohort_state,
            event.target_time,
        )
        return

    payload = [{"token_id": row.venue_token_id} for row in rows]
    request_started = utcnow()
    response = client.post(CLOB_BOOKS_URL, json=payload)
    capture_time = utcnow()
    response.raise_for_status()

    data = response.json()
    if not isinstance(data, list):
        raise ValueError(
            f"unexpected /books response type: {type(data).__name__}"
        )

    by_token = {
        str(book.get("asset_id")): book
        for book in data
        if isinstance(book, dict) and book.get("asset_id") is not None
    }

    duration_ms = (
        capture_time - request_started
    ).total_seconds() * 1000
    target_lag_ms = (
        capture_time - event.target_time
    ).total_seconds() * 1000
    timing = "CAUSAL" if target_lag_ms <= 0 else "LATE"

    LOG.info(
        "%s boundary=%s requested=%d returned=%d "
        "http=%.1fms target_lag=%+.1fms %s %s raw_ref=%s",
        event.label,
        event.end_date,
        len(rows),
        len(by_token),
        duration_ms,
        target_lag_ms,
        timing,
        cohort_state,
        event.raw_ref,
    )

    if by_token:
        try:
            write_queue.put_nowait(
                WriteJob(
                    event=event,
                    rows=rows,
                    books_by_token=by_token,
                    capture_time=capture_time,
                )
            )
        except queue.Full:
            LOG.error(
                "writer queue full; dropping %s boundary=%s",
                event.label,
                event.end_date,
            )


def stop_handler(signum, _frame) -> None:
    LOG.info("received signal %s; stopping", signum)
    STOP.set()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--offsets",
        type=parse_csv_ints,
        default=(4, 5, 59, 60),
    )
    parser.add_argument(
        "--probe-leads",
        type=parse_csv_floats,
        default=(2.6, 1.2),
    )
    parser.add_argument("--lookahead-sec", type=int, default=12 * 60)
    parser.add_argument("--refresh-sec", type=float, default=3.0)
    parser.add_argument("--max-lateness-sec", type=float, default=0.75)
    parser.add_argument("--poll-sec", type=float, default=0.01)
    parser.add_argument("--timeout-sec", type=float, default=5.0)
    parser.add_argument("--expected-min-tokens", type=int, default=12)
    parser.add_argument("--writer-queue-size", type=int, default=100)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.lookahead_sec < max(args.offsets) + 30:
        parser.error("lookahead is too short")
    if args.refresh_sec <= 0 or args.max_lateness_sec < 0:
        parser.error("invalid refresh/lateness values")
    if args.expected_min_tokens < 2:
        parser.error("expected-min-tokens must be at least 2")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)sZ %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    signal.signal(signal.SIGTERM, stop_handler)
    signal.signal(signal.SIGINT, stop_handler)

    run_id = create_run(args)
    LOG.info(
        "started v2 run_id=%s offsets=%s leads=%s source=%s",
        run_id,
        args.offsets,
        args.probe_leads,
        SOURCE,
    )

    discovery = threading.Thread(
        target=discovery_worker,
        args=(args.lookahead_sec, args.refresh_sec),
        name="cohort-discovery",
        daemon=True,
    )
    write_queue: queue.Queue[WriteJob | None] = queue.Queue(
        maxsize=args.writer_queue_size
    )
    writer = threading.Thread(
        target=writer_worker,
        args=(write_queue, run_id),
        name="snapshot-writer",
        daemon=True,
    )
    discovery.start()
    writer.start()

    attempted: set[tuple[datetime, int, int]] = set()
    limits = httpx.Limits(
        max_connections=2,
        max_keepalive_connections=2,
    )

    with httpx.Client(
        headers=USER_AGENT,
        timeout=args.timeout_sec,
        follow_redirects=True,
        limits=limits,
    ) as client:
        while not STOP.is_set():
            now = utcnow()
            events = build_events(
                CACHE.boundaries(),
                args.offsets,
                args.probe_leads,
            )

            due: CaptureEvent | None = None
            for event in events:
                key = (
                    event.end_date,
                    event.offset_sec,
                    event.probe_index,
                )
                if key in attempted:
                    continue
                if event.trigger_time <= now:
                    due = event
                    break

            if due is not None:
                key = (
                    due.end_date,
                    due.offset_sec,
                    due.probe_index,
                )
                attempted.add(key)
                lateness = (
                    now - due.trigger_time
                ).total_seconds()

                if lateness > args.max_lateness_sec:
                    LOG.warning(
                        "MISSED %s boundary=%s trigger_late=%.3fs",
                        due.label,
                        due.end_date,
                        lateness,
                    )
                    continue

                rows = CACHE.rows(due.end_date)
                try:
                    capture_event(
                        client,
                        due,
                        rows,
                        write_queue,
                        args.expected_min_tokens,
                        args.dry_run,
                    )
                except Exception:
                    LOG.exception(
                        "capture failed %s boundary=%s",
                        due.label,
                        due.end_date,
                    )
                continue

            attempted = {
                key
                for key in attempted
                if key[0] >= now - timedelta(minutes=10)
            }

            future = [
                event.trigger_time
                for event in events
                if (
                    event.end_date,
                    event.offset_sec,
                    event.probe_index,
                ) not in attempted
                and event.trigger_time > now
            ]
            if future:
                sleep_for = min(
                    args.poll_sec,
                    max(
                        0.001,
                        (
                            min(future) - now
                        ).total_seconds(),
                    ),
                )
            else:
                sleep_for = args.poll_sec
            STOP.wait(sleep_for)

    STOP.set()
    discovery.join(timeout=5)
    write_queue.join()
    try:
        write_queue.put_nowait(None)
    except queue.Full:
        pass
    writer.join(timeout=10)

    LOG.info("stopped v2 run_id=%s", run_id)
    return 0


if __name__ == "__main__":
    sys.exit(main())