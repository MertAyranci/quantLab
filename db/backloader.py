"""Backloader — parse raw collected files into the quant-lab Postgres schema.

Usage:
    .venv/bin/python db/backloader.py --date 2026-07-09      # sample import
    .venv/bin/python db/backloader.py --all                   # everything
    .venv/bin/python db/backloader.py --all --recheck-counts  # report only

Design (schema v0.2 contract):
  * one collector_runs row per invocation; its id stamped on every row
  * token-resolution cache: venue_token_id -> internal id (upsert-on-miss)
  * markets_* files parsed BEFORE books_* within each day (books reference
    tokens; alphabetical order would do books first)
  * change detection for metadata versions / fees / ticks / status: only
    write a row when the observed value differs from the last known
  * book sides normalized to ASCENDING price_mc; best bid = last element,
    best ask = FIRST element (lowest ask) — never trust venue order
  * prices via Decimal, stored as integer milli (_mc = price*1000)
  * idempotency: parsed_files receipt written in the same transaction as the
    file's rows; commit every BATCH_FILES files; already-receipted files skip
"""

import argparse
import gzip
import json
import logging
import subprocess
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import dotenv_values

REPO = Path(__file__).resolve().parent.parent
RAW = REPO / "data" / "raw"
BATCH_FILES = 50
VENUE_ID = 1  # polymarket

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)sZ %(levelname)s %(message)s",
                    datefmt="%Y-%m-%dT%H:%M:%S")
logging.Formatter.converter = __import__("time").gmtime
log = logging.getLogger("backloader")


# ---------- helpers ----------------------------------------------------------

def mc(price_str) -> int | None:
    """'0.999' -> 999. None-safe."""
    if price_str is None or price_str == "":
        return None
    return int(Decimal(str(price_str)) * 1000)


def ms_to_dt(ms) -> datetime | None:
    if not ms:
        return None
    try:
        return datetime.fromtimestamp(int(ms) / 1000, timezone.utc)
    except (ValueError, OSError):
        return None


def iso(dt_str) -> datetime | None:
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except ValueError:
        return None


def jloads_maybe(s):
    """Gamma serializes some list fields as JSON strings."""
    if s is None:
        return None
    if isinstance(s, (list, dict)):
        return s
    try:
        return json.loads(s)
    except (json.JSONDecodeError, TypeError):
        return None


def git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=REPO).decode().strip()
    except Exception:
        return "unknown"


# ---------- loader -----------------------------------------------------------

class Backloader:
    def __init__(self, conn, run_id: int):
        self.conn = conn
        self.run_id = run_id
        self.cur = conn.cursor()
        # caches (loaded once; kept in sync as we insert)
        self.market_ids: dict[str, int] = {}        # venue_market_id -> id
        self.token_ids: dict[str, int] = {}         # venue_token_id -> id
        self.event_ids: dict[str, int] = {}         # venue_event_id -> id
        self.meta_state: dict[int, tuple] = {}      # market_id -> (q, rs, ed, slug)
        self.fee_state: dict[int, tuple] = {}
        self.tick_state: dict[int, int] = {}        # token internal id -> tick_mc
        self.status_state: dict[int, str] = {}
        self.parsed: set[str] = set()
        self._load_caches()

    def _load_caches(self):
        c = self.cur
        c.execute("SELECT venue_market_id, id FROM markets WHERE venue_id=%s", (VENUE_ID,))
        self.market_ids = dict(c.fetchall())
        c.execute("SELECT venue_token_id, id FROM tokens WHERE venue_id=%s", (VENUE_ID,))
        self.token_ids = dict(c.fetchall())
        c.execute("SELECT venue_event_id, id FROM events WHERE venue_id=%s", (VENUE_ID,))
        self.event_ids = dict(c.fetchall())
        c.execute("SELECT file_path FROM parsed_files")
        self.parsed = {r[0] for r in c.fetchall()}
        c.execute("""SELECT DISTINCT ON (market_id) market_id, question,
                     resolution_source_text, end_date, slug
                     FROM market_metadata_versions ORDER BY market_id, capture_time DESC""")
        for mid, q, rs, ed, slug in c.fetchall():
            self.meta_state[mid] = (q, rs, ed, slug)
        c.execute("""SELECT DISTINCT ON (market_id) market_id, status
                     FROM market_status ORDER BY market_id, observed_at DESC""")
        self.status_state = dict(c.fetchall())
        c.execute("""SELECT DISTINCT ON (token_id) token_id, tick_mc
                     FROM tick_sizes ORDER BY token_id, capture_time DESC""")
        self.tick_state = dict(c.fetchall())
        log.info("caches: %d markets, %d tokens, %d parsed files",
                 len(self.market_ids), len(self.token_ids), len(self.parsed))

    # ---- reference upserts -------------------------------------------------

    def upsert_event(self, ev: dict, cap: datetime, raw_ref: str) -> int | None:
        vid = str(ev.get("id") or "")
        if not vid:
            return None
        if vid in self.event_ids:
            return self.event_ids[vid]
        self.cur.execute(
            """INSERT INTO events (venue_id, venue_event_id, slug, title, neg_risk,
                                   first_seen_at, source, raw_ref, collector_run_id)
               VALUES (%s,%s,%s,%s,%s,%s,'gamma',%s,%s)
               ON CONFLICT (venue_id, venue_event_id) DO UPDATE SET slug=EXCLUDED.slug
               RETURNING id""",
            (VENUE_ID, vid, ev.get("slug"), ev.get("title"),
             ev.get("negRisk"), cap, raw_ref, self.run_id))
        eid = self.cur.fetchone()[0]
        self.event_ids[vid] = eid
        return eid

    def upsert_market(self, m: dict, cap: datetime, raw_ref: str,
                      source: str = "gamma") -> int:
        vmid = str(m.get("id"))
        if vmid in self.market_ids:
            return self.market_ids[vmid]
        ev_id = None
        evs = jloads_maybe(m.get("events"))
        if isinstance(evs, list) and evs:
            ev_id = self.upsert_event(evs[0], cap, raw_ref)
        self.cur.execute(
            """INSERT INTO markets (venue_id, event_id, venue_market_id, condition_id,
                                    slug, question, resolution_source_text, end_date,
                                    first_seen_at, source, raw_ref, collector_run_id)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (venue_id, venue_market_id) DO UPDATE
                 SET condition_id = COALESCE(markets.condition_id, EXCLUDED.condition_id)
               RETURNING id""",
            (VENUE_ID, ev_id, vmid, m.get("conditionId"), m.get("slug"),
             m.get("question"), m.get("resolutionSource"), iso(m.get("endDate")),
             cap, source, raw_ref, self.run_id))
        mid = self.cur.fetchone()[0]
        self.market_ids[vmid] = mid
        return mid

    def upsert_tokens(self, market_id: int, m: dict, raw_ref: str) -> list[int]:
        toks = jloads_maybe(m.get("clobTokenIds")) or []
        outs = jloads_maybe(m.get("outcomes")) or []
        ids = []
        for i, t in enumerate(toks):
            t = str(t)
            if t in self.token_ids:
                ids.append(self.token_ids[t])
                continue
            outcome = outs[i] if i < len(outs) else None
            self.cur.execute(
                """INSERT INTO tokens (venue_id, market_id, venue_token_id, outcome,
                                       outcome_index, source, raw_ref, collector_run_id)
                   VALUES (%s,%s,%s,%s,%s,'gamma',%s,%s)
                   ON CONFLICT (venue_id, venue_token_id) DO UPDATE SET outcome=EXCLUDED.outcome
                   RETURNING id""",
                (VENUE_ID, market_id, t, outcome, i, raw_ref, self.run_id))
            tid = self.cur.fetchone()[0]
            self.token_ids[t] = tid
            ids.append(tid)
        return ids

    def token_for_book(self, venue_token_id: str, meta: dict,
                       cap: datetime, raw_ref: str) -> int | None:
        """Books can reference tokens whose market we haven't parsed yet."""
        if venue_token_id in self.token_ids:
            return self.token_ids[venue_token_id]
        vmid = str(meta.get("market_id") or "")
        if not vmid:
            return None
        stub = {"id": vmid, "conditionId": meta.get("condition_id"),
                "question": meta.get("question")}
        mid = self.upsert_market(stub, cap, raw_ref, source="rest_book_stub")
        self.cur.execute(
            """INSERT INTO tokens (venue_id, market_id, venue_token_id,
                                   source, raw_ref, collector_run_id)
               VALUES (%s,%s,%s,'rest_book_stub',%s,%s)
               ON CONFLICT (venue_id, venue_token_id) DO NOTHING
               RETURNING id""",
            (VENUE_ID, mid, venue_token_id, raw_ref, self.run_id))
        row = self.cur.fetchone()
        if row is None:  # raced/existing
            self.cur.execute("SELECT id FROM tokens WHERE venue_id=%s AND venue_token_id=%s",
                             (VENUE_ID, venue_token_id))
            row = self.cur.fetchone()
        self.token_ids[venue_token_id] = row[0]
        return row[0]

    # ---- change-detected facts ---------------------------------------------

    def record_meta_version(self, mid: int, m: dict, cap: datetime, raw_ref: str):
        cur = (m.get("question"), m.get("resolutionSource"),
               iso(m.get("endDate")), m.get("slug"))
        if self.meta_state.get(mid) == cur:
            return 0
        # first sighting: markets row already holds it; still record baseline once
        self.cur.execute(
            """INSERT INTO market_metadata_versions
               (market_id, capture_time, question, resolution_source_text,
                end_date, slug, source, raw_ref, collector_run_id)
               VALUES (%s,%s,%s,%s,%s,%s,'gamma',%s,%s)""",
            (mid, cap, *cur, raw_ref, self.run_id))
        self.meta_state[mid] = cur
        return 1

    def record_status(self, mid: int, m: dict, cap: datetime, raw_ref: str):
        active, closed = m.get("active"), m.get("closed")
        status = "closed" if closed else ("active" if active else None)
        if status is None or self.status_state.get(mid) == status:
            return 0
        self.cur.execute(
            """INSERT INTO market_status (market_id, observed_at, status, source,
                                          raw_ref, collector_run_id)
               VALUES (%s,%s,%s,'gamma_poll',%s,%s)
               ON CONFLICT DO NOTHING""",
            (mid, cap, status, raw_ref, self.run_id))
        self.status_state[mid] = status
        return 1

    def record_tick(self, token_internal_ids: list[int], m: dict,
                    cap: datetime, raw_ref: str):
        tick = m.get("orderPriceMinTickSize")
        if tick is None:
            return 0
        t_mc = mc(tick)
        n = 0
        for tid in token_internal_ids:
            if self.tick_state.get(tid) == t_mc:
                continue
            self.cur.execute(
                """INSERT INTO tick_sizes (token_id, capture_time, tick_mc, source,
                                           raw_ref, collector_run_id)
                   VALUES (%s,%s,%s,'gamma',%s,%s) ON CONFLICT DO NOTHING""",
                (tid, cap, t_mc, raw_ref, self.run_id))
            self.tick_state[tid] = t_mc
            n += 1
        return n

    # ---- file parsers ------------------------------------------------------

    def parse_markets_file(self, path: Path) -> dict:
        raw_ref = str(path.relative_to(REPO))
        env = json.load(gzip.open(path, "rt"))
        cap = iso(env["captured_at_utc"])
        counts = {"markets": 0, "tokens": 0, "meta_versions": 0,
                  "status": 0, "ticks": 0}
        for m in env["payload"]["markets"]:
            mid = self.upsert_market(m, cap, raw_ref)
            tids = self.upsert_tokens(mid, m, raw_ref)
            counts["markets"] += 1
            counts["tokens"] += len(tids)
            counts["meta_versions"] += self.record_meta_version(mid, m, cap, raw_ref)
            counts["status"] += self.record_status(mid, m, cap, raw_ref)
            counts["ticks"] += self.record_tick(tids, m, cap, raw_ref)
        return counts

    def parse_books_file(self, path: Path) -> dict:
        raw_ref = str(path.relative_to(REPO))
        env = json.load(gzip.open(path, "rt"))
        cap = iso(env["captured_at_utc"])
        book_rows, tob_rows = [], []
        for venue_tid, meta in env["payload"]["books"].items():
            tid = self.token_for_book(venue_tid, meta, cap, raw_ref)
            if tid is None:
                continue
            book = meta.get("book") or {}
            ev_t = ms_to_dt(book.get("timestamp"))
            bids = sorted(((mc(l["price"]), Decimal(str(l["size"])))
                           for l in book.get("bids") or []), key=lambda x: x[0])
            asks = sorted(((mc(l["price"]), Decimal(str(l["size"])))
                           for l in book.get("asks") or []), key=lambda x: x[0])
            book_rows.append((tid, ev_t, cap,
                              json.dumps([[p, str(s)] for p, s in bids]),
                              json.dumps([[p, str(s)] for p, s in asks]),
                              book.get("hash"), "rest_book", "vol24h_top",
                              raw_ref, self.run_id))
            bb = bids[-1] if bids else (None, None)   # best bid = highest = LAST
            ba = asks[0] if asks else (None, None)    # best ask = lowest = FIRST
            tob_rows.append((tid, ev_t, cap, bb[0], bb[1], ba[0], ba[1],
                             "rest_book", "vol24h_top", raw_ref, self.run_id))
        if book_rows:
            psycopg2.extras.execute_values(self.cur,
                """INSERT INTO book_snapshots (token_id, event_time, capture_time,
                   bids, asks, venue_hash, source, watchlist_rule, raw_ref,
                   collector_run_id) VALUES %s""", book_rows)
            psycopg2.extras.execute_values(self.cur,
                """INSERT INTO tob_snapshots (token_id, event_time, capture_time,
                   best_bid_mc, best_bid_size, best_ask_mc, best_ask_size,
                   source, watchlist_rule, raw_ref, collector_run_id) VALUES %s""",
                tob_rows)
        return {"book_snapshots": len(book_rows), "tob_snapshots": len(tob_rows)}

    def receipt(self, path: Path, counts: dict, sha: str):
        self.cur.execute(
            """INSERT INTO parsed_files (file_path, parsed_at, row_counts, parser_sha)
               VALUES (%s, now(), %s, %s)""",
            (str(path.relative_to(REPO)), json.dumps(counts), sha))


# ---------- main -------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--date", help="one day, e.g. 2026-07-09 (sample import)")
    g.add_argument("--all", action="store_true")
    args = ap.parse_args()

    env = dotenv_values(REPO / ".env")
    conn = psycopg2.connect(host="127.0.0.1", port=5432, dbname="quantlab",
                            user="quantlab", password=env["PG_PASSWORD"])
    conn.autocommit = False
    sha = git_sha()

    with conn.cursor() as c:
        c.execute("""INSERT INTO collector_runs (component, started_at, version_sha, notes)
                     VALUES ('backloader', now(), %s, %s) RETURNING id""",
                  (sha, f"args={vars(args)}"))
        run_id = c.fetchone()[0]
    conn.commit()

    bl = Backloader(conn, run_id)
    days = sorted(d for d in RAW.iterdir() if d.is_dir())
    if args.date:
        days = [d for d in days if d.name == args.date]
        if not days:
            log.error("no raw dir for %s", args.date)
            sys.exit(1)

    totals: dict[str, int] = {}
    pending = 0
    for day in days:
        files = (sorted(day.glob("markets_*.json.gz"))
                 + sorted(day.glob("books_*.json.gz")))   # markets FIRST
        log.info("day %s: %d files", day.name, len(files))
        for path in files:
            rel = str(path.relative_to(REPO))
            if rel in bl.parsed:
                continue
            try:
                if path.name.startswith("markets_"):
                    counts = bl.parse_markets_file(path)
                else:
                    counts = bl.parse_books_file(path)
            except Exception:
                conn.rollback()
                log.exception("FAILED on %s — rolled back current batch; stopping", rel)
                sys.exit(1)
            bl.receipt(path, counts, sha)
            bl.parsed.add(rel)
            for k, v in counts.items():
                totals[k] = totals.get(k, 0) + v
            pending += 1
            if pending >= BATCH_FILES:
                conn.commit()
                pending = 0
        conn.commit()
        pending = 0
        log.info("day %s done. running totals: %s", day.name, totals)

    conn.commit()
    log.info("BACKLOAD COMPLETE run_id=%s totals=%s", run_id, totals)


if __name__ == "__main__":
    main()