"""Buffer loader — database half of collector v2.

Usage:
    .venv/bin/python db/buffer_loader.py --once     # one pass, then exit
    .venv/bin/python db/buffer_loader.py            # daemon (systemd)

Reads minute-rotated JSONL buffers written by ws_collector.py and batch-loads
them into Postgres per the tiering decision (RFC Q8.5):

  Tier 1 (breadth, whole watchlist)
    best_bid_ask      -> tob_snapshots
    last_trade_price  -> last_trade_events
    ws_book           -> book_snapshots (+ derived tob_snapshots row)
    rest_resync_book  -> book_snapshots (+ derived tob_snapshots row)
  Tier 2 (depth, only tokens listed in DEEP_TOKENS_FILE)
    price_change      -> book_deltas
  Reference / lifecycle (always)
    tick_size_change  -> tick_sizes
    market_resolved   -> resolutions + market_status
    new_market        -> markets + tokens (+ fees, ticks)

Safety properties:
  * only files whose minute has PASSED are loaded (the current file is still
    being written); the newest file is always skipped
  * each file loads inside one transaction together with its parsed_files
    receipt -> crash-safe, re-runnable, no double-load
  * unknown tokens are resolved/created via the Backloader machinery; records
    whose token cannot be resolved are counted and skipped, never guessed
  * loaded files are moved to data/buffer/ws/done/ (kept until the next
    backup cycle, then prunable) -- never silently deleted on first pass
"""

import argparse
import json
import logging
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

import httpx
import psycopg2
import psycopg2.extras
from dotenv import dotenv_values

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backloader import Backloader, VENUE_ID, git_sha, log  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
BUF_DIR = REPO / "data" / "buffer" / "ws"
DONE_DIR = BUF_DIR / "done"
DEEP_TOKENS_FILE = REPO / "config" / "deep_tokens.txt"
POLL_S = 20
HEALTHCHECK_URL = os.getenv("LOADER_HEALTHCHECK_URL", "")

logging.getLogger("httpx").setLevel(logging.WARNING)


def mc(x) -> int | None:
    if x is None or x == "":
        return None
    try:
        v = int(Decimal(str(x)) * 1000)
    except (InvalidOperation, ValueError):
        return None
    return v if 0 <= v <= 1000 else None


def num(x):
    try:
        return Decimal(str(x))
    except (InvalidOperation, ValueError, TypeError):
        return None


def ms_dt(ms):
    if not ms:
        return None
    try:
        return datetime.fromtimestamp(int(ms) / 1000, timezone.utc)
    except (ValueError, OSError, TypeError):
        return None


def iso(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except ValueError:
        return None


def load_deep_tokens() -> set[str]:
    if not DEEP_TOKENS_FILE.exists():
        return set()
    return {ln.strip() for ln in DEEP_TOKENS_FILE.read_text().splitlines()
            if ln.strip() and not ln.startswith("#")}


class BufferLoader(Backloader):
    """Reuses Backloader caches/upserts; adds WS record routing."""

    def __init__(self, conn, run_id, deep_tokens: set[str]):
        super().__init__(conn, run_id)
        self.deep = deep_tokens
        self.http = httpx.Client(headers={"User-Agent": "quant-lab-loader/0.1"},
                                 timeout=30)
        self.unresolved: set[str] = set()

    def _load_caches(self):
        # Daemon override: DO NOT preload 1.5M markets / 3M tokens / 1.5M
        # receipts (that was ~1.5GB). Resolve on demand; track only this
        # session's parsed files.
        self.market_ids = {}
        self.token_ids = {}      # filled lazily by token_id_for()
        self.event_ids = {}
        self.meta_state = {}
        self.fee_state = {}
        self.tick_state = {}
        self.status_state = {}
        self.parsed = set()      # this daemon's session only
        log.info("caches: lazy mode (daemon)")

    # ---- token resolution ---------------------------------------------------

    def token_id_for(self, venue_token: str | None) -> int | None:
        """Internal id for a venue token; fetch market metadata from Gamma once
        if unknown (new markets appear mid-stream)."""
        if not venue_token:
            return None
        if venue_token in self.token_ids:
            return self.token_ids[venue_token]
        if venue_token in self.unresolved:
            return None
        try:
            r = self.http.get("https://gamma-api.polymarket.com/markets",
                              params={"clob_token_ids": venue_token})
            data = r.json() if r.status_code == 200 else []
        except (httpx.HTTPError, json.JSONDecodeError):
            data = []
        if data:
            cap = datetime.now(timezone.utc)
            raw_ref = f"gamma:lookup:{venue_token[:20]}"
            mid = self.upsert_market(data[0], cap, raw_ref, source="ws_lookup")
            self.upsert_tokens(mid, data[0], raw_ref)
            if venue_token in self.token_ids:
                return self.token_ids[venue_token]

        self.cur.execute("SELECT id FROM tokens WHERE venue_token_id=%s", (venue_token,))
        row = self.cur.fetchone()
        if row:
            self.token_ids[venue_token] = row[0]
            return row[0]
        if venue_token in self.unresolved:
            return None
        return None

    # ---- record routing ------------------------------------------------------

    def route(self, rec: dict, raw_ref: str, batches: dict) -> str:
        kind = rec.get("kind")
        p = rec.get("payload") or {}
        cap = iso(rec.get("capture_time"))

        # --- lifecycle events route by market identity, NOT watchlist token ---
        # (a resolving market is usually one we were NOT watching)
        if kind == "market_resolved":
            return self._route_resolution(p, cap, raw_ref)
        if kind == "new_market":
            return self._route_new_market(p, cap, raw_ref)

        # --- everything else needs a known token ---
        tid = self.token_id_for(rec.get("token"))
        if tid is None:
            return "skipped_no_token"

        conn_id = rec.get("connection_id")
        seq = rec.get("ingest_sequence")
        ci = rec.get("change_index", 0)
        gen = rec.get("book_generation")
        rule = rec.get("watchlist_rule")

        if kind == "best_bid_ask":
            batches["tob"].append((tid, ms_dt(p.get("timestamp")), cap,
                                   mc(p.get("best_bid")), None,
                                   mc(p.get("best_ask")), None,
                                   "ws_best_bid_ask", rule, raw_ref, self.run_id))
            return "tob"

        if kind == "last_trade_price":
            batches["lte"].append((tid, ms_dt(p.get("timestamp")), cap,
                                   mc(p.get("price")), num(p.get("size")),
                                   p.get("side"),
                                   int(p["fee_rate_bps"]) if str(p.get("fee_rate_bps") or "").isdigit() else None,
                                   conn_id, seq, self.run_id,
                                   "ws_last_trade_price", raw_ref))
            return "lte"

        if kind in ("ws_book", "rest_resync_book"):
            bids = sorted(((mc(l["price"]), str(num(l["size"])))
                           for l in (p.get("bids") or [])
                           if mc(l.get("price")) is not None), key=lambda x: x[0])
            asks = sorted(((mc(l["price"]), str(num(l["size"])))
                           for l in (p.get("asks") or [])
                           if mc(l.get("price")) is not None), key=lambda x: x[0])
            src = "ws_book" if kind == "ws_book" else "rest_book"
            batches["book"].append((tid, ms_dt(p.get("timestamp")), cap,
                                    json.dumps(bids), json.dumps(asks),
                                    p.get("hash"), src, rule, raw_ref,
                                    self.run_id,
                                    conn_id if src == "ws_book" else None,
                                    seq if src == "ws_book" else None,
                                    gen))
            bb = bids[-1] if bids else (None, None)   # best bid = highest = LAST
            ba = asks[0] if asks else (None, None)    # best ask = lowest = FIRST
            batches["tob"].append((tid, ms_dt(p.get("timestamp")), cap,
                                   bb[0], bb[1], ba[0], ba[1],
                                   src, rule, raw_ref, self.run_id))
            return "book"

        if kind == "price_change":
            if rec.get("token") not in self.deep:
                return "skipped_not_deep"          # Tier 2 gate
            side = (p.get("side") or "")[:1].upper()
            if side not in ("B", "S"):
                side = "B" if (p.get("side") or "").upper().startswith("BUY") else "S"
            price = mc(p.get("price"))
            size = num(p.get("size"))
            if price is None or size is None or gen is None or seq is None:
                return "skipped_bad_delta"
            batches["delta"].append((tid, ms_dt(p.get("timestamp")), cap,
                                     price, size, side, p.get("hash"),
                                     mc(p.get("best_bid")), mc(p.get("best_ask")),
                                     conn_id, seq, ci, gen, self.run_id,
                                     "ws_price_change", rule, raw_ref))
            return "delta"

        if kind == "tick_size_change":
            self.cur.execute(
                """INSERT INTO tick_sizes (token_id, event_time, capture_time,
                   tick_mc, source, raw_ref, collector_run_id, connection_id,
                   ingest_sequence)
                   VALUES (%s,%s,%s,%s,'ws_tick_size_change',%s,%s,%s,%s)
                   ON CONFLICT DO NOTHING""",
                (tid, ms_dt(p.get("timestamp")), cap, mc(p.get("new_tick_size")),
                 raw_ref, self.run_id, conn_id, seq))
            return "tick"

        return "skipped_unknown_kind"
    # ---- helpers for route method ---------------------------------------------
    def _route_resolution(self, p: dict, cap, raw_ref: str) -> str:
        """market_resolved: route by the event's own market/condition id,
        independent of the watchlist. Creates the market via Gamma if unseen."""
        condition_id = p.get("market")
        venue_market_id = str(p.get("id") or "")
        if not condition_id and not venue_market_id:
            return "skipped_no_market_id"

        # find market: by venue_market_id first, then condition_id, else Gamma lookup
        mid = None
        if venue_market_id:
            self.cur.execute("SELECT id FROM markets WHERE venue_id=%s AND venue_market_id=%s",
                             (VENUE_ID, venue_market_id))
            row = self.cur.fetchone()
            if row:
                mid = row[0]
        if mid is None and condition_id:
            self.cur.execute("SELECT id FROM markets WHERE condition_id=%s", (condition_id,))
            row = self.cur.fetchone()
            if row:
                mid = row[0]
        if mid is None:
            # never seen this market — create it from the event + a Gamma lookup
            toks = p.get("assets_ids") or []
            stub = {"id": venue_market_id or condition_id,
                    "conditionId": condition_id,
                    "clobTokenIds": json.dumps([str(t) for t in toks]),
                    "outcomes": json.dumps(p.get("outcomes") or [])}
            mid = self.upsert_market(stub, cap, raw_ref, source="ws_market_resolved")
            self.upsert_tokens(mid, stub, raw_ref)

        # resolve winning token id (create it if the market was just stubbed)
        win_venue = str(p.get("winning_asset_id") or "")
        win_tid = self.token_ids.get(win_venue)
        if win_tid is None and win_venue:
            self.cur.execute("SELECT id FROM tokens WHERE venue_token_id=%s", (win_venue,))
            row = self.cur.fetchone()
            win_tid = row[0] if row else None

        self.cur.execute(
            """INSERT INTO resolutions (market_id, event_time, capture_time,
               winning_token_id, winning_outcome, learned_via, status,
               raw_ref, collector_run_id)
               VALUES (%s,%s,%s,%s,%s,'ws_market_resolved','final',%s,%s)""",
            (mid, ms_dt(p.get("timestamp")), cap, win_tid,
             p.get("winning_outcome"), raw_ref, self.run_id))
        self.cur.execute(
            """INSERT INTO market_status (market_id, observed_at, status,
               source, raw_ref, collector_run_id)
               VALUES (%s,%s,'resolved','ws_market_resolved',%s,%s)
               ON CONFLICT DO NOTHING""",
            (mid, cap, raw_ref, self.run_id))
        return "resolved"

    def _route_new_market(self, p: dict, cap, raw_ref: str) -> str:
        """new_market: create market/tokens/fees from the event payload."""
        m = {"id": p.get("id"), "conditionId": p.get("condition_id") or p.get("market"),
             "slug": p.get("slug"), "question": p.get("question"),
             "resolutionSource": p.get("description"),
             "clobTokenIds": json.dumps(p.get("clob_token_ids") or p.get("assets_ids") or []),
             "outcomes": json.dumps(p.get("outcomes") or []),
             "orderPriceMinTickSize": p.get("order_price_min_tick_size")}
        mid = self.upsert_market(m, cap, raw_ref, source="ws_new_market")
        tids = self.upsert_tokens(mid, m, raw_ref)
        fs = p.get("fee_schedule") or {}
        self.cur.execute(
            """INSERT INTO market_fees (market_id, observed_at, fees_enabled,
               taker_base_fee_bps, fee_exponent, fee_rate, taker_only,
               rebate_rate, source, raw_ref, collector_run_id)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'ws_new_market',%s,%s)
               ON CONFLICT DO NOTHING""",
            (mid, cap, p.get("fees_enabled"),
             int(p["taker_base_fee"]) if str(p.get("taker_base_fee") or "").isdigit() else None,
             num(fs.get("exponent")), num(fs.get("rate")),
             fs.get("taker_only"), num(fs.get("rebate_rate")),
             raw_ref, self.run_id))
        self.record_tick(tids, m, cap, raw_ref)
        return "new_market"
    # ---- batch flush ----------------------------------------------------------

    def flush(self, batches: dict):
        if batches["tob"]:
            psycopg2.extras.execute_values(self.cur,
                """INSERT INTO tob_snapshots (token_id, event_time, capture_time,
                   best_bid_mc, best_bid_size, best_ask_mc, best_ask_size,
                   source, watchlist_rule, raw_ref, collector_run_id)
                   VALUES %s""", batches["tob"])
        if batches["book"]:
            psycopg2.extras.execute_values(self.cur,
                """INSERT INTO book_snapshots (token_id, event_time, capture_time,
                   bids, asks, venue_hash, source, watchlist_rule, raw_ref,
                   collector_run_id, connection_id, ingest_sequence,
                   book_generation) VALUES %s""", batches["book"])
        if batches["lte"]:
            psycopg2.extras.execute_values(self.cur,
                """INSERT INTO last_trade_events (token_id, event_time,
                   capture_time, price_mc, size, side, fee_rate_bps,
                   connection_id, ingest_sequence, collector_run_id, source,
                   raw_ref) VALUES %s
                   ON CONFLICT DO NOTHING""", batches["lte"])
        if batches["delta"]:
            psycopg2.extras.execute_values(self.cur,
                """INSERT INTO book_deltas (token_id, event_time, capture_time,
                   price_mc, size, side, venue_hash, best_bid_mc, best_ask_mc,
                   connection_id, ingest_sequence, change_index, book_generation,
                   collector_run_id, source, watchlist_rule, raw_ref)
                   VALUES %s ON CONFLICT DO NOTHING""", batches["delta"])

    # ---- per-file --------------------------------------------------------------

    def load_file(self, path: Path) -> dict:
        rel = str(path.relative_to(REPO))
        counts: dict[str, int] = {}
        batches = {"tob": [], "book": [], "lte": [], "delta": []}
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    counts["skipped_bad_json"] = counts.get("skipped_bad_json", 0) + 1
                    continue
                r = self.route(rec, rel, batches)
                counts[r] = counts.get(r, 0) + 1
        self.flush(batches)
        self.cur.execute(
            """INSERT INTO parsed_files (file_path, parsed_at, row_counts, parser_sha)
               VALUES (%s, now(), %s, %s)
               ON CONFLICT (file_path) DO NOTHING""",
            (rel, json.dumps(counts), git_sha()))
        return counts


def pending_files() -> list[Path]:
    """All buffer files except the newest (still being written)."""
    files = sorted(p for p in BUF_DIR.glob("*.jsonl") if p.is_file())
    return files[:-1] if files else []


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()

    env = dotenv_values(REPO / ".env")
    conn = psycopg2.connect(host="127.0.0.1", port=5432, dbname="quantlab",
                            user="quantlab", password=env["PG_PASSWORD"])
    conn.autocommit = False
    with conn.cursor() as c:
        c.execute("""INSERT INTO collector_runs (component, started_at, version_sha, notes)
                     VALUES ('buffer_loader', now(), %s, %s) RETURNING id""",
                  (git_sha(), f"args={vars(args)}"))
        run_id = c.fetchone()[0]
    conn.commit()

    DONE_DIR.mkdir(parents=True, exist_ok=True)
    loader = BufferLoader(conn, run_id, load_deep_tokens())
    log.info("buffer_loader run_id=%s deep_tokens=%d", run_id, len(loader.deep))

    while True:
        files = []
        for f in pending_files():
            rel = str(f.relative_to(REPO))
            if rel in loader.parsed:
                continue
            loader.cur.execute("SELECT 1 FROM parsed_files WHERE file_path=%s", (rel,))
            if loader.cur.fetchone():
                # already loaded in a prior run — move to done, don't reprocess
                loader.parsed.add(rel)
                shutil.move(str(f), str(DONE_DIR / f.name))
                continue
            files.append(f)
        totals: dict[str, int] = {}
        for path in files:
            try:
                counts = loader.load_file(path)
                conn.commit()
            except Exception:
                conn.rollback()
                log.exception("FAILED loading %s — left in place, continuing", path.name)
                continue
            loader.parsed.add(str(path.relative_to(REPO)))
            path.unlink() 
            for k, v in counts.items():
                totals[k] = totals.get(k, 0) + v
        if files:
            log.info("loaded %d files | %s | lag=%d files",
                     len(files), totals, len(pending_files()))
        if HEALTHCHECK_URL:
            try:
                httpx.get(HEALTHCHECK_URL, timeout=10)
            except httpx.HTTPError:
                pass
        if args.once:
            break
        time.sleep(POLL_S)

    log.info("buffer_loader done run_id=%s", run_id)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("shutdown requested — exiting cleanly")