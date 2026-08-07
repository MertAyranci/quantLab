"""Wallet forensics — bounded pilot collector (global-feed, date-cutoff).

RN1 is huge (20k+ positions, 10k+ markets), so per-market fetching is impractical.
This pilot uses the GLOBAL /trades feed, paginated BACKWARD in time until a date
cutoff (aligned to H2 odds start minus a lookback buffer) or a safety cap.

Purpose: validate the pipeline + test RN1's MLB activity against H2, BEFORE
investing in complete-history infrastructure.

Enrichment (sport/league/game-time/is_mlb/pregame/inplay) is NOT stored here —
it derives at analysis time by joining trades -> markets -> odds_games (H2's
existing table with sport_key, commence_time, polymarket_market_id). This keeps
the trades table clean and reuses H2's game metadata as the single source.

Idempotent via deterministic venue_trade_key (multi-attribute, NOT tx alone).
Resumable/checkpointed via wallet_collection_receipts. If the cap is hit before
the cutoff, the run is marked 'partial' — incomplete data is never silently
treated as complete.

Usage:
  .venv/bin/python collectors/polymarket/wallet_collector.py --wallet RN1 --dry-run
  .venv/bin/python collectors/polymarket/wallet_collector.py --wallet RN1 \
      --h2-start 2026-08-04T12:53:00Z --lookback-days 5 --max-pages 60
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import psycopg2
from dotenv import dotenv_values

REPO = Path(__file__).resolve().parents[2]
ENV = dotenv_values(REPO / ".env")
CONFIG = REPO / "config" / "research_wallets.json"

DATA_API = "https://data-api.polymarket.com"
UA = {"User-Agent": "quant-lab-wallet-forensics/0.2"}
POLY_VENUE_ID = 1
PAGE_LIMIT = 500
MAX_RETRIES = 4
TIMEOUT_S = 30
DEFAULT_H2_START = "2026-08-04T12:53:00Z"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)sZ %(levelname)s %(message)s")
log = logging.getLogger("wallet_pilot")


def connect():
    return psycopg2.connect(host="127.0.0.1", port=5432, dbname="quantlab",
                            user="quantlab", password=ENV["PG_PASSWORD"])


def load_wallet(name):
    cfg = json.loads(CONFIG.read_text())
    for w in cfg["wallets"]:
        if w["name"] == name:
            return w
    raise SystemExit(f"wallet '{name}' not found. Have: "
                     f"{[w['name'] for w in cfg['wallets']]}")


def _get(http, path, params):
    last = None
    for attempt in range(MAX_RETRIES):
        try:
            r = http.get(f"{DATA_API}{path}", params=params, timeout=TIMEOUT_S)
            if r.status_code == 200:
                return r.json()
            last = f"HTTP {r.status_code}"
        except httpx.HTTPError as e:
            last = str(e)
        time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"GET {path} failed after {MAX_RETRIES}: {last}")


def venue_trade_key(wallet, t):
    raw = "|".join(str(x) for x in [
        wallet, t.get("transactionHash"), t.get("conditionId"), t.get("asset"),
        t.get("timestamp"), t.get("side"), t.get("price"), t.get("size")])
    return hashlib.sha256(raw.encode()).hexdigest()


def resolve_token(cur, asset_id):
    cur.execute("SELECT id FROM tokens WHERE venue_id=%s AND venue_token_id=%s",
                (POLY_VENUE_ID, str(asset_id)))
    r = cur.fetchone()
    return r[0] if r else None


def insert_trade(cur, wallet, t, token_id):
    ts = datetime.fromtimestamp(int(t["timestamp"]), tz=timezone.utc)
    price_mc = int(round(float(t["price"]) * 1000))
    cur.execute("""
        INSERT INTO trades (venue_id, token_id, condition_id, event_time,
            capture_time, price_mc, size, side, outcome, wallet, tx_hash,
            venue_trade_key, source)
        VALUES (%s,%s,%s,%s, now(), %s,%s,%s,%s,%s,%s,%s,'data_api')
        ON CONFLICT (venue_id, venue_trade_key, event_time) DO NOTHING""",
        (POLY_VENUE_ID, token_id, t.get("conditionId"), ts, price_mc,
         float(t["size"]), t.get("side"), t.get("outcome"), wallet,
         t.get("transactionHash"), venue_trade_key(wallet, t)))
    return cur.rowcount


def log_unresolved(cur, wallet, cid, asset):
    cur.execute("""
        INSERT INTO wallet_unresolved_assets (wallet, condition_id, asset_id)
        VALUES (%s,%s,%s)
        ON CONFLICT (wallet, asset_id) DO UPDATE SET
            occurrences=wallet_unresolved_assets.occurrences+1""",
        (wallet, cid, str(asset)))


def write_receipt(cur, wallet, cutoff, oldest, newest, pages, rows, status, note):
    # reuse wallet_collection_receipts with a synthetic condition_id for the run
    cur.execute("""
        INSERT INTO wallet_collection_receipts (wallet, condition_id, status,
            trade_count, earliest_trade, latest_trade, collected_at, attempts, notes)
        VALUES (%s,%s,%s,%s,%s,%s, now(), 1, %s)
        ON CONFLICT (wallet, condition_id) DO UPDATE SET
            status=EXCLUDED.status, trade_count=EXCLUDED.trade_count,
            earliest_trade=EXCLUDED.earliest_trade, latest_trade=EXCLUDED.latest_trade,
            collected_at=now(), attempts=wallet_collection_receipts.attempts+1,
            notes=EXCLUDED.notes""",
        (wallet, f"__pilot_run_cutoff_{cutoff.date()}", status, rows,
         oldest, newest, note))


def iso(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wallet", required=True)
    ap.add_argument("--h2-start", default=DEFAULT_H2_START)
    ap.add_argument("--lookback-days", type=int, default=5)
    ap.add_argument("--max-pages", type=int, default=60,
                    help="safety cap; hitting it before cutoff => partial")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    w = load_wallet(args.wallet)
    addr = w["address"].lower()
    cutoff = iso(args.h2_start) - timedelta(days=args.lookback_days)
    log.info("wallet %s (%s)  cutoff=%s (H2 %s - %dd)  max_pages=%d",
             w["name"], addr, cutoff.isoformat(), args.h2_start,
             args.lookback_days, args.max_pages)

    http = httpx.Client(headers=UA)

    if args.dry_run:
        b = _get(http, "/trades", {"user": addr, "limit": 3, "takerOnly": "false"})
        log.info("[dry-run] newest 3 trades:")
        for t in b:
            ts = datetime.fromtimestamp(int(t["timestamp"]), tz=timezone.utc)
            log.info("  %s  %s  %s  %.4f x %.2f  %s",
                     ts.isoformat(), t.get("side"), (t.get("slug") or "")[:32],
                     float(t["price"]), float(t["size"]), t.get("conditionId")[:12])
        log.info("[dry-run] would page backward until %s or %d pages",
                 cutoff.date(), args.max_pages)
        return

    conn = connect(); cur = conn.cursor()
    offset = 0; page = 0
    rows_ins = rows_seen = unresolved = 0
    oldest = newest = None
    reached_cutoff = False

    while page < args.max_pages:
        batch = _get(http, "/trades",
                     {"user": addr, "limit": PAGE_LIMIT, "offset": offset,
                      "takerOnly": "false"})
        if not batch:
            reached_cutoff = True  # exhausted feed = we have everything
            break
        page += 1
        stop = False
        for t in batch:
            ts = datetime.fromtimestamp(int(t["timestamp"]), tz=timezone.utc)
            if newest is None or ts > newest:
                newest = ts
            if ts < cutoff:
                stop = True
                reached_cutoff = True
                break
            if oldest is None or ts < oldest:
                oldest = ts
            tok = resolve_token(cur, t.get("asset"))
            if tok is None:
                log_unresolved(cur, w["name"], t.get("conditionId"), t.get("asset"))
                unresolved += 1
                continue
            rows_ins += insert_trade(cur, w["name"], t, tok)
            rows_seen += 1
        conn.commit()
        log.info("page %d (offset %d): seen=%d inserted=%d unresolved=%d oldest=%s",
                 page, offset, rows_seen, rows_ins, unresolved,
                 oldest.isoformat() if oldest else "-")
        if stop:
            break
        if len(batch) < PAGE_LIMIT:
            reached_cutoff = True
            break
        offset += PAGE_LIMIT

    status = "complete" if reached_cutoff else "partial"
    note = (f"pages={page} rows_seen={rows_seen} inserted={rows_ins} "
            f"unresolved={unresolved} "
            f"{'reached cutoff' if reached_cutoff else 'HIT PAGE CAP before cutoff'}")
    write_receipt(cur, w["name"], cutoff, oldest, newest, page, rows_seen, status, note)
    conn.commit()

    log.info("=== %s ===", status.upper())
    log.info(note)
    if status == "partial":
        log.warning("PARTIAL: increase --max-pages to reach %s", cutoff.date())
    log.info("(re-run identical args should insert 0 — idempotency check)")


if __name__ == "__main__":
    main()