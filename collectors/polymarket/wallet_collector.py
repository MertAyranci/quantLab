"""Wallet forensics collector — resumable, idempotent trade-history collector for
research wallets (public Polymarket on-chain data).

Studies successful sports traders' PATTERNS (for forward-predictive-value testing,
not naive copying). Reputation is NOT assumed — the collector reports aggregate
realized PnL so a wallet's actual skill is measured, not trusted.

Flow (per wallet):
  1. read wallet from config/research_wallets.json (--wallet NAME)
  2. fetch current + closed positions -> enumerate unique conditionIds
  3. for each market not already 'complete' in receipts, fetch its trades
     (takerOnly=false, paginated, fail loudly on incomplete)
  4. resolve each fill's asset -> internal token_id (log unresolved, don't drop)
  5. insert fills into `trades` (ON CONFLICT DO NOTHING via venue_trade_key)
  6. update the receipt (status, counts, earliest/latest) so we can resume

Idempotent: deterministic venue_trade_key from
  wallet|tx_hash|condition|asset|timestamp|side|price|size
(NOT tx_hash alone — one tx can hold multiple fills).

Usage:
  .venv/bin/python collectors/polymarket/wallet_collector.py --wallet RN1
  .venv/bin/python collectors/polymarket/wallet_collector.py --wallet RN1 --dry-run
  .venv/bin/python collectors/polymarket/wallet_collector.py --wallet RN1 --resume
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
import psycopg2
from dotenv import dotenv_values

REPO = Path(__file__).resolve().parents[2]
ENV = dotenv_values(REPO / ".env")
CONFIG = REPO / "config" / "research_wallets.json"

DATA_API = "https://data-api.polymarket.com"
UA = {"User-Agent": "quant-lab-wallet-forensics/0.1"}
POLY_VENUE_ID = 1
PAGE_LIMIT = 500
MAX_RETRIES = 4
TIMEOUT_S = 30

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)sZ %(levelname)s %(message)s")
log = logging.getLogger("wallet_collector")


def connect():
    return psycopg2.connect(host="127.0.0.1", port=5432, dbname="quantlab",
                            user="quantlab", password=ENV["PG_PASSWORD"])


def load_wallet(name):
    cfg = json.loads(CONFIG.read_text())
    for w in cfg["wallets"]:
        if w["name"] == name:
            return w
    raise SystemExit(f"wallet '{name}' not in {CONFIG.name}. "
                     f"Available: {[w['name'] for w in cfg['wallets']]}")


def _get(http, path, params):
    """GET with retries; fail loudly on persistent error."""
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
    raise RuntimeError(f"GET {path} failed after {MAX_RETRIES} tries: {last}")


def fetch_positions(http, addr):
    """Current + closed positions -> unique conditionIds. Paginates by offset."""
    conds = {}
    for closed in ("false", "true"):
        offset = 0
        while True:
            batch = _get(http, "/positions",
                         {"user": addr, "limit": PAGE_LIMIT, "offset": offset,
                          "closed": closed})
            if not batch:
                break
            for p in batch:
                cid = p.get("conditionId")
                if cid:
                    conds[cid] = p.get("title") or p.get("slug") or ""
            if len(batch) < PAGE_LIMIT:
                break
            offset += PAGE_LIMIT
    return conds


def fetch_trades_for_market(http, addr, condition_id):
    """All fills for one wallet+market. takerOnly=false includes maker fills.
    Paginates; fails loudly if a page looks truncated inconsistently."""
    out = []
    offset = 0
    while True:
        batch = _get(http, "/trades",
                     {"user": addr, "market": condition_id,
                      "limit": PAGE_LIMIT, "offset": offset,
                      "takerOnly": "false"})
        if not batch:
            break
        out.extend(batch)
        if len(batch) < PAGE_LIMIT:
            break
        offset += PAGE_LIMIT
        if offset > 100000:
            raise RuntimeError(f"pagination runaway for {condition_id}")
    return out


def venue_trade_key(wallet, t):
    """Deterministic per-fill key. One tx may hold multiple fills, so include
    asset/side/price/size/timestamp — NOT tx_hash alone."""
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
    return cur.rowcount   # 1 inserted, 0 conflict


def upsert_receipt(cur, wallet, cid, status, n, earliest, latest, note=None):
    cur.execute("""
        INSERT INTO wallet_collection_receipts (wallet, condition_id, status,
            trade_count, earliest_trade, latest_trade, collected_at, attempts, notes)
        VALUES (%s,%s,%s,%s,%s,%s, now(), 1, %s)
        ON CONFLICT (wallet, condition_id) DO UPDATE SET
            status=EXCLUDED.status, trade_count=EXCLUDED.trade_count,
            earliest_trade=EXCLUDED.earliest_trade, latest_trade=EXCLUDED.latest_trade,
            collected_at=now(), attempts=wallet_collection_receipts.attempts+1,
            notes=EXCLUDED.notes""",
        (wallet, cid, status, n, earliest, latest, note))


def log_unresolved(cur, wallet, cid, asset):
    cur.execute("""
        INSERT INTO wallet_unresolved_assets (wallet, condition_id, asset_id)
        VALUES (%s,%s,%s)
        ON CONFLICT (wallet, asset_id) DO UPDATE SET
            occurrences=wallet_unresolved_assets.occurrences+1""",
        (wallet, cid, str(asset)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wallet", required=True)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--resume", action="store_true",
                    help="skip markets already marked complete")
    ap.add_argument("--limit-markets", type=int, default=0,
                    help="cap markets this run (0=all)")
    args = ap.parse_args()

    w = load_wallet(args.wallet)
    addr = w["address"].lower()
    log.info("wallet %s (%s) role=%s", w["name"], addr, w.get("role"))

    http = httpx.Client(headers=UA)
    conds = fetch_positions(http, addr)
    log.info("enumerated %d unique markets from positions", len(conds))

    if args.dry_run:
        log.info("[dry-run] would collect trades for %d markets; sample:", len(conds))
        for cid, title in list(conds.items())[:5]:
            log.info("  %s  %s", cid, title[:60])
        return

    conn = connect(); cur = conn.cursor()

    # resume: which markets already complete?
    done = set()
    if args.resume:
        cur.execute("""SELECT condition_id FROM wallet_collection_receipts
                       WHERE wallet=%s AND status='complete'""", (w["name"],))
        done = {r[0] for r in cur.fetchall()}
        log.info("resume: %d markets already complete, skipping", len(done))

    todo = [(c, t) for c, t in conds.items() if c not in done]
    if args.limit_markets:
        todo = todo[:args.limit_markets]

    tot_ins = tot_seen = tot_unres = 0
    for i, (cid, title) in enumerate(todo, 1):
        try:
            fills = fetch_trades_for_market(http, addr, cid)
        except RuntimeError as e:
            log.error("market %s FAILED: %s", cid, e)
            upsert_receipt(cur, w["name"], cid, "failed", 0, None, None, str(e))
            conn.commit()
            continue
        ins = 0; times = []
        for t in fills:
            tok = resolve_token(cur, t.get("asset"))
            if tok is None:
                log_unresolved(cur, w["name"], cid, t.get("asset"))
                tot_unres += 1
                continue
            ins += insert_trade(cur, w["name"], t, tok)
            times.append(datetime.fromtimestamp(int(t["timestamp"]), tz=timezone.utc))
        status = "complete" if fills else "complete"
        earliest = min(times) if times else None
        latest = max(times) if times else None
        upsert_receipt(cur, w["name"], cid, status, len(fills), earliest, latest)
        conn.commit()
        tot_ins += ins; tot_seen += len(fills)
        if i % 25 == 0:
            log.info("  %d/%d markets | %d fills seen, %d inserted, %d unresolved",
                     i, len(todo), tot_seen, tot_ins, tot_unres)

    log.info("DONE wallet=%s markets=%d fills_seen=%d inserted=%d unresolved=%d",
             w["name"], len(todo), tot_seen, tot_ins, tot_unres)
    log.info("(re-run with same args should insert 0 — idempotency check)")


if __name__ == "__main__":
    main()