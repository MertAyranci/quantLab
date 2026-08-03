"""Paper-soak harness — run the engine against LIVE recorded data.

Exercises the complete plumbing (book read -> risk gate -> fill sim -> tracker
-> ledger) using real collector data, previewing the H4 strategy WITHOUT
claiming profitability.

Two phases:
  1. smoke test: a couple of trivial fixed intents, to prove the pipe works.
  2. H4-shaped rule (default): for each actively-watched, open market, if the
     YES token's executable best ask >= 0.95 on a fresh valid book with no
     collector gap, submit a small fixed-notional ($20) marketable-limit IOC
     buy. One entry per market. Record every skip and rejection reason.

This is paper mode: no real orders, no jurisdiction issue. Run from anywhere.

Usage:
  .venv/bin/python execution/paper_soak.py --once          # one pass, report
  .venv/bin/python execution/paper_soak.py --smoke         # fixed-intent smoke
  .venv/bin/python execution/paper_soak.py                 # loop every 60s
"""

from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from dotenv import dotenv_values

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent))
from positions import Tracker
from fill_sim import FillConfig
from order_manager import OrderManager, Intent
from book_reader import read_book

REPO = Path(__file__).resolve().parents[1]
ENV = dotenv_values(REPO / ".env")

ENTRY_THRESHOLD = 0.95        # buy likely-winner at >= 95c
NOTIONAL = 20.0               # $ per entry
LIVE_CAPITAL = 500.0
LEDGER = REPO / "logs" / "paper_soak_ledger.jsonl"


def connect():
    return psycopg2.connect(host="127.0.0.1", port=5432, dbname="quantlab",
                            user="quantlab", password=ENV["PG_PASSWORD"])


def watched_open_markets(cur):
    """Actively watched markets with their YES venue-token price context."""
    cur.execute("""
        SELECT w.market_id, m.condition_id,
               (SELECT id FROM tokens WHERE market_id=w.market_id AND outcome_index=0) AS yes_tok
        FROM h4_watch w
        JOIN markets m ON m.id = w.market_id
        WHERE w.status='watching'""")
    return cur.fetchall()


def run_h4_rule(mgr: OrderManager, cur, entered: set) -> dict:
    now = datetime.now(timezone.utc)
    stats = {"considered": 0, "entered": 0, "skipped_below_thresh": 0,
             "skipped_already": 0, "risk_rejected": 0, "fill_rejected": 0,
             "filled": 0}
    for market_id, condition_id, yes_tok in watched_open_markets(cur):
        stats["considered"] += 1
        if market_id in entered:
            stats["skipped_already"] += 1
            continue
        book = read_book(cur, market_id, now, outcome_index=0)
        # H4 rule: enter only if executable ask >= threshold
        ask = book.best_ask
        # prefer full-depth best ask if present
        if book.full_asks:
            valid_asks = [p for p, s in book.full_asks if p is not None and s > 0]
            if valid_asks:
                ask = min(valid_asks)
        if ask is None:
            continue
        if ask < ENTRY_THRESHOLD:
            stats["skipped_below_thresh"] += 1
            continue
        # submit a small buy at a marketable limit (1 tick above ask, capped <1)
        limit = min(0.999, ask + 0.005)
        intent = Intent(market_id=market_id, token_id=yes_tok or market_id,
                        condition_id=condition_id or f"0x{market_id}",
                        side="buy", size_usd=NOTIONAL, limit_price=limit,
                        allow_extreme=True)   # near-certainties are legitimately >99c
        row = mgr.submit(intent, book)
        st = row["status"]
        if st == "risk_rejected":
            stats["risk_rejected"] += 1
        elif st == "fill_rejected":
            stats["fill_rejected"] += 1
        elif st in ("filled", "partial"):
            stats["filled"] += 1
            stats["entered"] += 1
            entered.add(market_id)
    return stats


def run_smoke(mgr: OrderManager, cur):
    """Two trivial fixed intents against the first two watched markets."""
    now = datetime.now(timezone.utc)
    mkts = watched_open_markets(cur)[:2]
    print("smoke test: 2 fixed intents")
    for market_id, condition_id, yes_tok in mkts:
        book = read_book(cur, market_id, now, outcome_index=0)
        intent = Intent(market_id=market_id, token_id=yes_tok or market_id,
                        condition_id=condition_id or f"0x{market_id}",
                        side="buy", size_usd=10.0, limit_price=0.999,
                        allow_extreme=True)
        row = mgr.submit(intent, book)
        print(f"  market {market_id}: {row['status']} "
              f"{row.get('reason', row.get('book_source',''))}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--smoke", action="store_true")
    args = ap.parse_args()

    LEDGER.parent.mkdir(exist_ok=True)
    conn = connect()
    cur = conn.cursor()
    tracker = Tracker(live_capital=LIVE_CAPITAL)
    cfg = FillConfig.base()
    cfg.full_book_ttl_s = 600.0     # near-certainties are stable; a 10-min-old
    cfg.bbo_ttl_s = 600.0           # quote on a pinned market is still valid
    mgr = OrderManager(tracker, cfg, mode="paper",
                       ledger_path=LEDGER, fee_bps=0.0)
    entered: set = set()

    if args.smoke:
        run_smoke(mgr, cur)
        print("\ntracker:", tracker.summary())
        return

    import httpx
    from book_reader import read_book
    sweep_http = httpx.Client(headers={"User-Agent": "quant-lab-exec/0.1"}, timeout=30)
    pass_n = 0
    while True:
        pass_n += 1
        stats = run_h4_rule(mgr, cur, entered)

        # mark open positions to current conservative price (bid) each pass
        now = datetime.now(timezone.utc)
        for mid in list(tracker.positions.keys()):
            book = read_book(cur, mid, now, outcome_index=0)
            if book.best_bid is not None:
                tracker.update_mark(mid, book.best_bid)

        # resolution sweep every 5th pass (~5 min) — settle resolved markets
        if pass_n % 5 == 0:
            actions = tracker.resolution_sweep(sweep_http)
            settled = [a for a in actions if a.get("action") == "settled"]
            if settled:
                print(f"  RESOLVED: {len(settled)} positions settled -> "
                      f"realized now ${tracker.realized_pnl:.2f}")
                # allow re-entry if a settled market re-appears (it won't, but clean)
                for a in settled:
                    entered.discard(a["market_id"])

        print(f"{now.isoformat()} pass {pass_n}: {stats}")
        print(f"  tracker: {tracker.summary()}")
        conn.commit()
        if args.once:
            break
        time.sleep(60)


if __name__ == "__main__":
    main()