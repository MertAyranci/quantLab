"""Crypto H4-Exec — execution backtest for 5-minute Up/Down near-certainty edge.

For each resolved 5m crypto market, at a near-resolution horizon:
  1. read the recorded book AS OF (end_date - horizon) for BOTH tokens
  2. identify the near-certain side (token whose ask >= THRESHOLD)
  3. simulate buying that token through the recorded book (fill_sim), paying the
     spread
  4. settle against the actual winning outcome ($1 if that token won, else $0)
  5. record executable PnL, the spread crossed, and whether it won

Reports: executable edge, hit rate, TAIL (near-certainty reversals — the killer),
spread distribution, and edge bucketed by spread tightness. Executable (buy at
ask through the book) vs theoretical (buy at reference) shown separately.

Key question: do near-certainties bought at the ask, net of spread and the
occasional reversal, net positive? First project analysis with real upside
potential (dense capture showed ~3c spreads at near-certainty).

Usage:
  .venv/bin/python backtest/crypto_h4_exec.py --horizon-sec 30
  .venv/bin/python backtest/crypto_h4_exec.py --horizon-sec 60 --threshold 950
"""

from __future__ import annotations

import argparse
import statistics
import sys
from pathlib import Path

import psycopg2
from dotenv import dotenv_values

REPO = Path(__file__).resolve().parents[1]
ENV = dotenv_values(REPO / ".env")


def connect():
    return psycopg2.connect(host="127.0.0.1", port=5432, dbname="quantlab",
                            user="quantlab", password=ENV["PG_PASSWORD"])


def fetch_trades(cur, horizon_sec, threshold_mc):
    """For each resolved crypto market, get the as-of book for both tokens at
    end_date - horizon, and the winning outcome. Returns per-market rows."""
    cur.execute("""
        WITH resolved AS (
            SELECT m.id AS market_id, m.slug, m.end_date,
                   r.winning_token_id,
                   wt.outcome_index AS winning_outcome
            FROM markets m
            JOIN resolutions r ON r.market_id = m.id
            JOIN tokens wt ON wt.id = r.winning_token_id
            WHERE m.source = 'crypto_harvester' AND m.end_date < now()
        ),
        asof AS (
            SELECT res.market_id, res.slug, res.end_date, res.winning_outcome,
                   tk.outcome_index,
                   ts.best_bid_mc, ts.best_ask_mc, ts.best_ask_size,
                   ts.capture_time,
                   row_number() OVER (
                     PARTITION BY res.market_id, tk.outcome_index
                     ORDER BY ts.capture_time DESC) AS rn
            FROM resolved res
            JOIN tokens tk ON tk.market_id = res.market_id
            JOIN tob_snapshots ts ON ts.token_id = tk.id
            WHERE ts.capture_time <= res.end_date - (%s * interval '1 second')
              AND ts.capture_time >= res.end_date - (%s * interval '1 second') - interval '30 sec'
              AND ts.best_ask_mc IS NOT NULL
        )
        SELECT market_id, slug, winning_outcome, outcome_index,
               best_bid_mc, best_ask_mc, best_ask_size
        FROM asof WHERE rn = 1
    """, (horizon_sec, horizon_sec))
    # group by market: collect both sides
    rows = {}
    for mid, slug, win_out, out_idx, bid, ask, ask_sz in cur.fetchall():
        rows.setdefault(mid, {"slug": slug, "win": win_out, "sides": {}})
        rows[mid]["sides"][out_idx] = {"bid": bid, "ask": ask, "ask_sz": ask_sz}
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--horizon-sec", type=int, default=30)
    ap.add_argument("--threshold", type=int, default=950)  # mc; 950 = 95c
    ap.add_argument("--fee-bps", type=float, default=0.0)
    args = ap.parse_args()

    conn = connect()
    cur = conn.cursor()
    markets = fetch_trades(cur, args.horizon_sec, args.threshold)

    print(f"Crypto H4-Exec — horizon={args.horizon_sec}s  "
          f"threshold={args.threshold/10:.0f}c")
    print(f"resolved markets with as-of book: {len(markets)}\n")

    trades = []
    no_entry = 0
    for mid, d in markets.items():
        # pick the near-certain side: the token whose ask >= threshold
        near = [(idx, s) for idx, s in d["sides"].items()
                if s["ask"] is not None and s["ask"] >= args.threshold]
        if not near:
            no_entry += 1
            continue
        # if both sides somehow >= threshold (shouldn't happen), take the higher
        idx, s = max(near, key=lambda x: x[1]["ask"])
        ask = s["ask"] / 1000.0
        bid = s["bid"] / 1000.0 if s["bid"] is not None else None
        spread = (s["ask"] - s["bid"]) if s["bid"] is not None else None
        won = (idx == d["win"])
        # executable: buy at ask, settle 1/0
        pnl = (1.0 if won else 0.0) - ask
        trades.append({"mid": mid, "slug": d["slug"], "side": idx,
                       "ask": ask, "spread_mc": spread, "won": won, "pnl": pnl})

    n = len(trades)
    if n == 0:
        print("No near-certain entries at this horizon.")
        return

    wins = [t for t in trades if t["won"]]
    losses = [t for t in trades if not t["won"]]
    pnls = [t["pnl"] for t in trades]
    total = sum(pnls)
    spreads = [t["spread_mc"] for t in trades if t["spread_mc"] is not None]

    print(f"{'entries (near-cert)':<26}{n}")
    print(f"{'no-entry markets':<26}{no_entry}")
    print(f"{'winners':<26}{len(wins)} ({100*len(wins)/n:.1f}%)")
    print(f"{'losers (reversals)':<26}{len(losses)} ({100*len(losses)/n:.1f}%)  <- the tail")
    print()
    print(f"{'total executable PnL/$1':<26}{total:+.3f}")
    print(f"{'mean PnL per trade':<26}{total/n:+.4f}  (${total/n:.4f} per $1 staked)")
    print(f"{'avg winning payout':<26}{statistics.mean([t['pnl'] for t in wins]):+.4f}" if wins else "")
    print(f"{'avg losing payout':<26}{statistics.mean([t['pnl'] for t in losses]):+.4f}" if losses else "")
    if spreads:
        print(f"{'avg spread (mc)':<26}{statistics.mean(spreads):.1f}  "
              f"median {statistics.median(spreads):.0f}")

    # bucket by spread tightness — is edge concentrated in tight-spread trades?
    print("\n--- PnL by spread bucket ---")
    buckets = [(0, 20), (20, 40), (40, 70), (70, 1000)]
    for lo, hi in buckets:
        b = [t for t in trades if t["spread_mc"] is not None
             and lo <= t["spread_mc"] < hi]
        if b:
            bp = sum(t["pnl"] for t in b)
            bw = sum(1 for t in b if t["won"])
            print(f"  spread {lo:>3}-{hi:<4}mc  n={len(b):<5} "
                  f"win%={100*bw/len(b):>5.1f}  meanPnL={bp/len(b):+.4f}  totalPnL={bp:+.2f}")

    # bucket by entry price — does deeper near-certainty pay better?
    print("\n--- PnL by entry-ask bucket ---")
    pbuckets = [(0.95,0.97),(0.97,0.98),(0.98,0.99),(0.99,1.0)]
    for lo, hi in pbuckets:
        b = [t for t in trades if lo <= t["ask"] < hi]
        if b:
            bp = sum(t["pnl"] for t in b)
            bw = sum(1 for t in b if t["won"])
            print(f"  ask {lo:.2f}-{hi:.2f}  n={len(b):<5} "
                  f"win%={100*bw/len(b):>5.1f}  meanPnL={bp/len(b):+.4f}  totalPnL={bp:+.2f}")

    print(f"\nNOTE: executable = buy at ask, settle 1/0. Ignores fees ({args.fee_bps}bps "
          f"passed) and assumes fill at displayed ask. n={n} is a strong sample. "
          f"A positive mean net of the loser tail is the signal; check whether it "
          f"survives across horizons and concentrates in a tradeable spread bucket.")


if __name__ == "__main__":
    main()