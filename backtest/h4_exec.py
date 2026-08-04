"""H4-Exec — execution backtester for the resolution-drift hypothesis.

Unlike H4-Cal (calibration on reference prices), this REPLAYS resolved Grade-A
markets through the SAME fill simulator the live engine uses, so it answers the
question that actually matters:

  Does the near-certainty edge survive spread, fees, liquidity, latency?
  i.e. is the EXECUTABLE return (buy at the real ask through the book) still
  positive after the THEORETICAL edge (buy at reference price) pays its costs?

For each resolved Grade-A market:
  1. entry point = last valid book at (resolution_time - horizon)
  2. read the recorded book AS OF that time (strict, no look-ahead)
  3. simulate a marketable-limit near-certainty buy through fill_sim
  4. hold to resolution; settle won->$1, lost->$0
  5. record theoretical vs executable P&L, book source, fill quality

Reports (per spec): P&L by book source, fill/partial rates, three latency
scenarios, and tail metrics (complete losses, CVaR, worst streak). Executable
results are NEVER presented as equivalent to theoretical.

Usage:
  .venv/bin/python backtest/h4_exec.py --horizon-hours 1
  .venv/bin/python backtest/h4_exec.py --horizon-hours 3 --scenario conservative
"""

from __future__ import annotations

import argparse
import sys
from datetime import timedelta
from pathlib import Path

import psycopg2
from dotenv import dotenv_values

REPO = Path(__file__).resolve().parents[1]
ENV = dotenv_values(REPO / ".env")
sys.path.insert(0, str(REPO / "execution"))
from fill_sim import simulate_fill, FillConfig, FillStatus, BookSource  # noqa: E402
from book_reader import read_book  # noqa: E402

ENTRY_THRESHOLD_MC = 950       # only "buy" near-certainties >= 95c
NOTIONAL = 20.0                # $ per simulated entry


def connect():
    return psycopg2.connect(host="127.0.0.1", port=5432, dbname="quantlab",
                            user="quantlab", password=ENV["PG_PASSWORD"])


def grade_a_markets(cur, horizon_hours):
    """Resolved markets with last-mile book data around the entry horizon and a
    known winner. Returns (market_id, closed_time, yes_won)."""
    cur.execute("""
        WITH yes AS (SELECT id AS token_id, market_id FROM tokens WHERE outcome_index=0)
        SELECT w.market_id, w.closed_time,
               (r.winning_token_id = y.token_id) AS yes_won
        FROM h4_watch w
        JOIN yes y ON y.market_id = w.market_id
        JOIN resolutions r ON r.market_id = w.market_id
        WHERE w.status='closed' AND w.closed_time IS NOT NULL
          AND EXISTS (
            SELECT 1 FROM tob_snapshots ts
            WHERE ts.token_id = y.token_id
              AND ts.capture_time < w.closed_time
              AND ts.capture_time > w.closed_time - (%s * interval '1 hour') - interval '30 min'
          )
        ORDER BY w.closed_time DESC
    """, (horizon_hours,))
    return cur.fetchall()


def cvar(pnls, alpha=0.05):
    if not pnls:
        return 0.0
    s = sorted(pnls)
    k = max(1, int(alpha * len(s)))
    return sum(s[:k]) / k


def worst_streak(pnls):
    worst = cur = 0
    for p in pnls:
        cur = cur + 1 if p < 0 else 0
        worst = max(worst, cur)
    return worst


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--horizon-hours", type=float, default=1.0)
    ap.add_argument("--scenario", choices=["optimistic", "base", "conservative"],
                    default="base")
    args = ap.parse_args()

    cfg = {"optimistic": FillConfig.optimistic(),
           "base": FillConfig.base(),
           "conservative": FillConfig.conservative()}[args.scenario]

    conn = connect()
    cur = conn.cursor()
    markets = grade_a_markets(cur, args.horizon_hours)

    print(f"H4-Exec — horizon={args.horizon_hours}h  scenario={args.scenario}")
    print(f"Grade-A markets with book at horizon: {len(markets)}\n")
    if not markets:
        print("No replayable markets at this horizon yet. Grade-A set grows as "
              "watched markets resolve; retry as it accumulates.")
        return

    rows = []
    for market_id, closed_time, yes_won in markets:
        entry_time = closed_time - timedelta(hours=args.horizon_hours)
        book = read_book(cur, market_id, entry_time, outcome_index=0)

        # executable ask (prefer full depth)
        ask = book.best_ask
        if book.full_asks:
            valid = [p for p, s in book.full_asks if p is not None and s > 0]
            if valid:
                ask = min(valid)
        if ask is None or ask < ENTRY_THRESHOLD_MC / 1000.0:
            rows.append({"market_id": market_id, "status": "no_entry",
                         "won": yes_won})
            continue

        limit = min(0.999, ask + 0.005)
        shares_req = NOTIONAL / limit
        fr = simulate_fill(market_id, 0, "buy", shares_req, limit, book, cfg)

        if fr.fill_status is FillStatus.REJECTED:
            rows.append({"market_id": market_id, "status": "fill_rejected",
                         "won": yes_won, "reason": fr.rejection_reason})
            continue

        # settle: each filled share pays $1 if won else $0
        cost = fr.filled_shares * fr.avg_fill_price + fr.fees
        payout = fr.filled_shares * (1.0 if yes_won else 0.0)
        exec_pnl = payout - cost
        # theoretical: same shares bought at reference (mid ~ ask here) -> same
        # shares, but pay reference (use ask as proxy reference; the honest point
        # is executable already includes any walk-up beyond best)
        theo_cost = fr.filled_shares * ask
        theo_pnl = fr.filled_shares * (1.0 if yes_won else 0.0) - theo_cost

        rows.append({
            "market_id": market_id, "status": fr.fill_status.value,
            "won": yes_won, "book_source": fr.book_source.value,
            "ask": ask, "avg_fill": fr.avg_fill_price,
            "filled": fr.filled_shares, "requested": fr.requested_shares,
            "exec_pnl": exec_pnl, "theo_pnl": theo_pnl, "cost": cost})

    # ---- aggregate ----
    fills = [r for r in rows if r["status"] in ("filled", "partial")]
    n_entry = len(fills)
    n_no_entry = sum(1 for r in rows if r["status"] == "no_entry")
    n_reject = sum(1 for r in rows if r["status"] == "fill_rejected")
    n_partial = sum(1 for r in rows if r["status"] == "partial")

    print(f"{'entries':<22}{n_entry}")
    print(f"{'  partial fills':<22}{n_partial}")
    print(f"{'no-entry (<95c)':<22}{n_no_entry}")
    print(f"{'fill-rejected':<22}{n_reject}\n")

    if not fills:
        print("No executable entries — nothing to score.")
        return

    exec_pnls = [r["exec_pnl"] for r in fills]
    theo_pnls = [r["theo_pnl"] for r in fills]
    losses = [r for r in fills if not r["won"]]
    by_source = {}
    for r in fills:
        by_source.setdefault(r["book_source"], []).append(r["exec_pnl"])

    print("--- RETURN (executable vs theoretical) ---")
    print(f"{'total executable PnL':<28}${sum(exec_pnls):+.2f}")
    print(f"{'total theoretical PnL':<28}${sum(theo_pnls):+.2f}")
    print(f"{'mean executable / trade':<28}${sum(exec_pnls)/n_entry:+.3f}")
    print(f"{'mean theoretical / trade':<28}${sum(theo_pnls)/n_entry:+.3f}")
    print(f"{'cost drag (theo - exec)':<28}${(sum(theo_pnls)-sum(exec_pnls)):.2f}")

    print("\n--- TAIL / RISK ---")
    print(f"{'complete losses':<28}{len(losses)} / {n_entry} "
          f"({100.0*len(losses)/n_entry:.0f}%)")
    print(f"{'worst single trade':<28}${min(exec_pnls):+.2f}")
    print(f"{'CVaR(5%)':<28}${cvar(exec_pnls):+.2f}")
    print(f"{'worst losing streak':<28}{worst_streak(exec_pnls)}")

    print("\n--- BY BOOK SOURCE ---")
    for src, pnls in by_source.items():
        print(f"  {src:<16} n={len(pnls):<4} total=${sum(pnls):+.2f} "
              f"mean=${sum(pnls)/len(pnls):+.3f}")

    print(f"\nNOTE: {len(markets)} Grade-A markets is a SMALL sample. This "
          f"validates the pipeline; the verdict firms up as more watched markets "
          f"resolve. Executable != theoretical — the gap is the cost of trading.")


if __name__ == "__main__":
    main()