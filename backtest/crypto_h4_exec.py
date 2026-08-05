"""Crypto H4-Exec (CORRECTED) — favourite-buying near-certainty test + H4b underdog.

CORRECTION vs the flawed first version: a favourite is near-certain when its
executable BID is high (someone pays >=95c), NOT when its ask is high (bid 70c /
ask 99c is illiquid, not near-certain). Signal:
    favourite best_bid >= 950mc AND spread <= 20mc AND ask < 1000mc

Two strategies, separate verdicts:
  H4  — buy the favourite at its ask (does the near-certainty pay net of cost?)
  H4b — buy the UNDERDOG (opposite token) at its ACTUAL recorded ask
        (are favourites overpriced, making the cheap underdog +EV?)

Both: 1s latency, first book at/after arrival, full-depth walk or BBO+50% haircut,
marketable-limit IOC, partial fills, fees, ROI on FILLED capital, event-boundary
(5-min end_date) clustered bootstrap CIs. One entry per market.

Discipline: NOTHING here is a confirmed edge. Any rule that looks good must be
frozen and validated on a later out-of-sample cohort (the cron is capturing it).

Usage:
  .venv/bin/python backtest/crypto_h4_exec.py --horizon-sec 30
  .venv/bin/python backtest/crypto_h4_exec.py --strategy h4b --horizon-sec 30
  .venv/bin/python backtest/crypto_h4_exec.py --all-horizons
"""

from __future__ import annotations

import argparse
import random
import statistics
import sys
from datetime import timedelta
from pathlib import Path

import psycopg2
from dotenv import dotenv_values

REPO = Path(__file__).resolve().parents[1]
ENV = dotenv_values(REPO / ".env")
sys.path.insert(0, str(REPO / "execution"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from crypto_book_reader import read_market_book  # noqa: E402

FAV_BID_MIN = 950         # favourite near-certain: bid >= 95c
FAV_SPREAD_MAX = 20       # mc
LATENCY_S = 1.0
NOTIONAL = 20.0
FEE_BPS = 0.0
HAIRCUT = 0.50            # BBO fallback displayed-size haircut


def connect():
    return psycopg2.connect(host="127.0.0.1", port=5432, dbname="quantlab",
                            user="quantlab", password=ENV["PG_PASSWORD"])


def resolved_markets(cur):
    cur.execute("""
        SELECT m.id, m.slug, m.end_date, wt.outcome_index AS winning_outcome
        FROM markets m
        JOIN resolutions r ON r.market_id=m.id
        JOIN tokens wt ON wt.id=r.winning_token_id
        WHERE m.source='crypto_harvester' AND m.end_date < now()
        ORDER BY m.end_date""")
    return cur.fetchall()


def walk_buy(side, notional, limit_mc):
    """Marketable-limit IOC buy of `notional` dollars against `side`'s asks.
    Full depth if present else BBO+haircut. Returns (filled_shares, cost, worst_mc, src)."""
    if side.src == "FULL_DEPTH" and side.full_asks:
        filled = 0.0; cost = 0.0; worst = None
        want_dollars = notional
        for mc, sz in side.full_asks:
            if mc > limit_mc:
                break
            px = mc / 1000.0
            # dollars available at this level
            lvl_dollars = sz * px
            take_dollars = min(want_dollars - cost, lvl_dollars)
            if take_dollars <= 0:
                break
            shares = take_dollars / px
            filled += shares; cost += take_dollars; worst = mc
            if cost >= want_dollars - 1e-9:
                break
        return filled, cost, worst, "FULL_DEPTH"
    # BBO fallback
    if side.best_ask_mc is not None and side.best_ask_mc <= limit_mc:
        px = side.best_ask_mc / 1000.0
        usable = (side.best_ask_size or 0) * HAIRCUT
        want_shares = notional / px
        shares = min(want_shares, usable)
        return shares, shares * px, side.best_ask_mc, "BBO_FALLBACK"
    return 0.0, 0.0, None, "NONE"


def run(strategy, horizon_sec):
    conn = connect(); cur = conn.cursor()
    mkts = resolved_markets(cur)
    trades = []
    rej = {}
    for mid, slug, end_date, win in mkts:
        as_of = end_date - timedelta(seconds=horizon_sec) + timedelta(seconds=LATENCY_S)
        mb, reason = read_market_book(cur, mid, slug, end_date, win, as_of)
        if mb is None:
            rej[reason] = rej.get(reason, 0) + 1
            continue
        # identify favourite: side with bid >= FAV_BID_MIN and spread <= max and ask<1000
        fav = None
        for oi, sb in mb.sides.items():
            sp = sb.spread_mc()
            if (sb.best_bid_mc is not None and sb.best_bid_mc >= FAV_BID_MIN
                    and sp is not None and sp <= FAV_SPREAD_MAX
                    and sb.best_ask_mc is not None and sb.best_ask_mc < 1000):
                fav = (oi, sb)
        if fav is None:
            rej["no_favourite"] = rej.get("no_favourite", 0) + 1
            continue
        fav_oi, fav_sb = fav
        under_oi = 1 - fav_oi
        under_sb = mb.sides[under_oi]

        if strategy == "h4":
            # buy favourite at its ask
            limit = fav_sb.best_ask_mc
            filled, cost, worst, src = walk_buy(fav_sb, NOTIONAL, limit)
            if filled <= 1e-9:
                rej["fav_no_fill"] = rej.get("fav_no_fill", 0) + 1
                continue
            won = (fav_oi == win)
            entry_mc = fav_sb.best_ask_mc
            spread = fav_sb.spread_mc()
            asset = slug.split("-")[0]
        else:  # h4b — buy underdog at its ACTUAL ask
            if under_sb.best_ask_mc is None:
                rej["under_no_ask"] = rej.get("under_no_ask", 0) + 1
                continue
            # base rule: underdog ask in [10,100] mc
            if not (10 <= under_sb.best_ask_mc <= 100):
                rej["under_ask_out_of_band"] = rej.get("under_ask_out_of_band", 0) + 1
                continue
            limit = under_sb.best_ask_mc
            filled, cost, worst, src = walk_buy(under_sb, NOTIONAL, limit)
            if filled <= 1e-9:
                rej["under_no_fill"] = rej.get("under_no_fill", 0) + 1
                continue
            won = (under_oi == win)
            entry_mc = under_sb.best_ask_mc
            spread = fav_sb.spread_mc()   # favourite spread (the signal quality)
            asset = slug.split("-")[0]

        fees = cost * (FEE_BPS / 10000.0)
        payout = filled * (1.0 if won else 0.0)
        pnl = payout - cost - fees
        roi = pnl / cost if cost > 0 else 0.0
        # boundary = the 5-min end_date (for clustering)
        boundary = int(end_date.timestamp()) // 300
        trades.append({"mid": mid, "asset": asset, "boundary": boundary,
                       "entry_mc": entry_mc, "spread_mc": spread, "won": won,
                       "filled": filled, "cost": cost, "pnl": pnl, "roi": roi,
                       "src": src})
    return trades, rej, len(mkts)


def boundary_bootstrap_ci(trades, key="pnl", iters=2000):
    """Bootstrap resampling BY 5-min boundary (crypto assets at the same boundary
    are correlated). Returns (lo, hi) 95% CI of the MEAN of `key`."""
    if not trades:
        return (0.0, 0.0)
    by_b = {}
    for t in trades:
        by_b.setdefault(t["boundary"], []).append(t[key])
    boundaries = list(by_b.values())
    means = []
    for _ in range(iters):
        sample = []
        for _ in range(len(boundaries)):
            grp = random.choice(boundaries)
            sample.extend(grp)
        if sample:
            means.append(statistics.mean(sample))
    means.sort()
    lo = means[int(0.025 * len(means))]
    hi = means[int(0.975 * len(means))]
    return (lo, hi)


def report(strategy, horizon_sec, trades, rej, n_mkts):
    label = "H4 favourite-buy" if strategy == "h4" else "H4b underdog-value"
    print(f"\n{'='*66}")
    print(f"{label}  |  horizon T-{horizon_sec}s  |  markets={n_mkts}")
    print('='*66)
    if not trades:
        print("no executed trades. rejections:", rej)
        return
    n = len(trades)
    wins = sum(1 for t in trades if t["won"])
    total_pnl = sum(t["pnl"] for t in trades)
    total_cost = sum(t["cost"] for t in trades)
    mean_pnl = total_pnl / n
    roi = total_pnl / total_cost if total_cost > 0 else 0.0
    lo, hi = boundary_bootstrap_ci(trades, "pnl")
    n_boundaries = len(set(t["boundary"] for t in trades))
    avg_entry = statistics.mean(t["entry_mc"] for t in trades) / 10.0

    print(f"executed trades           {n}   (across {n_boundaries} distinct 5-min boundaries)")
    print(f"win rate                  {100*wins/n:.1f}%")
    print(f"avg entry price           {avg_entry:.1f}c")
    if strategy == "h4b":
        be = avg_entry / 100.0
        print(f"break-even win rate       {100*be:.1f}%  (need to win this often at {avg_entry:.1f}c)")
    print(f"total filled capital      ${total_cost:.2f}")
    print(f"net PnL                    ${total_pnl:+.2f}")
    print(f"mean PnL / trade          ${mean_pnl:+.4f}")
    print(f"ROI on filled capital     {100*roi:+.2f}%")
    print(f"95% CI mean PnL (boundary-clustered)  [{lo:+.4f}, {hi:+.4f}]")
    verdict = "POSITIVE (CI excludes 0)" if lo > 0 else \
              "negative (CI excludes 0)" if hi < 0 else "INCONCLUSIVE (CI spans 0)"
    print(f"  -> {verdict}")

    # tail
    losses = [t for t in trades if not t["won"]]
    if losses:
        print(f"\ntail: {len(losses)} losses ({100*len(losses)/n:.1f}%), "
              f"avg loss ${statistics.mean(t['pnl'] for t in losses):+.4f}")
    # drawdown & streak (sequential by boundary then mid)
    seq = sorted(trades, key=lambda t: (t["boundary"], t["mid"]))
    cum = 0; peak = 0; maxdd = 0; streak = 0; worst_streak = 0
    for t in seq:
        cum += t["pnl"]; peak = max(peak, cum); maxdd = min(maxdd, cum - peak)
        streak = streak + 1 if t["pnl"] < 0 else 0
        worst_streak = max(worst_streak, streak)
    print(f"max drawdown ${maxdd:.2f}  |  longest losing streak {worst_streak}")

    # breakdowns
    def bucket(name, keyfn, order=None):
        groups = {}
        for t in trades:
            k = keyfn(t)
            if k is None: continue
            groups.setdefault(k, []).append(t)
        print(f"\n--- by {name} ---")
        keys = order if order else sorted(groups.keys())
        for k in keys:
            g = groups.get(k)
            if not g: continue
            gp = sum(x["pnl"] for x in g); gc = sum(x["cost"] for x in g)
            gw = sum(1 for x in g if x["won"])
            print(f"  {str(k):<14} n={len(g):<5} win%={100*gw/len(g):>5.1f} "
                  f"meanPnL=${gp/len(g):+.4f} ROI={100*gp/gc if gc>0 else 0:+.1f}%")

    bucket("asset", lambda t: t["asset"])
    bucket("book source", lambda t: t["src"])
    if strategy == "h4":
        bucket("favourite price band", lambda t:
               "95-97" if t["entry_mc"]<970 else "97-98" if t["entry_mc"]<980
               else "98-99" if t["entry_mc"]<990 else "99-100",
               order=["95-97","97-98","98-99","99-100"])
        bucket("spread", lambda t: "<=10mc" if t["spread_mc"]<=10 else
               "<=20mc" if t["spread_mc"]<=20 else ">20mc",
               order=["<=10mc","<=20mc",">20mc"])
    else:
        bucket("underdog ask band", lambda t:
               "1-2c" if t["entry_mc"]<20 else "2-3c" if t["entry_mc"]<30
               else "3-5c" if t["entry_mc"]<50 else "5-7c" if t["entry_mc"]<70
               else "7-10c", order=["1-2c","2-3c","3-5c","5-7c","7-10c"])
    if rej:
        print(f"\nrejections: {dict(sorted(rej.items(), key=lambda x:-x[1]))}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--strategy", choices=["h4","h4b"], default="h4")
    ap.add_argument("--horizon-sec", type=int, default=30)
    ap.add_argument("--all-horizons", action="store_true")
    args = ap.parse_args()

    horizons = [60,30,15,5] if args.all_horizons else [args.horizon_sec]
    strategies = ["h4","h4b"] if args.all_horizons else [args.strategy]
    for strat in strategies:
        for h in horizons:
            trades, rej, nm = run(strat, h)
            report(strat, h, trades, rej, nm)
    print("\nDISCIPLINE: no bucket above is a confirmed edge. Freeze one rule and "
          "validate on a FORWARD out-of-sample cohort before any claim.")


if __name__ == "__main__":
    main()