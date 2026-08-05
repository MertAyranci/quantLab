"""Crypto H4-Exec — corrected favourite and underdog execution tests.

The signal is formed from the latest causal synchronized book at T-horizon.
Execution is simulated from a separate causal book at signal_time + latency.
The selected token and order limit are frozen at signal time.

Strategies:
  H4  - buy the near-certain favourite
  H4b - buy the opposite underdog

This is still research code. Any selected rule must be frozen and validated on a
strictly later out-of-sample cohort.
"""

from __future__ import annotations

import argparse
import random
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
from dotenv import dotenv_values

REPO = Path(__file__).resolve().parents[1]
ENV = dotenv_values(REPO / ".env")
sys.path.insert(0, str(Path(__file__).resolve().parent))

from crypto_book_reader import read_market_book  # noqa: E402

FAV_BID_MIN_MC = 950
FAV_SPREAD_MAX_MC = 20
UNDERDOG_ASK_MIN_MC = 10
UNDERDOG_ASK_MAX_MC = 100


def connect():
    return psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="quantlab",
        user="quantlab",
        password=ENV["PG_PASSWORD"],
    )


def parse_timestamp(value: str | None):
    if value is None:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def resolved_markets(cur, start_time=None, end_time=None):
    clauses = [
        "m.source = 'crypto_harvester'",
        "m.end_date < now()",
    ]
    params = []

    if start_time is not None:
        clauses.append("m.end_date >= %s")
        params.append(start_time)
    if end_time is not None:
        clauses.append("m.end_date < %s")
        params.append(end_time)

    cur.execute(
        f"""
        SELECT m.id,
               m.slug,
               m.end_date,
               wt.outcome_index AS winning_outcome
        FROM markets m
        JOIN resolutions r ON r.market_id = m.id
        JOIN tokens wt ON wt.id = r.winning_token_id
        WHERE {' AND '.join(clauses)}
        ORDER BY m.end_date, m.id
        """,
        params,
    )
    return cur.fetchall()


def identify_favourite(market_book):
    candidates = []
    for outcome_index, side in market_book.sides.items():
        spread = side.spread_mc()
        if (
            side.best_bid_mc is not None
            and side.best_bid_mc >= FAV_BID_MIN_MC
            and spread is not None
            and 0 <= spread <= FAV_SPREAD_MAX_MC
            and side.best_ask_mc is not None
            and side.best_ask_mc < 1000
        ):
            candidates.append((outcome_index, side))

    if not candidates:
        return None, "no_favourite"
    if len(candidates) > 1:
        return None, "multiple_favourites"
    return candidates[0], None


def walk_buy(side, notional, limit_mc, bbo_haircut):
    """Simulate a marketable-limit IOC buy against one execution-side book."""
    if limit_mc is None or limit_mc <= 0:
        return 0.0, 0.0, None, "NONE"

    if side.src == "FULL_DEPTH" and side.full_asks:
        filled_shares = 0.0
        cost = 0.0
        worst_mc = None

        for price_mc, available_shares in side.full_asks:
            if price_mc > limit_mc:
                break

            price = price_mc / 1000.0
            dollars_remaining = notional - cost
            if dollars_remaining <= 1e-9:
                break

            level_dollars = available_shares * price
            take_dollars = min(dollars_remaining, level_dollars)
            if take_dollars <= 0:
                continue

            filled_shares += take_dollars / price
            cost += take_dollars
            worst_mc = price_mc

        return filled_shares, cost, worst_mc, "FULL_DEPTH"

    if (
        side.src == "BBO_FALLBACK"
        and side.best_ask_mc is not None
        and side.best_ask_mc <= limit_mc
    ):
        price = side.best_ask_mc / 1000.0
        usable_shares = max(0.0, (side.best_ask_size or 0.0) * bbo_haircut)
        requested_shares = notional / price
        filled_shares = min(requested_shares, usable_shares)
        return (
            filled_shares,
            filled_shares * price,
            side.best_ask_mc if filled_shares > 0 else None,
            "BBO_FALLBACK" if filled_shares > 0 else "NONE",
        )

    return 0.0, 0.0, None, "NONE"


def increment(rejections, reason):
    rejections[reason] = rejections.get(reason, 0) + 1


def run(strategy, horizon_sec, args):
    with connect() as conn:
        with conn.cursor() as cur:
            markets = resolved_markets(cur, args.start_time, args.end_time)
            trades = []
            rejections = {}

            for market_id, slug, end_date, winning_outcome in markets:
                signal_time = end_date - timedelta(seconds=horizon_sec)
                arrival_time = signal_time + timedelta(seconds=args.latency_sec)

                signal_book, reason = read_market_book(
                    cur,
                    market_id,
                    slug,
                    end_date,
                    winning_outcome,
                    signal_time,
                )
                if signal_book is None:
                    increment(rejections, f"signal_{reason}")
                    continue

                favourite, reason = identify_favourite(signal_book)
                if favourite is None:
                    increment(rejections, reason)
                    continue

                favourite_outcome, favourite_signal_side = favourite
                underdog_outcome = 1 - favourite_outcome
                underdog_signal_side = signal_book.sides[underdog_outcome]

                if strategy == "h4":
                    target_outcome = favourite_outcome
                    signal_target_side = favourite_signal_side
                    if signal_target_side.best_ask_mc is None:
                        increment(rejections, "fav_signal_no_ask")
                        continue
                else:
                    target_outcome = underdog_outcome
                    signal_target_side = underdog_signal_side
                    if signal_target_side.best_ask_mc is None:
                        increment(rejections, "under_signal_no_ask")
                        continue
                    if not (
                        UNDERDOG_ASK_MIN_MC
                        <= signal_target_side.best_ask_mc
                        <= UNDERDOG_ASK_MAX_MC
                    ):
                        increment(rejections, "under_ask_out_of_band")
                        continue

                signal_ask_mc = signal_target_side.best_ask_mc
                limit_mc = min(999, signal_ask_mc + args.max_slippage_mc)

                execution_book, reason = read_market_book(
                    cur,
                    market_id,
                    slug,
                    end_date,
                    winning_outcome,
                    arrival_time,
                )
                if execution_book is None:
                    increment(rejections, f"execution_{reason}")
                    continue

                execution_side = execution_book.sides[target_outcome]
                filled, cost, worst_mc, source = walk_buy(
                    execution_side,
                    args.notional,
                    limit_mc,
                    args.bbo_haircut,
                )
                if filled <= 1e-9:
                    increment(
                        rejections,
                        "fav_no_fill" if strategy == "h4" else "under_no_fill",
                    )
                    continue

                fill_ratio = cost / args.notional if args.notional > 0 else 0.0
                if fill_ratio < args.min_fill_ratio:
                    increment(rejections, "fill_below_min_ratio")
                    continue

                fees = cost * (args.fee_bps / 10_000.0)
                capital = cost + fees
                won = target_outcome == winning_outcome
                payout = filled if won else 0.0
                pnl = payout - capital
                roi = pnl / capital if capital > 0 else 0.0
                average_fill_mc = (cost / filled) * 1000.0
                boundary = int(end_date.timestamp()) // 300

                trades.append(
                    {
                        "mid": market_id,
                        "slug": slug,
                        "end_date": end_date,
                        "asset": slug.split("-")[0],
                        "boundary": boundary,
                        "strategy": strategy,
                        "target_outcome": target_outcome,
                        "winning_outcome": winning_outcome,
                        "won": won,
                        "signal_time": signal_time,
                        "arrival_time": arrival_time,
                        "signal_book_time": signal_target_side.capture_time,
                        "execution_book_time": execution_side.capture_time,
                        "signal_fav_bid_mc": favourite_signal_side.best_bid_mc,
                        "signal_fav_ask_mc": favourite_signal_side.best_ask_mc,
                        "signal_fav_spread_mc": favourite_signal_side.spread_mc(),
                        "signal_target_ask_mc": signal_ask_mc,
                        "limit_mc": limit_mc,
                        "average_fill_mc": average_fill_mc,
                        "worst_fill_mc": worst_mc,
                        "filled": filled,
                        "fill_ratio": fill_ratio,
                        "cost": cost,
                        "fees": fees,
                        "capital": capital,
                        "payout": payout,
                        "pnl": pnl,
                        "roi": roi,
                        "src": source,
                    }
                )

    return trades, rejections, len(markets)


def aggregate_boundaries(trades):
    grouped = defaultdict(lambda: {"pnl": 0.0, "capital": 0.0, "trades": 0})
    for trade in trades:
        row = grouped[trade["boundary"]]
        row["pnl"] += trade["pnl"]
        row["capital"] += trade["capital"]
        row["trades"] += 1
    return dict(grouped)


def boundary_bootstrap_roi_ci(trades, iters=5000, seed=42):
    """Bootstrap complete five-minute portfolio boundaries and return ROI CI."""
    if not trades:
        return 0.0, 0.0

    grouped = list(aggregate_boundaries(trades).values())
    rng = random.Random(seed)
    estimates = []

    for _ in range(iters):
        pnl = 0.0
        capital = 0.0
        for _ in range(len(grouped)):
            sampled = rng.choice(grouped)
            pnl += sampled["pnl"]
            capital += sampled["capital"]
        estimates.append(pnl / capital if capital > 0 else 0.0)

    estimates.sort()
    lo = estimates[int(0.025 * (len(estimates) - 1))]
    hi = estimates[int(0.975 * (len(estimates) - 1))]
    return lo, hi


def boundary_drawdown_and_streak(trades):
    grouped = aggregate_boundaries(trades)
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    losing_streak = 0
    longest_losing_streak = 0

    for boundary in sorted(grouped):
        boundary_pnl = grouped[boundary]["pnl"]
        cumulative += boundary_pnl
        peak = max(peak, cumulative)
        max_drawdown = min(max_drawdown, cumulative - peak)

        if boundary_pnl < 0:
            losing_streak += 1
        else:
            losing_streak = 0
        longest_losing_streak = max(longest_losing_streak, losing_streak)

    return max_drawdown, longest_losing_streak


def print_bucket(trades, name, key_function, order=None):
    groups = defaultdict(list)
    for trade in trades:
        key = key_function(trade)
        if key is not None:
            groups[key].append(trade)

    print(f"\n--- by {name} ---")
    keys = order if order is not None else sorted(groups)
    for key in keys:
        group = groups.get(key)
        if not group:
            continue

        pnl = sum(row["pnl"] for row in group)
        capital = sum(row["capital"] for row in group)
        wins = sum(row["won"] for row in group)
        print(
            f"  {str(key):<14} n={len(group):<5} "
            f"win%={100 * wins / len(group):>5.1f} "
            f"meanPnL=${pnl / len(group):+.4f} "
            f"ROI={100 * pnl / capital if capital > 0 else 0:+.1f}%"
        )


def report(strategy, horizon_sec, trades, rejections, market_count, args):
    label = "H4 favourite-buy" if strategy == "h4" else "H4b underdog-value"
    print(f"\n{'=' * 72}")
    print(
        f"{label} | T-{horizon_sec}s | markets={market_count} | "
        f"latency={args.latency_sec:g}s | fee={args.fee_bps:g}bps | "
        f"slippage_limit={args.max_slippage_mc}mc"
    )
    print("=" * 72)

    if not trades:
        print("No executed trades.")
        print("Rejections:", dict(sorted(rejections.items(), key=lambda item: -item[1])))
        return

    trade_count = len(trades)
    boundary_count = len({trade["boundary"] for trade in trades})
    wins = sum(trade["won"] for trade in trades)
    losses = trade_count - wins
    total_pnl = sum(trade["pnl"] for trade in trades)
    total_cost = sum(trade["cost"] for trade in trades)
    total_fees = sum(trade["fees"] for trade in trades)
    total_capital = sum(trade["capital"] for trade in trades)
    total_filled = sum(trade["filled"] for trade in trades)
    roi = total_pnl / total_capital if total_capital > 0 else 0.0
    all_in_price = total_capital / total_filled if total_filled > 0 else 0.0
    roi_lo, roi_hi = boundary_bootstrap_roi_ci(
        trades,
        iters=args.bootstrap_iters,
        seed=args.bootstrap_seed,
    )
    max_drawdown, losing_boundary_streak = boundary_drawdown_and_streak(trades)

    print(f"executed trades            {trade_count}")
    print(f"distinct boundaries        {boundary_count}")
    print(f"win rate                   {100 * wins / trade_count:.1f}%")
    print(f"volume-weighted entry      {100 * all_in_price:.2f}c including fees")
    print(f"break-even win rate        {100 * all_in_price:.2f}%")
    fill_ratios = [trade["fill_ratio"] for trade in trades]
    print(f"filled shares              {total_filled:.4f}")
    print(
        f"fill ratio                 mean {100 * statistics.mean(fill_ratios):.1f}% | "
        f"median {100 * statistics.median(fill_ratios):.1f}%"
    )
    print(f"gross execution cost       ${total_cost:.2f}")
    print(f"fees                       ${total_fees:.4f}")
    print(f"total filled capital       ${total_capital:.2f}")
    print(f"net PnL                     ${total_pnl:+.2f}")
    print(f"ROI on filled capital      {100 * roi:+.2f}%")
    print(
        "95% boundary-bootstrap ROI CI "
        f"[{100 * roi_lo:+.2f}%, {100 * roi_hi:+.2f}%]"
    )

    if losses == 0:
        verdict = (
            "TAIL-INCONCLUSIVE: zero observed reversals; the bootstrap cannot "
            "estimate unseen blow-up risk"
        )
    elif roi_lo > 0:
        verdict = "POSITIVE (bootstrap ROI CI excludes 0)"
    elif roi_hi < 0:
        verdict = "NEGATIVE (bootstrap ROI CI excludes 0)"
    else:
        verdict = "INCONCLUSIVE (bootstrap ROI CI spans 0)"
    print(f"  -> {verdict}")

    if losses:
        losing_trades = [trade for trade in trades if not trade["won"]]
        print(
            f"\ntail: {losses} losing trades ({100 * losses / trade_count:.1f}%), "
            f"average loss ${statistics.mean(t['pnl'] for t in losing_trades):+.4f}"
        )
    else:
        # Conservative reminder: a non-parametric bootstrap cannot create an
        # unseen reversal.
        upper_losing_boundary_rate = min(1.0, 3.0 / boundary_count)
        print(
            "\ntail: zero observed losing trades. The bootstrap cannot model "
            "an unseen reversal."
        )
        print(
            "rule-of-three upper bound on losing-boundary probability: "
            f"approximately {100 * upper_losing_boundary_rate:.1f}%"
        )

    print(
        f"boundary max drawdown ${max_drawdown:.2f} | "
        f"longest losing-boundary streak {losing_boundary_streak}"
    )

    positive = sorted(
        (trade["pnl"] for trade in trades if trade["pnl"] > 0),
        reverse=True,
    )
    if positive:
        positive_total = sum(positive)
        top_one = positive[0] / positive_total
        top_three = sum(positive[:3]) / positive_total
        print(
            f"positive-PnL concentration: top 1={100 * top_one:.1f}% | "
            f"top 3={100 * top_three:.1f}%"
        )

    print_bucket(trades, "asset", lambda trade: trade["asset"])
    print_bucket(trades, "execution source", lambda trade: trade["src"])

    if strategy == "h4":
        print_bucket(
            trades,
            "actual fill-price band",
            lambda trade: (
                "95-97"
                if trade["average_fill_mc"] < 970
                else "97-98"
                if trade["average_fill_mc"] < 980
                else "98-99"
                if trade["average_fill_mc"] < 990
                else "99-100"
            ),
            ["95-97", "97-98", "98-99", "99-100"],
        )
        print_bucket(
            trades,
            "signal favourite spread",
            lambda trade: (
                "<=10mc"
                if trade["signal_fav_spread_mc"] <= 10
                else "11-20mc"
            ),
            ["<=10mc", "11-20mc"],
        )
    else:
        print_bucket(
            trades,
            "actual underdog fill band",
            lambda trade: (
                "1-2c"
                if trade["average_fill_mc"] < 20
                else "2-3c"
                if trade["average_fill_mc"] < 30
                else "3-5c"
                if trade["average_fill_mc"] < 50
                else "5-7c"
                if trade["average_fill_mc"] < 70
                else "7-10c"
            ),
            ["1-2c", "2-3c", "3-5c", "5-7c", "7-10c"],
        )

        winners = sorted(
            (trade for trade in trades if trade["won"]),
            key=lambda trade: trade["pnl"],
            reverse=True,
        )
        if winners:
            print("\n--- winning H4b trades for manual audit ---")
            for trade in winners[:10]:
                print(
                    f"  {trade['slug']} end={trade['end_date'].isoformat()} "
                    f"signalFav={trade['signal_fav_bid_mc']}/"
                    f"{trade['signal_fav_ask_mc']}mc "
                    f"targetAsk={trade['signal_target_ask_mc']}mc "
                    f"avgFill={trade['average_fill_mc']:.1f}mc "
                    f"filled={trade['filled']:.4f} "
                    f"PnL=${trade['pnl']:+.2f}"
                )

    if rejections:
        print(
            "\nrejections:",
            dict(sorted(rejections.items(), key=lambda item: -item[1])),
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy", choices=["h4", "h4b"], default="h4")
    parser.add_argument("--horizon-sec", type=int, default=30)
    parser.add_argument("--all-horizons", action="store_true")
    parser.add_argument("--latency-sec", type=float, default=1.0)
    parser.add_argument("--notional", type=float, default=20.0)
    parser.add_argument("--fee-bps", type=float, default=0.0)
    parser.add_argument("--bbo-haircut", type=float, default=0.50)
    parser.add_argument(
        "--min-fill-ratio",
        type=float,
        default=0.0,
        help="Minimum fraction of requested notional that must fill (0 to 1).",
    )
    parser.add_argument("--max-slippage-mc", type=int, default=0)
    parser.add_argument("--start-time")
    parser.add_argument("--end-time")
    parser.add_argument("--bootstrap-iters", type=int, default=5000)
    parser.add_argument("--bootstrap-seed", type=int, default=42)
    args = parser.parse_args()

    if args.notional <= 0:
        parser.error("--notional must be positive")
    if not 0 <= args.bbo_haircut <= 1:
        parser.error("--bbo-haircut must be between 0 and 1")
    if not 0 <= args.min_fill_ratio <= 1:
        parser.error("--min-fill-ratio must be between 0 and 1")
    if args.latency_sec < 0:
        parser.error("--latency-sec cannot be negative")
    if args.fee_bps < 0:
        parser.error("--fee-bps cannot be negative")
    if args.max_slippage_mc < 0:
        parser.error("--max-slippage-mc cannot be negative")

    args.start_time = parse_timestamp(args.start_time)
    args.end_time = parse_timestamp(args.end_time)

    horizons = [60, 30, 15, 5] if args.all_horizons else [args.horizon_sec]
    strategies = ["h4", "h4b"] if args.all_horizons else [args.strategy]

    for strategy in strategies:
        for horizon in horizons:
            trades, rejections, market_count = run(strategy, horizon, args)
            report(
                strategy,
                horizon,
                trades,
                rejections,
                market_count,
                args,
            )

    print(
        "\nDISCIPLINE: this output is exploratory unless the rule and date window "
        "were frozen before collection. Validate any candidate on a strictly "
        "later forward cohort."
    )


if __name__ == "__main__":
    main()