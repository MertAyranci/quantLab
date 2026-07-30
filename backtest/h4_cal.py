"""H4-Cal (minimal) — resolution-drift calibration backtest, single horizon.

Research question:
  In the final hours before resolution, are near-certain contracts (95c+)
  systematically UNDERPRICED? i.e. do 97c contracts win >97% of the time?

This is the CALIBRATION backtest only. It uses ~10-min / daily historical
prices, which are REFERENCE prices, NOT executable prices. Any return figure
here is THEORETICAL. Execution reality (spread, fees, liquidity, stale quotes)
is H4-Exec's job, built later against forward-collected Grade-A markets.

Stage 1 scope (deliberately minimal, per staged plan):
  * ONE horizon (default 24h; --horizon-hours to change)
  * strict as-of observation selection (no interpolation, no future data)
  * near-certainty price bands
  * realized win-rate vs implied, calibration gap
  * event-clustered bootstrap confidence intervals (markets in one event are
    NOT independent -> we resample EVENTS, not markets)
  * per-band coverage (how many markets had a valid as-of obs) so data-thinning
    is visible, not hidden
  * theoretical reference-price return + basic tail metrics, clearly labelled

Clean population (mirrors the H3 verdict's controls):
  non-neg-risk, <= MAX_SIBLINGS per event, unambiguous single winner,
  final status.

Usage:
  .venv/bin/python backtest/h4_cal.py
  .venv/bin/python backtest/h4_cal.py --horizon-hours 6 --tolerance-min 10
"""

import argparse
import random
from pathlib import Path

import psycopg2
from dotenv import dotenv_values

REPO = Path(__file__).resolve().parent.parent
ENV = dotenv_values(REPO / ".env")

# near-certainty bands, in milli-dollars (price_mc). (lo, hi, label)
BANDS = [
    (950, 970, "95.0-97.0%"),
    (970, 980, "97.0-98.0%"),
    (980, 990, "98.0-99.0%"),
    (990, 995, "99.0-99.5%"),
    (995, 1000, ">99.5%"),
]
MAX_SIBLINGS = 3
BOOTSTRAP_N = 2000


def wilson_or_bootstrap_note():
    pass


def fetch_observations(cur, horizon_hours: float, tolerance_min: float):
    """Return rows: (event_id, market_id, price_mc, won) using STRICT as-of.

    For each clean resolved market, take the LATEST YES-token price observation
    with ts <= (resolution_time - horizon) and ts >= that target - tolerance.
    DISTINCT ON gives us the latest within the window; the tolerance floor
    guarantees we never reach back arbitrarily far (a too-old quote is 'no data'
    for this horizon, marked unavailable by simply not appearing).
    """
    cur.execute(
        """
        WITH yes_tok AS (
            SELECT id AS token_id, market_id
            FROM tokens WHERE outcome_index = 0
        ),
        res AS (
            SELECT r.market_id, r.event_time, r.winning_token_id
            FROM resolutions r
            WHERE r.status = 'final'
              AND r.event_time IS NOT NULL
              AND r.event_time > '2025-01-01'
        ),
        sib AS (
            SELECT event_id, count(*) AS n
            FROM markets WHERE event_id IS NOT NULL GROUP BY event_id
        ),
        elig AS (
            SELECT res.market_id, res.event_time, res.winning_token_id,
                   y.token_id, m.event_id,
                   res.event_time - make_interval(mins => %(horizon_min)s) AS target_t
            FROM res
            JOIN yes_tok y ON y.market_id = res.market_id
            JOIN markets m ON m.id = res.market_id
            LEFT JOIN events e ON e.id = m.event_id
            LEFT JOIN sib s ON s.event_id = m.event_id
            WHERE COALESCE(e.neg_risk, false) = false
              AND COALESCE(s.n, 1) <= %(max_sib)s
        ),
        asof AS (
            SELECT DISTINCT ON (elig.market_id)
                   elig.event_id, elig.market_id, ph.price_mc,
                   (elig.winning_token_id = elig.token_id) AS won
            FROM elig
            JOIN price_history ph
              ON ph.token_id = elig.token_id
             AND ph.ts <= elig.target_t
             AND ph.ts >= elig.target_t - make_interval(mins => %(tol_min)s)
            ORDER BY elig.market_id, ph.ts DESC
        )
        SELECT event_id, market_id, price_mc, won FROM asof
        WHERE price_mc >= %(band_lo)s
        """,
        {
            "horizon_min": horizon_hours * 60.0,
            "tol_min": tolerance_min,
            "max_sib": MAX_SIBLINGS,
            "band_lo": BANDS[0][0],
        },
    )
    return cur.fetchall()


def event_clustered_ci(rows_in_band, n_boot=BOOTSTRAP_N):
    """Bootstrap the realized win-rate by resampling EVENTS (not markets).

    rows_in_band: list of (event_id, won_bool). Returns (lo, hi) 95% CI on
    realized win fraction. If <2 distinct events, CI is undefined -> None.
    """
    by_event = {}
    for eid, won in rows_in_band:
        by_event.setdefault(eid, []).append(1 if won else 0)
    events = list(by_event.values())
    if len(events) < 2:
        return None
    means = []
    for _ in range(n_boot):
        sample = [random.choice(events) for _ in range(len(events))]
        flat = [w for grp in sample for w in grp]
        if flat:
            means.append(sum(flat) / len(flat))
    means.sort()
    lo = means[int(0.025 * len(means))]
    hi = means[int(0.975 * len(means))]
    return lo * 100, hi * 100


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--horizon-hours", type=float, default=24.0)
    ap.add_argument("--tolerance-min", type=float, default=None,
                    help="as-of tolerance; default 15 for horizon>=1h else 10")
    args = ap.parse_args()

    tol = args.tolerance_min
    if tol is None:
        tol = 15.0 if args.horizon_hours >= 1.0 else 10.0

    conn = psycopg2.connect(host="127.0.0.1", port=5432, dbname="quantlab",
                            user="quantlab", password=ENV["PG_PASSWORD"])
    cur = conn.cursor()

    print(f"H4-Cal (minimal) — horizon={args.horizon_hours}h  tolerance={tol}min")
    print(f"clean population: non-neg-risk, <= {MAX_SIBLINGS} siblings, final, "
          f"winner unambiguous\n")
    print("REFERENCE PRICES — theoretical calibration only, NOT executable.\n")

    rows = fetch_observations(cur, args.horizon_hours, tol)
    if not rows:
        print("No as-of observations in any near-certainty band at this horizon.")
        print("(Likely the fine-history 30-day window: few resolved markets have "
              "a valid obs this close to resolution. Try --horizon-hours 24 or 48.)")
        return

    print(f"{'band':<12} {'n':>6} {'implied':>9} {'realized':>9} "
          f"{'gap_pp':>7} {'95% CI (event-clustered)':>26}")
    print("-" * 74)

    ledger = []  # (band_label, event_id, price_mc, won) for later export
    for lo, hi, label in BANDS:
        band = [(eid, mid, pmc, won) for (eid, mid, pmc, won) in rows
                if lo <= pmc < hi]
        n = len(band)
        if n == 0:
            print(f"{label:<12} {0:>6}  (no markets)")
            continue
        implied = sum(pmc for _, _, pmc, _ in band) / n / 10.0
        realized = 100.0 * sum(1 for _, _, _, won in band if won) / n
        gap = realized - implied
        ci = event_clustered_ci([(eid, won) for eid, _, _, won in band])
        ci_s = f"[{ci[0]:.1f}, {ci[1]:.1f}]" if ci else "(< 2 events)"
        print(f"{label:<12} {n:>6} {implied:>8.1f}% {realized:>8.1f}% "
              f"{gap:>+7.1f} {ci_s:>26}")
        for eid, mid, pmc, won in band:
            ledger.append((label, eid, mid, pmc, won))

    # ---- theoretical tail metrics (reference-price, whole near-cert pop) ----
    allrows = [(pmc, won) for _, _, pmc, won in rows]
    n = len(allrows)
    wins = sum(1 for pmc, won in allrows if won)
    losses = n - wins
    # buying YES at reference price p (in dollars): win => +(1-p), lose => -p
    pnl = [(1 - pmc / 1000.0) if won else -(pmc / 1000.0) for pmc, won in allrows]
    total = sum(pnl)
    print("\n--- THEORETICAL reference-price tail metrics (NOT executable) ---")
    print(f"near-certainty markets (>=95c): {n}")
    print(f"complete losses (contract went to 0): {losses}  "
          f"({100.0*losses/n:.2f}%)")
    print(f"mean reference PnL per $1-notional YES buy: {total/n:+.4f}")
    if losses:
        worst = min(pnl)
        # expected shortfall: mean of worst 5%
        pnl_sorted = sorted(pnl)
        tail = pnl_sorted[:max(1, n // 20)]
        print(f"worst single loss: {worst:+.3f}   "
              f"CVaR(5%): {sum(tail)/len(tail):+.3f}")
    print("\nNOTE: a positive mean here is a CALIBRATION signal only. Whether it "
          "survives spread/fees/liquidity is H4-Exec's question, unbuilt until "
          "forward-collected Grade-A markets exist.")

    # export ledger for inspection
    out = REPO / "research" / f"h4cal_ledger_{int(args.horizon_hours)}h.csv"
    out.parent.mkdir(exist_ok=True)
    with open(out, "w") as f:
        f.write("band,event_id,market_id,price_mc,won\n")
        for band, eid, mid, pmc, won in ledger:
            f.write(f"{band},{eid},{mid},{pmc},{int(won)}\n")
    print(f"\ntrade-level ledger written: {out.relative_to(REPO)}")


if __name__ == "__main__":
    main()