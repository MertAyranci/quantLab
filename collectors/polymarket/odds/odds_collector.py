"""Odds collector — H2 bookmaker/exchange feed.

Polls The Odds API for MLB head-to-head (moneyline) odds, converts decimal odds
to OVERROUND-ADJUSTED implied probabilities (normalized across the two
outcomes per book), and writes to odds_games + odds_snapshots.

Budget: free tier = 500 requests/month (~16/day). ONE request pulls the whole
MLB slate, so poll thoughtfully — this script is meant for cron every ~30-60min
during the season, denser near game time if the paid tier is enabled. Each run
also prints remaining quota from the API response headers.

Exchanges (Betfair, Smarkets) are flagged is_exchange=true — they're the sharp
venues most likely to LEAD, and the point of H2.

Usage:
  .venv/bin/python collectors/odds/odds_collector.py            # one poll
  .venv/bin/python collectors/odds/odds_collector.py --dry-run  # fetch, don't write
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx
import psycopg2
from dotenv import dotenv_values

REPO = Path(__file__).resolve().parents[2]
ENV = dotenv_values(REPO / ".env")
API_KEY = ENV.get("ODDS_API_KEY", "")
SPORT = "baseball_mlb"
BASE = "https://api.the-odds-api.com/v4"
UA = {"User-Agent": "quant-lab-odds/0.1"}

# venues that are exchanges (near-zero overround, likely leaders)
EXCHANGES = {"betfair_ex_uk", "betfair_ex_eu", "betfair_ex_au",
             "smarkets", "matchbook"}

HEALTHCHECK_URL = ENV.get("ODDS_HEALTHCHECK_URL", "")


def iso(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except ValueError:
        return None


def fetch_odds(http: httpx.Client):
    r = http.get(f"{BASE}/sports/{SPORT}/odds/",
                 params={"apiKey": API_KEY, "regions": "us,uk,eu",
                         "markets": "h2h", "oddsFormat": "decimal"})
    remaining = r.headers.get("x-requests-remaining")
    used = r.headers.get("x-requests-used")
    r.raise_for_status()
    return r.json(), remaining, used


def normalize_game(game: dict):
    """Yield (bookmaker, is_exchange, outcome_team, is_home, decimal, raw_prob,
    implied_prob, book_last_update) rows for one game, overround-adjusted per
    book across the two outcomes."""
    home = game.get("home_team")
    away = game.get("away_team")
    for bk in game.get("bookmakers", []):
        key = bk.get("key")
        last_update = iso(bk.get("last_update"))
        # find the h2h market
        h2h = next((m for m in bk.get("markets", []) if m.get("key") == "h2h"), None)
        if not h2h:
            continue
        outs = h2h.get("outcomes", [])
        # need exactly two priced outcomes to normalize the overround
        priced = [(o.get("name"), float(o["price"]))
                  for o in outs if o.get("price")]
        if len(priced) != 2:
            continue
        raw = {name: 1.0 / price for name, price in priced}
        total = sum(raw.values())
        if total <= 0:
            continue
        for name, price in priced:
            yield (key, key in EXCHANGES, name, name == home,
                   price, raw[name], raw[name] / total, last_update)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not API_KEY:
        print("ERROR: ODDS_API_KEY not in .env")
        sys.exit(1)

    http = httpx.Client(headers=UA, timeout=30)
    games, remaining, used = fetch_odds(http)
    print(f"fetched {len(games)} MLB games | quota remaining={remaining} used={used}")

    if args.dry_run:
        for g in games[:2]:
            print(f"\n{g['away_team']} @ {g['home_team']} "
                  f"({g['commence_time']})")
            rows = list(normalize_game(g))
            for bk, exch, team, home, dec, raw, imp, upd in rows[:6]:
                tag = "EXCH" if exch else "book"
                print(f"  [{tag}] {bk:<14} {team:<24} dec={dec:<6} "
                      f"impl={imp:.3f}")
        print("\n[dry-run] nothing written")
        return

    conn = psycopg2.connect(host="127.0.0.1", port=5432, dbname="quantlab",
                            user="quantlab", password=ENV["PG_PASSWORD"])
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute("""INSERT INTO collector_runs (component, started_at, version_sha, notes)
                   VALUES ('odds_collector', now(), 'v0.1', %s) RETURNING id""",
                (f"games={len(games)}",))
    run_id = cur.fetchone()[0]

    n_games = n_snaps = 0
    for g in games:
        oddsapi_id = g.get("id")
        commence = iso(g.get("commence_time"))
        home, away = g.get("home_team"), g.get("away_team")
        # upsert the game
        cur.execute("""
            INSERT INTO odds_games (sport_key, oddsapi_id, commence_time,
                                    home_team, away_team)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (sport_key, oddsapi_id) DO UPDATE SET commence_time=EXCLUDED.commence_time
            RETURNING id""",
            (SPORT, oddsapi_id, commence, home, away))
        game_id = cur.fetchone()[0]
        n_games += 1
        for (bk, exch, team, is_home, dec, raw, imp, upd) in normalize_game(g):
            cur.execute("""
                INSERT INTO odds_snapshots (game_id, bookmaker, is_exchange,
                    outcome_team, is_home, decimal_odds, raw_prob, implied_prob,
                    book_last_update, capture_time, collector_run_id)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),%s)""",
                (game_id, bk, exch, team, is_home, dec, raw, imp, upd, run_id))
            n_snaps += 1

    conn.commit()
    print(f"wrote {n_games} games, {n_snaps} odds snapshots (run {run_id})")

    if HEALTHCHECK_URL:
        try:
            http.get(HEALTHCHECK_URL, timeout=10)
        except httpx.HTTPError:
            pass


if __name__ == "__main__":
    main()