"""Event matcher — link Odds API games to Polymarket moneyline markets.

The linchpin of H2. For each odds_games row, find the Polymarket market that is
the same matchup on the same date, and set polymarket_market_id.

Polymarket moneyline markets are phrased 'Team A vs. Team B' (with a period after
vs, no colon, no spread/innings/O-U qualifiers). The Odds API gives home_team /
away_team as full names ('New York Yankees'). Both use the same canonical team
names for MLB, so matching is: same team SET (order-independent) + same game date.

Confidence levels:
  exact  — both teams matched on canonical names AND dates within 1 day
  fuzzy  — teams matched after light normalization, dates within 1 day
  (manual is reserved for hand-entered links)

Usage:
  .venv/bin/python collectors/odds/event_matcher.py            # match unlinked games
  .venv/bin/python collectors/odds/event_matcher.py --dry-run  # show, don't write
  .venv/bin/python collectors/odds/event_matcher.py --report   # matched/unmatched summary
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import psycopg2
from dotenv import dotenv_values

REPO = Path(__file__).resolve().parents[2]
ENV = dotenv_values(REPO / ".env")

# light normalization: lowercase, strip punctuation, collapse spaces.
# MLB names align across feeds, but this guards against 'St.Louis' vs 'St Louis'
# and 'Athletics' vs 'Oakland Athletics' style drift.
CITY_ALIASES = {
    "oakland athletics": "athletics",
    "st louis cardinals": "st. louis cardinals",
}


def norm_team(name: str) -> str:
    n = (name or "").strip().lower()
    n = n.replace(".", "").replace("-", " ")
    n = re.sub(r"\s+", " ", n).strip()
    return CITY_ALIASES.get(n, n)


def parse_pm_matchup(question: str):
    """Return (team_a_norm, team_b_norm) from 'Team A vs. Team B', else None."""
    m = re.match(r"^\s*(.+?)\s+vs\.?\s+(.+?)\s*$", question)
    if not m:
        return None
    return norm_team(m.group(1)), norm_team(m.group(2))


def connect():
    return psycopg2.connect(host="127.0.0.1", port=5432, dbname="quantlab",
                            user="quantlab", password=ENV["PG_PASSWORD"])


def candidate_pm_markets(cur, commence_time):
    """Polymarket moneyline markets whose slug-encoded date matches the game
    date (slugs look like 'mlb-stl-nyy-2026-08-05'). Date from slug, since
    end_date is null for per-game markets."""
    game_date = commence_time.date().isoformat()   # 'YYYY-MM-DD'
    cur.execute("""
        SELECT id, question, slug
        FROM markets
        WHERE question ~ '^[A-Za-z. ]+ vs\\. [A-Za-z. ]+$'
          AND question NOT ILIKE '%%:%%'
          AND question NOT ILIKE '%%spread%%'
          AND question NOT ILIKE '%%innings%%'
          AND question NOT ILIKE '%%O/U%%'
          AND slug LIKE %s
    """, (f"%{game_date}",))
    return cur.fetchall()


def match_all(cur, dry_run=False):
    cur.execute("""
        SELECT id, oddsapi_id, commence_time, home_team, away_team
        FROM odds_games
        WHERE polymarket_market_id IS NULL
        ORDER BY commence_time""")
    games = cur.fetchall()
    matched = unmatched = 0
    results = []
    for game_id, oddsapi_id, commence, home, away in games:
        want = {norm_team(home), norm_team(away)}
        found = None
        for pm_id, question, slug in candidate_pm_markets(cur, commence):
            parsed = parse_pm_matchup(question)
            if not parsed:
                continue
            if set(parsed) == want:
                found = (pm_id, question, slug)
                break
        if found:
            matched += 1
            results.append((game_id, home, away, found[0], found[1]))
            if not dry_run:
                cur.execute("""UPDATE odds_games
                               SET polymarket_market_id=%s, match_confidence='exact'
                               WHERE id=%s""", (found[0], game_id))
        else:
            unmatched += 1
            results.append((game_id, home, away, None, None))
    return matched, unmatched, results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--report", action="store_true")
    args = ap.parse_args()

    conn = connect()
    cur = conn.cursor()

    if args.report:
        cur.execute("""SELECT match_confidence, count(*) FROM odds_games
                       GROUP BY 1 ORDER BY 1""")
        print("odds_games by match status:")
        for conf, n in cur.fetchall():
            print(f"  {conf or 'UNMATCHED':<12} {n}")
        return

    matched, unmatched, results = match_all(cur, dry_run=args.dry_run)
    for game_id, home, away, pm_id, q in results:
        if pm_id:
            print(f"  MATCH  {away} @ {home}  ->  [{pm_id}] {q}")
        else:
            print(f"  ----   {away} @ {home}  ->  (no Polymarket moneyline found)")
    if not args.dry_run:
        conn.commit()
    print(f"\n{matched} matched, {unmatched} unmatched"
          f"{'  [dry-run, nothing written]' if args.dry_run else ''}")


if __name__ == "__main__":
    main()