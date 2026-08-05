"""Crypto harvester — last-mile capture for 5-minute Up/Down markets.

The 5-minute BTC/ETH markets live only ~5 minutes, so the slow H4 harvester and
daily discovery never capture their order books. This job runs every 1-2 minutes,
finds 5m crypto markets resolving in the next ~6 minutes, and writes their YES
tokens to a dedicated watchlist file the WS collector reads. That gives us dense
last-mile books (the 95->99c climb) needed to test crypto-H4.

Writes to config/watchlist_crypto.txt (separate from the H4 watchlist so the two
don't clobber each other). The WS collector must be configured to read BOTH files.

Design:
  * admit markets whose resolve_time is in [now-1min, now+6min] -> covers a
    market's full 5-min life plus a small grace window
  * one YES token per market (outcome_index 0)
  * cap the list so we never over-subscribe (5m markets are frequent)
  * records admissions to a crypto_watch table for later Grade-A selection

Usage:
  .venv/bin/python db/crypto_harvester.py            # one pass, write watchlist
  .venv/bin/python db/crypto_harvester.py --dry-run  # show, don't write
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from dotenv import dotenv_values

REPO = Path(__file__).resolve().parents[1]
ENV = dotenv_values(REPO / ".env")
WATCHLIST = REPO / "config" / "watchlist_crypto.txt"

WINDOW_PAST_MIN = 1        # keep markets up to 1 min past resolve (grace)
WINDOW_FUTURE_MIN = 6      # admit markets resolving within 6 min
MAX_MARKETS = 40           # cap concurrent watched 5m markets


def connect():
    return psycopg2.connect(host="127.0.0.1", port=5432, dbname="quantlab",
                            user="quantlab", password=ENV["PG_PASSWORD"])


def upcoming_5m_markets(cur):
    """5-minute crypto markets resolving within the capture window, with their
    YES venue token id for the watchlist."""
    cur.execute("""
        WITH c AS (
          SELECT id, slug,
                 to_timestamp(substring(slug from 'updown-5m-([0-9]+)')::bigint) AS resolve_time
          FROM markets
          WHERE slug ~ '(btc|eth)-updown-5m-[0-9]+'
        )
        SELECT c.id, c.slug, c.resolve_time, tk.venue_token_id
        FROM c
        JOIN tokens tk ON tk.market_id = c.id AND tk.outcome_index = 0
        WHERE c.resolve_time BETWEEN now() - (%s * interval '1 min')
                                 AND now() + (%s * interval '1 min')
          AND tk.venue_token_id IS NOT NULL
        ORDER BY c.resolve_time
        LIMIT %s
    """, (WINDOW_PAST_MIN, WINDOW_FUTURE_MIN, MAX_MARKETS))
    return cur.fetchall()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = connect()
    cur = conn.cursor()
    rows = upcoming_5m_markets(cur)

    tokens = [r[3] for r in rows]
    now = datetime.now(timezone.utc)
    print(f"{now.isoformat()} crypto-harvester: {len(rows)} live 5m markets")
    for mid, slug, rt, tok in rows[:8]:
        secs = (rt - now).total_seconds()
        print(f"  {slug}  resolve in {secs:+.0f}s")

    if args.dry_run:
        print(f"[dry-run] would write {len(tokens)} tokens to {WATCHLIST.name}")
        return

    # record admissions (idempotent) for later Grade-A selection
    for mid, slug, rt, tok in rows:
        cur.execute("""
            INSERT INTO crypto_watch (market_id, slug, resolve_time, admitted_at)
            VALUES (%s,%s,%s, now())
            ON CONFLICT (market_id) DO NOTHING""", (mid, slug, rt))
    conn.commit()

    # atomic watchlist write
    WATCHLIST.parent.mkdir(exist_ok=True)
    tmp = WATCHLIST.with_suffix(".tmp")
    tmp.write_text("\n".join(tokens) + ("\n" if tokens else ""))
    tmp.replace(WATCHLIST)
    print(f"wrote {len(tokens)} tokens to {WATCHLIST.name}")


if __name__ == "__main__":
    main()