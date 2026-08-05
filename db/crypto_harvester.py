"""Crypto harvester (compute-and-fetch) — last-mile capture for 5-minute markets.

The 5m crypto markets are clockwork: they resolve on 5-minute boundaries and
their slug encodes the resolution Unix timestamp (e.g. btc-updown-5m-1785891000).
So we don't SEARCH Gamma's firehose (which mixes stale + future + all timeframes);
we COMPUTE the live slugs from the clock and fetch them directly. Deterministic
and reliable.

Each pass: for each asset, for the current + next 5-min boundary, build the slug,
fetch it from Gamma, upsert it, and watchlist its 'Up' (YES) token so the WS
collector captures its last-mile book.

clobTokenIds[0] = "Up" (YES), [1] = "Down". endDate = resolution time.

Usage:
  .venv/bin/python db/crypto_harvester.py            # one pass
  .venv/bin/python db/crypto_harvester.py --dry-run  # show, don't write
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
import psycopg2
from dotenv import dotenv_values

REPO = Path(__file__).resolve().parents[1]
ENV = dotenv_values(REPO / ".env")
WATCHLIST = REPO / "config" / "watchlist_crypto.txt"

GAMMA = "https://gamma-api.polymarket.com"
UA = {"User-Agent": "quant-lab-crypto/0.1"}
POLY_VENUE_ID = 1

# assets seen on the 5m board; harmless if some don't exist for a given boundary
ASSETS = ["btc", "eth", "sol", "xrp", "doge", "hype", "bnb", "zec"]
BOUNDARIES_AHEAD = 2       # current + next 5-min boundary (covers the live + on-deck)


def connect():
    return psycopg2.connect(host="127.0.0.1", port=5432, dbname="quantlab",
                            user="quantlab", password=ENV["PG_PASSWORD"])


def iso(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except ValueError:
        return None


def live_slugs():
    """Compute the slugs for markets resolving on the upcoming 5-min boundaries."""
    now = int(time.time())
    base = (now // 300) * 300
    slugs = []
    for i in range(0, BOUNDARIES_AHEAD + 1):
        ts = base + i * 300
        if ts < now - 60:          # skip boundaries already well past
            continue
        for a in ASSETS:
            slugs.append(f"{a}-updown-5m-{ts}")
    return slugs


def fetch_by_slug(http, slug):
    r = http.get(f"{GAMMA}/markets", params={"slug": slug})
    if r.status_code != 200:
        return None
    data = r.json()
    if not data:
        return None
    m = data[0]
    toks = m.get("clobTokenIds")
    if isinstance(toks, str):
        toks = json.loads(toks)
    if not toks:
        return None
    return {
        "slug": slug, "question": m.get("question"),
        "condition_id": m.get("conditionId"),
        "yes_token": str(toks[0]),
        "no_token": str(toks[1]) if len(toks) > 1 else None,
        "end_date": iso(m.get("endDate")),
        "venue_market_id": str(m.get("id") or m.get("conditionId") or slug),
    }


def fetch_live_5m(http):
    out = []
    for slug in live_slugs():
        m = fetch_by_slug(http, slug)
        if m:
            out.append(m)
    return out


def upsert_market(cur, m):
    cur.execute("""
        INSERT INTO markets (venue_id, venue_market_id, condition_id, slug,
                             question, end_date, first_seen_at, source)
        VALUES (%s,%s,%s,%s,%s,%s, now(), 'crypto_harvester')
        ON CONFLICT (venue_id, venue_market_id) DO UPDATE SET slug=EXCLUDED.slug
        RETURNING id""",
        (POLY_VENUE_ID, m["venue_market_id"], m["condition_id"], m["slug"],
         m["question"], m["end_date"]))
    mid = cur.fetchone()[0]
    for idx, tok, outcome in ((0, m["yes_token"], "Up"), (1, m["no_token"], "Down")):
        if not tok:
            continue
        cur.execute("""
            INSERT INTO tokens (market_id, venue_id, venue_token_id, outcome_index,
                                outcome, source)
            VALUES (%s,%s,%s,%s,%s,'crypto_harvester')
            ON CONFLICT (venue_id, venue_token_id) DO NOTHING""",
            (mid, POLY_VENUE_ID, tok, idx, outcome))
    return mid


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    http = httpx.Client(headers=UA, timeout=30)
    markets = fetch_live_5m(http)
    now = datetime.now(timezone.utc)
    print(f"{now.isoformat()} crypto-harvester: {len(markets)} live 5m markets")
    for m in markets[:12]:
        secs = (m["end_date"] - now).total_seconds() if m["end_date"] else None
        if secs is not None:
            print(f"  {m['slug']}  resolve in {secs:+.0f}s")
        else:
            print(f"  {m['slug']}")

    yes_tokens = [m["yes_token"] for m in markets]

    if args.dry_run:
        print(f"[dry-run] would write {len(yes_tokens)} YES tokens to {WATCHLIST.name}")
        return

    conn = connect()
    cur = conn.cursor()
    for m in markets:
        mid = upsert_market(cur, m)
        cur.execute("""
            INSERT INTO crypto_watch (market_id, slug, resolve_time, admitted_at)
            VALUES (%s,%s,%s, now())
            ON CONFLICT (market_id) DO NOTHING""",
            (mid, m["slug"], m["end_date"]))
    conn.commit()

    WATCHLIST.parent.mkdir(exist_ok=True)
    tmp = WATCHLIST.with_suffix(".tmp")
    tmp.write_text("\n".join(yes_tokens) + ("\n" if yes_tokens else ""))
    tmp.replace(WATCHLIST)
    print(f"wrote {len(yes_tokens)} tokens to {WATCHLIST.name}, "
          f"upserted {len(markets)} markets")


if __name__ == "__main__":
    main()