"""Crypto harvester (Gamma-direct) — last-mile capture for 5-minute Up/Down markets.

The 5m crypto markets live ~5 minutes and daily discovery never catches them in
time. This job polls Gamma DIRECTLY every ~60s for live 'updown-5m' markets
(all assets: btc/eth/sol/xrp/doge/hype/bnb/zec/...), upserts any it hasn't seen
into markets+tokens, and writes their 'Up' (YES) tokens to a dedicated watchlist
the WS collector also reads. That yields dense last-mile books for crypto-H4.

clobTokenIds[0] = "Up" (YES), clobTokenIds[1] = "Down". endDate = resolution time.

Writes:
  config/watchlist_crypto.txt   (WS collector reads this + the H4 watchlist)
  crypto_watch table            (admission ledger for Grade-A selection)

Usage:
  .venv/bin/python db/crypto_harvester.py            # one pass
  .venv/bin/python db/crypto_harvester.py --dry-run  # show, don't write
"""

from __future__ import annotations

import argparse
import json
import re
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
SLUG_RE = re.compile(r"-updown-5m-\d+$")
POLY_VENUE_ID = 1          # Polymarket venue id in your venues table
MAX_TOKENS = 60            # safety cap on concurrent watched crypto tokens


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


def fetch_live_5m(http):
    """Live 5m markets from Gamma, newest first (freshly-created = about to run)."""
    r = http.get(f"{GAMMA}/markets",
                 params={"closed": "false", "order": "startDate",
                         "ascending": "false", "limit": 100})
    r.raise_for_status()
    out = []
    for m in r.json():
        slug = m.get("slug") or ""
        if not SLUG_RE.search(slug):
            continue
        toks = m.get("clobTokenIds")
        if isinstance(toks, str):
            toks = json.loads(toks)
        if not toks or len(toks) < 1:
            continue
        end = iso(m.get("endDate"))
        if end is not None:
            secs = (end - datetime.now(timezone.utc)).total_seconds()
            # only currently-live markets: resolving within the next 6 min,
            # or just resolved in the last 1 min (grace for capture)
            if secs < -60 or secs > 360:
                continue
        out.append({
            "slug": slug, "question": m.get("question"),
            "condition_id": m.get("conditionId"),
            "yes_token": str(toks[0]),
            "no_token": str(toks[1]) if len(toks) > 1 else None,
            "end_date": end,
            "venue_market_id": str(m.get("id") or m.get("conditionId") or slug),
        })
    return out


def upsert_market(cur, m):
    """Insert market + Up/Down tokens if unseen. Returns internal market id."""
    cur.execute("""
        INSERT INTO markets (venue_id, venue_market_id, condition_id, slug,
                             question, end_date, first_seen_at, source)
        VALUES (%s,%s,%s,%s,%s,%s, now(), 'crypto_harvester')
        ON CONFLICT (venue_id, venue_market_id) DO UPDATE SET slug=EXCLUDED.slug
        RETURNING id""",
        (POLY_VENUE_ID, m["venue_market_id"], m["condition_id"], m["slug"],
         m["question"], m["end_date"]))
    mid = cur.fetchone()[0]
    for idx, tok in ((0, m["yes_token"]), (1, m["no_token"])):
        if not tok:
            continue
        cur.execute("""
            INSERT INTO tokens (market_id, venue_id, venue_token_id, outcome_index)
            VALUES (%s,%s,%s,%s)
            ON CONFLICT (venue_id, venue_token_id) DO NOTHING""",
            (mid, POLY_VENUE_ID, tok, idx))
    return mid


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    http = httpx.Client(headers=UA, timeout=30)
    markets = fetch_live_5m(http)
    now = datetime.now(timezone.utc)
    print(f"{now.isoformat()} crypto-harvester: {len(markets)} live 5m markets")
    for m in markets[:8]:
        secs = (m["end_date"] - now).total_seconds() if m["end_date"] else None
        if secs is not None:
            print(f"  {m['slug']}  resolve in {secs:+.0f}s")
        else:
            print(f"  {m['slug']}")

    yes_tokens = [m["yes_token"] for m in markets][:MAX_TOKENS]

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