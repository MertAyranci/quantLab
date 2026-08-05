"""Crypto harvester (compute-and-fetch, union-retention) — last-mile capture for
5-minute Up/Down markets.

The 5m crypto markets are clockwork: they resolve on 5-minute boundaries and the
slug encodes the resolution Unix timestamp. We COMPUTE the live slugs from the
clock and fetch them directly from Gamma (no firehose search).

CRITICAL FIX (union retention): the watchlist is the UNION of every cohort whose
end_date is in [now - GRACE, now + LOOKAHEAD]. A cohort is retained until
end_date + GRACE so it is NEVER unsubscribed before it resolves (the earlier bug:
newer cohorts pushed resolving ones off the list ~60-235s before resolution,
destroying exactly the last-mile data H4 needs).

BOTH tokens (Up outcome_index 0 AND Down outcome_index 1) are watchlisted, since
near-certainty appears on either side (a near-certain 'Down' shows as Up~5c).

FINAL CHECKPOINT: once a cohort crosses end_date, the harvester issues a direct
REST book fetch for both tokens and records a final snapshot, guaranteeing the
T-0 state regardless of collector timing, before the cohort ages out.

Writes:
  config/watchlist_crypto.txt   (WS collector reads this + the H4 watchlist)
  crypto_watch table            (admission ledger)
  tob_snapshots                 (final checkpoint rows)

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
CLOB = "https://clob.polymarket.com"
UA = {"User-Agent": "quant-lab-crypto/0.2"}
POLY_VENUE_ID = 1

ASSETS = ["btc", "eth", "sol", "xrp", "doge", "hype", "bnb", "zec"]

# union-retention window (seconds relative to a cohort's resolution boundary)
GRACE_S = 15          # keep a cohort this long AFTER its end_date
LOOKAHEAD_S = 11 * 60 # subscribe cohorts resolving up to ~11 min ahead
FINAL_CHECKPOINT_WINDOW_S = 20   # if a cohort ended within this window, REST-snapshot it


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


def retained_boundaries():
    """All 5-min boundaries whose resolution time is in
    [now - GRACE, now + LOOKAHEAD] -> union retention, nothing dropped early."""
    now = int(time.time())
    base = (now // 300) * 300
    bounds = []
    b = base
    # walk back a little (to keep just-resolved cohorts in grace) and forward
    while b >= now - GRACE_S - 300:
        b -= 300
    b += 300
    while b <= now + LOOKAHEAD_S:
        if b >= now - GRACE_S - 300:      # within grace of already-passed, or future
            # keep boundaries whose end (=b) is >= now-GRACE
            if b >= now - GRACE_S:
                bounds.append(b)
        b += 300
    return sorted(set(bounds))


def slugs_for_boundaries(bounds):
    return [f"{a}-updown-5m-{ts}" for ts in bounds for a in ASSETS]


def fetch_by_slug(http, slug):
    try:
        r = http.get(f"{GAMMA}/markets", params={"slug": slug})
    except httpx.HTTPError:
        return None
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
    for slug in slugs_for_boundaries(retained_boundaries()):
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
    tok_ids = {}
    for idx, tok, outcome in ((0, m["yes_token"], "Up"), (1, m["no_token"], "Down")):
        if not tok:
            continue
        cur.execute("""
            INSERT INTO tokens (market_id, venue_id, venue_token_id, outcome_index,
                                outcome, source)
            VALUES (%s,%s,%s,%s,%s,'crypto_harvester')
            ON CONFLICT (venue_id, venue_token_id) DO NOTHING
            RETURNING id""",
            (mid, POLY_VENUE_ID, tok, idx, outcome))
        row = cur.fetchone()
        if row:
            tok_ids[tok] = row[0]
        else:
            cur.execute("SELECT id FROM tokens WHERE venue_id=%s AND venue_token_id=%s",
                        (POLY_VENUE_ID, tok))
            r = cur.fetchone()
            if r:
                tok_ids[tok] = r[0]
    return mid, tok_ids


def final_checkpoint(http, cur, m, tok_ids):
    """If this market just crossed end_date, REST-fetch both books and record a
    final tob_snapshot so the T-0 state is never lost."""
    end = m["end_date"]
    if end is None:
        return 0
    secs_since_end = (datetime.now(timezone.utc) - end).total_seconds()
    if not (0 <= secs_since_end <= FINAL_CHECKPOINT_WINDOW_S):
        return 0
    written = 0
    for venue_tok, internal_tok in tok_ids.items():
        try:
            r = http.get(f"{CLOB}/book", params={"token_id": venue_tok})
            if r.status_code != 200:
                continue
            b = r.json()
        except (httpx.HTTPError, json.JSONDecodeError):
            continue
        bids = b.get("bids") or []
        asks = b.get("asks") or []
        best_bid = max((float(x["price"]) for x in bids), default=None)
        best_ask = min((float(x["price"]) for x in asks), default=None)
        bb_sz = next((float(x["size"]) for x in bids
                      if float(x["price"]) == best_bid), None) if best_bid else None
        ba_sz = next((float(x["size"]) for x in asks
                      if float(x["price"]) == best_ask), None) if best_ask else None
        cur.execute("""
            INSERT INTO tob_snapshots (token_id, best_bid_mc, best_bid_size,
                best_ask_mc, best_ask_size, capture_time, source)
            VALUES (%s,%s,%s,%s,%s, now(), 'crypto_final_checkpoint')""",
            (internal_tok,
             int(best_bid*1000) if best_bid is not None else None, bb_sz,
             int(best_ask*1000) if best_ask is not None else None, ba_sz))
        written += 1
    return written


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    http = httpx.Client(headers=UA, timeout=30)
    markets = fetch_live_5m(http)
    now = datetime.now(timezone.utc)
    print(f"{now.isoformat()} crypto-harvester: {len(markets)} markets in retention window")

    # both tokens for every retained market -> the watchlist
    watch_tokens = []
    for m in markets:
        watch_tokens.append(m["yes_token"])
        if m["no_token"]:
            watch_tokens.append(m["no_token"])
    watch_tokens = list(dict.fromkeys(watch_tokens))   # dedupe

    # show a few with countdowns and which are in final-checkpoint window
    for m in sorted(markets, key=lambda x: x["end_date"] or now)[:10]:
        secs = (m["end_date"] - now).total_seconds() if m["end_date"] else None
        tag = ""
        if secs is not None and -FINAL_CHECKPOINT_WINDOW_S <= secs <= 0:
            tag = "  <-- FINAL CHECKPOINT"
        print(f"  {m['slug']}  resolve in {secs:+.0f}s{tag}" if secs is not None
              else f"  {m['slug']}")

    if args.dry_run:
        print(f"[dry-run] would write {len(watch_tokens)} tokens "
              f"(both sides) for {len(markets)} markets to {WATCHLIST.name}")
        return

    conn = connect()
    cur = conn.cursor()
    checkpoints = 0
    for m in markets:
        mid, tok_ids = upsert_market(cur, m)
        cur.execute("""
            INSERT INTO crypto_watch (market_id, slug, resolve_time, admitted_at)
            VALUES (%s,%s,%s, now())
            ON CONFLICT (market_id) DO NOTHING""",
            (mid, m["slug"], m["end_date"]))
        checkpoints += final_checkpoint(http, cur, m, tok_ids)
    conn.commit()

    WATCHLIST.parent.mkdir(exist_ok=True)
    tmp = WATCHLIST.with_suffix(".tmp")
    tmp.write_text("\n".join(watch_tokens) + ("\n" if watch_tokens else ""))
    tmp.replace(WATCHLIST)
    print(f"wrote {len(watch_tokens)} tokens (both sides) for {len(markets)} "
          f"markets; {checkpoints} final checkpoints recorded")


if __name__ == "__main__":
    main()