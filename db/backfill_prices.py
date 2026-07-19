"""Backfill — resolved-market universe: resolutions + price history to birth.

Usage:
    .venv/bin/python db/backfill_prices.py --limit 50        # sample
    .venv/bin/python db/backfill_prices.py                    # full (run in tmux)
    .venv/bin/python db/backfill_prices.py --skip-fine        # daily pass only

Design:
  * keyset walk over Gamma closed=true markets (oldest-first, full archive)
  * reuses Backloader's upsert machinery + caches (subclass)
  * resolution rule (defensive): outcomePrices has EXACTLY ONE "1" -> write
    resolutions row (status='final', learned_via='gamma_backfill',
    event_time=closedTime). Anything else (["0","0"], missing) -> market
    archived + status row only; counted as 'unresolvable', never fabricated.
  * price harvest: YES token (outcome_index 0) daily-to-birth (fidelity=1440);
    markets closed within FINE_WINDOW_DAYS also get fidelity=10 (the ~30-day
    fine-history window measured empirically 2026-07-19).
  * pacing: 0.3s between Gamma pages, 0.12s between prices-history calls
    (~8 rps, far under the 100 rps budget; polite by policy)
  * resumability: parsed_files receipt 'backfill:market:<venue_market_id>'
    written after each market completes; commit per Gamma page.
"""

import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import psycopg2
import psycopg2.extras
from dotenv import dotenv_values

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backloader import Backloader, VENUE_ID, git_sha, iso, jloads_maybe, log, mc  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
UA = {"User-Agent": "quant-lab-day1/0.1"}
PAGE_SLEEP = 0.3
PRICE_SLEEP = 0.12
FINE_WINDOW_DAYS = 25


class BackfillLoader(Backloader):

    def __init__(self, conn, run_id, client: httpx.Client, skip_fine: bool):
        super().__init__(conn, run_id)
        self.client = client
        self.skip_fine = skip_fine
        self.stats = {"markets_seen": 0, "resolved": 0, "unresolvable": 0,
                      "no_tokens": 0, "price_points": 0, "fine_points": 0,
                      "skipped_done": 0}

    # ---- resolution parsing -------------------------------------------------

    def parse_resolution(self, m: dict):
        """Return (winning_index, closed_time) or (None, closed_time)."""
        prices = jloads_maybe(m.get("outcomePrices")) or []
        winners = [i for i, p in enumerate(prices) if str(p) == "1"]
        ct = iso(str(m.get("closedTime")).replace(" ", "T")) if m.get("closedTime") else None
        return (winners[0] if len(winners) == 1 else None), ct

    # ---- price harvest ------------------------------------------------------

    def fetch_history(self, venue_token_id: str, fidelity: int) -> list:
        for attempt in (1, 2, 3):
            try:
                r = self.client.get(f"{CLOB}/prices-history",
                                    params={"market": venue_token_id,
                                            "interval": "max",
                                            "fidelity": str(fidelity)},
                                    timeout=30)
                if r.status_code == 404:
                    return []
                r.raise_for_status()
                return r.json().get("history", [])
            except (httpx.HTTPError, json.JSONDecodeError) as e:
                log.warning("prices-history attempt %d failed (%s): %s",
                            attempt, venue_token_id[:16], e)
                time.sleep(2 ** attempt)
        return []

    def store_history(self, token_internal_id: int, hist: list,
                      fidelity: int, raw_ref: str) -> int:
        if not hist:
            return 0
        now = datetime.now(timezone.utc)
        rows = [(token_internal_id,
                 datetime.fromtimestamp(p["t"], timezone.utc),
                 mc(p["p"]), fidelity, now, "prices_history", raw_ref, self.run_id)
                for p in hist
                if p.get("p") is not None and 0 <= mc(p["p"]) <= 1000]
        psycopg2.extras.execute_values(self.cur,
            """INSERT INTO price_history (token_id, ts, price_mc, fidelity_min,
               backfilled_at, source, raw_ref, collector_run_id) VALUES %s
               ON CONFLICT (token_id, ts, fidelity_min) DO NOTHING""", rows)
        return len(rows)

    # ---- per-market ---------------------------------------------------------

    def process_market(self, m: dict):
        vmid = str(m.get("id"))
        key = f"backfill:market:{vmid}"
        if key in self.parsed:
            self.stats["skipped_done"] += 1
            return
        self.stats["markets_seen"] += 1
        cap = datetime.now(timezone.utc)
        raw_ref = f"gamma:markets/keyset:closed:{vmid}"

        mid = self.upsert_market(m, cap, raw_ref, source="gamma_backfill")
        tids = self.upsert_tokens(mid, m, raw_ref)
        win_idx, closed_time = self.parse_resolution(m)

        # lifecycle: closed (+ resolved if winner known)
        obs = closed_time or cap
        self.cur.execute(
            """INSERT INTO market_status (market_id, observed_at, status, source,
               raw_ref, collector_run_id) VALUES (%s,%s,'closed','gamma_backfill',%s,%s)
               ON CONFLICT DO NOTHING""", (mid, obs, raw_ref, self.run_id))

        if win_idx is not None and win_idx < len(tids):
            self.cur.execute(
                """INSERT INTO resolutions (market_id, event_time, capture_time,
                   winning_token_id, winning_outcome, learned_via, status,
                   raw_ref, collector_run_id)
                   VALUES (%s,%s,%s,%s,%s,'gamma_backfill','final',%s,%s)""",
                (mid, closed_time, cap, tids[win_idx],
                 (jloads_maybe(m.get("outcomes")) or [None]*(win_idx+1))[win_idx],
                 raw_ref, self.run_id))
            self.cur.execute(
                """INSERT INTO market_status (market_id, observed_at, status, source,
                   raw_ref, collector_run_id) VALUES (%s,%s,'resolved','gamma_backfill',%s,%s)
                   ON CONFLICT DO NOTHING""", (mid, obs, raw_ref, self.run_id))
            self.stats["resolved"] += 1
        else:
            self.stats["unresolvable"] += 1

        # price harvest: YES token only (index 0), only if tokens exist
        toks = jloads_maybe(m.get("clobTokenIds")) or []
        if toks and tids:
            yes_venue_id = str(toks[0])
            n = self.store_history(tids[0],
                                   self.fetch_history(yes_venue_id, 1440),
                                   1440, raw_ref)
            self.stats["price_points"] += n
            time.sleep(PRICE_SLEEP)
            if (not self.skip_fine and closed_time
                    and closed_time > datetime.now(timezone.utc)
                    - timedelta(days=FINE_WINDOW_DAYS)):
                nf = self.store_history(tids[0],
                                        self.fetch_history(yes_venue_id, 10),
                                        10, raw_ref)
                self.stats["fine_points"] += nf
                time.sleep(PRICE_SLEEP)
        else:
            self.stats["no_tokens"] += 1

        self.cur.execute(
            """INSERT INTO parsed_files (file_path, parsed_at, row_counts, parser_sha)
               VALUES (%s, now(), %s, %s) ON CONFLICT DO NOTHING""",
            (key, json.dumps({"resolved": win_idx is not None}), git_sha()))
        self.parsed.add(key)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, help="stop after N markets (sample)")
    ap.add_argument("--skip-fine", action="store_true")
    args = ap.parse_args()

    env = dotenv_values(REPO / ".env")
    conn = psycopg2.connect(host="127.0.0.1", port=5432, dbname="quantlab",
                            user="quantlab", password=env["PG_PASSWORD"])
    conn.autocommit = False
    with conn.cursor() as c:
        c.execute("""INSERT INTO collector_runs (component, started_at, version_sha, notes)
                     VALUES ('backfill_prices', now(), %s, %s) RETURNING id""",
                  (git_sha(), f"args={vars(args)}"))
        run_id = c.fetchone()[0]
    conn.commit()

    client = httpx.Client(headers=UA)
    bl = BackfillLoader(conn, run_id, client, args.skip_fine)

    cursor, page, processed = None, 0, 0
    while True:
        params = {"closed": "true", "limit": "100"}
        if cursor:
            params["after_cursor"] = cursor
        r = client.get(f"{GAMMA}/markets/keyset", params=params, timeout=30)
        r.raise_for_status()
        d = r.json()
        markets = d.get("markets", [])
        page += 1
        for m in markets:
            bl.process_market(m)
            processed += 1
            if args.limit and processed >= args.limit:
                break
        conn.commit()
        if page % 10 == 0:
            log.info("page %d | %s", page, bl.stats)
        cursor = d.get("next_cursor")
        if not markets or not cursor or (args.limit and processed >= args.limit):
            break
        time.sleep(PAGE_SLEEP)

    conn.commit()
    log.info("BACKFILL COMPLETE run_id=%s pages=%d stats=%s", run_id, page, bl.stats)


if __name__ == "__main__":
    main()