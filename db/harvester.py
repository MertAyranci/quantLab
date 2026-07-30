"""Grade-A harvester (v1) — steer the WS collector toward resolving markets.

Turns the WS collector from "top 25 by volume" into an unbiased cohort of
markets approaching resolution, which is the data H4 actually needs.

v1 scope (staged; corrections applied):
  * TWO cohorts, both from a VENUE-WIDE Gamma sweep (not watchlist-limited):
    - imminent:       end_date within IMMINENT_HOURS (default 60h)
    - near_certainty: outcomePrices crosses >=0.95 or <=0.05 (any market)
  * survivorship guard: once admitted, a near_certainty market is NEVER evicted
    for price reversal; it stays until trading closes. The 7-day max-duration
    eviction applies ONLY to misclassified 'imminent' markets.
  * both outcome tokens per market (market is the unit)
  * diversification: <= MAX_PER_EVENT per event
  * atomic watchlist write (temp file + rename) to config/watchlist.txt
  * --dry-run: report proposed admits/evicts WITHOUT touching the live watchlist

Deferred to later versions (need data we don't collect yet):
  matched controls & liquidity grading (needs a metric-snapshot table),
  sports-calendar / expiry triggers, special-study cohort, Tier-2 deep capture.

Usage:
  .venv/bin/python db/harvester.py --dry-run     # report only
  .venv/bin/python db/harvester.py               # update ledger + watchlist
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import psycopg2
import psycopg2.extras
from dotenv import dotenv_values

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backloader import Backloader, VENUE_ID, git_sha, iso, jloads_maybe, log  # noqa

REPO = Path(__file__).resolve().parent.parent
ENV = dotenv_values(REPO / ".env")
GAMMA = "https://gamma-api.polymarket.com"
UA = {"User-Agent": "quant-lab-harvester/0.1"}
WATCHLIST_FILE = REPO / "config" / "watchlist.txt"

# tunables
IMMINENT_HOURS = 60
NEAR_HIGH_MC = 950          # >= 0.95
NEAR_LOW_MC = 50            # <= 0.05
MAX_MARKETS = 60
MAX_TOKENS = 120
MAX_PER_EVENT = 3
IMMINENT_MAX_WATCH_DAYS = 7  # applies ONLY to imminent misclassifications
PAGE_SLEEP = 0.25


class Harvester(Backloader):
    def __init__(self, conn, run_id, dry_run: bool):
        super().__init__(conn, run_id)
        self.dry_run = dry_run
        self.http = httpx.Client(headers=UA, timeout=30)
        self.now = datetime.now(timezone.utc)
        self.admits: list[dict] = []
        self.evicts: list[dict] = []

    # ---- venue-wide sweep ---------------------------------------------------

    def sweep_gamma(self):
        """Walk active markets; yield candidate dicts with parsed fields."""
        cursor = None
        pages = 0
        while True:
            params = {"active": "true", "closed": "false", "limit": "100"}
            if cursor:
                params["after_cursor"] = cursor
            r = self.http.get(f"{GAMMA}/markets/keyset", params=params)
            r.raise_for_status()
            d = r.json()
            for m in d.get("markets", []):
                yield m
            cursor = d.get("next_cursor")
            pages += 1
            if not d.get("markets") or not cursor:
                break
            time.sleep(PAGE_SLEEP)
        log.info("gamma sweep: %d pages", pages)

    def classify(self, m: dict):
        end = iso(m.get("endDate"))
        # HARD REQUIREMENT: must be open and resolving within the window
        if end is None or end <= self.now:
            return None, None                      # past-end / no date = junk
        within_imminent = end <= self.now + timedelta(hours=IMMINENT_HOURS)
        within_30d = end <= self.now + timedelta(days=30)

        prices = jloads_maybe(m.get("outcomePrices")) or []
        yes_mc = None
        if prices:
            try:
                yes_mc = int(float(prices[0]) * 1000)
            except (ValueError, TypeError):
                yes_mc = None

        # near-certainty: extreme price AND resolving within 30 days
        if (yes_mc is not None and within_30d
                and (yes_mc >= NEAR_HIGH_MC or yes_mc <= NEAR_LOW_MC)):
            side = "high" if yes_mc >= NEAR_HIGH_MC else "low"
            before_48h = end - self.now > timedelta(hours=48)
            return "near_certainty", {
                "crossing_price_mc": yes_mc, "crossing_side": side,
                "crossing_before_48h": before_48h, "scheduled_close": end}

        # imminent: resolving within 60h at any price
        if within_imminent:
            return "imminent", {"scheduled_close": end}

        return None, None

    # ---- ledger ops ---------------------------------------------------------

    def currently_watching(self) -> dict:
        self.cur.execute("""
            SELECT w.market_id, w.admit_reason, w.scheduled_close, w.admit_time,
                   w.event_id
            FROM h4_watch w WHERE w.status = 'watching'""")
        return {r[0]: {"reason": r[1], "sched": r[2], "admit": r[3], "event": r[4]}
                for r in self.cur.fetchall()}

    def event_counts(self, watching: dict) -> dict:
        counts = {}
        for info in watching.values():
            if info["event"] is not None:
                counts[info["event"]] = counts.get(info["event"], 0) + 1
        return counts

    def admit(self, market_id, event_id, reason, extra, cat):
        self.admits.append({"market_id": market_id, "reason": reason,
                            "event_id": event_id, **extra})
        if self.dry_run:
            return
        self.cur.execute("""
            INSERT INTO h4_watch (market_id, admit_reason, admit_source,
                crossing_price_mc, crossing_time, crossing_side,
                crossing_before_48h, scheduled_close, event_id, category)
            VALUES (%s,%s,'gamma_sweep',%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (market_id) WHERE status='watching' DO NOTHING""",
            (market_id, reason, extra.get("crossing_price_mc"),
             self.now if extra.get("crossing_price_mc") else None,
             extra.get("crossing_side"), extra.get("crossing_before_48h"),
             extra.get("scheduled_close"), event_id, cat))

    def evict(self, market_id, reason):
        self.evicts.append({"market_id": market_id, "reason": reason})
        if self.dry_run:
            return
        self.cur.execute("""
            UPDATE h4_watch SET status=%s, evicted_time=now(), evicted_reason=%s
            WHERE market_id=%s AND status='watching'""",
            ("evicted_maxdur" if reason == "maxdur" else "closed", reason, market_id))

    # ---- main pass ----------------------------------------------------------

    def run_pass(self):
        watching = self.currently_watching()
        ev_counts = self.event_counts(watching)
        n_watching = len(watching)

        # 1. evictions first (free up slots)
        for mid, info in watching.items():
            # imminent misclassification: watched > 7 days AND its scheduled
            # close is well past -> it never actually resolved as expected
            if info["reason"] == "imminent":
                age = self.now - info["admit"]
                if age > timedelta(days=IMMINENT_MAX_WATCH_DAYS):
                    self.evict(mid, "maxdur")
                    n_watching -= 1
                    if info["event"]:
                        ev_counts[info["event"]] = ev_counts.get(info["event"], 1) - 1
            # NOTE: near_certainty markets are intentionally NOT evicted here.
            # They leave only when the collector/status marks trading closed
            # (handled by a separate close-detection pass, not price reversal).

        # 2. sweep venue-wide, admit new candidates
        seen_markets = set(watching.keys())
        for m in self.sweep_gamma():
            if n_watching >= MAX_MARKETS:
                break
            vmid = str(m.get("id"))
            # resolve to internal id (create if unseen — cheap, cached)
            cap = self.now
            raw_ref = f"gamma:harvester:{vmid}"
            mid = self.market_ids.get(vmid)
            if mid is None:
                mid = self.upsert_market(m, cap, raw_ref, source="harvester")
                self.upsert_tokens(mid, m, raw_ref)
            if mid in seen_markets:
                continue

            reason, extra = self.classify(m)
            if reason is None:
                continue

            # diversification: event cap
            evs = jloads_maybe(m.get("events")) or []
            event_id = None
            if evs:
                event_id = self.event_ids.get(str(evs[0].get("id")))
            if event_id is not None and ev_counts.get(event_id, 0) >= MAX_PER_EVENT:
                continue

            cat = (m.get("category") or
                   (jloads_maybe(m.get("tags")) or [None])[0]
                   if isinstance(jloads_maybe(m.get("tags")), list) else None)
            self.admit(mid, event_id, reason, extra, cat if isinstance(cat, str) else None)
            seen_markets.add(mid)
            n_watching += 1
            if event_id is not None:
                ev_counts[event_id] = ev_counts.get(event_id, 0) + 1

        if not self.dry_run:
            self.conn.commit()

    # ---- watchlist file -----------------------------------------------------

    def write_watchlist(self):
        """Write YES+NO venue token ids, atomically. Dry-run previews from the
        in-memory admits (ledger wasn't written); live reads the ledger."""
        if self.dry_run:
            mids = [a["market_id"] for a in self.admits]
            if not mids:
                log.info("[dry-run] no admits -> 0 tokens")
                return []
            self.cur.execute("""
                SELECT venue_token_id FROM tokens
                WHERE market_id = ANY(%s)
                ORDER BY market_id, outcome_index
                LIMIT %s""", (mids, MAX_TOKENS))
            tokens = [r[0] for r in self.cur.fetchall()]
            log.info("[dry-run] would write %d tokens (%d markets)",
                     len(tokens), len(mids))
            return tokens

        # live path: read the ledger (admits were persisted)
        self.cur.execute("""
            SELECT t.venue_token_id
            FROM h4_watch w
            JOIN tokens t ON t.market_id = w.market_id
            WHERE w.status = 'watching'
            ORDER BY t.market_id, t.outcome_index
            LIMIT %s""", (MAX_TOKENS,))
        tokens = [r[0] for r in self.cur.fetchall()]
        tmp = WATCHLIST_FILE.with_suffix(".tmp")
        tmp.write_text("\n".join(tokens) + "\n")
        tmp.replace(WATCHLIST_FILE)          # atomic rename
        log.info("watchlist written: %d tokens", len(tokens))
        return tokens

    def report(self):
        print(f"\n=== HARVESTER {'DRY-RUN' if self.dry_run else 'LIVE'} "
              f"{self.now.isoformat()} ===")
        print(f"admits: {len(self.admits)}")
        by_reason = {}
        for a in self.admits:
            by_reason[a["reason"]] = by_reason.get(a["reason"], 0) + 1
        for r, n in by_reason.items():
            print(f"  {r}: {n}")
        print(f"evicts: {len(self.evicts)}")
        for e in self.evicts[:10]:
            print(f"  market {e['market_id']}: {e['reason']}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = psycopg2.connect(host="127.0.0.1", port=5432, dbname="quantlab",
                            user="quantlab", password=ENV["PG_PASSWORD"])
    conn.autocommit = False
    with conn.cursor() as c:
        c.execute("""INSERT INTO collector_runs (component, started_at, version_sha, notes)
                     VALUES ('harvester', now(), %s, %s) RETURNING id""",
                  (git_sha(), f"dry_run={args.dry_run}"))
        run_id = c.fetchone()[0]
    conn.commit()

    h = Harvester(conn, run_id, args.dry_run)
    h.run_pass()
    tokens = h.write_watchlist()
    h.report()
    print(f"\nwatchlist size: {len(tokens)} tokens "
          f"({'NOT written (dry-run)' if args.dry_run else 'written'})")


if __name__ == "__main__":
    main()