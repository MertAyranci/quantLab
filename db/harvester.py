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
IMMINENT_MAX_WATCH_DAYS = 7
IMMINENT_SLOTS = 45           # reserved for fast-resolving markets
NEAR_MAX_DAYS = 7             # only admit near-certainties resolving within 7d
IMMINENT_STALE_DAYS = 1   # applies ONLY to imminent misclassifications
PAGE_SLEEP = 0.25


class Harvester(Backloader):
    def __init__(self, conn, run_id, dry_run: bool):
        super().__init__(conn, run_id)
        self.dry_run = dry_run
        self.http = httpx.Client(headers=UA, timeout=30)
        self.now = datetime.now(timezone.utc)
        self.admits: list[dict] = []
        self.evicts: list[dict] = []

    # ---- detect closures-----------------------------------------------------
    def detect_closures(self):
        """Mark watching markets as 'closed' once trading has actually stopped
        (resolution recorded OR closed/resolved status observed). Records the
        final observed price for the survivorship trajectory. Runs before
        admissions so freed slots can be refilled the same pass."""
        self.cur.execute("""
            SELECT w.market_id
            FROM h4_watch w
            WHERE w.status = 'watching'
              AND (EXISTS (SELECT 1 FROM resolutions r WHERE r.market_id = w.market_id)
                OR EXISTS (SELECT 1 FROM market_status s
                           WHERE s.market_id = w.market_id
                             AND s.status IN ('closed','resolved')))
        """)
        to_close = [r[0] for r in self.cur.fetchall()]
        for mid in to_close:
            if self.dry_run:
                self.evicts.append({"market_id": mid, "reason": "closed"})
                continue
            # capture final observed YES price for the trajectory record
            self.cur.execute("""
                SELECT ts.best_bid_mc
                FROM tob_snapshots ts
                JOIN tokens tk ON tk.id = ts.token_id
                WHERE tk.market_id = %s AND tk.outcome_index = 0
                ORDER BY ts.capture_time DESC LIMIT 1""", (mid,))
            row = self.cur.fetchone()
            last_price = row[0] if row else None
            self.cur.execute("""
                UPDATE h4_watch w
                SET status='closed', final_snapshot_taken=true,
                    closed_time = COALESCE(
                        (SELECT r.event_time FROM resolutions r
                         WHERE r.market_id = w.market_id
                         ORDER BY r.event_time LIMIT 1),
                        (SELECT max(ts.capture_time) FROM tob_snapshots ts
                         JOIN tokens tk ON tk.id = ts.token_id
                         WHERE tk.market_id = w.market_id),
                        now()),
                    last_observed_price_mc = COALESCE(last_observed_price_mc, %s),
                    last_observed_time = now()
                WHERE market_id = %s AND status='watching'""", (last_price, mid))
        if to_close:
            log.info("close-detection: marked %d markets closed", len(to_close))
        return to_close

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
        within_near = end <= self.now + timedelta(days=NEAR_MAX_DAYS)

        prices = jloads_maybe(m.get("outcomePrices")) or []
        yes_mc = None
        if prices:
            try:
                yes_mc = int(float(prices[0]) * 1000)
            except (ValueError, TypeError):
                yes_mc = None

        # near-certainty: HIGH side only (>=0.95) AND resolving within 30d.
        # Low-side (<=0.05) near-certainties are overwhelmingly losing siblings
        # in multi-candidate events (H3's negative-risk structure), not H4
        # last-mile winners — excluded from v1.
        if yes_mc is not None and within_near and yes_mc >= NEAR_HIGH_MC:
            before_48h = end - self.now > timedelta(hours=48)
            return "near_certainty", {
                "crossing_price_mc": yes_mc, "crossing_side": "high",
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
        status = {
            "maxdur":      "evicted_maxdur",
            "too_distant": "evicted_maxdur",   # evicted while open, NOT closed
            "closed":      "closed",           # genuinely resolved (close-detection)
        }.get(reason, "evicted_maxdur")
        self.cur.execute("""
            UPDATE h4_watch SET status=%s, evicted_time=now(), evicted_reason=%s
            WHERE market_id=%s AND status='watching'""",
            (status, reason, market_id))

    # ---- main pass ----------------------------------------------------------

    def run_pass(self):
        self.detect_closures()
        watching = self.currently_watching()
        ev_counts = self.event_counts(watching)
        n_watching = len(watching)

        # 1. evictions first (free up slots)
        for mid, info in watching.items():
            if info["reason"] == "imminent":
                age = self.now - info["admit"]
                sched = info["sched"]
                # (a) misclassification cleanup: watched too long
                too_old = age > timedelta(days=IMMINENT_MAX_WATCH_DAYS)
                # (b) stale: scheduled close passed but never resolved (lingering)
                stale = (sched is not None
                         and sched < self.now - timedelta(days=IMMINENT_STALE_DAYS))
                if too_old or stale:
                    self.evict(mid, "maxdur")
                    n_watching -= 1
                    if info["event"]:
                        ev_counts[info["event"]] = ev_counts.get(info["event"], 1) - 1
            # near_certainty: survivorship guard protects markets in their final
            # window, but a market resolving weeks out has no last-mile yet and
            # may be evicted to free slots (re-admitted as its date approaches).
            elif info["reason"] == "near_certainty":
                sched = info["sched"]
                if (sched is not None
                        and sched > self.now + timedelta(days=NEAR_MAX_DAYS)):
                    self.evict(mid, "too_distant")
                    n_watching -= 1
                    if info["event"]:
                        ev_counts[info["event"]] = ev_counts.get(info["event"], 1) - 1

        # 2. sweep venue-wide, admit new candidates
        seen_markets = set(watching.keys())
        n_imminent = sum(1 for i in watching.values() if i["reason"] == "imminent")
        n_near = sum(1 for i in watching.values() if i["reason"] == "near_certainty")
        near_cap = MAX_MARKETS - IMMINENT_SLOTS      # e.g. 60 - 45 = 15
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
            # enforce near-certainty ceiling so imminent slots stay reserved
            if reason == "near_certainty" and n_near >= near_cap:
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
            if reason == "imminent":
                n_imminent += 1
            else:
                n_near += 1
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