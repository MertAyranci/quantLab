"""Day 1 snapshot collector — deliberately ugly, deliberately correct.

Captures, every cycle:
  1. Active-market metadata from the Gamma API (top markets by 24h volume)
  2. Full order books from the CLOB API for a liquid watchlist

Raw JSON is gzipped to disk untouched. Parsing/schema comes on Day 3;
capture starts NOW because order books cannot be backfilled.

Principles baked in (don't remove them when you refactor):
  - UTC capture timestamp on every artifact (event-time vs capture-time
    distinction is future research data)
  - Client-side pacing + per-request latency logging (Polymarket throttles
    via Cloudflare: slowness comes BEFORE 429s, so latency IS the signal)
  - Raw payloads archived verbatim (reinterpretable when fields change)
  - Dead-man's-switch ping after each successful cycle (HEALTHCHECK_URL)

Usage:
    pip install httpx python-dotenv
    python day1_snapshot.py --watchlist-size 20 --interval 60
"""

import argparse
import gzip
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

try:  # optional; .env is a Day 2 nicety
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

_dsn = os.getenv("SENTRY_DSN")
if _dsn:
    import sentry_sdk
    sentry_sdk.init(dsn=_dsn)

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
DATA_DIR = Path(os.getenv("DATA_DIR", "data/raw"))
HEALTHCHECK_URL = os.getenv("HEALTHCHECK_URL", "")  # set on Day 2
MIN_REQUEST_GAP_S = 0.25  # client-side pacing: stay far below any budget

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)sZ %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logging.Formatter.converter = time.gmtime  # log in UTC, always
log = logging.getLogger("day1")

_last_request_ts = 0.0


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def paced_get(client: httpx.Client, url: str, params: dict | None = None,
              context: str = ""):
    """GET with pacing, retries for transient failures only, latency logging.

    4xx (except 429) = permanent for this cycle: no retry, warn, return None.
    5xx / network / 429 = transient: retry with backoff.
    """
    global _last_request_ts
    for attempt in range(1, 4):
        wait = MIN_REQUEST_GAP_S - (time.monotonic() - _last_request_ts)
        if wait > 0:
            time.sleep(wait)
        t0 = time.monotonic()
        try:
            _last_request_ts = time.monotonic()
            resp = client.get(url, params=params, timeout=30)
            latency_ms = (time.monotonic() - t0) * 1000
            if latency_ms > 3000:
                log.warning("SLOW %.0fms %s %s", latency_ms, url, context)
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as exc:
            code = exc.response.status_code
            if code == 404:
                log.info("gone (404): %s %s", url, context)   # expected lifecycle event
                return None
            if 400 <= code < 500 and code != 429:
                log.warning("client error %d: %s %s — not retrying", code, url, context)
                return None
            log.warning("attempt %d/3 got %d for %s %s", attempt, code, url, context)
            time.sleep(2 ** attempt)
        except (httpx.HTTPError, json.JSONDecodeError) as exc:
            log.warning("attempt %d/3 failed for %s %s: %s", attempt, url, context, exc)
            time.sleep(2 ** attempt)
    log.warning("giving up on %s %s", url, context)
    return None


def save_raw(kind: str, payload: dict) -> Path:
    """Gzip raw payload + capture metadata to data/raw/YYYY-MM-DD/."""
    now = datetime.now(timezone.utc)
    day_dir = DATA_DIR / now.strftime("%Y-%m-%d")
    day_dir.mkdir(parents=True, exist_ok=True)
    path = day_dir / f"{kind}_{now.strftime('%H%M%S')}.json.gz"
    envelope = {"captured_at_utc": now.isoformat(), "kind": kind, "payload": payload}
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump(envelope, f, separators=(",", ":"))
    return path


def fetch_markets(client: httpx.Client, limit: int = 100) -> list[dict]:
    """Top active markets by 24h volume from Gamma (metadata + discovery)."""
    data = paced_get(
        client,
        f"{GAMMA}/markets",
        params={
            "active": "true",
            "closed": "false",
            "order": "volume24hr",
            "ascending": "false",
            "limit": limit,
        },
    )
    return data if isinstance(data, list) else []


def extract_token_ids(market: dict) -> list[str]:
    """Gamma serialises clobTokenIds as a JSON *string* — a classic trap."""
    raw = market.get("clobTokenIds")
    if not raw:
        return []
    try:
        ids = json.loads(raw) if isinstance(raw, str) else raw
        return [str(t) for t in ids]
    except (json.JSONDecodeError, TypeError):
        log.warning("unparseable clobTokenIds on market %s", market.get("id"))
        return []


def fetch_books(client: httpx.Client, markets: list[dict], watchlist_size: int) -> dict:
    """Full CLOB order books for the watchlist (first/YES token per market)."""
    books: dict[str, dict] = {}
    for market in markets[:watchlist_size]:
        tokens = extract_token_ids(market)
        if not tokens:
            continue
        token_id = tokens[0]  # YES side; NO book is its mirror (Day 3 decision)
        book = paced_get(
            client, f"{CLOB}/book", params={"token_id": token_id},
            context=f"token={token_id[:16]}… q={str(market.get('question'))[:40]!r}",
        )
        if book is not None:
            books[token_id] = {
                "market_id": market.get("id"),
                "condition_id": market.get("conditionId"),
                "question": market.get("question"),
                "book": book,
            }
    return books


def ping_healthcheck(client: httpx.Client) -> None:
    if not HEALTHCHECK_URL:
        return
    try:
        client.get(HEALTHCHECK_URL, timeout=10)
    except httpx.HTTPError as exc:
        log.warning("healthcheck ping failed: %s", exc)


def run_cycle(client: httpx.Client, watchlist_size: int) -> bool:
    markets = fetch_markets(client)
    if not markets:
        log.error("cycle aborted: no markets returned")
        return False
    save_raw("markets", {"count": len(markets), "markets": markets})

    books = fetch_books(client, markets, watchlist_size)
    path = save_raw("books", {"count": len(books), "books": books})
    log.info(
        "cycle ok: %d markets, %d/%d books -> %s",
        len(markets), len(books), watchlist_size, path.name,
    )
    ping_healthcheck(client)
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--watchlist-size", type=int, default=20)
    parser.add_argument("--interval", type=int, default=60, help="seconds between cycles")
    parser.add_argument("--once", action="store_true", help="single cycle, then exit")
    args = parser.parse_args()

    log.info(
        "day1 snapshot starting | watchlist=%d interval=%ds dir=%s | %s",
        args.watchlist_size, args.interval, DATA_DIR, utc_now_iso(),
    )
    failures_in_a_row = 0
    with httpx.Client(headers={"User-Agent": "quant-lab-day1/0.1"}) as client:
        while True:
            started = time.monotonic()
            ok = run_cycle(client, args.watchlist_size)
            failures_in_a_row = 0 if ok else failures_in_a_row + 1
            if failures_in_a_row >= 5:
                log.critical("5 consecutive failed cycles — exiting so systemd restarts fresh")
                sys.exit(1)
            if args.once:
                break
            elapsed = time.monotonic() - started
            time.sleep(max(0.0, args.interval - elapsed))


if __name__ == "__main__":
    main()