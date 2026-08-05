"""WS collector — network half of collector v2. Records, never databases.

Usage:
    .venv/bin/python collectors/polymarket/ws_collector.py --watchlist-size 50

Architecture (RFC R5 write-path decision):
  * ONE websocket connection, watchlist YES-tokens subscribed with
    custom_feature_enabled (book / price_change / tick_size_change /
    last_trade_price / best_bid_ask / new_market / market_resolved).
  * Every message becomes JSONL lines in minute-rotated buffer files under
    data/buffer/ws/. This process NEVER touches Postgres — buffer_loader.py
    (separate process) batch-loads completed files. A DB hiccup cannot drop
    messages; loader lag is measurable and alertable.
  * Sequencing per schema v0.2: connection_id (uuid per connect),
    ingest_sequence (per-connection message counter), change_index (position
    within a multi-change message), book_generation (per-token counter,
    incremented on every (re)connect and reconciliation mismatch).

HONEST SIMPLIFICATION (v1, documented in RFC):
  Polymarket's book `hash` algorithm is undocumented, so we cannot verify it
  locally. Instead of maintaining full local books, we track the venue-reported
  best bid/ask from events and run a PERIODIC REST RECONCILIATION: every
  RECONCILE_S seconds, POST /books for all watchlist tokens; if REST top-of-book
  disagrees with our last-seen TOB for a token, we bump its generation and
  record the fresh REST book as a resync snapshot. Full book reconstruction
  happens research-side from snapshots + deltas (which is why they're stored).

Heartbeat: PING every 10s (per docs). Silence watchdog: no message for 30s ->
reconnect. Healthcheck ping (WS_HEALTHCHECK_URL) once per minute while flowing.
"""

import argparse
import asyncio
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import httpx
import websockets

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
except ImportError:
    pass

_dsn = os.getenv("SENTRY_DSN")
if _dsn:
    import sentry_sdk
    sentry_sdk.init(dsn=_dsn)

REPO = Path(__file__).resolve().parents[2]
BUF_DIR = REPO / "data" / "buffer" / "ws"
GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
WS_URI = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
UA = {"User-Agent": "quant-lab-ws/0.1"}
PING_S = 10
SILENCE_S = 30
RECONCILE_S = 300
HEALTHCHECK_URL = os.getenv("WS_HEALTHCHECK_URL", "")
WATCHLIST_FILE = REPO / "config" / "watchlist.txt"
WATCHLIST_CRYPTO_FILE = REPO / "config" / "watchlist_crypto.txt"   # ADD THIS
WATCHLIST_RELOAD_S = 60          # re-read the harvester's file this often

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)sZ %(levelname)s %(message)s",
                    datefmt="%Y-%m-%dT%H:%M:%S")
logging.Formatter.converter = time.gmtime
log = logging.getLogger("ws")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("websockets").setLevel(logging.WARNING)


def utcnow():
    return datetime.now(timezone.utc)


class BufferWriter:
    """Minute-rotated JSONL files; loader consumes files whose minute has passed."""

    def __init__(self):
        BUF_DIR.mkdir(parents=True, exist_ok=True)
        self._fh = None
        self._minute = None

    def write(self, record: dict):
        minute = utcnow().strftime("%Y%m%d_%H%M")
        if minute != self._minute:
            if self._fh:
                self._fh.close()
            self._minute = minute
            self._fh = open(BUF_DIR / f"{minute}.jsonl", "a", encoding="utf-8")
        self._fh.write(json.dumps(record, separators=(",", ":")) + "\n")

    def flush(self):
        if self._fh:
            self._fh.flush()


class WSCollector:
    def __init__(self, watchlist_size: int):
        self.watchlist_size = watchlist_size
        self.http = httpx.Client(headers=UA, timeout=30)
        self.buf = BufferWriter()
        self.tokens: list[str] = []          # venue token ids (YES side)
        self.generation: dict[str, int] = {}
        self.last_tob: dict[str, tuple] = {} # token -> (bid, ask) venue strings
        self.last_snap_t: dict[str, float] = {}
        self.conn_id = None
        self.seq = 0
        self.last_msg_t = 0.0
        self.counts: dict[str, int] = {}
        self.last_hc = 0.0

    # ---- watchlist ---------------------------------------------------------

    def fetch_watchlist(self):
        # Prefer the harvester's file; fall back to volume-ranked if absent.
        if WATCHLIST_FILE.exists():
            toks = [ln.strip() for ln in WATCHLIST_FILE.read_text().splitlines()
                    if ln.strip() and not ln.startswith("#")]
            if toks:
                self.tokens = toks
                for t in toks:
                    self.generation.setdefault(t, 0)
                log.info("watchlist: %d tokens from harvester file", len(toks))
                return
        # fallback: top-N by 24h volume (original behavior)
        r = self.http.get(f"{GAMMA}/markets",
                          params={"active": "true", "closed": "false",
                                  "order": "volume24hr", "ascending": "false",
                                  "limit": self.watchlist_size})
        r.raise_for_status()
        toks = []
        for m in r.json():
            try:
                ids = json.loads(m.get("clobTokenIds") or "[]")
                if ids:
                    toks.append(str(ids[0]))
            except (json.JSONDecodeError, TypeError):
                continue
        self.tokens = toks
        for t in toks:
            self.generation.setdefault(t, 0)
        log.info("watchlist: %d tokens (volume fallback)", len(toks))
#----- for crypto watchlist -----------------------------------------------
    def _read_watchlist_files(self):
        toks = []
        for f in (WATCHLIST_FILE, WATCHLIST_CRYPTO_FILE):
            if f.exists():
                toks += [ln.strip() for ln in f.read_text().splitlines()
                         if ln.strip() and not ln.startswith("#")]
        return list(dict.fromkeys(toks))
    # ---- resync tokens --------------------------------------------------

    def resync_tokens(self, tokens, reason):
        books = self.rest_books(tokens)
        for b in books:
            t = str(b.get("asset_id"))
            self.generation[t] = self.generation.get(t, 0) + 1
            self.seq += 1
            self.emit("rest_resync_book", t, b)
        log.info("resync (%s): %d books", reason, len(books))

    # ---- reconcile watchlist -----------------------------------------------
    async def reconcile_watchlist(self, ws):
        """Re-read the harvester file; subscribe/unsubscribe the diff over the
        live connection. New tokens get an immediate REST book snapshot so no
        last-mile data is lost during a cohort change."""
        if not (WATCHLIST_FILE.exists() or WATCHLIST_CRYPTO_FILE.exists()):
            return
        new = self._read_watchlist_files()
        if not new:
            return
        cur = set(self.tokens)
        want = set(new)
        added = want - cur
        removed = cur - want
        if not added and not removed:
            return
        if added:
            await ws.send(json.dumps({"assets_ids": list(added),
                                      "operation": "subscribe"}))
            for t in added:
                self.generation.setdefault(t, 0)
            # immediate REST snapshot for each new token (post-subscribe)
            self.resync_tokens(list(added), "watchlist_add")
        if removed:
            await ws.send(json.dumps({"assets_ids": list(removed),
                                      "operation": "unsubscribe"}))
        self.tokens = new
        log.info("watchlist reconciled: +%d -%d (now %d)",
                 len(added), len(removed), len(new))


    # ---- buffer records ----------------------------------------------------

    def emit(self, kind: str, token: str | None, payload: dict, change_index=0):
        self.buf.write({
            "kind": kind,
            "token": token,
            "capture_time": utcnow().isoformat(),
            "connection_id": self.conn_id,
            "ingest_sequence": self.seq,
            "change_index": change_index,
            "book_generation": self.generation.get(token, 0) if token else None,
            "watchlist_rule": "vol24h_top",
            "payload": payload,
        })
        self.counts[kind] = self.counts.get(kind, 0) + 1

    # ---- REST resync / reconciliation --------------------------------------

    def rest_books(self, tokens: list[str]) -> list[dict]:
        out = []
        for i in range(0, len(tokens), 50):
            chunk = tokens[i:i + 50]
            r = self.http.post(f"{CLOB}/books",
                               json=[{"token_id": t} for t in chunk])
            if r.status_code == 200:
                out.extend(r.json() or [])
            else:
                log.warning("REST /books chunk got %d", r.status_code)
        return out

    def resync_all(self, reason: str):
        """Fetch REST books for whole watchlist; bump generations; record snapshots."""
        books = self.rest_books(self.tokens)
        for b in books:
            t = str(b.get("asset_id"))
            self.generation[t] = self.generation.get(t, 0) + 1
            self.seq += 1
            self.emit("rest_resync_book", t, b)
            bids, asks = b.get("bids") or [], b.get("asks") or []
            bb = max((Decimal(l["price"]) for l in bids), default=None)
            ba = min((Decimal(l["price"]) for l in asks), default=None)
            self.last_tob[t] = (str(bb) if bb is not None else None,
                                str(ba) if ba is not None else None)
        log.info("resync (%s): %d books, generations bumped", reason, len(books))

    def reconcile(self):
        """Compare REST TOB vs last-seen WS TOB. Emit a snapshot when the book
        changed, when we've never recorded this token, or when the last snapshot
        is older than the heartbeat interval (so frozen/illiquid markets still
        record their stable price — critical for quiet near-certainties)."""
        HEARTBEAT_S = 300          # force a snapshot at least this often
        now = time.monotonic()
        books = self.rest_books(self.tokens)
        mismatched = heartbeat = 0
        for b in books:
            t = str(b.get("asset_id"))
            bids, asks = b.get("bids") or [], b.get("asks") or []
            bb = max((Decimal(l["price"]) for l in bids), default=None)
            ba = min((Decimal(l["price"]) for l in asks), default=None)
            rest = (str(bb) if bb is not None else None,
                    str(ba) if ba is not None else None)
            seen = self.last_tob.get(t)
            last_snap = self.last_snap_t.get(t, 0.0)
            changed = seen is not None and seen != rest
            never = seen is None
            stale = (now - last_snap) > HEARTBEAT_S
            if changed or never or stale:
                if changed:
                    mismatched += 1
                elif not never:
                    heartbeat += 1
                self.generation[t] = self.generation.get(t, 0) + 1
                self.seq += 1
                self.emit("rest_resync_book", t, b)
                self.last_tob[t] = rest
                self.last_snap_t[t] = now
        log.info("reconcile: %d changed, %d heartbeat, %d tokens "
                 "(books moving between polls is normal)",
                 mismatched, heartbeat, len(books))

    # ---- WS message handling ------------------------------------------------

    def handle(self, raw: str):
        if raw == "PONG":
            return
        try:
            msgs = json.loads(raw)
        except json.JSONDecodeError:
            log.warning("unparseable frame: %.80s", raw)
            return
        if isinstance(msgs, dict):
            msgs = [msgs]
        for d in msgs:
            self.seq += 1
            et = d.get("event_type")
            token = str(d.get("asset_id")) if d.get("asset_id") else None
            if et == "price_change":
                changes = d.get("price_changes") or d.get("changes") or [d]
                for ci, ch in enumerate(changes):
                    tok = str(ch.get("asset_id")) if ch.get("asset_id") else token
                    self.emit("price_change", tok,
                              {**ch,
                               "market": d.get("market"),
                               "timestamp": d.get("timestamp")}, ci)
                    if ch.get("best_bid") or ch.get("best_ask"):
                        self.last_tob[tok] = (ch.get("best_bid"), ch.get("best_ask"))
            elif et == "book":
                self.emit("ws_book", token, d)
                bids, asks = d.get("bids") or [], d.get("asks") or []
                bb = max((Decimal(l["price"]) for l in bids), default=None)
                ba = min((Decimal(l["price"]) for l in asks), default=None)
                self.last_tob[token] = (str(bb) if bb is not None else None,
                                        str(ba) if ba is not None else None)
            elif et == "best_bid_ask":
                self.emit("best_bid_ask", token, d)
                self.last_tob[token] = (d.get("best_bid"), d.get("best_ask"))
            elif et in ("last_trade_price", "tick_size_change",
                        "market_resolved", "new_market"):
                self.emit(et, token, d)
            else:
                self.emit("unknown", token, d)

    # ---- main loop -----------------------------------------------------------

    async def run(self):
        self.fetch_watchlist()
        while True:
            self.conn_id = str(uuid.uuid4())
            self.seq = 0
            try:
                async with websockets.connect(WS_URI, max_size=None) as ws:
                    await ws.send(json.dumps({
                        "assets_ids": self.tokens,
                        "type": "market",
                        "custom_feature_enabled": True,
                    }))
                    log.info("connected %s; subscribed %d tokens",
                             self.conn_id[:8], len(self.tokens))
                    self.resync_all("connect")
                    self.last_msg_t = time.monotonic()
                    last_ping = last_reconcile = last_stats = last_watchlist = time.monotonic()
                    while True:
                        now = time.monotonic()
                        if now - last_watchlist >= WATCHLIST_RELOAD_S:
                            await self.reconcile_watchlist(ws)
                            last_watchlist = now
                        if now - last_ping >= PING_S:
                            await ws.send("PING")
                            last_ping = now
                        if now - last_reconcile >= RECONCILE_S:
                            self.reconcile()
                            last_reconcile = now
                        if now - last_stats >= 60:
                            self.buf.flush()
                            log.info("1m stats: %s", self.counts)
                            self.counts = {}
                            last_stats = now
                            if HEALTHCHECK_URL:
                                try:
                                    self.http.get(HEALTHCHECK_URL, timeout=10)
                                except httpx.HTTPError:
                                    pass
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5)
                            self.last_msg_t = time.monotonic()
                            self.handle(raw)
                        except asyncio.TimeoutError:
                            if time.monotonic() - self.last_msg_t > SILENCE_S:
                                raise ConnectionError("silence watchdog")
            except Exception as e:
                log.warning("connection ended (%s) — reconnecting in 5s", e)
                self.buf.flush()
                await asyncio.sleep(5)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist-size", type=int, default=50)
    args = ap.parse_args()
    try:
        asyncio.run(WSCollector(args.watchlist_size).run())
    except KeyboardInterrupt:
        log.info("shutdown requested — flushing buffers and exiting cleanly")

if __name__ == "__main__":
    main()