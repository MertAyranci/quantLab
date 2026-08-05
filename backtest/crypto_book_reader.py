"""Shared synchronized-book reader for crypto H4 / H4b analyses.

Reconstructs the order book for BOTH tokens of a 5m market at a given as-of time
(end_date - horizon + latency), with strict quality gates per the research spec:
  * synchronized: reject if the two tokens' snapshots are > SYNC_TOL_S apart
  * fresh: reject if either quote is older than MAX_QUOTE_AGE_S at as-of
  * full depth preferred (walk real levels), BBO fallback with haircut
  * price in milli-dollars (mc); book jsonb is [[price_mc, "size"], ...]

Returns a MarketBook with both sides, or None with a rejection reason.
"""

from __future__ import annotations

from dataclasses import dataclass, field

SYNC_TOL_S = 2.0          # two tokens' books must be within this many seconds
MAX_QUOTE_AGE_S = 3.0     # quote must be no older than this at as-of


@dataclass
class SideBook:
    outcome_index: int
    full_bids: list[tuple[int, float]] = field(default_factory=list)  # (mc, size) high->low
    full_asks: list[tuple[int, float]] = field(default_factory=list)  # (mc, size) low->high
    best_bid_mc: int | None = None
    best_ask_mc: int | None = None
    best_bid_size: float | None = None
    best_ask_size: float | None = None
    full_age_s: float | None = None
    bbo_age_s: float | None = None
    src: str = "NONE"         # FULL_DEPTH | BBO_FALLBACK | NONE

    def spread_mc(self):
        if self.best_bid_mc is None or self.best_ask_mc is None:
            return None
        return self.best_ask_mc - self.best_bid_mc


@dataclass
class MarketBook:
    market_id: int
    slug: str
    end_date: object
    winning_outcome: int
    sides: dict           # outcome_index -> SideBook


def _parse_levels(raw):
    """[[price_mc, 'size'], ...] -> list of (int mc, float size)."""
    out = []
    if not raw:
        return out
    for lvl in raw:
        try:
            mc = int(lvl[0]); sz = float(lvl[1])
            if sz > 0:
                out.append((mc, sz))
        except (ValueError, TypeError, IndexError):
            continue
    return out


def _side_from_full(cur, token_id, as_of, outcome_index):
    """Latest full-depth book_snapshot <= as_of."""
    cur.execute("""
        SELECT bids, asks, EXTRACT(EPOCH FROM (%s - capture_time)) AS age
        FROM book_snapshots
        WHERE token_id=%s AND capture_time <= %s
        ORDER BY capture_time DESC LIMIT 1""", (as_of, token_id, as_of))
    row = cur.fetchone()
    if not row:
        return None
    bids_raw, asks_raw, age = row
    bids = sorted(_parse_levels(bids_raw), key=lambda x: -x[0])   # high->low
    asks = sorted(_parse_levels(asks_raw), key=lambda x: x[0])    # low->high
    sb = SideBook(outcome_index=outcome_index, full_bids=bids, full_asks=asks,
                  full_age_s=float(age) if age is not None else None)
    if bids:
        sb.best_bid_mc, sb.best_bid_size = bids[0]
    if asks:
        sb.best_ask_mc, sb.best_ask_size = asks[0]
    sb.src = "FULL_DEPTH"
    return sb


def _side_from_bbo(cur, token_id, as_of, outcome_index):
    """Latest tob_snapshot <= as_of (BBO fallback)."""
    cur.execute("""
        SELECT best_bid_mc, best_bid_size, best_ask_mc, best_ask_size,
               EXTRACT(EPOCH FROM (%s - capture_time)) AS age
        FROM tob_snapshots
        WHERE token_id=%s AND capture_time <= %s
        ORDER BY capture_time DESC LIMIT 1""", (as_of, token_id, as_of))
    row = cur.fetchone()
    if not row:
        return None
    bb, bbs, ba, bas, age = row
    sb = SideBook(outcome_index=outcome_index,
                  best_bid_mc=bb, best_bid_size=float(bbs) if bbs is not None else None,
                  best_ask_mc=ba, best_ask_size=float(bas) if bas is not None else None,
                  bbo_age_s=float(age) if age is not None else None, src="BBO_FALLBACK")
    return sb


def read_side(cur, token_id, as_of, outcome_index):
    """Full depth if fresh, else BBO if fresh, else None."""
    full = _side_from_full(cur, token_id, as_of, outcome_index)
    if full and full.full_age_s is not None and full.full_age_s <= MAX_QUOTE_AGE_S \
            and (full.full_bids or full.full_asks):
        return full
    bbo = _side_from_bbo(cur, token_id, as_of, outcome_index)
    if bbo and bbo.bbo_age_s is not None and bbo.bbo_age_s <= MAX_QUOTE_AGE_S \
            and bbo.best_ask_mc is not None:
        return bbo
    return None


def read_market_book(cur, market_id, slug, end_date, winning_outcome, as_of):
    """Both sides, synchronized. Returns (MarketBook, None) or (None, reason)."""
    cur.execute("""SELECT id, outcome_index FROM tokens WHERE market_id=%s
                   AND outcome_index IN (0,1)""", (market_id,))
    toks = {oi: tid for tid, oi in cur.fetchall()}
    if 0 not in toks or 1 not in toks:
        return None, "missing_token"
    sides = {}
    ages = []
    for oi, tid in toks.items():
        sb = read_side(cur, tid, as_of, oi)
        if sb is None:
            return None, f"no_fresh_book_side_{oi}"
        sides[oi] = sb
        age = sb.full_age_s if sb.src == "FULL_DEPTH" else sb.bbo_age_s
        if age is not None:
            ages.append(age)
    # synchronization: the two sides' snapshot times must be close
    if len(ages) == 2 and abs(ages[0] - ages[1]) > SYNC_TOL_S:
        return None, "unsynchronized"
    return MarketBook(market_id, slug, end_date, winning_outcome, sides), None