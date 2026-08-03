"""Fill simulator — the shared execution core.

A PURE function: given a book state and an order, what fills? No network, no DB.
The caller (order manager in paper mode, or H4-Exec in replay) fetches the book
and passes it in. This keeps it exhaustively testable and reusable across both.

Hierarchical book model (per spec):
  1. full-depth snapshot if fresh enough (walk visible levels) -> FULL_DEPTH
  2. else BBO + configurable liquidity haircut               -> BBO_FALLBACK
  3. else reject                                              -> NONE

Honesty rule: a BBO-fallback fill is NEVER presented as equivalent to a
full-depth walk. Every fill records book_source so downstream reporting can
separate the two.

v1 models ONLY aggressive marketable-limit IOC orders. No passive fills, queue
position, hidden liquidity, market impact beyond visible depth, or resting orders.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class BookSource(Enum):
    FULL_DEPTH = "FULL_DEPTH"
    BBO_FALLBACK = "BBO_FALLBACK"
    NONE = "NONE"


class FillStatus(Enum):
    FILLED = "filled"
    PARTIAL = "partial"
    REJECTED = "rejected"


@dataclass
class FillConfig:
    latency_ms: int = 1000
    full_book_ttl_s: float = 60.0
    bbo_ttl_s: float = 10.0
    liquidity_haircut: float = 0.50      # BBO fallback only
    # scenario presets
    @staticmethod
    def optimistic():
        return FillConfig(latency_ms=250, liquidity_haircut=1.00)
    @staticmethod
    def base():
        return FillConfig(latency_ms=1000, liquidity_haircut=0.50)
    @staticmethod
    def conservative():
        return FillConfig(latency_ms=3000, liquidity_haircut=0.25)


@dataclass
class BookState:
    """What the caller passes in. Prices/sizes as floats; ages in seconds
    relative to the (already latency-adjusted) arrival time.
    full_depth: list of (price, size) levels; bids high->low, asks low->high.
    Empty/None where unavailable."""
    market_open: bool = True
    collector_gap: bool = False
    full_bids: list[tuple[float, float]] | None = None
    full_asks: list[tuple[float, float]] | None = None
    full_age_s: float | None = None
    best_bid: float | None = None
    best_ask: float | None = None
    best_bid_size: float | None = None
    best_ask_size: float | None = None
    bbo_age_s: float | None = None
    book_timestamp: str | None = None


@dataclass
class FillResult:
    market_id: int
    token_id: int
    side: str
    requested_shares: float
    limit_price: float
    book_source: BookSource
    fill_status: FillStatus
    filled_shares: float = 0.0
    unfilled_shares: float = 0.0
    avg_fill_price: float | None = None
    worst_fill_price: float | None = None
    fees: float = 0.0
    book_age_s: float | None = None
    liquidity_haircut: float | None = None
    book_timestamp: str | None = None
    rejection_reason: str = ""
    levels: list[tuple[float, float]] = field(default_factory=list)  # (price, shares)


def _reject(order_ctx, reason) -> FillResult:
    return FillResult(
        market_id=order_ctx["market_id"], token_id=order_ctx["token_id"],
        side=order_ctx["side"], requested_shares=order_ctx["shares"],
        limit_price=order_ctx["limit_price"], book_source=BookSource.NONE,
        fill_status=FillStatus.REJECTED, unfilled_shares=order_ctx["shares"],
        rejection_reason=reason)


def simulate_fill(market_id: int, token_id: int, side: str,
                  shares: float, limit_price: float,
                  book: BookState, cfg: FillConfig,
                  fee_bps: float = 0.0) -> FillResult:
    """Aggressive marketable-limit IOC fill against a recorded book.
    `shares` = requested contract quantity. `fee_bps` applied to filled notional.
    """
    ctx = {"market_id": market_id, "token_id": token_id, "side": side,
           "shares": shares, "limit_price": limit_price}

    # ---- validity gates (reject before touching liquidity) ----
    if not book.market_open:
        return _reject(ctx, "market closed/suspended")
    if book.collector_gap:
        return _reject(ctx, "collector gap active")
    if shares <= 0:
        return _reject(ctx, "non-positive size")
    if not (0.0 < limit_price < 1.0):
        return _reject(ctx, f"limit price {limit_price} out of (0,1)")

    # ---- choose book source: full depth if fresh, else BBO, else reject ----
    have_full = (book.full_age_s is not None
                 and book.full_age_s <= cfg.full_book_ttl_s
                 and (book.full_asks if side == "buy" else book.full_bids))
    have_bbo = (book.bbo_age_s is not None and book.bbo_age_s <= cfg.bbo_ttl_s
                and (book.best_ask if side == "buy" else book.best_bid) is not None)

    if have_full:
        return _fill_full_depth(ctx, book, cfg, fee_bps)
    if have_bbo:
        return _fill_bbo(ctx, book, cfg, fee_bps)
    return _reject(ctx, "no fresh executable book (full stale, BBO stale/empty)")


def _fill_full_depth(ctx, book: BookState, cfg: FillConfig, fee_bps) -> FillResult:
    side = ctx["side"]
    levels = (sorted(book.full_asks) if side == "buy"
              else sorted(book.full_bids, reverse=True))
    limit = ctx["limit_price"]
    want = ctx["shares"]
    filled = 0.0
    cost = 0.0
    taken = []
    for price, size in levels:
        # buy: only lift asks <= limit; sell: only hit bids >= limit
        if side == "buy" and price > limit:
            break
        if side == "sell" and price < limit:
            break
        if size <= 0:
            continue
        take = min(size, want - filled)
        if take <= 0:
            break
        filled += take
        cost += take * price
        taken.append((price, take))
        if filled >= want - 1e-9:
            break

    if filled <= 1e-9:
        r = _reject(ctx, "limit did not cross visible full-depth book")
        r.book_source = BookSource.FULL_DEPTH
        r.book_age_s = book.full_age_s
        return r

    avg = cost / filled
    worst = taken[-1][0]
    fees = cost * (fee_bps / 10000.0)
    return FillResult(
        market_id=ctx["market_id"], token_id=ctx["token_id"], side=side,
        requested_shares=want, limit_price=limit,
        book_source=BookSource.FULL_DEPTH,
        fill_status=FillStatus.FILLED if filled >= want - 1e-9 else FillStatus.PARTIAL,
        filled_shares=filled, unfilled_shares=max(0.0, want - filled),
        avg_fill_price=avg, worst_fill_price=worst, fees=fees,
        book_age_s=book.full_age_s, book_timestamp=book.book_timestamp,
        levels=taken)


def _fill_bbo(ctx, book: BookState, cfg: FillConfig, fee_bps) -> FillResult:
    side = ctx["side"]
    px = book.best_ask if side == "buy" else book.best_bid
    disp = book.best_ask_size if side == "buy" else book.best_bid_size
    limit = ctx["limit_price"]
    want = ctx["shares"]

    # limit must cross
    if side == "buy" and px > limit:
        r = _reject(ctx, f"buy limit {limit} below best ask {px}")
        r.book_source = BookSource.BBO_FALLBACK; r.book_age_s = book.bbo_age_s
        return r
    if side == "sell" and px < limit:
        r = _reject(ctx, f"sell limit {limit} above best bid {px}")
        r.book_source = BookSource.BBO_FALLBACK; r.book_age_s = book.bbo_age_s
        return r
    if disp is None or disp <= 0:
        r = _reject(ctx, "BBO displayed size missing/non-positive")
        r.book_source = BookSource.BBO_FALLBACK; r.book_age_s = book.bbo_age_s
        return r

    usable = disp * cfg.liquidity_haircut       # do NOT infer deeper liquidity
    filled = min(want, usable)
    if filled <= 1e-9:
        r = _reject(ctx, "haircut usable size ~0")
        r.book_source = BookSource.BBO_FALLBACK; r.book_age_s = book.bbo_age_s
        return r
    cost = filled * px
    fees = cost * (fee_bps / 10000.0)
    return FillResult(
        market_id=ctx["market_id"], token_id=ctx["token_id"], side=side,
        requested_shares=want, limit_price=limit,
        book_source=BookSource.BBO_FALLBACK,
        fill_status=FillStatus.FILLED if filled >= want - 1e-9 else FillStatus.PARTIAL,
        filled_shares=filled, unfilled_shares=max(0.0, want - filled),
        avg_fill_price=px, worst_fill_price=px, fees=fees,
        book_age_s=book.bbo_age_s, liquidity_haircut=cfg.liquidity_haircut,
        book_timestamp=book.book_timestamp, levels=[(px, filled)])