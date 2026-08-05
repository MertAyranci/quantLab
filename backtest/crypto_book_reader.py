"""Shared synchronized-book reader for crypto H4 / H4b analyses.

Reads the latest causal market state available at or before ``as_of`` for BOTH
tokens of a 5-minute binary market.

Quality gates:
  * both token books must be fresh
  * token snapshots must be synchronized
  * full depth is used only when it is two-sided and not materially older than
    a newer BBO observation
  * crossed or materially incoherent complementary books are rejected
  * prices are stored in milli-dollars (mc)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

SYNC_TOL_S = 2.0
MAX_QUOTE_AGE_S = 3.0
SOURCE_FRESHNESS_TOL_S = 0.25
COMPLEMENT_TOL_MC = 20


@dataclass
class SideBook:
    outcome_index: int
    token_id: int
    capture_time: datetime
    full_bids: list[tuple[int, float]] = field(default_factory=list)
    full_asks: list[tuple[int, float]] = field(default_factory=list)
    best_bid_mc: int | None = None
    best_ask_mc: int | None = None
    best_bid_size: float | None = None
    best_ask_size: float | None = None
    age_s: float | None = None
    src: str = "NONE"  # FULL_DEPTH | BBO_FALLBACK | NONE

    def spread_mc(self) -> int | None:
        if self.best_bid_mc is None or self.best_ask_mc is None:
            return None
        return self.best_ask_mc - self.best_bid_mc

    def is_two_sided(self) -> bool:
        return self.best_bid_mc is not None and self.best_ask_mc is not None


@dataclass
class MarketBook:
    market_id: int
    slug: str
    end_date: object
    winning_outcome: int
    as_of: datetime
    sides: dict[int, SideBook]


def _parse_levels(raw) -> list[tuple[int, float]]:
    """Convert ``[[price_mc, size], ...]`` into validated numeric levels."""
    out: list[tuple[int, float]] = []
    if not raw:
        return out

    for level in raw:
        try:
            price_mc = int(level[0])
            size = float(level[1])
        except (ValueError, TypeError, IndexError):
            continue

        if 0 <= price_mc <= 1000 and size > 0:
            out.append((price_mc, size))

    return out


def _side_from_full(cur, token_id: int, as_of, outcome_index: int) -> SideBook | None:
    cur.execute(
        """
        SELECT bids,
               asks,
               capture_time,
               EXTRACT(EPOCH FROM (%s - capture_time)) AS age
        FROM book_snapshots
        WHERE token_id = %s
          AND capture_time <= %s
        ORDER BY capture_time DESC
        LIMIT 1
        """,
        (as_of, token_id, as_of),
    )
    row = cur.fetchone()
    if not row:
        return None

    bids_raw, asks_raw, capture_time, age = row
    bids = sorted(_parse_levels(bids_raw), key=lambda x: -x[0])
    asks = sorted(_parse_levels(asks_raw), key=lambda x: x[0])

    side = SideBook(
        outcome_index=outcome_index,
        token_id=token_id,
        capture_time=capture_time,
        full_bids=bids,
        full_asks=asks,
        age_s=float(age) if age is not None else None,
        src="FULL_DEPTH",
    )
    if bids:
        side.best_bid_mc, side.best_bid_size = bids[0]
    if asks:
        side.best_ask_mc, side.best_ask_size = asks[0]
    return side


def _side_from_bbo(cur, token_id: int, as_of, outcome_index: int) -> SideBook | None:
    cur.execute(
        """
        SELECT best_bid_mc,
               best_bid_size,
               best_ask_mc,
               best_ask_size,
               capture_time,
               EXTRACT(EPOCH FROM (%s - capture_time)) AS age
        FROM tob_snapshots
        WHERE token_id = %s
          AND capture_time <= %s
        ORDER BY capture_time DESC
        LIMIT 1
        """,
        (as_of, token_id, as_of),
    )
    row = cur.fetchone()
    if not row:
        return None

    best_bid, best_bid_size, best_ask, best_ask_size, capture_time, age = row
    return SideBook(
        outcome_index=outcome_index,
        token_id=token_id,
        capture_time=capture_time,
        best_bid_mc=best_bid,
        best_bid_size=float(best_bid_size) if best_bid_size is not None else None,
        best_ask_mc=best_ask,
        best_ask_size=float(best_ask_size) if best_ask_size is not None else None,
        age_s=float(age) if age is not None else None,
        src="BBO_FALLBACK",
    )


def _valid_common(side: SideBook | None) -> bool:
    if side is None or side.age_s is None:
        return False
    if side.age_s < -1e-6 or side.age_s > MAX_QUOTE_AGE_S:
        return False
    if not side.is_two_sided():
        return False
    if side.best_bid_mc > side.best_ask_mc:
        return False
    return True


def _valid_full(side: SideBook | None) -> bool:
    return bool(
        _valid_common(side)
        and side is not None
        and side.full_bids
        and side.full_asks
    )


def _valid_bbo(side: SideBook | None) -> bool:
    return _valid_common(side)


def _tops_match(left: SideBook, right: SideBook) -> bool:
    return (
        left.best_bid_mc == right.best_bid_mc
        and left.best_ask_mc == right.best_ask_mc
    )


def read_side(cur, token_id: int, as_of, outcome_index: int) -> SideBook | None:
    """Choose the best causal source available at ``as_of``.

    Full depth is preferred only if it is two-sided and not contradicted by a
    materially newer BBO. A newer contradictory BBO wins because its top level
    is the more reliable execution state.
    """
    full = _side_from_full(cur, token_id, as_of, outcome_index)
    bbo = _side_from_bbo(cur, token_id, as_of, outcome_index)

    full_ok = _valid_full(full)
    bbo_ok = _valid_bbo(bbo)

    if full_ok and bbo_ok:
        assert full is not None and bbo is not None
        bbo_newer_by = (bbo.capture_time - full.capture_time).total_seconds()

        if bbo_newer_by > SOURCE_FRESHNESS_TOL_S:
            return bbo
        if bbo.capture_time > full.capture_time and not _tops_match(full, bbo):
            return bbo
        return full

    if full_ok:
        return full
    if bbo_ok:
        return bbo
    return None


def read_market_book(
    cur,
    market_id: int,
    slug: str,
    end_date,
    winning_outcome: int,
    as_of,
) -> tuple[MarketBook | None, str | None]:
    """Read synchronized causal books for both binary outcomes."""
    cur.execute(
        """
        SELECT id, outcome_index
        FROM tokens
        WHERE market_id = %s
          AND outcome_index IN (0, 1)
        """,
        (market_id,),
    )
    tokens = {outcome_index: token_id for token_id, outcome_index in cur.fetchall()}
    if 0 not in tokens or 1 not in tokens:
        return None, "missing_token"

    sides: dict[int, SideBook] = {}
    for outcome_index in (0, 1):
        side = read_side(cur, tokens[outcome_index], as_of, outcome_index)
        if side is None:
            return None, f"no_fresh_two_sided_book_{outcome_index}"
        sides[outcome_index] = side

    time_gap_s = abs(
        (sides[0].capture_time - sides[1].capture_time).total_seconds()
    )
    if time_gap_s > SYNC_TOL_S:
        return None, "unsynchronized"

    for outcome_index, side in sides.items():
        spread = side.spread_mc()
        if spread is None or spread < 0:
            return None, f"crossed_side_{outcome_index}"

    bid_sum = sides[0].best_bid_mc + sides[1].best_bid_mc
    ask_sum = sides[0].best_ask_mc + sides[1].best_ask_mc

    if bid_sum > 1000 + COMPLEMENT_TOL_MC:
        return None, "incoherent_bid_sum"
    if ask_sum < 1000 - COMPLEMENT_TOL_MC:
        return None, "incoherent_ask_sum"

    return (
        MarketBook(
            market_id=market_id,
            slug=slug,
            end_date=end_date,
            winning_outcome=winning_outcome,
            as_of=as_of,
            sides=sides,
        ),
        None,
    )