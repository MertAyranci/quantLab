"""Book reader — bridge from Postgres recorded data to the fill simulator.

Reads the most recent recorded book for a market's token and packs it into a
BookState (full depth if a fresh book_snapshot exists, else BBO from
tob_snapshots), computing ages relative to a given 'as-of' time. This is what
lets the order manager run against real collected data in paper mode.

Prices in the DB are milli-dollars (price_mc, 0-1000); we convert to
probabilities [0,1] for the simulator. Sizes are stored as strings/numerics in
the JSON book; we parse to float.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from fill_sim import BookState


def _mc_to_prob(mc):
    return None if mc is None else mc / 1000.0


def read_book(cur, market_id: int, as_of: datetime,
              outcome_index: int = 0) -> BookState:
    """Build a BookState for the YES token (outcome_index=0 by default) of a
    market, using only data with capture_time <= as_of (no look-ahead)."""
    # resolve the token
    cur.execute("""SELECT id FROM tokens
                   WHERE market_id=%s AND outcome_index=%s""",
                (market_id, outcome_index))
    row = cur.fetchone()
    if not row:
        return BookState(market_open=True)   # unknown token -> empty book -> reject
    token_id = row[0]

    # market open? (latest status <= as_of)
    cur.execute("""SELECT status FROM market_status
                   WHERE market_id=%s AND observed_at <= %s
                   ORDER BY observed_at DESC LIMIT 1""", (market_id, as_of))
    srow = cur.fetchone()
    market_open = not (srow and srow[0] in ("closed", "resolved"))

    st = BookState(market_open=market_open)

    # full-depth: latest book_snapshot <= as_of
    cur.execute("""SELECT bids, asks, capture_time
                   FROM book_snapshots
                   WHERE token_id=%s AND capture_time <= %s
                   ORDER BY capture_time DESC LIMIT 1""", (token_id, as_of))
    brow = cur.fetchone()
    if brow:
        bids_raw, asks_raw, cap = brow
        try:
            bids = json.loads(bids_raw) if isinstance(bids_raw, str) else bids_raw
            asks = json.loads(asks_raw) if isinstance(asks_raw, str) else asks_raw
            st.full_bids = [(_mc_to_prob(p), float(s)) for p, s in (bids or [])]
            st.full_asks = [(_mc_to_prob(p), float(s)) for p, s in (asks or [])]
            st.full_age_s = (as_of - cap).total_seconds()
            st.book_timestamp = cap.isoformat()
        except (json.JSONDecodeError, TypeError, ValueError):
            pass

    # BBO: latest tob_snapshot <= as_of
    cur.execute("""SELECT best_bid_mc, best_bid_size, best_ask_mc, best_ask_size,
                          capture_time
                   FROM tob_snapshots
                   WHERE token_id=%s AND capture_time <= %s
                   ORDER BY capture_time DESC LIMIT 1""", (token_id, as_of))
    trow = cur.fetchone()
    if trow:
        bb, bbs, ba, bas, cap = trow
        st.best_bid = _mc_to_prob(bb)
        st.best_ask = _mc_to_prob(ba)
        st.best_bid_size = float(bbs) if bbs is not None else None
        st.best_ask_size = float(bas) if bas is not None else None
        st.bbo_age_s = (as_of - cap).total_seconds()
        if st.book_timestamp is None:
            st.book_timestamp = cap.isoformat()

    return st