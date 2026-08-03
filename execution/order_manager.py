"""Order manager — ties the engine together.

Flow for every trade intent:
  1. risk gate: check_order() -- rejected -> log reason, done. NO order reaches
     a fill without passing risk.
  2. fetch the current book (caller provides it; paper mode reads recorded data,
     live mode would read the venue -- same manager, different book source).
  3. simulate_fill() against that book.
  4. report the fill to the position tracker (apply_fill).
  5. log the full fill metadata to a trade-level ledger.

Paper vs live is ONE branch: the book source and whether a real order is sent.
Everything else -- risk, tracking, ledger -- is identical, so what you soak in
paper is what runs live.

v1 is paper-mode only. Live-mode order submission is a stub to be filled for
Istanbul (marked clearly).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path

from risk import Order, RiskState, check_order, evaluate_halts
from positions import Tracker, Fill
from fill_sim import simulate_fill, BookState, FillConfig, FillStatus, BookSource


@dataclass
class Intent:
    """What a signal emits."""
    market_id: int
    token_id: int
    condition_id: str
    side: str                 # "buy" | "sell"
    size_usd: float           # notional to trade at the limit price
    limit_price: float
    allow_extreme: bool = False


class OrderManager:
    def __init__(self, tracker: Tracker, cfg: FillConfig,
                 mode: str = "paper", ledger_path: Path | None = None,
                 fee_bps: float = 0.0):
        assert mode in ("paper", "live")
        self.tracker = tracker
        self.cfg = cfg
        self.mode = mode
        self.fee_bps = fee_bps
        self.ledger_path = ledger_path
        self.ledger: list[dict] = []

    def _risk_state(self) -> RiskState:
        """Build current risk state from the tracker (single source of truth)."""
        st = RiskState(
            live_capital=self.tracker.live_capital,
            day_start_equity=self.tracker.live_capital,   # set by daily reset in soak
            peak_equity=self.tracker.live_capital,
            positions=self.tracker.to_risk_positions())
        return evaluate_halts(st)

    def submit(self, intent: Intent, book: BookState) -> dict:
        """Process one intent against a provided book state. Returns a ledger row."""
        ts = datetime.now(timezone.utc)

        # 1. RISK GATE — the non-negotiable checkpoint
        order = Order(market_id=intent.market_id, token_id=intent.token_id,
                      side=intent.side, size_usd=intent.size_usd,
                      limit_price=intent.limit_price,
                      allow_extreme=intent.allow_extreme)
        risk = check_order(order, self._risk_state())
        if not risk.allowed:
            return self._log(intent, ts, status="risk_rejected",
                             reason=risk.reason)

        # 2/3. FILL — paper: simulate against recorded book; live: send to venue
        shares_req = intent.size_usd / intent.limit_price if intent.limit_price > 0 else 0.0
        if self.mode == "paper":
            fr = simulate_fill(intent.market_id, intent.token_id, intent.side,
                               shares_req, intent.limit_price, book, self.cfg,
                               fee_bps=self.fee_bps)
        else:
            raise NotImplementedError(
                "live order submission stub — implement venue POST for Istanbul")

        if fr.fill_status is FillStatus.REJECTED:
            return self._log(intent, ts, status="fill_rejected",
                             reason=fr.rejection_reason,
                             book_source=fr.book_source.value)

        # 4. REPORT FILL to the tracker
        filled_usd = fr.filled_shares * fr.avg_fill_price
        self.tracker.apply_fill(Fill(
            market_id=intent.market_id, token_id=intent.token_id,
            condition_id=intent.condition_id, side=intent.side,
            size_usd=filled_usd, price=fr.avg_fill_price,
            fee_usd=fr.fees, ts=ts))

        # 5. LEDGER
        return self._log(intent, ts, status=fr.fill_status.value,
                         book_source=fr.book_source.value,
                         requested_shares=fr.requested_shares,
                         filled_shares=fr.filled_shares,
                         unfilled_shares=fr.unfilled_shares,
                         avg_fill_price=fr.avg_fill_price,
                         worst_fill_price=fr.worst_fill_price,
                         fees=fr.fees, book_age_s=fr.book_age_s,
                         liquidity_haircut=fr.liquidity_haircut)

    def _log(self, intent: Intent, ts, **fields) -> dict:
        row = {"ts": ts.isoformat(), "market_id": intent.market_id,
               "token_id": intent.token_id, "side": intent.side,
               "size_usd": intent.size_usd, "limit_price": intent.limit_price,
               "mode": self.mode, **fields}
        self.ledger.append(row)
        if self.ledger_path:
            with open(self.ledger_path, "a") as f:
                f.write(json.dumps(row) + "\n")
        return row

    def ledger_summary(self) -> dict:
        n = len(self.ledger)
        by_status: dict[str, int] = {}
        by_source: dict[str, int] = {}
        for r in self.ledger:
            by_status[r["status"]] = by_status.get(r["status"], 0) + 1
            src = r.get("book_source")
            if src:
                by_source[src] = by_source.get(src, 0) + 1
        return {"orders": n, "by_status": by_status, "by_book_source": by_source}