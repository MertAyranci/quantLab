"""Risk module — the gate every order passes through before placement.

Design contract (from execution-requirements.md sec.1):
  * PURE where it can be: check_order() is a function of (order, state) with no
    network and no side effects, so it is exhaustively unit-testable.
  * Limits are HARD-CODED here, not configurable at call sites. Raising them is
    a deliberate code change, reviewed, after clean operation.
  * The engine can REFUSE before it can PLACE. Nothing places an order without
    a passing check_order().

P&L convention for halts (operator's choice):
  realized + CONSERVATIVE mark-to-market. Open positions are marked at the price
  you could actually exit at NOW (bid for longs), never mid/last — so halts fire
  on losses you could really realize, not optimistic marks.

Money units: dollars as float for readability at this layer (position sizes are
small, <$2.5k). Prices are probabilities in [0,1].
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


# ---- hard-coded limits (raise only by editing here, after clean operation) ----

MAX_PER_MARKET = 100.0        # $ open exposure in one market
MAX_TOTAL_DEPLOYED = 500.0    # $ open exposure across all markets
DAILY_LOSS_HALT = 0.02        # 2% of live capital -> halt new orders for the day
DRAWDOWN_STOP = 0.10          # 10% of live capital -> full stop, manual review
PRICE_MIN = 0.01              # reject fills outside [1c, 99c] unless flagged
PRICE_MAX = 0.99


class Decision(Enum):
    ALLOW = "allow"
    REJECT = "reject"


@dataclass(frozen=True)
class Order:
    market_id: int
    token_id: int
    side: str                 # "buy" | "sell"
    size_usd: float           # notional at the limit price
    limit_price: float        # probability [0,1]
    allow_extreme: bool = False   # explicit opt-in for <1c / >99c (near-cert strat)


@dataclass
class Position:
    market_id: int
    token_id: int
    size_usd: float           # current open notional (cost basis)
    exit_price: float         # CONSERVATIVE current exit price (bid for a long)
    entry_price: float


@dataclass
class RiskState:
    live_capital: float                     # capital allocated to live trading
    day_start_equity: float                 # equity at start of trading day
    peak_equity: float                      # high-water mark for drawdown
    positions: list[Position] = field(default_factory=list)
    halted_today: bool = False              # daily-loss halt latched
    stopped: bool = False                   # drawdown full-stop latched
    kill: bool = False                      # kill switch engaged

    # ---- derived P&L (realized + conservative MTM) ----

    def open_exposure(self) -> float:
        return sum(p.size_usd for p in self.positions)

    def exposure_in_market(self, market_id: int) -> float:
        return sum(p.size_usd for p in self.positions if p.market_id == market_id)

    def unrealized_pnl(self) -> float:
        # conservative: mark each long at its exit (bid) price
        pnl = 0.0
        for p in self.positions:
            if p.entry_price > 0:
                shares = p.size_usd / p.entry_price
                pnl += shares * (p.exit_price - p.entry_price)
        return pnl

    def current_equity(self) -> float:
        # equity = capital not at risk + conservative value of open positions.
        # realized P&L is already folded into live_capital by the tracker; here
        # we add the conservative MTM of open positions.
        return self.live_capital + self.unrealized_pnl()


@dataclass
class RiskResult:
    decision: Decision
    reason: str = ""

    @property
    def allowed(self) -> bool:
        return self.decision is Decision.ALLOW


def evaluate_halts(state: RiskState) -> RiskState:
    """Update latched halt/stop flags from current equity. Called continuously,
    not just per-order. Returns the same state (mutated) for convenience."""
    equity = state.current_equity()
    if equity > state.peak_equity:
        state.peak_equity = equity

    daily_change = equity - state.day_start_equity
    if daily_change <= -DAILY_LOSS_HALT * state.live_capital:
        state.halted_today = True

    drawdown = (state.peak_equity - equity)
    if drawdown >= DRAWDOWN_STOP * state.live_capital:
        state.stopped = True

    return state


def check_order(order: Order, state: RiskState) -> RiskResult:
    """The gate. Returns ALLOW or REJECT(reason). No side effects, no network."""

    # 0. global stops (checked first — they override everything)
    if state.kill:
        return RiskResult(Decision.REJECT, "kill switch engaged")
    if state.stopped:
        return RiskResult(Decision.REJECT, "drawdown full-stop latched")
    if state.halted_today:
        return RiskResult(Decision.REJECT, "daily-loss halt latched")

    # 1. basic validity
    if order.size_usd <= 0:
        return RiskResult(Decision.REJECT, "non-positive size")
    if not (0.0 < order.limit_price < 1.0):
        return RiskResult(Decision.REJECT, f"price {order.limit_price} out of (0,1)")

    # 2. price sanity (fat-finger / stale-quote guard)
    if not order.allow_extreme and not (PRICE_MIN <= order.limit_price <= PRICE_MAX):
        return RiskResult(Decision.REJECT,
                          f"price {order.limit_price} outside [{PRICE_MIN},{PRICE_MAX}] "
                          f"and allow_extreme not set")

    # 3. per-market cap (only buys add exposure; sells reduce it)
    if order.side == "buy":
        new_market = state.exposure_in_market(order.market_id) + order.size_usd
        if new_market > MAX_PER_MARKET + 1e-9:
            return RiskResult(Decision.REJECT,
                              f"per-market cap: {new_market:.2f} > {MAX_PER_MARKET}")

        # 4. total deployed cap
        new_total = state.open_exposure() + order.size_usd
        if new_total > MAX_TOTAL_DEPLOYED + 1e-9:
            return RiskResult(Decision.REJECT,
                              f"total deployed cap: {new_total:.2f} > {MAX_TOTAL_DEPLOYED}")

    return RiskResult(Decision.ALLOW, "ok")


def flatten_reason(state: RiskState) -> str | None:
    """Why the engine should flatten/halt, if any (for alerts)."""
    if state.kill:
        return "kill switch"
    if state.stopped:
        return "drawdown full-stop"
    if state.halted_today:
        return "daily-loss halt"
    return None