"""Risk module — the gate every order passes through before placement.

Design contract (from execution-requirements.md sec.1):
  * PURE where it can be: check_order() is a function of (order, state) with no
    network and no side effects, so it is exhaustively unit-testable.
  * Limits are HARD-CODED here, not configurable at call sites.
  * The engine can REFUSE before it can PLACE.

Phase 2A/2B (persistent risk state + correct equity):
  * RiskState now carries EXPLICIT realized_pnl and fee_drag, so a realized loss
    or fees ALONE can move equity and trip halts (not only open-position MTM).
  * equity = starting_capital + realized_pnl + unrealized_pnl - fee_drag
  * day_start_equity and peak_equity are LATCHED (set once / ratchet), never
    recomputed to current on each call. Persistence (load/save) keeps latched
    halt/kill/baseline across process restart.
  * check_order()/evaluate_halts() remain PURE (no DB). Persistence is a
    separate boundary (load_risk_state/save_risk_state/reset_daily_baseline).

Money units: dollars as float. Prices are probabilities in [0,1].
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path

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
    day_start_equity: float                 # equity at start of trading day (LATCHED)
    peak_equity: float                      # high-water mark for drawdown (RATCHETS)
    positions: list[Position] = field(default_factory=list)
    halted_today: bool = False              # daily-loss halt latched
    stopped: bool = False                   # drawdown full-stop latched
    kill: bool = False                      # kill switch engaged
    # ---- explicit P&L components (Phase 2B) ----
    realized_pnl: float = 0.0               # realized gains/losses this day
    fee_drag: float = 0.0                   # cumulative fees this day

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
        """equity = live_capital + realized_pnl + unrealized_pnl - fee_drag.

        With the defaults realized_pnl=0 and fee_drag=0 this reduces EXACTLY to
        the previous formula (live_capital + unrealized_pnl), so existing tests
        are unaffected. Explicit realized_pnl/fee_drag let a realized loss or
        fees alone move equity."""
        return (self.live_capital
                + self.realized_pnl
                + self.unrealized_pnl()
                - self.fee_drag)


@dataclass
class RiskResult:
    decision: Decision
    reason: str = ""

    @property
    def allowed(self) -> bool:
        return self.decision is Decision.ALLOW


def evaluate_halts(state: RiskState) -> RiskState:
    """Update latched halt/stop flags from current equity. Called continuously.
    day_start_equity is NOT modified here (it is the fixed daily baseline set by
    reset_daily_baseline). peak_equity ratchets up only. Flags latch on (never
    cleared here)."""
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


# ==========================================================================
# Persistence boundary (Phase 2A) — keeps latched state across process restart.
# Kept OUT of the pure gate above; the engine calls these at startup / on
# mutation / at daily reset. Postgres-backed (single latched row per key).
# ==========================================================================

_ENV = None
def _connect():
    global _ENV
    import psycopg2
    from dotenv import dotenv_values
    if _ENV is None:
        _ENV = dotenv_values(Path(__file__).resolve().parents[1] / ".env")
    return psycopg2.connect(host="127.0.0.1", port=5432, dbname="quantlab",
                            user="quantlab", password=_ENV["PG_PASSWORD"])


def save_risk_state(state: RiskState, key: str = "live") -> None:
    """Persist the latched fields + baseline. Positions are NOT persisted here
    (they are reconciled from the venue/ledger); this stores the risk LATCH."""
    conn = _connect(); cur = conn.cursor()
    cur.execute("""
        INSERT INTO risk_state (key, trading_day, live_capital, day_start_equity,
            peak_equity, realized_pnl, fee_drag, halted_today, stopped, kill,
            updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
        ON CONFLICT (key) DO UPDATE SET
            trading_day=EXCLUDED.trading_day, live_capital=EXCLUDED.live_capital,
            day_start_equity=EXCLUDED.day_start_equity, peak_equity=EXCLUDED.peak_equity,
            realized_pnl=EXCLUDED.realized_pnl, fee_drag=EXCLUDED.fee_drag,
            halted_today=EXCLUDED.halted_today, stopped=EXCLUDED.stopped,
            kill=EXCLUDED.kill, updated_at=now()""",
        (key, date.today(), state.live_capital, state.day_start_equity,
         state.peak_equity, state.realized_pnl, state.fee_drag,
         state.halted_today, state.stopped, state.kill))
    conn.commit(); cur.close(); conn.close()


def load_risk_state(key: str = "live") -> RiskState | None:
    """Reload the latched state (halts/kill/baseline survive restart). Positions
    are attached separately by the engine from the reconciled ledger. Returns
    None if no state persisted yet."""
    conn = _connect(); cur = conn.cursor()
    cur.execute("""SELECT live_capital, day_start_equity, peak_equity,
        realized_pnl, fee_drag, halted_today, stopped, kill
        FROM risk_state WHERE key=%s""", (key,))
    row = cur.fetchone(); cur.close(); conn.close()
    if not row:
        return None
    lc, dse, pk, rp, fd, halt, stop, kill = row
    return RiskState(live_capital=float(lc), day_start_equity=float(dse),
                     peak_equity=float(pk), realized_pnl=float(rp),
                     fee_drag=float(fd), halted_today=halt, stopped=stop, kill=kill)


def reset_daily_baseline(state: RiskState, key: str = "live") -> RiskState:
    """Start a new trading day: fix day_start_equity to current equity, reset
    peak to it, clear the DAILY halt and daily P&L accumulators. Does NOT clear
    the drawdown stop or operator kill (those require explicit review/reset)."""
    eq = state.current_equity()
    state.day_start_equity = eq
    state.peak_equity = max(eq, eq)
    state.halted_today = False
    state.realized_pnl = 0.0
    state.fee_drag = 0.0
    save_risk_state(state, key)
    return state