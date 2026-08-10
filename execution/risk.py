"""Risk module — mandatory gate before any order placement.

Design:
  * check_order() is pure: no network/database side effects.
  * hard risk limits live here, not at strategy call sites.
  * RiskState contains persistent safety state.
  * realized P&L, unrealized P&L and fees all affect equity.
  * day_start_equity is a fixed daily baseline.
  * peak_equity ratchets upward and is not rebuilt per order.
  * halt/stop/kill flags latch until explicitly reset.
  * database persistence is kept outside the pure order gate.

Equity convention:

    equity
      = live_capital
      + realized_pnl
      + unrealized_pnl
      - fee_drag

`live_capital` is the base capital allocation, not a value that is repeatedly
rewritten to absorb realized P&L.

Money values are USD floats at this layer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path


# ---------------------------------------------------------------------------
# Hard risk limits
# ---------------------------------------------------------------------------

MAX_PER_MARKET = 100.0
MAX_TOTAL_DEPLOYED = 500.0

DAILY_LOSS_HALT = 0.02
DRAWDOWN_STOP = 0.10

PRICE_MIN = 0.01
PRICE_MAX = 0.99


# ---------------------------------------------------------------------------
# Core models
# ---------------------------------------------------------------------------

class Decision(Enum):
    ALLOW = "allow"
    REJECT = "reject"


@dataclass(frozen=True)
class Order:
    market_id: int
    token_id: int
    side: str
    size_usd: float
    limit_price: float
    allow_extreme: bool = False


@dataclass
class Position:
    market_id: int
    token_id: int
    size_usd: float
    exit_price: float
    entry_price: float


@dataclass
class RiskState:
    # Base capital allocation.
    live_capital: float

    # Fixed equity level captured at start of current trading day.
    day_start_equity: float

    # High-water mark. Ratchets upward only.
    peak_equity: float

    positions: list[Position] = field(default_factory=list)

    # Latched safety states.
    halted_today: bool = False
    stopped: bool = False
    kill: bool = False

    # Explicit cumulative accounting components.
    #
    # Do NOT fold these into live_capital and also put them here, otherwise
    # realized P&L/fees would be double-counted.
    realized_pnl: float = 0.0
    fee_drag: float = 0.0

    def open_exposure(self) -> float:
        return sum(p.size_usd for p in self.positions)

    def exposure_in_market(self, market_id: int) -> float:
        return sum(
            p.size_usd
            for p in self.positions
            if p.market_id == market_id
        )

    def unrealized_pnl(self) -> float:
        """Conservative MTM: long positions marked at executable exit price."""
        pnl = 0.0

        for p in self.positions:
            if p.entry_price <= 0:
                continue

            shares = p.size_usd / p.entry_price
            pnl += shares * (p.exit_price - p.entry_price)

        return pnl

    def current_equity(self) -> float:
        return (
            self.live_capital
            + self.realized_pnl
            + self.unrealized_pnl()
            - self.fee_drag
        )


@dataclass
class RiskResult:
    decision: Decision
    reason: str = ""

    @property
    def allowed(self) -> bool:
        return self.decision is Decision.ALLOW


# ---------------------------------------------------------------------------
# Halt evaluation
# ---------------------------------------------------------------------------

def evaluate_halts(state: RiskState) -> RiskState:
    """Evaluate and latch daily-loss and drawdown safety states.

    Important:
      * day_start_equity is NEVER rewritten here.
      * peak_equity may only move upward.
      * halted_today and stopped only move False -> True here.
    """

    equity = state.current_equity()

    # High-water mark ratchets upward.
    if equity > state.peak_equity:
        state.peak_equity = equity

    # Daily baseline remains fixed.
    daily_change = equity - state.day_start_equity

    if daily_change <= -(DAILY_LOSS_HALT * state.live_capital):
        state.halted_today = True

    # Maximum drawdown from persistent high-water mark.
    drawdown = state.peak_equity - equity

    if drawdown >= DRAWDOWN_STOP * state.live_capital:
        state.stopped = True

    return state


# ---------------------------------------------------------------------------
# Order gate
# ---------------------------------------------------------------------------

def check_order(order: Order, state: RiskState) -> RiskResult:
    """Mandatory risk gate. Pure: no DB/network side effects."""

    # Global safety states override everything.
    if state.kill:
        return RiskResult(
            Decision.REJECT,
            "kill switch engaged",
        )

    if state.stopped:
        return RiskResult(
            Decision.REJECT,
            "drawdown full-stop latched",
        )

    if state.halted_today:
        return RiskResult(
            Decision.REJECT,
            "daily-loss halt latched",
        )

    # Basic validation.
    if order.size_usd <= 0:
        return RiskResult(
            Decision.REJECT,
            "non-positive size",
        )

    if not (0.0 < order.limit_price < 1.0):
        return RiskResult(
            Decision.REJECT,
            f"price {order.limit_price} out of (0,1)",
        )

    # Fat-finger/stale quote protection.
    if (
        not order.allow_extreme
        and not (PRICE_MIN <= order.limit_price <= PRICE_MAX)
    ):
        return RiskResult(
            Decision.REJECT,
            (
                f"price {order.limit_price} outside "
                f"[{PRICE_MIN},{PRICE_MAX}] "
                "and allow_extreme not set"
            ),
        )

    if order.side == "buy":
        new_market_exposure = (
            state.exposure_in_market(order.market_id)
            + order.size_usd
        )

        if new_market_exposure > MAX_PER_MARKET + 1e-9:
            return RiskResult(
                Decision.REJECT,
                (
                    "per-market cap: "
                    f"{new_market_exposure:.2f} > {MAX_PER_MARKET}"
                ),
            )

        new_total_exposure = (
            state.open_exposure()
            + order.size_usd
        )

        if new_total_exposure > MAX_TOTAL_DEPLOYED + 1e-9:
            return RiskResult(
                Decision.REJECT,
                (
                    "total deployed cap: "
                    f"{new_total_exposure:.2f} > {MAX_TOTAL_DEPLOYED}"
                ),
            )

    return RiskResult(
        Decision.ALLOW,
        "ok",
    )


def flatten_reason(state: RiskState) -> str | None:
    if state.kill:
        return "kill switch"

    if state.stopped:
        return "drawdown full-stop"

    if state.halted_today:
        return "daily-loss halt"

    return None


# ---------------------------------------------------------------------------
# Persistence boundary
# ---------------------------------------------------------------------------

_ENV = None


def _connect():
    """Create PostgreSQL connection lazily.

    DB access intentionally lives outside check_order()/evaluate_halts().
    """
    global _ENV

    import psycopg2
    from dotenv import dotenv_values

    if _ENV is None:
        repo_root = Path(__file__).resolve().parents[1]
        _ENV = dotenv_values(repo_root / ".env")

    password = _ENV.get("PG_PASSWORD")

    if not password:
        raise RuntimeError(
            "PG_PASSWORD missing from repository .env"
        )

    return psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="quantlab",
        user="quantlab",
        password=password,
    )


def save_risk_state(
    state: RiskState,
    key: str = "live",
) -> None:
    """Persist safety state.

    Positions are deliberately NOT stored here. They must eventually come from
    the reconciled position/venue ledger rather than this risk-state table.
    """

    trading_day = datetime.now(timezone.utc).date()

    conn = _connect()

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO risk_state (
                    key,
                    trading_day,
                    live_capital,
                    day_start_equity,
                    peak_equity,
                    realized_pnl,
                    fee_drag,
                    halted_today,
                    stopped,
                    kill,
                    updated_at
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    now()
                )
                ON CONFLICT (key)
                DO UPDATE SET
                    trading_day = EXCLUDED.trading_day,
                    live_capital = EXCLUDED.live_capital,
                    day_start_equity = EXCLUDED.day_start_equity,
                    peak_equity = EXCLUDED.peak_equity,
                    realized_pnl = EXCLUDED.realized_pnl,
                    fee_drag = EXCLUDED.fee_drag,
                    halted_today = EXCLUDED.halted_today,
                    stopped = EXCLUDED.stopped,
                    kill = EXCLUDED.kill,
                    updated_at = now()
                """,
                (
                    key,
                    trading_day,
                    state.live_capital,
                    state.day_start_equity,
                    state.peak_equity,
                    state.realized_pnl,
                    state.fee_drag,
                    state.halted_today,
                    state.stopped,
                    state.kill,
                ),
            )

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


def load_risk_state(
    key: str = "live",
) -> RiskState | None:
    """Load persistent latch/baseline state.

    Positions intentionally return empty here. They are attached by the engine
    from its authoritative/reconciled position source.
    """

    conn = _connect()

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    live_capital,
                    day_start_equity,
                    peak_equity,
                    realized_pnl,
                    fee_drag,
                    halted_today,
                    stopped,
                    kill
                FROM risk_state
                WHERE key = %s
                """,
                (key,),
            )

            row = cur.fetchone()

    finally:
        conn.close()

    if row is None:
        return None

    (
        live_capital,
        day_start_equity,
        peak_equity,
        realized_pnl,
        fee_drag,
        halted_today,
        stopped,
        kill,
    ) = row

    return RiskState(
        live_capital=float(live_capital),
        day_start_equity=float(day_start_equity),
        peak_equity=float(peak_equity),
        positions=[],
        realized_pnl=float(realized_pnl),
        fee_drag=float(fee_drag),
        halted_today=bool(halted_today),
        stopped=bool(stopped),
        kill=bool(kill),
    )


def reset_daily_baseline(
    state: RiskState,
    key: str = "live",
) -> RiskState:
    """Begin a new risk day without creating an accounting discontinuity.

    Clears ONLY the daily-loss halt.

    It deliberately does NOT clear:
      * realized_pnl
      * fee_drag
      * peak_equity
      * stopped
      * kill

    realized_pnl/fee_drag remain cumulative accounting values. The new
    day_start_equity captures current equity and therefore provides the fresh
    daily-loss baseline without changing current equity itself.
    """

    equity = state.current_equity()

    state.day_start_equity = equity

    # Never lower the high-water mark.
    state.peak_equity = max(
        state.peak_equity,
        equity,
    )

    # Daily halt may reset at the new trading day.
    state.halted_today = False

    # stopped and kill intentionally remain latched.

    save_risk_state(
        state,
        key=key,
    )

    return state
