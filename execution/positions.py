"""Position & P&L tracker — the engine's memory of what it holds.

Design (execution-requirements.md sec.2):
  * Holds open positions; applies fills (partial/full), tracking avg entry.
  * P&L three ways: realized, unrealized (CONSERVATIVE mark), fee drag (separate).
  * Settles positions on resolution (YES win -> $1, loss -> $0).
  * Resolution sweep: confirm resolution DIRECTLY from Gamma by condition_id,
    independent of the harvester watchlist. A position that resolves unnoticed
    is an unreconciled position.
  * Produces the Position list the risk module consumes (same shape/convention).

Prices are probabilities in [0,1]. Money is dollars (small notionals).
The tracker holds state; it does not place orders and (except the sweep) makes
no network calls, so its accounting is unit-testable.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone

import httpx

# reuse the risk module's Position shape so the two stay in lockstep
from risk import Position

GAMMA = "https://gamma-api.polymarket.com"
UA = {"User-Agent": "quant-lab-exec/0.1"}


@dataclass
class Fill:
    market_id: int
    token_id: int
    condition_id: str
    side: str                 # "buy" | "sell"
    size_usd: float           # notional filled at price
    price: float              # fill price (probability)
    fee_usd: float
    ts: datetime


@dataclass
class OpenPosition:
    market_id: int
    token_id: int
    condition_id: str
    shares: float             # number of contracts held (>=0)
    cost_basis_usd: float     # total $ paid for those shares (net of sells)
    avg_entry: float          # cost_basis / shares
    mark: float               # CONSERVATIVE current price (bid for a long)
    opened: datetime

    def notional(self) -> float:
        # current cost-basis exposure (what the risk module caps on)
        return self.cost_basis_usd

    def unrealized(self) -> float:
        # conservative: value shares at the mark (exit) price
        return self.shares * self.mark - self.cost_basis_usd


@dataclass
class Tracker:
    live_capital: float
    realized_pnl: float = 0.0
    fee_drag: float = 0.0
    positions: dict[int, OpenPosition] = field(default_factory=dict)  # by market_id
    settled: list[dict] = field(default_factory=list)

    # ---- fills -------------------------------------------------------------

    def apply_fill(self, f: Fill):
        self.fee_drag += f.fee_usd
        shares = f.size_usd / f.price if f.price > 0 else 0.0
        pos = self.positions.get(f.market_id)
        if f.side == "buy":
            if pos is None:
                self.positions[f.market_id] = OpenPosition(
                    market_id=f.market_id, token_id=f.token_id,
                    condition_id=f.condition_id, shares=shares,
                    cost_basis_usd=f.size_usd, avg_entry=f.price,
                    mark=f.price, opened=f.ts)
            else:
                pos.shares += shares
                pos.cost_basis_usd += f.size_usd
                pos.avg_entry = pos.cost_basis_usd / pos.shares if pos.shares else 0.0
        elif f.side == "sell":
            if pos is None:
                return  # can't sell what we don't track; order mgr guards this
            # realize P&L on the sold shares at the sell price vs avg entry
            sold = min(shares, pos.shares)
            realized = sold * (f.price - pos.avg_entry)
            self.realized_pnl += realized
            pos.shares -= sold
            pos.cost_basis_usd = pos.shares * pos.avg_entry
            if pos.shares <= 1e-9:
                del self.positions[f.market_id]

    # ---- marking -----------------------------------------------------------

    def update_mark(self, market_id: int, conservative_price: float):
        pos = self.positions.get(market_id)
        if pos is not None:
            pos.mark = conservative_price

    # ---- settlement --------------------------------------------------------

    def settle(self, market_id: int, won: bool):
        """Resolve a held position: winning shares pay $1, losers pay $0."""
        pos = self.positions.get(market_id)
        if pos is None:
            return
        payout = pos.shares * (1.0 if won else 0.0)
        realized = payout - pos.cost_basis_usd
        self.realized_pnl += realized
        self.settled.append({
            "market_id": market_id, "won": won, "shares": pos.shares,
            "cost_basis": pos.cost_basis_usd, "payout": payout,
            "realized": realized, "ts": datetime.now(timezone.utc).isoformat()})
        del self.positions[market_id]

    # ---- resolution sweep (canonical requirement) --------------------------

    def resolution_sweep(self, http: httpx.Client | None = None) -> list[dict]:
        """For every open position, ask Gamma directly whether it resolved.
        Independent of the harvester watchlist. Settles any that have.
        Returns a list of actions taken (for logging/alerting)."""
        client = http or httpx.Client(headers=UA, timeout=30)
        actions = []
        for market_id, pos in list(self.positions.items()):
            try:
                r = client.get(f"{GAMMA}/markets",
                               params={"condition_ids": pos.condition_id})
                data = r.json() if r.status_code == 200 else []
            except (httpx.HTTPError, json.JSONDecodeError):
                actions.append({"market_id": market_id, "action": "sweep_error"})
                continue
            if not data:
                continue
            m = data[0]
            status = (m.get("umaResolutionStatus") or "").lower()
            prices = m.get("outcomePrices")
            if status != "resolved" or not prices:
                continue
            try:
                op = json.loads(prices) if isinstance(prices, str) else prices
                # YES token is index 0 by our convention; won if its price == "1"
                won = str(op[0]) == "1"
            except (json.JSONDecodeError, IndexError, TypeError):
                actions.append({"market_id": market_id, "action": "parse_error"})
                continue
            self.settle(market_id, won)
            actions.append({"market_id": market_id, "action": "settled",
                            "won": won})
        return actions

    # ---- P&L views ---------------------------------------------------------

    def open_exposure(self) -> float:
        return sum(p.notional() for p in self.positions.values())

    def unrealized_pnl(self) -> float:
        return sum(p.unrealized() for p in self.positions.values())

    def total_pnl(self) -> float:
        # realized already net of fees? No — fees tracked separately; subtract.
        return self.realized_pnl + self.unrealized_pnl() - self.fee_drag

    def capital_utilization(self) -> float:
        if self.live_capital <= 0:
            return 0.0
        return self.open_exposure() / self.live_capital

    # ---- bridge to the risk module -----------------------------------------

    def to_risk_positions(self) -> list[Position]:
        """Convert to the risk module's Position shape (for cap/halt checks)."""
        out = []
        for p in self.positions.values():
            out.append(Position(
                market_id=p.market_id, token_id=p.token_id,
                size_usd=p.cost_basis_usd, exit_price=p.mark,
                entry_price=p.avg_entry))
        return out

    def summary(self) -> dict:
        return {
            "open_positions": len(self.positions),
            "open_exposure": round(self.open_exposure(), 2),
            "realized_pnl": round(self.realized_pnl, 2),
            "unrealized_pnl": round(self.unrealized_pnl(), 2),
            "fee_drag": round(self.fee_drag, 2),
            "total_pnl": round(self.total_pnl(), 2),
            "capital_utilization": round(self.capital_utilization(), 3),
        }