"""Tests for the position & P&L tracker.

Run:  .venv/bin/python execution/test_positions.py

Proves the accounting is correct BEFORE it feeds real money decisions:
avg-entry across partials, conservative marking, resolution settlement,
fee drag, and the bridge to the risk module.
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from positions import Tracker, Fill, OpenPosition  # noqa: E402
from risk import check_order, Order, RiskState  # noqa: E402


def now():
    return datetime.now(timezone.utc)


def fill(mid=1, side="buy", size=50.0, price=0.96, fee=0.5, cond="0xabc"):
    return Fill(market_id=mid, token_id=mid * 10, condition_id=cond,
                side=side, size_usd=size, price=price, fee_usd=fee, ts=now())


# ---- fills & averaging -----------------------------------------------------

def test_single_buy_opens_position():
    t = Tracker(live_capital=500)
    t.apply_fill(fill(size=48.0, price=0.96))
    p = t.positions[1]
    assert abs(p.shares - 50.0) < 1e-6          # 48/0.96 = 50 shares
    assert abs(p.cost_basis_usd - 48.0) < 1e-6
    assert abs(p.avg_entry - 0.96) < 1e-6


def test_partial_fills_average_entry():
    t = Tracker(live_capital=500)
    t.apply_fill(fill(size=48.0, price=0.96))    # 50 sh @ .96
    t.apply_fill(fill(size=49.0, price=0.98))    # 50 sh @ .98
    p = t.positions[1]
    assert abs(p.shares - 100.0) < 1e-6
    assert abs(p.cost_basis_usd - 97.0) < 1e-6
    assert abs(p.avg_entry - 0.97) < 1e-6        # 97/100


def test_fee_drag_accumulates():
    t = Tracker(live_capital=500)
    t.apply_fill(fill(fee=0.5))
    t.apply_fill(fill(fee=0.3))
    assert abs(t.fee_drag - 0.8) < 1e-6


# ---- conservative marking --------------------------------------------------

def test_unrealized_uses_conservative_mark():
    t = Tracker(live_capital=500)
    t.apply_fill(fill(size=97.0, price=0.97))    # 100 sh @ .97
    t.update_mark(1, 0.95)                        # bid dropped to .95
    # unrealized = 100*0.95 - 97 = -2.0
    assert abs(t.unrealized_pnl() - (-2.0)) < 1e-6


def test_mark_up_shows_gain():
    t = Tracker(live_capital=500)
    t.apply_fill(fill(size=97.0, price=0.97))
    t.update_mark(1, 0.99)
    assert t.unrealized_pnl() > 0


# ---- settlement ------------------------------------------------------------

def test_settle_win_pays_one():
    t = Tracker(live_capital=500)
    t.apply_fill(fill(size=97.0, price=0.97))     # 100 sh, cost 97
    t.settle(1, won=True)                          # payout 100, realized +3
    assert abs(t.realized_pnl - 3.0) < 1e-6
    assert 1 not in t.positions


def test_settle_loss_pays_zero():
    t = Tracker(live_capital=500)
    t.apply_fill(fill(size=97.0, price=0.97))     # cost 97
    t.settle(1, won=False)                          # payout 0, realized -97
    assert abs(t.realized_pnl - (-97.0)) < 1e-6
    assert 1 not in t.positions


# ---- sells realize P&L -----------------------------------------------------

def test_sell_realizes_partial():
    t = Tracker(live_capital=500)
    t.apply_fill(fill(size=97.0, price=0.97))     # 100 sh @ .97
    t.apply_fill(fill(side="sell", size=49.5, price=0.99))  # sell 50 sh @ .99
    # realized = 50*(0.99-0.97) = 1.0
    assert abs(t.realized_pnl - 1.0) < 1e-6
    assert abs(t.positions[1].shares - 50.0) < 1e-6


# ---- P&L aggregation -------------------------------------------------------

def test_total_pnl_nets_fees():
    t = Tracker(live_capital=500)
    t.apply_fill(fill(size=97.0, price=0.97, fee=1.0))
    t.update_mark(1, 0.98)                          # unreal = 100*.98-97 = +1
    # total = realized(0) + unreal(1) - fees(1) = 0
    assert abs(t.total_pnl() - 0.0) < 1e-6


def test_capital_utilization():
    t = Tracker(live_capital=500)
    t.apply_fill(fill(size=100.0, price=0.97))
    assert abs(t.capital_utilization() - 0.2) < 1e-6   # 100/500


# ---- bridge to risk module -------------------------------------------------

def test_bridge_feeds_risk_caps():
    t = Tracker(live_capital=500)
    t.apply_fill(fill(mid=1, size=80.0, price=0.96))
    t.update_mark(1, 0.96)
    st = RiskState(live_capital=500, day_start_equity=500, peak_equity=500,
                   positions=t.to_risk_positions())
    # $80 in market 1; a $30 more buy should exceed the $100 per-market cap
    o = Order(market_id=1, token_id=10, side="buy", size_usd=30, limit_price=0.96)
    r = check_order(o, st)
    assert not r.allowed and "per-market" in r.reason


def _run_all():
    fns = [v for k, v in sorted(globals().items())
           if k.startswith("test_") and callable(v)]
    passed = 0
    for fn in fns:
        try:
            fn()
            print(f"  PASS {fn.__name__}")
            passed += 1
        except AssertionError as e:
            print(f"  FAIL {fn.__name__}: {e}")
        except Exception as e:
            print(f"  ERROR {fn.__name__}: {type(e).__name__}: {e}")
    print(f"\n{passed}/{len(fns)} passed")
    return passed == len(fns)


if __name__ == "__main__":
    sys.exit(0 if _run_all() else 1)