"""Exhaustive tests for the risk module.

Run:  .venv/bin/python -m pytest execution/test_risk.py -v
  or: .venv/bin/python execution/test_risk.py   (runs a plain assert harness)

The point of this file: PROVE the gate refuses every over-limit order before any
code that can touch real money is written. If a test here fails, the risk module
is not trusted and nothing goes live.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from risk import (  # noqa: E402
    Order, Position, RiskState, Decision,
    check_order, evaluate_halts,
    MAX_PER_MARKET, MAX_TOTAL_DEPLOYED, DAILY_LOSS_HALT, DRAWDOWN_STOP,
)


def fresh_state(capital=500.0, positions=None):
    return RiskState(
        live_capital=capital,
        day_start_equity=capital,
        peak_equity=capital,
        positions=positions or [],
    )


def buy(market_id=1, size=50.0, price=0.97, extreme=False):
    return Order(market_id=market_id, token_id=market_id * 10,
                 side="buy", size_usd=size, limit_price=price,
                 allow_extreme=extreme)


# ---- basic allow -----------------------------------------------------------

def test_clean_order_allowed():
    r = check_order(buy(size=50), fresh_state())
    assert r.allowed, r.reason


def test_zero_size_rejected():
    r = check_order(buy(size=0), fresh_state())
    assert not r.allowed and "size" in r.reason


def test_price_out_of_unit_interval_rejected():
    assert not check_order(buy(price=0.0), fresh_state()).allowed
    assert not check_order(buy(price=1.0), fresh_state()).allowed
    assert not check_order(buy(price=1.5), fresh_state()).allowed


# ---- price sanity ----------------------------------------------------------

def test_extreme_price_rejected_without_flag():
    r = check_order(buy(price=0.995), fresh_state())
    assert not r.allowed and "allow_extreme" in r.reason


def test_extreme_price_allowed_with_flag():
    r = check_order(buy(price=0.995, extreme=True), fresh_state())
    assert r.allowed, r.reason


# ---- per-market cap --------------------------------------------------------

def test_per_market_cap_blocks_oversize():
    r = check_order(buy(size=MAX_PER_MARKET + 1), fresh_state())
    assert not r.allowed and "per-market" in r.reason


def test_per_market_cap_accumulates():
    # already $80 in market 1; a $30 buy would make $110 > $100
    st = fresh_state(positions=[Position(1, 10, 80.0, 0.96, 0.95)])
    r = check_order(buy(market_id=1, size=30), st)
    assert not r.allowed and "per-market" in r.reason


def test_per_market_cap_exact_boundary_allowed():
    st = fresh_state(positions=[Position(1, 10, 60.0, 0.96, 0.95)])
    r = check_order(buy(market_id=1, size=40), st)   # exactly $100
    assert r.allowed, r.reason


# ---- total deployed cap ----------------------------------------------------

def test_total_deployed_cap():
    # five markets at $100 each = $500 already deployed; any new buy exceeds
    pos = [Position(i, i * 10, 100.0, 0.96, 0.95) for i in range(1, 6)]
    st = fresh_state(positions=pos)
    r = check_order(buy(market_id=99, size=10), st)
    assert not r.allowed and "total deployed" in r.reason


def test_total_deployed_boundary_allowed():
    pos = [Position(i, i * 10, 100.0, 0.96, 0.95) for i in range(1, 5)]  # $400
    st = fresh_state(positions=pos)
    r = check_order(buy(market_id=99, size=100), st)   # -> $500 exactly
    assert r.allowed, r.reason


# ---- global stops ----------------------------------------------------------

def test_kill_switch_blocks_everything():
    st = fresh_state()
    st.kill = True
    assert not check_order(buy(size=1), st).allowed


def test_daily_halt_blocks():
    st = fresh_state()
    st.halted_today = True
    assert not check_order(buy(size=1), st).allowed


def test_drawdown_stop_blocks():
    st = fresh_state()
    st.stopped = True
    assert not check_order(buy(size=1), st).allowed


# ---- halt evaluation (conservative MTM) ------------------------------------

def test_daily_loss_triggers_halt():
    # $500 capital, 2% = $10 daily loss budget. A position marked down $12.
    # entry 0.97, exit 0.94 -> shares = 100/0.97 = 103.1; loss = 103.1*0.03 = 3.09
    # need a bigger position: $400 at entry 0.97, exit 0.94
    st = fresh_state(positions=[Position(1, 10, 400.0, 0.94, 0.97)])
    evaluate_halts(st)
    # shares = 400/0.97 = 412.4; loss = 412.4 * (0.94-0.97) = -12.37 < -10
    assert st.halted_today, "should halt on >2% daily loss"


def test_small_loss_no_halt():
    st = fresh_state(positions=[Position(1, 10, 50.0, 0.97, 0.97)])
    evaluate_halts(st)
    assert not st.halted_today


def test_drawdown_triggers_stop():
    # capital 500, peak 500, 10% = $50 drawdown. Mark positions down $60.
    st = fresh_state(positions=[Position(1, 10, 400.0, 0.855, 0.97)])
    # shares = 412.4; loss = 412.4*(0.855-0.97) = -47.4 ... push harder:
    st.positions = [Position(1, 10, 400.0, 0.82, 0.97)]
    evaluate_halts(st)
    # shares 412.4 * (0.82-0.97) = -61.9 -> drawdown 61.9 > 50
    assert st.stopped, "should full-stop on >10% drawdown"


# ---- conservative marking sanity ------------------------------------------

def test_conservative_mtm_uses_exit_price():
    # a long marked at bid (exit) below entry shows a loss, not a gain
    st = fresh_state(positions=[Position(1, 10, 100.0, 0.95, 0.98)])
    assert st.unrealized_pnl() < 0   # exit 0.95 < entry 0.98 -> loss


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
    print(f"\n{passed}/{len(fns)} passed")
    return passed == len(fns)


if __name__ == "__main__":
    ok = _run_all()
    sys.exit(0 if ok else 1)