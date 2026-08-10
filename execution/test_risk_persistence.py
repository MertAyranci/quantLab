"""Phase 2A/2B failing tests — persistent, latched risk state + correct equity.

These encode the roadmap's requirements and FAIL against the current code:
  * a halt/kill must survive process restart (persistence)
  * the daily baseline and peak must be SET ONCE and held, not recomputed
  * equity = starting_capital + realized_pnl + unrealized_pnl - fees
  * a realized loss ALONE (no open positions) must be able to trip the halt
  * fees reduce equity

Run:  .venv/bin/python -m pytest execution/test_risk_persistence.py -v

Written red-first per the roadmap: these define the contract Phase 2A/2B must
satisfy. Do NOT weaken a test to make it pass — fix the risk module.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from risk import (  # noqa: E402
    RiskState, Position, evaluate_halts, DAILY_LOSS_HALT, DRAWDOWN_STOP,
)

# These imports are expected to NOT EXIST yet — that's the point (red first).
# Phase 2A must add a persistence layer exposing load/save + explicit pnl fields.
try:
    from risk import load_risk_state, save_risk_state, reset_daily_baseline  # noqa
    HAS_PERSISTENCE = True
except ImportError:
    HAS_PERSISTENCE = False


# --------------------------------------------------------------------------
# Phase 2B — correct equity semantics
#   equity = starting_capital + realized_pnl + unrealized_pnl - fees
# --------------------------------------------------------------------------

def test_realized_loss_alone_trips_daily_halt():
    """Roadmap mandatory example:
        starting $100, realized -$3, no open positions, 2% threshold -> HALT.
    Fails now because current_equity ignores an explicit realized_pnl field and
    folds it implicitly into live_capital via the tracker."""
    st = RiskState(live_capital=100.0, day_start_equity=100.0, peak_equity=100.0)
    # the engine must be able to record a realized loss + fees explicitly:
    st.realized_pnl = -3.0        # <-- field expected to exist (Phase 2B)
    st.fee_drag = 0.0
    evaluate_halts(st)
    assert st.halted_today, "realized -$3 on $100 (2%) must halt"


def test_fees_reduce_equity():
    st = RiskState(live_capital=100.0, day_start_equity=100.0, peak_equity=100.0)
    st.realized_pnl = 0.0
    st.fee_drag = 2.5
    assert st.current_equity() == 97.5, "fees must reduce equity"


def test_realized_and_unrealized_combine_in_equity():
    st = RiskState(live_capital=100.0, day_start_equity=100.0, peak_equity=100.0,
                   positions=[Position(1, 10, 20.0, 0.90, 1.00)])  # -$2 unreal
    st.realized_pnl = -1.0
    st.fee_drag = 0.5
    # equity = 100 (start) + (-1 realized) + (-2 unreal) - 0.5 fees = 96.5
    assert abs(st.current_equity() - 96.5) < 1e-6, st.current_equity()


def test_realized_gain_updates_peak():
    st = RiskState(live_capital=100.0, day_start_equity=100.0, peak_equity=100.0)
    st.realized_pnl = 10.0
    st.fee_drag = 0.0
    evaluate_halts(st)
    assert st.peak_equity >= 110.0, "peak must rise with realized gains"


# --------------------------------------------------------------------------
# Phase 2A — latched baseline (set once, not recomputed every call)
# --------------------------------------------------------------------------

def test_daily_baseline_does_not_chase_equity():
    """The daily-loss baseline must be FIXED at day start, not reset to current
    equity on each evaluation (the current _risk_state bug). Otherwise a slow
    bleed never trips the halt."""
    st = RiskState(live_capital=100.0, day_start_equity=100.0, peak_equity=100.0)
    st.realized_pnl = -1.0; st.fee_drag = 0.0
    evaluate_halts(st)                      # equity 99, baseline still 100
    st.realized_pnl = -2.5; st.fee_drag = 0.0
    evaluate_halts(st)                      # equity 97.5, -2.5% from FIXED 100
    assert st.halted_today, "cumulative bleed vs FIXED baseline must halt at 2%"


# --------------------------------------------------------------------------
# Phase 2A — persistence across restart
# --------------------------------------------------------------------------

def test_halt_survives_restart():
    if not HAS_PERSISTENCE:
        raise AssertionError("no persistence layer yet (load/save_risk_state)")
    st = RiskState(live_capital=100.0, day_start_equity=100.0, peak_equity=100.0)
    st.halted_today = True
    save_risk_state(st, key="test_restart")
    reloaded = load_risk_state(key="test_restart")
    assert reloaded.halted_today, "halt must survive restart"


def test_kill_survives_restart():
    if not HAS_PERSISTENCE:
        raise AssertionError("no persistence layer yet")
    st = RiskState(live_capital=100.0, day_start_equity=100.0, peak_equity=100.0)
    st.kill = True
    save_risk_state(st, key="test_kill")
    assert load_risk_state(key="test_kill").kill, "kill must survive restart"


def test_restart_preserves_daily_baseline():
    if not HAS_PERSISTENCE:
        raise AssertionError("no persistence layer yet")
    st = RiskState(live_capital=100.0, day_start_equity=123.0, peak_equity=150.0)
    save_risk_state(st, key="test_baseline")
    r = load_risk_state(key="test_baseline")
    assert r.day_start_equity == 123.0 and r.peak_equity == 150.0, \
        "baseline + peak must survive restart, not reset to current"


def _run_all():
    fns = [v for k, v in sorted(globals().items())
           if k.startswith("test_") and callable(v)]
    passed = failed = 0
    for fn in fns:
        try:
            fn(); print(f"  PASS {fn.__name__}"); passed += 1
        except AssertionError as e:
            print(f"  FAIL {fn.__name__}: {e}"); failed += 1
    print(f"\n{passed}/{passed+failed} passed  ({failed} failing — expected red)")
    return failed


if __name__ == "__main__":
    sys.exit(1 if _run_all() else 0)
    