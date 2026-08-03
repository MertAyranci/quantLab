"""Integration tests for the order manager — proves the whole engine interlocks:
risk gate BEFORE fill, fills update the tracker, over-limit orders never fill,
kill switch blocks everything, ledger records the truth.

Run: .venv/bin/python execution/test_order_manager.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from order_manager import OrderManager, Intent  # noqa: E402
from positions import Tracker  # noqa: E402
from fill_sim import BookState, FillConfig, FillStatus  # noqa: E402


def mgr(capital=500.0):
    return OrderManager(Tracker(live_capital=capital), FillConfig.base(), mode="paper")


def good_book():
    return BookState(full_asks=[(0.96, 1000), (0.97, 1000)], full_age_s=2.0)


def intent(mid=1, size=50.0, price=0.97):
    return Intent(market_id=mid, token_id=mid * 10, condition_id=f"0x{mid}",
                  side="buy", size_usd=size, limit_price=price)


# ---- happy path: fill updates tracker --------------------------------------

def test_clean_order_fills_and_updates_tracker():
    m = mgr()
    row = m.submit(intent(size=48.0, price=0.97), good_book())
    assert row["status"] in ("filled", "partial")
    assert m.tracker.positions[1].cost_basis_usd > 0     # tracker got the fill
    assert m.tracker.positions[1].shares > 0


# ---- risk gate runs BEFORE fill --------------------------------------------

def test_oversize_order_rejected_before_fill():
    m = mgr()
    row = m.submit(intent(size=150.0, price=0.97), good_book())  # > $100 cap
    assert row["status"] == "risk_rejected"
    assert "per-market" in row["reason"]
    assert 1 not in m.tracker.positions                   # nothing filled


def test_total_cap_enforced_across_orders():
    m = mgr()
    # fill five markets at $100 each = $500 deployed
    for i in range(1, 6):
        m.submit(intent(mid=i, size=100.0, price=0.97), good_book())
    # sixth market should be blocked by total-deployed cap
    row = m.submit(intent(mid=6, size=50.0, price=0.97), good_book())
    assert row["status"] == "risk_rejected"
    assert "total deployed" in row["reason"]


def test_kill_switch_blocks_via_state():
    # kill lives on RiskState built from tracker; simulate by exhausting drawdown
    m = mgr(capital=500)
    # open a big position then mark it to a big loss so drawdown stop latches
    m.submit(intent(mid=1, size=100.0, price=0.97), good_book())
    m.tracker.update_mark(1, 0.60)      # brutal markdown
    row = m.submit(intent(mid=2, size=50.0, price=0.97), good_book())
    assert row["status"] == "risk_rejected"
    assert "drawdown" in row["reason"] or "halt" in row["reason"]


# ---- fill rejection (bad book) is logged, tracker untouched -----------------

def test_stale_book_fill_rejected():
    m = mgr()
    stale = BookState(full_asks=[(0.96, 1000)], full_age_s=999.0)  # past TTL, no BBO
    row = m.submit(intent(size=50.0), stale)
    assert row["status"] == "fill_rejected"
    assert 1 not in m.tracker.positions


def test_limit_not_crossing_rejected():
    m = mgr()
    book = BookState(full_asks=[(0.98, 1000)], full_age_s=2.0)
    row = m.submit(intent(size=50.0, price=0.97), book)   # limit below ask
    assert row["status"] == "fill_rejected"


# ---- ledger integrity ------------------------------------------------------

def test_ledger_records_all_attempts():
    m = mgr()
    m.submit(intent(mid=1, size=50.0, price=0.97), good_book())   # fills
    m.submit(intent(mid=1, size=150.0, price=0.97), good_book())  # risk reject
    m.submit(intent(mid=2, size=50.0, price=0.97),
             BookState(full_asks=[(0.99, 100)], full_age_s=2.0))  # fill reject
    s = m.ledger_summary()
    assert s["orders"] == 3
    assert sum(s["by_status"].values()) == 3


def _run_all():
    fns = [v for k, v in sorted(globals().items())
           if k.startswith("test_") and callable(v)]
    passed = 0
    for fn in fns:
        try:
            fn(); print(f"  PASS {fn.__name__}"); passed += 1
        except AssertionError as e:
            print(f"  FAIL {fn.__name__}: {e}")
        except Exception as e:
            print(f"  ERROR {fn.__name__}: {type(e).__name__}: {e}")
    print(f"\n{passed}/{len(fns)} passed")
    return passed == len(fns)


if __name__ == "__main__":
    sys.exit(0 if _run_all() else 1)