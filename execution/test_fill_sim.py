"""Tests for the fill simulator — proves the book-walking and rejection logic
before it drives any paper (or backtest) fills.

Run: .venv/bin/python execution/test_fill_sim.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fill_sim import (  # noqa: E402
    simulate_fill, BookState, FillConfig, BookSource, FillStatus,
)


def base_cfg():
    return FillConfig.base()


# ---- full-depth walking (the spec's worked example) ------------------------

def test_full_depth_walks_levels():
    book = BookState(
        full_asks=[(0.970, 200), (0.972, 500), (0.975, 100), (0.980, 1000)],
        full_age_s=5.0)
    r = simulate_fill(1, 10, "buy", 1000, 0.975, book, base_cfg())
    # 200@.970 + 500@.972 + 100@.975 = 800 filled, 200 unfilled (0.980 > limit)
    assert r.book_source is BookSource.FULL_DEPTH
    assert abs(r.filled_shares - 800) < 1e-6
    assert abs(r.unfilled_shares - 200) < 1e-6
    assert r.fill_status is FillStatus.PARTIAL
    assert abs(r.worst_fill_price - 0.975) < 1e-6
    # avg = (200*.970 + 500*.972 + 100*.975)/800
    exp_avg = (200*0.970 + 500*0.972 + 100*0.975) / 800
    assert abs(r.avg_fill_price - exp_avg) < 1e-6


def test_full_depth_complete_fill():
    book = BookState(full_asks=[(0.97, 500), (0.98, 500)], full_age_s=1.0)
    r = simulate_fill(1, 10, "buy", 300, 0.98, book, base_cfg())
    assert r.fill_status is FillStatus.FILLED
    assert abs(r.filled_shares - 300) < 1e-6
    assert abs(r.avg_fill_price - 0.97) < 1e-6   # all from first level


def test_full_depth_limit_blocks():
    book = BookState(full_asks=[(0.98, 500)], full_age_s=1.0)
    r = simulate_fill(1, 10, "buy", 100, 0.97, book, base_cfg())   # limit below ask
    assert r.fill_status is FillStatus.REJECTED
    assert "did not cross" in r.rejection_reason


def test_full_depth_sell_walks_bids():
    book = BookState(full_bids=[(0.96, 300), (0.95, 300)], full_age_s=1.0)
    r = simulate_fill(1, 10, "sell", 400, 0.95, book, base_cfg())
    assert abs(r.filled_shares - 400) < 1e-6
    # 300@.96 + 100@.95
    assert abs(r.worst_fill_price - 0.95) < 1e-6


# ---- staleness / source selection ------------------------------------------

def test_stale_full_book_falls_back_to_bbo():
    book = BookState(
        full_asks=[(0.97, 1000)], full_age_s=120.0,      # older than 60s TTL
        best_ask=0.975, best_ask_size=400, bbo_age_s=3.0)
    r = simulate_fill(1, 10, "buy", 100, 0.98, book, base_cfg())
    assert r.book_source is BookSource.BBO_FALLBACK
    # base haircut 0.5 -> usable 200, want 100 -> filled 100
    assert abs(r.filled_shares - 100) < 1e-6
    assert abs(r.avg_fill_price - 0.975) < 1e-6


def test_both_stale_rejects():
    book = BookState(
        full_asks=[(0.97, 1000)], full_age_s=120.0,
        best_ask=0.975, best_ask_size=400, bbo_age_s=30.0)   # both past TTL
    r = simulate_fill(1, 10, "buy", 100, 0.98, book, base_cfg())
    assert r.fill_status is FillStatus.REJECTED
    assert r.book_source is BookSource.NONE


# ---- BBO haircut behaviour -------------------------------------------------

def test_bbo_haircut_limits_size():
    book = BookState(best_ask=0.97, best_ask_size=100, bbo_age_s=2.0)
    r = simulate_fill(1, 10, "buy", 100, 0.98, book, FillConfig.base())
    assert abs(r.filled_shares - 50) < 1e-6      # 100 * 0.5 haircut
    assert r.fill_status is FillStatus.PARTIAL


def test_bbo_haircut_scenarios():
    book = lambda: BookState(best_ask=0.97, best_ask_size=100, bbo_age_s=2.0)
    opt = simulate_fill(1, 10, "buy", 100, 0.98, book(), FillConfig.optimistic())
    con = simulate_fill(1, 10, "buy", 100, 0.98, book(), FillConfig.conservative())
    assert abs(opt.filled_shares - 100) < 1e-6   # 100% haircut
    assert abs(con.filled_shares - 25) < 1e-6    # 25% haircut


def test_bbo_limit_must_cross():
    book = BookState(best_ask=0.98, best_ask_size=100, bbo_age_s=2.0)
    r = simulate_fill(1, 10, "buy", 100, 0.97, book, base_cfg())   # limit < ask
    assert r.fill_status is FillStatus.REJECTED


# ---- validity gates --------------------------------------------------------

def test_closed_market_rejected():
    book = BookState(market_open=False, full_asks=[(0.97, 100)], full_age_s=1.0)
    r = simulate_fill(1, 10, "buy", 50, 0.98, book, base_cfg())
    assert r.fill_status is FillStatus.REJECTED and "closed" in r.rejection_reason


def test_collector_gap_rejected():
    book = BookState(collector_gap=True, full_asks=[(0.97, 100)], full_age_s=1.0)
    r = simulate_fill(1, 10, "buy", 50, 0.98, book, base_cfg())
    assert r.fill_status is FillStatus.REJECTED and "gap" in r.rejection_reason


def test_empty_book_rejected():
    book = BookState(bbo_age_s=2.0)   # no full, no BBO prices
    r = simulate_fill(1, 10, "buy", 50, 0.98, book, base_cfg())
    assert r.fill_status is FillStatus.REJECTED


def test_never_infers_deeper_than_displayed():
    # BBO shows only 40 usable after haircut; requesting 1000 fills only 40
    book = BookState(best_ask=0.97, best_ask_size=80, bbo_age_s=2.0)
    r = simulate_fill(1, 10, "buy", 1000, 0.98, book, FillConfig.base())
    assert abs(r.filled_shares - 40) < 1e-6      # 80*0.5, NOT 1000
    assert r.fill_status is FillStatus.PARTIAL


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