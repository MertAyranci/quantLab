import sys
from pathlib import Path

sys.path.insert(
    0,
    str(
        Path(__file__).resolve().parent
    ),
)

from rn1_f21_shadow_mm import (  # noqa: E402
    AggressorSide,
    OrderState,
    PassiveOrder,
    ShadowConfig,
    Side,
    activate_passive_order,
    advance_time,
    apply_aggressive_trade,
    apply_book_size_change,
    invalidate_for_gap,
    request_cancel,
)


CFG = ShadowConfig(
    placement_latency_ms=500,
    cancel_latency_ms=500,
)


def make_bid(
    *,
    queue=100.0,
    shares=10.0,
):
    o = PassiveOrder(
        order_id="synthetic",
        side=Side.BUY,
        price=0.50,
        shares=shares,
        submit_ts_ms=1000,
        config=CFG,
    )

    activate_passive_order(
        o,
        ts_ms=1500,
        displayed_size_at_price=queue,
        best_bid=0.50,
        best_ask=0.51,
    )

    return o


def make_ask(
    *,
    queue=100.0,
    shares=10.0,
):
    o = PassiveOrder(
        order_id="synthetic",
        side=Side.SELL,
        price=0.50,
        shares=shares,
        submit_ts_ms=1000,
        config=CFG,
    )

    activate_passive_order(
        o,
        ts_ms=1500,
        displayed_size_at_price=queue,
        best_bid=0.49,
        best_ask=0.50,
    )

    return o


def test_queue_initialized_behind_visible_size():
    o = make_bid(
        queue=125.0
    )

    assert o.state is OrderState.RESTING
    assert o.queue_ahead == 125.0
    assert o.filled_shares == 0.0


def test_book_cancellation_gives_no_queue_credit():
    o = make_bid(
        queue=100.0
    )

    apply_book_size_change(
        o,
        ts_ms=1600,
        new_displayed_size_at_price=10.0,
    )

    assert o.queue_ahead == 100.0
    assert o.filled_shares == 0.0


def test_new_liquidity_does_not_move_us_backward():
    o = make_bid(
        queue=100.0
    )

    apply_book_size_change(
        o,
        ts_ms=1600,
        new_displayed_size_at_price=500.0,
    )

    assert o.queue_ahead == 100.0


def test_wrong_aggressor_does_nothing_to_bid():
    o = make_bid(
        queue=100.0
    )

    fill = apply_aggressive_trade(
        o,
        ts_ms=1600,
        aggressor=AggressorSide.BUY,
        trade_price=0.50,
        trade_size=500.0,
    )

    assert fill == 0.0
    assert o.queue_ahead == 100.0


def test_wrong_price_does_nothing():
    o = make_bid(
        queue=100.0
    )

    fill = apply_aggressive_trade(
        o,
        ts_ms=1600,
        aggressor=AggressorSide.SELL,
        trade_price=0.49,
        trade_size=500.0,
    )

    assert fill == 0.0
    assert o.queue_ahead == 100.0


def test_trade_first_consumes_queue_ahead():
    o = make_bid(
        queue=100.0,
        shares=10.0,
    )

    fill = apply_aggressive_trade(
        o,
        ts_ms=1600,
        aggressor=AggressorSide.SELL,
        trade_price=0.50,
        trade_size=70.0,
    )

    assert fill == 0.0
    assert o.queue_ahead == 30.0
    assert o.filled_shares == 0.0


def test_trade_can_clear_queue_and_partially_fill():
    o = make_bid(
        queue=100.0,
        shares=10.0,
    )

    fill = apply_aggressive_trade(
        o,
        ts_ms=1600,
        aggressor=AggressorSide.SELL,
        trade_price=0.50,
        trade_size=104.0,
    )

    assert fill == 4.0
    assert o.queue_ahead == 0.0
    assert o.filled_shares == 4.0
    assert (
        o.state
        is OrderState.PARTIALLY_FILLED
    )


def test_multiple_trades_continue_partial_fill():
    o = make_bid(
        queue=5.0,
        shares=10.0,
    )

    f1 = apply_aggressive_trade(
        o,
        ts_ms=1600,
        aggressor=AggressorSide.SELL,
        trade_price=0.50,
        trade_size=8.0,
    )

    f2 = apply_aggressive_trade(
        o,
        ts_ms=1700,
        aggressor=AggressorSide.SELL,
        trade_price=0.50,
        trade_size=7.0,
    )

    assert f1 == 3.0
    assert f2 == 7.0
    assert o.filled_shares == 10.0
    assert o.state is OrderState.FILLED


def test_ask_is_consumed_by_buy_aggressor():
    o = make_ask(
        queue=5.0,
        shares=10.0,
    )

    fill = apply_aggressive_trade(
        o,
        ts_ms=1600,
        aggressor=AggressorSide.BUY,
        trade_price=0.50,
        trade_size=15.0,
    )

    assert fill == 10.0
    assert o.state is OrderState.FILLED


def test_trade_before_placement_cannot_fill():
    o = PassiveOrder(
        order_id="preplace",
        side=Side.BUY,
        price=0.50,
        shares=10.0,
        submit_ts_ms=1000,
        config=CFG,
    )

    fill = apply_aggressive_trade(
        o,
        ts_ms=1400,
        aggressor=AggressorSide.SELL,
        trade_price=0.50,
        trade_size=1000.0,
    )

    assert fill == 0.0
    assert o.filled_shares == 0.0


def test_marketable_bid_rejected_from_passive_engine():
    o = PassiveOrder(
        order_id="crossing",
        side=Side.BUY,
        price=0.51,
        shares=10.0,
        submit_ts_ms=1000,
        config=CFG,
    )

    activate_passive_order(
        o,
        ts_ms=1500,
        displayed_size_at_price=0.0,
        best_bid=0.50,
        best_ask=0.51,
    )

    assert o.state is OrderState.REJECTED


def test_cancel_request_does_not_cancel_immediately():
    o = make_bid(
        queue=0.0
    )

    request_cancel(
        o,
        ts_ms=2000,
    )

    assert (
        o.state
        is OrderState.CANCEL_PENDING
    )

    assert (
        o.cancel_effective_ts_ms
        == 2500
    )


def test_fill_before_cancel_effective():
    o = make_bid(
        queue=0.0,
        shares=10.0,
    )

    request_cancel(
        o,
        ts_ms=2000,
    )

    fill = apply_aggressive_trade(
        o,
        ts_ms=2499,
        aggressor=AggressorSide.SELL,
        trade_price=0.50,
        trade_size=6.0,
    )

    assert fill == 6.0
    assert o.filled_shares == 6.0
    assert (
        o.state
        is OrderState.CANCEL_PENDING
    )


def test_trade_exactly_at_cancel_effective_can_fill():
    o = make_bid(
        queue=0.0,
        shares=10.0,
    )

    request_cancel(
        o,
        ts_ms=2000,
    )

    fill = apply_aggressive_trade(
        o,
        ts_ms=2500,
        aggressor=AggressorSide.SELL,
        trade_price=0.50,
        trade_size=10.0,
    )

    assert fill == 10.0
    assert o.state is OrderState.FILLED


def test_trade_after_cancel_effective_cannot_fill():
    o = make_bid(
        queue=0.0,
        shares=10.0,
    )

    request_cancel(
        o,
        ts_ms=2000,
    )

    fill = apply_aggressive_trade(
        o,
        ts_ms=2501,
        aggressor=AggressorSide.SELL,
        trade_price=0.50,
        trade_size=10.0,
    )

    assert fill == 0.0
    assert o.state is OrderState.CANCELLED
    assert o.filled_shares == 0.0


def test_advance_time_effects_cancel():
    o = make_bid(
        queue=100.0
    )

    request_cancel(
        o,
        ts_ms=2000,
    )

    advance_time(
        o,
        ts_ms=2500,
    )

    assert o.state is OrderState.CANCELLED


def test_repeated_cancel_is_idempotent():
    o = make_bid()

    request_cancel(
        o,
        ts_ms=2000,
    )

    request_cancel(
        o,
        ts_ms=2100,
    )

    assert (
        o.cancel_request_ts_ms
        == 2000
    )

    assert (
        o.cancel_effective_ts_ms
        == 2500
    )


def test_gap_invalidates_order():
    o = make_bid()

    invalidate_for_gap(
        o,
        ts_ms=1800,
    )

    assert (
        o.state
        is OrderState.INVALIDATED
    )

    fill = apply_aggressive_trade(
        o,
        ts_ms=1900,
        aggressor=AggressorSide.SELL,
        trade_price=0.50,
        trade_size=1000.0,
    )

    assert fill == 0.0


def test_non_positive_trade_rejected():
    o = make_bid()

    try:
        apply_aggressive_trade(
            o,
            ts_ms=1600,
            aggressor=AggressorSide.SELL,
            trade_price=0.50,
            trade_size=0.0,
        )
    except ValueError:
        pass
    else:
        raise AssertionError(
            "zero trade size should reject"
        )


def _run_all():
    tests = [
        value
        for name, value
        in sorted(
            globals().items()
        )
        if (
            name.startswith("test_")
            and callable(value)
        )
    ]

    passed = 0

    for test in tests:

        try:
            test()

        except Exception as exc:
            print(
                "FAIL",
                test.__name__,
                type(exc).__name__,
                str(exc),
            )

        else:
            passed += 1

            print(
                "PASS",
                test.__name__,
            )

    print()
    print(
        f"{passed}/{len(tests)} passed"
    )

    return passed == len(tests)


if __name__ == "__main__":
    raise SystemExit(
        0 if _run_all() else 1
    )
