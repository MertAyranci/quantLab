import json
import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(
    0,
    str(
        Path(__file__).resolve().parent
    ),
)

from rn1_f21_accounting import (  # noqa: E402
    MakerAccountingConfig,
    account_protection_pair,
    account_replay,
)

from rn1_f21_protection import (  # noqa: E402
    ProtectionAlert,
    ProtectionConfig,
    replay_protection_pair,
)

from rn1_f21_replay import (  # noqa: E402
    EventKind,
    OrderSpec,
    ReplayEvent,
    replay_passive_order,
)

from rn1_f21_shadow_mm import (  # noqa: E402
    AggressorSide,
    ShadowConfig,
    Side,
)


SC = ShadowConfig(
    placement_latency_ms=500,
    cancel_latency_ms=100,
)


def bid_spec(
    *,
    shares=10,
    price=0.50,
):
    return OrderSpec(
        order_id="buy",
        side=Side.BUY,
        price=price,
        shares=shares,
        submit_ts_ms=1000,
    )


def ask_spec(
    *,
    shares=10,
    price=0.50,
):
    return OrderSpec(
        order_id="sell",
        side=Side.SELL,
        price=price,
        shares=shares,
        submit_ts_ms=1000,
    )


def bid_placement(
    queue=0,
):
    return ReplayEvent(
        ts_ms=1500,
        kind=EventKind.PLACEMENT_BOOK,
        displayed_size_at_price=queue,
        best_bid=0.50,
        best_ask=0.51,
    )


def ask_placement(
    queue=0,
):
    return ReplayEvent(
        ts_ms=1500,
        kind=EventKind.PLACEMENT_BOOK,
        displayed_size_at_price=queue,
        best_bid=0.49,
        best_ask=0.50,
    )


def sell_trade(
    ts,
    size,
    price=0.50,
):
    return ReplayEvent(
        ts_ms=ts,
        kind=EventKind.TRADE,
        aggressor=AggressorSide.SELL,
        trade_price=price,
        trade_size=size,
    )


def buy_trade(
    ts,
    size,
    price=0.50,
):
    return ReplayEvent(
        ts_ms=ts,
        kind=EventKind.TRADE,
        aggressor=AggressorSide.BUY,
        trade_price=price,
        trade_size=size,
    )


def full_buy_replay():
    return replay_passive_order(
        spec=bid_spec(),
        config=SC,
        events=[
            bid_placement(),
            sell_trade(1600, 10),
        ],
        end_ts_ms=2000,
    )


def full_sell_replay():
    return replay_passive_order(
        spec=ask_spec(),
        config=SC,
        events=[
            ask_placement(),
            buy_trade(1600, 10),
        ],
        end_ts_ms=2000,
    )


def protected_buy_pair(
    *,
    first_trade=None,
    later_trade=10,
):
    events = [
        bid_placement(),
    ]

    if first_trade is not None:
        events.append(
            sell_trade(
                1800,
                first_trade,
            )
        )

    if later_trade is not None:
        events.append(
            sell_trade(
                2101,
                later_trade,
            )
        )

    return replay_protection_pair(
        spec=bid_spec(),
        shadow_config=SC,
        protection_config=
            ProtectionConfig(
                controller_latency_ms=100,
            ),
        market_events=events,
        alerts=[
            ProtectionAlert(
                "alert",
                1800,
            )
        ],
        end_ts_ms=2300,
    )


def test_buy_inventory_positive():
    a = account_replay(
        full_buy_replay(),
        MakerAccountingConfig(),
    )

    assert (
        a.inventory_delta
        == Decimal("10")
    )


def test_buy_cash_negative():
    a = account_replay(
        full_buy_replay(),
        MakerAccountingConfig(),
    )

    assert (
        a.gross_cash_delta
        == Decimal("-5.0")
    )


def test_sell_inventory_negative():
    a = account_replay(
        full_sell_replay(),
        MakerAccountingConfig(),
    )

    assert (
        a.inventory_delta
        == Decimal("-10")
    )


def test_sell_cash_positive():
    a = account_replay(
        full_sell_replay(),
        MakerAccountingConfig(),
    )

    assert (
        a.gross_cash_delta
        == Decimal("5.0")
    )


def test_filled_notional():
    a = account_replay(
        full_buy_replay(),
        MakerAccountingConfig(),
    )

    assert (
        a.filled_notional
        == Decimal("5.0")
    )


def test_maker_fee_cost():
    a = account_replay(
        full_buy_replay(),
        MakerAccountingConfig(
            maker_fee_bps=Decimal("10"),
        ),
    )

    assert (
        a.maker_fee_cost
        == Decimal("0.005")
    )


def test_maker_rebate_credit():
    a = account_replay(
        full_buy_replay(),
        MakerAccountingConfig(
            maker_rebate_bps=Decimal("2"),
        ),
    )

    assert (
        a.maker_rebate_credit
        == Decimal("0.001")
    )


def test_fee_and_rebate_net_cash():
    a = account_replay(
        full_buy_replay(),
        MakerAccountingConfig(
            maker_fee_bps=Decimal("10"),
            maker_rebate_bps=Decimal("2"),
        ),
    )

    assert (
        a.net_cash_delta
        == Decimal("-5.004")
    )


def test_sell_fee_and_rebate_net_cash():
    a = account_replay(
        full_sell_replay(),
        MakerAccountingConfig(
            maker_fee_bps=Decimal("10"),
            maker_rebate_bps=Decimal("2"),
        ),
    )

    assert (
        a.net_cash_delta
        == Decimal("4.996")
    )


def test_zero_fill_zero_accounting():
    r = replay_passive_order(
        spec=bid_spec(),
        config=SC,
        events=[
            bid_placement(
                queue=100,
            ),
            sell_trade(
                1600,
                10,
            ),
        ],
        end_ts_ms=1800,
    )

    a = account_replay(
        r,
        MakerAccountingConfig(
            maker_fee_bps=10,
            maker_rebate_bps=2,
        ),
    )

    assert a.filled_shares == 0
    assert a.filled_notional == 0
    assert a.inventory_delta == 0
    assert a.net_cash_delta == 0


def test_partial_fill_accounting():
    r = replay_passive_order(
        spec=bid_spec(),
        config=SC,
        events=[
            bid_placement(
                queue=5,
            ),
            sell_trade(
                1600,
                8,
            ),
        ],
        end_ts_ms=1800,
    )

    a = account_replay(
        r,
        MakerAccountingConfig(),
    )

    assert (
        a.filled_shares
        == Decimal("3.0")
    )

    assert (
        a.filled_notional
        == Decimal("1.500")
    )


def test_negative_fee_rejected():
    try:
        MakerAccountingConfig(
            maker_fee_bps=-1,
        )
    except ValueError:
        pass
    else:
        raise AssertionError(
            "negative maker fee accepted"
        )


def test_negative_rebate_rejected():
    try:
        MakerAccountingConfig(
            maker_rebate_bps=-1,
        )
    except ValueError:
        pass
    else:
        raise AssertionError(
            "negative maker rebate accepted"
        )


def test_protection_avoids_full_fill():
    pair = protected_buy_pair()

    a = account_protection_pair(
        pair,
        MakerAccountingConfig(),
    )

    assert (
        a.baseline.filled_shares
        == Decimal("10")
    )

    assert (
        a.protected.filled_shares
        == Decimal("0")
    )

    assert (
        a.delta.avoided_filled_shares
        == Decimal("10")
    )

    assert (
        a.delta.avoided_filled_notional
        == Decimal("5.0")
    )


def test_partial_fill_then_protection():
    pair = protected_buy_pair(
        first_trade=4,
        later_trade=6,
    )

    a = account_protection_pair(
        pair,
        MakerAccountingConfig(),
    )

    assert (
        a.baseline.filled_shares
        == Decimal("10")
    )

    assert (
        a.protected.filled_shares
        == Decimal("4")
    )

    assert (
        a.delta.avoided_filled_shares
        == Decimal("6")
    )

    assert (
        a.delta.inventory_difference
        == Decimal("-6")
    )


def test_protection_fee_and_rebate_deltas():
    pair = protected_buy_pair()

    a = account_protection_pair(
        pair,
        MakerAccountingConfig(
            maker_fee_bps=10,
            maker_rebate_bps=2,
        ),
    )

    assert (
        a.delta.fee_cost_difference
        == Decimal("-0.005")
    )

    assert (
        a.delta.rebate_credit_difference
        == Decimal("-0.001")
    )


def test_net_cash_difference_is_not_pnl_label():
    pair = protected_buy_pair()

    a = account_protection_pair(
        pair,
        MakerAccountingConfig(),
    )

    summary = (
        a.canonical_summary()
    )

    assert (
        summary[
            "interpretation"
        ][
            "net_cash_difference_is_pnl"
        ]
        is False
    )

    assert (
        summary[
            "interpretation"
        ][
            "inventory_is_valued"
        ]
        is False
    )


def test_deterministic_accounting_summary():
    pair = protected_buy_pair(
        first_trade=4,
        later_trade=6,
    )

    cfg = MakerAccountingConfig(
        maker_fee_bps=Decimal("7.5"),
        maker_rebate_bps=Decimal("1.25"),
    )

    a = account_protection_pair(
        pair,
        cfg,
    )

    b = account_protection_pair(
        pair,
        cfg,
    )

    assert (
        json.dumps(
            a.canonical_summary(),
            sort_keys=True,
        )
        ==
        json.dumps(
            b.canonical_summary(),
            sort_keys=True,
        )
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
