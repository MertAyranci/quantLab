import sys
from decimal import Decimal
from pathlib import Path


sys.path.insert(
    0,
    str(
        Path(__file__).resolve().parent
    ),
)


from rn1_f21_polymarket_venue import (  # noqa: E402
    VenueMarketParams,
    maker_fee_equivalent,
    maker_platform_fee,
    maker_rebate_from_pool,
    matches_frozen_sports_reference,
    parse_market_params,
    prepare_post_only_order,
    price_conforms_to_tick,
)


def params(
    *,
    tick="0.01",
    minimum="5",
    rate="0.05",
    exponent="1",
    taker_only=True,
    rebate="0.15",
):
    return VenueMarketParams(
        condition_id="synthetic",
        min_order_size=minimum,
        tick_size=tick,
        fee_rate=rate,
        fee_exponent=exponent,
        taker_only=taker_only,
        rebate_rate=rebate,
        taker_order_delay_enabled=False,
        minimum_order_age_s=0,
    )


def test_parse_market_payloads():
    p = parse_market_params(
        condition_id="abc",
        clob_info={
            "mos": 5,
            "mts": 0.01,
            "fd": {
                "r": 0.05,
                "e": 1,
                "to": True,
            },
            "itode": True,
            "oas": 2,
        },
        gamma_market={
            "feeSchedule": {
                "rate": 0.05,
                "exponent": 1,
                "takerOnly": True,
                "rebateRate": 0.15,
            }
        },
    )

    assert p.min_order_size == Decimal("5")
    assert p.tick_size == Decimal("0.01")
    assert p.fee_rate == Decimal("0.05")
    assert p.rebate_rate == Decimal("0.15")
    assert p.taker_order_delay_enabled is True
    assert p.minimum_order_age_s == 2


def test_clob_gamma_rate_mismatch_rejected():
    try:
        parse_market_params(
            condition_id="abc",
            clob_info={
                "mos": 5,
                "mts": 0.01,
                "fd": {
                    "r": 0.05,
                    "e": 1,
                    "to": True,
                },
            },
            gamma_market={
                "feeSchedule": {
                    "rate": 0.04,
                    "exponent": 1,
                    "takerOnly": True,
                    "rebateRate": 0.15,
                }
            },
        )
    except ValueError:
        pass
    else:
        raise AssertionError(
            "fee-rate mismatch accepted"
        )


def test_exponent_mismatch_rejected():
    try:
        parse_market_params(
            condition_id="abc",
            clob_info={
                "mos": 5,
                "mts": 0.01,
                "fd": {
                    "r": 0.05,
                    "e": 1,
                    "to": True,
                },
            },
            gamma_market={
                "feeSchedule": {
                    "rate": 0.05,
                    "exponent": 2,
                    "takerOnly": True,
                    "rebateRate": 0.15,
                }
            },
        )
    except ValueError:
        pass
    else:
        raise AssertionError(
            "exponent mismatch accepted"
        )


def test_tick_conformance():
    assert (
        price_conforms_to_tick(
            "0.52",
            "0.01",
        )
        is True
    )

    assert (
        price_conforms_to_tick(
            "0.525",
            "0.01",
        )
        is False
    )


def test_quarter_cent_tick():
    assert (
        price_conforms_to_tick(
            "0.5025",
            "0.0025",
        )
        is True
    )


def test_size_rounds_down_two_decimals():
    o = prepare_post_only_order(
        side="buy",
        price="0.52",
        size="10.129",
        params=params(),
        best_ask="0.53",
    )

    assert (
        o.rounded_size
        == Decimal("10.12")
    )


def test_minimum_checked_after_rounding():
    try:
        prepare_post_only_order(
            side="buy",
            price="0.52",
            size="4.999",
            params=params(
                minimum="5",
            ),
            best_ask="0.53",
        )
    except ValueError:
        pass
    else:
        raise AssertionError(
            "below-minimum rounded order accepted"
        )


def test_exact_minimum_survives_rounding():
    o = prepare_post_only_order(
        side="buy",
        price="0.52",
        size="5.009",
        params=params(
            minimum="5",
        ),
        best_ask="0.53",
    )

    assert (
        o.rounded_size
        == Decimal("5.00")
    )


def test_buy_amount_encoding():
    o = prepare_post_only_order(
        side="buy",
        price="0.52",
        size="10",
        params=params(),
        best_ask="0.53",
    )

    assert o.maker_amount == 5_200_000
    assert o.taker_amount == 10_000_000


def test_sell_amount_encoding_reversed():
    o = prepare_post_only_order(
        side="sell",
        price="0.52",
        size="10",
        params=params(),
        best_bid="0.51",
    )

    assert o.maker_amount == 10_000_000
    assert o.taker_amount == 5_200_000


def test_buy_cross_rejected():
    try:
        prepare_post_only_order(
            side="buy",
            price="0.52",
            size="10",
            params=params(),
            best_ask="0.52",
        )
    except ValueError as exc:
        assert "marketable" in str(exc)
    else:
        raise AssertionError(
            "marketable post-only buy accepted"
        )


def test_sell_cross_rejected():
    try:
        prepare_post_only_order(
            side="sell",
            price="0.52",
            size="10",
            params=params(),
            best_bid="0.52",
        )
    except ValueError as exc:
        assert "marketable" in str(exc)
    else:
        raise AssertionError(
            "marketable post-only sell accepted"
        )


def test_non_post_only_rejected():
    try:
        prepare_post_only_order(
            side="buy",
            price="0.52",
            size="10",
            params=params(),
            best_ask="0.53",
            post_only=False,
        )
    except ValueError:
        pass
    else:
        raise AssertionError(
            "non-post-only maker path accepted"
        )


def test_builder_code_rejected():
    try:
        prepare_post_only_order(
            side="buy",
            price="0.52",
            size="10",
            params=params(),
            best_ask="0.53",
            builder_code_attached=True,
        )
    except ValueError:
        pass
    else:
        raise AssertionError(
            "builder fee path accepted"
        )


def test_maker_platform_fee_zero():
    f = maker_platform_fee(
        filled_shares="100",
        price="0.50",
        params=params(),
    )

    assert f == Decimal("0")


def test_non_taker_only_schedule_rejected():
    try:
        maker_platform_fee(
            filled_shares="100",
            price="0.50",
            params=params(
                taker_only=False,
            ),
        )
    except ValueError:
        pass
    else:
        raise AssertionError(
            "non-taker-only schedule accepted"
        )


def test_sports_fee_equivalent_midpoint():
    score = maker_fee_equivalent(
        filled_shares="100",
        price="0.50",
        params=params(),
    )

    # 100 * .05 * .5 * .5 = 1.25
    assert (
        score
        == Decimal("1.2500")
    )


def test_fee_equivalent_symmetric():
    a = maker_fee_equivalent(
        filled_shares="100",
        price="0.30",
        params=params(),
    )

    b = maker_fee_equivalent(
        filled_shares="100",
        price="0.70",
        params=params(),
    )

    assert a == b


def test_unknown_exponent_fails_closed():
    try:
        maker_fee_equivalent(
            filled_shares="100",
            price="0.50",
            params=params(
                exponent="2",
            ),
        )
    except ValueError as exc:
        assert "not frozen" in str(exc)
    else:
        raise AssertionError(
            "undocumented exponent formula invented"
        )


def test_rebate_pool_allocation():
    r = maker_rebate_from_pool(
        own_fee_equivalent="10",
        market_total_fee_equivalent="100",
        rebate_pool="20",
    )

    assert (
        r
        == Decimal("2.0")
    )


def test_zero_own_score_zero_rebate():
    r = maker_rebate_from_pool(
        own_fee_equivalent="0",
        market_total_fee_equivalent="0",
        rebate_pool="20",
    )

    assert r == 0


def test_positive_score_requires_market_total():
    try:
        maker_rebate_from_pool(
            own_fee_equivalent="1",
            market_total_fee_equivalent="0",
            rebate_pool="20",
        )
    except ValueError:
        pass
    else:
        raise AssertionError(
            "rebate invented without market total"
        )


def test_own_score_cannot_exceed_total():
    try:
        maker_rebate_from_pool(
            own_fee_equivalent="11",
            market_total_fee_equivalent="10",
            rebate_pool="2",
        )
    except ValueError:
        pass
    else:
        raise AssertionError(
            "invalid fee-equivalent shares accepted"
        )


def test_frozen_sports_reference():
    assert (
        matches_frozen_sports_reference(
            params()
        )
        is True
    )


def test_reference_does_not_hide_changed_market():
    assert (
        matches_frozen_sports_reference(
            params(
                rate="0.06",
            )
        )
        is False
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
