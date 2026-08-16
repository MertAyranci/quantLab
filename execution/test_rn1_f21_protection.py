import json
import sys
from pathlib import Path

sys.path.insert(
    0,
    str(
        Path(__file__).resolve().parent
    ),
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
)

from rn1_f21_shadow_mm import (  # noqa: E402
    AggressorSide,
    OrderState,
    ShadowConfig,
    Side,
)


def spec(
    *,
    shares=10.0,
):
    return OrderSpec(
        order_id="m3-synthetic",
        side=Side.BUY,
        price=0.50,
        shares=shares,
        submit_ts_ms=1000,
    )


def placement(
    *,
    queue=0.0,
):
    return ReplayEvent(
        ts_ms=1500,
        kind=EventKind.PLACEMENT_BOOK,
        displayed_size_at_price=queue,
        best_bid=0.50,
        best_ask=0.51,
    )


def trade(
    ts,
    size,
):
    return ReplayEvent(
        ts_ms=ts,
        kind=EventKind.TRADE,
        aggressor=AggressorSide.SELL,
        trade_price=0.50,
        trade_size=size,
    )


def configs(
    *,
    control=200,
    cancel=300,
):
    return (
        ShadowConfig(
            placement_latency_ms=500,
            cancel_latency_ms=cancel,
        ),
        ProtectionConfig(
            controller_latency_ms=control,
        ),
    )


def test_no_alert_protected_equals_baseline():
    sc, pc = configs()

    r = replay_protection_pair(
        spec=spec(),
        shadow_config=sc,
        protection_config=pc,
        market_events=[
            placement(),
            trade(2000, 10),
        ],
        alerts=[],
        end_ts_ms=2500,
    )

    assert (
        r.baseline.canonical_summary()
        ==
        r.protected.canonical_summary()
    )

    assert (
        r.plan.cancel_request_ts_ms
        is None
    )


def test_alert_schedules_control_latency():
    sc, pc = configs(
        control=200,
        cancel=300,
    )

    r = replay_protection_pair(
        spec=spec(),
        shadow_config=sc,
        protection_config=pc,
        market_events=[
            placement(),
        ],
        alerts=[
            ProtectionAlert(
                "a1",
                1800,
            )
        ],
        end_ts_ms=3000,
    )

    assert (
        r.plan.cancel_request_ts_ms
        == 2000
    )

    assert (
        r.plan.cancel_effective_ts_ms
        == 2300
    )

    assert (
        r.protected.order.cancel_request_ts_ms
        == 2000
    )


def test_trade_before_cancel_request_fills():
    sc, pc = configs(
        control=200,
        cancel=300,
    )

    r = replay_protection_pair(
        spec=spec(),
        shadow_config=sc,
        protection_config=pc,
        market_events=[
            placement(),
            trade(1999, 10),
        ],
        alerts=[
            ProtectionAlert(
                "a1",
                1800,
            )
        ],
        end_ts_ms=2500,
    )

    assert (
        r.protected.order.filled_shares
        == 10
    )


def test_trade_during_cancel_flight_fills():
    sc, pc = configs(
        control=200,
        cancel=300,
    )

    r = replay_protection_pair(
        spec=spec(),
        shadow_config=sc,
        protection_config=pc,
        market_events=[
            placement(),
            trade(2200, 10),
        ],
        alerts=[
            ProtectionAlert(
                "a1",
                1800,
            )
        ],
        end_ts_ms=2500,
    )

    assert (
        r.plan.cancel_request_ts_ms
        == 2000
    )

    assert (
        r.plan.cancel_effective_ts_ms
        == 2300
    )

    assert (
        r.protected.order.filled_shares
        == 10
    )


def test_trade_exactly_at_cancel_effective_fills():
    sc, pc = configs(
        control=200,
        cancel=300,
    )

    r = replay_protection_pair(
        spec=spec(),
        shadow_config=sc,
        protection_config=pc,
        market_events=[
            placement(),
            trade(2300, 10),
        ],
        alerts=[
            ProtectionAlert(
                "a1",
                1800,
            )
        ],
        end_ts_ms=2300,
    )

    assert (
        r.protected.order.filled_shares
        == 10
    )

    assert (
        r.protected.order.state
        is OrderState.FILLED
    )


def test_trade_strictly_after_cancel_effective_prevented():
    sc, pc = configs(
        control=200,
        cancel=300,
    )

    r = replay_protection_pair(
        spec=spec(),
        shadow_config=sc,
        protection_config=pc,
        market_events=[
            placement(),
            trade(2301, 10),
        ],
        alerts=[
            ProtectionAlert(
                "a1",
                1800,
            )
        ],
        end_ts_ms=2500,
    )

    assert (
        r.baseline.order.filled_shares
        == 10
    )

    assert (
        r.protected.order.filled_shares
        == 0
    )

    assert (
        r.protected.order.state
        is OrderState.CANCELLED
    )


def test_zero_total_latency_same_timestamp_trade_still_fills():
    sc, pc = configs(
        control=0,
        cancel=0,
    )

    r = replay_protection_pair(
        spec=spec(),
        shadow_config=sc,
        protection_config=pc,
        market_events=[
            placement(),
            trade(2000, 10),
        ],
        alerts=[
            ProtectionAlert(
                "a1",
                2000,
            )
        ],
        end_ts_ms=2000,
    )

    assert (
        r.plan.cancel_effective_ts_ms
        == 2000
    )

    assert (
        r.protected.order.filled_shares
        == 10
    )


def test_partial_fill_then_cancel():
    sc, pc = configs(
        control=100,
        cancel=100,
    )

    r = replay_protection_pair(
        spec=spec(shares=10),
        shadow_config=sc,
        protection_config=pc,
        market_events=[
            placement(),
            trade(1900, 4),
            trade(2101, 6),
        ],
        alerts=[
            ProtectionAlert(
                "a1",
                1900,
            )
        ],
        end_ts_ms=2300,
    )

    # request=2000, effective=2100
    assert (
        r.protected.order.filled_shares
        == 4
    )

    assert (
        r.protected.order.state
        is OrderState.CANCELLED
    )

    assert (
        r.baseline.order.filled_shares
        == 10
    )


def test_preplacement_alert_can_cancel_before_arrival():
    sc, pc = configs(
        control=50,
        cancel=50,
    )

    r = replay_protection_pair(
        spec=spec(),
        shadow_config=sc,
        protection_config=pc,
        market_events=[
            placement(),
        ],
        alerts=[
            ProtectionAlert(
                "a1",
                1200,
            )
        ],
        end_ts_ms=1800,
    )

    assert (
        r.plan.cancel_effective_ts_ms
        == 1300
    )

    assert (
        r.protected.order.state
        is OrderState.CANCELLED
    )


def test_preplacement_alert_can_remain_cancel_pending():
    sc, pc = configs(
        control=100,
        cancel=1000,
    )

    r = replay_protection_pair(
        spec=spec(),
        shadow_config=sc,
        protection_config=pc,
        market_events=[
            placement(),
        ],
        alerts=[
            ProtectionAlert(
                "a1",
                1200,
            )
        ],
        end_ts_ms=1600,
    )

    assert (
        r.plan.cancel_request_ts_ms
        == 1300
    )

    assert (
        r.plan.cancel_effective_ts_ms
        == 2300
    )

    assert (
        r.protected.order.state
        is OrderState.CANCEL_PENDING
    )


def test_first_alert_wins():
    sc, pc = configs(
        control=100,
        cancel=100,
    )

    r = replay_protection_pair(
        spec=spec(),
        shadow_config=sc,
        protection_config=pc,
        market_events=[
            placement(),
        ],
        alerts=[
            ProtectionAlert(
                "first",
                1800,
            ),
            ProtectionAlert(
                "second",
                1900,
            ),
        ],
        end_ts_ms=2500,
    )

    assert (
        r.plan.alert_count
        == 2
    )

    assert (
        r.plan.controlling_alert_id
        == "first"
    )

    assert (
        r.plan.cancel_request_ts_ms
        == 1900
    )


def test_alert_after_replay_end_rejected():
    sc, pc = configs()

    try:
        replay_protection_pair(
            spec=spec(),
            shadow_config=sc,
            protection_config=pc,
            market_events=[
                placement(),
            ],
            alerts=[
                ProtectionAlert(
                    "late",
                    3000,
                )
            ],
            end_ts_ms=2500,
        )

    except ValueError as exc:
        assert (
            "replay end"
            in str(exc)
        )

    else:
        raise AssertionError(
            "late alert accepted"
        )


def test_alert_before_submit_rejected():
    sc, pc = configs()

    try:
        replay_protection_pair(
            spec=spec(),
            shadow_config=sc,
            protection_config=pc,
            market_events=[
                placement(),
            ],
            alerts=[
                ProtectionAlert(
                    "early",
                    999,
                )
            ],
            end_ts_ms=2500,
        )

    except ValueError as exc:
        assert (
            "precedes order submit"
            in str(exc)
        )

    else:
        raise AssertionError(
            "pre-submit alert accepted"
        )


def test_out_of_order_alerts_rejected():
    sc, pc = configs()

    try:
        replay_protection_pair(
            spec=spec(),
            shadow_config=sc,
            protection_config=pc,
            market_events=[
                placement(),
            ],
            alerts=[
                ProtectionAlert(
                    "a",
                    1900,
                ),
                ProtectionAlert(
                    "b",
                    1800,
                ),
            ],
            end_ts_ms=2500,
        )

    except ValueError as exc:
        assert (
            "nondecreasing"
            in str(exc)
        )

    else:
        raise AssertionError(
            "out-of-order alerts accepted"
        )


def test_external_cancel_request_rejected():
    sc, pc = configs()

    try:
        replay_protection_pair(
            spec=spec(),
            shadow_config=sc,
            protection_config=pc,
            market_events=[
                placement(),
                ReplayEvent(
                    ts_ms=1800,
                    kind=EventKind.CANCEL_REQUEST,
                ),
            ],
            alerts=[],
            end_ts_ms=2500,
        )

    except ValueError as exc:
        assert (
            "external CANCEL_REQUEST"
            in str(exc)
        )

    else:
        raise AssertionError(
            "external cancel accepted"
        )


def test_request_after_replay_end_not_inserted():
    sc, pc = configs(
        control=1000,
        cancel=100,
    )

    r = replay_protection_pair(
        spec=spec(),
        shadow_config=sc,
        protection_config=pc,
        market_events=[
            placement(),
            trade(2400, 10),
        ],
        alerts=[
            ProtectionAlert(
                "a1",
                2000,
            )
        ],
        end_ts_ms=2500,
    )

    assert (
        r.plan.cancel_request_ts_ms
        == 3000
    )

    assert (
        r.plan.request_within_replay
        is False
    )

    assert (
        r.protected.order.cancel_request_ts_ms
        is None
    )

    assert (
        r.baseline.canonical_summary()
        ==
        r.protected.canonical_summary()
    )


def test_baseline_never_receives_controller_cancel():
    sc, pc = configs(
        control=100,
        cancel=100,
    )

    r = replay_protection_pair(
        spec=spec(),
        shadow_config=sc,
        protection_config=pc,
        market_events=[
            placement(),
        ],
        alerts=[
            ProtectionAlert(
                "a1",
                1800,
            )
        ],
        end_ts_ms=2500,
    )

    assert (
        r.baseline.order.cancel_request_ts_ms
        is None
    )

    assert (
        r.protected.order.cancel_request_ts_ms
        == 1900
    )


def test_gap_still_invalidates_protected_order():
    sc, pc = configs()

    r = replay_protection_pair(
        spec=spec(),
        shadow_config=sc,
        protection_config=pc,
        market_events=[
            placement(),
            ReplayEvent(
                ts_ms=1900,
                kind=EventKind.COLLECTOR_GAP,
                reason="synthetic gap",
            ),
            trade(2200, 10),
        ],
        alerts=[
            ProtectionAlert(
                "a1",
                1800,
            )
        ],
        end_ts_ms=2500,
    )

    assert (
        r.protected.order.state
        is OrderState.INVALIDATED
    )

    assert (
        r.protected.order.filled_shares
        == 0
    )


def test_deterministic_pair_summary():
    sc, pc = configs(
        control=120,
        cancel=350,
    )

    events = [
        placement(queue=10),
        trade(1900, 4),
        trade(2400, 10),
    ]

    alerts = [
        ProtectionAlert(
            "a1",
            1800,
        )
    ]

    a = replay_protection_pair(
        spec=spec(),
        shadow_config=sc,
        protection_config=pc,
        market_events=events,
        alerts=alerts,
        end_ts_ms=2600,
    )

    b = replay_protection_pair(
        spec=spec(),
        shadow_config=sc,
        protection_config=pc,
        market_events=events,
        alerts=alerts,
        end_ts_ms=2600,
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
