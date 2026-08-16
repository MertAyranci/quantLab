import json
import sys
from pathlib import Path

sys.path.insert(
    0,
    str(
        Path(__file__).resolve().parent
    ),
)

from rn1_f21_replay import (  # noqa: E402
    EventKind,
    OrderSpec,
    ReplayEvent,
    replay_passive_order,
)

from rn1_f21_shadow_mm import (  # noqa: E402
    AggressorSide,
    OrderState,
    ShadowConfig,
    Side,
)


CFG = ShadowConfig(
    placement_latency_ms=500,
    cancel_latency_ms=500,
)


def bid_spec(
    *,
    shares=10.0,
    price=0.50,
):
    return OrderSpec(
        order_id="synthetic",
        side=Side.BUY,
        price=price,
        shares=shares,
        submit_ts_ms=1000,
    )


def placement(
    *,
    queue=100.0,
    ts=1500,
    best_bid=0.50,
    best_ask=0.51,
):
    return ReplayEvent(
        ts_ms=ts,
        kind=EventKind.PLACEMENT_BOOK,
        displayed_size_at_price=queue,
        best_bid=best_bid,
        best_ask=best_ask,
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


def test_basic_queue_replay():
    r = replay_passive_order(
        spec=bid_spec(),
        config=CFG,
        events=[
            placement(queue=5.0),
            sell_trade(1600, 8.0),
            sell_trade(1700, 7.0),
        ],
        end_ts_ms=2000,
    )

    assert r.order.state is OrderState.FILLED
    assert r.order.filled_shares == 10.0
    assert r.order.queue_ahead == 0.0


def test_out_of_order_input_rejected():
    try:
        replay_passive_order(
            spec=bid_spec(),
            config=CFG,
            events=[
                sell_trade(1600, 1.0),
                placement(),
            ],
            end_ts_ms=2000,
        )
    except ValueError as e:
        assert "nondecreasing" in str(e)
    else:
        raise AssertionError(
            "out-of-order input accepted"
        )


def test_same_timestamp_placement_before_trade():
    r = replay_passive_order(
        spec=bid_spec(shares=10),
        config=CFG,
        events=[
            sell_trade(1500, 15.0),
            placement(queue=5.0),
        ],
        end_ts_ms=1600,
    )

    assert r.order.state is OrderState.FILLED
    assert r.order.filled_shares == 10.0


def test_duplicate_placement_rejected():
    try:
        replay_passive_order(
            spec=bid_spec(),
            config=CFG,
            events=[
                placement(),
                placement(),
            ],
            end_ts_ms=2000,
        )
    except ValueError as e:
        assert "duplicate" in str(e)
    else:
        raise AssertionError(
            "duplicate placement accepted"
        )


def test_wrong_placement_timestamp_rejected():
    try:
        replay_passive_order(
            spec=bid_spec(),
            config=CFG,
            events=[
                placement(ts=1501),
            ],
            end_ts_ms=2000,
        )
    except ValueError as e:
        assert "placement effective" in str(e)
    else:
        raise AssertionError(
            "wrong placement timestamp accepted"
        )


def test_missing_placement_invalidates():
    r = replay_passive_order(
        spec=bid_spec(),
        config=CFG,
        events=[
            ReplayEvent(
                ts_ms=1600,
                kind=EventKind.CLOCK,
            )
        ],
        end_ts_ms=2000,
    )

    assert (
        r.order.state
        is OrderState.INVALIDATED
    )

    assert (
        "missing exact placement snapshot"
        in r.order.invalidation_reason
    )


def test_missing_placement_invalidates_at_end():
    r = replay_passive_order(
        spec=bid_spec(),
        config=CFG,
        events=[],
        end_ts_ms=2000,
    )

    assert (
        r.order.state
        is OrderState.INVALIDATED
    )


def test_end_before_placement_remains_pending():
    r = replay_passive_order(
        spec=bid_spec(),
        config=CFG,
        events=[],
        end_ts_ms=1400,
    )

    assert (
        r.order.state
        is OrderState.PENDING_PLACEMENT
    )


def test_marketable_placement_rejected():
    r = replay_passive_order(
        spec=bid_spec(price=0.51),
        config=CFG,
        events=[
            placement(
                queue=0,
                best_bid=0.50,
                best_ask=0.51,
            )
        ],
        end_ts_ms=2000,
    )

    assert (
        r.order.state
        is OrderState.REJECTED
    )


def test_cancel_before_trade():
    r = replay_passive_order(
        spec=bid_spec(),
        config=CFG,
        events=[
            placement(queue=0),
            ReplayEvent(
                ts_ms=2000,
                kind=EventKind.CANCEL_REQUEST,
            ),
            sell_trade(2499, 4),
        ],
        end_ts_ms=2500,
    )

    assert r.order.filled_shares == 4
    assert (
        r.order.state
        is OrderState.CANCELLED
    )


def test_trade_exact_cancel_timestamp_wins():
    r = replay_passive_order(
        spec=bid_spec(),
        config=CFG,
        events=[
            placement(queue=0),
            ReplayEvent(
                ts_ms=2000,
                kind=EventKind.CANCEL_REQUEST,
            ),
            sell_trade(2500, 10),
        ],
        end_ts_ms=2500,
    )

    assert r.order.filled_shares == 10
    assert r.order.state is OrderState.FILLED


def test_trade_after_cancel_cannot_fill():
    r = replay_passive_order(
        spec=bid_spec(),
        config=CFG,
        events=[
            placement(queue=0),
            ReplayEvent(
                ts_ms=2000,
                kind=EventKind.CANCEL_REQUEST,
            ),
            sell_trade(2501, 10),
        ],
        end_ts_ms=2600,
    )

    assert r.order.filled_shares == 0
    assert (
        r.order.state
        is OrderState.CANCELLED
    )


def test_zero_latency_cancel_same_ts_trade_still_fills():
    cfg = ShadowConfig(
        placement_latency_ms=500,
        cancel_latency_ms=0,
    )

    r = replay_passive_order(
        spec=bid_spec(),
        config=cfg,
        events=[
            placement(queue=0),
            ReplayEvent(
                ts_ms=2000,
                kind=EventKind.CANCEL_REQUEST,
            ),
            sell_trade(2000, 10),
        ],
        end_ts_ms=2000,
    )

    assert r.order.filled_shares == 10
    assert r.order.state is OrderState.FILLED


def test_gap_same_timestamp_beats_trade():
    r = replay_passive_order(
        spec=bid_spec(),
        config=CFG,
        events=[
            placement(queue=0),
            sell_trade(2000, 10),
            ReplayEvent(
                ts_ms=2000,
                kind=EventKind.COLLECTOR_GAP,
                reason="synthetic gap",
            ),
        ],
        end_ts_ms=2100,
    )

    assert (
        r.order.state
        is OrderState.INVALIDATED
    )

    assert r.order.filled_shares == 0


def test_book_decrease_no_queue_credit():
    r = replay_passive_order(
        spec=bid_spec(),
        config=CFG,
        events=[
            placement(queue=100),
            ReplayEvent(
                ts_ms=1600,
                kind=EventKind.BOOK_SIZE,
                new_displayed_size_at_price=1,
            ),
            sell_trade(1700, 50),
        ],
        end_ts_ms=1800,
    )

    assert r.order.queue_ahead == 50
    assert r.order.filled_shares == 0


def test_wrong_price_trade_no_fill():
    r = replay_passive_order(
        spec=bid_spec(),
        config=CFG,
        events=[
            placement(queue=0),
            sell_trade(
                1600,
                100,
                price=0.49,
            ),
        ],
        end_ts_ms=1700,
    )

    assert r.order.filled_shares == 0


def test_preplacement_trade_ignored():
    r = replay_passive_order(
        spec=bid_spec(),
        config=CFG,
        events=[
            sell_trade(1400, 1000),
            placement(queue=5),
        ],
        end_ts_ms=1600,
    )

    assert r.order.queue_ahead == 5
    assert r.order.filled_shares == 0


def test_preplacement_cancel_effective_before_placement():
    r = replay_passive_order(
        spec=bid_spec(),
        config=ShadowConfig(
            placement_latency_ms=500,
            cancel_latency_ms=100,
        ),
        events=[
            ReplayEvent(
                ts_ms=1200,
                kind=EventKind.CANCEL_REQUEST,
            ),
            placement(queue=0),
        ],
        end_ts_ms=1600,
    )

    assert (
        r.order.state
        is OrderState.CANCELLED
    )

    assert r.order.filled_shares == 0


def test_preplacement_cancel_pending_after_placement():
    r = replay_passive_order(
        spec=bid_spec(),
        config=ShadowConfig(
            placement_latency_ms=500,
            cancel_latency_ms=1000,
        ),
        events=[
            ReplayEvent(
                ts_ms=1200,
                kind=EventKind.CANCEL_REQUEST,
            ),
            placement(queue=0),
        ],
        end_ts_ms=1600,
    )

    assert (
        r.order.state
        is OrderState.CANCEL_PENDING
    )

    assert (
        r.order.cancel_effective_ts_ms
        == 2200
    )


def test_preplacement_cancel_exact_placement_time():
    r = replay_passive_order(
        spec=bid_spec(),
        config=ShadowConfig(
            placement_latency_ms=500,
            cancel_latency_ms=300,
        ),
        events=[
            ReplayEvent(
                ts_ms=1200,
                kind=EventKind.CANCEL_REQUEST,
            ),
            placement(queue=0),
            sell_trade(1500, 10),
        ],
        end_ts_ms=1500,
    )

    # placement -> trade -> cancel effective
    assert r.order.filled_shares == 10
    assert r.order.state is OrderState.FILLED


def test_end_timestamp_before_last_event_rejected():
    try:
        replay_passive_order(
            spec=bid_spec(),
            config=CFG,
            events=[
                placement(),
                sell_trade(2000, 1),
            ],
            end_ts_ms=1900,
        )
    except ValueError as e:
        assert "last input event" in str(e)
    else:
        raise AssertionError(
            "invalid replay end accepted"
        )


def test_deterministic_replay_summary():
    events = [
        placement(queue=10),
        sell_trade(1600, 12),
        ReplayEvent(
            ts_ms=1700,
            kind=EventKind.CANCEL_REQUEST,
        ),
        sell_trade(1800, 2),
    ]

    a = replay_passive_order(
        spec=bid_spec(),
        config=CFG,
        events=events,
        end_ts_ms=2300,
    )

    b = replay_passive_order(
        spec=bid_spec(),
        config=CFG,
        events=events,
        end_ts_ms=2300,
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
