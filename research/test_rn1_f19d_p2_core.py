from datetime import datetime, timedelta, timezone
from decimal import Decimal as D

import pytest

from research.rn1_f19d_p2_core import (
    MakerOpportunity,
    MatchedResult,
    Trade,
    bootstrap_market_mean_ci,
    build_matched_result,
    classify_rn1_roles,
    confirmation_decision,
    control_is_eligible,
    decluster_usable,
    flow_imbalance,
    future_markout_print,
    game_phase,
    is_flagged,
    leave_one_market_out,
    market_equal_difference,
    market_medians,
    market_sign_count,
    normalized_inventory_price,
    p1_exposure_direction,
    primary_markout,
    select_controls,
    signal_window,
)


UTC = timezone.utc
T0 = datetime(2026, 8, 20, 20, 0, tzinfo=UTC)


def trade(
    *,
    ts,
    side="BUY",
    outcome_index=0,
    price="0.50",
    size="10",
    wallet="X",
    asset="p1",
    condition="m1",
    tx="tx",
    ordinal=0,
):
    return Trade(
        proxy_wallet=wallet,
        side=side,
        asset=asset,
        condition_id=condition,
        size=D(size),
        price=D(price),
        timestamp=ts,
        transaction_hash=tx,
        outcome="P1" if outcome_index == 0 else "P0",
        outcome_index=outcome_index,
        duplicate_ordinal=ordinal,
    )


def opp(
    *,
    ts,
    flagged,
    side="BUY",
    outcome_index=0,
    price="0.50",
    size="10",
    asset="p1",
    condition="m1",
    game_start=T0,
    tx="tx",
    ordinal=0,
):
    return MakerOpportunity(
        trade=trade(
            ts=ts,
            side=side,
            outcome_index=outcome_index,
            price=price,
            size=size,
            asset=asset,
            condition=condition,
            tx=tx,
            ordinal=ordinal,
        ),
        game_start_time=game_start,
        flagged=flagged,
    )


@pytest.mark.parametrize(
    ("outcome_index", "side", "expected"),
    [
        (0, "BUY", 1),
        (1, "SELL", 1),
        (0, "SELL", -1),
        (1, "BUY", -1),
    ],
)
def test_binary_axis(outcome_index, side, expected):
    assert p1_exposure_direction(outcome_index, side) == expected


@pytest.mark.parametrize(
    ("price", "outcome_index", "direction", "expected"),
    [
        ("0.60", 0, 1, D("0.60")),
        ("0.40", 1, 1, D("0.60")),
        ("0.40", 1, -1, D("0.40")),
        ("0.60", 0, -1, D("0.40")),
    ],
)
def test_normalized_inventory_price(price, outcome_index, direction, expected):
    assert normalized_inventory_price(
        price=D(price),
        outcome_index=outcome_index,
        inventory_direction=direction,
    ) == expected


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [
        (-1, "pregame"),
        (0, "live_0_5m"),
        (299, "live_0_5m"),
        (300, "live_5_30m"),
        (1799, "live_5_30m"),
        (1800, "live_30_60m"),
        (3599, "live_30_60m"),
        (3600, "live_60_120m"),
        (7199, "live_60_120m"),
        (7200, "live_120m_plus"),
    ],
)
def test_game_phase_boundaries(seconds, expected):
    assert game_phase(
        event_time=T0 + timedelta(seconds=seconds),
        game_start_time=T0,
    ) == expected


def test_signal_window_exact_boundaries_and_self_exclusion():
    maker_time = T0 + timedelta(minutes=10)
    start, end = signal_window(maker_time)

    tape = [
        # Included: exactly start, adverse relative to +1 maker.
        trade(
            ts=start,
            side="SELL",
            outcome_index=0,
            price="0.50",
            size="10",
            tx="a",
        ),
        # Included: exactly end, adverse relative to +1 maker.
        trade(
            ts=end,
            side="BUY",
            outcome_index=1,
            price="0.50",
            size="10",
            tx="b",
        ),
        # Excluded: one microsecond before window.
        trade(
            ts=start - timedelta(microseconds=1),
            side="BUY",
            outcome_index=0,
            price="0.50",
            size="1000",
            tx="c",
        ),
        # Excluded: after end.
        trade(
            ts=end + timedelta(microseconds=1),
            side="BUY",
            outcome_index=0,
            price="0.50",
            size="1000",
            tx="d",
        ),
        # Excluded: RN1 self trade.
        trade(
            ts=start,
            side="BUY",
            outcome_index=0,
            price="0.50",
            size="1000",
            wallet="RN1",
            tx="e",
        ),
    ]

    value = flow_imbalance(
        maker_inventory_direction=1,
        maker_fill_time=maker_time,
        market_taker=tape,
        rn1_wallet="RN1",
    )
    assert value == D("-1")
    assert is_flagged(value)


def test_signal_undefined_zero_denominator_is_unflagged():
    maker_time = T0 + timedelta(minutes=10)
    assert flow_imbalance(
        maker_inventory_direction=1,
        maker_fill_time=maker_time,
        market_taker=[],
        rn1_wallet="RN1",
    ) is None
    assert is_flagged(None) is False


def test_flag_threshold_is_inclusive():
    assert is_flagged(D("-0.75"))
    assert not is_flagged(D("-0.749999"))


def test_rn1_role_multiset_matching_preserves_multiplicity():
    t1 = trade(ts=T0, wallet="RN1", tx="same")
    t2 = trade(ts=T0, wallet="RN1", tx="same", ordinal=1)

    rn1_taker, rn1_maker = classify_rn1_roles(
        rn1_all=[t2, t1],
        market_taker=[t1],
        rn1_wallet="RN1",
    )

    assert len(rn1_taker) == 1
    assert len(rn1_maker) == 1


def test_control_boundaries_are_inclusive():
    flagged = opp(
        ts=T0 + timedelta(minutes=40),
        flagged=True,
        price="0.50",
        size="10",
        tx="f",
    )

    exact_min = opp(
        ts=flagged.trade.timestamp - timedelta(seconds=120),
        flagged=False,
        price="0.55",
        size="5",
        tx="c1",
    )
    exact_max = opp(
        ts=flagged.trade.timestamp + timedelta(seconds=900),
        flagged=False,
        price="0.45",
        size="20",
        tx="c2",
    )

    assert control_is_eligible(flagged, exact_min)
    assert control_is_eligible(flagged, exact_max)


def test_control_wrong_phase_rejected():
    flagged = opp(
        ts=T0 + timedelta(seconds=299),
        flagged=True,
        tx="f",
    )
    control = opp(
        ts=T0 + timedelta(seconds=300 + 120),
        flagged=False,
        tx="c",
    )
    assert not control_is_eligible(flagged, control)


def test_control_selection_order_and_no_reuse():
    f1 = opp(
        ts=T0 + timedelta(minutes=40),
        flagged=True,
        tx="f1",
    )
    f2 = opp(
        ts=T0 + timedelta(minutes=41),
        flagged=True,
        tx="f2",
    )

    # All are eligible for f1. c1 is nearest in time, then c2, then c3.
    controls = [
        opp(
            ts=f1.trade.timestamp - timedelta(seconds=150),
            flagged=False,
            price="0.50",
            size="10",
            tx="c1",
        ),
        opp(
            ts=f1.trade.timestamp - timedelta(seconds=160),
            flagged=False,
            price="0.50",
            size="10",
            tx="c2",
        ),
        opp(
            ts=f1.trade.timestamp - timedelta(seconds=170),
            flagged=False,
            price="0.50",
            size="10",
            tx="c3",
        ),
        opp(
            ts=f1.trade.timestamp - timedelta(seconds=180),
            flagged=False,
            price="0.50",
            size="10",
            tx="c4",
        ),
    ]

    selected = select_controls(
        flagged_events=[f2, f1],
        control_pool=controls,
    )

    s1 = selected[f1.stable_event_key]
    s2 = selected[f2.stable_event_key]

    assert [c.trade.transaction_hash for c in s1] == ["c1", "c2", "c3"]
    assert all(c.stable_event_key not in {x.stable_event_key for x in s1} for c in s2)


def test_future_print_uses_target_inclusive_and_same_asset_side():
    maker = opp(
        ts=T0 + timedelta(minutes=40),
        flagged=True,
        side="BUY",
        outcome_index=0,
        asset="p1",
        tx="maker",
    )
    target = maker.trade.timestamp + timedelta(seconds=15)

    tape = [
        # Wrong aggressor side.
        trade(
            ts=target,
            side="BUY",
            outcome_index=0,
            asset="p1",
            tx="wrong-side",
        ),
        # Wrong asset.
        trade(
            ts=target,
            side="SELL",
            outcome_index=1,
            asset="p0",
            tx="wrong-asset",
        ),
        # Exact target and eligible.
        trade(
            ts=target,
            side="SELL",
            outcome_index=0,
            asset="p1",
            price="0.45",
            tx="chosen",
        ),
        # Later eligible.
        trade(
            ts=target + timedelta(seconds=1),
            side="SELL",
            outcome_index=0,
            asset="p1",
            price="0.40",
            tx="later",
        ),
    ]

    chosen = future_markout_print(
        maker=maker,
        market_taker=tape,
    )
    assert chosen is not None
    assert chosen.transaction_hash == "chosen"
    assert primary_markout(
        maker=maker,
        market_taker=tape,
    ) == D("-0.05")


def test_future_print_latest_boundary_inclusive():
    maker = opp(
        ts=T0 + timedelta(minutes=40),
        flagged=True,
        side="BUY",
        asset="p1",
        tx="maker",
    )
    latest = maker.trade.timestamp + timedelta(seconds=25)

    eligible = trade(
        ts=latest,
        side="SELL",
        asset="p1",
        price="0.49",
        tx="at-latest",
    )
    too_late = trade(
        ts=latest + timedelta(microseconds=1),
        side="SELL",
        asset="p1",
        price="0.10",
        tx="late",
    )

    chosen = future_markout_print(
        maker=maker,
        market_taker=[too_late, eligible],
    )
    assert chosen is not None
    assert chosen.transaction_hash == "at-latest"


def test_selected_control_missing_markout_does_not_trigger_fallback():
    flagged = opp(
        ts=T0 + timedelta(minutes=40),
        flagged=True,
        tx="f",
    )
    control = opp(
        ts=T0 + timedelta(minutes=37),
        flagged=False,
        tx="c",
    )
    unselected = opp(
        ts=T0 + timedelta(minutes=36),
        flagged=False,
        tx="u",
    )

    # Only flagged and unselected get future prints.
    tape = [
        trade(
            ts=flagged.trade.timestamp + timedelta(seconds=15),
            side="SELL",
            asset="p1",
            price="0.49",
            tx="fp",
        ),
        trade(
            ts=unselected.trade.timestamp + timedelta(seconds=15),
            side="SELL",
            asset="p1",
            price="0.49",
            tx="up",
        ),
    ]

    result = build_matched_result(
        flagged=flagged,
        selected_controls=[control],
        market_taker=tape,
    )

    assert result.matched
    assert not result.usable
    assert result.valid_control_markouts == ()


def _result(condition, ts, diff, *, direction=1):
    flagged = opp(
        ts=ts,
        flagged=True,
        side="BUY" if direction == 1 else "SELL",
        outcome_index=0,
        condition=condition,
        tx=f"{condition}-{ts.timestamp()}",
    )
    return MatchedResult(
        flagged=flagged,
        selected_controls=(
            opp(
                ts=ts - timedelta(seconds=120),
                flagged=False,
                side="BUY" if direction == 1 else "SELL",
                outcome_index=0,
                condition=condition,
                tx=f"c-{condition}-{ts.timestamp()}",
            ),
        ),
        flagged_markout=D(diff),
        valid_control_markouts=(D("0"),),
    )


def test_decluster_exact_60_seconds_retained_and_direction_not_separate():
    r1 = _result("m1", T0 + timedelta(minutes=40), "-0.01", direction=1)
    r2 = _result("m1", T0 + timedelta(minutes=40, seconds=59), "-0.02", direction=-1)
    r3 = _result("m1", T0 + timedelta(minutes=41), "-0.03", direction=-1)

    kept = decluster_usable([r3, r2, r1])

    assert kept == [r1, r3]


def test_market_equal_is_market_equal_not_event_equal():
    results = [
        _result("m1", T0 + timedelta(minutes=40), "-0.10"),
        _result("m1", T0 + timedelta(minutes=45), "-0.10"),
        _result("m2", T0 + timedelta(minutes=40), "0.10"),
    ]
    # m1 mean -0.10, m2 mean +0.10 => market-equal 0.
    assert market_equal_difference(results) == D("0")



def test_rn1_role_inconsistent_self_taker_multiplicity_fails_closed():
    t1 = trade(ts=T0, wallet="RN1", tx="same")
    t2 = trade(ts=T0, wallet="RN1", tx="same", ordinal=1)

    with pytest.raises(ValueError):
        classify_rn1_roles(
            rn1_all=[t1],
            market_taker=[t1, t2],
            rn1_wallet="RN1",
        )


def test_market_medians_signs_and_leave_one_out():
    results = [
        _result("m1", T0 + timedelta(minutes=40), "-0.10"),
        _result("m1", T0 + timedelta(minutes=45), "-0.30"),
        _result("m2", T0 + timedelta(minutes=40), "0.20"),
    ]
    med = market_medians(results)
    assert med == {"m1": D("-0.20"), "m2": D("0.20")}

    per_market = {"m1": D("-0.20"), "m2": D("0.20"), "m3": D("0")}
    assert market_sign_count(per_market) == {
        "negative": 1,
        "zero": 1,
        "positive": 1,
    }
    assert leave_one_market_out(per_market) == {
        "m1": D("0.10"),
        "m2": D("-0.10"),
        "m3": D("0"),
    }

def test_bootstrap_is_deterministic():
    per_market = {
        "a": D("-0.10"),
        "b": D("-0.20"),
        "c": D("-0.30"),
    }
    a = bootstrap_market_mean_ci(per_market, draws=1000, seed=19018)
    b = bootstrap_market_mean_ci(per_market, draws=1000, seed=19018)
    assert a == b


def test_confirmation_uses_matched_count_not_usable_event_count():
    # The decision API intentionally receives matched event count + usable market count.
    d = confirmation_decision(
        matched_flagged_events=50,
        usable_market_count=20,
        primary_market_equal=D("-0.001"),
        declustered_market_equal=D("-0.001"),
        bootstrap_ci=(D("-0.003"), D("0.001")),
    )
    assert d.status == "CONFIRMED"


def test_confirmation_insufficient_if_only_49_matched():
    d = confirmation_decision(
        matched_flagged_events=49,
        usable_market_count=100,
        primary_market_equal=D("-1"),
        declustered_market_equal=D("-1"),
        bootstrap_ci=(D("-2"), D("-1")),
    )
    assert d.status == "INCONCLUSIVE_INSUFFICIENT_SAMPLE"


def test_confirmation_insufficient_if_only_19_usable_markets():
    d = confirmation_decision(
        matched_flagged_events=100,
        usable_market_count=19,
        primary_market_equal=D("-1"),
        declustered_market_equal=D("-1"),
        bootstrap_ci=(D("-2"), D("-1")),
    )
    assert d.status == "INCONCLUSIVE_INSUFFICIENT_SAMPLE"


def test_strong_confirmation_requires_ci_upper_below_zero():
    d = confirmation_decision(
        matched_flagged_events=50,
        usable_market_count=20,
        primary_market_equal=D("-0.01"),
        declustered_market_equal=D("-0.02"),
        bootstrap_ci=(D("-0.03"), D("-0.001")),
    )
    assert d.status == "STRONGLY_CONFIRMED"


def test_not_confirmed_if_declustered_nonnegative():
    d = confirmation_decision(
        matched_flagged_events=50,
        usable_market_count=20,
        primary_market_equal=D("-0.01"),
        declustered_market_equal=D("0"),
        bootstrap_ci=(D("-0.03"), D("-0.001")),
    )
    assert d.status == "NOT_CONFIRMED"
