from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import numpy as np

from research import h6_m2_analyze as a


def dt(text: str):
    return datetime.fromisoformat(
        text
    ).astimezone(
        timezone.utc
    )


def point(
    when: str,
    serial: int,
    *,
    valid=True,
    connection="c1",
    generation=1,
    bid="0.40",
    ask="0.60",
    bid_size="10",
    ask_size="10",
):
    return a.StatePoint(
        time=dt(when),
        serial=serial,
        valid=valid,
        connection_id=connection,
        generation=generation,
        bid=(
            Decimal(bid)
            if bid is not None
            else None
        ),
        ask=(
            Decimal(ask)
            if ask is not None
            else None
        ),
        bid_size=(
            Decimal(bid_size)
            if bid_size is not None
            else None
        ),
        ask_size=(
            Decimal(ask_size)
            if ask_size is not None
            else None
        ),
    )


def transition():
    return {
        "condition_id":
            "m1",

        "token":
            "p1",

        "capture_time":
            "2026-01-01T00:00:00+00:00",

        "event_serial":
            10,

        "event_midpoint":
            "0.50",

        "event_connection_id":
            "c1",

        "event_generation":
            1,

        "imbalance_change":
            "0.20",
    }


def test_state_persists_through_target():
    points = [
        point(
            "2026-01-01T00:00:00+00:00",
            10,
        ),
        point(
            "2026-01-01T00:00:00.500000+00:00",
            11,
            bid="0.42",
            ask="0.60",
        ),
    ]

    out = a.state_as_of_target(
        points=points,
        transition=transition(),
        target=dt(
            "2026-01-01T00:00:01+00:00"
        ),
    )

    assert out is not None
    assert out.midpoint == Decimal("0.51")


def test_gap_makes_target_missing():
    points = [
        point(
            "2026-01-01T00:00:00+00:00",
            10,
        ),
        point(
            "2026-01-01T00:00:00.200000+00:00",
            11,
            valid=False,
        ),
        point(
            "2026-01-01T00:00:00.300000+00:00",
            12,
            valid=True,
            generation=2,
        ),
    ]

    out = a.state_as_of_target(
        points=points,
        transition=transition(),
        target=dt(
            "2026-01-01T00:00:01+00:00"
        ),
    )

    assert out is None


def test_generation_change_makes_target_missing():
    points = [
        point(
            "2026-01-01T00:00:00+00:00",
            10,
        ),
        point(
            "2026-01-01T00:00:00.500000+00:00",
            11,
            generation=2,
        ),
    ]

    assert (
        a.state_as_of_target(
            points=points,
            transition=transition(),
            target=dt(
                "2026-01-01T00:00:01+00:00"
            ),
        )
        is None
    )


def sample_observations(
    positive=True,
):
    sign = (
        1.0
        if positive
        else -1.0
    )

    rows = []

    for market in (
        "m1",
        "m2",
        "m3",
    ):
        for x in (
            0.1,
            0.2,
            0.3,
        ):
            rows.append(
                {
                    "condition_id":
                        market,

                    "imbalance_change":
                        x,

                    "raw_future_midpoint_change":
                        sign
                        * x
                        * 0.05,

                    "directional_response":
                        sign
                        * x
                        * 0.05,
                }
            )

    return rows


def test_market_equal_is_equal_market_weight():
    rows = [
        {
            "condition_id": "a",
            "directional_response": 1.0,
        },
        {
            "condition_id": "a",
            "directional_response": 1.0,
        },
        {
            "condition_id": "a",
            "directional_response": 1.0,
        },
        {
            "condition_id": "b",
            "directional_response": -1.0,
        },
    ]

    means = a.market_means(
        rows
    )

    assert means == {
        "a": 1.0,
        "b": -1.0,
    }

    assert (
        np.mean(
            list(
                means.values()
            )
        )
        == 0.0
    )


def test_bootstrap_is_deterministic():
    values = {
        "a": 0.01,
        "b": 0.02,
        "c": 0.03,
    }

    x = a.bootstrap_market_equal(
        values
    )

    y = a.bootstrap_market_equal(
        values
    )

    assert x == y
    assert x["replicates"] == 10000
    assert x["seed"] == 20260820


def test_clustered_ols_positive_slope():
    out = a.clustered_ols(
        sample_observations(
            positive=True
        )
    )

    assert out["slope"] is not None
    assert out["slope"] > 0
    assert out["market_clusters"] == 3


def test_lomo_positive_fraction():
    out = a.lomo_market_equal(
        {
            "m1": 0.01,
            "m2": 0.02,
            "m3": 0.03,
        }
    )

    assert (
        out["positive_fraction"]
        == 1.0
    )

    assert (
        len(
            out["omissions"]
        )
        == 3
    )


def strong_summary():
    return {
        "event_weighted_mean_directional_response":
            0.01,

        "market_equal_mean_directional_response":
            0.01,

        "market_cluster_bootstrap_95_ci": {
            "lower": 0.001,
            "upper": 0.02,
        },

        "continuous_relationship": {
            "slope": 0.02,
        },

        "leave_one_market_out": {
            "positive_fraction": 1.0,
        },
    }


def weak_summary():
    return {
        "event_weighted_mean_directional_response":
            -0.01,

        "market_equal_mean_directional_response":
            -0.01,

        "market_cluster_bootstrap_95_ci": {
            "lower": -0.02,
            "upper": 0.01,
        },

        "continuous_relationship": {
            "slope": -0.02,
        },

        "leave_one_market_out": {
            "positive_fraction": 0.0,
        },
    }


def test_promising_requires_adjacent_pair():
    summaries = {
        horizon:
            weak_summary()

        for horizon in a.HORIZONS
    }

    summaries[3] = strong_summary()
    summaries[5] = strong_summary()

    out = a.development_verdict(
        summaries
    )

    assert (
        out["status"]
        ==
        "PROMISING_DEVELOPMENT"
    )

    assert (
        out[
            "supportive_adjacent_pairs"
        ]
        == [[3, 5]]
    )

    assert (
        out[
            "proposed_m3_primary_horizon_seconds"
        ]
        == 3
    )


def test_single_strong_horizon_is_not_enough():
    summaries = {
        horizon:
            weak_summary()

        for horizon in a.HORIZONS
    }

    summaries[5] = strong_summary()

    out = a.development_verdict(
        summaries
    )

    assert (
        out["status"]
        ==
        "NOT_PROMISING_DEVELOPMENT"
    )

    assert (
        out[
            "proposed_m3_primary_horizon_seconds"
        ]
        is None
    )


def test_analysis_environment_is_pinned():
    assert np.__version__ == "2.5.2"

    import statsmodels

    assert (
        statsmodels.__version__
        == "0.14.6"
    )
