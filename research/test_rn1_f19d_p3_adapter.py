from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import sys

import pytest

sys.path.insert(
    0,
    str(Path(__file__).resolve().parent),
)

from rn1_f19d_p3_adapter import (
    adapt_rows,
    canonicalize_row,
)


COND = "0x" + "a" * 64
RN1 = "0x" + "1" * 40


def row(**overrides):
    base = {
        "proxyWallet": "0x" + "2" * 40,
        "side": "BUY",
        "asset": "123",
        "conditionId": COND,
        "size": "10.5",
        "price": "0.42",
        "timestamp": 1_700_000_000,
        "transactionHash": "0xabc",
        "outcome": "Team A",
        "outcomeIndex": 0,
    }

    base.update(overrides)
    return base


def test_canonicalize_epoch_seconds():
    got = canonicalize_row(
        row(),
        expected_condition_id=COND,
    )

    assert got["timestamp"] == (
        datetime.fromtimestamp(
            1_700_000_000,
            tz=timezone.utc,
        )
    )

    assert got["size"] == Decimal("10.5")
    assert got["price"] == Decimal("0.42")


def test_canonicalize_iso_z():
    got = canonicalize_row(
        row(
            timestamp="2026-08-17T00:00:00Z"
        ),
        expected_condition_id=COND,
    )

    assert got["timestamp"] == datetime(
        2026,
        8,
        17,
        tzinfo=timezone.utc,
    )


def test_extra_api_fields_are_ignored():
    got = canonicalize_row(
        row(extraField="ignored"),
        expected_condition_id=COND,
    )

    assert "extraField" not in got


def test_missing_required_field_fails_closed():
    x = row()
    del x["asset"]

    with pytest.raises(
        ValueError,
        match="missing required",
    ):
        canonicalize_row(
            x,
            expected_condition_id=COND,
        )


def test_condition_mismatch_fails_closed():
    with pytest.raises(
        ValueError,
        match="condition mismatch",
    ):
        canonicalize_row(
            row(
                conditionId="0x" + "b" * 64
            ),
            expected_condition_id=COND,
        )


def test_rn1_wallet_gate():
    with pytest.raises(
        ValueError,
        match="RN1 wallet mismatch",
    ):
        canonicalize_row(
            row(
                proxyWallet="0x" + "3" * 40
            ),
            expected_condition_id=COND,
            rn1_wallet=RN1,
        )

    got = canonicalize_row(
        row(proxyWallet=RN1.upper()),
        expected_condition_id=COND,
        rn1_wallet=RN1,
    )

    assert got["proxy_wallet"] == RN1


def test_invalid_side_fails_closed():
    with pytest.raises(
        ValueError,
        match="invalid side",
    ):
        canonicalize_row(
            row(side="HOLD"),
            expected_condition_id=COND,
        )


@pytest.mark.parametrize(
    "value",
    [-1, 2, 0.5, "0.5", "x"],
)
def test_invalid_outcome_index_fails_closed(
    value,
):
    with pytest.raises(ValueError):
        canonicalize_row(
            row(outcomeIndex=value),
            expected_condition_id=COND,
        )


@pytest.mark.parametrize(
    "value",
    [0, "-1", "NaN"],
)
def test_nonpositive_or_nonfinite_size_fails(
    value,
):
    with pytest.raises(ValueError):
        canonicalize_row(
            row(size=value),
            expected_condition_id=COND,
        )


@pytest.mark.parametrize(
    "value",
    ["-0.01", "1.01", "NaN"],
)
def test_invalid_price_fails(value):
    with pytest.raises(ValueError):
        canonicalize_row(
            row(price=value),
            expected_condition_id=COND,
        )


def test_naive_iso_timestamp_fails():
    with pytest.raises(
        ValueError,
        match="timezone-aware",
    ):
        canonicalize_row(
            row(
                timestamp="2026-08-17T00:00:00"
            ),
            expected_condition_id=COND,
        )


def test_duplicate_multiplicity_gets_ordinals():
    trades = adapt_rows(
        [row(), row()],
        expected_condition_id=COND,
    )

    assert [
        t.duplicate_ordinal
        for t in trades
    ] == [0, 1]


def test_adapter_order_is_input_order_independent():
    a = row(
        timestamp=1_700_000_100,
        transactionHash="0x2",
    )

    b = row(
        timestamp=1_700_000_000,
        transactionHash="0x1",
    )

    left = adapt_rows(
        [a, b],
        expected_condition_id=COND,
    )

    right = adapt_rows(
        [b, a],
        expected_condition_id=COND,
    )

    assert [
        x.stable_event_key
        for x in left
    ] == [
        x.stable_event_key
        for x in right
    ]


def test_rn1_stream_adapter_applies_wallet_gate_to_every_row():
    with pytest.raises(
        ValueError,
        match="RN1 wallet mismatch",
    ):
        adapt_rows(
            [
                row(proxyWallet=RN1),
                row(
                    proxyWallet="0x" + "9" * 40,
                    transactionHash="0xdef",
                ),
            ],
            expected_condition_id=COND,
            rn1_wallet=RN1,
        )


def test_adapter_constructs_p2_trade():
    trade = adapt_rows(
        [
            row(
                side="SELL",
                outcomeIndex=1,
            )
        ],
        expected_condition_id=COND,
    )[0]

    assert (
        trade.proxy_wallet
        == "0x" + "2" * 40
    )

    assert trade.side == "SELL"
    assert trade.asset == "123"
    assert trade.condition_id == COND
    assert trade.outcome_index == 1
    assert trade.duplicate_ordinal == 0
