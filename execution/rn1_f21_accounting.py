"""
RN1-F21-M4 deterministic maker accounting.

This module accounts for shadow passive fills produced by frozen F21-M1/M2/M3.

It does NOT:
- read market data;
- calculate the protection signal;
- discover venue fee schedules;
- value inventory;
- mark positions to market;
- use settlement/winner information;
- calculate PnL.

Fee/rebate bps are explicit synthetic inputs.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from rn1_f21_protection import (
    ProtectionPairResult,
)

from rn1_f21_replay import (
    ReplayResult,
)

from rn1_f21_shadow_mm import (
    Side,
)


ZERO = Decimal("0")
BPS_DENOMINATOR = Decimal("10000")


def D(value) -> Decimal:
    """
    Construct Decimal from the string representation,
    avoiding binary-float accumulation.
    """

    if isinstance(
        value,
        Decimal,
    ):
        return value

    return Decimal(
        str(value)
    )


def decimal_text(
    value: Decimal,
) -> str:

    if value == ZERO:
        return "0"

    normalized = value.normalize()

    return format(
        normalized,
        "f",
    )


@dataclass(frozen=True)
class MakerAccountingConfig:
    maker_fee_bps: Decimal = ZERO
    maker_rebate_bps: Decimal = ZERO

    def __post_init__(self):

        fee = D(
            self.maker_fee_bps
        )

        rebate = D(
            self.maker_rebate_bps
        )

        if fee < ZERO:
            raise ValueError(
                "maker_fee_bps must be >= 0"
            )

        if rebate < ZERO:
            raise ValueError(
                "maker_rebate_bps must be >= 0"
            )

        object.__setattr__(
            self,
            "maker_fee_bps",
            fee,
        )

        object.__setattr__(
            self,
            "maker_rebate_bps",
            rebate,
        )


@dataclass(frozen=True)
class OrderAccounting:
    order_id: str
    side: Side

    quote_price: Decimal
    filled_shares: Decimal

    filled_notional: Decimal
    inventory_delta: Decimal

    gross_cash_delta: Decimal

    maker_fee_cost: Decimal
    maker_rebate_credit: Decimal

    net_cash_delta: Decimal

    def canonical_summary(
        self,
    ) -> dict[str, Any]:

        return {
            "order_id":
                self.order_id,

            "side":
                self.side.value,

            "quote_price":
                decimal_text(
                    self.quote_price
                ),

            "filled_shares":
                decimal_text(
                    self.filled_shares
                ),

            "filled_notional":
                decimal_text(
                    self.filled_notional
                ),

            "inventory_delta":
                decimal_text(
                    self.inventory_delta
                ),

            "gross_cash_delta":
                decimal_text(
                    self.gross_cash_delta
                ),

            "maker_fee_cost":
                decimal_text(
                    self.maker_fee_cost
                ),

            "maker_rebate_credit":
                decimal_text(
                    self.maker_rebate_credit
                ),

            "net_cash_delta":
                decimal_text(
                    self.net_cash_delta
                ),
        }


@dataclass(frozen=True)
class AccountingDelta:
    avoided_filled_shares: Decimal
    avoided_filled_notional: Decimal

    inventory_difference: Decimal
    gross_cash_difference: Decimal

    fee_cost_difference: Decimal
    rebate_credit_difference: Decimal

    net_cash_difference: Decimal

    def canonical_summary(
        self,
    ) -> dict[str, str]:

        return {
            "avoided_filled_shares":
                decimal_text(
                    self.avoided_filled_shares
                ),

            "avoided_filled_notional":
                decimal_text(
                    self.avoided_filled_notional
                ),

            "inventory_difference":
                decimal_text(
                    self.inventory_difference
                ),

            "gross_cash_difference":
                decimal_text(
                    self.gross_cash_difference
                ),

            "fee_cost_difference":
                decimal_text(
                    self.fee_cost_difference
                ),

            "rebate_credit_difference":
                decimal_text(
                    self.rebate_credit_difference
                ),

            "net_cash_difference":
                decimal_text(
                    self.net_cash_difference
                ),
        }


@dataclass(frozen=True)
class AccountingPairResult:
    baseline: OrderAccounting
    protected: OrderAccounting
    delta: AccountingDelta

    def canonical_summary(
        self,
    ) -> dict[str, Any]:

        return {
            "baseline":
                self.baseline.canonical_summary(),

            "protected":
                self.protected.canonical_summary(),

            "delta":
                self.delta.canonical_summary(),

            "interpretation": {
                "net_cash_difference_is_pnl":
                    False,

                "inventory_is_valued":
                    False,

                "settlement_used":
                    False,
            },
        }


def account_replay(
    result: ReplayResult,
    config: MakerAccountingConfig,
) -> OrderAccounting:

    order = result.order

    price = D(
        order.price
    )

    quantity = D(
        order.filled_shares
    )

    if quantity < ZERO:
        raise ValueError(
            "filled shares cannot be negative"
        )

    notional = (
        quantity
        * price
    )

    fee = (
        notional
        * config.maker_fee_bps
        / BPS_DENOMINATOR
    )

    rebate = (
        notional
        * config.maker_rebate_bps
        / BPS_DENOMINATOR
    )

    if order.side is Side.BUY:

        inventory_delta = quantity

        gross_cash_delta = (
            -notional
        )

    elif order.side is Side.SELL:

        inventory_delta = (
            -quantity
        )

        gross_cash_delta = (
            notional
        )

    else:
        raise ValueError(
            f"unsupported side: {order.side}"
        )

    net_cash_delta = (
        gross_cash_delta
        - fee
        + rebate
    )

    return OrderAccounting(
        order_id=
            order.order_id,

        side=
            order.side,

        quote_price=
            price,

        filled_shares=
            quantity,

        filled_notional=
            notional,

        inventory_delta=
            inventory_delta,

        gross_cash_delta=
            gross_cash_delta,

        maker_fee_cost=
            fee,

        maker_rebate_credit=
            rebate,

        net_cash_delta=
            net_cash_delta,
    )


def account_protection_pair(
    pair: ProtectionPairResult,
    config: MakerAccountingConfig,
) -> AccountingPairResult:

    baseline = account_replay(
        pair.baseline,
        config,
    )

    protected = account_replay(
        pair.protected,
        config,
    )

    if (
        protected.filled_shares
        >
        baseline.filled_shares
    ):
        raise ValueError(
            "protected fill exceeds baseline fill"
        )

    delta = AccountingDelta(
        avoided_filled_shares=(
            baseline.filled_shares
            - protected.filled_shares
        ),

        avoided_filled_notional=(
            baseline.filled_notional
            - protected.filled_notional
        ),

        inventory_difference=(
            protected.inventory_delta
            - baseline.inventory_delta
        ),

        gross_cash_difference=(
            protected.gross_cash_delta
            - baseline.gross_cash_delta
        ),

        fee_cost_difference=(
            protected.maker_fee_cost
            - baseline.maker_fee_cost
        ),

        rebate_credit_difference=(
            protected.maker_rebate_credit
            - baseline.maker_rebate_credit
        ),

        net_cash_difference=(
            protected.net_cash_delta
            - baseline.net_cash_delta
        ),
    )

    return AccountingPairResult(
        baseline=baseline,
        protected=protected,
        delta=delta,
    )
