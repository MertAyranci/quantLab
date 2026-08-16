"""
RN1-F21-M5 Polymarket venue-faithful maker constraints.

Pure deterministic adapter.

No network.
No F19/F20 access.
No signal.
No PnL.

Per-market payloads must be supplied by the caller.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import (
    Decimal,
    ROUND_DOWN,
    ROUND_UP,
)


ZERO = Decimal("0")
ONE = Decimal("1")
MICRO = Decimal("1000000")


def D(value) -> Decimal:
    if isinstance(
        value,
        Decimal,
    ):
        return value

    return Decimal(
        str(value)
    )


TICK_RULES = {
    "0.1": (1, 2, 3),
    "0.01": (2, 2, 4),
    "0.005": (3, 2, 5),
    "0.0025": (4, 2, 6),
    "0.001": (3, 2, 5),
    "0.0001": (4, 2, 6),
}


def decimal_key(
    value: Decimal,
) -> str:
    return format(
        value.normalize(),
        "f",
    )


@dataclass(frozen=True)
class VenueMarketParams:
    condition_id: str

    min_order_size: Decimal
    tick_size: Decimal

    fee_rate: Decimal
    fee_exponent: Decimal
    taker_only: bool

    rebate_rate: Decimal

    taker_order_delay_enabled: bool
    minimum_order_age_s: int

    def __post_init__(self):

        for name in (
            "min_order_size",
            "tick_size",
            "fee_rate",
            "fee_exponent",
            "rebate_rate",
        ):
            object.__setattr__(
                self,
                name,
                D(
                    getattr(
                        self,
                        name,
                    )
                ),
            )

        if not self.condition_id:
            raise ValueError(
                "condition_id required"
            )

        if self.min_order_size <= ZERO:
            raise ValueError(
                "min_order_size must be > 0"
            )

        if (
            decimal_key(
                self.tick_size
            )
            not in TICK_RULES
        ):
            raise ValueError(
                "unsupported tick size"
            )

        if self.fee_rate < ZERO:
            raise ValueError(
                "fee_rate must be >= 0"
            )

        if self.fee_exponent < ZERO:
            raise ValueError(
                "fee_exponent must be >= 0"
            )

        if not (
            ZERO
            <= self.rebate_rate
            <= ONE
        ):
            raise ValueError(
                "rebate_rate outside [0,1]"
            )

        if self.minimum_order_age_s < 0:
            raise ValueError(
                "minimum_order_age_s must be >= 0"
            )


@dataclass(frozen=True)
class PreparedPostOnlyOrder:
    side: str

    price: Decimal
    requested_size: Decimal
    rounded_size: Decimal

    usd_amount: Decimal

    maker_amount: int
    taker_amount: int

    tick_size: Decimal
    min_order_size: Decimal

    post_only: bool = True


def parse_market_params(
    *,
    condition_id: str,
    clob_info: dict,
    gamma_market: dict,
) -> VenueMarketParams:
    """
    Cross-check CLOB fd against Gamma feeSchedule.

    Rebate rate comes from Gamma feeSchedule.
    """

    for key in (
        "mos",
        "mts",
        "fd",
    ):
        if key not in clob_info:
            raise ValueError(
                f"CLOB field missing: {key}"
            )

    fd = clob_info["fd"]

    if not isinstance(
        fd,
        dict,
    ):
        raise ValueError(
            "CLOB fd must be object"
        )

    for key in (
        "r",
        "e",
        "to",
    ):
        if key not in fd:
            raise ValueError(
                f"CLOB fd field missing: {key}"
            )

    fs = gamma_market.get(
        "feeSchedule"
    )

    if not isinstance(
        fs,
        dict,
    ):
        raise ValueError(
            "Gamma feeSchedule required"
        )

    for key in (
        "rate",
        "exponent",
        "takerOnly",
        "rebateRate",
    ):
        if key not in fs:
            raise ValueError(
                f"Gamma feeSchedule field missing: {key}"
            )

    clob_rate = D(
        fd["r"]
    )

    gamma_rate = D(
        fs["rate"]
    )

    clob_exp = D(
        fd["e"]
    )

    gamma_exp = D(
        fs["exponent"]
    )

    clob_to = bool(
        fd["to"]
    )

    gamma_to = bool(
        fs["takerOnly"]
    )

    if clob_rate != gamma_rate:
        raise ValueError(
            "CLOB/Gamma fee-rate mismatch"
        )

    if clob_exp != gamma_exp:
        raise ValueError(
            "CLOB/Gamma exponent mismatch"
        )

    if clob_to != gamma_to:
        raise ValueError(
            "CLOB/Gamma taker-only mismatch"
        )

    return VenueMarketParams(
        condition_id=
            condition_id,

        min_order_size=
            D(
                clob_info["mos"]
            ),

        tick_size=
            D(
                clob_info["mts"]
            ),

        fee_rate=
            clob_rate,

        fee_exponent=
            clob_exp,

        taker_only=
            clob_to,

        rebate_rate=
            D(
                fs["rebateRate"]
            ),

        taker_order_delay_enabled=
            bool(
                clob_info.get(
                    "itode",
                    False,
                )
            ),

        minimum_order_age_s=
            int(
                clob_info.get(
                    "oas",
                    0,
                )
            ),
    )


def price_conforms_to_tick(
    price,
    tick_size,
) -> bool:

    p = D(
        price
    )

    tick = D(
        tick_size
    )

    if not (
        ZERO < p < ONE
    ):
        return False

    return (
        p % tick
        == ZERO
    )


def _quantum(
    decimals: int,
) -> Decimal:

    return Decimal(
        "1"
    ).scaleb(
        -decimals
    )


def _round_size_down(
    size,
    decimals: int,
) -> Decimal:

    return D(
        size
    ).quantize(
        _quantum(
            decimals
        ),
        rounding=ROUND_DOWN,
    )


def _round_amount(
    amount: Decimal,
    decimals: int,
) -> Decimal:
    """
    Polymarket documented order:
      1. round up to amount decimals + 4
      2. round down to amount decimals
    """

    high_precision = (
        amount.quantize(
            _quantum(
                decimals + 4
            ),
            rounding=ROUND_UP,
        )
    )

    return high_precision.quantize(
        _quantum(
            decimals
        ),
        rounding=ROUND_DOWN,
    )


def _to_micro_integer(
    value: Decimal,
) -> int:

    scaled = (
        value
        * MICRO
    )

    if (
        scaled
        != scaled.to_integral_value()
    ):
        raise ValueError(
            "value cannot be encoded at six decimals"
        )

    return int(
        scaled
    )


def prepare_post_only_order(
    *,
    side: str,
    price,
    size,
    params: VenueMarketParams,
    best_bid=None,
    best_ask=None,
    post_only: bool = True,
    builder_code_attached: bool = False,
) -> PreparedPostOnlyOrder:

    side = side.lower()

    if side not in (
        "buy",
        "sell",
    ):
        raise ValueError(
            "side must be buy or sell"
        )

    if post_only is not True:
        raise ValueError(
            "F21-M5 requires post-only"
        )

    if builder_code_attached:
        raise ValueError(
            "builder fee path is outside F21-M5"
        )

    p = D(
        price
    )

    requested_size = D(
        size
    )

    if requested_size <= ZERO:
        raise ValueError(
            "size must be > 0"
        )

    if not price_conforms_to_tick(
        p,
        params.tick_size,
    ):
        raise ValueError(
            "price does not conform to current tick"
        )

    if (
        side == "buy"
        and best_ask is not None
        and p >= D(best_ask)
    ):
        raise ValueError(
            "post-only buy would be marketable"
        )

    if (
        side == "sell"
        and best_bid is not None
        and p <= D(best_bid)
    ):
        raise ValueError(
            "post-only sell would be marketable"
        )

    tick_key = decimal_key(
        params.tick_size
    )

    (
        _price_decimals,
        size_decimals,
        amount_decimals,
    ) = TICK_RULES[
        tick_key
    ]

    rounded_size = (
        _round_size_down(
            requested_size,
            size_decimals,
        )
    )

    if (
        rounded_size
        < params.min_order_size
    ):
        raise ValueError(
            "rounded size below minimum order size"
        )

    raw_amount = (
        p
        * rounded_size
    )

    amount = _round_amount(
        raw_amount,
        amount_decimals,
    )

    size_encoded = (
        _to_micro_integer(
            rounded_size
        )
    )

    amount_encoded = (
        _to_micro_integer(
            amount
        )
    )

    if side == "buy":

        maker_amount = (
            amount_encoded
        )

        taker_amount = (
            size_encoded
        )

    else:

        maker_amount = (
            size_encoded
        )

        taker_amount = (
            amount_encoded
        )

    return PreparedPostOnlyOrder(
        side=
            side,

        price=
            p,

        requested_size=
            requested_size,

        rounded_size=
            rounded_size,

        usd_amount=
            amount,

        maker_amount=
            maker_amount,

        taker_amount=
            taker_amount,

        tick_size=
            params.tick_size,

        min_order_size=
            params.min_order_size,
    )


def maker_platform_fee(
    *,
    filled_shares,
    price,
    params: VenueMarketParams,
) -> Decimal:
    """
    Current frozen venue path requires platform fees to be taker-only.
    Therefore maker platform fee is zero.
    """

    if not params.taker_only:
        raise ValueError(
            "non-taker-only fee schedule unsupported"
        )

    if D(filled_shares) < ZERO:
        raise ValueError(
            "filled_shares must be >= 0"
        )

    p = D(
        price
    )

    if not (
        ZERO < p < ONE
    ):
        raise ValueError(
            "price outside (0,1)"
        )

    return ZERO


def maker_fee_equivalent(
    *,
    filled_shares,
    price,
    params: VenueMarketParams,
) -> Decimal:
    """
    Published maker-rebate formula frozen in M5:
        C * feeRate * p * (1-p)

    We only apply it when exponent == 1.

    If another exponent is observed, fail closed rather than inventing
    an undocumented generalized curve.
    """

    quantity = D(
        filled_shares
    )

    p = D(
        price
    )

    if quantity < ZERO:
        raise ValueError(
            "filled_shares must be >= 0"
        )

    if not (
        ZERO < p < ONE
    ):
        raise ValueError(
            "price outside (0,1)"
        )

    if (
        params.fee_exponent
        != Decimal("1")
    ):
        raise ValueError(
            "fee exponent != 1: formula not frozen"
        )

    return (
        quantity
        * params.fee_rate
        * p
        * (
            ONE - p
        )
    )


def maker_rebate_from_pool(
    *,
    own_fee_equivalent,
    market_total_fee_equivalent,
    rebate_pool,
) -> Decimal:
    """
    Ex-post market-level rebate allocation.

    This is NOT a fixed rebate bps applied directly to a fill.
    """

    own = D(
        own_fee_equivalent
    )

    total = D(
        market_total_fee_equivalent
    )

    pool = D(
        rebate_pool
    )

    if own < ZERO:
        raise ValueError(
            "own fee-equivalent must be >= 0"
        )

    if total < ZERO:
        raise ValueError(
            "market total fee-equivalent must be >= 0"
        )

    if pool < ZERO:
        raise ValueError(
            "rebate pool must be >= 0"
        )

    if own == ZERO:
        return ZERO

    if total <= ZERO:
        raise ValueError(
            "positive own score with zero market total"
        )

    if own > total:
        raise ValueError(
            "own fee-equivalent exceeds market total"
        )

    return (
        own
        / total
        * pool
    )


def matches_frozen_sports_reference(
    params: VenueMarketParams,
) -> bool:
    """
    Observation-date reference only.

    This does NOT override captured per-market parameters.
    """

    return (
        params.fee_rate
        == Decimal("0.05")
        and
        params.rebate_rate
        == Decimal("0.15")
        and
        params.taker_only
        is True
    )
