"""
RN1-F21 shadow passive maker execution semantics.

Pure deterministic state machine.

This module does NOT:
- connect to Polymarket;
- read F19/F20 data;
- generate strategy signals;
- calculate PnL;
- model aggressive IOC execution;
- infer fills from price moves or anonymous book decreases.

Its only job in F21-M1 is to make passive queue/cancel semantics explicit
and exhaustively testable on synthetic events.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


EPS = 1e-12


class Side(str, Enum):
    BUY = "buy"
    SELL = "sell"


class AggressorSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderState(str, Enum):
    PENDING_PLACEMENT = "pending_placement"
    RESTING = "resting"
    PARTIALLY_FILLED = "partially_filled"
    CANCEL_PENDING = "cancel_pending"
    CANCELLED = "cancelled"
    FILLED = "filled"
    REJECTED = "rejected"
    INVALIDATED = "invalidated"


@dataclass(frozen=True)
class ShadowConfig:
    placement_latency_ms: int
    cancel_latency_ms: int

    def __post_init__(self):
        if self.placement_latency_ms < 0:
            raise ValueError(
                "placement_latency_ms must be >= 0"
            )

        if self.cancel_latency_ms < 0:
            raise ValueError(
                "cancel_latency_ms must be >= 0"
            )


@dataclass
class AuditEvent:
    ts_ms: int
    kind: str
    detail: str


@dataclass
class PassiveOrder:
    order_id: str
    side: Side
    price: float
    shares: float
    submit_ts_ms: int
    config: ShadowConfig

    state: OrderState = (
        OrderState.PENDING_PLACEMENT
    )

    placement_effective_ts_ms: int = 0

    cancel_request_ts_ms: int | None = None
    cancel_effective_ts_ms: int | None = None

    queue_ahead: float | None = None

    filled_shares: float = 0.0

    rejection_reason: str | None = None
    invalidation_reason: str | None = None

    audit: list[AuditEvent] = field(
        default_factory=list
    )

    def __post_init__(self):
        if not self.order_id:
            raise ValueError(
                "order_id required"
            )

        if not (
            0.0 < self.price < 1.0
        ):
            raise ValueError(
                "price must be inside (0,1)"
            )

        if self.shares <= 0:
            raise ValueError(
                "shares must be > 0"
            )

        self.placement_effective_ts_ms = (
            self.submit_ts_ms
            + self.config.placement_latency_ms
        )

        self.audit.append(
            AuditEvent(
                self.submit_ts_ms,
                "submitted",
                (
                    "placement_effective="
                    f"{self.placement_effective_ts_ms}"
                ),
            )
        )

    @property
    def remaining_shares(self) -> float:
        return max(
            0.0,
            self.shares
            - self.filled_shares,
        )

    @property
    def terminal(self) -> bool:
        return self.state in {
            OrderState.CANCELLED,
            OrderState.FILLED,
            OrderState.REJECTED,
            OrderState.INVALIDATED,
        }


def _finalize_due_cancel(
    order: PassiveOrder,
    *,
    event_ts_ms: int,
) -> None:
    """
    Cancel becomes effective before events strictly AFTER its effective
    timestamp.

    An aggressive trade at exactly cancel_effective_ts_ms is therefore
    eligible to execute first.
    """

    if (
        order.cancel_effective_ts_ms
        is None
    ):
        return

    if order.terminal:
        return

    if (
        event_ts_ms
        > order.cancel_effective_ts_ms
    ):
        order.state = (
            OrderState.CANCELLED
        )

        order.audit.append(
            AuditEvent(
                order.cancel_effective_ts_ms,
                "cancel_effective",
                "order removed",
            )
        )


def activate_passive_order(
    order: PassiveOrder,
    *,
    ts_ms: int,
    displayed_size_at_price: float,
    best_bid: float | None,
    best_ask: float | None,
) -> None:

    if order.state is not (
        OrderState.PENDING_PLACEMENT
    ):
        raise ValueError(
            "order is not pending placement"
        )

    if (
        ts_ms
        != order.placement_effective_ts_ms
    ):
        raise ValueError(
            "activation must occur exactly at "
            "placement effective timestamp"
        )

    if displayed_size_at_price < 0:
        raise ValueError(
            "displayed size cannot be negative"
        )

    # Passive-only engine.
    if order.side is Side.BUY:

        if (
            best_ask is not None
            and order.price
            >= best_ask
        ):
            order.state = (
                OrderState.REJECTED
            )

            order.rejection_reason = (
                "buy quote marketable at arrival"
            )

            order.audit.append(
                AuditEvent(
                    ts_ms,
                    "placement_rejected",
                    order.rejection_reason,
                )
            )

            return

    else:

        if (
            best_bid is not None
            and order.price
            <= best_bid
        ):
            order.state = (
                OrderState.REJECTED
            )

            order.rejection_reason = (
                "sell quote marketable at arrival"
            )

            order.audit.append(
                AuditEvent(
                    ts_ms,
                    "placement_rejected",
                    order.rejection_reason,
                )
            )

            return

    order.queue_ahead = float(
        displayed_size_at_price
    )

    order.state = OrderState.RESTING

    order.audit.append(
        AuditEvent(
            ts_ms,
            "placement_effective",
            (
                "queue_ahead="
                f"{order.queue_ahead}"
            ),
        )
    )


def request_cancel(
    order: PassiveOrder,
    *,
    ts_ms: int,
) -> None:

    _finalize_due_cancel(
        order,
        event_ts_ms=ts_ms,
    )

    if order.terminal:
        return

    # First cancel request wins.
    if (
        order.cancel_request_ts_ms
        is not None
    ):
        return

    if (
        ts_ms
        < order.placement_effective_ts_ms
    ):
        # Cancellation of an order still in placement flight.
        # It becomes effective using the same cancellation latency rule.
        pass

    order.cancel_request_ts_ms = ts_ms

    order.cancel_effective_ts_ms = (
        ts_ms
        + order.config.cancel_latency_ms
    )

    if order.state in {
        OrderState.RESTING,
        OrderState.PARTIALLY_FILLED,
    }:
        order.state = (
            OrderState.CANCEL_PENDING
        )

    order.audit.append(
        AuditEvent(
            ts_ms,
            "cancel_requested",
            (
                "cancel_effective="
                f"{order.cancel_effective_ts_ms}"
            ),
        )
    )


def advance_time(
    order: PassiveOrder,
    *,
    ts_ms: int,
) -> None:

    if order.terminal:
        return

    if (
        order.cancel_effective_ts_ms
        is not None
        and ts_ms
        >= order.cancel_effective_ts_ms
    ):
        order.state = (
            OrderState.CANCELLED
        )

        order.audit.append(
            AuditEvent(
                order.cancel_effective_ts_ms,
                "cancel_effective",
                "order removed",
            )
        )


def apply_book_size_change(
    order: PassiveOrder,
    *,
    ts_ms: int,
    new_displayed_size_at_price: float,
) -> None:
    """
    Deliberately conservative.

    Anonymous book-size changes provide NO queue credit and NO fill.
    """

    _finalize_due_cancel(
        order,
        event_ts_ms=ts_ms,
    )

    if order.terminal:
        return

    if new_displayed_size_at_price < 0:
        raise ValueError(
            "displayed size cannot be negative"
        )

    order.audit.append(
        AuditEvent(
            ts_ms,
            "book_size_observed",
            (
                "no_queue_credit;"
                f"displayed={new_displayed_size_at_price}"
            ),
        )
    )


def _trade_is_eligible(
    order: PassiveOrder,
    *,
    aggressor: AggressorSide,
    trade_price: float,
) -> bool:

    if order.side is Side.BUY:
        return (
            aggressor
            is AggressorSide.SELL
            and abs(
                trade_price
                - order.price
            )
            <= EPS
        )

    return (
        aggressor
        is AggressorSide.BUY
        and abs(
            trade_price
            - order.price
        )
        <= EPS
    )


def apply_aggressive_trade(
    order: PassiveOrder,
    *,
    ts_ms: int,
    aggressor: AggressorSide,
    trade_price: float,
    trade_size: float,
) -> float:
    """
    Returns incremental shadow fill quantity.

    Exact-price aggressive execution only in M1. No inference from trade-through,
    price movement, or book disappearance.
    """

    if trade_size <= 0:
        raise ValueError(
            "trade_size must be > 0"
        )

    if not (
        0.0 < trade_price < 1.0
    ):
        raise ValueError(
            "trade_price must be inside (0,1)"
        )

    # A trade at exactly cancel-effective time executes before cancellation.
    _finalize_due_cancel(
        order,
        event_ts_ms=ts_ms,
    )

    if order.terminal:
        return 0.0

    if (
        ts_ms
        < order.placement_effective_ts_ms
    ):
        return 0.0

    if order.queue_ahead is None:
        # Placement has not been established from an exact-depth snapshot.
        return 0.0

    if not _trade_is_eligible(
        order,
        aggressor=aggressor,
        trade_price=trade_price,
    ):
        return 0.0

    if (
        order.cancel_effective_ts_ms
        is not None
        and ts_ms
        > order.cancel_effective_ts_ms
    ):
        return 0.0

    quantity = float(
        trade_size
    )

    consumed_ahead = min(
        order.queue_ahead,
        quantity,
    )

    order.queue_ahead -= (
        consumed_ahead
    )

    quantity -= consumed_ahead

    fill = min(
        order.remaining_shares,
        quantity,
    )

    if fill > EPS:

        order.filled_shares += fill

        if (
            order.remaining_shares
            <= EPS
        ):
            order.state = (
                OrderState.FILLED
            )

        elif (
            order.cancel_request_ts_ms
            is not None
        ):
            order.state = (
                OrderState.CANCEL_PENDING
            )

        else:
            order.state = (
                OrderState.PARTIALLY_FILLED
            )

    order.audit.append(
        AuditEvent(
            ts_ms,
            "aggressive_trade",
            (
                f"size={trade_size};"
                f"queue_consumed={consumed_ahead};"
                f"fill={fill};"
                f"queue_remaining={order.queue_ahead}"
            ),
        )
    )

    return fill


def invalidate_for_gap(
    order: PassiveOrder,
    *,
    ts_ms: int,
    reason: str = "collector gap",
) -> None:

    if order.terminal:
        return

    order.state = (
        OrderState.INVALIDATED
    )

    order.invalidation_reason = reason

    order.audit.append(
        AuditEvent(
            ts_ms,
            "invalidated",
            reason,
        )
    )
