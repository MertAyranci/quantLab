"""
RN1-F21-M2 deterministic passive-maker replay engine.

Synthetic events only.

This module wraps the frozen F21-M1 passive-order state machine with:
- chronological input validation;
- deterministic same-timestamp event ordering;
- exact placement-book initialization;
- cancel-effective scheduling;
- gap invalidation;
- explicit replay termination.

No network, database, F19/F20 access, strategy signal, markout, or PnL.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from rn1_f21_shadow_mm import (
    AggressorSide,
    AuditEvent,
    OrderState,
    PassiveOrder,
    ShadowConfig,
    Side,
    activate_passive_order,
    advance_time,
    apply_aggressive_trade,
    apply_book_size_change,
    invalidate_for_gap,
    request_cancel,
)


class EventKind(str, Enum):
    PLACEMENT_BOOK = "PLACEMENT_BOOK"
    CANCEL_REQUEST = "CANCEL_REQUEST"
    COLLECTOR_GAP = "COLLECTOR_GAP"
    TRADE = "TRADE"
    BOOK_SIZE = "BOOK_SIZE"
    CLOCK = "CLOCK"


PRIORITY = {
    EventKind.PLACEMENT_BOOK: 10,
    EventKind.CANCEL_REQUEST: 20,
    EventKind.COLLECTOR_GAP: 30,
    EventKind.TRADE: 40,
    EventKind.BOOK_SIZE: 50,
    EventKind.CLOCK: 60,
}


@dataclass(frozen=True)
class OrderSpec:
    order_id: str
    side: Side
    price: float
    shares: float
    submit_ts_ms: int


@dataclass(frozen=True)
class ReplayEvent:
    ts_ms: int
    kind: EventKind

    displayed_size_at_price: float | None = None
    best_bid: float | None = None
    best_ask: float | None = None

    aggressor: AggressorSide | None = None
    trade_price: float | None = None
    trade_size: float | None = None

    new_displayed_size_at_price: float | None = None

    reason: str | None = None


@dataclass
class ReplayResult:
    order: PassiveOrder
    input_event_count: int
    processed_event_count: int
    ignored_after_terminal: int
    placement_snapshot_seen: bool
    end_ts_ms: int

    def canonical_summary(
        self,
    ) -> dict[str, Any]:
        return {
            "order_id":
                self.order.order_id,

            "state":
                self.order.state.value,

            "filled_shares":
                self.order.filled_shares,

            "remaining_shares":
                self.order.remaining_shares,

            "queue_ahead":
                self.order.queue_ahead,

            "cancel_request_ts_ms":
                self.order.cancel_request_ts_ms,

            "cancel_effective_ts_ms":
                self.order.cancel_effective_ts_ms,

            "rejection_reason":
                self.order.rejection_reason,

            "invalidation_reason":
                self.order.invalidation_reason,

            "input_event_count":
                self.input_event_count,

            "processed_event_count":
                self.processed_event_count,

            "ignored_after_terminal":
                self.ignored_after_terminal,

            "placement_snapshot_seen":
                self.placement_snapshot_seen,

            "end_ts_ms":
                self.end_ts_ms,

            "audit":
                [
                    {
                        "ts_ms":
                            x.ts_ms,

                        "kind":
                            x.kind,

                        "detail":
                            x.detail,
                    }
                    for x in self.order.audit
                ],
        }


def _validate_event(
    event: ReplayEvent,
) -> None:

    if event.ts_ms < 0:
        raise ValueError(
            "event timestamp must be >= 0"
        )

    if (
        event.kind
        is EventKind.PLACEMENT_BOOK
    ):
        if (
            event.displayed_size_at_price
            is None
        ):
            raise ValueError(
                "PLACEMENT_BOOK requires "
                "displayed_size_at_price"
            )

        if (
            event.displayed_size_at_price
            < 0
        ):
            raise ValueError(
                "negative displayed size"
            )

    elif event.kind is EventKind.TRADE:

        if event.aggressor is None:
            raise ValueError(
                "TRADE requires aggressor"
            )

        if event.trade_price is None:
            raise ValueError(
                "TRADE requires trade_price"
            )

        if event.trade_size is None:
            raise ValueError(
                "TRADE requires trade_size"
            )

        if event.trade_size <= 0:
            raise ValueError(
                "TRADE size must be > 0"
            )

    elif (
        event.kind
        is EventKind.BOOK_SIZE
    ):
        if (
            event.new_displayed_size_at_price
            is None
        ):
            raise ValueError(
                "BOOK_SIZE requires "
                "new_displayed_size_at_price"
            )

        if (
            event.new_displayed_size_at_price
            < 0
        ):
            raise ValueError(
                "negative BOOK_SIZE"
            )


def _validate_input_order(
    events: list[ReplayEvent],
) -> None:

    previous = None

    for event in events:

        _validate_event(
            event
        )

        if (
            previous is not None
            and event.ts_ms < previous
        ):
            raise ValueError(
                "input event timestamps must "
                "be nondecreasing"
            )

        previous = event.ts_ms


def _group_events(
    events: list[ReplayEvent],
):
    """
    Input must already be chronological.

    We only reorder events sharing the exact same timestamp,
    using the frozen F21-M2 priority.
    """

    i = 0

    while i < len(events):

        ts = events[i].ts_ms

        group = []

        while (
            i < len(events)
            and events[i].ts_ms == ts
        ):
            group.append(
                (
                    i,
                    events[i],
                )
            )

            i += 1

        group.sort(
            key=lambda item: (
                PRIORITY[
                    item[1].kind
                ],
                item[0],
            )
        )

        yield (
            ts,
            [
                event
                for _, event
                in group
            ],
        )


def _invalidate_missing_placement(
    order: PassiveOrder,
) -> None:

    if (
        order.state
        is not OrderState.PENDING_PLACEMENT
    ):
        return

    invalidate_for_gap(
        order,
        ts_ms=
            order.placement_effective_ts_ms,
        reason=(
            "missing exact placement snapshot"
        ),
    )


def replay_passive_order(
    *,
    spec: OrderSpec,
    config: ShadowConfig,
    events: list[ReplayEvent],
    end_ts_ms: int,
) -> ReplayResult:

    if end_ts_ms < spec.submit_ts_ms:
        raise ValueError(
            "end timestamp precedes submit"
        )

    _validate_input_order(
        events
    )

    if events:

        if (
            events[0].ts_ms
            < spec.submit_ts_ms
        ):
            raise ValueError(
                "event precedes order submit"
            )

        if (
            end_ts_ms
            < events[-1].ts_ms
        ):
            raise ValueError(
                "end timestamp precedes "
                "last input event"
            )

    order = PassiveOrder(
        order_id=spec.order_id,
        side=spec.side,
        price=spec.price,
        shares=spec.shares,
        submit_ts_ms=spec.submit_ts_ms,
        config=config,
    )

    placement_events = [
        event
        for event in events
        if (
            event.kind
            is EventKind.PLACEMENT_BOOK
        )
    ]

    if len(placement_events) > 1:
        raise ValueError(
            "duplicate PLACEMENT_BOOK events"
        )

    if placement_events:

        if (
            placement_events[0].ts_ms
            !=
            order.placement_effective_ts_ms
        ):
            raise ValueError(
                "PLACEMENT_BOOK timestamp must "
                "equal placement effective timestamp"
            )

    placement_seen = False
    processed = 0
    ignored_terminal = 0

    for ts_ms, group in _group_events(
        events
    ):

        # If cancellation became effective strictly
        # before this timestamp, remove the order
        # before processing the later event group.
        if (
            not order.terminal
            and
            order.cancel_effective_ts_ms
            is not None
            and
            order.cancel_effective_ts_ms
            < ts_ms
        ):
            advance_time(
                order,
                ts_ms=
                    order.cancel_effective_ts_ms,
            )

        # If placement time has already passed
        # without the required exact-depth snapshot,
        # invalidate before later events.
        if (
            not order.terminal
            and
            ts_ms
            >
            order.placement_effective_ts_ms
            and
            not placement_seen
            and
            order.state
            is OrderState.PENDING_PLACEMENT
        ):
            _invalidate_missing_placement(
                order
            )

        for event in group:

            if order.terminal:

                ignored_terminal += 1
                continue

            processed += 1

            if (
                event.kind
                is EventKind.PLACEMENT_BOOK
            ):

                placement_seen = True

                activate_passive_order(
                    order,
                    ts_ms=event.ts_ms,
                    displayed_size_at_price=
                        event.displayed_size_at_price,
                    best_bid=event.best_bid,
                    best_ask=event.best_ask,
                )

                # A cancel request can have been sent
                # while placement was still in flight.
                # If it is not yet effective, the quote
                # rests but remains cancel-pending.
                if (
                    not order.terminal
                    and
                    order.cancel_request_ts_ms
                    is not None
                    and
                    order.cancel_effective_ts_ms
                    is not None
                    and
                    order.cancel_effective_ts_ms
                    > event.ts_ms
                ):
                    order.state = (
                        OrderState.CANCEL_PENDING
                    )

                    order.audit.append(
                        AuditEvent(
                            event.ts_ms,
                            "preplacement_cancel_pending",
                            (
                                "quote active while "
                                "cancel remains in flight"
                            ),
                        )
                    )

            elif (
                event.kind
                is EventKind.CANCEL_REQUEST
            ):

                request_cancel(
                    order,
                    ts_ms=event.ts_ms,
                )

            elif (
                event.kind
                is EventKind.COLLECTOR_GAP
            ):

                invalidate_for_gap(
                    order,
                    ts_ms=event.ts_ms,
                    reason=(
                        event.reason
                        or "collector gap"
                    ),
                )

            elif (
                event.kind
                is EventKind.TRADE
            ):

                apply_aggressive_trade(
                    order,
                    ts_ms=event.ts_ms,
                    aggressor=event.aggressor,
                    trade_price=
                        event.trade_price,
                    trade_size=
                        event.trade_size,
                )

            elif (
                event.kind
                is EventKind.BOOK_SIZE
            ):

                apply_book_size_change(
                    order,
                    ts_ms=event.ts_ms,
                    new_displayed_size_at_price=
                        event.new_displayed_size_at_price,
                )

            elif (
                event.kind
                is EventKind.CLOCK
            ):
                # Explicit structural clock event.
                # State progression occurs below.
                pass

            else:
                raise RuntimeError(
                    f"unsupported event: "
                    f"{event.kind}"
                )

        # Frozen priority:
        # all events exactly at cancel-effective
        # timestamp occur before cancel takes effect.
        if not order.terminal:

            if (
                order.cancel_effective_ts_ms
                is not None
                and
                order.cancel_effective_ts_ms
                == ts_ms
            ):
                advance_time(
                    order,
                    ts_ms=ts_ms,
                )

        # If we reached exact placement time but
        # still have no placement snapshot and the
        # order was not cancelled/otherwise terminal,
        # refuse to invent the initial queue.
        if (
            not order.terminal
            and
            ts_ms
            ==
            order.placement_effective_ts_ms
            and
            not placement_seen
            and
            order.state
            is OrderState.PENDING_PLACEMENT
        ):
            _invalidate_missing_placement(
                order
            )

    # Explicit replay termination.
    if not order.terminal:

        if (
            not placement_seen
            and
            end_ts_ms
            >=
            order.placement_effective_ts_ms
            and
            order.state
            is OrderState.PENDING_PLACEMENT
        ):
            _invalidate_missing_placement(
                order
            )

    if not order.terminal:
        advance_time(
            order,
            ts_ms=end_ts_ms,
        )

    return ReplayResult(
        order=order,
        input_event_count=len(events),
        processed_event_count=processed,
        ignored_after_terminal=ignored_terminal,
        placement_snapshot_seen=
            placement_seen,
        end_ts_ms=end_ts_ms,
    )
