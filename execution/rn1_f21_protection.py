"""
RN1-F21-M3 latency-aware maker-protection controller.

This module does NOT calculate a trading signal.

It receives already-qualified synthetic ProtectionAlert objects and converts the
first eligible alert into a deterministic cancel request after fixed controller
latency. The actual cancel-effective semantics remain those frozen in F21-M1/M2.

It can replay two identical shadow orders:
    baseline  -> no protection-generated cancel
    protected -> first alert generates cancel

No network, DB, F19/F20 access, markout, settlement, or PnL.
"""

from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from typing import Any

from rn1_f21_replay import (
    EventKind,
    OrderSpec,
    ReplayEvent,
    ReplayResult,
    replay_passive_order,
)

from rn1_f21_shadow_mm import (
    ShadowConfig,
)


@dataclass(frozen=True)
class ProtectionConfig:
    controller_latency_ms: int

    def __post_init__(self):
        if self.controller_latency_ms < 0:
            raise ValueError(
                "controller_latency_ms must be >= 0"
            )


@dataclass(frozen=True)
class ProtectionAlert:
    alert_id: str
    ts_ms: int

    def __post_init__(self):
        if not self.alert_id:
            raise ValueError(
                "alert_id required"
            )

        if self.ts_ms < 0:
            raise ValueError(
                "alert timestamp must be >= 0"
            )


@dataclass(frozen=True)
class ProtectionPlan:
    alert_count: int

    controlling_alert_id: str | None
    alert_ts_ms: int | None

    controller_latency_ms: int

    cancel_request_ts_ms: int | None
    cancel_effective_ts_ms: int | None

    request_within_replay: bool


@dataclass
class ProtectionPairResult:
    baseline: ReplayResult
    protected: ReplayResult
    plan: ProtectionPlan

    def canonical_summary(
        self,
    ) -> dict[str, Any]:

        return {
            "plan": {
                "alert_count":
                    self.plan.alert_count,

                "controlling_alert_id":
                    self.plan.controlling_alert_id,

                "alert_ts_ms":
                    self.plan.alert_ts_ms,

                "controller_latency_ms":
                    self.plan.controller_latency_ms,

                "cancel_request_ts_ms":
                    self.plan.cancel_request_ts_ms,

                "cancel_effective_ts_ms":
                    self.plan.cancel_effective_ts_ms,

                "request_within_replay":
                    self.plan.request_within_replay,
            },

            "baseline":
                self.baseline.canonical_summary(),

            "protected":
                self.protected.canonical_summary(),
        }


def _validate_market_events(
    events: list[ReplayEvent],
) -> None:

    previous = None

    for event in events:

        if (
            previous is not None
            and event.ts_ms < previous
        ):
            raise ValueError(
                "market event timestamps must "
                "be nondecreasing"
            )

        previous = event.ts_ms

        if (
            event.kind
            is EventKind.CANCEL_REQUEST
        ):
            raise ValueError(
                "external CANCEL_REQUEST is forbidden "
                "in F21-M3 paired replay"
            )


def _validate_alerts(
    alerts: list[ProtectionAlert],
    *,
    submit_ts_ms: int,
    end_ts_ms: int,
) -> None:

    previous = None

    for alert in alerts:

        if (
            previous is not None
            and alert.ts_ms < previous
        ):
            raise ValueError(
                "alert timestamps must be nondecreasing"
            )

        previous = alert.ts_ms

        if alert.ts_ms < submit_ts_ms:
            raise ValueError(
                "protection alert precedes order submit"
            )

        if alert.ts_ms > end_ts_ms:
            raise ValueError(
                "protection alert exceeds replay end"
            )


def build_protection_plan(
    *,
    spec: OrderSpec,
    shadow_config: ShadowConfig,
    protection_config: ProtectionConfig,
    alerts: list[ProtectionAlert],
    end_ts_ms: int,
) -> ProtectionPlan:

    _validate_alerts(
        alerts,
        submit_ts_ms=spec.submit_ts_ms,
        end_ts_ms=end_ts_ms,
    )

    if not alerts:

        return ProtectionPlan(
            alert_count=0,
            controlling_alert_id=None,
            alert_ts_ms=None,
            controller_latency_ms=
                protection_config.controller_latency_ms,
            cancel_request_ts_ms=None,
            cancel_effective_ts_ms=None,
            request_within_replay=False,
        )

    # Frozen M3 rule: first already-qualified alert wins.
    alert = alerts[0]

    request_ts = (
        alert.ts_ms
        + protection_config.controller_latency_ms
    )

    effective_ts = (
        request_ts
        + shadow_config.cancel_latency_ms
    )

    return ProtectionPlan(
        alert_count=len(alerts),
        controlling_alert_id=
            alert.alert_id,
        alert_ts_ms=
            alert.ts_ms,
        controller_latency_ms=
            protection_config.controller_latency_ms,
        cancel_request_ts_ms=
            request_ts,
        cancel_effective_ts_ms=
            effective_ts,
        request_within_replay=
            request_ts <= end_ts_ms,
    )


def augment_with_protection_cancel(
    *,
    events: list[ReplayEvent],
    plan: ProtectionPlan,
) -> list[ReplayEvent]:
    """
    Preserve relative order of all original market events.

    The only inserted event is the derived protection CANCEL_REQUEST.
    Equal-timestamp event priority is still applied by frozen F21-M2.
    """

    _validate_market_events(
        events
    )

    out = list(events)

    if (
        not plan.request_within_replay
        or plan.cancel_request_ts_ms is None
    ):
        return out

    cancel = ReplayEvent(
        ts_ms=
            plan.cancel_request_ts_ms,
        kind=
            EventKind.CANCEL_REQUEST,
    )

    timestamps = [
        event.ts_ms
        for event in out
    ]

    position = bisect_right(
        timestamps,
        cancel.ts_ms,
    )

    out.insert(
        position,
        cancel,
    )

    return out


def replay_protection_pair(
    *,
    spec: OrderSpec,
    shadow_config: ShadowConfig,
    protection_config: ProtectionConfig,
    market_events: list[ReplayEvent],
    alerts: list[ProtectionAlert],
    end_ts_ms: int,
) -> ProtectionPairResult:

    _validate_market_events(
        market_events
    )

    plan = build_protection_plan(
        spec=spec,
        shadow_config=shadow_config,
        protection_config=
            protection_config,
        alerts=alerts,
        end_ts_ms=end_ts_ms,
    )

    baseline = replay_passive_order(
        spec=spec,
        config=shadow_config,
        events=list(market_events),
        end_ts_ms=end_ts_ms,
    )

    protected_events = (
        augment_with_protection_cancel(
            events=market_events,
            plan=plan,
        )
    )

    protected = replay_passive_order(
        spec=spec,
        config=shadow_config,
        events=protected_events,
        end_ts_ms=end_ts_ms,
    )

    return ProtectionPairResult(
        baseline=baseline,
        protected=protected,
        plan=plan,
    )
