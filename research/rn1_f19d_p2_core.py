from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import random
from statistics import mean
from typing import Iterable, Sequence


D = Decimal


def _utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return dt.astimezone(timezone.utc)


def _side(side: str) -> str:
    value = str(side).upper()
    if value not in {"BUY", "SELL"}:
        raise ValueError(f"invalid side: {side!r}")
    return value


def _outcome_index(value: int) -> int:
    value = int(value)
    if value not in {0, 1}:
        raise ValueError(f"outcome_index must be 0 or 1, got {value}")
    return value


def p1_exposure_direction(outcome_index: int, side: str) -> int:
    """Change in common binary p1 exposure: BUY p1 / SELL p0 => +1."""
    oi = _outcome_index(outcome_index)
    s = _side(side)
    if (oi == 0 and s == "BUY") or (oi == 1 and s == "SELL"):
        return 1
    return -1


def normalized_inventory_price(
    *,
    price: Decimal,
    outcome_index: int,
    inventory_direction: int,
) -> Decimal:
    """
    Probability price of the outcome corresponding to inventory direction.

    +1 means p1 exposure; -1 means p0 exposure.
    """
    price = D(price)
    oi = _outcome_index(outcome_index)
    if inventory_direction not in {-1, 1}:
        raise ValueError("inventory_direction must be -1 or +1")
    if not (D("0") <= price <= D("1")):
        raise ValueError("price must lie in [0, 1]")

    token_is_inventory_outcome = (
        (inventory_direction == 1 and oi == 0)
        or
        (inventory_direction == -1 and oi == 1)
    )
    return price if token_is_inventory_outcome else D("1") - price


def game_phase(
    *,
    event_time: datetime,
    game_start_time: datetime,
) -> str:
    seconds = (_utc(event_time) - _utc(game_start_time)).total_seconds()
    if seconds < 0:
        return "pregame"
    if seconds < 300:
        return "live_0_5m"
    if seconds < 1800:
        return "live_5_30m"
    if seconds < 3600:
        return "live_30_60m"
    if seconds < 7200:
        return "live_60_120m"
    return "live_120m_plus"


@dataclass(frozen=True)
class Trade:
    proxy_wallet: str
    side: str
    asset: str
    condition_id: str
    size: Decimal
    price: Decimal
    timestamp: datetime
    transaction_hash: str
    outcome: str
    outcome_index: int
    duplicate_ordinal: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "side", _side(self.side))
        object.__setattr__(self, "outcome_index", _outcome_index(self.outcome_index))
        object.__setattr__(self, "size", D(self.size))
        object.__setattr__(self, "price", D(self.price))
        object.__setattr__(self, "timestamp", _utc(self.timestamp))
        if self.size <= 0:
            raise ValueError("trade size must be positive")
        if not (D("0") <= self.price <= D("1")):
            raise ValueError("trade price must lie in [0, 1]")
        if self.duplicate_ordinal < 0:
            raise ValueError("duplicate_ordinal must be nonnegative")

    @property
    def core_key(self) -> tuple:
        return (
            self.proxy_wallet,
            self.side,
            self.asset,
            self.condition_id,
            str(self.size),
            str(self.price),
            self.timestamp.isoformat(),
            self.transaction_hash,
            self.outcome,
            self.outcome_index,
        )

    @property
    def stable_event_key(self) -> tuple:
        return (
            self.timestamp,
            self.transaction_hash,
            self.asset,
            self.side,
            self.price,
            self.size,
            self.outcome_index,
            self.duplicate_ordinal,
        )

    @property
    def base_notional(self) -> Decimal:
        return self.size * self.price

    @property
    def p1_direction(self) -> int:
        return p1_exposure_direction(self.outcome_index, self.side)


def classify_rn1_roles(
    *,
    rn1_all: Sequence[Trade],
    market_taker: Sequence[Trade],
    rn1_wallet: str,
) -> tuple[list[Trade], list[Trade]]:
    """
    Multiset-match RN1 rows against RN1 rows present in market_taker.

    Returns (rn1_taker, rn1_maker). Multiplicity is preserved and any
    inconsistent self-taker multiplicity fails closed.
    """
    if any(t.proxy_wallet != rn1_wallet for t in rn1_all):
        raise ValueError("rn1_all contains a non-RN1 wallet row")

    taker_counts = Counter(
        t.core_key
        for t in market_taker
        if t.proxy_wallet == rn1_wallet
    )
    rn1_taker: list[Trade] = []
    rn1_maker: list[Trade] = []

    for trade in sorted(rn1_all, key=lambda t: t.stable_event_key):
        key = trade.core_key
        if taker_counts[key] > 0:
            taker_counts[key] -= 1
            rn1_taker.append(trade)
        else:
            rn1_maker.append(trade)

    leftovers = sum(taker_counts.values())
    if leftovers:
        raise ValueError(
            "market_taker contains RN1 self-taker multiplicity absent from rn1_all"
        )

    return rn1_taker, rn1_maker


def decision_time(maker_fill_time: datetime) -> datetime:
    return _utc(maker_fill_time) - timedelta(seconds=5)


def signal_window(maker_fill_time: datetime) -> tuple[datetime, datetime]:
    d = decision_time(maker_fill_time)
    return d - timedelta(seconds=30), d - timedelta(seconds=1)


def flow_imbalance(
    *,
    maker_inventory_direction: int,
    maker_fill_time: datetime,
    market_taker: Iterable[Trade],
    rn1_wallet: str,
) -> Decimal | None:
    """
    Frozen F18 signal over inclusive [decision-30s, decision-1s].

    Non-self taker flow only. Returns None when denominator is zero.
    """
    if maker_inventory_direction not in {-1, 1}:
        raise ValueError("maker_inventory_direction must be -1 or +1")

    start, end = signal_window(maker_fill_time)
    signed = D("0")
    gross = D("0")

    for trade in market_taker:
        if trade.proxy_wallet == rn1_wallet:
            continue
        if trade.timestamp < start or trade.timestamp > end:
            continue
        notional = trade.base_notional
        relative_sign = trade.p1_direction * maker_inventory_direction
        signed += D(relative_sign) * notional
        gross += notional

    if gross == 0:
        return None
    return signed / gross


def is_flagged(imbalance: Decimal | None) -> bool:
    return imbalance is not None and D(imbalance) <= D("-0.75")


@dataclass(frozen=True)
class MakerOpportunity:
    trade: Trade
    game_start_time: datetime
    flagged: bool

    @property
    def inventory_direction(self) -> int:
        return self.trade.p1_direction

    @property
    def phase(self) -> str:
        return game_phase(
            event_time=self.trade.timestamp,
            game_start_time=self.game_start_time,
        )

    @property
    def normalized_price(self) -> Decimal:
        return normalized_inventory_price(
            price=self.trade.price,
            outcome_index=self.trade.outcome_index,
            inventory_direction=self.inventory_direction,
        )

    @property
    def stable_event_key(self) -> tuple:
        return self.trade.stable_event_key


def control_is_eligible(
    flagged: MakerOpportunity,
    control: MakerOpportunity,
) -> bool:
    if not flagged.flagged:
        raise ValueError("flagged opportunity must be flagged")
    if control.flagged:
        return False
    if control.trade.condition_id != flagged.trade.condition_id:
        return False
    if control.inventory_direction != flagged.inventory_direction:
        return False
    if control.phase != flagged.phase:
        return False

    price_distance = abs(control.normalized_price - flagged.normalized_price)
    if price_distance > D("0.05"):
        return False

    ratio = control.trade.size / flagged.trade.size
    if ratio < D("0.5") or ratio > D("2.0"):
        return False

    seconds = abs(
        (control.trade.timestamp - flagged.trade.timestamp).total_seconds()
    )
    if seconds < 120 or seconds > 900:
        return False

    return True


def _size_distance_from_ratio_1(
    flagged: MakerOpportunity,
    control: MakerOpportunity,
) -> Decimal:
    a = control.trade.size / flagged.trade.size
    b = flagged.trade.size / control.trade.size
    return max(a, b)


def control_sort_key(
    flagged: MakerOpportunity,
    control: MakerOpportunity,
) -> tuple:
    return (
        abs((control.trade.timestamp - flagged.trade.timestamp).total_seconds()),
        abs(control.normalized_price - flagged.normalized_price),
        _size_distance_from_ratio_1(flagged, control),
        control.trade.timestamp,
        control.stable_event_key,
    )


def flagged_processing_key(event: MakerOpportunity) -> tuple:
    return (
        _utc(event.game_start_time),
        event.trade.condition_id,
        event.trade.timestamp,
        event.stable_event_key,
    )


def select_controls(
    *,
    flagged_events: Sequence[MakerOpportunity],
    control_pool: Sequence[MakerOpportunity],
) -> dict[tuple, list[MakerOpportunity]]:
    """
    Greedy no-reuse matching, frozen by P1.

    Matching does not inspect future-markout availability.
    """
    used: set[tuple] = set()
    result: dict[tuple, list[MakerOpportunity]] = {}

    for flagged in sorted(flagged_events, key=flagged_processing_key):
        eligible = [
            c
            for c in control_pool
            if c.stable_event_key not in used
            and control_is_eligible(flagged, c)
        ]
        eligible.sort(key=lambda c: control_sort_key(flagged, c))
        chosen = eligible[:3]
        result[flagged.stable_event_key] = chosen
        used.update(c.stable_event_key for c in chosen)

    return result


def _opposite_side(side: str) -> str:
    return "SELL" if _side(side) == "BUY" else "BUY"


def future_markout_print(
    *,
    maker: MakerOpportunity,
    market_taker: Sequence[Trade],
) -> Trade | None:
    target = maker.trade.timestamp + timedelta(seconds=15)
    latest = target + timedelta(seconds=10)
    aggressor_side = _opposite_side(maker.trade.side)

    candidates = [
        t
        for t in market_taker
        if t.asset == maker.trade.asset
        and t.side == aggressor_side
        and target <= t.timestamp <= latest
    ]

    if not candidates:
        return None

    candidates.sort(key=lambda t: (t.timestamp, t.stable_event_key))
    return candidates[0]


def primary_markout(
    *,
    maker: MakerOpportunity,
    market_taker: Sequence[Trade],
) -> Decimal | None:
    future = future_markout_print(
        maker=maker,
        market_taker=market_taker,
    )
    if future is None:
        return None

    entry = maker.normalized_price
    future_price = normalized_inventory_price(
        price=future.price,
        outcome_index=future.outcome_index,
        inventory_direction=maker.inventory_direction,
    )
    return future_price - entry


@dataclass(frozen=True)
class MatchedResult:
    flagged: MakerOpportunity
    selected_controls: tuple[MakerOpportunity, ...]
    flagged_markout: Decimal | None
    valid_control_markouts: tuple[Decimal, ...]

    @property
    def matched(self) -> bool:
        return len(self.selected_controls) >= 1

    @property
    def usable(self) -> bool:
        return (
            self.matched
            and self.flagged_markout is not None
            and len(self.valid_control_markouts) >= 1
        )

    @property
    def control_mean(self) -> Decimal | None:
        if not self.valid_control_markouts:
            return None
        return sum(self.valid_control_markouts, D("0")) / D(
            len(self.valid_control_markouts)
        )

    @property
    def event_difference(self) -> Decimal | None:
        if not self.usable:
            return None
        assert self.flagged_markout is not None
        assert self.control_mean is not None
        return self.flagged_markout - self.control_mean


def build_matched_result(
    *,
    flagged: MakerOpportunity,
    selected_controls: Sequence[MakerOpportunity],
    market_taker: Sequence[Trade],
) -> MatchedResult:
    flagged_m = primary_markout(
        maker=flagged,
        market_taker=market_taker,
    )
    control_ms = tuple(
        m
        for m in (
            primary_markout(maker=c, market_taker=market_taker)
            for c in selected_controls
        )
        if m is not None
    )
    return MatchedResult(
        flagged=flagged,
        selected_controls=tuple(selected_controls),
        flagged_markout=flagged_m,
        valid_control_markouts=control_ms,
    )


def decluster_usable(
    results: Sequence[MatchedResult],
) -> list[MatchedResult]:
    """
    Greedy earliest-event retention, grouped only by condition_id.

    Opposite inventory directions are NOT separate clusters.
    """
    grouped: dict[str, list[MatchedResult]] = defaultdict(list)
    for result in results:
        if result.usable:
            grouped[result.flagged.trade.condition_id].append(result)

    kept: list[MatchedResult] = []

    for condition_id in sorted(grouped):
        events = sorted(
            grouped[condition_id],
            key=lambda r: (
                r.flagged.trade.timestamp,
                r.flagged.stable_event_key,
            ),
        )
        last_time: datetime | None = None
        for result in events:
            ts = result.flagged.trade.timestamp
            if last_time is None or (ts - last_time).total_seconds() >= 60:
                kept.append(result)
                last_time = ts

    return kept


def market_equal_difference(
    results: Sequence[MatchedResult],
) -> Decimal | None:
    per_market: dict[str, list[Decimal]] = defaultdict(list)

    for result in results:
        diff = result.event_difference
        if diff is not None:
            per_market[result.flagged.trade.condition_id].append(diff)

    if not per_market:
        return None

    market_means = [
        sum(values, D("0")) / D(len(values))
        for _, values in sorted(per_market.items())
    ]
    return sum(market_means, D("0")) / D(len(market_means))


def event_equal_difference(
    results: Sequence[MatchedResult],
) -> Decimal | None:
    values = [
        r.event_difference
        for r in results
        if r.event_difference is not None
    ]
    if not values:
        return None
    return sum(values, D("0")) / D(len(values))


def per_market_primary_means(
    results: Sequence[MatchedResult],
) -> dict[str, Decimal]:
    grouped: dict[str, list[Decimal]] = defaultdict(list)
    for result in results:
        diff = result.event_difference
        if diff is not None:
            grouped[result.flagged.trade.condition_id].append(diff)

    return {
        condition_id: sum(values, D("0")) / D(len(values))
        for condition_id, values in sorted(grouped.items())
    }



def market_medians(
    results: Sequence[MatchedResult],
) -> dict[str, Decimal]:
    grouped: dict[str, list[Decimal]] = defaultdict(list)
    for result in results:
        diff = result.event_difference
        if diff is not None:
            grouped[result.flagged.trade.condition_id].append(diff)

    out: dict[str, Decimal] = {}
    for condition_id, values in sorted(grouped.items()):
        values = sorted(values)
        n = len(values)
        mid = n // 2
        if n % 2:
            med = values[mid]
        else:
            med = (values[mid - 1] + values[mid]) / D("2")
        out[condition_id] = med
    return out


def market_sign_count(
    per_market: dict[str, Decimal],
) -> dict[str, int]:
    counts = {"negative": 0, "zero": 0, "positive": 0}
    for value in per_market.values():
        if value < 0:
            counts["negative"] += 1
        elif value > 0:
            counts["positive"] += 1
        else:
            counts["zero"] += 1
    return counts


def leave_one_market_out(
    per_market: dict[str, Decimal],
) -> dict[str, Decimal]:
    keys = sorted(per_market)
    if len(keys) < 2:
        return {}
    total = sum((per_market[k] for k in keys), D("0"))
    return {
        omitted: (total - per_market[omitted]) / D(len(keys) - 1)
        for omitted in keys
    }

def bootstrap_market_mean_ci(
    per_market: dict[str, Decimal],
    *,
    draws: int = 10_000,
    seed: int = 19018,
) -> tuple[Decimal, Decimal] | None:
    if draws <= 0:
        raise ValueError("draws must be positive")
    values = [per_market[k] for k in sorted(per_market)]
    if not values:
        return None

    rng = random.Random(seed)
    n = len(values)
    sims: list[Decimal] = []

    for _ in range(draws):
        sample = [values[rng.randrange(n)] for _ in range(n)]
        sims.append(sum(sample, D("0")) / D(n))

    sims.sort()

    # Deterministic nearest-rank percentile indices for 2.5% / 97.5%.
    lo_index = max(0, int((draws - 1) * 0.025))
    hi_index = min(draws - 1, int((draws - 1) * 0.975))
    return sims[lo_index], sims[hi_index]


@dataclass(frozen=True)
class ConfirmationDecision:
    status: str
    matched_flagged_events: int
    usable_markets: int
    primary_market_equal: Decimal | None
    declustered_market_equal: Decimal | None
    bootstrap_ci: tuple[Decimal, Decimal] | None


def confirmation_decision(
    *,
    matched_flagged_events: int,
    usable_market_count: int,
    primary_market_equal: Decimal | None,
    declustered_market_equal: Decimal | None,
    bootstrap_ci: tuple[Decimal, Decimal] | None,
) -> ConfirmationDecision:
    if matched_flagged_events < 0 or usable_market_count < 0:
        raise ValueError("counts must be nonnegative")

    sample_pass = (
        matched_flagged_events >= 50
        and usable_market_count >= 20
    )

    if not sample_pass:
        status = "INCONCLUSIVE_INSUFFICIENT_SAMPLE"
    else:
        confirmed = (
            primary_market_equal is not None
            and declustered_market_equal is not None
            and primary_market_equal < 0
            and declustered_market_equal < 0
        )
        if not confirmed:
            status = "NOT_CONFIRMED"
        elif (
            bootstrap_ci is not None
            and bootstrap_ci[1] < 0
        ):
            status = "STRONGLY_CONFIRMED"
        else:
            status = "CONFIRMED"

    return ConfirmationDecision(
        status=status,
        matched_flagged_events=matched_flagged_events,
        usable_markets=usable_market_count,
        primary_market_equal=primary_market_equal,
        declustered_market_equal=declustered_market_equal,
        bootstrap_ci=bootstrap_ci,
    )
