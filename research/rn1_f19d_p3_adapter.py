from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from rn1_f19d_p2_core import Trade


CANONICAL_CORE_FIELDS = (
    "proxyWallet",
    "side",
    "asset",
    "conditionId",
    "size",
    "price",
    "timestamp",
    "transactionHash",
    "outcome",
    "outcomeIndex",
)


def _require_fields(row: dict[str, Any]) -> None:
    missing = [field for field in CANONICAL_CORE_FIELDS if field not in row]
    if missing:
        raise ValueError(f"missing required trade fields: {missing}")


def _nonempty(value: Any, field: str) -> str:
    text = str(value if value is not None else "").strip()
    if not text:
        raise ValueError(f"{field} must be non-empty")
    return text


def _decimal(value: Any, field: str) -> Decimal:
    try:
        out = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"invalid {field}: {value!r}") from exc
    if not out.is_finite():
        raise ValueError(f"{field} must be finite")
    return out


def _timestamp(value: Any) -> datetime:
    if isinstance(value, bool):
        raise ValueError("timestamp cannot be boolean")

    if isinstance(value, (int, float, Decimal)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (OverflowError, OSError, ValueError) as exc:
            raise ValueError(f"invalid epoch timestamp: {value!r}") from exc

    text = _nonempty(value, "timestamp")

    try:
        numeric = Decimal(text)
    except InvalidOperation:
        numeric = None

    if numeric is not None:
        if not numeric.is_finite():
            raise ValueError("timestamp must be finite")
        try:
            return datetime.fromtimestamp(float(numeric), tz=timezone.utc)
        except (OverflowError, OSError, ValueError) as exc:
            raise ValueError(f"invalid epoch timestamp: {value!r}") from exc

    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"invalid timestamp: {value!r}") from exc

    if dt.tzinfo is None:
        raise ValueError("ISO timestamp must be timezone-aware")
    return dt.astimezone(timezone.utc)


def canonicalize_row(
    row: dict[str, Any],
    *,
    expected_condition_id: str,
    rn1_wallet: str | None = None,
) -> dict[str, Any]:
    if not isinstance(row, dict):
        raise ValueError("trade row must be an object")

    _require_fields(row)

    expected_condition = _nonempty(
        expected_condition_id,
        "expected_condition_id",
    ).lower()

    condition = _nonempty(row["conditionId"], "conditionId").lower()
    if condition != expected_condition:
        raise ValueError(
            f"condition mismatch: expected {expected_condition}, got {condition}"
        )

    wallet = _nonempty(row["proxyWallet"], "proxyWallet").lower()
    if rn1_wallet is not None:
        expected_wallet = _nonempty(rn1_wallet, "rn1_wallet").lower()
        if wallet != expected_wallet:
            raise ValueError(
                f"RN1 wallet mismatch: expected {expected_wallet}, got {wallet}"
            )

    side = _nonempty(row["side"], "side").upper()
    if side not in {"BUY", "SELL"}:
        raise ValueError(f"invalid side: {side!r}")

    asset = _nonempty(row["asset"], "asset")
    tx_hash = _nonempty(row["transactionHash"], "transactionHash").lower()
    outcome = _nonempty(row["outcome"], "outcome")

    size = _decimal(row["size"], "size")
    price = _decimal(row["price"], "price")
    if size <= 0:
        raise ValueError("size must be positive")
    if price < 0 or price > 1:
        raise ValueError("price must lie in [0, 1]")

    try:
        outcome_index = int(row["outcomeIndex"])
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"invalid outcomeIndex: {row['outcomeIndex']!r}"
        ) from exc

    try:
        outcome_index_decimal = Decimal(str(row["outcomeIndex"]))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(
            f"invalid outcomeIndex: {row['outcomeIndex']!r}"
        ) from exc

    if outcome_index_decimal != Decimal(outcome_index):
        raise ValueError(
            f"outcomeIndex must be an integer: {row['outcomeIndex']!r}"
        )

    if outcome_index not in {0, 1}:
        raise ValueError("outcomeIndex must be 0 or 1")

    return {
        "proxy_wallet": wallet,
        "side": side,
        "asset": asset,
        "condition_id": condition,
        "size": size,
        "price": price,
        "timestamp": _timestamp(row["timestamp"]),
        "transaction_hash": tx_hash,
        "outcome": outcome,
        "outcome_index": outcome_index,
    }


def _canonical_tuple(fields: dict[str, Any]) -> tuple:
    return (
        fields["timestamp"],
        fields["transaction_hash"],
        fields["asset"],
        fields["side"],
        fields["price"],
        fields["size"],
        fields["outcome_index"],
        fields["proxy_wallet"],
        fields["condition_id"],
        fields["outcome"],
    )


def adapt_rows(
    rows: Iterable[dict[str, Any]],
    *,
    expected_condition_id: str,
    rn1_wallet: str | None = None,
) -> list[Trade]:
    """
    Convert raw F19b-style rows into frozen P2 Trade objects.

    No file, database, or network I/O occurs here.
    Multiplicity is retained with deterministic duplicate ordinals.
    """
    canonical = [
        canonicalize_row(
            row,
            expected_condition_id=expected_condition_id,
            rn1_wallet=rn1_wallet,
        )
        for row in rows
    ]

    canonical.sort(key=_canonical_tuple)

    seen: Counter[tuple] = Counter()
    out: list[Trade] = []

    for fields in canonical:
        key = _canonical_tuple(fields)
        ordinal = seen[key]
        seen[key] += 1

        out.append(
            Trade(
                **fields,
                duplicate_ordinal=ordinal,
            )
        )

    out.sort(key=lambda trade: trade.stable_event_key)
    return out
