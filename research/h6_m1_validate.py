from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable

REPO = Path(__file__).resolve().parents[1]
CONTRACT_PATH = REPO / "research" / "h6_m1_validation_contract.json"
REGISTRY_PATH = REPO / "data" / "research" / "h6" / "m1" / "registry.jsonl"

EXPECTED_CONTRACT_SHA256 = (
    "1c3f34dfa252dffa2614572b94ea2ddb317deafe05df8eaa279f0d76db491d69"
)

STREAM_FILENAMES = {
    "raw_frames": "raw_frames.jsonl",
    "normalized": "normalized.jsonl",
    "rest_snapshots": "rest_snapshots.jsonl",
    "control": "control.jsonl",
    "boundary_states": "boundary_states.jsonl",
}

WINDOW_SECONDS = 1800


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def parse_dt(value: Any) -> datetime:
    if not value:
        raise ValueError("missing datetime")
    dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError(f"naive datetime: {value!r}")
    return dt.astimezone(timezone.utc)


def dec(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not result.is_finite():
        return None
    return result


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, start=1):
            if not line.strip():
                continue
            value = json.loads(line)
            if not isinstance(value, dict):
                raise ValueError(f"{path}:{lineno}: row is not an object")
            rows.append(value)
    return rows


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def git_head(repo: Path = REPO) -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        text=True,
    ).strip()


def committed_self_matches_head(path: Path | None = None, repo: Path = REPO) -> bool:
    path = path or Path(__file__).resolve()
    try:
        rel = path.relative_to(repo).as_posix()
        committed = subprocess.check_output(
            ["git", "show", f"HEAD:{rel}"],
            cwd=repo,
        )
    except (ValueError, subprocess.CalledProcessError):
        return False
    return sha256_bytes(committed) == sha256_file(path)


@dataclass(frozen=True)
class MarketMeta:
    condition_id: str
    p1_token: str
    p0_token: str
    game_start: datetime

    @property
    def window_start(self) -> datetime:
        return self.game_start - timedelta(seconds=WINDOW_SECONDS)


@dataclass
class State:
    token: str
    connection_id: str = ""
    generation: int = 0
    bids: dict[Decimal, Decimal] | None = None
    asks: dict[Decimal, Decimal] | None = None
    tick_size: Decimal | None = None
    initialized: bool = False
    continuity_valid: bool = False
    last_venue_timestamp_ms: int | None = None

    def __post_init__(self) -> None:
        if self.bids is None:
            self.bids = {}
        if self.asks is None:
            self.asks = {}

    @staticmethod
    def levels(raw: Any) -> dict[Decimal, Decimal]:
        if not isinstance(raw, list):
            raise ValueError("book side is not a list")
        out: dict[Decimal, Decimal] = {}
        for level in raw:
            if not isinstance(level, dict):
                raise ValueError("book level is not an object")
            price = dec(level.get("price"))
            size = dec(level.get("size"))
            if price is None or size is None:
                raise ValueError("invalid book level decimal")
            if not Decimal("0") < price < Decimal("1"):
                raise ValueError("book price outside (0,1)")
            if size < 0:
                raise ValueError("negative book size")
            if size == 0:
                continue
            if price in out:
                raise ValueError("duplicate price level")
            out[price] = size
        return out

    @staticmethod
    def venue_timestamp(value: Any) -> int | None:
        if value is None:
            return None
        try:
            result = int(str(value))
        except (TypeError, ValueError):
            return None
        return result if result >= 0 else None

    def stale(self, value: Any) -> bool:
        ts = self.venue_timestamp(value)
        return bool(
            ts is not None
            and self.last_venue_timestamp_ms is not None
            and ts < self.last_venue_timestamp_ms
        )

    def observe_timestamp(self, value: Any) -> None:
        ts = self.venue_timestamp(value)
        if ts is None:
            return
        if self.last_venue_timestamp_ms is None or ts > self.last_venue_timestamp_ms:
            self.last_venue_timestamp_ms = ts

    def replace_full(
        self,
        book: dict[str, Any],
        *,
        connection_id: str,
        increment_generation: bool,
    ) -> None:
        asset = str(book.get("asset_id") or "")
        if asset and asset != self.token:
            raise ValueError("book token mismatch")
        bids = self.levels(book.get("bids") or [])
        asks = self.levels(book.get("asks") or [])
        if increment_generation:
            self.generation += 1
        self.connection_id = connection_id
        self.bids = bids
        self.asks = asks
        tick = dec(book.get("tick_size"))
        if tick is not None:
            if tick <= 0:
                raise ValueError("invalid tick size")
            self.tick_size = tick
        self.observe_timestamp(book.get("timestamp"))
        self.initialized = True
        self.continuity_valid = True

    def apply_level(self, side: Any, price: Any, size: Any) -> None:
        if not self.initialized or not self.continuity_valid:
            raise ValueError("cannot mutate invalid state")
        p = dec(price)
        z = dec(size)
        if p is None or z is None:
            raise ValueError("invalid price_change decimal")
        if not Decimal("0") < p < Decimal("1"):
            raise ValueError("price_change price outside (0,1)")
        if z < 0:
            raise ValueError("negative price_change size")
        side_text = str(side or "").upper()
        if side_text == "BUY":
            levels = self.bids
        elif side_text == "SELL":
            levels = self.asks
        else:
            raise ValueError("unknown price_change side")
        assert levels is not None
        if z == 0:
            levels.pop(p, None)
        else:
            levels[p] = z

    def apply_tick(self, value: Any) -> None:
        tick = dec(value)
        if tick is None or tick <= 0:
            raise ValueError("invalid tick size")
        self.tick_size = tick

    def best(self) -> tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None]:
        assert self.bids is not None and self.asks is not None
        bid = max(self.bids, default=None)
        ask = min(self.asks, default=None)
        bid_size = self.bids.get(bid) if bid is not None else None
        ask_size = self.asks.get(ask) if ask is not None else None
        return bid, bid_size, ask, ask_size

    def structurally_valid(self) -> bool:
        if not self.initialized or not self.continuity_valid:
            return False
        bid, bid_size, ask, ask_size = self.best()
        return bool(
            bid is not None
            and ask is not None
            and bid_size is not None
            and ask_size is not None
            and bid_size > 0
            and ask_size > 0
            and bid < ask
        )

    def same_full_book(self, book: dict[str, Any]) -> bool:
        try:
            bids = self.levels(book.get("bids") or [])
            asks = self.levels(book.get("asks") or [])
        except ValueError:
            return False
        tick = dec(book.get("tick_size"))
        tick_equal = (
            tick is None
            or self.tick_size is None
            or tick == self.tick_size
        )
        return self.bids == bids and self.asks == asks and tick_equal

    def snapshot(self) -> dict[str, Any]:
        bid, bid_size, ask, ask_size = self.best()
        assert self.bids is not None and self.asks is not None
        return {
            "token": self.token,
            "connection_id": self.connection_id,
            "book_generation": self.generation,
            "initialized": self.initialized,
            "valid": self.continuity_valid,
            "structurally_valid": self.structurally_valid(),
            "tick_size": str(self.tick_size) if self.tick_size is not None else None,
            "last_venue_timestamp_ms": self.last_venue_timestamp_ms,
            "best_bid": str(bid) if bid is not None else None,
            "best_bid_size": str(bid_size) if bid_size is not None else None,
            "best_ask": str(ask) if ask is not None else None,
            "best_ask_size": str(ask_size) if ask_size is not None else None,
            "bids": [
                {"price": str(x), "size": str(self.bids[x])}
                for x in sorted(self.bids, reverse=True)
            ],
            "asks": [
                {"price": str(x), "size": str(self.asks[x])}
                for x in sorted(self.asks)
            ],
        }


def imbalance(state: State) -> Decimal | None:
    if not state.structurally_valid():
        return None
    _, bid_size, _, ask_size = state.best()
    assert bid_size is not None and ask_size is not None
    denom = bid_size + ask_size
    if denom <= 0:
        return None
    return (bid_size - ask_size) / denom


def market_metadata(registry_rows: list[dict[str, Any]]) -> tuple[
    dict[str, MarketMeta],
    dict[str, tuple[str, str]],
]:
    markets: dict[str, MarketMeta] = {}
    token_meta: dict[str, tuple[str, str]] = {}
    for row in registry_rows:
        cid = str(row["condition_id"]).lower()
        p1 = str(row["p1_token_id"])
        p0 = str(row["p0_token_id"])
        start = parse_dt(row["game_start_time_utc"])
        if cid in markets:
            raise ValueError(f"duplicate condition: {cid}")
        markets[cid] = MarketMeta(
            condition_id=cid,
            p1_token=p1,
            p0_token=p0,
            game_start=start,
        )
        for token, role in ((p1, "p1"), (p0, "p0")):
            if token in token_meta:
                raise ValueError(f"duplicate token: {token}")
            token_meta[token] = (cid, role)
    return markets, token_meta


def group_rest_batches(rest_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rest_rows:
        key = (
            str(row.get("connection_id") or ""),
            str(row.get("reason") or ""),
            str(row.get("request_started_at_utc") or ""),
            str(row.get("response_received_at_utc") or ""),
        )
        grouped[key].append(row)

    batches: list[dict[str, Any]] = []
    for key, rows in grouped.items():
        connection_id, reason, requested, received = key
        batches.append({
            "kind": "rest",
            "time": parse_dt(received),
            "connection_id": connection_id,
            "reason": reason,
            "request_started_at_utc": requested,
            "response_received_at_utc": received,
            "rows": sorted(rows, key=lambda x: str(x.get("token") or "")),
        })
    return batches


def raw_events(raw_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for index, row in enumerate(raw_rows):
        events.append({
            "kind": "raw",
            "time": parse_dt(row["capture_time"]),
            "connection_id": str(row.get("connection_id") or ""),
            "frame_sequence": int(row["frame_sequence"]),
            "row_index": index,
            "row": row,
        })
    return events


def event_sort_key(event: dict[str, Any]) -> tuple[Any, ...]:
    # Exact timestamp ties are not expected; raw-before-REST is fixed for determinism.
    priority = 0 if event["kind"] == "raw" else 1
    if event["kind"] == "raw":
        secondary = (
            event["connection_id"],
            event["frame_sequence"],
            event["row_index"],
        )
    else:
        secondary = (
            event["connection_id"],
            event["reason"],
            event["request_started_at_utc"],
        )
    return (event["time"], priority, secondary)


class Replay:
    def __init__(
        self,
        registry_rows: list[dict[str, Any]],
        raw_rows: list[dict[str, Any]],
        rest_rows: list[dict[str, Any]],
        control_rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self.registry_rows = registry_rows
        self.raw_rows = raw_rows
        self.rest_rows = rest_rows
        self.control_rows = control_rows or []

        self.markets, self.token_meta = market_metadata(registry_rows)
        self.states = {token: State(token) for token in self.token_meta}

        self.p1_by_condition = {
            cid: meta.p1_token for cid, meta in self.markets.items()
        }
        self.coverage_cursor = {
            cid: meta.window_start for cid, meta in self.markets.items()
        }
        self.coverage_seconds = {cid: 0.0 for cid in self.markets}

        self.transitions: list[dict[str, Any]] = []
        self.transition_counts = Counter()
        self.message_type_counts = Counter()

        self.reconstruction_integrity_errors: list[str] = []
        self.generation_or_connection_mixing = 0
        self.future_backfill = 0
        self.in_play_primary_transitions = 0
        self.state_errors = 0
        self.venue_tob_mismatch_tokens = 0
        self.rest_reconciliation_mismatches = 0
        self.rest_flag_crosscheck_failures = 0
        self.rest_generation_crosscheck_failures = 0
        self.post_frame_crosscheck_failures = 0

        self.initial_p1_connect_tokens: set[str] = set()
        self.connection_ids: set[str] = set()
        self.generation_history: list[dict[str, Any]] = []

        self.expected_post_frame: dict[tuple[str, int, str], dict[str, Any]] = {}
        for row in self.control_rows:
            if row.get("kind") != "post_frame_state":
                continue
            token = str(row.get("token") or "")
            connection_id = str(row.get("connection_id") or "")
            frame_sequence = int(row.get("frame_sequence"))
            state = row.get("state")
            if token and isinstance(state, dict):
                self.expected_post_frame[(connection_id, frame_sequence, token)] = state

    def valid_for_market(self, cid: str) -> bool:
        token = self.p1_by_condition[cid]
        return self.states[token].structurally_valid()

    def advance_coverage(self, event_time: datetime) -> None:
        for cid, meta in self.markets.items():
            cursor = self.coverage_cursor[cid]
            end = min(event_time, meta.game_start)
            if end <= cursor:
                continue
            if self.valid_for_market(cid):
                self.coverage_seconds[cid] += (end - cursor).total_seconds()
            self.coverage_cursor[cid] = end

    def finish_coverage(self) -> None:
        for cid, meta in self.markets.items():
            self.advance_coverage(meta.game_start)

    def invalidate_for_new_connection(self, connection_id: str) -> None:
        for state in self.states.values():
            if (
                state.initialized
                and state.continuity_valid
                and state.connection_id
                and state.connection_id != connection_id
            ):
                state.continuity_valid = False

    def apply_rest_batch(self, event: dict[str, Any]) -> None:
        conn = event["connection_id"]
        reason = event["reason"]
        self.connection_ids.add(conn)
        self.invalidate_for_new_connection(conn)

        for row in event["rows"]:
            token = str(row.get("token") or "")
            if token not in self.states:
                self.reconstruction_integrity_errors.append(
                    f"REST unknown token {token}"
                )
                continue

            state = self.states[token]
            book = row.get("book")
            if not isinstance(book, dict):
                self.reconstruction_integrity_errors.append(
                    f"REST missing book {token}"
                )
                state.continuity_valid = False
                continue

            before_generation = state.generation

            if reason == "reconciliation_check":
                independent_match = (
                    state.continuity_valid
                    and state.same_full_book(book)
                )
                stored_match = row.get("matched_local_state_before")
                if stored_match is not independent_match:
                    self.rest_flag_crosscheck_failures += 1

                if independent_match:
                    state.observe_timestamp(book.get("timestamp"))
                else:
                    self.rest_reconciliation_mismatches += 1
                    try:
                        state.replace_full(
                            book,
                            connection_id=conn,
                            increment_generation=True,
                        )
                    except ValueError as exc:
                        state.continuity_valid = False
                        self.state_errors += 1
                        self.reconstruction_integrity_errors.append(
                            f"REST reconciliation state error {token}: {exc}"
                        )
            else:
                try:
                    state.replace_full(
                        book,
                        connection_id=conn,
                        increment_generation=True,
                    )
                except ValueError as exc:
                    state.continuity_valid = False
                    self.state_errors += 1
                    self.reconstruction_integrity_errors.append(
                        f"REST resync state error {token}: {exc}"
                    )

            if reason == "connect":
                cid, role = self.token_meta[token]
                if (
                    role == "p1"
                    and event["time"] <= self.markets[cid].window_start
                ):
                    self.initial_p1_connect_tokens.add(token)

            stored_generation = row.get("book_generation")
            if (
                stored_generation is not None
                and int(stored_generation) != state.generation
            ):
                self.rest_generation_crosscheck_failures += 1

            if state.generation != before_generation:
                self.generation_history.append({
                    "time": event["time"].isoformat(),
                    "token": token,
                    "connection_id": conn,
                    "generation": state.generation,
                    "reason": reason,
                })

    def _state_snapshot_for_transition(self, state: State) -> dict[str, Any]:
        bid, bid_size, ask, ask_size = state.best()
        return {
            "connection_id": state.connection_id,
            "generation": state.generation,
            "valid": state.structurally_valid(),
            "bid": bid,
            "bid_size": bid_size,
            "ask": ask,
            "ask_size": ask_size,
            "tick": state.tick_size,
            "imbalance": imbalance(state),
        }

    def _maybe_transition(
        self,
        *,
        token: str,
        pre: dict[str, Any],
        post: dict[str, Any],
        capture_time: datetime,
        connection_id: str,
        frame_sequence: int,
        mismatch: bool,
    ) -> None:
        cid, role = self.token_meta[token]
        if role != "p1":
            return
        meta = self.markets[cid]

        if capture_time >= meta.game_start:
            # An otherwise valid transition after game start would be contamination.
            if pre["valid"] and post["valid"]:
                if (
                    pre["bid"] == post["bid"]
                    and pre["ask"] == post["ask"]
                    and pre["tick"] == post["tick"]
                    and (
                        pre["bid_size"] != post["bid_size"]
                        or pre["ask_size"] != post["ask_size"]
                    )
                    and pre["imbalance"] != post["imbalance"]
                ):
                    self.in_play_primary_transitions += 1
            return

        if capture_time < meta.window_start:
            return
        if mismatch:
            return
        if not pre["valid"] or not post["valid"]:
            return

        if (
            pre["connection_id"] != post["connection_id"]
            or pre["generation"] != post["generation"]
            or post["connection_id"] != connection_id
        ):
            self.generation_or_connection_mixing += 1
            return

        if pre["bid"] != post["bid"] or pre["ask"] != post["ask"]:
            return
        if pre["tick"] != post["tick"]:
            return
        if (
            pre["bid_size"] == post["bid_size"]
            and pre["ask_size"] == post["ask_size"]
        ):
            return

        pre_imb = pre["imbalance"]
        post_imb = post["imbalance"]
        if pre_imb is None or post_imb is None:
            return
        change = post_imb - pre_imb
        if change == 0:
            return

        identity = {
            "condition_id": cid,
            "token": token,
            "capture_time": capture_time.isoformat(),
            "connection_id": connection_id,
            "frame_sequence": frame_sequence,
            "book_generation": post["generation"],
            "imbalance_change": str(change),
        }
        self.transitions.append(identity)
        self.transition_counts[cid] += 1

    def apply_raw_frame(self, event: dict[str, Any]) -> None:
        row = event["row"]
        conn = event["connection_id"]
        frame_sequence = event["frame_sequence"]
        capture_time = event["time"]

        self.connection_ids.add(conn)
        self.invalidate_for_new_connection(conn)

        raw = row.get("raw")
        if not isinstance(raw, str):
            self.reconstruction_integrity_errors.append(
                f"raw frame is not text: {conn}/{frame_sequence}"
            )
            return

        expected_sha = str(row.get("raw_frame_sha256") or "")
        if sha256_bytes(raw.encode("utf-8")) != expected_sha:
            self.reconstruction_integrity_errors.append(
                f"raw SHA mismatch: {conn}/{frame_sequence}"
            )
            return

        if raw == "PONG":
            self.message_type_counts["PONG"] += 1
            return

        try:
            decoded = json.loads(raw)
        except json.JSONDecodeError:
            self.message_type_counts["unparseable"] += 1
            return

        messages = decoded if isinstance(decoded, list) else [decoded]
        if not all(isinstance(x, dict) for x in messages):
            self.message_type_counts["invalid_shape"] += 1
            return

        pre = {
            token: self._state_snapshot_for_transition(state)
            for token, state in self.states.items()
            if self.token_meta[token][1] == "p1"
        }

        touched: set[str] = set()
        venue_tob: dict[str, tuple[Any, Any]] = {}
        state_error_tokens: set[str] = set()

        for message in messages:
            event_type = str(message.get("event_type") or "unknown")
            self.message_type_counts[event_type] += 1
            token = (
                str(message.get("asset_id"))
                if message.get("asset_id")
                else None
            )
            msg_ts = message.get("timestamp")

            if event_type == "book" and token in self.states:
                state = self.states[token]
                if capture_time >= self.markets[self.token_meta[token][0]].game_start:
                    continue
                if state.stale(msg_ts):
                    continue
                try:
                    state.replace_full(
                        message,
                        connection_id=conn,
                        increment_generation=(
                            not state.continuity_valid
                            or not state.initialized
                        ),
                    )
                    state.observe_timestamp(msg_ts)
                    touched.add(token)
                except ValueError:
                    state.continuity_valid = False
                    state_error_tokens.add(token)

            elif event_type == "price_change":
                changes = message.get("price_changes") or message.get("changes") or []
                if not isinstance(changes, list):
                    continue
                for change in changes:
                    if not isinstance(change, dict):
                        continue
                    tok = (
                        str(change.get("asset_id"))
                        if change.get("asset_id")
                        else token
                    )
                    if tok not in self.states:
                        continue
                    if capture_time >= self.markets[self.token_meta[tok][0]].game_start:
                        continue
                    state = self.states[tok]
                    if state.stale(msg_ts):
                        continue
                    try:
                        state.apply_level(
                            change.get("side"),
                            change.get("price"),
                            change.get("size"),
                        )
                        state.observe_timestamp(msg_ts)
                        touched.add(tok)
                    except ValueError:
                        state.continuity_valid = False
                        state_error_tokens.add(tok)
                        continue
                    if (
                        change.get("best_bid") is not None
                        or change.get("best_ask") is not None
                    ):
                        venue_tob[tok] = (
                            change.get("best_bid"),
                            change.get("best_ask"),
                        )

            elif event_type == "tick_size_change" and token in self.states:
                if capture_time >= self.markets[self.token_meta[token][0]].game_start:
                    continue
                state = self.states[token]
                if state.stale(msg_ts):
                    continue
                try:
                    state.apply_tick(message.get("new_tick_size"))
                    state.observe_timestamp(msg_ts)
                    touched.add(token)
                except ValueError:
                    state.continuity_valid = False
                    state_error_tokens.add(token)

            elif event_type == "best_bid_ask" and token in self.states:
                if capture_time >= self.markets[self.token_meta[token][0]].game_start:
                    continue
                state = self.states[token]
                if state.stale(msg_ts):
                    continue
                state.observe_timestamp(msg_ts)
                venue_tob[token] = (
                    message.get("best_bid"),
                    message.get("best_ask"),
                )

        mismatches: set[str] = set(state_error_tokens)
        for token, pair in venue_tob.items():
            state = self.states[token]
            if not state.structurally_valid():
                mismatches.add(token)
                continue
            best_bid, _, best_ask, _ = state.best()
            venue_bid = dec(pair[0]) if pair[0] is not None else None
            venue_ask = dec(pair[1]) if pair[1] is not None else None
            if venue_bid is not None and venue_bid != best_bid:
                mismatches.add(token)
            if venue_ask is not None and venue_ask != best_ask:
                mismatches.add(token)

        for token in sorted(touched):
            expected = self.expected_post_frame.get(
                (conn, frame_sequence, token)
            )
            if expected is not None:
                actual = self.states[token].snapshot()
                if actual != expected:
                    self.post_frame_crosscheck_failures += 1

        for token, pre_state in pre.items():
            if token not in touched:
                continue
            post_state = self._state_snapshot_for_transition(self.states[token])
            self._maybe_transition(
                token=token,
                pre=pre_state,
                post=post_state,
                capture_time=capture_time,
                connection_id=conn,
                frame_sequence=frame_sequence,
                mismatch=token in mismatches,
            )

        if mismatches:
            self.venue_tob_mismatch_tokens += len(mismatches)
            for token in mismatches:
                if token in self.states:
                    self.states[token].continuity_valid = False

        if state_error_tokens:
            self.state_errors += len(state_error_tokens)

    def replay(self) -> dict[str, Any]:
        events = raw_events(self.raw_rows) + group_rest_batches(self.rest_rows)
        events.sort(key=event_sort_key)

        last_time: datetime | None = None
        for event in events:
            event_time = event["time"]
            if last_time is not None and event_time < last_time:
                self.future_backfill += 1
            self.advance_coverage(event_time)
            if event["kind"] == "raw":
                self.apply_raw_frame(event)
            else:
                self.apply_rest_batch(event)
            last_time = event_time

        self.finish_coverage()

        coverage_by_market = {
            cid: self.coverage_seconds[cid] / WINDOW_SECONDS
            for cid in sorted(self.markets)
        }
        aggregate_coverage = (
            sum(self.coverage_seconds.values())
            / (len(self.markets) * WINDOW_SECONDS)
            if self.markets else 0.0
        )

        final_states = {
            token: self.states[token].snapshot()
            for token in sorted(self.states)
        }

        transition_ids = sorted(
            self.transitions,
            key=lambda x: (
                x["capture_time"],
                x["condition_id"],
                x["token"],
                x["frame_sequence"],
            ),
        )

        summary = {
            "distinct_markets": len(self.markets),
            "distinct_connections": len(self.connection_ids),
            "message_type_counts": dict(sorted(self.message_type_counts.items())),
            "initial_full_p1_snapshot_count": len(self.initial_p1_connect_tokens),
            "coverage_seconds_by_market": {
                cid: round(self.coverage_seconds[cid], 6)
                for cid in sorted(self.markets)
            },
            "coverage_fraction_by_market": {
                cid: round(coverage_by_market[cid], 12)
                for cid in sorted(coverage_by_market)
            },
            "aggregate_coverage_fraction": round(aggregate_coverage, 12),
            "transition_count_total": len(transition_ids),
            "transition_count_by_market": {
                cid: int(self.transition_counts[cid])
                for cid in sorted(self.markets)
            },
            "transition_distinct_markets": sum(
                1 for cid in self.markets if self.transition_counts[cid] > 0
            ),
            "markets_with_at_least_25_transitions": sum(
                1 for cid in self.markets if self.transition_counts[cid] >= 25
            ),
            "generation_or_connection_mixing": self.generation_or_connection_mixing,
            "future_backfill": self.future_backfill,
            "in_play_primary_transitions": self.in_play_primary_transitions,
            "state_errors": self.state_errors,
            "venue_tob_mismatch_tokens": self.venue_tob_mismatch_tokens,
            "rest_reconciliation_mismatches": self.rest_reconciliation_mismatches,
            "rest_flag_crosscheck_failures": self.rest_flag_crosscheck_failures,
            "rest_generation_crosscheck_failures":
                self.rest_generation_crosscheck_failures,
            "post_frame_crosscheck_failures": self.post_frame_crosscheck_failures,
            "reconstruction_integrity_errors": list(self.reconstruction_integrity_errors),
            "generation_history": self.generation_history,
            "transition_identities": transition_ids,
            "final_states": final_states,
        }
        summary["canonical_digest"] = sha256_bytes(canonical_json_bytes(summary))
        return summary


def reconciliation_interval_metrics(
    control_rows: list[dict[str, Any]],
    latest_game_start: datetime,
) -> dict[str, Any]:
    initialized = [
        parse_dt(row["capture_time"])
        for row in control_rows
        if row.get("kind") == "capture_initialized"
    ]
    reconciles = sorted(
        parse_dt(row["capture_time"])
        for row in control_rows
        if row.get("kind") == "rest_reconcile_complete"
    )

    errors: list[str] = []
    intervals: list[float] = []

    if len(initialized) != 1:
        errors.append(
            f"expected exactly one capture_initialized, got {len(initialized)}"
        )
    if not reconciles:
        errors.append("no rest_reconcile_complete events")

    if initialized and reconciles:
        intervals.append((reconciles[0] - initialized[0]).total_seconds())
        intervals.extend(
            (b - a).total_seconds()
            for a, b in zip(reconciles, reconciles[1:])
        )
        intervals.append((latest_game_start - reconciles[-1]).total_seconds())

    if any(x < 0 for x in intervals):
        errors.append("negative reconciliation interval")

    return {
        "count": len(reconciles),
        "max_interval_seconds": max(intervals, default=float("inf")),
        "intervals_seconds": [round(x, 6) for x in intervals],
        "errors": errors,
    }


def verify_lineage(
    raw_rows: list[dict[str, Any]],
    normalized_rows: list[dict[str, Any]],
) -> dict[str, int]:
    raw_keys: set[tuple[str, int, str]] = set()
    raw_sha_failures = 0
    duplicate_raw_keys = 0

    for row in raw_rows:
        raw = row.get("raw")
        digest = str(row.get("raw_frame_sha256") or "")
        if not isinstance(raw, str) or sha256_bytes(raw.encode("utf-8")) != digest:
            raw_sha_failures += 1
        key = (
            str(row.get("connection_id") or ""),
            int(row["frame_sequence"]),
            digest,
        )
        if key in raw_keys:
            duplicate_raw_keys += 1
        raw_keys.add(key)

    lineage_failures = 0
    for row in normalized_rows:
        key = (
            str(row.get("connection_id") or ""),
            int(row["frame_sequence"]),
            str(row.get("raw_frame_sha256") or ""),
        )
        if key not in raw_keys:
            lineage_failures += 1

    return {
        "raw_sha_failures": raw_sha_failures,
        "duplicate_raw_keys": duplicate_raw_keys,
        "normalized_lineage_failures": lineage_failures,
    }


def validate_contract_and_capture(repo: Path = REPO) -> dict[str, Any]:
    if sha256_file(repo / "research" / "h6_m1_validation_contract.json") != EXPECTED_CONTRACT_SHA256:
        raise RuntimeError("H6-M1 validation contract SHA mismatch")

    contract = json.loads(
        (repo / "research" / "h6_m1_validation_contract.json").read_text(
            encoding="utf-8"
        )
    )

    frozen = contract["frozen_capture"]
    for key in ("capture_start", "capture_complete"):
        info = frozen[key]
        path = repo / info["path"]
        if sha256_file(path) != info["sha256"]:
            raise RuntimeError(f"{key} SHA mismatch")

    capture_root = repo / frozen["capture_root"]
    for stream, filename in STREAM_FILENAMES.items():
        expected = frozen["streams"][stream]["sha256"]
        actual = sha256_file(capture_root / filename)
        if actual != expected:
            raise RuntimeError(f"{stream} SHA mismatch")

    scientific_paths = {
        "h6_m0_mechanism_data_design_sha256":
            repo / "research" / "h6_m0_mechanism_data_design.json",
        "h6_m0_independence_policy_sha256":
            repo / "research" / "h6_m0_independence_policy.json",
        "h6_m1_capture_contract_sha256":
            repo / "research" / "h6_m1_capture_contract.json",
        "h6_m1_collector_sha256":
            repo / "collectors" / "polymarket" / "h6_m1_collector.py",
    }
    for key, path in scientific_paths.items():
        if sha256_file(path) != contract["frozen_scientific_inputs"][key]:
            raise RuntimeError(f"frozen scientific input SHA mismatch: {key}")

    if sha256_file(repo / "data" / "research" / "h6" / "m1" / "registry.jsonl") != frozen["registry_sha256"]:
        raise RuntimeError("H6-M1 registry SHA mismatch")

    return contract


def verdict_from_metrics(
    *,
    contract: dict[str, Any],
    replay: dict[str, Any],
    lineage: dict[str, int],
    reconciliation: dict[str, Any],
    deterministic_replay_pass: bool,
) -> tuple[str, dict[str, bool]]:
    gates = contract["success_gates"]

    integrity = {
        "distinct_markets":
            replay["distinct_markets"] >= gates["distinct_markets_minimum"],
        "initial_full_p1_snapshots":
            replay["initial_full_p1_snapshot_count"] == replay["distinct_markets"],
        "raw_text_frame_capture":
            lineage["raw_sha_failures"] == 0
            and lineage["duplicate_raw_keys"] == 0
            and lineage["normalized_lineage_failures"] == 0,
        "deterministic_replay":
            deterministic_replay_pass,
        "generation_or_connection_mixing":
            replay["generation_or_connection_mixing"]
            == gates["generation_or_connection_mixing"],
        "future_backfill":
            replay["future_backfill"] == gates["future_backfill"],
        "in_play_primary_transitions":
            replay["in_play_primary_transitions"]
            == gates["in_play_primary_transitions"],
        "reconstruction_integrity_errors":
            len(replay["reconstruction_integrity_errors"]) == 0
            and replay["state_errors"] == 0
            and replay["rest_flag_crosscheck_failures"] == 0
            and replay["rest_generation_crosscheck_failures"] == 0
            and replay["post_frame_crosscheck_failures"] == 0,
        "reconciliation_interval":
            not reconciliation["errors"]
            and reconciliation["max_interval_seconds"]
            <= gates["reconciliation_interval_seconds_max"],
        "valid_two_sided_l1_wall_clock_coverage":
            replay["aggregate_coverage_fraction"]
            >= gates["valid_two_sided_l1_wall_clock_coverage_min_fraction"],
    }

    density = {
        "transition_count":
            replay["transition_count_total"]
            >= gates[
                "valid_price_stationary_nonzero_imbalance_transitions_minimum"
            ],
        "transition_distinct_markets":
            replay["transition_distinct_markets"]
            >= gates["transition_distinct_markets_minimum"],
        "market_breadth_25":
            replay["markets_with_at_least_25_transitions"]
            >= gates[
                "markets_with_at_least_25_valid_transitions_minimum"
            ],
    }

    all_gates = {**integrity, **density}

    if not all(integrity.values()):
        verdict = "FAIL_ENGINEERING_FEASIBILITY"
    elif not all(density.values()):
        verdict = "INCONCLUSIVE_ENGINEERING"
    else:
        verdict = "PASS_ENGINEERING_FEASIBILITY"

    return verdict, all_gates


def live_readout(repo: Path = REPO) -> dict[str, Any]:
    contract = validate_contract_and_capture(repo)

    if not committed_self_matches_head(repo=repo):
        raise RuntimeError(
            "refusing live H6-M1 validation: validator differs from committed HEAD"
        )

    result_path = repo / contract["readout_policy"]["live_result_path"]
    if result_path.exists():
        raise RuntimeError(
            f"refusing overwrite of existing H6-M1 validation result: {result_path}"
        )

    capture_root = repo / contract["frozen_capture"]["capture_root"]
    registry = load_jsonl(repo / "data" / "research" / "h6" / "m1" / "registry.jsonl")
    raw = load_jsonl(capture_root / "raw_frames.jsonl")
    normalized = load_jsonl(capture_root / "normalized.jsonl")
    rest = load_jsonl(capture_root / "rest_snapshots.jsonl")
    control = load_jsonl(capture_root / "control.jsonl")

    lineage = verify_lineage(raw, normalized)

    replay_a = Replay(registry, raw, rest, control).replay()
    replay_b = Replay(registry, raw, rest, control).replay()
    deterministic_pass = (
        replay_a["canonical_digest"] == replay_b["canonical_digest"]
        and canonical_json_bytes(replay_a) == canonical_json_bytes(replay_b)
    )

    latest_start = max(
        parse_dt(row["game_start_time_utc"]) for row in registry
    )
    reconciliation = reconciliation_interval_metrics(control, latest_start)

    verdict, gate_results = verdict_from_metrics(
        contract=contract,
        replay=replay_a,
        lineage=lineage,
        reconciliation=reconciliation,
        deterministic_replay_pass=deterministic_pass,
    )

    result = {
        "study": "H6",
        "milestone": "H6-M1",
        "status": verdict,
        "capture_id": contract["frozen_capture"]["capture_id"],
        "validation_contract_sha256": EXPECTED_CONTRACT_SHA256,
        "validator_git_head": git_head(repo),
        "validator_sha256": sha256_file(Path(__file__).resolve()),
        "deterministic_replay": {
            "status": "PASS" if deterministic_pass else "FAIL",
            "digest_run_1": replay_a["canonical_digest"],
            "digest_run_2": replay_b["canonical_digest"],
        },
        "lineage": lineage,
        "reconciliation": {
            "count": reconciliation["count"],
            "max_interval_seconds": reconciliation["max_interval_seconds"],
            "errors": reconciliation["errors"],
        },
        "engineering_metrics": {
            "distinct_markets": replay_a["distinct_markets"],
            "distinct_connections": replay_a["distinct_connections"],
            "initial_full_p1_snapshot_count":
                replay_a["initial_full_p1_snapshot_count"],
            "aggregate_coverage_fraction":
                replay_a["aggregate_coverage_fraction"],
            "coverage_fraction_by_market":
                replay_a["coverage_fraction_by_market"],
            "transition_count_total":
                replay_a["transition_count_total"],
            "transition_count_by_market":
                replay_a["transition_count_by_market"],
            "transition_distinct_markets":
                replay_a["transition_distinct_markets"],
            "markets_with_at_least_25_transitions":
                replay_a["markets_with_at_least_25_transitions"],
            "generation_or_connection_mixing":
                replay_a["generation_or_connection_mixing"],
            "future_backfill":
                replay_a["future_backfill"],
            "in_play_primary_transitions":
                replay_a["in_play_primary_transitions"],
            "state_errors":
                replay_a["state_errors"],
            "venue_tob_mismatch_tokens":
                replay_a["venue_tob_mismatch_tokens"],
            "rest_reconciliation_mismatches":
                replay_a["rest_reconciliation_mismatches"],
            "rest_flag_crosscheck_failures":
                replay_a["rest_flag_crosscheck_failures"],
            "rest_generation_crosscheck_failures":
                replay_a["rest_generation_crosscheck_failures"],
            "post_frame_crosscheck_failures":
                replay_a["post_frame_crosscheck_failures"],
            "reconstruction_integrity_errors":
                replay_a["reconstruction_integrity_errors"],
            "message_type_counts":
                replay_a["message_type_counts"],
        },
        "gate_results": gate_results,
        "analysis_boundary": {
            "future_midpoint_response_calculated": False,
            "directional_response_calculated": False,
            "directional_hit_rate_calculated": False,
            "correlation_calculated": False,
            "regression_calculated": False,
            "markout_calculated": False,
            "edge_calculated": False,
            "pnl_calculated": False,
            "winner_used": False,
            "settlement_used": False,
            "f19_trade_content_read": False,
            "f20_read": False,
            "h5_response_read": False,
            "external_odds_used": False,
        },
    }

    result_path.parent.mkdir(parents=True, exist_ok=True)
    with result_path.open("x", encoding="utf-8") as fh:
        json.dump(result, fh, indent=2, sort_keys=True)
        fh.write("\n")

    return result


def validate_config(repo: Path = REPO) -> None:
    contract = validate_contract_and_capture(repo)
    print("H6-M1 VALIDATOR CONFIG: PASS")
    print("validation contract SHA:", EXPECTED_CONTRACT_SHA256)
    print("capture id:", contract["frozen_capture"]["capture_id"])
    print("capture hashes verified: YES")
    print("real capture replayed: NO")
    print("transition count calculated: NO")
    print("coverage calculated: NO")
    print("future midpoint response calculated: NO")
    print("PnL calculated: NO")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--validate-config", action="store_true")
    parser.add_argument("--run-live", action="store_true")
    args = parser.parse_args()

    if args.validate_config:
        validate_config()
        return

    if args.run_live:
        result = live_readout()
        print("========================================")
        print("H6-M1 ENGINEERING VALIDATION")
        print("========================================")
        print("status:", result["status"])
        print(
            "deterministic replay:",
            result["deterministic_replay"]["status"],
        )
        metrics = result["engineering_metrics"]
        print(
            "aggregate valid p1 L1 coverage:",
            metrics["aggregate_coverage_fraction"],
        )
        print(
            "valid price-stationary nonzero imbalance transitions:",
            metrics["transition_count_total"],
        )
        print(
            "transition distinct markets:",
            metrics["transition_distinct_markets"],
        )
        print(
            "markets with >=25 transitions:",
            metrics["markets_with_at_least_25_transitions"],
        )
        print("future midpoint response calculated: NO")
        print("PnL calculated: NO")
        print(
            "result:",
            json.loads(CONTRACT_PATH.read_text())["readout_policy"][
                "live_result_path"
            ],
        )
        return

    raise SystemExit("choose --validate-config or --run-live")


if __name__ == "__main__":
    main()
