from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import shutil
import subprocess
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import httpx
import websockets


REPO = Path(__file__).resolve().parents[2]
RESEARCH = REPO / "research"

M0 = RESEARCH / "h6_m0_mechanism_data_design.json"
INDEPENDENCE = RESEARCH / "h6_m0_independence_policy.json"
REGISTRATION_CONTRACT = RESEARCH / "h6_m1_registration_contract.json"
REGISTRAR = RESEARCH / "h6_m1_register.py"
CAPTURE_CONTRACT = RESEARCH / "h6_m1_capture_contract.json"

REGISTRY = REPO / "data" / "research" / "h6" / "m1" / "registry.jsonl"
REGISTRY_RECEIPT = RESEARCH / "h6_m1_registry_freeze.json"
CAPTURE_ROOT = REPO / "data" / "research" / "h6" / "m1" / "capture"

CLOB = "https://clob.polymarket.com"
WS_URI = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

EXPECTED_PARENT_HEAD = "c451376560fa77d12a5a1413a9fee070862cd344"
EXPECTED_M0_SHA = "bcbcf5e5ffc72517df5c6162771da62fc11113c2b514b297c9848172444e9213"
EXPECTED_INDEPENDENCE_SHA = "13b58c64e5c63275c533be12272eec2a96a7a71a5da2b79eb3bc52f995637c32"
EXPECTED_REGISTRATION_CONTRACT_SHA = "34e43b9b69e9988ea92728a7d7c08ae2063505171b54d3d8b19fa64557eb9dd8"
EXPECTED_REGISTRAR_SHA = "bc4c7d799e45f9ab0e4326058cb6ca7b214a85abf5ee0ffaff69631d45bcf7b5"
EXPECTED_CAPTURE_CONTRACT_SHA = "97380ad5f75b999cc9f0253dabcb90542b43e1829e42fc3189ca38f0a9342a3b"

MARKET_COUNT = 5
TOKEN_COUNT = 10
PRIMARY_WINDOW_SECONDS = 1800
PREARM_SECONDS = 120
RECONCILE_SECONDS = 30
PING_SECONDS = 10
SILENCE_SECONDS = 30
MIN_FREE_DISK_BYTES = 4 * 1024**3

UA = {"User-Agent": "quant-lab-h6-m1-capture/1.0"}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        return None
    return dt.astimezone(timezone.utc)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git_head() -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=REPO, text=True
    ).strip()


def git_is_ancestor(ancestor: str, descendant: str = "HEAD") -> bool:
    result = subprocess.run(
        ["git", "merge-base", "--is-ancestor", ancestor, descendant],
        cwd=REPO,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return result.returncode == 0


def committed_self_matches_head() -> bool:
    path = Path(__file__).resolve()
    try:
        rel = path.relative_to(REPO).as_posix()
        committed = subprocess.check_output(
            ["git", "show", f"HEAD:{rel}"], cwd=REPO
        )
    except (ValueError, subprocess.CalledProcessError):
        return False
    return sha256_bytes(committed) == sha256_file(path)


def json_clean(value: Any):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): json_clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_clean(v) for v in value]
    return value


def append_jsonl(fh, record: dict[str, Any]) -> None:
    fh.write(
        json.dumps(
            json_clean(record),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        + "\n"
    )


def decimal_value(value: Any) -> Decimal | None:
    try:
        d = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not d.is_finite():
        return None
    return d


def boundary_flags():
    return {
        "l1_imbalance_calculated": False,
        "imbalance_change_calculated": False,
        "primary_transition_count_calculated": False,
        "future_midpoint_change_calculated": False,
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
    }


class BookState:
    def __init__(self, token: str):
        self.token = token
        self.connection_id = ""
        self.generation = 0
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}
        self.tick_size: Decimal | None = None
        self.initialized = False
        self.valid = False
        self.last_venue_timestamp_ms: int | None = None

    @staticmethod
    def _levels(raw: Any) -> dict[Decimal, Decimal]:
        if not isinstance(raw, list):
            raise ValueError("book side is not a list")
        out: dict[Decimal, Decimal] = {}
        for level in raw:
            if not isinstance(level, dict):
                raise ValueError("book level is not an object")
            price = decimal_value(level.get("price"))
            size = decimal_value(level.get("size"))
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

    def is_stale_venue_timestamp(self, value: Any) -> bool:
        ts = self.venue_timestamp(value)
        return bool(
            ts is not None
            and self.last_venue_timestamp_ms is not None
            and ts < self.last_venue_timestamp_ms
        )

    def observe_venue_timestamp(self, value: Any) -> None:
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
        bids = self._levels(book.get("bids") or [])
        asks = self._levels(book.get("asks") or [])
        if increment_generation:
            self.generation += 1
        self.connection_id = connection_id
        self.bids = bids
        self.asks = asks
        tick = decimal_value(book.get("tick_size"))
        if tick is not None:
            if tick <= 0:
                raise ValueError("invalid tick size")
            self.tick_size = tick
        self.observe_venue_timestamp(book.get("timestamp"))
        self.initialized = True
        self.valid = True

    def apply_level(self, *, side: Any, price: Any, size: Any) -> None:
        if not self.initialized or not self.valid:
            raise ValueError("cannot apply level to invalid state")
        p = decimal_value(price)
        z = decimal_value(size)
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
        if z == 0:
            levels.pop(p, None)
        else:
            levels[p] = z

    def apply_tick(self, new_tick: Any) -> None:
        tick = decimal_value(new_tick)
        if tick is None or tick <= 0:
            raise ValueError("invalid new tick size")
        self.tick_size = tick

    def best(self):
        best_bid = max(self.bids, default=None)
        best_ask = min(self.asks, default=None)
        bid_size = self.bids.get(best_bid) if best_bid is not None else None
        ask_size = self.asks.get(best_ask) if best_ask is not None else None
        return best_bid, bid_size, best_ask, ask_size

    def structurally_valid(self) -> bool:
        if not self.initialized or not self.valid:
            return False
        best_bid, bid_size, best_ask, ask_size = self.best()
        return bool(
            best_bid is not None
            and best_ask is not None
            and bid_size is not None
            and ask_size is not None
            and bid_size > 0
            and ask_size > 0
            and best_bid < best_ask
        )

    def canonical_levels(self):
        return {
            "bids": [
                {"price": str(x), "size": str(self.bids[x])}
                for x in sorted(self.bids, reverse=True)
            ],
            "asks": [
                {"price": str(x), "size": str(self.asks[x])}
                for x in sorted(self.asks)
            ],
        }

    def snapshot(self):
        best_bid, bid_size, best_ask, ask_size = self.best()
        return {
            "token": self.token,
            "connection_id": self.connection_id,
            "book_generation": self.generation,
            "initialized": self.initialized,
            "valid": self.valid,
            "structurally_valid": self.structurally_valid(),
            "tick_size": self.tick_size,
            "last_venue_timestamp_ms": self.last_venue_timestamp_ms,
            "best_bid": best_bid,
            "best_bid_size": bid_size,
            "best_ask": best_ask,
            "best_ask_size": ask_size,
            **self.canonical_levels(),
        }

    def same_full_book(self, book: dict[str, Any]) -> bool:
        try:
            bids = self._levels(book.get("bids") or [])
            asks = self._levels(book.get("asks") or [])
        except ValueError:
            return False
        tick = decimal_value(book.get("tick_size"))
        tick_equal = tick is None or self.tick_size is None or tick == self.tick_size
        return self.bids == bids and self.asks == asks and tick_equal


def validate_frozen_inputs(*, require_committed_self: bool):
    identities = {
        M0: EXPECTED_M0_SHA,
        INDEPENDENCE: EXPECTED_INDEPENDENCE_SHA,
        REGISTRATION_CONTRACT: EXPECTED_REGISTRATION_CONTRACT_SHA,
        REGISTRAR: EXPECTED_REGISTRAR_SHA,
        CAPTURE_CONTRACT: EXPECTED_CAPTURE_CONTRACT_SHA,
    }
    for path, expected in identities.items():
        if not path.is_file():
            raise RuntimeError(f"missing frozen input: {path}")
        actual = sha256_file(path)
        if actual != expected:
            raise RuntimeError(
                f"frozen input SHA mismatch: {path}\n"
                f"expected={expected}\nactual={actual}"
            )

    if not git_is_ancestor(EXPECTED_PARENT_HEAD):
        raise RuntimeError("frozen H6 registrar commit is not an ancestor of HEAD")

    c = json.loads(CAPTURE_CONTRACT.read_text(encoding="utf-8"))
    cap = c["capture"]
    if cap["primary_window"]["start_seconds_before_game_start"] != PRIMARY_WINDOW_SECONDS:
        raise RuntimeError("primary window mismatch")
    if cap["prearm_seconds_before_earliest_primary_window"] != PREARM_SECONDS:
        raise RuntimeError("prearm mismatch")
    if cap["rest_reconciliation_seconds"] != RECONCILE_SECONDS:
        raise RuntimeError("reconcile interval mismatch")
    if cap["ping_seconds"] != PING_SECONDS or cap["silence_seconds"] != SILENCE_SECONDS:
        raise RuntimeError("websocket heartbeat mismatch")
    if cap["minimum_free_disk_bytes"] != MIN_FREE_DISK_BYTES:
        raise RuntimeError("disk gate mismatch")

    if require_committed_self and not committed_self_matches_head():
        raise RuntimeError(
            "refusing live H6-M1 capture: collector differs from committed HEAD"
        )
    return c


def validate_registry_rows(rows: list[dict[str, Any]]) -> None:
    if len(rows) != MARKET_COUNT:
        raise RuntimeError(f"H6-M1 registry must contain exactly {MARKET_COUNT} rows")
    required = {
        "study", "milestone", "registration_index", "registered_at_utc",
        "gamma_market_id", "condition_id", "game_start_time_utc",
        "p1_token_id", "p0_token_id", "p1_outcome", "p0_outcome",
    }
    conditions: set[str] = set()
    tokens: set[str] = set()

    for index, row in enumerate(rows, start=1):
        missing = required - set(row)
        if missing:
            raise RuntimeError(f"registry row {index} missing {sorted(missing)}")
        if row["study"] != "H6" or row["milestone"] != "H6-M1":
            raise RuntimeError("registry study/milestone mismatch")
        if row["registration_index"] != index:
            raise RuntimeError("registry index/order mismatch")
        if parse_dt(row["registered_at_utc"]) is None:
            raise RuntimeError("invalid registration timestamp")
        start = parse_dt(row["game_start_time_utc"])
        if start is None:
            raise RuntimeError("invalid game start")

        cid = str(row["condition_id"]).lower()
        p1 = str(row["p1_token_id"])
        p0 = str(row["p0_token_id"])
        if not cid or cid in conditions:
            raise RuntimeError("duplicate/empty condition id")
        if p1 == p0 or p1 in tokens or p0 in tokens:
            raise RuntimeError("duplicate token id")
        conditions.add(cid)
        tokens.update((p1, p0))

    if len(tokens) != TOKEN_COUNT:
        raise RuntimeError("H6-M1 registry token count mismatch")


def load_registry_bundle():
    validate_frozen_inputs(require_committed_self=True)
    if not REGISTRY.is_file() or not REGISTRY_RECEIPT.is_file():
        raise RuntimeError("H6-M1 registry/receipt not frozen")

    rows = [
        json.loads(line)
        for line in REGISTRY.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    validate_registry_rows(rows)

    receipt = json.loads(REGISTRY_RECEIPT.read_text(encoding="utf-8"))
    if receipt.get("status") != "REGISTRY_FROZEN":
        raise RuntimeError("registry receipt not frozen")
    if receipt.get("registration_code", {}).get("sha256") != EXPECTED_REGISTRAR_SHA:
        raise RuntimeError("registry receipt registrar SHA mismatch")
    if (
        receipt.get("registration_contract", {}).get("sha256")
        != EXPECTED_REGISTRATION_CONTRACT_SHA
    ):
        raise RuntimeError("registry receipt registration-contract SHA mismatch")

    registry_sha = sha256_file(REGISTRY)
    meta = receipt.get("registry", {})
    if meta.get("sha256") != registry_sha:
        raise RuntimeError("registry SHA mismatch against receipt")
    if int(meta.get("row_count", -1)) != MARKET_COUNT:
        raise RuntimeError("registry receipt row count mismatch")
    if int(meta.get("distinct_token_id_count", -1)) != TOKEN_COUNT:
        raise RuntimeError("registry receipt token count mismatch")

    boundary = receipt.get("analysis_boundary", {})
    for key in (
        "book_price_read", "book_depth_read", "trade_content_read",
        "h6_transition_calculated", "h6_response_calculated",
        "markout_calculated", "edge_calculated", "pnl_calculated",
        "winner_used", "settlement_used", "f19_trade_content_read",
        "f20_read", "h5_response_read",
    ):
        if boundary.get(key) is not False:
            raise RuntimeError(f"registry analysis boundary invalid: {key}")

    return {
        "rows": rows,
        "receipt": receipt,
        "registry_sha256": registry_sha,
        "receipt_sha256": sha256_file(REGISTRY_RECEIPT),
    }


class CaptureWriter:
    def __init__(self, capture_id: str, bundle: dict[str, Any]):
        if not capture_id or any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-" for ch in capture_id):
            raise ValueError("invalid capture_id")

        self.capture_id = capture_id
        self.capture_dir = CAPTURE_ROOT / capture_id
        self.capture_dir.mkdir(parents=True, exist_ok=False)

        self.paths = {
            "raw_frames": self.capture_dir / "raw_frames.jsonl",
            "normalized": self.capture_dir / "normalized.jsonl",
            "rest_snapshots": self.capture_dir / "rest_snapshots.jsonl",
            "control": self.capture_dir / "control.jsonl",
            "boundary_states": self.capture_dir / "boundary_states.jsonl",
        }
        self.handles = {
            name: path.open("x", encoding="utf-8", buffering=1)
            for name, path in self.paths.items()
        }
        self.counts = {name: 0 for name in self.paths}

        starts = [
            parse_dt(row["game_start_time_utc"])
            for row in bundle["rows"]
        ]
        assert all(x is not None for x in starts)
        starts = [x for x in starts if x is not None]

        start_manifest = {
            "study": "H6",
            "milestone": "H6-M1",
            "status": "CAPTURE_ARMED",
            "capture_id": capture_id,
            "created_at_utc": utcnow(),
            "git_head": git_head(),
            "collector_path": "collectors/polymarket/h6_m1_collector.py",
            "collector_sha256": sha256_file(Path(__file__).resolve()),
            "capture_contract_sha256": EXPECTED_CAPTURE_CONTRACT_SHA,
            "registration_contract_sha256": EXPECTED_REGISTRATION_CONTRACT_SHA,
            "registrar_sha256": EXPECTED_REGISTRAR_SHA,
            "registry_sha256": bundle["registry_sha256"],
            "registry_receipt_sha256": bundle["receipt_sha256"],
            "registered_market_count": len(bundle["rows"]),
            "registered_token_count": sum(2 for _ in bundle["rows"]),
            "primary_window_seconds": PRIMARY_WINDOW_SECONDS,
            "prearm_seconds": PREARM_SECONDS,
            "reconcile_seconds": RECONCILE_SECONDS,
            "ping_seconds": PING_SECONDS,
            "silence_seconds": SILENCE_SECONDS,
            "minimum_free_disk_bytes": MIN_FREE_DISK_BYTES,
            "earliest_primary_window_start": min(starts) - timedelta(seconds=PRIMARY_WINDOW_SECONDS),
            "latest_game_start": max(starts),
            "analysis_boundary": boundary_flags(),
        }
        self.write_exclusive_json(
            self.capture_dir / "capture_start.json",
            start_manifest,
        )

    @staticmethod
    def write_exclusive_json(path: Path, record: dict[str, Any]) -> None:
        with path.open("x", encoding="utf-8") as fh:
            json.dump(
                json_clean(record),
                fh,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            )
            fh.write("\n")

    def write(self, stream: str, record: dict[str, Any]) -> None:
        append_jsonl(self.handles[stream], record)
        self.counts[stream] += 1

    def flush(self) -> None:
        for fh in self.handles.values():
            fh.flush()

    def finalize(self, *, stop_reason: str, extra_counts: dict[str, Any] | None = None):
        for fh in self.handles.values():
            fh.flush()
            fh.close()
        hashes = {
            name: {
                "path": str(path.relative_to(REPO)),
                "sha256": sha256_file(path),
                "bytes": path.stat().st_size,
            }
            for name, path in self.paths.items()
        }
        final = {
            "study": "H6",
            "milestone": "H6-M1",
            "status": "CAPTURE_COMPLETE",
            "capture_id": self.capture_id,
            "completed_at_utc": utcnow(),
            "stop_reason": stop_reason,
            "counts": {**self.counts, **(extra_counts or {})},
            "stream_hashes": hashes,
            "analysis_boundary": boundary_flags(),
        }
        self.write_exclusive_json(
            self.capture_dir / "capture_complete.json",
            final,
        )
        return final


class H6Capture:
    def __init__(self, *, bundle: dict[str, Any], writer: CaptureWriter):
        self.bundle = bundle
        self.writer = writer
        self.rows = bundle["rows"]
        self.conn_id = ""
        self.frame_sequence = 0
        self.ingest_sequence = 0
        self.states: dict[str, BookState] = {}
        self.token_meta: dict[str, dict[str, Any]] = {}
        self.window_open_recorded: set[str] = set()
        self.window_close_recorded: set[str] = set()
        self.last_message_monotonic = 0.0

        for row in self.rows:
            start = parse_dt(row["game_start_time_utc"])
            assert start is not None
            cid = str(row["condition_id"]).lower()
            for role in ("p1", "p0"):
                token = str(row[f"{role}_token_id"])
                self.states[token] = BookState(token=token)
                self.token_meta[token] = {
                    "condition_id": cid,
                    "role": role,
                    "game_start": start,
                    "window_start": start - timedelta(seconds=PRIMARY_WINDOW_SECONDS),
                }

    def all_tokens(self):
        return sorted(self.states)

    def active_tokens(self, now: datetime | None = None):
        now = now or utcnow()
        return sorted(
            token for token in self.states
            if self.before_game_start(token, now)
        )

    def latest_start(self):
        return max(meta["game_start"] for meta in self.token_meta.values())

    def earliest_window_start(self):
        return min(meta["window_start"] for meta in self.token_meta.values())

    def prearm_time(self):
        return self.earliest_window_start() - timedelta(seconds=PREARM_SECONDS)

    def in_primary_window(self, token: str, capture_time: datetime) -> bool:
        meta = self.token_meta[token]
        return meta["window_start"] <= capture_time < meta["game_start"]

    def before_game_start(self, token: str, capture_time: datetime) -> bool:
        return capture_time < self.token_meta[token]["game_start"]

    def control(self, kind: str, **payload) -> None:
        self.writer.write(
            "control",
            {
                "capture_id": self.writer.capture_id,
                "capture_time": utcnow(),
                "connection_id": self.conn_id or None,
                "kind": kind,
                **payload,
            },
        )

    def boundary_state(self, token: str, *, boundary: str, scheduled_at: datetime) -> None:
        state = self.states[token]
        meta = self.token_meta[token]
        self.writer.write(
            "boundary_states",
            {
                "capture_id": self.writer.capture_id,
                "recorded_at_utc": utcnow(),
                "scheduled_boundary_utc": scheduled_at,
                "boundary": boundary,
                "condition_id": meta["condition_id"],
                "token": token,
                "role": meta["role"],
                "state": state.snapshot(),
            },
        )

    def record_due_boundaries(self, now: datetime) -> None:
        by_condition: dict[str, list[str]] = {}
        for token, meta in self.token_meta.items():
            by_condition.setdefault(meta["condition_id"], []).append(token)

        for cid, tokens in by_condition.items():
            sample = self.token_meta[tokens[0]]
            window_start = sample["window_start"]
            game_start = sample["game_start"]

            if cid not in self.window_open_recorded and now >= window_start:
                self.window_open_recorded.add(cid)
                self.control(
                    "primary_window_open",
                    condition_id=cid,
                    scheduled_at_utc=window_start,
                )
                for token in sorted(tokens):
                    self.boundary_state(
                        token,
                        boundary="primary_window_open",
                        scheduled_at=window_start,
                    )

            if cid not in self.window_close_recorded and now >= game_start:
                self.window_close_recorded.add(cid)
                self.control(
                    "primary_window_close",
                    condition_id=cid,
                    scheduled_at_utc=game_start,
                )
                for token in sorted(tokens):
                    self.boundary_state(
                        token,
                        boundary="game_start",
                        scheduled_at=game_start,
                    )

    def write_raw(self, *, raw: str, capture_time: datetime) -> str:
        data = raw.encode("utf-8")
        digest = sha256_bytes(data)
        self.writer.write(
            "raw_frames",
            {
                "capture_id": self.writer.capture_id,
                "capture_time": capture_time,
                "connection_id": self.conn_id,
                "frame_sequence": self.frame_sequence,
                "raw_frame_sha256": digest,
                "raw": raw,
            },
        )
        return digest

    def normalized(
        self,
        *,
        kind: str,
        token: str | None,
        payload: dict[str, Any],
        capture_time: datetime,
        raw_sha: str,
        change_index: int,
    ) -> None:
        meta = self.token_meta.get(token) if token else None
        self.writer.write(
            "normalized",
            {
                "capture_id": self.writer.capture_id,
                "kind": kind,
                "token": token,
                "condition_id": meta["condition_id"] if meta else None,
                "role": meta["role"] if meta else None,
                "capture_time": capture_time,
                "in_primary_window": (
                    self.in_primary_window(token, capture_time)
                    if meta else False
                ),
                "connection_id": self.conn_id,
                "frame_sequence": self.frame_sequence,
                "raw_frame_sha256": raw_sha,
                "ingest_sequence": self.ingest_sequence,
                "change_index": change_index,
                "book_generation": (
                    self.states[token].generation if token in self.states else None
                ),
                "venue_timestamp": payload.get("timestamp"),
                "payload": payload,
            },
        )

    def _apply_full_ws_book(self, token: str, payload: dict[str, Any]) -> None:
        state = self.states[token]
        # A WS book is an authoritative in-stream full snapshot, not a new
        # generation. If no state exists yet, initialize within current
        # generation rather than inventing a transition across a resync.
        state.replace_full(
            payload,
            connection_id=self.conn_id,
            increment_generation=(not state.valid or not state.initialized),
        )

    def handle_raw(
        self,
        raw: str,
        *,
        capture_time: datetime | None = None,
    ) -> set[str]:
        capture_time = capture_time or utcnow()
        self.frame_sequence += 1
        raw_sha = self.write_raw(raw=raw, capture_time=capture_time)
        if raw == "PONG":
            return set()

        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            self.control("unparseable_frame", frame_sequence=self.frame_sequence)
            return set()

        messages = payload if isinstance(payload, list) else [payload]
        if not all(isinstance(x, dict) for x in messages):
            self.control("invalid_frame_shape", frame_sequence=self.frame_sequence)
            return set()

        message_sequences: list[int] = []
        for _ in messages:
            self.ingest_sequence += 1
            message_sequences.append(self.ingest_sequence)

        touched: set[str] = set()
        venue_tob: dict[str, tuple[Any, Any]] = {}
        resync_required: set[str] = set()
        state_applied: dict[tuple[int, int], bool] = {}

        # Frame atomicity: all mutations are completed before post-frame states.
        for mi, message in enumerate(messages):
            event_type = message.get("event_type")
            token = str(message.get("asset_id")) if message.get("asset_id") else None
            msg_ts = message.get("timestamp")

            if event_type == "book" and token in self.states:
                state = self.states[token]
                applied = False
                if self.before_game_start(token, capture_time):
                    if state.is_stale_venue_timestamp(msg_ts):
                        self.control(
                            "stale_ws_state_event_ignored",
                            token=token,
                            event_type="book",
                            frame_sequence=self.frame_sequence,
                        )
                    else:
                        try:
                            self._apply_full_ws_book(token, message)
                            state.observe_venue_timestamp(msg_ts)
                            touched.add(token)
                            applied = True
                        except ValueError:
                            state.valid = False
                            resync_required.add(token)
                            self.control(
                                "ws_book_state_error",
                                token=token,
                                frame_sequence=self.frame_sequence,
                            )
                state_applied[(mi, 0)] = applied

            elif event_type == "price_change":
                changes = message.get("price_changes") or message.get("changes") or []
                if not isinstance(changes, list):
                    self.control("invalid_price_change_shape")
                    continue
                for ci, change in enumerate(changes):
                    if not isinstance(change, dict):
                        continue
                    tok = str(change.get("asset_id")) if change.get("asset_id") else token
                    applied = False
                    if tok not in self.states or not self.before_game_start(tok, capture_time):
                        state_applied[(mi, ci)] = False
                        continue
                    state = self.states[tok]
                    if state.is_stale_venue_timestamp(msg_ts):
                        self.control(
                            "stale_ws_state_event_ignored",
                            token=tok,
                            event_type="price_change",
                            frame_sequence=self.frame_sequence,
                        )
                    else:
                        try:
                            state.apply_level(
                                side=change.get("side"),
                                price=change.get("price"),
                                size=change.get("size"),
                            )
                            state.observe_venue_timestamp(msg_ts)
                            touched.add(tok)
                            applied = True
                        except ValueError:
                            state.valid = False
                            resync_required.add(tok)
                            self.control(
                                "price_change_state_error",
                                token=tok,
                                frame_sequence=self.frame_sequence,
                            )
                    if applied and (
                        change.get("best_bid") is not None
                        or change.get("best_ask") is not None
                    ):
                        venue_tob[tok] = (
                            change.get("best_bid"),
                            change.get("best_ask"),
                        )
                    state_applied[(mi, ci)] = applied

            elif event_type == "tick_size_change" and token in self.states:
                state = self.states[token]
                applied = False
                if self.before_game_start(token, capture_time):
                    if state.is_stale_venue_timestamp(msg_ts):
                        self.control(
                            "stale_ws_state_event_ignored",
                            token=token,
                            event_type="tick_size_change",
                            frame_sequence=self.frame_sequence,
                        )
                    else:
                        try:
                            state.apply_tick(message.get("new_tick_size"))
                            state.observe_venue_timestamp(msg_ts)
                            touched.add(token)
                            applied = True
                        except ValueError:
                            state.valid = False
                            resync_required.add(token)
                            self.control(
                                "tick_size_state_error",
                                token=token,
                                frame_sequence=self.frame_sequence,
                            )
                state_applied[(mi, 0)] = applied

            elif event_type == "best_bid_ask" and token in self.states:
                if self.before_game_start(token, capture_time):
                    state = self.states[token]
                    if state.is_stale_venue_timestamp(msg_ts):
                        self.control(
                            "stale_ws_diagnostic_event_ignored",
                            token=token,
                            event_type="best_bid_ask",
                            frame_sequence=self.frame_sequence,
                        )
                    else:
                        state.observe_venue_timestamp(msg_ts)
                        venue_tob[token] = (
                            message.get("best_bid"),
                            message.get("best_ask"),
                        )
                state_applied[(mi, 0)] = False

        # Normalization occurs after all state mutations but uses the one frame time.
        for mi, message in enumerate(messages):
            event_type = str(message.get("event_type") or "unknown")
            token = str(message.get("asset_id")) if message.get("asset_id") else None
            seq = message_sequences[mi]
            old_seq = self.ingest_sequence
            self.ingest_sequence = seq
            try:
                if event_type == "price_change":
                    changes = message.get("price_changes") or message.get("changes") or []
                    if isinstance(changes, list):
                        for ci, change in enumerate(changes):
                            if not isinstance(change, dict):
                                continue
                            tok = str(change.get("asset_id")) if change.get("asset_id") else token
                            if tok in self.states:
                                self.normalized(
                                    kind="price_change",
                                    token=tok,
                                    payload={
                                        **change,
                                        "timestamp": message.get("timestamp"),
                                        "market": message.get("market"),
                                        "state_applied": state_applied.get((mi, ci), False),
                                    },
                                    capture_time=capture_time,
                                    raw_sha=raw_sha,
                                    change_index=ci,
                                )
                elif token in self.states:
                    self.normalized(
                        kind=event_type,
                        token=token,
                        payload={**message, "state_applied": state_applied.get((mi, 0), False)},
                        capture_time=capture_time,
                        raw_sha=raw_sha,
                        change_index=0,
                    )
                else:
                    self.normalized(
                        kind=event_type,
                        token=None,
                        payload={**message, "state_applied": False},
                        capture_time=capture_time,
                        raw_sha=raw_sha,
                        change_index=0,
                    )
            finally:
                self.ingest_sequence = old_seq

        mismatches: set[str] = set(resync_required)
        for token, pair in venue_tob.items():
            state = self.states[token]
            if not state.structurally_valid():
                mismatches.add(token)
                continue
            best_bid, _, best_ask, _ = state.best()
            venue_bid = decimal_value(pair[0]) if pair[0] is not None else None
            venue_ask = decimal_value(pair[1]) if pair[1] is not None else None
            if venue_bid is not None and best_bid != venue_bid:
                mismatches.add(token)
            if venue_ask is not None and best_ask != venue_ask:
                mismatches.add(token)

        for token in sorted(touched):
            self.control(
                "post_frame_state",
                token=token,
                frame_sequence=self.frame_sequence,
                in_primary_window=self.in_primary_window(token, capture_time),
                state=self.states[token].snapshot(),
            )
        return mismatches

    def process_text_frame(
        self,
        raw: str,
        *,
        capture_time: datetime | None = None,
    ) -> set[str]:
        capture_time = capture_time or utcnow()

        # A boundary snapshot must represent the last reconstructed
        # state known before a frame received at/after that cutoff.
        self.record_due_boundaries(capture_time)

        return self.handle_raw(
            raw,
            capture_time=capture_time,
        )

    async def rest_books(
        self,
        http: httpx.AsyncClient,
        tokens: list[str],
    ) -> list[dict[str, Any]]:
        if not tokens:
            return []
        response = await http.post(
            f"{CLOB}/books",
            json=[{"token_id": token} for token in tokens],
        )
        if response.status_code != 200:
            raise RuntimeError(f"CLOB /books failed: HTTP {response.status_code}")
        payload = response.json()
        if not isinstance(payload, list):
            raise RuntimeError("CLOB /books response is not a list")
        return [x for x in payload if isinstance(x, dict)]

    async def rest_resync(
        self,
        http: httpx.AsyncClient,
        *,
        tokens: list[str],
        reason: str,
        require_all: bool,
    ) -> None:
        requested_at = utcnow()
        books = await self.rest_books(http, tokens)
        received_at = utcnow()
        by_token = {
            str(book.get("asset_id")): book
            for book in books
            if book.get("asset_id")
        }
        missing = sorted(set(tokens) - set(by_token))
        if require_all and missing:
            raise RuntimeError(f"REST snapshot missing {len(missing)} tokens")

        for token in tokens:
            book = by_token.get(token)
            if book is None:
                self.states[token].valid = False
                continue
            self.states[token].replace_full(
                book,
                connection_id=self.conn_id,
                increment_generation=True,
            )
            self.writer.write(
                "rest_snapshots",
                {
                    "capture_id": self.writer.capture_id,
                    "request_started_at_utc": requested_at,
                    "response_received_at_utc": received_at,
                    "reason": reason,
                    "matched_local_state_before": None,
                    "connection_id": self.conn_id,
                    "token": token,
                    "condition_id": self.token_meta[token]["condition_id"],
                    "role": self.token_meta[token]["role"],
                    "book_generation": self.states[token].generation,
                    "book": book,
                },
            )
        self.control(
            "rest_resync_complete",
            reason=reason,
            requested_tokens=len(tokens),
            returned_books=len(books),
            missing_tokens=len(missing),
            missing_token_ids=missing,
        )

    async def reconcile(
        self,
        http: httpx.AsyncClient,
        *,
        tokens: list[str],
    ) -> None:
        if not tokens:
            return
        requested_at = utcnow()
        books = await self.rest_books(http, tokens)
        received_at = utcnow()
        by_token = {
            str(book.get("asset_id")): book
            for book in books
            if book.get("asset_id")
        }
        mismatched: list[str] = []
        missing: list[str] = []

        for token in tokens:
            book = by_token.get(token)
            if book is None:
                self.states[token].valid = False
                missing.append(token)
                continue

            matched = self.states[token].valid and self.states[token].same_full_book(book)
            if not matched:
                mismatched.append(token)
                self.states[token].replace_full(
                    book,
                    connection_id=self.conn_id,
                    increment_generation=True,
                )
            else:
                # A matching later REST snapshot strengthens the stale-message guard
                # without changing generation or creating any transition.
                self.states[token].observe_venue_timestamp(book.get("timestamp"))

            self.writer.write(
                "rest_snapshots",
                {
                    "capture_id": self.writer.capture_id,
                    "request_started_at_utc": requested_at,
                    "response_received_at_utc": received_at,
                    "reason": "reconciliation_check",
                    "matched_local_state_before": matched,
                    "connection_id": self.conn_id,
                    "token": token,
                    "condition_id": self.token_meta[token]["condition_id"],
                    "role": self.token_meta[token]["role"],
                    "book_generation": self.states[token].generation,
                    "book": book,
                },
            )

        self.control(
            "rest_reconcile_complete",
            checked_tokens=len(tokens),
            returned_books=len(books),
            mismatch_tokens=len(mismatched),
            missing_tokens=len(missing),
            mismatched_token_ids=mismatched,
            missing_token_ids=missing,
        )

    async def run(self) -> str:
        now = utcnow()
        prearm = self.prearm_time()
        if now > prearm:
            raise RuntimeError(
                "late H6-M1 launch: collector must start before frozen prearm time"
            )

        self.control(
            "collector_waiting_for_prearm",
            prearm_time_utc=prearm,
            earliest_primary_window_start_utc=self.earliest_window_start(),
            latest_game_start_utc=self.latest_start(),
        )

        while utcnow() < prearm:
            if shutil.disk_usage(REPO).free < MIN_FREE_DISK_BYTES:
                raise RuntimeError("disk safety gate failed while waiting for prearm")
            remaining = (prearm - utcnow()).total_seconds()
            await asyncio.sleep(min(5.0, max(0.05, remaining)))

        async with httpx.AsyncClient(headers=UA, timeout=30) as http:
            while utcnow() < self.latest_start():
                if shutil.disk_usage(REPO).free < MIN_FREE_DISK_BYTES:
                    return "DISK_SAFETY"

                self.conn_id = str(uuid.uuid4())
                self.frame_sequence = 0
                self.ingest_sequence = 0
                active_tokens = self.active_tokens()
                if not active_tokens:
                    break
                self.control("connection_start", subscribed_tokens=len(active_tokens))

                try:
                    async with websockets.connect(WS_URI, max_size=None) as ws:
                        await ws.send(
                            json.dumps(
                                {
                                    "assets_ids": active_tokens,
                                    "type": "market",
                                    "custom_feature_enabled": True,
                                }
                            )
                        )
                        await self.rest_resync(
                            http,
                            tokens=active_tokens,
                            reason="connect",
                            require_all=True,
                        )
                        self.last_message_monotonic = time.monotonic()
                        last_ping = time.monotonic()
                        last_reconcile = time.monotonic()
                        self.control("capture_initialized")

                        while utcnow() < self.latest_start():
                            now = utcnow()
                            self.record_due_boundaries(now)

                            try:
                                raw = await asyncio.wait_for(ws.recv(), timeout=0.25)
                                self.last_message_monotonic = time.monotonic()
                                if isinstance(raw, bytes):
                                    self.control("binary_frame_ignored", bytes=len(raw))
                                else:
                                    capture_time = utcnow()

                                    mismatches = self.process_text_frame(
                                        raw,
                                        capture_time=capture_time,
                                    )

                                    if mismatches:
                                        self.control(
                                            "venue_tob_mismatch",
                                            tokens=sorted(mismatches),
                                        )
                                        await self.rest_resync(
                                            http,
                                            tokens=sorted(mismatches),
                                            reason="venue_tob_mismatch",
                                            require_all=True,
                                        )
                            except asyncio.TimeoutError:
                                pass

                            now_mono = time.monotonic()
                            if now_mono - last_ping >= PING_SECONDS:
                                await ws.send("PING")
                                last_ping = now_mono

                            if now_mono - last_reconcile >= RECONCILE_SECONDS:
                                active = self.active_tokens()
                                await self.reconcile(http, tokens=active)
                                last_reconcile = now_mono

                            if now_mono - self.last_message_monotonic > SILENCE_SECONDS:
                                raise RuntimeError("websocket silence watchdog fired")

                            self.writer.flush()

                        self.record_due_boundaries(utcnow())
                        return "ALL_REGISTERED_GAMES_REACHED_START"

                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    active = self.active_tokens()
                    self.control(
                        "connection_error",
                        error_type=type(exc).__name__,
                        invalidated_tokens=active,
                    )
                    for token in active:
                        self.states[token].valid = False
                    if utcnow() >= self.latest_start():
                        break
                    await asyncio.sleep(1)

        self.record_due_boundaries(utcnow())
        return "ALL_REGISTERED_GAMES_REACHED_START"


async def run_live(capture_id: str | None) -> None:
    bundle = load_registry_bundle()

    starts = [
        parse_dt(row["game_start_time_utc"])
        for row in bundle["rows"]
    ]
    assert all(x is not None for x in starts)
    starts = [x for x in starts if x is not None]
    prearm = min(starts) - timedelta(
        seconds=PRIMARY_WINDOW_SECONDS + PREARM_SECONDS
    )
    if utcnow() > prearm:
        raise RuntimeError(
            "late H6-M1 launch: refusing incomplete fixed-window capture"
        )

    if shutil.disk_usage(REPO).free < MIN_FREE_DISK_BYTES:
        raise RuntimeError("disk safety gate failed before capture arm")

    capture_id = capture_id or (
        "h6m1_" + utcnow().strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
    )
    writer = CaptureWriter(capture_id, bundle)
    capture = H6Capture(bundle=bundle, writer=writer)
    stop_reason = "COLLECTOR_FAILURE"

    try:
        stop_reason = await capture.run()
    finally:
        writer.finalize(
            stop_reason=stop_reason,
            extra_counts={
                "window_open_records": len(capture.window_open_recorded),
                "window_close_records": len(capture.window_close_recorded),
            },
        )

    print("========================================")
    print("H6-M1 DEDICATED CLOB CAPTURE COMPLETE")
    print("========================================")
    print("capture_id:", capture_id)
    print("stop_reason:", stop_reason)
    print("H6 imbalance calculated: NO")
    print("H6 transition count calculated: NO")
    print("future midpoint response calculated: NO")
    print("PnL calculated: NO")
    print("F19 trade content read: NO")
    print("F20 read: NO")
    print("H5 response read: NO")


def validate_config_output() -> None:
    validate_frozen_inputs(require_committed_self=False)
    registry_frozen = REGISTRY.is_file() and REGISTRY_RECEIPT.is_file()
    print("H6-M1 COLLECTOR CONFIG: PASS")
    print("parent registrar commit:", EXPECTED_PARENT_HEAD)
    print("M0 SHA:", EXPECTED_M0_SHA)
    print("independence SHA:", EXPECTED_INDEPENDENCE_SHA)
    print("registration contract SHA:", EXPECTED_REGISTRATION_CONTRACT_SHA)
    print("registrar SHA:", EXPECTED_REGISTRAR_SHA)
    print("capture contract SHA:", EXPECTED_CAPTURE_CONTRACT_SHA)
    print("market count:", MARKET_COUNT)
    print("primary window seconds:", PRIMARY_WINDOW_SECONDS)
    print("prearm seconds:", PREARM_SECONDS)
    print("reconciliation seconds:", RECONCILE_SECONDS)
    print("registry frozen:", "YES" if registry_frozen else "NO")
    print(
        "live acquisition permitted now:",
        "YES" if registry_frozen and committed_self_matches_head() else "NO",
    )
    print("network calls made: NO")
    print("H6 imbalance calculated: NO")
    print("H6 transition count calculated: NO")
    print("future midpoint response calculated: NO")
    print("PnL calculated: NO")
    print("F19 trade content read: NO")
    print("F20 read: NO")
    print("H5 response read: NO")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--validate-config", action="store_true")
    parser.add_argument("--run-live", action="store_true")
    parser.add_argument("--capture-id", default=None)
    args = parser.parse_args()

    if args.validate_config:
        validate_config_output()
        return
    if args.run_live:
        asyncio.run(run_live(args.capture_id))
        return
    raise SystemExit("choose --validate-config or --run-live")


if __name__ == "__main__":
    main()
