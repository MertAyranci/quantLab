from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from statistics import median
from typing import Any

import httpx
import websockets
from dotenv import dotenv_values


REPO = Path(__file__).resolve().parents[2]
RESEARCH = REPO / "research"

sys.path.insert(0, str(RESEARCH))
import h5_m1_collector_compat as compat  # noqa: E402


M1A_CONTRACT = RESEARCH / "h5_m1a_acquisition_contract.json"

COMPATIBILITY_CONTRACT = (
    RESEARCH
    / "h5_m1_collector_compatibility_contract.json"
)

COMPAT_LOADER = (
    RESEARCH
    / "h5_m1_collector_compat.py"
)

EXPECTED_M1A_SHA = (
    "6da26ac94413b5a79566764dcc43d4584"
    "bff94c3b88531225dec09a4db99b1be"
)
EXPECTED_M1A_COMMIT = "de347d585687a3aaf33e704f37b43a75ba7816a9"

EXPECTED_COMPATIBILITY_CONTRACT_SHA = (
    "f7e69a2f4fc0734e62fea7abd5615b99"
    "25c86cf23438698ea014fa1a164eaecf"
)

EXPECTED_COMPAT_LOADER_SHA = (
    "4bba3a0c46d227b2aa64e9ee8bdf18ac"
    "35414f1135c3edbb2c0372a4c87e88dc"
)

EXPECTED_COMPAT_CONTRACT_COMMIT = (
    "5f06e9b27125a7b2a02a0edaa4b72cf6d82ef10f"
)

EXPECTED_COMPAT_LOADER_COMMIT = (
    "a7549f659583bb1b443d32a9ebaeb41aa25f571f"
)

PINNED_PROVENANCE = {
    "collectors/odds/h2_v2_sharp_collector.py": (
        "81d2a6abadc9d5fe2f218be35f301d33"
        "f00a963dd46626c7fb55445883d5130d"
    ),
    "collectors/odds/h2_v2_event_mapper.py": (
        "c59e10af09640c77c9f97d4a91c172f"
        "fa84f0dd68512f9a984d42335643d3bb7"
    ),
    "collectors/polymarket/ws_collector.py": (
        "50f4ee810ae38974f3156431b6596980"
        "c514c8d17ca11f4c8cc341ff058951c0"
    ),
}

DATA_ROOT = REPO / "data" / "research" / "h5_m1"
ODDS_URL = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"
CLOB = "https://clob.polymarket.com"
WS_URI = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

BOOKMAKERS = ["betfair_ex_uk", "matchbook", "smarkets"]
MAX_SOURCE_AGE_SECONDS = Decimal("25")
MIN_FRESH_FAMILIES = 2
POLL_INTERVAL_SECONDS = 10
MAX_POLLS = 60
INITIAL_RUN_SECONDS = 600
PING_SECONDS = 10
SILENCE_SECONDS = 30
RECONCILE_SECONDS = 300
MIN_FREE_DISK_BYTES = 4 * 1024**3
MIN_REMAINING_CREDITS = 50

UA_ODDS = {"User-Agent": "quant-lab-h5-m1-odds/1.0"}
UA_PM = {"User-Agent": "quant-lab-h5-m1-pm/1.0"}
CAPTURE_RE = re.compile(r"^[A-Za-z0-9._-]+$")


class MappingError(RuntimeError):
    pass


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


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


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


def append_jsonl(fh, record: dict[str, Any]):
    fh.write(
        json.dumps(
            json_clean(record),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        + "\n"
    )


def header_int(headers, key: str) -> int | None:
    value = headers.get(key)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def validate_frozen_engineering_inputs(
    *,
    require_committed_self: bool,
):
    if not M1A_CONTRACT.is_file():
        raise RuntimeError(
            "missing H5-M1A contract"
        )

    if (
        sha256_file(M1A_CONTRACT)
        != EXPECTED_M1A_SHA
    ):
        raise RuntimeError(
            "H5-M1A contract SHA mismatch"
        )

    if (
        not COMPATIBILITY_CONTRACT.is_file()
        or
        sha256_file(
            COMPATIBILITY_CONTRACT
        )
        !=
        EXPECTED_COMPATIBILITY_CONTRACT_SHA
    ):
        raise RuntimeError(
            "H5 compatibility contract "
            "SHA mismatch"
        )

    if (
        not COMPAT_LOADER.is_file()
        or
        sha256_file(
            COMPAT_LOADER
        )
        !=
        EXPECTED_COMPAT_LOADER_SHA
    ):
        raise RuntimeError(
            "H5 compatibility loader "
            "SHA mismatch"
        )

    # Static-only validation of the frozen
    # Stage-1 / compatibility provenance.
    # This performs no network access and does
    # not require the Stage-2 map to exist.
    compat.load_static_inputs()

    contract = json.loads(
        M1A_CONTRACT.read_text(
            encoding="utf-8"
        )
    )

    source = contract[
        "external_source"
    ]

    polling = contract[
        "diagnostic_external_polling"
    ]

    capture = contract[
        "polymarket_capture"
    ]

    if (
        source["exchange_families"]
        != BOOKMAKERS
    ):
        raise RuntimeError(
            "exchange-family order/set "
            "mismatch"
        )

    if (
        source[
            "maximum_source_age_seconds"
        ]
        != 25
    ):
        raise RuntimeError(
            "source-age freeze mismatch"
        )

    if (
        source[
            "minimum_fresh_complete_families"
        ]
        != 2
    ):
        raise RuntimeError(
            "fresh-family freeze mismatch"
        )

    if (
        polling["interval_seconds"]
        != POLL_INTERVAL_SECONDS
    ):
        raise RuntimeError(
            "poll interval freeze mismatch"
        )

    if (
        polling[
            "maximum_polls_initial_run"
        ]
        != MAX_POLLS
    ):
        raise RuntimeError(
            "poll-count freeze mismatch"
        )

    if (
        polling[
            "maximum_initial_run_seconds"
        ]
        != INITIAL_RUN_SECONDS
    ):
        raise RuntimeError(
            "run-duration freeze mismatch"
        )

    if (
        capture["source"]
        != "live CLOB WebSocket"
    ):
        raise RuntimeError(
            "PM source freeze mismatch"
        )

    for rel, expected in (
        PINNED_PROVENANCE.items()
    ):
        provenance_path = (
            REPO / rel
        )

        if (
            not provenance_path.is_file()
            or
            sha256_file(
                provenance_path
            )
            != expected
        ):
            raise RuntimeError(
                "pinned provenance SHA "
                f"mismatch: {rel}"
            )

    if require_committed_self:
        for ancestor in (
            EXPECTED_M1A_COMMIT,
            EXPECTED_COMPAT_CONTRACT_COMMIT,
            EXPECTED_COMPAT_LOADER_COMMIT,
        ):
            if not git_is_ancestor(
                ancestor
            ):
                raise RuntimeError(
                    "required H5 freeze commit "
                    "is not an ancestor of HEAD: "
                    f"{ancestor}"
                )

        if not committed_self_matches_head():
            raise RuntimeError(
                "refusing live H5 capture: "
                "collector differs from "
                "committed HEAD"
            )

    return contract


def validate_registry_rows(rows: list[dict[str, Any]]):
    if not 5 <= len(rows) <= 10:
        raise RuntimeError(f"invalid H5 registry size: {len(rows)}")

    odds_ids: set[str] = set()
    condition_ids: set[str] = set()
    tokens: set[str] = set()

    required = {
        "odds_game_id",
        "condition_id",
        "canonical_commence_time",
        "home_team",
        "away_team",
        "p1_team",
        "p0_team",
        "p1_token_id",
        "p0_token_id",
    }

    for i, row in enumerate(rows, start=1):
        missing = required - set(row)
        if missing:
            raise RuntimeError(f"registry row {i} missing fields: {sorted(missing)}")
        if row.get("study") != "H5" or row.get("milestone") != "H5-M1":
            raise RuntimeError(f"registry row {i} wrong study/milestone")
        if row.get("registration_index") != i:
            raise RuntimeError(f"registry row {i} order/index mismatch")
        if parse_dt(row["canonical_commence_time"]) is None:
            raise RuntimeError(f"registry row {i} invalid commence time")

        oid = str(row["odds_game_id"])
        cid = str(row["condition_id"]).lower()
        p1 = str(row["p1_token_id"])
        p0 = str(row["p0_token_id"])

        if oid in odds_ids or cid in condition_ids:
            raise RuntimeError("duplicate H5 market identity")
        if p1 == p0 or p1 in tokens or p0 in tokens:
            raise RuntimeError("duplicate H5 token identity")

        odds_ids.add(oid)
        condition_ids.add(cid)
        tokens.update((p1, p0))


def load_registry_bundle():
    validate_frozen_engineering_inputs(
        require_committed_self=True
    )

    return (
        compat.load_runtime_bundle()
    )


def decimal_price(outcome: dict[str, Any] | None) -> Decimal | None:
    if not outcome or outcome.get("price") is None:
        return None
    try:
        value = Decimal(str(outcome["price"]))
    except (InvalidOperation, ValueError, TypeError):
        return None
    if value <= Decimal("1"):
        return None
    return value


def probability(decimal_odds: Decimal | None) -> Decimal | None:
    if decimal_odds is None:
        return None
    return Decimal("1") / decimal_odds


def outcomes_by_norm_name(market: dict[str, Any] | None):
    if not market:
        return {}
    out = {}
    for outcome in market.get("outcomes", []) or []:
        name = outcome.get("name")
        if not name:
            continue
        key = compat.norm_team(name)
        if key in out:
            raise MappingError(f"duplicate normalized external outcome: {key}")
        out[key] = outcome
    return out


def safe_odds_params(event_ids: list[str]) -> dict[str, str]:
    return {
        "bookmakers": ",".join(BOOKMAKERS),
        "markets": "h2h",
        "oddsFormat": "decimal",
        "dateFormat": "iso",
        "eventIds": ",".join(event_ids),
    }


def api_odds_params(api_key: str, event_ids: list[str]):
    return {"apiKey": api_key, **safe_odds_params(event_ids)}


def _external_team_keys(reg: dict[str, Any]):
    home_key = compat.norm_team(reg["home_team"])
    away_key = compat.norm_team(reg["away_team"])
    p1_key = compat.norm_team(reg["p1_team"])
    p0_key = compat.norm_team(reg["p0_team"])

    expected = {home_key, away_key}
    if p1_key not in expected or p0_key not in expected or p1_key == p0_key:
        raise MappingError("registry P1/P0 team mapping is not unique")
    return home_key, away_key, p1_key, p0_key


def parse_external_observation(
    *,
    capture_id: str,
    poll_sequence: int,
    reg: dict[str, Any],
    game: dict[str, Any],
    requested_at: datetime,
    received_at: datetime,
    latency_ms: Decimal,
):
    if str(game.get("id")) != str(reg["odds_game_id"]):
        raise MappingError("Odds API event ID mismatch")

    _, _, p1_key, p0_key = _external_team_keys(reg)
    game_teams = {
        compat.norm_team(game.get("home_team")),
        compat.norm_team(game.get("away_team")),
    }
    if game_teams != {p1_key, p0_key}:
        raise MappingError("Odds API game team-set mismatch")

    canonical_start = parse_dt(reg["canonical_commence_time"])
    if canonical_start is None:
        raise MappingError("registry canonical commence time invalid")
    if not received_at < canonical_start:
        raise MappingError("attempted to admit in-play external observation")

    books = {
        str(book.get("key")): book
        for book in game.get("bookmakers", []) or []
        if book.get("key")
    }

    family_rows = []
    fresh_midpoints: list[tuple[str, Decimal]] = []

    for family in BOOKMAKERS:
        book = books.get(family)
        source_ts = parse_dt(book.get("last_update")) if book else None
        source_age = None
        if source_ts is not None:
            source_age = Decimal(str((received_at - source_ts).total_seconds()))

        markets = {
            str(m.get("key")): m
            for m in (book.get("markets", []) if book else [])
            if m.get("key")
        }
        backs = outcomes_by_norm_name(markets.get("h2h"))
        lays = outcomes_by_norm_name(markets.get("h2h_lay"))

        family_pair_complete = all(
            decimal_price(backs.get(team_key)) is not None
            and decimal_price(lays.get(team_key)) is not None
            for team_key in (p1_key, p0_key)
        )

        p1_back = decimal_price(backs.get(p1_key))
        p1_lay = decimal_price(lays.get(p1_key))
        p1_family_probability = None
        if p1_back is not None and p1_lay is not None:
            p1_family_probability = (
                probability(p1_back) + probability(p1_lay)
            ) / Decimal("2")

        fresh = bool(
            family_pair_complete
            and source_age is not None
            and Decimal("0") <= source_age <= MAX_SOURCE_AGE_SECONDS
        )

        row = {
            "capture_id": capture_id,
            "poll_sequence": poll_sequence,
            "odds_game_id": str(reg["odds_game_id"]),
            "condition_id": str(reg["condition_id"]).lower(),
            "canonical_commence_time": canonical_start,
            "p1_team": reg["p1_team"],
            "p0_team": reg["p0_team"],
            "request_started_at_utc": requested_at,
            "response_received_at_utc": received_at,
            "request_latency_ms": latency_ms,
            "exchange_family": family,
            "family_source_timestamp_utc": source_ts,
            "source_age_seconds": source_age,
            "p1_back_decimal": p1_back,
            "p1_lay_decimal": p1_lay,
            "p1_family_probability": p1_family_probability,
            "family_pair_complete": family_pair_complete,
            "family_fresh": fresh,
        }
        family_rows.append(row)

        if fresh and p1_family_probability is not None:
            fresh_midpoints.append((family, p1_family_probability))

    families_used = [name for name, _ in fresh_midpoints]
    consensus = None
    if len(fresh_midpoints) >= MIN_FRESH_FAMILIES:
        consensus = median([value for _, value in fresh_midpoints])

    consensus_row = {
        "capture_id": capture_id,
        "poll_sequence": poll_sequence,
        "odds_game_id": str(reg["odds_game_id"]),
        "condition_id": str(reg["condition_id"]).lower(),
        "p1_team": reg["p1_team"],
        "response_received_at_utc": received_at,
        "valid_fresh_family_count": len(fresh_midpoints),
        "families_used": families_used,
        "consensus_p1_probability": consensus,
        "accepted_external_state": consensus is not None,
    }

    return family_rows, consensus_row


class CaptureWriter:
    def __init__(self, capture_id: str, bundle: dict[str, Any]):
        if not capture_id or not CAPTURE_RE.fullmatch(capture_id):
            raise ValueError("invalid H5 capture_id")

        self.capture_id = capture_id
        self.capture_dir = DATA_ROOT / capture_id
        self.capture_dir.mkdir(parents=True, exist_ok=False)
        self.raw_dir = self.capture_dir / "external_raw"
        self.raw_dir.mkdir(exist_ok=False)

        self.paths = {
            "external_polls": self.capture_dir / "external_polls.jsonl",
            "external_family": self.capture_dir / "external_family.jsonl",
            "external_consensus": self.capture_dir / "external_consensus.jsonl",
            "pm_raw_frames": self.capture_dir / "pm_raw_frames.jsonl",
            "pm_normalized": self.capture_dir / "pm_normalized.jsonl",
            "pm_control": self.capture_dir / "pm_control.jsonl",
        }
        self.handles = {
            name: path.open("x", encoding="utf-8", buffering=1)
            for name, path in self.paths.items()
        }
        self.counts = {name: 0 for name in self.paths}
        self.counts.update(
            {
                "accepted_external_states": 0,
                "eligible_external_observations": 0,
                "mapping_failures": 0,
            }
        )

        start_manifest = {
            "study": "H5",
            "milestone": "H5-M1",
            "status": "CAPTURE_STARTED",
            "capture_id": capture_id,
            "created_at_utc": utcnow(),
            "git_head": git_head(),
            "collector_path": "collectors/odds/h5_m1_collector.py",
            "collector_sha256": sha256_file(Path(__file__).resolve()),
            "m1a_contract_sha256": EXPECTED_M1A_SHA,
            "compatibility_contract_sha256": (
                bundle[
                    "compatibility_contract_sha256"
                ]
            ),
            "stage1_registry_sha256": (
                bundle[
                    "stage1_registry_sha256"
                ]
            ),
            "stage1_receipt_sha256": (
                bundle[
                    "stage1_receipt_sha256"
                ]
            ),
            "stage2_map_sha256": (
                bundle[
                    "stage2_map_sha256"
                ]
            ),
            "stage2_receipt_sha256": (
                bundle[
                    "stage2_receipt_sha256"
                ]
            ),
            "stage1_registered_market_count": (
                bundle[
                    "stage1_registered_market_count"
                ]
            ),
            "acquisition_market_count": (
                bundle[
                    "acquisition_market_count"
                ]
            ),
            "poll_interval_seconds": POLL_INTERVAL_SECONDS,
            "maximum_polls": MAX_POLLS,
            "initial_run_seconds": INITIAL_RUN_SECONDS,
            "minimum_remaining_odds_credits": MIN_REMAINING_CREDITS,
            "minimum_free_disk_bytes": MIN_FREE_DISK_BYTES,
            "analysis_boundary": boundary_flags(),
        }
        self._write_exclusive_json(self.capture_dir / "capture_start.json", start_manifest)

    @staticmethod
    def _write_exclusive_json(path: Path, record: dict[str, Any]):
        with path.open("x", encoding="utf-8") as fh:
            json.dump(
                json_clean(record),
                fh,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            )
            fh.write("\n")

    def write(self, stream: str, record: dict[str, Any]):
        append_jsonl(self.handles[stream], record)
        self.counts[stream] += 1

    def save_external_raw(self, poll_sequence: int, data: bytes):
        path = self.raw_dir / f"poll_{poll_sequence:03d}.json"
        with path.open("xb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        return {
            "path": str(path.relative_to(REPO)),
            "sha256": sha256_bytes(data),
            "bytes": len(data),
        }

    def write_raw_pm_frame(self, *, connection_id: str, frame_sequence: int, capture_time: datetime, raw: str):
        raw_bytes = raw.encode("utf-8")
        record = {
            "capture_id": self.capture_id,
            "capture_time": capture_time,
            "connection_id": connection_id,
            "frame_sequence": frame_sequence,
            "raw_frame_sha256": sha256_bytes(raw_bytes),
            "raw": raw,
        }
        self.write("pm_raw_frames", record)
        return record["raw_frame_sha256"]

    def flush(self):
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
        raw_files = sorted(self.raw_dir.glob("poll_*.json"))
        raw_hashes = [
            {
                "path": str(path.relative_to(REPO)),
                "sha256": sha256_file(path),
                "bytes": path.stat().st_size,
            }
            for path in raw_files
        ]
        final = {
            "study": "H5",
            "milestone": "H5-M1",
            "status": "CAPTURE_COMPLETE",
            "capture_id": self.capture_id,
            "completed_at_utc": utcnow(),
            "stop_reason": stop_reason,
            "counts": {**self.counts, **(extra_counts or {})},
            "stream_hashes": hashes,
            "external_raw_files": raw_hashes,
            "analysis_boundary": boundary_flags(),
        }
        self._write_exclusive_json(self.capture_dir / "capture_complete.json", final)
        return final


def boundary_flags():
    return {
        "external_state_differences_calculated": False,
        "h5_innovation_calculated": False,
        "pm_response_calculated": False,
        "future_pm_price_change_calculated": False,
        "directional_hit_rate_calculated": False,
        "correlation_calculated": False,
        "regression_calculated": False,
        "markout_calculated": False,
        "pnl_calculated": False,
        "winner_used": False,
        "settlement_used": False,
        "f19_trade_content_read": False,
        "f20_read": False,
    }


def _best_tob(book: dict[str, Any]):
    bids = book.get("bids") or []
    asks = book.get("asks") or []
    try:
        best_bid = max((Decimal(str(x["price"])) for x in bids), default=None)
        best_ask = min((Decimal(str(x["price"])) for x in asks), default=None)
    except (InvalidOperation, KeyError, TypeError, ValueError):
        return None, None
    return (
        str(best_bid) if best_bid is not None else None,
        str(best_ask) if best_ask is not None else None,
    )


class H5PMCapture:
    def __init__(self, *, bundle: dict[str, Any], writer: CaptureWriter):
        self.bundle = bundle
        self.writer = writer
        self.rows = bundle["rows"]
        self.token_start: dict[str, datetime] = {}
        for row in self.rows:
            start = parse_dt(row["canonical_commence_time"])
            assert start is not None
            self.token_start[str(row["p1_token_id"])] = start
            self.token_start[str(row["p0_token_id"])] = start

        self.generation = {token: 0 for token in self.token_start}
        self.last_tob: dict[str, tuple[str | None, str | None]] = {}
        self.conn_id = ""
        self.seq = 0
        self.frame_sequence = 0
        self.last_msg_monotonic = 0.0
        self.initialized_once = False

    def active_tokens(self, now: datetime | None = None):
        now = now or utcnow()
        return sorted(token for token, start in self.token_start.items() if now < start)

    def token_active(self, token: str, capture_time: datetime):
        start = self.token_start.get(token)
        return start is not None and capture_time < start

    def control(self, kind: str, **payload):
        self.writer.write(
            "pm_control",
            {
                "capture_id": self.writer.capture_id,
                "capture_time": utcnow(),
                "connection_id": self.conn_id or None,
                "kind": kind,
                **payload,
            },
        )

    def emit(self, kind: str, token: str, payload: dict[str, Any], *, capture_time: datetime, change_index: int = 0, raw_frame_sequence: int | None = None, raw_frame_sha256: str | None = None):
        if token not in self.token_start:
            return
        if not self.token_active(token, capture_time):
            return
        record = {
            "capture_id": self.writer.capture_id,
            "kind": kind,
            "token": token,
            "capture_time": capture_time,
            "connection_id": self.conn_id,
            "ingest_sequence": self.seq,
            "change_index": change_index,
            "book_generation": self.generation[token],
            "venue_timestamp": payload.get("timestamp"),
            "raw_frame_sequence": raw_frame_sequence,
            "raw_frame_sha256": raw_frame_sha256,
            "payload": payload,
        }
        self.writer.write("pm_normalized", record)

    def handle_raw(self, raw: str, *, capture_time: datetime | None = None):
        capture_time = capture_time or utcnow()
        self.frame_sequence += 1
        digest = self.writer.write_raw_pm_frame(
            connection_id=self.conn_id,
            frame_sequence=self.frame_sequence,
            capture_time=capture_time,
            raw=raw,
        )
        if raw == "PONG":
            return
        try:
            msgs = json.loads(raw)
        except json.JSONDecodeError:
            self.control("unparseable_frame")
            return
        if isinstance(msgs, dict):
            msgs = [msgs]
        if not isinstance(msgs, list):
            self.control("unexpected_frame_shape")
            return

        for d in msgs:
            if not isinstance(d, dict):
                continue
            self.seq += 1
            et = d.get("event_type")
            token = str(d.get("asset_id")) if d.get("asset_id") else None

            if et == "price_change":
                changes = d.get("price_changes") or d.get("changes") or [d]
                for ci, change in enumerate(changes):
                    tok = str(change.get("asset_id")) if change.get("asset_id") else token
                    if not tok:
                        continue
                    payload = {**change, "market": d.get("market"), "timestamp": d.get("timestamp")}
                    self.emit(
                        "price_change",
                        tok,
                        payload,
                        capture_time=capture_time,
                        change_index=ci,
                        raw_frame_sequence=self.frame_sequence,
                        raw_frame_sha256=digest,
                    )
                    if change.get("best_bid") is not None or change.get("best_ask") is not None:
                        self.last_tob[tok] = (change.get("best_bid"), change.get("best_ask"))

            elif et == "book" and token:
                self.emit(
                    "book",
                    token,
                    d,
                    capture_time=capture_time,
                    raw_frame_sequence=self.frame_sequence,
                    raw_frame_sha256=digest,
                )
                self.last_tob[token] = _best_tob(d)

            elif et == "best_bid_ask" and token:
                self.emit(
                    "best_bid_ask",
                    token,
                    d,
                    capture_time=capture_time,
                    raw_frame_sequence=self.frame_sequence,
                    raw_frame_sha256=digest,
                )
                self.last_tob[token] = (d.get("best_bid"), d.get("best_ask"))

            elif et == "tick_size_change" and token:
                self.emit(
                    "tick_size_change",
                    token,
                    d,
                    capture_time=capture_time,
                    raw_frame_sequence=self.frame_sequence,
                    raw_frame_sha256=digest,
                )

            elif et in ("market_resolved",):
                self.control("forbidden_resolution_event_ignored", event_type=et)

    async def rest_books(self, http: httpx.AsyncClient, tokens: list[str]):
        out = []
        for i in range(0, len(tokens), 50):
            chunk = tokens[i : i + 50]
            try:
                response = await http.post(
                    f"{CLOB}/books",
                    json=[{"token_id": token} for token in chunk],
                )
            except Exception as exc:
                raise RuntimeError(f"PM REST request failed: {type(exc).__name__}") from None
            if response.status_code != 200:
                raise RuntimeError(f"PM REST /books failed: HTTP {response.status_code}")
            payload = response.json()
            if not isinstance(payload, list):
                raise RuntimeError("PM REST /books response is not a list")
            out.extend(payload)
        return out

    async def resync(self, http: httpx.AsyncClient, *, reason: str, require_all: bool):
        tokens = self.active_tokens()
        if not tokens:
            return
        books = await self.rest_books(http, tokens)
        returned = {str(book.get("asset_id")) for book in books if book.get("asset_id")}
        missing = set(tokens) - returned
        if require_all and missing:
            raise RuntimeError(f"initial PM REST snapshot missing {len(missing)} tokens")

        capture_time = utcnow()
        for book in books:
            token = str(book.get("asset_id"))
            if token not in self.token_start or not self.token_active(token, capture_time):
                continue
            self.generation[token] += 1
            self.seq += 1
            self.emit("rest_resync_book", token, book, capture_time=capture_time)
            self.last_tob[token] = _best_tob(book)
        self.control("rest_resync_complete", reason=reason, books=len(books), missing=len(missing))

    async def reconcile(self, http: httpx.AsyncClient):
        tokens = self.active_tokens()
        if not tokens:
            return
        books = await self.rest_books(http, tokens)
        capture_time = utcnow()
        changed = 0
        for book in books:
            token = str(book.get("asset_id"))
            if token not in self.token_start or not self.token_active(token, capture_time):
                continue
            rest_tob = _best_tob(book)
            if self.last_tob.get(token) != rest_tob:
                self.generation[token] += 1
                self.seq += 1
                self.emit("rest_resync_book", token, book, capture_time=capture_time)
                self.last_tob[token] = rest_tob
                changed += 1
        self.control("rest_reconcile_complete", changed=changed, books=len(books))

    async def run(self, *, ready_event: asyncio.Event, stop_event: asyncio.Event):
        async with httpx.AsyncClient(headers=UA_PM, timeout=30) as http:
            while not stop_event.is_set():
                tokens = self.active_tokens()
                if not tokens:
                    self.control("all_tokens_reached_start")
                    return

                self.conn_id = str(uuid.uuid4())
                self.seq = 0
                self.frame_sequence = 0
                self.control("connection_start", subscribed_tokens=len(tokens))

                try:
                    async with websockets.connect(WS_URI, max_size=None) as ws:
                        await ws.send(
                            json.dumps(
                                {
                                    "assets_ids": tokens,
                                    "type": "market",
                                    "custom_feature_enabled": True,
                                }
                            )
                        )
                        await self.resync(http, reason="connect", require_all=True)
                        if not self.initialized_once:
                            self.initialized_once = True
                            ready_event.set()
                            self.control("initial_pm_capture_initialized")

                        subscribed = set(tokens)
                        self.last_msg_monotonic = time.monotonic()
                        last_ping = time.monotonic()
                        last_reconcile = time.monotonic()

                        while not stop_event.is_set():
                            active = set(self.active_tokens())
                            expired = subscribed - active
                            if expired:
                                await ws.send(
                                    json.dumps(
                                        {"assets_ids": sorted(expired), "operation": "unsubscribe"}
                                    )
                                )
                                subscribed -= expired
                                self.control("tokens_unsubscribed_at_start", count=len(expired))
                            if not subscribed:
                                self.control("all_subscribed_tokens_reached_start")
                                return

                            try:
                                raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                                self.last_msg_monotonic = time.monotonic()
                                if isinstance(raw, bytes):
                                    self.control("binary_frame_ignored", bytes=len(raw))
                                else:
                                    self.handle_raw(raw)
                            except asyncio.TimeoutError:
                                pass

                            now_mono = time.monotonic()
                            if now_mono - last_ping >= PING_SECONDS:
                                await ws.send("PING")
                                last_ping = now_mono
                            if now_mono - last_reconcile >= RECONCILE_SECONDS:
                                await self.reconcile(http)
                                last_reconcile = now_mono
                            if now_mono - self.last_msg_monotonic > SILENCE_SECONDS:
                                raise RuntimeError("PM websocket silence watchdog fired")

                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.control("connection_error", error_type=type(exc).__name__)
                    if not ready_event.is_set():
                        await asyncio.sleep(1)
                        continue
                    if stop_event.is_set():
                        return
                    await asyncio.sleep(1)


async def collect_external_once(
    *,
    http: httpx.AsyncClient,
    api_key: str,
    bundle: dict[str, Any],
    writer: CaptureWriter,
    poll_sequence: int,
):
    now = utcnow()
    active_rows = [
        row
        for row in bundle["rows"]
        if (parse_dt(row["canonical_commence_time"]) or now) > now
    ]
    if not active_rows:
        return {"all_games_started": True, "credits_remaining": None}

    event_ids = [str(row["odds_game_id"]) for row in active_rows]
    requested_at = utcnow()
    t0 = time.monotonic()
    try:
        response = await http.get(
            ODDS_URL,
            params=api_odds_params(api_key, event_ids),
        )
    except Exception as exc:
        raise RuntimeError(f"Odds API request failed: {type(exc).__name__}") from None
    received_at = utcnow()
    latency_ms = Decimal(str((time.monotonic() - t0) * 1000))

    raw_meta = writer.save_external_raw(poll_sequence, response.content)
    credits_remaining = header_int(response.headers, "x-requests-remaining")

    poll_record = {
        "capture_id": writer.capture_id,
        "poll_sequence": poll_sequence,
        "request_started_at_utc": requested_at,
        "response_received_at_utc": received_at,
        "request_latency_ms": latency_ms,
        "http_status": response.status_code,
        "request_params": safe_odds_params(event_ids),
        "requested_event_count": len(event_ids),
        "credits_last": header_int(response.headers, "x-requests-last"),
        "credits_used": header_int(response.headers, "x-requests-used"),
        "credits_remaining": credits_remaining,
        "raw_ref": raw_meta,
    }

    if response.status_code != 200:
        writer.write("external_polls", poll_record)
        body = response.text[:300]
        raise RuntimeError(
            f"Odds API returned HTTP {response.status_code}; body={body!r}"
        )

    games = response.json()
    if not isinstance(games, list):
        raise RuntimeError("Odds API response is not a list")
    poll_record["games_returned"] = len(games)

    games_by_id = {
        str(game.get("id")): game
        for game in games
        if isinstance(game, dict) and game.get("id")
    }

    eligible = 0
    accepted = 0
    omitted = 0

    for reg in active_rows:
        game = games_by_id.get(str(reg["odds_game_id"]))
        if game is None:
            omitted += 1
            continue
        try:
            family_rows, consensus_row = parse_external_observation(
                capture_id=writer.capture_id,
                poll_sequence=poll_sequence,
                reg=reg,
                game=game,
                requested_at=requested_at,
                received_at=received_at,
                latency_ms=latency_ms,
            )
        except MappingError:
            writer.counts["mapping_failures"] += 1
            raise

        for row in family_rows:
            writer.write("external_family", row)
        writer.write("external_consensus", consensus_row)
        eligible += 1
        accepted += int(consensus_row["accepted_external_state"])

    poll_record["eligible_external_observations"] = eligible
    poll_record["accepted_external_states"] = accepted
    poll_record["registered_events_omitted"] = omitted
    writer.write("external_polls", poll_record)

    writer.counts["eligible_external_observations"] += eligible
    writer.counts["accepted_external_states"] += accepted

    return {
        "all_games_started": False,
        "credits_remaining": credits_remaining,
        "eligible": eligible,
        "accepted": accepted,
    }


def disk_ok() -> bool:
    return shutil.disk_usage(REPO).free >= MIN_FREE_DISK_BYTES


def assert_pm_alive(pm_task: asyncio.Task):
    if not pm_task.done():
        return
    exc = pm_task.exception()
    if exc is not None:
        raise RuntimeError(f"PM capture task failed: {type(exc).__name__}") from None
    raise RuntimeError("PM capture task ended before acquisition completed")


async def sleep_with_pm_monitor(seconds: float, pm_task: asyncio.Task):
    end = time.monotonic() + max(0.0, seconds)
    while time.monotonic() < end:
        assert_pm_alive(pm_task)
        await asyncio.sleep(min(1.0, max(0.0, end - time.monotonic())))


async def run_live(capture_id: str | None):
    bundle = load_registry_bundle()
    env = dotenv_values(REPO / ".env")
    api_key = env.get("ODDS_API_KEY")
    if not api_key:
        raise RuntimeError("ODDS_API_KEY missing from .env")

    min_remaining = MIN_REMAINING_CREDITS

    if not disk_ok():
        raise RuntimeError("disk safety gate failed before capture")

    capture_id = capture_id or (
        "h5m1_" + utcnow().strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
    )
    writer = CaptureWriter(capture_id, bundle)
    stop_event = asyncio.Event()
    ready_event = asyncio.Event()
    pm = H5PMCapture(bundle=bundle, writer=writer)
    pm_task = asyncio.create_task(pm.run(ready_event=ready_event, stop_event=stop_event))
    stop_reason = "COLLECTOR_FAILURE"
    completed_polls = 0

    try:
        try:
            await asyncio.wait_for(ready_event.wait(), timeout=45)
        except asyncio.TimeoutError:
            raise RuntimeError("PM capture failed to initialize within 45 seconds") from None

        start_mono = time.monotonic()
        async with httpx.AsyncClient(headers=UA_ODDS, timeout=30) as http:
            for poll_sequence in range(1, MAX_POLLS + 1):
                if not disk_ok():
                    stop_reason = "DISK_SAFETY"
                    break
                assert_pm_alive(pm_task)

                result = await collect_external_once(
                    http=http,
                    api_key=api_key,
                    bundle=bundle,
                    writer=writer,
                    poll_sequence=poll_sequence,
                )
                completed_polls = poll_sequence
                writer.flush()

                if result.get("all_games_started"):
                    stop_reason = "ALL_SELECTED_GAMES_REACHED_START"
                    break

                remaining = result.get("credits_remaining")
                if remaining is not None and remaining < min_remaining:
                    stop_reason = "API_QUOTA_SAFETY"
                    break

                target = start_mono + poll_sequence * POLL_INTERVAL_SECONDS
                await sleep_with_pm_monitor(target - time.monotonic(), pm_task)
            else:
                elapsed = time.monotonic() - start_mono
                if elapsed < INITIAL_RUN_SECONDS:
                    await sleep_with_pm_monitor(INITIAL_RUN_SECONDS - elapsed, pm_task)
                stop_reason = "INITIAL_ENGINEERING_RUN_COMPLETE"

    finally:
        stop_event.set()
        try:
            await asyncio.wait_for(pm_task, timeout=10)
        except Exception:
            pm_task.cancel()
            try:
                await pm_task
            except BaseException:
                pass
        writer.finalize(
            stop_reason=stop_reason,
            extra_counts={"completed_external_polls": completed_polls},
        )

    print("========================================")
    print("H5-M1 SYNCHRONIZED CAPTURE COMPLETE")
    print("========================================")
    print("capture_id:", capture_id)
    print("stop_reason:", stop_reason)
    print("completed_external_polls:", completed_polls)
    print("eligible_external_observations:", writer.counts["eligible_external_observations"])
    print("accepted_external_states:", writer.counts["accepted_external_states"])
    print("H5 innovation calculated: NO")
    print("PM response calculated: NO")
    print("PnL calculated: NO")
    print("F19 trade content read: NO")
    print("F20 read: NO")


def validate_config_output():
    validate_frozen_engineering_inputs(
        require_committed_self=False
    )

    map_exists = (
        compat.STAGE2_MAP.is_file()
    )

    receipt_exists = (
        compat.STAGE2_RECEIPT.is_file()
    )

    if map_exists != receipt_exists:
        raise RuntimeError(
            "incomplete H5 Stage-2 freeze: "
            "map/receipt presence mismatch"
        )

    runtime_bundle = None

    if (
        map_exists
        and
        receipt_exists
    ):
        runtime_bundle = (
            compat.load_runtime_bundle()
        )

    live_permitted = bool(
        runtime_bundle is not None
        and
        committed_self_matches_head()
    )

    print(
        "H5-M1 COLLECTOR CONFIG: PASS"
    )

    print(
        "M1A SHA:",
        EXPECTED_M1A_SHA,
    )

    print(
        "compatibility contract SHA:",
        EXPECTED_COMPATIBILITY_CONTRACT_SHA,
    )

    print(
        "compatibility loader SHA:",
        EXPECTED_COMPAT_LOADER_SHA,
    )

    print(
        "Stage-1 registry frozen: YES"
    )

    print(
        "Stage-2 map frozen:",
        (
            "YES"
            if runtime_bundle is not None
            else
            "NO"
        ),
    )

    print(
        "acquisition markets:",
        (
            runtime_bundle[
                "acquisition_market_count"
            ]
            if runtime_bundle is not None
            else 0
        ),
    )

    print(
        "live acquisition permitted now:",
        (
            "YES"
            if live_permitted
            else
            "NO"
        ),
    )

    print(
        "network calls made: NO"
    )

    print(
        "H5 innovation calculated: NO"
    )

    print(
        "PM response calculated: NO"
    )

    print(
        "PnL calculated: NO"
    )

    print(
        "F19 trade content read: NO"
    )

    print(
        "F20 read: NO"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--validate-config", action="store_true")
    parser.add_argument("--run-live", action="store_true")
    parser.add_argument("--capture-id")
    args = parser.parse_args()

    if args.validate_config == args.run_live:
        parser.error("choose exactly one of --validate-config or --run-live")

    if args.validate_config:
        validate_config_output()
        return

    asyncio.run(run_live(args.capture_id))


if __name__ == "__main__":
    main()
