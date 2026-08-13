#!/usr/bin/env python3

import hashlib
import importlib.util
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
COLLECTOR_PATH = REPO / "collectors/polymarket/ws_collector.py"
CONTRACT_PATH = REPO / "research/h2_v2_m2e_contract.json"
OUTPUT_PATH = REPO / "research/h2_v2_m2e_validation.json"
VALIDATOR_PATH = Path(__file__).resolve()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


spec = importlib.util.spec_from_file_location(
    "h2_v2_m2e_ws_collector",
    COLLECTOR_PATH,
)

if spec is None or spec.loader is None:
    raise RuntimeError("could not load ws_collector.py")

ws = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ws)

# Freeze timestamps so enabled/disabled production records can be compared
# exactly rather than approximately.
FIXED_NOW = datetime(
    2026, 8, 13, 16, 0, 0,
    tzinfo=timezone.utc,
)

ws.utcnow = lambda: FIXED_NOW


class MemoryBuffer:
    def __init__(self):
        self.records = []
        self.flushes = 0

    def write(self, record):
        # Deep-copy through JSON so later mutation cannot affect evidence.
        self.records.append(
            json.loads(json.dumps(record))
        )

    def flush(self):
        self.flushes += 1


def jsonl(path: Path):
    if not path.exists():
        return []

    return [
        json.loads(line)
        for line in path.read_text(
            encoding="utf-8"
        ).splitlines()
        if line.strip()
    ]


def make_collector(
    root: Path,
    *,
    evidence=True,
    capture_id="synthetic",
):
    buf = MemoryBuffer()

    c = ws.WSCollector(
        1,
        evidence_capture_id=(
            capture_id
            if evidence
            else None
        ),
        evidence_root=root,
        buffer_writer=buf,
    )

    c.h2_v2_tokens = {
        "tok1",
        "tok2",
    }

    c.tokens = [
        "tok1",
        "tok2",
    ]

    c.generation = {
        "tok1": 7,
        "tok2": 11,
    }

    return c, buf


def price_change_raw():
    return json.dumps(
        {
            "event_type": "price_change",
            "market": "market-1",
            "timestamp": "1776096000000",
            "price_changes": [
                {
                    "asset_id": "tok1",
                    "price": "0.42",
                    "size": "7.5",
                    "side": "BUY",
                    "best_bid": "0.41",
                    "best_ask": "0.43",
                }
            ],
        },
        separators=(",", ":"),
    )


def multi_change_raw():
    return json.dumps(
        {
            "event_type": "price_change",
            "market": "market-1",
            "timestamp": "1776096000100",
            "price_changes": [
                {
                    "asset_id": "tok1",
                    "price": "0.42",
                    "size": "6.5",
                    "side": "BUY",
                    "best_bid": "0.41",
                    "best_ask": "0.43",
                },
                {
                    "asset_id": "tok2",
                    "price": "0.58",
                    "size": "9.25",
                    "side": "SELL",
                    "best_bid": "0.57",
                    "best_ask": "0.59",
                },
            ],
        },
        separators=(",", ":"),
    )


def book_raw():
    return json.dumps(
        {
            "event_type": "book",
            "asset_id": "tok1",
            "market": "market-1",
            "timestamp": "1776096000200",
            "bids": [
                {
                    "price": "0.41",
                    "size": "10",
                }
            ],
            "asks": [
                {
                    "price": "0.43",
                    "size": "12",
                }
            ],
            "hash": "synthetic-book-hash",
        },
        separators=(",", ":"),
    )


def trade_raw():
    return json.dumps(
        {
            "event_type": "last_trade_price",
            "asset_id": "tok1",
            "market": "market-1",
            "timestamp": "1776096000300",
            "price": "0.42",
            "size": "3",
            "side": "BUY",
            "fee_rate_bps": "0",
        },
        separators=(",", ":"),
    )


results = []


def test(name, fn):
    try:
        detail = fn()

        results.append(
            {
                "name": name,
                "pass": True,
                "detail": detail,
            }
        )

        print(f"PASS  {name}")

    except Exception as exc:
        results.append(
            {
                "name": name,
                "pass": False,
                "detail": (
                    f"{type(exc).__name__}: {exc}"
                ),
            }
        )

        print(
            f"FAIL  {name}: "
            f"{type(exc).__name__}: {exc}"
        )


# ----------------------------------------------------------------------
# 1. Disabled mode writes no evidence
# ----------------------------------------------------------------------

def t_disabled():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "evidence"

        c, buf = make_collector(
            root,
            evidence=False,
        )

        c._begin_connection(
            "conn-disabled"
        )

        c.handle(
            price_change_raw()
        )

        assert c.evidence is None
        assert len(buf.records) == 1
        assert not root.exists()

        c.http.close()

        return {
            "production_records": 1,
            "evidence_created": False,
        }


test(
    "disabled mode writes no M2e evidence",
    t_disabled,
)


# ----------------------------------------------------------------------
# 2. Exact raw frame preservation
# ----------------------------------------------------------------------

def t_raw_exact():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)

        c, _ = make_collector(
            root,
            capture_id="raw-exact",
        )

        c._begin_connection(
            "conn-raw"
        )

        raw = (
            '  {"event_type":"unknown_synthetic",'
            '"asset_id":"tok1","x":"αβ"}  '
        )

        c.handle(raw)
        c.evidence.flush()

        rows = jsonl(
            c.evidence.raw_path
        )

        assert len(rows) == 1
        assert rows[0]["raw"] == raw

        c.http.close()

        return {
            "raw_length": len(raw),
            "exact_match": True,
        }


test(
    "raw frame is preserved exactly",
    t_raw_exact,
)


# ----------------------------------------------------------------------
# 3. Exact raw SHA256
# ----------------------------------------------------------------------

def t_raw_sha():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)

        c, _ = make_collector(
            root,
            capture_id="raw-sha",
        )

        c._begin_connection(
            "conn-sha"
        )

        raw = book_raw()

        expected = hashlib.sha256(
            raw.encode("utf-8")
        ).hexdigest()

        c.handle(raw)
        c.evidence.flush()

        rows = jsonl(
            c.evidence.raw_path
        )

        assert len(rows) == 1
        assert (
            rows[0]["raw_frame_sha256"]
            == expected
        )

        c.http.close()

        return {
            "sha256": expected,
        }


test(
    "raw frame SHA256 is exact",
    t_raw_sha,
)


# ----------------------------------------------------------------------
# 4. Frame sequence monotonic/reset per connection
# ----------------------------------------------------------------------

def t_frame_sequence():
    with tempfile.TemporaryDirectory() as td:
        writer = ws.M2eEvidenceWriter(
            "frame-sequence",
            root=Path(td),
        )

        writer.start_connection(
            "conn-A"
        )

        a1, _ = writer.write_raw_frame(
            connection_id="conn-A",
            capture_time=FIXED_NOW.isoformat(),
            raw="frame-A1",
        )

        a2, _ = writer.write_raw_frame(
            connection_id="conn-A",
            capture_time=FIXED_NOW.isoformat(),
            raw="frame-A2",
        )

        writer.start_connection(
            "conn-B"
        )

        b1, _ = writer.write_raw_frame(
            connection_id="conn-B",
            capture_time=FIXED_NOW.isoformat(),
            raw="frame-B1",
        )

        writer.flush()

        assert [a1, a2, b1] == [
            1,
            2,
            1,
        ]

        return {
            "conn_A": [
                a1,
                a2,
            ],
            "conn_B": [
                b1,
            ],
        }


test(
    "frame_sequence is monotonic per connection",
    t_frame_sequence,
)


# ----------------------------------------------------------------------
# 5. Single price_change provenance
# ----------------------------------------------------------------------

def t_single_price_change():
    with tempfile.TemporaryDirectory() as td:
        c, buf = make_collector(
            Path(td),
            capture_id="single-price",
        )

        c._begin_connection(
            "conn-price"
        )

        raw = price_change_raw()

        expected_sha = hashlib.sha256(
            raw.encode("utf-8")
        ).hexdigest()

        c.handle(raw)
        c.evidence.flush()

        evidence = jsonl(
            c.evidence.normalized_path
        )

        assert len(buf.records) == 1
        assert len(evidence) == 1

        row = evidence[0]

        assert row["kind"] == "price_change"
        assert row["token"] == "tok1"
        assert row["ingest_sequence"] == 1
        assert row["change_index"] == 0
        assert row["raw_frame_sequence"] == 1
        assert (
            row["raw_frame_sha256"]
            == expected_sha
        )

        c.http.close()

        return {
            "normalized_records": 1,
            "frame_sequence": 1,
            "ingest_sequence": 1,
            "change_index": 0,
        }


test(
    "single price_change maps to correct provenance",
    t_single_price_change,
)


# ----------------------------------------------------------------------
# 6. Multi-change provenance
# ----------------------------------------------------------------------

def t_multi_change():
    with tempfile.TemporaryDirectory() as td:
        c, buf = make_collector(
            Path(td),
            capture_id="multi-price",
        )

        c._begin_connection(
            "conn-multi"
        )

        raw = multi_change_raw()

        expected_sha = hashlib.sha256(
            raw.encode("utf-8")
        ).hexdigest()

        c.handle(raw)
        c.evidence.flush()

        raw_rows = jsonl(
            c.evidence.raw_path
        )

        evidence = jsonl(
            c.evidence.normalized_path
        )

        assert len(raw_rows) == 1
        assert len(buf.records) == 2
        assert len(evidence) == 2

        assert [
            r["change_index"]
            for r in evidence
        ] == [
            0,
            1,
        ]

        assert {
            r["raw_frame_sequence"]
            for r in evidence
        } == {
            1,
        }

        assert {
            r["raw_frame_sha256"]
            for r in evidence
        } == {
            expected_sha,
        }

        assert {
            r["ingest_sequence"]
            for r in evidence
        } == {
            1,
        }

        c.http.close()

        return {
            "raw_frames": 1,
            "normalized_records": 2,
            "change_indexes": [
                0,
                1,
            ],
        }


test(
    "multi-change price_change shares frame provenance and retains change_index",
    t_multi_change,
)


# ----------------------------------------------------------------------
# 7. ws_book provenance
# ----------------------------------------------------------------------

def t_book():
    with tempfile.TemporaryDirectory() as td:
        c, _ = make_collector(
            Path(td),
            capture_id="book",
        )

        c._begin_connection(
            "conn-book"
        )

        raw = book_raw()

        expected_sha = hashlib.sha256(
            raw.encode("utf-8")
        ).hexdigest()

        c.handle(raw)
        c.evidence.flush()

        evidence = jsonl(
            c.evidence.normalized_path
        )

        assert len(evidence) == 1

        row = evidence[0]

        assert row["kind"] == "ws_book"
        assert row["raw_frame_sequence"] == 1
        assert (
            row["raw_frame_sha256"]
            == expected_sha
        )

        c.http.close()

        return {
            "kind": "ws_book",
            "frame_sequence": 1,
        }


test(
    "ws_book maps to correct provenance",
    t_book,
)


# ----------------------------------------------------------------------
# 8. last_trade_price provenance
# ----------------------------------------------------------------------

def t_trade():
    with tempfile.TemporaryDirectory() as td:
        c, _ = make_collector(
            Path(td),
            capture_id="trade",
        )

        c._begin_connection(
            "conn-trade"
        )

        raw = trade_raw()

        expected_sha = hashlib.sha256(
            raw.encode("utf-8")
        ).hexdigest()

        c.handle(raw)
        c.evidence.flush()

        evidence = jsonl(
            c.evidence.normalized_path
        )

        assert len(evidence) == 1

        row = evidence[0]

        assert (
            row["kind"]
            == "last_trade_price"
        )

        assert row["raw_frame_sequence"] == 1

        assert (
            row["raw_frame_sha256"]
            == expected_sha
        )

        c.http.close()

        return {
            "kind": "last_trade_price",
            "frame_sequence": 1,
        }


test(
    "last_trade_price maps to correct provenance",
    t_trade,
)


# ----------------------------------------------------------------------
# 9. REST resync has null raw provenance
# ----------------------------------------------------------------------

def t_rest_null():
    with tempfile.TemporaryDirectory() as td:
        c, buf = make_collector(
            Path(td),
            capture_id="rest",
        )

        c._begin_connection(
            "conn-rest"
        )

        c.tokens = [
            "tok1",
        ]

        c.rest_books = lambda tokens: [
            {
                "asset_id": "tok1",
                "timestamp": "1776096000400",
                "bids": [
                    {
                        "price": "0.40",
                        "size": "4",
                    }
                ],
                "asks": [
                    {
                        "price": "0.44",
                        "size": "5",
                    }
                ],
                "hash": "rest-hash",
            }
        ]

        c.resync_all(
            "synthetic"
        )

        c.evidence.flush()

        raw_rows = jsonl(
            c.evidence.raw_path
        )

        evidence = jsonl(
            c.evidence.normalized_path
        )

        assert len(buf.records) == 1
        assert len(raw_rows) == 0
        assert len(evidence) == 1

        row = evidence[0]

        assert (
            row["kind"]
            == "rest_resync_book"
        )

        assert (
            row["raw_frame_sequence"]
            is None
        )

        assert (
            row["raw_frame_sha256"]
            is None
        )

        c.http.close()

        return {
            "raw_frames": 0,
            "normalized_rest_records": 1,
            "raw_provenance": None,
        }


test(
    "REST resync evidence has null raw-frame provenance",
    t_rest_null,
)


# ----------------------------------------------------------------------
# 10. Production buffer record is exactly unchanged
# ----------------------------------------------------------------------

def t_production_unchanged():
    raw = multi_change_raw()

    with tempfile.TemporaryDirectory() as td:
        root = Path(td)

        disabled, buf_disabled = (
            make_collector(
                root / "disabled",
                evidence=False,
            )
        )

        enabled, buf_enabled = (
            make_collector(
                root / "enabled",
                evidence=True,
                capture_id="enabled",
            )
        )

        disabled._begin_connection(
            "conn-production"
        )

        enabled._begin_connection(
            "conn-production"
        )

        disabled.handle(raw)
        enabled.handle(raw)

        assert (
            buf_enabled.records
            == buf_disabled.records
        )

        expected_keys = {
            "kind",
            "token",
            "capture_time",
            "connection_id",
            "ingest_sequence",
            "change_index",
            "book_generation",
            "watchlist_rule",
            "payload",
        }

        assert len(
            buf_enabled.records
        ) == 2

        for row in buf_enabled.records:
            assert set(row) == expected_keys
            assert (
                "raw_frame_sequence"
                not in row
            )
            assert (
                "raw_frame_sha256"
                not in row
            )

        enabled.http.close()
        disabled.http.close()

        return {
            "records_compared": 2,
            "exact_equal": True,
            "production_keys": sorted(
                expected_keys
            ),
        }


test(
    "normal production buffer record remains unchanged by instrumentation",
    t_production_unchanged,
)


all_passed = all(
    r["pass"]
    for r in results
)

summary = {
    "version": "H2-v2-M2e-VALIDATION-1",
    "milestone": (
        "H2-v2 M2e — immutable WS evidence instrumentation"
    ),
    "synthetic_tests_only": True,
    "contract_sha256": sha256_file(
        CONTRACT_PATH
    ),
    "collector_sha256": sha256_file(
        COLLECTOR_PATH
    ),
    "validator_sha256": sha256_file(
        VALIDATOR_PATH
    ),
    "tests_required": 10,
    "tests_passed": sum(
        1
        for r in results
        if r["pass"]
    ),
    "tests_failed": sum(
        1
        for r in results
        if not r["pass"]
    ),
    "all_tests_passed": all_passed,
    "performance_data_inspected": False,
    "database_schema_changed": False,
    "live_collector_restarted": False,
    "results": results,
}

OUTPUT_PATH.write_text(
    json.dumps(
        summary,
        indent=2,
        sort_keys=True,
    )
    + "\n",
    encoding="utf-8",
)

print()
print("=== H2-v2 M2e VALIDATION ===")
print(
    "tests:",
    f"{summary['tests_passed']}"
    f"/{summary['tests_required']}",
)

print(
    "overall:",
    "PASS"
    if all_passed
    else "FAIL",
)

print(
    "NO EDGE / NO TRADES / NO PNL"
)

if not all_passed:
    raise SystemExit(1)
