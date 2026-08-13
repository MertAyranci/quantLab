#!/usr/bin/env python3

import hashlib
import json
import os
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]

CAPTURE_ID = Path(
    "/tmp/h2_v2_m2f_capture_id"
).read_text().strip()

EVIDENCE_DIR = (
    REPO
    / "data/evidence/h2_v2_m2e"
    / CAPTURE_ID
)

MANIFEST = EVIDENCE_DIR / "manifest.json"
RAW = EVIDENCE_DIR / "raw_frames.jsonl"
NORMALIZED = EVIDENCE_DIR / "normalized_h2_v2.jsonl"

OUTPUT = (
    REPO
    / "research/h2_v2_m2f_evidence_minimum_result.json"
)

MIN_PAIRS = 50
MIN_TOKENS = 4

EXPECTED_CAPTURE_HASHES = {
    "manifest": "57f4c1a8f175a51c425a13972a7a6645561ad442460dc34702556ef7dadcff25",
    "raw_frames": "f682edd032854aa52f8480faac5291204e3215bffd2c772bdc558a8694f6319d",
    "normalized_h2_v2": "728a9bca260fdb88ca9808a07fd7a3d049608aa9a2f910430cb57b6e0ec9eb82",
}

ORIGINAL_CHECKER_SHA256 = (
    "8f83f47e0f7df491aeec68a03f71c9456ef07ad39b6e28e7ae901ad36e0a34fc"
)


def dt(value):
    return datetime.fromisoformat(value)


manifest = json.loads(
    MANIFEST.read_text(
        encoding="utf-8"
    )
)

if manifest["capture_id"] != CAPTURE_ID:
    raise SystemExit(
        "capture_id mismatch"
    )

start = dt(
    manifest["created_at"]
)

cutoff_30 = start + timedelta(
    minutes=30
)

cutoff_60 = start + timedelta(
    minutes=60
)


# ----------------------------------------------------------------------
# Temporary disk-backed raw provenance index.
# ----------------------------------------------------------------------

fd, db_name = tempfile.mkstemp(
    prefix="h2_v2_m2f_provenance_",
    suffix=".sqlite3",
)

os.close(fd)

db_path = Path(db_name)

con = sqlite3.connect(
    db_path
)

con.execute(
    "PRAGMA journal_mode=OFF"
)

con.execute(
    "PRAGMA synchronous=OFF"
)

con.execute(
    "PRAGMA temp_store=FILE"
)

con.execute(
    "PRAGMA cache_size=-65536"
)

con.execute(
    """
    CREATE TABLE raw_provenance (
        connection_id TEXT NOT NULL,
        frame_sequence INTEGER NOT NULL,
        raw_frame_sha256 TEXT NOT NULL,
        valid INTEGER NOT NULL,
        PRIMARY KEY (
            connection_id,
            frame_sequence
        )
    ) WITHOUT ROWID
    """
)


raw_counts = {
    "30": 0,
    "60": 0,
    "after_60": 0,
}

raw_hash_failures = {
    "30": 0,
    "60": 0,
}

raw_batch = []


print(
    "=== BUILD DISK-BACKED RAW PROVENANCE INDEX ===",
    flush=True,
)

with RAW.open(
    "r",
    encoding="utf-8",
) as fh:

    for n, line in enumerate(
        fh,
        start=1,
    ):
        if not line.strip():
            continue

        r = json.loads(line)

        capture_time = dt(
            r["capture_time"]
        )

        if capture_time > cutoff_60:
            raw_counts[
                "after_60"
            ] += 1
            continue

        digest = hashlib.sha256(
            r["raw"].encode("utf-8")
        ).hexdigest()

        valid = (
            digest
            == r["raw_frame_sha256"]
        )

        raw_counts["60"] += 1

        if not valid:
            raw_hash_failures[
                "60"
            ] += 1

        if capture_time <= cutoff_30:
            raw_counts[
                "30"
            ] += 1

            if not valid:
                raw_hash_failures[
                    "30"
                ] += 1

        raw_batch.append(
            (
                r["connection_id"],
                int(r["frame_sequence"]),
                r["raw_frame_sha256"],
                int(valid),
            )
        )

        if len(raw_batch) >= 5000:
            con.executemany(
                """
                INSERT INTO raw_provenance
                VALUES (?, ?, ?, ?)
                """,
                raw_batch,
            )

            con.commit()
            raw_batch.clear()

        if n % 100000 == 0:
            print(
                f"raw rows scanned: {n}",
                flush=True,
            )


if raw_batch:
    con.executemany(
        """
        INSERT INTO raw_provenance
        VALUES (?, ?, ?, ?)
        """,
        raw_batch,
    )

    con.commit()


def provenance_valid(row):
    conn = row.get(
        "connection_id"
    )

    seq = row.get(
        "raw_frame_sequence"
    )

    sha = row.get(
        "raw_frame_sha256"
    )

    if (
        conn is None
        or seq is None
        or sha is None
    ):
        return False

    found = con.execute(
        """
        SELECT
            raw_frame_sha256,
            valid
        FROM raw_provenance
        WHERE
            connection_id = ?
            AND frame_sequence = ?
        """,
        (
            conn,
            int(seq),
        ),
    ).fetchone()

    if found is None:
        return False

    return (
        bool(found[1])
        and found[0] == sha
    )


class WindowState:

    def __init__(
        self,
        label,
    ):
        self.label = label

        self.normalized_records = 0

        self.consecutive_ws_book_pairs = 0
        self.same_connection_generation_pairs = 0
        self.provenance_valid_pairs = 0
        self.delta_bearing_pairs = 0
        self.eligible_pairs = 0

        self.eligible_tokens = set()

        self.last_ws = {}
        self.price_change_since_ws = {}

    def ingest(
        self,
        row,
    ):
        if (
            row.get("watchlist_rule")
            != "h2_v2"
        ):
            return

        self.normalized_records += 1

        token = row.get(
            "token"
        )

        if not token:
            return

        kind = row.get(
            "kind"
        )

        if kind == "price_change":

            base = self.last_ws.get(
                token
            )

            if base is None:
                return

            same_context = (
                row.get("connection_id")
                == base["connection_id"]
                and
                row.get("book_generation")
                == base["book_generation"]
            )

            if same_context:
                self.price_change_since_ws[
                    token
                ] = True

            return

        if kind != "ws_book":
            return

        current = {
            "connection_id": row.get(
                "connection_id"
            ),
            "book_generation": row.get(
                "book_generation"
            ),
            "provenance_valid": provenance_valid(
                row
            ),
        }

        base = self.last_ws.get(
            token
        )

        if base is not None:

            self.consecutive_ws_book_pairs += 1

            same_context = (
                base["connection_id"]
                == current["connection_id"]
                and
                base["book_generation"]
                == current["book_generation"]
            )

            if same_context:

                self.same_connection_generation_pairs += 1

                if (
                    base["provenance_valid"]
                    and current[
                        "provenance_valid"
                    ]
                ):

                    self.provenance_valid_pairs += 1

                    if self.price_change_since_ws.get(
                        token,
                        False,
                    ):

                        self.delta_bearing_pairs += 1
                        self.eligible_pairs += 1

                        self.eligible_tokens.add(
                            token
                        )

        self.last_ws[
            token
        ] = current

        self.price_change_since_ws[
            token
        ] = False

    def result(
        self,
        raw_frames,
        raw_failures,
    ):
        minimum_met = (
            self.eligible_pairs
            >= MIN_PAIRS
            and
            len(self.eligible_tokens)
            >= MIN_TOKENS
            and
            raw_failures == 0
        )

        return {
            "window_minutes": int(
                self.label
            ),
            "raw_frames": raw_frames,
            "raw_hash_failures": raw_failures,
            "normalized_h2_v2_records": self.normalized_records,
            "consecutive_ws_book_pairs": self.consecutive_ws_book_pairs,
            "same_connection_generation_pairs": (
                self.same_connection_generation_pairs
            ),
            "provenance_valid_pairs": self.provenance_valid_pairs,
            "delta_bearing_pairs": self.delta_bearing_pairs,
            "eligible_pairs": self.eligible_pairs,
            "distinct_eligible_tokens": len(
                self.eligible_tokens
            ),
            "minimum_pairs_required": MIN_PAIRS,
            "minimum_tokens_required": MIN_TOKENS,
            "minimum_met": minimum_met,
        }


state_30 = WindowState(
    "30"
)

state_60 = WindowState(
    "60"
)

normalized_after_60 = 0


print()
print(
    "=== STREAM NORMALIZED H2-v2 EVIDENCE ===",
    flush=True,
)

with NORMALIZED.open(
    "r",
    encoding="utf-8",
) as fh:

    for n, line in enumerate(
        fh,
        start=1,
    ):
        if not line.strip():
            continue

        row = json.loads(
            line
        )

        capture_time = dt(
            row["capture_time"]
        )

        if capture_time <= cutoff_30:
            state_30.ingest(
                row
            )

        if capture_time <= cutoff_60:
            state_60.ingest(
                row
            )
        else:
            normalized_after_60 += 1

        if n % 100000 == 0:
            print(
                f"normalized rows scanned: {n}",
                flush=True,
            )


result_30 = state_30.result(
    raw_counts["30"],
    raw_hash_failures["30"],
)

result_60 = state_60.result(
    raw_counts["60"],
    raw_hash_failures["60"],
)


if result_30["minimum_met"]:
    disposition = "STOP_AT_30_MINUTES"
    frozen_window_minutes = 30

elif result_60["minimum_met"]:
    disposition = "STOP_AT_60_MINUTES"
    frozen_window_minutes = 60

else:
    disposition = (
        "INSUFFICIENT_FRESH_EVIDENCE"
    )

    frozen_window_minutes = None


mtime_overrun_seconds = max(
    0.0,
    max(
        RAW.stat().st_mtime,
        NORMALIZED.stat().st_mtime,
    )
    - cutoff_60.timestamp(),
)


summary = {
    "version": (
        "H2-v2-M2f-EVIDENCE-MINIMUM-STREAMING-1"
    ),

    "capture_id": CAPTURE_ID,

    "capture_start": start.isoformat(),

    "cutoff_30": cutoff_30.isoformat(),

    "cutoff_60": cutoff_60.isoformat(),

    "frozen_thresholds_unchanged": True,

    "original_checker_sha256": (
        ORIGINAL_CHECKER_SHA256
    ),

    "original_checker_runtime_result": (
        "OOM_KILLED_AT_54.697_MINUTES"
    ),

    "capture_hashes": (
        EXPECTED_CAPTURE_HASHES
    ),

    "window_30": result_30,

    "window_60": result_60,

    "rows_after_frozen_60_minute_boundary": {
        "raw_frames": raw_counts[
            "after_60"
        ],
        "normalized_h2_v2": normalized_after_60,
        "used_for_eligibility": False,
    },

    "protocol_deviation": {
        "instrumented_capture_exceeded_60_minutes": (
            mtime_overrun_seconds > 0
        ),
        "approx_evidence_file_mtime_overrun_seconds": (
            mtime_overrun_seconds
        ),
        "reason": (
            "sudo authentication retry delayed service stop"
        ),
        "records_after_60_minutes_excluded": True,
    },

    "disposition": disposition,

    "frozen_analysis_window_minutes": (
        frozen_window_minutes
    ),

    "performance_data_inspected": False,

    "replay_performed": False,

    "edge_calculated": False,

    "trades_simulated": False,

    "pnl_calculated": False,
}


OUTPUT.write_text(
    json.dumps(
        summary,
        indent=2,
        sort_keys=True,
    )
    + "\n",
    encoding="utf-8",
)


print()
print(
    "=== M2f STREAMING EVIDENCE-MINIMUM RESULT ==="
)

for r in (
    result_30,
    result_60,
):
    print()
    print(
        f"WINDOW {r['window_minutes']} MINUTES"
    )

    print(
        "raw_frames:",
        r["raw_frames"],
    )

    print(
        "raw_hash_failures:",
        r["raw_hash_failures"],
    )

    print(
        "normalized_h2_v2_records:",
        r[
            "normalized_h2_v2_records"
        ],
    )

    print(
        "consecutive_ws_book_pairs:",
        r[
            "consecutive_ws_book_pairs"
        ],
    )

    print(
        "same_connection_generation_pairs:",
        r[
            "same_connection_generation_pairs"
        ],
    )

    print(
        "provenance_valid_pairs:",
        r[
            "provenance_valid_pairs"
        ],
    )

    print(
        "delta_bearing_pairs:",
        r[
            "delta_bearing_pairs"
        ],
    )

    print(
        "eligible_pairs:",
        r[
            "eligible_pairs"
        ],
    )

    print(
        "distinct_eligible_tokens:",
        r[
            "distinct_eligible_tokens"
        ],
    )

    print(
        "minimum_met:",
        r[
            "minimum_met"
        ],
    )


print()
print(
    "raw_rows_after_60:",
    raw_counts["after_60"],
)

print(
    "normalized_rows_after_60:",
    normalized_after_60,
)

print(
    "approx_overrun_seconds:",
    f"{mtime_overrun_seconds:.3f}",
)

print(
    "disposition:",
    disposition,
)

print(
    "frozen_analysis_window_minutes:",
    frozen_window_minutes,
)

print(
    "NO REPLAY / NO EDGE / NO TRADES / NO PNL"
)


con.close()

try:
    db_path.unlink()
except FileNotFoundError:
    pass
