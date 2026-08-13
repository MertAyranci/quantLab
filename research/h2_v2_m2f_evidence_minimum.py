#!/usr/bin/env python3

import hashlib
import json
from collections import defaultdict
from datetime import datetime, timezone
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

MIN_PAIRS = 50
MIN_TOKENS = 4
INITIAL_MINUTES = 30
MAX_MINUTES = 60


def read_jsonl(path):
    with path.open(
        "r",
        encoding="utf-8",
    ) as fh:
        return [
            json.loads(line)
            for line in fh
            if line.strip()
        ]


manifest = json.loads(
    MANIFEST.read_text(
        encoding="utf-8"
    )
)

created = datetime.fromisoformat(
    manifest["created_at"]
)

now = datetime.now(
    timezone.utc
)

elapsed_minutes = (
    now - created
).total_seconds() / 60

print("=== M2f FROZEN EVIDENCE-MINIMUM CHECK ===")
print("capture_id:", CAPTURE_ID)
print("capture_start:", created.isoformat())
print("check_time:", now.isoformat())
print(
    "elapsed_minutes:",
    f"{elapsed_minutes:.3f}",
)

if elapsed_minutes < INITIAL_MINUTES:
    raise SystemExit(
        "REFUSING EARLY CHECK: "
        "frozen 30-minute capture window has not elapsed"
    )


# ------------------------------------------------------------------
# Raw-frame integrity map.
# No event interpretation is performed here.
# ------------------------------------------------------------------

raw_rows = read_jsonl(
    RAW
)

raw_map = {}
raw_hash_failures = 0

for r in raw_rows:
    key = (
        r["connection_id"],
        r["frame_sequence"],
    )

    recomputed = hashlib.sha256(
        r["raw"].encode("utf-8")
    ).hexdigest()

    valid = (
        recomputed
        == r["raw_frame_sha256"]
    )

    if not valid:
        raw_hash_failures += 1

    raw_map[key] = {
        "sha256": r["raw_frame_sha256"],
        "valid": valid,
    }


# ------------------------------------------------------------------
# Load normalized H2-v2 evidence in immutable file order.
# ------------------------------------------------------------------

rows = read_jsonl(
    NORMALIZED
)

by_token = defaultdict(list)

for file_index, row in enumerate(rows):
    if row.get("watchlist_rule") != "h2_v2":
        continue

    token = row.get("token")

    if token:
        by_token[token].append(
            (
                file_index,
                row,
            )
        )


def raw_provenance_valid(row):
    seq = row.get(
        "raw_frame_sequence"
    )

    digest = row.get(
        "raw_frame_sha256"
    )

    conn = row.get(
        "connection_id"
    )

    if (
        seq is None
        or digest is None
        or conn is None
    ):
        return False

    raw = raw_map.get(
        (
            conn,
            seq,
        )
    )

    if raw is None:
        return False

    return (
        raw["valid"]
        and raw["sha256"] == digest
    )


eligible_pairs = 0
eligible_tokens = set()

candidate_consecutive_ws_pairs = 0
same_connection_generation_pairs = 0
provenance_valid_pairs = 0
delta_bearing_pairs = 0


for token, events in by_token.items():

    ws_positions = [
        i
        for i, (_, row) in enumerate(events)
        if row.get("kind") == "ws_book"
    ]

    for j in range(
        len(ws_positions) - 1
    ):
        left = ws_positions[j]
        right = ws_positions[j + 1]

        base = events[left][1]
        checkpoint = events[right][1]

        candidate_consecutive_ws_pairs += 1

        same_context = (
            base.get("connection_id")
            == checkpoint.get("connection_id")
            and
            base.get("book_generation")
            == checkpoint.get("book_generation")
        )

        if not same_context:
            continue

        same_connection_generation_pairs += 1

        if not (
            raw_provenance_valid(base)
            and raw_provenance_valid(
                checkpoint
            )
        ):
            continue

        provenance_valid_pairs += 1

        intervening = [
            row
            for _, row in events[
                left + 1:right
            ]
        ]

        has_price_change = any(
            row.get("kind")
            == "price_change"
            and row.get("connection_id")
            == base.get("connection_id")
            and row.get("book_generation")
            == base.get("book_generation")
            for row in intervening
        )

        if not has_price_change:
            continue

        delta_bearing_pairs += 1
        eligible_pairs += 1
        eligible_tokens.add(
            token
        )


minimum_met = (
    eligible_pairs >= MIN_PAIRS
    and len(eligible_tokens)
    >= MIN_TOKENS
    and raw_hash_failures == 0
)


if minimum_met:
    disposition = "STOP_CAPTURE"

elif elapsed_minutes < MAX_MINUTES:
    disposition = "CONTINUE_TO_60_MINUTES"

else:
    disposition = "INSUFFICIENT_FRESH_EVIDENCE"


print()
print("raw_frames:", len(raw_rows))
print(
    "raw_hash_failures:",
    raw_hash_failures,
)
print(
    "normalized_h2_v2_records:",
    len(rows),
)
print(
    "consecutive_ws_book_pairs:",
    candidate_consecutive_ws_pairs,
)
print(
    "same_connection_generation_pairs:",
    same_connection_generation_pairs,
)
print(
    "provenance_valid_pairs:",
    provenance_valid_pairs,
)
print(
    "delta_bearing_pairs:",
    delta_bearing_pairs,
)
print(
    "eligible_pairs:",
    eligible_pairs,
)
print(
    "distinct_eligible_tokens:",
    len(eligible_tokens),
)

print()
print(
    "minimum_required:",
    f">={MIN_PAIRS} pairs, "
    f">={MIN_TOKENS} tokens",
)
print(
    "minimum_met:",
    minimum_met,
)
print(
    "disposition:",
    disposition,
)
print(
    "NO REPLAY / NO EDGE / NO TRADES / NO PNL"
)
