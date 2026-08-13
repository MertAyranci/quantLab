#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
R = REPO / "research"

CONTRACT = R / "h2_v2_m2g_contract.json"
M2F_FREEZE = R / "h2_v2_m2f_analysis_freeze.json"
M2F_PAIRS = R / "h2_v2_m2f_pair_diagnostics.jsonl"
M2F_MISMATCHES = R / "h2_v2_m2f_mismatch_diagnostics.jsonl"

PAIR_OUT = R / "h2_v2_m2g_pair_diagnostics.jsonl"
LEVEL_OUT = R / "h2_v2_m2g_level_diagnostics.jsonl"
SUMMARY_OUT = R / "h2_v2_m2g_summary.json"

CAPTURE_ID = "h2_v2_m2f_20260813T170210Z"

EVIDENCE_DIR = (
    REPO
    / "data/evidence/h2_v2_m2e"
    / CAPTURE_ID
)

MANIFEST = EVIDENCE_DIR / "manifest.json"
RAW = EVIDENCE_DIR / "raw_frames.jsonl"
NORMALIZED = EVIDENCE_DIR / "normalized_h2_v2.jsonl"


EXPECTED_PARENT = (
    "43aa0ecf30b7ce30a3a64e449ef7ccd862df94da"
)

EXPECTED_HASHES = {
    "contract": (
        "ecab36dfe465b4a32486b2601a9c3356"
        "377e017cf4acb5e8973c994ee74e5f5c"
    ),
    "m2f_contract": (
        "7f97e5ec6c7b4268123d176bb6646427"
        "53955e0be049b025bdeda3700fe89115"
    ),
    "m2f_analysis_freeze": (
        "e2d472b938b5bdeda44faff880b33a05"
        "8926d84399144f29f6377d8b6b342e93"
    ),
    "m2f_diagnostic_source": (
        "4765245376aff83586e30ca829d843ec"
        "fa150b1e1111fb94a6aa0f9321dd8933"
    ),
    "m2f_pairs": (
        "e3a8d9bb0a8be854a6d901d332a2e9a0"
        "3e50dabc3ce7ca3fcadac1ad6f233f9b"
    ),
    "m2f_mismatches": (
        "5b832360ead66ea024951d7bb35c02308"
        "f5e9559d2c2caa45a2538d54ce596f9"
    ),
    "m2f_summary": (
        "1c4de294b462075aea0a4c5d7b89526"
        "feaf7849661344d3d64208c71da01df59"
    ),
    "m2f_conclusion": (
        "3fc2579987f761903326ef00eec13de93"
        "0e855ed5336f83061998c65ce61884e"
    ),
    "capture_manifest": (
        "57f4c1a8f175a51c425a13972a7a6645"
        "561ad442460dc34702556ef7dadcff25"
    ),
    "capture_raw": (
        "f682edd032854aa52f8480faac5291204"
        "e3215bffd2c772bdc558a8694f6319d"
    ),
    "capture_normalized": (
        "728a9bca260fdb88ca9808a07fd7a3d0"
        "49608aa9a2f910430cb57b6e0ec9eb82"
    ),
}

EXPECTED_PAIRS = 124

EPOCH = datetime(
    1970, 1, 1,
    tzinfo=timezone.utc,
)


def sha_file(path: Path) -> str:
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def parse_dt(x):
    if isinstance(x, datetime):
        return x

    return datetime.fromisoformat(
        str(x).replace(
            "Z",
            "+00:00",
        )
    )


def dt_us(x) -> int:
    d = parse_dt(x)
    delta = d - EPOCH

    return (
        delta.days
        * 86400
        * 1_000_000
        + delta.seconds
        * 1_000_000
        + delta.microseconds
    )


def mc(x):
    if x is None or x == "":
        return None

    try:
        return int(
            Decimal(str(x))
            * 1000
        )
    except (
        InvalidOperation,
        ValueError,
        TypeError,
    ):
        return None


def dec(x):
    if x is None:
        return None

    try:
        return Decimal(str(x))
    except (
        InvalidOperation,
        ValueError,
        TypeError,
    ):
        return None


def clean(x):
    if isinstance(x, Decimal):
        return str(x)

    if isinstance(x, datetime):
        return x.isoformat()

    if isinstance(x, dict):
        return {
            str(k): clean(v)
            for k, v in x.items()
        }

    if isinstance(x, (list, tuple)):
        return [
            clean(v)
            for v in x
        ]

    return x


# ---------------------------------------------------------------------
# Frozen input verification
# ---------------------------------------------------------------------

checks = {
    "contract":
        CONTRACT,

    "m2f_contract":
        R / "h2_v2_m2f_contract.json",

    "m2f_analysis_freeze":
        M2F_FREEZE,

    "m2f_diagnostic_source":
        R / "h2_v2_m2f_diagnose.py",

    "m2f_pairs":
        M2F_PAIRS,

    "m2f_mismatches":
        M2F_MISMATCHES,

    "m2f_summary":
        R / "h2_v2_m2f_summary.json",

    "m2f_conclusion":
        R / "h2_v2_m2f_conclusion.json",

    "capture_manifest":
        MANIFEST,

    "capture_raw":
        RAW,

    "capture_normalized":
        NORMALIZED,
}

for name, path in checks.items():
    actual = sha_file(path)

    if actual != EXPECTED_HASHES[name]:
        raise SystemExit(
            "REFUSING M2g: "
            f"{name} hash changed: "
            f"{actual}"
        )


head = subprocess.check_output(
    [
        "git",
        "-C",
        str(REPO),
        "rev-parse",
        "HEAD",
    ],
    text=True,
).strip()

if head != EXPECTED_PARENT:
    raise SystemExit(
        "REFUSING M2g: parent HEAD "
        f"is {head}, expected "
        f"{EXPECTED_PARENT}"
    )


freeze = json.loads(
    M2F_FREEZE.read_text()
)

if (
    freeze["capture_id"]
    != CAPTURE_ID
    or
    freeze["analysis_window"]["minutes"]
    != 30
):
    raise SystemExit(
        "REFUSING M2g: M2f analysis "
        "window/capture mismatch"
    )


START = parse_dt(
    freeze[
        "analysis_window"
    ]["start_inclusive"]
)

END = parse_dt(
    freeze[
        "analysis_window"
    ]["end_inclusive"]
)

END_US = dt_us(END)


# ---------------------------------------------------------------------
# Load frozen 124-pair population
# ---------------------------------------------------------------------

pairs = []

with M2F_PAIRS.open(
    encoding="utf-8",
) as fh:
    for line in fh:
        if not line.strip():
            continue

        row = json.loads(line)

        if (
            row.get("classification")
            == "UNEXPLAINED_SOURCE_TRANSITION"
        ):
            pairs.append(row)


if len(pairs) != EXPECTED_PAIRS:
    raise RuntimeError(
        "M2g frozen population "
        f"expected {EXPECTED_PAIRS}, "
        f"got {len(pairs)}"
    )


pair_by_index = {
    int(p["pair_index"]): p
    for p in pairs
}

if len(pair_by_index) != EXPECTED_PAIRS:
    raise RuntimeError(
        "duplicate pair_index in "
        "M2g population"
    )


mismatches = defaultdict(list)

with M2F_MISMATCHES.open(
    encoding="utf-8",
) as fh:
    for line in fh:
        if not line.strip():
            continue

        row = json.loads(line)

        idx = int(
            row["pair_index"]
        )

        if idx in pair_by_index:
            mismatches[idx].append(
                row
            )


for idx in pair_by_index:
    if not mismatches[idx]:
        raise RuntimeError(
            f"pair {idx} has no frozen "
            "M2f mismatch levels"
        )


# ---------------------------------------------------------------------
# Reconstruct the M2f normalized row IDs for pair boundaries.
#
# M2f assigned norm_id sequentially to every H2-v2 normalized record
# within the frozen 30-minute window. Reproduce exactly; do not inspect
# pair contents manually.
# ---------------------------------------------------------------------

needed_norm_ids = set()

for p in pairs:
    needed_norm_ids.add(
        int(p["base_norm_id"])
    )
    needed_norm_ids.add(
        int(p["checkpoint_norm_id"])
    )


boundaries = {}

norm_id = 0

print(
    "=== M2g BOUNDARY RECONSTRUCTION ===",
    flush=True,
)

with NORMALIZED.open(
    encoding="utf-8",
) as fh:

    for line_no, line in enumerate(
        fh,
        start=1,
    ):
        if not line.strip():
            continue

        row = json.loads(line)

        cap_us = dt_us(
            row["capture_time"]
        )

        if cap_us > END_US:
            continue

        if (
            row.get("watchlist_rule")
            != "h2_v2"
        ):
            continue

        norm_id += 1

        if norm_id in needed_norm_ids:
            boundaries[norm_id] = row

        if line_no % 100000 == 0:
            print(
                "normalized lines scanned:",
                line_no,
                flush=True,
            )


missing_ids = (
    needed_norm_ids
    - set(boundaries)
)

if missing_ids:
    raise RuntimeError(
        "missing frozen boundary "
        f"norm ids: {sorted(missing_ids)[:10]}"
    )


# Determine candidate markets solely from frozen pair boundaries.
relevant_markets = set()

for p in pairs:
    b = boundaries[
        int(p["base_norm_id"])
    ]

    c = boundaries[
        int(p["checkpoint_norm_id"])
    ]

    bm = (
        b.get("payload")
        or {}
    ).get("market")

    cm = (
        c.get("payload")
        or {}
    ).get("market")

    if bm:
        relevant_markets.add(
            str(bm)
        )

    if cm:
        relevant_markets.add(
            str(cm)
        )


# ---------------------------------------------------------------------
# Disk-backed exact raw-frame + trade-event index.
# ---------------------------------------------------------------------

fd, tmp_name = tempfile.mkstemp(
    prefix="h2_v2_m2g_",
    suffix=".sqlite3",
)

os.close(fd)

TMP_DB = Path(tmp_name)

db = sqlite3.connect(
    TMP_DB
)

db.execute(
    "PRAGMA journal_mode=OFF"
)

db.execute(
    "PRAGMA synchronous=OFF"
)

db.execute(
    "PRAGMA temp_store=FILE"
)

db.execute(
    "PRAGMA cache_size=-65536"
)

db.executescript(
    """
    CREATE TABLE raw_frames (
        connection_id TEXT NOT NULL,
        frame_sequence INTEGER NOT NULL,
        raw_sha TEXT NOT NULL,
        hash_valid INTEGER NOT NULL,
        PRIMARY KEY (
            connection_id,
            frame_sequence
        )
    ) WITHOUT ROWID;

    CREATE TABLE trades (
        id INTEGER PRIMARY KEY,
        connection_id TEXT NOT NULL,
        frame_sequence INTEGER NOT NULL,
        msg_index INTEGER NOT NULL,
        raw_sha TEXT NOT NULL,

        market TEXT NOT NULL,
        token TEXT,

        event_ms INTEGER,
        price_mc INTEGER,
        size_text TEXT,
        side TEXT
    );

    CREATE INDEX trades_market_idx
    ON trades (
        market
    );
    """
)


raw_frames = 0
raw_hash_failures = 0
trade_events_indexed = 0

frame_batch = []
trade_batch = []

trade_id = 0


print()
print(
    "=== M2g RAW TRADE INDEX ===",
    flush=True,
)


with RAW.open(
    encoding="utf-8",
) as fh:

    for line_no, line in enumerate(
        fh,
        start=1,
    ):
        if not line.strip():
            continue

        r = json.loads(line)

        if (
            dt_us(r["capture_time"])
            > END_US
        ):
            continue

        raw_frames += 1

        raw_text = r["raw"]

        digest = hashlib.sha256(
            raw_text.encode("utf-8")
        ).hexdigest()

        valid = (
            digest
            == r["raw_frame_sha256"]
        )

        if not valid:
            raw_hash_failures += 1

        conn = str(
            r["connection_id"]
        )

        frame_seq = int(
            r["frame_sequence"]
        )

        frame_batch.append(
            (
                conn,
                frame_seq,
                r["raw_frame_sha256"],
                int(valid),
            )
        )

        parsed = None

        if raw_text != "PONG":
            try:
                parsed = json.loads(
                    raw_text
                )
            except json.JSONDecodeError:
                parsed = None

        if isinstance(parsed, dict):
            messages = [parsed]

        elif isinstance(parsed, list):
            messages = parsed

        else:
            messages = []

        for msg_index, d in enumerate(
            messages
        ):
            if not isinstance(d, dict):
                continue

            if (
                d.get("event_type")
                != "last_trade_price"
            ):
                continue

            market = d.get(
                "market"
            )

            if (
                market is None
                or str(market)
                not in relevant_markets
            ):
                continue

            token = (
                str(
                    d.get("asset_id")
                )
                if d.get("asset_id")
                is not None
                else None
            )

            try:
                event_ms = (
                    int(d["timestamp"])
                    if d.get("timestamp")
                    is not None
                    else None
                )
            except (
                ValueError,
                TypeError,
            ):
                event_ms = None

            trade_id += 1
            trade_events_indexed += 1

            trade_batch.append(
                (
                    trade_id,
                    conn,
                    frame_seq,
                    msg_index,
                    r[
                        "raw_frame_sha256"
                    ],
                    str(market),
                    token,
                    event_ms,
                    mc(
                        d.get("price")
                    ),
                    (
                        str(
                            d.get("size")
                        )
                        if d.get("size")
                        is not None
                        else None
                    ),
                    (
                        str(
                            d.get("side")
                        ).upper()
                        if d.get("side")
                        is not None
                        else None
                    ),
                )
            )

        if len(frame_batch) >= 5000:
            db.executemany(
                """
                INSERT INTO raw_frames
                VALUES (?, ?, ?, ?)
                """,
                frame_batch,
            )
            frame_batch.clear()

        if len(trade_batch) >= 5000:
            db.executemany(
                """
                INSERT INTO trades
                VALUES (
                    ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?
                )
                """,
                trade_batch,
            )
            trade_batch.clear()

        if line_no % 100000 == 0:
            print(
                "raw lines scanned:",
                line_no,
                flush=True,
            )


if frame_batch:
    db.executemany(
        """
        INSERT INTO raw_frames
        VALUES (?, ?, ?, ?)
        """,
        frame_batch,
    )

if trade_batch:
    db.executemany(
        """
        INSERT INTO trades
        VALUES (
            ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?
        )
        """,
        trade_batch,
    )

db.commit()


if raw_hash_failures != 0:
    raise RuntimeError(
        "raw hash integrity changed "
        f"from M2f: {raw_hash_failures}"
    )


# ---------------------------------------------------------------------
# Pair diagnostics
# ---------------------------------------------------------------------

classification_counts = Counter()

mechanism_counts = Counter()

candidate_count_histogram = Counter()

level_total = 0
level_price_aligned = 0
level_accounting_aligned = 0
level_size_covered = 0
level_direct_supported = 0

invalid_reasons = Counter()


PAIR_OUT.unlink(
    missing_ok=True
)

LEVEL_OUT.unlink(
    missing_ok=True
)


def raw_provenance_ok(
    normalized_row,
):
    conn = normalized_row.get(
        "connection_id"
    )

    frame = normalized_row.get(
        "raw_frame_sequence"
    )

    sha = normalized_row.get(
        "raw_frame_sha256"
    )

    if (
        conn is None
        or frame is None
        or sha is None
    ):
        return False

    found = db.execute(
        """
        SELECT
            raw_sha,
            hash_valid

        FROM raw_frames

        WHERE
            connection_id = ?
            AND frame_sequence = ?
        """,
        (
            str(conn),
            int(frame),
        ),
    ).fetchone()

    return (
        found is not None
        and bool(found[1])
        and found[0] == sha
    )


def mismatch_reduction(
    m,
):
    replayed = dec(
        m.get(
            "replayed_size"
        )
    )

    checkpoint = dec(
        m.get(
            "checkpoint_size"
        )
    )

    kind = m.get(
        "mismatch_type"
    )

    if kind == "size":
        if (
            replayed is None
            or checkpoint is None
        ):
            return None

        return (
            replayed
            - checkpoint
        )

    if kind == "presence":
        if replayed is None:
            return None

        return replayed

    return None


def candidate_dict(row):
    (
        tid,
        conn,
        frame_seq,
        msg_index,
        raw_sha,
        market,
        token,
        event_ms,
        price_mc,
        size_text,
        side,
    ) = row

    return {
        "trade_id": int(tid),
        "connection_id": conn,
        "frame_sequence": int(
            frame_seq
        ),
        "msg_index": int(
            msg_index
        ),
        "raw_sha": raw_sha,
        "market": market,
        "token": token,
        "event_ms": event_ms,
        "price_mc": price_mc,
        "size": (
            dec(size_text)
            if size_text is not None
            else None
        ),
        "side": side,
    }


print()
print(
    "=== CLASSIFY 124 FROZEN M2g PAIRS ===",
    flush=True,
)


with PAIR_OUT.open(
    "w",
    encoding="utf-8",
) as pair_fh, LEVEL_OUT.open(
    "w",
    encoding="utf-8",
) as level_fh:

    for n, p in enumerate(
        sorted(
            pairs,
            key=lambda x: int(
                x["pair_index"]
            ),
        ),
        start=1,
    ):
        idx = int(
            p["pair_index"]
        )

        token = str(
            p["token"]
        )

        base = boundaries.get(
            int(
                p["base_norm_id"]
            )
        )

        checkpoint = boundaries.get(
            int(
                p[
                    "checkpoint_norm_id"
                ]
            )
        )

        invalid = []

        if (
            base is None
            or checkpoint is None
        ):
            invalid.append(
                "missing_boundary"
            )

        if not invalid:
            if (
                base.get("kind")
                != "ws_book"
                or
                checkpoint.get("kind")
                != "ws_book"
            ):
                invalid.append(
                    "boundary_not_ws_book"
                )

            if (
                str(base.get("token"))
                != token
                or
                str(
                    checkpoint.get(
                        "token"
                    )
                )
                != token
            ):
                invalid.append(
                    "boundary_token_mismatch"
                )

            if (
                str(
                    base.get(
                        "connection_id"
                    )
                )
                != str(
                    p[
                        "connection_id"
                    ]
                )
                or
                str(
                    checkpoint.get(
                        "connection_id"
                    )
                )
                != str(
                    p[
                        "connection_id"
                    ]
                )
            ):
                invalid.append(
                    "boundary_connection_mismatch"
                )

            if (
                base.get(
                    "book_generation"
                )
                != p[
                    "book_generation"
                ]
                or
                checkpoint.get(
                    "book_generation"
                )
                != p[
                    "book_generation"
                ]
            ):
                invalid.append(
                    "boundary_generation_mismatch"
                )

            if (
                base.get(
                    "ingest_sequence"
                )
                != p[
                    "base_seq"
                ]
                or
                checkpoint.get(
                    "ingest_sequence"
                )
                != p[
                    "checkpoint_seq"
                ]
            ):
                invalid.append(
                    "boundary_ingest_sequence_mismatch"
                )

            if (
                base.get(
                    "raw_frame_sequence"
                )
                != p[
                    "base_frame"
                ]
                or
                checkpoint.get(
                    "raw_frame_sequence"
                )
                != p[
                    "checkpoint_frame"
                ]
            ):
                invalid.append(
                    "boundary_frame_mismatch"
                )


        base_market = None
        checkpoint_market = None
        base_ts = None
        checkpoint_ts = None

        if not invalid:
            bp = (
                base.get(
                    "payload"
                )
                or {}
            )

            cp = (
                checkpoint.get(
                    "payload"
                )
                or {}
            )

            base_market = bp.get(
                "market"
            )

            checkpoint_market = cp.get(
                "market"
            )

            if (
                not base_market
                or
                not checkpoint_market
                or
                str(base_market)
                != str(
                    checkpoint_market
                )
            ):
                invalid.append(
                    "market_identifier_missing_or_inconsistent"
                )

            try:
                base_ts = int(
                    bp["timestamp"]
                )
                checkpoint_ts = int(
                    cp["timestamp"]
                )
            except (
                KeyError,
                ValueError,
                TypeError,
            ):
                invalid.append(
                    "book_timestamp_missing_or_invalid"
                )

            if (
                base_ts is not None
                and
                checkpoint_ts is not None
                and
                base_ts
                > checkpoint_ts
            ):
                invalid.append(
                    "book_timestamp_reversed"
                )

            if not raw_provenance_ok(
                base
            ):
                invalid.append(
                    "base_raw_provenance_invalid"
                )

            if not raw_provenance_ok(
                checkpoint
            ):
                invalid.append(
                    "checkpoint_raw_provenance_invalid"
                )


        pair_levels = mismatches[
            idx
        ]

        if not pair_levels:
            invalid.append(
                "missing_mismatch_levels"
            )


        eligible_candidates = []

        mechanism_flags = {
            "same_token_exact_checkpoint_timestamp":
                False,

            "same_market_different_token_exact_checkpoint_timestamp":
                False,

            "same_token_event_time_interval_out_of_ingest_order":
                False,

            "same_market_different_token_ingest_interval":
                False,

            "same_market_different_token_event_time_interval":
                False,
        }


        if not invalid:
            conn = str(
                p[
                    "connection_id"
                ]
            )

            base_frame = int(
                p[
                    "base_frame"
                ]
            )

            checkpoint_frame = int(
                p[
                    "checkpoint_frame"
                ]
            )

            raw_trades = db.execute(
                """
                SELECT
                    id,
                    connection_id,
                    frame_sequence,
                    msg_index,
                    raw_sha,
                    market,
                    token,
                    event_ms,
                    price_mc,
                    size_text,
                    side

                FROM trades

                WHERE market = ?

                ORDER BY
                    frame_sequence,
                    msg_index,
                    id
                """,
                (
                    str(base_market),
                ),
            ).fetchall()


            for raw_trade in raw_trades:
                c = candidate_dict(
                    raw_trade
                )

                same_token = (
                    c["token"]
                    == token
                )

                different_token = (
                    c["token"]
                    is not None
                    and
                    c["token"]
                    != token
                )

                ingest_interval = (
                    c[
                        "connection_id"
                    ]
                    == conn
                    and
                    base_frame
                    < c[
                        "frame_sequence"
                    ]
                    <= checkpoint_frame
                )

                event_interval = (
                    c["event_ms"]
                    is not None
                    and
                    base_ts
                    < c["event_ms"]
                    <= checkpoint_ts
                )

                exact_checkpoint = (
                    c["event_ms"]
                    is not None
                    and
                    c["event_ms"]
                    == checkpoint_ts
                )

                if not (
                    ingest_interval
                    or event_interval
                    or exact_checkpoint
                ):
                    continue

                if same_token:
                    asset_class = (
                        "SAME_TOKEN"
                    )

                elif different_token:
                    asset_class = (
                        "SAME_MARKET_DIFFERENT_TOKEN"
                    )

                else:
                    asset_class = (
                        "UNKNOWN_ASSET"
                    )


                c.update(
                    {
                        "asset_class":
                            asset_class,

                        "scope_ingest_interval":
                            ingest_interval,

                        "scope_book_event_time_interval":
                            event_interval,

                        "scope_exact_checkpoint_timestamp":
                            exact_checkpoint,
                    }
                )

                eligible_candidates.append(
                    c
                )


                if (
                    same_token
                    and exact_checkpoint
                ):
                    mechanism_flags[
                        "same_token_exact_checkpoint_timestamp"
                    ] = True

                if (
                    different_token
                    and exact_checkpoint
                ):
                    mechanism_flags[
                        "same_market_different_token_exact_checkpoint_timestamp"
                    ] = True

                if (
                    same_token
                    and event_interval
                    and not ingest_interval
                ):
                    mechanism_flags[
                        "same_token_event_time_interval_out_of_ingest_order"
                    ] = True

                if (
                    different_token
                    and ingest_interval
                ):
                    mechanism_flags[
                        "same_market_different_token_ingest_interval"
                    ] = True

                if (
                    different_token
                    and event_interval
                ):
                    mechanism_flags[
                        "same_market_different_token_event_time_interval"
                    ] = True


        candidate_count_histogram[
            len(
                eligible_candidates
            )
        ] += 1


        pair_all_levels_supported = True
        pair_level_records = []


        if not invalid:
            for m in pair_levels:
                level_total += 1

                mismatch_price = int(
                    m[
                        "price_mc"
                    ]
                )

                mismatch_side = str(
                    m["side"]
                )

                reduction = (
                    mismatch_reduction(
                        m
                    )
                )

                price_aligned = []
                accounting_aligned = []


                for c in eligible_candidates:
                    asset_class = c[
                        "asset_class"
                    ]

                    price = c[
                        "price_mc"
                    ]

                    if (
                        asset_class
                        == "SAME_TOKEN"
                    ):
                        price_ok = (
                            price is not None
                            and
                            price
                            == mismatch_price
                        )

                        if price_ok:
                            price_aligned.append(
                                c
                            )

                        side_ok = (
                            (
                                mismatch_side
                                == "S"
                                and
                                c["side"]
                                == "BUY"
                            )
                            or
                            (
                                mismatch_side
                                == "B"
                                and
                                c["side"]
                                == "SELL"
                            )
                        )

                        if (
                            price_ok
                            and side_ok
                        ):
                            accounting_aligned.append(
                                c
                            )


                    elif (
                        asset_class
                        == "SAME_MARKET_DIFFERENT_TOKEN"
                    ):
                        price_ok = (
                            price is not None
                            and
                            price
                            + mismatch_price
                            == 1000
                        )

                        if price_ok:
                            price_aligned.append(
                                c
                            )
                            accounting_aligned.append(
                                c
                            )


                aggregate_size = Decimal(
                    "0"
                )

                aligned_trade_details = []

                for c in accounting_aligned:
                    size = c["size"]

                    if (
                        size is not None
                        and
                        size > 0
                    ):
                        aggregate_size += size

                    aligned_trade_details.append(
                        {
                            "trade_id":
                                c["trade_id"],

                            "asset_class":
                                c[
                                    "asset_class"
                                ],

                            "token":
                                c["token"],

                            "frame_sequence":
                                c[
                                    "frame_sequence"
                                ],

                            "event_ms":
                                c[
                                    "event_ms"
                                ],

                            "price_mc":
                                c[
                                    "price_mc"
                                ],

                            "size":
                                c["size"],

                            "side":
                                c["side"],

                            "scope_ingest_interval":
                                c[
                                    "scope_ingest_interval"
                                ],

                            "scope_book_event_time_interval":
                                c[
                                    "scope_book_event_time_interval"
                                ],

                            "scope_exact_checkpoint_timestamp":
                                c[
                                    "scope_exact_checkpoint_timestamp"
                                ],
                        }
                    )


                price_supported = (
                    len(price_aligned)
                    > 0
                )

                accounting_supported = (
                    len(accounting_aligned)
                    > 0
                )

                size_covered = (
                    reduction is not None
                    and
                    reduction > 0
                    and
                    aggregate_size
                    >= reduction
                )

                direct_level = (
                    accounting_supported
                    and
                    size_covered
                )


                if price_supported:
                    level_price_aligned += 1

                if accounting_supported:
                    level_accounting_aligned += 1

                if size_covered:
                    level_size_covered += 1

                if direct_level:
                    level_direct_supported += 1

                else:
                    pair_all_levels_supported = False


                level_record = {
                    "pair_index": idx,
                    "token": token,
                    "market": str(
                        base_market
                    ),

                    "side":
                        mismatch_side,

                    "mismatch_type":
                        m[
                            "mismatch_type"
                        ],

                    "mismatch_direction":
                        m[
                            "direction"
                        ],

                    "price_mc":
                        mismatch_price,

                    "quantity_reduction":
                        reduction,

                    "eligible_candidate_count":
                        len(
                            eligible_candidates
                        ),

                    "price_aligned_candidate_count":
                        len(
                            price_aligned
                        ),

                    "accounting_aligned_candidate_count":
                        len(
                            accounting_aligned
                        ),

                    "aggregate_accounting_trade_size":
                        aggregate_size,

                    "price_alignment_supported":
                        price_supported,

                    "accounting_alignment_supported":
                        accounting_supported,

                    "size_covered":
                        size_covered,

                    "direct_level_accounting_supported":
                        direct_level,

                    "aligned_trades":
                        aligned_trade_details,
                }

                pair_level_records.append(
                    level_record
                )

                level_fh.write(
                    json.dumps(
                        clean(
                            level_record
                        ),
                        sort_keys=True,
                    )
                    + "\n"
                )


        if invalid:
            classification = (
                "INVALID_PAIR_METADATA"
            )

            for reason in invalid:
                invalid_reasons[
                    reason
                ] += 1

        elif (
            eligible_candidates
            and
            pair_all_levels_supported
        ):
            classification = (
                "DIRECT_TRADE_ACCOUNTING_SUPPORTED"
            )

        elif eligible_candidates:
            classification = (
                "TRADE_CANDIDATE_BUT_INCOMPLETE_ACCOUNTING"
            )

        else:
            classification = (
                "NO_ELIGIBLE_TRADE_CANDIDATE"
            )


        classification_counts[
            classification
        ] += 1


        true_mechanisms = [
            k
            for k, v
            in mechanism_flags.items()
            if v
        ]

        for mechanism in true_mechanisms:
            mechanism_counts[
                mechanism
            ] += 1

        if len(
            true_mechanisms
        ) > 1:
            mechanism_counts[
                "pairs_with_multiple_candidate_mechanisms"
            ] += 1


        candidate_summary = []

        for c in eligible_candidates:
            candidate_summary.append(
                {
                    "trade_id":
                        c["trade_id"],

                    "asset_class":
                        c[
                            "asset_class"
                        ],

                    "token":
                        c["token"],

                    "connection_id":
                        c[
                            "connection_id"
                        ],

                    "frame_sequence":
                        c[
                            "frame_sequence"
                        ],

                    "event_ms":
                        c[
                            "event_ms"
                        ],

                    "price_mc":
                        c[
                            "price_mc"
                        ],

                    "size":
                        c[
                            "size"
                        ],

                    "side":
                        c[
                            "side"
                        ],

                    "scope_ingest_interval":
                        c[
                            "scope_ingest_interval"
                        ],

                    "scope_book_event_time_interval":
                        c[
                            "scope_book_event_time_interval"
                        ],

                    "scope_exact_checkpoint_timestamp":
                        c[
                            "scope_exact_checkpoint_timestamp"
                        ],
                }
            )


        pair_record = {
            "pair_index":
                idx,

            "token":
                token,

            "market":
                (
                    str(base_market)
                    if base_market
                    is not None
                    else None
                ),

            "base_book_timestamp_ms":
                base_ts,

            "checkpoint_book_timestamp_ms":
                checkpoint_ts,

            "base_frame_sequence":
                p[
                    "base_frame"
                ],

            "checkpoint_frame_sequence":
                p[
                    "checkpoint_frame"
                ],

            "mismatch_level_count":
                len(
                    pair_levels
                ),

            "metadata_valid":
                not bool(
                    invalid
                ),

            "invalid_reasons":
                invalid,

            "eligible_candidate_count":
                len(
                    eligible_candidates
                ),

            "mechanisms":
                mechanism_flags,

            "classification":
                classification,

            "all_levels_directly_accounted":
                (
                    pair_all_levels_supported
                    if not invalid
                    else None
                ),

            "candidate_trades":
                candidate_summary,
        }


        pair_fh.write(
            json.dumps(
                clean(
                    pair_record
                ),
                sort_keys=True,
            )
            + "\n"
        )


        if n % 25 == 0:
            print(
                "pairs classified:",
                n,
                flush=True,
            )


if (
    sum(
        classification_counts.values()
    )
    != EXPECTED_PAIRS
):
    raise RuntimeError(
        "classification total does "
        "not equal 124"
    )


direct_pairs = (
    classification_counts[
        "DIRECT_TRADE_ACCOUNTING_SUPPORTED"
    ]
)

if direct_pairs == EXPECTED_PAIRS:
    outcome = (
        "COMPLETE_TRADE_ACCOUNTING"
    )

elif direct_pairs > 0:
    outcome = (
        "PARTIAL_TRADE_ACCOUNTING"
    )

else:
    outcome = (
        "NO_TRADE_ACCOUNTING_SUPPORT"
    )


summary = {
    "version":
        "H2-v2-M2g-DIAGNOSTIC-1",

    "milestone":
        "H2-v2 M2g — unexplained source-transition semantics diagnosis",

    "parent_m2f_commit":
        EXPECTED_PARENT,

    "contract_sha256":
        EXPECTED_HASHES[
            "contract"
        ],

    "population": {
        "pairs":
            EXPECTED_PAIRS,

        "selection":
            "M2f UNEXPLAINED_SOURCE_TRANSITION",

        "posthoc_exclusions":
            0,
    },

    "capture": {
        "capture_id":
            CAPTURE_ID,

        "window_minutes":
            30,

        "start_inclusive":
            START.isoformat(),

        "end_inclusive":
            END.isoformat(),

        "raw_frames_reverified":
            raw_frames,

        "raw_hash_failures":
            raw_hash_failures,

        "same_market_trade_events_indexed":
            trade_events_indexed,
    },

    "pair_classifications": {
        key:
            classification_counts[
                key
            ]

        for key in (
            "INVALID_PAIR_METADATA",
            "DIRECT_TRADE_ACCOUNTING_SUPPORTED",
            "TRADE_CANDIDATE_BUT_INCOMPLETE_ACCOUNTING",
            "NO_ELIGIBLE_TRADE_CANDIDATE",
        )
    },

    "nonexclusive_mechanisms": {
        key:
            mechanism_counts[
                key
            ]

        for key in (
            "same_token_exact_checkpoint_timestamp",
            "same_market_different_token_exact_checkpoint_timestamp",
            "same_token_event_time_interval_out_of_ingest_order",
            "same_market_different_token_ingest_interval",
            "same_market_different_token_event_time_interval",
            "pairs_with_multiple_candidate_mechanisms",
        )
    },

    "candidate_count_distribution": {
        str(k): v
        for k, v in sorted(
            candidate_count_histogram.items()
        )
    },

    "level_accounting": {
        "mismatch_levels":
            level_total,

        "levels_with_price_alignment":
            level_price_aligned,

        "levels_with_accounting_alignment":
            level_accounting_aligned,

        "levels_size_covered":
            level_size_covered,

        "levels_directly_supported":
            level_direct_supported,
    },

    "invalid_pair_reasons": {
        k: v
        for k, v in sorted(
            invalid_reasons.items()
        )
    },

    "milestone_outcome":
        outcome,

    "rules_unchanged_after_contract_freeze":
        True,

    "performance_data_inspected":
        False,

    "resolutions_inspected":
        False,

    "edge_calculated":
        False,

    "signal_calculated":
        False,

    "threshold_selected":
        False,

    "trades_simulated":
        False,

    "pnl_calculated":
        False,
}


SUMMARY_OUT.write_text(
    json.dumps(
        clean(summary),
        indent=2,
        sort_keys=True,
    )
    + "\n",
    encoding="utf-8",
)


print()
print(
    "=== H2-v2 M2g DIAGNOSTIC RESULT ==="
)

print(
    "population pairs:",
    EXPECTED_PAIRS,
)

print(
    "raw frames reverified:",
    raw_frames,
)

print(
    "raw hash failures:",
    raw_hash_failures,
)

print(
    "same-market trade events indexed:",
    trade_events_indexed,
)

print()

print(
    "INVALID_PAIR_METADATA:",
    classification_counts[
        "INVALID_PAIR_METADATA"
    ],
)

print(
    "DIRECT_TRADE_ACCOUNTING_SUPPORTED:",
    classification_counts[
        "DIRECT_TRADE_ACCOUNTING_SUPPORTED"
    ],
)

print(
    "TRADE_CANDIDATE_BUT_INCOMPLETE_ACCOUNTING:",
    classification_counts[
        "TRADE_CANDIDATE_BUT_INCOMPLETE_ACCOUNTING"
    ],
)

print(
    "NO_ELIGIBLE_TRADE_CANDIDATE:",
    classification_counts[
        "NO_ELIGIBLE_TRADE_CANDIDATE"
    ],
)

print()
print(
    "NONEXCLUSIVE MECHANISMS"
)

for key in (
    "same_token_exact_checkpoint_timestamp",
    "same_market_different_token_exact_checkpoint_timestamp",
    "same_token_event_time_interval_out_of_ingest_order",
    "same_market_different_token_ingest_interval",
    "same_market_different_token_event_time_interval",
    "pairs_with_multiple_candidate_mechanisms",
):
    print(
        f"{key}:",
        mechanism_counts[
            key
        ],
    )

print()
print(
    "LEVEL ACCOUNTING"
)

print(
    "mismatch levels:",
    level_total,
)

print(
    "price aligned:",
    level_price_aligned,
)

print(
    "accounting aligned:",
    level_accounting_aligned,
)

print(
    "size covered:",
    level_size_covered,
)

print(
    "directly supported:",
    level_direct_supported,
)

print()
print(
    "candidate count distribution:",
    dict(
        sorted(
            candidate_count_histogram.items()
        )
    ),
)

print()
print(
    "M2g outcome:",
    outcome,
)

print()
print(
    "pair_output_sha256:",
    sha_file(
        PAIR_OUT
    ),
)

print(
    "level_output_sha256:",
    sha_file(
        LEVEL_OUT
    ),
)

print(
    "summary_sha256:",
    sha_file(
        SUMMARY_OUT
    ),
)

print()
print(
    "NO EDGE / NO SIGNAL / "
    "NO TRADES / NO PNL"
)


db.close()

try:
    TMP_DB.unlink()
except FileNotFoundError:
    pass
