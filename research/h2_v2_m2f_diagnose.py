#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

import psycopg2
from dotenv import dotenv_values


REPO = Path(__file__).resolve().parents[1]
R = REPO / "research"

CONTRACT = R / "h2_v2_m2f_contract.json"
FREEZE = R / "h2_v2_m2f_analysis_freeze.json"

PAIR_OUT = R / "h2_v2_m2f_pair_diagnostics.jsonl"
MISMATCH_OUT = R / "h2_v2_m2f_mismatch_diagnostics.jsonl"
SUMMARY_OUT = R / "h2_v2_m2f_summary.json"

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

WATCHLIST = REPO / "config/watchlist_h2_v2.txt"


EXPECTED_HASHES = {
    "contract": (
        "7f97e5ec6c7b4268123d176bb6646427"
        "53955e0be049b025bdeda3700fe89115"
    ),
    "analysis_freeze": (
        "e2d472b938b5bdeda44faff880b33a05"
        "8926d84399144f29f6377d8b6b342e93"
    ),
    "minimum_result": (
        "ef6de5c8b67c54ca9acd2517a99e540"
        "c58c7d59d4e37a7a1461636ac9ccf5eac"
    ),
    "original_minimum_checker": (
        "8f83f47e0f7df491aeec68a03f71c945"
        "6ef07ad39b6e28e7ae901ad36e0a34fc"
    ),
    "streaming_minimum_checker": (
        "3418e71964bbb6e88d5fedafda7f8b40"
        "25f401a9390701756d3dcbc79d8493e3"
    ),
    "m2b_validator": (
        "95e1faf114bb6d05297be93b909bcbf0"
        "72a8e7718a47260bc7fb69acaa387506"
    ),
    "buffer_loader": (
        "7831201e80325f060551ffc4274180512"
        "ebee556be6aede2bc51729db6f053e3"
    ),
    "watchlist": (
        "928e000c50bd69c3792685a12571e89b"
        "1c259d241d9174c0b4a9444e53e8d766"
    ),
    "collector": (
        "50f4ee810ae38974f3156431b6596980"
        "c514c8d17ca11f4c8cc341ff058951c0"
    ),
}

EXPECTED_CAPTURE_HASHES = {
    "manifest": (
        "57f4c1a8f175a51c425a13972a7a6645"
        "561ad442460dc34702556ef7dadcff25"
    ),
    "raw": (
        "f682edd032854aa52f8480faac5291204"
        "e3215bffd2c772bdc558a8694f6319d"
    ),
    "normalized": (
        "728a9bca260fdb88ca9808a07fd7a3d0"
        "49608aa9a2f910430cb57b6e0ec9eb82"
    ),
}

EXPECTED_ELIGIBLE_PAIRS = 1184
EXPECTED_ELIGIBLE_TOKENS = 14


EPOCH = datetime(
    1970, 1, 1,
    tzinfo=timezone.utc,
)


def sha_file(path: Path) -> str:
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def sha_text(text: str) -> str:
    return hashlib.sha256(
        text.encode("utf-8")
    ).hexdigest()


def canonical_json(obj) -> str:
    return json.dumps(
        obj,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def parse_dt(value) -> datetime:
    if isinstance(value, datetime):
        return value

    return datetime.fromisoformat(
        str(value).replace(
            "Z",
            "+00:00",
        )
    )


def dt_us(value) -> int:
    d = parse_dt(value)
    delta = d - EPOCH

    return (
        delta.days * 86400 * 1_000_000
        + delta.seconds * 1_000_000
        + delta.microseconds
    )


def us_iso(value: int) -> str:
    return (
        EPOCH
        + timedelta(
            microseconds=value
        )
    ).isoformat()


def decimal_key(value):
    if value is None:
        return None

    try:
        d = Decimal(str(value))
    except (
        InvalidOperation,
        ValueError,
        TypeError,
    ):
        return None

    if d == 0:
        return "0"

    return format(
        d.normalize(),
        "f",
    )


def mc(value):
    if value is None or value == "":
        return None

    try:
        v = int(
            Decimal(str(value))
            * 1000
        )
    except (
        InvalidOperation,
        ValueError,
        TypeError,
    ):
        return None

    if not 0 <= v <= 1000:
        return None

    return v


def event_ms(value):
    if value is None or value == "":
        return None

    try:
        return int(value)
    except (
        ValueError,
        TypeError,
    ):
        return None


def db_event_ms(value):
    if value is None:
        return None

    d = parse_dt(value)
    delta = d - EPOCH

    total_us = (
        delta.days * 86400 * 1_000_000
        + delta.seconds * 1_000_000
        + delta.microseconds
    )

    return total_us // 1000


def side_code(value):
    side = (
        str(value or "")[:1]
        .upper()
    )

    if side not in (
        "B",
        "S",
    ):
        side = (
            "B"
            if str(
                value or ""
            ).upper().startswith("BUY")
            else "S"
        )

    return side


def json_value(value):
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value

    return value


def payload_book_levels(
    levels,
):
    out = []

    for level in levels or []:
        p = mc(
            level.get("price")
        )

        if p is None:
            continue

        out.append(
            [
                p,
                decimal_key(
                    level.get("size")
                ),
            ]
        )

    out.sort(
        key=lambda x: x[0]
    )

    return out


def db_book_levels(
    levels,
):
    levels = json_value(
        levels
    )

    out = []

    for level in levels or []:
        out.append(
            [
                int(level[0]),
                decimal_key(
                    level[1]
                ),
            ]
        )

    out.sort(
        key=lambda x: x[0]
    )

    return out


def book_loader_signature(
    payload,
):
    return sha_text(
        canonical_json(
            {
                "event_ms": event_ms(
                    payload.get(
                        "timestamp"
                    )
                ),
                "bids": payload_book_levels(
                    payload.get("bids")
                    or []
                ),
                "asks": payload_book_levels(
                    payload.get("asks")
                    or []
                ),
                "venue_hash": payload.get(
                    "hash"
                ),
            }
        )
    )


def book_db_signature(
    event_time,
    bids,
    asks,
    venue_hash,
):
    return sha_text(
        canonical_json(
            {
                "event_ms": db_event_ms(
                    event_time
                ),
                "bids": db_book_levels(
                    bids
                ),
                "asks": db_book_levels(
                    asks
                ),
                "venue_hash": venue_hash,
            }
        )
    )


def delta_loader_signature(
    payload,
):
    return sha_text(
        canonical_json(
            {
                "event_ms": event_ms(
                    payload.get(
                        "timestamp"
                    )
                ),
                "price_mc": mc(
                    payload.get("price")
                ),
                "size": decimal_key(
                    payload.get("size")
                ),
                "side": side_code(
                    payload.get("side")
                ),
                "venue_hash": payload.get(
                    "hash"
                ),
                "best_bid_mc": mc(
                    payload.get(
                        "best_bid"
                    )
                ),
                "best_ask_mc": mc(
                    payload.get(
                        "best_ask"
                    )
                ),
            }
        )
    )


def delta_db_signature(
    event_time,
    price_mc,
    size,
    side,
    venue_hash,
    best_bid_mc,
    best_ask_mc,
):
    return sha_text(
        canonical_json(
            {
                "event_ms": db_event_ms(
                    event_time
                ),
                "price_mc": (
                    int(price_mc)
                    if price_mc is not None
                    else None
                ),
                "size": decimal_key(
                    size
                ),
                "side": side,
                "venue_hash": venue_hash,
                "best_bid_mc": (
                    int(best_bid_mc)
                    if best_bid_mc
                    is not None
                    else None
                ),
                "best_ask_mc": (
                    int(best_ask_mc)
                    if best_ask_mc
                    is not None
                    else None
                ),
            }
        )
    )


def book_state(
    payload,
):
    bids = {}
    asks = {}

    for side, levels in (
        (
            "B",
            payload.get("bids")
            or [],
        ),
        (
            "S",
            payload.get("asks")
            or [],
        ),
    ):
        target = (
            bids
            if side == "B"
            else asks
        )

        for level in levels:
            price = mc(
                level.get("price")
            )

            size = Decimal(
                str(
                    level.get("size")
                )
            )

            if size > 0:
                target[price] = size

    return bids, asks


def clean(obj):
    if isinstance(obj, Decimal):
        return str(obj)

    if isinstance(obj, datetime):
        return obj.isoformat()

    if isinstance(obj, dict):
        return {
            str(k): clean(v)
            for k, v in obj.items()
        }

    if isinstance(
        obj,
        (list, tuple),
    ):
        return [
            clean(v)
            for v in obj
        ]

    return obj


# ----------------------------------------------------------------------
# Frozen input verification
# ----------------------------------------------------------------------

CHECKS = {
    "contract": CONTRACT,
    "analysis_freeze": FREEZE,
    "minimum_result": (
        R
        / "h2_v2_m2f_evidence_minimum_result.json"
    ),
    "original_minimum_checker": (
        R
        / "h2_v2_m2f_evidence_minimum.py"
    ),
    "streaming_minimum_checker": (
        R
        / "h2_v2_m2f_evidence_minimum_streaming.py"
    ),
    "m2b_validator": (
        R
        / "h2_v2_m2b_validate.py"
    ),
    "buffer_loader": (
        REPO
        / "db/buffer_loader.py"
    ),
    "watchlist": WATCHLIST,
    "collector": (
        REPO
        / "collectors/polymarket/ws_collector.py"
    ),
}

for name, path in CHECKS.items():
    actual = sha_file(
        path
    )

    if actual != EXPECTED_HASHES[
        name
    ]:
        raise SystemExit(
            "REFUSING DIAGNOSIS: "
            f"{name} hash changed "
            f"{actual}"
        )


capture_checks = {
    "manifest": MANIFEST,
    "raw": RAW,
    "normalized": NORMALIZED,
}

for name, path in (
    capture_checks.items()
):
    actual = sha_file(
        path
    )

    if (
        actual
        != EXPECTED_CAPTURE_HASHES[
            name
        ]
    ):
        raise SystemExit(
            "REFUSING DIAGNOSIS: "
            f"capture {name} hash "
            "changed"
        )


freeze = json.loads(
    FREEZE.read_text()
)

if (
    freeze[
        "analysis_window"
    ]["minutes"] != 30
):
    raise SystemExit(
        "analysis window is not "
        "frozen at 30 minutes"
    )

if (
    freeze[
        "minimum_evidence"
    ]["eligible_pairs"]
    != EXPECTED_ELIGIBLE_PAIRS
):
    raise SystemExit(
        "eligible pair freeze mismatch"
    )

if (
    freeze[
        "minimum_evidence"
    ]["distinct_tokens"]
    != EXPECTED_ELIGIBLE_TOKENS
):
    raise SystemExit(
        "eligible token freeze mismatch"
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

START_US = dt_us(
    START
)

END_US = dt_us(
    END
)


H2_TOKENS = {
    ln.strip()
    for ln in WATCHLIST.read_text().splitlines()
    if ln.strip()
    and not ln.lstrip().startswith("#")
}

if len(H2_TOKENS) != 34:
    raise SystemExit(
        "frozen H2-v2 watchlist "
        "does not contain 34 tokens"
    )


# ----------------------------------------------------------------------
# Disk-backed diagnostic index
# ----------------------------------------------------------------------

fd, tmp_name = tempfile.mkstemp(
    prefix="h2_v2_m2f_diag_",
    suffix=".sqlite3",
)

os.close(fd)

DB_PATH = Path(
    tmp_name
)

db = sqlite3.connect(
    DB_PATH
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
        capture_us INTEGER NOT NULL,
        raw_sha TEXT NOT NULL,
        hash_valid INTEGER NOT NULL,
        parse_ok INTEGER NOT NULL,
        PRIMARY KEY (
            connection_id,
            frame_sequence
        )
    ) WITHOUT ROWID;

    CREATE TABLE raw_events (
        id INTEGER PRIMARY KEY,
        connection_id TEXT NOT NULL,
        frame_sequence INTEGER NOT NULL,
        msg_index INTEGER NOT NULL,
        kind TEXT NOT NULL,
        token TEXT NOT NULL,
        change_index INTEGER NOT NULL,
        payload_hash TEXT NOT NULL,
        raw_sha TEXT NOT NULL,
        match_count INTEGER
    );

    CREATE TABLE norm (
        id INTEGER PRIMARY KEY,
        file_order INTEGER NOT NULL,
        kind TEXT NOT NULL,
        token TEXT,
        capture_us INTEGER NOT NULL,
        connection_id TEXT,
        ingest_sequence INTEGER,
        change_index INTEGER,
        book_generation INTEGER,
        frame_sequence INTEGER,
        raw_sha TEXT,
        payload_hash TEXT NOT NULL,
        payload_json TEXT,
        loader_sig TEXT,
        raw_match_count INTEGER,
        db_match_count INTEGER
    );

    CREATE TABLE db_books (
        token TEXT NOT NULL,
        capture_us INTEGER NOT NULL,
        connection_id TEXT,
        ingest_sequence INTEGER,
        book_generation INTEGER,
        loader_sig TEXT NOT NULL
    );

    CREATE TABLE db_deltas (
        token TEXT NOT NULL,
        capture_us INTEGER NOT NULL,
        connection_id TEXT,
        ingest_sequence INTEGER,
        change_index INTEGER,
        book_generation INTEGER,
        loader_sig TEXT NOT NULL
    );
    """
)


# ----------------------------------------------------------------------
# Raw websocket -> expected collector normalization
# ----------------------------------------------------------------------

raw_frame_count = 0
raw_hash_failures = 0
raw_parse_failures = 0
raw_event_count = 0

raw_event_id = 0
raw_batch = []
frame_batch = []


def add_expected(
    *,
    conn,
    frame_seq,
    msg_index,
    kind,
    token,
    ci,
    payload,
    raw_sha,
):
    global raw_event_id
    global raw_event_count

    if token not in H2_TOKENS:
        return

    raw_event_id += 1
    raw_event_count += 1

    payload_text = canonical_json(
        payload
    )

    raw_batch.append(
        (
            raw_event_id,
            conn,
            frame_seq,
            msg_index,
            kind,
            token,
            ci,
            sha_text(
                payload_text
            ),
            raw_sha,
        )
    )


print(
    "=== M2f RAW FRAME RECONCILIATION ===",
    flush=True,
)

with RAW.open(
    "r",
    encoding="utf-8",
) as fh:

    for line_number, line in enumerate(
        fh,
        start=1,
    ):
        if not line.strip():
            continue

        r = json.loads(
            line
        )

        capture_us = dt_us(
            r["capture_time"]
        )

        if capture_us > END_US:
            continue

        conn = str(
            r["connection_id"]
        )

        frame_seq = int(
            r["frame_sequence"]
        )

        raw_text = r["raw"]

        recomputed = hashlib.sha256(
            raw_text.encode("utf-8")
        ).hexdigest()

        valid = (
            recomputed
            == r["raw_frame_sha256"]
        )

        raw_frame_count += 1

        if not valid:
            raw_hash_failures += 1

        parse_ok = True
        parsed = None

        if raw_text != "PONG":
            try:
                parsed = json.loads(
                    raw_text
                )
            except json.JSONDecodeError:
                parse_ok = False
                raw_parse_failures += 1

        frame_batch.append(
            (
                conn,
                frame_seq,
                capture_us,
                r[
                    "raw_frame_sha256"
                ],
                int(valid),
                int(parse_ok),
            )
        )

        if (
            raw_text != "PONG"
            and parse_ok
        ):
            if isinstance(
                parsed,
                dict,
            ):
                messages = [
                    parsed
                ]
            elif isinstance(
                parsed,
                list,
            ):
                messages = parsed
            else:
                messages = []
                parse_ok = False
                raw_parse_failures += 1

            for msg_index, d in enumerate(
                messages
            ):
                if not isinstance(
                    d,
                    dict,
                ):
                    continue

                et = d.get(
                    "event_type"
                )

                outer_token = (
                    str(
                        d.get(
                            "asset_id"
                        )
                    )
                    if d.get(
                        "asset_id"
                    )
                    else None
                )

                if et == "price_change":
                    changes = (
                        d.get(
                            "price_changes"
                        )
                        or d.get(
                            "changes"
                        )
                        or [d]
                    )

                    for ci, ch in enumerate(
                        changes
                    ):
                        if not isinstance(
                            ch,
                            dict,
                        ):
                            continue

                        token = (
                            str(
                                ch.get(
                                    "asset_id"
                                )
                            )
                            if ch.get(
                                "asset_id"
                            )
                            else outer_token
                        )

                        payload = {
                            **ch,
                            "market": d.get(
                                "market"
                            ),
                            "timestamp": d.get(
                                "timestamp"
                            ),
                        }

                        add_expected(
                            conn=conn,
                            frame_seq=frame_seq,
                            msg_index=msg_index,
                            kind="price_change",
                            token=token,
                            ci=ci,
                            payload=payload,
                            raw_sha=r[
                                "raw_frame_sha256"
                            ],
                        )

                else:
                    if et == "book":
                        kind = "ws_book"

                    elif et in (
                        "best_bid_ask",
                        "last_trade_price",
                        "tick_size_change",
                        "market_resolved",
                        "new_market",
                    ):
                        kind = et

                    else:
                        kind = "unknown"

                    add_expected(
                        conn=conn,
                        frame_seq=frame_seq,
                        msg_index=msg_index,
                        kind=kind,
                        token=outer_token,
                        ci=0,
                        payload=d,
                        raw_sha=r[
                            "raw_frame_sha256"
                        ],
                    )

        if len(
            frame_batch
        ) >= 5000:
            db.executemany(
                """
                INSERT INTO raw_frames
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                frame_batch,
            )

            frame_batch.clear()

        if len(
            raw_batch
        ) >= 5000:
            db.executemany(
                """
                INSERT INTO raw_events
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                """,
                raw_batch,
            )

            raw_batch.clear()

        if (
            line_number
            % 100000
            == 0
        ):
            print(
                "raw lines scanned:",
                line_number,
                flush=True,
            )


if frame_batch:
    db.executemany(
        """
        INSERT INTO raw_frames
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        frame_batch,
    )

if raw_batch:
    db.executemany(
        """
        INSERT INTO raw_events
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
        """,
        raw_batch,
    )

db.commit()


db.executescript(
    """
    CREATE INDEX raw_event_match_idx
    ON raw_events (
        connection_id,
        frame_sequence,
        raw_sha,
        kind,
        token,
        change_index,
        payload_hash
    );

    CREATE INDEX raw_event_interval_idx
    ON raw_events (
        token,
        connection_id,
        frame_sequence
    );
    """
)


# ----------------------------------------------------------------------
# Normalized evidence + frozen eligible pairs
# ----------------------------------------------------------------------

pairs = []

last_ws = {}
price_change_since_ws = {}

normalized_count = 0
norm_id = 0

print()
print(
    "=== M2f NORMALIZED EVIDENCE INDEX ===",
    flush=True,
)

with NORMALIZED.open(
    "r",
    encoding="utf-8",
) as fh:

    for file_order, line in enumerate(
        fh,
        start=1,
    ):
        if not line.strip():
            continue

        row = json.loads(
            line
        )

        capture_us = dt_us(
            row["capture_time"]
        )

        if capture_us > END_US:
            continue

        if (
            row.get(
                "watchlist_rule"
            )
            != "h2_v2"
        ):
            continue

        normalized_count += 1
        norm_id += 1

        kind = row.get(
            "kind"
        )

        token = row.get(
            "token"
        )

        payload = (
            row.get(
                "payload"
            )
            or {}
        )

        payload_text = canonical_json(
            payload
        )

        payload_hash = sha_text(
            payload_text
        )

        loader_sig = None
        payload_store = None

        if kind == "ws_book":
            loader_sig = (
                book_loader_signature(
                    payload
                )
            )
            payload_store = (
                payload_text
            )

        elif kind == "price_change":
            loader_sig = (
                delta_loader_signature(
                    payload
                )
            )
            payload_store = (
                payload_text
            )

        db.execute(
            """
            INSERT INTO norm (
                id,
                file_order,
                kind,
                token,
                capture_us,
                connection_id,
                ingest_sequence,
                change_index,
                book_generation,
                frame_sequence,
                raw_sha,
                payload_hash,
                payload_json,
                loader_sig,
                raw_match_count,
                db_match_count
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, NULL, NULL
            )
            """,
            (
                norm_id,
                file_order,
                kind,
                token,
                capture_us,
                (
                    str(
                        row.get(
                            "connection_id"
                        )
                    )
                    if row.get(
                        "connection_id"
                    )
                    is not None
                    else None
                ),
                row.get(
                    "ingest_sequence"
                ),
                row.get(
                    "change_index"
                ),
                row.get(
                    "book_generation"
                ),
                row.get(
                    "raw_frame_sequence"
                ),
                row.get(
                    "raw_frame_sha256"
                ),
                payload_hash,
                payload_store,
                loader_sig,
            ),
        )

        if not token:
            continue

        if kind == "price_change":
            base = last_ws.get(
                token
            )

            if (
                base is not None
                and
                row.get(
                    "connection_id"
                )
                == base[
                    "connection_id"
                ]
                and
                row.get(
                    "book_generation"
                )
                == base[
                    "book_generation"
                ]
            ):
                price_change_since_ws[
                    token
                ] = True

            continue

        if kind != "ws_book":
            continue

        current = {
            "norm_id": norm_id,
            "token": token,
            "capture_us": capture_us,
            "connection_id": row.get(
                "connection_id"
            ),
            "ingest_sequence": row.get(
                "ingest_sequence"
            ),
            "book_generation": row.get(
                "book_generation"
            ),
            "frame_sequence": row.get(
                "raw_frame_sequence"
            ),
            "raw_sha": row.get(
                "raw_frame_sha256"
            ),
        }

        base = last_ws.get(
            token
        )

        if base is not None:
            same_context = (
                current[
                    "connection_id"
                ]
                == base[
                    "connection_id"
                ]
                and
                current[
                    "book_generation"
                ]
                == base[
                    "book_generation"
                ]
            )

            if same_context:
                def provenance_ok(
                    x
                ):
                    found = db.execute(
                        """
                        SELECT
                            hash_valid,
                            raw_sha
                        FROM raw_frames
                        WHERE
                            connection_id = ?
                            AND frame_sequence = ?
                        """,
                        (
                            str(
                                x[
                                    "connection_id"
                                ]
                            ),
                            x[
                                "frame_sequence"
                            ],
                        ),
                    ).fetchone()

                    return (
                        found is not None
                        and bool(
                            found[0]
                        )
                        and found[1]
                        == x[
                            "raw_sha"
                        ]
                    )

                if (
                    provenance_ok(
                        base
                    )
                    and
                    provenance_ok(
                        current
                    )
                    and
                    price_change_since_ws.get(
                        token,
                        False,
                    )
                ):
                    pairs.append(
                        {
                            "pair_index": (
                                len(pairs)
                                + 1
                            ),
                            "token": token,
                            "connection_id": str(
                                current[
                                    "connection_id"
                                ]
                            ),
                            "book_generation": (
                                current[
                                    "book_generation"
                                ]
                            ),
                            "base_norm_id": (
                                base[
                                    "norm_id"
                                ]
                            ),
                            "checkpoint_norm_id": (
                                current[
                                    "norm_id"
                                ]
                            ),
                            "base_capture_us": (
                                base[
                                    "capture_us"
                                ]
                            ),
                            "checkpoint_capture_us": (
                                current[
                                    "capture_us"
                                ]
                            ),
                            "base_seq": (
                                base[
                                    "ingest_sequence"
                                ]
                            ),
                            "checkpoint_seq": (
                                current[
                                    "ingest_sequence"
                                ]
                            ),
                            "base_frame": (
                                base[
                                    "frame_sequence"
                                ]
                            ),
                            "checkpoint_frame": (
                                current[
                                    "frame_sequence"
                                ]
                            ),
                        }
                    )

        last_ws[
            token
        ] = current

        price_change_since_ws[
            token
        ] = False

        if (
            file_order
            % 100000
            == 0
        ):
            print(
                "normalized lines scanned:",
                file_order,
                flush=True,
            )


db.commit()


if len(pairs) != (
    EXPECTED_ELIGIBLE_PAIRS
):
    raise RuntimeError(
        "frozen eligible-pair "
        "reproduction failed: "
        f"{len(pairs)}"
    )

pair_tokens = {
    p["token"]
    for p in pairs
}

if len(pair_tokens) != (
    EXPECTED_ELIGIBLE_TOKENS
):
    raise RuntimeError(
        "frozen eligible-token "
        "reproduction failed"
    )


db.executescript(
    """
    CREATE INDEX norm_raw_match_idx
    ON norm (
        connection_id,
        frame_sequence,
        raw_sha,
        kind,
        token,
        change_index,
        payload_hash
    );

    CREATE INDEX norm_interval_idx
    ON norm (
        token,
        connection_id,
        book_generation,
        ingest_sequence,
        change_index
    );

    CREATE INDEX norm_kind_idx
    ON norm (
        kind
    );
    """
)


# ----------------------------------------------------------------------
# Exact raw -> normalized reconciliation
#
# Ingest sequence is validated inside each raw websocket frame.
# REST resyncs can increment global ingest_sequence between frames, so
# no false assumption of one constant offset is made across the capture.
# ----------------------------------------------------------------------

print()
print(
    "=== RAW -> NORMALIZED EXACT MAPPING ===",
    flush=True,
)


db.execute(
    """
    UPDATE raw_events

    SET match_count = (
        SELECT count(*)

        FROM norm n

        WHERE
            n.connection_id =
                raw_events.connection_id
            AND n.frame_sequence =
                raw_events.frame_sequence
            AND n.raw_sha =
                raw_events.raw_sha
            AND n.kind =
                raw_events.kind
            AND n.token =
                raw_events.token
            AND n.change_index =
                raw_events.change_index
            AND n.payload_hash =
                raw_events.payload_hash
    )
    """
)


db.execute(
    """
    UPDATE norm

    SET raw_match_count = (
        SELECT count(*)

        FROM raw_events r

        WHERE
            norm.frame_sequence
                IS NOT NULL
            AND r.connection_id =
                norm.connection_id
            AND r.frame_sequence =
                norm.frame_sequence
            AND r.raw_sha =
                norm.raw_sha
            AND r.kind =
                norm.kind
            AND r.token =
                norm.token
            AND r.change_index =
                norm.change_index
            AND r.payload_hash =
                norm.payload_hash
    )

    WHERE
        frame_sequence IS NOT NULL
    """
)


db.execute(
    """
    CREATE TEMP TABLE bad_sequence_frames AS

    SELECT
        r.connection_id,
        r.frame_sequence

    FROM raw_events r

    JOIN norm n
      ON n.connection_id =
            r.connection_id
     AND n.frame_sequence =
            r.frame_sequence
     AND n.raw_sha =
            r.raw_sha
     AND n.kind =
            r.kind
     AND n.token =
            r.token
     AND n.change_index =
            r.change_index
     AND n.payload_hash =
            r.payload_hash

    WHERE
        r.match_count = 1
        AND n.raw_match_count = 1

    GROUP BY
        r.connection_id,
        r.frame_sequence

    HAVING
        count(
            DISTINCT (
                n.ingest_sequence
                - r.msg_index
            )
        ) > 1
    """
)


db.execute(
    """
    CREATE INDEX bad_sequence_frame_idx
    ON bad_sequence_frames (
        connection_id,
        frame_sequence
    )
    """
)

db.commit()


raw_expected_matched = (
    db.execute(
        """
        SELECT count(*)
        FROM raw_events
        WHERE match_count = 1
        """
    ).fetchone()[0]
)

norm_ws_total = (
    db.execute(
        """
        SELECT count(*)
        FROM norm
        WHERE frame_sequence
              IS NOT NULL
        """
    ).fetchone()[0]
)

norm_ws_matched = (
    db.execute(
        """
        SELECT count(*)
        FROM norm
        WHERE
            frame_sequence
                IS NOT NULL
            AND raw_match_count = 1
        """
    ).fetchone()[0]
)

bad_sequence_frame_count = (
    db.execute(
        """
        SELECT count(*)
        FROM bad_sequence_frames
        """
    ).fetchone()[0]
)


# ----------------------------------------------------------------------
# Postgres -> disk-backed loader output index
# ----------------------------------------------------------------------

env = dotenv_values(
    REPO / ".env"
)

pg = psycopg2.connect(
    host="127.0.0.1",
    port=5432,
    dbname="quantlab",
    user="quantlab",
    password=env[
        "PG_PASSWORD"
    ],
)

pg.set_session(
    readonly=True,
    autocommit=False,
)

token_list = sorted(
    H2_TOKENS
)


print()
print(
    "=== POSTGRES WS_BOOK INDEX ===",
    flush=True,
)

cur = pg.cursor(
    name="m2f_books"
)

cur.itersize = 5000

cur.execute(
    """
    SELECT
        t.venue_token_id,
        b.event_time,
        b.capture_time,
        b.bids,
        b.asks,
        b.venue_hash,
        b.connection_id,
        b.ingest_sequence,
        b.book_generation

    FROM book_snapshots b

    JOIN tokens t
      ON t.id = b.token_id

    WHERE
        t.venue_token_id = ANY(%s)
        AND b.capture_time >= %s
        AND b.capture_time <= %s
        AND b.watchlist_rule =
            'h2_v2'
        AND b.source =
            'ws_book'

    ORDER BY
        b.capture_time,
        b.ingest_sequence
    """,
    (
        token_list,
        START,
        END,
    ),
)

book_batch = []
book_db_rows = 0

for row in cur:
    (
        token,
        ev,
        cap,
        bids,
        asks,
        venue_hash,
        conn,
        seq,
        gen,
    ) = row

    book_db_rows += 1

    book_batch.append(
        (
            str(token),
            dt_us(cap),
            (
                str(conn)
                if conn is not None
                else None
            ),
            seq,
            gen,
            book_db_signature(
                ev,
                bids,
                asks,
                venue_hash,
            ),
        )
    )

    if len(
        book_batch
    ) >= 5000:
        db.executemany(
            """
            INSERT INTO db_books
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            book_batch,
        )

        book_batch.clear()

if book_batch:
    db.executemany(
        """
        INSERT INTO db_books
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        book_batch,
    )

cur.close()


print(
    "book rows indexed:",
    book_db_rows,
    flush=True,
)


print()
print(
    "=== POSTGRES PRICE_CHANGE INDEX ===",
    flush=True,
)

cur = pg.cursor(
    name="m2f_deltas"
)

cur.itersize = 10000

cur.execute(
    """
    SELECT
        t.venue_token_id,
        d.event_time,
        d.capture_time,
        d.price_mc,
        d.size,
        d.side,
        d.venue_hash,
        d.best_bid_mc,
        d.best_ask_mc,
        d.connection_id,
        d.ingest_sequence,
        d.change_index,
        d.book_generation

    FROM book_deltas d

    JOIN tokens t
      ON t.id = d.token_id

    WHERE
        t.venue_token_id = ANY(%s)
        AND d.capture_time >= %s
        AND d.capture_time <= %s
        AND d.watchlist_rule =
            'h2_v2'
        AND d.source =
            'ws_price_change'

    ORDER BY
        d.capture_time,
        d.ingest_sequence,
        d.change_index
    """,
    (
        token_list,
        START,
        END,
    ),
)

delta_batch = []
delta_db_rows = 0

for row in cur:
    (
        token,
        ev,
        cap,
        price,
        size,
        side,
        venue_hash,
        bb,
        ba,
        conn,
        seq,
        ci,
        gen,
    ) = row

    delta_db_rows += 1

    delta_batch.append(
        (
            str(token),
            dt_us(cap),
            (
                str(conn)
                if conn is not None
                else None
            ),
            seq,
            ci,
            gen,
            delta_db_signature(
                ev,
                price,
                size,
                side,
                venue_hash,
                bb,
                ba,
            ),
        )
    )

    if len(
        delta_batch
    ) >= 10000:
        db.executemany(
            """
            INSERT INTO db_deltas
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            delta_batch,
        )

        delta_batch.clear()

if delta_batch:
    db.executemany(
        """
        INSERT INTO db_deltas
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        delta_batch,
    )

cur.close()

pg.rollback()
pg.close()

db.commit()


db.executescript(
    """
    CREATE INDEX db_book_match_idx
    ON db_books (
        token,
        capture_us,
        connection_id,
        ingest_sequence,
        book_generation,
        loader_sig
    );

    CREATE INDEX db_delta_match_idx
    ON db_deltas (
        token,
        capture_us,
        connection_id,
        ingest_sequence,
        change_index,
        book_generation,
        loader_sig
    );
    """
)


print(
    "delta rows indexed:",
    delta_db_rows,
    flush=True,
)


# ----------------------------------------------------------------------
# Exact normalized -> DB mapping
# ----------------------------------------------------------------------

print()
print(
    "=== NORMALIZED -> DB EXACT MAPPING ===",
    flush=True,
)


db.execute(
    """
    UPDATE norm

    SET db_match_count = (
        SELECT count(*)

        FROM db_books b

        WHERE
            b.token = norm.token
            AND b.capture_us =
                norm.capture_us
            AND b.connection_id =
                norm.connection_id
            AND b.ingest_sequence =
                norm.ingest_sequence
            AND b.book_generation =
                norm.book_generation
            AND b.loader_sig =
                norm.loader_sig
    )

    WHERE kind = 'ws_book'
    """
)


db.execute(
    """
    UPDATE norm

    SET db_match_count = (
        SELECT count(*)

        FROM db_deltas d

        WHERE
            d.token = norm.token
            AND d.capture_us =
                norm.capture_us
            AND d.connection_id =
                norm.connection_id
            AND d.ingest_sequence =
                norm.ingest_sequence
            AND d.change_index =
                norm.change_index
            AND d.book_generation =
                norm.book_generation
            AND d.loader_sig =
                norm.loader_sig
    )

    WHERE kind = 'price_change'
    """
)

db.commit()


def mapping_totals(
    kind,
):
    total, exact = db.execute(
        """
        SELECT
            count(*),
            sum(
                CASE
                    WHEN db_match_count = 1
                    THEN 1
                    ELSE 0
                END
            )

        FROM norm

        WHERE kind = ?
        """,
        (
            kind,
        ),
    ).fetchone()

    return {
        "total": total,
        "exact_db_matches": (
            exact or 0
        ),
        "non_exact": (
            total
            - (exact or 0)
        ),
    }


db_book_mapping = (
    mapping_totals(
        "ws_book"
    )
)

db_delta_mapping = (
    mapping_totals(
        "price_change"
    )
)


# ----------------------------------------------------------------------
# Pair classification
# ----------------------------------------------------------------------

classification_counts = Counter()
mismatch_type_counts = Counter()
mismatch_direction_counts = Counter()
mismatch_side_counts = Counter()

mismatch_pair_count = 0
mismatch_level_count = 0

PAIR_OUT.unlink(
    missing_ok=True
)

MISMATCH_OUT.unlink(
    missing_ok=True
)


def norm_payload(
    norm_id,
):
    row = db.execute(
        """
        SELECT payload_json
        FROM norm
        WHERE id = ?
        """,
        (
            norm_id,
        ),
    ).fetchone()

    if (
        row is None
        or row[0] is None
    ):
        raise RuntimeError(
            "missing replay payload"
        )

    return json.loads(
        row[0]
    )


def replay_pair(
    pair,
):
    base_payload = norm_payload(
        pair[
            "base_norm_id"
        ]
    )

    checkpoint_payload = norm_payload(
        pair[
            "checkpoint_norm_id"
        ]
    )

    bids, asks = book_state(
        base_payload
    )

    rows = db.execute(
        """
        SELECT
            payload_json

        FROM norm

        WHERE
            kind = 'price_change'
            AND token = ?
            AND connection_id = ?
            AND book_generation = ?
            AND ingest_sequence > ?
            AND ingest_sequence < ?

        ORDER BY
            ingest_sequence,
            change_index,
            id
        """,
        (
            pair["token"],
            pair[
                "connection_id"
            ],
            pair[
                "book_generation"
            ],
            pair["base_seq"],
            pair[
                "checkpoint_seq"
            ],
        ),
    ).fetchall()

    for (payload_text,) in rows:
        p = json.loads(
            payload_text
        )

        price = mc(
            p.get("price")
        )

        size = Decimal(
            str(
                p.get("size")
            )
        )

        side = side_code(
            p.get("side")
        )

        book = (
            bids
            if side == "B"
            else asks
        )

        if size == 0:
            book.pop(
                price,
                None,
            )
        else:
            book[
                price
            ] = size

    target_bids, target_asks = (
        book_state(
            checkpoint_payload
        )
    )

    mismatches = []

    for side, replayed, target in (
        (
            "B",
            bids,
            target_bids,
        ),
        (
            "S",
            asks,
            target_asks,
        ),
    ):
        prices = sorted(
            set(replayed)
            | set(target)
        )

        for price in prices:
            r = replayed.get(
                price
            )

            t = target.get(
                price
            )

            if r == t:
                continue

            if (
                r is None
                or t is None
            ):
                mismatch_type = (
                    "presence"
                )
                delta = None

                direction = (
                    "checkpoint_added_level"
                    if r is None
                    else "checkpoint_removed_level"
                )

            else:
                mismatch_type = (
                    "size"
                )

                delta = (
                    t - r
                )

                if delta < 0:
                    direction = (
                        "checkpoint_lower"
                    )

                elif delta > 0:
                    direction = (
                        "checkpoint_higher"
                    )

                else:
                    direction = "equal"

            mismatches.append(
                {
                    "side": side,
                    "price_mc": price,
                    "replayed_size": r,
                    "checkpoint_size": t,
                    "checkpoint_minus_replay": delta,
                    "mismatch_type": mismatch_type,
                    "direction": direction,
                }
            )

    return (
        len(rows),
        mismatches,
    )


print()
print(
    "=== CLASSIFY 1184 FROZEN PAIRS ===",
    flush=True,
)


with PAIR_OUT.open(
    "w",
    encoding="utf-8",
) as pair_fh, MISMATCH_OUT.open(
    "w",
    encoding="utf-8",
) as mismatch_fh:

    for i, pair in enumerate(
        pairs,
        start=1,
    ):
        token = pair[
            "token"
        ]

        conn = pair[
            "connection_id"
        ]

        base_frame = pair[
            "base_frame"
        ]

        checkpoint_frame = pair[
            "checkpoint_frame"
        ]


        raw_integrity_failures = (
            db.execute(
                """
                SELECT count(*)

                FROM raw_frames

                WHERE
                    connection_id = ?
                    AND frame_sequence >= ?
                    AND frame_sequence <= ?
                    AND hash_valid != 1
                """,
                (
                    conn,
                    base_frame,
                    checkpoint_frame,
                ),
            ).fetchone()[0]
        )


        parse_failures = (
            db.execute(
                """
                SELECT count(*)

                FROM raw_frames

                WHERE
                    connection_id = ?
                    AND frame_sequence >= ?
                    AND frame_sequence <= ?
                    AND parse_ok != 1
                """,
                (
                    conn,
                    base_frame,
                    checkpoint_frame,
                ),
            ).fetchone()[0]
        )


        expected_normalization_losses = (
            db.execute(
                """
                SELECT count(*)

                FROM raw_events

                WHERE
                    token = ?
                    AND connection_id = ?
                    AND frame_sequence >= ?
                    AND frame_sequence <= ?
                    AND match_count != 1
                """,
                (
                    token,
                    conn,
                    base_frame,
                    checkpoint_frame,
                ),
            ).fetchone()[0]
        )


        actual_normalization_losses = (
            db.execute(
                """
                SELECT count(*)

                FROM norm

                WHERE
                    token = ?
                    AND connection_id = ?
                    AND frame_sequence
                        IS NOT NULL
                    AND frame_sequence >= ?
                    AND frame_sequence <= ?
                    AND raw_match_count != 1
                """,
                (
                    token,
                    conn,
                    base_frame,
                    checkpoint_frame,
                ),
            ).fetchone()[0]
        )


        sequence_failures = (
            db.execute(
                """
                SELECT count(*)

                FROM bad_sequence_frames

                WHERE
                    connection_id = ?
                    AND frame_sequence >= ?
                    AND frame_sequence <= ?
                """,
                (
                    conn,
                    base_frame,
                    checkpoint_frame,
                ),
            ).fetchone()[0]
        )


        boundary_db_failures = (
            db.execute(
                """
                SELECT count(*)

                FROM norm

                WHERE
                    id IN (?, ?)
                    AND db_match_count != 1
                """,
                (
                    pair[
                        "base_norm_id"
                    ],
                    pair[
                        "checkpoint_norm_id"
                    ],
                ),
            ).fetchone()[0]
        )


        delta_db_failures = (
            db.execute(
                """
                SELECT count(*)

                FROM norm

                WHERE
                    kind = 'price_change'
                    AND token = ?
                    AND connection_id = ?
                    AND book_generation = ?
                    AND ingest_sequence > ?
                    AND ingest_sequence < ?
                    AND db_match_count != 1
                """,
                (
                    token,
                    conn,
                    pair[
                        "book_generation"
                    ],
                    pair[
                        "base_seq"
                    ],
                    pair[
                        "checkpoint_seq"
                    ],
                ),
            ).fetchone()[0]
        )


        raw_interval_counts = dict(
            db.execute(
                """
                SELECT
                    kind,
                    count(*)

                FROM raw_events

                WHERE
                    token = ?
                    AND connection_id = ?
                    AND frame_sequence > ?
                    AND frame_sequence <= ?

                GROUP BY kind

                ORDER BY kind
                """,
                (
                    token,
                    conn,
                    base_frame,
                    checkpoint_frame,
                ),
            ).fetchall()
        )


        raw_trade_count = int(
            raw_interval_counts.get(
                "last_trade_price",
                0,
            )
        )


        replay_delta_count = None
        mismatches = []
        exact_replay = None


        if raw_integrity_failures:
            classification = (
                "RAW_FRAME_INTEGRITY_FAILURE"
            )

        elif (
            parse_failures
            or expected_normalization_losses
            or actual_normalization_losses
            or sequence_failures
        ):
            classification = (
                "COLLECTOR_NORMALIZATION_LOSS"
            )

        elif (
            boundary_db_failures
            or delta_db_failures
        ):
            classification = (
                "NORMALIZED_TO_DB_LOSS"
            )

        else:
            (
                replay_delta_count,
                mismatches,
            ) = replay_pair(
                pair
            )

            exact_replay = (
                len(mismatches)
                == 0
            )

            if exact_replay:
                classification = (
                    "EXACT_REPLAY_MATCH"
                )

            elif raw_trade_count > 0:
                classification = (
                    "TRADE_BOOK_TRANSITION_SUPPORTED"
                )

            else:
                classification = (
                    "UNEXPLAINED_SOURCE_TRANSITION"
                )


        classification_counts[
            classification
        ] += 1


        if mismatches:
            mismatch_pair_count += 1

            for m in mismatches:
                mismatch_level_count += 1

                mismatch_type_counts[
                    m[
                        "mismatch_type"
                    ]
                ] += 1

                mismatch_direction_counts[
                    m[
                        "direction"
                    ]
                ] += 1

                mismatch_side_counts[
                    m["side"]
                ] += 1

                mismatch_record = {
                    "pair_index": pair[
                        "pair_index"
                    ],
                    "token": token,
                    "classification": (
                        classification
                    ),
                    "raw_trade_count": (
                        raw_trade_count
                    ),
                    **clean(m),
                }

                mismatch_fh.write(
                    json.dumps(
                        mismatch_record,
                        sort_keys=True,
                    )
                    + "\n"
                )


        pair_record = {
            **pair,
            "base_capture_time": (
                us_iso(
                    pair[
                        "base_capture_us"
                    ]
                )
            ),
            "checkpoint_capture_time": (
                us_iso(
                    pair[
                        "checkpoint_capture_us"
                    ]
                )
            ),
            "classification": classification,
            "raw_integrity_failures": (
                raw_integrity_failures
            ),
            "raw_parse_failures": (
                parse_failures
            ),
            "raw_expected_normalization_losses": (
                expected_normalization_losses
            ),
            "normalized_unmatched_to_raw": (
                actual_normalization_losses
            ),
            "sequence_failures": (
                sequence_failures
            ),
            "boundary_db_failures": (
                boundary_db_failures
            ),
            "delta_db_failures": (
                delta_db_failures
            ),
            "raw_interval_event_counts": (
                raw_interval_counts
            ),
            "raw_trade_count": (
                raw_trade_count
            ),
            "replayed_delta_count": (
                replay_delta_count
            ),
            "exact_replay": (
                exact_replay
            ),
            "mismatch_count": (
                len(mismatches)
            ),
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


        if i % 100 == 0:
            print(
                "pairs classified:",
                i,
                flush=True,
            )


if sum(
    classification_counts.values()
) != EXPECTED_ELIGIBLE_PAIRS:
    raise RuntimeError(
        "pair classification count "
        "does not equal frozen pair count"
    )


nonreproduction = (
    classification_counts[
        "EXACT_REPLAY_MATCH"
    ]
    == EXPECTED_ELIGIBLE_PAIRS
)


summary = {
    "version": (
        "H2-v2-M2f-DIAGNOSTIC-1"
    ),

    "milestone": (
        "H2-v2 M2f — fresh raw-to-DB "
        "quantity-reduction diagnosis"
    ),

    "capture_id": CAPTURE_ID,

    "analysis_window": {
        "minutes": 30,
        "start_inclusive": (
            START.isoformat()
        ),
        "end_inclusive": (
            END.isoformat()
        ),
    },

    "frozen_evidence": {
        "eligible_pairs": (
            EXPECTED_ELIGIBLE_PAIRS
        ),
        "distinct_tokens": (
            EXPECTED_ELIGIBLE_TOKENS
        ),
    },

    "raw_frame_integrity": {
        "frames": raw_frame_count,
        "hash_failures": (
            raw_hash_failures
        ),
        "parse_failures": (
            raw_parse_failures
        ),
    },

    "raw_to_normalized": {
        "expected_h2_v2_events": (
            raw_event_count
        ),
        "expected_exactly_matched": (
            raw_expected_matched
        ),
        "expected_non_exact": (
            raw_event_count
            - raw_expected_matched
        ),
        "normalized_ws_records": (
            norm_ws_total
        ),
        "normalized_exactly_matched": (
            norm_ws_matched
        ),
        "normalized_non_exact": (
            norm_ws_total
            - norm_ws_matched
        ),
        "bad_sequence_frames": (
            bad_sequence_frame_count
        ),
    },

    "normalized_to_db": {
        "ws_book": (
            db_book_mapping
        ),
        "price_change": (
            db_delta_mapping
        ),
        "db_ws_book_rows_indexed": (
            book_db_rows
        ),
        "db_price_change_rows_indexed": (
            delta_db_rows
        ),
    },

    "pair_classifications": dict(
        sorted(
            classification_counts.items()
        )
    ),

    "replay": {
        "mismatch_pairs": (
            mismatch_pair_count
        ),
        "mismatch_levels": (
            mismatch_level_count
        ),
        "mismatch_types": dict(
            sorted(
                mismatch_type_counts.items()
            )
        ),
        "mismatch_directions": dict(
            sorted(
                mismatch_direction_counts.items()
            )
        ),
        "mismatch_sides": dict(
            sorted(
                mismatch_side_counts.items()
            )
        ),
    },

    "frozen_nonreproduction_rule": {
        "triggered": nonreproduction,
        "classification_if_triggered": (
            "FRESH_DIVERGENCE_NOT_REPRODUCED"
        ),
    },

    "classification_priority": [
        "RAW_FRAME_INTEGRITY_FAILURE",
        "COLLECTOR_NORMALIZATION_LOSS",
        "NORMALIZED_TO_DB_LOSS",
        "EXACT_REPLAY_MATCH",
        "TRADE_BOOK_TRANSITION_SUPPORTED",
        "UNEXPLAINED_SOURCE_TRANSITION",
    ],

    "input_hashes": {
        **EXPECTED_HASHES,
        "capture_manifest": (
            EXPECTED_CAPTURE_HASHES[
                "manifest"
            ]
        ),
        "capture_raw": (
            EXPECTED_CAPTURE_HASHES[
                "raw"
            ]
        ),
        "capture_normalized": (
            EXPECTED_CAPTURE_HASHES[
                "normalized"
            ]
        ),
    },

    "protocol_deviation": {
        "capture_overrun_data_used": False,
        "records_after_30_minutes_used": False,
    },

    "performance_data_inspected": False,
    "resolutions_inspected": False,
    "edge_calculated": False,
    "trades_simulated": False,
    "pnl_calculated": False,
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
    "=== H2-v2 M2f DIAGNOSTIC RESULT ==="
)

print(
    "eligible pairs:",
    EXPECTED_ELIGIBLE_PAIRS,
)

print(
    "distinct tokens:",
    EXPECTED_ELIGIBLE_TOKENS,
)

print()
print(
    "RAW FRAME INTEGRITY"
)

print(
    "frames:",
    raw_frame_count,
)

print(
    "hash failures:",
    raw_hash_failures,
)

print(
    "parse failures:",
    raw_parse_failures,
)

print()
print(
    "RAW -> NORMALIZED"
)

print(
    "expected events:",
    raw_event_count,
)

print(
    "expected exact:",
    raw_expected_matched,
)

print(
    "normalized WS:",
    norm_ws_total,
)

print(
    "normalized exact:",
    norm_ws_matched,
)

print(
    "bad sequence frames:",
    bad_sequence_frame_count,
)

print()
print(
    "NORMALIZED -> DB"
)

print(
    "ws_book:",
    db_book_mapping,
)

print(
    "price_change:",
    db_delta_mapping,
)

print()
print(
    "PAIR CLASSIFICATIONS"
)

for key in (
    "RAW_FRAME_INTEGRITY_FAILURE",
    "COLLECTOR_NORMALIZATION_LOSS",
    "NORMALIZED_TO_DB_LOSS",
    "EXACT_REPLAY_MATCH",
    "TRADE_BOOK_TRANSITION_SUPPORTED",
    "UNEXPLAINED_SOURCE_TRANSITION",
):
    print(
        f"{key}:",
        classification_counts[
            key
        ],
    )

print()
print(
    "REPLAY MISMATCHES"
)

print(
    "mismatch pairs:",
    mismatch_pair_count,
)

print(
    "mismatch levels:",
    mismatch_level_count,
)

print(
    "mismatch types:",
    dict(
        mismatch_type_counts
    ),
)

print(
    "mismatch directions:",
    dict(
        mismatch_direction_counts
    ),
)

print(
    "mismatch sides:",
    dict(
        mismatch_side_counts
    ),
)

print()
print(
    "fresh divergence not reproduced:",
    nonreproduction,
)

print()
print(
    "pair_output_sha256:",
    sha_file(
        PAIR_OUT
    ),
)

print(
    "mismatch_output_sha256:",
    sha_file(
        MISMATCH_OUT
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
    "NO EDGE / NO TRADES / NO PNL"
)


db.close()

try:
    DB_PATH.unlink()
except FileNotFoundError:
    pass
