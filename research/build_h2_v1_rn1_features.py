"""Build and freeze H2-v1 RN1 pregame feature artifacts.

Parent experiment:
    research/h2_v1_cohort.csv

Outputs:
    research/h2_v1_rn1_pregame_tape.csv
    research/h2_v1_rn1_features.csv
    research/h2_v1_rn1_manifest.json

The source wallet tape contains 3,626 complete RN1 fills, but only fills strictly
before each market's frozen canonical commencement time are eligible for H2-v1
pregame features.

Directional convention, expressed on a HOME-team axis:

    BUY home   -> +1
    SELL away  -> +1
    SELL home  -> -1
    BUY away   -> -1

This measures CHANGE IN DIRECTIONAL EXPOSURE inferred from observed fills.
It is not an estimate of RN1's absolute position because starting inventory is
unknown.

Primary directional unit:
    shares

Secondary activity/cash-flow unit:
    executed cash = shares * execution price

Exact-T0 trades are excluded conservatively.
Post-start trades are excluded from H2-v1 pregame features.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal, getcontext
from pathlib import Path

import psycopg2
from dotenv import dotenv_values


getcontext().prec = 40


REPO = Path(__file__).resolve().parents[1]
ENV = dotenv_values(REPO / ".env")

COHORT_CSV = REPO / "research" / "h2_v1_cohort.csv"
COHORT_MANIFEST = REPO / "research" / "h2_v1_manifest.json"

TAPE_CSV = REPO / "research" / "h2_v1_rn1_pregame_tape.csv"
FEATURES_CSV = REPO / "research" / "h2_v1_rn1_features.csv"
MANIFEST_JSON = REPO / "research" / "h2_v1_rn1_manifest.json"

COHORT_VERSION = "H2-v1"
FEATURE_VERSION = "H2-v1-RN1-M2"
WALLET = "RN1"
VENUE_ID = 1

EXPECTED_MARKETS = 42
EXPECTED_SOURCE_FILLS = 3626
EXPECTED_PREGAME_FILLS = 213
EXPECTED_EXACT_T0_FILLS = 3
EXPECTED_POST_START_FILLS = 3410
EXPECTED_PREGAME_ACTIVE_MARKETS = 26

EXPECTED_WINDOW_FILL_COUNTS = {
    "all_pregame": 213,
    "6h": 171,
    "60m": 158,
    "30m": 113,
    "10m": 94,
    "5m": 94,
}

WINDOWS = [
    ("all_pregame", None),
    ("6h", 6 * 60 * 60),
    ("60m", 60 * 60),
    ("30m", 30 * 60),
    ("10m", 10 * 60),
    ("5m", 5 * 60),
]

TAPE_FIELDS = [
    "feature_version",
    "cohort_version",
    "market_id",
    "condition_id",
    "slug",
    "home_team",
    "away_team",
    "canonical_commence_time",

    "token_id",
    "token_outcome",
    "token_team_side",
    "outcome_index",

    "event_time",
    "seconds_before_start",

    "side",
    "price",
    "size_shares",
    "cash_notional",

    "home_direction_sign",
    "signed_home_shares",
    "signed_home_cash_flow",

    "tx_hash",
    "venue_trade_key",

    "in_6h",
    "in_60m",
    "in_30m",
    "in_10m",
    "in_5m",
]

FEATURE_FIELDS = [
    "feature_version",
    "cohort_version",
    "market_id",
    "condition_id",
    "slug",
    "home_team",
    "away_team",
    "canonical_commence_time",
    "include_broad",
    "include_tight",

    "window",
    "window_seconds",
    "window_start",

    "has_activity",
    "fill_count",
    "unique_tx_count",

    "buy_fill_count",
    "sell_fill_count",

    "home_buy_fill_count",
    "home_sell_fill_count",
    "away_buy_fill_count",
    "away_sell_fill_count",

    "home_directed_fill_count",
    "away_directed_fill_count",

    "gross_shares",
    "gross_cash_notional",

    "home_directed_shares",
    "away_directed_shares",
    "signed_home_shares",

    "home_directed_cash_flow",
    "away_directed_cash_flow",
    "signed_home_cash_flow",

    "direction_share_score",
    "direction_cash_score",

    "first_trade_time",
    "last_trade_time",
    "last_trade_seconds_before_start",
]


def connect():
    password = ENV.get("PG_PASSWORD")

    if not password:
        raise RuntimeError("PG_PASSWORD missing from repository .env")

    return psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="quantlab",
        user="quantlab",
        password=password,
    )


def iso(value):
    if value is None:
        return ""

    return value.astimezone(timezone.utc).isoformat()


def bool_text(value):
    return "true" if bool(value) else "false"


def norm(value):
    return "".join(
        ch.lower()
        for ch in str(value or "")
        if ch.isalnum()
    )


def dstr(value: Decimal) -> str:
    """Stable non-scientific Decimal output."""
    if value == 0:
        return "0"

    text = format(value.normalize(), "f")

    if "." in text:
        text = text.rstrip("0").rstrip(".")

    return text


def score_str(numerator: Decimal, denominator: Decimal) -> str:
    if denominator == 0:
        return "0"

    value = numerator / denominator

    # Enough precision for research features without unstable gigantic strings.
    return format(value, ".12f").rstrip("0").rstrip(".")


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_parent_cohort():
    if not COHORT_CSV.exists():
        raise RuntimeError(f"Missing parent cohort: {COHORT_CSV}")

    if not COHORT_MANIFEST.exists():
        raise RuntimeError(f"Missing parent manifest: {COHORT_MANIFEST}")

    manifest = json.loads(COHORT_MANIFEST.read_text())

    actual_hash = sha256_file(COHORT_CSV)
    expected_hash = manifest.get("csv_sha256")

    if actual_hash != expected_hash:
        raise RuntimeError(
            "Parent H2-v1 cohort hash differs from its manifest"
        )

    if manifest.get("cohort") != COHORT_VERSION:
        raise RuntimeError(
            f"Unexpected parent cohort {manifest.get('cohort')!r}"
        )

    with COHORT_CSV.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if len(rows) != EXPECTED_MARKETS:
        raise RuntimeError(
            f"Expected {EXPECTED_MARKETS} frozen markets, got {len(rows)}"
        )

    return rows, actual_hash


def load_tokens(cur, market_id, home_team, away_team):
    cur.execute(
        """
        SELECT
            id,
            outcome,
            outcome_index
        FROM tokens
        WHERE market_id = %s
        ORDER BY outcome_index, id
        """,
        (market_id,),
    )

    rows = cur.fetchall()

    if len(rows) != 2:
        raise RuntimeError(
            f"market_id={market_id}: expected two tokens, got {len(rows)}"
        )

    home_norm = norm(home_team)
    away_norm = norm(away_team)

    result = {}

    for token_id, outcome, outcome_index in rows:
        outcome_norm = norm(outcome)

        if outcome_norm == home_norm:
            team_side = "home"
        elif outcome_norm == away_norm:
            team_side = "away"
        else:
            raise RuntimeError(
                f"market_id={market_id}: token outcome {outcome!r} "
                f"matches neither home={home_team!r} nor away={away_team!r}"
            )

        result[token_id] = {
            "outcome": outcome,
            "outcome_index": outcome_index,
            "team_side": team_side,
        }

    if {x["team_side"] for x in result.values()} != {"home", "away"}:
        raise RuntimeError(
            f"market_id={market_id}: failed unique home/away token mapping"
        )

    return result


def load_trades(cur, condition_id):
    cur.execute(
        """
        SELECT
            token_id,
            event_time,
            price_mc,
            size,
            side,
            outcome,
            tx_hash,
            venue_trade_key
        FROM trades
        WHERE venue_id = %s
          AND wallet = %s
          AND condition_id = %s
        ORDER BY event_time, venue_trade_key
        """,
        (VENUE_ID, WALLET, condition_id),
    )

    return cur.fetchall()


def direction_sign(team_side, side):
    side = str(side or "").upper()

    if team_side == "home" and side == "BUY":
        return 1

    if team_side == "away" and side == "SELL":
        return 1

    if team_side == "home" and side == "SELL":
        return -1

    if team_side == "away" and side == "BUY":
        return -1

    raise RuntimeError(
        f"Cannot derive direction for team_side={team_side!r} side={side!r}"
    )


def in_window(seconds_before_start, seconds):
    if seconds_before_start <= 0:
        return False

    if seconds is None:
        return True

    return seconds_before_start <= seconds


def build_source_tape(conn, cohort_rows):
    tape = []
    timing = Counter()
    per_market_pregame = Counter()

    with conn.cursor() as cur:
        for cohort in cohort_rows:
            market_id = int(cohort["market_id"])
            condition_id = cohort["condition_id"]
            slug = cohort["slug"]
            home_team = cohort["home_team"]
            away_team = cohort["away_team"]
            t0 = datetime.fromisoformat(
                cohort["canonical_commence_time"]
            )

            tokens = load_tokens(
                cur,
                market_id,
                home_team,
                away_team,
            )

            trades = load_trades(
                cur,
                condition_id,
            )

            expected_market_fills = int(
                cohort["rn1_trade_count"]
            )

            if len(trades) != expected_market_fills:
                raise RuntimeError(
                    f"{slug}: frozen RN1 count={expected_market_fills}, "
                    f"DB={len(trades)}"
                )

            for (
                token_id,
                event_time,
                price_mc,
                size,
                side,
                trade_outcome,
                tx_hash,
                venue_trade_key,
            ) in trades:

                timing["source"] += 1

                if token_id not in tokens:
                    raise RuntimeError(
                        f"{slug}: token_id={token_id} not in frozen market"
                    )

                token = tokens[token_id]

                side = str(side or "").upper()

                if side not in {"BUY", "SELL"}:
                    raise RuntimeError(
                        f"{slug}: invalid side {side!r}"
                    )

                if (
                    trade_outcome is not None
                    and norm(trade_outcome) != norm(token["outcome"])
                ):
                    raise RuntimeError(
                        f"{slug}: trade outcome {trade_outcome!r} "
                        f"!= token outcome {token['outcome']!r}"
                    )

                seconds_before_start = (
                    t0 - event_time
                ).total_seconds()

                if seconds_before_start > 0:
                    timing["pregame"] += 1
                    per_market_pregame[market_id] += 1

                elif seconds_before_start == 0:
                    timing["exact_t0"] += 1
                    continue

                else:
                    timing["post_start"] += 1
                    continue

                size = Decimal(size)
                price = Decimal(int(price_mc)) / Decimal(1000)
                cash_notional = size * price

                sign = direction_sign(
                    token["team_side"],
                    side,
                )

                signed_shares = (
                    size if sign > 0 else -size
                )

                signed_cash = (
                    cash_notional
                    if sign > 0
                    else -cash_notional
                )

                tape.append(
                    {
                        "feature_version": FEATURE_VERSION,
                        "cohort_version": COHORT_VERSION,
                        "market_id": market_id,
                        "condition_id": condition_id,
                        "slug": slug,
                        "home_team": home_team,
                        "away_team": away_team,
                        "canonical_commence_time": iso(t0),

                        "token_id": token_id,
                        "token_outcome": token["outcome"] or "",
                        "token_team_side": token["team_side"],
                        "outcome_index": (
                            ""
                            if token["outcome_index"] is None
                            else token["outcome_index"]
                        ),

                        "event_time": iso(event_time),
                        "seconds_before_start": dstr(
                            Decimal(str(seconds_before_start))
                        ),

                        "side": side,
                        "price": dstr(price),
                        "size_shares": dstr(size),
                        "cash_notional": dstr(cash_notional),

                        "home_direction_sign": sign,
                        "signed_home_shares": dstr(signed_shares),
                        "signed_home_cash_flow": dstr(signed_cash),

                        "tx_hash": tx_hash or "",
                        "venue_trade_key": venue_trade_key,

                        "in_6h": bool_text(
                            in_window(seconds_before_start, 6 * 60 * 60)
                        ),
                        "in_60m": bool_text(
                            in_window(seconds_before_start, 60 * 60)
                        ),
                        "in_30m": bool_text(
                            in_window(seconds_before_start, 30 * 60)
                        ),
                        "in_10m": bool_text(
                            in_window(seconds_before_start, 10 * 60)
                        ),
                        "in_5m": bool_text(
                            in_window(seconds_before_start, 5 * 60)
                        ),

                        # private calculation helpers; stripped before CSV
                        "_event_time": event_time,
                        "_size": size,
                        "_cash": cash_notional,
                        "_sign": sign,
                        "_team_side": token["team_side"],
                    }
                )

    if timing["source"] != EXPECTED_SOURCE_FILLS:
        raise RuntimeError(
            f"Source fill count changed: expected "
            f"{EXPECTED_SOURCE_FILLS}, got {timing['source']}"
        )

    if timing["pregame"] != EXPECTED_PREGAME_FILLS:
        raise RuntimeError(
            f"Pregame fill count changed: expected "
            f"{EXPECTED_PREGAME_FILLS}, got {timing['pregame']}"
        )

    if timing["exact_t0"] != EXPECTED_EXACT_T0_FILLS:
        raise RuntimeError(
            f"Exact-T0 fill count changed: expected "
            f"{EXPECTED_EXACT_T0_FILLS}, got {timing['exact_t0']}"
        )

    if timing["post_start"] != EXPECTED_POST_START_FILLS:
        raise RuntimeError(
            f"Post-start fill count changed: expected "
            f"{EXPECTED_POST_START_FILLS}, got {timing['post_start']}"
        )

    active_markets = sum(
        count > 0
        for count in per_market_pregame.values()
    )

    if active_markets != EXPECTED_PREGAME_ACTIVE_MARKETS:
        raise RuntimeError(
            f"Pregame-active markets changed: expected "
            f"{EXPECTED_PREGAME_ACTIVE_MARKETS}, got {active_markets}"
        )

    tape.sort(
        key=lambda row: (
            row["canonical_commence_time"],
            row["slug"],
            row["_event_time"],
            row["venue_trade_key"],
        )
    )

    return tape, timing, per_market_pregame


def select_window(tape_rows, market_id, seconds):
    result = []

    for row in tape_rows:
        if int(row["market_id"]) != market_id:
            continue

        seconds_before = Decimal(
            row["seconds_before_start"]
        )

        if seconds is None or seconds_before <= seconds:
            result.append(row)

    return result


def aggregate_window(cohort, rows, label, seconds):
    t0 = datetime.fromisoformat(
        cohort["canonical_commence_time"]
    )

    gross_shares = Decimal(0)
    gross_cash = Decimal(0)

    home_directed_shares = Decimal(0)
    away_directed_shares = Decimal(0)

    home_directed_cash = Decimal(0)
    away_directed_cash = Decimal(0)

    counters = Counter()
    tx_hashes = set()

    event_times = []

    for row in rows:
        size = row["_size"]
        cash = row["_cash"]
        sign = row["_sign"]
        team_side = row["_team_side"]
        side = row["side"]

        gross_shares += size
        gross_cash += cash

        counters["fills"] += 1
        counters[side.lower()] += 1
        counters[f"{team_side}_{side.lower()}"] += 1

        if sign > 0:
            counters["home_directed"] += 1
            home_directed_shares += size
            home_directed_cash += cash
        else:
            counters["away_directed"] += 1
            away_directed_shares += size
            away_directed_cash += cash

        if row["tx_hash"]:
            tx_hashes.add(row["tx_hash"])
        else:
            # Preserve uniqueness if tx hash happens to be absent.
            tx_hashes.add(
                "venue_trade_key:" + row["venue_trade_key"]
            )

        event_times.append(row["_event_time"])

    signed_home_shares = (
        home_directed_shares
        - away_directed_shares
    )

    signed_home_cash = (
        home_directed_cash
        - away_directed_cash
    )

    first_time = min(event_times) if event_times else None
    last_time = max(event_times) if event_times else None

    if last_time is None:
        last_seconds = ""
    else:
        last_seconds = dstr(
            Decimal(
                str(
                    (
                        t0 - last_time
                    ).total_seconds()
                )
            )
        )

    if seconds is None:
        window_seconds = ""
        window_start = ""
    else:
        window_seconds = seconds
        window_start = iso(
            t0 - timedelta(seconds=seconds)
        )

    return {
        "feature_version": FEATURE_VERSION,
        "cohort_version": COHORT_VERSION,
        "market_id": int(cohort["market_id"]),
        "condition_id": cohort["condition_id"],
        "slug": cohort["slug"],
        "home_team": cohort["home_team"],
        "away_team": cohort["away_team"],
        "canonical_commence_time": cohort[
            "canonical_commence_time"
        ],
        "include_broad": cohort["include_broad"],
        "include_tight": cohort["include_tight"],

        "window": label,
        "window_seconds": window_seconds,
        "window_start": window_start,

        "has_activity": bool_text(bool(rows)),
        "fill_count": counters["fills"],
        "unique_tx_count": len(tx_hashes),

        "buy_fill_count": counters["buy"],
        "sell_fill_count": counters["sell"],

        "home_buy_fill_count": counters["home_buy"],
        "home_sell_fill_count": counters["home_sell"],
        "away_buy_fill_count": counters["away_buy"],
        "away_sell_fill_count": counters["away_sell"],

        "home_directed_fill_count": counters["home_directed"],
        "away_directed_fill_count": counters["away_directed"],

        "gross_shares": dstr(gross_shares),
        "gross_cash_notional": dstr(gross_cash),

        "home_directed_shares": dstr(home_directed_shares),
        "away_directed_shares": dstr(away_directed_shares),
        "signed_home_shares": dstr(signed_home_shares),

        "home_directed_cash_flow": dstr(home_directed_cash),
        "away_directed_cash_flow": dstr(away_directed_cash),
        "signed_home_cash_flow": dstr(signed_home_cash),

        "direction_share_score": score_str(
            signed_home_shares,
            gross_shares,
        ),
        "direction_cash_score": score_str(
            signed_home_cash,
            gross_cash,
        ),

        "first_trade_time": iso(first_time),
        "last_trade_time": iso(last_time),
        "last_trade_seconds_before_start": last_seconds,
    }


def build_features(cohort_rows, tape):
    rows = []

    for cohort in cohort_rows:
        market_id = int(cohort["market_id"])

        for label, seconds in WINDOWS:
            selected = select_window(
                tape,
                market_id,
                seconds,
            )

            rows.append(
                aggregate_window(
                    cohort,
                    selected,
                    label,
                    seconds,
                )
            )

    window_order = {
        label: i
        for i, (label, _) in enumerate(WINDOWS)
    }

    rows.sort(
        key=lambda row: (
            row["canonical_commence_time"],
            row["slug"],
            window_order[row["window"]],
        )
    )

    return rows


def validate_feature_rows(rows):
    expected_rows = EXPECTED_MARKETS * len(WINDOWS)

    if len(rows) != expected_rows:
        raise RuntimeError(
            f"Expected {expected_rows} feature rows, got {len(rows)}"
        )

    per_window = Counter()

    market_window_pairs = set()

    for row in rows:
        key = (
            int(row["market_id"]),
            row["window"],
        )

        if key in market_window_pairs:
            raise RuntimeError(
                f"Duplicate feature key {key}"
            )

        market_window_pairs.add(key)

        per_window[row["window"]] += int(
            row["fill_count"]
        )

        # Arithmetic invariants.
        if (
            int(row["buy_fill_count"])
            + int(row["sell_fill_count"])
            != int(row["fill_count"])
        ):
            raise RuntimeError(
                f"{row['slug']} {row['window']}: BUY+SELL != fills"
            )

        if (
            int(row["home_directed_fill_count"])
            + int(row["away_directed_fill_count"])
            != int(row["fill_count"])
        ):
            raise RuntimeError(
                f"{row['slug']} {row['window']}: "
                "direction counts != fills"
            )

        gross_shares = Decimal(
            row["gross_shares"]
        )

        home_shares = Decimal(
            row["home_directed_shares"]
        )

        away_shares = Decimal(
            row["away_directed_shares"]
        )

        signed_shares = Decimal(
            row["signed_home_shares"]
        )

        if gross_shares != home_shares + away_shares:
            raise RuntimeError(
                f"{row['slug']} {row['window']}: "
                "gross share arithmetic failure"
            )

        if signed_shares != home_shares - away_shares:
            raise RuntimeError(
                f"{row['slug']} {row['window']}: "
                "signed share arithmetic failure"
            )

        score = Decimal(
            row["direction_share_score"]
        )

        if score < Decimal("-1") or score > Decimal("1"):
            raise RuntimeError(
                f"{row['slug']} {row['window']}: "
                f"direction score outside [-1,1]"
            )

        has_activity = (
            row["has_activity"] == "true"
        )

        if has_activity != (
            int(row["fill_count"]) > 0
        ):
            raise RuntimeError(
                f"{row['slug']} {row['window']}: "
                "has_activity inconsistent with fill_count"
            )

    for label, expected in EXPECTED_WINDOW_FILL_COUNTS.items():
        observed = per_window[label]

        if observed != expected:
            raise RuntimeError(
                f"{label} fill count changed: "
                f"expected {expected}, got {observed}"
            )


def csv_payload(rows, fields):
    buffer = io.StringIO(newline="")

    writer = csv.DictWriter(
        buffer,
        fieldnames=fields,
        lineterminator="\n",
    )

    writer.writeheader()

    for source in rows:
        row = {
            key: source.get(key, "")
            for key in fields
        }

        writer.writerow(row)

    return buffer.getvalue().encode("utf-8")


def write_outputs(
    cohort_rows,
    parent_hash,
    tape,
    features,
    timing,
):
    validate_feature_rows(features)

    if len(tape) != EXPECTED_PREGAME_FILLS:
        raise RuntimeError(
            f"Expected {EXPECTED_PREGAME_FILLS} tape rows, "
            f"got {len(tape)}"
        )

    tape_payload = csv_payload(
        tape,
        TAPE_FIELDS,
    )

    feature_payload = csv_payload(
        features,
        FEATURE_FIELDS,
    )

    TAPE_CSV.write_bytes(tape_payload)
    FEATURES_CSV.write_bytes(feature_payload)

    tape_hash = sha256_file(TAPE_CSV)
    feature_hash = sha256_file(FEATURES_CSV)

    active_by_window = {}

    for label, _ in WINDOWS:
        active_by_window[label] = sum(
            row["window"] == label
            and row["has_activity"] == "true"
            for row in features
        )

    zero_pregame_markets = [
        {
            "market_id": int(cohort["market_id"]),
            "slug": cohort["slug"],
        }
        for cohort in cohort_rows
        if not any(
            int(row["market_id"]) == int(cohort["market_id"])
            and row["window"] == "all_pregame"
            and row["has_activity"] == "true"
            for row in features
        )
    ]

    manifest = {
        "feature_version": FEATURE_VERSION,
        "parent_cohort": COHORT_VERSION,
        "parent_cohort_csv_sha256": parent_hash,
        "wallet": WALLET,

        "built_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),

        "timing_policy": {
            "clock": "trades.event_time",
            "pregame": "event_time < canonical_commence_time",
            "exact_t0": "excluded",
            "post_start": "excluded",
        },

        "direction_policy": {
            "reference_axis": "home_team",
            "positive": [
                "BUY home token",
                "SELL away token",
            ],
            "negative": [
                "SELL home token",
                "BUY away token",
            ],
            "primary_unit": "shares",
            "cash_flow_unit": "size_shares * execution_price",
            "interpretation": (
                "Observed change in directional exposure, not absolute "
                "wallet position; starting inventory is unknown."
            ),
        },

        "windows": {
            "all_pregame": "(-infinity, T0)",
            "6h": "[T0-6h, T0)",
            "60m": "[T0-60m, T0)",
            "30m": "[T0-30m, T0)",
            "10m": "[T0-10m, T0)",
            "5m": "[T0-5m, T0)",
        },

        "source_fill_counts": {
            "all": timing["source"],
            "pregame": timing["pregame"],
            "exact_t0_excluded": timing["exact_t0"],
            "post_start_excluded": timing["post_start"],
        },

        "window_fill_counts": dict(
            EXPECTED_WINDOW_FILL_COUNTS
        ),

        "market_counts": {
            "cohort": EXPECTED_MARKETS,
            "pregame_active": (
                EXPECTED_PREGAME_ACTIVE_MARKETS
            ),
            "pregame_zero": (
                EXPECTED_MARKETS
                - EXPECTED_PREGAME_ACTIVE_MARKETS
            ),
            "active_by_window": active_by_window,
        },

        "zero_pregame_markets": zero_pregame_markets,

        "artifacts": {
            "pregame_tape": {
                "file": TAPE_CSV.name,
                "rows": len(tape),
                "sha256": tape_hash,
            },
            "features": {
                "file": FEATURES_CSV.name,
                "rows": len(features),
                "sha256": feature_hash,
            },
        },
    }

    MANIFEST_JSON.write_text(
        json.dumps(
            manifest,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )

    return tape_hash, feature_hash


def read_csv(path):
    with path.open(
        newline="",
        encoding="utf-8",
    ) as f:
        return list(csv.DictReader(f))


def validate_artifacts():
    if not MANIFEST_JSON.exists():
        raise RuntimeError(
            f"Missing {MANIFEST_JSON}"
        )

    manifest = json.loads(
        MANIFEST_JSON.read_text()
    )

    cohort_rows, parent_hash = (
        verify_parent_cohort()
    )

    if (
        manifest.get("parent_cohort_csv_sha256")
        != parent_hash
    ):
        raise RuntimeError(
            "M2 manifest points to a different H2-v1 cohort hash"
        )

    tape = read_csv(TAPE_CSV)
    features = read_csv(FEATURES_CSV)

    if len(tape) != EXPECTED_PREGAME_FILLS:
        raise RuntimeError(
            f"Expected {EXPECTED_PREGAME_FILLS} frozen tape rows, "
            f"got {len(tape)}"
        )

    validate_feature_rows(features)

    expected_tape_hash = manifest[
        "artifacts"
    ]["pregame_tape"]["sha256"]

    expected_features_hash = manifest[
        "artifacts"
    ]["features"]["sha256"]

    tape_hash = sha256_file(TAPE_CSV)
    features_hash = sha256_file(FEATURES_CSV)

    if tape_hash != expected_tape_hash:
        raise RuntimeError(
            "Pregame tape SHA-256 mismatch"
        )

    if features_hash != expected_features_hash:
        raise RuntimeError(
            "RN1 feature SHA-256 mismatch"
        )

    return (
        cohort_rows,
        tape,
        features,
        tape_hash,
        features_hash,
    )


def print_summary(
    tape,
    features,
    tape_hash,
    features_hash,
):
    print()
    print("H2-v1 RN1 pregame features")
    print("==========================")
    print(f"eligible pregame fills: {len(tape)}")
    print(f"feature rows:           {len(features)}")

    print()
    print("window summary:")

    for label, _ in WINDOWS:
        rows = [
            row
            for row in features
            if row["window"] == label
        ]

        fills = sum(
            int(row["fill_count"])
            for row in rows
        )

        active = sum(
            row["has_activity"] == "true"
            for row in rows
        )

        print(
            f"  {label:11s} "
            f"fills={fills:3d} "
            f"active_markets={active:2d}"
        )

    print()
    print(f"pregame tape SHA-256: {tape_hash}")
    print(f"features SHA-256:     {features_hash}")


def main():
    parser = argparse.ArgumentParser()

    mode = parser.add_mutually_exclusive_group(
        required=True
    )

    mode.add_argument(
        "--build",
        action="store_true",
        help="Create frozen H2-v1 RN1 M2 artifacts",
    )

    mode.add_argument(
        "--validate",
        action="store_true",
        help="Validate existing frozen M2 artifacts",
    )

    args = parser.parse_args()

    if args.build:
        existing = [
            path
            for path in (
                TAPE_CSV,
                FEATURES_CSV,
                MANIFEST_JSON,
            )
            if path.exists()
        ]

        if existing:
            raise SystemExit(
                "REFUSING TO RE-BUILD: M2 artifact(s) already exist: "
                + ", ".join(str(path) for path in existing)
                + ". Use --validate instead."
            )

        cohort_rows, parent_hash = (
            verify_parent_cohort()
        )

        conn = connect()

        try:
            tape, timing, _ = build_source_tape(
                conn,
                cohort_rows,
            )
        finally:
            conn.close()

        features = build_features(
            cohort_rows,
            tape,
        )

        tape_hash, feature_hash = write_outputs(
            cohort_rows,
            parent_hash,
            tape,
            features,
            timing,
        )

        print_summary(
            tape,
            features,
            tape_hash,
            feature_hash,
        )

        print()
        print("BUILD COMPLETE")
        print(f"  {TAPE_CSV}")
        print(f"  {FEATURES_CSV}")
        print(f"  {MANIFEST_JSON}")

        return

    (
        _cohort_rows,
        tape,
        features,
        tape_hash,
        feature_hash,
    ) = validate_artifacts()

    print_summary(
        tape,
        features,
        tape_hash,
        feature_hash,
    )

    print()
    print("VALIDATION PASSED")


if __name__ == "__main__":
    main()
