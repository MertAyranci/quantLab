"""Freeze H2-v1 Polymarket executed-price response table.

Parents
-------
research/h2_v1_cohort.csv
research/h2_v1_rn1_pregame_tape.csv
research/h2_v1_sharp_tape.csv

Outputs
-------
research/h2_v1_pm_pregame_tape.csv
research/h2_v1_pm_responses.csv
research/h2_v1_pm_manifest.json

Scientific timing policy
------------------------
RN1 signal:
    frozen M2 trades.event_time

Sharp signal:
    frozen M3 observed_at

Polymarket source:
    public market-wide execution tape from Data API.

Public trade timestamps have one-second resolution. To avoid accidental
look-ahead around a sub-second sharp observation, the baseline is the last
execution strictly before the completed second containing the signal.

Responses must have execution timestamp strictly greater than signal_time.

No response window may cross canonical T0.

This is an executed-price response study, not an order-book or executable-PnL
study. Bid/ask, depth and liquidity are unavailable for H2-v1 and are handled
as a research limitation.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, getcontext
from pathlib import Path

import httpx


getcontext().prec = 40

REPO = Path(__file__).resolve().parents[1]

COHORT_CSV = REPO / "research/h2_v1_cohort.csv"
COHORT_MANIFEST = REPO / "research/h2_v1_manifest.json"

RN1_CSV = REPO / "research/h2_v1_rn1_pregame_tape.csv"
RN1_MANIFEST = REPO / "research/h2_v1_rn1_manifest.json"

SHARP_CSV = REPO / "research/h2_v1_sharp_tape.csv"
SHARP_MANIFEST = REPO / "research/h2_v1_sharp_manifest.json"

PM_TAPE_CSV = REPO / "research/h2_v1_pm_pregame_tape.csv"
RESPONSES_CSV = REPO / "research/h2_v1_pm_responses.csv"
MANIFEST_JSON = REPO / "research/h2_v1_pm_manifest.json"

DATA_API = "https://data-api.polymarket.com"

FEATURE_VERSION = "H2-v1-PM-M4"
COHORT_VERSION = "H2-v1"

EXPECTED_MARKETS = 42
EXPECTED_RN1_SIGNALS = 213
EXPECTED_SHARP_SIGNALS = 548
EXPECTED_PM_PREGAME_TRADES = 16047
EXPECTED_RESPONSE_ROWS = (
    EXPECTED_RN1_SIGNALS + EXPECTED_SHARP_SIGNALS
) * 4

HORIZONS = [10, 30, 60, 300]

EXPECTED_HORIZON_COUNTS = {
    "rn1": {
        10: (203, 141),
        30: (185, 153),
        60: (135, 111),
        300: (119, 117),
    },
    "sharp": {
        10: (548, 21),
        30: (548, 56),
        60: (547, 84),
        300: (546, 211),
    },
}

TAPE_FIELDS = [
    "feature_version",
    "cohort_version",
    "market_id",
    "condition_id",
    "slug",
    "home_team",
    "away_team",
    "canonical_commence_time",

    "market_trade_index",
    "event_time",
    "event_time_unix",

    "home_price",
    "token_price",
    "outcome",
    "token_team_side",

    "side",
    "size",
    "wallet",
    "tx_hash",
]

RESPONSE_FIELDS = [
    "feature_version",
    "cohort_version",

    "signal_id",
    "signal_type",
    "signal_parent_row",

    "market_id",
    "condition_id",
    "slug",
    "home_team",
    "away_team",
    "canonical_commence_time",

    "signal_time",
    "signal_seconds_before_start",
    "signal_tx_hash",

    "horizon_seconds",
    "target_time",
    "pregame_eligible",

    "baseline_available",
    "baseline_time",
    "baseline_age_seconds",
    "baseline_home_price",
    "baseline_tx_hash",

    "baseline_fresh_30s",
    "baseline_fresh_60s",
    "baseline_fresh_300s",
    "baseline_fresh_600s",

    "response_available",
    "response_trade_count",

    "first_response_time",
    "first_response_latency_seconds",
    "first_response_home_price",
    "first_response_tx_hash",

    "last_response_time",
    "last_response_latency_seconds",
    "last_response_target_age_seconds",
    "last_response_home_price",
    "last_response_tx_hash",

    "response_min_home_price",
    "response_max_home_price",
    "response_home_price_range",

    "first_response_change",
    "last_response_change",

    "last_response_target_fresh_5s",
    "last_response_target_fresh_15s",
    "last_response_target_fresh_30s",
    "last_response_target_fresh_60s",
]


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_csv(path: Path):
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def parse_dt(value):
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def unix_dt(value):
    return datetime.fromtimestamp(
        int(value),
        tz=timezone.utc,
    )


def iso(value):
    if value is None:
        return ""
    return value.astimezone(timezone.utc).isoformat()


def btxt(value):
    return "true" if bool(value) else "false"


def dstr(value):
    if value is None:
        return ""

    if not isinstance(value, Decimal):
        value = Decimal(str(value))

    if value == 0:
        return "0"

    text = format(value.normalize(), "f")

    if "." in text:
        text = text.rstrip("0").rstrip(".")

    return text


def load_parents():
    cohort = read_csv(COHORT_CSV)
    rn1 = read_csv(RN1_CSV)
    sharp = read_csv(SHARP_CSV)

    assert len(cohort) == EXPECTED_MARKETS
    assert len(rn1) == EXPECTED_RN1_SIGNALS
    assert len(sharp) == EXPECTED_SHARP_SIGNALS

    cm = json.loads(COHORT_MANIFEST.read_text())
    rm = json.loads(RN1_MANIFEST.read_text())
    sm = json.loads(SHARP_MANIFEST.read_text())

    cohort_hash = sha256_file(COHORT_CSV)
    rn1_hash = sha256_file(RN1_CSV)
    sharp_hash = sha256_file(SHARP_CSV)

    if cohort_hash != cm["csv_sha256"]:
        raise RuntimeError("Parent cohort hash mismatch")

    if rn1_hash != rm["artifacts"]["pregame_tape"]["sha256"]:
        raise RuntimeError("Parent RN1 tape hash mismatch")

    if sharp_hash != sm["artifacts"]["sharp_tape"]["sha256"]:
        raise RuntimeError("Parent sharp tape hash mismatch")

    return (
        cohort,
        rn1,
        sharp,
        cohort_hash,
        rn1_hash,
        sharp_hash,
    )


def fetch_market_trades(client, condition_id):
    for attempt in range(1, 4):
        try:
            r = client.get(
                f"{DATA_API}/trades",
                params={
                    "market": condition_id,
                    "limit": 10000,
                    "offset": 0,
                    "takerOnly": "true",
                },
                timeout=30,
            )
            r.raise_for_status()

            data = r.json()

            if not isinstance(data, list):
                raise RuntimeError(
                    f"Expected list, got {type(data).__name__}"
                )

            return data

        except (
            httpx.HTTPError,
            json.JSONDecodeError,
        ):
            if attempt == 3:
                raise
            time.sleep(2 ** attempt)

    raise RuntimeError("unreachable")


def build_pm_tape(cohort):
    client = httpx.Client(
        headers={
            "User-Agent": "quant-lab-h2-m4/0.1"
        },
        timeout=30,
    )

    out = []

    for i, c in enumerate(cohort, 1):
        mid = int(c["market_id"])
        t0 = parse_dt(c["canonical_commence_time"])

        data = fetch_market_trades(
            client,
            c["condition_id"],
        )

        if len(data) >= 10000:
            raise RuntimeError(
                f"{c['slug']}: hit 10,000 trade cap"
            )

        rows = []

        for trade in data:
            outcome = trade.get("outcome")
            token_price = Decimal(str(trade["price"]))

            if outcome == c["home_team"]:
                home_price = token_price
                team_side = "home"

            elif outcome == c["away_team"]:
                home_price = Decimal("1") - token_price
                team_side = "away"

            else:
                raise RuntimeError(
                    f"{c['slug']}: outcome mapping failure: "
                    f"{outcome!r}"
                )

            event_time = unix_dt(trade["timestamp"])

            if event_time >= t0:
                continue

            rows.append({
                "feature_version": FEATURE_VERSION,
                "cohort_version": COHORT_VERSION,

                "market_id": mid,
                "condition_id": c["condition_id"],
                "slug": c["slug"],
                "home_team": c["home_team"],
                "away_team": c["away_team"],
                "canonical_commence_time":
                    c["canonical_commence_time"],

                "event_time": iso(event_time),
                "event_time_unix": int(trade["timestamp"]),

                "home_price": dstr(home_price),
                "token_price": dstr(token_price),
                "outcome": outcome,
                "token_team_side": team_side,

                "side": trade.get("side") or "",
                "size": dstr(trade.get("size") or 0),
                "wallet": str(
                    trade.get("proxyWallet") or ""
                ).lower(),
                "tx_hash": str(
                    trade.get("transactionHash") or ""
                ).lower(),

                "_event_time": event_time,
                "_home_price": home_price,
            })

        rows.sort(
            key=lambda r: (
                r["_event_time"],
                r["tx_hash"],
                r["outcome"],
                r["token_price"],
                r["size"],
                r["wallet"],
                r["side"],
            )
        )

        for index, row in enumerate(rows, 1):
            row["market_trade_index"] = index

        out.extend(rows)

        print(
            f"[{i:02d}/42] {c['slug']:28s} "
            f"pregame_trades={len(rows):5d}"
        )

    client.close()

    if len(out) != EXPECTED_PM_PREGAME_TRADES:
        raise RuntimeError(
            "Public PM pregame trade count changed: "
            f"expected={EXPECTED_PM_PREGAME_TRADES}, "
            f"observed={len(out)}"
        )

    active = {
        int(r["market_id"])
        for r in out
    }

    if len(active) != EXPECTED_MARKETS:
        raise RuntimeError(
            f"Expected PM activity in 42 markets, "
            f"got {len(active)}"
        )

    return out


def build_signals(cohort, rn1, sharp):
    market_meta = {
        int(c["market_id"]): c
        for c in cohort
    }

    signals = []

    for idx, row in enumerate(rn1, 1):
        mid = int(row["market_id"])
        c = market_meta[mid]
        t = parse_dt(row["event_time"])
        t0 = parse_dt(c["canonical_commence_time"])

        if not t < t0:
            raise RuntimeError(
                f"RN1 signal not pregame: {c['slug']}"
            )

        signals.append({
            "signal_id": f"rn1:{idx:04d}",
            "signal_type": "rn1",
            "signal_parent_row": idx,
            "market_id": mid,
            "condition_id": c["condition_id"],
            "slug": c["slug"],
            "home_team": c["home_team"],
            "away_team": c["away_team"],
            "canonical_commence_time":
                c["canonical_commence_time"],
            "signal_time": t,
            "signal_tx_hash":
                str(row.get("tx_hash") or "").lower(),
        })

    for idx, row in enumerate(sharp, 1):
        mid = int(row["market_id"])
        c = market_meta[mid]
        t = parse_dt(row["observed_at"])
        t0 = parse_dt(c["canonical_commence_time"])

        if not t < t0:
            raise RuntimeError(
                f"Sharp signal not pregame: {c['slug']}"
            )

        signals.append({
            "signal_id": f"sharp:{idx:04d}",
            "signal_type": "sharp",
            "signal_parent_row": idx,
            "market_id": mid,
            "condition_id": c["condition_id"],
            "slug": c["slug"],
            "home_team": c["home_team"],
            "away_team": c["away_team"],
            "canonical_commence_time":
                c["canonical_commence_time"],
            "signal_time": t,
            "signal_tx_hash": "",
        })

    return signals


def group_tape(pm_tape):
    grouped = defaultdict(list)

    for row in pm_tape:
        grouped[int(row["market_id"])].append(row)

    return grouped


def baseline_trade(rows, signal_time):
    # Public tape only has second-level timestamps.
    # Exclude the entire second containing the signal.
    cutoff = signal_time.replace(microsecond=0)

    candidate = None

    for row in rows:
        if row["_event_time"] < cutoff:
            candidate = row
        else:
            break

    return candidate


def response_trades(rows, signal_time, target):
    return [
        row
        for row in rows
        if (
            row["_event_time"] > signal_time
            and row["_event_time"] <= target
        )
    ]


def freshness(age, seconds):
    return (
        age is not None
        and Decimal("0")
        <= age
        <= Decimal(seconds)
    )


def build_responses(cohort, signals, pm_tape):
    grouped = group_tape(pm_tape)

    rows = []

    for signal in signals:
        mid = signal["market_id"]
        tape = grouped[mid]

        signal_time = signal["signal_time"]
        t0 = parse_dt(
            signal["canonical_commence_time"]
        )

        signal_seconds_before = Decimal(
            str(
                (
                    t0 - signal_time
                ).total_seconds()
            )
        )

        baseline = baseline_trade(
            tape,
            signal_time,
        )

        if baseline is None:
            baseline_age = None
            baseline_price = None
        else:
            baseline_age = Decimal(
                str(
                    (
                        signal_time
                        - baseline["_event_time"]
                    ).total_seconds()
                )
            )
            baseline_price = baseline["_home_price"]

        for horizon in HORIZONS:
            target = signal_time + timedelta(
                seconds=horizon
            )

            eligible = target < t0

            resp = (
                response_trades(
                    tape,
                    signal_time,
                    target,
                )
                if eligible
                else []
            )

            first = resp[0] if resp else None
            last = resp[-1] if resp else None

            if resp:
                prices = [
                    r["_home_price"]
                    for r in resp
                ]

                first_latency = Decimal(
                    str(
                        (
                            first["_event_time"]
                            - signal_time
                        ).total_seconds()
                    )
                )

                last_latency = Decimal(
                    str(
                        (
                            last["_event_time"]
                            - signal_time
                        ).total_seconds()
                    )
                )

                target_age = Decimal(
                    str(
                        (
                            target
                            - last["_event_time"]
                        ).total_seconds()
                    )
                )

                first_change = (
                    first["_home_price"]
                    - baseline_price
                    if baseline_price is not None
                    else None
                )

                last_change = (
                    last["_home_price"]
                    - baseline_price
                    if baseline_price is not None
                    else None
                )

                pmin = min(prices)
                pmax = max(prices)

            else:
                first_latency = None
                last_latency = None
                target_age = None
                first_change = None
                last_change = None
                pmin = None
                pmax = None

            rows.append({
                "feature_version": FEATURE_VERSION,
                "cohort_version": COHORT_VERSION,

                "signal_id": signal["signal_id"],
                "signal_type": signal["signal_type"],
                "signal_parent_row":
                    signal["signal_parent_row"],

                "market_id": mid,
                "condition_id": signal["condition_id"],
                "slug": signal["slug"],
                "home_team": signal["home_team"],
                "away_team": signal["away_team"],
                "canonical_commence_time":
                    signal["canonical_commence_time"],

                "signal_time": iso(signal_time),
                "signal_seconds_before_start":
                    dstr(signal_seconds_before),
                "signal_tx_hash":
                    signal["signal_tx_hash"],

                "horizon_seconds": horizon,
                "target_time": iso(target),
                "pregame_eligible": btxt(eligible),

                "baseline_available":
                    btxt(baseline is not None),
                "baseline_time":
                    iso(
                        baseline["_event_time"]
                        if baseline
                        else None
                    ),
                "baseline_age_seconds":
                    dstr(baseline_age),
                "baseline_home_price":
                    dstr(baseline_price),
                "baseline_tx_hash":
                    baseline["tx_hash"]
                    if baseline
                    else "",

                "baseline_fresh_30s":
                    btxt(freshness(baseline_age, 30)),
                "baseline_fresh_60s":
                    btxt(freshness(baseline_age, 60)),
                "baseline_fresh_300s":
                    btxt(freshness(baseline_age, 300)),
                "baseline_fresh_600s":
                    btxt(freshness(baseline_age, 600)),

                "response_available":
                    btxt(bool(resp)),
                "response_trade_count":
                    len(resp),

                "first_response_time":
                    iso(
                        first["_event_time"]
                        if first
                        else None
                    ),
                "first_response_latency_seconds":
                    dstr(first_latency),
                "first_response_home_price":
                    dstr(
                        first["_home_price"]
                        if first
                        else None
                    ),
                "first_response_tx_hash":
                    first["tx_hash"]
                    if first
                    else "",

                "last_response_time":
                    iso(
                        last["_event_time"]
                        if last
                        else None
                    ),
                "last_response_latency_seconds":
                    dstr(last_latency),
                "last_response_target_age_seconds":
                    dstr(target_age),
                "last_response_home_price":
                    dstr(
                        last["_home_price"]
                        if last
                        else None
                    ),
                "last_response_tx_hash":
                    last["tx_hash"]
                    if last
                    else "",

                "response_min_home_price":
                    dstr(pmin),
                "response_max_home_price":
                    dstr(pmax),
                "response_home_price_range":
                    dstr(
                        pmax - pmin
                        if pmin is not None
                        else None
                    ),

                "first_response_change":
                    dstr(first_change),
                "last_response_change":
                    dstr(last_change),

                "last_response_target_fresh_5s":
                    btxt(freshness(target_age, 5)),
                "last_response_target_fresh_15s":
                    btxt(freshness(target_age, 15)),
                "last_response_target_fresh_30s":
                    btxt(freshness(target_age, 30)),
                "last_response_target_fresh_60s":
                    btxt(freshness(target_age, 60)),
            })

    return rows


def validate_response_rows(rows):
    if len(rows) != EXPECTED_RESPONSE_ROWS:
        raise RuntimeError(
            f"Expected {EXPECTED_RESPONSE_ROWS} response rows, "
            f"got {len(rows)}"
        )

    seen = set()

    observed = defaultdict(
        lambda: {
            "eligible": 0,
            "response": 0,
        }
    )

    for row in rows:
        key = (
            row["signal_id"],
            int(row["horizon_seconds"]),
        )

        if key in seen:
            raise RuntimeError(
                f"Duplicate response key {key}"
            )

        seen.add(key)

        typ = row["signal_type"]
        horizon = int(row["horizon_seconds"])

        eligible = (
            row["pregame_eligible"]
            == "true"
        )

        available = (
            row["response_available"]
            == "true"
        )

        if available and not eligible:
            raise RuntimeError(
                f"{key}: response on ineligible horizon"
            )

        if eligible:
            observed[(typ, horizon)]["eligible"] += 1

        if available:
            observed[(typ, horizon)]["response"] += 1

            n = int(row["response_trade_count"])

            if n <= 0:
                raise RuntimeError(
                    f"{key}: response count <= 0"
                )

            latency = Decimal(
                row["last_response_latency_seconds"]
            )

            if not (
                Decimal("0")
                < latency
                <= Decimal(horizon)
            ):
                raise RuntimeError(
                    f"{key}: invalid response latency"
                )

            if row["baseline_available"] == "true":
                expected_change = (
                    Decimal(
                        row["last_response_home_price"]
                    )
                    - Decimal(
                        row["baseline_home_price"]
                    )
                )

                if Decimal(
                    row["last_response_change"]
                ) != expected_change:
                    raise RuntimeError(
                        f"{key}: response arithmetic mismatch"
                    )

        else:
            if int(row["response_trade_count"]) != 0:
                raise RuntimeError(
                    f"{key}: unavailable response count != 0"
                )

            for field in (
                "first_response_time",
                "last_response_time",
                "first_response_home_price",
                "last_response_home_price",
                "first_response_change",
                "last_response_change",
            ):
                if row[field] != "":
                    raise RuntimeError(
                        f"{key}: unavailable response "
                        f"has {field}"
                    )

    for typ, by_horizon in EXPECTED_HORIZON_COUNTS.items():
        for horizon, (
            expected_eligible,
            expected_response,
        ) in by_horizon.items():

            got = observed[(typ, horizon)]

            if got["eligible"] != expected_eligible:
                raise RuntimeError(
                    f"{typ} +{horizon}s eligibility changed: "
                    f"expected={expected_eligible}, "
                    f"observed={got['eligible']}"
                )

            if got["response"] != expected_response:
                raise RuntimeError(
                    f"{typ} +{horizon}s response coverage changed: "
                    f"expected={expected_response}, "
                    f"observed={got['response']}"
                )


def verify_rn1_public_match(rn1, pm_tape):
    txs = {
        row["tx_hash"]
        for row in pm_tape
        if row["tx_hash"]
    }

    matched = sum(
        1
        for row in rn1
        if (
            row.get("tx_hash", "").lower()
            in txs
        )
    )

    if matched != EXPECTED_RN1_SIGNALS:
        raise RuntimeError(
            f"RN1 public-tape tx match changed: "
            f"{matched}/{EXPECTED_RN1_SIGNALS}"
        )

    return matched


def csv_payload(rows, fields):
    buf = io.StringIO(newline="")

    writer = csv.DictWriter(
        buf,
        fieldnames=fields,
        lineterminator="\n",
    )

    writer.writeheader()

    for row in rows:
        writer.writerow({
            field: row.get(field, "")
            for field in fields
        })

    return buf.getvalue().encode("utf-8")


def write_outputs(
    cohort_hash,
    rn1_hash,
    sharp_hash,
    rn1,
    pm_tape,
    responses,
):
    validate_response_rows(responses)

    rn1_matches = verify_rn1_public_match(
        rn1,
        pm_tape,
    )

    tape_payload = csv_payload(
        pm_tape,
        TAPE_FIELDS,
    )

    response_payload = csv_payload(
        responses,
        RESPONSE_FIELDS,
    )

    PM_TAPE_CSV.write_bytes(tape_payload)
    RESPONSES_CSV.write_bytes(response_payload)

    tape_hash = sha256_file(PM_TAPE_CSV)
    responses_hash = sha256_file(RESPONSES_CSV)

    baseline_summary = {}
    response_summary = {}

    for typ in ("rn1", "sharp"):
        typ_rows = [
            r for r in responses
            if (
                r["signal_type"] == typ
                and r["horizon_seconds"] == HORIZONS[0]
            )
        ]

        baseline_summary[typ] = {
            "signals": len(typ_rows),
            "available": sum(
                r["baseline_available"] == "true"
                for r in typ_rows
            ),
            "fresh_30s": sum(
                r["baseline_fresh_30s"] == "true"
                for r in typ_rows
            ),
            "fresh_60s": sum(
                r["baseline_fresh_60s"] == "true"
                for r in typ_rows
            ),
            "fresh_300s": sum(
                r["baseline_fresh_300s"] == "true"
                for r in typ_rows
            ),
            "fresh_600s": sum(
                r["baseline_fresh_600s"] == "true"
                for r in typ_rows
            ),
        }

        response_summary[typ] = {}

        for horizon in HORIZONS:
            hr = [
                r for r in responses
                if (
                    r["signal_type"] == typ
                    and int(
                        r["horizon_seconds"]
                    ) == horizon
                )
            ]

            response_summary[typ][str(horizon)] = {
                "pregame_eligible": sum(
                    r["pregame_eligible"] == "true"
                    for r in hr
                ),
                "response_available": sum(
                    r["response_available"] == "true"
                    for r in hr
                ),
            }

    manifest = {
        "feature_version": FEATURE_VERSION,
        "parent_cohort": COHORT_VERSION,

        "parent_hashes": {
            "cohort_csv": cohort_hash,
            "rn1_pregame_tape": rn1_hash,
            "sharp_tape": sharp_hash,
        },

        "built_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),

        "polymarket_source": {
            "endpoint": "Data API /trades",
            "scope": "market-wide executions",
            "taker_only": True,
            "timestamp_resolution": "1 second",
            "price_representation":
                "executed trade price transformed to HOME axis",
            "order_book_available": False,
            "bid_ask_available": False,
            "liquidity_available": False,
        },

        "timing_policy": {
            "rn1_signal_clock":
                "M2 trades.event_time",
            "sharp_signal_clock":
                "M3 observed_at",
            "baseline":
                "last PM execution strictly before the completed "
                "second containing signal_time",
            "response":
                "PM executions with event_time > signal_time "
                "and <= target_time",
            "same_second_policy":
                "same-second executions are excluded from baseline "
                "and response when ordering is ambiguous",
            "game_start":
                "target_time >= canonical T0 is ineligible",
        },

        "freshness_policy": {
            "rows_dropped_for_staleness": False,
            "baseline_flags_seconds": [
                30,
                60,
                300,
                600,
            ],
            "response_target_flags_seconds": [
                5,
                15,
                30,
                60,
            ],
            "interpretation":
                "freshness is recorded, not silently filtered; "
                "M5/M6 choose sensitivity subsets",
        },

        "horizons_seconds": HORIZONS,

        "counts": {
            "markets": EXPECTED_MARKETS,
            "pregame_pm_trades":
                len(pm_tape),
            "rn1_signals":
                EXPECTED_RN1_SIGNALS,
            "sharp_signals":
                EXPECTED_SHARP_SIGNALS,
            "response_rows":
                len(responses),
            "rn1_tx_matches":
                rn1_matches,
        },

        "baseline_summary":
            baseline_summary,

        "response_summary":
            response_summary,

        "limitations": [
            "H2-v1 has no locally recorded pregame TOB for these "
            "42 markets.",
            "M4 measures executed-price reaction rather than "
            "order-book midpoint reaction.",
            "Absence of a response execution is missing response "
            "data, not a zero price move.",
            "Public execution timestamps have one-second resolution.",
            "Tradeability, spread, depth and realistic execution "
            "are not inferred in M4.",
        ],

        "artifacts": {
            "pm_pregame_tape": {
                "file": PM_TAPE_CSV.name,
                "rows": len(pm_tape),
                "sha256": tape_hash,
            },
            "pm_responses": {
                "file": RESPONSES_CSV.name,
                "rows": len(responses),
                "sha256": responses_hash,
            },
        },
    }

    MANIFEST_JSON.write_text(
        json.dumps(
            manifest,
            indent=2,
            sort_keys=True,
        ) + "\n"
    )

    return tape_hash, responses_hash


def validate_artifacts():
    for path in (
        PM_TAPE_CSV,
        RESPONSES_CSV,
        MANIFEST_JSON,
    ):
        if not path.exists():
            raise RuntimeError(
                f"Missing artifact: {path}"
            )

    (
        _cohort,
        _rn1,
        _sharp,
        cohort_hash,
        rn1_hash,
        sharp_hash,
    ) = load_parents()

    manifest = json.loads(
        MANIFEST_JSON.read_text()
    )

    expected_parent = {
        "cohort_csv": cohort_hash,
        "rn1_pregame_tape": rn1_hash,
        "sharp_tape": sharp_hash,
    }

    if manifest["parent_hashes"] != expected_parent:
        raise RuntimeError(
            "M4 parent hash mismatch"
        )

    tape = read_csv(PM_TAPE_CSV)
    responses = read_csv(RESPONSES_CSV)

    if len(tape) != EXPECTED_PM_PREGAME_TRADES:
        raise RuntimeError(
            "Frozen PM tape row count mismatch"
        )

    validate_response_rows(responses)

    tape_hash = sha256_file(PM_TAPE_CSV)
    responses_hash = sha256_file(RESPONSES_CSV)

    if tape_hash != manifest[
        "artifacts"
    ]["pm_pregame_tape"]["sha256"]:
        raise RuntimeError(
            "PM tape SHA mismatch"
        )

    if responses_hash != manifest[
        "artifacts"
    ]["pm_responses"]["sha256"]:
        raise RuntimeError(
            "PM response SHA mismatch"
        )

    return (
        tape,
        responses,
        tape_hash,
        responses_hash,
    )


def print_summary(
    tape,
    responses,
    tape_hash,
    responses_hash,
):
    print()
    print("H2-v1 Polymarket responses")
    print("==========================")
    print(
        f"pregame PM trades: {len(tape)}"
    )
    print(
        f"response rows:     {len(responses)}"
    )

    print()
    print("baseline coverage:")

    for typ in ("rn1", "sharp"):
        rows = [
            r for r in responses
            if (
                r["signal_type"] == typ
                and int(
                    r["horizon_seconds"]
                ) == 10
            )
        ]

        print(
            f"  {typ:5s} "
            f"available="
            f"{sum(r['baseline_available']=='true' for r in rows):3d} "
            f"fresh<=60s="
            f"{sum(r['baseline_fresh_60s']=='true' for r in rows):3d} "
            f"fresh<=300s="
            f"{sum(r['baseline_fresh_300s']=='true' for r in rows):3d}"
        )

    print()
    print("response coverage:")

    for typ in ("rn1", "sharp"):
        print(f"  {typ.upper()}")

        for horizon in HORIZONS:
            rows = [
                r for r in responses
                if (
                    r["signal_type"] == typ
                    and int(
                        r["horizon_seconds"]
                    ) == horizon
                )
            ]

            eligible = sum(
                r["pregame_eligible"] == "true"
                for r in rows
            )

            available = sum(
                r["response_available"] == "true"
                for r in rows
            )

            print(
                f"    +{horizon:3d}s "
                f"eligible={eligible:3d} "
                f"response={available:3d}"
            )

    print()
    print(
        f"PM tape SHA-256: "
        f"{tape_hash}"
    )
    print(
        f"responses SHA-256: "
        f"{responses_hash}"
    )


def main():
    parser = argparse.ArgumentParser()

    mode = parser.add_mutually_exclusive_group(
        required=True
    )

    mode.add_argument(
        "--build",
        action="store_true",
    )

    mode.add_argument(
        "--validate",
        action="store_true",
    )

    args = parser.parse_args()

    if args.build:
        existing = [
            p
            for p in (
                PM_TAPE_CSV,
                RESPONSES_CSV,
                MANIFEST_JSON,
            )
            if p.exists()
        ]

        if existing:
            raise SystemExit(
                "REFUSING TO RE-BUILD: "
                "M4 artifact(s) already exist: "
                + ", ".join(
                    str(p)
                    for p in existing
                )
                + ". Use --validate instead."
            )

        (
            cohort,
            rn1,
            sharp,
            cohort_hash,
            rn1_hash,
            sharp_hash,
        ) = load_parents()

        pm_tape = build_pm_tape(
            cohort
        )

        signals = build_signals(
            cohort,
            rn1,
            sharp,
        )

        responses = build_responses(
            cohort,
            signals,
            pm_tape,
        )

        (
            tape_hash,
            responses_hash,
        ) = write_outputs(
            cohort_hash,
            rn1_hash,
            sharp_hash,
            rn1,
            pm_tape,
            responses,
        )

        print_summary(
            pm_tape,
            responses,
            tape_hash,
            responses_hash,
        )

        print()
        print("BUILD COMPLETE")

        return

    (
        tape,
        responses,
        tape_hash,
        responses_hash,
    ) = validate_artifacts()

    print_summary(
        tape,
        responses,
        tape_hash,
        responses_hash,
    )

    print()
    print("VALIDATION PASSED")


if __name__ == "__main__":
    main()
