"""Build and freeze H2-v1 sharp exchange features.

Parent:
    research/h2_v1_cohort.csv

Outputs:
    research/h2_v1_sharp_tape.csv
    research/h2_v1_sharp_features.csv
    research/h2_v1_sharp_manifest.json

Primary information clock:
    odds_snapshots.capture_time

Poll identity:
    collector_run_id

Exchange families:
    betfair:
        betfair_ex_eu
        betfair_ex_uk
    matchbook:
        matchbook
    smarkets:
        smarkets

Within a collector poll:
    1. Collapse Betfair EU/UK into one family probability using median.
    2. Keep Matchbook and Smarkets as separate families.
    3. Sharp consensus = median of available family probabilities.

This prevents Betfair regional feeds from receiving double weight.

Window features use ONLY polls actually observed inside that window. We never
carry an old observation forward and pretend it occurred inside a shorter
window.

Consequences:
    0 polls -> has_data=false, movement missing
    1 poll  -> level observable, movement missing
    2+      -> movement measurable
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, getcontext
from pathlib import Path
from statistics import median

import psycopg2
from dotenv import dotenv_values


getcontext().prec = 40


REPO = Path(__file__).resolve().parents[1]
ENV = dotenv_values(REPO / ".env")

COHORT_CSV = REPO / "research" / "h2_v1_cohort.csv"
COHORT_MANIFEST = REPO / "research" / "h2_v1_manifest.json"

TAPE_CSV = REPO / "research" / "h2_v1_sharp_tape.csv"
FEATURES_CSV = REPO / "research" / "h2_v1_sharp_features.csv"
MANIFEST_JSON = REPO / "research" / "h2_v1_sharp_manifest.json"

FEATURE_VERSION = "H2-v1-SHARP-M3"
COHORT_VERSION = "H2-v1"

EXPECTED_MARKETS = 42

# Result already established in the M1/M3 audits.
EXPECTED_FINAL6_MARKETS = 39

FAMILIES = {
    "betfair": {
        "betfair_ex_eu",
        "betfair_ex_uk",
    },
    "matchbook": {
        "matchbook",
    },
    "smarkets": {
        "smarkets",
    },
}

KNOWN_EXCHANGES = set().union(
    *FAMILIES.values()
)

WINDOWS = [
    ("all_pregame", None),
    ("6h", 6 * 60 * 60),
    ("60m", 60 * 60),
    ("30m", 30 * 60),
    ("10m", 10 * 60),
    ("5m", 5 * 60),
]

EXPECTED_WINDOW_MARKET_COVERAGE = {
    "6h": 39,
    "60m": 32,
    "30m": 17,
    "10m": 4,
    "5m": 2,
}

TAPE_FIELDS = [
    "feature_version",
    "cohort_version",

    "market_id",
    "condition_id",
    "slug",
    "home_team",
    "away_team",
    "canonical_odds_game_id",
    "canonical_commence_time",

    "collector_run_id",
    "observed_at",
    "seconds_before_start",

    "raw_exchange_count",
    "family_count",
    "is_multi_family",

    "betfair_home_prob",
    "matchbook_home_prob",
    "smarkets_home_prob",

    "sharp_consensus_home_prob",

    "betfair_source_count",
    "matchbook_source_count",
    "smarkets_source_count",

    "min_book_last_update",
    "max_book_last_update",
    "median_source_lag_seconds",
    "max_source_lag_seconds",
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

    "has_data",
    "move_available",

    "poll_count",
    "multi_family_poll_count",

    "first_home_prob",
    "last_home_prob",
    "sharp_home_move",

    "min_home_prob",
    "max_home_prob",
    "home_prob_range",

    "first_poll_time",
    "last_poll_time",
    "last_poll_seconds_before_start",

    "mean_family_count",
    "min_family_count",
    "max_family_count",

    "first_poll_family_count",
    "last_poll_family_count",

    "median_source_lag_seconds",
    "max_source_lag_seconds",
]


def connect():
    password = ENV.get("PG_PASSWORD")

    if not password:
        raise RuntimeError(
            "PG_PASSWORD missing from repository .env"
        )

    return psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="quantlab",
        user="quantlab",
        password=password,
    )


def sha256_file(path: Path) -> str:
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def iso(value):
    if value is None:
        return ""

    return value.astimezone(
        timezone.utc
    ).isoformat()


def bool_text(value):
    return "true" if bool(value) else "false"


def dstr(value):
    if value is None:
        return ""

    if not isinstance(value, Decimal):
        value = Decimal(str(value))

    if value == 0:
        return "0"

    text = format(
        value.normalize(),
        "f",
    )

    if "." in text:
        text = text.rstrip("0").rstrip(".")

    return text


def median_decimal(values):
    values = sorted(
        Decimal(str(x))
        for x in values
    )

    n = len(values)

    if n == 0:
        raise ValueError(
            "median_decimal requires values"
        )

    if n % 2:
        return values[n // 2]

    return (
        values[n // 2 - 1]
        + values[n // 2]
    ) / Decimal(2)


def mean_decimal(values):
    values = [
        Decimal(str(x))
        for x in values
    ]

    if not values:
        return None

    return sum(values) / Decimal(len(values))


def family_for_bookmaker(bookmaker):
    for family, books in FAMILIES.items():
        if bookmaker in books:
            return family

    return None


def verify_parent_cohort():
    if not COHORT_CSV.exists():
        raise RuntimeError(
            f"Missing {COHORT_CSV}"
        )

    if not COHORT_MANIFEST.exists():
        raise RuntimeError(
            f"Missing {COHORT_MANIFEST}"
        )

    manifest = json.loads(
        COHORT_MANIFEST.read_text()
    )

    actual_hash = sha256_file(
        COHORT_CSV
    )

    if actual_hash != manifest.get(
        "csv_sha256"
    ):
        raise RuntimeError(
            "Parent H2-v1 cohort hash mismatch"
        )

    with COHORT_CSV.open(
        newline="",
        encoding="utf-8",
    ) as f:
        rows = list(
            csv.DictReader(f)
        )

    if len(rows) != EXPECTED_MARKETS:
        raise RuntimeError(
            f"Expected {EXPECTED_MARKETS} "
            f"cohort rows, got {len(rows)}"
        )

    return rows, actual_hash


def load_exchange_rows(
    cur,
    game_id,
):
    cur.execute(
        """
        SELECT
            collector_run_id,
            bookmaker,
            outcome_team,
            is_home,
            implied_prob,
            book_last_update,
            capture_time
        FROM odds_snapshots
        WHERE game_id = %s
          AND is_exchange
        ORDER BY
            collector_run_id,
            bookmaker,
            is_home DESC,
            capture_time
        """,
        (game_id,),
    )

    return cur.fetchall()


def validate_book_poll(
    slug,
    home_team,
    away_team,
    bookmaker,
    run_id,
    rows,
):
    if len(rows) != 2:
        raise RuntimeError(
            f"{slug}: run={run_id} "
            f"book={bookmaker} expected "
            f"2 outcomes, got {len(rows)}"
        )

    home_rows = [
        row
        for row in rows
        if row["is_home"]
    ]

    away_rows = [
        row
        for row in rows
        if not row["is_home"]
    ]

    if (
        len(home_rows) != 1
        or len(away_rows) != 1
    ):
        raise RuntimeError(
            f"{slug}: invalid home/away "
            f"cardinality run={run_id} "
            f"book={bookmaker}"
        )

    h = home_rows[0]
    a = away_rows[0]

    if (
        h["outcome_team"] != home_team
        or a["outcome_team"] != away_team
    ):
        raise RuntimeError(
            f"{slug}: team mismatch "
            f"book={bookmaker} "
            f"home={h['outcome_team']!r} "
            f"away={a['outcome_team']!r}"
        )

    total = (
        h["implied_prob"]
        + a["implied_prob"]
    )

    if abs(
        total - Decimal("1")
    ) > Decimal("0.000001"):
        raise RuntimeError(
            f"{slug}: normalized probability "
            f"sum={total} run={run_id} "
            f"book={bookmaker}"
        )

    return h, a


def build_market_polls(
    cur,
    cohort,
):
    market_id = int(
        cohort["market_id"]
    )

    game_id = int(
        cohort["canonical_odds_game_id"]
    )

    slug = cohort["slug"]
    home_team = cohort["home_team"]
    away_team = cohort["away_team"]

    t0 = datetime.fromisoformat(
        cohort["canonical_commence_time"]
    )

    raw = load_exchange_rows(
        cur,
        game_id,
    )

    by_book_poll = defaultdict(
        list
    )

    for (
        run_id,
        bookmaker,
        outcome_team,
        is_home,
        implied_prob,
        book_last_update,
        capture_time,
    ) in raw:

        if run_id is None:
            raise RuntimeError(
                f"{slug}: NULL collector_run_id"
            )

        if bookmaker not in KNOWN_EXCHANGES:
            raise RuntimeError(
                f"{slug}: unexpected exchange "
                f"{bookmaker!r}"
            )

        by_book_poll[
            (int(run_id), bookmaker)
        ].append(
            {
                "outcome_team": outcome_team,
                "is_home": bool(is_home),
                "implied_prob": Decimal(
                    implied_prob
                ),
                "book_last_update": (
                    book_last_update
                ),
                "capture_time": (
                    capture_time
                ),
            }
        )

    validated = {}

    for (
        run_id,
        bookmaker,
    ), rows in by_book_poll.items():

        h, a = validate_book_poll(
            slug,
            home_team,
            away_team,
            bookmaker,
            run_id,
            rows,
        )

        observed_at = max(
            h["capture_time"],
            a["capture_time"],
        )

        source_updates = [
            value
            for value in (
                h["book_last_update"],
                a["book_last_update"],
            )
            if value is not None
        ]

        source_update = (
            max(source_updates)
            if source_updates
            else None
        )

        source_lag_seconds = (
            None
            if source_update is None
            else Decimal(
                str(
                    (
                        observed_at
                        - source_update
                    ).total_seconds()
                )
            )
        )

        if (
            source_lag_seconds is not None
            and source_lag_seconds < Decimal("-5")
        ):
            raise RuntimeError(
                f"{slug}: materially negative "
                f"source lag {source_lag_seconds}"
            )

        validated[
            (run_id, bookmaker)
        ] = {
            "home_prob": (
                h["implied_prob"]
            ),
            "observed_at": observed_at,
            "source_update": (
                source_update
            ),
            "source_lag_seconds": (
                source_lag_seconds
            ),
        }

    by_run = defaultdict(
        dict
    )

    for (
        run_id,
        bookmaker,
    ), observation in validated.items():

        by_run[run_id][
            bookmaker
        ] = observation

    polls = []

    for run_id, books in by_run.items():
        family_probs = defaultdict(
            list
        )

        family_source_counts = Counter()

        observed_at = max(
            obs["observed_at"]
            for obs in books.values()
        )

        source_updates = [
            obs["source_update"]
            for obs in books.values()
            if obs["source_update"]
            is not None
        ]

        source_lags = [
            obs["source_lag_seconds"]
            for obs in books.values()
            if obs["source_lag_seconds"]
            is not None
        ]

        for bookmaker, observation in books.items():
            family = family_for_bookmaker(
                bookmaker
            )

            family_probs[
                family
            ].append(
                observation["home_prob"]
            )

            family_source_counts[
                family
            ] += 1

        collapsed = {
            family: median_decimal(
                probs
            )
            for family, probs
            in family_probs.items()
        }

        consensus = median_decimal(
            collapsed.values()
        )

        seconds_before_start = Decimal(
            str(
                (
                    t0 - observed_at
                ).total_seconds()
            )
        )

        if source_lags:
            median_source_lag = (
                median_decimal(
                    source_lags
                )
            )
            max_source_lag = max(
                source_lags
            )
        else:
            median_source_lag = None
            max_source_lag = None

        poll = {
            "feature_version": (
                FEATURE_VERSION
            ),
            "cohort_version": (
                COHORT_VERSION
            ),

            "market_id": market_id,
            "condition_id": (
                cohort["condition_id"]
            ),
            "slug": slug,
            "home_team": home_team,
            "away_team": away_team,
            "canonical_odds_game_id": (
                game_id
            ),
            "canonical_commence_time": (
                cohort[
                    "canonical_commence_time"
                ]
            ),

            "collector_run_id": run_id,
            "observed_at": iso(
                observed_at
            ),
            "seconds_before_start": (
                dstr(
                    seconds_before_start
                )
            ),

            "raw_exchange_count": (
                len(books)
            ),
            "family_count": (
                len(collapsed)
            ),
            "is_multi_family": (
                bool_text(
                    len(collapsed) >= 2
                )
            ),

            "betfair_home_prob": dstr(
                collapsed.get(
                    "betfair"
                )
            ),
            "matchbook_home_prob": dstr(
                collapsed.get(
                    "matchbook"
                )
            ),
            "smarkets_home_prob": dstr(
                collapsed.get(
                    "smarkets"
                )
            ),

            "sharp_consensus_home_prob": (
                dstr(consensus)
            ),

            "betfair_source_count": (
                family_source_counts[
                    "betfair"
                ]
            ),
            "matchbook_source_count": (
                family_source_counts[
                    "matchbook"
                ]
            ),
            "smarkets_source_count": (
                family_source_counts[
                    "smarkets"
                ]
            ),

            "min_book_last_update": (
                iso(
                    min(
                        source_updates
                    )
                )
                if source_updates
                else ""
            ),
            "max_book_last_update": (
                iso(
                    max(
                        source_updates
                    )
                )
                if source_updates
                else ""
            ),

            "median_source_lag_seconds": (
                dstr(
                    median_source_lag
                )
            ),
            "max_source_lag_seconds": (
                dstr(
                    max_source_lag
                )
            ),

            # private helpers
            "_observed_at": (
                observed_at
            ),
            "_consensus": (
                consensus
            ),
            "_family_count": (
                len(collapsed)
            ),
            "_source_lag": (
                median_source_lag
            ),
            "_max_source_lag": (
                max_source_lag
            ),
        }

        polls.append(
            poll
        )

    polls.sort(
        key=lambda row: (
            row["_observed_at"],
            row["collector_run_id"],
        )
    )

    return polls


def build_tape(
    conn,
    cohort_rows,
):
    tape = []

    with conn.cursor() as cur:
        for cohort in cohort_rows:
            polls = build_market_polls(
                cur,
                cohort,
            )

            # M3 sharp tape is PREGAME ONLY.
            t0 = datetime.fromisoformat(
                cohort[
                    "canonical_commence_time"
                ]
            )

            for poll in polls:
                if (
                    poll["_observed_at"]
                    < t0
                ):
                    tape.append(
                        poll
                    )

    tape.sort(
        key=lambda row: (
            row[
                "canonical_commence_time"
            ],
            row["slug"],
            row["_observed_at"],
            row["collector_run_id"],
        )
    )

    return tape


def window_polls(
    tape,
    market_id,
    t0,
    seconds,
):
    rows = [
        row
        for row in tape
        if int(
            row["market_id"]
        ) == market_id
    ]

    if seconds is None:
        return rows

    start = t0 - timedelta(
        seconds=seconds
    )

    return [
        row
        for row in rows
        if (
            start
            <= row["_observed_at"]
            < t0
        )
    ]


def aggregate_window(
    cohort,
    rows,
    label,
    seconds,
):
    t0 = datetime.fromisoformat(
        cohort[
            "canonical_commence_time"
        ]
    )

    if seconds is None:
        window_seconds = ""
        window_start = ""
    else:
        window_seconds = seconds
        window_start = iso(
            t0 - timedelta(
                seconds=seconds
            )
        )

    if not rows:
        return {
            "feature_version": (
                FEATURE_VERSION
            ),
            "cohort_version": (
                COHORT_VERSION
            ),

            "market_id": int(
                cohort["market_id"]
            ),
            "condition_id": (
                cohort["condition_id"]
            ),
            "slug": cohort["slug"],
            "home_team": (
                cohort["home_team"]
            ),
            "away_team": (
                cohort["away_team"]
            ),
            "canonical_commence_time": (
                cohort[
                    "canonical_commence_time"
                ]
            ),
            "include_broad": (
                cohort[
                    "include_broad"
                ]
            ),
            "include_tight": (
                cohort[
                    "include_tight"
                ]
            ),

            "window": label,
            "window_seconds": (
                window_seconds
            ),
            "window_start": (
                window_start
            ),

            "has_data": "false",
            "move_available": (
                "false"
            ),

            "poll_count": 0,
            "multi_family_poll_count": (
                0
            ),

            "first_home_prob": "",
            "last_home_prob": "",
            "sharp_home_move": "",

            "min_home_prob": "",
            "max_home_prob": "",
            "home_prob_range": "",

            "first_poll_time": "",
            "last_poll_time": "",
            "last_poll_seconds_before_start": "",

            "mean_family_count": "",
            "min_family_count": "",
            "max_family_count": "",

            "first_poll_family_count": "",
            "last_poll_family_count": "",

            "median_source_lag_seconds": "",
            "max_source_lag_seconds": "",
        }

    rows = sorted(
        rows,
        key=lambda row: (
            row["_observed_at"],
            row["collector_run_id"],
        )
    )

    probs = [
        row["_consensus"]
        for row in rows
    ]

    family_counts = [
        Decimal(
            row["_family_count"]
        )
        for row in rows
    ]

    source_lags = [
        row["_source_lag"]
        for row in rows
        if row["_source_lag"]
        is not None
    ]

    max_source_lags = [
        row["_max_source_lag"]
        for row in rows
        if row["_max_source_lag"]
        is not None
    ]

    first = rows[0]
    last = rows[-1]

    last_seconds = Decimal(
        str(
            (
                t0
                - last[
                    "_observed_at"
                ]
            ).total_seconds()
        )
    )

    move_available = (
        len(rows) >= 2
    )

    sharp_move = (
        last["_consensus"]
        - first["_consensus"]
        if move_available
        else None
    )

    return {
        "feature_version": (
            FEATURE_VERSION
        ),
        "cohort_version": (
            COHORT_VERSION
        ),

        "market_id": int(
            cohort["market_id"]
        ),
        "condition_id": (
            cohort["condition_id"]
        ),
        "slug": cohort["slug"],
        "home_team": (
            cohort["home_team"]
        ),
        "away_team": (
            cohort["away_team"]
        ),
        "canonical_commence_time": (
            cohort[
                "canonical_commence_time"
            ]
        ),
        "include_broad": (
            cohort[
                "include_broad"
            ]
        ),
        "include_tight": (
            cohort[
                "include_tight"
            ]
        ),

        "window": label,
        "window_seconds": (
            window_seconds
        ),
        "window_start": (
            window_start
        ),

        "has_data": "true",
        "move_available": (
            bool_text(
                move_available
            )
        ),

        "poll_count": len(rows),
        "multi_family_poll_count": (
            sum(
                row[
                    "is_multi_family"
                ] == "true"
                for row in rows
            )
        ),

        "first_home_prob": dstr(
            first["_consensus"]
        ),
        "last_home_prob": dstr(
            last["_consensus"]
        ),
        "sharp_home_move": dstr(
            sharp_move
        ),

        "min_home_prob": dstr(
            min(probs)
        ),
        "max_home_prob": dstr(
            max(probs)
        ),
        "home_prob_range": dstr(
            max(probs)
            - min(probs)
        ),

        "first_poll_time": iso(
            first[
                "_observed_at"
            ]
        ),
        "last_poll_time": iso(
            last[
                "_observed_at"
            ]
        ),
        "last_poll_seconds_before_start": (
            dstr(
                last_seconds
            )
        ),

        "mean_family_count": dstr(
            mean_decimal(
                family_counts
            )
        ),
        "min_family_count": dstr(
            min(
                family_counts
            )
        ),
        "max_family_count": dstr(
            max(
                family_counts
            )
        ),

        "first_poll_family_count": (
            first[
                "_family_count"
            ]
        ),
        "last_poll_family_count": (
            last[
                "_family_count"
            ]
        ),

        "median_source_lag_seconds": (
            dstr(
                median_decimal(
                    source_lags
                )
            )
            if source_lags
            else ""
        ),
        "max_source_lag_seconds": (
            dstr(
                max(
                    max_source_lags
                )
            )
            if max_source_lags
            else ""
        ),
    }


def build_features(
    cohort_rows,
    tape,
):
    rows = []

    for cohort in cohort_rows:
        market_id = int(
            cohort["market_id"]
        )

        t0 = datetime.fromisoformat(
            cohort[
                "canonical_commence_time"
            ]
        )

        for label, seconds in WINDOWS:
            selected = window_polls(
                tape,
                market_id,
                t0,
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

    order = {
        label: index
        for index, (
            label,
            _,
        ) in enumerate(WINDOWS)
    }

    rows.sort(
        key=lambda row: (
            row[
                "canonical_commence_time"
            ],
            row["slug"],
            order[
                row["window"]
            ],
        )
    )

    return rows


def validate_features(
    features,
):
    expected_rows = (
        EXPECTED_MARKETS
        * len(WINDOWS)
    )

    if len(features) != expected_rows:
        raise RuntimeError(
            f"Expected {expected_rows} "
            f"feature rows, got "
            f"{len(features)}"
        )

    keys = set()

    coverage = Counter()

    for row in features:
        key = (
            int(
                row["market_id"]
            ),
            row["window"],
        )

        if key in keys:
            raise RuntimeError(
                f"Duplicate feature key "
                f"{key}"
            )

        keys.add(key)

        has_data = (
            row["has_data"]
            == "true"
        )

        poll_count = int(
            row["poll_count"]
        )

        if has_data != (
            poll_count > 0
        ):
            raise RuntimeError(
                f"{row['slug']} "
                f"{row['window']}: "
                "has_data mismatch"
            )

        if (
            row[
                "move_available"
            ] == "true"
        ) != (
            poll_count >= 2
        ):
            raise RuntimeError(
                f"{row['slug']} "
                f"{row['window']}: "
                "move availability "
                "mismatch"
            )

        if has_data:
            coverage[
                row["window"]
            ] += 1

            first = Decimal(
                row[
                    "first_home_prob"
                ]
            )

            last = Decimal(
                row[
                    "last_home_prob"
                ]
            )

            minimum = Decimal(
                row[
                    "min_home_prob"
                ]
            )

            maximum = Decimal(
                row[
                    "max_home_prob"
                ]
            )

            for value in (
                first,
                last,
                minimum,
                maximum,
            ):
                if not (
                    Decimal("0")
                    <= value
                    <= Decimal("1")
                ):
                    raise RuntimeError(
                        f"{row['slug']} "
                        f"{row['window']}: "
                        f"probability {value}"
                    )

            if minimum > maximum:
                raise RuntimeError(
                    f"{row['slug']} "
                    f"{row['window']}: "
                    "min > max"
                )

            observed_range = Decimal(
                row[
                    "home_prob_range"
                ]
            )

            if (
                observed_range
                != maximum - minimum
            ):
                raise RuntimeError(
                    f"{row['slug']} "
                    f"{row['window']}: "
                    "range arithmetic"
                )

            if poll_count >= 2:
                move = Decimal(
                    row[
                        "sharp_home_move"
                    ]
                )

                if move != (
                    last - first
                ):
                    raise RuntimeError(
                        f"{row['slug']} "
                        f"{row['window']}: "
                        "move arithmetic"
                    )

            else:
                if (
                    row[
                        "sharp_home_move"
                    ]
                    != ""
                ):
                    raise RuntimeError(
                        f"{row['slug']} "
                        f"{row['window']}: "
                        "one-poll move must "
                        "be missing"
                    )

        else:
            probability_fields = [
                "first_home_prob",
                "last_home_prob",
                "sharp_home_move",
                "min_home_prob",
                "max_home_prob",
                "home_prob_range",
            ]

            if any(
                row[field] != ""
                for field
                in probability_fields
            ):
                raise RuntimeError(
                    f"{row['slug']} "
                    f"{row['window']}: "
                    "no-data probability "
                    "fields not blank"
                )

    for (
        window,
        expected,
    ) in (
        EXPECTED_WINDOW_MARKET_COVERAGE.items()
    ):
        observed = coverage[
            window
        ]

        if observed != expected:
            raise RuntimeError(
                f"{window} market "
                f"coverage changed: "
                f"expected={expected}, "
                f"observed={observed}"
            )

    if (
        coverage["6h"]
        != EXPECTED_FINAL6_MARKETS
    ):
        raise RuntimeError(
            "Final-6h market count "
            "does not match frozen "
            "tight sample"
        )


def csv_payload(
    rows,
    fields,
):
    buffer = io.StringIO(
        newline=""
    )

    writer = csv.DictWriter(
        buffer,
        fieldnames=fields,
        lineterminator="\n",
    )

    writer.writeheader()

    for source in rows:
        row = {
            field: source.get(
                field,
                "",
            )
            for field in fields
        }

        writer.writerow(
            row
        )

    return buffer.getvalue().encode(
        "utf-8"
    )


def write_outputs(
    cohort_rows,
    parent_hash,
    tape,
    features,
):
    validate_features(
        features
    )

    tape_payload = csv_payload(
        tape,
        TAPE_FIELDS,
    )

    feature_payload = csv_payload(
        features,
        FEATURE_FIELDS,
    )

    TAPE_CSV.write_bytes(
        tape_payload
    )

    FEATURES_CSV.write_bytes(
        feature_payload
    )

    tape_hash = sha256_file(
        TAPE_CSV
    )

    features_hash = sha256_file(
        FEATURES_CSV
    )

    market_coverage = {}

    move_coverage = {}

    poll_counts = {}

    for label, _ in WINDOWS:
        rows = [
            row
            for row in features
            if (
                row["window"]
                == label
            )
        ]

        market_coverage[
            label
        ] = sum(
            row["has_data"]
            == "true"
            for row in rows
        )

        move_coverage[
            label
        ] = sum(
            row[
                "move_available"
            ] == "true"
            for row in rows
        )

        poll_counts[
            label
        ] = sum(
            int(
                row[
                    "poll_count"
                ]
            )
            for row in rows
        )

    family_composition = (
        Counter()
    )

    for row in tape:
        present = tuple(
            family
            for family, field
            in (
                (
                    "betfair",
                    "betfair_home_prob",
                ),
                (
                    "matchbook",
                    "matchbook_home_prob",
                ),
                (
                    "smarkets",
                    "smarkets_home_prob",
                ),
            )
            if row[field] != ""
        )

        family_composition[
            present
        ] += 1

    manifest = {
        "feature_version": (
            FEATURE_VERSION
        ),
        "parent_cohort": (
            COHORT_VERSION
        ),
        "parent_cohort_csv_sha256": (
            parent_hash
        ),

        "built_at_utc": (
            datetime.now(
                timezone.utc
            ).isoformat()
        ),

        "information_clock": (
            "odds_snapshots.capture_time; "
            "poll observed_at is maximum "
            "capture_time across exchange "
            "rows in collector_run_id"
        ),

        "price_representation": {
            "price_side": "back_only",
            "lay_prices_collected": False,
            "market_depth_collected": False,
            "liquidity_collected": False,
            "probability_source": (
                "overround-adjusted implied probabilities derived from "
                "exchange h2h back odds"
            ),
            "consensus_interpretation": (
                "median consensus of available exchange-family "
                "back-odds-derived home probabilities; not an exchange "
                "back/lay midpoint"
            ),
        },

        "exchange_family_policy": {
            "betfair": sorted(
                FAMILIES[
                    "betfair"
                ]
            ),
            "matchbook": sorted(
                FAMILIES[
                    "matchbook"
                ]
            ),
            "smarkets": sorted(
                FAMILIES[
                    "smarkets"
                ]
            ),
            "family_collapse": (
                "median home implied "
                "probability within family"
            ),
            "sharp_consensus": (
                "median home implied "
                "probability across available "
                "exchange families"
            ),
            "single_family_allowed": True,
        },

        "movement_policy": (
            "Window movement uses only polls "
            "actually observed inside the "
            "window. Zero polls => missing. "
            "One poll => level available but "
            "movement missing. No stale poll "
            "is carried into a window."
        ),

        "windows": {
            "all_pregame": (
                "all observed exchange "
                "polls before T0"
            ),
            "6h": "[T0-6h, T0)",
            "60m": "[T0-60m, T0)",
            "30m": "[T0-30m, T0)",
            "10m": "[T0-10m, T0)",
            "5m": "[T0-5m, T0)",
        },

        "market_coverage": (
            market_coverage
        ),
        "move_coverage": (
            move_coverage
        ),
        "poll_counts": (
            poll_counts
        ),

        "family_composition": {
            "+".join(key): value
            for key, value
            in sorted(
                family_composition.items()
            )
        },

        "artifacts": {
            "sharp_tape": {
                "file": (
                    TAPE_CSV.name
                ),
                "rows": len(tape),
                "sha256": (
                    tape_hash
                ),
            },
            "sharp_features": {
                "file": (
                    FEATURES_CSV.name
                ),
                "rows": len(
                    features
                ),
                "sha256": (
                    features_hash
                ),
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

    return (
        tape_hash,
        features_hash,
    )


def read_csv(
    path,
):
    with path.open(
        newline="",
        encoding="utf-8",
    ) as f:
        return list(
            csv.DictReader(f)
        )


def validate_artifacts():
    if not (
        TAPE_CSV.exists()
        and FEATURES_CSV.exists()
        and MANIFEST_JSON.exists()
    ):
        raise RuntimeError(
            "One or more M3 artifacts "
            "are missing"
        )

    cohort_rows, parent_hash = (
        verify_parent_cohort()
    )

    manifest = json.loads(
        MANIFEST_JSON.read_text()
    )

    if manifest.get(
        "parent_cohort_csv_sha256"
    ) != parent_hash:
        raise RuntimeError(
            "M3 parent cohort hash "
            "mismatch"
        )

    tape = read_csv(
        TAPE_CSV
    )

    features = read_csv(
        FEATURES_CSV
    )

    validate_features(
        features
    )

    tape_hash = sha256_file(
        TAPE_CSV
    )

    features_hash = sha256_file(
        FEATURES_CSV
    )

    if tape_hash != manifest[
        "artifacts"
    ][
        "sharp_tape"
    ][
        "sha256"
    ]:
        raise RuntimeError(
            "Sharp tape SHA mismatch"
        )

    if features_hash != manifest[
        "artifacts"
    ][
        "sharp_features"
    ][
        "sha256"
    ]:
        raise RuntimeError(
            "Sharp feature SHA mismatch"
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
    print(
        "H2-v1 sharp exchange features"
    )
    print(
        "============================="
    )
    print(
        f"pregame sharp polls: "
        f"{len(tape)}"
    )
    print(
        f"feature rows:        "
        f"{len(features)}"
    )

    print()
    print("window summary:")

    for label, _ in WINDOWS:
        rows = [
            row
            for row in features
            if (
                row["window"]
                == label
            )
        ]

        markets = sum(
            row["has_data"]
            == "true"
            for row in rows
        )

        moves = sum(
            row[
                "move_available"
            ] == "true"
            for row in rows
        )

        polls = sum(
            int(
                row[
                    "poll_count"
                ]
            )
            for row in rows
        )

        print(
            f"  {label:11s} "
            f"polls={polls:4d} "
            f"markets={markets:2d} "
            f"moves={moves:2d}"
        )

    print()
    print(
        f"sharp tape SHA-256: "
        f"{tape_hash}"
    )
    print(
        f"features SHA-256:   "
        f"{features_hash}"
    )


def main():
    parser = (
        argparse.ArgumentParser()
    )

    mode = (
        parser.add_mutually_exclusive_group(
            required=True
        )
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
                "REFUSING TO RE-BUILD: "
                "M3 artifact(s) already exist: "
                + ", ".join(
                    str(path)
                    for path in existing
                )
                + ". Use --validate instead."
            )

        (
            cohort_rows,
            parent_hash,
        ) = verify_parent_cohort()

        conn = connect()

        try:
            tape = build_tape(
                conn,
                cohort_rows,
            )
        finally:
            conn.close()

        features = build_features(
            cohort_rows,
            tape,
        )

        (
            tape_hash,
            features_hash,
        ) = write_outputs(
            cohort_rows,
            parent_hash,
            tape,
            features,
        )

        print_summary(
            tape,
            features,
            tape_hash,
            features_hash,
        )

        print()
        print("BUILD COMPLETE")
        print(
            f"  {TAPE_CSV}"
        )
        print(
            f"  {FEATURES_CSV}"
        )
        print(
            f"  {MANIFEST_JSON}"
        )

        return

    (
        _cohort_rows,
        tape,
        features,
        tape_hash,
        features_hash,
    ) = validate_artifacts()

    print_summary(
        tape,
        features,
        tape_hash,
        features_hash,
    )

    print()
    print("VALIDATION PASSED")


if __name__ == "__main__":
    main()
