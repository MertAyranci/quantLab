"""Freeze and validate the canonical H2-v1 MLB research cohort.

H2-v1 is a historical research experiment, not a live database query.

First freeze:
    .venv/bin/python research/build_h2_v1_cohort.py --freeze

Later integrity check:
    .venv/bin/python research/build_h2_v1_cohort.py --validate

Membership definition at freeze time:
    * MLB Polymarket market
    * linked to Odds API
    * RN1 collection receipt is complete

The first freeze MUST produce exactly:
    * 42 markets
    * 3,626 RN1 raw fills
    * 42 broad-coverage markets
    * 39 tight final-6h markets
    * 3 explicit final-6h exclusions

After the CSV is written, H2-v1 membership is immutable. New MLB markets belong
to later cohorts such as H2-v2.

Five markets had two Odds API games incorrectly linked to one Polymarket market
because adjacent US-local-date games can share the same UTC calendar date.
Those five were manually audited before freeze. Their exact canonical start
times are encoded below; MAX(commence_time) is NOT used as a general rule.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
from dotenv import dotenv_values


REPO = Path(__file__).resolve().parents[1]
ENV = dotenv_values(REPO / ".env")

COHORT_CSV = REPO / "research" / "h2_v1_cohort.csv"
MANIFEST_JSON = REPO / "research" / "h2_v1_manifest.json"

COHORT_VERSION = "H2-v1"
WALLET = "RN1"
POLYMARKET_VENUE_ID = 1

EXPECTED_MARKETS = 42
EXPECTED_RN1_RAW_FILLS = 3626
EXPECTED_BROAD = 42
EXPECTED_TIGHT = 39

EXPECTED_TIGHT_EXCLUSIONS = {
    "mlb-laa-bal-2026-08-06",
    "mlb-oak-cin-2026-08-06",
    "mlb-nym-cle-2026-08-06",
}

# ---------------------------------------------------------------------------
# Manually audited start-time conflicts.
#
# Each of these Polymarket markets had TWO linked Odds API games involving the
# same teams, separated by ~18 hours. The earlier UTC timestamp corresponds to
# the previous local-calendar game in the series. The later candidate matches
# the game date encoded by the Polymarket slug.
#
# These are explicit overrides. We deliberately do NOT write a generic
# "choose MAX()" rule.
# ---------------------------------------------------------------------------

AUDITED_CANONICAL_STARTS = {
    1614971: "2026-08-05T18:10:03+00:00",  # TOR @ HOU
    1614975: "2026-08-05T18:21:00+00:00",  # LAD @ CHC
    1614976: "2026-08-05T19:11:00+00:00",  # TB @ COL
    1614979: "2026-08-05T18:36:00+00:00",  # SF @ TEX
    1673840: "2026-08-06T20:11:00+00:00",  # DET @ SEA
}


FIELDS = [
    "cohort_version",
    "market_id",
    "condition_id",
    "slug",
    "question",

    "canonical_odds_game_id",
    "canonical_oddsapi_id",
    "home_team",
    "away_team",
    "canonical_commence_time",
    "match_confidence",

    "commence_candidate_count",
    "commence_min",
    "commence_max",
    "commence_span_seconds",
    "commence_resolution",

    "token_count",

    "rn1_status",
    "rn1_trade_count",
    "rn1_db_trade_count",
    "rn1_earliest_trade",
    "rn1_latest_trade",

    "pregame_odds_snapshot_count",
    "pregame_exchange_snapshot_count",
    "first_pregame_capture",
    "last_pregame_capture",
    "last_capture_before_start_seconds",

    "final_6h_snapshot_count",
    "final_6h_exchange_snapshot_count",
    "final_6h_book_count",
    "final_6h_exchange_count",

    "include_broad",
    "include_tight",
    "exclusion_reason",
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


def bool_text(value: bool) -> str:
    return "true" if value else "false"


def discover_freeze_membership(cur):
    """One-time discovery of the original completed RN1 H2-v1 universe."""

    cur.execute(
        """
        SELECT DISTINCT
            m.id,
            m.condition_id,
            m.slug,
            m.question
        FROM markets m
        JOIN odds_games og
          ON og.polymarket_market_id = m.id
         AND og.sport_key ILIKE '%%mlb%%'
        JOIN wallet_collection_receipts r
          ON r.wallet = %s
         AND r.condition_id = m.condition_id
         AND r.status = 'complete'
        WHERE m.condition_id IS NOT NULL
        ORDER BY m.id
        """,
        (WALLET,),
    )

    rows = cur.fetchall()

    if len(rows) != EXPECTED_MARKETS:
        raise RuntimeError(
            "Freeze membership changed: "
            f"expected {EXPECTED_MARKETS}, found {len(rows)}. "
            "Do NOT change EXPECTED_MARKETS to make this pass."
        )

    market_ids = [row[0] for row in rows]
    conditions = [row[1] for row in rows]

    if len(set(market_ids)) != EXPECTED_MARKETS:
        raise RuntimeError("market_id is not unique in freeze membership")

    if len(set(conditions)) != EXPECTED_MARKETS:
        raise RuntimeError("condition_id is not unique in freeze membership")

    return rows


def load_odds_candidates(cur, market_id: int):
    cur.execute(
        """
        SELECT
            id,
            oddsapi_id,
            commence_time,
            home_team,
            away_team,
            match_confidence
        FROM odds_games
        WHERE polymarket_market_id = %s
          AND sport_key ILIKE '%%mlb%%'
        ORDER BY commence_time, id
        """,
        (market_id,),
    )

    return cur.fetchall()


def choose_canonical_game(market_id: int, candidates):
    """Choose exactly one Odds API game for a frozen Polymarket market."""

    if not candidates:
        raise RuntimeError(
            f"market_id={market_id}: no linked Odds API game"
        )

    # All linked candidates should describe the same matchup.
    matchup_pairs = {
        (row[3], row[4])
        for row in candidates
    }

    if len(matchup_pairs) != 1:
        raise RuntimeError(
            f"market_id={market_id}: conflicting team identities: "
            f"{sorted(matchup_pairs)}"
        )

    distinct_times = sorted(
        {row[2] for row in candidates}
    )

    if len(distinct_times) == 1:
        # If duplicate rows somehow share exactly the same start, select the
        # lowest DB id deterministically and keep the anomaly visible through
        # candidate_count.
        matching = [
            row for row in candidates
            if row[2] == distinct_times[0]
        ]

        canonical = sorted(
            matching,
            key=lambda row: row[0],
        )[0]

        resolution = "single_start_candidate"

        return (
            canonical,
            distinct_times,
            resolution,
        )

    # Multiple distinct starts require an explicit, audited override.
    if market_id not in AUDITED_CANONICAL_STARTS:
        raise RuntimeError(
            f"market_id={market_id}: unexpected multiple commencement "
            f"times {distinct_times}; manual review required"
        )

    target = datetime.fromisoformat(
        AUDITED_CANONICAL_STARTS[market_id]
    )

    matches = [
        row
        for row in candidates
        if row[2] == target
    ]

    if len(matches) != 1:
        raise RuntimeError(
            f"market_id={market_id}: audited canonical start {target} "
            f"does not resolve to exactly one odds_game row"
        )

    canonical = matches[0]

    resolution = (
        "manual_audit_previous_local_date_collision_later_candidate"
    )

    return (
        canonical,
        distinct_times,
        resolution,
    )


def token_count(cur, market_id: int) -> int:
    cur.execute(
        """
        SELECT COUNT(*)
        FROM tokens
        WHERE market_id = %s
        """,
        (market_id,),
    )

    return int(cur.fetchone()[0])


def rn1_information(cur, condition_id: str):
    cur.execute(
        """
        SELECT
            status,
            trade_count,
            earliest_trade,
            latest_trade
        FROM wallet_collection_receipts
        WHERE wallet = %s
          AND condition_id = %s
        """,
        (WALLET, condition_id),
    )

    receipt = cur.fetchone()

    if receipt is None:
        raise RuntimeError(
            f"condition_id={condition_id}: RN1 receipt missing"
        )

    status, receipt_count, earliest, latest = receipt

    cur.execute(
        """
        SELECT COUNT(*)
        FROM trades
        WHERE venue_id = %s
          AND wallet = %s
          AND condition_id = %s
        """,
        (
            POLYMARKET_VENUE_ID,
            WALLET,
            condition_id,
        ),
    )

    db_count = int(cur.fetchone()[0])

    return (
        status,
        int(receipt_count),
        db_count,
        earliest,
        latest,
    )


def unresolved_rn1_assets(cur, condition_id: str) -> int:
    cur.execute(
        """
        SELECT COUNT(*)
        FROM wallet_unresolved_assets
        WHERE wallet = %s
          AND condition_id = %s
        """,
        (WALLET, condition_id),
    )

    return int(cur.fetchone()[0])


def odds_coverage(cur, game_id: int, canonical_start):
    """Measure collector availability for ONE canonical Odds API game.

    capture_time is used here because M1 is auditing whether our collector had
    data available. M3 will separately use book_last_update where appropriate
    for actual lead-lag timing.
    """

    window_start = canonical_start - timedelta(hours=6)

    cur.execute(
        """
        SELECT
            COUNT(*) FILTER (
                WHERE capture_time <= %s
            ) AS pregame_n,

            COUNT(*) FILTER (
                WHERE is_exchange
                  AND capture_time <= %s
            ) AS pregame_exchange_n,

            MIN(capture_time) FILTER (
                WHERE capture_time <= %s
            ) AS first_pregame,

            MAX(capture_time) FILTER (
                WHERE capture_time <= %s
            ) AS last_pregame,

            COUNT(*) FILTER (
                WHERE capture_time BETWEEN %s AND %s
            ) AS final6_n,

            COUNT(*) FILTER (
                WHERE is_exchange
                  AND capture_time BETWEEN %s AND %s
            ) AS final6_exchange_n,

            COUNT(DISTINCT bookmaker) FILTER (
                WHERE capture_time BETWEEN %s AND %s
            ) AS final6_books,

            COUNT(DISTINCT bookmaker) FILTER (
                WHERE is_exchange
                  AND capture_time BETWEEN %s AND %s
            ) AS final6_exchanges

        FROM odds_snapshots
        WHERE game_id = %s
        """,
        (
            canonical_start,
            canonical_start,
            canonical_start,
            canonical_start,

            window_start,
            canonical_start,

            window_start,
            canonical_start,

            window_start,
            canonical_start,

            window_start,
            canonical_start,

            game_id,
        ),
    )

    (
        pregame_n,
        pregame_exchange_n,
        first_pregame,
        last_pregame,
        final6_n,
        final6_exchange_n,
        final6_books,
        final6_exchanges,
    ) = cur.fetchone()

    lag_seconds = None

    if last_pregame is not None:
        lag_seconds = (
            canonical_start - last_pregame
        ).total_seconds()

    return {
        "pregame_n": int(pregame_n or 0),
        "pregame_exchange_n": int(
            pregame_exchange_n or 0
        ),
        "first_pregame": first_pregame,
        "last_pregame": last_pregame,
        "last_capture_before_start_seconds": lag_seconds,
        "final6_n": int(final6_n or 0),
        "final6_exchange_n": int(
            final6_exchange_n or 0
        ),
        "final6_books": int(final6_books or 0),
        "final6_exchanges": int(
            final6_exchanges or 0
        ),
    }


def build_rows(conn):
    rows = []

    with conn.cursor() as cur:
        membership = discover_freeze_membership(cur)

        observed_multi_start_markets = set()

        for (
            market_id,
            condition_id,
            slug,
            question,
        ) in membership:

            candidates = load_odds_candidates(
                cur,
                market_id,
            )

            (
                canonical,
                distinct_times,
                commence_resolution,
            ) = choose_canonical_game(
                market_id,
                candidates,
            )

            (
                canonical_game_id,
                canonical_oddsapi_id,
                canonical_start,
                home_team,
                away_team,
                match_confidence,
            ) = canonical

            if len(distinct_times) > 1:
                observed_multi_start_markets.add(
                    market_id
                )

            if len(distinct_times) > 1:
                commence_min = min(distinct_times)
                commence_max = max(distinct_times)
            else:
                commence_min = distinct_times[0]
                commence_max = distinct_times[0]

            commence_span_seconds = (
                commence_max - commence_min
            ).total_seconds()

            n_tokens = token_count(
                cur,
                market_id,
            )

            if n_tokens != 2:
                raise RuntimeError(
                    f"{slug}: expected exactly 2 tokens, "
                    f"found {n_tokens}"
                )

            (
                rn1_status,
                rn1_receipt_count,
                rn1_db_count,
                rn1_earliest,
                rn1_latest,
            ) = rn1_information(
                cur,
                condition_id,
            )

            if rn1_status != "complete":
                raise RuntimeError(
                    f"{slug}: RN1 receipt is {rn1_status!r}, "
                    "expected 'complete'"
                )

            if rn1_receipt_count != rn1_db_count:
                raise RuntimeError(
                    f"{slug}: RN1 receipt says "
                    f"{rn1_receipt_count} raw fills but trades table "
                    f"contains {rn1_db_count}. Investigate before freeze."
                )

            unresolved = unresolved_rn1_assets(
                cur,
                condition_id,
            )

            if unresolved != 0:
                raise RuntimeError(
                    f"{slug}: {unresolved} unresolved RN1 assets"
                )

            coverage = odds_coverage(
                cur,
                canonical_game_id,
                canonical_start,
            )

            # Broad H2 requires actual pregame odds AND at least some exchange
            # observations. Tight H2 additionally requires both in the final 6h.
            include_broad = (
                coverage["pregame_n"] > 0
                and coverage["pregame_exchange_n"] > 0
            )

            include_tight = (
                include_broad
                and coverage["final6_n"] > 0
                and coverage["final6_exchange_n"] > 0
            )

            exclusion_reason = ""

            if not include_broad:
                exclusion_reason = (
                    "no_usable_pregame_odds_or_exchange_coverage"
                )
            elif not include_tight:
                exclusion_reason = (
                    "no_final_6h_odds_or_exchange_coverage"
                )

            row = {
                "cohort_version": COHORT_VERSION,
                "market_id": market_id,
                "condition_id": condition_id,
                "slug": slug,
                "question": question or "",

                "canonical_odds_game_id": canonical_game_id,
                "canonical_oddsapi_id": canonical_oddsapi_id,
                "home_team": home_team,
                "away_team": away_team,
                "canonical_commence_time": iso(
                    canonical_start
                ),
                "match_confidence": (
                    match_confidence or ""
                ),

                "commence_candidate_count": len(
                    distinct_times
                ),
                "commence_min": iso(commence_min),
                "commence_max": iso(commence_max),
                "commence_span_seconds": int(
                    commence_span_seconds
                ),
                "commence_resolution": (
                    commence_resolution
                ),

                "token_count": n_tokens,

                "rn1_status": rn1_status,
                "rn1_trade_count": rn1_receipt_count,
                "rn1_db_trade_count": rn1_db_count,
                "rn1_earliest_trade": iso(
                    rn1_earliest
                ),
                "rn1_latest_trade": iso(
                    rn1_latest
                ),

                "pregame_odds_snapshot_count": (
                    coverage["pregame_n"]
                ),
                "pregame_exchange_snapshot_count": (
                    coverage["pregame_exchange_n"]
                ),
                "first_pregame_capture": iso(
                    coverage["first_pregame"]
                ),
                "last_pregame_capture": iso(
                    coverage["last_pregame"]
                ),
                "last_capture_before_start_seconds": (
                    ""
                    if coverage[
                        "last_capture_before_start_seconds"
                    ] is None
                    else round(
                        coverage[
                            "last_capture_before_start_seconds"
                        ],
                        6,
                    )
                ),

                "final_6h_snapshot_count": (
                    coverage["final6_n"]
                ),
                "final_6h_exchange_snapshot_count": (
                    coverage["final6_exchange_n"]
                ),
                "final_6h_book_count": (
                    coverage["final6_books"]
                ),
                "final_6h_exchange_count": (
                    coverage["final6_exchanges"]
                ),

                "include_broad": bool_text(
                    include_broad
                ),
                "include_tight": bool_text(
                    include_tight
                ),
                "exclusion_reason": exclusion_reason,
            }

            rows.append(row)

        if (
            observed_multi_start_markets
            != set(AUDITED_CANONICAL_STARTS)
        ):
            raise RuntimeError(
                "Observed multi-start market set changed.\n"
                f"Expected: {sorted(AUDITED_CANONICAL_STARTS)}\n"
                f"Observed: {sorted(observed_multi_start_markets)}"
            )

    rows.sort(
        key=lambda row: (
            row["canonical_commence_time"],
            row["slug"],
            row["market_id"],
        )
    )

    return rows


def validate_rows(rows):
    if len(rows) != EXPECTED_MARKETS:
        raise RuntimeError(
            f"expected {EXPECTED_MARKETS} rows, got {len(rows)}"
        )

    market_ids = [
        int(row["market_id"])
        for row in rows
    ]

    condition_ids = [
        row["condition_id"]
        for row in rows
    ]

    slugs = [
        row["slug"]
        for row in rows
    ]

    if len(set(market_ids)) != EXPECTED_MARKETS:
        raise RuntimeError("market_id uniqueness failure")

    if len(set(condition_ids)) != EXPECTED_MARKETS:
        raise RuntimeError("condition_id uniqueness failure")

    if len(set(slugs)) != EXPECTED_MARKETS:
        raise RuntimeError("slug uniqueness failure")

    if any(int(row["token_count"]) != 2 for row in rows):
        raise RuntimeError("not every cohort market has exactly 2 tokens")

    if any(row["rn1_status"] != "complete" for row in rows):
        raise RuntimeError("not every cohort market has complete RN1 collection")

    receipt_total = sum(
        int(row["rn1_trade_count"])
        for row in rows
    )

    db_total = sum(
        int(row["rn1_db_trade_count"])
        for row in rows
    )

    if receipt_total != EXPECTED_RN1_RAW_FILLS:
        raise RuntimeError(
            f"RN1 receipt total changed: expected "
            f"{EXPECTED_RN1_RAW_FILLS}, got {receipt_total}"
        )

    if db_total != EXPECTED_RN1_RAW_FILLS:
        raise RuntimeError(
            f"RN1 trades-table total changed: expected "
            f"{EXPECTED_RN1_RAW_FILLS}, got {db_total}"
        )

    broad = [
        row
        for row in rows
        if row["include_broad"] == "true"
    ]

    tight = [
        row
        for row in rows
        if row["include_tight"] == "true"
    ]

    excluded = {
        row["slug"]
        for row in rows
        if row["include_tight"] != "true"
    }

    if len(broad) != EXPECTED_BROAD:
        raise RuntimeError(
            f"broad cohort expected {EXPECTED_BROAD}, "
            f"got {len(broad)}"
        )

    if len(tight) != EXPECTED_TIGHT:
        raise RuntimeError(
            f"tight cohort expected {EXPECTED_TIGHT}, "
            f"got {len(tight)}"
        )

    if excluded != EXPECTED_TIGHT_EXCLUSIONS:
        raise RuntimeError(
            "tight exclusion set changed.\n"
            f"Expected: {sorted(EXPECTED_TIGHT_EXCLUSIONS)}\n"
            f"Observed: {sorted(excluded)}"
        )

    for row in rows:
        if row["slug"] in EXPECTED_TIGHT_EXCLUSIONS:
            if int(row["final_6h_snapshot_count"]) != 0:
                raise RuntimeError(
                    f"{row['slug']}: expected zero final-6h snapshots"
                )

            if int(
                row["final_6h_exchange_snapshot_count"]
            ) != 0:
                raise RuntimeError(
                    f"{row['slug']}: expected zero final-6h "
                    "exchange snapshots"
                )


def csv_bytes(rows) -> bytes:
    import io

    buffer = io.StringIO(newline="")

    writer = csv.DictWriter(
        buffer,
        fieldnames=FIELDS,
        lineterminator="\n",
    )

    writer.writeheader()

    for row in rows:
        writer.writerow(row)

    return buffer.getvalue().encode("utf-8")


def write_freeze(rows):
    validate_rows(rows)

    payload = csv_bytes(rows)

    digest = hashlib.sha256(
        payload
    ).hexdigest()

    COHORT_CSV.write_bytes(payload)

    excluded = [
        {
            "market_id": int(row["market_id"]),
            "slug": row["slug"],
            "reason": row["exclusion_reason"],
        }
        for row in rows
        if row["include_tight"] != "true"
    ]

    manifest = {
        "cohort": COHORT_VERSION,
        "definition_version": 1,
        "sport": "MLB",
        "wallet": WALLET,

        "frozen_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),

        "membership_basis": (
            "42 MLB Polymarket markets with complete RN1 targeted "
            "collection at initial H2-v1 freeze; membership thereafter "
            "fixed by committed cohort CSV"
        ),

        "canonical_time_policy": (
            "single linked commencement time unless explicitly listed in "
            "audited_start_overrides; five UTC/local-date collision cases "
            "were manually reviewed before freeze"
        ),

        "coverage_policy": {
            "broad": (
                "at least one pregame odds snapshot and at least one "
                "pregame exchange snapshot for the canonical Odds API game"
            ),
            "tight": (
                "broad coverage plus at least one odds snapshot and one "
                "exchange snapshot in [T-6h, T0] for the canonical game"
            ),
            "coverage_clock": "odds_snapshots.capture_time",
            "note": (
                "M1 measures collector availability. M3 will use "
                "book_last_update where appropriate for lead-lag timing."
            ),
        },

        "broad_n": EXPECTED_BROAD,
        "tight_n": EXPECTED_TIGHT,
        "rn1_raw_fill_count": EXPECTED_RN1_RAW_FILLS,

        "tight_exclusions": excluded,

        "audited_start_overrides": {
            str(market_id): timestamp
            for market_id, timestamp
            in sorted(
                AUDITED_CANONICAL_STARTS.items()
            )
        },

        "market_ids": [
            int(row["market_id"])
            for row in rows
        ],

        "csv_file": COHORT_CSV.name,
        "csv_sha256": digest,
    }

    MANIFEST_JSON.write_text(
        json.dumps(
            manifest,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )

    return digest


def read_csv_rows():
    if not COHORT_CSV.exists():
        raise RuntimeError(
            f"{COHORT_CSV} does not exist"
        )

    with COHORT_CSV.open(
        newline="",
        encoding="utf-8",
    ) as f:
        return list(
            csv.DictReader(f)
        )


def validate_frozen_artifacts():
    if not MANIFEST_JSON.exists():
        raise RuntimeError(
            f"{MANIFEST_JSON} does not exist"
        )

    rows = read_csv_rows()

    validate_rows(rows)

    manifest = json.loads(
        MANIFEST_JSON.read_text()
    )

    digest = hashlib.sha256(
        COHORT_CSV.read_bytes()
    ).hexdigest()

    expected_digest = manifest.get(
        "csv_sha256"
    )

    if digest != expected_digest:
        raise RuntimeError(
            "cohort CSV SHA-256 does not match manifest"
        )

    manifest_ids = [
        int(value)
        for value in manifest["market_ids"]
    ]

    csv_ids = [
        int(row["market_id"])
        for row in rows
    ]

    if manifest_ids != csv_ids:
        raise RuntimeError(
            "manifest market membership differs from cohort CSV"
        )

    return rows, digest


def print_summary(rows, digest=None):
    broad_n = sum(
        row["include_broad"] == "true"
        for row in rows
    )

    tight_n = sum(
        row["include_tight"] == "true"
        for row in rows
    )

    fills = sum(
        int(row["rn1_trade_count"])
        for row in rows
    )

    print()
    print("H2-v1 canonical cohort")
    print("=======================")
    print(f"markets:              {len(rows)}")
    print(f"broad:                {broad_n}")
    print(f"tight:                {tight_n}")
    print(f"RN1 fills:            {fills}")
    print(
        "multi-start audited:  "
        f"{sum(int(r['commence_candidate_count']) > 1 for r in rows)}"
    )

    print()
    print("tight exclusions:")

    for row in rows:
        if row["include_tight"] != "true":
            print(
                f"  {row['slug']}  "
                f"{row['exclusion_reason']}"
            )

    print()
    print("audited canonical starts:")

    for row in rows:
        if int(
            row["commence_candidate_count"]
        ) > 1:
            print(
                f"  {row['slug']}  "
                f"{row['canonical_commence_time']}"
            )

    if digest:
        print()
        print(f"CSV SHA-256: {digest}")


def main():
    parser = argparse.ArgumentParser()

    mode = parser.add_mutually_exclusive_group(
        required=True
    )

    mode.add_argument(
        "--freeze",
        action="store_true",
        help="Create the initial immutable H2-v1 CSV + manifest",
    )

    mode.add_argument(
        "--validate",
        action="store_true",
        help="Validate already-frozen CSV + manifest without rewriting",
    )

    args = parser.parse_args()

    if args.freeze:
        if COHORT_CSV.exists() or MANIFEST_JSON.exists():
            raise SystemExit(
                "REFUSING TO RE-FREEZE: H2-v1 artifact already exists. "
                "Use --validate instead."
            )

        conn = connect()

        try:
            rows = build_rows(conn)
        finally:
            conn.close()

        digest = write_freeze(rows)

        print_summary(
            rows,
            digest=digest,
        )

        print()
        print("FREEZE COMPLETE")
        print(f"  {COHORT_CSV}")
        print(f"  {MANIFEST_JSON}")

        return

    rows, digest = validate_frozen_artifacts()

    print_summary(
        rows,
        digest=digest,
    )

    print()
    print("VALIDATION PASSED")


if __name__ == "__main__":
    main()
