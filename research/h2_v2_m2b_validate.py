from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import dotenv_values


REPO = Path(__file__).resolve().parents[1]
R = REPO / "research"

CONTRACT = R / "h2_v2_m2b_contract.json"

RESULTS = (
    R / "h2_v2_m2b_checkpoint_results.jsonl"
)

VALIDATION = (
    R / "h2_v2_m2b_validation.json"
)

EXPECTED_CONTRACT_SHA = (
    "86babc92e1a32bbe95361bd66a24d09b"
    "0c729082589ea8cd7a5ea18e8bbfdb4c"
)

EXPECTED_PARENT = {
    "m2_contract": (
        R / "h2_v2_m2_contract.json",
        "4a09e3124bd0e05624a577916b10a712"
        "802bcc1096f6218a765c3830cf239cc4",
    ),
    "m2_builder": (
        R / "h2_v2_m2_build.py",
        "324ccba8cc74d0853de640d5758c3cc7"
        "7c7fbba30cad81022adac138e5022ab8",
    ),
    "m2_tape": (
        R / "h2_v2_m2_tape.jsonl",
        "88a1bd4cbaf3254850c0dc1c4c2c305e"
        "18db68700a5e90d7e86f64a74706d312",
    ),
    "m2_validation": (
        R / "h2_v2_m2_validation.json",
        "06983e20c4fcb1d09254221e99213a2e"
        "8497088263fb20d95955a884e7005f5e",
    ),
    "m2_failure_diagnosis": (
        R / "h2_v2_m2_failure_diagnosis.json",
        "481ef1eb7116da2bf83490253080e400c"
        "d10265fb03d6e465fca5bc1a92bb8b7",
    ),
}

RUN_ID = 11691
MIN_PAIRS = 100
MIN_TOKENS = 9


def sha256_file(path: Path) -> str:
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


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


def connect():
    env = dotenv_values(
        REPO / ".env"
    )

    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="quantlab",
        user="quantlab",
        password=env["PG_PASSWORD"],
        cursor_factory=(
            psycopg2.extras.RealDictCursor
        ),
    )

    conn.set_session(
        readonly=True,
        autocommit=False,
    )

    return conn


def normalized_book(raw):
    out = {}

    for level in raw or []:
        if (
            not isinstance(
                level,
                (list, tuple),
            )
            or len(level) < 2
        ):
            raise RuntimeError(
                f"invalid book level: {level!r}"
            )

        price = int(level[0])
        size = Decimal(str(level[1]))

        if not 0 <= price <= 1000:
            raise RuntimeError(
                f"invalid price_mc={price}"
            )

        if size < 0:
            raise RuntimeError(
                f"negative size={size}"
            )

        if price in out:
            raise RuntimeError(
                f"duplicate price level={price}"
            )

        if size > 0:
            out[price] = size

    return out


def window_bounds(cur):
    cur.execute(
        """
        SELECT
            min(c.capture_time)
                AS window_start,

            max(c.capture_time)
                + interval '500 milliseconds'
                AS window_end

        FROM h2_v2_sharp_consensus c

        JOIN h2_v2_sharp_polls p
          ON p.id = c.poll_id

        JOIN odds_games g
          ON g.id = c.odds_game_id

        WHERE
            p.collector_run_id = %s
            AND c.eligible
            AND g.commence_time
                > c.capture_time
            AND g.commence_time
                <= c.capture_time
                   + interval '23 hours'
        """,
        (RUN_ID,),
    )

    return dict(cur.fetchone())


def target_tokens(cur):
    cur.execute(
        """
        SELECT DISTINCT
            t.id AS token_id

        FROM h2_v2_sharp_consensus c

        JOIN h2_v2_sharp_polls p
          ON p.id = c.poll_id

        JOIN odds_games g
          ON g.id = c.odds_game_id

        JOIN h2_v2_market_matches mm
          ON mm.odds_game_id =
             c.odds_game_id

        JOIN tokens t
          ON t.market_id =
             mm.polymarket_market_id
         AND lower(trim(t.outcome))
             =
             lower(trim(c.outcome_team))

        WHERE
            p.collector_run_id = %s
            AND c.eligible
            AND g.commence_time
                > c.capture_time
            AND g.commence_time
                <= c.capture_time
                   + interval '23 hours'

        ORDER BY t.id
        """,
        (RUN_ID,),
    )

    return [
        x["token_id"]
        for x in cur.fetchall()
    ]


def snapshots_for_token(
    cur,
    token_id,
    start,
    end,
):
    cur.execute(
        """
        SELECT
            token_id,
            capture_time,
            bids,
            asks,
            source,
            connection_id,
            ingest_sequence,
            book_generation

        FROM book_snapshots

        WHERE
            token_id = %s
            AND capture_time >= %s
            AND capture_time <= %s
            AND watchlist_rule = 'h2_v2'
            AND source IN (
                'ws_book',
                'rest_book'
            )
            AND book_generation
                IS NOT NULL

        ORDER BY
            capture_time ASC,
            ingest_sequence ASC NULLS LAST
        """,
        (
            token_id,
            start,
            end,
        ),
    )

    return [
        dict(x)
        for x in cur.fetchall()
    ]


def interval_deltas(
    cur,
    token_id,
    base,
    checkpoint,
):
    cur.execute(
        """
        SELECT
            capture_time,
            price_mc,
            size,
            side,
            connection_id,
            ingest_sequence,
            change_index,
            book_generation

        FROM book_deltas

        WHERE
            token_id = %s
            AND capture_time
                > %s
            AND capture_time
                < %s
            AND watchlist_rule =
                'h2_v2'

        ORDER BY
            ingest_sequence ASC,
            change_index ASC,
            capture_time ASC
        """,
        (
            token_id,
            base["capture_time"],
            checkpoint["capture_time"],
        ),
    )

    return [
        dict(x)
        for x in cur.fetchall()
    ]


def evaluate_pair(
    cur,
    token_id,
    pair_index,
    base,
    checkpoint,
):
    base_gen = base[
        "book_generation"
    ]

    checkpoint_gen = checkpoint[
        "book_generation"
    ]

    if checkpoint["source"] != "ws_book":
        return None

    if base_gen != checkpoint_gen:
        return None

    deltas = interval_deltas(
        cur,
        token_id,
        base,
        checkpoint,
    )

    same_gen = [
        d
        for d in deltas
        if d["book_generation"]
        == base_gen
    ]

    # Primary M2b requires at least one
    # actual delta to test replay semantics.
    if not same_gen:
        return None

    generation_contamination = (
        len(same_gen) != len(deltas)
    )

    checkpoint_conn = checkpoint[
        "connection_id"
    ]

    checkpoint_seq = checkpoint[
        "ingest_sequence"
    ]

    if (
        checkpoint_conn is None
        or checkpoint_seq is None
    ):
        raise RuntimeError(
            "ws_book checkpoint missing "
            "connection/sequence"
        )

    connection_contamination = False
    sequence_violations = 0
    lookahead = 0

    replay = []

    if base["source"] == "ws_book":
        if (
            base["connection_id"]
            != checkpoint_conn
        ):
            connection_contamination = True

        if base["ingest_sequence"] is None:
            sequence_violations += 1

        for d in same_gen:
            if (
                d["connection_id"]
                != checkpoint_conn
            ):
                connection_contamination = True
                continue

            if (
                base["ingest_sequence"]
                is None
                or not (
                    base["ingest_sequence"]
                    < d["ingest_sequence"]
                    < checkpoint_seq
                )
            ):
                sequence_violations += 1
                continue

            replay.append(d)

    elif base["source"] == "rest_book":
        for d in same_gen:
            if (
                d["connection_id"]
                != checkpoint_conn
            ):
                connection_contamination = True
                continue

            if not (
                d["ingest_sequence"]
                < checkpoint_seq
            ):
                sequence_violations += 1
                continue

            if not (
                d["capture_time"]
                > base["capture_time"]
            ):
                sequence_violations += 1
                continue

            replay.append(d)

    else:
        raise RuntimeError(
            "unexpected base source"
        )

    for d in replay:
        if (
            d["capture_time"]
            >= checkpoint["capture_time"]
        ):
            lookahead += 1

    bids = normalized_book(
        base["bids"]
    )

    asks = normalized_book(
        base["asks"]
    )

    for d in replay:
        price = int(
            d["price_mc"]
        )

        size = Decimal(
            str(d["size"])
        )

        if d["side"] == "B":
            book = bids

        elif d["side"] == "S":
            book = asks

        else:
            raise RuntimeError(
                f"bad side={d['side']!r}"
            )

        if size == 0:
            book.pop(
                price,
                None,
            )
        else:
            book[price] = size

    target_bids = normalized_book(
        checkpoint["bids"]
    )

    target_asks = normalized_book(
        checkpoint["asks"]
    )

    bid_presence_mismatches = len(
        set(bids)
        ^ set(target_bids)
    )

    ask_presence_mismatches = len(
        set(asks)
        ^ set(target_asks)
    )

    bid_size_mismatches = sum(
        bids[p] != target_bids[p]
        for p in (
            set(bids)
            & set(target_bids)
        )
    )

    ask_size_mismatches = sum(
        asks[p] != target_asks[p]
        for p in (
            set(asks)
            & set(target_asks)
        )
    )

    size_mismatches = (
        bid_size_mismatches
        + ask_size_mismatches
    )

    exact_match = (
        bids == target_bids
        and asks == target_asks
    )

    return {
        "token_id":
            token_id,

        "pair_index":
            pair_index,

        "base_source":
            base["source"],

        "checkpoint_source":
            checkpoint["source"],

        "book_generation":
            base_gen,

        "base_time":
            base["capture_time"],

        "checkpoint_time":
            checkpoint["capture_time"],

        "base_connection_id":
            (
                str(base["connection_id"])
                if base["connection_id"]
                is not None
                else None
            ),

        "checkpoint_connection_id":
            (
                str(checkpoint_conn)
            ),

        "base_ingest_sequence":
            base["ingest_sequence"],

        "checkpoint_ingest_sequence":
            checkpoint_seq,

        "interval_deltas":
            len(deltas),

        "same_generation_deltas":
            len(same_gen),

        "replayed_deltas":
            len(replay),

        "generation_contamination":
            generation_contamination,

        "connection_contamination":
            connection_contamination,

        "sequence_violations":
            sequence_violations,

        "lookahead":
            lookahead,

        "bid_presence_mismatches":
            bid_presence_mismatches,

        "ask_presence_mismatches":
            ask_presence_mismatches,

        "bid_size_mismatches":
            bid_size_mismatches,

        "ask_size_mismatches":
            ask_size_mismatches,

        "size_mismatches":
            size_mismatches,

        "exact_full_book_match":
            exact_match,
    }


def main():
    actual_contract_sha = (
        sha256_file(CONTRACT)
    )

    if (
        actual_contract_sha
        != EXPECTED_CONTRACT_SHA
    ):
        raise SystemExit(
            "M2b contract SHA mismatch"
        )

    for name, (
        path,
        expected_sha,
    ) in EXPECTED_PARENT.items():
        actual = sha256_file(
            path
        )

        if actual != expected_sha:
            raise SystemExit(
                f"parent SHA mismatch: {name}"
            )

    contract = json.loads(
        CONTRACT.read_text(
            encoding="utf-8"
        )
    )

    assert (
        contract["version"]
        == "H2-v2-M2b-CONTRACT-1"
    )

    assert (
        contract[
            "primary_success_rule"
        ][
            "minimum_checkpoint_pairs"
        ]
        == MIN_PAIRS
    )

    assert (
        contract[
            "primary_success_rule"
        ][
            "minimum_distinct_tokens"
        ]
        == MIN_TOKENS
    )

    assert (
        contract[
            "primary_success_rule"
        ][
            "full_book_exact_match_rate"
        ]
        == "100%"
    )

    assert (
        contract[
            "engineering_dataset"
        ][
            "performance_use"
        ]
        is False
    )

    conn = connect()

    pair_results = []

    try:
        with conn.cursor() as cur:
            bounds = window_bounds(
                cur
            )

            tokens = target_tokens(
                cur
            )

            for token_id in tokens:
                snaps = snapshots_for_token(
                    cur,
                    token_id,
                    bounds["window_start"],
                    bounds["window_end"],
                )

                for i in range(
                    1,
                    len(snaps),
                ):
                    base = snaps[i - 1]
                    checkpoint = snaps[i]

                    result = evaluate_pair(
                        cur,
                        token_id,
                        i,
                        base,
                        checkpoint,
                    )

                    if result is not None:
                        pair_results.append(
                            result
                        )

        conn.rollback()

    finally:
        conn.close()

    # Deterministic order.
    pair_results.sort(
        key=lambda x: (
            x["token_id"],
            x["base_time"],
            x["checkpoint_time"],
        )
    )

    result_text = "".join(
        json.dumps(
            clean(x),
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
        for x in pair_results
    )

    RESULTS.write_text(
        result_text,
        encoding="utf-8",
    )

    results_sha = sha256_file(
        RESULTS
    )

    validator_sha = sha256_file(
        Path(__file__)
    )

    pairs = len(
        pair_results
    )

    distinct_tokens = len({
        x["token_id"]
        for x in pair_results
    })

    base_sources = Counter(
        x["base_source"]
        for x in pair_results
    )

    exact_matches = sum(
        x["exact_full_book_match"]
        for x in pair_results
    )

    failed_pairs = (
        pairs - exact_matches
    )

    bid_level_mismatches = sum(
        x["bid_presence_mismatches"]
        for x in pair_results
    )

    ask_level_mismatches = sum(
        x["ask_presence_mismatches"]
        for x in pair_results
    )

    size_mismatches = sum(
        x["size_mismatches"]
        for x in pair_results
    )

    generation_contamination = sum(
        x["generation_contamination"]
        for x in pair_results
    )

    connection_contamination = sum(
        x["connection_contamination"]
        for x in pair_results
    )

    sequence_violations = sum(
        x["sequence_violations"]
        for x in pair_results
    )

    lookahead = sum(
        x["lookahead"]
        for x in pair_results
    )

    replayed_deltas = sum(
        x["replayed_deltas"]
        for x in pair_results
    )

    evidence_pass = (
        pairs >= MIN_PAIRS
        and distinct_tokens >= MIN_TOKENS
        and base_sources.get(
            "ws_book",
            0,
        ) > 0
        and base_sources.get(
            "rest_book",
            0,
        ) > 0
    )

    replay_pass = (
        pairs > 0
        and exact_matches == pairs
        and bid_level_mismatches == 0
        and ask_level_mismatches == 0
        and size_mismatches == 0
        and generation_contamination == 0
        and connection_contamination == 0
        and sequence_violations == 0
        and lookahead == 0
    )

    overall = (
        evidence_pass
        and replay_pass
    )

    validation = {
        "version":
            "H2-v2-M2b-VALIDATION-1",

        "contract_sha256":
            actual_contract_sha,

        "validator_sha256":
            validator_sha,

        "checkpoint_results_sha256":
            results_sha,

        "engineering_dataset": {
            "sharp_collector_run_id":
                RUN_ID,

            "window_start":
                bounds["window_start"],

            "window_end":
                bounds["window_end"],

            "target_tokens":
                len(tokens),
        },

        "primary_checkpoint_test": {
            "eligible_pairs":
                pairs,

            "distinct_tokens":
                distinct_tokens,

            "base_sources":
                dict(
                    sorted(
                        base_sources.items()
                    )
                ),

            "replayed_deltas":
                replayed_deltas,

            "exact_full_book_matches":
                exact_matches,

            "failed_pairs":
                failed_pairs,

            "exact_match_rate":
                (
                    Decimal(exact_matches)
                    / Decimal(pairs)
                    if pairs
                    else Decimal("0")
                ),

            "bid_level_mismatches":
                bid_level_mismatches,

            "ask_level_mismatches":
                ask_level_mismatches,

            "size_mismatches":
                size_mismatches,

            "generation_contamination":
                generation_contamination,

            "connection_contamination":
                connection_contamination,

            "sequence_violations":
                sequence_violations,

            "lookahead":
                lookahead,
        },

        "decisions": {
            "minimum_evidence_pass":
                evidence_pass,

            "replay_validation_pass":
                replay_pass,

            "m2b_overall_pass":
                overall,
        },

        "interpretation": (
            "M2b validates exact full-book "
            "price-level and size replay semantics "
            "against later independent ws_book "
            "checkpoints. M2 remains frozen FAIL. "
            "No edge, threshold, trade, fill, PnL, "
            "resolution, calibration, or OOS "
            "strategy result is evaluated."
        ),
    }

    VALIDATION.write_text(
        json.dumps(
            clean(validation),
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    print(
        "H2-v2 M2b REPLAY VALIDATION COMPLETE"
    )

    print(
        "contract SHA:",
        actual_contract_sha,
    )

    print(
        "eligible checkpoint pairs:",
        pairs,
    )

    print(
        "distinct tokens:",
        distinct_tokens,
    )

    print(
        "base sources:",
        dict(
            sorted(
                base_sources.items()
            )
        ),
    )

    print(
        "replayed deltas:",
        replayed_deltas,
    )

    print()

    print(
        "exact full-book matches:",
        f"{exact_matches}/{pairs}",
    )

    print(
        "failed pairs:",
        failed_pairs,
    )

    print(
        "bid level mismatches:",
        bid_level_mismatches,
    )

    print(
        "ask level mismatches:",
        ask_level_mismatches,
    )

    print(
        "size mismatches:",
        size_mismatches,
    )

    print(
        "generation contamination:",
        generation_contamination,
    )

    print(
        "connection contamination:",
        connection_contamination,
    )

    print(
        "sequence violations:",
        sequence_violations,
    )

    print(
        "lookahead:",
        lookahead,
    )

    print()

    print(
        "minimum evidence:",
        evidence_pass,
    )

    print(
        "replay validation:",
        replay_pass,
    )

    print(
        "M2b overall:",
        overall,
    )

    print(
        "results:",
        RESULTS.relative_to(REPO),
    )

    print(
        "validation:",
        VALIDATION.relative_to(REPO),
    )

    print(
        "NO EDGE / NO TRADES / NO PNL"
    )


if __name__ == "__main__":
    main()
