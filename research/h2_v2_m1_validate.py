from __future__ import annotations

import hashlib
import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import dotenv_values


REPO = Path(__file__).resolve().parents[1]

CONTRACT = REPO / "research/h2_v2_m1_data_contract.json"
OUT = REPO / "research/h2_v2_m1_validation.json"

EXPECTED_CONTRACT_SHA = (
    "c539abbf046e173d3f50f57b071d483b"
    "cb549b034cf9e7716151fe1752d1143b"
)

SHARP_MIN_RATE = Decimal("0.80")


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


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
        return [clean(v) for v in x]

    return x


def q1(cur, sql, params=()):
    cur.execute(sql, params)
    row = cur.fetchone()
    return dict(row)


def qa(cur, sql, params=()):
    cur.execute(sql, params)
    return [
        dict(r)
        for r in cur.fetchall()
    ]


def rate(n, d):
    if not d:
        return Decimal("0")

    return (
        Decimal(n)
        / Decimal(d)
    )


def main():
    actual_contract_sha = sha256_file(CONTRACT)

    if actual_contract_sha != EXPECTED_CONTRACT_SHA:
        raise SystemExit(
            "H2-v2 M1 contract SHA mismatch"
        )

    env = dotenv_values(REPO / ".env")

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

    try:
        with conn.cursor() as cur:

            run = q1(
                cur,
                """
                SELECT max(id) AS run_id
                FROM collector_runs
                WHERE component =
                      'h2_v2_sharp_collector'
                """
            )

            run_id = run["run_id"]

            if run_id is None:
                raise RuntimeError(
                    "no H2-v2 sharp run"
                )

            sharp_run = q1(
                cur,
                """
                SELECT
                    count(*) AS polls,
                    min(received_at) AS first_poll,
                    max(received_at) AS last_poll,
                    extract(
                        epoch FROM (
                            max(received_at)
                            - min(received_at)
                        )
                    ) AS span_seconds,
                    sum(credits_last) AS credits_consumed,
                    min(credits_remaining)
                        AS final_credits_remaining
                FROM h2_v2_sharp_polls
                WHERE collector_run_id = %s
                """,
                (run_id,),
            )

            sharp_outcomes = q1(
                cur,
                """
                SELECT
                    count(*) AS outcome_snapshots,
                    count(*) FILTER (
                        WHERE c.eligible
                    ) AS eligible_outcomes,
                    count(DISTINCT c.poll_id)
                        AS polls,
                    count(DISTINCT c.odds_game_id)
                        AS games
                FROM h2_v2_sharp_consensus c
                JOIN h2_v2_sharp_polls p
                  ON p.id = c.poll_id
                WHERE p.collector_run_id = %s
                """,
                (run_id,),
            )

            sharp_games = q1(
                cur,
                """
                WITH gp AS (
                    SELECT
                        c.poll_id,
                        c.odds_game_id,
                        count(*) AS outcomes,
                        count(*) FILTER (
                            WHERE c.eligible
                        ) AS eligible_outcomes
                    FROM h2_v2_sharp_consensus c
                    JOIN h2_v2_sharp_polls p
                      ON p.id = c.poll_id
                    WHERE p.collector_run_id = %s
                    GROUP BY
                        c.poll_id,
                        c.odds_game_id
                )
                SELECT
                    count(*) AS game_snapshots,

                    count(*) FILTER (
                        WHERE outcomes = 2
                          AND eligible_outcomes = 2
                    ) AS eligible_game_snapshots,

                    count(*) FILTER (
                        WHERE outcomes <> 2
                           OR eligible_outcomes
                              NOT IN (0, 2)
                    ) AS asymmetric_or_bad_games,

                    min(outcomes)
                        AS min_outcomes_per_game,

                    max(outcomes)
                        AS max_outcomes_per_game

                FROM gp
                """,
                (run_id,),
            )

            quote_integrity = q1(
                cur,
                """
                SELECT
                    count(*) AS quote_rows,

                    count(*) FILTER (
                        WHERE q.pair_complete
                    ) AS paired_rows,

                    count(*) FILTER (
                        WHERE q.is_fresh
                    ) AS fresh_rows,

                    count(*) FILTER (
                        WHERE q.capture_time
                              >= g.commence_time
                    ) AS inplay_rows,

                    count(*) FILTER (
                        WHERE q.pair_complete
                          AND (
                              q.source_update_time
                                  IS NULL
                              OR q.back_decimal
                                  IS NULL
                              OR q.lay_decimal
                                  IS NULL
                              OR q.p_back
                                  IS NULL
                              OR q.p_lay
                                  IS NULL
                              OR q.family_midpoint
                                  IS NULL
                          )
                    ) AS incomplete_pair_rows,

                    count(*) FILTER (
                        WHERE q.is_fresh IS DISTINCT FROM (
                            q.pair_complete
                            AND q.source_age_seconds
                                BETWEEN 0 AND 25
                        )
                    ) AS bad_freshness_rows,

                    count(*) FILTER (
                        WHERE q.source_update_time
                              IS NOT NULL
                          AND abs(
                              q.source_age_seconds
                              - extract(
                                  epoch FROM (
                                      q.capture_time
                                      - q.source_update_time
                                  )
                              )
                          ) > 0.001
                    ) AS bad_source_age_rows,

                    count(*) FILTER (
                        WHERE q.pair_complete
                          AND abs(
                              q.family_midpoint
                              - (
                                  (
                                      1 / q.back_decimal
                                      + 1 / q.lay_decimal
                                  ) / 2
                              )
                          ) > 0.000000000001
                    ) AS bad_midpoint_rows

                FROM h2_v2_exchange_quotes q
                JOIN h2_v2_sharp_polls p
                  ON p.id = q.poll_id
                JOIN odds_games g
                  ON g.id = q.odds_game_id
                WHERE p.collector_run_id = %s
                """,
                (run_id,),
            )

            consensus_integrity = q1(
                cur,
                """
                SELECT
                    count(*) AS consensus_rows,

                    count(*) FILTER (
                        WHERE c.eligible
                        IS DISTINCT FROM (
                            c.fresh_family_count >= 2
                            AND c.consensus_prob
                                IS NOT NULL
                        )
                    ) AS bad_eligibility_rows,

                    count(*) FILTER (
                        WHERE c.fresh_family_count
                              > c.paired_family_count
                    ) AS bad_family_count_rows

                FROM h2_v2_sharp_consensus c
                JOIN h2_v2_sharp_polls p
                  ON p.id = c.poll_id
                WHERE p.collector_run_id = %s
                """,
                (run_id,),
            )

            books = qa(
                cur,
                """
                SELECT
                    q.bookmaker,
                    count(*) AS rows,
                    count(*) FILTER (
                        WHERE q.pair_complete
                    ) AS paired_rows,
                    count(*) FILTER (
                        WHERE q.is_fresh
                    ) AS fresh_rows,
                    percentile_cont(0.5)
                    WITHIN GROUP (
                        ORDER BY q.source_age_seconds
                    ) FILTER (
                        WHERE q.pair_complete
                    ) AS median_paired_age_s

                FROM h2_v2_exchange_quotes q
                JOIN h2_v2_sharp_polls p
                  ON p.id = q.poll_id
                WHERE p.collector_run_id = %s
                GROUP BY q.bookmaker
                ORDER BY q.bookmaker
                """,
                (run_id,),
            )

            per_game = qa(
                cur,
                """
                WITH gp AS (
                    SELECT
                        c.poll_id,
                        c.odds_game_id,
                        count(*) AS outcomes,
                        count(*) FILTER (
                            WHERE c.eligible
                        ) AS eligible_outcomes
                    FROM h2_v2_sharp_consensus c
                    JOIN h2_v2_sharp_polls p
                      ON p.id = c.poll_id
                    WHERE p.collector_run_id = %s
                    GROUP BY
                        c.poll_id,
                        c.odds_game_id
                )
                SELECT
                    g.id AS odds_game_id,
                    g.away_team,
                    g.home_team,
                    g.commence_time,
                    count(*) AS snapshots,
                    count(*) FILTER (
                        WHERE gp.outcomes = 2
                          AND gp.eligible_outcomes = 2
                    ) AS eligible_snapshots
                FROM gp
                JOIN odds_games g
                  ON g.id = gp.odds_game_id
                GROUP BY
                    g.id,
                    g.away_team,
                    g.home_team,
                    g.commence_time
                ORDER BY g.commence_time
                """,
                (run_id,),
            )

            mapping = q1(
                cur,
                """
                SELECT
                    count(*) AS matches,
                    count(
                        DISTINCT polymarket_market_id
                    ) AS markets
                FROM h2_v2_market_matches
                """
            )

            tokens = q1(
                cur,
                """
                SELECT
                    count(*) AS tokens,
                    count(
                        DISTINCT t.venue_token_id
                    ) AS distinct_tokens
                FROM h2_v2_market_matches mm
                JOIN tokens t
                  ON t.market_id =
                     mm.polymarket_market_id
                """
            )

            pm_config = q1(
                cur,
                """
                WITH latest AS (
                    SELECT DISTINCT ON (
                        polymarket_market_id
                    )
                        *
                    FROM h2_v2_pm_market_config
                    ORDER BY
                        polymarket_market_id,
                        observed_at DESC
                )
                SELECT
                    count(*) AS markets,

                    count(*) FILTER (
                        WHERE minimum_order_size
                              IS NOT NULL
                    ) AS with_min_order,

                    count(*) FILTER (
                        WHERE minimum_tick_size
                              IS NOT NULL
                    ) AS with_tick,

                    count(*) FILTER (
                        WHERE fee_rate
                              IS NOT NULL
                          AND fee_exponent
                              IS NOT NULL
                          AND taker_only
                              IS NOT NULL
                    ) AS with_fee_curve,

                    count(*) FILTER (
                        WHERE token_count = 2
                    ) AS with_two_tokens

                FROM latest
                """
            )

            pm_window = q1(
                cur,
                """
                WITH w AS (
                    SELECT
                        min(received_at) AS t0,
                        max(received_at) AS t1
                    FROM h2_v2_sharp_polls
                    WHERE collector_run_id = %s
                ),
                ht AS (
                    SELECT DISTINCT
                        t.id AS token_id
                    FROM h2_v2_market_matches mm
                    JOIN tokens t
                      ON t.market_id =
                         mm.polymarket_market_id
                )
                SELECT
                    (
                        SELECT count(*)
                        FROM book_deltas d
                        JOIN ht
                          ON ht.token_id =
                             d.token_id
                        CROSS JOIN w
                        WHERE
                            d.capture_time >= w.t0
                            AND d.capture_time
                                <= w.t1
                                   + interval '5 seconds'
                            AND d.watchlist_rule =
                                'h2_v2'
                    ) AS delta_rows,

                    (
                        SELECT count(
                            DISTINCT d.token_id
                        )
                        FROM book_deltas d
                        JOIN ht
                          ON ht.token_id =
                             d.token_id
                        CROSS JOIN w
                        WHERE
                            d.capture_time >= w.t0
                            AND d.capture_time
                                <= w.t1
                                   + interval '5 seconds'
                            AND d.watchlist_rule =
                                'h2_v2'
                    ) AS tokens_with_delta,

                    (
                        SELECT count(*)
                        FROM book_snapshots b
                        JOIN ht
                          ON ht.token_id =
                             b.token_id
                        CROSS JOIN w
                        WHERE
                            b.capture_time >= w.t0
                            AND b.capture_time
                                <= w.t1
                                   + interval '5 seconds'
                            AND b.watchlist_rule =
                                'h2_v2'
                    ) AS full_book_rows,

                    (
                        SELECT count(
                            DISTINCT b.token_id
                        )
                        FROM book_snapshots b
                        JOIN ht
                          ON ht.token_id =
                             b.token_id
                        CROSS JOIN w
                        WHERE
                            b.capture_time >= w.t0
                            AND b.capture_time
                                <= w.t1
                                   + interval '5 seconds'
                            AND b.watchlist_rule =
                                'h2_v2'
                    ) AS tokens_with_full_book,

                    (
                        SELECT count(
                            DISTINCT ts.token_id
                        )
                        FROM tob_snapshots ts
                        JOIN ht
                          ON ht.token_id =
                             ts.token_id
                        CROSS JOIN w
                        WHERE
                            ts.capture_time >= w.t0
                            AND ts.capture_time
                                <= w.t1
                                   + interval '5 seconds'
                            AND ts.watchlist_rule =
                                'h2_v2'
                            AND ts.best_bid_mc
                                IS NOT NULL
                            AND ts.best_ask_mc
                                IS NOT NULL
                            AND ts.best_bid_size
                                IS NOT NULL
                            AND ts.best_ask_size
                                IS NOT NULL
                    ) AS tokens_with_sized_tob,

                    (
                        SELECT min(d.capture_time)
                        FROM book_deltas d
                        JOIN ht
                          ON ht.token_id =
                             d.token_id
                        CROSS JOIN w
                        WHERE
                            d.capture_time >= w.t0
                            AND d.capture_time
                                <= w.t1
                                   + interval '5 seconds'
                            AND d.watchlist_rule =
                                'h2_v2'
                    ) AS first_delta,

                    (
                        SELECT max(d.capture_time)
                        FROM book_deltas d
                        JOIN ht
                          ON ht.token_id =
                             d.token_id
                        CROSS JOIN w
                        WHERE
                            d.capture_time >= w.t0
                            AND d.capture_time
                                <= w.t1
                                   + interval '5 seconds'
                            AND d.watchlist_rule =
                                'h2_v2'
                    ) AS last_delta
                """,
                (run_id,),
            )

        game_rate = rate(
            sharp_games[
                "eligible_game_snapshots"
            ],
            sharp_games[
                "game_snapshots"
            ],
        )

        outcome_rate = rate(
            sharp_outcomes[
                "eligible_outcomes"
            ],
            sharp_outcomes[
                "outcome_snapshots"
            ],
        )

        sharp_integrity_pass = all([
            sharp_run["polls"] == 60,
            sharp_games[
                "asymmetric_or_bad_games"
            ] == 0,
            quote_integrity[
                "inplay_rows"
            ] == 0,
            quote_integrity[
                "incomplete_pair_rows"
            ] == 0,
            quote_integrity[
                "bad_freshness_rows"
            ] == 0,
            quote_integrity[
                "bad_source_age_rows"
            ] == 0,
            quote_integrity[
                "bad_midpoint_rows"
            ] == 0,
            consensus_integrity[
                "bad_eligibility_rows"
            ] == 0,
            consensus_integrity[
                "bad_family_count_rows"
            ] == 0,
        ])

        sharp_coverage_pass = (
            game_rate >= SHARP_MIN_RATE
        )

        sharp_pass = (
            sharp_integrity_pass
            and sharp_coverage_pass
        )

        pm_pass = all([
            mapping["matches"] == 17,
            mapping["markets"] == 17,
            tokens["tokens"] == 34,
            tokens["distinct_tokens"] == 34,
            pm_config["markets"] == 17,
            pm_config["with_min_order"] == 17,
            pm_config["with_tick"] == 17,
            pm_config["with_fee_curve"] == 17,
            pm_config["with_two_tokens"] == 17,
            pm_window["tokens_with_delta"] == 34,
            pm_window[
                "tokens_with_full_book"
            ] == 34,
            pm_window[
                "tokens_with_sized_tob"
            ] == 34,
        ])

        overall_pass = (
            sharp_pass
            and pm_pass
        )

        result = {
            "version":
                "H2-v2-M1-VALIDATION-1",

            "contract_sha256":
                EXPECTED_CONTRACT_SHA,

            "sharp_run_id":
                run_id,

            "sharp_run":
                sharp_run,

            "sharp_outcomes":
                sharp_outcomes,

            "sharp_game_snapshots":
                sharp_games,

            "sharp_game_eligibility_rate":
                format(
                    game_rate,
                    ".12f",
                ),

            "sharp_outcome_eligibility_rate":
                format(
                    outcome_rate,
                    ".12f",
                ),

            "sharp_required_rate":
                format(
                    SHARP_MIN_RATE,
                    ".12f",
                ),

            "sharp_integrity":
                quote_integrity,

            "consensus_integrity":
                consensus_integrity,

            "bookmaker_coverage":
                books,

            "per_game_coverage":
                per_game,

            "polymarket_mapping":
                mapping,

            "polymarket_tokens":
                tokens,

            "polymarket_config":
                pm_config,

            "polymarket_window":
                pm_window,

            "decisions": {
                "sharp_integrity_pass":
                    sharp_integrity_pass,

                "sharp_coverage_pass":
                    sharp_coverage_pass,

                "sharp_data_plane_pass":
                    sharp_pass,

                "polymarket_data_plane_pass":
                    pm_pass,

                "m1_overall_pass":
                    overall_pass,
            },

            "interpretation": (
                "M1 is an engineering/data-"
                "feasibility validation only. "
                "No signal, threshold, trade, "
                "or PnL was evaluated."
            ),
        }

        OUT.write_text(
            json.dumps(
                clean(result),
                indent=2,
                sort_keys=True,
            ) + "\n",
            encoding="utf-8",
        )

        print(
            "H2-v2 M1 VALIDATION COMPLETE"
        )
        print(
            "sharp game snapshots:",
            sharp_games["game_snapshots"],
        )
        print(
            "sharp eligible:",
            sharp_games[
                "eligible_game_snapshots"
            ],
        )
        print(
            "sharp eligibility rate:",
            format(game_rate, ".6%"),
        )
        print(
            "required:",
            format(
                SHARP_MIN_RATE,
                ".2%",
            ),
        )
        print(
            "sharp integrity:",
            sharp_integrity_pass,
        )
        print(
            "sharp coverage:",
            sharp_coverage_pass,
        )
        print(
            "PM data plane:",
            pm_pass,
        )
        print(
            "M1 overall:",
            overall_pass,
        )
        print(
            "artifact:",
            OUT.relative_to(REPO),
        )

    finally:
        conn.close()


if __name__ == "__main__":
    main()
