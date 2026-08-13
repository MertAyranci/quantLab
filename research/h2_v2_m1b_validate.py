from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import dotenv_values


REPO = Path(__file__).resolve().parents[1]

CONTRACT = (
    REPO
    / "research"
    / "h2_v2_m1b_contract.json"
)

BASELINE = (
    REPO
    / "research"
    / "h2_v2_m1b_baseline.json"
)

OUT = (
    REPO
    / "research"
    / "h2_v2_m1b_validation.json"
)

EXPECTED_CONTRACT_SHA = (
    "388bb7020403591a2cdd19343c17cb41"
    "ba48b1c44d38662a562da3bbe23614a3"
)

EXPECTED_BASELINE_SHA = (
    "9e2bd8564868557c7126eea60278f68e"
    "f8bffe1b51052c85b312f53252dadcb1"
)

MIN_RATE = Decimal("0.80")

MIN_SNAPSHOTS = 100
MIN_GAMES = 3

MAX_HOURS_TO_GAME = Decimal("23")

EXPECTED_POLLS = 60
EXPECTED_HTTP_SUCCESSES = 60


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


def q1(cur, sql, params=()):
    cur.execute(
        sql,
        params,
    )

    row = cur.fetchone()

    return (
        dict(row)
        if row
        else {}
    )


def qa(cur, sql, params=()):
    cur.execute(
        sql,
        params,
    )

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


def connect():
    env = dotenv_values(
        REPO / ".env"
    )

    return psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="quantlab",
        user="quantlab",
        password=env[
            "PG_PASSWORD"
        ],
        cursor_factory=(
            psycopg2.extras.RealDictCursor
        ),
    )


def load_frozen_inputs():

    actual_contract_sha = (
        sha256_file(
            CONTRACT
        )
    )

    actual_baseline_sha = (
        sha256_file(
            BASELINE
        )
    )

    if (
        actual_contract_sha
        != EXPECTED_CONTRACT_SHA
    ):
        raise RuntimeError(
            "M1b contract SHA mismatch"
        )

    if (
        actual_baseline_sha
        != EXPECTED_BASELINE_SHA
    ):
        raise RuntimeError(
            "M1b baseline SHA mismatch"
        )

    contract = json.loads(
        CONTRACT.read_text(
            encoding="utf-8"
        )
    )

    baseline = json.loads(
        BASELINE.read_text(
            encoding="utf-8"
        )
    )

    # Defensive verification that the
    # frozen files contain exactly the
    # preregistered design we intend.
    horizon = (
        contract[
            "remediation"
        ][
            "time_to_game"
        ]
    )

    validation = (
        contract[
            "fresh_validation"
        ]
    )

    sharp = (
        contract[
            "sharp_data_contract"
        ]
    )

    assert (
        horizon[
            "lower_bound_hours"
        ]
        == 0
    )

    assert (
        horizon[
            "lower_bound_inclusive"
        ]
        is False
    )

    assert (
        horizon[
            "upper_bound_hours"
        ]
        == 23
    )

    assert (
        horizon[
            "upper_bound_inclusive"
        ]
        is True
    )

    assert (
        sharp[
            "max_source_age_seconds"
        ]
        == 25
    )

    assert (
        sharp[
            "required_fresh_families"
        ]
        == 2
    )

    assert (
        validation[
            "minimum_eligibility_rate"
        ]
        == "0.80"
    )

    assert (
        validation[
            "polls"
        ]
        == 60
    )

    assert (
        validation[
            "poll_interval_seconds"
        ]
        == 30
    )

    assert (
        validation[
            "minimum_operational_game_snapshots"
        ]
        == 100
    )

    assert (
        validation[
            "minimum_distinct_operational_games"
        ]
        == 3
    )

    return (
        contract,
        baseline,
    )


def current_state(cur):

    state = q1(
        cur,
        """
        SELECT
            (
                SELECT max(id)
                FROM collector_runs
                WHERE component =
                      'h2_v2_sharp_collector'
            ) AS max_sharp_collector_run_id,

            (
                SELECT max(id)
                FROM h2_v2_sharp_polls
            ) AS max_sharp_poll_id,

            (
                SELECT max(received_at)
                FROM h2_v2_sharp_polls
            ) AS last_sharp_poll_received_at,

            (
                SELECT credits_remaining
                FROM h2_v2_sharp_polls
                ORDER BY id DESC
                LIMIT 1
            ) AS latest_odds_credits_remaining
        """
    )

    return state


def validate_config_only(
    cur,
    baseline,
):

    frozen = baseline[
        "pre_validation_state"
    ]

    current = current_state(
        cur
    )

    current_time = (
        current[
            "last_sharp_poll_received_at"
        ]
    )

    current_time_str = (
        current_time.isoformat()
        if current_time
        else None
    )

    checks = {
        "contract_sha_ok":
            sha256_file(CONTRACT)
            == EXPECTED_CONTRACT_SHA,

        "baseline_sha_ok":
            sha256_file(BASELINE)
            == EXPECTED_BASELINE_SHA,

        "sharp_run_unchanged":
            current[
                "max_sharp_collector_run_id"
            ]
            == frozen[
                "max_sharp_collector_run_id"
            ],

        "sharp_poll_unchanged":
            current[
                "max_sharp_poll_id"
            ]
            == frozen[
                "max_sharp_poll_id"
            ],

        "last_poll_time_unchanged":
            current_time_str
            == frozen[
                "last_sharp_poll_received_at"
            ],

        "credits_unchanged":
            current[
                "latest_odds_credits_remaining"
            ]
            == frozen[
                "latest_odds_credits_remaining"
            ],
    }

    if not all(
        checks.values()
    ):
        print(
            json.dumps(
                clean(
                    {
                        "checks":
                            checks,
                        "frozen":
                            frozen,
                        "current":
                            current,
                    }
                ),
                indent=2,
                sort_keys=True,
            )
        )

        raise SystemExit(
            "M1b pre-run config "
            "validation FAILED"
        )

    print(
        "H2-v2 M1b VALIDATOR "
        "CONFIG PASSED"
    )

    print(
        "operational horizon:",
        "0 < time_to_game <= 23h",
    )

    print(
        "freshness:",
        "<=25s",
    )

    print(
        "coverage gate:",
        ">=80%",
    )

    print(
        "minimum evidence:",
        ">=100 snapshots / >=3 games",
    )

    print(
        "baseline sharp run:",
        frozen[
            "max_sharp_collector_run_id"
        ],
    )

    print(
        "baseline poll:",
        frozen[
            "max_sharp_poll_id"
        ],
    )

    print(
        "credits:",
        frozen[
            "latest_odds_credits_remaining"
        ],
    )

    print(
        "ODDS API CALLED: NO"
    )

    print(
        "DATABASE WRITES: NO"
    )


def find_fresh_run(
    cur,
    baseline,
):

    baseline_run = (
        baseline[
            "pre_validation_state"
        ][
            "max_sharp_collector_run_id"
        ]
    )

    post_runs = qa(
        cur,
        """
        SELECT
            cr.id AS run_id,
            cr.started_at,
            count(p.id) AS polls,
            count(*) FILTER (
                WHERE p.http_status = 200
            ) AS http_successes,
            min(p.id) AS first_poll_id,
            max(p.id) AS last_poll_id,
            min(p.received_at)
                AS first_poll,
            max(p.received_at)
                AS last_poll

        FROM collector_runs cr

        LEFT JOIN h2_v2_sharp_polls p
          ON p.collector_run_id =
             cr.id

        WHERE
            cr.component =
                'h2_v2_sharp_collector'
            AND cr.id > %s

        GROUP BY
            cr.id,
            cr.started_at

        ORDER BY cr.id
        """,
        (
            baseline_run,
        ),
    )

    if len(post_runs) != 1:
        raise RuntimeError(
            "Expected exactly one "
            "post-baseline H2-v2 sharp "
            "collector run; found "
            f"{len(post_runs)}: "
            f"{clean(post_runs)}"
        )

    return (
        post_runs[0],
        baseline_run,
    )


def run_validation(
    conn,
    contract,
    baseline,
):

    with conn.cursor() as cur:

        (
            run_meta,
            baseline_run,
        ) = find_fresh_run(
            cur,
            baseline,
        )

        run_id = (
            run_meta[
                "run_id"
            ]
        )

        baseline_poll = (
            baseline[
                "pre_validation_state"
            ][
                "max_sharp_poll_id"
            ]
        )

        baseline_time = (
            datetime.fromisoformat(
                baseline[
                    "pre_validation_state"
                ][
                    "last_sharp_poll_received_at"
                ]
            )
        )

        run_summary = q1(
            cur,
            """
            SELECT
                count(*) AS polls,

                count(*) FILTER (
                    WHERE http_status = 200
                ) AS http_successes,

                min(id) AS first_poll_id,

                max(id) AS last_poll_id,

                min(received_at)
                    AS first_poll,

                max(received_at)
                    AS last_poll,

                extract(
                    epoch FROM (
                        max(received_at)
                        - min(received_at)
                    )
                ) AS span_seconds,

                sum(credits_last)
                    AS credits_consumed,

                min(credits_remaining)
                    AS final_credits_remaining

            FROM h2_v2_sharp_polls

            WHERE collector_run_id = %s
            """,
            (
                run_id,
            ),
        )

        # -------------------------------------------------
        # Operational game-poll universe
        # 0 < hours_to_game <= 23
        # -------------------------------------------------

        operational = q1(
            cur,
            """
            WITH gp AS (
                SELECT
                    c.poll_id,
                    c.odds_game_id,

                    g.commence_time,

                    max(c.capture_time)
                        AS capture_time,

                    extract(
                        epoch FROM (
                            g.commence_time
                            - max(c.capture_time)
                        )
                    ) / 3600.0
                        AS hours_to_game,

                    count(*) AS outcomes,

                    count(*) FILTER (
                        WHERE c.eligible
                    ) AS eligible_outcomes

                FROM h2_v2_sharp_consensus c

                JOIN h2_v2_sharp_polls p
                  ON p.id =
                     c.poll_id

                JOIN odds_games g
                  ON g.id =
                     c.odds_game_id

                WHERE
                    p.collector_run_id = %s

                GROUP BY
                    c.poll_id,
                    c.odds_game_id,
                    g.commence_time
            ),

            op AS (
                SELECT *
                FROM gp
                WHERE
                    hours_to_game > 0
                    AND hours_to_game <= 23
            )

            SELECT
                count(*)
                    AS game_snapshots,

                count(*) FILTER (
                    WHERE outcomes = 2
                      AND eligible_outcomes = 2
                )
                    AS eligible_game_snapshots,

                count(
                    DISTINCT odds_game_id
                )
                    AS distinct_games,

                count(
                    DISTINCT poll_id
                )
                    AS polls_with_operational_games,

                min(hours_to_game)
                    AS min_hours_to_game,

                max(hours_to_game)
                    AS max_hours_to_game,

                count(*) FILTER (
                    WHERE outcomes <> 2
                       OR eligible_outcomes
                          NOT IN (0, 2)
                )
                    AS asymmetric_or_bad_games

            FROM op
            """,
            (
                run_id,
            ),
        )

        all_game_integrity = q1(
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
                  ON p.id =
                     c.poll_id

                WHERE
                    p.collector_run_id = %s

                GROUP BY
                    c.poll_id,
                    c.odds_game_id
            )

            SELECT
                count(*) AS all_game_snapshots,

                count(*) FILTER (
                    WHERE outcomes <> 2
                       OR eligible_outcomes
                          NOT IN (0, 2)
                )
                    AS asymmetric_or_bad_games

            FROM gp
            """,
            (
                run_id,
            ),
        )

        quote_integrity = q1(
            cur,
            """
            SELECT
                count(*) AS quote_rows,

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
                    WHERE q.is_fresh
                    IS DISTINCT FROM (
                        q.pair_complete
                        AND
                        q.source_age_seconds
                            BETWEEN 0 AND 25
                    )
                ) AS bad_freshness_rows,

                count(*) FILTER (
                    WHERE
                        q.source_update_time
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
                    WHERE
                        q.pair_complete
                        AND abs(
                            q.family_midpoint
                            - (
                                (
                                    1
                                    / q.back_decimal
                                    +
                                    1
                                    / q.lay_decimal
                                )
                                / 2
                            )
                        )
                        > 0.000000000001
                ) AS bad_midpoint_rows

            FROM h2_v2_exchange_quotes q

            JOIN h2_v2_sharp_polls p
              ON p.id =
                 q.poll_id

            JOIN odds_games g
              ON g.id =
                 q.odds_game_id

            WHERE
                p.collector_run_id = %s
            """,
            (
                run_id,
            ),
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
                        AND
                        c.consensus_prob
                            IS NOT NULL
                    )
                ) AS bad_eligibility_rows,

                count(*) FILTER (
                    WHERE
                        c.fresh_family_count
                        >
                        c.paired_family_count
                ) AS bad_family_count_rows

            FROM h2_v2_sharp_consensus c

            JOIN h2_v2_sharp_polls p
              ON p.id =
                 c.poll_id

            WHERE
                p.collector_run_id = %s
            """,
            (
                run_id,
            ),
        )

        bookmaker_coverage = qa(
            cur,
            """
            WITH gb AS (
                SELECT
                    q.poll_id,
                    q.odds_game_id,
                    q.bookmaker,

                    g.commence_time,

                    max(q.capture_time)
                        AS capture_time,

                    extract(
                        epoch FROM (
                            g.commence_time
                            - max(q.capture_time)
                        )
                    ) / 3600.0
                        AS hours_to_game,

                    (
                        count(*) = 2
                        AND
                        bool_and(
                            q.pair_complete
                        )
                    ) AS paired,

                    (
                        count(*) = 2
                        AND
                        bool_and(
                            q.is_fresh
                        )
                    ) AS fresh

                FROM h2_v2_exchange_quotes q

                JOIN h2_v2_sharp_polls p
                  ON p.id =
                     q.poll_id

                JOIN odds_games g
                  ON g.id =
                     q.odds_game_id

                WHERE
                    p.collector_run_id = %s

                GROUP BY
                    q.poll_id,
                    q.odds_game_id,
                    q.bookmaker,
                    g.commence_time
            )

            SELECT
                bookmaker,

                count(*) AS snapshots,

                count(*) FILTER (
                    WHERE paired
                ) AS paired_snapshots,

                count(*) FILTER (
                    WHERE fresh
                ) AS fresh_snapshots

            FROM gb

            WHERE
                hours_to_game > 0
                AND hours_to_game <= 23

            GROUP BY bookmaker

            ORDER BY bookmaker
            """,
            (
                run_id,
            ),
        )

        per_game = qa(
            cur,
            """
            WITH gp AS (
                SELECT
                    c.poll_id,
                    c.odds_game_id,

                    g.away_team,
                    g.home_team,
                    g.commence_time,

                    max(c.capture_time)
                        AS capture_time,

                    extract(
                        epoch FROM (
                            g.commence_time
                            - max(c.capture_time)
                        )
                    ) / 3600.0
                        AS hours_to_game,

                    count(*) AS outcomes,

                    count(*) FILTER (
                        WHERE c.eligible
                    ) AS eligible_outcomes

                FROM h2_v2_sharp_consensus c

                JOIN h2_v2_sharp_polls p
                  ON p.id =
                     c.poll_id

                JOIN odds_games g
                  ON g.id =
                     c.odds_game_id

                WHERE
                    p.collector_run_id = %s

                GROUP BY
                    c.poll_id,
                    c.odds_game_id,
                    g.away_team,
                    g.home_team,
                    g.commence_time
            )

            SELECT
                odds_game_id,
                away_team,
                home_team,
                commence_time,

                count(*) AS snapshots,

                count(*) FILTER (
                    WHERE
                        outcomes = 2
                        AND
                        eligible_outcomes = 2
                ) AS eligible_snapshots,

                min(hours_to_game)
                    AS closest_observed_h,

                max(hours_to_game)
                    AS furthest_observed_h

            FROM gp

            WHERE
                hours_to_game > 0
                AND hours_to_game <= 23

            GROUP BY
                odds_game_id,
                away_team,
                home_team,
                commence_time

            ORDER BY commence_time
            """,
            (
                run_id,
            ),
        )

    eligibility_rate = rate(
        operational[
            "eligible_game_snapshots"
        ],
        operational[
            "game_snapshots"
        ],
    )

    run_freshness_pass = all([
        run_id > baseline_run,

        run_summary[
            "polls"
        ] == EXPECTED_POLLS,

        run_summary[
            "http_successes"
        ] == EXPECTED_HTTP_SUCCESSES,

        run_summary[
            "first_poll_id"
        ] is not None,

        run_summary[
            "first_poll_id"
        ] > baseline_poll,

        run_summary[
            "first_poll"
        ] is not None,

        run_summary[
            "first_poll"
        ] > baseline_time,
    ])

    evidence_pass = all([
        operational[
            "game_snapshots"
        ] >= MIN_SNAPSHOTS,

        operational[
            "distinct_games"
        ] >= MIN_GAMES,
    ])

    coverage_pass = (
        eligibility_rate
        >= MIN_RATE
    )

    integrity_pass = all([
        all_game_integrity[
            "asymmetric_or_bad_games"
        ] == 0,

        operational[
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

    overall_pass = all([
        run_freshness_pass,
        evidence_pass,
        coverage_pass,
        integrity_pass,
    ])

    result = {
        "version":
            "H2-v2-M1b-VALIDATION-1",

        "contract_sha256":
            EXPECTED_CONTRACT_SHA,

        "baseline_sha256":
            EXPECTED_BASELINE_SHA,

        "validator_sha256":
            sha256_file(
                Path(__file__)
            ),

        "fresh_run": {
            "baseline_run_id":
                baseline_run,

            "run_id":
                run_id,

            "run_meta":
                run_meta,

            "summary":
                run_summary,
        },

        "operational_universe": {
            "definition":
                "0 < time_to_game <= 23h",

            "max_hours_to_game":
                "23",

            "game_snapshots":
                operational[
                    "game_snapshots"
                ],

            "eligible_game_snapshots":
                operational[
                    "eligible_game_snapshots"
                ],

            "distinct_games":
                operational[
                    "distinct_games"
                ],

            "polls_with_operational_games":
                operational[
                    "polls_with_operational_games"
                ],

            "eligibility_rate":
                format(
                    eligibility_rate,
                    ".12f",
                ),

            "required_rate":
                format(
                    MIN_RATE,
                    ".12f",
                ),

            "minimum_snapshots":
                MIN_SNAPSHOTS,

            "minimum_games":
                MIN_GAMES,

            "min_observed_hours_to_game":
                operational[
                    "min_hours_to_game"
                ],

            "max_observed_hours_to_game":
                operational[
                    "max_hours_to_game"
                ],
        },

        "all_game_integrity":
            all_game_integrity,

        "quote_integrity":
            quote_integrity,

        "consensus_integrity":
            consensus_integrity,

        "bookmaker_coverage_operational":
            bookmaker_coverage,

        "per_game_operational":
            per_game,

        "decisions": {
            "fresh_run_pass":
                run_freshness_pass,

            "minimum_evidence_pass":
                evidence_pass,

            "sharp_integrity_pass":
                integrity_pass,

            "sharp_coverage_pass":
                coverage_pass,

            "m1b_overall_pass":
                overall_pass,
        },

        "interpretation": (
            "H2-v2 M1b is strictly a "
            "sharp-feed engineering and "
            "data-feasibility remediation "
            "validation. No signal, edge "
            "threshold, trade simulation, "
            "PnL, calibration, or OOS "
            "strategy result is evaluated."
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
        "H2-v2 M1b VALIDATION COMPLETE"
    )

    print(
        "fresh run:",
        run_id,
    )

    print(
        "polls:",
        run_summary["polls"],
    )

    print(
        "HTTP successes:",
        run_summary[
            "http_successes"
        ],
    )

    print(
        "operational snapshots:",
        operational[
            "game_snapshots"
        ],
    )

    print(
        "operational games:",
        operational[
            "distinct_games"
        ],
    )

    print(
        "eligible snapshots:",
        operational[
            "eligible_game_snapshots"
        ],
    )

    print(
        "eligibility rate:",
        format(
            eligibility_rate,
            ".6%",
        ),
    )

    print(
        "required:",
        format(
            MIN_RATE,
            ".2%",
        ),
    )

    print(
        "fresh-run provenance:",
        run_freshness_pass,
    )

    print(
        "minimum evidence:",
        evidence_pass,
    )

    print(
        "integrity:",
        integrity_pass,
    )

    print(
        "coverage:",
        coverage_pass,
    )

    print(
        "M1b overall:",
        overall_pass,
    )

    print(
        "artifact:",
        OUT.relative_to(REPO),
    )


def main():

    ap = argparse.ArgumentParser()

    ap.add_argument(
        "--validate-config",
        action="store_true",
    )

    args = ap.parse_args()

    (
        contract,
        baseline,
    ) = load_frozen_inputs()

    conn = connect()

    try:
        if args.validate_config:
            with conn.cursor() as cur:
                validate_config_only(
                    cur,
                    baseline,
                )

            return

        run_validation(
            conn,
            contract,
            baseline,
        )

    finally:
        conn.close()


if __name__ == "__main__":
    main()
