from __future__ import annotations

import hashlib
import importlib.util
import json
import subprocess
from collections import Counter
from datetime import timedelta
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
R = REPO / "research"

CONTRACT = R / "h2_v2_m2h_contract.json"
M2_BUILDER = R / "h2_v2_m2_build.py"

TAPE = R / "h2_v2_m2h_tape.jsonl"
VALIDATION = R / "h2_v2_m2h_validation.json"

EXPECTED_CONTRACT_SHA = (
    "ea0ce05b254a7da951303c1538609926"
    "716dfb7200cf23b50ae67570d2aefe43"
)

EXPECTED_PARENT = (
    "4078edf8a52703c64409a353574896d7bf0a698e"
)

EXPECTED_M2_BUILDER_SHA = (
    "324ccba8cc74d0853de640d5758c3cc7"
    "7c7fbba30cad81022adac138e5022ab8"
)

RUN_ID = 11691
EXPECTED_ANCHORS = 1080
EXPECTED_GAMES = 9
EXPECTED_ENDPOINTS = 2160


def sha256_file(path: Path) -> str:
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def load_m2_module():
    actual = sha256_file(
        M2_BUILDER
    )

    if actual != EXPECTED_M2_BUILDER_SHA:
        raise SystemExit(
            "REFUSING M2h: frozen M2 builder "
            f"hash changed: {actual}"
        )

    spec = importlib.util.spec_from_file_location(
        "frozen_h2_v2_m2_build",
        M2_BUILDER,
    )

    if (
        spec is None
        or spec.loader is None
    ):
        raise RuntimeError(
            "could not load frozen M2 builder"
        )

    module = importlib.util.module_from_spec(
        spec
    )

    spec.loader.exec_module(
        module
    )

    return module


m2 = load_m2_module()


def snapshot_audit(
    cur,
    token_id,
    target_time,
    state,
):
    out = {
        "authoritative_base_present": False,
        "latest_full_snapshot_rule": False,
        "intervening_full_snapshot_violation": False,
        "strictly_later_full_snapshots": 0,
    }

    if token_id is None:
        return out

    cur.execute(
        """
        SELECT
            capture_time,
            source,
            connection_id,
            ingest_sequence,
            book_generation

        FROM book_snapshots

        WHERE
            token_id = %s
            AND capture_time <= %s
            AND source IN (
                'ws_book',
                'rest_book'
            )
            AND watchlist_rule = 'h2_v2'
            AND book_generation IS NOT NULL

        ORDER BY
            capture_time DESC,
            ingest_sequence DESC NULLS LAST

        LIMIT 1
        """,
        (
            token_id,
            target_time,
        ),
    )

    latest = cur.fetchone()

    if not latest:
        return out

    latest = dict(
        latest
    )

    out[
        "authoritative_base_present"
    ] = True

    latest_conn = (
        str(
            latest[
                "connection_id"
            ]
        )
        if latest[
            "connection_id"
        ] is not None
        else None
    )

    out[
        "latest_full_snapshot_rule"
    ] = (
        state["base_time"]
        == latest["capture_time"]
        and
        state["base_source"]
        == latest["source"]
        and
        state["base_generation"]
        == latest["book_generation"]
        and
        state["base_connection_id"]
        == latest_conn
    )

    if state["base_time"] is None:
        return out

    # Independent check: there must not be any
    # strictly later full snapshot before T.
    #
    # Same-capture-time ordering is already checked
    # above by comparing the replay base against the
    # independently selected latest row using the
    # same frozen ordering rule.
    cur.execute(
        """
        SELECT count(*)

        FROM book_snapshots

        WHERE
            token_id = %s
            AND capture_time > %s
            AND capture_time <= %s
            AND source IN (
                'ws_book',
                'rest_book'
            )
            AND watchlist_rule = 'h2_v2'
            AND book_generation IS NOT NULL
        """,
        (
            token_id,
            state["base_time"],
            target_time,
        ),
    )

    count = int(
        cur.fetchone()[
            "count"
        ]
    )

    out[
        "strictly_later_full_snapshots"
    ] = count

    out[
        "intervening_full_snapshot_violation"
    ] = (
        count != 0
    )

    return out


def audit_metrics(
    audits,
):
    return {
        "endpoints":
            len(audits),

        "authoritative_base_present":
            sum(
                x[
                    "authoritative_base_present"
                ]
                for x in audits
            ),

        "latest_full_snapshot_rule":
            sum(
                x[
                    "latest_full_snapshot_rule"
                ]
                for x in audits
            ),

        "intervening_full_snapshot_violations":
            sum(
                x[
                    "intervening_full_snapshot_violation"
                ]
                for x in audits
            ),

        "strictly_later_full_snapshots":
            sum(
                x[
                    "strictly_later_full_snapshots"
                ]
                for x in audits
            ),
    }


def serialize_rows(
    rows,
):
    return "".join(
        json.dumps(
            row,
            sort_keys=True,
            separators=(
                ",",
                ":",
            ),
        )
        + "\n"
        for row in rows
    )


def build_dataset(
    conn,
    label,
):
    rows = []

    decision_states = []
    execution_states = []

    decision_audits = []
    execution_audits = []

    market_cache = {}
    token_cache = {}

    market_unique_count = 0
    token_unique_count = 0
    config_covered = 0

    with conn.cursor() as cur:
        anchors = m2.anchor_rows(
            cur
        )

        for i, anchor in enumerate(
            anchors,
            start=1,
        ):
            identity = m2.market_identity(
                cur,
                anchor[
                    "odds_game_id"
                ],
                anchor[
                    "outcome_team"
                ],
                market_cache,
                token_cache,
            )

            if identity[
                "market_unique"
            ]:
                market_unique_count += 1

            if identity[
                "token_unique"
            ]:
                token_unique_count += 1

            cfg = m2.config_at(
                cur,
                identity[
                    "polymarket_market_id"
                ],
                anchor[
                    "decision_time"
                ],
            )

            config_ok = bool(
                cfg
                and cfg[
                    "minimum_order_size"
                ] is not None
                and cfg[
                    "minimum_tick_size"
                ] is not None
                and cfg[
                    "fee_rate"
                ] is not None
                and cfg[
                    "fee_exponent"
                ] is not None
                and cfg[
                    "taker_only"
                ] is not None
            )

            if config_ok:
                config_covered += 1

            decision_time = (
                anchor[
                    "decision_time"
                ]
            )

            execution_time = (
                decision_time
                + timedelta(
                    milliseconds=500
                )
            )

            decision = m2.reconstruct(
                cur,
                identity["token_id"],
                decision_time,
            )

            execution = m2.reconstruct(
                cur,
                identity["token_id"],
                execution_time,
            )

            decision_audit = (
                snapshot_audit(
                    cur,
                    identity[
                        "token_id"
                    ],
                    decision_time,
                    decision,
                )
            )

            execution_audit = (
                snapshot_audit(
                    cur,
                    identity[
                        "token_id"
                    ],
                    execution_time,
                    execution,
                )
            )

            decision_states.append(
                decision
            )

            execution_states.append(
                execution
            )

            decision_audits.append(
                decision_audit
            )

            execution_audits.append(
                execution_audit
            )

            row = {
                "sharp_consensus_id":
                    anchor[
                        "sharp_consensus_id"
                    ],

                "poll_id":
                    anchor[
                        "poll_id"
                    ],

                "odds_game_id":
                    anchor[
                        "odds_game_id"
                    ],

                "outcome_team":
                    anchor[
                        "outcome_team"
                    ],

                "decision_time":
                    decision_time,

                "consensus_prob":
                    anchor[
                        "consensus_prob"
                    ],

                "families_used":
                    anchor[
                        "families_used"
                    ],

                "polymarket_market_id":
                    identity[
                        "polymarket_market_id"
                    ],

                "token_id":
                    identity[
                        "token_id"
                    ],

                "pm_outcome":
                    identity[
                        "pm_outcome"
                    ],

                "decision_best_bid_mc":
                    decision[
                        "best_bid_mc"
                    ],

                "decision_best_bid_size":
                    decision[
                        "best_bid_size"
                    ],

                "decision_best_ask_mc":
                    decision[
                        "best_ask_mc"
                    ],

                "decision_best_ask_size":
                    decision[
                        "best_ask_size"
                    ],

                "execution_time":
                    execution_time,

                "execution_best_bid_mc":
                    execution[
                        "best_bid_mc"
                    ],

                "execution_best_bid_size":
                    execution[
                        "best_bid_size"
                    ],

                "execution_best_ask_mc":
                    execution[
                        "best_ask_mc"
                    ],

                "execution_best_ask_size":
                    execution[
                        "best_ask_size"
                    ],

                "decision_base_source":
                    decision[
                        "base_source"
                    ],

                "decision_base_time":
                    decision[
                        "base_time"
                    ],

                "decision_base_generation":
                    decision[
                        "base_generation"
                    ],

                "decision_replayed_deltas":
                    decision[
                        "replayed_deltas"
                    ],

                "decision_authoritative_base_present":
                    decision_audit[
                        "authoritative_base_present"
                    ],

                "decision_latest_full_snapshot_rule":
                    decision_audit[
                        "latest_full_snapshot_rule"
                    ],

                "decision_intervening_full_snapshot_violation":
                    decision_audit[
                        "intervening_full_snapshot_violation"
                    ],

                "execution_base_source":
                    execution[
                        "base_source"
                    ],

                "execution_base_time":
                    execution[
                        "base_time"
                    ],

                "execution_base_generation":
                    execution[
                        "base_generation"
                    ],

                "execution_replayed_deltas":
                    execution[
                        "replayed_deltas"
                    ],

                "execution_authoritative_base_present":
                    execution_audit[
                        "authoritative_base_present"
                    ],

                "execution_latest_full_snapshot_rule":
                    execution_audit[
                        "latest_full_snapshot_rule"
                    ],

                "execution_intervening_full_snapshot_violation":
                    execution_audit[
                        "intervening_full_snapshot_violation"
                    ],

                "config_observed_at":
                    (
                        cfg[
                            "observed_at"
                        ]
                        if cfg
                        else None
                    ),

                "minimum_order_size":
                    (
                        cfg[
                            "minimum_order_size"
                        ]
                        if cfg
                        else None
                    ),

                "minimum_tick_size":
                    (
                        cfg[
                            "minimum_tick_size"
                        ]
                        if cfg
                        else None
                    ),

                "fee_rate":
                    (
                        cfg[
                            "fee_rate"
                        ]
                        if cfg
                        else None
                    ),

                "fee_exponent":
                    (
                        cfg[
                            "fee_exponent"
                        ]
                        if cfg
                        else None
                    ),

                "taker_only":
                    (
                        cfg[
                            "taker_only"
                        ]
                        if cfg
                        else None
                    ),

                # Independent BBO price reference only.
                "decision_reference_tob_time":
                    decision[
                        "reference_time"
                    ],

                "decision_reference_tob_source":
                    decision[
                        "reference_source"
                    ],

                "execution_reference_tob_time":
                    execution[
                        "reference_time"
                    ],

                "execution_reference_tob_source":
                    execution[
                        "reference_source"
                    ],
            }

            rows.append(
                m2.clean(
                    row
                )
            )

            if (
                i % 100 == 0
                or
                i == len(anchors)
            ):
                print(
                    f"{label}: "
                    f"{i}/{len(anchors)} "
                    "anchors"
                )

    return {
        "rows":
            rows,

        "distinct_games":
            len({
                x[
                    "odds_game_id"
                ]
                for x in rows
            }),

        "market_unique_count":
            market_unique_count,

        "token_unique_count":
            token_unique_count,

        "config_covered":
            config_covered,

        "decision_metrics":
            m2.endpoint_metrics(
                decision_states
            ),

        "execution_metrics":
            m2.endpoint_metrics(
                execution_states
            ),

        "decision_audit":
            audit_metrics(
                decision_audits
            ),

        "execution_audit":
            audit_metrics(
                execution_audits
            ),
    }


def comparable_summary(
    run,
):
    return m2.clean({
        "distinct_games":
            run[
                "distinct_games"
            ],

        "market_unique_count":
            run[
                "market_unique_count"
            ],

        "token_unique_count":
            run[
                "token_unique_count"
            ],

        "config_covered":
            run[
                "config_covered"
            ],

        "decision_metrics":
            run[
                "decision_metrics"
            ],

        "execution_metrics":
            run[
                "execution_metrics"
            ],

        "decision_audit":
            run[
                "decision_audit"
            ],

        "execution_audit":
            run[
                "execution_audit"
            ],
    })


def main():
    contract_sha = sha256_file(
        CONTRACT
    )

    if (
        contract_sha
        != EXPECTED_CONTRACT_SHA
    ):
        raise SystemExit(
            "REFUSING M2h: contract "
            f"hash changed: {contract_sha}"
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
            "REFUSING M2h: parent HEAD "
            f"{head} != {EXPECTED_PARENT}"
        )

    contract = json.loads(
        CONTRACT.read_text(
            encoding="utf-8"
        )
    )

    assert (
        contract["version"]
        == "H2-v2-M2h-CONTRACT-1"
    )

    assert (
        contract[
            "parent_m2g_commit"
        ]
        == EXPECTED_PARENT
    )

    assert (
        contract[
            "scope"
        ][
            "engineering_only"
        ]
        is True
    )

    assert (
        contract[
            "scope"
        ][
            "performance_analysis"
        ]
        is False
    )

    assert (
        contract[
            "frozen_universe"
        ][
            "expected_outcome_anchors"
        ]
        == EXPECTED_ANCHORS
    )

    assert (
        contract[
            "frozen_universe"
        ][
            "expected_total_endpoints"
        ]
        == EXPECTED_ENDPOINTS
    )

    assert (
        contract[
            "replay_rule"
        ][
            "generation_crossing_allowed"
        ]
        is False
    )

    assert (
        contract[
            "replay_rule"
        ][
            "lookahead_allowed"
        ]
        is False
    )

    assert (
        contract[
            "replay_rule"
        ][
            "trade_inference_allowed"
        ]
        is False
    )

    assert (
        contract[
            "validation"
        ][
            "size_validation"
        ][
            "stale_tob_size_as_ground_truth"
        ]
        is False
    )

    assert (
        contract[
            "validation"
        ][
            "size_validation"
        ][
            "next_ws_book_reproduction_as_ground_truth"
        ]
        is False
    )

    conn = m2.connect()

    # Both independent rebuilds operate against
    # one frozen Postgres snapshot.
    conn.set_session(
        isolation_level="REPEATABLE READ",
        readonly=True,
        autocommit=False,
    )

    try:
        first = build_dataset(
            conn,
            "build-1",
        )

        second = build_dataset(
            conn,
            "build-2",
        )

        conn.rollback()

    finally:
        conn.close()

    first_text = serialize_rows(
        first["rows"]
    )

    second_text = serialize_rows(
        second["rows"]
    )

    deterministic_rebuild = (
        first_text
        == second_text
        and
        comparable_summary(first)
        == comparable_summary(second)
    )

    TAPE.write_text(
        first_text,
        encoding="utf-8",
    )

    tape_sha = sha256_file(
        TAPE
    )

    builder_sha = sha256_file(
        Path(__file__)
    )

    dm = first[
        "decision_metrics"
    ]

    em = first[
        "execution_metrics"
    ]

    da = first[
        "decision_audit"
    ]

    ea = first[
        "execution_audit"
    ]

    anchor_count = len(
        first["rows"]
    )

    endpoint_count = (
        dm["anchors"]
        + em["anchors"]
    )

    overall = (
        anchor_count
            == EXPECTED_ANCHORS

        and endpoint_count
            == EXPECTED_ENDPOINTS

        and first[
            "distinct_games"
        ] == EXPECTED_GAMES

        and first[
            "market_unique_count"
        ] == EXPECTED_ANCHORS

        and first[
            "token_unique_count"
        ] == EXPECTED_ANCHORS

        and first[
            "config_covered"
        ] == EXPECTED_ANCHORS

        and dm[
            "reconstructed"
        ] == EXPECTED_ANCHORS

        and em[
            "reconstructed"
        ] == EXPECTED_ANCHORS

        and da[
            "authoritative_base_present"
        ] == EXPECTED_ANCHORS

        and ea[
            "authoritative_base_present"
        ] == EXPECTED_ANCHORS

        and da[
            "latest_full_snapshot_rule"
        ] == EXPECTED_ANCHORS

        and ea[
            "latest_full_snapshot_rule"
        ] == EXPECTED_ANCHORS

        and da[
            "intervening_full_snapshot_violations"
        ] == 0

        and ea[
            "intervening_full_snapshot_violations"
        ] == 0

        and dm[
            "generation_mixing"
        ] == 0

        and em[
            "generation_mixing"
        ] == 0

        and dm[
            "connection_mixing"
        ] == 0

        and em[
            "connection_mixing"
        ] == 0

        and dm[
            "lookahead"
        ] == 0

        and em[
            "lookahead"
        ] == 0

        and dm[
            "crossed"
        ] == 0

        and em[
            "crossed"
        ] == 0

        and dm[
            "reference_covered"
        ] == EXPECTED_ANCHORS

        and em[
            "reference_covered"
        ] == EXPECTED_ANCHORS

        and dm[
            "reference_price_matches"
        ] == EXPECTED_ANCHORS

        and em[
            "reference_price_matches"
        ] == EXPECTED_ANCHORS

        and deterministic_rebuild
    )

    validation = {
        "version":
            "H2-v2-M2h-VALIDATION-1",

        "contract_sha256":
            contract_sha,

        "builder_sha256":
            builder_sha,

        "frozen_m2_builder_sha256":
            EXPECTED_M2_BUILDER_SHA,

        "parent_m2g_commit":
            EXPECTED_PARENT,

        "tape_sha256":
            tape_sha,

        "engineering_dataset": {
            "sharp_collector_run_id":
                RUN_ID,

            "tape_rows":
                anchor_count,

            "endpoints":
                endpoint_count,

            "distinct_games":
                first[
                    "distinct_games"
                ],

            "unique_market_mapping":
                first[
                    "market_unique_count"
                ],

            "unique_outcome_token_mapping":
                first[
                    "token_unique_count"
                ],

            "configuration_covered":
                first[
                    "config_covered"
                ],
        },

        "decision_endpoint":
            dm,

        "execution_500ms_endpoint":
            em,

        "decision_snapshot_audit":
            da,

        "execution_500ms_snapshot_audit":
            ea,

        "determinism": {
            "independent_rebuilds":
                2,

            "same_repeatable_read_snapshot":
                True,

            "tape_identical":
                (
                    first_text
                    == second_text
                ),

            "summary_identical":
                (
                    comparable_summary(
                        first
                    )
                    ==
                    comparable_summary(
                        second
                    )
                ),

            "deterministic_rebuild":
                deterministic_rebuild,
        },

        # Retained only to show continuity with M2.
        # These values are explicitly NOT acceptance
        # criteria under the frozen M2h contract.
        "nonbinding_legacy_size_diagnostic": {
            "decision_reference_size_comparisons":
                dm[
                    "reference_size_comparisons"
                ],

            "decision_reference_size_mismatch_anchors":
                dm[
                    "reference_size_mismatch_anchors"
                ],

            "execution_reference_size_comparisons":
                em[
                    "reference_size_comparisons"
                ],

            "execution_reference_size_mismatch_anchors":
                em[
                    "reference_size_mismatch_anchors"
                ],

            "used_for_m2h_acceptance":
                False,
        },

        "decisions": {
            "m2h_overall_pass":
                overall,
        },

        "interpretation": (
            "M2h validates the authoritative "
            "snapshot-reset synchronized replay "
            "model only. Latest full snapshots "
            "are authoritative; subsequent "
            "price_change events are absolute "
            "level updates. Stale TOB sizes and "
            "reproduction of later ws_book "
            "snapshots are not ground-truth "
            "acceptance tests. No edge, signal, "
            "threshold, resolution, fill, trade, "
            "calibration, OOS, or PnL result is "
            "computed."
        ),

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

    VALIDATION.write_text(
        json.dumps(
            m2.clean(
                validation
            ),
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    print()
    print(
        "=== H2-v2 M2h VALIDATION RESULT ==="
    )

    print(
        "tape rows:",
        anchor_count,
    )

    print(
        "endpoints:",
        endpoint_count,
    )

    print(
        "games:",
        first[
            "distinct_games"
        ],
    )

    print()

    print(
        "market mapping:",
        f"{first['market_unique_count']}/"
        f"{EXPECTED_ANCHORS}",
    )

    print(
        "token mapping:",
        f"{first['token_unique_count']}/"
        f"{EXPECTED_ANCHORS}",
    )

    print(
        "config:",
        f"{first['config_covered']}/"
        f"{EXPECTED_ANCHORS}",
    )

    print()
    print(
        "DECISION"
    )

    print(
        "reconstructed:",
        f"{dm['reconstructed']}/"
        f"{EXPECTED_ANCHORS}",
    )

    print(
        "authoritative base:",
        f"{da['authoritative_base_present']}/"
        f"{EXPECTED_ANCHORS}",
    )

    print(
        "latest snapshot rule:",
        f"{da['latest_full_snapshot_rule']}/"
        f"{EXPECTED_ANCHORS}",
    )

    print(
        "intervening snapshot violations:",
        da[
            "intervening_full_snapshot_violations"
        ],
    )

    print(
        "price agreement:",
        f"{dm['reference_price_matches']}/"
        f"{EXPECTED_ANCHORS}",
    )

    print(
        "generation mixing:",
        dm[
            "generation_mixing"
        ],
    )

    print(
        "connection mixing:",
        dm[
            "connection_mixing"
        ],
    )

    print(
        "lookahead:",
        dm[
            "lookahead"
        ],
    )

    print(
        "crossed:",
        dm[
            "crossed"
        ],
    )

    print()
    print(
        "EXECUTION +500ms"
    )

    print(
        "reconstructed:",
        f"{em['reconstructed']}/"
        f"{EXPECTED_ANCHORS}",
    )

    print(
        "authoritative base:",
        f"{ea['authoritative_base_present']}/"
        f"{EXPECTED_ANCHORS}",
    )

    print(
        "latest snapshot rule:",
        f"{ea['latest_full_snapshot_rule']}/"
        f"{EXPECTED_ANCHORS}",
    )

    print(
        "intervening snapshot violations:",
        ea[
            "intervening_full_snapshot_violations"
        ],
    )

    print(
        "price agreement:",
        f"{em['reference_price_matches']}/"
        f"{EXPECTED_ANCHORS}",
    )

    print(
        "generation mixing:",
        em[
            "generation_mixing"
        ],
    )

    print(
        "connection mixing:",
        em[
            "connection_mixing"
        ],
    )

    print(
        "lookahead:",
        em[
            "lookahead"
        ],
    )

    print(
        "crossed:",
        em[
            "crossed"
        ],
    )

    print()
    print(
        "DETERMINISTIC REBUILD:",
        deterministic_rebuild,
    )

    print()
    print(
        "NONBINDING LEGACY SIZE DIAGNOSTIC"
    )

    print(
        "decision size mismatch anchors:",
        dm[
            "reference_size_mismatch_anchors"
        ],
    )

    print(
        "execution size mismatch anchors:",
        em[
            "reference_size_mismatch_anchors"
        ],
    )

    print(
        "used for acceptance: False"
    )

    print()
    print(
        "M2h overall:",
        overall,
    )

    print(
        "builder_sha256:",
        builder_sha,
    )

    print(
        "tape_sha256:",
        tape_sha,
    )

    print(
        "validation_sha256:",
        sha256_file(
            VALIDATION
        ),
    )

    print()
    print(
        "NO EDGE / NO SIGNAL / "
        "NO TRADES / NO PNL"
    )


if __name__ == "__main__":
    main()
