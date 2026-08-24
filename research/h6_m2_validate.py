from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO = Path(__file__).resolve().parents[1]

if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from research import h6_m1_validate as core  # noqa: E402


RESEARCH = REPO / "research"

READOUT_CONTRACT = (
    RESEARCH
    / "h6_m2_readout_contract.json"
)

CODE_FREEZE_RECEIPT = (
    RESEARCH
    / "h6_m2_readout_code_freeze.json"
)

REGISTRY = (
    REPO
    / "data"
    / "research"
    / "h6"
    / "m2"
    / "registry.jsonl"
)

CAPTURE_ID = "h6m2_dev_20260822T223600Z"

EXPECTED_READOUT_CONTRACT_SHA = (
    "afca061d959bac4c4cc30c6705213514"
    "47fc1c8508f73f4285fd75e3614ebc71"
)

EXPECTED_M1_REPLAY_CORE_SHA = (
    "24d5731555dd3eab1adf5fb4fbc19f39"
    "c9b4692d0a06bf35ca87a62089549c1c"
)

MARKET_COUNT = 30
WINDOW_SECONDS = 1800

MIN_TOTAL_TRANSITIONS = 5000
MIN_TRANSITION_MARKETS = 20
MIN_MARKETS_WITH_25 = 20
MIN_AGGREGATE_COVERAGE = 0.95

MAX_RECONCILIATION_SECONDS = 60

NORMAL_STOP_REASON = (
    "ALL_REGISTERED_GAMES_REACHED_START"
)


def sha256_file(path: Path) -> str:
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def git_head(
    repo: Path = REPO,
) -> str:
    return subprocess.check_output(
        [
            "git",
            "rev-parse",
            "HEAD",
        ],
        cwd=repo,
        text=True,
    ).strip()


def committed_file_matches_head(
    path: Path,
    repo: Path = REPO,
) -> bool:
    try:
        rel = (
            path.resolve()
            .relative_to(repo.resolve())
            .as_posix()
        )

        committed = subprocess.check_output(
            [
                "git",
                "show",
                f"HEAD:{rel}",
            ],
            cwd=repo,
        )

    except (
        ValueError,
        subprocess.CalledProcessError,
    ):
        return False

    return (
        hashlib.sha256(
            committed
        ).hexdigest()
        ==
        sha256_file(path)
    )


def read_json(
    path: Path,
) -> dict[str, Any]:
    value = json.loads(
        path.read_text(
            encoding="utf-8"
        )
    )

    if not isinstance(
        value,
        dict,
    ):
        raise RuntimeError(
            f"expected JSON object: {path}"
        )

    return value


def all_false(
    value: dict[str, Any],
) -> bool:
    return bool(value) and all(
        item is False
        for item in value.values()
    )


def validate_static_inputs(
    repo: Path = REPO,
) -> tuple[
    dict[str, Any],
    list[dict[str, Any]],
    dict[str, Any],
]:
    contract_path = (
        repo
        / "research"
        / "h6_m2_readout_contract.json"
    )

    if (
        sha256_file(contract_path)
        != EXPECTED_READOUT_CONTRACT_SHA
    ):
        raise RuntimeError(
            "H6-M2 readout contract "
            "SHA mismatch"
        )

    contract = read_json(
        contract_path
    )

    if (
        contract.get("study") != "H6"
        or
        contract.get("milestone")
        != "H6-M2-READOUT"
        or
        contract.get("status")
        != "READOUT_CONTRACT_FREEZE"
    ):
        raise RuntimeError(
            "H6-M2 readout contract "
            "identity mismatch"
        )

    core_path = (
        repo
        / "research"
        / "h6_m1_validate.py"
    )

    if (
        sha256_file(core_path)
        != EXPECTED_M1_REPLAY_CORE_SHA
    ):
        raise RuntimeError(
            "pinned H6-M1 replay core "
            "SHA mismatch"
        )

    frozen = contract[
        "frozen_scientific_inputs"
    ]

    scientific_paths = {
        "h6_m0_mechanism_data_design_sha256":
            repo
            / "research"
            / "h6_m0_mechanism_data_design.json",

        "h6_m0_independence_policy_sha256":
            repo
            / "research"
            / "h6_m0_independence_policy.json",

        "h6_m2_development_contract_sha256":
            repo
            / "research"
            / "h6_m2_development_contract.json",

        "h6_m2_registrar_sha256":
            repo
            / "research"
            / "h6_m2_register.py",

        "h6_m2_registry_sha256":
            repo
            / "data"
            / "research"
            / "h6"
            / "m2"
            / "registry.jsonl",

        "h6_m2_registry_receipt_sha256":
            repo
            / "research"
            / "h6_m2_registry_freeze.json",

        "h6_m2_capture_contract_sha256":
            repo
            / "research"
            / "h6_m2_capture_contract.json",

        "h6_m2_scalability_amendment_sha256":
            repo
            / "research"
            / "h6_m2_capture_scalability_amendment.json",

        "h6_m2_collector_sha256":
            repo
            / "collectors"
            / "polymarket"
            / "h6_m2_collector.py",
    }

    for key, path in (
        scientific_paths.items()
    ):
        expected = frozen[key]

        if (
            not path.is_file()
            or sha256_file(path)
            != expected
        ):
            raise RuntimeError(
                "frozen scientific input "
                f"SHA mismatch: {key}"
            )

    development = read_json(
        scientific_paths[
            "h6_m2_development_contract_sha256"
        ]
    )

    if (
        development[
            "capture"
        ][
            "periodic_rest_reconciliation_max_seconds"
        ]
        != MAX_RECONCILIATION_SECONDS
    ):
        raise RuntimeError(
            "parent reconciliation maximum "
            "mismatch"
        )

    registry_path = (
        scientific_paths[
            "h6_m2_registry_sha256"
        ]
    )

    registry = core.load_jsonl(
        registry_path
    )

    if len(registry) != MARKET_COUNT:
        raise RuntimeError(
            "H6-M2 registry must contain "
            "exactly 30 markets"
        )

    markets, tokens = (
        core.market_metadata(
            registry
        )
    )

    if (
        len(markets) != MARKET_COUNT
        or len(tokens) != 60
    ):
        raise RuntimeError(
            "H6-M2 registry identity "
            "breadth mismatch"
        )

    binding = contract[
        "capture_binding"
    ]

    if (
        binding["capture_id"]
        != CAPTURE_ID
    ):
        raise RuntimeError(
            "capture ID binding mismatch"
        )

    capture_start_path = (
        repo
        / binding[
            "capture_start"
        ]["path"]
    )

    if (
        not capture_start_path.is_file()
    ):
        raise RuntimeError(
            "capture_start missing"
        )

    if (
        sha256_file(
            capture_start_path
        )
        !=
        binding[
            "capture_start"
        ]["sha256"]
    ):
        raise RuntimeError(
            "capture_start SHA mismatch"
        )

    capture_start = read_json(
        capture_start_path
    )

    if (
        capture_start.get("status")
        != "CAPTURE_ARMED"
        or
        capture_start.get("capture_id")
        != CAPTURE_ID
    ):
        raise RuntimeError(
            "capture_start identity mismatch"
        )

    if not all_false(
        capture_start.get(
            "analysis_boundary",
            {},
        )
    ):
        raise RuntimeError(
            "capture_start analysis boundary "
            "was crossed"
        )

    if (
        capture_start.get(
            "collector_sha256"
        )
        !=
        frozen[
            "h6_m2_collector_sha256"
        ]
    ):
        raise RuntimeError(
            "capture_start collector SHA "
            "mismatch"
        )

    gates = contract[
        "engineering_validation"
    ]

    if (
        gates[
            "integrity_gates"
        ]["registered_markets"]
        != MARKET_COUNT
    ):
        raise RuntimeError(
            "registered-market gate mismatch"
        )

    suff = gates[
        "data_sufficiency_gates"
    ]

    if (
        suff[
            "minimum_total_valid_primary_transitions"
        ]
        != MIN_TOTAL_TRANSITIONS
        or
        suff[
            "minimum_markets_with_valid_primary_transitions"
        ]
        != MIN_TRANSITION_MARKETS
        or
        suff[
            "minimum_markets_with_at_least_25_valid_transitions"
        ]
        != MIN_MARKETS_WITH_25
    ):
        raise RuntimeError(
            "M2 transition sufficiency "
            "gate mismatch"
        )

    return (
        contract,
        registry,
        capture_start,
    )


def validate_capture_complete(
    contract: dict[str, Any],
    repo: Path = REPO,
) -> tuple[
    dict[str, Any],
    dict[str, str],
]:
    binding = contract[
        "capture_binding"
    ]

    complete_path = (
        repo
        / binding[
            "capture_complete"
        ]["path"]
    )

    if not complete_path.is_file():
        raise RuntimeError(
            "H6-M2 capture is not complete"
        )

    complete = read_json(
        complete_path
    )

    if (
        complete.get("status")
        != "CAPTURE_COMPLETE"
        or
        complete.get("capture_id")
        != CAPTURE_ID
    ):
        raise RuntimeError(
            "capture_complete identity mismatch"
        )

    if not all_false(
        complete.get(
            "analysis_boundary",
            {},
        )
    ):
        raise RuntimeError(
            "capture_complete analysis boundary "
            "was crossed"
        )

    capture_root = (
        repo
        / binding[
            "capture_root"
        ]
    )

    stream_hashes: dict[str, str] = {}

    manifest_hashes = complete.get(
        "stream_hashes",
        {},
    )

    for stream, filename in (
        core.STREAM_FILENAMES.items()
    ):
        path = (
            capture_root
            / filename
        )

        if not path.is_file():
            raise RuntimeError(
                f"missing capture stream: {stream}"
            )

        info = manifest_hashes.get(
            stream
        )

        if not isinstance(
            info,
            dict,
        ):
            raise RuntimeError(
                "capture_complete missing "
                f"stream hash: {stream}"
            )

        actual = sha256_file(
            path
        )

        if (
            info.get("sha256")
            != actual
        ):
            raise RuntimeError(
                "capture stream SHA mismatch: "
                f"{stream}"
            )

        if (
            int(
                info.get(
                    "bytes",
                    -1,
                )
            )
            != path.stat().st_size
        ):
            raise RuntimeError(
                "capture stream byte count "
                f"mismatch: {stream}"
            )

        stream_hashes[
            stream
        ] = actual

    return (
        complete,
        stream_hashes,
    )


def validate_code_freeze_receipt(
    *,
    contract: dict[str, Any],
    complete: dict[str, Any],
    stream_hashes: dict[str, str],
    repo: Path = REPO,
) -> dict[str, Any]:
    receipt_path = (
        repo
        / contract[
            "implementation_freeze"
        ][
            "code_freeze_receipt"
        ]
    )

    if not receipt_path.is_file():
        raise RuntimeError(
            "H6-M2 readout code freeze "
            "receipt missing"
        )

    if not committed_file_matches_head(
        receipt_path,
        repo,
    ):
        raise RuntimeError(
            "code freeze receipt differs "
            "from committed HEAD"
        )

    receipt = read_json(
        receipt_path
    )

    if (
        receipt.get("status")
        != "H6_M2_READOUT_CODE_FROZEN"
        or
        receipt.get("capture_id")
        != CAPTURE_ID
        or
        receipt.get(
            "readout_contract_sha256"
        )
        != EXPECTED_READOUT_CONTRACT_SHA
    ):
        raise RuntimeError(
            "code freeze receipt identity "
            "mismatch"
        )

    binding = contract[
        "capture_binding"
    ]

    if (
        receipt.get(
            "capture_start_sha256"
        )
        != binding[
            "capture_start"
        ]["sha256"]
    ):
        raise RuntimeError(
            "code receipt capture_start "
            "SHA mismatch"
        )

    complete_path = (
        repo
        / binding[
            "capture_complete"
        ]["path"]
    )

    if (
        receipt.get(
            "capture_complete_sha256"
        )
        != sha256_file(
            complete_path
        )
    ):
        raise RuntimeError(
            "code receipt capture_complete "
            "SHA mismatch"
        )

    code = receipt.get(
        "code",
        {},
    )

    expected_code_paths = {
        "validator":
            "research/h6_m2_validate.py",

        "validator_tests":
            "research/test_h6_m2_validate.py",

        "analyzer":
            "research/h6_m2_analyze.py",

        "analyzer_tests":
            "research/test_h6_m2_analyze.py",
    }

    for key, rel in (
        expected_code_paths.items()
    ):
        info = code.get(
            key
        )

        if (
            not isinstance(info, dict)
            or info.get("path") != rel
        ):
            raise RuntimeError(
                "code freeze path mismatch: "
                f"{key}"
            )

        path = (
            repo / rel
        )

        if (
            not path.is_file()
            or info.get("sha256")
            != sha256_file(path)
        ):
            raise RuntimeError(
                "code freeze SHA mismatch: "
                f"{key}"
            )

        if not committed_file_matches_head(
            path,
            repo,
        ):
            raise RuntimeError(
                "frozen code differs from "
                f"HEAD: {key}"
            )

    receipt_streams = receipt.get(
        "streams",
        {},
    )

    for stream, expected in (
        stream_hashes.items()
    ):
        info = receipt_streams.get(
            stream
        )

        if (
            not isinstance(info, dict)
            or info.get("sha256")
            != expected
        ):
            raise RuntimeError(
                "code freeze stream binding "
                f"mismatch: {stream}"
            )

    if not all_false(
        receipt.get(
            "analysis_boundary",
            {},
        )
    ):
        raise RuntimeError(
            "code freeze analysis boundary "
            "invalid"
        )

    return receipt


def active_reconciliation_metrics(
    control_rows: list[
        dict[str, Any]
    ],
    latest_game_start: datetime,
) -> dict[str, Any]:
    parsed: list[
        tuple[
            datetime,
            dict[str, Any],
        ]
    ] = []

    errors: list[str] = []

    for row in control_rows:
        try:
            when = core.parse_dt(
                row["capture_time"]
            )
        except (
            KeyError,
            ValueError,
        ):
            errors.append(
                "invalid control timestamp"
            )
            continue

        parsed.append(
            (
                when,
                row,
            )
        )

    parsed.sort(
        key=lambda item: item[0]
    )

    connection_starts = [
        when
        for when, row
        in parsed
        if row.get("kind")
        == "connection_start"
    ]

    initialized = [
        (
            when,
            str(
                row.get(
                    "connection_id"
                )
                or ""
            ),
        )
        for when, row
        in parsed
        if row.get("kind")
        == "capture_initialized"
    ]

    seen_connections: set[str] = set()

    max_interval = 0.0
    intervals: list[float] = []
    connection_metrics: list[
        dict[str, Any]
    ] = []

    for init_time, conn in initialized:
        if not conn:
            errors.append(
                "capture_initialized missing "
                "connection_id"
            )
            continue

        if conn in seen_connections:
            errors.append(
                "duplicate capture_initialized "
                f"for connection {conn}"
            )
            continue

        seen_connections.add(
            conn
        )

        explicit_ends = [
            when
            for when, row
            in parsed
            if (
                when >= init_time
                and
                str(
                    row.get(
                        "connection_id"
                    )
                    or ""
                )
                == conn
                and
                row.get("kind")
                in {
                    "active_token_set_change",
                    "connection_error",
                }
            )
        ]

        next_starts = [
            when
            for when
            in connection_starts
            if when > init_time
        ]

        candidates = [
            latest_game_start,
            *explicit_ends,
            *next_starts,
        ]

        end_time = min(
            candidates
        )

        if end_time < init_time:
            errors.append(
                "connection end precedes "
                f"initialization: {conn}"
            )
            continue

        reconciles = [
            when
            for when, row
            in parsed
            if (
                row.get("kind")
                == "rest_reconcile_complete"
                and
                str(
                    row.get(
                        "connection_id"
                    )
                    or ""
                )
                == conn
                and
                init_time
                <= when
                <= end_time
            )
        ]

        points = [
            init_time,
            *reconciles,
            end_time,
        ]

        local_intervals = [
            (
                b - a
            ).total_seconds()
            for a, b
            in zip(
                points,
                points[1:],
            )
        ]

        if any(
            value < 0
            for value
            in local_intervals
        ):
            errors.append(
                "negative reconciliation "
                f"interval: {conn}"
            )

        intervals.extend(
            local_intervals
        )

        local_max = max(
            local_intervals,
            default=0.0,
        )

        max_interval = max(
            max_interval,
            local_max,
        )

        connection_metrics.append(
            {
                "connection_id":
                    conn,

                "initialized_at":
                    init_time.isoformat(),

                "ended_at":
                    end_time.isoformat(),

                "reconciliation_count":
                    len(reconciles),

                "max_interval_seconds":
                    round(
                        local_max,
                        6,
                    ),
            }
        )

    if not initialized:
        errors.append(
            "no capture_initialized events"
        )

    return {
        "initialized_connection_count":
            len(initialized),

        "max_interval_seconds":
            round(
                max_interval,
                6,
            ),

        "intervals_seconds": [
            round(
                value,
                6,
            )
            for value in intervals
        ],

        "connections":
            connection_metrics,

        "errors":
            errors,
    }


def verdict_from_metrics(
    *,
    contract: dict[str, Any],
    replay: dict[str, Any],
    lineage: dict[str, int],
    reconciliation: dict[str, Any],
    deterministic_replay_pass: bool,
    capture_stop_reason: str,
) -> tuple[
    str,
    dict[str, dict[str, bool]],
]:
    integrity = {
        "capture_completed_normally":
            capture_stop_reason
            == NORMAL_STOP_REASON,

        "registered_markets_exact":
            replay[
                "distinct_markets"
            ]
            == MARKET_COUNT,

        "initial_full_p1_snapshots":
            replay[
                "initial_full_p1_snapshot_count"
            ]
            == MARKET_COUNT,

        "raw_and_normalized_lineage":
            lineage[
                "raw_sha_failures"
            ]
            == 0
            and
            lineage[
                "duplicate_raw_keys"
            ]
            == 0
            and
            lineage[
                "normalized_lineage_failures"
            ]
            == 0,

        "deterministic_replay":
            deterministic_replay_pass,

        "generation_or_connection_mixing":
            replay[
                "generation_or_connection_mixing"
            ]
            == 0,

        "future_backfill":
            replay[
                "future_backfill"
            ]
            == 0,

        "in_play_primary_transitions":
            replay[
                "in_play_primary_transitions"
            ]
            == 0,

        "reconstruction_integrity":
            len(
                replay[
                    "reconstruction_integrity_errors"
                ]
            )
            == 0
            and
            replay[
                "state_errors"
            ]
            == 0
            and
            replay[
                "rest_flag_crosscheck_failures"
            ]
            == 0
            and
            replay[
                "rest_generation_crosscheck_failures"
            ]
            == 0
            and
            replay[
                "post_frame_crosscheck_failures"
            ]
            == 0,

        "active_reconciliation_interval":
            not reconciliation[
                "errors"
            ]
            and
            reconciliation[
                "max_interval_seconds"
            ]
            <= MAX_RECONCILIATION_SECONDS,
    }

    sufficiency = {
        "aggregate_l1_coverage":
            replay[
                "aggregate_coverage_fraction"
            ]
            >= MIN_AGGREGATE_COVERAGE,

        "total_primary_transitions":
            replay[
                "transition_count_total"
            ]
            >= MIN_TOTAL_TRANSITIONS,

        "transition_market_breadth":
            replay[
                "transition_distinct_markets"
            ]
            >= MIN_TRANSITION_MARKETS,

        "market_breadth_25":
            replay[
                "markets_with_at_least_25_transitions"
            ]
            >= MIN_MARKETS_WITH_25,
    }

    if not all(
        integrity.values()
    ):
        verdict = (
            "FAIL_INTEGRITY"
        )

    elif not all(
        sufficiency.values()
    ):
        verdict = (
            "INCONCLUSIVE_DEVELOPMENT_DATA"
        )

    else:
        verdict = (
            "PASS_ENGINEERING_AND_DATA_SUFFICIENCY"
        )

    return (
        verdict,
        {
            "integrity":
                integrity,

            "sufficiency":
                sufficiency,
        },
    )


def live_readout(
    repo: Path = REPO,
) -> dict[str, Any]:
    (
        contract,
        registry,
        _capture_start,
    ) = validate_static_inputs(
        repo
    )

    validator_path = (
        repo
        / "research"
        / "h6_m2_validate.py"
    )

    if not committed_file_matches_head(
        validator_path,
        repo,
    ):
        raise RuntimeError(
            "refusing real H6-M2 validation: "
            "validator differs from committed HEAD"
        )

    (
        complete,
        stream_hashes,
    ) = validate_capture_complete(
        contract,
        repo,
    )

    code_receipt = (
        validate_code_freeze_receipt(
            contract=contract,
            complete=complete,
            stream_hashes=stream_hashes,
            repo=repo,
        )
    )

    result_path = (
        repo
        / contract[
            "real_readout_policy"
        ][
            "validator_result_path"
        ]
    )

    if result_path.exists():
        raise RuntimeError(
            "refusing overwrite of existing "
            "H6-M2 engineering result"
        )

    capture_root = (
        repo
        / contract[
            "capture_binding"
        ][
            "capture_root"
        ]
    )

    raw = core.load_jsonl(
        capture_root
        / "raw_frames.jsonl"
    )

    normalized = core.load_jsonl(
        capture_root
        / "normalized.jsonl"
    )

    rest = core.load_jsonl(
        capture_root
        / "rest_snapshots.jsonl"
    )

    control = core.load_jsonl(
        capture_root
        / "control.jsonl"
    )

    lineage = core.verify_lineage(
        raw,
        normalized,
    )

    replay_a = core.Replay(
        registry,
        raw,
        rest,
        control,
    ).replay()

    replay_b = core.Replay(
        registry,
        raw,
        rest,
        control,
    ).replay()

    deterministic_pass = (
        replay_a[
            "canonical_digest"
        ]
        ==
        replay_b[
            "canonical_digest"
        ]
        and
        core.canonical_json_bytes(
            replay_a
        )
        ==
        core.canonical_json_bytes(
            replay_b
        )
    )

    latest_start = max(
        core.parse_dt(
            row[
                "game_start_time_utc"
            ]
        )
        for row in registry
    )

    reconciliation = (
        active_reconciliation_metrics(
            control,
            latest_start,
        )
    )

    (
        verdict,
        gate_results,
    ) = verdict_from_metrics(
        contract=contract,
        replay=replay_a,
        lineage=lineage,
        reconciliation=reconciliation,
        deterministic_replay_pass=(
            deterministic_pass
        ),
        capture_stop_reason=str(
            complete.get(
                "stop_reason"
            )
            or ""
        ),
    )

    result = {
        "study":
            "H6",

        "milestone":
            "H6-M2-ENGINEERING",

        "status":
            verdict,

        "capture_id":
            CAPTURE_ID,

        "readout_contract_sha256":
            EXPECTED_READOUT_CONTRACT_SHA,

        "m1_replay_core_sha256":
            EXPECTED_M1_REPLAY_CORE_SHA,

        "validator_git_head":
            git_head(
                repo
            ),

        "validator_sha256":
            sha256_file(
                validator_path
            ),

        "code_freeze_receipt_sha256":
            sha256_file(
                repo
                / contract[
                    "implementation_freeze"
                ][
                    "code_freeze_receipt"
                ]
            ),

        "capture_complete_sha256":
            sha256_file(
                repo
                / contract[
                    "capture_binding"
                ][
                    "capture_complete"
                ]["path"]
            ),

        "capture_stop_reason":
            complete.get(
                "stop_reason"
            ),

        "stream_hashes":
            stream_hashes,

        "deterministic_replay": {
            "status":
                (
                    "PASS"
                    if deterministic_pass
                    else "FAIL"
                ),

            "digest_run_1":
                replay_a[
                    "canonical_digest"
                ],

            "digest_run_2":
                replay_b[
                    "canonical_digest"
                ],
        },

        "lineage":
            lineage,

        "reconciliation": {
            "initialized_connection_count":
                reconciliation[
                    "initialized_connection_count"
                ],

            "max_interval_seconds":
                reconciliation[
                    "max_interval_seconds"
                ],

            "errors":
                reconciliation[
                    "errors"
                ],
        },

        "engineering_metrics": {
            "distinct_markets":
                replay_a[
                    "distinct_markets"
                ],

            "distinct_connections":
                replay_a[
                    "distinct_connections"
                ],

            "initial_full_p1_snapshot_count":
                replay_a[
                    "initial_full_p1_snapshot_count"
                ],

            "aggregate_coverage_fraction":
                replay_a[
                    "aggregate_coverage_fraction"
                ],

            "coverage_fraction_by_market":
                replay_a[
                    "coverage_fraction_by_market"
                ],

            "transition_count_total":
                replay_a[
                    "transition_count_total"
                ],

            "transition_count_by_market":
                replay_a[
                    "transition_count_by_market"
                ],

            "transition_distinct_markets":
                replay_a[
                    "transition_distinct_markets"
                ],

            "markets_with_at_least_25_transitions":
                replay_a[
                    "markets_with_at_least_25_transitions"
                ],

            "generation_or_connection_mixing":
                replay_a[
                    "generation_or_connection_mixing"
                ],

            "future_backfill":
                replay_a[
                    "future_backfill"
                ],

            "in_play_primary_transitions":
                replay_a[
                    "in_play_primary_transitions"
                ],

            "state_errors":
                replay_a[
                    "state_errors"
                ],

            "venue_tob_mismatch_tokens":
                replay_a[
                    "venue_tob_mismatch_tokens"
                ],

            "rest_reconciliation_mismatches":
                replay_a[
                    "rest_reconciliation_mismatches"
                ],

            "rest_flag_crosscheck_failures":
                replay_a[
                    "rest_flag_crosscheck_failures"
                ],

            "rest_generation_crosscheck_failures":
                replay_a[
                    "rest_generation_crosscheck_failures"
                ],

            "post_frame_crosscheck_failures":
                replay_a[
                    "post_frame_crosscheck_failures"
                ],

            "reconstruction_integrity_errors":
                replay_a[
                    "reconstruction_integrity_errors"
                ],

            "message_type_counts":
                replay_a[
                    "message_type_counts"
                ],
        },

        "gate_results":
            gate_results,

        "scientific_analyzer_permitted":
            verdict
            ==
            "PASS_ENGINEERING_AND_DATA_SUFFICIENCY",

        "analysis_boundary": {
            "future_midpoint_response_calculated":
                False,

            "directional_response_calculated":
                False,

            "directional_hit_rate_calculated":
                False,

            "bootstrap_calculated":
                False,

            "correlation_calculated":
                False,

            "regression_calculated":
                False,

            "lomo_calculated":
                False,

            "markout_calculated":
                False,

            "edge_calculated":
                False,

            "pnl_calculated":
                False,

            "winner_used":
                False,

            "settlement_used":
                False,

            "f19_trade_content_read":
                False,

            "f20_read":
                False,

            "h5_response_read":
                False,

            "external_odds_used":
                False,
        },
    }

    result_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with result_path.open(
        "x",
        encoding="utf-8",
    ) as fh:
        json.dump(
            result,
            fh,
            indent=2,
            sort_keys=True,
        )

        fh.write("\n")

    return result


def validate_config(
    repo: Path = REPO,
) -> None:
    (
        contract,
        registry,
        capture_start,
    ) = validate_static_inputs(
        repo
    )

    complete_path = (
        repo
        / contract[
            "capture_binding"
        ][
            "capture_complete"
        ]["path"]
    )

    receipt_path = (
        repo
        / contract[
            "implementation_freeze"
        ][
            "code_freeze_receipt"
        ]
    )

    print(
        "H6-M2 VALIDATOR CONFIG: PASS"
    )

    print(
        "readout contract SHA:",
        EXPECTED_READOUT_CONTRACT_SHA,
    )

    print(
        "pinned M1 replay core SHA:",
        EXPECTED_M1_REPLAY_CORE_SHA,
    )

    print(
        "capture id:",
        CAPTURE_ID,
    )

    print(
        "capture_start SHA:",
        contract[
            "capture_binding"
        ][
            "capture_start"
        ]["sha256"],
    )

    print(
        "capture_start collector SHA:",
        capture_start[
            "collector_sha256"
        ],
    )

    print(
        "registered markets:",
        len(registry),
    )

    print(
        "capture complete:",
        (
            "YES"
            if complete_path.is_file()
            else "NO"
        ),
    )

    print(
        "code freeze receipt:",
        (
            "YES"
            if receipt_path.is_file()
            else "NO"
        ),
    )

    print(
        "real capture streams read: NO"
    )

    print(
        "real H6 transition count calculated: NO"
    )

    print(
        "coverage calculated: NO"
    )

    print(
        "future midpoint response calculated: NO"
    )

    print(
        "bootstrap calculated: NO"
    )

    print(
        "regression calculated: NO"
    )

    print(
        "PnL calculated: NO"
    )


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--validate-config",
        action="store_true",
    )

    parser.add_argument(
        "--run-live",
        action="store_true",
    )

    args = parser.parse_args()

    if args.validate_config:
        validate_config()
        return

    if args.run_live:
        result = live_readout()

        print(
            "========================================"
        )

        print(
            "H6-M2 ENGINEERING VALIDATION"
        )

        print(
            "========================================"
        )

        print(
            "status:",
            result["status"],
        )

        print(
            "transition count:",
            result[
                "engineering_metrics"
            ][
                "transition_count_total"
            ],
        )

        print(
            "transition markets:",
            result[
                "engineering_metrics"
            ][
                "transition_distinct_markets"
            ],
        )

        print(
            "markets >=25:",
            result[
                "engineering_metrics"
            ][
                "markets_with_at_least_25_transitions"
            ],
        )

        print(
            "coverage:",
            result[
                "engineering_metrics"
            ][
                "aggregate_coverage_fraction"
            ],
        )

        print(
            "future midpoint response calculated: NO"
        )

        print(
            "PnL calculated: NO"
        )

        return

    raise SystemExit(
        "choose --validate-config "
        "or --run-live"
    )


if __name__ == "__main__":
    main()
