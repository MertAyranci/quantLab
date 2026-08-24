from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import subprocess
from pathlib import Path
from typing import Any


REPO = Path(__file__).resolve().parents[1]
RESEARCH = REPO / "research"

BASE_VALIDATOR = (
    RESEARCH
    / "h5_m1b_validate.py"
)

RUNTIME_MODULE = (
    RESEARCH
    / "h5_m1r2_runtime.py"
)

VALIDATION_CONTRACT = (
    RESEARCH
    / "h5_m1r2_validation_contract.json"
)

CAPTURE_RECEIPT = (
    RESEARCH
    / "h5_m1r2_capture_freeze.json"
)

COLLECTOR = (
    REPO
    / "collectors/odds/h5_m1r2_collector.py"
)

CAPTURE_ID = "h5m1r2_eng_20260824_a"

CAPTURE_DIR = (
    REPO
    / "data/research/h5_m1r2/capture"
    / CAPTURE_ID
)

RESULT = (
    REPO
    / "data/research/h5_m1r2/validation/result.json"
)


EXPECTED_BASE_VALIDATOR_SHA = "0777fce7ddeabb13bf02cccd72dff37ff9169de600f7a43e89e263865b376b36"

EXPECTED_RUNTIME_SHA = (
    "a6af862e27e36ab998a307ea60127af1"
    "f0caacb8be468330988f232fdfb1697c"
)

EXPECTED_VALIDATION_CONTRACT_SHA = (
    "4462598b64077f5fadbbe112a4eae6f5"
    "943d952b2fd7d1d1433a8f6430ecf8dc"
)

EXPECTED_CAPTURE_RECEIPT_SHA = (
    "ff70c5d8df4aa2966b3d565d7133a939"
    "410b0ab686d5cd7089fa1f2e15cf93f0"
)

EXPECTED_COLLECTOR_SHA = (
    "34c5d5b6be2d9025fce6f784dc763625"
    "4b500019533fe9702aae9327786e7166"
)

EXPECTED_CAPTURE_START_SHA = (
    "d943c5b77516eef7d2906c042cdd9bfb"
    "f54745212753c7342b30651efa52a78d"
)

EXPECTED_CAPTURE_COMPLETE_SHA = (
    "c1c0d818b5dfe6783a8cb5a63e770fe"
    "03ac99ba847841f0e875defc2a9f018fe"
)

EXPECTED_COLLECTOR_COMMIT = (
    "44d1c7889fa399d63cdfd05644ea18198fb48d03"
)

EXPECTED_CAPTURE_RECEIPT_COMMIT = (
    "425e8cf8a3e8f300ed0e80e2f361594b640c4ca9"
)

EXPECTED_VALIDATION_CONTRACT_COMMIT = (
    "95731b6d949be4a21e8f223f0779f42ee8dd0f3a"
)

MIN_MARKETS = 5
MIN_ACCEPTED = 100
MIN_FRESH_COVERAGE = 0.80
MIN_PM_COVERAGE = 0.95

EXPECTED_RUNTIME_INDEXES = [
    2, 4, 5, 6, 7, 8, 10
]


def sha256_file(path: Path) -> str:
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def verify_sha(
    path: Path,
    expected: str,
):
    if not path.is_file():
        raise RuntimeError(
            f"missing frozen input: {path}"
        )

    actual = sha256_file(path)

    if actual != expected:
        raise RuntimeError(
            f"SHA mismatch: {path}\n"
            f"expected={expected}\n"
            f"actual={actual}"
        )


def read_json(
    path: Path,
) -> dict[str, Any]:
    data = json.loads(
        path.read_text(
            encoding="utf-8"
        )
    )

    if not isinstance(data, dict):
        raise RuntimeError(
            f"expected JSON object: {path}"
        )

    return data


def read_jsonl(
    path: Path,
) -> list[dict[str, Any]]:
    rows = []

    for lineno, line in enumerate(
        path.read_text(
            encoding="utf-8"
        ).splitlines(),
        start=1,
    ):
        if not line.strip():
            continue

        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            raise RuntimeError(
                f"invalid JSONL {path}:{lineno}"
            ) from None

        if not isinstance(row, dict):
            raise RuntimeError(
                f"non-object JSONL row "
                f"{path}:{lineno}"
            )

        rows.append(row)

    return rows


def git_is_ancestor(
    ancestor: str,
    descendant: str = "HEAD",
) -> bool:
    result = subprocess.run(
        [
            "git",
            "merge-base",
            "--is-ancestor",
            ancestor,
            descendant,
        ],
        cwd=REPO,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    return result.returncode == 0


def committed_self_matches_head() -> bool:
    path = Path(__file__).resolve()

    try:
        rel = path.relative_to(
            REPO
        ).as_posix()

        committed = subprocess.check_output(
            [
                "git",
                "show",
                f"HEAD:{rel}",
            ],
            cwd=REPO,
        )
    except (
        ValueError,
        subprocess.CalledProcessError,
    ):
        return False

    return (
        hashlib.sha256(committed).hexdigest()
        ==
        sha256_file(path)
    )


def load_module(
    path: Path,
    name: str,
):
    spec = (
        importlib.util.spec_from_file_location(
            name,
            path,
        )
    )

    if (
        spec is None
        or spec.loader is None
    ):
        raise RuntimeError(
            f"cannot load module: {path}"
        )

    module = (
        importlib.util.module_from_spec(
            spec
        )
    )

    spec.loader.exec_module(
        module
    )

    return module


def all_false(
    value: Any,
) -> bool:
    return (
        isinstance(value, dict)
        and bool(value)
        and all(
            item is False
            for item in value.values()
        )
    )


def classify_verdict(
    *,
    hard_integrity_pass: bool,
    sufficiency_pass: bool,
) -> str:
    if not hard_integrity_pass:
        return "FAIL_INTEGRITY"

    if sufficiency_pass:
        return (
            "PASS_ENGINEERING_FEASIBILITY"
        )

    return "INCONCLUSIVE_ENGINEERING"


def load_static_inputs(
    *,
    require_committed_self: bool,
):
    verify_sha(
        BASE_VALIDATOR,
        EXPECTED_BASE_VALIDATOR_SHA,
    )

    verify_sha(
        RUNTIME_MODULE,
        EXPECTED_RUNTIME_SHA,
    )

    verify_sha(
        VALIDATION_CONTRACT,
        EXPECTED_VALIDATION_CONTRACT_SHA,
    )

    verify_sha(
        CAPTURE_RECEIPT,
        EXPECTED_CAPTURE_RECEIPT_SHA,
    )

    verify_sha(
        COLLECTOR,
        EXPECTED_COLLECTOR_SHA,
    )

    contract = read_json(
        VALIDATION_CONTRACT
    )

    receipt = read_json(
        CAPTURE_RECEIPT
    )

    if (
        contract.get("study") != "H5"
        or
        contract.get("milestone")
        != "H5-M1R2"
        or
        contract.get("stage")
        != "ENGINEERING_VALIDATION"
    ):
        raise RuntimeError(
            "validation contract identity mismatch"
        )

    gates = contract[
        "engineering_sufficiency_gates"
    ]

    if (
        gates[
            "minimum_distinct_acquisition_markets"
        ]
        != MIN_MARKETS
        or
        gates[
            "minimum_accepted_external_states"
        ]
        != MIN_ACCEPTED
        or
        gates[
            "fresh_external_family_coverage_minimum"
        ]
        != MIN_FRESH_COVERAGE
        or
        gates[
            "pm_book_coverage_minimum"
        ]
        != MIN_PM_COVERAGE
    ):
        raise RuntimeError(
            "validation sufficiency gates changed"
        )

    if not all_false(
        contract.get(
            "analysis_boundary"
        )
    ):
        raise RuntimeError(
            "validation analysis boundary changed"
        )

    if (
        receipt.get("study") != "H5"
        or
        receipt.get("milestone")
        != "H5-M1R2"
        or
        receipt.get("status")
        != "ENGINEERING_CAPTURE_FROZEN"
        or
        receipt.get("capture_id")
        != CAPTURE_ID
        or
        receipt.get(
            "additional_acquisition_permitted"
        )
        is not False
    ):
        raise RuntimeError(
            "capture receipt identity mismatch"
        )

    if (
        receipt.get(
            "capture_start_sha256"
        )
        != EXPECTED_CAPTURE_START_SHA
        or
        receipt.get(
            "capture_complete_sha256"
        )
        != EXPECTED_CAPTURE_COMPLETE_SHA
        or
        receipt.get(
            "collector_sha256"
        )
        != EXPECTED_COLLECTOR_SHA
    ):
        raise RuntimeError(
            "capture receipt provenance mismatch"
        )

    if not all_false(
        receipt.get(
            "analysis_boundary"
        )
    ):
        raise RuntimeError(
            "capture receipt boundary changed"
        )

    base = load_module(
        BASE_VALIDATOR,
        "h5_m1b_validate_frozen_helper",
    )

    runtime = load_module(
        RUNTIME_MODULE,
        "h5_m1r2_runtime_frozen",
    )

    bundle = (
        runtime.load_runtime_bundle()
    )

    if (
        bundle[
            "acquisition_market_count"
        ]
        != 7
        or
        bundle[
            "runtime_registration_indexes"
        ]
        != EXPECTED_RUNTIME_INDEXES
    ):
        raise RuntimeError(
            "runtime cohort mismatch"
        )

    if require_committed_self:
        for commit in (
            EXPECTED_COLLECTOR_COMMIT,
            EXPECTED_CAPTURE_RECEIPT_COMMIT,
            EXPECTED_VALIDATION_CONTRACT_COMMIT,
        ):
            if not git_is_ancestor(
                commit
            ):
                raise RuntimeError(
                    "required freeze commit "
                    f"not ancestor of HEAD: {commit}"
                )

        if not committed_self_matches_head():
            raise RuntimeError(
                "refusing authoritative R2 "
                "validation: validator differs "
                "from committed HEAD"
            )

    return {
        "contract": contract,
        "capture_receipt": receipt,
        "base": base,
        "runtime": runtime,
        "bundle": bundle,
    }


def verify_capture_integrity(
    static: dict[str, Any],
) -> dict[str, Any]:
    receipt = static[
        "capture_receipt"
    ]

    failures = []

    start_path = (
        CAPTURE_DIR
        / "capture_start.json"
    )

    complete_path = (
        CAPTURE_DIR
        / "capture_complete.json"
    )

    if not start_path.is_file():
        failures.append(
            "capture_start_missing"
        )

    if not complete_path.is_file():
        failures.append(
            "capture_complete_missing"
        )

    if failures:
        return {
            "safe_to_parse": False,
            "failures": failures,
            "stream_hash_failures": None,
            "raw_hash_failures": None,
        }

    start_sha = sha256_file(
        start_path
    )

    complete_sha = sha256_file(
        complete_path
    )

    if (
        start_sha
        != EXPECTED_CAPTURE_START_SHA
    ):
        failures.append(
            "capture_start_sha"
        )

    if (
        complete_sha
        != EXPECTED_CAPTURE_COMPLETE_SHA
    ):
        failures.append(
            "capture_complete_sha"
        )

    if failures:
        return {
            "safe_to_parse": False,
            "failures": failures,
            "stream_hash_failures": None,
            "raw_hash_failures": None,
        }

    start = read_json(
        start_path
    )

    complete = read_json(
        complete_path
    )

    provenance_ok = bool(
        start.get("study") == "H5"
        and
        start.get("milestone")
        == "H5-M1R2"
        and
        start.get("status")
        == "CAPTURE_STARTED"
        and
        start.get("capture_id")
        == CAPTURE_ID
        and
        start.get("collector_sha256")
        == EXPECTED_COLLECTOR_SHA
        and
        complete.get("study") == "H5"
        and
        complete.get("milestone")
        == "H5-M1R2"
        and
        complete.get("status")
        == "CAPTURE_COMPLETE"
        and
        complete.get("capture_id")
        == CAPTURE_ID
        and
        complete.get("stop_reason")
        ==
        "INITIAL_ENGINEERING_RUN_COMPLETE"
    )

    if not provenance_ok:
        failures.append(
            "capture_provenance"
        )

    if not all_false(
        start.get(
            "analysis_boundary"
        )
    ):
        failures.append(
            "capture_start_boundary"
        )

    if not all_false(
        complete.get(
            "analysis_boundary"
        )
    ):
        failures.append(
            "capture_complete_boundary"
        )

    required_streams = {
        "external_polls",
        "external_family",
        "external_consensus",
        "pm_raw_frames",
        "pm_normalized",
        "pm_control",
    }

    stream_meta = complete.get(
        "stream_hashes",
        {},
    )

    stream_hash_failures = 0

    if (
        set(stream_meta)
        != required_streams
    ):
        stream_hash_failures += 1

    for name in sorted(
        required_streams
    ):
        meta = stream_meta.get(
            name
        )

        if not isinstance(
            meta,
            dict,
        ):
            stream_hash_failures += 1
            continue

        path = (
            REPO
            / str(meta.get("path") or "")
        )

        if not path.is_file():
            stream_hash_failures += 1
            continue

        try:
            path.resolve().relative_to(
                CAPTURE_DIR.resolve()
            )
        except ValueError:
            stream_hash_failures += 1
            continue

        if (
            sha256_file(path)
            != meta.get("sha256")
        ):
            stream_hash_failures += 1

        if (
            path.stat().st_size
            != int(
                meta.get(
                    "bytes",
                    -1,
                )
            )
        ):
            stream_hash_failures += 1

        receipt_meta = (
            receipt.get(
                "stream_hashes",
                {}
            ).get(name)
        )

        if (
            not isinstance(
                receipt_meta,
                dict,
            )
            or
            receipt_meta.get("sha256")
            != meta.get("sha256")
            or
            receipt_meta.get("bytes")
            != meta.get("bytes")
        ):
            stream_hash_failures += 1

    raw_hash_failures = 0

    raws = complete.get(
        "external_raw_files",
        [],
    )

    receipt_raws = (
        receipt.get(
            "external_raw_files",
            []
        )
    )

    if (
        len(raws) != 60
        or
        len(receipt_raws) != 60
    ):
        raw_hash_failures += 1

    receipt_raw_by_path = {
        str(x.get("path")): x
        for x in receipt_raws
        if isinstance(x, dict)
    }

    for meta in raws:
        path_text = str(
            meta.get("path") or ""
        )

        path = (
            REPO
            / path_text
        )

        if not path.is_file():
            raw_hash_failures += 1
            continue

        try:
            path.resolve().relative_to(
                CAPTURE_DIR.resolve()
            )
        except ValueError:
            raw_hash_failures += 1
            continue

        if (
            sha256_file(path)
            != meta.get("sha256")
        ):
            raw_hash_failures += 1

        if (
            path.stat().st_size
            != int(
                meta.get(
                    "bytes",
                    -1,
                )
            )
        ):
            raw_hash_failures += 1

        frozen = (
            receipt_raw_by_path.get(
                path_text
            )
        )

        if (
            not isinstance(
                frozen,
                dict,
            )
            or
            frozen.get("sha256")
            != meta.get("sha256")
            or
            frozen.get("bytes")
            != meta.get("bytes")
        ):
            raw_hash_failures += 1

    safe_to_parse = bool(
        not failures
        and
        stream_hash_failures == 0
        and
        raw_hash_failures == 0
    )

    return {
        "safe_to_parse":
            safe_to_parse,

        "failures":
            failures,

        "stream_hash_failures":
            stream_hash_failures,

        "raw_hash_failures":
            raw_hash_failures,

        "start":
            start,

        "complete":
            complete,

        "stream_paths": {
            name:
                REPO
                / stream_meta[
                    name
                ]["path"]
            for name
            in required_streams
            if (
                name in stream_meta
                and isinstance(
                    stream_meta[name],
                    dict,
                )
            )
        },
    }


def scan_engineering_data(
    *,
    integrity: dict[str, Any],
    static: dict[str, Any],
) -> dict[str, Any]:
    base = static[
        "base"
    ]

    bundle = static[
        "bundle"
    ]

    paths = integrity[
        "stream_paths"
    ]

    complete = integrity[
        "complete"
    ]

    polls = read_jsonl(
        paths["external_polls"]
    )

    family = read_jsonl(
        paths["external_family"]
    )

    consensus = read_jsonl(
        paths["external_consensus"]
    )

    pm = read_jsonl(
        paths["pm_normalized"]
    )

    control = read_jsonl(
        paths["pm_control"]
    )

    runtime_rows = bundle[
        "rows"
    ]

    expected_pairs = {
        (
            str(
                row[
                    "condition_id"
                ]
            ).lower(),
            str(
                row[
                    "odds_game_id"
                ]
            ),
        )
        for row in runtime_rows
    }

    expected_tokens = set()

    token_start = {}

    p1_by_condition = {}

    for row in runtime_rows:
        cid = str(
            row[
                "condition_id"
            ]
        ).lower()

        p1 = str(
            row[
                "p1_token_id"
            ]
        )

        p0 = str(
            row[
                "p0_token_id"
            ]
        )

        start = base.parse_dt(
            row[
                "canonical_commence_time"
            ]
        )

        if start is None:
            raise RuntimeError(
                "runtime start became invalid"
            )

        expected_tokens.update(
            (p1, p0)
        )

        token_start[p1] = start
        token_start[p0] = start

        p1_by_condition[
            cid
        ] = p1

    unexpected_identity = 0

    for rows in (
        family,
        consensus,
    ):
        for row in rows:
            pair = (
                str(
                    row.get(
                        "condition_id"
                    )
                    or ""
                ).lower(),
                str(
                    row.get(
                        "odds_game_id"
                    )
                    or ""
                ),
            )

            if pair not in expected_pairs:
                unexpected_identity += 1

    for row in pm:
        token = str(
            row.get("token")
            or ""
        )

        if (
            not token
            or
            token not in expected_tokens
        ):
            unexpected_identity += 1

    clock_missing = 0
    in_play = 0

    for row in polls:
        if (
            base.parse_dt(
                row.get(
                    "request_started_at_utc"
                )
            )
            is None
        ):
            clock_missing += 1

        if (
            base.parse_dt(
                row.get(
                    "response_received_at_utc"
                )
            )
            is None
        ):
            clock_missing += 1

    for row in consensus:
        if (
            base.parse_dt(
                row.get(
                    "response_received_at_utc"
                )
            )
            is None
        ):
            clock_missing += 1

    for row in family:
        requested = base.parse_dt(
            row.get(
                "request_started_at_utc"
            )
        )

        received = base.parse_dt(
            row.get(
                "response_received_at_utc"
            )
        )

        start = base.parse_dt(
            row.get(
                "canonical_commence_time"
            )
        )

        source_raw = row.get(
            "family_source_timestamp_utc"
        )

        source = (
            base.parse_dt(
                source_raw
            )
            if source_raw is not None
            else None
        )

        if requested is None:
            clock_missing += 1

        if received is None:
            clock_missing += 1

        if start is None:
            clock_missing += 1

        if (
            source_raw is not None
            and source is None
        ):
            clock_missing += 1

        if (
            received is not None
            and start is not None
            and received >= start
        ):
            in_play += 1

    pm_post_start = 0
    normalized_resolution = 0

    for row in pm:
        capture_time = (
            base.parse_dt(
                row.get(
                    "capture_time"
                )
            )
        )

        if capture_time is None:
            clock_missing += 1

        token = str(
            row.get("token")
            or ""
        )

        start = token_start.get(
            token
        )

        if (
            capture_time is not None
            and start is not None
            and capture_time >= start
        ):
            pm_post_start += 1

        kind = str(
            row.get("kind")
            or ""
        ).lower()

        payload = (
            row.get("payload")
            if isinstance(
                row.get("payload"),
                dict,
            )
            else {}
        )

        event_type = str(
            payload.get(
                "event_type"
            )
            or ""
        ).lower()

        if (
            "resolved" in kind
            or
            event_type
            == "market_resolved"
        ):
            normalized_resolution += 1

    for row in control:
        if (
            base.parse_dt(
                row.get(
                    "capture_time"
                )
            )
            is None
        ):
            clock_missing += 1

    eligible = len(
        consensus
    )

    accepted = sum(
        1
        for row in consensus
        if (
            row.get(
                "accepted_external_state"
            )
            is True
        )
    )

    fresh_coverage = (
        accepted / eligible
        if eligible
        else None
    )

    distinct_markets = len({
        str(
            row.get(
                "condition_id"
            )
            or ""
        ).lower()
        for row in consensus
        if row.get(
            "condition_id"
        )
    })

    pm_coverage = (
        base.pm_checkpoint_coverage(
            consensus_rows=consensus,
            pm_rows=pm,
            pm_control=control,
            p1_by_condition=p1_by_condition,
        )
    )

    request_latencies = []

    for row in polls:
        try:
            request_latencies.append(
                float(
                    row.get(
                        "request_latency_ms"
                    )
                )
            )
        except (
            TypeError,
            ValueError,
        ):
            pass

    source_ages_all = []
    source_ages_fresh = []

    for row in family:
        try:
            age = float(
                row.get(
                    "source_age_seconds"
                )
            )
        except (
            TypeError,
            ValueError,
        ):
            continue

        source_ages_all.append(
            age
        )

        if (
            row.get(
                "family_fresh"
            )
            is True
        ):
            source_ages_fresh.append(
                age
            )

    counts = complete.get(
        "counts",
        {},
    )

    manifest_counts_match = bool(
        int(
            counts.get(
                "completed_external_polls",
                -1,
            )
        )
        == len(polls)
        == 60
        and
        int(
            counts.get(
                "eligible_external_observations",
                -1,
            )
        )
        == eligible
        and
        int(
            counts.get(
                "accepted_external_states",
                -1,
            )
        )
        == accepted
    )

    mapping_failures = int(
        counts.get(
            "mapping_failures",
            -1,
        )
    )

    hard = {
        "frozen_artifact_hashes_match": {
            "observed": True,
            "required": True,
            "pass": True,
        },

        "capture_provenance_matches": {
            "observed":
                manifest_counts_match,
            "required":
                True,
            "pass":
                manifest_counts_match,
        },

        "only_authoritative_r2_capture_used": {
            "observed": True,
            "required": True,
            "pass": True,
        },

        "analysis_boundary_all_false": {
            "observed": True,
            "required": True,
            "pass": True,
        },

        "team_mapping_failures": {
            "observed":
                mapping_failures,
            "required":
                0,
            "pass":
                mapping_failures == 0,
        },

        "f19_overlap": {
            "observed": 0,
            "required": 0,
            "pass": True,
            "verification":
                "Frozen R2 runtime lineage and Stage1 exclusion freeze.",
        },

        "h2_burned_overlap": {
            "observed": 0,
            "required": 0,
            "pass": True,
            "verification":
                "Frozen Stage2 RESOLVED_NONBURNED map and H2 identity binding.",
        },

        "clock_fields_missing": {
            "observed":
                clock_missing,
            "required":
                0,
            "pass":
                clock_missing == 0,
        },

        "in_play_external_records_admitted": {
            "observed":
                in_play,
            "required":
                0,
            "pass":
                in_play == 0,
        },

        "normalized_pm_post_start_records_admitted": {
            "observed":
                pm_post_start,
            "required":
                0,
            "pass":
                pm_post_start == 0,
        },

        "normalized_resolution_events_admitted": {
            "observed":
                normalized_resolution,
            "required":
                0,
            "pass":
                normalized_resolution == 0,
        },

        "capture_stream_hash_failures": {
            "observed":
                integrity[
                    "stream_hash_failures"
                ],
            "required":
                0,
            "pass":
                integrity[
                    "stream_hash_failures"
                ]
                == 0,
        },

        "raw_external_hash_failures": {
            "observed":
                integrity[
                    "raw_hash_failures"
                ],
            "required":
                0,
            "pass":
                integrity[
                    "raw_hash_failures"
                ]
                == 0,
        },

        "unexpected_market_or_token_identity": {
            "observed":
                unexpected_identity,
            "required":
                0,
            "pass":
                unexpected_identity == 0,
        },
    }

    pm_cov_value = (
        pm_coverage[
            "coverage"
        ]
    )

    sufficiency = {
        "minimum_distinct_acquisition_markets": {
            "observed":
                distinct_markets,
            "required":
                MIN_MARKETS,
            "pass":
                distinct_markets
                >= MIN_MARKETS,
        },

        "minimum_accepted_external_states": {
            "observed":
                accepted,
            "required":
                MIN_ACCEPTED,
            "pass":
                accepted
                >= MIN_ACCEPTED,
        },

        "fresh_external_family_coverage": {
            "observed":
                fresh_coverage,
            "required_minimum":
                MIN_FRESH_COVERAGE,
            "pass":
                fresh_coverage is not None
                and
                fresh_coverage
                >= MIN_FRESH_COVERAGE,
        },

        "pm_book_coverage": {
            "observed":
                pm_cov_value,
            "required_minimum":
                MIN_PM_COVERAGE,
            "pass":
                pm_cov_value is not None
                and
                pm_cov_value
                >= MIN_PM_COVERAGE,
        },
    }

    metrics = {
        "external_polls":
            len(polls),

        "eligible_external_observations":
            eligible,

        "accepted_external_states":
            accepted,

        "distinct_markets_with_external_observation":
            distinct_markets,

        "fresh_external_family_coverage":
            fresh_coverage,

        "request_latency_ms":
            base.summarize_numeric(
                request_latencies
            ),

        "source_age_seconds_all":
            base.summarize_numeric(
                source_ages_all
            ),

        "source_age_seconds_fresh":
            base.summarize_numeric(
                source_ages_fresh
            ),

        "family_source_update_interval_seconds":
            base.source_update_intervals(
                family
            ),

        "pm": {
            **base.pm_message_diagnostics(
                pm,
                control,
            ),

            "checkpoint_coverage":
                pm_coverage,
        },
    }

    return {
        "hard_integrity_gates":
            hard,

        "engineering_sufficiency_gates":
            sufficiency,

        "engineering_metrics":
            metrics,
    }


def minimal_integrity_failure_report(
    *,
    integrity: dict[str, Any],
) -> dict[str, Any]:
    hard = {
        "capture_structure_or_hash_integrity": {
            "observed":
                False,
            "required":
                True,
            "pass":
                False,
            "failures":
                integrity.get(
                    "failures",
                    []
                ),
            "stream_hash_failures":
                integrity.get(
                    "stream_hash_failures"
                ),
            "raw_hash_failures":
                integrity.get(
                    "raw_hash_failures"
                ),
        }
    }

    return {
        "study": "H5",
        "milestone": "H5-M1R2",
        "stage": "ENGINEERING_VALIDATION",
        "status": "FAIL_INTEGRITY",
        "capture_id": CAPTURE_ID,
        "hard_integrity_gates": hard,
        "engineering_sufficiency_gates": {},
        "engineering_metrics": {},
        "analysis_boundary": {
            "external_consensus_difference_calculated": False,
            "innovation_calculated": False,
            "pm_response_calculated": False,
            "future_pm_price_change_calculated": False,
            "directional_hit_rate_calculated": False,
            "correlation_calculated": False,
            "regression_calculated": False,
            "markout_calculated": False,
            "edge_calculated": False,
            "pnl_calculated": False,
            "winner_used": False,
            "settlement_used": False,
            "f19_trade_content_read": False,
            "f20_read": False,
            "h2_performance_read": False,
            "h6_response_read": False
        }
    }


def evaluate_authoritative() -> dict[str, Any]:
    static = load_static_inputs(
        require_committed_self=True
    )

    integrity = verify_capture_integrity(
        static
    )

    if not integrity[
        "safe_to_parse"
    ]:
        return (
            minimal_integrity_failure_report(
                integrity=integrity
            )
        )

    scan = scan_engineering_data(
        integrity=integrity,
        static=static,
    )

    hard_pass = all(
        gate["pass"]
        for gate in scan[
            "hard_integrity_gates"
        ].values()
    )

    sufficiency_pass = all(
        gate["pass"]
        for gate in scan[
            "engineering_sufficiency_gates"
        ].values()
    )

    verdict = classify_verdict(
        hard_integrity_pass=hard_pass,
        sufficiency_pass=sufficiency_pass,
    )

    report = {
        "study":
            "H5",

        "milestone":
            "H5-M1R2",

        "stage":
            "ENGINEERING_VALIDATION",

        "status":
            verdict,

        "capture_id":
            CAPTURE_ID,

        "validation_contract_sha256":
            EXPECTED_VALIDATION_CONTRACT_SHA,

        "capture_receipt_sha256":
            EXPECTED_CAPTURE_RECEIPT_SHA,

        "collector_sha256":
            EXPECTED_COLLECTOR_SHA,

        "runtime_sha256":
            EXPECTED_RUNTIME_SHA,

        "base_validator_helper_sha256":
            EXPECTED_BASE_VALIDATOR_SHA,

        "hard_integrity_gates":
            scan[
                "hard_integrity_gates"
            ],

        "engineering_sufficiency_gates":
            scan[
                "engineering_sufficiency_gates"
            ],

        "engineering_metrics":
            scan[
                "engineering_metrics"
            ],

        "primary_horizon_selected":
            False,

        "primary_horizon_seconds":
            None,

        "analysis_boundary": {
            "external_consensus_difference_calculated": False,
            "innovation_calculated": False,
            "pm_response_calculated": False,
            "future_pm_price_change_calculated": False,
            "directional_hit_rate_calculated": False,
            "correlation_calculated": False,
            "regression_calculated": False,
            "markout_calculated": False,
            "edge_calculated": False,
            "pnl_calculated": False,
            "winner_used": False,
            "settlement_used": False,
            "f19_trade_content_read": False,
            "f20_read": False,
            "h2_performance_read": False,
            "h6_response_read": False
        },

        "interpretation": (
            "Engineering feasibility only. "
            "No H5 innovation, PM response, "
            "directional effect, edge, or PnL "
            "was calculated."
        ),
    }

    return report


def write_report_exclusive(
    report: dict[str, Any],
):
    RESULT.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with RESULT.open(
        "x",
        encoding="utf-8",
    ) as fh:
        json.dump(
            report,
            fh,
            sort_keys=True,
            separators=(",", ":"),
        )

        fh.write("\n")


def validate_config_output():
    static = load_static_inputs(
        require_committed_self=False
    )

    _ = static

    print(
        "H5-M1R2 ENGINEERING VALIDATOR CONFIG: PASS"
    )

    print(
        "validation contract SHA:",
        EXPECTED_VALIDATION_CONTRACT_SHA,
    )

    print(
        "capture receipt SHA:",
        EXPECTED_CAPTURE_RECEIPT_SHA,
    )

    print(
        "runtime SHA:",
        EXPECTED_RUNTIME_SHA,
    )

    print(
        "collector SHA:",
        EXPECTED_COLLECTOR_SHA,
    )

    print(
        "old causal checkpoint helper SHA:",
        EXPECTED_BASE_VALIDATOR_SHA,
    )

    print(
        "authoritative capture:",
        CAPTURE_ID,
    )

    print(
        "authoritative capture streams read: NO"
    )

    print(
        "network access: NO"
    )

    print(
        "additional acquisition permitted: NO"
    )

    print(
        "H5 innovation calculated: NO"
    )

    print(
        "PM response calculated: NO"
    )

    print(
        "primary horizon selected: NO"
    )

    print(
        "PnL calculated: NO"
    )


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--validate-config",
        action="store_true",
    )

    parser.add_argument(
        "--run-authoritative",
        action="store_true",
    )

    args = parser.parse_args()

    if (
        args.validate_config
        ==
        args.run_authoritative
    ):
        parser.error(
            "choose exactly one of "
            "--validate-config or "
            "--run-authoritative"
        )

    if args.validate_config:
        validate_config_output()
        return

    report = (
        evaluate_authoritative()
    )

    write_report_exclusive(
        report
    )

    print(
        "========================================"
    )

    print(
        "H5-M1R2 ENGINEERING VALIDATION"
    )

    print(
        "========================================"
    )

    print(
        "status:",
        report["status"],
    )

    print(
        "capture_id:",
        report["capture_id"],
    )

    if report.get(
        "engineering_metrics"
    ):
        metrics = report[
            "engineering_metrics"
        ]

        print(
            "accepted_external_states:",
            metrics[
                "accepted_external_states"
            ],
        )

        print(
            "fresh_external_family_coverage:",
            metrics[
                "fresh_external_family_coverage"
            ],
        )

        print(
            "pm_book_coverage:",
            metrics[
                "pm"
            ][
                "checkpoint_coverage"
            ][
                "coverage"
            ],
        )

    print(
        "H5 innovation calculated: NO"
    )

    print(
        "PM response calculated: NO"
    )

    print(
        "PnL calculated: NO"
    )

    print(
        "output:",
        RESULT,
    )


if __name__ == "__main__":
    main()
