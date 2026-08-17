from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
import subprocess
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO = Path(__file__).resolve().parents[1]
RESEARCH = REPO / "research"

M1A_CONTRACT = RESEARCH / "h5_m1a_acquisition_contract.json"
REGISTRY = REPO / "data" / "research" / "h5_m1" / "registry.jsonl"
REGISTRY_RECEIPT = RESEARCH / "h5_m1_registry_freeze.json"
COLLECTOR = REPO / "collectors" / "odds" / "h5_m1_collector.py"

EXPECTED_M1A_SHA = (
    "6da26ac94413b5a79566764dcc43d4584"
    "bff94c3b88531225dec09a4db99b1be"
)
EXPECTED_COLLECTOR_SHA = (
    "995db8aed0f1305b705339d61c5ab647"
    "bb5805ec753abfed6fab3c4b0061b518"
)
EXPECTED_REGISTRAR_SHA = (
    "fe6bafa6c21aa6dbc7490d6b82109a1"
    "90f0f563b8e42e93557ce9e245b6e5719"
)
EXPECTED_M0_EXCLUSIONS_SHA = (
    "fb4a268f3148e746e31232c658cdc879"
    "3c2e082a73c5a8260af6779d89b1949a"
)
EXPECTED_COLLECTOR_COMMIT = (
    "8d3b62de3c54d6522ce438fcd6cbde560fd38bf5"
)

MIN_MARKETS = 5
MIN_ACCEPTED_EXTERNAL_STATES = 100
MIN_FRESH_COVERAGE = 0.80
MIN_PM_COVERAGE = 0.95
CANDIDATE_HORIZONS_SECONDS = [1, 3, 5, 10, 30, 60]

BOUNDARY_FALSE_KEYS = (
    "external_state_differences_calculated",
    "h5_innovation_calculated",
    "pm_response_calculated",
    "future_pm_price_change_calculated",
    "directional_hit_rate_calculated",
    "correlation_calculated",
    "regression_calculated",
    "markout_calculated",
    "pnl_calculated",
    "winner_used",
    "settlement_used",
    "f19_trade_content_read",
    "f20_read",
)


def parse_dt(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        return None
    return dt.astimezone(timezone.utc)


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def git_head() -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO,
        text=True,
    ).strip()


def git_is_ancestor(ancestor: str, descendant: str = "HEAD") -> bool:
    result = subprocess.run(
        ["git", "merge-base", "--is-ancestor", ancestor, descendant],
        cwd=REPO,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return result.returncode == 0


def committed_self_matches_head() -> bool:
    path = Path(__file__).resolve()
    try:
        rel = path.relative_to(REPO).as_posix()
        committed = subprocess.check_output(
            ["git", "show", f"HEAD:{rel}"],
            cwd=REPO,
        )
    except (ValueError, subprocess.CalledProcessError):
        return False
    return sha256_bytes(committed) == sha256_file(path)


def read_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise RuntimeError(f"expected JSON object: {path}")
    return data


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    out = []
    with path.open("r", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, start=1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise RuntimeError(
                    f"invalid JSONL {path}:{lineno}: {exc.msg}"
                ) from None
            if not isinstance(row, dict):
                raise RuntimeError(
                    f"non-object JSONL row {path}:{lineno}"
                )
            out.append(row)
    return out


def validate_boundary(boundary: dict[str, Any], *, where: str):
    for key in BOUNDARY_FALSE_KEYS:
        if boundary.get(key) is not False:
            raise RuntimeError(
                f"scientific boundary violation in {where}: {key}"
            )


def load_contract(*, require_committed_self: bool):
    if not M1A_CONTRACT.is_file():
        raise RuntimeError("missing H5-M1A contract")
    if sha256_file(M1A_CONTRACT) != EXPECTED_M1A_SHA:
        raise RuntimeError("H5-M1A contract SHA mismatch")
    if not COLLECTOR.is_file():
        raise RuntimeError("missing frozen H5-M1 collector")
    if sha256_file(COLLECTOR) != EXPECTED_COLLECTOR_SHA:
        raise RuntimeError("frozen H5-M1 collector SHA mismatch")

    contract = read_json(M1A_CONTRACT)
    gates = contract["m1_success_requirements"]

    if gates["minimum_distinct_markets"] != MIN_MARKETS:
        raise RuntimeError("M1 market-count gate mismatch")
    if gates["minimum_accepted_external_states"] != MIN_ACCEPTED_EXTERNAL_STATES:
        raise RuntimeError("M1 accepted-state gate mismatch")
    if contract["candidate_horizons_seconds"] != CANDIDATE_HORIZONS_SECONDS:
        raise RuntimeError("candidate horizon set mismatch")

    if require_committed_self:
        if not git_is_ancestor(EXPECTED_COLLECTOR_COMMIT):
            raise RuntimeError(
                "frozen H5-M1 collector commit is not an ancestor of HEAD"
            )
        if not committed_self_matches_head():
            raise RuntimeError(
                "refusing M1B readout: validator differs from committed HEAD"
            )

    return contract


def load_registry() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not REGISTRY.is_file() or not REGISTRY_RECEIPT.is_file():
        raise RuntimeError("H5 registry/receipt not frozen")

    rows = read_jsonl(REGISTRY)
    receipt = read_json(REGISTRY_RECEIPT)

    if not 5 <= len(rows) <= 10:
        raise RuntimeError("invalid H5 registry size")
    if receipt.get("status") != "REGISTRY_FROZEN":
        raise RuntimeError("registry receipt not frozen")
    if receipt.get("m1a_contract_sha256") != EXPECTED_M1A_SHA:
        raise RuntimeError("registry receipt M1A SHA mismatch")
    if (
        receipt.get("registration_code", {}).get("sha256")
        != EXPECTED_REGISTRAR_SHA
    ):
        raise RuntimeError("registry receipt registrar SHA mismatch")
    if receipt.get("m0_exclusions_sha256") != EXPECTED_M0_EXCLUSIONS_SHA:
        raise RuntimeError("registry receipt exclusions SHA mismatch")

    selection = receipt.get("selection", {})
    if int(selection.get("excluded_h2_v2_external_ids", -1)) != 9:
        raise RuntimeError("registry receipt H2 exclusion universe mismatch")
    if int(selection.get("excluded_f19_condition_ids", -1)) != 100:
        raise RuntimeError("registry receipt F19 exclusion universe mismatch")

    if receipt.get("registry", {}).get("sha256") != sha256_file(REGISTRY):
        raise RuntimeError("registry SHA mismatch")
    if int(receipt.get("registry", {}).get("row_count", -1)) != len(rows):
        raise RuntimeError("registry row-count mismatch")

    registration_boundary = receipt.get("analysis_boundary", {})
    for key in (
        "external_odds_read",
        "external_consensus_calculated",
        "h5_innovation_calculated",
        "pm_price_used_for_selection",
        "pm_response_calculated",
        "future_pm_state_read",
        "pnl_calculated",
        "winner_used",
        "settlement_used",
        "f19_trade_content_read",
        "f20_read",
    ):
        if registration_boundary.get(key) is not False:
            raise RuntimeError(
                f"invalid registry analysis boundary: {key}"
            )

    return rows, receipt


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    xs = sorted(values)
    if len(xs) == 1:
        return xs[0]
    pos = (len(xs) - 1) * q
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return xs[lo]
    weight = pos - lo
    return xs[lo] * (1 - weight) + xs[hi] * weight


def summarize_numeric(values: list[float]) -> dict[str, Any]:
    clean = [
        float(v)
        for v in values
        if v is not None and math.isfinite(float(v))
    ]
    if not clean:
        return {
            "n": 0,
            "min": None,
            "median": None,
            "p95": None,
            "max": None,
        }
    return {
        "n": len(clean),
        "min": min(clean),
        "median": statistics.median(clean),
        "p95": percentile(clean, 0.95),
        "max": max(clean),
    }


def file_meta(path: Path) -> dict[str, Any]:
    return {
        "path": str(path.relative_to(REPO)),
        "sha256": sha256_file(path),
        "bytes": path.stat().st_size,
    }


def verify_capture_integrity(
    capture_dir: Path,
    *,
    registry_sha: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    start_path = capture_dir / "capture_start.json"
    complete_path = capture_dir / "capture_complete.json"

    if not start_path.is_file():
        raise RuntimeError("capture_start.json missing")
    if not complete_path.is_file():
        raise RuntimeError("capture_complete.json missing")

    start = read_json(start_path)
    complete = read_json(complete_path)

    if start.get("study") != "H5" or start.get("milestone") != "H5-M1":
        raise RuntimeError("capture_start wrong study/milestone")
    if start.get("status") != "CAPTURE_STARTED":
        raise RuntimeError("capture_start wrong status")
    if complete.get("study") != "H5" or complete.get("milestone") != "H5-M1":
        raise RuntimeError("capture_complete wrong study/milestone")
    if complete.get("status") != "CAPTURE_COMPLETE":
        raise RuntimeError("capture_complete wrong status")
    if start.get("capture_id") != complete.get("capture_id"):
        raise RuntimeError("capture ID mismatch")
    if start.get("m1a_contract_sha256") != EXPECTED_M1A_SHA:
        raise RuntimeError("capture M1A SHA mismatch")
    if start.get("collector_sha256") != EXPECTED_COLLECTOR_SHA:
        raise RuntimeError("capture collector SHA mismatch")
    if start.get("registry_sha256") != registry_sha:
        raise RuntimeError("capture registry SHA mismatch")
    if int(start.get("registered_market_count", -1)) < MIN_MARKETS:
        raise RuntimeError("capture registered fewer than five markets")

    validate_boundary(
        start.get("analysis_boundary", {}),
        where="capture_start",
    )
    validate_boundary(
        complete.get("analysis_boundary", {}),
        where="capture_complete",
    )

    streams = complete.get("stream_hashes", {})
    required_streams = {
        "external_polls",
        "external_family",
        "external_consensus",
        "pm_raw_frames",
        "pm_normalized",
        "pm_control",
    }
    if set(streams) != required_streams:
        raise RuntimeError("capture stream set mismatch")

    for name in sorted(required_streams):
        meta = streams[name]
        path = REPO / str(meta["path"])
        if not path.is_file():
            raise RuntimeError(f"missing capture stream: {name}")
        if path.resolve().parent != capture_dir.resolve():
            raise RuntimeError(f"capture stream escaped directory: {name}")
        if sha256_file(path) != meta.get("sha256"):
            raise RuntimeError(f"capture stream SHA mismatch: {name}")
        if path.stat().st_size != int(meta.get("bytes", -1)):
            raise RuntimeError(f"capture stream size mismatch: {name}")

    for raw in complete.get("external_raw_files", []):
        path = REPO / str(raw["path"])
        if not path.is_file():
            raise RuntimeError("missing external raw file")
        try:
            path.resolve().relative_to(capture_dir.resolve())
        except ValueError:
            raise RuntimeError("external raw file escaped capture directory") from None
        if sha256_file(path) != raw.get("sha256"):
            raise RuntimeError("external raw SHA mismatch")
        if path.stat().st_size != int(raw.get("bytes", -1)):
            raise RuntimeError("external raw size mismatch")

    return start, complete


def tob_presence_from_payload(
    kind: str,
    payload: dict[str, Any],
    current: tuple[bool, bool] | None,
) -> tuple[bool, bool]:
    current = current or (False, False)
    bid, ask = current

    if kind in ("rest_resync_book", "book"):
        bids = payload.get("bids") or []
        asks = payload.get("asks") or []
        return bool(bids), bool(asks)

    if kind == "best_bid_ask":
        return (
            payload.get("best_bid") is not None,
            payload.get("best_ask") is not None,
        )

    if kind == "price_change":
        if "best_bid" in payload:
            bid = payload.get("best_bid") is not None
        if "best_ask" in payload:
            ask = payload.get("best_ask") is not None
        return bid, ask

    return bid, ask


def pm_checkpoint_coverage(
    *,
    consensus_rows: list[dict[str, Any]],
    pm_rows: list[dict[str, Any]],
    pm_control: list[dict[str, Any]],
    p1_by_condition: dict[str, str],
) -> dict[str, Any]:
    timeline = []

    for row in pm_rows:
        ts = parse_dt(row.get("capture_time"))
        token = str(row.get("token") or "")
        if ts is None or not token:
            continue
        timeline.append(
            (
                ts,
                1,
                {
                    "type": "pm",
                    "kind": str(row.get("kind") or ""),
                    "token": token,
                    "payload": row.get("payload") or {},
                    "connection_id": row.get("connection_id"),
                    "book_generation": row.get("book_generation"),
                },
            )
        )

    for row in pm_control:
        ts = parse_dt(row.get("capture_time"))
        if ts is None:
            continue
        timeline.append(
            (
                ts,
                0,
                {
                    "type": "control",
                    "kind": str(row.get("kind") or ""),
                    "connection_id": row.get("connection_id"),
                },
            )
        )

    checkpoints = []
    for row in consensus_rows:
        ts = parse_dt(row.get("response_received_at_utc"))
        cid = str(row.get("condition_id") or "").lower()
        token = p1_by_condition.get(cid)
        if ts is None or not token:
            continue
        checkpoints.append((ts, token, row))

    timeline.sort(key=lambda x: (x[0], x[1]))
    checkpoints.sort(key=lambda x: x[0])

    state: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "valid": False,
            "bid": False,
            "ask": False,
            "connection_id": None,
            "book_generation": None,
        }
    )

    pointer = 0
    valid = 0
    denominator = 0
    invalid_by_reason = defaultdict(int)

    for checkpoint_ts, token, _ in checkpoints:
        while pointer < len(timeline) and timeline[pointer][0] <= checkpoint_ts:
            _, _, event = timeline[pointer]
            pointer += 1

            if event["type"] == "control":
                if event["kind"] in ("connection_start", "connection_error"):
                    for token_state in state.values():
                        token_state["valid"] = False
                continue

            tok = event["token"]
            kind = event["kind"]
            payload = event["payload"]
            s = state[tok]

            if kind in ("rest_resync_book", "book"):
                s["valid"] = True
                s["connection_id"] = event.get("connection_id")
                s["book_generation"] = event.get("book_generation")

            s["bid"], s["ask"] = tob_presence_from_payload(
                kind,
                payload,
                (s["bid"], s["ask"]),
            )

        denominator += 1
        s = state[token]
        if not s["valid"]:
            invalid_by_reason["not_initialized_or_gap"] += 1
        elif not (s["bid"] and s["ask"]):
            invalid_by_reason["not_two_sided"] += 1
        else:
            valid += 1

    coverage = (
        valid / denominator
        if denominator
        else None
    )

    return {
        "eligible_checkpoints": denominator,
        "valid_checkpoints": valid,
        "coverage": coverage,
        "invalid_by_reason": dict(sorted(invalid_by_reason.items())),
    }


def source_update_intervals(
    family_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    grouped: dict[tuple[str, str], list[datetime]] = defaultdict(list)

    for row in family_rows:
        ts = parse_dt(row.get("family_source_timestamp_utc"))
        if ts is None:
            continue
        key = (
            str(row.get("odds_game_id") or ""),
            str(row.get("exchange_family") or ""),
        )
        grouped[key].append(ts)

    by_family: dict[str, list[float]] = defaultdict(list)

    for (_, family), timestamps in grouped.items():
        unique = sorted(set(timestamps))
        for a, b in zip(unique, unique[1:]):
            delta = (b - a).total_seconds()
            if delta > 0:
                by_family[family].append(delta)

    return {
        family: summarize_numeric(values)
        for family, values in sorted(by_family.items())
    }


def pm_message_diagnostics(
    pm_rows: list[dict[str, Any]],
    pm_control: list[dict[str, Any]],
) -> dict[str, Any]:
    times = [
        ts
        for ts in (
            parse_dt(row.get("capture_time"))
            for row in pm_rows
        )
        if ts is not None
    ]

    gaps = []
    if len(times) >= 2:
        ordered = sorted(times)
        gaps = [
            (b - a).total_seconds()
            for a, b in zip(ordered, ordered[1:])
            if b >= a
        ]

    kinds = defaultdict(int)
    for row in pm_rows:
        kinds[str(row.get("kind") or "unknown")] += 1

    controls = defaultdict(int)
    for row in pm_control:
        controls[str(row.get("kind") or "unknown")] += 1

    duration = None
    rate = None
    if times:
        duration = max(0.0, (max(times) - min(times)).total_seconds())
        if duration > 0:
            rate = len(pm_rows) / duration

    return {
        "normalized_events": len(pm_rows),
        "event_kind_counts": dict(sorted(kinds.items())),
        "control_kind_counts": dict(sorted(controls.items())),
        "observed_duration_seconds": duration,
        "normalized_messages_per_second": rate,
        "inter_event_gap_seconds": summarize_numeric(gaps),
        "connection_starts": controls.get("connection_start", 0),
        "connection_errors": controls.get("connection_error", 0),
        "rest_resync_completions": controls.get("rest_resync_complete", 0),
        "rest_reconcile_completions": controls.get(
            "rest_reconcile_complete", 0
        ),
    }


def evaluate_capture(capture_dir: Path) -> dict[str, Any]:
    contract = load_contract(require_committed_self=True)
    registry_rows, registry_receipt = load_registry()
    registry_sha = sha256_file(REGISTRY)

    start, complete = verify_capture_integrity(
        capture_dir,
        registry_sha=registry_sha,
    )

    paths = {
        name: REPO / meta["path"]
        for name, meta in complete["stream_hashes"].items()
    }

    external_polls = read_jsonl(paths["external_polls"])
    external_family = read_jsonl(paths["external_family"])
    external_consensus = read_jsonl(paths["external_consensus"])
    pm_normalized = read_jsonl(paths["pm_normalized"])
    pm_control = read_jsonl(paths["pm_control"])

    p1_by_condition = {
        str(row["condition_id"]).lower(): str(row["p1_token_id"])
        for row in registry_rows
    }

    eligible = len(external_consensus)
    accepted = sum(
        1
        for row in external_consensus
        if row.get("accepted_external_state") is True
    )
    fresh_coverage = accepted / eligible if eligible else None

    distinct_markets = len(
        {
            str(row.get("condition_id") or "").lower()
            for row in external_consensus
            if row.get("condition_id")
        }
    )

    mapping_failures = int(
        complete.get("counts", {}).get("mapping_failures", 0)
    )

    in_play = 0
    clock_missing = 0
    for row in external_family:
        recv = parse_dt(row.get("response_received_at_utc"))
        start_ts = parse_dt(row.get("canonical_commence_time"))
        source_ts = parse_dt(row.get("family_source_timestamp_utc"))

        if recv is None or start_ts is None:
            clock_missing += 1
        elif recv >= start_ts:
            in_play += 1

        if row.get("family_source_timestamp_utc") is not None and source_ts is None:
            clock_missing += 1

    request_latencies = []
    for row in external_polls:
        value = row.get("request_latency_ms")
        try:
            request_latencies.append(float(value))
        except (TypeError, ValueError):
            pass

    source_ages_all = []
    source_ages_fresh = []
    for row in external_family:
        value = row.get("source_age_seconds")
        try:
            x = float(value)
        except (TypeError, ValueError):
            continue
        source_ages_all.append(x)
        if row.get("family_fresh") is True:
            source_ages_fresh.append(x)

    pm_coverage = pm_checkpoint_coverage(
        consensus_rows=external_consensus,
        pm_rows=pm_normalized,
        pm_control=pm_control,
        p1_by_condition=p1_by_condition,
    )

    pm_cov_value = pm_coverage["coverage"]

    gates = {
        "minimum_distinct_markets": {
            "observed": distinct_markets,
            "required": MIN_MARKETS,
            "pass": distinct_markets >= MIN_MARKETS,
        },
        "minimum_accepted_external_states": {
            "observed": accepted,
            "required": MIN_ACCEPTED_EXTERNAL_STATES,
            "pass": accepted >= MIN_ACCEPTED_EXTERNAL_STATES,
        },
        "fresh_external_family_coverage": {
            "observed": fresh_coverage,
            "required_minimum": MIN_FRESH_COVERAGE,
            "pass": (
                fresh_coverage is not None
                and fresh_coverage >= MIN_FRESH_COVERAGE
            ),
        },
        "pm_book_coverage": {
            "observed": pm_cov_value,
            "required_minimum": MIN_PM_COVERAGE,
            "pass": (
                pm_cov_value is not None
                and pm_cov_value >= MIN_PM_COVERAGE
            ),
        },
        "team_mapping_failures": {
            "observed": mapping_failures,
            "required": 0,
            "pass": mapping_failures == 0,
        },
        "f19_overlap": {
            "observed": 0,
            "required": 0,
            "pass": True,
            "verification": (
                "frozen registrar SHA verified; frozen F19 exclusion "
                "universe size verified at 100 before registry freeze"
            ),
        },
        "h2_v2_burned_overlap": {
            "observed": 0,
            "required": 0,
            "pass": True,
            "verification": (
                "frozen registrar SHA verified; translated H2-v2 burned "
                "external exclusion universe size verified at 9 before "
                "registry freeze"
            ),
        },
        "clock_fields_missing": {
            "observed": clock_missing,
            "required": 0,
            "pass": clock_missing == 0,
        },
        "in_play_records_admitted": {
            "observed": in_play,
            "required": 0,
            "pass": in_play == 0,
        },
    }

    all_pass = all(item["pass"] for item in gates.values())

    status = (
        "PASS_M1_ENGINEERING_FEASIBILITY"
        if all_pass
        else "FAIL_M1_ENGINEERING_FEASIBILITY"
    )

    report = {
        "study": "H5",
        "milestone": "H5-M1B",
        "status": status,
        "capture_id": start["capture_id"],
        "capture_stop_reason": complete.get("stop_reason"),
        "collector_sha256": EXPECTED_COLLECTOR_SHA,
        "m1a_contract_sha256": EXPECTED_M1A_SHA,
        "registry_sha256": registry_sha,
        "registry_receipt_sha256": sha256_file(REGISTRY_RECEIPT),
        "capture_start_sha256": sha256_file(
            capture_dir / "capture_start.json"
        ),
        "capture_complete_sha256": sha256_file(
            capture_dir / "capture_complete.json"
        ),
        "gates": gates,
        "engineering_metrics": {
            "registered_markets": int(
                start.get("registered_market_count", 0)
            ),
            "distinct_markets_with_external_observation": distinct_markets,
            "external_polls": len(external_polls),
            "eligible_external_observations": eligible,
            "accepted_external_states": accepted,
            "fresh_external_family_coverage": fresh_coverage,
            "request_latency_ms": summarize_numeric(request_latencies),
            "source_age_seconds_all": summarize_numeric(source_ages_all),
            "source_age_seconds_fresh": summarize_numeric(source_ages_fresh),
            "family_source_update_interval_seconds":
                source_update_intervals(external_family),
            "pm": {
                **pm_message_diagnostics(
                    pm_normalized,
                    pm_control,
                ),
                "checkpoint_coverage": pm_coverage,
            },
            "capture_stream_integrity": {
                name: file_meta(path)
                for name, path in sorted(paths.items())
            },
        },
        "candidate_horizons_seconds": CANDIDATE_HORIZONS_SECONDS,
        "primary_horizon_selected": False,
        "primary_horizon_seconds": None,
        "scientific_boundary": {
            "external_state_differences_calculated": False,
            "h5_innovation_calculated": False,
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
        },
        "interpretation": (
            "Engineering feasibility only. This report does not test the "
            "H5 mechanism, select a primary horizon, or evaluate any "
            "directional Polymarket response."
        ),
    }

    # Contract identity is read above; keeping this assignment explicit
    # prevents future code from silently dropping contract validation.
    _ = contract
    _ = registry_receipt

    return report


def write_report_exclusive(
    report: dict[str, Any],
    *,
    output: Path,
):
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8") as fh:
        json.dump(
            report,
            fh,
            sort_keys=True,
            separators=(",", ":"),
        )
        fh.write("\n")


def validate_config_output():
    load_contract(require_committed_self=False)
    print("H5-M1B VALIDATOR CONFIG: PASS")
    print("M1A SHA:", EXPECTED_M1A_SHA)
    print("collector SHA:", EXPECTED_COLLECTOR_SHA)
    print("network access: NO")
    print("external state differences calculated: NO")
    print("H5 innovation calculated: NO")
    print("PM response calculated: NO")
    print("primary horizon selected: NO")
    print("PnL calculated: NO")
    print("F19 trade content read: NO")
    print("F20 read: NO")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--validate-config", action="store_true")
    parser.add_argument("--capture-id")
    parser.add_argument("--output")
    args = parser.parse_args()

    if args.validate_config:
        if args.capture_id or args.output:
            parser.error(
                "--validate-config cannot be combined with capture readout"
            )
        validate_config_output()
        return

    if not args.capture_id:
        parser.error("provide --capture-id or use --validate-config")

    capture_dir = (
        REPO / "data" / "research" / "h5_m1" / args.capture_id
    )
    if not capture_dir.is_dir():
        raise RuntimeError(f"capture not found: {capture_dir}")

    report = evaluate_capture(capture_dir)

    if args.output:
        output = Path(args.output)
        if not output.is_absolute():
            output = REPO / output
    else:
        output = (
            RESEARCH
            / f"h5_m1b_{args.capture_id}_validation.json"
        )

    write_report_exclusive(report, output=output)

    print("========================================")
    print("H5-M1B ENGINEERING READOUT")
    print("========================================")
    print("status:", report["status"])
    print("capture_id:", report["capture_id"])
    print(
        "distinct_markets:",
        report["engineering_metrics"][
            "distinct_markets_with_external_observation"
        ],
    )
    print(
        "accepted_external_states:",
        report["engineering_metrics"]["accepted_external_states"],
    )
    print(
        "fresh_external_family_coverage:",
        report["engineering_metrics"][
            "fresh_external_family_coverage"
        ],
    )
    print(
        "pm_book_coverage:",
        report["engineering_metrics"]["pm"][
            "checkpoint_coverage"
        ]["coverage"],
    )
    print("primary horizon selected: NO")
    print("H5 innovation calculated: NO")
    print("PM response calculated: NO")
    print("PnL calculated: NO")
    print("F19 trade content read: NO")
    print("F20 read: NO")
    print("output:", output)


if __name__ == "__main__":
    main()
