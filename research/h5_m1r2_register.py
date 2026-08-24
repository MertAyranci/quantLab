from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx


REPO = Path(__file__).resolve().parents[1]

PREREG = REPO / "research/h5_m1r2_rerun_amendment.json"
ADDENDUM = REPO / "research/h5_m1r1_identity_exclusion_addendum.json"
M0 = REPO / "research/h5_m0_mechanism_data_design.json"
EXCLUSIONS = REPO / "research/h5_m0_exclusions.json"
M1A = REPO / "research/h5_m1a_acquisition_contract.json"
H2_IDS = REPO / "research/h6_m2_h2_identity_exclusions.json"

OLD_REGISTRY = REPO / "data/research/h5_m1/registry.jsonl"
OLD_CLOSURE = REPO / "research/h5_m1_operational_closure.json"

REGISTRY = REPO / "data/research/h5_m1r2/registry.jsonl"
RECEIPT = REPO / "research/h5_m1r2_registry_freeze.json"

EXPECTED_PREREG_SHA = (
    "b805cc2fc9a01e1cd4b4d2d5c7966fde"
    "d4e19840bcbefcb72f102d50a0c380ed"
)

EXPECTED_R1_PREREG_SHA = (
    "bcfd8b2433d6c8bb4d55c5d478c29ab"
    "0540ef1ed956eaf2040c39aad3b69ab61"
)

EXPECTED_ADDENDUM_SHA = (
    "a6ad0066a98d06f24075d28fa6fa4544"
    "5affe3fa83de2b926ab992cb67721b31"
)

EXPECTED_M0_SHA = (
    "073730512baa587e5be730f534daf1e3"
    "89c8934d301769a77f387e1fff0a553c"
)

EXPECTED_EXCLUSIONS_SHA = (
    "fb4a268f3148e746e31232c658cdc879"
    "3c2e082a73c5a8260af6779d89b1949a"
)

EXPECTED_M1A_SHA = (
    "6da26ac94413b5a79566764dcc43d4584"
    "bff94c3b88531225dec09a4db99b1be"
)

EXPECTED_H2_SHA = (
    "0907e1bd4ccc2aaaf7cef332e7fe96b8"
    "fb05355afdfc2e912d412e2f10c78d73"
)

EXPECTED_OLD_REGISTRY_SHA = (
    "c73e65dea1ba76eea40f1d724aa7d963"
    "81c06db4cb77e411df2213ce8f7cf95b"
)

EXPECTED_OLD_CLOSURE_SHA = (
    "60f56eb7978c37ca5a5792e04b2ced34"
    "64cf1857841f12640fa242470d84f732"
)

TARGET = 10
WINDOW_MIN_HOURS = 8
WINDOW_MAX_HOURS = 32

GAMMA = "https://gamma-api.polymarket.com"

UA = {
    "User-Agent":
        "quant-lab-h5-m1r1-stage1-registration/1.0"
}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def sha256_file(path: Path) -> str:
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def git_head() -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO,
        text=True,
    ).strip()


def committed_self_matches_head() -> bool:
    path = Path(__file__).resolve()
    rel = path.relative_to(REPO).as_posix()

    try:
        committed = subprocess.check_output(
            ["git", "show", f"HEAD:{rel}"],
            cwd=REPO,
        )
    except subprocess.CalledProcessError:
        return False

    return (
        hashlib.sha256(committed).hexdigest()
        == sha256_file(path)
    )


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(
        path.read_text(encoding="utf-8")
    )

    if not isinstance(value, dict):
        raise RuntimeError(
            f"expected JSON object: {path}"
        )

    return value


def parse_dt(value: Any) -> datetime | None:
    if not value:
        return None

    try:
        dt = datetime.fromisoformat(
            str(value).replace(
                "Z",
                "+00:00",
            )
        )
    except ValueError:
        return None

    if dt.tzinfo is None:
        return None

    return dt.astimezone(
        timezone.utc
    )


def jloads_maybe(value: Any):
    if value is None:
        return None

    if isinstance(value, (list, dict)):
        return value

    try:
        return json.loads(value)
    except (
        json.JSONDecodeError,
        TypeError,
    ):
        return None


def token_list(
    market: dict[str, Any],
) -> list[str]:
    value = jloads_maybe(
        market.get("clobTokenIds")
    )

    if not isinstance(value, list):
        return []

    return [
        str(x)
        for x in value
    ]


def outcome_list(
    market: dict[str, Any],
) -> list[str]:
    value = jloads_maybe(
        market.get("outcomes")
    )

    if not isinstance(value, list):
        return []

    return [
        str(x)
        for x in value
    ]


def verify_file(
    path: Path,
    expected_sha: str,
) -> None:
    if not path.is_file():
        raise RuntimeError(
            f"missing frozen input: {path}"
        )

    actual = sha256_file(path)

    if actual != expected_sha:
        raise RuntimeError(
            "frozen input SHA mismatch:\n"
            f"path={path}\n"
            f"expected={expected_sha}\n"
            f"actual={actual}"
        )


def registry_condition_ids(
    path: Path,
) -> set[str]:
    output = []

    with path.open(
        "r",
        encoding="utf-8",
    ) as fh:
        for lineno, line in enumerate(
            fh,
            start=1,
        ):
            if not line.strip():
                continue

            row = json.loads(line)

            cid = str(
                row.get("condition_id")
                or ""
            ).strip().lower()

            if not cid:
                raise RuntimeError(
                    "missing condition_id "
                    f"at {path}:{lineno}"
                )

            output.append(cid)

    return set(output)


def load_frozen_inputs():
    frozen = {
        PREREG:
            EXPECTED_PREREG_SHA,
        ADDENDUM:
            EXPECTED_ADDENDUM_SHA,
        M0:
            EXPECTED_M0_SHA,
        EXCLUSIONS:
            EXPECTED_EXCLUSIONS_SHA,
        M1A:
            EXPECTED_M1A_SHA,
        H2_IDS:
            EXPECTED_H2_SHA,
        OLD_REGISTRY:
            EXPECTED_OLD_REGISTRY_SHA,
        OLD_CLOSURE:
            EXPECTED_OLD_CLOSURE_SHA,
    }

    for path, expected in frozen.items():
        verify_file(
            path,
            expected,
        )

    prereg = read_json(PREREG)

    if (
        prereg.get("status")
        !=
        "PROSPECTIVE_ENGINEERING_RERUN_PREREGISTRATION"
    ):
        raise RuntimeError(
            "unexpected R1 preregistration status"
        )

    stage1 = prereg[
        "stage1_registration"
    ]

    if stage1["registry_target"] != TARGET:
        raise RuntimeError(
            "R2 target mismatch"
        )

    window = stage1[
        "eligible_game_start_window"
    ]

    if (
        window[
            "minimum_hours_after_selection_anchor"
        ]
        != WINDOW_MIN_HOURS
        or
        window[
            "maximum_hours_after_selection_anchor"
        ]
        != WINDOW_MAX_HOURS
        or
        window[
            "lower_bound_inclusive"
        ]
        is not True
        or
        window[
            "upper_bound_inclusive"
        ]
        is not False
    ):
        raise RuntimeError(
            "R2 time-window mismatch"
        )

    if (
        stage1[
            "external_provider_calls_before_stage1_freeze"
        ]
        is not False
        or
        stage1[
            "odds_read_before_stage1_freeze"
        ]
        is not False
        or
        stage1[
            "provider_identity_read_before_stage1_freeze"
        ]
        is not False
    ):
        raise RuntimeError(
            "R2 Stage-1 provider boundary mismatch"
        )

    freeze_gate = stage1[
        "freeze_gate"
    ]

    if (
        freeze_gate[
            "minimum_eligible_conditions"
        ]
        != TARGET
        or
        freeze_gate[
            "exactly_10_required"
        ]
        is not False
        or
        stage1[
            "truncate_if_more_than_10"
        ]
        is not True
        or
        stage1[
            "real_gamma_preview_before_freeze"
        ]
        is not False
        or
        stage1[
            "repeated_preview_until_pass"
        ]
        is not False
    ):
        raise RuntimeError(
            "R2 deterministic selection gate mismatch"
        )

    addendum = read_json(
        ADDENDUM
    )

    if (
        addendum.get("status")
        !=
        "PREREGISTERED_IDENTITY_EXCLUSION_IMPLEMENTATION_ADDENDUM"
    ):
        raise RuntimeError(
            "identity addendum status mismatch"
        )

    if (
        addendum[
            "r1_preregistration"
        ]["sha256"]
        != EXPECTED_R1_PREREG_SHA
    ):
        raise RuntimeError(
            "addendum/R1-prereg binding mismatch"
        )

    closure = read_json(
        OLD_CLOSURE
    )

    if (
        closure.get("status")
        !=
        "CLOSED_INCONCLUSIVE_PROSPECTIVE_ACQUISITION_NOT_STARTED"
    ):
        raise RuntimeError(
            "old H5 closure status mismatch"
        )

    if (
        closure.get(
            "scientific_verdict"
        )
        != "INCONCLUSIVE"
    ):
        raise RuntimeError(
            "old H5 verdict mismatch"
        )

    if (
        closure[
            "stage2_final_state"
        ][
            "acquisition_market_count"
        ]
        != 0
    ):
        raise RuntimeError(
            "old H5 acquisition count mismatch"
        )

    exclusions = read_json(
        EXCLUSIONS
    )

    f19 = {
        str(x).strip().lower()
        for x
        in exclusions[
            "rn1_f19_prospective"
        ]["condition_ids"]
        if str(x).strip()
    }

    if len(f19) != 100:
        raise RuntimeError(
            f"expected 100 F19 exclusions; "
            f"got {len(f19)}"
        )

    h2_payload = read_json(
        H2_IDS
    )

    if (
        h2_payload.get("artifact")
        !=
        "H2_V2_BURNED_IDENTITY_EXCLUSIONS"
    ):
        raise RuntimeError(
            "unexpected H2 identity artifact"
        )

    h2 = {
        str(x).strip().lower()
        for x
        in h2_payload[
            "condition_ids"
        ]
        if str(x).strip()
    }

    if len(h2) != 9:
        raise RuntimeError(
            f"expected 9 H2 exclusions; "
            f"got {len(h2)}"
        )

    for key in (
        "external_odds_read",
        "h2_edge_read",
        "h2_outcome_read",
        "h2_performance_read",
        "h2_prices_read",
        "h5_response_read",
        "pnl_calculated",
    ):
        if (
            h2_payload[
                "analysis_boundary"
            ].get(key)
            is not False
        ):
            raise RuntimeError(
                "H2 identity artifact "
                f"boundary violation: {key}"
            )

    old_h5 = registry_condition_ids(
        OLD_REGISTRY
    )

    if len(old_h5) != 10:
        raise RuntimeError(
            f"expected 10 old H5 conditions; "
            f"got {len(old_h5)}"
        )

    if f19 & h2:
        raise RuntimeError(
            "F19/H2 exclusion overlap"
        )

    if f19 & old_h5:
        raise RuntimeError(
            "F19/old-H5 exclusion overlap"
        )

    if h2 & old_h5:
        raise RuntimeError(
            "H2/old-H5 exclusion overlap"
        )

    return {
        "f19": f19,
        "h2": h2,
        "original_h5": old_h5,
        "all": (
            f19
            | h2
            | old_h5
        ),
    }


def mlb_tag_id(
    client: httpx.Client,
) -> str:
    response = client.get(
        f"{GAMMA}/sports"
    )

    response.raise_for_status()

    payload = response.json()

    matches = [
        row
        for row in payload
        if str(
            row.get("sport")
            or ""
        ).strip().lower()
        == "mlb"
    ]

    if len(matches) != 1:
        raise RuntimeError(
            "expected exactly one MLB "
            "sports metadata record"
        )

    tag = matches[
        0
    ].get(
        "primaryTagId"
    )

    if tag is None:
        raise RuntimeError(
            "MLB metadata missing primaryTagId"
        )

    return str(tag)


def fetch_gamma_events(
    client: httpx.Client,
    tag_id: str,
) -> list[dict[str, Any]]:
    output = []
    after_cursor = None
    seen = set()

    for _ in range(100):
        params = {
            "tag_id":
                int(tag_id),
            "closed":
                "false",
            "limit":
                500,
        }

        if after_cursor:
            params[
                "after_cursor"
            ] = after_cursor

        response = client.get(
            f"{GAMMA}/events/keyset",
            params=params,
        )

        response.raise_for_status()

        payload = response.json()

        if not isinstance(
            payload,
            dict,
        ):
            raise RuntimeError(
                "Gamma response not an object"
            )

        batch = payload.get(
            "events"
        )

        if not isinstance(
            batch,
            list,
        ):
            raise RuntimeError(
                "Gamma response missing events"
            )

        output.extend(batch)

        cursor = payload.get(
            "next_cursor"
        )

        if not cursor:
            return output

        cursor = str(cursor)

        if cursor in seen:
            raise RuntimeError(
                "Gamma cursor repeated"
            )

        seen.add(cursor)
        after_cursor = cursor

    raise RuntimeError(
        "Gamma pagination exceeded 100 pages"
    )


def eligible_candidate(
    market: dict[str, Any],
    *,
    anchor: datetime,
    excluded_conditions: set[str],
):
    if (
        market.get(
            "sportsMarketType"
        )
        != "moneyline"
    ):
        return None

    if market.get("active") is not True:
        return None

    if market.get("closed") is not False:
        return None

    if (
        market.get(
            "acceptingOrders"
        )
        is not True
    ):
        return None

    cid = str(
        market.get(
            "conditionId"
        )
        or ""
    ).strip().lower()

    if not cid:
        return None

    if cid in excluded_conditions:
        return None

    tokens = token_list(
        market
    )

    outcomes = outcome_list(
        market
    )

    if (
        len(tokens) != 2
        or
        len(outcomes) != 2
    ):
        return None

    start = parse_dt(
        market.get(
            "gameStartTime"
        )
    )

    if start is None:
        return None

    lower = (
        anchor
        + timedelta(
            hours=WINDOW_MIN_HOURS
        )
    )

    upper = (
        anchor
        + timedelta(
            hours=WINDOW_MAX_HOURS
        )
    )

    if start < lower:
        return None

    if start >= upper:
        return None

    market_id = str(
        market.get("id")
        or ""
    ).strip()

    if not market_id:
        return None

    return {
        "condition_id":
            cid,

        "gamma_market_id":
            market_id,

        "gamma_slug":
            (
                str(
                    market.get("slug")
                )
                if market.get("slug")
                else None
            ),

        "gamma_question":
            str(
                market.get("question")
                or ""
            ),

        "pm_game_start_time":
            start,

        "registration_lead_seconds":
            int(
                (
                    start
                    - anchor
                ).total_seconds()
            ),

        "p1_team":
            outcomes[0],

        "p0_team":
            outcomes[1],

        "p1_token_id":
            tokens[0],

        "p0_token_id":
            tokens[1],
    }


def select_candidates(
    events: list[dict[str, Any]],
    *,
    anchor: datetime,
    excluded_conditions: set[str],
):
    candidates = {}

    for event in events:
        if not isinstance(
            event,
            dict,
        ):
            continue

        markets = (
            event.get("markets")
            or []
        )

        if not isinstance(
            markets,
            list,
        ):
            continue

        for market in markets:
            if not isinstance(
                market,
                dict,
            ):
                continue

            row = eligible_candidate(
                market,
                anchor=anchor,
                excluded_conditions=(
                    excluded_conditions
                ),
            )

            if row is None:
                continue

            cid = row[
                "condition_id"
            ]

            if cid in candidates:
                if (
                    candidates[cid]
                    != row
                ):
                    raise RuntimeError(
                        "conflicting duplicate "
                        f"Gamma condition: {cid}"
                    )

                continue

            candidates[cid] = row

    return sorted(
        candidates.values(),
        key=lambda row: (
            row[
                "pm_game_start_time"
            ],
            row[
                "condition_id"
            ],
        ),
    )


def validate_freeze_gate(
    eligible,
) -> None:
    count = len(eligible)

    if count < TARGET:
        raise RuntimeError(
            "H5-M1R2 freeze requires at least "
            f"{TARGET} eligible conditions "
            "inside [anchor+8h, anchor+32h); "
            f"observed {count}. "
            "No writes are permitted."
        )


def select_for_freeze(
    eligible,
):
    validate_freeze_gate(
        eligible
    )

    # Prospective R2 rule:
    # deterministic first 10 after the
    # fully enumerated/sorted eligible set.
    return list(
        eligible[:TARGET]
    )


def market_projection(
    market: dict[str, Any],
) -> dict[str, Any]:
    """
    Explicit metadata-only projection.
    No prices, probabilities, volumes,
    liquidity, winner or settlement data.
    """
    return {
        "id":
            (
                str(market["id"])
                if market.get("id")
                is not None
                else None
            ),

        "conditionId":
            (
                str(
                    market[
                        "conditionId"
                    ]
                )
                if market.get(
                    "conditionId"
                )
                is not None
                else None
            ),

        "slug":
            (
                str(market["slug"])
                if market.get("slug")
                is not None
                else None
            ),

        "question":
            (
                str(
                    market["question"]
                )
                if market.get("question")
                is not None
                else None
            ),

        "gameStartTime":
            (
                str(
                    market[
                        "gameStartTime"
                    ]
                )
                if market.get(
                    "gameStartTime"
                )
                is not None
                else None
            ),

        "sportsMarketType":
            (
                str(
                    market[
                        "sportsMarketType"
                    ]
                )
                if market.get(
                    "sportsMarketType"
                )
                is not None
                else None
            ),

        "active":
            market.get("active"),

        "closed":
            market.get("closed"),

        "acceptingOrders":
            market.get(
                "acceptingOrders"
            ),

        "clobTokenIds":
            token_list(market),

        "outcomes":
            outcome_list(market),
    }


def gamma_selection_projection(
    events: list[dict[str, Any]],
):
    rows = []

    for event in events:
        if not isinstance(
            event,
            dict,
        ):
            continue

        markets = (
            event.get("markets")
            or []
        )

        if not isinstance(
            markets,
            list,
        ):
            continue

        for market in markets:
            if isinstance(
                market,
                dict,
            ):
                rows.append(
                    market_projection(
                        market
                    )
                )

    rows.sort(
        key=lambda row: (
            str(
                row.get(
                    "gameStartTime"
                )
                or ""
            ),
            str(
                row.get(
                    "conditionId"
                )
                or ""
            ).lower(),
            str(
                row.get("id")
                or ""
            ),
        )
    )

    return rows


def projection_bytes(
    projection,
) -> bytes:
    return (
        json.dumps(
            projection,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        + "\n"
    ).encode("utf-8")


def canonical_rows(
    selected,
    *,
    anchor: datetime,
    registrar_sha: str,
    gamma_tag_id: str,
    projection_sha: str,
    projection_market_count: int,
):
    lower = (
        anchor
        + timedelta(
            hours=WINDOW_MIN_HOURS
        )
    )

    upper = (
        anchor
        + timedelta(
            hours=WINDOW_MAX_HOURS
        )
    )

    rows = []

    for index, market in enumerate(
        selected,
        start=1,
    ):
        rows.append(
            {
                "study":
                    "H5",

                "milestone":
                    "H5-M1R2",

                "registration_stage":
                    "STAGE1_CONDITION_FREEZE",

                "registration_index":
                    index,

                "selection_anchor_utc":
                    anchor.isoformat(),

                "selection_window_lower_utc":
                    lower.isoformat(),

                "selection_window_upper_utc":
                    upper.isoformat(),

                "selection_window_lower_inclusive":
                    True,

                "selection_window_upper_inclusive":
                    False,

                "condition_id":
                    market[
                        "condition_id"
                    ],

                "gamma_market_id":
                    market[
                        "gamma_market_id"
                    ],

                "gamma_slug":
                    market[
                        "gamma_slug"
                    ],

                "gamma_question":
                    market[
                        "gamma_question"
                    ],

                "pm_game_start_time":
                    market[
                        "pm_game_start_time"
                    ].isoformat(),

                "registration_lead_seconds":
                    market[
                        "registration_lead_seconds"
                    ],

                "p1_team":
                    market["p1_team"],

                "p0_team":
                    market["p0_team"],

                "p1_token_id":
                    market[
                        "p1_token_id"
                    ],

                "p0_token_id":
                    market[
                        "p0_token_id"
                    ],

                "sports_market_type":
                    "moneyline",

                "selection_basis":
                    (
                        "gamma_metadata_only_exact_"
                        "8h_32h_window_sorted_"
                        "game_start_then_condition"
                    ),

                "external_identity_status":
                    "PENDING_PROVIDER_VISIBILITY",

                "odds_game_id":
                    None,

                "gamma_mlb_tag_id":
                    gamma_tag_id,

                "gamma_selection_projection_sha256":
                    projection_sha,

                "gamma_projection_market_count":
                    projection_market_count,

                "r2_preregistration_sha256":
                    EXPECTED_PREREG_SHA,

                "identity_exclusion_addendum_sha256":
                    EXPECTED_ADDENDUM_SHA,

                "h2_identity_exclusions_sha256":
                    EXPECTED_H2_SHA,

                "original_h5_m1_registry_sha256":
                    EXPECTED_OLD_REGISTRY_SHA,

                "registrar_sha256":
                    registrar_sha,
            }
        )

    return rows


def serialize_rows(
    rows,
) -> bytes:
    return "".join(
        json.dumps(
            row,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        + "\n"
        for row in rows
    ).encode("utf-8")


def create_exclusive(
    path: Path,
    data: bytes,
):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    fd = os.open(
        path,
        (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
        ),
        0o644,
    )

    with os.fdopen(
        fd,
        "wb",
    ) as fh:
        fh.write(data)
        fh.flush()
        os.fsync(
            fh.fileno()
        )


def print_candidates(
    eligible,
    *,
    anchor: datetime,
):
    lower = (
        anchor
        + timedelta(
            hours=WINDOW_MIN_HOURS
        )
    )

    upper = (
        anchor
        + timedelta(
            hours=WINDOW_MAX_HOURS
        )
    )

    print("=" * 72)
    print("H5-M1R2 STAGE-1 REGISTRATION")
    print("GAMMA METADATA ONLY")
    print("=" * 72)

    print(
        "selection_anchor_utc:",
        anchor.isoformat(),
    )

    print(
        "window_lower_utc:",
        lower.isoformat(),
    )

    print(
        "window_upper_utc:",
        upper.isoformat(),
    )

    print(
        "eligible conditions:",
        len(eligible),
    )

    print(
        "minimum eligible required:",
        TARGET,
    )

    print(
        "freeze_gate:",
        (
            "PASS"
            if len(eligible) >= TARGET
            else "FAIL"
        ),
    )

    print()

    for index, row in enumerate(
        eligible,
        start=1,
    ):
        print(
            index,
            "|",
            row[
                "pm_game_start_time"
            ].isoformat(),
            "|",
            row[
                "gamma_slug"
            ],
            "|",
            row[
                "condition_id"
            ],
        )

    print()
    print("The Odds API called: NO")
    print("odds-bearing endpoint used: NO")
    print("external provider identity read: NO")
    print("external consensus calculated: NO")
    print("Polymarket price fields inspected: NO")
    print("F19 trade content read: NO")
    print("H2 performance read: NO")
    print("H6 response read: NO")
    print("PM response calculated: NO")
    print("PnL calculated: NO")


def validate_config():
    sets = load_frozen_inputs()

    print(
        "H5-M1R2 REGISTRAR CONFIG: PASS"
    )

    print(
        "R2 preregistration SHA:",
        EXPECTED_PREREG_SHA,
    )

    print(
        "identity addendum SHA:",
        EXPECTED_ADDENDUM_SHA,
    )

    print(
        "target:",
        TARGET,
    )

    print(
        "window:",
        "[anchor+8h, anchor+32h)",
    )

    print(
        "F19 exclusions:",
        len(sets["f19"]),
    )

    print(
        "H2 exclusions:",
        len(sets["h2"]),
    )

    print(
        "original H5 exclusions:",
        len(
            sets[
                "original_h5"
            ]
        ),
    )

    print(
        "combined exclusions:",
        len(sets["all"]),
    )

    print("Gamma called: NO")
    print("The Odds API called: NO")
    print("registry written: NO")
    print("Stage2 map written: NO")
    print("response calculated: NO")
    print("PnL calculated: NO")


def main():
    parser = argparse.ArgumentParser()

    modes = (
        parser.add_mutually_exclusive_group(
            required=True
        )
    )

    modes.add_argument(
        "--validate-config",
        action="store_true",
    )

    modes.add_argument(
        "--freeze",
        action="store_true",
    )

    args = parser.parse_args()

    if args.validate_config:
        validate_config()
        return

    if REGISTRY.exists():
        raise RuntimeError(
            "R2 registry already exists; "
            "registrar MUST NEVER be rerun"
        )

    if RECEIPT.exists():
        raise RuntimeError(
            "R2 receipt already exists; "
            "registrar MUST NEVER be rerun"
        )

    sets = load_frozen_inputs()

    if not committed_self_matches_head():
        raise RuntimeError(
            "refusing real Gamma access: "
            "registrar is not byte-identical "
            "to committed HEAD"
        )

    registrar_sha = sha256_file(
        Path(__file__).resolve()
    )

    # The sole real Gamma invocation is the
    # irreversible --freeze run. This timestamp
    # is therefore the authoritative R2 anchor.
    anchor = utcnow()

    with httpx.Client(
        headers=UA,
        timeout=30,
    ) as client:
        tag_id = mlb_tag_id(
            client
        )

        events = fetch_gamma_events(
            client,
            tag_id,
        )

    projection = (
        gamma_selection_projection(
            events
        )
    )

    projection_sha = (
        sha256_bytes(
            projection_bytes(
                projection
            )
        )
    )

    eligible = select_candidates(
        events,
        anchor=anchor,
        excluded_conditions=(
            sets["all"]
        ),
    )

    print_candidates(
        eligible,
        anchor=anchor,
    )

    print(
        "registrar_sha256:",
        registrar_sha,
    )

    print(
        "gamma_selection_projection_sha256:",
        projection_sha,
    )

    print(
        "gamma_projection_market_count:",
        len(projection),
    )

    selected = select_for_freeze(
        eligible
    )

    rows = canonical_rows(
        selected,
        anchor=anchor,
        registrar_sha=registrar_sha,
        gamma_tag_id=tag_id,
        projection_sha=projection_sha,
        projection_market_count=(
            len(projection)
        ),
    )

    if len(rows) != TARGET:
        raise RuntimeError(
            "internal row-count invariant failed"
        )

    condition_ids = [
        row["condition_id"]
        for row in rows
    ]

    if (
        len(set(condition_ids))
        != TARGET
    ):
        raise RuntimeError(
            "duplicate condition_id "
            "in R1 registry"
        )

    if (
        set(condition_ids)
        & sets["all"]
    ):
        raise RuntimeError(
            "excluded condition entered "
            "R1 registry"
        )

    registry_bytes = serialize_rows(
        rows
    )

    registry_sha = sha256_bytes(
        registry_bytes
    )

    receipt = {
        "study":
            "H5",

        "milestone":
            "H5-M1R2",

        "status":
            "STAGE1_CONDITION_REGISTRY_FROZEN",

        "frozen_at_utc":
            anchor.isoformat(),

        "registry_path":
            str(
                REGISTRY.relative_to(
                    REPO
                )
            ),

        "registry_sha256":
            registry_sha,

        "row_count":
            len(rows),

        "distinct_condition_ids":
            len(
                set(condition_ids)
            ),

        "condition_ids":
            condition_ids,

        "selection_window": {
            "minimum_hours_after_anchor":
                WINDOW_MIN_HOURS,

            "maximum_hours_after_anchor":
                WINDOW_MAX_HOURS,

            "lower_inclusive":
                True,

            "upper_inclusive":
                False,
        },

        "freeze_gate": {
            "minimum_eligible_conditions":
                TARGET,

            "observed_eligible_count":
                len(eligible),

            "selected_count":
                len(selected),

            "passed":
                len(eligible) >= TARGET,

            "truncation_used":
                len(eligible) > TARGET,

            "truncation_rule":
                (
                    "deterministic_first_10_by_"
                    "game_start_then_condition_id"
                ),

            "replacement_used":
                False,
        },

        "registrar_path":
            "research/h5_m1r1_register.py",

        "registrar_sha256":
            registrar_sha,

        "git_head":
            git_head(),

        "r2_preregistration_sha256":
            EXPECTED_PREREG_SHA,

        "identity_exclusion_addendum_sha256":
            EXPECTED_ADDENDUM_SHA,

        "m0_sha256":
            EXPECTED_M0_SHA,

        "m0_exclusions_sha256":
            EXPECTED_EXCLUSIONS_SHA,

        "m1a_sha256":
            EXPECTED_M1A_SHA,

        "h2_identity_exclusions_sha256":
            EXPECTED_H2_SHA,

        "original_h5_m1_registry_sha256":
            EXPECTED_OLD_REGISTRY_SHA,

        "original_h5_m1_closure_sha256":
            EXPECTED_OLD_CLOSURE_SHA,

        "gamma_metadata": {
            "mlb_tag_id":
                tag_id,

            "selection_projection_sha256":
                projection_sha,

            "selection_projection_market_count":
                len(projection),

            "projection_contains_prices":
                False,

            "projection_contains_volume":
                False,

            "projection_contains_liquidity":
                False,
        },

        "exclusion_counts": {
            "f19":
                len(sets["f19"]),

            "h2_burned":
                len(sets["h2"]),

            "original_h5_m1":
                len(
                    sets[
                        "original_h5"
                    ]
                ),

            "union":
                len(sets["all"]),
        },

        "external_identity_status":
            "PENDING_PROVIDER_VISIBILITY",

        "registry_mutable":
            False,

        "network_boundary": {
            "gamma_metadata_used":
                True,

            "odds_api_called":
                False,

            "odds_bearing_endpoint_used":
                False,

            "external_provider_identity_read":
                False,

            "polymarket_price_fields_inspected":
                False,
        },

        "analysis_boundary": {
            "external_odds_read":
                False,

            "external_consensus_calculated":
                False,

            "external_state_difference_calculated":
                False,

            "pm_response_calculated":
                False,

            "correlation_calculated":
                False,

            "regression_calculated":
                False,

            "f19_trade_content_read":
                False,

            "f20_read":
                False,

            "h2_performance_read":
                False,

            "h6_response_read":
                False,

            "markout_calculated":
                False,

            "edge_calculated":
                False,

            "winner_used":
                False,

            "settlement_used":
                False,

            "pnl_calculated":
                False,
        },
    }

    receipt_bytes = (
        json.dumps(
            receipt,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")

    create_exclusive(
        REGISTRY,
        registry_bytes,
    )

    create_exclusive(
        RECEIPT,
        receipt_bytes,
    )

    print()
    print(
        "H5-M1R2 STAGE-1 REGISTRY FROZEN"
    )

    print(
        "registry_sha256:",
        registry_sha,
    )

    print(
        "registry rows:",
        len(rows),
    )

    print(
        "IMPORTANT: --freeze MUST NEVER "
        "BE RUN AGAIN"
    )


if __name__ == "__main__":
    main()
