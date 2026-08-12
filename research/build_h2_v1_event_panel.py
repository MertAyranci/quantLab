"""Freeze the unified H2-v1 event panel.

Parents
-------
research/h2_v1_cohort.csv
research/h2_v1_rn1_pregame_tape.csv
research/h2_v1_sharp_tape.csv
research/h2_v1_pm_responses.csv

Outputs
-------
research/h2_v1_event_panel.csv
research/h2_v1_event_panel_manifest.json

Unit of observation
-------------------
One row per information event:
    213 RN1 fills
    548 sharp exchange polls
    = 761 rows

M4's four PM response horizons are pivoted to columns.

Important policies
------------------
* HOME is the common directional axis.
* RN1 fills remain individual rows, but same-second clustering metadata is
  preserved for later clustered/aggregated inference.
* Sharp innovation = current consensus - previous sharp consensus within the
  same market.
* The first sharp poll in each market has no innovation.
* Sharp consensus composition changes are flagged, never silently discarded.
* Cross-clock nearest events use strict temporal ordering.
* PM freshness and response missingness from M4 are preserved exactly.
* No statistical sample selection occurs in M5.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal, getcontext
from pathlib import Path


getcontext().prec = 40

REPO = Path(__file__).resolve().parents[1]

COHORT_CSV = REPO / "research/h2_v1_cohort.csv"
COHORT_MANIFEST = REPO / "research/h2_v1_manifest.json"

RN1_CSV = REPO / "research/h2_v1_rn1_pregame_tape.csv"
RN1_MANIFEST = REPO / "research/h2_v1_rn1_manifest.json"

SHARP_CSV = REPO / "research/h2_v1_sharp_tape.csv"
SHARP_MANIFEST = REPO / "research/h2_v1_sharp_manifest.json"

PM_CSV = REPO / "research/h2_v1_pm_responses.csv"
PM_MANIFEST = REPO / "research/h2_v1_pm_manifest.json"

PANEL_CSV = REPO / "research/h2_v1_event_panel.csv"
MANIFEST_JSON = REPO / "research/h2_v1_event_panel_manifest.json"

FEATURE_VERSION = "H2-v1-EVENT-M5"
COHORT_VERSION = "H2-v1"

EXPECTED_MARKETS = 42
EXPECTED_RN1 = 213
EXPECTED_SHARP = 548
EXPECTED_SIGNALS = 761
EXPECTED_M4_ROWS = 3044

HORIZONS = [10, 30, 60, 300]

EXPECTED_SHARP_INNOVATION = {
    "first_no_delta": 42,
    "delta_available": 506,
    "home": 174,
    "away": 137,
    "zero": 195,
    "composition_changed": 60,
}

EXPECTED_RN1_CLUSTERING = {
    "same_second_clusters": 37,
    "rows_in_same_second_clusters": 83,
    "largest_same_second_cluster": 4,
    "same_tx_clusters": 1,
    "rows_in_same_tx_clusters": 2,
    "largest_same_tx_cluster": 2,
}

EXPECTED_CROSS_AVAILABILITY = {
    "rn1_prev_sharp": 171,
    "rn1_next_sharp": 117,
    "sharp_prev_rn1": 82,
    "sharp_next_rn1": 327,
}

EXPECTED_M6_FEASIBILITY = {
    "rn1": {
        10: (134, 108),
        30: (145, 117),
        60: (103, 41),
        300: (62, 33),
    },
    "sharp": {
        10: (20, 10),
        30: (48, 23),
        60: (68, 20),
        300: (73, 18),
    },
}


BASE_FIELDS = [
    "feature_version",
    "cohort_version",

    "signal_id",
    "signal_type",
    "signal_parent_row",

    "market_id",
    "condition_id",
    "slug",
    "home_team",
    "away_team",
    "canonical_commence_time",

    "signal_time",
    "signal_seconds_before_start",

    "signal_direction_available",
    "signal_direction_home_sign",

    # RN1 native information.
    "rn1_token_id",
    "rn1_token_outcome",
    "rn1_token_team_side",
    "rn1_side",
    "rn1_token_price",
    "rn1_execution_home_price",
    "rn1_size_shares",
    "rn1_cash_notional",
    "rn1_signed_home_shares",
    "rn1_signed_home_cash_flow",
    "rn1_tx_hash",
    "rn1_venue_trade_key",

    # RN1 clustering.
    "rn1_second_cluster_id",
    "rn1_second_cluster_size",
    "rn1_second_cluster_rank",
    "rn1_second_cluster_signed_home_shares",
    "rn1_second_cluster_signed_home_cash_flow",
    "rn1_second_cluster_net_direction_sign",
    "rn1_tx_cluster_size",

    # Sharp native information.
    "sharp_collector_run_id",
    "sharp_home_prob",
    "sharp_prev_home_prob",
    "sharp_home_delta",
    "sharp_direction_home_sign",

    "sharp_prev_poll_time",
    "sharp_prev_poll_gap_seconds",

    "sharp_family_count",
    "sharp_is_multi_family",
    "sharp_family_composition",
    "sharp_prev_family_composition",
    "sharp_family_composition_changed",

    "sharp_betfair_home_prob",
    "sharp_matchbook_home_prob",
    "sharp_smarkets_home_prob",

    "sharp_median_source_lag_seconds",
    "sharp_max_source_lag_seconds",

    # Latest sharp state observable at signal.
    "aligned_sharp_signal_id",
    "aligned_sharp_time",
    "aligned_sharp_age_seconds",
    "aligned_sharp_home_prob",
    "aligned_sharp_home_delta",
    "aligned_sharp_direction_home_sign",
    "aligned_sharp_family_count",
    "aligned_sharp_composition_changed",

    # Cross-clock nearest neighbors.
    "prev_sharp_signal_id",
    "prev_sharp_lag_seconds",
    "next_sharp_signal_id",
    "next_sharp_lag_seconds",

    "prev_rn1_signal_id",
    "prev_rn1_lag_seconds",
    "next_rn1_signal_id",
    "next_rn1_lag_seconds",

    # PM baseline shared across horizons.
    "pm_baseline_available",
    "pm_baseline_time",
    "pm_baseline_age_seconds",
    "pm_baseline_home_price",
    "pm_baseline_tx_hash",

    "pm_baseline_fresh_30s",
    "pm_baseline_fresh_60s",
    "pm_baseline_fresh_300s",
    "pm_baseline_fresh_600s",

    # Cross-market state features.
    "sharp_pm_gap_at_signal",
    "rn1_execution_vs_pm_baseline_change",
    "rn1_agrees_with_aligned_sharp_direction",
]


HORIZON_SUFFIX_FIELDS = [
    "pregame_eligible",
    "response_available",
    "response_trade_count",

    "first_response_time",
    "first_response_latency_seconds",
    "first_response_home_price",
    "first_response_change",
    "first_response_tx_hash",

    "last_response_time",
    "last_response_latency_seconds",
    "last_response_target_age_seconds",
    "last_response_home_price",
    "last_response_change",
    "last_response_tx_hash",

    "response_min_home_price",
    "response_max_home_price",
    "response_home_price_range",

    "last_response_target_fresh_5s",
    "last_response_target_fresh_15s",
    "last_response_target_fresh_30s",
    "last_response_target_fresh_60s",
]


FIELDS = list(BASE_FIELDS)

for horizon in HORIZONS:
    for suffix in HORIZON_SUFFIX_FIELDS:
        FIELDS.append(
            f"pm_{horizon}s_{suffix}"
        )


def read_csv(path):
    with path.open(
        newline="",
        encoding="utf-8",
    ) as f:
        return list(
            csv.DictReader(f)
        )


def sha256_file(path):
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def dt(value):
    return datetime.fromisoformat(
        str(value).replace(
            "Z",
            "+00:00",
        )
    )


def iso(value):
    if value is None:
        return ""

    return value.astimezone(
        timezone.utc
    ).isoformat()


def btxt(value):
    return (
        "true"
        if bool(value)
        else "false"
    )


def dstr(value):
    if value is None:
        return ""

    if not isinstance(
        value,
        Decimal,
    ):
        value = Decimal(
            str(value)
        )

    if value == 0:
        return "0"

    text = format(
        value.normalize(),
        "f",
    )

    if "." in text:
        text = (
            text.rstrip("0")
            .rstrip(".")
        )

    return text


def sign(value):
    if value is None:
        return None

    if value > 0:
        return 1

    if value < 0:
        return -1

    return 0


def family_composition(row):
    families = []

    if row[
        "betfair_home_prob"
    ] != "":
        families.append(
            "betfair"
        )

    if row[
        "matchbook_home_prob"
    ] != "":
        families.append(
            "matchbook"
        )

    if row[
        "smarkets_home_prob"
    ] != "":
        families.append(
            "smarkets"
        )

    return "+".join(
        families
    )


def load_parents():
    cohort = read_csv(
        COHORT_CSV
    )

    rn1 = read_csv(
        RN1_CSV
    )

    sharp = read_csv(
        SHARP_CSV
    )

    pm = read_csv(
        PM_CSV
    )

    if len(cohort) != EXPECTED_MARKETS:
        raise RuntimeError(
            "Unexpected cohort size"
        )

    if len(rn1) != EXPECTED_RN1:
        raise RuntimeError(
            "Unexpected RN1 size"
        )

    if len(sharp) != EXPECTED_SHARP:
        raise RuntimeError(
            "Unexpected sharp size"
        )

    if len(pm) != EXPECTED_M4_ROWS:
        raise RuntimeError(
            "Unexpected M4 size"
        )

    cohort_manifest = json.loads(
        COHORT_MANIFEST.read_text()
    )

    rn1_manifest = json.loads(
        RN1_MANIFEST.read_text()
    )

    sharp_manifest = json.loads(
        SHARP_MANIFEST.read_text()
    )

    pm_manifest = json.loads(
        PM_MANIFEST.read_text()
    )

    hashes = {
        "cohort_csv":
            sha256_file(
                COHORT_CSV
            ),

        "rn1_pregame_tape":
            sha256_file(
                RN1_CSV
            ),

        "sharp_tape":
            sha256_file(
                SHARP_CSV
            ),

        "pm_responses":
            sha256_file(
                PM_CSV
            ),
    }

    if hashes[
        "cohort_csv"
    ] != cohort_manifest[
        "csv_sha256"
    ]:
        raise RuntimeError(
            "Cohort parent hash mismatch"
        )

    if hashes[
        "rn1_pregame_tape"
    ] != rn1_manifest[
        "artifacts"
    ][
        "pregame_tape"
    ][
        "sha256"
    ]:
        raise RuntimeError(
            "RN1 parent hash mismatch"
        )

    if hashes[
        "sharp_tape"
    ] != sharp_manifest[
        "artifacts"
    ][
        "sharp_tape"
    ][
        "sha256"
    ]:
        raise RuntimeError(
            "Sharp parent hash mismatch"
        )

    if hashes[
        "pm_responses"
    ] != pm_manifest[
        "artifacts"
    ][
        "pm_responses"
    ][
        "sha256"
    ]:
        raise RuntimeError(
            "PM response parent hash mismatch"
        )

    return (
        cohort,
        rn1,
        sharp,
        pm,
        hashes,
    )


def build_rn1_signals(
    rn1,
):
    signals = []
    by_market = defaultdict(
        list
    )

    second_groups = defaultdict(
        list
    )

    tx_groups = defaultdict(
        list
    )

    for parent_row, row in enumerate(
        rn1,
        1,
    ):
        mid = int(
            row["market_id"]
        )

        event_time = dt(
            row["event_time"]
        )

        token_price = Decimal(
            row["price"]
        )

        if (
            row[
                "token_team_side"
            ]
            == "home"
        ):
            home_price = (
                token_price
            )

        elif (
            row[
                "token_team_side"
            ]
            == "away"
        ):
            home_price = (
                Decimal("1")
                - token_price
            )

        else:
            raise RuntimeError(
                f"{row['slug']}: "
                "invalid RN1 team side"
            )

        x = dict(row)

        x["_signal_id"] = (
            f"rn1:{parent_row:04d}"
        )

        x["_parent_row"] = (
            parent_row
        )

        x["_time"] = event_time

        x["_direction"] = int(
            row[
                "home_direction_sign"
            ]
        )

        x["_signed_shares"] = Decimal(
            row[
                "signed_home_shares"
            ]
        )

        x["_signed_cash"] = Decimal(
            row[
                "signed_home_cash_flow"
            ]
        )

        x["_execution_home_price"] = (
            home_price
        )

        signals.append(
            x
        )

        by_market[mid].append(
            x
        )

        second_groups[
            (
                mid,
                event_time,
            )
        ].append(
            x
        )

        tx_groups[
            (
                mid,
                row[
                    "tx_hash"
                ].lower(),
            )
        ].append(
            x
        )

    for sequence in by_market.values():
        sequence.sort(
            key=lambda x: (
                x["_time"],
                x["tx_hash"],
                x["venue_trade_key"],
            )
        )

    for (
        mid,
        event_time,
    ), group in second_groups.items():

        group.sort(
            key=lambda x: (
                x["tx_hash"],
                x["venue_trade_key"],
            )
        )

        cluster_shares = sum(
            (
                x[
                    "_signed_shares"
                ]
                for x in group
            ),
            Decimal("0"),
        )

        cluster_cash = sum(
            (
                x[
                    "_signed_cash"
                ]
                for x in group
            ),
            Decimal("0"),
        )

        cluster_direction = sign(
            cluster_shares
        )

        cluster_id = (
            f"{mid}:"
            f"{iso(event_time)}"
        )

        for rank, x in enumerate(
            group,
            1,
        ):
            x[
                "_second_cluster_id"
            ] = cluster_id

            x[
                "_second_cluster_size"
            ] = len(group)

            x[
                "_second_cluster_rank"
            ] = rank

            x[
                "_second_cluster_shares"
            ] = cluster_shares

            x[
                "_second_cluster_cash"
            ] = cluster_cash

            x[
                "_second_cluster_direction"
            ] = cluster_direction

    for group in tx_groups.values():
        for x in group:
            x[
                "_tx_cluster_size"
            ] = len(group)

    return (
        signals,
        by_market,
        second_groups,
        tx_groups,
    )


def build_sharp_signals(
    sharp,
):
    signals = []
    by_market = defaultdict(
        list
    )

    for parent_row, row in enumerate(
        sharp,
        1,
    ):
        mid = int(
            row["market_id"]
        )

        x = dict(row)

        x["_signal_id"] = (
            f"sharp:{parent_row:04d}"
        )

        x["_parent_row"] = (
            parent_row
        )

        x["_time"] = dt(
            row["observed_at"]
        )

        x["_prob"] = Decimal(
            row[
                "sharp_consensus_home_prob"
            ]
        )

        x["_family"] = (
            family_composition(
                row
            )
        )

        x["_prev"] = None
        x["_delta"] = None
        x["_direction"] = None
        x["_composition_changed"] = None
        x["_prev_gap"] = None

        signals.append(
            x
        )

        by_market[mid].append(
            x
        )

    for sequence in by_market.values():
        sequence.sort(
            key=lambda x: (
                x["_time"],
                int(
                    x[
                        "collector_run_id"
                    ]
                ),
            )
        )

        for index, x in enumerate(
            sequence
        ):
            if index == 0:
                continue

            prev = sequence[
                index - 1
            ]

            delta = (
                x["_prob"]
                - prev["_prob"]
            )

            x["_prev"] = prev
            x["_delta"] = delta
            x["_direction"] = sign(
                delta
            )

            x[
                "_composition_changed"
            ] = (
                x["_family"]
                != prev["_family"]
            )

            x["_prev_gap"] = Decimal(
                str(
                    (
                        x["_time"]
                        - prev["_time"]
                    ).total_seconds()
                )
            )

    return (
        signals,
        by_market,
    )


def previous_strict(
    sequence,
    timestamp,
):
    result = None

    for row in sequence:
        if row["_time"] < timestamp:
            result = row
        else:
            break

    return result


def next_strict(
    sequence,
    timestamp,
):
    for row in sequence:
        if row["_time"] > timestamp:
            return row

    return None


def build_pm_map(
    pm,
):
    result = defaultdict(
        dict
    )

    for row in pm:
        sid = row[
            "signal_id"
        ]

        horizon = int(
            row[
                "horizon_seconds"
            ]
        )

        if horizon not in HORIZONS:
            raise RuntimeError(
                f"Unexpected M4 horizon "
                f"{horizon}"
            )

        if horizon in result[sid]:
            raise RuntimeError(
                f"Duplicate M4 response "
                f"{sid} +{horizon}s"
            )

        result[sid][
            horizon
        ] = row

    if len(result) != EXPECTED_SIGNALS:
        raise RuntimeError(
            "M4 signal count mismatch"
        )

    for sid, horizons in result.items():
        if set(
            horizons
        ) != set(
            HORIZONS
        ):
            raise RuntimeError(
                f"{sid}: incomplete horizons"
            )

        anchor = horizons[10]

        for horizon in HORIZONS[
            1:
        ]:
            other = horizons[
                horizon
            ]

            for field in (
                "baseline_available",
                "baseline_time",
                "baseline_age_seconds",
                "baseline_home_price",
                "baseline_tx_hash",
                "baseline_fresh_30s",
                "baseline_fresh_60s",
                "baseline_fresh_300s",
                "baseline_fresh_600s",
            ):
                if (
                    anchor[field]
                    != other[field]
                ):
                    raise RuntimeError(
                        f"{sid}: baseline "
                        f"changed across horizons"
                    )

    return result


def lag_seconds(
    later,
    earlier,
):
    if (
        later is None
        or earlier is None
    ):
        return None

    return Decimal(
        str(
            (
                later["_time"]
                - earlier["_time"]
            ).total_seconds()
        )
    )


def base_row(
    signal,
    signal_type,
    rn1_by_market,
    sharp_by_market,
    pm_map,
):
    sid = signal[
        "_signal_id"
    ]

    mid = int(
        signal[
            "market_id"
        ]
    )

    timestamp = signal[
        "_time"
    ]

    m4 = pm_map[
        sid
    ]

    anchor = m4[
        10
    ]

    if int(
        anchor[
            "signal_parent_row"
        ]
    ) != signal[
        "_parent_row"
    ]:
        raise RuntimeError(
            f"{sid}: M4 parent row mismatch"
        )

    if dt(
        anchor[
            "signal_time"
        ]
    ) != timestamp:
        raise RuntimeError(
            f"{sid}: M4 signal time mismatch"
        )

    row = {
        field: ""
        for field in FIELDS
    }

    row[
        "feature_version"
    ] = FEATURE_VERSION

    row[
        "cohort_version"
    ] = COHORT_VERSION

    row["signal_id"] = sid
    row[
        "signal_type"
    ] = signal_type

    row[
        "signal_parent_row"
    ] = signal[
        "_parent_row"
    ]

    for field in (
        "market_id",
        "condition_id",
        "slug",
        "home_team",
        "away_team",
        "canonical_commence_time",
    ):
        row[field] = signal[
            field
        ]

    row[
        "signal_time"
    ] = iso(
        timestamp
    )

    row[
        "signal_seconds_before_start"
    ] = anchor[
        "signal_seconds_before_start"
    ]

    # --------------------------------------------------------
    # PM baseline
    # --------------------------------------------------------

    for src, dst in (
        (
            "baseline_available",
            "pm_baseline_available",
        ),
        (
            "baseline_time",
            "pm_baseline_time",
        ),
        (
            "baseline_age_seconds",
            "pm_baseline_age_seconds",
        ),
        (
            "baseline_home_price",
            "pm_baseline_home_price",
        ),
        (
            "baseline_tx_hash",
            "pm_baseline_tx_hash",
        ),
        (
            "baseline_fresh_30s",
            "pm_baseline_fresh_30s",
        ),
        (
            "baseline_fresh_60s",
            "pm_baseline_fresh_60s",
        ),
        (
            "baseline_fresh_300s",
            "pm_baseline_fresh_300s",
        ),
        (
            "baseline_fresh_600s",
            "pm_baseline_fresh_600s",
        ),
    ):
        row[dst] = anchor[
            src
        ]

    baseline_price = (
        Decimal(
            anchor[
                "baseline_home_price"
            ]
        )
        if anchor[
            "baseline_available"
        ] == "true"
        else None
    )

    # --------------------------------------------------------
    # Cross-clock nearest signals
    # --------------------------------------------------------

    prev_sharp = previous_strict(
        sharp_by_market[
            mid
        ],
        timestamp,
    )

    next_sharp = next_strict(
        sharp_by_market[
            mid
        ],
        timestamp,
    )

    prev_rn1 = previous_strict(
        rn1_by_market[
            mid
        ],
        timestamp,
    )

    next_rn1 = next_strict(
        rn1_by_market[
            mid
        ],
        timestamp,
    )

    if prev_sharp is not None:
        row[
            "prev_sharp_signal_id"
        ] = prev_sharp[
            "_signal_id"
        ]

        row[
            "prev_sharp_lag_seconds"
        ] = dstr(
            lag_seconds(
                signal,
                prev_sharp,
            )
        )

    if next_sharp is not None:
        row[
            "next_sharp_signal_id"
        ] = next_sharp[
            "_signal_id"
        ]

        row[
            "next_sharp_lag_seconds"
        ] = dstr(
            lag_seconds(
                next_sharp,
                signal,
            )
        )

    if prev_rn1 is not None:
        row[
            "prev_rn1_signal_id"
        ] = prev_rn1[
            "_signal_id"
        ]

        row[
            "prev_rn1_lag_seconds"
        ] = dstr(
            lag_seconds(
                signal,
                prev_rn1,
            )
        )

    if next_rn1 is not None:
        row[
            "next_rn1_signal_id"
        ] = next_rn1[
            "_signal_id"
        ]

        row[
            "next_rn1_lag_seconds"
        ] = dstr(
            lag_seconds(
                next_rn1,
                signal,
            )
        )

    # --------------------------------------------------------
    # Native RN1
    # --------------------------------------------------------

    if signal_type == "rn1":
        direction = signal[
            "_direction"
        ]

        row[
            "signal_direction_available"
        ] = "true"

        row[
            "signal_direction_home_sign"
        ] = direction

        row[
            "rn1_token_id"
        ] = signal[
            "token_id"
        ]

        row[
            "rn1_token_outcome"
        ] = signal[
            "token_outcome"
        ]

        row[
            "rn1_token_team_side"
        ] = signal[
            "token_team_side"
        ]

        row[
            "rn1_side"
        ] = signal[
            "side"
        ]

        row[
            "rn1_token_price"
        ] = signal[
            "price"
        ]

        row[
            "rn1_execution_home_price"
        ] = dstr(
            signal[
                "_execution_home_price"
            ]
        )

        row[
            "rn1_size_shares"
        ] = signal[
            "size_shares"
        ]

        row[
            "rn1_cash_notional"
        ] = signal[
            "cash_notional"
        ]

        row[
            "rn1_signed_home_shares"
        ] = signal[
            "signed_home_shares"
        ]

        row[
            "rn1_signed_home_cash_flow"
        ] = signal[
            "signed_home_cash_flow"
        ]

        row[
            "rn1_tx_hash"
        ] = signal[
            "tx_hash"
        ]

        row[
            "rn1_venue_trade_key"
        ] = signal[
            "venue_trade_key"
        ]

        row[
            "rn1_second_cluster_id"
        ] = signal[
            "_second_cluster_id"
        ]

        row[
            "rn1_second_cluster_size"
        ] = signal[
            "_second_cluster_size"
        ]

        row[
            "rn1_second_cluster_rank"
        ] = signal[
            "_second_cluster_rank"
        ]

        row[
            "rn1_second_cluster_signed_home_shares"
        ] = dstr(
            signal[
                "_second_cluster_shares"
            ]
        )

        row[
            "rn1_second_cluster_signed_home_cash_flow"
        ] = dstr(
            signal[
                "_second_cluster_cash"
            ]
        )

        row[
            "rn1_second_cluster_net_direction_sign"
        ] = signal[
            "_second_cluster_direction"
        ]

        row[
            "rn1_tx_cluster_size"
        ] = signal[
            "_tx_cluster_size"
        ]

        if baseline_price is not None:
            row[
                "rn1_execution_vs_pm_baseline_change"
            ] = dstr(
                signal[
                    "_execution_home_price"
                ]
                - baseline_price
            )

        aligned_sharp = (
            prev_sharp
        )

    # --------------------------------------------------------
    # Native sharp
    # --------------------------------------------------------

    else:
        direction = signal[
            "_direction"
        ]

        row[
            "signal_direction_available"
        ] = btxt(
            signal[
                "_delta"
            ]
            is not None
        )

        row[
            "signal_direction_home_sign"
        ] = (
            ""
            if direction is None
            else direction
        )

        row[
            "sharp_collector_run_id"
        ] = signal[
            "collector_run_id"
        ]

        row[
            "sharp_home_prob"
        ] = signal[
            "sharp_consensus_home_prob"
        ]

        row[
            "sharp_family_count"
        ] = signal[
            "family_count"
        ]

        row[
            "sharp_is_multi_family"
        ] = signal[
            "is_multi_family"
        ]

        row[
            "sharp_family_composition"
        ] = signal[
            "_family"
        ]

        row[
            "sharp_betfair_home_prob"
        ] = signal[
            "betfair_home_prob"
        ]

        row[
            "sharp_matchbook_home_prob"
        ] = signal[
            "matchbook_home_prob"
        ]

        row[
            "sharp_smarkets_home_prob"
        ] = signal[
            "smarkets_home_prob"
        ]

        row[
            "sharp_median_source_lag_seconds"
        ] = signal[
            "median_source_lag_seconds"
        ]

        row[
            "sharp_max_source_lag_seconds"
        ] = signal[
            "max_source_lag_seconds"
        ]

        if signal[
            "_prev"
        ] is not None:
            prev = signal[
                "_prev"
            ]

            row[
                "sharp_prev_home_prob"
            ] = dstr(
                prev[
                    "_prob"
                ]
            )

            row[
                "sharp_home_delta"
            ] = dstr(
                signal[
                    "_delta"
                ]
            )

            row[
                "sharp_direction_home_sign"
            ] = direction

            row[
                "sharp_prev_poll_time"
            ] = iso(
                prev[
                    "_time"
                ]
            )

            row[
                "sharp_prev_poll_gap_seconds"
            ] = dstr(
                signal[
                    "_prev_gap"
                ]
            )

            row[
                "sharp_prev_family_composition"
            ] = prev[
                "_family"
            ]

            row[
                "sharp_family_composition_changed"
            ] = btxt(
                signal[
                    "_composition_changed"
                ]
            )

        aligned_sharp = signal

    # --------------------------------------------------------
    # Sharp state observable at this signal
    # --------------------------------------------------------

    if aligned_sharp is not None:
        row[
            "aligned_sharp_signal_id"
        ] = aligned_sharp[
            "_signal_id"
        ]

        row[
            "aligned_sharp_time"
        ] = iso(
            aligned_sharp[
                "_time"
            ]
        )

        age = Decimal(
            str(
                (
                    timestamp
                    - aligned_sharp[
                        "_time"
                    ]
                ).total_seconds()
            )
        )

        row[
            "aligned_sharp_age_seconds"
        ] = dstr(
            age
        )

        row[
            "aligned_sharp_home_prob"
        ] = dstr(
            aligned_sharp[
                "_prob"
            ]
        )

        row[
            "aligned_sharp_home_delta"
        ] = dstr(
            aligned_sharp[
                "_delta"
            ]
        )

        row[
            "aligned_sharp_direction_home_sign"
        ] = (
            ""
            if aligned_sharp[
                "_direction"
            ] is None
            else aligned_sharp[
                "_direction"
            ]
        )

        row[
            "aligned_sharp_family_count"
        ] = aligned_sharp[
            "family_count"
        ]

        if aligned_sharp[
            "_composition_changed"
        ] is not None:
            row[
                "aligned_sharp_composition_changed"
            ] = btxt(
                aligned_sharp[
                    "_composition_changed"
                ]
            )

        if baseline_price is not None:
            row[
                "sharp_pm_gap_at_signal"
            ] = dstr(
                aligned_sharp[
                    "_prob"
                ]
                - baseline_price
            )

        if (
            signal_type == "rn1"
            and aligned_sharp[
                "_direction"
            ] in (-1, 1)
        ):
            row[
                "rn1_agrees_with_aligned_sharp_direction"
            ] = btxt(
                signal[
                    "_direction"
                ]
                == aligned_sharp[
                    "_direction"
                ]
            )

    # --------------------------------------------------------
    # M4 horizons -> columns
    # --------------------------------------------------------

    for horizon in HORIZONS:
        source = m4[
            horizon
        ]

        mapping = {
            "pregame_eligible":
                "pregame_eligible",

            "response_available":
                "response_available",

            "response_trade_count":
                "response_trade_count",

            "first_response_time":
                "first_response_time",

            "first_response_latency_seconds":
                "first_response_latency_seconds",

            "first_response_home_price":
                "first_response_home_price",

            "first_response_change":
                "first_response_change",

            "first_response_tx_hash":
                "first_response_tx_hash",

            "last_response_time":
                "last_response_time",

            "last_response_latency_seconds":
                "last_response_latency_seconds",

            "last_response_target_age_seconds":
                "last_response_target_age_seconds",

            "last_response_home_price":
                "last_response_home_price",

            "last_response_change":
                "last_response_change",

            "last_response_tx_hash":
                "last_response_tx_hash",

            "response_min_home_price":
                "response_min_home_price",

            "response_max_home_price":
                "response_max_home_price",

            "response_home_price_range":
                "response_home_price_range",

            "last_response_target_fresh_5s":
                "last_response_target_fresh_5s",

            "last_response_target_fresh_15s":
                "last_response_target_fresh_15s",

            "last_response_target_fresh_30s":
                "last_response_target_fresh_30s",

            "last_response_target_fresh_60s":
                "last_response_target_fresh_60s",
        }

        for src, suffix in mapping.items():
            row[
                f"pm_{horizon}s_{suffix}"
            ] = source[
                src
            ]

    return row


def build_panel(
    rn1_signals,
    sharp_signals,
    rn1_by_market,
    sharp_by_market,
    pm_map,
):
    panel = []

    for signal in rn1_signals:
        panel.append(
            base_row(
                signal,
                "rn1",
                rn1_by_market,
                sharp_by_market,
                pm_map,
            )
        )

    for signal in sharp_signals:
        panel.append(
            base_row(
                signal,
                "sharp",
                rn1_by_market,
                sharp_by_market,
                pm_map,
            )
        )

    panel.sort(
        key=lambda row: (
            row[
                "canonical_commence_time"
            ],
            row["slug"],
            row["signal_time"],
            0
            if row[
                "signal_type"
            ] == "sharp"
            else 1,
            row["signal_id"],
        )
    )

    return panel


def summarize_sharp(
    sharp_signals,
):
    counts = Counter()

    for signal in sharp_signals:
        if signal[
            "_delta"
        ] is None:
            counts[
                "first_no_delta"
            ] += 1
            continue

        counts[
            "delta_available"
        ] += 1

        if signal[
            "_direction"
        ] > 0:
            counts["home"] += 1

        elif signal[
            "_direction"
        ] < 0:
            counts["away"] += 1

        else:
            counts["zero"] += 1

        if signal[
            "_composition_changed"
        ]:
            counts[
                "composition_changed"
            ] += 1

    return dict(counts)


def summarize_rn1_clusters(
    second_groups,
    tx_groups,
):
    second_sizes = [
        len(group)
        for group
        in second_groups.values()
    ]

    tx_sizes = [
        len(group)
        for group
        in tx_groups.values()
    ]

    return {
        "same_second_clusters":
            sum(
                size > 1
                for size
                in second_sizes
            ),

        "rows_in_same_second_clusters":
            sum(
                size
                for size
                in second_sizes
                if size > 1
            ),

        "largest_same_second_cluster":
            max(
                second_sizes
            ),

        "same_tx_clusters":
            sum(
                size > 1
                for size
                in tx_sizes
            ),

        "rows_in_same_tx_clusters":
            sum(
                size
                for size
                in tx_sizes
                if size > 1
            ),

        "largest_same_tx_cluster":
            max(
                tx_sizes
            ),
    }


def summarize_cross(
    panel,
):
    return {
        "rn1_prev_sharp":
            sum(
                row[
                    "signal_type"
                ] == "rn1"
                and row[
                    "prev_sharp_signal_id"
                ] != ""
                for row in panel
            ),

        "rn1_next_sharp":
            sum(
                row[
                    "signal_type"
                ] == "rn1"
                and row[
                    "next_sharp_signal_id"
                ] != ""
                for row in panel
            ),

        "sharp_prev_rn1":
            sum(
                row[
                    "signal_type"
                ] == "sharp"
                and row[
                    "prev_rn1_signal_id"
                ] != ""
                for row in panel
            ),

        "sharp_next_rn1":
            sum(
                row[
                    "signal_type"
                ] == "sharp"
                and row[
                    "next_rn1_signal_id"
                ] != ""
                for row in panel
            ),
    }


def feasibility(
    panel,
):
    result = {}

    for typ in (
        "rn1",
        "sharp",
    ):
        result[
            typ
        ] = {}

        typed = [
            row
            for row in panel
            if row[
                "signal_type"
            ] == typ
        ]

        for horizon in HORIZONS:
            broad = 0
            strict = 0

            for row in typed:
                if (
                    typ == "sharp"
                    and row[
                        "sharp_home_delta"
                    ] == ""
                ):
                    continue

                broad_ok = (
                    row[
                        "pm_baseline_fresh_300s"
                    ] == "true"
                    and row[
                        f"pm_{horizon}s_response_available"
                    ] == "true"
                    and row[
                        f"pm_{horizon}s_last_response_target_fresh_60s"
                    ] == "true"
                )

                strict_ok = (
                    row[
                        "pm_baseline_fresh_60s"
                    ] == "true"
                    and row[
                        f"pm_{horizon}s_response_available"
                    ] == "true"
                    and row[
                        f"pm_{horizon}s_last_response_target_fresh_30s"
                    ] == "true"
                )

                broad += int(
                    broad_ok
                )

                strict += int(
                    strict_ok
                )

            result[
                typ
            ][
                horizon
            ] = (
                broad,
                strict,
            )

    return result


def validate_panel(
    panel,
    sharp_summary,
    cluster_summary,
):
    if len(
        panel
    ) != EXPECTED_SIGNALS:
        raise RuntimeError(
            f"Expected 761 panel rows, "
            f"got {len(panel)}"
        )

    ids = [
        row["signal_id"]
        for row in panel
    ]

    if len(
        set(ids)
    ) != EXPECTED_SIGNALS:
        raise RuntimeError(
            "Duplicate signal_id"
        )

    type_counts = Counter(
        row[
            "signal_type"
        ]
        for row in panel
    )

    if type_counts != Counter({
        "rn1": EXPECTED_RN1,
        "sharp": EXPECTED_SHARP,
    }):
        raise RuntimeError(
            f"Signal type counts changed: "
            f"{type_counts}"
        )

    for key, expected in (
        EXPECTED_SHARP_INNOVATION.items()
    ):
        observed = sharp_summary.get(
            key,
            0,
        )

        if observed != expected:
            raise RuntimeError(
                f"Sharp summary {key}: "
                f"expected={expected}, "
                f"observed={observed}"
            )

    if (
        cluster_summary
        != EXPECTED_RN1_CLUSTERING
    ):
        raise RuntimeError(
            "RN1 clustering contract changed: "
            f"{cluster_summary}"
        )

    cross = summarize_cross(
        panel
    )

    if (
        cross
        != EXPECTED_CROSS_AVAILABILITY
    ):
        raise RuntimeError(
            "Cross-clock availability "
            f"changed: {cross}"
        )

    observed_feasibility = feasibility(
        panel
    )

    for typ, expected_by_h in (
        EXPECTED_M6_FEASIBILITY.items()
    ):
        for horizon, expected in (
            expected_by_h.items()
        ):
            observed = (
                observed_feasibility[
                    typ
                ][
                    horizon
                ]
            )

            if observed != expected:
                raise RuntimeError(
                    f"M6 feasibility "
                    f"{typ} +{horizon}s: "
                    f"expected={expected}, "
                    f"observed={observed}"
                )

    # HOME-axis and timing checks.
    for row in panel:
        signal_time = dt(
            row[
                "signal_time"
            ]
        )

        t0 = dt(
            row[
                "canonical_commence_time"
            ]
        )

        if not signal_time < t0:
            raise RuntimeError(
                f"{row['signal_id']}: "
                "not pregame"
            )

        if (
            row[
                "pm_baseline_available"
            ] == "true"
        ):
            p = Decimal(
                row[
                    "pm_baseline_home_price"
                ]
            )

            if not (
                Decimal("0")
                <= p
                <= Decimal("1")
            ):
                raise RuntimeError(
                    "Invalid PM baseline"
                )

        if (
            row[
                "aligned_sharp_home_prob"
            ] != ""
        ):
            p = Decimal(
                row[
                    "aligned_sharp_home_prob"
                ]
            )

            if not (
                Decimal("0")
                <= p
                <= Decimal("1")
            ):
                raise RuntimeError(
                    "Invalid aligned sharp prob"
                )

            age = Decimal(
                row[
                    "aligned_sharp_age_seconds"
                ]
            )

            if age < 0:
                raise RuntimeError(
                    "Negative aligned sharp age"
                )

    return (
        cross,
        observed_feasibility,
    )


def csv_payload(
    rows,
):
    buffer = io.StringIO(
        newline=""
    )

    writer = csv.DictWriter(
        buffer,
        fieldnames=FIELDS,
        lineterminator="\n",
    )

    writer.writeheader()

    for row in rows:
        writer.writerow({
            field: row.get(
                field,
                "",
            )
            for field in FIELDS
        })

    return buffer.getvalue().encode(
        "utf-8"
    )


def write_outputs(
    panel,
    parent_hashes,
    sharp_summary,
    cluster_summary,
    cross_summary,
    feasibility_summary,
):
    PANEL_CSV.write_bytes(
        csv_payload(
            panel
        )
    )

    panel_hash = sha256_file(
        PANEL_CSV
    )

    manifest = {
        "feature_version":
            FEATURE_VERSION,

        "parent_cohort":
            COHORT_VERSION,

        "built_at_utc":
            datetime.now(
                timezone.utc
            ).isoformat(),

        "parent_hashes":
            parent_hashes,

        "unit_of_observation":
            "one RN1 fill or one sharp exchange poll",

        "counts": {
            "markets":
                EXPECTED_MARKETS,
            "rn1_signals":
                EXPECTED_RN1,
            "sharp_signals":
                EXPECTED_SHARP,
            "total_signals":
                EXPECTED_SIGNALS,
        },

        "sharp_innovation_policy": {
            "definition":
                "current sharp HOME consensus probability "
                "minus immediately previous sharp HOME "
                "consensus probability in same market",

            "first_poll":
                "missing innovation",

            "composition_change":
                "flagged, never silently filtered",
        },

        "sharp_innovation_summary":
            sharp_summary,

        "rn1_clustering_policy": {
            "same_second":
                "individual fills retained; cluster size, rank "
                "and cluster net HOME exposure preserved",

            "same_transaction":
                "individual fills retained; transaction cluster "
                "size preserved",

            "statistical_interpretation":
                "M6 must account for non-independence rather "
                "than treating clustering as additional "
                "independent information",
        },

        "rn1_clustering_summary":
            cluster_summary,

        "cross_clock_policy": {
            "previous":
                "strictly earlier event in other clock",
            "next":
                "strictly later event in other clock",
            "threshold_filtering":
                "none in M5; raw lags preserved for M6",
        },

        "cross_clock_availability":
            cross_summary,

        "aligned_sharp_policy": {
            "sharp_event":
                "current sharp poll",
            "rn1_event":
                "last sharp poll strictly before RN1 fill",
            "sharp_pm_gap":
                "aligned sharp HOME probability minus M4 "
                "PM baseline HOME execution price",
            "tradeability":
                "not inferred; PM and sharp staleness remain "
                "explicit",
        },

        "pm_response_policy": {
            "source":
                "frozen M4 Polymarket response table",
            "pivot":
                "four horizons become columns on each signal",
            "horizons_seconds":
                HORIZONS,
            "missing_response":
                "remains missing, never converted to zero",
        },

        "m6_feasibility_definitions": {
            "broad":
                "direction available; PM baseline <=300s old; "
                "response exists; last response <=60s old "
                "at target",

            "strict":
                "direction available; PM baseline <=60s old; "
                "response exists; last response <=30s old "
                "at target",
        },

        "m6_feasibility":
            {
                typ: {
                    str(h): {
                        "broad": values[0],
                        "strict": values[1],
                    }
                    for h, values
                    in by_h.items()
                }
                for typ, by_h
                in feasibility_summary.items()
            },

        "limitations": [
            "Sharp polling cadence is coarse in H2-v1; nearest "
            "cross-clock lags do not imply causal ordering.",
            "Some sharp innovations coincide with exchange-family "
            "composition changes.",
            "RN1 fills include same-second clusters and are not "
            "assumed statistically independent.",
            "Polymarket response prices are executed prices, not "
            "historical bid/ask midpoint observations.",
            "No freshness, magnitude or direction threshold is "
            "selected in M5.",
        ],

        "artifact": {
            "file":
                PANEL_CSV.name,
            "rows":
                len(panel),
            "sha256":
                panel_hash,
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

    return panel_hash


def validate_artifacts():
    if not PANEL_CSV.exists():
        raise RuntimeError(
            "Missing event panel"
        )

    if not MANIFEST_JSON.exists():
        raise RuntimeError(
            "Missing M5 manifest"
        )

    (
        _cohort,
        rn1,
        sharp,
        pm,
        parent_hashes,
    ) = load_parents()

    manifest = json.loads(
        MANIFEST_JSON.read_text()
    )

    if (
        manifest[
            "parent_hashes"
        ]
        != parent_hashes
    ):
        raise RuntimeError(
            "M5 parent hashes changed"
        )

    (
        rn1_signals,
        _rn1_by_market,
        second_groups,
        tx_groups,
    ) = build_rn1_signals(
        rn1
    )

    (
        sharp_signals,
        _sharp_by_market,
    ) = build_sharp_signals(
        sharp
    )

    sharp_summary = (
        summarize_sharp(
            sharp_signals
        )
    )

    cluster_summary = (
        summarize_rn1_clusters(
            second_groups,
            tx_groups,
        )
    )

    panel = read_csv(
        PANEL_CSV
    )

    cross_summary, feasibility_summary = (
        validate_panel(
            panel,
            sharp_summary,
            cluster_summary,
        )
    )

    if (
        cross_summary
        != manifest[
            "cross_clock_availability"
        ]
    ):
        raise RuntimeError(
            "Cross summary manifest mismatch"
        )

    panel_hash = sha256_file(
        PANEL_CSV
    )

    if (
        panel_hash
        != manifest[
            "artifact"
        ][
            "sha256"
        ]
    ):
        raise RuntimeError(
            "Event panel SHA mismatch"
        )

    return (
        panel,
        panel_hash,
        sharp_summary,
        cluster_summary,
        cross_summary,
        feasibility_summary,
    )


def print_summary(
    panel,
    panel_hash,
    sharp_summary,
    cluster_summary,
    cross_summary,
    feasibility_summary,
):
    print()
    print(
        "H2-v1 unified event panel"
    )
    print(
        "========================="
    )

    print(
        f"event rows:    {len(panel)}"
    )

    print(
        "RN1 rows:     "
        f"{sum(r['signal_type']=='rn1' for r in panel)}"
    )

    print(
        "sharp rows:   "
        f"{sum(r['signal_type']=='sharp' for r in panel)}"
    )

    print()
    print(
        "sharp innovation:"
    )

    for key in (
        "first_no_delta",
        "delta_available",
        "home",
        "away",
        "zero",
        "composition_changed",
    ):
        print(
            f"  {key:22s} "
            f"{sharp_summary[key]}"
        )

    print()
    print(
        "RN1 clustering:"
    )

    for key, value in (
        cluster_summary.items()
    ):
        print(
            f"  {key:30s} "
            f"{value}"
        )

    print()
    print(
        "cross-clock availability:"
    )

    for key, value in (
        cross_summary.items()
    ):
        print(
            f"  {key:20s} "
            f"{value}"
        )

    print()
    print(
        "M6 feasibility:"
    )

    for typ in (
        "rn1",
        "sharp",
    ):
        print(
            f"  {typ.upper()}"
        )

        for horizon in HORIZONS:
            broad, strict = (
                feasibility_summary[
                    typ
                ][
                    horizon
                ]
            )

            print(
                f"    +{horizon:3d}s "
                f"broad={broad:3d} "
                f"strict={strict:3d}"
            )

    print()
    print(
        "event panel SHA-256: "
        f"{panel_hash}"
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
                PANEL_CSV,
                MANIFEST_JSON,
            )
            if path.exists()
        ]

        if existing:
            raise SystemExit(
                "REFUSING TO RE-BUILD: "
                "M5 artifact(s) already exist: "
                + ", ".join(
                    str(path)
                    for path in existing
                )
                + ". Use --validate instead."
            )

        (
            _cohort,
            rn1,
            sharp,
            pm,
            parent_hashes,
        ) = load_parents()

        (
            rn1_signals,
            rn1_by_market,
            second_groups,
            tx_groups,
        ) = build_rn1_signals(
            rn1
        )

        (
            sharp_signals,
            sharp_by_market,
        ) = build_sharp_signals(
            sharp
        )

        pm_map = build_pm_map(
            pm
        )

        panel = build_panel(
            rn1_signals,
            sharp_signals,
            rn1_by_market,
            sharp_by_market,
            pm_map,
        )

        sharp_summary = (
            summarize_sharp(
                sharp_signals
            )
        )

        cluster_summary = (
            summarize_rn1_clusters(
                second_groups,
                tx_groups,
            )
        )

        (
            cross_summary,
            feasibility_summary,
        ) = validate_panel(
            panel,
            sharp_summary,
            cluster_summary,
        )

        panel_hash = write_outputs(
            panel,
            parent_hashes,
            sharp_summary,
            cluster_summary,
            cross_summary,
            feasibility_summary,
        )

        print_summary(
            panel,
            panel_hash,
            sharp_summary,
            cluster_summary,
            cross_summary,
            feasibility_summary,
        )

        print()
        print(
            "BUILD COMPLETE"
        )

        return

    (
        panel,
        panel_hash,
        sharp_summary,
        cluster_summary,
        cross_summary,
        feasibility_summary,
    ) = validate_artifacts()

    print_summary(
        panel,
        panel_hash,
        sharp_summary,
        cluster_summary,
        cross_summary,
        feasibility_summary,
    )

    print()
    print(
        "VALIDATION PASSED"
    )


if __name__ == "__main__":
    main()
