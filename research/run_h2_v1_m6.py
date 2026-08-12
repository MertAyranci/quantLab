"""Run the preregistered H2-v1 M6 statistical analysis.

IMPORTANT
---------
This runner refuses to execute unless the frozen M6 specification has the
exact preregistration SHA-256:

    15f8a60806f90408d2e31be2af23024deaa330453237af096c5c2fdce73636cd

Primary tests
-------------
RN1:
    same-second RN1 cluster
    +30s Polymarket response
    broad observability
    market-equal inference

Sharp:
    nonzero sharp innovation
    +60s Polymarket response
    broad observability
    market-equal inference

Two primary two-sided p-values are Holm-adjusted.

M6 tests predictive executed-price reaction only. It does not establish
tradeability or net profitability.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, stdev

from scipy import stats


REPO = Path(__file__).resolve().parents[1]

PANEL_CSV = REPO / "research/h2_v1_event_panel.csv"
PANEL_MANIFEST = REPO / "research/h2_v1_event_panel_manifest.json"

SPEC_JSON = REPO / "research/h2_v1_m6_spec.json"

RESULTS_CSV = REPO / "research/h2_v1_m6_results.csv"
MARKET_EFFECTS_CSV = REPO / "research/h2_v1_m6_market_effects.csv"
MANIFEST_JSON = REPO / "research/h2_v1_m6_manifest.json"

EXPECTED_SPEC_SHA = (
    "15f8a60806f90408d2e31be2af23024d"
    "eaa330453237af096c5c2fdce73636cd"
)

EXPECTED_PANEL_SHA = (
    "12fe2f912e45c5e11f3c7ee70bb3a755"
    "6f49d0129df987815361c8d53d1bcee8"
)

VERSION = "H2-v1-M6"

HORIZONS = [10, 30, 60, 300]


RESULT_FIELDS = [
    "analysis_id",
    "analysis_class",
    "source",
    "horizon_seconds",
    "observability",
    "composition_policy",

    "event_count",
    "market_count",

    "event_weighted_mean_signed_response",
    "event_weighted_median_signed_response",
    "event_positive_count",
    "event_zero_count",
    "event_negative_count",

    "market_equal_mean_signed_response",
    "market_sd",
    "market_se",
    "ci95_low",
    "ci95_high",
    "t_statistic",
    "p_value_two_sided",
    "holm_p_value",

    "positive_market_count",
    "zero_market_count",
    "negative_market_count",

    "mean_signal_magnitude",
    "median_signal_magnitude",
]


MARKET_FIELDS = [
    "analysis_id",
    "source",
    "horizon_seconds",
    "observability",
    "composition_policy",
    "market_id",
    "slug",
    "event_count",
    "market_mean_signed_response",
    "market_mean_raw_response",
]


def sha256_file(path: Path) -> str:
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def read_csv(path: Path):
    with path.open(
        newline="",
        encoding="utf-8",
    ) as f:
        return list(
            csv.DictReader(f)
        )


def b(value):
    return value == "true"


def f(value):
    return float(value)


def sign(value):
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def fmt(value):
    if value is None:
        return ""

    if isinstance(value, int):
        return str(value)

    if math.isnan(value):
        return ""

    if value == 0:
        return "0"

    return format(
        value,
        ".12g",
    )


def median(values):
    x = sorted(values)
    n = len(x)

    if n == 0:
        return None

    if n % 2:
        return x[n // 2]

    return (
        x[n // 2 - 1]
        + x[n // 2]
    ) / 2


def write_csv(path, rows, fields):
    buf = io.StringIO(newline="")

    writer = csv.DictWriter(
        buf,
        fieldnames=fields,
        lineterminator="\n",
    )

    writer.writeheader()

    for row in rows:
        writer.writerow({
            field: row.get(field, "")
            for field in fields
        })

    path.write_text(
        buf.getvalue(),
        encoding="utf-8",
    )


def validate_parents():
    if sha256_file(SPEC_JSON) != EXPECTED_SPEC_SHA:
        raise RuntimeError(
            "M6 SPEC SHA MISMATCH. "
            "The preregistered specification changed. "
            "Refusing to calculate effects."
        )

    if sha256_file(PANEL_CSV) != EXPECTED_PANEL_SHA:
        raise RuntimeError(
            "M5 event panel SHA mismatch"
        )

    spec = json.loads(
        SPEC_JSON.read_text()
    )

    panel_manifest = json.loads(
        PANEL_MANIFEST.read_text()
    )

    if (
        panel_manifest["artifact"]["sha256"]
        != EXPECTED_PANEL_SHA
    ):
        raise RuntimeError(
            "M5 manifest/panel mismatch"
        )

    if (
        spec["parent_panel"]["sha256"]
        != EXPECTED_PANEL_SHA
    ):
        raise RuntimeError(
            "M6 spec parent panel mismatch"
        )

    if not spec[
        "frozen_before_effect_estimation"
    ]:
        raise RuntimeError(
            "M6 specification not marked frozen"
        )

    return spec


def broad(row, horizon):
    return (
        b(row["pm_baseline_fresh_300s"])
        and b(
            row[
                f"pm_{horizon}s_response_available"
            ]
        )
        and b(
            row[
                f"pm_{horizon}s_"
                "last_response_target_fresh_60s"
            ]
        )
    )


def strict(row, horizon):
    return (
        b(row["pm_baseline_fresh_60s"])
        and b(
            row[
                f"pm_{horizon}s_response_available"
            ]
        )
        and b(
            row[
                f"pm_{horizon}s_"
                "last_response_target_fresh_30s"
            ]
        )
    )


def observable(
    row,
    horizon,
    policy,
):
    if policy == "broad":
        return broad(
            row,
            horizon,
        )

    if policy == "strict":
        return strict(
            row,
            horizon,
        )

    raise ValueError(
        f"Unknown observability: {policy}"
    )


def rn1_cluster_reps(panel):
    rn1 = [
        row
        for row in panel
        if row["signal_type"] == "rn1"
    ]

    groups = defaultdict(list)

    for row in rn1:
        groups[
            row[
                "rn1_second_cluster_id"
            ]
        ].append(row)

    reps = []

    for cluster_id, rows in groups.items():
        rows.sort(
            key=lambda row: int(
                row[
                    "rn1_second_cluster_rank"
                ]
            )
        )

        rep = rows[0]

        assert int(
            rep[
                "rn1_second_cluster_size"
            ]
        ) == len(rows)

        # M5 already certified PM fields equal within cluster.
        # Recheck the key outcome fields used by M6.
        for horizon in HORIZONS:
            for field in (
                f"pm_{horizon}s_response_available",
                f"pm_{horizon}s_last_response_change",
                f"pm_{horizon}s_"
                "last_response_target_fresh_30s",
                f"pm_{horizon}s_"
                "last_response_target_fresh_60s",
            ):
                values = {
                    row[field]
                    for row in rows
                }

                if len(values) != 1:
                    raise RuntimeError(
                        f"{cluster_id}: "
                        f"cluster PM mismatch {field}"
                    )

        reps.append(rep)

    if len(reps) != 167:
        raise RuntimeError(
            f"Expected 167 RN1 clusters, "
            f"got {len(reps)}"
        )

    return reps


def sharp_nonzero(panel):
    out = []

    for row in panel:
        if row["signal_type"] != "sharp":
            continue

        if row["sharp_home_delta"] == "":
            continue

        if f(row["sharp_home_delta"]) == 0:
            continue

        out.append(row)

    if len(out) != 311:
        raise RuntimeError(
            f"Expected 311 nonzero sharp innovations, "
            f"got {len(out)}"
        )

    return out


def select_rn1(
    rn1_reps,
    horizon,
    observability,
):
    rows = []

    for row in rn1_reps:
        direction = int(
            row[
                "rn1_second_cluster_net_direction_sign"
            ]
        )

        if direction == 0:
            continue

        if not observable(
            row,
            horizon,
            observability,
        ):
            continue

        raw = f(
            row[
                f"pm_{horizon}s_last_response_change"
            ]
        )

        magnitude = abs(
            f(
                row[
                    "rn1_second_cluster_signed_home_shares"
                ]
            )
        )

        rows.append({
            "market_id":
                row["market_id"],
            "slug":
                row["slug"],
            "signal_id":
                row["signal_id"],
            "direction":
                direction,
            "raw_response":
                raw,
            "signed_response":
                direction * raw,
            "signal_magnitude":
                magnitude,
        })

    return rows


def select_sharp(
    sharp_rows,
    horizon,
    observability,
    composition_policy,
):
    rows = []

    for row in sharp_rows:

        if (
            composition_policy
            == "stable_only"
            and row[
                "sharp_family_composition_changed"
            ] != "false"
        ):
            continue

        if not observable(
            row,
            horizon,
            observability,
        ):
            continue

        delta = f(
            row["sharp_home_delta"]
        )

        direction = sign(
            delta
        )

        if direction == 0:
            raise RuntimeError(
                "Zero sharp innovation leaked "
                "into directional sample"
            )

        raw = f(
            row[
                f"pm_{horizon}s_last_response_change"
            ]
        )

        rows.append({
            "market_id":
                row["market_id"],
            "slug":
                row["slug"],
            "signal_id":
                row["signal_id"],
            "direction":
                direction,
            "raw_response":
                raw,
            "signed_response":
                direction * raw,
            "signal_magnitude":
                abs(delta),
        })

    return rows


def select_sharp_gap(
    sharp_rows,
    horizon,
    observability,
):
    rows = []

    for row in sharp_rows:

        if not observable(
            row,
            horizon,
            observability,
        ):
            continue

        if row[
            "sharp_pm_gap_at_signal"
        ] == "":
            continue

        gap = f(
            row[
                "sharp_pm_gap_at_signal"
            ]
        )

        direction = sign(
            gap
        )

        if direction == 0:
            continue

        raw = f(
            row[
                f"pm_{horizon}s_last_response_change"
            ]
        )

        rows.append({
            "market_id":
                row["market_id"],
            "slug":
                row["slug"],
            "signal_id":
                row["signal_id"],
            "direction":
                direction,
            "raw_response":
                raw,
            "signed_response":
                direction * raw,
            "signal_magnitude":
                abs(gap),
        })

    return rows


def summarize_analysis(
    analysis_id,
    analysis_class,
    source,
    horizon,
    observability,
    composition_policy,
    events,
):
    if not events:
        raise RuntimeError(
            f"{analysis_id}: empty sample"
        )

    by_market = defaultdict(list)

    for event in events:
        by_market[
            event["market_id"]
        ].append(event)

    market_rows = []

    market_effects = []

    for market_id, rows in sorted(
        by_market.items(),
        key=lambda item: int(
            item[0]
        ),
    ):
        signed = [
            row["signed_response"]
            for row in rows
        ]

        raw = [
            row["raw_response"]
            for row in rows
        ]

        market_effect = mean(
            signed
        )

        market_effects.append(
            market_effect
        )

        market_rows.append({
            "analysis_id":
                analysis_id,
            "source":
                source,
            "horizon_seconds":
                horizon,
            "observability":
                observability,
            "composition_policy":
                composition_policy,
            "market_id":
                market_id,
            "slug":
                rows[0]["slug"],
            "event_count":
                len(rows),
            "market_mean_signed_response":
                fmt(market_effect),
            "market_mean_raw_response":
                fmt(
                    mean(raw)
                ),
        })

    n_market = len(
        market_effects
    )

    if n_market < 2:
        raise RuntimeError(
            f"{analysis_id}: fewer than 2 markets"
        )

    market_mean = mean(
        market_effects
    )

    market_sd = stdev(
        market_effects
    )

    market_se = (
        market_sd
        / math.sqrt(
            n_market
        )
    )

    t_stat = (
        market_mean
        / market_se
        if market_se > 0
        else (
            math.inf
            if market_mean > 0
            else -math.inf
            if market_mean < 0
            else 0.0
        )
    )

    if market_se > 0:
        p_value = float(
            2
            * stats.t.sf(
                abs(t_stat),
                df=n_market - 1,
            )
        )

        critical = float(
            stats.t.ppf(
                0.975,
                df=n_market - 1,
            )
        )

        ci_low = (
            market_mean
            - critical
            * market_se
        )

        ci_high = (
            market_mean
            + critical
            * market_se
        )
    else:
        p_value = (
            0.0
            if market_mean != 0
            else 1.0
        )

        ci_low = market_mean
        ci_high = market_mean

    signed_events = [
        row["signed_response"]
        for row in events
    ]

    magnitudes = [
        row["signal_magnitude"]
        for row in events
    ]

    result = {
        "analysis_id":
            analysis_id,
        "analysis_class":
            analysis_class,
        "source":
            source,
        "horizon_seconds":
            horizon,
        "observability":
            observability,
        "composition_policy":
            composition_policy,

        "event_count":
            len(events),
        "market_count":
            n_market,

        "event_weighted_mean_signed_response":
            fmt(
                mean(
                    signed_events
                )
            ),
        "event_weighted_median_signed_response":
            fmt(
                median(
                    signed_events
                )
            ),
        "event_positive_count":
            sum(
                value > 0
                for value
                in signed_events
            ),
        "event_zero_count":
            sum(
                value == 0
                for value
                in signed_events
            ),
        "event_negative_count":
            sum(
                value < 0
                for value
                in signed_events
            ),

        "market_equal_mean_signed_response":
            fmt(
                market_mean
            ),
        "market_sd":
            fmt(
                market_sd
            ),
        "market_se":
            fmt(
                market_se
            ),
        "ci95_low":
            fmt(
                ci_low
            ),
        "ci95_high":
            fmt(
                ci_high
            ),
        "t_statistic":
            fmt(
                t_stat
            ),
        "p_value_two_sided":
            fmt(
                p_value
            ),
        "holm_p_value":
            "",

        "positive_market_count":
            sum(
                value > 0
                for value
                in market_effects
            ),
        "zero_market_count":
            sum(
                value == 0
                for value
                in market_effects
            ),
        "negative_market_count":
            sum(
                value < 0
                for value
                in market_effects
            ),

        "mean_signal_magnitude":
            fmt(
                mean(
                    magnitudes
                )
            ),
        "median_signal_magnitude":
            fmt(
                median(
                    magnitudes
                )
            ),
    }

    return (
        result,
        market_rows,
    )


def holm_adjust(
    results,
    primary_ids,
):
    primary = [
        row
        for row in results
        if row[
            "analysis_id"
        ] in primary_ids
    ]

    if len(primary) != len(
        primary_ids
    ):
        raise RuntimeError(
            "Primary Holm family incomplete"
        )

    ordered = sorted(
        primary,
        key=lambda row: float(
            row[
                "p_value_two_sided"
            ]
        ),
    )

    m = len(
        ordered
    )

    running = 0.0

    adjusted = {}

    for rank, row in enumerate(
        ordered,
        1,
    ):
        raw = float(
            row[
                "p_value_two_sided"
            ]
        )

        candidate = min(
            1.0,
            (m - rank + 1)
            * raw,
        )

        running = max(
            running,
            candidate,
        )

        adjusted[
            row["analysis_id"]
        ] = running

    for row in results:
        if row[
            "analysis_id"
        ] in adjusted:
            row[
                "holm_p_value"
            ] = fmt(
                adjusted[
                    row["analysis_id"]
                ]
            )


def validate_expected_counts(
    spec,
    result_rows,
):
    by_id = {
        row["analysis_id"]:
            row
        for row in result_rows
    }

    # Primary counts.
    for source in (
        "rn1",
        "sharp",
    ):
        cfg = spec[
            "primary_tests"
        ][source]

        row = by_id[
            cfg[
                "analysis_id"
            ]
        ]

        if int(
            row["event_count"]
        ) != int(
            cfg[
                "expected_events"
            ]
        ):
            raise RuntimeError(
                f"{row['analysis_id']}: "
                "primary event count changed"
            )

        if int(
            row["market_count"]
        ) != int(
            cfg[
                "expected_markets"
            ]
        ):
            raise RuntimeError(
                f"{row['analysis_id']}: "
                "primary market count changed"
            )

    # Prespecified sensitivity counts.
    for cfg in spec[
        "prespecified_sensitivities"
    ]:
        row = by_id[
            cfg[
                "analysis_id"
            ]
        ]

        if int(
            row["event_count"]
        ) != int(
            cfg[
                "expected_events"
            ]
        ):
            raise RuntimeError(
                f"{row['analysis_id']}: "
                "sensitivity event count changed"
            )

        if int(
            row["market_count"]
        ) != int(
            cfg[
                "expected_markets"
            ]
        ):
            raise RuntimeError(
                f"{row['analysis_id']}: "
                "sensitivity market count changed"
            )


def build():
    spec = validate_parents()

    panel = read_csv(
        PANEL_CSV
    )

    if len(panel) != 761:
        raise RuntimeError(
            "Unexpected panel row count"
        )

    rn1_reps = rn1_cluster_reps(
        panel
    )

    sharp_rows = sharp_nonzero(
        panel
    )

    analyses = []

    # ----------------------------------------
    # Primary RN1
    # ----------------------------------------
    rn1_primary = spec[
        "primary_tests"
    ]["rn1"]

    analyses.append({
        "analysis_id":
            rn1_primary[
                "analysis_id"
            ],
        "analysis_class":
            "primary",
        "source":
            "rn1",
        "horizon":
            int(
                rn1_primary[
                    "horizon_seconds"
                ]
            ),
        "observability":
            rn1_primary[
                "observability"
            ],
        "composition_policy":
            "not_applicable",
    })

    # ----------------------------------------
    # Primary sharp
    # ----------------------------------------
    sharp_primary = spec[
        "primary_tests"
    ]["sharp"]

    analyses.append({
        "analysis_id":
            sharp_primary[
                "analysis_id"
            ],
        "analysis_class":
            "primary",
        "source":
            "sharp",
        "horizon":
            int(
                sharp_primary[
                    "horizon_seconds"
                ]
            ),
        "observability":
            sharp_primary[
                "observability"
            ],
        "composition_policy":
            "all",
    })

    # ----------------------------------------
    # Prespecified sensitivities
    # ----------------------------------------
    for cfg in spec[
        "prespecified_sensitivities"
    ]:
        analyses.append({
            "analysis_id":
                cfg[
                    "analysis_id"
                ],
            "analysis_class":
                "sensitivity",
            "source":
                cfg["source"],
            "horizon":
                int(
                    cfg[
                        "horizon_seconds"
                    ]
                ),
            "observability":
                cfg[
                    "observability"
                ],
            "composition_policy":
                cfg.get(
                    "composition_policy",
                    "not_applicable",
                ),
        })

    # ----------------------------------------
    # Secondary sharp-gap convergence test
    # ----------------------------------------
    gap_cfg = spec[
        "secondary_sharp_gap_test"
    ]

    analyses.append({
        "analysis_id":
            gap_cfg[
                "analysis_id"
            ],
        "analysis_class":
            "secondary",
        "source":
            "sharp_gap",
        "horizon":
            60,
        "observability":
            "broad",
        "composition_policy":
            "all",
    })

    result_rows = []
    market_rows = []

    seen = set()

    for cfg in analyses:

        analysis_id = cfg[
            "analysis_id"
        ]

        if analysis_id in seen:
            raise RuntimeError(
                f"Duplicate analysis id: "
                f"{analysis_id}"
            )

        seen.add(
            analysis_id
        )

        if cfg[
            "source"
        ] == "rn1":
            events = select_rn1(
                rn1_reps,
                cfg["horizon"],
                cfg["observability"],
            )

        elif cfg[
            "source"
        ] == "sharp":
            events = select_sharp(
                sharp_rows,
                cfg["horizon"],
                cfg["observability"],
                cfg[
                    "composition_policy"
                ],
            )

        elif cfg[
            "source"
        ] == "sharp_gap":
            events = select_sharp_gap(
                sharp_rows,
                cfg["horizon"],
                cfg["observability"],
            )

        else:
            raise RuntimeError(
                f"Unknown source "
                f"{cfg['source']}"
            )

        result, markets = (
            summarize_analysis(
                analysis_id,
                cfg[
                    "analysis_class"
                ],
                cfg["source"],
                cfg["horizon"],
                cfg["observability"],
                cfg[
                    "composition_policy"
                ],
                events,
            )
        )

        result_rows.append(
            result
        )

        market_rows.extend(
            markets
        )

    validate_expected_counts(
        spec,
        result_rows,
    )

    primary_ids = {
        spec[
            "primary_tests"
        ]["rn1"][
            "analysis_id"
        ],
        spec[
            "primary_tests"
        ]["sharp"][
            "analysis_id"
        ],
    }

    holm_adjust(
        result_rows,
        primary_ids,
    )

    write_csv(
        RESULTS_CSV,
        result_rows,
        RESULT_FIELDS,
    )

    write_csv(
        MARKET_EFFECTS_CSV,
        market_rows,
        MARKET_FIELDS,
    )

    results_sha = sha256_file(
        RESULTS_CSV
    )

    markets_sha = sha256_file(
        MARKET_EFFECTS_CSV
    )

    primary_results = {
        row[
            "analysis_id"
        ]: row
        for row in result_rows
        if row[
            "analysis_id"
        ] in primary_ids
    }

    decisions = {}

    for source in (
        "rn1",
        "sharp",
    ):
        cfg = spec[
            "primary_tests"
        ][source]

        row = primary_results[
            cfg[
                "analysis_id"
            ]
        ]

        effect = float(
            row[
                "market_equal_mean_signed_response"
            ]
        )

        holm_p = float(
            row[
                "holm_p_value"
            ]
        )

        decisions[
            f"{source}_channel_supported"
        ] = (
            effect > 0
            and holm_p < 0.05
        )

    manifest = {
        "version":
            VERSION,

        "built_at_utc":
            datetime.now(
                timezone.utc
            ).isoformat(),

        "preregistration": {
            "spec_file":
                SPEC_JSON.name,
            "spec_sha256":
                EXPECTED_SPEC_SHA,
            "panel_sha256":
                EXPECTED_PANEL_SHA,
            "verified_before_analysis":
                True,
        },

        "inference": {
            "primary_estimator":
                "equal-weighted mean of market-level "
                "mean signed responses",

            "primary_test":
                "two-sided one-sample Student t-test "
                "across market-level effects",

            "multiple_testing":
                "Holm adjustment across the two "
                "prespecified primary tests",

            "alpha":
                0.05,

            "confidence_interval":
                0.95,
        },

        "primary_analysis_ids":
            sorted(
                primary_ids
            ),

        "decisions":
            decisions,

        "interpretation_limits": [
            "M6 tests predictive executed-price reaction, "
            "not executable net profitability.",
            "Market-level equal weighting is the primary "
            "independence treatment.",
            "RN1 same-second fills are collapsed to one "
            "cluster-level signal.",
            "Sharp zero innovations are excluded from "
            "directional testing.",
            "Sharp composition-stable results are "
            "prespecified sensitivity analyses.",
            "Sharp polling cadence is too coarse to establish "
            "a minute-scale causal sharp->RN1->PM chain.",
        ],

        "artifacts": {
            "results": {
                "file":
                    RESULTS_CSV.name,
                "rows":
                    len(result_rows),
                "sha256":
                    results_sha,
            },

            "market_effects": {
                "file":
                    MARKET_EFFECTS_CSV.name,
                "rows":
                    len(market_rows),
                "sha256":
                    markets_sha,
            },
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

    return (
        spec,
        result_rows,
        market_rows,
        manifest,
        results_sha,
        markets_sha,
    )


def validate():
    validate_parents()

    for path in (
        RESULTS_CSV,
        MARKET_EFFECTS_CSV,
        MANIFEST_JSON,
    ):
        if not path.exists():
            raise RuntimeError(
                f"Missing M6 artifact: {path}"
            )

    manifest = json.loads(
        MANIFEST_JSON.read_text()
    )

    if (
        manifest[
            "preregistration"
        ][
            "spec_sha256"
        ]
        != EXPECTED_SPEC_SHA
    ):
        raise RuntimeError(
            "Manifest preregistration SHA mismatch"
        )

    if (
        manifest[
            "artifacts"
        ][
            "results"
        ][
            "sha256"
        ]
        != sha256_file(
            RESULTS_CSV
        )
    ):
        raise RuntimeError(
            "M6 results SHA mismatch"
        )

    if (
        manifest[
            "artifacts"
        ][
            "market_effects"
        ][
            "sha256"
        ]
        != sha256_file(
            MARKET_EFFECTS_CSV
        )
    ):
        raise RuntimeError(
            "M6 market effects SHA mismatch"
        )

    results = read_csv(
        RESULTS_CSV
    )

    spec = json.loads(
        SPEC_JSON.read_text()
    )

    validate_expected_counts(
        spec,
        results,
    )

    primary_ids = {
        spec[
            "primary_tests"
        ]["rn1"][
            "analysis_id"
        ],
        spec[
            "primary_tests"
        ]["sharp"][
            "analysis_id"
        ],
    }

    primary = [
        row
        for row in results
        if row[
            "analysis_id"
        ] in primary_ids
    ]

    if len(primary) != 2:
        raise RuntimeError(
            "Expected exactly 2 primary results"
        )

    for row in primary:
        if row[
            "holm_p_value"
        ] == "":
            raise RuntimeError(
                "Primary Holm p missing"
            )

    return (
        results,
        manifest,
    )


def print_results(
    results,
    manifest,
):
    print()
    print(
        "H2-v1 M6 statistical results"
    )
    print(
        "============================"
    )

    print()
    print("PRIMARY TESTS")
    print()

    for row in results:
        if row[
            "analysis_class"
        ] != "primary":
            continue

        print(
            f"{row['analysis_id']}"
        )

        print(
            f"  n={row['event_count']} events / "
            f"{row['market_count']} markets"
        )

        print(
            "  market-equal effect: "
            f"{float(row['market_equal_mean_signed_response']):+.6f}"
        )

        print(
            "  95% CI: "
            f"[{float(row['ci95_low']):+.6f}, "
            f"{float(row['ci95_high']):+.6f}]"
        )

        print(
            "  t: "
            f"{float(row['t_statistic']):+.4f}"
        )

        print(
            "  raw two-sided p: "
            f"{float(row['p_value_two_sided']):.6g}"
        )

        print(
            "  Holm p: "
            f"{float(row['holm_p_value']):.6g}"
        )

        print(
            "  positive/zero/negative markets: "
            f"{row['positive_market_count']}/"
            f"{row['zero_market_count']}/"
            f"{row['negative_market_count']}"
        )

        print(
            "  event-weighted descriptive effect: "
            f"{float(row['event_weighted_mean_signed_response']):+.6f}"
        )

        print()

    print("SENSITIVITIES / SECONDARY")
    print()

    for row in results:
        if row[
            "analysis_class"
        ] == "primary":
            continue

        print(
            f"{row['analysis_id']:30s} "
            f"n={int(row['event_count']):3d}/"
            f"{int(row['market_count']):2d}m "
            f"effect="
            f"{float(row['market_equal_mean_signed_response']):+.6f} "
            f"p="
            f"{float(row['p_value_two_sided']):.6g}"
        )

    print()
    print("DECISIONS")

    for key, value in (
        manifest[
            "decisions"
        ].items()
    ):
        print(
            f"  {key}: {value}"
        )

    print()
    print(
        "results SHA-256: "
        f"{manifest['artifacts']['results']['sha256']}"
    )

    print(
        "market effects SHA-256: "
        f"{manifest['artifacts']['market_effects']['sha256']}"
    )


def main():
    parser = argparse.ArgumentParser()

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
                RESULTS_CSV,
                MARKET_EFFECTS_CSV,
                MANIFEST_JSON,
            )
            if path.exists()
        ]

        if existing:
            raise SystemExit(
                "REFUSING TO RE-BUILD: "
                "M6 result artifact(s) already exist: "
                + ", ".join(
                    str(path)
                    for path in existing
                )
                + ". Use --validate instead."
            )

        (
            _spec,
            results,
            _market_rows,
            manifest,
            _results_sha,
            _markets_sha,
        ) = build()

        print_results(
            results,
            manifest,
        )

        print()
        print("BUILD COMPLETE")

        return

    results, manifest = (
        validate()
    )

    print_results(
        results,
        manifest,
    )

    print()
    print("VALIDATION PASSED")


if __name__ == "__main__":
    main()
