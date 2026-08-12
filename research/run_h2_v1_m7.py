"""Run the preregistered H2-v1 M7 economic/tradeability analysis.

M7 evaluates a historical print-supported economic proxy.

Primary sharp proxy:
    signal       = nonzero sharp innovation
    action delay = 10 seconds
    target       = signal + 60 seconds
    entry        = first desired-outcome taker BUY after action time,
                   strictly before target
    close        = first opposite-outcome taker BUY at/after target,
                   no more than 300 seconds after target
    size gate    = both selected prints >= 5 shares
    settlement   = merge one complete binary pair -> $1

Fees:
    historical frozen feeSchedule.rate = 0.05
    taker fee/share = rate * p * (1-p)
    applied to both BUY legs

Important:
    This is NOT proof that our hypothetical orders would have filled.
    H2-v1 has no historical order book, bid/ask, or depth.

RN1:
    oracle-only because the historical collector did not observe RN1
    fills in actionable real time.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_EVEN, getcontext
from pathlib import Path


getcontext().prec = 40

REPO = Path(__file__).resolve().parents[1]
R = REPO / "research"

PANEL_CSV = R / "h2_v1_event_panel.csv"
PM_TAPE_CSV = R / "h2_v1_pm_pregame_tape.csv"
M6_SPEC_JSON = R / "h2_v1_m6_spec.json"
M6_RESULTS_CSV = R / "h2_v1_m6_results.csv"

SPEC_JSON = R / "h2_v1_m7_spec.json"

TRADES_CSV = R / "h2_v1_m7_trades.csv"
MARKET_EFFECTS_CSV = R / "h2_v1_m7_market_effects.csv"
SUMMARY_CSV = R / "h2_v1_m7_summary.csv"
MANIFEST_JSON = R / "h2_v1_m7_manifest.json"

VERSION = "H2-v1-M7"

EXPECTED_SPEC_SHA = (
    "9ccd61fb85ed10696f40456e2640c9da"
    "f9442ca2297057c695d15032f495fe1d"
)

EXPECTED_PANEL_SHA = (
    "12fe2f912e45c5e11f3c7ee70bb3a755"
    "6f49d0129df987815361c8d53d1bcee8"
)

EXPECTED_PM_TAPE_SHA = (
    "c9efa7ee7fd05de709ca79e163174afda"
    "c1f838482b7948e22ee19fe7f5362c7"
)

EXPECTED_M6_SPEC_SHA = (
    "15f8a60806f90408d2e31be2af23024d"
    "eaa330453237af096c5c2fdce73636cd"
)

EXPECTED_M6_RESULTS_SHA = (
    "25392ba88b660aeefab3861615d4fb165"
    "eebb9ecfe5683e898e6168c136148de"
)

Q = Decimal("0.000000000001")


TRADE_FIELDS = [
    "analysis_id",
    "analysis_class",
    "channel",

    "signal_id",
    "market_id",
    "slug",
    "signal_time",

    "desired_token",
    "opposite_token",

    "action_latency_seconds",
    "signal_target_seconds",
    "max_delay_after_target_seconds",
    "minimum_print_size_shares",

    "entry_time",
    "entry_delay_seconds",
    "entry_price",
    "entry_print_size",

    "hedge_time",
    "hedge_delay_after_target_seconds",
    "hedge_price",
    "hedge_print_size",

    "gross_pnl_per_share",
    "entry_fee_per_share",
    "hedge_fee_per_share",
    "total_fee_per_share",
    "platform_fee_adjusted_pnl_per_share",

    "five_share_platform_fee_adjusted_pnl",
    "total_pair_cost_per_share",
    "return_on_total_pair_cost",
]


MARKET_FIELDS = [
    "analysis_id",
    "analysis_class",
    "channel",
    "market_id",
    "slug",
    "trade_count",
    "market_mean_gross_pnl_per_share",
    "market_mean_total_fee_per_share",
    "market_mean_net_pnl_per_share",
    "market_mean_five_share_net",
]


SUMMARY_FIELDS = [
    "analysis_id",
    "analysis_class",
    "channel",

    "candidate_event_count",
    "event_count",
    "market_count",

    "action_latency_seconds",
    "signal_target_seconds",
    "max_delay_after_target_seconds",
    "minimum_print_size_shares",

    "event_weighted_mean_gross_per_share",
    "event_weighted_mean_fee_per_share",
    "event_weighted_mean_net_per_share",
    "event_weighted_median_net_per_share",

    "positive_event_count",
    "zero_event_count",
    "negative_event_count",

    "market_equal_mean_net_per_share",

    "positive_market_count",
    "zero_market_count",
    "negative_market_count",

    "mean_five_share_net",
    "median_five_share_net",

    "mean_return_on_total_pair_cost",
    "median_return_on_total_pair_cost",
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
        return list(csv.DictReader(f))


def parse_dt(value):
    return datetime.fromisoformat(
        str(value).replace(
            "Z",
            "+00:00",
        )
    )


def dec(value):
    return Decimal(str(value))


def mean_decimal(values):
    if not values:
        raise ValueError("empty mean")

    return sum(
        values,
        Decimal("0"),
    ) / Decimal(
        len(values)
    )


def median_decimal(values):
    if not values:
        raise ValueError("empty median")

    values = sorted(values)

    n = len(values)

    if n % 2:
        return values[n // 2]

    return (
        values[n // 2 - 1]
        + values[n // 2]
    ) / Decimal("2")


def dstr(value):
    if value is None:
        return ""

    value = Decimal(value)

    value = value.quantize(
        Q,
        rounding=ROUND_HALF_EVEN,
    )

    if value == 0:
        return "0"

    s = format(
        value,
        "f",
    )

    if "." in s:
        s = s.rstrip("0").rstrip(".")

    return s


def write_csv(path, rows, fields):
    buf = io.StringIO(
        newline=""
    )

    writer = csv.DictWriter(
        buf,
        fieldnames=fields,
        lineterminator="\n",
    )

    writer.writeheader()

    for row in rows:
        writer.writerow({
            field:
                row.get(
                    field,
                    "",
                )
            for field in fields
        })

    path.write_text(
        buf.getvalue(),
        encoding="utf-8",
    )


def validate_parents():
    checks = {
        SPEC_JSON:
            EXPECTED_SPEC_SHA,

        PANEL_CSV:
            EXPECTED_PANEL_SHA,

        PM_TAPE_CSV:
            EXPECTED_PM_TAPE_SHA,

        M6_SPEC_JSON:
            EXPECTED_M6_SPEC_SHA,

        M6_RESULTS_CSV:
            EXPECTED_M6_RESULTS_SHA,
    }

    for path, expected in checks.items():

        actual = sha256_file(
            path
        )

        if actual != expected:
            raise RuntimeError(
                f"SHA mismatch: {path}\n"
                f"expected={expected}\n"
                f"actual={actual}"
            )

    spec = json.loads(
        SPEC_JSON.read_text()
    )

    if (
        spec["spec_version"]
        != "H2-v1-M7-SPEC-1"
    ):
        raise RuntimeError(
            "Unexpected M7 spec version"
        )

    if not spec[
        "frozen_before_economic_estimation"
    ]:
        raise RuntimeError(
            "M7 spec not marked frozen"
        )

    fee = spec[
        "fee_contract"
    ]

    if dec(
        fee["rate"]
    ) != Decimal("0.05"):
        raise RuntimeError(
            "Unexpected fee rate"
        )

    if int(
        fee["exponent"]
    ) != 1:
        raise RuntimeError(
            "Unexpected fee exponent"
        )

    if not fee[
        "taker_only"
    ]:
        raise RuntimeError(
            "Expected taker-only fee contract"
        )

    return spec


def broad(row, horizon):
    return (
        row[
            "pm_baseline_fresh_300s"
        ] == "true"

        and row[
            f"pm_{horizon}s_"
            "response_available"
        ] == "true"

        and row[
            f"pm_{horizon}s_"
            "last_response_target_fresh_60s"
        ] == "true"
    )


def build_populations(panel):
    rn1 = [
        row
        for row in panel
        if row[
            "signal_type"
        ] == "rn1"
    ]

    sharp = [
        row
        for row in panel
        if row[
            "signal_type"
        ] == "sharp"
    ]

    if len(rn1) != 213:
        raise RuntimeError(
            "Expected 213 RN1 rows"
        )

    if len(sharp) != 548:
        raise RuntimeError(
            "Expected 548 sharp rows"
        )

    # RN1: one representative per same-second cluster.
    groups = defaultdict(list)

    for row in rn1:
        groups[
            row[
                "rn1_second_cluster_id"
            ]
        ].append(row)

    rn1_reps = []

    for rows in groups.values():

        rows.sort(
            key=lambda row: int(
                row[
                    "rn1_second_cluster_rank"
                ]
            )
        )

        rn1_reps.append(
            rows[0]
        )

    if len(rn1_reps) != 167:
        raise RuntimeError(
            "Expected 167 RN1 clusters"
        )

    rn1_primary = [
        row
        for row in rn1_reps
        if (
            int(
                row[
                    "rn1_second_cluster_"
                    "net_direction_sign"
                ]
            ) != 0

            and broad(
                row,
                30,
            )
        )
    ]

    sharp_nonzero = []

    for row in sharp:

        value = row[
            "sharp_home_delta"
        ]

        if value == "":
            continue

        if dec(value) == 0:
            continue

        sharp_nonzero.append(
            row
        )

    sharp_primary = [
        row
        for row in sharp_nonzero
        if broad(
            row,
            60,
        )
    ]

    sharp_gap = [
        row
        for row in sharp_primary
        if (
            row[
                "sharp_pm_gap_at_signal"
            ] != ""

            and dec(
                row[
                    "sharp_pm_gap_at_signal"
                ]
            ) != 0
        )
    ]

    if len(rn1_primary) != 106:
        raise RuntimeError(
            "Expected 106 RN1 candidate events"
        )

    if len(sharp_primary) != 57:
        raise RuntimeError(
            "Expected 57 sharp candidate events"
        )

    if len(sharp_gap) != 56:
        raise RuntimeError(
            "Expected 56 sharp-gap candidate events"
        )

    return {
        "rn1_oracle":
            rn1_primary,

        "sharp":
            sharp_primary,

        "sharp_gap":
            sharp_gap,
    }


def desired_token(
    row,
    channel,
):
    if channel == "rn1_oracle":

        direction = int(
            row[
                "rn1_second_cluster_"
                "net_direction_sign"
            ]
        )

    elif channel == "sharp":

        delta = dec(
            row[
                "sharp_home_delta"
            ]
        )

        direction = (
            1 if delta > 0
            else -1
        )

    elif channel == "sharp_gap":

        gap = dec(
            row[
                "sharp_pm_gap_at_signal"
            ]
        )

        direction = (
            1 if gap > 0
            else -1
        )

    else:
        raise ValueError(
            channel
        )

    if direction not in (-1, 1):
        raise RuntimeError(
            "Non-directional signal leaked "
            "into M7"
        )

    return (
        "home"
        if direction > 0
        else "away"
    )


def opposite_token(token):
    if token == "home":
        return "away"

    if token == "away":
        return "home"

    raise ValueError(
        token
    )


def build_pm_index(pm_rows):
    by_market = defaultdict(list)

    for row in pm_rows:

        x = dict(row)

        x["_time"] = parse_dt(
            row["event_time"]
        )

        x["_price"] = dec(
            row["token_price"]
        )

        x["_size"] = dec(
            row["size"]
        )

        x["_side"] = (
            row["side"].upper()
        )

        if x["_side"] not in (
            "BUY",
            "SELL",
        ):
            raise RuntimeError(
                "Unexpected PM trade side"
            )

        if row[
            "token_team_side"
        ] not in (
            "home",
            "away",
        ):
            raise RuntimeError(
                "Unexpected PM team side"
            )

        by_market[
            int(
                row["market_id"]
            )
        ].append(x)

    for sequence in (
        by_market.values()
    ):
        sequence.sort(
            key=lambda row: (
                row["_time"],
                int(
                    row[
                        "market_trade_index"
                    ]
                ),
            )
        )

    return by_market


def first_buy(
    sequence,
    *,
    token,
    start,
    strict_after,
):
    for trade in sequence:

        if strict_after:

            if trade["_time"] <= start:
                continue

        else:

            if trade["_time"] < start:
                continue

        if trade["_side"] != "BUY":
            continue

        if (
            trade["token_team_side"]
            != token
        ):
            continue

        return trade

    return None


def fee_per_share(
    price,
    rate,
):
    return (
        rate
        * price
        * (
            Decimal("1")
            - price
        )
    )


def select_round_trips(
    sample,
    pm_by_market,
    *,
    analysis_id,
    analysis_class,
    channel,
    action_latency_seconds,
    target_seconds,
    hedge_cap_seconds,
    minimum_size,
    fee_rate,
):
    out = []

    for row in sample:

        signal = parse_dt(
            row[
                "signal_time"
            ]
        )

        target = (
            signal
            + timedelta(
                seconds=target_seconds
            )
        )

        actionable = (
            signal
            + timedelta(
                seconds=action_latency_seconds
            )
        )

        t0 = parse_dt(
            row[
                "canonical_commence_time"
            ]
        )

        if not target < t0:
            raise RuntimeError(
                f"{analysis_id}: "
                "target crossed T0"
            )

        desired = desired_token(
            row,
            channel,
        )

        opposite = opposite_token(
            desired
        )

        sequence = pm_by_market[
            int(
                row["market_id"]
            )
        ]

        # RN1 zero-latency oracle must not use
        # any print in the signal second.
        strict_entry = (
            channel == "rn1_oracle"
            and action_latency_seconds == 0
        )

        entry = first_buy(
            sequence,
            token=desired,
            start=actionable,
            strict_after=strict_entry,
        )

        if entry is None:
            continue

        # Entry must occur BEFORE the predictive target.
        if entry["_time"] >= target:
            continue

        hedge = first_buy(
            sequence,
            token=opposite,
            start=target,
            strict_after=False,
        )

        if hedge is None:
            continue

        if hedge["_time"] >= t0:
            continue

        hedge_delay = (
            hedge["_time"]
            - target
        ).total_seconds()

        if (
            hedge_delay
            > hedge_cap_seconds
        ):
            continue

        # This is deliberately a gate on the selected
        # first prints. We do not skip an undersized first
        # print to search for a later favorable print.
        if minimum_size is not None:

            min_size = dec(
                minimum_size
            )

            if (
                entry["_size"] < min_size
                or hedge["_size"] < min_size
            ):
                continue

        entry_price = entry[
            "_price"
        ]

        hedge_price = hedge[
            "_price"
        ]

        if not (
            Decimal("0")
            < entry_price
            < Decimal("1")
        ):
            raise RuntimeError(
                "Invalid entry price"
            )

        if not (
            Decimal("0")
            < hedge_price
            < Decimal("1")
        ):
            raise RuntimeError(
                "Invalid hedge price"
            )

        gross = (
            Decimal("1")
            - entry_price
            - hedge_price
        )

        entry_fee = fee_per_share(
            entry_price,
            fee_rate,
        )

        hedge_fee = fee_per_share(
            hedge_price,
            fee_rate,
        )

        total_fee = (
            entry_fee
            + hedge_fee
        )

        net = (
            gross
            - total_fee
        )

        five_share_net = (
            net
            * Decimal("5")
        )

        total_pair_cost = (
            entry_price
            + hedge_price
            + total_fee
        )

        if total_pair_cost <= 0:
            raise RuntimeError(
                "Invalid total pair cost"
            )

        return_on_cost = (
            net
            / total_pair_cost
        )

        entry_delay = Decimal(
            str(
                (
                    entry["_time"]
                    - signal
                ).total_seconds()
            )
        )

        hedge_delay_dec = Decimal(
            str(
                hedge_delay
            )
        )

        out.append({
            "analysis_id":
                analysis_id,

            "analysis_class":
                analysis_class,

            "channel":
                channel,

            "signal_id":
                row["signal_id"],

            "market_id":
                row["market_id"],

            "slug":
                row["slug"],

            "signal_time":
                row["signal_time"],

            "desired_token":
                desired,

            "opposite_token":
                opposite,

            "action_latency_seconds":
                action_latency_seconds,

            "signal_target_seconds":
                target_seconds,

            "max_delay_after_target_seconds":
                hedge_cap_seconds,

            "minimum_print_size_shares":
                (
                    ""
                    if minimum_size is None
                    else minimum_size
                ),

            "entry_time":
                entry["event_time"],

            "entry_delay_seconds":
                dstr(
                    entry_delay
                ),

            "entry_price":
                dstr(
                    entry_price
                ),

            "entry_print_size":
                dstr(
                    entry["_size"]
                ),

            "hedge_time":
                hedge["event_time"],

            "hedge_delay_after_target_seconds":
                dstr(
                    hedge_delay_dec
                ),

            "hedge_price":
                dstr(
                    hedge_price
                ),

            "hedge_print_size":
                dstr(
                    hedge["_size"]
                ),

            "gross_pnl_per_share":
                dstr(
                    gross
                ),

            "entry_fee_per_share":
                dstr(
                    entry_fee
                ),

            "hedge_fee_per_share":
                dstr(
                    hedge_fee
                ),

            "total_fee_per_share":
                dstr(
                    total_fee
                ),

            "platform_fee_adjusted_pnl_per_share":
                dstr(
                    net
                ),

            "five_share_platform_fee_adjusted_pnl":
                dstr(
                    five_share_net
                ),

            "total_pair_cost_per_share":
                dstr(
                    total_pair_cost
                ),

            "return_on_total_pair_cost":
                dstr(
                    return_on_cost
                ),

            # Internal exact values.
            "_gross":
                gross,

            "_fee":
                total_fee,

            "_net":
                net,

            "_five":
                five_share_net,

            "_return":
                return_on_cost,
        })

    return out


def summarize(
    analysis,
    trades,
):
    if not trades:
        raise RuntimeError(
            f"{analysis['analysis_id']}: "
            "empty economic sample"
        )

    by_market = defaultdict(
        list
    )

    for trade in trades:
        by_market[
            trade["market_id"]
        ].append(trade)

    market_rows = []
    market_net = []

    for market_id, rows in sorted(
        by_market.items(),
        key=lambda item: int(
            item[0]
        ),
    ):
        gross = mean_decimal([
            row["_gross"]
            for row in rows
        ])

        fee = mean_decimal([
            row["_fee"]
            for row in rows
        ])

        net = mean_decimal([
            row["_net"]
            for row in rows
        ])

        five = mean_decimal([
            row["_five"]
            for row in rows
        ])

        market_net.append(
            net
        )

        market_rows.append({
            "analysis_id":
                analysis[
                    "analysis_id"
                ],

            "analysis_class":
                analysis[
                    "analysis_class"
                ],

            "channel":
                analysis[
                    "channel"
                ],

            "market_id":
                market_id,

            "slug":
                rows[0]["slug"],

            "trade_count":
                len(rows),

            "market_mean_gross_pnl_per_share":
                dstr(gross),

            "market_mean_total_fee_per_share":
                dstr(fee),

            "market_mean_net_pnl_per_share":
                dstr(net),

            "market_mean_five_share_net":
                dstr(five),
        })

    event_net = [
        row["_net"]
        for row in trades
    ]

    event_gross = [
        row["_gross"]
        for row in trades
    ]

    event_fee = [
        row["_fee"]
        for row in trades
    ]

    event_five = [
        row["_five"]
        for row in trades
    ]

    event_return = [
        row["_return"]
        for row in trades
    ]

    result = {
        "analysis_id":
            analysis[
                "analysis_id"
            ],

        "analysis_class":
            analysis[
                "analysis_class"
            ],

        "channel":
            analysis[
                "channel"
            ],

        "candidate_event_count":
            analysis[
                "candidate_event_count"
            ],

        "event_count":
            len(trades),

        "market_count":
            len(by_market),

        "action_latency_seconds":
            analysis[
                "action_latency_seconds"
            ],

        "signal_target_seconds":
            analysis[
                "signal_target_seconds"
            ],

        "max_delay_after_target_seconds":
            analysis[
                "hedge_cap_seconds"
            ],

        "minimum_print_size_shares":
            (
                ""
                if analysis[
                    "minimum_size"
                ] is None
                else analysis[
                    "minimum_size"
                ]
            ),

        "event_weighted_mean_gross_per_share":
            dstr(
                mean_decimal(
                    event_gross
                )
            ),

        "event_weighted_mean_fee_per_share":
            dstr(
                mean_decimal(
                    event_fee
                )
            ),

        "event_weighted_mean_net_per_share":
            dstr(
                mean_decimal(
                    event_net
                )
            ),

        "event_weighted_median_net_per_share":
            dstr(
                median_decimal(
                    event_net
                )
            ),

        "positive_event_count":
            sum(
                value > 0
                for value in event_net
            ),

        "zero_event_count":
            sum(
                value == 0
                for value in event_net
            ),

        "negative_event_count":
            sum(
                value < 0
                for value in event_net
            ),

        "market_equal_mean_net_per_share":
            dstr(
                mean_decimal(
                    market_net
                )
            ),

        "positive_market_count":
            sum(
                value > 0
                for value in market_net
            ),

        "zero_market_count":
            sum(
                value == 0
                for value in market_net
            ),

        "negative_market_count":
            sum(
                value < 0
                for value in market_net
            ),

        "mean_five_share_net":
            dstr(
                mean_decimal(
                    event_five
                )
            ),

        "median_five_share_net":
            dstr(
                median_decimal(
                    event_five
                )
            ),

        "mean_return_on_total_pair_cost":
            dstr(
                mean_decimal(
                    event_return
                )
            ),

        "median_return_on_total_pair_cost":
            dstr(
                median_decimal(
                    event_return
                )
            ),
    }

    return (
        result,
        market_rows,
    )


def analysis_contracts(
    spec,
    populations,
):
    analyses = []

    primary = spec[
        "sharp_primary"
    ]

    analyses.append({
        "analysis_id":
            primary[
                "analysis_id"
            ],

        "analysis_class":
            "primary",

        "channel":
            "sharp",

        "sample":
            populations[
                "sharp"
            ],

        "candidate_event_count":
            primary[
                "candidate_events"
            ],

        "action_latency_seconds":
            primary[
                "action_latency_seconds"
            ],

        "signal_target_seconds":
            primary[
                "signal_target_seconds"
            ],

        "hedge_cap_seconds":
            primary[
                "close"
            ][
                "max_delay_after_target_seconds"
            ],

        "minimum_size":
            primary[
                "minimum_print_size_shares"
            ],

        "expected_events":
            primary[
                "expected_round_trips"
            ],

        "expected_markets":
            primary[
                "expected_markets"
            ],
    })

    for sensitivity in spec[
        "sharp_prespecified_sensitivities"
    ]:

        analyses.append({
            "analysis_id":
                sensitivity[
                    "analysis_id"
                ],

            "analysis_class":
                "sensitivity",

            "channel":
                "sharp",

            "sample":
                populations[
                    "sharp"
                ],

            "candidate_event_count":
                primary[
                    "candidate_events"
                ],

            "action_latency_seconds":
                sensitivity[
                    "action_latency_seconds"
                ],

            "signal_target_seconds":
                sensitivity[
                    "signal_target_seconds"
                ],

            "hedge_cap_seconds":
                sensitivity[
                    "max_delay_after_target_seconds"
                ],

            "minimum_size":
                sensitivity[
                    "minimum_print_size_shares"
                ],

            "expected_events":
                sensitivity[
                    "expected_round_trips"
                ],

            "expected_markets":
                sensitivity[
                    "expected_markets"
                ],
        })

    gap = spec[
        "sharp_gap_secondary"
    ]

    analyses.append({
        "analysis_id":
            gap[
                "analysis_id"
            ],

        "analysis_class":
            "secondary",

        "channel":
            "sharp_gap",

        "sample":
            populations[
                "sharp_gap"
            ],

        "candidate_event_count":
            gap[
                "candidate_events"
            ],

        "action_latency_seconds":
            gap[
                "action_latency_seconds"
            ],

        "signal_target_seconds":
            gap[
                "signal_target_seconds"
            ],

        "hedge_cap_seconds":
            gap[
                "max_delay_after_target_seconds"
            ],

        "minimum_size":
            gap[
                "minimum_print_size_shares"
            ],

        "expected_events":
            gap[
                "expected_round_trips"
            ],

        "expected_markets":
            gap[
                "expected_markets"
            ],
    })

    rn1 = spec[
        "rn1_oracle"
    ]

    analyses.append({
        "analysis_id":
            rn1[
                "analysis_id"
            ],

        "analysis_class":
            "oracle",

        "channel":
            "rn1_oracle",

        "sample":
            populations[
                "rn1_oracle"
            ],

        "candidate_event_count":
            rn1[
                "candidate_events"
            ],

        "action_latency_seconds":
            rn1[
                "action_latency_seconds"
            ],

        "signal_target_seconds":
            rn1[
                "signal_target_seconds"
            ],

        "hedge_cap_seconds":
            rn1[
                "max_delay_after_target_seconds"
            ],

        "minimum_size":
            rn1[
                "minimum_print_size_shares"
            ],

        "expected_events":
            rn1[
                "expected_round_trips"
            ],

        "expected_markets":
            rn1[
                "expected_markets"
            ],
    })

    return analyses


def build():
    spec = validate_parents()

    panel = read_csv(
        PANEL_CSV
    )

    pm = read_csv(
        PM_TAPE_CSV
    )

    if len(panel) != 761:
        raise RuntimeError(
            "Expected 761 M5 rows"
        )

    if len(pm) != 16047:
        raise RuntimeError(
            "Expected 16047 PM pregame prints"
        )

    populations = build_populations(
        panel
    )

    pm_by_market = build_pm_index(
        pm
    )

    fee_rate = dec(
        spec[
            "fee_contract"
        ][
            "rate"
        ]
    )

    analyses = analysis_contracts(
        spec,
        populations,
    )

    all_trades = []
    summary_rows = []
    market_rows = []

    for analysis in analyses:

        trades = select_round_trips(
            analysis["sample"],
            pm_by_market,

            analysis_id=
                analysis[
                    "analysis_id"
                ],

            analysis_class=
                analysis[
                    "analysis_class"
                ],

            channel=
                analysis[
                    "channel"
                ],

            action_latency_seconds=
                analysis[
                    "action_latency_seconds"
                ],

            target_seconds=
                analysis[
                    "signal_target_seconds"
                ],

            hedge_cap_seconds=
                analysis[
                    "hedge_cap_seconds"
                ],

            minimum_size=
                analysis[
                    "minimum_size"
                ],

            fee_rate=
                fee_rate,
        )

        actual_events = len(
            trades
        )

        actual_markets = len({
            row["market_id"]
            for row in trades
        })

        if (
            actual_events
            != int(
                analysis[
                    "expected_events"
                ]
            )
        ):
            raise RuntimeError(
                f"{analysis['analysis_id']}: "
                f"expected "
                f"{analysis['expected_events']} "
                f"round trips, got "
                f"{actual_events}"
            )

        if (
            actual_markets
            != int(
                analysis[
                    "expected_markets"
                ]
            )
        ):
            raise RuntimeError(
                f"{analysis['analysis_id']}: "
                f"expected "
                f"{analysis['expected_markets']} "
                f"markets, got "
                f"{actual_markets}"
            )

        (
            summary,
            markets,
        ) = summarize(
            analysis,
            trades,
        )

        summary_rows.append(
            summary
        )

        market_rows.extend(
            markets
        )

        for trade in trades:

            # Remove internal exact arithmetic fields
            # before writing the frozen artifact.
            all_trades.append({
                field:
                    trade.get(
                        field,
                        "",
                    )
                for field
                in TRADE_FIELDS
            })

    write_csv(
        TRADES_CSV,
        all_trades,
        TRADE_FIELDS,
    )

    write_csv(
        MARKET_EFFECTS_CSV,
        market_rows,
        MARKET_FIELDS,
    )

    write_csv(
        SUMMARY_CSV,
        summary_rows,
        SUMMARY_FIELDS,
    )

    primary_id = spec[
        "sharp_primary"
    ][
        "analysis_id"
    ]

    primary = next(
        row
        for row in summary_rows
        if row[
            "analysis_id"
        ] == primary_id
    )

    primary_market_equal_net = dec(
        primary[
            "market_equal_mean_net_per_share"
        ]
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

            "verified_before_economic_estimation":
                True,
        },

        "parents": {
            "event_panel_sha256":
                EXPECTED_PANEL_SHA,

            "pm_pregame_tape_sha256":
                EXPECTED_PM_TAPE_SHA,

            "m6_spec_sha256":
                EXPECTED_M6_SPEC_SHA,

            "m6_results_sha256":
                EXPECTED_M6_RESULTS_SHA,
        },

        "fee_model": {
            "rate":
                str(
                    spec[
                        "fee_contract"
                    ][
                        "rate"
                    ]
                ),

            "exponent":
                spec[
                    "fee_contract"
                ][
                    "exponent"
                ],

            "formula":
                "rate * p * (1-p)",

            "charged_on":
                "both taker BUY legs",
        },

        "primary_analysis_id":
            primary_id,

        "decisions": {
            "positive_print_proxy":
                (
                    primary_market_equal_net
                    > 0
                ),

            "historically_executable_strategy_demonstrated":
                False,

            "rn1_live_strategy_supported":
                False,
        },

        "interpretation_limits": [
            "Historical order-book bid/ask and depth are unavailable.",
            "Observed taker BUY prints do not prove our hypothetical order "
            "would have filled at the same price or size.",
            "The primary sharp predictive target remains +60 seconds; "
            "the 300-second cap is an execution unwind allowance.",
            "Both historical BUY prints must be at least 5 shares in the "
            "primary economic proxy.",
            "Platform taker fees are included using the frozen historical "
            "market feeSchedule rate.",
            "Historical merge gas is not modeled.",
            "RN1 economics are oracle-only because RN1 was not historically "
            "captured by the collector in actionable real time.",
            "No new M7 significance test is performed.",
        ],

        "artifacts": {
            "trades": {
                "file":
                    TRADES_CSV.name,

                "rows":
                    len(
                        all_trades
                    ),

                "sha256":
                    sha256_file(
                        TRADES_CSV
                    ),
            },

            "market_effects": {
                "file":
                    MARKET_EFFECTS_CSV.name,

                "rows":
                    len(
                        market_rows
                    ),

                "sha256":
                    sha256_file(
                        MARKET_EFFECTS_CSV
                    ),
            },

            "summary": {
                "file":
                    SUMMARY_CSV.name,

                "rows":
                    len(
                        summary_rows
                    ),

                "sha256":
                    sha256_file(
                        SUMMARY_CSV
                    ),
            },
        },
    }

    MANIFEST_JSON.write_text(
        json.dumps(
            manifest,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    return (
        summary_rows,
        manifest,
    )


def validate_trade_arithmetic(
    trade_rows,
    fee_rate,
):
    for row in trade_rows:

        entry = dec(
            row["entry_price"]
        )

        hedge = dec(
            row["hedge_price"]
        )

        expected_gross = (
            Decimal("1")
            - entry
            - hedge
        )

        expected_entry_fee = (
            fee_per_share(
                entry,
                fee_rate,
            )
        )

        expected_hedge_fee = (
            fee_per_share(
                hedge,
                fee_rate,
            )
        )

        expected_fee = (
            expected_entry_fee
            + expected_hedge_fee
        )

        expected_net = (
            expected_gross
            - expected_fee
        )

        tolerance = Decimal(
            "0.000000000002"
        )

        checks = [
            (
                dec(
                    row[
                        "gross_pnl_per_share"
                    ]
                ),
                expected_gross,
                "gross",
            ),
            (
                dec(
                    row[
                        "entry_fee_per_share"
                    ]
                ),
                expected_entry_fee,
                "entry fee",
            ),
            (
                dec(
                    row[
                        "hedge_fee_per_share"
                    ]
                ),
                expected_hedge_fee,
                "hedge fee",
            ),
            (
                dec(
                    row[
                        "total_fee_per_share"
                    ]
                ),
                expected_fee,
                "total fee",
            ),
            (
                dec(
                    row[
                        "platform_fee_adjusted_"
                        "pnl_per_share"
                    ]
                ),
                expected_net,
                "net",
            ),
        ]

        for actual, expected, label in checks:

            if abs(
                actual
                - expected
            ) > tolerance:
                raise RuntimeError(
                    f"{row['analysis_id']} "
                    f"{row['signal_id']}: "
                    f"{label} arithmetic mismatch"
                )


def validate():
    spec = validate_parents()

    required = [
        TRADES_CSV,
        MARKET_EFFECTS_CSV,
        SUMMARY_CSV,
        MANIFEST_JSON,
    ]

    for path in required:

        if not path.exists():
            raise RuntimeError(
                f"Missing M7 artifact: "
                f"{path}"
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
            "M7 manifest spec SHA mismatch"
        )

    artifact_paths = {
        "trades":
            TRADES_CSV,

        "market_effects":
            MARKET_EFFECTS_CSV,

        "summary":
            SUMMARY_CSV,
    }

    for name, path in (
        artifact_paths.items()
    ):

        expected = manifest[
            "artifacts"
        ][name][
            "sha256"
        ]

        actual = sha256_file(
            path
        )

        if expected != actual:
            raise RuntimeError(
                f"{name} artifact SHA mismatch"
            )

    trades = read_csv(
        TRADES_CSV
    )

    summary = read_csv(
        SUMMARY_CSV
    )

    markets = read_csv(
        MARKET_EFFECTS_CSV
    )

    if len(summary) != 6:
        raise RuntimeError(
            "Expected 6 M7 analyses"
        )

    fee_rate = dec(
        spec[
            "fee_contract"
        ][
            "rate"
        ]
    )

    validate_trade_arithmetic(
        trades,
        fee_rate,
    )

    expected = {
        "sharp_60s_econ_primary":
            (20, 17),

        "sharp_60s_econ_hedge60":
            (10, 10),

        "sharp_60s_econ_hedge120":
            (15, 13),

        "sharp_60s_econ_no_size_gate":
            (22, 19),

        "sharp_gap_60s_econ_secondary":
            (16, 14),

        "rn1_30s_econ_oracle":
            (20, 7),
    }

    by_id = {
        row[
            "analysis_id"
        ]: row
        for row in summary
    }

    if set(by_id) != set(expected):
        raise RuntimeError(
            "Unexpected M7 analysis set"
        )

    for analysis_id, (
        expected_events,
        expected_markets,
    ) in expected.items():

        row = by_id[
            analysis_id
        ]

        if int(
            row[
                "event_count"
            ]
        ) != expected_events:
            raise RuntimeError(
                f"{analysis_id}: "
                "event count mismatch"
            )

        if int(
            row[
                "market_count"
            ]
        ) != expected_markets:
            raise RuntimeError(
                f"{analysis_id}: "
                "market count mismatch"
            )

    if (
        by_id[
            "sharp_60s_econ_primary"
        ][
            "analysis_class"
        ]
        != "primary"
    ):
        raise RuntimeError(
            "Primary classification mismatch"
        )

    if (
        by_id[
            "sharp_gap_60s_econ_secondary"
        ][
            "analysis_class"
        ]
        != "secondary"
    ):
        raise RuntimeError(
            "Sharp-gap classification mismatch"
        )

    if (
        by_id[
            "rn1_30s_econ_oracle"
        ][
            "analysis_class"
        ]
        != "oracle"
    ):
        raise RuntimeError(
            "RN1 oracle classification mismatch"
        )

    if manifest[
        "decisions"
    ][
        "historically_executable_strategy_demonstrated"
    ] is not False:
        raise RuntimeError(
            "Executable-strategy claim must remain false"
        )

    if manifest[
        "decisions"
    ][
        "rn1_live_strategy_supported"
    ] is not False:
        raise RuntimeError(
            "RN1 live-actionability claim must remain false"
        )

    return (
        summary,
        manifest,
        markets,
    )


def print_results(
    summary,
    manifest,
):
    print()
    print(
        "H2-v1 M7 economic results"
    )
    print(
        "=========================="
    )

    primary_id = manifest[
        "primary_analysis_id"
    ]

    print()
    print("PRIMARY ECONOMIC PROXY")
    print()

    for row in summary:

        if (
            row[
                "analysis_id"
            ]
            != primary_id
        ):
            continue

        print(
            row[
                "analysis_id"
            ]
        )

        print(
            f"  n={row['event_count']} trades / "
            f"{row['market_count']} markets"
        )

        print(
            "  mean gross/share: "
            f"{float(row['event_weighted_mean_gross_per_share']):+.6f}"
        )

        print(
            "  mean platform fees/share: "
            f"{float(row['event_weighted_mean_fee_per_share']):.6f}"
        )

        print(
            "  event-weighted mean net/share: "
            f"{float(row['event_weighted_mean_net_per_share']):+.6f}"
        )

        print(
            "  event-weighted median net/share: "
            f"{float(row['event_weighted_median_net_per_share']):+.6f}"
        )

        print(
            "  market-equal mean net/share: "
            f"{float(row['market_equal_mean_net_per_share']):+.6f}"
        )

        print(
            "  positive/zero/negative trades: "
            f"{row['positive_event_count']}/"
            f"{row['zero_event_count']}/"
            f"{row['negative_event_count']}"
        )

        print(
            "  positive/zero/negative markets: "
            f"{row['positive_market_count']}/"
            f"{row['zero_market_count']}/"
            f"{row['negative_market_count']}"
        )

        print(
            "  mean 5-share net: "
            f"{float(row['mean_five_share_net']):+.6f}"
        )

        print(
            "  mean return on pair cost: "
            f"{100 * float(row['mean_return_on_total_pair_cost']):+.3f}%"
        )

    print()
    print(
        "SENSITIVITIES / SECONDARY / ORACLE"
    )
    print()

    for row in summary:

        if (
            row[
                "analysis_id"
            ]
            == primary_id
        ):
            continue

        print(
            f"{row['analysis_id']:34s} "
            f"n={int(row['event_count']):2d}/"
            f"{int(row['market_count']):2d}m "
            f"market-net="
            f"{float(row['market_equal_mean_net_per_share']):+.6f} "
            f"event-net="
            f"{float(row['event_weighted_mean_net_per_share']):+.6f}"
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
        "trades SHA-256: "
        + manifest[
            "artifacts"
        ][
            "trades"
        ][
            "sha256"
        ]
    )

    print(
        "market effects SHA-256: "
        + manifest[
            "artifacts"
        ][
            "market_effects"
        ][
            "sha256"
        ]
    )

    print(
        "summary SHA-256: "
        + manifest[
            "artifacts"
        ][
            "summary"
        ][
            "sha256"
        ]
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
                TRADES_CSV,
                MARKET_EFFECTS_CSV,
                SUMMARY_CSV,
                MANIFEST_JSON,
            )
            if path.exists()
        ]

        if existing:
            raise SystemExit(
                "REFUSING TO RE-BUILD: "
                "M7 result artifact(s) already exist: "
                + ", ".join(
                    str(path)
                    for path in existing
                )
                + ". Use --validate instead."
            )

        summary, manifest = (
            build()
        )

        print_results(
            summary,
            manifest,
        )

        print()
        print("BUILD COMPLETE")

        return

    (
        summary,
        manifest,
        _markets,
    ) = validate()

    print_results(
        summary,
        manifest,
    )

    print()
    print("VALIDATION PASSED")


if __name__ == "__main__":
    main()
