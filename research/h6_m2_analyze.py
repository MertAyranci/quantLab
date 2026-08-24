from __future__ import annotations

import argparse
import bisect
import hashlib
import json
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import numpy as np
import statsmodels.api as sm


REPO = Path(__file__).resolve().parents[1]

if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from research import h6_m1_validate as core  # noqa: E402
from research import h6_m2_validate as validator  # noqa: E402


CAPTURE_ID = "h6m2_dev_20260822T223600Z"

EXPECTED_READOUT_CONTRACT_SHA = (
    "afca061d959bac4c4cc30c6705213514"
    "47fc1c8508f73f4285fd75e3614ebc71"
)

EXPECTED_VALIDATOR_SHA = (
    "faba97d57706bbbf399e3d8462e22324"
    "37d471e506d570ddeefbe3072b5ee6e8"
)

EXPECTED_M1_REPLAY_CORE_SHA = (
    "24d5731555dd3eab1adf5fb4fbc19f39"
    "c9b4692d0a06bf35ca87a62089549c1c"
)

HORIZONS = [1, 3, 5, 10, 30, 60]

BOOTSTRAP_REPLICATES = 10000
BOOTSTRAP_SEED = 20260820

ADJACENT_PAIRS = [
    (1, 3),
    (3, 5),
    (5, 10),
    (10, 30),
    (30, 60),
]


def sha256_file(path: Path) -> str:
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def git_head(repo: Path = REPO) -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
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
            ["git", "show", f"HEAD:{rel}"],
            cwd=repo,
        )

    except (
        ValueError,
        subprocess.CalledProcessError,
    ):
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


def validate_static_inputs(
    repo: Path = REPO,
) -> dict[str, Any]:
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
            "H6-M2 readout contract SHA mismatch"
        )

    validator_path = (
        repo
        / "research"
        / "h6_m2_validate.py"
    )

    if (
        sha256_file(validator_path)
        != EXPECTED_VALIDATOR_SHA
    ):
        raise RuntimeError(
            "H6-M2 validator SHA mismatch"
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
            "H6-M1 replay core SHA mismatch"
        )

    contract = read_json(
        contract_path
    )

    if (
        contract[
            "scientific_readout"
        ][
            "candidate_horizons_seconds"
        ]
        != HORIZONS
    ):
        raise RuntimeError(
            "horizon freeze mismatch"
        )

    bootstrap = contract[
        "cluster_bootstrap"
    ]

    if (
        bootstrap["replicates"]
        != BOOTSTRAP_REPLICATES
        or
        bootstrap["random_seed"]
        != BOOTSTRAP_SEED
        or
        bootstrap["statistic"]
        !=
        "market-equal mean directional response"
    ):
        raise RuntimeError(
            "bootstrap freeze mismatch"
        )

    expected_pairs = [
        list(pair)
        for pair in ADJACENT_PAIRS
    ]

    if (
        contract[
            "development_verdict"
        ][
            "adjacent_horizon_pairs"
        ]
        != expected_pairs
    ):
        raise RuntimeError(
            "adjacent-pair freeze mismatch"
        )

    return contract


@dataclass(frozen=True)
class StatePoint:
    time: datetime
    serial: int
    valid: bool
    connection_id: str
    generation: int
    bid: Decimal | None
    ask: Decimal | None
    bid_size: Decimal | None
    ask_size: Decimal | None

    @property
    def midpoint(
        self,
    ) -> Decimal | None:
        if (
            not self.valid
            or self.bid is None
            or self.ask is None
        ):
            return None

        return (
            self.bid
            + self.ask
        ) / Decimal("2")


class ScientificReplay(
    core.Replay
):
    """
    The frozen H6-M1 replay engine remains
    authoritative for reconstruction and
    transition eligibility.

    This subclass only records causal p1
    state-history points and enriches the
    already-eligible primary transitions
    with post-frame midpoint information.
    """

    def __init__(
        self,
        registry_rows,
        raw_rows,
        rest_rows,
        control_rows=None,
    ):
        super().__init__(
            registry_rows,
            raw_rows,
            rest_rows,
            control_rows,
        )

        self.event_serial = 0

        self.history: dict[
            str,
            list[StatePoint],
        ] = defaultdict(list)

        self.last_signature: dict[
            str,
            tuple[Any, ...],
        ] = {}

        self.scientific_transitions: list[
            dict[str, Any]
        ] = []

    def _state_point(
        self,
        token: str,
        *,
        when: datetime,
        serial: int,
    ) -> StatePoint:
        state = self.states[token]

        (
            bid,
            bid_size,
            ask,
            ask_size,
        ) = state.best()

        return StatePoint(
            time=when,
            serial=serial,
            valid=state.structurally_valid(),
            connection_id=state.connection_id,
            generation=state.generation,
            bid=bid,
            ask=ask,
            bid_size=bid_size,
            ask_size=ask_size,
        )

    @staticmethod
    def _signature(
        point: StatePoint,
    ) -> tuple[Any, ...]:
        return (
            point.valid,
            point.connection_id,
            point.generation,
            point.bid,
            point.ask,
            point.bid_size,
            point.ask_size,
        )

    def _record_p1_states(
        self,
        when: datetime,
    ) -> None:
        for token in (
            self.p1_by_condition.values()
        ):
            point = self._state_point(
                token,
                when=when,
                serial=self.event_serial,
            )

            signature = self._signature(
                point
            )

            if (
                self.last_signature.get(token)
                != signature
            ):
                self.history[
                    token
                ].append(point)

                self.last_signature[
                    token
                ] = signature

    def apply_rest_batch(
        self,
        event,
    ) -> None:
        self.event_serial += 1

        super().apply_rest_batch(
            event
        )

        self._record_p1_states(
            event["time"]
        )

    def apply_raw_frame(
        self,
        event,
    ) -> None:
        self.event_serial += 1

        super().apply_raw_frame(
            event
        )

        self._record_p1_states(
            event["time"]
        )

    def _maybe_transition(
        self,
        *,
        token,
        pre,
        post,
        capture_time,
        connection_id,
        frame_sequence,
        mismatch,
    ) -> None:
        before = len(
            self.transitions
        )

        super()._maybe_transition(
            token=token,
            pre=pre,
            post=post,
            capture_time=capture_time,
            connection_id=connection_id,
            frame_sequence=frame_sequence,
            mismatch=mismatch,
        )

        if (
            len(self.transitions)
            == before
        ):
            return

        bid = post["bid"]
        ask = post["ask"]

        if (
            bid is None
            or ask is None
        ):
            raise RuntimeError(
                "eligible transition missing midpoint"
            )

        base = dict(
            self.transitions[-1]
        )

        base.update(
            {
                "event_serial":
                    self.event_serial,

                "event_midpoint":
                    str(
                        (
                            bid + ask
                        )
                        / Decimal("2")
                    ),

                "event_bid":
                    str(bid),

                "event_ask":
                    str(ask),

                "event_connection_id":
                    str(
                        post[
                            "connection_id"
                        ]
                    ),

                "event_generation":
                    int(
                        post[
                            "generation"
                        ]
                    ),
            }
        )

        self.scientific_transitions.append(
            base
        )

    def run_scientific(
        self,
    ) -> dict[str, Any]:
        engineering = super().replay()

        return {
            "engineering":
                engineering,

            "transitions":
                list(
                    self.scientific_transitions
                ),

            "history":
                {
                    token:
                        list(points)

                    for token, points
                    in self.history.items()
                },
        }


def state_as_of_target(
    *,
    points: list[StatePoint],
    transition: dict[str, Any],
    target: datetime,
) -> StatePoint | None:
    if not points:
        return None

    event_time = core.parse_dt(
        transition[
            "capture_time"
        ]
    )

    event_serial = int(
        transition[
            "event_serial"
        ]
    )

    event_conn = str(
        transition[
            "event_connection_id"
        ]
    )

    event_generation = int(
        transition[
            "event_generation"
        ]
    )

    keys = [
        (
            point.time,
            point.serial,
        )
        for point in points
    ]

    target_index = (
        bisect.bisect_right(
            keys,
            (
                target,
                10**18,
            ),
        )
        - 1
    )

    if target_index < 0:
        return None

    target_point = points[
        target_index
    ]

    if (
        not target_point.valid
        or
        target_point.connection_id
        != event_conn
        or
        target_point.generation
        != event_generation
    ):
        return None

    event_key = (
        event_time,
        event_serial,
    )

    for point in points:
        key = (
            point.time,
            point.serial,
        )

        if key <= event_key:
            continue

        if point.time > target:
            break

        if (
            not point.valid
            or
            point.connection_id
            != event_conn
            or
            point.generation
            != event_generation
        ):
            return None

    return target_point


def build_observations(
    *,
    replay: ScientificReplay,
    horizons: list[int] = HORIZONS,
) -> dict[
    int,
    list[dict[str, Any]],
]:
    output = {
        horizon: []
        for horizon in horizons
    }

    for transition in (
        replay.scientific_transitions
    ):
        cid = str(
            transition[
                "condition_id"
            ]
        )

        token = str(
            transition["token"]
        )

        event_time = core.parse_dt(
            transition[
                "capture_time"
            ]
        )

        game_start = (
            replay.markets[
                cid
            ].game_start
        )

        event_midpoint = Decimal(
            str(
                transition[
                    "event_midpoint"
                ]
            )
        )

        imbalance_change = Decimal(
            str(
                transition[
                    "imbalance_change"
                ]
            )
        )

        if imbalance_change == 0:
            raise RuntimeError(
                "zero imbalance transition "
                "entered scientific replay"
            )

        direction = (
            Decimal("1")
            if imbalance_change > 0
            else Decimal("-1")
        )

        points = replay.history.get(
            token,
            [],
        )

        for horizon in horizons:
            target = (
                event_time
                + timedelta(
                    seconds=horizon
                )
            )

            if target >= game_start:
                continue

            point = state_as_of_target(
                points=points,
                transition=transition,
                target=target,
            )

            if point is None:
                continue

            midpoint = point.midpoint

            if midpoint is None:
                continue

            raw_change = (
                midpoint
                - event_midpoint
            )

            directional = (
                direction
                * raw_change
            )

            output[
                horizon
            ].append(
                {
                    "condition_id":
                        cid,

                    "token":
                        token,

                    "event_time":
                        event_time,

                    "target_time":
                        target,

                    "imbalance_change":
                        float(
                            imbalance_change
                        ),

                    "raw_future_midpoint_change":
                        float(
                            raw_change
                        ),

                    "directional_response":
                        float(
                            directional
                        ),
                }
            )

    return output


def market_means(
    observations: list[
        dict[str, Any]
    ],
) -> dict[str, float]:
    grouped: dict[
        str,
        list[float],
    ] = defaultdict(list)

    for row in observations:
        grouped[
            str(
                row[
                    "condition_id"
                ]
            )
        ].append(
            float(
                row[
                    "directional_response"
                ]
            )
        )

    return {
        cid:
            float(
                np.mean(values)
            )

        for cid, values
        in sorted(
            grouped.items()
        )
    }


def bootstrap_market_equal(
    values_by_market: dict[
        str,
        float,
    ],
) -> dict[str, Any]:
    values = np.asarray(
        list(
            values_by_market.values()
        ),
        dtype=float,
    )

    n = len(values)

    if n == 0:
        return {
            "lower": None,
            "upper": None,
            "replicates":
                BOOTSTRAP_REPLICATES,
            "seed":
                BOOTSTRAP_SEED,
            "market_count": 0,
        }

    rng = np.random.default_rng(
        BOOTSTRAP_SEED
    )

    indices = rng.integers(
        0,
        n,
        size=(
            BOOTSTRAP_REPLICATES,
            n,
        ),
    )

    boot = values[
        indices
    ].mean(
        axis=1
    )

    lower, upper = np.quantile(
        boot,
        [0.025, 0.975],
        method="linear",
    )

    return {
        "lower":
            float(lower),

        "upper":
            float(upper),

        "replicates":
            BOOTSTRAP_REPLICATES,

        "seed":
            BOOTSTRAP_SEED,

        "market_count":
            n,
    }


def clustered_ols(
    observations: list[
        dict[str, Any]
    ],
) -> dict[str, Any]:
    if not observations:
        return {
            "slope": None,
            "standard_error": None,
            "ci_lower": None,
            "ci_upper": None,
            "observation_count": 0,
            "market_clusters": 0,
        }

    x = np.asarray(
        [
            float(
                row[
                    "imbalance_change"
                ]
            )
            for row in observations
        ],
        dtype=float,
    )

    y = np.asarray(
        [
            float(
                row[
                    "raw_future_midpoint_change"
                ]
            )
            for row in observations
        ],
        dtype=float,
    )

    groups = np.asarray(
        [
            str(
                row[
                    "condition_id"
                ]
            )
            for row in observations
        ],
        dtype=object,
    )

    cluster_count = len(
        set(
            groups.tolist()
        )
    )

    if (
        len(observations) < 2
        or
        cluster_count < 2
        or
        np.allclose(
            x,
            x[0],
        )
    ):
        return {
            "slope": None,
            "standard_error": None,
            "ci_lower": None,
            "ci_upper": None,
            "observation_count":
                len(observations),
            "market_clusters":
                cluster_count,
        }

    X = sm.add_constant(
        x,
        has_constant="add",
    )

    try:
        fit = sm.OLS(
            y,
            X,
        ).fit(
            cov_type="cluster",
            cov_kwds={
                "groups":
                    groups,

                "use_correction":
                    True,

                "df_correction":
                    True,
            },
        )

        ci = fit.conf_int(
            alpha=0.05
        )

        return {
            "slope":
                float(
                    fit.params[1]
                ),

            "standard_error":
                float(
                    fit.bse[1]
                ),

            "ci_lower":
                float(
                    ci[1, 0]
                ),

            "ci_upper":
                float(
                    ci[1, 1]
                ),

            "observation_count":
                len(observations),

            "market_clusters":
                cluster_count,
        }

    except Exception:
        return {
            "slope": None,
            "standard_error": None,
            "ci_lower": None,
            "ci_upper": None,
            "observation_count":
                len(observations),
            "market_clusters":
                cluster_count,
        }


def lomo_market_equal(
    values_by_market: dict[
        str,
        float,
    ],
) -> dict[str, Any]:
    markets = sorted(
        values_by_market
    )

    omissions: list[
        dict[str, Any]
    ] = []

    if len(markets) <= 1:
        return {
            "omissions": [],
            "positive_fraction": None,
        }

    for omitted in markets:
        remaining = [
            values_by_market[
                market
            ]
            for market in markets
            if market != omitted
        ]

        value = float(
            np.mean(
                remaining
            )
        )

        omissions.append(
            {
                "omitted_market":
                    omitted,

                "market_equal_mean":
                    value,

                "positive":
                    value > 0,
            }
        )

    positive = sum(
        row["positive"]
        for row in omissions
    )

    return {
        "omissions":
            omissions,

        "positive_fraction":
            (
                positive
                / len(omissions)
            ),
    }


def summarize_horizon(
    observations: list[
        dict[str, Any]
    ],
) -> dict[str, Any]:
    responses = np.asarray(
        [
            float(
                row[
                    "directional_response"
                ]
            )
            for row in observations
        ],
        dtype=float,
    )

    by_market = market_means(
        observations
    )

    event_mean = (
        float(
            np.mean(
                responses
            )
        )
        if len(responses)
        else None
    )

    market_equal = (
        float(
            np.mean(
                list(
                    by_market.values()
                )
            )
        )
        if by_market
        else None
    )

    median = (
        float(
            np.median(
                responses
            )
        )
        if len(responses)
        else None
    )

    positive = int(
        np.sum(
            responses > 0
        )
    )

    zero = int(
        np.sum(
            responses == 0
        )
    )

    negative = int(
        np.sum(
            responses < 0
        )
    )

    return {
        "usable_transition_count":
            len(observations),

        "distinct_market_count":
            len(by_market),

        "event_weighted_mean_directional_response":
            event_mean,

        "market_equal_mean_directional_response":
            market_equal,

        "median_directional_response":
            median,

        "positive_response_count":
            positive,

        "zero_response_count":
            zero,

        "negative_response_count":
            negative,

        "market_level_means":
            by_market,

        "market_cluster_bootstrap_95_ci":
            bootstrap_market_equal(
                by_market
            ),

        "continuous_relationship":
            clustered_ols(
                observations
            ),

        "leave_one_market_out":
            lomo_market_equal(
                by_market
            ),
    }


def strong_horizon(
    summary: dict[str, Any],
) -> bool:
    lower = (
        summary[
            "market_cluster_bootstrap_95_ci"
        ][
            "lower"
        ]
    )

    slope = (
        summary[
            "continuous_relationship"
        ][
            "slope"
        ]
    )

    lomo_fraction = (
        summary[
            "leave_one_market_out"
        ][
            "positive_fraction"
        ]
    )

    return bool(
        lower is not None
        and lower > 0
        and slope is not None
        and slope > 0
        and lomo_fraction is not None
        and lomo_fraction >= 0.80
    )


def supportive_horizon(
    summary: dict[str, Any],
) -> bool:
    event_mean = summary[
        "event_weighted_mean_directional_response"
    ]

    market_mean = summary[
        "market_equal_mean_directional_response"
    ]

    return bool(
        event_mean is not None
        and event_mean > 0
        and market_mean is not None
        and market_mean > 0
    )


def development_verdict(
    summaries: dict[
        int,
        dict[str, Any],
    ],
) -> dict[str, Any]:
    supportive_pairs: list[
        list[int]
    ] = []

    strong = {
        horizon:
            strong_horizon(
                summaries[
                    horizon
                ]
            )

        for horizon in HORIZONS
    }

    for (
        left,
        right,
    ) in ADJACENT_PAIRS:
        if (
            supportive_horizon(
                summaries[left]
            )
            and
            supportive_horizon(
                summaries[right]
            )
        ):
            supportive_pairs.append(
                [left, right]
            )

    qualifying = sorted(
        {
            horizon
            for pair
            in supportive_pairs
            for horizon
            in pair
            if strong[
                horizon
            ]
        }
    )

    promising = bool(
        supportive_pairs
        and qualifying
    )

    return {
        "status":
            (
                "PROMISING_DEVELOPMENT"
                if promising
                else
                "NOT_PROMISING_DEVELOPMENT"
            ),

        "supportive_adjacent_pairs":
            supportive_pairs,

        "strong_horizon_flags":
            {
                str(horizon):
                    strong[horizon]

                for horizon in HORIZONS
            },

        "qualifying_strong_horizons":
            qualifying,

        "proposed_m3_primary_horizon_seconds":
            (
                qualifying[0]
                if promising
                else None
            ),
    }


def analyze_replay(
    replay: ScientificReplay,
) -> dict[str, Any]:
    observations = build_observations(
        replay=replay,
        horizons=HORIZONS,
    )

    summaries = {
        horizon:
            summarize_horizon(
                observations[
                    horizon
                ]
            )

        for horizon in HORIZONS
    }

    verdict = development_verdict(
        summaries
    )

    return {
        "horizons": {
            str(horizon):
                summaries[
                    horizon
                ]

            for horizon in HORIZONS
        },

        "development_verdict":
            verdict,
    }


def load_authoritative_inputs(
    repo: Path = REPO,
):
    contract = validate_static_inputs(
        repo
    )

    validator_result_path = (
        repo
        / contract[
            "real_readout_policy"
        ][
            "validator_result_path"
        ]
    )

    if not validator_result_path.is_file():
        raise RuntimeError(
            "H6-M2 engineering result missing"
        )

    validator_result = read_json(
        validator_result_path
    )

    if (
        validator_result.get(
            "status"
        )
        !=
        "PASS_ENGINEERING_AND_DATA_SUFFICIENCY"
        or
        validator_result.get(
            "scientific_analyzer_permitted"
        )
        is not True
    ):
        raise RuntimeError(
            "engineering validator did not "
            "permit scientific analysis"
        )

    if not committed_file_matches_head(
        repo
        / "research"
        / "h6_m2_analyze.py",
        repo,
    ):
        raise RuntimeError(
            "analyzer differs from committed HEAD"
        )

    (
        _contract,
        registry,
        _capture_start,
    ) = validator.validate_static_inputs(
        repo
    )

    (
        complete,
        stream_hashes,
    ) = validator.validate_capture_complete(
        contract,
        repo,
    )

    validator.validate_code_freeze_receipt(
        contract=contract,
        complete=complete,
        stream_hashes=stream_hashes,
        repo=repo,
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

    rest = core.load_jsonl(
        capture_root
        / "rest_snapshots.jsonl"
    )

    control = core.load_jsonl(
        capture_root
        / "control.jsonl"
    )

    return (
        contract,
        validator_result,
        registry,
        raw,
        rest,
        control,
    )


def live_readout(
    repo: Path = REPO,
) -> dict[str, Any]:
    (
        contract,
        _validator_result,
        registry,
        raw,
        rest,
        control,
    ) = load_authoritative_inputs(
        repo
    )

    result_path = (
        repo
        / contract[
            "real_readout_policy"
        ][
            "scientific_result_path"
        ]
    )

    if result_path.exists():
        raise RuntimeError(
            "refusing overwrite of existing "
            "H6-M2 scientific result"
        )

    replay = ScientificReplay(
        registry,
        raw,
        rest,
        control,
    )

    replay_output = (
        replay.run_scientific()
    )

    analysis = analyze_replay(
        replay
    )

    result = {
        "study":
            "H6",

        "milestone":
            "H6-M2-DEVELOPMENT",

        "status":
            analysis[
                "development_verdict"
            ][
                "status"
            ],

        "capture_id":
            CAPTURE_ID,

        "readout_contract_sha256":
            EXPECTED_READOUT_CONTRACT_SHA,

        "validator_result_sha256":
            sha256_file(
                repo
                / contract[
                    "real_readout_policy"
                ][
                    "validator_result_path"
                ]
            ),

        "analyzer_git_head":
            git_head(repo),

        "analyzer_sha256":
            sha256_file(
                repo
                / "research"
                / "h6_m2_analyze.py"
            ),

        "primary_transition_count":
            len(
                replay_output[
                    "transitions"
                ]
            ),

        "horizons":
            analysis[
                "horizons"
            ],

        "development_verdict":
            analysis[
                "development_verdict"
            ],

        "analysis_environment": {
            "numpy":
                np.__version__,

            "statsmodels":
                sm.__version__,
        },

        "analysis_boundary": {
            "winner_used":
                False,

            "settlement_used":
                False,

            "external_odds_used":
                False,

            "f19_trade_content_read":
                False,

            "f20_read":
                False,

            "h5_response_read":
                False,

            "execution_economics_calculated":
                False,

            "markout_calculated":
                False,

            "edge_calculated":
                False,

            "pnl_calculated":
                False,

            "prospective_confirmation_claimed":
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
    contract = validate_static_inputs(
        repo
    )

    validator_result_path = (
        repo
        / contract[
            "real_readout_policy"
        ][
            "validator_result_path"
        ]
    )

    scientific_result_path = (
        repo
        / contract[
            "real_readout_policy"
        ][
            "scientific_result_path"
        ]
    )

    print(
        "H6-M2 ANALYZER CONFIG: PASS"
    )

    print(
        "readout contract SHA:",
        EXPECTED_READOUT_CONTRACT_SHA,
    )

    print(
        "validator SHA:",
        EXPECTED_VALIDATOR_SHA,
    )

    print(
        "horizons:",
        HORIZONS,
    )

    print(
        "bootstrap replicates:",
        BOOTSTRAP_REPLICATES,
    )

    print(
        "bootstrap seed:",
        BOOTSTRAP_SEED,
    )

    print(
        "numpy:",
        np.__version__,
    )

    print(
        "statsmodels:",
        sm.__version__,
    )

    print(
        "engineering result exists:",
        (
            "YES"
            if validator_result_path.is_file()
            else "NO"
        ),
    )

    print(
        "scientific result exists:",
        (
            "YES"
            if scientific_result_path.is_file()
            else "NO"
        ),
    )

    print(
        "real capture streams read: NO"
    )

    print(
        "real future midpoint response calculated: NO"
    )

    print(
        "real bootstrap calculated: NO"
    )

    print(
        "real regression calculated: NO"
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
            "H6-M2 DEVELOPMENT READOUT"
        )

        print(
            "========================================"
        )

        print(
            "status:",
            result["status"],
        )

        print(
            "primary transitions:",
            result[
                "primary_transition_count"
            ],
        )

        print(
            "proposed M3 primary horizon:",
            result[
                "development_verdict"
            ][
                "proposed_m3_primary_horizon_seconds"
            ],
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
