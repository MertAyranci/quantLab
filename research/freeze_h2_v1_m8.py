"""Freeze the H2-v1 conclusion and H2-v2 forward OOS protocol."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


R = Path(__file__).resolve().parent

M6_RESULTS = R / "h2_v1_m6_results.csv"
M7_SPEC = R / "h2_v1_m7_spec.json"
M7_SUMMARY = R / "h2_v1_m7_summary.csv"
M7_TRADES = R / "h2_v1_m7_trades.csv"

DECISION = R / "h2_v1_m8_decision.json"
MANIFEST = R / "h2_v1_m8_manifest.json"


EXPECTED = {
    "m6_results": (
        "25392ba88b660aeefab3861615d4fb165"
        "eebb9ecfe5683e898e6168c136148de"
    ),
    "m7_spec": (
        "9ccd61fb85ed10696f40456e2640c9da"
        "f9442ca2297057c695d15032f495fe1d"
    ),
    "m7_summary": (
        "b6656666b1ccb7fddaf0931600fa6adc"
        "2d5c773320c14ed57f92ff55752a8d2a"
    ),
    "m7_trades": (
        "033027a7e446e8cc0e75a09a8bded764"
        "4ca97c4191ac7559ec8b26eca449fd7e"
    ),
}


def sha(path):
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


for name, path in {
    "m6_results": M6_RESULTS,
    "m7_spec": M7_SPEC,
    "m7_summary": M7_SUMMARY,
    "m7_trades": M7_TRADES,
}.items():

    actual = sha(path)

    if actual != EXPECTED[name]:
        raise RuntimeError(
            f"{name} SHA mismatch\n"
            f"expected={EXPECTED[name]}\n"
            f"actual={actual}"
        )


for path in (DECISION, MANIFEST):
    if path.exists():
        raise SystemExit(
            f"REFUSING TO OVERWRITE: {path}"
        )


decision = {
    "version": "H2-v1-M8",

    "frozen_at_utc": datetime.now(
        timezone.utc
    ).isoformat(),

    "h2_v1_final_conclusion": {
        "rn1_primary_statistically_supported": False,
        "sharp_primary_statistically_supported": False,
        "positive_economic_proxy": False,
        "rn1_historically_live_actionable": False,

        "disposition": (
            "Reject H2-v1 RN1/sharp directional taker-lag strategy "
            "as a deployable trading strategy."
        ),

        "important_secondary_result": (
            "Sharp-PM fair-value gap showed evidence of subsequent "
            "Polymarket convergence, motivating H2-v2."
        ),
    },

    "rn1_decision": {
        "required_for_h2_v2": False,
        "primary_signal": False,
        "role": "optional secondary confirmation feature",

        "reason": (
            "H2-v1 did not establish RN1 as necessary or independently "
            "tradeable, and historical RN1 collection was not actionable."
        ),

        "future_use_condition": (
            "RN1 may be added only as a prespecified secondary feature "
            "after a genuinely real-time collector is demonstrated."
        ),

        "real_time_quality_gate": {
            "target_p90_capture_lag_seconds": 5,
            "failure_consequence": (
                "RN1 remains excluded from live signal generation."
            ),
        },
    },

    "h2_v2": {
        "name": (
            "External fair-value dislocation versus "
            "Polymarket executable price"
        ),

        "primary_hypothesis": (
            "When Polymarket's executable outcome price is sufficiently "
            "below a fresh external sharp-market fair probability after "
            "accounting for taker fees, buying that outcome and holding "
            "to resolution produces positive out-of-sample net PnL."
        ),

        "universe": {
            "sport": "MLB",
            "market_type": "pregame moneyline",
            "in_play_allowed": False,
            "one_canonical_polymarket_market_per_game": True,
        },

        "rn1_used_in_primary_model": False,

        "external_fair_value": {
            "preferred_families": [
                "Betfair",
                "Matchbook",
                "Smarkets",
            ],

            "required_fresh_families": 2,

            "family_probability": (
                "midpoint in probability space between the best "
                "available back and lay prices"
            ),

            "consensus": (
                "median of available fresh family probabilities"
            ),

            "stale_carry_forward": False,

            "target_max_source_age_seconds": 10,

            "requirements": [
                "capture best back",
                "capture best lay",
                "capture source timestamp",
                "capture local receive timestamp",
            ],
        },

        "polymarket_execution_data": {
            "source": "live CLOB",

            "required_fields": [
                "best bid",
                "best ask",
                "bid depth",
                "ask depth",
                "tick size",
                "minimum order size",
                "market fee schedule",
                "exchange/event timestamp where available",
                "local receive timestamp",
            ],

            "book_capture": (
                "capture every available real-time book update rather "
                "than periodic historical snapshots"
            ),
        },

        "primary_strategy": {
            "style": "single-leg taker value trade",

            "signal_edge_formula": (
                "fair_probability_of_outcome "
                "- executable_ask_price "
                "- taker_fee_per_share"
            ),

            "entry_direction": (
                "buy only the outcome with positive frozen-threshold edge"
            ),

            "positions_per_market": 1,

            "position_size_shares": 5,

            "entry_execution_model": (
                "paper fill uses the executable ask observed after the "
                "prespecified latency allowance, never the earlier "
                "decision-time quote"
            ),

            "paper_execution_latency_ms": 500,

            "exit": "hold to market resolution",

            "resolution_payout_per_share": {
                "winner": 1,
                "loser": 0,
            },

            "realized_net_pnl_formula": (
                "resolution_payout "
                "- executed_entry_price "
                "- actual_market_entry_fee"
            ),

            "second_trading_fee": False,
            "forced_short_horizon_exit": False,
        },

        "why_hold_to_resolution": (
            "The primary question is whether external fair value identifies "
            "underpriced contracts. Holding to resolution avoids forcing a "
            "second spread/fee crossing that destroyed H2-v1 scalp economics."
        ),

        "calibration_phase": {
            "eligible_markets": 60,

            "purpose": [
                "verify collector quality",
                "measure actual latency",
                "select the edge threshold",
                "validate fair-value construction",
            ],

            "candidate_edge_thresholds": [
                0.005,
                0.010,
                0.015,
                0.020,
                0.025,
                0.030,
                0.040,
                0.050,
            ],

            "threshold_selection": (
                "Among thresholds producing at least 20 calibration trades, "
                "choose the threshold with the highest market-equal realized "
                "after-fee PnL per share."
            ),

            "no_go_rule": (
                "If no candidate threshold with at least 20 trades has "
                "positive calibration mean net PnL, do not start the "
                "confirmatory OOS test without declaring a new hypothesis."
            ),

            "oos_data_may_not_be_used": True,
        },

        "oos_phase": {
            "eligible_markets": 100,

            "starts": (
                "Immediately after calibration is completed and the "
                "threshold/configuration is cryptographically frozen."
            ),

            "parameter_changes_allowed": False,

            "primary_unit": "market",

            "maximum_primary_positions_per_market": 1,

            "minimum_completed_trades_for_conclusion": 30,

            "primary_metric": (
                "market-equal mean realized after-fee PnL per share"
            ),

            "secondary_metrics": [
                "total fixed-5-share net PnL",
                "median net PnL per trade",
                "win rate",
                "mean predicted edge",
                "calibration of external fair probability",
                "Brier score",
            ],

            "primary_success_rule": (
                "At least 30 completed primary trades and positive "
                "market-equal mean after-fee PnL per share."
            ),

            "strong_confirmation_rule": (
                "Primary success rule plus a market-level bootstrap "
                "95% confidence interval whose lower bound exceeds zero."
            ),

            "insufficient_trade_rule": (
                "Fewer than 30 completed primary trades after 100 eligible "
                "markets is classified as inconclusive, not failure."
            ),
        },

        "maker_branch": {
            "status": "secondary separate experiment",
            "may_replace_primary_post_hoc": False,

            "purpose": (
                "Test whether passive execution improves economics "
                "after the taker-value hypothesis is independently assessed."
            ),

            "requirements": [
                "live order-book depth",
                "queue-aware conservative fill model",
                "trade tape",
                "quote placement/cancellation timestamps",
                "adverse-selection measurement",
            ],
        },

        "rn1_secondary_branch": {
            "status": "optional secondary experiment",

            "question": (
                "Does genuinely real-time RN1 flow improve prediction or "
                "trade selection conditional on fair-value dislocation?"
            ),

            "primary_result_may_not_depend_on_it": True,
        },

        "future_separate_hypotheses": [
            "cross-market arbitrage",
            "logical consistency arbitrage",
            "maker market-making",
            "news/lineup latency",
            "multi-wallet informed-flow models",
        ],
    },

    "research_rules": [
        "Do not use RN1 as a required H2-v2 input.",
        "Do not use historical H2-v1 outcomes to select the H2-v2 threshold.",
        "Do not alter the OOS threshold after the first OOS market begins.",
        "Do not count multiple primary trades in one market.",
        "Do not infer executable prices from trade prints when live book data exists.",
        "Always use the market-specific fee schedule.",
        "Do not call paper fills realized fills.",
        "Do not promote maker or RN1 secondary analyses after seeing a failed primary result.",
    ],
}


DECISION.write_text(
    json.dumps(
        decision,
        indent=2,
        sort_keys=True,
    ) + "\n",
    encoding="utf-8",
)


manifest = {
    "version": "H2-v1-M8",

    "parents": {
        "m6_results_sha256":
            EXPECTED["m6_results"],

        "m7_spec_sha256":
            EXPECTED["m7_spec"],

        "m7_summary_sha256":
            EXPECTED["m7_summary"],

        "m7_trades_sha256":
            EXPECTED["m7_trades"],
    },

    "artifacts": {
        "decision": {
            "file":
                DECISION.name,

            "sha256":
                sha(DECISION),
        },
    },

    "final_decisions": {
        "h2_v1_deployable_strategy_supported": False,
        "rn1_required_for_h2_v2": False,
        "h2_v2_primary_is_fair_value_dislocation": True,
        "h2_v2_primary_holds_to_resolution": True,
        "live_clob_required": True,
        "maker_is_secondary": True,
    },
}


MANIFEST.write_text(
    json.dumps(
        manifest,
        indent=2,
        sort_keys=True,
    ) + "\n",
    encoding="utf-8",
)


print("H2-M8 FREEZE COMPLETE")
print("decision SHA:", sha(DECISION))
print("manifest SHA:", sha(MANIFEST))
