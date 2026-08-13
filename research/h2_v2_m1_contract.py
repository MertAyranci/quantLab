"""Freeze H2-v2 M1 real-time data-plane contract.

This amendment is frozen before:
- H2-v2 calibration
- signal threshold selection
- H2-v2 PnL
- any H2-v2 trading decision

Reason for amendment:
The frozen M8 target source age <=10s is incompatible with the empirically
observed and vendor-documented ~20s pregame exchange refresh cadence of
The Odds API.

The source is retained because back+lay coverage across Betfair,
Matchbook and Smarkets was strong. Freshness is changed to <=25s,
which allows one documented source refresh cycle plus small transport jitter.

This is a data-feasibility amendment, not a performance-driven amendment.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


R = Path("research")

M8 = R / "h2_v1_m8_decision.json"

OUT = R / "h2_v2_m1_data_contract.json"

EXPECTED_M8_SHA = (
    "315c600526eeb935ad6d7511290b6a38d"
    "9c1469cafe2e217e6b097509bf711bc"
)


def sha(path):
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


assert sha(M8) == EXPECTED_M8_SHA

if OUT.exists():
    raise SystemExit(
        f"REFUSING TO OVERWRITE: {OUT}"
    )


contract = {
    "version": "H2-v2-M1-DATA-CONTRACT-1",

    "frozen_at_utc": datetime.now(
        timezone.utc
    ).isoformat(),

    "frozen_before_h2_v2_performance_observation": True,

    "parent": {
        "file": M8.name,
        "sha256": EXPECTED_M8_SHA,
    },

    "amendment": {
        "field": (
            "h2_v2.external_fair_value."
            "target_max_source_age_seconds"
        ),

        "previous_value_seconds": 10,

        "new_value_seconds": 25,

        "reason": (
            "The selected aggregate exchange feed refreshes pregame "
            "exchange prices on approximately a 20-second cadence. "
            "A sustained 10-poll feasibility audit observed source "
            "update intervals around 21 seconds across Betfair, "
            "Matchbook and Smarkets. The change is based only on "
            "source feasibility and was made before H2-v2 outcomes, "
            "PnL or signal-threshold analysis."
        ),

        "performance_driven": False,
    },

    "sharp_source": {
        "provider": "The Odds API",

        "sport": "baseball_mlb",

        "market": "h2h",

        "bookmakers": [
            "betfair_ex_uk",
            "matchbook",
            "smarkets",
        ],

        "required_market_components": [
            "h2h",
            "h2h_lay",
        ],

        "required_fresh_families": 2,

        "maximum_source_age_seconds": 25,

        "stale_carry_forward": False,

        "pregame_only": True,

        "pregame_rule": (
            "capture_time < canonical commence_time"
        ),

        "family_probability": (
            "probability-space midpoint of best back and best lay"
        ),

        "family_formula": (
            "(1/back_decimal + 1/lay_decimal) / 2"
        ),

        "consensus": (
            "median of all valid fresh family probabilities"
        ),

        "timestamps": {
            "source": "bookmaker.last_update",
            "local": "local response receive time",
        },

        "valid_family_requirements": [
            "bookmaker is one of the three frozen exchange families",
            "both h2h and h2h_lay exist",
            "exactly two common team outcomes exist",
            "back and lay prices are positive valid decimal odds",
            "source timestamp exists",
            "source age is between 0 and 25 seconds inclusive",
        ],
    },

    "m1_collection": {
        "purpose": (
            "Validate synchronized sharp and Polymarket CLOB "
            "research data quality only."
        ),

        "sharp_poll_interval_seconds": 30,

        "reason_for_30s_m1_cadence": (
            "M1 is an engineering/data-quality test. Thirty seconds "
            "provides repeated observations while conserving API credits. "
            "This cadence does not freeze the later calibration signal "
            "evaluation cadence."
        ),

        "initial_live_validation_minutes": 30,

        "expected_max_sharp_api_calls": 60,

        "signals_allowed": False,

        "trading_allowed": False,

        "threshold_selection_allowed": False,

        "pnl_calculation_allowed": False,
    },

    "polymarket": {
        "source": "existing quantLab live CLOB WebSocket",

        "required": [
            "best bid",
            "best ask",
            "best bid size",
            "best ask size",
            "full book snapshots",
            "book deltas where available",
            "venue event timestamp where available",
            "local capture timestamp",
            "tick size",
            "market-specific fee schedule",
        ],

        "historical_trade_print_substitution_allowed": False,
    },

    "synchronization": {
        "clock": "UTC",

        "primary_alignment_clock": "local capture time",

        "preserve_source_timestamps": True,

        "do_not_replace_source_time_with_capture_time": True,
    },

    "m1_success_requirements": {
        "sharp": [
            "at least two valid fresh exchange families on >=80% of eligible snapshots",
            "back+lay parsing valid",
            "source and local timestamps preserved",
            "no in-play observations admitted",
        ],

        "polymarket": [
            "both outcome tokens mapped correctly",
            "live bid and ask captured",
            "top-of-book sizes captured",
            "full book snapshot available",
            "market fee schedule available",
        ],

        "joint": [
            "sharp and PM observations can be aligned by local capture time",
            "no signal or PnL logic is required for M1",
        ],
    },

    "explicit_non_decisions": [
        "No trading edge threshold is selected in M1.",
        "No optimal polling cadence for calibration is selected in M1.",
        "No maker versus taker decision is revised in M1.",
        "RN1 is not added to the H2-v2 primary model.",
        "No H2-v2 PnL is calculated in M1.",
    ],
}


OUT.write_text(
    json.dumps(
        contract,
        indent=2,
        sort_keys=True,
    ) + "\n",
    encoding="utf-8",
)


print("H2-v2 M1 DATA CONTRACT FROZEN")
print("M8 SHA:      ", sha(M8))
print("contract SHA:", sha(OUT))
