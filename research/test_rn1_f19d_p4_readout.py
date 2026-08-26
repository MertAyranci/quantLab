from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import sys

import pytest

sys.path.insert(
    0,
    str(Path(__file__).resolve().parent),
)

import rn1_f19d_p2_core as core
import rn1_f19d_p4_readout as p4


D = Decimal
BASE = datetime(
    2026,
    1,
    1,
    tzinfo=timezone.utc,
)
RN1 = "0x" + "1" * 40
OTHER = "0x" + "2" * 40


def raw_row(
    *,
    condition: str,
    wallet: str,
    side: str,
    timestamp: int,
    price: str = "0.50",
    size: str = "10",
    asset: str = "123",
    tx: str = "0xabc",
    outcome_index: int = 0,
):
    return {
        "proxyWallet": wallet,
        "side": side,
        "asset": asset,
        "conditionId": condition,
        "size": size,
        "price": price,
        "timestamp": timestamp,
        "transactionHash": tx,
        "outcome": "Team A",
        "outcomeIndex": outcome_index,
    }


def synthetic_result(
    *,
    condition: str,
    difference: Decimal | None,
    offset_seconds: int,
    matched: bool = True,
) -> core.MatchedResult:
    trade = core.Trade(
        proxy_wallet=RN1,
        side="BUY",
        asset="123",
        condition_id=condition,
        size=D("10"),
        price=D("0.50"),
        timestamp=(
            BASE
            + timedelta(
                seconds=offset_seconds
            )
        ),
        transaction_hash=(
            "0x"
            + condition[-4:]
            + str(offset_seconds)
        ),
        outcome="Team A",
        outcome_index=0,
    )

    flagged = core.MakerOpportunity(
        trade=trade,
        game_start_time=(
            BASE
            + timedelta(hours=5)
        ),
        flagged=True,
    )

    selected = (
        (flagged,)
        if matched
        else ()
    )

    if difference is None:
        flagged_markout = None
        controls = ()
    else:
        flagged_markout = difference
        controls = (D("0"),)

    return core.MatchedResult(
        flagged=flagged,
        selected_controls=selected,
        flagged_markout=flagged_markout,
        valid_control_markouts=controls,
    )


def result_panel(
    *,
    markets: int,
    events_per_market: int,
    difference: Decimal,
):
    out = []

    for market in range(markets):
        condition = (
            "0x"
            + f"{market + 1:064x}"
        )

        for event in range(
            events_per_market
        ):
            out.append(
                synthetic_result(
                    condition=condition,
                    difference=difference,
                    offset_seconds=(
                        market * 10_000
                        + event * 120
                    ),
                )
            )

    return out


def test_fingerprint_matches_frozen_algorithm():
    condition = "0x" + "a" * 64

    rows = [
        raw_row(
            condition=condition,
            wallet=OTHER,
            side="BUY",
            timestamp=1_700_000_000,
            price="0.42",
            size="10.5",
            tx="0xabc",
        ),
        raw_row(
            condition=condition,
            wallet="0x" + "3" * 40,
            side="SELL",
            timestamp=1_700_000_001,
            price="0.43",
            size="7",
            tx="0xdef",
        ),
    ]

    assert p4.fingerprint_rows(rows) == (
        "0f74bd3891d75a7b20e9655f7d52fe10"
        "b552b6de9f4ad2392a6399266b51d4b6"
    )


def test_flatten_pass_rows():
    payload = {
        "pages": [
            {
                "rows": [
                    {"a": 1},
                    {"a": 2},
                ]
            },
            {
                "rows": [
                    {"a": 3},
                ]
            },
        ]
    }

    assert p4.flatten_pass_rows(
        payload
    ) == [
        {"a": 1},
        {"a": 2},
        {"a": 3},
    ]


def test_flatten_pass_rejects_nonlist_rows():
    with pytest.raises(RuntimeError):
        p4.flatten_pass_rows(
            {
                "pages": [
                    {
                        "rows":
                            {"bad": True}
                    }
                ]
            }
        )


def test_summary_strongly_confirmed():
    results = result_panel(
        markets=20,
        events_per_market=3,
        difference=D("-0.01"),
    )

    got = p4.summarize_results(
        results
    )

    assert (
        got["matched_flagged_events"]
        == 60
    )

    assert (
        got["usable_markets"]
        == 20
    )

    assert (
        got["primary_market_equal"]
        == "-0.01"
    )

    assert (
        got["declustered_market_equal"]
        == "-0.01"
    )

    assert (
        got["bootstrap_95_ci"][
            "upper"
        ]
        == "-0.01"
    )

    assert (
        got["status"]
        == "STRONGLY_CONFIRMED"
    )


def test_summary_insufficient_market_breadth():
    results = result_panel(
        markets=19,
        events_per_market=3,
        difference=D("-0.01"),
    )

    got = p4.summarize_results(
        results
    )

    assert (
        got["matched_flagged_events"]
        == 57
    )

    assert (
        got["usable_markets"]
        == 19
    )

    assert (
        got["status"]
        == "INCONCLUSIVE_INSUFFICIENT_SAMPLE"
    )


def test_summary_not_confirmed():
    results = result_panel(
        markets=20,
        events_per_market=3,
        difference=D("0.01"),
    )

    got = p4.summarize_results(
        results
    )

    assert (
        got["status"]
        == "NOT_CONFIRMED"
    )


def test_matched_count_not_replaced_by_usable_count():
    results = result_panel(
        markets=20,
        events_per_market=3,
        difference=D("-0.01"),
    )

    results[0] = synthetic_result(
        condition=(
            "0x"
            + f"{1:064x}"
        ),
        difference=None,
        offset_seconds=0,
        matched=True,
    )

    got = p4.summarize_results(
        results
    )

    assert (
        got["matched_flagged_events"]
        == 60
    )

    assert (
        got[
            "usable_matched_flagged_events"
        ]
        == 59
    )


def test_end_to_end_single_market_synthetic():
    condition = "0x" + "a" * 64
    base = 1_700_000_000

    registry = [
        {
            "condition_id":
                condition,

            "game_start_time_utc":
                datetime.fromtimestamp(
                    base + 2_000,
                    tz=timezone.utc,
                ).isoformat(),
        }
    ]

    market_taker = [
        # Adverse non-self flow inside
        # maker-1 frozen signal window.
        raw_row(
            condition=condition,
            wallet=OTHER,
            side="SELL",
            timestamp=base + 970,
            price="0.50",
            tx="0xflow",
        ),

        # Flagged maker future print:
        # 0.50 -> 0.49 in maker inventory direction.
        raw_row(
            condition=condition,
            wallet=OTHER,
            side="SELL",
            timestamp=base + 1_016,
            price="0.49",
            tx="0xfuture1",
        ),

        # Control future print remains 0.50.
        raw_row(
            condition=condition,
            wallet=OTHER,
            side="SELL",
            timestamp=base + 1_316,
            price="0.50",
            tx="0xfuture2",
        ),
    ]

    rn1_all = [
        # Flagged RN1 maker fill.
        raw_row(
            condition=condition,
            wallet=RN1,
            side="BUY",
            timestamp=base + 1_000,
            price="0.50",
            tx="0xmaker1",
        ),

        # Same-market, same-phase,
        # same-direction unflagged control.
        raw_row(
            condition=condition,
            wallet=RN1,
            side="BUY",
            timestamp=base + 1_300,
            price="0.50",
            tx="0xmaker2",
        ),
    ]

    summary, diagnostics = (
        p4.analyze_cohort_rows(
            registry_rows=registry,
            rows_by_condition={
                condition: {
                    "market_taker":
                        market_taker,
                    "rn1_all":
                        rn1_all,
                }
            },
            rn1_wallet=RN1,
        )
    )

    assert (
        diagnostics[
            condition
        ][
            "flagged_maker_events"
        ]
        == 1
    )

    assert (
        diagnostics[
            condition
        ][
            "unflagged_maker_events"
        ]
        == 1
    )

    assert (
        summary[
            "matched_flagged_events"
        ]
        == 1
    )

    assert (
        summary[
            "usable_matched_flagged_events"
        ]
        == 1
    )

    assert (
        summary[
            "primary_market_equal"
        ]
        == "-0.01"
    )

    assert (
        summary["status"]
        == "INCONCLUSIVE_INSUFFICIENT_SAMPLE"
    )


def test_config_validation_is_static_and_passes():
    contract = p4.validate_config()

    assert (
        contract["study"]
        == "RN1-F19d-P4"
    )


def test_implementation_has_no_network_or_db_client():
    source = Path(
        p4.__file__
    ).read_text(
        encoding="utf-8"
    )

    forbidden = (
        "import httpx",
        "import requests",
        "import psycopg2",
        "urllib.request",
        "gamma-api.polymarket.com",
        "data-api.polymarket.com",
        "clob.polymarket.com",
    )

    for token in forbidden:
        assert token not in source


def test_authoritative_invocation_claim_is_create_only(
    tmp_path,
    monkeypatch,
):
    repo = tmp_path

    receipt = (
        repo
        / "research"
        / "rn1_f19d_p4_activation_receipt.json"
    )

    receipt.parent.mkdir(
        parents=True
    )

    receipt.write_text(
        '{"status":"synthetic"}\n',
        encoding="utf-8",
    )

    monkeypatch.setattr(
        p4,
        "git_head",
        lambda repo: "synthetic-head",
    )

    sentinel = (
        p4.claim_authoritative_invocation(
            repo
        )
    )

    assert sentinel.is_file()

    payload = p4.read_json(
        sentinel
    )

    assert (
        payload["status"]
        == "AUTHORITATIVE_INVOCATION_CLAIMED"
    )

    assert (
        payload[
            "analysis_boundary_at_claim"
        ][
            "trade_json_parsed"
        ]
        is False
    )

    with pytest.raises(
        RuntimeError,
        match="rerun prohibited",
    ):
        p4.claim_authoritative_invocation(
            repo
        )


def test_authoritative_invocation_claim_requires_receipt(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(
        p4,
        "git_head",
        lambda repo: "synthetic-head",
    )

    with pytest.raises(
        RuntimeError,
        match="activation receipt missing",
    ):
        p4.claim_authoritative_invocation(
            tmp_path
        )
