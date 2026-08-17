from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx


REPO = Path(__file__).resolve().parents[1]

M0 = REPO / "research" / "h6_m0_mechanism_data_design.json"
INDEPENDENCE = REPO / "research" / "h6_m0_independence_policy.json"
CONTRACT = REPO / "research" / "h6_m1_registration_contract.json"

REGISTRY = REPO / "data" / "research" / "h6" / "m1" / "registry.jsonl"
RECEIPT = REPO / "research" / "h6_m1_registry_freeze.json"

SPORTS_URL = "https://gamma-api.polymarket.com/sports"
EVENTS_URL = "https://gamma-api.polymarket.com/events"

EXPECTED_PARENT_HEAD = "c2f274683eb4168442088621df0ea9b315d3379d"
EXPECTED_M0_SHA = "bcbcf5e5ffc72517df5c6162771da62fc11113c2b514b297c9848172444e9213"
EXPECTED_INDEPENDENCE_SHA = "13b58c64e5c63275c533be12272eec2a96a7a71a5da2b79eb3bc52f995637c32"
EXPECTED_CONTRACT_SHA = "34e43b9b69e9988ea92728a7d7c08ae2063505171b54d3d8b19fa64557eb9dd8"

SPORT = "mlb"
TARGET_MARKETS = 5
MIN_MARKETS = 5
M0_MIN_LEAD_SECONDS = 1800
REGISTRATION_LEAD_SECONDS = 3600
PAGE_LIMIT = 100
MAX_EVENT_PAGES = 20

MAPPING_RULE = (
    "gamma_mlb_primary_tag+moneyline+2_outcomes+2_tokens+"
    "active_open_accepting+question_outcome_team_set_match"
)

UA = {"User-Agent": "quant-lab-h6-m1-registration/1.0"}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt(value: Any) -> datetime | None:
    if not value:
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


def jloads_maybe(value: Any):
    if isinstance(value, (list, dict)):
        return value
    if value is None:
        return None
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return None


def norm_team(name: Any) -> str:
    text = str(name or "").strip().lower()
    text = text.replace(".", "").replace("-", " ")
    text = re.sub(r"\s+", " ", text).strip()
    aliases = {
        "oakland athletics": "athletics",
        "athletics": "athletics",
    }
    return aliases.get(text, text)


def parse_matchup(question: Any) -> set[str] | None:
    if not question:
        return None
    match = re.match(r"^\s*(.+?)\s+vs\.?\s+(.+?)\s*$", str(question), flags=re.I)
    if not match:
        return None
    return {norm_team(match.group(1)), norm_team(match.group(2))}


def tokens(market: dict[str, Any]) -> list[str]:
    raw = jloads_maybe(market.get("clobTokenIds"))
    if not isinstance(raw, list):
        return []
    return [str(x) for x in raw]


def outcomes(market: dict[str, Any]) -> list[str]:
    raw = jloads_maybe(market.get("outcomes"))
    if not isinstance(raw, list):
        return []
    return [str(x) for x in raw]


def atomic_create(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise RuntimeError(f"refusing to overwrite: {path}")

    fd, tmp_name = tempfile.mkstemp(prefix=".tmp_h6_m1_", dir=path.parent)
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(content)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


def load_frozen_inputs(*, require_committed_self: bool) -> dict[str, Any]:
    if not git_is_ancestor(EXPECTED_PARENT_HEAD):
        raise RuntimeError("frozen H6-M0 parent is not an ancestor of HEAD")

    identities = {
        M0: EXPECTED_M0_SHA,
        INDEPENDENCE: EXPECTED_INDEPENDENCE_SHA,
        CONTRACT: EXPECTED_CONTRACT_SHA,
    }
    for path, expected in identities.items():
        if not path.is_file():
            raise RuntimeError(f"missing frozen input: {path}")
        actual = sha256_file(path)
        if actual != expected:
            raise RuntimeError(
                f"frozen input SHA mismatch: {path}\n"
                f"expected={expected}\nactual={actual}"
            )

    if require_committed_self and not committed_self_matches_head():
        raise RuntimeError(
            "refusing live H6-M1 registration: registrar differs from committed HEAD"
        )

    contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
    if contract["eligibility"]["target_markets"] != TARGET_MARKETS:
        raise RuntimeError("target market mismatch")
    if contract["eligibility"]["minimum_markets"] != MIN_MARKETS:
        raise RuntimeError("minimum market mismatch")
    if contract["eligibility"]["m0_minimum_registration_lead_seconds"] != M0_MIN_LEAD_SECONDS:
        raise RuntimeError("M0 registration lead mismatch")
    if contract["eligibility"]["registrar_operational_lead_seconds"] != REGISTRATION_LEAD_SECONDS:
        raise RuntimeError("operational registration lead mismatch")

    return contract


def safe_get_json(
    client: httpx.Client,
    url: str,
    *,
    params: dict[str, Any] | None = None,
):
    response = client.get(url, params=params)
    if response.status_code != 200:
        raise RuntimeError(
            f"Gamma metadata request failed: HTTP {response.status_code}; "
            f"body={response.text[:300]!r}"
        )
    return response.json()


def resolve_mlb_tag(client: httpx.Client) -> str:
    payload = safe_get_json(client, SPORTS_URL)
    if not isinstance(payload, list):
        raise RuntimeError("/sports response is not a list")

    matches = [
        row for row in payload
        if isinstance(row, dict)
        and str(row.get("sport") or "").strip().lower() == SPORT
    ]
    if len(matches) != 1:
        raise RuntimeError(f"expected one MLB sports metadata row, got {len(matches)}")

    tag = str(matches[0].get("primaryTagId") or "").strip()
    if not tag.isdigit():
        raise RuntimeError("MLB primaryTagId missing/invalid")
    return tag


def fetch_active_mlb_events(client: httpx.Client, *, tag_id: str) -> list[dict[str, Any]]:
    all_events: list[dict[str, Any]] = []

    for page in range(MAX_EVENT_PAGES):
        offset = page * PAGE_LIMIT
        payload = safe_get_json(
            client,
            EVENTS_URL,
            params={
                "tag_id": tag_id,
                "active": "true",
                "closed": "false",
                "limit": PAGE_LIMIT,
                "offset": offset,
                "order": "start_date",
                "ascending": "true",
            },
        )
        if not isinstance(payload, list):
            raise RuntimeError("/events response is not a list")

        rows = [row for row in payload if isinstance(row, dict)]
        all_events.extend(rows)

        if len(payload) < PAGE_LIMIT:
            break
    else:
        raise RuntimeError("MLB event pagination exceeded frozen safety bound")

    return all_events


def canonical_candidate(
    *,
    gamma_event: dict[str, Any],
    market: dict[str, Any],
    now: datetime,
) -> dict[str, Any] | None:
    if market.get("sportsMarketType") != "moneyline":
        return None
    if market.get("active") is not True:
        return None
    if market.get("closed") is not False:
        return None
    if market.get("acceptingOrders") is not True:
        return None

    toks = tokens(market)
    outs = outcomes(market)
    if len(toks) != 2 or len(outs) != 2:
        return None
    if len(set(toks)) != 2:
        return None

    question_set = parse_matchup(market.get("question"))
    outcome_set = {norm_team(outs[0]), norm_team(outs[1])}
    if question_set is None or question_set != outcome_set:
        return None

    start = parse_dt(market.get("gameStartTime"))
    if start is None:
        return None
    if start < now + timedelta(seconds=REGISTRATION_LEAD_SECONDS):
        return None

    condition = str(market.get("conditionId") or "").strip().lower()
    market_id = str(market.get("id") or "").strip()
    event_id = str(gamma_event.get("id") or "").strip()
    if not condition or not market_id or not event_id:
        return None

    event_slug = (
        str(gamma_event.get("slug"))
        if gamma_event.get("slug")
        else None
    )
    market_slug = (
        str(market.get("slug"))
        if market.get("slug")
        else None
    )

    # Explicit allowlist. No price/spread/depth/liquidity/volume fields.
    return {
        "gamma_event_id": event_id,
        "gamma_event_slug": event_slug,
        "gamma_market_id": market_id,
        "condition_id": condition,
        "gamma_market_slug": market_slug,
        "gamma_question": str(market.get("question") or ""),
        "game_start_time_utc": start.isoformat(),
        "p1_outcome": outs[0],
        "p0_outcome": outs[1],
        "p1_token_id": toks[0],
        "p0_token_id": toks[1],
        "mapping_rule": MAPPING_RULE,
    }


def select_candidates(
    events: list[dict[str, Any]],
    *,
    now: datetime,
) -> list[dict[str, Any]]:
    candidates: dict[str, dict[str, Any]] = {}

    for event in events:
        markets = event.get("markets") or []
        if not isinstance(markets, list):
            continue

        for market in markets:
            if not isinstance(market, dict):
                continue
            row = canonical_candidate(
                gamma_event=event,
                market=market,
                now=now,
            )
            if row is None:
                continue

            condition = row["condition_id"]
            if condition in candidates:
                raise RuntimeError(f"duplicate condition_id in Gamma discovery: {condition}")
            candidates[condition] = row

    rows = sorted(
        candidates.values(),
        key=lambda row: (
            row["game_start_time_utc"],
            row["condition_id"],
        ),
    )

    selected = rows[:TARGET_MARKETS]
    if len(selected) < MIN_MARKETS:
        raise RuntimeError(
            f"insufficient eligible H6-M1 engineering markets: "
            f"{len(selected)} < {MIN_MARKETS}"
        )

    token_ids = [
        token
        for row in selected
        for token in (row["p1_token_id"], row["p0_token_id"])
    ]
    if len(token_ids) != len(set(token_ids)):
        raise RuntimeError("duplicate token identity in selected H6-M1 registry")

    return selected


def serialize_registry(
    rows: list[dict[str, Any]],
    *,
    registered_at: datetime,
) -> bytes:
    payload = []
    for index, row in enumerate(rows, start=1):
        payload.append({
            "study": "H6",
            "milestone": "H6-M1",
            "registration_index": index,
            "registered_at_utc": registered_at.isoformat(),
            **row,
        })

    text = "".join(
        json.dumps(
            row,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ) + "\n"
        for row in payload
    )
    return text.encode("utf-8")


def register(*, client: httpx.Client, now: datetime) -> dict[str, Any]:
    load_frozen_inputs(require_committed_self=True)

    if REGISTRY.exists():
        raise RuntimeError("H6-M1 registry already exists")
    if RECEIPT.exists():
        raise RuntimeError("H6-M1 registry receipt already exists")

    tag_id = resolve_mlb_tag(client)
    events = fetch_active_mlb_events(client, tag_id=tag_id)
    selected = select_candidates(events, now=now)

    registry_bytes = serialize_registry(selected, registered_at=now)
    registry_sha = sha256_bytes(registry_bytes)

    atomic_create(REGISTRY, registry_bytes)

    receipt = {
        "study": "H6",
        "milestone": "H6-M1",
        "status": "REGISTRY_FROZEN",
        "registered_at_utc": now.isoformat(),
        "git_commit": git_head(),
        "registration_code": {
            "path": "research/h6_m1_register.py",
            "sha256": sha256_file(Path(__file__).resolve()),
        },
        "registration_contract": {
            "path": "research/h6_m1_registration_contract.json",
            "sha256": EXPECTED_CONTRACT_SHA,
        },
        "h6_m0_sha256": EXPECTED_M0_SHA,
        "h6_m0_independence_sha256": EXPECTED_INDEPENDENCE_SHA,
        "registry": {
            "path": "data/research/h6/m1/registry.jsonl",
            "sha256": registry_sha,
            "row_count": len(selected),
            "distinct_condition_id_count": len({x["condition_id"] for x in selected}),
            "distinct_token_id_count": len({
                token
                for x in selected
                for token in (x["p1_token_id"], x["p0_token_id"])
            }),
        },
        "selection": {
            "source": "Polymarket Gamma metadata only",
            "sport": SPORT,
            "mlb_primary_tag_id": tag_id,
            "target_markets": TARGET_MARKETS,
            "minimum_markets": MIN_MARKETS,
            "m0_minimum_registration_lead_seconds": M0_MIN_LEAD_SECONDS,
            "registrar_operational_lead_seconds": REGISTRATION_LEAD_SECONDS,
            "ordering": [
                "game_start_time_utc ascending",
                "condition_id ascending",
            ],
            "f19_registry_used_for_selection": False,
            "odds_api_used": False,
            "book_or_price_endpoint_used": False,
        },
        "burn_policy": {
            "all_registered_markets_burned_for_h6_m2_and_m4": True,
            "f19_identity_overlap_allowed_for_m1_engineering": True,
        },
        "analysis_boundary": {
            "book_price_read": False,
            "book_depth_read": False,
            "trade_content_read": False,
            "h6_transition_calculated": False,
            "h6_response_calculated": False,
            "markout_calculated": False,
            "edge_calculated": False,
            "pnl_calculated": False,
            "winner_used": False,
            "settlement_used": False,
            "f19_trade_content_read": False,
            "f20_read": False,
            "h5_response_read": False,
        },
    }

    # Receipt follows successful exclusive registry creation; receipt itself
    # is exclusive and never overwritten.
    atomic_create(
        RECEIPT,
        (json.dumps(receipt, indent=2, sort_keys=True) + "\n").encode("utf-8"),
    )
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--validate-config", action="store_true")
    args = parser.parse_args()

    load_frozen_inputs(require_committed_self=False)

    if args.validate_config:
        print("H6-M1 REGISTRAR CONFIG: PASS")
        print("parent HEAD:", EXPECTED_PARENT_HEAD)
        print("M0 SHA:", EXPECTED_M0_SHA)
        print("independence SHA:", EXPECTED_INDEPENDENCE_SHA)
        print("registration contract SHA:", EXPECTED_CONTRACT_SHA)
        print("target markets:", TARGET_MARKETS)
        print("M0 minimum lead seconds:", M0_MIN_LEAD_SECONDS)
        print("operational registration lead seconds:", REGISTRATION_LEAD_SECONDS)
        print("network calls made: NO")
        print("book price/depth read: NO")
        print("H6 transition calculated: NO")
        print("H6 response calculated: NO")
        print("PnL calculated: NO")
        print("F19 trade content read: NO")
        print("F20 read: NO")
        print("H5 response read: NO")
        return

    with httpx.Client(timeout=20, headers=UA) as client:
        receipt = register(client=client, now=utcnow())

    print("========================================")
    print("H6-M1 ENGINEERING REGISTRY FROZEN")
    print("========================================")
    print("registered_at_utc:", receipt["registered_at_utc"])
    print("markets:", receipt["registry"]["row_count"])
    print("registry SHA:", receipt["registry"]["sha256"])
    print("book price/depth read: NO")
    print("H6 response calculated: NO")
    print("PnL calculated: NO")


if __name__ == "__main__":
    main()
