"""Targeted Polymarket wallet collector for the H2 MLB research universe.

This collector does NOT attempt to download a wallet's complete global history.

Instead, it:

1. Loads unique MLB Polymarket markets already matched to H2 odds_games.
2. Fetches RN1's trades separately for each condition_id.
3. Requests maker and taker fills with takerOnly=false.
4. Preserves BUY and SELL fills.
5. Resolves every asset to the expected internal market/token.
6. Writes one collection receipt per wallet + condition_id.
7. Marks open markets as partial so --resume will revisit them later.
8. Remains idempotent through the existing venue_trade_key constraint.

Usage:

    # Show target markets only
    .venv/bin/python collectors/polymarket/wallet_collector.py \
        --wallet RN1 --dry-run

    # Collect all matched H2 MLB markets
    .venv/bin/python collectors/polymarket/wallet_collector.py \
        --wallet RN1

    # Skip ended markets already marked complete
    .venv/bin/python collectors/polymarket/wallet_collector.py \
        --wallet RN1 --resume

    # Test only a few markets
    .venv/bin/python collectors/polymarket/wallet_collector.py \
        --wallet RN1 --limit-markets 3
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
import psycopg2
from dotenv import dotenv_values


REPO = Path(__file__).resolve().parents[2]
ENV = dotenv_values(REPO / ".env")
CONFIG = REPO / "config" / "research_wallets.json"

DATA_API = "https://data-api.polymarket.com"
USER_AGENT = {"User-Agent": "quant-lab-wallet-forensics/0.3"}

POLYMARKET_VENUE_ID = 1
PAGE_LIMIT = 500

# The global API documents a bounded offset range. Per-market collections
# should normally finish long before this value.
MAX_OFFSET = 10_000

DEFAULT_MAX_PAGES_PER_MARKET = 21
DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_MAX_RETRIES = 4

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)sZ %(levelname)s %(message)s",
)
log = logging.getLogger("wallet_h2_mlb")


def connect():
    """Connect to the local quantlab PostgreSQL instance."""

    password = ENV.get("PG_PASSWORD")
    if not password:
        raise RuntimeError("PG_PASSWORD is missing from .env")

    return psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="quantlab",
        user="quantlab",
        password=password,
    )


def load_wallet(name: str) -> dict[str, Any]:
    """Load one configured research wallet by name."""

    payload = json.loads(CONFIG.read_text())

    for wallet in payload["wallets"]:
        if wallet["name"].lower() == name.lower():
            return wallet

    available = [wallet["name"] for wallet in payload["wallets"]]
    raise SystemExit(
        f"Wallet {name!r} not found in {CONFIG}. Available: {available}"
    )


def api_get(
    client: httpx.Client,
    path: str,
    params: dict[str, Any],
    *,
    max_retries: int,
    timeout_seconds: float,
) -> list[dict[str, Any]]:
    """GET a Data API list endpoint with bounded retries."""

    last_error: str | None = None

    for attempt in range(1, max_retries + 1):
        try:
            response = client.get(
                f"{DATA_API}{path}",
                params=params,
                timeout=timeout_seconds,
            )

            if response.status_code == 200:
                payload = response.json()

                if not isinstance(payload, list):
                    raise RuntimeError(
                        f"Expected list from {path}, got "
                        f"{type(payload).__name__}"
                    )

                return payload

            body_preview = response.text[:300].replace("\n", " ")
            last_error = (
                f"HTTP {response.status_code}: {body_preview}"
            )

        except (
            httpx.HTTPError,
            json.JSONDecodeError,
            RuntimeError,
        ) as exc:
            last_error = str(exc)

        if attempt < max_retries:
            delay = 1.5 * attempt
            log.warning(
                "API attempt %d/%d failed for %s: %s; retrying in %.1fs",
                attempt,
                max_retries,
                path,
                last_error,
                delay,
            )
            time.sleep(delay)

    raise RuntimeError(
        f"GET {path} failed after {max_retries} attempts: {last_error}"
    )


def load_h2_mlb_targets(cur) -> list[dict[str, Any]]:
    """Load one row per unique H2-matched MLB Polymarket market.

    Some markets were matched to more than one odds_game row. MAX(commence_time)
    selects the later and credible MLB start time for those duplicate mappings.
    """

    cur.execute(
        """
        SELECT
            m.id AS market_id,
            m.condition_id,
            m.slug,
            m.end_date,
            MAX(og.commence_time) AS commence_time,
            (
                SELECT COUNT(*)
                FROM tokens t
                WHERE t.market_id = m.id
            ) AS token_count
        FROM odds_games og
        JOIN markets m
          ON m.id = og.polymarket_market_id
        WHERE og.polymarket_market_id IS NOT NULL
          AND og.sport_key ILIKE '%mlb%'
          AND m.condition_id IS NOT NULL
        GROUP BY
            m.id,
            m.condition_id,
            m.slug,
            m.end_date
        ORDER BY m.id
        """
    )

    targets = []

    for (
        market_id,
        condition_id,
        slug,
        end_date,
        commence_time,
        token_count,
    ) in cur.fetchall():
        targets.append(
            {
                "market_id": market_id,
                "condition_id": condition_id,
                "slug": slug,
                "end_date": end_date,
                "commence_time": commence_time,
                "token_count": int(token_count),
            }
        )

    return targets


def completed_condition_ids(cur, wallet_name: str) -> set[str]:
    """Return condition IDs already marked complete for this wallet."""

    cur.execute(
        """
        SELECT condition_id
        FROM wallet_collection_receipts
        WHERE wallet = %s
          AND status = 'complete'
          AND condition_id LIKE '0x%%'
        """,
        (wallet_name,),
    )

    return {row[0] for row in cur.fetchall()}


def venue_trade_key(wallet_name: str, trade: dict[str, Any]) -> str:
    """Create a deterministic fill-level key.

    Transaction hash alone is insufficient because one transaction can contain
    multiple fills. Keep this formatting compatible with the previous collector
    so existing rows remain idempotent.
    """

    price_q = f"{float(trade.get('price', 0)):.4f}"
    size_q = f"{float(trade.get('size', 0)):.6f}"

    components = [
        wallet_name,
        trade.get("transactionHash"),
        trade.get("conditionId"),
        trade.get("asset"),
        trade.get("timestamp"),
        trade.get("side"),
        price_q,
        size_q,
    ]

    raw = "|".join(str(component) for component in components)
    return hashlib.sha256(raw.encode()).hexdigest()


def parse_trade_timestamp(trade: dict[str, Any]) -> datetime:
    """Parse the Data API's Unix timestamp."""

    return datetime.fromtimestamp(
        int(trade["timestamp"]),
        tz=timezone.utc,
    )


def resolve_expected_token(
    cur,
    *,
    asset_id: Any,
    expected_market_id: int,
) -> tuple[int | None, str | None]:
    """Resolve an API asset and verify it belongs to the target market."""

    if asset_id is None:
        return None, "missing_asset_id"

    cur.execute(
        """
        SELECT id, market_id
        FROM tokens
        WHERE venue_id = %s
          AND venue_token_id = %s
        """,
        (POLYMARKET_VENUE_ID, str(asset_id)),
    )

    row = cur.fetchone()

    if row is None:
        return None, "asset_not_in_tokens"

    token_id, actual_market_id = row

    if actual_market_id != expected_market_id:
        return None, (
            f"asset_market_mismatch:"
            f"expected={expected_market_id},actual={actual_market_id}"
        )

    return token_id, None


def insert_trade(
    cur,
    *,
    wallet_name: str,
    trade: dict[str, Any],
    token_id: int,
) -> int:
    """Insert one resolved wallet fill idempotently."""

    event_time = parse_trade_timestamp(trade)
    price_mc = int(round(float(trade["price"]) * 1000))
    size = float(trade["size"])
    side = str(trade.get("side") or "").upper()

    cur.execute(
        """
        INSERT INTO trades (
            venue_id,
            token_id,
            condition_id,
            event_time,
            capture_time,
            price_mc,
            size,
            side,
            outcome,
            wallet,
            tx_hash,
            venue_trade_key,
            source
        )
        VALUES (
            %s, %s, %s, %s, now(), %s, %s, %s,
            %s, %s, %s, %s, 'data_api'
        )
        ON CONFLICT (
            venue_id,
            venue_trade_key,
            event_time
        )
        DO NOTHING
        """,
        (
            POLYMARKET_VENUE_ID,
            token_id,
            trade.get("conditionId"),
            event_time,
            price_mc,
            size,
            side,
            trade.get("outcome"),
            wallet_name,
            trade.get("transactionHash"),
            venue_trade_key(wallet_name, trade),
        ),
    )

    return cur.rowcount


def record_unresolved_asset(
    cur,
    *,
    wallet_name: str,
    condition_id: str,
    asset_id: Any,
) -> None:
    """Record an unresolved asset instead of silently dropping it."""

    cur.execute(
        """
        INSERT INTO wallet_unresolved_assets (
            wallet,
            condition_id,
            asset_id
        )
        VALUES (%s, %s, %s)
        ON CONFLICT (wallet, asset_id)
        DO UPDATE SET
            occurrences = wallet_unresolved_assets.occurrences + 1
        """,
        (
            wallet_name,
            condition_id,
            str(asset_id),
        ),
    )


def upsert_receipt(
    cur,
    *,
    wallet_name: str,
    condition_id: str,
    status: str,
    trade_count: int,
    earliest_trade: datetime | None,
    latest_trade: datetime | None,
    notes: str,
) -> None:
    """Write one receipt per wallet and target condition ID."""

    cur.execute(
        """
        INSERT INTO wallet_collection_receipts (
            wallet,
            condition_id,
            status,
            trade_count,
            earliest_trade,
            latest_trade,
            collected_at,
            attempts,
            notes
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, now(), 1, %s
        )
        ON CONFLICT (wallet, condition_id)
        DO UPDATE SET
            status = EXCLUDED.status,
            trade_count = EXCLUDED.trade_count,
            earliest_trade = EXCLUDED.earliest_trade,
            latest_trade = EXCLUDED.latest_trade,
            collected_at = now(),
            attempts = wallet_collection_receipts.attempts + 1,
            notes = EXCLUDED.notes
        """,
        (
            wallet_name,
            condition_id,
            status,
            trade_count,
            earliest_trade,
            latest_trade,
            notes,
        ),
    )


def fetch_trades_for_market(
    client: httpx.Client,
    *,
    wallet_address: str,
    condition_id: str,
    max_pages: int,
    max_retries: int,
    timeout_seconds: float,
) -> tuple[list[dict[str, Any]], int, bool, str]:
    """Fetch every available wallet fill for one target market.

    Returns:
        trades
        pages_fetched
        pagination_complete
        completion_note
    """

    trades: list[dict[str, Any]] = []
    offset = 0
    pages_fetched = 0

    while pages_fetched < max_pages:
        batch = api_get(
            client,
            "/trades",
            {
                "user": wallet_address,
                "market": condition_id,
                "limit": PAGE_LIMIT,
                "offset": offset,
                "takerOnly": "false",
            },
            max_retries=max_retries,
            timeout_seconds=timeout_seconds,
        )

        pages_fetched += 1
        trades.extend(batch)

        if len(batch) < PAGE_LIMIT:
            return (
                trades,
                pages_fetched,
                True,
                "exhausted_market_feed",
            )

        next_offset = offset + PAGE_LIMIT

        if next_offset > MAX_OFFSET:
            return (
                trades,
                pages_fetched,
                False,
                f"offset_limit_reached:{MAX_OFFSET}",
            )

        offset = next_offset

    return (
        trades,
        pages_fetched,
        False,
        f"page_cap_reached:{max_pages}",
    )


def is_market_finished(
    end_date: datetime | None,
    commence_time: datetime | None,
) -> bool:
    """Return whether collection can be treated as final.

    Prefer the Polymarket end date when available. MLB rows currently have NULL
    end_date, so use the canonical game start plus an eight-hour safety buffer.
    """

    now = datetime.now(timezone.utc)

    if end_date is not None:
        return end_date <= now

    if commence_time is not None:
        return commence_time + timedelta(hours=8) <= now

    return False


def process_market(
    conn,
    client: httpx.Client,
    *,
    wallet_name: str,
    wallet_address: str,
    target: dict[str, Any],
    max_pages: int,
    max_retries: int,
    timeout_seconds: float,
) -> dict[str, Any]:
    """Fetch, validate, insert and receipt one target market."""

    market_id = target["market_id"]
    condition_id = target["condition_id"]
    slug = target["slug"]
    end_date = target["end_date"]
    commence_time = target["commence_time"]
    token_count = target["token_count"]

    counters: Counter[str] = Counter()

    earliest_trade: datetime | None = None
    latest_trade: datetime | None = None

    if token_count != 2:
        note = (
            f"slug={slug} market_id={market_id} "
            f"expected_two_tokens actual_token_count={token_count}"
        )

        with conn.cursor() as cur:
            upsert_receipt(
                cur,
                wallet_name=wallet_name,
                condition_id=condition_id,
                status="failed",
                trade_count=0,
                earliest_trade=None,
                latest_trade=None,
                notes=note,
            )

        conn.commit()

        return {
            "status": "failed",
            "raw": 0,
            "inserted": 0,
            "unresolved": 0,
            "buy": 0,
            "sell": 0,
            "note": note,
        }

    try:
        (
            trades,
            pages_fetched,
            pagination_complete,
            pagination_note,
        ) = fetch_trades_for_market(
            client,
            wallet_address=wallet_address,
            condition_id=condition_id,
            max_pages=max_pages,
            max_retries=max_retries,
            timeout_seconds=timeout_seconds,
        )

        counters["raw"] = len(trades)

        with conn.cursor() as cur:
            for trade in trades:
                side = str(trade.get("side") or "").upper()

                if side == "BUY":
                    counters["buy"] += 1
                elif side == "SELL":
                    counters["sell"] += 1
                else:
                    counters["unknown_side"] += 1

                try:
                    event_time = parse_trade_timestamp(trade)
                except (KeyError, TypeError, ValueError, OSError):
                    counters["malformed"] += 1
                    continue

                if earliest_trade is None or event_time < earliest_trade:
                    earliest_trade = event_time

                if latest_trade is None or event_time > latest_trade:
                    latest_trade = event_time

                returned_condition = str(
                    trade.get("conditionId") or ""
                ).lower()

                if returned_condition != condition_id.lower():
                    counters["wrong_condition"] += 1
                    continue

                if side not in {"BUY", "SELL"}:
                    continue

                token_id, resolution_error = resolve_expected_token(
                    cur,
                    asset_id=trade.get("asset"),
                    expected_market_id=market_id,
                )

                if token_id is None:
                    counters["unresolved"] += 1

                    record_unresolved_asset(
                        cur,
                        wallet_name=wallet_name,
                        condition_id=condition_id,
                        asset_id=trade.get("asset"),
                    )

                    log.error(
                        "unresolved target fill wallet=%s slug=%s "
                        "asset=%s reason=%s",
                        wallet_name,
                        slug,
                        trade.get("asset"),
                        resolution_error,
                    )
                    continue

                counters["resolved"] += 1

                counters["inserted"] += insert_trade(
                    cur,
                    wallet_name=wallet_name,
                    trade=trade,
                    token_id=token_id,
                )

            problems = []

            if not pagination_complete:
                problems.append(pagination_note)

            if counters["unresolved"]:
                problems.append(
                    f"unresolved={counters['unresolved']}"
                )

            if counters["wrong_condition"]:
                problems.append(
                    f"wrong_condition={counters['wrong_condition']}"
                )

            if counters["unknown_side"]:
                problems.append(
                    f"unknown_side={counters['unknown_side']}"
                )

            if counters["malformed"]:
                problems.append(
                    f"malformed={counters['malformed']}"
                )

            if not is_market_finished(end_date, commence_time):
                problems.append("market_not_finished")

            status = "complete" if not problems else "partial"

            note = " ".join(
                [
                    f"slug={slug}",
                    f"market_id={market_id}",
                    f"pages={pages_fetched}",
                    f"raw={counters['raw']}",
                    f"buy={counters['buy']}",
                    f"sell={counters['sell']}",
                    f"resolved={counters['resolved']}",
                    f"inserted={counters['inserted']}",
                    f"unresolved={counters['unresolved']}",
                    f"wrong_condition={counters['wrong_condition']}",
                    f"unknown_side={counters['unknown_side']}",
                    f"malformed={counters['malformed']}",
                    f"pagination={pagination_note}",
                    f"end_date={end_date.isoformat() if end_date else 'NULL'}",
                    f"commence_time={commence_time.isoformat() if commence_time else 'NULL'}",
                    (
                        "problems=" + ",".join(problems)
                        if problems
                        else "problems=none"
                    ),
                ]
            )

            upsert_receipt(
                cur,
                wallet_name=wallet_name,
                condition_id=condition_id,
                status=status,
                trade_count=counters["raw"],
                earliest_trade=earliest_trade,
                latest_trade=latest_trade,
                notes=note,
            )

        conn.commit()

        return {
            "status": status,
            "raw": counters["raw"],
            "inserted": counters["inserted"],
            "unresolved": counters["unresolved"],
            "buy": counters["buy"],
            "sell": counters["sell"],
            "note": note,
        }

    except Exception as exc:
        conn.rollback()

        error_note = (
            f"slug={slug} market_id={market_id} "
            f"collection_failed={type(exc).__name__}:{exc}"
        )

        with conn.cursor() as cur:
            upsert_receipt(
                cur,
                wallet_name=wallet_name,
                condition_id=condition_id,
                status="failed",
                trade_count=0,
                earliest_trade=None,
                latest_trade=None,
                notes=error_note,
            )

        conn.commit()

        log.exception(
            "FAILED wallet=%s condition=%s slug=%s",
            wallet_name,
            condition_id,
            slug,
        )

        return {
            "status": "failed",
            "raw": 0,
            "inserted": 0,
            "unresolved": 0,
            "buy": 0,
            "sell": 0,
            "note": error_note,
        }


def main() -> int:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--wallet",
        required=True,
        help="Wallet name from config/research_wallets.json, e.g. RN1",
    )
    parser.add_argument(
        "--scope",
        choices=["h2-mlb"],
        default="h2-mlb",
        help="Target universe. Currently only h2-mlb is supported.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip condition IDs already marked complete.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show target markets without fetching or writing trades.",
    )
    parser.add_argument(
        "--limit-markets",
        type=int,
        default=0,
        help="Process only the first N targets; 0 means all.",
    )
    parser.add_argument(
        "--max-pages-per-market",
        type=int,
        default=DEFAULT_MAX_PAGES_PER_MARKET,
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_MAX_RETRIES,
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.05,
        help="Delay between target markets.",
    )

    args = parser.parse_args()

    if args.limit_markets < 0:
        parser.error("--limit-markets cannot be negative")

    if args.max_pages_per_market <= 0:
        parser.error("--max-pages-per-market must be positive")

    if args.max_retries <= 0:
        parser.error("--max-retries must be positive")

    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be positive")

    if args.sleep_seconds < 0:
        parser.error("--sleep-seconds cannot be negative")

    wallet = load_wallet(args.wallet)
    wallet_name = wallet["name"]
    wallet_address = wallet["address"].lower()

    conn = connect()
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            targets = load_h2_mlb_targets(cur)

            completed = (
                completed_condition_ids(cur, wallet_name)
                if args.resume
                else set()
            )

        if args.resume:
            before = len(targets)
            targets = [
                target
                for target in targets
                if target["condition_id"] not in completed
            ]
            log.info(
                "resume: skipped %d complete markets",
                before - len(targets),
            )

        if args.limit_markets:
            targets = targets[: args.limit_markets]

        log.info(
            "wallet=%s address=%s scope=%s targets=%d",
            wallet_name,
            wallet_address,
            args.scope,
            len(targets),
        )

        if args.dry_run:
            for index, target in enumerate(targets, start=1):
                log.info(
                    "[dry-run] %02d market_id=%s tokens=%s "
                    "commence=%s end=%s condition=%s slug=%s",
                    index,
                    target["market_id"],
                    target["token_count"],
                    (
                    target["commence_time"].isoformat()
                    if target["commence_time"]
                    else "NULL"
                    ),
                    (
                        target["end_date"].isoformat()
                        if target["end_date"]
                        else "NULL"
                    ),
                    target["condition_id"],
                    target["slug"],
                )
            return 0

        totals: Counter[str] = Counter()

        with httpx.Client(headers=USER_AGENT) as client:
            for index, target in enumerate(targets, start=1):
                log.info(
                    "[%d/%d] collecting %s",
                    index,
                    len(targets),
                    target["slug"],
                )

                result = process_market(
                    conn,
                    client,
                    wallet_name=wallet_name,
                    wallet_address=wallet_address,
                    target=target,
                    max_pages=args.max_pages_per_market,
                    max_retries=args.max_retries,
                    timeout_seconds=args.timeout_seconds,
                )

                totals[f"status_{result['status']}"] += 1
                totals["raw"] += result["raw"]
                totals["buy"] += result["buy"]
                totals["sell"] += result["sell"]
                totals["inserted"] += result["inserted"]
                totals["unresolved"] += result["unresolved"]

                log.info(
                    "[%d/%d] %s status=%s raw=%d BUY=%d SELL=%d "
                    "inserted=%d unresolved=%d",
                    index,
                    len(targets),
                    target["slug"],
                    result["status"],
                    result["raw"],
                    result["buy"],
                    result["sell"],
                    result["inserted"],
                    result["unresolved"],
                )

                if args.sleep_seconds:
                    time.sleep(args.sleep_seconds)

        log.info("=" * 72)
        log.info(
            "DONE wallet=%s targets=%d complete=%d partial=%d failed=%d",
            wallet_name,
            len(targets),
            totals["status_complete"],
            totals["status_partial"],
            totals["status_failed"],
        )
        log.info(
            "fills raw=%d BUY=%d SELL=%d inserted=%d unresolved=%d",
            totals["raw"],
            totals["buy"],
            totals["sell"],
            totals["inserted"],
            totals["unresolved"],
        )
        log.info(
            "Re-run without --resume to verify inserted=0. "
            "Use --resume later to revisit only partial/failed/open markets."
        )

        return 0

    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
