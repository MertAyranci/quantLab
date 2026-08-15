from __future__ import annotations

import argparse
import asyncio
import gzip
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


REPO = Path(__file__).resolve().parents[2]

if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from collectors.polymarket import ws_collector as base


CONTRACT_PATH = (
    REPO
    / "research"
    / "rn1_f20_m1_clob_capture_contract.json"
)

REGISTRY_PATH = (
    REPO
    / "data"
    / "prospective"
    / "rn1_f19"
    / "registry.jsonl"
)

ROOT = (
    REPO
    / "data"
    / "prospective"
    / "rn1_f19"
    / "clob_f20_m1"
)

RAW_ROOT = ROOT / "raw"
NORMALIZED_ROOT = ROOT / "normalized"
RUN_ROOT = ROOT / "runs"

EXPECTED_CONTRACT_SHA = "2715a7ff43a7748ef826f359296374851dfb9246e8f8152fa960e9397c05ae13"

MIN_FREE_BYTES = 4 * 1024**3


# Never make this dedicated research process ping
# the production websocket healthcheck.
base.HEALTHCHECK_URL = os.getenv(
    "F20_WS_HEALTHCHECK_URL",
    "",
)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def sha256_file(path: Path) -> str:
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def git_head() -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO,
        text=True,
    ).strip()


def load_contract() -> dict[str, Any]:

    contract_sha = sha256_file(
        CONTRACT_PATH
    )

    if contract_sha != EXPECTED_CONTRACT_SHA:
        raise RuntimeError(
            "F20 contract changed after universe freeze: "
            f"{contract_sha}"
        )

    payload = json.loads(
        CONTRACT_PATH.read_text(
            encoding="utf-8"
        )
    )

    expected_registry = payload[
        "source_registry"
    ]["sha256"]

    current_registry = sha256_file(
        REGISTRY_PATH
    )

    if current_registry != expected_registry:
        raise RuntimeError(
            "F19 registry SHA changed after "
            "F20 universe freeze"
        )

    parent_path = (
        REPO
        / payload[
            "parent_collector"
        ]["path"]
    )

    current_parent_sha = sha256_file(
        parent_path
    )

    expected_parent_sha = payload[
        "parent_collector"
    ]["sha256"]

    if current_parent_sha != expected_parent_sha:
        raise RuntimeError(
            "parent ws_collector.py changed "
            "after F20 universe freeze"
        )

    markets = payload[
        "capture_universe"
    ]["markets"]

    tokens = [
        str(token)
        for market in markets
        for token in market["token_ids"]
    ]

    if len(markets) != 96:
        raise RuntimeError(
            f"expected 96 frozen markets, "
            f"got {len(markets)}"
        )

    if len(tokens) != 192:
        raise RuntimeError(
            f"expected 192 frozen tokens, "
            f"got {len(tokens)}"
        )

    if len(set(tokens)) != 192:
        raise RuntimeError(
            "duplicate token IDs in F20 universe"
        )

    return payload


class NullWriter:

    def write(
        self,
        record: dict[str, Any],
    ) -> None:
        return

    def flush(self) -> None:
        return


class MinuteGzipWriter:

    def __init__(
        self,
        root: Path,
    ):
        self.root = root
        self._minute: str | None = None
        self._fh = None

    def _check_disk(self) -> None:

        free = shutil.disk_usage(
            REPO
        ).free

        if free < MIN_FREE_BYTES:
            raise RuntimeError(
                "F20 disk guard triggered: "
                f"free_bytes={free} "
                f"minimum={MIN_FREE_BYTES}"
            )

    def _rotate(
        self,
    ) -> None:

        now = utcnow()

        minute = now.strftime(
            "%Y%m%d_%H%M"
        )

        if minute == self._minute:
            return

        self.close()
        self._check_disk()

        day_dir = (
            self.root
            / now.strftime("%Y%m%d")
        )

        day_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        path = (
            day_dir
            / f"{minute}.jsonl.gz"
        )

        self._fh = gzip.open(
            path,
            mode="at",
            encoding="utf-8",
            compresslevel=3,
        )

        self._minute = minute

    def write(
        self,
        record: dict[str, Any],
    ) -> None:

        self._rotate()

        self._fh.write(
            json.dumps(
                record,
                separators=(",", ":"),
                ensure_ascii=False,
            )
            + "\n"
        )

    def flush(self) -> None:

        if self._fh is not None:
            self._fh.flush()

    def close(self) -> None:

        if self._fh is not None:
            self._fh.flush()
            self._fh.close()

        self._fh = None
        self._minute = None


class F20EvidenceWriter:

    def __init__(self):

        self.raw = MinuteGzipWriter(
            RAW_ROOT
        )

        self.normalized = MinuteGzipWriter(
            NORMALIZED_ROOT
        )

        self._connection_id: str | None = None
        self._frame_sequence = 0

    def start_connection(
        self,
        connection_id: str,
    ) -> None:

        self._connection_id = connection_id
        self._frame_sequence = 0

    def write_raw_frame(
        self,
        *,
        connection_id: str,
        capture_time: str,
        raw: str,
    ) -> tuple[int, str]:

        if not isinstance(raw, str):
            raise TypeError(
                "F20 requires UTF-8 websocket "
                "text frames"
            )

        if connection_id != self._connection_id:
            self.start_connection(
                connection_id
            )

        self._frame_sequence += 1

        digest = hashlib.sha256(
            raw.encode("utf-8")
        ).hexdigest()

        self.raw.write(
            {
                "capture_time":
                    capture_time,

                "connection_id":
                    connection_id,

                "frame_sequence":
                    self._frame_sequence,

                "raw_frame_sha256":
                    digest,

                "raw":
                    raw,
            }
        )

        return (
            self._frame_sequence,
            digest,
        )

    def write_normalized(
        self,
        record: dict[str, Any],
        *,
        raw_frame_sequence: int | None,
        raw_frame_sha256: str | None,
    ) -> None:

        out = dict(record)

        out[
            "raw_frame_sequence"
        ] = raw_frame_sequence

        out[
            "raw_frame_sha256"
        ] = raw_frame_sha256

        # Preserve raw venue evidence, but do not expose
        # winner fields through the normal analysis stream.
        if out.get("kind") == "market_resolved":

            payload = out.get(
                "payload"
            ) or {}

            out["token"] = None

            out["payload"] = {
                "event_type":
                    "market_resolved",

                "market":
                    payload.get("market"),

                "timestamp":
                    payload.get("timestamp"),

                "resolution_payload_redacted":
                    True,
            }

        self.normalized.write(
            out
        )

    def flush(self) -> None:
        self.raw.flush()
        self.normalized.flush()

    def close(self) -> None:
        self.raw.close()
        self.normalized.close()


class F20Collector(
    base.WSCollector
):

    def __init__(
        self,
        contract: dict[str, Any],
    ):

        super().__init__(
            watchlist_size=192,
            evidence_capture_id=None,
            buffer_writer=NullWriter(),
        )

        self.contract = contract

        self.evidence = (
            F20EvidenceWriter()
        )

    def _desired_tokens(
        self,
        *,
        now: datetime | None = None,
    ) -> list[str]:

        if now is None:
            now = utcnow()

        window = self.contract[
            "capture"
        ]["subscription_window"]

        before = timedelta(
            seconds=int(
                window[
                    "start_seconds_before_registered_game_start"
                ]
            )
        )

        after = timedelta(
            seconds=int(
                window[
                    "end_seconds_after_registered_game_start"
                ]
            )
        )

        desired: list[str] = []

        for market in self.contract[
            "capture_universe"
        ]["markets"]:

            start = datetime.fromisoformat(
                market[
                    "game_start_time_utc"
                ].replace(
                    "Z",
                    "+00:00",
                )
            )

            if (
                start - before
                <= now
                <= start + after
            ):
                desired.extend(
                    str(token)
                    for token
                    in market["token_ids"]
                )

        if len(desired) != len(set(desired)):
            raise RuntimeError(
                "duplicate token in active F20 window"
            )

        return desired

    def fetch_watchlist(
        self,
    ) -> None:

        frozen_tokens = [
            str(token)
            for market in self.contract[
                "capture_universe"
            ]["markets"]
            for token in market["token_ids"]
        ]

        if len(frozen_tokens) != 192:
            raise RuntimeError(
                "F20 frozen token count != 192"
            )

        if len(set(frozen_tokens)) != 192:
            raise RuntimeError(
                "F20 frozen tokens not unique"
            )

        tokens = self._desired_tokens()

        if not tokens:
            raise RuntimeError(
                "F20 has no active capture-window tokens "
                "at collector startup"
            )

        self.tokens = tokens
        self.h2_v2_tokens = set()

        for token in frozen_tokens:
            self.generation.setdefault(
                token,
                0,
            )

        base.log.info(
            "F20 active capture window: "
            "%d markets / %d tokens "
            "(frozen universe 96 / 192)",
            len(tokens) // 2,
            len(tokens),
        )

    async def run(
        self,
    ) -> None:

        # A system restart may occur between capture
        # windows. Wait without opening a useless WS
        # connection until at least one frozen market
        # enters its deterministic capture window.
        while not self._desired_tokens():

            base.log.info(
                "F20 no active capture-window "
                "markets; sleeping 60s"
            )

            await asyncio.sleep(60)

        await super().run()

    async def reconcile_watchlist(
        self,
        ws,
    ) -> None:

        new = self._desired_tokens()

        current = set(
            self.tokens
        )

        wanted = set(
            new
        )

        added = wanted - current
        removed = current - wanted

        if not added and not removed:
            return

        if added:

            await ws.send(
                json.dumps(
                    {
                        "assets_ids":
                            sorted(added),

                        "operation":
                            "subscribe",

                        "custom_feature_enabled":
                            True,
                    }
                )
            )

            for token in added:
                self.generation.setdefault(
                    token,
                    0,
                )

            # Establish an authoritative complete
            # book immediately after subscribing.
            self.resync_tokens(
                sorted(added),
                "f20_window_add",
            )

        if removed:

            await ws.send(
                json.dumps(
                    {
                        "assets_ids":
                            sorted(removed),

                        "operation":
                            "unsubscribe",
                    }
                )
            )

        self.tokens = new

        base.log.info(
            "F20 window reconciled: "
            "+%d -%d (now %d tokens)",
            len(added),
            len(removed),
            len(new),
        )

    def emit(
        self,
        kind: str,
        token: str | None,
        payload: dict,
        change_index=0,
        *,
        raw_frame_sequence: int | None = None,
        raw_frame_sha256: str | None = None,
    ) -> None:

        record = {
            "kind":
                kind,

            "token":
                token,

            "capture_time":
                utcnow().isoformat(),

            "connection_id":
                self.conn_id,

            "ingest_sequence":
                self.seq,

            "change_index":
                change_index,

            "book_generation":
                (
                    self.generation.get(
                        token,
                        0,
                    )
                    if token
                    else None
                ),

            "watchlist_rule":
                "rn1_f19_f20_m1_frozen",

            "payload":
                payload,
        }

        self.evidence.write_normalized(
            record,
            raw_frame_sequence=
                raw_frame_sequence,
            raw_frame_sha256=
                raw_frame_sha256,
        )

        self.counts[kind] = (
            self.counts.get(
                kind,
                0,
            )
            + 1
        )


def write_run_start(
    contract: dict[str, Any],
    *,
    mode: str,
) -> Path:

    RUN_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    now = utcnow()

    path = (
        RUN_ROOT
        / (
            "start_"
            + now.strftime(
                "%Y%m%dT%H%M%S.%fZ"
            )
            + ".json"
        )
    )

    payload = {
        "study":
            "RN1-F20-M1",

        "status":
            "STARTED",

        "mode":
            mode,

        "started_at_utc":
            now.isoformat(),

        "git_head":
            git_head(),

        "contract_sha256":
            sha256_file(
                CONTRACT_PATH
            ),

        "registry_sha256":
            sha256_file(
                REGISTRY_PATH
            ),

        "market_count":
            contract[
                "capture_universe"
            ]["market_count"],

        "token_count":
            contract[
                "capture_universe"
            ]["token_count"],

        "analysis_boundary": {
            "signal_calculated":
                False,

            "markout_calculated":
                False,

            "settlement_used":
                False,

            "winner_used":
                False,

            "pnl_calculated":
                False,
        },
    }

    path.write_text(
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    return path


async def run_smoke(
    collector: F20Collector,
    seconds: float,
) -> None:

    try:
        await asyncio.wait_for(
            collector.run(),
            timeout=seconds,
        )

    except asyncio.TimeoutError:
        print(
            "F20 smoke duration reached:",
            seconds,
        )


def main() -> None:

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--smoke-seconds",
        type=float,
        default=None,
    )

    args = parser.parse_args()

    contract = load_contract()

    if (
        args.smoke_seconds is not None
        and args.smoke_seconds < 5
    ):
        raise RuntimeError(
            "--smoke-seconds must be >= 5"
        )

    mode = (
        "smoke"
        if args.smoke_seconds is not None
        else "service"
    )

    receipt = write_run_start(
        contract,
        mode=mode,
    )

    print(
        "F20 contract:",
        sha256_file(CONTRACT_PATH),
    )

    print(
        "F19 registry:",
        sha256_file(REGISTRY_PATH),
    )

    print(
        "capture markets:",
        contract[
            "capture_universe"
        ]["market_count"],
    )

    print(
        "capture tokens:",
        contract[
            "capture_universe"
        ]["token_count"],
    )

    print(
        "run receipt:",
        receipt,
    )

    collector = F20Collector(
        contract
    )

    def terminate(
        signum,
        frame,
    ):
        raise KeyboardInterrupt

    signal.signal(
        signal.SIGTERM,
        terminate,
    )

    try:

        if args.smoke_seconds is not None:

            asyncio.run(
                run_smoke(
                    collector,
                    args.smoke_seconds,
                )
            )

        else:

            asyncio.run(
                collector.run()
            )

    except KeyboardInterrupt:

        print(
            "F20 shutdown requested"
        )

    finally:

        collector.evidence.flush()
        collector.evidence.close()
        collector.http.close()


if __name__ == "__main__":
    main()
