"""Data-quality checks — daily audit of the quant-lab database.

Usage:
    .venv/bin/python db/dq_checks.py            # run all checks, alert on issues
    .venv/bin/python db/dq_checks.py --quiet    # only Telegram if something fails

Cron (daily 06:00 UTC):
    0 6 * * * /home/lab/quantLab/.venv/bin/python /home/lab/quantLab/db/dq_checks.py >> /home/lab/quantLab/logs/dq.log 2>&1

Each check appends to dq_incidents (severity: info|warn|crit) and contributes
one line to a Telegram summary. Checks are READ-ONLY except for writing
incidents — they never modify or delete data. A crossed book, a gap, a missing
resolution: all RECORDED, never "fixed" silently.
"""

import argparse
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

import httpx
import psycopg2
from dotenv import dotenv_values

REPO = Path(__file__).resolve().parent.parent
ENV = dotenv_values(REPO / ".env")
TG_TOKEN = ENV.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = ENV.get("TELEGRAM_CHAT_ID", "")
DQ_HEALTHCHECK_URL = ENV.get("DQ_HEALTHCHECK_URL", "")

# thresholds
STALE_WS_MIN = 15          # WS TOB should be fresher than this
STALE_REST_MIN = 10        # REST snapshot cadence is ~60s; 10min = a real gap
MIN_FUTURE_PARTITIONS = 2  # RFC alert
LOADER_LAG_MIN = 30        # buffer files older than this = loader behind


def telegram(msg: str):
    if not (TG_TOKEN and TG_CHAT):
        print("[no telegram configured]\n" + msg)
        return
    try:
        httpx.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                   json={"chat_id": TG_CHAT, "text": msg}, timeout=15)
    except httpx.HTTPError as e:
        print(f"telegram failed: {e}")


class DQ:
    def __init__(self, conn):
        self.conn = conn
        self.cur = conn.cursor()
        self.lines: list[str] = []
        self.worst = "info"

    def incident(self, check: str, severity: str, detail: dict, line: str):
        self.cur.execute(
            """INSERT INTO dq_incidents (observed_at, check_name, severity, details)
               VALUES (now(), %s, %s, %s)""",
            (check, severity, psycopg2.extras.Json(detail) if hasattr(psycopg2.extras, "Json")
             else __import__("json").dumps(detail)))
        order = {"info": 0, "warn": 1, "crit": 2}
        if order[severity] > order[self.worst]:
            self.worst = severity
        icon = {"info": "ok", "warn": "WARN", "crit": "CRIT"}[severity]
        self.lines.append(f"[{icon}] {line}")

    # ---- checks -------------------------------------------------------------

    def check_ws_freshness(self):
        self.cur.execute("""SELECT max(capture_time) FROM tob_snapshots
                            WHERE source LIKE 'ws%'""")
        latest = self.cur.fetchone()[0]
        if latest is None:
            self.incident("ws_freshness", "warn", {}, "WS: no data at all")
            return
        age = (datetime.now(timezone.utc) - latest).total_seconds() / 60
        if age > STALE_WS_MIN:
            self.incident("ws_freshness", "crit", {"age_min": round(age, 1)},
                          f"WS stale: {age:.0f}min old")
        else:
            self.incident("ws_freshness", "info", {"age_min": round(age, 1)},
                          f"WS fresh ({age:.0f}min)")

    def check_rest_freshness(self):
        self.cur.execute("""SELECT max(capture_time) FROM tob_snapshots
                            WHERE source = 'rest_book'""")
        latest = self.cur.fetchone()[0]
        if latest is None:
            self.incident("rest_freshness", "info", {}, "REST: no rows via loader (expected)")
            return
        age = (datetime.now(timezone.utc) - latest).total_seconds() / 60
        sev = "warn" if age > STALE_REST_MIN else "info"
        self.incident("rest_freshness", sev, {"age_min": round(age, 1)},
                      f"REST TOB {age:.0f}min old")

    def check_unresolved_past_end(self):
        self.cur.execute("""
            SELECT count(*) FROM markets m
            WHERE m.end_date < now() - interval '3 days'
              AND m.end_date > now() - interval '30 days'
              AND NOT EXISTS (SELECT 1 FROM resolutions r WHERE r.market_id = m.id)
              AND EXISTS (SELECT 1 FROM market_status s
                          WHERE s.market_id = m.id AND s.status IN ('closed','resolved'))
        """)
        n = self.cur.fetchone()[0]
        sev = "warn" if n > 0 else "info"
        self.incident("unresolved_past_end", sev, {"count": n},
                      f"{n} closed markets >3d past end w/o resolution")

    def check_future_partitions(self):
        problems = []
        for tbl in ("tob_snapshots", "book_snapshots", "book_deltas",
                    "last_trade_events", "trades"):
            self.cur.execute("""
                SELECT count(*) FROM pg_inherits i
                JOIN pg_class c ON c.oid = i.inhrelid
                JOIN pg_class p ON p.oid = i.inhparent
                WHERE p.relname = %s
                  AND substring(c.relname from '\\d{6}$') >= %s
            """, (tbl, datetime.now(timezone.utc).strftime("%Y%m")))
            future = self.cur.fetchone()[0]
            if future < MIN_FUTURE_PARTITIONS:
                problems.append(f"{tbl}={future}")
        if problems:
            self.incident("future_partitions", "crit", {"tables": problems},
                          f"LOW future partitions: {', '.join(problems)}")
        else:
            self.incident("future_partitions", "info", {}, "partitions OK (>=2 ahead)")

    def check_crossed_books(self):
        self.cur.execute("""
            SELECT count(*) FROM tob_snapshots
            WHERE capture_time > now() - interval '1 day'
              AND best_bid_mc IS NOT NULL AND best_ask_mc IS NOT NULL
              AND best_bid_mc >= best_ask_mc
        """)
        n = self.cur.fetchone()[0]
        # crossed books happen transiently; only flag if unusually frequent
        sev = "warn" if n > 500 else "info"
        self.incident("crossed_books", sev, {"count_24h": n},
                      f"{n} crossed-book TOBs in 24h")

    def check_loader_lag(self):
        buf = REPO / "data" / "buffer" / "ws"
        if not buf.exists():
            return
        now = datetime.now(timezone.utc).timestamp()
        stale = [p for p in buf.glob("*.jsonl")
                 if (now - p.stat().st_mtime) / 60 > LOADER_LAG_MIN]
        sev = "crit" if len(stale) > 2 else "info"
        self.incident("loader_lag", sev, {"stale_files": len(stale)},
                      f"{len(stale)} buffer files >{LOADER_LAG_MIN}min unprocessed")

    def check_delta_sequencing(self):
        # within a (connection, generation), ingest_sequence should be dense-ish;
        # large gaps hint at dropped WS messages
        self.cur.execute("""
            SELECT count(*) FROM (
              SELECT connection_id,
                     max(ingest_sequence) - min(ingest_sequence) + 1 AS span,
                     count(*) AS rows
              FROM book_deltas
              WHERE capture_time > now() - interval '1 day'
              GROUP BY connection_id
              HAVING count(*) > 100 AND count(*) < (max(ingest_sequence) - min(ingest_sequence) + 1) * 0.5
            ) x
        """)
        n = self.cur.fetchone()[0]
        sev = "warn" if n > 0 else "info"
        self.incident("delta_sequencing", sev, {"suspect_connections": n},
                      f"{n} WS connections with sparse sequence coverage")

    def run(self):
        self.check_ws_freshness()
        self.check_rest_freshness()
        self.check_unresolved_past_end()
        self.check_future_partitions()
        self.check_crossed_books()
        self.check_loader_lag()
        self.check_delta_sequencing()
        self.conn.commit()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quiet", action="store_true",
                    help="only send Telegram if worst severity > info")
    args = ap.parse_args()

    import psycopg2.extras  # noqa
    conn = psycopg2.connect(host="127.0.0.1", port=5432, dbname="quantlab",
                            user="quantlab", password=ENV["PG_PASSWORD"])
    dq = DQ(conn)
    dq.run()

    header = {"info": "DQ OK", "warn": "DQ WARN", "crit": "DQ CRIT"}[dq.worst]
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%MZ")
    body = f"{header} — {stamp}\n" + "\n".join(dq.lines)

    if DQ_HEALTHCHECK_URL:
        url = DQ_HEALTHCHECK_URL if dq.worst == "info" else DQ_HEALTHCHECK_URL.rstrip("/") + "/fail"
        try:
            httpx.post(url, content=body.encode(), timeout=15)
        except httpx.HTTPError:
            pass

    print(body)


if __name__ == "__main__":
    main()