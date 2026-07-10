# 2026-07-08 — Day 1

## Shipped
- First successful snapshot cycle 19:54 UTC. Collector running since.
- Hetzner signup submitted (ID verification pending). Azure storage attempted
  (region policy + directory question open). GitHub Pack checked.

## Learned / surprised
- Ronaldo market: eliminated ≠ resolved — live example of Hyp #4; 3.16M shares
  bid at 0.001 (Hyp #3 in the wild).
- CLOB bids array sorted ASCENDING — best bid is LAST. Never trust array order.
- Top-by-24h-volume rule over-selects dying markets (death throes = volume).
- macOS zcat ≠ gzip; use gzcat.

## Broke / open
- Cursor lag from file watcher → fixed with watcherExclude.
- Azure: which directory holds the student subscription? (check tomorrow)

## Tomorrow
- Overnight run review → docs questions → --slugs → Kalshi phone → Hetzner ID → deploy
paused collection ~03:50 UTC, resuming on wake-~480 cycles, ~960 files, zero gaps


"2026-07-09 ~23:28 UTC - Day 2
collection canonical on quantlab-collector-01 (Helsinki). Laptop retired. Kill test passed."
the material: Hetzner verified → Helsinki CX23 provisioned → hardened (key-only, no root, fail2ban, NTP) → deploy key → clone → systemd service live at 23:28 UTC → laptop retired. Plus tonight's learned items: silence = success, dashboard vs SSH, the quantLab path catch, Ctrl+C vs q, SSH keepalives.

# 2026-07-10 — Day 3 (infra completion)

## Shipped
- Overnight audit: 702/702 cycles, zero gaps. First flawless unattended night.
- Dead-man's switch live: healthchecks (10min/5min grace) + Telegram integration.
  Alarm drill executed — saw real DOWN and UP alerts on both channels.
- Azure backup pipeline: nightly cron (02:30 UTC) → tar → blob upload → own
  cron-type healthcheck (2h grace). Manual run OK, blob verified in portal,
  RESTORE TESTED (downloaded + listed contents). Storage key rotated after
  screenshot leak.
- Sentry certified end-to-end: deliberate crash → issue + email → resolved.
  Collector service carries DSN (verified via /proc/<pid>/environ).

## Learned / surprised
- Secrets discipline: leaked Telegram token in chat → revoked/reissued.
  Rule: secrets only ever go into password manager or server .env.
- Env var pasted was the bare account KEY, not the connection string — the
  fail-loud KeyError in the Sentry test caught a genuinely missing DSN too.
  Fail-loud > fail-silent in every test.
- systemctl show -p Environment does NOT display EnvironmentFile contents —
  measure process env at /proc/<pid>/environ. Distrust the check before the
  behavior when they conflict.
- crontab -e's "Choose 1-4" is an editor picker, not the crontab.
- Broken pipe after `| head` = tool whining that you stopped listening. Judge
  commands by what failure would look like.

## Broke / open
- Docs questions STILL not done (3 days deferred) — tomorrow's first block.
- --slugs + graceful shutdown not yet added.
- Day 2 journal entry missing? backfill.

## Tomorrow
- Docs questions (first, 60-90min) → Day 3 schema session → --slugs