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