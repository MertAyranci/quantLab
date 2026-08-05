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




Day A:

# Backfilled journal entries — Days B & C + weekly review

> Save each section as its own file under `docs/journal/`.
> Dates approximate where the session ran past midnight UTC.

---

## FILE: docs/journal/2026-07-19.md

# 2026-07-19 — Day B: the database gets its memories

## Shipped
- `db/backloader.py` written, reviewed, shipped through the Mac→GitHub→server
  pipeline.
- **Sample import** (2026-07-09, 66 files) reconciled exactly:
  103 unique markets · 206 tokens · 645 book_snapshots · 645 tob_snapshots ·
  66 receipts. Per-file processing counts (3,300 market upserts) vs unique rows
  (103) confirmed dedup working.
- **Full load: ~27,400 files → 273,357 book snapshots in 7m18s, zero failures.**
  Idempotency demonstrated live: previously-parsed day returned empty totals.
- Ronaldo resurrection queryable in SQL (bid 999mc, ask NULL — the empty-ask
  book first seen in raw JSON on Day 1, now a 3-table JOIN in milliseconds).
  First row at 23:25:02 = the laptop's final cycle before server handover;
  both machines' data seamlessly in one table.
- pg_dump added to nightly Azure backup; restore-tested.

## Learned
- **First research query on own data — spread histogram:** modal spread 1mc
  (117k obs), then 10mc (52k), 2mc, 3mc. **Bimodal by tick regime** — spreads
  cluster at integer multiples of the *local* tick size (0.001 regime above
  0.96 / below 0.04; 0.01 otherwise).
  → **Methodological rule: raw spread is not comparable across markets;
  spread-in-ticks is.** Pooling raw spreads fabricates structure. This is why
  v0.2 stores tick history per token.
- Transaction-wrapped migrations turn a schema bug into a free lesson
  (partition-key constraint on `trades` UNIQUE → clean ROLLBACK, nothing half-applied).
- 1,852 metadata versions / ~1,800 status changes across ~1,850 distinct
  markets — metadata churn is measurable and now queryable.

## Broke / open
- Migration 001 rewritten in place twice (v0 → v0.1 → v0.2) under the
  empty-database exception. **Exception window now closed: all future schema
  changes are numbered migrations 002+, motivated by real data.**

---

## FILE: docs/journal/2026-07-21.md

# 2026-07-19..23 — Day C: the resolved-market archive

## Shipped
- `db/backfill_prices.py` — subclasses the backloader (shared caches, token
  resolution, upsert machinery), walks Gamma `closed=true` keyset, records
  resolutions, harvests price history.
- Ran ~4 days in tmux. **Stopped by decision, not exhaustion:**
  **1,462,357 resolutions · 25,397,743 price points · ~1.5M markets.**
  Sufficiency reached; disk contested by the incoming WS collector.
  Resumable via `parsed_files` receipts.

## Probe findings (all now in polymarket-api-reference.md)
- `closed=true` keyset walk starts at the OLDEST markets — the archive reaches
  back to 2020 (Biden-coronavirus, Airbnb-IPO era).
- Ancient markets carry `outcomePrices: ["0","0"]` — no winner encoded.
  Modern ones carry `["0","1"]` + `umaResolutionStatus: resolved`.
  → **Defensive rule adopted: exactly one outcome == "1" → write resolution;
  otherwise archive as closed and COUNT it. Never fabricate a winner.**
  Resolved-rate climbed 81% → 91% as the walk reached the modern era.
- **`closedTime` ≠ `endDate`.** Markets resolve early when reality settles the
  question (observed: closedTime 2026-07-18 vs endDate 2028-01-01).
  closedTime is the true resolution timestamp.
- **Fidelity floor ≈ 10 min, but fine history only reaches ~30 days back.**
  Only `fidelity=1440` (daily) reaches market birth.
  → Fine-grained history EXPIRES on a rolling window; harvest promptly after
  resolution (backfill does a 25-day fine pass; live collector inherits duty).
- `/books` batch endpoint confirmed: POST `[{"token_id": …}]` → list of books.

## Learned
- tmux is a chord, not typed text (Ctrl+B release, then D). `docker kill` is
  treated as an operator stop by `unless-stopped`; a genuine crash test needs
  an in-container kill.
- Placeholder strings (`PASTE_TOKEN_ID`, `FIRST_ID`) reach the API literally.
  → Chain commands with shell vars instead of paste-slots.

---

## FILE: docs/journal/weekly-review-2026-07-24.md

# Weekly review — through 2026-07-24 (roadmap Week ~3)

## Against the roadmap

**Phase 1 (Data Layer, Weeks 1–4) — substantially ahead on depth, behind on breadth.**
- ✅ Polymarket collector: 16 days continuous, ~2,880 files/day, zero unexplained gaps
- ✅ Deployed, monitored (healthchecks + Telegram + Sentry), backed up (2 domains, restore-tested)
- ✅ Postgres + schema v0.2 (survived 2 external review rounds), 273k snapshots loaded
- ✅ Resolved-market archive: 1.46M resolutions, 25.4M price points — **far beyond roadmap ambition**
- ❌ Kalshi collector — not started
- ❌ Odds feed (Betfair/aggregator) — not started
- ⏳ Data-quality checks — designed, not built
- ⏳ Write-up #1 — material accumulating (spread histogram, census, H3 result)

**Phase 2 (Research) — started early, first verdict delivered.**
- ✅ **H3 (favorite–longshot): TESTED AND KILLED** with four documented artifacts
- Remaining hypotheses now better prioritized (see below)

## Key judgement this week

The H3 result reframes the project: **Polymarket is efficient at the level a
retail participant can measure it.** Any surviving edge must come from *speed*
(H2 lead–lag, H6 news latency), *cross-venue relationships* (H1), or
*microstructure* (H5 adverse selection) — not from the crowd being wrong about
probabilities. H4 (resolution drift) remains open but needs fine-fidelity data
that expires on a 30-day window.

## Corrections to plan
1. **Storage tiering** replaces "collect everything" — measured 1,000
   changes/min/token makes full-delta capture infeasible (10 GB/day at 50 tokens).
2. **Betfair moves up the priority list** (see open question below).
3. Kalshi remains valuable for H1 but is no longer the obvious next venue.

## Open question — LOCATION
Now in London. Roadmap assumed Istanbul through September; UK blocks Polymarket
order placement. **Phase 3 (live trading, Weeks 9–12) needs re-planning if the
move is permanent** — natural substitute is Betfair Exchange (legal here,
order-book venue, account already exists). Decide before building further
venue adapters.

## Next fortnight
1. buffer_loader + systemd + 48h soak (finish collector v2)
2. Data-quality checks v1
3. Write-up #1 draft ("Mapping the Event-Market Landscape") — has census,
   spread/tick finding, composition finding, and H3 as content
4. Venue #2 decision (Betfair vs Kalshi) → collector



2026-07-24 — Day C+ : first research verdict (H3 killed)

Shipped


Backfill stopped by decision (sufficiency, disk contention). Final census:
1,462,357 resolutions · 25,397,743 price points · ~1.5M markets processed
across Polymarket's 6-year closed archive. Resumable via receipts if ever needed.
WS collector certified: price_changes fan-out bug fixed (was undercounting
10x), graceful shutdown added (no more Sentry mail on Ctrl+C), sequencing
contiguous (1..N, no gaps), reconcile 1/10 drift over 5 min.
Catch-up backload run (run_id=7): all days 07-09→07-23 already receipted
(idempotency proven across 7 runs); only today's 8 files were new.
First hypothesis tested end-to-end and killed (H3 — see verdict memo).


Measured / decided


WS delta rate ~1,000 price_changes/min/token after fan-out fix.
50-token full-delta storage = ~10 GB/day → INFEASIBLE on 38 GB disk.
Tiering adopted (answers RFC Q8.5 by measurement, not guesswork):

Tier 1: best_bid_ask + last_trade_price + ws_book for full watchlist
Tier 2: full deltas for 3–5 hand-picked "deep study" markets only (H5)
Tier 3: REST snapshot collector continues as-is (safety net + continuity)



Disk 46% after backfill stop; DB ~8.5 GB.


Learned / surprised


Dataset composition: only ~35% of markets are classic Yes/No.
471k are "Up" (crypto/equity direction), 243k "Over" (sports totals).
Any calibration claim must be stratified by market type.
"Up" markets are calibrated to 0.1pp (45,583 obs, implied 50.0 →
realized 50.1). This doubles as an instrument-validation certificate: if the
token/resolution join were broken, this number would not land on 50.
The 48–52¢ pile is NOT untraded initialization prices — it's genuine
coin-flips (esports odd/even, first blood, O/U props). Correctly priced.
neg_risk flag ≠ logical exclusivity. 562,841 events flagged FALSE, yet
events with 11+ sibling markets show 49¢ contracts realizing 26%.
Sibling count is the correct control; the venue's flag is not.
Never edit code on the server (git pull conflict). Never hand-fix
indentation (3-space def cost a session). py_compile before every commit.


Broke / open


Bucket 10 anomaly chased through 4 queries → fully explained by event size.
Paperwork backlog now cleared through today.
Still pending: buffer_loader.py, systemd units for WS collector, 48h soak,
002 migration (unapplied, waiting on Day D source strings).


Tomorrow


H3 robustness checks (horizon stability, year-by-year, confidence intervals)
→ then buffer_loader + soak.


# Documentation patch — 2026-07-24

> Apply these edits to the two knowledge artifacts. Each block says which file
> and where. After applying, the docs match reality and the TODO lists shrink.

---

## PATCH 1 → `docs/polymarket-api-reference.md`, §6 (Price history)

Replace the TODO line with:

```markdown
- **Fidelity/depth trade-off (VERIFIED 2026-07-19):** `fidelity` is minutes per
  point. Floor is ~10 min (fidelity=1 returns the same series as 10). BUT
  granularity trades against reach: fidelity=60 → ~720 points ≈ 30 days;
  fidelity=10 → ~4,300 points ≈ 30 days; **only fidelity=1440 (daily) reaches
  market birth** (verified: 425 points to 2025-05-03 on a 14-month market).
  → Daily = archive workhorse. **Fine history EXPIRES on a ~30-day rolling
  window** — harvest promptly after each resolution or lose it permanently.
  (Backfill runs a 25-day fine pass; the live collector inherits this duty.)
```

## PATCH 2 → `docs/polymarket-api-reference.md`, new subsection in §5

```markdown
### 5.1 Resolution encoding (VERIFIED 2026-07-19)
- Gamma market objects carry `outcomePrices` (JSON string array). Modern
  resolved markets: `["1","0"]` / `["0","1"]` — the "1" marks the winner.
  Also `umaResolutionStatus: "resolved"`.
- **Ancient markets (2020–21 era) carry `["0","0"]`** — no winner encoded.
  Resolved-rate over the full archive walk: ~81% early, ~91% by the modern era.
- **Parsing rule:** exactly one outcome equal to "1" → record resolution;
  anything else → record market as closed and COUNT as unresolvable.
  Never fabricate a winner.
- **`closedTime` is the true resolution timestamp, NOT `endDate`.** Markets
  settle early when reality answers the question (observed: closedTime
  2026-07-18 vs endDate 2028-01-01).
- `closed=true` keyset walk returns OLDEST markets first; archive reaches 2020.
```

## PATCH 3 → `docs/polymarket-api-reference.md`, §4 (Rate limits) — append

```markdown
**`/books` batch endpoint shape (VERIFIED 2026-07-24):**
`POST https://clob.polymarket.com/books` with body `[{"token_id":"…"}, …]`
returns a list of book objects (same shape as GET /book). Use for multi-token
sweeps; 500 req/10s budget applies to the batched calls, not the tokens inside.
```

## PATCH 4 → `docs/polymarket-api-reference.md`, §11 (TODOs) — update

```markdown
- [x] Fidelity floor — ANSWERED (see §6): ~10 min floor, ~30-day reach; daily reaches birth
- [x] `/books` batch shape — ANSWERED (see §4): POST with [{"token_id": …}]
- [x] Market-WS connection/subscription caps — ANSWERED empirically 2026-07-17:
      200 tokens on ONE connection accepted; single connection suffices at our scale
- [x] Live WS listen incl. custom_feature events — DONE 2026-07-17 and 2026-07-24
- [ ] Liquidity histogram over census (Write-up #1 chart) — needs volume/liquidity
      fields, NOT currently captured by the backloader (candidate migration 003)
- [ ] Sports-socket mini-collector (H4/H6 event triggers)
```

## PATCH 5 → `docs/polymarket-api-reference.md`, §7.1 — append to the market-channel block

```markdown
**Measured message rates (2026-07-24, top-10 markets by 24h volume):**
~1,000 `price_change` **changes** per minute PER TOKEN after correct fan-out
(each WS message carries a `price_changes` array; each element is a level
change with its own `asset_id`). Naive per-message counting undercounts ~10x.
Also observed: `ws_book` re-snapshots ~1/token/2min (strong self-healing),
`best_bid_ask` ~10/min/token, `last_trade_price` ~0.5/min/token,
`new_market` 3–24/min across the venue (markets are created constantly).
```

---

## PATCH 6 → `docs/schema-design-rfc.md`, §8 — mark Q8.5 DECIDED

```markdown
**8.5 Retention — DECIDED 2026-07-24 BY MEASUREMENT.**
Measured WS delta rate: ~1,000 changes/min/token. At a 50-token watchlist that
is ~72M rows/day (~10 GB/day Postgres, ~25 GB/day buffer) against a 38 GB disk.
Full-delta capture across the watchlist is therefore INFEASIBLE, and the
question answers itself as a tiering design rather than a retention policy:

- **Tier 1 (breadth):** `best_bid_ask` + `last_trade_price` + `ws_book`
  snapshots for the full watchlist. Few hundred MB/month. Serves H1, H2, H4, H6.
- **Tier 2 (depth):** full `price_change` deltas for 3–5 hand-picked deep-study
  markets only. Serves H5 (adverse selection). Depth research needs depth, not
  breadth — a few well-observed books beat fifty shallow ones.
- **Tier 3 (continuity):** the REST snapshot collector continues unchanged
  (~30 MB/day) as safety net and as the unbroken series since 2026-07-08.
- Cold storage (Parquet → Azure) remains available if Tier 2 grows; not needed
  at current sizing.

Loader consequence: `buffer_loader.py` routes by `kind`; `ws_collector.py`
takes a `--deep-tokens` flag so Tier 2 membership is explicit and small.
```

## PATCH 7 → `docs/schema-design-rfc.md`, §8 — add a new decided question

```markdown
**8.10 (NEW) Event-exclusivity control — DECIDED 2026-07-24 BY MEASUREMENT.**
Polymarket's `neg_risk` flag does NOT reliably identify mutually-exclusive
market groups (562,841 events flagged FALSE still exhibit the pattern).
Measured: in the 44–56¢ band, gap-to-realized scales with sibling count —
1 market: +6.6pp · 2–3: −6.4pp · 4–10: −2.9pp · **11+: −23.4pp**.
→ Any calibration/probability study MUST control on markets-per-event
(sibling count), not on the venue's flag. See research/h3_favorite_longshot.md.
```

# 2026-07-24 (evening) — Collector v2 deployed; soak surfaces two issues

## Shipped
- `db/buffer_loader.py` written, reviewed, deployed. Routes WS buffer records
  into tiered tables per RFC Q8.5:
  - Tier 1 (breadth): best_bid_ask → tob_snapshots; last_trade_price →
    last_trade_events; ws_book/rest_resync → book_snapshots (+ derived tob).
  - Tier 2 (depth): price_change → book_deltas, ONLY for config/deep_tokens.txt.
  - Reference: tick_size_change → tick_sizes; market_resolved → resolutions +
    status; new_market → markets/tokens/fees.
- Both halves under systemd (ws-collector, buffer-loader), auto-restart,
  EnvironmentFile-wired, two new healthchecks (ws-collector, buffer-loader).
- **Certified end to end:** delta 52,650 in one manual run; live tick_size_change
  captured (tick:2 — a market crossing the 0.96/0.04 boundary in real time);
  lag=0 sustained.

## Measured (the soak earning its keep)
- **Tier-2 delta rate WILDLY above estimate.** 3 deep tokens = ~52M rows/day.
  Cut to 1 token → still ~10 GB/day disk growth (47%→54% in 7h). That single
  token is one of the venue's busiest.
  → **Lesson: Tier-2 membership must be sized by MEASURED message rate, not
  token count.** Even one hot token can dominate.
- **Loader memory LEAK: 592 MB → 1.8 GB over 7h, still climbing.** Inherited
  Backloader caches (1.5M markets, 3M tokens, 1.5M parsed_files preloaded) were
  designed for one-off batch runs, not a long-lived daemon. On a 4 GB box with
  Postgres resident, this will eventually OOM. **Tech debt, must fix before
  Tier 2 runs unattended.**

## Decision
- **Tier 2 PAUSED for the soak** (deep_tokens.txt emptied). Rationale: delta
  pipeline already proven; soak's remaining job is stability, which is easier
  to diagnose without the firehose. Also isolates the memory-leak diagnosis:
  if memory still climbs with Tier 2 off, leak is in caches; if it stabilizes,
  leak scales with delta volume.

## Open / tech debt
- [ ] buffer_loader memory: replace preloaded caches with DB lookups (token
      resolution via query, drop parsed_files preload — daemon tracks its own
      session). Blocks unattended Tier 2.
- [ ] Retention design for Tier 2 before re-enabling (partition drop / Parquet
      export / sampling) — RFC Q8.5 sizing was guesswork, now measured.
- [ ] scripts/ not in git (backup_to_azure.sh exists in one place, no history);
      gitignore backups/.
- [ ] Watchlist is 25; can raise Tier 1 later (cheap) once loader is fixed.

## Milestone
**Phase 1 (Data Layer) functionally complete** ~1 week ahead of the 4-week
roadmap allotment: REST collector (16d unbroken) + WS collector v2 (tiered) +
loader + queryable DB (273k snapshots, 1.46M resolutions, 25M price points) +
5-component monitoring + 2-domain backups + H3 tested & killed.

## Next
1. Fix loader memory (unblocks Tier 2).
2. Data-quality checks (Day F) — last Phase-1 piece.
3. Write-up #1 (census + spread/tick + composition + H3).
4. LONDON DECISION → venue #2 (Betfair vs Kalshi).



2026-08-01 — Close-detection + loader hardening.

Diagnosed closed: 0 → confirmed missing close-detection: resolved markets (has_resolution=t) stuck in watching. Built detect_closures() in harvester: marks markets closed on resolution/closed-status, records final price, runs first in run_pass() so freed slots refill same-pass. First 7 Grade-A markets banked. Cohort self-refreshes.
Loader memory creep (672 MB) fixed: DB-first token lookup + bounded token_ids/unresolved caches (cap 50k, clear-on-overflow). Memory 672 MB → 32 MB, now flat on long runs. Also fixed dead unresolved-miss code that re-queried Gamma endlessly.
Full health sweep green: services active, disk 55%, DQ OK all checks, fresh data flowing (732 tob/5min).
Grade-A dataset now accumulating autonomously toward tight-horizon H4-Cal (runnable ~mid-week when closed reaches a few dozen).

2026-08-02 — Harvester cohort tuning. Diagnosed closed flatlined at 7: cohort silted with slow-resolving markets (imminent lingerers past scheduled_close + near-certainties admitted up to 30d out, squatting slots). Fix: reserved 45/60 slots for imminent (fast churn guaranteed), tightened near-certainty window to 7d, evict stale imminent (>1d past close) and too-distant near-certainties (>7d out, no last-mile yet — safe re: survivorship). Fixed evict() bug: too_distant/maxdur → evicted_maxdur, only close-detection → closed (keeps Grade-A set clean). Reshaped cohort: 25 evicted, 60 refilled imminent-heavy. Grade-A now accumulating faster.

# Journal — 2026-08-05

## Mac compromise & full recovery
- Downloaded a phishing app; wiped Macintosh HD and rebuilt from scratch.
- **Zero project loss** — everything lives on GitHub + the Hetzner server, which
  ran autonomously throughout. Mac was only an editing terminal.
- Recovery: reinstalled brew/git/python, re-cloned repo over SSH (generated new
  key, added to GitHub), restored server access via Hetzner console + pulling the
  new key from `github.com/MertAyranci.keys` into authorized_keys.
- **Security rotation** (phishing hygiene): changed GitHub password, email,
  exchange, and confirmed no wallet seed/keys were on the wiped Mac. Old Mac SSH
  key to be removed from server authorized_keys.
- Server health after recovery: all collectors active, disk 58%, loader memory
  bounded. Nothing broke while away.

## Paper soak — H4 live verdict (concluded)
- Soak ran ~1,490 passes over the downtime. Result: 19 open positions, ~$332
  deployed, unrealized drifting to ~-$25, **realized 0 (nothing resolved)**.
- Root cause revealed: the >=95c entry filter selects LONG-DATED parked markets
  (2026 political primaries, central-bank meetings) that resolve weeks/months out.
  Capital locks up, drifts negative on spread, never realizes.
- This is the **capital-lockup mechanism** made concrete — a sharper H4 negative
  than H4-Exec alone. Soak stopped; final state saved to
  logs/paper_soak_final_state.txt.
- H4 verdict now has THREE independent confirmations: H3 calibration (efficient),
  H4-Exec (no near-cert window on sports), paper soak (capital lockup on parked).

## Crypto-H4 — new hypothesis, capture layer built & PROVEN
User's insight: 5-minute crypto Up/Down markets DO reach near-certainty before
resolving (unlike sports), so they may be the right shape for H4. Decided to test.

Findings & builds:
- **Discovery**: daily discovery misses 5m markets (they live 5 min). Firehose
  queries (startDate desc / endDate asc) return stale/future markets, not live
  ones. **Solution: compute-from-clock** — slugs encode a timestamp; fetch the
  live slugs directly. Deterministic and reliable.
- **CRITICAL: slug timestamp = START boundary, resolves at slug_ts + 300.** Not
  the end time. All retention/checkpoint timing must key off Gamma `endDate`.
  (Caught before it shifted every observation by a full market duration.)
- **Eviction bug (diagnosed from ws-collector logs)**: harvester's "current +
  next N" watchlist dropped resolving cohorts 60-235s BEFORE resolution —
  destroying exactly the last-mile data H4 tests. Fix: **union retention** — keep
  every cohort until end_date + 15s grace; never unsubscribe an unresolved market.
- **Both tokens**: near-certainty appears on either side (near-cert Down = Up~5c),
  so watchlist BOTH outcome tokens (0 and 1).
- **Final checkpoint**: harvester REST-fetches both books at T-0 and writes a
  `crypto_final_checkpoint` tob_snapshot, guaranteeing the resolution-instant
  state regardless of collector timing.
- Collector wired to read watchlist_crypto.txt (union of both watchlist files);
  battle-tested core otherwise untouched (fix lives in harvester, not collector).
- Migration 005: crypto_watch admission ledger.
- Cron: `* * * * * flock -n ... crypto_harvester.py` (flock prevents overlapping
  passes when a checkpoint loop runs long).

**Acceptance test PASSED**: dense final-60s capture (BTC 482 snaps in last minute),
final_checkpoints >=1 per market, closest_to_resolve at -2 to -90s (reaches
resolution, not cut off early), both outcome_index 0 AND 1 captured. Valid
last-mile data now accumulating every 5 min across 8 assets.

Preliminary signal (from sparse pre-fix data): near-certainty exists (BNB ~5c at
T-60) BUT spreads are huge (XRP bid 340/ask 560 = 22c spread at T-60). Likely the
same execution wall — but now we'll MEASURE it rigorously instead of assuming.

## Disk watch
Crypto capture is dense (BTC ~2,121 snaps/market/5min). Monitor disk over next day.
BBO-dedup optimization (suppress identical consecutive snapshots, keep heartbeat)
deferred until data quality confirmed — per "fix coverage first, optimize second."

## State at session end
- Crypto-H4: capture live, proven, accumulating. NEXT: let accumulate ~1 day →
  build crypto Grade-A selector + run H4-Exec (spread is the key metric).
- H2: ready to analyze (20k odds snaps, 30 matched games) — deferred.
- Execution engine: complete (48 tests), paper soak concluded.
- Machine healthy post-recovery.

## Next session options
1. Crypto-H4 H4-Exec run (once a day of cohorts banked) — the verdict.
2. H2 lead-lag analysis (data ready now).
3. Disk/dedup optimization if crypto capture pressures storage.