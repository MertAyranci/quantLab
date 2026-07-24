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