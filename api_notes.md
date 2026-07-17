## Q1 — Pagination / enumerating the universe
Doc says: [1-line confirm from Gamma docs: /markets/keyset, after_cursor, limit≤100]
API showed:
- /markets + offset works but hard-caps between 2000–3000 → JSON error OBJECT
  ("use /markets/keyset"), not a list. Parse defensively.
- /markets/keyset envelope: {$schema, markets[], next_cursor}. Request param
  is after_cursor. Walk verified (pages 1-3 distinct).
- CENSUS 2026-07-13: 57,809 active markets, 579 pages, ~3.5 min at 0.3s pacing.
- urllib default UA gets 403 from Cloudflare; custom UA (httpx) passes. Always
  send a real User-Agent.
Design consequence: discovery job = daily keyset walk (full universe metadata);
snapshot tiers forced: universe metadata daily / top-N books minutely / watch-
list depth 30-60s. Census is an upper bound (active flag lags resolution — see
404 incident). TODO Day 3: liquidity histogram over full census.

## Q2 — Historical trades: endpoint + depth
**Doc says:** [pending 1-line check of data-api /trades docs — confirm no
  deeper-history params exist beyond offset]
**API showed:**
- data-api /trades returns rich records: price, size, side, timestamp,
  outcome/outcomeIndex, BOTH tokens' assets, proxyWallet, transactionHash.
  Newest-first, offset-paginated.
- HARD CAP: offset ≤ 3000 → rolling window of most recent ~3,000 trades per
  market ("max historical activity offset of 3000 exceeded"). For Rihanna
  market, 3,000 trades ≈ ~6 weeks; busier markets = shorter window in time.
- `before` timestamp param is ignored (returned today's trades) — no time
  tunnel past the cap.
- BUT: clob /prices-history?market=<token>&interval=max&fidelity=1440 returns
  full price series back to MARKET BIRTH — verified: Rihanna market, 425 daily
  points, 2025-05-03 → today. Full history in price form, on demand.
**Design consequence:**
- Week 4 backfill = prices-history over the resolved-market census (Q1 recipe)
  joined with outcomes → Hypothesis 3 calibration study runnable as soon as
  schema + backfill script exist (moved up from Week 6).
- Trade TAPE: collect forward from now (our own tape = the asset); the 3k
  rolling window means launch the trade collector soon or lose tape.
- Probe fidelity floor (fidelity=60?) for Hyp 4's last-48h study.
- Trades schema includes wallet + tx_hash columns from birth (parking lot:
  wallet-level analysis; full historical tape via Polygon/subgraph if ever
  needed).


## Q3 — Websocket: full book vs deltas [CLOSED 2026-07-17]
Doc says: market channel (wss://ws-subscriptions-clob.../ws/market), subscribe by
  assets_ids (token IDs), custom_feature_enabled:true unlocks best_bid_ask /
  new_market / market_resolved. Event types: book (full snapshot on subscribe +
  after book-affecting trades), price_change (DELTAS: changed levels only,
  size "0" = level removed), tick_size_change (tick shrinks near 0.04/0.96!),
  last_trade_price, best_bid_ask.
Desync: hash field on book + price_change (state checksum); NO sequence numbers
  → design = maintain local book, apply deltas, hash-verify, REST re-fetch on
  mismatch + on reconnect; periodic REST reconciliation as backstop. Fresh book
  snapshots after trades partially self-heal.
Heartbeat: WE send PING every 10s (sports channel is reversed: reply pong).
  Verified live: PONG per PING, 8/8 over 90s.
Limits — EMPIRICAL (2026-07-17): 200 tokens subscribed on ONE connection,
  accepted without error; messages flowed for full 90s test. Single connection
  suffices for watchlist scale (2x planned watchlist); no multiplexing needed.
Live listen results (top-100-by-volume markets, both tokens, 90s):
  price_change 14,816 | best_bid_ask 680 | book 338 | last_trade_price 69 | PONG 8
  → 1. DELTA FIREHOSE: ~165 msg/s on liquid-200. Depth collector's real
       challenge is the WRITE PATH — must batch inserts (or append-then-load),
       not row-per-message into Postgres.
  → 2. SELF-HEALING IS STRONG: 338 books = 200 subscribe-snapshots + ~138
       re-snapshots in 90s ≈ one free full refresh per token per ~2 min on
       liquid markets → lowers desync-recovery burden on hash-checking.
  → 3. QUOTE/TRADE RATIO ~200:1 (14,816 deltas vs 69 trades) = heavy MM/bot
       repositioning visible in raw flow — preview of Hyp 5's adverse-selection
       environment. last_trade_price too sparse to be the sole trade tape;
       keep data-api polling as planned.
  custom_feature verified: best_bid_ask events flowed (only exist if flag took).
  new_market / market_resolved not observed in 90s (nothing resolved) —
  subscription acceptance is the verification; event handling to be confirmed
  when first one arrives in production.
Design consequence: Day 4-5 depth collector = book engine (nontrivial, budget
  2 days) with BATCHED write path sized for ~165+ msg/s. Subscribe watchlist
  tokens + custom features on one connection. market_resolved feed REPLACES
  404-inference for resolutions; new_market feed replaces discovery polling
  (census → reconciliation). Capture fee_schedule/taker_base_fee per market →
  Week 5 cost model is per-market, curved fees. tick_size_change must be
  stored (affects price grid). Sports socket = free event-trigger feed for
  Hyp 4/6 (Week 2-3 candidate). RTDS crypto feed = reference prices for the
  crypto-calibration study.


  ## Q4 — Rate budgets → collector frequencies
Doc says: Gamma /markets 300/10s; CLOB /book 1500/10s, /books(batch!) 500/10s,
  /prices-history 1000/10s; data-api /trades 200/10s. Cloudflare throttling,
  sliding windows. (Perps doc = separate product, N/A; WS caps to cross-check
  in Q3.)
Arithmetic: current load 0.4 req/s = 0.3% of /book budget. Day-4 tiers
  (50-depth/30s + 500-sweep/5min + census/day + tape/2min) ≈ 3.5 req/s ≈ 2-3%
  of relevant budgets. Backfill: ~40k prices-history @ 10-20 req/s = 35-70 min
  TOTAL — one evening, not multiple nights.
Design consequence: rate limits are NOT a binding constraint at our scale;
  tier sizing driven by data value + storage instead. Adopt /books batch
  endpoint in Day-4 collector (probe request shape). Keep 50% ceiling + pacing
  as etiquette. Latency logging remains the throttle detector.