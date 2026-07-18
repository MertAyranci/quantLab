-- ============================================================================
-- quant-lab schema v0
-- File: db/migrations/001_schema_v0.sql
-- Apply once:  docker exec -i quantlab-pg psql -U quantlab -d quantlab \
--                < db/migrations/001_schema_v0.sql
-- Implements docs/schema-design-rfc.md §6 with provisional decisions:
--   8.1 hybrid (jsonb full books + derived TOB table)
--   8.4 YES-token books only (mirror-check sampled later)
--   8.5 hot-in-PG + Parquet cold export (partitions monthly to enable it)
--   8.6 plain PG range partitions
-- Conventions (RFC R1-R12):
--   * all prices INTEGER MILLI-CENTS: price_mc = round(price * 1000)  [R3]
--   * every observation: event_time (venue) + capture_time (ours)     [R2]
--   * append-only observations; lifecycle as rows, not updates        [R4,R7]
--   * provenance: source / watchlist_rule / raw_ref everywhere        [R6,R10]
-- ============================================================================

BEGIN;

-- ---------- reference layer -------------------------------------------------

CREATE TABLE venues (
    id          smallint PRIMARY KEY,
    name        text NOT NULL UNIQUE,
    added_at    timestamptz NOT NULL DEFAULT now()
);
INSERT INTO venues (id, name) VALUES (1, 'polymarket');

CREATE TABLE events (
    id              bigserial PRIMARY KEY,
    venue_id        smallint NOT NULL REFERENCES venues(id),
    venue_event_id  text,                    -- Gamma event id (may be null early)
    slug            text,
    title           text,
    neg_risk        boolean,                 -- R11: sibling-market grouping
    category        text,
    tags            jsonb,
    first_seen_at   timestamptz NOT NULL,
    raw_ref         text,                    -- R10: raw file this came from
    UNIQUE (venue_id, venue_event_id)
);

CREATE TABLE markets (
    id                      bigserial PRIMARY KEY,
    venue_id                smallint NOT NULL REFERENCES venues(id),
    event_id                bigint REFERENCES events(id),
    venue_market_id         text NOT NULL,   -- Gamma "id"
    condition_id            text,            -- 0x… ; CLOB "market"
    slug                    text,
    question                text,
    resolution_source_text  text,            -- fine print (Hyp-4 resolution risk)
    end_date                timestamptz,
    first_seen_at           timestamptz NOT NULL,
    raw_ref                 text,
    UNIQUE (venue_id, venue_market_id)
);
CREATE INDEX idx_markets_condition ON markets (condition_id);
CREATE INDEX idx_markets_slug      ON markets (slug);

CREATE TABLE tokens (
    id            bigserial PRIMARY KEY,
    market_id     bigint NOT NULL REFERENCES markets(id),
    token_id      text NOT NULL UNIQUE,      -- huge int as text
    outcome       text,                      -- 'Yes' / 'No' / group title
    outcome_index smallint
);
CREATE INDEX idx_tokens_market ON tokens (market_id);

-- ---------- slowly-changing per-market facts (append-only) [R8] -------------

CREATE TABLE market_fees (
    market_id          bigint NOT NULL REFERENCES markets(id),
    observed_at        timestamptz NOT NULL,
    fees_enabled       boolean,
    taker_base_fee_bps integer,
    fee_exponent       numeric,
    fee_rate           numeric,
    taker_only         boolean,
    rebate_rate        numeric,
    source             text NOT NULL,        -- 'gamma' | 'ws_new_market' | …
    PRIMARY KEY (market_id, observed_at)
);

CREATE TABLE tick_sizes (
    market_id       bigint NOT NULL REFERENCES markets(id),
    observed_at     timestamptz NOT NULL,
    tick_millicents integer NOT NULL,        -- 10 = $0.01, 1 = $0.001
    source          text NOT NULL,
    PRIMARY KEY (market_id, observed_at)
);

CREATE TABLE market_status (                 -- R7 lifecycle ledger
    market_id   bigint NOT NULL REFERENCES markets(id),
    observed_at timestamptz NOT NULL,
    status      text NOT NULL CHECK (status IN
                ('active','closed','proposed','disputed',
                 'resolved','delisted_observed')),
    source      text NOT NULL,               -- 'gamma_poll' | 'ws' | '404_inference'
    PRIMARY KEY (market_id, observed_at, status)
);

CREATE TABLE resolutions (
    market_id        bigint NOT NULL REFERENCES markets(id),
    resolved_at      timestamptz,            -- venue's time if known
    winning_token_id text,
    winning_outcome  text,
    learned_via      text NOT NULL,          -- 'ws_market_resolved' | 'gamma' | …
    capture_time     timestamptz NOT NULL,
    raw_ref          text,
    PRIMARY KEY (market_id)                  -- one resolution per market
);

-- ---------- observation layer (partitioned monthly) -------------------------
-- Partition helper: new partitions created by ops script / loader on demand.

CREATE TABLE tob_snapshots (                 -- top-of-book (REST tier + derived)
    token_id      text NOT NULL,
    event_time    timestamptz,               -- venue book timestamp (ms epoch src)
    capture_time  timestamptz NOT NULL,
    best_bid_mc   integer,                   -- NULL = empty side (e.g. dead mkt)
    best_bid_size numeric,
    best_ask_mc   integer,
    best_ask_size numeric,
    source        text NOT NULL,             -- 'rest_book' | 'ws_best_bid_ask' | 'derived'
    watchlist_rule text,                     -- R6: 'vol24h_top' | 'pinned' | 'sweep'
    raw_ref       text
) PARTITION BY RANGE (capture_time);

CREATE TABLE book_snapshots (                -- full depth: REST + WS 'book' events
    token_id     text NOT NULL,
    event_time   timestamptz,
    capture_time timestamptz NOT NULL,
    bids         jsonb NOT NULL,             -- [[price_mc, size], …] ASC by price
    asks         jsonb NOT NULL,             -- [[price_mc, size], …] ASC by price
    venue_hash   text,
    source       text NOT NULL,
    watchlist_rule text,
    raw_ref      text
) PARTITION BY RANGE (capture_time);
-- NOTE: loader normalizes both sides to ASCENDING price order (raw asks arrive
-- descending — never trust venue array order; RFC book conventions).

CREATE TABLE book_deltas (                   -- WS price_change firehose [R5]
    token_id     text NOT NULL,
    event_time   timestamptz,
    capture_time timestamptz NOT NULL,
    price_mc     integer NOT NULL,
    size         numeric NOT NULL,           -- 0 = level removed
    side         char(1) NOT NULL CHECK (side IN ('B','S')),
    venue_hash   text,
    best_bid_mc  integer,
    best_ask_mc  integer
) PARTITION BY RANGE (capture_time);

CREATE TABLE trades (                        -- data-api tape + WS last_trade_price
    venue_id        smallint NOT NULL REFERENCES venues(id),
    token_id        text,
    condition_id    text,
    event_time      timestamptz NOT NULL,
    capture_time    timestamptz NOT NULL,
    price_mc        integer NOT NULL,
    size            numeric NOT NULL,
    side            text,
    outcome         text,
    wallet          text,                    -- R9
    tx_hash         text,                    -- R9
    venue_trade_key text NOT NULL,           -- tx_hash+asset or WS synthetic key
    source          text NOT NULL,           -- 'data_api' | 'ws_last_trade'
    UNIQUE (venue_id, venue_trade_key)
) PARTITION BY RANGE (event_time);

CREATE TABLE price_history (                 -- backfilled /prices-history
    token_id      text NOT NULL,
    ts            timestamptz NOT NULL,
    price_mc      integer NOT NULL,
    fidelity_min  integer NOT NULL,
    backfilled_at timestamptz NOT NULL,
    PRIMARY KEY (token_id, ts, fidelity_min)
);

-- ---------- cross-venue IP [R1] ----------------------------------------------

CREATE TABLE event_matches (
    id         bigserial PRIMARY KEY,
    event_a    bigint NOT NULL REFERENCES events(id),
    event_b    bigint NOT NULL REFERENCES events(id),
    method     text NOT NULL,                -- 'manual' | 'slug_fuzzy' | …
    confidence numeric,
    notes      text,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (event_a < event_b)                -- one row per pair
);

-- ---------- ops --------------------------------------------------------------

CREATE TABLE parsed_files (                  -- backloader idempotency ledger
    file_path  text PRIMARY KEY,
    parsed_at  timestamptz NOT NULL,
    row_counts jsonb,
    parser_sha text
);

CREATE TABLE collector_runs (
    id          bigserial PRIMARY KEY,
    component   text NOT NULL,
    started_at  timestamptz NOT NULL,
    version_sha text,
    notes       text
);

CREATE TABLE dq_incidents (
    id          bigserial PRIMARY KEY,
    observed_at timestamptz NOT NULL,
    check_name  text NOT NULL,
    severity    text NOT NULL,
    details     jsonb
);

-- ---------- initial partitions (Jul–Sep 2026; loader auto-extends) ----------

DO $$
DECLARE t text; m date;
BEGIN
  FOREACH t IN ARRAY ARRAY['tob_snapshots','book_snapshots','book_deltas','trades']
  LOOP
    FOR i IN 0..2 LOOP
      m := date '2026-07-01' + (i || ' months')::interval;
      EXECUTE format(
        'CREATE TABLE %I_%s PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
        t, to_char(m,'YYYYMM'), t, m, m + interval '1 month');
    END LOOP;
  END LOOP;
END $$;

-- ---------- indexes on observations ------------------------------------------

CREATE INDEX idx_tob_token_time   ON tob_snapshots  (token_id, capture_time);
CREATE INDEX idx_books_token_time ON book_snapshots (token_id, capture_time);
CREATE INDEX idx_deltas_token_time ON book_deltas   (token_id, capture_time);
CREATE INDEX idx_trades_cond_time ON trades         (condition_id, event_time);
CREATE INDEX idx_trades_wallet    ON trades         (wallet);
CREATE INDEX idx_ph_token         ON price_history  (token_id, ts);

COMMIT;