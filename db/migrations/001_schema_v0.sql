-- ============================================================================
-- quant-lab schema v0.2 — FROZEN for sample import (further changes = 002+)
-- File: db/migrations/001_schema_v0.sql
-- Reset + apply (permitted ONLY while DB is empty):
--   docker exec -i quantlab-pg psql -U quantlab -d quantlab \
--     -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
--   docker exec -i quantlab-pg psql -U quantlab -d quantlab \
--     < db/migrations/001_schema_v0.sql
--
-- UNIT DEFINITION (deliberate, documented, not renaming):
--   *_mc columns = price x 1000 (thousandths of a DOLLAR): 0.001->1, 1.00->1000.
--   Valid range 0..1000. tick_mc likewise (10 = $0.01 tick, 1 = $0.001 tick).
--
-- v0.2 (external review round 2, triaged):
--   ACCEPTED: composite FKs for token-market & resolution-token consistency;
--     sequencing + book_generation on WS snapshots & deltas; token-level
--     tick_sizes; fee_rate_bps + required sequencing on last_trade_events;
--     trades source CHECK; price/size CHECKs; replay + token-time indexes;
--     partitions through Nov 2026; provenance columns completed.
--   MODIFIED: collector_run_id is NULLABLE everywhere (operability; loaders
--     are expected to populate it, DB does not force it).
--   REJECTED: price_mc rename (definition documented above); default partition
--     (DQ alerts on <2 future partitions instead).
-- ============================================================================

BEGIN;

-- ---------- reference & ops -------------------------------------------------

CREATE TABLE venues (
    id       smallint PRIMARY KEY,
    name     text NOT NULL UNIQUE,
    added_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO venues (id, name) VALUES (1, 'polymarket');

CREATE TABLE collector_runs (
    id          bigserial PRIMARY KEY,
    component   text NOT NULL,
    started_at  timestamptz NOT NULL,
    version_sha text,
    notes       text
);

CREATE TABLE events (
    id             bigserial PRIMARY KEY,
    venue_id       smallint NOT NULL REFERENCES venues(id),
    venue_event_id text,
    slug           text,
    title          text,
    neg_risk       boolean,
    category       text,
    tags           jsonb,
    first_seen_at  timestamptz NOT NULL,
    source         text NOT NULL,
    raw_ref        text,
    collector_run_id bigint REFERENCES collector_runs(id),
    UNIQUE (venue_id, venue_event_id)
);

CREATE TABLE markets (
    id                     bigserial PRIMARY KEY,
    venue_id               smallint NOT NULL REFERENCES venues(id),
    event_id               bigint REFERENCES events(id),
    venue_market_id        text NOT NULL,
    condition_id           text,
    slug                   text,
    question               text,
    resolution_source_text text,
    end_date               timestamptz,
    first_seen_at          timestamptz NOT NULL,
    source                 text NOT NULL,
    raw_ref                text,
    collector_run_id       bigint REFERENCES collector_runs(id),
    UNIQUE (venue_id, venue_market_id),
    UNIQUE (id, venue_id)          -- enables composite FK from tokens
);
CREATE INDEX idx_markets_condition ON markets (condition_id);
CREATE INDEX idx_markets_slug      ON markets (slug);

CREATE TABLE tokens (
    id             bigserial PRIMARY KEY,
    venue_id       smallint NOT NULL REFERENCES venues(id),
    market_id      bigint NOT NULL,
    venue_token_id text NOT NULL,
    outcome        text,
    outcome_index  smallint,
    source         text NOT NULL,
    raw_ref        text,
    collector_run_id bigint REFERENCES collector_runs(id),
    UNIQUE (venue_id, venue_token_id),
    UNIQUE (market_id, outcome_index),
    UNIQUE (market_id, id),        -- enables composite FK from resolutions
    FOREIGN KEY (market_id, venue_id) REFERENCES markets (id, venue_id)
);
CREATE INDEX idx_tokens_market ON tokens (market_id);

CREATE TABLE market_metadata_versions (
    id                     bigserial PRIMARY KEY,
    market_id              bigint NOT NULL REFERENCES markets(id),
    capture_time           timestamptz NOT NULL,
    question               text,
    resolution_source_text text,
    end_date               timestamptz,
    slug                   text,
    source                 text NOT NULL,
    raw_ref                text,
    collector_run_id       bigint REFERENCES collector_runs(id)
);
CREATE INDEX idx_mmv_market ON market_metadata_versions (market_id, capture_time);

-- ---------- slowly-changing facts (append-only) ------------------------------

CREATE TABLE market_fees (
    market_id          bigint NOT NULL REFERENCES markets(id),
    observed_at        timestamptz NOT NULL,
    fees_enabled       boolean,
    taker_base_fee_bps integer,
    fee_exponent       numeric,
    fee_rate           numeric,
    taker_only         boolean,
    rebate_rate        numeric,
    source             text NOT NULL,
    raw_ref            text,
    collector_run_id   bigint REFERENCES collector_runs(id),
    PRIMARY KEY (market_id, observed_at)
);

CREATE TABLE tick_sizes (              -- v0.2: token-level (WS event carries asset_id)
    token_id         bigint NOT NULL REFERENCES tokens(id),
    event_time       timestamptz,
    capture_time     timestamptz NOT NULL,
    tick_mc          integer NOT NULL CHECK (tick_mc > 0),
    source           text NOT NULL,
    raw_ref          text,
    collector_run_id bigint REFERENCES collector_runs(id),
    connection_id    uuid,
    ingest_sequence  bigint,
    PRIMARY KEY (token_id, capture_time)
);

CREATE TABLE market_status (
    market_id        bigint NOT NULL REFERENCES markets(id),
    observed_at      timestamptz NOT NULL,
    status           text NOT NULL CHECK (status IN
                     ('active','closed','proposed','disputed',
                      'resolved','delisted_observed')),
    source           text NOT NULL,
    raw_ref          text,
    collector_run_id bigint REFERENCES collector_runs(id),
    PRIMARY KEY (market_id, observed_at, status)
);

CREATE TABLE resolutions (
    id               bigserial PRIMARY KEY,
    market_id        bigint NOT NULL REFERENCES markets(id),
    event_time       timestamptz,
    capture_time     timestamptz NOT NULL,
    winning_token_id bigint,
    winning_outcome  text,
    learned_via      text NOT NULL,
    status           text NOT NULL CHECK (status IN
                     ('proposed','disputed','final','corrected')),
    raw_ref          text,
    collector_run_id bigint REFERENCES collector_runs(id),
    FOREIGN KEY (market_id, winning_token_id) REFERENCES tokens (market_id, id)
);
CREATE INDEX idx_resolutions_market ON resolutions (market_id, capture_time);

-- ---------- observation layer (partitioned monthly) ---------------------------

CREATE TABLE tob_snapshots (
    token_id       bigint NOT NULL REFERENCES tokens(id),
    event_time     timestamptz,
    capture_time   timestamptz NOT NULL,
    best_bid_mc    integer CHECK (best_bid_mc IS NULL OR best_bid_mc BETWEEN 0 AND 1000),
    best_bid_size  numeric CHECK (best_bid_size IS NULL OR best_bid_size >= 0),
    best_ask_mc    integer CHECK (best_ask_mc IS NULL OR best_ask_mc BETWEEN 0 AND 1000),
    best_ask_size  numeric CHECK (best_ask_size IS NULL OR best_ask_size >= 0),
    source         text NOT NULL,
    watchlist_rule text,
    raw_ref        text,
    collector_run_id bigint REFERENCES collector_runs(id)
) PARTITION BY RANGE (capture_time);

CREATE TABLE book_snapshots (
    token_id        bigint NOT NULL REFERENCES tokens(id),
    event_time      timestamptz,
    capture_time    timestamptz NOT NULL,
    bids            jsonb NOT NULL,   -- [[price_mc,size],…] normalized ASC
    asks            jsonb NOT NULL,   -- [[price_mc,size],…] normalized ASC
    venue_hash      text,
    source          text NOT NULL,
    watchlist_rule  text,
    raw_ref         text,
    collector_run_id bigint REFERENCES collector_runs(id),
    connection_id   uuid,             -- WS-sourced snapshots only
    ingest_sequence bigint,
    book_generation bigint,           -- reconstruction segment id (WS)
    CHECK (source <> 'ws_book'
           OR (connection_id IS NOT NULL AND ingest_sequence IS NOT NULL
               AND book_generation IS NOT NULL))
) PARTITION BY RANGE (capture_time);

CREATE TABLE book_deltas (
    token_id         bigint NOT NULL REFERENCES tokens(id),
    event_time       timestamptz,
    capture_time     timestamptz NOT NULL,
    price_mc         integer NOT NULL CHECK (price_mc BETWEEN 0 AND 1000),
    size             numeric NOT NULL CHECK (size >= 0),  -- 0 = level removed
    side             char(1) NOT NULL CHECK (side IN ('B','S')),
    venue_hash       text,
    best_bid_mc      integer CHECK (best_bid_mc IS NULL OR best_bid_mc BETWEEN 0 AND 1000),
    best_ask_mc      integer CHECK (best_ask_mc IS NULL OR best_ask_mc BETWEEN 0 AND 1000),
    connection_id    uuid NOT NULL,
    ingest_sequence  bigint NOT NULL,
    change_index     smallint NOT NULL DEFAULT 0,
    book_generation  bigint NOT NULL,
    collector_run_id bigint REFERENCES collector_runs(id),
    source           text NOT NULL,
    watchlist_rule   text,
    raw_ref          text,
    UNIQUE (capture_time, connection_id, ingest_sequence, change_index)
) PARTITION BY RANGE (capture_time);

CREATE TABLE last_trade_events (
    token_id         bigint NOT NULL REFERENCES tokens(id),
    event_time       timestamptz,
    capture_time     timestamptz NOT NULL,
    price_mc         integer NOT NULL CHECK (price_mc BETWEEN 0 AND 1000),
    size             numeric CHECK (size IS NULL OR size >= 0),
    side             text,
    fee_rate_bps     integer,
    connection_id    uuid NOT NULL,
    ingest_sequence  bigint NOT NULL,
    collector_run_id bigint REFERENCES collector_runs(id),
    source           text NOT NULL,
    raw_ref          text,
    UNIQUE (capture_time, connection_id, ingest_sequence)
) PARTITION BY RANGE (capture_time);

CREATE TABLE trades (                  -- confirmed data-api tape ONLY (enforced)
    venue_id        smallint NOT NULL REFERENCES venues(id),
    token_id        bigint REFERENCES tokens(id),
    condition_id    text,
    event_time      timestamptz NOT NULL,
    capture_time    timestamptz NOT NULL,
    price_mc        integer NOT NULL CHECK (price_mc BETWEEN 0 AND 1000),
    size            numeric NOT NULL CHECK (size >= 0),
    side            text,
    outcome         text,
    wallet          text,
    tx_hash         text,
    venue_trade_key text NOT NULL,
    source          text NOT NULL DEFAULT 'data_api' CHECK (source = 'data_api'),
    raw_ref         text,
    collector_run_id bigint REFERENCES collector_runs(id),
    UNIQUE (venue_id, venue_trade_key, event_time)
) PARTITION BY RANGE (event_time);

CREATE TABLE price_history (
    token_id         bigint NOT NULL REFERENCES tokens(id),
    ts               timestamptz NOT NULL,
    price_mc         integer NOT NULL CHECK (price_mc BETWEEN 0 AND 1000),
    fidelity_min     integer NOT NULL,
    backfilled_at    timestamptz NOT NULL,
    source           text NOT NULL DEFAULT 'prices_history',
    raw_ref          text,
    collector_run_id bigint REFERENCES collector_runs(id),
    PRIMARY KEY (token_id, ts, fidelity_min)
);

-- ---------- cross-venue IP -----------------------------------------------------

CREATE TABLE event_matches (
    id         bigserial PRIMARY KEY,
    event_a    bigint NOT NULL REFERENCES events(id),
    event_b    bigint NOT NULL REFERENCES events(id),
    method     text NOT NULL,
    confidence numeric,
    notes      text,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (event_a < event_b)
);

-- ---------- ops -----------------------------------------------------------------

CREATE TABLE parsed_files (
    file_path  text PRIMARY KEY,
    parsed_at  timestamptz NOT NULL,
    row_counts jsonb,
    parser_sha text
);

CREATE TABLE dq_incidents (
    id          bigserial PRIMARY KEY,
    observed_at timestamptz NOT NULL,
    check_name  text NOT NULL,
    severity    text NOT NULL,
    details     jsonb
);
-- DQ rule (implemented Day F): alert if any partitioned table has <2 future
-- monthly partitions. No DEFAULT partition by design.

-- ---------- partitions: Jul–Nov 2026 --------------------------------------------

DO $$
DECLARE t text; m date;
BEGIN
  FOREACH t IN ARRAY ARRAY['tob_snapshots','book_snapshots','book_deltas',
                           'last_trade_events','trades']
  LOOP
    FOR i IN 0..4 LOOP
      m := date '2026-07-01' + (i || ' months')::interval;
      EXECUTE format(
        'CREATE TABLE %I_%s PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
        t, to_char(m,'YYYYMM'), t, m, m + interval '1 month');
    END LOOP;
  END LOOP;
END $$;

-- ---------- indexes --------------------------------------------------------------

CREATE INDEX idx_tob_token_time    ON tob_snapshots     (token_id, capture_time);
CREATE INDEX idx_books_token_time  ON book_snapshots    (token_id, capture_time);
CREATE INDEX idx_deltas_token_time ON book_deltas       (token_id, capture_time);
CREATE INDEX idx_deltas_replay     ON book_deltas       (token_id, connection_id,
                                                         ingest_sequence, change_index);
CREATE INDEX idx_lte_token_time    ON last_trade_events (token_id, capture_time);
CREATE INDEX idx_trades_cond_time  ON trades            (condition_id, event_time);
CREATE INDEX idx_trades_token_time ON trades            (token_id, event_time);
CREATE INDEX idx_trades_wallet     ON trades            (wallet);
CREATE INDEX idx_ph_token          ON price_history     (token_id, ts);

COMMIT;