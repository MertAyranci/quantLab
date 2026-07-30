-- ============================================================================
-- Migration 003 — H4 admission ledger (Grade-A harvester)
-- Additive only: creates ONE new table, touches nothing existing.
-- Safe to apply on the live database.
-- Apply:
--   docker exec -i quantlab-pg psql -U quantlab -d quantlab \
--     < db/migrations/003_h4_watch.sql
--
-- Purpose: track which markets the harvester has admitted to WS collection for
-- H4 last-mile study, WHY, and their survivorship trajectory. The ledger is the
-- source of truth for the watchlist AND the guard against survivorship bias:
-- a near-certainty market, once admitted, is never dropped because its price
-- reversed — the reversal is the signal H4 measures.
-- ============================================================================

BEGIN;

CREATE TABLE h4_watch (
    id              bigserial PRIMARY KEY,
    market_id       bigint NOT NULL REFERENCES markets(id),

    -- admission
    admit_reason    text NOT NULL CHECK (admit_reason IN
                    ('imminent','near_certainty','control','special')),
    admit_time      timestamptz NOT NULL DEFAULT now(),
    admit_source    text NOT NULL,           -- 'gamma_sweep' | 'manual' | ...

    -- near-certainty crossing details (null for other reasons)
    crossing_price_mc   integer,             -- price at first >=950 / <=50 crossing
    crossing_time       timestamptz,
    crossing_side       text,                -- 'high' (>=0.95) | 'low' (<=0.05)
    crossing_before_48h boolean,             -- crossed before the scheduled 48h window?

    -- imminent-resolution details
    scheduled_close timestamptz,             -- end_date at admission (may be wrong)

    -- lifecycle
    status          text NOT NULL DEFAULT 'watching' CHECK (status IN
                    ('watching','closed','evicted_misclassified','evicted_maxdur')),
    closed_time     timestamptz,             -- when trading actually stopped
    final_snapshot_taken boolean NOT NULL DEFAULT false,
    evicted_time    timestamptz,
    evicted_reason  text,

    -- survivorship trajectory (updated by harvester over the market's life)
    max_price_mc    integer,                 -- highest YES price seen while watched
    min_price_mc    integer,                 -- lowest  YES price seen while watched
    largest_reversal_pp numeric,             -- biggest drop from a local peak
    last_observed_price_mc integer,
    last_observed_time  timestamptz,

    -- diversification bookkeeping
    event_id        bigint REFERENCES events(id),
    category        text,

    notes           text,

    -- one active admission per market (re-admission after close = new row via
    -- partial-unique below)
    UNIQUE (market_id, admit_time)
);

-- at most ONE 'watching' row per market (enforce the market-as-unit rule)
CREATE UNIQUE INDEX idx_h4_watch_active
    ON h4_watch (market_id) WHERE status = 'watching';

CREATE INDEX idx_h4_watch_status ON h4_watch (status);
CREATE INDEX idx_h4_watch_reason ON h4_watch (admit_reason, status);
CREATE INDEX idx_h4_watch_event  ON h4_watch (event_id) WHERE status = 'watching';

COMMIT;