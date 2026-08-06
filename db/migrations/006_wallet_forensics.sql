-- ============================================================================
-- Migration 006 — wallet_forensics (resumable receipt table for wallet trade
-- collection). Additive only.
-- Apply:
--   docker exec -i quantlab-pg psql -U quantlab -d quantlab < db/migrations/006_wallet_forensics.sql
--
-- Trades themselves go into the existing `trades` table (which already has
-- wallet, condition_id, tx_hash, venue_trade_key + UNIQUE(venue_id,
-- venue_trade_key, event_time)). This receipt table tracks, per (wallet,
-- condition_id), whether that market's fills have been fully collected, so the
-- collector can stop and resume without re-fetching completed markets.
-- ============================================================================

BEGIN;

CREATE TABLE wallet_collection_receipts (
    wallet           text        NOT NULL,
    condition_id     text        NOT NULL,
    status           text        NOT NULL DEFAULT 'pending',  -- pending|complete|partial|failed
    trade_count      integer     NOT NULL DEFAULT 0,
    earliest_trade   timestamptz,
    latest_trade     timestamptz,
    collected_at     timestamptz,
    attempts         integer     NOT NULL DEFAULT 0,
    notes            text,
    PRIMARY KEY (wallet, condition_id)
);

CREATE INDEX idx_wallet_receipts_status ON wallet_collection_receipts (wallet, status);

-- Ledger of asset/token IDs the collector could NOT resolve to an internal
-- token_id, so unresolved fills are visible rather than silently dropped.
CREATE TABLE wallet_unresolved_assets (
    wallet        text        NOT NULL,
    condition_id  text        NOT NULL,
    asset_id      text        NOT NULL,
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    occurrences   integer     NOT NULL DEFAULT 1,
    PRIMARY KEY (wallet, asset_id)
);

COMMIT;