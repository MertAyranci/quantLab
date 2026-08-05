-- ============================================================================
-- Migration 005 — crypto_watch (last-mile capture ledger for 5m crypto markets)
-- Additive only.
-- Apply:
--   docker exec -i quantlab-pg psql -U quantlab -d quantlab < db/migrations/005_crypto_watch.sql
-- ============================================================================

BEGIN;

CREATE TABLE crypto_watch (
    market_id     bigint PRIMARY KEY REFERENCES markets(id),
    slug          text NOT NULL,
    resolve_time  timestamptz NOT NULL,
    admitted_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_crypto_watch_resolve ON crypto_watch (resolve_time);

COMMIT;