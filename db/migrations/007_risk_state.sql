-- ============================================================================
-- Migration 007 — risk_state (persistent latched risk state, Phase 2A)
-- Additive only.
-- Apply:
--   docker exec -i quantlab-pg psql -U quantlab -d quantlab < db/migrations/007_risk_state.sql
--
-- One latched row per engine `key` (e.g. 'live', 'paper', or a test key). Stores
-- the daily baseline + high-water mark + latched halt/kill flags + explicit
-- realized P&L and fee drag, so the safety latch survives process/machine
-- restart. Positions are NOT stored here (reconciled from venue/ledger).
-- ============================================================================

BEGIN;

CREATE TABLE risk_state (
    key               text        PRIMARY KEY,          -- 'live' | 'paper' | test key
    trading_day       date        NOT NULL,
    live_capital      numeric     NOT NULL,
    day_start_equity  numeric     NOT NULL,             -- LATCHED daily baseline
    peak_equity       numeric     NOT NULL,             -- high-water mark (ratchets)
    realized_pnl      numeric     NOT NULL DEFAULT 0,
    fee_drag          numeric     NOT NULL DEFAULT 0,
    halted_today      boolean     NOT NULL DEFAULT false,
    stopped           boolean     NOT NULL DEFAULT false,
    kill              boolean     NOT NULL DEFAULT false,
    updated_at        timestamptz NOT NULL DEFAULT now()
);

COMMIT;