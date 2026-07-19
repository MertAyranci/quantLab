-- ============================================================================
-- Migration 002 — source-consistency CHECKs (deferred from review round 2.5)
-- STATUS: NOT YET APPLIED.
-- Apply AFTER Day D fixes the collector's actual source strings — the CHECK
-- values below must match what the WS collector really emits; verify before
-- applying, then run:
--   docker exec -i quantlab-pg psql -U quantlab -d quantlab \
--     < db/migrations/002_source_checks.sql
-- ============================================================================

BEGIN;

-- 1. WS-sourced tick-size rows must carry sequencing
ALTER TABLE tick_sizes
  ADD CONSTRAINT chk_tick_ws_sequenced
  CHECK (
    source <> 'ws_tick_size_change'
    OR (connection_id IS NOT NULL AND ingest_sequence IS NOT NULL)
  );

-- 2. last_trade_events accepts only its intended source
ALTER TABLE last_trade_events
  ADD CONSTRAINT chk_lte_source
  CHECK (source = 'ws_last_trade_price');

COMMIT;