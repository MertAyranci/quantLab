-- ============================================================================
-- Migration 004 — odds_snapshots (H2 bookmaker/exchange lead-lag)
-- Additive only. Stores timestamped implied probabilities from The Odds API,
-- one row per (game, bookmaker, outcome, poll), for alignment against
-- Polymarket price series.
-- Apply:
--   docker exec -i quantlab-pg psql -U quantlab -d quantlab < db/migrations/004_odds.sql
-- ============================================================================

BEGIN;

-- one row per game we track (from The Odds API)
CREATE TABLE odds_games (
    id              bigserial PRIMARY KEY,
    sport_key       text NOT NULL,              -- 'baseball_mlb'
    oddsapi_id      text NOT NULL,              -- The Odds API game id
    commence_time   timestamptz NOT NULL,
    home_team       text NOT NULL,
    away_team       text NOT NULL,
    -- link to Polymarket once matched (nullable until event-matching runs)
    polymarket_market_id  bigint REFERENCES markets(id),
    match_confidence      text,                 -- 'exact'|'fuzzy'|'manual'|null
    created_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (sport_key, oddsapi_id)
);

CREATE INDEX idx_odds_games_commence ON odds_games (commence_time);
CREATE INDEX idx_odds_games_pm ON odds_games (polymarket_market_id)
    WHERE polymarket_market_id IS NOT NULL;

-- append-only odds observations. implied_prob is OVERROUND-ADJUSTED (normalized
-- across the two outcomes of the game for that book at that poll). raw_prob is
-- the un-normalized 1/decimal, kept for audit.
CREATE TABLE odds_snapshots (
    id              bigserial PRIMARY KEY,
    game_id         bigint NOT NULL REFERENCES odds_games(id),
    bookmaker       text NOT NULL,              -- 'fanduel','betfair_ex_uk','smarkets',...
    is_exchange     boolean NOT NULL DEFAULT false,
    outcome_team    text NOT NULL,              -- which team this prob is for
    is_home         boolean NOT NULL,
    decimal_odds    numeric NOT NULL,
    raw_prob        numeric NOT NULL,           -- 1/decimal_odds (un-normalized)
    implied_prob    numeric NOT NULL,           -- overround-adjusted
    book_last_update timestamptz,               -- when the BOOK changed (lead-lag key)
    capture_time    timestamptz NOT NULL DEFAULT now(),  -- when WE polled
    collector_run_id bigint,
    CHECK (decimal_odds > 1.0),
    CHECK (implied_prob >= 0 AND implied_prob <= 1)
);

-- monthly-ish access patterns; index for the lead-lag joins
CREATE INDEX idx_odds_snap_game_time ON odds_snapshots (game_id, capture_time);
CREATE INDEX idx_odds_snap_book ON odds_snapshots (bookmaker, capture_time);
CREATE INDEX idx_odds_snap_bookupdate ON odds_snapshots (game_id, bookmaker, book_last_update);

COMMIT;