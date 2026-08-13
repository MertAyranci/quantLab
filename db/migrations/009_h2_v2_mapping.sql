-- ============================================================================
-- Migration 009 — H2-v2 Odds API <-> Polymarket market mapping provenance
--
-- Mapping identity is based on:
--   exact normalized team set
--   + live Gamma sportsMarketType=moneyline
--   + exactly two CLOB tokens
--   + active/open/accepting-orders state
--   + unique gameStartTime match within +/- 10 minutes
--
-- Slug date is NOT used for identity.
-- ============================================================================

BEGIN;

CREATE TABLE h2_v2_market_matches (
    id                      bigserial PRIMARY KEY,

    odds_game_id            bigint NOT NULL
                            REFERENCES odds_games(id),

    polymarket_market_id    bigint NOT NULL
                            REFERENCES markets(id),

    matched_at              timestamptz NOT NULL,

    odds_commence_time      timestamptz NOT NULL,
    pm_game_start_time      timestamptz NOT NULL,

    start_delta_seconds     numeric NOT NULL,

    exact_team_candidates   integer NOT NULL
                            CHECK (exact_team_candidates >= 1),

    in_window_candidates    integer NOT NULL
                            CHECK (in_window_candidates >= 1),

    mapping_rule            text NOT NULL,

    gamma_market_id         text NOT NULL,
    gamma_slug              text,
    gamma_question          text,

    token_count             smallint NOT NULL
                            CHECK (token_count = 2),

    active                  boolean,
    closed                  boolean,
    accepting_orders        boolean,

    contract_sha256         text NOT NULL,

    raw_ref                 text,

    collector_run_id        bigint
                            REFERENCES collector_runs(id),

    UNIQUE (odds_game_id, polymarket_market_id)
);

CREATE INDEX idx_h2v2_matches_odds
    ON h2_v2_market_matches (odds_game_id);

CREATE INDEX idx_h2v2_matches_pm
    ON h2_v2_market_matches (polymarket_market_id);

COMMIT;
