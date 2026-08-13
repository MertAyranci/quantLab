-- H2-v2 mapping must be one-to-one.
-- One Odds API game -> at most one Polymarket market.
-- One Polymarket market -> at most one Odds API game.

BEGIN;

CREATE UNIQUE INDEX uq_h2v2_match_odds_game
    ON h2_v2_market_matches (odds_game_id);

CREATE UNIQUE INDEX uq_h2v2_match_pm_market
    ON h2_v2_market_matches (polymarket_market_id);

COMMIT;
