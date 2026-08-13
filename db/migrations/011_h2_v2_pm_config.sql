-- ============================================================================
-- Migration 011 — H2-v2 Polymarket executable-market configuration
--
-- Captures the live CLOB market parameters required by the H2-v2 M1
-- real-time data contract.
--
-- Source:
--   GET /clob-markets/{condition_id}
--
-- No signals, thresholds, PnL, or trading.
-- ============================================================================

BEGIN;

CREATE TABLE h2_v2_pm_market_config (
    id                      bigserial PRIMARY KEY,

    polymarket_market_id    bigint NOT NULL
                            REFERENCES markets(id),

    observed_at             timestamptz NOT NULL,

    condition_id            text NOT NULL,

    game_start_time         timestamptz,

    minimum_order_size      numeric NOT NULL
                            CHECK (minimum_order_size > 0),

    minimum_tick_size       numeric NOT NULL
                            CHECK (
                                minimum_tick_size > 0
                                AND minimum_tick_size <= 1
                            ),

    maker_base_fee_bps      integer,
    taker_base_fee_bps      integer,

    fee_rate                numeric,
    fee_exponent            numeric,
    taker_only              boolean,

    minimum_order_age_s     integer,

    token_count             smallint NOT NULL
                            CHECK (token_count = 2),

    contract_sha256         text NOT NULL,

    raw_response            jsonb NOT NULL,
    raw_sha256              text NOT NULL,

    collector_run_id        bigint
                            REFERENCES collector_runs(id),

    UNIQUE (
        polymarket_market_id,
        observed_at
    )
);

CREATE INDEX idx_h2v2_pm_config_market_time
    ON h2_v2_pm_market_config (
        polymarket_market_id,
        observed_at
    );

COMMIT;
