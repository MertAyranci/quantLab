-- ============================================================================
-- Migration 008 — H2-v2 sharp fair-value research tape
--
-- Additive only.
--
-- Purpose:
--   Preserve the raw polling clock, exchange back/lay observations, and
--   derived >=2-family sharp consensus required by H2-v2.
--
-- IMPORTANT:
--   - This schema does NOT contain signals, thresholds, trades, or PnL.
--   - Stale/incomplete exchange observations are preserved rather than dropped.
--   - Eligibility is recorded explicitly.
--   - odds_games remains the canonical Odds API game identity table.
--
-- Apply:
--   docker exec -i quantlab-pg \
--     psql -U quantlab -d quantlab \
--     < db/migrations/008_h2_v2_sharp.sql
-- ============================================================================

BEGIN;


-- ---------------------------------------------------------------------------
-- 1. One row per H2-v2 Odds API request
-- ---------------------------------------------------------------------------

CREATE TABLE h2_v2_sharp_polls (
    id                  bigserial PRIMARY KEY,

    requested_at        timestamptz NOT NULL,
    received_at         timestamptz NOT NULL,

    request_latency_ms  numeric NOT NULL
                        CHECK (request_latency_ms >= 0),

    sport_key           text NOT NULL,
    requested_market    text NOT NULL,

    requested_bookmakers text[] NOT NULL,

    request_params      jsonb NOT NULL,

    http_status         integer NOT NULL,

    games_returned      integer
                        CHECK (games_returned IS NULL OR games_returned >= 0),

    credits_last        integer
                        CHECK (credits_last IS NULL OR credits_last >= 0),

    credits_used        integer
                        CHECK (credits_used IS NULL OR credits_used >= 0),

    credits_remaining   integer
                        CHECK (
                            credits_remaining IS NULL
                            OR credits_remaining >= 0
                        ),

    contract_sha256     text NOT NULL,

    collector_version   text NOT NULL,

    collector_run_id    bigint
                        REFERENCES collector_runs(id),

    raw_ref             text,

    CHECK (received_at >= requested_at)
);

CREATE INDEX idx_h2v2_polls_received
    ON h2_v2_sharp_polls (received_at);


-- ---------------------------------------------------------------------------
-- 2. Raw exchange outcome observations
--
-- One row per:
--   poll x Odds API game x exchange x outcome
--
-- Rows are retained even when:
--   - lay is missing
--   - pair is incomplete
--   - source timestamp is stale
--
-- family_midpoint is populated only when both back + lay are valid.
-- ---------------------------------------------------------------------------

CREATE TABLE h2_v2_exchange_quotes (
    id                  bigserial PRIMARY KEY,

    poll_id             bigint NOT NULL
                        REFERENCES h2_v2_sharp_polls(id),

    odds_game_id        bigint NOT NULL
                        REFERENCES odds_games(id),

    bookmaker           text NOT NULL,

    outcome_team        text NOT NULL,
    is_home             boolean NOT NULL,

    capture_time        timestamptz NOT NULL,

    source_update_time  timestamptz,

    source_age_seconds  numeric,

    back_decimal        numeric,
    lay_decimal         numeric,

    p_back              numeric,
    p_lay               numeric,

    family_midpoint     numeric,

    pair_complete       boolean NOT NULL,
    is_fresh            boolean NOT NULL,

    raw_back_outcome    jsonb,
    raw_lay_outcome     jsonb,

    collector_run_id    bigint
                        REFERENCES collector_runs(id),

    CHECK (
        back_decimal IS NULL
        OR back_decimal > 1.0
    ),

    CHECK (
        lay_decimal IS NULL
        OR lay_decimal > 1.0
    ),

    CHECK (
        p_back IS NULL
        OR p_back BETWEEN 0 AND 1
    ),

    CHECK (
        p_lay IS NULL
        OR p_lay BETWEEN 0 AND 1
    ),

    CHECK (
        family_midpoint IS NULL
        OR family_midpoint BETWEEN 0 AND 1
    ),

    CHECK (
        NOT pair_complete
        OR (
            back_decimal IS NOT NULL
            AND lay_decimal IS NOT NULL
            AND p_back IS NOT NULL
            AND p_lay IS NOT NULL
            AND family_midpoint IS NOT NULL
        )
    ),

    UNIQUE (
        poll_id,
        odds_game_id,
        bookmaker,
        outcome_team
    )
);

CREATE INDEX idx_h2v2_quotes_game_capture
    ON h2_v2_exchange_quotes (
        odds_game_id,
        capture_time
    );

CREATE INDEX idx_h2v2_quotes_book_source
    ON h2_v2_exchange_quotes (
        bookmaker,
        source_update_time
    );

CREATE INDEX idx_h2v2_quotes_poll
    ON h2_v2_exchange_quotes (poll_id);


-- ---------------------------------------------------------------------------
-- 3. Derived sharp consensus
--
-- One row per:
--   poll x Odds API game x outcome
--
-- Consensus:
--   median family_midpoint across FRESH, complete families.
--
-- H2-v2 M1 contract:
--   eligible iff >=2 fresh families.
--
-- Ineligible rows are retained with consensus_prob = NULL.
-- ---------------------------------------------------------------------------

CREATE TABLE h2_v2_sharp_consensus (
    id                      bigserial PRIMARY KEY,

    poll_id                 bigint NOT NULL
                            REFERENCES h2_v2_sharp_polls(id),

    odds_game_id            bigint NOT NULL
                            REFERENCES odds_games(id),

    outcome_team            text NOT NULL,
    is_home                 boolean NOT NULL,

    capture_time            timestamptz NOT NULL,

    paired_family_count     smallint NOT NULL
                            CHECK (
                                paired_family_count
                                BETWEEN 0 AND 3
                            ),

    fresh_family_count      smallint NOT NULL
                            CHECK (
                                fresh_family_count
                                BETWEEN 0 AND 3
                            ),

    families_used           text[] NOT NULL,

    family_midpoints        jsonb NOT NULL,

    min_family_age_seconds  numeric,
    max_family_age_seconds  numeric,

    consensus_prob          numeric,

    eligible                boolean NOT NULL,

    collector_run_id        bigint
                            REFERENCES collector_runs(id),

    CHECK (
        fresh_family_count
        <= paired_family_count
    ),

    CHECK (
        consensus_prob IS NULL
        OR consensus_prob BETWEEN 0 AND 1
    ),

    CHECK (
        (
            eligible
            AND fresh_family_count >= 2
            AND consensus_prob IS NOT NULL
        )
        OR
        (
            NOT eligible
            AND consensus_prob IS NULL
        )
    ),

    UNIQUE (
        poll_id,
        odds_game_id,
        outcome_team
    )
);

CREATE INDEX idx_h2v2_consensus_game_capture
    ON h2_v2_sharp_consensus (
        odds_game_id,
        capture_time
    );

CREATE INDEX idx_h2v2_consensus_eligible
    ON h2_v2_sharp_consensus (
        capture_time
    )
    WHERE eligible;


COMMIT;
