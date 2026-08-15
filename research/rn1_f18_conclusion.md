# RN1-F18 — Discovery conclusion and prospective freeze

## Status

**PROMISING CANDIDATE — NOT YET CONFIRMED OR DEPLOYABLE**

Retrospective discovery stops after RN1-F17.

## What survived

The frozen signal is:

- 30-second non-self aggressive-flow window;
- flow measured relative to proposed maker inventory direction;
- notional-weighted imbalance;
- threshold <= -0.75;
- signal observed with a 5-second decision lead;
- primary adverse-selection horizon = +15 seconds.

RN1-F17 compared flagged maker fills with unflagged maker fills matched on
market, inventory direction, game phase, price, size, and nearby time.

At the prespecified +15-second horizon:

- usable matched flagged events: 29;
- markets: 13;
- flagged mean markout: -0.6356c/share;
- control mean markout: -0.3991c/share;
- flagged minus control: -0.2365c/share;
- market-equal difference: -0.8069c/share;
- market sign count: 4 positive, 9 negative;
- leave-one-market-out estimates negative in 12 of 13 exclusions.

The 60-second de-clustered sensitivity produced:

- usable observations: 22;
- markets: 12;
- flagged minus control: -0.7639c/share;
- market-equal difference: -0.7352c/share;
- market sign count: 3 positive, 1 flat, 8 negative.

## What did not survive

The incremental effect was not stable at +5, +30, or +60 seconds.

Therefore no claim is made outside the frozen +15-second primary horizon.

Settlement-based cancellation accounting is descriptive only and is not the
primary evidence for the mechanism.

The existing RN1 maker book remains negative after removing flagged fills, so
the discovery does not demonstrate a profitable standalone maker strategy.

## Main limitation

Only 169 of 917 flagged discovery events could be matched under the frozen
control specification, and only 29 matched pairs had usable +15-second
markouts.

This is insufficient for deployment or a profitability claim.

## Frozen candidate

Strong adverse Polymarket taker flow may be useful as a **quote-protection
overlay**:

    adverse flow against proposed inventory
                    |
                    v
        withdraw / cancel passive quote
                    |
                    v
       avoid short-horizon adverse selection

The mechanism must now be tested prospectively on unseen full-game windows.

No further retrospective threshold, horizon, phase, price, or size
optimization is permitted before the confirmatory readout.

## Prospective sequence

1. Capture 100 unseen full MLB moneyline game windows.
2. Reproduce the frozen adverse-selection test.
3. Require at least 50 matched flagged events across at least 20 markets.
4. Primary metric: market-equal flagged-minus-control +15s markout.
5. Require the primary metric and the 60-second de-clustered metric to be
   negative.
6. Strong confirmation additionally requires the market-bootstrap 95% CI
   upper bound below zero.
7. Only after signal confirmation build an authoritative CLOB queue/cancel
   simulation for economic testing.

No paid external odds feed is required for this confirmation.
