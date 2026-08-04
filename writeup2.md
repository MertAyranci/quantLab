# Two Prediction-Market Inefficiency Hypotheses, Tested and Rejected

*Why simple price-level rules don't beat Polymarket — and what that implies for
where an edge might actually live.*

---

## The setup

Prediction markets like Polymarket look, to a quant, like an invitation: real
probabilities, real prices, a resolution that tells you the truth at the end.
Surely the crowd is wrong somewhere. This is a write-up of two specific "the
crowd is wrong about probabilities" hypotheses, tested against a proprietary
tick-level dataset, and why both were rejected — which turns out to be the more
useful result.

The dataset: continuous order-book-level collection across ~1,500 active
Polymarket markets, plus a six-year historical backfill of ~1.46M resolved
markets and ~25M price points, plus forward "Grade-A" collection that watches
markets through their resolution at second-to-minute fidelity. All self-collected
on a small cloud server, with data-quality auditing and honest cost modeling.

## Hypothesis 3: the favorite–longshot bias

Decades of racetrack and sports-betting literature document that longshots are
overpriced and favorites underpriced — people overpay for lottery tickets. If
prediction markets inherit this, systematically selling longshots should pay.

**Naively, the bias is dramatic.** Bucket 1.46M resolved markets by price and
compare implied vs realized win rates, and low-priced contracts appear
substantially overpriced. But the effect dissolves under controls:

- **Activity filtering induces look-ahead.** Restricting to "genuinely traded"
  markets (price moved a lot) preferentially selects markets that moved *toward*
  their outcome — inflating apparent edge by ~8pp. A methodological trap most
  naive backtests contain invisibly.
- **Event structure dominates.** In events with many mutually-exclusive
  outcomes (e.g. 20 election candidates), the average contract mechanically
  loses regardless of price — and the venue's own `neg_risk` flag does not
  reliably identify these. The correct control is sibling count.
- **Composition and staleness** add further artifacts.

On the clean population (independent binary markets, controlled for the above),
Polymarket is **calibrated to within 1–3 percentage points across the entire
probability range** — with sub-0.5pp deviations on the largest samples.
Crypto-direction ("Up") markets calibrate to 0.1pp on 45,000+ observations,
which doubles as a validation that the measurement pipeline is correct.

**Verdict: killed.** No tradeable longshot edge survives the controls, let alone
fees and spread.

## Hypothesis 4: resolution-drift / last-mile underpricing

If the crowd isn't wrong on average, maybe it's wrong at the *end* — in the
final hours, are effectively-decided contracts (97¢, truly ~99.5%) underpriced?

This can't be tested on daily-fidelity history (the last mile is invisible), so
it required forward collection: a harvester steering the collector to watch
markets *through* resolution, plus an execution backtester that replays the
recorded order books through the same fill simulator the live trading engine
uses — so "would this have filled?" is answered honestly, walking real depth,
paying real spread.

The result splits by market type:

- **Fast-resolving markets** (sports, short-fuse events) *never reached
  near-certainty* before resolving — at 1h, 30min, and 15min horizons, they sat
  below 95¢, then resolved suddenly. The "last mile at near-certainty" the
  hypothesis assumes simply doesn't exist for them.
- **Parked near-certainties** (candidates at 99¢ for days) *do* sit at
  near-certainty — but they're barely traded, so their books are stale and thin.
  You can see the price; you can't execute size against it. And where you can,
  the spread paid on entry rivals the 1–2¢ theoretical edge.

**Verdict: conditional negative.** Naive H4 is not tradeable on the observed
sample — either the window doesn't exist or it has no liquidity. (Sample is
still small and collection continues; the rules were deliberately not tuned to
manufacture trades.)

## What both results have in common

Two independent "the crowd is wrong about probabilities" hypotheses, two
rejections, one conclusion: **Polymarket is efficient at the level a simple
retail price-rule can measure and exploit.** The prices are, to a first
approximation, right — and where they're theoretically slightly wrong, the cost
of trading eats the difference.

This is not a disappointing result. It's a *map*. If static mispricing is a dead
end, the edges that remain are the ones that survive efficiency:

- **Information speed** — does one venue (a sharp bookmaker, an exchange) move
  before Polymarket on the same event? That's a signal, not a claim that anyone
  is wrong about probability.
- **Cross-venue relationships** — the same event priced on two venues that
  converge.
- **Microstructure** — order-flow and adverse selection, not price level.

The next investigation targets the first of these. The infrastructure built for
H3 and H4 — the collectors, the honest backtester, the risk-controlled execution
engine — points at it directly.

## The honest footer

The reliable outputs of this project were never the P&L. They are the dataset,
the infrastructure, the tested execution engine, and these documented negative
results. A rejected hypothesis, rigorously demonstrated, is worth more than an
equity curve you can't trust — because it tells you the truth about where to
look next.

*Methods, data-quality notes, and the full control tables are in the project
repository.*