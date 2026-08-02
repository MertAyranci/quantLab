# H3 — Favorite–Longshot Bias on Polymarket

**Status: KILLED** (no tradeable edge survives controls)
**Tested:** 2026-07-24 · **Data:** 1,462,357 resolved markets, 25.4M daily price points
**Analyst:** Mert Ayrancı · **Repo:** quantLab

---

## Thesis

Decades of betting-market literature (Thaler & Ziemba's survey being the classic
entry point) document that low-probability outcomes are systematically
overpriced and high-probability outcomes underpriced — punters overpay for
lottery tickets. If prediction markets inherit this bias, systematically selling
longshots (buying NO on low-priced contracts) should earn a risk premium.

**Testable prediction:** bucketing resolved markets by pre-resolution price,
realized win frequency should fall *below* implied probability in low buckets
and *above* it in high buckets.

## Method

For each resolved market: take the last daily price of the YES token at least
24h before resolution, bucket into 20 price bins, compute realized frequency of
that token winning. Data from Polymarket's full closed-market archive
(`/markets/keyset?closed=true`) joined to `/prices-history` series harvested to
market birth. Resolution determined from `outcomePrices` with a defensive rule:
exactly one outcome equal to "1", else market recorded as closed-without-winner
and excluded (never fabricated).

## Results — the naive curve, and what dissolved it

**Run 1 (naive, n=595k):** textbook-looking bias. 27.2¢ contracts realized
22.9%; 92.5¢ contracts realized 94.3%. Apparent edge ~5.9% per trade on
mid-range longshots.

**Artifact 1 — activity-based selection (look-ahead by the back door).**
Filtering to "genuinely traded" markets via `distinct_prices >= 20` *inflated*
apparent underpricing from ~+1pp to ~+9pp across mid/high buckets. Filtering on
price *movement* preferentially selects markets that moved toward their eventual
outcome. Removing the filter collapsed the effect (bucket 12: +12.9pp → +1.1pp).
*Methodological lesson: activity filters leak the future into the sample.*

**Artifact 2 — event structure (the dominant one).** Gap scales with sibling
count, not price. In the 44–56¢ band, grouping by markets-per-event:

| Event size | n | Implied | Realized | Gap |
|---|---|---|---|---|
| 1 market | 379 | 50.1% | 56.7% | +6.6pp |
| 2–3 | 15,321 | 49.3% | 42.9% | −6.4pp |
| 4–10 | 9,028 | 50.0% | 47.2% | −2.9pp |
| 11+ | 35,187 | 49.3% | 25.9% | **−23.4pp** |

Mechanical, not behavioural: when N sibling markets share one event and exactly
one resolves YES, average siblings must lose regardless of price. Critically,
**Polymarket's `neg_risk` flag does not identify these** (562,841 events flagged
FALSE, yet exhibiting the pattern) — sibling count is the correct control.

**Artifact 3 — price staleness.** Daily-fidelity history means "last price ≥24h
before resolution" can be 48h+ stale. Long-lived markets (>30d) showed wild
deviations (48.5¢ → 16.7% realized) where short-lived ones sat near fair.
Controlled by requiring the observation within a 1–7 day window.

**Artifact 4 — composition.** Only ~35% of the archive is Yes/No; 471k markets
are "Up" (crypto/equity direction), 243k "Over" (sports totals). Pooling them
mixes populations with different dynamics.

## Corrected result (the defensible one)

Population: non-neg-risk, ≤3 sibling markets, price observed 1–7 days pre-resolution.

| Implied | n | Realized | Gap |
|---|---|---|---|
| 1.1% | 4,377 | 1.0% | −0.1pp |
| 12.3% | 679 | 10.6% | −1.7pp |
| 27.2% | 949 | 29.0% | +1.7pp |
| 42.4% | 2,089 | 42.8% | +0.4pp |
| **50.2%** | **56,167** | **50.1%** | **−0.1pp** |
| 72.2% | 1,047 | 72.2% | 0.0pp |
| 87.3% | 554 | 89.4% | +2.1pp |
| 98.3% | 1,085 | 98.9% | +0.6pp |

All deviations within ±3.6pp; the two largest samples within 0.2pp. Given
bucket sample sizes of 500–2,000 (noise band roughly ±2–4pp), **no economically
meaningful miscalibration survives.** Independent validation: "Up" markets,
45,583 observations, implied 50.0% → realized 50.1%.

## Verdict

**KILL.** The favorite–longshot bias is not present in Polymarket's independent
binary markets at any magnitude that could survive fees (per-market curved) and
spread (typically 1–2 ticks). What appears as a dramatic bias in naive analysis
is an artifact of event structure, selection filtering, and price staleness.

Secondary conclusion, and arguably the more useful one: **this venue is
efficient at the level a retail participant can measure it.** Any edge must come
from speed, cross-venue relationships, or microstructure — not from the crowd
being wrong about probabilities.

## Limitations

- Daily fidelity only; fine (10-min) history exists just ~30 days back.
  A resolution-adjacent study (H4) needs the fine window harvested promptly.
- Prices are historical prints, not executable quotes; no spread/depth data
  joined at this stage.
- Sibling-count control is a proxy for logical exclusivity; some multi-market
  events are genuinely independent (e.g. unrelated questions grouped by topic).
- Single horizon tested (24h–7d). Robustness across horizons and across years
  still to run.

## Follow-ups

1. Horizon stability (1h / 24h / 7d) and per-bucket confidence intervals.
2. Year-by-year cut — is calibration improving as the venue matures?
3. Stratified curves by market type (Yes/No vs Up vs Over).
4. Re-run restricted to markets with observed depth once the WS/TOB dataset
   matures — calibration among *liquid* markets is the tradeable question.