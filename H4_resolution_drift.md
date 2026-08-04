# H4 — Resolution Drift / Last-Mile Underpricing

**Status: CONDITIONAL NEGATIVE** (not tradeable on the observed sample; not a universal rejection)
**Tested:** 2026-08-03/04 · **Method:** forward Grade-A collection + H4-Cal (calibration) + H4-Exec (execution replay) + live paper soak
**Analyst:** Mert Ayrancı · **Repo:** quantLab

---

## Thesis

In the final hours before resolution, contracts that are *effectively decided*
should be underpriced relative to their true ~100% probability — e.g. a 97¢
contract that resolves YES ~99.5% of the time. Systematically buying such
near-certainties in their last mile would harvest that gap.

**Testable prediction:** at a fixed horizon before resolution, near-certainty
contracts (≥95¢) win MORE often than their price implies, by enough to beat
spread, fees, and the tail risk of rare blowups.

## Why this needed forward data

H4-Cal on the historical archive (daily fidelity) was inconclusive: at a 24h
horizon on 572 clean markets, calibration gaps were within their confidence
intervals (mean reference PnL +0.0025 ≈ 0). The last-mile question (final
30min–few hours) is invisible to daily-fidelity history, so it required
**forward collection**: a Grade-A harvester steering the WS collector to watch
markets *through* their resolution at second-to-minute fidelity.

## Method

Two backtesters, both on forward-collected Grade-A markets (watched through
resolution, unambiguous winner, last-mile book data present):

- **H4-Cal** — calibration on reference prices (strict as-of, event-clustered
  bootstrap CIs). Theoretical, not executable.
- **H4-Exec** — execution replay through the SAME fill simulator the live engine
  uses: read the recorded book as-of (resolution − horizon), simulate a
  marketable-limit near-certainty buy, walk the real book, hold to resolution,
  settle $1/$0. Reports executable vs theoretical PnL. Three latency scenarios.
- **Live paper soak** — the H4-shaped rule (buy ≥95¢, $20 notional) running on
  live collector data through the full risk→fill→track→ledger engine.

Grade-A sample at time of verdict: **9 resolved markets** with last-mile book
data (small — see limitations).

## Findings

### 1. Imminent-cohort markets never reached near-certainty before resolving
H4-Exec at horizons of 1h, 30min, and 15min: **all 9 markets were below 95¢ at
every horizon** (`no-entry`). These markets (mostly sports/short-fuse events)
transition from genuinely uncertain prices *directly* to resolved — there is no
sustained near-certain last mile to trade. The entry setup H4 assumes did not
exist in this cohort.

### 2. Near-certainty-cohort markets were stale / illiquid
The markets that DID sit at ≥95¢ (candidates/events parked near 99¢ for days)
were, in the live paper soak, frozen and thinly quoted: books aged into the
hours because a decided market generates no trades. A reconcile-heartbeat fix
(force a snapshot every 300s) restored freshness, after which the soak *could*
enter — but fills were small and partial (thin depth), and entry paid the spread
immediately (marked conservatively at the bid, positions showed ~-$3 on ~$250
deployed on entry alone).

### 3. The two cohorts fail H4 for opposite reasons
- Imminent: near-certainty window does not exist (resolve suddenly).
- Parked near-certainty: window exists but no liquidity to execute against.

### 4. Executable ≠ theoretical
Where entries existed (live soak), the spread paid on entry was the same order
of magnitude as H4's theoretical edge (1–2¢). The cost of trading plausibly
consumes the edge — consistent with H3's conclusion that the venue is efficient
at the retail-measurable level.

## Verdict

**CONDITIONAL NEGATIVE.** Naive H4 ("buy any contract ≥95¢ in its last mile")
is not tradeable on the observed Grade-A sample: either the near-certainty
window does not exist (imminent markets) or it exists without executable
liquidity (parked near-certainties), and where execution is possible the spread
rivals the theoretical edge. This is NOT a universal rejection — the sample is
small (9 markets) and the harvester continues collecting out-of-sample data. The
rules were deliberately NOT tuned to manufacture trades.

Consistent with H3: Polymarket is efficient at the level a simple retail
price-rule can exploit. A real edge must come from information speed (H2/H6),
cross-venue relationships (H1), or microstructure (H5) — not from the crowd
being wrong about probabilities.

## Limitations

- **Sample size: 9 Grade-A markets.** Validates the pipeline; the verdict firms
  as more watched markets resolve. Re-run H4-Exec as the cohort grows.
- Grade-A markets skew toward the imminent cohort (fast-resolving sports/events)
  because parked near-certainties resolve slowly and were partly evicted for
  slot efficiency. A future run should over-sample genuinely parked markets.
- Live paper P&L uses conservative (bid) marks and entry at the recorded ask —
  an honest but pessimistic basis; a maker/passive variant was not tested.
- `closed_time` for the first cohort lagged true resolution (fixed mid-study);
  early markets have imperfect last-mile alignment.

## What was built (reusable regardless of verdict)

- Grade-A harvester (resolution-proximity market selection, survivorship guard).
- Reconcile-heartbeat fix (frozen markets record stable prices — also improves
  data completeness venue-wide).
- Shared fill simulator (full-depth walk / BBO haircut / three latency
  scenarios) used by BOTH the live engine and H4-Exec.
- Complete paper-trading engine: risk module, position/P&L tracker with Gamma
  resolution sweep, order manager, paper soak — 48 passing tests.

## Follow-ups

1. Keep harvester + paper soak running; re-run H4-Exec at 50–100 Grade-A markets.
2. Over-sample genuinely parked near-certainties (separate cohort) to test the
   liquidity-limited branch directly.
3. Pivot main research to H2 (bookmaker/exchange lead-lag) — an information-speed
   edge that does not require the crowd to misprice probabilities.