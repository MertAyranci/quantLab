Every market with an open or recently-closed position must have its resolution confirmed directly from Gamma (GET /markets by condition_id, read outcomePrices + umaResolutionStatus), on a daily sweep — never rely solely on the WS market_resolved feed, which only covers watchlisted markets. P&L reconciliation depends on this; a position that resolves unnoticed is an unreconciled position


# Execution Engine — Requirements & Design

**Status:** pre-build spec. Paper-mode first, built from London, hardened before Istanbul.
**Governing principle:** the engine must be able to *refuse* an order before it can place one. Risk limits are built and tested first; order placement wraps them.

---

## 0. Context & constraints

- **Jurisdiction:** Polymarket order *placement* is blocked from the UK. Data collection and **paper trading are not placement** and run fine from London. Live order placement happens only during the Istanbul window (~10-15 days).
- **Capital:** $500 initial live cap → $2,000-2,500 maximum, and only after clean operation. The $7k reserve is untouchable this summer.
- **Strategy:** H4 (resolution drift) is the first candidate, pending tight-horizon H4-Cal + H4-Exec verdicts. The engine is **strategy-agnostic**: a signal module emits intents; the engine executes and controls risk. H5/H2 plug into the same engine later.
- **Mode discipline:** every component runs identically in paper and live mode; the ONLY difference is whether the order manager hits the real venue API or a simulated fill. This guarantees what you soak in London is what runs in Istanbul.

---

## 1. Risk module (BUILD FIRST — nothing places orders until this is trusted)

Hard-coded limits, checked before EVERY order. Limits live in code, not in the operator's head. If the operator ever finds themselves manually overriding a limit, that is the signal to stop for 48 hours.

**Pre-order checks (all must pass or the order is rejected):**
- **Max position per market:** $100-150 initially. Sum of open exposure in one market may not exceed this.
- **Max total deployed:** starts at $500. Sum of all open exposure. Raised to $2,000-2,500 only after 2 clean weeks.
- **Order sizing:** no single order may push per-market or total exposure past the caps above.
- **Price sanity:** reject orders at implausible prices (e.g. buying above $0.99 or below $0.01 unless explicitly a near-certainty strategy intent) — guards against fat-finger and stale-quote fills.

**Portfolio-level halts (checked continuously, not just per-order):**
- **Max daily loss:** 3% of live capital → engine halts new orders for the day, holds existing positions.
- **Max drawdown:** 15% of live capital → full stop, flatten decision requires manual review.

**Kill switch:**
- One command / one Telegram message flattens all positions (or halts, per config) and stops the engine.
- Must work even if the signal module is broken — it is independent of strategy logic.

**Design note:** the risk module is a pure function of (proposed order, current positions, capital state) → allow/reject + reason. It has no side effects and no network calls, so it is unit-testable exhaustively. Every rejection is logged with its reason.

---

## 2. Position & P&L tracker

- Tracks every open position: market, token, size, entry price, entry time, current mark.
- **Reconciled against the venue's own numbers daily.** They WILL disagree sometimes (partial fills, fees, rounding); every mismatch is investigated, not smoothed over.
- P&L split into: realized (resolved/closed positions), unrealized (open, marked to current price), and fee drag (tracked separately — fees are a first-class cost, not an afterthought).
- Capital-lock-up tracking: a position in a market resolving in N weeks ties up capital for N weeks. Track deployed-vs-available capital and capital utilization; slow markets quietly destroy annualized return.

### 2.1 Resolution sweep (canonical requirement)

> Every market with an open or recently-closed position must have its resolution confirmed directly from Gamma (GET /markets by condition_id, read outcomePrices + umaResolutionStatus), on a daily sweep — never rely solely on the WS market_resolved feed, which only covers watchlisted markets. P&L reconciliation depends on this; a position that resolves unnoticed is an unreconciled position.

Implementation: a daily job iterates every market with an open or recently-closed position, fetches its Gamma resolution directly, and settles the position in the P&L tracker. This is independent of the harvester's watchlist — a position may be in a market the collector isn't watching.

---

## 3. Order manager

- Places / cancels / tracks orders via the venue API (live) or a simulated fill (paper).
- Handles partial fills, rejections, reconnects, and API errors gracefully.
- **Every order passes through the risk module first.** No path places an order without a risk check.
- Records every order intent, submission, fill, and rejection to a trade-level ledger (the same honesty standard as the backtester: requested size, filled size, avg price, fees, unfilled qty, rejection reason).
- Idempotency / dedup: a reconnect or retry must not double-submit an order.

### 3.1 Paper-mode fill model (mirrors H4-Exec)

- Aggressive marketable limit orders, immediate-or-cancel.
- Fill against the recorded live order book (best ask for buys), walk visible levels, never fill more than displayed liquidity; 50% haircut when only BBO is available.
- Configurable latency (base 1s). Reject on closed/suspended market, stale quote, or collector gap.
- Fees applied to filled quantity only.

---

## 4. Signal module (plugs in LAST)

- Reads live collector data, emits trade intents (market, side, size, limit price, TTL).
- For H4: identify effectively-decided near-certainties in their final window, size per risk caps, respect tail risk.
- Strategy-agnostic interface: `signal(market_state) -> [intents]`. H5/H2 implement the same interface later.
- **Not built until the strategy has a verified edge** (H4-Cal + H4-Exec pass). Building a signal before knowing it works is the trap.

---

## 5. Build & validation order

1. **Risk module** + exhaustive unit tests (can it correctly reject every over-limit order?).
2. **Position & P&L tracker** + resolution sweep.
3. **Order manager** in paper mode, wrapping the risk module.
4. **Paper-soak** against live collector data for a full week: log every intended order, compare to what the book would have given, fix every surprise.
5. **Signal module** (only once H4 has a verified edge).
6. **Live**, $500, minimum sizes, operational focus: fills happen, P&L reconciles, alerts fire, nothing crashes at 3am.
7. Scale to full risk budget only after 2 clean weeks.

---

## 6. Non-negotiables (from the roadmap risk rules)

- No strategy goes live without a completed backtest memo and one week of paper trading.
- All risk limits enforced in code. Manual override = stop for 48 hours.
- No leverage, no borrowing, no single conviction trade on news.
- Every week the engine runs, the operator can instantly answer: "what is my max loss if everything resolves against me tonight?" If not, flatten until they can.
- Kill switch tested before any live capital.