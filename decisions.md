# Decisions

Log of design decisions for Cascade Clock. Newest last. Status: `decided`, `open`.

## D1: Remove stray worktree gitlink (decided)
Commit `47100af` ("building order book") only added `.claude/worktrees/cascade-clock`
as a mode-160000 gitlink, not source code. It is untracked and `.claude/worktrees/`
is now gitignored.

## D2: Next module is `cascade/fuel/liquidation.py` (decided)
Rationale: `sources/hyperliquid.py` documents that no public liquidation feed exists,
so liquidations must be inferred from the trade stream. This feeds the Hawkes model.
Alternatives deferred: order book builder, Hawkes fitting (needs D2 events first).

## D3: Inference rule (decided)
- Primary signal A: a trade that reduces or closes a tracked address's position at or
  beyond that position's liquidation price (long: px <= liq_px, short: px >= liq_px,
  within a small tolerance), liquidation price from `MarginTable` arithmetic.
- Cross-check signal B: trade price deviation from mark, and counterparty identity.
  B never creates an event alone. It only lowers or raises confidence in A.
- Tradeoff accepted: trades carry no collateral data, so liq_px for tracked addresses is
  seeded from `clearinghouseState` (API-reported `liquidationPx`) and updated by
  position deltas from the trade stream between polls. Untracked addresses are missed,
  so recall is bounded by universe coverage. Coverage is reported, not hidden.

## D4: Ground truth (decided)
No labelled liquidation set exists. Validation is: (1) synthetic replay tests with
known liquidations, (2) A/B agreement rate on live data as a precision proxy,
(3) manual spot check of a small live sample. Claims about accuracy stay "unverified"
until a labelled set exists.

## D5: Output (decided)
All rows go to `EVENT_SCHEMA` as `long_liq` / `short_liq` with `is_liquidation=True`.
Non-liquidation trades from the same stream are written as `aggr` rows, as the schema
intends. Schema is not extended: confidence is not stored (tradeoff: simpler schema,
cost: no per-row confidence for downstream filtering). Revisit if the Hawkes fit needs it.

## Open items to verify before coding
- Liquidator counterparty address(es) for signal B: verify against live data, not memory.
- Tolerance for the liq_px comparison: tune on live sample.
