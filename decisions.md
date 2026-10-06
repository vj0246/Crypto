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

## D6: Implementation tradeoffs (decided)
- One row per trade: a liquidation trade emits only a `long_liq`/`short_liq` row
  (trader = liquidated address, side = side of the forced order), never also an `aggr`
  row. Other trades emit one `aggr` row (trader = taker). Keeps counts honest for Hawkes.
- Only seeded addresses are tracked. Starting a position at zero from the stream would be
  wrong, so recall is bounded by the polled universe.
- Add/flip trades clear the stored liq_px (cannot recompute without collateral) and put the
  address in `needs_reseed` so the poller can prioritize it. Cost: misses between re-seeds.
- Trades are deduplicated by (coin, tid) with a bounded 200k window, because websocket
  reconnects replay trades and a double-applied fill corrupts positions.
- Detector is not wired into a recorder yet. It works as the `stream_trades` callback and
  takes an optional sink (e.g. `ParquetWriter.append`). Wiring and the fuel poller are separate work.
- Defaults `liq_price_tolerance=0.001`, `liq_cross_check_min_dev=0.0005` are untuned guesses.

## Verification status
- Verified: 15 unit tests pass (synthetic liquidations both sides, tolerance edges,
  reseed, dedupe, malformed users, Parquet schema roundtrip, margin-table liq px
  reproducing the 77024.426 figure). A deliberate bug injection made 8 tests fail.
- NOT verified: live API is blocked in this sandbox (proxy 403), so these are unconfirmed:
  trade payload shape (`users = [buyer, seller]`, `side` = taker side), `liquidationPx`
  field name in `clearinghouseState`, and the liquidator counterparty addresses.
  `liquidator_addresses` therefore defaults to empty and signal B uses price deviation only.
- Accuracy of the inference is unmeasured: no labelled data.

## Open items
- Run against live data from an unrestricted network and confirm the three unverified items.
- Tune tolerance on a live sample.
