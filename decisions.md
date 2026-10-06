# Decisions

Log of design decisions for Cascade Clock. Newest last. Status: `decided`, `open`.

## D1: Remove stray worktree gitlink (decided)
Commit `47100af` ("building order book") only added `.claude/worktrees/cascade-clock`
as a mode-160000 gitlink, not source code. It is untracked and `.claude/worktrees/`
is now gitignored.

## D2: Next module is `cascade/fuel/liquidation.py` (open, awaiting confirmation)
Rationale: `sources/hyperliquid.py` documents that no public liquidation feed exists,
so liquidations must be inferred from the trade stream. This feeds the Hawkes model.
Alternatives: order book builder (needs L2 subscription, not yet in recorder),
Hawkes fitting (needs events from D2 first).

## Open questions for D2
- Inference rule: which signals mark a fill as a liquidation (e.g. position crosses
  computed liquidation price via `MarginTable`, counterparty is the liquidator vault,
  price vs mark deviation)?
- Ground truth: is there any labelled liquidation sample to validate against, or only
  the margin-math check already done in `fuel/margin.py`?
- Output: `EventType.LONG_LIQUIDATION` / `SHORT_LIQUIDATION` rows in `EVENT_SCHEMA` only?
- Order book: deferred until D2 is done, unless priorities change.
