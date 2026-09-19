"""Hyperliquid margin and liquidation-price arithmetic.

Verified against a live isolated position on 2026-09-19:
    szi=1.0 BTC, mark=81383.5, isolated_margin=5321.879258, max_leverage=40
    -> 77024.4260678 computed vs 77024.4260678481 reported by the API.

Reference: liquidation price solves the point at which equity falls to the
maintenance requirement, with maintenance leverage fixed at twice the tier's
max leverage.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MarginTier:
    lower_bound: float  # position notional at/above which this tier applies
    max_leverage: float


@dataclass(frozen=True)
class MarginTable:
    tiers: tuple[MarginTier, ...]

    @classmethod
    def from_api(cls, payload: dict) -> "MarginTable":
        tiers = tuple(
            MarginTier(float(t["lowerBound"]), float(t["maxLeverage"]))
            for t in payload["marginTiers"]
        )
        if not tiers:
            raise ValueError("marginTable has no tiers")
        return cls(tuple(sorted(tiers, key=lambda t: t.lower_bound)))

    def max_leverage_at(self, notional: float) -> float:
        lev = self.tiers[0].max_leverage
        for t in self.tiers:
            if abs(notional) >= t.lower_bound:
                lev = t.max_leverage
            else:
                break
        return lev

    def maintenance_fraction(self, notional: float) -> float:
        """l = 1 / (2 * max_leverage). Maintenance leverage is twice max."""
        return 1.0 / (2.0 * self.max_leverage_at(notional))


def maintenance_margin(notional: float, table: MarginTable) -> float:
    return abs(notional) * table.maintenance_fraction(notional)


def liquidation_price(
    *,
    szi: float,
    mark_px: float,
    margin_available: float,
    table: MarginTable,
) -> float | None:
    """Price at which the position is liquidated, or None if unreachable.

    `margin_available` is account equity net of the maintenance requirement:
      cross    -> account_value - cross_maintenance_margin_used
      isolated -> isolated_margin - maintenance_margin(this position)

    Returns None when the implied price is non-positive, matching the API,
    which reports `liquidationPx: null` in that case.
    """
    if szi == 0:
        return None
    side = 1.0 if szi > 0 else -1.0
    l = table.maintenance_fraction(szi * mark_px)
    denom = 1.0 - l * side
    if denom == 0:
        return None
    px = mark_px - side * margin_available / abs(szi) / denom
    return px if px > 0 else None


def distance_to_liquidation(
    mark_px: float, liq_px: float | None, szi: float
) -> float | None:
    """Signed fractional gap: how far price must move to trigger liquidation.

    Always returned as a positive fraction of mark price for a live position
    (longs liquidate below, shorts above). None when unreachable.
    """
    if liq_px is None or mark_px <= 0:
        return None
    if szi > 0:
        return max(0.0, (mark_px - liq_px) / mark_px)
    return max(0.0, (liq_px - mark_px) / mark_px)
