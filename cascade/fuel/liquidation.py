"""Liquidation inference from the public trade stream.

Hyperliquid has no global liquidation feed, so liquidations are inferred.

Signal A (creates the event): a tracked address's position is reduced by a trade
at or beyond that position's liquidation price. Signal B (cross-check only):
the fill price deviates from mark, or the counterparty is a known liquidator.
B never creates an event; it is counted so A/B agreement can serve as a
precision proxy while no labelled ground truth exists.

Limits, by construction:
- Trades carry no collateral, so the liquidation price of an address cannot be
  recomputed from the stream. It is seeded from `clearinghouseState`
  (`liquidationPx`) and kept while the trade only reduces the position.
  A trade that opens, adds to or flips a position invalidates it until the
  next re-seed, and the address is listed in `needs_reseed`.
- Addresses never seeded are not tracked, so recall is bounded by the polled
  universe. `stats()` exposes how many addresses are tracked.

Trade message shape assumed (verify against a live payload): coin, side (taker
side, "B" or "A"), px, sz, time (ms), tid, users = [buyer, seller].
"""
from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Callable

from cascade.config import settings
from cascade.schema import EventType

_EPS = 1e-12
_DEDUPE_CAPACITY = 200_000


@dataclass
class _Position:
    szi: float
    liq_px: float | None


def _row(
    *,
    ts_ns: int,
    recv_ns: int,
    asset: str,
    event_type: str,
    side: str,
    size: float,
    price: float,
    trader: str | None,
    counterparty: str | None,
    is_liquidation: bool,
    seq: int | None,
    raw_key: str,
) -> dict[str, Any]:
    return {
        "ts_ns": ts_ns,
        "ts_recv_ns": recv_ns,
        "venue": "hyperliquid",
        "asset": asset,
        "event_type": event_type,
        "side": side,
        "size": size,
        "price": price,
        "notional": size * price,
        "trader": trader,
        "counterparty": counterparty,
        "trader_class": "unknown",
        "is_liquidation": is_liquidation,
        "surprise": None,
        "seq": seq,
        "raw_key": raw_key,
    }


class LiquidationDetector:
    """Turns raw trades into `EVENT_SCHEMA` rows, flagging inferred liquidations.

    Not thread-safe; feed it from one asyncio task. Usable directly as the
    `on_message` callback of `stream_trades`.
    """

    def __init__(
        self,
        sink: Callable[[dict[str, Any]], None] | None = None,
        *,
        tolerance: float | None = None,
        cross_check_min_dev: float | None = None,
        liquidators: tuple[str, ...] | None = None,
    ) -> None:
        self._sink = sink
        self._tol = settings.liq_price_tolerance if tolerance is None else tolerance
        self._min_dev = (
            settings.liq_cross_check_min_dev
            if cross_check_min_dev is None
            else cross_check_min_dev
        )
        liq = settings.liquidator_addresses if liquidators is None else liquidators
        self._liquidators = frozenset(a.lower() for a in liq)
        if self._tol < 0 or self._min_dev < 0:
            raise ValueError("tolerance and cross_check_min_dev must be >= 0")

        self._pos: dict[tuple[str, str], _Position] = {}
        self._mark: dict[str, float] = {}
        self._seen: OrderedDict[tuple[str, Any], None] = OrderedDict()
        self.needs_reseed: set[tuple[str, str]] = set()
        self._n_trades = 0
        self._n_dupes = 0
        self._n_liq = 0
        self._n_b_agree = 0

    # --- state in -----------------------------------------------------

    def set_mark(self, asset: str, mark_px: float) -> None:
        if mark_px > 0:
            self._mark[asset] = mark_px

    def seed(self, address: str, asset: str, szi: float, liq_px: float | None) -> None:
        """Set (or correct) an address's position and liquidation price."""
        key = (address.lower(), asset)
        if abs(szi) < _EPS:
            self._pos.pop(key, None)
            self.needs_reseed.discard(key)
            return
        self._pos[key] = _Position(szi, liq_px if liq_px and liq_px > 0 else None)
        if self._pos[key].liq_px is None:
            self.needs_reseed.add(key)
        else:
            self.needs_reseed.discard(key)

    def seed_from_clearinghouse(self, address: str, state: dict[str, Any]) -> int:
        """Seed every position in a `clearinghouseState` payload. Returns count seeded."""
        n = 0
        for ap in state.get("assetPositions", []):
            p = ap.get("position", ap)
            szi = float(p["szi"])
            liq = p.get("liquidationPx")
            self.seed(address, p["coin"], szi, float(liq) if liq is not None else None)
            n += abs(szi) >= _EPS
        return n

    # --- trades in ----------------------------------------------------

    def __call__(self, trade: dict[str, Any], recv_ns: int) -> list[dict[str, Any]]:
        rows = self.on_trade(trade, recv_ns)
        if self._sink is not None:
            for r in rows:
                self._sink(r)
        return rows

    def on_trade(self, trade: dict[str, Any], recv_ns: int) -> list[dict[str, Any]]:
        asset = trade["coin"]
        tid = trade.get("tid", trade.get("hash"))
        dedupe_key = (asset, tid)
        if tid is not None:
            # Reconnects replay recent trades; applying one twice corrupts positions.
            if dedupe_key in self._seen:
                self._n_dupes += 1
                return []
            self._seen[dedupe_key] = None
            if len(self._seen) > _DEDUPE_CAPACITY:
                self._seen.popitem(last=False)
        self._n_trades += 1

        px = float(trade["px"])
        sz = float(trade["sz"])
        ts_ns = int(trade["time"]) * 1_000_000
        taker_is_buyer = trade["side"] == "B"
        users = trade.get("users") or []
        buyer, seller = (
            (str(users[0]).lower(), str(users[1]).lower()) if len(users) == 2 else (None, None)
        )
        seq = int(tid) if isinstance(tid, (int, float)) or str(tid).isdigit() else None
        base_key = f"{asset}:{tid}"

        liq_rows: list[dict[str, Any]] = []
        for addr, other, delta in ((seller, buyer, -sz), (buyer, seller, sz)):
            if addr is None:
                continue
            if self._apply(addr, asset, delta, px, other):
                is_long = delta < 0  # a seller being liquidated was long
                liq_rows.append(
                    _row(
                        ts_ns=ts_ns,
                        recv_ns=recv_ns,
                        asset=asset,
                        event_type=(
                            EventType.LONG_LIQUIDATION if is_long else EventType.SHORT_LIQUIDATION
                        ).value,
                        side="A" if is_long else "B",  # side of the forced order
                        size=sz,
                        price=px,
                        trader=addr,
                        counterparty=other,
                        is_liquidation=True,
                        seq=seq,
                        raw_key=f"{base_key}:liq:{addr}",
                    )
                )
        if liq_rows:
            return liq_rows

        taker, maker = (buyer, seller) if taker_is_buyer else (seller, buyer)
        return [
            _row(
                ts_ns=ts_ns,
                recv_ns=recv_ns,
                asset=asset,
                event_type=EventType.AGGRESSIVE_FLOW.value,
                side=trade["side"],
                size=sz,
                price=px,
                trader=taker,
                counterparty=maker,
                is_liquidation=False,
                seq=seq,
                raw_key=base_key,
            )
        ]

    def _apply(
        self, addr: str, asset: str, delta: float, px: float, other: str | None
    ) -> bool:
        """Update a tracked position with a fill. True if the fill is a liquidation."""
        key = (addr, asset)
        pos = self._pos.get(key)
        if pos is None:
            return False
        prev = pos.szi
        new = prev + delta
        if abs(new) < _EPS:
            new = 0.0

        liquidated = False
        if pos.liq_px is not None:
            if prev > 0 and delta < 0:
                liquidated = px <= pos.liq_px * (1.0 + self._tol)
            elif prev < 0 and delta > 0:
                liquidated = px >= pos.liq_px * (1.0 - self._tol)

        reduces_same_side = prev * new > 0 and abs(new) < abs(prev)
        pos.szi = new
        if new == 0.0:
            pos.liq_px = None
        elif not reduces_same_side:
            # Opened, added to, or flipped: stale liquidation price.
            pos.liq_px = None
            self.needs_reseed.add(key)

        if liquidated:
            self._n_liq += 1
            mark = self._mark.get(asset)
            dev_hit = mark is not None and abs(px - mark) / mark >= self._min_dev
            if dev_hit or (other is not None and other in self._liquidators):
                self._n_b_agree += 1
        return liquidated

    # --- reporting ----------------------------------------------------

    def stats(self) -> dict[str, float | int]:
        return {
            "trades": self._n_trades,
            "duplicates_skipped": self._n_dupes,
            "liquidations": self._n_liq,
            "b_agreements": self._n_b_agree,
            "ab_agreement_rate": self._n_b_agree / self._n_liq if self._n_liq else float("nan"),
            "tracked_positions": len(self._pos),
            "needs_reseed": len(self.needs_reseed),
        }
