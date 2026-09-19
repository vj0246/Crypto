"""Hyperliquid info API client and live websocket recorder.

Two hard constraints discovered by probing the live API on 2026-09-19, both of
which shape this module:

1. There is no batch position endpoint (`clearinghouseStates` returns 422) and
   the info budget is roughly 1200 weight/min, with `clearinghouseState` at
   weight 2. That caps position polling near 10 req/s, so polling a large
   address universe on a one-minute cadence is impossible. Positions are
   therefore tracked incrementally from the trade stream, which carries both
   counterparty addresses for free, and polling is used only to correct drift.

2. There is no public global liquidation feed. `userFills` carries no
   `liquidation` field and `dir` has no liquidation value. Liquidations are
   inferred (see `cascade.fuel.liquidation`), never read off a flag.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import time
from typing import Any, AsyncIterator, Callable

import aiohttp
import websockets

from cascade.config import settings
from cascade.fuel.margin import MarginTable

log = logging.getLogger(__name__)

WEIGHT = {"clearinghouseState": 2, "meta": 20, "metaAndAssetCtxs": 20, "userFills": 20}
DEFAULT_WEIGHT = 2
BUDGET_PER_MIN = 1000  # documented 1200; leave headroom for the WS handshake


class RateLimiter:
    """Token bucket over the documented per-IP weight budget."""

    def __init__(self, budget_per_min: int = BUDGET_PER_MIN) -> None:
        self.capacity = float(budget_per_min)
        self.tokens = float(budget_per_min)
        self.refill_per_s = budget_per_min / 60.0
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, weight: int) -> None:
        async with self._lock:
            while True:
                now = time.monotonic()
                self.tokens = min(
                    self.capacity, self.tokens + (now - self._last) * self.refill_per_s
                )
                self._last = now
                if self.tokens >= weight:
                    self.tokens -= weight
                    return
                await asyncio.sleep((weight - self.tokens) / self.refill_per_s)


class HyperliquidInfo:
    """Async client for the POST /info endpoint."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        url: str | None = None,
        limiter: RateLimiter | None = None,
    ) -> None:
        self._session = session
        self._url = url or settings.hl_info_url
        self._limiter = limiter or RateLimiter()

    async def post(self, payload: dict[str, Any], retries: int = 3) -> Any:
        weight = WEIGHT.get(payload.get("type", ""), DEFAULT_WEIGHT)
        last: Exception | None = None
        for attempt in range(retries):
            await self._limiter.acquire(weight)
            try:
                async with self._session.post(
                    self._url, json=payload, timeout=aiohttp.ClientTimeout(total=20)
                ) as r:
                    if r.status == 429:
                        raise aiohttp.ClientResponseError(
                            r.request_info, r.history, status=429, message="rate limited"
                        )
                    r.raise_for_status()
                    return await r.json()
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last = exc
                await asyncio.sleep(min(30.0, 2**attempt) * (0.5 + random.random()))
        raise RuntimeError(f"hyperliquid info failed after {retries}: {last}") from last

    async def meta(self) -> dict[str, Any]:
        return await self.post({"type": "meta"})

    async def margin_tables(self) -> dict[str, MarginTable]:
        """Map asset name -> MarginTable, resolved through marginTableId."""
        m = await self.meta()
        by_id = {int(i): MarginTable.from_api(t) for i, t in m["marginTables"]}
        out: dict[str, MarginTable] = {}
        for u in m["universe"]:
            tid = u.get("marginTableId")
            if tid is not None and int(tid) in by_id:
                out[u["name"]] = by_id[int(tid)]
        return out

    async def asset_contexts(self) -> dict[str, dict[str, Any]]:
        """Map asset name -> {markPx, oraclePx, funding, openInterest, ...}."""
        meta, ctxs = await self.post({"type": "metaAndAssetCtxs"})
        return {u["name"]: c for u, c in zip(meta["universe"], ctxs)}

    async def clearinghouse_state(self, address: str) -> dict[str, Any]:
        return await self.post({"type": "clearinghouseState", "user": address})


async def stream_trades(
    assets: tuple[str, ...],
    on_message: Callable[[dict[str, Any], int], None],
    url: str | None = None,
    stop: asyncio.Event | None = None,
) -> None:
    """Subscribe to `trades` for each asset and pump messages to `on_message`.

    Reconnects with exponential backoff forever; a recorder that dies on a
    dropped socket cannot support a 24/7 claim.
    """
    url = url or settings.hl_ws_url
    backoff = 1.0
    while not (stop and stop.is_set()):
        try:
            async with websockets.connect(
                url, max_size=None, ping_interval=settings.ws_ping_interval_s
            ) as ws:
                for coin in assets:
                    await ws.send(
                        json.dumps(
                            {
                                "method": "subscribe",
                                "subscription": {"type": "trades", "coin": coin},
                            }
                        )
                    )
                backoff = 1.0
                log.info("hyperliquid ws connected: %s", ",".join(assets))
                while not (stop and stop.is_set()):
                    raw = await asyncio.wait_for(ws.recv(), timeout=120)
                    recv_ns = time.time_ns()
                    msg = json.loads(raw)
                    if msg.get("channel") == "trades":
                        for t in msg["data"]:
                            on_message(t, recv_ns)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - reconnect on anything
            log.warning("hyperliquid ws dropped (%s); retry in %.1fs", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(settings.ws_reconnect_max_backoff_s, backoff * 2)
