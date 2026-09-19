"""The one event table. Everything downstream reads this and nothing else.

Time is UTC nanoseconds since epoch, int64, always. Venue clocks differ; the
`ts_ns` column is exchange time where the venue provides it and local receive
time otherwise, with `ts_recv_ns` always local so clock skew stays measurable.
"""
from __future__ import annotations

from enum import Enum

import pyarrow as pa


class EventType(str, Enum):
    """Phase 1 event types. Values are stored as-is in Parquet."""

    LONG_LIQUIDATION = "long_liq"
    SHORT_LIQUIDATION = "short_liq"
    AGGRESSIVE_FLOW = "aggr"          # taker trade, class in `trader_class`
    LIQUIDITY_WITHDRAWAL = "cancel"   # maker cancel near touch
    AUTO_DELEVERAGE = "adl"
    # exogenous
    MACRO_RELEASE = "macro"
    FUNDING_SETTLEMENT = "funding"
    VENUE_JUMP = "venue_jump"         # e.g. Binance price jump
    # non-event state rows (not fed to the point process)
    BOOK_SNAPSHOT = "book"
    OPTION_TRADE = "opt_trade"
    OPTION_QUOTE = "opt_quote"
    INDEX = "index"


class TraderClass(str, Enum):
    """Assigned only from an address's *past* behaviour. No lookahead."""

    MARKET_MAKER = "mm"
    LARGE_DIRECTIONAL = "large"
    RETAIL = "retail"
    UNKNOWN = "unknown"


#: Point-process event types. Anything else is state, not an event.
POINT_EVENT_TYPES: tuple[str, ...] = (
    EventType.LONG_LIQUIDATION.value,
    EventType.SHORT_LIQUIDATION.value,
    EventType.AGGRESSIVE_FLOW.value,
    EventType.LIQUIDITY_WITHDRAWAL.value,
    EventType.AUTO_DELEVERAGE.value,
)

#: Exogenous types get their own kernel psi, not a self-exciting phi.
EXOGENOUS_EVENT_TYPES: tuple[str, ...] = (
    EventType.MACRO_RELEASE.value,
    EventType.FUNDING_SETTLEMENT.value,
    EventType.VENUE_JUMP.value,
)

EVENT_SCHEMA = pa.schema(
    [
        pa.field("ts_ns", pa.int64(), nullable=False),
        pa.field("ts_recv_ns", pa.int64(), nullable=False),
        pa.field("venue", pa.string(), nullable=False),
        pa.field("asset", pa.string(), nullable=False),
        pa.field("event_type", pa.string(), nullable=False),
        pa.field("side", pa.string()),            # "B" / "A" / None
        pa.field("size", pa.float64()),           # base units
        pa.field("price", pa.float64()),
        pa.field("notional", pa.float64()),       # size * price, quote units
        pa.field("trader", pa.string()),          # address, when public
        pa.field("counterparty", pa.string()),
        pa.field("trader_class", pa.string()),
        pa.field("is_liquidation", pa.bool_()),
        pa.field("surprise", pa.float64()),       # exogenous events only
        pa.field("seq", pa.int64()),              # venue sequence / trade id
        pa.field("raw_key", pa.string()),         # dedupe key
    ]
)

#: Fuel map rows, written on the poll cadence, joined onto events by asof time.
FUEL_SCHEMA = pa.schema(
    [
        pa.field("ts_ns", pa.int64(), nullable=False),
        pa.field("asset", pa.string(), nullable=False),
        pa.field("mark_price", pa.float64(), nullable=False),
        pa.field("band", pa.float64(), nullable=False),      # delta
        pa.field("fuel_long", pa.float64(), nullable=False),  # notional at risk
        pa.field("fuel_short", pa.float64(), nullable=False),
        pa.field("oi_notional", pa.float64()),               # venue-reported OI
        pa.field("n_addresses", pa.int64()),                 # polled this cycle
        pa.field("covered_notional", pa.float64()),          # sum |pos| polled
        pa.field("coverage_ratio", pa.float64()),            # covered / OI
    ]
)

POSITION_SCHEMA = pa.schema(
    [
        pa.field("ts_ns", pa.int64(), nullable=False),
        pa.field("address", pa.string(), nullable=False),
        pa.field("asset", pa.string(), nullable=False),
        pa.field("szi", pa.float64(), nullable=False),        # signed size
        pa.field("entry_px", pa.float64()),
        pa.field("liquidation_px", pa.float64()),
        pa.field("leverage", pa.float64()),
        pa.field("margin_type", pa.string()),                 # cross / isolated
        pa.field("position_value", pa.float64()),
        pa.field("unrealized_pnl", pa.float64()),
        pa.field("account_value", pa.float64()),
    ]
)

SCHEMAS: dict[str, pa.Schema] = {
    "events": EVENT_SCHEMA,
    "fuel": FUEL_SCHEMA,
    "positions": POSITION_SCHEMA,
}


def empty_table(name: str) -> pa.Table:
    return SCHEMAS[name].empty_table()
