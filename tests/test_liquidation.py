import math

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from cascade.fuel.liquidation import LiquidationDetector
from cascade.fuel.margin import MarginTable, liquidation_price
from cascade.schema import EVENT_SCHEMA
from cascade.store.parquet import ParquetWriter

A, B, C = "0xaaa", "0xbbb", "0xccc"
T0 = 1_760_000_000_000  # ms


def trade(tid, px, sz, side, buyer, seller, coin="BTC", t=T0):
    return {
        "coin": coin, "side": side, "px": str(px), "sz": str(sz),
        "time": t + tid, "tid": tid, "hash": f"0x{tid}", "users": [buyer, seller],
    }


def det(**kw):
    return LiquidationDetector(tolerance=0.001, cross_check_min_dev=0.0005, **kw)


def test_long_liquidation_detected():
    d = det(liquidators=(C,))
    d.set_mark("BTC", 90_000)
    d.seed(A, "BTC", 1.0, 80_000)
    # A (long) is the seller, taker buys; fill below liq px, counterparty = liquidator C
    (r,) = d.on_trade(trade(1, 79_900, 0.4, "B", C, A), 5)
    assert r["event_type"] == "long_liq" and r["is_liquidation"] is True
    assert r["side"] == "A" and r["trader"] == A and r["counterparty"] == C
    assert r["notional"] == pytest.approx(79_900 * 0.4)
    assert r["ts_ns"] == (T0 + 1) * 1_000_000 and r["ts_recv_ns"] == 5
    s = d.stats()
    assert s["liquidations"] == 1 and s["b_agreements"] == 1
    assert s["ab_agreement_rate"] == 1.0


def test_short_liquidation_detected():
    d = det()
    d.set_mark("BTC", 100)
    d.seed(A, "BTC", -2.0, 110)
    (r,) = d.on_trade(trade(1, 111, 1.0, "B", A, B), 0)
    assert r["event_type"] == "short_liq" and r["side"] == "B" and r["trader"] == A


def test_voluntary_reduce_is_not_liquidation():
    d = det()
    d.seed(A, "BTC", 1.0, 80_000)
    (r,) = d.on_trade(trade(1, 95_000, 0.5, "A", B, A), 0)
    assert r["event_type"] == "aggr" and r["is_liquidation"] is False
    assert r["trader"] == A  # taker side "A" means the seller is the taker
    assert r["counterparty"] == B


def test_tolerance_boundary():
    d = det()
    d.seed(A, "BTC", 1.0, 80_000)
    # just inside tolerance (80_000 * 1.001 = 80_080)
    (inside,) = d.on_trade(trade(1, 80_079, 0.1, "B", B, A), 0)
    (outside,) = d.on_trade(trade(2, 80_081, 0.1, "B", B, A), 0)
    assert inside["is_liquidation"] and not outside["is_liquidation"]


def test_untracked_addresses_emit_aggr_only():
    d = det()
    (r,) = d.on_trade(trade(1, 100, 1, "B", A, B), 0)
    assert r["event_type"] == "aggr" and r["trader"] == A and r["counterparty"] == B
    assert d.stats()["tracked_positions"] == 0


def test_add_to_position_invalidates_liq_px():
    d = det()
    d.seed(A, "BTC", 1.0, 80_000)
    d.on_trade(trade(1, 100_000, 1.0, "B", A, B), 0)  # A buys more: szi=2, stale
    assert (A, "BTC") in d.needs_reseed
    (r,) = d.on_trade(trade(2, 70_000, 1.0, "A", B, A), 0)
    assert not r["is_liquidation"]  # cannot infer without a liq px
    d.seed(A, "BTC", 1.0, 80_000)
    assert (A, "BTC") not in d.needs_reseed


def test_partial_reduce_keeps_liq_px_and_flip_clears_it():
    d = det()
    d.seed(A, "BTC", 1.0, 80_000)
    d.on_trade(trade(1, 95_000, 0.5, "A", B, A), 0)   # reduce, liq px kept
    assert (A, "BTC") not in d.needs_reseed
    (r,) = d.on_trade(trade(2, 79_000, 0.2, "A", B, A), 0)
    assert r["is_liquidation"]
    d.on_trade(trade(3, 95_000, 1.0, "A", B, A), 0)    # flips short, stale
    assert (A, "BTC") in d.needs_reseed


def test_replayed_trade_is_not_applied_twice():
    d = det()
    d.seed(A, "BTC", 1.0, 80_000)
    t = trade(1, 79_000, 0.4, "B", B, A)
    assert len(d.on_trade(t, 0)) == 1
    assert d.on_trade(t, 0) == []
    assert d.stats()["duplicates_skipped"] == 1 and d.stats()["liquidations"] == 1


def test_no_b_agreement_without_mark_or_liquidator():
    d = det()
    d.seed(A, "BTC", 1.0, 80_000)
    d.on_trade(trade(1, 79_000, 0.4, "B", B, A), 0)
    s = d.stats()
    assert s["liquidations"] == 1 and s["b_agreements"] == 0 and s["ab_agreement_rate"] == 0.0


def test_agreement_rate_nan_without_liquidations():
    assert math.isnan(det().stats()["ab_agreement_rate"])


def test_seed_from_clearinghouse_handles_null_liq_px_and_flat():
    d = det()
    state = {"assetPositions": [
        {"type": "oneWay", "position": {"coin": "BTC", "szi": "-0.5", "liquidationPx": "120000.0"}},
        {"type": "oneWay", "position": {"coin": "ETH", "szi": "3", "liquidationPx": None}},
        {"type": "oneWay", "position": {"coin": "SOL", "szi": "0", "liquidationPx": None}},
    ]}
    assert d.seed_from_clearinghouse(A.upper(), state) == 2
    assert d.stats()["tracked_positions"] == 2
    assert (A, "ETH") in d.needs_reseed  # address lowercased, null liq px flagged


def test_malformed_users_does_not_crash():
    d = det()
    t = trade(1, 100, 1, "B", A, B)
    t["users"] = []
    (r,) = d.on_trade(t, 0)
    assert r["event_type"] == "aggr" and r["trader"] is None


def test_rows_roundtrip_through_parquet(tmp_path):
    rows = []
    d = det(sink=rows.append)
    d.seed(A, "BTC", 1.0, 80_000)
    d(trade(1, 79_000, 0.4, "B", B, A), 1)
    d(trade(2, 100, 1.0, "B", B, C), 2)
    assert {r["event_type"] for r in rows} == {"long_liq", "aggr"}
    with ParquetWriter("events", tmp_path) as w:
        w.extend(rows)
    tbl = pa.concat_tables([pq.read_table(p) for p in (tmp_path / "events").glob("*.parquet")])
    assert tbl.schema.equals(EVENT_SCHEMA) and tbl.num_rows == 2
    assert len(set(tbl.column("raw_key").to_pylist())) == 2


def test_negative_params_rejected():
    with pytest.raises(ValueError):
        LiquidationDetector(tolerance=-1)


def test_seed_from_margin_table_math():
    """Primary signal wired to the verified MarginTable arithmetic (hl_meta.json)."""
    meta = json.loads((Path(__file__).parent.parent / "hl_meta.json").read_text())
    tables = {int(i): MarginTable.from_api(t) for i, t in meta["marginTables"]}
    btc = next(u for u in meta["universe"] if u["name"] == "BTC")
    table = tables[int(btc["marginTableId"])]
    # Same isolated position as the margin.py docstring: ~77024.43 liquidation px.
    liq = liquidation_price(szi=1.0, mark_px=81383.5, margin_available=5321.879258
                            - 81383.5 * table.maintenance_fraction(81383.5), table=table)
    assert liq == pytest.approx(77024.4260678, rel=1e-6)
    d = det()
    d.seed(A, "BTC", 1.0, liq)
    (r,) = d.on_trade(trade(1, liq - 10, 0.1, "B", B, A), 0)
    assert r["event_type"] == "long_liq"
