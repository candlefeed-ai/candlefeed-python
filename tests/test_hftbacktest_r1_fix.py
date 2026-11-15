"""Neighbours of the PR231 R1 findings: late snapshots across breaks and midnight, unanchored days, trade layouts."""
from __future__ import annotations

import json
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from l2_synth import day_ms, write_day
from test_hftbacktest import DAY, SYM, _load, _refresh_manifest, _replay, _write_trades

from candlefeed import hftbacktest as hb
from candlefeed.l2book import IncompleteDay

DAY2 = "2026-09-02"
RECV_REMOVED = pytest.mark.skip(reason="recv latency mode removed in 0.3.8")


def ev(ms, u, pu, price, qty):
    return (ms, u, u, pu, [("bid", price, Decimal(qty))])


def snap(T, L, recv_ms, bids, asks=((10001, "3"),)):
    return ("tokyo", L, T, recv_ms * 10**6, [(p, Decimal(q)) for p, q in bids], [(p, Decimal(q)) for p, q in asks])


def books(data, flag, times_ms):
    return list(_replay(data, flag, [t * 10**6 for t in times_ms]))


@RECV_REMOVED
def test_two_late_snapshots_in_one_segment(tmp_path):
    t = day_ms(DAY)
    write_day(tmp_path, DAY, SYM, [ev(t + 1000 * i, 100 + i, 99 + i, 9989 + i, str(i)) for i in range(1, 6)],
              [snap(t + 1000, 101, t + 2500, [(9990, "1")]),
               snap(t + 3000, 103, t + 3500, [(9990, "1"), (9991, "2"), (9992, "3")])])
    result = hb.convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "o", latency="recv")
    data = _load(result)
    asks = {10001: 3.0}
    local = books(data, hb.LOCAL_EVENT, [t + 2400, t + 2600, t + 3100, t + 3600, t + 4100, t + 5100])
    assert local == [({}, {}),
                     ({9990: 1.0, 9991: 2.0}, asks),
                     ({9990: 1.0, 9991: 2.0, 9992: 3.0}, asks),
                     ({9990: 1.0, 9991: 2.0, 9992: 3.0}, asks),
                     ({9990: 1.0, 9991: 2.0, 9992: 3.0, 9993: 4.0}, asks),
                     ({9990: 1.0, 9991: 2.0, 9992: 3.0, 9993: 4.0, 9994: 5.0}, asks)]
    (exch, _), = books(data, hb.EXCH_EVENT, [t + 5100])
    assert exch == local[-1][0]


@RECV_REMOVED
def test_chain_break_arriving_before_a_late_anchor(tmp_path):
    t = day_ms(DAY)
    write_day(tmp_path, DAY, SYM,
              [ev(t + 1000, 101, 100, 9990, "1"), ev(t + 2000, 102, 101, 9991, "2"),
               ev(t + 3000, 201, 199, 8000, "1"), ev(t + 4000, 202, 201, 8001, "2"), ev(t + 5000, 203, 202, 8002, "3")],
              [snap(t + 1000, 101, t + 1000, [(9990, "1")]),
               snap(t + 3000, 201, t + 4500, [(8000, "1")], [(10005, "2")])])
    result = hb.convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "o", latency="recv")
    assert result.stats["chain_breaks_cleared"] == 1
    data = _load(result)
    local = books(data, hb.LOCAL_EVENT, [t + 2500, t + 3500, t + 4600, t + 5100])
    assert local == [({9990: 1.0, 9991: 2.0}, {10001: 3.0}),
                     ({}, {}),
                     ({8000: 1.0, 8001: 2.0}, {10005: 2.0}),
                     ({8000: 1.0, 8001: 2.0, 8002: 3.0}, {10005: 2.0})]
    (exch, _), = books(data, hb.EXCH_EVENT, [t + 5100])
    assert exch == local[-1][0]


@RECV_REMOVED
@pytest.mark.parametrize("continuous", [True, False])
def test_late_snapshot_across_midnight(tmp_path, continuous):
    t = day_ms(DAY2)
    write_day(tmp_path, DAY, SYM, [ev(t - 2000, 101, 100, 9990, "1"), ev(t - 1000, 102, 101, 9991, "2")],
              [snap(t - 2000, 101, t + 300, [(9990, "1")])])
    if continuous:
        day2 = [ev(t + 100, 103, 102, 9992, "3"), ev(t + 500, 104, 103, 9993, "4")]
    else:
        day2 = [ev(t + 100, 201, 199, 8000, "1"), ev(t + 500, 202, 201, 8001, "2")]
    write_day(tmp_path, DAY2, SYM, day2, [])
    result = hb.convert_l2(tmp_path, SYM, DAY, DAY2, tmp_path / "o", latency="recv")
    data = _load(result)
    local = books(data, hb.LOCAL_EVENT, [t - 500, t + 200, t + 400, t + 600])
    exch = books(data, hb.EXCH_EVENT, [t + 600])
    assert local[:2] == [({}, {}), ({}, {})]
    if continuous:
        assert local[2:] == [({9990: 1.0, 9991: 2.0, 9992: 3.0}, {10001: 3.0}),
                             ({9990: 1.0, 9991: 2.0, 9992: 3.0, 9993: 4.0}, {10001: 3.0})]
        assert exch == local[3:]
        assert result.unanchored_days == []
    else:
        assert local[2:] == [({}, {}), ({}, {})]
        assert exch == [({}, {})]
        assert result.unanchored_days == [(DAY2, 2, 1)]


def test_unanchored_day_without_anything_to_write_is_named_and_skipped(tmp_path, capsys):
    t1, t2 = day_ms(DAY), day_ms(DAY2)
    write_day(tmp_path, DAY, SYM, [ev(t1 + 1000, 101, 100, 9990, "1")], [])
    write_day(tmp_path, DAY2, SYM, [ev(t2 + 1000, 201, 200, 9990, "1")], [snap(t2 + 1000, 201, t2 + 1000, [(9990, "1")])])
    out = tmp_path / "o"
    assert hb.main(["--symbol", SYM, "--start", DAY, "--end", DAY2, "--data-dir", str(tmp_path), "--out", str(out),
                    "--latency", "4ms"]) == 0
    summary = json.loads((out / hb.SUMMARY_NAME).read_text())
    assert summary["files"] == ["BTCUSDT_20260902_00.npz"]
    assert summary["days_without_anchor"] == [{"day": DAY, "diff_events_not_written": 1, "files": 0}]
    assert set(summary["latency_by_day"]) == {DAY, DAY2}
    assert "recorded_recv_latency_depth_p50_us" in summary["latency_by_day"][DAY2]
    assert f"{DAY}: no usable snapshot anchors the book, so its 1 diff events weren't written" in capsys.readouterr().out


def test_failed_run_that_writes_nothing_keeps_the_old_file_list(tmp_path):
    t = day_ms(DAY)
    write_day(tmp_path, DAY, SYM, [ev(t + 1000, 101, 100, 9990, "1")], [snap(t + 1000, 101, t + 1000, [(9990, "1")])])
    first = hb.convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "o", latency="4ms")
    write_day(tmp_path / "x", DAY, SYM, [ev(t + 1000, 101, 100, 9990, "1")], [])
    with pytest.raises(hb.ConvertError, match="anchor"):
        hb.convert_l2(tmp_path / "x", SYM, DAY, out_dir=tmp_path / "o", latency="4ms")
    assert json.loads(first.summary.read_text())["files"] == ["BTCUSDT_20260901_00.npz"]


def _trades_with(tmp_path, column, array):
    t = day_ms(DAY)
    write_day(tmp_path, DAY, SYM, [ev(t + 1000, 101, 100, 9990, "1")], [snap(t + 1000, 101, t + 1000, [(9990, "1")])])
    ddir = _write_trades(tmp_path, DAY, [(1, t + 1000, (t + 1001) * 10**6, Decimal("999.9"), Decimal("1"),
                                          "buy", "MARKET", False)])
    table = pq.read_table(ddir / "trades.parquet")
    table = table.set_column(table.schema.get_field_index(column), column, array)
    pq.write_table(table, ddir / "trades.parquet")
    _refresh_manifest(ddir)


@pytest.mark.parametrize("column,array,match", [
    ("side", pa.array([1], pa.int64()), "'side' has type int64"),
    ("order_type", pa.array([0], pa.int32()), "'order_type' has type int32"),
    ("is_backfill", pa.array([None], pa.bool_()), "'is_backfill' has 1 nulls"),
    ("is_backfill", pa.array([0], pa.int8()), "'is_backfill' has type int8"),
    ("side", pa.array(["buy"]).dictionary_encode(), None),
])
def test_trade_column_layouts(tmp_path, column, array, match):
    _trades_with(tmp_path, column, array)
    if match is None:
        assert hb.convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "o", latency="4ms", trades=True).stats["trades"] == 1
        return
    with pytest.raises(IncompleteDay, match=match):
        hb.convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "o", latency="4ms", trades=True)


@pytest.mark.parametrize("latency", [pytest.param("recv", marks=RECV_REMOVED), "4ms"])
def test_only_market_and_untyped_backfill_trades_are_kept(tmp_path, latency):
    t = day_ms(DAY)
    write_day(tmp_path, DAY, SYM, [ev(t + 1000, 101, 100, 9990, "1")], [snap(t + 1000, 101, t + 1000, [(9990, "1")])])
    _write_trades(tmp_path, DAY, [
        (1, t + 500, (t + 501) * 10**6, Decimal("999.9"), Decimal("1"), "buy", "MARKET", False),
        (2, t + 600, (t + 9000) * 10**6, Decimal("999.9"), Decimal("2"), "sell", None, True),
        (3, t + 700, (t + 701) * 10**6, Decimal("999.9"), Decimal("3"), "buy", None, False),
        (4, t + 800, (t + 801) * 10**6, Decimal("999.9"), Decimal("4"), "buy", "NA", False),
        (5, t + 900, (t + 901) * 10**6, Decimal("999.9"), Decimal("5"), "buy", "NA", True),
    ])
    result = hb.convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "o", latency=latency, trades=True)
    s = result.stats
    assert (s["trades"], s["trades_dropped_NA"], s["trades_dropped_live_null_order_type"],
            s["trades_backfill_null_order_type"]) == (2, 2, 1, 1)
    data = _load(result)
    rows = data[(data["ev"] & 0xFF) == hb.TRADE_EVENT]
    assert sorted(rows["qty"].tolist()) == [1.0, 2.0]
    backfill = rows[rows["qty"] == 2.0]["ev"][0]
    expect = hb.EXCH_EVENT if latency == "recv" else hb.EXCH_EVENT | hb.LOCAL_EVENT
    assert int(backfill) & (hb.EXCH_EVENT | hb.LOCAL_EVENT) == expect
