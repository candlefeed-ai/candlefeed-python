"""Offline PR231 R2 regressions; failing assertions describe the required behavior."""
from __future__ import annotations

import hashlib
import json
import socket
from decimal import Decimal
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from l2_synth import day_ms, write_day
from test_hftbacktest import DAY, SYM, _load, _refresh_manifest, _replay, _write_trades
from test_hftbacktest_r1_fix import ev, snap

from candlefeed import hftbacktest as hb
from candlefeed.l2book import IncompleteDay

DAY2 = "2026-09-02"
RECV = pytest.param("recv", marks=pytest.mark.skip(reason="recv latency mode removed in 0.3.8"))


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("PR231 R2 review is offline")
    for name in ("connect", "connect_ex"):
        monkeypatch.setattr(socket.socket, name, forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)


def local_book(data, ms):
    return next(_replay(data, hb.LOCAL_EVENT, [ms * 10**6]))


def assert_constant(data, latency):
    if latency == "4ms":
        assert np.all(data["local_ts"] - data["exch_ts"] == 4_000_000)


@pytest.mark.parametrize("latency", [RECV, "4ms"])
def test_r2_snapshot_received_before_break_is_delivered(tmp_path, latency):
    t = day_ms(DAY)
    # Two breaks: snapshot L=201 arrives after its last diff, BEFORE the break at 5s.
    write_day(tmp_path, DAY, SYM,
              [ev(t + 1000, 101, 100, 9990, "1"),
               ev(t + 2000, 102, 101, 9991, "2"),
               ev(t + 3000, 201, 199, 8000, "3"),
               ev(t + 4000, 202, 201, 8001, "4"),
               ev(t + 5000, 301, 299, 7000, "5")],
              [snap(t + 1000, 101, t + 1000, [(9990, "1")]),
               snap(t + 3000, 201, t + 4500, [(8000, "3")])])
    result = hb.convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "o", latency=latency)
    data = _load(result)
    assert_constant(data, latency)
    assert result.stats["chain_breaks_cleared"] == 2
    assert local_book(data, t + 4600) == ({8000: 3.0, 8001: 4.0}, {10001: 3.0})
    assert local_book(data, t + 5100) == ({}, {})


@pytest.mark.parametrize("latency", [RECV, "4ms"])
def test_r2_two_pending_snapshots_delivered_in_receive_order(tmp_path, latency):
    t = day_ms(DAY)
    write_day(tmp_path, DAY, SYM,
              [ev(t + 1000, 101, 100, 9990, "1"),
               ev(t + 2000, 102, 101, 9990, "2"),
               ev(t + 3000, 103, 102, 9990, "3")],
              [snap(t + 1000, 101, t + 2800, [(9990, "1")]),
               snap(t + 2000, 102, t + 2200, [(9990, "2")])])
    result = hb.convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "o", latency=latency)
    data = _load(result)
    assert_constant(data, latency)
    assert local_book(data, t + 2300) == ({9990: 2.0}, {10001: 3.0})


@pytest.mark.parametrize("latency", [RECV, "4ms"])
@pytest.mark.parametrize("broken", [False, True])
def test_r2_previous_day_pending_snapshot_cannot_delay_new_anchor(tmp_path, latency, broken):
    t = day_ms(DAY2)
    write_day(tmp_path, DAY, SYM,
              [ev(t - 2000, 101, 100, 9990, "1"), ev(t - 1000, 102, 101, 9991, "2")],
              [snap(t - 2000, 101, t + 800, [(9990, "1")])])
    uid, pu = (201, 199) if broken else (103, 102)
    write_day(tmp_path, DAY2, SYM,
              [ev(t + 100, uid, pu, 8000, "3"), ev(t + 1000, uid + 1, uid, 8001, "4")],
              [snap(t + 100, uid, t + 200, [(8000, "3")], [(10005, "2")])])
    result = hb.convert_l2(tmp_path, SYM, DAY, DAY2, tmp_path / "o", latency=latency)
    data = _load(result)
    assert_constant(data, latency)
    assert local_book(data, t + 300) == ({8000: 3.0}, {10005: 2.0})


@pytest.mark.parametrize("latency", [RECV, "4ms"])
def test_control_r2_very_late_snapshot_after_two_breaks_cannot_restore_old_book(tmp_path, latency):
    t = day_ms(DAY)
    write_day(tmp_path, DAY, SYM,
              [ev(t + 1000, 101, 100, 9990, "1"), ev(t + 2000, 201, 199, 8000, "2"),
               ev(t + 3000, 301, 299, 7000, "3"), ev(t + 5000, 302, 301, 7001, "4")],
              [snap(t + 1000, 101, t + 4500, [(9990, "1")]),
               snap(t + 2000, 201, t + 2000, [(8000, "2")]),
               snap(t + 3000, 301, t + 3200, [(7000, "3")])])
    result = hb.convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "o", latency=latency)
    data = _load(result)
    assert_constant(data, latency)
    assert local_book(data, t + 4600) == ({7000: 3.0}, {10001: 3.0})


@pytest.mark.parametrize("latency", [RECV, "4ms"])
def test_r2_late_refresh_cannot_delay_next_days_chain_clear(tmp_path, latency):
    t = day_ms(DAY2)
    write_day(tmp_path, DAY, SYM,
              [ev(t - 2000, 101, 100, 9990, "1"), ev(t - 1000, 102, 101, 9991, "2")],
              [snap(t - 2000, 101, t - 2000, [(9990, "1")]),
               snap(t - 1000, 102, t + 800, [(9990, "1"), (9991, "2")])])
    write_day(tmp_path, DAY2, SYM, [ev(t + 100, 201, 199, 8000, "3")], [])
    result = hb.convert_l2(tmp_path, SYM, DAY, DAY2, tmp_path / "o", latency=latency)
    data = _load(result)
    assert_constant(data, latency)
    assert result.unanchored_days == [(DAY2, 1, 1)]
    assert local_book(data, t + 300) == ({}, {})


@pytest.mark.parametrize("latency", [RECV, "4ms"])
def test_control_r2_next_day_snapshot_later_than_previous_day_receipt(tmp_path, latency):
    t = day_ms(DAY2)
    write_day(tmp_path, DAY, SYM,
              [ev(t - 1000, 101, 100, 9990, "1")],
              [snap(t - 1000, 101, t + 200, [(9990, "1")])])
    write_day(tmp_path, DAY2, SYM,
              [ev(t + 100, 102, 101, 9991, "2"), ev(t + 1000, 103, 102, 9992, "3")],
              [snap(t + 100, 102, t + 800, [(9990, "1"), (9991, "2")])])
    result = hb.convert_l2(tmp_path, SYM, DAY, DAY2, tmp_path / "o", latency=latency)
    data = _load(result)
    assert_constant(data, latency)
    assert local_book(data, t + 300) == ({9990: 1.0, 9991: 2.0}, {10001: 3.0})
    assert local_book(data, t + 900) == ({9990: 1.0, 9991: 2.0}, {10001: 3.0})


def test_control_r2_failure_after_manifest_delete_before_first_replace(tmp_path, monkeypatch):
    t = day_ms(DAY)
    write_day(tmp_path, DAY, SYM, [ev(t + 1000, 101, 100, 9990, "1")],
              [snap(t + 1000, 101, t + 1000, [(9990, "1")])])
    first = hb.convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "o", latency="4ms")
    before = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in first.files}

    def fail_first(path, data, compress):
        assert not first.summary.exists()
        raise OSError("failure after manifest deletion, before any replacement")

    monkeypatch.setattr(hb, "_save", fail_first)
    with pytest.raises(OSError, match="before any replacement"):
        hb.convert_l2(tmp_path, SYM, DAY, out_dir=first.out_dir, latency="5ms")
    assert not first.summary.exists()
    assert all(hashlib.sha256(p.read_bytes()).hexdigest() == digest for p, digest in before.items())


def test_control_r2_shorter_rerun_excludes_old_extra_files(tmp_path):
    for day, uid in ((DAY, 101), (DAY2, 102)):
        t = day_ms(day)
        write_day(tmp_path, day, SYM, [ev(t + 1000, uid, uid - 1, 9990, "1")],
                  [snap(t + 1000, uid, t + 1000, [(9990, "1")])])
    first = hb.convert_l2(tmp_path, SYM, DAY, DAY2, tmp_path / "o", latency="4ms")
    shorter = hb.convert_l2(tmp_path, SYM, DAY, out_dir=first.out_dir, latency="5ms")
    summary = json.loads(shorter.summary.read_text())
    assert summary["days"] == [DAY]
    assert summary["files"] == [shorter.files[0].name]
    assert len(list(first.out_dir.glob("*.npz"))) == 2  # harmless: the documented loader uses the JSON
    assert DAY2.replace("-", "") not in summary["files"][0]


def test_r2_trade_receive_percentiles_use_observed_not_held_times(tmp_path):
    t = day_ms(DAY)
    write_day(tmp_path, DAY, SYM, [ev(t + 1000, 101, 100, 9990, "1")],
              [snap(t + 1000, 101, t + 1000, [(9990, "1")])])
    _write_trades(tmp_path, DAY, [
        (1, t + 1000, (t + 2000) * 10**6, Decimal("999.9"), Decimal("1"), "buy", "MARKET", False),
        (2, t + 1500, (t + 1501) * 10**6, Decimal("999.9"), Decimal("1"), "sell", "MARKET", False),
    ])
    result = hb.convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "o", latency="4ms", trades=True)
    stats = json.loads(result.summary.read_text())["latency_by_day"][DAY]
    assert stats["recorded_recv_latency_trades_p50_us"] == 500_500  # observed latencies are 1000ms and 1ms
    assert stats["recorded_recv_latency_trades_p99_us"] == 990_010


def test_r2_pause_paragraph_identifies_window_medians_and_sample_size():
    page = " ".join((Path(__file__).resolve().parents[3] / "website/src/pages/docs/hftbacktest.astro").read_text().split())
    # 0.3.8 deletes the trades-during-pauses sentence instead of qualifying its median.
    assert "Trades, captured by a separate" not in page
    assert "within about 3 ms" not in page


@pytest.mark.parametrize("latency", [RECV, "4ms"])
@pytest.mark.parametrize("empty_side", ["bid", "ask"])
def test_control_r2_nan_clear_removes_exactly_the_empty_side(tmp_path, latency, empty_side):
    t = day_ms(DAY)
    changes = [("bid", 9990, Decimal("0" if empty_side == "bid" else "5")),
               ("ask", 10001, Decimal("0" if empty_side == "ask" else "5"))]
    write_day(tmp_path, DAY, SYM,
              [ev(t + 1000, 101, 100, 9990, "1"), (t + 2000, 102, 103, 101, changes)],
              [snap(t + 1000, 101, t + 1000, [(9990, "1")]),
               snap(t + 1500, 102, t + 1800,
                    [] if empty_side == "bid" else [(9990, "1")],
                    [] if empty_side == "ask" else [(10001, "3")])])
    result = hb.convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "o", latency=latency)
    data = _load(result)
    assert_constant(data, latency)
    want = ({}, {10001: 3.0}) if empty_side == "bid" else ({9990: 1.0}, {})
    for flag in (hb.EXCH_EVENT, hb.LOCAL_EVENT):
        assert next(_replay(data, flag, [(t + 1900) * 10**6])) == want


@pytest.mark.parametrize("latency", [RECV, "4ms"])
def test_control_r2_trade_null_filter_and_dictionary_order_types(tmp_path, latency):
    t = day_ms(DAY)
    write_day(tmp_path, DAY, SYM, [ev(t + 1000, 101, 100, 9990, "1")],
              [snap(t + 1000, 101, t + 1000, [(9990, "1")])])
    ddir = _write_trades(tmp_path, DAY, [
        (i + 1, t + 100 + i, (t + 200 + i) * 10**6, Decimal("999.9"), Decimal(i + 1),
         "buy" if i % 2 else "sell", order_type, backfill)
        for i, (order_type, backfill) in enumerate([
            ("MARKET", False), (None, True), (None, False), ("NA", True),
            ("LIMIT", False), ("MARKET", True), ("LIMIT", True)])
    ])
    path = ddir / "trades.parquet"
    table = pq.read_table(path)
    c = "order_type"
    table = table.set_column(table.schema.get_field_index(c), c, table[c].dictionary_encode())
    pq.write_table(table, path)
    _refresh_manifest(ddir)
    result = hb.convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "o", latency=latency, trades=True)
    data = _load(result)
    assert_constant(data, latency)
    trade = data[(data["ev"] & 0xFF) == hb.TRADE_EVENT]
    assert trade["qty"].tolist() == [1.0, 2.0, 6.0]
    assert np.count_nonzero(trade["ev"] & hb.LOCAL_EVENT) == (1 if latency == "recv" else 3)
    assert result.stats["trades_dropped_NA"] == 1
    assert result.stats["trades_dropped_LIMIT"] == 2


@pytest.mark.parametrize("column,values", [
    ("side", pa.array([None], pa.string())),
    ("side", pa.array(["unknown"], pa.string())),
    ("order_type", pa.array([b"MARKET"], pa.binary())),
    ("event_time", pa.array([None], pa.int64())),
    ("amount", pa.array([None], pa.decimal128(38, 12))),
])
def test_control_r2_trade_layout_rejects_bad_required_values(tmp_path, column, values):
    t = day_ms(DAY)
    write_day(tmp_path, DAY, SYM, [ev(t + 1000, 101, 100, 9990, "1")],
              [snap(t + 1000, 101, t + 1000, [(9990, "1")])])
    ddir = _write_trades(tmp_path, DAY, [
        (1, t + 500, (t + 501) * 10**6, Decimal("999.9"), Decimal("1"), "buy", "MARKET", False)])
    path = ddir / "trades.parquet"
    table = pq.read_table(path)
    table = table.set_column(table.schema.get_field_index(column), column, values)
    pq.write_table(table, path)
    _refresh_manifest(ddir)
    with pytest.raises(IncompleteDay, match=column):
        hb.convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "o", latency="4ms", trades=True)
