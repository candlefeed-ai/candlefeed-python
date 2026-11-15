"""Offline PR231 R3 regressions and raw-latency controls; no source changes."""
from __future__ import annotations

import json
import socket
from decimal import Decimal

import numpy as np
import pytest
from l2_synth import day_ms, write_day
from test_hftbacktest import DAY, SYM, _load, _rewrite_depth, _set_col, _write_trades
from test_hftbacktest_r1_fix import DAY2, ev, snap

from candlefeed import hftbacktest as hb


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("PR231 R3 review is offline")

    for name in ("connect", "connect_ex"):
        monkeypatch.setattr(socket.socket, name, forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)


def trade(uid, event_ms, recv_ms, *, kind="MARKET", backfill=False):
    return (uid, event_ms, recv_ms * 10**6, Decimal("999.9"), Decimal("1"),
            "buy", kind, backfill)


@pytest.mark.parametrize("latency", ["0ns", "4ms"])
def test_r3_unanchored_day_trades_do_not_precede_earlier_next_day_depth(tmp_path, latency):
    """E partitions the depth day; T may be a few ms before midnight without any clock reversal."""
    midnight = day_ms(DAY2)
    write_day(tmp_path, DAY, SYM, [ev(midnight - 1000, 101, 100, 9990, "1")], [])
    next_day = write_day(
        tmp_path, DAY2, SYM, [ev(midnight, 102, 101, 9991, "2")],
        [snap(midnight + 1, 102, midnight + 2, [(9990, "1"), (9991, "2")])],
    )
    # This is an ordinary 2 ms E-T delay. T increases from the previous day's last diff.
    _rewrite_depth(next_day, lambda table: _set_col(table, "tx_time", [midnight - 2] * table.num_rows))
    _write_trades(tmp_path, DAY, [trade(1, midnight - 1, midnight)])
    _write_trades(tmp_path, DAY2, [trade(2, midnight + 20, midnight + 21)])

    result = hb.convert_l2(tmp_path, SYM, DAY, DAY2, tmp_path / "o", latency=latency, trades=True)
    data = _load(result)  # Concatenate in the published completion manifest's file order.
    assert result.unanchored_days[0][:2] == (DAY, 1)
    assert np.count_nonzero((data["ev"] & np.uint64(0xFF)) == hb.TRADE_EVENT) == 2
    assert np.all(data["local_ts"] - data["exch_ts"] == hb.parse_latency(latency))
    for flag, column in ((hb.EXCH_EVENT, "exch_ts"), (hb.LOCAL_EVENT, "local_ts")):
        values = data[column][(data["ev"] & np.uint64(flag)) != 0]
        assert np.all(np.diff(values) >= 0), (
            f"{column} goes backwards across output files: "
            f"{(values - midnight * 10**6).tolist()} ns relative to midnight"
        )


def test_control_r3_trade_receive_stats_precede_exchange_holds_and_filter_backfills(tmp_path):
    t = day_ms(DAY)
    write_day(tmp_path, DAY, SYM, [ev(t + 1000, 101, 100, 9990, "1")],
              [snap(t + 1000, 101, t + 1001, [(9990, "1")])])
    _write_trades(tmp_path, DAY, [
        trade(1, t + 1000, t + 2000),       # 1000 ms recorded
        trade(2, t + 900, t + 901),         # 1 ms recorded, but exchange time is held at +1000
        trade(3, t + 1200, t + 60_000, kind=None, backfill=True),
        trade(4, t + 1300, t + 70_000, kind="NA"),
    ])
    result = hb.convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "o", latency="4ms", trades=True)
    stats = json.loads(result.summary.read_text())["latency_by_day"][DAY]
    assert result.stats["trade_exch_ts_raised"] == 1
    assert stats["recorded_recv_latency_trades_p50_us"] == 500_500
    assert stats["recorded_recv_latency_trades_p99_us"] == 990_010


def test_control_r3_depth_receive_stats_precede_exchange_holds(tmp_path):
    t = day_ms(DAY)
    ddir = write_day(tmp_path, DAY, SYM,
                     [ev(t + 1000, 101, 100, 9990, "1"), ev(t + 1500, 102, 101, 9991, "2")],
                     [snap(t + 1000, 101, t + 1001, [(9990, "1")])])

    def skew(table):
        first = table["final_update_id"].to_numpy() == 101
        tx = np.where(first, t + 1000, t + 900)
        recv = (tx + np.where(first, 1000, 1)) * 10**6
        return _set_col(_set_col(table, "tx_time", tx), "recv_time", recv)

    _rewrite_depth(ddir, skew)
    result = hb.convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "o", latency="4ms")
    stats = json.loads(result.summary.read_text())["latency_by_day"][DAY]
    assert result.stats["depth_exch_ts_raised"] == 1
    assert stats["recorded_recv_latency_depth_p50_us"] == 500_500
    assert stats["recorded_recv_latency_depth_p99_us"] == 990_010
