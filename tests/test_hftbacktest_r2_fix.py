"""0.3.8 offers constant latency only: recv is refused, and every row has local_ts == exch_ts + latency."""
from __future__ import annotations

import json
from decimal import Decimal

import numpy as np
import pyarrow.parquet as pq
import pytest
from l2_synth import day_ms, write_day
from test_hftbacktest import DAY, SYM, _load, _refresh_manifest, _replay, _set_col, _write_trades
from test_hftbacktest_r1_fix import DAY2, ev, snap

from candlefeed import hftbacktest as hb

MESSAGE = ("receive-time latency ('recv') isn't offered in this version, because CandleFeed's recorded receive "
           "times include capture-node pauses. Pass a constant such as 4ms.")


def _one_day(tmp_path):
    t = day_ms(DAY)
    write_day(tmp_path, DAY, SYM, [ev(t + 1000, 101, 100, 9990, "1")], [snap(t + 1000, 101, t + 1000, [(9990, "1")])])


@pytest.mark.parametrize("value", ["recv", "RECV", " recv "])
def test_python_refuses_recv(tmp_path, value):
    _one_day(tmp_path)
    assert hb.RECV_REFUSED == MESSAGE
    with pytest.raises(hb.ConvertError) as exc:
        hb.convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "o", latency=value)
    assert str(exc.value) == MESSAGE
    assert not (tmp_path / "o").exists()


def test_cli_refuses_recv_as_a_usage_error(tmp_path, capsys):
    _one_day(tmp_path)
    with pytest.raises(SystemExit) as exc:
        hb.main(["--symbol", SYM, "--start", DAY, "--data-dir", str(tmp_path), "--out", str(tmp_path / "o"),
                 "--latency", "recv"])
    assert exc.value.code == 2
    err = " ".join(capsys.readouterr().err.split())
    assert "usage:" in err and f"argument --latency: {MESSAGE}" in err
    assert not (tmp_path / "o").exists()


def _assert_constant(result, latency_ns):
    data = _load(result)
    assert len(data)
    both = np.uint64(hb.EXCH_EVENT | hb.LOCAL_EVENT)
    assert np.all((data["ev"] & both) == both)
    assert np.all(data["local_ts"] - data["exch_ts"] == latency_ns)
    assert np.all(np.diff(data["exch_ts"]) >= 0)
    return data


def test_snapshots_received_late_sit_at_their_exchange_time(tmp_path):
    t = day_ms(DAY)
    write_day(tmp_path, DAY, SYM,
              [ev(t + 1000, 101, 100, 9990, "1"), ev(t + 2000, 102, 101, 9991, "2"), ev(t + 3000, 103, 102, 9992, "3"),
               ev(t + 4000, 201, 199, 8000, "4"), ev(t + 5000, 202, 201, 8001, "5")],
              [snap(t + 1000, 101, t + 2800, [(9990, "1")]),
               snap(t + 2000, 102, t + 2200, [(9990, "1"), (9991, "2")]),
               snap(t + 4000, 201, t + 9000, [(8000, "4")])])
    result = hb.convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "o", latency="4ms")
    data = _assert_constant(result, 4_000_000)
    snaps = data[(data["ev"] & np.uint64(0xFF)) == hb.DEPTH_SNAPSHOT_EVENT]
    assert sorted(set((snaps["exch_ts"] // 10**6).tolist())) == [t + 999, t + 1999, t + 3999]
    for flag in (hb.EXCH_EVENT, hb.LOCAL_EVENT):
        shift = 0 if flag == hb.EXCH_EVENT else 4
        books = list(_replay(data, flag, [(t + ms + shift) * 10**6 for ms in (1500, 3500, 4500)]))
        assert books == [({9990: 1.0}, {10001: 3.0}),
                         ({9990: 1.0, 9991: 2.0, 9992: 3.0}, {10001: 3.0}),
                         ({8000: 4.0}, {10001: 3.0})]


def test_backfilled_trades_go_to_both_sides_at_trade_time(tmp_path):
    _one_day(tmp_path)
    t = day_ms(DAY)
    _write_trades(tmp_path, DAY, [
        (1, t + 500, (t + 501) * 10**6, Decimal("999.9"), Decimal("1"), "buy", "MARKET", False),
        (2, t + 600, (t + 60_000) * 10**6, Decimal("999.9"), Decimal("2"), "sell", None, True),
        (3, t + 700, (t + 650) * 10**6, Decimal("999.9"), Decimal("3"), "buy", "MARKET", False),
    ])
    result = hb.convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "o", latency="4ms", trades=True)
    data = _assert_constant(result, 4_000_000)
    trades = data[(data["ev"] & np.uint64(0xFF)) == hb.TRADE_EVENT]
    assert trades["qty"].tolist() == [1.0, 2.0, 3.0]
    assert (trades["exch_ts"] // 10**6).tolist() == [t + 500, t + 600, t + 700]


@pytest.mark.parametrize("continuous", [True, False])
def test_no_anchor_day_rows_are_exch_plus_latency(tmp_path, continuous):
    t = day_ms(DAY2)
    write_day(tmp_path, DAY, SYM, [ev(t - 2000, 101, 100, 9990, "1"), ev(t - 1000, 102, 101, 9991, "2")],
              [snap(t - 2000, 101, t + 800, [(9990, "1")])])
    uid, pu = (103, 102) if continuous else (201, 199)
    write_day(tmp_path, DAY2, SYM, [ev(t + 100, uid, pu, 8000, "3")], [])
    _write_trades(tmp_path, DAY, [(1, t - 500, (t + 300) * 10**6, Decimal("999.9"), Decimal("1"), "buy", "MARKET",
                                   False)])
    _write_trades(tmp_path, DAY2, [(2, t + 200, (t + 205) * 10**6, Decimal("999.9"), Decimal("2"), "sell", "MARKET",
                                    False)])
    result = hb.convert_l2(tmp_path, SYM, DAY, DAY2, tmp_path / "o", latency="7ms", trades=True)
    data = _assert_constant(result, 7_000_000)
    assert np.count_nonzero((data["ev"] & np.uint64(0xFF)) == hb.TRADE_EVENT) == 2
    book = next(_replay(data, hb.LOCAL_EVENT, [(t + 300) * 10**6]))
    if continuous:
        assert result.unanchored_days == []
        assert book == ({9990: 1.0, 9991: 2.0, 8000: 3.0}, {10001: 3.0})
    else:
        assert result.unanchored_days == [(DAY2, 1, 1)]
        assert book == ({}, {})


def test_recorded_depth_latency_uses_original_receive_times(tmp_path):
    t = day_ms(DAY)
    ddir = write_day(tmp_path, DAY, SYM, [ev(t + 1000, 101, 100, 9990, "1"), ev(t + 1500, 102, 101, 9991, "2")],
                     [snap(t + 1000, 101, t + 1000, [(9990, "1")])])
    for f in (ddir / "depth").glob("*.parquet"):
        table = pq.read_table(f)
        tx = table["tx_time"].to_numpy()
        # 1000 ms late, then 1 ms late: the second receive time is earlier than the first
        recv = tx * 10**6 + np.where(table["final_update_id"].to_numpy() == 101, 1_000_000_000, 1_000_000)
        pq.write_table(_set_col(table, "recv_time", recv), f)
    _refresh_manifest(ddir)
    result = hb.convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "o", latency="4ms")
    _assert_constant(result, 4_000_000)
    stats = json.loads(result.summary.read_text())["latency_by_day"][DAY]
    assert stats["recorded_recv_latency_depth_p50_us"] == 500_500
    assert stats["recorded_recv_latency_depth_p99_us"] == 990_010
    assert "recorded_recv_latency_trades_p50_us" not in stats
