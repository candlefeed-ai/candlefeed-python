"""PR231 R3 fixes: trades held against every diff read, trades filed under the wrong day, the file-order guard."""
from __future__ import annotations

import numpy as np
import pytest
from l2_synth import day_ms, write_day
from test_astra_pr231_r3 import trade
from test_hftbacktest import DAY, SYM, _load, _write_trades
from test_hftbacktest_r1_fix import DAY2, ev, snap

from candlefeed import hftbacktest as hb


def _in_order(data):
    for flag, column in ((hb.EXCH_EVENT, "exch_ts"), (hb.LOCAL_EVENT, "local_ts")):
        values = data[column][(data["ev"] & np.uint64(flag)) != 0]
        assert np.all(np.diff(values) >= 0), column


@pytest.mark.parametrize("latency", ["0ns", "4ms"])
@pytest.mark.parametrize("offset", [-20, 86_400_000])
def test_trade_outside_its_files_day_is_refused(tmp_path, latency, offset):
    """Found by the fuzz test as a trade 20 ms before midnight in the next day's file. The collector files trades
    by trade time, so such a file is malformed: the run stops before any time is held, with no file list."""
    midnight = day_ms(DAY2)
    write_day(tmp_path, DAY, SYM, [ev(midnight - 10, 101, 100, 9990, "1")],
              [snap(midnight - 11, 101, midnight - 9, [(9990, "1")])])
    write_day(tmp_path, DAY2, SYM, [ev(midnight + 1000, 102, 101, 9991, "2")], [])
    _write_trades(tmp_path, DAY, [trade(1, midnight - 500, midnight - 499)])
    _write_trades(tmp_path, DAY2, [trade(2, midnight + 5, midnight + 6),
                                   trade(3, midnight + offset, midnight + offset + 1)])
    out = tmp_path / "o"
    with pytest.raises(hb.ConvertError) as info:
        hb.convert_l2(tmp_path, SYM, DAY, DAY2, out, latency=latency, trades=True)
    msg = str(info.value)
    assert f"trades/binance/{SYM}/{DAY2}/trades.parquet" in msg
    assert f"for {DAY2} UTC" in msg and f"trade 3 has trade time {midnight + offset} ms" in msg
    assert not (out / hb.SUMMARY_NAME).exists()


def test_unanchored_day_holds_trades_after_its_last_diff(tmp_path):
    midnight = day_ms(DAY2)
    write_day(tmp_path, DAY, SYM, [ev(midnight - 1000, 101, 100, 9990, "1")], [])
    write_day(tmp_path, DAY2, SYM, [ev(midnight, 102, 101, 9991, "2")],
              [snap(midnight - 2, 102, midnight + 2, [(9990, "1"), (9991, "2")])])
    _write_trades(tmp_path, DAY, [trade(1, midnight - 2000, midnight - 1999), trade(2, midnight - 1, midnight)])
    _write_trades(tmp_path, DAY2, [trade(3, midnight + 500, midnight + 501)])
    result = hb.convert_l2(tmp_path, SYM, DAY, DAY2, tmp_path / "o", latency="4ms", trades=True)
    assert [p.name for p in result.files] == ["BTCUSDT_20260901_23.npz", "BTCUSDT_20260902_00.npz"]
    first = np.load(result.files[0])["data"]
    assert first["exch_ts"].tolist() == [(midnight - 2000) * 10**6]      # only the trade before the last diff
    _in_order(_load(result))


def _two_days(root):
    t1, t2 = day_ms(DAY), day_ms(DAY2)
    write_day(root, DAY, SYM, [ev(t1 + 1000, 101, 100, 9990, "1")], [snap(t1 + 1000, 101, t1 + 1000, [(9990, "1")])])
    write_day(root, DAY2, SYM, [ev(t2 + 1000, 102, 101, 9991, "2")], [])


@pytest.mark.parametrize("clock", ["exch_ts", "local_ts"])
def test_guard_refuses_a_file_that_starts_before_the_previous_one(tmp_path, monkeypatch, clock):
    _two_days(tmp_path)
    out = tmp_path / "o"
    hb.convert_l2(tmp_path, SYM, DAY, DAY2, out, latency="4ms")
    assert (out / hb.SUMMARY_NAME).exists()
    real = hb._fill
    calls = []

    def back_in_time(*args, **kwargs):
        data = real(*args, **kwargs)
        calls.append(data)
        if len(calls) == 2:
            data[clock] -= 2 * 86_400 * 10**9
        return data

    monkeypatch.setattr(hb, "_fill", back_in_time)
    with pytest.raises(hb.ConvertError) as exc:
        hb.convert_l2(tmp_path, SYM, DAY, DAY2, out, latency="4ms")
    msg = str(exc.value)
    first_last = calls[0][-1]
    second_first = calls[1][0]
    assert "BTCUSDT_20260902_00.npz" in msg
    assert f"exch_ts {second_first['exch_ts']} / local_ts {second_first['local_ts']}" in msg
    assert f"exch_ts {first_last['exch_ts']} / local_ts {first_last['local_ts']}" in msg
    assert not (out / hb.SUMMARY_NAME).exists()           # the old list went with the first replaced file
