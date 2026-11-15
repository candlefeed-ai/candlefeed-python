"""Offline round-five review: refusal boundaries, publication state and oracle mutations."""
from __future__ import annotations

import json
import socket
from datetime import date
from pathlib import Path

import numpy as np
import pytest
from l2_synth import DAY_MS, day_ms, write_day
from test_astra_pr231_r4 import trade
from test_hftbacktest import DAY, SYM, _load, _replay, _write_trades
from test_hftbacktest_r1_fix import DAY2, ev, snap

from candlefeed import hftbacktest as hb
from candlefeed.l2book import L2Book


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("PR231 R5 review is offline")

    for name in ("connect", "connect_ex"):
        monkeypatch.setattr(socket.socket, name, forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)


def _book(root, day=DAY, uid=101):
    t = day_ms(day)
    return write_day(root, day, SYM, [ev(t + 1000, uid, uid - 1, 9990, "2")],
                     [snap(t + 999, uid, t + 1001, [(9990, "1")])])


@pytest.mark.parametrize("offset", [-1, 0, DAY_MS - 1, DAY_MS])
@pytest.mark.parametrize("kind,backfill", [("MARKET", False), (None, True), ("NA", False)])
def test_control_r5_exact_raw_trade_day_bounds_before_filter_or_hold(tmp_path, monkeypatch, offset, kind, backfill):
    t = day_ms(DAY)
    # Valid first row would hide the backward invalid second row if a hold ran first.
    rows = [trade(1, t + 1000), trade(2, t + offset, kind=kind, backfill=backfill)]
    ddir = _write_trades(tmp_path, DAY, rows)
    state, stats = hb._State(), {}
    if offset in (-1, DAY_MS):
        def no_hold(*args):
            raise AssertionError("raw day validation must precede any trade normalization")

        monkeypatch.setattr(hb, "_cummax", no_hold)
        with pytest.raises(hb.ConvertError) as info:
            hb._read_trades(ddir / "trades.parquet", date.fromisoformat(DAY), 2, SYM, stats, state)
        msg = str(info.value)
        assert str(ddir / "trades.parquet") in msg
        assert f"for {DAY} UTC" in msg and f"trade 2 has trade time {t + offset} ms" in msg
        assert "1 of its 2 trades" in msg and "Download" in msg
        stamp = "2026-08-31T23:59:59.999+00:00" if offset == -1 else "2026-09-02T00:00:00.000+00:00"
        assert stamp in msg
        assert stats == {} and state.trade_exch == hb._MIN
    else:
        got, _ = hb._read_trades(ddir / "trades.parquet", date.fromisoformat(DAY), 2, SYM, stats, state)
        kept = kind == "MARKET" or backfill
        assert len(got) == 1 + kept
        if kept:
            assert got.exch.tolist() == [rows[0][1] * 10**6, max(rows[0][1], rows[1][1]) * 10**6]
        assert stats["trade_exch_ts_raised"] == int(kept and offset == 0)


@pytest.mark.parametrize("bad_day", [DAY, DAY2])
@pytest.mark.parametrize("rerun", [False, True])
def test_control_r5_partition_refusal_has_no_partial_success(tmp_path, capsys, bad_day, rerun):
    for i, d in enumerate((DAY, DAY2)):
        _book(tmp_path, d, 101 + i)
        _write_trades(tmp_path, d, [trade(i + 1, day_ms(d) + 1000)])
    out = tmp_path / "o"
    old = {}
    if rerun:
        hb.convert_l2(tmp_path, SYM, DAY, DAY2, out, latency="4ms", trades=True)
        old = {p.name: p.read_bytes() for p in out.iterdir()}
    _write_trades(tmp_path, bad_day, [trade(3, day_ms(bad_day) - 1)])
    status = hb.main(["--symbol", SYM, "--start", DAY, "--end", DAY2, "--data-dir", str(tmp_path),
                      "--out", str(out), "--trades", "--latency", "5ms"])
    captured = capsys.readouterr()
    assert status == 2 and "outside that day" in captured.err
    assert "file list:" not in captured.out and "Load in hftbacktest" not in captured.out
    assert not list(out.glob("*.part"))
    if rerun and bad_day == DAY:
        # Existing policy: failure before the first replacement preserves the untouched old dataset.
        assert {p.name: p.read_bytes() for p in out.iterdir()} == old
    else:
        assert not (out / hb.SUMMARY_NAME).exists()
        if bad_day == DAY:
            assert not list(out.glob("*.npz"))
        else:
            assert (out / "BTCUSDT_20260901_00.npz").exists()


@pytest.mark.parametrize("count", [2, 8])
def test_control_r5_snapshot_refs_exceed_event_count_in_later_hour(tmp_path, count):
    t = day_ms(DAY)
    end_u = 102 + count
    ddir = write_day(tmp_path, DAY, SYM,
                     [ev(t + 1000, 101, 100, 9990, "1"), ev(t + 3_601_000, end_u, 101, 9990, "9")],
                     [snap(t + 999, 101, t + 1001, [(9990, "1")])] +
                     [snap(t + 3_600_000 + i, 102 + i, t + 3_600_100 + i, [(9990, str(i + 2))])
                      for i in range(count)])
    book = L2Book([ddir])
    assert len(book._anchors) == count + 1 and book._n == 2
    result = hb.convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "o", latency="4ms")
    assert [p.name for p in result.files] == ["BTCUSDT_20260901_00.npz", "BTCUSDT_20260901_01.npz"]
    assert result.stats["snapshots_refresh"] == 1
    assert list(_replay(_load(result), hb.EXCH_EVENT, [(t + 3_601_000) * 10**6])) == [({9990: 9.0}, {10001: 3.0})]


@pytest.mark.parametrize("mutation", ["snapshot", "diff", "clear"])
def test_control_r5_oracle_rejects_corruption_at_each_distinct_kind(tmp_path, mutation):
    from test_hftbacktest_fuzz import _check_books

    t = day_ms(DAY)
    ddir = write_day(tmp_path, DAY, SYM,
                     [ev(t + 1000, 101, 100, 9990, "2"), ev(t + 2000, 102, 101, 9990, "3"),
                      ev(t + 3000, 201, 199, 9990, "4")],
                     [snap(t + 900, 101, t + 1001, [(9990, "1")])])
    book = L2Book([ddir])
    result = hb.convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "o", latency="4ms")
    data = _load(result)
    tx = {101: t + 999, 102: t + 1999, 201: t + 2999}
    checked, _, times = _check_books(data, 4_000_000, book, tx)
    assert checked == 4 and times == {(t + off) * 10**6 for off in (900, 999, 1999, 2999)}
    kind = data["ev"] & np.uint64(0xFF)
    if mutation == "clear":
        data = data[~((kind == hb.DEPTH_CLEAR_EVENT) & (data["exch_ts"] == (t + 2999) * 10**6))]
    else:
        target = hb.DEPTH_SNAPSHOT_EVENT if mutation == "snapshot" else hb.DEPTH_EVENT
        data["qty"][np.flatnonzero(kind == target)[0]] = 99
    with pytest.raises(AssertionError):
        _check_books(data, 4_000_000, book, tx)


@pytest.mark.parametrize("description", ["json", "readme", "changelog"])
def test_r5_timestamp_description_discloses_snapshot_clamp(tmp_path, description):
    """A forward-timed snapshot is moved earlier without either 'raised' counter increasing."""
    t = day_ms(DAY)
    write_day(tmp_path, DAY, SYM, [ev(t + 1000, 101, 100, 9990, "2")],
              [snap(t + 2000, 101, t + 2001, [(9990, "1")])])
    result = hb.convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "o", latency="4ms")
    data = _load(result)
    snapshot = data[(data["ev"] & np.uint64(0xFF)) == hb.DEPTH_SNAPSHOT_EVENT]
    assert np.all(snapshot["exch_ts"] == (t + 999) * 10**6)
    assert result.stats["depth_exch_ts_raised"] == 0
    if description == "json":
        text = json.loads(result.summary.read_text())["exch_ts"]
    else:
        root = Path(__file__).resolve().parents[1]
        text = (root / ("README.md" if description == "readme" else "CHANGELOG.md")).read_text()
        if description == "readme":
            text = text.split("## hftbacktest:", 1)[1].split("## Error handling", 1)[0]
        else:
            text = text.split("## 0.3.8", 1)[1].split("## 0.3.7", 1)[0]
    # Require disclosure of the demonstrated snapshot-specific exception, without pinning exact prose.
    assert "snapshot" in text.lower() and ("earlier" in text.lower() or "min(" in text.lower()), text


@pytest.mark.parametrize("entry", ["python", "cli"])
def test_r5_outside_day_timestamp_unrepresentable_as_datetime_has_actionable_refusal(tmp_path, capsys, entry):
    """A nanosecond value accidentally stored in the millisecond column is still an int64."""
    t = day_ms(DAY)
    _book(tmp_path)
    row = list(trade(1, t + 1000))
    row[1] *= 10**6
    ddir = _write_trades(tmp_path, DAY, [tuple(row)])
    out = tmp_path / "o"
    if entry == "python":
        with pytest.raises(hb.ConvertError) as info:
            hb.convert_l2(tmp_path, SYM, DAY, out_dir=out, latency="4ms", trades=True)
        msg = str(info.value)
    else:
        assert hb.main(["--symbol", SYM, "--start", DAY, "--data-dir", str(tmp_path), "--out", str(out),
                        "--latency", "4ms", "--trades"]) == 2
        captured = capsys.readouterr()
        assert "file list:" not in captured.out
        msg = captured.err
    assert str(ddir / "trades.parquet") in msg and f"for {DAY} UTC" in msg
    assert f"trade 1 has trade time {row[1]} ms" in msg and "outside that day" in msg
    assert not list(out.iterdir())
