"""Offline PR231 R4 regressions and boundary controls; production sources stay unchanged."""
from __future__ import annotations

import json
import socket
from datetime import date, timedelta
from decimal import Decimal

import numpy as np
import pytest
from l2_synth import DAY_MS, day_ms, write_day
from test_hftbacktest import DAY, SYM, _load, _replay, _rewrite_depth, _set_col, _write_trades
from test_hftbacktest_r1_fix import DAY2, ev, snap

from candlefeed import hftbacktest as hb
from candlefeed.l2book import IncompleteDay, L2Book


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("PR231 R4 review is offline")

    for name in ("connect", "connect_ex"):
        monkeypatch.setattr(socket.socket, name, forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)


def trade(uid, ms, *, side="buy", kind="MARKET", backfill=False):
    # Unique payloads make lost, duplicated, shuffled or corrupted trades observable.
    return (uid, ms, (ms + 1) * 10**6, Decimal("999.0") + Decimal(uid) / 10,
            Decimal(uid) / 100, side, kind, backfill)


def assert_output(result, rows, latency):
    try:
        from hftbacktest.data import validate_event_order
    except ImportError:  # CI's client job doesn't install hftbacktest; the checks below still run
        validate_event_order = None

    data = _load(result)
    if validate_event_order is not None:
        validate_event_order(data)
    for clock in ("exch_ts", "local_ts"):
        assert np.all(np.diff(data[clock]) >= 0)
    assert np.all(data["local_ts"] - data["exch_ts"] == latency)
    assert np.all((data["ev"] & np.uint64(hb.EXCH_EVENT | hb.LOCAL_EVENT)) == hb.EXCH_EVENT | hb.LOCAL_EVENT)
    actual = data[(data["ev"] & np.uint64(0xFF)) == hb.TRADE_EVENT]
    kept = [r for r in rows if r[6] == "MARKET" or (r[6] is None and r[7])]
    assert actual["px"].tolist() == [float(r[3]) for r in kept]
    assert actual["qty"].tolist() == [float(r[4]) for r in kept]
    assert ((actual["ev"] & np.uint64(hb.BUY_EVENT)) != 0).tolist() == [r[5] == "buy" for r in kept]
    assert result.stats["trades"] == len(kept) == len(actual)
    assert result.stats["rows"] == len(data)
    summary = json.loads(result.summary.read_text())
    assert summary["files"] == [p.name for p in result.files]
    return data, actual


@pytest.mark.parametrize("latency", [0, 4_000_000])
@pytest.mark.parametrize("shape", ["cold_days", "broken_days", "anchored_then_tail", "final_unanchored"])
def test_control_r4_boundary_neighbors_and_final_trade_flush(tmp_path, latency, shape):
    days = [(date.fromisoformat(DAY) + timedelta(days=i)).isoformat() for i in range(4)]
    rows, last_u = [], 100
    all_events = 0
    for i, d in enumerate(days):
        t = day_ms(d)
        anchored = ((shape == "cold_days" and i == 3) or
                    (shape == "broken_days" and i in (0, 3)) or
                    (shape == "anchored_then_tail" and i in (0, 3)) or
                    (shape == "final_unanchored" and i == 0))
        u = last_u + 10
        pu = last_u if (i == 0 or shape == "cold_days" or i == 3) else last_u + 2
        events = [ev(t + 1000, u, pu, 9990 + i, str(i + 1))]
        snapshots = [snap(t + 999, u, t + 1001, [(9990 + i, str(i + 1))])] if anchored else []
        if shape == "anchored_then_tail" and i == 0:
            # A live first event, then a break and two unreadable diffs in a later hour.
            events += [ev(t + 3_600_000, u + 10, u + 5, 9980, "4"),
                       ev(t + DAY_MS - 10, u + 11, u + 10, 9981, "5")]
        else:
            events += [ev(t + DAY_MS - 10, u + 1, u, 9980 + i, "2")]
        last_u = events[-1][2]
        all_events += len(events)
        write_day(tmp_path, d, SYM, events, snapshots)
        ts = [t + 500, t + DAY_MS - 20, t + DAY_MS - 1]
        fresh = [trade(3 * i + j + 1, ms, side="buy" if j % 2 else "sell") for j, ms in enumerate(ts)]
        rows.extend(fresh)
        _write_trades(tmp_path, d, fresh)
    result = hb.convert_l2(tmp_path, SYM, days[0], days[-1], tmp_path / "o", latency=latency, trades=True)
    data, trades = assert_output(result, rows, latency)
    assert result.stats["depth_events_in_files"] == all_events
    assert result.stats["depth_events"] + result.stats["depth_events_skipped_unanchored"] == all_events
    assert result.stats["trade_exch_ts_raised"] == 0
    assert trades["exch_ts"].tolist() == [r[1] * 10**6 for r in rows]
    assert trades[-1]["exch_ts"] == (day_ms(days[-1]) + DAY_MS - 1) * 10**6
    assert (np.load(result.files[-1])["data"][-1]["ev"] & np.uint64(0xFF)) == hb.TRADE_EVENT
    if shape == "final_unanchored":
        assert result.stats["chain_breaks_cleared"] == 1
        assert list(_replay(data, hb.EXCH_EVENT, [int(data[-1]["exch_ts"])])) == [({}, {})]
    if shape == "anchored_then_tail":
        day1 = np.concatenate([np.load(p)["data"] for p in result.files if "20260901" in p.name])
        # The middle trade is before the raw final diff, but after the final written depth/clear.
        assert float(rows[1][3]) in day1["px"]
        assert float(rows[2][3]) not in day1["px"]


@pytest.mark.parametrize("latency", [0, 4_000_000])
def test_control_r4_raising_preserves_payload_order_and_counts(tmp_path, latency):
    t = day_ms(DAY2)
    write_day(tmp_path, DAY, SYM, [ev(t - 10, 101, 100, 9990, "1")],
              [snap(t - 11, 101, t - 9, [(9990, "1")])])
    write_day(tmp_path, DAY2, SYM, [ev(t + 100, 102, 101, 9991, "2")], [])
    first = [trade(1, t - 30), trade(2, t - 5, side="sell")]
    second = [trade(3, t + 20), trade(4, t + 15, side="sell", kind=None, backfill=True),
              trade(5, t + 1, kind="NA"), trade(6, t + 10), trade(7, t + 5, side="sell"),
              trade(8, t + 1000, kind=None)]
    _write_trades(tmp_path, DAY, first)
    _write_trades(tmp_path, DAY2, second)
    result = hb.convert_l2(tmp_path, SYM, DAY, DAY2, tmp_path / "o", latency=latency, trades=True)
    _, rows = assert_output(result, first + second, latency)
    assert rows["exch_ts"].tolist() == [(t + n) * 10**6 for n in (-30, -5, 20, 20, 20, 20)]
    assert result.stats["trade_exch_ts_raised"] == 3
    assert result.stats["trades_dropped_NA"] == 1
    assert result.stats["trades_dropped_live_null_order_type"] == 1
    assert result.stats["trades_backfill_null_order_type"] == 1
    stats = json.loads(result.summary.read_text())["latency_by_day"][DAY2]
    assert stats["recorded_recv_latency_trades_p50_us"] == stats["recorded_recv_latency_trades_p99_us"] == 1000


@pytest.mark.parametrize("latency", [0, 4_000_000])
def test_control_r4_guard_allows_equal_timestamps_across_days(tmp_path, latency):
    t = day_ms(DAY2)
    write_day(tmp_path, DAY, SYM, [ev(t - 1, 101, 100, 9990, "1")],
              [snap(t - 2, 101, t - 1, [(9990, "1")])])
    ddir = write_day(tmp_path, DAY2, SYM, [ev(t, 102, 101, 9991, "2")], [])
    _rewrite_depth(ddir, lambda table: _set_col(table, "tx_time", [t - 2] * table.num_rows))
    result = hb.convert_l2(tmp_path, SYM, DAY, DAY2, tmp_path / "o", latency=latency)
    a, b = [np.load(p)["data"] for p in result.files]
    assert a[-1]["exch_ts"] == b[0]["exch_ts"]
    assert a[-1]["local_ts"] == b[0]["local_ts"]
    assert result.summary.exists()


@pytest.mark.parametrize("clock", ["exch_ts", "local_ts"])
@pytest.mark.parametrize("rerun", [False, True])
def test_control_r4_guard_refuses_same_day_file_and_no_completion(tmp_path, monkeypatch, clock, rerun):
    t = day_ms(DAY)
    write_day(tmp_path, DAY, SYM, [ev(t + 1000, 101, 100, 9990, "1"),
                                 ev(t + 3_600_000, 102, 101, 9991, "2")],
              [snap(t + 999, 101, t + 1001, [(9990, "1")])])
    out = tmp_path / "o"
    if rerun:
        hb.convert_l2(tmp_path, SYM, DAY, out_dir=out, latency="4ms")
    real, count = hb._fill, 0

    def backwards(*args, **kwargs):
        nonlocal count
        count += 1
        data = real(*args, **kwargs)
        if count == 2:
            data[clock] -= DAY_MS * 10**6
        return data

    monkeypatch.setattr(hb, "_fill", backwards)
    with pytest.raises(hb.ConvertError, match="BTCUSDT_20260901_01.npz"):
        hb.convert_l2(tmp_path, SYM, DAY, out_dir=out, latency="4ms")
    assert not (out / hb.SUMMARY_NAME).exists()
    assert not (out / (hb.SUMMARY_NAME + ".part")).exists()


def test_r4_multiple_snapshots_before_one_diff_can_convert(tmp_path):
    """Seed 2430 reduced: snapshot references are not indices into the diff-event array."""
    t = day_ms(DAY)
    ddir = write_day(tmp_path, DAY, SYM, [ev(t + 3000, 103, 100, 9990, "3")],
                     [snap(t + 1000, 101, t + 1001, [(9990, "1")]),
                      snap(t + 2000, 102, t + 2001, [(9990, "2")])])
    book = L2Book([ddir])
    assert book._n == 1 and [a.j for a in book._anchors] == [0, 0]
    result = hb.convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "o", latency="4ms")
    assert result.stats["depth_events"] == 1
    assert list(_replay(_load(result), hb.EXCH_EVENT, [(t + 3000) * 10**6])) == [({9990: 3.0}, {10001: 3.0})]


@pytest.mark.parametrize("latency", [0, 4_000_000])
@pytest.mark.parametrize("previous_day_anchored", [False, True])
def test_r4_cross_partition_trade_is_preserved_or_refused(tmp_path, latency, previous_day_anchored):
    """A success claiming raw Binance T cannot silently move a previous-day trade after written depth."""
    t = day_ms(DAY2)
    write_day(tmp_path, DAY, SYM, [ev(t - 10, 101, 100, 9990, "1")],
              [snap(t - 11, 101, t - 9, [(9990, "1")])] if previous_day_anchored else [])
    write_day(tmp_path, DAY2, SYM, [ev(t + 100, 102, 101, 9991, "2")],
              [] if previous_day_anchored else [snap(t + 99, 102, t + 101, [(9991, "2")])])
    first, late = trade(1, t - 30), trade(2, t - 20, side="sell")
    _write_trades(tmp_path, DAY, [first])
    _write_trades(tmp_path, DAY2, [late])
    out = tmp_path / "o"
    try:
        result = hb.convert_l2(tmp_path, SYM, DAY, DAY2, out, latency=latency, trades=True)
    except (hb.ConvertError, IncompleteDay):
        assert not (out / hb.SUMMARY_NAME).exists()
        return
    data = _load(result)
    row = data[(data["ev"] & np.uint64(0xFF)) == hb.TRADE_EVENT][-1]
    assert int(row["exch_ts"]) == late[1] * 10**6, (
        f"published {int(row['exch_ts']) - late[1] * 10**6} ns after Binance T; "
        f"summary still says {json.loads(result.summary.read_text())['exch_ts']!r}"
    )


@pytest.mark.parametrize("latency", [0, 4_000_000])
def test_control_r4_trade_precedes_equal_depth_across_day_boundary(tmp_path, latency):
    """No off-date trades: next-day depth T can legitimately equal a previous-day trade's T."""
    t = day_ms(DAY2)
    write_day(tmp_path, DAY, SYM, [ev(t - 10, 101, 100, 9990, "1")],
              [snap(t - 11, 101, t - 9, [(9990, "1")])])
    ddir = write_day(tmp_path, DAY2, SYM, [ev(t, 102, 101, 9991, "2")], [])
    _rewrite_depth(ddir, lambda table: _set_col(table, "tx_time", [t - 11] * table.num_rows))
    _write_trades(tmp_path, DAY, [trade(1, t - 11)])
    _write_trades(tmp_path, DAY2, [trade(2, t + 10)])
    result = hb.convert_l2(tmp_path, SYM, DAY, DAY2, tmp_path / "o", latency=latency, trades=True)
    data = _load(result)
    same = data[data["exch_ts"] == (t - 11) * 10**6]
    kinds = (same["ev"] & np.uint64(0xFF)).tolist()
    assert kinds.index(hb.TRADE_EVENT) < kinds.index(hb.DEPTH_EVENT)


def test_r4_fuzz_oracle_checks_unique_time_before_later_raised_snapshot(tmp_path):
    """An early timestamp is unambiguous even though a later snapshot's raw T goes backwards."""
    from test_hftbacktest_fuzz import _check_books

    t = day_ms(DAY)
    ddir = write_day(tmp_path, DAY, SYM,
                     [ev(t + 1000, 101, 100, 9990, "1"), ev(t + 1002, 102, 101, 9990, "2"),
                      ev(t + 1003, 103, 102, 9990, "3")],
                     [snap(t + 999, 101, t + 1001, [(9990, "1")]),
                      snap(t + 998, 103, t + 1004, [(9990, "3")])])
    tx = {101: t + 999, 102: t + 1001, 103: t + 998}
    _rewrite_depth(ddir, lambda table: _set_col(table, "tx_time", [tx[u] for u in table["final_update_id"].to_pylist()]))
    result = hb.convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "o", latency=0)
    data = _load(result)
    book = L2Book([ddir])
    _check_books(data, 0, book, tx)  # The unmodified stream is correct.
    # Corrupt only the first diff, at a time strictly before every later event/snapshot unit.
    first = ((data["ev"] & np.uint64(0xFF)) == hb.DEPTH_EVENT) & (data["exch_ts"] == (t + 999) * 10**6)
    assert first.sum() == 1
    data["qty"][first] = 99
    assert list(_replay(data, hb.EXCH_EVENT, [(t + 999) * 10**6])) == [({9990: 99.0}, {10001: 3.0})]
    assert book.book_at(t + 1000).bids.iloc[0]["qty"] == 1
    with pytest.raises(AssertionError):
        _check_books(data, 0, book, tx)
