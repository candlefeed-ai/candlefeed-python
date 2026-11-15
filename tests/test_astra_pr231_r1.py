"""Offline adversarial PR231 R1 regressions; assertions describe the desired behavior."""
from __future__ import annotations

import gc
import hashlib
import os
import re
import socket
import weakref
from decimal import Decimal
from pathlib import Path

import numpy as np
import pytest
from l2_synth import day_ms, write_day
from test_hftbacktest import DAY, SYM, _load, _refresh_manifest, _replay, _write_trades

from candlefeed import hftbacktest as hb

RECV_REMOVED = pytest.mark.skip(reason="recv latency mode removed in 0.3.8")


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("PR231 review is offline")
    for name in ("connect", "connect_ex"):
        monkeypatch.setattr(socket.socket, name, forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)


def event(ms, u, pu, qty="2"):
    return (ms, u, u, pu, [("bid", 9999, Decimal(qty))])


def snap(ms, L, recv_ms=None, bids=None, asks=None):
    return ("tokyo", L, ms, (ms if recv_ms is None else recv_ms) * 10**6,
            [(9999, Decimal("1"))] if bids is None else bids,
            [(10001, Decimal("3"))] if asks is None else asks)


@RECV_REMOVED
def test_r1_late_snapshot_must_not_resurrect_a_broken_segment(tmp_path):
    t = day_ms(DAY)
    # The snapshot's response arrives AFTER the collector has observed a chain break.
    write_day(tmp_path, DAY, SYM,
              [event(t + 1000, 101, 100), event(t + 2000, 102, 101),
               event(t + 3000, 201, 199), event(t + 5000, 202, 201)],
              [snap(t + 999, 101, recv_ms=t + 4000, bids=[(9999, Decimal("2"))])])
    result = hb.convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "out", latency="recv")
    data = _load(result)
    assert result.stats["chain_breaks_cleared"] == 1
    assert list(_replay(data, hb.EXCH_EVENT, [(t + 4500) * 10**6])) == [({}, {})]
    assert list(_replay(data, hb.LOCAL_EVENT, [(t + 4500) * 10**6])) == [({}, {})]


@RECV_REMOVED
def test_r1_newer_snapshot_received_first_must_release_live_diffs(tmp_path):
    t = day_ms(DAY)
    write_day(tmp_path, DAY, SYM,
              [event(t + 1000, 101, 100),
               (t + 2000, 102, 103, 101, [("bid", 9999, Decimal("5"))]),
               event(t + 3000, 104, 103)],
              [snap(t + 999, 101, recv_ms=t + 2500, bids=[(9999, Decimal("2"))]), snap(t + 1500, 102)])
    result = hb.convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "out", latency="recv")
    # L=102 arrived at 1.5s and anchored the local book before the 2s diff.
    (bids, _), = _replay(_load(result), hb.LOCAL_EVENT, [(t + 2100) * 10**6])
    assert bids == {9999: 5.0}, "the pending older snapshot must not suppress a live diff"


def test_r1_valid_day_without_any_usable_anchor_has_controlled_result(tmp_path):
    t = day_ms(DAY)
    write_day(tmp_path, DAY, SYM, [event(t + 1000, 101, 100)], [])
    # Either a deliberate actionable refusal or a successful empty file list is valid.
    try:
        result = hb.convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "out", latency="4ms")
    except hb.ConvertError as exc:
        assert "anchor" in str(exc).lower() or "snapshot" in str(exc).lower()
    else:
        assert result.files == []
        assert result.stats["depth_events_skipped_unanchored"] == 1


def test_r1_snapshot_with_empty_side_clears_that_side(tmp_path):
    t = day_ms(DAY)
    # Update 102 removes the last ask; 103 changes a bid. The REST snapshot is taken
    # between these updates, before the straddling diff is published at 2s.
    write_day(tmp_path, DAY, SYM,
              [event(t + 1000, 101, 100),
               (t + 2000, 102, 103, 101, [("ask", 10001, Decimal("0")), ("bid", 9999, Decimal("5"))])],
              [snap(t + 999, 101, bids=[(9999, Decimal("2"))]),
               snap(t + 1500, 102, bids=[(9999, Decimal("2"))], asks=[])])
    result = hb.convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "out", latency="4ms")
    for flag in (hb.EXCH_EVENT, hb.LOCAL_EVENT):
        (_, asks), = _replay(_load(result), flag, [(t + 1600) * 10**6])
        assert asks == {}, "a complete snapshot with zero asks must remove the old ask"


def test_r1_failed_rerun_cannot_leave_success_manifest_pointing_at_mixed_files(tmp_path, monkeypatch):
    t = day_ms(DAY)
    write_day(tmp_path, DAY, SYM,
              [event(t + 1000, 101, 100), event(t + 3_601_000, 102, 101)],
              [snap(t + 999, 101)])
    result = hb.convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "out", latency="4ms")
    before = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in result.files}
    save = hb._save
    calls = 0

    def fail_second_file(path, data, compress):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("injected disk full on second hour")
        return save(path, data, compress)

    monkeypatch.setattr(hb, "_save", fail_second_file)
    with pytest.raises(OSError, match="injected disk full"):
        hb.convert_l2(tmp_path, SYM, DAY, out_dir=result.out_dir, latency="5ms")
    # Preserve the old generation, or invalidate its completion manifest before replacing any data.
    assert (not result.summary.exists()
            or all(hashlib.sha256(p.read_bytes()).hexdigest() == digest for p, digest in before.items())), (
                "the old JSON still claims 4ms although hour 00 is now 5ms and hour 01 is 4ms")


def test_r1_latency_samples_from_finished_days_are_released(tmp_path, monkeypatch):
    for day, uid in ((DAY, 101), ("2026-09-02", 102), ("2026-09-03", 103)):
        t = day_ms(day)
        write_day(tmp_path, day, SYM, [event(t + 1000, uid, uid - 1)], [snap(t + 999, uid)])
    retained = []
    observations = []
    original = hb._event_times

    class TrackedReceiveTimes(np.ndarray):
        def __sub__(self, other):
            result = np.asarray(self) - other
            retained.append(weakref.ref(result))
            return result

    def tracked_event_times(book, stats):
        gc.collect()
        observations.append(sum(ref() is not None for ref in retained))
        T, R = original(book, stats)
        return T, R.view(TrackedReceiveTimes)

    monkeypatch.setattr(hb, "_event_times", tracked_event_times)
    hb.convert_l2(tmp_path, SYM, DAY, "2026-09-03", tmp_path / "out", latency="4ms")
    assert observations == [0, 0, 0], f"finished days' full latency arrays remain resident: {observations}"


def test_r1_documented_last_hour_count_matches_recorded_run():
    root = Path(__file__).resolve().parents[3]
    log_path = os.environ.get("CANDLEFEED_HFTBT_E2E_LOG")
    if not log_path or not Path(log_path).exists():
        pytest.skip("set CANDLEFEED_HFTBT_E2E_LOG to the recorded convert-4ms.log")
    log = Path(log_path)
    pattern = r"BTCUSDT_20261001_23\.npz: ([\d,]+) rows"
    page = (root / "website/src/pages/docs/hftbacktest.astro").read_text()
    assert re.search(pattern, page)[1] == re.search(pattern, log.read_text())[1]


def test_r1_documented_machine_matches_evidence():
    root = Path(__file__).resolve().parents[3]
    evidence = root / "E2E_EVIDENCE.md"
    if not evidence.exists():
        pytest.skip("requires the supplied uncommitted E2E evidence")
    pattern = r"Mac (?:mini|Studio)"
    page = (root / "website/src/pages/docs/hftbacktest.astro").read_text()
    assert re.search(pattern, page)[0] == re.search(pattern, evidence.read_text())[0]


def test_r1_crosschecks_describe_the_selected_local_clock():
    root = Path(__file__).resolve().parents[3]
    page = (root / "website/src/pages/docs/hftbacktest.astro").read_text()
    # In the two constant-latency runs crosscheck.py steps to T + latency, not recv_time.
    assert "to the receive time of the first diff" not in " ".join(page.split())


def test_r1_unknown_backfill_column_layout_is_refused(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from candlefeed.l2book import IncompleteDay

    t = day_ms(DAY)
    write_day(tmp_path, DAY, SYM, [event(t + 1000, 101, 100)], [snap(t + 999, 101)])
    ddir = _write_trades(tmp_path, DAY,
                        [(1, t + 1000, (t + 1001) * 10**6, Decimal("999.9"), Decimal("1"),
                          "buy", "MARKET", False)])
    path = ddir / "trades.parquet"
    table = pq.read_table(path)
    table = table.set_column(table.schema.get_field_index("is_backfill"), "is_backfill", pa.array(["false"]))
    pq.write_table(table, path)
    _refresh_manifest(ddir)
    with pytest.raises((IncompleteDay, hb.ConvertError), match="is_backfill|layout|type"):
        hb.convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "out", latency="4ms", trades=True)


def test_r1_market_only_does_not_admit_live_trade_with_unknown_order_type(tmp_path):
    t = day_ms(DAY)
    write_day(tmp_path, DAY, SYM, [event(t + 1000, 101, 100)], [snap(t + 999, 101)])
    _write_trades(tmp_path, DAY,
                  [(1, t + 1000, (t + 1001) * 10**6, Decimal("999.9"), Decimal("1"),
                    "buy", None, False)])
    result = hb.convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "out", latency="4ms", trades=True)
    rows = _load(result)
    assert not np.any((rows["ev"] & 0xFF) == hb.TRADE_EVENT), "unknown live order type is not MARKET"


@pytest.mark.parametrize("unsupported", [
    "Those aren't network delays",
    "a quote can't be moved or replaced while the node is paused",
])
def test_r1_page_limits_pause_explanation_to_observed_facts(unsupported):
    # Two independent streams' receive stamps do not rule out a network-path stall.
    # The page's fixed-order-latency engine also accepts submit/cancel/replace during feed gaps;
    # The reviewer's engine run with a four-second feed gap confirmed this.
    root = Path(__file__).resolve().parents[3]
    page = (root / "website/src/pages/docs/hftbacktest.astro").read_text()
    assert unsupported not in " ".join(page.split())


@pytest.mark.parametrize("latency", [pytest.param("recv", marks=RECV_REMOVED), "4ms"])
@pytest.mark.parametrize("continuous", [True, False])
def test_control_r1_midnight_with_carried_trades_and_no_second_day_snapshot(tmp_path, latency, continuous):
    t = day_ms("2026-09-02")
    write_day(tmp_path, DAY, SYM, [event(t - 100, 101, 100)], [snap(t - 101, 101)])
    write_day(tmp_path, "2026-09-02", SYM,
              [event(t + 1, 102, 101 if continuous else -1), event(t + 500, 103, 102)], [])
    _write_trades(tmp_path, DAY, [(1, t - 50, (t + 200) * 10**6, Decimal("1000"), Decimal("1"),
                                 "buy", "MARKET", False)])
    _write_trades(tmp_path, "2026-09-02",
                  [(2, t + 100, (t + 110) * 10**6, Decimal("1000"), Decimal("1"),
                    "sell", "MARKET", False)])
    result = hb.convert_l2(tmp_path, SYM, DAY, "2026-09-02", tmp_path / "out", latency=latency, trades=True)
    data = _load(result)
    for flag, column in ((hb.EXCH_EVENT, "exch_ts"), (hb.LOCAL_EVENT, "local_ts")):
        rows = data[(data["ev"] & flag) != 0]
        assert np.all(np.diff(rows[column]) >= 0)
        assert np.count_nonzero((rows["ev"] & 0xFF) == hb.TRADE_EVENT) == 2
        book, = _replay(data, flag, [(t + 1000) * 10**6])
        assert bool(book[0]) == continuous and bool(book[1]) == continuous
    if latency == "4ms":
        assert np.all(data["local_ts"] - data["exch_ts"] == 4_000_000)
    else:
        assert result.stats["trade_local_ts_raised"] == 1


def test_control_r1_download_examples_bind_to_actual_client_signatures():
    import inspect

    from candlefeed import CandleFeed

    sample = inspect.signature(CandleFeed.download_l2_sample).bind(None, "BTCUSDT", "2026-10-01", "data/")
    assert sample.arguments["dest_dir"] == "data/"
    trade = inspect.signature(CandleFeed.download_l2).bind(None, "trades", "BTCUSDT", "2026-10-01", None, "data/")
    assert trade.arguments["end"] is None and trade.arguments["dest_dir"] == "data/"
