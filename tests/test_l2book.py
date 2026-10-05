"""candlefeed.l2book against synthetic days whose true book is known after every event."""
from __future__ import annotations

import contextlib
import importlib.util
import io
import random
import re
import subprocess
import sys
from collections import Counter
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from l2_synth import SCALE, classify, day_ms, make_day, ticks, write_day

from candlefeed.l2book import BookUnavailable, IncompleteDay, L2Book

DAY = "2026-09-01"


def _book_dicts(view):
    bids = {int(round(p * 10)): q for p, q in zip(view.bids["price"], view.bids["qty"])}
    asks = {int(round(p * 10)): q for p, q in zip(view.asks["price"], view.asks["qty"])}
    return bids, asks


def _truth_dicts(truth, i):
    k = SCALE // 10
    return ({p // k: q for p, q in truth.bids[i].items()}, {p // k: q for p, q in truth.asks[i].items()})


@pytest.fixture(scope="module")
def broken_day(tmp_path_factory):
    """Full-depth snapshots, two dropped stretches (each with an in_gap snapshot) and a reset."""
    root = tmp_path_factory.mktemp("broken")
    ddir, truth, _ = make_day(root, DAY, seed=7, drops=[(530, 6), (1699, 3)], resets=[1110],
                              moves={k: random.Random(k).choice((-2, -1, 1, 2)) for k in range(0, 2400, 9)})
    return root, ddir, truth


def test_every_event_matches_the_true_book(broken_day):
    root, ddir, truth = broken_day
    book = L2Book([ddir])
    index = {u: i for i, u in enumerate(truth.u)}
    seen = 0
    for view in book.iterate(levels=None):
        i = index[view.update_id]
        assert _book_dicts(view) == _truth_dicts(truth, i), f"event u={view.update_id}"
        seen += 1
    classes = Counter(s["class"] for s in truth.snapshots)
    assert classes["straddle"] >= 15 and classes["clean"] >= 15 and classes["in_gap"] >= 2
    assert seen > 0.9 * len(truth.u)
    assert book.skipped_snapshots == classes["in_gap"]


def test_straddle_anchors_are_used_and_needed(tmp_path):
    """A hand-built straddle: L covers only the event's first row. Applying the whole event is right;
    skipping it would leave the ask at 1000.1 that the event deleted."""
    t0 = day_ms(DAY)
    bids, asks = [(9999, Decimal("2"))], [(10001, Decimal("3")), (10002, Decimal("4"))]
    written = [
        (t0 + 1000, 101, 101, 100, [("bid", 9999, Decimal("2"))]),
        (t0 + 2000, 102, 103, 101, [("bid", 10000, Decimal("5")), ("ask", 10001, Decimal("0"))]),
        (t0 + 3000, 104, 104, 103, [("ask", 10002, Decimal("6"))]),
    ]
    # snapshot taken with L=102: the bid at 1000.0 is in, the ask deletion isn't yet
    snap_bids = [(10000, Decimal("5"))] + bids
    snaps = [("tokyo", 102, t0 + 2000, (t0 + 2000) * 10**6, snap_bids, asks)]
    ddir = write_day(tmp_path, DAY, "BTCUSDT", written, snaps)
    book = L2Book([ddir])
    view = book.book_at(t0 + 2000, levels=None)
    assert view.anchor_class == "straddle" and view.anchor_update_id == 102
    assert view.bids.values.tolist() == [[1000.0, 5.0], [999.9, 2.0]]
    assert view.asks.values.tolist() == [[1000.2, 4.0]]
    assert book.book_at(t0 + 3000).asks.values.tolist() == [[1000.2, 6.0]]
    with pytest.raises(BookUnavailable, match="no usable snapshot"):
        book.book_at(t0 + 1500)


def test_clean_between_events_and_exact_boundary(tmp_path):
    t0 = day_ms(DAY)
    written = [
        (t0 + 1000, 101, 101, 100, [("bid", 9999, Decimal("1"))]),
        (t0 + 2000, 110, 110, 101, [("bid", 9999, Decimal("0")), ("bid", 9998, Decimal("3"))]),
        (t0 + 3000, 111, 111, 110, [("ask", 10001, Decimal("9"))]),
    ]
    between = ("tokyo", 105, t0 + 1500, (t0 + 1500) * 10**6, [(9999, Decimal("1"))], [(10001, Decimal("2"))])
    ddir = write_day(tmp_path, DAY, "BTCUSDT", written, [between])
    view = L2Book([ddir]).book_at(t0 + 2000)
    assert view.anchor_class == "clean"
    assert view.bids.values.tolist() == [[999.8, 3.0]] and view.asks.values.tolist() == [[1000.1, 2.0]]

    exact = ("tokyo", 110, t0 + 2000, (t0 + 2000) * 10**6, [(9998, Decimal("3"))], [(10001, Decimal("2"))])
    ddir = write_day(tmp_path / "b", DAY, "BTCUSDT", written, [exact])
    book = L2Book([ddir])
    assert book.book_at(t0 + 2000).bids.values.tolist() == [[999.8, 3.0]]
    assert book.book_at(t0 + 3000).asks.values.tolist() == [[1000.1, 9.0]]


def test_gap_starts_a_new_segment_and_nothing_is_reported_until_it_is_anchored(broken_day):
    root, ddir, truth = broken_day
    book = L2Book([ddir])
    segs = book.segments()
    assert len(segs) == 4 and segs["starts_with_reset"].tolist() == [False, False, True, False]
    for _, s in segs.iloc[1:].iterrows():
        assert s["anchored_from"] > s["first_event"]
        # between the break and the first snapshot of the new segment there is no book
        with pytest.raises(BookUnavailable, match="no usable snapshot"):
            book.book_at(s["first_event"])
        # inside the hole itself, either
        prev_last = segs.loc[segs["segment"] == s["segment"] - 1, "last_event"].iloc[0]
        if s["first_event"] - prev_last > pd.Timedelta(milliseconds=2):
            with pytest.raises(BookUnavailable, match="break in the update chain"):
                book.book_at(prev_last + pd.Timedelta(milliseconds=1))
        view = book.book_at(s["anchored_from"], levels=None)
        assert view.segment == s["segment"]
        assert _book_dicts(view) == _truth_dicts(truth, truth.u.index(view.update_id))


def test_first_received_copy_wins_over_a_later_node_copy(broken_day):
    root, ddir, truth = broken_day
    book = L2Book([ddir])
    nodes = Counter(a.node for a in book._anchors)
    assert set(nodes) == {"tokyo"}
    import pyarrow.parquet as pq
    assert "singapore" in set(pq.read_table(ddir / "snapshot.parquet", columns=["node"])["node"].to_pylist())


def test_truncated_snapshots_match_the_truth_inside_the_window(tmp_path):
    ddir, truth, _ = make_day(tmp_path, DAY, seed=3, snapshot_depth=25, drops=[(900, 4)],
                              moves={k: random.Random(k).choice((-1, 1)) for k in range(0, 2400, 7)})
    book = L2Book([ddir], snapshot_levels=25)
    index = {u: i for i, u in enumerate(truth.u)}
    checked = 0
    for view in book.iterate(levels=None):
        tb, ta = _truth_dicts(truth, index[view.update_id])
        mb, ma = _book_dicts(view)
        floor = round(view.bid_window_floor * 10)
        ceil = round(view.ask_window_ceiling * 10)
        assert {p: q for p, q in mb.items() if p >= floor} == {p: q for p, q in tb.items() if p >= floor}
        assert {p: q for p, q in ma.items() if p <= ceil} == {p: q for p, q in ta.items() if p <= ceil}
        assert sorted(mb.items(), reverse=True)[:5] == sorted(tb.items(), reverse=True)[:5]
        assert sorted(ma.items())[:5] == sorted(ta.items())[:5]
        checked += 1
    assert checked > 2000


def test_book_carries_across_midnight_only_when_the_chain_continues(tmp_path):
    d1, t1, m = make_day(tmp_path, "2026-09-01", seed=11)
    d2, t2, _ = make_day(tmp_path, "2026-09-02", seed=12, market=m, first_event_ms=500)
    early = "2026-09-02T00:05:00Z"          # before day 2's first snapshot (about 00:12)
    view = L2Book([d1, d2]).book_at(early, levels=None)
    assert _book_dicts(view) == _truth_dicts(t2, t2.u.index(view.update_id))
    assert view.event_time.date().isoformat() == "2026-09-02"
    with pytest.raises(BookUnavailable):
        L2Book([d2]).book_at(early)
    assert len(L2Book.load(tmp_path, "btcusdt", "2026-09-01", "2026-09-02").segments()) == 1

    d3, _, _ = make_day(tmp_path / "x", "2026-09-02", seed=12, market=m, resets=[0])
    with pytest.raises(BookUnavailable):
        L2Book([d1, d3]).book_at(early)


def test_spread_series_and_depth_agree_with_book_at(broken_day):
    root, ddir, truth = broken_day
    book = L2Book([ddir])
    s = book.spread_series("7min", depth_bps=[1, 5])
    assert s.index.tz is not None and s.index[0] == pd.Timestamp(DAY, tz="UTC")
    assert s["bid"].isna().sum() >= 1          # before the first snapshot
    for t, row in s.dropna().iloc[::5].iterrows():
        v = book.book_at(t)
        assert row["bid"] == v.best_bid and row["ask"] == v.best_ask
        assert row["spread_bps"] == pytest.approx(v.spread_bps)
        d = book.depth_at(t, [1, 5])
        assert row["bid_depth_1bps"] == pytest.approx(d.loc[1.0, "bid_qty"])
        assert row["ask_depth_5bps"] == pytest.approx(d.loc[5.0, "ask_qty"])


def test_depth_at_sums_the_true_book(broken_day):
    root, ddir, truth = broken_day
    book = L2Book([ddir])
    t = "2026-09-01T15:00:00Z"
    d = book.depth_at(t, [2, 20])
    view = book.book_at(t, levels=None)
    tb, ta = _truth_dicts(truth, truth.u.index(view.update_id))
    mid = (max(tb) + min(ta)) / 2 / 10
    for bps in (2, 20):
        lo, hi = mid * (1 - bps / 1e4), mid * (1 + bps / 1e4)
        assert d.loc[float(bps), "bid_qty"] == pytest.approx(sum(q for p, q in tb.items() if p / 10 >= lo))
        assert d.loc[float(bps), "ask_qty"] == pytest.approx(sum(q for p, q in ta.items() if p / 10 <= hi))
        assert d.loc[float(bps), "bid_notional"] == pytest.approx(
            sum(p / 10 * q for p, q in tb.items() if p / 10 >= lo))
    assert d["mid"].iloc[0] == pytest.approx(mid)
    assert d["bid_complete"].all()           # synthetic snapshots are shorter than 1,000 levels


def test_iterate_every_matches_book_at(broken_day):
    root, ddir, truth = broken_day
    book = L2Book([ddir])
    views = list(book.iterate("2026-09-01T10:00:00Z", "2026-09-01T11:00:00Z", every="10min", levels=3))
    assert [v.time.minute for v in views] == [0, 10, 20, 30, 40, 50]
    for v in views:
        assert v.bids.equals(book.book_at(v.time, levels=3).bids)
    d = views[0].to_dict()
    assert d["anchor"]["class"] in ("clean", "straddle") and len(d["bids"]) == 3 and d["age_ms"] >= 0


def test_out_of_range_and_bad_input(broken_day, tmp_path):
    root, ddir, truth = broken_day
    book = L2Book([ddir])
    with pytest.raises(ValueError, match="outside the loaded days"):
        book.book_at("2026-09-02T00:00:00Z")
    with pytest.raises(ValueError):
        book.depth_at("2026-09-01T12:00:00Z", bps=0)
    with pytest.raises(FileNotFoundError, match="download_l2"):
        L2Book.load(root, "BTCUSDT", "2026-09-01", "2026-09-02")


REPO = Path(__file__).resolve().parents[3]
REPLAY_DOCS = ["website/candlefeed-l2-docs.md", "website/public/llms-full.txt"]
REPLAY_DAY = '"data/book/binance/BTCUSDT/2026-09-25"'
PRE_PR132 = "b8f649661fdc771e70c16884da82521f6a38a4aa"   # the examples before the chain-break fix


def _replay_example(rel, revision=None):
    """The reference replay as published, read from the doc itself so the test can't drift from it."""
    if revision is None:
        text = (REPO / rel).read_text()
    else:
        try:
            text = subprocess.run(["git", "show", f"{revision}:{rel}"], cwd=REPO, text=True,
                                  capture_output=True, check=True).stdout
        except (OSError, subprocess.CalledProcessError):
            pytest.skip(f"{revision[:7]} isn't in this checkout")
    blocks = [b for b in re.findall(r"```python\n(.*?)```", text, re.S) if "anchors.append" in b]
    assert len(blocks) == 1, rel
    return blocks[0]


def _run_replay(rel, ddir, revision=None):
    code = _replay_example(rel, revision)
    assert code.count(REPLAY_DAY) == 1, rel
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        exec(compile(code.replace(REPLAY_DAY, repr(str(ddir))), rel, "exec"), {})
    return out.getvalue().splitlines()


def _parse_line(line):
    label = line.split(" UTC")[0]
    t = pd.Timestamp(f"{DAY} {label}", tz="UTC")
    if len(label) == 5:     # a bare hh:mm label claims the book at the end of that minute
        t += pd.Timedelta(minutes=1) - pd.Timedelta(milliseconds=1)
    bid = Decimal(line.split("bid ")[1].split()[0])
    ask = Decimal(line.split("ask ")[1].split()[0])
    return t, float(bid), float(ask)


def _assert_lines_match_book_at(lines, book):
    for line in lines:
        t, bid, ask = _parse_line(line)
        # book_at raises BookUnavailable for a moment inside a break, which is the case this guards against
        view = book.book_at(t)
        assert (view.best_bid, view.best_ask) == (bid, ask), line


def test_both_published_replays_are_the_same_code():
    a, b = (_replay_example(rel) for rel in REPLAY_DOCS)
    assert a == b


@pytest.mark.parametrize("rel", REPLAY_DOCS)
def test_published_worked_example_agrees_with_book_at_and_never_reports_inside_a_break(broken_day, rel):
    root, ddir, truth = broken_day
    lines = _run_replay(rel, ddir)
    assert len(lines) >= 22
    book = L2Book([ddir])
    breaks = book.segments()["last_event"].iloc[:-1]
    _assert_lines_match_book_at(lines, book)
    # the 16:00 hour ends inside the drop at event 1699: its line is the book as of the last event before it
    assert any(_parse_line(line)[0] in set(breaks) for line in lines)


def _first_snapshot(t0, bids=((9999, Decimal(1)), (9998, Decimal(1)))):
    return ("tokyo", 101, t0 + 1000, (t0 + 1000) * 10**6, list(bids), [(10001, Decimal(1))])


def _hourly_events(t0, hours=range(24)):
    """One event a second into each listed hour, ids 101, 102, ... in that order."""
    return [(t0 + h * 3_600_000 + 1000, 101 + i, 101 + i, 100 + i, [("ask", 10001, Decimal(1))])
            for i, h in enumerate(hours)]


def _shift(events, by):
    return [(t, U + by, u + by, pu + by, rows) for t, U, u, pu, rows in events]


@pytest.mark.parametrize("rel", REPLAY_DOCS)
def test_original_example_is_rejected_at_chain_break(tmp_path, rel, monkeypatch):
    """Control: the examples as they were before this fix fail the printed-time oracle."""
    old = _replay_example(rel, PRE_PR132)
    ddir, _, _ = make_day(tmp_path, DAY, seed=7, drops=[(530, 6), (1699, 3)], resets=[1110])
    monkeypatch.setattr(sys.modules[__name__], "_replay_example", lambda rel, revision=None: old)
    with pytest.raises(BookUnavailable, match="break in the update chain"):
        test_published_worked_example_agrees_with_book_at_and_never_reports_inside_a_break(
            (tmp_path, ddir, None), rel)


@pytest.mark.parametrize("rel", REPLAY_DOCS)
def test_replay_accepts_hours_the_manifest_omits(tmp_path, rel):
    """A partial day starting at 05:00 and an empty 09:00 hour: neither file exists, and that's fine."""
    t0 = day_ms(DAY)
    hours = [h for h in range(5, 24) if h != 9]
    ddir = write_day(tmp_path, DAY, "BTCUSDT", _hourly_events(t0, hours), [_first_snapshot(t0 + 5 * 3_600_000)])
    assert not (ddir / "depth/00.parquet").exists() and not (ddir / "depth/09.parquet").exists()
    lines = _run_replay(rel, ddir)
    assert [_parse_line(line)[0].hour for line in lines] == hours
    _assert_lines_match_book_at(lines, L2Book([ddir]))


@pytest.mark.parametrize("rel", REPLAY_DOCS)
def test_missing_manifest_listed_hour_is_rejected(tmp_path, rel):
    """Skipping a lost download must not silently carry a stale best bid forward."""
    t0 = day_ms(DAY)
    events = _hourly_events(t0)
    events[1] = (*events[1][:4], [("bid", 9999, Decimal(0))])
    ddir = write_day(tmp_path, DAY, "BTCUSDT", events, [_first_snapshot(t0)])
    assert L2Book([ddir]).book_at(events[2][0]).best_bid == 999.8
    (ddir / "depth/01.parquet").unlink()
    with pytest.raises(IncompleteDay, match="listed in the manifest but missing"):
        L2Book([ddir])
    code = _replay_example(rel).replace(REPLAY_DAY, repr(str(ddir)))
    out = io.StringIO()
    with contextlib.redirect_stdout(out), pytest.raises(FileNotFoundError, match="manifest.json.*01.parquet"):
        exec(compile(code, rel, "exec"), {})
    assert out.getvalue() == ""     # it stops before printing anything, not partway through the day


@pytest.mark.parametrize("rel", REPLAY_DOCS)
def test_backward_event_time_labels_the_book_with_the_latest_time(tmp_path, rel):
    """Ids rise but E goes 1000, 3000, 2000: book_at(2000) hasn't applied the 3000 event yet."""
    t0 = day_ms(DAY)
    events = [
        (t0 + 1000, 101, 101, 100, [("ask", 10001, Decimal(1))]),
        (t0 + 3000, 102, 102, 101, [("bid", 10000, Decimal(1))]),
        (t0 + 2000, 103, 103, 102, [("bid", 10000, Decimal(2))]),
    ] + _shift(_hourly_events(t0)[1:], 2)
    ddir = write_day(tmp_path, DAY, "BTCUSDT", events, [_first_snapshot(t0)])
    lines = _run_replay(rel, ddir)
    assert len(lines) == 24
    assert lines[0].startswith("00:00:03.000 UTC  bid 1000.0")
    _assert_lines_match_book_at(lines, L2Book([ddir]))


@pytest.mark.parametrize("rel", REPLAY_DOCS)
def test_backward_event_time_at_end_of_data_prints_nothing_for_that_hour(tmp_path, rel):
    """The day's last event is earlier than the one before it: book_at has no book at the later time."""
    t0, t23 = day_ms(DAY), day_ms(DAY) + 23 * 3_600_000
    events = _hourly_events(t0, range(23)) + [
        (t23 + 3000, 124, 124, 123, [("bid", 9997, Decimal(1))]),
        (t23 + 2000, 125, 125, 124, [("bid", 10000, Decimal(1))]),
    ]
    ddir = write_day(tmp_path, DAY, "BTCUSDT", events, [_first_snapshot(t0)])
    book = L2Book([ddir])
    with pytest.raises(BookUnavailable, match="after the last event"):
        book.book_at(pd.Timestamp(t23 + 3000, unit="ms", tz="UTC"))
    lines = _run_replay(rel, ddir)
    assert [_parse_line(line)[0].hour for line in lines] == list(range(23))
    _assert_lines_match_book_at(lines, book)


@pytest.mark.parametrize("rel", REPLAY_DOCS)
def test_backward_event_time_before_a_break_prints_nothing_for_that_hour(tmp_path, rel):
    t0, t5 = day_ms(DAY), day_ms(DAY) + 5 * 3_600_000
    events = _hourly_events(t0, range(5)) + [
        (t5 + 3000, 106, 106, 105, [("bid", 9997, Decimal(1))]),
        (t5 + 2000, 107, 107, 106, [("bid", 10000, Decimal(1))]),
    ] + _shift(_hourly_events(t0, range(6, 24)), 1000)      # pu jumps: the chain breaks before 06:00
    ddir = write_day(tmp_path, DAY, "BTCUSDT", events, [_first_snapshot(t0)])
    book = L2Book([ddir])
    with pytest.raises(BookUnavailable, match="break in the update chain"):
        book.book_at(pd.Timestamp(t5 + 3000, unit="ms", tz="UTC"))
    lines = _run_replay(rel, ddir)
    assert [_parse_line(line)[0].hour for line in lines] == list(range(5))
    _assert_lines_match_book_at(lines, book)


@pytest.mark.parametrize("rel", REPLAY_DOCS)
def test_milliseconds_and_last_event_of_each_hour_are_exact(tmp_path, rel):
    t0 = day_ms(DAY)
    events = [(t - 1000 + 3_599_999, U, u, pu, rows) for t, U, u, pu, rows in _hourly_events(t0)]
    ddir = write_day(tmp_path, DAY, "BTCUSDT", events, [_first_snapshot(t0)])
    lines = _run_replay(rel, ddir)
    assert [_parse_line(line)[0].value // 1_000_000 for line in lines] == [e[0] for e in events]
    _assert_lines_match_book_at(lines, L2Book([ddir]))


@pytest.mark.parametrize("rel", REPLAY_DOCS)
def test_exhausted_snapshot_window_is_a_documented_limitation(tmp_path, rel):
    """Every bid in a 1,000-level snapshot is deleted and one appears below it. The true best bid is
    unknown: book_at says so, and the example (which doesn't track the window) must say in its prose
    that it doesn't detect this and point to L2Book."""
    t0 = day_ms(DAY)
    events = _hourly_events(t0)
    events[1] = (*events[1][:4], [("bid", p, Decimal(0)) for p in range(9000, 10000)]
                 + [("bid", 8990, Decimal(1))])
    ddir = write_day(tmp_path, DAY, "BTCUSDT", events,
                     [_first_snapshot(t0, [(p, Decimal(1)) for p in range(9000, 10000)])])
    book = L2Book([ddir])
    assert book.book_at(events[1][0]).best_bid is None
    lines = _run_replay(rel, ddir)
    _assert_lines_match_book_at(lines[:1], book)          # before the window runs out they agree
    text = (REPO / rel).read_text()
    prose = text[:text.index(_replay_example(rel))].rsplit("```", 1)[0][-1500:]
    assert "1,000 levels a side" in prose and "past the edge of that window" in prose
    assert "`candlefeed.l2book.L2Book`" in prose and "None" in prose


@pytest.mark.parametrize("rel", REPLAY_DOCS)
def test_reset_at_hour_boundary_waits_for_straddle_anchor(tmp_path, rel):
    t0 = day_ms(DAY)
    events = [
        (t0 + 1000, 101, 101, 100, [("ask", 10001, Decimal(1))]),
        (t0 + 3_600_000 + 1000, 210, 210, -1, [("bid", 9999, Decimal(0))]),
        (t0 + 7_200_000 + 1000, 211, 212, 210, [("bid", 9998, Decimal(2)), ("ask", 10001, Decimal(0))]),
    ] + _shift(_hourly_events(t0)[3:], 109)
    snapshots = [_first_snapshot(t0),
                 ("tokyo", 205, t0 + 3_600_000, (t0 + 3_600_000) * 10**6,
                  [(9999, Decimal(1))], [(10001, Decimal(1))]),
                 ("tokyo", 211, t0 + 7_200_000 + 1000, (t0 + 7_200_000 + 1000) * 10**6,
                  [(9998, Decimal(2))], [(10001, Decimal(1)), (10002, Decimal(1))])]
    ddir = write_day(tmp_path, DAY, "BTCUSDT", events, snapshots)
    book = L2Book([ddir])
    assert book.skipped_snapshots == 1
    lines = _run_replay(rel, ddir)
    assert len(lines) == 23
    assert not any(line.startswith("01:") for line in lines)
    assert _parse_line(lines[1])[1:] == (999.8, 1000.2)
    _assert_lines_match_book_at(lines, book)


def test_classification_matches_the_reconcile_job(broken_day):
    scripts = Path(__file__).resolve().parents[3] / "scripts"
    if not (scripts / "reconcile_l2.py").exists():
        pytest.skip("not in the monorepo")
    sys.path.insert(0, str(scripts))
    try:
        spec = importlib.util.spec_from_file_location("reconcile_l2_for_test", scripts / "reconcile_l2.py")
        rl2 = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(rl2)
    except ImportError as exc:
        pytest.skip(f"reconcile_l2 dependencies missing: {exc}")
    finally:
        sys.path.remove(str(scripts))
    root, ddir, truth = broken_day
    book = L2Book([ddir])
    rng = random.Random(5)
    probes = [s["L"] for s in truth.snapshots] + [rng.randint(int(book._u[0]) - 50, int(book._u[-1]) + 50)
                                                  for _ in range(3000)]
    for L in probes:
        cls, j, _ = rl2.classify_snapshot(L, book._u, book._U, book._pu)
        mine, mj = book._classify(L)
        assert (mine, mj) == (cls, j), L
        assert mine == classify(L, list(book._u), list(book._U), list(book._pu))


def test_float_prices_are_exact_for_the_decimal_ticks():
    assert ticks(1_092_341) / 10**12 == 109234.1
    assert np.float64(ticks(5)) / 10**12 == 0.5


def test_spread_series_keeps_per_band_completeness(tmp_path):
    """Astra #7: the flags depth_at computes survive into the series."""
    ddir, truth, _ = make_day(tmp_path, DAY, seed=3, snapshot_depth=25)
    book = L2Book([ddir], snapshot_levels=25)
    s = book.spread_series("10min", depth_bps=[0.1, 5])
    have = s[s["mid"].notna()]
    assert have["bid_depth_0.1bps_complete"].all() and not have["bid_depth_5bps_complete"].any()
    assert not s.loc[s["mid"].isna(), "ask_depth_0.1bps_complete"].any()
    t = have.index[3]
    d = book.depth_at(t, [0.1, 5])
    assert bool(d.loc[5.0, "ask_complete"]) is bool(have.loc[t, "ask_depth_5bps_complete"]) is False
