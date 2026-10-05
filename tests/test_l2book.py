"""candlefeed.l2book against synthetic days whose true book is known after every event."""
from __future__ import annotations

import contextlib
import importlib.util
import io
import random
import sys
from collections import Counter
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from l2_synth import SCALE, classify, day_ms, make_day, ticks, write_day

from candlefeed.l2book import BookUnavailable, L2Book

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


# The worked example from the published docs (website/candlefeed-l2-docs.md, "Worked example"),
# unchanged except for DAY. Its hourly output must match book_at at each hour's end.
DOCS_EXAMPLE = '''
import pyarrow.parquet as pq

DAY = "data/book/binance/BTCUSDT/2026-09-25"   # the folder download_l2 wrote for that day

# Snapshots are one row per price level. Group them into books by lastUpdateId,
# skipping any that landed inside a gap.
snap = pq.read_table(f"{DAY}/snapshot.parquet").to_pandas()
snap = snap[snap["anchor_class"] != "in_gap"]
anchors = []
for last_update_id, rows in snap.groupby("final_update_id", sort=True):
    rows = rows[rows["recv_time"] == rows["recv_time"].min()]  # one node's copy
    book = {"bid": {}, "ask": {}}
    for side, price, qty in zip(rows["side"], rows["price"], rows["qty"]):
        book[side][price] = qty
    anchors.append((last_update_id, book))

book = None      # no usable book until a snapshot anchors it
segment = None   # segment of the previous event
last_u = -1      # u of the previous event
k = 0            # next snapshot to try
cols = ["final_update_id", "segment", "side", "price", "qty"]

for hour in range(24):
    f = pq.ParquetFile(f"{DAY}/depth/{hour:02d}.parquet")
    for batch in f.iter_batches(batch_size=500_000, columns=cols):
        for u, seg, side, price, qty in zip(*(c.to_pylist() for c in batch.columns)):
            if u != last_u:                 # first row of a new event
                if seg != segment:          # chain broke: the old book is stale
                    book, segment = None, seg
                if book is None:
                    while k < len(anchors) and anchors[k][0] <= last_u:
                        k += 1
                    # This is the first event with u >= lastUpdateId. If the
                    # snapshot's id falls inside this event, still apply it whole.
                    if k < len(anchors) and anchors[k][0] <= u:
                        book = {s: dict(lv) for s, lv in anchors[k][1].items()}
                last_u = u
            if book is not None:
                if qty == 0:
                    book[side].pop(price, None)
                else:
                    book[side][price] = qty
    if book:
        print(f"{hour:02d}:59 UTC  bid {max(book['bid'])}  ask {min(book['ask'])}")
'''


def test_published_worked_example_agrees_hour_by_hour(broken_day):
    root, ddir, truth = broken_day
    code = DOCS_EXAMPLE.replace('"data/book/binance/BTCUSDT/2026-09-25"', repr(str(ddir)))
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        exec(compile(code, "docs_example", "exec"), {})
    lines = out.getvalue().splitlines()
    assert len(lines) >= 22
    book = L2Book([ddir])
    declined = []
    for line in lines:
        hh = int(line[:2])
        bid = Decimal(line.split("bid ")[1].split()[0])
        ask = Decimal(line.split("ask ")[1].split()[0])
        t = pd.Timestamp(DAY, tz="UTC") + pd.Timedelta(hours=hh + 1) - pd.Timedelta(milliseconds=1)
        try:
            view = book.book_at(t)
        except BookUnavailable as exc:
            # The example prints the last book even when a break has already begun; the library
            # declines. Both agree on the book as of the last event before the break.
            # The tail after the day's last event is declined the same way.
            kind = "break" if "break in the update chain" in str(exc) else "tail"
            assert kind == "break" or "after the last event" in str(exc)
            seg = book.segments()
            last = seg.loc[seg["last_event"] <= t, "last_event"].max()
            view = book.book_at(last)
            declined.append(kind)
        assert (view.best_bid, view.best_ask) == (float(bid), float(ask)), line
    assert sorted(declined) == ["break", "tail"]     # the 16:59 drop, and 23:59 after the last event


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
