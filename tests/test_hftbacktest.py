"""candlefeed.hftbacktest against synthetic days whose true book is known after every event.

The files are replayed the way hftbacktest 2.4.4's processors read them (Rust ``backtest/proc/local.rs`` and
``nopartialfillexchange.rs`` with ``depth/hashmapmarketdepth.rs``): the exchange side takes rows flagged
EXCH_EVENT in exch_ts order, the local side rows flagged LOCAL_EVENT in local_ts order; a side-flagged
DEPTH_CLEAR_EVENT removes that side's levels from the best price to ``px`` (all of them when ``px`` is NaN) and
an unflagged one empties both sides."""
from __future__ import annotations

import hashlib
import json
import random
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from l2_synth import DEC, SCALE, day_ms, make_day, write_day

from candlefeed import hftbacktest as hb
from candlefeed.hftbacktest import (
    BUY_EVENT,
    DEPTH_CLEAR_EVENT,
    DEPTH_EVENT,
    DEPTH_SNAPSHOT_EVENT,
    EVENT_DTYPE,
    EXCH_EVENT,
    LOCAL_EVENT,
    SELL_EVENT,
    TRADE_EVENT,
    ConvertError,
    convert_l2,
    parse_latency,
)
from candlefeed.l2book import L2Book

DAY = "2026-09-01"
SYM = "BTCUSDT"


def _load(result):
    return np.concatenate([np.load(p)["data"] for p in result.files])


def _key(px):
    return int(round(px * 10))


def _replay(data, flag, stop_at):
    """Apply the rows hftbacktest's side ``flag`` processes; after the last row with timestamp <= each value in
    ``stop_at`` (sorted), yield a copy of (bids, asks) keyed by price in 0.1 units."""
    field = "exch_ts" if flag == EXCH_EVENT else "local_ts"
    rows = data[(data["ev"] & np.uint64(flag)) != 0]
    ts = rows[field]
    assert np.all(np.diff(ts) >= 0), f"{field} out of order"
    bids, asks = {}, {}
    i = 0
    for stop in stop_at:
        while i < len(rows) and ts[i] <= stop:
            ev, px, qty = int(rows["ev"][i]), float(rows["px"][i]), float(rows["qty"][i])
            kind = ev & 0xFF
            if kind == DEPTH_CLEAR_EVENT:
                if ev & BUY_EVENT:
                    if np.isnan(px):
                        bids.clear()
                    else:
                        for k in [k for k in bids if k >= _key(px)]:
                            del bids[k]
                elif ev & SELL_EVENT:
                    if np.isnan(px):
                        asks.clear()
                    else:
                        for k in [k for k in asks if k <= _key(px)]:
                            del asks[k]
                else:
                    bids.clear()
                    asks.clear()
            elif kind in (DEPTH_EVENT, DEPTH_SNAPSHOT_EVENT):
                book = bids if ev & BUY_EVENT else asks
                if qty == 0:
                    book.pop(_key(px), None)
                else:
                    book[_key(px)] = qty
            i += 1
        yield dict(bids), dict(asks)


def _truth(truth, i):
    k = SCALE // 10
    return ({p // k: q for p, q in truth.bids[i].items()}, {p // k: q for p, q in truth.asks[i].items()})


def _refresh_manifest(ddir: Path) -> None:
    m = json.loads((ddir / "manifest.json").read_text())
    for o in m["outputs"]:
        f = ddir / o["key"].split("/gen=synthetic/", 1)[1]
        body = f.read_bytes()
        o["bytes"], o["sha256"] = len(body), hashlib.sha256(body).hexdigest()
    (ddir / "manifest.json").write_text(json.dumps(m))


def _rewrite_depth(ddir: Path, fn) -> None:
    for f in sorted((ddir / "depth").glob("*.parquet")):
        pq.write_table(fn(pq.read_table(f)), f)
    _refresh_manifest(ddir)


def _copy_day(src: Path, root: Path) -> Path:
    dst = root / "book" / "binance" / SYM / src.name
    for f in src.rglob("*"):
        if f.is_file():
            (dst / f.relative_to(src)).parent.mkdir(parents=True, exist_ok=True)
            (dst / f.relative_to(src)).write_bytes(f.read_bytes())
    return dst


def _set_col(t, name, values):
    return t.set_column(t.schema.get_field_index(name), name, pa.array(values, type=t.schema.field(name).type))


@pytest.fixture(scope="module")
def broken_day(tmp_path_factory):
    root = tmp_path_factory.mktemp("broken")
    ddir, truth, _ = make_day(root, DAY, seed=7, drops=[(530, 6), (1699, 3)], resets=[1110],
                              moves={k: random.Random(k).choice((-2, -1, 1, 2)) for k in range(0, 2400, 9)})
    return root, ddir, truth


@pytest.fixture(scope="module")
def converted(broken_day, tmp_path_factory):
    root, ddir, truth = broken_day
    out = tmp_path_factory.mktemp("out")
    return convert_l2(root, SYM, DAY, out_dir=out, latency="1ms"), truth, ddir


def test_dtype_files_and_summary(converted):
    result, _, ddir = converted
    assert EVENT_DTYPE.itemsize == 64
    assert EVENT_DTYPE.names == ("ev", "exch_ts", "local_ts", "px", "qty", "order_id", "ival", "fval")
    hours = sorted(p.stem for p in (ddir / "depth").glob("*.parquet"))
    assert [p.name for p in result.files] == [f"{SYM}_20260901_{h}.npz" for h in hours]
    for p in result.files:
        with np.load(p) as z:
            assert list(z.keys()) == ["data"] and z["data"].dtype == EVENT_DTYPE
    summary = json.loads(result.summary.read_text())
    assert summary["files"] == [p.name for p in result.files]
    assert summary["hftbacktest_format"] == "2.4.4"
    data = _load(result)
    ev = data["ev"]
    kinds = set((ev & np.uint64(0xFF)).tolist())
    assert kinds == {DEPTH_EVENT, DEPTH_CLEAR_EVENT, DEPTH_SNAPSHOT_EVENT}
    assert np.all((ev & np.uint64(EXCH_EVENT | LOCAL_EVENT)) != 0)
    sided = (ev & np.uint64(0xFF)) != DEPTH_CLEAR_EVENT
    assert np.all(((ev[sided] & np.uint64(BUY_EVENT)) != 0) ^ ((ev[sided] & np.uint64(SELL_EVENT)) != 0))
    assert np.all(data["order_id"] == 0) and np.all(data["ival"] == 0) and np.all(data["fval"] == 0)


def test_both_sides_match_the_true_book_after_every_anchored_event(converted):
    result, truth, ddir = converted
    data = _load(result)
    book = L2Book([ddir])
    E, u = book._E, book._u
    live = np.array([book._anchor_for(e) is not None for e in range(book._n)])
    assert 0.9 * book._n < live.sum() < book._n
    index = {x: i for i, x in enumerate(truth.u)}
    for flag, ts in ((EXCH_EVENT, (E - 1) * 1_000_000), (LOCAL_EVENT, E * 1_000_000 + 5)):
        for e, (bids, asks) in zip(range(book._n), _replay(data, flag, ts)):
            if live[e]:
                assert (bids, asks) == _truth(truth, index[int(u[e])]), f"flag {flag:#x} event {e}"
            else:
                assert bids == {} and asks == {}, f"flag {flag:#x} event {e} should have no book"


def test_breaks_clear_the_book_and_unanchored_diffs_are_left_out(converted):
    result, truth, ddir = converted
    data = _load(result)
    book = L2Book([ddir])
    clears = data[(data["ev"] & np.uint64(0xFF | BUY_EVENT | SELL_EVENT)) == DEPTH_CLEAR_EVENT]
    seg_starts = np.flatnonzero(np.diff(book._seg)) + 1
    assert len(clears) == len(seg_starts) == 3
    assert clears["exch_ts"].tolist() == ((book._E[seg_starts] - 1) * 1_000_000).tolist()
    assert result.stats["chain_breaks_cleared"] == 3
    unanchored = sum(book._anchor_for(e) is None for e in range(book._n))
    assert result.stats["depth_events_skipped_unanchored"] == unanchored
    rows_out = int(np.count_nonzero((data["ev"] & np.uint64(0xFF)) == DEPTH_EVENT))
    rows_live = sum(int(book._r1[e] - book._r0[e]) for e in range(book._n) if book._anchor_for(e) is not None)
    assert rows_out == rows_live


def test_in_gap_snapshots_are_never_used(converted):
    result, truth, ddir = converted
    book = L2Book([ddir])
    assert book.skipped_snapshots >= 2
    data = _load(result)
    snap_clears = data[(data["ev"] & np.uint64(0xFF | BUY_EVENT)) == (DEPTH_CLEAR_EVENT | BUY_EVENT)]
    usable = len({a.j for a in book._anchors})
    assert len(snap_clears) == usable == result.stats["snapshots_anchor"] + result.stats["snapshots_refresh"]
    assert result.stats["snapshots_anchor"] == 4                # the day's start and the three breaks


def test_timestamps_constant(broken_day, tmp_path):
    root, ddir, _ = broken_day
    book = L2Book([ddir])
    const = convert_l2(root, SYM, DAY, out_dir=tmp_path / "c", latency="2ms")
    d = _load(const)
    dep = d[(d["ev"] & np.uint64(0xFF)) == DEPTH_EVENT]
    assert set(np.isin(dep["exch_ts"] // 1_000_000 + 1, book._E).tolist()) == {True}
    assert np.all(d["local_ts"] - d["exch_ts"] == 2_000_000)
    assert json.loads(const.summary.read_text())["local_ts"] == "exch_ts + 2000000 ns (constant feed latency)"
    with pytest.raises(TypeError):
        convert_l2(root, SYM, DAY, out_dir=tmp_path / "x")


def test_parse_latency():
    with pytest.raises(ConvertError, match="isn't offered in this version"):
        parse_latency("recv")
    assert parse_latency("5ms") == 5_000_000
    assert parse_latency("800us") == 800_000
    assert parse_latency("1.5ms") == 1_500_000
    assert parse_latency(250) == 250
    assert parse_latency("0") == 0
    for bad in ("fast", "-1ms", True):
        with pytest.raises(ConvertError):
            parse_latency(bad)


def test_straddle_event_is_applied_in_full(tmp_path):
    t0 = day_ms(DAY)
    written = [
        (t0 + 1000, 101, 101, 100, [("bid", 9999, Decimal("2"))]),
        (t0 + 2000, 102, 103, 101, [("bid", 10000, Decimal("5")), ("ask", 10001, Decimal("0"))]),
        (t0 + 3000, 104, 104, 103, [("ask", 10002, Decimal("6"))]),
    ]
    snap = ("tokyo", 102, t0 + 2000, (t0 + 2000) * 10**6, [(10000, Decimal("5")), (9999, Decimal("2"))],
            [(10001, Decimal("3")), (10002, Decimal("4"))])
    write_day(tmp_path, DAY, SYM, written, [snap])
    d = _load(convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "o", latency="1ms"))
    for flag in (EXCH_EVENT, LOCAL_EVENT):
        b1, b2 = list(_replay(d, flag, [(t0 + 2000) * 10**6 + 5, (t0 + 3000) * 10**6 + 5]))
        assert b1 == ({10000: 5.0, 9999: 2.0}, {10002: 4.0})       # the ask at 1000.1 is gone
        assert b2 == ({10000: 5.0, 9999: 2.0}, {10002: 6.0})
    first = d[d["exch_ts"] < (t0 + 2000 - 1) * 1_000_000]
    assert len(first) == 0                                          # the event before the anchor is left out


def test_empty_day_and_missing_files(tmp_path):
    ddir = write_day(tmp_path, DAY, SYM, [], [])
    m = json.loads((ddir / "manifest.json").read_text())
    m["canonical"].update(first_event_ms=0, last_event_ms=0)
    (ddir / "manifest.json").write_text(json.dumps(m))
    with pytest.raises(ConvertError, match="no diff events"):
        convert_l2(tmp_path, SYM, DAY, out_dir=tmp_path / "o", latency="1ms")
    with pytest.raises(ConvertError) as exc:
        convert_l2(tmp_path, SYM, "2026-09-02", out_dir=tmp_path / "o", latency="1ms")
    msg = str(exc.value)
    assert "download_l2_sample" in msg and "utm_source=hftbacktest&utm_medium=client" in msg
    make_day(tmp_path / "t", DAY, seed=2, n_events=50)
    with pytest.raises(ConvertError, match="trades files not downloaded"):
        convert_l2(tmp_path / "t", SYM, DAY, out_dir=tmp_path / "o2", latency="1ms", trades=True)


def test_chain_carries_across_midnight(tmp_path):
    d1, t1, m = make_day(tmp_path, "2026-09-01", seed=11, n_events=600, snapshot_every=200)
    d2, t2, _ = make_day(tmp_path, "2026-09-02", seed=12, market=m, n_events=600, snapshot_every=200,
                         first_event_ms=500)
    result = convert_l2(tmp_path, SYM, "2026-09-01", "2026-09-02", out_dir=tmp_path / "o", latency="1ms")
    assert result.stats.get("chain_breaks_cleared", 0) == 0
    assert result.stats["snapshots_anchor"] == 1
    data = _load(result)
    b2 = L2Book([d2])
    first_e2 = int(b2._E[0])
    (bids, asks), = _replay(data, LOCAL_EVENT, [first_e2 * 1_000_000 + 5])
    assert (bids, asks) == _truth(t2, 0)        # before day 2's first snapshot, the book carried over

    d3, _, _ = make_day(tmp_path / "x", "2026-09-02", seed=12, market=m, resets=[0], n_events=600,
                        snapshot_every=200)
    make_day(tmp_path / "x", "2026-09-01", seed=11, n_events=600, snapshot_every=200)
    broken = convert_l2(tmp_path / "x", SYM, "2026-09-01", "2026-09-02", out_dir=tmp_path / "o2", latency="1ms")
    assert broken.stats["chain_breaks_cleared"] == 1 and broken.stats["snapshots_anchor"] == 2


TRADE_NAMES = ["node", "exchange", "symbol", "trade_id", "event_time", "local_timestamp", "price", "amount",
               "side", "order_type", "is_backfill"]
TRADE_TYPES = [pa.string(), pa.string(), pa.string(), pa.int64(), pa.int64(), pa.int64(), DEC, DEC, pa.string(),
               pa.string(), pa.bool_()]


def _write_trades(root: Path, day: str, rows) -> Path:
    ddir = root / "trades" / "binance" / SYM / day
    ddir.mkdir(parents=True, exist_ok=True)
    full = [("tokyo", "binance", SYM, *r) for r in rows]
    cols = list(zip(*full))
    pq.write_table(pa.table([pa.array(c, type=t) for c, t in zip(cols, TRADE_TYPES)], names=TRADE_NAMES),
                   ddir / "trades.parquet")
    body = (ddir / "trades.parquet").read_bytes()
    manifest = {"dataset": "trades", "exchange": "binance", "symbol": SYM, "date": day, "generation": "synthetic",
                "canonical": {"trades": len(rows)},
                "outputs": [{"key": f"canonical/trades/binance/{SYM}/{day}/gen=synthetic/trades.parquet",
                             "rows": len(rows), "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}]}
    (ddir / "manifest.json").write_text(json.dumps(manifest))
    return ddir


def test_trades_are_merged_with_flags_sides_and_order(broken_day, tmp_path):
    _, src, _ = broken_day
    root = tmp_path / "data"
    _copy_day(src, root)
    book = L2Book([src])
    t0 = day_ms(DAY)
    rows, tid = [], 1000
    rng = random.Random(5)
    for e in range(0, book._n, 3):
        T = int(book._E[e]) - 1                  # same exch time as the diff: the trade must come first
        if T < t0:
            continue                             # a trades file only holds its own day's trades
        lat = rng.choice((1, 2, 40))             # sometimes slower than the diff, so local order differs
        rows.append((tid, T, T * 1_000_000 + lat * 1_000_000, Decimal("100000.1"), Decimal("0.002"),
                     rng.choice(("buy", "sell")), "MARKET", False))
        tid += 1
    rows.append((tid, t0 + 50_000_000, (t0 + 60_000_000) * 1_000_000, Decimal("100000.2"), Decimal("1"),
                 "buy", None, True))
    rows.append((tid + 1, t0 + 50_000_001, (t0 + 50_000_003) * 1_000_000, Decimal("100000.3"), Decimal("1"),
                 "sell", "NA", False))
    rows.sort(key=lambda r: r[0])
    _write_trades(root, DAY, rows)
    result = convert_l2(root, SYM, DAY, out_dir=tmp_path / "o", latency="3ms", trades=True)
    s = result.stats
    assert s["trades"] == len(rows) - 1 and s["trades_dropped_NA"] == 1
    d = _load(result)
    tr = d[(d["ev"] & np.uint64(0xFF)) == TRADE_EVENT]
    assert not np.any(tr["px"] == 100000.3)
    back = tr[tr["px"] == 100000.2]
    assert len(back) == 1 and (int(back["ev"][0]) & (EXCH_EVENT | LOCAL_EVENT)) == EXCH_EVENT | LOCAL_EVENT
    exch_rows = d[(d["ev"] & np.uint64(EXCH_EVENT)) != 0]
    loc_rows = d[(d["ev"] & np.uint64(LOCAL_EVENT)) != 0]
    assert np.all(np.diff(exch_rows["exch_ts"]) >= 0) and np.all(np.diff(loc_rows["local_ts"]) >= 0)
    n_tr = len(rows) - 1
    assert np.count_nonzero((exch_rows["ev"] & np.uint64(0xFF)) == TRADE_EVENT) == n_tr
    assert np.count_nonzero((loc_rows["ev"] & np.uint64(0xFF)) == TRADE_EVENT) == n_tr
    buy = tr[(tr["ev"] & np.uint64(BUY_EVENT)) != 0]
    want_buys = sum(1 for r in rows if r[5] == "buy" and r[6] != "NA")
    assert len({(int(b["exch_ts"]), float(b["px"])) for b in buy}) <= want_buys
    for i in np.flatnonzero((exch_rows["ev"] & np.uint64(0xFF)) == DEPTH_EVENT)[:200]:
        same_t = exch_rows[exch_rows["exch_ts"] == exch_rows["exch_ts"][i]]
        kinds = (same_t["ev"] & np.uint64(0xFF)).tolist()
        if TRADE_EVENT in kinds:
            assert kinds.index(TRADE_EVENT) < kinds.index(DEPTH_EVENT)
    assert np.all(d["local_ts"] - d["exch_ts"] == 3_000_000)
    stop = int(book._E[-1]) * 1_000_000 + 2_000_005
    book_after = list(_replay(d, LOCAL_EVENT, [stop]))[0]
    plain = convert_l2(root, SYM, DAY, out_dir=tmp_path / "p", latency="3ms")
    assert book_after == list(_replay(_load(plain), LOCAL_EVENT, [stop]))[0]


def test_numpy_guard(monkeypatch, broken_day, tmp_path):
    root, _, _ = broken_day
    monkeypatch.setattr(hb.sys, "version_info", (3, 14, 0))
    monkeypatch.setattr(hb.np, "__version__", "2.2.6")
    with pytest.raises(ConvertError, match="Python 3.11 to 3.13"):
        convert_l2(root, SYM, DAY, out_dir=tmp_path / "o", latency="1ms")


def test_cli(broken_day, tmp_path):
    root, _, _ = broken_day
    run = [sys.executable, "-m", "candlefeed.hftbacktest", "--symbol", SYM, "--start", DAY, "--data-dir", str(root),
           "--out", str(tmp_path / "o")]
    p = subprocess.run(run, capture_output=True, text=True)
    assert p.returncode == 2 and "--latency" in p.stderr
    p = subprocess.run(run + ["--latency", "recv"], capture_output=True, text=True)
    assert p.returncode == 2 and hb.RECV_REFUSED in " ".join(p.stderr.split())
    p = subprocess.run(run + ["--latency", "1ms"], capture_output=True, text=True)
    assert p.returncode == 0, p.stderr
    assert "snapshots 4 anchor" in p.stdout and "chain breaks 3" in p.stdout
    assert "utm_source=hftbacktest&utm_medium=client" in p.stdout
    assert "book only" in p.stdout
    p = subprocess.run(run[:-2] + ["--out", str(tmp_path / "x"), "--latency", "1ms", "--start", "2026-09-03"],
                       capture_output=True, text=True)
    assert p.returncode == 2 and "not downloaded" in p.stderr
