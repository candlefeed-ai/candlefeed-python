"""Seeded random multi-day ranges through the real Parquet/manifest readers and the converter.

Each case is 2-4 days mixing anchored and unanchored days, chain breaks and continuous chains across midnight,
empty snapshot sides, refresh and in-gap snapshots, transaction times published late enough that the next day's
first diff comes before the previous day's last ones, and (with trades on) trades near the boundaries, backfilled
trades, dropped trade types and trade times that step backwards. Trades stay inside their file's UTC day, as the
collector files them; a separate set of cases puts one outside and must be refused. The concatenated output in the
file list's order must keep both clocks in order, carry both flags with local_ts = exch_ts + latency, put trades
before depth rows at equal times, hold exactly the kept trades in input order with their payloads, and replay to
L2Book's book at every depth state that has an output time of its own. CANDLEFEED_FUZZ_CASES raises the case count
for a local run."""
from __future__ import annotations

import json
import os
import random
from datetime import date, timedelta
from decimal import Decimal

import numpy as np
import pytest
from l2_synth import DAY_MS, Market, day_ms, write_day
from test_hftbacktest import SYM, _replay, _rewrite_depth, _set_col, _write_trades

from candlefeed import hftbacktest as hb
from candlefeed.l2book import L2Book

try:
    from hftbacktest.data import validate_event_order
except ImportError:
    validate_event_order = None

CASES = int(os.environ.get("CANDLEFEED_FUZZ_CASES", "100"))
FEATURES = {"unanchored_day", "anchored_day", "break_at_midnight", "continuous_midnight", "next_day_T_earlier",
            "empty_snapshot_side", "refresh", "in_gap", "trades_carried", "trades_dropped", "backfill_kept",
            "trade_held", "snapshot_state"}
_MIN, _MAX = np.iinfo(np.int64).min, np.iinfo(np.int64).max


def _times(rng, t0, n):
    zones = (lambda: t0 + rng.randint(0, 40),
             lambda: t0 + rng.choice((3, 12)) * 3_600_000 + rng.randint(0, 1000),
             lambda: t0 + DAY_MS - 1 - rng.randint(0, 40))
    return sorted(rng.choice(zones)() for _ in range(n))


def _book_side(levels, rng):
    return [] if rng.random() < 0.15 else levels


def _generate(rng, root, days):
    m = Market(rng, levels=rng.randint(3, 12))
    tx = {}
    features = set()
    last_T_prev = None
    m_last_written = None
    day_end, day_T = {}, {}
    for di, d in enumerate(days):
        t0 = day_ms(d)
        n = rng.randint(3, 25)
        p_snap = rng.choice((0.0, 0.0, 0.08, 0.25))
        written, snaps = [], []
        skip = 0
        for k, E in enumerate(_times(rng, t0, n)):
            rows = m.event(rng.choice((0, 0, -1, 1)))
            U = m.last_u + 1 + rng.choice((0, 0, 2, 5))
            u = U + len(rows) - 1
            delay = rng.randint(0, 60) if k < 3 and rng.random() < 0.5 else rng.randint(0, 3)
            T = E - delay
            if k == 0 and di and rng.random() < 0.3:
                skip = max(skip, 1)                                      # break the chain at midnight
            pu = -1 if rng.random() < 0.03 else m.last_u
            kind = None
            if rng.random() < p_snap or (skip and rng.random() < 0.3):
                kind = rng.choice(["exact", "exact"] + (["straddle"] if len(rows) >= 2 else [])
                                  + (["between"] if U > m.last_u + 1 else []))
            snap_T = T + rng.randint(0, 6)
            if kind == "between":
                b, a = m.state()
                snaps.append(("tokyo", U - 1, T - rng.randint(0, 2), 0, _book_side(b, rng), _book_side(a, rng)))
            if kind == "straddle":
                cut = rng.randint(0, len(rows) - 2)
                m.apply(rows[:cut + 1])
                b, a = m.state()
                snaps.append(("tokyo", U + cut, snap_T, 0, _book_side(b, rng), _book_side(a, rng)))
                m.apply(rows[cut + 1:])
            else:
                m.apply(rows)
            m.last_u = u
            if kind == "exact":
                b, a = m.state()
                snaps.append(("tokyo", u, snap_T, 0, _book_side(b, rng), _book_side(a, rng)))
            if skip and k < n - 1:
                skip -= 1
                continue
            skip = 0
            if k == 0 and di:
                if pu == m_last_written:
                    features.add("continuous_midnight")
                else:
                    features.add("break_at_midnight")
                if last_T_prev is not None and T < last_T_prev:
                    features.add("next_day_T_earlier")
            written.append((E, U, u, pu, rows))
            tx[u] = T
            m_last_written = u
            if rng.random() < 0.06:
                skip = rng.randint(1, 3)
        last_T_prev = tx[written[-1][2]]
        day_end[d] = max(tx[w[2]] for w in written)
        day_T[d] = [tx[w[2]] for w in written]
        ddir = write_day(root, d, SYM, written, snaps)
        _rewrite_depth(ddir, lambda t: _set_col(t, "tx_time", [tx[u] for u in t["final_update_id"].to_pylist()]))
        if any(not s[4] or not s[5] for s in snaps):
            features.add("empty_snapshot_side")
    return tx, day_end, day_T, features


def _trade_rows(rng, d, day_T, day_end, last):
    """One day's trades, all timed inside the day, in trade-id order; times sometimes step backwards."""
    t0 = day_ms(d)
    rows, features = [], set()
    for _ in range(rng.randint(1, 20)):
        zone = rng.random()
        if zone < 0.3:
            t = rng.choice(day_T) + rng.randint(-3, 3)
        elif zone < 0.6:
            t = t0 + DAY_MS - rng.randint(1, 30)
        elif zone < 0.85:
            t = t0 + rng.randint(0, 40)
        else:
            t = t0 + rng.randint(0, DAY_MS - 1)
        t = min(max(t, t0), t0 + DAY_MS - 1)
        r = rng.random()
        kind, backfill = (("MARKET", False) if r < 0.65 else ("MARKET", True) if r < 0.72 else
                          (None, True) if r < 0.8 else ("LIMIT", False) if r < 0.87 else
                          (None, False) if r < 0.93 else ("NA", True))
        if (kind == "MARKET" or (kind is None and backfill)) and not last and t > day_end:
            features.add("trades_carried")
        if kind is None and backfill:
            features.add("backfill_kept")
        if kind != "MARKET" and not (kind is None and backfill):
            features.add("trades_dropped")
        recv = t + (60_000 if backfill else rng.randint(0, 5))
        rows.append([0, t, recv * 10**6, Decimal(rng.randint(9_990, 10_010)) / 10,
                     Decimal(rng.randint(1, 9_999)) / 1000, rng.choice(("buy", "sell")), kind, backfill])
    if rng.random() < 0.5:
        rows.sort(key=lambda r: r[1])
    return rows, features


def _generate_trades(rng, days, day_T, day_end):
    by_day, features = {}, set()
    for d in days:
        by_day[d], more = _trade_rows(rng, d, day_T[d], day_end[d], d == days[-1])
        features |= more
    return by_day, features


def _write_all_trades(root, days, by_day):
    tid = 1
    for d in days:
        for r in by_day[d]:
            r[0] = tid
            tid += 1
        _write_trades(root, d, [tuple(r) for r in by_day[d]])


def _expected_trades(days, by_day):
    """The kept trades in input order: (exch_ts ns, px, qty, buy), exch_ts held at the running maximum."""
    kept = [r for d in days for r in by_day[d] if r[6] == "MARKET" or (r[6] is None and r[7])]
    raw = np.array([r[1] for r in kept], np.int64) * 1_000_000
    exch = np.maximum.accumulate(raw) if len(raw) else raw
    return ([(int(e), float(r[3]), float(r[4]), r[5] == "buy") for e, r in zip(exch, kept)],
            int(np.count_nonzero(exch != raw)))


def _check_order(data, lat):
    assert np.all((data["ev"] & np.uint64(hb.EXCH_EVENT)) != 0) and np.all((data["ev"] & np.uint64(hb.LOCAL_EVENT)) != 0)
    assert np.all(data["local_ts"] - data["exch_ts"] == lat)
    for flag, col in ((hb.EXCH_EVENT, "exch_ts"), (hb.LOCAL_EVENT, "local_ts")):
        values = data[col][(data["ev"] & np.uint64(flag)) == np.uint64(flag)]
        back = np.flatnonzero(np.diff(values) < 0)
        assert not len(back), f"{col} goes back at row {back[0] + 1}: {values[back[0]]} -> {values[back[0] + 1]}"
    is_trade = (data["ev"] & np.uint64(0xFF)) == hb.TRADE_EVENT
    tie = np.flatnonzero((data["exch_ts"][1:] == data["exch_ts"][:-1]) & ~is_trade[:-1] & is_trade[1:])
    assert not len(tie), f"a trade follows a depth row at the same exch_ts, row {tie[0] + 1}"
    if validate_event_order is not None:
        validate_event_order(data)


def _apply_snapshot(prev, a, k):
    """The published refresh rule: each side clears from the best price to the snapshot's deepest level (the whole
    side if the snapshot holds all of it or none of it), then takes the snapshot's levels."""
    out = []
    for is_bid, edge, side in ((True, a.bid_floor, prev[0]), (False, a.ask_ceiling, prev[1])):
        m = a.rows.is_bid == is_bid
        levels = {int(t) // k: float(q) for t, q in zip(a.rows.ticks[m], a.rows.qty[m])}
        if not levels or edge is None:
            kept = {}
        elif is_bid:
            kept = {p: q for p, q in side.items() if p < edge // k}
        else:
            kept = {p: q for p, q in side.items() if p > edge // k}
        out.append({**kept, **levels})
    return tuple(out)


def _check_books(data, lat, book, tx):
    """Replays the output and compares it with L2Book at every depth state that has an output time of its own.

    The model of what's written, from the published rule: a chain-break clear at the first diff of a stretch
    after an anchored one, the last snapshot at each anchor place, then every anchored diff, in update-id order.
    A diff's time is the running maximum of T over every diff read; a snapshot's is the earlier of its T and its
    diff's; each unit is held at the latest time written before it in its day, and a day starts at the previous
    day's last diff time. The state after a unit (L2Book's after a diff, the snapshot laid over the book before
    it, empty after a clear) is checked when the next written unit is strictly later, and an unwritten diff's empty
    book is checked at its own time when no written unit sits at or around it; only states inside a group of rows
    with the same output time are skipped. Returns (checked, total, checked times)."""
    n = book._n
    eff = np.maximum.accumulate(np.array([tx[int(u)] for u in book._u], np.int64) * 1_000_000)
    k = 10 ** (book._scale - 1)
    anchored = [book._anchor_for(e) is not None for e in range(n)]
    day = np.asarray(book._E, np.int64) // DAY_MS

    def after(e):
        if e < 0 or not anchored[e]:
            return {}, {}
        c = book._seek(e, book._anchor_for(e))
        return {p // k: q for p, q in c.bids.items()}, {p // k: q for p, q in c.asks.items()}

    snaps = {a.j for a in book._anchors}
    units, run = [], _MIN                        # (time, position, label, expected state)
    for e in range(n):
        if e and day[e] != day[e - 1]:
            run = max(run, int(eff[e - 1]))
        same_seg = e > 0 and book._seg[e] == book._seg[e - 1]
        if e > 0 and not same_seg and anchored[e - 1]:
            run = max(run, int(eff[e]))
            units.append((run, e, f"clear at event {e}", lambda: ({}, {})))
        if e in snaps:
            a = book._anchors[book._anchor_for(e)]
            run = max(run, min(a.T * 1_000_000, int(eff[e])))
            prev = after(e - 1) if same_seg else ({}, {})
            units.append((run, e, f"snapshot before event {e}", lambda a=a, prev=prev: _apply_snapshot(prev, a, k)))
        if anchored[e]:
            run = max(run, int(eff[e]))
            units.append((run, e, f"event {e} (u={int(book._u[e])})", lambda e=e: after(e)))
    checks = {}

    def add(t, label, state):
        if t in checks:
            assert checks[t][1]() == state(), f"oracle model: {checks[t][0]} and {label} at {t} disagree"
        else:
            checks[t] = (label, state)

    for i, (t, _, label, state) in enumerate(units):
        if i + 1 == len(units) or units[i + 1][0] > t:
            add(t, label, state)
    times = [u[0] for u in units]
    pos = [u[1] for u in units]
    for e in (e for e in range(n) if not anchored[e]):
        i = int(np.searchsorted(pos, e, side="right"))
        if (i == len(units) or times[i] > eff[e]) and (i == 0 or times[i - 1] <= eff[e]):
            add(int(eff[e]), f"unwritten event {e}", lambda: ({}, {}))
    order = sorted(checks)
    want = [checks[t][1]() for t in order]
    for flag, shift in ((hb.EXCH_EVENT, 0), (hb.LOCAL_EVENT, lat)):
        got = list(_replay(data, flag, [t + shift for t in order]))
        for t, g, w in zip(order, got, want):
            assert g == w, f"flag {flag:#x}, {checks[t][0]} at {t}: book {g} != expected {w}"
    # a snapshot at its own diff's time is the straddle the published rule writes as one instant, not a state
    tied = sum(1 for i, u in enumerate(units) if u[2].startswith("snapshot") and units[i + 1][0] == u[0])
    return len(checks), len(units) - tied + anchored.count(False), set(order)


def _setup(seed, root):
    rng = random.Random(seed)
    days = [(date(2026, 9, 1) + timedelta(i)).isoformat() for i in range(rng.randint(2, 4))]
    trades = rng.random() < 0.6
    lat = rng.choice((0, 4_000_000))
    tx, day_end, day_T, features = _generate(rng, root, days)
    by_day = None
    if trades:
        by_day, more = _generate_trades(rng, days, day_T, day_end)
        features |= more
    return rng, days, lat, tx, by_day, features


def run_case(seed, root):
    _, days, lat, tx, by_day, features = _setup(seed, root)
    trades = by_day is not None
    if trades:
        _write_all_trades(root, days, by_day)
    book = L2Book([root / "book" / "binance" / SYM / d for d in days])
    anchored = [book._anchor_for(e) is not None for e in range(book._n)]
    out = root / "out"
    if not any(anchored):
        with pytest.raises(hb.ConvertError, match="Nothing written"):
            hb.convert_l2(root, SYM, days[0], days[-1], out, latency=lat, trades=trades)
        assert not (out / hb.SUMMARY_NAME).exists()
        return features | {"unanchored_day"}, 0, 0
    result = hb.convert_l2(root, SYM, days[0], days[-1], out, latency=lat, trades=trades)
    names = json.loads(result.summary.read_text())["files"]
    data = np.concatenate([np.load(out / name)["data"] for name in names])
    _check_order(data, lat)
    tr = data[(data["ev"] & np.uint64(0xFF)) == hb.TRADE_EVENT]
    got = [(int(r["exch_ts"]), float(r["px"]), float(r["qty"]), bool(int(r["ev"]) & hb.BUY_EVENT)) for r in tr]
    want, held = _expected_trades(days, by_day) if trades else ([], 0)
    assert got == want, "trades differ from the kept input trades in order"
    assert result.stats.get("trade_exch_ts_raised", 0) == held
    if held:
        features.add("trade_held")
    if result.unanchored_days:
        features.add("unanchored_day")
    if len(result.unanchored_days) < len(days):
        features.add("anchored_day")
    if result.stats.get("snapshots_refresh"):
        features.add("refresh")
    if book.skipped_snapshots:
        features.add("in_gap")
    checked, total, times = _check_books(data, lat, book, tx)
    depth_times = set(data["exch_ts"][(data["ev"] & np.uint64(0xFF)) != hb.TRADE_EVENT].tolist())
    assert depth_times <= times, f"depth rows at {sorted(depth_times - times)[:3]} were never checked"
    eff = {int(t) for t in np.maximum.accumulate(np.array([tx[int(u)] for u in book._u], np.int64) * 1_000_000)}
    if times - eff:
        features.add("snapshot_state")
    return features, checked, total


def test_random_multi_day_ranges(tmp_path):
    seen, checked, total = set(), 0, 0
    by_feature = {}
    for seed in range(CASES):
        try:
            f, c, t = run_case(seed, tmp_path / str(seed))
        except Exception as exc:
            raise AssertionError(f"seed {seed}: {exc}") from exc
        seen |= f
        checked += c
        total += t
        for name in f:
            fc, ft = by_feature.get(name, (0, 0))
            by_feature[name] = (fc + c, ft + t)
    assert checked > 0.8 * total, f"only {checked} of {total} depth states were checkable"
    if CASES >= 60:
        assert FEATURES <= seen, f"never generated: {sorted(FEATURES - seen)}"
        low = {name: (c, t) for name, (c, t) in by_feature.items() if t and c < 0.75 * t}
        assert not low, f"scenarios with under 75% of their depth states checked: {low}"


def test_trades_outside_their_day_are_refused(tmp_path):
    for seed in range(max(CASES // 4, 10)):
        root = tmp_path / str(seed)
        rng, days, lat, _, by_day, _ = _setup(seed, root)
        if by_day is None:
            by_day, _ = _generate_trades(rng, days, {d: [day_ms(d)] for d in days}, {d: _MAX for d in days})
        d = rng.choice(days)
        t0 = day_ms(d)
        t = rng.choice((t0 - rng.randint(1, 40), t0 + DAY_MS + rng.randint(0, 40),
                        t0 + rng.randint(0, DAY_MS - 1) + DAY_MS * rng.choice((-2, -1, 1, 2))))
        kind, backfill = rng.choice((("MARKET", False), ("MARKET", True), (None, True), ("NA", True), (None, False)))
        rows = by_day[d]
        rows.insert(rng.randint(0, len(rows)), [0, t, (t + 1) * 10**6, Decimal("999.5"), Decimal("1"), "buy", kind,
                                                backfill])
        _write_all_trades(root, days, by_day)
        out = root / "out"
        try:
            with pytest.raises(hb.ConvertError) as info:
                hb.convert_l2(root, SYM, days[0], days[-1], out, latency=lat, trades=True)
            msg = str(info.value)
            assert f"trades/binance/{SYM}/{d}/trades.parquet" in msg and f"for {d} UTC" in msg
            assert f"trade time {t} ms" in msg or "outside that day" in msg
            assert not (out / hb.SUMMARY_NAME).exists()
        except Exception as exc:
            raise AssertionError(f"seed {seed}: {exc}") from exc
