"""Convert CandleFeed's Binance USD-M order-book files into hftbacktest event files.

    candlefeed-hftbacktest --symbol BTCUSDT --start 2026-10-01 --data-dir data --out hbt/BTCUSDT --latency 4ms

reads the day folders ``cf.download_l2`` / ``cf.download_l2_sample`` wrote under ``data/`` and writes one
``.npz`` per UTC hour that hftbacktest's ``BacktestAsset().data([...])`` loads, plus
``candlefeed_hftbacktest.json`` listing the files in order with the conversion counts.

The layout was checked against hftbacktest 2.4.4 (``hftbacktest/types.py``, ``data/validation.py``,
``data/utils/tardis.py``, ``data/utils/binancefutures.py`` and the Rust reader, which loads the ``data``
array of each ``.npz``):

- one structured array of ``event_dtype`` (``ev u8, exch_ts i8, local_ts i8, px f8, qty f8, order_id u8,
  ival i8, fval f8``, aligned, 64 bytes a row); timestamps in nanoseconds
- ``ev`` is ``DEPTH_EVENT`` (1), ``TRADE_EVENT`` (2), ``DEPTH_CLEAR_EVENT`` (3) or ``DEPTH_SNAPSHOT_EVENT`` (4),
  OR'd with ``BUY_EVENT`` (bid side / buyer-initiated) or ``SELL_EVENT``, and with ``EXCH_EVENT`` and
  ``LOCAL_EVENT``. Every row carries both flags and is in ``exch_ts`` order, and so in ``local_ts`` order,
  across all files (what ``validate_event_order`` checks).

Timestamps. ``exch_ts`` is Binance's transaction time ``T`` (the ``tx_time`` column for diffs, the REST
response's ``T`` for snapshots, the trade time for trades), as hftbacktest's own Binance futures converter
uses, except where ``T`` steps backwards within a stream: a diff (in update-id order, across the range) or a
snapshot timed before the diff written ahead of it, or a trade (in trade-id order) timed before the previous
kept trade, is held at the previous value, and the summary counts those rows under ``depth_exch_ts_raised`` and
``trade_exch_ts_raised``. That happens where rows from two nodes with slightly different clocks meet. A
snapshot goes in at its own ``T`` or the ``T`` of the diff it anchors, whichever is earlier.
``local_ts = exch_ts + latency`` for every row, the constant feed latency you pass; there's no default.
Receive-time latency isn't offered in this version: CandleFeed's recorded receive times include pauses on the
capture node of up to several seconds, which would show a replayed strategy a stale book. The recorded receive
latency (receive time minus ``T``) is still reported per day under ``latency_by_day``, as
``recorded_recv_latency_depth_*`` and, for live trades, ``recorded_recv_latency_trades_*``.

CandleFeed files each trade under the UTC day of its trade time, so a trades file holding a trade timed
outside its day is malformed, and the run stops with ``ConvertError`` naming the file, the day and the trade
time, checked before any time is held. Trades timed after the last diff time a day has read (its processed
input horizon, the running maximum of every diff ``T`` so far, written or not) are carried into the next day's
files, because no later depth row can come before that horizon. Within the output, a trade and a depth row
with the same ``exch_ts`` always have the trade first.

The book follows CandleFeed's published rebuild rule (see ``candlefeed.l2book``). For each unbroken stretch of the
update chain, the first usable snapshot (``anchor_class`` not ``in_gap``) goes in as ``DEPTH_CLEAR_EVENT`` +
``DEPTH_SNAPSHOT_EVENT`` rows just before the first diff with ``u >= L``, that diff is applied in full
(straddle), and every later diff follows. Diffs before the anchor are left out. Every later usable snapshot
refreshes the book the same way, clearing only its own price range, as hftbacktest's converters do. A chain
break (``segment`` changes or ``pu`` isn't the previous ``u``) writes a ``DEPTH_CLEAR_EVENT`` for both sides at
the first diff after the break, and no depth rows follow until a snapshot of the new stretch anchors it: the
book is empty, not stale, while nobody knows it. Trades recovered over REST (``is_backfill``) sit at their trade
time like live ones.

A day none of whose diffs can be anchored is named under ``days_without_anchor`` in the summary; it writes a
file only for what it still has to carry (a chain-break clear ending the previous day's book, trades), and none
if it has nothing. The summary is deleted before the first output file is replaced and written again only after
the last one, so a failed run never leaves a file list naming a mix of old and new files. A file whose first row
would come before the previous file's last row on either clock stops the run with ``ConvertError`` and no
summary.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple, Union

import numpy as np

from .l2book import (
    _BATCH_ROWS,
    _MAX_FILE_DECODE_BYTES,
    IncompleteDay,
    L2Book,
    _check_inventory,
    _check_symbol,
    _codes,
    _decimal_ticks,
    _floats,
    _parquet,
    _sha256,
)

__all__ = [
    "BUY_EVENT",
    "DEPTH_CLEAR_EVENT",
    "DEPTH_EVENT",
    "DEPTH_SNAPSHOT_EVENT",
    "EVENT_DTYPE",
    "EXCH_EVENT",
    "LOCAL_EVENT",
    "SELL_EVENT",
    "TRADE_EVENT",
    "ConvertError",
    "ConvertResult",
    "convert_l2",
    "main",
    "parse_latency",
]

# hftbacktest 2.4.4, py-hftbacktest/hftbacktest/types.py
DEPTH_EVENT = 1
TRADE_EVENT = 2
DEPTH_CLEAR_EVENT = 3
DEPTH_SNAPSHOT_EVENT = 4
EXCH_EVENT = 1 << 31
LOCAL_EVENT = 1 << 30
BUY_EVENT = 1 << 29
SELL_EVENT = 1 << 28
EVENT_DTYPE = np.dtype([("ev", "u8"), ("exch_ts", "i8"), ("local_ts", "i8"), ("px", "f8"), ("qty", "f8"),
                        ("order_id", "u8"), ("ival", "i8"), ("fval", "f8")], align=True)

HFTBACKTEST_VERSION = "2.4.4"
PRICING_URL = "https://candlefeed.ai/pricing/?utm_source=hftbacktest&utm_medium=client"
DOCS_URL = "https://candlefeed.ai/docs/hftbacktest?utm_source=hftbacktest&utm_medium=client"
SAMPLE_DAYS = ("book BTCUSDT 2026-10-01", "book DOTUSDT 2026-10-01")
SUMMARY_NAME = "candlefeed_hftbacktest.json"

_BOTH = EXCH_EVENT | LOCAL_EVENT
_MIN = np.iinfo(np.int64).min
_EV, _TRADE, _CLEAR, _SNAP = 0, 2, 3, 4
_TRADE_COLS = ["trade_id", "event_time", "local_timestamp", "price", "amount", "side", "order_type",
               "is_backfill"]
_MAX_TRADE_ROWS = 50_000_000


class ConvertError(Exception):
    """A problem with the input the user can fix; the message says what to do."""


@dataclass
class ConvertResult:
    out_dir: Path
    files: List[Path] = field(default_factory=list)
    summary: Path = None
    stats: Dict[str, int] = field(default_factory=dict)
    notes: List[str] = field(default_factory=list)
    unanchored_days: List[Tuple[str, int, int]] = field(default_factory=list)


def _prices(ticks: np.ndarray, scale: int) -> np.ndarray:
    """Exact decimal ticks -> float64, correctly rounded: strip the trailing zeros every value shares so the
    integers convert exactly, then do one division."""
    k = scale
    while k > 0 and len(ticks) and np.any(ticks % (10 ** k)):
        k -= 1
    return (ticks // 10 ** k).astype(np.float64) / 10.0 ** (scale - k)


def _cummax(values: np.ndarray, start: int) -> np.ndarray:
    return np.maximum.accumulate(np.r_[np.int64(start), values])[1:] if len(values) else values


def _concat_ranges(starts: np.ndarray, lens: np.ndarray) -> np.ndarray:
    total = int(lens.sum())
    if not total:
        return np.empty(0, np.int64)
    off = np.cumsum(lens) - lens
    return np.repeat(starts - off, lens) + np.arange(total, dtype=np.int64)


@dataclass
class _Trades:
    exch: np.ndarray
    px: np.ndarray
    qty: np.ndarray
    ev: np.ndarray

    def __len__(self) -> int:
        return len(self.exch)

    def take(self, sl) -> "_Trades":
        return _Trades(self.exch[sl], self.px[sl], self.qty[sl], self.ev[sl])

    @staticmethod
    def cat(parts: Sequence["_Trades"]) -> "_Trades":
        return _Trades(*(np.concatenate([getattr(p, f) for p in parts]) for f in ("exch", "px", "qty", "ev")))

    @staticmethod
    def empty() -> "_Trades":
        return _Trades(np.empty(0, np.int64), np.empty(0), np.empty(0), np.empty(0, np.uint64))


@dataclass
class _State:
    live: bool = False                 # the book is anchored at the end of what's been written
    last_u: Optional[int] = None
    depth_exch: int = _MIN             # processed input horizon: running max of every diff T read, written or
                                       # not, so no later depth row can come before it
    trade_exch: int = _MIN
    carry: _Trades = None              # trades whose exch_ts is past the processed input horizon, for the next day


def _stat(stats: Dict[str, int], key: str, n: int) -> None:
    stats[key] = stats.get(key, 0) + int(n)


def _check_trades_day(ddir: Path, day: date, symbol: str, exchange: str, verify: bool) -> Tuple[Path, int]:
    def fail(why: str):
        raise IncompleteDay(f"{ddir} isn't a complete, verified trades download: {why}. Download the day again.")

    mpath = ddir / "manifest.json"
    if not mpath.is_file() or mpath.is_symlink():
        fail("manifest.json is missing")
    try:
        manifest = json.loads(mpath.read_text())
    except ValueError:
        fail("manifest.json isn't valid JSON")
    want = {"dataset": "trades", "exchange": exchange, "symbol": symbol, "date": day.isoformat()}
    if not isinstance(manifest, dict) or any(manifest.get(k) != v for k, v in want.items()):
        fail(f"manifest.json isn't the trades manifest for {exchange} {symbol} {day.isoformat()}")
    gen = manifest.get("generation")
    prefix = f"canonical/trades/{exchange}/{symbol}/{day.isoformat()}/gen={gen}/"
    listed = [o for o in manifest.get("outputs") or [] if isinstance(o, dict) and o.get("key") == prefix + "trades.parquet"]
    if len(listed) != 1 or len(manifest.get("outputs") or []) != 1 or not isinstance(gen, str):
        fail("manifest.json doesn't list exactly this generation's trades.parquet")
    path = ddir / "trades.parquet"
    if not path.is_file() or path.is_symlink():
        fail("trades.parquet is missing")
    if path.stat().st_size != listed[0].get("bytes"):
        fail("trades.parquet's size differs from the manifest")
    if verify and _sha256(path) != listed[0].get("sha256"):
        fail("trades.parquet doesn't match its SHA-256 in the manifest")
    count = (manifest.get("canonical") or {}).get("trades")
    return path, count if isinstance(count, int) and not isinstance(count, bool) else -1


def _read_trades(path: Path, day: date, expected: int, symbol: str, stats: Dict[str, int],
                 state: _State) -> Tuple[_Trades, np.ndarray]:
    """Returns the kept trades and, for the live ones, the recorded receive latency in ns."""
    import pyarrow as pa

    pf = _parquet(path, _MAX_TRADE_ROWS, ["trade_id", "event_time", "local_timestamp", "price", "amount"],
                  _MAX_FILE_DECODE_BYTES)
    schema = pf.schema_arrow
    for c in ("side", "order_type", "is_backfill"):
        if c not in schema.names:
            raise IncompleteDay(f"{path}: column {c!r} is missing")
    for c in ("side", "order_type"):
        tp = schema.field(c).type
        if pa.types.is_dictionary(tp):
            tp = tp.value_type
        if not (pa.types.is_string(tp) or pa.types.is_large_string(tp)):
            raise IncompleteDay(f"{path}: column {c!r} has type {schema.field(c).type}, not string")
    if not pa.types.is_boolean(schema.field("is_backfill").type):
        raise IncompleteDay(f"{path}: column 'is_backfill' has type {schema.field('is_backfill').type}, not bool")
    _check_symbol(pf, path, symbol)
    t = pf.read(columns=_TRADE_COLS)
    for c in _TRADE_COLS:
        if c != "order_type" and t[c].null_count:
            raise IncompleteDay(f"{path}: column {c!r} has {t[c].null_count:,} nulls")
    if expected >= 0 and t.num_rows != expected:
        raise IncompleteDay(f"{path} holds {t.num_rows:,} trades but its manifest says {expected:,}")
    ids = t["trade_id"].to_numpy()
    if len(ids) > 1 and np.any(np.diff(ids) <= 0):
        raise IncompleteDay(f"{path} isn't sorted by trade_id")
    times = t["event_time"].to_numpy().astype(np.int64)
    t0 = (day - date(1970, 1, 1)).days * 86_400_000
    outside = np.flatnonzero((times < t0) | (times >= t0 + 86_400_000))
    if len(outside):
        i = int(outside[0])
        try:
            at = datetime.fromtimestamp(int(times[i]) / 1000, timezone.utc).isoformat(timespec="milliseconds")
        except (ValueError, OverflowError, OSError):
            at = "outside the supported date range"
        raise ConvertError(f"{path} is the trades file for {day.isoformat()} UTC, but trade {int(ids[i])} has trade "
                           f"time {int(times[i])} ms ({at}), outside that day ({len(outside):,} of its "
                           f"{len(times):,} trades are). "
                           "CandleFeed files each trade under the UTC day of its trade time, so the file is "
                           f"malformed. Download {day.isoformat()}'s trades again; if it happens again, report it "
                           "with the date.")
    backfill = t["is_backfill"].to_numpy(zero_copy_only=False).astype(bool)
    otype, onames = _codes(t["order_type"].fill_null("\x00"))
    market = np.isin(otype, [i for i, n in enumerate(onames) if n == "MARKET"])
    untyped = np.isin(otype, [i for i, n in enumerate(onames) if n == "\x00"])
    # the collector writes REST-recovered trades without an order type; a live trade without one isn't known to be MARKET
    keep = market | (untyped & backfill)
    for i, n in enumerate(onames):
        if n not in ("MARKET", "\x00"):
            _stat(stats, f"trades_dropped_{n}", np.count_nonzero(otype == i))
    _stat(stats, "trades_dropped_live_null_order_type", np.count_nonzero(untyped & ~backfill))
    _stat(stats, "trades_backfill_null_order_type", np.count_nonzero(untyped & backfill))
    side, snames = _codes(t["side"])
    if not set(snames) <= {"buy", "sell"}:
        raise IncompleteDay(f"{path}: side holds {sorted(set(snames) - {'buy', 'sell'})}")
    buy = np.isin(side, [i for i, n in enumerate(snames) if n == "buy"])
    ticks, scale = _decimal_ticks(t["price"])
    exch_raw = times[keep] * 1_000_000
    live = ~backfill[keep]
    # a backfilled trade's local_timestamp is when it was recovered, not when it was seen
    recorded = t["local_timestamp"].to_numpy().astype(np.int64)[keep][live] - exch_raw[live]
    exch = _cummax(exch_raw, state.trade_exch)
    _stat(stats, "trade_exch_ts_raised", np.count_nonzero(exch != exch_raw))
    if len(exch):
        state.trade_exch = int(exch[-1])
    ev = np.where(buy[keep], TRADE_EVENT | BUY_EVENT, TRADE_EVENT | SELL_EVENT).astype(np.uint64)
    _stat(stats, "trades", len(exch))
    return _Trades(exch, _prices(ticks, scale)[keep], _floats(t["amount"])[keep], ev), recorded


def _event_times(book: L2Book, stats: Dict[str, int]) -> Tuple[np.ndarray, np.ndarray]:
    """Per diff event: tx_time (ms) and recv_time (ns), checked to be the same on every row of the event."""
    T = np.empty(book._n, np.int64)
    R = np.empty(book._n, np.int64)
    for path, n0, n1 in book._hours:
        pf = _parquet(path, book._max_rows, ["final_update_id", "tx_time", "recv_time"], _MAX_FILE_DECODE_BYTES)
        at = n0
        prev = None
        for batch in pf.iter_batches(batch_size=_BATCH_ROWS, columns=["final_update_id", "tx_time", "recv_time"]):
            u = batch.column("final_update_id").to_numpy()
            t = batch.column("tx_time").to_numpy()
            r = batch.column("recv_time").to_numpy()
            if prev is not None:
                u, t, r = np.r_[prev[0], u], np.r_[prev[1], t], np.r_[prev[2], r]
            same = u[1:] == u[:-1]
            if np.any(same & ((t[1:] != t[:-1]) | (r[1:] != r[:-1]))):
                raise IncompleteDay(f"{path}: rows of one event carry different tx_time or recv_time")
            st = np.flatnonzero(np.r_[prev is None, ~same])
            T[at:at + len(st)] = t[st]
            R[at:at + len(st)] = r[st]
            at += len(st)
            prev = (u[-1:], t[-1:], r[-1:])
        if at != n1:
            raise IncompleteDay(f"{path}: event count changed between reads")
    _stat(stats, "depth_events_in_files", book._n)
    return T * 1_000_000, R


class _Day:
    """Builds the ordered units (one diff event, one snapshot, one clear) for one day, in exch_ts order."""

    def __init__(self, book: L2Book, T: np.ndarray, state: _State, stats: Dict[str, int]):
        n = book._n
        T0 = T
        T = _cummax(T0, state.depth_exch)
        _stat(stats, "depth_exch_ts_raised", np.count_nonzero(T != T0))
        seg = book._seg
        bounds = np.flatnonzero(np.r_[True, seg[1:] != seg[:-1]])
        continuous = state.live and state.last_u is not None and int(book._pu[0]) == state.last_u
        anchors: Dict[int, int] = {}
        for i, a in enumerate(book._anchors):
            anchors[a.j] = i                     # several snapshots before one event: the last one is enough
        # a segment is anchored from its first usable snapshot's place in the chain
        live_from = np.full(len(bounds), n + 1, np.int64)
        if continuous:
            live_from[0] = 0
        for j in anchors:
            s = int(seg[j])
            live_from[s] = min(live_from[s], j)
        units = []                               # (p, sub, kind, ref, exch)
        for s, first in enumerate(bounds.tolist()):
            prior = state.live if s == 0 else live_from[s - 1] <= n
            if prior and not (s == 0 and continuous):
                units.append((first, 0, _CLEAR, -1, int(T[first])))
                _stat(stats, "chain_breaks_cleared", 1)
        for j in sorted(anchors):
            s = int(seg[j])
            start = j == live_from[s] and not (s == 0 and continuous)
            _stat(stats, "snapshots_anchor" if start else "snapshots_refresh", 1)
            # the snapshot precedes the event it anchors
            units.append((j, 1, _SNAP, anchors[j], min(book._anchors[anchors[j]].T * 1_000_000, int(T[j]))))
        ev = np.flatnonzero(np.arange(n) >= live_from[seg])
        self.depth_events = len(ev)
        _stat(stats, "depth_events_skipped_unanchored", n - len(ev))
        _stat(stats, "depth_events", len(ev))
        ins = np.array(units, dtype=np.int64).reshape(-1, 5)
        order = np.lexsort((np.r_[np.full(len(ev), 2), ins[:, 1]], np.r_[ev, ins[:, 0]]))
        self.kind = np.r_[np.full(len(ev), _EV, np.int8), ins[:, 2].astype(np.int8)][order]
        self.ref = np.r_[ev, ins[:, 3]][order]
        exch = np.r_[T[ev], ins[:, 4]][order]
        self.exch = _cummax(exch, state.depth_exch)
        _stat(stats, "depth_exch_ts_raised", np.count_nonzero(self.exch != exch))
        state.depth_exch = int(T[-1])
        state.live = bool(live_from[-1] <= n)
        state.last_u = int(book._u[-1])
        self.hour = np.full(len(self.kind), -1, np.int64)
        is_ev, is_snap = self.kind == _EV, self.kind == _SNAP
        self.hour[is_ev] = book._ev_hour[self.ref[is_ev]]
        # a snapshot's ref indexes book._anchors, not the diff events; several can anchor the same diff
        self.hour[is_snap] = book._ev_hour[np.array([book._anchors[i].j for i in self.ref[is_snap]], np.int64)]


def _merge(day: _Day, trades: _Trades):
    """Interleave a day's depth units with its trades in exch_ts order. Returns parallel arrays in output order."""
    nt = len(trades)
    kind = np.r_[np.full(nt, _TRADE, np.int8), day.kind]
    ref = np.r_[np.arange(nt, dtype=np.int64), day.ref]
    exch = np.r_[trades.exch, day.exch]
    hour = np.r_[np.full(nt, -1, np.int64), day.hour]
    order = np.argsort(exch, kind="stable")      # trades first on equal times
    return kind[order], ref[order], exch[order], hour[order]


def _snapshot_rows(book: L2Book, i: int) -> np.ndarray:
    a = book._anchors[i]
    scale = book._scale
    parts = []
    for is_bid, edge, sgn in ((True, a.bid_floor, BUY_EVENT), (False, a.ask_ceiling, SELL_EVENT)):
        m = a.rows.is_bid == is_bid
        ticks = a.rows.ticks[m]
        if not len(ticks):
            # the snapshot holds no levels on this side, so none of the old ones are left
            parts.append(np.array([(DEPTH_CLEAR_EVENT | sgn, 0, 0, np.nan, 0.0, 0, 0, 0.0)], EVENT_DTYPE))
            continue
        order = np.argsort(-ticks if is_bid else ticks, kind="stable")
        rows = np.zeros(len(ticks) + 1, EVENT_DTYPE)
        rows["ev"][0] = DEPTH_CLEAR_EVENT | sgn
        # hftbacktest clears from the best price to px; NaN clears the whole side (the snapshot held all of it)
        rows["px"][0] = np.nan if edge is None else _prices(np.array([edge], np.int64), scale)[0]
        rows["ev"][1:] = DEPTH_SNAPSHOT_EVENT | sgn
        rows["px"][1:] = _prices(ticks[order], scale)
        rows["qty"][1:] = a.rows.qty[m][order]
        parts.append(rows)
    return np.concatenate(parts) if parts else np.zeros(0, EVENT_DTYPE)


def _fill(book: L2Book, trades: _Trades, units, a: int, b: int, snap_cache: Dict[int, np.ndarray],
          latency: int) -> np.ndarray:
    kind, ref, exch, _ = (x[a:b] for x in units)
    is_dep = kind == _EV
    counts = np.ones(len(kind), np.int64)
    dref = ref[is_dep]
    counts[is_dep] = book._r1[dref] - book._r0[dref]
    for i in np.flatnonzero(kind == _SNAP):
        r = int(ref[i])
        if r not in snap_cache:
            snap_cache[r] = _snapshot_rows(book, r)
        counts[i] = len(snap_cache[r])
    off = np.cumsum(counts) - counts
    out = np.zeros(int(counts.sum()), EVENT_DTYPE)
    ut = np.flatnonzero(kind == _TRADE)
    if len(ut):
        tr = ref[ut]
        d = off[ut]
        out["ev"][d] = trades.ev[tr] | np.uint64(_BOTH)
        out["exch_ts"][d] = exch[ut]
        out["px"][d] = trades.px[tr]
        out["qty"][d] = trades.qty[tr]
    ud = np.flatnonzero(is_dep)
    hours = book._ev_hour[ref[ud]]
    for h in np.unique(hours).tolist():
        sel = ud[hours == h]
        e = ref[sel]
        lens = counts[sel]
        src = _concat_ranges(book._r0[e], lens)
        dst = _concat_ranges(off[sel], lens)
        rows = book._rows(h)
        out["ev"][dst] = np.where(rows.is_bid[src], DEPTH_EVENT | BUY_EVENT, DEPTH_EVENT | SELL_EVENT).astype(
            np.uint64) | np.uint64(_BOTH)
        out["exch_ts"][dst] = np.repeat(exch[sel], lens)
        out["px"][dst] = _prices(rows.ticks[src], book._scale)
        out["qty"][dst] = rows.qty[src]
    for i in np.flatnonzero(kind == _SNAP).tolist():
        rows = snap_cache[int(ref[i])].copy()
        rows["ev"] |= np.uint64(_BOTH)
        rows["exch_ts"] = exch[i]
        out[off[i]:off[i] + len(rows)] = rows
    for i in np.flatnonzero(kind == _CLEAR).tolist():
        out[off[i]] = (DEPTH_CLEAR_EVENT | _BOTH, exch[i], 0, np.nan, 0.0, 0, 0, 0.0)
    out["local_ts"] = out["exch_ts"] + latency
    return out


def _save(path: Path, data: np.ndarray, compress: bool) -> None:
    tmp = path.with_name(path.name + ".part")
    with open(tmp, "wb") as fh:
        (np.savez_compressed if compress else np.savez)(fh, data=data)
    os.replace(tmp, path)


def _days(start: Union[str, date], end: Union[str, date, None]) -> List[date]:
    first = date.fromisoformat(str(start)[:10])
    last = date.fromisoformat(str(end)[:10]) if end is not None else first
    if last < first:
        raise ConvertError("end is before start")
    return [first + timedelta(days=i) for i in range((last - first).days + 1)]


def _missing_message(kind: str, symbol: str, missing: List[str], data_dir: Path) -> str:
    return (f"{kind} files not downloaded for {symbol}: {', '.join(missing)} (looked in {data_dir}). Download "
            f"them first, e.g. CandleFeed(public=True).download_l2_sample('{symbol}', '{missing[0]}', "
            f"'{data_dir}') for the free sample days ({'; '.join(SAMPLE_DAYS)}), or "
            f"CandleFeed().download_l2('{kind}', '{symbol}', '{missing[0]}', None, '{data_dir}') with a key. "
            f"The Pro plan has every published day: {PRICING_URL}")


_UNITS = {"ns": 1, "us": 1_000, "ms": 1_000_000, "s": 1_000_000_000}


RECV_REFUSED = ("receive-time latency ('recv') isn't offered in this version, because CandleFeed's recorded "
                "receive times include capture-node pauses. Pass a constant such as 4ms.")


def parse_latency(value: Union[str, int]) -> int:
    """``"4ms"``, ``"800us"``, ``"2000000ns"`` or an int of nanoseconds -> that constant latency in ns."""
    if isinstance(value, bool):
        raise ConvertError("latency must be a duration such as '4ms'")
    if isinstance(value, (int, np.integer)):
        ns = int(value)
    else:
        text = str(value).strip().lower()
        if text == "recv":
            raise ConvertError(RECV_REFUSED)
        unit = next((u for u in ("ns", "us", "ms", "s") if text.endswith(u)), None)
        try:
            ns = round(float(text[:-len(unit)] if unit else text) * _UNITS[unit or "ns"])
        except ValueError:
            raise ConvertError(f"latency must be a duration such as '4ms', not {value!r}") from None
    if ns < 0:
        raise ConvertError("latency can't be negative")
    return ns


def _latency_stats(out: Dict[str, int], key: str, values: np.ndarray) -> None:
    if len(values):
        p50, p99 = np.percentile(values, [50, 99])
        out[f"{key}_p50_us"] = int(p50 // 1000)
        out[f"{key}_p99_us"] = int(p99 // 1000)


def _convert_day(d: date, ddir: Path, trade_dir: Optional[Path], *, symbol: str, exchange: str, verify: bool,
                 lat: int, last: bool, out_dir: Path, state: _State, stats: Dict[str, int], save,
                 say) -> Tuple[List[Path], Dict[str, int], int]:
    """Writes one day's hour files. Returns them, the day's recorded receive-latency percentiles and, when no
    diff of the day could be anchored, the day's diff event count."""
    say(f"{d.isoformat()}: reading the book index")
    book = L2Book([ddir], cache_hours=3, verify=False, symbol=symbol, exchange=exchange)
    T, R = _event_times(book, stats)
    latency: Dict[str, int] = {}
    _latency_stats(latency, "recorded_recv_latency_depth", R - T)
    del R
    day = _Day(book, T, state, stats)
    tr = state.carry
    if trade_dir is not None:
        path, count = _check_trades_day(trade_dir, d, symbol, exchange, verify)
        fresh, recorded = _read_trades(path, d, count, symbol, stats, state)
        _latency_stats(latency, "recorded_recv_latency_trades", recorded)
        tr = _Trades.cat([tr, fresh])
    unanchored = 0 if day.depth_events else book._n
    if last:
        state.carry = _Trades.empty()
    else:
        # the next day's depth starts at or after the processed input horizon, whether or not this day wrote any
        cut = int(np.searchsorted(tr.exch, state.depth_exch, side="right"))
        state.carry = tr.take(slice(cut, None))
        tr = tr.take(slice(0, cut))
    if not len(day.kind) and not len(tr):
        return [], latency, unanchored
    units = _merge(day, tr)
    hour = units[3]
    first_hour = int(day.hour[day.hour >= 0].min()) if day.depth_events else 0
    file_of = np.maximum.accumulate(np.r_[np.int64(first_hour), hour])[1:]
    bounds = np.flatnonzero(np.r_[True, file_of[1:] != file_of[:-1], True])
    snap_cache: Dict[int, np.ndarray] = {}
    files = []
    for a, b in zip(bounds[:-1].tolist(), bounds[1:].tolist()):
        hh = book._hours[int(file_of[a])][0].stem
        path = out_dir / f"{symbol}_{d:%Y%m%d}_{hh}.npz"
        data = _fill(book, tr, units, a, b, snap_cache, lat)
        _stat(stats, "rows", len(data))
        save(path, data)
        files.append(path)
        say(f"  {path.name}: {len(data):,} rows")
        del data
    return files, latency, unanchored


def convert_l2(
    data_dir: Union[str, os.PathLike],
    symbol: str,
    start: Union[str, date],
    end: Union[str, date, None] = None,
    out_dir: Union[str, os.PathLike] = "hftbacktest_data",
    *,
    latency: Union[str, int],
    trades: bool = False,
    compress: bool = True,
    verify: bool = True,
    exchange: str = "binance",
    progress=None,
) -> ConvertResult:
    """Convert downloaded ``book`` days (and with ``trades=True`` the matching ``trades`` days) from
    ``data_dir`` into hftbacktest ``.npz`` files in ``out_dir``, one per UTC hour of diffs.

    ``latency`` is a constant feed latency, a duration such as ``"4ms"`` or an int of nanoseconds:
    local_ts = exch_ts + latency for every row. ``"recv"`` is refused; see the module docstring.

    Days are processed one at a time, and within a day one hour file at a time, so memory grows with the
    largest hour and the day's event index, not with the range. The update chain carries across midnight
    when the next day's first ``pu`` equals the previous day's last ``u``."""
    if sys.version_info >= (3, 14) and np.lib.NumpyVersion(np.__version__) < "2.3.0":
        raise ConvertError(f"numpy {np.__version__} computes wrong results on Python 3.14 (it reuses arrays that are "
                           f"still in use), and hftbacktest {HFTBACKTEST_VERSION} pins numpy below 2.3. Run the "
                           "conversion and hftbacktest on Python 3.11 to 3.13, or convert in an environment with "
                           "numpy 2.3 or later.")
    symbol = symbol.upper()
    lat = parse_latency(latency)
    data_dir, out_dir = Path(data_dir), Path(out_dir)
    days = _days(start, end)
    book_dirs = [data_dir / "book" / exchange / symbol / d.isoformat() for d in days]
    missing = [d.isoformat() for d, p in zip(days, book_dirs) if not p.is_dir()]
    if missing:
        raise ConvertError(_missing_message("book", symbol, missing, data_dir))
    trade_dirs = [data_dir / "trades" / exchange / symbol / d.isoformat() for d in days]
    if trades:
        missing = [d.isoformat() for d, p in zip(days, trade_dirs) if not p.is_dir()]
        if missing:
            raise ConvertError(_missing_message("trades", symbol, missing, data_dir))
    for d, p in zip(days, book_dirs):
        manifest = _check_inventory(p, d, verify, symbol, exchange)
        if not manifest["canonical"]["events"]:
            raise ConvertError(f"{symbol} {d.isoformat()} has no diff events (its manifest says 0). Convert the "
                               "days before and after it separately.")
    out_dir.mkdir(parents=True, exist_ok=True)
    result = ConvertResult(out_dir=out_dir)
    stats = result.stats
    state = _State(carry=_Trades.empty())
    say = progress or (lambda msg: None)
    summary_path = out_dir / SUMMARY_NAME
    stale = [summary_path]
    written = [_MIN, _MIN]

    def save(path: Path, data: np.ndarray) -> None:
        if len(data):
            e0, l0 = int(data["exch_ts"][0]), int(data["local_ts"][0])
            if e0 < written[0] or l0 < written[1]:
                raise ConvertError(f"{path.name} would start at exch_ts {e0} / local_ts {l0} ns, before the previous "
                                   f"file's last row at exch_ts {written[0]} / local_ts {written[1]} ns, so "
                                   "hftbacktest would reject the file list. No file list was written; please "
                                   "report this with the dates you converted.")
            written[:] = int(data["exch_ts"][-1]), int(data["local_ts"][-1])
        if stale:
            # an old file list must not outlive the first file this run replaces
            stale.pop().unlink(missing_ok=True)
        _save(path, data, compress)

    latency_by_day: Dict[str, Dict[str, int]] = {}
    for di, (d, ddir) in enumerate(zip(days, book_dirs)):
        files, latency_by_day[d.isoformat()], unanchored = _convert_day(
            d, ddir, trade_dirs[di] if trades else None, symbol=symbol, exchange=exchange, verify=verify, lat=lat,
            last=di + 1 == len(days), out_dir=out_dir, state=state, stats=stats, save=save, say=say)
        result.files.extend(files)
        if unanchored:
            result.unanchored_days.append((d.isoformat(), unanchored, len(files)))
            say(f"  {unanchored:,} diff events and no usable snapshot to anchor them, so none were written"
                + ("" if files else "; no file for this day"))
    if not stats.get("depth_events"):
        raise ConvertError(f"Nothing written: no usable snapshot (one outside a gap) anchors the book on any day from "
                           f"{days[0].isoformat()} to {days[-1].isoformat()}, so every diff event came before an "
                           "anchor. Include a day that has one, such as the day before.")
    _stat(stats, "days_without_anchor", len(result.unanchored_days))
    if not trades:
        result.notes.append("book only: no trade events, so hftbacktest fills resting orders only when the "
                            "opposite best price moves through them, never from trades at your price")
    stats["output_bytes"] = sum(p.stat().st_size for p in result.files)
    summary = {
        "symbol": symbol, "exchange": exchange, "days": [d.isoformat() for d in days],
        "trades": trades, "hftbacktest_format": HFTBACKTEST_VERSION,
        "files": [p.name for p in result.files],
        "days_without_anchor": [{"day": d, "diff_events_not_written": n, "files": f}
                                for d, n, f in result.unanchored_days],
        "exch_ts": ("Binance transaction time T, ns, except: a snapshot goes in at the earlier of its own T and "
                    "the T of the diff it anchors; and where T stepped backwards within a stream, the row is held at "
                    "the previous value and counted under stats.depth_exch_ts_raised and stats.trade_exch_ts_raised"),
        "local_ts": f"exch_ts + {lat} ns (constant feed latency)",
        "stats": stats,
        "latency_by_day": latency_by_day,
        "notes": result.notes,
    }
    result.summary = summary_path
    tmp = summary_path.with_name(SUMMARY_NAME + ".part")
    tmp.write_text(json.dumps(summary, indent=1))
    os.replace(tmp, summary_path)
    return result


def _latency_arg(value: str) -> int:
    try:
        return parse_latency(value)
    except ConvertError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from None


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="candlefeed-hftbacktest",
        description="Convert downloaded CandleFeed L2 files (Binance USD-M) into hftbacktest .npz event files.")
    p.add_argument("--symbol", "-s", required=True, help="For example BTCUSDT.")
    p.add_argument("--start", required=True, help="First UTC day, YYYY-MM-DD.")
    p.add_argument("--end", help="Last UTC day (inclusive). Default: the start day.")
    p.add_argument("--data-dir", default="data", help="The dest_dir you gave download_l2. Default data.")
    p.add_argument("--out", default="hftbacktest_data", help="Output folder. Default hftbacktest_data.")
    p.add_argument("--latency", required=True, type=_latency_arg,
                   help="Constant feed latency, required, such as 4ms: local_ts = exchange time + 4 ms. Use your "
                        "own measured latency if you have one.")
    p.add_argument("--trades", action="store_true",
                   help="Add trade events from the downloaded trades days (needs an API key to download them).")
    p.add_argument("--no-compress", action="store_true", help="Write uncompressed .npz (faster, about 3x larger).")
    p.add_argument("--no-verify", action="store_true", help="Skip the SHA-256 check of the input files.")
    return p


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    try:
        result = convert_l2(args.data_dir, args.symbol, args.start, args.end, args.out, latency=args.latency,
                            trades=args.trades,
                            compress=not args.no_compress, verify=not args.no_verify,
                            progress=lambda m: print(m, flush=True))
    except (ConvertError, IncompleteDay, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    s = result.stats
    print(f"\n{len(result.files)} files, {s['rows']:,} rows, {s['output_bytes'] / 1e6:,.1f} MB in {result.out_dir}")
    print(f"diff events {s.get('depth_events', 0):,} (skipped before an anchor {s.get('depth_events_skipped_unanchored', 0):,}), "
          f"snapshots {s.get('snapshots_anchor', 0)} anchor + {s.get('snapshots_refresh', 0)} refresh, "
          f"chain breaks {s.get('chain_breaks_cleared', 0)}, trades {s.get('trades', 0):,}")
    for day, n, files in result.unanchored_days:
        print(f"{day}: no usable snapshot anchors the book, so its {n:,} diff events weren't written"
              + ("" if files else " and it has no file"))
    for note in result.notes:
        print(f"note: {note}")
    print(f"file list: {result.summary}\nLoad in hftbacktest: {DOCS_URL}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
