"""Rebuild the Binance USD-M order book from CandleFeed's daily L2 files.

    from candlefeed.l2book import L2Book

    book = L2Book.load("data/", "BTCUSDT", "2026-09-01")      # folders written by cf.download_l2
    view = book.book_at("2026-09-01T13:05:00Z", levels=10)
    spreads = book.spread_series("1s")
    depth = book.depth_at("2026-09-01T13:05:00Z", bps=[10, 50])

The rebuild follows the published rule (docs/order-book, "Rebuilding the book"):

1. Anchor on a REST snapshot whose ``anchor_class`` isn't ``in_gap``. Call its ``lastUpdateId`` L.
2. Skip diff events whose ``u`` is below L.
3. Apply the first event with ``u >= L`` in full on top of the snapshot. When L falls inside that
   event (``U <= L < u``, a straddle), the whole event is still applied: rows carry absolute
   quantities, so levels the snapshot already had get the same value again.
4. Apply every later event in order: set the level to ``qty``, or delete it when ``qty`` is 0.
5. A break in the update chain (``segment`` changes, or ``pu`` isn't the previous ``u``) makes the
   book stale. Nothing is reported until a snapshot of the new segment anchors it again.

Each query anchors on the latest usable snapshot at or before the requested moment and replays
from there. So a result never depends on where you started reading, and it usually sits within
one snapshot interval (about 5 minutes) of diffs from a real REST snapshot. Inside the snapshot's
price window this is the same book as one replay from the day's first snapshot; CandleFeed's own
rebuild check compares exactly that against every other snapshot in an hour. Levels outside the window
(deeper than the 1,000 levels Binance returns) are only known once a diff touches them, so
``depth_at`` flags bands that reach past the window.

A moment ``t`` means the book after every event whose Binance event time ``E`` is at or before
``t``. Times are UTC. Prices are kept as exact integers internally and returned as floats.
"""
from __future__ import annotations

import hashlib
import heapq
import json
import os
import re
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Tuple, Union

import numpy as np
import pandas as pd

try:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
except ImportError as exc:  # pragma: no cover - exercised only without the extra
    raise ImportError("candlefeed.l2book needs pyarrow. Install it with: pip install 'candlefeed[l2]'") from exc

__all__ = ["L2Book", "BookView", "BookUnavailable", "IncompleteDay", "L2BudgetExceeded"]

DAY_MS = 86_400_000
_INDEX_COLS = ["event_time", "first_update_id", "final_update_id", "prev_final_update_id", "segment"]
_ROW_COLS = ["side", "price", "qty"]
_SNAP_COLS = ["node", "event_time", "final_update_id", "recv_time", "side", "price", "qty", "anchor_class"]
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_MAX_SAMPLES = 5_000_000

TimeLike = Union[str, int, datetime, pd.Timestamp]


class BookUnavailable(LookupError):
    """No trustworthy book exists at that moment: before the first anchor, inside a chain break, or after
    the last event in the loaded files."""


class L2BudgetExceeded(MemoryError):
    """A file is over one of the limits checked from its footer before decoding: footer size, rows, or the
    estimated decoded size of the columns read. These are per-file caps, not a bound on the process."""


class IncompleteDay(ValueError):
    """A day folder isn't one complete, verified generation: a file the manifest lists is missing or
    differs, a file it doesn't list is present, or a download didn't finish."""


_MAX_MANIFEST_BYTES = 64 * 1024 * 1024
_MAX_PARQUET_METADATA_BYTES = 64 * 1024 * 1024
_BATCH_ROWS = 1_000_000
_MAX_SNAPSHOT_ROWS = 2_000_000        # a day is about 288 snapshots x 2,000 rows (576,000)
_MAX_SNAPSHOT_BYTES = 512 * 1024 ** 2   # decoded size of the snapshot columns read, per day
_MAX_FILE_DECODE_BYTES = 2 * 1024 ** 3  # decoded size of the columns read from one hour file
_ROW_BYTES = 17                       # is_bid (1) + price ticks (8) + qty (8) per decoded diff row


_KIND = {"event_time": "int", "first_update_id": "int", "final_update_id": "int", "prev_final_update_id": "int",
         "segment": "int", "recv_time": "int", "side": "str", "node": "str", "anchor_class": "str", "symbol": "str",
         "price": "dec", "qty": "dec"}
_WIDTH = {"int": 8, "str": 4, "dec": 16}      # decoded bytes per row; strings are read as dictionary indices
_STRING_COLS = tuple(c for c, k in _KIND.items() if k == "str")


def _type_ok(kind: str, t: "pa.DataType") -> bool:
    if kind == "int":
        return pa.types.is_integer(t)
    if kind == "dec":
        return pa.types.is_decimal128(t)
    return pa.types.is_string(t) or pa.types.is_large_string(t) or (
        pa.types.is_dictionary(t) and (pa.types.is_string(t.value_type) or pa.types.is_large_string(t.value_type)))


def _parquet(path: Path, max_rows: int, columns: Sequence[str], max_bytes: int) -> "pq.ParquetFile":
    """Open a Parquet file for reading ``columns`` after checking, from the footer alone, that it's the expected
    shape and small enough: footer size, row count, column types, and an estimate of the decoded size of those
    columns (fixed width per row, string columns read as dictionaries, plus their uncompressed pages)."""
    with open(path, "rb") as fh:
        fh.seek(0, 2)
        size = fh.tell()
        if size < 8:
            raise IncompleteDay(f"{path} is too short to be Parquet")
        fh.seek(size - 8)
        footer = fh.read(8)
    meta_len = int.from_bytes(footer[:4], "little")
    if footer[4:] != b"PAR1" or meta_len > _MAX_PARQUET_METADATA_BYTES:
        raise L2BudgetExceeded(f"{path}: Parquet metadata of {meta_len:,} bytes is over the "
                               f"{_MAX_PARQUET_METADATA_BYTES:,}-byte limit")
    meta = pq.ParquetFile(path)
    schema = meta.schema_arrow
    for c in columns:
        if c not in schema.names or not _type_ok(_KIND[c], schema.field(c).type):
            raise IncompleteDay(f"{path}: column {c!r} is missing or has an unexpected type")
    rows = meta.metadata.num_rows
    if rows > max_rows:
        raise L2BudgetExceeded(f"{path}: {rows:,} rows is over the limit of {max_rows:,} rows for this file")
    idx = [schema.names.index(c) for c in columns]
    pages = sum(meta.metadata.row_group(g).column(i).total_uncompressed_size
                for g in range(meta.metadata.num_row_groups) for i in idx)
    estimate = rows * sum(_WIDTH[_KIND[c]] for c in columns) + pages
    if estimate > max_bytes:
        raise L2BudgetExceeded(f"{path}: reading {', '.join(columns)} would decode about {estimate:,} bytes, over "
                               f"the {max_bytes:,}-byte limit for this file")
    return pq.ParquetFile(path, read_dictionary=[c for c in _STRING_COLS if c in schema.names])


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


_FILE_NAMES = frozenset([f"depth/{h:02d}.parquet" for h in range(24)] + ["snapshot.parquet"])
_GENERATION_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
_REQUIRED_COUNTERS = ("events", "segments", "first_event_ms", "last_event_ms")


def _check_inventory(ddir: Path, day: date, verify: bool, symbol: str, exchange: str) -> dict:
    """The day's manifest must be the book manifest for this exchange, symbol and date, name its
    generation, carry the canonical counters, and list exactly the Parquet files on disk under their full
    published keys, with matching sizes (and SHA-256 with verify). Hours the manifest doesn't list had no
    events; hours it lists must be present.

    This proves the files are consistent with what CandleFeed published for that day. It isn't a
    signature: hashes that came from the same source as the files can't protect against that source
    itself being malicious."""
    def fail(why: str):
        raise IncompleteDay(f"{ddir} isn't a complete, verified download: {why}. "
                            "Download the day again (download_l2 skips files that are already right).")

    mpath = ddir / "manifest.json"
    if not mpath.is_file() or mpath.is_symlink():
        fail("manifest.json is missing")
    if mpath.stat().st_size > _MAX_MANIFEST_BYTES:
        fail("manifest.json is implausibly large")
    try:
        with open(mpath) as fh:
            manifest = json.load(fh)
    except ValueError:
        fail("manifest.json isn't valid JSON")
    want = {"dataset": "book", "exchange": exchange, "symbol": symbol, "date": day.isoformat()}
    if not isinstance(manifest, dict) or any(manifest.get(k) != v for k, v in want.items()):
        got = {k: manifest.get(k) for k in want} if isinstance(manifest, dict) else None
        fail(f"manifest.json isn't the book manifest for {exchange} {symbol} {day.isoformat()} (it says {got})"[:400])
    generation = manifest.get("generation")
    if not isinstance(generation, str) or not _GENERATION_RE.match(generation):
        fail("manifest.json doesn't name its generation")
    canonical = manifest.get("canonical")
    if not isinstance(canonical, dict) or any(
            isinstance(canonical.get(k), bool) or not isinstance(canonical.get(k), int) for k in _REQUIRED_COUNTERS):
        fail(f"manifest.json is missing canonical counters ({', '.join(_REQUIRED_COUNTERS)})")
    outputs = manifest.get("outputs")
    if not isinstance(outputs, list):
        fail("manifest.json has no file list (outputs)")
    prefix = f"canonical/book/{exchange}/{symbol}/{day.isoformat()}/gen={generation}/"
    expected = {}
    for o in outputs:
        key = o.get("key") if isinstance(o, dict) else None
        name = key[len(prefix):] if isinstance(key, str) and key.startswith(prefix) else None
        if name not in _FILE_NAMES or name in expected or isinstance(o.get("bytes"), bool) \
                or not isinstance(o.get("bytes"), int):
            fail(f"manifest.json lists a file outside {prefix}: {key!r}"[:300])
        expected[name] = o
    on_disk = set()
    for folder, prefix in ((ddir, ""), (ddir / "depth", "depth/")):
        if not folder.exists():
            continue
        if folder.is_symlink():
            fail(f"{prefix or 'the day folder'} is a symlink")
        for entry in os.scandir(folder):
            name = prefix + entry.name
            if entry.is_symlink():
                fail(f"{name} is a symlink")
            if entry.name.endswith(".part"):
                fail(f"{name} is a partial download")
            if entry.name.endswith(".parquet"):
                on_disk.add(name)
    extra = sorted(on_disk - set(expected))
    if extra:
        fail(f"{', '.join(extra)} isn't in this generation's manifest (left over from an earlier download)")
    for name, o in sorted(expected.items()):
        path = ddir / name
        if name not in on_disk:
            fail(f"{name} is listed in the manifest but missing")
        if path.stat().st_size != o["bytes"]:
            fail(f"{name} is {path.stat().st_size:,} bytes, the manifest says {o['bytes']:,}")
        if verify and _sha256(path) != o.get("sha256"):
            fail(f"{name} doesn't match its SHA-256 in the manifest")
    return manifest


def _check_symbol(pf: "pq.ParquetFile", path: Path, symbol: str) -> None:
    """Every row's symbol must be the one asked for: from the row-group statistics when they're there,
    otherwise by reading that one column in batches."""
    names = pf.schema_arrow.names
    if "symbol" not in names:
        raise IncompleteDay(f"{path} has no symbol column")
    i = names.index("symbol")
    for g in range(pf.metadata.num_row_groups):
        st = pf.metadata.row_group(g).column(i).statistics
        if st is not None and st.has_min_max and st.null_count == 0:
            lo, hi = (v.decode() if isinstance(v, bytes) else v for v in (st.min, st.max))
            if lo != symbol or hi != symbol:
                raise IncompleteDay(f"{path} holds {lo}..{hi} rows, not {symbol}")
            continue
        for batch in pf.iter_batches(batch_size=_BATCH_ROWS, columns=["symbol"], row_groups=[g]):
            col = batch.column(0)
            if col.null_count or not pc.all(pc.equal(col.cast(pa.string()), symbol)).as_py():
                raise IncompleteDay(f"{path} holds rows for a symbol other than {symbol}")


def _to_ms(value: TimeLike) -> int:
    if isinstance(value, (int, np.integer)) and not isinstance(value, bool):
        return int(value)
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return int(ts.tz_convert("UTC").value // 1_000_000)


def _ts(ms: int) -> pd.Timestamp:
    return pd.Timestamp(int(ms), unit="ms", tz="UTC")


def _decimal_ticks(col) -> Tuple[np.ndarray, int]:
    """decimal128 column -> its unscaled integers (exact) and the scale. Raises if a value needs more than 64 bits."""
    typ = col.type
    if not pa.types.is_decimal128(typ):
        raise TypeError(f"expected a decimal128 column, got {typ}")
    chunks = col.chunks if isinstance(col, pa.ChunkedArray) else [col]
    out = []
    for ch in chunks:
        if len(ch) == 0:
            continue
        if ch.null_count:
            raise ValueError("null decimal value in an L2 file")
        words = np.frombuffer(ch.buffers()[1], dtype="<i8")[2 * ch.offset: 2 * (ch.offset + len(ch))].reshape(-1, 2)
        lo, hi = words[:, 0], words[:, 1]
        if not np.array_equal(hi, lo >> 63):
            raise OverflowError("value does not fit in 64 bits at the file's scale")
        out.append(lo.copy())
    return (np.concatenate(out) if out else np.empty(0, dtype=np.int64)), typ.scale


def _floats(col) -> np.ndarray:
    """decimal128 -> float64, correctly rounded below 2**53 units. pyarrow's own cast isn't
    (it returns 21.791999999999998 for 21.792), so it's only used for larger values."""
    try:
        units, scale = _decimal_ticks(col)
    except OverflowError:
        return pc.cast(col, pa.float64()).to_numpy()
    out = units.astype(np.float64) / 10.0 ** scale
    big = np.abs(units) >= 2 ** 53
    if big.any():
        out[big] = pc.cast(col, pa.float64()).to_numpy()[big]
    return out


def _codes(col) -> Tuple[np.ndarray, List[str]]:
    """Per-row integer codes and their names for a (dictionary) string column, without building one Python
    string per row."""
    chunks = col.chunks if isinstance(col, pa.ChunkedArray) else [col]
    names: List[str] = []
    lookup: Dict[str, int] = {}
    out = []
    for ch in chunks:
        if len(ch) == 0:
            continue
        if ch.null_count:
            raise IncompleteDay("null in a string column of an L2 file")
        if not pa.types.is_dictionary(ch.type):
            ch = pc.dictionary_encode(ch)
        remap = np.array([lookup.setdefault(v, len(lookup)) for v in ch.dictionary.to_pylist()], dtype=np.int64)
        out.append(remap[ch.indices.to_numpy(zero_copy_only=False)] if len(remap) else np.empty(0, np.int64))
    names = [None] * len(lookup)
    for v, i in lookup.items():
        names[i] = v
    return (np.concatenate(out) if out else np.empty(0, dtype=np.int64)), names


def _is_bid(col) -> np.ndarray:
    codes, names = _codes(col)
    return np.isin(codes, [i for i, n in enumerate(names) if n == "bid"])


@dataclass
class _Rows:
    is_bid: np.ndarray
    ticks: np.ndarray
    qty: np.ndarray


@dataclass
class _Anchor:
    L: int
    j: int
    segment: int
    cls: str
    node: str
    rows: _Rows
    bid_floor: Optional[int]     # None: the snapshot held the whole side
    ask_ceiling: Optional[int]


@dataclass(frozen=True)
class BookView:
    """The rebuilt book at one moment. ``bids`` is best first (highest price), ``asks`` best first (lowest).

    ``bid_window_floor`` and ``ask_window_ceiling`` are the deepest prices in the anchor snapshot,
    beyond which untouched levels aren't known; None means the snapshot held the whole side. Only
    levels inside that window are returned, because only they are known exactly. ``bids_complete`` is
    False when fewer levels than asked for are inside it; an empty side (best price None, so mid and
    spread None) means every known level has moved outside the window."""

    time: pd.Timestamp
    event_time: pd.Timestamp
    update_id: int
    segment: int
    anchor_update_id: int
    anchor_class: str
    anchor_node: str
    bids: pd.DataFrame
    asks: pd.DataFrame
    bids_complete: bool
    asks_complete: bool
    bid_window_floor: Optional[float]
    ask_window_ceiling: Optional[float]

    @property
    def best_bid(self) -> Optional[float]:
        return float(self.bids["price"].iat[0]) if len(self.bids) else None

    @property
    def best_ask(self) -> Optional[float]:
        return float(self.asks["price"].iat[0]) if len(self.asks) else None

    @property
    def mid(self) -> Optional[float]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2

    @property
    def spread(self) -> Optional[float]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask - self.best_bid

    @property
    def spread_bps(self) -> Optional[float]:
        if self.spread is None or not self.mid:
            return None
        return self.spread / self.mid * 1e4

    @property
    def age_ms(self) -> int:
        """Milliseconds between the last applied event and the moment asked for."""
        return int((self.time - self.event_time) / pd.Timedelta(milliseconds=1))

    def to_dict(self) -> dict:
        return {
            "time": self.time.isoformat(),
            "event_time": self.event_time.isoformat(),
            "update_id": self.update_id,
            "age_ms": self.age_ms,
            "segment": self.segment,
            "anchor": {"update_id": self.anchor_update_id, "class": self.anchor_class, "node": self.anchor_node},
            "best_bid": self.best_bid,
            "best_ask": self.best_ask,
            "mid": self.mid,
            "spread": self.spread,
            "spread_bps": self.spread_bps,
            "bids": self.bids[["price", "qty"]].values.tolist(),
            "asks": self.asks[["price", "qty"]].values.tolist(),
            "bids_complete": self.bids_complete,
            "asks_complete": self.asks_complete,
            "snapshot_window": {"bid_floor": self.bid_window_floor, "ask_ceiling": self.ask_window_ceiling},
        }


class _Cursor:
    __slots__ = ("anchor", "applied", "bids", "asks")

    def __init__(self) -> None:
        self.anchor = -1
        self.applied = -1
        self.bids: Dict[int, float] = {}
        self.asks: Dict[int, float] = {}


def _apply_rows(bids: Dict[int, float], asks: Dict[int, float], is_bid, ticks, qty) -> None:
    for mask, book in ((is_bid, bids), (~is_bid, asks)):
        k = ticks[mask]
        if not len(k):
            continue
        q = qty[mask]
        book.update(zip(k.tolist(), q.tolist()))
        zero = q == 0
        if zero.any():
            for key in k[zero].tolist():
                if book.get(key) == 0.0:
                    del book[key]


class L2Book:
    """Order book rebuilt from one or more consecutive downloaded ``book`` days of one symbol.

    ``row_cache_bytes`` limits the decoded diff rows kept between queries (and an hour file must fit in it).
    It is not a bound on total memory: the event index (about 80 bytes per diff event), the decoded
    snapshots, Arrow read buffers and returned frames come on top. Each file is also checked against hard
    caps from its footer before decoding (see L2BudgetExceeded).

    An L2Book isn't thread-safe: queries share a cursor and the row cache. Use one instance per thread, or
    hold a lock around each whole query (including iterating what iterate() yields)."""

    def __init__(self, day_dirs: Sequence[Union[str, Path]], cache_hours: int = 2,
                 snapshot_levels: int = 1000, verify: bool = True, row_cache_bytes: int = 2 * 1024 ** 3,
                 symbol: Optional[str] = None, exchange: str = "binance") -> None:
        if not day_dirs:
            raise ValueError("no day folders given")
        self._snapshot_levels = snapshot_levels
        self._cache_hours = max(1, int(cache_hours))
        self._row_cache_bytes = int(row_cache_bytes)
        self._max_rows = max(1, self._row_cache_bytes // _ROW_BYTES)   # an hour file must fit in the row cache
        self._cached_bytes = 0
        self._row_cache: "OrderedDict[int, _Rows]" = OrderedDict()
        self._scale: Optional[int] = None
        days = []
        for d in day_dirs:
            p = Path(d)
            if not p.is_dir():
                raise FileNotFoundError(f"not a folder: {p}")
            days.append((self._day_of(p), p))
        days.sort()
        self.days: List[date] = [d for d, _ in days]
        if len(set(self.days)) != len(self.days):
            raise ValueError("the same day was given twice")
        if symbol is None:
            names = {p.parent.name for _, p in days}
            if len(names) != 1 or not re.match(r"^[A-Z0-9]{2,30}$", next(iter(names))):
                raise ValueError("can't tell the symbol from the folder layout; pass symbol=")
            symbol = next(iter(names))
        self.symbol, self.exchange = symbol.upper(), exchange
        self._manifests = [_check_inventory(p, d, verify, self.symbol, exchange) for d, p in days]
        self._build_index(days)
        self._check_against_manifests()
        self._build_anchors(days)
        self._cursor = _Cursor()

    @classmethod
    def load(cls, root: Union[str, Path], symbol: str, start: Union[str, date],
             end: Union[str, date, None] = None, exchange: str = "binance") -> "L2Book":
        """Load days written by ``CandleFeed.download_l2(..., dest_dir=root)``."""
        first = date.fromisoformat(str(start)[:10])
        last = date.fromisoformat(str(end)[:10]) if end is not None else first
        if last < first:
            raise ValueError("end is before start")
        base = Path(root) / "book" / exchange / symbol.upper()
        dirs, missing = [], []
        d = first
        while d <= last:
            p = base / d.isoformat()
            (dirs if p.is_dir() else missing).append(p)
            d += timedelta(days=1)
        if missing:
            raise FileNotFoundError(
                "not downloaded: " + ", ".join(p.name for p in missing)
                + f". Fetch them with CandleFeed().download_l2('book', '{symbol.upper()}', ..., dest_dir='{root}')")
        return cls(dirs, symbol=symbol.upper(), exchange=exchange)

    @staticmethod
    def _day_of(path: Path) -> date:
        if _DATE_RE.match(path.name):
            return date.fromisoformat(path.name)
        manifest = path / "manifest.json"
        if manifest.exists():
            with open(manifest) as fh:
                value = json.load(fh).get("date")
            if value and _DATE_RE.match(str(value)):
                return date.fromisoformat(value)
        raise ValueError(f"can't tell which day {path} holds; name the folder YYYY-MM-DD")

    def _check_scale(self, scale: int) -> None:
        if self._scale is None:
            self._scale = scale
        elif scale != self._scale:
            raise ValueError(f"price scale changed between files ({self._scale} vs {scale})")

    # ------------------------------------------------------------------ index
    def _build_index(self, days) -> None:
        cols = {k: [] for k in ("E", "U", "u", "pu", "fseg", "day", "hour", "r0", "r1")}
        self._hours: List[Tuple[Path, int, int]] = []
        n = 0
        for di, (_, ddir) in enumerate(days):
            m = self._manifests[di]
            prefix = f"canonical/book/{self.exchange}/{self.symbol}/{m['date']}/gen={m['generation']}/"
            listed = {o["key"][len(prefix):] for o in m["outputs"]}
            for hh in range(24):
                if f"depth/{hh:02d}.parquet" not in listed:
                    continue                                    # no events that hour, per the manifest
                path = ddir / "depth" / f"{hh:02d}.parquet"
                pf = _parquet(path, self._max_rows, _INDEX_COLS + ["symbol"], _MAX_FILE_DECODE_BYTES)
                _check_symbol(pf, path, self.symbol)
                total = pf.metadata.num_rows
                if total == 0:
                    continue
                parts = {k: [] for k in ("E", "U", "u", "pu", "fseg", "start")}
                offset, last_u = 0, None
                for batch in pf.iter_batches(batch_size=_BATCH_ROWS, columns=_INDEX_COLS):
                    u = batch.column("final_update_id").to_numpy()
                    first = last_u is None or u[0] != last_u      # an event can continue from the last batch
                    st = np.flatnonzero(np.r_[first, u[1:] != u[:-1]])
                    parts["start"].append(st + offset)
                    parts["E"].append(batch.column("event_time").to_numpy()[st])
                    parts["U"].append(batch.column("first_update_id").to_numpy()[st])
                    parts["u"].append(u[st])
                    parts["pu"].append(batch.column("prev_final_update_id").to_numpy()[st])
                    parts["fseg"].append(batch.column("segment").to_numpy()[st].astype(np.int64))
                    offset += len(u)
                    last_u = u[-1]
                starts = np.concatenate(parts["start"]).astype(np.int64)
                ends = np.r_[starts[1:], total]
                k = len(starts)
                for name in ("E", "U", "u", "pu", "fseg"):
                    cols[name].append(np.concatenate(parts[name]))
                cols["day"].append(np.full(k, di, dtype=np.int32))
                cols["hour"].append(np.full(k, len(self._hours), dtype=np.int32))
                cols["r0"].append(starts.astype(np.int64))
                cols["r1"].append(ends.astype(np.int64))
                self._hours.append((path, n, n + k))
                n += k
        if n == 0:
            raise ValueError("no depth rows in the given days")
        a = {k: np.concatenate(v) for k, v in cols.items()}
        if np.any(np.diff(a["u"]) <= 0):
            raise ValueError("diff events are not in update-id order across files")
        self._E, self._U, self._u, self._pu = a["E"], a["U"], a["u"], a["pu"]
        self._Ecum = np.maximum.accumulate(self._E)
        self._ev_hour, self._r0, self._r1 = a["hour"], a["r0"], a["r1"]
        same_day = a["day"][1:] == a["day"][:-1]
        breaks = (self._pu[1:] != self._u[:-1]) | ((a["fseg"][1:] != a["fseg"][:-1]) & same_day)
        self._seg = np.r_[0, np.cumsum(breaks)].astype(np.int64)
        self._ev_day = a["day"]
        self._day_fseg = a["fseg"]
        self._n = n
        self._t0 = int(pd.Timestamp(self.days[0]).tz_localize("UTC").value // 1_000_000)
        self._t1 = int(pd.Timestamp(self.days[-1]).tz_localize("UTC").value // 1_000_000) + DAY_MS

    def _check_against_manifests(self) -> None:
        """What was read must be what the manifest describes: event count, first and last event, segments."""
        for di, m in enumerate(self._manifests):
            c = m.get("canonical") or {}
            idx = np.flatnonzero(self._ev_day == di)
            got = {"events": len(idx),
                   "first_event_ms": int(self._E[idx[0]]) if len(idx) else None,
                   "last_event_ms": int(self._E[idx[-1]]) if len(idx) else None,
                   "segments": int(len(np.unique(self._day_fseg[idx]))) if len(idx) else 0}
            for k, v in got.items():
                if isinstance(c.get(k), int) and c[k] != v:
                    raise IncompleteDay(f"{self.days[di].isoformat()}: the files hold {k}={v} but the manifest says "
                                        f"{c[k]}. Download the day again.")

    def _classify(self, L: int) -> Tuple[str, Optional[int]]:
        """Same placement as reconcile_l2.classify_snapshot, on the loaded chain."""
        j = int(np.searchsorted(self._u, L, side="left"))
        if j == self._n:
            return "in_gap", None
        if self._u[j] == L:
            return "clean", j
        if self._U[j] <= L:
            return "straddle", j
        if 0 <= self._pu[j] <= L:
            return "clean", j
        return "in_gap", None

    def _build_anchors(self, days) -> None:
        anchors: List[_Anchor] = []
        self.skipped_snapshots = 0
        for _, ddir in days:
            path = ddir / "snapshot.parquet"
            if not path.exists():
                continue
            spf = _parquet(path, _MAX_SNAPSHOT_ROWS, _SNAP_COLS + ["symbol"], _MAX_SNAPSHOT_BYTES)
            _check_symbol(spf, path, self.symbol)
            t = spf.read(columns=_SNAP_COLS)
            if t.num_rows == 0:
                continue
            L = t["final_update_id"].to_numpy()
            recv = t["recv_time"].to_numpy()
            node_codes, node_names = _codes(t["node"])
            cls_codes, cls_names = _codes(t["anchor_class"])
            order = np.lexsort((node_codes, recv, L))
            Ls, rs, ns = L[order], recv[order], node_codes[order]
            new_copy = np.r_[True, (Ls[1:] != Ls[:-1]) | (rs[1:] != rs[:-1]) | (ns[1:] != ns[:-1])]
            new_L = np.r_[True, Ls[1:] != Ls[:-1]]
            copy_id = np.cumsum(new_copy) - 1
            keep = np.isin(copy_id, copy_id[new_L])
            idx = order[keep]
            ticks, scale = _decimal_ticks(t["price"].take(pa.array(idx)))
            self._check_scale(scale)
            rows = _Rows(_is_bid(t["side"].take(pa.array(idx))), ticks, _floats(t["qty"].take(pa.array(idx))))
            cls_kept = cls_codes[idx]
            node_kept = ns[keep]
            Lk = L[idx]
            bounds = np.flatnonzero(np.r_[True, Lk[1:] != Lk[:-1], True])
            for b0, b1 in zip(bounds[:-1], bounds[1:]):
                Lval = int(Lk[b0])
                published = cls_names[cls_kept[b0]]
                mine, j = self._classify(Lval)
                if published == "in_gap" or mine == "in_gap":
                    self.skipped_snapshots += 1
                    continue
                r = _Rows(rows.is_bid[b0:b1], rows.ticks[b0:b1], rows.qty[b0:b1])
                bid_t = r.ticks[r.is_bid]
                ask_t = r.ticks[~r.is_bid]
                anchors.append(_Anchor(
                    L=Lval, j=j, segment=int(self._seg[j]), cls=mine, node=str(node_names[node_kept[b0]]), rows=r,
                    bid_floor=int(bid_t.min()) if len(bid_t) >= self._snapshot_levels else None,
                    ask_ceiling=int(ask_t.max()) if len(ask_t) >= self._snapshot_levels else None))
        anchors.sort(key=lambda a: (a.j, a.L))
        self._anchors = anchors
        self._aj = np.array([a.j for a in anchors], dtype=np.int64)
        self._aseg = np.array([a.segment for a in anchors], dtype=np.int64)

    # ------------------------------------------------------------------ rows
    def _rows(self, h: int) -> _Rows:
        rows = self._row_cache.get(h)
        if rows is not None:
            self._row_cache.move_to_end(h)
            return rows
        pf = _parquet(self._hours[h][0], self._max_rows, _ROW_COLS, _MAX_FILE_DECODE_BYTES)
        n = pf.metadata.num_rows
        need = n * _ROW_BYTES
        # make room first, so the old hours are gone before the new one is decoded
        while self._row_cache and (len(self._row_cache) >= self._cache_hours
                                   or self._cached_bytes + need > self._row_cache_bytes):
            _, old = self._row_cache.popitem(last=False)
            self._cached_bytes -= len(old.ticks) * _ROW_BYTES
        rows = _Rows(np.empty(n, dtype=bool), np.empty(n, dtype=np.int64), np.empty(n, dtype=np.float64))
        at = 0
        for batch in pf.iter_batches(batch_size=_BATCH_ROWS, columns=_ROW_COLS):
            m = batch.num_rows
            ticks, scale = _decimal_ticks(batch.column("price"))
            self._check_scale(scale)
            rows.ticks[at:at + m] = ticks
            rows.is_bid[at:at + m] = _is_bid(batch.column("side"))
            rows.qty[at:at + m] = _floats(batch.column("qty"))
            at += m
        self._row_cache[h] = rows
        self._cached_bytes += need
        return rows

    def _apply_events(self, c: _Cursor, e0: int, e1: int) -> None:
        e = e0
        while e <= e1:
            h = int(self._ev_hour[e])
            last = min(e1, self._hours[h][2] - 1)
            rows = self._rows(h)
            r0, r1 = int(self._r0[e]), int(self._r1[last])
            _apply_rows(c.bids, c.asks, rows.is_bid[r0:r1], rows.ticks[r0:r1], rows.qty[r0:r1])
            e = last + 1
        c.applied = e1

    # ------------------------------------------------------------------ positioning
    def _event_at(self, t_ms: int) -> int:
        return int(np.searchsorted(self._Ecum, t_ms, side="right")) - 1

    def _anchor_for(self, e: int) -> Optional[int]:
        i = int(np.searchsorted(self._aj, e, side="right")) - 1
        if i < 0 or self._aseg[i] != self._seg[e]:
            return None
        return i

    def _locate(self, t_ms: int) -> Tuple[Optional[int], Optional[int], str]:
        """(event, anchor, reason). event/anchor are None when there's no trustworthy book at t."""
        if not self._t0 <= t_ms < self._t1:
            raise ValueError(f"{_ts(t_ms).isoformat()} is outside the loaded days "
                             f"({self.days[0].isoformat()} to {self.days[-1].isoformat()})")
        e = self._event_at(t_ms)
        if e < 0:
            return None, None, "before the first diff event in the loaded files"
        if e == self._n - 1 and t_ms > self._E[e]:
            return None, None, (f"after the last event in the loaded files ({_ts(self._E[e]).isoformat()}); "
                                "load the next day too to see past it")
        if e + 1 < self._n and self._seg[e + 1] != self._seg[e] and t_ms > self._E[e]:
            return None, None, (f"inside a break in the update chain ({_ts(self._E[e]).isoformat()} to "
                                f"{_ts(self._E[e + 1]).isoformat()}); see the day's gaps in its manifest")
        a = self._anchor_for(e)
        if a is None:
            return None, None, "no usable snapshot yet in this stretch of the chain (after the start of data or a break)"
        return e, a, ""

    def _seek(self, e: int, a: int) -> _Cursor:
        c = self._cursor
        if c.anchor != a or c.applied > e:
            anchor = self._anchors[a]
            c.bids, c.asks = {}, {}
            _apply_rows(c.bids, c.asks, anchor.rows.is_bid, anchor.rows.ticks, anchor.rows.qty)
            c.anchor = a
            c.applied = anchor.j - 1
        if c.applied < e:
            self._apply_events(c, c.applied + 1, e)
        return c

    @staticmethod
    def _best(c: _Cursor, anchor: _Anchor) -> Tuple[Optional[int], Optional[int]]:
        """Best bid and ask, or None for a side whose best known level is outside the anchor snapshot's
        window: an untouched level nobody can see could be better than it."""
        b = max(c.bids) if c.bids else None
        a = min(c.asks) if c.asks else None
        if b is not None and anchor.bid_floor is not None and b < anchor.bid_floor:
            b = None
        if a is not None and anchor.ask_ceiling is not None and a > anchor.ask_ceiling:
            a = None
        return b, a

    def _price(self, ticks: int) -> float:
        return ticks / 10 ** self._scale

    def _view(self, t_ms: int, e: int, a: int, c: _Cursor, levels: Optional[int]) -> BookView:
        anchor = self._anchors[a]
        lo, hi = anchor.bid_floor, anchor.ask_ceiling
        bids = c.bids.items() if lo is None else [kv for kv in c.bids.items() if kv[0] >= lo]
        asks = c.asks.items() if hi is None else [kv for kv in c.asks.items() if kv[0] <= hi]
        if levels is None:
            bid_items = sorted(bids, reverse=True)
            ask_items = sorted(asks)
        else:
            bid_items = heapq.nlargest(levels, bids)
            ask_items = heapq.nsmallest(levels, asks)

        def frame(items):
            return pd.DataFrame({"price": [self._price(k) for k, _ in items],
                                 "qty": [q for _, q in items]}, columns=["price", "qty"])

        return BookView(
            time=_ts(t_ms), event_time=_ts(self._E[e]), update_id=int(self._u[e]), segment=int(self._seg[e]),
            anchor_update_id=anchor.L, anchor_class=anchor.cls, anchor_node=anchor.node,
            bids=frame(bid_items), asks=frame(ask_items),
            bids_complete=lo is None or (levels is not None and len(bid_items) == levels),
            asks_complete=hi is None or (levels is not None and len(ask_items) == levels),
            bid_window_floor=None if anchor.bid_floor is None else self._price(anchor.bid_floor),
            ask_window_ceiling=None if anchor.ask_ceiling is None else self._price(anchor.ask_ceiling))

    # ------------------------------------------------------------------ public API
    @property
    def start(self) -> pd.Timestamp:
        return _ts(self._t0)

    @property
    def end(self) -> pd.Timestamp:
        return _ts(self._t1)

    def book_at(self, ts: TimeLike, levels: Optional[int] = 10) -> BookView:
        """Top ``levels`` bids and asks after every event with event time at or before ``ts``
        (``levels=None`` returns every known level). Raises :class:`BookUnavailable` before the first
        anchor or inside a chain break."""
        t_ms = _to_ms(ts)
        e, a, reason = self._locate(t_ms)
        if e is None:
            raise BookUnavailable(f"no book at {_ts(t_ms).isoformat()}: {reason}")
        return self._view(t_ms, e, a, self._seek(e, a), levels)

    def _band(self, c: _Cursor, anchor: _Anchor, mid: float, bps: float) -> dict:
        scale = 10.0 ** self._scale
        lo, hi = mid * (1 - bps / 1e4) * scale, mid * (1 + bps / 1e4) * scale
        bk = np.fromiter(c.bids.keys(), dtype=np.int64, count=len(c.bids))
        bq = np.fromiter(c.bids.values(), dtype=np.float64, count=len(c.bids))
        ak = np.fromiter(c.asks.keys(), dtype=np.int64, count=len(c.asks))
        aq = np.fromiter(c.asks.values(), dtype=np.float64, count=len(c.asks))
        bm = bk.astype(np.float64) >= lo
        am = ak.astype(np.float64) <= hi
        return {
            "bps": bps,
            "bid_qty": float(bq[bm].sum()),
            "ask_qty": float(aq[am].sum()),
            "bid_notional": float((bk[bm].astype(np.float64) / scale * bq[bm]).sum()),
            "ask_notional": float((ak[am].astype(np.float64) / scale * aq[am]).sum()),
            "bid_levels": int(bm.sum()),
            "ask_levels": int(am.sum()),
            "bid_complete": anchor.bid_floor is None or anchor.bid_floor <= lo,
            "ask_complete": anchor.ask_ceiling is None or anchor.ask_ceiling >= hi,
        }

    def depth_at(self, ts: TimeLike, bps: Union[float, Sequence[float]] = (10, 50)) -> pd.DataFrame:
        """Resting quantity within ``bps`` basis points of the mid, per side, one row per band.

        ``*_qty`` is in the base asset, ``*_notional`` in the quote asset (price x qty). ``*_complete``
        is False when the band reaches past the anchor snapshot's 1,000-level window, where levels
        nobody touched since the snapshot aren't known."""
        bands = [float(b) for b in (bps if isinstance(bps, (list, tuple, np.ndarray)) else [bps])]
        if not bands or any(not (0 < b <= 10_000) for b in bands):
            raise ValueError("bps must be between 0 and 10000")
        t_ms = _to_ms(ts)
        e, a, reason = self._locate(t_ms)
        if e is None:
            raise BookUnavailable(f"no book at {_ts(t_ms).isoformat()}: {reason}")
        c = self._seek(e, a)
        best_bid, best_ask = self._best(c, self._anchors[a])
        if best_bid is None or best_ask is None:
            raise BookUnavailable(f"no trustworthy best bid and ask at {_ts(t_ms).isoformat()}: a side is empty "
                                  "or its best known level is outside the anchor snapshot's window")
        mid = (self._price(best_bid) + self._price(best_ask)) / 2
        df = pd.DataFrame([self._band(c, self._anchors[a], mid, b) for b in bands]).set_index("bps")
        df.insert(0, "mid", mid)
        df.attrs["time"] = _ts(t_ms)
        return df

    def _grid(self, freq: str, start: Optional[TimeLike], end: Optional[TimeLike]) -> np.ndarray:
        step = int(pd.Timedelta(freq) / pd.Timedelta(milliseconds=1))
        if step <= 0:
            raise ValueError("freq must be at least 1ms")
        t0 = max(self._t0, _to_ms(start)) if start is not None else self._t0
        t1 = min(self._t1, _to_ms(end)) if end is not None else self._t1
        if t1 <= t0:
            raise ValueError("end must be after start, inside the loaded days")
        if (t1 - t0) // step > _MAX_SAMPLES:
            raise ValueError(f"more than {_MAX_SAMPLES:,} samples; use a coarser freq or a shorter window")
        return np.arange(t0, t1, step, dtype=np.int64)

    def spread_series(self, freq: str = "1s", start: Optional[TimeLike] = None, end: Optional[TimeLike] = None,
                      depth_bps: Optional[Sequence[float]] = None) -> pd.DataFrame:
        """Best bid, best ask, mid, spread and spread in bps sampled every ``freq`` (pandas offset, e.g.
        ``"1s"``, ``"1min"``) from ``start`` to ``end`` (default: the whole loaded range). Rows where no
        trustworthy book exists are NaN. With ``depth_bps=[10, 50]`` it adds ``bid_depth_10bps``,
        ``ask_depth_10bps`` and so on, in the base asset, each with a ``..._complete`` column. Depth outside
        the anchor snapshot's known price window is partial: a sample whose ``..._complete`` is False is a
        lower bound, so exclude those from full-depth statistics."""
        times = self._grid(freq, start, end)
        n = len(times)
        bid = np.full(n, np.nan)
        ask = np.full(n, np.nan)
        bands = [float(b) for b in depth_bps] if depth_bps else []
        depth = {f"{s}_depth_{b:g}bps": np.full(n, np.nan) for b in bands for s in ("bid", "ask")}
        complete = {f"{s}_depth_{b:g}bps_complete": np.zeros(n, dtype=bool) for b in bands for s in ("bid", "ask")}
        for i, t_ms in enumerate(times.tolist()):
            e, a, _ = self._locate(t_ms)
            if e is None:
                continue
            c = self._seek(e, a)
            best_bid, best_ask = self._best(c, self._anchors[a])
            if best_bid is None or best_ask is None:
                continue
            bid[i] = self._price(best_bid)
            ask[i] = self._price(best_ask)
            for b in bands:
                band = self._band(c, self._anchors[a], (bid[i] + ask[i]) / 2, b)
                depth[f"bid_depth_{b:g}bps"][i] = band["bid_qty"]
                depth[f"ask_depth_{b:g}bps"][i] = band["ask_qty"]
                complete[f"bid_depth_{b:g}bps_complete"][i] = band["bid_complete"]
                complete[f"ask_depth_{b:g}bps_complete"][i] = band["ask_complete"]
        mid = (bid + ask) / 2
        out = pd.DataFrame({"bid": bid, "ask": ask, "mid": mid, "spread": ask - bid,
                            "spread_bps": (ask - bid) / mid * 1e4, **depth, **complete},
                           index=pd.DatetimeIndex(pd.to_datetime(times, unit="ms", utc=True), name="time"))
        return out

    def biggest_move(self, start: Optional[TimeLike] = None, end: Optional[TimeLike] = None) -> Optional[dict]:
        """The largest mid-price change over one minute, between two consecutive minute marks (hh:mm:00)
        that both have a trustworthy book on the same unbroken stretch of the update chain. A minute that
        spans a break or an unavailable close never counts, so a jump across a gap can't pass for a
        one-minute move. Returns None when no minute qualifies.

        ``minute_start``/``minute_end`` label the interval; ``book_time`` (= ``minute_end``) is a moment with
        a trustworthy book inside the loaded days, safe to pass to ``book_at`` or ``depth_at``."""
        t0 = max(self._t0, _to_ms(start)) if start is not None else self._t0
        t0 = -(-t0 // 60_000) * 60_000
        t1 = min(self._t1, _to_ms(end)) if end is not None else self._t1
        if t1 - t0 < 60_000:
            return None
        s = self.spread_series("1min", _ts(t0), _ts(t1))
        times = self._grid("1min", _ts(t0), _ts(t1))
        seg = np.array([self._seg[self._event_at(int(t))] if not np.isnan(m) else -1
                        for t, m in zip(times.tolist(), s["mid"].to_numpy())], dtype=np.int64)
        mid = s["mid"].to_numpy()
        ok = (seg[1:] >= 0) & (seg[:-1] >= 0) & (seg[1:] == seg[:-1]) & (np.diff(times) == 60_000)
        if not ok.any():
            return None
        moves = np.where(ok, (mid[1:] - mid[:-1]) / mid[:-1] * 1e4, np.nan)
        i = int(np.nanargmax(np.abs(moves)))
        return {"minute_start": _ts(times[i]), "minute_end": _ts(times[i + 1]), "book_time": _ts(times[i + 1]),
                "bps": float(moves[i]), "from_mid": float(mid[i]), "to_mid": float(mid[i + 1]),
                "eligible_minutes": int(ok.sum())}

    def iterate(self, start: Optional[TimeLike] = None, end: Optional[TimeLike] = None,
                every: Optional[str] = None, levels: Optional[int] = 10) -> Iterator[BookView]:
        """Yield the book every ``every`` (pandas offset) between ``start`` and ``end``, skipping moments
        with no trustworthy book. Without ``every`` it yields after each diff event, which is thorough
        but slow on BTC (about 850,000 events a day)."""
        if every is not None:
            for t_ms in self._grid(every, start, end).tolist():
                e, a, _ = self._locate(t_ms)
                if e is not None:
                    yield self._view(t_ms, e, a, self._seek(e, a), levels)
            return
        t0 = max(self._t0, _to_ms(start)) if start is not None else self._t0
        t1 = min(self._t1, _to_ms(end)) if end is not None else self._t1
        first = int(np.searchsorted(self._Ecum, t0, side="left"))
        last = int(np.searchsorted(self._Ecum, t1, side="left"))
        for e in range(first, last):
            a = self._anchor_for(e)
            if a is None:
                continue
            yield self._view(int(self._E[e]), e, a, self._seek(e, a), levels)

    def segments(self) -> pd.DataFrame:
        """One row per unbroken stretch of the update chain: when it starts and ends, how many events
        it has, and when the first usable snapshot anchors it (before that there's no book)."""
        rows = []
        bounds = np.flatnonzero(np.r_[True, self._seg[1:] != self._seg[:-1], True])
        for s0, s1 in zip(bounds[:-1], bounds[1:]):
            seg = int(self._seg[s0])
            idx = np.flatnonzero(self._aseg == seg)
            first_anchor = self._anchors[idx[0]] if len(idx) else None
            anchored_from = _ts(self._E[first_anchor.j]) if first_anchor else pd.NaT
            rows.append({
                "segment": seg,
                "first_event": _ts(self._E[s0]),
                "last_event": _ts(self._E[s1 - 1]),
                "events": int(s1 - s0),
                "snapshots": int(len(idx)),
                "anchored_from": anchored_from,
                "starts_with_reset": bool(self._pu[s0] < 0),
            })
        return pd.DataFrame(rows)

    def __repr__(self) -> str:
        span = self.days[0].isoformat() if len(self.days) == 1 else f"{self.days[0]}..{self.days[-1]}"
        return f"L2Book({span}, events={self._n:,}, snapshots={len(self._anchors)}, segments={int(self._seg[-1]) + 1})"
