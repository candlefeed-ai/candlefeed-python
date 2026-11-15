"""Write CandleFeed history into a Freqtrade data directory.

    candlefeed-freqtrade --pairs BTC/USDT:USDT ETH/USDT:USDT --timeframes 1h --days 30 \\
        --datadir user_data/data/binance

Files follow Freqtrade's own data handlers (checked against freqtrade 2026.10,
``freqtrade/data/history/datahandlers/`` and ``freqtrade/candle_columns.py``):

- spot candles: ``<datadir>/BTC_USDT-1h.feather``
- futures candles: ``<datadir>/futures/BTC_USDT_USDT-1h-futures.feather``
- funding rates: ``<datadir>/futures/BTC_USDT_USDT-1h-funding_rate.feather`` (``date, funding_rate``)
- mark price: ``<datadir>/futures/BTC_USDT_USDT-1h-mark.feather``: one row per funding settlement, with
  open = high = low = close = the mark price Binance reported for that settlement and volume 0. It is not
  an hourly mark-price candle series; Freqtrade's backtester only reads ``open`` at settlement times. Rows already
  in a mark file (an hourly one Freqtrade downloaded, say) are never changed: only settlements missing from it are
  added, and a settlement row with no usable ``open`` is refused rather than overwritten.
- open interest (opt-in): ``<datadir>/futures/BTC_USDT_USDT-1h-open_interest.feather``
  (``date, open_interest_amount, open_interest_value``)
"""
from __future__ import annotations

import argparse
import math
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple, Union

import pandas as pd

from .client import _CLIENT_VERSION, OHLCV_INTERVALS, OPEN_INTEREST_INTERVALS, CandleFeed
from .exceptions import AuthenticationError, CandleFeedError, RateLimitError, TierRestrictedError

__all__ = [
    "EXCHANGES",
    "FORMATS",
    "TIMEFRAMES",
    "ExportResult",
    "FileReport",
    "export_freqtrade",
    "freqtrade_pair",
    "main",
]

SIGNUP_URL = "https://candlefeed.ai/signup?utm_source=freqtrade&utm_medium=client"
PRICING_URL = "https://candlefeed.ai/pricing/?utm_source=freqtrade&utm_medium=client"
USER_AGENT_MARKER = "candlefeed-freqtrade"

# CandleFeed exchange -> (Freqtrade exchange directory, trading mode)
EXCHANGES: Dict[str, Tuple[str, str]] = {
    "binance": ("binance", "futures"),
    "binance_spot": ("binance", "spot"),
}
TIMEFRAMES = OHLCV_INTERVALS
FORMATS = ("feather", "parquet")
# Freqtrade's Binance futures backtest reads funding_rate and mark files at this timeframe
# (Exchange._ft_has defaults "funding_fee_timeframe" and "mark_ohlcv_timeframe").
FUNDING_TIMEFRAME = "1h"

_OHLCV_COLUMNS = ["date", "open", "high", "low", "close", "volume"]
_FUNDING_COLUMNS = ["date", "funding_rate"]
_OI_COLUMNS = ["date", "open_interest_amount", "open_interest_value"]
_QUOTES = ("USDT", "USDC", "FDUSD", "BUSD", "TUSD", "BTC", "ETH", "BNB")
# CandleFeed stores these Binance perps in per-coin units (collectors/config.py BINANCE_SYMBOL_ALIAS), while
# Freqtrade's market is the listed 1000-coin contract, so prices and volumes wouldn't match its market data.
_PER_COIN_PERPS = {"PEPEUSDT": "1000PEPE/USDT:USDT"}
_TF_DELTA = {"1m": timedelta(minutes=1), "5m": timedelta(minutes=5), "15m": timedelta(minutes=15),
             "1h": timedelta(hours=1), "4h": timedelta(hours=4), "1d": timedelta(days=1)}
_UPGRADE_RE = re.compile(r"\s*Upgrade:\s*\S+")
_URL_RE = re.compile(r"\s*https?://\S+")


class ExportError(Exception):
    """A problem the user can fix: bad pair, timeframe or range. The message says what to change."""


class _Refused(ExportError):
    """Writing this file would lose history or give Freqtrade wrong funding, so it's left as it is."""


@dataclass
class FileReport:
    path: Path
    kind: str
    pair: str
    rows_written: int
    rows_fetched: int
    first: Optional[pd.Timestamp] = None
    last: Optional[pd.Timestamp] = None
    missing: int = 0
    starts_late: bool = False
    notes: List[str] = field(default_factory=list)


@dataclass
class ExportResult:
    datadir: Path
    trading_mode: str
    files: List[FileReport] = field(default_factory=list)
    error: Optional[str] = None
    refused: List[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.error is None and not self.refused


def freqtrade_pair(pair: str, exchange: str) -> Tuple[str, str]:
    """(CandleFeed symbol, Freqtrade pair) for ``BTCUSDT``, ``BTC/USDT`` or ``BTC/USDT:USDT``."""
    if exchange not in EXCHANGES:
        raise ExportError(f"Unsupported exchange {exchange!r}. Supported: {', '.join(EXCHANGES)}.")
    futures = EXCHANGES[exchange][1] == "futures"
    text = pair.strip().upper()
    if "/" in text:
        base, _, rest = text.partition("/")
        quote, _, settle = rest.partition(":")
        if futures and settle and settle != quote:
            raise ExportError(f"{pair}: only linear USD-M perpetuals (settled in the quote coin) are supported.")
        if not futures and settle:
            raise ExportError(f"{pair} is a futures pair; use exchange 'binance' for perpetuals.")
    else:
        quote = next((q for q in _QUOTES if text.endswith(q) and len(text) > len(q)), None)
        if quote is None:
            raise ExportError(f"Can't split {pair!r} into base and quote; pass it as BASE/QUOTE, e.g. BTC/USDT.")
        base = text[: -len(quote)]
    if not base or not quote or not re.fullmatch(r"[A-Z0-9]+", base + quote):
        raise ExportError(f"Can't read pair {pair!r}; pass it as BASE/QUOTE, e.g. BTC/USDT.")
    symbol = base + quote
    if futures and (symbol in _PER_COIN_PERPS or base.startswith("1000")):
        listed = _PER_COIN_PERPS.get(symbol, pair)
        raise ExportError(
            f"{pair}: Binance lists this perpetual as {listed} (1,000-coin contracts) and CandleFeed stores it in "
            "per-coin units, so its prices and volumes wouldn't match Freqtrade's market. Not exported.")
    return symbol, (f"{base}/{quote}:{quote}" if futures else f"{base}/{quote}")


def _filename(datadir: Path, pair: str, timeframe: str, candle_type: str, data_format: str) -> Path:
    pair_s = pair
    for ch in ["/", " ", ".", "@", "$", "+", ":"]:
        pair_s = pair_s.replace(ch, "_")
    if candle_type == "spot":
        return datadir / f"{pair_s}-{timeframe}.{data_format}"
    return datadir / "futures" / f"{pair_s}-{timeframe}-{candle_type}.{data_format}"


def _store(df: pd.DataFrame, path: Path, data_format: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    df = df.reset_index(drop=True)
    if data_format == "feather":
        df.to_feather(tmp, compression_level=9, compression="lz4")
    else:
        df.to_parquet(tmp)
    os.replace(tmp, path)


_LEAVE_IT = "It was left untouched: move it out of the data directory, or pass --no-incremental to replace it."


def _load(path: Path, data_format: str, columns: List[str], step: Optional[timedelta] = None) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    df = pd.read_feather(path) if data_format == "feather" else pd.read_parquet(path)
    if columns == _FUNDING_COLUMNS and "funding_rate" not in df.columns and set(_OHLCV_COLUMNS).issubset(df.columns):
        # Freqtrade's pre-2026.8 funding layout keeps the rate in "open"; Freqtrade still reads it the same way.
        df = df.rename(columns={"open": "funding_rate"})
    if not set(columns).issubset(df.columns):
        raise _Refused(f"{path} isn't in the layout Freqtrade uses for this file (columns: "
                       f"{', '.join(map(str, df.columns))}). {_LEAVE_IT}")
    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        raise _Refused(f"{path} stores its dates as {df['date'].dtype}, not as timestamps. {_LEAVE_IT}")
    df = df.loc[:, columns].copy()
    df["date"] = pd.to_datetime(df["date"], utc=True)
    if columns == _FUNDING_COLUMNS:
        # Freqtrade floors funding times to the second when it loads them; CandleFeed's are floored the same way.
        df = df.sort_values("date", kind="stable")
        df["date"] = df["date"].dt.floor("s")
    if step is not None and ((df["date"] - pd.Timestamp(0, tz="UTC")) % step != pd.Timedelta(0)).any():
        raise _Refused(f"{path} has rows that don't start on a {path.stem.split('-')[1]} boundary. {_LEAVE_IT}")
    if df.empty:
        return None
    return _merge(None, df)


def _merge(old: Optional[pd.DataFrame], new: pd.DataFrame) -> pd.DataFrame:
    if old is None or old.empty:
        out = new
    elif new.empty:
        out = old
    else:
        out = pd.concat([old, new], ignore_index=True)
    return out.drop_duplicates("date", keep="last").sort_values("date").reset_index(drop=True)


def _ceil(ts: datetime, step: timedelta) -> datetime:
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    n = math.ceil((ts - epoch) / step)
    return epoch + n * step


def _parse_timerange(text: str) -> Tuple[datetime, Optional[datetime]]:
    m = re.fullmatch(r"(\d{8})?-(\d{8})?", text.strip())
    if not m or not m.group(1):
        raise ExportError(f"--timerange {text!r}: use YYYYMMDD-YYYYMMDD or YYYYMMDD- (start required).")

    def day(s: str) -> datetime:
        return datetime.strptime(s, "%Y%m%d").replace(tzinfo=timezone.utc)

    start = day(m.group(1))
    end = day(m.group(2)) if m.group(2) else None
    if end is not None and end <= start:
        raise ExportError(f"--timerange {text!r}: the end date must be after the start date.")
    return start, end


def _window(days: Optional[float], timerange: Optional[str], now: datetime) -> Tuple[datetime, Optional[datetime]]:
    if timerange:
        return _parse_timerange(timerange)
    days = 30 if days is None else days
    if days <= 0:
        raise ExportError("--days must be more than 0.")
    # A few minutes inside the window and on a whole hour, so a 30-day request on a free key isn't
    # refused for asking a second further back than the server's own 30-day line.
    return _ceil(now - timedelta(days=days) + timedelta(minutes=5), timedelta(hours=1)), None


def _gap_count(dates: pd.Series, step: timedelta) -> int:
    if len(dates) < 2:
        return 0
    expected = int((dates.iloc[-1] - dates.iloc[0]) / step) + 1
    return max(expected - dates.nunique(), 0)


def _funding_gaps(frame: pd.DataFrame) -> int:
    if len(frame) < 2:
        return 0
    times = frame.index.to_series()
    hours = pd.to_numeric(frame["interval_hours"], errors="coerce")
    missing = 0
    for delta, interval in zip(times.diff().iloc[1:], hours.iloc[1:]):
        if pd.isna(interval) or interval <= 0:
            continue
        missing += max(int(round(delta / timedelta(hours=float(interval)))) - 1, 0)
    return missing


def _fetch_start(old: Optional[pd.DataFrame], start: datetime, step: timedelta) -> datetime:
    """Incremental mode: when the file already covers the start, refetch from its last row (which may have been
    written before it was final) onwards."""
    if old is None or old.empty:
        return start
    first, last = old["date"].iloc[0].to_pydatetime(), old["date"].iloc[-1].to_pydatetime()
    if first <= _ceil(start, step) and last >= start:
        return last
    return start


def _drop_open(old: Optional[pd.DataFrame], step: timedelta, now: datetime) -> Tuple[Optional[pd.DataFrame], int]:
    if old is None:
        return None, 0
    closed = old[old["date"] + step <= pd.Timestamp(now)].reset_index(drop=True)
    return closed, len(old) - len(closed)


def _when(ts: pd.Timestamp) -> str:
    return f"{ts:%Y-%m-%d %H:%M} UTC"


class _Exporter:
    def __init__(self, cf: CandleFeed, datadir: Path, exchange: str, data_format: str, incremental: bool,
                 now: datetime) -> None:
        self.cf = cf
        self.datadir = datadir
        self.exchange = exchange
        self.data_format = data_format
        self.incremental = incremental
        self.now = now

    def candles(self, symbol: str, pair: str, timeframe: str, candle_type: str, start: datetime,
                end: Optional[datetime]) -> FileReport:
        step = _TF_DELTA[timeframe]
        path = _filename(self.datadir, pair, timeframe, candle_type, self.data_format)
        report = FileReport(path, candle_type, pair, 0, 0)
        old, dropped = _drop_open(self._existing(path, _OHLCV_COLUMNS, step), step, self.now)
        if dropped:
            report.notes.append(f"removed {dropped} candle(s) on disk that were still open at export time")
        fetch_from = _fetch_start(old, start, step)
        if end is not None and fetch_from >= end:
            report.notes.append("already on disk, nothing fetched")
            return self._finish(report, old, _empty(_OHLCV_COLUMNS), step, rewrite=bool(dropped))
        if fetch_from > self.now - step:
            report.notes.append("already on disk up to the last closed candle, nothing fetched")
            return self._finish(report, old, _empty(_OHLCV_COLUMNS), step, rewrite=bool(dropped))
        query_end = None if end is None else end - timedelta(microseconds=1)
        raw = self.cf.get_ohlcv(symbol, exchange=self.exchange, interval=timeframe, start=fetch_from, end=query_end)
        closed = raw[raw.index + step <= self.now]
        if len(closed) < len(raw):
            report.notes.append(f"dropped {len(raw) - len(closed)} candle(s) still open at export time")
        frame = pd.DataFrame({
            "date": closed.index,
            "open": closed["open"].astype("float64").to_numpy(),
            "high": closed["high"].astype("float64").to_numpy(),
            "low": closed["low"].astype("float64").to_numpy(),
            "close": closed["close"].astype("float64").to_numpy(),
            "volume": closed["volume"].astype("float64").to_numpy(),
        })
        frame = frame.dropna(subset=["open", "high", "low", "close"])
        report.rows_fetched = len(frame)
        if not frame.empty and fetch_from == start and frame["date"].iloc[0] > pd.Timestamp(start) + step:
            report.starts_late = True
        return self._finish(report, old, frame, step, rewrite=bool(dropped))

    def funding(self, symbol: str, pair: str, start: datetime,
                end: Optional[datetime]) -> Tuple[FileReport, FileReport]:
        """Funding-rate and mark files are written as a pair, and only when every settlement in the merged funding-rate
        file has a finite rate and a finite, positive mark price at the same time in the merged mark file. Otherwise
        Freqtrade would drop the settlement or price it with another settlement's mark, so neither file is touched.
        Existing mark rows are kept as they are (Freqtrade's own downloads hold hourly mark candles, and it only
        charges at funding-rate rows); CandleFeed's settlement mark only fills settlement times the file lacks. The
        whole range is refetched on every run; it's three rows a day for an 8h contract."""
        fr_path = _filename(self.datadir, pair, FUNDING_TIMEFRAME, "funding_rate", self.data_format)
        mk_path = _filename(self.datadir, pair, FUNDING_TIMEFRAME, "mark", self.data_format)
        old_fr = self._existing(fr_path, _FUNDING_COLUMNS)
        old_mk = self._existing(mk_path, _OHLCV_COLUMNS)
        if old_fr is not None:
            _refuse_broken_marks(pair, mk_path, old_mk, old_fr["date"])
        bad = _unpriced(old_fr, old_mk)
        outside = bad[(bad < pd.Timestamp(start)) | (bad >= pd.Timestamp(end) if end is not None else False)]
        if not outside.empty:
            raise _Refused(
                _unpriced_message(pair, fr_path, mk_path, outside) + " They're outside the requested range, so neither "
                "file was changed. Export a --timerange that covers them to repair both files, or pass "
                "--no-incremental to rebuild both from this range.")
        query_end = None if end is None else end - timedelta(microseconds=1)
        raw = self.cf.get_funding_rates(symbol, exchange=self.exchange, start=start, end=query_end)
        raw = raw.dropna(subset=["funding_rate"])
        dates = raw.index.floor("s")
        mark = _floats(raw["mark_price"])
        # Only settlements the mark file lacks take CandleFeed's mark; a row already there is kept (and checked below).
        adding = ~dates.isin(old_mk["date"] if old_mk is not None else [])
        no_mark = dates[adding & ~_usable_mark(mark).to_numpy()]
        if len(no_mark):
            raise _Refused(
                f"{pair}: {len(no_mark)} funding settlement(s) between {_when(no_mark[0])} and {_when(no_mark[-1])} "
                "have no mark price in CandleFeed and none in the mark file. Freqtrade would leave them out or charge "
                "them at another settlement's price, so this pair's funding-rate and mark files weren't written. Use a "
                "--timerange that avoids those settlements, or --no-funding to export candles only.")
        fr = pd.DataFrame({"date": dates, "funding_rate": _floats(raw["funding_rate"]).to_numpy()})
        mk = pd.DataFrame({"date": dates, "open": mark.to_numpy(), "high": mark.to_numpy(), "low": mark.to_numpy(),
                           "close": mark.to_numpy(), "volume": 0.0})[adding].reset_index(drop=True)
        merged_fr = _merge(old_fr, fr)
        _refuse_broken_marks(pair, mk_path, old_mk, merged_fr["date"])
        still = _unpriced(merged_fr, _merge(old_mk, mk))
        if not still.empty:
            raise _Refused(
                _unpriced_message(pair, fr_path, mk_path, still) + " CandleFeed didn't return those settlements, so "
                "neither file was changed. Pass --no-incremental to rebuild both from this range.")
        fr_report = FileReport(fr_path, "funding_rate", pair, 0, len(fr))
        mk_report = FileReport(mk_path, "mark", pair, 0, len(mk))
        fr_report.missing = _funding_gaps(raw)
        intervals = sorted({float(h) for h in pd.to_numeric(raw["interval_hours"], errors="coerce").dropna()})
        if intervals:
            fr_report.notes.append("settlement interval(s): " + ", ".join(f"{h:g}h" for h in intervals))
        # Mark first: if the second write fails, the extra mark rows are harmless and the next run repairs the pair.
        mk_report = self._finish(mk_report, old_mk, mk, None)
        return self._finish(fr_report, old_fr, fr, None), mk_report

    def open_interest(self, symbol: str, pair: str, timeframe: str, start: datetime,
                      end: Optional[datetime]) -> FileReport:
        step = _TF_DELTA[timeframe]
        path = _filename(self.datadir, pair, timeframe, "open_interest", self.data_format)
        report = FileReport(path, "open_interest", pair, 0, 0)
        old, dropped = self._existing(path, _OI_COLUMNS), 0
        if timeframe != "5m":
            old, dropped = _drop_open(old, step, self.now)
        if dropped:
            report.notes.append(f"removed {dropped} bucket(s) on disk that were still open at export time")
        fetch_from = _fetch_start(old, start, step)
        if (end is not None and fetch_from >= end) or fetch_from > self.now - step:
            report.notes.append("already on disk, nothing fetched")
            return self._finish(report, old, _empty(_OI_COLUMNS), step, rewrite=bool(dropped))
        query_end = None if end is None else end - timedelta(microseconds=1)
        raw = self.cf.get_open_interest(symbol, exchange=self.exchange, interval=timeframe, start=fetch_from,
                                        end=query_end)
        if timeframe != "5m":
            closed = raw[raw.index + step <= self.now]
            if len(closed) < len(raw):
                report.notes.append(f"dropped {len(raw) - len(closed)} bucket(s) still open at export time")
            raw = closed
        frame = pd.DataFrame({
            "date": raw.index,
            "open_interest_amount": raw["open_interest"].astype("float64").to_numpy(),
            "open_interest_value": raw["open_interest_value"].astype("float64").to_numpy(),
        })
        report.rows_fetched = len(frame)
        return self._finish(report, old, frame, step, rewrite=bool(dropped))

    def _existing(self, path: Path, columns: List[str], step: Optional[timedelta] = None) -> Optional[pd.DataFrame]:
        return _load(path, self.data_format, columns, step) if self.incremental else None

    def _finish(self, report: FileReport, old: Optional[pd.DataFrame], new: pd.DataFrame,
                step: Optional[timedelta], rewrite: bool = False) -> FileReport:
        merged = _merge(old, new)
        if merged.empty and not self.incremental and report.path.exists():
            raise _Refused(f"{report.path}: CandleFeed returned no rows for this range, so --no-incremental had "
                           "nothing to replace it with. It was left untouched; check the pair and --timerange.")
        if merged.empty:
            if rewrite:
                _store(merged, report.path, self.data_format)
            report.notes.append("no rows in this range; no file written")
            return report
        if not new.empty or old is None or rewrite:
            _store(merged, report.path, self.data_format)
        report.rows_written = len(merged)
        report.first, report.last = merged["date"].iloc[0], merged["date"].iloc[-1]
        if step is not None:
            report.missing = _gap_count(merged["date"], step)
        return report


def _unpriced(fr: Optional[pd.DataFrame], mk: Optional[pd.DataFrame]) -> pd.Series:
    """Funding-rate rows Freqtrade would charge wrongly: the rate isn't finite, or the mark file has no finite,
    positive ``open`` at that time. Sorted settlement times."""
    if fr is None:
        return pd.Series(dtype="datetime64[ns, UTC]")
    rate = _floats(fr["funding_rate"]).abs().lt(math.inf).to_numpy()
    priced = [False] * len(fr)
    if mk is not None:
        priced = fr["date"].isin(mk.loc[_usable_mark(_floats(mk["open"])).to_numpy(), "date"]).to_numpy()
    return fr.loc[~(rate & priced), "date"].sort_values().reset_index(drop=True)


def _refuse_broken_marks(pair: str, mk_path: Path, mk: Optional[pd.DataFrame], settlements: pd.Series) -> None:
    """Rows already in a mark file are never replaced, so one at a settlement time with no usable open can't be
    repaired by an incremental run."""
    if mk is None:
        return
    kept = mk[mk["date"].isin(settlements)]
    broken = kept.loc[~_usable_mark(_floats(kept["open"])).to_numpy(), "date"].reset_index(drop=True)
    if not broken.empty:
        raise _Refused(
            f"{pair}: {mk_path.name} already has {len(broken)} row(s) at settlement times, from "
            f"{_when(broken.iloc[0])} to {_when(broken.iloc[-1])}, with no finite, positive open, so Freqtrade "
            "would charge wrong funding there. The exporter never changes rows already in a mark file, so neither "
            "file was changed. Move the mark file out of the data directory, or pass --no-incremental to rebuild "
            "both from this range.")


def _floats(values: pd.Series) -> pd.Series:
    """Plain float64 with NaN for anything missing or non-numeric, so comparisons are never pandas NA."""
    return pd.Series(pd.to_numeric(values, errors="coerce").to_numpy(dtype="float64", na_value=math.nan),
                     index=values.index)


def _usable_mark(mark: pd.Series) -> pd.Series:
    return mark.gt(0) & mark.lt(math.inf)


def _unpriced_message(pair: str, fr_path: Path, mk_path: Path, dates: pd.Series) -> str:
    return (f"{pair}: {len(dates)} settlement(s) in {fr_path.name}, from {_when(dates.iloc[0])} to "
            f"{_when(dates.iloc[-1])}, have no finite rate or no finite, positive mark price at the same time in "
            f"{mk_path.name}, so Freqtrade would charge wrong funding there.")


def _empty(columns: List[str]) -> pd.DataFrame:
    frame = pd.DataFrame({c: pd.Series(dtype="float64") for c in columns})
    frame["date"] = pd.Series(dtype="datetime64[ns, UTC]")
    return frame.loc[:, columns]


def _client(api_key: Union[str, CandleFeed, None]) -> CandleFeed:
    cf = api_key if isinstance(api_key, CandleFeed) else CandleFeed(api_key=api_key)
    ua = cf._session.headers.get("User-Agent", f"candlefeed-python/{_CLIENT_VERSION}")
    if USER_AGENT_MARKER not in ua:
        cf._session.headers["User-Agent"] = f"{ua} {USER_AGENT_MARKER}"
    return cf


def _tier_message(exc: TierRestrictedError) -> str:
    reason = _UPGRADE_RE.sub("", exc.message or "").strip()
    return f"CandleFeed refused this request for your plan: {reason}\nPlans: {PRICING_URL}"


def export_freqtrade(
    pairs: Sequence[str],
    api_key: Union[str, CandleFeed, None] = None,
    exchange: str = "binance",
    timeframes: Sequence[str] = ("1h",),
    datadir: Union[str, os.PathLike] = "user_data/data/binance",
    days: Optional[float] = None,
    timerange: Optional[str] = None,
    data_format: str = "feather",
    funding: bool = True,
    open_interest: bool = False,
    incremental: bool = True,
    now: Optional[datetime] = None,
) -> ExportResult:
    """Download CandleFeed history and write it as Freqtrade data files.

    ``pairs`` take ``BTCUSDT``, ``BTC/USDT`` or ``BTC/USDT:USDT``. ``exchange`` is ``binance`` (USD-M
    perpetuals, written as Freqtrade futures data with funding-rate and mark files) or ``binance_spot``.
    The range is the last ``days`` (default 30) or a Freqtrade-style ``timerange`` (``20261001-20261101``,
    end exclusive). Candles still open at export time aren't written; missing candles are counted, never filled.

    A plan limit (HTTP 403 ``tier_restricted``) or the daily request limit stops the export: files already
    written stay, and ``ExportResult.error`` says what happened. Other API errors are raised. A file that can't be
    written without losing existing history or giving Freqtrade wrong funding (an existing file in another layout,
    funding settlements without a mark price) is left as it is and explained in ``ExportResult.refused``; the rest
    of the export carries on. Every file is written to a temporary name and then renamed into place.
    """
    if exchange not in EXCHANGES:
        raise ExportError(f"Unsupported exchange {exchange!r}. Supported: {', '.join(EXCHANGES)}.")
    if data_format not in FORMATS:
        raise ExportError(f"Unsupported format {data_format!r}. Supported: {', '.join(FORMATS)}.")
    bad = [t for t in timeframes if t not in TIMEFRAMES]
    if bad or not timeframes:
        raise ExportError(f"Unsupported timeframe(s) {', '.join(bad) or '(none)'}. CandleFeed serves: "
                          f"{', '.join(TIMEFRAMES)}.")
    if not pairs:
        raise ExportError("Pass at least one pair.")
    trading_mode = EXCHANGES[exchange][1]
    if open_interest and trading_mode != "futures":
        raise ExportError("Open interest exists for perpetuals only (exchange 'binance').")
    oi_timeframes = [t for t in timeframes if t in OPEN_INTEREST_INTERVALS]
    if open_interest and not oi_timeframes:
        raise ExportError(f"Open interest timeframes: {', '.join(OPEN_INTEREST_INTERVALS)}.")
    try:
        import pyarrow  # noqa: F401
    except ImportError:
        raise ExportError('Writing Freqtrade files needs pyarrow: pip install "candlefeed[freqtrade]"') from None

    resolved = [freqtrade_pair(p, exchange) for p in pairs]
    if api_key is None and not os.environ.get("CANDLEFEED_API_KEY"):
        raise ExportError(f"No API key. Pass --api-key or set CANDLEFEED_API_KEY. Get a free key (no card): "
                          f"{SIGNUP_URL}")
    now = now or datetime.now(timezone.utc)
    start, end = _window(days, timerange, now)
    cf = _client(api_key)
    result = ExportResult(Path(datadir), trading_mode)
    exporter = _Exporter(cf, Path(datadir), exchange, data_format, incremental, now)
    candle_type = "futures" if trading_mode == "futures" else "spot"
    jobs = []
    for symbol, pair in resolved:
        jobs += [(exporter.candles, (symbol, pair, tf, candle_type, start, end)) for tf in timeframes]
        if trading_mode == "futures" and funding:
            jobs.append((exporter.funding, (symbol, pair, start, end)))
        if open_interest:
            jobs += [(exporter.open_interest, (symbol, pair, tf, start, end)) for tf in oi_timeframes]
    try:
        for job, args in jobs:
            try:
                out = job(*args)
            except _Refused as exc:
                result.refused.append(str(exc))
                continue
            result.files.extend(out if isinstance(out, tuple) else [out])
    except TierRestrictedError as exc:
        result.error = _tier_message(exc)
    except RateLimitError as exc:
        when = f" It resets at {exc.reset_at:%Y-%m-%d %H:%M} UTC." if exc.reset_at else ""
        result.error = (f"CandleFeed's request limit for your key was reached: {exc.message}{when} Files written "
                        f"so far are kept; run the same command again later and it carries on from them. "
                        f"Higher limits: {PRICING_URL}")
    return result


def _format_report(result: ExportResult, out=None) -> None:
    out = out or sys.stdout
    for f in result.files:
        span = f"{f.first:%Y-%m-%d %H:%M} -> {f.last:%Y-%m-%d %H:%M} UTC" if f.first is not None else "-"
        line = f"{f.kind:<13} {f.pair:<18} {f.rows_written:>7} rows  {span}  {f.path}"
        print(line, file=out)
        if f.missing:
            what = "settlements" if f.kind == "funding_rate" else "candles"
            print(f"    {f.missing} {what} missing in CandleFeed's data for this range (not filled in)", file=out)
        if f.starts_late:
            print("    data starts later than requested (listing date or start of coverage)", file=out)
        for note in f.notes:
            print(f"    {note}", file=out)


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="candlefeed-freqtrade",
        description="Write CandleFeed history as Freqtrade data files, ready for freqtrade backtesting.",
    )
    p.add_argument("--pairs", "-p", nargs="+", required=True,
                   help="Pairs: BTC/USDT:USDT (perps), BTC/USDT (spot) or BTCUSDT.")
    p.add_argument("--exchange", default="binance", choices=sorted(EXCHANGES),
                   help="binance = USD-M perpetuals (futures files + funding); binance_spot = spot. Default binance.")
    p.add_argument("--timeframes", "-t", nargs="+", default=["1h"], choices=TIMEFRAMES, help="Default 1h.")
    p.add_argument("--datadir", default="user_data/data/binance",
                   help="Freqtrade data directory for the exchange. Default user_data/data/binance.")
    span = p.add_mutually_exclusive_group()
    span.add_argument("--days", type=float, help="Last N days (default 30; a free key covers 30).")
    span.add_argument("--timerange", help="Freqtrade-style YYYYMMDD-YYYYMMDD (end exclusive) or YYYYMMDD-.")
    p.add_argument("--data-format", default="feather", choices=FORMATS,
                   help="Freqtrade's dataformat_ohlcv. Default feather.")
    p.add_argument("--no-funding", action="store_true", help="Skip funding-rate and mark files (perps only).")
    p.add_argument("--open-interest", action="store_true",
                   help="Also write open-interest files (perps; Builder plan or above).")
    p.add_argument("--no-incremental", action="store_true",
                   help="Refetch the whole range and replace existing files, even ones in another layout.")
    p.add_argument("--api-key", help="CandleFeed API key. Default: the CANDLEFEED_API_KEY environment variable.")
    return p


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    try:
        result = export_freqtrade(
            args.pairs,
            api_key=args.api_key,
            exchange=args.exchange,
            timeframes=args.timeframes,
            datadir=args.datadir,
            days=args.days,
            timerange=args.timerange,
            data_format=args.data_format,
            funding=not args.no_funding,
            open_interest=args.open_interest,
            incremental=not args.no_incremental,
        )
    except ExportError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except AuthenticationError as exc:
        reason = _URL_RE.sub("", exc.message or "").strip()
        print(f"error: CandleFeed rejected the API key: {reason}\nGet a free key (no card): {SIGNUP_URL}",
              file=sys.stderr)
        return 2
    except CandleFeedError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    _format_report(result)
    sys.stdout.flush()
    for reason in result.refused:
        print(f"\nnot written: {reason}", file=sys.stderr)
    if result.error:
        print(f"\nstopped: {result.error}", file=sys.stderr)
    if not result.ok:
        return 3
    mode = result.trading_mode
    print(f"\nDone. Backtest with: freqtrade backtesting --datadir {result.datadir} "
          f"--timeframe {args.timeframes[0]}" + (" (config: \"trading_mode\": \"futures\", "
                                                 "\"margin_mode\": \"isolated\")" if mode == "futures" else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
