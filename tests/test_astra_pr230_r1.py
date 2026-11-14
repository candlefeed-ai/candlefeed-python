"""Offline adversarial regression tests for PR #230; source is deliberately unchanged.

Tests named ``test_r1_*`` encode required behavior and intentionally fail on the
reviewed branch. Freqtrade-dependent cases skip if the optional local install is
absent. No Exchange instance is initialized and no network is permitted.
"""
from __future__ import annotations

import ast
import re
import socket
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest
from conftest import ok

from candlefeed import freqtrade as ft

ROOT = Path(__file__).resolve().parents[3]
T0 = datetime(2026, 11, 13, tzinfo=timezone.utc)
NOW = T0 + timedelta(days=1, hours=12, minutes=15)
PAIR = "BTC/USDT:USDT"


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("PR230 review tests must not use the network")

    for name in ("connect", "connect_ex"):
        monkeypatch.setattr(socket.socket, name, forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)


def candle(at, price=100):
    return {"time": at.isoformat(), "open": price, "high": price + 2,
            "low": price - 1, "close": price + 1, "volume": 12.5,
            "quote_volume": 1250}


def settlements():
    return [{"time": (T0 + timedelta(hours=h, milliseconds=4)).isoformat(),
             "funding_rate": 0.01, "mark_price": price, "interval_hours": 8}
            for h, price in [(0, 100), (8, 200), (16, 300)]]


class Api:
    """Inclusive API bounds, opaque cursor paging, no HTTP transport."""
    def __init__(self, candles=None, funding=None, oi=None):
        self.rows = {"candles": candles if candles is not None else [candle(T0)],
                     "funding-rates": funding if funding is not None else settlements(),
                     "open-interest": oi if oi is not None else []}
        self.calls = []

    def __call__(self, url, params):
        endpoint = url.rsplit("/", 1)[-1]
        self.calls.append((endpoint, dict(params)))
        rows = self.rows[endpoint]
        if params.get("start"):
            rows = [r for r in rows if pd.Timestamp(r["time"]) >= pd.Timestamp(params["start"])]
        if params.get("end"):
            rows = [r for r in rows if pd.Timestamp(r["time"]) <= pd.Timestamp(params["end"])]
        offset = int(params.get("cursor", "row:0").split(":")[1])
        page = rows[offset:offset + 1000]
        more = offset + len(page) < len(rows)
        return ok(page, has_more=more, next_cursor=f"row:{offset + len(page)}" if more else None)


def export(client, tmp_path, **kwargs):
    kwargs.setdefault("now", NOW)
    kwargs.setdefault("timerange", "20261113-")
    return ft.export_freqtrade([PAIR], api_key=client, datadir=tmp_path, **kwargs)


def file(tmp_path, kind, fmt="feather"):
    return tmp_path / "futures" / f"BTC_USDT_USDT-1h-{kind}.{fmt}"


def write_candles(path, times, prices=None):
    prices = prices or [100] * len(times)
    frame = pd.DataFrame({"date": pd.to_datetime(times, utc=True),
                          **{c: [float(p) for p in prices] for c in ["open", "high", "low", "close"]},
                          "volume": 12.5})
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_feather(path)


def funding_calculation(tmp_path, fmt="feather", timerange=None):
    pytest.importorskip("freqtrade")
    from freqtrade.data.history import load_pair_history
    from freqtrade.enums import CandleType
    from freqtrade.exchange import Exchange

    # Matches Backtesting.load_bt_data_detail: funding is not filled; mark is.
    rates = load_pair_history(PAIR, "1h", tmp_path, candle_type=CandleType.FUNDING_RATE,
                              fill_up_missing=False, data_format=fmt, timerange=timerange)
    marks = load_pair_history(PAIR, "1h", tmp_path, candle_type=CandleType.MARK,
                              data_format=fmt, timerange=timerange)
    combined = Exchange.combine_funding_and_mark(rates, marks)
    # The helper only uses the combined frame, so constructing an exchange is unnecessary.
    fees = Exchange.calculate_funding_fees(None, combined, 1, True, T0, NOW)
    return combined, fees


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
@pytest.mark.parametrize("missing_index", [0, 1, 2], ids=["leading", "interior", "trailing"])
def test_r1_missing_mark_must_not_succeed_with_wrong_fees(client, fake_session, tmp_path, fmt, missing_index):
    rows = settlements()
    rows[missing_index]["mark_price"] = 0
    fake_session.handler(Api(funding=rows))
    result = export(client, tmp_path, data_format=fmt)
    if not result.ok:  # Refusing an inexact funding export is safe.
        return
    combined, fees = funding_calculation(tmp_path, fmt)
    # True marks were 100, 200, 300: three 1% settlements on one BTC cost 6.
    assert fees == pytest.approx(6), (f"Successful export charged {fees} instead of 6; "
                                    f"retained settlement dates: {combined['date'].tolist()}")


def test_r1_interrupted_mark_write_is_repaired_on_retry(client, fake_session, tmp_path, monkeypatch):
    fake_session.handler(Api(funding=settlements()[:1]))
    export(client, tmp_path)
    original_store = ft._store

    def interrupted_store(frame, path, fmt):
        if path == file(tmp_path, "mark"):
            raise OSError("simulated disk failure after funding-rate replace")
        original_store(frame, path, fmt)

    fake_session.handler(Api())
    with monkeypatch.context() as patch:
        patch.setattr(ft, "_store", interrupted_store)
        with pytest.raises(OSError, match="simulated disk failure"):
            export(client, tmp_path)
    fake_session.handler(Api())
    assert export(client, tmp_path).ok
    rates = pd.read_feather(file(tmp_path, "funding_rate"))
    marks = pd.read_feather(file(tmp_path, "mark"))
    assert set(rates.date) <= set(marks.date), "retry skipped the two settlements whose mark write failed"


def test_r1_existing_legacy_funding_history_is_not_deleted(client, fake_session, tmp_path):
    previous = T0 - timedelta(days=10)
    # Valid legacy Freqtrade funding layout, still readable by Freqtrade 2026.10.
    write_candles(file(tmp_path, "funding_rate"), [previous, T0], [0.01, 0.01])
    write_candles(file(tmp_path, "mark"), [previous, T0])
    fake_session.handler(Api())
    try:
        result = export(client, tmp_path)
    except ft.ExportError:
        result = None
    # Either convert and merge it or refuse it; do not erase pre-range history.
    assert previous in set(pd.read_feather(file(tmp_path, "funding_rate")).date), (
        f"legacy funding history was discarded; result={result}")
    assert previous in set(pd.read_feather(file(tmp_path, "mark")).date)


def test_r1_missing_companion_does_not_erase_existing_funding_history(client, fake_session, tmp_path):
    previous = T0 - timedelta(days=10)
    path = file(tmp_path, "funding_rate")
    path.parent.mkdir(parents=True)
    pd.DataFrame({"date": pd.to_datetime([previous, T0], utc=True),
                  "funding_rate": [0.01, 0.01]}).to_feather(path)
    fake_session.handler(Api())
    try:
        export(client, tmp_path)
    except ft.ExportError:
        pass
    assert previous in set(pd.read_feather(path).date), "missing mark file erased valid funding history"


def test_r1_foreign_schema_is_not_silently_overwritten(client, fake_session, tmp_path):
    path = file(tmp_path, "futures")
    path.parent.mkdir(parents=True)
    pd.DataFrame({"date": [T0], "price": [100.0]}).to_feather(path)
    before = path.read_bytes()
    fake_session.handler(Api())
    try:
        export(client, tmp_path, funding=False)
    except ft.ExportError:
        pass
    assert path.read_bytes() == before, "unrecognized file was silently treated as nonexistent"


def test_r1_existing_epoch_milliseconds_are_not_reinterpreted_as_nanoseconds(client, fake_session, tmp_path):
    path = file(tmp_path, "futures")
    write_candles(path, [T0])
    foreign = pd.read_feather(path)
    foreign["date"] = [int(T0.timestamp() * 1000)]
    foreign.to_feather(path)
    before = path.read_bytes()
    fake_session.handler(Api(candles=[candle(T0), candle(T0 + timedelta(hours=1))]))
    try:
        result = export(client, tmp_path, funding=False)
    except ft.ExportError:
        assert path.read_bytes() == before
        return
    if not result.ok:
        assert path.read_bytes() == before
        return
    dates = pd.read_feather(path).date
    assert set(dates) == {pd.Timestamp(T0), pd.Timestamp(T0 + timedelta(hours=1))}, dates


def test_r1_existing_open_candle_is_not_kept(client, fake_session, tmp_path):
    path = file(tmp_path, "futures")
    write_candles(path, [T0, T0 + timedelta(hours=1)], [100, 999])
    fake_session.handler(Api(candles=[candle(T0), candle(T0 + timedelta(hours=1))]))
    export(client, tmp_path, funding=False, now=T0 + timedelta(hours=1, minutes=30))
    written = pd.read_feather(path)
    assert (written.date + pd.Timedelta(hours=1) <= T0 + timedelta(hours=1, minutes=30)).all()


def test_r1_existing_last_candle_is_refreshed_after_it_closes(client, fake_session, tmp_path):
    path = file(tmp_path, "futures")
    write_candles(path, [T0, T0 + timedelta(hours=1)], [100, 999])
    api = Api(candles=[candle(T0 + timedelta(hours=i)) for i in range(3)])
    fake_session.handler(api)
    export(client, tmp_path, funding=False, now=T0 + timedelta(hours=3))
    written = pd.read_feather(path)
    assert written.loc[written.date == T0 + timedelta(hours=1), "close"].item() == 101, api.calls


def test_r1_requesting_one_earlier_candle_backfills_it(client, fake_session, tmp_path):
    path = file(tmp_path, "futures")
    write_candles(path, [T0 + timedelta(hours=1), T0 + timedelta(hours=2)])
    fake_session.handler(Api(candles=[candle(T0 + timedelta(hours=i)) for i in range(4)]))
    export(client, tmp_path, funding=False)
    assert T0 in set(pd.read_feather(path).date), "start + one timeframe was incorrectly considered covered"


def test_r1_requesting_earlier_hourly_settlements_backfills_them(client, fake_session, tmp_path):
    path = file(tmp_path, "funding_rate")
    path.parent.mkdir(parents=True)
    old_times = pd.date_range(T0 + timedelta(hours=4), periods=5, freq="h")
    pd.DataFrame({"date": old_times, "funding_rate": 0.01}).to_feather(path)
    write_candles(file(tmp_path, "mark"), old_times)
    rows = [{"time": (T0 + timedelta(hours=i)).isoformat(), "funding_rate": 0.01,
             "mark_price": 100, "interval_hours": 1} for i in range(10)]
    fake_session.handler(Api(funding=rows))
    export(client, tmp_path)
    assert len(pd.read_feather(path)) == 10, "hard-coded 8h tolerance skipped four hourly settlements"


def test_r1_docs_one_minute_request_count_includes_funding(client, fake_session, tmp_path):
    start, _ = ft._window(30, None, NOW)
    rows = [candle(t.to_pydatetime()) for t in pd.date_range(start, NOW, freq="min")]
    api = Api(candles=rows)
    fake_session.handler(api)
    export(client, tmp_path, timeframes=["1m"], timerange=None, days=30)
    page = (ROOT / "website/src/pages/docs/freqtrade.astro").read_text()
    stated = int(re.search(r"A 1m export of the same month is\s+(\d+) requests per pair", page)[1])
    assert stated == len(api.calls), f"{len(api.calls)} requests: 44 candle pages plus 1 funding page"


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
def test_control_real_loader_preserves_funding_cadence_signs_and_types(client, fake_session, tmp_path, fmt):
    rows = [{"time": (T0 + timedelta(hours=h, milliseconds=4)).isoformat(),
             "funding_rate": rate, "mark_price": price, "interval_hours": interval}
            for h, rate, price, interval in [(0, .01, 100, 8), (8, -.01, 200, 8),
                                            (12, .02, 300, 4), (13, .01, 400, 1)]]
    fake_session.handler(Api(funding=rows))
    assert export(client, tmp_path, data_format=fmt).ok
    combined, short_fee = funding_calculation(tmp_path, fmt)
    assert len(combined) == 4  # No charges at the forward-filled non-settlement hours.
    assert short_fee == pytest.approx(9)
    from freqtrade.exchange import Exchange
    assert Exchange.calculate_funding_fees(None, combined, 1, False, T0, NOW) == pytest.approx(-9)
    assert str(combined.date.dtype) == "datetime64[ms, UTC]"
    assert ft._funding_gaps(pd.DataFrame(rows).set_index(pd.to_datetime([r["time"] for r in rows]))) == 0


def test_control_oi_is_safe_with_freqtrade_informative_merge(client, fake_session, tmp_path):
    pytest.importorskip("freqtrade")
    from freqtrade.data.history import load_pair_history
    from freqtrade.enums import CandleType
    from freqtrade.strategy import merge_informative_pair

    fake_session.handler(Api(oi=[{"time": T0.isoformat(), "open_interest": 155,
                                  "open_interest_value": 15500}]))
    export(client, tmp_path, open_interest=True)
    oi = load_pair_history(PAIR, "1h", tmp_path, candle_type=CandleType.OPEN_INTEREST)
    base = pd.DataFrame({"date": pd.date_range(T0, periods=5, freq="15min")})
    merged = merge_informative_pair(base, oi, "15m", "1h")
    # The 00:55 reading must not be exposed until the 00:45 candle closes at 01:00.
    assert merged.loc[:2, "open_interest_amount_1h"].isna().all()
    assert merged.loc[3, "open_interest_amount_1h"] == 155
    assert oi.open_interest_value.iloc[0] == 15500


def test_control_new_module_parses_as_python39():
    source = (ROOT / "clients/python/candlefeed/freqtrade.py").read_text()
    ast.parse(source, feature_version=(3, 9))
