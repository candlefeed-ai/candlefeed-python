"""candlefeed.freqtrade: Freqtrade data files from faked API responses, fully offline."""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest
from conftest import FakeResponse, error, ok

from candlefeed import freqtrade as ft

pytest.importorskip("pyarrow")

NOW = datetime(2026, 11, 14, 12, 30, tzinfo=timezone.utc)
T0 = datetime(2026, 11, 13, 0, 0, tzinfo=timezone.utc)
# freqtrade 2026.10 IDataHandler._OHLCV_REGEX: how Freqtrade finds pair, timeframe and candle type in a filename
FT_REGEX = re.compile(r"^([\w-]+)\-(\d+[a-zA-Z]{1,2})\-?([a-zA-Z_]*)?(?=\.)")


def iso(t: datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def candle(t: datetime, price: float = 100.0) -> dict:
    return {"time": iso(t), "open": str(price), "high": str(price + 2), "low": str(price - 1),
            "close": str(price + 1), "volume": "12.5", "quote_volume": str(12.5 * price)}


def hours(n: int, start: datetime = T0, skip=()):
    return [candle(start + timedelta(hours=i), 100 + i) for i in range(n) if i not in skip]


def funding_rows():
    return [
        {"time": "2026-11-13T00:00:00.004Z", "funding_rate": "0.0001", "mark_price": "90000.5", "interval_hours": 8},
        {"time": "2026-11-13T08:00:00.000Z", "funding_rate": "-0.00005", "mark_price": "91000", "interval_hours": 8},
        {"time": "2026-11-13T16:00:00.001Z", "funding_rate": "0.0002", "mark_price": "92000", "interval_hours": 8},
    ]


class Api:
    """Answers by endpoint; records each request's endpoint and params."""

    def __init__(self, candles=None, funding=None, oi=None, fail=None):
        self.candles = candles if candles is not None else hours(12)
        self.funding = funding if funding is not None else funding_rows()
        self.oi = oi or []
        self.fail = fail or {}
        self.calls = []

    def __call__(self, url, params):
        path = url.split("/api/v1/", 1)[1]
        self.calls.append((path, dict(params)))
        if path in self.fail:
            return self.fail[path]
        rows = {"candles": self.candles, "funding-rates": self.funding, "open-interest": self.oi}[path]
        start = params.get("start")
        if start:
            lo = pd.Timestamp(start)
            rows = [r for r in rows if pd.Timestamp(r["time"]) >= lo]
        return ok(rows)


@pytest.fixture
def api(fake_session):
    a = Api()
    fake_session.handler(a)
    return a


def run(client, tmp_path, **kw):
    kw.setdefault("pairs", ["BTC/USDT:USDT"])
    kw.setdefault("now", NOW)
    kw.setdefault("timerange", "20261113-")
    return ft.export_freqtrade(api_key=client, datadir=tmp_path, **kw)


def read(path):
    return pd.read_feather(path) if path.suffix == ".feather" else pd.read_parquet(path)


# --------------------------------------------------------------------------- #
# Pair names
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("given,exchange,expected", [
    ("BTCUSDT", "binance", ("BTCUSDT", "BTC/USDT:USDT")),
    ("btc/usdt:usdt", "binance", ("BTCUSDT", "BTC/USDT:USDT")),
    ("ETH/USDT", "binance", ("ETHUSDT", "ETH/USDT:USDT")),
    ("SOLUSDT", "binance_spot", ("SOLUSDT", "SOL/USDT")),
    ("DOGE/USDT", "binance_spot", ("DOGEUSDT", "DOGE/USDT")),
    ("PEPEUSDT", "binance_spot", ("PEPEUSDT", "PEPE/USDT")),
])
def test_pair_mapping(given, exchange, expected):
    assert ft.freqtrade_pair(given, exchange) == expected


@pytest.mark.parametrize("given,exchange,match", [
    ("BTC/USD:BTC", "binance", "linear"),
    ("BTC/USDT:USDT", "binance_spot", "futures pair"),
    ("PEPEUSDT", "binance", "1000PEPE/USDT:USDT"),
    ("1000PEPE/USDT:USDT", "binance", "per-coin"),
    ("BTCXYZ", "binance", "BASE/QUOTE"),
    ("BTCUSDT", "okx", "Unsupported exchange"),
])
def test_pair_mapping_refusals(given, exchange, match):
    with pytest.raises(ft.ExportError, match=match):
        ft.freqtrade_pair(given, exchange)


# --------------------------------------------------------------------------- #
# Files, columns, dtypes
# --------------------------------------------------------------------------- #
def test_futures_export_writes_freqtrade_layout(client, api, tmp_path):
    result = run(client, tmp_path)
    assert result.ok and result.trading_mode == "futures"
    names = sorted(p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*.feather"))
    assert names == ["futures/BTC_USDT_USDT-1h-funding_rate.feather", "futures/BTC_USDT_USDT-1h-futures.feather",
                     "futures/BTC_USDT_USDT-1h-mark.feather"]
    parsed = {FT_REGEX.search(n.split("/")[-1]).groups() for n in names}
    assert parsed == {("BTC_USDT_USDT", "1h", "funding_rate"), ("BTC_USDT_USDT", "1h", "futures"),
                      ("BTC_USDT_USDT", "1h", "mark")}


def test_candle_columns_dtypes_and_utc(client, api, tmp_path):
    run(client, tmp_path)
    df = read(tmp_path / "futures/BTC_USDT_USDT-1h-futures.feather")
    assert list(df.columns) == ["date", "open", "high", "low", "close", "volume"]
    assert isinstance(df["date"].dtype, pd.DatetimeTZDtype) and str(df["date"].dt.tz) == "UTC"
    assert all(df[c].dtype == "float64" for c in ["open", "high", "low", "close", "volume"])
    assert df["date"].iloc[0] == pd.Timestamp(T0)
    assert df.iloc[0][["open", "high", "low", "close", "volume"]].tolist() == [100.0, 102.0, 99.0, 101.0, 12.5]


def test_volume_is_the_api_base_volume_not_quote_volume(client, api, tmp_path):
    run(client, tmp_path)
    df = read(tmp_path / "futures/BTC_USDT_USDT-1h-futures.feather")
    assert (df["volume"] == 12.5).all()


def test_funding_and_mark_files(client, api, tmp_path):
    result = run(client, tmp_path)
    fr = read(tmp_path / "futures/BTC_USDT_USDT-1h-funding_rate.feather")
    mk = read(tmp_path / "futures/BTC_USDT_USDT-1h-mark.feather")
    assert list(fr.columns) == ["date", "funding_rate"]
    assert list(mk.columns) == ["date", "open", "high", "low", "close", "volume"]
    expected = [pd.Timestamp("2026-11-13T00:00Z"), pd.Timestamp("2026-11-13T08:00Z"), pd.Timestamp("2026-11-13T16:00Z")]
    # settlement times floored to the second, identical in both files so Freqtrade's inner merge keeps every row
    assert fr["date"].tolist() == expected and mk["date"].tolist() == expected
    assert fr["funding_rate"].tolist() == [0.0001, -0.00005, 0.0002]
    for col in ["open", "high", "low", "close"]:
        assert mk[col].tolist() == [90000.5, 91000.0, 92000.0]
    assert (mk["volume"] == 0).all()
    notes = next(f for f in result.files if f.kind == "funding_rate").notes
    assert "settlement interval(s): 8h" in notes


def test_non_8h_settlements_are_written_as_reported(client, fake_session, tmp_path):
    rows = [{"time": iso(T0 + timedelta(hours=4 * i)), "funding_rate": "0.0001", "mark_price": "10",
             "interval_hours": 4} for i in range(4)]
    fake_session.handler(Api(funding=rows))
    result = run(client, tmp_path)
    fr = read(tmp_path / "futures/BTC_USDT_USDT-1h-funding_rate.feather")
    assert len(fr) == 4 and fr["date"].diff().iloc[1:].eq(pd.Timedelta(hours=4)).all()
    assert next(f for f in result.files if f.kind == "funding_rate").missing == 0


def test_settlement_without_mark_price_stops_that_pairs_funding(client, fake_session, tmp_path):
    rows = funding_rows()
    rows[1]["mark_price"] = "0"
    fake_session.handler(Api(funding=rows))
    result = run(client, tmp_path)
    assert not result.ok and result.error is None
    assert "1 funding settlement(s) between 2026-11-13 08:00 UTC and 2026-11-13 08:00 UTC" in result.refused[0]
    assert (tmp_path / "futures/BTC_USDT_USDT-1h-futures.feather").exists()
    assert not (tmp_path / "futures/BTC_USDT_USDT-1h-funding_rate.feather").exists()
    assert not (tmp_path / "futures/BTC_USDT_USDT-1h-mark.feather").exists()


def test_cli_reports_refused_files_with_exit_3(client, fake_session, tmp_path, monkeypatch, capsys):
    rows = funding_rows()
    rows[0]["mark_price"] = None
    fake_session.handler(Api(funding=rows))
    monkeypatch.setattr(ft, "_client", lambda key: client)
    monkeypatch.setenv("CANDLEFEED_API_KEY", "cf_live_testkey")
    code = ft.main(["--pairs", "BTC/USDT:USDT", "--timerange", "20261113-", "--datadir", str(tmp_path)])
    err = capsys.readouterr().err
    assert code == 3 and "not written: BTC/USDT:USDT: 1 funding settlement(s)" in err and "--no-funding" in err


def test_rate_rows_without_marks_on_disk_are_refetched(client, fake_session, tmp_path):
    fake_session.handler(Api())
    run(client, tmp_path)
    mk_path = tmp_path / "futures/BTC_USDT_USDT-1h-mark.feather"
    read(mk_path).iloc[:1].to_feather(mk_path)          # as if the mark write had failed after the first row
    again = Api()
    fake_session.handler(again)
    assert run(client, tmp_path).ok
    call = next(c for c in again.calls if c[0] == "funding-rates")
    assert pd.Timestamp(call[1]["start"]) == pd.Timestamp("2026-11-13T00:00Z")     # funding: whole range, every run
    assert read(mk_path)["date"].tolist() == read(tmp_path / "futures/BTC_USDT_USDT-1h-funding_rate.feather")[
        "date"].tolist()


def test_unmatched_settlements_before_the_range_are_refused_untouched(client, fake_session, tmp_path):
    fr_path = tmp_path / "futures/BTC_USDT_USDT-1h-funding_rate.feather"
    fr_path.parent.mkdir(parents=True)
    pd.DataFrame({"date": pd.to_datetime(["2026-11-01T00:00Z"]), "funding_rate": [0.0001]}).to_feather(fr_path)
    before = fr_path.read_bytes()
    api = Api()
    fake_session.handler(api)
    result = run(client, tmp_path)
    assert "no finite, positive mark price" in result.refused[0] and "2026-11-01 00:00 UTC" in result.refused[0]
    assert fr_path.read_bytes() == before and not (tmp_path / "futures/BTC_USDT_USDT-1h-mark.feather").exists()
    assert [c[0] for c in api.calls] == ["candles"]


def test_legacy_funding_layout_is_migrated_with_its_history(client, fake_session, tmp_path):
    fr_path = tmp_path / "futures/BTC_USDT_USDT-1h-funding_rate.feather"
    mk_path = tmp_path / "futures/BTC_USDT_USDT-1h-mark.feather"
    fr_path.parent.mkdir(parents=True)
    old = pd.to_datetime(["2026-11-12T16:00Z", "2026-11-13T00:00Z"])
    legacy = pd.DataFrame({"date": old, "open": [0.0003, 0.0001], "high": 0.0, "low": 0.0, "close": 0.0,
                           "volume": 0.0})
    legacy.to_feather(fr_path)
    legacy.assign(open=[89000.0, 90000.5], volume=0.0).to_feather(mk_path)
    fake_session.handler(Api())
    assert run(client, tmp_path).ok
    fr = read(fr_path)
    assert list(fr.columns) == ["date", "funding_rate"]
    assert fr["funding_rate"].tolist() == [0.0003, 0.0001, -0.00005, 0.0002]


def test_existing_file_with_dates_off_the_timeframe_grid_is_refused(client, api, tmp_path):
    path = tmp_path / "futures/BTC_USDT_USDT-1h-futures.feather"
    path.parent.mkdir(parents=True)
    pd.DataFrame({"date": pd.to_datetime(["2026-11-13T00:30Z"]), "open": [1.0], "high": [1.0], "low": [1.0],
                  "close": [1.0], "volume": [1.0]}).to_feather(path)
    result = run(client, tmp_path, funding=False)
    assert "don't start on a 1h boundary" in result.refused[0] and len(read(path)) == 1


def test_open_candle_on_disk_is_removed_even_when_nothing_new_arrives(client, fake_session, tmp_path):
    path = tmp_path / "futures/BTC_USDT_USDT-1h-futures.feather"
    path.parent.mkdir(parents=True)
    pd.DataFrame({"date": pd.to_datetime(["2026-11-13T00:00Z", "2026-11-13T01:00Z"]), "open": [1.0, 2.0],
                  "high": [1.0, 2.0], "low": [1.0, 2.0], "close": [1.0, 2.0], "volume": [1.0, 1.0]}).to_feather(path)
    fake_session.handler(Api(candles=[]))
    result = run(client, tmp_path, funding=False, timerange="20261113-20261114", now=T0 + timedelta(minutes=90))
    assert read(path)["date"].tolist() == [pd.Timestamp(T0)]
    assert "removed 1 candle(s) on disk that were still open at export time" in result.files[0].notes


def test_spot_export_writes_root_file_only(client, api, tmp_path):
    result = run(client, tmp_path, pairs=["BTCUSDT"], exchange="binance_spot", timeframes=["1h", "4h"])
    assert result.trading_mode == "spot"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["BTC_USDT-1h.feather", "BTC_USDT-4h.feather"]
    assert all(c[0] == "candles" and c[1]["exchange"] == "binance_spot" for c in api.calls)
    assert FT_REGEX.search("BTC_USDT-1h.feather").groups() == ("BTC_USDT", "1h", "")


def test_parquet_format(client, api, tmp_path):
    run(client, tmp_path, data_format="parquet", funding=False)
    df = pd.read_parquet(tmp_path / "futures/BTC_USDT_USDT-1h-futures.parquet")
    assert list(df.columns) == ["date", "open", "high", "low", "close", "volume"] and len(df) == 12


def test_open_interest_file(client, fake_session, tmp_path):
    oi = [{"time": iso(T0 + timedelta(hours=i)), "open_interest": str(80000 + i), "open_interest_value": "7.2e9"}
          for i in range(3)]
    fake_session.handler(Api(oi=oi))
    run(client, tmp_path, open_interest=True)
    df = read(tmp_path / "futures/BTC_USDT_USDT-1h-open_interest.feather")
    assert list(df.columns) == ["date", "open_interest_amount", "open_interest_value"]
    assert df["open_interest_amount"].tolist() == [80000.0, 80001.0, 80002.0]
    assert FT_REGEX.search("BTC_USDT_USDT-1h-open_interest.feather").groups()[2] == "open_interest"


# --------------------------------------------------------------------------- #
# Gaps, open candles, no invented rows
# --------------------------------------------------------------------------- #
def test_missing_candles_are_counted_not_filled(client, fake_session, tmp_path):
    fake_session.handler(Api(candles=hours(12, skip={3, 4, 7})))
    result = run(client, tmp_path, funding=False)
    df = read(tmp_path / "futures/BTC_USDT_USDT-1h-futures.feather")
    assert len(df) == 9
    assert pd.Timestamp(T0 + timedelta(hours=3)) not in set(df["date"])
    report = result.files[0]
    assert report.missing == 3 and report.rows_written == 9


def test_missing_settlements_are_counted(client, fake_session, tmp_path):
    rows = funding_rows()
    del rows[1]
    fake_session.handler(Api(funding=rows))
    result = run(client, tmp_path)
    assert next(f for f in result.files if f.kind == "funding_rate").missing == 1
    assert len(read(tmp_path / "futures/BTC_USDT_USDT-1h-funding_rate.feather")) == 2


def test_candle_still_open_at_export_time_is_dropped(client, fake_session, tmp_path):
    day = datetime(2026, 11, 14, tzinfo=timezone.utc)
    fake_session.handler(Api(candles=hours(13, start=day)))     # the 12:00 candle closes at 13:00, after NOW
    result = run(client, tmp_path, funding=False, timerange="20261114-")
    df = read(tmp_path / "futures/BTC_USDT_USDT-1h-futures.feather")
    assert df["date"].iloc[-1] == pd.Timestamp(day + timedelta(hours=11))
    assert any("still open" in n for n in result.files[0].notes)


def test_report_prints_gap_count(client, fake_session, tmp_path, capsys):
    fake_session.handler(Api(candles=hours(12, skip={5})))
    ft._format_report(run(client, tmp_path, funding=False))
    assert "1 candles missing in CandleFeed's data for this range (not filled in)" in capsys.readouterr().out


# --------------------------------------------------------------------------- #
# Ranges, incremental runs, quota
# --------------------------------------------------------------------------- #
def test_days_window_stays_inside_free_30_days(client, api, tmp_path):
    run(client, tmp_path, timerange=None, days=30, funding=False)
    start = pd.Timestamp(api.calls[0][1]["start"])
    assert start >= pd.Timestamp(NOW - timedelta(days=30))
    assert start.minute == 0 and start.second == 0
    assert "end" not in api.calls[0][1]


def test_timerange_end_is_exclusive(client, api, tmp_path):
    run(client, tmp_path, timerange="20261101-20261113", funding=False)
    end = pd.Timestamp(api.calls[0][1]["end"])
    assert end < pd.Timestamp("2026-11-13T00:00Z") and end > pd.Timestamp("2026-11-12T23:59:59Z")


def test_incremental_run_fetches_only_new_rows(client, fake_session, tmp_path):
    first = Api(candles=hours(6))
    fake_session.handler(first)
    run(client, tmp_path)
    second = Api(candles=hours(12))
    fake_session.handler(second)
    run(client, tmp_path)
    candle_call = next(c for c in second.calls if c[0] == "candles")
    # the last candle on disk is fetched again, in case the file came from a tool that wrote it before it closed
    assert pd.Timestamp(candle_call[1]["start"]) == pd.Timestamp(T0 + timedelta(hours=5))
    funding_call = next(c for c in second.calls if c[0] == "funding-rates")
    # funding is a few rows a day, so the whole range is refetched and checked again
    assert pd.Timestamp(funding_call[1]["start"]) == pd.Timestamp(T0)
    df = read(tmp_path / "futures/BTC_USDT_USDT-1h-futures.feather")
    assert len(df) == 12 and df["date"].is_monotonic_increasing and df["date"].is_unique


def test_no_incremental_refetches(client, fake_session, tmp_path):
    fake_session.handler(Api())
    run(client, tmp_path)
    again = Api()
    fake_session.handler(again)
    run(client, tmp_path, incremental=False)
    assert pd.Timestamp(again.calls[0][1]["start"]) == pd.Timestamp("2026-11-13T00:00Z")


def test_user_agent_carries_exporter_marker(client, api, tmp_path, fake_session):
    run(client, tmp_path, funding=False)
    assert fake_session.headers["User-Agent"].endswith(" candlefeed-freqtrade")
    assert fake_session.headers["User-Agent"].startswith("candlefeed-python/")


# --------------------------------------------------------------------------- #
# Plan limits and errors
# --------------------------------------------------------------------------- #
TIER_MSG = ("Data older than 30 days requires the Builder plan ($29/mo) or above. Free covers OHLCV and funding "
            "rates for BTCUSDT, ETHUSDT, SOLUSDT, XRPUSDT and DOGEUSDT, last 30 days. "
            "Upgrade: https://candlefeed.ai/pricing/?utm_source=api&utm_medium=error&utm_campaign=history")


def test_403_stops_cleanly_with_tagged_link(client, fake_session, tmp_path):
    api = Api(fail={"candles": error(403, "tier_restricted", TIER_MSG)})
    fake_session.handler(api)
    result = run(client, tmp_path, pairs=["BTCUSDT", "ETHUSDT"], timerange="20260101-")
    assert not result.ok
    assert "utm_source=freqtrade&utm_medium=client" in result.error
    assert "utm_source=api" not in result.error
    assert "Data older than 30 days" in result.error
    assert len(api.calls) == 1                       # stops at once: no quota spent on the other pair
    assert not list(tmp_path.rglob("*.feather"))


def test_403_after_some_files_keeps_them(client, fake_session, tmp_path):
    fake_session.handler(Api(fail={"funding-rates": error(403, "tier_restricted", TIER_MSG)}))
    result = run(client, tmp_path)
    assert not result.ok
    assert (tmp_path / "futures/BTC_USDT_USDT-1h-futures.feather").exists()
    assert not (tmp_path / "futures/BTC_USDT_USDT-1h-funding_rate.feather").exists()


def test_cli_403_exit_code_and_message(client, fake_session, tmp_path, monkeypatch, capsys):
    fake_session.handler(Api(fail={"candles": error(403, "tier_restricted", TIER_MSG)}))
    monkeypatch.setattr(ft, "_client", lambda key: client)
    monkeypatch.setenv("CANDLEFEED_API_KEY", "cf_live_testkey")
    code = ft.main(["--pairs", "BTC/USDT:USDT", "--timerange", "20260101-", "--datadir", str(tmp_path)])
    err = capsys.readouterr().err
    assert code == 3
    assert "https://candlefeed.ai/pricing/?utm_source=freqtrade&utm_medium=client" in err
    assert "utm_source=api" not in err and "cf_live_testkey" not in err


def test_missing_key_points_to_tagged_signup(monkeypatch, tmp_path, capsys):
    monkeypatch.delenv("CANDLEFEED_API_KEY", raising=False)
    code = ft.main(["--pairs", "BTCUSDT", "--datadir", str(tmp_path)])
    err = capsys.readouterr().err
    assert code == 2
    assert "https://candlefeed.ai/signup?utm_source=freqtrade&utm_medium=client" in err
    assert "utm_source=client" not in err.replace("utm_source=freqtrade&utm_medium=client", "")


def test_invalid_key_message_is_tagged_and_scrubbed(client, fake_session, tmp_path, monkeypatch, capsys):
    fake_session.handler(Api(fail={"candles": error(401, "unauthorized",
                                                    "Invalid API key. Get one at https://candlefeed.ai/signup")}))
    monkeypatch.setattr(ft, "_client", lambda key: client)
    monkeypatch.setenv("CANDLEFEED_API_KEY", "cf_live_testkey")
    code = ft.main(["--pairs", "BTCUSDT", "--datadir", str(tmp_path), "--no-funding"])
    err = capsys.readouterr().err
    assert code == 2
    assert "Get a free key (no card): https://candlefeed.ai/signup?utm_source=freqtrade&utm_medium=client" in err
    assert "https://candlefeed.ai/signup\n" not in err and "cf_live_testkey" not in err


def test_daily_limit_message(client, fake_session, tmp_path):
    resp = FakeResponse(429, {"status": "error", "code": "rate_limited", "message": "Daily request limit reached."},
                        headers={"Retry-After": "40000"})
    fake_session.handler(Api(fail={"candles": resp}))
    result = run(client, tmp_path)
    assert not result.ok and "request limit" in result.error and "carries on" in result.error


@pytest.mark.parametrize("kw,match", [
    ({"timeframes": ["2h"]}, "1m, 5m, 15m, 1h, 4h, 1d"),
    ({"data_format": "json"}, "feather, parquet"),
    ({"exchange": "binance_spot", "pairs": ["BTCUSDT"], "open_interest": True}, "perpetuals only"),
    ({"timerange": "20261113-20261101"}, "after the start"),
    ({"timerange": "-20261101"}, "start required"),
])
def test_bad_arguments(client, api, tmp_path, kw, match):
    with pytest.raises(ft.ExportError, match=re.escape(match)):
        run(client, tmp_path, **kw)
    assert api.calls == []
