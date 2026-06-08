"""Unit tests for the CandleFeed client — fully mocked, no network."""
from __future__ import annotations

import pandas as pd
import pytest
from conftest import FakeResponse, error, ok, page

from candlefeed import (
    AuthenticationError,
    CandleFeed,
    InvalidParameterError,
    RateLimitError,
    TierRestrictedError,
)


# --------------------------------------------------------------------------- #
# Construction / auth
# --------------------------------------------------------------------------- #
def test_api_key_required(monkeypatch):
    monkeypatch.delenv("CANDLEFEED_API_KEY", raising=False)
    with pytest.raises(AuthenticationError):
        CandleFeed()


def test_api_key_from_env(monkeypatch):
    monkeypatch.setenv("CANDLEFEED_API_KEY", "cf_live_fromenv")
    cf = CandleFeed()
    assert cf.api_key == "cf_live_fromenv"


def test_auth_header_sent(client, fake_session):
    assert fake_session.headers["X-API-Key"] == "cf_live_testkey"
    fake_session.queue(ok([{"time": "2026-01-01T00:00:00Z", "close": "1"}]))
    client.get_ohlcv("BTCUSDT", limit=1)
    assert len(fake_session.calls) == 1
    assert fake_session.calls[0]["params"]["symbol"] == "BTCUSDT"


def test_repr_masks_key(client):
    assert "cf_live_tes" in repr(client)
    assert "testkey" not in repr(client).replace("cf_live_tes", "")


# --------------------------------------------------------------------------- #
# DataFrame shape / dtypes
# --------------------------------------------------------------------------- #
def test_ohlcv_dataframe_shape_and_dtypes(client, fake_session):
    fake_session.queue(
        ok(
            [
                {"time": "2026-01-01T00:00:00Z", "open": "100.5", "high": "101", "low": "99",
                 "close": "100.7", "volume": "12.3", "quote_volume": "1234.5"},
                {"time": "2026-01-01T00:01:00Z", "open": "100.7", "high": "102", "low": "100",
                 "close": "101.9", "volume": "9.1", "quote_volume": "919.0"},
            ]
        )
    )
    df = client.get_ohlcv("BTCUSDT", interval="1m")
    assert isinstance(df, pd.DataFrame)
    assert isinstance(df.index, pd.DatetimeIndex)
    assert df.index.name == "time"
    assert list(df.columns) == ["open", "high", "low", "close", "volume", "quote_volume"]
    assert df["close"].dtype == float
    assert df["close"].iloc[0] == pytest.approx(100.7)
    assert len(df) == 2


def test_empty_response_returns_empty_frame(client, fake_session):
    fake_session.queue(ok([]))
    df = client.get_ohlcv("BTCUSDT")
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_aggregated_funding_uses_timestamp_index(client, fake_session):
    fake_session.queue(
        ok(
            [
                {"timestamp": "2026-01-01T00:00:00Z", "symbol": "BTCUSDT",
                 "weighted_funding_rate": "0.0001", "total_oi_usd": "1000000",
                 "exchange_count": 3, "contributing_exchanges": ["binance", "bybit", "okx"]},
            ]
        )
    )
    df = client.get_funding_rates_aggregated("BTCUSDT")
    assert df.index.name == "timestamp"
    assert df["weighted_funding_rate"].dtype == float
    assert df["contributing_exchanges"].iloc[0] == ["binance", "bybit", "okx"]


def test_aggregated_liquidations_no_pagination(client, fake_session):
    fake_session.queue(
        FakeResponse(
            200,
            {
                "status": "ok",
                "data": [
                    {"timestamp": "2026-01-01T00:00:00Z", "exchange": "binance",
                     "symbol": "BTCUSDT", "interval": "1d",
                     "long_liq_usd": "1000.5", "short_liq_usd": "2000.7"},
                ],
                "meta": {"total": 1, "exchange": "binance", "symbol": "BTCUSDT", "interval": "1d"},
            },
        )
    )
    df = client.get_liquidations_aggregated("BTCUSDT", interval="1d")
    assert len(df) == 1
    assert df["long_liq_usd"].dtype == float
    assert len(fake_session.calls) == 1


# --------------------------------------------------------------------------- #
# Cursor auto-pagination
# --------------------------------------------------------------------------- #
def test_auto_pagination_concatenates(client, fake_session):
    fake_session.queue(
        page([{"time": "2026-01-01T00:00:00Z", "close": "1"}], next_cursor="2026-01-01T00:00:00Z"),
        page([{"time": "2026-01-01T00:01:00Z", "close": "2"}], next_cursor="2026-01-01T00:01:00Z"),
        page([{"time": "2026-01-01T00:02:00Z", "close": "3"}], next_cursor=None),
    )
    df = client.get_ohlcv("BTCUSDT", start="2026-01-01", end="2026-01-02")
    assert len(df) == 3
    assert len(fake_session.calls) == 3
    # Cursor is threaded through on subsequent calls.
    assert fake_session.calls[1]["params"]["cursor"] == "2026-01-01T00:00:00Z"
    assert fake_session.calls[2]["params"]["cursor"] == "2026-01-01T00:01:00Z"


def test_paginate_false_fetches_one_page(client, fake_session):
    fake_session.queue(
        page([{"time": "2026-01-01T00:00:00Z", "close": "1"}], next_cursor="2026-01-01T00:00:00Z"),
    )
    df = client.get_ohlcv("BTCUSDT", paginate=False)
    assert len(df) == 1
    assert len(fake_session.calls) == 1


def test_max_rows_caps_pagination(client, fake_session):
    fake_session.queue(
        page([{"time": "2026-01-01T00:00:00Z", "close": "1"},
              {"time": "2026-01-01T00:01:00Z", "close": "2"}], next_cursor="2026-01-01T00:01:00Z"),
        page([{"time": "2026-01-01T00:02:00Z", "close": "3"}], next_cursor="2026-01-01T00:02:00Z"),
    )
    df = client.get_ohlcv("BTCUSDT", start="2026-01-01", end="2026-01-02", max_rows=3)
    assert len(df) == 3
    assert len(fake_session.calls) == 2


def test_stuck_cursor_guard(client, fake_session):
    same = "2026-01-01T00:00:00Z"
    fake_session.handler(
        lambda url, params: page([{"time": same, "close": "1"}], next_cursor=same)
    )
    df = client.get_ohlcv("BTCUSDT", start="2026-01-01")
    # Second page would repeat the same cursor → bail out instead of looping.
    assert len(fake_session.calls) == 2
    assert len(df) == 2


# --------------------------------------------------------------------------- #
# Error mapping
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "status,code,exc",
    [
        (401, "unauthorized", AuthenticationError),
        (403, "tier_restricted", TierRestrictedError),
        (400, "invalid_parameter", InvalidParameterError),
        (422, "invalid_parameter", InvalidParameterError),
    ],
)
def test_error_mapping(client, fake_session, status, code, exc):
    fake_session.queue(error(status, code, f"boom {status}"))
    with pytest.raises(exc) as ei:
        client.get_ohlcv("BTCUSDT")
    assert ei.value.code == code
    assert ei.value.status_code == status
    assert str(status) in str(ei.value)


def test_tier_restricted_message_nudges_upgrade(client, fake_session):
    fake_session.queue(
        error(403, "tier_restricted",
              "Multi-exchange access requires Pro tier or higher. Upgrade at https://candlefeed.ai/pricing")
    )
    with pytest.raises(TierRestrictedError) as ei:
        client.get_options(currency="BTC")
    assert "Upgrade" in ei.value.message


def test_error_detail_wrapped_by_fastapi(client, fake_session):
    # FastAPI wraps HTTPException detail under "detail".
    fake_session.queue(
        FakeResponse(401, {"detail": {"status": "error", "code": "unauthorized", "message": "no key"}})
    )
    with pytest.raises(AuthenticationError) as ei:
        client.get_ohlcv("BTCUSDT")
    assert ei.value.message == "no key"


# --------------------------------------------------------------------------- #
# Rate-limit retry / backoff
# --------------------------------------------------------------------------- #
def test_429_retries_then_succeeds(client, fake_session):
    fake_session.queue(
        error(429, "rate_limit_exceeded", "slow down", headers={"Retry-After": "2"}),
        ok([{"time": "2026-01-01T00:00:00Z", "close": "1"}]),
    )
    df = client.get_ohlcv("BTCUSDT", limit=1)
    assert len(df) == 1
    assert len(fake_session.calls) == 2
    assert fake_session.sleeps == [2.0]  # honored Retry-After


def test_429_exhausts_retries_and_raises(client, fake_session):
    client.max_retries = 2
    fake_session.handler(
        lambda url, params: error(429, "rate_limit_exceeded", "nope", headers={"Retry-After": "1"})
    )
    with pytest.raises(RateLimitError) as ei:
        client.get_ohlcv("BTCUSDT")
    assert ei.value.retry_after == 1.0
    assert len(fake_session.calls) == 3  # initial + 2 retries


def test_rate_limit_headers_captured(client, fake_session):
    fake_session.queue(
        FakeResponse(
            200,
            {"status": "ok", "data": [], "has_more": False, "next_cursor": None},
            headers={"X-RateLimit-Remaining": "999", "X-Plan": "pro"},
        )
    )
    client.get_ohlcv("BTCUSDT")
    assert client.last_rate_limit["X-RateLimit-Remaining"] == "999"
    assert client.last_rate_limit["X-Plan"] == "pro"


# --------------------------------------------------------------------------- #
# Param plumbing
# --------------------------------------------------------------------------- #
def test_list_params_joined(client, fake_session):
    fake_session.handler(lambda url, params: ok([]))
    client.get_funding_rates_aggregated("BTCUSDT", exchanges=["binance", "bybit"])
    assert fake_session.calls[0]["params"]["exchanges"] == "binance,bybit"


def test_combined_fields_list_joined(client, fake_session):
    fake_session.handler(lambda url, params: ok([]))
    client.get_combined("BTCUSDT", fields=["ohlcv", "funding_rate"])
    assert fake_session.calls[0]["params"]["fields"] == "ohlcv,funding_rate"


def test_none_params_dropped(client, fake_session):
    fake_session.handler(lambda url, params: ok([]))
    client.get_ohlcv("BTCUSDT")
    assert "start" not in fake_session.calls[0]["params"]
    assert "end" not in fake_session.calls[0]["params"]


def test_datetime_param_serialized(client, fake_session):
    from datetime import datetime, timezone

    fake_session.handler(lambda url, params: ok([]))
    client.get_ohlcv("BTCUSDT", start=datetime(2026, 1, 1, tzinfo=timezone.utc))
    assert fake_session.calls[0]["params"]["start"].startswith("2026-01-01T00:00:00")


def test_get_candles_alias(client):
    assert client.get_candles == client.get_ohlcv


# --------------------------------------------------------------------------- #
# Metadata
# --------------------------------------------------------------------------- #
def test_symbols_returns_frame(client, fake_session):
    fake_session.queue(
        FakeResponse(
            200,
            {"status": "ok", "count": 1,
             "symbols": [{"symbol": "BTCUSDT", "exchange": "binance",
                          "available_from": "2026-01-01T00:00:00Z",
                          "available_to": "2026-06-01T00:00:00Z"}]},
        )
    )
    df = client.symbols()
    assert "symbol" in df.columns
    assert pd.api.types.is_datetime64_any_dtype(df["available_from"])


def test_status_returns_dict(client, fake_session):
    fake_session.queue(FakeResponse(200, {"status": "ok", "api_version": "1.0.0"}))
    s = client.status()
    assert s["api_version"] == "1.0.0"
