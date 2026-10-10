"""Regressions for the 0.3.4 audit fixes: timestamp parsing, page sizes, long 429 waits, 5xx retries,
aggregated liquidation paging, empty frames and restricted L2 windows."""
from __future__ import annotations

import logging
import warnings
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import pandas as pd
import pytest
from conftest import FakeResponse, error, ok, page

from candlefeed import CandleFeed, CandleFeedError, RateLimitError, TierRestrictedError
from candlefeed.exceptions import InvalidParameterError


@pytest.mark.parametrize("times", [
    ["2026-06-01T00:00:01+00:00", "2026-06-01T00:00:01.123000+00:00"],
    ["2026-06-01T00:00:00.123000+00:00", "2026-06-01T00:00:01+00:00"],
    ["2026-06-01T00:00:00Z", "2026-06-01T01:00:00+00:00", "2026-06-01T02:00:00"],
])
def test_mixed_timestamp_formats_all_parse(client, fake_session, times):
    fake_session.queue(ok([{"time": t, "usd_value": "1"} for t in times]))
    df = client.get_liquidations("BTCUSDT", max_rows=10)
    assert not df.index.isna().any()
    assert len(df) == len(times)
    assert str(df.index.tz) == "UTC"


def test_unparseable_timestamp_raises_instead_of_nat(client, fake_session):
    fake_session.queue(ok([{"time": "2026-06-01T00:00:00Z", "close": "1"}, {"time": "yesterday", "close": "2"}]))
    with pytest.raises(CandleFeedError) as ei:
        client.get_ohlcv("BTCUSDT", max_rows=10)
    assert ei.value.code == "unparseable_timestamp"


def test_max_rows_is_one_request_of_that_size(client, fake_session):
    fake_session.queue(page([{"time": f"2026-06-01T0{i}:00:00Z", "close": "1"} for i in range(5)],
                            next_cursor="2026-06-01T04:00:00Z"))
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        df = client.get_ohlcv("BTCUSDT", interval="1h", max_rows=5)
    assert len(df) == 5
    assert len(fake_session.calls) == 1
    assert fake_session.calls[0]["params"]["limit"] == 5


def test_default_page_size_asks_for_the_plan_maximum(client, fake_session):
    fake_session.handler(lambda url, params: ok([]))
    client.get_ohlcv("BTCUSDT")
    client.get_combined("BTCUSDT")
    client.get_liquidations_aggregated("BTCUSDT")
    assert [c["params"]["limit"] for c in fake_session.calls] == [10_000, 5_000, 5_000]


def test_small_limit_on_a_paginating_call_warns(client, fake_session):
    fake_session.queue(ok([]))
    with pytest.warns(UserWarning, match="max_rows=5"):
        client.get_ohlcv("BTCUSDT", limit=5)
    assert fake_session.calls[0]["params"]["limit"] == 5


def test_page_size_keeps_small_pages_without_a_warning(client, fake_session):
    fake_session.queue(page([{"time": "2026-06-01T00:00:00Z", "close": "1"}], next_cursor="2026-06-01T00:00:00Z"),
                       ok([{"time": "2026-06-01T00:01:00Z", "close": "2"}]))
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        df = client.get_ohlcv("BTCUSDT", page_size=1)
    assert len(df) == 2
    assert [c["params"]["limit"] for c in fake_session.calls] == [1, 1]


def test_limit_and_page_size_conflict(client):
    with pytest.raises(InvalidParameterError):
        client.get_ohlcv("BTCUSDT", limit=10, page_size=20)


def test_daily_quota_429_raises_without_sleeping(client, fake_session):
    fake_session.handler(lambda url, params: error(429, "rate_limit_exceeded", "Daily limit reached.",
                                                   headers={"Retry-After": "61234"}))
    with pytest.raises(RateLimitError) as ei:
        client.get_ohlcv("BTCUSDT")
    assert fake_session.sleeps == []
    assert len(fake_session.calls) == 1
    assert ei.value.retry_after == 61234.0
    assert ei.value.reset_at is not None
    assert ei.value.reset_at > datetime.now(timezone.utc) + timedelta(hours=16)
    assert "UTC" in ei.value.message and "max_retry_wait" in ei.value.message


def test_short_429_is_still_slept_through(client, fake_session):
    fake_session.queue(error(429, "rate_limit_exceeded", "slow", headers={"Retry-After": "30"}), ok([]))
    client.get_ohlcv("BTCUSDT")
    assert fake_session.sleeps == [30.0]


def test_max_retry_wait_none_waits_as_long_as_asked(fake_session, monkeypatch):
    import candlefeed.client as client_module
    cf = CandleFeed(api_key="cf_live_testkey", session=fake_session, max_retry_wait=None)
    monkeypatch.setattr(client_module.time, "sleep", fake_session.sleeps.append)
    fake_session.queue(error(429, "rate_limit_exceeded", "slow", headers={"Retry-After": "600"}), ok([]))
    cf.get_ohlcv("BTCUSDT")
    assert fake_session.sleeps == [600.0]


def test_long_sleeps_are_logged(client, fake_session, caplog):
    fake_session.queue(error(429, "rate_limit_exceeded", "slow", headers={"Retry-After": "12"}), ok([]))
    with caplog.at_level(logging.WARNING, logger="candlefeed"):
        client.get_ohlcv("BTCUSDT")
    assert any("Waiting 12 s" in r.getMessage() for r in caplog.records)


def test_http_date_retry_after_is_honoured(client, fake_session):
    when = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=20), usegmt=True)
    fake_session.queue(error(429, "rate_limit_exceeded", "slow", headers={"Retry-After": when}), ok([]))
    client.get_ohlcv("BTCUSDT")
    assert len(fake_session.sleeps) == 1 and 15 <= fake_session.sleeps[0] <= 20


def test_http_date_retry_after_far_away_raises(client, fake_session):
    when = format_datetime(datetime.now(timezone.utc) + timedelta(hours=3), usegmt=True)
    fake_session.queue(error(429, "rate_limit_exceeded", "slow", headers={"Retry-After": when}))
    with pytest.raises(RateLimitError) as ei:
        client.get_ohlcv("BTCUSDT")
    assert ei.value.retry_after > 3 * 3600 - 60 and fake_session.sleeps == []


@pytest.mark.parametrize("status", [500, 502, 503, 504])
def test_5xx_is_retried_and_earlier_pages_are_kept(client, fake_session, status):
    fake_session.queue(
        page([{"time": "2026-06-01T00:00:00Z", "close": "1"}], next_cursor="2026-06-01T00:00:00Z"),
        FakeResponse(status, None, headers={"X-RateLimit-Reset": "2099-01-01T00:00:00Z"}),
        ok([{"time": "2026-06-01T00:01:00Z", "close": "2"}]),
    )
    df = client.get_ohlcv("BTCUSDT", page_size=1)
    assert list(df["close"]) == [1.0, 2.0]
    assert fake_session.sleeps == [0.5]      # backoff, not the hours to X-RateLimit-Reset


def test_5xx_after_retries_carries_partial_rows_and_resume_cursor(client, fake_session):
    client.max_retries = 1
    fake_session.queue(
        page([{"time": "2026-06-01T00:00:00Z", "close": "1"}], next_cursor="2026-06-01T00:00:00Z"),
        FakeResponse(503, None), FakeResponse(503, None),
    )
    with pytest.raises(CandleFeedError) as ei:
        client.get_ohlcv("BTCUSDT", page_size=1)
    assert ei.value.status_code == 503
    assert ei.value.resume_cursor == "2026-06-01T00:00:00Z"
    assert list(ei.value.partial["close"]) == [1.0]


def test_4xx_is_not_retried(client, fake_session):
    fake_session.queue(error(404, "not_found", "nope"))
    with pytest.raises(CandleFeedError):
        client.get_ohlcv("BTCUSDT")
    assert len(fake_session.calls) == 1


def _agg(rows, total):
    return FakeResponse(200, {"status": "ok", "data": rows, "meta": {"total": total, "history_from": "2019-09-13Z"}})


def _day(i):
    return {"timestamp": (pd.Timestamp("2019-09-13", tz="UTC") + pd.Timedelta(days=i)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "long_liq_usd": 1.0, "short_liq_usd": 2.0}


def test_aggregated_liquidations_page_until_total(client, fake_session):
    fake_session.queue(_agg([_day(i) for i in range(3)], 5), _agg([_day(i) for i in range(2, 5)], 3))
    df = client.get_liquidations_aggregated("BTCUSDT", page_size=3)
    assert len(df) == 5 and df.index.is_unique
    second = fake_session.calls[1]["params"]
    assert pd.Timestamp(second["start"]) == pd.Timestamp("2019-09-16T00:00:00Z")      # next 1d bucket's open
    assert df.attrs["meta"]["total"] == 5


def test_aggregated_liquidations_limit_alone_is_one_page_and_warns(client, fake_session):
    fake_session.queue(_agg([_day(i) for i in range(3)], 2580))
    with pytest.warns(UserWarning, match="3 of 2,580"):
        df = client.get_liquidations_aggregated("BTCUSDT", limit=3)
    assert len(df) == 3 and len(fake_session.calls) == 1


def test_aggregated_liquidations_max_rows(client, fake_session):
    fake_session.queue(_agg([_day(i) for i in range(4)], 10))
    df = client.get_liquidations_aggregated("BTCUSDT", max_rows=4)
    assert len(df) == 4 and fake_session.calls[0]["params"]["limit"] == 4


@pytest.mark.parametrize("call,cols,index_name", [
    (lambda cf: cf.get_ohlcv("BTCUSDT"), ["open", "high", "low", "close", "volume", "quote_volume"], "time"),
    (lambda cf: cf.get_liquidations("BTCUSDT"), ["side", "quantity", "price", "usd_value", "position_side"], "time"),
    (lambda cf: cf.get_liquidations("BTCUSDT", interval="1h"),
     ["count", "long_liq_usd", "short_liq_usd", "total_liq_usd"], "time"),
    (lambda cf: cf.get_funding_rates_aggregated("BTCUSDT"),
     ["symbol", "weighted_funding_rate", "total_oi_usd", "exchange_count", "contributing_exchanges"], "timestamp"),
])
def test_empty_frames_keep_their_shape(client, fake_session, call, cols, index_name):
    fake_session.queue(ok([]))
    df = call(client)
    assert list(df.columns) == cols
    assert isinstance(df.index, pd.DatetimeIndex) and str(df.index.tz) == "UTC"
    assert df.index.name == index_name
    df.resample("1h").sum()


def test_empty_aggregated_liquidations_frame(client, fake_session):
    fake_session.queue(_agg([], 0))
    df = client.get_liquidations_aggregated("BTCUSDT")
    assert "long_liq_usd" in df.columns and isinstance(df.index, pd.DatetimeIndex)


def _l2_listing(days=(), missing=()):
    return FakeResponse(200, {"status": "ok", "days": list(days), "missing": list(missing), "usage": None})


def test_download_l2_restricted_tail_window_is_listed_as_missing(client, fake_session, tmp_path):
    fake_session.queue(_l2_listing(missing=[{"date": "2026-09-02", "reason": "plan_restricted"}]),
                       error(403, "tier_restricted", "Order book depth for 2026-10-02 requires Pro."))
    out = client.download_l2("book", "BTCUSDT", "2026-09-01", "2026-10-15", tmp_path)
    tail = [m for m in out["missing"] if m["date"] >= "2026-10-02"]
    assert [m["date"] for m in tail] == [f"2026-10-{d:02d}" for d in range(2, 16)]
    assert all(m["reason"] == "plan_restricted" and "Pro" in m["message"] for m in tail)


def test_download_l2_restricted_window_with_a_sample_day_still_raises(client, fake_session, tmp_path):
    fake_session.queue(error(403, "tier_restricted", "Your plan can't download order book files."))
    with pytest.raises(TierRestrictedError):
        client.download_l2("book", "BTCUSDT", "2026-09-01", "2026-09-10", tmp_path)


def test_download_l2_range_without_any_sample_day_raises(client, fake_session, tmp_path):
    fake_session.queue(error(403, "tier_restricted", "requires Pro"))
    with pytest.raises(TierRestrictedError):
        client.download_l2("book", "BTCUSDT", "2026-10-02", "2026-10-15", tmp_path)


# --------------------------------------------------------------------------- #
# Astra PR #156 review
# --------------------------------------------------------------------------- #
def _agg_server(semantics, width_h, rolled, n_base=40):
    """Plays liquidations/aggregated over n_base stored rows. A rolled-up interval sums 4h rows; "deployed"
    reads those from 4 h before an inclusive start, "next" aligns start down to its bucket (whole buckets).
    A native interval is stored at its own width and filtered by an inclusive start either way."""
    base_h = 4 if rolled else width_h
    t0 = pd.Timestamp("2026-10-01", tz="UTC")
    origin = pd.Timestamp("2000-01-03", tz="UTC")
    base = [t0 + pd.Timedelta(hours=base_h * i) for i in range(n_base)]
    width = pd.Timedelta(hours=width_h)

    def bucket(t):
        return origin + ((t - origin) // width) * width

    def handler(url, params):
        start = pd.Timestamp(params["start"]) if params.get("start") else None
        if start is None:
            lower = None
        elif not rolled:
            lower = start
        elif semantics == "deployed":
            lower = start - pd.Timedelta(hours=base_h)
        else:
            lower = bucket(start)
        rows = [t for t in base if lower is None or t >= lower]
        labels = sorted({bucket(t) for t in rows})
        page = labels[:params["limit"]]
        data = [{"timestamp": t.strftime("%Y-%m-%dT%H:%M:%SZ"), "long_liq_usd": 1.0, "short_liq_usd": 2.0}
                for t in page]
        return FakeResponse(200, {"status": "ok", "data": data, "meta": {"total": len(labels)}})
    return handler


@pytest.mark.parametrize("semantics", ["deployed", "next"])
@pytest.mark.parametrize("rolled", [True, False])
@pytest.mark.parametrize("width_h,page_size", [(6, 1), (6, 2), (8, 1), (12, 1), (4, 1), (24, 3)])
def test_aggregated_small_pages_match_one_large_page(client, fake_session, semantics, rolled, width_h, page_size):
    if rolled and width_h == 4:
        pytest.skip("4h is the rollup base")
    interval = {4: "4h", 6: "6h", 8: "8h", 12: "12h", 24: "1d"}[width_h]
    fake_session.handler(_agg_server(semantics, width_h, rolled))
    whole = client.get_liquidations_aggregated("BTCUSDT", interval=interval, paginate=False)
    paged = client.get_liquidations_aggregated("BTCUSDT", interval=interval, page_size=page_size)
    pd.testing.assert_frame_equal(paged, whole)
    capped = client.get_liquidations_aggregated("BTCUSDT", interval=interval, page_size=3, max_rows=4)
    pd.testing.assert_frame_equal(capped, whole.head(4))


def test_aggregated_no_progress_raises_instead_of_returning_short(client, fake_session):
    stuck = [_day(0)]
    fake_session.handler(lambda url, params: _agg(stuck, 5))
    with pytest.raises(CandleFeedError) as ei:
        client.get_liquidations_aggregated("BTCUSDT", page_size=1)
    assert ei.value.code == "incomplete_result"
    assert len(ei.value.partial) == 1 and ei.value.resume_start and ei.value.resume_cursor is None


def test_aggregated_follows_a_server_cursor_verbatim(client, fake_session):
    fake_session.queue(
        FakeResponse(200, {"data": [_day(0)], "has_more": True, "next_cursor": "2019-09-13T00:00:00Z~opaque",
                           "meta": {"total": 2}}),
        FakeResponse(200, {"data": [_day(1)], "has_more": False, "next_cursor": None, "meta": {"total": 1}}))
    df = client.get_liquidations_aggregated("BTCUSDT", page_size=1)
    assert len(df) == 2
    assert fake_session.calls[1]["params"]["cursor"] == "2019-09-13T00:00:00Z~opaque"
    assert "start" not in fake_session.calls[1]["params"]


def test_compound_cursor_is_passed_back_verbatim_and_resumable(client, fake_session):
    compound = "2026-06-01T00:00:00.123000+00:00~long~0.5~65000"
    client.max_retries = 0
    fake_session.queue(
        FakeResponse(200, {"data": [{"time": "2026-06-01T00:00:00.123000+00:00", "usd_value": 1}],
                           "has_more": True, "next_cursor": compound}),
        FakeResponse(503, None))
    with pytest.raises(CandleFeedError) as ei:
        client.get_liquidations("BTCUSDT", page_size=1)
    assert fake_session.calls[1]["params"]["cursor"] == compound
    assert ei.value.resume_cursor == compound and len(ei.value.partial) == 1
    fake_session.queue(ok([{"time": "2026-06-01T00:00:00.123000+00:00", "usd_value": 2}]))
    client.get_liquidations("BTCUSDT", page_size=1, start="2026-06-01", cursor=ei.value.resume_cursor)
    assert fake_session.calls[2]["params"]["cursor"] == compound


def test_empty_page_stops_paging_even_with_a_cursor(client, fake_session):
    fake_session.queue(page([{"time": "2026-06-01T00:00:00Z", "close": "1"}], next_cursor="c1"),
                       FakeResponse(200, {"data": [], "has_more": True, "next_cursor": "c2"}))
    df = client.get_ohlcv("BTCUSDT", page_size=1)
    assert len(df) == 1 and len(fake_session.calls) == 2


def test_empty_combined_has_the_requested_columns(client, fake_session):
    fake_session.queue(ok([]))
    df = client.get_combined("BTCUSDT", fields=["ohlcv", "funding_rate", "liquidations"])
    assert list(df.columns) == ["open", "high", "low", "close", "volume", "quote_volume", "funding_rate",
                                "mark_price", "liquidation_count", "liquidation_volume_usd"]
    assert isinstance(df.index, pd.DatetimeIndex) and df.index.name == "time"


def test_max_retry_wait_caps_headerless_429_and_network_backoff(fake_session, monkeypatch):
    import requests

    import candlefeed.client as client_module
    cf = CandleFeed(api_key="cf_live_testkey", session=fake_session, max_retries=3, max_retry_wait=0.1)
    monkeypatch.setattr(client_module.time, "sleep", fake_session.sleeps.append)
    fake_session.handler(lambda url, params: error(429, "rate_limit_exceeded", "slow"))
    with pytest.raises(RateLimitError):
        cf.get_ohlcv("BTCUSDT")

    def boom(url, params):
        raise requests.ConnectionError("reset")
    fake_session.handler(boom)
    with pytest.raises(CandleFeedError):
        cf.get_ohlcv("BTCUSDT")
    assert len(fake_session.sleeps) == 6 and all(s <= 0.1 for s in fake_session.sleeps)


@pytest.mark.parametrize("bad", [-1, float("inf"), float("nan"), "60", True])
def test_max_retry_wait_is_validated(fake_session, bad):
    with pytest.raises(InvalidParameterError):
        CandleFeed(api_key="cf_live_testkey", session=fake_session, max_retry_wait=bad)


@pytest.mark.parametrize("semantics", ["deployed", "next"])
@pytest.mark.parametrize("rolled,width_h", [(True, 8), (False, 6), (False, 24)])
def test_aggregated_resume_start_has_no_duplicate_or_partial_bucket(client, fake_session, semantics, rolled,
                                                                     width_h):
    interval = {6: "6h", 8: "8h", 24: "1d"}[width_h]
    serve = _agg_server(semantics, width_h, rolled)
    fake_session.handler(serve)
    whole = client.get_liquidations_aggregated("BTCUSDT", interval=interval, paginate=False)
    client.max_retries = 0
    calls = []

    def flaky(url, params):
        calls.append(1)
        return FakeResponse(503, None) if len(calls) == 2 else serve(url, params)
    fake_session.handler(flaky)
    with pytest.raises(CandleFeedError) as ei:
        client.get_liquidations_aggregated("BTCUSDT", interval=interval, page_size=3)
    start = pd.Timestamp(ei.value.resume_start)
    assert start == ei.value.partial.index[-1] + pd.Timedelta(hours=width_h)
    fake_session.handler(serve)
    rest = client.get_liquidations_aggregated("BTCUSDT", interval=interval, start=ei.value.resume_start,
                                              page_size=3)
    pd.testing.assert_frame_equal(pd.concat([ei.value.partial, rest]), whole)


def test_aggregated_empty_page_with_rows_remaining_raises(client, fake_session):
    fake_session.queue(_agg([_day(0)], 5), _agg([], 4))
    with pytest.raises(CandleFeedError) as ei:
        client.get_liquidations_aggregated("BTCUSDT", page_size=1)
    assert ei.value.code == "incomplete_result" and len(ei.value.partial) == 1
    assert pd.Timestamp(ei.value.resume_start) == pd.Timestamp("2019-09-14T00:00:00Z")


def test_aggregated_empty_page_with_zero_total_is_done(client, fake_session):
    fake_session.queue(_agg([], 0))
    assert client.get_liquidations_aggregated("BTCUSDT").empty
