"""Astra #3: a redirect from the API must not carry X-API-Key to another origin."""
from __future__ import annotations

import pytest
import requests
import responses

from candlefeed import CandleFeed, CandleFeedError

KEY = "cf_live_REDIRECTKEY123456"


@pytest.mark.parametrize("target", ["https://evil.test/collect", "http://candlefeed.ai/api/v1/candles",
                                    "https://candlefeed.ai/api/v1/elsewhere"])
def test_api_redirects_are_not_followed_and_the_key_stays_put(target):
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        rsps.add(responses.GET, "https://candlefeed.ai/api/v1/candles", status=302, headers={"Location": target})
        rsps.add(responses.GET, target.split("?")[0], json={"status": "ok", "data": []})
        cf = CandleFeed(api_key=KEY, session=requests.Session())
        with pytest.raises(CandleFeedError, match="redirect") as exc:
            cf.get_ohlcv("BTCUSDT", limit=1)
        assert len(rsps.calls) == 1
        assert rsps.calls[0].request.url.startswith("https://candlefeed.ai/api/v1/candles")
        assert KEY not in str(exc.value)


def test_without_the_guard_requests_would_forward_the_key():
    """Documents why the guard exists: requests keeps custom headers across origins."""
    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, "https://candlefeed.ai/api/v1/candles", status=302,
                 headers={"Location": "https://evil.test/collect"})
        rsps.add(responses.GET, "https://evil.test/collect", json={})
        s = requests.Session()
        s.headers["X-API-Key"] = KEY
        s.get("https://candlefeed.ai/api/v1/candles")
        assert rsps.calls[1].request.headers.get("X-API-Key") == KEY
