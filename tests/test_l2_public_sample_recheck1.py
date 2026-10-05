"""PR 133 recheck: exercise real requests header merging without any sockets."""
from __future__ import annotations

import json

import pytest
import requests
from test_l2 import BODY_A, BODY_M, _day

from candlefeed import CandleFeed, CandleFeedError

SECRET = "cf_live_recheck_download_session_key"


class SampleAdapter(requests.adapters.BaseAdapter):
    def __init__(self, fail_download=False):
        self.calls = []
        self.fail_download = fail_download

    def send(self, request, **kwargs):
        self.calls.append(request)
        if "/l2/sample" in request.url:
            body = json.dumps({"status": "ok", "days": [_day("2026-10-01")]}).encode()
        elif self.fail_download:
            raise requests.ConnectionError(f"download transport rejected {SECRET}")
        else:
            body = BODY_M if "/manifest.json?" in request.url else BODY_A
        response = requests.Response()
        response.status_code = 200
        response._content = body
        response._content_consumed = True
        response.request = request
        return response

    def close(self):
        pass


@pytest.mark.parametrize("shared", [False, True])
def test_recheck_public_download_strips_inherited_storage_key(tmp_path, shared):
    session = requests.Session()
    download_session = session if shared else requests.Session()
    download_session.headers["x-aPi-kEy"] = SECRET
    adapter = SampleAdapter()
    session.mount("https://", adapter)
    download_session.mount("https://", adapter)
    client = CandleFeed(public=True, session=session, download_session=download_session,
                        storage_host="sgp1.example", max_retries=0)
    result = client.download_l2_sample("BTCUSDT", "2026-10-01", tmp_path)
    assert len(result["downloaded"]) == 2
    assert "X-API-Key" not in adapter.calls[0].headers  # the API request is fixed
    assert all("X-API-Key" not in call.headers for call in adapter.calls[1:])


def test_recheck_public_download_error_redacts_storage_session_key(tmp_path):
    session, download_session = requests.Session(), requests.Session()
    download_session.headers["X-API-Key"] = SECRET
    adapter = SampleAdapter(fail_download=True)
    session.mount("https://", adapter)
    download_session.mount("https://", adapter)
    client = CandleFeed(public=True, session=session, download_session=download_session,
                        storage_host="sgp1.example", max_retries=0)
    with pytest.raises(CandleFeedError) as caught:
        client.download_l2_sample("BTCUSDT", "2026-10-01", tmp_path)
    assert SECRET not in str(caught.value)


@pytest.mark.parametrize("header", ["x-api-key", "X-API-KEY", "x-ApI-kEy"])
def test_recheck_public_suppresses_keys_added_to_api_session_after_creation(header):
    session = requests.Session()
    adapter = SampleAdapter()
    session.mount("https://", adapter)
    client = CandleFeed(public=True, session=session)
    client.l2_sample()
    session.headers[header] = SECRET
    client.l2_sample()
    assert all("X-API-Key" not in call.headers for call in adapter.calls)
    assert session.headers[header] == SECRET  # shared authenticated clients keep their credential
