"""Astra's PR #133 review: a public client must never send or echo a key, and the sample's daily cap is a quota."""
from __future__ import annotations

import json

import pytest
import requests


class RecordingAdapter(requests.adapters.BaseAdapter):
    def __init__(self, status=200, body=None, headers=None):
        self.calls = []
        self.status, self.body, self.headers = status, body or {'status': 'ok', 'samples': []}, headers or {}

    def send(self, request, **kwargs):
        self.calls.append(request)
        response = requests.Response()
        response.status_code, response._content = self.status, json.dumps(self.body).encode()
        response.headers.update(self.headers)
        response.request = request
        response._content_consumed = True
        return response

    def close(self):
        pass


def test_pr133_public_client_strips_inherited_session_key():
    from candlefeed import CandleFeed
    session = requests.Session()
    session.headers['x-api-key'] = 'cf_live_private_session_key'
    adapter = RecordingAdapter()
    session.mount('https://', adapter)
    client = CandleFeed(public=True, api_key='cf_live_ignored_explicit_key', session=session)
    client.l2_sample()
    assert 'X-API-Key' not in adapter.calls[0].headers


def test_pr133_public_client_cannot_echo_inherited_key_in_error():
    from candlefeed import CandleFeed, CandleFeedError
    secret = 'cf_live_private_session_key'
    session = requests.Session()
    session.headers['X-API-Key'] = secret
    session.mount('https://', RecordingAdapter(401, {'detail': {'message': f'invalid {secret}'}}))
    client = CandleFeed(public=True, session=session)
    with pytest.raises(CandleFeedError) as exc:
        client.l2_sample()
    assert secret not in str(exc.value)


def test_pr133_daily_sample_limit_must_not_sleep_until_tomorrow(monkeypatch):
    import candlefeed.client as module
    from candlefeed import CandleFeed, QuotaExceededError
    session = requests.Session()
    session.mount('https://', RecordingAdapter(429, {'detail': {'code': 'sample_daily_limit',
                                                              'message': 'daily sample budget used'}},
                                               {'Retry-After': '82801'}))

    def no_sleep(seconds):
        pytest.fail(f'Client tried to sleep {seconds} seconds instead of reporting the daily quota')
    monkeypatch.setattr(module.time, 'sleep', no_sleep)
    with pytest.raises(QuotaExceededError):
        CandleFeed(public=True, session=session).l2_sample('DOTUSDT', '2026-10-01')
