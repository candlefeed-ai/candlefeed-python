"""Regression tests from Astra's recheck 1 of PR #128 (2026-10-04). HTTP is mocked or on 127.0.0.1;
all credentials are dummy values."""
from __future__ import annotations

import ast
import gzip
import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pandas as pd
import pytest
import requests
import responses

from candlefeed import (
    AuthenticationError,
    CandleFeed,
    CandleFeedError,
    InvalidParameterError,
    QuotaExceededError,
    RateLimitError,
    TierRestrictedError,
)

ROOT = Path(__file__).resolve().parents[3]
BASE = 'http://127.0.0.1:1/v1'
KEY = 'cf_test_RECHECK_ONLY_12345'
DAY = '2026-09-01'


@pytest.fixture(autouse=True)
def local_only(monkeypatch):
    original = socket.socket.connect

    def connect(sock, address):
        if isinstance(address, tuple) and address[0] != '127.0.0.1':
            raise AssertionError('Review blocked non-loopback connection')
        return original(sock, address)

    monkeypatch.setattr(socket.socket, 'connect', connect)
    monkeypatch.setattr(socket, 'getfqdn', lambda name='': name or '127.0.0.1')


@pytest.mark.parametrize('field', ['message', 'code'])
@responses.activate
def test_client_scrubs_nested_error_fields(field):
    body = {'code': 'invalid_api_key', 'message': 'Rejected credential'}
    body[field] = {'echo': [KEY]}
    responses.add(responses.GET, BASE + '/candles', status=401, json=body)
    with pytest.raises(AuthenticationError) as caught:
        CandleFeed(api_key=KEY, base_url=BASE).get_candles('BTCUSDT')
    exc = caught.value
    assert exc.status_code == 401
    exposed = repr(exc.args) + repr(exc.message) + repr(exc.code)
    assert KEY not in exposed, 'Structured upstream error fields retain the configured key'
    assert KEY not in str(exc)


@pytest.mark.parametrize('status,code,kind', [
    (401, 'invalid_api_key', AuthenticationError),
    (403, 'tier_restricted', TierRestrictedError),
    (422, 'invalid_parameter', InvalidParameterError),
    (429, 'rate_limit_exceeded', RateLimitError),
    (429, 'quota_exceeded', QuotaExceededError),
    (500, 'internal_error', CandleFeedError),
])
@responses.activate
def test_scrubbing_preserves_exception_class_and_attributes(status, code, kind):
    responses.add(responses.GET, BASE + '/candles', status=status,
                  json={'code': code, 'message': 'Rejected ' + KEY}, headers={'Retry-After': '7'})
    with pytest.raises(kind) as caught:
        CandleFeed(api_key=KEY, base_url=BASE, max_retries=0).get_candles('BTCUSDT')
    exc = caught.value
    assert type(exc) is kind
    assert exc.status_code == status and exc.code == code
    assert KEY not in str(exc) + repr(exc.args) + exc.message
    if kind is RateLimitError:
        assert exc.retry_after == 7


@responses.activate
def test_real_64_mib_body_cap_refuses_instead_of_returning_prefix():
    import candlefeed.client as client_module
    assert client_module._MAX_API_BODY == 64 * 1024 ** 2
    # A syntactically complete response followed by whitespace must be refused,
    # even though returning its valid JSON prefix would otherwise look successful.
    raw = b'{"data": []}' + b' ' * client_module._MAX_API_BODY
    responses.add(responses.GET, BASE + '/candles', body=raw)
    with pytest.raises(CandleFeedError, match='over 67,108,864 bytes'):
        CandleFeed(api_key=KEY, base_url=BASE).get_candles('BTCUSDT')


@responses.activate
def test_64_mib_limit_accepts_complete_exact_boundary():
    import candlefeed.client as client_module
    prefix = b'{"data": [{"time": "2026-09-01T00:00:00Z", "close": 100}]}'
    raw = prefix + b' ' * (client_module._MAX_API_BODY - len(prefix))
    responses.add(responses.GET, BASE + '/candles', body=raw)
    frame = CandleFeed(api_key=KEY, base_url=BASE).get_candles('BTCUSDT')
    assert len(frame) == 1 and frame.iloc[0]['close'] == 100


@responses.activate
def test_compressed_body_is_capped_after_decoding():
    import candlefeed.client as client_module
    raw = b'{"data": []}' + b' ' * client_module._MAX_API_BODY
    responses.add(responses.GET, BASE + '/candles', body=gzip.compress(raw),
                  headers={'Content-Encoding': 'gzip'})
    with pytest.raises(CandleFeedError, match='over 67,108,864 bytes'):
        CandleFeed(api_key=KEY, base_url=BASE).get_candles('BTCUSDT')


def test_truncated_http_body_never_returns_valid_json_prefix(tmp_path):
    body = b'{"data": []}'

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            self.send_response(200)
            self.send_header('Content-Length', str(len(body) + 10))
            self.end_headers()
            self.wfile.write(body)

    backend = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    threading.Thread(target=backend.serve_forever, daemon=True).start()
    session = requests.Session()
    session.trust_env = False
    try:
        cf = CandleFeed(api_key=KEY, session=session, max_retries=0,
                        base_url=f'http://127.0.0.1:{backend.server_port}/v1')
        with pytest.raises(CandleFeedError, match='failed'):
            cf.get_candles('BTCUSDT')
    finally:
        session.close()
        backend.shutdown()
        backend.server_close()


def test_notebook_cumulative_carry_counts_each_native_settlement():
    notebook = json.loads((ROOT / 'clients/python/examples/funding_carry_backtest.ipynb').read_text())
    cell = next(c for c in notebook['cells'] if c['cell_type'] == 'code'
                and any('hourly = ' in line for line in c['source']))
    assignments = [node for node in ast.parse(''.join(cell['source'])).body
                   if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name)
                   and node.targets[0].id in ('wfr', 'hourly', 'total_pct', 'annualised')]
    # One venue, three native 8h settlements of 0.08%; API divides each by 8.
    # Collected funding is 0.24%, regardless of how a chart fills between events.
    ns = {'agg': pd.DataFrame({'weighted_funding_rate': [0.0001] * 3},
                            index=pd.date_range(DAY, periods=3, freq='8h', tz='UTC'))}
    exec(compile(ast.Module(body=assignments, type_ignores=[]), '<notebook arithmetic>', 'exec'), ns)
    assert ns['annualised'] == pytest.approx(0.0001 * 24 * 365 * 100)
    assert ns['total_pct'] == pytest.approx(3 * 8 * 0.0001 * 100), ns['total_pct']


@pytest.mark.parametrize('encoding', ['gzip', 'deflate'])
@responses.activate
def test_compressed_api_responses_still_decode(encoding):
    import zlib
    raw = b'{"data": [{"time": "2026-09-01T00:00:00Z", "close": 100}]}'
    body = gzip.compress(raw) if encoding == 'gzip' else zlib.compress(raw)
    responses.add(responses.GET, BASE + '/candles', body=body, headers={'Content-Encoding': encoding})
    frame = CandleFeed(api_key=KEY, base_url=BASE).get_candles('BTCUSDT')
    assert frame.iloc[0]['close'] == 100
    assert responses.calls[0].request.headers['Accept-Encoding'] == 'gzip, deflate'


@responses.activate
def test_unsupported_api_encoding_is_refused():
    responses.add(responses.GET, BASE + '/candles', body=b'xx', headers={'Content-Encoding': 'br'})
    with pytest.raises(CandleFeedError, match="Content-Encoding 'br'"):
        CandleFeed(api_key=KEY, base_url=BASE, max_retries=0).get_candles('BTCUSDT')


def _trickle_server(release):
    """Answers 200 with headers, then one byte every 10 ms until `release` is set."""
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            try:
                self.wfile.write(b'{')
                self.wfile.flush()
                while not release.wait(0.01):
                    self.wfile.write(b' ')
                    self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass

    backend = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    threading.Thread(target=backend.serve_forever, daemon=True).start()
    return backend


def test_a_trickling_rest_answer_stops_at_the_request_deadline():
    import time
    release = threading.Event()
    backend = _trickle_server(release)
    session = requests.Session()
    session.trust_env = False
    try:
        cf = CandleFeed(api_key=KEY, session=session, request_deadline=0.3,
                        base_url=f'http://127.0.0.1:{backend.server_port}/v1')
        started = time.monotonic()
        with pytest.raises(CandleFeedError, match='request_deadline') as caught:
            cf.get_candles('BTCUSDT')
        assert caught.value.code == 'request_deadline'
        assert time.monotonic() - started < 3          # not retried: one deadline, not max_retries + 1 of them
    finally:
        release.set()
        session.close()
        backend.shutdown()
        backend.server_close()


def test_request_deadline_defaults_to_60_seconds():
    assert CandleFeed(api_key=KEY).request_deadline == 60.0
