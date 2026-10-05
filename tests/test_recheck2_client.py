"""Regression tests from Astra's recheck 2 of PR #128 (2026-10-04). HTTP is mocked or on 127.0.0.1;
all credentials are dummy values."""
from __future__ import annotations

import gzip
import io
import json
import socket
import subprocess
import sys
import threading
import time
import zlib
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
import requests
import responses
import urllib3

from candlefeed import AuthenticationError, CandleFeed, CandleFeedError

KEY = 'cf_test_RECHECK2_DUMMY'
BASE = 'http://127.0.0.1:1/v1'
PAYLOAD = b'{"data": [{"time": "2026-09-01T00:00:00Z", "close": 100}]}'


@pytest.fixture(autouse=True)
def local_only(monkeypatch):
    original = socket.socket.connect
    def connect(sock, address):
        if isinstance(address, tuple) and address[0] != '127.0.0.1':
            raise AssertionError('Non-loopback connection refused')
        return original(sock, address)
    monkeypatch.setattr(socket.socket, 'connect', connect)
    monkeypatch.setattr(socket, 'getfqdn', lambda name='': name or '127.0.0.1')


@contextmanager
def backend(serve):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass
        def do_GET(self):
            try:
                serve(self)
            except (BrokenPipeError, ConnectionResetError):
                pass
    http = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    threading.Thread(target=http.serve_forever, daemon=True).start()
    try:
        yield f'http://127.0.0.1:{http.server_port}/v1'
    finally:
        http.shutdown()
        http.server_close()


def respond(h, body, encoding='identity'):
    h.send_response(200)
    h.send_header('Content-Length', str(len(body)))
    h.send_header('Content-Encoding', encoding)
    h.end_headers()
    h.wfile.write(body)


def test_supported_urllib3_1x_can_read_a_rest_response():
    # Actual cached 1.26.6 implementation, not a simulated missing-method object.
    vendor = Path('/Library/Developer/CommandLineTools/Library/Frameworks/Python3.framework/Versions/3.9/lib/python3.9/site-packages')
    if not (vendor / 'pip/_vendor/urllib3/response.py').exists():
        pytest.skip('needs the macOS Command Line Tools pip-vendored urllib3 1.26.6')
    code = '''
import sys, socket
sys.path.insert(0, sys.argv[1])
import pip._vendor.urllib3 as legacy
sys.path.pop(0)
for name, module in list(sys.modules.items()):
    if name == 'pip._vendor.urllib3' or name.startswith('pip._vendor.urllib3.'):
        sys.modules[name.removeprefix('pip._vendor.')] = module
old = socket.socket.connect
def connect(self, address):
    assert not isinstance(address, tuple) or address[0] == '127.0.0.1'
    return old(self, address)
socket.socket.connect = connect
from candlefeed import CandleFeed
import requests
session = requests.Session()
session.trust_env = False
print('requests', requests.__version__, 'urllib3', legacy.__version__, flush=True)
cf = CandleFeed(api_key='dummy', session=session, base_url=sys.argv[2], max_retries=0)
assert cf.get_candles('BTCUSDT').iloc[0]['close'] == 100
'''
    with backend(lambda h: respond(h, PAYLOAD)) as base:
        proc = subprocess.run([sys.executable, '-c', code, str(vendor), base],
                              text=True, capture_output=True, timeout=10)
    assert proc.returncode == 0, proc.stdout + proc.stderr


@pytest.mark.parametrize('encoding', ['gzip', 'deflate'])
@pytest.mark.parametrize('case', ['single', 'members', 'truncated', 'bad_checksum', 'trailing_junk', 'exact_cap', 'over_cap'])
def test_decoder_boundaries(encoding, case, monkeypatch):
    import candlefeed.client as module
    compress = gzip.compress if encoding == 'gzip' else zlib.compress
    body = compress(PAYLOAD)
    if case == 'members':
        body = compress(PAYLOAD[:17]) + compress(PAYLOAD[17:])
    elif case == 'truncated':
        body = body[:-1]
    elif case == 'bad_checksum':
        damaged = bytearray(body)
        damaged[-4] ^= 1
        body = bytes(damaged)
    elif case == 'trailing_junk':
        body += b'junk'
    monkeypatch.setattr(module, '_MAX_API_BODY', len(PAYLOAD) - (case == 'over_cap'))
    with backend(lambda h: respond(h, body, encoding)) as base:
        session = requests.Session()
        session.trust_env = False
        with CandleFeed(api_key=KEY, base_url=base, session=session, max_retries=0) as cf:
            if case in ('truncated', 'bad_checksum', 'trailing_junk', 'over_cap'):
                with pytest.raises(CandleFeedError):
                    cf.get_candles('BTCUSDT')
            else:
                assert cf.get_candles('BTCUSDT').iloc[0]['close'] == 100


@pytest.mark.parametrize('split', [1, 2, 9, 10, 19, 30, 59, 1000])
def test_multimember_gzip_across_read_boundaries(split):
    wire = gzip.compress(PAYLOAD[:20]) + gzip.compress(b'') + gzip.compress(PAYLOAD[20:])
    class Fragmented(io.BytesIO):
        def read1(self, amount=-1):
            return super().read(min(amount, split))
    response = requests.Response()
    response.headers['Content-Encoding'] = 'gzip'
    response.raw = urllib3.HTTPResponse(body=Fragmented(wire), preload_content=False)
    assert b''.join(CandleFeed._body_chunks(response, 'test', len(PAYLOAD))) == PAYLOAD


def test_encoded_comment_cap(monkeypatch):
    import candlefeed.client as module
    monkeypatch.setattr(module, '_MAX_API_BODY', 128)
    compressed = gzip.compress(PAYLOAD)
    header = bytearray(compressed[:10])
    header[3] |= 16
    body = bytes(header) + b'x' * 70000 + b'\0' + compressed[10:]
    with backend(lambda h: respond(h, body, 'gzip')) as base:
        with CandleFeed(api_key=KEY, base_url=base, max_retries=0) as cf:
            with pytest.raises(CandleFeedError, match='over 128 bytes'):
                cf.get_candles('BTCUSDT')


@pytest.mark.parametrize('field', ['code', 'message'])
@responses.activate
def test_nested_fastapi_detail_is_scrubbed(field):
    detail = {'code': 'invalid_api_key', 'message': 'bad'}
    detail[field] = {'nested': [KEY]}
    responses.add(responses.GET, BASE + '/candles', status=401, json={'detail': detail})
    with pytest.raises(AuthenticationError) as exc:
        CandleFeed(api_key=KEY, base_url=BASE).get_candles('BTCUSDT')
    assert KEY not in str(exc.value) + repr(exc.value.__dict__) + repr(exc.value.args)


def test_fifty_thousand_row_page_with_default_deadline():
    body = json.dumps({'data': [
        {'time': '2026-09-01T00:00:00Z', 'open': 100, 'high': 101, 'low': 99,
         'close': 100 + i / 100000, 'volume': 1000} for i in range(50000)]}).encode()
    def serve(h):
        h.send_response(200)
        h.send_header('Content-Length', str(len(body)))
        h.end_headers()
        for start in range(0, len(body), 65536):
            h.wfile.write(body[start:start + 65536])
            h.wfile.flush()
            time.sleep(0.01)
    with backend(serve) as base:
        with CandleFeed(api_key=KEY, base_url=base) as cf:
            start = time.monotonic()
            frame = cf.get_candles('BTCUSDT', limit=50000)
            elapsed = time.monotonic() - start
    print(f'50k page: {len(body)} bytes, {elapsed:.3f}s, {len(frame)} rows')
    assert len(frame) == 50000 and frame.iloc[-1]['close'] == 100.49999


def test_retry_starts_a_fresh_request_deadline():
    attempts = []
    def serve(h):
        attempts.append(time.monotonic())
        if len(attempts) == 1:
            h.send_response(429)
            h.send_header('Retry-After', '1')
            h.send_header('Content-Length', '2')
            h.end_headers()
            h.wfile.write(b'{}')
        else:
            respond(h, PAYLOAD)
    with backend(serve) as base:
        with CandleFeed(api_key=KEY, base_url=base, request_deadline=0.2, max_retries=1) as cf:
            assert cf.get_candles('BTCUSDT').iloc[0]['close'] == 100
    assert len(attempts) == 2 and attempts[1] - attempts[0] >= 1


def test_real_sixty_second_budget_and_none_on_valid_slow_50k_page():
    """Real wall-clock run: valid ~6 MB page over a ~0.75 Mbps link, two concurrent clients."""
    from concurrent.futures import ThreadPoolExecutor
    body = json.dumps({'data': [
        {'time': '2026-09-01T00:00:00Z', 'open': 100, 'high': 101, 'low': 99,
         'close': 100 + i / 100000, 'volume': 1000} for i in range(50000)]}).encode()
    chunk_size = 65536
    chunks = (len(body) + chunk_size - 1) // chunk_size
    attempts = []
    def serve(h):
        attempts.append(h.path)
        h.send_response(200)
        h.send_header('Content-Length', str(len(body)))
        h.end_headers()
        for start in range(0, len(body), chunk_size):
            h.wfile.write(body[start:start + chunk_size])
            h.wfile.flush()
            time.sleep(64 / chunks)
    with backend(serve) as base:
        def fetch(deadline):
            session = requests.Session()
            session.trust_env = False
            with CandleFeed(api_key=KEY, session=session, base_url=base,
                            request_deadline=deadline) as cf:
                started = time.monotonic()
                try:
                    result = cf.get_candles('BTCUSDT', limit=50000)
                except CandleFeedError as exc:
                    result = exc
                return result, time.monotonic() - started
        with ThreadPoolExecutor(max_workers=2) as pool:
            normal = pool.submit(fetch, 60)
            disabled = pool.submit(fetch, None)
            error, elapsed = normal.result(timeout=75)
            frame, elapsed_disabled = disabled.result(timeout=75)
    print(f'{len(body)} bytes; default: {elapsed:.3f}s; None: {elapsed_disabled:.3f}s; requests: {len(attempts)}')
    assert isinstance(error, CandleFeedError) and error.code == 'request_deadline'
    assert 60 <= elapsed < 63 and len(frame) == 50000
    assert len(attempts) == 2, 'Deadline failures must not retry'


def test_urllib3_without_read1_falls_back_to_read():
    """urllib3 1.26 has no HTTPResponse.read1; the reader must use read() there, with the same bounds."""
    class Legacy(urllib3.HTTPResponse):
        @property
        def read1(self):
            raise AttributeError('read1')
    for wire, encoding in ((PAYLOAD, 'identity'), (gzip.compress(PAYLOAD), 'gzip')):
        response = requests.Response()
        response.headers['Content-Encoding'] = encoding
        response.raw = Legacy(body=io.BytesIO(wire), preload_content=False)
        assert b''.join(CandleFeed._body_chunks(response, 'test', len(PAYLOAD))) == PAYLOAD


def test_requests_after_a_deadline_on_the_same_session_succeed():
    """A request that runs out of time must not leave the session or its connection pool unusable."""
    calls = []

    def serve(h):
        calls.append(h.path)
        if len(calls) == 1:                         # first answer: headers, then a slow body
            h.send_response(200)
            h.send_header('Content-Length', str(len(PAYLOAD)))
            h.end_headers()
            for byte in PAYLOAD[:20]:
                h.wfile.write(bytes([byte]))
                h.wfile.flush()
                time.sleep(0.1)
        else:
            respond(h, PAYLOAD)

    with backend(serve) as base:
        session = requests.Session()
        session.trust_env = False
        try:
            cf = CandleFeed(api_key=KEY, base_url=base, session=session, request_deadline=0.5, max_retries=0)
            with pytest.raises(CandleFeedError) as caught:
                cf.get_candles('BTCUSDT')
            assert caught.value.code == 'request_deadline'
            cf.request_deadline = 30
            assert cf.get_candles('BTCUSDT').iloc[0]['close'] == 100
            assert cf.get_candles('BTCUSDT').iloc[0]['close'] == 100
        finally:
            session.close()
    assert len(calls) == 3
