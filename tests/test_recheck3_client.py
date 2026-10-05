"""Regression tests from Astra's recheck 3 of PR #128 (2026-10-04); local sockets only."""
import hashlib
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import requests
import urllib3

from candlefeed import CandleFeed, CandleFeedError
from candlefeed._safefs import SafeDir
from candlefeed.client import bounded_get

PAYLOAD = b'{"data": [{"time": "2026-09-01T00:00:00Z", "close": 100}]}'


HARD_STOP = ("needs a hard wall-clock stop inside one read; the socket watchdog that gave this was removed on "
             "2026-10-04. The elapsed deadline is checked between reads, so a stall inside one read is bounded "
             "by the socket read timeout only.")


@pytest.fixture(autouse=True)
def local_only(monkeypatch):
    original = socket.socket.connect
    def connect(sock, address):
        assert not isinstance(address, tuple) or address[0] == '127.0.0.1'
        return original(sock, address)
    monkeypatch.setattr(socket.socket, 'connect', connect)
    monkeypatch.setattr(socket, 'getfqdn', lambda name='': name or '127.0.0.1')



@contextmanager
def backend(serve, connect=None):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'
        def log_message(self, *args):
            pass
        def do_GET(self):
            try:
                serve(self)
            except (BrokenPipeError, ConnectionResetError):
                pass
        def do_CONNECT(self):
            try:
                connect(self)
            except (BrokenPipeError, ConnectionResetError):
                pass
    http = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    worker = threading.Thread(target=http.serve_forever, daemon=True)
    worker.start()
    try:
        yield f'http://127.0.0.1:{http.server_port}'
    finally:
        http.shutdown()
        http.server_close()
        worker.join(2)



def session():
    value = requests.Session()
    value.trust_env = False
    return value



def respond(h, body=PAYLOAD):
    h.send_response(200)
    h.send_header('Content-Length', str(len(body)))
    h.end_headers()
    h.wfile.write(body)
    h.wfile.flush()



def test_expired_watchdog_cannot_shutdown_reborrowed_keepalive_socket(monkeypatch):
    """Pause after real pool return, while another real request borrows the same TCP connection."""
    returned, resume, second_entered, send_second = [threading.Event() for _ in range(4)]
    ports = []
    original = urllib3.HTTPResponse.read1
    def scheduled_read(self, *args, **kwargs):
        data = original(self, *args, **kwargs)
        if threading.current_thread().name == 'first-request' and data:
            assert self._connection is None, 'Fixture must pause after real pool release'
            returned.set()
            assert resume.wait(5)
        return data
    monkeypatch.setattr(urllib3.HTTPResponse, 'read1', scheduled_read)
    def serve(h):
        ports.append(h.client_address[1])
        if h.path == '/second':
            second_entered.set()
            send_second.wait(5)
        respond(h)
    first_errors = []
    with backend(serve) as base, session() as sess:
        def first():
            try:
                bounded_get(sess, base + '/first', deadline=0.3, timeout=2)
            except Exception as exc:
                first_errors.append(exc)
        worker = threading.Thread(target=first, name='first-request')
        worker.start()
        with ThreadPoolExecutor(max_workers=1) as pool:
            try:
                assert returned.wait(3)
                second = pool.submit(bounded_get, sess, base + '/second', deadline=3, timeout=2)
                assert second_entered.wait(3)
                time.sleep(0.6)
                send_second.set()
                try:
                    result = second.result(3)
                except Exception as exc:
                    result = exc
            finally:
                resume.set()
                send_second.set()
                worker.join(3)
        assert len(ports) == 2 and ports[0] == ports[1], ports
        assert not isinstance(result, Exception), f'Healthy second request was killed: {result!r}'
        assert result.content == PAYLOAD



@pytest.mark.xfail(strict=True, reason=HARD_STOP + ' Here: proxy CONNECT headers.')
def test_proxy_connect_headers_obey_elapsed_deadline():
    entered, release = threading.Event(), threading.Event()
    def connect(h):
        h.wfile.write(b'HTTP/1.1 200 Connection Established\r\nX-Slow: ')
        h.wfile.flush()
        entered.set()
        while not release.wait(0.01):
            h.wfile.write(b'x')
            h.wfile.flush()
        # Close without beginning TLS; no target connection is made.
        h.close_connection = True
    with backend(lambda h: respond(h), connect=connect) as proxy, session() as sess:
        sess.proxies['https'] = proxy
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(bounded_get, sess, 'https://127.0.0.1:443/candles',
                                 deadline=0.2, timeout=0.1)
            try:
                assert entered.wait(3)
                time.sleep(0.8)
                finished_at_deadline = future.done()
            finally:
                release.set()
                try:
                    future.result(3)
                except Exception:
                    pass
    assert finished_at_deadline, 'CONNECT header parsing outlived the deadline fourfold'



@pytest.mark.skip(reason=HARD_STOP + ' Here: chunk trailers that never end. Without the watchdog the download blocks in http.client until the fixture releases, and the fixture only releases after the call returns, so it would hang. test_download_that_ends_after_its_deadline_is_not_committed covers the commit rule.')
def test_download_eof_after_deadline_is_not_committed(tmp_path):
    release = threading.Event()
    def serve(h):
        h.send_response(200)
        h.send_header('Transfer-Encoding', 'chunked')
        h.end_headers()
        h.wfile.write(f'{len(PAYLOAD):x}\r\n'.encode() + PAYLOAD + b'\r\n0\r\n')
        h.wfile.flush()
        while not release.wait(0.01):
            h.wfile.write(b'X-Trailer: padding\r\n')
            h.wfile.flush()
        h.wfile.write(b'\r\n')
    with backend(serve) as base, session() as sess:
        cf = CandleFeed(api_key='dummy', base_url=base, download_session=sess, max_retries=0)
        info = {'name': 'trades.parquet', 'size': len(PAYLOAD), 'key': 'k',
                'sha256': hashlib.sha256(PAYLOAD).hexdigest()}
        started = time.monotonic()
        deadline = started + 0.2
        cf._deadline_at = deadline
        error = None
        try:
            with SafeDir(tmp_path, []) as folder:
                try:
                    cf._fetch_l2_file({'k': base + '/file'}, info, folder, 'trades.parquet', True,
                                      lambda: None, deadline)
                except CandleFeedError as exc:
                    error = exc
        finally:
            release.set()
            cf.close()
    assert error is not None and error.code == 'download_deadline', (
        f'Expired download returned success after {time.monotonic()-started:.3f}s; '
        f'committed={(tmp_path / "trades.parquet").exists()}, error={error!r}')
    assert not (tmp_path / 'trades.parquet').exists()
    assert not (tmp_path / 'trades.parquet.part').exists()



def test_watchdogs_exit_after_success_and_timeout_bursts():
    def serve(h):
        if h.path == '/slow':
            h.wfile.write(b'HTTP/1.1 200 OK\r\nX-Slow: ')
            h.wfile.flush()
            for _ in range(50):
                time.sleep(0.005)
                h.wfile.write(b'x')
                h.wfile.flush()
        else:
            respond(h)
    before = {t.ident for t in threading.enumerate() if t.name == 'candlefeed-deadline'}
    with backend(serve) as base, session() as sess:
        for _ in range(30):
            assert bounded_get(sess, base, deadline=10).content == PAYLOAD
        for _ in range(10):
            with pytest.raises(CandleFeedError):
                bounded_get(sess, base + '/slow', deadline=0.02, timeout=1)
        # Timed-out connections must not poison subsequent pool use.
        assert bounded_get(sess, base, deadline=1).content == PAYLOAD
    end = time.monotonic() + 1
    while time.monotonic() < end:
        remaining = {t.ident for t in threading.enumerate() if t.name == 'candlefeed-deadline'} - before
        if not remaining:
            break
        time.sleep(0.01)
    assert not remaining



def test_normal_http_proxy_and_custom_adapter_preserved():
    seen = []
    with backend(lambda h: (seen.append(h.path), respond(h))) as proxy, session() as sess:
        sess.proxies['http'] = proxy
        assert bounded_get(sess, 'http://127.0.0.1:1/proxied', deadline=1).content == PAYLOAD
        assert seen == ['http://127.0.0.1:1/proxied']
    class CustomAdapter(requests.adapters.HTTPAdapter):
        pass
    with backend(respond) as base, session() as sess:
        adapter = CustomAdapter()
        sess.mount('http://', adapter)
        assert bounded_get(sess, base, deadline=1).content == PAYLOAD
        assert sess.adapters['http://'] is adapter
        assert adapter.poolmanager.pool_classes_by_scheme['http'] is urllib3.HTTPConnectionPool
    assert urllib3.poolmanager.pool_classes_by_scheme['http'] is urllib3.HTTPConnectionPool


def test_download_that_ends_after_its_deadline_is_not_committed(tmp_path):
    """The whole file arrives, but the response only ends (EOF) after the deadline: nothing is committed and
    the .part file is gone."""
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):                            # HTTP/1.0, no Content-Length: the body ends at close
            self.send_response(200)
            self.end_headers()
            self.wfile.write(PAYLOAD)
            self.wfile.flush()
            time.sleep(3)

    http = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    threading.Thread(target=http.serve_forever, daemon=True).start()
    try:
        with session() as sess:
            cf = CandleFeed(api_key='dummy', base_url='http://127.0.0.1:1', download_session=sess, max_retries=0)
            info = {'name': 'trades.parquet', 'size': len(PAYLOAD), 'key': 'k',
                    'sha256': hashlib.sha256(PAYLOAD).hexdigest()}
            deadline = time.monotonic() + 0.5
            with SafeDir(tmp_path, []) as folder:
                with pytest.raises(CandleFeedError) as caught:
                    cf._fetch_l2_file({'k': f'http://127.0.0.1:{http.server_port}/file'}, info, folder,
                                      'trades.parquet', True, lambda: None, deadline)
    finally:
        http.shutdown()
        http.server_close()
    assert caught.value.code == 'download_deadline'
    assert not (tmp_path / 'trades.parquet').exists() and not (tmp_path / 'trades.parquet.part').exists()


@pytest.mark.parametrize('verify', [True, False])
def test_a_download_with_the_wrong_hash_is_never_committed(tmp_path, verify):
    def serve(h):
        respond(h)
    with backend(serve) as base, session() as sess:
        cf = CandleFeed(api_key='dummy', base_url=base, download_session=sess, max_retries=0)
        info = {'name': 'trades.parquet', 'size': len(PAYLOAD), 'key': 'k', 'sha256': '0' * 64}
        with SafeDir(tmp_path, []) as folder:
            with pytest.raises(CandleFeedError, match='SHA-256 mismatch'):
                cf._fetch_l2_file({'k': base + '/file'}, info, folder, 'trades.parquet', verify, lambda: None)
    assert not (tmp_path / 'trades.parquet').exists() and not (tmp_path / 'trades.parquet.part').exists()


def test_bounded_get_sends_exactly_gzip_deflate_on_a_plain_session():
    seen = []

    def serve(h):
        seen.append(h.headers.get('Accept-Encoding'))
        respond(h)
    with backend(serve) as base, session() as sess:
        sess.headers['Accept-Encoding'] = 'gzip, deflate, br, zstd'        # a session default is overridden
        assert bounded_get(sess, base, deadline=5).content == PAYLOAD
        assert bounded_get(requests.Session(), base, deadline=5).content == PAYLOAD
    assert seen == ['gzip, deflate', 'gzip, deflate']
