"""L2 file listing and downloads, fully offline: a fake API session plus a fake storage session."""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Dict, List

import pytest
import requests
from conftest import FakeResponse, error

import candlefeed.client as client_module
from candlefeed import CandleFeed, CandleFeedError, QuotaExceededError


class FakeStream:
    def __init__(self, status: int, body: bytes = b"", fail_after: int = -1) -> None:
        self.status_code = status
        self._body = body
        self._fail_after = fail_after
        self.closed = False

    def iter_content(self, chunk_size: int = 1):
        for i in range(0, len(self._body), 4):
            if self._fail_after >= 0 and i >= self._fail_after:
                raise requests.ConnectionError("connection reset")
            yield self._body[i:i + 4]

    def close(self) -> None:
        self.closed = True


class FakeStorage:
    """Plays the presigned-URL host. script[url] is a list of responses served in order."""

    def __init__(self) -> None:
        self.headers: Dict[str, str] = {}
        self.calls: List[str] = []
        self.script: Dict[str, list] = {}
        self.timeouts: list = []

    def get(self, url: str, stream: bool = False, timeout: float = 0, allow_redirects: bool = True) -> FakeStream:
        assert stream and allow_redirects is False
        self.calls.append(url)
        self.timeouts.append(timeout)
        queue = self.script[url.split("?")[0]]
        return queue.pop(0) if len(queue) > 1 else queue[0]


def _file(name: str, body: bytes, gen: str = "g1", day: str = "2026-09-01") -> dict:
    key = f"canonical/book/binance/BTCUSDT/{day}/gen={gen}/{name}"
    return {"name": name, "key": key, "size": len(body), "sha256": hashlib.sha256(body).hexdigest(),
            "rows": 1, "url": f"https://sgp1.example/{key}?X-Amz-Signature={gen}"}


def _listing(days, missing=()):
    return FakeResponse(200, {"status": "ok", "days": days, "missing": list(missing), "usage": None})


@pytest.fixture
def storage() -> FakeStorage:
    return FakeStorage()


@pytest.fixture
def cf(fake_session, storage, monkeypatch) -> CandleFeed:
    c = CandleFeed(api_key="cf_live_testkey", session=fake_session, download_session=storage,
                   storage_host="sgp1.example")
    monkeypatch.setattr(client_module.time, "sleep", lambda s: fake_session.sleeps.append(s))
    return c


BODY_A = b"depth-hour-zero-bytes"
BODY_M = b'{"manifest": true}'


def _day(day="2026-09-01", gen="g1"):
    return {"date": day, "files": [_file("depth/00.parquet", BODY_A, gen, day), _file("manifest.json", BODY_M, gen, day)]}


def _serve(storage, day_obj, bodies):
    for f, body in zip(day_obj["files"], bodies):
        storage.script[f["url"].split("?")[0]] = [FakeStream(200, body)]


def test_l2_files_sends_params(cf, fake_session):
    fake_session.queue(_listing([]))
    cf.l2_files("book", "btcusdt", "2026-09-01", "2026-09-03")
    call = fake_session.calls[0]
    assert call["url"].endswith("/l2/files")
    assert call["params"] == {"dataset": "book", "symbol": "BTCUSDT", "start": "2026-09-01", "end": "2026-09-03"}


def test_download_writes_verified_files_and_never_sends_the_api_key(cf, fake_session, storage, tmp_path):
    day = _day()
    fake_session.queue(_listing([day], missing=[{"date": "2026-09-02", "reason": "not_published"}]))
    _serve(storage, day, [BODY_A, BODY_M])
    out = cf.download_l2("book", "BTCUSDT", "2026-09-01", "2026-09-02", tmp_path)
    target = tmp_path / "book" / "binance" / "BTCUSDT" / "2026-09-01"
    assert (target / "depth" / "00.parquet").read_bytes() == BODY_A
    assert (target / "manifest.json").read_bytes() == BODY_M
    assert out["missing"] == [{"date": "2026-09-02", "reason": "not_published"}]
    assert out["bytes"] == len(BODY_A) + len(BODY_M) and len(out["downloaded"]) == 2
    assert "X-API-Key" not in storage.headers
    assert all("sgp1.example" not in c["url"] for c in fake_session.calls)
    assert not list(target.rglob("*.part"))


def test_files_already_present_with_matching_hash_are_skipped(cf, fake_session, storage, tmp_path):
    day = _day()
    target = tmp_path / "book" / "binance" / "BTCUSDT" / "2026-09-01"
    (target / "depth").mkdir(parents=True)
    (target / "depth" / "00.parquet").write_bytes(BODY_A)
    (target / "manifest.json").write_bytes(b'{"manifest": fals}')   # same size, wrong bytes
    fake_session.queue(_listing([day]))
    _serve(storage, day, [BODY_A, BODY_M])
    out = cf.download_l2("book", "BTCUSDT", "2026-09-01", None, tmp_path)
    assert out["skipped"] == [str(target / "depth" / "00.parquet")]
    assert out["downloaded"] == [str(target / "manifest.json")]
    assert (target / "manifest.json").read_bytes() == BODY_M
    assert len(storage.calls) == 1


def test_transient_errors_are_retried(cf, fake_session, storage, tmp_path):
    day = _day()
    fake_session.queue(_listing([day]))
    a, m = (f["url"].split("?")[0] for f in day["files"])
    storage.script[a] = [FakeStream(503), FakeStream(200, BODY_A, fail_after=8), FakeStream(200, BODY_A)]
    storage.script[m] = [FakeStream(200, b"x" * len(BODY_M)), FakeStream(200, BODY_M)]
    out = cf.download_l2("book", "BTCUSDT", "2026-09-01", None, tmp_path)
    assert len(out["downloaded"]) == 2 and len(storage.calls) == 5
    assert (tmp_path / "book/binance/BTCUSDT/2026-09-01/manifest.json").read_bytes() == BODY_M


def test_persistent_hash_mismatch_raises_and_leaves_no_partial_file(cf, fake_session, storage, tmp_path):
    day = _day()
    fake_session.queue(_listing([day]))
    storage.script[day["files"][0]["url"].split("?")[0]] = [FakeStream(200, b"z" * len(BODY_A))]
    with pytest.raises(CandleFeedError, match="SHA-256 mismatch"):
        cf.download_l2("book", "BTCUSDT", "2026-09-01", None, tmp_path)
    assert not list(tmp_path.rglob("*.part")) and not list(tmp_path.rglob("00.parquet"))


def test_expired_link_is_refreshed(cf, fake_session, storage, tmp_path):
    old, new = _day(gen="g1"), _day(gen="g1")
    for f in new["files"]:
        f["url"] = f["url"].replace("Signature=g1", "Signature=fresh")
    fake_session.queue(_listing([old]), _listing([new]))
    a = old["files"][0]["url"].split("?")[0]
    storage.script[a] = [FakeStream(403), FakeStream(200, BODY_A)]
    storage.script[old["files"][1]["url"].split("?")[0]] = [FakeStream(200, BODY_M)]
    cf.download_l2("book", "BTCUSDT", "2026-09-01", None, tmp_path)
    assert storage.calls[1].endswith("Signature=fresh")
    assert fake_session.calls[1]["params"]["start"] == "2026-09-01"


def test_long_ranges_are_split_into_31_day_calls(cf, fake_session, tmp_path):
    fake_session.queue(_listing([]), _listing([]), _listing([]))
    cf.download_l2("trades", "ETHUSDT", "2026-06-06", "2026-08-10", tmp_path)
    spans = [(c["params"]["start"], c["params"]["end"]) for c in fake_session.calls]
    assert spans == [("2026-06-06", "2026-07-06"), ("2026-07-07", "2026-08-06"), ("2026-08-07", "2026-08-10")]


def test_unsafe_names_from_the_server_are_refused(cf, fake_session, tmp_path):
    day = {"date": "2026-09-01", "files": [_file("../../escape.parquet", b"x")]}
    fake_session.queue(_listing([day]))
    with pytest.raises(CandleFeedError, match="unsafe"):
        cf.download_l2("book", "BTCUSDT", "2026-09-01", None, tmp_path)
    assert not (Path(tmp_path).parent / "escape.parquet").exists()


@pytest.mark.parametrize("code", ["quota_exceeded", "daily_download_limit"])
def test_quota_429_is_raised_without_retrying(cf, fake_session, code):
    fake_session.queue(error(429, code, "Sample-day downloads are capped at 10 GiB per month."))
    with pytest.raises(QuotaExceededError) as exc:
        cf.l2_files("book", "BTCUSDT", "2026-09-01")
    assert exc.value.code == code and len(fake_session.calls) == 1 and fake_session.sleeps == []


def test_network_errors_never_carry_the_presigned_query(cf, fake_session, storage, tmp_path):
    # Astra #12: requests puts the full URL (with its signature) in connection errors
    day = _day()
    fake_session.queue(_listing([day]))
    url = day["files"][0]["url"] + "&X-Amz-Credential=DO00SECRETKEYID&X-Amz-Signature=deadbeef"
    day["files"][0]["url"] = url

    class Boom(FakeStream):
        def iter_content(self, chunk_size=1):
            raise requests.ConnectionError(f"HTTPSConnectionPool: Max retries exceeded with url: {url} (reset)")
            yield b""

    storage.script[url.split("?")[0]] = [Boom(200)]
    with pytest.raises(CandleFeedError) as exc:
        cf.download_l2("book", "BTCUSDT", "2026-09-01", None, tmp_path)
    text = str(exc.value) + repr(exc.value.__cause__) + repr(exc.value.__context__)
    assert "X-Amz-Signature" not in text and "deadbeef" not in text and "DO00SECRETKEYID" not in text
    assert "ConnectionError" in text and "?<redacted>" in text


def test_api_request_errors_are_sanitised_and_unchained(cf, fake_session, monkeypatch):
    def fail(url, params=None, timeout=0, allow_redirects=True):
        raise requests.ConnectionError(f"Max retries exceeded with url: {url}?token=abc123")

    monkeypatch.setattr(fake_session, "get", fail)
    with pytest.raises(CandleFeedError) as exc:
        cf.l2_files("book", "BTCUSDT", "2026-09-01")
    assert "abc123" not in str(exc.value) and exc.value.__cause__ is None


@pytest.mark.parametrize("message", [
    # Astra recheck #8: urllib3 reports a relative URL, which the https:// pattern missed
    "HTTPSConnectionPool(host='candlefeed-l2-canonical.sgp1.digitaloceanspaces.com', port=443): Max retries "
    "exceeded with url: /canonical/book/x.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=DO00KEYID"
    "%2F20261003&X-Amz-Signature=deadbeef (Caused by NewConnectionError('reset'))",
    "error for /canonical/book/x.parquet%3FX-Amz-Signature%3Ddeadbeef%26X-Amz-Credential%3DDO00KEYID",
    "signed params leaked bare: X-Amz-Signature=deadbeef X-Amz-Credential=DO00KEYID",
])
def test_relative_and_encoded_presigned_queries_are_redacted(message):
    from candlefeed.client import _describe_error

    out = _describe_error(requests.ConnectionError(message))
    assert "deadbeef" not in out and "DO00KEYID" not in out
    assert out.startswith("ConnectionError: ")


def test_coverage_and_gaps_send_their_filters(cf, fake_session):
    fake_session.queue(FakeResponse(200, {"status": "ok", "datasets": {}}),
                       FakeResponse(200, {"status": "ok", "gaps": []}))
    cf.l2_coverage("book", "btcusdt")
    cf.l2_gaps("trades", "dotusdt", "2026-08-01", "2026-08-31")
    assert fake_session.calls[0]["url"].endswith("/l2/coverage")
    assert fake_session.calls[0]["params"] == {"dataset": "book", "symbol": "BTCUSDT"}
    assert fake_session.calls[1]["url"].endswith("/l2/gaps")
    assert fake_session.calls[1]["params"] == {"dataset": "trades", "symbol": "DOTUSDT",
                                               "start": "2026-08-01", "end": "2026-08-31"}
    with pytest.raises(CandleFeedError):
        cf.l2_gaps("depth")


@pytest.mark.parametrize("url", [
    "http://sgp1.example/canonical/x?X-Amz-Signature=s",                 # not https
    "https://127.0.0.1/canonical/x?X-Amz-Signature=s",                   # localhost
    "https://169.254.169.254/latest/meta-data/",                         # cloud metadata
    "https://10.0.0.5/canonical/x",                                      # private network
    "https://evil.test/canonical/x?X-Amz-Signature=s",                   # another host
    "https://sgp1.example.evil.test/canonical/x",                        # suffix trick
    "https://sgp1.example:8443/canonical/x",                             # wrong port
    "https://user:pass@sgp1.example/canonical/x",                        # credentials in the URL
    "https://sgp1.example:99999/canonical/x",                            # unparseable port
])
def test_download_links_must_be_https_to_the_storage_host(cf, fake_session, storage, tmp_path, url):
    """Astra #2: a listing can't point the downloader anywhere but the storage host."""
    day = _day()
    day["files"][0]["url"] = url
    fake_session.queue(_listing([day]))
    with pytest.raises(CandleFeedError, match="Refusing a download link"):
        cf.download_l2("book", "BTCUSDT", "2026-09-01", None, tmp_path)
    assert storage.calls == []


def test_storage_redirects_are_not_followed(cf, fake_session, storage, tmp_path):
    day = _day()
    fake_session.queue(_listing([day]))
    storage.script[day["files"][0]["url"].split("?")[0]] = [FakeStream(302)]
    with pytest.raises(CandleFeedError, match="redirect"):
        cf.download_l2("book", "BTCUSDT", "2026-09-01", None, tmp_path)
    assert len(storage.calls) == 1


def test_storage_host_defaults_to_the_canonical_bucket_and_env_overrides(monkeypatch):
    monkeypatch.delenv("CANDLEFEED_L2_STORAGE_HOST", raising=False)
    assert CandleFeed(api_key="k").storage_host == "candlefeed-l2-canonical.sgp1.digitaloceanspaces.com"
    monkeypatch.setenv("CANDLEFEED_L2_STORAGE_HOST", "Other.Example")
    assert CandleFeed(api_key="k").storage_host == "other.example"


# ---- Astra #1: nothing is written outside dest_dir, whatever symlinks sit below it -------------------

def _outside(tmp_path):
    out = tmp_path / "outside"
    out.mkdir()
    victim = out / "victim.bin"
    victim.write_bytes(b"do not touch")
    return out, victim


def test_symlinked_depth_folder_is_refused_and_nothing_lands_outside(cf, fake_session, storage, tmp_path):
    out, victim = _outside(tmp_path)
    dest = tmp_path / "dest"
    day_dir = dest / "book" / "binance" / "BTCUSDT" / "2026-09-01"
    day_dir.mkdir(parents=True)
    os.symlink(out, day_dir / "depth")
    day = _day()
    fake_session.queue(_listing([day]))
    _serve(storage, day, [BODY_A, BODY_M])
    with pytest.raises(CandleFeedError, match="symlink"):
        cf.download_l2("book", "BTCUSDT", "2026-09-01", None, dest)
    assert sorted(p.name for p in out.iterdir()) == ["victim.bin"] and victim.read_bytes() == b"do not touch"


@pytest.mark.parametrize("link", ["BTCUSDT", "2026-09-01", "binance"])
def test_symlinked_parent_folders_are_refused(cf, fake_session, storage, tmp_path, link):
    out, victim = _outside(tmp_path)
    dest = tmp_path / "dest"
    chain = ["book", "binance", "BTCUSDT", "2026-09-01"]
    parent = dest.joinpath(*chain[:chain.index(link)])
    parent.mkdir(parents=True)
    os.symlink(out, parent / link)
    day = _day()
    fake_session.queue(_listing([day]))
    _serve(storage, day, [BODY_A, BODY_M])
    with pytest.raises(CandleFeedError, match="symlink"):
        cf.download_l2("book", "BTCUSDT", "2026-09-01", None, dest)
    assert sorted(p.name for p in out.iterdir()) == ["victim.bin"] and storage.calls == []


def test_planted_part_symlink_cannot_truncate_an_outside_file(cf, fake_session, storage, tmp_path):
    out, victim = _outside(tmp_path)
    dest = tmp_path / "dest"
    depth = dest / "book" / "binance" / "BTCUSDT" / "2026-09-01" / "depth"
    depth.mkdir(parents=True)
    os.symlink(victim, depth / "00.parquet.part")
    day = _day()
    fake_session.queue(_listing([day]))
    _serve(storage, day, [BODY_A, BODY_M])
    cf.download_l2("book", "BTCUSDT", "2026-09-01", None, dest)
    assert victim.read_bytes() == b"do not touch"
    assert (depth / "00.parquet").read_bytes() == BODY_A and not (depth / "00.parquet").is_symlink()
    assert not (depth / "00.parquet.part").exists() and not (depth / "00.parquet.part").is_symlink()


def test_symlinked_final_file_is_replaced_not_written_through(cf, fake_session, storage, tmp_path):
    out, victim = _outside(tmp_path)
    dest = tmp_path / "dest"
    depth = dest / "book" / "binance" / "BTCUSDT" / "2026-09-01" / "depth"
    depth.mkdir(parents=True)
    os.symlink(victim, depth / "00.parquet")
    day = _day()
    fake_session.queue(_listing([day]))
    _serve(storage, day, [BODY_A, BODY_M])
    out_ = cf.download_l2("book", "BTCUSDT", "2026-09-01", None, dest)
    assert victim.read_bytes() == b"do not touch"
    assert not (depth / "00.parquet").is_symlink() and (depth / "00.parquet").read_bytes() == BODY_A
    assert len(out_["downloaded"]) == 2


@pytest.mark.parametrize("name", ["depth/24.parquet", "evil.sh", "depth/../manifest.json", "depth/00.parquet/x",
                                  "trades.parquet", "/etc/passwd", "depth\\00.parquet", None])
def test_only_the_dataset_file_names_are_accepted(cf, fake_session, storage, tmp_path, name):
    day = {"date": "2026-09-01", "files": [_file("manifest.json", BODY_M)]}
    day["files"][0]["name"] = name
    fake_session.queue(_listing([day]))
    with pytest.raises(CandleFeedError, match="unexpected file name"):
        cf.download_l2("book", "BTCUSDT", "2026-09-01", None, tmp_path)
    assert storage.calls == [] and not list(tmp_path.rglob("*.*"))


def test_duplicate_file_names_are_refused(cf, fake_session, storage, tmp_path):
    day = {"date": "2026-09-01", "files": [_file("manifest.json", BODY_M), _file("manifest.json", b"other")]}
    fake_session.queue(_listing([day]))
    with pytest.raises(CandleFeedError, match="unexpected file name"):
        cf.download_l2("book", "BTCUSDT", "2026-09-01", None, tmp_path)


@pytest.mark.parametrize("days", [["2026-09-02"], ["2026-08-31"], ["2026-09-01", "2026-09-01"], ["2026-02-30"],
                                  ["../2026-09-01"], [None]])
def test_days_must_be_the_ones_asked_for(cf, fake_session, storage, tmp_path, days):
    fake_session.queue(_listing([{"date": d, "files": [_file("manifest.json", BODY_M)]} for d in days]))
    with pytest.raises(CandleFeedError, match="Unexpected day"):
        cf.download_l2("book", "BTCUSDT", "2026-09-01", None, tmp_path)
    assert storage.calls == []


def test_without_dir_fd_downloads_fail_closed(cf, fake_session, storage, tmp_path, monkeypatch):
    """Astra recheck-2 #2: a path-checking fallback can't stop a folder being swapped for a symlink between
    the check and the write, so there is no fallback. Nothing is requested or written."""
    import candlefeed._safefs as safefs
    monkeypatch.setattr(safefs, "DIR_FD", False)
    with pytest.raises(CandleFeedError, match="directory-relative") as exc:
        cf.download_l2("book", "BTCUSDT", "2026-09-01", None, tmp_path / "dest")
    assert exc.value.code == "unsupported_platform"
    assert fake_session.calls == [] and storage.calls == [] and not (tmp_path / "dest").exists()
    with pytest.raises(safefs.UnsupportedPlatform):
        safefs.SafeDir(tmp_path / "dest", ["book"])


def test_swapping_a_checked_folder_for_a_symlink_cannot_redirect_the_write(tmp_path):
    """The race the fallback lost: depth/ is opened, then replaced by a symlink before the file is created.
    With the directory descriptor the write still lands in the original folder, never outside."""
    from candlefeed._safefs import SafeDir
    out = tmp_path / "outside"
    out.mkdir()
    folder = SafeDir(tmp_path / "dest", ["book", "depth"])
    real = tmp_path / "dest" / "book" / "depth"
    real.rename(tmp_path / "dest" / "book" / "moved")
    import os as _os
    _os.symlink(out, real)
    with folder:
        with folder.create_exclusive("00.parquet.part") as fh:
            fh.write(b"data")
        folder.replace("00.parquet.part", "00.parquet")
    assert list(out.iterdir()) == []
    assert (tmp_path / "dest" / "book" / "moved" / "00.parquet").read_bytes() == b"data"


# ---- Astra #6: byte and time budgets -------------------------------------------------------------------

class EndlessStream(FakeStream):
    """Advertised as a small file, streams forever."""

    def __init__(self):
        super().__init__(200)
        self.sent = 0

    def iter_content(self, chunk_size: int = 1):
        while True:
            self.sent += 4096
            yield b"\0" * 4096


def test_a_stream_longer_than_its_listed_size_is_cut_off(cf, fake_session, storage, tmp_path):
    day = _day()
    fake_session.queue(_listing([day]))
    endless = EndlessStream()
    storage.script[day["files"][0]["url"].split("?")[0]] = [endless]
    with pytest.raises(CandleFeedError, match="ran past its listed size"):
        cf.download_l2("book", "BTCUSDT", "2026-09-01", None, tmp_path)
    assert endless.sent == 4096 and len(storage.calls) == 1
    assert not list(tmp_path.rglob("*.part")) and not list(tmp_path.rglob("00.parquet"))


@pytest.mark.parametrize("field,value,match", [
    ("size", 5 * 1024 ** 3, "listed size"), ("size", -1, "listed size"), ("size", True, "listed size"),
    ("size", "21", "listed size"), ("sha256", "abc", "sha256"), ("sha256", None, "sha256"),
    ("key", "", "malformed"), ("url", "https://sgp1.example/" + "a" * 9000, "malformed"),
])
def test_malformed_listing_entries_are_refused_before_downloading(cf, fake_session, storage, tmp_path,
                                                                  field, value, match):
    day = _day()
    day["files"][0][field] = value
    fake_session.queue(_listing([day]))
    with pytest.raises(CandleFeedError, match=match):
        cf.download_l2("book", "BTCUSDT", "2026-09-01", None, tmp_path)
    assert storage.calls == []


def test_total_budget_is_checked_before_any_file_is_fetched(cf, fake_session, storage, tmp_path):
    day = _day()
    fake_session.queue(_listing([day]))
    _serve(storage, day, [BODY_A, BODY_M])
    with pytest.raises(CandleFeedError, match="over the budget") as exc:
        cf.download_l2("book", "BTCUSDT", "2026-09-01", None, tmp_path, max_bytes=len(BODY_A))
    assert exc.value.code == "download_budget" and storage.calls == []
    fake_session.queue(_listing([day]))
    out = cf.download_l2("book", "BTCUSDT", "2026-09-01", None, tmp_path, max_bytes=len(BODY_A) + len(BODY_M))
    assert out["bytes"] == len(BODY_A) + len(BODY_M)


def test_overall_deadline_stops_the_download(cf, fake_session, storage, tmp_path, monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(client_module.time, "monotonic", lambda: clock[0])

    class SlowStream(FakeStream):
        def iter_content(self, chunk_size=1):
            for i in range(0, len(self._body), 4):
                clock[0] += 30          # every chunk takes 30 seconds
                yield self._body[i:i + 4]

    day = _day()
    fake_session.queue(_listing([day]))
    storage.script[day["files"][0]["url"].split("?")[0]] = [SlowStream(200, BODY_A)]
    with pytest.raises(CandleFeedError, match="deadline") as exc:
        cf.download_l2("book", "BTCUSDT", "2026-09-01", None, tmp_path, deadline=60)
    assert exc.value.code == "download_deadline" and not list(tmp_path.rglob("*.part"))


# ---- Astra recheck-2 #3: a corrupt cached file can't slip past the budget ------------------------------

def _plant_corrupt_cache(tmp_path):
    target = tmp_path / "book" / "binance" / "BTCUSDT" / "2026-09-01"
    (target / "depth").mkdir(parents=True)
    (target / "depth" / "00.parquet").write_bytes(b"Z" * len(BODY_A))      # right size, wrong bytes
    (target / "manifest.json").write_bytes(BODY_M)                        # genuinely cached
    return target


def test_same_size_corrupt_file_counts_against_the_budget(cf, fake_session, storage, tmp_path):
    """Astra's replay: max_bytes=1 let the corrupt file's replacement download anyway."""
    _plant_corrupt_cache(tmp_path)
    day = _day()
    fake_session.queue(_listing([day]))
    _serve(storage, day, [BODY_A, BODY_M])
    with pytest.raises(CandleFeedError, match="over the budget") as exc:
        cf.download_l2("book", "BTCUSDT", "2026-09-01", None, tmp_path, max_bytes=1)
    assert exc.value.code == "download_budget" and storage.calls == []


def test_the_replacement_fits_a_budget_of_exactly_its_size(cf, fake_session, storage, tmp_path):
    target = _plant_corrupt_cache(tmp_path)
    day = _day()
    fake_session.queue(_listing([day]))
    _serve(storage, day, [BODY_A, BODY_M])
    out = cf.download_l2("book", "BTCUSDT", "2026-09-01", None, tmp_path, max_bytes=len(BODY_A))
    assert out["bytes"] == len(BODY_A) and len(out["skipped"]) == 1 and len(storage.calls) == 1
    assert (target / "depth" / "00.parquet").read_bytes() == BODY_A


def test_disk_space_for_the_temp_file_is_reserved_before_fetching(cf, fake_session, storage, tmp_path, monkeypatch):
    _plant_corrupt_cache(tmp_path)
    day = _day()
    fake_session.queue(_listing([day]))
    _serve(storage, day, [BODY_A, BODY_M])
    usage = client_module.shutil.disk_usage(tmp_path)
    monkeypatch.setattr(client_module.shutil, "disk_usage",
                        lambda p: usage._replace(free=client_module._L2_DISK_HEADROOM + len(BODY_A) - 1))
    with pytest.raises(CandleFeedError, match="Not enough disk space") as exc:
        cf.download_l2("book", "BTCUSDT", "2026-09-01", None, tmp_path)
    assert exc.value.code == "disk_space" and storage.calls == []


# ---- Astra recheck-2 #4: the deadline reaches every request and every socket read ----------------------

def test_request_timeouts_are_cut_to_the_time_left(cf, fake_session, storage, tmp_path, monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(client_module.time, "monotonic", lambda: clock[0])
    timeouts = []
    real_get = fake_session.get

    def get(url, params=None, timeout=0, allow_redirects=True, stream=False):
        timeouts.append(timeout)
        return real_get(url, params=params, timeout=timeout, allow_redirects=allow_redirects, stream=stream)

    monkeypatch.setattr(fake_session, "get", get)
    day = _day()
    fake_session.queue(_listing([day]))
    _serve(storage, day, [BODY_A, BODY_M])
    cf.download_l2("book", "BTCUSDT", "2026-09-01", None, tmp_path, deadline=7)
    assert timeouts == [(7, 7)] and storage.timeouts == [(7, 7), (7, 7)]
    fake_session.queue(_listing([]))
    cf.l2_files("book", "BTCUSDT", "2026-09-01")                    # outside download_l2: the plain timeout
    assert timeouts[-1] == cf.timeout


def test_a_retry_wait_that_would_pass_the_deadline_stops_instead(cf, fake_session, storage, tmp_path, monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(client_module.time, "monotonic", lambda: clock[0])
    day = _day()
    fake_session.queue(_listing([day]))
    storage.script[day["files"][0]["url"].split("?")[0]] = [FakeStream(503)]
    with pytest.raises(CandleFeedError, match="waiting to retry") as exc:
        cf.download_l2("book", "BTCUSDT", "2026-09-01", None, tmp_path, deadline=0.2)
    assert exc.value.code == "download_deadline" and fake_session.sleeps == []


# ---- Astra recheck-3: streamed bodies keep requests' decoding, retries, cleanup and deadline checks -------

import gzip  # noqa: E402
import io  # noqa: E402

import urllib3  # noqa: E402


class _Body(io.BytesIO):
    """A response body whose reads can advance a fake clock or raise like a dropped socket."""

    def __init__(self, data, clock=None, per_read=0.0, fail_after=None):
        super().__init__(data)
        self.clock, self.per_read, self.fail_after, self.reads = clock, per_read, fail_after, 0

    def read(self, amt=-1):
        self.reads += 1
        if self.clock is not None:
            self.clock[0] += self.per_read
        if self.fail_after is not None and self.reads > self.fail_after:
            raise urllib3.exceptions.ReadTimeoutError(None, "https://sgp1.example/x", "Read timed out.")
        return super().read(amt)


def _real(body: _Body, headers=None, status=200) -> requests.Response:
    raw = urllib3.HTTPResponse(body=body, headers=headers or {}, status=status, preload_content=False,
                               decode_content=False)
    resp = requests.Response()
    resp.status_code, resp.raw, resp.url = status, raw, "https://sgp1.example/x"
    resp.headers = requests.structures.CaseInsensitiveDict(headers or {})
    return resp


def test_gzip_content_encoding_is_decoded_before_size_and_hash_checks(cf, fake_session, storage, tmp_path):
    """Astra recheck-3 #3: the raw read1 path handed compressed wire bytes to the size and hash checks."""
    day = _day()
    fake_session.queue(_listing([day]))
    a_url, m_url = (f["url"].split("?")[0] for f in day["files"])
    storage.script[a_url] = [_real(_Body(gzip.compress(BODY_A)), {"Content-Encoding": "gzip"})]
    storage.script[m_url] = [_real(_Body(BODY_M))]
    cf.download_l2("book", "BTCUSDT", "2026-09-01", None, tmp_path)
    assert (tmp_path / "book/binance/BTCUSDT/2026-09-01/depth/00.parquet").read_bytes() == BODY_A


def test_urllib3_read_timeouts_are_retried_and_leave_no_part(cf, fake_session, storage, tmp_path):
    """Astra recheck-3 #2: a ReadTimeoutError from the body escaped the retries and left 00.parquet.part."""
    day = _day()
    fake_session.queue(_listing([day]))
    a_url, m_url = (f["url"].split("?")[0] for f in day["files"])
    storage.script[a_url] = [_real(_Body(BODY_A, fail_after=1)), _real(_Body(BODY_A))]
    storage.script[m_url] = [_real(_Body(BODY_M))]
    out = cf.download_l2("book", "BTCUSDT", "2026-09-01", None, tmp_path)
    assert len(out["downloaded"]) == 2 and len(storage.calls) == 3
    assert not list(tmp_path.rglob("*.part"))


def test_persistent_urllib3_errors_end_in_a_clean_failure(cf, fake_session, storage, tmp_path):
    day = _day()
    fake_session.queue(_listing([day]))
    storage.script[day["files"][0]["url"].split("?")[0]] = [_real(_Body(BODY_A, fail_after=0))]
    with pytest.raises(CandleFeedError, match="after 5 attempts: network error"):
        cf.download_l2("book", "BTCUSDT", "2026-09-01", None, tmp_path)
    assert not list(tmp_path.rglob("*.part"))


def test_a_deadline_hit_before_a_retry_removes_the_part(cf, fake_session, storage, tmp_path, monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(client_module.time, "monotonic", lambda: clock[0])
    day = _day()
    fake_session.queue(_listing([day]))
    # writes some bytes, then the socket drops; the retry wait would pass the deadline
    storage.script[day["files"][0]["url"].split("?")[0]] = [_real(_Body(BODY_A * 1000, fail_after=1))]
    day["files"][0]["size"] = len(BODY_A) * 1000
    with pytest.raises(CandleFeedError, match="deadline") as exc:
        cf.download_l2("book", "BTCUSDT", "2026-09-01", None, tmp_path, deadline=0.3)
    assert exc.value.code == "download_deadline" and not list(tmp_path.rglob("*.part"))


def test_a_trickling_body_is_stopped_at_the_deadline_through_decoding(cf, fake_session, storage, tmp_path,
                                                                      monkeypatch):
    """Each 8 KiB chunk takes 25 s to arrive; with a 60 s deadline the download stops after the third chunk,
    with gzip decoding in the path."""
    clock = [1000.0]
    monkeypatch.setattr(client_module.time, "monotonic", lambda: clock[0])
    big = bytes(range(256)) * 400                         # 102,400 bytes
    day = _day()
    day["files"][0].update(size=len(big), sha256=hashlib.sha256(big).hexdigest())
    fake_session.queue(_listing([day]))
    body = _Body(gzip.compress(big, compresslevel=0), clock=clock, per_read=25)
    storage.script[day["files"][0]["url"].split("?")[0]] = [_real(body, {"Content-Encoding": "gzip"})]
    with pytest.raises(CandleFeedError, match="deadline") as exc:
        cf.download_l2("book", "BTCUSDT", "2026-09-01", None, tmp_path, deadline=60)
    assert exc.value.code == "download_deadline" and body.reads <= 4
    assert not list(tmp_path.rglob("*.part"))


def test_a_trickling_listing_is_stopped_at_the_deadline(cf, fake_session, storage, tmp_path, monkeypatch):
    """Astra recheck-3 #1: the listing was read whole by requests before the deadline was looked at."""
    clock = [1000.0]
    monkeypatch.setattr(client_module.time, "monotonic", lambda: clock[0])

    class SlowListing(FakeResponse):
        reads = 0

        def iter_content(self, chunk_size=1):
            for chunk in super().iter_content(chunk_size):
                clock[0] += 25
                SlowListing.reads += 1
                yield chunk

    listing = SlowListing(200, {"status": "ok", "days": [], "missing": [], "usage": None, "pad": "x" * 100_000})
    fake_session.queue(listing)
    with pytest.raises(CandleFeedError, match="deadline") as exc:
        cf.download_l2("book", "BTCUSDT", "2026-09-01", None, tmp_path, deadline=60)
    assert exc.value.code == "download_deadline" and SlowListing.reads == 3
    assert storage.calls == []


# ---- Astra recheck-4 #1: the listing cap applies without a deadline too, refreshes included -------------

def test_an_oversized_listing_is_refused_without_a_deadline(cf, fake_session, storage, tmp_path, monkeypatch):
    monkeypatch.setattr(client_module, "_MAX_API_BODY", 10_000)
    fake_session.queue(FakeResponse(200, {"status": "ok", "days": [], "missing": [], "pad": "x" * 20_000}))
    with pytest.raises(CandleFeedError, match="is over 10,000 bytes"):
        cf.download_l2("book", "BTCUSDT", "2026-09-01", None, tmp_path)          # default deadline=None
    assert storage.calls == [] and fake_session.calls[0]["stream"] is True


def test_an_oversized_refreshed_listing_is_refused(cf, fake_session, storage, tmp_path, monkeypatch):
    monkeypatch.setattr(client_module, "_MAX_API_BODY", 10_000)
    old = _day()
    fake_session.queue(_listing([old]), FakeResponse(200, {"status": "ok", "days": [old], "pad": "x" * 20_000}))
    storage.script[old["files"][0]["url"].split("?")[0]] = [FakeStream(403)]
    with pytest.raises(CandleFeedError, match="is over 10,000 bytes"):
        cf.download_l2("book", "BTCUSDT", "2026-09-01", None, tmp_path)
    assert len(fake_session.calls) == 2 and not list(tmp_path.rglob("*.part"))


def test_outside_download_l2_api_calls_are_not_streamed(cf, fake_session):
    fake_session.queue(FakeResponse(200, {"status": "ok", "days": []}))
    cf.l2_files("book", "BTCUSDT", "2026-09-01")
    assert fake_session.calls[-1]["stream"] is False
