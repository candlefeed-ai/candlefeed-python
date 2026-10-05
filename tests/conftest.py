"""Shared test fixtures: a fake requests session, fully offline."""
from __future__ import annotations

import json as _json
from typing import Any, Callable, Dict, List, Optional

import pytest

from candlefeed import CandleFeed


class FakeResponse:
    def __init__(
        self,
        status_code: int = 200,
        json_body: Any = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self.status_code = status_code
        self._json = json_body
        self.headers = headers or {}
        self.text = _json.dumps(json_body) if json_body is not None else ""

    def json(self) -> Any:
        if self._json is None:
            raise ValueError("no json body")
        return self._json

    def iter_content(self, chunk_size: int = 1):
        body = self.text.encode()
        for i in range(0, len(body), chunk_size):
            yield body[i:i + chunk_size]

    def close(self) -> None:
        pass


class FakeSession:
    """Records requests and replays a scripted queue of responses."""

    def __init__(self) -> None:
        self.headers: Dict[str, str] = {}
        self.calls: List[Dict[str, Any]] = []
        self._queue: List[FakeResponse] = []
        self._handler: Optional[Callable[[str, dict], FakeResponse]] = None
        self.sleeps: List[float] = []

    def queue(self, *responses: FakeResponse) -> None:
        self._queue.extend(responses)

    def handler(self, fn: Callable[[str, dict], FakeResponse]) -> None:
        self._handler = fn

    def get(self, url: str, params: Optional[dict] = None, timeout: float = 0,
            allow_redirects: bool = True, stream: bool = False) -> FakeResponse:
        params = params or {}
        self.calls.append({"url": url, "params": params, "allow_redirects": allow_redirects, "stream": stream})
        if self._handler is not None:
            return self._handler(url, params)
        if not self._queue:
            raise AssertionError("FakeSession queue exhausted")
        return self._queue.pop(0)

    def close(self) -> None:
        pass


@pytest.fixture
def fake_session() -> FakeSession:
    return FakeSession()


@pytest.fixture
def client(fake_session, monkeypatch) -> CandleFeed:
    cf = CandleFeed(api_key="cf_live_testkey", session=fake_session)
    # Make backoff sleeps instant and observable.
    import candlefeed.client as client_module

    def fake_sleep(seconds: float) -> None:
        fake_session.sleeps.append(seconds)

    monkeypatch.setattr(client_module.time, "sleep", fake_sleep)
    return cf


def ok(data: List[dict], **extra: Any) -> FakeResponse:
    body = {"status": "ok", "data": data, "has_more": False, "next_cursor": None}
    body.update(extra)
    return FakeResponse(200, body)


def page(data: List[dict], next_cursor: Optional[str]) -> FakeResponse:
    return FakeResponse(
        200,
        {
            "status": "ok",
            "data": data,
            "has_more": next_cursor is not None,
            "next_cursor": next_cursor,
        },
    )


def error(status: int, code: str, message: str, headers: Optional[dict] = None) -> FakeResponse:
    return FakeResponse(
        status,
        {"status": "error", "code": code, "message": message},
        headers=headers,
    )
