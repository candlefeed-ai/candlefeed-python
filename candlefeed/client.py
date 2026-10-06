"""CandleFeed API client — pandas-native access to crypto market data."""
from __future__ import annotations

import functools
import hashlib
import logging
import os
import re
import shutil
import threading
import time
import zlib
from datetime import date, datetime, timedelta, timezone
from importlib.metadata import PackageNotFoundError as _PkgNotFound
from importlib.metadata import version as _pkg_version
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlsplit

import pandas as pd
import requests
import urllib3

from . import _safefs
from ._safefs import SafeDir, UnsafePath
from .exceptions import (
    AuthenticationError,
    CandleFeedError,
    InvalidParameterError,
    QuotaExceededError,
    RateLimitError,
    TierRestrictedError,
)

__all__ = [
    "CandleFeed",
    "OHLCV_INTERVALS",
    "OPEN_INTEREST_INTERVALS",
    "FUNDING_AGGREGATED_INTERVALS",
    "LIQUIDATION_INTERVALS",
    "LIQUIDATIONS_AGGREGATED_INTERVALS",
    "LONG_SHORT_INTERVALS",
    "BASIS_INTERVALS",
    "L2_DATASETS",
]

DEFAULT_BASE_URL = "https://candlefeed.ai/api/v1"
SIGNUP_URL = "https://candlefeed.ai/signup?utm_source=client&utm_medium=error"
DEFAULT_TIMEOUT = 30.0
DEFAULT_MAX_RETRIES = 4
DEFAULT_REQUEST_DEADLINE = 60.0

# Intervals the API accepts per dataset. Exposed for reference/validation in
# calling code — the client itself does not reject unknown values, so a newly
# shipped interval works before this list catches up.
OHLCV_INTERVALS = ("1m", "5m", "15m", "1h", "4h", "1d")
OPEN_INTEREST_INTERVALS = ("5m", "15m", "1h", "4h", "1d")
FUNDING_AGGREGATED_INTERVALS = ("1h", "4h", "1d")
LIQUIDATION_INTERVALS = ("1m", "5m", "15m", "1h", "4h", "1d")
LIQUIDATIONS_AGGREGATED_INTERVALS = ("1h", "4h", "6h", "8h", "12h", "1d")
LONG_SHORT_INTERVALS = ("5m", "15m", "1h", "4h", "1d")
BASIS_INTERVALS = ("5m", "1h", "4h")

TimeLike = Union[str, datetime, None]

# Columns that should be coerced to float when present in a response.
_NUMERIC_COLUMNS = {
    "open", "high", "low", "close", "volume", "quote_volume",
    "funding_rate", "mark_price",
    "weighted_funding_rate", "total_oi_usd",
    "open_interest", "open_interest_value",
    "quantity", "price", "usd_value",
    "long_liq_usd", "short_liq_usd", "total_liq_usd",
    "long_short_ratio", "long_account_ratio", "short_account_ratio",
    "buy_volume", "sell_volume", "buy_sell_ratio",
    "open_basis", "close_basis", "open_change", "close_change",
    "strike", "mark_iv", "delta", "gamma", "vega", "theta", "rho",
    "bid_price", "ask_price", "underlying_price", "index_price",
    "liquidation_count", "liquidation_volume_usd",
}

# Candidate timestamp column names, in priority order. The API uses ``time`` on
# most endpoints and ``timestamp`` on the aggregated ones.
_TIME_COLUMNS = ("time", "timestamp")

L2_DATASETS = ("book", "trades")
# Presigned download links must point here (https, port 443). Override with storage_host= or
# CANDLEFEED_L2_STORAGE_HOST if CandleFeed ever moves the bucket.
DEFAULT_L2_STORAGE_HOST = "candlefeed-l2-canonical.sgp1.digitaloceanspaces.com"
# Download links last 15 minutes; ask for fresh ones a little before that.
_L2_URL_REFRESH_SECONDS = 12 * 60
_L2_MAX_DAYS_PER_CALL = 31
# A BTCUSDT hour file is tens of MB; anything listed above this is refused.
_L2_MAX_FILE_BYTES = 4 * 1024 ** 3
L2_SAMPLE_LICENSE = "Internal use only. See https://candlefeed.ai/terms (section 5.3)."
logger = logging.getLogger("candlefeed")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_L2_DISK_HEADROOM = 64 * 1024 ** 2
# Bodies are read in small chunks so the download deadline is checked often.
_BODY_CHUNK = 8 * 1024
_ACCEPT = "gzip, deflate"
_ENCODINGS = {
    "identity": None,
    "gzip": lambda: zlib.decompressobj(16 + zlib.MAX_WBITS),
    "deflate": lambda: zlib.decompressobj(zlib.MAX_WBITS),
}
_MAX_API_BODY = 64 * 1024 ** 2
# The only files a day may contain; anything else in a listing is refused.
_L2_FILE_NAMES = {
    "book": frozenset([f"depth/{h:02d}.parquet" for h in range(24)] + ["snapshot.parquet", "manifest.json"]),
    "trades": frozenset(["trades.parquet", "manifest.json"]),
}
# 429s that a retry within this process cannot clear (monthly allowance, daily download limit).
_NON_RETRYABLE_429 = ("quota_exceeded", "daily_download_limit", "sample_daily_limit")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

try:
    _CLIENT_VERSION = _pkg_version("candlefeed")
except _PkgNotFound:  # running from a source checkout without an install
    _CLIENT_VERSION = "0.0.0+unknown"


def _to_day(value: Union[str, date, datetime, None]) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


# Any query string, on an absolute URL or a relative one (urllib3 reports "with url: /path?X-Amz-...").
_URL_QUERY_RE = re.compile(r"(?i)(\?|%3F)[^\s'\")]*")
_CREDENTIAL_RE = re.compile(
    r"(?i)\b(X-Amz-[A-Za-z-]+|Signature|Expires|AWSAccessKeyId|x-amz-security-token)(=|%3D)[^\s&'\")]*")


def _describe_error(exc: BaseException) -> str:
    """Exception class and message with every URL query string removed: a presigned download link's query
    is a bearer credential, and requests puts the full URL in connection errors."""
    message = _CREDENTIAL_RE.sub(r"\1\2<redacted>", _URL_QUERY_RE.sub(r"\1<redacted>", str(exc)))
    return f"{type(exc).__name__}: {message}"


def _get_capped(session, url: str, params, timeout, limit: int, what: str, check, headers=None) -> requests.Response:
    """One GET, redirects not followed, read in full: at most ``limit`` decoded bytes and ``check()`` (the
    elapsed-time deadline) after every network read, at EOF and before any error is passed on, so a request
    that's out of time always reports the deadline. The check runs between reads: a server that stalls inside
    one read is bounded by the socket read timeout. Network errors propagate."""
    try:
        extra = {"headers": headers} if headers else {}
        resp = session.get(url, params=params, timeout=timeout, allow_redirects=False, stream=True, **extra)
        data = bytearray()
        try:
            for chunk in CandleFeed._body_chunks(resp, what, limit):
                data += chunk
                if len(data) > limit:
                    raise CandleFeedError(f"Response from {what} is over {limit:,} bytes; stopped.",
                                          code="response_too_large")
                check()
        finally:
            resp.close()
    except (CandleFeedError, requests.RequestException, urllib3.exceptions.HTTPError):
        check()
        raise
    check()
    resp._content = bytes(data)
    resp._content_consumed = True
    return resp


def bounded_get(session: requests.Session, url: str, params: Optional[Dict[str, Any]] = None,
                timeout: float = DEFAULT_TIMEOUT, deadline: Optional[float] = DEFAULT_REQUEST_DEADLINE,
                max_bytes: int = _MAX_API_BODY) -> requests.Response:
    """GET ``url`` without following redirects and read the whole answer, like the client's own requests: at
    most ``max_bytes`` decoded bytes, with ``deadline`` seconds checked after every read and at the end.
    Raises CandleFeedError (codes ``request_deadline``, ``response_too_large``, ``unsupported_encoding``);
    network errors propagate as requests or urllib3 exceptions. The response's body is already read."""
    deadline_at = None if deadline is None else time.monotonic() + deadline

    def check() -> None:
        if deadline_at is not None and time.monotonic() > deadline_at:
            raise CandleFeedError(f"The answer from {url} took longer than {deadline:g} s; stopped.",
                                  code="request_deadline")
    # Explicit per request: requests' default also offers br and zstd when it can decode them, and those
    # would be refused by _body_chunks.
    return _get_capped(session, url, params, timeout, max_bytes, url, check, headers={"Accept-Encoding": _ACCEPT})


def _no_key_in_errors(method):
    """The API may echo the key back in an error; strip it from anything raised before it reaches the caller."""
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except CandleFeedError as exc:
            keys = self._known_keys()

            def clean(v):
                if isinstance(v, str):
                    for key in keys:
                        v = v.replace(key, "***")
                return v
            exc.message, exc.code = clean(exc.message), clean(exc.code)
            exc.args = tuple(clean(a) for a in exc.args)
            raise
    return wrapper


def _to_iso(value: TimeLike) -> Optional[str]:
    """Normalize a datetime/str to an ISO8601 string the API accepts."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    return str(value)


class CandleFeed:
    """Client for the CandleFeed crypto market-data API.

    Every data method returns a tidy :class:`pandas.DataFrame` indexed by the
    parsed timestamp. Range requests auto-paginate (follow ``next_cursor``) and
    rate limits are retried with bounded backoff.

    Args:
        api_key: Your CandleFeed API key. Falls back to the ``CANDLEFEED_API_KEY``
            environment variable.
        base_url: API base URL. Defaults to production.
        timeout: Per-request timeout in seconds.
        max_retries: Max retry attempts on HTTP 429 / transient network errors.
        request_deadline: Elapsed seconds allowed for one attempt at an API request (default 60), checked
            after every read of the answer and at its end. ``timeout`` only bounds the wait between reads, so
            a server sending a byte at a time could otherwise keep a request open indefinitely. A stall inside
            a single read (slow headers, say) is bounded by ``timeout``, not by this. ``None`` turns it off.
        session: Optional pre-configured :class:`requests.Session`.
        public: Make a client without an API key (``CANDLEFEED_API_KEY`` is ignored too, and an ``X-API-Key`` on a
            supplied session is removed from every request). It can only call the no-account endpoints:
            :meth:`l2_sample`, :meth:`download_l2_sample`, :meth:`l2_coverage`, :meth:`l2_gaps`.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        session: Optional[requests.Session] = None,
        download_session: Optional[requests.Session] = None,
        storage_host: Optional[str] = None,
        request_deadline: Optional[float] = DEFAULT_REQUEST_DEADLINE,
        public: bool = False,
    ) -> None:
        key = None if public else (api_key or os.environ.get("CANDLEFEED_API_KEY"))
        if not key and not public:
            raise AuthenticationError(
                "No API key provided. Pass api_key= or set CANDLEFEED_API_KEY. "
                "Get a free key (no card, about a minute) at "
                f"{SIGNUP_URL}",
                code="unauthorized",
            )
        self.api_key = key
        self.public = public
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.request_deadline = request_deadline
        self._local = threading.local()          # download_l2's deadline, per thread
        self._session = session or requests.Session()
        self._session.headers.update(
            {
                "Accept": "application/json",
                "Accept-Encoding": _ACCEPT,                 # the encodings _body_chunks decodes itself
                "User-Agent": f"candlefeed-python/{_CLIENT_VERSION}",
            }
        )
        if key:
            self._session.headers["X-API-Key"] = key
        # File downloads go to presigned storage URLs; this session never carries the API key.
        self._download_session = download_session
        self.storage_host = (storage_host or os.environ.get("CANDLEFEED_L2_STORAGE_HOST")
                             or DEFAULT_L2_STORAGE_HOST).strip().lower()
        self.last_rate_limit: Dict[str, Optional[str]] = {}
        self._deadline_at = None
        # ``meta`` block from the most recent response — carries per-request
        # provenance such as ``history_from`` (earliest bucket available for the
        # requested exchange/symbol/interval) and ``source`` (native vs
        # resampled). Also attached to each returned frame as ``df.attrs["meta"]``.
        self.last_meta: Dict[str, Any] = {}

    def _known_keys(self) -> List[str]:
        """Every key this client could send: its own and any X-API-Key on the API or download session."""
        keys = [self.api_key] if self.api_key else []
        for name in ("_session", "_download_session"):
            headers = getattr(getattr(self, name, None), "headers", None) or {}
            keys += [v for k, v in headers.items() if str(k).lower() == "x-api-key" and isinstance(v, str) and v]
        return sorted(set(keys), key=len, reverse=True)

    def __repr__(self) -> str:
        # Redact the secret — keep only the non-sensitive environment marker
        # (cf_live_/cf_test_) so a rendered client in a published notebook
        # never leaks usable key material.
        key = self.api_key or ""
        prefix = next((p for p in ("cf_live_", "cf_test_") if key.startswith(p)), "")
        masked = f"{prefix}***" if key else "unset"
        return f"CandleFeed(base_url={self.base_url!r}, api_key={masked!r})"

    @property
    def _deadline_at(self) -> Optional[float]:
        return getattr(self._local, "deadline_at", None)

    @_deadline_at.setter
    def _deadline_at(self, value: Optional[float]) -> None:
        self._local.deadline_at = value

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self._session.close()

    def __enter__(self) -> "CandleFeed":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------ #
    # HTTP plumbing
    # ------------------------------------------------------------------ #
    @_no_key_in_errors
    def _request(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Issue a single GET, map errors, and retry on 429/transient failures."""
        url = f"{self.base_url}/{path.lstrip('/')}"
        clean = {k: v for k, v in params.items() if v is not None}

        attempt = 0
        while True:
            request_deadline_at = (None if self.request_deadline is None
                                   else time.monotonic() + self.request_deadline)
            download_deadline_at = self._deadline_at

            def check(download_at=download_deadline_at, request_at=request_deadline_at) -> None:
                self._check_deadline(download_at, url)
                if request_at is not None and time.monotonic() > request_at:
                    raise CandleFeedError(f"The answer from {url} took longer than {self.request_deadline:g} s "
                                          "(request_deadline); stopped. Try again, or ask for fewer rows.",
                                          code="request_deadline")
            try:
                # Never follow redirects: requests would carry the X-API-Key header to the new origin.
                resp = _get_capped(self._session, url, clean, self._timeouts(url), _MAX_API_BODY, url, check,
                                   headers={"X-API-Key": None} if self.public else None)
            except (requests.RequestException, urllib3.exceptions.HTTPError) as exc:
                check()
                if attempt < self.max_retries:
                    self._sleep(self._backoff(attempt), url)
                    attempt += 1
                    continue
                raise CandleFeedError(f"Request to {url} failed: {_describe_error(exc)}") from None

            self._capture_rate_limit(resp)

            if 300 <= resp.status_code < 400:
                raise CandleFeedError(
                    f"The API answered HTTP {resp.status_code} with a redirect. The client doesn't follow "
                    "redirects, so your API key is only ever sent to the configured base URL.",
                    status_code=resp.status_code)

            if resp.status_code == 429:
                code, message = self._extract_error(resp)
                if code in _NON_RETRYABLE_429:
                    raise QuotaExceededError(message or "Download limit reached.", code=code, status_code=429)
                retry_after = self._retry_after_seconds(resp)
                if attempt < self.max_retries:
                    self._sleep(retry_after if retry_after is not None else self._backoff(attempt), url)
                    attempt += 1
                    continue
                code, message = self._extract_error(resp)
                raise RateLimitError(
                    message or "Rate limit exceeded.",
                    code=code or "rate_limit_exceeded",
                    status_code=429,
                    retry_after=retry_after,
                )

            if resp.status_code >= 400:
                self._raise_for_error(resp)

            try:
                body = resp.json()
            except ValueError as exc:
                raise CandleFeedError(
                    f"Non-JSON response from {url} (HTTP {resp.status_code})."
                ) from exc

            meta = body.get("meta") if isinstance(body, dict) else None
            self.last_meta = meta if isinstance(meta, dict) else {}
            return body

    def _raise_for_error(self, resp: requests.Response) -> None:
        code, message = self._extract_error(resp)
        status = resp.status_code
        msg = message or f"HTTP {status} error"
        if status == 401:
            raise AuthenticationError(msg, code=code, status_code=status)
        if status == 403:
            raise TierRestrictedError(msg, code=code or "tier_restricted", status_code=status)
        if status in (400, 422):
            raise InvalidParameterError(msg, code=code, status_code=status)
        raise CandleFeedError(msg, code=code, status_code=status)

    @staticmethod
    def _extract_error(resp: requests.Response) -> tuple[Optional[str], Optional[str]]:
        try:
            body = resp.json()
        except ValueError:
            return None, resp.text or None
        # FastAPI wraps HTTPException detail under "detail"; our handlers also
        # return the envelope at the top level. Support both. Only string fields are kept: anything else
        # (objects, lists) could carry text the key scrub can't see, so it's replaced by the generic message.
        detail = body.get("detail") if isinstance(body, dict) else None
        if isinstance(detail, str):
            return None, detail
        envelope = detail if isinstance(detail, dict) else body if isinstance(body, dict) else {}
        code, message = envelope.get("code"), envelope.get("message")
        return (code if isinstance(code, str) else None), (message if isinstance(message, str) else None)

    def _capture_rate_limit(self, resp: requests.Response) -> None:
        for header in ("X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Reset", "X-Plan"):
            if header in resp.headers:
                self.last_rate_limit[header] = resp.headers[header]

    @staticmethod
    def _retry_after_seconds(resp: requests.Response) -> Optional[float]:
        ra = resp.headers.get("Retry-After")
        if ra is not None:
            try:
                return float(ra)
            except ValueError:
                pass
        reset = resp.headers.get("X-RateLimit-Reset")
        if reset:
            try:
                reset_dt = datetime.fromisoformat(reset.replace("Z", "+00:00"))
                if reset_dt.tzinfo is None:
                    reset_dt = reset_dt.replace(tzinfo=timezone.utc)
                delta = (reset_dt - datetime.now(timezone.utc)).total_seconds()
                return max(delta, 0.0)
            except ValueError:
                pass
        return None

    @staticmethod
    def _backoff(attempt: int) -> float:
        # 0.5s, 1s, 2s, 4s … capped at 30s.
        return min(0.5 * (2 ** attempt), 30.0)

    # ------------------------------------------------------------------ #
    # Pagination + DataFrame assembly
    # ------------------------------------------------------------------ #
    def _fetch(
        self,
        path: str,
        params: Dict[str, Any],
        paginate: bool,
        max_rows: Optional[int],
    ) -> List[Dict[str, Any]]:
        """Fetch rows from a cursor-paginated endpoint."""
        rows: List[Dict[str, Any]] = []
        cursor: Optional[str] = params.get("cursor")
        while True:
            page_params = dict(params)
            if cursor is not None:
                page_params["cursor"] = cursor
            body = self._request(path, page_params)
            page = body.get("data") or []
            rows.extend(page)

            if max_rows is not None and len(rows) >= max_rows:
                return rows[:max_rows]

            next_cursor = body.get("next_cursor")
            if not paginate or not body.get("has_more") or not next_cursor:
                return rows
            if next_cursor == cursor:  # guard against a stuck cursor
                return rows
            cursor = next_cursor

    @staticmethod
    def _to_frame(rows: List[Dict[str, Any]], index: bool = True) -> pd.DataFrame:
        """Build a tidy DataFrame: parsed datetime index, float numerics."""
        df = pd.DataFrame(rows)
        if df.empty:
            return df

        time_col = next((c for c in _TIME_COLUMNS if c in df.columns), None)
        if time_col is not None:
            df[time_col] = pd.to_datetime(df[time_col], utc=True, errors="coerce")

        for col in df.columns:
            if col in _NUMERIC_COLUMNS:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if index and time_col is not None:
            df = df.set_index(time_col).sort_index()
        return df

    def _query(
        self,
        path: str,
        params: Dict[str, Any],
        paginate: bool,
        max_rows: Optional[int],
    ) -> pd.DataFrame:
        rows = self._fetch(path, params, paginate=paginate, max_rows=max_rows)
        df = self._to_frame(rows)
        df.attrs["meta"] = dict(self.last_meta)
        return df

    # ------------------------------------------------------------------ #
    # OHLCV / candles
    # ------------------------------------------------------------------ #
    def get_ohlcv(
        self,
        symbol: str,
        exchange: str = "binance",
        interval: str = "1m",
        start: TimeLike = None,
        end: TimeLike = None,
        limit: Optional[int] = None,
        paginate: bool = True,
        max_rows: Optional[int] = None,
    ) -> pd.DataFrame:
        """Historical OHLCV candles.

        Intervals: ``1m, 5m, 15m, 1h, 4h, 1d`` — stored natively at 1m and
        resampled on request, so every higher interval is consistent with the
        underlying minute bars. Returns a DataFrame indexed by ``time`` with
        ``open, high, low, close, volume, quote_volume`` columns.

        Exchanges: ``binance`` (perp, the default), ``binance_spot``, ``bybit``,
        ``okx``, ``dydx``, ``hyperliquid``. History starts differ by venue —
        Binance spot 2017, Binance perp 2019, OKX/Bybit 2020, Hyperliquid 1d
        2023 / 4h 2024 / 1h 2025 / 1m–15m 2026, dYdX 2025. See
        https://candlefeed.ai/coverage/ for the per-venue matrix.
        """
        params = {
            "symbol": symbol,
            "exchange": exchange,
            "interval": interval,
            "start": _to_iso(start),
            "end": _to_iso(end),
            "limit": limit,
        }
        return self._query("candles", params, paginate, max_rows)

    get_candles = get_ohlcv

    # ------------------------------------------------------------------ #
    # Funding rates
    # ------------------------------------------------------------------ #
    def get_funding_rates(
        self,
        symbol: str,
        exchange: str = "binance",
        start: TimeLike = None,
        end: TimeLike = None,
        limit: Optional[int] = None,
        paginate: bool = True,
        max_rows: Optional[int] = None,
    ) -> pd.DataFrame:
        """Per-exchange funding rates (``funding_rate``, ``mark_price``).

        Native settlement cadence (8h on most venues), not a resampled series.
        Exchanges: ``binance`` (from 2020), ``okx``, ``bybit``, ``dydx``,
        ``hyperliquid`` — Hyperliquid is hourly and continuous from each
        contract's HL listing (BTC/ETH/SOL from 2023-05-12).
        """
        params = {
            "symbol": symbol,
            "exchange": exchange,
            "start": _to_iso(start),
            "end": _to_iso(end),
            "limit": limit,
        }
        return self._query("funding-rates", params, paginate, max_rows)

    def get_funding_rates_aggregated(
        self,
        symbol: str,
        interval: str = "1h",
        exchanges: Optional[Union[str, List[str]]] = None,
        start: TimeLike = None,
        end: TimeLike = None,
        limit: Optional[int] = None,
        paginate: bool = True,
        max_rows: Optional[int] = None,
    ) -> pd.DataFrame:
        """OI-weighted funding rate across exchanges.

        Intervals: ``1h, 4h, 1d``. Returns ``weighted_funding_rate``,
        ``total_oi_usd``, ``exchange_count``, ``contributing_exchanges``,
        indexed by ``timestamp``.
        """
        if isinstance(exchanges, (list, tuple)):
            exchanges = ",".join(exchanges)
        params = {
            "symbol": symbol,
            "interval": interval,
            "exchanges": exchanges,
            "start": _to_iso(start),
            "end": _to_iso(end),
            "limit": limit,
        }
        return self._query("funding-rates/aggregated", params, paginate, max_rows)

    # ------------------------------------------------------------------ #
    # Open interest
    # ------------------------------------------------------------------ #
    def get_open_interest(
        self,
        symbol: str,
        exchange: str = "binance",
        interval: str = "5m",
        start: TimeLike = None,
        end: TimeLike = None,
        limit: Optional[int] = None,
        paginate: bool = True,
        max_rows: Optional[int] = None,
    ) -> pd.DataFrame:
        """Open interest (``open_interest``, ``open_interest_value``).

        Intervals: ``5m, 15m, 1h, 4h, 1d``.
        """
        params = {
            "symbol": symbol,
            "exchange": exchange,
            "interval": interval,
            "start": _to_iso(start),
            "end": _to_iso(end),
            "limit": limit,
        }
        return self._query("open-interest", params, paginate, max_rows)

    # ------------------------------------------------------------------ #
    # Liquidations
    # ------------------------------------------------------------------ #
    def get_liquidations(
        self,
        symbol: str,
        exchange: str = "binance",
        side: Optional[str] = None,
        interval: Optional[str] = None,
        start: TimeLike = None,
        end: TimeLike = None,
        limit: Optional[int] = None,
        paginate: bool = True,
        max_rows: Optional[int] = None,
    ) -> pd.DataFrame:
        """Liquidation events.

        Omit ``interval`` for tick-level rows (``side, quantity, price,
        usd_value``); pass an interval (``1m, 5m, 15m, 1h, 4h, 1d``) for bucketed
        ``long_liq_usd / short_liq_usd / total_liq_usd / count``. ``side`` filters
        to ``long`` or ``short``.

        Tick-level liquidations are forward-collected and start in late May 2026
        — OKX 2026-05-27, Binance/Bybit/Hyperliquid 2026-05-28, Huobi
        2026-06-02. There is no pre-2026 tick history on any venue; for
        long-horizon work use :meth:`get_liquidations_aggregated` at ``1d``.
        """
        params = {
            "symbol": symbol,
            "exchange": exchange,
            "side": side,
            "interval": interval,
            "start": _to_iso(start),
            "end": _to_iso(end),
            "limit": limit,
        }
        return self._query("liquidations", params, paginate, max_rows)

    def get_liquidations_aggregated(
        self,
        symbol: str,
        exchange: str = "binance",
        interval: str = "1d",
        start: TimeLike = None,
        end: TimeLike = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Pre-aggregated liquidation history (native cross-venue rollup).

        Intervals: ``1h, 4h, 6h, 8h, 12h, 1d``. Returns ``long_liq_usd``,
        ``short_liq_usd`` indexed by ``timestamp``. This endpoint is not
        cursor-paginated (it returns ``meta.total``); use ``limit`` to size the
        single page.

        Depth varies by interval. ``1d`` is the deep series, running from each
        contract's perpetual listing — Binance 2019-09, Bybit 2020-01,
        OKX/Huobi 2022-04, Hyperliquid 2026-05. The sub-daily buckets are
        forward-built and start between 2023 and 2026 by venue, so use
        ``interval="1d"`` for multi-year backtests. The response ``meta`` block
        carries ``history_from`` for the exact exchange/symbol/interval asked
        for; it is available afterwards as ``cf.last_meta`` and on the returned
        frame as ``df.attrs["meta"]``.
        """
        params = {
            "symbol": symbol,
            "exchange": exchange,
            "interval": interval,
            "start": _to_iso(start),
            "end": _to_iso(end),
            "limit": limit,
        }
        body = self._request("liquidations/aggregated", params)
        df = self._to_frame(body.get("data") or [])
        df.attrs["meta"] = dict(self.last_meta)
        return df

    # ------------------------------------------------------------------ #
    # Long/short ratio
    # ------------------------------------------------------------------ #
    def get_long_short_ratio(
        self,
        symbol: str,
        exchange: str = "binance",
        interval: str = "5m",
        ratio_type: Optional[str] = None,
        start: TimeLike = None,
        end: TimeLike = None,
        limit: Optional[int] = None,
        paginate: bool = True,
        max_rows: Optional[int] = None,
    ) -> pd.DataFrame:
        """Long/short account ratio.

        Intervals: ``5m, 15m, 1h, 4h, 1d``. ``ratio_type`` is one of
        ``top_account`` (default), ``global_account``, or ``both``. Binance
        only, from 2020.
        """
        params = {
            "symbol": symbol,
            "exchange": exchange,
            "interval": interval,
            "ratio_type": ratio_type,
            "start": _to_iso(start),
            "end": _to_iso(end),
            "limit": limit,
        }
        return self._query("long-short-ratio", params, paginate, max_rows)

    # ------------------------------------------------------------------ #
    # Taker volume
    # ------------------------------------------------------------------ #
    def get_taker_volume(
        self,
        symbol: str,
        exchange: str = "binance",
        start: TimeLike = None,
        end: TimeLike = None,
        limit: Optional[int] = None,
        paginate: bool = True,
        max_rows: Optional[int] = None,
    ) -> pd.DataFrame:
        """Taker buy/sell volume (``buy_volume, sell_volume, buy_sell_ratio``).

        True 5-minute data from each symbol's listing (BTC 2019-09). Binance only.
        """
        params = {
            "symbol": symbol,
            "exchange": exchange,
            "start": _to_iso(start),
            "end": _to_iso(end),
            "limit": limit,
        }
        return self._query("taker-volume", params, paginate, max_rows)

    # ------------------------------------------------------------------ #
    # Basis
    # ------------------------------------------------------------------ #
    def get_basis(
        self,
        symbol: str,
        exchange: str = "binance",
        interval: str = "4h",
        start: TimeLike = None,
        end: TimeLike = None,
        limit: Optional[int] = None,
        paginate: bool = True,
        max_rows: Optional[int] = None,
    ) -> pd.DataFrame:
        """Futures basis / premium (``open_basis, close_basis, ...``).

        Intervals: ``5m`` (native), ``1h``, ``4h``. Binance only, across all 52
        symbols — 5m from 2026-04-27, 1h from 2025-12-10, 4h from 2025-05-31.
        ``meta.source`` on the response says whether the series came back native
        or resampled from the 5m base.
        """
        params = {
            "symbol": symbol,
            "exchange": exchange,
            "interval": interval,
            "start": _to_iso(start),
            "end": _to_iso(end),
            "limit": limit,
        }
        return self._query("basis", params, paginate, max_rows)

    # ------------------------------------------------------------------ #
    # Combined
    # ------------------------------------------------------------------ #
    def get_combined(
        self,
        symbol: str,
        interval: str = "1m",
        fields: Union[str, List[str]] = "ohlcv,funding_rate",
        exchange: str = "binance",
        start: TimeLike = None,
        end: TimeLike = None,
        limit: Optional[int] = None,
        paginate: bool = True,
        max_rows: Optional[int] = None,
    ) -> pd.DataFrame:
        """Time-aligned multi-dataset frame on an OHLCV base timeline.

        ``fields`` is a comma-separated string or list drawn from ``ohlcv,
        open_interest, funding_rate, long_short, taker_volume, liquidations``;
        supplementary datasets are forward-filled onto each candle.
        """
        if isinstance(fields, (list, tuple)):
            fields = ",".join(fields)
        params = {
            "symbol": symbol,
            "interval": interval,
            "fields": fields,
            "exchange": exchange,
            "start": _to_iso(start),
            "end": _to_iso(end),
            "limit": limit,
        }
        return self._query("combined", params, paginate, max_rows)

    # ------------------------------------------------------------------ #
    # Options
    # ------------------------------------------------------------------ #
    def get_options(
        self,
        currency: str = "BTC",
        instrument_name: Optional[str] = None,
        option_type: Optional[str] = None,
        expiry: Optional[str] = None,
        start: TimeLike = None,
        end: TimeLike = None,
        limit: Optional[int] = None,
        paginate: bool = True,
        max_rows: Optional[int] = None,
    ) -> pd.DataFrame:
        """Deribit options chain snapshots with greeks and IV (Pro+).

        ``currency`` is ``BTC`` or ``ETH``. Optional filters: ``instrument_name``,
        ``option_type`` (``C``/``P``), ``expiry`` (``YYYY-MM-DD``).

        The API serves a rolling ~60-day window of chain snapshots; anything
        older is archived to object storage and is not queryable here. Greeks
        are captured natively from Deribit from June 2026 onward; earlier greeks
        are computed via Black-76 from the stored implied volatility (IV and
        open interest are native throughout).
        """
        params = {
            "currency": currency,
            "instrument_name": instrument_name,
            "option_type": option_type,
            "expiry": expiry,
            "start": _to_iso(start),
            "end": _to_iso(end),
            "limit": limit,
        }
        return self._query("options", params, paginate, max_rows)

    # ------------------------------------------------------------------ #
    # Metadata
    # ------------------------------------------------------------------ #
    def symbols(self) -> pd.DataFrame:
        """Available symbols with per-exchange date coverage."""
        body = self._request("symbols", {})
        df = pd.DataFrame(body.get("symbols") or [])
        for col in ("available_from", "available_to"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
        return df

    def exchanges(self) -> pd.DataFrame:
        """Supported exchanges with their datasets and symbol counts."""
        body = self._request("exchanges", {})
        return pd.DataFrame(body.get("exchanges") or [])

    def datasets(self) -> pd.DataFrame:
        """Available datasets and descriptions."""
        body = self._request("datasets", {})
        return pd.DataFrame(body.get("datasets") or [])

    def status(self) -> Dict[str, Any]:
        """API health and per-dataset data freshness (raw dict)."""
        return self._request("status", {})

    # ------------------------------------------------------------------ #
    # L2 order book and raw trades (daily Parquet files)
    # ------------------------------------------------------------------ #
    def l2_files(
        self,
        dataset: str,
        symbol: str,
        start: Union[str, date, datetime],
        end: Union[str, date, datetime, None] = None,
    ) -> Dict[str, Any]:
        """List the daily L2 files for a symbol, with 15-minute download links.

        ``dataset`` is ``"book"`` (hourly diff files plus the day's REST snapshots) or ``"trades"``
        (one file of raw tick trades). Binance USD-M perpetuals, UTC days, at most 31 days per call.
        Pro and Enterprise get every day; other plans get the 1st of each month, up to 10 GiB of
        new files per month.

        Returns the raw response: ``days`` (each with ``files``: name, key, size, sha256, url, rows,
        and a QC ``summary``), ``missing`` (days not available, with a reason) and ``usage``.
        """
        if dataset not in L2_DATASETS:
            raise InvalidParameterError(f"dataset must be one of {L2_DATASETS}", code="invalid_parameter")
        first = _to_day(start)
        last = _to_day(end) if end is not None else None
        return self._request("l2/files", {
            "dataset": dataset,
            "symbol": symbol.upper(),
            "start": first.isoformat() if first else None,
            "end": last.isoformat() if last else None,
        })

    def l2_coverage(self, dataset: Optional[str] = None, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Published L2 days per dataset and symbol: first and last day, day count, and any days in
        between that aren't published, with the reason. Public endpoint, cached for 10 minutes."""
        if dataset is not None and dataset not in L2_DATASETS:
            raise InvalidParameterError(f"dataset must be one of {L2_DATASETS}", code="invalid_parameter")
        return self._request("l2/coverage", {"dataset": dataset, "symbol": symbol.upper() if symbol else None})

    def l2_gaps(
        self,
        dataset: Optional[str] = None,
        symbol: Optional[str] = None,
        start: Union[str, date, datetime, None] = None,
        end: Union[str, date, datetime, None] = None,
    ) -> Dict[str, Any]:
        """The public gap log: every stretch on a published day that none of the three capture nodes
        recorded, each with the reason. Public endpoint, cached for 10 minutes."""
        if dataset is not None and dataset not in L2_DATASETS:
            raise InvalidParameterError(f"dataset must be one of {L2_DATASETS}", code="invalid_parameter")
        first, last = _to_day(start), _to_day(end)
        return self._request("l2/gaps", {
            "dataset": dataset,
            "symbol": symbol.upper() if symbol else None,
            "start": first.isoformat() if first else None,
            "end": last.isoformat() if last else None,
        })

    @_no_key_in_errors
    def download_l2(
        self,
        dataset: str,
        symbol: str,
        start: Union[str, date, datetime],
        end: Union[str, date, datetime, None],
        dest_dir: Union[str, os.PathLike],
        verify: bool = True,
        max_bytes: Optional[int] = None,
        max_file_bytes: int = _L2_MAX_FILE_BYTES,
        deadline: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Download daily L2 files to ``dest_dir/<dataset>/binance/<SYMBOL>/<YYYY-MM-DD>/``.

        Ranges longer than 31 days are split into several calls. Each file streams to a ``.part``
        file and is renamed into place only after it arrived before the deadline with the size and SHA-256
        the API reported; otherwise the ``.part`` file is deleted. Files already on disk with a matching
        size (and, with ``verify=True``, hash) are skipped, so a rerun resumes. Network errors and 5xx responses are retried with backoff, and expired links
        are refreshed. Days the API can't serve are returned under ``missing``, not raised.

        Limits: the listing is refused if any file is bigger than ``max_file_bytes`` or the files still
        to fetch add up to more than ``max_bytes`` (checked per 31-day call, before that call downloads
        anything). A stream that runs past its listed size is cut off at once.

        ``deadline`` (seconds) is checked before every request, file and retry wait, and after each chunk
        requests yields while reading a response body, listings included (8 KiB chunks are requested; a
        compressed response can yield larger decoded chunks). Each request's connect and read timeouts are
        cut to the time left. It isn't a hard time limit: a server that keeps sending bytes, each gap shorter
        than the read timeout, can stretch one chunk's read past the deadline for as long as it keeps that up.
        Listings are always read through a 64 MiB cap here, with or without a deadline.

        Returns ``{"downloaded": [paths], "skipped": [paths], "missing": [...], "bytes": n}``.
        """
        return self._download_l2(dataset, symbol, start, end, dest_dir, verify, max_bytes, max_file_bytes, deadline)

    @_no_key_in_errors
    def l2_sample(self, symbol: Optional[str] = None, date: Union[str, date, None] = None,
                  dataset: Optional[str] = None) -> Dict[str, Any]:
        """The no-account sample: without arguments, the sample days on offer (``samples``); with a symbol and
        date, that day's listing with 15-minute links, shaped like :meth:`l2_files`. Works on a
        ``CandleFeed(public=True)`` client. The server limits listings and bytes per address."""
        if dataset is not None and dataset not in L2_DATASETS:
            raise InvalidParameterError(f"dataset must be one of {L2_DATASETS}", code="invalid_parameter")
        day = _to_day(date) if date is not None else None
        return self._request("l2/sample", {"dataset": dataset, "symbol": symbol.upper() if symbol else None,
                                           "date": day.isoformat() if day else None})

    @_no_key_in_errors
    def download_l2_sample(self, symbol: str, date: Union[str, date, datetime], dest_dir: Union[str, os.PathLike],
                           dataset: str = "book", verify: bool = True, max_file_bytes: int = _L2_MAX_FILE_BYTES,
                           deadline: Optional[float] = None) -> Dict[str, Any]:
        """Download one no-account sample day, no API key needed::

            CandleFeed(public=True).download_l2_sample("BTCUSDT", "2026-10-01", "data/")

        Same layout, checks and return value as :meth:`download_l2` (size and SHA-256 before the rename into
        place, resume, refreshed links), plus ``license``, the sample licence line. The same line is logged once
        per call at INFO on the ``candlefeed`` logger, which is silent unless you configure logging. Call
        ``l2_sample()`` for the days on offer."""
        try:
            logger.info(L2_SAMPLE_LICENSE)
        except Exception:  # the notice is best effort; a broken app log handler mustn't block the download
            pass
        day = _to_day(date)
        result = self._download_l2(dataset, symbol, day, day, dest_dir, verify, None, max_file_bytes, deadline,
                                   lister=lambda first, last: self.l2_sample(symbol, first, dataset))
        result["license"] = L2_SAMPLE_LICENSE
        return result

    def _download_l2(self, dataset, symbol, start, end, dest_dir, verify, max_bytes, max_file_bytes, deadline,
                     lister=None):
        if dataset not in L2_DATASETS:
            raise InvalidParameterError(f"dataset must be one of {L2_DATASETS}", code="invalid_parameter")
        symbol = symbol.upper()
        if not re.match(r"^[A-Z0-9]{2,30}$", symbol):
            raise InvalidParameterError("symbol must look like BTCUSDT", code="invalid_parameter")
        first = _to_day(start)
        last = _to_day(end) if end is not None else first
        if first is None or last is None or last < first:
            raise InvalidParameterError("end must be on or after start", code="invalid_parameter")
        lister = lister or (lambda f, to: self.l2_files(dataset, symbol, f, to))
        if not _safefs.DIR_FD:
            raise CandleFeedError(
                "download_l2 needs directory-relative, no-follow file operations (dir_fd and O_NOFOLLOW) to keep "
                "writes inside dest_dir, and this Python doesn't provide them. They're available in CPython on "
                "Linux and macOS. Nothing was downloaded.", code="unsupported_platform")
        result: Dict[str, Any] = {"downloaded": [], "skipped": [], "missing": [], "bytes": 0}
        limits = {"max_bytes": max_bytes, "max_file_bytes": max_file_bytes,
                  "deadline_at": None if deadline is None else time.monotonic() + deadline}
        self._deadline_at = limits["deadline_at"]
        try:
            window = first
            while window <= last:
                window_end = min(window + timedelta(days=_L2_MAX_DAYS_PER_CALL - 1), last)
                self._download_l2_window(dataset, symbol, window, window_end, Path(dest_dir), verify, result, limits,
                                         lister)
                window = window_end + timedelta(days=1)
        finally:
            self._deadline_at = None
        return result

    @staticmethod
    def _check_l2_listing(listing: Dict[str, Any], dataset: str, first: date, last: date,
                          max_file_bytes: int = _L2_MAX_FILE_BYTES) -> List[Dict[str, Any]]:
        """Every day must be one that was asked for, once, and every file one of the names this dataset
        publishes, once. Anything else is refused before a byte is written."""
        days = listing.get("days") or []
        if not isinstance(days, list):
            raise CandleFeedError("Unexpected L2 listing: days is not a list")
        seen = set()
        for day in days:
            d = str(day.get("date")) if isinstance(day, dict) else None
            try:
                ok = bool(d and _DATE_RE.match(d)) and first <= date.fromisoformat(d) <= last and d not in seen
            except ValueError:
                ok = False
            if not ok:
                raise CandleFeedError(f"Unexpected day in response: {d!r} (asked for {first} to {last})")
            seen.add(d)
            files = day.get("files")
            if not isinstance(files, list):
                raise CandleFeedError(f"Unexpected L2 listing for {d}: files is not a list")
            names = set()
            for f in files:
                name = f.get("name") if isinstance(f, dict) else None
                if name not in _L2_FILE_NAMES[dataset] or name in names:
                    raise CandleFeedError(f"Refusing unsafe or unexpected file name from the API: {name!r}")
                names.add(name)
                size, sha, key, url = f.get("size"), f.get("sha256"), f.get("key"), f.get("url")
                if isinstance(size, bool) or not isinstance(size, int) or not 0 <= size <= max_file_bytes:
                    raise CandleFeedError(f"Refusing {name} for {d}: listed size {size!r} is outside 0..{max_file_bytes}")
                if not isinstance(sha, str) or not _SHA256_RE.match(sha):
                    raise CandleFeedError(f"Refusing {name} for {d}: listed sha256 isn't 64 hex characters")
                if not isinstance(key, str) or not 0 < len(key) <= 1024 or not isinstance(url, str) or len(url) > 8192:
                    raise CandleFeedError(f"Refusing {name} for {d}: malformed key or link")
        return days

    @staticmethod
    def _open_l2_folder(dest_dir: Path, parts: List[str], f: Dict[str, Any], day: str) -> SafeDir:
        try:
            return SafeDir(dest_dir, parts)
        except UnsafePath as exc:
            raise CandleFeedError(f"Refusing to write {f['name']} for {day}: {exc}") from None

    def _download_l2_window(self, dataset, symbol, first, last, dest_dir: Path, verify, result, limits, lister):
        listing = lister(first, last)
        days = self._check_l2_listing(listing, dataset, first, last, limits["max_file_bytes"])
        # One decision per file, made once: a file counts as cached only if it's a plain file with the listed
        # size and (with verify) hash. The same decision drives the budget check and the skip below, so a
        # same-sized but corrupt file is budgeted as the download it will become.
        plan = []
        for day in days:
            for f in day["files"]:
                self._check_deadline(limits["deadline_at"], f["name"])
                sub, _, fname = f["name"].rpartition("/")
                parts = [dataset, "binance", symbol, day["date"]] + ([sub] if sub else [])
                with self._open_l2_folder(dest_dir, parts, f, day["date"]) as folder:
                    cached = folder.regular_size(fname) == f["size"] and (
                        not verify or folder.sha256(fname) == f["sha256"])
                plan.append((day["date"], f, parts, fname, cached))
        need = sum(f["size"] for _, f, _, _, cached in plan if not cached)
        if limits["max_bytes"] is not None and result["bytes"] + need > limits["max_bytes"]:
            raise CandleFeedError(
                f"{first} to {last} needs {need:,} bytes of new files, over the budget of "
                f"{limits['max_bytes']:,} bytes ({result['bytes']:,} already downloaded in this call). "
                "Nothing was downloaded for this range.", code="download_budget")
        result["missing"].extend(listing.get("missing") or [])
        urls: Dict[str, str] = {}
        issued = [0.0]

        def refresh(from_day: str) -> None:
            self._check_deadline(limits["deadline_at"], "a fresh listing")
            fresh = listing if not urls else lister(from_day, last)
            fresh_days = self._check_l2_listing(fresh, dataset, date.fromisoformat(from_day), last,
                                                limits["max_file_bytes"])
            urls.update({f["key"]: self._checked_download_url(f["url"]) for d in fresh_days for f in d["files"]})
            issued[0] = time.monotonic()

        refresh(first.isoformat())
        for day, f, parts, fname, cached in plan:
            with self._open_l2_folder(dest_dir, parts, f, day) as folder:
                path = folder.path / fname
                if cached:
                    result["skipped"].append(str(path))
                    continue
                if limits["max_bytes"] is not None and result["bytes"] + f["size"] > limits["max_bytes"]:
                    raise CandleFeedError(f"Fetching {f['name']} for {day} would pass the budget of "
                                          f"{limits['max_bytes']:,} bytes.", code="download_budget")
                # the .part sits next to the old file until the rename, so its full size must fit on disk now
                free = shutil.disk_usage(folder.path).free
                if free < f["size"] + _L2_DISK_HEADROOM:
                    raise CandleFeedError(f"Not enough disk space for {f['name']} for {day}: it needs "
                                          f"{f['size']:,} bytes plus {_L2_DISK_HEADROOM:,} spare, and "
                                          f"{free:,} are free.", code="disk_space")
                if time.monotonic() - issued[0] > _L2_URL_REFRESH_SECONDS:
                    refresh(day)
                self._fetch_l2_file(urls, f, folder, fname, verify, lambda d=day: refresh(d), limits["deadline_at"])
            result["downloaded"].append(str(path))
            result["bytes"] += f["size"]

    def _checked_download_url(self, url: Any) -> str:
        """Only https links to the configured storage host on port 443, without credentials, are fetched,
        so a bad listing can't point the client at localhost, a private network or any other server."""
        try:
            u = urlsplit(str(url))
            port = u.port
        except ValueError:
            u, port = None, -1
        if (u is None or u.scheme != "https" or (u.hostname or "").lower() != self.storage_host
                or port not in (None, 443) or u.username is not None or u.password is not None):
            where = "an unparseable link" if u is None else f"{u.scheme}://{u.hostname}"
            raise CandleFeedError(f"Refusing a download link to {where}: L2 files are only fetched over "
                                  f"https from {self.storage_host}.")
        return str(url)

    def _timeouts(self, what: str):
        """Connect and read timeouts, cut to the time left when a download_l2 deadline is running."""
        if self._deadline_at is None:
            return self.timeout
        left = self._deadline_at - time.monotonic()
        if left <= 0:
            self._check_deadline(self._deadline_at, what)
        t = min(self.timeout, left)
        return (t, t)

    def _sleep(self, seconds: float, what: str) -> None:
        if self._deadline_at is not None and time.monotonic() + seconds > self._deadline_at:
            raise CandleFeedError(f"Download deadline would pass while waiting to retry {what}; rerun to resume, "
                                  "finished files are kept.", code="download_deadline")
        time.sleep(seconds)

    @staticmethod
    def _body_chunks(resp, what: str, limit: int, identity_only: bool = False):
        """The decoded body, one item per network read, at most ``limit`` + 1 decoded bytes in total.

        requests' own decoder keeps reading until a read decodes to something, so a gzip member with an
        endless comment never hands control back and no deadline is checked. Here every read1() from the socket
        yields (b"" when it decoded to nothing), compressed input is capped too, and decompression is bounded
        by what's left of ``limit``. urllib3 errors pass through; callers retry on them."""
        raw = getattr(resp, "raw", None)
        if not isinstance(raw, urllib3.HTTPResponse):
            yield from resp.iter_content(chunk_size=_BODY_CHUNK)     # test doubles without a real transport
            return
        encoding = (resp.headers.get("Content-Encoding") or "identity").strip().lower()
        if encoding not in _ENCODINGS or (identity_only and encoding != "identity"):
            raise CandleFeedError(f"Response from {what} came with Content-Encoding {encoding[:40]!r}, which "
                                  "the client doesn't accept here; stopped.", code="unsupported_encoding")
        wire_limit = limit + limit // 100 + 65536            # gzip of incompressible data is slightly bigger
        decoder = _ENCODINGS[encoding]() if _ENCODINGS[encoding] else None
        wire = produced = 0
        # read1 returns after one socket read. urllib3 1.26 doesn't have it; its read(amt) waits for amt bytes
        # (8 KiB), so there the deadline is checked per 8 KiB, each read still bounded by the socket timeout.
        read = getattr(raw, "read1", None) or raw.read
        while True:
            data = read(_BODY_CHUNK, decode_content=False)
            if not data:
                break
            wire += len(data)
            if wire > wire_limit:
                raise CandleFeedError(f"Response from {what} is over {limit:,} bytes; stopped.")
            if decoder is None:
                out = data
            else:
                try:
                    out = decoder.decompress(data, limit - produced + 1)
                    while decoder.eof and decoder.unused_data and len(out) <= limit - produced:
                        rest, decoder = decoder.unused_data, _ENCODINGS[encoding]()   # next gzip member
                        out += decoder.decompress(rest, limit - produced - len(out) + 1)
                except zlib.error as exc:
                    raise CandleFeedError(f"Response from {what} isn't valid {encoding}: {exc}") from None
            produced += len(out)
            yield out
            if produced > limit:
                return
        if decoder is not None and not decoder.eof:
            raise CandleFeedError(f"Response from {what} ended in the middle of its {encoding} stream.")

    @staticmethod
    def _check_deadline(deadline_at: Optional[float], what: str) -> None:
        if deadline_at is not None and time.monotonic() > deadline_at:
            raise CandleFeedError(f"Download deadline passed while fetching {what}; rerun to resume, "
                                  "finished files are kept.", code="download_deadline")

    def _fetch_l2_file(self, urls, f, folder: SafeDir, fname: str, verify: bool, refresh,
                       deadline_at: Optional[float] = None) -> None:
        if self._download_session is None:
            self._download_session = requests.Session()
            self._download_session.headers["User-Agent"] = f"candlefeed-python/{_CLIENT_VERSION}"
        part = fname + ".part"
        problem = "no attempt made"
        try:
            for attempt in range(self.max_retries + 1):
                if attempt:
                    self._sleep(self._backoff(attempt - 1), f["name"])
                self._check_deadline(deadline_at, f["name"])
                try:
                    # Spaces serves the Parquet files as they are: no HTTP compression, every read is file bytes.
                    # Storage never needs the API key: drop one inherited from a shared session, per request.
                    resp = self._download_session.get(urls[f["key"]], stream=True,
                                                      headers={"Accept-Encoding": "identity", "X-API-Key": None},
                                                      timeout=self._timeouts(f["name"]), allow_redirects=False)
                except (requests.RequestException, urllib3.exceptions.HTTPError) as exc:
                    self._check_deadline(deadline_at, f["name"])
                    problem = f"network error: {_describe_error(exc)}"
                    continue
                try:
                    if 300 <= resp.status_code < 400:
                        raise CandleFeedError(f"Download of {f['name']} answered HTTP {resp.status_code} with a "
                                              "redirect, which the client doesn't follow.",
                                              status_code=resp.status_code)
                    if resp.status_code in (401, 403):
                        problem = f"HTTP {resp.status_code} (link expired?)"
                        refresh()
                        continue
                    if resp.status_code == 429 or resp.status_code >= 500:
                        problem = f"HTTP {resp.status_code}"
                        continue
                    if resp.status_code != 200:
                        raise CandleFeedError(f"Download of {f['name']} failed with HTTP {resp.status_code}",
                                              status_code=resp.status_code)
                    h, size = hashlib.sha256(), 0
                    folder.unlink(part)            # a stale .part, or a symlink planted in its place, goes first
                    with folder.create_exclusive(part) as out:
                        for chunk in self._body_chunks(resp, f["name"], f["size"], identity_only=True):
                            if not chunk:
                                continue
                            if size + len(chunk) > f["size"]:
                                raise CandleFeedError(f"Download of {f['name']} ran past its listed size of "
                                                      f"{f['size']:,} bytes; stopped.", code="oversized_download")
                            out.write(chunk)
                            h.update(chunk)
                            size += len(chunk)
                            self._check_deadline(deadline_at, f["name"])
                except (requests.RequestException, urllib3.exceptions.HTTPError) as exc:
                    self._check_deadline(deadline_at, f["name"])     # out of time wins over the error
                    problem = f"network error: {_describe_error(exc)}"
                    continue
                except CandleFeedError:
                    self._check_deadline(deadline_at, f["name"])
                    raise
                finally:
                    resp.close()
                # Nothing is committed unless the whole file arrived in time with the listed size and hash. The
                # hash was computed while streaming, so it's checked even with verify=False.
                self._check_deadline(deadline_at, f["name"])
                if size != f["size"]:
                    problem = f"got {size} bytes, expected {f['size']}"
                    continue
                if h.hexdigest() != f["sha256"]:
                    problem = "SHA-256 mismatch"
                    continue
                folder.replace(part, fname)
                return
            self._check_deadline(deadline_at, f["name"])
            raise CandleFeedError(f"Could not download {f['name']} after {self.max_retries + 1} attempts: {problem}")
        finally:
            folder.unlink(part)                # every exit: success (already renamed), failure, deadline, error
