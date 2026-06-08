"""CandleFeed API client — pandas-native access to crypto market data."""
from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import requests

from .exceptions import (
    AuthenticationError,
    CandleFeedError,
    InvalidParameterError,
    RateLimitError,
    TierRestrictedError,
)

__all__ = ["CandleFeed"]

DEFAULT_BASE_URL = "https://candlefeed.ai/api/v1"
DEFAULT_TIMEOUT = 30.0
DEFAULT_MAX_RETRIES = 4

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
        session: Optional pre-configured :class:`requests.Session`.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        session: Optional[requests.Session] = None,
    ) -> None:
        key = api_key or os.environ.get("CANDLEFEED_API_KEY")
        if not key:
            raise AuthenticationError(
                "No API key provided. Pass api_key= or set the "
                "CANDLEFEED_API_KEY environment variable. Get a key at "
                "https://candlefeed.ai",
                code="unauthorized",
            )
        self.api_key = key
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self._session = session or requests.Session()
        self._session.headers.update(
            {
                "X-API-Key": self.api_key,
                "Accept": "application/json",
                "User-Agent": "candlefeed-python/0.1.0",
            }
        )
        self.last_rate_limit: Dict[str, Optional[str]] = {}

    def __repr__(self) -> str:
        masked = f"{self.api_key[:11]}…" if len(self.api_key) > 11 else "set"
        return f"CandleFeed(base_url={self.base_url!r}, api_key={masked!r})"

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
    def _request(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Issue a single GET, map errors, and retry on 429/transient failures."""
        url = f"{self.base_url}/{path.lstrip('/')}"
        clean = {k: v for k, v in params.items() if v is not None}

        attempt = 0
        while True:
            try:
                resp = self._session.get(url, params=clean, timeout=self.timeout)
            except requests.RequestException as exc:
                if attempt < self.max_retries:
                    time.sleep(self._backoff(attempt))
                    attempt += 1
                    continue
                raise CandleFeedError(f"Request to {url} failed: {exc}") from exc

            self._capture_rate_limit(resp)

            if resp.status_code == 429:
                retry_after = self._retry_after_seconds(resp)
                if attempt < self.max_retries:
                    time.sleep(retry_after if retry_after is not None else self._backoff(attempt))
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
                return resp.json()
            except ValueError as exc:
                raise CandleFeedError(
                    f"Non-JSON response from {url} (HTTP {resp.status_code})."
                ) from exc

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
        # return the envelope at the top level. Support both.
        detail = body.get("detail") if isinstance(body, dict) else None
        if isinstance(detail, dict):
            return detail.get("code"), detail.get("message")
        if isinstance(detail, str):
            return None, detail
        if isinstance(body, dict):
            return body.get("code"), body.get("message")
        return None, None

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
        return self._to_frame(rows)

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

        Intervals: ``1m, 5m, 15m, 1h, 4h, 1d``. Returns a DataFrame indexed by
        ``time`` with ``open, high, low, close, volume, quote_volume`` columns.
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
        """Per-exchange funding rates (``funding_rate``, ``mark_price``)."""
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
        """Pre-aggregated liquidation history (CoinGlass backfill).

        Intervals: ``4h, 6h, 8h, 12h, 1d``. Returns ``long_liq_usd``,
        ``short_liq_usd`` indexed by ``timestamp``. This endpoint is not
        cursor-paginated (it returns ``meta.total``); use ``limit`` to size the
        single page.
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
        return self._to_frame(body.get("data") or [])

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
        ``top_account`` (default), ``global_account``, or ``both``.
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
        """Taker buy/sell volume (``buy_volume, sell_volume, buy_sell_ratio``)."""
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

        Intervals: ``1h, 4h``.
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
