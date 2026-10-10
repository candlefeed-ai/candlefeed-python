"""Exception hierarchy for the CandleFeed client."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional


class CandleFeedError(Exception):
    """Base exception for all CandleFeed client errors.

    Carries the API error ``code`` and ``message`` from the response envelope
    (``{"status": "error", "code": ..., "message": ...}``) when available.

    When an auto-paginating call fails after some pages arrived, ``partial`` is a
    DataFrame of the rows fetched so far and ``resume_cursor`` the server cursor
    the failed request sent. To carry on, repeat the call with the same
    arguments plus ``cursor=exc.resume_cursor``. The cursor is opaque (it can
    be a compound value): pass it back unchanged, never as ``start``. Endpoints
    without a cursor (``get_liquidations_aggregated``) set ``resume_start``
    instead, to pass as ``start``. All are ``None`` otherwise.
    """

    partial: Optional[Any] = None
    partial_rows: Optional[List[Dict[str, Any]]] = None
    resume_cursor: Optional[str] = None
    resume_start: Optional[str] = None

    def __init__(
        self,
        message: str,
        code: Optional[str] = None,
        status_code: Optional[int] = None,
    ) -> None:
        self.code = code
        self.status_code = status_code
        self.message = message
        super().__init__(message)

    def __str__(self) -> str:
        parts = []
        if self.status_code is not None:
            parts.append(f"HTTP {self.status_code}")
        if self.code:
            parts.append(str(self.code))
        prefix = f"[{' '.join(parts)}] " if parts else ""
        return f"{prefix}{self.message}"


class AuthenticationError(CandleFeedError):
    """Raised on HTTP 401 — missing, invalid, or revoked API key."""


class TierRestrictedError(CandleFeedError):
    """Raised on HTTP 403 ``tier_restricted`` — the resource needs an upgrade.

    The ``message`` echoes the API's upgrade nudge (which symbol/dataset/exchange
    or history window is gated, and the plan required).
    """


class InvalidParameterError(CandleFeedError):
    """Raised on HTTP 400/422 — a query parameter was rejected by the API."""


class RateLimitError(CandleFeedError):
    """Raised on HTTP 429 after the client's retries are exhausted, or at once when
    the server asks for a longer wait than the client's ``max_retry_wait`` (the
    daily request limit, which resets at 00:00 UTC).

    ``retry_after`` is the server-advertised seconds until the limit resets
    (from ``Retry-After``, in seconds or as an HTTP date, or ``X-RateLimit-Reset``)
    and ``reset_at`` the same moment as a UTC datetime, when known.
    """

    def __init__(
        self,
        message: str,
        code: Optional[str] = None,
        status_code: Optional[int] = None,
        retry_after: Optional[float] = None,
        reset_at: Optional[datetime] = None,
    ) -> None:
        self.retry_after = retry_after
        self.reset_at = reset_at
        super().__init__(message, code=code, status_code=status_code)


class QuotaExceededError(RateLimitError):
    """Raised on HTTP 429 ``quota_exceeded`` or ``daily_download_limit`` for L2 files.

    Not retried: the monthly sample-day allowance resets on the 1st (UTC) and the daily
    download limit at 00:00 UTC. The message says which one and when.
    """
