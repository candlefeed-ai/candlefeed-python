"""Exception hierarchy for the CandleFeed client."""
from __future__ import annotations

from typing import Optional


class CandleFeedError(Exception):
    """Base exception for all CandleFeed client errors.

    Carries the API error ``code`` and ``message`` from the response envelope
    (``{"status": "error", "code": ..., "message": ...}``) when available.
    """

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
    """Raised on HTTP 429 after the client's retries are exhausted.

    ``retry_after`` is the server-advertised seconds until the limit resets
    (from the ``Retry-After`` header), when present.
    """

    def __init__(
        self,
        message: str,
        code: Optional[str] = None,
        status_code: Optional[int] = None,
        retry_after: Optional[float] = None,
    ) -> None:
        self.retry_after = retry_after
        super().__init__(message, code=code, status_code=status_code)


class QuotaExceededError(RateLimitError):
    """Raised on HTTP 429 ``quota_exceeded`` or ``daily_download_limit`` for L2 files.

    Not retried: the monthly sample-day allowance resets on the 1st (UTC) and the daily
    download limit at 00:00 UTC. The message says which one and when.
    """
