"""CandleFeed — the official Python client for the CandleFeed crypto market-data API.

    from candlefeed import CandleFeed

    cf = CandleFeed(api_key="cf_live_...")
    df = cf.get_ohlcv("BTCUSDT", interval="1h", limit=5)
"""
from importlib.metadata import PackageNotFoundError as _PkgNotFound
from importlib.metadata import version as _pkg_version

from .client import CandleFeed
from .exceptions import (
    AuthenticationError,
    CandleFeedError,
    InvalidParameterError,
    RateLimitError,
    TierRestrictedError,
)

try:
    __version__ = _pkg_version("candlefeed")
except _PkgNotFound:  # running from a source checkout without an install
    __version__ = "0.0.0+unknown"

__all__ = [
    "CandleFeed",
    "CandleFeedError",
    "AuthenticationError",
    "TierRestrictedError",
    "InvalidParameterError",
    "RateLimitError",
    "__version__",
]
