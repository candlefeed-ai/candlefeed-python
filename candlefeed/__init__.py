"""CandleFeed — the official Python client for the CandleFeed crypto market-data API.

    from candlefeed import CandleFeed

    cf = CandleFeed(api_key="cf_live_...")
    df = cf.get_ohlcv("BTCUSDT", interval="1h", limit=5)
"""
from .client import CandleFeed
from .exceptions import (
    AuthenticationError,
    CandleFeedError,
    InvalidParameterError,
    RateLimitError,
    TierRestrictedError,
)

__version__ = "0.1.0"

__all__ = [
    "CandleFeed",
    "CandleFeedError",
    "AuthenticationError",
    "TierRestrictedError",
    "InvalidParameterError",
    "RateLimitError",
    "__version__",
]
