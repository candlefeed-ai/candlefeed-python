"""CandleFeed — the official Python client for the CandleFeed crypto market-data API.

    from candlefeed import CandleFeed

    cf = CandleFeed(api_key="cf_live_...")
    df = cf.get_ohlcv("BTCUSDT", interval="1h", limit=5)
"""
from importlib.metadata import PackageNotFoundError as _PkgNotFound
from importlib.metadata import version as _pkg_version

from .client import (
    BASIS_INTERVALS,
    FUNDING_AGGREGATED_INTERVALS,
    L2_DATASETS,
    LIQUIDATION_INTERVALS,
    LIQUIDATIONS_AGGREGATED_INTERVALS,
    LONG_SHORT_INTERVALS,
    OHLCV_INTERVALS,
    OPEN_INTEREST_INTERVALS,
    CandleFeed,
)
from .exceptions import (
    AuthenticationError,
    CandleFeedError,
    InvalidParameterError,
    QuotaExceededError,
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
    "QuotaExceededError",
    "L2_DATASETS",
    "OHLCV_INTERVALS",
    "OPEN_INTEREST_INTERVALS",
    "FUNDING_AGGREGATED_INTERVALS",
    "LIQUIDATION_INTERVALS",
    "LIQUIDATIONS_AGGREGATED_INTERVALS",
    "LONG_SHORT_INTERVALS",
    "BASIS_INTERVALS",
    "__version__",
]
