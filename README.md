# candlefeed

**The official Python client for [CandleFeed](https://candlefeed.ai) — crypto market data that lands straight in a pandas DataFrame.**

OHLCV, funding rates, open interest, liquidations, long/short ratio, taker volume, basis, and Deribit options — across Binance, Bybit, OKX, and more. Auth, cursor pagination, and rate limits are handled for you. One call, one DataFrame.

```bash
pip install candlefeed
```

## Time to first DataFrame

```python
from candlefeed import CandleFeed

cf = CandleFeed(api_key="cf_live_...")          # or set CANDLEFEED_API_KEY
df = cf.get_ohlcv("BTCUSDT", interval="1h", limit=5)
print(df)
```

```
                              open      high       low     close      volume   quote_volume
time
2026-06-06 18:00:00+00:00  69841.0   70120.5   69770.0   70011.2   1183.42   8.27e+07
2026-06-06 19:00:00+00:00  70011.2   70250.0   69905.1   70180.9   964.18    6.77e+07
...
```

The frame is indexed by a tz-aware `DatetimeIndex` and every numeric column is a float — ready for `.resample()`, `.rolling()`, or a backtest loop.

## Authentication

Pass your key directly or via the environment:

```python
cf = CandleFeed(api_key="cf_live_...")
# or
export CANDLEFEED_API_KEY=cf_live_...
cf = CandleFeed()
```

Get a key at **[candlefeed.ai](https://candlefeed.ai)**. The free tier covers Binance majors and the last 30 days; Builder/Pro unlock full history, every exchange, and the derived datasets.

## Endpoints

| Method | Data | Key params |
| --- | --- | --- |
| `get_ohlcv` / `get_candles` | OHLCV candles | `symbol, exchange, interval, start, end, limit` |
| `get_funding_rates` | Per-exchange funding | `symbol, exchange, ...` |
| `get_funding_rates_aggregated` | OI-weighted funding across venues | `symbol, interval, exchanges` |
| `get_open_interest` | Open interest | `symbol, exchange, interval` |
| `get_liquidations` | Liquidation events (tick or bucketed) | `symbol, exchange, side, interval` |
| `get_liquidations_aggregated` | Pre-aggregated liquidation history | `symbol, exchange, interval` |
| `get_long_short_ratio` | Long/short account ratio | `symbol, exchange, interval, ratio_type` |
| `get_taker_volume` | Taker buy/sell volume | `symbol, exchange, ...` |
| `get_basis` | Futures basis / premium | `symbol, exchange, interval` |
| `get_combined` | Time-aligned multi-dataset frame | `symbol, interval, fields` |
| `get_options` | Deribit options chain + greeks | `currency, instrument_name, expiry` |
| `symbols` / `exchanges` / `datasets` / `status` | Metadata | — |

**Intervals** — OHLCV: `1m 5m 15m 1h 4h 1d` · aggregated funding: `1h 4h 1d` · open interest: `5m 15m 1h 4h 1d` · aggregated liquidations: `4h 6h 8h 12h 1d` · basis: `1h 4h`.

## Date ranges auto-paginate

Ask for a window and the client transparently follows the API's `next_cursor` until the range is complete, concatenating into one DataFrame:

```python
df = cf.get_funding_rates(
    "BTCUSDT", exchange="binance",
    start="2026-01-01", end="2026-03-01",
)            # many pages → a single tidy frame
```

Cap the result with `max_rows=...`, control page size with `limit=...`, or disable
auto-paging entirely with `paginate=False` to fetch a single page and inspect the cursor yourself.

## Multi-exchange, time-aligned

```python
agg = cf.get_funding_rates_aggregated(
    "BTCUSDT", interval="1h",
    exchanges=["binance", "bybit", "okx"],
)
agg[["weighted_funding_rate", "total_oi_usd", "exchange_count"]].tail()
```

```python
panel = cf.get_combined(
    "BTCUSDT", interval="1h",
    fields=["ohlcv", "funding_rate", "open_interest"],
)   # OHLCV base timeline with funding + OI forward-filled onto each candle
```

## Error handling

Every API error maps to a typed exception carrying the API `code` and `message`:

```python
from candlefeed import (
    CandleFeed, AuthenticationError, TierRestrictedError,
    InvalidParameterError, RateLimitError,
)

cf = CandleFeed(api_key="cf_live_...")
try:
    df = cf.get_options(currency="BTC")
except TierRestrictedError as e:
    print(e.message)        # "...requires Pro tier or higher... Upgrade at https://candlefeed.ai/pricing"
except RateLimitError as e:
    print("retry after", e.retry_after, "seconds")
except (AuthenticationError, InvalidParameterError) as e:
    print(e.code, e.message)
```

| Exception | HTTP | When |
| --- | --- | --- |
| `AuthenticationError` | 401 | missing / invalid / revoked key |
| `TierRestrictedError` | 403 | symbol, dataset, exchange, or history window above your plan |
| `InvalidParameterError` | 400 / 422 | bad symbol, interval, or timestamp |
| `RateLimitError` | 429 | raised only after the client's bounded backoff retries are exhausted |
| `CandleFeedError` | — | base class; network / unexpected errors |

On HTTP 429 the client honors `Retry-After` / `X-RateLimit-Reset` and retries with exponential backoff before giving up. Remaining-quota headers are exposed on `cf.last_rate_limit`.

## Requirements

Python ≥ 3.9, `requests`, and `pandas`. That's the whole dependency surface.

## Links

- Site & API keys — **https://candlefeed.ai**
- Pricing — **https://candlefeed.ai/#pricing**

MIT licensed.
