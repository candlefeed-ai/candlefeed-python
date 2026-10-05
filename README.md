# candlefeed

**The official Python client for [CandleFeed](https://candlefeed.ai) — crypto market data that lands straight in a pandas DataFrame.**

OHLCV, funding rates, open interest, liquidations, long/short ratio, taker volume, basis, and Deribit options — across Binance (perp + spot), Bybit, OKX, dYdX, Hyperliquid, Huobi and Deribit. Auth, cursor pagination, and rate limits are handled for you. One call, one DataFrame.

REST only — there is no WebSocket feed and no announced date for one.

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

Get a key at **[candlefeed.ai](https://candlefeed.ai)**.

| Tier | Requests/day | Max rows/request | Symbols | Datasets | History | Exchanges |
| --- | --- | --- | --- | --- | --- | --- |
| Free | 100 | 1,000 | BTC, ETH, SOL, XRP, DOGE | OHLCV + funding | last 30 days | Binance |
| Builder | 1,000 | 10,000 | all | Binance REST datasets; L2 sample days only | full | Binance |
| Advanced (id `pro`) | 10,000 | 50,000 | all | + Deribit options; L2 sample days only | full | all venues |
| Pro (id `pro_depth`) | 10,000 | 50,000 | all | Advanced + full published L2 archive | full | all venues |

Builder ($29/month) includes Binance REST datasets; Advanced ($99/month) adds other venues and Deribit options. Both include L2 sample days only: the 1st of each month, subject to the 10 GiB monthly allowance. Pro ($149/month) adds the full published L2 archive. Pro includes the full published L2 archive: book from 2026-06-04 and trades from 2026-06-06 for the original 10 symbols; the other 15 symbols start on 2026-09-26. First days are partial.

Everything beyond Binance — Bybit, OKX, dYdX, Hyperliquid, Huobi, Deribit — needs Advanced or higher.

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

**Intervals** — OHLCV: `1m 5m 15m 1h 4h 1d` · aggregated funding: `1h 4h 1d` · open interest: `5m 15m 1h 4h 1d` · bucketed liquidations: `1m 5m 15m 1h 4h 1d` · aggregated liquidations: `1h 4h 6h 8h 12h 1d` · long/short ratio: `5m 15m 1h 4h 1d` · basis: `5m 1h 4h`. The same lists are importable as `candlefeed.OHLCV_INTERVALS`, `candlefeed.BASIS_INTERVALS`, and friends.

## How far back the data goes

The honest version, because a backtest will find out anyway. Full matrix at **[candlefeed.ai/coverage](https://candlefeed.ai/coverage/)**.

| Dataset | Depth |
| --- | --- |
| OHLCV | Binance spot 2017 · Binance perp 2019 · OKX/Bybit 2020 · Hyperliquid 1d 2023 / 4h 2024 / 1h 2025 / 1m–15m 2026 · dYdX 2025 |
| Funding rates | Binance 2020 · Hyperliquid hourly from each contract's HL listing (BTC/ETH/SOL 2023-05-12) · OKX Dec 2025 · Bybit Jan 2026 · dYdX Mar 2026 |
| Open interest | Binance Sep 2020 · other venues Mar 2026 |
| Liquidations — aggregated | `1d` runs from each contract's listing: Binance 2019-09, Bybit 2020-01, OKX/Huobi 2022-04, Hyperliquid 2026-05. Sub-daily buckets are forward-built (2023→2026 by venue) — use `interval="1d"` for multi-year work |
| Liquidations — tick | Forward-collected from late May 2026: OKX 05-27, Binance/Bybit/Hyperliquid 05-28, Huobi 06-02. No pre-2026 tick history anywhere |
| Long/short ratio, taker volume | Binance only — ratio 2020, taker volume from listing (BTC 2019-09) |
| Basis (spot–perp) | Binance only, all 52 symbols — 5m from 2026-04-27, 1h from 2025-12-10, 4h from 2025-05-31 |
| Deribit options | Rolling ~60-day API window (older snapshots archived); greeks native from Jun 2026, Black-76 before that |

Symbol universe: 52 on Binance, 50 Bybit, 45 OKX, 19 Hyperliquid, ~5 dYdX.

Where an endpoint knows its own depth it tells you — `meta.history_from` and `meta.source` come back on `cf.last_meta` and on each frame as `df.attrs["meta"]`.

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

## Order book (L2) and tick trades: daily files

Binance USD-M order book diffs with REST snapshots (`"book"`) and raw tick trades (`"trades"`) come as
daily Parquet files, not DataFrames. `download_l2` fetches them, checks each file's size and SHA-256,
and skips anything already on disk with a matching hash, so a rerun picks up where it stopped.
Downloads need CPython on Linux or macOS: files are written with directory-relative, no-follow
operations so nothing can be redirected outside `dest_dir`, and on platforms without them (Windows)
`download_l2` refuses to run rather than fall back to weaker path checks.

```python
cf = CandleFeed(api_key="cf_live_...")
out = cf.download_l2("book", "BTCUSDT", "2026-09-01", "2026-09-30", "data/")
# data/book/binance/BTCUSDT/2026-09-01/depth/00.parquet ... depth/23.parquet, snapshot.parquet, manifest.json
print(len(out["downloaded"]), "files,", out["bytes"] / 1e9, "GB")
print(out["missing"])   # days not available, each with a reason
```

`cf.l2_files(dataset, symbol, start, end)` returns the listing itself: per day the files (name, size,
sha256, a download link valid for 15 minutes) and a QC summary from that day's manifest. Pro and
Enterprise get every day. Other plans get the 1st of each month, up to 10 GiB of new files per account
per month; downloading the same file again that month doesn't count twice. When an allowance runs out
the client raises `QuotaExceededError` straight away instead of retrying.

`cf.l2_coverage()` and `cf.l2_gaps()` return the public coverage table and gap log (no plan needed).

### Rebuilding the book

`candlefeed.l2book` turns a downloaded `book` day into an order book you can query. It needs pyarrow:

```bash
pip install "candlefeed[l2]"
```

```python
from candlefeed.l2book import L2Book

book = L2Book.load("data/", "BTCUSDT", "2026-09-01")       # one day, or pass an end date for a range
view = book.book_at("2026-09-01T13:05:00Z", levels=10)    # top 10 a side after every event up to then
print(view.best_bid, view.best_ask, view.spread_bps)
print(view.bids.head())                                    # price, qty, best first

spreads = book.spread_series("1s")                         # bid, ask, mid, spread, spread_bps
depth = book.depth_at("2026-09-01T13:05:00Z", bps=[10, 50])  # base and quote size within 10 and 50 bps of mid
for v in book.iterate("2026-09-01T13:00Z", "2026-09-01T14:00Z", every="1min"):
    ...
```

It applies the published rule: anchor on a snapshot that isn't `in_gap`, apply the first event with
`u >= lastUpdateId` whole even when the snapshot's id falls inside it (the straddle case: 848 of the
861 BTCUSDT snapshots on 25 September 2026), and drop the book at every break in the update chain until a snapshot anchors it
again. Moments with no trustworthy book raise `BookUnavailable` in `book_at` and come back as NaN in
`spread_series`. `book.segments()` lists the unbroken stretches of the day and when each was anchored.
Each query anchors on the latest snapshot at or before it, so results don't depend on where you start
reading. A 10-million-row synthetic day loads in under a second and gives a one-second spread series
for the full day in about 4 seconds on an M-series Mac; a BTC day has about 100 million rows. Real-day runtime and peak memory have not been validated; synthetic timings are not production benchmarks.

Depth outside the anchor snapshot's known price window is partial. Report completeness per band and exclude incomplete samples from full-depth statistics. `depth_at` and `spread_series(depth_bps=...)` carry a `*_complete` flag per band, and
`book_at` returns only levels inside the window (`bids_complete`/`asks_complete` say when that's fewer than
asked for; an empty side has no best price). Days are checked against their manifest before use, so an
incomplete or mixed download raises `IncompleteDay`, and a moment after the last loaded event raises
`BookUnavailable`. `row_cache_bytes` (default 2 GiB) limits the decoded diff rows kept between queries, and an hour file that wouldn't fit in it is refused. It isn't a bound on the rebuild's total memory: the event index (about 80 bytes per diff event, roughly 70 MB for a BTCUSDT day), the decoded snapshots, Arrow's read buffers and any series you build come on top. Hard per-file caps are checked from each footer before decoding: snapshot files at 2 million rows and 512 MiB decoded, hour files at 2 GiB decoded, footers at 64 MB, with column types validated and strings read as dictionaries. The manifest has to be the one for that exchange, symbol, date and generation, every
file has to sit under its full published key, and each file's rows have to carry the requested symbol.
The SHA-256 checks prove the files are consistent with what CandleFeed published for that day. They aren't a signature: hashes delivered by the same service as the files can't detect that service itself being compromised or malicious.

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
    print(e.message)        # "...requires the Advanced plan or higher... Upgrade at https://candlefeed.ai/pricing"
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
| `QuotaExceededError` | 429 | L2 monthly sample allowance or daily download limit reached; not retried |
| `CandleFeedError` | — | base class; network / unexpected errors |

On HTTP 429 the client honors `Retry-After` / `X-RateLimit-Reset` and retries with exponential backoff before giving up. Remaining-quota headers are exposed on `cf.last_rate_limit`.

## Requirements

Python ≥ 3.9, `requests`, and `pandas`. That's the whole dependency surface.

## Links

- Site & API keys — **https://candlefeed.ai**
- Pricing — **https://candlefeed.ai/#pricing**

The software is MIT-licensed. CandleFeed data, including samples, is licensed for internal use under Terms §5.3. Published charts, statistics, and research must not include Raw Data or Substantially Raw Derivatives.
