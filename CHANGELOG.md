# Changelog

## 0.3.0

Order book rebuild.

- New `candlefeed.l2book` (install with `pip install "candlefeed[l2]"`). `L2Book.load(dest_dir,
  symbol, start, end)` reads days written by `download_l2` and rebuilds the book with the published
  snapshot and straddle rule. `book_at(ts, levels)`, `spread_series(freq, depth_bps=...)`,
  `depth_at(ts, bps)`, `iterate(start, end, every)` and `segments()`. Prices stay exact integers
  internally; moments inside a chain break or before the first snapshot of a segment are reported as
  unavailable rather than guessed.
- `cf.l2_coverage()` and `cf.l2_gaps()` for the public coverage table and gap log.
- `L2Book` checks every day against its manifest (exchange, symbol, date, generation, full output keys,
  sizes, SHA-256, required event counters, and each file's symbol column) and raises `IncompleteDay` for
  missing, extra, partial, changed or mislabelled files. The SHA-256 checks prove the files are consistent with what CandleFeed published for that day. They aren't a signature: hashes delivered by the same service as the files can't detect that service itself being compromised or malicious. Moments after the last loaded event are
  unavailable. Only levels inside the anchor snapshot's price window are returned, with
  `bids_complete`/`asks_complete`; a side whose best known level has left the window has no best price.
  `spread_series(depth_bps=...)` adds per-band `*_complete` columns. `biggest_move()` finds the largest
  one-minute mid change without spanning a chain break. Memory: `row_cache_bytes` (default 2 GiB) limits the decoded diff rows kept between queries, and an hour file that wouldn't fit in it is refused. It isn't a bound on the rebuild's total memory: the event index (about 80 bytes per diff event, roughly 70 MB for a BTCUSDT day), the decoded snapshots, Arrow's read buffers and any series you build come on top. Hard per-file caps are checked from each footer before decoding: snapshot files at 2 million rows and 512 MiB decoded, hour files at 2 GiB decoded, footers at 64 MB, with column types validated and strings read as dictionaries. Over a cap raises `L2BudgetExceeded`.

Download safety (Astra audit of PR #123):

- API requests no longer follow redirects, so `X-API-Key` can't be carried to another origin.
- Download links must be https on the storage host (`storage_host=` or `CANDLEFEED_L2_STORAGE_HOST`,
  default `candlefeed-l2-canonical.sgp1.digitaloceanspaces.com`), port 443, without credentials; storage
  redirects aren't followed.
- Listings are validated before anything is written: only the requested days, only the dataset's file
  names, sane sizes and hashes. On Linux and macOS (CPython), files are written with no-follow,
  directory-relative operations, so a symlink below `dest_dir` can't redirect a write, even one swapped in
  mid-download. Where those operations don't exist (Windows), `download_l2` refuses to run
  (`code="unsupported_platform"`) instead of falling back to path checks.
- `download_l2(max_bytes=, max_file_bytes=, deadline=)` budgets; a stream that runs past its listed size
  is cut off at once. A cached file counts as cached only with the listed size and hash, for both the
  budget and the skip, and each fetch needs free disk for its whole temp file.
- `deadline` is checked before every request, file and retry wait, and after each chunk requests yields
  while reading a response body, listings included (8 KiB chunks are requested; a compressed response can
  yield larger decoded chunks). Each request's connect and read timeouts are cut to the time left. It
  isn't a hard time limit: a server that keeps sending bytes, each gap shorter than the read timeout, can
  stretch one chunk's read past it. Listings inside `download_l2` are always capped at 64 MiB. Bodies are read through requests, so Content-Encoding is decoded
  and transport errors are retried, and the temp file is removed on every exit that doesn't finish it.

## 0.2.0

Order book (L2) and tick-trade files.

- `cf.l2_files(dataset, symbol, start, end)` lists the daily Parquet files for a Binance USD-M
  symbol (`"book"`: hourly diffs, the day's REST snapshots and a QC manifest; `"trades"`: one file
  of raw tick trades) with 15-minute download links and a per-day QC summary.
- `cf.download_l2(dataset, symbol, start, end, dest_dir, verify=True)` downloads them to
  `dest_dir/<dataset>/binance/<SYMBOL>/<date>/`. Each file streams to a `.part` file and is renamed
  only after its size and SHA-256 match. Files already present with a matching hash are skipped.
  Network errors and 5xx answers are retried with backoff, expired links are refreshed, and ranges
  over 31 days are split into several calls.
- Download requests use their own HTTP session, so the API key is never sent to the storage host.
  Pass `download_session=` to the constructor to supply one.
- Error messages never include a URL's query string. A presigned link's query is a bearer credential, and `requests` puts the full URL in connection errors, so the client reports the exception class and the message with every query replaced by `?<redacted>`.
- New `QuotaExceededError` (a `RateLimitError`) for the L2 monthly allowance and daily download
  limit. These 429s are raised at once instead of being retried.

## 0.1.3

Coverage claims brought in line with what the API actually serves, plus the
interval sets that shipped since 0.1.2.

- **Docs corrected.** `get_liquidations_aggregated` no longer claims "values
  from 2021" — the daily series runs from each contract's perpetual listing
  (Binance 2019-09, Bybit 2020-01, OKX/Huobi 2022-04, Hyperliquid 2026-05), and
  the sub-daily buckets are forward-built and shallower. Funding, OHLCV, basis,
  liquidation, long/short, taker-volume and options docstrings now carry their
  real per-venue history depths and venue lists.
- **Missing intervals documented.** Aggregated liquidations accept `1h` (was
  documented as `4h 6h 8h 12h 1d`); basis accepts native `5m` (was `1h 4h`).
  Both already worked over the wire — only the docs were behind.
- **Interval sets exported.** `OHLCV_INTERVALS`, `OPEN_INTEREST_INTERVALS`,
  `FUNDING_AGGREGATED_INTERVALS`, `LIQUIDATION_INTERVALS`,
  `LIQUIDATIONS_AGGREGATED_INTERVALS`, `LONG_SHORT_INTERVALS` and
  `BASIS_INTERVALS` are importable from `candlefeed`. The client still does not
  reject unknown values, so a newly shipped interval works before this list
  catches up.
- **Response `meta` is no longer discarded.** `cf.last_meta` holds the `meta`
  block from the most recent response (`history_from`, `source`, `total`), and
  every returned frame carries the same dict as `df.attrs["meta"]`.
- **Options window disclosed.** `get_options` documents the rolling ~60-day API
  window and the Black-76 vs native greeks cutover.
- **User-Agent** now reports the installed package version instead of a
  hardcoded `0.1.0`.
- Example notebooks: corrected the funding-depth and aggregated-liquidation
  claims, and flagged that the Deribit notebook's stored window predates the
  rolling 60-day API window.

## 0.1.2

Initial public release on PyPI.
