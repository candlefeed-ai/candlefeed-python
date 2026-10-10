# Changelog

## 0.3.4

- Timestamps that mix whole seconds and fractions in one response (`...:01+00:00` next to
  `...:01.123000+00:00`, which tick liquidations and Hyperliquid funding produce) are parsed correctly. Before,
  pandas inferred one format from the first row and every row in the other format got a NaT index. Any
  timestamp that still can't be parsed now raises `CandleFeedError` (code `unparseable_timestamp`) instead of
  becoming NaT.
- `max_rows=N` now asks for at most N rows per request, so `get_ohlcv(..., max_rows=5)` is one request. The
  README's first example used `limit=5`, which is a page size: it paged through the whole range five rows at a
  time and could use the Free plan's 100 daily requests in one call. The example now uses `max_rows=5`.
- New `page_size=` on every paging method, the clearer name for `limit`. `limit` still means rows per request,
  as before. A `limit` under 1,000 on a paging call without `max_rows` raises a `UserWarning` explaining that,
  and passing both `limit` and `page_size` with different values raises `InvalidParameterError`.
- Without `limit`, `page_size` or `max_rows`, each request now asks for 10,000 rows (5,000 on `/combined` and
  aggregated liquidations) instead of the server default of 1,000 (100 on `/basis` and `/combined`). The server
  trims it to the plan's maximum, so Free still gets 1,000 per request and Builder fetches a year of 1h candles
  in one request instead of nine. A single `paginate=False` call can now return up to that many rows.
- A 429 whose `Retry-After` is longer than the new `max_retry_wait` (default 60 s) raises `RateLimitError` at
  once, with `retry_after` and the new `reset_at` (UTC datetime) set, instead of sleeping. Before, the daily
  request limit made the client sleep until 00:00 UTC, up to four times. Every other retry wait (network errors,
  429s without a wait header, 5xx) is cut to `max_retry_wait`; 0 retries at once. It must be a finite number of
  seconds, 0 or more, or `None`, which keeps the old wait-however-long behaviour. Waits over 5 s are logged at
  WARNING on the `candlefeed` logger.
- `Retry-After` given as an HTTP date is honoured; it used to be ignored in favour of short backoff.
- HTTP 500, 502, 503 and 504 on API requests are retried with backoff (and a `Retry-After` capped at
  `max_retry_wait`), keeping the pages already fetched. If retries run out, the exception carries the rows so
  far as `partial` (a DataFrame) and `resume_cursor`, the server cursor the failed request sent.
- New `cursor=` on every cursor-paginated method: repeat a failed call with `cursor=e.resume_cursor` to carry on.
  Cursors are passed back verbatim and never parsed, so compound cursors (`"<time>~<tiebreak>"`) work. Paging
  also stops on an empty page, not only when `next_cursor` or `has_more` says so.
- `get_liquidations_aggregated` asks for 5,000 rows (the endpoint maximum) and pages until it has
  `meta.total` rows. Each follow-up starts at the open of the bucket after the last one returned and asks for
  one extra row: 8h is summed from 4h rows read from 4 h before `start`, which repeats the last bucket once,
  and a bucket-aligned inclusive start doesn't. Repeated buckets are dropped, a follow-up that brings nothing new while rows remain raises `CandleFeedError` (code
  `incomplete_result`) with `partial` and `resume_start` (an empty page while `meta.total` is positive counts
  as no progress too), and a `next_cursor`, if the API ever sends one here, is followed verbatim. `resume_start`
  is the open of the bucket after the last complete one returned, and a `start` on a bucket open drops buckets
  labelled before it, so resuming with `start=e.resume_start` can't return a duplicate or partly rebuilt
  bucket. It used to stop at the server's
  default of 1,000 rows: a Binance BTC `1d` series from 2019 ended in June 2022. Passing `limit` alone keeps the
  single-request behaviour and now warns when the page holds fewer rows than `meta.total`. New `paginate`,
  `max_rows` and `page_size` parameters.
- Tick liquidation rows carry `position_side`, the liquidated position (long or short) on every exchange; a
  row's `side` stays the exchange's raw value. The docstring says which raw value means what per venue, and an
  empty tick frame includes the column.
- Empty results have the endpoint's columns on an empty UTC `DatetimeIndex` instead of a bare `DataFrame()`,
  so `df["close"]` and `df.resample()` work on a quiet window. `get_combined` gets the OHLCV columns plus those
  of each requested field.
- `download_l2` over a range longer than 31 days no longer raises `TierRestrictedError` when one of its 31-day
  parts has no 1st of the month on a plan below Pro. Those days are listed under `missing` with reason
  `plan_restricted`, and the files from the other parts are returned. A range with no 1st of the month at all
  still raises.

## 0.3.3

- `download_l2_sample` returns the sample licence line as `result["license"]`: "Internal use only. See
  https://candlefeed.ai/terms (section 5.3)." It also logs that line once per call at INFO on the `candlefeed`
  logger. The package adds a `NullHandler`, so nothing is printed unless you configure logging, and stdout is
  left alone.

## 0.3.2

- New no-account sample download: `CandleFeed(public=True).download_l2_sample("BTCUSDT", "2026-10-01", "data/")`
  fetches one of the sample days the API offers without an API key, into the same folder layout as
  `download_l2`, with the same size and SHA-256 checks, atomic rename, resume and link refresh.
  `l2_sample()` lists the days on offer. The server limits listings and bytes per address.
- `CandleFeed(public=True)` makes a client that never sends a key, even if `CANDLEFEED_API_KEY` is set or the
  `session` it's given carries an `X-API-Key` header (removed from every request, any capitalisation). Error
  messages redact a key found on the session too. Without `public=True`, a missing key still raises
  `AuthenticationError`.
- A 429 `sample_daily_limit` raises `QuotaExceededError` straight away with the server's message, like the other
  daily and monthly allowances, instead of waiting until the next UTC day.

## 0.3.1

- The README says how to get a key before the first code example, with a link to the free signup page.
- The no-key `AuthenticationError` now points to https://candlefeed.ai/signup instead of the homepage,
  and says the free key needs no card and takes about a minute.
- New `request_deadline` (default 60 s, `None` to turn off): an elapsed-time budget for one attempt at an
  API request, checked after every read of the answer, at its end and before any network error is passed
  on, so a request that's out of time always reports `CandleFeedError` with code `request_deadline`, and
  isn't retried. It's checked between reads: a server that stalls inside a single read (slow response
  headers, endless chunk trailers, a slow proxy CONNECT) is bounded by the socket read timeout
  (`timeout`), not by a hard wall clock. It's per attempt: 429 retries
  start a fresh one, and backoff waits aren't counted. A valid 50,000-row page over a link slower than about
  0.7 Mbit/s needs a larger value or `None`.
- New `candlefeed.client.bounded_get()`: the same capped, deadline-bound GET for callers with their own
  session (the MCP server uses it for the public endpoints). It always sends `Accept-Encoding: gzip,
  deflate`, overriding the session's default, which can offer br or zstd that the reader refuses.
- candlefeed.ai links in the README carry UTM tags so signups from PyPI and GitHub can be counted.

Fixes from Astra's full review of 0.3.0:

- Error messages and codes no longer repeat the client's own API key if the API echoes it back. This covers
  JSON and non-JSON error bodies, retry-exhausted errors and everything raised from `download_l2`.
- Every API response is now read through the 64 MiB capped reader, not only listings inside `download_l2`.
- `L2Book` documents that an instance isn't thread-safe: queries share a cursor and the row cache.
- `examples/funding_carry_backtest.ipynb`: the aggregated funding rate is per hour
  (`funding_rate_basis="per_hour"`), so it's now annualised with 24 hours a day instead of 3 settlements,
  which understated carry 8x. Cumulative carry now sums each settlement as the hourly rate times 8 (the
  basket's stated 8-hour cadence) instead of forward-filling, the text and labels say so, and the stale
  stored outputs of those cells were cleared.
- Download deadlines are checked the same way: after every read, at the end of each file and before a
  network error leads to a retry, so running out of time always reports `download_deadline`.
- A downloaded file is committed (renamed from `.part`) only if it arrived before the deadline with the
  listed size and SHA-256. The hash is computed while streaming, so it's checked even with `verify=False`
  (which now only skips re-hashing files already on disk). Anything else deletes the `.part` file.
- Bodies are decoded by the client (gzip and deflate), bounded by what's left of the size cap, and the
  compressed input is capped too. API requests ask for gzip or deflate only; other encodings are refused.
  L2 files are requested with `Accept-Encoding: identity`, and a compressed file response is refused.
- Works with urllib3 1.26 as well as 2.x: without `HTTPResponse.read1()` the reader falls back to
  `read(8 KiB)`, so there the deadline is checked per 8 KiB received.
- `download_l2`'s deadline is kept per thread, so concurrent requests on one client don't share it.
- Error messages and codes are taken from the API's error JSON only when they're strings. Objects or lists
  in those fields are replaced by the generic message, so a key nested inside them can't reach the exception.

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
