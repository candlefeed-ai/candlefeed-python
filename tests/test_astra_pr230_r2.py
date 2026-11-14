"""Offline second-round PR230 review. Regression assertions are intentionally not xfailed."""
from __future__ import annotations

import socket
from datetime import timedelta
from pathlib import Path

import pandas as pd
import pytest
from conftest import error
from test_astra_pr230_r1 import (
    PAIR,
    T0,
    Api,
    export,
    file,
    funding_calculation,
    settlements,
)

from candlefeed import freqtrade as ft


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("PR230 R2 tests must not use the network")

    for name in ("connect", "connect_ex"):
        monkeypatch.setattr(socket.socket, name, forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)


def read(path):
    return pd.read_feather(path) if path.suffix == ".feather" else pd.read_parquet(path)


def write(frame, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix == ".feather":
        frame.to_feather(path)
    else:
        frame.to_parquet(path)


def seed_pair(tmp_path, times, fmt="feather", marks=None):
    dates = pd.to_datetime(times, utc=True)
    write(pd.DataFrame({"date": dates, "funding_rate": 0.01}), file(tmp_path, "funding_rate", fmt))
    marks = [100.0] * len(times) if marks is None else marks
    write(pd.DataFrame({"date": dates, **{c: marks for c in ("open", "high", "low", "close")}, "volume": 0.0}),
          file(tmp_path, "mark", fmt))


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
@pytest.mark.parametrize("bad_mark", [0.0, -10.0, float("nan"), float("inf")], ids=["zero", "negative", "nan", "inf"])
def test_r2_cached_invalid_mark_cannot_succeed_with_wrong_fees(client, fake_session, tmp_path, fmt, bad_mark):
    """Timestamp equality does not establish a usable settlement mark."""
    times = [T0 + timedelta(hours=h) for h in (0, 8, 16)]
    seed_pair(tmp_path, times, fmt, [100.0, bad_mark, 300.0])
    api = Api()
    fake_session.handler(api)
    result = export(client, tmp_path, data_format=fmt)
    if not result.ok:  # A non-destructive refusal is also an acceptable fix.
        return
    _, fees = funding_calculation(tmp_path, fmt)
    assert fees == pytest.approx(6.0), f"cached mark={bad_mark}, short fee={fees}, calls={api.calls}"


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
@pytest.mark.parametrize("cached_hours", [(8, 24), (1, 4, 8, 16)], ids=["sparse_8h", "changing_1h_4h_8h"])
def test_r2_inferred_settlement_gap_cannot_prove_prefix_coverage(client, fake_session, tmp_path, fmt, cached_hours):
    seed_pair(tmp_path, [T0 + timedelta(hours=h) for h in cached_hours], fmt)
    schedule = [(0, 8), (8, 8), (16, 8), (24, 8)] if cached_hours[0] == 8 else [
        (0, 1), (1, 1), (4, 4), (8, 4), (16, 8)]
    rows = [{"time": (T0 + timedelta(hours=h)).isoformat(), "funding_rate": .01,
             "mark_price": 100, "interval_hours": interval} for h, interval in schedule]
    api = Api(funding=rows)
    fake_session.handler(api)
    result = export(client, tmp_path, data_format=fmt)
    if not result.ok:
        return
    dates = set(read(file(tmp_path, "funding_rate", fmt)).date)
    expected = {pd.Timestamp(r["time"]) for r in rows}
    assert dates == expected, f"skipped available settlements {expected - dates}; calls={api.calls}"


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
def test_r2_no_incremental_empty_rebuild_must_not_leave_broken_pair_and_succeed(client, fake_session, tmp_path, fmt):
    # Follow the exact --no-incremental remedy printed for an unrepairable pair.
    seed_pair(tmp_path, [T0, T0 + timedelta(hours=8)], fmt)
    mark_path = file(tmp_path, "mark", fmt)
    write(read(mark_path).iloc[:1], mark_path)
    fake_session.handler(Api(funding=[]))
    assert not export(client, tmp_path, data_format=fmt).ok
    result = export(client, tmp_path, data_format=fmt, incremental=False)
    if not result.ok:
        return
    rates = read(file(tmp_path, "funding_rate", fmt)) if file(tmp_path, "funding_rate", fmt).exists() else pd.DataFrame()
    marks = read(mark_path) if mark_path.exists() else pd.DataFrame()
    assert rates.empty and marks.empty, "successful replacement kept the old, mismatched funding files"


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
@pytest.mark.parametrize("legacy", [False, True], ids=["current_funding", "legacy_funding"])
def test_r2_native_hourly_mark_history_is_not_mistaken_for_missing_settlements(client, fake_session, tmp_path, fmt,
                                                                          legacy):
    seed_pair(tmp_path, [T0, T0 + timedelta(hours=8)], fmt, [100.0, 200.0])
    rate_path, mark_path = file(tmp_path, "funding_rate", fmt), file(tmp_path, "mark", fmt)
    if legacy:
        write(read(mark_path).assign(open=.01, high=0.0, low=0.0, close=0.0), rate_path)
    marks = read(mark_path).set_index("date").resample("h").ffill().reset_index()
    write(marks, mark_path)
    # This is valid Freqtrade data: hourly marks plus 8-hourly settlements.
    _, fees = funding_calculation(tmp_path, fmt)
    assert fees == pytest.approx(3.0)
    fake_session.handler(Api())
    result = export(client, tmp_path, data_format=fmt)
    assert result.ok, f"valid hourly mark rows treated as missing funding settlements: {result.refused}"
    assert set(marks.date) <= set(read(mark_path).date), "migration must retain existing hourly mark history"
    assert len(read(rate_path)) == 3


def test_r2_readme_python_example_surfaces_refused_funding(client, fake_session, tmp_path, monkeypatch, capsys):
    rows = settlements()
    rows[1]["mark_price"] = None
    fake_session.handler(Api(funding=rows))
    result = export(client, tmp_path)
    assert not result.ok and result.refused
    monkeypatch.setattr(ft, "export_freqtrade", lambda *args, **kwargs: result)
    readme = (Path(__file__).resolve().parents[1] / "README.md").read_text()
    example = readme.split("The same thing from Python:", 1)[1].split("```python", 1)[1].split("```", 1)[0]
    try:
        exec(compile(example, "README.md Freqtrade example", "exec"), {})
    except (RuntimeError, SystemExit) as exc:
        assert result.refused[0] in str(exc)
        return
    output = capsys.readouterr()
    assert result.refused[0] in output.out + output.err, "README prints completed candles but hides the refusal"


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
@pytest.mark.parametrize("bad_mark", [None, -1, "nan", "inf", "not-a-price"])
def test_control_r2_fetched_invalid_marks_refuse_without_touching_existing_pair(client, fake_session, tmp_path, fmt,
                                                                            bad_mark):
    seed_pair(tmp_path, [T0], fmt)
    before = {kind: file(tmp_path, kind, fmt).read_bytes() for kind in ("funding_rate", "mark")}
    rows = settlements()
    rows[1]["mark_price"] = bad_mark
    fake_session.handler(Api(funding=rows))
    result = export(client, tmp_path, data_format=fmt)
    assert not result.ok and result.error is None and len(result.refused) == 1
    assert {kind: file(tmp_path, kind, fmt).read_bytes() for kind in before} == before


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
@pytest.mark.parametrize("fail_kind", ["mark", "funding_rate"])
def test_control_r2_replace_failure_preserves_old_file_and_retry_repairs_pair(client, fake_session, tmp_path,
                                                                          monkeypatch, fmt, fail_kind):
    seed_pair(tmp_path, [T0], fmt)
    path = file(tmp_path, fail_kind, fmt)
    before = path.read_bytes()
    original_replace = ft.os.replace

    def fail_replace(src, dst):
        if dst == path:
            raise OSError("R2 injected replace failure")
        return original_replace(src, dst)

    fake_session.handler(Api())
    with monkeypatch.context() as patch:
        patch.setattr(ft.os, "replace", fail_replace)
        with pytest.raises(OSError, match="R2 injected"):
            export(client, tmp_path, data_format=fmt)
    assert path.read_bytes() == before
    api = Api()
    fake_session.handler(api)
    assert export(client, tmp_path, data_format=fmt).ok
    rates, marks = [read(file(tmp_path, kind, fmt)) for kind in ("funding_rate", "mark")]
    assert rates.date.tolist() == marks.date.tolist()
    assert len(rates) == 3
    assert not list(tmp_path.rglob("*.tmp"))


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
@pytest.mark.parametrize("offset", [-1, 24])
def test_control_r2_outside_range_mismatch_refuses_untouched(client, fake_session, tmp_path, fmt, offset):
    seed_pair(tmp_path, [T0, T0 + timedelta(hours=offset)], fmt)
    mark_path = file(tmp_path, "mark", fmt)
    write(read(mark_path).iloc[:1], mark_path)
    before = {kind: file(tmp_path, kind, fmt).read_bytes() for kind in ("funding_rate", "mark")}
    api = Api()
    fake_session.handler(api)
    result = export(client, tmp_path, data_format=fmt, timerange="20261113-20261114")
    assert not result.ok and len(result.refused) == 1
    assert not any(endpoint == "funding-rates" for endpoint, _ in api.calls)
    assert {kind: file(tmp_path, kind, fmt).read_bytes() for kind in before} == before


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
def test_control_r2_missing_rate_companion_is_rebuilt(client, fake_session, tmp_path, fmt):
    seed_pair(tmp_path, [T0, T0 + timedelta(hours=8)], fmt)
    file(tmp_path, "funding_rate", fmt).unlink()
    fake_session.handler(Api())
    assert export(client, tmp_path, data_format=fmt).ok
    assert read(file(tmp_path, "funding_rate", fmt)).date.tolist() == read(file(tmp_path, "mark", fmt)).date.tolist()


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
@pytest.mark.parametrize("layout", ["legacy_funding", "numeric_dates", "unknown", "off_grid"])
def test_control_r2_migration_and_schema_refusal(client, fake_session, tmp_path, fmt, layout):
    seed_pair(tmp_path, [T0 - timedelta(days=1), T0], fmt)
    kind = "funding_rate" if layout == "legacy_funding" else "futures"
    path = file(tmp_path, kind, fmt)
    if layout == "legacy_funding":
        frame = read(file(tmp_path, "mark", fmt)).assign(open=.01)
    else:
        frame = read(file(tmp_path, "mark", fmt))
        if layout == "numeric_dates":
            frame["date"] = [int(t.timestamp() * 1000) for t in frame.date]
        elif layout == "unknown":
            frame = frame.rename(columns={"open": "price"})
        else:
            frame["date"] += pd.Timedelta(minutes=1)
    write(frame, path)
    before = path.read_bytes()
    fake_session.handler(Api())
    result = export(client, tmp_path, data_format=fmt)
    if layout == "legacy_funding":
        assert result.ok
        migrated = read(path)
        assert list(migrated.columns) == ["date", "funding_rate"]
        assert T0 - timedelta(days=1) in set(migrated.date)
    else:
        assert not result.ok and result.refused
        assert path.read_bytes() == before


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
@pytest.mark.parametrize("kind", ["futures", "open_interest"])
def test_control_r2_all_open_rows_are_removed_even_with_empty_response(client, fake_session, tmp_path, fmt, kind):
    path = file(tmp_path, kind, fmt)
    dates = pd.to_datetime([T0])
    frame = pd.DataFrame({"date": dates, "open_interest_amount": [100.0], "open_interest_value": [10000.0]})
    if kind == "futures":
        frame = pd.DataFrame({"date": dates, **{c: [100.0] for c in ("open", "high", "low", "close", "volume")}})
    write(frame, path)
    fake_session.handler(Api(candles=[], funding=[], oi=[]))
    result = export(client, tmp_path, data_format=fmt, now=T0 + timedelta(minutes=30), funding=False,
                    open_interest=kind == "open_interest")
    assert result.ok and read(path).empty


@pytest.mark.parametrize("step", [timedelta(minutes=1), timedelta(minutes=15), timedelta(hours=4), timedelta(days=1)])
def test_control_r2_candle_coverage_uses_first_eligible_grid_point(step):
    start = T0 + timedelta(microseconds=1)
    first = T0 + step
    frame = pd.DataFrame({"date": [pd.Timestamp(first), pd.Timestamp(first + step)]})
    assert ft._ceil(T0, step) == T0
    assert ft._ceil(start, step) == first
    assert ft._fetch_start(frame, start, step) == first + step
    assert ft._fetch_start(frame, T0, step) == T0


def test_control_r2_refused_jobs_continue_and_later_tier_error_stops(client, fake_session, tmp_path, monkeypatch, capsys):
    base = Api()

    def handler(url, params):
        if params["symbol"] == "ETHUSDT":
            return error(403, "tier_restricted", "R2 plan refusal")
        if url.endswith("funding-rates"):
            rows = settlements()
            rows[1]["mark_price"] = None
            return Api(funding=rows)(url, params)
        return base(url, params)

    fake_session.handler(handler)
    monkeypatch.setattr(ft, "_client", lambda key: client)
    code = ft.main(["--pairs", PAIR, "ETH/USDT:USDT", "SOL/USDT:USDT", "--api-key", "cf_live_fake_r2",
                    "--timerange", "20261113-", "--datadir", str(tmp_path)])
    output = capsys.readouterr()
    assert code == 3 and "not written:" in output.err and "stopped:" in output.err
    assert "Done." not in output.out and "cf_live_fake_r2" not in output.err
    assert file(tmp_path, "futures").exists()
    assert not any(call["params"]["symbol"] == "SOLUSDT" for call in fake_session.calls)
