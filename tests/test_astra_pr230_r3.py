"""Offline R3 correctness review; assertions for reported defects are not xfailed."""
from __future__ import annotations

import ast
import socket
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest
from conftest import ok
from test_astra_pr230_r1 import T0, Api, export, file, funding_calculation, settlements
from test_astra_pr230_r2 import read, seed_pair, write

from candlefeed import freqtrade as ft

ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("PR230 R3 tests must not use the network")

    for name in ("connect", "connect_ex"):
        monkeypatch.setattr(socket.socket, name, forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)


def snapshot(tmp_path, fmt):
    return {kind: file(tmp_path, kind, fmt).read_bytes() if file(tmp_path, kind, fmt).exists() else None
            for kind in ("funding_rate", "mark")}


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
@pytest.mark.parametrize("native_opens", [[110., 220., 330.], [100., 200., 300.]],
                         ids=["different_fee_basis", "same_open_different_ohlcv"])
def test_r3_existing_native_mark_rows_keep_values_and_fees(client, fake_session, tmp_path, fmt, native_opens):
    seed_pair(tmp_path, [T0 + timedelta(hours=h) for h in (0, 8, 16)], fmt, native_opens)
    path = file(tmp_path, "mark", fmt)
    native = read(path).set_index("date").resample("h").ffill().reset_index()
    native = native.assign(high=native.open + 5, low=native.open - 3, close=native.open + 2, volume=7.)
    write(native, path)
    _, original_fee = funding_calculation(tmp_path, fmt)
    assert original_fee == pytest.approx(sum(native_opens) * .01)
    fake_session.handler(Api())
    result = export(client, tmp_path, data_format=fmt)
    assert result.ok, result.refused
    _, exported_fee = funding_calculation(tmp_path, fmt)
    assert exported_fee == pytest.approx(original_fee), "export changed a usable native funding fee basis"
    pd.testing.assert_frame_equal(read(path), native, check_dtype=False)


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
@pytest.mark.parametrize("dtype", ["Float64", "double[pyarrow]"])
@pytest.mark.parametrize("offset", [8, -24], ids=["unreturned_inside_range", "before_range"])
def test_r3_nullable_cached_rate_must_not_bypass_validation(client, fake_session, tmp_path, fmt, dtype, offset):
    seed_pair(tmp_path, [T0 + timedelta(hours=h) for h in (0, offset, 16)], fmt, [100., 200., 300.])
    rate_path = file(tmp_path, "funding_rate", fmt)
    rates = read(rate_path)
    rates["funding_rate"] = pd.Series([.01, None, .01], dtype=dtype)
    write(rates, rate_path)
    before = snapshot(tmp_path, fmt)
    api = Api(funding=[settlements()[0], settlements()[2]])
    fake_session.handler(api)
    result = export(client, tmp_path, data_format=fmt)
    if not result.ok:
        assert snapshot(tmp_path, fmt) == before
        if offset < 0:
            assert not any(endpoint == "funding-rates" for endpoint, _ in api.calls)
        return
    _, fee = funding_calculation(tmp_path, fmt)
    assert result.refused, f"nullable missing rate passed merged validation; Freqtrade short funding={fee}"


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
def test_control_r3_nullable_rate_is_repaired_when_response_contains_it(client, fake_session, tmp_path, fmt):
    seed_pair(tmp_path, [T0 + timedelta(hours=h) for h in (0, 8, 16)], fmt, [100., 200., 300.])
    path = file(tmp_path, "funding_rate", fmt)
    rates = read(path)
    rates["funding_rate"] = pd.Series([.01, None, .01], dtype="Float64")
    write(rates, path)
    fake_session.handler(Api())
    assert export(client, tmp_path, data_format=fmt).ok
    assert funding_calculation(tmp_path, fmt)[1] == pytest.approx(6.)


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
@pytest.mark.parametrize("offset", [-24, 8], ids=["before_range", "inside_range"])
def test_r3_freqtrade_compatible_millisecond_rates_are_normalized(client, fake_session, tmp_path, fmt, offset):
    times = [T0 + timedelta(hours=h) for h in (offset, 16)]
    seed_pair(tmp_path, times, fmt, [200., 300.])
    path = file(tmp_path, "funding_rate", fmt)
    native_rates = read(path)
    native_rates["date"] += pd.Timedelta(milliseconds=4)
    write(native_rates, path)
    # The actual Freqtrade loader floors funding timestamps to seconds.
    combined, fee = funding_calculation(tmp_path, fmt)
    assert len(combined) == 2 and fee == pytest.approx(3. if offset < 0 else 5.)
    before = snapshot(tmp_path, fmt)
    fake_session.handler(Api())
    result = export(client, tmp_path, data_format=fmt)
    if not result.ok:
        assert snapshot(tmp_path, fmt) == before
    assert result.ok, f"Freqtrade-readable funding times falsely rejected: {result.refused}"
    written = read(path)
    assert written.date.equals(written.date.dt.floor("s"))
    assert written.date.is_unique


def test_r3_readme_must_qualify_paginated_funding_request_count(client, fake_session, tmp_path):
    # Builder-sized pages, with a supported hourly contract and a historical range.
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    rows = [{"time": (start + timedelta(hours=i)).isoformat(), "funding_rate": .01,
             "mark_price": 100., "interval_hours": 1} for i in range(10001)]
    funding_calls = []

    def handler(url, params):
        if not url.endswith("funding-rates"):
            return ok([])
        funding_calls.append(dict(params))
        offset = int(params.get("cursor", "0"))
        page = rows[offset:offset + 10000]
        more = offset + len(page) < len(rows)
        return ok(page, has_more=more, next_cursor=str(offset + len(page)) if more else None)

    fake_session.handler(handler)
    for _ in range(2):
        assert export(client, tmp_path, timerange="20250101-20261114").ok
    assert len(funding_calls) == 4  # two pages on every run, including an already-complete bounded range
    assert len(read(file(tmp_path, "funding_rate"))) == 10001
    readme = (ROOT / "clients/python/README.md").read_text()
    assert "whole range's settlements in one request per pair" not in readme, (
        "README promises one request for the whole range, but this valid range takes two on every run")


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
@pytest.mark.parametrize("fault", ["mark_zero", "mark_nan", "mark_inf", "rate_nan", "rate_inf", "no_mark_file"])
@pytest.mark.parametrize("offset", [-1, 24], ids=["before_start", "exclusive_end"])
def test_control_r3_outside_range_invalid_rows_refuse_before_fetch(client, fake_session, tmp_path, fmt, fault,
                                                               offset):
    seed_pair(tmp_path, [T0, T0 + timedelta(hours=offset)], fmt)
    kind = "funding_rate" if fault.startswith("rate") else "mark"
    path = file(tmp_path, kind, fmt)
    if fault == "no_mark_file":
        path.unlink()
    else:
        frame = read(path)
        column = "funding_rate" if kind == "funding_rate" else "open"
        frame.loc[1, column] = {"zero": 0., "nan": float("nan"), "inf": float("inf")}[fault.split("_")[1]]
        write(frame, path)
    before = snapshot(tmp_path, fmt)
    api = Api()
    fake_session.handler(api)
    result = export(client, tmp_path, data_format=fmt, timerange="20261113-20261114")
    assert not result.ok and len(result.refused) == 1
    assert not any(endpoint == "funding-rates" for endpoint, _ in api.calls)
    assert snapshot(tmp_path, fmt) == before


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
@pytest.mark.parametrize("response", ["complete", "missing_middle", "empty"])
def test_control_r3_missing_mark_file_repairs_or_refuses_untouched(client, fake_session, tmp_path, fmt, response):
    seed_pair(tmp_path, [T0 + timedelta(hours=h) for h in (0, 8, 16)], fmt)
    file(tmp_path, "mark", fmt).unlink()
    before = snapshot(tmp_path, fmt)
    rows = settlements() if response == "complete" else [settlements()[0], settlements()[2]]
    api = Api(funding=[] if response == "empty" else rows)
    fake_session.handler(api)
    result = export(client, tmp_path, data_format=fmt)
    assert len([x for x in api.calls if x[0] == "funding-rates"]) == 1
    if response == "complete":
        assert result.ok
        assert funding_calculation(tmp_path, fmt)[1] == pytest.approx(6.)
    else:
        assert not result.ok and snapshot(tmp_path, fmt) == before


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
@pytest.mark.parametrize("present", ["both", "mark", "funding_rate", "neither"])
def test_control_r3_empty_rebuild_preserves_all_existing_files(client, fake_session, tmp_path, fmt, present):
    seed_pair(tmp_path, [T0], fmt)
    for kind in ("mark", "funding_rate"):
        if present not in ("both", kind):
            file(tmp_path, kind, fmt).unlink()
    before = snapshot(tmp_path, fmt)
    fake_session.handler(Api(funding=[]))
    result = export(client, tmp_path, data_format=fmt, incremental=False)
    assert result.ok == (present == "neither")
    assert snapshot(tmp_path, fmt) == before
    assert not list(tmp_path.rglob("*.tmp"))


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
@pytest.mark.parametrize("kind", ["futures", "open_interest"])
def test_control_r3_empty_rebuild_refuses_incompatible_candle_or_oi_file(client, fake_session, tmp_path, fmt, kind):
    path = file(tmp_path, kind, fmt)
    write(pd.DataFrame({"date": [T0], "price": [123.]}), path)
    before = path.read_bytes()
    fake_session.handler(Api(candles=[], funding=[], oi=[]))
    result = export(client, tmp_path, data_format=fmt, funding=False, open_interest=kind == "open_interest",
                    incremental=False)
    assert not result.ok and path.read_bytes() == before


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
def test_control_r3_extra_mark_hours_outside_range_do_not_require_rates(client, fake_session, tmp_path, fmt):
    seed_pair(tmp_path, [T0, T0 + timedelta(hours=8)], fmt, [100., 200.])
    path = file(tmp_path, "mark", fmt)
    native = read(path).set_index("date").reindex(pd.date_range(T0 - timedelta(days=1),
                       T0 + timedelta(days=2), freq="h")).ffill().bfill().rename_axis("date").reset_index()
    write(native, path)
    fake_session.handler(Api())
    assert export(client, tmp_path, data_format=fmt, timerange="20261113-20261114").ok
    result = read(path)
    assert result.date.tolist() == native.date.tolist()
    nonsettlement = ~native.date.isin([T0 + timedelta(hours=h) for h in (0, 8, 16)])
    pd.testing.assert_frame_equal(result.loc[nonsettlement], native.loc[nonsettlement], check_dtype=False)
    # Existing mark rows are kept (H1 policy), so 16:00 keeps its forward-filled native open of 200, not 300.
    assert funding_calculation(tmp_path, fmt)[1] == pytest.approx(1. + 2. + 2.)


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
@pytest.mark.parametrize("rate", [0., -.01])
def test_control_r3_zero_and_negative_finite_rates_are_valid(client, fake_session, tmp_path, fmt, rate):
    rows = settlements()
    for row in rows:
        row["funding_rate"] = rate
    fake_session.handler(Api(funding=rows))
    assert export(client, tmp_path, data_format=fmt).ok
    assert funding_calculation(tmp_path, fmt)[1] == pytest.approx(rate * 600)


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
def test_control_r3_no_incremental_can_replace_invalid_outside_history(client, fake_session, tmp_path, fmt):
    seed_pair(tmp_path, [T0 - timedelta(days=1)], fmt, [0.])
    fake_session.handler(Api())
    assert export(client, tmp_path, data_format=fmt, incremental=False).ok
    assert len(read(file(tmp_path, "funding_rate", fmt))) == 3
    assert funding_calculation(tmp_path, fmt)[1] == pytest.approx(6.)


def test_control_r3_deleted_funding_helpers_have_no_live_references():
    source = (ROOT / "clients/python/candlefeed/freqtrade.py").read_text()
    tree = ast.parse(source)
    names = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
    names |= {node.name for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.ClassDef))}
    assert not names & {"_unmatched", "_settlements_cover"}
    funding = next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == "funding")
    assert not any(isinstance(node, ast.Name) and node.id == "fetch_from" for node in ast.walk(funding))


@pytest.mark.parametrize("reason", ["plan limit", "quota limit"])
def test_control_r3_readme_example_surfaces_result_error(monkeypatch, reason):
    result = ft.ExportResult(Path("unused"), "futures", error=reason)
    monkeypatch.setattr(ft, "export_freqtrade", lambda *args, **kwargs: result)
    text = (ROOT / "clients/python/README.md").read_text()
    example = text.split("The same thing from Python:", 1)[1].split("```python", 1)[1].split("```", 1)[0]
    with pytest.raises(SystemExit, match=reason):
        exec(compile(example, "README example", "exec"), {})
