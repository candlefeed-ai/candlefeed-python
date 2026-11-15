"""Offline R4 review: real Freqtrade arithmetic, fake transport, no source changes."""
from __future__ import annotations

import socket
from datetime import timedelta

import pandas as pd
import pytest
from test_astra_pr230_r1 import T0, Api, export, file, funding_calculation, settlements
from test_astra_pr230_r2 import read, seed_pair, write
from test_astra_pr230_r3 import snapshot

from candlefeed import freqtrade as ft


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("PR230 R4 review must remain offline")

    for name in ("connect", "connect_ex"):
        monkeypatch.setattr(socket.socket, name, forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
@pytest.mark.parametrize("cached_rates", [False, True], ids=["native_marks_only", "complete_pair"])
@pytest.mark.parametrize("bad_mark", [0., -1., None, float("nan"), float("inf"), "unknown"],
                         ids=["zero", "negative", "null", "nan", "infinity", "text"])
def test_r4_unused_fetched_mark_must_not_refuse_usable_native_marks(
    client, fake_session, tmp_path, fmt, cached_rates, bad_mark,
):
    # The middle settlement is already priced. The tail still needs adding.
    seed_pair(tmp_path, [T0, T0 + timedelta(hours=8)], fmt, [110., 220.])
    if not cached_rates:
        file(tmp_path, "funding_rate", fmt).unlink()
    before = snapshot(tmp_path, fmt)
    native = read(file(tmp_path, "mark", fmt))
    rows = settlements()
    rows[1]["mark_price"] = bad_mark
    fake_session.handler(Api(funding=rows))
    result = export(client, tmp_path, data_format=fmt)
    if not result.ok:
        # This bug fails closed; it does not corrupt or partially publish the pair.
        assert snapshot(tmp_path, fmt) == before
        assert not list(tmp_path.rglob("*.tmp"))
    assert result.ok, f"unused API mark rejected despite usable retained native open: {result.refused}"
    after = read(file(tmp_path, "mark", fmt))
    pd.testing.assert_frame_equal(after.iloc[:2], native, check_dtype=False)
    assert funding_calculation(tmp_path, fmt)[1] == pytest.approx(6.3)


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
@pytest.mark.parametrize("cached_rate", [False, True], ids=["found_after_fetch", "found_before_fetch"])
@pytest.mark.parametrize("dtype", ["Float64", "double[pyarrow]", "string"])
@pytest.mark.parametrize("bad", [None, "0", "-1", "inf", "-inf", "nan"])
def test_control_r4_unusable_kept_settlement_mark_refuses_without_writes(
    client, fake_session, tmp_path, fmt, cached_rate, dtype, bad,
):
    seed_pair(tmp_path, [T0, T0 + timedelta(hours=8)], fmt)
    if not cached_rate:
        rates = read(file(tmp_path, "funding_rate", fmt)).iloc[:1]
        write(rates, file(tmp_path, "funding_rate", fmt))
    path = file(tmp_path, "mark", fmt)
    marks = read(path)
    marks["open"] = pd.Series(["100", bad], dtype=dtype)
    write(marks, path)
    before = snapshot(tmp_path, fmt)
    api = Api()
    fake_session.handler(api)
    result = export(client, tmp_path, data_format=fmt)
    assert not result.ok and "already has" in result.refused[0]
    assert snapshot(tmp_path, fmt) == before
    assert not list(tmp_path.rglob("*.tmp"))
    assert sum(endpoint == "funding-rates" for endpoint, _ in api.calls) == int(not cached_rate)


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
@pytest.mark.parametrize("rate", [.02, -.02, 0.])
def test_control_r4_refetched_rates_replace_cached_rates_but_keep_native_marks(
    client, fake_session, tmp_path, fmt, rate,
):
    seed_pair(tmp_path, [T0 + timedelta(hours=h) for h in (0, 8, 16)], fmt, [110., 220., 330.])
    mark_bytes = file(tmp_path, "mark", fmt).read_bytes()
    assert funding_calculation(tmp_path, fmt)[1] == pytest.approx(6.6)
    rows = settlements()
    for row in rows:
        row["funding_rate"] = rate
    fake_session.handler(Api(funding=rows))
    assert export(client, tmp_path, data_format=fmt).ok
    assert file(tmp_path, "mark", fmt).read_bytes() == mark_bytes
    assert read(file(tmp_path, "funding_rate", fmt)).funding_rate.tolist() == [rate] * 3
    combined, fee = funding_calculation(tmp_path, fmt)
    assert fee == pytest.approx(rate * 660)
    from freqtrade.exchange import Exchange
    assert Exchange.calculate_funding_fees(None, combined, 1, False, T0, T0 + timedelta(days=1)) == pytest.approx(-fee)


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
def test_control_r4_user_rate_revision_is_replaced_by_candlefeed_snapshot(client, fake_session, tmp_path, fmt):
    # The exporter chooses a source, not the newest observation: either side could be newer.
    seed_pair(tmp_path, [T0 + timedelta(hours=h) for h in (0, 8, 16)], fmt, [110., 220., 330.])
    path = file(tmp_path, "funding_rate", fmt)
    rates = read(path).assign(funding_rate=.02)
    write(rates, path)
    assert funding_calculation(tmp_path, fmt)[1] == pytest.approx(13.2)
    mark_bytes = file(tmp_path, "mark", fmt).read_bytes()
    fake_session.handler(Api())  # CandleFeed has .01 for the same settlements.
    assert export(client, tmp_path, data_format=fmt).ok
    assert read(path).funding_rate.tolist() == [.01] * 3
    assert file(tmp_path, "mark", fmt).read_bytes() == mark_bytes
    assert funding_calculation(tmp_path, fmt)[1] == pytest.approx(6.6)


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
@pytest.mark.parametrize("reverse", [False, True])
def test_control_r4_floored_duplicate_keys_select_matching_rate_and_mark(
    client, fake_session, tmp_path, fmt, reverse,
):
    rows = [{"time": (T0 + timedelta(milliseconds=ms)).isoformat(), "funding_rate": rate,
             "mark_price": mark, "interval_hours": 8}
            for ms, rate, mark in [(4, .01, 100.), (47, .02, 200.)]]
    if reverse:
        rows.reverse()
    fake_session.handler(Api(funding=rows))
    assert export(client, tmp_path, data_format=fmt).ok
    rates, marks = [read(file(tmp_path, kind, fmt)) for kind in ("funding_rate", "mark")]
    assert rates.date.tolist() == marks.date.tolist() == [pd.Timestamp(T0)]
    assert rates.funding_rate.tolist() == [.02]
    assert marks.open.tolist() == [200.]
    assert funding_calculation(tmp_path, fmt)[1] == pytest.approx(4.)


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
def test_control_r4_cached_fractional_duplicates_keep_latest_before_merging(client, fake_session, tmp_path, fmt):
    seed_pair(tmp_path, [T0], fmt, [110.])
    path = file(tmp_path, "funding_rate", fmt)
    # Deliberately reverse physical order; latest original timestamp must win after flooring.
    write(pd.DataFrame({"date": [T0 + timedelta(milliseconds=47), T0 + timedelta(milliseconds=4)],
                        "funding_rate": [.02, .01]}), path)
    fake_session.handler(Api(funding=settlements()[1:]))
    assert export(client, tmp_path, data_format=fmt).ok
    rates = read(path)
    assert rates.date.is_unique and rates.date.equals(rates.date.dt.floor("s"))
    assert rates.funding_rate.tolist() == [.02, .01, .01]
    assert funding_calculation(tmp_path, fmt)[1] == pytest.approx(7.2)


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
def test_control_r4_flooring_keeps_separate_hourly_and_changed_cadence_settlements(
    client, fake_session, tmp_path, fmt,
):
    hours = [0, 1, 2, 4, 8, 16]
    rows = [{"time": (T0 + timedelta(hours=h, milliseconds=ms)).isoformat(),
             "funding_rate": .01, "mark_price": 100. + h, "interval_hours": interval}
            for h, ms, interval in zip(hours, [0, 4, 47, 999, 4, 47], [1, 1, 1, 2, 4, 8])]
    fake_session.handler(Api(funding=rows))
    assert export(client, tmp_path, data_format=fmt).ok
    rates, marks = [read(file(tmp_path, kind, fmt)) for kind in ("funding_rate", "mark")]
    assert len(rates) == len(marks) == 6
    assert rates.date.equals(marks.date)
    assert funding_calculation(tmp_path, fmt)[1] == pytest.approx(sum(100 + h for h in hours) * .01)


@pytest.mark.parametrize("dtype", ["Float64", "double[pyarrow]", "string", "object"])
def test_control_r4_numeric_validity_masks_have_no_nullable_holes(dtype):
    values = pd.Series([None, "nan", "inf", "-inf", "0", "-0.01", "1.25"], dtype=dtype)
    converted = ft._floats(values)
    assert str(converted.dtype) == "float64"
    assert converted.abs().lt(float("inf")).tolist() == [False, False, False, False, True, True, True]
    assert ft._usable_mark(converted).tolist() == [False, False, False, False, False, False, True]


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
@pytest.mark.parametrize("value", [float("inf"), float("-inf")])
def test_control_r4_fetched_nonfinite_rate_refuses_without_writes(client, fake_session, tmp_path, fmt, value):
    seed_pair(tmp_path, [T0], fmt)
    before = snapshot(tmp_path, fmt)
    rows = settlements()
    rows[1]["funding_rate"] = value
    fake_session.handler(Api(funding=rows))
    result = export(client, tmp_path, data_format=fmt)
    assert not result.ok and snapshot(tmp_path, fmt) == before


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
def test_control_r4_invalid_mark_at_nonsettlement_hour_is_not_a_funding_event(
    client, fake_session, tmp_path, fmt,
):
    seed_pair(tmp_path, [T0 + timedelta(hours=h) for h in (0, 1, 8, 16)], fmt,
              [100., float("nan"), 200., 300.])
    path = file(tmp_path, "funding_rate", fmt)
    rates = read(path)
    write(rates.drop(index=1).reset_index(drop=True), path)
    mark_bytes = file(tmp_path, "mark", fmt).read_bytes()
    fake_session.handler(Api())
    assert export(client, tmp_path, data_format=fmt).ok
    assert file(tmp_path, "mark", fmt).read_bytes() == mark_bytes
    assert funding_calculation(tmp_path, fmt)[1] == pytest.approx(6.)
