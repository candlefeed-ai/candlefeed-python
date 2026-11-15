"""A bad CandleFeed mark price still refuses the pair whenever that settlement has no usable mark on disk to keep."""
from __future__ import annotations

from datetime import timedelta

import pytest
from test_astra_pr230_r1 import T0, Api, export, file, settlements
from test_astra_pr230_r2 import seed_pair
from test_astra_pr230_r3 import snapshot
from test_astra_pr230_r4 import no_network  # noqa: F401


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
@pytest.mark.parametrize("bad_mark", [0., None, float("nan")], ids=["zero", "null", "nan"])
def test_bad_api_mark_at_a_settlement_the_file_lacks_refuses(client, fake_session, tmp_path, fmt, bad_mark):
    seed_pair(tmp_path, [T0, T0 + timedelta(hours=8)], fmt, [110., 220.])
    before = snapshot(tmp_path, fmt)
    rows = settlements()
    rows[2]["mark_price"] = bad_mark
    fake_session.handler(Api(funding=rows))
    result = export(client, tmp_path, data_format=fmt)
    assert not result.ok and "none in the mark file" in result.refused[0]
    assert snapshot(tmp_path, fmt) == before


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
def test_no_incremental_has_no_retained_mark_to_fall_back_on(client, fake_session, tmp_path, fmt):
    seed_pair(tmp_path, [T0, T0 + timedelta(hours=8)], fmt, [110., 220.])
    before = snapshot(tmp_path, fmt)
    rows = settlements()
    rows[1]["mark_price"] = 0.
    fake_session.handler(Api(funding=rows))
    result = export(client, tmp_path, data_format=fmt, incremental=False)
    assert not result.ok and "no mark price in CandleFeed" in result.refused[0]
    assert snapshot(tmp_path, fmt) == before


@pytest.mark.parametrize("fmt", ["feather", "parquet"])
def test_fresh_export_with_a_bad_api_mark_refuses(client, fake_session, tmp_path, fmt):
    rows = settlements()
    rows[1]["mark_price"] = None
    fake_session.handler(Api(funding=rows))
    result = export(client, tmp_path, data_format=fmt)
    assert not result.ok and "no mark price in CandleFeed" in result.refused[0]
    assert not file(tmp_path, "funding_rate", fmt).exists() and not file(tmp_path, "mark", fmt).exists()
