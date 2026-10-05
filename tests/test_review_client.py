"""Regression tests from Astra's full review of candlefeed 0.3.0 (2026-10-04). HTTP is mocked."""
from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest
import responses

from candlefeed import CandleFeed, CandleFeedError

ROOT = Path(__file__).resolve().parents[3]
KEY = 'cf_test_REVIEW_ONLY_12345'
BASE = 'http://127.0.0.1:1/v1'


@responses.activate
def test_client_api_error_never_echoes_its_key():
    responses.add(responses.GET, BASE+'/candles', status=401,
                  json={'code': 'invalid_api_key', 'message': 'Unknown key '+KEY})
    with pytest.raises(CandleFeedError) as caught:
        CandleFeed(api_key=KEY, base_url=BASE).get_candles('BTCUSDT')
    assert KEY not in str(caught.value)
    assert KEY not in str(caught.value.message)

@responses.activate
def test_client_non_json_error_never_echoes_its_key():
    responses.add(responses.GET, BASE+'/candles', status=500, body='upstream said '+KEY)
    with pytest.raises(CandleFeedError) as caught:
        CandleFeed(api_key=KEY, base_url=BASE, max_retries=0).get_candles('BTCUSDT')
    assert KEY not in str(caught.value) and KEY not in repr(caught.value.args)


def test_packaged_funding_example_annualizes_hourly_rates():
    import pandas as pd
    notebook = json.loads((ROOT/'clients/python/examples/funding_carry_backtest.ipynb').read_text())
    assignments = [node for cell in notebook['cells'] if cell['cell_type'] == 'code'
                   for node in ast.parse(''.join(cell['source'])).body
                   if isinstance(node, ast.Assign) and any(
                       isinstance(target, ast.Name) and target.id == 'annualised' for target in node.targets)]
    assert len(assignments) == 1
    # Execute only the shipped arithmetic expression, never notebook HTTP cells.
    hourly_rate = 0.0001
    actual = eval(compile(ast.Expression(assignments[0].value), '<notebook annualised>', 'eval'),
                  {'wfr': pd.Series([hourly_rate] * 24)})
    assert actual == pytest.approx(hourly_rate * 24 * 365 * 100)

