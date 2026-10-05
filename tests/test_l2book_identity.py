"""Astra recheck-2 #5: a day's manifest and files must be for the requested symbol, exchange, date and generation."""
from __future__ import annotations

import hashlib
import json
import shutil

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from l2_synth import make_day

from candlefeed.l2book import IncompleteDay, L2Book

DAY = "2026-09-01"


def _rewrite_manifest(ddir, fn):
    m = json.loads((ddir / "manifest.json").read_text())
    fn(m)
    (ddir / "manifest.json").write_text(json.dumps(m))


def test_another_symbols_complete_generation_in_a_btc_folder_is_refused(tmp_path):
    """Astra's replay: a self-consistent ETHUSDT day copied into the BTCUSDT folder passed inventory."""
    eth, _, _ = make_day(tmp_path / "src", DAY, symbol="ETHUSDT", seed=4)
    btc = tmp_path / "data" / "book" / "binance" / "BTCUSDT" / DAY
    shutil.copytree(eth, btc)
    with pytest.raises(IncompleteDay, match="isn't the book manifest for binance BTCUSDT"):
        L2Book.load(tmp_path / "data", "BTCUSDT", DAY)
    with pytest.raises(IncompleteDay, match="BTCUSDT"):
        L2Book([btc])                                          # symbol taken from the folder name


@pytest.mark.parametrize("change,match", [
    (lambda m: m.update(exchange="bybit"), "isn't the book manifest"),
    (lambda m: m.update(dataset="trades"), "isn't the book manifest"),
    (lambda m: m.pop("generation"), "generation"),
    (lambda m: m.update(generation="../x"), "generation"),
    (lambda m: m["canonical"].pop("segments"), "canonical counters"),
    (lambda m: m["canonical"].update(events="12"), "canonical counters"),
    (lambda m: m["outputs"][0].update(key=m["outputs"][0]["key"].replace("BTCUSDT", "ETHUSDT")), "outside"),
    (lambda m: m["outputs"][0].update(key=m["outputs"][0]["key"].replace("gen=synthetic", "gen=other")), "outside"),
    (lambda m: m["outputs"][0].update(key="elsewhere/" + m["outputs"][0]["key"]), "outside"),
])
def test_manifest_identity_and_counters_are_required(tmp_path, change, match):
    ddir, _, _ = make_day(tmp_path, DAY, seed=4)
    _rewrite_manifest(ddir, change)
    with pytest.raises(IncompleteDay, match=match):
        L2Book([ddir])


@pytest.mark.parametrize("statistics", [True, False])
def test_files_whose_rows_are_for_another_symbol_are_refused(tmp_path, statistics):
    """Manifest and hashes all say BTCUSDT, but one hour file's rows are ETHUSDT."""
    ddir, _, _ = make_day(tmp_path, DAY, seed=4)
    path = ddir / "depth" / "07.parquet"
    t = pq.read_table(path)
    t = t.set_column(t.schema.get_field_index("symbol"), "symbol", pa.array(["ETHUSDT"] * t.num_rows))
    pq.write_table(t, path, write_statistics=statistics)
    body = path.read_bytes()

    def fix(m):
        for o in m["outputs"]:
            if o["key"].endswith("depth/07.parquet"):
                o.update(bytes=len(body), sha256=hashlib.sha256(body).hexdigest())

    _rewrite_manifest(ddir, fix)
    with pytest.raises(IncompleteDay, match="BTCUSDT"):
        L2Book([ddir])


def test_a_layout_without_a_symbol_folder_needs_symbol(tmp_path):
    ddir, _, _ = make_day(tmp_path, DAY, seed=4)
    odd = tmp_path / "odd" / DAY
    shutil.copytree(ddir, odd)
    with pytest.raises(ValueError, match="pass symbol="):
        L2Book([odd])
    assert L2Book([odd], symbol="BTCUSDT").symbol == "BTCUSDT"
