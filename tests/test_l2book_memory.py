"""Astra #10 and recheck-2 #1: what the memory settings really bound, and the hard caps on each file."""
from __future__ import annotations

import hashlib
import json
import random
import re
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from l2_synth import SCALE, make_day

import candlefeed.l2book as l2book
from candlefeed.l2book import IncompleteDay, L2Book, L2BudgetExceeded

DAY = "2026-09-01"
REPO = Path(__file__).resolve().parents[3]


@pytest.fixture(scope="module")
def day(tmp_path_factory):
    ddir, truth, _ = make_day(tmp_path_factory.mktemp("mem"), DAY, seed=8, drops=[(700, 4)],
                              moves={k: random.Random(k).choice((-1, 1)) for k in range(0, 2400, 7)})
    return ddir, truth


def _rehash(ddir: Path, name: str) -> None:
    body = (ddir / name).read_bytes()
    m = json.loads((ddir / "manifest.json").read_text())
    for o in m["outputs"]:
        if o["key"].endswith(name):
            o.update(bytes=len(body), sha256=hashlib.sha256(body).hexdigest())
    (ddir / "manifest.json").write_text(json.dumps(m))


def test_events_split_across_read_batches_are_rebuilt_exactly(day, monkeypatch):
    ddir, truth = day
    monkeypatch.setattr(l2book, "_BATCH_ROWS", 7)          # nearly every batch boundary cuts an event
    book = L2Book([ddir])
    index = {u: i for i, u in enumerate(truth.u)}
    k = SCALE // 10
    n = 0
    for view in book.iterate(levels=None):
        i = index[view.update_id]
        assert {int(round(p * 10)): q for p, q in view.bids.values.tolist()} == {p // k: q for p, q in truth.bids[i].items()}
        n += 1
    assert n > 2000


def test_the_setting_is_a_row_cache_limit_and_says_so(day):
    """Astra recheck-2 #1: a 6,715-byte "decoded memory budget" was accepted while the event index alone
    took 199,264 bytes. The setting is now named for what it bounds, the old name is gone, and no doc claims
    a bound on rebuild memory."""
    ddir, _ = day
    with pytest.raises(TypeError):
        L2Book([ddir], max_decoded_bytes=10 ** 9)
    biggest = max(pq.ParquetFile(p).metadata.num_rows for p in (ddir / "depth").glob("*.parquet"))
    book = L2Book([ddir], row_cache_bytes=biggest * 17)
    index_bytes = sum(a.nbytes for a in (book._E, book._U, book._u, book._pu, book._seg, book._r0, book._r1))
    assert index_bytes > book._row_cache_bytes             # the index sits outside the row cache, by design
    docs = ["clients/python/README.md", "clients/python/CHANGELOG.md", "integrations/mcp/README.md",
            "website/public/llms-full.txt", "website/public/llms.txt", "clients/python/candlefeed/l2book.py",
            "integrations/mcp/candlefeed_mcp/kit.py"]
    for rel in docs:
        path = REPO / rel
        if not path.exists():
            continue
        text = path.read_text()
        assert "max_decoded_bytes" not in text and "MAX_DECODED" not in text, rel
        assert not re.search(r"[Dd]ecoding is bounded|bound(?:s|ed)? (?:the )?(?:total|rebuild) memory", text), rel


def test_a_file_bigger_than_the_row_cache_is_refused_before_decoding(day):
    ddir, _ = day
    biggest = max(pq.ParquetFile(p).metadata.num_rows for p in (ddir / "depth").glob("*.parquet"))
    with pytest.raises(L2BudgetExceeded, match="rows for this file"):
        L2Book([ddir], row_cache_bytes=(biggest - 1) * 17)


def test_the_row_cache_stays_inside_its_limit(day, monkeypatch):
    ddir, _ = day
    biggest = max(pq.ParquetFile(p).metadata.num_rows for p in (ddir / "depth").glob("*.parquet"))
    limit = int(biggest * 17 * 1.5)                          # room for one hour, not two
    book = L2Book([ddir], row_cache_bytes=limit, cache_hours=5)
    seen = []
    original = book._rows

    def watched(h):
        rows = original(h)
        seen.append((book._cached_bytes, len(book._row_cache)))
        return rows

    monkeypatch.setattr(book, "_rows", watched)
    views = list(book.iterate(every="20min", levels=3))
    assert len(views) > 60
    assert max(b for b, _ in seen) <= limit and max(n for _, n in seen) == 1


def test_oversized_parquet_metadata_is_refused(day, monkeypatch):
    ddir, _ = day
    monkeypatch.setattr(l2book, "_MAX_PARQUET_METADATA_BYTES", 16)
    with pytest.raises(L2BudgetExceeded, match="metadata"):
        L2Book([ddir])


def test_snapshot_row_cap(day, monkeypatch):
    ddir, _ = day
    rows = pq.ParquetFile(ddir / "snapshot.parquet").metadata.num_rows
    monkeypatch.setattr(l2book, "_MAX_SNAPSHOT_ROWS", rows - 1)
    with pytest.raises(L2BudgetExceeded, match="rows"):
        L2Book([ddir])


def test_snapshot_byte_cap_counts_variable_length_strings(tmp_path, monkeypatch):
    """A snapshot whose node strings are huge and unique would decode to far more than its row count
    suggests; the estimate includes the string pages, so it's refused before reading."""
    ddir, _, _ = make_day(tmp_path, DAY, seed=8)
    path = ddir / "snapshot.parquet"
    t = pq.read_table(path)
    huge = pa.array([f"{i:08d}" + "x" * 20_000 for i in range(t.num_rows)])
    pq.write_table(t.set_column(t.schema.get_field_index("node"), "node", huge), path)
    _rehash(ddir, "snapshot.parquet")
    monkeypatch.setattr(l2book, "_MAX_SNAPSHOT_BYTES", 32 * 1024 ** 2)
    with pytest.raises(L2BudgetExceeded, match="would decode about"):
        L2Book([ddir])


def test_a_snapshot_column_with_the_wrong_type_is_refused(tmp_path):
    ddir, _, _ = make_day(tmp_path, DAY, seed=8)
    path = ddir / "snapshot.parquet"
    t = pq.read_table(path)
    t = t.set_column(t.schema.get_field_index("price"), "price", pc_cast_float(t["price"]))
    pq.write_table(t, path)
    _rehash(ddir, "snapshot.parquet")
    with pytest.raises(IncompleteDay, match="'price' is missing or has an unexpected type"):
        L2Book([ddir])


def pc_cast_float(col):
    import pyarrow.compute as pc
    return pc.cast(col, pa.float64())
