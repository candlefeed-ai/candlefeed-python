"""Astra #4: partial, early-ending or incomplete days never produce a book."""
from __future__ import annotations

import json
from decimal import Decimal

import pandas as pd
import pytest
from l2_synth import day_ms, make_day, write_day

from candlefeed.l2book import BookUnavailable, IncompleteDay, L2Book

DAY = "2026-09-01"


def test_a_day_that_ends_at_00_00_02_has_no_book_at_23_59_59(tmp_path):
    """Astra's reproduction: capture stops two seconds into the day."""
    ddir, truth, _ = make_day(tmp_path, DAY, n_events=40, snapshot_every=20, last_event_ms=2000)
    book = L2Book([ddir])
    assert book.book_at("2026-09-01T00:00:01.900Z").update_id in truth.u
    with pytest.raises(BookUnavailable, match="after the last event"):
        book.book_at("2026-09-01T23:59:59Z")
    s = book.spread_series("1s")
    assert s.loc["2026-09-01T00:00:03Z":, "bid"].isna().all()


@pytest.fixture
def day(tmp_path):
    ddir, truth, _ = make_day(tmp_path, DAY, seed=2)
    return ddir


def test_a_listed_hour_that_is_missing_is_refused(day):
    (day / "depth" / "13.parquet").unlink()
    with pytest.raises(IncompleteDay, match="depth/13.parquet is listed in the manifest but missing"):
        L2Book([day])


def test_a_file_from_another_generation_is_refused(day):
    (day / "depth" / "13.parquet").rename(day / "depth" / "13.bak")
    manifest = json.loads((day / "manifest.json").read_text())
    manifest["outputs"] = [o for o in manifest["outputs"] if not o["key"].endswith("depth/13.parquet")]
    (day / "manifest.json").write_text(json.dumps(manifest))
    (day / "depth" / "13.bak").rename(day / "depth" / "13.parquet")
    with pytest.raises(IncompleteDay, match="13.parquet isn't in this generation's manifest"):
        L2Book([day])


def test_a_partial_download_is_refused(day):
    (day / "depth" / "05.parquet.part").write_bytes(b"half")
    with pytest.raises(IncompleteDay, match="partial download"):
        L2Book([day])


def test_a_corrupted_file_of_the_right_size_is_refused(day):
    path = day / "depth" / "08.parquet"
    body = bytearray(path.read_bytes())
    body[len(body) // 2] ^= 0xFF
    path.write_bytes(bytes(body))
    with pytest.raises(IncompleteDay, match="SHA-256"):
        L2Book([day])


def test_a_missing_manifest_is_refused(day):
    (day / "manifest.json").unlink()
    with pytest.raises(IncompleteDay, match="manifest.json is missing"):
        L2Book([day])


@pytest.mark.parametrize("field,delta", [("events", 1), ("last_event_ms", 1000), ("segments", 1)])
def test_files_that_disagree_with_the_manifest_counts_are_refused(day, field, delta):
    manifest = json.loads((day / "manifest.json").read_text())
    manifest["canonical"][field] += delta
    (day / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(IncompleteDay, match=field):
        L2Book([day])


def test_wrong_day_manifest_is_refused(day):
    manifest = json.loads((day / "manifest.json").read_text())
    manifest["date"] = "2026-09-02"
    (day / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(IncompleteDay, match="book manifest for binance BTCUSDT 2026-09-01"):
        L2Book([day])


def test_a_genuinely_empty_hour_is_fine(tmp_path):
    """No file for 05:00 because nothing happened (the chain continues across it): the book carries."""
    t0 = day_ms(DAY)
    h = 3_600_000
    written = [
        (t0 + 4 * h + 1000, 101, 101, 100, [("bid", 9999, Decimal("1")), ("ask", 10001, Decimal("2"))]),
        (t0 + 4 * h + 2000, 102, 102, 101, [("bid", 9998, Decimal("3"))]),
        (t0 + 6 * h + 1000, 103, 103, 102, [("ask", 10001, Decimal("5"))]),
    ]
    snap = ("tokyo", 101, t0 + 4 * h + 1000, (t0 + 4 * h + 1000) * 10**6,
            [(9999, Decimal("1"))], [(10001, Decimal("2"))])
    ddir = write_day(tmp_path, DAY, "BTCUSDT", written, [snap])
    assert not (ddir / "depth" / "05.parquet").exists()
    book = L2Book([ddir])
    view = book.book_at("2026-09-01T05:30:00Z", levels=None)
    assert view.update_id == 102 and view.bids.values.tolist() == [[999.9, 1.0], [999.8, 3.0]]
    assert book.book_at("2026-09-01T06:00:01Z").asks.values.tolist() == [[1000.1, 5.0]]


def test_verify_false_skips_only_the_hashing(day):
    path = day / "depth" / "08.parquet"
    manifest = json.loads((day / "manifest.json").read_text())
    for o in manifest["outputs"]:
        if o["key"].endswith("depth/08.parquet"):
            o["sha256"] = "0" * 64
    (day / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(IncompleteDay):
        L2Book([day])
    assert L2Book([day], verify=False).book_at(pd.Timestamp("2026-09-01T09:00:00Z")).best_bid
    path.unlink()
    with pytest.raises(IncompleteDay, match="missing"):
        L2Book([day], verify=False)
