"""Astra #5: when known levels run out of the snapshot window, the best price isn't guessed."""
from __future__ import annotations

from decimal import Decimal

import pytest
from l2_synth import day_ms, write_day

from candlefeed.l2book import BookUnavailable, L2Book

DAY = "2026-09-01"
D = Decimal


def _day(tmp_path):
    """Snapshot (truncated to two levels a side): bids 100, 99; asks 101, 102. The true book also has an
    untouched bid at 98 that the snapshot couldn't hold. Then a diff adds a bid at 97 (now known) and
    later ones delete 100 and 99."""
    t0 = day_ms(DAY)
    written = [
        (t0 + 1000, 101, 101, 100, [("ask", 1020, D("4"))]),
        (t0 + 2000, 102, 102, 101, [("bid", 970, D("7"))]),
        (t0 + 3000, 103, 103, 102, [("bid", 1000, D("0"))]),
        (t0 + 4000, 104, 104, 103, [("bid", 990, D("0"))]),
    ]
    snap = ("tokyo", 101, t0 + 1000, (t0 + 1000) * 10**6,
            [(1000, D("1")), (990, D("2"))], [(1010, D("3")), (1020, D("4"))])
    ddir = write_day(tmp_path, DAY, "BTCUSDT", written, [snap])
    return L2Book([ddir], snapshot_levels=2), t0


def test_known_levels_below_the_window_are_not_reported(tmp_path):
    book, t0 = _day(tmp_path)
    view = book.book_at(t0 + 2000, levels=3)
    assert view.bids.values.tolist() == [[100.0, 1.0], [99.0, 2.0]]   # 97 is known but 98 might sit above it
    assert view.bids_complete is False and view.bid_window_floor == 99.0
    assert view.best_bid == 100.0 and view.spread == 1.0


def test_best_bid_inside_the_window_is_still_trusted(tmp_path):
    book, t0 = _day(tmp_path)
    view = book.book_at(t0 + 3000, levels=1)
    assert view.best_bid == 99.0 and view.bids_complete is True


def test_exhausted_window_gives_no_best_price_spread_or_mid(tmp_path):
    """Astra's case: 100 and 99 deleted, 98 untouched and unseen, 97 known. 97 must not be the best bid."""
    book, t0 = _day(tmp_path)
    view = book.book_at(t0 + 4000, levels=5)
    assert view.best_bid is None and view.mid is None and view.spread is None and view.spread_bps is None
    assert view.bids.empty and view.bids_complete is False
    assert view.best_ask == 101.0
    assert view.to_dict()["best_bid"] is None
    with pytest.raises(BookUnavailable, match="outside the anchor snapshot's window"):
        book.depth_at(t0 + 4000, [10])
    s = book.spread_series("1s", "2026-09-01T00:00:01Z", "2026-09-01T00:00:05Z")
    assert s["bid"].tolist()[:3] == [100.0, 100.0, 99.0] and s["bid"].isna().tolist()[3]
    assert s["spread"].isna().tolist()[3] and s["mid"].isna().tolist()[3]
