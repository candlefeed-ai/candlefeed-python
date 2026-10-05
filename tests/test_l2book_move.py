"""Astra #8: the biggest 1-minute move is a real one-minute move inside the loaded day."""
from __future__ import annotations

import importlib.util
import io
from pathlib import Path

import pandas as pd
import pytest
from l2_synth import make_day

from candlefeed.l2book import L2Book

DAY = "2026-09-01"


def _gap_day(tmp_path):
    # one tick up every 10 events (36 s apart), a 30-tick jump inside events 1200..1204, which are
    # dropped from the files: the chain breaks from 11:59:24 to 12:03:00 and re-anchors at 12:04:12
    moves = {k: 1 for k in range(0, 2400, 10)}
    moves[1201] = 30
    ddir, _, _ = make_day(tmp_path, DAY, seed=5, snapshot_every=5, drops=[(1199, 5)], moves=moves)
    return L2Book([ddir])


def test_a_jump_across_a_gap_is_not_a_one_minute_move(tmp_path):
    book = _gap_day(tmp_path)
    # the old approach (drop missing closes, then difference) turns the gap into a "1-minute" move
    s = book.spread_series("1s")
    minute = s["mid"].resample("1min", label="right", closed="right").last().dropna()
    old = (minute.diff() / minute.shift() * 1e4).dropna()
    assert old.abs().idxmax() == pd.Timestamp("2026-09-01T12:05:00Z") and old.abs().max() > 0.25
    move = book.biggest_move()
    assert abs(move["bps"]) < 0.05
    assert not pd.Timestamp("2026-09-01T11:59Z") <= move["minute_end"] <= pd.Timestamp("2026-09-01T12:05Z")
    assert move["minute_end"] - move["minute_start"] == pd.Timedelta("1min")
    assert book.book_at(move["book_time"]).mid == pytest.approx(move["to_mid"])


def test_no_eligible_minute_gives_none(tmp_path):
    ddir, _, _ = make_day(tmp_path, DAY, n_events=40, snapshot_every=20, last_event_ms=2000)
    assert L2Book([ddir]).biggest_move() is None


def test_the_label_never_rolls_into_the_next_day(tmp_path):
    moves = {2399: 40}                     # the day's last event, 23:59:24
    ddir, _, _ = make_day(tmp_path, DAY, seed=6, moves=moves)
    move = L2Book([ddir]).biggest_move()
    assert move["minute_end"].date().isoformat() == DAY and move["book_time"] < pd.Timestamp("2026-09-02", tz="UTC")


def test_window_start_is_aligned_to_a_minute_mark(tmp_path):
    book = _gap_day(tmp_path)
    move = book.biggest_move("2026-09-01T06:00:30Z", "2026-09-01T07:00:00Z")
    assert move["minute_start"] >= pd.Timestamp("2026-09-01T06:01:00Z") and move["minute_start"].second == 0


def test_demo_handles_a_day_without_an_eligible_move(tmp_path):
    demo = Path(__file__).resolve().parents[3] / "examples" / "l2_agent_demo.py"
    if not demo.exists():
        pytest.skip("not in the monorepo")
    spec = importlib.util.spec_from_file_location("l2_agent_demo", demo)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    make_day(tmp_path, DAY, n_events=40, snapshot_every=20, last_event_ms=2000)
    out = io.StringIO()
    assert mod.run("BTCUSDT", DAY, str(tmp_path), window=5, download=False, out=out) is None
    assert "no 1-minute move" in out.getvalue()
