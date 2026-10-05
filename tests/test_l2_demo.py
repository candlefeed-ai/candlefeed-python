"""examples/l2_agent_demo.py on a synthetic sample day with one known big move."""
from __future__ import annotations

import importlib.util
import io
from pathlib import Path

import pandas as pd
import pytest
from l2_synth import make_day

DEMO = Path(__file__).resolve().parents[3] / "examples" / "l2_agent_demo.py"


@pytest.fixture(scope="module")
def demo():
    if not DEMO.exists():
        pytest.skip("not in the monorepo")
    spec = importlib.util.spec_from_file_location("l2_agent_demo", DEMO)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_demo_finds_the_move_and_prints_the_window(demo, tmp_path):
    # background drift of one tick every 10 events, and a 6-tick jump at event 1500 (15:00:00 UTC)
    moves = {k: 1 for k in range(0, 2400, 10)}
    moves[1500] = 6
    make_day(tmp_path, "2026-09-01", seed=4, moves=moves)
    out = io.StringIO()
    result = demo.run("BTCUSDT", "2026-09-01", str(tmp_path), window=5, download=False, out=out)
    assert result["minute_ending"] == pd.Timestamp("2026-09-01T15:00:00Z")
    assert result["bps"] > 0 and len(result["table"]) == 11
    text = out.getvalue()
    assert "Biggest 1-minute move" in text and "minute ending 15:00 UTC" in text and "14:55" in text
    assert not result["table"][["spread_bps", "bid_10bps", "ask_50bps"]].isna().any().any()


def test_demo_plot(demo, tmp_path):
    pytest.importorskip("matplotlib")
    make_day(tmp_path, "2026-09-01", seed=4, moves={1500: 6})
    png = tmp_path / "move.png"
    demo.run("BTCUSDT", "2026-09-01", str(tmp_path), window=3, download=False, plot=str(png), out=io.StringIO())
    assert png.stat().st_size > 10_000
