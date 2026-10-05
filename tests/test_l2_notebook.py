"""The sample-day notebook's cells, run offline against synthetic inputs (Astra's PR #130 review).

Cells are addressed by index, so keep the notebook's cell order or update the indices here. No API client and
no network: socket connections are refused.
"""
import hashlib
import json
import re
import socket
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from candlefeed.l2book import L2Book

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

NOTEBOOK = json.loads((Path(__file__).resolve().parents[1] / "examples" / "l2_free_sample_day.ipynb").read_text())


def source(index):
    return "".join(NOTEBOOK["cells"][index]["source"])


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def deny(*args, **kwargs):
        raise AssertionError("Network forbidden during PR130 review")
    monkeypatch.setattr(socket.socket, "connect", deny)
    monkeypatch.setattr(socket, "create_connection", deny)
    yield
    plt.close("all")


def test_verification_cell_accepts_full_plan_usage(tmp_path):
    """Pro/Enterprise have limit_bytes=None and no used_bytes/resets_on."""
    day = tmp_path / "book/binance/BTCUSDT/2026-10-01"
    day.mkdir(parents=True)
    payload = b"{}"
    (day / "manifest.json").write_bytes(payload)
    listing = {
        "days": [{"files": [{"name": "manifest.json", "size": len(payload),
                             "sha256": hashlib.sha256(payload).hexdigest()}]}],
        "usage": {"month": "2026-10-01", "new_bytes": len(payload),
                  "issued_today_bytes": len(payload), "daily_limit_bytes": 1024**4,
                  "limit_bytes": None},
    }
    env = dict(cf=SimpleNamespace(l2_files=lambda *args: listing), DATA=tmp_path,
               SYMBOL="BTCUSDT", DAY="2026-10-01", pd=pd, hashlib=hashlib)
    exec(compile(source(5), "notebook-cell-5", "exec"), env)


def test_day_chart_preserves_unavailable_seconds():
    """A 20-second hole must not become a continuous mid line."""
    index = pd.date_range("2026-10-01", periods=180, freq="1s", tz="UTC")
    spread = pd.DataFrame({"mid": 84000.0, "spread": 0.1}, index=index)
    spread.iloc[100:120] = np.nan
    env = dict(pd=pd, plt=plt, spread=spread, tick=0.1, SYMBOL="BTCUSDT", DAY="2026-10-01")
    exec(compile(source(11), "notebook-cell-11", "exec"), env)
    plotted_mid = env["ax1"].lines[0].get_ydata()
    assert np.isnan(plotted_mid).any(), "Chart bridges all 20 unavailable seconds"


def test_move_chart_has_units_timezone_and_source():
    """Inspect actual axes produced by the notebook with a local fake book."""
    before = pd.Timestamp("2026-10-01T14:00:00Z")
    after = before + pd.Timedelta(minutes=1)
    view = SimpleNamespace(
        bids=pd.DataFrame({"price": [84000, 83999], "qty": [1, 2]}),
        asks=pd.DataFrame({"price": [84001, 84002], "qty": [3, 4]}),
        bid_window_floor=83999, ask_window_ceiling=84002,
    )
    book = SimpleNamespace(
        spread_series=lambda *args: pd.DataFrame({"mid": [84000.5, 83900.5]}, index=[before, after]),
        book_at=lambda *args, **kwargs: view,
    )
    env = dict(pd=pd, plt=plt, book=book, t_before=before, t_after=after,
               SYMBOL="BTCUSDT", DAY="2026-10-01")
    exec(compile(source(17), "notebook-cell-17", "exec"), env)
    fig, axes = env["fig"], env["axes"]
    labels = " ".join([text.get_text() for text in fig.texts] +
                      [ax.get_title() + ax.get_xlabel() + ax.get_ylabel() for ax in axes])
    missing = []
    if "USDT" not in axes[0].get_ylabel():
        missing.append("mid price unit (USDT)")
    if "UTC" not in labels:
        missing.append("UTC time zone")
    if "CandleFeed" not in labels:
        missing.append("CandleFeed source")
    assert not missing, "Missing chart labels: " + ", ".join(missing)


def test_unavailability_note_acknowledges_pre_anchor_seconds():
    """Saved first-event time proves midnight is missing before any edge exhaustion."""
    output = "".join(NOTEBOOK["cells"][7]["outputs"][0]["text"])
    first = pd.Timestamp(re.search(r"first event (\S+)", output).group(1))
    book = object.__new__(L2Book)
    book._t0 = int(first.normalize().timestamp() * 1000)
    book._t1 = book._t0 + 86400000
    book._Ecum = np.array([int(first.timestamp() * 1000)], dtype=np.int64)
    event, anchor, reason = book._locate(book._t0)
    assert event is None and anchor is None and "before the first diff" in reason
    note = source(10) + " " + source(18)
    assert re.search(r"before (?:the )?first|pre[- ]anchor|initial anchor|before.*snapshot", note, re.I), (
        "The 6.4% includes pre-anchor seconds, but the limitation attributes all of it to snapshot edges"
    )
