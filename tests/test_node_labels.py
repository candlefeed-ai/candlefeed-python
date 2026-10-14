"""Capture-node labels renamed after days were published: views show the current label, files stay as published."""
import contextlib
import io
import json
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from l2_synth import day_ms, write_day

from candlefeed.l2book import NODE_RENAMES, L2Book, public_node

ROOT = Path(__file__).resolve().parents[3]
OLD = next(old for old, new in NODE_RENAMES.items() if new == "tokyo-2")


def legacy_day(tmp_path):
    day = "2026-09-01"
    t, q = day_ms(day), Decimal("1")
    written = [(t, 100, 100, 99, [("bid", 1000, q), ("ask", 1010, q)]),
               (t + 60_000, 101, 101, 100, [("bid", 1000, q)])]
    snaps = [(OLD, 100, t, t * 1_000_000, [(1000, q)], [(1010, q)])]
    return write_day(tmp_path, day, "BTCUSDT", written, snaps)


def test_public_node_maps_only_renamed_labels():
    assert public_node(OLD) == "tokyo-2"
    assert [public_node(n) for n in ("tokyo", "singapore", "tokyo-2")] == ["tokyo", "singapore", "tokyo-2"]


def test_book_view_shows_the_current_label_for_a_day_published_before_the_rename(tmp_path):
    ddir = legacy_day(tmp_path)
    view = L2Book([ddir]).book_at("2026-09-01T00:00:00Z")
    assert view.anchor_node == "tokyo-2" and view.to_dict()["anchor"]["node"] == "tokyo-2"
    assert OLD not in json.dumps(view.to_dict())
    assert pq.read_table(ddir / "snapshot.parquet")["node"].to_pylist()[0] == OLD   # the file isn't rewritten


@pytest.mark.parametrize("path", ["clients/python/examples/l2_free_sample_day.ipynb",
                                  "website/public/examples/l2_free_sample_day.ipynb"])
def test_rerun_notebook_prints_current_labels_for_a_manifest_published_before_the_rename(tmp_path, path):
    manifest = {"canonical": {"events": 1, "rows": 2, "snapshots_kept": 1,
                              "first_event": "2026-10-01T00:00:00Z", "last_event": "2026-10-01T23:59:59Z",
                              "coverage_pct": 100, "segments": 1},
                "gaps": [], "node_priority": ["tokyo", OLD, "singapore"], "base_node": OLD}
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    notebook = json.loads((ROOT / path).read_text())
    cell, = [c for c in notebook["cells"] if c["cell_type"] == "code"
             and "capture nodes:" in "".join(c["source"]) and "node_priority" in "".join(c["source"])]
    stdout = io.StringIO()
    with contextlib.redirect_stdout(stdout):
        exec("".join(cell["source"]), {"json": json, "day_dir": tmp_path})
    assert "capture nodes: tokyo, tokyo-2, singapore (base: tokyo-2)" in stdout.getvalue()
    assert OLD not in stdout.getvalue()
    assert json.loads((tmp_path / "manifest.json").read_text()) == manifest


# The version that first shipped each candlefeed name the sample notebook imports. When the notebook imports
# something new, add it here; the notebook's pip line must then require at least that version, because pip keeps an
# older installed client that still satisfies the line.
NOTEBOOK_SDK_NAMES = {
    ("candlefeed", "CandleFeed"): "0.3.1",
    ("candlefeed", "TierRestrictedError"): "0.3.1",
    ("candlefeed.l2book", "L2Book"): "0.3.1",
    ("candlefeed.l2book", "public_node"): "0.3.6",
}


@pytest.mark.parametrize("path", ["clients/python/examples/l2_free_sample_day.ipynb",
                                  "website/public/examples/l2_free_sample_day.ipynb"])
def test_notebook_pip_line_requires_a_client_with_every_name_it_imports(path):
    import ast
    import importlib
    import importlib.metadata
    import re

    from packaging.requirements import Requirement
    from packaging.version import Version

    cells = json.loads((ROOT / path).read_text())["cells"]
    code = "\n".join("".join(c["source"]) for c in cells if c["cell_type"] == "code")
    req = Requirement(re.search(r'pip install "([^"]+)"', code).group(1))
    assert req.name == "candlefeed" and "l2" in req.extras
    floor = Version(next(s.version for s in req.specifier if s.operator == ">="))
    tree = ast.parse("\n".join(line for line in code.splitlines() if not line.lstrip().startswith(("%", "!"))))
    imported = {(n.module, a.name) for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)
                and (n.module or "").split(".")[0] == "candlefeed" for a in n.names}
    assert imported, "the notebook should import from candlefeed"
    assert imported <= NOTEBOOK_SDK_NAMES.keys(), f"record the version that added {imported - NOTEBOOK_SDK_NAMES.keys()}"
    assert floor >= max(Version(NOTEBOOK_SDK_NAMES[k]) for k in imported), req
    for module, name in imported:
        assert hasattr(importlib.import_module(module), name), (module, name)
    assert importlib.metadata.version("candlefeed") in req.specifier
