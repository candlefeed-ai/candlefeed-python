"""Synthetic canonical L2 days with a known true book after every event.

Writes the published layout (depth/HH.parquet, snapshot.parquet, manifest.json) with the published
schema, so the rebuild code reads exactly what download_l2 leaves on disk. Update ids skip like a
real futures stream (U is not always pu + 1), so snapshots can land on a boundary, between events,
inside an event (straddle) or inside a dropped stretch (in_gap).
"""
from __future__ import annotations

import bisect
import hashlib
import json
import random
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

DAY_MS = 86_400_000
SCALE = 10 ** 12
DEC = pa.decimal128(38, 12)
TICK = Decimal("0.1")

DEPTH_NAMES = ["node", "symbol", "event_time", "tx_time", "first_update_id", "final_update_id",
               "prev_final_update_id", "side", "price", "qty", "is_snapshot", "recv_time", "segment"]
DEPTH_TYPES = [pa.string(), pa.string(), pa.int64(), pa.int64(), pa.int64(), pa.int64(), pa.int64(),
               pa.string(), DEC, DEC, pa.bool_(), pa.int64(), pa.int32()]
SNAP_NAMES = DEPTH_NAMES[:-1] + ["anchor_class"]
SNAP_TYPES = DEPTH_TYPES[:-1] + [pa.string()]


def day_ms(day: str) -> int:
    return (date.fromisoformat(day) - date(1970, 1, 1)).days * DAY_MS


def classify(L, u, U, pu):
    """reconcile_l2.classify_snapshot, restated so the generator labels anchor_class on its own."""
    j = bisect.bisect_left(u, L)
    if j == len(u):
        return "in_gap"
    if u[j] == L:
        return "clean"
    if U[j] <= L:
        return "straddle"
    if 0 <= pu[j] <= L:
        return "clean"
    return "in_gap"


def ticks(p: int) -> int:
    """Price in 0.1 units -> unscaled decimal128(38,12) integer."""
    return int(Decimal(p) * TICK * SCALE)


def as_ticks(book: Dict[int, Decimal]) -> Dict[int, float]:
    return {ticks(p): float(q) for p, q in book.items()}


@dataclass
class Truth:
    u: List[int] = field(default_factory=list)
    E: List[int] = field(default_factory=list)
    bids: List[Dict[int, float]] = field(default_factory=list)
    asks: List[Dict[int, float]] = field(default_factory=list)
    snapshots: List[dict] = field(default_factory=list)


class Market:
    """A two-sided book on a 0.1 grid that drifts. Keys are prices in 0.1 units, values Decimal qty."""

    def __init__(self, rng: random.Random, mid: int = 1_000_000, levels: int = 60) -> None:
        self.rng = rng
        self.mid = mid
        self.bids: Dict[int, Decimal] = {}
        self.asks: Dict[int, Decimal] = {}
        for k in range(1, levels + 1):
            self.bids[mid - k] = self._qty()
            self.asks[mid + k] = self._qty()
        self.last_u = 10_000

    def _qty(self) -> Decimal:
        return Decimal(self.rng.randint(1, 50_000)) / Decimal(1000)

    def event(self, move: int = 0):
        rows: Dict[Tuple[str, int], Decimal] = {}
        for _ in range(abs(move)):
            if move > 0:
                self.mid += 1
                rows[("ask", self.mid)] = Decimal(0)
                rows[("bid", self.mid - 1)] = self._qty()
            else:
                self.mid -= 1
                rows[("bid", self.mid)] = Decimal(0)
                rows[("ask", self.mid + 1)] = self._qty()
        for _ in range(self.rng.randint(1, 6)):
            side = self.rng.choice(("bid", "ask"))
            off = int(abs(self.rng.gauss(0, 12))) + 1
            p = self.mid - off if side == "bid" else self.mid + off
            if ("bid", p) in rows or ("ask", p) in rows:
                continue
            book = self.bids if side == "bid" else self.asks
            delete = self.rng.random() < 0.25 and p in book and len(book) > 20
            rows[(side, p)] = Decimal(0) if delete else self._qty()
        out = [(s, p, q) for (s, p), q in rows.items()]
        self.rng.shuffle(out)
        return out

    def apply(self, rows) -> None:
        for side, p, q in rows:
            book = self.bids if side == "bid" else self.asks
            if q == 0:
                book.pop(p, None)
            else:
                book[p] = q

    def state(self, depth: Optional[int] = None):
        bids = sorted(self.bids.items(), reverse=True)
        asks = sorted(self.asks.items())
        if depth is not None:
            bids, asks = bids[:depth], asks[:depth]
        return bids, asks


def write_day(root: Path, day: str, symbol: str, written, snaps, extra_node_every: int = 0) -> Path:
    """written: [(E, U, u, pu, [(side, price in 0.1 units, Decimal qty), ...])] in update-id order.
    snaps: [(node, L, T, recv_ns, bids [(price, qty)], asks [(price, qty)])]."""
    t0 = day_ms(day)
    ddir = Path(root) / "book" / "binance" / symbol / day
    (ddir / "depth").mkdir(parents=True, exist_ok=True)
    us = [w[2] for w in written]
    Us = [w[1] for w in written]
    pus = [w[3] for w in written]
    seg, segs, prev_u = 0, [], None
    for (_, _, u, pu, _) in written:
        if prev_u is not None and pu != prev_u:
            seg += 1
        segs.append(seg)
        prev_u = u
    by_hour: Dict[int, list] = {}
    for (E, U, u, pu, rows), s in zip(written, segs):
        for side, p, q in rows:
            by_hour.setdefault((E - t0) // 3_600_000, []).append(
                ("tokyo", symbol, E, E - 1, U, u, pu, side, Decimal(p) * TICK, q, False, E * 1_000_000 + 5, s))
    for hh, rows in by_hour.items():
        cols = list(zip(*rows))
        pq.write_table(pa.table([pa.array(c, type=t) for c, t in zip(cols, DEPTH_TYPES)], names=DEPTH_NAMES),
                       ddir / "depth" / f"{hh:02d}.parquet")
    snap_rows = []
    for i, (node, L, T, recv, b, a) in enumerate(snaps):
        cls = classify(L, us, Us, pus)
        for side, levels in (("bid", b), ("ask", a)):
            for p, q in levels:
                snap_rows.append((node, symbol, T, T, L, L, 0, side, Decimal(p) * TICK, q, True, recv, cls))
        # a second node's copy, received later and deliberately wrong: the rebuild must use the
        # first-received copy, as the published worked example does
        if extra_node_every and i % extra_node_every == 3:
            for p, q in b:
                snap_rows.append(("singapore", symbol, T, T, L, L, 0, "bid", Decimal(p) * TICK, q + 7, True,
                                  recv + 50_000_000, cls))
    cols = list(zip(*snap_rows)) if snap_rows else [[] for _ in SNAP_NAMES]
    pq.write_table(pa.table([pa.array(c, type=t) for c, t in zip(cols, SNAP_TYPES)], names=SNAP_NAMES),
                   ddir / "snapshot.parquet")
    write_manifest(ddir, day, symbol, written, segs)
    return ddir


def write_manifest(ddir: Path, day: str, symbol: str, written, segs) -> dict:
    """The fields of the published manifest that the rebuild checks: outputs (key, rows, bytes, sha256),
    canonical counts and the gaps."""
    outputs = []
    for f in sorted(p for p in ddir.rglob("*.parquet")):
        name = str(f.relative_to(ddir))
        body = f.read_bytes()
        outputs.append({"key": f"canonical/book/binance/{symbol}/{day}/gen=synthetic/{name}",
                        "rows": pq.ParquetFile(f).metadata.num_rows, "bytes": len(body),
                        "sha256": hashlib.sha256(body).hexdigest()})
    gaps = [{"kind": "pu_reset" if w[3] == -1 else "pu_jump", "start_ms": prev[0], "end_ms": w[0]}
            for prev, w, s0, s1 in zip(written, written[1:], segs, segs[1:]) if s1 != s0]
    manifest = {"dataset": "book", "exchange": "binance", "symbol": symbol, "date": day, "generation": "synthetic",
                "canonical": {"events": len(written), "segments": (segs[-1] + 1) if segs else 0,
                              "first_event_ms": written[0][0] if written else None,
                              "last_event_ms": written[-1][0] if written else None},
                "gaps": gaps, "outputs": outputs}
    (ddir / "manifest.json").write_text(json.dumps(manifest))
    return manifest


def make_day(root: Path, day: str = "2026-09-01", symbol: str = "BTCUSDT", *, seed: int = 1,
             n_events: int = 2400, snapshot_every: int = 40, snapshot_depth: Optional[int] = None,
             drops: Sequence[Tuple[int, int]] = (), resets: Sequence[int] = (), first_event_ms: int = 0,
             moves: Optional[Dict[int, int]] = None, market: Optional[Market] = None,
             extra_node_every: int = 7, last_event_ms: Optional[int] = None):
    """One day of events spread over the 24 hours. drops=[(k, n)] leaves the n events after event k
    out of the files (the truth still applies them), which breaks the chain; one snapshot is taken
    inside the dropped stretch (in_gap). resets=[k] writes pu=-1 on event k. Returns
    (day folder, truth, market); pass the market to the next day to continue the chain."""
    rng = random.Random(seed)
    m = market or Market(rng)
    m.rng = rng
    moves = moves or {}
    t0 = day_ms(day)
    span = (last_event_ms if last_event_ms is not None else DAY_MS - 1000) - first_event_ms
    drop_at = dict(drops)
    truth = Truth()
    written, snaps = [], []
    skip = 0
    for k in range(n_events):
        E = t0 + first_event_ms + span * k // n_events
        rows = m.event(moves.get(k, 0))
        U = m.last_u + 1 + rng.choice((0, 0, 0, 2, 5))
        u = U + len(rows) - 1
        pu = -1 if k in resets else m.last_u
        kind = None
        if skip == 0 and k % snapshot_every == snapshot_every // 2:
            kinds = ["exact"] + (["straddle", "straddle"] if len(rows) >= 2 else []) \
                + (["between"] if U > m.last_u + 1 else [])
            kind = rng.choice(kinds)
        if kind == "between":
            b, a = m.state(snapshot_depth)
            snaps.append(("tokyo", U - 1, E, E * 1_000_000 + 1, b, a))
        if kind == "straddle":
            cut = rng.randint(0, len(rows) - 2)       # rows[i] carries update id U + i
            m.apply(rows[:cut + 1])
            b, a = m.state(snapshot_depth)
            snaps.append(("tokyo", U + cut, E, E * 1_000_000 + 1, b, a))
            m.apply(rows[cut + 1:])
        else:
            m.apply(rows)
        m.last_u = u
        if kind == "exact":
            b, a = m.state(snapshot_depth)
            snaps.append(("tokyo", u, E, E * 1_000_000 + 2, b, a))
        if skip > 0:
            skip -= 1
            if skip == 1:
                b, a = m.state(snapshot_depth)
                snaps.append(("tokyo", u, E, E * 1_000_000 + 3, b, a))
        else:
            written.append((E, U, u, pu, rows))
            truth.u.append(u)
            truth.E.append(E)
            truth.bids.append(as_ticks(m.bids))
            truth.asks.append(as_ticks(m.asks))
        if k in drop_at:
            skip = drop_at[k]
    ddir = write_day(root, day, symbol, written, snaps, extra_node_every)
    us, Us, pus = [w[2] for w in written], [w[1] for w in written], [w[3] for w in written]
    truth.snapshots = [{"L": s[1], "class": classify(s[1], us, Us, pus), "node": s[0]} for s in snaps]
    return ddir, truth, m
