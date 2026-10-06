"""PR134: a library download must leave the caller's machine-readable stdout intact."""
import json

from candlefeed import CandleFeed


def test_sample_download_preserves_json_stdout(monkeypatch, capsys, tmp_path):
    """A JSON-emitting CLI/agent wrapper previously produced one valid JSON document."""
    result = {"downloaded": [], "skipped": [], "missing": [], "bytes": 0}
    monkeypatch.setattr(CandleFeed, "_download_l2", lambda *args, **kwargs: result)
    with CandleFeed(public=True) as client:
        downloaded = client.download_l2_sample("BTCUSDT", "2026-10-01", tmp_path)
    print(json.dumps(downloaded))
    stdout = capsys.readouterr().out
    assert json.loads(stdout) == result
