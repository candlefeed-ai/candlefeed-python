"""Offline adversarial checks for PR134's licence logging change."""
import logging

import pytest

from candlefeed import CandleFeed
from candlefeed.client import L2_SAMPLE_LICENSE


@pytest.mark.parametrize("raise_exceptions", [False, True])
def test_sample_download_survives_unavailable_standard_log_file(monkeypatch, tmp_path, raise_exceptions):
    """A delayed FileHandler opens on first INFO; lost log storage must not block data access."""
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    handler = logging.FileHandler(log_dir / "app.log", delay=True)
    log_dir.rmdir()  # e.g. the application's log volume disappeared after configuration
    logger = logging.getLogger("candlefeed")
    previous_level = logger.level
    monkeypatch.setattr(logger, "handlers", [handler])
    logger.setLevel(logging.INFO)
    monkeypatch.setattr(logger, "disabled", False)
    monkeypatch.setattr(logger, "propagate", False)
    monkeypatch.setattr(logging, "raiseExceptions", raise_exceptions)
    called = []

    def download(*args, **kwargs):
        called.append(True)
        return {"downloaded": [], "skipped": [], "missing": [], "bytes": 0}

    monkeypatch.setattr(CandleFeed, "_download_l2", download)
    try:
        with CandleFeed(public=True) as client:
            result = client.download_l2_sample("BTCUSDT", "2026-10-01", tmp_path)
        assert called == [True]
        assert result["bytes"] == 0
    finally:
        logger.setLevel(previous_level)
        handler.close()


def test_sample_notice_logs_only_constant_and_leaves_stdout_clean(monkeypatch, tmp_path, caplog, capsys):
    key = "cf_live_recheck_secret_do_not_log"
    monkeypatch.setattr(CandleFeed, "_download_l2", lambda *a, **kw: {"bytes": 0})
    with caplog.at_level(logging.INFO, logger="candlefeed"):
        with CandleFeed(api_key=key) as client:
            result = client.download_l2_sample("BTCUSDT", "2026-10-01", tmp_path / key)
    records = [r for r in caplog.records if r.name == "candlefeed"]
    assert len(records) == 1
    assert records[0].getMessage() == result["license"] == L2_SAMPLE_LICENSE
    assert records[0].args == () and records[0].exc_info is None
    assert key not in caplog.text
    assert capsys.readouterr().out == ""
    assert any(isinstance(h, logging.NullHandler) for h in logging.getLogger("candlefeed").handlers)
