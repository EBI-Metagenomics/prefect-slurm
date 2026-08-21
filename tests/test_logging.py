"""
Unit tests for prefect_slurm.log_filters (mask_sensitive_data and RedactingFilter).
"""

import logging

from prefect_slurm.log_filters import RedactingFilter, mask_sensitive_data


def test_mask_sensitive_data():
    value = {
        "environment": ["PASSWORD=secret"],
        "message": "token=literal-secret",
    }

    assert mask_sensitive_data(
        value, {"PASSWORD": "secret"}, ("PASSWORD", "literal-secret")
    ) == {
        "environment": ["PASSWORD=********"],
        "message": "token=********",
    }


def test_redacting_filter_masks_msg(monkeypatch):
    monkeypatch.setenv("PREFECT_API_KEY", "super-secret-key")

    record = logging.LogRecord(
        name="test",
        level=logging.DEBUG,
        pathname=__file__,
        lineno=0,
        msg="token=super-secret-key",
        args=None,
        exc_info=None,
    )

    assert RedactingFilter().filter(record) is True
    assert "super-secret-key" not in record.msg
    assert "********" in record.msg


def test_redacting_filter_masks_args(monkeypatch):
    monkeypatch.setenv("PREFECT_API_AUTH_STRING", "prefect-admin:super-secret-token")

    record = logging.LogRecord(
        name="test",
        level=logging.WARNING,
        pathname=__file__,
        lineno=0,
        msg="Could not reach %s using %s",
        args=("server", "prefect-admin:super-secret-token"),
        exc_info=None,
    )

    assert RedactingFilter().filter(record) is True
    assert "super-secret-token" not in record.args[1]
    assert "********" in record.args[1]


def test_redacting_filter_is_noop_without_matching_env(monkeypatch):
    monkeypatch.delenv("PREFECT_API_KEY", raising=False)
    monkeypatch.delenv("PREFECT_API_AUTH_STRING", raising=False)

    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=0,
        msg="nothing sensitive here",
        args=None,
        exc_info=None,
    )

    assert RedactingFilter().filter(record) is True
    assert record.msg == "nothing sensitive here"
