"""Tests for the Submitter HTTP publisher configuration."""

import sys
import types
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

dummy_urllib3 = types.ModuleType("urllib3")


class _DummyPoolManager:  # pragma: no cover - simple stub
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


class _DummyTimeout:  # pragma: no cover - simple stub
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


dummy_urllib3.PoolManager = _DummyPoolManager
dummy_urllib3.Timeout = _DummyTimeout
sys.modules.setdefault("urllib3", dummy_urllib3)

from lambda_function.publisher_http import SubmitterHttpPublisher  # noqa: E402


@pytest.mark.parametrize(
    "base_url,path,expected",
    [
        ("https://internal.example.com/service", "sendMessage", "https://internal.example.com/service/sendMessage"),
        ("https://internal.example.com/service", "/sendMessage", "https://internal.example.com/service/sendMessage"),
        ("https://internal.example.com/service/", "sendMessage", "https://internal.example.com/service/sendMessage"),
        ("https://internal.example.com/service/", "/sendMessage", "https://internal.example.com/service/sendMessage"),
    ],
)
def test_submitter_http_publisher_normalizes_path_variants(base_url, path, expected):
    publisher = SubmitterHttpPublisher(base_url, path)

    assert publisher.url == expected
    assert publisher.url.endswith("/sendMessage")
