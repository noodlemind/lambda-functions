"""Tests for ALB-wrapped Lambda invocations."""

import base64
import json
import sys
import types
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

dummy_urllib3 = types.ModuleType("urllib3")


class _DummyPoolManager:  # pragma: no cover - simple stub
    def __init__(self, *args, **kwargs):
        pass

    def request(self, method, url, body=None, headers=None):
        class _Response:
            status = 200

        return _Response()


class _DummyTimeout:  # pragma: no cover - simple stub
    def __init__(self, *args, **kwargs):
        pass


dummy_urllib3.PoolManager = _DummyPoolManager
dummy_urllib3.Timeout = _DummyTimeout
sys.modules.setdefault("urllib3", dummy_urllib3)

dummy_boto3 = types.ModuleType("boto3")


class _DummySNSClient:  # pragma: no cover - simple stub
    def publish_batch(self, *args, **kwargs):
        return {"Failed": []}


def _dummy_boto3_client(*args, **kwargs):
    return _DummySNSClient()


dummy_boto3.client = _dummy_boto3_client
sys.modules.setdefault("boto3", dummy_boto3)

dummy_botocore = types.ModuleType("botocore")
dummy_botocore_config = types.ModuleType("botocore.config")


class _DummyConfig:  # pragma: no cover - simple stub
    def __init__(self, *args, **kwargs):
        pass


dummy_botocore_config.Config = _DummyConfig
dummy_botocore.config = dummy_botocore_config
sys.modules.setdefault("botocore", dummy_botocore)
sys.modules.setdefault("botocore.config", dummy_botocore_config)

from lambda_function import handler  # noqa: E402 - import after stubbing deps


class DummyContext:
    """Minimal Lambda context stub for time budget calculations."""

    def get_remaining_time_in_millis(self):
        return 900_000


class DummyLaneMux:
    """Records submissions without spinning up worker threads."""

    last_instance = None

    def __init__(self, lane_count, max_workers, worker_factory):
        self.lane_count = lane_count
        self.max_workers = max_workers
        self.worker_factory = worker_factory
        self.submissions = []
        DummyLaneMux.last_instance = self

    def submit(self, lane_id, item):
        self.submissions.append((lane_id, item))

    def drain_and_close(self, deadline_epoch):
        return len(self.submissions), 0

    def force_close(self):
        pass


@pytest.fixture(autouse=True)
def stub_lane_mux(monkeypatch):
    """Replace the real LaneMux with a lightweight recorder for each test."""

    DummyLaneMux.last_instance = None
    monkeypatch.setattr(handler, "LaneMux", DummyLaneMux)
    yield DummyLaneMux
    DummyLaneMux.last_instance = None


def _base_event(job_id: str) -> dict:
    """Base Lambda event used across ALB scenarios."""

    return {
        "job_id": job_id,
        "mode": "TEMPLATE_CLONE",
        "backend": "submitter_http",
        "http": {"base_url": "http://example.com"},
        "publish": {"lane_count": 1, "max_workers": 1, "time_budget_secs": 60},
        "template_clone": {
            "count": 1,
            "seq_start": 0,
            "template_inline": {
                "loanNumber": "1234567890",
                "payload": {"hello": "world"},
            },
            "event_name": "InlineEvent",
        },
    }


def _alb_event(payload: dict, *, encode_base64: bool) -> dict:
    body_bytes = json.dumps(payload).encode("utf-8")
    if encode_base64:
        body = base64.b64encode(body_bytes).decode("ascii")
    else:
        body = body_bytes.decode("utf-8")

    return {
        "requestContext": {"elb": {"targetGroupArn": "arn"}},
        "body": body,
        "isBase64Encoded": encode_base64,
    }


def _assert_submission(job_id: str, mux_cls: type[DummyLaneMux]):
    mux = mux_cls.last_instance
    assert mux is not None
    assert mux.lane_count == 1
    assert mux.max_workers == 1
    assert len(mux.submissions) == 1
    lane_id, item = mux.submissions[0]
    assert lane_id == 0
    assert item["attributes"]["jobId"] == job_id
    assert item["event_name"] == "InlineEvent"


def test_lambda_handler_decodes_base64_alb_event(stub_lane_mux):
    event = _alb_event(_base_event("ALBJOB1"), encode_base64=True)
    response = handler.lambda_handler(event, DummyContext())

    assert response["statusCode"] == 200
    assert response["headers"] == {"Content-Type": "application/json"}
    assert response["isBase64Encoded"] is False

    payload = json.loads(response["body"])
    assert payload["processed"] == 1
    assert payload["failed"] == 0

    _assert_submission("ALBJOB1", stub_lane_mux)


def test_lambda_handler_handles_plain_alb_event(stub_lane_mux):
    event = _alb_event(_base_event("ALBJOB2"), encode_base64=False)
    response = handler.lambda_handler(event, DummyContext())

    assert response["statusCode"] == 200
    payload = json.loads(response["body"])
    assert payload["processed"] == 1
    assert payload["failed"] == 0

    _assert_submission("ALBJOB2", stub_lane_mux)
