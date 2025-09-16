from lambda_function import handler, util


class DummyContext:
    def get_remaining_time_in_millis(self):
        return 100000


class DummyLaneMux:
    instances = []

    def __init__(self, lane_count, max_workers, worker_factory):
        self.submitted = []
        DummyLaneMux.instances.append(self)

    def submit(self, lane_id, item):
        self.submitted.append((lane_id, item))

    def drain_and_close(self, deadline_epoch):
        return len(self.submitted), 0

    def force_close(self):
        pass


def test_lambda_handler_template_clone(monkeypatch):
    DummyLaneMux.instances = []
    monkeypatch.setattr(handler, "LaneMux", DummyLaneMux)
    event = {
        "mode": "TEMPLATE_CLONE",
        "job_id": "JOB-1234",
        "http": {"base_url": "http://example.com"},
        "publish": {"lane_count": 2},
        "template_clone": {
            "count": 2,
            "event_name": "SampleEvent",
            "template_inline": {},
        },
    }
    result = handler.lambda_handler(event, DummyContext())
    assert result["processed"] == 2
    instance = DummyLaneMux.instances[0]
    assert len(instance.submitted) == 2
    loans = [item[1]["loan"] for item in instance.submitted]
    expected = [
        util.generate_loan_number("", i, "JOB-1234") for i in range(2)
    ]
    assert loans == expected
    for _, item in instance.submitted:
        assert item["attributes"]["eventName"] == "SampleEvent"
