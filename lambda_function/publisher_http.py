import json
import time
import random
from typing import Dict
import urllib3

class SubmitterHttpPublisher:
    """
    Sequential per-lane sender to /sendMessage.
    Body: { "loanNumber": "<10 digits>", "eventName": "<derived>", "payload": {...} }
    """

    def __init__(self, base_url: str, path: str = "/sendMessage", max_pool: int = 256, timeout_s: float = 3.0):
        normalized_base = base_url.rstrip("/")
        normalized_path = path.lstrip("/") if path is not None else ""
        if normalized_path:
            self.url = f"{normalized_base}/{normalized_path}"
        else:
            self.url = normalized_base
        self.pool = urllib3.PoolManager(
            num_pools=max_pool, maxsize=max_pool, timeout=urllib3.Timeout(total=timeout_s, connect=1.0, read=timeout_s), retries=False
        )

    def send(self, loan: str, event_name: str, payload: Dict, attributes: Dict, seq: int) -> bool:
        # Merge attributes into payload or top-level? Requirement: endpoint takes loanNumber, eventName, payload.
        body = {"loanNumber": loan, "eventName": event_name, "payload": payload}
        data = json.dumps(body).encode("utf-8")

        # Retry on 5xx/429/timeouts up to 3x
        attempts = 0
        while True:
            attempts += 1
            try:
                resp = self.pool.request("POST", self.url, body=data, headers={"Content-Type": "application/json"})
                status = resp.status
                if 200 <= status < 300:
                    return True
                if status in (429, 500, 502, 503, 504):
                    if attempts <= 3:
                        time.sleep(min(0.5 * attempts + random.random() * 0.2, 2.0))
                        continue
                return False
            except Exception:
                if attempts <= 3:
                    time.sleep(min(0.5 * attempts + random.random() * 0.2, 2.0))
                    continue
                return False

    def flush(self):
        return
