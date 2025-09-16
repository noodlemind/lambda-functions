import json
import time
import random
import uuid
from typing import Dict, List

import boto3
from botocore.config import Config

class SnsLanePublisher:
    """
    Per-lane publisher with simple batching. Preserves order per loan by design (lane serializes work).
    """

    def __init__(self, topic_arn: str, batch_size: int = 10):
        self.topic_arn = topic_arn
        self.batch_size = max(1, min(10, batch_size))
        self.pending: List[Dict] = []
        self.sns = boto3.client(
            "sns",
            config=Config(
                retries={"max_attempts": 3, "mode": "standard"},
                read_timeout=3,
                connect_timeout=1,
                max_pool_connections=512,
            ),
        )

    def send(self, loan: str, event_name: str, payload: Dict, attributes: Dict, seq: int) -> bool:
        msg = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        if len(msg.encode("utf-8")) > 256_000:
            # Too big for SNS; starter code: drop with failure. (Or route to pointer if you enable it)
            return False

        dedup = f"{attributes.get('jobId','JOB')}:{loan}:{event_name}:{seq}:{uuid.uuid4()}"
        msg_attrs = {k: {"DataType": "String", "StringValue": str(v)} for k, v in attributes.items()}
        entry = {
            "Id": str(uuid.uuid4()),
            "Message": msg,
            "MessageGroupId": loan,
            "MessageDeduplicationId": dedup,
            "MessageAttributes": msg_attrs,
        }
        self.pending.append(entry)
        if len(self.pending) >= self.batch_size:
            return self._flush_batch()
        return True

    def _flush_batch(self) -> bool:
        if not self.pending:
            return True
        batch = self.pending[:10]
        ok = False
        attempts = 0
        while True:
            attempts += 1
            try:
                resp = self.sns.publish_batch(TopicArn=self.topic_arn, PublishBatchRequestEntries=batch)
                failed = resp.get("Failed") or []
                if not failed:
                    ok = True
                    break
                # retry failed entries only
                retry_ids = {f["Id"] for f in failed}
                batch = [e for e in batch if e["Id"] in retry_ids]
                if attempts > 3:
                    ok = False
                    break
                time.sleep(min(0.5 * attempts + random.random() * 0.2, 2.0))
            except Exception:
                if attempts > 3:
                    ok = False
                    break
                time.sleep(min(0.5 * attempts + random.random() * 0.2, 2.0))
        # remove flushed entries from pending
        flushed_ids = {e["Id"] for e in self.pending[: len(self.pending) if len(self.pending) <= 10 else 10]}
        self.pending = [e for e in self.pending if e["Id"] not in flushed_ids]
        return ok

    def flush(self):
        while self.pending:
            if not self._flush_batch():
                # give up to avoid infinite loop
                break
