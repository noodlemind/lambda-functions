import threading
import queue
import time
from typing import Callable, Dict, Tuple

_SENTINEL = object()

class LaneWorker(threading.Thread):
    def __init__(self, lane_id: int, publisher_factory: Callable[[int], "BaseLanePublisher"]):
        super().__init__(daemon=True, name=f"lane-{lane_id}")
        self.lane_id = lane_id
        self.pub = publisher_factory(lane_id)
        self.q: "queue.Queue[dict|object]" = queue.Queue(maxsize=10000)
        self.processed = 0
        self.failed = 0
        self._should_stop = False

    def submit(self, item: dict) -> None:
        self.q.put(item)

    def run(self) -> None:
        while not self._should_stop:
            item = self.q.get()
            if item is _SENTINEL:
                break
            try:
                ok = self.pub.send(
                    loan=item["loan"],
                    event_name=item["event_name"],
                    payload=item["payload"],
                    attributes=item.get("attributes") or {},
                    seq=item.get("seq") or 0,
                )
                if ok:
                    self.processed += 1
                else:
                    self.failed += 1
            except Exception:
                self.failed += 1

        # flush publisher (e.g., SNS batch leftovers)
        try:
            self.pub.flush()
        except Exception:
            pass

    def close(self):
        self.q.put(_SENTINEL)

    def force_close(self):
        self._should_stop = True
        try:
            while True:
                self.q.get_nowait()
        except queue.Empty:
            pass
        try:
            self.pub.flush()
        except Exception:
            pass


class LaneMux:
    def __init__(self, lane_count: int, max_workers: int, worker_factory: Callable[[int], "BaseLanePublisher"]):
        self.lanes = [LaneWorker(i, worker_factory) for i in range(lane_count)]
        # Start up to max_workers threads; if lane_count > max_workers, still start all lanes (cheap threads)
        for w in self.lanes:
            w.start()

    def submit(self, lane_id: int, item: dict) -> None:
        self.lanes[lane_id].submit(item)

    def drain_and_close(self, deadline_epoch: float) -> Tuple[int, int]:
        for w in self.lanes:
            w.close()
        for w in self.lanes:
            timeout = max(0.0, deadline_epoch - time.time())
            w.join(timeout=timeout)
        processed = sum(w.processed for w in self.lanes)
        failed = sum(w.failed for w in self.lanes)
        return processed, failed

    def force_close(self):
        for w in self.lanes:
            w.force_close()
            try:
                w.join(timeout=0.1)
            except Exception:
                pass
