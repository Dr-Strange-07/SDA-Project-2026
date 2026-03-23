import threading
import time
from multiprocessing import Queue
from typing import Dict, List

from core.contracts import TelemetryObserver


class PipelineTelemetry:
    """
    Subject in Observer pattern.
    Polls queue sizes independently and notifies all observers.
    """

    def __init__(
        self,
        raw_queue: Queue,
        intermediate_queue: Queue,
        processed_queue: Queue,
        queue_max_size: int,
    ) -> None:
        self.raw_queue = raw_queue
        self.intermediate_queue = intermediate_queue
        self.processed_queue = processed_queue
        self.queue_max_size = queue_max_size
        self.observers: List[TelemetryObserver] = []
        self._running = False
        self._thread = None

    def subscribe(self, observer: TelemetryObserver) -> None:
        self.observers.append(observer)

    def unsubscribe(self, observer: TelemetryObserver) -> None:
        self.observers = [obs for obs in self.observers if obs != observer]

    def _snapshot(self) -> Dict:
        raw_size = max(0, self.raw_queue.qsize())
        intermediate_size = max(0, self.intermediate_queue.qsize())
        processed_size = max(0, self.processed_queue.qsize())

        return {
            "raw_stream": self._build_metric(raw_size),
            "intermediate_stream": self._build_metric(intermediate_size),
            "processed_stream": self._build_metric(processed_size),
        }

    def _build_metric(self, size: int) -> Dict:
        ratio = (size / self.queue_max_size) if self.queue_max_size > 0 else 0
        if ratio < 0.5:
            level = "green"
        elif ratio < 0.8:
            level = "yellow"
        else:
            level = "red"
        return {"size": size, "ratio": ratio, "level": level}

    def _run(self, poll_seconds: float) -> None:
        while self._running:
            snap = self._snapshot()
            for observer in self.observers:
                observer.on_telemetry(snap)
            time.sleep(poll_seconds)

    def start(self, poll_seconds: float = 0.25) -> None:
        self._running = True
        self._thread = threading.Thread(target=self._run, args=(poll_seconds,), daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=2)
