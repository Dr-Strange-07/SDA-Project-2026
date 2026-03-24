from collections import deque
from multiprocessing import Queue
from queue import Empty
from typing import Deque, Dict

from .functional import update_running_average


class ResequencingAggregator:
    """
    Gather node: receives verified packets from all workers, reorders by sequence_id,
    computes sliding running average, and emits processed packets.
    """

    def __init__(
        self,
        intermediate_queue: Queue,
        processed_queue: Queue,
        worker_count: int,
        window_size: int,
        entity_field: str,
        time_field: str,
        value_field: str,
        computed_field: str,
    ) -> None:
        self.intermediate_queue = intermediate_queue
        self.processed_queue = processed_queue
        self.worker_count = worker_count
        self.window_size = window_size
        self.entity_field = entity_field
        self.time_field = time_field
        self.value_field = value_field
        self.computed_field = computed_field

    def run(self) -> None:
        expected_sequence = 0
        pending_packets: Dict[int, Dict] = {}
        running_window: Deque[float] = deque(maxlen=self.window_size)
        done_workers = 0

        def flush_available_from_expected() -> int:
            nonlocal expected_sequence, running_window
            emitted = 0
            while expected_sequence in pending_packets:
                current = pending_packets.pop(expected_sequence)
                running_window, avg = update_running_average(
                    running_window,
                    current[self.value_field],
                    self.window_size,
                )
                processed_packet = dict(current)
                processed_packet[self.value_field] = float(current[self.value_field])
                processed_packet[self.computed_field] = avg
                self.processed_queue.put(processed_packet)
                expected_sequence += 1
                emitted += 1
            return emitted

        while True:
            flush_available_from_expected()

            if done_workers == self.worker_count and expected_sequence not in pending_packets:
                if not pending_packets:
                    break
                expected_sequence = min(pending_packets.keys())
                flush_available_from_expected()
                if not pending_packets:
                    break

            try:
                event = self.intermediate_queue.get(timeout=1)
            except Empty:
                continue

            event_type = event.get("type")

            if event_type == "worker_done":
                done_workers += 1
                continue

            if event_type != "verified_packet":
                continue

            packet = event["packet"]
            seq_id = packet["sequence_id"]
            pending_packets[seq_id] = packet
            flush_available_from_expected()

        self.processed_queue.put(None)
