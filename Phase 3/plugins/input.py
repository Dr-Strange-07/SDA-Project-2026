import csv
import json
import os
import time
from multiprocessing import Queue
from typing import Dict, List


def _cast_value(value: str, target_type: str):
    if target_type == "string":
        return str(value).strip()
    if target_type == "integer":
        return int(float(value))
    if target_type == "float":
        return float(value)
    raise ValueError(f"Unsupported data_type in schema mapping: {target_type}")


class GenericInputProducer:
    """
    Domain-agnostic input module:
    - reads unseen dataset
    - maps source columns -> internal generic names
    - casts primitive types from config schema
    - pushes packets into bounded raw stream queue
    """

    def __init__(
        self,
        dataset_path: str,
        schema_columns: List[Dict],
        raw_queue: Queue,
        worker_count: int,
        input_delay_seconds: float,
    ) -> None:
        self.dataset_path = dataset_path
        self.schema_columns = schema_columns
        self.raw_queue = raw_queue
        self.worker_count = worker_count
        self.input_delay_seconds = input_delay_seconds

    def _read_rows(self) -> List[Dict]:
        ext = os.path.splitext(self.dataset_path.lower())[1]
        if ext == ".json":
            with open(self.dataset_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, list):
                raise ValueError("JSON dataset must be a list of objects.")
            return data

        with open(self.dataset_path, "r", encoding="utf-8-sig", newline="") as f:
            sample = f.read(4096)
            f.seek(0)

            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=[",", "\t", ";", "|"])
                reader = csv.DictReader(f, dialect=dialect)
            except csv.Error:
                reader = csv.DictReader(f)

            return list(reader)

    def run(self) -> None:
        rows = self._read_rows()

        for sequence_id, row in enumerate(rows):
            packet = {"sequence_id": sequence_id}
            for col in self.schema_columns:
                source_name = col["source_name"]
                internal_mapping = col["internal_mapping"]
                data_type = col["data_type"]

                if source_name not in row:
                    raise KeyError(f"Missing source column '{source_name}' in dataset")

                packet[internal_mapping] = _cast_value(row[source_name], data_type)

            self.raw_queue.put(packet)
            time.sleep(self.input_delay_seconds)

        for _ in range(self.worker_count):
            self.raw_queue.put(None)
