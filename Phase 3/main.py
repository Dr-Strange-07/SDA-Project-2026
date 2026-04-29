import json
import multiprocessing as mp
import os
from typing import Dict

from core.aggregator import ResequencingAggregator
from core.workers import StatelessVerifierWorker
from monitoring.telemetry import PipelineTelemetry
from plugins.input import GenericInputProducer
from plugins.output import RealTimeDashboard


def _input_entry(input_component: GenericInputProducer) -> None:
    input_component.run()


def _worker_entry(worker: StatelessVerifierWorker) -> None:
    worker.run()


def _aggregator_entry(aggregator: ResequencingAggregator) -> None:
    aggregator.run()


def _dashboard_entry(
    processed_queue,
    raw_queue,
    intermediate_queue,
    queue_max_size: int,
    output_dir: str,
    visuals_config: Dict,
    entity_field: str,
    time_field: str,
    value_field: str,
    computed_field: str,
) -> None:
    dashboard = RealTimeDashboard(
        processed_queue=processed_queue,
        output_dir=output_dir,
        visuals_config=visuals_config,
        entity_field=entity_field,
        time_field=time_field,
        value_field=value_field,
        computed_field=computed_field,
    )

    telemetry = PipelineTelemetry(
        raw_queue=raw_queue,
        intermediate_queue=intermediate_queue,
        processed_queue=processed_queue,
        queue_max_size=queue_max_size,
    )
    telemetry.subscribe(dashboard)
    telemetry.start(poll_seconds=0.2)

    dashboard.run()

    telemetry.stop()


def load_config(config_path: str) -> Dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _resolve_internal_fields(schema_columns, verify_conf: Dict, stateful_conf: Dict) -> Dict[str, str]:
    mapped_fields = [c.get("internal_mapping") for c in schema_columns if c.get("internal_mapping")]

    def first_with(tokens, fallback=""):
        for field in mapped_fields:
            lowered = field.lower()
            if any(token in lowered for token in tokens):
                return field
        return fallback

    entity_field = first_with(["entity", "name"], "entity_name")
    time_field = first_with(["time", "timestamp", "period"], "time_period")
    value_field = (
        verify_conf.get("value_field")
        or stateful_conf.get("value_field")
        or first_with(["metric", "value", "amount"], "metric_value")
    )
    signature_field = verify_conf.get("signature_field") or first_with(["sign", "hash", "auth"], "security_hash")
    computed_field = stateful_conf.get("computed_field", "computed_metric")

    return {
        "entity_field": entity_field,
        "time_field": time_field,
        "value_field": value_field,
        "signature_field": signature_field,
        "computed_field": computed_field,
    }


def bootstrap() -> None:
    mp.set_start_method("spawn", force=True)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, "config.json")
    config = load_config(config_path)

    dynamics = config["pipeline_dynamics"]
    schema_columns = config["schema_mapping"]["columns"]
    processing = config["processing"]

    input_delay_seconds = float(dynamics["input_delay_seconds"])
    core_parallelism = int(dynamics["core_parallelism"])
    queue_max_size = int(dynamics["stream_queue_max_size"])

    verify_conf = processing["stateless_tasks"]
    stateful_conf = processing["stateful_tasks"]
    internal_fields = _resolve_internal_fields(schema_columns, verify_conf, stateful_conf)

    secret_key = verify_conf["secret_key"]
    iterations = int(verify_conf["iterations"])
    window_size = int(stateful_conf["running_average_window_size"])

    dataset_path = config["dataset_path"]
    if not os.path.isabs(dataset_path):
        dataset_path = os.path.join(script_dir, dataset_path)

    raw_queue = mp.Queue(maxsize=queue_max_size)
    intermediate_queue = mp.Queue(maxsize=queue_max_size)
    processed_queue = mp.Queue(maxsize=queue_max_size)

    input_component = GenericInputProducer(
        dataset_path=dataset_path,
        schema_columns=schema_columns,
        raw_queue=raw_queue,
        worker_count=core_parallelism,
        input_delay_seconds=input_delay_seconds,
    )

    workers = [
        StatelessVerifierWorker(
            worker_id=index,
            raw_queue=raw_queue,
            intermediate_queue=intermediate_queue,
            secret_key=secret_key,
            iterations=iterations,
            value_field=internal_fields["value_field"],
            signature_field=internal_fields["signature_field"],
        )
        for index in range(core_parallelism)
    ]

    aggregator = ResequencingAggregator(
        intermediate_queue=intermediate_queue,
        processed_queue=processed_queue,
        worker_count=core_parallelism,
        window_size=window_size,
        entity_field=internal_fields["entity_field"],
        time_field=internal_fields["time_field"],
        value_field=internal_fields["value_field"],
        computed_field=internal_fields["computed_field"],
    )

    output_dir = os.path.join(script_dir, "out")

    input_process = mp.Process(target=_input_entry, args=(input_component,), name="InputProducer")
    worker_processes = [
        mp.Process(target=_worker_entry, args=(worker,), name=f"CoreWorker-{i}")
        for i, worker in enumerate(workers)
    ]
    aggregator_process = mp.Process(target=_aggregator_entry, args=(aggregator,), name="Aggregator")
    dashboard_process = mp.Process(
        target=_dashboard_entry,
        args=(
            processed_queue,
            raw_queue,
            intermediate_queue,
            queue_max_size,
            output_dir,
            config.get("visualizations", {}),
            internal_fields["entity_field"],
            internal_fields["time_field"],
            internal_fields["value_field"],
            internal_fields["computed_field"],
        ),
        name="Dashboard",
    )

    print("\n=== Phase 3: Generic Concurrent Real-Time Pipeline ===", flush=True)
    print(f"Dataset: {dataset_path}", flush=True)
    print(f"Core parallelism: {core_parallelism}", flush=True)
    print(f"Queue max size: {queue_max_size}", flush=True)

    dashboard_process.start()
    aggregator_process.start()
    for p in worker_processes:
        p.start()
    input_process.start()

    input_process.join()
    for p in worker_processes:
        p.join()
    aggregator_process.join()
    dashboard_process.join()

    print("Pipeline completed successfully.", flush=True)


if __name__ == "__main__":
    bootstrap()
