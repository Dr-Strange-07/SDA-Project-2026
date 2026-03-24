from typing import Protocol, Dict, Any, runtime_checkable


@runtime_checkable
class InputPort(Protocol):
    def run(self) -> None:
        ...


@runtime_checkable
class StatelessProcessor(Protocol):
    def run(self) -> None:
        ...


@runtime_checkable
class AggregationProcessor(Protocol):
    def run(self) -> None:
        ...


@runtime_checkable
class TelemetryObserver(Protocol):
    def on_telemetry(self, snapshot: Dict[str, Any]) -> None:
        ...
