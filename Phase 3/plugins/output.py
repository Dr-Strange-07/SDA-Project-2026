import os
import time
from typing import Dict, List

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from core.contracts import TelemetryObserver


class RealTimeDashboard(TelemetryObserver):
    """
    Observer + Output module.
    Consumes processed packets and renders real-time telemetry + charts.
    """

    def __init__(
        self,
        processed_queue,
        output_dir: str,
        visuals_config: Dict,
        entity_field: str,
        time_field: str,
        value_field: str,
        computed_field: str,
    ) -> None:
        self.processed_queue = processed_queue
        self.output_dir = output_dir
        self.visuals_config = visuals_config
        self.entity_field = entity_field
        self.time_field = time_field
        self.value_field = value_field
        self.computed_field = computed_field

        self.telemetry_snapshot = {
            "raw_stream": {"size": 0, "ratio": 0.0, "level": "green"},
            "intermediate_stream": {"size": 0, "ratio": 0.0, "level": "green"},
            "processed_stream": {"size": 0, "ratio": 0.0, "level": "green"},
        }

        self.time_points: List[float] = []
        self.metric_values: List[float] = []
        self.running_averages: List[float] = []
        self.final_records: List[Dict] = []
        self.max_points = 250
        render_every_n_frames = int(self.visuals_config.get("render_every_n_frames", 5))
        self.render_every_n_frames = max(1, render_every_n_frames)
        self.progress_log_every = 25

        os.makedirs(self.output_dir, exist_ok=True)

    def on_telemetry(self, snapshot: Dict) -> None:
        self.telemetry_snapshot = snapshot

    def _bar_color(self, level: str) -> str:
        return {
            "green": "#2ecc71",
            "yellow": "#f1c40f",
            "red": "#e74c3c",
        }.get(level, "#95a5a6")

    def _render(self, frame_index: int) -> None:
        fig = plt.figure(figsize=(14, 9))
        gs = fig.add_gridspec(2, 2, height_ratios=[1, 2])

        ax_telemetry = fig.add_subplot(gs[0, :])
        ax_values = fig.add_subplot(gs[1, 0])
        ax_avg = fig.add_subplot(gs[1, 1])

        telemetry_cfg = self.visuals_config.get("telemetry", {})
        labels = []
        keys = []
        if telemetry_cfg.get("show_raw_stream", True):
            labels.append("Raw")
            keys.append("raw_stream")
        if telemetry_cfg.get("show_intermediate_stream", True):
            labels.append("Intermediate")
            keys.append("intermediate_stream")
        if telemetry_cfg.get("show_processed_stream", True):
            labels.append("Processed")
            keys.append("processed_stream")

        if not keys:
            labels = ["Raw", "Intermediate", "Processed"]
            keys = ["raw_stream", "intermediate_stream", "processed_stream"]

        ratios = [self.telemetry_snapshot[k]["ratio"] * 100 for k in keys]
        colors = [self._bar_color(self.telemetry_snapshot[k]["level"]) for k in keys]

        ax_telemetry.bar(labels, ratios, color=colors, edgecolor="black")
        ax_telemetry.set_ylim(0, 100)
        ax_telemetry.set_ylabel("Queue Fill %")
        ax_telemetry.set_title("Pipeline Telemetry (Observer View)")
        ax_telemetry.grid(axis="y", linestyle="--", alpha=0.35)

        show_values = self.visuals_config.get("data_charts", [])
        title_values = "Live Sensor Values (Authentic Only)"
        title_avg = "Live Sensor Running Average"
        if len(show_values) > 0:
            title_values = show_values[0].get("title", title_values)
        if len(show_values) > 1:
            title_avg = show_values[1].get("title", title_avg)

        ax_values.plot(self.time_points, self.metric_values, color="#3498db", linewidth=2)
        ax_values.set_title(title_values)
        x_axis = show_values[0].get("x_axis", self.time_field) if len(show_values) > 0 else self.time_field
        y_axis = show_values[0].get("y_axis", self.value_field) if len(show_values) > 0 else self.value_field
        ax_values.set_xlabel(x_axis)
        ax_values.set_ylabel(y_axis)
        ax_values.grid(True, linestyle="--", alpha=0.35)

        ax_avg.plot(self.time_points, self.running_averages, color="#9b59b6", linewidth=2)
        ax_avg.set_title(title_avg)
        avg_x_axis = show_values[1].get("x_axis", self.time_field) if len(show_values) > 1 else self.time_field
        avg_y_axis = show_values[1].get("y_axis", self.computed_field) if len(show_values) > 1 else self.computed_field
        ax_avg.set_xlabel(avg_x_axis)
        ax_avg.set_ylabel(avg_y_axis)
        ax_avg.grid(True, linestyle="--", alpha=0.35)

        fig.suptitle("Generic Concurrent Real-Time Pipeline Dashboard", fontsize=14, fontweight="bold")
        fig.tight_layout()

        out_path = os.path.join(self.output_dir, "live_dashboard_latest.png")
        fig.savefig(out_path, dpi=120)
        plt.close(fig)

        if frame_index % 20 == 0:
            archived = os.path.join(self.output_dir, f"live_dashboard_frame_{frame_index:05d}.png")
            fig2 = plt.figure(figsize=(2, 2))
            plt.text(0.5, 0.5, f"Frame {frame_index}", ha="center", va="center")
            plt.axis("off")
            fig2.savefig(archived, dpi=80)
            plt.close(fig2)

    def run(self) -> None:
        frame_index = 0
        print(
            f"[Dashboard] Processing stream started (render every {self.render_every_n_frames} frames)",
            flush=True,
        )
        while True:
            item = self.processed_queue.get()
            if item is None:
                break

            self.final_records.append(item)

            self.time_points.append(item[self.time_field])
            self.metric_values.append(float(item[self.value_field]))
            self.running_averages.append(float(item[self.computed_field]))

            if len(self.time_points) > self.max_points:
                self.time_points = self.time_points[-self.max_points :]
                self.metric_values = self.metric_values[-self.max_points :]
                self.running_averages = self.running_averages[-self.max_points :]

            frame_index += 1
            if frame_index % self.render_every_n_frames == 0:
                self._render(frame_index)

            if frame_index % self.progress_log_every == 0:
                print(f"[Dashboard] Processed records: {frame_index}", flush=True)

        self._render(frame_index)
        self._print_previous_style_summary()
        self._render_previous_style_graphs()
        time.sleep(0.1)
        print(f"[Dashboard] Final snapshot written to: {os.path.join(self.output_dir, 'live_dashboard_latest.png')}", flush=True)

    def _print_previous_style_summary(self) -> None:
        if not self.final_records:
            print("[Summary] No records received.")
            return

        count = len(self.final_records)
        unique_entities = len({r[self.entity_field] for r in self.final_records})
        min_metric = min(self.metric_values)
        max_metric = max(self.metric_values)
        avg_metric = sum(self.metric_values) / count

        print("\n" + "=" * 78)
        print("FINAL DATA SUMMARY".center(78))
        print("=" * 78)
        print(f"Total authentic records: {count}")
        print(f"Unique entities: {unique_entities}")
        print(f"Metric range: {min_metric:.2f} -> {max_metric:.2f}")
        print(f"Average metric: {avg_metric:.2f}")
        print("\nSample records (first 10):")
        print(
            f"  sequence_id | {self.entity_field} | {self.time_field} | "
            f"{self.value_field} | {self.computed_field}"
        )

        for record in self.final_records[:10]:
            print(
                f"  {record['sequence_id']:>10} | "
                f"{str(record[self.entity_field]):<11} | "
                f"{str(record[self.time_field]):>11} | "
                f"{float(record[self.value_field]):>12.2f} | "
                f"{float(record[self.computed_field]):>15.2f}"
            )

        print("=" * 78)

    def _render_previous_style_graphs(self) -> None:
        if not self.final_records:
            return

        self._plot_top_entities_bar()
        self._plot_metric_histogram()
        self._plot_metric_dotplot()
        self._plot_entity_share_pie()

        print(
            "[Summary] Additional graphs written to: "
            f"{os.path.join(self.output_dir, 'summary_*.png')}"
        )

    def _plot_top_entities_bar(self) -> None:
        totals: Dict[str, float] = {}
        counts: Dict[str, int] = {}

        for record in self.final_records:
            name = str(record[self.entity_field])
            totals[name] = totals.get(name, 0.0) + float(record[self.value_field])
            counts[name] = counts.get(name, 0) + 1

        averages = [
            (name, totals[name] / counts[name])
            for name in totals
        ]
        top_entities = sorted(averages, key=lambda item: item[1], reverse=True)[:10]

        labels = [name for name, _ in reversed(top_entities)]
        values = [value for _, value in reversed(top_entities)]

        fig, ax = plt.subplots(figsize=(11, 7))
        ax.barh(labels, values, color="#3498db", edgecolor="black")
        ax.set_title("Top Entities by Average Metric")
        ax.set_xlabel(f"Average {self.value_field}")
        ax.grid(axis="x", linestyle="--", alpha=0.35)
        fig.tight_layout()
        fig.savefig(os.path.join(self.output_dir, "summary_01_top_entities_bar.png"), dpi=120)
        plt.close(fig)

    def _plot_metric_histogram(self) -> None:
        fig, ax = plt.subplots(figsize=(11, 7))
        ax.hist(self.metric_values, bins=12, color="#2ecc71", edgecolor="black")
        ax.set_title("Metric Value Distribution")
        ax.set_xlabel(self.value_field)
        ax.set_ylabel("Frequency")
        ax.grid(axis="y", linestyle="--", alpha=0.35)
        fig.tight_layout()
        fig.savefig(os.path.join(self.output_dir, "summary_02_metric_histogram.png"), dpi=120)
        plt.close(fig)

    def _plot_metric_dotplot(self) -> None:
        ordered_records = sorted(self.final_records, key=lambda r: r[self.time_field])
        time_points = [float(r[self.time_field]) for r in ordered_records]
        values = [float(r[self.value_field]) for r in ordered_records]

        fig, ax = plt.subplots(figsize=(11, 7))
        ax.plot(time_points, values, "o", color="#9b59b6")
        ax.vlines(time_points, [min(values)] * len(values), values, color="#c7b5d6", alpha=0.35)
        ax.set_title("Metric Values Over Time (Dot Plot)")
        ax.set_xlabel(self.time_field)
        ax.set_ylabel(self.value_field)
        ax.grid(True, linestyle="--", alpha=0.35)
        fig.tight_layout()
        fig.savefig(os.path.join(self.output_dir, "summary_03_metric_dotplot.png"), dpi=120)
        plt.close(fig)

    def _plot_entity_share_pie(self) -> None:
        totals: Dict[str, float] = {}
        for record in self.final_records:
            name = str(record[self.entity_field])
            totals[name] = totals.get(name, 0.0) + float(record[self.value_field])

        sorted_totals = sorted(totals.items(), key=lambda item: item[1], reverse=True)
        if len(sorted_totals) > 8:
            top = sorted_totals[:8]
            others = sum(value for _, value in sorted_totals[8:])
            pie_data = top + [("Others", others)]
        else:
            pie_data = sorted_totals

        labels = [name for name, _ in pie_data]
        values = [value for _, value in pie_data]

        fig, ax = plt.subplots(figsize=(10, 8))
        ax.pie(values, labels=labels, autopct="%1.1f%%", startangle=140)
        ax.set_title("Entity Contribution Share")
        fig.tight_layout()
        fig.savefig(os.path.join(self.output_dir, "summary_04_entity_share_pie.png"), dpi=120)
        plt.close(fig)
