#!/usr/bin/env python3
"""
Real-time Streaming Analytics with Kafka

This example shows how to build real-time analytics pipelines,
including windowing, aggregations, and stream processing patterns.
"""

import statistics
import time
import uuid
from collections import defaultdict, deque
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Dict

from apnamart_kafka import Config, Consumer, Producer


@dataclass
class MetricEvent:
    """Metric event for analytics."""

    timestamp: float
    user_id: str
    event_type: str
    value: float
    dimensions: Dict[str, str]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class SlidingWindowAggregator:
    """Sliding window aggregator for real-time metrics."""

    def __init__(self, window_size_seconds: int = 60):
        self.window_size = window_size_seconds
        self.events: deque = deque()
        self.metrics = defaultdict(list)

    def add_event(self, event: MetricEvent) -> None:
        """Add event to the sliding window."""
        current_time = time.time()

        # Remove old events outside the window
        while self.events and self.events[0].timestamp < (
            current_time - self.window_size
        ):
            old_event = self.events.popleft()
            self._remove_from_metrics(old_event)

        # Add new event
        self.events.append(event)
        self._add_to_metrics(event)

    def _add_to_metrics(self, event: MetricEvent) -> None:
        """Add event to metrics tracking."""
        key = f"{event.event_type}:{event.dimensions.get('region', 'unknown')}"
        self.metrics[key].append(event.value)

    def _remove_from_metrics(self, event: MetricEvent) -> None:
        """Remove event from metrics tracking."""
        key = f"{event.event_type}:{event.dimensions.get('region', 'unknown')}"
        if key in self.metrics and event.value in self.metrics[key]:
            self.metrics[key].remove(event.value)

    def get_aggregates(self) -> Dict[str, Dict[str, float]]:
        """Get current window aggregates."""
        results = {}

        for key, values in self.metrics.items():
            if values:
                results[key] = {
                    "count": len(values),
                    "sum": sum(values),
                    "avg": statistics.mean(values),
                    "min": min(values),
                    "max": max(values),
                    "p95": statistics.quantiles(values, n=20)[18]
                    if len(values) >= 20
                    else max(values),
                }
            else:
                results[key] = {
                    "count": 0,
                    "sum": 0,
                    "avg": 0,
                    "min": 0,
                    "max": 0,
                    "p95": 0,
                }

        return results


class RealTimeAlertEngine:
    """Real-time alerting based on streaming metrics."""

    def __init__(self, producer: Producer, alert_topic: str = "alerts"):
        self.producer = producer
        self.alert_topic = alert_topic
        self.thresholds = {
            "error_rate": {"threshold": 0.05, "type": "rate"},
            "response_time": {"threshold": 500, "type": "latency"},
            "cpu_usage": {"threshold": 80, "type": "percentage"},
        }

    def check_alerts(self, aggregates: Dict[str, Dict[str, float]]) -> None:
        """Check for alert conditions and send alerts."""
        for metric_key, metrics in aggregates.items():
            metric_type = metric_key.split(":")[0]

            if metric_type in self.thresholds:
                threshold_config = self.thresholds[metric_type]
                current_value = metrics.get("avg", 0)

                if current_value > threshold_config["threshold"]:
                    self._send_alert(metric_key, current_value, threshold_config)

    def _send_alert(
        self, metric_key: str, value: float, threshold_config: Dict[str, Any]
    ) -> None:
        """Send alert to alert topic."""
        alert = {
            "alert_id": str(uuid.uuid4()),
            "metric": metric_key,
            "value": value,
            "threshold": threshold_config["threshold"],
            "severity": "critical"
            if value > threshold_config["threshold"] * 2
            else "warning",
            "timestamp": time.time(),
            "message": f"{metric_key} is {value:.2f}, exceeding threshold of {threshold_config['threshold']}",
        }

        self.producer.send(self.alert_topic, alert)
        print(f" ALERT: {alert['message']}")


class StreamProcessor:
    """Generic stream processor for analytics."""

    def __init__(self, input_topic: str, output_topic: str, config: Config):
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.config = config
        self.aggregator = SlidingWindowAggregator(window_size_seconds=30)

    def process_stream(self, duration_seconds: int = 60) -> None:
        """Process stream for specified duration."""
        print(f" Starting stream processing for {duration_seconds} seconds")

        consumer_config = Config(
            **self.config.to_consumer_config(),
            group_id=f"analytics-processor-{uuid.uuid4().hex[:8]}",
            auto_offset_reset="latest",
        )

        with (
            Consumer(self.input_topic, consumer_config) as consumer,
            Producer(self.config) as producer,
        ):
            alert_engine = RealTimeAlertEngine(producer)
            start_time = time.time()

            while (time.time() - start_time) < duration_seconds:
                message = consumer.poll(timeout=1.0)

                if message:
                    try:
                        # Parse metric event
                        event_data = message.value
                        metric_event = MetricEvent(**event_data)

                        # Add to sliding window
                        self.aggregator.add_event(metric_event)

                        # Get current aggregates
                        aggregates = self.aggregator.get_aggregates()

                        # Check for alerts
                        alert_engine.check_alerts(aggregates)

                        # Publish aggregates every 10 seconds
                        if int(time.time()) % 10 == 0:
                            self._publish_aggregates(producer, aggregates)

                        consumer.commit(message)

                    except Exception as e:
                        print(f" Error processing message: {e}")

        print(" Stream processing completed")

    def _publish_aggregates(
        self, producer: Producer, aggregates: Dict[str, Dict[str, float]]
    ) -> None:
        """Publish aggregated metrics."""
        aggregate_event = {
            "timestamp": time.time(),
            "window_size_seconds": self.aggregator.window_size,
            "aggregates": aggregates,
        }

        producer.send(self.output_topic, aggregate_event)


def generate_sample_metrics(
    producer: Producer, topic: str, duration_seconds: int = 30
) -> None:
    """Generate sample metrics for demonstration."""
    print(f" Generating sample metrics for {duration_seconds} seconds")

    regions = ["us-east", "us-west", "eu-central", "asia-pacific"]
    event_types = ["page_view", "api_call", "error_rate", "response_time", "cpu_usage"]

    start_time = time.time()

    while (time.time() - start_time) < duration_seconds:
        # Generate various types of events
        for _ in range(10):  # Burst of events
            event = MetricEvent(
                timestamp=time.time(),
                user_id=f"user-{uuid.uuid4().hex[:8]}",
                event_type=event_types[int(time.time()) % len(event_types)],
                value=_generate_realistic_value(
                    event_types[int(time.time()) % len(event_types)]
                ),
                dimensions={
                    "region": regions[int(time.time()) % len(regions)],
                    "service": "web-api",
                    "version": "v1.2.3",
                },
            )

            producer.send(topic, event.to_dict(), key=event.user_id)

        # Send batch
        producer.flush()
        time.sleep(1)  # 1 second between batches

    print(" Sample metrics generation completed")


def _generate_realistic_value(event_type: str) -> float:
    """Generate realistic values for different metric types."""
    import random

    base_time = time.time()

    if event_type == "page_view":
        return 1.0  # Count
    elif event_type == "api_call":
        return 1.0  # Count
    elif event_type == "error_rate":
        # Simulate occasional spikes
        return 0.02 + (0.1 if int(base_time) % 30 < 5 else 0) + random.uniform(0, 0.01)
    elif event_type == "response_time":
        # Simulate latency with occasional spikes
        base_latency = random.uniform(50, 200)
        return base_latency + (400 if int(base_time) % 45 < 3 else 0)
    elif event_type == "cpu_usage":
        # Simulate CPU usage with patterns
        base_cpu = 30 + 20 * abs(time.time() % 60 - 30) / 30  # Oscillating pattern
        return min(95, base_cpu + random.uniform(-10, 20))
    else:
        return random.uniform(0, 100)


def windowed_aggregation_example():
    """Demonstrate windowed aggregations."""
    print(" Windowed Aggregation Example")
    print("-" * 35)

    config = Config()

    # Create topics
    metrics_topic = "real-time-metrics"
    aggregates_topic = "metric-aggregates"

    # Start metric generation in background
    with Producer(config) as producer:
        print(" Generating sample data...")
        generate_sample_metrics(producer, metrics_topic, duration_seconds=10)

    print(" Processing metrics with sliding window...")

    # Process the stream
    processor = StreamProcessor(metrics_topic, aggregates_topic, config)
    processor.process_stream(duration_seconds=20)


def real_time_analytics_dashboard():
    """Simulate a real-time analytics dashboard."""
    print("\n Real-time Analytics Dashboard")
    print("-" * 35)

    config = Config(group_id="dashboard-consumer", auto_offset_reset="latest")

    # Track metrics for dashboard display
    metrics = defaultdict(list)

    with Consumer("metric-aggregates", config) as consumer:
        print(" Dashboard listening for aggregated metrics...")

        start_time = time.time()
        while (time.time() - start_time) < 15:  # Listen for 15 seconds
            message = consumer.poll(timeout=2.0)

            if message:
                try:
                    data = message.value
                    aggregates = data.get("aggregates", {})

                    print(
                        f"\n Dashboard Update - {datetime.fromtimestamp(data['timestamp']).strftime('%H:%M:%S')}"
                    )
                    print("-" * 25)

                    for metric_key, stats in aggregates.items():
                        if stats["count"] > 0:
                            # Store metrics for dashboard
                            metrics[metric_key].append(stats)
                            print(f"{metric_key}:")
                            print(f"  Count: {stats['count']}")
                            print(f"  Avg: {stats['avg']:.2f}")
                            print(f"  P95: {stats['p95']:.2f}")

                    consumer.commit(message)

                except Exception as e:
                    print(f" Dashboard error: {e}")

    print(" Dashboard session completed")


def anomaly_detection_example():
    """Demonstrate anomaly detection on streaming data."""
    print("\n Anomaly Detection Example")
    print("-" * 30)

    # Simple anomaly detection using z-score
    class AnomalyDetector:
        def __init__(self, threshold: float = 2.0, window_size: int = 20):
            self.threshold = threshold
            self.window_size = window_size
            self.values = deque(maxlen=window_size)

        def is_anomaly(self, value: float) -> bool:
            if len(self.values) < 3:  # Need minimum data points
                self.values.append(value)
                return False

            mean = statistics.mean(self.values)
            std_dev = statistics.stdev(self.values) if len(self.values) > 1 else 0

            if std_dev == 0:
                z_score = 0
            else:
                z_score = abs(value - mean) / std_dev

            self.values.append(value)
            return z_score > self.threshold

    detector = AnomalyDetector(threshold=2.5)

    # Simulate some data with anomalies
    import random

    print(" Detecting anomalies in simulated response times...")

    for i in range(50):
        # Normal response times with occasional anomalies
        if i in [15, 25, 40]:  # Inject anomalies
            response_time = random.uniform(800, 1200)  # Anomalous high latency
        else:
            response_time = random.uniform(50, 200)  # Normal latency

        is_anomaly = detector.is_anomaly(response_time)

        if is_anomaly:
            print(
                f" ANOMALY DETECTED: Response time {response_time:.1f}ms at sample {i}"
            )
        elif i % 10 == 0:
            print(f" Normal: Response time {response_time:.1f}ms at sample {i}")


def main():
    """Run all streaming analytics examples."""
    print(" Real-time Streaming Analytics Examples")
    print("=" * 45)
    print("Demonstrates windowing, aggregations, and real-time processing.")
    print()

    try:
        windowed_aggregation_example()
        real_time_analytics_dashboard()
        anomaly_detection_example()

        print("\n All streaming analytics examples completed!")
        print()
        print(" Key Concepts Covered:")
        print("• Sliding Window Aggregations")
        print("• Real-time Alerting")
        print("• Stream Processing Patterns")
        print("• Anomaly Detection")
        print("• Live Dashboards")

    except Exception as e:
        print(f"\n Example failed: {e}")
        print("\n Setup Requirements:")
        print("• Kafka cluster running")
        print("• Topics created automatically")
        print("• Sufficient memory for windowing")


if __name__ == "__main__":
    main()
