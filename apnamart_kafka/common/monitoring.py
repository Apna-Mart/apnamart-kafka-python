"""Monitoring and observability implementations."""

import logging
import time
from typing import Any, Dict, Optional

from ..base.interfaces import IMonitoring

logger = logging.getLogger(__name__)


class BasicMonitoringHandler(IMonitoring):
    """Basic monitoring handler that logs events."""

    def __init__(self, log_level: str = "INFO") -> None:
        """Initialize the basic monitoring handler.

        Args:
            log_level: Log level for monitoring events
        """
        self.logger = logging.getLogger(f"{__name__}.BasicMonitoringHandler")
        self.logger.setLevel(getattr(logging, log_level.upper()))

    def on_message_sent(
        self, topic: str, key: Any, value: Any, metadata: Dict[str, Any]
    ) -> None:
        """Called when a message is successfully sent."""
        self.logger.info(
            f"Message sent to topic '{topic}' with key '{key}', metadata: {metadata}"
        )

    def on_message_failed(
        self, topic: str, key: Any, value: Any, error: Exception
    ) -> None:
        """Called when message sending fails."""
        self.logger.error(
            f"Message failed for topic '{topic}' with key '{key}': {error}"
        )

    def on_connection_established(self, bootstrap_servers: str) -> None:
        """Called when connection to Kafka is established."""
        self.logger.info(f"Connection established to Kafka: {bootstrap_servers}")

    def on_connection_lost(self, bootstrap_servers: str, error: Exception) -> None:
        """Called when connection to Kafka is lost."""
        self.logger.error(f"Connection lost to Kafka {bootstrap_servers}: {error}")


class MetricsCollector:
    """Collects and aggregates metrics for Kafka operations."""

    def __init__(self) -> None:
        """Initialize the metrics collector."""
        self._metrics: Dict[str, Any] = {
            "messages_sent": 0,
            "messages_failed": 0,
            "bytes_sent": 0,
            "connection_count": 0,
            "connection_failures": 0,
            "last_activity": None,
        }
        self._topic_metrics: Dict[str, Dict[str, Any]] = {}

    def record_message_sent(self, topic: str, message_size: int) -> None:
        """Record a successfully sent message."""
        self._metrics["messages_sent"] += 1
        self._metrics["bytes_sent"] += message_size
        self._metrics["last_activity"] = time.time()

        # Topic-specific metrics
        if topic not in self._topic_metrics:
            self._topic_metrics[topic] = {
                "messages_sent": 0,
                "messages_failed": 0,
                "bytes_sent": 0,
            }

        self._topic_metrics[topic]["messages_sent"] += 1
        self._topic_metrics[topic]["bytes_sent"] += message_size

    def record_message_failed(self, topic: str) -> None:
        """Record a failed message."""
        self._metrics["messages_failed"] += 1
        self._metrics["last_activity"] = time.time()

        # Topic-specific metrics
        if topic not in self._topic_metrics:
            self._topic_metrics[topic] = {
                "messages_sent": 0,
                "messages_failed": 0,
                "bytes_sent": 0,
            }

        self._topic_metrics[topic]["messages_failed"] += 1

    def record_connection_established(self) -> None:
        """Record a successful connection."""
        self._metrics["connection_count"] += 1
        self._metrics["last_activity"] = time.time()

    def record_connection_failed(self) -> None:
        """Record a failed connection."""
        self._metrics["connection_failures"] += 1
        self._metrics["last_activity"] = time.time()

    def get_metrics(self) -> Dict[str, Any]:
        """Get all collected metrics."""
        return {
            "global": self._metrics.copy(),
            "topics": self._topic_metrics.copy(),
        }

    def get_topic_metrics(self, topic: str) -> Optional[Dict[str, Any]]:
        """Get metrics for a specific topic."""
        return self._topic_metrics.get(topic)

    def reset_metrics(self) -> None:
        """Reset all metrics to zero."""
        self._metrics = {
            "messages_sent": 0,
            "messages_failed": 0,
            "bytes_sent": 0,
            "connection_count": 0,
            "connection_failures": 0,
            "last_activity": None,
        }
        self._topic_metrics.clear()


class MonitoringCollectorHandler(IMonitoring):
    """Monitoring handler that integrates with MetricsCollector."""

    def __init__(self, collector: MetricsCollector) -> None:
        """Initialize with a metrics collector.

        Args:
            collector: MetricsCollector instance to use
        """
        self.collector = collector

    def on_message_sent(
        self, topic: str, key: Any, value: Any, metadata: Dict[str, Any]
    ) -> None:
        """Called when a message is successfully sent."""
        # Estimate message size (this could be improved with actual serialized size)
        message_size = len(str(value).encode("utf-8")) if value else 0
        self.collector.record_message_sent(topic, message_size)

    def on_message_failed(
        self, topic: str, key: Any, value: Any, error: Exception
    ) -> None:
        """Called when message sending fails."""
        self.collector.record_message_failed(topic)

    def on_connection_established(self, bootstrap_servers: str) -> None:
        """Called when connection to Kafka is established."""
        self.collector.record_connection_established()

    def on_connection_lost(self, bootstrap_servers: str, error: Exception) -> None:
        """Called when connection to Kafka is lost."""
        self.collector.record_connection_failed()
