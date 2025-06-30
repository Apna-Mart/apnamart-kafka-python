"""Base Kafka client class for shared functionality."""

import logging
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from .config import BaseKafkaConfig
from .interfaces import IHealthCheck, IMetrics, IMonitoring

logger = logging.getLogger(__name__)


class BaseKafkaClient(ABC, IHealthCheck, IMetrics):
    """Abstract base class for Kafka clients (producer and consumer)."""

    def __init__(self, config: Optional[BaseKafkaConfig] = None, **kwargs: Any) -> None:
        """Initialize the base Kafka client.

        Args:
            config: Kafka configuration instance
            **kwargs: Additional configuration overrides
        """
        self._config = config or BaseKafkaConfig(**kwargs)
        self._is_closed = False
        self._monitoring_handlers: List[IMonitoring] = []
        self._client: Optional[Any] = None

        # Setup logging
        logging.basicConfig(level=getattr(logging, self._config.log_level))

    @abstractmethod
    def _create_client(self) -> Any:
        """Create the underlying Kafka client. Must be implemented by subclasses."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the client and clean up resources."""
        pass

    def add_monitoring_handler(self, handler: IMonitoring) -> None:
        """Add a monitoring handler for observability."""
        self._monitoring_handlers.append(handler)

    def remove_monitoring_handler(self, handler: IMonitoring) -> None:
        """Remove a monitoring handler."""
        if handler in self._monitoring_handlers:
            self._monitoring_handlers.remove(handler)

    def _notify_message_sent(
        self, topic: str, key: Any, value: Any, metadata: Dict[str, Any]
    ) -> None:
        """Notify monitoring handlers of successful message send."""
        for handler in self._monitoring_handlers:
            try:
                handler.on_message_sent(topic, key, value, metadata)
            except Exception as e:
                logger.error(f"Error in monitoring handler: {e}")

    def _notify_message_failed(
        self, topic: str, key: Any, value: Any, error: Exception
    ) -> None:
        """Notify monitoring handlers of failed message send."""
        for handler in self._monitoring_handlers:
            try:
                handler.on_message_failed(topic, key, value, error)
            except Exception as e:
                logger.error(f"Error in monitoring handler: {e}")

    def _notify_connection_established(self) -> None:
        """Notify monitoring handlers of established connection."""
        for handler in self._monitoring_handlers:
            try:
                handler.on_connection_established(self._config.bootstrap_servers)
            except Exception as e:
                logger.error(f"Error in monitoring handler: {e}")

    def _notify_connection_lost(self, error: Exception) -> None:
        """Notify monitoring handlers of lost connection."""
        for handler in self._monitoring_handlers:
            try:
                handler.on_connection_lost(self._config.bootstrap_servers, error)
            except Exception as e:
                logger.error(f"Error in monitoring handler: {e}")

    def health_check(self) -> Dict[str, Any]:
        """Perform a basic health check.

        Returns:
            Dictionary with health check results
        """
        try:
            # Try to create client if not exists
            if self._client is None:
                self._client = self._create_client()

            return {
                "status": "healthy",
                "bootstrap_servers": self._config.bootstrap_servers,
                "connection": "active",
                "timestamp": time.time(),
                "config": {
                    "security_protocol": self._config.security_protocol,
                    "topic_prefix": self._config.topic_prefix,
                },
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "bootstrap_servers": self._config.bootstrap_servers,
                "connection": "failed",
                "error": str(e),
                "timestamp": time.time(),
            }

    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics.

        Returns:
            Dictionary with current metrics
        """
        # Base implementation - can be overridden by subclasses
        return {
            "client_type": self.__class__.__name__,
            "is_closed": self._is_closed,
            "monitoring_handlers": len(self._monitoring_handlers),
            "config": {
                "bootstrap_servers": self._config.bootstrap_servers,
                "topic_prefix": self._config.topic_prefix,
            },
        }

    def record_metric(
        self, name: str, value: Any, tags: Optional[Dict[str, str]] = None
    ) -> None:
        """Record a metric value.

        Args:
            name: Metric name
            value: Metric value
            tags: Optional metric tags
        """
        # Base implementation - can be extended for specific metrics backends
        logger.debug(f"Metric recorded: {name}={value}, tags={tags}")

    @property
    def config(self) -> BaseKafkaConfig:
        """Get the client configuration."""
        return self._config

    @property
    def is_closed(self) -> bool:
        """Check if the client is closed."""
        return self._is_closed
