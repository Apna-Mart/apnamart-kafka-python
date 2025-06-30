"""Core interfaces for extensibility and future features."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Protocol


class IHealthCheck(Protocol):
    """Interface for health check capabilities."""

    def health_check(self) -> Dict[str, Any]:
        """Perform a health check and return status."""
        ...


class IMetrics(Protocol):
    """Interface for metrics collection capabilities."""

    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics."""
        ...

    def record_metric(
        self, name: str, value: Any, tags: Optional[Dict[str, str]] = None
    ) -> None:
        """Record a metric value."""
        ...


class IMonitoring(ABC):
    """Base interface for monitoring and observability."""

    @abstractmethod
    def on_message_sent(
        self, topic: str, key: Any, value: Any, metadata: Dict[str, Any]
    ) -> None:
        """Called when a message is successfully sent."""
        pass

    @abstractmethod
    def on_message_failed(
        self, topic: str, key: Any, value: Any, error: Exception
    ) -> None:
        """Called when message sending fails."""
        pass

    @abstractmethod
    def on_connection_established(self, bootstrap_servers: str) -> None:
        """Called when connection to Kafka is established."""
        pass

    @abstractmethod
    def on_connection_lost(self, bootstrap_servers: str, error: Exception) -> None:
        """Called when connection to Kafka is lost."""
        pass


class ISchemaRegistry(Protocol):
    """Interface for schema registry integration (Phase 3)."""

    def get_schema(self, subject: str, version: Optional[int] = None) -> Dict[str, Any]:
        """Get schema for a subject."""
        ...

    def register_schema(self, subject: str, schema: Dict[str, Any]) -> int:
        """Register a new schema version."""
        ...


class ISecurityProvider(Protocol):
    """Interface for security providers (Phase 2)."""

    def get_ssl_config(self) -> Dict[str, Any]:
        """Get SSL configuration."""
        ...

    def get_sasl_config(self) -> Dict[str, Any]:
        """Get SASL configuration."""
        ...


class IFrameworkIntegration(ABC):
    """Base interface for framework integrations (Phase 3)."""

    @abstractmethod
    def setup_middleware(self, app: Any) -> None:
        """Setup framework-specific middleware."""
        pass

    @abstractmethod
    def create_dependency_injection(self) -> Any:
        """Create framework-specific dependency injection."""
        pass
