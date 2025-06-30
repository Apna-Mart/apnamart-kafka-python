"""Base classes and interfaces for the Kafka library."""

from .client import BaseKafkaClient
from .config import BaseKafkaConfig
from .interfaces import IHealthCheck, IMetrics, IMonitoring

__all__ = [
    "BaseKafkaClient",
    "BaseKafkaConfig",
    "IHealthCheck",
    "IMetrics",
    "IMonitoring",
]
