"""Apna-Mart Kafka Python - A scalable Kafka producer and consumer library for Python services."""

from .base import BaseKafkaClient, BaseKafkaConfig, IHealthCheck, IMetrics, IMonitoring
from .common import BasicMonitoringHandler, MetricsCollector, PluginManager
from .config import KafkaConfig
from .consumer import ConsumerMessage, KafkaConsumer
from .consumer_config import KafkaConsumerConfig
from .exceptions import ConfigurationError, KafkaProducerError, SerializationError
from .producer import KafkaProducer

__version__ = "0.1.1"
__all__ = [
    # Core classes
    "KafkaProducer",
    "KafkaConsumer",
    "KafkaConfig",
    "KafkaConsumerConfig",
    "ConsumerMessage",
    # Base classes for extensibility
    "BaseKafkaClient",
    "BaseKafkaConfig",
    # Interfaces
    "IHealthCheck",
    "IMetrics",
    "IMonitoring",
    # Common utilities
    "BasicMonitoringHandler",
    "MetricsCollector",
    "PluginManager",
    # Exceptions
    "KafkaProducerError",
    "ConfigurationError",
    "SerializationError",
]
