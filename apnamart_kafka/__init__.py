"""Apna-Mart Kafka Python - A scalable Kafka producer and consumer library for Python services."""

from .base import BaseKafkaClient, BaseKafkaConfig, IHealthCheck, IMetrics, IMonitoring
from .common import BasicMonitoringHandler, MetricsCollector, PluginManager
from .config import KafkaConfig
from .config_presets import ConfigPresets
from .consumer import ConsumerMessage, KafkaConsumer
from .consumer_config import KafkaConsumerConfig
from .exceptions import (
    ConfigurationError, 
    KafkaProducerError, 
    SerializationError,
    ConsumerError,
    OffsetError,
    TopicError,
    TransactionError,
    ConnectionError,
    PublishError,
    map_confluent_error,
    handle_confluent_exception
)
from .producer import KafkaProducer
from .transactional_producer import TransactionalProducer

__version__ = "0.2.0"
__all__ = [
    # Core classes
    "KafkaProducer",
    "TransactionalProducer",
    "KafkaConsumer",
    "KafkaConfig",
    "KafkaConsumerConfig",
    "ConsumerMessage",
    "ConfigPresets",
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
    "ConsumerError",
    "OffsetError",
    "TopicError",
    "TransactionError",
    "ConnectionError",
    "PublishError",
    # Error handling utilities
    "map_confluent_error",
    "handle_confluent_exception",
]
