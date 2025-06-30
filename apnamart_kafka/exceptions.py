"""Custom exceptions for the Kafka producer."""


class KafkaProducerError(Exception):
    """Base exception for all Kafka producer errors."""

    pass


class ConfigurationError(KafkaProducerError):
    """Raised when there's an issue with configuration."""

    pass


class SerializationError(KafkaProducerError):
    """Raised when serialization fails."""

    pass


class ConnectionError(KafkaProducerError):
    """Raised when connection to Kafka fails."""

    pass


class PublishError(KafkaProducerError):
    """Raised when message publishing fails."""

    pass
