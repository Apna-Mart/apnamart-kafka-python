"""Custom exceptions for the Kafka library with confluent-kafka error mapping."""

from typing import Optional
from confluent_kafka import KafkaError, KafkaException


class KafkaProducerError(Exception):
    """Base exception for all Kafka producer/consumer errors."""

    def __init__(self, message: str, kafka_error: Optional[KafkaError] = None):
        super().__init__(message)
        self.kafka_error = kafka_error


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


class ConsumerError(KafkaProducerError):
    """Raised when consumer operations fail."""

    pass


class OffsetError(KafkaProducerError):
    """Raised when offset operations fail."""

    pass


class TopicError(KafkaProducerError):
    """Raised when topic operations fail."""

    pass


class TransactionError(KafkaProducerError):
    """Raised when transaction operations fail."""

    pass


def map_confluent_error(kafka_error: KafkaError, default_message: str = "Kafka operation failed") -> KafkaProducerError:
    """Map confluent-kafka errors to our custom exceptions.
    
    Args:
        kafka_error: The KafkaError from confluent-kafka
        default_message: Default message if error mapping is not specific
        
    Returns:
        Appropriate custom exception
    """
    error_code = kafka_error.code()
    error_msg = f"{default_message}: {kafka_error}"
    
    # Connection related errors
    if error_code in [
        KafkaError._NETWORK_EXCEPTION,
        KafkaError._RESOLVE, 
        KafkaError._TRANSPORT,
        KafkaError._BROKER_NOT_AVAILABLE,
        KafkaError._ALL_BROKERS_DOWN,
    ]:
        return ConnectionError(error_msg, kafka_error)
    
    # Configuration related errors  
    elif error_code in [
        KafkaError._INVALID_CONFIG,
        KafkaError._UNSUPPORTED_FEATURE,
        KafkaError._INVALID_ARG,
    ]:
        return ConfigurationError(error_msg, kafka_error)
    
    # Topic related errors
    elif error_code in [
        KafkaError.UNKNOWN_TOPIC_OR_PART,
        KafkaError.TOPIC_AUTHORIZATION_FAILED,
        KafkaError.INVALID_TOPIC_EXCEPTION,
        KafkaError.TOPIC_ALREADY_EXISTS,
    ]:
        return TopicError(error_msg, kafka_error)
    
    # Producer specific errors
    elif error_code in [
        KafkaError.MSG_SIZE_TOO_LARGE,
        KafkaError.RECORD_LIST_TOO_LARGE,
        KafkaError.INVALID_RECORD,
        KafkaError.CORRUPT_MESSAGE,
    ]:
        return PublishError(error_msg, kafka_error)
    
    # Consumer specific errors
    elif error_code in [
        KafkaError.GROUP_COORDINATOR_NOT_AVAILABLE,
        KafkaError.NOT_COORDINATOR_FOR_GROUP,
        KafkaError.ILLEGAL_GENERATION,
        KafkaError.INCONSISTENT_GROUP_PROTOCOL,
        KafkaError.INVALID_GROUP_ID,
        KafkaError.UNKNOWN_MEMBER_ID,
        KafkaError.INVALID_SESSION_TIMEOUT,
        KafkaError.REBALANCE_IN_PROGRESS,
        KafkaError.INVALID_COMMIT_OFFSET_SIZE,
        KafkaError.GROUP_AUTHORIZATION_FAILED,
    ]:
        return ConsumerError(error_msg, kafka_error)
    
    # Offset related errors
    elif error_code in [
        KafkaError.OFFSET_OUT_OF_RANGE,
        KafkaError.INVALID_COMMIT_OFFSET_SIZE,
        KafkaError.OFFSET_METADATA_TOO_LARGE,
    ]:
        return OffsetError(error_msg, kafka_error)
    
    # Transaction related errors
    elif error_code in [
        KafkaError.INVALID_TRANSACTION_STATE,
        KafkaError.INVALID_PRODUCER_EPOCH,
        KafkaError.INVALID_TXN_STATE,
        KafkaError.INVALID_PRODUCER_ID_MAPPING,
        KafkaError.TRANSACTION_COORDINATOR_NOT_AVAILABLE,
        KafkaError.NOT_COORDINATOR_FOR_TRANSACTION,
        KafkaError.COORDINATOR_LOAD_IN_PROGRESS,
        KafkaError.COORDINATOR_NOT_AVAILABLE,
        KafkaError.CONCURRENT_TRANSACTIONS,
    ]:
        return TransactionError(error_msg, kafka_error)
    
    # Default to generic KafkaProducerError
    else:
        return KafkaProducerError(error_msg, kafka_error)


def handle_confluent_exception(func):
    """Decorator to automatically map confluent-kafka exceptions to our custom exceptions.
    
    Usage:
        @handle_confluent_exception
        def some_kafka_operation(self):
            # Code that might raise KafkaError or KafkaException
            pass
    """
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (KafkaError, KafkaException) as e:
            if isinstance(e, KafkaError):
                raise map_confluent_error(e, f"Error in {func.__name__}")
            else:
                # KafkaException - extract the KafkaError if available
                kafka_error = getattr(e, 'args', [None])[0]
                if isinstance(kafka_error, KafkaError):
                    raise map_confluent_error(kafka_error, f"Error in {func.__name__}")
                else:
                    raise KafkaProducerError(f"Error in {func.__name__}: {e}")
        except Exception as e:
            # Re-raise non-Kafka exceptions as-is
            raise
    
    return wrapper
