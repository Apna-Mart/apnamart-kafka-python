"""ApnaMart Kafka Python - Minimal Kafka client boilerplate for Python applications."""

# Import everything from unified client
from .client import (
    Config,
    Consumer,
    ConsumerError,
    KafkaError,
    Message,
    Producer,
    ProducerError,
    TransactionalProducer,
    TransactionError,
    consume,
    deserialize,
    send,
    serialize,
)

__version__ = "2.0.0"

# Simple, clean exports
__all__ = [
    "Config",
    "Producer",
    "Consumer",
    "TransactionalProducer",
    "Message",
    "KafkaError",
    "ProducerError",
    "ConsumerError",
    "TransactionError",
    "send",
    "consume",
    "serialize",
    "deserialize",
]
