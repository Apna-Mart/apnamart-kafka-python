"""Unified Kafka client with Producer, Consumer, and TransactionalProducer."""

import json
import time
from typing import Any, Dict, Iterator, List, Optional, Union

from confluent_kafka import Consumer as ConfluentConsumer
from confluent_kafka import KafkaError as ConfluentKafkaError
from confluent_kafka import Producer as ConfluentProducer
from confluent_kafka import TopicPartition

# ============================================================================
# CONFIGURATION
# ============================================================================


class Config:
    """Simple configuration for Kafka clients."""

    def __init__(self, **kwargs: Any) -> None:
        """Initialize with configuration parameters."""
        self.bootstrap_servers = kwargs.get("bootstrap_servers", "localhost:9092")
        self.acks = kwargs.get("acks", "all")
        self.retries = kwargs.get("retries", 3)
        self.compression_type = kwargs.get("compression_type", None)
        self.batch_size = kwargs.get("batch_size", 16384)
        self.linger_ms = kwargs.get("linger_ms", 0)

        # Consumer-specific
        self.group_id = kwargs.get("group_id", "default-group")
        self.auto_offset_reset = kwargs.get("auto_offset_reset", "latest")
        self.enable_auto_commit = kwargs.get("enable_auto_commit", True)

        # Store all config
        self.config = kwargs
        self.config.update(
            {
                "bootstrap_servers": self.bootstrap_servers,
                "acks": self.acks,
                "retries": self.retries,
            }
        )

    def to_producer_config(self) -> Dict[str, Any]:
        """Convert to confluent-kafka producer config."""
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "acks": -1 if self.acks == "all" else self.acks,
            "retries": self.retries,
            "batch.size": self.batch_size,
            "linger.ms": self.linger_ms,
        }

        # Only add compression if it's set
        if self.compression_type:
            config["compression.type"] = self.compression_type

        return config

    def to_consumer_config(self) -> Dict[str, Any]:
        """Convert to confluent-kafka consumer config."""
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": self.enable_auto_commit,
        }


# ============================================================================
# EXCEPTIONS
# ============================================================================


class KafkaError(Exception):
    """Base exception for Kafka operations."""

    pass


class ProducerError(KafkaError):
    """Producer-specific errors."""

    pass


class ConsumerError(KafkaError):
    """Consumer-specific errors."""

    pass


class TransactionError(KafkaError):
    """Transaction-specific errors."""

    pass


# ============================================================================
# SERIALIZATION
# ============================================================================


def serialize(data: Any) -> bytes:
    """Auto-serialize data to bytes."""
    if isinstance(data, bytes):
        return data
    elif isinstance(data, str):
        return data.encode("utf-8")
    else:
        return json.dumps(data).encode("utf-8")


def deserialize(data: bytes) -> Any:
    """Auto-deserialize bytes to data."""
    try:
        return json.loads(data.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError:
            return data


# ============================================================================
# MESSAGE
# ============================================================================


class Message:
    """Kafka message with metadata."""

    def __init__(self, record: Any) -> None:
        """Initialize from confluent-kafka message."""
        self.topic = record.topic()
        self.partition = record.partition()
        self.offset = record.offset()
        self.key = record.key()
        self.value = deserialize(record.value()) if record.value() else None
        self.timestamp = record.timestamp()[1] if record.timestamp()[0] != -1 else None
        self.headers = dict(record.headers()) if record.headers() else {}

    def __repr__(self) -> str:
        return f"Message(topic='{self.topic}', partition={self.partition}, offset={self.offset})"


# ============================================================================
# PRODUCER
# ============================================================================


class Producer:
    """Simple Kafka producer."""

    def __init__(self, config: Optional[Config] = None, **kwargs: Any) -> None:
        """Initialize producer."""
        if config is None:
            config = Config(**kwargs)

        self.config = config
        self._producer = None
        self._closed = False

    def _get_producer(self) -> ConfluentProducer:
        """Get or create producer."""
        if self._producer is None:
            if self._closed:
                raise ProducerError("Producer is closed")
            self._producer = ConfluentProducer(self.config.to_producer_config())
        return self._producer

    def send(self, topic: str, value: Any, key: Any = None, **kwargs: Any) -> None:
        """Send a message."""
        if self._closed:
            raise ProducerError("Producer is closed")

        producer = self._get_producer()

        # Serialize data
        serialized_key = serialize(key) if key is not None else None
        serialized_value = serialize(value)

        # Send message
        producer.produce(
            topic=topic, value=serialized_value, key=serialized_key, **kwargs
        )
        producer.poll(0)  # Trigger delivery

    def send_batch(self, messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Send multiple messages."""
        if self._closed:
            raise ProducerError("Producer is closed")

        producer = self._get_producer()
        results = []

        for msg in messages:
            try:
                topic = msg.get("topic")
                value = msg.get("value")
                key = msg.get("key")

                if not topic or value is None:
                    results.append(
                        {"success": False, "error": "Missing topic or value"}
                    )
                    continue

                producer.produce(
                    topic=topic,
                    value=serialize(value),
                    key=serialize(key) if key is not None else None,
                )
                results.append({"success": True})

            except Exception as e:
                results.append({"success": False, "error": str(e)})

        # Wait for delivery
        producer.flush(timeout=10)
        return results

    def flush(self, timeout: float = 10) -> Optional[int]:
        """Wait for messages to be sent."""
        if self._producer:
            return self._producer.flush(timeout)
        return None

    def close(self) -> None:
        """Close producer."""
        if not self._closed:
            self._closed = True
            if self._producer:
                self.flush()
                self._producer = None

    def __enter__(self) -> "Producer":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


# ============================================================================
# CONSUMER
# ============================================================================


class Consumer:
    """Simple Kafka consumer."""

    def __init__(
        self,
        topics: Optional[Union[str, List[str]]] = None,
        config: Optional[Config] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize consumer."""
        if config is None:
            config = Config(**kwargs)

        self.config = config
        self._consumer = None
        self._closed = False

        # Handle topics
        if isinstance(topics, str):
            topics = [topics]
        self.topics = topics

    def _get_consumer(self) -> ConfluentConsumer:
        """Get or create consumer."""
        if self._consumer is None:
            if self._closed:
                raise ConsumerError("Consumer is closed")
            self._consumer = ConfluentConsumer(self.config.to_consumer_config())
            if self.topics and self._consumer:
                self._consumer.subscribe(self.topics)
        return self._consumer

    def poll(self, timeout: float = 1.0) -> Optional[Message]:
        """Poll for a message."""
        if self._closed:
            raise ConsumerError("Consumer is closed")

        consumer = self._get_consumer()
        msg = consumer.poll(timeout)

        if msg is None:
            return None

        if msg.error():
            if msg.error().code() == ConfluentKafkaError._PARTITION_EOF:
                return None
            raise ConsumerError(f"Consumer error: {msg.error()}")

        return Message(msg)

    def poll_batch(self, size: int = 100, timeout: float = 10) -> List[Message]:
        """Poll for multiple messages."""
        messages: List[Message] = []
        start_time = time.time()

        while len(messages) < size and (time.time() - start_time) < timeout:
            remaining = timeout - (time.time() - start_time)
            if remaining <= 0:
                break

            msg = self.poll(remaining)
            if msg:
                messages.append(msg)
            else:
                break

        return messages

    def commit(self, message: Optional[Message] = None) -> None:
        """Commit offsets."""
        if self._closed:
            raise ConsumerError("Consumer is closed")

        consumer = self._get_consumer()

        if message:
            tp = TopicPartition(message.topic, message.partition, message.offset + 1)
            consumer.commit(offsets=[tp])
        else:
            consumer.commit()

    def close(self) -> None:
        """Close consumer."""
        if not self._closed:
            self._closed = True
            if self._consumer:
                self._consumer.close()
                self._consumer = None

    def __enter__(self) -> "Consumer":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def __iter__(self) -> "Consumer":
        return self

    def __next__(self) -> Message:
        while not self._closed:
            msg = self.poll(1.0)
            if msg:
                return msg
        raise StopIteration


# ============================================================================
# TRANSACTIONAL PRODUCER
# ============================================================================


class TransactionalProducer(Producer):
    """Producer with transaction support."""

    def __init__(
        self, transactional_id: str, config: Optional[Config] = None, **kwargs: Any
    ) -> None:
        """Initialize transactional producer."""
        if config is None:
            config = Config(**kwargs)

        # Add transactional settings
        config.config.update(
            {"transactional.id": transactional_id, "enable.idempotence": True}
        )

        super().__init__(config)
        self.transactional_id = transactional_id
        self._in_transaction = False
        self._initialized = False

    def _get_producer(self) -> ConfluentProducer:
        """Get or create transactional producer."""
        if self._producer is None:
            if self._closed:
                raise TransactionError("Producer is closed")

            producer_config = self.config.to_producer_config()
            producer_config.update(
                {"transactional.id": self.transactional_id, "enable.idempotence": True}
            )

            self._producer = ConfluentProducer(producer_config)

            if not self._initialized and self._producer:
                self._producer.init_transactions()
                self._initialized = True

        return self._producer

    def begin(self) -> None:
        """Begin transaction."""
        if self._in_transaction:
            raise TransactionError("Transaction already in progress")

        producer = self._get_producer()
        producer.begin_transaction()
        self._in_transaction = True

    def commit(self) -> None:
        """Commit transaction."""
        if not self._in_transaction:
            raise TransactionError("No transaction in progress")

        producer = self._get_producer()
        producer.commit_transaction()
        self._in_transaction = False

    def abort(self) -> None:
        """Abort transaction."""
        if not self._in_transaction:
            raise TransactionError("No transaction in progress")

        producer = self._get_producer()
        producer.abort_transaction()
        self._in_transaction = False

    def send_transactional(self, messages: List[Dict[str, Any]]) -> None:
        """Send messages in a transaction."""
        if self._in_transaction:
            raise TransactionError("Transaction already in progress")

        self.begin()
        try:
            for msg in messages:
                topic = msg.get("topic")
                if topic:
                    self.send(topic, msg.get("value"), msg.get("key"))
            self.commit()
        except Exception as e:
            self.abort()
            raise TransactionError(f"Transaction failed: {e}")

    def close(self) -> None:
        """Close producer, aborting any active transaction."""
        if self._in_transaction:
            try:
                self.abort()
            except Exception:
                pass
        super().close()

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self._in_transaction:
            if exc_type is None:
                try:
                    self.commit()
                except Exception:
                    self.abort()
            else:
                self.abort()
        self.close()


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================


def send(
    topic: str,
    value: Any,
    key: Any = None,
    servers: str = "localhost:9092",
    **kwargs: Any,
) -> None:
    """Quick send function."""
    config = Config(bootstrap_servers=servers, **kwargs)
    with Producer(config) as producer:
        producer.send(topic, value, key)


def consume(
    topics: Union[str, List[str]],
    servers: str = "localhost:9092",
    group_id: str = "default",
    **kwargs: Any,
) -> Iterator[Message]:
    """Quick consume function."""
    config = Config(bootstrap_servers=servers, group_id=group_id, **kwargs)
    with Consumer(topics, config) as consumer:
        for message in consumer:
            yield message
