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
        """Send a message to Kafka topic.

        Args:
            topic: Kafka topic name
            value: Message value (will be auto-serialized to JSON/bytes)
            key: Message key (optional, will be auto-serialized)
            **kwargs: Additional arguments passed to confluent_kafka produce()

        Raises:
            ProducerError: On producer errors (closed producer, invalid topic, etc.)
        """
        if self._closed:
            raise ProducerError("Producer is closed")

        if not topic:
            raise ProducerError("Topic name cannot be empty")

        try:
            producer = self._get_producer()

            # Serialize data
            serialized_key = serialize(key) if key is not None else None
            serialized_value = serialize(value)

            # Send message
            producer.produce(
                topic=topic, value=serialized_value, key=serialized_key, **kwargs
            )
            producer.poll(0)  # Trigger delivery

        except Exception as e:
            if "Local: Queue full" in str(e):
                raise ProducerError(f"Producer queue is full. Try calling flush() or reduce message rate: {str(e)}")
            elif "Local: Message timed out" in str(e):
                raise ProducerError(f"Message delivery timed out. Check Kafka connection: {str(e)}")
            elif "Broker: Unknown topic" in str(e):
                raise ProducerError(f"Topic '{topic}' does not exist or is not accessible: {str(e)}")
            else:
                raise ProducerError(f"Failed to send message to topic '{topic}': {str(e)}")

    def send_batch(self, messages: List[Union[Dict[str, Any], tuple]]) -> List[Dict[str, Any]]:
        """Send multiple messages.

        Args:
            messages: List of messages in format:
                - Dict format: {"topic": "topic", "value": value, "key": key}
                - Tuple format: (topic, value) or (topic, value, key)

        Returns:
            List of results with success/error status for each message
        """
        if self._closed:
            raise ProducerError("Producer is closed")

        producer = self._get_producer()
        results = []

        for msg in messages:
            try:
                # Handle both dict and tuple formats
                if isinstance(msg, tuple):
                    if len(msg) == 2:
                        topic, value = msg
                        key = None
                    elif len(msg) == 3:
                        topic, value, key = msg
                    else:
                        results.append(
                            {"success": False, "error": "Tuple format must be (topic, value) or (topic, value, key)"}
                        )
                        continue
                elif isinstance(msg, dict):
                    topic = msg.get("topic")
                    value = msg.get("value")
                    key = msg.get("key")
                else:
                    results.append(
                        {"success": False, "error": "Message must be dict or tuple format"}
                    )
                    continue

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
                results.append({"success": True, "topic": topic})

            except Exception as e:
                results.append({"success": False, "error": f"Failed to send message: {str(e)}"})

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
        """Poll for a message.

        Args:
            timeout: Maximum time to wait for a message (in seconds)

        Returns:
            Message object if available, None if timeout or no messages

        Raises:
            ConsumerError: On consumer errors (connection issues, invalid config, etc.)
        """
        if self._closed:
            raise ConsumerError("Consumer is closed")

        try:
            consumer = self._get_consumer()
            msg = consumer.poll(timeout)

            if msg is None:
                return None

            if msg.error():
                error_code = msg.error().code()
                if error_code == ConfluentKafkaError._PARTITION_EOF:
                    # End of partition - not an error, just no more messages
                    return None
                elif error_code == ConfluentKafkaError.UNKNOWN_TOPIC_OR_PART:
                    raise ConsumerError(f"Unknown topic or partition: {msg.error()}")
                elif error_code == ConfluentKafkaError._TRANSPORT:
                    raise ConsumerError(f"Transport error (check Kafka connection): {msg.error()}")
                elif error_code == ConfluentKafkaError._AUTHENTICATION:
                    raise ConsumerError(f"Authentication failed: {msg.error()}")
                elif error_code == ConfluentKafkaError._AUTHORIZATION:
                    raise ConsumerError(f"Authorization failed: {msg.error()}")
                else:
                    raise ConsumerError(f"Consumer error [{error_code}]: {msg.error()}")

            return Message(msg)

        except ConsumerError:
            # Re-raise our custom errors
            raise
        except Exception as e:
            raise ConsumerError(f"Unexpected error while polling: {str(e)}")

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

    def send_transactional(self, topic: str, value: Any, key: Any = None, **kwargs: Any) -> None:
        """Send a single message within the current transaction.

        Args:
            topic: Kafka topic name
            value: Message value (will be auto-serialized)
            key: Message key (optional, will be auto-serialized)
            **kwargs: Additional arguments passed to confluent_kafka produce()

        Note:
            This method requires an active transaction. Use begin() first.
        """
        if not self._in_transaction:
            raise TransactionError("No active transaction. Call begin() first.")

        # Use the regular send method which handles serialization
        self.send(topic, value, key, **kwargs)

    def send_batch_transactional(self, messages: List[Union[Dict[str, Any], tuple]]) -> None:
        """Send multiple messages in a single transaction.

        Args:
            messages: List of messages in format:
                - Dict format: {"topic": "topic", "value": value, "key": key}
                - Tuple format: (topic, value) or (topic, value, key)

        This method automatically begins and commits the transaction.
        """
        if self._in_transaction:
            raise TransactionError("Transaction already in progress")

        self.begin()
        try:
            # Process messages similar to send_batch but use send method
            for msg in messages:
                if isinstance(msg, tuple):
                    if len(msg) == 2:
                        topic, value = msg
                        key = None
                    elif len(msg) == 3:
                        topic, value, key = msg
                    else:
                        raise TransactionError("Tuple format must be (topic, value) or (topic, value, key)")
                elif isinstance(msg, dict):
                    topic = msg.get("topic")
                    value = msg.get("value")
                    key = msg.get("key")
                else:
                    raise TransactionError("Message must be dict or tuple format")

                if not topic or value is None:
                    raise TransactionError("Missing topic or value in message")

                self.send(topic, value, key)

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
