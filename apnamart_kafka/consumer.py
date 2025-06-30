"""Core Kafka consumer implementation."""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, AsyncGenerator, Dict, Iterator, List, Optional, Set, Union

from kafka import KafkaConsumer as KafkaConsumerClient  # type: ignore
from kafka.errors import KafkaError  # type: ignore
from kafka.structs import OffsetAndMetadata, TopicPartition  # type: ignore

from .base.client import BaseKafkaClient
from .consumer_config import KafkaConsumerConfig
from .exceptions import (
    ConnectionError,
    KafkaProducerError,
)

logger = logging.getLogger(__name__)


class ConsumerMessage:
    """Represents a consumed message with metadata."""

    def __init__(self, record: Any) -> None:
        """Initialize from kafka-python ConsumerRecord."""
        self.topic = record.topic
        self.partition = record.partition
        self.offset = record.offset
        self.timestamp = record.timestamp
        self.timestamp_type = record.timestamp_type
        self.key = record.key
        self.value = record.value
        self.headers = dict(record.headers) if record.headers else {}

    def __repr__(self) -> str:
        return (
            f"ConsumerMessage(topic='{self.topic}', partition={self.partition}, "
            f"offset={self.offset}, key={self.key})"
        )


class KafkaConsumer(BaseKafkaClient):
    """Generic Kafka consumer with sync and async support."""

    def __init__(
        self, config: Optional[KafkaConsumerConfig] = None, **kwargs: Any
    ) -> None:
        """Initialize the Kafka consumer.

        Args:
            config: Kafka consumer configuration instance
            **kwargs: Additional configuration overrides
        """
        # Initialize base class with proper config type
        self._kafka_config = config or KafkaConsumerConfig(**kwargs)
        super().__init__(self._kafka_config, **kwargs)
        self._consumer: Optional[KafkaConsumerClient] = None
        self._executor: Optional[ThreadPoolExecutor] = None
        self._subscribed_topics: Set[str] = set()
        self._assigned_partitions: Set[TopicPartition] = set()

        # Validate consumer-specific configuration
        self._kafka_config.validate_consumer_config()

    def _create_client(self) -> KafkaConsumerClient:
        """Create the underlying Kafka consumer client."""
        try:
            kafka_config = self._kafka_config.to_kafka_config()
            logger.debug(f"Creating Kafka consumer with config: {kafka_config}")
            client = KafkaConsumerClient(**kafka_config)
            self._notify_connection_established()
            return client
        except Exception as e:
            self._notify_connection_lost(e)
            raise ConnectionError(f"Failed to create Kafka consumer: {e}") from e

    def _get_consumer(self) -> KafkaConsumerClient:
        """Get or create the Kafka consumer instance."""
        if self._consumer is None:
            self._consumer = self._create_client()
        return self._consumer

    def _get_executor(self) -> ThreadPoolExecutor:
        """Get or create the thread pool executor for async operations."""
        if self._executor is None:
            self._executor = ThreadPoolExecutor(
                max_workers=4, thread_name_prefix="kafka-consumer-async"
            )
        return self._executor

    def subscribe(
        self, topics: Union[str, List[str]], pattern: Optional[str] = None
    ) -> None:
        """Subscribe to topics or a topic pattern.

        Args:
            topics: Topic name(s) to subscribe to
            pattern: Regex pattern for topics (alternative to topics list)

        Raises:
            ConnectionError: If subscription fails
        """
        if self._is_closed:
            raise KafkaProducerError("Consumer is closed")

        try:
            consumer = self._get_consumer()

            if pattern:
                # Subscribe to pattern
                logger.info(f"Subscribing to topic pattern: {pattern}")
                consumer.subscribe(pattern=pattern)
                self._subscribed_topics.add(f"pattern:{pattern}")
            else:
                # Subscribe to specific topics
                if isinstance(topics, str):
                    topics = [topics]

                # Add topic prefixes if configured
                full_topics = [
                    self._kafka_config.get_topic_name(topic) for topic in topics
                ]

                logger.info(f"Subscribing to topics: {full_topics}")
                consumer.subscribe(full_topics)
                self._subscribed_topics.update(full_topics)

        except Exception as e:
            logger.error(f"Failed to subscribe to topics: {e}")
            raise ConnectionError(f"Failed to subscribe: {e}") from e

    def assign(self, partitions: List[TopicPartition]) -> None:
        """Manually assign specific topic partitions.

        Args:
            partitions: List of TopicPartition objects to assign

        Raises:
            ConnectionError: If assignment fails
        """
        if self._is_closed:
            raise KafkaProducerError("Consumer is closed")

        try:
            consumer = self._get_consumer()

            # Add topic prefixes to partitions if configured
            prefixed_partitions = []
            for tp in partitions:
                full_topic = self._kafka_config.get_topic_name(tp.topic)
                prefixed_partitions.append(TopicPartition(full_topic, tp.partition))

            logger.info(f"Assigning partitions: {prefixed_partitions}")
            consumer.assign(prefixed_partitions)
            self._assigned_partitions.update(prefixed_partitions)

        except Exception as e:
            logger.error(f"Failed to assign partitions: {e}")
            raise ConnectionError(f"Failed to assign partitions: {e}") from e

    def poll(
        self, timeout_ms: int = 1000, max_records: Optional[int] = None
    ) -> List[ConsumerMessage]:
        """Poll for new messages.

        Args:
            timeout_ms: Maximum time to block waiting for messages
            max_records: Maximum number of records to return (defaults to config setting)

        Returns:
            List of ConsumerMessage objects

        Raises:
            ConnectionError: If polling fails
        """
        if self._is_closed:
            raise KafkaProducerError("Consumer is closed")

        try:
            consumer = self._get_consumer()

            # Use max_records from parameter or config
            records_limit = max_records or self._kafka_config.max_poll_records

            # Poll for messages
            message_batch = consumer.poll(
                timeout_ms=timeout_ms, max_records=records_limit
            )

            # Convert to ConsumerMessage objects
            messages = []
            for topic_partition, records in message_batch.items():
                for record in records:
                    message = ConsumerMessage(record)
                    messages.append(message)

                    # Notify monitoring handlers
                    self._notify_message_received(
                        message.topic,
                        message.key,
                        message.value,
                        {
                            "partition": message.partition,
                            "offset": message.offset,
                            "timestamp": message.timestamp,
                        },
                    )

            logger.debug(f"Polled {len(messages)} messages")
            return messages

        except KafkaError as e:
            logger.error(f"Failed to poll messages: {e}")
            raise ConnectionError(f"Failed to poll messages: {e}") from e

    def consume(self, timeout_ms: int = 1000) -> Iterator[ConsumerMessage]:
        """Consume messages continuously.

        Args:
            timeout_ms: Timeout for each poll operation

        Yields:
            ConsumerMessage objects as they are received

        Raises:
            ConnectionError: If consumption fails
        """
        if self._is_closed:
            raise KafkaProducerError("Consumer is closed")

        try:
            while not self._is_closed:
                messages = self.poll(timeout_ms=timeout_ms)
                for message in messages:
                    yield message

        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except Exception as e:
            logger.error(f"Error during message consumption: {e}")
            raise ConnectionError(f"Consumption failed: {e}") from e

    async def consume_async(
        self, timeout_ms: int = 1000
    ) -> AsyncGenerator[ConsumerMessage, None]:
        """Consume messages asynchronously.

        Args:
            timeout_ms: Timeout for each poll operation

        Yields:
            ConsumerMessage objects as they are received

        Raises:
            ConnectionError: If consumption fails
        """
        if self._is_closed:
            raise KafkaProducerError("Consumer is closed")

        executor = self._get_executor()
        loop = asyncio.get_event_loop()

        try:
            while not self._is_closed:
                # Run poll in thread pool to avoid blocking
                messages = await loop.run_in_executor(executor, self.poll, timeout_ms)

                for message in messages:
                    yield message

                # Allow other async operations to run
                await asyncio.sleep(0)

        except Exception as e:
            logger.error(f"Error during async message consumption: {e}")
            raise ConnectionError(f"Async consumption failed: {e}") from e

    def commit_offsets(
        self, offsets: Optional[Dict[TopicPartition, OffsetAndMetadata]] = None
    ) -> None:
        """Commit message offsets.

        Args:
            offsets: Specific offsets to commit. If None, commits current position.

        Raises:
            ConnectionError: If commit fails
        """
        if self._is_closed:
            raise KafkaProducerError("Consumer is closed")

        try:
            consumer = self._get_consumer()

            if offsets:
                logger.debug(f"Committing specific offsets: {offsets}")
                consumer.commit(offsets)
            else:
                logger.debug("Committing current offsets")
                consumer.commit()

        except Exception as e:
            logger.error(f"Failed to commit offsets: {e}")
            raise ConnectionError(f"Failed to commit offsets: {e}") from e

    async def commit_offsets_async(
        self, offsets: Optional[Dict[TopicPartition, OffsetAndMetadata]] = None
    ) -> None:
        """Commit message offsets asynchronously.

        Args:
            offsets: Specific offsets to commit. If None, commits current position.

        Raises:
            ConnectionError: If commit fails
        """
        if self._is_closed:
            raise KafkaProducerError("Consumer is closed")

        executor = self._get_executor()
        loop = asyncio.get_event_loop()

        # Run commit in thread pool
        await loop.run_in_executor(executor, self.commit_offsets, offsets)

    def seek(self, partition: TopicPartition, offset: int) -> None:
        """Seek to a specific offset in a partition.

        Args:
            partition: TopicPartition to seek in
            offset: Offset to seek to

        Raises:
            ConnectionError: If seek fails
        """
        if self._is_closed:
            raise KafkaProducerError("Consumer is closed")

        try:
            consumer = self._get_consumer()

            # Add topic prefix if configured
            full_topic = self._kafka_config.get_topic_name(partition.topic)
            full_partition = TopicPartition(full_topic, partition.partition)

            logger.debug(f"Seeking to offset {offset} in partition {full_partition}")
            consumer.seek(full_partition, offset)

        except Exception as e:
            logger.error(f"Failed to seek to offset: {e}")
            raise ConnectionError(f"Failed to seek: {e}") from e

    def seek_to_beginning(
        self, partitions: Optional[List[TopicPartition]] = None
    ) -> None:
        """Seek to the beginning of partitions.

        Args:
            partitions: Specific partitions to seek. If None, seeks all assigned partitions.

        Raises:
            ConnectionError: If seek fails
        """
        if self._is_closed:
            raise KafkaProducerError("Consumer is closed")

        try:
            consumer = self._get_consumer()

            if partitions:
                # Add topic prefixes if configured
                full_partitions = []
                for tp in partitions:
                    full_topic = self._kafka_config.get_topic_name(tp.topic)
                    full_partitions.append(TopicPartition(full_topic, tp.partition))

                logger.debug(f"Seeking to beginning of partitions: {full_partitions}")
                consumer.seek_to_beginning(*full_partitions)
            else:
                logger.debug("Seeking to beginning of all assigned partitions")
                consumer.seek_to_beginning()

        except Exception as e:
            logger.error(f"Failed to seek to beginning: {e}")
            raise ConnectionError(f"Failed to seek to beginning: {e}") from e

    def seek_to_end(self, partitions: Optional[List[TopicPartition]] = None) -> None:
        """Seek to the end of partitions.

        Args:
            partitions: Specific partitions to seek. If None, seeks all assigned partitions.

        Raises:
            ConnectionError: If seek fails
        """
        if self._is_closed:
            raise KafkaProducerError("Consumer is closed")

        try:
            consumer = self._get_consumer()

            if partitions:
                # Add topic prefixes if configured
                full_partitions = []
                for tp in partitions:
                    full_topic = self._kafka_config.get_topic_name(tp.topic)
                    full_partitions.append(TopicPartition(full_topic, tp.partition))

                logger.debug(f"Seeking to end of partitions: {full_partitions}")
                consumer.seek_to_end(*full_partitions)
            else:
                logger.debug("Seeking to end of all assigned partitions")
                consumer.seek_to_end()

        except Exception as e:
            logger.error(f"Failed to seek to end: {e}")
            raise ConnectionError(f"Failed to seek to end: {e}") from e

    def get_partition_metadata(
        self, topics: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Get partition metadata for topics.

        Args:
            topics: Topics to get metadata for. If None, gets metadata for all topics.

        Returns:
            Dictionary containing partition metadata

        Raises:
            ConnectionError: If getting metadata fails
        """
        if self._is_closed:
            raise KafkaProducerError("Consumer is closed")

        try:
            consumer = self._get_consumer()

            # Note: topics parameter currently not used, but kept for future enhancement
            metadata = consumer.list_consumer_group_offsets()

            return dict(metadata)

        except Exception as e:
            logger.error(f"Failed to get partition metadata: {e}")
            raise ConnectionError(f"Failed to get metadata: {e}") from e

    def close(self) -> None:
        """Close the consumer and clean up resources."""
        if self._is_closed:
            return

        logger.debug("Closing Kafka consumer")
        self._is_closed = True

        # Close consumer
        if self._consumer is not None:
            try:
                self._consumer.close()
            except Exception as e:
                logger.error(f"Error closing consumer: {e}")
            finally:
                self._consumer = None

        # Shutdown executor
        if self._executor is not None:
            try:
                self._executor.shutdown(wait=True)
            except Exception as e:
                logger.error(f"Error shutting down executor: {e}")
            finally:
                self._executor = None

        # Clear tracking sets
        self._subscribed_topics.clear()
        self._assigned_partitions.clear()

    def __enter__(self) -> "KafkaConsumer":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()

    async def __aenter__(self) -> "KafkaConsumer":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        self.close()

    def get_metrics(self) -> Dict[str, Any]:
        """Get consumer-specific metrics."""
        base_metrics = super().get_metrics()
        base_metrics.update(
            {
                "consumer_type": "KafkaConsumer",
                "has_executor": self._executor is not None,
                "has_consumer": self._consumer is not None,
                "subscribed_topics": list(self._subscribed_topics),
                "assigned_partitions": [str(tp) for tp in self._assigned_partitions],
                "subscription_type": "assigned"
                if self._assigned_partitions
                else "subscribed",
            }
        )
        return base_metrics

    def _notify_message_received(
        self, topic: str, key: Any, value: Any, metadata: Dict[str, Any]
    ) -> None:
        """Notify monitoring handlers of received message."""
        # For now, reuse the message_sent notification for received messages
        # In future, we can add a specific interface method for message received
        for handler in self._monitoring_handlers:
            try:
                # Note: This uses the existing interface method
                # In a future version, we could add on_message_received to IMonitoring
                handler.on_message_sent(topic, key, value, metadata)
            except Exception as e:
                logger.error(f"Error in monitoring handler: {e}")

    @property
    def subscribed_topics(self) -> Set[str]:
        """Get currently subscribed topics."""
        return self._subscribed_topics.copy()

    @property
    def assigned_partitions(self) -> Set[TopicPartition]:
        """Get currently assigned partitions."""
        return self._assigned_partitions.copy()
