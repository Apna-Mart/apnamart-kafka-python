"""Core Kafka producer implementation."""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Optional, Union

from kafka import KafkaProducer as KafkaClient  # type: ignore
from kafka.errors import KafkaError  # type: ignore

from .base.client import BaseKafkaClient
from .config import KafkaConfig
from .exceptions import (
    ConnectionError,
    KafkaProducerError,
    PublishError,
)
from .serializers import Serializer, serialize_key, serialize_value

logger = logging.getLogger(__name__)


class KafkaProducer(BaseKafkaClient):
    """Generic Kafka producer with sync and async support."""

    def __init__(self, config: Optional[KafkaConfig] = None, **kwargs: Any) -> None:
        """Initialize the Kafka producer.

        Args:
            config: Kafka configuration instance
            **kwargs: Additional configuration overrides
        """
        # Initialize base class with proper config type
        self._kafka_config = config or KafkaConfig(**kwargs)
        super().__init__(self._kafka_config, **kwargs)
        self._producer: Optional[KafkaClient] = None
        self._executor: Optional[ThreadPoolExecutor] = None

    def _create_client(self) -> KafkaClient:
        """Create the underlying Kafka producer client."""
        try:
            kafka_config = self._kafka_config.to_kafka_config()
            logger.debug(f"Creating Kafka producer with config: {kafka_config}")
            client = KafkaClient(**kafka_config)
            self._notify_connection_established()
            return client
        except Exception as e:
            self._notify_connection_lost(e)
            raise ConnectionError(f"Failed to create Kafka producer: {e}") from e

    def _get_producer(self) -> KafkaClient:
        """Get or create the Kafka producer instance."""
        if self._producer is None:
            self._producer = self._create_client()
        return self._producer

    def _get_executor(self) -> ThreadPoolExecutor:
        """Get or create the thread pool executor for async operations."""
        if self._executor is None:
            self._executor = ThreadPoolExecutor(
                max_workers=4, thread_name_prefix="kafka-async"
            )
        return self._executor

    def send(
        self,
        topic: str,
        value: Any,
        key: Any = None,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        headers: Optional[Dict[str, bytes]] = None,
        serializer: Union[str, Serializer] = "json",
    ) -> None:
        """Send a message to Kafka (synchronous).

        Args:
            topic: Topic name
            value: Message value
            key: Message key (optional)
            partition: Partition to send to (optional)
            timestamp_ms: Message timestamp in milliseconds (optional)
            headers: Message headers (optional)
            serializer: Serializer to use for value

        Raises:
            PublishError: If message publishing fails
        """
        if self._is_closed:
            raise KafkaProducerError("Producer is closed")

        try:
            producer = self._get_producer()
            full_topic = self._config.get_topic_name(topic)

            # Serialize key and value
            serialized_key = serialize_key(key)
            serialized_value = serialize_value(value, serializer)

            logger.debug(f"Sending message to topic '{full_topic}' with key '{key}'")

            # Send the message
            future = producer.send(
                topic=full_topic,
                value=serialized_value,
                key=serialized_key,
                partition=partition,
                timestamp_ms=timestamp_ms,
                headers=headers,
            )

            # Wait for the message to be sent
            record_metadata = future.get(
                timeout=self._kafka_config.request_timeout_ms / 1000
            )
            logger.debug(
                f"Message sent successfully to {record_metadata.topic}:"
                f"{record_metadata.partition}:{record_metadata.offset}"
            )

            # Notify monitoring handlers
            self._notify_message_sent(
                full_topic,
                key,
                value,
                {
                    "partition": record_metadata.partition,
                    "offset": record_metadata.offset,
                    "timestamp": record_metadata.timestamp,
                },
            )

        except KafkaError as e:
            logger.error(f"Failed to send message to topic '{topic}': {e}")
            self._notify_message_failed(full_topic, key, value, e)
            raise PublishError(f"Failed to send message: {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error sending message to topic '{topic}': {e}")
            self._notify_message_failed(full_topic, key, value, e)
            raise PublishError(f"Unexpected error: {e}") from e

    async def send_async(
        self,
        topic: str,
        value: Any,
        key: Any = None,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        headers: Optional[Dict[str, bytes]] = None,
        serializer: Union[str, Serializer] = "json",
    ) -> None:
        """Send a message to Kafka (asynchronous).

        Args:
            topic: Topic name
            value: Message value
            key: Message key (optional)
            partition: Partition to send to (optional)
            timestamp_ms: Message timestamp in milliseconds (optional)
            headers: Message headers (optional)
            serializer: Serializer to use for value

        Raises:
            PublishError: If message publishing fails
        """
        if self._is_closed:
            raise KafkaProducerError("Producer is closed")

        executor = self._get_executor()
        loop = asyncio.get_event_loop()

        # Run the sync send method in a thread pool
        await loop.run_in_executor(
            executor,
            self.send,
            topic,
            value,
            key,
            partition,
            timestamp_ms,
            headers,
            serializer,
        )

    def flush(self, timeout: Optional[float] = None) -> None:
        """Flush any pending messages.

        Args:
            timeout: Maximum time to wait for messages to be sent
        """
        if self._producer is not None:
            try:
                logger.debug("Flushing producer")
                self._producer.flush(timeout=timeout)
            except Exception as e:
                logger.error(f"Error flushing producer: {e}")
                raise KafkaProducerError(f"Failed to flush producer: {e}") from e

    def close(self) -> None:
        """Close the producer and clean up resources."""
        if self._is_closed:
            return

        logger.debug("Closing Kafka producer")
        self._is_closed = True

        # Flush pending messages
        if self._producer is not None:
            try:
                self._producer.flush()
                self._producer.close()
            except Exception as e:
                logger.error(f"Error closing producer: {e}")
            finally:
                self._producer = None

        # Shutdown executor
        if self._executor is not None:
            try:
                self._executor.shutdown(wait=True)
            except Exception as e:
                logger.error(f"Error shutting down executor: {e}")
            finally:
                self._executor = None

    def __enter__(self) -> "KafkaProducer":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()

    async def __aenter__(self) -> "KafkaProducer":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        self.close()

    def get_metrics(self) -> Dict[str, Any]:
        """Get producer-specific metrics."""
        base_metrics = super().get_metrics()
        base_metrics.update(
            {
                "producer_type": "KafkaProducer",
                "has_executor": self._executor is not None,
                "has_producer": self._producer is not None,
            }
        )
        return base_metrics
