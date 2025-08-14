"""Core Kafka producer implementation."""

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor, Future
from typing import Any, Dict, List, Optional, Union

from confluent_kafka import Producer as KafkaClient, KafkaError, KafkaException  # type: ignore

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

            # Prepare headers for confluent-kafka format
            confluent_headers = None
            if headers:
                confluent_headers = [(k, v) for k, v in headers.items()]

            # Store delivery result for error handling
            delivery_error = None
            delivery_success = False

            # Create delivery callback for error handling
            def delivery_callback(err, msg):
                nonlocal delivery_error, delivery_success
                if err is not None:
                    delivery_error = err
                    logger.error(f"Failed to send message to topic '{topic}': {err}")
                    self._notify_message_failed(full_topic, key, value, err)
                else:
                    delivery_success = True
                    logger.debug(
                        f"Message sent successfully to {msg.topic()}:"
                        f"{msg.partition()}:{msg.offset()}"
                    )
                    # Notify monitoring handlers
                    self._notify_message_sent(
                        full_topic,
                        key,
                        value,
                        {
                            "partition": msg.partition(),
                            "offset": msg.offset(),
                            "timestamp": msg.timestamp()[1] if msg.timestamp()[0] != -1 else None,
                        },
                    )

            # Send the message with confluent-kafka
            produce_kwargs = {
                "topic": full_topic,
                "value": serialized_value,
                "key": serialized_key,
                "callback": delivery_callback,
            }
            
            # Only add optional parameters if they are not None
            if partition is not None:
                produce_kwargs["partition"] = partition
            if timestamp_ms is not None:
                produce_kwargs["timestamp"] = timestamp_ms
            if confluent_headers is not None:
                produce_kwargs["headers"] = confluent_headers
                
            producer.produce(**produce_kwargs)

            # Poll for delivery reports to trigger callbacks and wait for completion
            timeout = self._kafka_config.request_timeout_ms / 1000
            start_time = time.time()
            while not delivery_success and not delivery_error and (time.time() - start_time) < timeout:
                producer.poll(0.1)

            # Check for delivery errors
            if delivery_error:
                raise PublishError(f"Failed to send message: {delivery_error}")
            elif not delivery_success:
                raise PublishError("Message delivery timed out")

        except (KafkaError, KafkaException) as e:
            logger.error(f"Failed to send message to topic '{topic}': {e}")
            # Only call _notify_message_failed if full_topic was defined
            if 'full_topic' in locals():
                self._notify_message_failed(full_topic, key, value, e)
            raise PublishError(f"Failed to send message: {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error sending message to topic '{topic}': {e}")
            # Only call _notify_message_failed if full_topic was defined
            if 'full_topic' in locals():
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

    def send_batch(
        self,
        messages: List[Dict[str, Any]],
        serializer: Union[str, Serializer] = "json",
    ) -> List[Dict[str, Any]]:
        """Send multiple messages efficiently (synchronous).

        Args:
            messages: List of message dictionaries with keys:
                - topic: Topic name (required)
                - value: Message value (required)
                - key: Message key (optional)
                - partition: Partition to send to (optional)
                - timestamp_ms: Message timestamp (optional)
                - headers: Message headers (optional)
            serializer: Default serializer for all messages

        Returns:
            List of results for each message (with success/error info)

        Raises:
            PublishError: If message publishing fails
        """
        if self._is_closed:
            raise KafkaProducerError("Producer is closed")

        results = []
        producer = self._get_producer()
        delivery_results = {}  # Map to track delivery results

        for i, msg in enumerate(messages):
            try:
                topic = msg.get("topic")
                if not topic:
                    results.append({"success": False, "error": "Topic is required for each message"})
                    continue

                value = msg.get("value")
                if value is None:
                    results.append({"success": False, "error": "Value is required for each message"})
                    continue

                full_topic = self._config.get_topic_name(topic)
                msg_serializer = msg.get("serializer", serializer)

                # Serialize key and value
                serialized_key = serialize_key(msg.get("key"))
                serialized_value = serialize_value(value, msg_serializer)

                # Prepare headers for confluent-kafka format
                confluent_headers = None
                if msg.get("headers"):
                    confluent_headers = [(k, v) for k, v in msg.get("headers").items()]

                # Create delivery callback for this message
                def delivery_callback(err, kafka_msg, msg_index=i):
                    if err is not None:
                        delivery_results[msg_index] = {"success": False, "error": str(err)}
                    else:
                        delivery_results[msg_index] = {
                            "success": True, 
                            "partition": kafka_msg.partition(),
                            "offset": kafka_msg.offset(),
                            "timestamp": kafka_msg.timestamp()[1] if kafka_msg.timestamp()[0] != -1 else None,
                        }

                # Send the message
                produce_kwargs = {
                    "topic": full_topic,
                    "value": serialized_value,
                    "key": serialized_key,
                    "callback": delivery_callback,
                }
                
                # Only add optional parameters if they are not None
                if msg.get("partition") is not None:
                    produce_kwargs["partition"] = msg.get("partition")
                if msg.get("timestamp_ms") is not None:
                    produce_kwargs["timestamp"] = msg.get("timestamp_ms")
                if confluent_headers is not None:
                    produce_kwargs["headers"] = confluent_headers
                    
                producer.produce(**produce_kwargs)
                
                # Initialize result slot
                results.append({"pending": True})

            except Exception as e:
                logger.error(f"Failed to send batch message: {e}")
                results.append({"success": False, "error": str(e)})

        # Poll for all delivery reports
        timeout = self._kafka_config.request_timeout_ms / 1000
        start_time = time.time()
        
        while len(delivery_results) < len([r for r in results if r.get("pending")]) and (time.time() - start_time) < timeout:
            producer.poll(0.1)

        # Update results with delivery outcomes
        for i, result in enumerate(results):
            if result.get("pending"):
                if i in delivery_results:
                    results[i] = delivery_results[i]
                else:
                    results[i] = {"success": False, "error": "Delivery timed out"}

        return results

    async def send_batch_async(
        self,
        messages: List[Dict[str, Any]],
        serializer: Union[str, Serializer] = "json",
    ) -> List[Any]:
        """Send multiple messages efficiently (asynchronous).

        Args:
            messages: List of message dictionaries (see send_batch for format)
            serializer: Default serializer for all messages

        Returns:
            List of results for each message

        Raises:
            PublishError: If message publishing fails
        """
        if self._is_closed:
            raise KafkaProducerError("Producer is closed")

        # Create tasks for all messages
        tasks = []
        for msg in messages:
            topic = msg.get("topic")
            value = msg.get("value")
            
            if not topic or value is None:
                logger.error(f"Invalid message in batch: {msg}")
                continue

            task = self.send_async(
                topic=topic,
                value=value,
                key=msg.get("key"),
                partition=msg.get("partition"),
                timestamp_ms=msg.get("timestamp_ms"),
                headers=msg.get("headers"),
                serializer=msg.get("serializer", serializer),
            )
            tasks.append(task)

        # Wait for all messages to be sent
        return await asyncio.gather(*tasks, return_exceptions=True)

    def flush(self, timeout: Optional[float] = None) -> None:
        """Flush any pending messages.

        Args:
            timeout: Maximum time to wait for messages to be sent
        """
        if self._producer is not None:
            try:
                logger.debug("Flushing producer")
                # confluent-kafka flush returns number of messages still in queue
                # If timeout is None, use a reasonable default
                flush_timeout = timeout if timeout is not None else 10.0
                remaining = self._producer.flush(timeout=flush_timeout)
                if remaining > 0:
                    logger.warning(f"Flush timed out, {remaining} messages still in queue")
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
                # confluent-kafka Producer doesn't have close(), just flush and clear reference
            except Exception as e:
                logger.error(f"Error flushing producer during close: {e}")
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
