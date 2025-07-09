"""Transactional Kafka producer implementation."""

import logging
from typing import Any, Dict, List, Optional, Union

from kafka import KafkaProducer as KafkaClient  # type: ignore
from kafka.errors import KafkaError  # type: ignore

from .config import KafkaConfig
from .exceptions import (
    ConnectionError,
    KafkaProducerError,
    PublishError,
)
from .producer import KafkaProducer
from .serializers import Serializer

logger = logging.getLogger(__name__)


class TransactionalProducer(KafkaProducer):
    """Kafka producer with transaction support for exactly-once semantics."""

    def __init__(
        self,
        config: Optional[KafkaConfig] = None,
        transactional_id: Optional[str] = None,
        **kwargs: Any
    ) -> None:
        """Initialize the transactional Kafka producer.

        Args:
            config: Kafka configuration instance
            transactional_id: Unique transactional ID for this producer
            **kwargs: Additional configuration overrides
        """
        if not transactional_id:
            raise ValueError("transactional_id is required for TransactionalProducer")

        self._transactional_id = transactional_id
        self._in_transaction = False

        # Initialize base class
        super().__init__(config, **kwargs)

    def _create_client(self) -> KafkaClient:
        """Create the underlying Kafka producer client with transaction support."""
        try:
            kafka_config = self._kafka_config.to_kafka_config()
            
            # Add transactional configuration
            kafka_config.update({
                "transactional_id": self._transactional_id,
                "enable_idempotence": True,  # Required for transactions
                "acks": "all",  # Required for transactions
                "retries": 10,  # Higher retries for reliability
                "max_in_flight_requests_per_connection": 5,  # Kafka default for idempotent producer
            })

            logger.debug(f"Creating transactional Kafka producer with id: {self._transactional_id}")
            client = KafkaClient(**kafka_config)
            
            # Initialize transactions
            client.init_transactions()
            logger.info(f"Transactional producer initialized with id: {self._transactional_id}")
            
            self._notify_connection_established()
            return client
        except Exception as e:
            self._notify_connection_lost(e)
            raise ConnectionError(f"Failed to create transactional Kafka producer: {e}") from e

    def begin_transaction(self) -> None:
        """Begin a new transaction.

        Raises:
            KafkaProducerError: If already in a transaction or producer is closed
        """
        if self._is_closed:
            raise KafkaProducerError("Producer is closed")

        if self._in_transaction:
            raise KafkaProducerError("Transaction already in progress")

        try:
            producer = self._get_producer()
            producer.begin_transaction()
            self._in_transaction = True
            logger.debug("Transaction started")
        except Exception as e:
            logger.error(f"Failed to begin transaction: {e}")
            raise KafkaProducerError(f"Failed to begin transaction: {e}") from e

    def commit_transaction(self) -> None:
        """Commit the current transaction.

        Raises:
            KafkaProducerError: If not in a transaction or commit fails
        """
        if self._is_closed:
            raise KafkaProducerError("Producer is closed")

        if not self._in_transaction:
            raise KafkaProducerError("No transaction in progress")

        try:
            producer = self._get_producer()
            producer.commit_transaction()
            self._in_transaction = False
            logger.debug("Transaction committed")
        except Exception as e:
            self._in_transaction = False
            logger.error(f"Failed to commit transaction: {e}")
            raise KafkaProducerError(f"Failed to commit transaction: {e}") from e

    def abort_transaction(self) -> None:
        """Abort the current transaction.

        Raises:
            KafkaProducerError: If not in a transaction or abort fails
        """
        if self._is_closed:
            raise KafkaProducerError("Producer is closed")

        if not self._in_transaction:
            raise KafkaProducerError("No transaction in progress")

        try:
            producer = self._get_producer()
            producer.abort_transaction()
            self._in_transaction = False
            logger.debug("Transaction aborted")
        except Exception as e:
            self._in_transaction = False
            logger.error(f"Failed to abort transaction: {e}")
            raise KafkaProducerError(f"Failed to abort transaction: {e}") from e

    def send_transactional_batch(
        self,
        messages: List[Dict[str, Any]],
        serializer: Union[str, Serializer] = "json",
    ) -> None:
        """Send a batch of messages within a transaction.

        This method handles the complete transaction lifecycle automatically.

        Args:
            messages: List of message dictionaries (see send_batch for format)
            serializer: Default serializer for all messages

        Raises:
            PublishError: If any message fails to send
            KafkaProducerError: If transaction operations fail
        """
        if self._is_closed:
            raise KafkaProducerError("Producer is closed")

        if self._in_transaction:
            raise KafkaProducerError("Cannot start new transaction while one is in progress")

        self.begin_transaction()
        try:
            # Send all messages
            for msg in messages:
                topic = msg.get("topic")
                value = msg.get("value")
                
                if not topic or value is None:
                    raise ValueError(f"Invalid message in batch: {msg}")

                self.send(
                    topic=topic,
                    value=value,
                    key=msg.get("key"),
                    partition=msg.get("partition"),
                    timestamp_ms=msg.get("timestamp_ms"),
                    headers=msg.get("headers"),
                    serializer=msg.get("serializer", serializer),
                )

            # Commit if all messages sent successfully
            self.commit_transaction()
            logger.info(f"Successfully sent {len(messages)} messages in transaction")

        except Exception as e:
            logger.error(f"Transaction failed, aborting: {e}")
            self.abort_transaction()
            raise PublishError(f"Transactional batch send failed: {e}") from e

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
        """Send a message to Kafka.

        If called within a transaction, the message will be part of that transaction.
        Otherwise, it behaves like a regular send.

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
        # Use parent's send method which will use our transactional producer
        super().send(topic, value, key, partition, timestamp_ms, headers, serializer)

    def close(self) -> None:
        """Close the producer and clean up resources."""
        if self._is_closed:
            return

        # Abort any pending transaction
        if self._in_transaction:
            try:
                self.abort_transaction()
            except Exception as e:
                logger.error(f"Error aborting transaction during close: {e}")

        # Call parent's close
        super().close()

    def get_metrics(self) -> Dict[str, Any]:
        """Get producer-specific metrics."""
        base_metrics = super().get_metrics()
        base_metrics.update(
            {
                "producer_type": "TransactionalProducer",
                "transactional_id": self._transactional_id,
                "in_transaction": self._in_transaction,
            }
        )
        return base_metrics

    def __enter__(self) -> "TransactionalProducer":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit with automatic transaction handling."""
        if self._in_transaction:
            if exc_type is None:
                # No exception, try to commit
                try:
                    self.commit_transaction()
                except Exception as e:
                    logger.error(f"Failed to commit transaction on exit: {e}")
            else:
                # Exception occurred, abort transaction
                try:
                    self.abort_transaction()
                except Exception as e:
                    logger.error(f"Failed to abort transaction on exit: {e}")
        
        self.close()

    async def __aenter__(self) -> "TransactionalProducer":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        self.__exit__(exc_type, exc_val, exc_tb)