#!/usr/bin/env python3
"""
Tests specifically designed to achieve 100% code coverage.
These tests target uncovered error paths and edge cases.
"""

import uuid
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaError

from apnamart_kafka import (
    Config,
    Consumer,
    ConsumerError,
    Producer,
    ProducerError,
    TransactionalProducer,
    TransactionError,
)


@pytest.mark.unit
class TestErrorPathCoverage:
    """Test error paths to increase coverage."""

    def test_producer_queue_full_error(self, test_topic: str, kafka_servers: str):
        """Test producer queue full error handling."""
        config = Config(bootstrap_servers=kafka_servers)

        with patch("apnamart_kafka.client.ConfluentProducer") as mock_producer_class:
            mock_producer = MagicMock()
            mock_producer_class.return_value = mock_producer

            # Simulate queue full error
            mock_producer.produce.side_effect = Exception("Local: Queue full")

            with Producer(config) as producer:
                with pytest.raises(ProducerError, match="Producer queue is full"):
                    producer.send(test_topic, {"test": "queue_full"})

        print(" Producer queue full error path covered")

    def test_producer_timeout_error(self, test_topic: str, kafka_servers: str):
        """Test producer timeout error handling."""
        config = Config(bootstrap_servers=kafka_servers)

        with patch("apnamart_kafka.client.ConfluentProducer") as mock_producer_class:
            mock_producer = MagicMock()
            mock_producer_class.return_value = mock_producer

            # Simulate timeout error
            mock_producer.produce.side_effect = Exception("Local: Message timed out")

            with Producer(config) as producer:
                with pytest.raises(ProducerError, match="Message delivery timed out"):
                    producer.send(test_topic, {"test": "timeout"})

        print(" Producer timeout error path covered")

    def test_producer_unknown_topic_error(self, kafka_servers: str):
        """Test producer unknown topic error handling."""
        config = Config(bootstrap_servers=kafka_servers)

        with patch("apnamart_kafka.client.ConfluentProducer") as mock_producer_class:
            mock_producer = MagicMock()
            mock_producer_class.return_value = mock_producer

            # Simulate unknown topic error
            mock_producer.produce.side_effect = Exception("Broker: Unknown topic")

            with Producer(config) as producer:
                with pytest.raises(
                    ProducerError, match="does not exist or is not accessible"
                ):
                    producer.send("unknown-topic", {"test": "unknown_topic"})

        print(" Producer unknown topic error path covered")

    def test_send_batch_with_closed_producer(self, kafka_servers: str):
        """Test send_batch with closed producer."""
        config = Config(bootstrap_servers=kafka_servers)

        producer = Producer(config)
        producer.close()

        messages = [{"topic": "test", "value": {"test": "closed"}}]

        with pytest.raises(ProducerError, match="Producer is closed"):
            producer.send_batch(messages)

        print(" Send batch with closed producer covered")

    def test_send_batch_invalid_message_formats(
        self, test_topic: str, kafka_servers: str
    ):
        """Test send_batch with various invalid message formats."""
        config = Config(bootstrap_servers=kafka_servers)

        invalid_messages = [
            "string_message",  # Invalid string
            123,  # Invalid number
            ["list", "message"],  # Invalid list
            {"missing_topic": True},  # Missing topic
            {"topic": "", "value": "empty_topic"},  # Empty topic
            {"topic": test_topic},  # Missing value
            ("too", "few", "items", "in", "tuple"),  # Invalid tuple length
        ]

        with Producer(config) as producer:
            results = producer.send_batch(invalid_messages)

            # All should fail
            for i, result in enumerate(results):
                assert result["success"] is False
                assert "error" in result
                print(f" Invalid message {i}: {result['error']}")

    def test_consumer_transport_error(self, test_topic: str, kafka_servers: str):
        """Test consumer transport error handling."""
        config = Config(
            bootstrap_servers=kafka_servers,
            group_id=f"transport-error-{uuid.uuid4().hex[:8]}",
            auto_offset_reset="earliest",
        )

        with patch("apnamart_kafka.client.ConfluentConsumer") as mock_consumer_class:
            mock_consumer = MagicMock()
            mock_consumer_class.return_value = mock_consumer

            # Mock message with transport error
            mock_message = MagicMock()
            mock_message.error.return_value = MagicMock()
            mock_message.error.return_value.code.return_value = KafkaError._TRANSPORT

            mock_consumer.poll.return_value = mock_message

            with Consumer(test_topic, config) as consumer:
                with pytest.raises(ConsumerError, match="Transport error"):
                    consumer.poll(timeout=1.0)

        print(" Consumer transport error covered")

    def test_consumer_authentication_error(self, test_topic: str, kafka_servers: str):
        """Test consumer authentication error handling."""
        config = Config(
            bootstrap_servers=kafka_servers,
            group_id=f"auth-error-{uuid.uuid4().hex[:8]}",
            auto_offset_reset="earliest",
        )

        with patch("apnamart_kafka.client.ConfluentConsumer") as mock_consumer_class:
            mock_consumer = MagicMock()
            mock_consumer_class.return_value = mock_consumer

            # Mock message with authentication error
            mock_message = MagicMock()
            mock_message.error.return_value = MagicMock()
            mock_message.error.return_value.code.return_value = (
                KafkaError.SASL_AUTHENTICATION_FAILED
            )

            mock_consumer.poll.return_value = mock_message

            with Consumer(test_topic, config) as consumer:
                with pytest.raises(ConsumerError, match="Authentication failed"):
                    consumer.poll(timeout=1.0)

        print(" Consumer authentication error covered")

    def test_consumer_authorization_error(self, test_topic: str, kafka_servers: str):
        """Test consumer authorization error handling."""
        config = Config(
            bootstrap_servers=kafka_servers,
            group_id=f"authz-error-{uuid.uuid4().hex[:8]}",
            auto_offset_reset="earliest",
        )

        with patch("apnamart_kafka.client.ConfluentConsumer") as mock_consumer_class:
            mock_consumer = MagicMock()
            mock_consumer_class.return_value = mock_consumer

            # Mock message with authorization error
            mock_message = MagicMock()
            mock_message.error.return_value = MagicMock()
            mock_message.error.return_value.code.return_value = (
                KafkaError.TOPIC_AUTHORIZATION_FAILED
            )

            mock_consumer.poll.return_value = mock_message

            with Consumer(test_topic, config) as consumer:
                with pytest.raises(ConsumerError, match="Authorization failed"):
                    consumer.poll(timeout=1.0)

        print(" Consumer authorization error covered")

    def test_consumer_generic_error(self, test_topic: str, kafka_servers: str):
        """Test consumer generic error handling."""
        config = Config(
            bootstrap_servers=kafka_servers,
            group_id=f"generic-error-{uuid.uuid4().hex[:8]}",
            auto_offset_reset="earliest",
        )

        with patch("apnamart_kafka.client.ConfluentConsumer") as mock_consumer_class:
            mock_consumer = MagicMock()
            mock_consumer_class.return_value = mock_consumer

            # Mock message with generic error
            mock_message = MagicMock()
            mock_message.error.return_value = MagicMock()
            mock_message.error.return_value.code.return_value = (
                999  # Generic error code
            )

            mock_consumer.poll.return_value = mock_message

            with Consumer(test_topic, config) as consumer:
                with pytest.raises(ConsumerError, match="Consumer error \\[999\\]"):
                    consumer.poll(timeout=1.0)

        print(" Consumer generic error covered")

    def test_consumer_commit_with_closed_consumer(
        self, test_topic: str, kafka_servers: str
    ):
        """Test consumer commit with closed consumer."""
        config = Config(
            bootstrap_servers=kafka_servers,
            group_id=f"closed-commit-{uuid.uuid4().hex[:8]}",
            auto_offset_reset="earliest",
        )

        consumer = Consumer(test_topic, config)
        consumer.close()

        with pytest.raises(ConsumerError, match="Consumer is closed"):
            consumer.commit()

        print(" Commit with closed consumer covered")

    def test_transactional_producer_uncovered_paths(
        self, test_topic: str, kafka_servers: str
    ):
        """Test uncovered paths in transactional producer."""
        tx_id = f"coverage-tx-{uuid.uuid4().hex[:8]}"
        config = Config(bootstrap_servers=kafka_servers)

        # Test transaction failure during batch send
        with patch("apnamart_kafka.client.ConfluentProducer") as mock_producer_class:
            mock_producer = MagicMock()
            mock_producer_class.return_value = mock_producer

            # Simulate error during batch transaction
            mock_producer.produce.side_effect = Exception("Simulated batch error")

            with TransactionalProducer(tx_id, config) as tx_producer:
                messages = [(test_topic, {"batch_error": True})]

                with pytest.raises(TransactionError, match="Transaction failed"):
                    tx_producer.send_batch_transactional(messages)

        print(" Transactional producer batch error covered")

    def test_message_creation_edge_cases(self, test_topic: str, kafka_servers: str):
        """Test message creation with edge cases."""
        from unittest.mock import MagicMock

        from apnamart_kafka.client import Message

        # Test message without timestamp
        mock_record = MagicMock()
        mock_record.topic.return_value = test_topic
        mock_record.partition.return_value = 0
        mock_record.offset.return_value = 100
        mock_record.key.return_value = b"test-key"
        mock_record.value.return_value = b'{"no_timestamp": true}'
        mock_record.timestamp.return_value = (-1, None)  # No timestamp
        mock_record.headers.return_value = []

        msg_without_ts = Message(mock_record)

        assert msg_without_ts.timestamp is None
        repr_str = repr(msg_without_ts)
        assert test_topic in repr_str

        print(" Message creation edge cases covered")

    def test_config_edge_cases(self):
        """Test configuration edge cases."""
        # Test config with all parameters
        config = Config(
            bootstrap_servers="test:9092",
            acks="1",
            retries=5,
            compression_type="snappy",
            batch_size=32768,
            linger_ms=10,
            group_id="test-group",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            session_timeout_ms=6000,
            heartbeat_interval_ms=3000,
        )

        producer_config = config.to_producer_config()
        consumer_config = config.to_consumer_config()

        assert producer_config["compression.type"] == "snappy"
        assert consumer_config["enable.auto.commit"] is False

        print(" Config edge cases covered")


class TestConnectionPoolingCoverage:
    """Test connection pooling paths for coverage."""

    def test_producer_connection_reuse(self, test_topic: str, kafka_servers: str):
        """Test producer connection reuse logic."""
        config = Config(bootstrap_servers=kafka_servers)

        with Producer(config) as producer:
            # Send multiple messages to test connection reuse
            for i in range(3):
                producer.send(test_topic, {"reuse_test": i})

            producer.flush()

            # Test _get_producer multiple times
            p1 = producer._get_producer()
            p2 = producer._get_producer()
            assert p1 is p2  # Should be the same instance

        print(" Producer connection reuse covered")

    def test_consumer_connection_reuse(self, test_topic: str, kafka_servers: str):
        """Test consumer connection reuse logic."""
        config = Config(
            bootstrap_servers=kafka_servers,
            group_id=f"reuse-test-{uuid.uuid4().hex[:8]}",
            auto_offset_reset="latest",
        )

        with Consumer(test_topic, config) as consumer:
            # Test _get_consumer multiple times
            c1 = consumer._get_consumer()
            c2 = consumer._get_consumer()
            assert c1 is c2  # Should be the same instance

            # Test polling (might timeout or error, that's OK for this test)
            try:
                consumer.poll(timeout=0.1)
            except ConsumerError:
                pass  # Expected for non-existent topic

        print(" Consumer connection reuse covered")


if __name__ == "__main__":
    print(" Running 100% Coverage Tests")
    print("=" * 40)
    print("These tests target uncovered code paths to achieve 100% coverage.")
    print("Run with: uv run pytest tests/test_100_percent_coverage.py -v")
