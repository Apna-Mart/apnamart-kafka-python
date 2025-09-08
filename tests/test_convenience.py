"""Tests for convenience functions."""

import time
import uuid
from unittest.mock import patch

import pytest

from apnamart_kafka import consume, send


class TestConvenienceFunctionsUnit:
    """Unit tests for convenience functions."""

    @patch("apnamart_kafka.client.Producer")
    def test_send_function_calls_producer(self, mock_producer_class):
        """Test send function creates and uses producer."""
        mock_producer = mock_producer_class.return_value.__enter__.return_value

        send("test-topic", {"test": "data"}, key="test-key", servers="broker:9092")

        # Verify producer was created with correct config
        mock_producer_class.assert_called_once()
        config = mock_producer_class.call_args[0][0]
        assert config.bootstrap_servers == "broker:9092"

        # Verify send was called
        mock_producer.send.assert_called_once_with("test-topic", {"test": "data"}, "test-key")

    @patch("apnamart_kafka.client.Producer")
    def test_send_function_default_servers(self, mock_producer_class):
        """Test send function with default servers."""
        send("test-topic", {"test": "data"})

        config = mock_producer_class.call_args[0][0]
        assert config.bootstrap_servers == "localhost:9092"

    @patch("apnamart_kafka.client.Consumer")
    def test_consume_function_creates_consumer(self, mock_consumer_class):
        """Test consume function creates consumer."""
        mock_consumer = mock_consumer_class.return_value.__enter__.return_value
        mock_consumer.__iter__.return_value = iter([])

        # Consume from function (will be empty due to mock)
        list(consume("test-topic", servers="broker:9092", group_id="test-group"))

        # Verify consumer was created
        mock_consumer_class.assert_called_once()
        args = mock_consumer_class.call_args[0]
        assert args[0] == "test-topic"  # topics

        config = args[1]  # config
        assert config.bootstrap_servers == "broker:9092"
        assert config.group_id == "test-group"

    @patch("apnamart_kafka.client.Consumer")
    def test_consume_function_multiple_topics(self, mock_consumer_class):
        """Test consume function with multiple topics."""
        mock_consumer = mock_consumer_class.return_value.__enter__.return_value
        mock_consumer.__iter__.return_value = iter([])

        topics = ["topic1", "topic2"]
        list(consume(topics, group_id="test-group"))

        args = mock_consumer_class.call_args[0]
        assert args[0] == topics

    @patch("apnamart_kafka.client.Consumer")
    def test_consume_function_default_values(self, mock_consumer_class):
        """Test consume function with default values."""
        mock_consumer = mock_consumer_class.return_value.__enter__.return_value
        mock_consumer.__iter__.return_value = iter([])

        list(consume("test-topic"))

        config = mock_consumer_class.call_args[0][1]
        assert config.bootstrap_servers == "localhost:9092"
        assert config.group_id == "default"


@pytest.mark.integration
class TestConvenienceFunctionsIntegration:
    """Integration tests for convenience functions."""

    def test_send_and_consume_integration(self, kafka_servers: str):
        """Test send and consume functions together."""
        topic = f"convenience-{uuid.uuid4().hex[:8]}-{int(time.time())}"
        group = f"conv-group-{uuid.uuid4().hex[:8]}"
        test_data = {"convenience": "integration", "timestamp": time.time()}

        # Send message using convenience function
        send(topic, test_data, servers=kafka_servers)

        # Consume message using convenience function
        messages = []
        start_time = time.time()

        for message in consume(
            topic,
            servers=kafka_servers,
            group_id=group,
            auto_offset_reset="earliest"
        ):
            messages.append(message.value)
            if len(messages) >= 1 or (time.time() - start_time) > 10:
                break

        assert len(messages) >= 1
        assert messages[0]["convenience"] == "integration"

    def test_send_with_custom_config(self, kafka_servers: str):
        """Test send function with custom configuration."""
        topic = f"send-custom-{uuid.uuid4().hex[:8]}"

        # Send with custom settings
        send(
            topic,
            {"custom": "config"},
            servers=kafka_servers,
            acks="all",
            retries=5,
            compression_type="gzip"
        )

    def test_consume_with_custom_config(self, kafka_servers: str):
        """Test consume function with custom configuration."""
        topic = f"consume-custom-{uuid.uuid4().hex[:8]}"
        group = f"custom-group-{uuid.uuid4().hex[:8]}"

        # First send a message
        send(topic, {"custom": "consume"}, servers=kafka_servers)

        # Consume with custom settings
        messages = []
        for message in consume(
            topic,
            servers=kafka_servers,
            group_id=group,
            auto_offset_reset="earliest",
            enable_auto_commit=False
        ):
            messages.append(message.value)
            if len(messages) >= 1:
                break

        assert len(messages) >= 1

    def test_send_different_data_types(self, kafka_servers: str):
        """Test send function with different data types."""
        topic = f"send-types-{uuid.uuid4().hex[:8]}"

        # Test different data types
        send(topic, "string message", servers=kafka_servers)
        send(topic, {"json": "object"}, servers=kafka_servers)
        send(topic, 42, servers=kafka_servers)
        send(topic, [1, 2, 3], servers=kafka_servers)

    def test_send_with_key(self, kafka_servers: str):
        """Test send function with message key."""
        topic = f"send-key-{uuid.uuid4().hex[:8]}"

        send(
            topic,
            {"message": "with key"},
            key="test-key",
            servers=kafka_servers
        )

    def test_consume_iterator_behavior(self, kafka_servers: str):
        """Test consume function iterator behavior."""
        topic = f"consume-iter-{uuid.uuid4().hex[:8]}"
        group = f"iter-group-{uuid.uuid4().hex[:8]}"

        # Send multiple messages
        for i in range(3):
            send(topic, {"iter": i}, servers=kafka_servers)

        # Consume and verify iterator behavior
        messages = []
        count = 0
        for message in consume(
            topic,
            servers=kafka_servers,
            group_id=group,
            auto_offset_reset="earliest"
        ):
            messages.append(message.value)
            count += 1
            if count >= 3:
                break  # Stop iteration manually

        assert len(messages) == 3
        for i in range(3):
            assert any(msg.get("iter") == i for msg in messages)
