"""Unit tests for convenience functions."""

from unittest.mock import patch

import pytest

from apnamart_kafka import consume, send


@pytest.mark.unit
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
        mock_producer.send.assert_called_once_with(
            "test-topic", {"test": "data"}, "test-key"
        )

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
