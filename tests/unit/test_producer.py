"""Unit tests for Producer class functionality."""

from unittest.mock import Mock, patch

import pytest

from apnamart_kafka import Config, Producer, ProducerError


class TestProducer:
    """Test Producer class functionality with mocked dependencies."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock configuration."""
        return Config(bootstrap_servers="test:9092")

    @patch("apnamart_kafka.client.ConfluentProducer")
    def test_producer_init_default_config(self, mock_producer_class):
        """Test producer initialization with default config."""
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer

        producer = Producer()

        assert not producer._closed
        assert producer._producer is None  # Lazy initialization

    @patch("apnamart_kafka.client.ConfluentProducer")
    def test_producer_init_custom_config(self, mock_producer_class, mock_config):
        """Test producer initialization with custom config."""
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer

        producer = Producer(mock_config)

        assert not producer._closed
        assert producer._producer is None

    @patch("apnamart_kafka.client.ConfluentProducer")
    def test_producer_init_kwargs(self, mock_producer_class):
        """Test producer initialization with keyword arguments."""
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer

        producer = Producer(bootstrap_servers="custom:9092", acks="1")

        assert not producer._closed

    @patch("apnamart_kafka.client.ConfluentProducer")
    def test_producer_context_manager(self, mock_producer_class):
        """Test producer as context manager."""
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer

        with Producer() as producer:
            assert not producer._closed

        assert producer._closed

    @patch("apnamart_kafka.client.ConfluentProducer")
    def test_producer_send_closed_error(self, mock_producer_class):
        """Test sending with closed producer raises error."""
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer

        producer = Producer()
        producer.close()

        with pytest.raises(ProducerError, match="Producer is closed"):
            producer.send("test-topic", {"data": "test"})

    @patch("apnamart_kafka.client.ConfluentProducer")
    def test_producer_get_producer_closed_error(self, mock_producer_class):
        """Test accessing producer when closed raises error."""
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer

        producer = Producer()
        producer.close()

        with pytest.raises(ProducerError, match="Producer is closed"):
            producer._get_producer()

    @patch("apnamart_kafka.client.ConfluentProducer")
    def test_producer_send_empty_topic_error(self, mock_producer_class):
        """Test sending to empty topic raises error."""
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer

        producer = Producer()

        with pytest.raises(ProducerError, match="Topic name cannot be empty"):
            producer.send("", {"data": "test"})

    @patch("apnamart_kafka.client.ConfluentProducer")
    def test_producer_send_success(self, mock_producer_class):
        """Test successful message sending."""
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer

        producer = Producer()
        producer.send("test-topic", {"data": "test"})

        # Verify produce was called
        mock_producer.produce.assert_called_once()
        mock_producer.poll.assert_called_once_with(0)

    @patch("apnamart_kafka.client.ConfluentProducer")
    def test_producer_send_with_key(self, mock_producer_class):
        """Test sending message with key."""
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer

        producer = Producer()
        producer.send("test-topic", {"data": "test"}, key="test-key")

        # Verify produce was called with key
        mock_producer.produce.assert_called_once()
        call_args = mock_producer.produce.call_args
        assert call_args.kwargs["key"] is not None

    @patch("apnamart_kafka.client.ConfluentProducer")
    def test_producer_flush(self, mock_producer_class):
        """Test producer flush operation."""
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer

        producer = Producer()
        # Force producer initialization by calling _get_producer
        producer._get_producer()
        result = producer.flush()

        mock_producer.flush.assert_called_once_with(10)
        assert result == mock_producer.flush.return_value

    @patch("apnamart_kafka.client.ConfluentProducer")
    def test_producer_close_twice(self, mock_producer_class):
        """Test closing producer multiple times."""
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer

        producer = Producer()

        # First close
        producer.close()
        assert producer._closed

        # Second close should not raise error
        producer.close()
        assert producer._closed

    @patch("apnamart_kafka.client.ConfluentProducer")
    def test_producer_connection_reuse(self, mock_producer_class):
        """Test that producer connection is reused."""
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer

        producer = Producer()

        # Get producer twice
        p1 = producer._get_producer()
        p2 = producer._get_producer()

        # Should be the same instance
        assert p1 is p2

        # Should only create producer once
        mock_producer_class.assert_called_once()

    @patch("apnamart_kafka.client.ConfluentProducer")
    def test_producer_error_handling_queue_full(self, mock_producer_class):
        """Test handling of queue full error."""
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer
        mock_producer.produce.side_effect = Exception("Local: Queue full")

        producer = Producer()

        with pytest.raises(ProducerError, match="Producer queue is full"):
            producer.send("test-topic", {"data": "test"})

    @patch("apnamart_kafka.client.ConfluentProducer")
    def test_producer_error_handling_timeout(self, mock_producer_class):
        """Test handling of timeout error."""
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer
        mock_producer.produce.side_effect = Exception("Local: Message timed out")

        producer = Producer()

        with pytest.raises(ProducerError, match="Message delivery timed out"):
            producer.send("test-topic", {"data": "test"})

    @patch("apnamart_kafka.client.ConfluentProducer")
    def test_producer_error_handling_unknown_topic(self, mock_producer_class):
        """Test handling of unknown topic error."""
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer
        mock_producer.produce.side_effect = Exception("Broker: Unknown topic")

        producer = Producer()

        with pytest.raises(ProducerError, match="does not exist or is not accessible"):
            producer.send("unknown-topic", {"data": "test"})
