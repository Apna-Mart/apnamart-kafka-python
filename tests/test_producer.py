"""Tests for the Kafka producer."""

from unittest.mock import Mock, patch

import pytest
from kafka.errors import KafkaError

from apnamart_kafka.config import KafkaConfig
from apnamart_kafka.exceptions import ConnectionError, KafkaProducerError, PublishError
from apnamart_kafka.producer import KafkaProducer


class TestKafkaProducer:
    """Test cases for KafkaProducer."""

    def test_init_with_default_config(self):
        """Test initialization with default config."""
        producer = KafkaProducer()

        assert producer.config.bootstrap_servers == "localhost:9092"
        assert not producer.is_closed

    def test_init_with_custom_config(self):
        """Test initialization with custom config."""
        config = KafkaConfig(bootstrap_servers="custom:9092")
        producer = KafkaProducer(config=config)

        assert producer.config.bootstrap_servers == "custom:9092"

    def test_init_with_kwargs(self):
        """Test initialization with keyword arguments."""
        producer = KafkaProducer(bootstrap_servers="kwargs:9092", acks="all")

        assert producer.config.bootstrap_servers == "kwargs:9092"
        assert producer.config.acks == "all"

    @patch("apnamart_kafka.producer.KafkaClient")
    def test_get_producer_creates_client(self, mock_kafka_client):
        """Test that _get_producer creates a Kafka client."""
        producer = KafkaProducer()

        kafka_producer = producer._get_producer()

        mock_kafka_client.assert_called_once()
        assert kafka_producer is not None

    @patch("apnamart_kafka.producer.KafkaClient")
    def test_get_producer_reuses_client(self, mock_kafka_client):
        """Test that _get_producer reuses existing client."""
        producer = KafkaProducer()

        # First call creates client
        producer._get_producer()
        # Second call should reuse
        producer._get_producer()

        # Should only be called once
        mock_kafka_client.assert_called_once()

    @patch("apnamart_kafka.producer.KafkaClient")
    def test_get_producer_connection_error(self, mock_kafka_client):
        """Test connection error when creating producer."""
        mock_kafka_client.side_effect = Exception("Connection failed")
        producer = KafkaProducer()

        with pytest.raises(ConnectionError, match="Failed to create Kafka producer"):
            producer._get_producer()

    @patch("apnamart_kafka.producer.KafkaClient")
    def test_send_success(self, mock_kafka_client):
        """Test successful message send."""
        # Mock the Kafka client and future
        mock_client = Mock()
        mock_future = Mock()
        mock_metadata = Mock()
        mock_metadata.topic = "test-topic"
        mock_metadata.partition = 0
        mock_metadata.offset = 123
        mock_future.get.return_value = mock_metadata
        mock_client.send.return_value = mock_future
        mock_kafka_client.return_value = mock_client

        producer = KafkaProducer()

        # Should not raise any exception
        producer.send("test-topic", {"key": "value"})

        mock_client.send.assert_called_once()
        mock_future.get.assert_called_once()

    @patch("apnamart_kafka.producer.KafkaClient")
    def test_send_with_key(self, mock_kafka_client):
        """Test message send with key."""
        mock_client = Mock()
        mock_future = Mock()
        mock_metadata = Mock()
        mock_future.get.return_value = mock_metadata
        mock_client.send.return_value = mock_future
        mock_kafka_client.return_value = mock_client

        producer = KafkaProducer()
        producer.send("test-topic", {"key": "value"}, key="message-key")

        # Check that send was called with serialized key
        call_args = mock_client.send.call_args
        assert call_args[1]["key"] == b"message-key"

    @patch("apnamart_kafka.producer.KafkaClient")
    def test_send_with_topic_prefix(self, mock_kafka_client):
        """Test message send with topic prefix."""
        mock_client = Mock()
        mock_future = Mock()
        mock_metadata = Mock()
        mock_future.get.return_value = mock_metadata
        mock_client.send.return_value = mock_future
        mock_kafka_client.return_value = mock_client

        producer = KafkaProducer(topic_prefix="myservice")
        producer.send("test-topic", {"key": "value"})

        # Check that send was called with prefixed topic
        call_args = mock_client.send.call_args
        assert call_args[1]["topic"] == "myservice.test-topic"

    @patch("apnamart_kafka.producer.KafkaClient")
    def test_send_kafka_error(self, mock_kafka_client):
        """Test send with Kafka error."""
        mock_client = Mock()
        mock_future = Mock()
        mock_future.get.side_effect = KafkaError("Send failed")
        mock_client.send.return_value = mock_future
        mock_kafka_client.return_value = mock_client

        producer = KafkaProducer()

        with pytest.raises(PublishError, match="Failed to send message"):
            producer.send("test-topic", {"key": "value"})

    @patch("apnamart_kafka.producer.KafkaClient")
    def test_send_closed_producer(self, mock_kafka_client):
        """Test send with closed producer."""
        producer = KafkaProducer()
        producer.close()

        with pytest.raises(KafkaProducerError, match="Producer is closed"):
            producer.send("test-topic", {"key": "value"})

    @patch("apnamart_kafka.producer.KafkaClient")
    @pytest.mark.asyncio
    async def test_send_async_success(self, mock_kafka_client):
        """Test successful async message send."""
        mock_client = Mock()
        mock_future = Mock()
        mock_metadata = Mock()
        mock_future.get.return_value = mock_metadata
        mock_client.send.return_value = mock_future
        mock_kafka_client.return_value = mock_client

        producer = KafkaProducer()

        # Should not raise any exception
        await producer.send_async("test-topic", {"key": "value"})

        mock_client.send.assert_called_once()

    @patch("apnamart_kafka.producer.KafkaClient")
    @pytest.mark.asyncio
    async def test_send_async_closed_producer(self, mock_kafka_client):
        """Test async send with closed producer."""
        producer = KafkaProducer()
        producer.close()

        with pytest.raises(KafkaProducerError, match="Producer is closed"):
            await producer.send_async("test-topic", {"key": "value"})

    @patch("apnamart_kafka.producer.KafkaClient")
    def test_flush(self, mock_kafka_client):
        """Test flush method."""
        mock_client = Mock()
        mock_kafka_client.return_value = mock_client

        producer = KafkaProducer()
        # Create the producer instance
        producer._get_producer()

        producer.flush(timeout=5.0)

        mock_client.flush.assert_called_once_with(timeout=5.0)

    @patch("apnamart_kafka.producer.KafkaClient")
    def test_flush_error(self, mock_kafka_client):
        """Test flush with error."""
        mock_client = Mock()
        mock_client.flush.side_effect = Exception("Flush failed")
        mock_kafka_client.return_value = mock_client

        producer = KafkaProducer()
        producer._get_producer()

        with pytest.raises(KafkaProducerError, match="Failed to flush producer"):
            producer.flush()

    @patch("apnamart_kafka.producer.KafkaClient")
    def test_close(self, mock_kafka_client):
        """Test close method."""
        mock_client = Mock()
        mock_kafka_client.return_value = mock_client

        producer = KafkaProducer()
        producer._get_producer()  # Create the client
        producer._get_executor()  # Create the executor

        producer.close()

        assert producer.is_closed
        mock_client.flush.assert_called_once()
        mock_client.close.assert_called_once()

    def test_close_idempotent(self):
        """Test that close is idempotent."""
        producer = KafkaProducer()

        # Should not raise any exception
        producer.close()
        producer.close()  # Second call should be safe

        assert producer.is_closed

    @patch("apnamart_kafka.producer.KafkaClient")
    def test_context_manager(self, mock_kafka_client):
        """Test context manager usage."""
        mock_client = Mock()
        mock_kafka_client.return_value = mock_client

        with KafkaProducer() as producer:
            assert not producer.is_closed

        assert producer.is_closed

    @patch("apnamart_kafka.producer.KafkaClient")
    @pytest.mark.asyncio
    async def test_async_context_manager(self, mock_kafka_client):
        """Test async context manager usage."""
        mock_client = Mock()
        mock_kafka_client.return_value = mock_client

        async with KafkaProducer() as producer:
            assert not producer.is_closed

        assert producer.is_closed

    @patch("apnamart_kafka.producer.KafkaClient")
    def test_health_check_healthy(self, mock_kafka_client):
        """Test health check when healthy."""
        mock_client = Mock()
        mock_client.list_consumer_group_offsets.return_value = {}
        mock_kafka_client.return_value = mock_client

        producer = KafkaProducer()
        health = producer.health_check()

        assert health["status"] == "healthy"
        assert health["connection"] == "active"
        assert "timestamp" in health

    @patch("apnamart_kafka.producer.KafkaClient")
    def test_health_check_unhealthy(self, mock_kafka_client):
        """Test health check when unhealthy."""
        mock_kafka_client.side_effect = Exception("Connection failed")

        producer = KafkaProducer()
        health = producer.health_check()

        assert health["status"] == "unhealthy"
        assert health["connection"] == "failed"
        assert "error" in health
        assert "timestamp" in health
