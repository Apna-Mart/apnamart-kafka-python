"""Tests for Kafka consumer."""

from unittest.mock import Mock, patch

import pytest

from apnamart_kafka.consumer import ConsumerMessage, KafkaConsumer
from apnamart_kafka.consumer_config import KafkaConsumerConfig
from apnamart_kafka.exceptions import ConnectionError, KafkaProducerError


class MockConsumerRecord:
    """Mock ConsumerRecord for testing."""

    def __init__(
        self,
        topic="test-topic",
        partition=0,
        offset=1,
        key=None,
        value=None,
        timestamp=None,
    ):
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.key = key
        self.value = value
        self.timestamp = timestamp or 1000
        self.timestamp_type = 1
        self.headers = []


class TestConsumerMessage:
    """Test cases for ConsumerMessage."""

    def test_consumer_message_creation(self):
        """Test ConsumerMessage creation from record."""
        record = MockConsumerRecord(
            topic="test-topic",
            partition=1,
            offset=100,
            key=b"test-key",
            value=b"test-value",
            timestamp=1234567890,
        )

        message = ConsumerMessage(record)

        assert message.topic == "test-topic"
        assert message.partition == 1
        assert message.offset == 100
        assert message.key == b"test-key"
        assert message.value == b"test-value"
        assert message.timestamp == 1234567890

    def test_consumer_message_repr(self):
        """Test ConsumerMessage string representation."""
        record = MockConsumerRecord(topic="test", partition=0, offset=1, key="key")
        message = ConsumerMessage(record)

        repr_str = repr(message)
        assert "ConsumerMessage" in repr_str
        assert "test" in repr_str
        assert "partition=0" in repr_str
        assert "offset=1" in repr_str


class TestKafkaConsumer:
    """Test cases for KafkaConsumer."""

    def test_init_with_default_config(self):
        """Test consumer initialization with default config."""
        with patch("apnamart_kafka.consumer.KafkaConsumerClient"):
            consumer = KafkaConsumer(KafkaConsumerConfig(group_id="test-group"))

            assert consumer._kafka_config.group_id == "test-group"
            assert consumer._kafka_config.bootstrap_servers == "localhost:9092"
            assert not consumer._is_closed
            assert len(consumer._subscribed_topics) == 0

    def test_init_with_custom_config(self):
        """Test consumer initialization with custom config."""
        config = KafkaConsumerConfig(
            group_id="custom-group",
            bootstrap_servers="custom-broker:9092",
            auto_offset_reset="earliest",
        )

        with patch("apnamart_kafka.consumer.KafkaConsumerClient"):
            consumer = KafkaConsumer(config=config)

            assert consumer._kafka_config.group_id == "custom-group"
            assert consumer._kafka_config.bootstrap_servers == "custom-broker:9092"
            assert consumer._kafka_config.auto_offset_reset == "earliest"

    def test_init_with_kwargs(self):
        """Test consumer initialization with kwargs."""
        with patch("apnamart_kafka.consumer.KafkaConsumerClient"):
            consumer = KafkaConsumer(
                group_id="kwargs-group", auto_offset_reset="earliest"
            )

            assert consumer._kafka_config.group_id == "kwargs-group"
            assert consumer._kafka_config.auto_offset_reset == "earliest"

    @patch("apnamart_kafka.consumer.KafkaConsumerClient")
    def test_create_client_success(self, mock_client_class):
        """Test successful client creation."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        consumer = KafkaConsumer(KafkaConsumerConfig(group_id="test"))
        client = consumer._create_client()

        assert client is mock_client
        mock_client_class.assert_called_once()

    @patch("apnamart_kafka.consumer.KafkaConsumerClient")
    def test_create_client_failure(self, mock_client_class):
        """Test client creation failure."""
        mock_client_class.side_effect = Exception("Connection failed")

        consumer = KafkaConsumer(KafkaConsumerConfig(group_id="test"))

        with pytest.raises(ConnectionError) as exc_info:
            consumer._create_client()

        assert "Failed to create Kafka consumer" in str(exc_info.value)

    @patch("apnamart_kafka.consumer.KafkaConsumerClient")
    def test_get_consumer_creates_client(self, mock_client_class):
        """Test that _get_consumer creates client when needed."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        consumer = KafkaConsumer(KafkaConsumerConfig(group_id="test"))
        client = consumer._get_consumer()

        assert client is mock_client
        assert consumer._consumer is mock_client

    @patch("apnamart_kafka.consumer.KafkaConsumerClient")
    def test_get_consumer_reuses_client(self, mock_client_class):
        """Test that _get_consumer reuses existing client."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        consumer = KafkaConsumer(KafkaConsumerConfig(group_id="test"))
        client1 = consumer._get_consumer()
        client2 = consumer._get_consumer()

        assert client1 is client2
        mock_client_class.assert_called_once()

    @patch("apnamart_kafka.consumer.KafkaConsumerClient")
    def test_subscribe_single_topic(self, mock_client_class):
        """Test subscribing to a single topic."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        consumer = KafkaConsumer(KafkaConsumerConfig(group_id="test"))
        consumer.subscribe("test-topic")

        mock_client.subscribe.assert_called_once_with(["test-topic"])
        assert "test-topic" in consumer._subscribed_topics

    @patch("apnamart_kafka.consumer.KafkaConsumerClient")
    def test_subscribe_multiple_topics(self, mock_client_class):
        """Test subscribing to multiple topics."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        consumer = KafkaConsumer(KafkaConsumerConfig(group_id="test"))
        consumer.subscribe(["topic1", "topic2", "topic3"])

        mock_client.subscribe.assert_called_once_with(["topic1", "topic2", "topic3"])
        assert "topic1" in consumer._subscribed_topics
        assert "topic2" in consumer._subscribed_topics
        assert "topic3" in consumer._subscribed_topics

    @patch("apnamart_kafka.consumer.KafkaConsumerClient")
    def test_subscribe_with_prefix(self, mock_client_class):
        """Test subscribing with topic prefix."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        config = KafkaConsumerConfig(group_id="test", topic_prefix="myapp")
        consumer = KafkaConsumer(config=config)
        consumer.subscribe(["topic1", "topic2"])

        mock_client.subscribe.assert_called_once_with(["myapp.topic1", "myapp.topic2"])
        assert "myapp.topic1" in consumer._subscribed_topics
        assert "myapp.topic2" in consumer._subscribed_topics

    @patch("apnamart_kafka.consumer.KafkaConsumerClient")
    def test_subscribe_pattern(self, mock_client_class):
        """Test subscribing with pattern."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        consumer = KafkaConsumer(KafkaConsumerConfig(group_id="test"))
        consumer.subscribe(topics=None, pattern="test-.*")

        mock_client.subscribe.assert_called_once_with(pattern="test-.*")
        assert "pattern:test-.*" in consumer._subscribed_topics

    def test_subscribe_closed_consumer(self):
        """Test subscribing to closed consumer raises error."""
        consumer = KafkaConsumer(KafkaConsumerConfig(group_id="test"))
        consumer._is_closed = True

        with pytest.raises(KafkaProducerError) as exc_info:
            consumer.subscribe("test-topic")

        assert "Consumer is closed" in str(exc_info.value)

    @patch("apnamart_kafka.consumer.KafkaConsumerClient")
    def test_poll_success(self, mock_client_class):
        """Test successful message polling."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock poll return value
        from kafka.structs import TopicPartition

        tp = TopicPartition("test-topic", 0)
        records = [
            MockConsumerRecord("test-topic", 0, 1, b"key1", b"value1"),
            MockConsumerRecord("test-topic", 0, 2, b"key2", b"value2"),
        ]
        mock_client.poll.return_value = {tp: records}

        consumer = KafkaConsumer(KafkaConsumerConfig(group_id="test"))
        messages = consumer.poll(timeout_ms=1000)

        assert len(messages) == 2
        assert isinstance(messages[0], ConsumerMessage)
        assert messages[0].topic == "test-topic"
        assert messages[0].offset == 1
        assert messages[1].offset == 2

    @patch("apnamart_kafka.consumer.KafkaConsumerClient")
    def test_poll_no_messages(self, mock_client_class):
        """Test polling with no messages."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.poll.return_value = {}

        consumer = KafkaConsumer(KafkaConsumerConfig(group_id="test"))
        messages = consumer.poll(timeout_ms=1000)

        assert len(messages) == 0

    def test_poll_closed_consumer(self):
        """Test polling closed consumer raises error."""
        consumer = KafkaConsumer(KafkaConsumerConfig(group_id="test"))
        consumer._is_closed = True

        with pytest.raises(KafkaProducerError) as exc_info:
            consumer.poll()

        assert "Consumer is closed" in str(exc_info.value)

    @patch("apnamart_kafka.consumer.KafkaConsumerClient")
    def test_commit_offsets_default(self, mock_client_class):
        """Test committing offsets without specific offsets."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        consumer = KafkaConsumer(KafkaConsumerConfig(group_id="test"))
        consumer.commit_offsets()

        mock_client.commit.assert_called_once_with()

    @patch("apnamart_kafka.consumer.KafkaConsumerClient")
    def test_commit_offsets_specific(self, mock_client_class):
        """Test committing specific offsets."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        from kafka.structs import OffsetAndMetadata, TopicPartition

        offsets = {
            TopicPartition("test-topic", 0): OffsetAndMetadata(100, "metadata", None)
        }

        consumer = KafkaConsumer(KafkaConsumerConfig(group_id="test"))
        consumer.commit_offsets(offsets)

        mock_client.commit.assert_called_once_with(offsets)

    @patch("apnamart_kafka.consumer.KafkaConsumerClient")
    def test_seek(self, mock_client_class):
        """Test seeking to specific offset."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        from kafka.structs import TopicPartition

        consumer = KafkaConsumer(KafkaConsumerConfig(group_id="test"))
        tp = TopicPartition("test-topic", 0)
        consumer.seek(tp, 100)

        mock_client.seek.assert_called_once_with(tp, 100)

    @patch("apnamart_kafka.consumer.KafkaConsumerClient")
    def test_seek_with_prefix(self, mock_client_class):
        """Test seeking with topic prefix."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        from kafka.structs import TopicPartition

        config = KafkaConsumerConfig(group_id="test", topic_prefix="myapp")
        consumer = KafkaConsumer(config=config)
        tp = TopicPartition("test-topic", 0)
        consumer.seek(tp, 100)

        expected_tp = TopicPartition("myapp.test-topic", 0)
        mock_client.seek.assert_called_once_with(expected_tp, 100)

    @patch("apnamart_kafka.consumer.KafkaConsumerClient")
    def test_seek_to_beginning(self, mock_client_class):
        """Test seeking to beginning."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        consumer = KafkaConsumer(KafkaConsumerConfig(group_id="test"))
        consumer.seek_to_beginning()

        mock_client.seek_to_beginning.assert_called_once_with()

    @patch("apnamart_kafka.consumer.KafkaConsumerClient")
    def test_seek_to_end(self, mock_client_class):
        """Test seeking to end."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        consumer = KafkaConsumer(KafkaConsumerConfig(group_id="test"))
        consumer.seek_to_end()

        mock_client.seek_to_end.assert_called_once_with()

    @patch("apnamart_kafka.consumer.KafkaConsumerClient")
    def test_close(self, mock_client_class):
        """Test consumer close."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        consumer = KafkaConsumer(KafkaConsumerConfig(group_id="test"))
        consumer._get_consumer()  # Create the client
        consumer.close()

        assert consumer._is_closed
        mock_client.close.assert_called_once()
        assert consumer._consumer is None

    def test_close_idempotent(self):
        """Test that close is idempotent."""
        consumer = KafkaConsumer(KafkaConsumerConfig(group_id="test"))
        consumer.close()
        consumer.close()  # Should not raise error

        assert consumer._is_closed

    @patch("apnamart_kafka.consumer.KafkaConsumerClient")
    def test_context_manager(self, mock_client_class):
        """Test consumer as context manager."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        with KafkaConsumer(KafkaConsumerConfig(group_id="test")) as consumer:
            assert not consumer._is_closed

        assert consumer._is_closed

    @patch("apnamart_kafka.consumer.KafkaConsumerClient")
    @pytest.mark.asyncio
    async def test_async_context_manager(self, mock_client_class):
        """Test consumer as async context manager."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        async with KafkaConsumer(KafkaConsumerConfig(group_id="test")) as consumer:
            assert not consumer._is_closed

        assert consumer._is_closed

    @patch("apnamart_kafka.consumer.KafkaConsumerClient")
    @pytest.mark.asyncio
    async def test_commit_offsets_async(self, mock_client_class):
        """Test async offset commit."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        consumer = KafkaConsumer(KafkaConsumerConfig(group_id="test"))
        await consumer.commit_offsets_async()

        # Should have been called in thread pool
        mock_client.commit.assert_called_once_with()

    def test_get_metrics(self):
        """Test consumer metrics."""
        consumer = KafkaConsumer(KafkaConsumerConfig(group_id="test"))
        consumer._subscribed_topics.add("topic1")
        consumer._subscribed_topics.add("topic2")

        metrics = consumer.get_metrics()

        assert metrics["consumer_type"] == "KafkaConsumer"
        assert metrics["has_consumer"] is False
        assert set(metrics["subscribed_topics"]) == {"topic1", "topic2"}
        assert metrics["subscription_type"] == "subscribed"

    def test_subscribed_topics_property(self):
        """Test subscribed_topics property."""
        consumer = KafkaConsumer(KafkaConsumerConfig(group_id="test"))
        consumer._subscribed_topics.add("topic1")
        consumer._subscribed_topics.add("topic2")

        topics = consumer.subscribed_topics
        assert "topic1" in topics
        assert "topic2" in topics

        # Should return a copy
        topics.add("topic3")
        assert "topic3" not in consumer._subscribed_topics

    def test_assigned_partitions_property(self):
        """Test assigned_partitions property."""
        from kafka.structs import TopicPartition

        consumer = KafkaConsumer(KafkaConsumerConfig(group_id="test"))
        tp1 = TopicPartition("topic1", 0)
        tp2 = TopicPartition("topic2", 1)
        consumer._assigned_partitions.add(tp1)
        consumer._assigned_partitions.add(tp2)

        partitions = consumer.assigned_partitions
        assert tp1 in partitions
        assert tp2 in partitions

        # Should return a copy
        tp3 = TopicPartition("topic3", 0)
        partitions.add(tp3)
        assert tp3 not in consumer._assigned_partitions

    @patch("apnamart_kafka.consumer.KafkaConsumerClient")
    def test_health_check_healthy(self, mock_client_class):
        """Test health check when consumer is healthy."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        consumer = KafkaConsumer(KafkaConsumerConfig(group_id="test"))
        health = consumer.health_check()

        assert health["status"] == "healthy"
        assert health["connection"] == "active"
        assert health["bootstrap_servers"] == "localhost:9092"

    @patch("apnamart_kafka.consumer.KafkaConsumerClient")
    def test_health_check_unhealthy(self, mock_client_class):
        """Test health check when consumer is unhealthy."""
        mock_client_class.side_effect = Exception("Connection failed")

        consumer = KafkaConsumer(KafkaConsumerConfig(group_id="test"))
        health = consumer.health_check()

        assert health["status"] == "unhealthy"
        assert health["connection"] == "failed"
        assert "Connection failed" in health["error"]
