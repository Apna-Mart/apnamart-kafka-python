"""Tests for new features added to the library."""

import asyncio
import pytest
from unittest.mock import Mock, MagicMock, patch, AsyncMock

from apnamart_kafka import (
    KafkaProducer,
    KafkaConsumer,
    TransactionalProducer,
    KafkaConfig,
    KafkaConsumerConfig,
    ConfigPresets,
)


class TestBatchOperations:
    """Test batch operations in producer."""

    @patch("apnamart_kafka.producer.KafkaClient")
    def test_send_batch(self, mock_kafka_client):
        """Test synchronous batch send."""
        # Setup mock
        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_producer.send.return_value = mock_future
        mock_kafka_client.return_value = mock_producer

        # Create producer and send batch
        config = KafkaConfig(bootstrap_servers="localhost:9092")
        producer = KafkaProducer(config)
        
        messages = [
            {"topic": "test", "value": {"id": 1}, "key": "key1"},
            {"topic": "test", "value": {"id": 2}, "key": "key2"},
        ]
        
        futures = producer.send_batch(messages)
        
        # Verify
        assert len(futures) == 2
        assert all(f == mock_future for f in futures)
        assert mock_producer.send.call_count == 2

    @pytest.mark.asyncio
    @patch("apnamart_kafka.producer.KafkaClient")
    async def test_send_batch_async(self, mock_kafka_client):
        """Test asynchronous batch send."""
        # Setup mock
        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_future.get.return_value = MagicMock()
        mock_producer.send.return_value = mock_future
        mock_kafka_client.return_value = mock_producer

        # Create producer and send batch
        config = KafkaConfig(bootstrap_servers="localhost:9092")
        producer = KafkaProducer(config)
        
        messages = [
            {"topic": "test", "value": {"id": 1}, "key": "key1"},
            {"topic": "test", "value": {"id": 2}, "key": "key2"},
        ]
        
        results = await producer.send_batch_async(messages)
        
        # Verify
        assert len(results) == 2
        # Should have called send for each message through send_async


class TestPerformanceConfig:
    """Test performance configuration parameters."""

    def test_performance_parameters(self):
        """Test that performance parameters are properly configured."""
        config = KafkaConfig(
            bootstrap_servers="localhost:9092",
            batch_size=65536,
            linger_ms=100,
            buffer_memory=67108864,
            compression_type="lz4"
        )
        
        kafka_config = config.to_kafka_config()
        
        assert kafka_config["batch_size"] == 65536
        assert kafka_config["linger_ms"] == 100
        assert kafka_config["buffer_memory"] == 67108864
        assert kafka_config["compression_type"] == "lz4"


class TestConfigPresets:
    """Test configuration presets."""

    def test_high_throughput_preset(self):
        """Test high throughput producer preset."""
        config = ConfigPresets.producer.high_throughput()
        
        assert config.batch_size == 65536
        assert config.linger_ms == 100
        assert config.compression_type == "lz4"
        assert config.buffer_memory == 67108864
        assert config.max_request_size == 5242880
        assert config.acks == "1"

    def test_low_latency_preset(self):
        """Test low latency producer preset."""
        config = ConfigPresets.producer.low_latency()
        
        assert config.batch_size == 0
        assert config.linger_ms == 0
        assert config.compression_type is None
        assert config.acks == "1"
        assert config.max_block_ms == 5000

    def test_reliable_preset(self):
        """Test reliable producer preset."""
        config = ConfigPresets.producer.reliable()
        
        assert config.acks == "all"
        assert config.retries == 10
        assert config.retry_backoff_ms == 500
        assert config.compression_type == "gzip"
        assert config.request_timeout_ms == 60000
        assert config.max_block_ms == 120000

    def test_batch_processing_consumer_preset(self):
        """Test batch processing consumer preset."""
        config = ConfigPresets.consumer.batch_processing()
        
        assert config.max_poll_records == 1000
        assert config.max_poll_interval_ms == 600000
        assert config.enable_auto_commit is False
        assert config.fetch_min_bytes == 1048576
        assert config.fetch_max_wait_ms == 1000
        assert config.group_id == "batch-processing-group"

    def test_real_time_consumer_preset(self):
        """Test real-time consumer preset."""
        config = ConfigPresets.consumer.real_time()
        
        assert config.max_poll_records == 10
        assert config.max_poll_interval_ms == 30000
        assert config.enable_auto_commit is True
        assert config.auto_commit_interval_ms == 1000
        assert config.fetch_min_bytes == 1
        assert config.fetch_max_wait_ms == 100
        assert config.group_id == "real-time-group"


class TestParallelHelpers:
    """Test consumer parallel processing helpers."""

    @patch("apnamart_kafka.consumer.KafkaConsumerClient")
    def test_consume_batches(self, mock_kafka_consumer_client):
        """Test consume_batches method."""
        # Setup mock
        mock_consumer = MagicMock()
        mock_record1 = MagicMock(
            topic="test", partition=0, offset=1,
            timestamp=123, timestamp_type=0,
            key=b"key1", value=b'{"test": 1}',
            headers=[]
        )
        mock_record2 = MagicMock(
            topic="test", partition=0, offset=2,
            timestamp=124, timestamp_type=0,
            key=b"key2", value=b'{"test": 2}',
            headers=[]
        )
        
        # Mock poll to return proper format (dict of TopicPartition -> list)
        from confluent_kafka import TopicPartition
        tp = TopicPartition("test", 0)
        
        # First poll returns 2 messages, second returns empty
        mock_consumer.poll.side_effect = [
            {tp: [mock_record1, mock_record2]},
            {}
        ]
        mock_kafka_consumer_client.return_value = mock_consumer

        # Create consumer and consume batches
        config = KafkaConsumerConfig(
            bootstrap_servers="localhost:9092",
            group_id="test"
        )
        consumer = KafkaConsumer(config)
        
        # Get first batch only to avoid infinite loop
        batch_iterator = consumer.consume_batches(batch_size=10, timeout_ms=1000)
        first_batch = next(batch_iterator)
        
        # Verify
        assert len(first_batch) == 2
        assert first_batch[0].key == b"key1"
        assert first_batch[1].key == b"key2"

    @pytest.mark.asyncio
    @patch("apnamart_kafka.consumer.KafkaConsumerClient")
    async def test_consume_parallel(self, mock_kafka_consumer_client):
        """Test consume_parallel method."""
        # Setup mock
        mock_consumer = MagicMock()
        mock_record = MagicMock(
            topic="test", partition=0, offset=1,
            timestamp=123, timestamp_type=0,
            key=b"key1", value=b'{"test": 1}',
            headers=[]
        )
        
        # Mock poll to return proper format (dict of TopicPartition -> list)
        from confluent_kafka import TopicPartition
        tp = TopicPartition("test", 0)
        
        # Return one message then empty
        mock_consumer.poll.side_effect = [
            {tp: [mock_record]},
            {}
        ]
        mock_kafka_consumer_client.return_value = mock_consumer

        # Create consumer
        config = KafkaConsumerConfig(
            bootstrap_servers="localhost:9092",
            group_id="test"
        )
        consumer = KafkaConsumer(config)
        
        # Track processed messages
        processed = []
        
        async def handler(msg):
            processed.append(msg.key)
            await asyncio.sleep(0.01)
        
        # Run with timeout to prevent infinite loop
        try:
            await asyncio.wait_for(
                consumer.consume_parallel(
                    handler=handler,
                    max_workers=2,
                    batch_size=10,
                    commit_interval=1
                ),
                timeout=0.5
            )
        except asyncio.TimeoutError:
            pass  # Expected
        
        # Verify
        assert len(processed) == 1
        assert processed[0] == b"key1"


class TestTransactionalProducer:
    """Test transactional producer."""

    @patch("apnamart_kafka.transactional_producer.KafkaClient")
    def test_transactional_producer_init(self, mock_kafka_client):
        """Test transactional producer initialization."""
        # Setup mock
        mock_producer = MagicMock()
        mock_kafka_client.return_value = mock_producer

        # Create transactional producer
        config = KafkaConfig(bootstrap_servers="localhost:9092")
        producer = TransactionalProducer(config, transactional_id="test-tx-1")
        
        # Force client creation
        producer._get_producer()
        
        # Verify
        mock_producer.init_transactions.assert_called_once()

    @patch("apnamart_kafka.transactional_producer.KafkaClient")
    def test_transactional_batch_send(self, mock_kafka_client):
        """Test transactional batch send."""
        # Setup mock
        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_future.get.return_value = MagicMock()
        mock_producer.send.return_value = mock_future
        mock_kafka_client.return_value = mock_producer

        # Create transactional producer
        config = KafkaConfig(bootstrap_servers="localhost:9092")
        producer = TransactionalProducer(config, transactional_id="test-tx-2")
        
        messages = [
            {"topic": "test", "value": {"id": 1}},
            {"topic": "test", "value": {"id": 2}},
        ]
        
        # Send transactional batch
        producer.send_transactional_batch(messages)
        
        # Verify transaction lifecycle
        mock_producer.begin_transaction.assert_called_once()
        assert mock_producer.send.call_count == 2
        mock_producer.commit_transaction.assert_called_once()
        mock_producer.abort_transaction.assert_not_called()

    def test_transactional_producer_requires_id(self):
        """Test that transactional producer requires transactional_id."""
        config = KafkaConfig(bootstrap_servers="localhost:9092")
        
        with pytest.raises(ValueError, match="transactional_id is required"):
            TransactionalProducer(config)