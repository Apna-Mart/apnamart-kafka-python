"""Unit tests for apnamart-kafka-python core functionality."""

import json
from unittest.mock import Mock, patch

import pytest

from apnamart_kafka import (
    Config,
    Consumer,
    ConsumerError,
    KafkaError,
    Message,
    Producer,
    ProducerError,
    TransactionalProducer,
    TransactionError,
    deserialize,
    serialize,
)


class TestConfig:
    """Test configuration functionality."""

    def test_config_defaults(self):
        """Test default configuration values."""
        config = Config()
        assert config.bootstrap_servers == "localhost:9092"
        assert config.acks == "all"
        assert config.retries == 3
        assert config.group_id == "default-group"
        assert config.auto_offset_reset == "latest"
        assert config.enable_auto_commit is True

    def test_config_custom_values(self):
        """Test custom configuration values."""
        config = Config(
            bootstrap_servers="broker1:9092,broker2:9092",
            acks="1",
            retries=5,
            compression_type="gzip",
            batch_size=32768,
            group_id="test-group",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        assert config.bootstrap_servers == "broker1:9092,broker2:9092"
        assert config.acks == "1"
        assert config.retries == 5
        assert config.compression_type == "gzip"
        assert config.batch_size == 32768
        assert config.group_id == "test-group"
        assert config.auto_offset_reset == "earliest"
        assert config.enable_auto_commit is False

    def test_producer_config_conversion(self):
        """Test producer configuration conversion."""
        config = Config(
            bootstrap_servers="broker:9092",
            acks="all",
            compression_type="snappy",
            batch_size=16384,
        )
        prod_config = config.to_producer_config()

        assert prod_config["bootstrap.servers"] == "broker:9092"
        assert prod_config["acks"] == -1  # "all" -> -1
        assert prod_config["compression.type"] == "snappy"
        assert prod_config["batch.size"] == 16384

    def test_producer_config_no_compression(self):
        """Test producer config without compression."""
        config = Config(bootstrap_servers="broker:9092")
        prod_config = config.to_producer_config()

        assert "compression.type" not in prod_config

    def test_consumer_config_conversion(self):
        """Test consumer configuration conversion."""
        config = Config(
            bootstrap_servers="broker:9092",
            group_id="test-group",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        cons_config = config.to_consumer_config()

        assert cons_config["bootstrap.servers"] == "broker:9092"
        assert cons_config["group.id"] == "test-group"
        assert cons_config["auto.offset.reset"] == "earliest"
        assert cons_config["enable.auto.commit"] is False


class TestSerialization:
    """Test serialization functionality."""

    def test_serialize_bytes(self):
        """Test serializing bytes data."""
        data = b"test bytes"
        result = serialize(data)
        assert result == data
        assert isinstance(result, bytes)

    def test_serialize_string(self):
        """Test serializing string data."""
        data = "test string"
        result = serialize(data)
        assert result == data.encode("utf-8")
        assert isinstance(result, bytes)

    def test_serialize_json(self):
        """Test serializing JSON data."""
        data = {"key": "value", "number": 42, "list": [1, 2, 3]}
        result = serialize(data)
        expected = json.dumps(data).encode("utf-8")
        assert result == expected
        assert isinstance(result, bytes)

    def test_serialize_none(self):
        """Test serializing None."""
        data = None
        result = serialize(data)
        assert result == b"null"

    def test_deserialize_json(self):
        """Test deserializing JSON data."""
        data = {"key": "value", "number": 42}
        serialized = json.dumps(data).encode("utf-8")
        result = deserialize(serialized)
        assert result == data

    def test_deserialize_string(self):
        """Test deserializing string data."""
        data = "test string"
        serialized = data.encode("utf-8")
        result = deserialize(serialized)
        assert result == data

    def test_deserialize_invalid_json(self):
        """Test deserializing invalid JSON falls back to string."""
        data = b"invalid json {"
        result = deserialize(data)
        assert result == "invalid json {"

    def test_deserialize_invalid_utf8(self):
        """Test deserializing invalid UTF-8 falls back to bytes."""
        data = b"\xff\xfe\xfd"
        result = deserialize(data)
        assert result == data


class TestMessage:
    """Test Message class."""

    def test_message_creation(self):
        """Test message creation from confluent-kafka record."""
        mock_record = Mock()
        mock_record.topic.return_value = "test-topic"
        mock_record.partition.return_value = 0
        mock_record.offset.return_value = 123
        mock_record.key.return_value = b"test-key"
        mock_record.value.return_value = b'{"test": "value"}'
        mock_record.timestamp.return_value = (1, 1234567890000)
        mock_record.headers.return_value = [("header1", b"value1")]

        message = Message(mock_record)

        assert message.topic == "test-topic"
        assert message.partition == 0
        assert message.offset == 123
        assert message.key == b"test-key"
        assert message.value == {"test": "value"}
        assert message.timestamp == 1234567890000
        assert message.headers == {"header1": b"value1"}

    def test_message_no_timestamp(self):
        """Test message with no timestamp."""
        mock_record = Mock()
        mock_record.topic.return_value = "test-topic"
        mock_record.partition.return_value = 0
        mock_record.offset.return_value = 123
        mock_record.key.return_value = None
        mock_record.value.return_value = b"test"
        mock_record.timestamp.return_value = (-1, None)
        mock_record.headers.return_value = None

        message = Message(mock_record)

        assert message.timestamp is None
        assert message.headers == {}

    def test_message_repr(self):
        """Test message string representation."""
        mock_record = Mock()
        mock_record.topic.return_value = "test-topic"
        mock_record.partition.return_value = 0
        mock_record.offset.return_value = 123
        mock_record.key.return_value = None
        mock_record.value.return_value = None
        mock_record.timestamp.return_value = (-1, None)
        mock_record.headers.return_value = None

        message = Message(mock_record)
        repr_str = repr(message)

        assert "test-topic" in repr_str
        assert "partition=0" in repr_str
        assert "offset=123" in repr_str


class TestExceptions:
    """Test exception classes."""

    def test_kafka_error_inheritance(self):
        """Test KafkaError is base exception."""
        error = KafkaError("test error")
        assert isinstance(error, Exception)
        assert str(error) == "test error"

    def test_producer_error_inheritance(self):
        """Test ProducerError inherits from KafkaError."""
        error = ProducerError("producer error")
        assert isinstance(error, KafkaError)
        assert isinstance(error, Exception)

    def test_consumer_error_inheritance(self):
        """Test ConsumerError inherits from KafkaError."""
        error = ConsumerError("consumer error")
        assert isinstance(error, KafkaError)
        assert isinstance(error, Exception)

    def test_transaction_error_inheritance(self):
        """Test TransactionError inherits from KafkaError."""
        error = TransactionError("transaction error")
        assert isinstance(error, KafkaError)
        assert isinstance(error, Exception)


@pytest.mark.unit
class TestProducerUnit:
    """Unit tests for Producer class."""

    def test_producer_init_default_config(self):
        """Test producer initialization with default config."""
        with patch("apnamart_kafka.client.ConfluentProducer"):
            producer = Producer()
            assert producer.config.bootstrap_servers == "localhost:9092"
            assert not producer._closed

    def test_producer_init_custom_config(self, test_config):
        """Test producer initialization with custom config."""
        with patch("apnamart_kafka.client.ConfluentProducer"):
            producer = Producer(test_config)
            assert producer.config == test_config

    def test_producer_init_kwargs(self):
        """Test producer initialization with kwargs."""
        with patch("apnamart_kafka.client.ConfluentProducer"):
            producer = Producer(bootstrap_servers="test:9092", acks="1")
            assert producer.config.bootstrap_servers == "test:9092"
            assert producer.config.acks == "1"

    def test_producer_context_manager(self, mock_confluent_producer):
        """Test producer as context manager."""
        with Producer() as producer:
            assert isinstance(producer, Producer)
            assert not producer._closed
        assert producer._closed

    def test_producer_send_closed_error(self, mock_confluent_producer):
        """Test sending with closed producer raises error."""
        producer = Producer()
        producer.close()

        with pytest.raises(ProducerError, match="Producer is closed"):
            producer.send("topic", "value")

    def test_producer_get_producer_closed_error(self):
        """Test getting producer when closed raises error."""
        with patch("apnamart_kafka.client.ConfluentProducer"):
            producer = Producer()
            producer._closed = True

            with pytest.raises(ProducerError, match="Producer is closed"):
                producer._get_producer()


@pytest.mark.unit
class TestConsumerUnit:
    """Unit tests for Consumer class."""

    def test_consumer_init_single_topic(self):
        """Test consumer initialization with single topic."""
        with patch("apnamart_kafka.client.ConfluentConsumer"):
            consumer = Consumer("test-topic")
            assert consumer.topics == ["test-topic"]

    def test_consumer_init_multiple_topics(self):
        """Test consumer initialization with multiple topics."""
        topics = ["topic1", "topic2"]
        with patch("apnamart_kafka.client.ConfluentConsumer"):
            consumer = Consumer(topics)
            assert consumer.topics == topics

    def test_consumer_init_no_topics(self):
        """Test consumer initialization without topics."""
        with patch("apnamart_kafka.client.ConfluentConsumer"):
            consumer = Consumer()
            assert consumer.topics is None

    def test_consumer_context_manager(self, mock_confluent_consumer):
        """Test consumer as context manager."""
        with Consumer("test-topic") as consumer:
            assert isinstance(consumer, Consumer)
            assert not consumer._closed
        assert consumer._closed

    def test_consumer_poll_closed_error(self, mock_confluent_consumer):
        """Test polling with closed consumer raises error."""
        consumer = Consumer("test-topic")
        consumer.close()

        with pytest.raises(ConsumerError, match="Consumer is closed"):
            consumer.poll()

    def test_consumer_commit_closed_error(self, mock_confluent_consumer):
        """Test committing with closed consumer raises error."""
        consumer = Consumer("test-topic")
        consumer.close()

        with pytest.raises(ConsumerError, match="Consumer is closed"):
            consumer.commit()


@pytest.mark.unit
class TestTransactionalProducerUnit:
    """Unit tests for TransactionalProducer class."""

    def test_transactional_producer_init(self):
        """Test transactional producer initialization."""
        with patch("apnamart_kafka.client.ConfluentProducer") as mock_prod:
            mock_instance = Mock()
            mock_instance.init_transactions = Mock()
            mock_prod.return_value = mock_instance

            tx_producer = TransactionalProducer("test-tx-id")
            assert tx_producer.transactional_id == "test-tx-id"
            assert not tx_producer._in_transaction
            assert not tx_producer._initialized

    def test_transactional_producer_config_update(self):
        """Test transactional producer updates config."""
        config = Config()
        TransactionalProducer("test-tx-id", config)

        assert config.config["transactional.id"] == "test-tx-id"
        assert config.config["enable.idempotence"] is True

    def test_begin_transaction_already_in_progress(self):
        """Test beginning transaction when already in progress."""
        with patch("apnamart_kafka.client.ConfluentProducer"):
            tx_producer = TransactionalProducer("test-tx-id")
            tx_producer._in_transaction = True

            with pytest.raises(TransactionError, match="Transaction already in progress"):
                tx_producer.begin()

    def test_commit_no_transaction(self):
        """Test committing when no transaction in progress."""
        with patch("apnamart_kafka.client.ConfluentProducer"):
            tx_producer = TransactionalProducer("test-tx-id")

            with pytest.raises(TransactionError, match="No transaction in progress"):
                tx_producer.commit()

    def test_abort_no_transaction(self):
        """Test aborting when no transaction in progress."""
        with patch("apnamart_kafka.client.ConfluentProducer"):
            tx_producer = TransactionalProducer("test-tx-id")

            with pytest.raises(TransactionError, match="No transaction in progress"):
                tx_producer.abort()

    def test_send_transactional_already_in_progress(self):
        """Test send_transactional when transaction already in progress."""
        with patch("apnamart_kafka.client.ConfluentProducer"):
            tx_producer = TransactionalProducer("test-tx-id")
            tx_producer._in_transaction = True

            with pytest.raises(TransactionError, match="Transaction already in progress"):
                tx_producer.send_transactional([{"topic": "test", "value": "data"}])
