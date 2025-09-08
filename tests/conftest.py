"""Pytest configuration and fixtures."""

import os
import time
import uuid
from typing import Generator
from unittest.mock import Mock, patch

import pytest

from apnamart_kafka import Config, Consumer, Producer, TransactionalProducer


@pytest.fixture
def kafka_servers() -> str:
    """Return Kafka server address."""
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


@pytest.fixture
def test_config(kafka_servers: str) -> Config:
    """Create test configuration."""
    return Config(
        bootstrap_servers=kafka_servers,
        acks="all",
        retries=3,
        auto_offset_reset="earliest"
    )


@pytest.fixture
def test_topic() -> str:
    """Generate unique test topic."""
    return f"test-topic-{uuid.uuid4().hex[:8]}-{int(time.time())}"


@pytest.fixture
def test_group() -> str:
    """Generate unique test group."""
    return f"test-group-{uuid.uuid4().hex[:8]}-{int(time.time())}"


@pytest.fixture
def mock_confluent_producer():
    """Mock confluent-kafka Producer."""
    with patch("apnamart_kafka.client.ConfluentProducer") as mock:
        producer_instance = Mock()
        producer_instance.produce = Mock()
        producer_instance.flush = Mock(return_value=0)
        producer_instance.poll = Mock()
        mock.return_value = producer_instance
        yield producer_instance


@pytest.fixture
def mock_confluent_consumer():
    """Mock confluent-kafka Consumer."""
    with patch("apnamart_kafka.client.ConfluentConsumer") as mock:
        consumer_instance = Mock()
        consumer_instance.subscribe = Mock()
        consumer_instance.poll = Mock()
        consumer_instance.commit = Mock()
        consumer_instance.close = Mock()
        mock.return_value = consumer_instance
        yield consumer_instance


@pytest.fixture
def producer(test_config: Config) -> Generator[Producer, None, None]:
    """Create test producer."""
    with Producer(test_config) as producer:
        yield producer


@pytest.fixture
def consumer(test_topic: str, test_group: str, kafka_servers: str) -> Generator[Consumer, None, None]:
    """Create test consumer."""
    config = Config(
        bootstrap_servers=kafka_servers,
        group_id=test_group,
        auto_offset_reset="earliest"
    )
    with Consumer(test_topic, config) as consumer:
        yield consumer


@pytest.fixture
def transactional_producer(kafka_servers: str) -> Generator[TransactionalProducer, None, None]:
    """Create test transactional producer."""
    tx_id = f"test-tx-{uuid.uuid4().hex[:8]}"
    config = Config(bootstrap_servers=kafka_servers)
    with TransactionalProducer(tx_id, config) as tx_producer:
        yield tx_producer


@pytest.fixture(autouse=True)
def cleanup_kafka():
    """Cleanup fixture that runs after each test."""
    yield
    # Optional: Add cleanup logic here if needed
    time.sleep(0.1)  # Small delay to ensure Kafka operations complete


# Test markers
def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line(
        "markers", "unit: marks tests as unit tests (no external dependencies)"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests (requires Kafka)"
    )
    config.addinivalue_line(
        "markers", "slow: marks tests as slow running"
    )
