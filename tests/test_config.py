"""Tests for configuration management."""

import os
from unittest.mock import patch

import pytest

from apnamart_kafka.config import KafkaConfig


class TestKafkaConfig:
    """Test cases for KafkaConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = KafkaConfig()

        assert config.bootstrap_servers == "localhost:9092"
        assert config.acks == "1"
        assert config.retries == 3
        assert config.retry_backoff_ms == 100
        assert config.request_timeout_ms == 30000
        assert config.max_block_ms == 60000
        assert config.compression_type is None
        assert config.max_request_size == 1048576
        assert config.topic_prefix is None
        assert config.log_level == "INFO"

    def test_custom_config(self):
        """Test custom configuration values."""
        config = KafkaConfig(
            bootstrap_servers="broker1:9092,broker2:9092",
            acks="all",
            retries=5,
            topic_prefix="myservice",
        )

        assert config.bootstrap_servers == "broker1:9092,broker2:9092"
        assert config.acks == "all"
        assert config.retries == 5
        assert config.topic_prefix == "myservice"

    @patch.dict(
        os.environ,
        {
            "KAFKA_BOOTSTRAP_SERVERS": "env-broker:9092",
            "KAFKA_ACKS": "0",
            "KAFKA_TOPIC_PREFIX": "env-prefix",
        },
    )
    def test_environment_variables(self):
        """Test configuration from environment variables."""
        config = KafkaConfig()

        assert config.bootstrap_servers == "env-broker:9092"
        assert config.acks == "0"
        assert config.topic_prefix == "env-prefix"

    def test_invalid_acks(self):
        """Test validation of invalid acks value."""
        with pytest.raises(ValueError, match="acks must be"):
            KafkaConfig(acks="invalid")

    def test_invalid_compression_type(self):
        """Test validation of invalid compression type."""
        with pytest.raises(ValueError, match="compression_type must be"):
            KafkaConfig(compression_type="invalid")

    def test_invalid_bootstrap_servers(self):
        """Test validation of empty bootstrap servers."""
        with pytest.raises(ValueError, match="bootstrap_servers cannot be empty"):
            KafkaConfig(bootstrap_servers="")

    def test_to_kafka_config(self):
        """Test conversion to kafka-python config."""
        config = KafkaConfig(
            bootstrap_servers="broker1:9092,broker2:9092",
            acks="all",
            compression_type="gzip",
        )

        kafka_config = config.to_kafka_config()

        assert kafka_config["bootstrap_servers"] == ["broker1:9092", "broker2:9092"]
        assert kafka_config["acks"] == "all"
        assert kafka_config["compression_type"] == "gzip"
        assert "retries" in kafka_config
        assert "retry_backoff_ms" in kafka_config

    def test_get_topic_name_without_prefix(self):
        """Test topic name without prefix."""
        config = KafkaConfig()
        assert config.get_topic_name("test-topic") == "test-topic"

    def test_get_topic_name_with_prefix(self):
        """Test topic name with prefix."""
        config = KafkaConfig(topic_prefix="myservice")
        assert config.get_topic_name("test-topic") == "myservice.test-topic"
