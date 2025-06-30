"""Tests for consumer configuration."""

import pytest
from pydantic import ValidationError

from apnamart_kafka.consumer_config import KafkaConsumerConfig


class TestKafkaConsumerConfig:
    """Test cases for KafkaConsumerConfig."""

    def test_default_config(self):
        """Test default configuration."""
        config = KafkaConsumerConfig(group_id="test-group")

        assert config.group_id == "test-group"
        assert config.bootstrap_servers == "localhost:9092"
        assert config.auto_offset_reset == "latest"
        assert config.enable_auto_commit is True
        assert config.auto_commit_interval_ms == 5000
        assert config.max_poll_records == 500
        assert config.session_timeout_ms == 30000
        assert config.heartbeat_interval_ms == 3000

    def test_custom_config(self):
        """Test custom configuration."""
        config = KafkaConsumerConfig(
            group_id="custom-group",
            bootstrap_servers="broker1:9092,broker2:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            max_poll_records=100,
            session_timeout_ms=60000,
            heartbeat_interval_ms=10000,
            topic_prefix="myapp",
        )

        assert config.group_id == "custom-group"
        assert config.bootstrap_servers == "broker1:9092,broker2:9092"
        assert config.auto_offset_reset == "earliest"
        assert config.enable_auto_commit is False
        assert config.max_poll_records == 100
        assert config.session_timeout_ms == 60000
        assert config.heartbeat_interval_ms == 10000
        assert config.topic_prefix == "myapp"

    def test_environment_variables(self, monkeypatch):
        """Test configuration from environment variables."""
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "env-broker:9092")
        monkeypatch.setenv("KAFKA_GROUP_ID", "env-group")
        monkeypatch.setenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
        monkeypatch.setenv("KAFKA_ENABLE_AUTO_COMMIT", "false")

        config = KafkaConsumerConfig()

        assert config.bootstrap_servers == "env-broker:9092"
        assert config.group_id == "env-group"
        assert config.auto_offset_reset == "earliest"
        assert config.enable_auto_commit is False

    def test_missing_group_id(self):
        """Test that group_id is required."""
        with pytest.raises(ValidationError) as exc_info:
            KafkaConsumerConfig()

        # Check that group_id field is mentioned in the error
        error_str = str(exc_info.value)
        assert "group_id" in error_str.lower()

    def test_invalid_auto_offset_reset(self):
        """Test validation of auto_offset_reset."""
        with pytest.raises(ValidationError) as exc_info:
            KafkaConsumerConfig(group_id="test", auto_offset_reset="invalid")

        assert "auto_offset_reset must be" in str(exc_info.value)

    def test_invalid_isolation_level(self):
        """Test validation of isolation_level."""
        with pytest.raises(ValidationError) as exc_info:
            KafkaConsumerConfig(group_id="test", isolation_level="invalid")

        assert "isolation_level must be" in str(exc_info.value)

    def test_invalid_partition_assignment_strategy(self):
        """Test validation of partition assignment strategy."""
        with pytest.raises(ValidationError) as exc_info:
            KafkaConsumerConfig(
                group_id="test", partition_assignment_strategy=["invalid"]
            )

        assert "Invalid strategy" in str(exc_info.value)

    def test_valid_partition_assignment_strategies(self):
        """Test valid partition assignment strategies."""
        config = KafkaConsumerConfig(
            group_id="test",
            partition_assignment_strategy=["range", "roundrobin", "sticky"],
        )

        assert config.partition_assignment_strategy == ["range", "roundrobin", "sticky"]

    def test_numeric_validations(self):
        """Test numeric field validations."""
        # Test minimum values
        config = KafkaConsumerConfig(
            group_id="test",
            max_poll_records=1,
            max_poll_interval_ms=1000,
            session_timeout_ms=6000,
            heartbeat_interval_ms=1000,
            fetch_min_bytes=1,
            max_partition_fetch_bytes=1024,
            consumer_timeout_ms=100,
        )

        assert config.max_poll_records == 1
        assert config.session_timeout_ms == 6000

    def test_invalid_numeric_values(self):
        """Test invalid numeric values."""
        with pytest.raises(ValidationError):
            KafkaConsumerConfig(group_id="test", max_poll_records=0)

        with pytest.raises(ValidationError):
            KafkaConsumerConfig(
                group_id="test", session_timeout_ms=5000
            )  # Below minimum

    def test_to_kafka_config(self):
        """Test conversion to kafka-python config."""
        config = KafkaConsumerConfig(
            group_id="test-group",
            bootstrap_servers="broker1:9092,broker2:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            max_poll_records=100,
        )

        kafka_config = config.to_kafka_config()

        assert kafka_config["bootstrap_servers"] == ["broker1:9092", "broker2:9092"]
        assert kafka_config["group_id"] == "test-group"
        assert kafka_config["auto_offset_reset"] == "earliest"
        assert kafka_config["enable_auto_commit"] is False
        assert kafka_config["max_poll_records"] == 100

    def test_to_kafka_config_with_security(self):
        """Test conversion to kafka-python config with security."""
        config = KafkaConsumerConfig(
            group_id="test-group",
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_username="user",
            sasl_password="pass",
        )

        kafka_config = config.to_kafka_config()

        assert kafka_config["security_protocol"] == "SASL_SSL"
        assert kafka_config["sasl_mechanism"] == "PLAIN"
        assert kafka_config["sasl_username"] == "user"
        assert kafka_config["sasl_password"] == "pass"

    def test_to_kafka_config_with_group_instance_id(self):
        """Test conversion with group instance ID."""
        config = KafkaConsumerConfig(
            group_id="test-group", group_instance_id="consumer-1"
        )

        kafka_config = config.to_kafka_config()

        assert kafka_config["group_instance_id"] == "consumer-1"

    def test_get_topic_name_without_prefix(self):
        """Test topic name without prefix."""
        config = KafkaConsumerConfig(group_id="test")

        assert config.get_topic_name("my-topic") == "my-topic"

    def test_get_topic_name_with_prefix(self):
        """Test topic name with prefix."""
        config = KafkaConsumerConfig(group_id="test", topic_prefix="myapp")

        assert config.get_topic_name("my-topic") == "myapp.my-topic"

    def test_validate_consumer_config_valid(self):
        """Test valid consumer configuration validation."""
        config = KafkaConsumerConfig(
            group_id="test",
            session_timeout_ms=30000,
            heartbeat_interval_ms=3000,  # 10% of session timeout
            max_poll_interval_ms=300000,  # 10x session timeout
        )

        # Should not raise any exception
        config.validate_consumer_config()

    def test_validate_consumer_config_invalid_heartbeat(self):
        """Test invalid heartbeat interval validation."""
        config = KafkaConsumerConfig(
            group_id="test",
            session_timeout_ms=30000,
            heartbeat_interval_ms=15000,  # Too high (>1/3 of session timeout)
            max_poll_interval_ms=300000,
        )

        with pytest.raises(ValueError) as exc_info:
            config.validate_consumer_config()

        assert "heartbeat_interval_ms" in str(exc_info.value)

    def test_validate_consumer_config_invalid_session_timeout(self):
        """Test invalid session timeout validation."""
        config = KafkaConsumerConfig(
            group_id="test",
            session_timeout_ms=300000,  # Same as max poll interval
            heartbeat_interval_ms=3000,
            max_poll_interval_ms=300000,
        )

        with pytest.raises(ValueError) as exc_info:
            config.validate_consumer_config()

        assert "session_timeout_ms" in str(exc_info.value)

    def test_security_config_inheritance(self):
        """Test that security configuration is inherited from base."""
        config = KafkaConsumerConfig(
            group_id="test",
            security_protocol="SSL",
            ssl_keystore_location="/path/to/keystore",
        )

        security_config = config.get_security_config()

        assert security_config["security_protocol"] == "SSL"
        assert security_config["ssl_keystore_location"] == "/path/to/keystore"

    def test_monitoring_config_inheritance(self):
        """Test that monitoring configuration is inherited from base."""
        config = KafkaConsumerConfig(
            group_id="test", enable_metrics=True, metrics_registry="prometheus"
        )

        monitoring_config = config.get_monitoring_config()

        assert monitoring_config["enable_metrics"] is True
        assert monitoring_config["metrics_registry"] == "prometheus"
