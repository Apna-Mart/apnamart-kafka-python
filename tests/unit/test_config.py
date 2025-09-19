"""Unit tests for Config class functionality."""

from apnamart_kafka import Config


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

    def test_config_to_producer_config(self):
        """Test conversion to producer configuration."""
        config = Config(
            bootstrap_servers="test:9092",
            acks="1",
            compression_type="snappy",
            batch_size=16384,
            linger_ms=5,
        )

        producer_config = config.to_producer_config()

        assert producer_config["bootstrap.servers"] == "test:9092"
        assert producer_config["acks"] == "1"
        assert producer_config["compression.type"] == "snappy"
        assert producer_config["batch.size"] == 16384
        assert producer_config["linger.ms"] == 5

    def test_config_to_consumer_config(self):
        """Test conversion to consumer configuration."""
        config = Config(
            bootstrap_servers="test:9092",
            group_id="test-group",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )

        consumer_config = config.to_consumer_config()

        assert consumer_config["bootstrap.servers"] == "test:9092"
        assert consumer_config["group.id"] == "test-group"
        assert consumer_config["auto.offset.reset"] == "earliest"
        assert consumer_config["enable.auto.commit"] is False

    def test_config_without_compression(self):
        """Test config conversion without compression type."""
        config = Config(bootstrap_servers="test:9092")
        producer_config = config.to_producer_config()

        # Should not have compression.type key
        assert "compression.type" not in producer_config

    def test_config_acks_conversion(self):
        """Test acks parameter conversion."""
        # Test "all" conversion to -1
        config_all = Config(acks="all")
        producer_config = config_all.to_producer_config()
        assert producer_config["acks"] == -1

        # Test numeric acks
        config_numeric = Config(acks="1")
        producer_config = config_numeric.to_producer_config()
        assert producer_config["acks"] == "1"
