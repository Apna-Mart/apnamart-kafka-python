"""Configuration presets for common use cases."""

from .config import KafkaConfig
from .consumer_config import KafkaConsumerConfig


class ProducerPresets:
    """Pre-configured producer settings for common scenarios."""

    @staticmethod
    def high_throughput() -> KafkaConfig:
        """Configuration optimized for high throughput.
        
        - Larger batch sizes
        - Compression enabled
        - Increased buffer memory
        - Longer linger time to accumulate more messages
        """
        return KafkaConfig(
            batch_size=65536,  # 64KB
            linger_ms=100,  # Wait up to 100ms for batching
            compression_type="lz4",  # Fast compression
            buffer_memory=67108864,  # 64MB
            max_request_size=5242880,  # 5MB
            acks="1",  # Leader acknowledgment only
        )

    @staticmethod
    def low_latency() -> KafkaConfig:
        """Configuration optimized for low latency.
        
        - No batching delay
        - Smaller batch sizes
        - No compression
        - Faster acknowledgments
        """
        return KafkaConfig(
            batch_size=0,  # Disable batching
            linger_ms=0,  # Send immediately
            compression_type=None,  # No compression
            acks="1",  # Leader acknowledgment only
            max_block_ms=5000,  # Fail fast if buffer full
        )

    @staticmethod
    def reliable() -> KafkaConfig:
        """Configuration optimized for reliability.
        
        - All replicas acknowledgment
        - Higher retry count
        - Compression for data integrity
        - Conservative timeouts
        """
        return KafkaConfig(
            acks="all",  # All in-sync replicas must acknowledge
            retries=10,  # More retries
            retry_backoff_ms=500,  # Longer backoff
            compression_type="gzip",  # Better compression ratio
            request_timeout_ms=60000,  # 1 minute timeout
            max_block_ms=120000,  # 2 minute block timeout
        )


class ConsumerPresets:
    """Pre-configured consumer settings for common scenarios."""

    @staticmethod
    def batch_processing(bootstrap_servers: str = "localhost:9092") -> KafkaConsumerConfig:
        """Configuration optimized for batch processing.
        
        - Large batch sizes
        - Longer poll intervals
        - Manual commit for better control
        """
        return KafkaConsumerConfig(
            bootstrap_servers=bootstrap_servers,
            group_id="batch-processing-group",
            max_poll_records=1000,  # Large batches
            max_poll_interval_ms=600000,  # 10 minutes for processing
            enable_auto_commit=False,  # Manual commit after batch
            fetch_min_bytes=1048576,  # 1MB minimum fetch
            fetch_max_wait_ms=1000,  # Wait up to 1s for min bytes
        )

    @staticmethod
    def real_time(bootstrap_servers: str = "localhost:9092") -> KafkaConsumerConfig:
        """Configuration optimized for real-time processing.
        
        - Small batch sizes
        - Short poll intervals
        - Auto-commit for simplicity
        """
        return KafkaConsumerConfig(
            bootstrap_servers=bootstrap_servers,
            group_id="real-time-group",
            max_poll_records=10,  # Small batches
            max_poll_interval_ms=30000,  # 30 seconds
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,  # Commit every second
            fetch_min_bytes=1,  # Don't wait for data accumulation
            fetch_max_wait_ms=100,  # Low wait time
        )

    @staticmethod
    def parallel_processing(bootstrap_servers: str = "localhost:9092") -> KafkaConsumerConfig:
        """Configuration optimized for parallel processing within a consumer.
        
        - Medium batch sizes
        - Manual commits for safety
        - Balanced timeouts
        """
        return KafkaConsumerConfig(
            bootstrap_servers=bootstrap_servers,
            group_id="parallel-processing-group",
            max_poll_records=100,  # Medium batches for parallelism
            max_poll_interval_ms=300000,  # 5 minutes
            enable_auto_commit=False,  # Manual commit required
            session_timeout_ms=45000,  # 45 seconds
            heartbeat_interval_ms=15000,  # 15 seconds
        )


class ConfigPresets:
    """Main class providing access to all configuration presets."""
    
    producer = ProducerPresets
    consumer = ConsumerPresets