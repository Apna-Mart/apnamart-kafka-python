"""Consumer-specific configuration for Kafka consumer."""

from typing import Any, Dict, List, Optional

from pydantic import Field, field_validator

from .base.config import BaseKafkaConfig


class KafkaConsumerConfig(BaseKafkaConfig):
    """Kafka consumer configuration with consumer-specific settings."""

    # Consumer Group Settings
    group_id: str = Field(
        description="Consumer group ID - required for consumer group coordination"
    )

    group_instance_id: Optional[str] = Field(
        default=None,
        description="Unique identifier for this consumer instance within the group",
    )

    # Offset Management
    auto_offset_reset: str = Field(
        default="latest",
        description="What to do when there is no initial offset ('earliest', 'latest', 'none')",
    )

    enable_auto_commit: bool = Field(
        default=True,
        description="Enable automatic offset commits",
    )

    auto_commit_interval_ms: int = Field(
        default=5000,
        description="Frequency in milliseconds for auto-committing offsets",
        ge=0,
    )

    # Consumer Behavior
    max_poll_records: int = Field(
        default=500,
        description="Maximum number of records returned per poll()",
        ge=1,
        le=10000,
    )

    max_poll_interval_ms: int = Field(
        default=300000,  # 5 minutes
        description="Maximum delay between poll() calls before consumer is considered dead",
        ge=1000,
    )

    session_timeout_ms: int = Field(
        default=30000,  # 30 seconds
        description="Timeout for consumer session within group",
        ge=6000,  # Min 6 seconds
        le=1800000,  # Max 30 minutes
    )

    heartbeat_interval_ms: int = Field(
        default=3000,  # 3 seconds
        description="Expected time between heartbeats to the consumer coordinator",
        ge=1000,
    )

    # Fetch Settings
    fetch_min_bytes: int = Field(
        default=1,
        description="Minimum amount of data server should return for fetch request",
        ge=1,
    )

    fetch_max_bytes: int = Field(
        default=52428800,  # 50MB
        description="Maximum amount of data server should return for fetch request",
        ge=1024,
    )

    fetch_max_wait_ms: int = Field(
        default=500,
        description="Maximum amount of time server will block before answering fetch request",
        ge=0,
    )

    max_partition_fetch_bytes: int = Field(
        default=1048576,  # 1MB
        description="Maximum amount of data per partition server returns",
        ge=1024,
    )

    # Consumer Timeout
    consumer_timeout_ms: int = Field(
        default=1000,
        description="Timeout for consumer operations in milliseconds",
        ge=100,
    )

    # Message Processing
    check_crcs: bool = Field(
        default=True,
        description="Check CRC32 of records consumed",
    )

    exclude_internal_topics: bool = Field(
        default=True,
        description="Whether to exclude internal topics matching pattern",
    )

    # Partition Assignment Strategy
    partition_assignment_strategy: List[str] = Field(
        default=["range", "roundrobin"],
        description="List of partition assignment strategies",
    )

    # Isolation Level
    isolation_level: str = Field(
        default="read_uncommitted",
        description="Controls how transactional messages are read",
    )

    @field_validator("auto_offset_reset")
    @classmethod
    def validate_auto_offset_reset(cls, v: str) -> str:
        """Validate auto offset reset strategy."""
        if v not in ["earliest", "latest", "none"]:
            raise ValueError(
                "auto_offset_reset must be 'earliest', 'latest', or 'none'"
            )
        return v

    @field_validator("isolation_level")
    @classmethod
    def validate_isolation_level(cls, v: str) -> str:
        """Validate isolation level."""
        if v not in ["read_uncommitted", "read_committed"]:
            raise ValueError(
                "isolation_level must be 'read_uncommitted' or 'read_committed'"
            )
        return v

    @field_validator("heartbeat_interval_ms")
    @classmethod
    def validate_heartbeat_interval(cls, v: int, info: Any) -> int:
        """Validate heartbeat interval against session timeout."""
        # Note: We can't access session_timeout_ms here directly in v2 validation
        # This will be checked in the consumer implementation
        return v

    @field_validator("partition_assignment_strategy")
    @classmethod
    def validate_partition_assignment_strategy(cls, v: List[str]) -> List[str]:
        """Validate partition assignment strategies."""
        valid_strategies = ["range", "roundrobin", "sticky", "cooperative-sticky"]
        for strategy in v:
            if strategy not in valid_strategies:
                raise ValueError(
                    f"Invalid strategy '{strategy}'. Must be one of: {valid_strategies}"
                )
        return v

    def to_kafka_config(self) -> Dict[str, Any]:
        """Convert to kafka-python consumer configuration dictionary."""
        config = {
            "bootstrap_servers": self.bootstrap_servers.split(","),
            "group_id": self.group_id,
            "auto_offset_reset": self.auto_offset_reset,
            "enable_auto_commit": self.enable_auto_commit,
            "auto_commit_interval_ms": self.auto_commit_interval_ms,
            "max_poll_records": self.max_poll_records,
            "max_poll_interval_ms": self.max_poll_interval_ms,
            "session_timeout_ms": self.session_timeout_ms,
            "heartbeat_interval_ms": self.heartbeat_interval_ms,
            "fetch_min_bytes": self.fetch_min_bytes,
            "fetch_max_bytes": self.fetch_max_bytes,
            "fetch_max_wait_ms": self.fetch_max_wait_ms,
            "max_partition_fetch_bytes": self.max_partition_fetch_bytes,
            "consumer_timeout_ms": self.consumer_timeout_ms,
            "check_crcs": self.check_crcs,
            "exclude_internal_topics": self.exclude_internal_topics,
        }

        # Add group instance ID if specified
        if self.group_instance_id:
            config["group_instance_id"] = self.group_instance_id

        # Add security configuration
        security_config = self.get_security_config()
        if security_config.get("security_protocol") != "PLAINTEXT":
            config.update(security_config)

        return config

    def validate_consumer_config(self) -> None:
        """Validate consumer-specific configuration constraints."""
        # Heartbeat should be less than 1/3 of session timeout
        if self.heartbeat_interval_ms >= self.session_timeout_ms / 3:
            raise ValueError(
                f"heartbeat_interval_ms ({self.heartbeat_interval_ms}) should be less than "
                f"session_timeout_ms/3 ({self.session_timeout_ms / 3})"
            )

        # Session timeout should be less than max poll interval
        if self.session_timeout_ms >= self.max_poll_interval_ms:
            raise ValueError(
                f"session_timeout_ms ({self.session_timeout_ms}) should be less than "
                f"max_poll_interval_ms ({self.max_poll_interval_ms})"
            )
