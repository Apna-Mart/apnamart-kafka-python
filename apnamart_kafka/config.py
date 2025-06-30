"""Configuration management for Kafka producer."""

from typing import Any, Dict, Optional, Union

from pydantic import Field, field_validator

from .base.config import BaseKafkaConfig


class KafkaConfig(BaseKafkaConfig):
    """Kafka producer configuration with environment variable support."""

    # Producer settings
    acks: str = Field(default="1", description="Acknowledgment level (0, 1, or 'all')")

    retries: int = Field(
        default=3, description="Number of retries for failed sends", ge=0
    )

    retry_backoff_ms: int = Field(
        default=100, description="Backoff time in milliseconds between retries", ge=0
    )

    request_timeout_ms: int = Field(
        default=30000, description="Request timeout in milliseconds", ge=1000
    )

    max_block_ms: int = Field(
        default=60000, description="Maximum time to block for buffer space", ge=1000
    )

    # Message settings
    compression_type: Optional[str] = Field(
        default=None, description="Compression type (gzip, snappy, lz4, zstd, or None)"
    )

    max_request_size: int = Field(
        default=1048576,  # 1MB
        description="Maximum size of a request in bytes",
        ge=1024,
    )

    @field_validator("acks")
    @classmethod
    def validate_acks(cls, v: str) -> str:
        """Validate acknowledgment level."""
        if v not in ["0", "1", "all"]:
            raise ValueError("acks must be '0', '1', or 'all'")
        return v

    @field_validator("compression_type")
    @classmethod
    def validate_compression_type(cls, v: Optional[str]) -> Optional[str]:
        """Validate compression type."""
        if v is not None and v not in ["gzip", "snappy", "lz4", "zstd"]:
            raise ValueError("compression_type must be one of: gzip, snappy, lz4, zstd")
        return v

    @field_validator("bootstrap_servers")
    @classmethod
    def validate_bootstrap_servers(cls, v: str) -> str:
        """Validate bootstrap servers format."""
        if not v or not v.strip():
            raise ValueError("bootstrap_servers cannot be empty")
        return v.strip()

    def to_kafka_config(self) -> Dict[str, Any]:
        """Convert to kafka-python configuration dictionary."""
        # Convert acks to proper type for kafka-python
        acks_value: Union[int, str] = self.acks
        if acks_value == "0":
            acks_value = 0
        elif acks_value == "1":
            acks_value = 1
        elif acks_value == "all":
            acks_value = "all"

        config = {
            "bootstrap_servers": self.bootstrap_servers.split(","),
            "acks": acks_value,
            "retries": self.retries,
            "retry_backoff_ms": self.retry_backoff_ms,
            "request_timeout_ms": self.request_timeout_ms,
            "max_block_ms": self.max_block_ms,
            "max_request_size": self.max_request_size,
        }

        if self.compression_type:
            config["compression_type"] = self.compression_type

        return config
