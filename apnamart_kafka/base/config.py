"""Base configuration classes for extensibility."""

from typing import Any, Dict, Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class BaseKafkaConfig(BaseSettings):
    """Base configuration class for Kafka clients with extensibility."""

    model_config = SettingsConfigDict(
        env_prefix="KAFKA_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="allow",  # Allow extra fields for extensibility
    )

    # Core connection settings
    bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Comma-separated list of Kafka bootstrap servers",
    )

    # Security settings (Phase 2)
    security_protocol: str = Field(
        default="PLAINTEXT",
        description="Security protocol (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)",
    )

    ssl_keystore_location: Optional[str] = Field(
        default=None,
        description="SSL keystore location",
    )

    ssl_keystore_password: Optional[str] = Field(
        default=None,
        description="SSL keystore password",
    )

    sasl_mechanism: Optional[str] = Field(
        default=None,
        description="SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER)",
    )

    sasl_username: Optional[str] = Field(
        default=None,
        description="SASL username",
    )

    sasl_password: Optional[str] = Field(
        default=None,
        description="SASL password",
    )

    # Monitoring settings (Phase 2)
    enable_metrics: bool = Field(
        default=False,
        description="Enable metrics collection",
    )

    metrics_registry: Optional[str] = Field(
        default=None,
        description="Metrics registry type (prometheus, statsd, etc.)",
    )

    # Schema registry settings (Phase 3)
    schema_registry_url: Optional[str] = Field(
        default=None,
        description="Schema registry URL",
    )

    schema_registry_auth: Optional[str] = Field(
        default=None,
        description="Schema registry authentication",
    )

    # Cloud platform settings (Phase 3)
    cloud_provider: Optional[str] = Field(
        default=None,
        description="Cloud provider (aws, azure, gcp, confluent)",
    )

    cloud_region: Optional[str] = Field(
        default=None,
        description="Cloud region",
    )

    # Topic settings
    topic_prefix: Optional[str] = Field(
        default=None,
        description="Prefix to add to all topic names",
    )

    # Logging
    log_level: str = Field(default="INFO", description="Logging level")

    def get_topic_name(self, topic: str) -> str:
        """Get the full topic name with prefix if configured."""
        if self.topic_prefix:
            return f"{self.topic_prefix}.{topic}"
        return topic

    def get_security_config(self) -> Dict[str, Any]:
        """Get security-related configuration."""
        config: Dict[str, Any] = {"security_protocol": self.security_protocol}

        if self.ssl_keystore_location:
            config.update(
                {
                    "ssl_keystore_location": self.ssl_keystore_location,
                    "ssl_keystore_password": self.ssl_keystore_password,
                }
            )

        if self.sasl_mechanism:
            config.update(
                {
                    "sasl_mechanism": self.sasl_mechanism,
                    "sasl_username": self.sasl_username,
                    "sasl_password": self.sasl_password,
                }
            )

        return config

    def get_monitoring_config(self) -> Dict[str, Any]:
        """Get monitoring-related configuration."""
        config: Dict[str, Any] = {
            "enable_metrics": self.enable_metrics,
        }
        if self.metrics_registry:
            config["metrics_registry"] = self.metrics_registry
        return config

    def get_schema_registry_config(self) -> Dict[str, Any]:
        """Get schema registry configuration."""
        config: Dict[str, Any] = {}
        if self.schema_registry_url:
            config["schema_registry_url"] = self.schema_registry_url
        if self.schema_registry_auth:
            config["schema_registry_auth"] = self.schema_registry_auth
        return config

    def get_cloud_config(self) -> Dict[str, Any]:
        """Get cloud platform configuration."""
        config: Dict[str, Any] = {}
        if self.cloud_provider:
            config["cloud_provider"] = self.cloud_provider
        if self.cloud_region:
            config["cloud_region"] = self.cloud_region
        return config
