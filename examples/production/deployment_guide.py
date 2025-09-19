#!/usr/bin/env python3
"""
Production Deployment Guide and Best Practices

This example demonstrates production-ready configurations, monitoring,
error handling, and deployment patterns for Kafka applications.
"""

import logging
import os
import signal
import threading
import time
from contextlib import contextmanager
from typing import Any, Callable, Dict, Optional

from apnamart_kafka import Config, Consumer, ConsumerError, Producer, ProducerError


# Production logging configuration
def setup_production_logging():
    """Set up production-grade logging."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - [%(thread)d] - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("/var/log/kafka-app.log")
            if os.path.exists("/var/log")
            else logging.StreamHandler(),
        ],
    )
    return logging.getLogger(__name__)


logger = setup_production_logging()


class ProductionConfig:
    """Production configuration management."""

    @staticmethod
    def from_environment() -> Config:
        """Create configuration from environment variables."""
        return Config(
            # Basic connection
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            # Security (enable for production)
            security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
            sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM"),
            sasl_username=os.getenv("KAFKA_SASL_USERNAME"),
            sasl_password=os.getenv("KAFKA_SASL_PASSWORD"),
            ssl_ca_location=os.getenv("KAFKA_SSL_CA_LOCATION"),
            ssl_certificate_location=os.getenv("KAFKA_SSL_CERT_LOCATION"),
            ssl_key_location=os.getenv("KAFKA_SSL_KEY_LOCATION"),
            # Producer settings for reliability
            acks=os.getenv("KAFKA_ACKS", "all"),
            retries=int(os.getenv("KAFKA_RETRIES", "10")),
            retry_backoff_ms=int(os.getenv("KAFKA_RETRY_BACKOFF_MS", "1000")),
            max_in_flight_requests_per_connection=int(
                os.getenv("KAFKA_MAX_IN_FLIGHT", "1")
            ),
            enable_idempotence=os.getenv("KAFKA_ENABLE_IDEMPOTENCE", "true").lower()
            == "true",
            # Performance tuning
            batch_size=int(os.getenv("KAFKA_BATCH_SIZE", "16384")),
            linger_ms=int(os.getenv("KAFKA_LINGER_MS", "10")),
            compression_type=os.getenv("KAFKA_COMPRESSION_TYPE", "snappy"),
            buffer_memory=int(os.getenv("KAFKA_BUFFER_MEMORY", "33554432")),
            # Consumer settings
            session_timeout_ms=int(os.getenv("KAFKA_SESSION_TIMEOUT_MS", "30000")),
            heartbeat_interval_ms=int(
                os.getenv("KAFKA_HEARTBEAT_INTERVAL_MS", "10000")
            ),
            max_poll_records=int(os.getenv("KAFKA_MAX_POLL_RECORDS", "500")),
            auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
            # Timeouts
            request_timeout_ms=int(os.getenv("KAFKA_REQUEST_TIMEOUT_MS", "60000")),
            delivery_timeout_ms=int(os.getenv("KAFKA_DELIVERY_TIMEOUT_MS", "120000")),
        )

    @staticmethod
    def get_high_throughput_config() -> Config:
        """Configuration optimized for high throughput."""
        return Config(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            acks="1",  # Leader acknowledgment only
            retries=5,
            batch_size=32768,  # Larger batches
            linger_ms=50,  # Wait longer to build batches
            compression_type="snappy",
            buffer_memory=67108864,  # 64MB buffer
            max_in_flight_requests_per_connection=5,
        )

    @staticmethod
    def get_low_latency_config() -> Config:
        """Configuration optimized for low latency."""
        return Config(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            acks="1",
            retries=3,
            batch_size=1,  # Send immediately
            linger_ms=0,  # No waiting
            compression_type="none",  # No compression overhead
            max_in_flight_requests_per_connection=1,
        )


class GracefulShutdownHandler:
    """Handle graceful application shutdown."""

    def __init__(self):
        self.shutdown_event = threading.Event()
        self.consumers: list[Consumer] = []
        self.producers: list[Producer] = []

    def register_consumer(self, consumer: Consumer):
        """Register a consumer for graceful shutdown."""
        self.consumers.append(consumer)

    def register_producer(self, producer: Producer):
        """Register a producer for graceful shutdown."""
        self.producers.append(producer)

    def setup_signal_handlers(self):
        """Set up signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.shutdown_event.set()

    def shutdown(self):
        """Perform graceful shutdown."""
        logger.info("Starting graceful shutdown...")

        # Close consumers first
        for consumer in self.consumers:
            try:
                consumer.close()
                logger.info("Consumer closed successfully")
            except Exception as e:
                logger.error(f"Error closing consumer: {e}")

        # Flush and close producers
        for producer in self.producers:
            try:
                producer.flush(timeout=30)  # Wait up to 30 seconds
                producer.close()
                logger.info("Producer closed successfully")
            except Exception as e:
                logger.error(f"Error closing producer: {e}")

        logger.info("Graceful shutdown completed")


class HealthChecker:
    """Health checking for Kafka components."""

    def __init__(self, config: Config):
        self.config = config

    def check_kafka_connectivity(self) -> bool:
        """Check if Kafka cluster is reachable."""
        try:
            with Producer(self.config) as producer:
                # Try to get metadata
                producer.flush()
                return True
        except Exception as e:
            logger.error(f"Kafka connectivity check failed: {e}")
            return False

    def check_topic_exists(self, topic: str) -> bool:
        """Check if a topic exists."""
        try:
            with Producer(self.config) as producer:
                # Try to send a dummy message to check topic
                producer.send(topic, {"health_check": True})
                producer.flush()
                return True
        except Exception as e:
            logger.error(f"Topic {topic} health check failed: {e}")
            return False


class MetricsCollector:
    """Collect application metrics."""

    def __init__(self):
        self.metrics = {
            "messages_produced": 0,
            "messages_consumed": 0,
            "production_errors": 0,
            "consumption_errors": 0,
            "last_message_timestamp": 0,
        }
        self.lock = threading.Lock()

    def increment_produced(self):
        """Increment produced message counter."""
        with self.lock:
            self.metrics["messages_produced"] += 1
            self.metrics["last_message_timestamp"] = time.time()

    def increment_consumed(self):
        """Increment consumed message counter."""
        with self.lock:
            self.metrics["messages_consumed"] += 1

    def increment_production_error(self):
        """Increment production error counter."""
        with self.lock:
            self.metrics["production_errors"] += 1

    def increment_consumption_error(self):
        """Increment consumption error counter."""
        with self.lock:
            self.metrics["consumption_errors"] += 1

    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics snapshot."""
        with self.lock:
            return self.metrics.copy()

    def log_metrics(self):
        """Log current metrics."""
        metrics = self.get_metrics()
        logger.info(f"Metrics: {metrics}")


class RobustProducer:
    """Production-ready producer with error handling and monitoring."""

    def __init__(self, config: Config, metrics: MetricsCollector):
        self.config = config
        self.metrics = metrics
        self.producer: Optional[Producer] = None

    @contextmanager
    def get_producer(self):
        """Get producer with connection management."""
        try:
            if not self.producer:
                self.producer = Producer(self.config)
            yield self.producer
        except Exception as e:
            logger.error(f"Producer error: {e}")
            raise
        finally:
            # Keep connection open for reuse in production
            pass

    def send_with_retry(
        self, topic: str, message: Any, key: str = None, max_retries: int = 3
    ) -> bool:
        """Send message with retry logic."""
        for attempt in range(max_retries + 1):
            try:
                with self.get_producer() as producer:
                    producer.send(topic, message, key=key)
                    producer.flush()
                    self.metrics.increment_produced()
                    return True

            except ProducerError as e:
                logger.warning(f"Send attempt {attempt + 1} failed: {e}")
                if attempt == max_retries:
                    self.metrics.increment_production_error()
                    logger.error(
                        f"Failed to send message after {max_retries + 1} attempts"
                    )
                    return False
                time.sleep(2**attempt)  # Exponential backoff

        return False

    def close(self):
        """Close producer connection."""
        if self.producer:
            try:
                self.producer.flush(timeout=30)
                self.producer.close()
            except Exception as e:
                logger.error(f"Error closing producer: {e}")


class RobustConsumer:
    """Production-ready consumer with error handling and monitoring."""

    def __init__(
        self,
        topic: str,
        config: Config,
        metrics: MetricsCollector,
        message_handler: Callable[[Any], bool],
    ):
        self.topic = topic
        self.config = config
        self.metrics = metrics
        self.message_handler = message_handler
        self.running = False

    def start_consuming(self, shutdown_event: threading.Event):
        """Start consuming messages."""
        self.running = True
        logger.info(f"Starting consumer for topic: {self.topic}")

        try:
            with Consumer(self.topic, self.config) as consumer:
                while self.running and not shutdown_event.is_set():
                    try:
                        message = consumer.poll(timeout=1.0)

                        if message:
                            # Process message
                            success = self.message_handler(message.value)

                            if success:
                                consumer.commit(message)
                                self.metrics.increment_consumed()
                            else:
                                logger.warning(
                                    "Message processing failed, will not commit"
                                )
                                self.metrics.increment_consumption_error()

                    except ConsumerError as e:
                        logger.error(f"Consumer error: {e}")
                        self.metrics.increment_consumption_error()
                        time.sleep(5)  # Back off on error

                    except Exception as e:
                        logger.error(f"Unexpected error in consumer: {e}")
                        self.metrics.increment_consumption_error()

        except Exception as e:
            logger.error(f"Fatal consumer error: {e}")
        finally:
            logger.info("Consumer stopped")

    def stop(self):
        """Stop consuming."""
        self.running = False


def production_deployment_example():
    """Demonstrate production deployment patterns."""
    print(" Production Deployment Example")
    print("-" * 35)

    # Setup
    config = ProductionConfig.from_environment()
    metrics = MetricsCollector()
    shutdown_handler = GracefulShutdownHandler()
    health_checker = HealthChecker(config)

    # Setup signal handlers
    shutdown_handler.setup_signal_handlers()

    # Health checks
    logger.info("Performing health checks...")
    if not health_checker.check_kafka_connectivity():
        logger.error("Kafka connectivity check failed!")
        return

    # Create robust producer
    robust_producer = RobustProducer(config, metrics)
    shutdown_handler.register_producer(robust_producer.producer)

    # Message handler for consumer
    def handle_message(message_data: Any) -> bool:
        """Handle incoming message."""
        try:
            logger.info(f"Processing message: {message_data}")
            # Simulate processing
            time.sleep(0.1)
            return True
        except Exception as e:
            logger.error(f"Message processing error: {e}")
            return False

    # Create robust consumer
    consumer_config = Config(
        **config.to_consumer_config(), group_id="production-consumer-group"
    )

    robust_consumer = RobustConsumer(
        topic="production-topic",
        config=consumer_config,
        metrics=metrics,
        message_handler=handle_message,
    )

    # Start consumer in background thread
    consumer_thread = threading.Thread(
        target=robust_consumer.start_consuming, args=(shutdown_handler.shutdown_event,)
    )
    consumer_thread.start()

    try:
        # Simulate production workload
        logger.info("Starting production workload simulation...")

        for i in range(20):
            if shutdown_handler.shutdown_event.is_set():
                break

            message = {
                "id": i,
                "timestamp": time.time(),
                "data": f"Production message {i}",
                "metadata": {"version": "1.0", "environment": "production"},
            }

            success = robust_producer.send_with_retry(
                topic="production-topic", message=message, key=f"key-{i}"
            )

            if not success:
                logger.error(f"Failed to send message {i}")

            # Log metrics every 10 messages
            if i % 10 == 0:
                metrics.log_metrics()

            time.sleep(0.5)

    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        # Graceful shutdown
        robust_consumer.stop()
        shutdown_handler.shutdown()
        consumer_thread.join(timeout=10)

        # Final metrics
        logger.info("Final metrics:")
        metrics.log_metrics()


def configuration_examples():
    """Show different configuration examples."""
    print("\n️  Configuration Examples")
    print("-" * 30)

    # High throughput config
    ht_config = ProductionConfig.get_high_throughput_config()
    print(" High Throughput Config:")
    print(f"  Batch Size: {ht_config.batch_size}")
    print(f"  Linger MS: {ht_config.linger_ms}")
    print(f"  Compression: {ht_config.compression_type}")

    # Low latency config
    ll_config = ProductionConfig.get_low_latency_config()
    print("\n Low Latency Config:")
    print(f"  Batch Size: {ll_config.batch_size}")
    print(f"  Linger MS: {ll_config.linger_ms}")
    print(f"  Compression: {ll_config.compression_type}")


def monitoring_example():
    """Demonstrate monitoring and observability."""
    print("\n Monitoring Example")
    print("-" * 20)

    metrics = MetricsCollector()

    # Simulate some activity
    for i in range(10):
        metrics.increment_produced()
        if i % 3 == 0:
            metrics.increment_consumed()
        if i % 7 == 0:
            metrics.increment_production_error()

    # Show metrics
    current_metrics = metrics.get_metrics()
    print("Current Metrics:")
    for key, value in current_metrics.items():
        print(f"  {key}: {value}")


def main():
    """Run production deployment examples."""
    print(" Production Deployment Guide")
    print("=" * 35)
    print("Best practices for deploying Kafka applications in production.")
    print()

    try:
        configuration_examples()
        monitoring_example()
        production_deployment_example()

        print("\n Production deployment examples completed!")
        print()
        print(" Production Checklist:")
        print("• Environment-based configuration")
        print("• Graceful shutdown handling")
        print("• Health checks and monitoring")
        print("• Error handling and retries")
        print("• Security configuration")
        print("• Performance tuning")

    except Exception as e:
        logger.error(f"Example failed: {e}")
        print("\n Production Requirements:")
        print("• Kafka cluster properly configured")
        print("• Environment variables set")
        print("• Monitoring systems in place")
        print("• Log aggregation configured")


if __name__ == "__main__":
    main()
