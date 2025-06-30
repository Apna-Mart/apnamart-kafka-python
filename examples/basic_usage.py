"""Basic usage examples for the Generic Kafka Producer."""

import time

from apnamart_kafka import KafkaConfig, KafkaProducer


def basic_example():
    """Basic synchronous usage example."""
    print("=== Basic Synchronous Usage ===")

    # Create producer with default configuration
    # Uses environment variables with KAFKA_ prefix if available
    producer = KafkaProducer()

    try:
        # Send a simple message
        producer.send(
            "user-events", {"user_id": 123, "action": "login", "timestamp": time.time()}
        )
        print("✅ Message sent successfully")

        # Send with message key for partitioning
        producer.send(
            topic="user-events",
            value={"user_id": 456, "action": "logout"},
            key="user-456",
        )
        print("✅ Message with key sent successfully")

        # Flush any pending messages
        producer.flush()
        print("✅ All messages flushed")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        producer.close()


def context_manager_example():
    """Context manager usage example."""
    print("\n=== Context Manager Usage ===")

    # Using context manager ensures proper cleanup
    with KafkaProducer() as producer:
        try:
            # Send multiple messages
            for i in range(5):
                producer.send(
                    "test-topic",
                    {
                        "message_id": i,
                        "content": f"Test message {i}",
                        "timestamp": time.time(),
                    },
                )

            print("✅ All messages sent successfully")

        except Exception as e:
            print(f"❌ Error: {e}")


def custom_config_example():
    """Custom configuration example."""
    print("\n=== Custom Configuration ===")

    # Create custom configuration
    config = KafkaConfig(
        bootstrap_servers="localhost:9092,localhost:9093",
        acks="all",  # Wait for all replicas to acknowledge
        retries=5,
        topic_prefix="myservice",
        compression_type="gzip",
    )

    with KafkaProducer(config=config) as producer:
        try:
            # Topic will be prefixed: "myservice.orders"
            producer.send(
                "orders",
                {
                    "order_id": "order-123",
                    "customer_id": "customer-456",
                    "amount": 99.99,
                    "items": ["item1", "item2"],
                },
            )

            print("✅ Order message sent with custom config")

        except Exception as e:
            print(f"❌ Error: {e}")


def different_serializers_example():
    """Different serialization formats example."""
    print("\n=== Different Serializers ===")

    with KafkaProducer() as producer:
        try:
            # JSON serialization (default)
            producer.send("json-topic", {"key": "value"}, serializer="json")

            # String serialization
            producer.send("string-topic", "Plain text message", serializer="string")

            # Bytes serialization
            producer.send("bytes-topic", b"Binary data", serializer="bytes")

            print("✅ Messages sent with different serializers")

        except Exception as e:
            print(f"❌ Error: {e}")


def error_handling_example():
    """Error handling example."""
    print("\n=== Error Handling ===")

    # Configuration with invalid broker (for demonstration)
    config = KafkaConfig(
        bootstrap_servers="invalid-broker:9092",
        request_timeout_ms=5000,  # Short timeout for quick failure
    )

    with KafkaProducer(config=config) as producer:
        try:
            producer.send("test-topic", {"test": "message"})
        except Exception as e:
            print(f"❌ Expected error (invalid broker): {type(e).__name__}: {e}")


def health_check_example():
    """Health check example."""
    print("\n=== Health Check ===")

    producer = KafkaProducer()

    try:
        # Perform health check
        health = producer.health_check()
        print(f"Health Status: {health['status']}")
        print(f"Connection: {health['connection']}")
        print(f"Bootstrap Servers: {health['bootstrap_servers']}")

        if health["status"] == "unhealthy":
            print(f"Error: {health.get('error', 'Unknown error')}")

    except Exception as e:
        print(f"❌ Health check error: {e}")
    finally:
        producer.close()


if __name__ == "__main__":
    print("🚀 Generic Kafka Producer - Basic Usage Examples")
    print("=" * 50)

    # Run examples
    basic_example()
    context_manager_example()
    custom_config_example()
    different_serializers_example()
    error_handling_example()
    health_check_example()

    print("\n✨ All examples completed!")
    print("\n💡 Tips:")
    print("- Set KAFKA_BOOTSTRAP_SERVERS environment variable")
    print("- Set KAFKA_TOPIC_PREFIX for automatic topic prefixing")
    print("- Use context managers for automatic cleanup")
    print("- Check health_check() for connection status")
