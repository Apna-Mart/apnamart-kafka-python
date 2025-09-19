#!/usr/bin/env python3
"""
Getting Started with ApnaMart Kafka Python Client

This example shows the absolute basics - how to send and receive messages.
Perfect for developers new to Kafka or this library.
"""

import time

from apnamart_kafka import Config, Consumer, Producer, consume, send


def quick_start_example():
    """The fastest way to get started - 5 lines of code."""
    print(" Quick Start Example")
    print("-" * 30)

    # 1. Send a message (one-liner)
    send("my-topic", {"message": "Hello Kafka!", "timestamp": time.time()})
    print(" Message sent!")

    # 2. Receive messages (iterator style)
    print(" Waiting for messages...")
    for message in consume("my-topic", group_id="getting-started"):
        print(f"Received: {message.value}")
        break  # Just show one message

    print(" Message received!")


def basic_producer_example():
    """How to use Producer class for more control."""
    print("\n Basic Producer Example")
    print("-" * 30)

    # Create producer with default settings
    with Producer() as producer:
        # Send different types of data
        producer.send("events", {"event": "user_login", "user_id": 123})
        producer.send("events", "Simple string message")
        producer.send("events", {"data": [1, 2, 3, 4, 5]})

        # Make sure all messages are sent
        producer.flush()
        print(" All messages sent successfully!")


def basic_consumer_example():
    """How to use Consumer class for more control."""
    print("\n Basic Consumer Example")
    print("-" * 30)

    # Configure consumer
    config = Config(
        group_id="my-app",
        auto_offset_reset="earliest",  # Start from beginning
    )

    with Consumer("events", config) as consumer:
        print(" Listening for messages (5 second timeout)...")

        # Poll for a single message
        message = consumer.poll(timeout=5.0)

        if message:
            print(f" Received: {message.value}")
            print(f"   Topic: {message.topic}")
            print(f"   Key: {message.key}")

            # Manually commit the message
            consumer.commit(message)
            print(" Message processed and committed")
        else:
            print("⏰ No messages received (timeout)")


def configuration_example():
    """How to configure Kafka connection and behavior."""
    print("\n️  Configuration Example")
    print("-" * 30)

    # Custom configuration
    config = Config(
        bootstrap_servers="localhost:9092",  # Kafka server address
        acks="all",  # Wait for all replicas
        retries=3,  # Retry failed sends
        compression_type="gzip",  # Compress messages
        group_id="my-application",  # Consumer group
        auto_offset_reset="latest",  # Start from newest messages
    )

    print(" Configuration created:")
    print(f"   Servers: {config.bootstrap_servers}")
    print(f"   Compression: {config.compression_type}")
    print(f"   Consumer Group: {config.group_id}")

    # Use configuration with producer
    with Producer(config) as producer:
        producer.send(
            "configured-topic",
            {
                "message": "This message is compressed and will be retried if it fails!",
                "config": "custom",
            },
        )
        producer.flush()
        print(" Message sent with custom configuration!")


def error_handling_example():
    """How to handle common errors gracefully."""
    print("\n️  Error Handling Example")
    print("-" * 30)

    from apnamart_kafka import ConsumerError, ProducerError

    # Producer error handling
    try:
        with Producer() as producer:
            # This might fail if Kafka is not running
            producer.send("test-topic", {"test": "data"})
            producer.flush()
            print(" Producer: No errors!")

    except ProducerError as e:
        print(f" Producer Error: {e}")
        print(" Make sure Kafka is running on localhost:9092")

    # Consumer error handling
    try:
        config = Config(group_id="error-test", auto_offset_reset="earliest")
        with Consumer("test-topic", config) as consumer:
            message = consumer.poll(timeout=1.0)
            if message:
                print(" Consumer: No errors!")
            else:
                print("ℹ️  Consumer: No messages available")

    except ConsumerError as e:
        print(f" Consumer Error: {e}")
        print(" Check your Kafka connection and topic name")


def main():
    """Run all basic examples."""
    print(" ApnaMart Kafka Python - Getting Started Examples")
    print("=" * 55)
    print("These examples show the basics of sending and receiving messages.")
    print()

    try:
        # Run examples in order
        quick_start_example()
        basic_producer_example()
        basic_consumer_example()
        configuration_example()
        error_handling_example()

        print("\n All examples completed!")
        print()
        print(" Next Steps:")
        print("• Check out examples/advanced/ for more complex patterns")
        print("• Read examples/patterns/ for architectural examples")
        print("• See examples/production/ for deployment guidance")

    except Exception as e:
        print(f"\n Example failed: {e}")
        print("\n Common Issues:")
        print("• Make sure Kafka is running: docker run -p 9092:9092 apache/kafka")
        print("• Check network connectivity to localhost:9092")
        print("• Verify you have installed: uv add apnamart-kafka-python")


if __name__ == "__main__":
    main()
