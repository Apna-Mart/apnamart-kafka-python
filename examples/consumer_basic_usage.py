"""Basic usage examples for the Generic Kafka Consumer."""

import time

from apnamart_kafka import KafkaConsumer, KafkaConsumerConfig


def basic_consumer_example():
    """Basic synchronous consumer usage example."""
    print("=== Basic Consumer Usage ===")

    # Create consumer configuration
    config = KafkaConsumerConfig(
        bootstrap_servers="localhost:9092",
        group_id="basic-consumer-group",
        auto_offset_reset="earliest",  # Start from beginning for demo
    )

    # Create consumer with configuration
    consumer = KafkaConsumer(config=config)

    try:
        # Subscribe to topics
        consumer.subscribe(["user-events", "system-metrics"])
        print("✅ Subscribed to topics: user-events, system-metrics")

        # Poll for messages (basic approach)
        print("🔍 Polling for messages...")
        for i in range(5):  # Poll 5 times for demo
            messages = consumer.poll(timeout_ms=2000)

            if messages:
                print(f"📦 Received {len(messages)} messages:")
                for message in messages:
                    print(f"  Topic: {message.topic}")
                    print(f"  Partition: {message.partition}")
                    print(f"  Offset: {message.offset}")
                    print(f"  Key: {message.key}")
                    print(f"  Value: {message.value}")
                    print(f"  Timestamp: {message.timestamp}")
                    print("  ---")
            else:
                print("  No messages received in this poll")

            time.sleep(1)

        # Commit offsets
        consumer.commit_offsets()
        print("✅ Offsets committed")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        consumer.close()


def continuous_consumer_example():
    """Continuous message consumption example."""
    print("\n=== Continuous Consumer Usage ===")

    # Create consumer with different settings
    config = KafkaConsumerConfig(
        bootstrap_servers="localhost:9092",
        group_id="continuous-consumer-group",
        auto_offset_reset="latest",  # Only new messages
        enable_auto_commit=True,  # Auto-commit offsets
        auto_commit_interval_ms=1000,  # Commit every second
    )

    with KafkaConsumer(config=config) as consumer:
        try:
            # Subscribe to topics
            consumer.subscribe(["user-events"])
            print("✅ Subscribed to user-events (waiting for new messages)")

            # Consume messages continuously
            message_count = 0
            start_time = time.time()

            for message in consumer.consume(timeout_ms=1000):
                message_count += 1
                print(
                    f"📨 Message {message_count}: {message.topic}:{message.partition}:{message.offset}"
                )
                print(f"   Key: {message.key}")
                print(f"   Value: {message.value}")

                # Stop after 10 messages or 30 seconds for demo
                if message_count >= 10 or (time.time() - start_time) > 30:
                    break

            print(f"✅ Processed {message_count} messages")

        except KeyboardInterrupt:
            print("🛑 Consumer stopped by user")
        except Exception as e:
            print(f"❌ Error: {e}")


def manual_partition_assignment_example():
    """Manual partition assignment example."""
    print("\n=== Manual Partition Assignment ===")

    from kafka.structs import TopicPartition

    config = KafkaConsumerConfig(
        bootstrap_servers="localhost:9092",
        group_id="manual-assignment-group",
        auto_offset_reset="earliest",
    )

    with KafkaConsumer(config=config) as consumer:
        try:
            # Manually assign specific partitions
            partitions = [
                TopicPartition("user-events", 0),
                TopicPartition("user-events", 1),
            ]

            consumer.assign(partitions)
            print("✅ Manually assigned partitions:", partitions)

            # Seek to specific positions
            consumer.seek_to_beginning()
            print("✅ Seeked to beginning of partitions")

            # Consume a few messages
            messages = consumer.poll(timeout_ms=5000, max_records=5)

            if messages:
                print(f"📦 Received {len(messages)} messages:")
                for message in messages:
                    print(
                        f"  {message.topic}:{message.partition}:{message.offset} = {message.value}"
                    )
            else:
                print("  No messages found in assigned partitions")

        except Exception as e:
            print(f"❌ Error: {e}")


def offset_management_example():
    """Offset management and seeking example."""
    print("\n=== Offset Management ===")

    from kafka.structs import OffsetAndMetadata, TopicPartition

    config = KafkaConsumerConfig(
        bootstrap_servers="localhost:9092",
        group_id="offset-management-group",
        enable_auto_commit=False,  # Manual offset management
    )

    with KafkaConsumer(config=config) as consumer:
        try:
            # Subscribe to topic
            consumer.subscribe(["user-events"])
            print("✅ Subscribed to user-events with manual offset management")

            # Consume some messages
            messages = consumer.poll(timeout_ms=3000, max_records=3)

            if messages:
                print(f"📦 Received {len(messages)} messages")

                # Process messages and manually commit specific offsets
                for message in messages:
                    print(
                        f"  Processing: {message.topic}:{message.partition}:{message.offset}"
                    )

                    # Simulate message processing
                    time.sleep(0.1)

                    # Manually commit this specific message's offset
                    tp = TopicPartition(message.topic, message.partition)
                    offset_metadata = OffsetAndMetadata(message.offset + 1, "processed")

                    consumer.commit_offsets({tp: offset_metadata})
                    print(f"    ✅ Committed offset {message.offset + 1}")
            else:
                print("  No messages to process")

        except Exception as e:
            print(f"❌ Error: {e}")


def consumer_with_monitoring_example():
    """Consumer with monitoring and metrics example."""
    print("\n=== Consumer with Monitoring ===")

    from apnamart_kafka import BasicMonitoringHandler, MetricsCollector
    from apnamart_kafka.common.monitoring import MonitoringCollectorHandler

    # Create monitoring components
    metrics_collector = MetricsCollector()
    basic_handler = BasicMonitoringHandler(log_level="INFO")
    metrics_handler = MonitoringCollectorHandler(metrics_collector)

    config = KafkaConsumerConfig(
        bootstrap_servers="localhost:9092",
        group_id="monitored-consumer-group",
        topic_prefix="monitored-app",
    )

    with KafkaConsumer(config=config) as consumer:
        try:
            # Add monitoring handlers
            consumer.add_monitoring_handler(basic_handler)
            consumer.add_monitoring_handler(metrics_handler)
            print("🔍 Monitoring handlers added")

            # Subscribe to topics
            consumer.subscribe(["user-events", "system-metrics"])

            # Consume messages with monitoring
            message_count = 0
            for message in consumer.consume(timeout_ms=2000):
                message_count += 1
                print(f"📨 Monitored message {message_count}: {message.value}")

                if message_count >= 5:
                    break

            # Get metrics
            metrics = metrics_collector.get_metrics()
            print("\n📊 Metrics collected:")
            print(
                f"  Messages received: {metrics['global']['messages_sent']}"
            )  # Reusing sent metric
            print(f"  Bytes received: {metrics['global']['bytes_sent']}")

            # Get consumer metrics
            consumer_metrics = consumer.get_metrics()
            print(f"\n🔧 Consumer metrics: {consumer_metrics}")

        except Exception as e:
            print(f"❌ Error: {e}")


def error_handling_example():
    """Error handling and recovery example."""
    print("\n=== Error Handling ===")

    # Configuration with invalid broker (for demonstration)
    config = KafkaConsumerConfig(
        bootstrap_servers="invalid-broker:9092",
        group_id="error-demo-group",
        consumer_timeout_ms=3000,  # Short timeout for quick failure
    )

    try:
        with KafkaConsumer(config=config) as consumer:
            consumer.subscribe(["test-topic"])
            consumer.poll(timeout_ms=2000)  # This should fail with invalid broker
    except Exception as e:
        print(f"❌ Expected error (invalid broker): {type(e).__name__}: {e}")


def health_check_example():
    """Consumer health check example."""
    print("\n=== Consumer Health Check ===")

    config = KafkaConsumerConfig(
        bootstrap_servers="localhost:9092", group_id="health-check-group"
    )

    consumer = KafkaConsumer(config=config)

    try:
        # Perform health check
        health = consumer.health_check()
        print(f"🏥 Health Status: {health['status']}")
        print(f"🔌 Connection: {health['connection']}")
        print(f"📡 Bootstrap Servers: {health['bootstrap_servers']}")
        print(f"⚙️  Configuration: {health.get('config', {})}")

        if health["status"] == "unhealthy":
            print(f"❌ Error: {health.get('error', 'Unknown error')}")
        else:
            print("✅ Consumer is healthy!")

    except Exception as e:
        print(f"❌ Health check error: {e}")
    finally:
        consumer.close()


if __name__ == "__main__":
    print("🚀 Generic Kafka Consumer - Basic Usage Examples")
    print("=" * 50)

    # Run examples
    basic_consumer_example()
    continuous_consumer_example()
    manual_partition_assignment_example()
    offset_management_example()
    consumer_with_monitoring_example()
    error_handling_example()
    health_check_example()

    print("\n✨ All consumer examples completed!")
    print("\n💡 Tips:")
    print("- Set KAFKA_BOOTSTRAP_SERVERS environment variable")
    print("- Use unique group_id for each consumer application")
    print("- Configure auto_offset_reset based on your needs")
    print("- Use context managers for automatic cleanup")
    print("- Monitor consumer lag and processing rates")
