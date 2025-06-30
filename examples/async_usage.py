"""Async usage examples for the Generic Kafka Producer."""

import asyncio
import time

from apnamart_kafka import KafkaConfig, KafkaProducer


async def basic_async_example():
    """Basic asynchronous usage example."""
    print("=== Basic Async Usage ===")

    async with KafkaProducer() as producer:
        try:
            # Send messages asynchronously
            await producer.send_async(
                "async-events",
                {
                    "event_id": "evt-001",
                    "event_type": "user_action",
                    "timestamp": time.time(),
                },
            )
            print("✅ Async message sent successfully")

            # Send with key
            await producer.send_async(
                topic="async-events",
                value={"event_id": "evt-002", "event_type": "system_event"},
                key="system",
            )
            print("✅ Async message with key sent successfully")

        except Exception as e:
            print(f"❌ Error: {e}")


async def concurrent_sends_example():
    """Concurrent message sending example."""
    print("\n=== Concurrent Sends ===")

    async with KafkaProducer() as producer:
        try:
            # Create multiple concurrent send tasks
            tasks = []
            for i in range(10):
                task = producer.send_async(
                    "concurrent-topic",
                    {
                        "message_id": i,
                        "content": f"Concurrent message {i}",
                        "timestamp": time.time(),
                    },
                )
                tasks.append(task)

            # Wait for all sends to complete
            await asyncio.gather(*tasks)
            print("✅ All 10 concurrent messages sent successfully")

        except Exception as e:
            print(f"❌ Error: {e}")


async def batch_processing_example():
    """Batch message processing example."""
    print("\n=== Batch Processing ===")

    # Sample data to process
    user_events = [
        {"user_id": 1, "action": "login", "timestamp": time.time()},
        {"user_id": 2, "action": "purchase", "amount": 25.99, "timestamp": time.time()},
        {"user_id": 3, "action": "logout", "timestamp": time.time()},
        {
            "user_id": 1,
            "action": "view_product",
            "product_id": "prod-123",
            "timestamp": time.time(),
        },
        {"user_id": 4, "action": "signup", "timestamp": time.time()},
    ]

    async with KafkaProducer() as producer:
        try:
            # Process events in batches
            batch_size = 3
            for i in range(0, len(user_events), batch_size):
                batch = user_events[i : i + batch_size]

                # Send batch concurrently
                tasks = [
                    producer.send_async(
                        topic="user-events", value=event, key=f"user-{event['user_id']}"
                    )
                    for event in batch
                ]

                await asyncio.gather(*tasks)
                print(f"✅ Batch {i // batch_size + 1} sent ({len(batch)} messages)")

            print("✅ All batches processed successfully")

        except Exception as e:
            print(f"❌ Error: {e}")


async def different_topics_example():
    """Sending to different topics example."""
    print("\n=== Different Topics ===")

    config = KafkaConfig(topic_prefix="myapp")

    async with KafkaProducer(config=config) as producer:
        try:
            # Send to different topics concurrently
            tasks = [
                producer.send_async(
                    "user-events",
                    {"type": "user_event", "data": {"user_id": 123, "action": "login"}},
                ),
                producer.send_async(
                    "system-metrics",
                    {
                        "type": "metric",
                        "data": {"cpu_usage": 45.6, "memory_usage": 78.2},
                    },
                ),
                producer.send_async(
                    "application-logs",
                    {
                        "type": "log",
                        "level": "INFO",
                        "message": "Application started successfully",
                    },
                ),
            ]

            await asyncio.gather(*tasks)
            print("✅ Messages sent to different topics successfully")

        except Exception as e:
            print(f"❌ Error: {e}")


async def error_handling_async_example():
    """Async error handling example."""
    print("\n=== Async Error Handling ===")

    # Invalid configuration for demonstration
    config = KafkaConfig(
        bootstrap_servers="invalid-broker:9092", request_timeout_ms=3000
    )

    try:
        async with KafkaProducer(config=config) as producer:
            await producer.send_async("test-topic", {"test": "data"})
    except Exception as e:
        print(f"❌ Expected error (invalid broker): {type(e).__name__}: {e}")


async def mixed_sync_async_example():
    """Mixed sync and async usage example."""
    print("\n=== Mixed Sync/Async Usage ===")

    producer = KafkaProducer()

    try:
        # Synchronous send
        producer.send("sync-topic", {"type": "sync", "message": "Synchronous message"})
        print("✅ Sync message sent")

        # Asynchronous send
        await producer.send_async(
            "async-topic", {"type": "async", "message": "Asynchronous message"}
        )
        print("✅ Async message sent")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        producer.close()


async def performance_test_example():
    """Performance test example."""
    print("\n=== Performance Test ===")

    message_count = 100
    start_time = time.time()

    async with KafkaProducer() as producer:
        try:
            # Send messages concurrently for better performance
            tasks = [
                producer.send_async(
                    "perf-test",
                    {
                        "message_id": i,
                        "timestamp": time.time(),
                        "data": f"Performance test message {i}",
                    },
                )
                for i in range(message_count)
            ]

            await asyncio.gather(*tasks)

            end_time = time.time()
            duration = end_time - start_time
            rate = message_count / duration

            print(f"✅ Sent {message_count} messages in {duration:.2f}s")
            print(f"📊 Rate: {rate:.2f} messages/second")

        except Exception as e:
            print(f"❌ Error: {e}")


async def main():
    """Run all async examples."""
    print("🚀 Generic Kafka Producer - Async Usage Examples")
    print("=" * 50)

    # Run examples
    await basic_async_example()
    await concurrent_sends_example()
    await batch_processing_example()
    await different_topics_example()
    await error_handling_async_example()
    await mixed_sync_async_example()
    await performance_test_example()

    print("\n✨ All async examples completed!")
    print("\n💡 Tips:")
    print("- Use async context manager for automatic cleanup")
    print("- Leverage asyncio.gather() for concurrent sends")
    print("- Process data in batches for better throughput")
    print("- Mix sync and async as needed in your application")


if __name__ == "__main__":
    asyncio.run(main())
