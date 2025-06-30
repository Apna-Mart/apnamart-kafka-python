"""Async usage examples for the Generic Kafka Consumer."""

import asyncio
import time

from apnamart_kafka import KafkaConsumer, KafkaConsumerConfig


async def basic_async_consumer_example():
    """Basic asynchronous consumer usage example."""
    print("=== Basic Async Consumer Usage ===")

    config = KafkaConsumerConfig(
        bootstrap_servers="localhost:9092",
        group_id="async-consumer-group",
        auto_offset_reset="latest",
    )

    async with KafkaConsumer(config=config) as consumer:
        try:
            # Subscribe to topics
            consumer.subscribe(["user-events", "system-metrics"])
            print("✅ Subscribed to topics: user-events, system-metrics")

            # Consume messages asynchronously
            message_count = 0
            async for message in consumer.consume_async(timeout_ms=1000):
                message_count += 1
                print(
                    f"📨 Async message {message_count}: {message.topic}:{message.offset}"
                )
                print(f"   Value: {message.value}")

                # Async commit offsets
                await consumer.commit_offsets_async()

                # Stop after 5 messages or break on no activity
                if message_count >= 5:
                    break

            print(f"✅ Processed {message_count} messages asynchronously")

        except Exception as e:
            print(f"❌ Error: {e}")


async def concurrent_processing_example():
    """Concurrent message processing example."""
    print("\n=== Concurrent Message Processing ===")

    config = KafkaConsumerConfig(
        bootstrap_servers="localhost:9092",
        group_id="concurrent-processing-group",
        auto_offset_reset="earliest",
        max_poll_records=10,  # Get more messages per poll
    )

    async def process_message(message):
        """Simulate async message processing."""
        await asyncio.sleep(0.1)  # Simulate processing time
        return f"Processed {message.topic}:{message.offset} = {message.value}"

    async with KafkaConsumer(config=config) as consumer:
        try:
            consumer.subscribe(["user-events"])
            print("✅ Subscribed to user-events for concurrent processing")

            # Poll for a batch of messages
            messages = consumer.poll(timeout_ms=3000, max_records=5)

            if messages:
                print(f"📦 Received {len(messages)} messages for concurrent processing")

                # Process messages concurrently
                tasks = [process_message(message) for message in messages]
                results = await asyncio.gather(*tasks)

                # Print results
                for result in results:
                    print(f"  ✅ {result}")

                # Commit offsets after all processing is done
                await consumer.commit_offsets_async()
                print("✅ All messages processed and committed")
            else:
                print("  No messages to process")

        except Exception as e:
            print(f"❌ Error: {e}")


async def multi_consumer_example():
    """Multiple consumers working together example."""
    print("\n=== Multiple Consumers ===")

    async def create_consumer(group_id: str, consumer_id: str):
        """Create a consumer with specific configuration."""
        config = KafkaConsumerConfig(
            bootstrap_servers="localhost:9092",
            group_id=group_id,
            auto_offset_reset="earliest",
            session_timeout_ms=30000,
            heartbeat_interval_ms=3000,
        )
        return KafkaConsumer(config=config), consumer_id

    async def run_consumer(consumer, consumer_id: str, max_messages: int = 3):
        """Run a consumer and process messages."""
        async with consumer:
            try:
                consumer.subscribe(["user-events"])
                print(f"✅ {consumer_id} subscribed")

                message_count = 0
                async for message in consumer.consume_async(timeout_ms=1000):
                    message_count += 1
                    print(
                        f"📨 {consumer_id} received: {message.topic}:{message.offset}"
                    )

                    # Simulate processing
                    await asyncio.sleep(0.2)

                    if message_count >= max_messages:
                        break

                print(f"✅ {consumer_id} processed {message_count} messages")

            except Exception as e:
                print(f"❌ {consumer_id} error: {e}")

    try:
        # Create multiple consumers in the same group
        consumer1, id1 = await create_consumer("multi-consumer-group", "Consumer-1")
        consumer2, id2 = await create_consumer("multi-consumer-group", "Consumer-2")

        # Run consumers concurrently
        await asyncio.gather(run_consumer(consumer1, id1), run_consumer(consumer2, id2))

        print("✅ All consumers completed")

    except Exception as e:
        print(f"❌ Multi-consumer error: {e}")


async def backpressure_handling_example():
    """Backpressure and rate limiting example."""
    print("\n=== Backpressure Handling ===")

    config = KafkaConsumerConfig(
        bootstrap_servers="localhost:9092",
        group_id="backpressure-group",
        auto_offset_reset="earliest",
        max_poll_records=3,  # Small batches
        enable_auto_commit=False,  # Manual commit control
    )

    async def slow_processing(message, processing_time: float = 0.5):
        """Simulate slow message processing."""
        await asyncio.sleep(processing_time)
        return f"Slowly processed: {message.topic}:{message.offset}"

    async with KafkaConsumer(config=config) as consumer:
        try:
            consumer.subscribe(["user-events"])
            print("✅ Subscribed with backpressure handling")

            # Use a semaphore to limit concurrent processing
            semaphore = asyncio.Semaphore(2)  # Max 2 concurrent messages

            async def process_with_backpressure(message):
                async with semaphore:
                    return await slow_processing(message)

            # Process messages with controlled concurrency
            processed_count = 0
            for _ in range(3):  # Poll 3 times
                messages = consumer.poll(timeout_ms=2000)

                if messages:
                    print(
                        f"📦 Processing {len(messages)} messages with backpressure control"
                    )

                    # Process with controlled concurrency
                    tasks = [process_with_backpressure(msg) for msg in messages]
                    results = await asyncio.gather(*tasks)

                    for result in results:
                        processed_count += 1
                        print(f"  ✅ {result}")

                    # Commit after processing batch
                    await consumer.commit_offsets_async()
                    print(f"  📝 Committed batch of {len(messages)} messages")

                # Small delay between polls
                await asyncio.sleep(0.5)

            print(f"✅ Processed {processed_count} messages with backpressure control")

        except Exception as e:
            print(f"❌ Error: {e}")


async def error_recovery_example():
    """Async error handling and recovery example."""
    print("\n=== Async Error Recovery ===")

    config = KafkaConsumerConfig(
        bootstrap_servers="localhost:9092",
        group_id="error-recovery-group",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )

    async def risky_processing(message):
        """Simulate processing that might fail."""
        # Simulate random failures
        import random

        if random.random() < 0.3:  # 30% failure rate
            raise Exception(f"Processing failed for {message.topic}:{message.offset}")

        await asyncio.sleep(0.1)
        return f"Successfully processed: {message.topic}:{message.offset}"

    async with KafkaConsumer(config=config) as consumer:
        try:
            consumer.subscribe(["user-events"])
            print("✅ Subscribed with error recovery handling")

            retry_count = 0
            max_retries = 3

            while retry_count < max_retries:
                try:
                    messages = consumer.poll(timeout_ms=2000, max_records=3)

                    if not messages:
                        print("  No messages, retrying...")
                        retry_count += 1
                        continue

                    print(
                        f"📦 Processing {len(messages)} messages (attempt {retry_count + 1})"
                    )

                    # Process each message with error handling
                    successful_count = 0
                    for message in messages:
                        try:
                            result = await risky_processing(message)
                            print(f"  ✅ {result}")
                            successful_count += 1

                        except Exception as e:
                            print(f"  ❌ Failed: {e}")
                            # In production, you might send to DLQ or retry later

                    # Commit successful messages
                    if successful_count > 0:
                        await consumer.commit_offsets_async()
                        print(f"  📝 Committed {successful_count} successful messages")

                    break  # Success, exit retry loop

                except Exception as e:
                    retry_count += 1
                    print(f"  ⚠️  Attempt {retry_count} failed: {e}")
                    if retry_count < max_retries:
                        await asyncio.sleep(1)  # Wait before retry

            if retry_count >= max_retries:
                print("❌ Max retries exceeded")
            else:
                print("✅ Error recovery completed successfully")

        except Exception as e:
            print(f"❌ Fatal error: {e}")


async def performance_monitoring_example():
    """Performance monitoring and metrics example."""
    print("\n=== Performance Monitoring ===")

    from apnamart_kafka import MetricsCollector
    from apnamart_kafka.common.monitoring import MonitoringCollectorHandler

    # Setup monitoring
    metrics_collector = MetricsCollector()
    monitoring_handler = MonitoringCollectorHandler(metrics_collector)

    config = KafkaConsumerConfig(
        bootstrap_servers="localhost:9092",
        group_id="performance-monitoring-group",
        auto_offset_reset="earliest",
        max_poll_records=10,
    )

    async with KafkaConsumer(config=config) as consumer:
        try:
            # Add monitoring
            consumer.add_monitoring_handler(monitoring_handler)
            print("🔍 Performance monitoring enabled")

            consumer.subscribe(["user-events"])

            # Track performance metrics
            start_time = time.time()
            message_count = 0

            for _ in range(5):  # Poll 5 times
                poll_start = time.time()
                messages = consumer.poll(timeout_ms=1000)
                poll_time = time.time() - poll_start

                if messages:
                    process_start = time.time()

                    # Simulate concurrent processing
                    async def process_message(msg):
                        await asyncio.sleep(0.05)  # Simulate work
                        return msg

                    tasks = [process_message(msg) for msg in messages]
                    await asyncio.gather(*tasks)

                    process_time = time.time() - process_start
                    message_count += len(messages)

                    print(
                        f"📊 Poll: {poll_time:.3f}s, Process: {process_time:.3f}s, "
                        f"Messages: {len(messages)}"
                    )

                await asyncio.sleep(0.1)

            # Performance summary
            total_time = time.time() - start_time
            if message_count > 0:
                rate = message_count / total_time
                print("\n📈 Performance Summary:")
                print(f"  Total messages: {message_count}")
                print(f"  Total time: {total_time:.3f}s")
                print(f"  Processing rate: {rate:.2f} messages/second")

            # Get metrics
            metrics = metrics_collector.get_metrics()
            print(f"  Metrics: {metrics['global']}")

        except Exception as e:
            print(f"❌ Error: {e}")


async def main():
    """Run all async consumer examples."""
    print("🚀 Generic Kafka Consumer - Async Usage Examples")
    print("=" * 55)

    # Run examples
    await basic_async_consumer_example()
    await concurrent_processing_example()
    await multi_consumer_example()
    await backpressure_handling_example()
    await error_recovery_example()
    await performance_monitoring_example()

    print("\n✨ All async consumer examples completed!")
    print("\n💡 Async Tips:")
    print("- Use async context managers for proper cleanup")
    print("- Leverage asyncio.gather() for concurrent processing")
    print("- Implement backpressure control with semaphores")
    print("- Handle errors gracefully with retry logic")
    print("- Monitor performance and consumer lag")


if __name__ == "__main__":
    asyncio.run(main())
