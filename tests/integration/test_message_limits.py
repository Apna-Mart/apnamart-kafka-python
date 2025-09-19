"""Integration tests for Kafka message size limits."""

import time

import pytest

from apnamart_kafka import Config, Consumer, Producer, ProducerError


class TestMessageSizeLimits:
    """Test Kafka message size limits with real broker."""

    def test_large_message_512kb(self, test_topic: str, kafka_servers: str):
        """Test 512KB message (safe under Kafka default limits)."""
        config = Config(bootstrap_servers=kafka_servers)

        # Create 512KB message
        large_data = {
            "test": "large_message",
            "size": "512KB",
            "payload": "x" * (512 * 1024 - 200),  # Reserve space for JSON overhead
            "timestamp": time.time(),
        }

        # Send large message
        with Producer(config) as producer:
            start_time = time.time()
            producer.send(test_topic, large_data)
            producer.flush()
            send_duration = time.time() - start_time

        # Verify we can consume it
        consumer_config = Config(
            bootstrap_servers=kafka_servers,
            group_id=f"large-msg-test-{int(time.time())}",
            auto_offset_reset="earliest",
        )

        with Consumer(test_topic, consumer_config) as consumer:
            start_time = time.time()
            message = consumer.poll(timeout=10.0)
            receive_duration = time.time() - start_time

            assert message is not None
            assert message.value["test"] == "large_message"
            assert message.value["size"] == "512KB"
            consumer.commit(message)

        print(
            f" 512KB message: sent in {send_duration:.3f}s, received in {receive_duration:.3f}s"
        )

    def test_very_large_message_fails(self, test_topic: str, kafka_servers: str):
        """Test that messages over Kafka limit fail gracefully."""
        config = Config(bootstrap_servers=kafka_servers)

        # Create message that should exceed Kafka's limit
        oversized_data = {
            "test": "oversized_message",
            "payload": "x" * (2 * 1024 * 1024),  # 2MB - definitely too large
        }

        with Producer(config) as producer:
            with pytest.raises(ProducerError, match="Message size too large"):
                producer.send(test_topic, oversized_data)
                producer.flush()

        print(" Oversized message correctly rejected by Kafka")

    def test_multiple_message_sizes(self, test_topic: str, kafka_servers: str):
        """Test various message sizes to validate boundaries."""
        config = Config(bootstrap_servers=kafka_servers)

        # Test different message sizes
        test_sizes = [
            ("small", 1024),  # 1KB
            ("medium", 10240),  # 10KB
            ("large", 102400),  # 100KB
            ("xlarge", 512000),  # 500KB
        ]

        results = {}

        with Producer(config) as producer:
            for size_name, payload_size in test_sizes:
                try:
                    message_data = {
                        "size_test": size_name,
                        "payload_size": payload_size,
                        "payload": "x" * payload_size,
                        "timestamp": time.time(),
                    }

                    start_time = time.time()
                    producer.send(test_topic, message_data)
                    producer.flush()
                    duration = time.time() - start_time

                    results[size_name] = {
                        "success": True,
                        "size": payload_size,
                        "duration": duration,
                    }

                    print(
                        f" {size_name} ({payload_size:,} bytes): SUCCESS in {duration:.3f}s"
                    )

                except ProducerError as e:
                    results[size_name] = {
                        "success": False,
                        "size": payload_size,
                        "error": str(e),
                    }
                    print(f" {size_name} ({payload_size:,} bytes): FAILED - {e}")

        # Verify expected behavior
        assert results["small"]["success"], "Small messages should always work"
        assert results["medium"]["success"], "Medium messages should work"
        assert results["large"]["success"], "Large messages should work"
        assert results["xlarge"]["success"], "XLarge messages should work under 1MB"

    def test_message_size_with_different_data_types(
        self, test_topic: str, kafka_servers: str
    ):
        """Test message size limits with different data types."""
        config = Config(bootstrap_servers=kafka_servers)

        # Test with different serialization overhead
        test_cases = [
            {
                "name": "string_heavy",
                "data": {"text": "A" * 100000, "type": "string"},  # 100KB of strings
            },
            {
                "name": "number_heavy",
                "data": {
                    "numbers": list(range(10000)),
                    "type": "numbers",
                },  # Array of numbers
            },
            {
                "name": "nested_objects",
                "data": {
                    "level1": {
                        "level2": {
                            "level3": ["data"] * 5000  # Nested structure
                        }
                    },
                    "type": "nested",
                },
            },
            {
                "name": "unicode_text",
                "data": {
                    "unicode": "世界" * 20000,
                    "type": "unicode",
                },  # Unicode characters
            },
        ]

        with Producer(config) as producer:
            for test_case in test_cases:
                try:
                    producer.send(test_topic, test_case["data"])
                    producer.flush()
                    print(f" {test_case['name']}: SUCCESS")
                except ProducerError as e:
                    print(f" {test_case['name']}: FAILED - {e}")

    def test_batch_message_size_limits(self, test_topic: str, kafka_servers: str):
        """Test size limits when sending in batches."""
        config = Config(bootstrap_servers=kafka_servers)

        # Create batch of moderately sized messages
        batch_size = 50
        message_size = 10000  # 10KB per message

        messages = []
        for i in range(batch_size):
            messages.append(
                {
                    "topic": test_topic,
                    "value": {
                        "batch_test": True,
                        "message_id": i,
                        "payload": "x" * message_size,
                        "timestamp": time.time(),
                    },
                }
            )

        with Producer(config) as producer:
            start_time = time.time()
            results = producer.send_batch(messages)
            producer.flush()
            duration = time.time() - start_time

            successful = sum(1 for r in results if r["success"])
            total_data = successful * message_size

            print(f" Batch size test: {successful}/{batch_size} messages")
            print(f"  Total data: {total_data:,} bytes in {duration:.3f}s")

            assert successful == batch_size, "All batch messages should succeed"

    def test_message_size_error_messages(self, test_topic: str, kafka_servers: str):
        """Test that size limit errors provide helpful messages."""
        config = Config(bootstrap_servers=kafka_servers)

        # Create definitely oversized message
        huge_data = {
            "test": "error_message_test",
            "payload": "x" * (5 * 1024 * 1024),  # 5MB
        }

        with Producer(config) as producer:
            with pytest.raises(ProducerError) as exc_info:
                producer.send(test_topic, huge_data)
                producer.flush()

            error_message = str(exc_info.value)

            # Verify error message is helpful
            assert "Message size too large" in error_message
            assert test_topic in error_message  # Should mention the topic

            print(f" Error message: {error_message}")


if __name__ == "__main__":
    print(" Testing Kafka Message Size Limits")
    print("=" * 40)
    print("These tests validate message size boundaries with real Kafka.")
    print("Run with: uv run pytest tests/integration/test_message_limits.py -v")
