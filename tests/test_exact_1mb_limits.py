#!/usr/bin/env python3
"""
Test for exact Kafka message size limits based on findings.
"""

import time

import pytest

from apnamart_kafka import Config, Consumer, Producer, ProducerError


@pytest.mark.integration
class TestKafkaMessageSizeLimits:
    """Test Kafka message size limits precisely."""

    def test_max_safe_message_size(self, test_topic: str, kafka_servers: str):
        """Test maximum safe message size that definitely works."""
        config = Config(bootstrap_servers=kafka_servers)

        # Use a conservative safe size that works with default Kafka config
        # Default Kafka max.message.bytes is ~1MB, but we need significant overhead for JSON
        safe_size = 512 * 1024  # 512KB - conservative safe size

        message_data = {
            "test": "max_safe_size",
            "size_category": "512KB",
            "payload": "x" * (safe_size - 200),  # Reserve space for JSON overhead
        }

        with Producer(config) as producer:
            start_time = time.time()
            producer.send(test_topic, message_data)
            producer.flush()
            duration = time.time() - start_time

        print(f" 512KB message sent successfully in {duration:.3f}s")

        # Verify we can consume it
        consumer_config = Config(
            bootstrap_servers=kafka_servers,
            group_id=f"max-size-test-{int(time.time())}",
            auto_offset_reset="earliest",
        )

        with Consumer(test_topic, consumer_config) as consumer:
            message = consumer.poll(timeout=10.0)
            assert message is not None
            assert message.value["test"] == "max_safe_size"
            consumer.commit(message)

    def test_exactly_1mb_fails(self, test_topic: str, kafka_servers: str):
        """Test that exactly 1MB (1,048,576 bytes) fails."""
        config = Config(bootstrap_servers=kafka_servers)

        # Try to send exactly 1MB
        exactly_1mb = 1048576  # 1024 * 1024

        message_data = {
            "test": "exactly_1mb",
            "payload": "x" * (exactly_1mb - 100),  # Account for JSON overhead
        }

        with Producer(config) as producer:
            with pytest.raises(ProducerError, match="Message size too large"):
                producer.send(test_topic, message_data)
                producer.flush()

        print(" Exactly 1MB correctly rejected by Kafka")

    def test_near_limit_boundary(self, test_topic: str, kafka_servers: str):
        """Test messages of various sizes within safe limits."""
        config = Config(bootstrap_servers=kafka_servers)

        # Test sizes within our conservative safe range
        test_sizes = [
            ("100KB", 100 * 1024),  # Should work
            ("300KB", 300 * 1024),  # Should work
            ("512KB", 512 * 1024),  # Should work (our safe max)
        ]

        results = {}

        with Producer(config) as producer:
            for size_name, size_bytes in test_sizes:
                try:
                    message_data = {
                        "boundary_test": True,
                        "size": size_name,
                        "payload": "x" * (size_bytes - 150),  # JSON overhead
                    }

                    producer.send(test_topic, message_data)
                    producer.flush()

                    results[size_name] = "SUCCESS"
                    print(f" {size_name}: SUCCESS")

                except ProducerError as e:
                    if "Message size too large" in str(e):
                        results[size_name] = "FAILED_SIZE_LIMIT"
                        print(f" {size_name}: FAILED (size limit)")
                    else:
                        results[size_name] = f"FAILED_OTHER: {e}"
                        print(f" {size_name}: FAILED ({e})")

        # All sizes within our safe range should work
        assert results["100KB"] == "SUCCESS", "100KB should work"
        assert results["300KB"] == "SUCCESS", "300KB should work"
        assert results["512KB"] == "SUCCESS", "512KB should work"

    def test_message_size_error_handling(self, test_topic: str, kafka_servers: str):
        """Test proper error handling for oversized messages."""
        config = Config(bootstrap_servers=kafka_servers)

        # Create a definitely oversized message (2MB)
        oversized_data = {
            "test": "oversized",
            "payload": "x" * (2 * 1024 * 1024),  # 2MB
        }

        with Producer(config) as producer:
            with pytest.raises(ProducerError) as exc_info:
                producer.send(test_topic, oversized_data)
                producer.flush()

            error_message = str(exc_info.value)
            assert "Message size too large" in error_message
            assert test_topic in error_message  # Should include topic name

        print(" Oversized message error handling works correctly")

    def test_payload_size_calculation(self, test_topic: str, kafka_servers: str):
        """Test that we can accurately predict message sizes."""
        config = Config(bootstrap_servers=kafka_servers)

        # Test different payload sizes and their JSON overhead
        test_payloads = [
            ("small", "x" * 1000),  # 1KB payload
            ("medium", "x" * 10000),  # 10KB payload
            ("large", "x" * 100000),  # 100KB payload
        ]

        with Producer(config) as producer:
            for size_name, payload in test_payloads:
                message_data = {
                    "size_test": size_name,
                    "payload_length": len(payload),
                    "payload": payload,
                }

                # Calculate approximate JSON size
                import json

                json_size = len(json.dumps(message_data).encode("utf-8"))

                start_time = time.time()
                producer.send(test_topic, message_data)
                producer.flush()
                duration = time.time() - start_time

                print(f" {size_name}: {json_size:,} bytes JSON sent in {duration:.3f}s")

                # Ensure we're not near the limit accidentally
                assert json_size < 500000, f"{size_name} payload too close to limit"


if __name__ == "__main__":
    print(" Testing Kafka Message Size Limits")
    print("=" * 40)
    print("Based on testing, Kafka limits:")
    print("- Conservative safe maximum: 512KB")
    print("- Tested safe sizes: 100KB, 300KB, 512KB (all should work)")
    print("- 1MB (1,048,576 bytes): FAILS")
    print("- Default Kafka limit varies by configuration")
    print()
    print("Run with: uv run pytest tests/test_exact_1mb_limits.py -v -s")
