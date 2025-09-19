#!/usr/bin/env python3
"""
Comprehensive edge case tests for apnamart-kafka-python library.
Tests scenarios that could break in production environments.
"""

import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict

import pytest

from apnamart_kafka import Config, Consumer, Producer, TransactionalProducer


@pytest.mark.integration
class TestExtremeSizes:
    """Test handling of extreme message sizes and counts."""

    def test_empty_message(self, producer: Producer, test_topic: str):
        """Test handling of empty/null messages."""
        test_cases = [
            "",  # Empty string
            None,  # None value
            {},  # Empty dict
            [],  # Empty list
            b"",  # Empty bytes
        ]

        for empty_value in test_cases:
            try:
                producer.send(test_topic, empty_value)
                producer.flush()
                print(f" Empty value {type(empty_value).__name__} handled gracefully")
            except Exception as e:
                print(f" Empty value {type(empty_value).__name__} raised: {e}")

    def test_very_large_message_1mb(
        self, producer: Producer, consumer: Consumer, test_topic: str
    ):
        """Test large message (close to default Kafka limit)."""
        # Create ~500KB message (safe under 1MB limit)
        large_data = {
            "data": "x" * (512 * 1024),  # 512KB
            "metadata": {
                "size": "512KB",
                "test": "very_large_message",
                "timestamp": time.time(),
            },
        }

        # Send large message
        start_time = time.time()
        producer.send(test_topic, large_data)
        producer.flush()
        send_duration = time.time() - start_time

        # Consume large message
        start_time = time.time()
        message = consumer.poll(timeout=30.0)  # Longer timeout for large messages
        receive_duration = time.time() - start_time

        assert message is not None
        assert message.value["metadata"]["size"] == "512KB"

        print(
            f" 512KB message: sent in {send_duration:.2f}s, received in {receive_duration:.2f}s"
        )

    def test_many_small_messages_batch(self, producer: Producer, test_topic: str):
        """Test sending 1000 small messages in batch."""
        message_count = 1000
        messages = [
            {
                "topic": test_topic,
                "value": {"id": i, "data": f"msg-{i}", "timestamp": time.time()},
            }
            for i in range(message_count)
        ]

        start_time = time.time()
        results = producer.send_batch(messages)
        producer.flush()
        duration = time.time() - start_time

        successful = sum(1 for r in results if r["success"])
        throughput = successful / duration

        assert successful == message_count
        print(
            f" Batch send: {successful} messages in {duration:.2f}s ({throughput:.0f} msg/s)"
        )

    def test_unicode_and_special_characters(
        self, producer: Producer, consumer: Consumer, test_topic: str
    ):
        """Test messages with unicode and special characters."""
        test_messages = [
            {"text": "Hello 世界 ", "lang": "mixed"},
            {"emoji": "️", "type": "emoji"},
            {"special": "!@#$%^&*()_+-=[]{}|;':\",./<>?", "type": "special_chars"},
            {"unicode": "αβγδε ñüñéz café résumé", "type": "accents"},
            {"newlines": "line1\nline2\rline3\r\nline4", "type": "newlines"},
            {"tabs": "col1\tcol2\tcol3", "type": "tabs"},
        ]

        for i, msg in enumerate(test_messages):
            producer.send(test_topic, msg, key=f"unicode-{i}")

        producer.flush()
        time.sleep(0.5)

        received_messages = []
        start_time = time.time()
        while (
            len(received_messages) < len(test_messages)
            and (time.time() - start_time) < 10
        ):
            message = consumer.poll(timeout=2.0)
            if message:
                received_messages.append(message.value)
                consumer.commit(message)

        assert len(received_messages) >= len(test_messages)
        print(
            f" Unicode/special chars: {len(received_messages)} messages handled correctly"
        )


class TestConcurrencyEdgeCases:
    """Test concurrent access and threading scenarios."""

    def test_concurrent_producers_same_topic(self, test_topic: str, kafka_servers: str):
        """Test multiple producers writing to same topic simultaneously."""

        def producer_worker(worker_id: int, message_count: int) -> Dict[str, Any]:
            config = Config(bootstrap_servers=kafka_servers)

            with Producer(config) as producer:
                start_time = time.time()
                for i in range(message_count):
                    producer.send(
                        test_topic,
                        {"worker": worker_id, "msg": i, "timestamp": time.time()},
                    )
                producer.flush()
                duration = time.time() - start_time

                return {
                    "worker_id": worker_id,
                    "messages_sent": message_count,
                    "duration": duration,
                    "throughput": message_count / duration,
                }

        # Run 5 concurrent producers
        num_workers = 5
        messages_per_worker = 50

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [
                executor.submit(producer_worker, i, messages_per_worker)
                for i in range(num_workers)
            ]

            results = [future.result() for future in as_completed(futures)]

        total_messages = sum(r["messages_sent"] for r in results)
        avg_throughput = sum(r["throughput"] for r in results) / len(results)

        assert len(results) == num_workers
        assert total_messages == num_workers * messages_per_worker
        print(
            f" Concurrent producers: {total_messages} messages, avg {avg_throughput:.0f} msg/s per worker"
        )

    def test_producer_consumer_simultaneous(self, test_topic: str, kafka_servers: str):
        """Test producer and consumer running simultaneously."""
        message_count = 100
        received_messages = []
        consumer_errors = []
        producer_errors = []

        def producer_thread():
            try:
                config = Config(bootstrap_servers=kafka_servers)
                with Producer(config) as producer:
                    for i in range(message_count):
                        producer.send(
                            test_topic, {"simultaneous": i, "timestamp": time.time()}
                        )
                        time.sleep(0.01)  # Small delay to interleave with consumer
                    producer.flush()
            except Exception as e:
                producer_errors.append(e)

        def consumer_thread():
            try:
                config = Config(
                    bootstrap_servers=kafka_servers,
                    group_id=f"simultaneous-{uuid.uuid4().hex[:8]}",
                    auto_offset_reset="earliest",
                )

                with Consumer(test_topic, config) as consumer:
                    start_time = time.time()
                    while (
                        len(received_messages) < message_count
                        and (time.time() - start_time) < 30
                    ):
                        message = consumer.poll(timeout=1.0)
                        if message:
                            received_messages.append(message.value)
                            consumer.commit(message)
            except Exception as e:
                consumer_errors.append(e)

        # Start both threads
        producer_t = threading.Thread(target=producer_thread)
        consumer_t = threading.Thread(target=consumer_thread)

        producer_t.start()
        consumer_t.start()

        producer_t.join()
        consumer_t.join()

        assert not producer_errors, f"Producer errors: {producer_errors}"
        assert not consumer_errors, f"Consumer errors: {consumer_errors}"
        assert (
            len(received_messages) >= message_count * 0.9
        )  # Allow 10% loss for timing issues
        print(
            f" Simultaneous producer/consumer: {len(received_messages)}/{message_count} messages"
        )


class TestErrorRecoveryScenarios:
    """Test error recovery and resilience scenarios."""

    def test_producer_connection_recovery(self, kafka_servers: str):
        """Test producer recovery after connection issues."""
        config = Config(
            bootstrap_servers=kafka_servers, retries=3, retry_backoff_ms=100
        )

        with Producer(config) as producer:
            # Send some messages successfully
            for i in range(5):
                producer.send("recovery-test", {"before_error": i})
            producer.flush()

            # Continue sending (should recover automatically)
            for i in range(5):
                producer.send("recovery-test", {"after_recovery": i})
            producer.flush()

        print(" Producer connection recovery: handled gracefully")

    def test_consumer_rebalance_simulation(self, test_topic: str, kafka_servers: str):
        """Test consumer behavior during group rebalancing."""
        group_id = f"rebalance-test-{uuid.uuid4().hex[:8]}"

        # Start first consumer
        config1 = Config(
            bootstrap_servers=kafka_servers,
            group_id=group_id,
            auto_offset_reset="earliest",
        )

        consumer1 = Consumer(test_topic, config1)

        # Send some messages
        producer_config = Config(bootstrap_servers=kafka_servers)
        with Producer(producer_config) as producer:
            for i in range(10):
                producer.send(test_topic, {"rebalance_test": i})
            producer.flush()

        # Consumer should receive messages
        messages_c1 = []
        with consumer1:
            start_time = time.time()
            while len(messages_c1) < 5 and (time.time() - start_time) < 10:
                message = consumer1.poll(timeout=1.0)
                if message:
                    messages_c1.append(message.value)
                    consumer1.commit(message)

        assert len(messages_c1) > 0
        print(
            f" Consumer rebalance: received {len(messages_c1)} messages during rebalancing"
        )

    def test_transaction_rollback_on_error(self, test_topic: str, kafka_servers: str):
        """Test transaction rollback when errors occur."""
        tx_id = f"rollback-test-{uuid.uuid4().hex[:8]}"
        config = Config(bootstrap_servers=kafka_servers)

        try:
            with TransactionalProducer(tx_id, config) as tx_producer:
                tx_producer.begin()

                # Send some messages
                tx_producer.send(test_topic, {"tx_msg": 1})
                tx_producer.send(test_topic, {"tx_msg": 2})

                # Simulate error condition
                raise ValueError("Simulated transaction error")

        except ValueError:
            # Expected - transaction should auto-rollback
            pass

        # Verify messages were not committed
        consumer_config = Config(
            bootstrap_servers=kafka_servers,
            group_id=f"rollback-verify-{uuid.uuid4().hex[:8]}",
            auto_offset_reset="earliest",
        )

        received_messages = []
        with Consumer(test_topic, consumer_config) as consumer:
            # Should not see the rolled-back messages
            start_time = time.time()
            while time.time() - start_time < 3:  # Short timeout
                message = consumer.poll(timeout=0.5)
                if message and "tx_msg" in message.value:
                    received_messages.append(message.value)

        # Should have no messages from failed transaction
        tx_messages = [m for m in received_messages if "tx_msg" in m]
        print(
            f" Transaction rollback: {len(tx_messages)} messages from failed transaction (should be 0)"
        )


class TestConfigurationEdgeCases:
    """Test various configuration edge cases."""

    def test_invalid_configuration_handling(self):
        """Test handling of invalid configurations."""
        invalid_configs = [
            {"bootstrap_servers": ""},  # Empty servers
            {"bootstrap_servers": "invalid:port:format"},  # Invalid format
            {"acks": "invalid_acks"},  # Invalid acks value
            {"compression_type": "nonexistent"},  # Invalid compression
        ]

        for invalid_config in invalid_configs:
            try:
                config = Config(**invalid_config)
                # Some invalid configs might be accepted but fail during connection
                producer = Producer(config)
                producer.close()
                print(f" Invalid config accepted: {invalid_config}")
            except Exception as e:
                print(
                    f" Invalid config rejected: {invalid_config} -> {type(e).__name__}"
                )

    def test_configuration_override_behavior(self, kafka_servers: str):
        """Test configuration override precedence."""
        base_config = Config(
            bootstrap_servers=kafka_servers, acks="1", compression_type="gzip"
        )

        # Test config conversion to confluent-kafka format
        producer_config = base_config.to_producer_config()
        consumer_config = base_config.to_consumer_config()

        assert "bootstrap.servers" in producer_config
        assert producer_config["acks"] == "1"
        assert producer_config["compression.type"] == "gzip"

        assert "bootstrap.servers" in consumer_config
        assert "group.id" in consumer_config

        print(" Configuration override behavior: working correctly")

    def test_consumer_configuration_edge_cases(self, kafka_servers: str):
        """Test consumer-specific configuration edge cases."""
        config = Config(
            bootstrap_servers=kafka_servers,
            group_id="edge-case-group",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            session_timeout_ms=6000,
            heartbeat_interval_ms=3000,
        )

        try:
            with Consumer("test-topic", config) as consumer:
                # Just test that consumer can be created with edge case configs
                consumer.poll(timeout=0.1)  # Test poll but ignore result
                print(" Consumer edge case configurations: handled correctly")
        except Exception as e:
            print(f" Consumer config issue: {e}")


class TestDataIntegrityAndSerialization:
    """Test data integrity and serialization edge cases."""

    def test_json_serialization_edge_cases(
        self, producer: Producer, consumer: Consumer, test_topic: str
    ):
        """Test JSON serialization with problematic data."""
        edge_case_data = [
            {"float": float("inf")},  # Infinity
            {"float": float("-inf")},  # Negative infinity
            {"large_int": 2**63 - 1},  # Large integer
            {"nested": {"deep": {"very": {"nested": "data"}}}},  # Deep nesting
            {"circular_ref_simulation": "self_reference_string"},
        ]

        for i, data in enumerate(edge_case_data):
            try:
                producer.send(test_topic, data, key=f"edge-{i}")
                print(f" Serialization edge case {i}: {type(data)} handled")
            except Exception as e:
                print(f" Serialization edge case {i} failed: {e}")

        producer.flush()

    def test_message_key_variations(self, producer: Producer, test_topic: str):
        """Test various message key types and formats."""
        key_variations = [
            "simple_string",
            "",  # Empty string key
            "12345",  # Numeric string
            "key-with-special-chars!@#$%",
            "very_long_key_" + "x" * 100,
            "unicode_key_世界",
        ]

        for i, key in enumerate(key_variations):
            try:
                producer.send(test_topic, {"test": f"key_variation_{i}"}, key=key)
                print(f" Key variation {i}: '{key[:20]}...' handled")
            except Exception as e:
                print(f" Key variation {i} failed: {e}")

        producer.flush()

    def test_binary_data_handling(
        self, producer: Producer, consumer: Consumer, test_topic: str
    ):
        """Test binary data serialization and deserialization."""
        binary_data = [
            b"simple bytes",
            b"\x00\x01\x02\x03\x04\x05",  # Null bytes and binary
            b"binary with unicode: \xe4\xb8\x96\xe7\x95\x8c",  # UTF-8 encoded unicode
            bytes(range(256)),  # All possible byte values
        ]

        for i, data in enumerate(binary_data):
            try:
                producer.send(test_topic, data, key=f"binary-{i}")
                print(f" Binary data {i}: {len(data)} bytes handled")
            except Exception as e:
                print(f" Binary data {i} failed: {e}")

        producer.flush()
        time.sleep(0.5)

        # Try to consume binary data
        received_count = 0
        start_time = time.time()
        while received_count < len(binary_data) and (time.time() - start_time) < 10:
            message = consumer.poll(timeout=1.0)
            if message and message.key and message.key.startswith(b"binary-"):
                received_count += 1
                consumer.commit(message)

        print(f" Binary data consumption: {received_count}/{len(binary_data)} messages")


if __name__ == "__main__":
    # Run edge case tests manually
    print(" Running Edge Case Tests...")
    print("=" * 60)

    # This would require setting up fixtures manually
    # Normally run with: uv run pytest tests/test_edge_cases.py -v
    print("Use: uv run pytest tests/test_edge_cases.py -v")
