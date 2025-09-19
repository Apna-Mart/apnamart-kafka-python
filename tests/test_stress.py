#!/usr/bin/env python3
"""
Stress tests for apnamart-kafka-python library.
Tests the library under extreme loads and conditions.
"""

import statistics
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict

import pytest

from apnamart_kafka import (
    Config,
    Producer,
    TransactionalProducer,
)


@pytest.mark.stress
class TestHighLoadStress:
    """Stress tests with high message volumes and concurrent operations."""

    def test_high_volume_producer_stress(self, test_topic: str, kafka_servers: str):
        """Stress test with very high message volume."""
        config = Config(bootstrap_servers=kafka_servers)
        message_count = 10000  # 10K messages
        batch_size = 100

        start_time = time.time()
        messages_sent = 0
        errors = []

        try:
            with Producer(config) as producer:
                for batch_start in range(0, message_count, batch_size):
                    batch_messages = []
                    for i in range(
                        batch_start, min(batch_start + batch_size, message_count)
                    ):
                        batch_messages.append(
                            {
                                "topic": test_topic,
                                "value": {
                                    "stress_test": True,
                                    "message_id": i,
                                    "timestamp": time.time(),
                                    "data": f"stress_message_{i}"
                                    + "x" * 100,  # Add some payload
                                },
                            }
                        )

                    try:
                        results = producer.send_batch(batch_messages)
                        successful = sum(1 for r in results if r["success"])
                        messages_sent += successful

                        if messages_sent % 1000 == 0:
                            print(f"  Sent {messages_sent}/{message_count} messages...")

                    except Exception as e:
                        errors.append(
                            f"Batch {batch_start}-{batch_start + batch_size}: {e}"
                        )

                producer.flush()

        except Exception as e:
            errors.append(f"Producer error: {e}")

        duration = time.time() - start_time
        throughput = messages_sent / duration if duration > 0 else 0

        print(" High volume stress test:")
        print(f"  - Messages sent: {messages_sent}/{message_count}")
        print(f"  - Duration: {duration:.2f}s")
        print(f"  - Throughput: {throughput:.0f} msg/s")
        print(f"  - Errors: {len(errors)}")

        if errors:
            for error in errors[:5]:  # Show first 5 errors
                print(f"    {error}")

        assert messages_sent >= message_count * 0.95, (
            "Should send at least 95% of messages"
        )
        assert len(errors) < message_count * 0.01, "Should have less than 1% errors"

    def test_extreme_concurrency_stress(self, test_topic: str, kafka_servers: str):
        """Stress test with extreme concurrency."""
        config = Config(bootstrap_servers=kafka_servers)
        num_threads = 10  # 10 concurrent producers
        messages_per_thread = 500
        total_expected = num_threads * messages_per_thread

        results = []
        errors = []

        def stress_producer_worker(worker_id: int) -> Dict[str, Any]:
            try:
                with Producer(config) as producer:
                    start_time = time.time()
                    sent_count = 0

                    for i in range(messages_per_thread):
                        try:
                            producer.send(
                                test_topic,
                                {
                                    "worker_id": worker_id,
                                    "message_id": i,
                                    "timestamp": time.time(),
                                    "data": f"worker_{worker_id}_msg_{i}",
                                },
                            )
                            sent_count += 1

                            # Flush every 50 messages to maintain flow
                            if sent_count % 50 == 0:
                                producer.flush()

                        except Exception as e:
                            errors.append(f"Worker {worker_id}, msg {i}: {e}")

                    producer.flush()
                    duration = time.time() - start_time

                    return {
                        "worker_id": worker_id,
                        "sent_count": sent_count,
                        "duration": duration,
                        "throughput": sent_count / duration if duration > 0 else 0,
                    }

            except Exception as e:
                errors.append(f"Worker {worker_id} fatal error: {e}")
                return {
                    "worker_id": worker_id,
                    "sent_count": 0,
                    "duration": 0,
                    "throughput": 0,
                }

        # Run all workers concurrently
        overall_start = time.time()
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [
                executor.submit(stress_producer_worker, i) for i in range(num_threads)
            ]

            for future in as_completed(futures):
                results.append(future.result())

        overall_duration = time.time() - overall_start

        total_sent = sum(r["sent_count"] for r in results)
        successful_workers = sum(1 for r in results if r["sent_count"] > 0)
        avg_throughput = statistics.mean(
            [r["throughput"] for r in results if r["throughput"] > 0]
        )
        overall_throughput = (
            total_sent / overall_duration if overall_duration > 0 else 0
        )

        print(" Extreme concurrency stress test:")
        print(f"  - Workers: {num_threads}")
        print(f"  - Expected messages: {total_expected}")
        print(f"  - Actual messages sent: {total_sent}")
        print(f"  - Successful workers: {successful_workers}/{num_threads}")
        print(f"  - Overall duration: {overall_duration:.2f}s")
        print(f"  - Overall throughput: {overall_throughput:.0f} msg/s")
        print(f"  - Average per worker: {avg_throughput:.0f} msg/s")
        print(f"  - Errors: {len(errors)}")

        if errors:
            for error in errors[:3]:  # Show first 3 errors
                print(f"    {error}")

        assert total_sent >= total_expected * 0.9, (
            "Should send at least 90% of messages"
        )
        assert successful_workers >= num_threads * 0.8, (
            "At least 80% of workers should succeed"
        )

    def test_long_running_stress(self, test_topic: str, kafka_servers: str):
        """Long-running stress test to detect memory leaks and stability issues."""
        config = Config(bootstrap_servers=kafka_servers)
        test_duration = 30  # 30 seconds
        message_interval = 0.01  # 10ms between messages

        messages_sent = 0
        errors = []
        memory_samples = []

        try:
            import os

            import psutil

            process = psutil.Process(os.getpid())
            memory_monitoring = True
        except ImportError:
            memory_monitoring = False
            print("  (psutil not available - skipping memory monitoring)")

        start_time = time.time()

        try:
            with Producer(config) as producer:
                while (time.time() - start_time) < test_duration:
                    try:
                        producer.send(
                            test_topic,
                            {
                                "long_running": True,
                                "message_id": messages_sent,
                                "timestamp": time.time(),
                                "elapsed": time.time() - start_time,
                            },
                        )

                        messages_sent += 1

                        # Sample memory usage every 100 messages
                        if memory_monitoring and messages_sent % 100 == 0:
                            memory_mb = process.memory_info().rss / 1024 / 1024
                            memory_samples.append(memory_mb)

                        # Flush every 100 messages
                        if messages_sent % 100 == 0:
                            producer.flush()

                        time.sleep(message_interval)

                    except Exception as e:
                        errors.append(f"Message {messages_sent}: {e}")

                producer.flush()

        except Exception as e:
            errors.append(f"Producer error: {e}")

        actual_duration = time.time() - start_time
        throughput = messages_sent / actual_duration if actual_duration > 0 else 0

        print(" Long-running stress test:")
        print(f"  - Duration: {actual_duration:.1f}s")
        print(f"  - Messages sent: {messages_sent}")
        print(f"  - Throughput: {throughput:.1f} msg/s")
        print(f"  - Errors: {len(errors)}")

        if memory_monitoring and memory_samples:
            initial_memory = memory_samples[0]
            final_memory = memory_samples[-1]
            max_memory = max(memory_samples)
            memory_growth = final_memory - initial_memory

            print(
                f"  - Memory: {initial_memory:.1f} -> {final_memory:.1f} MB (peak: {max_memory:.1f} MB)"
            )
            print(f"  - Memory growth: {memory_growth:.1f} MB")

            # Check for excessive memory growth (should be < 50MB for this test)
            assert memory_growth < 50, (
                f"Excessive memory growth: {memory_growth:.1f} MB"
            )

        assert len(errors) < messages_sent * 0.01, "Should have less than 1% errors"


@pytest.mark.stress
class TestTransactionalStress:
    """Stress tests for transactional operations."""

    def test_many_concurrent_transactions(self, test_topic: str, kafka_servers: str):
        """Stress test with many concurrent transactions."""
        config = Config(bootstrap_servers=kafka_servers)
        num_transactions = 20
        messages_per_tx = 10

        results = []
        errors = []

        def transaction_worker(tx_id: int) -> Dict[str, Any]:
            transaction_id = f"stress-tx-{tx_id}-{uuid.uuid4().hex[:8]}"

            try:
                with TransactionalProducer(transaction_id, config) as tx_producer:
                    start_time = time.time()

                    tx_producer.begin()
                    for i in range(messages_per_tx):
                        tx_producer.send(
                            test_topic,
                            {
                                "transaction_id": transaction_id,
                                "message_id": i,
                                "timestamp": time.time(),
                            },
                        )

                    tx_producer.commit()
                    duration = time.time() - start_time

                    return {
                        "tx_id": tx_id,
                        "transaction_id": transaction_id,
                        "messages_sent": messages_per_tx,
                        "duration": duration,
                        "success": True,
                    }

            except Exception as e:
                errors.append(f"Transaction {tx_id}: {e}")
                return {
                    "tx_id": tx_id,
                    "transaction_id": transaction_id,
                    "messages_sent": 0,
                    "duration": 0,
                    "success": False,
                }

        # Run concurrent transactions
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=min(num_transactions, 10)) as executor:
            futures = [
                executor.submit(transaction_worker, i) for i in range(num_transactions)
            ]

            for future in as_completed(futures):
                results.append(future.result())

        overall_duration = time.time() - start_time

        successful_transactions = sum(1 for r in results if r["success"])
        total_messages = sum(r["messages_sent"] for r in results)
        avg_tx_duration = (
            statistics.mean([r["duration"] for r in results if r["success"]])
            if successful_transactions > 0
            else 0
        )

        print(" Concurrent transactions stress test:")
        print(f"  - Transactions: {num_transactions}")
        print(f"  - Successful: {successful_transactions}/{num_transactions}")
        print(f"  - Total messages: {total_messages}")
        print(f"  - Overall duration: {overall_duration:.2f}s")
        print(f"  - Average transaction duration: {avg_tx_duration:.3f}s")
        print(f"  - Errors: {len(errors)}")

        if errors:
            for error in errors[:3]:
                print(f"    {error}")

        assert successful_transactions >= num_transactions * 0.9, (
            "At least 90% of transactions should succeed"
        )

    def test_transaction_rollback_stress(self, test_topic: str, kafka_servers: str):
        """Stress test transaction rollback scenarios."""
        config = Config(bootstrap_servers=kafka_servers)
        num_rollbacks = 10
        messages_per_tx = 5

        successful_rollbacks = 0
        errors = []

        for i in range(num_rollbacks):
            tx_id = f"rollback-stress-{i}-{uuid.uuid4().hex[:8]}"

            try:
                with TransactionalProducer(tx_id, config) as tx_producer:
                    tx_producer.begin()

                    # Send some messages
                    for j in range(messages_per_tx):
                        tx_producer.send(
                            test_topic,
                            {"rollback_test": True, "tx_id": tx_id, "message_id": j},
                        )

                    # Force rollback
                    tx_producer.abort()
                    successful_rollbacks += 1

            except Exception as e:
                errors.append(f"Rollback {i}: {e}")

        print(" Transaction rollback stress test:")
        print(f"  - Rollback attempts: {num_rollbacks}")
        print(f"  - Successful rollbacks: {successful_rollbacks}")
        print(f"  - Errors: {len(errors)}")

        if errors:
            for error in errors:
                print(f"    {error}")

        assert successful_rollbacks >= num_rollbacks * 0.9, (
            "At least 90% of rollbacks should succeed"
        )


@pytest.mark.stress
class TestMemoryAndResourceStress:
    """Stress tests for memory usage and resource management."""

    def test_producer_connection_stress(self, test_topic: str, kafka_servers: str):
        """Stress test producer connection creation/destruction."""
        config = Config(bootstrap_servers=kafka_servers)
        num_connections = 50
        messages_per_connection = 10

        successful_connections = 0
        total_messages = 0
        errors = []

        start_time = time.time()

        for i in range(num_connections):
            try:
                with Producer(config) as producer:
                    for j in range(messages_per_connection):
                        producer.send(
                            test_topic,
                            {
                                "connection_stress": True,
                                "connection_id": i,
                                "message_id": j,
                            },
                        )

                    producer.flush()
                    successful_connections += 1
                    total_messages += messages_per_connection

            except Exception as e:
                errors.append(f"Connection {i}: {e}")

        duration = time.time() - start_time
        throughput = total_messages / duration if duration > 0 else 0

        print(" Producer connection stress test:")
        print(f"  - Connection attempts: {num_connections}")
        print(f"  - Successful connections: {successful_connections}")
        print(f"  - Total messages: {total_messages}")
        print(f"  - Duration: {duration:.2f}s")
        print(f"  - Throughput: {throughput:.0f} msg/s")
        print(f"  - Errors: {len(errors)}")

        if errors:
            for error in errors[:3]:
                print(f"    {error}")

        assert successful_connections >= num_connections * 0.95, (
            "At least 95% of connections should succeed"
        )

    def test_message_size_stress(self, test_topic: str, kafka_servers: str):
        """Stress test with varying message sizes."""
        config = Config(bootstrap_servers=kafka_servers)

        # Test different message sizes
        size_tests = [
            ("tiny", 10),  # 10 bytes
            ("small", 1024),  # 1KB
            ("medium", 10240),  # 10KB
            ("large", 102400),  # 100KB
            ("xlarge", 256000),  # 256KB
        ]

        results = {}

        for size_name, payload_size in size_tests:
            payload = "x" * payload_size
            message_count = max(
                10, 10000 // payload_size
            )  # Fewer messages for larger sizes

            start_time = time.time()
            errors = []

            try:
                with Producer(config) as producer:
                    for i in range(message_count):
                        try:
                            producer.send(
                                test_topic,
                                {
                                    "size_stress": True,
                                    "size_category": size_name,
                                    "message_id": i,
                                    "payload": payload,
                                },
                            )

                            # Flush every 10 messages for large messages
                            if payload_size > 50000 and i % 10 == 0:
                                producer.flush()

                        except Exception as e:
                            errors.append(f"Message {i}: {e}")

                    producer.flush()

            except Exception as e:
                errors.append(f"Producer error: {e}")

            duration = time.time() - start_time
            successful_messages = message_count - len(errors)
            throughput = successful_messages / duration if duration > 0 else 0
            data_rate = (
                (successful_messages * payload_size) / duration / 1024 / 1024
                if duration > 0
                else 0
            )  # MB/s

            results[size_name] = {
                "message_count": message_count,
                "successful": successful_messages,
                "payload_size": payload_size,
                "duration": duration,
                "throughput": throughput,
                "data_rate": data_rate,
                "errors": len(errors),
            }

        print(" Message size stress test:")
        for size_name, metrics in results.items():
            print(
                f"  - {size_name}: {metrics['successful']}/{metrics['message_count']} msgs, "
                f"{metrics['throughput']:.0f} msg/s, {metrics['data_rate']:.1f} MB/s, "
                f"{metrics['errors']} errors"
            )

        # Verify all size categories worked
        for size_name, metrics in results.items():
            success_rate = (
                metrics["successful"] / metrics["message_count"]
                if metrics["message_count"] > 0
                else 0
            )
            assert success_rate >= 0.9, (
                f"Size {size_name} should have 90%+ success rate"
            )


if __name__ == "__main__":
    # Run stress tests manually
    print(" Running Stress Tests...")
    print("=" * 60)
    print("Use: uv run pytest tests/test_stress.py -v -m stress")
    print("Warning: These tests are intensive and may take several minutes!")
