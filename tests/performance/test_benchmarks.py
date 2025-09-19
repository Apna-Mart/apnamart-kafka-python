"""Performance benchmark tests for apnamart-kafka-python library."""

import statistics
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from apnamart_kafka import Config, Consumer, Producer, TransactionalProducer


@pytest.mark.benchmark
class TestThroughputBenchmarks:
    """Benchmarks for message throughput."""

    def test_sustained_throughput(self, test_topic: str, kafka_servers: str):
        """Test sustained message throughput over time."""
        config = Config(bootstrap_servers=kafka_servers)
        duration_seconds = 10
        message_count = 0

        with Producer(config) as producer:
            start_time = time.time()

            while (time.time() - start_time) < duration_seconds:
                producer.send(
                    test_topic, {"sustained": message_count, "timestamp": time.time()}
                )
                message_count += 1

                # Flush periodically to maintain flow
                if message_count % 100 == 0:
                    producer.flush()

            producer.flush()

        actual_duration = time.time() - start_time
        throughput = message_count / actual_duration

        print(
            f" Sustained throughput: {throughput:.0f} messages/second over {actual_duration:.1f}s"
        )
        assert throughput > 1000, "Should achieve at least 1000 msg/s"

        return throughput

    def test_batch_throughput(self, test_topic: str, kafka_servers: str):
        """Test batch sending throughput."""
        config = Config(bootstrap_servers=kafka_servers)
        batch_sizes = [10, 50, 100, 500]
        results = {}

        for batch_size in batch_sizes:
            messages = [
                {"topic": test_topic, "value": {"batch": i, "size": batch_size}}
                for i in range(batch_size)
            ]

            with Producer(config) as producer:
                start_time = time.time()
                batch_results = producer.send_batch(messages)
                producer.flush()
                duration = time.time() - start_time

                successful = sum(1 for r in batch_results if r["success"])
                throughput = successful / duration if duration > 0 else 0

                results[batch_size] = {
                    "throughput": throughput,
                    "duration": duration,
                    "successful": successful,
                }

                print(
                    f" Batch {batch_size}: {throughput:.0f} msg/s ({successful} messages)"
                )

        # Verify larger batches are more efficient
        assert results[100]["throughput"] > results[10]["throughput"] * 0.8, (
            "Larger batches should be more efficient"
        )

        return results

    def test_message_size_vs_throughput(self, test_topic: str, kafka_servers: str):
        """Test how message size affects throughput."""
        config = Config(bootstrap_servers=kafka_servers)

        size_tests = [
            ("tiny", 100),  # 100 bytes
            ("small", 1024),  # 1KB
            ("medium", 10240),  # 10KB
            ("large", 102400),  # 100KB
        ]

        results = {}

        for size_name, payload_size in size_tests:
            payload = "x" * payload_size
            message_count = max(10, 50000 // payload_size)  # Adjust count for size

            with Producer(config) as producer:
                start_time = time.time()

                for i in range(message_count):
                    producer.send(
                        test_topic,
                        {"size_test": size_name, "message_id": i, "payload": payload},
                    )

                producer.flush()
                duration = time.time() - start_time

                throughput = message_count / duration
                data_rate = (
                    (message_count * payload_size) / duration / 1024 / 1024
                )  # MB/s

                results[size_name] = {
                    "throughput": throughput,
                    "data_rate": data_rate,
                    "payload_size": payload_size,
                }

                print(f" {size_name}: {throughput:.0f} msg/s, {data_rate:.1f} MB/s")

        return results


@pytest.mark.benchmark
class TestLatencyBenchmarks:
    """Benchmarks for message latency."""

    def test_end_to_end_latency(self, test_topic: str, kafka_servers: str):
        """Test end-to-end message latency."""
        producer_config = Config(bootstrap_servers=kafka_servers)
        consumer_config = Config(
            bootstrap_servers=kafka_servers,
            group_id=f"latency-test-{uuid.uuid4().hex[:8]}",
            auto_offset_reset="latest",
        )

        latencies = []
        message_count = 20

        with (
            Producer(producer_config) as producer,
            Consumer(test_topic, consumer_config) as consumer,
        ):
            for i in range(message_count):
                send_time = time.time()

                # Send message with timestamp
                producer.send(
                    test_topic,
                    {"latency_test": True, "message_id": i, "send_time": send_time},
                )
                producer.flush()

                # Poll for the message
                start_poll = time.time()
                while (time.time() - start_poll) < 5.0:  # 5 second timeout
                    message = consumer.poll(timeout=0.1)
                    if message and message.value.get("message_id") == i:
                        receive_time = time.time()
                        latency = receive_time - send_time
                        latencies.append(latency)
                        consumer.commit(message)
                        break

        if latencies:
            avg_latency = statistics.mean(latencies) * 1000  # Convert to ms
            min_latency = min(latencies) * 1000
            max_latency = max(latencies) * 1000

            print(" End-to-end latency:")
            print(f"  Average: {avg_latency:.2f}ms")
            print(f"  Min: {min_latency:.2f}ms")
            print(f"  Max: {max_latency:.2f}ms")

            assert avg_latency < 1000, "Average latency should be under 1 second"

            return {
                "average_ms": avg_latency,
                "min_ms": min_latency,
                "max_ms": max_latency,
                "sample_count": len(latencies),
            }

        pytest.fail("No latency measurements collected")


@pytest.mark.benchmark
class TestConcurrencyBenchmarks:
    """Benchmarks for concurrent operations."""

    def test_concurrent_producers(self, test_topic: str, kafka_servers: str):
        """Test multiple producers working concurrently."""
        config = Config(bootstrap_servers=kafka_servers)
        num_producers = 5
        messages_per_producer = 100

        def producer_worker(worker_id: int):
            with Producer(config) as producer:
                start_time = time.time()

                for i in range(messages_per_producer):
                    producer.send(
                        test_topic,
                        {
                            "concurrent_test": True,
                            "worker_id": worker_id,
                            "message_id": i,
                            "timestamp": time.time(),
                        },
                    )

                producer.flush()
                duration = time.time() - start_time

                return {
                    "worker_id": worker_id,
                    "messages": messages_per_producer,
                    "duration": duration,
                    "throughput": messages_per_producer / duration,
                }

        # Run concurrent producers
        overall_start = time.time()
        with ThreadPoolExecutor(max_workers=num_producers) as executor:
            futures = [
                executor.submit(producer_worker, i) for i in range(num_producers)
            ]
            results = [future.result() for future in as_completed(futures)]

        overall_duration = time.time() - overall_start
        total_messages = sum(r["messages"] for r in results)
        overall_throughput = total_messages / overall_duration

        individual_throughputs = [r["throughput"] for r in results]
        avg_individual = statistics.mean(individual_throughputs)

        print(" Concurrent producers:")
        print(f"  {num_producers} producers × {messages_per_producer} messages")
        print(f"  Overall throughput: {overall_throughput:.0f} msg/s")
        print(f"  Average individual: {avg_individual:.0f} msg/s")
        print(f"  Efficiency: {(overall_throughput / avg_individual) * 100:.1f}%")

        assert len(results) == num_producers, "All producers should complete"

        return {
            "overall_throughput": overall_throughput,
            "avg_individual_throughput": avg_individual,
            "total_messages": total_messages,
            "duration": overall_duration,
        }


@pytest.mark.benchmark
class TestTransactionBenchmarks:
    """Benchmarks for transactional operations."""

    def test_transaction_overhead(self, test_topic: str, kafka_servers: str):
        """Compare transactional vs non-transactional performance."""
        config = Config(bootstrap_servers=kafka_servers)
        message_count = 50

        # Test non-transactional baseline
        with Producer(config) as producer:
            start_time = time.time()
            for i in range(message_count):
                producer.send(test_topic, {"normal": i})
            producer.flush()
            normal_duration = time.time() - start_time

        # Test transactional
        tx_id = f"benchmark-tx-{uuid.uuid4().hex[:8]}"
        with TransactionalProducer(tx_id, config) as tx_producer:
            start_time = time.time()
            tx_producer.begin()
            for i in range(message_count):
                tx_producer.send(test_topic, {"transactional": i})
            tx_producer.commit()
            tx_duration = time.time() - start_time

        # Calculate overhead
        overhead_ratio = tx_duration / normal_duration if normal_duration > 0 else 0
        normal_throughput = message_count / normal_duration
        tx_throughput = message_count / tx_duration

        print(" Transaction overhead comparison:")
        print(f"  Normal: {normal_duration:.3f}s ({normal_throughput:.0f} msg/s)")
        print(f"  Transactional: {tx_duration:.3f}s ({tx_throughput:.0f} msg/s)")
        print(f"  Overhead: {overhead_ratio:.1f}x")

        return {
            "normal_throughput": normal_throughput,
            "transactional_throughput": tx_throughput,
            "overhead_ratio": overhead_ratio,
        }


if __name__ == "__main__":
    print(" Performance Benchmark Tests")
    print("=" * 40)
    print("These tests measure throughput, latency, and concurrency performance.")
    print(
        "Run with: uv run pytest tests/performance/test_benchmarks.py -v -m benchmark"
    )
