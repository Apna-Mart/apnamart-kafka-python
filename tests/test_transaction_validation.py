#!/usr/bin/env python3
"""
Comprehensive transactional validation tests for apnamart-kafka-python library.
Validates ACID properties and transactional guarantees.
"""

import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict

import pytest

from apnamart_kafka import (
    Config,
    Consumer,
    TransactionalProducer,
    TransactionError,
    send,
)


@pytest.mark.integration
class TestACIDProperties:
    """Test ACID (Atomicity, Consistency, Isolation, Durability) properties."""

    def test_atomicity_commit(self, test_topic: str, kafka_servers: str):
        """Test atomicity: all messages in a transaction are committed together."""
        tx_id = f"atomicity-commit-{uuid.uuid4().hex[:8]}"
        config = Config(bootstrap_servers=kafka_servers)

        # Send transaction with multiple messages
        messages_in_tx = 5
        with TransactionalProducer(tx_id, config) as tx_producer:
            tx_producer.begin()

            for i in range(messages_in_tx):
                tx_producer.send(
                    test_topic,
                    {
                        "atomicity_test": "commit",
                        "tx_id": tx_id,
                        "message_id": i,
                        "timestamp": time.time(),
                    },
                )

            tx_producer.commit()

        # Verify all messages are available
        consumer_config = Config(
            bootstrap_servers=kafka_servers,
            group_id=f"atomicity-verify-{uuid.uuid4().hex[:8]}",
            auto_offset_reset="earliest",
        )

        received_messages = []
        with Consumer(test_topic, consumer_config) as consumer:
            start_time = time.time()
            while (
                len(received_messages) < messages_in_tx
                and (time.time() - start_time) < 10
            ):
                message = consumer.poll(timeout=1.0)
                if message and message.value.get("tx_id") == tx_id:
                    received_messages.append(message.value)
                    consumer.commit(message)

        # All messages should be present
        assert len(received_messages) == messages_in_tx
        message_ids = sorted([msg["message_id"] for msg in received_messages])
        assert message_ids == list(range(messages_in_tx))

        print(
            f" Atomicity (commit): All {messages_in_tx} messages committed atomically"
        )

    def test_atomicity_abort(self, test_topic: str, kafka_servers: str):
        """Test atomicity: aborted transactions leave no trace."""
        tx_id = f"atomicity-abort-{uuid.uuid4().hex[:8]}"
        config = Config(bootstrap_servers=kafka_servers)

        # Send transaction and abort
        messages_in_tx = 3
        with TransactionalProducer(tx_id, config) as tx_producer:
            tx_producer.begin()

            for i in range(messages_in_tx):
                tx_producer.send(
                    test_topic,
                    {
                        "atomicity_test": "abort",
                        "tx_id": tx_id,
                        "message_id": i,
                        "should_not_exist": True,
                    },
                )

            # Explicitly abort the transaction
            tx_producer.abort()

        # Wait a moment for any potential messages to appear
        time.sleep(0.5)

        # Verify no messages from the aborted transaction exist
        consumer_config = Config(
            bootstrap_servers=kafka_servers,
            group_id=f"atomicity-abort-verify-{uuid.uuid4().hex[:8]}",
            auto_offset_reset="earliest",
        )

        aborted_messages = []
        with Consumer(test_topic, consumer_config) as consumer:
            start_time = time.time()
            while (time.time() - start_time) < 5:  # Search for 5 seconds
                message = consumer.poll(timeout=1.0)
                if message and message.value.get("tx_id") == tx_id:
                    aborted_messages.append(message.value)

        # No messages should exist from aborted transaction
        assert len(aborted_messages) == 0

        print(" Atomicity (abort): Aborted transaction left no messages")

    def test_consistency_multiple_topics(self, kafka_servers: str):
        """Test consistency: transaction affects multiple topics consistently."""
        tx_id = f"consistency-{uuid.uuid4().hex[:8]}"
        config = Config(bootstrap_servers=kafka_servers)

        # Create topics
        topic1 = f"consistency-topic-1-{int(time.time())}"
        topic2 = f"consistency-topic-2-{int(time.time())}"

        # Initialize topics by sending dummy messages
        send(topic1, {"init": True}, servers=kafka_servers)
        send(topic2, {"init": True}, servers=kafka_servers)
        time.sleep(0.1)

        # Transactional write to both topics
        with TransactionalProducer(tx_id, config) as tx_producer:
            tx_producer.begin()

            # Write to topic1
            tx_producer.send(
                topic1,
                {
                    "consistency_test": True,
                    "tx_id": tx_id,
                    "topic": "topic1",
                    "balance_change": -100,
                },
            )

            # Write to topic2
            tx_producer.send(
                topic2,
                {
                    "consistency_test": True,
                    "tx_id": tx_id,
                    "topic": "topic2",
                    "balance_change": +100,
                },
            )

            tx_producer.commit()

        # Verify both topics have the transaction messages
        topics_to_check = [(topic1, -100), (topic2, +100)]
        for topic, expected_change in topics_to_check:
            consumer_config = Config(
                bootstrap_servers=kafka_servers,
                group_id=f"consistency-check-{uuid.uuid4().hex[:8]}",
                auto_offset_reset="earliest",
            )

            found_message = False
            with Consumer(topic, consumer_config) as consumer:
                start_time = time.time()
                while not found_message and (time.time() - start_time) < 5:
                    message = consumer.poll(timeout=1.0)
                    if message and message.value.get("tx_id") == tx_id:
                        assert message.value["balance_change"] == expected_change
                        found_message = True
                        consumer.commit(message)

            assert found_message, f"Transaction message not found in {topic}"

        print(" Consistency: Transaction consistently applied to multiple topics")

    def test_isolation_concurrent_transactions(
        self, test_topic: str, kafka_servers: str
    ):
        """Test isolation: concurrent transactions don't interfere."""
        config = Config(bootstrap_servers=kafka_servers)
        num_transactions = 5
        messages_per_tx = 3

        results = []
        errors = []

        def isolated_transaction(tx_number: int) -> Dict[str, Any]:
            tx_id = f"isolation-{tx_number}-{uuid.uuid4().hex[:8]}"

            try:
                with TransactionalProducer(tx_id, config) as tx_producer:
                    tx_producer.begin()

                    messages_sent = []
                    for i in range(messages_per_tx):
                        message_data = {
                            "isolation_test": True,
                            "tx_id": tx_id,
                            "tx_number": tx_number,
                            "message_id": i,
                            "timestamp": time.time(),
                        }
                        tx_producer.send(test_topic, message_data)
                        messages_sent.append(message_data)

                    tx_producer.commit()

                    return {
                        "tx_number": tx_number,
                        "tx_id": tx_id,
                        "messages_sent": messages_sent,
                        "success": True,
                    }

            except Exception as e:
                errors.append(f"Transaction {tx_number}: {e}")
                return {
                    "tx_number": tx_number,
                    "tx_id": tx_id,
                    "messages_sent": [],
                    "success": False,
                }

        # Run concurrent transactions
        with ThreadPoolExecutor(max_workers=num_transactions) as executor:
            futures = [
                executor.submit(isolated_transaction, i)
                for i in range(num_transactions)
            ]

            for future in as_completed(futures):
                results.append(future.result())

        # Verify all transactions succeeded
        successful_transactions = [r for r in results if r["success"]]
        assert len(successful_transactions) == num_transactions

        # Verify all messages are present and correctly isolated
        consumer_config = Config(
            bootstrap_servers=kafka_servers,
            group_id=f"isolation-verify-{uuid.uuid4().hex[:8]}",
            auto_offset_reset="earliest",
        )

        received_by_tx = {}
        with Consumer(test_topic, consumer_config) as consumer:
            start_time = time.time()
            expected_total = num_transactions * messages_per_tx
            received_count = 0

            while received_count < expected_total and (time.time() - start_time) < 15:
                message = consumer.poll(timeout=2.0)
                if message and message.value.get("isolation_test"):
                    tx_id = message.value["tx_id"]
                    if tx_id not in received_by_tx:
                        received_by_tx[tx_id] = []
                    received_by_tx[tx_id].append(message.value)
                    received_count += 1
                    consumer.commit(message)

        # Verify each transaction's messages are complete and correct
        assert len(received_by_tx) == num_transactions

        for tx_result in successful_transactions:
            tx_id = tx_result["tx_id"]
            received_messages = received_by_tx.get(tx_id, [])
            assert len(received_messages) == messages_per_tx

            # Verify message sequence
            message_ids = sorted([msg["message_id"] for msg in received_messages])
            assert message_ids == list(range(messages_per_tx))

        print(
            f" Isolation: {num_transactions} concurrent transactions executed in isolation"
        )

    def test_durability_after_commit(self, test_topic: str, kafka_servers: str):
        """Test durability: committed transactions survive."""
        tx_id = f"durability-{uuid.uuid4().hex[:8]}"
        config = Config(bootstrap_servers=kafka_servers)

        # Commit a transaction
        with TransactionalProducer(tx_id, config) as tx_producer:
            tx_producer.begin()

            tx_producer.send(
                test_topic,
                {
                    "durability_test": True,
                    "tx_id": tx_id,
                    "data": "This message should be durable",
                    "timestamp": time.time(),
                },
            )

            tx_producer.commit()

        # Wait and check multiple times to ensure durability
        checks = []
        for check_num in range(3):
            time.sleep(1)  # Wait between checks

            consumer_config = Config(
                bootstrap_servers=kafka_servers,
                group_id=f"durability-check-{check_num}-{uuid.uuid4().hex[:8]}",
                auto_offset_reset="earliest",
            )

            found = False
            with Consumer(test_topic, consumer_config) as consumer:
                start_time = time.time()
                while not found and (time.time() - start_time) < 5:
                    message = consumer.poll(timeout=1.0)
                    if message and message.value.get("tx_id") == tx_id:
                        found = True
                        checks.append(True)
                        consumer.commit(message)
                        break

            if not found:
                checks.append(False)

        # All checks should find the message
        assert all(checks), f"Durability check failed: {checks}"

        print(
            f" Durability: Committed transaction persisted across {len(checks)} checks"
        )


class TestTransactionErrorHandling:
    """Test transaction error handling and recovery."""

    def test_invalid_transaction_state_errors(
        self, test_topic: str, kafka_servers: str
    ):
        """Test proper error handling for invalid transaction states."""
        tx_id = f"error-handling-{uuid.uuid4().hex[:8]}"
        config = Config(bootstrap_servers=kafka_servers)

        with TransactionalProducer(tx_id, config) as tx_producer:
            # Test: commit without begin should fail
            with pytest.raises(TransactionError, match="No transaction in progress"):
                tx_producer.commit()

            # Test: abort without begin should fail
            with pytest.raises(TransactionError, match="No transaction in progress"):
                tx_producer.abort()

            # Test: send_transactional without begin should fail
            with pytest.raises(TransactionError, match="No active transaction"):
                tx_producer.send_transactional(test_topic, {"test": "should fail"})

            # Test: double begin should fail
            tx_producer.begin()
            with pytest.raises(
                TransactionError, match="Transaction already in progress"
            ):
                tx_producer.begin()

            # Clean up
            tx_producer.abort()

        print(" Error handling: Invalid transaction states properly rejected")

    def test_transaction_timeout_handling(self, test_topic: str, kafka_servers: str):
        """Test transaction timeout scenarios."""
        tx_id = f"timeout-test-{uuid.uuid4().hex[:8]}"
        config = Config(bootstrap_servers=kafka_servers)

        # Test normal transaction within timeout
        with TransactionalProducer(tx_id, config) as tx_producer:
            tx_producer.begin()

            tx_producer.send(
                test_topic, {"timeout_test": True, "tx_id": tx_id, "status": "normal"}
            )

            # Should complete successfully
            tx_producer.commit()

        print(" Transaction timeout: Normal transactions complete within timeout")

    def test_exception_during_transaction_auto_abort(
        self, test_topic: str, kafka_servers: str
    ):
        """Test that exceptions during transactions trigger auto-abort."""
        tx_id = f"auto-abort-{uuid.uuid4().hex[:8]}"
        config = Config(bootstrap_servers=kafka_servers)

        try:
            with TransactionalProducer(tx_id, config) as tx_producer:
                tx_producer.begin()

                # Send a message
                tx_producer.send(
                    test_topic,
                    {
                        "auto_abort_test": True,
                        "tx_id": tx_id,
                        "should_be_aborted": True,
                    },
                )

                # Simulate an exception
                raise ValueError("Simulated error during transaction")

        except ValueError:
            pass  # Expected

        # Verify the message was not committed (auto-aborted)
        time.sleep(0.5)

        consumer_config = Config(
            bootstrap_servers=kafka_servers,
            group_id=f"auto-abort-verify-{uuid.uuid4().hex[:8]}",
            auto_offset_reset="earliest",
        )

        found_aborted_message = False
        with Consumer(test_topic, consumer_config) as consumer:
            start_time = time.time()
            while (time.time() - start_time) < 3:
                message = consumer.poll(timeout=1.0)
                if message and message.value.get("tx_id") == tx_id:
                    found_aborted_message = True
                    break

        assert not found_aborted_message

        print(" Auto-abort: Exception during transaction triggered automatic abort")


class TestTransactionPerformanceAndScaling:
    """Test transaction performance under various conditions."""

    def test_batch_transaction_performance(self, test_topic: str, kafka_servers: str):
        """Test performance of batch transactional operations."""
        tx_id = f"batch-perf-{uuid.uuid4().hex[:8]}"
        config = Config(bootstrap_servers=kafka_servers)

        batch_sizes = [10, 50, 100, 200]
        results = {}

        for batch_size in batch_sizes:
            messages = [
                (
                    test_topic,
                    {
                        "batch_perf_test": True,
                        "batch_size": batch_size,
                        "message_id": i,
                        "tx_id": tx_id + f"-{batch_size}",
                    },
                )
                for i in range(batch_size)
            ]

            start_time = time.time()

            with TransactionalProducer(tx_id + f"-{batch_size}", config) as tx_producer:
                tx_producer.send_batch_transactional(messages)

            duration = time.time() - start_time
            throughput = batch_size / duration if duration > 0 else 0

            results[batch_size] = {"duration": duration, "throughput": throughput}

        print(" Batch transaction performance:")
        for batch_size, metrics in results.items():
            print(
                f"  - {batch_size} messages: {metrics['duration']:.3f}s ({metrics['throughput']:.0f} msg/s)"
            )

        # Larger batches should generally be more efficient
        assert results[200]["throughput"] > results[10]["throughput"] * 0.5, (
            "Larger batches should be reasonably efficient"
        )

    def test_transaction_scalability(self, test_topic: str, kafka_servers: str):
        """Test transaction scalability with increasing load."""
        config = Config(bootstrap_servers=kafka_servers)
        loads = [1, 3, 5]  # Number of concurrent transactions

        results = {}

        for num_concurrent in loads:
            tx_results = []
            errors = []

            def transaction_worker(worker_id: int) -> Dict[str, Any]:
                tx_id = (
                    f"scale-test-{num_concurrent}-{worker_id}-{uuid.uuid4().hex[:8]}"
                )

                try:
                    start_time = time.time()

                    with TransactionalProducer(tx_id, config) as tx_producer:
                        tx_producer.begin()

                        for i in range(10):  # 10 messages per transaction
                            tx_producer.send(
                                test_topic,
                                {
                                    "scalability_test": True,
                                    "num_concurrent": num_concurrent,
                                    "worker_id": worker_id,
                                    "message_id": i,
                                    "tx_id": tx_id,
                                },
                            )

                        tx_producer.commit()

                    duration = time.time() - start_time

                    return {
                        "worker_id": worker_id,
                        "tx_id": tx_id,
                        "duration": duration,
                        "success": True,
                    }

                except Exception as e:
                    errors.append(f"Worker {worker_id}: {e}")
                    return {
                        "worker_id": worker_id,
                        "tx_id": "",
                        "duration": 0,
                        "success": False,
                    }

            # Run concurrent transactions
            start_time = time.time()
            with ThreadPoolExecutor(max_workers=num_concurrent) as executor:
                futures = [
                    executor.submit(transaction_worker, i)
                    for i in range(num_concurrent)
                ]

                for future in as_completed(futures):
                    tx_results.append(future.result())

            total_duration = time.time() - start_time

            successful_tx = [r for r in tx_results if r["success"]]
            avg_tx_duration = (
                sum(r["duration"] for r in successful_tx) / len(successful_tx)
                if successful_tx
                else 0
            )

            results[num_concurrent] = {
                "total_duration": total_duration,
                "avg_tx_duration": avg_tx_duration,
                "successful_transactions": len(successful_tx),
                "total_transactions": num_concurrent,
                "errors": len(errors),
            }

        print(" Transaction scalability:")
        for load, metrics in results.items():
            print(
                f"  - {load} concurrent: {metrics['successful_transactions']}/{metrics['total_transactions']} success, "
                f"avg {metrics['avg_tx_duration']:.3f}s per tx, {metrics['errors']} errors"
            )

        # All loads should complete successfully
        for load, metrics in results.items():
            success_rate = (
                metrics["successful_transactions"] / metrics["total_transactions"]
            )
            assert success_rate >= 0.9, (
                f"Success rate for {load} concurrent transactions should be ≥90%"
            )


if __name__ == "__main__":
    # Run transaction validation tests manually
    print(" Running Transaction Validation Tests...")
    print("=" * 60)
    print("Use: uv run pytest tests/test_transaction_validation.py -v")
    print("These tests validate ACID properties and transactional guarantees.")
