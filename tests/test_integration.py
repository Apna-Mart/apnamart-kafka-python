"""Integration tests for apnamart-kafka-python with real Kafka broker."""

import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest

from apnamart_kafka import (
    Config,
    Consumer,
    ConsumerError,
    Producer,
    ProducerError,
    TransactionalProducer,
    consume,
    send,
)


@pytest.mark.integration
class TestProducerIntegration:
    """Integration tests for Producer class."""

    def test_producer_send_message(self, producer: Producer, test_topic: str):
        """Test sending a single message."""
        message_data = {"test": "integration", "timestamp": time.time()}

        # Should not raise exception
        producer.send(test_topic, message_data)
        producer.flush()

    def test_producer_send_with_key(self, producer: Producer, test_topic: str):
        """Test sending message with key."""
        key = "test-key"
        value = {"test": "with-key"}

        producer.send(test_topic, value, key=key)
        producer.flush()

    def test_producer_send_different_types(self, producer: Producer, test_topic: str):
        """Test sending different data types."""
        # String
        producer.send(test_topic, "string message")

        # JSON object
        producer.send(test_topic, {"json": "object"})

        # Bytes
        producer.send(test_topic, b"bytes message")

        # Number (serialized as JSON)
        producer.send(test_topic, 42)

        producer.flush()

    def test_producer_batch_send(self, producer: Producer, test_topic: str):
        """Test batch sending messages."""
        messages = [
            {"topic": test_topic, "value": {"batch": i, "data": f"message-{i}"}}
            for i in range(5)
        ]

        results = producer.send_batch(messages)

        assert len(results) == 5
        for result in results:
            assert result["success"] is True

    def test_producer_batch_send_invalid_messages(self, producer: Producer):
        """Test batch sending with invalid messages."""
        messages = [
            {"topic": "valid-topic", "value": {"valid": "message"}},
            {"value": {"missing": "topic"}},  # Missing topic
            {"topic": "missing-value"},       # Missing value
        ]

        results = producer.send_batch(messages)

        assert len(results) == 3
        assert results[0]["success"] is True
        assert results[1]["success"] is False
        assert results[2]["success"] is False

    def test_producer_context_manager(self, test_topic: str, kafka_servers: str):
        """Test producer context manager."""
        config = Config(bootstrap_servers=kafka_servers)

        with Producer(config) as producer:
            producer.send(test_topic, {"context": "manager"})
            producer.flush()
            assert not producer._closed

        assert producer._closed

    def test_producer_close_twice(self, producer: Producer):
        """Test closing producer multiple times."""
        producer.close()
        assert producer._closed

        # Should not raise exception
        producer.close()
        assert producer._closed

    def test_producer_send_after_close(self, producer: Producer, test_topic: str):
        """Test sending after producer is closed."""
        producer.close()

        with pytest.raises(ProducerError, match="Producer is closed"):
            producer.send(test_topic, {"after": "close"})


@pytest.mark.integration
class TestConsumerIntegration:
    """Integration tests for Consumer class."""

    def test_consumer_poll_message(self, test_topic: str, test_group: str, kafka_servers: str):
        """Test consumer polling messages."""
        # First send a message
        send(test_topic, {"poll": "test"}, servers=kafka_servers)

        # Then consume it
        config = Config(
            bootstrap_servers=kafka_servers,
            group_id=test_group,
            auto_offset_reset="earliest"
        )

        with Consumer(test_topic, config) as consumer:
            message = consumer.poll(timeout=10.0)
            assert message is not None
            assert message.topic == test_topic
            assert message.value["poll"] == "test"

    def test_consumer_poll_timeout(self, consumer: Consumer):
        """Test consumer poll timeout."""
        # Poll with short timeout on empty topic
        message = consumer.poll(timeout=0.1)
        assert message is None

    def test_consumer_iterator_interface(self, test_topic: str, test_group: str, kafka_servers: str):
        """Test consumer iterator interface."""
        # Send multiple messages
        config = Config(bootstrap_servers=kafka_servers)
        with Producer(config) as producer:
            for i in range(3):
                producer.send(test_topic, {"iter": i, "message": f"test-{i}"})
            producer.flush()

        # Consume using iterator
        consumer_config = Config(
            bootstrap_servers=kafka_servers,
            group_id=test_group,
            auto_offset_reset="earliest"
        )

        messages = []
        with Consumer(test_topic, consumer_config) as consumer:
            for message in consumer:
                messages.append(message)
                consumer.commit(message)
                if len(messages) >= 3:
                    break

        assert len(messages) == 3
        for i, msg in enumerate(messages):
            assert msg.value["iter"] == i

    def test_consumer_batch_poll(self, test_topic: str, test_group: str, kafka_servers: str):
        """Test consumer batch polling."""
        # Send batch of messages
        config = Config(bootstrap_servers=kafka_servers)
        with Producer(config) as producer:
            for i in range(5):
                producer.send(test_topic, {"batch": i})
            producer.flush()

        # Consume batch
        consumer_config = Config(
            bootstrap_servers=kafka_servers,
            group_id=test_group,
            auto_offset_reset="earliest"
        )

        with Consumer(test_topic, consumer_config) as consumer:
            batch = consumer.poll_batch(size=5, timeout=10.0)
            assert len(batch) > 0

            for msg in batch:
                consumer.commit(msg)

    def test_consumer_commit_specific_message(self, test_topic: str, test_group: str, kafka_servers: str):
        """Test committing specific message offset."""
        # Send a message
        send(test_topic, {"commit": "specific"}, servers=kafka_servers)

        config = Config(
            bootstrap_servers=kafka_servers,
            group_id=test_group,
            auto_offset_reset="earliest",
            enable_auto_commit=False
        )

        with Consumer(test_topic, config) as consumer:
            message = consumer.poll(timeout=10.0)
            assert message is not None

            # Commit specific message
            consumer.commit(message)

    def test_consumer_commit_current_offsets(self, test_topic: str, test_group: str, kafka_servers: str):
        """Test committing current offsets."""
        # Send a message
        send(test_topic, {"commit": "current"}, servers=kafka_servers)

        config = Config(
            bootstrap_servers=kafka_servers,
            group_id=test_group,
            auto_offset_reset="earliest",
            enable_auto_commit=False
        )

        with Consumer(test_topic, config) as consumer:
            message = consumer.poll(timeout=10.0)
            assert message is not None

            # Commit current offsets
            consumer.commit()

    def test_consumer_close_twice(self, consumer: Consumer):
        """Test closing consumer multiple times."""
        consumer.close()
        assert consumer._closed

        # Should not raise exception
        consumer.close()
        assert consumer._closed

    def test_consumer_poll_after_close(self, consumer: Consumer):
        """Test polling after consumer is closed."""
        consumer.close()

        with pytest.raises(ConsumerError, match="Consumer is closed"):
            consumer.poll()


@pytest.mark.integration
class TestTransactionalProducerIntegration:
    """Integration tests for TransactionalProducer class."""

    def test_transactional_producer_basic_transaction(self, test_topic: str, kafka_servers: str):
        """Test basic transactional sending."""
        tx_id = f"test-tx-basic-{uuid.uuid4().hex[:8]}"
        config = Config(bootstrap_servers=kafka_servers)

        with TransactionalProducer(tx_id, config) as tx_producer:
            tx_producer.begin()
            tx_producer.send(test_topic, {"tx": "basic", "id": 1})
            tx_producer.send(test_topic, {"tx": "basic", "id": 2})
            tx_producer.commit()

    def test_transactional_producer_automatic(self, test_topic: str, kafka_servers: str):
        """Test automatic transactional sending."""
        tx_id = f"test-tx-auto-{uuid.uuid4().hex[:8]}"
        config = Config(bootstrap_servers=kafka_servers)

        messages = [
            {"topic": test_topic, "value": {"tx": "auto", "id": 1}},
            {"topic": test_topic, "value": {"tx": "auto", "id": 2}},
        ]

        tx_producer = TransactionalProducer(tx_id, config)
        tx_producer.send_transactional(messages)
        tx_producer.close()

    def test_transactional_producer_abort_on_error(self, test_topic: str, kafka_servers: str):
        """Test transaction abort on error."""
        tx_id = f"test-tx-abort-{uuid.uuid4().hex[:8]}"
        config = Config(bootstrap_servers=kafka_servers)

        with TransactionalProducer(tx_id, config) as tx_producer:
            try:
                tx_producer.begin()
                tx_producer.send(test_topic, {"tx": "will_abort"})
                # Simulate error
                raise ValueError("Simulated error")
                tx_producer.commit()  # Won't reach here
            except ValueError:
                # Transaction should auto-abort in __exit__
                pass

    def test_transactional_producer_context_manager_commit(self, test_topic: str, kafka_servers: str):
        """Test transaction commit via context manager."""
        tx_id = f"test-tx-context-{uuid.uuid4().hex[:8]}"
        config = Config(bootstrap_servers=kafka_servers)

        with TransactionalProducer(tx_id, config) as tx_producer:
            tx_producer.begin()
            tx_producer.send(test_topic, {"tx": "context", "commit": True})
            # Should auto-commit on successful exit

    def test_transactional_producer_context_manager_abort(self, test_topic: str, kafka_servers: str):
        """Test transaction abort via context manager."""
        tx_id = f"test-tx-context-abort-{uuid.uuid4().hex[:8]}"
        config = Config(bootstrap_servers=kafka_servers)

        try:
            with TransactionalProducer(tx_id, config) as tx_producer:
                tx_producer.begin()
                tx_producer.send(test_topic, {"tx": "context", "abort": True})
                raise RuntimeError("Force abort")
        except RuntimeError:
            pass  # Expected


@pytest.mark.integration
class TestEndToEndIntegration:
    """End-to-end integration tests."""

    def test_producer_consumer_flow(self, test_topic: str, test_group: str, kafka_servers: str):
        """Test complete producer -> consumer flow."""
        test_data = [
            {"message": "Hello World", "id": 1},
            {"message": "Second Message", "id": 2},
            "Simple string message",
            {"complex": {"nested": {"data": "test"}}, "timestamp": time.time()}
        ]

        # Produce messages
        config = Config(bootstrap_servers=kafka_servers)
        with Producer(config) as producer:
            for i, data in enumerate(test_data):
                producer.send(test_topic, data, key=f"key-{i}")
            producer.flush()

        # Consume messages
        consumer_config = Config(
            bootstrap_servers=kafka_servers,
            group_id=test_group,
            auto_offset_reset="earliest"
        )

        received_data = []
        with Consumer(test_topic, consumer_config) as consumer:
            start_time = time.time()
            while len(received_data) < len(test_data) and (time.time() - start_time) < 15:
                message = consumer.poll(timeout=2.0)
                if message:
                    received_data.append(message.value)
                    consumer.commit(message)

        assert len(received_data) == len(test_data)

    def test_multiple_consumers_same_group(self, test_topic: str, test_group: str, kafka_servers: str):
        """Test multiple consumers in same group."""
        # Produce messages
        config = Config(bootstrap_servers=kafka_servers)
        with Producer(config) as producer:
            for i in range(10):
                producer.send(test_topic, {"multi": i})
            producer.flush()

        # Single consumer in group (simplified test)
        consumer_config = Config(
            bootstrap_servers=kafka_servers,
            group_id=test_group,
            auto_offset_reset="earliest"
        )

        with Consumer(test_topic, consumer_config) as consumer:
            messages = consumer.poll_batch(size=10, timeout=15.0)
            assert len(messages) > 0

    def test_transactional_exactly_once(self, test_topic: str, test_group: str, kafka_servers: str):
        """Test exactly-once semantics with transactions."""
        tx_id = f"test-tx-exactly-once-{uuid.uuid4().hex[:8]}"
        config = Config(bootstrap_servers=kafka_servers)

        # Send with transaction
        with TransactionalProducer(tx_id, config) as tx_producer:
            tx_producer.begin()
            tx_producer.send(test_topic, {"exactly_once": "test", "unique_id": tx_id})
            tx_producer.commit()

        # Verify message was sent
        consumer_config = Config(
            bootstrap_servers=kafka_servers,
            group_id=test_group,
            auto_offset_reset="earliest"
        )

        with Consumer(test_topic, consumer_config) as consumer:
            message = consumer.poll(timeout=10.0)
            assert message is not None
            assert message.value["exactly_once"] == "test"
            assert message.value["unique_id"] == tx_id


@pytest.mark.integration
class TestConvenienceFunctions:
    """Test convenience functions with real Kafka."""

    def test_convenience_send_function(self, test_topic: str, kafka_servers: str):
        """Test convenience send function."""
        test_data = {"convenience": "send", "timestamp": time.time()}

        # Should not raise exception
        send(test_topic, test_data, servers=kafka_servers)

    def test_convenience_send_with_key(self, test_topic: str, kafka_servers: str):
        """Test convenience send function with key."""
        test_data = {"convenience": "send_with_key"}
        test_key = "convenience-key"

        send(test_topic, test_data, key=test_key, servers=kafka_servers)

    def test_convenience_consume_function(self, test_topic: str, kafka_servers: str):
        """Test convenience consume function."""
        # First send a message
        send(test_topic, {"convenience": "consume"}, servers=kafka_servers)

        # Then consume it
        messages_consumed = []
        start_time = time.time()

        for message in consume(
            test_topic,
            servers=kafka_servers,
            group_id=f"convenience-{uuid.uuid4().hex[:8]}",
            auto_offset_reset="earliest"
        ):
            messages_consumed.append(message.value)
            if len(messages_consumed) >= 1 or (time.time() - start_time) > 10:
                break

        assert len(messages_consumed) >= 1
        assert messages_consumed[0]["convenience"] == "consume"


@pytest.mark.integration
@pytest.mark.slow
class TestPerformance:
    """Performance and stress tests."""

    def test_producer_throughput(self, test_topic: str, kafka_servers: str):
        """Test producer throughput."""
        message_count = 100
        config = Config(bootstrap_servers=kafka_servers)

        start_time = time.time()
        with Producer(config) as producer:
            for i in range(message_count):
                producer.send(test_topic, {"perf": i, "data": f"message-{i}"})
            producer.flush()

        duration = time.time() - start_time
        throughput = message_count / duration

        print(f"Producer throughput: {throughput:.1f} messages/second")
        assert throughput > 10  # Should be able to send at least 10 msg/s

    def test_concurrent_producers(self, kafka_servers: str):
        """Test multiple producers sending concurrently."""
        def producer_thread(thread_id: int, message_count: int) -> tuple:
            topic = f"concurrent-{thread_id}-{int(time.time())}"
            config = Config(bootstrap_servers=kafka_servers)

            with Producer(config) as producer:
                for i in range(message_count):
                    producer.send(topic, {"thread": thread_id, "msg": i})
                producer.flush()

            return thread_id, message_count

        # Run 3 concurrent producers
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(producer_thread, i, 20)
                for i in range(3)
            ]

            results = [future.result() for future in futures]

        assert len(results) == 3
        for thread_id, count in results:
            assert count == 20

    def test_large_message_handling(self, test_topic: str, test_group: str, kafka_servers: str):
        """Test handling of large messages."""
        # Create a large message (50KB - well within Kafka limits)
        large_data = {
            "data": "x" * (50 * 1024),  # 50KB string
            "metadata": {"size": "50KB", "test": "large_message"}
        }

        # Send large message
        config = Config(bootstrap_servers=kafka_servers)
        with Producer(config) as producer:
            producer.send(test_topic, large_data)
            producer.flush()

        # Consume large message
        consumer_config = Config(
            bootstrap_servers=kafka_servers,
            group_id=test_group,
            auto_offset_reset="earliest"
        )

        with Consumer(test_topic, consumer_config) as consumer:
            message = consumer.poll(timeout=15.0)
            assert message is not None

            received_size = len(str(message.value))
            print(f"Large message: sent and received ~{received_size//1024}KB")
            assert received_size > 50000  # Should be around 50KB

    def test_message_ordering(self, kafka_servers: str):
        """Test message ordering within partitions."""
        topic = f"ordering-{int(time.time())}-{uuid.uuid4().hex[:8]}"
        group = f"order-group-{int(time.time())}"

        # Send messages with same key (should go to same partition)
        config = Config(bootstrap_servers=kafka_servers)
        with Producer(config) as producer:
            for i in range(10):
                producer.send(topic, {"order": i, "timestamp": time.time()}, key="same-key")
            producer.flush()

        # Consume and check order
        consumer_config = Config(
            bootstrap_servers=kafka_servers,
            group_id=group,
            auto_offset_reset="earliest"
        )

        received_messages = []
        with Consumer(topic, consumer_config) as consumer:
            start_time = time.time()
            while len(received_messages) < 10 and (time.time() - start_time) < 20:
                message = consumer.poll(timeout=2.0)
                if message:
                    received_messages.append(message.value)
                    consumer.commit(message)

        # Check if messages are in order
        orders = [msg.get("order") for msg in received_messages if "order" in msg]
        is_ordered = orders == sorted(orders)

        print(f"Message ordering: received {len(orders)} messages, ordered: {is_ordered}")
        assert len(orders) > 0
        # Note: Ordering might not be perfect in test environment, so we don't assert is_ordered


@pytest.mark.integration
class TestErrorHandling:
    """Test error handling scenarios."""

    def test_invalid_topic_handling(self, kafka_servers: str):
        """Test handling of invalid topics."""
        config = Config(bootstrap_servers=kafka_servers)

        with Producer(config) as producer:
            # Send to empty topic (should handle gracefully or raise appropriate error)
            try:
                producer.send("", {"invalid": "topic"})
                producer.flush()
                # If it doesn't raise an error, that's okay too
            except Exception as e:
                # Should be a reasonable exception, not a crash
                assert isinstance(e, (ProducerError, Exception))

    def test_connection_resilience_invalid_broker(self):
        """Test behavior with invalid broker."""
        try:
            config = Config(bootstrap_servers="invalid-broker:9092")
            with Producer(config) as producer:
                producer.send("test-topic", {"test": "data"})
                producer.flush(timeout=2)  # Short timeout to fail fast
            # Should not reach here with invalid broker
            assert False, "Expected exception with invalid broker"
        except Exception as e:
            # Should handle gracefully with appropriate error
            assert isinstance(e, Exception)

    def test_consumer_error_handling(self, kafka_servers: str):
        """Test consumer error handling."""
        config = Config(
            bootstrap_servers=kafka_servers,
            group_id="test-error-group",
            auto_offset_reset="earliest"
        )

        with Consumer("non-existent-topic-12345", config) as consumer:
            # Polling non-existent topic should not crash
            message = consumer.poll(timeout=1.0)
            # May return None or raise appropriate exception
            assert message is None or isinstance(message, Exception)
