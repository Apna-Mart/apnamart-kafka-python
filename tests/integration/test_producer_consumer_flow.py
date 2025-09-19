"""Integration tests for producer-consumer message flow."""

import time
import uuid

import pytest

from apnamart_kafka import Config, Consumer, Producer, consume, send


@pytest.mark.integration
class TestBasicProducerConsumerFlow:
    """Test basic producer and consumer operations with real Kafka."""

    def test_simple_send_receive(self, test_topic: str, kafka_servers: str):
        """Test simple message send and receive."""
        # Send a message
        test_data = {"message": "Hello Kafka!", "timestamp": time.time()}
        send(test_topic, test_data, servers=kafka_servers)

        # Consume the message
        consumer_config = Config(
            bootstrap_servers=kafka_servers,
            group_id=f"simple-test-{uuid.uuid4().hex[:8]}",
            auto_offset_reset="earliest",
        )

        with Consumer(test_topic, consumer_config) as consumer:
            message = consumer.poll(timeout=10.0)
            assert message is not None
            assert message.value["message"] == "Hello Kafka!"
            consumer.commit(message)

    def test_producer_send_different_types(self, producer: Producer, test_topic: str):
        """Test sending different data types."""
        test_cases = [
            "string message",
            {"json": "object", "number": 42},
            b"bytes message",
            42,
            True,
            None,
            ["list", "of", "items"],
        ]

        for i, test_data in enumerate(test_cases):
            producer.send(test_topic, test_data, key=f"type-test-{i}")

        producer.flush()

    def test_producer_send_with_key(self, producer: Producer, test_topic: str):
        """Test sending message with key."""
        message_data = {"test": "with-key", "data": "some value"}
        message_key = "test-key"

        producer.send(test_topic, message_data, key=message_key)
        producer.flush()

    def test_consumer_poll_timeout(self, test_topic: str, kafka_servers: str):
        """Test consumer poll timeout behavior."""
        # Create the topic first
        send(test_topic, {"setup": "message"}, servers=kafka_servers)
        time.sleep(0.1)

        # Use latest offset so we don't read the setup message
        config = Config(
            bootstrap_servers=kafka_servers,
            group_id=f"timeout-test-{uuid.uuid4().hex[:8]}",
            auto_offset_reset="latest",
        )

        with Consumer(test_topic, config) as consumer:
            # Poll with short timeout should return None
            message = consumer.poll(timeout=0.1)
            assert message is None

    def test_consumer_iterator_interface(self, test_topic: str, kafka_servers: str):
        """Test consumer iterator interface."""
        # Send multiple messages
        producer_config = Config(bootstrap_servers=kafka_servers)
        with Producer(producer_config) as producer:
            for i in range(5):
                producer.send(test_topic, {"iter": i, "message": f"test-{i}"})
            producer.flush()

        # Consume using iterator
        consumer_config = Config(
            bootstrap_servers=kafka_servers,
            group_id=f"iter-test-{uuid.uuid4().hex[:8]}",
            auto_offset_reset="earliest",
        )

        messages = []
        with Consumer(test_topic, consumer_config) as consumer:
            for message in consumer:
                messages.append(message)
                consumer.commit(message)
                if len(messages) >= 5:
                    break

        assert len(messages) == 5
        for i, msg in enumerate(messages):
            assert msg.value["iter"] == i

    def test_consumer_batch_polling(self, test_topic: str, kafka_servers: str):
        """Test consumer batch polling."""
        # Send batch of messages
        producer_config = Config(bootstrap_servers=kafka_servers)
        with Producer(producer_config) as producer:
            for i in range(10):
                producer.send(test_topic, {"batch": i})
            producer.flush()

        # Consume batch
        consumer_config = Config(
            bootstrap_servers=kafka_servers,
            group_id=f"batch-test-{uuid.uuid4().hex[:8]}",
            auto_offset_reset="earliest",
        )

        with Consumer(test_topic, consumer_config) as consumer:
            batch = consumer.poll_batch(size=10, timeout=10.0)
            assert len(batch) > 0

            for msg in batch:
                consumer.commit(msg)


@pytest.mark.integration
class TestBatchOperations:
    """Test batch producer operations."""

    def test_producer_batch_send_dict_format(self, producer: Producer, test_topic: str):
        """Test batch sending with dict format."""
        messages = [
            {"topic": test_topic, "value": {"batch": i, "data": f"message-{i}"}}
            for i in range(5)
        ]

        results = producer.send_batch(messages)

        assert len(results) == 5
        for result in results:
            assert result["success"] is True

    def test_producer_batch_send_tuple_format(
        self, producer: Producer, test_topic: str
    ):
        """Test batch sending with tuple format."""
        messages = [
            (test_topic, {"tuple_test": i, "data": f"message-{i}"}) for i in range(3)
        ]

        results = producer.send_batch(messages)

        assert len(results) == 3
        for result in results:
            assert result["success"] is True

    def test_producer_batch_send_with_keys(self, producer: Producer, test_topic: str):
        """Test batch sending with message keys."""
        messages = [(test_topic, {"key_test": i}, f"key-{i}") for i in range(3)]

        results = producer.send_batch(messages)

        assert len(results) == 3
        for result in results:
            assert result["success"] is True

    def test_producer_batch_send_invalid_messages(self, producer: Producer):
        """Test batch sending with invalid messages."""
        messages = [
            {"topic": "valid-topic", "value": {"valid": "message"}},
            {"value": {"missing": "topic"}},  # Missing topic
            {"topic": "missing-value"},  # Missing value
            "invalid_string",  # Invalid format
            123,  # Invalid number
        ]

        results = producer.send_batch(messages)

        assert len(results) == 5
        assert results[0]["success"] is True  # Valid message
        assert results[1]["success"] is False  # Missing topic
        assert results[2]["success"] is False  # Missing value
        assert results[3]["success"] is False  # Invalid string
        assert results[4]["success"] is False  # Invalid number


@pytest.mark.integration
class TestEndToEndFlow:
    """Test complete end-to-end message flow."""

    def test_complete_producer_consumer_flow(self, test_topic: str, kafka_servers: str):
        """Test complete producer -> consumer flow."""
        test_data = [
            {"message": "Hello World", "id": 1},
            {"message": "Second Message", "id": 2},
            "Simple string message",
            {"complex": {"nested": {"data": "test"}}, "timestamp": time.time()},
        ]

        # Produce messages
        producer_config = Config(bootstrap_servers=kafka_servers)
        with Producer(producer_config) as producer:
            for i, data in enumerate(test_data):
                producer.send(test_topic, data, key=f"key-{i}")
            producer.flush()

        # Consume messages
        consumer_config = Config(
            bootstrap_servers=kafka_servers,
            group_id=f"e2e-test-{uuid.uuid4().hex[:8]}",
            auto_offset_reset="earliest",
        )

        received_data = []
        with Consumer(test_topic, consumer_config) as consumer:
            start_time = time.time()
            while (
                len(received_data) < len(test_data) and (time.time() - start_time) < 15
            ):
                message = consumer.poll(timeout=2.0)
                if message:
                    received_data.append(message.value)
                    consumer.commit(message)

        assert len(received_data) == len(test_data)

    def test_convenience_functions(self, test_topic: str, kafka_servers: str):
        """Test convenience send and consume functions."""
        # Send using convenience function
        test_data = {"convenience": "test", "timestamp": time.time()}
        send(test_topic, test_data, servers=kafka_servers)

        # Consume using convenience function
        messages_consumed = []
        start_time = time.time()

        for message in consume(
            test_topic,
            servers=kafka_servers,
            group_id=f"convenience-{uuid.uuid4().hex[:8]}",
            auto_offset_reset="earliest",
        ):
            messages_consumed.append(message.value)
            if len(messages_consumed) >= 1 or (time.time() - start_time) > 10:
                break

        assert len(messages_consumed) >= 1
        assert messages_consumed[0]["convenience"] == "test"


if __name__ == "__main__":
    print(" Producer-Consumer Flow Integration Tests")
    print("=" * 50)
    print("These tests validate basic message flow with real Kafka.")
    print("Run with: uv run pytest tests/integration/test_producer_consumer_flow.py -v")
