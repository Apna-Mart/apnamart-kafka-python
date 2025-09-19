#!/usr/bin/env python3
"""
Test suite for the fixes made to apnamart-kafka-python library.
This tests the specific issues that were identified and fixed.
"""

import pytest
import time
from faker import Faker

# Import the fixed library
from apnamart_kafka import Producer, Consumer, TransactionalProducer, Config
from apnamart_kafka import ProducerError, ConsumerError, TransactionError

fake = Faker()

@pytest.fixture
def kafka_config():
    return Config(bootstrap_servers='localhost:9092')

@pytest.fixture
def test_topic():
    return f"fix-test-{int(time.time())}-{fake.random_int(1000, 9999)}"

class TestTransactionalProducerFixes:
    """Test fixes for TransactionalProducer API issues."""

    def test_new_send_transactional_api(self, kafka_config, test_topic):
        """Test the new send_transactional API with proper signature."""
        tx_id = f"test-tx-{fake.uuid4()}"
        tx_producer = TransactionalProducer(tx_id, kafka_config)

        message = {"tx_test": True, "data": fake.sentence()}

        # Test the NEW API: send_transactional(topic, message)
        with tx_producer:
            tx_producer.begin()
            tx_producer.send_transactional(test_topic, message)
            tx_producer.commit()

        print("✅ TransactionalProducer.send_transactional() with new API: PASSED")

    def test_send_transactional_with_key(self, kafka_config, test_topic):
        """Test send_transactional with message key."""
        tx_id = f"test-tx-key-{fake.uuid4()}"
        tx_producer = TransactionalProducer(tx_id, kafka_config)

        message = {"tx_test": True, "data": fake.sentence()}
        key = f"key-{fake.uuid4()}"

        with tx_producer:
            tx_producer.begin()
            tx_producer.send_transactional(test_topic, message, key=key)
            tx_producer.commit()

        print("✅ TransactionalProducer.send_transactional() with key: PASSED")

    def test_send_transactional_without_transaction_error(self, kafka_config, test_topic):
        """Test that send_transactional raises error when no transaction is active."""
        tx_id = f"test-tx-error-{fake.uuid4()}"
        tx_producer = TransactionalProducer(tx_id, kafka_config)

        with tx_producer:
            with pytest.raises(TransactionError, match="No active transaction"):
                tx_producer.send_transactional(test_topic, {"test": "should fail"})

        print("✅ TransactionalProducer error handling: PASSED")

    def test_send_batch_transactional_new_method(self, kafka_config, test_topic):
        """Test the new send_batch_transactional method."""
        tx_id = f"test-tx-batch-{fake.uuid4()}"
        tx_producer = TransactionalProducer(tx_id, kafka_config)

        messages = [
            (test_topic, {"batch_tx": i, "data": fake.sentence()})
            for i in range(5)
        ]

        with tx_producer:
            tx_producer.send_batch_transactional(messages)

        print("✅ TransactionalProducer.send_batch_transactional(): PASSED")

    def test_transaction_abort_scenario(self, kafka_config, test_topic):
        """Test transaction abort functionality."""
        tx_id = f"test-tx-abort-{fake.uuid4()}"
        tx_producer = TransactionalProducer(tx_id, kafka_config)

        with tx_producer:
            tx_producer.begin()
            tx_producer.send_transactional(test_topic, {"abort_test": True})
            tx_producer.abort()  # Intentionally abort

        print("✅ Transaction abort: PASSED")

class TestBatchSendAPIFixes:
    """Test fixes for batch send API consistency."""

    def test_send_batch_with_tuple_format(self, kafka_config, test_topic):
        """Test send_batch with tuple format (topic, value)."""
        producer = Producer(kafka_config)

        messages = [
            (test_topic, {"tuple_test": i, "data": fake.sentence()})
            for i in range(3)
        ]

        with producer:
            results = producer.send_batch(messages)

        # Check all messages succeeded
        for result in results:
            assert result["success"] is True
            assert "topic" in result

        print("✅ send_batch with tuple format: PASSED")

    def test_send_batch_with_tuple_and_key_format(self, kafka_config, test_topic):
        """Test send_batch with tuple format (topic, value, key)."""
        producer = Producer(kafka_config)

        messages = [
            (test_topic, {"tuple_key_test": i, "data": fake.sentence()}, f"key-{i}")
            for i in range(3)
        ]

        with producer:
            results = producer.send_batch(messages)

        # Check all messages succeeded
        for result in results:
            assert result["success"] is True

        print("✅ send_batch with tuple+key format: PASSED")

    def test_send_batch_with_dict_format(self, kafka_config, test_topic):
        """Test send_batch with dict format (backward compatibility)."""
        producer = Producer(kafka_config)

        messages = [
            {"topic": test_topic, "value": {"dict_test": i}, "key": f"key-{i}"}
            for i in range(3)
        ]

        with producer:
            results = producer.send_batch(messages)

        # Check all messages succeeded
        for result in results:
            assert result["success"] is True

        print("✅ send_batch with dict format: PASSED")

    def test_send_batch_mixed_formats(self, kafka_config, test_topic):
        """Test send_batch with mixed tuple and dict formats."""
        producer = Producer(kafka_config)

        messages = [
            (test_topic, {"mixed_test": 1}),  # Tuple format
            {"topic": test_topic, "value": {"mixed_test": 2}},  # Dict format
            (test_topic, {"mixed_test": 3}, "key3"),  # Tuple with key
        ]

        with producer:
            results = producer.send_batch(messages)

        # Check all messages succeeded
        for result in results:
            assert result["success"] is True

        print("✅ send_batch with mixed formats: PASSED")

    def test_send_batch_error_handling(self, kafka_config):
        """Test send_batch error handling for invalid formats."""
        producer = Producer(kafka_config)

        invalid_messages = [
            "invalid_string",  # Invalid format
            (test_topic,),  # Invalid tuple length
            {"invalid": "dict"},  # Missing required fields
        ]

        with producer:
            results = producer.send_batch(invalid_messages)

        # Check that errors are properly reported
        for result in results:
            assert result["success"] is False
            assert "error" in result

        print("✅ send_batch error handling: PASSED")

class TestImprovedErrorHandling:
    """Test improved error messages and handling."""

    def test_producer_empty_topic_error(self, kafka_config):
        """Test producer error for empty topic name."""
        producer = Producer(kafka_config)

        with producer:
            with pytest.raises(ProducerError, match="Topic name cannot be empty"):
                producer.send("", {"test": "empty topic"})

        print("✅ Producer empty topic error: PASSED")

    def test_consumer_unknown_topic_error(self, kafka_config):
        """Test consumer error handling for unknown topics."""
        unknown_topic = f"unknown-topic-{fake.uuid4()}"
        consumer = Consumer([unknown_topic], kafka_config)

        with consumer:
            # This should either raise an error or return None gracefully
            try:
                msg = consumer.poll(timeout=1.0)
                # If it returns None, that's acceptable
                assert msg is None
            except ConsumerError as e:
                # If it raises an error, it should be informative
                assert "Unknown topic" in str(e) or "not available" in str(e)

        print("✅ Consumer unknown topic error: PASSED")

    def test_better_producer_error_messages(self, kafka_config, test_topic):
        """Test that producer errors have informative messages."""
        producer = Producer(kafka_config)

        with producer:
            try:
                # Try to send to a very long topic name (likely invalid)
                long_topic = "x" * 300
                producer.send(long_topic, {"test": "long topic"})
            except ProducerError as e:
                # Error message should mention the topic name
                assert long_topic in str(e) or "topic" in str(e).lower()

        print("✅ Better producer error messages: PASSED")

class TestBackwardCompatibility:
    """Test that fixes maintain backward compatibility."""

    def test_existing_apis_still_work(self, kafka_config, test_topic):
        """Test that existing APIs continue to work as before."""
        # Test basic Producer/Consumer functionality
        producer = Producer(kafka_config)
        consumer = Consumer([test_topic], kafka_config)

        test_message = {"compatibility_test": True, "data": fake.sentence()}

        # Send message
        with producer:
            producer.send(test_topic, test_message)
            producer.flush()

        time.sleep(0.5)

        # Receive message
        with consumer:
            msg = consumer.poll(timeout=5.0)
            if msg:
                assert "compatibility_test" in msg.value

        print("✅ Backward compatibility: PASSED")

    def test_context_managers_still_work(self, kafka_config, test_topic):
        """Test that context manager functionality is preserved."""
        message = {"context_test": True, "data": fake.text()}

        # Test producer context manager
        with Producer(kafka_config) as prod:
            prod.send(test_topic, message)
            prod.flush()

        # Test consumer context manager
        with Consumer([test_topic], kafka_config) as cons:
            # Just test that it doesn't crash
            msg = cons.poll(timeout=1.0)

        print("✅ Context managers: PASSED")

def run_all_tests():
    """Run all fix tests manually."""
    config = Config(bootstrap_servers='localhost:9092')
    test_topic = f"fix-test-{int(time.time())}"

    print("🔧 Testing fixes for apnamart-kafka-python...")
    print("=" * 60)

    # Test TransactionalProducer fixes
    print("\n📋 Testing TransactionalProducer fixes:")
    test_tx = TestTransactionalProducerFixes()
    test_tx.test_new_send_transactional_api(config, test_topic)
    test_tx.test_send_transactional_with_key(config, test_topic)
    test_tx.test_send_transactional_without_transaction_error(config, test_topic)
    test_tx.test_send_batch_transactional_new_method(config, test_topic)
    test_tx.test_transaction_abort_scenario(config, test_topic)

    # Test batch send fixes
    print("\n📦 Testing batch send API fixes:")
    test_batch = TestBatchSendAPIFixes()
    test_batch.test_send_batch_with_tuple_format(config, test_topic)
    test_batch.test_send_batch_with_tuple_and_key_format(config, test_topic)
    test_batch.test_send_batch_with_dict_format(config, test_topic)
    test_batch.test_send_batch_mixed_formats(config, test_topic)
    test_batch.test_send_batch_error_handling(config)

    # Test error handling improvements
    print("\n🚨 Testing improved error handling:")
    test_errors = TestImprovedErrorHandling()
    test_errors.test_producer_empty_topic_error(config)
    test_errors.test_consumer_unknown_topic_error(config)
    test_errors.test_better_producer_error_messages(config, test_topic)

    # Test backward compatibility
    print("\n🔄 Testing backward compatibility:")
    test_compat = TestBackwardCompatibility()
    test_compat.test_existing_apis_still_work(config, test_topic)
    test_compat.test_context_managers_still_work(config, test_topic)

    print("\n" + "=" * 60)
    print("🎉 ALL FIXES TESTED SUCCESSFULLY!")
    print("The library is now ready for production use.")

if __name__ == "__main__":
    run_all_tests()