#!/usr/bin/env python3
"""
Test script to validate all enhancements made to the apnamart-kafka-python library.

This script tests:
1. Batch operations
2. Performance configuration
3. Parallel processing helpers
4. Transaction support
5. Configuration presets
"""

import asyncio
import logging
import sys
import time
from typing import List

# Add the package to Python path
sys.path.insert(0, '.')

from apnamart_kafka import (
    KafkaProducer,
    KafkaConsumer,
    TransactionalProducer,
    KafkaConfig,
    KafkaConsumerConfig,
    ConsumerMessage,
    ConfigPresets,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Test configuration
BOOTSTRAP_SERVERS = "localhost:9092"
TEST_TOPIC = "test_enhancements"


class TestEnhancements:
    """Test all library enhancements."""

    def __init__(self):
        self.passed_tests = []
        self.failed_tests = []

    def test_batch_operations(self):
        """Test batch send operations."""
        logger.info("\n=== Testing Batch Operations ===")
        
        try:
            config = KafkaConfig(bootstrap_servers=BOOTSTRAP_SERVERS)
            
            # Prepare test messages
            messages = [
                {
                    "topic": TEST_TOPIC,
                    "value": {"test": "batch", "msg_id": i},
                    "key": f"batch-key-{i}"
                }
                for i in range(5)
            ]
            
            with KafkaProducer(config) as producer:
                # Test sync batch send
                logger.info("Testing sync batch send...")
                futures = producer.send_batch(messages)
                
                # Verify all futures
                success_count = 0
                for future in futures:
                    if future:
                        try:
                            future.get(timeout=5)
                            success_count += 1
                        except Exception as e:
                            logger.error(f"Batch message failed: {e}")
                
                assert success_count == len(messages), f"Expected {len(messages)} successful sends, got {success_count}"
                logger.info(f"✓ Sync batch send successful: {success_count}/{len(messages)} messages")
            
            self.passed_tests.append("batch_operations")
            
        except Exception as e:
            logger.error(f"✗ Batch operations test failed: {e}")
            self.failed_tests.append(("batch_operations", str(e)))

    async def test_async_batch_operations(self):
        """Test async batch send operations."""
        logger.info("\n=== Testing Async Batch Operations ===")
        
        try:
            config = KafkaConfig(bootstrap_servers=BOOTSTRAP_SERVERS)
            
            # Prepare test messages
            messages = [
                {
                    "topic": TEST_TOPIC,
                    "value": {"test": "async_batch", "msg_id": i},
                    "key": f"async-batch-{i}"
                }
                for i in range(5)
            ]
            
            async with KafkaProducer(config) as producer:
                logger.info("Testing async batch send...")
                results = await producer.send_batch_async(messages)
                
                # Check results
                success_count = sum(1 for r in results if not isinstance(r, Exception))
                assert success_count == len(messages), f"Expected {len(messages)} successful sends, got {success_count}"
                logger.info(f"✓ Async batch send successful: {success_count}/{len(messages)} messages")
            
            self.passed_tests.append("async_batch_operations")
            
        except Exception as e:
            logger.error(f"✗ Async batch operations test failed: {e}")
            self.failed_tests.append(("async_batch_operations", str(e)))

    def test_performance_config(self):
        """Test performance configuration parameters."""
        logger.info("\n=== Testing Performance Configuration ===")
        
        try:
            # Test custom performance parameters
            config = KafkaConfig(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                batch_size=32768,  # 32KB
                linger_ms=50,
                buffer_memory=67108864,  # 64MB
                compression_type="lz4"
            )
            
            # Verify configuration
            kafka_config = config.to_kafka_config()
            assert kafka_config["batch_size"] == 32768
            assert kafka_config["linger_ms"] == 50
            assert kafka_config["buffer_memory"] == 67108864
            assert kafka_config["compression_type"] == "lz4"
            
            logger.info("✓ Performance configuration parameters working correctly")
            
            # Test with actual producer
            with KafkaProducer(config) as producer:
                producer.send(TEST_TOPIC, value={"test": "performance_config"})
                producer.flush()
            
            logger.info("✓ Producer with performance config working")
            self.passed_tests.append("performance_config")
            
        except Exception as e:
            logger.error(f"✗ Performance config test failed: {e}")
            self.failed_tests.append(("performance_config", str(e)))

    def test_config_presets(self):
        """Test configuration presets."""
        logger.info("\n=== Testing Configuration Presets ===")
        
        try:
            # Test high throughput preset
            ht_config = ConfigPresets.producer.high_throughput()
            ht_config.bootstrap_servers = BOOTSTRAP_SERVERS
            
            assert ht_config.batch_size == 65536
            assert ht_config.linger_ms == 100
            assert ht_config.compression_type == "lz4"
            logger.info("✓ High throughput preset configured correctly")
            
            # Test low latency preset
            ll_config = ConfigPresets.producer.low_latency()
            ll_config.bootstrap_servers = BOOTSTRAP_SERVERS
            
            assert ll_config.batch_size == 0
            assert ll_config.linger_ms == 0
            assert ll_config.compression_type is None
            logger.info("✓ Low latency preset configured correctly")
            
            # Test reliable preset
            rel_config = ConfigPresets.producer.reliable()
            rel_config.bootstrap_servers = BOOTSTRAP_SERVERS
            
            assert rel_config.acks == "all"
            assert rel_config.retries == 10
            logger.info("✓ Reliable preset configured correctly")
            
            # Test consumer presets
            batch_config = ConfigPresets.consumer.batch_processing()
            assert batch_config.max_poll_records == 1000
            assert batch_config.enable_auto_commit is False
            logger.info("✓ Batch processing consumer preset configured correctly")
            
            self.passed_tests.append("config_presets")
            
        except Exception as e:
            logger.error(f"✗ Config presets test failed: {e}")
            self.failed_tests.append(("config_presets", str(e)))

    async def test_parallel_helpers(self):
        """Test consumer parallel processing helpers."""
        logger.info("\n=== Testing Parallel Processing Helpers ===")
        
        try:
            # First, produce some test messages
            config = KafkaConfig(bootstrap_servers=BOOTSTRAP_SERVERS)
            with KafkaProducer(config) as producer:
                for i in range(10):
                    producer.send(TEST_TOPIC, value={"test": "parallel", "id": i})
                producer.flush()
            
            # Test consume_batches
            consumer_config = KafkaConsumerConfig(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                group_id="test-parallel-helpers",
                auto_offset_reset="earliest"
            )
            
            with KafkaConsumer(consumer_config) as consumer:
                consumer.subscribe([TEST_TOPIC])
                
                logger.info("Testing consume_batches...")
                batch_count = 0
                message_count = 0
                
                for batch in consumer.consume_batches(batch_size=5, timeout_ms=2000):
                    batch_count += 1
                    message_count += len(batch)
                    logger.info(f"Received batch {batch_count} with {len(batch)} messages")
                    
                    if batch_count >= 2:  # Process 2 batches
                        break
                
                assert batch_count >= 1, "Should have received at least 1 batch"
                logger.info(f"✓ consume_batches working: {batch_count} batches, {message_count} messages")
            
            self.passed_tests.append("parallel_helpers")
            
        except Exception as e:
            logger.error(f"✗ Parallel helpers test failed: {e}")
            self.failed_tests.append(("parallel_helpers", str(e)))

    def test_transaction_support(self):
        """Test transactional producer."""
        logger.info("\n=== Testing Transaction Support ===")
        
        try:
            config = KafkaConfig(bootstrap_servers=BOOTSTRAP_SERVERS)
            
            # Test transactional batch
            messages = [
                {
                    "topic": TEST_TOPIC,
                    "value": {"test": "transaction", "tx_msg": i},
                    "key": f"tx-{i}"
                }
                for i in range(3)
            ]
            
            with TransactionalProducer(
                config,
                transactional_id="test-tx-producer-001"
            ) as producer:
                logger.info("Testing transactional batch send...")
                producer.send_transactional_batch(messages)
                logger.info("✓ Transactional batch sent successfully")
            
            # Test manual transaction control
            with TransactionalProducer(
                config,
                transactional_id="test-tx-producer-002"
            ) as producer:
                logger.info("Testing manual transaction control...")
                
                producer.begin_transaction()
                producer.send(TEST_TOPIC, value={"test": "manual_tx", "msg": 1})
                producer.send(TEST_TOPIC, value={"test": "manual_tx", "msg": 2})
                producer.commit_transaction()
                
                logger.info("✓ Manual transaction control working")
            
            self.passed_tests.append("transaction_support")
            
        except Exception as e:
            logger.error(f"✗ Transaction support test failed: {e}")
            self.failed_tests.append(("transaction_support", str(e)))

    def print_summary(self):
        """Print test summary."""
        total_tests = len(self.passed_tests) + len(self.failed_tests)
        
        logger.info("\n" + "="*50)
        logger.info("TEST SUMMARY")
        logger.info("="*50)
        
        logger.info(f"Total tests: {total_tests}")
        logger.info(f"Passed: {len(self.passed_tests)}")
        logger.info(f"Failed: {len(self.failed_tests)}")
        
        if self.passed_tests:
            logger.info("\nPassed tests:")
            for test in self.passed_tests:
                logger.info(f"  ✓ {test}")
        
        if self.failed_tests:
            logger.info("\nFailed tests:")
            for test, error in self.failed_tests:
                logger.info(f"  ✗ {test}: {error}")
        
        logger.info("\n" + "="*50)
        
        return len(self.failed_tests) == 0


async def main():
    """Run all tests."""
    tester = TestEnhancements()
    
    # Run sync tests
    tester.test_batch_operations()
    tester.test_performance_config()
    tester.test_config_presets()
    tester.test_transaction_support()
    
    # Run async tests
    await tester.test_async_batch_operations()
    await tester.test_parallel_helpers()
    
    # Print summary
    success = tester.print_summary()
    
    if success:
        logger.info("\n✅ All enhancements tested successfully!")
        return 0
    else:
        logger.error("\n❌ Some tests failed!")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)