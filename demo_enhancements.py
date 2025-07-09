#!/usr/bin/env python3
"""
Demo script showcasing all enhancements made to apnamart-kafka-python.

This script demonstrates:
1. Batch operations
2. Performance configuration
3. Parallel processing helpers  
4. Transaction support
5. Configuration presets

Note: This is a demo script and doesn't require a running Kafka instance.
It shows the API usage patterns.
"""

import asyncio
import sys
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

print("🚀 apnamart-kafka-python Enhancement Demo")
print("=" * 50)

def demo_batch_operations():
    """Demonstrate batch operations."""
    print("\n📦 1. Batch Operations Demo")
    print("-" * 30)
    
    # Create producer with performance config
    config = KafkaConfig(
        bootstrap_servers="localhost:9092",
        batch_size=65536,  # 64KB batches
        linger_ms=100,     # Wait 100ms for batching
        compression_type="lz4"
    )
    
    # Prepare batch messages
    messages = [
        {
            "topic": "user_events",
            "value": {"user_id": i, "action": "click", "timestamp": 1234567890 + i},
            "key": f"user_{i}"
        }
        for i in range(10)
    ]
    
    print(f"✓ Prepared {len(messages)} messages for batch sending")
    print("✓ Producer configured with 64KB batches, 100ms linger, LZ4 compression")
    print("✓ Batch operations: send_batch() and send_batch_async() available")

async def demo_async_batch_operations():
    """Demonstrate async batch operations."""
    print("\n⚡ 2. Async Batch Operations Demo")
    print("-" * 35)
    
    config = KafkaConfig(bootstrap_servers="localhost:9092")
    
    messages = [
        {
            "topic": "analytics",
            "value": {"metric": f"metric_{i}", "value": i * 10},
            "key": f"metric_{i}"
        }
        for i in range(5)
    ]
    
    print(f"✓ Prepared {len(messages)} messages for async batch sending")
    print("✓ Async batch operations provide concurrent message sending")
    print("✓ Uses asyncio.gather() for parallel execution")

def demo_config_presets():
    """Demonstrate configuration presets."""
    print("\n⚙️  3. Configuration Presets Demo")
    print("-" * 33)
    
    # High throughput producer
    ht_config = ConfigPresets.producer.high_throughput()
    ht_config.bootstrap_servers = "localhost:9092"
    print(f"✓ High Throughput: batch_size={ht_config.batch_size}, linger_ms={ht_config.linger_ms}")
    
    # Low latency producer
    ll_config = ConfigPresets.producer.low_latency()
    ll_config.bootstrap_servers = "localhost:9092"
    print(f"✓ Low Latency: batch_size={ll_config.batch_size}, linger_ms={ll_config.linger_ms}")
    
    # Reliable producer
    rel_config = ConfigPresets.producer.reliable()
    rel_config.bootstrap_servers = "localhost:9092"
    print(f"✓ Reliable: acks={rel_config.acks}, retries={rel_config.retries}")
    
    # Consumer presets
    batch_consumer = ConfigPresets.consumer.batch_processing()
    print(f"✓ Batch Consumer: max_poll_records={batch_consumer.max_poll_records}")
    
    realtime_consumer = ConfigPresets.consumer.real_time()
    print(f"✓ Real-time Consumer: max_poll_records={realtime_consumer.max_poll_records}")

def demo_parallel_helpers():
    """Demonstrate consumer parallel processing helpers."""
    print("\n🔄 4. Parallel Processing Helpers Demo")
    print("-" * 38)
    
    config = KafkaConsumerConfig(
        bootstrap_servers="localhost:9092",
        group_id="demo-group"
    )
    
    print("✓ consume_batches(): Yield message batches for efficient processing")
    print("✓ consume_parallel(): Built-in parallel message processing with async/await")
    print("✓ process_with_thread_pool(): Thread pool processing for CPU-bound tasks")
    print("✓ Automatic offset management and error handling")

def demo_transactional_support():
    """Demonstrate transactional producer."""
    print("\n💳 5. Transaction Support Demo")
    print("-" * 30)
    
    config = KafkaConfig(bootstrap_servers="localhost:9092")
    
    # Transactional messages
    messages = [
        {"topic": "orders", "value": {"order_id": "001", "amount": 100}},
        {"topic": "inventory", "value": {"product_id": "ABC", "quantity": -1}},
        {"topic": "payments", "value": {"payment_id": "PAY001", "status": "pending"}},
    ]
    
    print("✓ TransactionalProducer with exactly-once semantics")
    print("✓ send_transactional_batch(): Atomic batch operations")
    print("✓ Manual transaction control: begin_transaction(), commit_transaction(), abort_transaction()")
    print("✓ Automatic transaction handling in context managers")

def demo_performance_config():
    """Demonstrate performance configuration."""
    print("\n🚄 6. Performance Configuration Demo")
    print("-" * 36)
    
    # Custom performance config
    config = KafkaConfig(
        bootstrap_servers="localhost:9092",
        batch_size=131072,      # 128KB
        linger_ms=200,          # 200ms
        buffer_memory=134217728,  # 128MB
        compression_type="snappy",
        acks="all",
        retries=5
    )
    
    kafka_config = config.to_kafka_config()
    print(f"✓ Batch Size: {kafka_config['batch_size']} bytes")
    print(f"✓ Linger Time: {kafka_config['linger_ms']} ms")
    print(f"✓ Buffer Memory: {kafka_config['buffer_memory']} bytes")
    print(f"✓ Compression: {kafka_config['compression_type']}")

async def demo_usage_patterns():
    """Demonstrate various usage patterns."""
    print("\n🎯 7. Usage Patterns Demo")
    print("-" * 26)
    
    print("Serial Processing:")
    print("  • Single partition topic")
    print("  • Synchronous send() and consume()")
    print("  • Guaranteed message ordering")
    
    print("\nParallel Processing (Multi-Process):")
    print("  • Multi-partition topic")
    print("  • Multiple consumer instances with same group_id")
    print("  • Kafka automatically distributes partitions")
    
    print("\nParallel Processing (Internal):")
    print("  • consume_parallel() with ThreadPoolExecutor")
    print("  • send_batch_async() with asyncio.gather()")
    print("  • Ideal for I/O-bound workloads")
    
    print("\nBatch Processing:")
    print("  • consume_batches() for efficient batch handling")
    print("  • send_batch() for efficient batch production")
    print("  • Large max_poll_records configuration")
    
    print("\nTransactional Processing:")
    print("  • send_transactional_batch() for atomic operations")
    print("  • Exactly-once semantics")
    print("  • Cross-topic consistency")

def main():
    """Run all demos."""
    demo_batch_operations()
    demo_config_presets()
    demo_parallel_helpers()
    demo_transactional_support()
    demo_performance_config()
    
    # Run async demos
    asyncio.run(demo_async_batch_operations())
    asyncio.run(demo_usage_patterns())
    
    print("\n🎉 Enhancement Demo Complete!")
    print("=" * 50)
    print("\nAll features are now available in apnamart-kafka-python:")
    print("• ✅ Batch Operations (sync & async)")
    print("• ✅ Performance Configuration (batch_size, linger_ms, buffer_memory)")
    print("• ✅ Parallel Processing Helpers (consume_parallel, consume_batches)")
    print("• ✅ Transaction Support (TransactionalProducer)")
    print("• ✅ Configuration Presets (high_throughput, low_latency, reliable)")
    print("• ✅ Enhanced Error Handling & Monitoring")
    
    print("\n📚 Check out the examples/ directory for comprehensive usage examples!")

if __name__ == "__main__":
    main()