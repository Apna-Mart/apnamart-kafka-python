"""
Comprehensive demonstration of all serial and parallel processing patterns
with the enhanced apnamart-kafka-python library.

This script demonstrates:
1. Serial Processing
2. Parallel Processing (Multi-Process)
3. Parallel Processing (Internal/Thread Pool)
4. Batch Operations
5. Async Operations
6. Transactional Operations
7. Configuration Presets
"""

import argparse
import asyncio
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any
from uuid import uuid4

from apnamart_kafka import (
    KafkaConsumer,
    KafkaProducer,
    TransactionalProducer,
    KafkaConfig,
    KafkaConsumerConfig,
    ConsumerMessage,
    ConfigPresets,
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Configuration
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SERIAL_TOPIC = "serial_processing_demo"
PARALLEL_TOPIC = "parallel_processing_demo"
BATCH_TOPIC = "batch_processing_demo"
TRANSACTION_TOPIC = "transaction_demo"


class DemoProducer:
    """Demonstrates various producer patterns."""

    def __init__(self):
        self.config = KafkaConfig(bootstrap_servers=BOOTSTRAP_SERVERS)

    def demo_serial_send(self):
        """Demonstrate serial message sending."""
        logger.info("=== SERIAL SEND DEMO ===")
        
        with KafkaProducer(self.config) as producer:
            for i in range(5):
                start = time.time()
                producer.send(
                    SERIAL_TOPIC,
                    value={"sequence": i, "type": "serial"},
                    key=f"serial-{i}"
                )
                elapsed = time.time() - start
                logger.info(f"Sent serial message {i} in {elapsed:.3f}s")

    async def demo_async_parallel_send(self):
        """Demonstrate async parallel message sending."""
        logger.info("=== ASYNC PARALLEL SEND DEMO ===")
        
        async with KafkaProducer(self.config) as producer:
            start = time.time()
            
            # Send 10 messages in parallel
            tasks = []
            for i in range(10):
                task = producer.send_async(
                    PARALLEL_TOPIC,
                    value={"sequence": i, "type": "async_parallel"},
                    key=f"async-{i}"
                )
                tasks.append(task)
            
            # Wait for all to complete
            await asyncio.gather(*tasks)
            
            elapsed = time.time() - start
            logger.info(f"Sent 10 messages in parallel in {elapsed:.3f}s")

    def demo_batch_send(self):
        """Demonstrate batch sending."""
        logger.info("=== BATCH SEND DEMO ===")
        
        # Prepare batch of messages
        messages = [
            {
                "topic": BATCH_TOPIC,
                "value": {"batch_id": 1, "msg_id": i},
                "key": f"batch-{i}"
            }
            for i in range(20)
        ]
        
        with KafkaProducer(self.config) as producer:
            start = time.time()
            futures = producer.send_batch(messages)
            
            # Wait for all messages
            for i, future in enumerate(futures):
                if future:
                    try:
                        future.get(timeout=10)
                    except Exception as e:
                        logger.error(f"Failed to send message {i}: {e}")
            
            elapsed = time.time() - start
            logger.info(f"Sent batch of {len(messages)} messages in {elapsed:.3f}s")

    def demo_transaction_send(self):
        """Demonstrate transactional sending."""
        logger.info("=== TRANSACTION SEND DEMO ===")
        
        messages = [
            {
                "topic": TRANSACTION_TOPIC,
                "value": {"tx_id": "tx-001", "msg_id": i},
                "key": f"tx-{i}"
            }
            for i in range(5)
        ]
        
        with TransactionalProducer(
            self.config,
            transactional_id=f"demo-tx-producer-{uuid4()}"
        ) as producer:
            start = time.time()
            
            # Send all messages in a transaction
            producer.send_transactional_batch(messages)
            
            elapsed = time.time() - start
            logger.info(f"Sent transactional batch of {len(messages)} messages in {elapsed:.3f}s")

    def demo_high_throughput_config(self):
        """Demonstrate high throughput configuration preset."""
        logger.info("=== HIGH THROUGHPUT CONFIG DEMO ===")
        
        # Use high throughput preset
        config = ConfigPresets.producer.high_throughput()
        config.bootstrap_servers = BOOTSTRAP_SERVERS
        
        with KafkaProducer(config) as producer:
            start = time.time()
            
            # Send many messages
            for i in range(100):
                producer.send(
                    PARALLEL_TOPIC,
                    value={"msg_id": i, "type": "high_throughput"},
                    key=f"ht-{i}"
                )
            
            # Flush to ensure all sent
            producer.flush()
            
            elapsed = time.time() - start
            logger.info(f"Sent 100 messages with high throughput config in {elapsed:.3f}s")


class DemoConsumer:
    """Demonstrates various consumer patterns."""

    def __init__(self):
        self.config = KafkaConsumerConfig(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id="demo-consumer-group",
            auto_offset_reset="earliest"
        )

    def demo_serial_consume(self):
        """Demonstrate serial message consumption."""
        logger.info("=== SERIAL CONSUME DEMO ===")
        
        with KafkaConsumer(self.config) as consumer:
            consumer.subscribe([SERIAL_TOPIC])
            
            count = 0
            start = time.time()
            
            # Consume messages serially
            for message in consumer.consume():
                logger.info(f"Consumed serial message: {message.value}")
                count += 1
                
                # Simulate processing
                time.sleep(0.1)
                
                if count >= 5:
                    break
            
            elapsed = time.time() - start
            logger.info(f"Consumed {count} messages serially in {elapsed:.3f}s")

    async def demo_async_consume(self):
        """Demonstrate async message consumption."""
        logger.info("=== ASYNC CONSUME DEMO ===")
        
        async with KafkaConsumer(self.config) as consumer:
            consumer.subscribe([PARALLEL_TOPIC])
            
            count = 0
            start = time.time()
            
            async for message in consumer.consume_async():
                logger.info(f"Consumed async message: {message.value}")
                count += 1
                
                # Simulate async processing
                await asyncio.sleep(0.05)
                
                if count >= 10:
                    break
            
            elapsed = time.time() - start
            logger.info(f"Consumed {count} messages asynchronously in {elapsed:.3f}s")

    def demo_batch_consume(self):
        """Demonstrate batch consumption."""
        logger.info("=== BATCH CONSUME DEMO ===")
        
        with KafkaConsumer(self.config) as consumer:
            consumer.subscribe([BATCH_TOPIC])
            
            total_messages = 0
            start = time.time()
            
            # Consume in batches
            for batch in consumer.consume_batches(batch_size=10, timeout_ms=3000):
                logger.info(f"Received batch of {len(batch)} messages")
                
                # Process batch
                for msg in batch:
                    logger.debug(f"Processing: {msg.value}")
                
                total_messages += len(batch)
                
                if total_messages >= 20:
                    break
            
            elapsed = time.time() - start
            logger.info(f"Consumed {total_messages} messages in batches in {elapsed:.3f}s")

    async def demo_parallel_consume(self):
        """Demonstrate built-in parallel consumption."""
        logger.info("=== PARALLEL CONSUME DEMO ===")
        
        async def process_message(msg: ConsumerMessage):
            """Simulate async message processing."""
            logger.info(f"Processing in parallel: {msg.value}")
            await asyncio.sleep(0.1)  # Simulate work
            return f"Processed: {msg.key}"
        
        async with KafkaConsumer(self.config) as consumer:
            consumer.subscribe([PARALLEL_TOPIC])
            
            # Use built-in parallel processing
            await asyncio.wait_for(
                consumer.consume_parallel(
                    handler=process_message,
                    max_workers=5,
                    batch_size=10,
                    commit_interval=2
                ),
                timeout=10  # Run for 10 seconds
            )

    def demo_thread_pool_consume(self):
        """Demonstrate thread pool consumption."""
        logger.info("=== THREAD POOL CONSUME DEMO ===")
        
        def process_message(msg: ConsumerMessage):
            """Simulate message processing."""
            logger.info(f"Processing in thread: {msg.value}")
            time.sleep(0.1)  # Simulate work
        
        # Use real-time preset for low latency
        config = ConfigPresets.consumer.real_time(bootstrap_servers=BOOTSTRAP_SERVERS)
        config.group_id = "thread-pool-demo"
        
        with KafkaConsumer(config) as consumer:
            consumer.subscribe([PARALLEL_TOPIC])
            
            # Process with thread pool for 5 seconds
            start = time.time()
            try:
                while time.time() - start < 5:
                    consumer.process_with_thread_pool(
                        handler=process_message,
                        max_workers=4,
                        batch_size=5,
                        timeout_ms=1000
                    )
            except Exception as e:
                logger.info(f"Thread pool processing stopped: {e}")


class MultiConsumerDemo:
    """Demonstrates multiple consumers working in parallel."""
    
    @staticmethod
    async def run_consumer_instance(instance_id: int, duration: int = 10):
        """Run a single consumer instance."""
        config = KafkaConsumerConfig(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id="multi-consumer-demo",
            group_instance_id=f"consumer-{instance_id}",
            auto_offset_reset="earliest"
        )
        
        async with KafkaConsumer(config) as consumer:
            consumer.subscribe([PARALLEL_TOPIC])
            
            count = 0
            start = time.time()
            
            async for message in consumer.consume_async():
                logger.info(f"[Consumer-{instance_id}] Consumed: {message.value}")
                count += 1
                
                if time.time() - start > duration:
                    break
            
            logger.info(f"[Consumer-{instance_id}] Processed {count} messages")
    
    @staticmethod
    async def demo_multi_consumer():
        """Demonstrate multiple consumers in parallel."""
        logger.info("=== MULTI-CONSUMER PARALLEL DEMO ===")
        
        # Run 3 consumers in parallel
        tasks = [
            MultiConsumerDemo.run_consumer_instance(i, duration=5)
            for i in range(3)
        ]
        
        await asyncio.gather(*tasks)


async def main():
    """Main demonstration function."""
    parser = argparse.ArgumentParser(description="Comprehensive Kafka Patterns Demo")
    parser.add_argument(
        "--pattern",
        choices=[
            "all",
            "serial-produce", "async-produce", "batch-produce", "transaction-produce",
            "high-throughput-produce", "serial-consume", "async-consume", 
            "batch-consume", "parallel-consume", "thread-pool-consume",
            "multi-consumer"
        ],
        default="all",
        help="Which pattern to demonstrate"
    )
    
    args = parser.parse_args()
    
    producer_demo = DemoProducer()
    consumer_demo = DemoConsumer()
    
    # Producer demos
    if args.pattern in ["all", "serial-produce"]:
        producer_demo.demo_serial_send()
        
    if args.pattern in ["all", "async-produce"]:
        await producer_demo.demo_async_parallel_send()
        
    if args.pattern in ["all", "batch-produce"]:
        producer_demo.demo_batch_send()
        
    if args.pattern in ["all", "transaction-produce"]:
        producer_demo.demo_transaction_send()
        
    if args.pattern in ["all", "high-throughput-produce"]:
        producer_demo.demo_high_throughput_config()
    
    # Give some time for messages to be produced
    if args.pattern == "all":
        logger.info("Waiting 2 seconds before consuming...")
        await asyncio.sleep(2)
    
    # Consumer demos
    if args.pattern in ["all", "serial-consume"]:
        consumer_demo.demo_serial_consume()
        
    if args.pattern in ["all", "async-consume"]:
        await consumer_demo.demo_async_consume()
        
    if args.pattern in ["all", "batch-consume"]:
        consumer_demo.demo_batch_consume()
        
    if args.pattern in ["all", "parallel-consume"]:
        try:
            await consumer_demo.demo_parallel_consume()
        except asyncio.TimeoutError:
            logger.info("Parallel consume demo timeout (expected)")
            
    if args.pattern in ["all", "thread-pool-consume"]:
        consumer_demo.demo_thread_pool_consume()
        
    if args.pattern in ["all", "multi-consumer"]:
        await MultiConsumerDemo.demo_multi_consumer()


if __name__ == "__main__":
    asyncio.run(main())