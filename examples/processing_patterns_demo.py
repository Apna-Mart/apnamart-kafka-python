"""
Example demonstrating how to implement sequential and parallel processing patterns.

This script can be run in three modes: 'producer', 'consumer', or 'consumer-internal-parallel'.

See the PROCESSING_PATTERNS.md file in the project root for a full explanation.
"""

import argparse
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

from apnamart_kafka import (
    KafkaConsumer,
    KafkaConsumerConfig,
    KafkaProducer,
    KafkaConfig,
)

# --- Topic Configuration ---
# To run this demo, create these topics first!
#
# For Sequential Processing (1 Partition):
# kafka-topics.sh --create --topic sequential_topic --bootstrap-server localhost:9092 --partitions 1
#
# For Parallel Processing (12 Partitions):
# kafka-topics.sh --create --topic parallel_topic --bootstrap-server localhost:9092 --partitions 12
#
SEQUENTIAL_TOPIC = "sequential_topic"
PARALLEL_TOPIC = "parallel_topic"
CONSUMER_GROUP_ID = "my-processing-service"
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def run_producer():
    """
    Sends a batch of messages to both the sequential and parallel topics.
    """
    logger.info("Starting producer...")
    try:
        config = KafkaConfig(bootstrap_servers=BOOTSTRAP_SERVERS)
        with KafkaProducer(config) as producer:
            # 1. Send ordered messages to the sequential topic
            logger.info(f"Sending 5 messages to sequential topic: {SEQUENTIAL_TOPIC}")
            for i in range(5):
                producer.send(
                    SEQUENTIAL_TOPIC,
                    value={"step": i + 1, "message": "This must be in order"},
                )
            logger.info("Finished sending to sequential topic.")

            # 2. Send messages with keys to the parallel topic
            logger.info(f"Sending 20 messages to parallel topic: {PARALLEL_TOPIC}")
            for i in range(20):
                # Using a key ensures messages for the same entity go to the same partition
                user_id = f"user_{i % 4}"  # Simulate 4 different users
                producer.send(
                    PARALLEL_TOPIC,
                    key=user_id,
                    value={"event_id": str(uuid4()), "payload": f"Event data {i}"},
                )
            logger.info("Finished sending to parallel topic.")

    except Exception as e:
        logger.error(f"Producer encountered an error: {e}", exc_info=True)


def run_consumer():
    """
    Starts a consumer instance that subscribes to both topics.
    Run multiple instances of this function to see the multi-process parallelism.
    """
    # A unique ID for this specific consumer instance
    instance_id = f"consumer-{os.getpid()}"
    logger.info(f"Starting consumer instance: {instance_id}")

    try:
        config = KafkaConsumerConfig(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUP_ID,
            group_instance_id=instance_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
        )

        with KafkaConsumer(config) as consumer:
            # Subscribe to both topics
            consumer.subscribe(topics=[SEQUENTIAL_TOPIC, PARALLEL_TOPIC])
            logger.info(
                f"Instance {instance_id} subscribed to topics: "
                f"'{SEQUENTIAL_TOPIC}' and '{PARALLEL_TOPIC}'"
            )

            while True:
                messages = consumer.poll(timeout_ms=1000, max_records=10)
                if not messages:
                    logger.debug(f"Instance {instance_id} - No new messages. Polling again.")
                    continue

                for message in messages:
                    logger.info(
                        f"[{instance_id}] | "
                        f"Topic: {message.topic} | "
                        f"Partition: {message.partition} | "
                        f"Key: {message.key} | "
                        f"Value: {message.value}"
                    )
                    # Simulate processing time
                    time.sleep(0.2)

    except KeyboardInterrupt:
        logger.info(f"Consumer instance {instance_id} shutting down.")
    except Exception as e:
        logger.error(f"Consumer instance {instance_id} failed: {e}", exc_info=True)


def process_message_threaded(message):
    """
    The business logic for processing a single message.
    This function runs in a separate thread.
    """
    instance_id = f"consumer-{os.getpid()}"
    thread_id = threading.get_ident()
    logger.info(
        f"[{instance_id}] [Thread-{thread_id}] | "
        f"START processing Topic: {message.topic}, Partition: {message.partition}, Offset: {message.offset}"
    )
    
    # Simulate I/O-bound work like a database call or API request
    time.sleep(1)
    
    logger.info(
        f"[{instance_id}] [Thread-{thread_id}] | "
        f"END processing Topic: {message.topic}, Partition: {message.partition}, Offset: {message.offset}"
    )


def run_consumer_parallel_internal():
    """
    Starts a single consumer instance that processes messages from its
    assigned partitions in parallel using a thread pool.
    """
    instance_id = f"consumer-{os.getpid()}"
    logger.info(f"Starting single-instance parallel consumer: {instance_id}")

    max_workers = 10
    executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="msg_worker")

    try:
        config = KafkaConsumerConfig(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=True,  # Note: DANGEROUS for production, see docs.
        )

        with KafkaConsumer(config) as consumer:
            consumer.subscribe(topics=[SEQUENTIAL_TOPIC, PARALLEL_TOPIC])
            logger.info(f"Instance {instance_id} subscribed. Max internal parallelism: {max_workers} threads.")

            while True:
                messages = consumer.poll(timeout_ms=1000, max_records=50)
                if not messages:
                    continue

                logger.info(f"Polled {len(messages)} messages. Submitting to thread pool...")
                for message in messages:
                    executor.submit(process_message_threaded, message)

    except KeyboardInterrupt:
        logger.info(f"Shutting down consumer instance {instance_id}...")
    finally:
        executor.shutdown(wait=True)


def main():
    """
    Main function to parse arguments and run the appropriate role.
    """
    parser = argparse.ArgumentParser(
        description="Kafka Sequential vs. Parallel Processing Demo"
    )
    parser.add_argument(
        "--role",
        type=str,
        required=True,
        choices=["producer", "consumer", "consumer-internal-parallel"],
        help="The role to run.",
    )
    args = parser.parse_args()

    if args.role == "producer":
        run_producer()
    elif args.role == "consumer":
        run_consumer()
    elif args.role == "consumer-internal-parallel":
        run_consumer_parallel_internal()


if __name__ == "__main__":
    main()
