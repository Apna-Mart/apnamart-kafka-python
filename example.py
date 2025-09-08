#!/usr/bin/env python3
"""Simple usage examples for apnamart-kafka-python."""

from apnamart_kafka import Producer, Consumer, TransactionalProducer, send, consume, Config

def producer_examples():
    """Producer usage examples."""
    print("=== Producer Examples ===")
    
    # 1. Quick send function (simplest)
    print("1. Quick send:")
    try:
        send("my-topic", {"message": "Hello World!"})
        print("✓ Sent with quick function")
    except Exception as e:
        print(f"✗ Failed (expected without Kafka): {e}")
    
    # 2. Producer instance
    print("\n2. Producer instance:")
    with Producer() as producer:
        try:
            producer.send("events", {"user": "john", "action": "login"})
            producer.send("events", "Simple string message")
            print("✓ Sent with Producer instance")
        except Exception as e:
            print(f"✗ Failed (expected without Kafka): {e}")
    
    # 3. Custom configuration
    print("\n3. Custom configuration:")
    config = Config(
        bootstrap_servers="localhost:9092",
        acks="all",
        compression_type="gzip"
    )
    with Producer(config) as producer:
        try:
            producer.send("important", {"critical": "data"})
            print("✓ Sent with custom config")
        except Exception as e:
            print(f"✗ Failed (expected without Kafka): {e}")
    
    # 4. Batch sending
    print("\n4. Batch sending:")
    messages = [
        {"topic": "batch", "value": {"msg": f"Message {i}"}}
        for i in range(3)
    ]
    with Producer() as producer:
        try:
            results = producer.send_batch(messages)
            print(f"✓ Batch results: {results}")
        except Exception as e:
            print(f"✗ Failed (expected without Kafka): {e}")


def consumer_examples():
    """Consumer usage examples."""
    print("\n=== Consumer Examples ===")
    
    # 1. Quick consume function (skip - would hang without Kafka)
    print("1. Quick consume (skipped - would hang):")
    print("   # for message in consume(\"my-topic\", group_id=\"example\"):")
    print("   #     print(message.value)")
    print("✓ Would work with Kafka running")
    
    # 2. Consumer instance
    print("\n2. Consumer instance:")
    try:
        with Consumer("events", group_id="test-group") as consumer:
            # Poll for one message with short timeout
            message = consumer.poll(timeout=0.1)  # Very short timeout
            if message:
                print(f"Received: {message.value}")
            else:
                print("No messages (timeout - expected)")
            print("✓ Consumer polling worked")
    except Exception as e:
        print(f"✗ Failed (expected without Kafka): {e}")
    
    # 3. Iterator interface (skip - would hang)
    print("\n3. Iterator interface (skipped - would hang):")
    print("   # with Consumer(\"notifications\") as consumer:")
    print("   #     for message in consumer:")
    print("   #         print(message.value)")
    print("✓ Would work with Kafka running")


def transactional_examples():
    """Transactional producer examples."""
    print("\n=== Transactional Producer Examples ===")
    
    # 1. Basic transactions
    print("1. Basic transaction:")
    try:
        with TransactionalProducer("my-tx-id") as tx_producer:
            tx_producer.begin()
            tx_producer.send("topic1", {"data": "message1"})
            tx_producer.send("topic2", {"data": "message2"})
            tx_producer.commit()
            print("✓ Transaction completed")
    except Exception as e:
        print(f"✗ Failed (expected without Kafka): {e}")
    
    # 2. Automatic transaction
    print("\n2. Automatic transaction:")
    try:
        tx_producer = TransactionalProducer("auto-tx-id")
        messages = [
            {"topic": "orders", "value": {"order_id": 1}},
            {"topic": "inventory", "value": {"item_id": 1, "qty": -1}}
        ]
        tx_producer.send_transactional(messages)
        tx_producer.close()
        print("✓ Automatic transaction completed")
    except Exception as e:
        print(f"✗ Failed (expected without Kafka): {e}")


def main():
    """Run all examples."""
    print("ApnaMart Kafka Python - Usage Examples")
    print("=====================================")
    print("Note: Examples will show connection errors without running Kafka")
    
    producer_examples()
    consumer_examples()
    transactional_examples()
    
    print("\n✓ All examples completed!")
    print("\nTo run with real Kafka:")
    print("1. Start Kafka: docker run -p 9092:9092 apache/kafka")
    print("2. Run this script again")


if __name__ == "__main__":
    main()