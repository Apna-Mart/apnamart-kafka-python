# ApnaMart Kafka Python

**Minimal Kafka client boilerplate for Python applications**

A simple, clean Kafka producer and consumer library built on `confluent-kafka` with zero external dependencies (except confluent-kafka). Perfect for dropping into any Python application.

## Features

- **Minimal**: Only 2 Python files + init
- **Fast**: Built on confluent-kafka (librdkafka)
- **Simple**: Easy to understand and modify
- **Portable**: Drop into any Python project
- **Reliable**: Proper error handling
- **Transactions**: Full ACID transaction support

## Installation

```bash
pip install apnamart-kafka-python
```

## Quick Start

### Producer
```python
from apnamart_kafka import send, Producer

# Simplest way - one-liner
send("my-topic", {"message": "Hello World!"})

# Producer instance for multiple messages
with Producer() as producer:
    producer.send("events", {"user": "john", "action": "login"})
    producer.send("events", {"user": "jane", "action": "logout"})
```

### Consumer
```python
from apnamart_kafka import consume, Consumer

# Simplest way - iterator
for message in consume("my-topic"):
    print(message.value)

# Consumer instance for more control
with Consumer("my-topic", group_id="my-service") as consumer:
    for message in consumer:
        print(f"Received: {message.value}")
        consumer.commit(message)  # Manual commit
```

### Transactions
```python
from apnamart_kafka import TransactionalProducer

# Manual transaction control
with TransactionalProducer("my-tx-id") as producer:
    producer.begin()
    producer.send_transactional("topic1", {"data": "message1"})
    producer.send_transactional("topic2", {"data": "message2"})
    producer.commit()

# Automatic batch transaction
with TransactionalProducer("my-tx-id") as producer:
    messages = [
        ("topic1", {"data": "message1"}),
        ("topic2", {"data": "message2"})
    ]
    producer.send_batch_transactional(messages)
```

## Configuration

```python
from apnamart_kafka import Config, Producer, Consumer

# Custom configuration
config = Config(
    bootstrap_servers="localhost:9092",
    acks="all",
    compression_type="gzip"
)

# Use with producer
with Producer(config) as producer:
    producer.send("topic", "message")

# Use with consumer  
with Consumer("topic", config) as consumer:
    for message in consumer:
        print(message.value)
```

## Connection Management

**IMPORTANT:** Understanding connection management is crucial for optimal performance.

### How It Works

- `confluent-kafka` (librdkafka) **automatically handles connection pooling**
- Connections are **persistent TCP connections** that remain open until explicitly closed
- **Sparse connections** (default) - connects only to required brokers, not all brokers
- **Automatic reconnection** handles network failures and broker failovers

### Performance Impact

**Connection reuse is 10-100x faster** than creating new connections:

```python
# ❌ BAD - Creates new connection each time (100-500ms overhead)
for i in range(1000):
    send("topic", f"message {i}")  # Creates new Producer each time!

# ✅ GOOD - Reuses connection (<10ms per message)
with Producer() as producer:
    for i in range(1000):
        producer.send("topic", f"message {i}")  # Reuses same Producer
```

### Best Practices

#### 1. Application-Level Singleton (Recommended)
```python
# Create once at app startup
producer = Producer(Config(bootstrap_servers="localhost:9092"))

class MessageService:
    def send_event(self, event_data):
        producer.send("events", event_data)  # Reuses connection
    
    def send_notification(self, notification):
        producer.send("notifications", notification)  # Same connection

# Use throughout application lifetime
service = MessageService()
service.send_event({"user": "john", "action": "login"})
```

#### 2. Thread-Safe Shared Instance
```python
import threading
from apnamart_kafka import Producer

class KafkaManager:
    _producer = None
    _lock = threading.Lock()
    
    @classmethod
    def get_producer(cls):
        if cls._producer is None:
            with cls._lock:
                if cls._producer is None:
                    cls._producer = Producer()
        return cls._producer

# Safe to use from multiple threads
def worker_function(data):
    producer = KafkaManager.get_producer()
    producer.send("topic", data)
```

#### 3. Context Manager for Scripts
```python
# Good for batch jobs that run and exit
def process_data_batch(data_items):
    with Producer() as producer:
        for item in data_items:
            producer.send("processed", item)
        # Connection automatically closed when script exits
```

### Connection Configuration

Optimize connection behavior:

```python
config = Config(
    bootstrap_servers="broker1:9092,broker2:9092",  # Multiple for resilience
    connections_max_idle_ms=540000,  # Keep connections alive (9 minutes)
    reconnect_backoff_ms=50,         # Quick reconnection attempts
    reconnect_backoff_max_ms=1000,   # Max 1 second between reconnects
    socket_keepalive_enable=True,    # Enable TCP keepalive
)

# Create once, use many times
producer = Producer(config)
```

### When to Create New Producers

Only create new Producer instances for:
- **Different configurations** (different brokers, settings)
- **Application isolation** (separate components need isolation)
- **Configuration changes** (when broker settings change)

### Memory Usage

A single Producer instance typically uses:
- **5-10MB RAM** for metadata and buffers
- **1-3 TCP connections** per broker (with sparse connections)
- **Automatic cleanup** when closed properly

## File Structure

```
apnamart_kafka/
├── __init__.py          # Clean exports
├── client.py            # All functionality in one file
```

That's it! Only 2 files with all the functionality you need.

## Batch Message Formats

The `send_batch` method supports both tuple and dict formats for flexibility:

```python
from apnamart_kafka import Producer

with Producer() as producer:
    # Tuple format: (topic, value) or (topic, value, key)
    tuple_messages = [
        ("events", {"user": "john", "action": "login"}),
        ("events", {"user": "jane", "action": "logout"}, "user-jane"),
        ("notifications", {"message": "Welcome!"})
    ]

    # Dict format: {"topic": topic, "value": value, "key": key}
    dict_messages = [
        {"topic": "events", "value": {"user": "bob"}, "key": "user-bob"},
        {"topic": "logs", "value": {"level": "info", "message": "Started"}}
    ]

    # Mixed formats work too
    mixed_messages = [
        ("events", {"mixed": True}),
        {"topic": "events", "value": {"dict": True}}
    ]

    # All formats work
    producer.send_batch(tuple_messages)
    producer.send_batch(dict_messages)
    producer.send_batch(mixed_messages)
```

## Recent Improvements (v2.0.0)

### Fixed TransactionalProducer API
- **Fixed**: `send_transactional(topic, value, key=None)` now works as expected
- **Added**: `send_batch_transactional()` for automatic batch transactions
- **Improved**: Better error messages for transaction failures

### Enhanced Batch Operations
- **Fixed**: `send_batch()` now supports both tuple `(topic, value)` and dict formats
- **Added**: Mixed format support in single batch
- **Improved**: Better error reporting for individual message failures

### Better Error Handling
- **Improved**: More descriptive error messages with context
- **Added**: Specific error types for different failure scenarios
- **Enhanced**: Consumer error handling for unknown topics and connection issues

## API Reference

### Producer
- `send(topic, value, key=None, **kwargs)` - Quick send function
- `Producer(config=None, **kwargs)` - Producer class
- `producer.send(topic, value, key=None)` - Send message
- `producer.send_batch(messages)` - Send multiple messages (supports tuple and dict formats)
- `producer.flush()` - Wait for delivery

### Consumer
- `consume(topics, **kwargs)` - Quick consume iterator
- `Consumer(topics, config=None, **kwargs)` - Consumer class
- `consumer.poll(timeout=1.0)` - Poll single message
- `consumer.poll_batch(size=100)` - Poll multiple messages
- `consumer.commit(message=None)` - Commit offsets

### TransactionalProducer
- `TransactionalProducer(transactional_id, **kwargs)` - Transaction producer
- `begin()`, `commit()`, `abort()` - Transaction control
- `send_transactional(topic, value, key=None)` - Send single message in transaction
- `send_batch_transactional(messages)` - Send batch in automatic transaction

### Message
- `message.topic` - Topic name
- `message.partition` - Partition number  
- `message.offset` - Message offset
- `message.key` - Message key
- `message.value` - Message value (auto-deserialized)
- `message.timestamp` - Message timestamp
- `message.headers` - Message headers

## Testing

```bash
# Run basic tests (no Kafka required)
python tests/test_basic.py

# Run example (shows connection errors without Kafka)
python example.py

# With pytest
pip install pytest
pytest tests/
```

## Use Cases

Perfect for:
- Microservices communication
- Event streaming
- Log aggregation  
- Real-time data pipelines
- Message queues
- Event sourcing

## Requirements

- Python 3.8+
- confluent-kafka

## License

MIT License