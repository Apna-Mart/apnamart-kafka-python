# Apna-Mart Kafka Python

A scalable Kafka producer and consumer library for Python services designed for enterprise use.

[![Tests](https://github.com/Apna-Mart/apnamart-kafka-python/workflows/tests/badge.svg)](https://github.com/Apna-Mart/apnamart-kafka-python/actions)
[![Coverage](https://img.shields.io/badge/coverage-71%25-yellow)](https://github.com/Apna-Mart/apnamart-kafka-python)
[![Python](https://img.shields.io/badge/python-3.13+-blue)](https://python.org)

## 🚀 Features

- **🎯 Producer & Consumer**: Complete Kafka client with both producer and consumer functionality
- **⚡ Sync & Async**: Both synchronous and asynchronous interfaces
- **🔧 Zero-Config**: Works out of the box with sensible defaults
- **🌍 Environment-Driven**: Configure via environment variables
- **📦 Multiple Serializers**: JSON, string, and bytes built-in
- **🛡️ Type Safe**: Full type hints and Pydantic validation
- **🔄 Production Ready**: Error handling, retries, health checks
- **🧪 Well Tested**: 105+ tests with comprehensive coverage
- **🔍 Enterprise Monitoring**: Built-in metrics and observability
- **🔌 Extensible**: Plugin system for custom functionality

## 📦 Installation

### From GitHub

```bash
# Install directly from GitHub
pip install git+https://github.com/Apna-Mart/apnamart-kafka-python.git

# Or with uv
uv add git+https://github.com/Apna-Mart/apnamart-kafka-python.git
```

### For Development

```bash
# Clone the repository
git clone https://github.com/Apna-Mart/apnamart-kafka-python.git
cd apnamart-kafka-python

# Install with uv (recommended)
uv sync

# Or with pip
pip install -e .
```

## 🏃‍♂️ Quick Start

### Producer Usage

```python
from apnamart_kafka import KafkaProducer

# Simple usage with environment config
# Set: KAFKA_BOOTSTRAP_SERVERS=localhost:9092
with KafkaProducer() as producer:
    producer.send("user-events", {
        "user_id": 123, 
        "action": "login",
        "timestamp": "2024-01-01T10:00:00Z"
    })
```

### Consumer Usage

```python
from apnamart_kafka import KafkaConsumer, KafkaConsumerConfig

# Configure consumer
config = KafkaConsumerConfig(
    group_id="my-service",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest"
)

# Consume messages
with KafkaConsumer(config=config) as consumer:
    consumer.subscribe(["user-events"])
    
    for message in consumer.consume(timeout_ms=1000):
        print(f"Received: {message.value}")
```

### Async Usage

```python
import asyncio
from apnamart_kafka import KafkaProducer, KafkaConsumer, KafkaConsumerConfig

# Async Producer
async def publish_events():
    async with KafkaProducer() as producer:
        # Send multiple messages concurrently
        tasks = [
            producer.send_async("user-events", {"user_id": i, "action": "signup"})
            for i in range(100)
        ]
        await asyncio.gather(*tasks)

# Async Consumer
async def consume_events():
    config = KafkaConsumerConfig(group_id="async-service")
    async with KafkaConsumer(config=config) as consumer:
        consumer.subscribe(["user-events"])
        
        async for message in consumer.consume_async():
            await process_message(message)
            await consumer.commit_offsets_async()

asyncio.run(publish_events())
```

### Service Integration

```python
from apnamart_kafka import KafkaProducer, KafkaConsumer, KafkaConfig, KafkaConsumerConfig

# Shared configuration
base_config = {
    "bootstrap_servers": "kafka-cluster:9092",
    "topic_prefix": "myservice",  # Auto-prefixes all topics
}

class UserService:
    def __init__(self):
        # Producer for publishing events
        producer_config = KafkaConfig(**base_config, acks="all", retries=5)
        self.producer = KafkaProducer(config=producer_config)
        
        # Consumer for processing events
        consumer_config = KafkaConsumerConfig(
            **base_config, 
            group_id="user-service"
        )
        self.consumer = KafkaConsumer(config=consumer_config)
    
    def create_user(self, user_data):
        # Business logic here...
        
        # Publish event (topic becomes "myservice.user-created")
        self.producer.send("user-created", {
            "user_id": user_data["id"],
            "email": user_data["email"],
            "created_at": user_data["created_at"]
        })
    
    def process_user_events(self):
        # Subscribe to user events
        self.consumer.subscribe(["user-created", "user-updated"])
        
        for message in self.consumer.consume():
            if message.topic.endswith("user-created"):
                self.handle_user_created(message.value)
            elif message.topic.endswith("user-updated"):
                self.handle_user_updated(message.value)
```

## ⚙️ Configuration

### Environment Variables

Set these environment variables for zero-config usage:

```bash
# Required
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"

# Optional - Producer
export KAFKA_TOPIC_PREFIX="myservice"
export KAFKA_ACKS="all"
export KAFKA_RETRIES="5"
export KAFKA_COMPRESSION_TYPE="gzip"

# Optional - Consumer  
export KAFKA_GROUP_ID="my-service"
export KAFKA_AUTO_OFFSET_RESET="latest"
export KAFKA_ENABLE_AUTO_COMMIT="true"

# Shared
export KAFKA_LOG_LEVEL="INFO"
```

### Programmatic Configuration

```python
from apnamart_kafka import KafkaConfig, KafkaConsumerConfig

# Producer Configuration
producer_config = KafkaConfig(
    # Connection
    bootstrap_servers="broker1:9092,broker2:9092",
    
    # Reliability  
    acks="all",                    # 0, 1, or "all"
    retries=5,
    retry_backoff_ms=100,
    request_timeout_ms=30000,
    
    # Performance
    compression_type="gzip",       # gzip, snappy, lz4, zstd
    max_request_size=1048576,      # 1MB
    
    # Topics
    topic_prefix="myservice",      # Auto-prefix all topics
)

# Consumer Configuration
consumer_config = KafkaConsumerConfig(
    # Connection (inherited from base)
    bootstrap_servers="broker1:9092,broker2:9092",
    topic_prefix="myservice",
    
    # Consumer Group
    group_id="my-consumer-group",
    auto_offset_reset="earliest",  # earliest, latest, none
    
    # Offset Management
    enable_auto_commit=True,
    auto_commit_interval_ms=5000,
    
    # Performance
    max_poll_records=500,
    session_timeout_ms=30000,
    heartbeat_interval_ms=3000,
    fetch_max_wait_ms=500,
)
```

### Serialization Options

```python
# JSON (default)
producer.send("events", {"key": "value"}, serializer="json")

# String 
producer.send("logs", "Plain text message", serializer="string")

# Raw bytes
producer.send("binary", b"Binary data", serializer="bytes")

# Custom serializer
from apnamart_kafka.serializers import serializer_registry, Serializer

class CustomSerializer(Serializer):
    def serialize(self, data):
        return str(data).upper().encode()

serializer_registry.register("custom", CustomSerializer())
producer.send("topic", "hello", serializer="custom")  # Sends b"HELLO"
```

## 🏗️ Development

### Building the Package

```bash
# Install development dependencies
uv sync --extra dev

# Run linting
uv run ruff check apnamart_kafka/ examples/ tests/
uv run ruff format apnamart_kafka/ examples/ tests/

# Type checking
uv run mypy apnamart_kafka/

# Build package
uv build
```

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=apnamart_kafka --cov-report=html

# Run specific test file
uv run pytest tests/test_producer.py -v

# Run with real Kafka (requires running Kafka)
uv run pytest tests/ -k "not mock"
```

### Testing with Real Kafka

Start Kafka using Docker:

```bash
# Start Kafka
docker run -d \
  --name kafka-test \
  -p 9092:9092 \
  apache/kafka:latest

# Test the producer
uv run python examples/basic_usage.py
```

### Examples

```bash
# Producer Examples
uv run python examples/basic_usage.py           # Basic producer patterns
uv run python examples/async_usage.py           # Async producer patterns
uv run python examples/advanced_monitoring.py  # Monitoring & plugins

# Consumer Examples  
uv run python examples/consumer_basic_usage.py  # Basic consumer patterns
uv run python examples/consumer_async_usage.py  # Async consumer patterns
```

## 🛡️ Error Handling

The library provides comprehensive error handling:

```python
from apnamart_kafka import KafkaProducer, KafkaConsumer, KafkaConsumerConfig
from apnamart_kafka.exceptions import PublishError, ConnectionError

# Producer Error Handling
try:
    with KafkaProducer() as producer:
        producer.send("topic", {"data": "value"})
except ConnectionError:
    # Handle connection issues
    print("Failed to connect to Kafka")
except PublishError:
    # Handle publish failures  
    print("Failed to send message")

# Consumer Error Handling  
try:
    config = KafkaConsumerConfig(group_id="error-handling-group")
    with KafkaConsumer(config=config) as consumer:
        consumer.subscribe(["events"])
        
        for message in consumer.consume():
            try:
                process_message(message)
                consumer.commit_offsets()
            except Exception as e:
                # Handle message processing errors
                print(f"Failed to process message: {e}")
                # Could send to dead letter queue, retry, etc.
                
except ConnectionError:
    print("Failed to connect to Kafka cluster")
```

## 🔍 Health Monitoring

```python
from apnamart_kafka import KafkaProducer, KafkaConsumer, KafkaConsumerConfig

# Producer Health Check
producer = KafkaProducer()
health = producer.health_check()
print(f"Producer Status: {health['status']}")  # healthy/unhealthy
print(f"Connection: {health['connection']}")

# Consumer Health Check
config = KafkaConsumerConfig(group_id="health-check")
consumer = KafkaConsumer(config=config)
health = consumer.health_check()
print(f"Consumer Status: {health['status']}")

# Advanced Monitoring
from apnamart_kafka import MetricsCollector, BasicMonitoringHandler

metrics_collector = MetricsCollector()
monitoring_handler = BasicMonitoringHandler()

# Add to producer/consumer
producer.add_monitoring_handler(monitoring_handler)
consumer.add_monitoring_handler(monitoring_handler)

# Get metrics
producer_metrics = producer.get_metrics()
consumer_metrics = consumer.get_metrics()
```

## 🎯 Use Cases

Perfect for:
- **Microservices**: Event publishing between services
- **Data Pipelines**: Streaming data to processing systems  
- **Activity Logging**: User actions, system events
- **Notifications**: Async message delivery
- **Analytics**: Real-time event tracking

## 🔄 Roadmap & Architecture

This library provides a complete producer and consumer implementation with enterprise-ready features:

### ✅ **Currently Available**
- **Complete Producer & Consumer** - Full-featured Kafka client
- **Sync & Async Support** - Both synchronous and asynchronous operations
- **Monitoring & Metrics** - Built-in observability and health checks
- **Plugin System** - Extensible architecture for custom functionality
- **Type Safety** - Full type hints and validation
- **Enterprise Config** - Security, schema registry, cloud platform settings

### 🚧 **Planned Enhancements** 
- Schema Registry integration (Avro, Protobuf)
- Enhanced authentication & SSL support
- Framework integrations (FastAPI, Django, Flask)
- Confluent Cloud optimizations
- Advanced monitoring dashboards
- CLI tools and testing utilities

The architecture is designed to support all these features through the extensible base classes and plugin system.

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/your-org/apnamart-kafka-python/issues)
- **Discussions**: [GitHub Discussions](https://github.com/your-org/apnamart-kafka-python/discussions)
- **Documentation**: [Wiki](https://github.com/your-org/apnamart-kafka-python/wiki)