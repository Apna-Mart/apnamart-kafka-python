# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ApnaMart Kafka Python is a production-ready Kafka client library built on confluent-kafka that simplifies Kafka operations for Python applications. The library provides three main components:

- **Producer**: For sending messages to Kafka topics
- **Consumer**: For reading messages from Kafka topics
- **TransactionalProducer**: For ACID transactions across multiple messages

## Architecture

### Core Design
- **Single-module architecture**: All implementation in `apnamart_kafka/client.py`
- **Unified API**: Clean imports through `apnamart_kafka/__init__.py`
- **Built on confluent-kafka**: Leverages the high-performance librdkafka C library
- **Auto-serialization**: JSON serialization/deserialization with fallback to string/bytes

### Key Components

1. **Config class**: `apnamart_kafka/client.py:17-68`
   - Handles both producer and consumer configuration
   - Converts to confluent-kafka format via `to_producer_config()` and `to_consumer_config()`

2. **Producer class**: `apnamart_kafka/client.py:152-293`
   - Lazy connection initialization via `_get_producer()`
   - Batch operations support with `send_batch()`
   - Context manager for automatic cleanup

3. **Consumer class**: `apnamart_kafka/client.py:300-432`
   - Iterator interface for message consumption
   - Batch polling with `poll_batch()`
   - Comprehensive error handling with specific error types

4. **TransactionalProducer class**: `apnamart_kafka/client.py:439-584`
   - Extends Producer with transaction support
   - Auto-initialization of transactions
   - Automatic abort on context manager exit with exceptions

## Development Commands

### Package Management
- **Install dependencies**: `uv sync` (uses uv for fast dependency management)
- **Install dev dependencies**: `uv sync --dev`
- **Add new dependency**: `uv add <package>` (NEVER edit pyproject.toml manually)
- **Remove dependency**: `uv remove <package>`

### Testing
- **Run all tests**: `uv run pytest`
- **Unit tests only**: `uv run pytest tests/unit/ -v`
- **Integration tests**: `uv run pytest tests/integration/ -v` (requires running Kafka)
- **Performance benchmarks**: `uv run pytest tests/performance/ -v -m benchmark`
- **With coverage**: `uv run pytest --cov=apnamart_kafka --cov-report=html`
- **Test specific markers**: `uv run pytest -m "unit and not slow"`

### Code Quality
- **Lint code**: `uv run ruff check`
- **Format code**: `uv run ruff format`
- **Type checking**: `uv run mypy apnamart_kafka/`
- **Run all quality checks**: `uv run ruff check && uv run ruff format && uv run mypy apnamart_kafka/`

### Performance Testing
- **Run benchmarks**: `uv run pytest tests/performance/ -v -m benchmark`
- **Stress tests**: `uv run pytest tests/test_stress.py -v -m stress`
- **Custom benchmark parameters**: `BENCHMARK_DURATION=30 BENCHMARK_MESSAGE_COUNT=100000 uv run pytest tests/performance/`

## Testing Architecture

### Test Categories (via pytest markers)
- `@pytest.mark.unit`: Unit tests with mocked dependencies
- `@pytest.mark.integration`: Integration tests requiring real Kafka
- `@pytest.mark.benchmark`: Performance benchmarks
- `@pytest.mark.stress`: High-load stress tests
- `@pytest.mark.slow`: Long-running tests

### Test Structure
- `tests/unit/`: Pure unit tests with mocks
- `tests/integration/`: Tests requiring Kafka server
- `tests/performance/`: Throughput and latency benchmarks
- `tests/conftest.py`: Shared fixtures and configuration

### Key Test Fixtures (tests/conftest.py)
- `kafka_servers`: Returns `$KAFKA_BOOTSTRAP_SERVERS` or "localhost:9092"
- `test_topic`: Generates unique topic names for isolation
- `test_config`: Pre-configured Config object for testing
- `producer`/`consumer`: Ready-to-use client instances
- `mock_confluent_producer`/`mock_confluent_consumer`: Mocked instances for unit tests

## Performance Considerations

### Critical Performance Rules
1. **Always reuse connections**: Creating new Producer/Consumer instances is 10-100x slower
2. **Use batch operations**: `send_batch()` for high throughput scenarios
3. **Configure wisely**:
   - High throughput: `batch_size=32768, linger_ms=50, compression_type="snappy"`
   - Low latency: `batch_size=1, linger_ms=0, compression_type="none"`

### Performance Targets
- **Producer throughput**: >30,000 msg/s
- **Consumer throughput**: >25,000 msg/s
- **End-to-end latency**: <5ms average
- **Memory usage**: <20MB per client instance

## Configuration Patterns

### Environment Variables
The library automatically reads:
- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_SECURITY_PROTOCOL`
- `KAFKA_SASL_USERNAME`
- `KAFKA_SASL_PASSWORD`

### Common Configurations
```python
# Production reliability
Config(acks="all", retries=10, enable_idempotence=True)

# High throughput
Config(batch_size=32768, linger_ms=50, compression_type="snappy")

# Low latency
Config(batch_size=1, linger_ms=0)
```

## Error Handling

### Exception Hierarchy
- `KafkaError`: Base exception
- `ProducerError`: Producer-specific errors
- `ConsumerError`: Consumer-specific errors
- `TransactionError`: Transaction-specific errors

### Common Error Patterns
- Connection failures: Check `bootstrap_servers` and network
- Topic not found: Verify topic exists and permissions
- Queue full: Call `flush()` or reduce message rate
- Authentication: Verify SASL credentials

## Common Development Tasks

### Adding New Features
1. Implement in `apnamart_kafka/client.py`
2. Add to exports in `apnamart_kafka/__init__.py`
3. Write unit tests in `tests/unit/`
4. Add integration tests if needed
5. Update examples in `examples/`

### Modifying Configuration
- Configuration logic is in `Config` class methods `to_producer_config()` and `to_consumer_config()`
- Always maintain backward compatibility
- Test with both unit and integration tests

### Performance Optimization
1. Profile with `tests/performance/test_benchmarks.py`
2. Focus on connection reuse and batching
3. Test with realistic message sizes (1KB typical)
4. Validate against performance targets

### Debugging Tips
- Use `KAFKA_BOOTSTRAP_SERVERS=localhost:9092` for local testing
- Enable debug logging for confluent-kafka issues
- Monitor system resources during performance testing
- Use unique topic names to avoid test interference

## Important Notes
- **Never edit dependencies manually** - always use `uv add/remove`
- **Integration tests require Kafka** - start with `docker run -p 9092:9092 apache/kafka`
- **Performance tests are timing-sensitive** - run on dedicated hardware for accurate results
- **Connection reuse is critical** - creating new clients repeatedly kills performance