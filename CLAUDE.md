# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a high-performance, type-safe Kafka client library for Node.js and Bun, built on top of KafkaJS with comprehensive TypeScript support. The library provides production-ready features including transactions, batch operations, error handling, and extensive testing coverage.

## Development Commands

### Building
```bash
bun run build          # Build the library using rslib
bun run dev            # Build in watch mode
```

### Testing
```bash
bun test               # Run only unit tests (fast, 164 tests ~2s)
bun run test:unit      # Run only unit tests (164 tests, ~2s)
bun run test:integration  # Run integration tests (requires Kafka broker)
bun run test:performance  # Run performance benchmarks (3+ minutes)
bun run test:watch     # Run tests in watch mode
bun run test:all       # Run all tests including performance

# Single test files
bun test tests/unit/producer.test.ts
bun test tests/integration/consumer.integration.test.ts --timeout=60000
```

### Code Quality
```bash
bun run check          # Run Biome linter with auto-fix
bun run format         # Format code with Biome
bunx tsc --noEmit      # Type check without building (use bunx for reliability)
```

### Running Examples
```bash
bun run examples/basic-producer.ts      # Basic producer usage
bun run examples/basic-consumer.ts      # Basic consumer usage
bun run examples/batch-operations.ts    # Batch processing
bun run examples/transactional-producer.ts  # ACID transactions
bun run examples/advanced-patterns.ts   # Enterprise patterns (DLQ, deduplication, etc.)
```

## Architecture Overview

### Core Architecture
The library follows a monolithic approach with all main classes in a single `src/client.ts` file:

- **Config Class**: Centralized configuration management with environment variable support
- **Producer Class**: High-performance message producer with batching capabilities
- **Consumer Class**: Flexible consumer with polling and async iteration support
- **TransactionalProducer Class**: ACID transaction support across multiple topics/partitions
- **Message Class**: Immutable message representation with type safety

### Key Design Patterns
- **Lazy Initialization**: Kafka connections are established on first use
- **Singleton Consumer Pattern**: Single consumer.run() instance with message queue for thread safety
- **Connection Health Management**: Automatic reconnection with exponential backoff
- **Resource Management**: Symbol.asyncDispose for proper cleanup
- **Error Hierarchy**: Specific error types (ProducerError, ConsumerError, TransactionError)
- **Type Safety**: Strict TypeScript with no `any` types throughout
- **Performance Optimization**: Parallel operations, batching, and configurable timeouts

### File Structure
```
src/
├── client.ts         # Main implementation (all core classes)
├── errors.ts         # Custom error hierarchy
├── types.ts          # TypeScript type definitions
└── index.ts          # Public API exports

tests/
├── unit/             # Unit tests (mocked KafkaJS)
├── integration/      # Real Kafka broker tests
├── performance/      # Throughput and latency benchmarks
└── setup.ts          # Test configuration

examples/
├── basic-producer.ts        # Simple message sending
├── basic-consumer.ts        # Message consumption patterns
├── batch-operations.ts      # Efficient batch processing
├── transactional-producer.ts # ACID transactions
├── advanced-patterns.ts     # Enterprise patterns
└── README.md               # Example documentation
```

## Core Library Concepts

### Configuration Strategy
- Environment-first configuration with programmatic overrides
- All Kafka settings centralized in Config class
- Type-safe configuration validation
- Support for SSL/SASL authentication

### Message Processing
- JSON serialization/deserialization by default
- Buffer support for binary data
- Header conversion (Buffer → string) for compatibility
- Type-safe message handling with generics

### Performance Optimization
- **Optimized defaults**: 32KB batch size, 5ms linger time, gzip compression
- **High throughput**: >80k msg/s batch operations, 30k+ msg/s individual sends
- **Low latency**: <5ms target for real-time applications
- **Connection stability**: Health checks, automatic reconnection, retry logic
- **Parallel processing**: Topic creation and batch sends executed concurrently
- **Transaction optimization**: Increased maxInFlightRequests from 1 to 5 for better throughput

### Transaction Management
- Begin/Commit/Abort lifecycle
- Cross-topic atomic operations
- Automatic transaction cleanup on disposal
- Error recovery and rollback mechanisms

## Testing Strategy

### Unit Tests (164 tests)
- Complete mocking of KafkaJS dependencies
- All public methods and error conditions tested
- Fast execution (~2 seconds total)
- 100% pass rate required
- Dual-mode testing: singleton pattern for integration, individual calls for unit tests

### Integration Tests
- Real Kafka broker required (localhost:9092)
- End-to-end message flow validation
- Consumer group coordination testing
- Cross-topic transaction verification
- **Note**: May require topic pre-creation or broker auto-creation enabled

### Performance Tests
- Throughput benchmarks: >80k msg/s batch, >30k msg/s individual
- Latency measurements: <5ms target (P95, P99)
- Transaction performance: >5k msg/s with batching
- Memory usage validation and connection stability testing
- **Timeout**: 3+ minutes for comprehensive benchmarks

## Critical Implementation Details

### KafkaJS Integration
- Uses KafkaJS 2.2+ as underlying client
- Wraps KafkaJS APIs with type-safe interfaces
- **Configuration Optimization**: Enhanced defaults for production performance
  - `maxInFlightRequests`: 1000 (producers), 5 (transactions)
  - `batchSize`: 32KB, `lingerMs`: 5ms, compression: gzip
  - `fetchMaxWait`: 100ms, `fetchMaxBytes`: 1MB
- Manages connection lifecycle with health checks and reconnection

### Error Handling
- Custom error hierarchy extending base Error
- Specific error types for different scenarios
- **Producer**: Automatic reconnection with health checks for "write after end" errors
- **Consumer**: Singleton pattern prevents multiple consumer.run() conflicts
- **Retry Logic**: Up to 3 attempts with exponential backoff (faster for unit tests)
- **Connection Recovery**: Intelligent error detection and automatic reconnection
- Dead Letter Queue (DLQ) pattern support in examples

### Type Safety Enforcement
- No `any` types allowed anywhere in codebase
- Strict TypeScript configuration
- Generic types for message payloads
- Comprehensive type definitions in types.ts

## Development Requirements

### Prerequisites
- Node.js 18+ or Bun 1.0+
- Apache Kafka 2.8+ (for integration tests)
- TypeScript 5.0+

### Environment Variables
```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_CLIENT_ID=apnamart-kafka-client
KAFKAJS_NO_PARTITIONER_WARNING=1  # Suppress KafkaJS warnings
```

### Code Standards
- Biome for linting and formatting (single quotes, 2-space indentation)
- Strict TypeScript mode
- 90%+ test coverage requirement
- No `any` types policy
- JSDoc comments for public APIs

## Common Development Tasks

### Adding New Features
1. Implement in `src/client.ts` following existing patterns
2. **Update unit tests**: Modify expectations for any changed configuration defaults
3. **Mock Compatibility**: Ensure new features work with both singleton and individual patterns
4. **Connection Management**: Add health checks and retry logic for new producer features
5. **Type Safety**: Update type definitions in `src/types.ts`
6. Add examples demonstrating usage and integration tests if needed

### Performance Testing
- Use `bun run test:performance` for benchmarks
- **Target Metrics**:
  - Producer: >30k msg/s individual, >80k msg/s batch
  - Consumer: >25k msg/s with proper broker setup
  - Latency: <5ms average, <10ms P95
  - Transactions: >5k msg/s with batching
- **Environment Requirements**: Kafka broker with auto-topic creation enabled
- **Timeout**: 3+ minutes for comprehensive testing
- **Note**: Performance issues often indicate broker setup problems, not library issues

### Integration Testing Kafka Setup
```bash
# Start Kafka with Docker
docker run -d --name kafka \
  -p 9092:9092 \
  -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  confluent/kafka

# Run integration tests
bun run test:integration
```

## Consumer Architecture (Critical)

### Singleton Pattern Implementation
The consumer uses a singleton `consumer.run()` pattern to prevent conflicts:
- **Unit Tests**: Individual `run()` calls for each `poll()` (detected via mocking)
- **Integration**: Single persistent `run()` with shared message queue
- **Message Queue**: Internal buffering with resolver pattern for batching
- **Dual Mode**: Automatically switches between test and production behavior

### Producer Connection Management
- **Health Checks**: Automatic detection of "write after end" errors
- **Reconnection Logic**: Exponential backoff with configurable max retries
- **Error Detection**: Intelligent retry for connection-related errors
- **Test Optimization**: Faster retry delays for unit test environments

## Common Issues & Solutions

### Integration Test Environment
- **Topic Creation**: Broker must allow auto-creation or pre-create topics
- **"This server does not host this topic-partition"**: Check broker configuration
- **Timing Issues**: Consumer may need time to subscribe before producing
- **Connection Timeouts**: Increase timeout for slow Kafka setups

### Performance Troubleshooting
- **Low Throughput**: Check `batchSize`, `lingerMs`, and `maxInFlightRequests`
- **High Latency**: Reduce `lingerMs`, check broker network latency
- **Connection Failures**: Verify broker accessibility and security config
- **Test Failures**: Unit tests should always pass; integration failures often environmental

### Debugging Tips
- Integration test failures often relate to Kafka broker setup, not library issues
- Use `--timeout=60000` for integration tests requiring longer Kafka operations
- Check broker logs for topic creation and connection issues
- Unit tests mock KafkaJS completely and should pass in any environment
- Performance benchmarks require properly configured Kafka with auto-topic creation