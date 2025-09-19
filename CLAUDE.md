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
bun test               # Run all tests (unit + integration + performance)
bun run test:unit      # Run only unit tests (164 tests, ~2s)
bun run test:integration  # Run integration tests (requires Kafka broker)
bun run test:performance  # Run performance benchmarks (3+ minutes)
bun run test:watch     # Run tests in watch mode
bun run test:all       # Run all tests including performance
```

### Code Quality
```bash
bun run check          # Run Biome linter with auto-fix
bun run format         # Format code with Biome
tsc --noEmit           # Type check without building
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
- **Resource Management**: Symbol.asyncDispose for proper cleanup
- **Error Hierarchy**: Specific error types (ProducerError, ConsumerError, TransactionError)
- **Type Safety**: Strict TypeScript with no `any` types throughout

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
- Batch operations for high throughput (>30k msg/s target)
- Configurable batching parameters (batchSize, lingerMs)
- Connection pooling and reuse
- Compression support (gzip, snappy, lz4, zstd)

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
- 90%+ coverage requirements

### Integration Tests
- Real Kafka broker required (localhost:9092)
- End-to-end message flow validation
- Consumer group coordination testing
- Cross-topic transaction verification

### Performance Tests
- Throughput benchmarks (producer/consumer)
- Latency measurements (P95, P99)
- Memory usage validation
- Concurrent operation testing

## Critical Implementation Details

### KafkaJS Integration
- Uses KafkaJS 2.2+ as underlying client
- Wraps KafkaJS APIs with type-safe interfaces
- Handles KafkaJS configuration mapping
- Manages connection lifecycle

### Error Handling
- Custom error hierarchy extending base Error
- Specific error types for different scenarios
- Automatic retry mechanisms with exponential backoff
- Dead Letter Queue (DLQ) pattern support

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
2. Add comprehensive unit tests with mocked KafkaJS
3. Create integration tests if Kafka interaction involved
4. Update type definitions in `src/types.ts`
5. Add examples demonstrating usage

### Performance Testing
- Use `bun run test:performance` for benchmarks
- Target metrics: >30k msg/s throughput, <5ms latency
- Memory usage should be reasonable for long-running tests
- Performance tests have 3-minute timeouts

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

### Debugging Tips
- Integration test failures often relate to Kafka timing/ordering
- Use `testTimeout` configuration for slow operations
- Check Kafka broker connectivity if integration tests fail
- Unit tests should always pass regardless of external dependencies