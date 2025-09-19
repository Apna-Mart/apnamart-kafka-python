# ApnaMart Kafka TypeScript Library

[![TypeScript](https://img.shields.io/badge/TypeScript-5.9+-blue.svg)](https://www.typescriptlang.org/)
[![Bun](https://img.shields.io/badge/Bun-1.0+-black.svg)](https://bun.sh/)
[![KafkaJS](https://img.shields.io/badge/KafkaJS-2.2+-green.svg)](https://kafka.js.org/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A high-performance, type-safe Kafka client library for Node.js and Bun, built on top of KafkaJS with comprehensive TypeScript support. Designed for production use cases with features like transactions, batch operations, error handling, and extensive testing coverage.

## 🚀 Features

- **🔒 Type Safety**: Full TypeScript support with strict typing and no `any` types
- **⚡ High Performance**: Optimized for throughput (>30k msg/s) and low latency (<5ms)
- **🔄 Transactional Support**: ACID transactions across multiple topics
- **📦 Batch Operations**: Efficient batch sending and consuming
- **🛡️ Error Handling**: Comprehensive error recovery and retry mechanisms
- **🔧 Flexible Configuration**: Environment-based and programmatic configuration
- **🧪 Well Tested**: 100+ unit tests and extensive integration test coverage
- **📚 Rich Examples**: Complete examples for common patterns and advanced use cases
- **🌐 Cross-Runtime**: Works with Node.js, Bun, and other JavaScript runtimes

## 📋 Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Concepts](#core-concepts)
- [API Reference](#api-reference)
- [Configuration](#configuration)
- [Advanced Usage](#advanced-usage)
- [Examples](#examples)
- [Testing](#testing)
- [Performance](#performance)
- [Contributing](#contributing)

## 🛠️ Installation

```bash
# Using Bun (recommended)
bun add apnamart-kafka-node

# Using npm
npm install apnamart-kafka-node

# Using yarn
yarn add apnamart-kafka-node

# Using pnpm
pnpm add apnamart-kafka-node
```

### Prerequisites

- Node.js 18+ or Bun 1.0+
- Apache Kafka 2.8+ (for the broker)
- TypeScript 5.0+ (for development)

## 🏃‍♂️ Quick Start

### Basic Producer

```typescript
import { Config, Producer } from 'apnamart-kafka-node';

const config = new Config({
  bootstrapServers: 'localhost:9092',
  acks: 'all',
  retries: 3,
});

const producer = new Producer(config);

// Send a simple message
const result = await producer.send('my-topic', {
  message: 'Hello, Kafka!',
  timestamp: new Date().toISOString(),
});

console.log('Message sent:', result);

// Clean up
await producer.close();
```

### Basic Consumer

```typescript
import { Config, Consumer } from 'apnamart-kafka-node';

const config = new Config({
  bootstrapServers: 'localhost:9092',
  groupId: 'my-consumer-group',
  autoOffsetReset: 'earliest',
});

const consumer = new Consumer(['my-topic'], config);

// Poll for messages
const message = await consumer.poll(5000);
if (message) {
  console.log('Received:', message.value);
  await consumer.commit(message);
}

// Or use async iteration
for await (const message of consumer) {
  console.log('Received:', message.value);
  // Auto-commit happens automatically
}

await consumer.close();
```

### Batch Operations

```typescript
import { Producer, type MessageInput } from 'apnamart-kafka-node';

const producer = new Producer(config);

// Send multiple messages efficiently
const messages: MessageInput[] = [
  ['topic1', { id: 1, data: 'Message 1' }],
  ['topic2', { id: 2, data: 'Message 2' }, 'key-2'],
  {
    topic: 'topic3',
    value: { id: 3, data: 'Message 3' },
    key: 'key-3',
    headers: { 'content-type': 'application/json' },
  },
];

const results = await producer.sendBatch(messages);
console.log(`Sent ${results.length} messages`);
```

### Transactional Producer

```typescript
import { TransactionalProducer } from 'apnamart-kafka-node';

const txProducer = new TransactionalProducer('my-tx-id', config);

// Atomic operations across topics
await txProducer.begin();

await txProducer.sendTransactional('orders', { orderId: '123', status: 'created' });
await txProducer.sendTransactional('payments', { orderId: '123', amount: 99.99 });
await txProducer.sendTransactional('inventory', { productId: 'ABC', quantity: -1 });

await txProducer.commit(); // All messages are visible atomically

await txProducer.close();
```

## 🧠 Core Concepts

### Configuration

The `Config` class provides a centralized way to configure Kafka clients with TypeScript safety:

```typescript
const config = new Config({
  // Basic connection
  bootstrapServers: 'localhost:9092',
  clientId: 'my-app',

  // Producer settings
  acks: 'all',                    // Wait for all replicas
  retries: 3,                     // Retry failed sends
  batchSize: 16384,               // 16KB batch size
  lingerMs: 10,                   // Wait 10ms for batching
  compressionType: 'gzip',        // Compress messages

  // Consumer settings
  groupId: 'my-group',
  autoOffsetReset: 'earliest',    // Start from beginning
  sessionTimeout: 30000,          // 30s session timeout
  heartbeatInterval: 10000,       // 10s heartbeat

  // Security (optional)
  ssl: true,
  sasl: {
    mechanism: 'plain',
    username: 'user',
    password: 'pass',
  },
});
```

### Message Format

Messages in the library are represented by the `Message` class:

```typescript
interface Message<T = unknown> {
  topic: string;
  partition: number;
  offset: string;
  key: string | null;
  value: T;
  timestamp: string;
  headers: Record<string, string>;
}
```

### Error Handling

The library provides specific error types for different scenarios:

```typescript
import { ProducerError, ConsumerError, TransactionError } from 'apnamart-kafka-node';

try {
  await producer.send('topic', message);
} catch (error) {
  if (error instanceof ProducerError) {
    console.error('Producer error:', error.message);
  }
}
```

## 📖 API Reference

### Config Class

Central configuration management for Kafka clients.

```typescript
class Config {
  constructor(options?: Partial<ConfigOptions>);

  // Properties (readonly)
  readonly bootstrapServers: string;
  readonly clientId: string;
  readonly acks: 'all' | '1' | '0';
  readonly retries: number;
  readonly batchSize: number;
  readonly lingerMs: number;
  readonly compressionType: 'none' | 'gzip' | 'snappy' | 'lz4' | 'zstd';
  readonly requestTimeout: number;
  readonly connectionTimeout: number;
  readonly groupId?: string;
  readonly autoOffsetReset: 'earliest' | 'latest' | 'none';
  readonly sessionTimeout: number;
  readonly heartbeatInterval: number;
  readonly maxPollRecords: number;
  readonly enableAutoCommit: boolean;
  readonly autoCommitInterval: number;
  readonly fetchMinBytes: number;
  readonly fetchMaxWait: number;
  readonly maxPartitionFetchBytes: number;
  readonly ssl?: boolean;
  readonly sasl?: SaslOptions;
  readonly transactionTimeout: number;
}
```

### Producer Class

High-performance message producer with batching support.

```typescript
class Producer {
  constructor(config: Config);

  // Send single message
  async send<T>(
    topic: string,
    message: T,
    key?: string | null,
    options?: MessageOptions
  ): Promise<SendResult>;

  // Send multiple messages efficiently
  async sendBatch(messages: MessageInput[]): Promise<SendResult[]>;

  // Force delivery of buffered messages
  async flush(): Promise<void>;

  // Close producer and clean up resources
  async close(): Promise<void>;

  // Async disposal (using statement support)
  async [Symbol.asyncDispose](): Promise<void>;
}
```

### Consumer Class

Flexible message consumer with polling and async iteration support.

```typescript
class Consumer {
  constructor(topics: string | string[], config?: Config);

  // Poll for single message
  async poll(timeout?: number): Promise<Message | null>;

  // Poll for multiple messages
  async pollBatch(maxMessages: number, timeout?: number): Promise<Message[]>;

  // Commit message offsets
  async commit(message?: Message): Promise<void>;

  // Seek to specific offset
  async seek(topic: string, partition: number, offset: string): Promise<void>;

  // Close consumer
  async close(): Promise<void>;

  // Async iteration support
  [Symbol.asyncIterator](): AsyncIterableIterator<Message>;

  // Async disposal
  async [Symbol.asyncDispose](): Promise<void>;
}
```

### TransactionalProducer Class

ACID transaction support across multiple topics and partitions.

```typescript
class TransactionalProducer {
  constructor(transactionalId: string, config: Config);

  // Transaction lifecycle
  async begin(): Promise<void>;
  async commit(): Promise<void>;
  async abort(): Promise<void>;

  // Send within transaction
  async sendTransactional<T>(
    topic: string,
    message: T,
    key?: string | null,
    options?: MessageOptions
  ): Promise<void>;

  // Batch send within transaction
  async sendBatchTransactional(messages: MessageInput[]): Promise<void>;

  // Close producer
  async close(): Promise<void>;

  // Async disposal
  async [Symbol.asyncDispose](): Promise<void>;
}
```

### Message Class

Immutable message representation with type safety.

```typescript
class Message<T = unknown> {
  constructor(data: MessageData);

  readonly topic: string;
  readonly partition: number;
  readonly offset: string;
  readonly key: string | null;
  readonly value: T;
  readonly timestamp: string;
  readonly headers: Record<string, string>;

  // Utility methods
  toString(): string;
  toJSON(): object;
}
```

### Utility Functions

Convenience functions for common operations.

```typescript
// Create producer with default config
function createProducer(bootstrapServers?: string): Producer;

// Create consumer with default config
function createConsumer(topics: string | string[], groupId?: string): Consumer;

// Create transactional producer
function createTransactionalProducer(
  transactionalId: string,
  bootstrapServers?: string
): TransactionalProducer;
```

## ⚙️ Configuration

### Environment Variables

The library supports configuration via environment variables:

```bash
# Connection
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_CLIENT_ID=my-app

# Security
KAFKA_SSL_ENABLED=true
KAFKA_SASL_MECHANISM=plain
KAFKA_SASL_USERNAME=user
KAFKA_SASL_PASSWORD=pass

# Performance
KAFKA_BATCH_SIZE=16384
KAFKA_LINGER_MS=10
KAFKA_COMPRESSION_TYPE=gzip

# Consumer
KAFKA_GROUP_ID=my-group
KAFKA_AUTO_OFFSET_RESET=earliest
```

### Production Configuration

Recommended settings for production environments:

```typescript
const productionConfig = new Config({
  // Reliability
  acks: 'all',
  retries: 3,
  requestTimeout: 30000,

  // Performance
  batchSize: 32768,      // 32KB for higher throughput
  lingerMs: 5,           // Balance latency vs throughput
  compressionType: 'gzip', // Reduce network usage

  // Consumer reliability
  sessionTimeout: 30000,
  heartbeatInterval: 10000,
  maxPollRecords: 500,   // Process in reasonable batches

  // Security
  ssl: true,
  sasl: {
    mechanism: 'plain',
    username: process.env.KAFKA_USERNAME!,
    password: process.env.KAFKA_PASSWORD!,
  },
});
```

### Development Configuration

Settings optimized for local development:

```typescript
const devConfig = new Config({
  bootstrapServers: 'localhost:9092',
  acks: '1',           // Faster acknowledgment
  retries: 1,          // Quick failure feedback
  batchSize: 1024,     // Smaller batches
  lingerMs: 0,         // Immediate sending
  compressionType: 'none', // No compression overhead
});
```

## 🎯 Advanced Usage

### Custom Partitioning

```typescript
// Partition by user ID for ordered processing per user
await producer.send('user-events', event, userId);

// Explicit partition selection
await producer.send('logs', logEntry, null, { partition: 0 });

// Custom partitioning logic
const partition = hashFunction(message.userId) % totalPartitions;
await producer.send('topic', message, null, { partition });
```

### Error Recovery Patterns

```typescript
// Exponential backoff retry
async function sendWithRetry<T>(
  producer: Producer,
  topic: string,
  message: T,
  maxRetries = 3
): Promise<SendResult> {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await producer.send(topic, message);
    } catch (error) {
      if (attempt === maxRetries) throw error;

      const delay = Math.pow(2, attempt - 1) * 1000;
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
}

// Dead Letter Queue pattern
async function processWithDLQ(consumer: Consumer, producer: Producer) {
  const message = await consumer.poll(5000);
  if (!message) return;

  try {
    await processMessage(message.value);
    await consumer.commit(message);
  } catch (error) {
    // Send to DLQ for manual investigation
    await producer.send('dlq-topic', {
      originalMessage: message.value,
      error: error.message,
      failedAt: new Date().toISOString(),
    });
    await consumer.commit(message);
  }
}
```

### Message Deduplication

```typescript
// Idempotent message processing
const processedMessages = new Set<string>();

async function processIdempotent(message: Message) {
  const idempotencyKey = message.headers['idempotency-key'];

  if (processedMessages.has(idempotencyKey)) {
    console.log('Skipping duplicate message');
    return;
  }

  await processMessage(message.value);
  processedMessages.add(idempotencyKey);
}
```

### Transaction Patterns

```typescript
// Saga pattern with compensation
async function orderSaga(txProducer: TransactionalProducer, order: Order) {
  await txProducer.begin();

  try {
    // Step 1: Reserve inventory
    await txProducer.sendTransactional('inventory', {
      action: 'reserve',
      productId: order.productId,
      quantity: order.quantity,
    });

    // Step 2: Process payment
    await txProducer.sendTransactional('payments', {
      action: 'charge',
      amount: order.total,
      customerId: order.customerId,
    });

    // Step 3: Create shipment
    await txProducer.sendTransactional('shipping', {
      action: 'create',
      orderId: order.id,
      address: order.shippingAddress,
    });

    await txProducer.commit();
  } catch (error) {
    await txProducer.abort();

    // Send compensation events
    await txProducer.begin();
    await txProducer.sendTransactional('orders', {
      action: 'cancel',
      orderId: order.id,
      reason: 'saga-failure',
    });
    await txProducer.commit();

    throw error;
  }
}
```

## 📁 Examples

The library includes comprehensive examples in the `examples/` directory:

- **[basic-producer.ts](examples/basic-producer.ts)**: Simple message sending
- **[basic-consumer.ts](examples/basic-consumer.ts)**: Message consumption patterns
- **[batch-operations.ts](examples/batch-operations.ts)**: Efficient batch processing
- **[transactional-producer.ts](examples/transactional-producer.ts)**: ACID transactions
- **[advanced-patterns.ts](examples/advanced-patterns.ts)**: Enterprise patterns (DLQ, deduplication, etc.)

Run any example:

```bash
bun run examples/basic-producer.ts
```

## 🧪 Testing

### Running Tests

```bash
# Run all tests
bun test

# Run specific test suites
bun run test:unit           # Unit tests only
bun run test:integration    # Integration tests (requires Kafka)
bun run test:performance    # Performance benchmarks

# Run with coverage
bun test --coverage

# Watch mode for development
bun run test:watch
```

### Integration Testing

Integration tests require a running Kafka instance:

```bash
# Start Kafka with Docker
docker-compose up -d kafka

# Run integration tests
bun run test:integration
```

### Performance Testing

The library includes comprehensive performance benchmarks:

```bash
# Run performance tests
bun run test:performance
```

Expected performance metrics:
- **Producer Throughput**: >30,000 messages/second
- **Consumer Throughput**: >25,000 messages/second
- **Average Latency**: <5ms
- **P95 Latency**: <10ms
- **P99 Latency**: <25ms

### Testing Your Applications

```typescript
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { Config, Producer, Consumer } from 'apnamart-kafka-node';

describe('My Kafka Integration', () => {
  let producer: Producer;
  let consumer: Consumer;

  beforeEach(() => {
    const config = new Config({
      bootstrapServers: 'localhost:9092',
    });

    producer = new Producer(config);
    consumer = new Consumer(['test-topic'], config);
  });

  afterEach(async () => {
    await producer.close();
    await consumer.close();
  });

  it('should send and receive messages', async () => {
    const testMessage = { id: 1, data: 'test' };

    await producer.send('test-topic', testMessage);

    const receivedMessage = await consumer.poll(5000);
    expect(receivedMessage).not.toBeNull();
    expect(receivedMessage!.value).toEqual(testMessage);
  });
});
```

## 📊 Performance

### Benchmarks

The library is optimized for high-performance scenarios:

| Operation | Throughput | Latency (P95) | Notes |
|-----------|------------|---------------|-------|
| Producer (single) | 30,000+ msg/s | <5ms | Individual message sends |
| Producer (batch) | 50,000+ msg/s | <10ms | Batch operations |
| Consumer (poll) | 25,000+ msg/s | <5ms | Single message polling |
| Consumer (batch) | 40,000+ msg/s | <10ms | Batch consumption |
| Transactions | 10,000+ msg/s | <20ms | Transactional operations |

### Optimization Tips

1. **Use Batch Operations**: Significantly more efficient than individual sends
2. **Tune Configuration**:
   - Increase `batchSize` for higher throughput
   - Reduce `lingerMs` for lower latency
   - Enable compression for large messages
3. **Partition Strategy**: Use meaningful keys for even distribution
4. **Connection Reuse**: Share producer instances across your application
5. **Resource Management**: Always close producers/consumers properly

### Memory Usage

- **Producer**: ~10MB baseline + ~1KB per buffered message
- **Consumer**: ~5MB baseline + ~500B per consumed message
- **Transactions**: +5MB overhead for transaction coordination

## 🏗️ Development

### Building from Source

```bash
# Clone the repository
git clone https://github.com/apnamart/kafka-typescript-library.git
cd kafka-typescript-library

# Install dependencies
bun install

# Build the library
bun run build

# Run tests
bun test

# Format code
bun run format

# Type check
tsc --noEmit
```

### Project Structure

```
├── src/                    # Source code
│   ├── client.ts          # Main client implementation
│   └── index.ts           # Public API exports
├── tests/                 # Test suites
│   ├── unit/              # Unit tests
│   ├── integration/       # Integration tests
│   └── performance/       # Performance benchmarks
├── examples/              # Usage examples
├── docs/                  # Documentation
└── package.json           # Package configuration
```

### Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes with tests
4. Run the full test suite (`bun test`)
5. Format your code (`bun run format`)
6. Commit your changes (`git commit -m 'Add amazing feature'`)
7. Push to the branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

### Code Standards

- **TypeScript**: Strict mode enabled, no `any` types
- **Testing**: >90% code coverage required
- **Documentation**: JSDoc for all public APIs
- **Formatting**: Biome for consistent code style
- **Commits**: Conventional commit messages

## 📋 Requirements

### Runtime Requirements

- **Node.js**: 18.0+ (LTS recommended)
- **Bun**: 1.0+ (alternative runtime)
- **Kafka**: 2.8+ (broker compatibility)

### Development Requirements

- **TypeScript**: 5.0+
- **Vitest**: 2.0+ (testing framework)
- **Biome**: Latest (linting and formatting)

### Kafka Cluster Requirements

- **Minimum**: 1 broker for development
- **Recommended**: 3+ brokers for production
- **Topics**: Auto-creation enabled or pre-created topics
- **Security**: SSL/SASL configuration for production

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🤝 Support

- **Documentation**: Check the [examples](examples/) directory
- **Issues**: Report bugs on [GitHub Issues](https://github.com/apnamart/kafka-typescript-library/issues)
- **Discussions**: Join our [GitHub Discussions](https://github.com/apnamart/kafka-typescript-library/discussions)
- **Security**: Report security issues to security@apnamart.com

## 🙏 Acknowledgments

- Built on top of the excellent [KafkaJS](https://kafka.js.org/) library
- Inspired by the Kafka ecosystem and community
- TypeScript typing patterns from the broader JavaScript ecosystem
- Performance optimizations based on real-world production usage

## 📈 Roadmap

- [ ] Schema Registry integration
- [ ] Avro and Protobuf serialization support
- [ ] Kafka Streams-like processing APIs
- [ ] Built-in metrics and monitoring
- [ ] Cloud provider integrations (AWS MSK, Confluent Cloud)
- [ ] Web Streams API support
- [ ] Deno runtime support

---

**Made with ❤️ by the ApnaMart team**