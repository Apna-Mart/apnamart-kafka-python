# ApnaMart Kafka TypeScript Library - Examples

This directory contains comprehensive examples demonstrating how to use the ApnaMart Kafka TypeScript library. Each example focuses on specific use cases and patterns commonly used in production Kafka applications.

## Quick Start

Before running any examples, make sure you have:

1. **Kafka broker running**: The examples assume Kafka is running on `localhost:9092`
2. **Dependencies installed**: Run `bun install` in the project root
3. **Library built**: Run `bun run build` to compile the TypeScript source

```bash
# Install dependencies
bun install

# Build the library
bun run build

# Run an example
bun run examples/basic-producer.ts
```

## Examples Overview

### 1. Basic Producer (`basic-producer.ts`)

Demonstrates fundamental producer operations:
- Sending simple messages
- Messages with keys for partitioning
- Messages with headers and metadata
- Different data types (strings, numbers, objects, arrays)
- Flushing for guaranteed delivery

```bash
bun run examples/basic-producer.ts
```

**Key Concepts:**
- Message serialization
- Partitioning with keys
- Headers and metadata
- Producer configuration

### 2. Basic Consumer (`basic-consumer.ts`)

Shows how to consume messages from Kafka topics:
- Single message polling
- Batch message consumption
- Manual offset commits
- Async iterator pattern
- Multiple consumer configuration patterns

```bash
bun run examples/basic-consumer.ts
```

**Key Concepts:**
- Message polling strategies
- Offset management
- Consumer groups
- Async iteration over messages

### 3. Batch Operations (`batch-operations.ts`)

Demonstrates efficient batch processing:
- Sending multiple messages in a single batch
- Mixed message formats (array and object)
- Multi-topic batch operations
- Performance comparisons
- Large batch handling

```bash
bun run examples/batch-operations.ts
```

**Key Concepts:**
- Batch efficiency
- Throughput optimization
- Resource utilization
- Performance measurement

### 4. Transactional Producer (`transactional-producer.ts`)

Covers advanced transactional operations:
- Basic transaction lifecycle (begin/commit/abort)
- Batch transactional operations
- Cross-topic transactions
- Transaction timeout handling
- Atomic message processing

```bash
bun run examples/transactional-producer.ts
```

**Key Concepts:**
- ACID properties in messaging
- Transaction isolation
- Atomic operations across topics
- Error recovery in transactions

### 5. Advanced Patterns (`advanced-patterns.ts`)

Implements enterprise-grade messaging patterns:
- **Dead Letter Queue (DLQ)**: Failed message handling
- **Message Deduplication**: Preventing duplicate processing
- **Partitioning Strategies**: Custom message distribution
- **Consumer Group Coordination**: Load balancing
- **Event Sourcing**: State reconstruction from events

```bash
bun run examples/advanced-patterns.ts
```

**Key Concepts:**
- Fault tolerance
- Message reliability
- Scalability patterns
- Event-driven architecture

## Example Scenarios

### E-commerce Order Processing

The examples demonstrate a complete e-commerce order flow:

1. **Order Creation** (`transactional-producer.ts`)
   ```typescript
   // Atomic order processing across multiple topics
   await txProducer.begin();
   await txProducer.sendTransactional('orders-topic', orderData);
   await txProducer.sendTransactional('payments-topic', paymentData);
   await txProducer.sendTransactional('inventory-topic', inventoryUpdate);
   await txProducer.commit();
   ```

2. **Event Sourcing** (`advanced-patterns.ts`)
   ```typescript
   // Track order lifecycle through events
   const events = [
     { eventType: 'OrderCreated', aggregateId: orderId, data: {...} },
     { eventType: 'PaymentProcessed', aggregateId: orderId, data: {...} },
     { eventType: 'OrderShipped', aggregateId: orderId, data: {...} }
   ];
   ```

3. **Error Handling** (`advanced-patterns.ts`)
   ```typescript
   // Failed message processing with retry and DLQ
   if (!processMessage(message)) {
     await producer.send('dlq-topic', {
       ...message,
       failureReason: 'Processing failed',
       originalTopic: 'orders-topic'
     });
   }
   ```

### Real-time Analytics Pipeline

```typescript
// High-throughput data ingestion
const analyticsMessages = events.map(event => [
  'analytics-topic',
  { ...event, timestamp: Date.now() },
  event.userId  // Partition by user for ordering
]);

await producer.sendBatch(analyticsMessages);
```

### Microservices Communication

```typescript
// Service-to-service messaging
await producer.send('user-service-events', {
  eventType: 'UserRegistered',
  userId: 'user123',
  timestamp: new Date().toISOString()
}, 'user123', {
  headers: {
    'source-service': 'auth-service',
    'correlation-id': correlationId,
    'event-version': '1.0'
  }
});
```

## Configuration Examples

### High-Throughput Producer
```typescript
const config = new Config({
  bootstrapServers: 'localhost:9092',
  acks: 'all',
  retries: 3,
  batchSize: 32768,      // 32KB batches
  lingerMs: 5,           // Wait 5ms for batching
  compressionType: 'gzip', // Compress for network efficiency
  maxInFlightRequests: 5   // Pipeline for throughput
});
```

### Low-Latency Producer
```typescript
const config = new Config({
  bootstrapServers: 'localhost:9092',
  acks: 1,               // Leader ack only
  retries: 0,            // No retries for speed
  batchSize: 0,          // No batching
  lingerMs: 0,           // Send immediately
  compressionType: 'none' // No compression overhead
});
```

### Reliable Consumer
```typescript
const config = new Config({
  bootstrapServers: 'localhost:9092',
  groupId: 'reliable-consumers',
  autoOffsetReset: 'earliest',
  enableAutoCommit: false,  // Manual offset control
  sessionTimeout: 30000,    // 30s session timeout
  heartbeatInterval: 10000, // 10s heartbeat
  maxPollRecords: 100       // Batch size limit
});
```

## Performance Tips

1. **Use Batch Operations**: Batch sending is significantly more efficient than individual sends
2. **Optimize Configuration**: Tune `batchSize`, `lingerMs`, and `compressionType` for your use case
3. **Partition Strategically**: Use meaningful keys to ensure even distribution
4. **Monitor Consumer Lag**: Track offset lag to ensure consumers keep up
5. **Handle Backpressure**: Implement flow control in high-throughput scenarios

## Error Handling Patterns

```typescript
// Exponential backoff retry
async function sendWithRetry(producer, topic, message, maxRetries = 3) {
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

// Circuit breaker pattern
class CircuitBreaker {
  private failures = 0;
  private readonly threshold = 5;
  private state = 'CLOSED'; // CLOSED, OPEN, HALF_OPEN

  async execute(operation: () => Promise<any>) {
    if (this.state === 'OPEN') {
      throw new Error('Circuit breaker is OPEN');
    }

    try {
      const result = await operation();
      this.reset();
      return result;
    } catch (error) {
      this.recordFailure();
      throw error;
    }
  }

  private recordFailure() {
    this.failures++;
    if (this.failures >= this.threshold) {
      this.state = 'OPEN';
      setTimeout(() => this.state = 'HALF_OPEN', 60000); // 1 minute
    }
  }

  private reset() {
    this.failures = 0;
    this.state = 'CLOSED';
  }
}
```

## Monitoring and Observability

```typescript
// Custom metrics collection
class KafkaMetrics {
  private messagesSent = 0;
  private messagesReceived = 0;
  private errors = 0;

  recordSent() { this.messagesSent++; }
  recordReceived() { this.messagesReceived++; }
  recordError() { this.errors++; }

  getStats() {
    return {
      sent: this.messagesSent,
      received: this.messagesReceived,
      errors: this.errors,
      successRate: this.messagesSent > 0 ?
        (this.messagesSent - this.errors) / this.messagesSent : 0
    };
  }
}

// Usage with producers/consumers
const metrics = new KafkaMetrics();

// Wrap producer sends
const result = await producer.send(topic, message);
metrics.recordSent();

// Wrap consumer polls
const message = await consumer.poll(timeout);
if (message) metrics.recordReceived();
```

## Testing Your Kafka Applications

```typescript
// Integration test example
describe('Kafka Integration', () => {
  let producer: Producer;
  let consumer: Consumer;

  beforeEach(async () => {
    const config = new Config({ bootstrapServers: 'localhost:9092' });
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
    expect(receivedMessage.value).toEqual(testMessage);
  });
});
```

## Deployment Considerations

1. **Resource Management**: Always close producers and consumers properly
2. **Connection Pooling**: Reuse producer instances across your application
3. **Graceful Shutdown**: Implement proper cleanup in signal handlers
4. **Health Checks**: Monitor connection status and consumer lag
5. **Configuration Management**: Use environment variables for broker URLs and credentials

```typescript
// Graceful shutdown example
process.on('SIGTERM', async () => {
  console.log('Shutting down gracefully...');
  await producer.close();
  await consumer.close();
  process.exit(0);
});
```

## Next Steps

1. Run the examples to get familiar with the library
2. Modify examples to match your specific use cases
3. Check out the [performance tests](../tests/performance/) for benchmarking
4. Review the [integration tests](../tests/integration/) for more complex scenarios
5. Read the main [README.md](../README.md) for complete API documentation

For questions or issues, please refer to the main documentation or create an issue in the project repository.