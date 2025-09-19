# ApnaMart Kafka TypeScript - Complete Low-Level Design & Specification

## 1. Project Overview & Architecture

### 1.1 Project Structure
```
apnamart-kafka-typescript/
├── src/
│   ├── index.ts                 # Public API exports
│   ├── client.ts                # Main implementation (single module)
│   ├── types.ts                 # TypeScript type definitions
│   └── errors.ts                # Custom error classes
├── tests/
│   ├── unit/                    # Unit tests with mocks
│   ├── integration/             # Integration tests requiring Kafka
│   ├── performance/             # Benchmarks and performance tests
│   └── fixtures/                # Test fixtures and utilities
├── examples/
│   ├── basic/                   # Getting started examples
│   ├── advanced/                # Complex patterns
│   └── production/              # Deployment guides
├── package.json                 # Node.js project configuration
├── tsconfig.json                # TypeScript configuration
├── jest.config.js               # Testing configuration
└── README.md                    # Documentation
```

### 1.2 Core Dependencies
```json
{
  "dependencies": {
    "kafkajs": "^2.2.4"
  },
  "devDependencies": {
    "@types/node": "^20.0.0",
    "typescript": "^5.0.0",
    "jest": "^29.0.0",
    "@types/jest": "^29.0.0",
    "ts-jest": "^29.0.0",
    "eslint": "^8.0.0",
    "@typescript-eslint/eslint-plugin": "^6.0.0",
    "@typescript-eslint/parser": "^6.0.0"
  }
}
```

## 2. Type Definitions (src/types.ts)

```typescript
export interface KafkaConfig {
  bootstrapServers?: string;
  acks?: 'all' | 0 | 1 | -1;
  retries?: number;
  compressionType?: 'gzip' | 'snappy' | 'lz4' | 'zstd' | null;
  batchSize?: number;
  lingerMs?: number;

  // Consumer-specific
  groupId?: string;
  autoOffsetReset?: 'latest' | 'earliest';
  enableAutoCommit?: boolean;

  // Additional KafkaJS options
  clientId?: string;
  connectionTimeout?: number;
  authenticationTimeout?: number;
  requestTimeout?: number;

  // SASL configuration
  sasl?: {
    mechanism: 'plain' | 'scram-sha-256' | 'scram-sha-512';
    username: string;
    password: string;
  };

  // SSL configuration
  ssl?: boolean | {
    rejectUnauthorized?: boolean;
    ca?: string[];
    key?: string;
    cert?: string;
  };
}

export interface MessageHeaders {
  [key: string]: Buffer | string | undefined;
}

export interface MessageMetadata {
  topic: string;
  partition: number;
  offset: string;
  key?: Buffer | string | null;
  timestamp: string;
  headers: MessageHeaders;
}

export interface SendResult {
  success: boolean;
  topic?: string;
  partition?: number;
  offset?: string;
  error?: string;
}

export interface BatchMessage {
  topic: string;
  value: any;
  key?: any;
  partition?: number;
  headers?: MessageHeaders;
}

export type MessageInput =
  | [string, any]                    // [topic, value]
  | [string, any, any]               // [topic, value, key]
  | BatchMessage;                    // Object format

// Environment variables
export interface EnvironmentConfig {
  KAFKA_BOOTSTRAP_SERVERS?: string;
  KAFKA_SECURITY_PROTOCOL?: string;
  KAFKA_SASL_USERNAME?: string;
  KAFKA_SASL_PASSWORD?: string;
  KAFKA_CLIENT_ID?: string;
}
```

## 3. Error Classes (src/errors.ts)

```typescript
export class KafkaError extends Error {
  constructor(message: string, public readonly cause?: Error) {
    super(message);
    this.name = 'KafkaError';
    if (cause) {
      this.stack = `${this.stack}\nCaused by: ${cause.stack}`;
    }
  }
}

export class ProducerError extends KafkaError {
  constructor(message: string, cause?: Error) {
    super(message, cause);
    this.name = 'ProducerError';
  }
}

export class ConsumerError extends KafkaError {
  constructor(message: string, cause?: Error) {
    super(message, cause);
    this.name = 'ConsumerError';
  }
}

export class TransactionError extends KafkaError {
  constructor(message: string, cause?: Error) {
    super(message, cause);
    this.name = 'TransactionError';
  }
}

export class SerializationError extends KafkaError {
  constructor(message: string, cause?: Error) {
    super(message, cause);
    this.name = 'SerializationError';
  }
}

export class ConfigurationError extends KafkaError {
  constructor(message: string, cause?: Error) {
    super(message, cause);
    this.name = 'ConfigurationError';
  }
}
```

## 4. Core Implementation (src/client.ts)

### 4.1 Configuration Class

```typescript
import { Kafka, KafkaConfig as KafkaJSConfig } from 'kafkajs';
import { KafkaConfig, EnvironmentConfig } from './types';
import { ConfigurationError } from './errors';

export class Config {
  public readonly bootstrapServers: string;
  public readonly acks: 'all' | 0 | 1 | -1;
  public readonly retries: number;
  public readonly compressionType: 'gzip' | 'snappy' | 'lz4' | 'zstd' | undefined;
  public readonly batchSize: number;
  public readonly lingerMs: number;

  // Consumer-specific
  public readonly groupId: string;
  public readonly autoOffsetReset: 'latest' | 'earliest';
  public readonly enableAutoCommit: boolean;

  // Additional options
  public readonly clientId: string;
  public readonly connectionTimeout: number;
  public readonly authenticationTimeout: number;
  public readonly requestTimeout: number;

  private readonly config: KafkaConfig;

  constructor(config: KafkaConfig = {}) {
    // Load environment variables
    const env = process.env as EnvironmentConfig;

    // Set defaults with environment fallbacks
    this.bootstrapServers = config.bootstrapServers ?? env.KAFKA_BOOTSTRAP_SERVERS ?? 'localhost:9092';
    this.acks = config.acks ?? 'all';
    this.retries = config.retries ?? 3;
    this.compressionType = config.compressionType;
    this.batchSize = config.batchSize ?? 16384;
    this.lingerMs = config.lingerMs ?? 0;

    // Consumer settings
    this.groupId = config.groupId ?? 'default-group';
    this.autoOffsetReset = config.autoOffsetReset ?? 'latest';
    this.enableAutoCommit = config.enableAutoCommit ?? true;

    // Connection settings
    this.clientId = config.clientId ?? env.KAFKA_CLIENT_ID ?? 'apnamart-kafka-client';
    this.connectionTimeout = config.connectionTimeout ?? 1000;
    this.authenticationTimeout = config.authenticationTimeout ?? 1000;
    this.requestTimeout = config.requestTimeout ?? 30000;

    this.config = config;

    // Validation
    this.validate();
  }

  private validate(): void {
    if (!this.bootstrapServers) {
      throw new ConfigurationError('Bootstrap servers must be specified');
    }

    if (this.retries < 0) {
      throw new ConfigurationError('Retries must be non-negative');
    }

    if (this.batchSize <= 0) {
      throw new ConfigurationError('Batch size must be positive');
    }
  }

  public toKafkaJSConfig(): KafkaJSConfig {
    const brokers = this.bootstrapServers.split(',').map(s => s.trim());

    const config: KafkaJSConfig = {
      clientId: this.clientId,
      brokers,
      connectionTimeout: this.connectionTimeout,
      authenticationTimeout: this.authenticationTimeout,
      requestTimeout: this.requestTimeout,
    };

    // Add SASL if configured
    if (this.config.sasl) {
      config.sasl = this.config.sasl;
    }

    // Add SSL if configured
    if (this.config.ssl) {
      config.ssl = this.config.ssl;
    }

    return config;
  }

  public toProducerConfig() {
    return {
      allowAutoTopicCreation: false,
      transactionTimeout: 30000,
      maxInFlightRequests: 5,
      idempotent: true,
      retry: {
        initialRetryTime: 300,
        retries: this.retries,
      },
    };
  }

  public toConsumerConfig() {
    return {
      groupId: this.groupId,
      sessionTimeout: 30000,
      rebalanceTimeout: 60000,
      heartbeatInterval: 3000,
      metadataMaxAge: 300000,
      allowAutoTopicCreation: false,
      retry: {
        initialRetryTime: 300,
        retries: this.retries,
      },
    };
  }
}
```

### 4.2 Serialization Functions

```typescript
export function serialize(data: any): Buffer {
  if (Buffer.isBuffer(data)) {
    return data;
  }

  if (typeof data === 'string') {
    return Buffer.from(data, 'utf-8');
  }

  try {
    return Buffer.from(JSON.stringify(data), 'utf-8');
  } catch (error) {
    throw new SerializationError(`Failed to serialize data: ${error.message}`, error);
  }
}

export function deserialize(data: Buffer | null): any {
  if (!data) {
    return null;
  }

  try {
    const str = data.toString('utf-8');
    return JSON.parse(str);
  } catch (jsonError) {
    try {
      return data.toString('utf-8');
    } catch (stringError) {
      return data;
    }
  }
}
```

### 4.3 Message Class

```typescript
import { EachMessagePayload } from 'kafkajs';
import { MessageMetadata, MessageHeaders } from './types';

export class Message {
  public readonly topic: string;
  public readonly partition: number;
  public readonly offset: string;
  public readonly key: any;
  public readonly value: any;
  public readonly timestamp: string;
  public readonly headers: MessageHeaders;

  constructor(payload: EachMessagePayload) {
    this.topic = payload.topic;
    this.partition = payload.partition;
    this.offset = payload.message.offset;
    this.key = payload.message.key ? deserialize(payload.message.key) : null;
    this.value = deserialize(payload.message.value);
    this.timestamp = payload.message.timestamp;
    this.headers = this.convertHeaders(payload.message.headers);
  }

  private convertHeaders(headers: any): MessageHeaders {
    if (!headers) return {};

    const result: MessageHeaders = {};
    for (const [key, value] of Object.entries(headers)) {
      result[key] = Buffer.isBuffer(value) ? value : String(value);
    }
    return result;
  }

  public toString(): string {
    return `Message(topic='${this.topic}', partition=${this.partition}, offset=${this.offset})`;
  }

  public toJSON() {
    return {
      topic: this.topic,
      partition: this.partition,
      offset: this.offset,
      key: this.key,
      value: this.value,
      timestamp: this.timestamp,
      headers: this.headers,
    };
  }
}
```

### 4.4 Producer Class

```typescript
import { Kafka, Producer as KafkaJSProducer, RecordMetadata } from 'kafkajs';
import { Config } from './Config';
import { ProducerError } from './errors';
import { serialize, MessageInput, SendResult, BatchMessage } from './types';

export class Producer {
  private kafka: Kafka | null = null;
  private producer: KafkaJSProducer | null = null;
  private closed = false;

  constructor(private config: Config = new Config()) {}

  private async getProducer(): Promise<KafkaJSProducer> {
    if (this.producer === null) {
      if (this.closed) {
        throw new ProducerError('Producer is closed');
      }

      this.kafka = new Kafka(this.config.toKafkaJSConfig());
      this.producer = this.kafka.producer(this.config.toProducerConfig());

      try {
        await this.producer.connect();
      } catch (error) {
        throw new ProducerError(`Failed to connect producer: ${error.message}`, error);
      }
    }

    return this.producer;
  }

  public async send(topic: string, value: any, key?: any, options: {
    partition?: number;
    headers?: Record<string, string>;
    timestamp?: string;
  } = {}): Promise<RecordMetadata[]> {
    if (this.closed) {
      throw new ProducerError('Producer is closed');
    }

    if (!topic) {
      throw new ProducerError('Topic name cannot be empty');
    }

    try {
      const producer = await this.getProducer();

      const message = {
        key: key !== undefined ? serialize(key) : undefined,
        value: serialize(value),
        partition: options.partition,
        headers: options.headers,
        timestamp: options.timestamp,
      };

      const result = await producer.send({
        topic,
        messages: [message],
      });

      return result;
    } catch (error) {
      this.handleProducerError(error, topic);
    }
  }

  public async sendBatch(messages: MessageInput[]): Promise<SendResult[]> {
    if (this.closed) {
      throw new ProducerError('Producer is closed');
    }

    const producer = await this.getProducer();
    const results: SendResult[] = [];

    // Group messages by topic for efficient sending
    const messagesByTopic = new Map<string, any[]>();

    for (let i = 0; i < messages.length; i++) {
      try {
        const parsed = this.parseMessageInput(messages[i]);

        if (!messagesByTopic.has(parsed.topic)) {
          messagesByTopic.set(parsed.topic, []);
        }

        messagesByTopic.get(parsed.topic)!.push({
          key: parsed.key !== undefined ? serialize(parsed.key) : undefined,
          value: serialize(parsed.value),
          partition: parsed.partition,
          headers: parsed.headers,
          originalIndex: i,
        });
      } catch (error) {
        results[i] = {
          success: false,
          error: `Failed to process message: ${error.message}`,
        };
      }
    }

    // Send messages topic by topic
    for (const [topic, topicMessages] of messagesByTopic) {
      try {
        const metadata = await producer.send({
          topic,
          messages: topicMessages,
        });

        // Map results back to original positions
        for (let j = 0; j < metadata.length; j++) {
          const originalIndex = topicMessages[j].originalIndex;
          results[originalIndex] = {
            success: true,
            topic,
            partition: metadata[j].partition,
            offset: metadata[j].offset,
          };
        }
      } catch (error) {
        // Mark all messages for this topic as failed
        for (const msg of topicMessages) {
          results[msg.originalIndex] = {
            success: false,
            error: `Failed to send to topic ${topic}: ${error.message}`,
          };
        }
      }
    }

    return results;
  }

  private parseMessageInput(input: MessageInput): BatchMessage {
    if (Array.isArray(input)) {
      if (input.length === 2) {
        return { topic: input[0], value: input[1] };
      } else if (input.length === 3) {
        return { topic: input[0], value: input[1], key: input[2] };
      } else {
        throw new Error('Array format must be [topic, value] or [topic, value, key]');
      }
    } else if (typeof input === 'object' && input !== null) {
      if (!input.topic || input.value === undefined) {
        throw new Error('Object format must include topic and value');
      }
      return input as BatchMessage;
    } else {
      throw new Error('Message must be array or object format');
    }
  }

  private handleProducerError(error: any, topic?: string): never {
    const message = error.message || String(error);

    if (message.includes('Request timed out')) {
      throw new ProducerError(
        `Message delivery timed out. Check Kafka connection: ${message}`
      );
    }

    if (message.includes('Unknown topic')) {
      throw new ProducerError(
        `Topic '${topic}' does not exist or is not accessible: ${message}`
      );
    }

    if (message.includes('NOT_LEADER_FOR_PARTITION')) {
      throw new ProducerError(
        `Partition leadership changed. Message will be retried: ${message}`
      );
    }

    throw new ProducerError(
      `Failed to send message${topic ? ` to topic '${topic}'` : ''}: ${message}`,
      error
    );
  }

  public async flush(timeout = 10000): Promise<void> {
    if (this.producer) {
      try {
        await this.producer.send({
          topic: '__flush__',
          messages: [],
        });
      } catch {
        // Ignore flush errors
      }
    }
  }

  public async close(): Promise<void> {
    if (!this.closed) {
      this.closed = true;

      if (this.producer) {
        try {
          await this.producer.disconnect();
        } catch (error) {
          // Log but don't throw on close errors
          console.warn('Error closing producer:', error.message);
        } finally {
          this.producer = null;
          this.kafka = null;
        }
      }
    }
  }

  // Async disposable support (Node.js 20+)
  public async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }
}
```

### 4.5 Consumer Class

```typescript
import { Kafka, Consumer as KafkaJSConsumer, EachMessagePayload } from 'kafkajs';
import { Config } from './Config';
import { ConsumerError } from './errors';
import { Message } from './Message';

export class Consumer {
  private kafka: Kafka | null = null;
  private consumer: KafkaJSConsumer | null = null;
  private closed = false;
  private subscribed = false;

  constructor(
    private topics: string | string[] = [],
    private config: Config = new Config()
  ) {
    this.topics = Array.isArray(topics) ? topics : [topics];
  }

  private async getConsumer(): Promise<KafkaJSConsumer> {
    if (this.consumer === null) {
      if (this.closed) {
        throw new ConsumerError('Consumer is closed');
      }

      this.kafka = new Kafka(this.config.toKafkaJSConfig());
      this.consumer = this.kafka.consumer(this.config.toConsumerConfig());

      try {
        await this.consumer.connect();

        if (this.topics.length > 0) {
          await this.consumer.subscribe({
            topics: this.topics,
            fromBeginning: this.config.autoOffsetReset === 'earliest'
          });
          this.subscribed = true;
        }
      } catch (error) {
        throw new ConsumerError(`Failed to connect consumer: ${error.message}`, error);
      }
    }

    return this.consumer;
  }

  public async poll(timeout = 1000): Promise<Message | null> {
    if (this.closed) {
      throw new ConsumerError('Consumer is closed');
    }

    const consumer = await this.getConsumer();

    if (!this.subscribed) {
      throw new ConsumerError('Consumer is not subscribed to any topics');
    }

    try {
      return new Promise<Message | null>((resolve, reject) => {
        const timer = setTimeout(() => {
          resolve(null);
        }, timeout);

        consumer.run({
          eachMessage: async (payload: EachMessagePayload) => {
            clearTimeout(timer);
            try {
              const message = new Message(payload);
              resolve(message);
            } catch (error) {
              reject(new ConsumerError(`Failed to process message: ${error.message}`, error));
            }

            // Stop the consumer after processing one message
            await consumer.pause([{ topic: payload.topic }]);
          },
        }).catch(reject);
      });
    } catch (error) {
      this.handleConsumerError(error);
    }
  }

  public async pollBatch(size = 100, timeout = 10000): Promise<Message[]> {
    const messages: Message[] = [];
    const startTime = Date.now();

    while (messages.length < size && (Date.now() - startTime) < timeout) {
      const remaining = timeout - (Date.now() - startTime);
      if (remaining <= 0) break;

      const message = await this.poll(Math.min(remaining, 1000));
      if (message) {
        messages.push(message);
      } else {
        break;
      }
    }

    return messages;
  }

  public async commit(message?: Message): Promise<void> {
    if (this.closed) {
      throw new ConsumerError('Consumer is closed');
    }

    const consumer = await this.getConsumer();

    try {
      if (message) {
        await consumer.commitOffsets([
          {
            topic: message.topic,
            partition: message.partition,
            offset: (parseInt(message.offset) + 1).toString(),
          },
        ]);
      } else {
        // Commit current offsets
        await consumer.commitOffsets([]);
      }
    } catch (error) {
      throw new ConsumerError(`Failed to commit offsets: ${error.message}`, error);
    }
  }

  public async seek(topic: string, partition: number, offset: string): Promise<void> {
    if (this.closed) {
      throw new ConsumerError('Consumer is closed');
    }

    const consumer = await this.getConsumer();

    try {
      consumer.seek({ topic, partition, offset });
    } catch (error) {
      throw new ConsumerError(`Failed to seek: ${error.message}`, error);
    }
  }

  private handleConsumerError(error: any): never {
    const message = error.message || String(error);

    if (message.includes('Unknown topic')) {
      throw new ConsumerError(`Unknown topic or partition: ${message}`);
    }

    if (message.includes('Connection failed')) {
      throw new ConsumerError(`Transport error (check Kafka connection): ${message}`);
    }

    if (message.includes('SASL authentication failed')) {
      throw new ConsumerError(`Authentication failed: ${message}`);
    }

    if (message.includes('Not authorized')) {
      throw new ConsumerError(`Authorization failed: ${message}`);
    }

    throw new ConsumerError(`Consumer error: ${message}`, error);
  }

  public async close(): Promise<void> {
    if (!this.closed) {
      this.closed = true;

      if (this.consumer) {
        try {
          await this.consumer.disconnect();
        } catch (error) {
          console.warn('Error closing consumer:', error.message);
        } finally {
          this.consumer = null;
          this.kafka = null;
        }
      }
    }
  }

  // Iterator support
  public async *[Symbol.asyncIterator](): AsyncIterableIterator<Message> {
    while (!this.closed) {
      const message = await this.poll(1000);
      if (message) {
        yield message;
      }
    }
  }

  // Async disposable support
  public async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }
}
```

### 4.6 TransactionalProducer Class

```typescript
import { Kafka, Producer as KafkaJSProducer } from 'kafkajs';
import { Producer } from './Producer';
import { Config } from './Config';
import { TransactionError } from './errors';
import { MessageInput } from './types';

export class TransactionalProducer extends Producer {
  private inTransaction = false;
  private initialized = false;

  constructor(
    private transactionalId: string,
    config: Config = new Config()
  ) {
    // Add transactional configuration
    const txConfig = new Config({
      ...config,
      // KafkaJS handles transactional configuration differently
    });

    super(txConfig);
  }

  protected async getProducer(): Promise<KafkaJSProducer> {
    if (this.producer === null) {
      if (this.closed) {
        throw new TransactionError('Producer is closed');
      }

      this.kafka = new Kafka(this.config.toKafkaJSConfig());
      this.producer = this.kafka.producer({
        ...this.config.toProducerConfig(),
        transactionalId: this.transactionalId,
        maxInFlightRequests: 1,
        idempotent: true,
      });

      try {
        await this.producer.connect();

        if (!this.initialized) {
          // KafkaJS handles transaction initialization automatically
          this.initialized = true;
        }
      } catch (error) {
        throw new TransactionError(`Failed to connect transactional producer: ${error.message}`, error);
      }
    }

    return this.producer;
  }

  public async begin(): Promise<void> {
    if (this.inTransaction) {
      throw new TransactionError('Transaction already in progress');
    }

    const producer = await this.getProducer();

    try {
      const transaction = await producer.transaction();
      this.currentTransaction = transaction;
      this.inTransaction = true;
    } catch (error) {
      throw new TransactionError(`Failed to begin transaction: ${error.message}`, error);
    }
  }

  public async commit(): Promise<void> {
    if (!this.inTransaction) {
      throw new TransactionError('No transaction in progress');
    }

    try {
      if (this.currentTransaction) {
        await this.currentTransaction.commit();
      }
      this.inTransaction = false;
      this.currentTransaction = null;
    } catch (error) {
      throw new TransactionError(`Failed to commit transaction: ${error.message}`, error);
    }
  }

  public async abort(): Promise<void> {
    if (!this.inTransaction) {
      throw new TransactionError('No transaction in progress');
    }

    try {
      if (this.currentTransaction) {
        await this.currentTransaction.abort();
      }
      this.inTransaction = false;
      this.currentTransaction = null;
    } catch (error) {
      throw new TransactionError(`Failed to abort transaction: ${error.message}`, error);
    }
  }

  public async sendTransactional(
    topic: string,
    value: any,
    key?: any,
    options: {
      partition?: number;
      headers?: Record<string, string>;
    } = {}
  ): Promise<void> {
    if (!this.inTransaction) {
      throw new TransactionError('No active transaction. Call begin() first.');
    }

    if (!this.currentTransaction) {
      throw new TransactionError('Transaction is in invalid state');
    }

    try {
      await this.currentTransaction.send({
        topic,
        messages: [{
          key: key !== undefined ? serialize(key) : undefined,
          value: serialize(value),
          partition: options.partition,
          headers: options.headers,
        }],
      });
    } catch (error) {
      throw new TransactionError(`Failed to send transactional message: ${error.message}`, error);
    }
  }

  public async sendBatchTransactional(messages: MessageInput[]): Promise<void> {
    if (this.inTransaction) {
      throw new TransactionError('Transaction already in progress');
    }

    await this.begin();

    try {
      for (const msg of messages) {
        const parsed = this.parseMessageInput(msg);
        await this.sendTransactional(
          parsed.topic,
          parsed.value,
          parsed.key,
          {
            partition: parsed.partition,
            headers: parsed.headers
          }
        );
      }

      await this.commit();
    } catch (error) {
      await this.abort();
      throw new TransactionError(`Transaction failed: ${error.message}`, error);
    }
  }

  public async close(): Promise<void> {
    if (this.inTransaction) {
      try {
        await this.abort();
      } catch {
        // Ignore abort errors during close
      }
    }

    await super.close();
  }

  // Enhanced async disposable support
  public async [Symbol.asyncDispose](): Promise<void> {
    if (this.inTransaction) {
      try {
        await this.commit();
      } catch {
        await this.abort();
      }
    }
    await this.close();
  }
}
```

## 5. Convenience Functions (src/client.ts)

```typescript
export async function send(
  topic: string,
  value: any,
  key?: any,
  options: {
    servers?: string;
    config?: KafkaConfig;
  } = {}
): Promise<void> {
  const config = new Config({
    bootstrapServers: options.servers ?? 'localhost:9092',
    ...options.config,
  });

  await using producer = new Producer(config);
  await producer.send(topic, value, key);
}

export async function* consume(
  topics: string | string[],
  options: {
    servers?: string;
    groupId?: string;
    config?: KafkaConfig;
  } = {}
): AsyncIterableIterator<Message> {
  const config = new Config({
    bootstrapServers: options.servers ?? 'localhost:9092',
    groupId: options.groupId ?? 'default',
    ...options.config,
  });

  await using consumer = new Consumer(topics, config);

  for await (const message of consumer) {
    yield message;
  }
}
```

## 6. Public API (src/index.ts)

```typescript
// Core classes
export { Config } from './client';
export { Producer } from './client';
export { Consumer } from './client';
export { TransactionalProducer } from './client';
export { Message } from './client';

// Convenience functions
export { send, consume } from './client';

// Serialization utilities
export { serialize, deserialize } from './client';

// Error classes
export {
  KafkaError,
  ProducerError,
  ConsumerError,
  TransactionError,
  SerializationError,
  ConfigurationError,
} from './errors';

// Type definitions
export type {
  KafkaConfig,
  MessageHeaders,
  MessageMetadata,
  SendResult,
  BatchMessage,
  MessageInput,
  EnvironmentConfig,
} from './types';

// Default export for convenience
export default {
  Config,
  Producer,
  Consumer,
  TransactionalProducer,
  Message,
  send,
  consume,
  serialize,
  deserialize,
};
```

## 7. Configuration Files

### 7.1 TypeScript Configuration (tsconfig.json)

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "lib": ["ES2022"],
    "module": "CommonJS",
    "declaration": true,
    "declarationMap": true,
    "sourceMap": true,
    "outDir": "./dist",
    "rootDir": "./src",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true,
    "moduleResolution": "node",
    "allowSyntheticDefaultImports": true,
    "experimentalDecorators": true,
    "emitDecoratorMetadata": true
  },
  "include": [
    "src/**/*"
  ],
  "exclude": [
    "node_modules",
    "dist",
    "tests"
  ]
}
```

### 7.2 Jest Configuration (jest.config.js)

```javascript
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  roots: ['<rootDir>/src', '<rootDir>/tests'],
  testMatch: ['**/__tests__/**/*.ts', '**/?(*.)+(spec|test).ts'],
  collectCoverageFrom: [
    'src/**/*.ts',
    '!src/**/*.d.ts',
  ],
  coverageDirectory: 'coverage',
  coverageReporters: ['text', 'lcov', 'html'],
  coverageThreshold: {
    global: {
      branches: 90,
      functions: 90,
      lines: 90,
      statements: 90,
    },
  },
  setupFilesAfterEnv: ['<rootDir>/tests/setup.ts'],
  testTimeout: 30000,
  testPathIgnorePatterns: ['/node_modules/', '/dist/'],
  moduleFileExtensions: ['ts', 'js', 'json'],
};
```

## 8. Testing Specifications

### 8.1 Test Setup (tests/setup.ts)

```typescript
import { jest } from '@jest/globals';

// Global test timeout
jest.setTimeout(30000);

// Mock environment variables
process.env.KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092';

// Global test utilities
declare global {
  var generateTestTopic: () => string;
  var generateTestGroup: () => string;
}

global.generateTestTopic = () =>
  `test-topic-${Math.random().toString(36).substring(7)}-${Date.now()}`;

global.generateTestGroup = () =>
  `test-group-${Math.random().toString(36).substring(7)}-${Date.now()}`;
```

### 8.2 Unit Test Examples

```typescript
// tests/unit/config.test.ts
import { Config, ConfigurationError } from '../../src';

describe('Config', () => {
  beforeEach(() => {
    // Clear environment variables
    delete process.env.KAFKA_BOOTSTRAP_SERVERS;
  });

  describe('constructor', () => {
    it('should use default values', () => {
      const config = new Config();

      expect(config.bootstrapServers).toBe('localhost:9092');
      expect(config.acks).toBe('all');
      expect(config.retries).toBe(3);
      expect(config.groupId).toBe('default-group');
    });

    it('should use environment variables', () => {
      process.env.KAFKA_BOOTSTRAP_SERVERS = 'kafka1:9092,kafka2:9092';

      const config = new Config();
      expect(config.bootstrapServers).toBe('kafka1:9092,kafka2:9092');
    });

    it('should validate configuration', () => {
      expect(() => new Config({ retries: -1 }))
        .toThrow(ConfigurationError);
    });
  });

  describe('toKafkaJSConfig', () => {
    it('should convert to KafkaJS format', () => {
      const config = new Config({
        bootstrapServers: 'broker1:9092,broker2:9092',
        clientId: 'test-client',
      });

      const kafkaConfig = config.toKafkaJSConfig();

      expect(kafkaConfig.brokers).toEqual(['broker1:9092', 'broker2:9092']);
      expect(kafkaConfig.clientId).toBe('test-client');
    });
  });
});
```

### 8.3 Performance Test Requirements

```typescript
// tests/performance/benchmark.test.ts
describe('Performance Benchmarks', () => {
  const BENCHMARK_DURATION = parseInt(process.env.BENCHMARK_DURATION || '10') * 1000;
  const TARGET_THROUGHPUT = {
    PRODUCER: 30000, // messages/second
    CONSUMER: 25000, // messages/second
  };

  it('should meet producer throughput targets', async () => {
    const config = new Config();
    const producer = new Producer(config);

    const startTime = Date.now();
    let messagesSent = 0;

    while (Date.now() - startTime < BENCHMARK_DURATION) {
      await producer.sendBatch(
        Array(1000).fill(null).map((_, i) => [
          'benchmark-topic',
          { id: i, timestamp: Date.now() }
        ])
      );
      messagesSent += 1000;
    }

    const throughput = messagesSent / (BENCHMARK_DURATION / 1000);
    expect(throughput).toBeGreaterThan(TARGET_THROUGHPUT.PRODUCER);

    await producer.close();
  });

  it('should maintain low latency', async () => {
    const latencies: number[] = [];
    const producer = new Producer();

    for (let i = 0; i < 1000; i++) {
      const start = process.hrtime.bigint();
      await producer.send('latency-test', { id: i });
      const end = process.hrtime.bigint();

      latencies.push(Number(end - start) / 1_000_000); // Convert to ms
    }

    const avgLatency = latencies.reduce((a, b) => a + b) / latencies.length;
    const p95Latency = latencies.sort()[Math.floor(latencies.length * 0.95)];

    expect(avgLatency).toBeLessThan(5); // < 5ms average
    expect(p95Latency).toBeLessThan(10); // < 10ms P95

    await producer.close();
  });
});
```

## 9. Usage Examples

### 9.1 Basic Usage

```typescript
import { send, consume, Producer, Consumer } from 'apnamart-kafka-typescript';

// Quick functions
await send('my-topic', { message: 'Hello World' });

for await (const message of consume('my-topic')) {
  console.log(message.value);
  break;
}

// Class-based usage
await using producer = new Producer();
await producer.send('events', { event: 'user_login', userId: 123 });

await using consumer = new Consumer(['events']);
const message = await consumer.poll();
if (message) {
  console.log(message.value);
  await consumer.commit(message);
}
```

### 9.2 Transactional Usage

```typescript
import { TransactionalProducer } from 'apnamart-kafka-typescript';

await using txProducer = new TransactionalProducer('my-tx-id');

await txProducer.begin();
try {
  await txProducer.sendTransactional('orders', { orderId: 1, status: 'created' });
  await txProducer.sendTransactional('inventory', { productId: 1, quantity: -1 });
  await txProducer.commit();
} catch (error) {
  await txProducer.abort();
  throw error;
}
```

## 10. Implementation Priorities

### Phase 1 - Core Implementation
1. Basic Config class with environment variable support
2. Serialization/deserialization functions
3. Message class for metadata handling
4. Basic Producer with send() method
5. Basic Consumer with poll() method
6. Error handling hierarchy

### Phase 2 - Advanced Features
1. Batch operations (sendBatch, pollBatch)
2. TransactionalProducer implementation
3. Connection pooling and lazy initialization
4. Comprehensive error handling
5. Iterator support for Consumer

### Phase 3 - Performance & Polish
1. Performance optimizations
2. Connection reuse strategies
3. Comprehensive test suite
4. Documentation and examples
5. Benchmarking and validation

## 11. Performance Requirements

### Throughput Targets
- **Producer**: >30,000 messages/second
- **Consumer**: >25,000 messages/second
- **Batch Operations**: >50,000 messages/second

### Latency Targets
- **Average Latency**: <5ms
- **P95 Latency**: <10ms
- **P99 Latency**: <25ms

### Memory Constraints
- **Per Client Instance**: <20MB
- **Connection Pooling**: Reuse connections across instances
- **Message Buffering**: Configurable limits with backpressure

## 12. Key Implementation Notes

### 12.1 Connection Management
- Implement lazy initialization for all clients
- Reuse Kafka instances across Producer/Consumer instances where possible
- Proper connection cleanup on close/dispose

### 12.2 Error Handling Strategy
- Map KafkaJS errors to custom error hierarchy
- Provide meaningful error messages with context
- Implement retry logic with exponential backoff

### 12.3 TypeScript Best Practices
- Use strict TypeScript configuration
- Provide comprehensive type definitions
- Support async/await patterns throughout
- Implement async disposable pattern for resource cleanup

### 12.4 KafkaJS Integration
- Replace confluent-kafka with kafkajs for Node.js compatibility
- Map configuration options between libraries
- Adapt transactional producer to KafkaJS transaction API
- Handle KafkaJS-specific connection and error patterns

This comprehensive specification provides all the details needed to implement the ApnaMart Kafka library in TypeScript with complete feature parity to the Python version, adapted for Node.js ecosystem and TypeScript best practices.