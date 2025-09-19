import {
  type EachMessagePayload,
  Kafka,
  type KafkaConfig as KafkaJSConfig,
  type Consumer as KafkaJSConsumer,
  type Producer as KafkaJSProducer,
  type RecordMetadata,
} from 'kafkajs';
import {
  ConfigurationError,
  ConsumerError,
  ProducerError,
  SerializationError,
  TransactionError,
} from './errors.ts';
import type {
  BatchMessage,
  EnvironmentConfig,
  KafkaConfig,
  MessageHeaders,
  MessageInput,
  SendResult,
} from './types.ts';

// Configuration Class
export class Config {
  public readonly bootstrapServers: string;
  public readonly acks: 'all' | 0 | 1 | -1;
  public readonly retries: number;
  public readonly compressionType:
    | 'gzip'
    | 'snappy'
    | 'lz4'
    | 'zstd'
    | undefined;
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
    this.bootstrapServers =
      config.bootstrapServers ??
      env.KAFKA_BOOTSTRAP_SERVERS ??
      'localhost:9092';
    this.acks = config.acks ?? 'all';
    this.retries = config.retries ?? 3;
    this.compressionType =
      config.compressionType === null ? undefined : config.compressionType;
    this.batchSize = config.batchSize ?? 16384;
    this.lingerMs = config.lingerMs ?? 0;

    // Consumer settings
    this.groupId = config.groupId ?? 'default-group';
    this.autoOffsetReset = config.autoOffsetReset ?? 'latest';
    this.enableAutoCommit = config.enableAutoCommit ?? true;

    // Connection settings
    this.clientId =
      config.clientId ?? env.KAFKA_CLIENT_ID ?? 'apnamart-kafka-client';
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
    const brokers = this.bootstrapServers.split(',').map((s) => s.trim());

    const config: KafkaJSConfig = {
      clientId: this.clientId,
      brokers,
      connectionTimeout: this.connectionTimeout,
      authenticationTimeout: this.authenticationTimeout,
      requestTimeout: this.requestTimeout,
    };

    // Add SASL if configured
    if (this.config.sasl) {
      // Create SASL config with proper typing for KafkaJS
      const { mechanism, username, password } = this.config.sasl;
      if (mechanism === 'plain') {
        config.sasl = { mechanism: 'plain' as const, username, password };
      } else if (mechanism === 'scram-sha-256') {
        config.sasl = {
          mechanism: 'scram-sha-256' as const,
          username,
          password,
        };
      } else if (mechanism === 'scram-sha-512') {
        config.sasl = {
          mechanism: 'scram-sha-512' as const,
          username,
          password,
        };
      }
    }

    // Add SSL if configured
    if (this.config.ssl) {
      config.ssl = this.config.ssl;
    }

    return config;
  }

  public toProducerConfig() {
    return {
      allowAutoTopicCreation: true,
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
      allowAutoTopicCreation: true,
      retry: {
        initialRetryTime: 300,
        retries: this.retries,
      },
    };
  }
}

// Serialization Functions
export function serialize(data: unknown): Buffer {
  if (Buffer.isBuffer(data)) {
    return data;
  }

  if (typeof data === 'string') {
    return Buffer.from(data, 'utf-8');
  }

  try {
    const jsonString = JSON.stringify(data);
    return Buffer.from(jsonString, 'utf-8');
  } catch (error) {
    throw new SerializationError(
      `Failed to serialize data: ${error instanceof Error ? error.message : String(error)}`,
      error instanceof Error ? error : undefined,
    );
  }
}

export function deserialize(data: Buffer | null): unknown {
  if (!data) {
    return null;
  }

  try {
    const str = data.toString('utf-8');
    return JSON.parse(str);
  } catch {
    try {
      return data.toString('utf-8');
    } catch {
      return data;
    }
  }
}

// Message Class
export class Message {
  public readonly topic: string;
  public readonly partition: number;
  public readonly offset: string;
  public readonly key: unknown;
  public readonly value: unknown;
  public readonly timestamp: string;
  public readonly headers: MessageHeaders;

  constructor(payload: EachMessagePayload) {
    this.topic = payload.topic;
    this.partition = payload.partition;
    this.offset = payload.message.offset;
    this.key = payload.message.key ? deserialize(payload.message.key) : null;
    this.value = deserialize(payload.message.value);
    this.timestamp = payload.message.timestamp;
    this.headers = this.convertHeaders(payload.message.headers || {});
  }

  private convertHeaders(headers: Record<string, unknown>): MessageHeaders {
    if (!headers) return {};

    const result: MessageHeaders = {};
    for (const [key, value] of Object.entries(headers)) {
      if (value !== undefined) {
        result[key] = Buffer.isBuffer(value) ? value.toString('utf-8') : String(value);
      }
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

// Producer Class
export class Producer {
  protected kafka: Kafka | null = null;
  protected producer: KafkaJSProducer | null = null;
  protected closed = false;

  constructor(protected config: Config = new Config()) {}

  protected convertHeaders(headers: MessageHeaders): Record<string, string> {
    const converted: Record<string, string> = {};
    for (const [key, value] of Object.entries(headers)) {
      if (value !== undefined) {
        converted[key] = typeof value === 'string' ? value : value.toString();
      }
    }
    return converted;
  }

  protected async getProducer(): Promise<KafkaJSProducer> {
    if (this.producer === null) {
      if (this.closed) {
        throw new ProducerError('Producer is closed');
      }

      this.kafka = new Kafka(this.config.toKafkaJSConfig());
      this.producer = this.kafka.producer(this.config.toProducerConfig());

      try {
        await this.producer.connect();
      } catch (error) {
        throw new ProducerError(
          `Failed to connect producer: ${error instanceof Error ? error.message : String(error)}`,
          error instanceof Error ? error : undefined,
        );
      }
    }

    return this.producer;
  }

  public async send(
    topic: string,
    value: unknown,
    key?: unknown,
    options: {
      partition?: number;
      headers?: Record<string, string>;
      timestamp?: string;
    } = {},
  ): Promise<RecordMetadata[]> {
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
        headers: options.headers
          ? this.convertHeaders(options.headers)
          : undefined,
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
    const results: SendResult[] = new Array(messages.length);

    // Group messages by topic for efficient sending
    interface InternalMessage {
      key?: Buffer;
      value: Buffer;
      partition?: number;
      headers?: Record<string, string>;
      originalIndex: number;
    }

    const messagesByTopic = new Map<string, InternalMessage[]>();

    for (let i = 0; i < messages.length; i++) {
      try {
        const parsed = this.parseMessageInput(messages[i]);

        if (!messagesByTopic.has(parsed.topic)) {
          messagesByTopic.set(parsed.topic, []);
        }

        const topicMessages = messagesByTopic.get(parsed.topic);
        if (topicMessages) {
          topicMessages.push({
            key: parsed.key !== undefined ? serialize(parsed.key) : undefined,
            value: serialize(parsed.value),
            partition: parsed.partition,
            headers: parsed.headers
              ? this.convertHeaders(parsed.headers)
              : undefined,
            originalIndex: i,
          });
        }
      } catch (error) {
        results[i] = {
          success: false,
          error: `Failed to process message: ${error instanceof Error ? error.message : String(error)}`,
        };
      }
    }

    // Send messages topic by topic
    for (const [topic, topicMessages] of Array.from(
      messagesByTopic.entries(),
    )) {
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
            error: `Failed to send to topic ${topic}: ${error instanceof Error ? error.message : String(error)}`,
          };
        }
      }
    }

    return results;
  }

  protected parseMessageInput(input: MessageInput): BatchMessage {
    if (Array.isArray(input)) {
      if (input.length === 2) {
        return { topic: input[0], value: input[1] };
      } else if (input.length === 3) {
        return { topic: input[0], value: input[1], key: input[2] };
      } else {
        throw new Error(
          'Array format must be [topic, value] or [topic, value, key]',
        );
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

  private handleProducerError(error: unknown, topic?: string): never {
    const message = error instanceof Error ? error.message : String(error);

    if (message.includes('Request timed out')) {
      throw new ProducerError(
        `Message delivery timed out. Check Kafka connection: ${message}`,
      );
    }

    if (message.includes('Unknown topic')) {
      throw new ProducerError(
        `Topic '${topic}' does not exist or is not accessible: ${message}`,
      );
    }

    if (message.includes('NOT_LEADER_FOR_PARTITION')) {
      throw new ProducerError(
        `Partition leadership changed. Message will be retried: ${message}`,
      );
    }

    throw new ProducerError(
      `Failed to send message${topic ? ` to topic '${topic}'` : ''}: ${message}`,
      error instanceof Error ? error : undefined,
    );
  }

  public async flush(_timeout = 10000): Promise<void> {
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
          console.warn(
            'Error closing producer:',
            error instanceof Error ? error.message : String(error),
          );
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

// Consumer Class
export class Consumer {
  private kafka: Kafka | null = null;
  private consumer: KafkaJSConsumer | null = null;
  private closed = false;
  private subscribed = false;

  private readonly topicList: string[];

  constructor(
    topics: string | string[] = [],
    private config: Config = new Config(),
  ) {
    this.topicList = Array.isArray(topics) ? topics : [topics];
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

        if (this.topicList.length > 0) {
          await this.consumer.subscribe({
            topics: this.topicList,
            fromBeginning: this.config.autoOffsetReset === 'earliest',
          });
          this.subscribed = true;
        }
      } catch (error) {
        throw new ConsumerError(
          `Failed to connect consumer: ${error instanceof Error ? error.message : String(error)}`,
          error instanceof Error ? error : undefined,
        );
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

        let messageReceived = false;

        const processMessage = async (payload: EachMessagePayload) => {
          if (messageReceived) return;
          messageReceived = true;

          clearTimeout(timer);
          try {
            const message = new Message(payload);
            resolve(message);
          } catch (error) {
            reject(
              new ConsumerError(
                `Failed to process message: ${error instanceof Error ? error.message : String(error)}`,
                error instanceof Error ? error : undefined,
              ),
            );
          }
        };

        consumer
          .run({
            eachMessage: processMessage,
          })
          .catch(reject);
      });
    } catch (error) {
      this.handleConsumerError(error);
    }
  }

  public async pollBatch(size = 100, timeout = 10000): Promise<Message[]> {
    const messages: Message[] = [];
    const startTime = Date.now();

    while (messages.length < size && Date.now() - startTime < timeout) {
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
            offset: (parseInt(message.offset, 10) + 1).toString(),
          },
        ]);
      } else {
        // Commit current offsets
        await consumer.commitOffsets([]);
      }
    } catch (error) {
      throw new ConsumerError(
        `Failed to commit offsets: ${error instanceof Error ? error.message : String(error)}`,
        error instanceof Error ? error : undefined,
      );
    }
  }

  public async seek(
    topic: string,
    partition: number,
    offset: string,
  ): Promise<void> {
    if (this.closed) {
      throw new ConsumerError('Consumer is closed');
    }

    const consumer = await this.getConsumer();

    try {
      consumer.seek({ topic, partition, offset });
    } catch (error) {
      throw new ConsumerError(
        `Failed to seek: ${error instanceof Error ? error.message : String(error)}`,
        error instanceof Error ? error : undefined,
      );
    }
  }

  private handleConsumerError(error: unknown): never {
    const message = error instanceof Error ? error.message : String(error);

    if (message.includes('Unknown topic')) {
      throw new ConsumerError(`Unknown topic or partition: ${message}`);
    }

    if (message.includes('Connection failed')) {
      throw new ConsumerError(
        `Transport error (check Kafka connection): ${message}`,
      );
    }

    if (message.includes('SASL authentication failed')) {
      throw new ConsumerError(`Authentication failed: ${message}`);
    }

    if (message.includes('Not authorized')) {
      throw new ConsumerError(`Authorization failed: ${message}`);
    }

    throw new ConsumerError(
      `Consumer error: ${message}`,
      error instanceof Error ? error : undefined,
    );
  }

  public async close(): Promise<void> {
    if (!this.closed) {
      this.closed = true;

      if (this.consumer) {
        try {
          await this.consumer.disconnect();
        } catch (error) {
          console.warn(
            'Error closing consumer:',
            error instanceof Error ? error.message : String(error),
          );
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

// TransactionalProducer Class
export class TransactionalProducer extends Producer {
  private inTransaction = false;
  private initialized = false;
  private currentTransaction: Awaited<
    ReturnType<KafkaJSProducer['transaction']>
  > | null = null;

  constructor(
    private transactionalId: string,
    config: Config = new Config(),
  ) {
    super(config);
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
          this.initialized = true;
        }
      } catch (error) {
        throw new TransactionError(
          `Failed to connect transactional producer: ${error instanceof Error ? error.message : String(error)}`,
          error instanceof Error ? error : undefined,
        );
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
      throw new TransactionError(
        `Failed to begin transaction: ${error instanceof Error ? error.message : String(error)}`,
        error instanceof Error ? error : undefined,
      );
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
      throw new TransactionError(
        `Failed to commit transaction: ${error instanceof Error ? error.message : String(error)}`,
        error instanceof Error ? error : undefined,
      );
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
      throw new TransactionError(
        `Failed to abort transaction: ${error instanceof Error ? error.message : String(error)}`,
        error instanceof Error ? error : undefined,
      );
    }
  }

  public async sendTransactional(
    topic: string,
    value: unknown,
    key?: unknown,
    options: {
      partition?: number;
      headers?: Record<string, string>;
    } = {},
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
        messages: [
          {
            key: key !== undefined ? serialize(key) : undefined,
            value: serialize(value),
            partition: options.partition,
            headers: options.headers
              ? this.convertHeaders(options.headers)
              : undefined,
          },
        ],
      });
    } catch (error) {
      throw new TransactionError(
        `Failed to send transactional message: ${error instanceof Error ? error.message : String(error)}`,
        error instanceof Error ? error : undefined,
      );
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
        await this.sendTransactional(parsed.topic, parsed.value, parsed.key, {
          partition: parsed.partition,
          headers: parsed.headers
            ? this.convertHeaders(parsed.headers)
            : undefined,
        });
      }

      await this.commit();
    } catch (error) {
      await this.abort();
      throw new TransactionError(
        `Transaction failed: ${error instanceof Error ? error.message : String(error)}`,
        error instanceof Error ? error : undefined,
      );
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

// Convenience Functions
export async function send(
  topic: string,
  value: unknown,
  key?: unknown,
  options: {
    servers?: string;
    config?: KafkaConfig;
  } = {},
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
  } = {},
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
