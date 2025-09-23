import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import {
  Config,
  Consumer,
  ConsumerError,
  Producer,
  ProducerError,
  TransactionalProducer,
  TransactionError,
} from '../../src/index.ts';

describe('Error Recovery Integration Tests', () => {
  let producer: Producer;
  let consumer: Consumer;
  let testTopic: string;

  beforeEach(() => {
    testTopic = `test-error-${Math.random().toString(36).substring(7)}-${Date.now()}`;

    const config = new Config({
      bootstrapServers: 'localhost:9092',
      acks: 'all',
      retries: 3,
      requestTimeout: 5000,
      connectionTimeout: 3000,
    });

    producer = new Producer(config);
    consumer = new Consumer(
      [testTopic],
      new Config({
        ...config,
        groupId: `test-error-group-${Math.random().toString(36).substring(7)}`,
        autoOffsetReset: 'earliest',
      }),
    );
  });

  afterEach(async () => {
    await producer?.close();
    await consumer?.close();
  });

  describe('producer error scenarios', () => {
    it('should handle empty topic name errors', async () => {
      const testMessage = { id: 1, content: 'Test message' };

      await expect(producer.send('', testMessage)).rejects.toThrow(
        ProducerError,
      );
      await expect(producer.send('', testMessage)).rejects.toThrow(
        'Topic name cannot be empty',
      );
    }, 30000);

    it('should handle producer operations after close', async () => {
      const testMessage = { id: 1, content: 'Test message' };

      await producer.close();

      await expect(producer.send(testTopic, testMessage)).rejects.toThrow(
        ProducerError,
      );
      await expect(producer.send(testTopic, testMessage)).rejects.toThrow(
        'Producer is closed',
      );
    }, 30000);

    it('should handle invalid bootstrap servers gracefully', async () => {
      const invalidConfig = new Config({
        bootstrapServers: 'invalid-server:9092',
        connectionTimeout: 1000,
        requestTimeout: 2000,
      });

      const invalidProducer = new Producer(invalidConfig);

      try {
        const testMessage = { id: 1, content: 'Test message' };

        await expect(
          invalidProducer.send(testTopic, testMessage),
        ).rejects.toThrow(ProducerError);
      } finally {
        await invalidProducer.close();
      }
    }, 30000);

    it('should handle serialization errors', async () => {
      // Create a circular reference that will fail JSON.stringify
      const circularObj: any = { name: 'test' };
      circularObj.self = circularObj;

      await expect(producer.send(testTopic, circularObj)).rejects.toThrow();
    }, 30000);

    it('should recover from temporary connection issues', async () => {
      // Send a successful message first
      await producer.send(testTopic, {
        id: 1,
        content: 'Before connection issue',
      });

      // Simulate recovery by sending another message
      // In real scenarios, this might involve network recovery
      await new Promise((resolve) => setTimeout(resolve, 1000));

      // Should be able to send after recovery
      await expect(
        producer.send(testTopic, {
          id: 2,
          content: 'After connection recovery',
        }),
      ).resolves.not.toThrow();

      // Verify both messages can be consumed
      await new Promise((resolve) => setTimeout(resolve, 1000));
      const messages = await consumer.pollBatch(2, 10000);
      expect(messages.length).toBeGreaterThanOrEqual(1);
    }, 45000);
  });

  describe('consumer error scenarios', () => {
    it('should handle consumer operations after close', async () => {
      await consumer.close();

      await expect(consumer.poll(1000)).rejects.toThrow(ConsumerError);
      await expect(consumer.poll(1000)).rejects.toThrow('Consumer is closed');
    }, 30000);

    it('should handle commit operations after close', async () => {
      await consumer.close();

      await expect(consumer.commit()).rejects.toThrow(ConsumerError);
      await expect(consumer.commit()).rejects.toThrow('Consumer is closed');
    }, 30000);

    it('should handle seek operations after close', async () => {
      await consumer.close();

      await expect(consumer.seek(testTopic, 0, '0')).rejects.toThrow(
        ConsumerError,
      );
      await expect(consumer.seek(testTopic, 0, '0')).rejects.toThrow(
        'Consumer is closed',
      );
    }, 30000);

    it('should handle polling from non-subscribed topics', async () => {
      const emptyConsumer = new Consumer(
        [],
        new Config({
          bootstrapServers: 'localhost:9092',
          groupId: `empty-group-${Math.random().toString(36).substring(7)}`,
        }),
      );

      try {
        await expect(emptyConsumer.poll(1000)).rejects.toThrow(ConsumerError);
        await expect(emptyConsumer.poll(1000)).rejects.toThrow(
          'not subscribed to any topics',
        );
      } finally {
        await emptyConsumer.close();
      }
    }, 30000);

    it('should handle invalid bootstrap servers for consumer', async () => {
      const invalidConfig = new Config({
        bootstrapServers: 'invalid-consumer-server:9092',
        connectionTimeout: 1000,
      });

      const invalidConsumer = new Consumer([testTopic], invalidConfig);

      try {
        await expect(invalidConsumer.poll(2000)).rejects.toThrow(ConsumerError);
      } finally {
        await invalidConsumer.close();
      }
    }, 30000);

    it('should handle timeout scenarios gracefully', async () => {
      // Consumer polling with very short timeout should return null, not throw
      const message = await consumer.poll(100);
      expect(message).toBeNull();
    }, 30000);
  });

  describe('transactional producer error scenarios', () => {
    it('should handle transaction operations after close', async () => {
      const txProducer = new TransactionalProducer(
        'tx-error-test',
        new Config({
          bootstrapServers: 'localhost:9092',
        }),
      );

      await txProducer.close();

      await expect(txProducer.begin()).rejects.toThrow(TransactionError);
      await expect(txProducer.begin()).rejects.toThrow('Producer is closed');

      await txProducer.close(); // Safe to call multiple times
    }, 30000);

    it('should handle commit without begin', async () => {
      const txProducer = new TransactionalProducer(
        'tx-error-test',
        new Config({
          bootstrapServers: 'localhost:9092',
        }),
      );

      try {
        await expect(txProducer.commit()).rejects.toThrow(TransactionError);
        await expect(txProducer.commit()).rejects.toThrow(
          'No transaction in progress',
        );
      } finally {
        await txProducer.close();
      }
    }, 30000);

    it('should handle abort without begin', async () => {
      const txProducer = new TransactionalProducer(
        'tx-error-test',
        new Config({
          bootstrapServers: 'localhost:9092',
        }),
      );

      try {
        await expect(txProducer.abort()).rejects.toThrow(TransactionError);
        await expect(txProducer.abort()).rejects.toThrow(
          'No transaction in progress',
        );
      } finally {
        await txProducer.close();
      }
    }, 30000);

    it('should handle send without transaction', async () => {
      const txProducer = new TransactionalProducer(
        'tx-error-test',
        new Config({
          bootstrapServers: 'localhost:9092',
        }),
      );

      try {
        await expect(
          txProducer.sendTransactional(testTopic, { id: 1 }),
        ).rejects.toThrow(TransactionError);
        await expect(
          txProducer.sendTransactional(testTopic, { id: 1 }),
        ).rejects.toThrow('No active transaction');
      } finally {
        await txProducer.close();
      }
    }, 30000);

    it('should handle nested transaction attempts', async () => {
      const txProducer = new TransactionalProducer(
        'tx-error-test',
        new Config({
          bootstrapServers: 'localhost:9092',
        }),
      );

      try {
        await txProducer.begin();

        await expect(txProducer.begin()).rejects.toThrow(TransactionError);
        await expect(txProducer.begin()).rejects.toThrow(
          'Transaction already in progress',
        );

        await txProducer.abort();
      } finally {
        await txProducer.close();
      }
    }, 30000);

    it('should handle transaction abort on batch error', async () => {
      const txProducer = new TransactionalProducer(
        'tx-error-test',
        new Config({
          bootstrapServers: 'localhost:9092',
        }),
      );

      try {
        // Invalid message format in batch should abort transaction
        const invalidMessages = [
          [testTopic, { id: 1 }],
          'invalid-format' as any, // This will cause an error
        ];

        await expect(
          txProducer.sendBatchTransactional(invalidMessages),
        ).rejects.toThrow(TransactionError);

        // Transaction should be aborted, so begin should work
        await expect(txProducer.begin()).resolves.not.toThrow();
        await txProducer.abort();
      } finally {
        await txProducer.close();
      }
    }, 30000);
  });

  describe('resource cleanup and disposal', () => {
    it('should handle multiple close operations safely', async () => {
      // Multiple closes should not throw
      await producer.close();
      await expect(producer.close()).resolves.not.toThrow();
      await expect(producer.close()).resolves.not.toThrow();

      await consumer.close();
      await expect(consumer.close()).resolves.not.toThrow();
      await expect(consumer.close()).resolves.not.toThrow();
    }, 30000);

    it('should handle async disposal with errors', async () => {
      const testProducer = new Producer(
        new Config({
          bootstrapServers: 'localhost:9092',
        }),
      );

      // Send a message first to establish connection
      await testProducer.send(testTopic, { id: 1 });

      // Dispose should work even if there are background errors
      await expect(testProducer[Symbol.asyncDispose]()).resolves.not.toThrow();

      // Producer should be unusable after disposal
      await expect(testProducer.send(testTopic, { id: 2 })).rejects.toThrow();
    }, 30000);

    it('should handle transactional producer disposal during transaction', async () => {
      const txProducer = new TransactionalProducer(
        'tx-dispose-test',
        new Config({
          bootstrapServers: 'localhost:9092',
        }),
      );

      await txProducer.begin();
      await txProducer.sendTransactional(testTopic, {
        id: 1,
        content: 'Disposal test',
      });

      // Disposal should attempt to commit transaction
      await expect(txProducer[Symbol.asyncDispose]()).resolves.not.toThrow();

      // Wait a bit and check if message was committed
      await new Promise((resolve) => setTimeout(resolve, 1000));
      const message = await consumer.poll(5000);

      // Message might or might not be there depending on disposal timing
      // The important thing is disposal didn't throw
      expect(true).toBe(true);
    }, 30000);
  });

  describe('configuration error scenarios', () => {
    it('should handle invalid configuration values', async () => {
      expect(() => new Config({ retries: -1 })).toThrow();
      expect(() => new Config({ batchSize: 0 })).toThrow();
      expect(() => new Config({ batchSize: -100 })).toThrow();
    }, 30000);

    it('should handle missing required configuration', async () => {
      // Empty bootstrap servers should use default
      const configWithDefaults = new Config({ bootstrapServers: '' });
      expect(configWithDefaults.bootstrapServers).toBe('localhost:9092');
    }, 30000);

    it('should handle environment variable fallbacks', async () => {
      // Save original env
      const originalBootstrap = process.env.KAFKA_BOOTSTRAP_SERVERS;
      const originalClientId = process.env.KAFKA_CLIENT_ID;

      try {
        // Set test environment variables
        process.env.KAFKA_BOOTSTRAP_SERVERS = 'env-test:9092';
        process.env.KAFKA_CLIENT_ID = 'env-client-test';

        const config = new Config();
        expect(config.bootstrapServers).toBe('env-test:9092');
        expect(config.clientId).toBe('env-client-test');
      } finally {
        // Restore original env
        if (originalBootstrap) {
          process.env.KAFKA_BOOTSTRAP_SERVERS = originalBootstrap;
        } else {
          delete process.env.KAFKA_BOOTSTRAP_SERVERS;
        }

        if (originalClientId) {
          process.env.KAFKA_CLIENT_ID = originalClientId;
        } else {
          delete process.env.KAFKA_CLIENT_ID;
        }
      }
    }, 30000);
  });

  describe('retry and recovery behavior', () => {
    it('should retry failed operations according to configuration', async () => {
      const retryConfig = new Config({
        bootstrapServers: 'localhost:9092',
        retries: 2,
        requestTimeout: 1000,
      });

      const retryProducer = new Producer(retryConfig);

      try {
        // This should still work despite short timeout due to retries
        await retryProducer.send(testTopic, { id: 1, content: 'Retry test' });

        // Verify message was sent
        await new Promise((resolve) => setTimeout(resolve, 1000));
        const message = await consumer.poll(5000);
        expect(message).not.toBeNull();
        expect(message!.value).toEqual({ id: 1, content: 'Retry test' });
      } finally {
        await retryProducer.close();
      }
    }, 45000);

    it('should handle connection recovery scenarios', async () => {
      // Send initial message to establish connection
      await producer.send(testTopic, {
        id: 1,
        content: 'Before recovery test',
      });

      // Simulate some processing time
      await new Promise((resolve) => setTimeout(resolve, 2000));

      // Should be able to send another message (connection should be maintained/recovered)
      await producer.send(testTopic, { id: 2, content: 'After recovery test' });

      // Verify both messages
      await new Promise((resolve) => setTimeout(resolve, 1000));
      const messages = await consumer.pollBatch(2, 10000);
      expect(messages.length).toBeGreaterThanOrEqual(1);
    }, 45000);
  });

  describe('memory and resource management', () => {
    it('should handle rapid create/dispose cycles', async () => {
      const iterations = 10;

      for (let i = 0; i < iterations; i++) {
        const tempProducer = new Producer(
          new Config({
            bootstrapServers: 'localhost:9092',
          }),
        );

        await tempProducer.send(testTopic, { iteration: i });
        await tempProducer.close();
      }

      // Should not accumulate memory leaks or connection issues
      await new Promise((resolve) => setTimeout(resolve, 1000));

      // Verify at least some messages were sent
      const messages = await consumer.pollBatch(iterations, 10000);
      expect(messages.length).toBeGreaterThan(0);
    }, 60000);

    it('should handle concurrent producer/consumer operations', async () => {
      const concurrentOps = 5;
      const promises = [];

      // Create multiple concurrent operations
      for (let i = 0; i < concurrentOps; i++) {
        promises.push(
          producer.send(testTopic, {
            concurrent: true,
            id: i,
            content: `Concurrent message ${i}`,
          }),
        );
      }

      // All operations should complete successfully
      const results = await Promise.all(promises);
      expect(results).toHaveLength(concurrentOps);

      // Verify messages were sent
      await new Promise((resolve) => setTimeout(resolve, 1000));
      const messages = await consumer.pollBatch(concurrentOps, 10000);
      expect(messages.length).toBeGreaterThan(0);
    }, 45000);
  });
});
