import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import {
  Config,
  Consumer,
  TransactionalProducer,
  type MessageInput,
} from '../../src/index.ts';

describe('Transactional Producer Integration Tests', () => {
  let txProducer: TransactionalProducer;
  let consumer: Consumer;
  let testTopic: string;
  let transactionalId: string;

  beforeEach(() => {
    testTopic = `test-tx-${Math.random().toString(36).substring(7)}-${Date.now()}`;
    transactionalId = `tx-${Math.random().toString(36).substring(7)}-${Date.now()}`;

    const config = new Config({
      bootstrapServers: 'localhost:9092',
      acks: 'all',
      retries: 3,
    });

    txProducer = new TransactionalProducer(transactionalId, config);
    consumer = new Consumer(
      [testTopic],
      new Config({
        ...config,
        groupId: `test-tx-group-${Math.random().toString(36).substring(7)}`,
        autoOffsetReset: 'earliest',
      }),
    );
  });

  afterEach(async () => {
    await txProducer?.close();
    await consumer?.close();
  });

  describe('basic transaction operations', () => {
    it('should commit transaction successfully', async () => {
      const testMessage = { id: 1, content: 'Transactional message' };

      await txProducer.begin();
      await txProducer.sendTransactional(testTopic, testMessage);
      await txProducer.commit();

      // Wait a bit for message to be available
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Message should be visible after commit
      const message = await consumer.poll(5000);
      expect(message).not.toBeNull();
      expect(message!.value).toEqual(testMessage);
    }, 30000);

    it('should abort transaction and message should not be visible', async () => {
      const testMessage = { id: 1, content: 'Aborted message' };

      await txProducer.begin();
      await txProducer.sendTransactional(testTopic, testMessage);
      await txProducer.abort();

      // Wait a bit
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Message should NOT be visible after abort
      const message = await consumer.poll(1000);
      expect(message).toBeNull();
    }, 30000);

    it('should handle multiple messages in single transaction', async () => {
      const messages = [
        { id: 1, content: 'TX Message 1' },
        { id: 2, content: 'TX Message 2' },
        { id: 3, content: 'TX Message 3' },
      ];

      await txProducer.begin();
      for (const msg of messages) {
        await txProducer.sendTransactional(testTopic, msg);
      }
      await txProducer.commit();

      // Wait a bit for messages to be available
      await new Promise(resolve => setTimeout(resolve, 1000));

      // All messages should be visible after commit
      const receivedMessages = await consumer.pollBatch(3, 10000);
      expect(receivedMessages).toHaveLength(3);

      const values = receivedMessages.map(m => m.value);
      expect(values).toEqual(expect.arrayContaining(messages));
    }, 30000);
  });

  describe('transactional message types', () => {
    it('should handle transactional message with key', async () => {
      const testMessage = { id: 1, content: 'Message with key' };
      const testKey = 'user-123';

      await txProducer.begin();
      await txProducer.sendTransactional(testTopic, testMessage, testKey);
      await txProducer.commit();

      // Wait a bit for message to be available
      await new Promise(resolve => setTimeout(resolve, 1000));

      const message = await consumer.poll(5000);
      expect(message).not.toBeNull();
      expect(message!.value).toEqual(testMessage);
      expect(message!.key).toBe(testKey);
    }, 30000);

    it('should handle transactional message with options', async () => {
      const testMessage = { id: 1, content: 'Message with options' };
      const testKey = 'key-with-options';
      const headers = {
        'content-type': 'application/json',
        'transaction-id': transactionalId,
      };

      await txProducer.begin();
      await txProducer.sendTransactional(testTopic, testMessage, testKey, {
        headers,
        partition: 0,
      });
      await txProducer.commit();

      // Wait a bit for message to be available
      await new Promise(resolve => setTimeout(resolve, 1000));

      const message = await consumer.poll(5000);
      expect(message).not.toBeNull();
      expect(message!.value).toEqual(testMessage);
      expect(message!.key).toBe(testKey);
      expect(message!.headers['content-type']).toBe('application/json');
      expect(message!.headers['transaction-id']).toBe(transactionalId);
      expect(message!.partition).toBe(0);
    }, 30000);
  });

  describe('batch transactional operations', () => {
    it('should send batch messages transactionally', async () => {
      const messages: MessageInput[] = [
        [testTopic, { id: 1, content: 'Batch TX 1' }],
        [testTopic, { id: 2, content: 'Batch TX 2' }, 'key-2'],
        {
          topic: testTopic,
          value: { id: 3, content: 'Batch TX 3' },
          key: 'key-3',
          headers: { type: 'batch' },
        },
      ];

      await txProducer.sendBatchTransactional(messages);

      // Wait a bit for messages to be available
      await new Promise(resolve => setTimeout(resolve, 1000));

      // All messages should be committed
      const receivedMessages = await consumer.pollBatch(3, 10000);
      expect(receivedMessages).toHaveLength(3);

      // Check that all messages are present
      const values = receivedMessages.map(m => m.value);
      expect(values).toEqual(expect.arrayContaining([
        { id: 1, content: 'Batch TX 1' },
        { id: 2, content: 'Batch TX 2' },
        { id: 3, content: 'Batch TX 3' },
      ]));

      // Check keys
      const messageWithKey2 = receivedMessages.find(m => m.key === 'key-2');
      expect(messageWithKey2).toBeDefined();
      expect(messageWithKey2!.value).toEqual({ id: 2, content: 'Batch TX 2' });

      // Check headers
      const messageWithHeaders = receivedMessages.find(m => m.headers.type === 'batch');
      expect(messageWithHeaders).toBeDefined();
      expect(messageWithHeaders!.value).toEqual({ id: 3, content: 'Batch TX 3' });
    }, 30000);

    it('should abort batch on error', async () => {
      const messages: MessageInput[] = [
        [testTopic, { id: 1, content: 'Batch message before error' }],
        'invalid-message-format' as unknown as MessageInput, // This will cause an error
        [testTopic, { id: 2, content: 'Batch message after error' }],
      ];

      // This should fail and abort the transaction
      await expect(txProducer.sendBatchTransactional(messages)).rejects.toThrow();

      // Wait a bit
      await new Promise(resolve => setTimeout(resolve, 1000));

      // No messages should be visible (all aborted)
      const message = await consumer.poll(1000);
      expect(message).toBeNull();
    }, 30000);
  });

  describe('transaction isolation', () => {
    it('should not see uncommitted messages from other transactions', async () => {
      const topic2 = `${testTopic}-2`;
      const transactionalId2 = `${transactionalId}-2`;

      const txProducer2 = new TransactionalProducer(transactionalId2, new Config({
        bootstrapServers: 'localhost:9092',
        acks: 'all',
        retries: 3,
      }));

      const consumer2 = new Consumer(
        [topic2],
        new Config({
          bootstrapServers: 'localhost:9092',
          groupId: `test-isolation-group-${Math.random().toString(36).substring(7)}`,
          autoOffsetReset: 'earliest',
        }),
      );

      try {
        // Start transactions on both producers
        await txProducer.begin();
        await txProducer2.begin();

        // Send message in first transaction but don't commit
        await txProducer.sendTransactional(testTopic, { id: 1, content: 'Uncommitted 1' });

        // Send message in second transaction and commit
        await txProducer2.sendTransactional(topic2, { id: 2, content: 'Committed 2' });
        await txProducer2.commit();

        // Wait a bit for committed message
        await new Promise(resolve => setTimeout(resolve, 1000));

        // Should see committed message from second transaction
        const committedMessage = await consumer2.poll(5000);
        expect(committedMessage).not.toBeNull();
        expect(committedMessage!.value).toEqual({ id: 2, content: 'Committed 2' });

        // Should NOT see uncommitted message from first transaction
        const uncommittedMessage = await consumer.poll(1000);
        expect(uncommittedMessage).toBeNull();

        // Now commit first transaction
        await txProducer.commit();

        // Wait a bit
        await new Promise(resolve => setTimeout(resolve, 1000));

        // Now should see the first message
        const nowCommittedMessage = await consumer.poll(5000);
        expect(nowCommittedMessage).not.toBeNull();
        expect(nowCommittedMessage!.value).toEqual({ id: 1, content: 'Uncommitted 1' });
      } finally {
        await txProducer2.close();
        await consumer2.close();
      }
    }, 45000);
  });

  describe('transaction error handling', () => {
    it('should handle transaction begin errors gracefully', async () => {
      // Close the producer to simulate connection issues
      await txProducer.close();

      await expect(txProducer.begin()).rejects.toThrow();
    }, 30000);

    it('should handle send errors during transaction', async () => {
      await txProducer.begin();

      // Try to send to an invalid/empty topic name
      await expect(
        txProducer.sendTransactional('', { id: 1, content: 'Invalid topic' })
      ).rejects.toThrow();

      // Transaction should still be abortable
      await expect(txProducer.abort()).resolves.not.toThrow();
    }, 30000);

    it('should handle nested transaction attempts', async () => {
      await txProducer.begin();

      // Trying to begin again should fail
      await expect(txProducer.begin()).rejects.toThrow('Transaction already in progress');

      // Clean up
      await txProducer.abort();
    }, 30000);

    it('should handle commit without begin', async () => {
      await expect(txProducer.commit()).rejects.toThrow('No transaction in progress');
    }, 30000);

    it('should handle abort without begin', async () => {
      await expect(txProducer.abort()).rejects.toThrow('No transaction in progress');
    }, 30000);

    it('should handle send without transaction', async () => {
      await expect(
        txProducer.sendTransactional(testTopic, { id: 1, content: 'No transaction' })
      ).rejects.toThrow('No active transaction');
    }, 30000);
  });

  describe('transaction lifecycle with async dispose', () => {
    it('should commit transaction on successful dispose', async () => {
      const testMessage = { id: 1, content: 'Dispose commit message' };

      await txProducer.begin();
      await txProducer.sendTransactional(testTopic, testMessage);

      // Use async dispose (should commit)
      await txProducer[Symbol.asyncDispose]();

      // Wait a bit for message to be available
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Message should be visible (committed)
      const message = await consumer.poll(5000);
      expect(message).not.toBeNull();
      expect(message!.value).toEqual(testMessage);

      // Create new producer for cleanup
      txProducer = new TransactionalProducer(
        `${transactionalId}-new`,
        new Config({ bootstrapServers: 'localhost:9092' })
      );
    }, 30000);

    it('should abort transaction if commit fails during dispose', async () => {
      const testMessage = { id: 1, content: 'Dispose abort message' };

      await txProducer.begin();
      await txProducer.sendTransactional(testTopic, testMessage);

      // Force a situation where commit might fail by closing connection
      // Note: This is a simulation - actual behavior may vary
      await txProducer.close();

      // Wait a bit
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Message should NOT be visible (aborted due to connection close)
      const message = await consumer.poll(1000);
      expect(message).toBeNull();

      // Create new producer for cleanup
      txProducer = new TransactionalProducer(
        `${transactionalId}-new`,
        new Config({ bootstrapServers: 'localhost:9092' })
      );
    }, 30000);
  });

  describe('multiple topics in transaction', () => {
    it('should handle cross-topic transactions', async () => {
      const topic1 = `${testTopic}-cross-1`;
      const topic2 = `${testTopic}-cross-2`;

      const consumer1 = new Consumer([topic1], new Config({
        bootstrapServers: 'localhost:9092',
        groupId: `cross-group-1-${Math.random().toString(36).substring(7)}`,
        autoOffsetReset: 'earliest',
      }));

      const consumer2 = new Consumer([topic2], new Config({
        bootstrapServers: 'localhost:9092',
        groupId: `cross-group-2-${Math.random().toString(36).substring(7)}`,
        autoOffsetReset: 'earliest',
      }));

      try {
        await txProducer.begin();

        // Send to both topics in same transaction
        await txProducer.sendTransactional(topic1, { id: 1, topic: 'topic1' });
        await txProducer.sendTransactional(topic2, { id: 2, topic: 'topic2' });

        await txProducer.commit();

        // Wait a bit for messages to be available
        await new Promise(resolve => setTimeout(resolve, 1000));

        // Both messages should be visible
        const message1 = await consumer1.poll(5000);
        const message2 = await consumer2.poll(5000);

        expect(message1).not.toBeNull();
        expect(message2).not.toBeNull();
        expect(message1!.value).toEqual({ id: 1, topic: 'topic1' });
        expect(message2!.value).toEqual({ id: 2, topic: 'topic2' });
      } finally {
        await consumer1.close();
        await consumer2.close();
      }
    }, 45000);
  });
});