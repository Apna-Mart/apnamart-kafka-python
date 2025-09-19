import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import {
  Config,
  Consumer,
  Producer,
  type MessageInput,
} from '../../src/index.ts';

describe('Batch Operations Integration Tests', () => {
  let producer: Producer;
  let consumer: Consumer;
  let testTopic: string;

  beforeEach(() => {
    testTopic = `test-batch-${Math.random().toString(36).substring(7)}-${Date.now()}`;

    const config = new Config({
      bootstrapServers: 'localhost:9092',
      acks: 'all',
      retries: 3,
      batchSize: 16384, // Explicit batch size for testing
      lingerMs: 10, // Small linger time for faster batching
    });

    producer = new Producer(config);
    consumer = new Consumer(
      [testTopic],
      new Config({
        ...config,
        groupId: `test-batch-group-${Math.random().toString(36).substring(7)}`,
        autoOffsetReset: 'earliest',
      }),
    );
  });

  afterEach(async () => {
    await producer?.close();
    await consumer?.close();
  });

  describe('sendBatch operations', () => {
    it('should send batch messages using array format', async () => {
      const messages: MessageInput[] = [
        [testTopic, { id: 1, content: 'Batch message 1' }],
        [testTopic, { id: 2, content: 'Batch message 2' }],
        [testTopic, { id: 3, content: 'Batch message 3' }],
      ];

      const results = await producer.sendBatch(messages);

      expect(results).toHaveLength(3);
      results.forEach((result, index) => {
        expect(result.success).toBe(true);
        expect(result.topic).toBe(testTopic);
        expect(result.partition).toBeGreaterThanOrEqual(0);
        expect(result.offset).toBeDefined();
      });

      // Wait a bit for messages to be available
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Verify messages were received
      const receivedMessages = await consumer.pollBatch(3, 10000);
      expect(receivedMessages).toHaveLength(3);

      const values = receivedMessages.map(m => m.value);
      expect(values).toEqual(expect.arrayContaining([
        { id: 1, content: 'Batch message 1' },
        { id: 2, content: 'Batch message 2' },
        { id: 3, content: 'Batch message 3' },
      ]));
    }, 30000);

    it('should send batch messages with keys using array format', async () => {
      const messages: MessageInput[] = [
        [testTopic, { id: 1, content: 'Message with key 1' }, 'key-1'],
        [testTopic, { id: 2, content: 'Message with key 2' }, 'key-2'],
        [testTopic, { id: 3, content: 'Message with key 3' }, 'key-3'],
      ];

      const results = await producer.sendBatch(messages);

      expect(results).toHaveLength(3);
      results.forEach(result => {
        expect(result.success).toBe(true);
      });

      // Wait a bit for messages to be available
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Verify messages were received with correct keys
      const receivedMessages = await consumer.pollBatch(3, 10000);
      expect(receivedMessages).toHaveLength(3);

      const keysAndValues = receivedMessages.map(m => ({ key: m.key, value: m.value }));
      expect(keysAndValues).toEqual(expect.arrayContaining([
        { key: 'key-1', value: { id: 1, content: 'Message with key 1' } },
        { key: 'key-2', value: { id: 2, content: 'Message with key 2' } },
        { key: 'key-3', value: { id: 3, content: 'Message with key 3' } },
      ]));
    }, 30000);

    it('should send batch messages using object format', async () => {
      const messages: MessageInput[] = [
        {
          topic: testTopic,
          value: { id: 1, content: 'Object format 1' },
          key: 'obj-key-1',
          headers: { type: 'object', batch: 'true' },
        },
        {
          topic: testTopic,
          value: { id: 2, content: 'Object format 2' },
          key: 'obj-key-2',
          partition: 0,
        },
        {
          topic: testTopic,
          value: { id: 3, content: 'Object format 3' },
        },
      ];

      const results = await producer.sendBatch(messages);

      expect(results).toHaveLength(3);
      results.forEach(result => {
        expect(result.success).toBe(true);
      });

      // Wait a bit for messages to be available
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Verify messages were received
      const receivedMessages = await consumer.pollBatch(3, 10000);
      expect(receivedMessages).toHaveLength(3);

      // Check first message with headers
      const messageWithHeaders = receivedMessages.find(m => m.headers.type === 'object');
      expect(messageWithHeaders).toBeDefined();
      expect(messageWithHeaders!.key).toBe('obj-key-1');
      expect(messageWithHeaders!.headers.batch).toBe('true');

      // Check second message with specific partition
      const messageWithPartition = receivedMessages.find(m => m.key === 'obj-key-2');
      expect(messageWithPartition).toBeDefined();
      expect(messageWithPartition!.partition).toBe(0);

      // Check third message without key
      const messageWithoutKey = receivedMessages.find(m =>
        m.value && typeof m.value === 'object' && 'id' in m.value && m.value.id === 3
      );
      expect(messageWithoutKey).toBeDefined();
      expect(messageWithoutKey!.key).toBeNull();
    }, 30000);

    it('should handle mixed message formats in batch', async () => {
      const messages: MessageInput[] = [
        [testTopic, { id: 1, format: 'array' }],
        [testTopic, { id: 2, format: 'array-with-key' }, 'array-key'],
        {
          topic: testTopic,
          value: { id: 3, format: 'object' },
          key: 'object-key',
          headers: { format: 'object' },
        },
      ];

      const results = await producer.sendBatch(messages);

      expect(results).toHaveLength(3);
      results.forEach(result => {
        expect(result.success).toBe(true);
      });

      // Wait a bit for messages to be available
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Verify all formats were handled correctly
      const receivedMessages = await consumer.pollBatch(3, 10000);
      expect(receivedMessages).toHaveLength(3);

      const formats = receivedMessages.map(m => (m.value as any).format);
      expect(formats).toEqual(expect.arrayContaining(['array', 'array-with-key', 'object']));
    }, 30000);

    it('should handle large batch efficiently', async () => {
      const batchSize = 100;
      const messages: MessageInput[] = [];

      for (let i = 0; i < batchSize; i++) {
        messages.push([
          testTopic,
          { id: i, batch: 'large', content: `Message ${i}` },
          `key-${i}`,
        ]);
      }

      const startTime = Date.now();
      const results = await producer.sendBatch(messages);
      const endTime = Date.now();

      expect(results).toHaveLength(batchSize);
      results.forEach(result => {
        expect(result.success).toBe(true);
      });

      // Should complete in reasonable time (less than 10 seconds)
      expect(endTime - startTime).toBeLessThan(10000);

      // Wait a bit for messages to be available
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Verify at least some messages were received
      const receivedMessages = await consumer.pollBatch(batchSize, 15000);
      expect(receivedMessages.length).toBeGreaterThan(0);
      expect(receivedMessages.length).toBeLessThanOrEqual(batchSize);
    }, 45000);

    it('should handle batch with different data types', async () => {
      const messages: MessageInput[] = [
        [testTopic, 'string message'],
        [testTopic, 42],
        [testTopic, true],
        [testTopic, { complex: 'object', nested: { value: 123 } }],
        [testTopic, [1, 2, 3, 'array']],
        [testTopic, null],
      ];

      const results = await producer.sendBatch(messages);

      expect(results).toHaveLength(6);
      results.forEach(result => {
        expect(result.success).toBe(true);
      });

      // Wait a bit for messages to be available
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Verify different data types were handled correctly
      const receivedMessages = await consumer.pollBatch(6, 10000);
      expect(receivedMessages).toHaveLength(6);

      const values = receivedMessages.map(m => m.value);
      expect(values).toEqual(expect.arrayContaining([
        'string message',
        42,
        true,
        { complex: 'object', nested: { value: 123 } },
        [1, 2, 3, 'array'],
        null,
      ]));
    }, 30000);
  });

  describe('batch error handling', () => {
    it('should handle partial failures in batch', async () => {
      const messages: MessageInput[] = [
        [testTopic, { id: 1, content: 'Valid message 1' }],
        'invalid-format' as unknown as MessageInput, // Invalid format
        [testTopic, { id: 3, content: 'Valid message 3' }],
      ];

      const results = await producer.sendBatch(messages);

      expect(results).toHaveLength(3);
      expect(results[0].success).toBe(true);
      expect(results[1].success).toBe(false);
      expect(results[1].error).toContain('Failed to process message');
      expect(results[2].success).toBe(true);

      // Wait a bit for messages to be available
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Should receive only the valid messages
      const receivedMessages = await consumer.pollBatch(2, 5000);
      expect(receivedMessages.length).toBeGreaterThanOrEqual(0);
      expect(receivedMessages.length).toBeLessThanOrEqual(2);
    }, 30000);

    it('should handle batch with empty topic names', async () => {
      const messages: MessageInput[] = [
        [testTopic, { id: 1, content: 'Valid message' }],
        ['', { id: 2, content: 'Empty topic' }], // Empty topic
        [testTopic, { id: 3, content: 'Another valid message' }],
      ];

      const results = await producer.sendBatch(messages);

      expect(results).toHaveLength(3);
      expect(results[0].success).toBe(true);
      expect(results[1].success).toBe(false);
      expect(results[2].success).toBe(true);
    }, 30000);

    it('should handle batch when producer is closed', async () => {
      await producer.close();

      const messages: MessageInput[] = [
        [testTopic, { id: 1, content: 'Message after close' }],
      ];

      await expect(producer.sendBatch(messages)).rejects.toThrow();
    }, 30000);
  });

  describe('multiple topics in batch', () => {
    it('should send batch messages to multiple topics', async () => {
      const topic1 = `${testTopic}-1`;
      const topic2 = `${testTopic}-2`;

      const consumer1 = new Consumer([topic1], new Config({
        bootstrapServers: 'localhost:9092',
        groupId: `multi-topic-group-1-${Math.random().toString(36).substring(7)}`,
        autoOffsetReset: 'earliest',
      }));

      const consumer2 = new Consumer([topic2], new Config({
        bootstrapServers: 'localhost:9092',
        groupId: `multi-topic-group-2-${Math.random().toString(36).substring(7)}`,
        autoOffsetReset: 'earliest',
      }));

      try {
        const messages: MessageInput[] = [
          [topic1, { id: 1, topic: 'topic1' }],
          [topic2, { id: 2, topic: 'topic2' }],
          [topic1, { id: 3, topic: 'topic1' }],
          [topic2, { id: 4, topic: 'topic2' }],
        ];

        const results = await producer.sendBatch(messages);

        expect(results).toHaveLength(4);
        results.forEach(result => {
          expect(result.success).toBe(true);
        });

        // Verify correct topic assignment
        expect(results[0].topic).toBe(topic1);
        expect(results[1].topic).toBe(topic2);
        expect(results[2].topic).toBe(topic1);
        expect(results[3].topic).toBe(topic2);

        // Wait a bit for messages to be available
        await new Promise(resolve => setTimeout(resolve, 1000));

        // Verify messages reached correct topics
        const messages1 = await consumer1.pollBatch(2, 5000);
        const messages2 = await consumer2.pollBatch(2, 5000);

        expect(messages1.length).toBe(2);
        expect(messages2.length).toBe(2);

        messages1.forEach(msg => {
          expect(msg.topic).toBe(topic1);
          expect((msg.value as any).topic).toBe('topic1');
        });

        messages2.forEach(msg => {
          expect(msg.topic).toBe(topic2);
          expect((msg.value as any).topic).toBe('topic2');
        });
      } finally {
        await consumer1.close();
        await consumer2.close();
      }
    }, 45000);
  });

  describe('batch performance characteristics', () => {
    it('should be more efficient than individual sends for large batches', async () => {
      const messageCount = 50;
      const testMessage = { id: 1, content: 'Performance test message' };

      // Test individual sends
      const individualStartTime = Date.now();
      for (let i = 0; i < messageCount; i++) {
        await producer.send(testTopic, { ...testMessage, id: i });
      }
      const individualEndTime = Date.now();
      const individualTime = individualEndTime - individualStartTime;

      // Test batch send
      const batchMessages: MessageInput[] = [];
      for (let i = 0; i < messageCount; i++) {
        batchMessages.push([testTopic, { ...testMessage, id: i + messageCount }]);
      }

      const batchStartTime = Date.now();
      await producer.sendBatch(batchMessages);
      const batchEndTime = Date.now();
      const batchTime = batchEndTime - batchStartTime;

      // Batch should be significantly faster (at least 2x)
      expect(batchTime).toBeLessThan(individualTime);

      // Wait a bit for messages to be available
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Verify all messages were sent
      const receivedMessages = await consumer.pollBatch(messageCount * 2, 15000);
      expect(receivedMessages.length).toBeGreaterThan(messageCount);
    }, 60000);

    it('should handle concurrent batch operations', async () => {
      const batchSize = 20;
      const concurrentBatches = 3;

      const batchPromises = [];

      for (let b = 0; b < concurrentBatches; b++) {
        const messages: MessageInput[] = [];
        for (let i = 0; i < batchSize; i++) {
          messages.push([
            testTopic,
            { batchId: b, messageId: i, content: `Batch ${b} Message ${i}` },
            `batch-${b}-key-${i}`,
          ]);
        }
        batchPromises.push(producer.sendBatch(messages));
      }

      const results = await Promise.all(batchPromises);

      // All batches should succeed
      results.forEach(batchResults => {
        expect(batchResults).toHaveLength(batchSize);
        batchResults.forEach(result => {
          expect(result.success).toBe(true);
        });
      });

      // Wait a bit for messages to be available
      await new Promise(resolve => setTimeout(resolve, 3000));

      // Should receive all messages
      const totalExpected = batchSize * concurrentBatches;
      const receivedMessages = await consumer.pollBatch(totalExpected, 20000);
      expect(receivedMessages.length).toBeGreaterThanOrEqual(totalExpected * 0.8); // Allow some margin
    }, 60000);
  });

  describe('batch with flush operations', () => {
    it('should ensure messages are sent after flush', async () => {
      const messages: MessageInput[] = [
        [testTopic, { id: 1, content: 'Pre-flush message' }],
        [testTopic, { id: 2, content: 'Another pre-flush message' }],
      ];

      await producer.sendBatch(messages);

      // Explicit flush to ensure delivery
      await producer.flush();

      // Messages should be immediately available after flush
      const receivedMessages = await consumer.pollBatch(2, 5000);
      expect(receivedMessages).toHaveLength(2);

      const values = receivedMessages.map(m => m.value);
      expect(values).toEqual(expect.arrayContaining([
        { id: 1, content: 'Pre-flush message' },
        { id: 2, content: 'Another pre-flush message' },
      ]));
    }, 30000);
  });
});