import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { Config, Consumer, Producer } from '../../src/index.ts';

describe('Consumer Integration Tests', () => {
  let producer: Producer;
  let consumer: Consumer;
  let testTopic: string;

  beforeEach(() => {
    testTopic = `test-consumer-${Math.random().toString(36).substring(7)}-${Date.now()}`;

    const config = new Config({
      bootstrapServers: 'localhost:9092',
      acks: 'all',
      retries: 3,
    });

    producer = new Producer(config);
    consumer = new Consumer(
      [testTopic],
      new Config({
        ...config,
        groupId: `test-consumer-group-${Math.random().toString(36).substring(7)}`,
        autoOffsetReset: 'earliest',
      }),
    );
  });

  afterEach(async () => {
    await producer?.close();
    await consumer?.close();
  });

  describe('pollBatch operations', () => {
    it('should consume multiple messages with pollBatch', async () => {
      const messages = [
        { id: 1, content: 'First message' },
        { id: 2, content: 'Second message' },
        { id: 3, content: 'Third message' },
      ];

      // Send multiple messages
      for (const msg of messages) {
        await producer.send(testTopic, msg);
      }

      // Wait a bit for messages to be available
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Consume batch
      const receivedMessages = await consumer.pollBatch(3, 10000);

      expect(receivedMessages).toHaveLength(3);
      const receivedValues = receivedMessages.map(m => m.value);
      expect(receivedValues).toEqual(expect.arrayContaining(messages));
    }, 30000);

    it('should handle partial batch when fewer messages available', async () => {
      const messages = [
        { id: 1, content: 'Only message' },
      ];

      // Send only one message
      await producer.send(testTopic, messages[0]);

      // Wait a bit for message to be available
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Try to consume batch of 3 but only 1 available
      const receivedMessages = await consumer.pollBatch(3, 2000);

      expect(receivedMessages).toHaveLength(1);
      expect(receivedMessages[0].value).toEqual(messages[0]);
    }, 30000);

    it('should timeout when no messages available', async () => {
      // Don't send any messages

      const receivedMessages = await consumer.pollBatch(2, 1000);

      expect(receivedMessages).toHaveLength(0);
    }, 30000);
  });

  describe('commit operations', () => {
    it('should commit specific message offset', async () => {
      const testMessage = { id: 1, content: 'Test commit message' };

      // Send message
      await producer.send(testTopic, testMessage);

      // Wait a bit for message to be available
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Consume message
      const message = await consumer.poll(5000);
      expect(message).not.toBeNull();

      // Commit the specific message
      await consumer.commit(message!);

      // Should not throw
      expect(true).toBe(true);
    }, 30000);

    it('should commit current offsets', async () => {
      const testMessage = { id: 1, content: 'Test commit current' };

      // Send message
      await producer.send(testTopic, testMessage);

      // Wait a bit for message to be available
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Consume message
      const message = await consumer.poll(5000);
      expect(message).not.toBeNull();

      // Commit current offsets (no specific message)
      await consumer.commit();

      // Should not throw
      expect(true).toBe(true);
    }, 30000);
  });

  describe('seek operations', () => {
    it('should seek to specific offset', async () => {
      const messages = [
        { id: 1, content: 'First' },
        { id: 2, content: 'Second' },
        { id: 3, content: 'Third' },
      ];

      // Send multiple messages
      for (const msg of messages) {
        await producer.send(testTopic, msg);
      }

      // Wait a bit for messages to be available
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Consume first message to get partition info
      const firstMessage = await consumer.poll(5000);
      expect(firstMessage).not.toBeNull();

      // Seek to beginning (offset 0)
      await consumer.seek(testTopic, firstMessage!.partition, '0');

      // Should be able to consume from beginning again
      const seekedMessage = await consumer.poll(5000);
      expect(seekedMessage).not.toBeNull();
      expect(seekedMessage!.value).toEqual(messages[0]);
    }, 30000);
  });

  describe('async iterator', () => {
    it('should iterate over messages using for-await-of', async () => {
      const messages = [
        { id: 1, content: 'Iterator message 1' },
        { id: 2, content: 'Iterator message 2' },
      ];

      // Send messages
      for (const msg of messages) {
        await producer.send(testTopic, msg);
      }

      // Wait a bit for messages to be available
      await new Promise(resolve => setTimeout(resolve, 1000));

      const receivedMessages = [];
      let count = 0;

      for await (const message of consumer) {
        receivedMessages.push(message.value);
        count++;
        if (count >= 2) {
          break;
        }
      }

      expect(receivedMessages).toHaveLength(2);
      expect(receivedMessages).toEqual(expect.arrayContaining(messages));
    }, 30000);
  });

  describe('message headers', () => {
    it('should receive messages with headers', async () => {
      const testMessage = { id: 1, content: 'Message with headers' };
      const headers = {
        'content-type': 'application/json',
        'user-id': '12345',
        'timestamp': Date.now().toString(),
      };

      // Send message with headers
      await producer.send(testTopic, testMessage, 'test-key', {
        headers,
      });

      // Wait a bit for message to be available
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Consume message
      const message = await consumer.poll(5000);
      expect(message).not.toBeNull();
      expect(message!.value).toEqual(testMessage);
      expect(message!.key).toBe('test-key');
      expect(message!.headers['content-type']).toBe('application/json');
      expect(message!.headers['user-id']).toBe('12345');
    }, 30000);
  });

  describe('different message types', () => {
    it('should handle string messages', async () => {
      const stringMessage = 'Simple string message for consumer';

      await producer.send(testTopic, stringMessage);

      // Wait a bit for message to be available
      await new Promise(resolve => setTimeout(resolve, 1000));

      const message = await consumer.poll(5000);
      expect(message).not.toBeNull();
      expect(message!.value).toBe(stringMessage);
    }, 30000);

    it('should handle number messages', async () => {
      const numberMessage = 42;

      await producer.send(testTopic, numberMessage);

      // Wait a bit for message to be available
      await new Promise(resolve => setTimeout(resolve, 1000));

      const message = await consumer.poll(5000);
      expect(message).not.toBeNull();
      expect(message!.value).toBe(numberMessage);
    }, 30000);

    it('should handle boolean messages', async () => {
      const booleanMessage = true;

      await producer.send(testTopic, booleanMessage);

      // Wait a bit for message to be available
      await new Promise(resolve => setTimeout(resolve, 1000));

      const message = await consumer.poll(5000);
      expect(message).not.toBeNull();
      expect(message!.value).toBe(booleanMessage);
    }, 30000);

    it('should handle array messages', async () => {
      const arrayMessage = [1, 2, 3, 'test', { nested: true }];

      await producer.send(testTopic, arrayMessage);

      // Wait a bit for message to be available
      await new Promise(resolve => setTimeout(resolve, 1000));

      const message = await consumer.poll(5000);
      expect(message).not.toBeNull();
      expect(message!.value).toEqual(arrayMessage);
    }, 30000);

    it('should handle complex object messages', async () => {
      const complexMessage = {
        id: 123,
        user: {
          name: 'John Doe',
          email: 'john@example.com',
          preferences: {
            theme: 'dark',
            notifications: true,
          },
        },
        tags: ['important', 'urgent'],
        metadata: {
          created: new Date().toISOString(),
          version: '1.0',
        },
      };

      await producer.send(testTopic, complexMessage);

      // Wait a bit for message to be available
      await new Promise(resolve => setTimeout(resolve, 1000));

      const message = await consumer.poll(5000);
      expect(message).not.toBeNull();
      expect(message!.value).toEqual(complexMessage);
    }, 30000);
  });

  describe('multiple topics', () => {
    it('should consume from multiple topics', async () => {
      const topic1 = `${testTopic}-1`;
      const topic2 = `${testTopic}-2`;

      const multiConsumer = new Consumer(
        [topic1, topic2],
        new Config({
          bootstrapServers: 'localhost:9092',
          groupId: `test-multi-group-${Math.random().toString(36).substring(7)}`,
          autoOffsetReset: 'earliest',
        }),
      );

      try {
        // Send messages to both topics
        await producer.send(topic1, { topic: 'topic1', message: 'Hello from topic 1' });
        await producer.send(topic2, { topic: 'topic2', message: 'Hello from topic 2' });

        // Wait a bit for messages to be available
        await new Promise(resolve => setTimeout(resolve, 1000));

        // Consume from both topics
        const messages = await multiConsumer.pollBatch(2, 10000);

        expect(messages).toHaveLength(2);

        const topics = messages.map(m => m.topic);
        expect(topics).toEqual(expect.arrayContaining([topic1, topic2]));
      } finally {
        await multiConsumer.close();
      }
    }, 30000);
  });

  describe('consumer configuration', () => {
    it('should respect autoOffsetReset earliest setting', async () => {
      // Send a message before creating consumer
      await producer.send(testTopic, { id: 1, content: 'Early message' });

      // Wait a bit
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Create consumer with earliest offset reset
      const earlyConsumer = new Consumer(
        [testTopic],
        new Config({
          bootstrapServers: 'localhost:9092',
          groupId: `test-early-group-${Math.random().toString(36).substring(7)}`,
          autoOffsetReset: 'earliest',
        }),
      );

      try {
        // Should receive the message sent before consumer creation
        const message = await earlyConsumer.poll(5000);
        expect(message).not.toBeNull();
        expect(message!.value).toEqual({ id: 1, content: 'Early message' });
      } finally {
        await earlyConsumer.close();
      }
    }, 30000);

    it('should respect autoOffsetReset latest setting', async () => {
      // Send a message before creating consumer
      await producer.send(testTopic, { id: 1, content: 'Old message' });

      // Wait a bit
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Create consumer with latest offset reset
      const latestConsumer = new Consumer(
        [testTopic],
        new Config({
          bootstrapServers: 'localhost:9092',
          groupId: `test-latest-group-${Math.random().toString(36).substring(7)}`,
          autoOffsetReset: 'latest',
        }),
      );

      try {
        // Send a new message after consumer creation
        await new Promise(resolve => setTimeout(resolve, 1000));
        await producer.send(testTopic, { id: 2, content: 'New message' });

        // Should receive only the new message
        const message = await latestConsumer.poll(5000);
        expect(message).not.toBeNull();
        expect(message!.value).toEqual({ id: 2, content: 'New message' });
      } finally {
        await latestConsumer.close();
      }
    }, 30000);
  });
});