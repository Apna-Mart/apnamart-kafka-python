import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { Config, Consumer, Producer } from '../../src/index.ts';
import '../setup.ts'; // Import global test utilities

describe('Basic Integration Tests', () => {
  let producer: Producer;
  let consumer: Consumer;
  let testTopic: string;

  beforeEach(() => {
    testTopic = `test-topic-${Math.random().toString(36).substring(7)}-${Date.now()}`;

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
        groupId: `test-group-${Math.random().toString(36).substring(7)}`,
      }),
    );
  });

  afterEach(async () => {
    await producer?.close();
    await consumer?.close();
  });

  it('should send and receive a simple message', async () => {
    const testMessage = { id: 1, message: 'Hello Kafka!' };

    // Send message (this will create the topic)
    await producer.send(testTopic, testMessage);

    // Wait longer for KRaft mode topic creation and message propagation
    await new Promise((resolve) => setTimeout(resolve, 3000));

    // Receive message with extended timeout
    const receivedMessage = await consumer.poll(15000);

    expect(receivedMessage).not.toBeNull();
    expect(receivedMessage?.topic).toBe(testTopic);
    expect(receivedMessage?.value).toEqual(testMessage);
  }, 60000);

  it('should handle message with key', async () => {
    // Wait for topic to be ready before producing
    await waitForTopicReady(testTopic);

    const testMessage = { id: 2, message: 'Hello with key!' };
    const testKey = 'user-123';

    // Send message with key
    await producer.send(testTopic, testMessage, testKey);

    // Extended wait for KRaft mode message propagation
    await new Promise((resolve) => setTimeout(resolve, 1000));

    // Receive message
    const receivedMessage = await consumer.poll(10000);

    expect(receivedMessage).not.toBeNull();
    expect(receivedMessage?.topic).toBe(testTopic);
    expect(receivedMessage?.value).toEqual(testMessage);
    expect(receivedMessage?.key).toBe(testKey);
  }, 60000);

  it('should handle string messages', async () => {
    const testMessage = 'Simple string message';

    // Send string message
    await producer.send(testTopic, testMessage);

    // Receive message
    const receivedMessage = await consumer.poll(5000);

    expect(receivedMessage).not.toBeNull();
    expect(receivedMessage?.topic).toBe(testTopic);
    expect(receivedMessage?.value).toBe(testMessage);
  }, 60000);

  it('should handle multiple messages', async () => {
    const messages = [
      { id: 1, message: 'First message' },
      { id: 2, message: 'Second message' },
      { id: 3, message: 'Third message' },
    ];

    // Send multiple messages
    for (const msg of messages) {
      await producer.send(testTopic, msg);
    }

    // Receive all messages
    const receivedMessages = [];
    for (let i = 0; i < messages.length; i++) {
      const msg = await consumer.poll(5000);
      expect(msg).not.toBeNull();
      if (msg?.value) receivedMessages.push(msg.value);
    }

    expect(receivedMessages).toHaveLength(3);
    expect(receivedMessages).toEqual(expect.arrayContaining(messages));
  }, 60000);
});
