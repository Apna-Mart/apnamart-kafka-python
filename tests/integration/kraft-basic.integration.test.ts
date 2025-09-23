import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { Config, Consumer, Producer } from '../../src/index.ts';
import {
  createKRaftTestConfig,
  createKRaftConsumerConfig,
  waitForKRaftSync,
  KRAFT_WAIT_TIMES,
} from '../kraft-config.ts';

describe('KRaft Basic Integration Tests', () => {
  let producer: Producer;
  let consumer: Consumer;
  let testTopic: string;
  let testGroup: string;

  beforeEach(async () => {
    testTopic = `kraft-test-${Math.random().toString(36).substring(7)}-${Date.now()}`;
    testGroup = `kraft-group-${Math.random().toString(36).substring(7)}`;

    // Use KRaft optimized configurations
    producer = new Producer(createKRaftTestConfig());
    consumer = new Consumer([testTopic], createKRaftConsumerConfig(testGroup));

    // Pre-create topic and wait for full availability
    await producer.ensureTopicExists(testTopic);
    await waitForKRaftSync(KRAFT_WAIT_TIMES.topicCreation);
  });

  afterEach(async () => {
    await producer?.close();
    await consumer?.close();
  });

  it('should send and receive message with KRaft optimizations', async () => {
    const testMessage = { id: 1, content: 'KRaft test message' };

    // Send message
    await producer.send(testTopic, testMessage);
    await waitForKRaftSync(KRAFT_WAIT_TIMES.messageProduction);

    // Start consumer and wait for readiness
    await waitForKRaftSync(KRAFT_WAIT_TIMES.consumerStart);

    // Poll with extended timeout for KRaft
    const receivedMessage = await consumer.poll(20000);

    expect(receivedMessage).not.toBeNull();
    expect(receivedMessage!.value).toEqual(testMessage);
    expect(receivedMessage!.topic).toBe(testTopic);
  }, 60000);

  it('should handle batch messages with KRaft timing', async () => {
    const messages = [
      [testTopic, { id: 1, content: 'Batch 1' }],
      [testTopic, { id: 2, content: 'Batch 2' }],
      [testTopic, { id: 3, content: 'Batch 3' }],
    ] as const;

    // Send batch
    await producer.sendBatch(messages);
    await waitForKRaftSync(KRAFT_WAIT_TIMES.messageProduction);

    // Wait for consumer readiness
    await waitForKRaftSync(KRAFT_WAIT_TIMES.consumerStart);

    // Receive all messages with KRaft-appropriate timeouts
    const receivedMessages = [];
    for (let i = 0; i < messages.length; i++) {
      const msg = await consumer.poll(15000);
      expect(msg).not.toBeNull();
      receivedMessages.push(msg!.value);
    }

    expect(receivedMessages).toHaveLength(3);
    expect(receivedMessages).toEqual(
      expect.arrayContaining([
        { id: 1, content: 'Batch 1' },
        { id: 2, content: 'Batch 2' },
        { id: 3, content: 'Batch 3' },
      ]),
    );
  }, 90000);
});
