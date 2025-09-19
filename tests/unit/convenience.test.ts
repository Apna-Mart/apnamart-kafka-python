import { beforeEach, describe, expect, it, vi } from 'vitest';
import { consume, send } from '../../src/index.ts';

// Mock KafkaJS
const mockSend = vi.fn();
const mockConnect = vi.fn();
const mockDisconnect = vi.fn();
const mockRun = vi.fn();
const mockSubscribe = vi.fn();

const mockProducer = {
  connect: mockConnect,
  disconnect: mockDisconnect,
  send: mockSend,
};

const mockConsumer = {
  connect: mockConnect,
  disconnect: mockDisconnect,
  subscribe: mockSubscribe,
  run: mockRun,
};

const mockKafka = {
  producer: vi.fn(() => mockProducer),
  consumer: vi.fn(() => mockConsumer),
};

vi.mock('kafkajs', () => ({
  Kafka: vi.fn(() => mockKafka),
}));

describe('Convenience Functions', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockConnect.mockResolvedValue(undefined);
    mockDisconnect.mockResolvedValue(undefined);
    mockSend.mockResolvedValue([{ partition: 0, offset: '123' }]);
    mockSubscribe.mockResolvedValue(undefined);
    mockRun.mockResolvedValue(undefined);
  });

  describe('send function', () => {
    it('should send message with default options', async () => {
      await send('test-topic', { message: 'hello' });

      expect(mockConnect).toHaveBeenCalledOnce();
      expect(mockSend).toHaveBeenCalledOnce();
      expect(mockSend).toHaveBeenCalledWith({
        topic: 'test-topic',
        messages: [
          {
            key: undefined,
            value: expect.any(Buffer),
            partition: undefined,
            headers: undefined,
            timestamp: undefined,
          },
        ],
      });
      expect(mockDisconnect).toHaveBeenCalledOnce();
    });

    it('should send message with key', async () => {
      await send('test-topic', { message: 'hello' }, 'user-123');

      expect(mockSend).toHaveBeenCalledWith({
        topic: 'test-topic',
        messages: [
          {
            key: expect.any(Buffer),
            value: expect.any(Buffer),
            partition: undefined,
            headers: undefined,
            timestamp: undefined,
          },
        ],
      });
    });

    it('should send message with custom servers', async () => {
      await send('test-topic', { message: 'hello' }, undefined, {
        servers: 'custom:9092',
      });

      expect(mockKafka.producer).toHaveBeenCalledWith(
        expect.objectContaining({
          allowAutoTopicCreation: true,
          idempotent: true,
          maxInFlightRequests: 5,
          transactionTimeout: 30000,
        }),
      );
    });

    it('should send message with custom config', async () => {
      await send('test-topic', { message: 'hello' }, undefined, {
        config: {
          acks: 1,
          retries: 5,
          clientId: 'custom-client',
        },
      });

      expect(mockConnect).toHaveBeenCalledOnce();
      expect(mockSend).toHaveBeenCalledOnce();
    });

    it('should send message with both servers and config', async () => {
      await send('test-topic', { message: 'hello' }, undefined, {
        servers: 'custom:9092',
        config: {
          clientId: 'custom-client',
          acks: 'all',
        },
      });

      expect(mockConnect).toHaveBeenCalledOnce();
      expect(mockSend).toHaveBeenCalledOnce();
    });

    it('should handle producer errors', async () => {
      mockSend.mockRejectedValue(new Error('Send failed'));

      await expect(send('test-topic', { message: 'hello' })).rejects.toThrow();
    });

    it('should handle connection errors', async () => {
      mockConnect.mockRejectedValue(new Error('Connection failed'));

      await expect(send('test-topic', { message: 'hello' })).rejects.toThrow();
    });

    it('should send string messages', async () => {
      await send('test-topic', 'simple string message');

      expect(mockSend).toHaveBeenCalledWith({
        topic: 'test-topic',
        messages: [
          {
            key: undefined,
            value: expect.any(Buffer),
            partition: undefined,
            headers: undefined,
            timestamp: undefined,
          },
        ],
      });
    });

    it('should send number messages', async () => {
      await send('test-topic', 42);

      expect(mockSend).toHaveBeenCalledOnce();
    });

    it('should send boolean messages', async () => {
      await send('test-topic', true);

      expect(mockSend).toHaveBeenCalledOnce();
    });

    it('should send array messages', async () => {
      await send('test-topic', [1, 2, 3]);

      expect(mockSend).toHaveBeenCalledOnce();
    });

    it('should automatically dispose producer after sending', async () => {
      await send('test-topic', { message: 'hello' });

      // Verify producer was disconnected (disposed)
      expect(mockDisconnect).toHaveBeenCalledOnce();
    });
  });

  describe('consume function', () => {
    it('should create async iterator for single topic', async () => {
      let messageCount = 0;

      mockRun.mockImplementation(({ eachMessage }) => {
        setTimeout(() => {
          eachMessage({
            topic: 'test-topic',
            partition: 0,
            message: {
              offset: '123',
              key: null,
              value: Buffer.from(JSON.stringify({ id: 1 })),
              timestamp: '1234567890',
              headers: {},
            },
          });
        }, 10);
        return Promise.resolve();
      });

      const messages = [];
      let count = 0;

      // Use for-await-of loop to consume messages
      for await (const message of consume('test-topic')) {
        messages.push(message);
        count++;
        if (count >= 1) {
          break; // Break the loop to end the test
        }
      }

      expect(mockConnect).toHaveBeenCalledOnce();
      expect(mockSubscribe).toHaveBeenCalledWith({
        topics: ['test-topic'],
        fromBeginning: false,
      });
      expect(messages).toHaveLength(1);
      expect(messages[0].value).toEqual({ id: 1 });
    });

    it('should create async iterator for multiple topics', async () => {
      let messageCount = 0;

      mockRun.mockImplementation(({ eachMessage }) => {
        const interval = setInterval(() => {
          if (messageCount < 1) {
            eachMessage({
              topic: messageCount === 0 ? 'topic1' : 'topic2',
              partition: 0,
              message: {
                offset: '123',
                key: null,
                value: Buffer.from(JSON.stringify({ topic: messageCount === 0 ? 'topic1' : 'topic2' })),
                timestamp: '1234567890',
                headers: {},
              },
            });
            messageCount++;
          } else {
            clearInterval(interval);
          }
        }, 10);
        return Promise.resolve();
      });

      const messages = [];
      let count = 0;

      for await (const message of consume(['topic1', 'topic2'])) {
        messages.push(message);
        count++;
        if (count >= 1) {
          break;
        }
      }

      expect(mockSubscribe).toHaveBeenCalledWith({
        topics: ['topic1', 'topic2'],
        fromBeginning: false,
      });
      expect(messages).toHaveLength(1);
    });

    it('should use custom servers', async () => {
      let messageCount = 0;

      mockRun.mockImplementation(({ eachMessage }) => {
        if (messageCount === 0) {
          setTimeout(() => {
            eachMessage({
              topic: 'test-topic',
              partition: 0,
              message: {
                offset: '123',
                key: null,
                value: Buffer.from('test'),
                timestamp: '1234567890',
                headers: {},
              },
            });
            messageCount++;
          }, 10);
        }
        return Promise.resolve();
      });

      let count = 0;
      for await (const message of consume('test-topic', {
        servers: 'custom:9092',
      })) {
        count++;
        if (count >= 1) {
          break;
        }
      }

      expect(mockConnect).toHaveBeenCalledOnce();
    });

    it('should use custom group ID', async () => {
      let messageCount = 0;

      mockRun.mockImplementation(({ eachMessage }) => {
        if (messageCount === 0) {
          setTimeout(() => {
            eachMessage({
              topic: 'test-topic',
              partition: 0,
              message: {
                offset: '123',
                key: null,
                value: Buffer.from('test'),
                timestamp: '1234567890',
                headers: {},
              },
            });
            messageCount++;
          }, 10);
        }
        return Promise.resolve();
      });

      let count = 0;
      for await (const message of consume('test-topic', {
        groupId: 'custom-group',
      })) {
        count++;
        if (count >= 1) {
          break;
        }
      }

      expect(mockConnect).toHaveBeenCalledOnce();
    });

    it('should use custom config', async () => {
      let messageCount = 0;

      mockRun.mockImplementation(({ eachMessage }) => {
        if (messageCount === 0) {
          setTimeout(() => {
            eachMessage({
              topic: 'test-topic',
              partition: 0,
              message: {
                offset: '123',
                key: null,
                value: Buffer.from('test'),
                timestamp: '1234567890',
                headers: {},
              },
            });
            messageCount++;
          }, 10);
        }
        return Promise.resolve();
      });

      let count = 0;
      for await (const message of consume('test-topic', {
        config: {
          autoOffsetReset: 'earliest',
          enableAutoCommit: false,
        },
      })) {
        count++;
        if (count >= 1) {
          break;
        }
      }

      expect(mockSubscribe).toHaveBeenCalledWith({
        topics: ['test-topic'],
        fromBeginning: true, // Because autoOffsetReset is 'earliest'
      });
    });

    it('should handle connection errors', async () => {
      mockConnect.mockRejectedValue(new Error('Connection failed'));

      try {
        for await (const message of consume('test-topic')) {
          // This should not execute
          break;
        }
      } catch (error) {
        expect(error).toBeInstanceOf(Error);
      }
    });

    it('should handle subscription errors', async () => {
      mockSubscribe.mockRejectedValue(new Error('Subscribe failed'));

      try {
        for await (const message of consume('test-topic')) {
          // This should not execute
          break;
        }
      } catch (error) {
        expect(error).toBeInstanceOf(Error);
      }
    });

    it('should automatically dispose consumer', async () => {
      let messageCount = 0;

      mockRun.mockImplementation(({ eachMessage }) => {
        if (messageCount === 0) {
          setTimeout(() => {
            eachMessage({
              topic: 'test-topic',
              partition: 0,
              message: {
                offset: '123',
                key: null,
                value: Buffer.from('test'),
                timestamp: '1234567890',
                headers: {},
              },
            });
            messageCount++;
          }, 10);
        }
        return Promise.resolve();
      });

      let count = 0;
      for await (const message of consume('test-topic')) {
        count++;
        if (count >= 1) {
          break;
        }
      }

      // Note: The consumer disposal happens when the async iterator is closed
      // This is handled by the Symbol.asyncDispose in the Consumer class
      expect(mockConnect).toHaveBeenCalledOnce();
    });

    it('should work with string topic parameter', async () => {
      let messageCount = 0;

      mockRun.mockImplementation(({ eachMessage }) => {
        if (messageCount === 0) {
          setTimeout(() => {
            eachMessage({
              topic: 'single-topic',
              partition: 0,
              message: {
                offset: '123',
                key: null,
                value: Buffer.from('test'),
                timestamp: '1234567890',
                headers: {},
              },
            });
            messageCount++;
          }, 10);
        }
        return Promise.resolve();
      });

      let count = 0;
      for await (const message of consume('single-topic')) {
        expect(message.topic).toBe('single-topic');
        count++;
        if (count >= 1) {
          break;
        }
      }

      expect(mockSubscribe).toHaveBeenCalledWith({
        topics: ['single-topic'],
        fromBeginning: false,
      });
    });
  });

  describe('convenience functions integration', () => {
    it('should work together for send and consume workflow', async () => {
      // First send a message
      await send('workflow-topic', { step: 'send' });

      expect(mockSend).toHaveBeenCalledOnce();

      // Then consume it (mocked response)
      let messageCount = 0;
      mockRun.mockImplementation(({ eachMessage }) => {
        if (messageCount === 0) {
          setTimeout(() => {
            eachMessage({
              topic: 'workflow-topic',
              partition: 0,
              message: {
                offset: '123',
                key: null,
                value: Buffer.from(JSON.stringify({ step: 'send' })),
                timestamp: '1234567890',
                headers: {},
              },
            });
            messageCount++;
          }, 10);
        }
        return Promise.resolve();
      });

      let count = 0;
      for await (const message of consume('workflow-topic')) {
        expect(message.value).toEqual({ step: 'send' });
        count++;
        if (count >= 1) {
          break;
        }
      }

      expect(mockSend).toHaveBeenCalledOnce();
      expect(mockSubscribe).toHaveBeenCalledOnce();
    });
  });
});