import { beforeEach, describe, expect, it, vi } from 'vitest';
import { Config, Consumer, ConsumerError } from '../../src/index.ts';

// Mock KafkaJS
const mockRun = vi.fn();
const mockConnect = vi.fn();
const mockDisconnect = vi.fn();
const mockSubscribe = vi.fn();
const mockCommitOffsets = vi.fn();
const mockSeek = vi.fn();
const mockPause = vi.fn();

const mockConsumer = {
  connect: mockConnect,
  disconnect: mockDisconnect,
  subscribe: mockSubscribe,
  run: mockRun,
  commitOffsets: mockCommitOffsets,
  seek: mockSeek,
  pause: mockPause,
};

const mockKafka = {
  consumer: vi.fn(() => mockConsumer),
};

vi.mock('kafkajs', () => ({
  Kafka: vi.fn(() => mockKafka),
}));

describe('Consumer', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockConnect.mockResolvedValue(undefined);
    mockDisconnect.mockResolvedValue(undefined);
    mockSubscribe.mockResolvedValue(undefined);
    mockCommitOffsets.mockResolvedValue(undefined);
    mockRun.mockResolvedValue(undefined);
  });

  describe('constructor', () => {
    it('should create consumer with single topic', () => {
      const consumer = new Consumer('test-topic');
      expect(consumer).toBeInstanceOf(Consumer);
    });

    it('should create consumer with multiple topics', () => {
      const consumer = new Consumer(['topic1', 'topic2']);
      expect(consumer).toBeInstanceOf(Consumer);
    });

    it('should create consumer with custom config', () => {
      const config = new Config({ bootstrapServers: 'localhost:9093' });
      const consumer = new Consumer(['test-topic'], config);
      expect(consumer).toBeInstanceOf(Consumer);
    });

    it('should create consumer with empty topics array', () => {
      const consumer = new Consumer([]);
      expect(consumer).toBeInstanceOf(Consumer);
    });
  });

  describe('poll', () => {
    it('should poll and return message', async () => {
      const consumer = new Consumer(['test-topic']);

      // Mock the run method to simulate receiving a message
      mockRun.mockImplementation(({ eachMessage }) => {
        setTimeout(() => {
          eachMessage({
            topic: 'test-topic',
            partition: 0,
            message: {
              offset: '123',
              key: Buffer.from('test-key'),
              value: Buffer.from(JSON.stringify({ message: 'hello' })),
              timestamp: '1234567890',
              headers: {},
            },
          });
        }, 10);
        return Promise.resolve();
      });

      const message = await consumer.poll(1000);

      expect(mockConnect).toHaveBeenCalledOnce();
      expect(mockSubscribe).toHaveBeenCalledWith({
        topics: ['test-topic'],
        fromBeginning: false,
      });
      expect(message).not.toBeNull();
      expect(message?.topic).toBe('test-topic');
      expect(message?.value).toEqual({ message: 'hello' });
      expect(message?.key).toBe('test-key');
    });

    it('should return null on timeout', async () => {
      const consumer = new Consumer(['test-topic']);

      // Mock run to not call eachMessage
      mockRun.mockResolvedValue(undefined);

      const message = await consumer.poll(100);

      expect(message).toBeNull();
    });

    it('should subscribe with fromBeginning when autoOffsetReset is earliest', async () => {
      const config = new Config({ autoOffsetReset: 'earliest' });
      const consumer = new Consumer(['test-topic'], config);

      mockRun.mockResolvedValue(undefined);
      await consumer.poll(100);

      expect(mockSubscribe).toHaveBeenCalledWith({
        topics: ['test-topic'],
        fromBeginning: true,
      });
    });

    it('should throw error when consumer is closed', async () => {
      const consumer = new Consumer(['test-topic']);
      await consumer.close();

      await expect(consumer.poll()).rejects.toThrow(ConsumerError);
    });

    it('should throw error when not subscribed to any topics', async () => {
      const consumer = new Consumer([]);

      await expect(consumer.poll()).rejects.toThrow('Consumer is not subscribed to any topics');
    });

    it('should handle connection errors', async () => {
      mockConnect.mockRejectedValue(new Error('Connection failed'));
      const consumer = new Consumer(['test-topic']);

      await expect(consumer.poll()).rejects.toThrow(ConsumerError);
    });

    it('should handle message processing errors', async () => {
      const consumer = new Consumer(['test-topic']);

      mockRun.mockImplementation(({ eachMessage }) => {
        setTimeout(() => {
          eachMessage({
            topic: 'test-topic',
            partition: 0,
            message: {
              offset: '123',
              key: null,
              value: null, // This will cause deserialization to return null
              timestamp: '1234567890',
              headers: {},
            },
          });
        }, 10);
        return Promise.resolve();
      });

      const message = await consumer.poll(1000);
      expect(message?.value).toBe(null);
    });
  });

  describe('pollBatch', () => {
    it('should poll multiple messages', async () => {
      const consumer = new Consumer(['test-topic']);
      let messageCount = 0;

      mockRun.mockImplementation(({ eachMessage }) => {
        // Each time poll() is called, we'll send one message with incremented ID
        setTimeout(() => {
          eachMessage({
            topic: 'test-topic',
            partition: 0,
            message: {
              offset: (123 + messageCount).toString(),
              key: Buffer.from(`key-${messageCount}`),
              value: Buffer.from(JSON.stringify({ id: messageCount })),
              timestamp: '1234567890',
              headers: {},
            },
          });
          messageCount++;
        }, 10);
        return Promise.resolve();
      });

      const messages = await consumer.pollBatch(3, 5000);

      expect(messages).toHaveLength(3);
      expect(messages[0].value).toEqual({ id: 0 });
      expect(messages[1].value).toEqual({ id: 1 });
      expect(messages[2].value).toEqual({ id: 2 });
    });

    it('should return empty array on timeout', async () => {
      const consumer = new Consumer(['test-topic']);
      mockRun.mockResolvedValue(undefined);

      const messages = await consumer.pollBatch(5, 100);

      expect(messages).toHaveLength(0);
    });

    it('should stop polling when no more messages', async () => {
      const consumer = new Consumer(['test-topic']);
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
                value: Buffer.from(JSON.stringify({ id: 1 })),
                timestamp: '1234567890',
                headers: {},
              },
            });
            messageCount++;
          }, 10);
        }
        return Promise.resolve();
      });

      const messages = await consumer.pollBatch(5, 5000);

      expect(messages).toHaveLength(1);
      expect(messages[0].value).toEqual({ id: 1 });
    });
  });

  describe('commit', () => {
    it('should commit specific message offset', async () => {
      const consumer = new Consumer(['test-topic']);

      // Create a mock message
      const mockMessage = {
        topic: 'test-topic',
        partition: 0,
        offset: '123',
        key: null,
        value: { test: true },
        timestamp: '1234567890',
        headers: {},
      };

      await consumer.commit(mockMessage as any);

      expect(mockCommitOffsets).toHaveBeenCalledWith([
        {
          topic: 'test-topic',
          partition: 0,
          offset: '124', // offset + 1
        },
      ]);
    });

    it('should commit current offsets when no message provided', async () => {
      const consumer = new Consumer(['test-topic']);

      await consumer.commit();

      expect(mockCommitOffsets).toHaveBeenCalledWith([]);
    });

    it('should throw error when consumer is closed', async () => {
      const consumer = new Consumer(['test-topic']);
      await consumer.close();

      await expect(consumer.commit()).rejects.toThrow(ConsumerError);
    });

    it('should handle commit errors', async () => {
      mockCommitOffsets.mockRejectedValue(new Error('Commit failed'));
      const consumer = new Consumer(['test-topic']);

      await expect(consumer.commit()).rejects.toThrow(ConsumerError);
      expect(mockCommitOffsets).toHaveBeenCalled();
    });
  });

  describe('seek', () => {
    it('should seek to specified offset', async () => {
      const consumer = new Consumer(['test-topic']);

      await consumer.seek('test-topic', 0, '100');

      expect(mockConnect).toHaveBeenCalledOnce();
      expect(mockSeek).toHaveBeenCalledWith({
        topic: 'test-topic',
        partition: 0,
        offset: '100',
      });
    });

    it('should throw error when consumer is closed', async () => {
      const consumer = new Consumer(['test-topic']);
      await consumer.close();

      await expect(consumer.seek('test-topic', 0, '100')).rejects.toThrow(ConsumerError);
    });

    it('should handle seek errors', async () => {
      mockSeek.mockImplementation(() => {
        throw new Error('Seek failed');
      });
      const consumer = new Consumer(['test-topic']);

      await expect(consumer.seek('test-topic', 0, '100')).rejects.toThrow(ConsumerError);
    });
  });

  describe('close', () => {
    it('should close consumer successfully', async () => {
      const consumer = new Consumer(['test-topic']);

      // Connect first
      await consumer.poll(10);

      await consumer.close();

      expect(mockDisconnect).toHaveBeenCalledOnce();
    });

    it('should handle close errors gracefully', async () => {
      mockDisconnect.mockRejectedValue(new Error('Disconnect failed'));
      const consumer = new Consumer(['test-topic']);

      // Connect first
      await consumer.poll(10);

      // Should not throw
      await expect(consumer.close()).resolves.toBeUndefined();
    });

    it('should allow multiple close calls', async () => {
      const consumer = new Consumer(['test-topic']);

      await consumer.close();
      await consumer.close();

      // Should only disconnect once if consumer was connected
      expect(mockDisconnect).toHaveBeenCalledTimes(0);
    });
  });

  describe('async iterator', () => {
    it('should iterate over messages', async () => {
      const consumer = new Consumer(['test-topic']);
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
          messageCount++;
        }, 10);
        return Promise.resolve();
      });

      const messages = [];
      let count = 0;

      for await (const message of consumer) {
        messages.push(message);
        count++;
        if (count >= 1) {
          await consumer.close();
          break;
        }
      }

      expect(messages).toHaveLength(1);
      expect(messages[0].value).toEqual({ id: 1 });
    });
  });

  describe('async dispose', () => {
    it('should dispose consumer using Symbol.asyncDispose', async () => {
      const consumer = new Consumer(['test-topic']);

      // Connect first
      await consumer.poll(10);

      await consumer[Symbol.asyncDispose]();

      expect(mockDisconnect).toHaveBeenCalledOnce();
    });
  });

  describe('error handling', () => {
    it('should handle unknown topic error', async () => {
      mockConnect.mockRejectedValue(new Error('Unknown topic'));
      const consumer = new Consumer(['unknown-topic']);

      await expect(consumer.poll()).rejects.toThrow('Failed to connect consumer: Unknown topic');
    });

    it('should handle connection failed error', async () => {
      mockConnect.mockRejectedValue(new Error('Connection failed'));
      const consumer = new Consumer(['test-topic']);

      await expect(consumer.poll()).rejects.toThrow('Failed to connect consumer: Connection failed');
    });

    it('should handle SASL authentication error', async () => {
      mockConnect.mockRejectedValue(new Error('SASL authentication failed'));
      const consumer = new Consumer(['test-topic']);

      await expect(consumer.poll()).rejects.toThrow('Failed to connect consumer: SASL authentication failed');
    });

    it('should handle authorization error', async () => {
      mockConnect.mockRejectedValue(new Error('Not authorized'));
      const consumer = new Consumer(['test-topic']);

      await expect(consumer.poll()).rejects.toThrow('Failed to connect consumer: Not authorized');
    });

    it('should handle generic consumer error', async () => {
      mockConnect.mockRejectedValue(new Error('Some other error'));
      const consumer = new Consumer(['test-topic']);

      await expect(consumer.poll()).rejects.toThrow('Failed to connect consumer: Some other error');
    });
  });
});