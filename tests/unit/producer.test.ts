import { beforeEach, describe, expect, it, vi } from 'vitest';
import {
  Config,
  type MessageInput,
  Producer,
  ProducerError,
} from '../../src/index.ts';

// Mock KafkaJS
const mockSend = vi.fn();
const mockConnect = vi.fn();
const mockDisconnect = vi.fn();
const mockProducer = {
  connect: mockConnect,
  disconnect: mockDisconnect,
  send: mockSend,
};
const mockKafka = {
  producer: vi.fn(() => mockProducer),
};

vi.mock('kafkajs', () => ({
  Kafka: vi.fn(() => mockKafka),
}));

describe('Producer', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockConnect.mockResolvedValue(undefined);
    mockDisconnect.mockResolvedValue(undefined);
    mockSend.mockResolvedValue([
      {
        partition: 0,
        offset: '123',
      },
    ]);
  });

  describe('constructor', () => {
    it('should create producer with default config', () => {
      const producer = new Producer();
      expect(producer).toBeInstanceOf(Producer);
    });

    it('should create producer with custom config', () => {
      const config = new Config({ bootstrapServers: 'localhost:9093' });
      const producer = new Producer(config);
      expect(producer).toBeInstanceOf(Producer);
    });
  });

  describe('send', () => {
    it('should send message successfully', async () => {
      const producer = new Producer();

      const result = await producer.send('test-topic', { message: 'hello' });

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
      expect(result).toEqual([
        {
          partition: 0,
          offset: '123',
        },
      ]);
    });

    it('should send message with key', async () => {
      const producer = new Producer();

      await producer.send('test-topic', { message: 'hello' }, 'user-123');

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

    it('should send message with options', async () => {
      const producer = new Producer();

      await producer.send('test-topic', { message: 'hello' }, 'key', {
        partition: 1,
        headers: { 'content-type': 'application/json' },
        timestamp: '1234567890',
      });

      expect(mockSend).toHaveBeenCalledWith({
        topic: 'test-topic',
        messages: [
          {
            key: expect.any(Buffer),
            value: expect.any(Buffer),
            partition: 1,
            headers: { 'content-type': 'application/json' },
            timestamp: '1234567890',
          },
        ],
      });
    });

    it('should throw error for empty topic', async () => {
      const producer = new Producer();

      await expect(producer.send('', { message: 'hello' })).rejects.toThrow(
        ProducerError,
      );
    });

    it('should throw error when producer is closed', async () => {
      const producer = new Producer();
      await producer.close();

      await expect(
        producer.send('test-topic', { message: 'hello' }),
      ).rejects.toThrow(ProducerError);
    });

    it('should handle connection errors', async () => {
      mockConnect.mockRejectedValue(new Error('Connection failed'));
      const producer = new Producer();

      await expect(
        producer.send('test-topic', { message: 'hello' }),
      ).rejects.toThrow(ProducerError);
    });

    it('should handle send errors', async () => {
      mockSend.mockRejectedValue(new Error('Send failed'));
      const producer = new Producer();

      await expect(
        producer.send('test-topic', { message: 'hello' }),
      ).rejects.toThrow(ProducerError);
    });
  });

  describe('sendBatch', () => {
    it('should send batch messages successfully', async () => {
      const producer = new Producer();
      mockSend.mockResolvedValue([
        { partition: 0, offset: '123' },
        { partition: 0, offset: '124' },
      ]);

      const messages = [
        ['topic1', { id: 1 }],
        ['topic1', { id: 2 }, 'key2'],
      ];

      const results = await producer.sendBatch(messages);

      expect(results).toHaveLength(2);
      expect(results[0]).toEqual({
        success: true,
        topic: 'topic1',
        partition: 0,
        offset: '123',
      });
      expect(results[1]).toEqual({
        success: true,
        topic: 'topic1',
        partition: 0,
        offset: '124',
      });
    });

    it('should handle mixed message formats', async () => {
      const producer = new Producer();
      mockSend.mockResolvedValue([
        { partition: 0, offset: '123' },
        { partition: 0, offset: '124' },
      ]);

      const messages = [
        ['topic1', { id: 1 }],
        { topic: 'topic1', value: { id: 2 }, key: 'key2' },
      ];

      const results = await producer.sendBatch(messages);

      expect(results).toHaveLength(2);
      expect(results.every((r) => r.success)).toBe(true);
    });

    it('should handle errors in individual messages', async () => {
      const producer = new Producer();

      const messages = ['invalid-format' as unknown as MessageInput];

      const results = await producer.sendBatch(messages);

      expect(results).toHaveLength(1);
      expect(results[0].success).toBe(false);
      expect(results[0].error).toContain('Failed to process message');
    });
  });

  describe('close', () => {
    it('should close producer successfully', async () => {
      const producer = new Producer();
      await producer.send('test-topic', { message: 'hello' });

      await producer.close();

      expect(mockDisconnect).toHaveBeenCalledOnce();
    });

    it('should handle close errors gracefully', async () => {
      mockDisconnect.mockRejectedValue(new Error('Disconnect failed'));
      const producer = new Producer();
      await producer.send('test-topic', { message: 'hello' });

      // Should not throw
      await expect(producer.close()).resolves.toBeUndefined();
    });

    it('should allow multiple close calls', async () => {
      const producer = new Producer();

      await producer.close();
      await producer.close();

      // Should only disconnect once if producer was connected
      expect(mockDisconnect).toHaveBeenCalledTimes(0);
    });
  });

  describe('async dispose', () => {
    it('should dispose producer using Symbol.asyncDispose', async () => {
      const producer = new Producer();
      await producer.send('test-topic', { message: 'hello' });

      await producer[Symbol.asyncDispose]();

      expect(mockDisconnect).toHaveBeenCalledOnce();
    });
  });
});
