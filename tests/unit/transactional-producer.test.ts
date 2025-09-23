import { beforeEach, describe, expect, it, vi } from 'vitest';
import {
  Config,
  type MessageInput,
  TransactionalProducer,
  TransactionError,
} from '../../src/index.ts';

// Mock transaction object
const mockCommit = vi.fn();
const mockAbort = vi.fn();
const mockSend = vi.fn();

const mockTransaction = {
  commit: mockCommit,
  abort: mockAbort,
  send: mockSend,
};

// Mock KafkaJS Producer
const mockConnect = vi.fn();
const mockDisconnect = vi.fn();
const mockCreateTransaction = vi.fn();

const mockProducer = {
  connect: mockConnect,
  disconnect: mockDisconnect,
  transaction: mockCreateTransaction,
};

const mockKafka = {
  producer: vi.fn(() => mockProducer),
};

vi.mock('kafkajs', () => ({
  Kafka: vi.fn(() => mockKafka),
}));

describe('TransactionalProducer', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockConnect.mockResolvedValue(undefined);
    mockDisconnect.mockResolvedValue(undefined);
    mockCreateTransaction.mockResolvedValue(mockTransaction);
    mockCommit.mockResolvedValue(undefined);
    mockAbort.mockResolvedValue(undefined);
    mockSend.mockResolvedValue([{ partition: 0, offset: '123' }]);
  });

  describe('constructor', () => {
    it('should create transactional producer with default config', () => {
      const producer = new TransactionalProducer('tx-id-1');
      expect(producer).toBeInstanceOf(TransactionalProducer);
    });

    it('should create transactional producer with custom config', () => {
      const config = new Config({ bootstrapServers: 'localhost:9093' });
      const producer = new TransactionalProducer('tx-id-2', config);
      expect(producer).toBeInstanceOf(TransactionalProducer);
    });
  });

  describe('transaction lifecycle', () => {
    it('should begin transaction successfully', async () => {
      const producer = new TransactionalProducer('tx-id-1');

      await producer.begin();

      expect(mockConnect).toHaveBeenCalledOnce();
      expect(mockCreateTransaction).toHaveBeenCalledOnce();
    });

    it('should commit transaction successfully', async () => {
      const producer = new TransactionalProducer('tx-id-1');

      await producer.begin();
      await producer.commit();

      expect(mockCommit).toHaveBeenCalledOnce();
    });

    it('should abort transaction successfully', async () => {
      const producer = new TransactionalProducer('tx-id-1');

      await producer.begin();
      await producer.abort();

      expect(mockAbort).toHaveBeenCalledOnce();
    });

    it('should throw error when beginning transaction twice', async () => {
      const producer = new TransactionalProducer('tx-id-1');

      await producer.begin();

      await expect(producer.begin()).rejects.toThrow(
        'Transaction already in progress',
      );
    });

    it('should throw error when committing without transaction', async () => {
      const producer = new TransactionalProducer('tx-id-1');

      await expect(producer.commit()).rejects.toThrow(
        'No transaction in progress',
      );
    });

    it('should throw error when aborting without transaction', async () => {
      const producer = new TransactionalProducer('tx-id-1');

      await expect(producer.abort()).rejects.toThrow(
        'No transaction in progress',
      );
    });

    it('should handle begin transaction errors', async () => {
      mockCreateTransaction.mockRejectedValue(new Error('Begin failed'));
      const producer = new TransactionalProducer('tx-id-1');

      await expect(producer.begin()).rejects.toThrow(TransactionError);
    });

    it('should handle commit transaction errors', async () => {
      mockCommit.mockRejectedValue(new Error('Commit failed'));
      const producer = new TransactionalProducer('tx-id-1');

      await producer.begin();

      await expect(producer.commit()).rejects.toThrow(TransactionError);
    });

    it('should handle abort transaction errors', async () => {
      mockAbort.mockRejectedValue(new Error('Abort failed'));
      const producer = new TransactionalProducer('tx-id-1');

      await producer.begin();

      await expect(producer.abort()).rejects.toThrow(TransactionError);
    });
  });

  describe('sendTransactional', () => {
    it('should send transactional message successfully', async () => {
      const producer = new TransactionalProducer('tx-id-1');

      await producer.begin();
      await producer.sendTransactional('test-topic', { message: 'hello' });

      expect(mockSend).toHaveBeenCalledOnce();
      expect(mockSend).toHaveBeenCalledWith({
        topic: 'test-topic',
        messages: [
          {
            key: undefined,
            value: expect.any(Buffer),
            partition: undefined,
            headers: undefined,
          },
        ],
      });
    });

    it('should send transactional message with key', async () => {
      const producer = new TransactionalProducer('tx-id-1');

      await producer.begin();
      await producer.sendTransactional(
        'test-topic',
        { message: 'hello' },
        'user-123',
      );

      expect(mockSend).toHaveBeenCalledWith({
        topic: 'test-topic',
        messages: [
          {
            key: expect.any(Buffer),
            value: expect.any(Buffer),
            partition: undefined,
            headers: undefined,
          },
        ],
      });
    });

    it('should send transactional message with options', async () => {
      const producer = new TransactionalProducer('tx-id-1');

      await producer.begin();
      await producer.sendTransactional(
        'test-topic',
        { message: 'hello' },
        'key',
        {
          partition: 1,
          headers: { 'content-type': 'application/json' },
        },
      );

      expect(mockSend).toHaveBeenCalledWith({
        topic: 'test-topic',
        messages: [
          {
            key: expect.any(Buffer),
            value: expect.any(Buffer),
            partition: 1,
            headers: { 'content-type': 'application/json' },
          },
        ],
      });
    });

    it('should throw error when no transaction in progress', async () => {
      const producer = new TransactionalProducer('tx-id-1');

      await expect(
        producer.sendTransactional('test-topic', { message: 'hello' }),
      ).rejects.toThrow('No active transaction. Call begin() first.');
    });

    it('should handle send errors', async () => {
      mockSend.mockRejectedValue(new Error('Send failed'));
      const producer = new TransactionalProducer('tx-id-1');

      await producer.begin();

      await expect(
        producer.sendTransactional('test-topic', { message: 'hello' }),
      ).rejects.toThrow(TransactionError);
    });
  });

  describe('sendBatchTransactional', () => {
    it('should send batch messages transactionally', async () => {
      const producer = new TransactionalProducer('tx-id-1');

      const messages: MessageInput[] = [
        ['topic1', { id: 1 }],
        ['topic2', { id: 2 }, 'key2'],
        {
          topic: 'topic3',
          value: { id: 3 },
          key: 'key3',
          partition: 1,
          headers: { type: 'test' },
        },
      ];

      await producer.sendBatchTransactional(messages);

      expect(mockCreateTransaction).toHaveBeenCalledOnce();
      expect(mockSend).toHaveBeenCalledTimes(3);
      expect(mockCommit).toHaveBeenCalledOnce();
    });

    it('should abort transaction on error during batch send', async () => {
      mockSend.mockRejectedValueOnce(new Error('Send failed'));
      const producer = new TransactionalProducer('tx-id-1');

      const messages: MessageInput[] = [
        ['topic1', { id: 1 }],
        ['topic2', { id: 2 }],
      ];

      await expect(producer.sendBatchTransactional(messages)).rejects.toThrow(
        TransactionError,
      );

      expect(mockCreateTransaction).toHaveBeenCalledOnce();
      expect(mockAbort).toHaveBeenCalledOnce();
    });

    it('should handle invalid message format in batch', async () => {
      const producer = new TransactionalProducer('tx-id-1');

      const messages: MessageInput[] = [
        'invalid-format' as unknown as MessageInput,
      ];

      await expect(producer.sendBatchTransactional(messages)).rejects.toThrow(
        TransactionError,
      );

      expect(mockAbort).toHaveBeenCalledOnce();
    });

    it('should throw error when transaction already in progress', async () => {
      const producer = new TransactionalProducer('tx-id-1');

      await producer.begin();

      const messages: MessageInput[] = [['topic1', { id: 1 }]];

      await expect(producer.sendBatchTransactional(messages)).rejects.toThrow(
        'Transaction already in progress',
      );
    });
  });

  describe('close', () => {
    it('should close producer successfully', async () => {
      const producer = new TransactionalProducer('tx-id-1');

      // First connect the producer
      await producer.begin();
      await producer.close();

      expect(mockDisconnect).toHaveBeenCalledOnce();
    });

    it('should abort transaction before closing if in progress', async () => {
      const producer = new TransactionalProducer('tx-id-1');

      await producer.begin();
      await producer.close();

      expect(mockAbort).toHaveBeenCalledOnce();
      expect(mockDisconnect).toHaveBeenCalledOnce();
    });

    it('should handle abort errors during close gracefully', async () => {
      mockAbort.mockRejectedValue(new Error('Abort failed'));
      const producer = new TransactionalProducer('tx-id-1');

      await producer.begin();

      // Should not throw
      await expect(producer.close()).resolves.toBeUndefined();
    });
  });

  describe('async dispose', () => {
    it('should dispose producer using Symbol.asyncDispose', async () => {
      const producer = new TransactionalProducer('tx-id-1');

      // First connect the producer
      await producer.begin();
      await producer[Symbol.asyncDispose]();

      expect(mockDisconnect).toHaveBeenCalledOnce();
    });

    it('should commit transaction before dispose if in progress', async () => {
      const producer = new TransactionalProducer('tx-id-1');

      await producer.begin();
      await producer[Symbol.asyncDispose]();

      expect(mockCommit).toHaveBeenCalledOnce();
      expect(mockDisconnect).toHaveBeenCalledOnce();
    });

    it('should abort transaction if commit fails during dispose', async () => {
      mockCommit.mockRejectedValue(new Error('Commit failed'));
      const producer = new TransactionalProducer('tx-id-1');

      await producer.begin();
      await producer[Symbol.asyncDispose]();

      expect(mockCommit).toHaveBeenCalledOnce();
      expect(mockAbort).toHaveBeenCalledOnce();
      expect(mockDisconnect).toHaveBeenCalledOnce();
    });
  });

  describe('configuration', () => {
    it('should configure producer with transactional settings', async () => {
      const producer = new TransactionalProducer('tx-id-1');

      // Trigger connection to check configuration
      await producer.begin();

      expect(mockKafka.producer).toHaveBeenCalledWith(
        expect.objectContaining({
          transactionalId: 'tx-id-1',
          maxInFlightRequests: 5, // Updated for performance
          idempotent: true,
          compression: 'gzip', // Added for performance
        }),
      );
    });
  });

  describe('error states', () => {
    it('should throw error when sending to closed producer', async () => {
      const producer = new TransactionalProducer('tx-id-1');

      await producer.close();

      await expect(producer.begin()).rejects.toThrow(TransactionError);
    });

    it('should handle connection errors', async () => {
      mockConnect.mockRejectedValue(new Error('Connection failed'));
      const producer = new TransactionalProducer('tx-id-1');

      await expect(producer.begin()).rejects.toThrow(TransactionError);
    });
  });
});
