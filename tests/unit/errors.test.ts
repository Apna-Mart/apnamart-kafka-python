import { describe, expect, it } from 'vitest';
import {
  ConfigurationError,
  ConsumerError,
  KafkaError,
  ProducerError,
  SerializationError,
  TransactionError,
} from '../../src/index.ts';

describe('Error Classes', () => {
  describe('KafkaError', () => {
    it('should create basic error', () => {
      const error = new KafkaError('Test error');

      expect(error.name).toBe('KafkaError');
      expect(error.message).toBe('Test error');
      expect(error.cause).toBeUndefined();
      expect(error instanceof Error).toBe(true);
      expect(error instanceof KafkaError).toBe(true);
    });

    it('should create error with cause', () => {
      const cause = new Error('Original error');
      const error = new KafkaError('Wrapper error', cause);

      expect(error.name).toBe('KafkaError');
      expect(error.message).toBe('Wrapper error');
      expect(error.cause).toBe(cause);
      expect(error.stack).toContain('Caused by:');
      expect(error.stack).toContain('Original error');
    });

    it('should handle undefined cause', () => {
      const error = new KafkaError('Test error', undefined);

      expect(error.name).toBe('KafkaError');
      expect(error.message).toBe('Test error');
      expect(error.cause).toBeUndefined();
    });
  });

  describe('ProducerError', () => {
    it('should extend KafkaError', () => {
      const error = new ProducerError('Producer failed');

      expect(error.name).toBe('ProducerError');
      expect(error.message).toBe('Producer failed');
      expect(error instanceof Error).toBe(true);
      expect(error instanceof KafkaError).toBe(true);
      expect(error instanceof ProducerError).toBe(true);
    });

    it('should create error with cause', () => {
      const cause = new Error('Connection failed');
      const error = new ProducerError('Producer failed', cause);

      expect(error.name).toBe('ProducerError');
      expect(error.message).toBe('Producer failed');
      expect(error.cause).toBe(cause);
      expect(error.stack).toContain('Caused by:');
    });
  });

  describe('ConsumerError', () => {
    it('should extend KafkaError', () => {
      const error = new ConsumerError('Consumer failed');

      expect(error.name).toBe('ConsumerError');
      expect(error.message).toBe('Consumer failed');
      expect(error instanceof Error).toBe(true);
      expect(error instanceof KafkaError).toBe(true);
      expect(error instanceof ConsumerError).toBe(true);
    });

    it('should create error with cause', () => {
      const cause = new Error('Subscription failed');
      const error = new ConsumerError('Consumer failed', cause);

      expect(error.name).toBe('ConsumerError');
      expect(error.message).toBe('Consumer failed');
      expect(error.cause).toBe(cause);
    });
  });

  describe('TransactionError', () => {
    it('should extend KafkaError', () => {
      const error = new TransactionError('Transaction failed');

      expect(error.name).toBe('TransactionError');
      expect(error.message).toBe('Transaction failed');
      expect(error instanceof Error).toBe(true);
      expect(error instanceof KafkaError).toBe(true);
      expect(error instanceof TransactionError).toBe(true);
    });

    it('should create error with cause', () => {
      const cause = new Error('Commit failed');
      const error = new TransactionError('Transaction failed', cause);

      expect(error.name).toBe('TransactionError');
      expect(error.message).toBe('Transaction failed');
      expect(error.cause).toBe(cause);
    });
  });

  describe('SerializationError', () => {
    it('should extend KafkaError', () => {
      const error = new SerializationError('Serialization failed');

      expect(error.name).toBe('SerializationError');
      expect(error.message).toBe('Serialization failed');
      expect(error instanceof Error).toBe(true);
      expect(error instanceof KafkaError).toBe(true);
      expect(error instanceof SerializationError).toBe(true);
    });

    it('should create error with cause', () => {
      const cause = new Error('JSON parse failed');
      const error = new SerializationError('Serialization failed', cause);

      expect(error.name).toBe('SerializationError');
      expect(error.message).toBe('Serialization failed');
      expect(error.cause).toBe(cause);
    });
  });

  describe('ConfigurationError', () => {
    it('should extend KafkaError', () => {
      const error = new ConfigurationError('Configuration invalid');

      expect(error.name).toBe('ConfigurationError');
      expect(error.message).toBe('Configuration invalid');
      expect(error instanceof Error).toBe(true);
      expect(error instanceof KafkaError).toBe(true);
      expect(error instanceof ConfigurationError).toBe(true);
    });

    it('should create error with cause', () => {
      const cause = new Error('Invalid value');
      const error = new ConfigurationError('Configuration invalid', cause);

      expect(error.name).toBe('ConfigurationError');
      expect(error.message).toBe('Configuration invalid');
      expect(error.cause).toBe(cause);
    });
  });

  describe('Error inheritance', () => {
    it('should properly handle instanceof checks', () => {
      const errors = [
        new ProducerError('test'),
        new ConsumerError('test'),
        new TransactionError('test'),
        new SerializationError('test'),
        new ConfigurationError('test'),
      ];

      for (const error of errors) {
        expect(error instanceof Error).toBe(true);
        expect(error instanceof KafkaError).toBe(true);
      }

      expect(errors[0] instanceof ProducerError).toBe(true);
      expect(errors[1] instanceof ConsumerError).toBe(true);
      expect(errors[2] instanceof TransactionError).toBe(true);
      expect(errors[3] instanceof SerializationError).toBe(true);
      expect(errors[4] instanceof ConfigurationError).toBe(true);
    });

    it('should have different error names', () => {
      const errors = [
        new KafkaError('test'),
        new ProducerError('test'),
        new ConsumerError('test'),
        new TransactionError('test'),
        new SerializationError('test'),
        new ConfigurationError('test'),
      ];

      const names = errors.map((e) => e.name);
      const uniqueNames = new Set(names);

      expect(uniqueNames.size).toBe(names.length);
      expect(names).toEqual([
        'KafkaError',
        'ProducerError',
        'ConsumerError',
        'TransactionError',
        'SerializationError',
        'ConfigurationError',
      ]);
    });
  });
});
