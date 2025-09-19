import { describe, expect, it } from 'vitest';
import type { EachMessagePayload } from 'kafkajs';
import { Message } from '../../src/index.ts';

describe('Message', () => {
  describe('constructor', () => {
    it('should create message from KafkaJS payload with all fields', () => {
      const payload: EachMessagePayload = {
        topic: 'test-topic',
        partition: 0,
        message: {
          offset: '123',
          key: Buffer.from('test-key'),
          value: Buffer.from(JSON.stringify({ message: 'hello' })),
          timestamp: '1234567890',
          headers: {
            'content-type': Buffer.from('application/json'),
            'user-id': Buffer.from('123'),
          },
        },
        highWatermark: '124',
        heartbeat: async () => {},
        pause: () => {},
      };

      const message = new Message(payload);

      expect(message.topic).toBe('test-topic');
      expect(message.partition).toBe(0);
      expect(message.offset).toBe('123');
      expect(message.key).toBe('test-key');
      expect(message.value).toEqual({ message: 'hello' });
      expect(message.timestamp).toBe('1234567890');
      expect(message.headers['content-type']).toBe('application/json');
      expect(message.headers['user-id']).toBe('123');
    });

    it('should handle null key', () => {
      const payload: EachMessagePayload = {
        topic: 'test-topic',
        partition: 0,
        message: {
          offset: '123',
          key: null,
          value: Buffer.from('simple string'),
          timestamp: '1234567890',
          headers: {},
        },
        highWatermark: '124',
        heartbeat: async () => {},
        pause: () => {},
      };

      const message = new Message(payload);

      expect(message.key).toBe(null);
      expect(message.value).toBe('simple string');
    });

    it('should handle null value', () => {
      const payload: EachMessagePayload = {
        topic: 'test-topic',
        partition: 0,
        message: {
          offset: '123',
          key: Buffer.from('key'),
          value: null,
          timestamp: '1234567890',
          headers: {},
        },
        highWatermark: '124',
        heartbeat: async () => {},
        pause: () => {},
      };

      const message = new Message(payload);

      expect(message.value).toBe(null);
    });

    it('should handle empty headers', () => {
      const payload: EachMessagePayload = {
        topic: 'test-topic',
        partition: 0,
        message: {
          offset: '123',
          key: null,
          value: Buffer.from('test'),
          timestamp: '1234567890',
          headers: {},
        },
        highWatermark: '124',
        heartbeat: async () => {},
        pause: () => {},
      };

      const message = new Message(payload);

      expect(message.headers).toEqual({});
    });

    it('should handle undefined headers', () => {
      const payload: EachMessagePayload = {
        topic: 'test-topic',
        partition: 0,
        message: {
          offset: '123',
          key: null,
          value: Buffer.from('test'),
          timestamp: '1234567890',
          headers: undefined,
        },
        highWatermark: '124',
        heartbeat: async () => {},
        pause: () => {},
      };

      const message = new Message(payload);

      expect(message.headers).toEqual({});
    });

    it('should deserialize JSON values correctly', () => {
      const complexObject = {
        id: 123,
        name: 'test',
        nested: { value: true },
        array: [1, 2, 3],
      };

      const payload: EachMessagePayload = {
        topic: 'test-topic',
        partition: 0,
        message: {
          offset: '123',
          key: Buffer.from(JSON.stringify({ userId: 456 })),
          value: Buffer.from(JSON.stringify(complexObject)),
          timestamp: '1234567890',
          headers: {},
        },
        highWatermark: '124',
        heartbeat: async () => {},
        pause: () => {},
      };

      const message = new Message(payload);

      expect(message.key).toEqual({ userId: 456 });
      expect(message.value).toEqual(complexObject);
    });

    it('should fallback to string for invalid JSON', () => {
      const payload: EachMessagePayload = {
        topic: 'test-topic',
        partition: 0,
        message: {
          offset: '123',
          key: Buffer.from('not-json'),
          value: Buffer.from('also-not-json'),
          timestamp: '1234567890',
          headers: {},
        },
        highWatermark: '124',
        heartbeat: async () => {},
        pause: () => {},
      };

      const message = new Message(payload);

      expect(message.key).toBe('not-json');
      expect(message.value).toBe('also-not-json');
    });

    it('should handle binary data', () => {
      const binaryData = Buffer.from([0xff, 0xfe, 0xfd, 0xfc]);

      const payload: EachMessagePayload = {
        topic: 'test-topic',
        partition: 0,
        message: {
          offset: '123',
          key: binaryData,
          value: binaryData,
          timestamp: '1234567890',
          headers: {},
        },
        highWatermark: '124',
        heartbeat: async () => {},
        pause: () => {},
      };

      const message = new Message(payload);

      // Binary data should be converted to string (as per deserialize function)
      expect(typeof message.key).toBe('string');
      expect(typeof message.value).toBe('string');
    });
  });

  describe('toString', () => {
    it('should return formatted string representation', () => {
      const payload: EachMessagePayload = {
        topic: 'test-topic',
        partition: 2,
        message: {
          offset: '456',
          key: null,
          value: Buffer.from('test'),
          timestamp: '1234567890',
          headers: {},
        },
        highWatermark: '457',
        heartbeat: async () => {},
        pause: () => {},
      };

      const message = new Message(payload);
      const result = message.toString();

      expect(result).toBe("Message(topic='test-topic', partition=2, offset=456)");
    });

    it('should handle different partition and offset values', () => {
      const payload: EachMessagePayload = {
        topic: 'my-topic',
        partition: 0,
        message: {
          offset: '0',
          key: null,
          value: Buffer.from('test'),
          timestamp: '1234567890',
          headers: {},
        },
        highWatermark: '1',
        heartbeat: async () => {},
        pause: () => {},
      };

      const message = new Message(payload);
      const result = message.toString();

      expect(result).toBe("Message(topic='my-topic', partition=0, offset=0)");
    });
  });

  describe('toJSON', () => {
    it('should return complete JSON representation', () => {
      const payload: EachMessagePayload = {
        topic: 'test-topic',
        partition: 1,
        message: {
          offset: '789',
          key: Buffer.from('my-key'),
          value: Buffer.from(JSON.stringify({ data: 'test' })),
          timestamp: '1234567890',
          headers: {
            'content-type': Buffer.from('application/json'),
          },
        },
        highWatermark: '790',
        heartbeat: async () => {},
        pause: () => {},
      };

      const message = new Message(payload);
      const result = message.toJSON();

      expect(result).toEqual({
        topic: 'test-topic',
        partition: 1,
        offset: '789',
        key: 'my-key',
        value: { data: 'test' },
        timestamp: '1234567890',
        headers: {
          'content-type': 'application/json',
        },
      });
    });

    it('should include null values in JSON', () => {
      const payload: EachMessagePayload = {
        topic: 'test-topic',
        partition: 0,
        message: {
          offset: '123',
          key: null,
          value: null,
          timestamp: '1234567890',
          headers: {},
        },
        highWatermark: '124',
        heartbeat: async () => {},
        pause: () => {},
      };

      const message = new Message(payload);
      const result = message.toJSON();

      expect(result.key).toBe(null);
      expect(result.value).toBe(null);
      expect(result.headers).toEqual({});
    });

    it('should be serializable with JSON.stringify', () => {
      const payload: EachMessagePayload = {
        topic: 'test-topic',
        partition: 0,
        message: {
          offset: '123',
          key: Buffer.from('key'),
          value: Buffer.from(JSON.stringify({ test: true })),
          timestamp: '1234567890',
          headers: {
            header1: Buffer.from('value1'),
          },
        },
        highWatermark: '124',
        heartbeat: async () => {},
        pause: () => {},
      };

      const message = new Message(payload);

      // Should not throw and should produce valid JSON
      const jsonString = JSON.stringify(message);
      const parsed = JSON.parse(jsonString);

      expect(parsed.topic).toBe('test-topic');
      expect(parsed.key).toBe('key');
      expect(parsed.value).toEqual({ test: true });
      expect(parsed.headers.header1).toBe('value1');
    });
  });

  describe('header conversion', () => {
    it('should convert various header types to strings', () => {
      const payload: EachMessagePayload = {
        topic: 'test-topic',
        partition: 0,
        message: {
          offset: '123',
          key: null,
          value: Buffer.from('test'),
          timestamp: '1234567890',
          headers: {
            stringHeader: 'already-string',
            bufferHeader: Buffer.from('buffer-value'),
            numberHeader: 123 as any, // Simulating different types that might come from KafkaJS
            booleanHeader: true as any,
            undefinedHeader: undefined,
          },
        },
        highWatermark: '124',
        heartbeat: async () => {},
        pause: () => {},
      };

      const message = new Message(payload);

      expect(message.headers.stringHeader).toBe('already-string');
      expect(message.headers.bufferHeader).toBe('buffer-value');
      expect(message.headers.numberHeader).toBe('123');
      expect(message.headers.booleanHeader).toBe('true');
      expect(message.headers.undefinedHeader).toBeUndefined();
    });

    it('should handle mixed header types', () => {
      const payload: EachMessagePayload = {
        topic: 'test-topic',
        partition: 0,
        message: {
          offset: '123',
          key: null,
          value: Buffer.from('test'),
          timestamp: '1234567890',
          headers: {
            string: 'text',
            buffer: Buffer.from('binary'),
            number: 42 as any,
            empty: '',
            whitespace: '   ',
          },
        },
        highWatermark: '124',
        heartbeat: async () => {},
        pause: () => {},
      };

      const message = new Message(payload);

      expect(message.headers).toEqual({
        string: 'text',
        buffer: 'binary',
        number: '42',
        empty: '',
        whitespace: '   ',
      });
    });
  });

  describe('message properties are readonly', () => {
    it('should have readonly properties', () => {
      const payload: EachMessagePayload = {
        topic: 'test-topic',
        partition: 0,
        message: {
          offset: '123',
          key: Buffer.from('key'),
          value: Buffer.from('value'),
          timestamp: '1234567890',
          headers: {},
        },
        highWatermark: '124',
        heartbeat: async () => {},
        pause: () => {},
      };

      const message = new Message(payload);

      // These should not be writable (TypeScript compilation will catch this)
      // but we can verify the properties exist and have the expected values
      expect(message.topic).toBe('test-topic');
      expect(message.partition).toBe(0);
      expect(message.offset).toBe('123');
      expect(message.key).toBe('key');
      expect(message.value).toBe('value');
      expect(message.timestamp).toBe('1234567890');
      expect(message.headers).toEqual({});
    });
  });
});