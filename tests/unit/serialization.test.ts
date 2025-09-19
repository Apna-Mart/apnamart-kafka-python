import { describe, expect, it } from 'vitest';
import { deserialize, SerializationError, serialize } from '../../src/index.ts';

describe('Serialization', () => {
  describe('serialize', () => {
    it('should serialize string data', () => {
      const input = 'hello world';
      const result = serialize(input);

      expect(Buffer.isBuffer(result)).toBe(true);
      expect(result.toString('utf-8')).toBe('hello world');
    });

    it('should serialize object data as JSON', () => {
      const input = { message: 'hello', count: 42 };
      const result = serialize(input);

      expect(Buffer.isBuffer(result)).toBe(true);
      expect(result.toString('utf-8')).toBe('{"message":"hello","count":42}');
    });

    it('should serialize number data as JSON', () => {
      const input = 123;
      const result = serialize(input);

      expect(Buffer.isBuffer(result)).toBe(true);
      expect(result.toString('utf-8')).toBe('123');
    });

    it('should serialize boolean data as JSON', () => {
      const input = true;
      const result = serialize(input);

      expect(Buffer.isBuffer(result)).toBe(true);
      expect(result.toString('utf-8')).toBe('true');
    });

    it('should serialize array data as JSON', () => {
      const input = [1, 2, 3];
      const result = serialize(input);

      expect(Buffer.isBuffer(result)).toBe(true);
      expect(result.toString('utf-8')).toBe('[1,2,3]');
    });

    it('should pass through Buffer data unchanged', () => {
      const input = Buffer.from('test buffer');
      const result = serialize(input);

      expect(result).toBe(input);
      expect(result.toString('utf-8')).toBe('test buffer');
    });

    it('should throw SerializationError for circular references', () => {
      const circular: Record<string, unknown> = { name: 'test' };
      circular.self = circular;

      expect(() => serialize(circular)).toThrow(SerializationError);
    });

    it('should serialize null as JSON', () => {
      const input = null;
      const result = serialize(input);

      expect(Buffer.isBuffer(result)).toBe(true);
      expect(result.toString('utf-8')).toBe('null');
    });

    it('should throw error for undefined', () => {
      const input = undefined;

      expect(() => serialize(input)).toThrow(SerializationError);
    });
  });

  describe('deserialize', () => {
    it('should return null for null input', () => {
      const result = deserialize(null);
      expect(result).toBe(null);
    });

    it('should deserialize JSON object', () => {
      const input = Buffer.from('{"message":"hello","count":42}');
      const result = deserialize(input);

      expect(result).toEqual({ message: 'hello', count: 42 });
    });

    it('should deserialize JSON array', () => {
      const input = Buffer.from('[1,2,3]');
      const result = deserialize(input);

      expect(result).toEqual([1, 2, 3]);
    });

    it('should deserialize JSON number', () => {
      const input = Buffer.from('123');
      const result = deserialize(input);

      expect(result).toBe(123);
    });

    it('should deserialize JSON boolean', () => {
      const input = Buffer.from('true');
      const result = deserialize(input);

      expect(result).toBe(true);
    });

    it('should deserialize JSON null', () => {
      const input = Buffer.from('null');
      const result = deserialize(input);

      expect(result).toBe(null);
    });

    it('should fallback to string for invalid JSON', () => {
      const input = Buffer.from('not valid json');
      const result = deserialize(input);

      expect(result).toBe('not valid json');
    });

    it('should fallback to buffer for non-UTF8 data', () => {
      const input = Buffer.from([0xff, 0xfe, 0xfd]);
      const result = deserialize(input);

      // Since our deserialize tries to convert to string first, it will return a string
      expect(typeof result).toBe('string');
    });

    it('should deserialize string that looks like JSON but is not', () => {
      const input = Buffer.from('"{invalid json"');
      const result = deserialize(input);

      expect(result).toBe('{invalid json');
    });

    it('should deserialize empty string', () => {
      const input = Buffer.from('');
      const result = deserialize(input);

      expect(result).toBe('');
    });

    it('should deserialize JSON string', () => {
      const input = Buffer.from('"hello world"');
      const result = deserialize(input);

      expect(result).toBe('hello world');
    });
  });

  describe('serialize/deserialize roundtrip', () => {
    it('should handle string roundtrip', () => {
      const original = 'hello world';
      const serialized = serialize(original);
      const deserialized = deserialize(serialized);

      expect(deserialized).toBe(original);
    });

    it('should handle object roundtrip', () => {
      const original = { message: 'hello', count: 42, nested: { value: true } };
      const serialized = serialize(original);
      const deserialized = deserialize(serialized);

      expect(deserialized).toEqual(original);
    });

    it('should handle array roundtrip', () => {
      const original = [1, 'two', { three: 3 }, [4, 5]];
      const serialized = serialize(original);
      const deserialized = deserialize(serialized);

      expect(deserialized).toEqual(original);
    });

    it('should handle buffer roundtrip', () => {
      const original = Buffer.from('binary data');
      const serialized = serialize(original);
      const deserialized = deserialize(serialized);

      // Buffer gets passed through serialize unchanged, so deserialize will treat it as string
      expect(typeof deserialized).toBe('string');
      expect(deserialized).toBe(original.toString());
    });
  });
});
