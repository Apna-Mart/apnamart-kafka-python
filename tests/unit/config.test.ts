import { beforeEach, describe, expect, it } from 'vitest';
import { Config, ConfigurationError } from '../../src/index.ts';

describe('Config', () => {
  beforeEach(() => {
    // Clear environment variables
    delete process.env.KAFKA_BOOTSTRAP_SERVERS;
    delete process.env.KAFKA_CLIENT_ID;
  });

  describe('constructor', () => {
    it('should use default values', () => {
      const config = new Config();

      expect(config.bootstrapServers).toBe('localhost:9092');
      expect(config.acks).toBe('all');
      expect(config.retries).toBe(3);
      expect(config.groupId).toBe('default-group');
      expect(config.clientId).toBe('apnamart-kafka-client');
      expect(config.batchSize).toBe(16384);
      expect(config.lingerMs).toBe(0);
      expect(config.autoOffsetReset).toBe('latest');
      expect(config.enableAutoCommit).toBe(true);
      expect(config.connectionTimeout).toBe(1000);
      expect(config.authenticationTimeout).toBe(1000);
      expect(config.requestTimeout).toBe(30000);
    });

    it('should use environment variables', () => {
      process.env.KAFKA_BOOTSTRAP_SERVERS = 'kafka1:9092,kafka2:9092';
      process.env.KAFKA_CLIENT_ID = 'env-client';

      const config = new Config();
      expect(config.bootstrapServers).toBe('kafka1:9092,kafka2:9092');
      expect(config.clientId).toBe('env-client');
    });

    it('should override environment variables with config', () => {
      process.env.KAFKA_BOOTSTRAP_SERVERS = 'kafka1:9092,kafka2:9092';

      const config = new Config({
        bootstrapServers: 'override:9092',
        clientId: 'override-client',
      });

      expect(config.bootstrapServers).toBe('override:9092');
      expect(config.clientId).toBe('override-client');
    });

    it('should use custom configuration values', () => {
      const config = new Config({
        bootstrapServers: 'custom:9092',
        acks: 1,
        retries: 5,
        compressionType: 'gzip',
        batchSize: 32768,
        lingerMs: 100,
        groupId: 'custom-group',
        autoOffsetReset: 'earliest',
        enableAutoCommit: false,
        clientId: 'custom-client',
        connectionTimeout: 5000,
        authenticationTimeout: 2000,
        requestTimeout: 60000,
      });

      expect(config.bootstrapServers).toBe('custom:9092');
      expect(config.acks).toBe(1);
      expect(config.retries).toBe(5);
      expect(config.compressionType).toBe('gzip');
      expect(config.batchSize).toBe(32768);
      expect(config.lingerMs).toBe(100);
      expect(config.groupId).toBe('custom-group');
      expect(config.autoOffsetReset).toBe('earliest');
      expect(config.enableAutoCommit).toBe(false);
      expect(config.clientId).toBe('custom-client');
      expect(config.connectionTimeout).toBe(5000);
      expect(config.authenticationTimeout).toBe(2000);
      expect(config.requestTimeout).toBe(60000);
    });

    it('should validate configuration', () => {
      expect(() => new Config({ retries: -1 })).toThrow(ConfigurationError);

      expect(() => new Config({ batchSize: 0 })).toThrow(ConfigurationError);

      expect(() => new Config({ batchSize: -100 })).toThrow(ConfigurationError);
    });
  });

  describe('toKafkaJSConfig', () => {
    it('should convert to KafkaJS format', () => {
      const config = new Config({
        bootstrapServers: 'broker1:9092,broker2:9092',
        clientId: 'test-client',
        connectionTimeout: 2000,
        authenticationTimeout: 1500,
        requestTimeout: 45000,
      });

      const kafkaConfig = config.toKafkaJSConfig();

      expect(kafkaConfig.brokers).toEqual(['broker1:9092', 'broker2:9092']);
      expect(kafkaConfig.clientId).toBe('test-client');
      expect(kafkaConfig.connectionTimeout).toBe(2000);
      expect(kafkaConfig.authenticationTimeout).toBe(1500);
      expect(kafkaConfig.requestTimeout).toBe(45000);
    });

    it('should handle SASL configuration', () => {
      const config = new Config({
        sasl: {
          mechanism: 'plain',
          username: 'test-user',
          password: 'test-pass',
        },
      });

      const kafkaConfig = config.toKafkaJSConfig();

      expect(kafkaConfig.sasl).toEqual({
        mechanism: 'plain',
        username: 'test-user',
        password: 'test-pass',
      });
    });

    it('should handle SSL configuration', () => {
      const config = new Config({
        ssl: {
          rejectUnauthorized: false,
          ca: ['cert1', 'cert2'],
          key: 'private-key',
          cert: 'certificate',
        },
      });

      const kafkaConfig = config.toKafkaJSConfig();

      expect(kafkaConfig.ssl).toEqual({
        rejectUnauthorized: false,
        ca: ['cert1', 'cert2'],
        key: 'private-key',
        cert: 'certificate',
      });
    });

    it('should handle boolean SSL configuration', () => {
      const config = new Config({
        ssl: true,
      });

      const kafkaConfig = config.toKafkaJSConfig();

      expect(kafkaConfig.ssl).toBe(true);
    });
  });

  describe('toProducerConfig', () => {
    it('should return producer configuration', () => {
      const config = new Config({ retries: 5 });
      const producerConfig = config.toProducerConfig();

      expect(producerConfig).toEqual({
        allowAutoTopicCreation: true,
        transactionTimeout: 30000,
        maxInFlightRequests: 5,
        idempotent: true,
        retry: {
          initialRetryTime: 300,
          retries: 5,
        },
      });
    });
  });

  describe('toConsumerConfig', () => {
    it('should return consumer configuration', () => {
      const config = new Config({
        groupId: 'test-group',
        retries: 3,
      });
      const consumerConfig = config.toConsumerConfig();

      expect(consumerConfig).toEqual({
        groupId: 'test-group',
        sessionTimeout: 30000,
        rebalanceTimeout: 60000,
        heartbeatInterval: 3000,
        metadataMaxAge: 300000,
        allowAutoTopicCreation: true,
        retry: {
          initialRetryTime: 300,
          retries: 3,
        },
      });
    });
  });
});
