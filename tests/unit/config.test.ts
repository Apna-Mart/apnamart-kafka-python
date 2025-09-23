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
      expect(config.batchSize).toBe(32768); // Updated for performance
      expect(config.lingerMs).toBe(5); // Updated for performance
      expect(config.autoOffsetReset).toBe('latest');
      expect(config.enableAutoCommit).toBe(true);
      expect(config.connectionTimeout).toBe(10000); // Updated for KRaft stability
      expect(config.authenticationTimeout).toBe(10000); // Updated for KRaft stability
      expect(config.requestTimeout).toBe(30000); // Updated for single-node KRaft
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
        transactionTimeout: 60000, // Updated for single-node KRaft
        maxInFlightRequests: 100, // Updated for single-node stability
        idempotent: true,
        compression: 'gzip',
        batch: {
          size: 32768,
          lingerMs: 10, // Updated for KRaft batching (5 + 5)
        },
        retry: {
          initialRetryTime: 1000, // Updated for KRaft metadata sync
          retries: 8, // Updated for single-node KRaft (5 + 3)
          maxRetryTime: 30000, // Updated for stability
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
        sessionTimeout: 45000, // Updated for single-node KRaft stability
        rebalanceTimeout: 90000, // Updated for KRaft coordination delay
        heartbeatInterval: 10000, // Updated for single-node setup
        metadataMaxAge: 180000, // Updated for faster refresh
        allowAutoTopicCreation: true,
        fetchMinBytes: 1,
        fetchMaxWait: 500, // Updated for single-node KRaft
        fetchMaxBytes: 1048576,
        maxPartitionFetchBytes: 1048576,
        retry: {
          initialRetryTime: 1000, // Updated for KRaft
          retries: 8, // Updated for single-node KRaft (3 + 5)
          maxRetryTime: 60000, // Updated for stability
        },
      });
    });
  });
});
