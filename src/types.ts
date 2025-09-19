export interface KafkaConfig {
  bootstrapServers?: string;
  acks?: 'all' | 0 | 1 | -1;
  retries?: number;
  compressionType?: 'gzip' | 'snappy' | 'lz4' | 'zstd' | null;
  batchSize?: number;
  lingerMs?: number;

  // Consumer-specific
  groupId?: string;
  autoOffsetReset?: 'latest' | 'earliest';
  enableAutoCommit?: boolean;

  // Additional KafkaJS options
  clientId?: string;
  connectionTimeout?: number;
  authenticationTimeout?: number;
  requestTimeout?: number;

  // SASL configuration
  sasl?: {
    mechanism: 'plain' | 'scram-sha-256' | 'scram-sha-512';
    username: string;
    password: string;
  };

  // SSL configuration
  ssl?:
    | boolean
    | {
        rejectUnauthorized?: boolean;
        ca?: string[];
        key?: string;
        cert?: string;
      };
}

export interface MessageHeaders {
  [key: string]: Buffer | string | undefined;
}

export interface MessageMetadata {
  topic: string;
  partition: number;
  offset: string;
  key?: Buffer | string | null;
  timestamp: string;
  headers: MessageHeaders;
}

export interface SendResult {
  success: boolean;
  topic?: string;
  partition?: number;
  offset?: string;
  error?: string;
}

export interface BatchMessage {
  topic: string;
  value: unknown;
  key?: unknown;
  partition?: number;
  headers?: MessageHeaders;
}

export type MessageInput =
  | [string, unknown] // [topic, value]
  | [string, unknown, unknown] // [topic, value, key]
  | BatchMessage; // Object format

// Environment variables
export interface EnvironmentConfig {
  KAFKA_BOOTSTRAP_SERVERS?: string;
  KAFKA_SECURITY_PROTOCOL?: string;
  KAFKA_SASL_USERNAME?: string;
  KAFKA_SASL_PASSWORD?: string;
  KAFKA_CLIENT_ID?: string;
}
