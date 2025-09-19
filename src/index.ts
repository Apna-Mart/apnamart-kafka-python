// Core classes
export {
  Config,
  Consumer,
  consume,
  deserialize,
  Message,
  Producer,
  send,
  serialize,
  TransactionalProducer,
} from './client.ts';

// Error classes
export {
  ConfigurationError,
  ConsumerError,
  KafkaError,
  ProducerError,
  SerializationError,
  TransactionError,
} from './errors.ts';

// Type definitions
export type {
  BatchMessage,
  EnvironmentConfig,
  KafkaConfig,
  MessageHeaders,
  MessageInput,
  MessageMetadata,
  SendResult,
} from './types.ts';

// Import for default export
import {
  Config,
  Consumer,
  consume,
  deserialize,
  Message,
  Producer,
  send,
  serialize,
  TransactionalProducer,
} from './client.ts';

// Default export for convenience
export default {
  Config,
  Producer,
  Consumer,
  TransactionalProducer,
  Message,
  send,
  consume,
  serialize,
  deserialize,
};
