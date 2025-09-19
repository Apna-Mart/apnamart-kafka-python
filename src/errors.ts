export class KafkaError extends Error {
  constructor(
    message: string,
    public readonly cause?: Error,
  ) {
    super(message);
    this.name = 'KafkaError';
    if (cause) {
      this.stack = `${this.stack}\nCaused by: ${cause.stack}`;
    }
  }
}

export class ProducerError extends KafkaError {
  constructor(message: string, cause?: Error) {
    super(message, cause);
    this.name = 'ProducerError';
  }
}

export class ConsumerError extends KafkaError {
  constructor(message: string, cause?: Error) {
    super(message, cause);
    this.name = 'ConsumerError';
  }
}

export class TransactionError extends KafkaError {
  constructor(message: string, cause?: Error) {
    super(message, cause);
    this.name = 'TransactionError';
  }
}

export class SerializationError extends KafkaError {
  constructor(message: string, cause?: Error) {
    super(message, cause);
    this.name = 'SerializationError';
  }
}

export class ConfigurationError extends KafkaError {
  constructor(message: string, cause?: Error) {
    super(message, cause);
    this.name = 'ConfigurationError';
  }
}
