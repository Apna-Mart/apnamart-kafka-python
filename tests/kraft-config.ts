// KRaft-specific test configuration for single-node Docker setup
import { Config } from '../src/index.ts';

export function createKRaftTestConfig(overrides: any = {}): Config {
  return new Config({
    bootstrapServers: 'localhost:9092',
    // Single-node KRaft optimizations
    acks: 'all',
    retries: 5,
    requestTimeout: 30000,
    connectionTimeout: 10000,
    authenticationTimeout: 10000,
    // Extended timeouts for KRaft metadata sync
    ...overrides,
  });
}

export function createKRaftConsumerConfig(
  groupId: string,
  overrides: any = {},
): Config {
  return new Config({
    bootstrapServers: 'localhost:9092',
    groupId,
    autoOffsetReset: 'earliest',
    // KRaft consumer optimizations
    acks: 'all',
    retries: 5,
    requestTimeout: 30000,
    connectionTimeout: 10000,
    authenticationTimeout: 10000,
    ...overrides,
  });
}

// Extended wait times for your single-node KRaft setup
export const KRAFT_WAIT_TIMES = {
  topicCreation: 5000, // Time to wait after topic creation
  messageProduction: 2000, // Time to wait after producing
  consumerStart: 3000, // Time for consumer to start properly
  metadataSync: 4000, // Time for metadata synchronization
};

// KRaft-specific test helpers
export async function waitForKRaftSync(
  ms: number = KRAFT_WAIT_TIMES.metadataSync,
): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

export async function retryForKRaft<T>(
  operation: () => Promise<T>,
  maxRetries: number = 5,
  delay: number = 2000,
): Promise<T> {
  for (let i = 0; i < maxRetries; i++) {
    try {
      return await operation();
    } catch (error) {
      if (i === maxRetries - 1) throw error;
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }
  throw new Error('All retries exhausted');
}
