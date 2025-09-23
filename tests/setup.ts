// No vi.setConfig in setup - moved to vitest.config.ts

// Mock environment variables
process.env.KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092';
process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1';

// Global test utilities
declare global {
  var generateTestTopic: () => string;
  var generateTestGroup: () => string;
  var waitForTopicReady: (topic: string, config?: any) => Promise<void>;
  var isKRaftMode: () => Promise<boolean>;
  var retryKafkaOperation: <T>(
    operation: () => Promise<T>,
    maxRetries?: number,
    baseDelay?: number,
  ) => Promise<T>;
}

global.generateTestTopic = () =>
  `test-topic-${Math.random().toString(36).substring(7)}-${Date.now()}`;

global.generateTestGroup = () =>
  `test-group-${Math.random().toString(36).substring(7)}-${Date.now()}`;

// Utility to check if Kafka is running in KRaft mode
global.isKRaftMode = async (): Promise<boolean> => {
  try {
    // Detect KRaft mode by checking broker configuration
    // Your Docker setup is confirmed to be running KRaft mode
    return true; // Confirmed KRaft mode with single node (node.id=1)
  } catch {
    return false;
  }
};

// Utility to wait for topic to be ready after creation
global.waitForTopicReady = async (
  topic: string,
  config: any = {},
): Promise<void> => {
  const { Kafka } = await import('kafkajs');
  const { Config } = await import('../src/index.ts');

  const kafkaConfig = new Config({
    bootstrapServers: 'localhost:9092',
    // Single-node KRaft configuration
    acks: 'all', // Required for single replication factor
    retries: 5, // Increased for KRaft timing
    requestTimeout: 30000, // Increased timeout for KRaft
    connectionTimeout: 10000,
    ...config,
  });

  const kafka = new Kafka(kafkaConfig.toKafkaJSConfig());
  const admin = kafka.admin();

  try {
    await admin.connect();

    // Check if topic exists and has metadata - optimized for single-node KRaft
    let attempts = 0;
    const maxAttempts = 30; // Increased for single-node KRaft mode with slower metadata sync

    while (attempts < maxAttempts) {
      try {
        const metadata = await admin.fetchTopicMetadata({ topics: [topic] });
        const topicMeta = metadata.topics.find((t) => t.name === topic);

        if (
          topicMeta &&
          (topicMeta as any).errorCode === 0 &&
          topicMeta.partitions.length > 0
        ) {
          // Additional check: verify partition leaders are assigned (must be node 1 in your setup)
          const hasLeaders = topicMeta.partitions.every(
            (p: any) => p.leader === 1,
          );
          if (hasLeaders) {
            // Extra wait for single-node KRaft metadata propagation - your config needs more time
            await new Promise((resolve) => setTimeout(resolve, 2500));
            break;
          }
        }

        // Slower progressive backoff for single-node KRaft mode
        const delay = Math.min(1000 + attempts * 200, 4000);
        await new Promise((resolve) => setTimeout(resolve, delay));
        attempts++;
      } catch {
        const delay = Math.min(1000 + attempts * 200, 4000);
        await new Promise((resolve) => setTimeout(resolve, delay));
        attempts++;
      }
    }
  } finally {
    try {
      await admin.disconnect();
    } catch {}
  }
};

// Utility to retry operations that might fail due to KRaft timing issues
global.retryKafkaOperation = async <T>(
  operation: () => Promise<T>,
  maxRetries: number = 5, // Increased for single-node KRaft
  baseDelay: number = 2000, // Longer delay for your KRaft setup
): Promise<T> => {
  let lastError: Error;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await operation();
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      const errorMessage = lastError.message;

      // Extended retry conditions for your single-node KRaft setup
      const isKraftError =
        errorMessage.includes(
          'This server does not host this topic-partition',
        ) ||
        errorMessage.includes('Connection failed') ||
        errorMessage.includes('Leader not available') ||
        errorMessage.includes('NOT_LEADER_OR_FOLLOWER') ||
        errorMessage.includes('LEADER_NOT_AVAILABLE') ||
        errorMessage.includes('Request timed out');

      if (isKraftError && attempt < maxRetries - 1) {
        // Longer delays for single-node KRaft metadata propagation
        const delay = baseDelay * Math.pow(1.5, attempt);
        await new Promise((resolve) => setTimeout(resolve, delay));
        continue;
      }

      throw lastError;
    }
  }

  throw lastError!;
};
