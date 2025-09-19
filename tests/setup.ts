import { vi } from 'vitest';

// Global test timeout
vi.setConfig({ testTimeout: 30000 });

// Mock environment variables
process.env.KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092';

// Global test utilities
declare global {
  var generateTestTopic: () => string;
  var generateTestGroup: () => string;
}

global.generateTestTopic = () =>
  `test-topic-${Math.random().toString(36).substring(7)}-${Date.now()}`;

global.generateTestGroup = () =>
  `test-group-${Math.random().toString(36).substring(7)}-${Date.now()}`;
