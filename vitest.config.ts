import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: ['tests/**/*.{test,spec}.{js,mjs,cjs,ts,mts,cts,jsx,tsx}'],
    exclude: ['node_modules', 'dist', 'build'],
    setupFiles: ['tests/setup.ts'],
    testTimeout: 60000, // Increased for KRaft mode stability
    pool: 'threads',
    coverage: {
      provider: 'v8',
      reporter: ['text', 'lcov', 'html'],
      exclude: [
        'node_modules',
        'dist',
        'tests',
        '**/*.d.ts',
        '**/*.config.{js,ts}',
      ],
      thresholds: {
        global: {
          branches: 90,
          functions: 90,
          lines: 90,
          statements: 90,
        },
      },
    },
  },
});

// Separate config for performance tests
export const performanceConfig = defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: [
      'tests/performance/**/*.{test,spec}.{js,mjs,cjs,ts,mts,cts,jsx,tsx}',
    ],
    exclude: ['node_modules', 'dist', 'build'],
    setupFiles: ['tests/setup.ts'],
    testTimeout: 180000, // 3 minutes for performance tests
    pool: 'threads',
    reporter: ['verbose'],
  },
});
