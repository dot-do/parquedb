/**
 * Vitest Default Configuration for ParqueDB
 *
 * This configuration is used when running `npm test` without specifying a workspace project.
 * It runs all Node.js tests (unit, integration, e2e) excluding browser and workers tests.
 *
 * For Cloudflare Workers tests with real bindings, use:
 *   npm run test:e2e:workers
 *
 * For specific projects:
 *   npm run test:unit         # Node.js unit tests only
 *   npm run test:integration  # Node.js integration tests only
 *   npm run test:e2e          # Node.js e2e tests only
 *
 * See vitest.workspace.ts for full workspace configuration including:
 *   - Worker tests with vitest-pool-workers
 *   - Real Cloudflare bindings (R2, Durable Objects, Service Bindings)
 */

import { defineConfig } from 'vitest/config'
import path from 'path'

export default defineConfig({
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
      '@tests': path.resolve(__dirname, './tests'),
    },
  },
  test: {
    // Global test configuration
    globals: true,

    // Memory management - prevent OOM by limiting parallelism
    pool: 'forks',
    poolOptions: {
      forks: {
        maxForks: 2,
        minForks: 1,
        isolate: true,
      },
    },
    fileParallelism: false, // Run test files sequentially
    maxConcurrency: 1, // Run tests within a file sequentially to prevent shared state issues
    sequence: {
      shuffle: false,
      concurrent: false, // Ensure tests run sequentially within files
    },

    // Test file patterns - exclude browser and workers tests
    // These require special environments (vitest-pool-workers, @vitest/browser)
    include: ['tests/**/*.test.ts'],
    exclude: ['**/*.browser.test.ts', '**/*.workers.test.ts'],

    // Setup
    setupFiles: ['tests/setup.ts'],

    // Default timeout
    testTimeout: 30000,

    // Coverage configuration
    coverage: {
      provider: 'v8',
      reporter: ['text', 'json', 'html', 'lcov'],
      reportsDirectory: './coverage',
      include: ['src/**/*.ts'],
      exclude: [
        'src/**/*.d.ts',
        'src/**/*.test.ts',
        'src/**/index.ts', // Re-export files
      ],
      thresholds: {
        lines: 80,
        branches: 70,
        functions: 80,
        statements: 80,
      },
    },

    // Benchmark configuration (only used with `vitest bench`)
    benchmark: {
      include: ['tests/benchmarks/**/*.bench.ts'],
      exclude: ['**/*.browser.bench.ts', '**/*.workers.bench.ts'],
      reporters: ['default'],
      outputJson: 'benchmark-results.json',
    },
  },
})
