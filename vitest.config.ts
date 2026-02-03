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
 *
 * Performance optimization notes:
 * - Tests are parallelized at the file level (fileParallelism: true)
 * - Each fork process has isolated module state (isolate: true)
 * - Tests within a file run with limited concurrency to balance speed vs isolation
 * - Most tests use fresh MemoryBackend instances or call clearGlobalStorage() in beforeEach
 */

import { defineConfig } from 'vitest/config'
import path from 'path'
import os from 'os'

// Calculate optimal fork count based on available CPUs
// Use 50% of cores to balance parallelism vs memory pressure
const cpuCount = os.cpus().length
const optimalForks = Math.max(4, Math.min(cpuCount / 2, 12))

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

    // Parallelization settings for performance
    pool: 'forks',
    poolOptions: {
      forks: {
        maxForks: optimalForks,
        minForks: 2,
        isolate: true, // Each fork has fresh module state
      },
    },
    fileParallelism: true, // Run test files in parallel across forks
    maxConcurrency: 5, // Allow some parallelism within files for independent tests
    sequence: {
      shuffle: false, // Keep deterministic order for debugging
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
