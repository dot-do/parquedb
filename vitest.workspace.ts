/**
 * Vitest Workspace Configuration for ParqueDB
 *
 * This workspace defines multiple test projects:
 * - node: Standard Node.js tests (unit, integration, e2e)
 * - workers: Cloudflare Workers tests with vitest-pool-workers
 *
 * Worker tests use real Cloudflare bindings:
 * - Real R2 bucket (BUCKET)
 * - Real Durable Objects (PARQUEDB) with SQLite storage
 *
 * Configuration is loaded from wrangler.jsonc with environment: 'test'
 *
 * Performance optimization notes:
 * - Tests are parallelized at the file level (fileParallelism: true)
 * - Each fork process has isolated module state (isolate: true)
 * - Tests within a file run sequentially (maxConcurrency: 1) to prevent intra-file race conditions
 * - Most tests use fresh MemoryBackend instances or call clearGlobalStorage() in beforeEach
 * - The forks pool provides process isolation, resetting module-level globals per fork
 */

import { defineWorkspace } from 'vitest/config'
import { defineWorkersConfig } from '@cloudflare/vitest-pool-workers/config'
import { resolve } from 'node:path'
import os from 'node:os'

// Calculate optimal fork count based on available CPUs
// Use 50% of cores to balance parallelism vs memory pressure
const cpuCount = os.cpus().length
const optimalForks = Math.max(4, Math.min(cpuCount / 2, 12))

// Shared resolve configuration for path aliases
const sharedResolve = {
  alias: {
    '@': resolve(__dirname, './src'),
    '@tests': resolve(__dirname, './tests'),
    // Mock cloudflare:workers for Node.js environment (workers projects use the real module)
    'cloudflare:workers': resolve(__dirname, './tests/__mocks__/cloudflare-workers.ts'),
  },
}

export default defineWorkspace([
  // ===========================================================================
  // Node.js Projects
  // ===========================================================================

  // Unit tests - fast, isolated tests for pure functions
  {
    resolve: sharedResolve,
    test: {
      name: 'node:unit',
      root: '.',
      include: ['tests/unit/**/*.test.ts'],
      exclude: ['**/*.workers.test.ts', '**/*.browser.test.ts'],
      globals: true,
      pool: 'forks',
      poolOptions: {
        forks: {
          maxForks: optimalForks,
          minForks: 2,
          isolate: true, // Each fork has fresh module state
        },
      },
      fileParallelism: true, // Run test files in parallel across forks
      maxConcurrency: 1, // Run tests within a file sequentially to avoid temp dir races
      sequence: {
        shuffle: false, // Keep deterministic order for debugging
      },
      setupFiles: ['tests/setup.ts'],
      testTimeout: 30000,
    },
  },

  // Integration tests - tests involving multiple components
  {
    resolve: sharedResolve,
    test: {
      name: 'node:integration',
      root: '.',
      include: ['tests/integration/**/*.test.ts'],
      exclude: ['**/*.workers.test.ts', '**/*.browser.test.ts'],
      globals: true,
      pool: 'forks',
      poolOptions: {
        forks: {
          maxForks: Math.max(2, optimalForks / 2), // Fewer forks for heavier tests
          minForks: 2,
          isolate: true,
        },
      },
      fileParallelism: true,
      maxConcurrency: 1, // Run tests sequentially within files
      sequence: {
        shuffle: false,
      },
      setupFiles: ['tests/setup.ts'],
      testTimeout: 30000,
    },
  },

  // E2E tests (Node.js mock environment)
  {
    resolve: sharedResolve,
    test: {
      name: 'node:e2e',
      root: '.',
      include: ['tests/e2e/**/*.test.ts'],
      exclude: ['**/*.workers.test.ts', '**/*.browser.test.ts'],
      globals: true,
      pool: 'forks',
      poolOptions: {
        forks: {
          maxForks: Math.max(2, optimalForks / 2),
          minForks: 2,
          isolate: true,
        },
      },
      fileParallelism: true,
      maxConcurrency: 1, // Run tests sequentially within files
      sequence: {
        shuffle: false,
      },
      setupFiles: ['tests/setup.ts'],
      testTimeout: 60000,
    },
  },

  // ===========================================================================
  // Cloudflare Workers Projects (using vitest-pool-workers)
  // ===========================================================================

  // Worker E2E tests with real Cloudflare bindings
  defineWorkersConfig({
    test: {
      name: 'e2e',
      root: '.',
      include: ['tests/e2e/**/*.workers.test.ts'],
      globals: true,
      testTimeout: 60000,
      hookTimeout: 30000,
      // Pool options for vitest-pool-workers
      poolOptions: {
        workers: {
          // Wrangler configuration for bindings
          wrangler: {
            configPath: './wrangler.jsonc',
            // Use test environment for isolation from production
            environment: 'test',
            experimentalJsonConfig: true,
          },
          // Isolate each test file for clean state
          isolatedStorage: true,
          // Main entry point for the worker
          main: './src/worker/index.ts',
          // Miniflare options for local testing
          miniflare: {
            // Enable compatibility flags for workers
            compatibilityDate: '2026-01-28',
            compatibilityFlags: ['nodejs_compat'],
            // Configure bindings directly for vitest-pool-workers
            r2Buckets: {
              BUCKET: 'parquedb-test',
              CDN_BUCKET: 'parquedb-test',
            },
            // Configure Durable Objects with SQLite enabled
            durableObjects: {
              PARQUEDB: {
                className: 'ParqueDBDO',
                // Enable SQLite for the DO
                useSQLite: true,
              },
            },
          },
        },
      },
    },
  }),

  // Worker integration tests
  defineWorkersConfig({
    test: {
      name: 'e2e:integration',
      root: '.',
      include: ['tests/integration/**/*.workers.test.ts'],
      globals: true,
      testTimeout: 60000,
      hookTimeout: 30000,
      poolOptions: {
        workers: {
          wrangler: {
            configPath: './wrangler.jsonc',
            environment: 'test',
            experimentalJsonConfig: true,
          },
          isolatedStorage: true,
          main: './src/worker/index.ts',
          miniflare: {
            compatibilityDate: '2026-01-28',
            compatibilityFlags: ['nodejs_compat'],
            r2Buckets: {
              BUCKET: 'parquedb-test',
              CDN_BUCKET: 'parquedb-test',
            },
            durableObjects: {
              PARQUEDB: {
                className: 'ParqueDBDO',
                useSQLite: true,
              },
            },
          },
        },
      },
    },
  }),

  // Worker benchmark tests
  defineWorkersConfig({
    test: {
      name: 'e2e:bench',
      root: '.',
      include: ['tests/benchmarks/**/*.workers.bench.ts'],
      globals: true,
      testTimeout: 120000,
      hookTimeout: 30000,
      benchmark: {
        include: ['tests/benchmarks/**/*.workers.bench.ts'],
        reporters: ['default'],
        outputJson: 'benchmark-workers-results.json',
      },
      poolOptions: {
        workers: {
          wrangler: {
            configPath: './wrangler.jsonc',
            environment: 'test',
            experimentalJsonConfig: true,
          },
          isolatedStorage: true,
          main: './src/worker/index.ts',
          miniflare: {
            compatibilityDate: '2026-01-28',
            compatibilityFlags: ['nodejs_compat'],
            r2Buckets: {
              BUCKET: 'parquedb-test',
              CDN_BUCKET: 'parquedb-test',
            },
            durableObjects: {
              PARQUEDB: {
                className: 'ParqueDBDO',
                useSQLite: true,
              },
            },
          },
        },
      },
    },
  }),
])
