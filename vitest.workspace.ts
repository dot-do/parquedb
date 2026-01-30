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
 */

import { defineWorkspace } from 'vitest/config'
import { defineWorkersConfig } from '@cloudflare/vitest-pool-workers/config'

export default defineWorkspace([
  // ===========================================================================
  // Node.js Projects
  // ===========================================================================

  // Unit tests - fast, isolated tests for pure functions
  {
    test: {
      name: 'node:unit',
      root: '.',
      include: ['tests/unit/**/*.test.ts'],
      exclude: ['**/*.workers.test.ts', '**/*.browser.test.ts'],
      globals: true,
      pool: 'forks',
      poolOptions: {
        forks: {
          maxForks: 2,
          minForks: 1,
          isolate: true,
        },
      },
      fileParallelism: false,
      maxConcurrency: 5,
      setupFiles: ['tests/setup.ts'],
      testTimeout: 30000,
    },
  },

  // Integration tests - tests involving multiple components
  {
    test: {
      name: 'node:integration',
      root: '.',
      include: ['tests/integration/**/*.test.ts'],
      exclude: ['**/*.workers.test.ts', '**/*.browser.test.ts'],
      globals: true,
      pool: 'forks',
      poolOptions: {
        forks: {
          maxForks: 2,
          minForks: 1,
          isolate: true,
        },
      },
      fileParallelism: false,
      maxConcurrency: 5,
      setupFiles: ['tests/setup.ts'],
      testTimeout: 30000,
    },
  },

  // E2E tests (Node.js mock environment)
  {
    test: {
      name: 'node:e2e',
      root: '.',
      include: ['tests/e2e/**/*.test.ts'],
      exclude: ['**/*.workers.test.ts', '**/*.browser.test.ts'],
      globals: true,
      pool: 'forks',
      poolOptions: {
        forks: {
          maxForks: 2,
          minForks: 1,
          isolate: true,
        },
      },
      fileParallelism: false,
      maxConcurrency: 5,
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
