/**
 * Vitest Configuration for Benchmarks
 *
 * A simple Node.js-only config for running benchmarks without workspace complexity.
 *
 * Run with: npx vitest bench --config vitest.bench.config.ts
 */

import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    // No workspace - just run in Node.js
    globals: true,

    // Use threads pool for better performance
    pool: 'threads',
    poolOptions: {
      threads: {
        maxThreads: 4,
        minThreads: 1,
      },
    },

    // Benchmark configuration
    benchmark: {
      include: ['tests/benchmarks/**/*.bench.ts'],
      exclude: ['**/*.browser.bench.ts', '**/*.workers.bench.ts'],
      reporters: ['default'],
      outputJson: 'benchmark-results.json',
    },

    // Setup files
    setupFiles: [],

    // Longer timeout for benchmarks
    testTimeout: 120000,
  },
})
