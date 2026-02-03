/**
 * E2E Benchmark Suite for Deployed Workers
 *
 * This module provides a comprehensive benchmark suite for testing ParqueDB
 * performance against deployed Cloudflare Workers.
 *
 * Components:
 * - types.ts: Type definitions for benchmark results and configuration
 * - utils.ts: Utility functions for HTTP requests, statistics, and formatting
 * - runner.ts: CLI runner for executing benchmarks
 * - deployed-worker.bench.ts: Vitest benchmark tests
 * - deployed-worker.test.ts: Standard vitest tests with performance assertions
 *
 * Usage:
 *
 *   # Run benchmarks via vitest (uses local miniflare)
 *   pnpm bench:e2e
 *
 *   # Run benchmarks against deployed worker
 *   WORKER_URL=https://api.parquedb.com pnpm bench:e2e:runner
 *
 *   # Run as tests with performance assertions
 *   WORKER_URL=https://api.parquedb.com pnpm test:e2e
 *
 * Performance Targets (from CLAUDE.md):
 *
 *   | Operation             | Target (p50) | Target (p99) |
 *   |-----------------------|--------------|--------------|
 *   | Get by ID             | 5ms          | 20ms         |
 *   | Find (indexed)        | 20ms         | 100ms        |
 *   | Find (scan)           | 100ms        | 500ms        |
 *   | Create                | 10ms         | 50ms         |
 *   | Update                | 15ms         | 75ms         |
 *   | Relationship traverse | 50ms         | 200ms        |
 */

// Re-export types
export * from './types'

// Re-export utilities
export * from './utils'

// Re-export runner (when used programmatically)
export { E2EBenchmarkRunner } from './runner'
