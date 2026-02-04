/**
 * Benchmark Handler Tests
 *
 * Tests for /benchmark* route handlers.
 * Tests R2 bucket validation and handler delegation.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  handleBenchmark,
  handleBenchmarkDatasets,
  handleBenchmarkIndexed,
  handleBenchmarkBackends,
  handleBenchmarkDatasetBackends,
} from '../../../../src/worker/handlers/benchmark'
import { MissingBucketError } from '../../../../src/worker/r2-errors'

// Mock the downstream benchmark modules
vi.mock('../../../../src/worker/benchmark', () => ({
  handleBenchmarkRequest: vi.fn().mockResolvedValue(new Response('benchmark-ok')),
}))
vi.mock('../../../../src/worker/benchmark-datasets', () => ({
  handleDatasetBenchmarkRequest: vi.fn().mockResolvedValue(new Response('dataset-ok')),
}))
vi.mock('../../../../src/worker/benchmark-indexed', () => ({
  handleIndexedBenchmarkRequest: vi.fn().mockResolvedValue(new Response('indexed-ok')),
}))
vi.mock('../../../../src/worker/benchmark-backends', () => ({
  handleBackendsBenchmarkRequest: vi.fn().mockResolvedValue(new Response('backends-ok')),
}))
vi.mock('../../../../src/worker/benchmark-datasets-backends', () => ({
  handleDatasetBackendsBenchmarkRequest: vi.fn().mockResolvedValue(new Response('dataset-backends-ok')),
}))

describe('Benchmark Handlers', () => {
  function createBenchmarkCtx(overrides: Record<string, unknown> = {}) {
    return {
      request: new Request('https://api.parquedb.com/benchmark'),
      url: new URL('https://api.parquedb.com/benchmark'),
      path: '/benchmark',
      baseUrl: 'https://api.parquedb.com',
      startTime: performance.now(),
      env: { BUCKET: {}, CDN_BUCKET: {}, ...overrides },
      params: {},
    } as any
  }

  beforeEach(() => {
    vi.clearAllMocks()
  })

  // ===========================================================================
  // handleBenchmark
  // ===========================================================================

  describe('handleBenchmark', () => {
    it('should throw MissingBucketError when BUCKET is not configured', async () => {
      const ctx = createBenchmarkCtx({ BUCKET: undefined })

      await expect(handleBenchmark(ctx)).rejects.toThrow(MissingBucketError)
    })

    it('should delegate to handleBenchmarkRequest when BUCKET is configured', async () => {
      const ctx = createBenchmarkCtx()

      const response = await handleBenchmark(ctx)

      expect(response).toBeInstanceOf(Response)
      const text = await response.text()
      expect(text).toBe('benchmark-ok')
    })
  })

  // ===========================================================================
  // handleBenchmarkDatasets
  // ===========================================================================

  describe('handleBenchmarkDatasets', () => {
    it('should throw MissingBucketError when BUCKET is not configured', async () => {
      const ctx = createBenchmarkCtx({ BUCKET: undefined })

      await expect(handleBenchmarkDatasets(ctx)).rejects.toThrow(MissingBucketError)
    })

    it('should delegate to handleDatasetBenchmarkRequest when configured', async () => {
      const ctx = createBenchmarkCtx()

      const response = await handleBenchmarkDatasets(ctx)

      const text = await response.text()
      expect(text).toBe('dataset-ok')
    })
  })

  // ===========================================================================
  // handleBenchmarkIndexed
  // ===========================================================================

  describe('handleBenchmarkIndexed', () => {
    it('should throw MissingBucketError when BUCKET is not configured', async () => {
      const ctx = createBenchmarkCtx({ BUCKET: undefined })

      await expect(handleBenchmarkIndexed(ctx)).rejects.toThrow(MissingBucketError)
    })

    it('should delegate to handleIndexedBenchmarkRequest when configured', async () => {
      const ctx = createBenchmarkCtx()

      const response = await handleBenchmarkIndexed(ctx)

      const text = await response.text()
      expect(text).toBe('indexed-ok')
    })
  })

  // ===========================================================================
  // handleBenchmarkBackends
  // ===========================================================================

  describe('handleBenchmarkBackends', () => {
    it('should throw MissingBucketError when CDN_BUCKET is not configured', async () => {
      const ctx = createBenchmarkCtx({ CDN_BUCKET: undefined })

      await expect(handleBenchmarkBackends(ctx)).rejects.toThrow(MissingBucketError)
    })

    it('should delegate to handleBackendsBenchmarkRequest when configured', async () => {
      const ctx = createBenchmarkCtx()

      const response = await handleBenchmarkBackends(ctx)

      const text = await response.text()
      expect(text).toBe('backends-ok')
    })
  })

  // ===========================================================================
  // handleBenchmarkDatasetBackends
  // ===========================================================================

  describe('handleBenchmarkDatasetBackends', () => {
    it('should throw MissingBucketError when BUCKET is not configured', async () => {
      const ctx = createBenchmarkCtx({ BUCKET: undefined })

      await expect(handleBenchmarkDatasetBackends(ctx)).rejects.toThrow(MissingBucketError)
    })

    it('should delegate to handleDatasetBackendsBenchmarkRequest when configured', async () => {
      const ctx = createBenchmarkCtx()

      const response = await handleBenchmarkDatasetBackends(ctx)

      const text = await response.text()
      expect(text).toBe('dataset-backends-ok')
    })
  })
})
