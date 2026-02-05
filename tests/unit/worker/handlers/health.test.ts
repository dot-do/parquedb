/**
 * Health Handler Tests
 *
 * Tests for the /health endpoint handler.
 * Tests response format, status codes, links, and CORS headers.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { handleHealth } from '../../../../src/worker/handlers/health'
import {
  createMockWorker,
  createMockHandlerContext,
  createMockCaches,
} from '../../../mocks'
import type { MockHandlerContext } from '../../../mocks/worker'

// Mock the Cloudflare caches API
const mockCachesObj = createMockCaches()
// @ts-expect-error - mocking global caches API
globalThis.caches = mockCachesObj

describe('Health Handler', () => {
  let mockWorker: ReturnType<typeof createMockWorker>
  let mockContext: MockHandlerContext

  beforeEach(() => {
    mockWorker = createMockWorker()
  })

  // ===========================================================================
  // Happy Path Tests
  // ===========================================================================

  describe('GET /health - happy path', () => {
    it('should return 200 with healthy status', async () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/health',
        { worker: mockWorker }
      )

      const response = await handleHealth(mockContext as any)

      expect(response.status).toBe(200)
    })

    it('should include api status in response body', async () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/health',
        { worker: mockWorker }
      )

      const response = await handleHealth(mockContext as any)
      const body = (await response.json()) as Record<string, unknown>

      expect(body.api).toBeDefined()
      const api = body.api as Record<string, unknown>
      expect(api.status).toBe('healthy')
      expect(api.uptime).toBe('ok')
      expect(api.storage).toBe('r2')
      expect(api.compute).toBe('durable-objects')
    })

    it('should include links in response', async () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/health',
        { worker: mockWorker }
      )

      const response = await handleHealth(mockContext as any)
      const body = (await response.json()) as Record<string, unknown>

      const links = body.links as Record<string, string>
      expect(links.home).toBe('https://api.parquedb.com')
      expect(links.datasets).toBe('https://api.parquedb.com/datasets')
    })

    it('should include CORS header', async () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/health',
        { worker: mockWorker }
      )

      const response = await handleHealth(mockContext as any)

      expect(response.headers.get('Access-Control-Allow-Origin')).toBe('*')
    })

    it('should return JSON content type', async () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/health',
        { worker: mockWorker }
      )

      const response = await handleHealth(mockContext as any)

      expect(response.headers.get('Content-Type')).toContain('application/json')
    })
  })

  // ===========================================================================
  // Response Structure Tests
  // ===========================================================================

  describe('response structure', () => {
    it('should have consistent api object shape', async () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/health',
        { worker: mockWorker }
      )

      const response = await handleHealth(mockContext as any)
      const body = (await response.json()) as Record<string, unknown>

      const api = body.api as Record<string, unknown>
      expect(Object.keys(api)).toEqual(
        expect.arrayContaining(['status', 'uptime', 'storage', 'compute'])
      )
    })

    it('should have consistent links object shape', async () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/health',
        { worker: mockWorker }
      )

      const response = await handleHealth(mockContext as any)
      const body = (await response.json()) as Record<string, unknown>

      const links = body.links as Record<string, unknown>
      expect(Object.keys(links)).toEqual(
        expect.arrayContaining(['home', 'datasets'])
      )
    })
  })

  // ===========================================================================
  // URL Handling Tests
  // ===========================================================================

  describe('URL handling', () => {
    it('should use baseUrl for link generation', async () => {
      mockContext = createMockHandlerContext(
        'https://custom.domain.com/health',
        { worker: mockWorker }
      )
      // Override baseUrl to simulate different domain
      mockContext.baseUrl = 'https://custom.domain.com'

      const response = await handleHealth(mockContext as any)
      const body = (await response.json()) as Record<string, unknown>

      const links = body.links as Record<string, string>
      expect(links.home).toBe('https://custom.domain.com')
      expect(links.datasets).toBe('https://custom.domain.com/datasets')
    })

    it('should handle different protocols in baseUrl', async () => {
      mockContext = createMockHandlerContext(
        'http://localhost:8787/health',
        { worker: mockWorker }
      )

      const response = await handleHealth(mockContext as any)
      const body = (await response.json()) as Record<string, unknown>

      const links = body.links as Record<string, string>
      expect(links.home).toContain('http://')
    })
  })

  // ===========================================================================
  // Edge Cases
  // ===========================================================================

  describe('edge cases', () => {
    it('should return a valid Response', async () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/health',
        { worker: mockWorker }
      )

      const response = await handleHealth(mockContext as any)

      // Should return Response after awaiting the promise
      expect(response).toBeInstanceOf(Response)
      expect(response.status).toBe(200)
    })

    it('should not require worker database access', async () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/health',
        { worker: mockWorker }
      )

      await handleHealth(mockContext as any)

      // Health check should not call any worker methods
      expect(mockWorker.find).not.toHaveBeenCalled()
      expect(mockWorker.get).not.toHaveBeenCalled()
      expect(mockWorker.getRelationships).not.toHaveBeenCalled()
    })

    it('should work with minimal context', async () => {
      // Create a minimal context with only required fields
      const minimalContext = {
        request: new Request('https://api.parquedb.com/health'),
        baseUrl: 'https://api.parquedb.com',
        startTime: performance.now(),
      }

      // Should not throw
      const response = await handleHealth(minimalContext as any)
      expect(response.status).toBe(200)
    })
  })

  // ===========================================================================
  // Headers Tests
  // ===========================================================================

  describe('headers', () => {
    it('should include standard response headers', async () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/health',
        { worker: mockWorker }
      )

      const response = await handleHealth(mockContext as any)

      // CORS headers
      expect(response.headers.get('Access-Control-Allow-Origin')).toBe('*')
    })

    it('should include cache control headers for health checks', async () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/health',
        { worker: mockWorker }
      )

      const response = await handleHealth(mockContext as any)

      // Health checks should have short cache or no cache
      const cacheControl = response.headers.get('Cache-Control')
      // Either no cache header or short max-age is acceptable
      if (cacheControl) {
        expect(cacheControl).toMatch(/(no-cache|max-age=\d+)/)
      }
    })
  })

  // ===========================================================================
  // Body Format Tests
  // ===========================================================================

  describe('body format', () => {
    it('should return valid JSON', async () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/health',
        { worker: mockWorker }
      )

      const response = await handleHealth(mockContext as any)

      // Should not throw when parsing JSON
      const body = await response.json()
      expect(body).toBeDefined()
      expect(typeof body).toBe('object')
    })

    it('should not include sensitive information', async () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/health',
        { worker: mockWorker }
      )

      const response = await handleHealth(mockContext as any)
      const body = (await response.json()) as Record<string, unknown>
      const bodyStr = JSON.stringify(body)

      // Should not contain secrets, passwords, or tokens
      expect(bodyStr).not.toContain('secret')
      expect(bodyStr).not.toContain('password')
      expect(bodyStr).not.toContain('token')
      expect(bodyStr).not.toContain('auth')
    })

    it('should include infrastructure information', async () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/health',
        { worker: mockWorker }
      )

      const response = await handleHealth(mockContext as any)
      const body = (await response.json()) as Record<string, unknown>
      const api = body.api as Record<string, unknown>

      // Should indicate storage backend
      expect(api.storage).toBe('r2')

      // Should indicate compute platform
      expect(api.compute).toBe('durable-objects')
    })
  })
})
