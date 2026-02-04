/**
 * Root Handler Tests
 *
 * Tests for the / root endpoint handler.
 * Tests API overview response, links, and response format.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { handleRoot } from '../../../../src/worker/handlers/root'
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

describe('Root Handler', () => {
  let mockWorker: ReturnType<typeof createMockWorker>
  let mockContext: MockHandlerContext

  beforeEach(() => {
    mockWorker = createMockWorker()
  })

  // ===========================================================================
  // Happy Path Tests
  // ===========================================================================

  describe('GET / - happy path', () => {
    it('should return 200 with API overview', () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/',
        { worker: mockWorker }
      )

      const response = handleRoot(mockContext as any)

      expect(response.status).toBe(200)
    })

    it('should include api info in response body', async () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/',
        { worker: mockWorker }
      )

      const response = handleRoot(mockContext as any)
      const body = (await response.json()) as Record<string, unknown>

      expect(body.api).toBeDefined()
      const api = body.api as Record<string, unknown>
      expect(api.name).toBe('ParqueDB')
      expect(api.version).toBe('0.1.0')
      expect(api.description).toContain('hybrid relational/document/graph database')
      expect(api.documentation).toContain('github.com')
    })

    it('should include navigation links in response', async () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/',
        { worker: mockWorker }
      )

      const response = handleRoot(mockContext as any)
      const body = (await response.json()) as Record<string, unknown>

      const links = body.links as Record<string, string>
      expect(links.self).toBeDefined()
      expect(links.datasets).toContain('/datasets')
      expect(links.health).toContain('/health')
      expect(links.benchmark).toContain('/benchmark')
    })

    it('should include CORS header', () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/',
        { worker: mockWorker }
      )

      const response = handleRoot(mockContext as any)

      expect(response.headers.get('Access-Control-Allow-Origin')).toBe('*')
    })

    it('should return JSON content type', () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/',
        { worker: mockWorker }
      )

      const response = handleRoot(mockContext as any)

      expect(response.headers.get('Content-Type')).toContain('application/json')
    })
  })

  // ===========================================================================
  // URL Handling Tests
  // ===========================================================================

  describe('URL handling', () => {
    it('should use baseUrl for link generation', async () => {
      mockContext = createMockHandlerContext(
        'https://custom.domain.com/',
        { worker: mockWorker }
      )
      mockContext.baseUrl = 'https://custom.domain.com'

      const response = handleRoot(mockContext as any)
      const body = (await response.json()) as Record<string, unknown>

      const links = body.links as Record<string, string>
      expect(links.self).toBe('https://custom.domain.com')
      expect(links.datasets).toBe('https://custom.domain.com/datasets')
      expect(links.health).toBe('https://custom.domain.com/health')
    })
  })

  // ===========================================================================
  // Edge Cases
  // ===========================================================================

  describe('edge cases', () => {
    it('should be synchronous (no async needed)', () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/',
        { worker: mockWorker }
      )

      const response = handleRoot(mockContext as any)

      expect(response).toBeInstanceOf(Response)
      expect(response.status).toBe(200)
    })

    it('should not call any worker methods', () => {
      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/',
        { worker: mockWorker }
      )

      handleRoot(mockContext as any)

      expect(mockWorker.find).not.toHaveBeenCalled()
      expect(mockWorker.get).not.toHaveBeenCalled()
      expect(mockWorker.getRelationships).not.toHaveBeenCalled()
    })

    it('should work with minimal context', () => {
      const minimalContext = {
        request: new Request('https://api.parquedb.com/'),
        baseUrl: 'https://api.parquedb.com',
        startTime: performance.now(),
      }

      const response = handleRoot(minimalContext as any)
      expect(response.status).toBe(200)
    })
  })
})
