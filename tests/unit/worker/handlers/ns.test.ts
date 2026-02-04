/**
 * NS Route Handler Tests
 *
 * Tests for /ns/:namespace routes.
 * Tests GET, POST, PATCH, DELETE operations and CSRF validation.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { handleNsRoute } from '../../../../src/worker/handlers/ns'
import {
  createMockWorker,
  createMockHandlerContext,
  createMockCaches,
} from '../../../mocks'
import type { MockHandlerContext } from '../../../mocks/worker'

// Mock logger
vi.mock('../../../../src/utils/logger', () => ({
  logger: {
    warn: vi.fn(),
    info: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
  },
}))

// Mock the Cloudflare caches API
const mockCachesObj = createMockCaches()
// @ts-expect-error - mocking global caches API
globalThis.caches = mockCachesObj

describe('NS Route Handler', () => {
  let mockWorker: ReturnType<typeof createMockWorker>

  beforeEach(() => {
    vi.clearAllMocks()
    mockWorker = createMockWorker()
  })

  // ===========================================================================
  // GET /ns/:namespace - List entities
  // ===========================================================================

  describe('GET /ns/:namespace - list', () => {
    it('should return items from find query', async () => {
      mockWorker.find.mockResolvedValue({
        items: [
          { $id: 'users/u1', $type: 'user', name: 'Alice' },
          { $id: 'users/u2', $type: 'user', name: 'Bob' },
        ],
        hasMore: false,
        stats: {},
      })

      const ctx = createMockHandlerContext(
        'https://api.parquedb.com/ns/users',
        { worker: mockWorker }
      )

      const response = await handleNsRoute(ctx as any, 'users')

      expect(response.status).toBe(200)
      const body = (await response.json()) as Record<string, unknown>
      const api = body.api as Record<string, unknown>
      expect(api.resource).toBe('collection')
      expect(api.namespace).toBe('users')
      expect(body.items).toBeDefined()
    })

    it('should parse filter from query params', async () => {
      mockWorker.find.mockResolvedValue({
        items: [],
        hasMore: false,
        stats: {},
      })

      const ctx = createMockHandlerContext(
        'https://api.parquedb.com/ns/users?filter={"status":"active"}',
        { worker: mockWorker }
      )

      await handleNsRoute(ctx as any, 'users')

      expect(mockWorker.find).toHaveBeenCalledWith(
        'users',
        { status: 'active' },
        expect.any(Object)
      )
    })

    it('should return 400 for invalid filter JSON', async () => {
      const ctx = createMockHandlerContext(
        'https://api.parquedb.com/ns/users?filter=not-json',
        { worker: mockWorker }
      )

      const response = await handleNsRoute(ctx as any, 'users')

      expect(response.status).toBe(400)
    })
  })

  // ===========================================================================
  // GET /ns/:namespace/:id - Get entity
  // ===========================================================================

  describe('GET /ns/:namespace/:id - get entity', () => {
    it('should return entity when found', async () => {
      mockWorker.get.mockResolvedValue({
        $id: 'users/u1',
        $type: 'user',
        name: 'Alice',
      })

      const ctx = createMockHandlerContext(
        'https://api.parquedb.com/ns/users/u1',
        { worker: mockWorker }
      )

      const response = await handleNsRoute(ctx as any, 'users', 'u1')

      expect(response.status).toBe(200)
      const body = (await response.json()) as Record<string, unknown>
      const api = body.api as Record<string, unknown>
      expect(api.resource).toBe('entity')
      expect(api.namespace).toBe('users')
      expect(api.id).toBe('u1')
    })

    it('should return 404 when entity not found', async () => {
      mockWorker.get.mockResolvedValue(null)

      const ctx = createMockHandlerContext(
        'https://api.parquedb.com/ns/users/nonexistent',
        { worker: mockWorker }
      )

      const response = await handleNsRoute(ctx as any, 'users', 'nonexistent')

      expect(response.status).toBe(404)
    })

    it('should include correct links', async () => {
      mockWorker.get.mockResolvedValue({
        $id: 'users/u1',
        $type: 'user',
        name: 'Alice',
      })

      const ctx = createMockHandlerContext(
        'https://api.parquedb.com/ns/users/u1',
        { worker: mockWorker }
      )

      const response = await handleNsRoute(ctx as any, 'users', 'u1')
      const body = (await response.json()) as Record<string, unknown>

      const links = body.links as Record<string, string>
      expect(links.self).toContain('/ns/users/u1')
      expect(links.collection).toContain('/ns/users')
      expect(links.home).toBeDefined()
    })
  })

  // ===========================================================================
  // POST /ns/:namespace - Create entity
  // ===========================================================================

  describe('POST /ns/:namespace - create', () => {
    it('should create entity and return 201', async () => {
      mockWorker.create.mockResolvedValue({
        $id: 'users/u-new',
        $type: 'user',
        name: 'Charlie',
      })

      const ctx = createMockHandlerContext(
        'https://api.parquedb.com/ns/users',
        {
          worker: mockWorker,
          method: 'POST',
          body: { type: 'user', name: 'Charlie' },
        }
      )

      const response = await handleNsRoute(ctx as any, 'users')

      expect(response.status).toBe(201)
      expect(mockWorker.create).toHaveBeenCalledWith('users', expect.objectContaining({ name: 'Charlie' }))
    })

    it('should reject POST without CSRF headers', async () => {
      const ctx = createMockHandlerContext(
        'https://api.parquedb.com/ns/users',
        {
          worker: mockWorker,
          method: 'POST',
          body: { name: 'Test' },
          headers: {
            'X-Requested-With': '', // empty
            'Origin': '',
          },
        }
      )
      // Remove CSRF headers that createMockHandlerContext auto-adds
      const request = new Request('https://api.parquedb.com/ns/users', {
        method: 'POST',
        body: JSON.stringify({ name: 'Test' }),
        headers: { 'Content-Type': 'application/json' },
      })
      ctx.request = request

      const response = await handleNsRoute(ctx as any, 'users')

      expect(response.status).toBe(403)
    })

    it('should throw error for invalid JSON body', async () => {
      const request = new Request('https://api.parquedb.com/ns/users', {
        method: 'POST',
        body: 'not-json',
        headers: {
          'Content-Type': 'text/plain',
          'Origin': 'https://api.parquedb.com',
          'X-Requested-With': 'XMLHttpRequest',
        },
      })

      const ctx = createMockHandlerContext(
        'https://api.parquedb.com/ns/users',
        { worker: mockWorker, method: 'POST' }
      )
      ctx.request = request

      await expect(handleNsRoute(ctx as any, 'users')).rejects.toThrow('Invalid JSON body')
    })
  })

  // ===========================================================================
  // PATCH /ns/:namespace/:id - Update entity
  // ===========================================================================

  describe('PATCH /ns/:namespace/:id - update', () => {
    it('should update entity and return result', async () => {
      mockWorker.update.mockResolvedValue({ matched: 1, modified: 1 })

      const ctx = createMockHandlerContext(
        'https://api.parquedb.com/ns/users/u1',
        {
          worker: mockWorker,
          method: 'PATCH',
          body: { $set: { name: 'Updated' } },
        }
      )

      const response = await handleNsRoute(ctx as any, 'users', 'u1')

      expect(response.status).toBe(200)
      expect(mockWorker.update).toHaveBeenCalledWith(
        'users',
        'u1',
        expect.objectContaining({ $set: { name: 'Updated' } })
      )
    })

    it('should return 400 when ID is missing for PATCH', async () => {
      const ctx = createMockHandlerContext(
        'https://api.parquedb.com/ns/users',
        {
          worker: mockWorker,
          method: 'PATCH',
          body: { $set: { name: 'Updated' } },
        }
      )

      const response = await handleNsRoute(ctx as any, 'users')

      expect(response.status).toBe(400)
    })

    it('should reject PATCH without CSRF headers', async () => {
      const request = new Request('https://api.parquedb.com/ns/users/u1', {
        method: 'PATCH',
        body: JSON.stringify({ $set: { name: 'Hacked' } }),
        headers: { 'Content-Type': 'application/json' },
      })

      const ctx = createMockHandlerContext(
        'https://api.parquedb.com/ns/users/u1',
        { worker: mockWorker, method: 'PATCH' }
      )
      ctx.request = request

      const response = await handleNsRoute(ctx as any, 'users', 'u1')

      expect(response.status).toBe(403)
    })
  })

  // ===========================================================================
  // DELETE /ns/:namespace/:id - Delete entity
  // ===========================================================================

  describe('DELETE /ns/:namespace/:id - delete', () => {
    it('should delete entity and return result', async () => {
      mockWorker.delete.mockResolvedValue({ deleted: 1 })

      const ctx = createMockHandlerContext(
        'https://api.parquedb.com/ns/users/u1',
        {
          worker: mockWorker,
          method: 'DELETE',
        }
      )

      const response = await handleNsRoute(ctx as any, 'users', 'u1')

      expect(response.status).toBe(200)
      expect(mockWorker.delete).toHaveBeenCalledWith('users', 'u1')
    })

    it('should return 400 when ID is missing for DELETE', async () => {
      const ctx = createMockHandlerContext(
        'https://api.parquedb.com/ns/users',
        {
          worker: mockWorker,
          method: 'DELETE',
        }
      )

      const response = await handleNsRoute(ctx as any, 'users')

      expect(response.status).toBe(400)
    })

    it('should reject DELETE without CSRF headers', async () => {
      const request = new Request('https://api.parquedb.com/ns/users/u1', {
        method: 'DELETE',
        headers: {},
      })

      const ctx = createMockHandlerContext(
        'https://api.parquedb.com/ns/users/u1',
        { worker: mockWorker, method: 'DELETE' }
      )
      ctx.request = request

      const response = await handleNsRoute(ctx as any, 'users', 'u1')

      expect(response.status).toBe(403)
    })
  })

  // ===========================================================================
  // Unsupported Methods
  // ===========================================================================

  describe('unsupported methods', () => {
    it('should return 405 for PUT', async () => {
      const ctx = createMockHandlerContext(
        'https://api.parquedb.com/ns/users',
        {
          worker: mockWorker,
          method: 'PUT',
        }
      )

      const response = await handleNsRoute(ctx as any, 'users')

      expect(response.status).toBe(405)
    })
  })
})
