/**
 * Tests for src/studio/api.ts
 *
 * Tests API route handlers for database management.
 * Uses mock factories from tests/mocks for consistent test doubles.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { Hono } from 'hono'
import {
  createDatabaseRoutes,
  requireAuthUser,
  requireActor,
} from '../../../src/studio/api'
import type { Context } from 'hono'
import {
  createMockDatabaseIndex,
  createTestDatabase,
  type MockDatabaseIndex,
} from '../../mocks/database-index'
import {
  createTestUser,
  TEST_USERS,
} from '../../mocks/auth'

// =============================================================================
// Mock Setup
// =============================================================================

// Mock the external dependencies
vi.mock('../../../src/integrations/hono/auth', () => ({
  getUser: vi.fn(),
}))

vi.mock('../../../src/worker/DatabaseIndexDO', () => ({
  getUserDatabaseIndex: vi.fn(),
}))

vi.mock('../../../src/security/csrf', () => ({
  validateCsrf: vi.fn(),
}))

import { getUser } from '../../../src/integrations/hono/auth'
import { getUserDatabaseIndex } from '../../../src/worker/DatabaseIndexDO'
import { validateCsrf } from '../../../src/security/csrf'

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Configure mocks for authenticated user with database index
 */
function setupAuthenticatedUser(
  user = TEST_USERS.regular,
  index?: MockDatabaseIndex
): MockDatabaseIndex {
  const dbIndex = index ?? createMockDatabaseIndex()
  vi.mocked(getUser).mockReturnValue(user)
  vi.mocked(getUserDatabaseIndex).mockReturnValue(dbIndex as any)
  return dbIndex
}

/**
 * Configure mocks for unauthenticated user
 */
function setupUnauthenticatedUser(): void {
  vi.mocked(getUser).mockReturnValue(null)
}

/**
 * Configure CSRF validation
 */
function setupCsrf(valid: boolean, reason?: string): void {
  vi.mocked(validateCsrf).mockReturnValue(valid ? { valid: true } : { valid: false, reason })
}

/**
 * Create a test app with actor middleware
 */
function createTestApp(app: ReturnType<typeof createDatabaseRoutes>, actor = 'users/user_123'): Hono {
  const testApp = new Hono<{ Bindings: { DEFAULT_BUCKET?: string } }>()
  testApp.use('*', (c, next) => {
    c.set('actor' as never, actor)
    ;(c.env as any) = { DEFAULT_BUCKET: 'test-bucket' }
    return next()
  })
  testApp.route('/', app)
  return testApp
}

// =============================================================================
// Tests
// =============================================================================

describe('api', () => {
  let app: ReturnType<typeof createDatabaseRoutes>

  beforeEach(() => {
    vi.clearAllMocks()
    app = createDatabaseRoutes()
  })

  describe('GET /', () => {
    it('returns 401 when not authenticated', async () => {
      setupUnauthenticatedUser()

      const res = await app.request('/', { method: 'GET' })

      expect(res.status).toBe(401)
      const body = await res.json()
      expect(body.error).toBe('Authentication required')
    })

    it('returns list of databases', async () => {
      const mockDatabases = [
        createTestDatabase({ id: 'db_1', name: 'Database 1' }),
        createTestDatabase({ id: 'db_2', name: 'Database 2' }),
      ]

      const index = createMockDatabaseIndex()
      index.list.mockResolvedValue(mockDatabases)
      setupAuthenticatedUser(TEST_USERS.regular, index)

      const res = await app.request('/', { method: 'GET' })

      expect(res.status).toBe(200)
      const body = await res.json()
      expect(body.databases).toHaveLength(2)
      expect(body.databases[0].id).toBe('db_1')
      expect(body.databases[0].name).toBe('Database 1')
      expect(body.databases[1].id).toBe('db_2')
      expect(body.databases[1].name).toBe('Database 2')
    })

    it('returns 500 on error', async () => {
      const index = createMockDatabaseIndex()
      index.list.mockRejectedValue(new Error('Database error'))
      setupAuthenticatedUser(TEST_USERS.regular, index)

      const res = await app.request('/', { method: 'GET' })

      expect(res.status).toBe(500)
      const body = await res.json()
      expect(body.error).toBe('Database error')
    })

    it('returns 500 with generic message for non-Error exceptions', async () => {
      const index = createMockDatabaseIndex()
      index.list.mockRejectedValue('not an error')
      setupAuthenticatedUser(TEST_USERS.regular, index)

      const res = await app.request('/', { method: 'GET' })

      expect(res.status).toBe(500)
      const body = await res.json()
      expect(body.error).toBe('Failed to list databases')
    })
  })

  describe('POST /create', () => {
    it('returns 403 when CSRF validation fails', async () => {
      setupCsrf(false, 'Missing header')

      const res = await app.request('/create', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'Test DB' }),
      })

      expect(res.status).toBe(403)
      const body = await res.json()
      expect(body.code).toBe('CSRF_VALIDATION_FAILED')
    })

    it('returns 401 when not authenticated', async () => {
      setupCsrf(true)
      setupUnauthenticatedUser()

      const res = await app.request('/create', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'Test DB' }),
      })

      expect(res.status).toBe(401)
    })

    it('returns 400 for invalid JSON', async () => {
      setupCsrf(true)
      setupAuthenticatedUser()
      const testApp = createTestApp(app)

      const res = await testApp.request('/create', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: 'not json',
      })

      expect(res.status).toBe(400)
      const body = await res.json()
      expect(body.error).toBe('Invalid JSON body')
    })

    it('returns 400 when name is missing', async () => {
      setupCsrf(true)
      setupAuthenticatedUser()
      const testApp = createTestApp(app)

      const res = await testApp.request('/create', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({}),
      })

      expect(res.status).toBe(400)
      const body = await res.json()
      expect(body.error).toBe('Database name is required')
    })

    it('returns 400 when name is empty', async () => {
      setupCsrf(true)
      setupAuthenticatedUser()
      const testApp = createTestApp(app)

      const res = await testApp.request('/create', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: '   ' }),
      })

      expect(res.status).toBe(400)
      const body = await res.json()
      expect(body.error).toBe('Database name is required')
    })

    it('creates database with actor context', async () => {
      const mockDatabase = createTestDatabase({ id: 'db_new', name: 'Test DB' })
      const index = createMockDatabaseIndex()
      index.register.mockResolvedValue(mockDatabase)

      setupCsrf(true)
      setupAuthenticatedUser(TEST_USERS.regular, index)
      const testApp = createTestApp(app)

      const res = await testApp.request('/create', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'Test DB', description: 'A test database' }),
      })

      expect(res.status).toBe(201)
      const body = await res.json()
      expect(body.id).toBe('db_new')
      expect(body.name).toBe('Test DB')
    })

    it('returns 409 for duplicate slug', async () => {
      const index = createMockDatabaseIndex()
      index.register.mockRejectedValue(new Error('Slug already exists'))

      setupCsrf(true)
      setupAuthenticatedUser(TEST_USERS.regular, index)
      const testApp = createTestApp(app)

      const res = await testApp.request('/create', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'Test DB' }),
      })

      expect(res.status).toBe(409)
    })
  })

  describe('GET /:id', () => {
    it('returns 401 when not authenticated', async () => {
      setupUnauthenticatedUser()

      const res = await app.request('/db_123', { method: 'GET' })

      expect(res.status).toBe(401)
    })

    it('returns database by ID', async () => {
      const mockDatabase = createTestDatabase({ id: 'db_123', name: 'Test DB' })
      const index = createMockDatabaseIndex()
      index.get.mockResolvedValue(mockDatabase)
      index.recordAccess.mockResolvedValue(undefined)
      setupAuthenticatedUser(TEST_USERS.regular, index)

      const res = await app.request('/db_123', { method: 'GET' })

      expect(res.status).toBe(200)
      const body = await res.json()
      expect(body.id).toBe('db_123')
      expect(body.name).toBe('Test DB')
    })

    it('returns 404 when database not found', async () => {
      const index = createMockDatabaseIndex()
      index.get.mockResolvedValue(null)
      setupAuthenticatedUser(TEST_USERS.regular, index)

      const res = await app.request('/db_nonexistent', { method: 'GET' })

      expect(res.status).toBe(404)
      const body = await res.json()
      expect(body.error).toBe('Database not found')
    })

    it('records access when database found', async () => {
      const mockDatabase = createTestDatabase({ id: 'db_123', name: 'Test' })
      const index = createMockDatabaseIndex()
      index.get.mockResolvedValue(mockDatabase)
      index.recordAccess.mockResolvedValue(undefined)
      setupAuthenticatedUser(TEST_USERS.regular, index)

      await app.request('/db_123', { method: 'GET' })

      expect(index.recordAccess).toHaveBeenCalledWith('db_123')
    })
  })

  describe('PATCH /:id', () => {
    it('returns 403 when CSRF validation fails', async () => {
      setupCsrf(false, 'Missing header')

      const res = await app.request('/db_123', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'New Name' }),
      })

      expect(res.status).toBe(403)
    })

    it('returns 401 when not authenticated', async () => {
      setupCsrf(true)
      setupUnauthenticatedUser()

      const res = await app.request('/db_123', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'New Name' }),
      })

      expect(res.status).toBe(401)
    })

    it('returns 400 for invalid JSON', async () => {
      setupCsrf(true)
      setupAuthenticatedUser()

      const res = await app.request('/db_123', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: 'not json',
      })

      expect(res.status).toBe(400)
    })

    it('returns 404 when database not found', async () => {
      const index = createMockDatabaseIndex()
      index.get.mockResolvedValue(null)
      setupCsrf(true)
      setupAuthenticatedUser(TEST_USERS.regular, index)

      const res = await app.request('/db_123', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'New Name' }),
      })

      expect(res.status).toBe(404)
    })

    it('updates database', async () => {
      const existingDb = createTestDatabase({ id: 'db_123', name: 'Old Name' })
      const updatedDb = createTestDatabase({ id: 'db_123', name: 'New Name' })
      const index = createMockDatabaseIndex()
      index.get.mockResolvedValue(existingDb)
      index.update.mockResolvedValue(updatedDb)

      setupCsrf(true)
      setupAuthenticatedUser(TEST_USERS.regular, index)

      const res = await app.request('/db_123', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'New Name' }),
      })

      expect(res.status).toBe(200)
      expect(index.update).toHaveBeenCalledWith('db_123', { name: 'New Name' })
    })

    it('handles update options correctly', async () => {
      const existingDb = createTestDatabase({ id: 'db_123' })
      const index = createMockDatabaseIndex()
      index.get.mockResolvedValue(existingDb)
      index.update.mockResolvedValue(existingDb)

      setupCsrf(true)
      setupAuthenticatedUser(TEST_USERS.regular, index)

      await app.request('/db_123', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: ' Test ',
          description: ' Desc ',
          visibility: 'public',
          slug: '',
          stats: { entityCount: 100 },
          metadata: { custom: 'data' },
        }),
      })

      expect(index.update).toHaveBeenCalledWith('db_123', {
        name: 'Test',
        description: 'Desc',
        visibility: 'public',
        slug: undefined,
        stats: { entityCount: 100 },
        metadata: { custom: 'data' },
      })
    })

    it('returns 409 for duplicate slug', async () => {
      const existingDb = createTestDatabase({ id: 'db_123' })
      const index = createMockDatabaseIndex()
      index.get.mockResolvedValue(existingDb)
      index.update.mockRejectedValue(new Error('Slug already exists'))

      setupCsrf(true)
      setupAuthenticatedUser(TEST_USERS.regular, index)

      const res = await app.request('/db_123', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ slug: 'taken-slug' }),
      })

      expect(res.status).toBe(409)
    })

    it('returns 400 for invalid visibility', async () => {
      const existingDb = createTestDatabase({ id: 'db_123' })
      const index = createMockDatabaseIndex()
      index.get.mockResolvedValue(existingDb)
      index.update.mockRejectedValue(new Error('Invalid visibility value'))

      setupCsrf(true)
      setupAuthenticatedUser(TEST_USERS.regular, index)

      const res = await app.request('/db_123', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ visibility: 'invalid' }),
      })

      expect(res.status).toBe(400)
    })

    it('returns 400 for invalid slug', async () => {
      const existingDb = createTestDatabase({ id: 'db_123' })
      const index = createMockDatabaseIndex()
      index.get.mockResolvedValue(existingDb)
      index.update.mockRejectedValue(new Error('Invalid slug: must be lowercase alphanumeric'))

      setupCsrf(true)
      setupAuthenticatedUser(TEST_USERS.regular, index)

      const res = await app.request('/db_123', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ slug: 'INVALID_SLUG!' }),
      })

      expect(res.status).toBe(400)
      const body = await res.json()
      expect(body.error).toContain('Invalid slug')
    })

    it('returns 404 when update returns null', async () => {
      const existingDb = createTestDatabase({ id: 'db_123' })
      const index = createMockDatabaseIndex()
      index.get.mockResolvedValue(existingDb)
      index.update.mockResolvedValue(null)

      setupCsrf(true)
      setupAuthenticatedUser(TEST_USERS.regular, index)

      const res = await app.request('/db_123', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'New Name' }),
      })

      expect(res.status).toBe(404)
      const body = await res.json()
      expect(body.error).toBe('Database not found')
    })

    it('returns 500 for unexpected errors', async () => {
      const existingDb = createTestDatabase({ id: 'db_123' })
      const index = createMockDatabaseIndex()
      index.get.mockResolvedValue(existingDb)
      index.update.mockRejectedValue(new Error('Unexpected database error'))

      setupCsrf(true)
      setupAuthenticatedUser(TEST_USERS.regular, index)

      const res = await app.request('/db_123', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'New Name' }),
      })

      expect(res.status).toBe(500)
      const body = await res.json()
      expect(body.error).toBe('Unexpected database error')
    })

    it('returns 500 with generic message for non-Error exceptions', async () => {
      const existingDb = createTestDatabase({ id: 'db_123' })
      const index = createMockDatabaseIndex()
      index.get.mockResolvedValue(existingDb)
      index.update.mockRejectedValue('not an error object')

      setupCsrf(true)
      setupAuthenticatedUser(TEST_USERS.regular, index)

      const res = await app.request('/db_123', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'New Name' }),
      })

      expect(res.status).toBe(500)
      const body = await res.json()
      expect(body.error).toBe('Failed to update database')
    })
  })

  describe('DELETE /:id', () => {
    it('returns 403 when CSRF validation fails', async () => {
      setupCsrf(false, 'Missing header')

      const res = await app.request('/db_123', { method: 'DELETE' })

      expect(res.status).toBe(403)
    })

    it('returns 401 when not authenticated', async () => {
      setupCsrf(true)
      setupUnauthenticatedUser()

      const res = await app.request('/db_123', { method: 'DELETE' })

      expect(res.status).toBe(401)
    })

    it('returns 404 when database not found', async () => {
      const index = createMockDatabaseIndex()
      index.get.mockResolvedValue(null)
      setupCsrf(true)
      setupAuthenticatedUser(TEST_USERS.regular, index)

      const res = await app.request('/db_123', { method: 'DELETE' })

      expect(res.status).toBe(404)
    })

    it('deletes database', async () => {
      const existingDb = createTestDatabase({ id: 'db_123' })
      const index = createMockDatabaseIndex()
      index.get.mockResolvedValue(existingDb)
      index.unregister.mockResolvedValue(true)

      setupCsrf(true)
      setupAuthenticatedUser(TEST_USERS.regular, index)

      const res = await app.request('/db_123', { method: 'DELETE' })

      expect(res.status).toBe(200)
      const body = await res.json()
      expect(body).toEqual({ deleted: true })
    })

    it('returns 404 when unregister returns false', async () => {
      const existingDb = createTestDatabase({ id: 'db_123' })
      const index = createMockDatabaseIndex()
      index.get.mockResolvedValue(existingDb)
      index.unregister.mockResolvedValue(false)

      setupCsrf(true)
      setupAuthenticatedUser(TEST_USERS.regular, index)

      const res = await app.request('/db_123', { method: 'DELETE' })

      expect(res.status).toBe(404)
    })

    it('returns 500 on error', async () => {
      const existingDb = createTestDatabase({ id: 'db_123' })
      const index = createMockDatabaseIndex()
      index.get.mockResolvedValue(existingDb)
      index.unregister.mockRejectedValue(new Error('Database connection error'))

      setupCsrf(true)
      setupAuthenticatedUser(TEST_USERS.regular, index)

      const res = await app.request('/db_123', { method: 'DELETE' })

      expect(res.status).toBe(500)
      const body = await res.json()
      expect(body.error).toBe('Database connection error')
    })

    it('returns 500 with generic message for non-Error exceptions', async () => {
      const existingDb = createTestDatabase({ id: 'db_123' })
      const index = createMockDatabaseIndex()
      index.get.mockResolvedValue(existingDb)
      index.unregister.mockRejectedValue('string error')

      setupCsrf(true)
      setupAuthenticatedUser(TEST_USERS.regular, index)

      const res = await app.request('/db_123', { method: 'DELETE' })

      expect(res.status).toBe(500)
      const body = await res.json()
      expect(body.error).toBe('Failed to delete database')
    })
  })

  describe('requireAuthUser', () => {
    it('returns user when authenticated', () => {
      const mockUser = createTestUser({ id: 'user_123', email: 'test@example.com' })
      vi.mocked(getUser).mockReturnValue(mockUser)

      const mockContext = {} as Context
      const result = requireAuthUser(mockContext)

      expect(result).toEqual(mockUser)
    })

    it('throws when not authenticated', () => {
      vi.mocked(getUser).mockReturnValue(null)

      const mockContext = {} as Context

      expect(() => requireAuthUser(mockContext)).toThrow('Authentication required')
    })
  })

  describe('requireActor', () => {
    it('returns actor when present', () => {
      const mockContext = {
        var: { actor: 'users/user_123' },
      } as unknown as Context<{ Variables: { actor: string } }>

      const result = requireActor(mockContext)

      expect(result).toBe('users/user_123')
    })

    it('throws when actor is null', () => {
      const mockContext = {
        var: { actor: null },
      } as unknown as Context<{ Variables: { actor: string | null } }>

      expect(() => requireActor(mockContext)).toThrow('Actor required')
    })

    it('throws when actor is undefined', () => {
      const mockContext = {
        var: {},
      } as unknown as Context<{ Variables: { actor?: string } }>

      expect(() => requireActor(mockContext)).toThrow('Actor required')
    })
  })
})
