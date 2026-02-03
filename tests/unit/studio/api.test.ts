/**
 * Tests for src/studio/api.ts
 *
 * Tests API route handlers for database management.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { Hono } from 'hono'
import {
  createDatabaseRoutes,
  requireAuthUser,
  requireActor,
} from '../../../src/studio/api'
import type { Context } from 'hono'

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

describe('api', () => {
  let app: ReturnType<typeof createDatabaseRoutes>

  beforeEach(() => {
    vi.clearAllMocks()
    app = createDatabaseRoutes()
  })

  describe('GET /', () => {
    it('returns 401 when not authenticated', async () => {
      vi.mocked(getUser).mockReturnValue(null)

      const res = await app.request('/', {
        method: 'GET',
      })

      expect(res.status).toBe(401)
      const body = await res.json()
      expect(body.error).toBe('Authentication required')
    })

    it('returns list of databases', async () => {
      const mockDatabases = [
        { id: 'db_1', name: 'Database 1' },
        { id: 'db_2', name: 'Database 2' },
      ]

      vi.mocked(getUser).mockReturnValue({ id: 'user_123', email: 'test@example.com' })
      vi.mocked(getUserDatabaseIndex).mockReturnValue({
        list: vi.fn().mockResolvedValue(mockDatabases),
      } as any)

      const res = await app.request('/', {
        method: 'GET',
      })

      expect(res.status).toBe(200)
      const body = await res.json()
      expect(body.databases).toEqual(mockDatabases)
    })

    it('returns 500 on error', async () => {
      vi.mocked(getUser).mockReturnValue({ id: 'user_123', email: 'test@example.com' })
      vi.mocked(getUserDatabaseIndex).mockReturnValue({
        list: vi.fn().mockRejectedValue(new Error('Database error')),
      } as any)

      const res = await app.request('/', {
        method: 'GET',
      })

      expect(res.status).toBe(500)
      const body = await res.json()
      expect(body.error).toBe('Database error')
    })

    it('returns 500 with generic message for non-Error exceptions', async () => {
      vi.mocked(getUser).mockReturnValue({ id: 'user_123', email: 'test@example.com' })
      vi.mocked(getUserDatabaseIndex).mockReturnValue({
        list: vi.fn().mockRejectedValue('not an error'),
      } as any)

      const res = await app.request('/', {
        method: 'GET',
      })

      expect(res.status).toBe(500)
      const body = await res.json()
      expect(body.error).toBe('Failed to list databases')
    })
  })

  describe('POST /create', () => {
    it('returns 403 when CSRF validation fails', async () => {
      vi.mocked(validateCsrf).mockReturnValue({ valid: false, reason: 'Missing header' })

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
      vi.mocked(validateCsrf).mockReturnValue({ valid: true })
      vi.mocked(getUser).mockReturnValue(null)

      const res = await app.request('/create', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'Test DB' }),
      })

      expect(res.status).toBe(401)
    })

    it('returns 400 for invalid JSON', async () => {
      vi.mocked(validateCsrf).mockReturnValue({ valid: true })
      vi.mocked(getUser).mockReturnValue({ id: 'user_123', email: 'test@example.com' })

      // Create test app with actor middleware to avoid 401
      const testApp = new Hono()
      testApp.use('*', (c, next) => {
        c.set('actor' as never, 'users/user_123')
        return next()
      })
      testApp.route('/', app)

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
      vi.mocked(validateCsrf).mockReturnValue({ valid: true })
      vi.mocked(getUser).mockReturnValue({ id: 'user_123', email: 'test@example.com' })

      const testApp = new Hono()
      testApp.use('*', (c, next) => {
        c.set('actor' as never, 'users/user_123')
        return next()
      })
      testApp.route('/', app)

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
      vi.mocked(validateCsrf).mockReturnValue({ valid: true })
      vi.mocked(getUser).mockReturnValue({ id: 'user_123', email: 'test@example.com' })

      const testApp = new Hono()
      testApp.use('*', (c, next) => {
        c.set('actor' as never, 'users/user_123')
        return next()
      })
      testApp.route('/', app)

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
      const mockDatabase = { id: 'db_new', name: 'Test DB' }
      const mockRegister = vi.fn().mockResolvedValue(mockDatabase)

      vi.mocked(validateCsrf).mockReturnValue({ valid: true })
      vi.mocked(getUser).mockReturnValue({ id: 'user_123', email: 'test@example.com' })
      vi.mocked(getUserDatabaseIndex).mockReturnValue({
        register: mockRegister,
      } as any)

      // Need to set up context with actor and env
      const testApp = new Hono<{ Bindings: { DEFAULT_BUCKET?: string } }>()
      testApp.use('*', (c, next) => {
        c.set('actor' as never, 'users/user_123')
        // Set env.DEFAULT_BUCKET
        ;(c.env as any) = { DEFAULT_BUCKET: 'test-bucket' }
        return next()
      })
      testApp.route('/', app)

      const res = await testApp.request('/create', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'Test DB', description: 'A test database' }),
      })

      expect(res.status).toBe(201)
      const body = await res.json()
      expect(body).toEqual(mockDatabase)
    })

    it('returns 409 for duplicate slug', async () => {
      vi.mocked(validateCsrf).mockReturnValue({ valid: true })
      vi.mocked(getUser).mockReturnValue({ id: 'user_123', email: 'test@example.com' })
      vi.mocked(getUserDatabaseIndex).mockReturnValue({
        register: vi.fn().mockRejectedValue(new Error('Slug already exists')),
      } as any)

      const testApp = new Hono<{ Bindings: { DEFAULT_BUCKET?: string } }>()
      testApp.use('*', (c, next) => {
        c.set('actor' as never, 'users/user_123')
        ;(c.env as any) = { DEFAULT_BUCKET: 'test-bucket' }
        return next()
      })
      testApp.route('/', app)

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
      vi.mocked(getUser).mockReturnValue(null)

      const res = await app.request('/db_123', {
        method: 'GET',
      })

      expect(res.status).toBe(401)
    })

    it('returns database by ID', async () => {
      const mockDatabase = { id: 'db_123', name: 'Test DB' }

      vi.mocked(getUser).mockReturnValue({ id: 'user_123', email: 'test@example.com' })
      vi.mocked(getUserDatabaseIndex).mockReturnValue({
        get: vi.fn().mockResolvedValue(mockDatabase),
        recordAccess: vi.fn().mockResolvedValue(undefined),
      } as any)

      const res = await app.request('/db_123', {
        method: 'GET',
      })

      expect(res.status).toBe(200)
      const body = await res.json()
      expect(body).toEqual(mockDatabase)
    })

    it('returns 404 when database not found', async () => {
      vi.mocked(getUser).mockReturnValue({ id: 'user_123', email: 'test@example.com' })
      vi.mocked(getUserDatabaseIndex).mockReturnValue({
        get: vi.fn().mockResolvedValue(null),
      } as any)

      const res = await app.request('/db_nonexistent', {
        method: 'GET',
      })

      expect(res.status).toBe(404)
      const body = await res.json()
      expect(body.error).toBe('Database not found')
    })

    it('records access when database found', async () => {
      const mockRecordAccess = vi.fn().mockResolvedValue(undefined)

      vi.mocked(getUser).mockReturnValue({ id: 'user_123', email: 'test@example.com' })
      vi.mocked(getUserDatabaseIndex).mockReturnValue({
        get: vi.fn().mockResolvedValue({ id: 'db_123', name: 'Test' }),
        recordAccess: mockRecordAccess,
      } as any)

      await app.request('/db_123', { method: 'GET' })

      expect(mockRecordAccess).toHaveBeenCalledWith('db_123')
    })
  })

  describe('PATCH /:id', () => {
    it('returns 403 when CSRF validation fails', async () => {
      vi.mocked(validateCsrf).mockReturnValue({ valid: false, reason: 'Missing header' })

      const res = await app.request('/db_123', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'New Name' }),
      })

      expect(res.status).toBe(403)
    })

    it('returns 401 when not authenticated', async () => {
      vi.mocked(validateCsrf).mockReturnValue({ valid: true })
      vi.mocked(getUser).mockReturnValue(null)

      const res = await app.request('/db_123', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'New Name' }),
      })

      expect(res.status).toBe(401)
    })

    it('returns 400 for invalid JSON', async () => {
      vi.mocked(validateCsrf).mockReturnValue({ valid: true })
      vi.mocked(getUser).mockReturnValue({ id: 'user_123', email: 'test@example.com' })

      const res = await app.request('/db_123', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: 'not json',
      })

      expect(res.status).toBe(400)
    })

    it('returns 404 when database not found', async () => {
      vi.mocked(validateCsrf).mockReturnValue({ valid: true })
      vi.mocked(getUser).mockReturnValue({ id: 'user_123', email: 'test@example.com' })
      vi.mocked(getUserDatabaseIndex).mockReturnValue({
        get: vi.fn().mockResolvedValue(null),
      } as any)

      const res = await app.request('/db_123', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'New Name' }),
      })

      expect(res.status).toBe(404)
    })

    it('updates database', async () => {
      const mockUpdate = vi.fn().mockResolvedValue({ id: 'db_123', name: 'New Name' })

      vi.mocked(validateCsrf).mockReturnValue({ valid: true })
      vi.mocked(getUser).mockReturnValue({ id: 'user_123', email: 'test@example.com' })
      vi.mocked(getUserDatabaseIndex).mockReturnValue({
        get: vi.fn().mockResolvedValue({ id: 'db_123', name: 'Old Name' }),
        update: mockUpdate,
      } as any)

      const res = await app.request('/db_123', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'New Name' }),
      })

      expect(res.status).toBe(200)
      expect(mockUpdate).toHaveBeenCalledWith('db_123', { name: 'New Name' })
    })

    it('handles update options correctly', async () => {
      const mockUpdate = vi.fn().mockResolvedValue({ id: 'db_123' })

      vi.mocked(validateCsrf).mockReturnValue({ valid: true })
      vi.mocked(getUser).mockReturnValue({ id: 'user_123', email: 'test@example.com' })
      vi.mocked(getUserDatabaseIndex).mockReturnValue({
        get: vi.fn().mockResolvedValue({ id: 'db_123' }),
        update: mockUpdate,
      } as any)

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

      expect(mockUpdate).toHaveBeenCalledWith('db_123', {
        name: 'Test',
        description: 'Desc',
        visibility: 'public',
        slug: undefined,
        stats: { entityCount: 100 },
        metadata: { custom: 'data' },
      })
    })

    it('returns 409 for duplicate slug', async () => {
      vi.mocked(validateCsrf).mockReturnValue({ valid: true })
      vi.mocked(getUser).mockReturnValue({ id: 'user_123', email: 'test@example.com' })
      vi.mocked(getUserDatabaseIndex).mockReturnValue({
        get: vi.fn().mockResolvedValue({ id: 'db_123' }),
        update: vi.fn().mockRejectedValue(new Error('Slug already exists')),
      } as any)

      const res = await app.request('/db_123', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ slug: 'taken-slug' }),
      })

      expect(res.status).toBe(409)
    })

    it('returns 400 for invalid visibility', async () => {
      vi.mocked(validateCsrf).mockReturnValue({ valid: true })
      vi.mocked(getUser).mockReturnValue({ id: 'user_123', email: 'test@example.com' })
      vi.mocked(getUserDatabaseIndex).mockReturnValue({
        get: vi.fn().mockResolvedValue({ id: 'db_123' }),
        update: vi.fn().mockRejectedValue(new Error('Invalid visibility value')),
      } as any)

      const res = await app.request('/db_123', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ visibility: 'invalid' }),
      })

      expect(res.status).toBe(400)
    })

    it('returns 400 for invalid slug', async () => {
      vi.mocked(validateCsrf).mockReturnValue({ valid: true })
      vi.mocked(getUser).mockReturnValue({ id: 'user_123', email: 'test@example.com' })
      vi.mocked(getUserDatabaseIndex).mockReturnValue({
        get: vi.fn().mockResolvedValue({ id: 'db_123' }),
        update: vi.fn().mockRejectedValue(new Error('Invalid slug: must be lowercase alphanumeric')),
      } as any)

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
      vi.mocked(validateCsrf).mockReturnValue({ valid: true })
      vi.mocked(getUser).mockReturnValue({ id: 'user_123', email: 'test@example.com' })
      vi.mocked(getUserDatabaseIndex).mockReturnValue({
        get: vi.fn().mockResolvedValue({ id: 'db_123' }),
        update: vi.fn().mockResolvedValue(null),
      } as any)

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
      vi.mocked(validateCsrf).mockReturnValue({ valid: true })
      vi.mocked(getUser).mockReturnValue({ id: 'user_123', email: 'test@example.com' })
      vi.mocked(getUserDatabaseIndex).mockReturnValue({
        get: vi.fn().mockResolvedValue({ id: 'db_123' }),
        update: vi.fn().mockRejectedValue(new Error('Unexpected database error')),
      } as any)

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
      vi.mocked(validateCsrf).mockReturnValue({ valid: true })
      vi.mocked(getUser).mockReturnValue({ id: 'user_123', email: 'test@example.com' })
      vi.mocked(getUserDatabaseIndex).mockReturnValue({
        get: vi.fn().mockResolvedValue({ id: 'db_123' }),
        update: vi.fn().mockRejectedValue('not an error object'),
      } as any)

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
      vi.mocked(validateCsrf).mockReturnValue({ valid: false, reason: 'Missing header' })

      const res = await app.request('/db_123', {
        method: 'DELETE',
      })

      expect(res.status).toBe(403)
    })

    it('returns 401 when not authenticated', async () => {
      vi.mocked(validateCsrf).mockReturnValue({ valid: true })
      vi.mocked(getUser).mockReturnValue(null)

      const res = await app.request('/db_123', {
        method: 'DELETE',
      })

      expect(res.status).toBe(401)
    })

    it('returns 404 when database not found', async () => {
      vi.mocked(validateCsrf).mockReturnValue({ valid: true })
      vi.mocked(getUser).mockReturnValue({ id: 'user_123', email: 'test@example.com' })
      vi.mocked(getUserDatabaseIndex).mockReturnValue({
        get: vi.fn().mockResolvedValue(null),
      } as any)

      const res = await app.request('/db_123', {
        method: 'DELETE',
      })

      expect(res.status).toBe(404)
    })

    it('deletes database', async () => {
      vi.mocked(validateCsrf).mockReturnValue({ valid: true })
      vi.mocked(getUser).mockReturnValue({ id: 'user_123', email: 'test@example.com' })
      vi.mocked(getUserDatabaseIndex).mockReturnValue({
        get: vi.fn().mockResolvedValue({ id: 'db_123' }),
        unregister: vi.fn().mockResolvedValue(true),
      } as any)

      const res = await app.request('/db_123', {
        method: 'DELETE',
      })

      expect(res.status).toBe(200)
      const body = await res.json()
      expect(body).toEqual({ deleted: true })
    })

    it('returns 404 when unregister returns false', async () => {
      vi.mocked(validateCsrf).mockReturnValue({ valid: true })
      vi.mocked(getUser).mockReturnValue({ id: 'user_123', email: 'test@example.com' })
      vi.mocked(getUserDatabaseIndex).mockReturnValue({
        get: vi.fn().mockResolvedValue({ id: 'db_123' }),
        unregister: vi.fn().mockResolvedValue(false),
      } as any)

      const res = await app.request('/db_123', {
        method: 'DELETE',
      })

      expect(res.status).toBe(404)
    })

    it('returns 500 on error', async () => {
      vi.mocked(validateCsrf).mockReturnValue({ valid: true })
      vi.mocked(getUser).mockReturnValue({ id: 'user_123', email: 'test@example.com' })
      vi.mocked(getUserDatabaseIndex).mockReturnValue({
        get: vi.fn().mockResolvedValue({ id: 'db_123' }),
        unregister: vi.fn().mockRejectedValue(new Error('Database connection error')),
      } as any)

      const res = await app.request('/db_123', {
        method: 'DELETE',
      })

      expect(res.status).toBe(500)
      const body = await res.json()
      expect(body.error).toBe('Database connection error')
    })

    it('returns 500 with generic message for non-Error exceptions', async () => {
      vi.mocked(validateCsrf).mockReturnValue({ valid: true })
      vi.mocked(getUser).mockReturnValue({ id: 'user_123', email: 'test@example.com' })
      vi.mocked(getUserDatabaseIndex).mockReturnValue({
        get: vi.fn().mockResolvedValue({ id: 'db_123' }),
        unregister: vi.fn().mockRejectedValue('string error'),
      } as any)

      const res = await app.request('/db_123', {
        method: 'DELETE',
      })

      expect(res.status).toBe(500)
      const body = await res.json()
      expect(body.error).toBe('Failed to delete database')
    })
  })

  describe('requireAuthUser', () => {
    it('returns user when authenticated', () => {
      const mockUser = { id: 'user_123', email: 'test@example.com' }
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
