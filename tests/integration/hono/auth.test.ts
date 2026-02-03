/**
 * Hono Authentication Middleware Tests
 *
 * Tests the auth middleware, requireAuth, and helper functions.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import { Hono } from 'hono'
import type { Context } from 'hono'
import {
  auth,
  requireAuth,
  getUser,
  assertAuth,
  assertRole,
  type AuthUser,
  type AuthOptions,
} from '../../../src/integrations/hono/auth.js'

// Mock oauth.do/hono
vi.mock('oauth.do/hono', () => ({
  auth: (options: { jwksUri: string }) => {
    return async (c: Context, next: () => Promise<void>) => {
      // Mock: check for test token
      const authHeader = c.req.header('Authorization')
      if (authHeader?.startsWith('Bearer test-token-')) {
        const userId = authHeader.replace('Bearer test-token-', '')
        const mockUser: AuthUser = {
          id: userId,
          email: `${userId}@test.com`,
          roles: userId === 'admin' ? ['admin'] : ['user'],
        }
        c.set('user' as never, mockUser)
      }
      await next()
    }
  },
}))

describe('Hono Auth Middleware', () => {
  describe('auth()', () => {
    it('sets user and actor when token is valid', async () => {
      const app = new Hono()
      app.use('*', auth({ jwksUri: 'https://test.jwks.uri' }))
      app.get('/test', (c) => {
        return c.json({
          user: (c.var as any).user,
          actor: (c.var as any).actor,
        })
      })

      const res = await app.request('/test', {
        headers: { Authorization: 'Bearer test-token-user123' },
      })

      expect(res.status).toBe(200)
      const body = await res.json()
      expect(body.user).toEqual({
        id: 'user123',
        email: 'user123@test.com',
        roles: ['user'],
      })
      expect(body.actor).toBe('users/user123')
    })

    it('sets null values when no token provided', async () => {
      const app = new Hono()
      app.use('*', auth({ jwksUri: 'https://test.jwks.uri' }))
      app.get('/test', (c) => {
        return c.json({
          user: (c.var as any).user,
          actor: (c.var as any).actor,
          token: (c.var as any).token,
        })
      })

      const res = await app.request('/test')

      expect(res.status).toBe(200)
      const body = await res.json()
      expect(body.user).toBeNull()
      expect(body.actor).toBeNull()
      expect(body.token).toBeNull()
    })

    it('uses custom actorNamespace', async () => {
      const app = new Hono()
      app.use('*', auth({
        jwksUri: 'https://test.jwks.uri',
        actorNamespace: 'members',
      }))
      app.get('/test', (c) => {
        return c.json({ actor: (c.var as any).actor })
      })

      const res = await app.request('/test', {
        headers: { Authorization: 'Bearer test-token-user123' },
      })

      const body = await res.json()
      expect(body.actor).toBe('members/user123')
    })

    it('uses custom extractToken function', async () => {
      const app = new Hono()
      app.use('*', auth({
        jwksUri: 'https://test.jwks.uri',
        extractToken: (c) => c.req.header('X-Auth-Token') ?? null,
      }))
      app.get('/test', (c) => {
        return c.json({
          token: (c.var as any).token,
        })
      })

      const res = await app.request('/test', {
        headers: { 'X-Auth-Token': 'my-custom-token' },
      })

      const body = await res.json()
      // Token is extracted from custom header
      expect(body.token).toBe('my-custom-token')
    })
  })

  describe('requireAuth()', () => {
    it('allows authenticated requests', async () => {
      const app = new Hono()
      app.use('*', auth({ jwksUri: 'https://test.jwks.uri' }))
      app.get('/protected', requireAuth(), (c) => {
        return c.json({ success: true })
      })

      const res = await app.request('/protected', {
        headers: { Authorization: 'Bearer test-token-user123' },
      })

      expect(res.status).toBe(200)
      const body = await res.json()
      expect(body.success).toBe(true)
    })

    it('returns 401 for unauthenticated requests', async () => {
      const app = new Hono()
      app.use('*', auth({ jwksUri: 'https://test.jwks.uri' }))
      app.get('/protected', requireAuth(), (c) => {
        return c.json({ success: true })
      })

      const res = await app.request('/protected')

      expect(res.status).toBe(401)
      const body = await res.json()
      expect(body.error).toBe('Authentication required')
    })

    it('uses custom error message', async () => {
      const app = new Hono()
      app.use('*', auth({ jwksUri: 'https://test.jwks.uri' }))
      app.get('/protected', requireAuth({ message: 'Please log in' }), (c) => {
        return c.json({ success: true })
      })

      const res = await app.request('/protected')

      expect(res.status).toBe(401)
      const body = await res.json()
      expect(body.error).toBe('Please log in')
    })

    it('allows requests with required role', async () => {
      const app = new Hono()
      app.use('*', auth({ jwksUri: 'https://test.jwks.uri' }))
      app.get('/admin', requireAuth({ roles: ['admin'] }), (c) => {
        return c.json({ success: true })
      })

      const res = await app.request('/admin', {
        headers: { Authorization: 'Bearer test-token-admin' },
      })

      expect(res.status).toBe(200)
    })

    it('returns 403 when missing required role', async () => {
      const app = new Hono()
      app.use('*', auth({ jwksUri: 'https://test.jwks.uri' }))
      app.get('/admin', requireAuth({ roles: ['admin'] }), (c) => {
        return c.json({ success: true })
      })

      const res = await app.request('/admin', {
        headers: { Authorization: 'Bearer test-token-user123' },
      })

      expect(res.status).toBe(403)
      const body = await res.json()
      expect(body.error).toBe('Required role: admin')
    })

    it('allows any of multiple roles', async () => {
      const app = new Hono()
      app.use('*', auth({ jwksUri: 'https://test.jwks.uri' }))
      app.get('/staff', requireAuth({ roles: ['admin', 'moderator'] }), (c) => {
        return c.json({ success: true })
      })

      const res = await app.request('/staff', {
        headers: { Authorization: 'Bearer test-token-admin' },
      })

      expect(res.status).toBe(200)
    })
  })

  describe('getUser()', () => {
    it('returns user when authenticated', async () => {
      const app = new Hono()
      app.use('*', auth({ jwksUri: 'https://test.jwks.uri' }))
      app.get('/test', (c) => {
        const user = getUser(c)
        return c.json({ user })
      })

      const res = await app.request('/test', {
        headers: { Authorization: 'Bearer test-token-user123' },
      })

      const body = await res.json()
      expect(body.user).toEqual({
        id: 'user123',
        email: 'user123@test.com',
        roles: ['user'],
      })
    })

    it('returns null when not authenticated', async () => {
      const app = new Hono()
      app.use('*', auth({ jwksUri: 'https://test.jwks.uri' }))
      app.get('/test', (c) => {
        const user = getUser(c)
        return c.json({ user })
      })

      const res = await app.request('/test')

      const body = await res.json()
      expect(body.user).toBeNull()
    })
  })

  describe('assertAuth()', () => {
    it('returns user when authenticated', async () => {
      const app = new Hono()
      app.use('*', auth({ jwksUri: 'https://test.jwks.uri' }))
      app.get('/test', (c) => {
        try {
          const user = assertAuth(c)
          return c.json({ user })
        } catch (error) {
          return c.json({ error: (error as Error).message }, 401)
        }
      })

      const res = await app.request('/test', {
        headers: { Authorization: 'Bearer test-token-user123' },
      })

      expect(res.status).toBe(200)
      const body = await res.json()
      expect(body.user.id).toBe('user123')
    })

    it('throws when not authenticated', async () => {
      const app = new Hono()
      app.use('*', auth({ jwksUri: 'https://test.jwks.uri' }))
      app.get('/test', (c) => {
        try {
          const user = assertAuth(c)
          return c.json({ user })
        } catch (error) {
          return c.json({ error: (error as Error).message }, 401)
        }
      })

      const res = await app.request('/test')

      expect(res.status).toBe(401)
      const body = await res.json()
      expect(body.error).toBe('Authentication required')
    })
  })

  describe('assertRole()', () => {
    it('returns user when role matches', async () => {
      const app = new Hono()
      app.use('*', auth({ jwksUri: 'https://test.jwks.uri' }))
      app.get('/test', (c) => {
        try {
          const user = assertRole(c, 'admin')
          return c.json({ user })
        } catch (error) {
          return c.json({ error: (error as Error).message }, 403)
        }
      })

      const res = await app.request('/test', {
        headers: { Authorization: 'Bearer test-token-admin' },
      })

      expect(res.status).toBe(200)
      const body = await res.json()
      expect(body.user.roles).toContain('admin')
    })

    it('throws when role does not match', async () => {
      const app = new Hono()
      app.use('*', auth({ jwksUri: 'https://test.jwks.uri' }))
      app.get('/test', (c) => {
        try {
          const user = assertRole(c, 'admin')
          return c.json({ user })
        } catch (error) {
          return c.json({ error: (error as Error).message }, 403)
        }
      })

      const res = await app.request('/test', {
        headers: { Authorization: 'Bearer test-token-user123' },
      })

      expect(res.status).toBe(403)
      const body = await res.json()
      expect(body.error).toBe('Required role: admin')
    })

    it('throws when not authenticated', async () => {
      const app = new Hono()
      app.use('*', auth({ jwksUri: 'https://test.jwks.uri' }))
      app.get('/test', (c) => {
        try {
          const user = assertRole(c, 'admin')
          return c.json({ user })
        } catch (error) {
          return c.json({ error: (error as Error).message }, 401)
        }
      })

      const res = await app.request('/test')

      expect(res.status).toBe(401)
      const body = await res.json()
      expect(body.error).toBe('Authentication required')
    })
  })

  describe('Actor with ParqueDB operations', () => {
    it('actor format is correct for ParqueDB', async () => {
      const app = new Hono()
      app.use('*', auth({ jwksUri: 'https://test.jwks.uri' }))
      app.post('/api/create', requireAuth(), (c) => {
        const actor = (c.var as any).actor
        // Actor should be in EntityId format: namespace/id
        expect(actor).toMatch(/^users\/[a-zA-Z0-9]+$/)
        return c.json({ actor })
      })

      const res = await app.request('/api/create', {
        method: 'POST',
        headers: { Authorization: 'Bearer test-token-user123' },
      })

      expect(res.status).toBe(200)
      const body = await res.json()
      expect(body.actor).toBe('users/user123')
    })
  })
})
