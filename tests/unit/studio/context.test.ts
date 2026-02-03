/**
 * Tests for src/studio/context.ts
 *
 * Tests database context management, cookie utilities, and middleware.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  parseCookies,
  buildSetCookie,
  buildClearCookie,
  getDatabaseContext,
  getCookieDatabaseId,
  setDatabaseContext,
  clearDatabaseContext,
  databaseContextMiddleware,
  requireDatabaseContext,
  autoSelectDatabase,
  PAYLOAD_DATABASE_COOKIE,
  DEFAULT_COOKIE_MAX_AGE,
  type DatabaseContextConfig,
  type DatabaseContextData,
} from '../../../src/studio/context'
import type { DatabaseInfo } from '../../../src/worker/DatabaseIndexDO'

describe('context', () => {
  describe('parseCookies', () => {
    it('parses single cookie', () => {
      const result = parseCookies('foo=bar')

      expect(result).toEqual({ foo: 'bar' })
    })

    it('parses multiple cookies', () => {
      const result = parseCookies('foo=bar; baz=qux; name=value')

      expect(result).toEqual({
        foo: 'bar',
        baz: 'qux',
        name: 'value',
      })
    })

    it('handles cookies with spaces around delimiters', () => {
      const result = parseCookies('foo = bar ; baz = qux')

      expect(result).toEqual({
        foo: 'bar',
        baz: 'qux',
      })
    })

    it('decodes URI-encoded values', () => {
      const result = parseCookies('name=John%20Doe; city=New%20York')

      expect(result).toEqual({
        name: 'John Doe',
        city: 'New York',
      })
    })

    it('removes quotes from quoted values', () => {
      const result = parseCookies('foo="bar"; baz="qux"')

      expect(result).toEqual({
        foo: 'bar',
        baz: 'qux',
      })
    })

    it('returns empty object for empty string', () => {
      const result = parseCookies('')

      expect(result).toEqual({})
    })

    it('handles malformed cookie gracefully', () => {
      const result = parseCookies('foo=bar; invalid; baz=qux')

      expect(result).toEqual({
        foo: 'bar',
        baz: 'qux',
      })
    })

    it('handles cookies with equals in value', () => {
      const result = parseCookies('token=abc=def=ghi')

      expect(result).toEqual({
        token: 'abc=def=ghi',
      })
    })

    it('handles cookie with PAYLOAD_DATABASE_COOKIE name', () => {
      const result = parseCookies(`${PAYLOAD_DATABASE_COOKIE}=db_123`)

      expect(result[PAYLOAD_DATABASE_COOKIE]).toBe('db_123')
    })

    it('uses raw value when decoding fails', () => {
      // Invalid percent encoding
      const result = parseCookies('foo=%ZZ')

      expect(result.foo).toBe('%ZZ')
    })
  })

  describe('buildSetCookie', () => {
    it('builds basic cookie string', () => {
      const result = buildSetCookie('foo', 'bar')

      expect(result).toBe('foo=bar')
    })

    it('encodes special characters in value', () => {
      const result = buildSetCookie('name', 'John Doe')

      expect(result).toBe('name=John%20Doe')
    })

    it('adds Max-Age option', () => {
      const result = buildSetCookie('foo', 'bar', { maxAge: 86400 })

      expect(result).toContain('Max-Age=86400')
    })

    it('adds Domain option', () => {
      const result = buildSetCookie('foo', 'bar', { domain: 'example.com' })

      expect(result).toContain('Domain=example.com')
    })

    it('adds Path option', () => {
      const result = buildSetCookie('foo', 'bar', { path: '/admin' })

      expect(result).toContain('Path=/admin')
    })

    it('adds HttpOnly flag', () => {
      const result = buildSetCookie('foo', 'bar', { httpOnly: true })

      expect(result).toContain('HttpOnly')
    })

    it('adds Secure flag', () => {
      const result = buildSetCookie('foo', 'bar', { secure: true })

      expect(result).toContain('Secure')
    })

    it('adds SameSite attribute with capitalized value', () => {
      expect(buildSetCookie('foo', 'bar', { sameSite: 'lax' })).toContain('SameSite=Lax')
      expect(buildSetCookie('foo', 'bar', { sameSite: 'strict' })).toContain('SameSite=Strict')
      expect(buildSetCookie('foo', 'bar', { sameSite: 'none' })).toContain('SameSite=None')
    })

    it('builds complete cookie with all options', () => {
      const result = buildSetCookie('session', 'abc123', {
        maxAge: 3600,
        domain: 'example.com',
        path: '/',
        httpOnly: true,
        secure: true,
        sameSite: 'strict',
      })

      expect(result).toBe(
        'session=abc123; Max-Age=3600; Domain=example.com; Path=/; HttpOnly; Secure; SameSite=Strict'
      )
    })
  })

  describe('buildClearCookie', () => {
    it('builds cookie with Max-Age=0', () => {
      const result = buildClearCookie('foo')

      expect(result).toContain('foo=')
      expect(result).toContain('Max-Age=0')
    })

    it('preserves path when clearing', () => {
      const result = buildClearCookie('foo', { path: '/admin' })

      expect(result).toContain('Path=/admin')
    })

    it('preserves domain when clearing', () => {
      const result = buildClearCookie('foo', { domain: 'example.com' })

      expect(result).toContain('Domain=example.com')
    })
  })

  describe('getDatabaseContext', () => {
    it('returns null when no context set', () => {
      const mockContext = {
        var: {},
      } as unknown as Parameters<typeof getDatabaseContext>[0]

      const result = getDatabaseContext(mockContext)

      expect(result).toBeNull()
    })

    it('returns context when set', () => {
      const mockDatabaseContext = {
        databaseId: 'db_123',
        database: { id: 'db_123', name: 'Test DB' },
        storage: {},
        basePath: '/admin/db_123',
      }

      const mockContext = {
        var: {
          databaseContext: mockDatabaseContext,
        },
      } as unknown as Parameters<typeof getDatabaseContext>[0]

      const result = getDatabaseContext(mockContext)

      expect(result).toBe(mockDatabaseContext)
    })
  })

  describe('getCookieDatabaseId', () => {
    it('returns database ID from cookie header', () => {
      const mockContext = {
        var: {},
        req: {
          header: vi.fn().mockReturnValue(`${PAYLOAD_DATABASE_COOKIE}=db_abc123`),
        },
      } as unknown as Parameters<typeof getCookieDatabaseId>[0]

      const result = getCookieDatabaseId(mockContext)

      expect(result).toBe('db_abc123')
    })

    it('returns null when no cookie header', () => {
      const mockContext = {
        var: {},
        req: {
          header: vi.fn().mockReturnValue(undefined),
        },
      } as unknown as Parameters<typeof getCookieDatabaseId>[0]

      const result = getCookieDatabaseId(mockContext)

      expect(result).toBeNull()
    })

    it('returns null when cookie not present', () => {
      const mockContext = {
        var: {},
        req: {
          header: vi.fn().mockReturnValue('other=value'),
        },
      } as unknown as Parameters<typeof getCookieDatabaseId>[0]

      const result = getCookieDatabaseId(mockContext)

      expect(result).toBeNull()
    })

    it('uses custom cookie name', () => {
      const mockContext = {
        var: {},
        req: {
          header: vi.fn().mockReturnValue('MY_DB_COOKIE=db_custom'),
        },
      } as unknown as Parameters<typeof getCookieDatabaseId>[0]

      const result = getCookieDatabaseId(mockContext, 'MY_DB_COOKIE')

      expect(result).toBe('db_custom')
    })

    it('returns cached value from var if available', () => {
      const mockContext = {
        var: {
          cookieDatabaseId: 'db_cached',
        },
        req: {
          header: vi.fn().mockReturnValue(`${PAYLOAD_DATABASE_COOKIE}=db_fresh`),
        },
      } as unknown as Parameters<typeof getCookieDatabaseId>[0]

      const result = getCookieDatabaseId(mockContext)

      expect(result).toBe('db_cached')
      expect(mockContext.req.header).not.toHaveBeenCalled()
    })
  })

  describe('setDatabaseContext', () => {
    it('creates response with Set-Cookie header', () => {
      const mockContext = {
        req: {
          header: vi.fn().mockReturnValue(null),
          url: 'http://localhost/admin',
        },
      } as unknown as Parameters<typeof setDatabaseContext>[0]

      const response = setDatabaseContext(mockContext, 'db_123')

      expect(response.status).toBe(200)
      expect(response.headers.get('Set-Cookie')).toContain(PAYLOAD_DATABASE_COOKIE)
      expect(response.headers.get('Set-Cookie')).toContain('db_123')
    })

    it('creates redirect response when redirectTo provided', () => {
      const mockContext = {
        req: {
          header: vi.fn().mockReturnValue(null),
          url: 'http://localhost/admin',
        },
      } as unknown as Parameters<typeof setDatabaseContext>[0]

      const response = setDatabaseContext(mockContext, 'db_123', '/admin/db_123')

      expect(response.status).toBe(302)
      expect(response.headers.get('Location')).toBe('/admin/db_123')
      expect(response.headers.get('Set-Cookie')).toContain('db_123')
    })

    it('uses default max age', () => {
      const mockContext = {
        req: {
          header: vi.fn().mockReturnValue(null),
          url: 'http://localhost/admin',
        },
      } as unknown as Parameters<typeof setDatabaseContext>[0]

      const response = setDatabaseContext(mockContext, 'db_123')

      expect(response.headers.get('Set-Cookie')).toContain(`Max-Age=${DEFAULT_COOKIE_MAX_AGE}`)
    })

    it('uses custom config options', () => {
      const mockContext = {
        req: {
          header: vi.fn().mockReturnValue(null),
          url: 'http://localhost/admin',
        },
      } as unknown as Parameters<typeof setDatabaseContext>[0]

      const response = setDatabaseContext(mockContext, 'db_123', undefined, {
        cookieName: 'MY_DB',
        cookieMaxAge: 3600,
        cookiePath: '/app',
        cookieDomain: 'example.com',
      })

      const cookie = response.headers.get('Set-Cookie')
      expect(cookie).toContain('MY_DB=db_123')
      expect(cookie).toContain('Max-Age=3600')
      expect(cookie).toContain('Path=/app')
      expect(cookie).toContain('Domain=example.com')
    })

    it('sets secure cookie for HTTPS', () => {
      const mockContext = {
        req: {
          header: vi.fn().mockReturnValue('https'),
          url: 'https://example.com/admin',
        },
      } as unknown as Parameters<typeof setDatabaseContext>[0]

      const response = setDatabaseContext(mockContext, 'db_123')

      expect(response.headers.get('Set-Cookie')).toContain('Secure')
    })

    it('returns JSON body for non-redirect response', async () => {
      const mockContext = {
        req: {
          header: vi.fn().mockReturnValue(null),
          url: 'http://localhost/admin',
        },
      } as unknown as Parameters<typeof setDatabaseContext>[0]

      const response = setDatabaseContext(mockContext, 'db_123')
      const body = await response.json()

      expect(body).toEqual({ success: true, databaseId: 'db_123' })
    })
  })

  describe('clearDatabaseContext', () => {
    it('creates response that clears cookie', () => {
      const mockContext = {} as Parameters<typeof clearDatabaseContext>[0]

      const response = clearDatabaseContext(mockContext)

      const cookie = response.headers.get('Set-Cookie')
      expect(cookie).toContain(`${PAYLOAD_DATABASE_COOKIE}=`)
      expect(cookie).toContain('Max-Age=0')
    })

    it('creates redirect response when redirectTo provided', () => {
      const mockContext = {} as Parameters<typeof clearDatabaseContext>[0]

      const response = clearDatabaseContext(mockContext, '/admin')

      expect(response.status).toBe(302)
      expect(response.headers.get('Location')).toBe('/admin')
    })

    it('uses custom config options', () => {
      const mockContext = {} as Parameters<typeof clearDatabaseContext>[0]

      const response = clearDatabaseContext(mockContext, undefined, {
        cookieName: 'MY_DB',
        cookiePath: '/app',
        cookieDomain: 'example.com',
      })

      const cookie = response.headers.get('Set-Cookie')
      expect(cookie).toContain('MY_DB=')
      expect(cookie).toContain('Path=/app')
      expect(cookie).toContain('Domain=example.com')
    })

    it('returns JSON body for non-redirect response', async () => {
      const mockContext = {} as Parameters<typeof clearDatabaseContext>[0]

      const response = clearDatabaseContext(mockContext)
      const body = await response.json()

      expect(body).toEqual({ success: true })
    })
  })

  describe('constants', () => {
    it('has correct cookie name', () => {
      expect(PAYLOAD_DATABASE_COOKIE).toBe('PAYLOAD_DATABASE')
    })

    it('has correct default max age (30 days)', () => {
      expect(DEFAULT_COOKIE_MAX_AGE).toBe(30 * 24 * 60 * 60)
    })
  })

  describe('databaseContextMiddleware', () => {
    const mockDatabase: DatabaseInfo = {
      id: 'db_123',
      name: 'Test Database',
      slug: 'test-db',
      owner: 'user_abc',
      bucket: 'test-bucket',
      prefix: 'test/',
      createdAt: new Date(),
      updatedAt: new Date(),
    }

    const mockStorage = { type: 'mock' }

    function createMockConfig(overrides: Partial<DatabaseContextConfig> = {}): DatabaseContextConfig {
      return {
        getStorage: vi.fn().mockResolvedValue(mockStorage),
        getDatabaseIndex: vi.fn().mockResolvedValue({
          get: vi.fn().mockResolvedValue(mockDatabase),
          getBySlug: vi.fn().mockResolvedValue(mockDatabase),
          recordAccess: vi.fn().mockResolvedValue(undefined),
        }),
        ...overrides,
      }
    }

    function createMockContext(overrides: {
      databaseId?: string
      userId?: string
      cookieHeader?: string
    } = {}) {
      const vars: Record<string, unknown> = {}
      if (overrides.userId) {
        vars.user = { id: overrides.userId }
      }

      return {
        req: {
          header: vi.fn().mockImplementation((name: string) => {
            if (name === 'Cookie') return overrides.cookieHeader
            return undefined
          }),
          param: vi.fn().mockImplementation((name: string) => {
            if (name === 'databaseId') return overrides.databaseId
            return undefined
          }),
        },
        var: vars,
        set: vi.fn().mockImplementation((key: string, value: unknown) => {
          vars[key] = value
        }),
      }
    }

    it('sets cookieDatabaseId from cookie header', async () => {
      const config = createMockConfig()
      const middleware = databaseContextMiddleware(config)
      const next = vi.fn().mockResolvedValue(undefined)
      const ctx = createMockContext({ cookieHeader: `${PAYLOAD_DATABASE_COOKIE}=db_456` })

      await middleware(ctx as any, next)

      expect(ctx.set).toHaveBeenCalledWith('cookieDatabaseId', 'db_456')
    })

    it('sets cookieDatabaseId to null when no cookie', async () => {
      const config = createMockConfig()
      const middleware = databaseContextMiddleware(config)
      const next = vi.fn().mockResolvedValue(undefined)
      const ctx = createMockContext()

      await middleware(ctx as any, next)

      expect(ctx.set).toHaveBeenCalledWith('cookieDatabaseId', null)
    })

    it('sets databaseContext to null when no databaseId in path', async () => {
      const config = createMockConfig()
      const middleware = databaseContextMiddleware(config)
      const next = vi.fn().mockResolvedValue(undefined)
      const ctx = createMockContext({ userId: 'user_abc' })

      await middleware(ctx as any, next)

      expect(ctx.set).toHaveBeenCalledWith('databaseContext', null)
      expect(next).toHaveBeenCalled()
    })

    it('sets databaseContext to null when no user', async () => {
      const config = createMockConfig()
      const middleware = databaseContextMiddleware(config)
      const next = vi.fn().mockResolvedValue(undefined)
      const ctx = createMockContext({ databaseId: 'db_123' })

      await middleware(ctx as any, next)

      expect(ctx.set).toHaveBeenCalledWith('databaseContext', null)
      expect(next).toHaveBeenCalled()
    })

    it('resolves database and sets full context', async () => {
      const config = createMockConfig()
      const middleware = databaseContextMiddleware(config)
      const next = vi.fn().mockResolvedValue(undefined)
      const ctx = createMockContext({ databaseId: 'db_123', userId: 'user_abc' })

      await middleware(ctx as any, next)

      expect(ctx.set).toHaveBeenCalledWith('databaseContext', expect.objectContaining({
        databaseId: 'db_123',
        database: mockDatabase,
        storage: mockStorage,
        basePath: '/admin/db_123',
      }))
      expect(next).toHaveBeenCalled()
    })

    it('uses custom path prefix', async () => {
      const config = createMockConfig({ pathPrefix: '/dashboard' })
      const middleware = databaseContextMiddleware(config)
      const next = vi.fn().mockResolvedValue(undefined)
      const ctx = createMockContext({ databaseId: 'db_123', userId: 'user_abc' })

      await middleware(ctx as any, next)

      expect(ctx.set).toHaveBeenCalledWith('databaseContext', expect.objectContaining({
        basePath: '/dashboard/db_123',
      }))
    })

    it('uses custom cookie name', async () => {
      const config = createMockConfig({ cookieName: 'CUSTOM_DB' })
      const middleware = databaseContextMiddleware(config)
      const next = vi.fn().mockResolvedValue(undefined)
      const ctx = createMockContext({ cookieHeader: 'CUSTOM_DB=db_custom' })

      await middleware(ctx as any, next)

      expect(ctx.set).toHaveBeenCalledWith('cookieDatabaseId', 'db_custom')
    })

    it('sets databaseContext to null when database not found', async () => {
      const config = createMockConfig()
      const index = await config.getDatabaseIndex('user_abc')
      vi.mocked(index.get).mockResolvedValue(null)

      const middleware = databaseContextMiddleware(config)
      const next = vi.fn().mockResolvedValue(undefined)
      const ctx = createMockContext({ databaseId: 'db_notfound', userId: 'user_abc' })

      await middleware(ctx as any, next)

      expect(ctx.set).toHaveBeenCalledWith('databaseContext', null)
      expect(next).toHaveBeenCalled()
    })

    it('handles errors and sets databaseContext to null', async () => {
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
      const config = createMockConfig({
        getDatabaseIndex: vi.fn().mockRejectedValue(new Error('Database error')),
      })
      const middleware = databaseContextMiddleware(config)
      const next = vi.fn().mockResolvedValue(undefined)
      const ctx = createMockContext({ databaseId: 'db_123', userId: 'user_abc' })

      await middleware(ctx as any, next)

      expect(ctx.set).toHaveBeenCalledWith('databaseContext', null)
      expect(next).toHaveBeenCalled()
      expect(consoleSpy).toHaveBeenCalled()
      consoleSpy.mockRestore()
    })
  })

  describe('requireDatabaseContext', () => {
    function createMockContext(overrides: {
      databaseContext?: DatabaseContextData | null
      databaseId?: string
    } = {}) {
      const vars: Record<string, unknown> = {
        databaseContext: overrides.databaseContext ?? null,
      }

      return {
        req: {
          param: vi.fn().mockImplementation((name: string) => {
            if (name === 'databaseId') return overrides.databaseId
            return undefined
          }),
        },
        var: vars,
        redirect: vi.fn().mockImplementation((url: string) => {
          return new Response(null, {
            status: 302,
            headers: { Location: url },
          })
        }),
        html: vi.fn().mockImplementation((html: string, status?: number) => {
          return new Response(html, {
            status: status ?? 200,
            headers: { 'Content-Type': 'text/html' },
          })
        }),
      }
    }

    it('continues when valid context exists', async () => {
      const mockContext: DatabaseContextData = {
        databaseId: 'db_123',
        database: { id: 'db_123', name: 'Test' } as any,
        storage: {} as any,
        basePath: '/admin/db_123',
      }
      const middleware = requireDatabaseContext()
      const next = vi.fn().mockResolvedValue(undefined)
      const ctx = createMockContext({ databaseContext: mockContext })

      await middleware(ctx as any, next)

      expect(next).toHaveBeenCalled()
    })

    it('redirects to /admin when no context', async () => {
      const middleware = requireDatabaseContext()
      const next = vi.fn()
      const ctx = createMockContext()

      const response = await middleware(ctx as any, next)

      expect(next).not.toHaveBeenCalled()
      expect(ctx.redirect).toHaveBeenCalledWith('/admin')
    })

    it('redirects to custom URL', async () => {
      const middleware = requireDatabaseContext({ redirectTo: '/login' })
      const next = vi.fn()
      const ctx = createMockContext()

      await middleware(ctx as any, next)

      expect(ctx.redirect).toHaveBeenCalledWith('/login')
    })

    it('shows error page for invalid database when showErrorPage is true', async () => {
      const middleware = requireDatabaseContext({ showErrorPage: true })
      const next = vi.fn()
      const ctx = createMockContext({ databaseId: 'db_invalid' })

      await middleware(ctx as any, next)

      expect(ctx.html).toHaveBeenCalled()
      const [html, status] = ctx.html.mock.calls[0]
      expect(status).toBe(404)
      expect(html).toContain('db_invalid')
      expect(html).toContain('not found')
    })

    it('uses custom pathPrefix for error page', async () => {
      const middleware = requireDatabaseContext({ pathPrefix: '/dashboard', showErrorPage: true })
      const next = vi.fn()
      const ctx = createMockContext({ databaseId: 'db_invalid' })

      await middleware(ctx as any, next)

      expect(ctx.html).toHaveBeenCalled()
      const [html] = ctx.html.mock.calls[0]
      expect(html).toContain('/dashboard')
    })

    it('calls onMissing handler when provided', async () => {
      const onMissing = vi.fn().mockReturnValue(new Response('Custom error', { status: 403 }))
      const middleware = requireDatabaseContext({ onMissing })
      const next = vi.fn()
      const ctx = createMockContext({ databaseId: 'db_invalid' })

      const response = await middleware(ctx as any, next)

      expect(onMissing).toHaveBeenCalledWith(ctx, 'db_invalid')
      expect(next).not.toHaveBeenCalled()
    })

    it('passes null databaseId to onMissing when no databaseId in path', async () => {
      const onMissing = vi.fn().mockReturnValue(new Response('Custom error', { status: 403 }))
      const middleware = requireDatabaseContext({ onMissing })
      const next = vi.fn()
      const ctx = createMockContext()

      await middleware(ctx as any, next)

      expect(onMissing).toHaveBeenCalledWith(ctx, null)
    })

    it('does not show error page when showErrorPage is false', async () => {
      const middleware = requireDatabaseContext({ showErrorPage: false })
      const next = vi.fn()
      const ctx = createMockContext({ databaseId: 'db_invalid' })

      await middleware(ctx as any, next)

      expect(ctx.redirect).toHaveBeenCalledWith('/admin')
      expect(ctx.html).not.toHaveBeenCalled()
    })
  })

  describe('autoSelectDatabase', () => {
    function createMockContext(overrides: {
      path?: string
      cookieHeader?: string
      cookieDatabaseId?: string
    } = {}) {
      const vars: Record<string, unknown> = {}
      if (overrides.cookieDatabaseId !== undefined) {
        vars.cookieDatabaseId = overrides.cookieDatabaseId
      }

      return {
        req: {
          path: overrides.path ?? '/admin',
          header: vi.fn().mockImplementation((name: string) => {
            if (name === 'Cookie') return overrides.cookieHeader
            return undefined
          }),
        },
        var: vars,
        redirect: vi.fn().mockImplementation((url: string) => {
          return new Response(null, {
            status: 302,
            headers: { Location: url },
          })
        }),
      }
    }

    it('continues when path has databaseId', async () => {
      const middleware = autoSelectDatabase()
      const next = vi.fn().mockResolvedValue(undefined)
      const ctx = createMockContext({ path: '/admin/db_123' })

      await middleware(ctx as any, next)

      expect(next).toHaveBeenCalled()
      expect(ctx.redirect).not.toHaveBeenCalled()
    })

    it('redirects to last database from cookie', async () => {
      const middleware = autoSelectDatabase()
      const next = vi.fn()
      const ctx = createMockContext({
        path: '/admin',
        cookieHeader: `${PAYLOAD_DATABASE_COOKIE}=db_last`,
      })

      await middleware(ctx as any, next)

      expect(ctx.redirect).toHaveBeenCalledWith('/admin/db_last')
      expect(next).not.toHaveBeenCalled()
    })

    it('uses custom path prefix for redirect', async () => {
      const middleware = autoSelectDatabase({ pathPrefix: '/dashboard' })
      const next = vi.fn()
      const ctx = createMockContext({
        path: '/dashboard',
        cookieHeader: `${PAYLOAD_DATABASE_COOKIE}=db_last`,
      })

      await middleware(ctx as any, next)

      expect(ctx.redirect).toHaveBeenCalledWith('/dashboard/db_last')
    })

    it('uses custom cookie name', async () => {
      const middleware = autoSelectDatabase({ cookieName: 'CUSTOM_DB' })
      const next = vi.fn()
      const ctx = createMockContext({
        path: '/admin',
        cookieHeader: 'CUSTOM_DB=db_custom',
      })

      await middleware(ctx as any, next)

      expect(ctx.redirect).toHaveBeenCalledWith('/admin/db_custom')
    })

    it('redirects to defaultPath when no cookie', async () => {
      const middleware = autoSelectDatabase({ defaultPath: '/admin/select' })
      const next = vi.fn()
      const ctx = createMockContext({ path: '/admin' })

      await middleware(ctx as any, next)

      expect(ctx.redirect).toHaveBeenCalledWith('/admin/select')
    })

    it('continues to next when no cookie and no defaultPath', async () => {
      const middleware = autoSelectDatabase()
      const next = vi.fn().mockResolvedValue(undefined)
      const ctx = createMockContext({ path: '/admin' })

      await middleware(ctx as any, next)

      expect(next).toHaveBeenCalled()
      expect(ctx.redirect).not.toHaveBeenCalled()
    })

    it('uses cached cookieDatabaseId from context var', async () => {
      const middleware = autoSelectDatabase()
      const next = vi.fn()
      const ctx = createMockContext({
        path: '/admin',
        cookieDatabaseId: 'db_cached',
      })

      await middleware(ctx as any, next)

      expect(ctx.redirect).toHaveBeenCalledWith('/admin/db_cached')
    })

    it('handles path with trailing content after prefix', async () => {
      const middleware = autoSelectDatabase()
      const next = vi.fn().mockResolvedValue(undefined)
      // Path like /admin/db_123/collections should continue (has databaseId)
      const ctx = createMockContext({ path: '/admin/db_123/collections' })

      await middleware(ctx as any, next)

      expect(next).toHaveBeenCalled()
    })
  })
})
