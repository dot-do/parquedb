/**
 * Migration Handler Tests
 *
 * Tests for /migrate* route handlers.
 * Tests DO delegation, path mapping, and error handling.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { handleMigration } from '../../../../src/worker/handlers/migration'

describe('Migration Handler', () => {
  let mockStub: Record<string, ReturnType<typeof vi.fn>>

  function createMigrationCtx(
    urlStr: string,
    method = 'GET',
    overrides: Record<string, unknown> = {}
  ) {
    const url = new URL(urlStr)
    mockStub = {
      fetch: vi.fn().mockResolvedValue(new Response('migration-ok', { status: 200 })),
    }

    return {
      request: new Request(urlStr, { method }),
      url,
      path: url.pathname,
      baseUrl: `${url.protocol}//${url.host}`,
      startTime: performance.now(),
      env: {
        MIGRATION: {
          idFromName: vi.fn().mockReturnValue('mock-migration-id'),
          get: vi.fn().mockReturnValue(mockStub),
        },
        ...overrides,
      },
      params: {},
    } as any
  }

  beforeEach(() => {
    vi.clearAllMocks()
  })

  // ===========================================================================
  // DO Availability
  // ===========================================================================

  describe('DO availability', () => {
    it('should return 500 when MIGRATION DO is not available', async () => {
      const ctx = createMigrationCtx(
        'https://api.parquedb.com/migrate',
        'GET',
        { MIGRATION: undefined }
      )

      const response = await handleMigration(ctx)

      expect(response.status).toBe(500)
    })
  })

  // ===========================================================================
  // Path Mapping
  // ===========================================================================

  describe('path mapping', () => {
    it('should map GET /migrate to /status on DO', async () => {
      const ctx = createMigrationCtx(
        'https://api.parquedb.com/migrate',
        'GET'
      )

      await handleMigration(ctx)

      expect(mockStub.fetch).toHaveBeenCalled()
      const fetchedRequest = mockStub.fetch.mock.calls[0]![0] as Request
      const fetchedUrl = new URL(fetchedRequest.url)
      expect(fetchedUrl.pathname).toBe('/status')
    })

    it('should map POST /migrate to /migrate on DO', async () => {
      const ctx = createMigrationCtx(
        'https://api.parquedb.com/migrate',
        'POST'
      )

      await handleMigration(ctx)

      expect(mockStub.fetch).toHaveBeenCalled()
      const fetchedRequest = mockStub.fetch.mock.calls[0]![0] as Request
      const fetchedUrl = new URL(fetchedRequest.url)
      expect(fetchedUrl.pathname).toBe('/migrate')
    })

    it('should map GET /migrate/status to /status on DO', async () => {
      const ctx = createMigrationCtx(
        'https://api.parquedb.com/migrate/status',
        'GET'
      )

      await handleMigration(ctx)

      const fetchedRequest = mockStub.fetch.mock.calls[0]![0] as Request
      const fetchedUrl = new URL(fetchedRequest.url)
      expect(fetchedUrl.pathname).toBe('/status')
    })

    it('should map POST /migrate/cancel to /cancel on DO', async () => {
      const ctx = createMigrationCtx(
        'https://api.parquedb.com/migrate/cancel',
        'POST'
      )

      await handleMigration(ctx)

      const fetchedRequest = mockStub.fetch.mock.calls[0]![0] as Request
      const fetchedUrl = new URL(fetchedRequest.url)
      expect(fetchedUrl.pathname).toBe('/cancel')
    })

    it('should map GET /migrate/jobs to /jobs on DO', async () => {
      const ctx = createMigrationCtx(
        'https://api.parquedb.com/migrate/jobs',
        'GET'
      )

      await handleMigration(ctx)

      const fetchedRequest = mockStub.fetch.mock.calls[0]![0] as Request
      const fetchedUrl = new URL(fetchedRequest.url)
      expect(fetchedUrl.pathname).toBe('/jobs')
    })
  })

  // ===========================================================================
  // Request Forwarding
  // ===========================================================================

  describe('request forwarding', () => {
    it('should forward request method to DO', async () => {
      const ctx = createMigrationCtx(
        'https://api.parquedb.com/migrate',
        'POST'
      )

      await handleMigration(ctx)

      const fetchedRequest = mockStub.fetch.mock.calls[0]![0] as Request
      expect(fetchedRequest.method).toBe('POST')
    })

    it('should forward request headers to DO', async () => {
      const ctx = createMigrationCtx(
        'https://api.parquedb.com/migrate',
        'GET'
      )
      ctx.request = new Request('https://api.parquedb.com/migrate', {
        method: 'GET',
        headers: { 'X-Custom': 'test-value' },
      })

      await handleMigration(ctx)

      const fetchedRequest = mockStub.fetch.mock.calls[0]![0] as Request
      expect(fetchedRequest.headers.get('X-Custom')).toBe('test-value')
    })

    it('should use default migration DO name', async () => {
      const ctx = createMigrationCtx(
        'https://api.parquedb.com/migrate',
        'GET'
      )

      await handleMigration(ctx)

      expect(ctx.env.MIGRATION.idFromName).toHaveBeenCalledWith('default')
    })

    it('should return the DO response', async () => {
      const ctx = createMigrationCtx(
        'https://api.parquedb.com/migrate',
        'GET'
      )

      const response = await handleMigration(ctx)

      const text = await response.text()
      expect(text).toBe('migration-ok')
    })
  })
})
