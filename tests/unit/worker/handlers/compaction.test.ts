/**
 * Compaction Handler Tests
 *
 * Tests for /compaction/* route handlers.
 * Tests status queries, health checks, dashboard, and metrics endpoints.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  handleCompactionStatus,
  handleCompactionHealth,
  handleCompactionDashboard,
  handleCompactionMetrics,
  handleCompactionMetricsJson,
} from '../../../../src/worker/handlers/compaction'

// Mock the compaction queue consumer module
vi.mock('../../../../src/workflows/compaction-queue-consumer', () => ({
  evaluateNamespaceHealth: vi.fn().mockReturnValue({
    namespace: 'test',
    status: 'healthy',
    metrics: {
      activeWindows: 0,
      oldestWindowAge: 0,
      totalPendingFiles: 0,
      windowsStuckInProcessing: 0,
    },
    issues: [],
  }),
  aggregateHealthStatus: vi.fn().mockReturnValue({
    status: 'healthy',
    namespaces: {},
    summary: { total: 1, healthy: 1, degraded: 0, unhealthy: 0 },
  }),
  isCompactionStatusResponse: vi.fn().mockReturnValue(true),
}))

// Mock the observability/compaction module
vi.mock('../../../../src/observability/compaction', () => ({
  generateDashboardHtml: vi.fn().mockReturnValue('<html>dashboard</html>'),
  exportPrometheusMetrics: vi.fn().mockReturnValue('# metrics'),
  exportJsonTimeSeries: vi.fn().mockReturnValue({ series: [] }),
}))

describe('Compaction Handlers', () => {
  function createCompactionCtx(urlStr: string, overrides: Record<string, unknown> = {}) {
    const url = new URL(urlStr)
    return {
      request: new Request(urlStr),
      url,
      path: url.pathname,
      baseUrl: `${url.protocol}//${url.host}`,
      startTime: performance.now(),
      env: {
        COMPACTION_STATE: {
          idFromName: vi.fn().mockReturnValue('mock-id'),
          get: vi.fn().mockReturnValue({
            fetch: vi.fn().mockResolvedValue(
              Response.json({
                activeWindows: 2,
                knownWriters: ['w1'],
              })
            ),
          }),
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
  // handleCompactionStatus
  // ===========================================================================

  describe('handleCompactionStatus', () => {
    it('should return 500 when COMPACTION_STATE is not available', async () => {
      const ctx = createCompactionCtx(
        'https://api.parquedb.com/compaction/status',
        { COMPACTION_STATE: undefined }
      )

      const response = await handleCompactionStatus(ctx)

      expect(response.status).toBe(500)
    })

    it('should return 400 with usage instructions when no namespace specified', async () => {
      const ctx = createCompactionCtx(
        'https://api.parquedb.com/compaction/status'
      )

      const response = await handleCompactionStatus(ctx)

      expect(response.status).toBe(400)
      const body = (await response.json()) as Record<string, unknown>
      expect(body.message).toContain('sharded by namespace')
      expect(body.usage).toBeDefined()
    })

    it('should query single namespace when namespace param provided', async () => {
      const ctx = createCompactionCtx(
        'https://api.parquedb.com/compaction/status?namespace=posts'
      )

      const response = await handleCompactionStatus(ctx)

      expect(response).toBeInstanceOf(Response)
      expect(ctx.env.COMPACTION_STATE.idFromName).toHaveBeenCalledWith('posts')
    })

    it('should aggregate results for multiple namespaces', async () => {
      const ctx = createCompactionCtx(
        'https://api.parquedb.com/compaction/status?namespaces=posts,comments,users'
      )

      const response = await handleCompactionStatus(ctx)

      expect(response.status).toBe(200)
      const body = (await response.json()) as Record<string, unknown>
      expect(body.namespaces).toBeDefined()
      expect(body.summary).toBeDefined()
      const summary = body.summary as Record<string, unknown>
      expect(summary.totalNamespaces).toBe(3)
    })

    it('should return 400 for empty namespaces parameter', async () => {
      const ctx = createCompactionCtx(
        'https://api.parquedb.com/compaction/status?namespaces='
      )

      const response = await handleCompactionStatus(ctx)

      expect(response.status).toBe(400)
    })

    it('should handle DO fetch errors gracefully per namespace', async () => {
      const ctx = createCompactionCtx(
        'https://api.parquedb.com/compaction/status?namespaces=posts'
      )
      const stub = ctx.env.COMPACTION_STATE.get()
      stub.fetch.mockRejectedValue(new Error('DO unavailable'))

      const response = await handleCompactionStatus(ctx)

      expect(response.status).toBe(200)
      const body = (await response.json()) as Record<string, unknown>
      const namespaces = body.namespaces as Array<Record<string, unknown>>
      expect(namespaces[0]?.error).toContain('DO unavailable')
    })
  })

  // ===========================================================================
  // handleCompactionHealth
  // ===========================================================================

  describe('handleCompactionHealth', () => {
    it('should return 500 when COMPACTION_STATE is not available', async () => {
      const ctx = createCompactionCtx(
        'https://api.parquedb.com/compaction/health',
        { COMPACTION_STATE: undefined }
      )

      const response = await handleCompactionHealth(ctx)

      expect(response.status).toBe(500)
    })

    it('should return 400 when namespaces param is missing', async () => {
      const ctx = createCompactionCtx(
        'https://api.parquedb.com/compaction/health'
      )

      const response = await handleCompactionHealth(ctx)

      expect(response.status).toBe(400)
      const body = (await response.json()) as Record<string, unknown>
      expect(body.error).toContain('namespaces parameter is required')
    })

    it('should return 400 for empty namespaces', async () => {
      const ctx = createCompactionCtx(
        'https://api.parquedb.com/compaction/health?namespaces='
      )

      const response = await handleCompactionHealth(ctx)

      expect(response.status).toBe(400)
    })

    it('should return health status for specified namespaces', async () => {
      const ctx = createCompactionCtx(
        'https://api.parquedb.com/compaction/health?namespaces=posts,users'
      )

      const response = await handleCompactionHealth(ctx)

      expect(response.status).toBe(200)
      const body = (await response.json()) as Record<string, unknown>
      expect(body.status).toBe('healthy')
    })

    it('should accept custom threshold parameters', async () => {
      const ctx = createCompactionCtx(
        'https://api.parquedb.com/compaction/health?namespaces=posts&maxPendingWindows=5&maxWindowAgeHours=1'
      )

      const response = await handleCompactionHealth(ctx)

      expect(response.status).toBe(200)
    })
  })

  // ===========================================================================
  // handleCompactionDashboard
  // ===========================================================================

  describe('handleCompactionDashboard', () => {
    it('should return 400 when namespaces param is missing', async () => {
      const ctx = createCompactionCtx(
        'https://api.parquedb.com/compaction/dashboard'
      )

      const response = await handleCompactionDashboard(ctx)

      expect(response.status).toBe(400)
      const body = (await response.json()) as Record<string, unknown>
      expect(body.error).toContain('namespaces parameter is required')
    })

    it('should return 400 for empty namespaces', async () => {
      const ctx = createCompactionCtx(
        'https://api.parquedb.com/compaction/dashboard?namespaces='
      )

      const response = await handleCompactionDashboard(ctx)

      expect(response.status).toBe(400)
    })

    it('should return HTML dashboard when namespaces provided', async () => {
      const ctx = createCompactionCtx(
        'https://api.parquedb.com/compaction/dashboard?namespaces=posts,users'
      )

      const response = await handleCompactionDashboard(ctx)

      expect(response.status).toBe(200)
      expect(response.headers.get('Content-Type')).toContain('text/html')
      const text = await response.text()
      expect(text).toContain('dashboard')
    })
  })

  // ===========================================================================
  // handleCompactionMetrics
  // ===========================================================================

  describe('handleCompactionMetrics', () => {
    it('should return Prometheus format metrics', async () => {
      const ctx = createCompactionCtx(
        'https://api.parquedb.com/compaction/metrics'
      )

      const response = await handleCompactionMetrics(ctx)

      expect(response.status).toBe(200)
      expect(response.headers.get('Content-Type')).toContain('text/plain')
    })

    it('should pass namespaces filter when provided', async () => {
      const ctx = createCompactionCtx(
        'https://api.parquedb.com/compaction/metrics?namespaces=posts,users'
      )

      const response = await handleCompactionMetrics(ctx)

      expect(response.status).toBe(200)
    })
  })

  // ===========================================================================
  // handleCompactionMetricsJson
  // ===========================================================================

  describe('handleCompactionMetricsJson', () => {
    it('should return JSON time series data', async () => {
      const ctx = createCompactionCtx(
        'https://api.parquedb.com/compaction/metrics/json'
      )

      const response = await handleCompactionMetricsJson(ctx)

      expect(response.status).toBe(200)
      expect(response.headers.get('Content-Type')).toContain('application/json')
    })

    it('should pass namespaces, since, and limit params', async () => {
      const ctx = createCompactionCtx(
        'https://api.parquedb.com/compaction/metrics/json?namespaces=posts&since=1700000000000&limit=50'
      )

      const response = await handleCompactionMetricsJson(ctx)

      expect(response.status).toBe(200)
    })
  })
})
