/**
 * Vacuum Handler Tests
 *
 * Tests for /vacuum/* route handlers.
 * Tests workflow start, status queries, input validation, and error handling.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  handleVacuumStart,
  handleVacuumStatus,
} from '../../../../src/worker/handlers/vacuum'

describe('Vacuum Handlers', () => {
  let mockWorkflowInstance: Record<string, ReturnType<typeof vi.fn>>

  function createVacuumCtx(
    urlStr: string,
    method = 'GET',
    body?: unknown,
    overrides: Record<string, unknown> = {}
  ) {
    const url = new URL(urlStr)
    mockWorkflowInstance = {
      id: 'workflow-123',
      status: vi.fn().mockResolvedValue({ status: 'running', progress: 50 }),
    }

    const requestInit: RequestInit = { method }
    if (body) {
      requestInit.body = JSON.stringify(body)
      requestInit.headers = { 'Content-Type': 'application/json' }
    }

    return {
      request: new Request(urlStr, requestInit),
      url,
      path: url.pathname,
      baseUrl: `${url.protocol}//${url.host}`,
      startTime: performance.now(),
      env: {
        VACUUM_WORKFLOW: {
          create: vi.fn().mockResolvedValue(mockWorkflowInstance),
          get: vi.fn().mockReturnValue(mockWorkflowInstance),
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
  // handleVacuumStart
  // ===========================================================================

  describe('handleVacuumStart', () => {
    it('should return 500 when VACUUM_WORKFLOW is not available', async () => {
      const ctx = createVacuumCtx(
        'https://api.parquedb.com/vacuum/start',
        'POST',
        { namespace: 'users' },
        { VACUUM_WORKFLOW: undefined }
      )

      const response = await handleVacuumStart(ctx)

      expect(response.status).toBe(500)
    })

    it('should return 400 when namespace is missing', async () => {
      const ctx = createVacuumCtx(
        'https://api.parquedb.com/vacuum/start',
        'POST',
        {}
      )

      const response = await handleVacuumStart(ctx)

      expect(response.status).toBe(400)
    })

    it('should start vacuum workflow and return 202', async () => {
      const ctx = createVacuumCtx(
        'https://api.parquedb.com/vacuum/start',
        'POST',
        { namespace: 'users' }
      )

      const response = await handleVacuumStart(ctx)

      expect(response.status).toBe(202)
      const body = (await response.json()) as Record<string, unknown>
      expect(body.success).toBe(true)
      expect(body.workflowId).toBe('workflow-123')
      expect(body.message).toContain('users')
      expect(body.statusUrl).toContain('/vacuum/status/')
    })

    it('should pass default parameters when not provided', async () => {
      const ctx = createVacuumCtx(
        'https://api.parquedb.com/vacuum/start',
        'POST',
        { namespace: 'users' }
      )

      await handleVacuumStart(ctx)

      expect(ctx.env.VACUUM_WORKFLOW.create).toHaveBeenCalledWith({
        params: {
          namespace: 'users',
          format: 'auto',
          retentionMs: 24 * 60 * 60 * 1000, // 24h
          dryRun: false,
          warehouse: '',
          database: '',
        },
      })
    })

    it('should use provided parameters when set', async () => {
      const ctx = createVacuumCtx(
        'https://api.parquedb.com/vacuum/start',
        'POST',
        {
          namespace: 'posts',
          format: 'iceberg',
          retentionMs: 3600000,
          dryRun: true,
          warehouse: 'my-warehouse',
          database: 'my-db',
        }
      )

      await handleVacuumStart(ctx)

      expect(ctx.env.VACUUM_WORKFLOW.create).toHaveBeenCalledWith({
        params: {
          namespace: 'posts',
          format: 'iceberg',
          retentionMs: 3600000,
          dryRun: true,
          warehouse: 'my-warehouse',
          database: 'my-db',
        },
      })
    })

    it('should handle workflow creation errors', async () => {
      const ctx = createVacuumCtx(
        'https://api.parquedb.com/vacuum/start',
        'POST',
        { namespace: 'users' }
      )
      ctx.env.VACUUM_WORKFLOW.create.mockRejectedValue(new Error('Workflow limit exceeded'))

      const response = await handleVacuumStart(ctx)

      expect(response.status).toBe(500)
    })
  })

  // ===========================================================================
  // handleVacuumStatus
  // ===========================================================================

  describe('handleVacuumStatus', () => {
    it('should return 500 when VACUUM_WORKFLOW is not available', async () => {
      const ctx = createVacuumCtx(
        'https://api.parquedb.com/vacuum/status/wf-123',
        'GET',
        undefined,
        { VACUUM_WORKFLOW: undefined }
      )
      ctx.params = { id: 'wf-123' }

      const response = await handleVacuumStatus(ctx)

      expect(response.status).toBe(500)
    })

    it('should return workflow status', async () => {
      const ctx = createVacuumCtx(
        'https://api.parquedb.com/vacuum/status/wf-123',
        'GET'
      )
      ctx.params = { id: 'wf-123' }

      const response = await handleVacuumStatus(ctx)

      expect(response.status).toBe(200)
      const body = (await response.json()) as Record<string, unknown>
      expect(body.status).toBe('running')
      expect(body.progress).toBe(50)
    })

    it('should use workflow ID from params', async () => {
      const ctx = createVacuumCtx(
        'https://api.parquedb.com/vacuum/status/my-workflow-id',
        'GET'
      )
      ctx.params = { id: 'my-workflow-id' }

      await handleVacuumStatus(ctx)

      expect(ctx.env.VACUUM_WORKFLOW.get).toHaveBeenCalledWith('my-workflow-id')
    })

    it('should return 404 when workflow not found', async () => {
      const ctx = createVacuumCtx(
        'https://api.parquedb.com/vacuum/status/nonexistent',
        'GET'
      )
      ctx.params = { id: 'nonexistent' }
      ctx.env.VACUUM_WORKFLOW.get.mockImplementation(() => ({
        status: vi.fn().mockRejectedValue(new Error('Workflow not found')),
      }))

      const response = await handleVacuumStatus(ctx)

      expect(response.status).toBe(404)
    })
  })
})
