/**
 * Vacuum Route Handlers
 *
 * Handles /vacuum/* routes for orphaned file cleanup.
 */

import type { RouteHandlerContext } from '../route-registry'
import { buildErrorResponse } from '../responses'

/**
 * Request body for starting a vacuum operation
 */
interface VacuumStartRequest {
  namespace?: string | undefined
  format?: 'iceberg' | 'delta' | 'auto' | undefined
  retentionMs?: number | undefined
  dryRun?: boolean | undefined
  warehouse?: string | undefined
  database?: string | undefined
}

/**
 * Handle POST /vacuum/start - Start vacuum workflow
 */
export async function handleVacuumStart(ctx: RouteHandlerContext): Promise<Response> {
  const { request, env, startTime } = ctx

  if (!env.VACUUM_WORKFLOW) {
    return buildErrorResponse(request, new Error('Vacuum Workflow not available'), 500, startTime)
  }

  try {
    const body = await request.json() as VacuumStartRequest

    if (!body.namespace) {
      return buildErrorResponse(
        request,
        new Error('namespace is required'),
        400,
        startTime
      )
    }

    // Start vacuum workflow
    const instance = await env.VACUUM_WORKFLOW.create({
      params: {
        namespace: body.namespace,
        format: body.format ?? 'auto',
        retentionMs: body.retentionMs ?? 24 * 60 * 60 * 1000, // 24 hours default
        dryRun: body.dryRun ?? false,
        warehouse: body.warehouse ?? '',
        database: body.database ?? '',
      },
    })

    return new Response(JSON.stringify({
      success: true,
      workflowId: instance.id,
      message: `Vacuum workflow started for namespace '${body.namespace}'`,
      statusUrl: `/vacuum/status/${instance.id}`,
    }, null, 2), {
      status: 202,
      headers: { 'Content-Type': 'application/json' },
    })
  } catch (error) {
    const err = error instanceof Error ? error : new Error(String(error))
    return buildErrorResponse(request, err, 500, startTime)
  }
}

/**
 * Handle GET /vacuum/status/:id - Get vacuum workflow status
 */
export async function handleVacuumStatus(ctx: RouteHandlerContext): Promise<Response> {
  const { request, env, params, startTime } = ctx

  if (!env.VACUUM_WORKFLOW) {
    return buildErrorResponse(request, new Error('Vacuum Workflow not available'), 500, startTime)
  }

  const workflowId = params.id!
  try {
    const instance = await env.VACUUM_WORKFLOW.get(workflowId)
    const status = await instance.status()

    return new Response(JSON.stringify(status, null, 2), {
      headers: { 'Content-Type': 'application/json' },
    })
  } catch (error) {
    const err = error instanceof Error ? error : new Error(String(error))
    return buildErrorResponse(request, err, 404, startTime)
  }
}
