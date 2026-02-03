/**
 * Migration Route Handlers
 *
 * Handles /migrate* routes for backend migration operations.
 */

import type { RouteHandlerContext } from '../route-registry'
import { buildErrorResponse } from '../responses'

/**
 * Handle /migrate routes - Backend migration operations
 *
 * Routes:
 * - POST /migrate - Start migration { to: 'iceberg'|'delta', namespaces?: string[] }
 * - GET /migrate/status or GET /migrate - Get current migration status
 * - POST /migrate/cancel - Cancel running migration
 * - GET /migrate/jobs - List migration history
 */
export async function handleMigration(ctx: RouteHandlerContext): Promise<Response> {
  const { request, path, env, startTime } = ctx

  if (!env.MIGRATION) {
    return buildErrorResponse(request, new Error('Migration DO not available'), 500, startTime)
  }

  const id = env.MIGRATION.idFromName('default')
  const stub = env.MIGRATION.get(id)

  // Map paths to Migration DO endpoints:
  // /migrate -> /migrate (POST starts migration)
  // /migrate/status -> /status
  // /migrate/cancel -> /cancel
  // /migrate/jobs -> /jobs
  let migrationPath = path.replace('/migrate', '')
  if (migrationPath === '' && request.method === 'GET') {
    migrationPath = '/status'
  } else if (migrationPath === '') {
    migrationPath = '/migrate'
  }

  const migrationUrl = new URL(request.url)
  migrationUrl.pathname = migrationPath

  return stub.fetch(new Request(migrationUrl.toString(), {
    method: request.method,
    headers: request.headers,
    body: request.body,
  }))
}
