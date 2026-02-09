/**
 * Migration Route Handlers
 *
 * Handles /migrate* routes for backend migration operations.
 * Uses Workers RPC to call MigrationDO methods directly.
 */

import type { RouteHandlerContext } from '../route-registry'
import { buildErrorResponse } from '../responses'
import type { MigrationDO } from '../MigrationDO'

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
  const stub = env.MIGRATION.get(id) as unknown as MigrationDO

  // Map paths to Migration DO RPC methods
  let migrationPath = path.replace('/migrate', '')
  if (migrationPath === '' && request.method === 'GET') {
    migrationPath = '/status'
  } else if (migrationPath === '') {
    migrationPath = '/migrate'
  }

  try {
    let data: unknown

    switch (migrationPath) {
      case '/migrate': {
        if (request.method !== 'POST') break
        const body = await request.json()
        data = await stub.startMigration(body as Parameters<MigrationDO['startMigration']>[0])
        return new Response(JSON.stringify(data), {
          status: 202,
          headers: { 'Content-Type': 'application/json' },
        })
      }
      case '/status':
        data = await stub.getMigrationStatus()
        return new Response(JSON.stringify(data), {
          headers: { 'Content-Type': 'application/json' },
        })
      case '/cancel': {
        if (request.method !== 'POST') break
        data = await stub.cancelMigration()
        return new Response(JSON.stringify(data), {
          headers: { 'Content-Type': 'application/json' },
        })
      }
      case '/jobs':
        data = await stub.getMigrationJobs()
        return new Response(JSON.stringify(data), {
          headers: { 'Content-Type': 'application/json' },
        })
    }

    return new Response('Not Found', { status: 404 })
  } catch (err) {
    return buildErrorResponse(request, err instanceof Error ? err : new Error(String(err)), 500, startTime)
  }
}
