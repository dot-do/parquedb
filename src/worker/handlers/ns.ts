/**
 * Legacy /ns Routes Handler
 *
 * Provides backwards compatibility for /ns/:namespace routes.
 */

import { buildResponse, buildErrorResponse } from '../responses'
import { parseQueryFilter, parseQueryOptions } from '../routing'
import type { EntityRecord } from '../../types/entity'
import type { Update } from '../../types/update'
import type { HandlerContext } from './types'

/**
 * Handle /ns/:namespace or /ns/:namespace/:id routes
 */
export async function handleNsRoute(
  context: HandlerContext,
  ns: string,
  id?: string
): Promise<Response> {
  const { request, url, baseUrl, path, startTime, worker } = context

  switch (request.method) {
    case 'GET': {
      if (id) {
        const entity = await worker.get(ns, id)
        if (!entity) {
          return buildErrorResponse(request, new Error(`Entity not found`), 404, startTime)
        }
        return buildResponse(request, {
          api: { resource: 'entity', namespace: ns, id },
          links: {
            self: `${baseUrl}${path}`,
            collection: `${baseUrl}/ns/${ns}`,
            home: baseUrl,
          },
          data: entity,
        }, startTime)
      } else {
        const filter = parseQueryFilter(url.searchParams)
        const options = parseQueryOptions(url.searchParams)
        const result = await worker.find<EntityRecord>(ns, filter, options)
        return buildResponse(request, {
          api: { resource: 'collection', namespace: ns },
          links: {
            self: `${baseUrl}${path}`,
            home: baseUrl,
          },
          items: result.items,
          stats: result.stats as unknown as Record<string, unknown>,
        }, startTime)
      }
    }

    case 'POST': {
      const data = (await request.json()) as Partial<EntityRecord>
      const entity = await worker.create(ns, data)
      return Response.json(entity, { status: 201 })
    }

    case 'PATCH': {
      if (!id) {
        return buildErrorResponse(request, new Error('ID required for update'), 400, startTime)
      }
      const updateData = (await request.json()) as Update
      const result = await worker.update(ns, id, updateData)
      return Response.json(result)
    }

    case 'DELETE': {
      if (!id) {
        return buildErrorResponse(request, new Error('ID required for delete'), 400, startTime)
      }
      const result = await worker.delete(ns, id)
      return Response.json(result)
    }

    default:
      return buildErrorResponse(request, new Error('Method not allowed'), 405, startTime)
  }
}
