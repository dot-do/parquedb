/**
 * Health Check Handler
 */

import { buildResponse } from '../responses'
import type { HandlerContext } from './types'

/**
 * Handle health check route (/health)
 */
export function handleHealth(context: HandlerContext): Response {
  const { request, baseUrl, startTime } = context

  return buildResponse(request, {
    api: {
      status: 'healthy',
      uptime: 'ok',
      storage: 'r2',
      compute: 'durable-objects',
    },
    links: {
      home: baseUrl,
      datasets: `${baseUrl}/datasets`,
    },
  }, startTime)
}
