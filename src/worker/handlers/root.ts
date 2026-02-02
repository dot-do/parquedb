/**
 * Root Route Handler - API Overview
 */

import { buildResponse } from '../responses'
import type { HandlerContext } from './types'

/**
 * Handle root route (/)
 * Returns API overview and links
 */
export function handleRoot(context: HandlerContext): Response {
  const { request, baseUrl, startTime } = context

  return buildResponse(request, {
    api: {
      name: 'ParqueDB',
      version: '0.1.0',
      description: 'A hybrid relational/document/graph database built on Apache Parquet',
      documentation: 'https://github.com/parquedb/parquedb',
    },
    links: {
      self: baseUrl,
      datasets: `${baseUrl}/datasets`,
      imdb: `${baseUrl}/datasets/imdb`,
      onet: `${baseUrl}/datasets/onet`,
      health: `${baseUrl}/health`,
      benchmark: `${baseUrl}/benchmark`,
      benchmarkDatasets: `${baseUrl}/benchmark-datasets`,
      benchmarkIndexed: `${baseUrl}/benchmark-indexed`,
    },
  }, startTime)
}
