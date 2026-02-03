/**
 * Category Filter Snippet
 *
 * Filter products by category with optional price range.
 * Demonstrates in-memory filtering of Parquet data.
 *
 * Endpoints:
 * - GET /products?category=electronics
 * - GET /products?category=electronics&minPrice=100&maxPrice=500
 * - GET /products?inStock=true
 *
 * Setup:
 * 1. Upload products.parquet to static assets
 * 2. Deploy this snippet
 */

import {
  parseFooter,
  readRows,
  arrayBufferToAsyncBuffer,
} from '../../lib/parquet-tiny'
import { filterRows } from '../../lib/filter'
import type { Filter } from '../../lib/types'

// =============================================================================
// Types
// =============================================================================

interface Product {
  id: string
  sku: string
  name: string
  price: number
  category: string
  inStock: boolean
}

interface Env {
  ASSETS: Fetcher
}

// =============================================================================
// Handler
// =============================================================================

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url)
    const path = url.pathname

    // Support both /categories and /search/categories paths
    const normalizedPath = path.replace(/^\/search/, '')

    if (normalizedPath !== '/categories') {
      return new Response('Not Found. Try /search/categories?category=electronics', { status: 404 })
    }

    return filterProducts(request, env)
  },
}

// =============================================================================
// Product Filter
// =============================================================================

/**
 * Filter products based on query parameters
 */
async function filterProducts(request: Request, env: Env): Promise<Response> {
  try {
    const url = new URL(request.url)

    // Build filter from query parameters
    const filter = buildFilter(url.searchParams)

    // Pagination
    const limit = parseInt(url.searchParams.get('limit') ?? '100', 10)
    const offset = parseInt(url.searchParams.get('offset') ?? '0', 10)

    // Load Parquet file
    const parquetResponse = await env.ASSETS.fetch(
      'https://example.com/products.parquet'
    )
    if (!parquetResponse.ok) {
      return new Response('Data file not found', { status: 500 })
    }

    const parquetBuffer = await parquetResponse.arrayBuffer()
    const asyncBuffer = arrayBufferToAsyncBuffer(parquetBuffer)

    // Parse and read
    const footer = await parseFooter(asyncBuffer)
    const allRows = await readRows(asyncBuffer, footer)

    // Apply filter
    let filtered = filterRows(allRows, filter) as unknown as Product[]

    // Get total before pagination
    const total = filtered.length

    // Apply pagination
    filtered = filtered.slice(offset, offset + limit)

    // Response
    return Response.json(
      {
        data: filtered,
        pagination: {
          total,
          limit,
          offset,
          hasMore: offset + filtered.length < total,
        },
      },
      {
        headers: {
          'Cache-Control': 'public, max-age=300',
          'Content-Type': 'application/json',
        },
      }
    )
  } catch (error) {
    console.error('Filter error:', error)
    return new Response('Internal error', { status: 500 })
  }
}

/**
 * Build a filter from query parameters
 */
function buildFilter(params: URLSearchParams): Filter {
  const filter: Filter = {}

  // Category filter (exact match)
  const category = params.get('category')
  if (category) {
    filter.category = category
  }

  // Price range filter
  const minPrice = params.get('minPrice')
  const maxPrice = params.get('maxPrice')

  if (minPrice && maxPrice) {
    filter.price = {
      $gte: parseFloat(minPrice),
      $lte: parseFloat(maxPrice),
    } as Filter['price']
  } else if (minPrice) {
    filter.price = { $gte: parseFloat(minPrice) }
  } else if (maxPrice) {
    filter.price = { $lte: parseFloat(maxPrice) }
  }

  // In stock filter
  const inStock = params.get('inStock')
  if (inStock !== null) {
    filter.inStock = inStock === 'true'
  }

  // Search by name (contains)
  const search = params.get('search')
  if (search) {
    // Note: $contains is not in our minimal filter lib
    // For Snippets, we handle this specially
    filter.name = { $eq: search } // Simplified - exact match only
  }

  return filter
}

// =============================================================================
// Cache Key Generation
// =============================================================================

/**
 * Generate cache key from filter
 *
 * Used for caching filtered results in KV or Cache API
 */
export function generateCacheKey(params: URLSearchParams): string {
  const parts: string[] = []

  // Sort params for consistent keys
  const sortedParams = Array.from(params.entries()).sort(([a], [b]) =>
    a.localeCompare(b)
  )

  for (const [key, value] of sortedParams) {
    if (['limit', 'offset'].includes(key)) continue // Don't include pagination in cache key
    parts.push(`${key}=${value}`)
  }

  return `products:${parts.join('&') || 'all'}`
}
