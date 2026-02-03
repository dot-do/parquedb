/**
 * Product Lookup Snippet
 *
 * Fast product lookup from a pre-indexed Parquet file.
 * Uses a JSON index for O(1) lookups instead of scanning.
 *
 * Setup:
 * 1. Upload products.parquet to static assets
 * 2. Upload products-index.json alongside it
 * 3. Deploy this snippet
 *
 * Index format (products-index.json):
 * {
 *   "byId": { "prod_001": 0, "prod_002": 1, ... },
 *   "bySku": { "SKU-ABC": 0, "SKU-DEF": 1, ... }
 * }
 */

import {
  parseFooter,
  readRows,
  arrayBufferToAsyncBuffer,
  type ParquetIndex,
} from '../../lib/parquet-tiny'

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

    // Support both /products and /search/products paths
    const normalizedPath = path.replace(/^\/search/, '')

    // Parse path: /products/:id or /products/sku/:sku
    if (normalizedPath.startsWith('/products/sku/')) {
      const sku = normalizedPath.slice('/products/sku/'.length)
      return lookupProduct(env, 'bySku', sku)
    }

    if (normalizedPath.startsWith('/products/')) {
      const id = normalizedPath.slice('/products/'.length)
      return lookupProduct(env, 'byId', id)
    }

    return new Response('Not Found. Try /search/products/:id or /search/products/sku/:sku', { status: 404 })
  },
}

// =============================================================================
// Product Lookup
// =============================================================================

/**
 * Look up a product by ID or SKU
 */
async function lookupProduct(
  env: Env,
  indexField: 'byId' | 'bySku',
  value: string
): Promise<Response> {
  try {
    // Load index (cached by Cloudflare)
    const indexResponse = await env.ASSETS.fetch(
      'https://example.com/products-index.json'
    )
    if (!indexResponse.ok) {
      return new Response('Index not found', { status: 500 })
    }
    const index: ParquetIndex = await indexResponse.json()

    // Look up row number
    const fieldIndex = index[indexField]
    if (!fieldIndex) {
      return new Response('Index field not found', { status: 500 })
    }

    const rowNumber = fieldIndex[value]
    if (rowNumber === undefined) {
      return new Response('Product not found', { status: 404 })
    }

    // Load Parquet file
    const parquetResponse = await env.ASSETS.fetch(
      'https://example.com/products.parquet'
    )
    if (!parquetResponse.ok) {
      return new Response('Data file not found', { status: 500 })
    }

    const parquetBuffer = await parquetResponse.arrayBuffer()
    const asyncBuffer = arrayBufferToAsyncBuffer(parquetBuffer)

    // Parse footer
    const footer = await parseFooter(asyncBuffer)

    // Find the row group containing our row
    let currentRow = 0
    for (const rowGroup of footer.rowGroups) {
      if (typeof rowNumber === 'number' && rowNumber < currentRow + rowGroup.numRows) {
        // Read this row group
        const rows = await readRows(asyncBuffer, {
          ...footer,
          rowGroups: [rowGroup],
        })

        const localIdx = rowNumber - currentRow
        const product = rows[localIdx] as unknown as Product | undefined

        if (!product) {
          return new Response('Product not found', { status: 404 })
        }

        return Response.json(product, {
          headers: {
            'Cache-Control': 'public, max-age=3600',
            'Content-Type': 'application/json',
          },
        })
      }
      currentRow += rowGroup.numRows
    }

    return new Response('Product not found', { status: 404 })
  } catch (error) {
    console.error('Lookup error:', error)
    return new Response('Internal error', { status: 500 })
  }
}

// =============================================================================
// Index Generator (Run offline)
// =============================================================================

/**
 * Generate index from products data
 *
 * Run this offline to create products-index.json:
 *
 * ```typescript
 * import { generateIndex } from './snippet'
 *
 * const products = [...] // Load from Parquet or JSON
 * const index = generateIndex(products)
 * await Deno.writeTextFile('products-index.json', JSON.stringify(index))
 * ```
 */
export function generateIndex(products: Product[]): ParquetIndex {
  const index: ParquetIndex = {
    byId: {},
    bySku: {},
  }

  products.forEach((product, rowNumber) => {
    index.byId![product.id] = rowNumber
    index.bySku![product.sku] = rowNumber
  })

  return index
}
