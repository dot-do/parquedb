/**
 * E-commerce Dataset Query Patterns
 *
 * 10 real-world query patterns for benchmarking an E-commerce dataset.
 * Designed to work with both:
 * - HTTP patterns for deployed Worker (parquedb.workers.do)
 * - Local patterns for FsBackend testing (small dataset)
 *
 * Based on BENCHMARK-DESIGN.md specifications.
 */

import type { Filter } from '../../../src/types'

// =============================================================================
// E-commerce Data Types
// =============================================================================

/**
 * Product entity
 */
export interface Product {
  $id: string
  $type: 'Product'
  name: string
  sku: string
  description: string
  price: number
  currency: string
  category: string
  subcategory?: string
  categoryPath?: string[] // Full hierarchy: ['Electronics', 'Computers', 'Laptops']
  stock: number
  active: boolean
  rating: number
  reviewCount: number
  brand?: string
  tags?: string[]
  attributes?: Record<string, unknown>
  createdAt: string
  updatedAt: string
}

/**
 * Category entity with hierarchy support
 */
export interface Category {
  $id: string
  $type: 'Category'
  name: string
  slug: string
  description?: string
  parentId?: string // Reference to parent category
  level: number // 0 = root, 1 = first child, etc.
  path: string[] // Full path from root: ['electronics', 'computers']
  productCount: number
  featured: boolean
  sortOrder: number
  createdAt: string
}

/**
 * Customer entity
 */
export interface Customer {
  $id: string
  $type: 'Customer'
  name: string
  email: string
  phone?: string
  tier: 'standard' | 'premium' | 'vip'
  totalOrders: number
  totalSpent: number
  createdAt: string
  lastOrderAt?: string
  addresses?: CustomerAddress[]
}

/**
 * Customer address
 */
export interface CustomerAddress {
  type: 'billing' | 'shipping'
  street: string
  city: string
  state: string
  zip: string
  country: string
  isDefault: boolean
}

/**
 * Order entity
 */
export interface Order {
  $id: string
  $type: 'Order'
  name: string
  orderNumber: string
  customerId: string
  status: 'pending' | 'processing' | 'shipped' | 'delivered' | 'cancelled' | 'refunded'
  items: OrderItem[]
  subtotal: number
  tax: number
  shipping: number
  total: number
  currency: string
  paymentMethod: string
  shippingAddress: CustomerAddress
  billingAddress: CustomerAddress
  createdAt: string
  updatedAt: string
  shippedAt?: string
  deliveredAt?: string
}

/**
 * Order line item
 */
export interface OrderItem {
  productId: string
  productName: string
  sku: string
  quantity: number
  unitPrice: number
  total: number
}

/**
 * Inventory record for tracking stock
 */
export interface InventoryRecord {
  $id: string
  $type: 'InventoryRecord'
  productId: string
  sku: string
  warehouse: string
  quantity: number
  reserved: number
  available: number
  reorderPoint: number
  lastRestockedAt?: string
  updatedAt: string
}

// =============================================================================
// Query Pattern Types
// =============================================================================

/**
 * Query pattern category
 */
export type PatternCategory =
  | 'point-lookup'
  | 'filtered'
  | 'relationship'
  | 'fts'
  | 'aggregation'
  | 'compound'
  | 'hierarchy'
  | 'range'
  | 'top-n'
  | 'equality'

/**
 * Query pattern definition
 */
export interface QueryPattern {
  /** Pattern name */
  name: string
  /** Pattern description */
  description: string
  /** Category of query pattern */
  category: PatternCategory
  /** Target latency in milliseconds */
  targetMs: number
  /** HTTP query function for deployed Worker */
  query: (baseUrl?: string) => Promise<Response>
  /** Local query function for FsBackend testing */
  localQuery?: (db: LocalQueryExecutor) => Promise<unknown[]>
  /** Expected result shape for validation */
  expectedShape?: {
    minResults?: number
    maxResults?: number
    fields?: string[]
  }
}

/**
 * Local query executor interface for FsBackend testing
 */
export interface LocalQueryExecutor {
  /** Find entities matching filter */
  find<T>(collection: string, filter?: Filter, options?: LocalQueryOptions): Promise<T[]>
  /** Get single entity by ID */
  get<T>(collection: string, id: string): Promise<T | null>
  /** Aggregate query */
  aggregate<T>(collection: string, pipeline: AggregationStage[]): Promise<T[]>
}

/**
 * Local query options
 */
export interface LocalQueryOptions {
  limit?: number
  skip?: number
  sort?: Record<string, 1 | -1>
  project?: Record<string, 0 | 1>
}

/**
 * Aggregation pipeline stage
 */
export type AggregationStage =
  | { $match: Filter }
  | { $group: { _id: string | Record<string, string>; [key: string]: unknown } }
  | { $sort: Record<string, 1 | -1> }
  | { $limit: number }
  | { $skip: number }
  | { $project: Record<string, 0 | 1 | string | Record<string, unknown>> }
  | { $count: string }
  | { $unwind: string | { path: string; preserveNullAndEmptyArrays?: boolean } }

// =============================================================================
// Constants
// =============================================================================

const DEFAULT_BASE_URL = 'https://parquedb.workers.do'
const DATASET = 'ecommerce'

// =============================================================================
// HTTP Query Helpers
// =============================================================================

/**
 * Build URL for collection query
 */
function buildCollectionUrl(
  baseUrl: string,
  collection: string,
  params?: Record<string, string | number | boolean | string[] | undefined>
): string {
  let url = `${baseUrl}/datasets/${DATASET}/${collection}`

  if (params) {
    const searchParams = new URLSearchParams()
    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined) {
        if (Array.isArray(value)) {
          searchParams.append(key, value.join(','))
        } else {
          searchParams.append(key, String(value))
        }
      }
    }
    const queryString = searchParams.toString()
    if (queryString) {
      url += `?${queryString}`
    }
  }

  return url
}

/**
 * Build URL for entity lookup
 */
function buildEntityUrl(baseUrl: string, collection: string, id: string): string {
  return `${baseUrl}/datasets/${DATASET}/${collection}/${encodeURIComponent(id)}`
}

/**
 * Execute HTTP query
 */
async function executeHttpQuery(url: string): Promise<Response> {
  return fetch(url, {
    headers: {
      Accept: 'application/json',
      'User-Agent': 'ParqueDB-Benchmark/1.0',
    },
  })
}

// =============================================================================
// E-commerce Query Patterns
// =============================================================================

/**
 * Pattern 1: Products by category (hierarchy)
 *
 * Find all products within a category hierarchy (e.g., all products in
 * "Electronics" including subcategories like "Computers" and "Phones").
 *
 * Category: Hierarchy
 * Target: 50ms
 */
const productsByCategoryPattern: QueryPattern = {
  name: 'Products by category (hierarchy)',
  description: 'Find all products within a category and its subcategories',
  category: 'hierarchy',
  targetMs: 50,

  query: async (baseUrl = DEFAULT_BASE_URL) => {
    // Query products where categoryPath contains 'Electronics'
    const url = buildCollectionUrl(baseUrl, 'products', {
      'categoryPath.$elemMatch': 'Electronics',
      limit: 100,
    })
    return executeHttpQuery(url)
  },

  localQuery: async (db) => {
    return db.find<Product>('products', {
      categoryPath: { $elemMatch: { $eq: 'Electronics' } },
    }, { limit: 100 })
  },

  expectedShape: {
    minResults: 1,
    maxResults: 100,
    fields: ['$id', 'name', 'price', 'category', 'categoryPath'],
  },
}

/**
 * Pattern 2: Price range filter
 *
 * Find products within a specific price range.
 *
 * Category: Range
 * Target: 30ms
 */
const priceRangePattern: QueryPattern = {
  name: 'Price range filter',
  description: 'Find products within a price range ($50-$200)',
  category: 'range',
  targetMs: 30,

  query: async (baseUrl = DEFAULT_BASE_URL) => {
    const url = buildCollectionUrl(baseUrl, 'products', {
      'price.$gte': 50,
      'price.$lte': 200,
      limit: 100,
    })
    return executeHttpQuery(url)
  },

  localQuery: async (db) => {
    return db.find<Product>('products', {
      price: { $gte: 50, $lte: 200 },
    }, { limit: 100 })
  },

  expectedShape: {
    minResults: 1,
    maxResults: 100,
    fields: ['$id', 'name', 'price'],
  },
}

/**
 * Pattern 3: In-stock items only
 *
 * Find products that are currently in stock.
 *
 * Category: Equality
 * Target: 20ms
 */
const inStockPattern: QueryPattern = {
  name: 'In-stock items only',
  description: 'Find products with stock > 0 and active = true',
  category: 'equality',
  targetMs: 20,

  query: async (baseUrl = DEFAULT_BASE_URL) => {
    const url = buildCollectionUrl(baseUrl, 'products', {
      'stock.$gt': 0,
      active: true,
      limit: 100,
    })
    return executeHttpQuery(url)
  },

  localQuery: async (db) => {
    return db.find<Product>('products', {
      stock: { $gt: 0 },
      active: true,
    }, { limit: 100 })
  },

  expectedShape: {
    minResults: 1,
    maxResults: 100,
    fields: ['$id', 'name', 'stock', 'active'],
  },
}

/**
 * Pattern 4: Multi-facet filter
 *
 * Complex filtering combining multiple facets: category, price range,
 * rating, and availability.
 *
 * Category: Compound
 * Target: 50ms
 */
const multiFacetPattern: QueryPattern = {
  name: 'Multi-facet filter',
  description: 'Filter by category + price range + rating + in-stock',
  category: 'compound',
  targetMs: 50,

  query: async (baseUrl = DEFAULT_BASE_URL) => {
    const url = buildCollectionUrl(baseUrl, 'products', {
      category: 'Electronics',
      'price.$gte': 100,
      'price.$lte': 500,
      'rating.$gte': 4.0,
      'stock.$gt': 0,
      active: true,
      limit: 50,
    })
    return executeHttpQuery(url)
  },

  localQuery: async (db) => {
    return db.find<Product>('products', {
      $and: [
        { category: 'Electronics' },
        { price: { $gte: 100, $lte: 500 } },
        { rating: { $gte: 4.0 } },
        { stock: { $gt: 0 } },
        { active: true },
      ],
    }, { limit: 50 })
  },

  expectedShape: {
    minResults: 0,
    maxResults: 50,
    fields: ['$id', 'name', 'category', 'price', 'rating', 'stock'],
  },
}

/**
 * Pattern 5: Product search (FTS)
 *
 * Full-text search across product names and descriptions.
 *
 * Category: FTS
 * Target: 50ms
 */
const productSearchPattern: QueryPattern = {
  name: 'Product search',
  description: 'Full-text search for "wireless headphones"',
  category: 'fts',
  targetMs: 50,

  query: async (baseUrl = DEFAULT_BASE_URL) => {
    const url = buildCollectionUrl(baseUrl, 'products', {
      q: 'wireless headphones',
      limit: 50,
    })
    return executeHttpQuery(url)
  },

  localQuery: async (db) => {
    // Fallback to regex search for local testing
    return db.find<Product>('products', {
      $or: [
        { name: { $regex: 'wireless|headphones', $options: 'i' } },
        { description: { $regex: 'wireless|headphones', $options: 'i' } },
      ],
    }, { limit: 50 })
  },

  expectedShape: {
    minResults: 0,
    maxResults: 50,
    fields: ['$id', 'name', 'description'],
  },
}

/**
 * Pattern 6: Customer order history
 *
 * Get all orders for a specific customer, sorted by date.
 *
 * Category: Filtered
 * Target: 20ms
 */
const customerOrderHistoryPattern: QueryPattern = {
  name: 'Customer order history',
  description: 'Get all orders for a specific customer',
  category: 'filtered',
  targetMs: 20,

  query: async (baseUrl = DEFAULT_BASE_URL) => {
    // Using a sample customer ID
    const customerId = 'customers/cust-00001'
    const url = buildCollectionUrl(baseUrl, 'orders', {
      customerId,
      orderBy: 'createdAt',
      order: 'desc',
      limit: 50,
    })
    return executeHttpQuery(url)
  },

  localQuery: async (db) => {
    return db.find<Order>('orders', {
      customerId: 'customers/cust-00001',
    }, {
      limit: 50,
      sort: { createdAt: -1 },
    })
  },

  expectedShape: {
    minResults: 0,
    maxResults: 50,
    fields: ['$id', 'orderNumber', 'customerId', 'status', 'total', 'createdAt'],
  },
}

/**
 * Pattern 7: Low stock inventory
 *
 * Find products with stock below the reorder point (low stock alert).
 *
 * Category: Range
 * Target: 30ms
 */
const lowStockPattern: QueryPattern = {
  name: 'Low stock inventory',
  description: 'Find products with stock <= 10 (low stock alert)',
  category: 'range',
  targetMs: 30,

  query: async (baseUrl = DEFAULT_BASE_URL) => {
    const url = buildCollectionUrl(baseUrl, 'products', {
      'stock.$lte': 10,
      'stock.$gt': 0,
      active: true,
      orderBy: 'stock',
      order: 'asc',
      limit: 100,
    })
    return executeHttpQuery(url)
  },

  localQuery: async (db) => {
    return db.find<Product>('products', {
      stock: { $lte: 10, $gt: 0 },
      active: true,
    }, {
      limit: 100,
      sort: { stock: 1 },
    })
  },

  expectedShape: {
    minResults: 0,
    maxResults: 100,
    fields: ['$id', 'name', 'sku', 'stock'],
  },
}

/**
 * Pattern 8: Best sellers by category
 *
 * Get top-selling products within a category, sorted by review count
 * as a proxy for sales (or use an actual salesCount field if available).
 *
 * Category: Top-N
 * Target: 30ms
 */
const bestSellersPattern: QueryPattern = {
  name: 'Best sellers by category',
  description: 'Get top 20 products in Electronics by review count',
  category: 'top-n',
  targetMs: 30,

  query: async (baseUrl = DEFAULT_BASE_URL) => {
    const url = buildCollectionUrl(baseUrl, 'products', {
      category: 'Electronics',
      active: true,
      orderBy: 'reviewCount',
      order: 'desc',
      limit: 20,
    })
    return executeHttpQuery(url)
  },

  localQuery: async (db) => {
    return db.find<Product>('products', {
      category: 'Electronics',
      active: true,
    }, {
      limit: 20,
      sort: { reviewCount: -1 },
    })
  },

  expectedShape: {
    minResults: 0,
    maxResults: 20,
    fields: ['$id', 'name', 'category', 'reviewCount', 'rating'],
  },
}

/**
 * Pattern 9: Related products
 *
 * Find products related to a given product (same category, similar price
 * range, different product).
 *
 * Category: Compound
 * Target: 30ms
 */
const relatedProductsPattern: QueryPattern = {
  name: 'Related products',
  description: 'Find products similar to a given product (same category, similar price)',
  category: 'compound',
  targetMs: 30,

  query: async (baseUrl = DEFAULT_BASE_URL) => {
    // Assuming the reference product is in Electronics, price ~$150
    const url = buildCollectionUrl(baseUrl, 'products', {
      category: 'Electronics',
      'price.$gte': 100,
      'price.$lte': 200,
      active: true,
      '$id.$ne': 'products/prod-00001', // Exclude the reference product
      limit: 10,
    })
    return executeHttpQuery(url)
  },

  localQuery: async (db) => {
    // In a real implementation, you'd first fetch the reference product
    // then query for similar products
    return db.find<Product>('products', {
      $and: [
        { category: 'Electronics' },
        { price: { $gte: 100, $lte: 200 } },
        { active: true },
        { $id: { $ne: 'products/prod-00001' } },
      ],
    }, { limit: 10 })
  },

  expectedShape: {
    minResults: 0,
    maxResults: 10,
    fields: ['$id', 'name', 'category', 'price'],
  },
}

/**
 * Pattern 10: Order analytics (date range)
 *
 * Aggregate order data over a date range to compute revenue, order count,
 * and average order value.
 *
 * Category: Aggregation
 * Target: 200ms
 */
const orderAnalyticsPattern: QueryPattern = {
  name: 'Order analytics (date range)',
  description: 'Compute revenue, order count, and AOV for last 30 days',
  category: 'aggregation',
  targetMs: 200,

  query: async (baseUrl = DEFAULT_BASE_URL) => {
    // Calculate date range (last 30 days)
    const endDate = new Date()
    const startDate = new Date()
    startDate.setDate(startDate.getDate() - 30)

    const url = buildCollectionUrl(baseUrl, 'orders', {
      'createdAt.$gte': startDate.toISOString(),
      'createdAt.$lte': endDate.toISOString(),
      status: 'delivered',
      aggregate: 'sum:total,count:$id,avg:total',
      groupBy: 'status',
    })
    return executeHttpQuery(url)
  },

  localQuery: async (db) => {
    const endDate = new Date()
    const startDate = new Date()
    startDate.setDate(startDate.getDate() - 30)

    return db.aggregate<{ _id: string; totalRevenue: number; orderCount: number; avgOrderValue: number }>('orders', [
      {
        $match: {
          createdAt: {
            $gte: startDate.toISOString(),
            $lte: endDate.toISOString(),
          },
          status: 'delivered',
        },
      },
      {
        $group: {
          _id: '$status',
          totalRevenue: { $sum: '$total' },
          orderCount: { $sum: 1 },
          avgOrderValue: { $avg: '$total' },
        },
      },
    ])
  },

  expectedShape: {
    minResults: 0,
    maxResults: 10,
    fields: ['_id', 'totalRevenue', 'orderCount', 'avgOrderValue'],
  },
}

// =============================================================================
// Export All Patterns
// =============================================================================

/**
 * All E-commerce query patterns
 *
 * 10 real-world patterns covering:
 * 1. Products by category (hierarchy) - Hierarchy - 50ms
 * 2. Price range filter - Range - 30ms
 * 3. In-stock items only - Equality - 20ms
 * 4. Multi-facet filter - Compound - 50ms
 * 5. Product search - FTS - 50ms
 * 6. Customer order history - Filtered - 20ms
 * 7. Low stock inventory - Range - 30ms
 * 8. Best sellers by category - Top-N - 30ms
 * 9. Related products - Compound - 30ms
 * 10. Order analytics (date range) - Aggregation - 200ms
 */
export const ecommercePatterns: QueryPattern[] = [
  productsByCategoryPattern,
  priceRangePattern,
  inStockPattern,
  multiFacetPattern,
  productSearchPattern,
  customerOrderHistoryPattern,
  lowStockPattern,
  bestSellersPattern,
  relatedProductsPattern,
  orderAnalyticsPattern,
]

// =============================================================================
// Pattern Lookup Helpers
// =============================================================================

/**
 * Get pattern by name
 */
export function getPatternByName(name: string): QueryPattern | undefined {
  return ecommercePatterns.find((p) => p.name === name)
}

/**
 * Get patterns by category
 */
export function getPatternsByCategory(category: PatternCategory): QueryPattern[] {
  return ecommercePatterns.filter((p) => p.category === category)
}

/**
 * Get patterns within target latency
 */
export function getPatternsWithinTarget(maxMs: number): QueryPattern[] {
  return ecommercePatterns.filter((p) => p.targetMs <= maxMs)
}

// =============================================================================
// Benchmark Execution Helpers
// =============================================================================

/**
 * Result from running a query pattern
 */
export interface PatternResult {
  pattern: QueryPattern
  success: boolean
  latencyMs: number
  withinTarget: boolean
  statusCode?: number
  rowCount?: number
  error?: string
}

/**
 * Run a single pattern and measure performance
 */
export async function runPattern(
  pattern: QueryPattern,
  baseUrl: string = DEFAULT_BASE_URL
): Promise<PatternResult> {
  const start = performance.now()

  try {
    const response = await pattern.query(baseUrl)
    const latencyMs = performance.now() - start

    if (!response.ok) {
      return {
        pattern,
        success: false,
        latencyMs,
        withinTarget: false,
        statusCode: response.status,
        error: `HTTP ${response.status}: ${response.statusText}`,
      }
    }

    const data = await response.json() as { data?: unknown[]; total?: number }
    const rowCount = Array.isArray(data.data) ? data.data.length : (data.total ?? 1)

    return {
      pattern,
      success: true,
      latencyMs,
      withinTarget: latencyMs <= pattern.targetMs,
      statusCode: response.status,
      rowCount,
    }
  } catch (error) {
    const latencyMs = performance.now() - start
    return {
      pattern,
      success: false,
      latencyMs,
      withinTarget: false,
      error: error instanceof Error ? error.message : String(error),
    }
  }
}

/**
 * Run all patterns and collect results
 */
export async function runAllPatterns(
  baseUrl: string = DEFAULT_BASE_URL,
  options?: { warmupIterations?: number; iterations?: number }
): Promise<{
  results: PatternResult[]
  summary: {
    total: number
    passed: number
    failed: number
    withinTarget: number
    avgLatencyMs: number
  }
}> {
  const warmup = options?.warmupIterations ?? 2
  const iterations = options?.iterations ?? 1

  const results: PatternResult[] = []

  for (const pattern of ecommercePatterns) {
    // Warmup
    for (let i = 0; i < warmup; i++) {
      try {
        await pattern.query(baseUrl)
      } catch {
        // Ignore warmup errors
      }
    }

    // Run iterations and take best result
    const iterResults: PatternResult[] = []
    for (let i = 0; i < iterations; i++) {
      const result = await runPattern(pattern, baseUrl)
      iterResults.push(result)
    }

    // Use best latency from successful runs, or first result if all failed
    const successfulRuns = iterResults.filter((r) => r.success)
    const bestResult = successfulRuns.length > 0
      ? successfulRuns.reduce((best, r) => (r.latencyMs < best.latencyMs ? r : best))
      : iterResults[0]!

    results.push(bestResult)
  }

  const successful = results.filter((r) => r.success)
  const avgLatency = successful.length > 0
    ? successful.reduce((sum, r) => sum + r.latencyMs, 0) / successful.length
    : 0

  return {
    results,
    summary: {
      total: results.length,
      passed: successful.length,
      failed: results.length - successful.length,
      withinTarget: results.filter((r) => r.withinTarget).length,
      avgLatencyMs: Math.round(avgLatency * 100) / 100,
    },
  }
}

// =============================================================================
// Sample Data Generation (for local testing)
// =============================================================================

/**
 * Generate sample product data for local testing
 */
export function generateSampleProducts(count: number): Partial<Product>[] {
  const categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books']
  const subcategories: Record<string, string[]> = {
    Electronics: ['Computers', 'Phones', 'Audio', 'Cameras'],
    Clothing: ['Mens', 'Womens', 'Kids', 'Shoes'],
    'Home & Garden': ['Furniture', 'Kitchen', 'Garden', 'Decor'],
    Sports: ['Fitness', 'Outdoor', 'Team Sports', 'Water Sports'],
    Books: ['Fiction', 'Non-Fiction', 'Technical', 'Childrens'],
  }

  const products: Partial<Product>[] = []

  for (let i = 0; i < count; i++) {
    const category = categories[i % categories.length]!
    const subs = subcategories[category]!
    const subcategory = subs[i % subs.length]!

    products.push({
      name: `Product ${i + 1}`,
      sku: `SKU-${String(i + 1).padStart(6, '0')}`,
      description: `Description for product ${i + 1}`,
      price: Math.round((10 + Math.random() * 990) * 100) / 100,
      currency: 'USD',
      category,
      subcategory,
      categoryPath: [category, subcategory],
      stock: Math.floor(Math.random() * 100),
      active: Math.random() > 0.1,
      rating: Math.round((3 + Math.random() * 2) * 10) / 10,
      reviewCount: Math.floor(Math.random() * 500),
      brand: `Brand ${(i % 20) + 1}`,
      tags: ['tag1', 'tag2'].slice(0, Math.floor(Math.random() * 3)),
      createdAt: new Date(Date.now() - Math.random() * 365 * 24 * 60 * 60 * 1000).toISOString(),
      updatedAt: new Date().toISOString(),
    })
  }

  return products
}

/**
 * Generate sample order data for local testing
 */
export function generateSampleOrders(count: number, customerCount: number): Partial<Order>[] {
  const statuses: Order['status'][] = ['pending', 'processing', 'shipped', 'delivered', 'cancelled']
  const orders: Partial<Order>[] = []

  for (let i = 0; i < count; i++) {
    const itemCount = 1 + Math.floor(Math.random() * 5)
    const subtotal = Math.round((20 + Math.random() * 480) * 100) / 100
    const tax = Math.round(subtotal * 0.08 * 100) / 100
    const shipping = Math.round((5 + Math.random() * 15) * 100) / 100

    orders.push({
      name: `Order ${i + 1}`,
      orderNumber: `ORD-${String(i + 1).padStart(8, '0')}`,
      customerId: `customers/cust-${String((i % customerCount) + 1).padStart(5, '0')}`,
      status: statuses[Math.floor(Math.random() * statuses.length)]!,
      items: Array.from({ length: itemCount }, (_, j) => ({
        productId: `products/prod-${String(Math.floor(Math.random() * 1000) + 1).padStart(5, '0')}`,
        productName: `Product ${j + 1}`,
        sku: `SKU-${String(j + 1).padStart(6, '0')}`,
        quantity: 1 + Math.floor(Math.random() * 3),
        unitPrice: Math.round((10 + Math.random() * 100) * 100) / 100,
        total: 0, // Calculated below
      })).map((item) => ({ ...item, total: item.quantity * item.unitPrice })),
      subtotal,
      tax,
      shipping,
      total: Math.round((subtotal + tax + shipping) * 100) / 100,
      currency: 'USD',
      paymentMethod: ['credit_card', 'paypal', 'bank_transfer'][Math.floor(Math.random() * 3)]!,
      shippingAddress: {
        type: 'shipping',
        street: `${Math.floor(Math.random() * 999) + 1} Main St`,
        city: ['New York', 'Los Angeles', 'Chicago', 'Houston'][Math.floor(Math.random() * 4)]!,
        state: ['NY', 'CA', 'IL', 'TX'][Math.floor(Math.random() * 4)]!,
        zip: String(10000 + Math.floor(Math.random() * 90000)),
        country: 'US',
        isDefault: true,
      },
      billingAddress: {
        type: 'billing',
        street: `${Math.floor(Math.random() * 999) + 1} Main St`,
        city: ['New York', 'Los Angeles', 'Chicago', 'Houston'][Math.floor(Math.random() * 4)]!,
        state: ['NY', 'CA', 'IL', 'TX'][Math.floor(Math.random() * 4)]!,
        zip: String(10000 + Math.floor(Math.random() * 90000)),
        country: 'US',
        isDefault: true,
      },
      createdAt: new Date(Date.now() - Math.random() * 90 * 24 * 60 * 60 * 1000).toISOString(),
      updatedAt: new Date().toISOString(),
    })
  }

  return orders
}

/**
 * Generate sample customer data for local testing
 */
export function generateSampleCustomers(count: number): Partial<Customer>[] {
  const tiers: Customer['tier'][] = ['standard', 'premium', 'vip']
  const customers: Partial<Customer>[] = []

  for (let i = 0; i < count; i++) {
    customers.push({
      name: `Customer ${i + 1}`,
      email: `customer${i + 1}@example.com`,
      phone: `+1-555-${String(Math.floor(Math.random() * 10000)).padStart(4, '0')}`,
      tier: tiers[Math.floor(Math.random() * tiers.length)]!,
      totalOrders: Math.floor(Math.random() * 50),
      totalSpent: Math.round(Math.random() * 5000 * 100) / 100,
      createdAt: new Date(Date.now() - Math.random() * 730 * 24 * 60 * 60 * 1000).toISOString(),
      lastOrderAt: Math.random() > 0.3
        ? new Date(Date.now() - Math.random() * 90 * 24 * 60 * 60 * 1000).toISOString()
        : undefined,
    })
  }

  return customers
}
