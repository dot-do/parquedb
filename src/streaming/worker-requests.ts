/**
 * WorkerRequests Materialized View
 *
 * A streaming materialized view for tracking and analyzing Cloudflare Worker HTTP requests.
 * Provides real-time analytics for:
 * - Request latency tracking (p50, p95, p99)
 * - Status code distribution
 * - Geographic distribution
 * - Cache hit/miss ratios
 * - Error rate monitoring
 *
 * @example
 * ```typescript
 * import { createWorkerRequestsMV, recordRequest } from 'parquedb/streaming'
 *
 * // Create the MV
 * const mv = createWorkerRequestsMV(db, {
 *   refreshMode: 'streaming',
 *   maxStalenessMs: 1000,
 * })
 *
 * // Record requests from worker handlers
 * await recordRequest(db, {
 *   method: 'GET',
 *   path: '/api/users',
 *   status: 200,
 *   latencyMs: 45,
 *   cached: true,
 *   colo: 'SJC',
 *   country: 'US',
 * })
 *
 * // Query aggregated metrics
 * const metrics = await getRequestMetrics(db, {
 *   since: new Date(Date.now() - 3600000), // Last hour
 *   groupBy: 'path',
 * })
 * ```
 *
 * @packageDocumentation
 */

import type { ParqueDB } from '../ParqueDB'
import type { ViewDefinition, ViewOptions } from '../materialized-views/types'
import { viewName } from '../materialized-views/types'

// =============================================================================
// Constants
// =============================================================================

/** Default collection name for storing raw requests */
export const DEFAULT_REQUESTS_COLLECTION = 'worker_requests'

/** Default collection name for aggregated metrics */
export const DEFAULT_METRICS_COLLECTION = 'worker_request_metrics'

/** Default flush interval in milliseconds */
export const DEFAULT_FLUSH_INTERVAL_MS = 5000

/** Default buffer size before flush */
export const DEFAULT_BUFFER_SIZE = 100

// =============================================================================
// Request Schema Types
// =============================================================================

/**
 * HTTP method type
 */
export type HttpMethod = 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE' | 'HEAD' | 'OPTIONS'

/**
 * HTTP status category for grouping
 */
export type StatusCategory = '1xx' | '2xx' | '3xx' | '4xx' | '5xx'

/**
 * Raw worker request record
 */
export interface WorkerRequest {
  /** Unique request ID (typically cf-ray or ULID) */
  requestId: string

  /** Request timestamp */
  timestamp: Date

  /** HTTP method */
  method: HttpMethod

  /** Request path (without query string) */
  path: string

  /** HTTP status code */
  status: number

  /** Status category (1xx, 2xx, etc.) */
  statusCategory: StatusCategory

  /** Total latency in milliseconds */
  latencyMs: number

  /** Whether the response was served from cache */
  cached: boolean

  /** Cache tier that served the response (if cached) */
  cacheTier?: 'edge' | 'cdn' | 'primary'

  /** Cloudflare colo code (e.g., 'SJC', 'LHR') */
  colo?: string

  /** ISO country code */
  country?: string

  /** City name */
  city?: string

  /** Region/state code */
  region?: string

  /** User's timezone */
  timezone?: string

  /** Request content length in bytes */
  requestSize?: number

  /** Response content length in bytes */
  responseSize?: number

  /** User agent string */
  userAgent?: string

  /** Dataset being accessed (if applicable) */
  dataset?: string

  /** Collection being accessed (if applicable) */
  collection?: string

  /** Resource type being accessed */
  resourceType?: 'entity' | 'collection' | 'dataset' | 'relationship' | 'api' | 'health'

  /** Error message (if status >= 400) */
  error?: string

  /** Error code (if applicable) */
  errorCode?: string

  /** Custom metadata */
  metadata?: Record<string, unknown>
}

/**
 * Input for recording a request (subset of WorkerRequest with auto-generated fields)
 */
export interface RecordRequestInput {
  /** HTTP method */
  method: HttpMethod

  /** Request path */
  path: string

  /** HTTP status code */
  status: number

  /** Total latency in milliseconds */
  latencyMs: number

  /** Whether the response was served from cache */
  cached?: boolean

  /** Cache tier that served the response */
  cacheTier?: 'edge' | 'cdn' | 'primary'

  /** Cloudflare colo code */
  colo?: string

  /** ISO country code */
  country?: string

  /** City name */
  city?: string

  /** Region/state code */
  region?: string

  /** User's timezone */
  timezone?: string

  /** Request content length in bytes */
  requestSize?: number

  /** Response content length in bytes */
  responseSize?: number

  /** User agent string */
  userAgent?: string

  /** Dataset being accessed */
  dataset?: string

  /** Collection being accessed */
  collection?: string

  /** Resource type being accessed */
  resourceType?: 'entity' | 'collection' | 'dataset' | 'relationship' | 'api' | 'health'

  /** Error message */
  error?: string

  /** Error code */
  errorCode?: string

  /** Custom request ID (auto-generated if not provided) */
  requestId?: string

  /** Custom timestamp (defaults to now) */
  timestamp?: Date

  /** Custom metadata */
  metadata?: Record<string, unknown>
}

// =============================================================================
// Aggregated Metrics Types
// =============================================================================

/**
 * Time bucket for aggregation
 */
export type TimeBucket = 'minute' | 'hour' | 'day' | 'week' | 'month'

/**
 * Aggregated request metrics
 */
export interface RequestMetrics {
  /** Aggregation bucket start time */
  bucketStart: Date

  /** Aggregation bucket end time */
  bucketEnd: Date

  /** Time bucket size */
  timeBucket: TimeBucket

  /** Grouping key (path, colo, country, etc.) */
  groupBy?: string

  /** Group value */
  groupValue?: string

  /** Total request count */
  totalRequests: number

  /** Successful requests (2xx) */
  successCount: number

  /** Client error requests (4xx) */
  clientErrorCount: number

  /** Server error requests (5xx) */
  serverErrorCount: number

  /** Error rate (0-1) */
  errorRate: number

  /** Cache hit count */
  cacheHits: number

  /** Cache miss count */
  cacheMisses: number

  /** Cache hit ratio (0-1) */
  cacheHitRatio: number

  /** Latency statistics */
  latency: {
    /** Minimum latency */
    min: number
    /** Maximum latency */
    max: number
    /** Average latency */
    avg: number
    /** Median latency (p50) */
    p50: number
    /** 95th percentile latency */
    p95: number
    /** 99th percentile latency */
    p99: number
  }

  /** Status code breakdown */
  statusCodes: Record<number, number>

  /** Method breakdown */
  methods: Record<HttpMethod, number>

  /** Total request bytes */
  totalRequestBytes: number

  /** Total response bytes */
  totalResponseBytes: number
}

/**
 * Options for querying request metrics
 */
export interface GetMetricsOptions {
  /** Collection name for raw requests */
  collection?: string

  /** Start time for query (inclusive) */
  since?: Date

  /** End time for query (exclusive) */
  until?: Date

  /** Time bucket for aggregation */
  timeBucket?: TimeBucket

  /** Field to group by */
  groupBy?: 'path' | 'method' | 'status' | 'colo' | 'country' | 'dataset' | 'collection' | 'resourceType'

  /** Filter by specific path pattern */
  pathPattern?: string

  /** Filter by status category */
  statusCategory?: StatusCategory

  /** Filter by method */
  method?: HttpMethod

  /** Filter by colo */
  colo?: string

  /** Filter by country */
  country?: string

  /** Limit number of results */
  limit?: number

  /** Include only cached/uncached requests */
  cachedOnly?: boolean
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Get status category from status code
 */
export function getStatusCategory(status: number): StatusCategory {
  if (status < 200) return '1xx'
  if (status < 300) return '2xx'
  if (status < 400) return '3xx'
  if (status < 500) return '4xx'
  return '5xx'
}

/**
 * Generate a unique request ID
 */
export function generateRequestId(): string {
  // Use timestamp + random for a simple unique ID
  const timestamp = Date.now().toString(36)
  const random = Math.random().toString(36).substring(2, 10)
  return `req_${timestamp}_${random}`
}

/**
 * Calculate percentile from sorted array of numbers
 */
export function percentile(sortedArr: number[], p: number): number {
  if (sortedArr.length === 0) return 0
  if (sortedArr.length === 1) return sortedArr[0]!

  const index = (p / 100) * (sortedArr.length - 1)
  const lower = Math.floor(index)
  const upper = Math.ceil(index)

  if (lower === upper) return sortedArr[lower]!

  const lowerValue = sortedArr[lower]!
  const upperValue = sortedArr[upper]!
  return lowerValue + (upperValue - lowerValue) * (index - lower)
}

/**
 * Get bucket start time based on time bucket
 */
export function getBucketStart(date: Date, bucket: TimeBucket): Date {
  const d = new Date(date)

  switch (bucket) {
    case 'minute':
      d.setSeconds(0, 0)
      break
    case 'hour':
      d.setMinutes(0, 0, 0)
      break
    case 'day':
      d.setHours(0, 0, 0, 0)
      break
    case 'week': {
      const day = d.getDay()
      d.setDate(d.getDate() - day)
      d.setHours(0, 0, 0, 0)
      break
    }
    case 'month':
      d.setDate(1)
      d.setHours(0, 0, 0, 0)
      break
  }

  return d
}

/**
 * Get bucket end time based on time bucket
 */
export function getBucketEnd(date: Date, bucket: TimeBucket): Date {
  const d = getBucketStart(date, bucket)

  switch (bucket) {
    case 'minute':
      d.setMinutes(d.getMinutes() + 1)
      break
    case 'hour':
      d.setHours(d.getHours() + 1)
      break
    case 'day':
      d.setDate(d.getDate() + 1)
      break
    case 'week':
      d.setDate(d.getDate() + 7)
      break
    case 'month':
      d.setMonth(d.getMonth() + 1)
      break
  }

  return d
}

// =============================================================================
// Request Recording
// =============================================================================

/**
 * Record a single worker request
 *
 * @param db - ParqueDB instance
 * @param input - Request data to record
 * @param options - Optional configuration
 * @returns Created request record
 *
 * @example
 * ```typescript
 * await recordRequest(db, {
 *   method: 'GET',
 *   path: '/api/users/123',
 *   status: 200,
 *   latencyMs: 45,
 *   cached: true,
 *   colo: 'SJC',
 *   country: 'US',
 * })
 * ```
 */
export async function recordRequest(
  db: ParqueDB,
  input: RecordRequestInput,
  options?: {
    collection?: string
  }
): Promise<WorkerRequest> {
  const collection = options?.collection ?? DEFAULT_REQUESTS_COLLECTION

  const request: WorkerRequest = {
    requestId: input.requestId ?? generateRequestId(),
    timestamp: input.timestamp ?? new Date(),
    method: input.method,
    path: input.path,
    status: input.status,
    statusCategory: getStatusCategory(input.status),
    latencyMs: input.latencyMs,
    cached: input.cached ?? false,
    cacheTier: input.cacheTier,
    colo: input.colo,
    country: input.country,
    city: input.city,
    region: input.region,
    timezone: input.timezone,
    requestSize: input.requestSize,
    responseSize: input.responseSize,
    userAgent: input.userAgent,
    dataset: input.dataset,
    collection: input.collection,
    resourceType: input.resourceType,
    error: input.error,
    errorCode: input.errorCode,
    metadata: input.metadata,
  }

  const created = await db.collection(collection).create({
    $type: 'WorkerRequest',
    name: request.requestId,
    ...request,
  })

  return created as unknown as WorkerRequest
}

/**
 * Record multiple worker requests in a batch
 *
 * @param db - ParqueDB instance
 * @param inputs - Array of request data to record
 * @param options - Optional configuration
 * @returns Array of created request records
 */
export async function recordRequests(
  db: ParqueDB,
  inputs: RecordRequestInput[],
  options?: {
    collection?: string
  }
): Promise<WorkerRequest[]> {
  const collection = options?.collection ?? DEFAULT_REQUESTS_COLLECTION

  const requests: WorkerRequest[] = inputs.map(input => ({
    requestId: input.requestId ?? generateRequestId(),
    timestamp: input.timestamp ?? new Date(),
    method: input.method,
    path: input.path,
    status: input.status,
    statusCategory: getStatusCategory(input.status),
    latencyMs: input.latencyMs,
    cached: input.cached ?? false,
    cacheTier: input.cacheTier,
    colo: input.colo,
    country: input.country,
    city: input.city,
    region: input.region,
    timezone: input.timezone,
    requestSize: input.requestSize,
    responseSize: input.responseSize,
    userAgent: input.userAgent,
    dataset: input.dataset,
    collection: input.collection,
    resourceType: input.resourceType,
    error: input.error,
    errorCode: input.errorCode,
    metadata: input.metadata,
  }))

  const created = await db.collection(collection).createMany(
    requests.map(r => ({
      $type: 'WorkerRequest',
      name: r.requestId,
      ...r,
    }))
  )

  return created as unknown as WorkerRequest[]
}

// =============================================================================
// Metrics Aggregation
// =============================================================================

/**
 * Calculate latency statistics from an array of requests
 */
function calculateLatencyStats(requests: WorkerRequest[]): RequestMetrics['latency'] {
  if (requests.length === 0) {
    return { min: 0, max: 0, avg: 0, p50: 0, p95: 0, p99: 0 }
  }

  const latencies = requests.map(r => r.latencyMs).sort((a, b) => a - b)
  const sum = latencies.reduce((a, b) => a + b, 0)

  return {
    min: latencies[0]!,
    max: latencies[latencies.length - 1]!,
    avg: sum / latencies.length,
    p50: percentile(latencies, 50),
    p95: percentile(latencies, 95),
    p99: percentile(latencies, 99),
  }
}

/**
 * Aggregate requests into metrics
 */
function aggregateRequests(
  requests: WorkerRequest[],
  timeBucket: TimeBucket,
  groupBy?: string,
  groupValue?: string
): RequestMetrics {
  if (requests.length === 0) {
    const now = new Date()
    return {
      bucketStart: getBucketStart(now, timeBucket),
      bucketEnd: getBucketEnd(now, timeBucket),
      timeBucket,
      groupBy,
      groupValue,
      totalRequests: 0,
      successCount: 0,
      clientErrorCount: 0,
      serverErrorCount: 0,
      errorRate: 0,
      cacheHits: 0,
      cacheMisses: 0,
      cacheHitRatio: 0,
      latency: { min: 0, max: 0, avg: 0, p50: 0, p95: 0, p99: 0 },
      statusCodes: {},
      methods: {} as Record<HttpMethod, number>,
      totalRequestBytes: 0,
      totalResponseBytes: 0,
    }
  }

  // Get time bucket from first request
  const firstRequest = requests[0]!
  const bucketStart = getBucketStart(firstRequest.timestamp, timeBucket)
  const bucketEnd = getBucketEnd(firstRequest.timestamp, timeBucket)

  // Count by status category
  const successCount = requests.filter(r => r.statusCategory === '2xx').length
  const clientErrorCount = requests.filter(r => r.statusCategory === '4xx').length
  const serverErrorCount = requests.filter(r => r.statusCategory === '5xx').length

  // Count cache hits/misses
  const cacheHits = requests.filter(r => r.cached).length
  const cacheMisses = requests.filter(r => !r.cached).length

  // Count by status code
  const statusCodes: Record<number, number> = {}
  for (const r of requests) {
    statusCodes[r.status] = (statusCodes[r.status] || 0) + 1
  }

  // Count by method
  const methods: Record<HttpMethod, number> = {} as Record<HttpMethod, number>
  for (const r of requests) {
    methods[r.method] = (methods[r.method] || 0) + 1
  }

  // Sum bytes
  const totalRequestBytes = requests.reduce((sum, r) => sum + (r.requestSize || 0), 0)
  const totalResponseBytes = requests.reduce((sum, r) => sum + (r.responseSize || 0), 0)

  return {
    bucketStart,
    bucketEnd,
    timeBucket,
    groupBy,
    groupValue,
    totalRequests: requests.length,
    successCount,
    clientErrorCount,
    serverErrorCount,
    errorRate: (clientErrorCount + serverErrorCount) / requests.length,
    cacheHits,
    cacheMisses,
    cacheHitRatio: cacheHits / requests.length,
    latency: calculateLatencyStats(requests),
    statusCodes,
    methods,
    totalRequestBytes,
    totalResponseBytes,
  }
}

/**
 * Get aggregated request metrics
 *
 * @param db - ParqueDB instance
 * @param options - Query options
 * @returns Aggregated request metrics
 *
 * @example
 * ```typescript
 * // Get hourly metrics for the last 24 hours
 * const metrics = await getRequestMetrics(db, {
 *   since: new Date(Date.now() - 24 * 60 * 60 * 1000),
 *   timeBucket: 'hour',
 * })
 *
 * // Get metrics grouped by path
 * const pathMetrics = await getRequestMetrics(db, {
 *   since: new Date(Date.now() - 3600000),
 *   groupBy: 'path',
 * })
 *
 * // Get metrics for a specific colo
 * const coloMetrics = await getRequestMetrics(db, {
 *   colo: 'SJC',
 *   timeBucket: 'minute',
 * })
 * ```
 */
export async function getRequestMetrics(
  db: ParqueDB,
  options?: GetMetricsOptions
): Promise<RequestMetrics[]> {
  const collection = options?.collection ?? DEFAULT_REQUESTS_COLLECTION
  const timeBucket = options?.timeBucket ?? 'hour'

  // Build filter
  const filter: Record<string, unknown> = {}

  if (options?.since || options?.until) {
    filter.timestamp = {}
    if (options?.since) {
      (filter.timestamp as Record<string, unknown>).$gte = options.since
    }
    if (options?.until) {
      (filter.timestamp as Record<string, unknown>).$lt = options.until
    }
  }

  if (options?.statusCategory) {
    filter.statusCategory = options.statusCategory
  }

  if (options?.method) {
    filter.method = options.method
  }

  if (options?.colo) {
    filter.colo = options.colo
  }

  if (options?.country) {
    filter.country = options.country
  }

  if (options?.cachedOnly !== undefined) {
    filter.cached = options.cachedOnly
  }

  if (options?.pathPattern) {
    filter.path = { $regex: options.pathPattern }
  }

  // Fetch requests
  const requests = await db.collection(collection).find(filter, {
    limit: options?.limit ?? 10000,
    sort: { timestamp: 1 },
  }) as unknown as WorkerRequest[]

  // Group by time bucket
  const bucketMap = new Map<string, WorkerRequest[]>()
  // Use a delimiter unlikely to appear in ISO dates or group values
  const BUCKET_DELIMITER = '||'

  for (const request of requests) {
    const bucketKey = getBucketStart(request.timestamp, timeBucket).toISOString()

    // If grouping, add group value to key
    let fullKey = bucketKey
    if (options?.groupBy) {
      const groupValue = String((request as Record<string, unknown>)[options.groupBy] ?? 'unknown')
      fullKey = `${bucketKey}${BUCKET_DELIMITER}${groupValue}`
    }

    if (!bucketMap.has(fullKey)) {
      bucketMap.set(fullKey, [])
    }
    bucketMap.get(fullKey)!.push(request)
  }

  // Aggregate each bucket
  const metrics: RequestMetrics[] = []

  for (const [key, bucketRequests] of bucketMap) {
    let groupValue: string | undefined
    if (options?.groupBy) {
      const delimiterIndex = key.indexOf(BUCKET_DELIMITER)
      if (delimiterIndex !== -1) {
        groupValue = key.slice(delimiterIndex + BUCKET_DELIMITER.length)
      }
    }

    metrics.push(aggregateRequests(
      bucketRequests,
      timeBucket,
      options?.groupBy,
      groupValue
    ))
  }

  // Sort by bucket start time
  metrics.sort((a, b) => a.bucketStart.getTime() - b.bucketStart.getTime())

  return metrics
}

/**
 * Get real-time metrics for the current time bucket
 *
 * @param db - ParqueDB instance
 * @param options - Query options
 * @returns Current metrics
 */
export async function getCurrentMetrics(
  db: ParqueDB,
  options?: {
    collection?: string
    timeBucket?: TimeBucket
    groupBy?: GetMetricsOptions['groupBy']
  }
): Promise<RequestMetrics[]> {
  const timeBucket = options?.timeBucket ?? 'minute'
  const now = new Date()

  return getRequestMetrics(db, {
    collection: options?.collection,
    since: getBucketStart(now, timeBucket),
    until: getBucketEnd(now, timeBucket),
    timeBucket,
    groupBy: options?.groupBy,
  })
}

/**
 * Get latency percentiles for a specific path
 *
 * @param db - ParqueDB instance
 * @param path - Request path to analyze
 * @param options - Query options
 * @returns Latency statistics
 */
export async function getPathLatency(
  db: ParqueDB,
  path: string,
  options?: {
    collection?: string
    since?: Date
    until?: Date
    limit?: number
  }
): Promise<RequestMetrics['latency']> {
  const collection = options?.collection ?? DEFAULT_REQUESTS_COLLECTION

  const filter: Record<string, unknown> = { path }

  if (options?.since || options?.until) {
    filter.timestamp = {}
    if (options?.since) {
      (filter.timestamp as Record<string, unknown>).$gte = options.since
    }
    if (options?.until) {
      (filter.timestamp as Record<string, unknown>).$lt = options.until
    }
  }

  const requests = await db.collection(collection).find(filter, {
    limit: options?.limit ?? 1000,
    sort: { timestamp: -1 },
  }) as unknown as WorkerRequest[]

  return calculateLatencyStats(requests)
}

/**
 * Get error summary for recent requests
 *
 * @param db - ParqueDB instance
 * @param options - Query options
 * @returns Error summary by path and error code
 */
export async function getErrorSummary(
  db: ParqueDB,
  options?: {
    collection?: string
    since?: Date
    until?: Date
    limit?: number
  }
): Promise<{
  totalErrors: number
  byPath: Record<string, number>
  byStatusCode: Record<number, number>
  byErrorCode: Record<string, number>
  recentErrors: Array<{
    requestId: string
    timestamp: Date
    path: string
    status: number
    error?: string
  }>
}> {
  const collection = options?.collection ?? DEFAULT_REQUESTS_COLLECTION

  const filter: Record<string, unknown> = {
    status: { $gte: 400 },
  }

  if (options?.since || options?.until) {
    filter.timestamp = {}
    if (options?.since) {
      (filter.timestamp as Record<string, unknown>).$gte = options.since
    }
    if (options?.until) {
      (filter.timestamp as Record<string, unknown>).$lt = options.until
    }
  }

  const requests = await db.collection(collection).find(filter, {
    limit: options?.limit ?? 1000,
    sort: { timestamp: -1 },
  }) as unknown as WorkerRequest[]

  // Aggregate by path
  const byPath: Record<string, number> = {}
  for (const r of requests) {
    byPath[r.path] = (byPath[r.path] || 0) + 1
  }

  // Aggregate by status code
  const byStatusCode: Record<number, number> = {}
  for (const r of requests) {
    byStatusCode[r.status] = (byStatusCode[r.status] || 0) + 1
  }

  // Aggregate by error code
  const byErrorCode: Record<string, number> = {}
  for (const r of requests) {
    if (r.errorCode) {
      byErrorCode[r.errorCode] = (byErrorCode[r.errorCode] || 0) + 1
    }
  }

  // Get recent errors (last 10)
  const recentErrors = requests.slice(0, 10).map(r => ({
    requestId: r.requestId,
    timestamp: r.timestamp,
    path: r.path,
    status: r.status,
    error: r.error,
  }))

  return {
    totalErrors: requests.length,
    byPath,
    byStatusCode,
    byErrorCode,
    recentErrors,
  }
}

// =============================================================================
// Materialized View Definition
// =============================================================================

/**
 * Options for creating the WorkerRequests materialized view
 */
export interface WorkerRequestsMVOptions extends Omit<ViewOptions, 'populateOnCreate'> {
  /** Collection name for raw requests */
  requestsCollection?: string
  /** Collection name for aggregated metrics */
  metricsCollection?: string
}

/**
 * Create the WorkerRequests materialized view definition
 *
 * This creates a view definition that aggregates worker requests into
 * time-bucketed metrics. Use with the materialized views system to
 * automatically maintain aggregated analytics.
 *
 * @param options - View options
 * @returns View definition
 *
 * @example
 * ```typescript
 * // Create the MV with streaming refresh
 * const viewDef = createWorkerRequestsMV({
 *   refreshMode: 'streaming',
 *   maxStalenessMs: 1000,
 * })
 *
 * // Register with the view manager (when implemented)
 * // await db.createView(viewDef)
 * ```
 */
export function createWorkerRequestsMV(
  options?: WorkerRequestsMVOptions
): ViewDefinition {
  const requestsCollection = options?.requestsCollection ?? DEFAULT_REQUESTS_COLLECTION
  const metricsCollection = options?.metricsCollection ?? DEFAULT_METRICS_COLLECTION

  return {
    name: viewName('worker_requests_metrics'),
    source: requestsCollection,
    query: {
      pipeline: [
        // Match all requests (can be customized with filters)
        { $match: {} },
        // Group by hour and calculate aggregates
        {
          $group: {
            _id: {
              // Group by hour-truncated timestamp
              // Note: This is a conceptual representation - actual implementation
              // would use date truncation operators
              hour: '$timestamp',
            },
            totalRequests: { $sum: 1 },
            successCount: {
              $sum: {
                $cond: [{ $eq: ['$statusCategory', '2xx'] }, 1, 0],
              },
            },
            errorCount: {
              $sum: {
                $cond: [
                  { $in: ['$statusCategory', ['4xx', '5xx']] },
                  1,
                  0,
                ],
              },
            },
            cacheHits: {
              $sum: { $cond: ['$cached', 1, 0] },
            },
            avgLatency: { $avg: '$latencyMs' },
            minLatency: { $min: '$latencyMs' },
            maxLatency: { $max: '$latencyMs' },
            totalRequestBytes: { $sum: '$requestSize' },
            totalResponseBytes: { $sum: '$responseSize' },
          },
        },
        // Sort by time
        { $sort: { '_id.hour': -1 } },
      ],
    },
    options: {
      refreshMode: options?.refreshMode ?? 'streaming',
      refreshStrategy: options?.refreshStrategy ?? 'incremental',
      maxStalenessMs: options?.maxStalenessMs ?? 5000,
      schedule: options?.schedule,
      indexes: ['bucketStart', 'path', 'colo', 'country'],
      description: 'Aggregated worker request metrics for analytics',
      tags: ['analytics', 'worker', 'requests', 'monitoring'],
      metadata: {
        metricsCollection,
        requestsCollection,
      },
    },
  }
}

// =============================================================================
// Request Buffer (for high-throughput scenarios)
// =============================================================================

/**
 * Buffered request writer for high-throughput scenarios
 *
 * Buffers requests in memory and flushes to storage in batches
 * to reduce write overhead.
 */
export class RequestBuffer {
  private buffer: RecordRequestInput[] = []
  private flushTimer: ReturnType<typeof setTimeout> | null = null
  private flushPromise: Promise<void> | null = null

  constructor(
    private db: ParqueDB,
    private options: {
      collection?: string
      maxBufferSize?: number
      flushIntervalMs?: number
    } = {}
  ) {
    this.options.maxBufferSize = options.maxBufferSize ?? DEFAULT_BUFFER_SIZE
    this.options.flushIntervalMs = options.flushIntervalMs ?? DEFAULT_FLUSH_INTERVAL_MS
  }

  /**
   * Add a request to the buffer
   */
  async add(input: RecordRequestInput): Promise<void> {
    this.buffer.push(input)

    if (this.buffer.length >= (this.options.maxBufferSize ?? DEFAULT_BUFFER_SIZE)) {
      await this.flush()
    }
  }

  /**
   * Start periodic flush timer
   */
  startTimer(): void {
    if (this.flushTimer) return

    this.flushTimer = setInterval(() => {
      if (this.buffer.length > 0 && !this.flushPromise) {
        this.flush().catch(err => {
          console.error('[RequestBuffer] Flush failed:', err)
        })
      }
    }, this.options.flushIntervalMs ?? DEFAULT_FLUSH_INTERVAL_MS)
  }

  /**
   * Stop periodic flush timer
   */
  stopTimer(): void {
    if (this.flushTimer) {
      clearInterval(this.flushTimer)
      this.flushTimer = null
    }
  }

  /**
   * Flush all buffered requests to storage
   */
  async flush(): Promise<void> {
    if (this.flushPromise) {
      await this.flushPromise
      return
    }

    if (this.buffer.length === 0) return

    const toFlush = this.buffer
    this.buffer = []

    this.flushPromise = recordRequests(this.db, toFlush, {
      collection: this.options.collection,
    }).then(() => {
      this.flushPromise = null
    }).catch(err => {
      // Put failed requests back in buffer
      this.buffer.unshift(...toFlush)
      this.flushPromise = null
      throw err
    })

    await this.flushPromise
  }

  /**
   * Close the buffer - flush remaining requests and stop timer
   */
  async close(): Promise<void> {
    this.stopTimer()
    await this.flush()
  }

  /**
   * Get current buffer size
   */
  getBufferSize(): number {
    return this.buffer.length
  }
}

/**
 * Create a request buffer for high-throughput scenarios
 */
export function createRequestBuffer(
  db: ParqueDB,
  options?: {
    collection?: string
    maxBufferSize?: number
    flushIntervalMs?: number
    autoStart?: boolean
  }
): RequestBuffer {
  const buffer = new RequestBuffer(db, options)
  if (options?.autoStart ?? true) {
    buffer.startTimer()
  }
  return buffer
}
