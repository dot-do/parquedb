/**
 * Worker Mock Factories
 *
 * Provides mock implementations of ParqueDB worker components for testing.
 * Includes mock ParqueDBWorker, HandlerContext, and related types.
 */

import { vi, type Mock } from 'vitest'
import { createMockExecutionContext, type MockExecutionContext } from './execution-context'

// =============================================================================
// Types
// =============================================================================

/**
 * Entity structure for mocks
 */
export interface MockEntity {
  $id: string
  $type?: string | undefined
  name?: string | undefined
  [key: string]: unknown
}

/**
 * Find result structure
 */
export interface MockFindResult {
  items: MockEntity[]
  stats?: Record<string, unknown> | undefined
  hasMore: boolean
  cursor?: string | undefined
}

/**
 * Update result structure
 */
export interface MockUpdateResult {
  matched: number
  modified: number
}

/**
 * Delete result structure
 */
export interface MockDeleteResult {
  deleted: number
}

/**
 * Relationship structure
 */
export interface MockRelationship {
  from: { $id: string; name?: string }
  predicate: string
  to: { $id: string; name?: string }
  reverse?: string | undefined
  createdAt?: string | undefined
}

/**
 * Storage stats structure
 */
export interface MockStorageStats {
  cdnHits: number
  primaryHits: number
  edgeHits: number
  cacheHits: number
  totalReads: number
  usingCdn: boolean
  usingEdge: boolean
}

/**
 * Mock ParqueDBWorker interface
 */
export interface MockParqueDBWorker {
  get: Mock<[string, string], Promise<MockEntity | null>>
  find: Mock<[string, Record<string, unknown>?], Promise<MockFindResult>>
  create: Mock<[string, Record<string, unknown>], Promise<MockEntity>>
  update: Mock<[string, Record<string, unknown>, Record<string, unknown>], Promise<MockUpdateResult>>
  delete: Mock<[string, Record<string, unknown>], Promise<MockDeleteResult>>
  getRelationships: Mock<[string, string, string?], Promise<MockRelationship[]>>
  getStorageStats: Mock<[], MockStorageStats>

  // Test helpers
  _entities: Map<string, MockEntity>
  _relationships: MockRelationship[]
  _clear: () => void
}

/**
 * Mock HandlerContext interface
 */
export interface MockHandlerContext {
  request: Request
  url: URL
  baseUrl: string
  path: string
  worker: MockParqueDBWorker
  startTime: number
  ctx: MockExecutionContext
}

/**
 * Options for creating mock worker
 */
export interface MockWorkerOptions {
  /**
   * If true, returns a functional implementation that stores data.
   * If false (default), returns spy-only mocks.
   */
  functional?: boolean | undefined

  /**
   * Initial entities to populate
   */
  entities?: MockEntity[] | undefined

  /**
   * Initial relationships to populate
   */
  relationships?: MockRelationship[] | undefined

  /**
   * Custom storage stats
   */
  storageStats?: Partial<MockStorageStats> | undefined

  /**
   * Override for get method
   */
  get?: Mock | undefined

  /**
   * Override for find method
   */
  find?: Mock | undefined

  /**
   * Override for create method
   */
  create?: Mock | undefined

  /**
   * Override for update method
   */
  update?: Mock | undefined

  /**
   * Override for delete method
   */
  delete?: Mock | undefined

  /**
   * Override for getRelationships method
   */
  getRelationships?: Mock | undefined

  /**
   * Override for getStorageStats method
   */
  getStorageStats?: Mock | undefined
}

// =============================================================================
// Default Values
// =============================================================================

const DEFAULT_STORAGE_STATS: MockStorageStats = {
  cdnHits: 0,
  primaryHits: 0,
  edgeHits: 0,
  cacheHits: 0,
  totalReads: 0,
  usingCdn: false,
  usingEdge: false,
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a mock ParqueDBWorker
 *
 * @param options - Configuration options
 * @returns Mock ParqueDBWorker instance
 *
 * @example
 * ```typescript
 * // Simple spy-based mock
 * const worker = createMockWorker()
 * worker.get.mockResolvedValue({ $id: 'users/1', name: 'Alice' })
 *
 * // Functional mock with initial data
 * const worker = createMockWorker({
 *   functional: true,
 *   entities: [{ $id: 'users/1', name: 'Alice' }]
 * })
 * ```
 */
export function createMockWorker(options?: MockWorkerOptions): MockParqueDBWorker {
  const entities = new Map<string, MockEntity>()
  const relationships: MockRelationship[] = []
  const storageStats = { ...DEFAULT_STORAGE_STATS, ...options?.storageStats }

  // Initialize with provided data
  if (options?.entities) {
    for (const entity of options.entities) {
      entities.set(entity.$id, entity)
    }
  }
  if (options?.relationships) {
    relationships.push(...options.relationships)
  }

  if (options?.functional) {
    // Functional implementation
    return {
      _entities: entities,
      _relationships: relationships,
      _clear: () => {
        entities.clear()
        relationships.length = 0
      },

      get: vi.fn(async (ns: string, id: string): Promise<MockEntity | null> => {
        const fullId = `${ns}/${id}`
        return entities.get(fullId) ?? null
      }),

      find: vi.fn(async (ns: string, filter?: Record<string, unknown>): Promise<MockFindResult> => {
        const items: MockEntity[] = []
        for (const [id, entity] of entities) {
          if (!id.startsWith(`${ns}/`)) continue

          // Simple filter matching
          if (filter) {
            let matches = true
            for (const [key, value] of Object.entries(filter)) {
              if (entity[key] !== value) {
                matches = false
                break
              }
            }
            if (!matches) continue
          }

          items.push(entity)
        }
        return { items, hasMore: false, stats: {} }
      }),

      create: vi.fn(async (ns: string, data: Record<string, unknown>): Promise<MockEntity> => {
        const id = data.$id as string ?? `${ns}/${Date.now()}`
        const entity: MockEntity = { $id: id, ...data }
        entities.set(id, entity)
        return entity
      }),

      update: vi.fn(async (ns: string, filter: Record<string, unknown>, update: Record<string, unknown>): Promise<MockUpdateResult> => {
        let matched = 0
        let modified = 0

        for (const [id, entity] of entities) {
          if (!id.startsWith(`${ns}/`)) continue

          // Simple filter matching
          let matches = true
          for (const [key, value] of Object.entries(filter)) {
            if (entity[key] !== value) {
              matches = false
              break
            }
          }

          if (matches) {
            matched++
            // Apply update
            const $set = update.$set as Record<string, unknown> | undefined
            if ($set) {
              for (const [key, value] of Object.entries($set)) {
                entity[key] = value
              }
              modified++
            }
          }
        }

        return { matched, modified }
      }),

      delete: vi.fn(async (ns: string, filter: Record<string, unknown>): Promise<MockDeleteResult> => {
        let deleted = 0

        for (const [id, entity] of entities) {
          if (!id.startsWith(`${ns}/`)) continue

          // Simple filter matching
          let matches = true
          for (const [key, value] of Object.entries(filter)) {
            if (entity[key] !== value) {
              matches = false
              break
            }
          }

          if (matches) {
            entities.delete(id)
            deleted++
          }
        }

        return { deleted }
      }),

      getRelationships: vi.fn(async (ns: string, id: string, predicate?: string): Promise<MockRelationship[]> => {
        const fullId = `${ns}/${id}`
        return relationships.filter((rel) => {
          if (rel.from.$id !== fullId) return false
          if (predicate && rel.predicate !== predicate) return false
          return true
        })
      }),

      getStorageStats: vi.fn((): MockStorageStats => storageStats),
    }
  }

  // Spy-based mock with sensible defaults (can be overridden via options)
  return {
    _entities: entities,
    _relationships: relationships,
    _clear: () => {
      entities.clear()
      relationships.length = 0
    },

    get: options?.get ?? vi.fn().mockResolvedValue(null),
    find: options?.find ?? vi.fn().mockResolvedValue({ items: [], stats: {}, hasMore: false }),
    create: options?.create ?? vi.fn().mockImplementation(async (_ns: string, data: Record<string, unknown>) => ({
      $id: data.$id ?? 'test/1',
      name: data.name ?? 'Test',
      ...data,
    })),
    update: options?.update ?? vi.fn().mockResolvedValue({ matched: 1, modified: 1 }),
    delete: options?.delete ?? vi.fn().mockResolvedValue({ deleted: 1 }),
    getRelationships: options?.getRelationships ?? vi.fn().mockResolvedValue([]),
    getStorageStats: options?.getStorageStats ?? vi.fn().mockReturnValue(storageStats),
  }
}

/**
 * Create a mock HandlerContext for testing HTTP handlers
 *
 * @param urlOrPath - URL string or path to create request for
 * @param options - Additional options
 * @returns Mock HandlerContext instance
 *
 * @example
 * ```typescript
 * const ctx = createMockHandlerContext('https://api.example.com/v1/users')
 * const response = await handleUsers(ctx)
 * expect(response.status).toBe(200)
 * ```
 */
export function createMockHandlerContext(
  urlOrPath: string,
  options?: {
    method?: string
    body?: unknown
    headers?: Record<string, string>
    worker?: MockParqueDBWorker
  }
): MockHandlerContext {
  // Build full URL
  const fullUrl = urlOrPath.startsWith('http')
    ? urlOrPath
    : `https://api.example.com${urlOrPath.startsWith('/') ? '' : '/'}${urlOrPath}`

  const url = new URL(fullUrl)
  const method = options?.method ?? 'GET'

  // Build headers - include CSRF headers for mutation methods
  const headers: Record<string, string> = { ...options?.headers }
  if (['POST', 'PATCH', 'PUT', 'DELETE'].includes(method)) {
    headers['X-Requested-With'] = headers['X-Requested-With'] ?? 'XMLHttpRequest'
    headers['Origin'] = headers['Origin'] ?? url.origin
  }

  // Build request init
  const requestInit: RequestInit = { method, headers }
  if (options?.body) {
    requestInit.body = JSON.stringify(options.body)
    headers['Content-Type'] = 'application/json'
  }

  const request = new Request(fullUrl, requestInit)

  return {
    request,
    url,
    baseUrl: `${url.protocol}//${url.host}`,
    path: url.pathname,
    worker: options?.worker ?? createMockWorker(),
    startTime: performance.now(),
    ctx: createMockExecutionContext(),
  }
}

/**
 * Create a mock worker that simulates errors
 *
 * @param errorType - Type of error to simulate
 * @returns Mock worker that throws errors
 */
export function createErrorWorker(
  errorType: 'notFound' | 'unauthorized' | 'serverError' | 'timeout'
): MockParqueDBWorker {
  const worker = createMockWorker()

  const createError = () => {
    switch (errorType) {
      case 'notFound':
        return new Error('Entity not found')
      case 'unauthorized':
        return new Error('Unauthorized')
      case 'serverError':
        return new Error('Internal server error')
      case 'timeout':
        return new Error('Operation timed out')
    }
  }

  worker.get.mockRejectedValue(createError())
  worker.find.mockRejectedValue(createError())
  worker.create.mockRejectedValue(createError())
  worker.update.mockRejectedValue(createError())
  worker.delete.mockRejectedValue(createError())
  worker.getRelationships.mockRejectedValue(createError())

  return worker
}

/**
 * Create a mock Cache API for Cloudflare Workers
 *
 * @returns Mock caches object with default cache
 */
export function createMockCaches(): {
  default: {
    match: Mock
    put: Mock
    delete: Mock
  }
  open: Mock
} {
  const mockCache = {
    match: vi.fn().mockResolvedValue(null),
    put: vi.fn().mockResolvedValue(undefined),
    delete: vi.fn().mockResolvedValue(true),
  }

  return {
    default: mockCache,
    open: vi.fn().mockResolvedValue(mockCache),
  }
}
