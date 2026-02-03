/**
 * Durable Object Mock Factory
 *
 * Provides mock implementations of Cloudflare Durable Object APIs for testing.
 * Includes DurableObjectState, DurableObjectStorage, SqlStorage, and DurableObjectStub.
 */

import { vi, type Mock } from 'vitest'

// =============================================================================
// Types
// =============================================================================

/**
 * SQL storage value type
 */
export type SqlStorageValue = string | number | null | ArrayBuffer

/**
 * SQL storage cursor interface
 */
export interface SqlStorageCursor<T = Record<string, SqlStorageValue>> {
  toArray(): T[]
  one(): T | null
  raw(): unknown[][]
  columnNames: string[]
  rowsRead: number
  rowsWritten: number
}

/**
 * SQL storage interface
 */
export interface SqlStorage {
  exec<T = Record<string, SqlStorageValue>>(query: string, ...params: unknown[]): SqlStorageCursor<T>
}

/**
 * Durable Object storage interface
 */
export interface DurableObjectStorage {
  get<T = unknown>(key: string): Promise<T | undefined>
  get<T = unknown>(keys: string[]): Promise<Map<string, T>>
  put<T = unknown>(key: string, value: T): Promise<void>
  put<T = unknown>(entries: Record<string, T>): Promise<void>
  delete(key: string): Promise<boolean>
  delete(keys: string[]): Promise<number>
  deleteAll(): Promise<void>
  list<T = unknown>(options?: {
    prefix?: string | undefined
    start?: string | undefined
    end?: string | undefined
    limit?: number | undefined
    reverse?: boolean | undefined
  }): Promise<Map<string, T>>
  sql: SqlStorage
}

/**
 * Durable Object ID interface
 */
export interface DurableObjectId {
  toString(): string
  equals(other: DurableObjectId): boolean
  name?: string | undefined
}

/**
 * Durable Object state interface
 */
export interface DurableObjectState {
  id: DurableObjectId
  storage: DurableObjectStorage
  waitUntil(promise: Promise<unknown>): void
  blockConcurrencyWhile<T>(callback: () => Promise<T>): Promise<T>
}

/**
 * Durable Object stub interface
 */
export interface DurableObjectStub {
  id: DurableObjectId
  name?: string | undefined
  fetch(request: Request | string, init?: RequestInit): Promise<Response>
}

/**
 * Durable Object namespace interface
 */
export interface DurableObjectNamespace {
  newUniqueId(): DurableObjectId
  idFromName(name: string): DurableObjectId
  idFromString(id: string): DurableObjectId
  get(id: DurableObjectId): DurableObjectStub
}

/**
 * Mock DurableObjectStorage with vi.fn() methods
 */
export interface MockDurableObjectStorage extends DurableObjectStorage {
  get: Mock
  put: Mock
  delete: Mock
  deleteAll: Mock
  list: Mock
  sql: MockSqlStorage

  // Test helpers
  _store: Map<string, unknown>
  _clear: () => void
}

/**
 * Mock SqlStorage with vi.fn() methods
 */
export interface MockSqlStorage extends SqlStorage {
  exec: Mock

  // Test helpers
  _queries: Array<{ query: string; params: unknown[] }>
  _results: Map<string, unknown[]>
  _setResult: (pattern: string, rows: unknown[]) => void
}

/**
 * Mock DurableObjectState with vi.fn() methods
 */
export interface MockDurableObjectState extends DurableObjectState {
  waitUntil: Mock
  blockConcurrencyWhile: Mock
  storage: MockDurableObjectStorage
}

/**
 * Mock DurableObjectStub with vi.fn() methods
 */
export interface MockDurableObjectStub extends DurableObjectStub {
  fetch: Mock
}

/**
 * Mock DurableObjectNamespace with vi.fn() methods
 */
export interface MockDurableObjectNamespace extends DurableObjectNamespace {
  newUniqueId: Mock
  idFromName: Mock
  idFromString: Mock
  get: Mock

  // Test helpers
  _stubs: Map<string, MockDurableObjectStub>
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a mock DurableObjectId
 */
export function createMockDurableObjectId(id?: string, name?: string): DurableObjectId {
  const idValue = id ?? `do-${Math.random().toString(36).slice(2)}`
  return {
    toString: () => idValue,
    equals: (other: DurableObjectId) => other.toString() === idValue,
    name,
  }
}

/**
 * Create a mock SqlStorage
 */
export function createMockSqlStorage(): MockSqlStorage {
  const queries: Array<{ query: string; params: unknown[] }> = []
  const results = new Map<string, unknown[]>()

  return {
    _queries: queries,
    _results: results,
    _setResult: (pattern: string, rows: unknown[]) => {
      results.set(pattern, rows)
    },

    exec: vi.fn(<T = Record<string, SqlStorageValue>>(query: string, ...params: unknown[]): SqlStorageCursor<T> => {
      queries.push({ query, params })

      // Find matching result pattern
      let rows: unknown[] = []
      for (const [pattern, resultRows] of results) {
        if (query.toLowerCase().includes(pattern.toLowerCase())) {
          rows = resultRows
          break
        }
      }

      return {
        toArray: () => rows as T[],
        one: () => (rows.length > 0 ? (rows[0] as T) : null),
        raw: () => rows.map((r) => Object.values(r as Record<string, unknown>)),
        columnNames: rows.length > 0 ? Object.keys(rows[0] as Record<string, unknown>) : [],
        rowsRead: rows.length,
        rowsWritten: 0,
      }
    }),
  }
}

/**
 * Create a mock DurableObjectStorage
 *
 * @param options - Configuration options
 * @returns Mock DurableObjectStorage instance
 */
export function createMockDurableObjectStorage(): MockDurableObjectStorage {
  const store = new Map<string, unknown>()
  const sql = createMockSqlStorage()

  return {
    _store: store,
    _clear: () => store.clear(),
    sql,

    get: vi.fn(async <T = unknown>(keyOrKeys: string | string[]): Promise<T | Map<string, T> | undefined> => {
      if (Array.isArray(keyOrKeys)) {
        const result = new Map<string, T>()
        for (const key of keyOrKeys) {
          const value = store.get(key)
          if (value !== undefined) {
            result.set(key, value as T)
          }
        }
        return result as Map<string, T>
      }
      return store.get(keyOrKeys) as T | undefined
    }),

    put: vi.fn(async <T = unknown>(keyOrEntries: string | Record<string, T>, value?: T): Promise<void> => {
      if (typeof keyOrEntries === 'string') {
        store.set(keyOrEntries, value)
      } else {
        for (const [k, v] of Object.entries(keyOrEntries)) {
          store.set(k, v)
        }
      }
    }),

    delete: vi.fn(async (keyOrKeys: string | string[]): Promise<boolean | number> => {
      if (Array.isArray(keyOrKeys)) {
        let count = 0
        for (const key of keyOrKeys) {
          if (store.delete(key)) count++
        }
        return count
      }
      return store.delete(keyOrKeys)
    }),

    deleteAll: vi.fn(async (): Promise<void> => {
      store.clear()
    }),

    list: vi.fn(async <T = unknown>(options?: {
      prefix?: string
      start?: string
      end?: string
      limit?: number
      reverse?: boolean
    }): Promise<Map<string, T>> => {
      const result = new Map<string, T>()
      let entries = Array.from(store.entries())

      if (options?.prefix) {
        entries = entries.filter(([k]) => k.startsWith(options.prefix!))
      }
      if (options?.start) {
        entries = entries.filter(([k]) => k >= options.start!)
      }
      if (options?.end) {
        entries = entries.filter(([k]) => k < options.end!)
      }
      if (options?.reverse) {
        entries.reverse()
      }
      if (options?.limit) {
        entries = entries.slice(0, options.limit)
      }

      for (const [k, v] of entries) {
        result.set(k, v as T)
      }
      return result
    }),
  }
}

/**
 * Create a mock DurableObjectState
 *
 * @param options - Configuration options
 * @returns Mock DurableObjectState instance
 */
export function createMockDurableObjectState(options?: {
  id?: DurableObjectId
  storage?: MockDurableObjectStorage
}): MockDurableObjectState {
  const id = options?.id ?? createMockDurableObjectId()
  const storage = options?.storage ?? createMockDurableObjectStorage()

  return {
    id,
    storage,

    waitUntil: vi.fn((_promise: Promise<unknown>): void => {
      // No-op in tests
    }),

    blockConcurrencyWhile: vi.fn(async <T>(callback: () => Promise<T>): Promise<T> => {
      return callback()
    }),
  }
}

/**
 * Create a mock DurableObjectStub
 *
 * @param options - Configuration options
 * @returns Mock DurableObjectStub instance
 */
export function createMockDurableObjectStub(options?: {
  id?: DurableObjectId
  fetchHandler?: (request: Request) => Promise<Response>
}): MockDurableObjectStub {
  const id = options?.id ?? createMockDurableObjectId()
  const defaultHandler = async (_request: Request) => new Response('OK', { status: 200 })
  const handler = options?.fetchHandler ?? defaultHandler

  return {
    id,
    name: id.name,

    fetch: vi.fn(async (requestOrUrl: Request | string, init?: RequestInit): Promise<Response> => {
      const request = typeof requestOrUrl === 'string'
        ? new Request(requestOrUrl, init)
        : requestOrUrl
      return handler(request)
    }),
  }
}

/**
 * Create a mock DurableObjectNamespace
 *
 * @param options - Configuration options
 * @returns Mock DurableObjectNamespace instance
 */
export function createMockDurableObjectNamespace(options?: {
  stubFactory?: (id: DurableObjectId) => MockDurableObjectStub
}): MockDurableObjectNamespace {
  const stubs = new Map<string, MockDurableObjectStub>()
  const stubFactory = options?.stubFactory ?? ((id) => createMockDurableObjectStub({ id }))

  return {
    _stubs: stubs,

    newUniqueId: vi.fn((): DurableObjectId => {
      return createMockDurableObjectId()
    }),

    idFromName: vi.fn((name: string): DurableObjectId => {
      return createMockDurableObjectId(`name:${name}`, name)
    }),

    idFromString: vi.fn((id: string): DurableObjectId => {
      return createMockDurableObjectId(id)
    }),

    get: vi.fn((id: DurableObjectId): MockDurableObjectStub => {
      const idStr = id.toString()
      let stub = stubs.get(idStr)
      if (!stub) {
        stub = stubFactory(id)
        stubs.set(idStr, stub)
      }
      return stub
    }),
  }
}

/**
 * Create a complete mock Durable Object environment
 *
 * @returns Object containing all DO mocks
 */
export function createMockDurableObjectEnv(): {
  state: MockDurableObjectState
  storage: MockDurableObjectStorage
  sql: MockSqlStorage
  namespace: MockDurableObjectNamespace
} {
  const storage = createMockDurableObjectStorage()
  const state = createMockDurableObjectState({ storage })
  const namespace = createMockDurableObjectNamespace()

  return {
    state,
    storage,
    sql: storage.sql,
    namespace,
  }
}
