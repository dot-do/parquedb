/**
 * KVNamespace Mock Factory
 *
 * Provides mock implementations of Cloudflare KV Namespace for testing.
 * Supports TTL expiration, metadata, and list operations.
 */

import { vi, type Mock } from 'vitest'

// =============================================================================
// Types
// =============================================================================

/**
 * KV value types
 */
export type KVValueType = 'text' | 'json' | 'arrayBuffer' | 'stream'

/**
 * Options for KV get operations
 */
export interface KVGetOptions<T extends KVValueType = 'text'> {
  type?: T | undefined
  cacheTtl?: number | undefined
}

/**
 * Options for KV put operations
 */
export interface KVPutOptions {
  expiration?: number | undefined
  expirationTtl?: number | undefined
  metadata?: Record<string, unknown> | undefined
}

/**
 * Options for KV list operations
 */
export interface KVListOptions {
  prefix?: string | undefined
  limit?: number | undefined
  cursor?: string | undefined
}

/**
 * Result of KV list operation
 */
export interface KVListResult<T = unknown> {
  keys: Array<{ name: string; expiration?: number; metadata?: T }>
  list_complete: boolean
  cursor?: string | undefined
}

/**
 * Result of getWithMetadata operation
 */
export interface KVValueWithMetadata<T, M = unknown> {
  value: T | null
  metadata: M | null
}

/**
 * Cloudflare KV Namespace interface
 */
export interface KVNamespace {
  get(key: string, options?: KVGetOptions<'text'>): Promise<string | null>
  get(key: string, options: KVGetOptions<'json'>): Promise<unknown | null>
  get(key: string, options: KVGetOptions<'arrayBuffer'>): Promise<ArrayBuffer | null>
  get(key: string, options: KVGetOptions<'stream'>): Promise<ReadableStream | null>
  get(key: string, type: 'text'): Promise<string | null>
  get(key: string, type: 'json'): Promise<unknown | null>
  get(key: string, type: 'arrayBuffer'): Promise<ArrayBuffer | null>
  get(key: string, type: 'stream'): Promise<ReadableStream | null>

  getWithMetadata<M = unknown>(
    key: string,
    options?: KVGetOptions<'text'> | undefined
  ): Promise<KVValueWithMetadata<string, M>>
  getWithMetadata<M = unknown>(
    key: string,
    type: 'text'
  ): Promise<KVValueWithMetadata<string, M>>

  put(key: string, value: string | ArrayBuffer | ReadableStream, options?: KVPutOptions): Promise<void>

  delete(key: string): Promise<void>

  list<M = unknown>(options?: KVListOptions): Promise<KVListResult<M>>
}

/**
 * Mock KVNamespace with vi.fn() methods for assertions
 */
export interface MockKVNamespace extends KVNamespace {
  get: Mock
  getWithMetadata: Mock
  put: Mock
  delete: Mock
  list: Mock

  // Test helpers
  _store: Map<string, { value: string; metadata?: Record<string, unknown>; expiration?: number }>
  _clear: () => void
  _setExpired: (key: string) => void
}

/**
 * Options for creating mock KVNamespace
 */
export interface MockKVNamespaceOptions {
  /**
   * If true, returns a functional in-memory implementation.
   * If false (default), returns spy-only mocks that return sensible defaults.
   */
  functional?: boolean | undefined

  /**
   * Initial data to populate the namespace with
   */
  initialData?: Map<string, string> | undefined
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a mock KVNamespace
 *
 * @param options - Configuration options
 * @returns Mock KVNamespace instance
 *
 * @example
 * ```typescript
 * // Simple spy-based mock (default)
 * const kv = createMockKVNamespace()
 * kv.get.mockResolvedValue('cached-value')
 *
 * // Functional in-memory implementation
 * const kv = createMockKVNamespace({ functional: true })
 * await kv.put('key', 'value')
 * const result = await kv.get('key')
 * ```
 */
export function createMockKVNamespace(options?: MockKVNamespaceOptions): MockKVNamespace {
  const store = new Map<string, { value: string; metadata?: Record<string, unknown>; expiration?: number }>()

  // Initialize with any provided data
  if (options?.initialData) {
    for (const [key, value] of options.initialData) {
      store.set(key, { value })
    }
  }

  /**
   * Check if a key has expired
   */
  function isExpired(key: string): boolean {
    const item = store.get(key)
    if (!item?.expiration) return false
    return Date.now() / 1000 > item.expiration
  }

  /**
   * Clean up expired keys
   */
  function cleanupExpired(): void {
    for (const key of store.keys()) {
      if (isExpired(key)) {
        store.delete(key)
      }
    }
  }

  if (options?.functional) {
    // Functional implementation
    return {
      _store: store,
      _clear: () => store.clear(),
      _setExpired: (key: string) => {
        const item = store.get(key)
        if (item) {
          item.expiration = Date.now() / 1000 - 1
        }
      },

      get: vi.fn(async (key: string, typeOrOptions?: KVValueType | KVGetOptions): Promise<unknown> => {
        cleanupExpired()
        const item = store.get(key)
        if (!item) return null

        const type = typeof typeOrOptions === 'string'
          ? typeOrOptions
          : (typeOrOptions as KVGetOptions | undefined)?.type ?? 'text'

        switch (type) {
          case 'json':
            return JSON.parse(item.value)
          case 'arrayBuffer':
            return new TextEncoder().encode(item.value).buffer
          case 'stream':
            return new ReadableStream({
              start(controller) {
                controller.enqueue(new TextEncoder().encode(item.value))
                controller.close()
              },
            })
          default:
            return item.value
        }
      }),

      getWithMetadata: vi.fn(async (key: string, _typeOrOptions?: unknown): Promise<KVValueWithMetadata<string, Record<string, unknown>>> => {
        cleanupExpired()
        const item = store.get(key)
        if (!item) return { value: null, metadata: null }
        return { value: item.value, metadata: item.metadata ?? null }
      }),

      put: vi.fn(async (key: string, value: string | ArrayBuffer | ReadableStream, putOptions?: KVPutOptions): Promise<void> => {
        let stringValue: string
        if (typeof value === 'string') {
          stringValue = value
        } else if (value instanceof ArrayBuffer) {
          stringValue = new TextDecoder().decode(value)
        } else {
          // ReadableStream - consume it
          const reader = value.getReader()
          const chunks: Uint8Array[] = []
          let result = await reader.read()
          while (!result.done) {
            chunks.push(result.value)
            result = await reader.read()
          }
          const combined = new Uint8Array(chunks.reduce((acc, c) => acc + c.length, 0))
          let offset = 0
          for (const chunk of chunks) {
            combined.set(chunk, offset)
            offset += chunk.length
          }
          stringValue = new TextDecoder().decode(combined)
        }

        let expiration: number | undefined
        if (putOptions?.expiration) {
          expiration = putOptions.expiration
        } else if (putOptions?.expirationTtl) {
          expiration = Date.now() / 1000 + putOptions.expirationTtl
        }

        store.set(key, {
          value: stringValue,
          metadata: putOptions?.metadata,
          expiration,
        })
      }),

      delete: vi.fn(async (key: string): Promise<void> => {
        store.delete(key)
      }),

      list: vi.fn(async <M = unknown>(listOptions?: KVListOptions): Promise<KVListResult<M>> => {
        cleanupExpired()
        const keys: Array<{ name: string; expiration?: number; metadata?: M }> = []

        for (const [key, item] of store) {
          if (listOptions?.prefix && !key.startsWith(listOptions.prefix)) {
            continue
          }

          keys.push({
            name: key,
            expiration: item.expiration,
            metadata: item.metadata as M | undefined,
          })

          if (listOptions?.limit && keys.length >= listOptions.limit) {
            break
          }
        }

        return {
          keys,
          list_complete: !listOptions?.limit || keys.length < listOptions.limit,
          cursor: undefined,
        }
      }),
    }
  }

  // Spy-based mock with sensible defaults
  return {
    _store: store,
    _clear: () => store.clear(),
    _setExpired: () => {},

    get: vi.fn().mockResolvedValue(null),
    getWithMetadata: vi.fn().mockResolvedValue({ value: null, metadata: null }),
    put: vi.fn().mockResolvedValue(undefined),
    delete: vi.fn().mockResolvedValue(undefined),
    list: vi.fn().mockResolvedValue({ keys: [], list_complete: true }),
  }
}

/**
 * Create a mock KVNamespace with pre-populated cache data
 *
 * @param data - Key-value pairs to populate
 * @returns Functional mock KVNamespace
 */
export function createPopulatedKVNamespace(
  data: Record<string, string | { value: string; metadata?: Record<string, unknown> }>
): MockKVNamespace {
  const initialData = new Map<string, string>()
  const kv = createMockKVNamespace({ functional: true, initialData })

  // Populate with data including metadata
  for (const [key, value] of Object.entries(data)) {
    if (typeof value === 'string') {
      kv._store.set(key, { value })
    } else {
      kv._store.set(key, { value: value.value, metadata: value.metadata })
    }
  }

  return kv
}
