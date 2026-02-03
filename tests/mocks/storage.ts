/**
 * Mock storage implementations for testing ParqueDB
 *
 * These mocks provide consistent behavior across all test environments
 * (Node.js, browser, and Cloudflare Workers).
 *
 * This module includes:
 * - MockStorage: Simple key-value storage interface
 * - MockStorageBackend: Full StorageBackend implementation for testing
 */

import { vi, type Mock } from 'vitest'
import { getEnvironment } from '../setup'
import type {
  StorageBackend,
  FileStat,
  ListOptions,
  ListResult,
  WriteOptions,
  WriteResult,
  RmdirOptions,
} from '../../src/types/storage'

// =============================================================================
// Simple Mock Storage Interface
// =============================================================================

/**
 * Storage interface that all implementations follow
 */
export interface MockStorage {
  get(key: string): Promise<Uint8Array | null>
  put(key: string, value: Uint8Array): Promise<void>
  delete(key: string): Promise<boolean>
  list(prefix?: string): Promise<string[]>
  clear(): Promise<void>
  size(): number
}

// =============================================================================
// StorageBackend Mock Types
// =============================================================================

/**
 * Mock StorageBackend with vi.fn() methods for assertions
 */
export interface MockStorageBackend extends StorageBackend {
  read: Mock<[string], Promise<Uint8Array>>
  readRange: Mock<[string, number, number], Promise<Uint8Array>>
  write: Mock<[string, Uint8Array, WriteOptions?], Promise<WriteResult>>
  writeAtomic: Mock<[string, Uint8Array, WriteOptions?], Promise<WriteResult>>
  append: Mock<[string, Uint8Array], Promise<void>>
  delete: Mock<[string], Promise<boolean>>
  deletePrefix: Mock<[string], Promise<number>>
  exists: Mock<[string], Promise<boolean>>
  stat: Mock<[string], Promise<FileStat | null>>
  list: Mock<[string, ListOptions?], Promise<ListResult>>
  mkdir: Mock<[string], Promise<void>>
  rmdir: Mock<[string, RmdirOptions?], Promise<void>>
  writeConditional: Mock<[string, Uint8Array, string | null, WriteOptions?], Promise<WriteResult>>
  copy: Mock<[string, string], Promise<void>>
  move: Mock<[string, string], Promise<void>>

  // Test helpers
  _store: Map<string, { data: Uint8Array; stat: FileStat }>
  _clear: () => void
}

/**
 * Options for creating MockStorageBackend
 */
export interface MockStorageBackendOptions {
  /**
   * If true, returns a functional in-memory implementation.
   * If false (default), returns spy-only mocks that return sensible defaults.
   */
  functional?: boolean

  /**
   * Backend type identifier
   */
  type?: string

  /**
   * Initial data to populate
   */
  initialData?: Map<string, Uint8Array>
}

// =============================================================================
// StorageBackend Factory
// =============================================================================

/**
 * Create a mock StorageBackend
 *
 * @param options - Configuration options
 * @returns Mock StorageBackend instance
 *
 * @example
 * ```typescript
 * // Simple spy-based mock (default)
 * const storage = createMockStorageBackend()
 * storage.read.mockResolvedValue(new Uint8Array([1, 2, 3]))
 *
 * // Functional in-memory implementation
 * const storage = createMockStorageBackend({ functional: true })
 * await storage.write('test.txt', data)
 * const result = await storage.read('test.txt')
 * ```
 */
export function createMockStorageBackend(options?: MockStorageBackendOptions): MockStorageBackend {
  const store = new Map<string, { data: Uint8Array; stat: FileStat }>()
  const backendType = options?.type ?? 'memory'

  // Initialize with provided data
  if (options?.initialData) {
    for (const [path, data] of options.initialData) {
      store.set(path, {
        data,
        stat: createFileStat(path, data.length),
      })
    }
  }

  function createFileStat(path: string, size: number): FileStat {
    return {
      path,
      size,
      mtime: new Date(),
      isDirectory: false,
      etag: `"${Math.random().toString(36).slice(2)}"`,
    }
  }

  function createWriteResult(size: number): WriteResult {
    return {
      etag: `"${Math.random().toString(36).slice(2)}"`,
      size,
    }
  }

  if (options?.functional) {
    // Functional implementation
    return {
      type: backendType,
      _store: store,
      _clear: () => store.clear(),

      read: vi.fn(async (path: string): Promise<Uint8Array> => {
        const item = store.get(path)
        if (!item) {
          throw new Error(`File not found: ${path}`)
        }
        return new Uint8Array(item.data)
      }),

      readRange: vi.fn(async (path: string, start: number, end: number): Promise<Uint8Array> => {
        const item = store.get(path)
        if (!item) {
          throw new Error(`File not found: ${path}`)
        }
        return item.data.slice(start, end)
      }),

      write: vi.fn(async (path: string, data: Uint8Array, _options?: WriteOptions): Promise<WriteResult> => {
        store.set(path, {
          data: new Uint8Array(data),
          stat: createFileStat(path, data.length),
        })
        return createWriteResult(data.length)
      }),

      writeAtomic: vi.fn(async (path: string, data: Uint8Array, _options?: WriteOptions): Promise<WriteResult> => {
        store.set(path, {
          data: new Uint8Array(data),
          stat: createFileStat(path, data.length),
        })
        return createWriteResult(data.length)
      }),

      append: vi.fn(async (path: string, data: Uint8Array): Promise<void> => {
        const existing = store.get(path)
        if (existing) {
          const newData = new Uint8Array(existing.data.length + data.length)
          newData.set(existing.data)
          newData.set(data, existing.data.length)
          store.set(path, {
            data: newData,
            stat: createFileStat(path, newData.length),
          })
        } else {
          store.set(path, {
            data: new Uint8Array(data),
            stat: createFileStat(path, data.length),
          })
        }
      }),

      delete: vi.fn(async (path: string): Promise<boolean> => {
        return store.delete(path)
      }),

      deletePrefix: vi.fn(async (prefix: string): Promise<number> => {
        let count = 0
        for (const key of store.keys()) {
          if (key.startsWith(prefix)) {
            store.delete(key)
            count++
          }
        }
        return count
      }),

      exists: vi.fn(async (path: string): Promise<boolean> => {
        return store.has(path)
      }),

      stat: vi.fn(async (path: string): Promise<FileStat | null> => {
        const item = store.get(path)
        return item?.stat ?? null
      }),

      list: vi.fn(async (prefix: string, options?: ListOptions): Promise<ListResult> => {
        const files: string[] = []
        const prefixes = new Set<string>()

        for (const key of store.keys()) {
          if (!key.startsWith(prefix)) continue

          if (options?.delimiter) {
            const rest = key.slice(prefix.length)
            const delimiterIndex = rest.indexOf(options.delimiter)
            if (delimiterIndex >= 0) {
              prefixes.add(key.slice(0, prefix.length + delimiterIndex + 1))
              continue
            }
          }

          files.push(key)

          if (options?.limit && files.length >= options.limit) {
            break
          }
        }

        return {
          files,
          prefixes: Array.from(prefixes),
          hasMore: options?.limit ? store.size > files.length : false,
        }
      }),

      mkdir: vi.fn(async (_path: string): Promise<void> => {
        // No-op for memory storage
      }),

      rmdir: vi.fn(async (path: string, options?: RmdirOptions): Promise<void> => {
        if (options?.recursive) {
          for (const key of store.keys()) {
            if (key.startsWith(path)) {
              store.delete(key)
            }
          }
        }
      }),

      writeConditional: vi.fn(async (
        path: string,
        data: Uint8Array,
        expectedVersion: string | null,
        _options?: WriteOptions
      ): Promise<WriteResult> => {
        const existing = store.get(path)
        if (expectedVersion !== null) {
          if (!existing || existing.stat.etag !== expectedVersion) {
            throw new Error('Version mismatch')
          }
        } else if (existing) {
          throw new Error('File already exists')
        }

        store.set(path, {
          data: new Uint8Array(data),
          stat: createFileStat(path, data.length),
        })
        return createWriteResult(data.length)
      }),

      copy: vi.fn(async (source: string, dest: string): Promise<void> => {
        const item = store.get(source)
        if (!item) {
          throw new Error(`Source not found: ${source}`)
        }
        store.set(dest, {
          data: new Uint8Array(item.data),
          stat: createFileStat(dest, item.data.length),
        })
      }),

      move: vi.fn(async (source: string, dest: string): Promise<void> => {
        const item = store.get(source)
        if (!item) {
          throw new Error(`Source not found: ${source}`)
        }
        store.set(dest, {
          data: item.data,
          stat: createFileStat(dest, item.data.length),
        })
        store.delete(source)
      }),
    }
  }

  // Spy-based mock with sensible defaults
  return {
    type: backendType,
    _store: store,
    _clear: () => store.clear(),

    read: vi.fn().mockRejectedValue(new Error('File not found')),
    readRange: vi.fn().mockRejectedValue(new Error('File not found')),
    write: vi.fn().mockResolvedValue(createWriteResult(0)),
    writeAtomic: vi.fn().mockResolvedValue(createWriteResult(0)),
    append: vi.fn().mockResolvedValue(undefined),
    delete: vi.fn().mockResolvedValue(false),
    deletePrefix: vi.fn().mockResolvedValue(0),
    exists: vi.fn().mockResolvedValue(false),
    stat: vi.fn().mockResolvedValue(null),
    list: vi.fn().mockResolvedValue({ files: [], hasMore: false }),
    mkdir: vi.fn().mockResolvedValue(undefined),
    rmdir: vi.fn().mockResolvedValue(undefined),
    writeConditional: vi.fn().mockResolvedValue(createWriteResult(0)),
    copy: vi.fn().mockResolvedValue(undefined),
    move: vi.fn().mockResolvedValue(undefined),
  }
}

/**
 * Create a mock StorageBackend that simulates errors
 */
export function createErrorStorageBackend(
  errorType: 'notFound' | 'permission' | 'network' | 'quota'
): MockStorageBackend {
  const backend = createMockStorageBackend()

  const createError = () => {
    switch (errorType) {
      case 'notFound':
        return new Error('File not found')
      case 'permission':
        return new Error('Permission denied')
      case 'network':
        return new Error('Network error')
      case 'quota':
        return new Error('Quota exceeded')
    }
  }

  backend.read.mockRejectedValue(createError())
  backend.readRange.mockRejectedValue(createError())
  backend.write.mockRejectedValue(createError())
  backend.writeAtomic.mockRejectedValue(createError())
  backend.append.mockRejectedValue(createError())
  backend.delete.mockRejectedValue(createError())
  backend.deletePrefix.mockRejectedValue(createError())
  backend.stat.mockRejectedValue(createError())
  backend.list.mockRejectedValue(createError())

  return backend
}

// =============================================================================
// Legacy Mock Storage Classes
// =============================================================================

/**
 * In-memory storage mock that works in all environments
 */
export class MemoryStorageMock implements MockStorage {
  private store = new Map<string, Uint8Array>()

  async get(key: string): Promise<Uint8Array | null> {
    const value = this.store.get(key)
    return value ? new Uint8Array(value) : null
  }

  async put(key: string, value: Uint8Array): Promise<void> {
    this.store.set(key, new Uint8Array(value))
  }

  async delete(key: string): Promise<boolean> {
    return this.store.delete(key)
  }

  async list(prefix?: string): Promise<string[]> {
    const keys = Array.from(this.store.keys())
    if (prefix) {
      return keys.filter(key => key.startsWith(prefix))
    }
    return keys
  }

  async clear(): Promise<void> {
    this.store.clear()
  }

  size(): number {
    return this.store.size
  }
}

/**
 * Mock R2 Bucket for Cloudflare Workers tests
 */
export class MockR2Bucket implements MockStorage {
  private store = new Map<string, Uint8Array>()
  private metadata = new Map<string, Record<string, string>>()

  async get(key: string): Promise<Uint8Array | null> {
    const value = this.store.get(key)
    return value ? new Uint8Array(value) : null
  }

  async put(key: string, value: Uint8Array, options?: { customMetadata?: Record<string, string> }): Promise<void> {
    this.store.set(key, new Uint8Array(value))
    if (options?.customMetadata) {
      this.metadata.set(key, options.customMetadata)
    }
  }

  async delete(key: string): Promise<boolean> {
    this.metadata.delete(key)
    return this.store.delete(key)
  }

  async list(prefix?: string): Promise<string[]> {
    const keys = Array.from(this.store.keys())
    if (prefix) {
      return keys.filter(key => key.startsWith(prefix))
    }
    return keys
  }

  async clear(): Promise<void> {
    this.store.clear()
    this.metadata.clear()
  }

  size(): number {
    return this.store.size
  }

  // R2-specific methods
  async head(key: string): Promise<{ size: number; customMetadata: Record<string, string> } | null> {
    const value = this.store.get(key)
    if (!value) return null
    return {
      size: value.byteLength,
      customMetadata: this.metadata.get(key) || {}
    }
  }
}

/**
 * Mock KV Namespace for Cloudflare Workers tests
 */
export class MockKVNamespace implements MockStorage {
  private store = new Map<string, Uint8Array>()
  private expirations = new Map<string, number>()

  async get(key: string): Promise<Uint8Array | null> {
    // Check expiration
    const expiration = this.expirations.get(key)
    if (expiration && Date.now() > expiration) {
      this.store.delete(key)
      this.expirations.delete(key)
      return null
    }

    const value = this.store.get(key)
    return value ? new Uint8Array(value) : null
  }

  async put(key: string, value: Uint8Array, options?: { expirationTtl?: number }): Promise<void> {
    this.store.set(key, new Uint8Array(value))
    if (options?.expirationTtl) {
      this.expirations.set(key, Date.now() + options.expirationTtl * 1000)
    }
  }

  async delete(key: string): Promise<boolean> {
    this.expirations.delete(key)
    return this.store.delete(key)
  }

  async list(prefix?: string): Promise<string[]> {
    const keys = Array.from(this.store.keys())
    if (prefix) {
      return keys.filter(key => key.startsWith(prefix))
    }
    return keys
  }

  async clear(): Promise<void> {
    this.store.clear()
    this.expirations.clear()
  }

  size(): number {
    return this.store.size
  }

  // KV-specific methods
  async getWithMetadata(key: string): Promise<{ value: Uint8Array | null; metadata: null }> {
    const value = await this.get(key)
    return { value, metadata: null }
  }
}

/**
 * Mock IndexedDB storage for browser tests
 */
export class MockIndexedDBStorage implements MockStorage {
  private store = new Map<string, Uint8Array>()

  async get(key: string): Promise<Uint8Array | null> {
    const value = this.store.get(key)
    return value ? new Uint8Array(value) : null
  }

  async put(key: string, value: Uint8Array): Promise<void> {
    this.store.set(key, new Uint8Array(value))
  }

  async delete(key: string): Promise<boolean> {
    return this.store.delete(key)
  }

  async list(prefix?: string): Promise<string[]> {
    const keys = Array.from(this.store.keys())
    if (prefix) {
      return keys.filter(key => key.startsWith(prefix))
    }
    return keys
  }

  async clear(): Promise<void> {
    this.store.clear()
  }

  size(): number {
    return this.store.size
  }
}

/**
 * Mock file system storage for Node.js tests
 */
export class MockFileSystemStorage implements MockStorage {
  private store = new Map<string, Uint8Array>()

  async get(key: string): Promise<Uint8Array | null> {
    const value = this.store.get(key)
    return value ? new Uint8Array(value) : null
  }

  async put(key: string, value: Uint8Array): Promise<void> {
    this.store.set(key, new Uint8Array(value))
  }

  async delete(key: string): Promise<boolean> {
    return this.store.delete(key)
  }

  async list(prefix?: string): Promise<string[]> {
    const keys = Array.from(this.store.keys())
    if (prefix) {
      return keys.filter(key => key.startsWith(prefix))
    }
    return keys
  }

  async clear(): Promise<void> {
    this.store.clear()
  }

  size(): number {
    return this.store.size
  }
}

/**
 * Create a storage mock appropriate for the current environment
 */
export function createMockStorage(): MockStorage {
  const env = getEnvironment()
  switch (env) {
    case 'node':
      return new MockFileSystemStorage()
    case 'browser':
      return new MockIndexedDBStorage()
    case 'workers':
      return new MockR2Bucket()
    default:
      return new MemoryStorageMock()
  }
}

/**
 * Create a fresh storage instance for each test
 */
export function useMockStorage(): MockStorage {
  const storage = createMockStorage()

  // Automatically clean up after each test
  afterEach(async () => {
    await storage.clear()
  })

  return storage
}
