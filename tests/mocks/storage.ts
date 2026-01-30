/**
 * Mock storage implementations for testing ParqueDB
 *
 * These mocks provide consistent behavior across all test environments
 * (Node.js, browser, and Cloudflare Workers).
 */

import { getEnvironment } from '../setup'

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
