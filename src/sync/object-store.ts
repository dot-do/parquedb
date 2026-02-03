/**
 * Object Store - Content-addressed storage for ParqueDB
 *
 * Stores objects (Parquet files, schemas, etc.) by their content hash.
 * Used for version control operations like checkout/commit.
 *
 * Storage layout:
 *   _meta/objects/{first-2-chars}/{full-hash}
 *
 * Example:
 *   _meta/objects/ab/abcdef1234567890...
 */

import type { StorageBackend } from '../types/storage'
import { sha256 } from './hash'

// =============================================================================
// Types
// =============================================================================

/**
 * Object store interface for content-addressed storage
 */
export interface ObjectStore {
  /**
   * Save data and return its content hash
   * @param data Data to store
   * @returns Content hash (SHA256)
   */
  save(data: Uint8Array): Promise<string>

  /**
   * Load data by content hash
   * @param hash Content hash
   * @returns Data
   * @throws If object not found
   */
  load(hash: string): Promise<Uint8Array>

  /**
   * Check if object exists
   * @param hash Content hash
   * @returns True if object exists
   */
  exists(hash: string): Promise<boolean>
}

// =============================================================================
// Hash Computation
// =============================================================================

/**
 * Compute SHA256 hash of binary data
 * @param data Data to hash
 * @returns Hex-encoded SHA256 hash
 */
export function computeObjectHash(data: Uint8Array): string {
  return sha256(data)
}

// =============================================================================
// Object Path Helpers
// =============================================================================

/**
 * Get storage path for an object
 * Objects are stored as _meta/objects/{first-2-chars}/{full-hash}
 *
 * @param hash Object hash
 * @returns Storage path
 */
export function getObjectPath(hash: string): string {
  const prefix = hash.slice(0, 2)
  return `_meta/objects/${prefix}/${hash}`
}

// =============================================================================
// Object Store Implementation
// =============================================================================

/**
 * Content-addressed object store backed by a StorageBackend
 */
class ObjectStoreImpl implements ObjectStore {
  constructor(private storage: StorageBackend) {}

  /**
   * Save data and return its content hash
   */
  async save(data: Uint8Array): Promise<string> {
    const hash = computeObjectHash(data)
    const path = getObjectPath(hash)

    // Only write if object doesn't already exist (content-addressed deduplication)
    const exists = await this.storage.exists(path)
    if (!exists) {
      await this.storage.write(path, data)
    }

    return hash
  }

  /**
   * Load data by content hash
   */
  async load(hash: string): Promise<Uint8Array> {
    const path = getObjectPath(hash)
    const exists = await this.storage.exists(path)

    if (!exists) {
      throw new Error(`Object not found: ${hash}`)
    }

    return this.storage.read(path)
  }

  /**
   * Check if object exists
   */
  async exists(hash: string): Promise<boolean> {
    const path = getObjectPath(hash)
    return this.storage.exists(path)
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create an object store backed by the given storage backend
 * @param storage StorageBackend to use
 * @returns ObjectStore instance
 */
export function createObjectStore(storage: StorageBackend): ObjectStore {
  return new ObjectStoreImpl(storage)
}

// =============================================================================
// Standalone Functions (for backwards compatibility)
// =============================================================================

/**
 * Save object to storage and return its hash
 * @param storage StorageBackend to use
 * @param data Data to store
 * @returns Content hash
 */
export async function saveObject(storage: StorageBackend, data: Uint8Array): Promise<string> {
  const store = createObjectStore(storage)
  return store.save(data)
}

/**
 * Load object from storage by hash
 * @param storage StorageBackend to use
 * @param hash Content hash
 * @returns Data
 * @throws If object not found
 */
export async function loadObject(storage: StorageBackend, hash: string): Promise<Uint8Array> {
  const store = createObjectStore(storage)
  return store.load(hash)
}
