/**
 * ObservedBackend - Storage backend wrapper with observability hooks
 *
 * Wraps any StorageBackend implementation to dispatch observability hooks
 * for all read/write/delete operations.
 */

import type {
  StorageBackend,
  FileStat,
  ListOptions,
  ListResult,
  WriteOptions,
  WriteResult,
  RmdirOptions,
  MultipartBackend,
  MultipartUpload,
  UploadedPart,
} from '../types/storage'
import { isMultipart } from '../types/storage'
import {
  globalHookRegistry,
  createStorageContext,
  type StorageResult,
  type StorageContext,
} from '../observability'
import { getGlobalTelemetry } from '../observability/telemetry'

/**
 * Wraps a storage backend with observability hooks
 */
export class ObservedBackend implements StorageBackend {
  readonly type: string

  constructor(private backend: StorageBackend) {
    this.type = `observed:${backend.type}`
  }

  /**
   * Get the underlying backend
   */
  get inner(): StorageBackend {
    return this.backend
  }

  // =========================================================================
  // Read Operations
  // =========================================================================

  async read(path: string): Promise<Uint8Array> {
    const startTime = Date.now()
    const context = createStorageContext('read', path)

    try {
      const data = await this.backend.read(path)

      const result: StorageResult = {
        bytesTransferred: data.length,
        durationMs: Date.now() - startTime,
      }
      await globalHookRegistry.dispatchStorageRead(context, result)

      return data
    } catch (error) {
      await globalHookRegistry.dispatchStorageError(
        context,
        error instanceof Error ? error : new Error(String(error))
      )
      throw error
    }
  }

  async readRange(path: string, start: number, end: number): Promise<Uint8Array> {
    const startTime = Date.now()
    const context = createStorageContext('readRange', path, { start, end })

    try {
      const data = await this.backend.readRange(path, start, end)

      const result: StorageResult = {
        bytesTransferred: data.length,
        durationMs: Date.now() - startTime,
      }
      await globalHookRegistry.dispatchStorageRead(context, result)

      return data
    } catch (error) {
      await globalHookRegistry.dispatchStorageError(
        context,
        error instanceof Error ? error : new Error(String(error))
      )
      throw error
    }
  }

  async exists(path: string): Promise<boolean> {
    // exists is a lightweight check, no hooks needed
    return this.backend.exists(path)
  }

  async stat(path: string): Promise<FileStat | null> {
    // stat is a lightweight check, no hooks needed
    return this.backend.stat(path)
  }

  async list(prefix: string, options?: ListOptions): Promise<ListResult> {
    const startTime = Date.now()
    const context = createStorageContext('list', prefix)

    try {
      const listResult = await this.backend.list(prefix, options)

      const result: StorageResult = {
        bytesTransferred: 0,
        durationMs: Date.now() - startTime,
        fileCount: listResult.files.length,
      }
      await globalHookRegistry.dispatchStorageRead(context, result)

      return listResult
    } catch (error) {
      await globalHookRegistry.dispatchStorageError(
        context,
        error instanceof Error ? error : new Error(String(error))
      )
      throw error
    }
  }

  // =========================================================================
  // Write Operations
  // =========================================================================

  async write(path: string, data: Uint8Array, options?: WriteOptions): Promise<WriteResult> {
    const startTime = Date.now()
    const context = createStorageContext('write', path)

    try {
      const writeResult = await this.backend.write(path, data, options)

      const durationMs = Date.now() - startTime
      const result: StorageResult = {
        bytesTransferred: data.length,
        durationMs,
        etag: writeResult.etag,
      }
      await globalHookRegistry.dispatchStorageWrite(context, result)

      // Record write throughput telemetry
      const ns = extractNamespaceFromPath(path)
      getGlobalTelemetry().recordWrite(ns, data.length, durationMs)

      return writeResult
    } catch (error) {
      await globalHookRegistry.dispatchStorageError(
        context,
        error instanceof Error ? error : new Error(String(error))
      )
      throw error
    }
  }

  async writeAtomic(path: string, data: Uint8Array, options?: WriteOptions): Promise<WriteResult> {
    const startTime = Date.now()
    const context = createStorageContext('writeAtomic', path)

    try {
      const writeResult = await this.backend.writeAtomic(path, data, options)

      const durationMs = Date.now() - startTime
      const result: StorageResult = {
        bytesTransferred: data.length,
        durationMs,
        etag: writeResult.etag,
      }
      await globalHookRegistry.dispatchStorageWrite(context, result)

      // Record write throughput telemetry
      const ns = extractNamespaceFromPath(path)
      getGlobalTelemetry().recordWrite(ns, data.length, durationMs)

      return writeResult
    } catch (error) {
      await globalHookRegistry.dispatchStorageError(
        context,
        error instanceof Error ? error : new Error(String(error))
      )
      throw error
    }
  }

  async append(path: string, data: Uint8Array): Promise<void> {
    const startTime = Date.now()
    const context = createStorageContext('append', path)

    try {
      await this.backend.append(path, data)

      const durationMs = Date.now() - startTime
      const result: StorageResult = {
        bytesTransferred: data.length,
        durationMs,
      }
      await globalHookRegistry.dispatchStorageWrite(context, result)

      // Record write throughput telemetry
      const ns = extractNamespaceFromPath(path)
      getGlobalTelemetry().recordWrite(ns, data.length, durationMs)
    } catch (error) {
      await globalHookRegistry.dispatchStorageError(
        context,
        error instanceof Error ? error : new Error(String(error))
      )
      throw error
    }
  }

  async delete(path: string): Promise<boolean> {
    const startTime = Date.now()
    const context = createStorageContext('delete', path)

    try {
      const deleted = await this.backend.delete(path)

      const result: StorageResult = {
        bytesTransferred: 0,
        durationMs: Date.now() - startTime,
        fileCount: deleted ? 1 : 0,
      }
      await globalHookRegistry.dispatchStorageDelete(context, result)

      return deleted
    } catch (error) {
      await globalHookRegistry.dispatchStorageError(
        context,
        error instanceof Error ? error : new Error(String(error))
      )
      throw error
    }
  }

  async deletePrefix(prefix: string): Promise<number> {
    const startTime = Date.now()
    const context = createStorageContext('deletePrefix', prefix)

    try {
      const count = await this.backend.deletePrefix(prefix)

      const result: StorageResult = {
        bytesTransferred: 0,
        durationMs: Date.now() - startTime,
        fileCount: count,
      }
      await globalHookRegistry.dispatchStorageDelete(context, result)

      return count
    } catch (error) {
      await globalHookRegistry.dispatchStorageError(
        context,
        error instanceof Error ? error : new Error(String(error))
      )
      throw error
    }
  }

  // =========================================================================
  // Directory Operations
  // =========================================================================

  async mkdir(path: string): Promise<void> {
    // mkdir is a lightweight operation, no hooks needed
    return this.backend.mkdir(path)
  }

  async rmdir(path: string, options?: RmdirOptions): Promise<void> {
    const startTime = Date.now()
    const context = createStorageContext('delete', path)

    try {
      await this.backend.rmdir(path, options)

      const result: StorageResult = {
        bytesTransferred: 0,
        durationMs: Date.now() - startTime,
        fileCount: 1,
      }
      await globalHookRegistry.dispatchStorageDelete(context, result)
    } catch (error) {
      await globalHookRegistry.dispatchStorageError(
        context,
        error instanceof Error ? error : new Error(String(error))
      )
      throw error
    }
  }

  // =========================================================================
  // Atomic Operations
  // =========================================================================

  async writeConditional(
    path: string,
    data: Uint8Array,
    expectedVersion: string | null,
    options?: WriteOptions
  ): Promise<WriteResult> {
    const startTime = Date.now()
    const context = createStorageContext('writeAtomic', path)

    try {
      const writeResult = await this.backend.writeConditional(path, data, expectedVersion, options)

      const result: StorageResult = {
        bytesTransferred: data.length,
        durationMs: Date.now() - startTime,
        etag: writeResult.etag,
      }
      await globalHookRegistry.dispatchStorageWrite(context, result)

      return writeResult
    } catch (error) {
      await globalHookRegistry.dispatchStorageError(
        context,
        error instanceof Error ? error : new Error(String(error))
      )
      throw error
    }
  }

  async copy(source: string, dest: string): Promise<void> {
    const startTime = Date.now()
    const context = createStorageContext('copy', `${source} -> ${dest}`)

    try {
      await this.backend.copy(source, dest)

      const result: StorageResult = {
        bytesTransferred: 0, // We don't know the size without reading
        durationMs: Date.now() - startTime,
      }
      await globalHookRegistry.dispatchStorageWrite(context, result)
    } catch (error) {
      await globalHookRegistry.dispatchStorageError(
        context,
        error instanceof Error ? error : new Error(String(error))
      )
      throw error
    }
  }

  async move(source: string, dest: string): Promise<void> {
    const startTime = Date.now()
    const context = createStorageContext('move', `${source} -> ${dest}`)

    try {
      await this.backend.move(source, dest)

      const result: StorageResult = {
        bytesTransferred: 0, // We don't know the size without reading
        durationMs: Date.now() - startTime,
      }
      await globalHookRegistry.dispatchStorageWrite(context, result)
    } catch (error) {
      await globalHookRegistry.dispatchStorageError(
        context,
        error instanceof Error ? error : new Error(String(error))
      )
      throw error
    }
  }

  // =========================================================================
  // Multipart Upload Operations
  // =========================================================================

  /**
   * Create a multipart upload (only available if inner backend supports it)
   */
  async createMultipartUpload(path: string, options?: WriteOptions): Promise<MultipartUpload> {
    if (!isMultipart(this.backend)) {
      throw new Error('Inner backend does not support multipart uploads')
    }

    const startTime = Date.now()
    // Use a custom operation type for multipart creation - cast as 'write' for the context
    const context = createStorageContext('write' as StorageContext['operationType'], path)
    context.metadata = { multipartOperation: 'create' }

    try {
      const upload = await this.backend.createMultipartUpload(path, options)

      const result: StorageResult = {
        bytesTransferred: 0,
        durationMs: Date.now() - startTime,
      }
      await globalHookRegistry.dispatchStorageWrite(context, result)

      // Wrap the upload with observability
      return new ObservedMultipartUpload(upload, path)
    } catch (error) {
      await globalHookRegistry.dispatchStorageError(
        context,
        error instanceof Error ? error : new Error(String(error))
      )
      throw error
    }
  }
}

/**
 * Wrapper for MultipartUpload that adds observability hooks
 */
class ObservedMultipartUpload implements MultipartUpload {
  readonly uploadId: string

  constructor(
    private readonly inner: MultipartUpload,
    private readonly path: string
  ) {
    this.uploadId = inner.uploadId
  }

  async uploadPart(partNumber: number, data: Uint8Array): Promise<UploadedPart> {
    const startTime = Date.now()
    // Use a custom operation type - need to extend StorageContext type or use metadata
    const context = createStorageContext('write' as StorageContext['operationType'], this.path)
    // Override the operationType to a custom value for multipart operations
    ;(context as { operationType: string }).operationType = 'uploadPart'
    context.metadata = { partNumber, uploadId: this.uploadId }

    try {
      const part = await this.inner.uploadPart(partNumber, data)

      const result: StorageResult = {
        bytesTransferred: data.length,
        durationMs: Date.now() - startTime,
        etag: part.etag,
      }
      await globalHookRegistry.dispatchStorageWrite(context, result)

      return part
    } catch (error) {
      await globalHookRegistry.dispatchStorageError(
        context,
        error instanceof Error ? error : new Error(String(error))
      )
      throw error
    }
  }

  async complete(parts: UploadedPart[]): Promise<WriteResult> {
    const startTime = Date.now()
    const context = createStorageContext('write' as StorageContext['operationType'], this.path)
    ;(context as { operationType: string }).operationType = 'multipartComplete'
    context.metadata = { uploadId: this.uploadId, partCount: parts.length }

    try {
      const result = await this.inner.complete(parts)

      const hookResult: StorageResult = {
        bytesTransferred: result.size,
        durationMs: Date.now() - startTime,
        etag: result.etag,
      }
      await globalHookRegistry.dispatchStorageWrite(context, hookResult)

      return result
    } catch (error) {
      await globalHookRegistry.dispatchStorageError(
        context,
        error instanceof Error ? error : new Error(String(error))
      )
      throw error
    }
  }

  async abort(): Promise<void> {
    const startTime = Date.now()
    const context = createStorageContext('delete' as StorageContext['operationType'], this.path)
    ;(context as { operationType: string }).operationType = 'multipartAbort'
    context.metadata = { uploadId: this.uploadId }

    try {
      await this.inner.abort()

      const result: StorageResult = {
        bytesTransferred: 0,
        durationMs: Date.now() - startTime,
        fileCount: 0,
      }
      await globalHookRegistry.dispatchStorageDelete(context, result)
    } catch (error) {
      await globalHookRegistry.dispatchStorageError(
        context,
        error instanceof Error ? error : new Error(String(error))
      )
      throw error
    }
  }
}

/**
 * Wrap a storage backend with observability hooks
 *
 * If the backend supports multipart uploads (MultipartBackend), the wrapped
 * backend will also expose createMultipartUpload with observability hooks.
 * Use isMultipart() to check if the returned backend supports multipart.
 */
export function withObservability(backend: StorageBackend): ObservedBackend
export function withObservability(backend: MultipartBackend): ObservedBackend & MultipartBackend
export function withObservability(backend: StorageBackend): ObservedBackend {
  // Don't double-wrap
  if (backend instanceof ObservedBackend) {
    return backend
  }
  return new ObservedBackend(backend)
}

// =============================================================================
// Helpers
// =============================================================================

/**
 * Extract namespace from storage path
 * e.g., 'data/posts/data.parquet' -> 'posts'
 *       'events/current.parquet' -> 'events'
 *       'rels/forward/users.parquet' -> 'users'
 */
function extractNamespaceFromPath(path: string): string {
  const parts = path.split('/')
  if (parts.length >= 2) {
    // For 'data/{ns}/...', extract ns
    if (parts[0] === 'data') return parts[1] ?? 'unknown'
    // For 'rels/forward/{ns}.parquet', extract ns
    if (parts[0] === 'rels' && parts.length >= 3) {
      return (parts[2] ?? '').replace(/\.parquet$/, '')
    }
    // For 'events/...', use 'events'
    if (parts[0] === 'events') return 'events'
  }
  return parts[0] || 'unknown'
}
