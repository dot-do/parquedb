/**
 * R2Backend - Cloudflare R2 implementation of StorageBackend
 *
 * Provides storage operations using Cloudflare R2 object storage.
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
import type {
  R2Bucket,
  R2PutOptions,
  R2MultipartUpload,
  R2UploadedPart,
} from './types/r2'
import { validateRange, InvalidRangeError } from './validation'
import { MIN_PART_SIZE, DEFAULT_PART_SIZE, MAX_PARTS } from '../constants'

/**
 * Error thrown when an R2 operation fails
 */
export class R2OperationError extends Error {
  override readonly name = 'R2OperationError'
  readonly operation: string
  readonly key?: string
  readonly underlyingCause?: Error

  constructor(
    message: string,
    operation: string,
    key?: string,
    cause?: Error
  ) {
    super(message)
    Object.setPrototypeOf(this, R2OperationError.prototype)
    this.operation = operation
    this.key = key
    this.underlyingCause = cause
  }
}

/**
 * Error thrown when a conditional write fails due to ETag mismatch
 */
export class R2ETagMismatchError extends Error {
  override readonly name = 'R2ETagMismatchError'
  constructor(
    public readonly key: string,
    public readonly expectedEtag: string | null,
    public readonly actualEtag: string | null
  ) {
    super(`ETag mismatch for ${key}: expected ${expectedEtag}, got ${actualEtag}`)
    Object.setPrototypeOf(this, R2ETagMismatchError.prototype)
  }
}

/**
 * Error thrown when an object is not found
 */
export class R2NotFoundError extends Error {
  override readonly name = 'R2NotFoundError'
  constructor(public readonly key: string) {
    super(`Object not found: ${key}`)
    Object.setPrototypeOf(this, R2NotFoundError.prototype)
  }
}

/**
 * Configuration options for R2Backend
 */
export interface R2BackendOptions {
  /** Prefix for all keys (optional) */
  prefix?: string
}

/**
 * Cloudflare R2 storage backend
 */
export class R2Backend implements StorageBackend, MultipartBackend {
  readonly type = 'r2'
  private readonly bucket: R2Bucket
  private readonly prefix: string

  constructor(bucket: R2Bucket, options?: R2BackendOptions) {
    this.bucket = bucket
    // Normalize prefix: ensure it ends with / if provided
    const rawPrefix = options?.prefix ?? ''
    this.prefix = rawPrefix && !rawPrefix.endsWith('/') ? rawPrefix + '/' : rawPrefix
  }

  /**
   * Apply prefix to a path
   */
  private withPrefix(path: string): string {
    return this.prefix + path
  }

  /**
   * Remove prefix from a path
   */
  private withoutPrefix(path: string): string {
    if (this.prefix && path.startsWith(this.prefix)) {
      return path.slice(this.prefix.length)
    }
    return path
  }

  async read(path: string): Promise<Uint8Array> {
    const key = this.withPrefix(path)
    try {
      const obj = await this.bucket.get(key, undefined)
      if (!obj) {
        throw new R2NotFoundError(path)
      }
      return new Uint8Array(await obj.arrayBuffer())
    } catch (error: unknown) {
      if (error instanceof R2NotFoundError) {
        throw error
      }
      const err = error instanceof Error ? error : new Error(String(error))
      throw new R2OperationError(
        `Failed to read ${path}: ${err.message}`,
        'read',
        path,
        err
      )
    }
  }

  async readRange(path: string, start: number, end: number): Promise<Uint8Array> {
    // Validate range parameters using shared validation
    try {
      validateRange(start, end)
    } catch (error) {
      if (error instanceof InvalidRangeError) {
        throw new R2OperationError(error.message, 'readRange', path)
      }
      throw error
    }

    const key = this.withPrefix(path)
    const length = end - start + 1 // end is inclusive

    try {
      const obj = await this.bucket.get(key, {
        range: { offset: start, length },
      })
      if (!obj) {
        throw new R2NotFoundError(path)
      }
      return new Uint8Array(await obj.arrayBuffer())
    } catch (error: unknown) {
      if (error instanceof R2NotFoundError) {
        throw error
      }
      const err = error instanceof Error ? error : new Error(String(error))
      throw new R2OperationError(
        `Failed to read range ${start}-${end} from ${path}: ${err.message}`,
        'readRange',
        path,
        err
      )
    }
  }

  async exists(path: string): Promise<boolean> {
    const key = this.withPrefix(path)
    try {
      const obj = await this.bucket.head(key)
      return obj !== null
    } catch (error: unknown) {
      const err = error instanceof Error ? error : new Error(String(error))
      throw new R2OperationError(
        `Failed to check existence of ${path}: ${err.message}`,
        'exists',
        path,
        err
      )
    }
  }

  async stat(path: string): Promise<FileStat | null> {
    const key = this.withPrefix(path)
    try {
      const obj = await this.bucket.head(key)
      if (!obj) {
        return null
      }

      return {
        path: path, // Return path without prefix
        size: obj.size,
        mtime: obj.uploaded,
        isDirectory: false,
        etag: obj.httpEtag,
        contentType: obj.httpMetadata?.contentType,
        metadata: obj.customMetadata,
      }
    } catch (error: unknown) {
      const err = error instanceof Error ? error : new Error(String(error))
      throw new R2OperationError(
        `Failed to stat ${path}: ${err.message}`,
        'stat',
        path,
        err
      )
    }
  }

  async list(prefix: string, options?: ListOptions): Promise<ListResult> {
    const fullPrefix = this.withPrefix(prefix)

    try {
      const r2Options: {
        prefix: string
        limit?: number
        cursor?: string
        delimiter?: string
        include?: ('httpMetadata' | 'customMetadata')[]
      } = {
        prefix: fullPrefix,
      }

      if (options?.limit !== undefined) {
        r2Options.limit = options.limit
      }
      if (options?.cursor !== undefined) {
        r2Options.cursor = options.cursor
      }
      if (options?.delimiter !== undefined) {
        r2Options.delimiter = options.delimiter
      }
      if (options?.includeMetadata) {
        r2Options.include = ['httpMetadata', 'customMetadata']
      }

      const result = await this.bucket.list(r2Options)

      const files = result.objects.map(obj => this.withoutPrefix(obj.key))
      const prefixes = result.delimitedPrefixes.length > 0
        ? result.delimitedPrefixes.map(p => this.withoutPrefix(p))
        : undefined

      const listResult: ListResult = {
        files,
        prefixes,
        cursor: result.truncated ? result.cursor : undefined,
        hasMore: result.truncated,
      }

      // Include stats if metadata was requested
      if (options?.includeMetadata) {
        listResult.stats = result.objects.map(obj => ({
          path: this.withoutPrefix(obj.key),
          size: obj.size,
          mtime: obj.uploaded,
          isDirectory: false,
          etag: obj.httpEtag,
          contentType: obj.httpMetadata?.contentType,
          metadata: obj.customMetadata,
        }))
      }

      return listResult
    } catch (error: unknown) {
      const err = error instanceof Error ? error : new Error(String(error))
      throw new R2OperationError(
        `Failed to list ${prefix}: ${err.message}`,
        'list',
        prefix,
        err
      )
    }
  }

  /**
   * Build R2PutOptions from WriteOptions
   */
  private buildPutOptions(options?: WriteOptions): R2PutOptions | undefined {
    if (!options) {
      return undefined
    }

    const r2Options: R2PutOptions = {}
    let hasOptions = false

    // Handle content type and cache control
    if (options.contentType || options.cacheControl) {
      r2Options.httpMetadata = {}
      if (options.contentType) {
        r2Options.httpMetadata.contentType = options.contentType
      }
      if (options.cacheControl) {
        r2Options.httpMetadata.cacheControl = options.cacheControl
      }
      hasOptions = true
    }

    // Handle custom metadata
    if (options.metadata) {
      r2Options.customMetadata = options.metadata
      hasOptions = true
    }

    // Handle conditional writes
    if (options.ifMatch) {
      r2Options.onlyIf = { etagMatches: options.ifMatch }
      hasOptions = true
    } else if (options.ifNoneMatch) {
      r2Options.onlyIf = { etagDoesNotMatch: options.ifNoneMatch }
      hasOptions = true
    }

    return hasOptions ? r2Options : undefined
  }

  async write(path: string, data: Uint8Array, options?: WriteOptions): Promise<WriteResult> {
    const key = this.withPrefix(path)
    const r2Options = this.buildPutOptions(options)

    try {
      const result = await this.bucket.put(key, data, r2Options)

      if (!result) {
        // This can happen with conditional writes when condition fails
        throw new R2ETagMismatchError(path, options?.ifMatch ?? null, null)
      }

      return {
        etag: result.etag,
        size: result.size,
        versionId: result.version,
      }
    } catch (error: unknown) {
      if (error instanceof R2ETagMismatchError) {
        throw error
      }
      const err = error instanceof Error ? error : new Error(String(error))
      throw new R2OperationError(
        `Failed to write ${path}: ${err.message}`,
        'write',
        path,
        err
      )
    }
  }

  async writeAtomic(path: string, data: Uint8Array, options?: WriteOptions): Promise<WriteResult> {
    // R2 puts are inherently atomic, so this is the same as write
    return this.write(path, data, options)
  }

  async append(path: string, data: Uint8Array): Promise<void> {
    const key = this.withPrefix(path)

    try {
      // Try to read existing content
      const existing = await this.bucket.get(key, undefined)

      let newData: Uint8Array
      if (existing) {
        const existingBuffer = await existing.arrayBuffer()
        const existingData = new Uint8Array(existingBuffer)
        newData = new Uint8Array(existingData.length + data.length)
        newData.set(existingData)
        newData.set(data, existingData.length)
      } else {
        newData = data
      }

      // Write the combined data
      await this.bucket.put(key, newData, {})
    } catch (error: unknown) {
      const err = error instanceof Error ? error : new Error(String(error))
      throw new R2OperationError(
        `Failed to append to ${path}: ${err.message}`,
        'append',
        path,
        err
      )
    }
  }

  async delete(path: string): Promise<boolean> {
    const key = this.withPrefix(path)

    try {
      // Check if object exists first to determine return value
      const exists = await this.bucket.head(key)

      // Delete the object (R2 delete is idempotent)
      await this.bucket.delete(key)

      return exists !== null
    } catch (error: unknown) {
      const err = error instanceof Error ? error : new Error(String(error))
      throw new R2OperationError(
        `Failed to delete ${path}: ${err.message}`,
        'delete',
        path,
        err
      )
    }
  }

  async deletePrefix(prefix: string): Promise<number> {
    const fullPrefix = this.withPrefix(prefix)

    try {
      let totalDeleted = 0
      let cursor: string | undefined

      do {
        const listOptions: { prefix: string; cursor?: string } = { prefix: fullPrefix }
        if (cursor) {
          listOptions.cursor = cursor
        }

        const result = await this.bucket.list(listOptions)

        if (result.objects.length > 0) {
          const keys = result.objects.map(obj => obj.key)
          await this.bucket.delete(keys)
          totalDeleted += keys.length
        }

        cursor = result.truncated ? result.cursor : undefined
      } while (cursor)

      return totalDeleted
    } catch (error: unknown) {
      const err = error instanceof Error ? error : new Error(String(error))
      throw new R2OperationError(
        `Failed to delete prefix ${prefix}: ${err.message}`,
        'deletePrefix',
        prefix,
        err
      )
    }
  }

  async mkdir(_path: string): Promise<void> {
    // R2/S3 doesn't have real directories, so mkdir is a no-op
    return
  }

  async rmdir(path: string, options?: RmdirOptions): Promise<void> {
    if (options?.recursive) {
      // Delete all objects with this prefix
      await this.deletePrefix(path)
    }
    // Non-recursive rmdir is a no-op for R2
  }

  async writeConditional(
    path: string,
    data: Uint8Array,
    expectedVersion: string | null,
    options?: WriteOptions
  ): Promise<WriteResult> {
    const key = this.withPrefix(path)

    // Build put options with conditional
    const r2Options: R2PutOptions = {}

    if (expectedVersion !== null) {
      // Only write if etag matches expected version
      r2Options.onlyIf = { etagMatches: expectedVersion }
    } else {
      // Only write if object doesn't exist
      r2Options.onlyIf = { etagDoesNotMatch: '*' }
    }

    // Add other options
    if (options?.contentType || options?.cacheControl) {
      r2Options.httpMetadata = {}
      if (options?.contentType) {
        r2Options.httpMetadata.contentType = options.contentType
      }
      if (options?.cacheControl) {
        r2Options.httpMetadata.cacheControl = options.cacheControl
      }
    }
    if (options?.metadata) {
      r2Options.customMetadata = options.metadata
    }

    try {
      const result = await this.bucket.put(key, data, r2Options)

      if (!result) {
        // Precondition failed
        throw new R2ETagMismatchError(path, expectedVersion, null)
      }

      return {
        etag: result.etag,
        size: result.size,
        versionId: result.version,
      }
    } catch (error: unknown) {
      if (error instanceof R2ETagMismatchError) {
        throw error
      }
      const err = error instanceof Error ? error : new Error(String(error))
      throw new R2OperationError(
        `Failed conditional write to ${path}: ${err.message}`,
        'writeConditional',
        path,
        err
      )
    }
  }

  async copy(source: string, dest: string): Promise<void> {
    const sourceKey = this.withPrefix(source)
    const destKey = this.withPrefix(dest)

    try {
      // Read source object with metadata
      const sourceObj = await this.bucket.get(sourceKey, undefined)
      if (!sourceObj) {
        throw new R2NotFoundError(source)
      }

      const data = new Uint8Array(await sourceObj.arrayBuffer())

      // Write to destination with same metadata
      const putOptions: R2PutOptions = {}
      if (sourceObj.httpMetadata || sourceObj.customMetadata) {
        if (sourceObj.httpMetadata) {
          putOptions.httpMetadata = sourceObj.httpMetadata
        }
        if (sourceObj.customMetadata) {
          putOptions.customMetadata = sourceObj.customMetadata
        }
      }

      await this.bucket.put(destKey, data, Object.keys(putOptions).length > 0 ? putOptions : undefined)
    } catch (error: unknown) {
      if (error instanceof R2NotFoundError) {
        throw error
      }
      const err = error instanceof Error ? error : new Error(String(error))
      throw new R2OperationError(
        `Failed to copy ${source} to ${dest}: ${err.message}`,
        'copy',
        source,
        err
      )
    }
  }

  async move(source: string, dest: string): Promise<void> {
    const sourceKey = this.withPrefix(source)

    try {
      // Copy first
      await this.copy(source, dest)

      // Then delete source
      await this.bucket.delete(sourceKey)
    } catch (error: unknown) {
      if (error instanceof R2NotFoundError) {
        throw error
      }
      const err = error instanceof Error ? error : new Error(String(error))
      throw new R2OperationError(
        `Failed to move ${source} to ${dest}: ${err.message}`,
        'move',
        source,
        err
      )
    }
  }

  async createMultipartUpload(path: string, options?: WriteOptions): Promise<MultipartUpload> {
    const key = this.withPrefix(path)

    try {
      // Build multipart options
      let r2Options: { httpMetadata?: { contentType?: string }; customMetadata?: Record<string, string> } | undefined
      if (options?.contentType || options?.metadata) {
        r2Options = {}
        if (options.contentType) {
          r2Options.httpMetadata = { contentType: options.contentType }
        }
        if (options.metadata) {
          r2Options.customMetadata = options.metadata
        }
      }

      const r2Upload = await this.bucket.createMultipartUpload(key, r2Options)

      return new R2MultipartUploadWrapper(r2Upload)
    } catch (error: unknown) {
      const err = error instanceof Error ? error : new Error(String(error))
      throw new R2OperationError(
        `Failed to create multipart upload for ${path}: ${err.message}`,
        'createMultipartUpload',
        path,
        err
      )
    }
  }

  // ===========================================================================
  // Standalone Multipart Upload Methods
  // ===========================================================================

  /**
   * Track active multipart uploads by uploadId
   * Maps uploadId -> R2MultipartUpload instance
   */
  private activeUploads = new Map<string, R2MultipartUpload>()

  /**
   * Create a multipart upload session
   * Returns the uploadId for use with other multipart methods
   *
   * @param path - The object path/key
   * @returns The upload ID for this multipart upload session
   */
  async startMultipartUpload(path: string): Promise<string> {
    const key = this.withPrefix(path)

    try {
      const r2Upload = await this.bucket.createMultipartUpload(key, undefined)
      this.activeUploads.set(r2Upload.uploadId, r2Upload)
      return r2Upload.uploadId
    } catch (error: unknown) {
      const err = error instanceof Error ? error : new Error(String(error))
      throw new R2OperationError(
        `Failed to create multipart upload for ${path}: ${err.message}`,
        'startMultipartUpload',
        path,
        err
      )
    }
  }

  /**
   * Upload a part for a multipart upload
   * Parts can be 5MB to 5GB, minimum 5MB except for the last part
   *
   * @param path - The object path/key
   * @param uploadId - The upload ID from startMultipartUpload
   * @param partNumber - Part number (1 to 10000)
   * @param data - The data for this part
   * @returns Object containing the ETag for this part
   */
  async uploadPart(
    path: string,
    uploadId: string,
    partNumber: number,
    data: Uint8Array
  ): Promise<{ etag: string }> {
    const upload = this.activeUploads.get(uploadId)
    if (!upload) {
      throw new R2OperationError(
        `No active upload found with ID ${uploadId}`,
        'uploadPart',
        path
      )
    }

    try {
      const part = await upload.uploadPart(partNumber, data)
      return { etag: part.etag }
    } catch (error: unknown) {
      const err = error instanceof Error ? error : new Error(String(error))
      throw new R2OperationError(
        `Failed to upload part ${partNumber} for ${path}: ${err.message}`,
        'uploadPart',
        path,
        err
      )
    }
  }

  /**
   * Complete a multipart upload
   *
   * @param path - The object path/key
   * @param uploadId - The upload ID from startMultipartUpload
   * @param parts - Array of uploaded parts with partNumber and etag
   */
  async completeMultipartUpload(
    path: string,
    uploadId: string,
    parts: Array<{ partNumber: number; etag: string }>
  ): Promise<void> {
    const upload = this.activeUploads.get(uploadId)
    if (!upload) {
      throw new R2OperationError(
        `No active upload found with ID ${uploadId}`,
        'completeMultipartUpload',
        path
      )
    }

    try {
      await upload.complete(parts)
      this.activeUploads.delete(uploadId)
    } catch (error: unknown) {
      const err = error instanceof Error ? error : new Error(String(error))
      throw new R2OperationError(
        `Failed to complete multipart upload for ${path}: ${err.message}`,
        'completeMultipartUpload',
        path,
        err
      )
    }
  }

  /**
   * Abort a multipart upload
   *
   * @param path - The object path/key
   * @param uploadId - The upload ID from startMultipartUpload
   */
  async abortMultipartUpload(path: string, uploadId: string): Promise<void> {
    const upload = this.activeUploads.get(uploadId)
    if (!upload) {
      throw new R2OperationError(
        `No active upload found with ID ${uploadId}`,
        'abortMultipartUpload',
        path
      )
    }

    try {
      await upload.abort()
      this.activeUploads.delete(uploadId)
    } catch (error: unknown) {
      const err = error instanceof Error ? error : new Error(String(error))
      throw new R2OperationError(
        `Failed to abort multipart upload for ${path}: ${err.message}`,
        'abortMultipartUpload',
        path,
        err
      )
    }
  }

  // ===========================================================================
  // Streaming Write Helper
  // ===========================================================================

  // Storage constants imported from constants.ts
  static readonly MIN_PART_SIZE = MIN_PART_SIZE
  static readonly DEFAULT_PART_SIZE = DEFAULT_PART_SIZE
  static readonly MAX_PARTS = MAX_PARTS

  /**
   * Write large data using multipart upload with automatic chunking
   * Handles chunking automatically for files up to 5TB
   *
   * @param path - The object path/key
   * @param data - The data to write
   * @param options - Optional settings including partSize
   * @returns WriteResult with etag and size
   */
  async writeStreaming(
    path: string,
    data: Uint8Array,
    options?: WriteOptions & { partSize?: number }
  ): Promise<WriteResult> {
    const partSize = options?.partSize ?? R2Backend.DEFAULT_PART_SIZE

    // Validate part size
    if (partSize < R2Backend.MIN_PART_SIZE) {
      throw new R2OperationError(
        `Part size must be at least ${R2Backend.MIN_PART_SIZE} bytes (5MB)`,
        'writeStreaming',
        path
      )
    }

    // For small files, use regular write
    if (data.length <= partSize) {
      return this.write(path, data, options)
    }

    // Calculate number of parts needed
    const numParts = Math.ceil(data.length / partSize)
    if (numParts > R2Backend.MAX_PARTS) {
      throw new R2OperationError(
        `Data too large: would require ${numParts} parts (max ${R2Backend.MAX_PARTS}). Increase partSize.`,
        'writeStreaming',
        path
      )
    }

    const key = this.withPrefix(path)

    try {
      // Build multipart options
      let r2Options: { httpMetadata?: { contentType?: string }; customMetadata?: Record<string, string> } | undefined
      if (options?.contentType || options?.metadata) {
        r2Options = {}
        if (options.contentType) {
          r2Options.httpMetadata = { contentType: options.contentType }
        }
        if (options.metadata) {
          r2Options.customMetadata = options.metadata
        }
      }

      // Create multipart upload
      const upload = await this.bucket.createMultipartUpload(key, r2Options)
      const uploadedParts: R2UploadedPart[] = []

      try {
        // Upload each part
        for (let partNumber = 1; partNumber <= numParts; partNumber++) {
          const start = (partNumber - 1) * partSize
          const end = Math.min(start + partSize, data.length)
          const chunk = data.slice(start, end)

          const part = await upload.uploadPart(partNumber, chunk)
          uploadedParts.push({
            partNumber: part.partNumber,
            etag: part.etag,
          })
        }

        // Complete the upload
        const result = await upload.complete(uploadedParts)

        return {
          etag: result.etag,
          size: result.size,
          versionId: result.version,
        }
      } catch (error: unknown) {
        // Attempt to abort on failure
        try {
          await upload.abort()
        } catch {
          // Ignore abort errors
        }
        throw error
      }
    } catch (error: unknown) {
      if (error instanceof R2OperationError) {
        throw error
      }
      const err = error instanceof Error ? error : new Error(String(error))
      throw new R2OperationError(
        `Failed to write streaming ${path}: ${err.message}`,
        'writeStreaming',
        path,
        err
      )
    }
  }
}

/**
 * Wrapper for R2MultipartUpload to implement our MultipartUpload interface
 */
class R2MultipartUploadWrapper implements MultipartUpload {
  readonly uploadId: string

  constructor(private readonly r2Upload: R2MultipartUpload) {
    this.uploadId = r2Upload.uploadId
  }

  async uploadPart(partNumber: number, data: Uint8Array): Promise<UploadedPart> {
    const r2Part = await this.r2Upload.uploadPart(partNumber, data)
    return {
      partNumber: r2Part.partNumber,
      etag: r2Part.etag,
      size: data.length,
    }
  }

  async complete(parts: UploadedPart[]): Promise<WriteResult> {
    // Convert to R2UploadedPart format (without size)
    const r2Parts: R2UploadedPart[] = parts.map(p => ({
      partNumber: p.partNumber,
      etag: p.etag,
    }))

    const result = await this.r2Upload.complete(r2Parts)
    return {
      etag: result.etag,
      size: result.size,
      versionId: result.version,
    }
  }

  async abort(): Promise<void> {
    await this.r2Upload.abort()
  }
}
