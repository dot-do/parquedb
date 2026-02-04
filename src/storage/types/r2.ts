/**
 * R2 Types - Cloudflare R2 bucket interface types
 *
 * These types mirror @cloudflare/workers-types for environments
 * that don't have access to the workers-types package.
 */

// =============================================================================
// R2 Object Types
// =============================================================================

/**
 * R2 object metadata
 */
export interface R2Object {
  /** Object key (path) */
  key: string

  /** Object version ID */
  version: string

  /** Size in bytes */
  size: number

  /** ETag for conditional operations */
  etag: string

  /** HTTP ETag (includes quotes) */
  httpEtag: string

  /** Upload timestamp */
  uploaded: Date

  /** HTTP metadata */
  httpMetadata?: R2HTTPMetadata | undefined

  /** Custom metadata */
  customMetadata?: Record<string, string> | undefined

  /** Storage class */
  storageClass: 'Standard'

  /** Checksums */
  checksums: R2Checksums

  /**
   * Write the object's HTTP metadata to the Headers object
   */
  writeHttpMetadata(headers: Headers): void
}

/**
 * R2 object body - includes data access methods
 */
export interface R2ObjectBody extends R2Object {
  /** Object body as ReadableStream */
  body: ReadableStream<Uint8Array>

  /** Whether the body has been used */
  bodyUsed: boolean

  /** Get body as ArrayBuffer */
  arrayBuffer(): Promise<ArrayBuffer>

  /** Get body as text */
  text(): Promise<string>

  /** Get body as JSON */
  json<T = unknown>(): Promise<T>

  /** Get body as Blob */
  blob(): Promise<Blob>
}

/**
 * HTTP metadata for R2 objects
 */
export interface R2HTTPMetadata {
  /** Content-Type header */
  contentType?: string | undefined

  /** Content-Language header */
  contentLanguage?: string | undefined

  /** Content-Disposition header */
  contentDisposition?: string | undefined

  /** Content-Encoding header */
  contentEncoding?: string | undefined

  /** Cache-Control header */
  cacheControl?: string | undefined

  /** Expiration time */
  cacheExpiry?: Date | undefined
}

/**
 * Checksums for R2 objects
 */
export interface R2Checksums {
  /** MD5 checksum */
  md5?: ArrayBuffer | undefined

  /** SHA-1 checksum */
  sha1?: ArrayBuffer | undefined

  /** SHA-256 checksum */
  sha256?: ArrayBuffer | undefined

  /** SHA-384 checksum */
  sha384?: ArrayBuffer | undefined

  /** SHA-512 checksum */
  sha512?: ArrayBuffer | undefined
}

// =============================================================================
// R2 Get Options
// =============================================================================

/**
 * Options for R2 get operations
 */
export interface R2GetOptions {
  /** Conditional headers for get */
  onlyIf?: R2Conditional | Headers | undefined

  /** Byte range to read */
  range?: R2Range | Headers | undefined
}

/**
 * Byte range specification
 */
export interface R2Range {
  /** Start offset (inclusive) */
  offset?: number | undefined

  /** Length to read */
  length?: number | undefined

  /** End offset (inclusive) - alternative to length */
  suffix?: number | undefined
}

/**
 * Conditional request options
 */
export interface R2Conditional {
  /** Only return if ETag matches */
  etagMatches?: string | undefined

  /** Only return if ETag doesn't match */
  etagDoesNotMatch?: string | undefined

  /** Only return if modified after this date */
  uploadedAfter?: Date | undefined

  /** Only return if modified before this date */
  uploadedBefore?: Date | undefined

  /** Return null instead of throwing on condition failure */
  secondsGranularity?: boolean | undefined
}

// =============================================================================
// R2 Put Options
// =============================================================================

/**
 * Options for R2 put operations
 */
export interface R2PutOptions {
  /** Conditional headers for put */
  onlyIf?: R2Conditional | Headers | undefined

  /** HTTP metadata */
  httpMetadata?: R2HTTPMetadata | Headers | undefined

  /** Custom metadata */
  customMetadata?: Record<string, string> | undefined

  /** MD5 checksum for integrity */
  md5?: ArrayBuffer | string | undefined

  /** SHA-1 checksum for integrity */
  sha1?: ArrayBuffer | string | undefined

  /** SHA-256 checksum for integrity */
  sha256?: ArrayBuffer | string | undefined

  /** SHA-384 checksum for integrity */
  sha384?: ArrayBuffer | string | undefined

  /** SHA-512 checksum for integrity */
  sha512?: ArrayBuffer | string | undefined
}

// =============================================================================
// R2 List Types
// =============================================================================

/**
 * Options for R2 list operations
 */
export interface R2ListOptions {
  /** Maximum number of objects to return (default 1000, max 1000) */
  limit?: number | undefined

  /** Only list objects with this prefix */
  prefix?: string | undefined

  /** Cursor for pagination */
  cursor?: string | undefined

  /** Delimiter for grouping (usually '/') */
  delimiter?: string | undefined

  /** Only list objects after this key */
  startAfter?: string | undefined

  /** Include custom metadata in results */
  include?: ('httpMetadata' | 'customMetadata')[] | undefined
}

/**
 * Result of R2 list operation
 */
export interface R2Objects {
  /** List of objects */
  objects: R2Object[]

  /** Whether there are more results */
  truncated: boolean

  /** Cursor for next page */
  cursor?: string | undefined

  /** Common prefixes when using delimiter */
  delimitedPrefixes: string[]
}

// =============================================================================
// R2 Multipart Upload Types
// =============================================================================

/**
 * Multipart upload handle
 */
export interface R2MultipartUpload {
  /** Upload ID */
  uploadId: string

  /** Object key */
  key: string

  /** Upload a part */
  uploadPart(partNumber: number, value: ReadableStream | ArrayBuffer | ArrayBufferView | string | Blob): Promise<R2UploadedPart>

  /** Abort the upload */
  abort(): Promise<void>

  /** Complete the upload */
  complete(uploadedParts: R2UploadedPart[]): Promise<R2Object>
}

/**
 * Uploaded part reference
 */
export interface R2UploadedPart {
  /** Part number (1-10000) */
  partNumber: number

  /** ETag of the part */
  etag: string
}

/**
 * Options for creating multipart upload
 */
export interface R2MultipartOptions {
  /** HTTP metadata */
  httpMetadata?: R2HTTPMetadata | Headers | undefined

  /** Custom metadata */
  customMetadata?: Record<string, string> | undefined
}

// =============================================================================
// R2 Bucket Interface
// =============================================================================

/**
 * Cloudflare R2 Bucket binding interface
 */
export interface R2Bucket {
  /**
   * Get an object from the bucket
   * Returns null if object doesn't exist
   */
  get(key: string, options?: R2GetOptions): Promise<R2ObjectBody | null>

  /**
   * Get object metadata only (HEAD)
   * Returns null if object doesn't exist
   */
  head(key: string): Promise<R2Object | null>

  /**
   * Put an object into the bucket
   */
  put(
    key: string,
    value: ReadableStream | ArrayBuffer | ArrayBufferView | string | Blob | null,
    options?: R2PutOptions | undefined
  ): Promise<R2Object | null>

  /**
   * Delete an object from the bucket
   */
  delete(keys: string | string[]): Promise<void>

  /**
   * List objects in the bucket
   */
  list(options?: R2ListOptions): Promise<R2Objects>

  /**
   * Create a multipart upload
   */
  createMultipartUpload(key: string, options?: R2MultipartOptions): Promise<R2MultipartUpload>

  /**
   * Resume a multipart upload
   */
  resumeMultipartUpload(key: string, uploadId: string): R2MultipartUpload
}

// =============================================================================
// R2 Error Types
// =============================================================================

/**
 * R2 specific error
 */
export class R2Error extends Error {
  override readonly name = 'R2Error'
  constructor(
    message: string,
    public readonly code: string,
    public readonly status?: number
  ) {
    super(message)
    Object.setPrototypeOf(this, R2Error.prototype)
  }
}

/**
 * Common R2 error codes
 */
export const R2ErrorCodes = {
  /** Object not found */
  NoSuchKey: 'NoSuchKey',

  /** Bucket not found */
  NoSuchBucket: 'NoSuchBucket',

  /** Precondition failed (conditional request) */
  PreconditionFailed: 'PreconditionFailed',

  /** Invalid range */
  InvalidRange: 'InvalidRange',

  /** Entity too large */
  EntityTooLarge: 'EntityTooLarge',

  /** Internal error */
  InternalError: 'InternalError',

  /** Access denied */
  AccessDenied: 'AccessDenied',

  /** Invalid request */
  BadRequest: 'BadRequest',
} as const

export type R2ErrorCode = (typeof R2ErrorCodes)[keyof typeof R2ErrorCodes]
