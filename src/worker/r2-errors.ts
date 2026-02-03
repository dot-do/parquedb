/**
 * R2 Bucket Error Handling
 *
 * Provides utilities for validating R2 bucket bindings and handling
 * missing bucket errors gracefully in worker code.
 */

import type { Env } from '../types/worker'

// =============================================================================
// Error Types
// =============================================================================

/**
 * Error thrown when a required R2 bucket binding is missing
 */
export class MissingBucketError extends Error {
  override readonly name = 'MissingBucketError'

  constructor(
    public readonly bucketName: string,
    public readonly context?: string
  ) {
    const message = context
      ? `R2 bucket '${bucketName}' is not configured. ${context}`
      : `R2 bucket '${bucketName}' is not configured. Check wrangler.toml bindings.`
    super(message)
    Object.setPrototypeOf(this, MissingBucketError.prototype)
  }
}

/**
 * Error thrown when R2 bucket operations fail
 */
export class BucketOperationError extends Error {
  override readonly name = 'BucketOperationError'

  constructor(
    public readonly operation: string,
    public readonly bucketName: string,
    public readonly path: string,
    public override readonly cause?: Error
  ) {
    super(`R2 ${operation} failed for '${path}' in bucket '${bucketName}': ${cause?.message ?? 'Unknown error'}`)
    Object.setPrototypeOf(this, BucketOperationError.prototype)
  }
}

// =============================================================================
// Validation Functions
// =============================================================================

/**
 * Validate that the primary R2 bucket (BUCKET) is configured
 *
 * @param env - Worker environment
 * @throws MissingBucketError if BUCKET is not configured
 */
export function requireBucket(env: Env): R2Bucket {
  if (!env.BUCKET) {
    throw new MissingBucketError(
      'BUCKET',
      'The primary R2 bucket binding is required for ParqueDB operations.'
    )
  }
  return env.BUCKET
}

/**
 * Validate that the CDN bucket is configured (optional)
 *
 * @param env - Worker environment
 * @returns CDN bucket or undefined if not configured
 */
export function getCdnBucket(env: Env): R2Bucket | undefined {
  return env.CDN_BUCKET
}

/**
 * Check if primary bucket is available
 *
 * @param env - Worker environment
 * @returns true if BUCKET is configured
 */
export function hasBucket(env: Env): boolean {
  return env.BUCKET !== undefined && env.BUCKET !== null
}

/**
 * Check if CDN bucket is available
 *
 * @param env - Worker environment
 * @returns true if CDN_BUCKET is configured
 */
export function hasCdnBucket(env: Env): boolean {
  return env.CDN_BUCKET !== undefined && env.CDN_BUCKET !== null
}

// =============================================================================
// Response Helpers
// =============================================================================

/**
 * Build an error response for missing bucket errors
 *
 * @param error - The MissingBucketError
 * @returns HTTP Response with appropriate status and message
 */
export function buildMissingBucketResponse(error: MissingBucketError): Response {
  return Response.json(
    {
      error: 'Service Unavailable',
      message: error.message,
      bucket: error.bucketName,
      code: 'MISSING_BUCKET',
    },
    { status: 503 }
  )
}

/**
 * Build an error response for bucket operation errors
 *
 * @param error - The BucketOperationError
 * @returns HTTP Response with appropriate status and message
 */
export function buildBucketOperationErrorResponse(error: BucketOperationError): Response {
  return Response.json(
    {
      error: 'Storage Error',
      message: error.message,
      operation: error.operation,
      bucket: error.bucketName,
      path: error.path,
      code: 'BUCKET_OPERATION_FAILED',
    },
    { status: 500 }
  )
}

/**
 * Handle bucket-related errors and return appropriate HTTP response
 *
 * @param error - The error to handle
 * @returns HTTP Response if error is bucket-related, null otherwise
 */
export function handleBucketError(error: unknown): Response | null {
  if (error instanceof MissingBucketError) {
    return buildMissingBucketResponse(error)
  }
  if (error instanceof BucketOperationError) {
    return buildBucketOperationErrorResponse(error)
  }
  return null
}

// =============================================================================
// Bucket Operation Wrappers
// =============================================================================

/**
 * Safely get an object from R2 with proper error handling
 *
 * @param bucket - R2 bucket
 * @param path - Object path
 * @param bucketName - Name for error messages (default: 'BUCKET')
 * @returns R2ObjectBody or null if not found
 * @throws BucketOperationError on failure
 */
export async function safeGet(
  bucket: R2Bucket,
  path: string,
  bucketName = 'BUCKET'
): Promise<R2ObjectBody | null> {
  try {
    return await bucket.get(path)
  } catch (error) {
    throw new BucketOperationError(
      'get',
      bucketName,
      path,
      error instanceof Error ? error : new Error(String(error))
    )
  }
}

/**
 * Safely put an object to R2 with proper error handling
 *
 * @param bucket - R2 bucket
 * @param path - Object path
 * @param data - Data to write
 * @param options - Put options
 * @param bucketName - Name for error messages (default: 'BUCKET')
 * @returns R2Object
 * @throws BucketOperationError on failure
 */
export async function safePut(
  bucket: R2Bucket,
  path: string,
  data: ReadableStream | ArrayBuffer | ArrayBufferView | string | Blob,
  options?: R2PutOptions,
  bucketName = 'BUCKET'
): Promise<R2Object | null> {
  try {
    return await bucket.put(path, data, options)
  } catch (error) {
    throw new BucketOperationError(
      'put',
      bucketName,
      path,
      error instanceof Error ? error : new Error(String(error))
    )
  }
}

/**
 * Safely check if an object exists in R2
 *
 * @param bucket - R2 bucket
 * @param path - Object path
 * @param bucketName - Name for error messages (default: 'BUCKET')
 * @returns R2Object (head result) or null if not found
 * @throws BucketOperationError on failure
 */
export async function safeHead(
  bucket: R2Bucket,
  path: string,
  bucketName = 'BUCKET'
): Promise<R2Object | null> {
  try {
    return await bucket.head(path)
  } catch (error) {
    throw new BucketOperationError(
      'head',
      bucketName,
      path,
      error instanceof Error ? error : new Error(String(error))
    )
  }
}

/**
 * Safely list objects in R2
 *
 * @param bucket - R2 bucket
 * @param options - List options
 * @param bucketName - Name for error messages (default: 'BUCKET')
 * @returns R2Objects list result
 * @throws BucketOperationError on failure
 */
export async function safeList(
  bucket: R2Bucket,
  options?: R2ListOptions,
  bucketName = 'BUCKET'
): Promise<R2Objects> {
  try {
    return await bucket.list(options)
  } catch (error) {
    throw new BucketOperationError(
      'list',
      bucketName,
      options?.prefix ?? '',
      error instanceof Error ? error : new Error(String(error))
    )
  }
}

/**
 * Safely delete an object from R2
 *
 * @param bucket - R2 bucket
 * @param path - Object path or array of paths
 * @param bucketName - Name for error messages (default: 'BUCKET')
 * @throws BucketOperationError on failure
 */
export async function safeDelete(
  bucket: R2Bucket,
  path: string | string[],
  bucketName = 'BUCKET'
): Promise<void> {
  try {
    await bucket.delete(path)
  } catch (error) {
    const pathStr = Array.isArray(path) ? path.join(', ') : path
    throw new BucketOperationError(
      'delete',
      bucketName,
      pathStr,
      error instanceof Error ? error : new Error(String(error))
    )
  }
}
