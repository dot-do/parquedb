/**
 * Workflow Utilities
 *
 * Helper functions for Cloudflare Workflows that need to interface
 * with internal types.
 *
 * Type guards for R2Bucket validation are available from src/types/cast.ts.
 * This module provides workflow-specific adapters that bridge from Workers
 * R2Bucket bindings to our internal R2Bucket type.
 */

import type { R2Bucket as InternalR2Bucket } from '../storage/types/r2'
import {
  isR2BucketLike,
  requireR2Bucket as requireR2BucketBase,
  toR2BucketOrUndefined as toR2BucketOrUndefinedBase,
  assertR2Bucket,
} from '../types/cast'

// Re-export base type guards for convenience
export { isR2BucketLike, assertR2Bucket }

// =============================================================================
// Internal R2Bucket Type Guards
// =============================================================================

/**
 * Type guard to check if a value looks like an internal R2Bucket
 *
 * Same checks as isR2BucketLike but narrows to the internal type.
 *
 * @param value - The value to check
 * @returns true if the value has the required R2Bucket methods
 *
 * @example
 * ```typescript
 * if (isInternalR2BucketLike(bucket)) {
 *   await bucket.get('path/to/file')
 * }
 * ```
 */
export function isInternalR2BucketLike(value: unknown): value is InternalR2Bucket {
  return isR2BucketLike(value)
}

// =============================================================================
// Validated Conversion Functions
// =============================================================================

/**
 * Convert a Cloudflare Workers R2Bucket binding to our internal R2Bucket type.
 *
 * This provides a typed adapter to eliminate unsafe `as unknown as R2Bucket` casts
 * throughout the workflow code. The runtime types are compatible, but TypeScript
 * sees them as different because our internal type definitions don't share a
 * common source with @cloudflare/workers-types.
 *
 * @param bucket - The R2Bucket from the Workers environment binding
 * @returns The same bucket typed as our internal R2Bucket interface
 *
 * @example
 * ```typescript
 * const storage = new R2Backend(toInternalR2Bucket(this.env.BUCKET))
 * ```
 */
export function toInternalR2Bucket(bucket: R2Bucket): InternalR2Bucket {
  return bucket as unknown as InternalR2Bucket
}

/**
 * Safely convert a value to internal R2Bucket with runtime validation
 *
 * Unlike toInternalR2Bucket, this function validates the value at runtime
 * and throws a descriptive error if the value is not a valid R2Bucket.
 *
 * @param value - The value to convert (usually from env.BUCKET)
 * @param name - Name of the binding for error messages (default: 'BUCKET')
 * @returns The value typed as internal R2Bucket
 * @throws Error if the value is not a valid R2Bucket
 *
 * @example
 * ```typescript
 * // With validation - throws if BUCKET is not configured
 * const storage = new R2Backend(requireR2Bucket(this.env.BUCKET))
 *
 * // With custom name for error messages
 * const cdnBucket = requireR2Bucket(this.env.CDN_BUCKET, 'CDN_BUCKET')
 * ```
 */
export function requireR2Bucket(value: unknown, name = 'BUCKET'): InternalR2Bucket {
  // Use base validation, then cast to internal type
  requireR2BucketBase(value, name)
  return value as unknown as InternalR2Bucket
}

/**
 * Safely convert a value to internal R2Bucket if valid, otherwise return undefined
 *
 * This is useful for optional bucket bindings where you want to check
 * if the bucket is configured without throwing an error.
 *
 * @param value - The value to convert
 * @returns The value typed as internal R2Bucket, or undefined if invalid
 *
 * @example
 * ```typescript
 * const cdnBucket = toR2BucketOrUndefined(this.env.CDN_BUCKET)
 * if (cdnBucket) {
 *   // CDN bucket is configured
 *   await cdnBucket.put('cdn/file', data)
 * }
 * ```
 */
export function toR2BucketOrUndefined(value: unknown): InternalR2Bucket | undefined {
  const bucket = toR2BucketOrUndefinedBase(value)
  if (bucket) {
    return bucket as unknown as InternalR2Bucket
  }
  return undefined
}
