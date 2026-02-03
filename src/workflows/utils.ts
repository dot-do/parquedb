/**
 * Workflow Utilities
 *
 * Helper functions for Cloudflare Workflows that need to interface
 * with internal types.
 */

import type { R2Bucket as InternalR2Bucket } from '../storage/types/r2'

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
