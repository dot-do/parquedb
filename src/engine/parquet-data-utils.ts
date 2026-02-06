/**
 * Parquet $data Field Utilities
 *
 * Shared utility functions for parsing the $data column from Parquet files.
 * Supports three formats for backward compatibility:
 *
 * 1. VARIANT binary format: hyparquet returns {metadata: Uint8Array, value: Uint8Array}
 *    which we decode using the variant-decoder module
 * 2. JSON object: hyparquet auto-decodes JSON converted type to JS objects
 * 3. Legacy JSON string: manually JSON.parse the string
 *
 * Used by:
 * - parquet-adapter.ts (Node.js read path)
 * - do-read-path.ts (Workers read path)
 * - do-compactor.ts (Workers compaction path)
 */

import { decodeVariant } from './variant-decoder'

/**
 * Check if a value is a raw VARIANT structure from hyparquet.
 * hyparquet reads VARIANT groups as {metadata: Uint8Array, value: Uint8Array}.
 */
function isRawVariant(data: unknown): data is { metadata: Uint8Array; value: Uint8Array } {
  if (data === null || typeof data !== 'object') return false
  const obj = data as Record<string, unknown>
  return (
    obj.metadata instanceof Uint8Array &&
    obj.value instanceof Uint8Array
  )
}

/**
 * Parse the $data field from a Parquet row.
 *
 * Handles three formats:
 * - VARIANT binary: {metadata: Uint8Array, value: Uint8Array} from VARIANT column
 * - JSON object: hyparquet auto-decoded from JSON converted type
 * - Legacy JSON string: manually JSON.parse the string
 *
 * @param data - The raw $data value from hyparquet row decoding
 * @returns Parsed data fields as a plain object
 */
export function parseDataField(data: unknown): Record<string, unknown> {
  if (data === null || data === undefined) return {}

  // VARIANT binary format: decode using variant-decoder
  if (isRawVariant(data)) {
    try {
      const decoded = decodeVariant(data.metadata, data.value)
      if (decoded === null || decoded === undefined) return {}
      if (typeof decoded === 'object' && !Array.isArray(decoded)) {
        return decoded as Record<string, unknown>
      }
      // VARIANT decoded to a non-object (shouldn't happen for $data, but handle gracefully)
      return {}
    } catch {
      console.warn('[parquet-data-utils] Failed to decode VARIANT $data')
      return {}
    }
  }

  // JSON object format: hyparquet auto-decodes JSON columns to JS objects
  if (typeof data === 'object') {
    return data as Record<string, unknown>
  }

  // Legacy JSON string format: parse the string
  if (typeof data === 'string') {
    if (data === '') return {}
    try {
      return JSON.parse(data) as Record<string, unknown>
    } catch {
      console.warn(`[parquet-data-utils] Failed to parse $data JSON: ${data.slice(0, 100)}`)
      return {}
    }
  }

  return {}
}
