/**
 * Shared Engine Utilities
 *
 * Common helper functions used across multiple engine modules.
 * Extracted to eliminate duplication in:
 * - parquet-adapter.ts
 * - do-compactor.ts
 * - do-read-path.ts
 * - parquet-encoders.ts
 * - engine.ts
 */

// =============================================================================
// Type coercion helpers
// =============================================================================

/**
 * Coerce a value to number. Handles BigInt (legacy INT64 files) and number (DOUBLE).
 */
export function toNumber(value: unknown): number {
  if (typeof value === 'number') return value
  if (typeof value === 'bigint') return Number(value)
  return 0
}

// =============================================================================
// System field constants
// =============================================================================

/** System fields stored as dedicated Parquet columns for DataLine */
export const DATA_SYSTEM_FIELDS = new Set(['$id', '$op', '$v', '$ts'])
