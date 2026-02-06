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
export const DATA_SYSTEM_FIELDS = new Set(['$id', '$op', '$v', '$ts', '$data'])

// =============================================================================
// ID Generation
// =============================================================================

/**
 * Shared time-sortable ID generator (ULID-like).
 *
 * Format: {timestamp_base36_padded}{counter_base36_padded}{random_base36}
 * - Timestamp portion ensures lexicographic sort across milliseconds
 * - Counter portion ensures sort order within the same millisecond
 * - Random portion provides additional uniqueness
 *
 * The class maintains a monotonic counter within each millisecond so that
 * IDs generated in the same ms are still lexicographically ordered.
 */
export class IdGenerator {
  /** Last timestamp used in ID generation (for monotonic counter) */
  private lastIdTs = 0
  /** Monotonic counter within the same millisecond */
  private idCounter = 0

  /**
   * Generate a time-sortable, unique ID.
   */
  generateId(): string {
    const now = Date.now()
    if (now === this.lastIdTs) {
      this.idCounter++
    } else {
      this.lastIdTs = now
      this.idCounter = 0
    }
    const ts = now.toString(36).padStart(9, '0')
    const counter = this.idCounter.toString(36).padStart(4, '0')
    const rand = Math.random().toString(36).substring(2, 6)
    return ts + counter + rand
  }
}
