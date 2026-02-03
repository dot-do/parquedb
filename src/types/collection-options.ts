/**
 * Collection Options Types
 *
 * These types are used to configure per-collection storage and behavior options.
 * They are defined in a separate file to avoid circular dependencies between
 * db.ts (DB factory) and ParqueDB/types.ts (ParqueDBConfig).
 */

/**
 * Per-collection storage and behavior options
 *
 * @example
 * ```typescript
 * const db = DB({
 *   Occupation: {
 *     $options: {
 *       includeDataVariant: true,  // Include $data variant column (default: true)
 *     },
 *     name: 'string!',
 *     socCode: 'string!#',
 *   },
 *   Logs: {
 *     $options: { includeDataVariant: false },  // Omit $data for write-heavy logs
 *     level: 'string',
 *     message: 'string',
 *   }
 * })
 * ```
 */
export interface CollectionOptions {
  /** Include $data variant column for fast full-row reads (default: true) */
  includeDataVariant?: boolean | undefined
  // Future options can be added here:
  // compression?: 'snappy' | 'gzip' | 'zstd' | 'none'
  // partitionBy?: string[]
  // rowGroupSize?: number
}

/**
 * Default collection options
 */
export const DEFAULT_COLLECTION_OPTIONS: Required<CollectionOptions> = {
  includeDataVariant: true,
}
