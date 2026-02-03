/**
 * Type definitions for ParqueDB Cloudflare Snippets
 *
 * Minimal type definitions for Parquet reading and filtering.
 * These are kept small to minimize bundle size for Snippets.
 */

// =============================================================================
// Parquet Types
// =============================================================================

/**
 * Parquet file footer containing metadata
 */
export interface ParquetFooter {
  version: number
  schema: SchemaElement[]
  numRows: number
  rowGroups: RowGroupMetadata[]
  keyValueMetadata?: KeyValue[]
}

/**
 * Schema element describing a column
 */
export interface SchemaElement {
  name: string
  type?: PhysicalType
  repetitionType?: RepetitionType
  convertedType?: ConvertedType
  numChildren?: number
}

/**
 * Physical types in Parquet
 */
export type PhysicalType =
  | 'BOOLEAN'
  | 'INT32'
  | 'INT64'
  | 'INT96'
  | 'FLOAT'
  | 'DOUBLE'
  | 'BYTE_ARRAY'
  | 'FIXED_LEN_BYTE_ARRAY'

/**
 * Repetition types for nested structures
 */
export type RepetitionType = 'REQUIRED' | 'OPTIONAL' | 'REPEATED'

/**
 * Converted types (logical types)
 */
export type ConvertedType =
  | 'UTF8'
  | 'DATE'
  | 'TIMESTAMP_MILLIS'
  | 'TIMESTAMP_MICROS'
  | 'DECIMAL'
  | 'JSON'
  | 'MAP'
  | 'LIST'

/**
 * Metadata for a row group
 */
export interface RowGroupMetadata {
  numRows: number
  totalByteSize: number
  columns: ColumnChunkMetadata[]
}

/**
 * Metadata for a column chunk within a row group
 */
export interface ColumnChunkMetadata {
  path: string[]
  fileOffset: number
  numValues: number
  totalCompressedSize: number
  totalUncompressedSize: number
  statistics?: ColumnStatistics
}

/**
 * Statistics for a column chunk (used for predicate pushdown)
 */
export interface ColumnStatistics {
  min?: unknown
  max?: unknown
  nullCount?: number
  distinctCount?: number
}

/**
 * Key-value metadata pair
 */
export interface KeyValue {
  key: string
  value?: string
}

// =============================================================================
// Filter Types (Minimal Subset)
// =============================================================================

/**
 * Simple filter for Snippets
 *
 * Supports basic comparison operators only:
 * - Equality (direct value)
 * - $eq, $ne
 * - $gt, $gte, $lt, $lte
 * - $in
 */
export interface Filter {
  [field: string]: FilterValue | undefined
}

/**
 * Filter value - either direct value or operator
 */
export type FilterValue =
  | string
  | number
  | boolean
  | null
  | { $eq: unknown }
  | { $ne: unknown }
  | { $gt: number | string }
  | { $gte: number | string }
  | { $lt: number | string }
  | { $lte: number | string }
  | { $in: unknown[] }

// =============================================================================
// AsyncBuffer for reading
// =============================================================================

/**
 * Async buffer interface for reading Parquet files
 */
export interface AsyncBuffer {
  byteLength: number
  slice(start: number, end?: number): Promise<ArrayBuffer>
}

/**
 * Row data from Parquet file
 */
export type Row = Record<string, unknown>
