/**
 * Type definitions for Parquet integration
 */

import type { StorageBackend, WriteResult } from '../types/storage'
import type { TypeDefinition } from '../types/schema'

// =============================================================================
// Parquet Schema Types
// =============================================================================

/** Parquet primitive types */
export type ParquetPrimitiveType =
  | 'BOOLEAN'
  | 'INT32'
  | 'INT64'
  | 'INT96'
  | 'FLOAT'
  | 'DOUBLE'
  | 'BYTE_ARRAY'
  | 'FIXED_LEN_BYTE_ARRAY'

/** Parquet logical types */
export type ParquetLogicalType =
  | 'STRING'
  | 'ENUM'
  | 'UUID'
  | 'INT8'
  | 'INT16'
  | 'INT32'
  | 'INT64'
  | 'UINT8'
  | 'UINT16'
  | 'UINT32'
  | 'UINT64'
  | 'DECIMAL'
  | 'DATE'
  | 'TIME_MILLIS'
  | 'TIME_MICROS'
  | 'TIMESTAMP_MILLIS'
  | 'TIMESTAMP_MICROS'
  | 'JSON'
  | 'BSON'
  | 'LIST'
  | 'MAP'

/** Parquet compression codecs */
export type CompressionCodec = 'UNCOMPRESSED' | 'SNAPPY' | 'GZIP' | 'LZO' | 'BROTLI' | 'LZ4' | 'ZSTD'

/** Parquet encoding types */
export type EncodingType =
  | 'PLAIN'
  | 'PLAIN_DICTIONARY'
  | 'RLE'
  | 'BIT_PACKED'
  | 'DELTA_BINARY_PACKED'
  | 'DELTA_LENGTH_BYTE_ARRAY'
  | 'DELTA_BYTE_ARRAY'
  | 'RLE_DICTIONARY'
  | 'BYTE_STREAM_SPLIT'

/** Parquet field definition */
export interface ParquetFieldSchema {
  /** Field type */
  type: ParquetPrimitiveType | ParquetLogicalType
  /** Whether field is optional */
  optional?: boolean | undefined
  /** For FIXED_LEN_BYTE_ARRAY */
  typeLength?: number | undefined
  /** For DECIMAL */
  precision?: number | undefined
  scale?: number | undefined
  /** Field encoding */
  encoding?: EncodingType | undefined
  /** Field compression */
  compression?: CompressionCodec | undefined
  /** Repetition type for nested structures */
  repetitionType?: 'REQUIRED' | 'OPTIONAL' | 'REPEATED' | undefined
}

/** Parquet schema definition */
export interface ParquetSchema {
  [fieldName: string]: ParquetFieldSchema
}

// =============================================================================
// Parquet Metadata Types
// =============================================================================

/** Column chunk metadata */
export interface ColumnChunkMetadata {
  /** Column path in schema */
  pathInSchema: string[]
  /** Total compressed size in bytes */
  totalCompressedSize: number
  /** Total uncompressed size in bytes */
  totalUncompressedSize: number
  /** Number of values */
  numValues: number
  /** Encoding used */
  encodings: EncodingType[]
  /** Compression codec */
  codec: CompressionCodec
  /** Statistics for the column */
  statistics?: ColumnStatistics | undefined
  /** Whether this column chunk has a bloom filter */
  hasBloomFilter?: boolean | undefined
  /** Offset to bloom filter data in the file (if present) */
  bloomFilterOffset?: bigint | undefined
  /** Length of bloom filter data in bytes (if present) */
  bloomFilterLength?: number | undefined
}

/** Column statistics */
export interface ColumnStatistics {
  /** Minimum value (encoded) */
  min?: unknown | undefined
  /** Maximum value (encoded) */
  max?: unknown | undefined
  /** Number of null values */
  nullCount?: number | undefined
  /** Number of distinct values (approximate) */
  distinctCount?: number | undefined
}

/** Row group metadata */
export interface RowGroupMetadata {
  /** Number of rows in this row group */
  numRows: number
  /** Total byte size of row group */
  totalByteSize: number
  /** Column chunk metadata */
  columns: ColumnChunkMetadata[]
  /** Sorting columns (if any) */
  sortingColumns?: SortingColumn[] | undefined
  /** File offset of row group */
  fileOffset?: number | undefined
  /** Total compressed size */
  totalCompressedSize?: number | undefined
  /** Ordinal in the file */
  ordinal?: number | undefined
}

/** Sorting column specification */
export interface SortingColumn {
  /** Column index */
  columnIdx: number
  /** Descending order */
  descending: boolean
  /** Nulls first */
  nullsFirst: boolean
}

/** Parquet file metadata */
export interface ParquetMetadata {
  /** Parquet format version */
  version: number
  /** Schema definition */
  schema: ParquetSchemaElement[]
  /** Number of rows in file */
  numRows: number
  /** Row group metadata */
  rowGroups: RowGroupMetadata[]
  /** Key-value metadata */
  keyValueMetadata?: KeyValueMetadata[] | undefined
  /** Creator application */
  createdBy?: string | undefined
}

/** Schema element in metadata */
export interface ParquetSchemaElement {
  /** Element type */
  type?: ParquetPrimitiveType | undefined
  /** Type length for fixed-length types */
  typeLength?: number | undefined
  /** Repetition type */
  repetitionType?: 'REQUIRED' | 'OPTIONAL' | 'REPEATED' | undefined
  /** Element name */
  name: string
  /** Number of children (for groups) */
  numChildren?: number | undefined
  /** Converted type (legacy logical type) */
  convertedType?: string | undefined
  /** Scale for DECIMAL */
  scale?: number | undefined
  /** Precision for DECIMAL */
  precision?: number | undefined
  /** Field ID */
  fieldId?: number | undefined
  /** Logical type */
  logicalType?: ParquetLogicalType | undefined
}

/** Key-value metadata pair */
export interface KeyValueMetadata {
  key: string
  value: string
}

// =============================================================================
// Reader Types
// =============================================================================

/** Options for ParquetReader */
export interface ParquetReaderOptions {
  /** Storage backend for reading bytes */
  storage: StorageBackend
  /** Default columns to read */
  columns?: string[] | undefined
  /** Default row groups to read */
  rowGroups?: number[] | undefined
}

/** Options for reading Parquet data */
export interface ReadOptions {
  /** Columns to read (default: all) */
  columns?: string[] | undefined
  /** Row groups to read (default: all) */
  rowGroups?: number[] | undefined
  /** Skip rows at the beginning */
  offset?: number | undefined
  /** Maximum rows to return */
  limit?: number | undefined
  /** Filter predicate for row-level filtering */
  filter?: RowFilter | undefined
  /** Whether to include row group metadata in results */
  includeRowGroupMetadata?: boolean | undefined
}

/** Row filter for predicate pushdown */
export interface RowFilter {
  /** Column name */
  column: string
  /** Filter operator */
  op: 'eq' | 'ne' | 'gt' | 'gte' | 'lt' | 'lte' | 'in' | 'isNull' | 'isNotNull'
  /** Filter value */
  value?: unknown | undefined
}

// =============================================================================
// Writer Types
// =============================================================================

/** Options for ParquetWriter */
export interface ParquetWriterOptions {
  /** Compression codec */
  compression?: 'lz4' | 'snappy' | 'gzip' | 'zstd' | 'none' | undefined
  /** Row group size (number of rows per group) */
  rowGroupSize?: number | undefined
  /** Use dictionary encoding */
  dictionary?: boolean | undefined
  /** Page size in bytes */
  pageSize?: number | undefined
  /** Enable statistics */
  statistics?: boolean | undefined
  /** Key-value metadata to include */
  metadata?: Record<string, string> | undefined
  /**
   * Enable column indexes (ColumnIndex) for page-level predicate pushdown
   * Default: true - enables hyparquet's parquetQuery() to skip pages based on min/max values
   */
  columnIndex?: boolean | undefined
  /**
   * Enable offset indexes (OffsetIndex) for efficient page location lookup
   * Default: true - required when columnIndex is enabled per Parquet spec
   */
  offsetIndex?: boolean | undefined
}

/** Result of write operation */
export interface ParquetWriteResult extends WriteResult {
  /** Number of rows written */
  rowCount: number
  /** Number of row groups created */
  rowGroupCount: number
  /** Columns written */
  columns: string[]
}

// =============================================================================
// Entity Schema Types
// =============================================================================

/** Schema for entity storage in Parquet */
export interface EntityParquetSchema extends ParquetSchema {
  /** Entity ID */
  $id: ParquetFieldSchema
  /** Entity type */
  $type: ParquetFieldSchema
  /** Variant-encoded data column */
  $data: ParquetFieldSchema
  /** Created timestamp */
  createdAt: ParquetFieldSchema
  /** Updated timestamp */
  updatedAt: ParquetFieldSchema
  /** Version number */
  version: ParquetFieldSchema
}

// =============================================================================
// AsyncBuffer Interface (for hyparquet)
// =============================================================================

/**
 * AsyncBuffer interface compatible with hyparquet
 * Provides async access to byte ranges in a file
 */
export interface AsyncBuffer {
  /** Get the total byte length of the buffer */
  byteLength: number
  /** Read a byte range from the buffer */
  slice(start: number, end?: number): Promise<ArrayBuffer>
}

// =============================================================================
// Utility Types
// =============================================================================

/** Type mapping from ParqueDB to Parquet */
export type ParqueDBToParquetType = {
  string: 'STRING'
  text: 'STRING'
  markdown: 'STRING'
  number: 'DOUBLE'
  int: 'INT64'
  float: 'FLOAT'
  double: 'DOUBLE'
  boolean: 'BOOLEAN'
  date: 'DATE'
  datetime: 'TIMESTAMP_MILLIS'
  timestamp: 'TIMESTAMP_MILLIS'
  uuid: 'UUID'
  email: 'STRING'
  url: 'STRING'
  json: 'JSON'
  binary: 'BYTE_ARRAY'
}

/** Options for creating entity schema */
export interface CreateEntitySchemaOptions {
  /** Type definition from ParqueDB schema */
  typeDef?: TypeDefinition | undefined
  /** Fields to shred from Variant for columnar efficiency */
  shredFields?: string[] | undefined
  /** Additional columns to include */
  additionalColumns?: ParquetSchema | undefined
}

// =============================================================================
// Typed Writer Types
// =============================================================================

/**
 * Options for writing typed entities
 *
 * These options configure how entities are written to Parquet files
 * with typed schema support.
 */
export interface TypedWriteOptions extends ParquetWriterOptions {
  /**
   * The TypeDefinition schema for the entities being written.
   * Used to generate the Parquet column schema.
   */
  schema: TypeDefinition

  /**
   * Include the $data variant column containing the full entity as JSON.
   * This enables flexible querying of non-shredded fields.
   * @default true
   */
  includeDataVariant?: boolean | undefined

  /**
   * Include audit columns (createdAt, createdBy, updatedAt, updatedBy, version).
   * @default true
   */
  includeAuditColumns?: boolean | undefined

  /**
   * Include soft delete columns (deletedAt, deletedBy).
   * @default true
   */
  includeSoftDeleteColumns?: boolean | undefined
}
