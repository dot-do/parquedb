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
  optional?: boolean
  /** For FIXED_LEN_BYTE_ARRAY */
  typeLength?: number
  /** For DECIMAL */
  precision?: number
  scale?: number
  /** Field encoding */
  encoding?: EncodingType
  /** Field compression */
  compression?: CompressionCodec
  /** Repetition type for nested structures */
  repetitionType?: 'REQUIRED' | 'OPTIONAL' | 'REPEATED'
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
  statistics?: ColumnStatistics
}

/** Column statistics */
export interface ColumnStatistics {
  /** Minimum value (encoded) */
  min?: unknown
  /** Maximum value (encoded) */
  max?: unknown
  /** Number of null values */
  nullCount?: number
  /** Number of distinct values (approximate) */
  distinctCount?: number
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
  sortingColumns?: SortingColumn[]
  /** File offset of row group */
  fileOffset?: number
  /** Total compressed size */
  totalCompressedSize?: number
  /** Ordinal in the file */
  ordinal?: number
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
  keyValueMetadata?: KeyValueMetadata[]
  /** Creator application */
  createdBy?: string
}

/** Schema element in metadata */
export interface ParquetSchemaElement {
  /** Element type */
  type?: ParquetPrimitiveType
  /** Type length for fixed-length types */
  typeLength?: number
  /** Repetition type */
  repetitionType?: 'REQUIRED' | 'OPTIONAL' | 'REPEATED'
  /** Element name */
  name: string
  /** Number of children (for groups) */
  numChildren?: number
  /** Converted type (legacy logical type) */
  convertedType?: string
  /** Scale for DECIMAL */
  scale?: number
  /** Precision for DECIMAL */
  precision?: number
  /** Field ID */
  fieldId?: number
  /** Logical type */
  logicalType?: ParquetLogicalType
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
  columns?: string[]
  /** Default row groups to read */
  rowGroups?: number[]
}

/** Options for reading Parquet data */
export interface ReadOptions {
  /** Columns to read (default: all) */
  columns?: string[]
  /** Row groups to read (default: all) */
  rowGroups?: number[]
  /** Skip rows at the beginning */
  offset?: number
  /** Maximum rows to return */
  limit?: number
  /** Filter predicate for row-level filtering */
  filter?: RowFilter
  /** Whether to include row group metadata in results */
  includeRowGroupMetadata?: boolean
}

/** Row filter for predicate pushdown */
export interface RowFilter {
  /** Column name */
  column: string
  /** Filter operator */
  op: 'eq' | 'ne' | 'gt' | 'gte' | 'lt' | 'lte' | 'in' | 'isNull' | 'isNotNull'
  /** Filter value */
  value?: unknown
}

// =============================================================================
// Writer Types
// =============================================================================

/** Options for ParquetWriter */
export interface ParquetWriterOptions {
  /** Compression codec */
  compression?: 'lz4' | 'snappy' | 'gzip' | 'zstd' | 'none'
  /** Row group size (number of rows per group) */
  rowGroupSize?: number
  /** Use dictionary encoding */
  dictionary?: boolean
  /** Page size in bytes */
  pageSize?: number
  /** Enable statistics */
  statistics?: boolean
  /** Key-value metadata to include */
  metadata?: Record<string, string>
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
  typeDef?: TypeDefinition
  /** Fields to shred from Variant for columnar efficiency */
  shredFields?: string[]
  /** Additional columns to include */
  additionalColumns?: ParquetSchema
}
