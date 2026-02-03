/**
 * Parquet Integration Layer for ParqueDB
 *
 * This module provides the Parquet reading/writing layer using hyparquet.
 * It works across Node.js, browsers, and Cloudflare Workers.
 *
 * @module parquet
 */

// =============================================================================
// Types
// =============================================================================

export type {
  // Schema types
  ParquetPrimitiveType,
  ParquetLogicalType,
  CompressionCodec,
  EncodingType,
  ParquetFieldSchema,
  ParquetSchema,

  // Metadata types
  ColumnChunkMetadata,
  ColumnStatistics,
  RowGroupMetadata,
  SortingColumn,
  ParquetMetadata,
  ParquetSchemaElement,
  KeyValueMetadata,

  // Reader types
  ParquetReaderOptions,
  ReadOptions,
  RowFilter,

  // Writer types
  ParquetWriterOptions,
  ParquetWriteResult,
  TypedWriteOptions,

  // Entity types
  EntityParquetSchema,
  CreateEntitySchemaOptions,

  // AsyncBuffer
  AsyncBuffer,
} from './types'

// =============================================================================
// Reader
// =============================================================================

export {
  ParquetReader,
  createAsyncBuffer,
  initializeAsyncBuffer,
  readParquetMetadata,
  readParquet,
} from './reader'

// =============================================================================
// Writer
// =============================================================================

export {
  ParquetWriter,
  writeParquet,
  appendParquet,
  compactParquet,
  writeTypedParquet,
} from './writer'

// =============================================================================
// Schema
// =============================================================================

export {
  inferParquetType,
  toParquetSchema,
  createEntitySchema,
  createRelationshipSchema,
  createEventSchema,
  validateParquetSchema,
  getShredFields,
  mergeSchemas,
  getColumnNames,
  hasColumn,
} from './schema'

// =============================================================================
// Variant
// =============================================================================

export {
  encodeVariant,
  decodeVariant,
  shredObject,
  mergeShredded,
  isEncodable,
  estimateVariantSize,
  variantEquals,
} from './variant'

// =============================================================================
// Compression
// =============================================================================

export {
  compressors,
  compressLz4,
  compressLz4Hadoop,
  writeCompressors,
} from './compression'

// =============================================================================
// Variant Shredding (Phase 1)
// =============================================================================

export {
  // Writer integration
  prepareShreddedVariantData,
  getDataShredFields,
  DEFAULT_DATA_SHRED_FIELDS,

  // Reader integration
  mapFilterToStatisticsPath,
  transformFilterForShredding,
  canPushdownWithShredding,

  // Predicate pushdown
  createShreddedPredicate,
  shouldSkipRowGroup,

  // Re-exports from hyparquet-writer
  createShreddedVariantColumn,
  getStatisticsPaths,
  mapFilterPathToStats,
} from './variant-shredding'

export type {
  VariantShredWriteConfig,
  ShreddedEntityData,
  ShreddedVariantReadConfig,
} from './variant-shredding'

// =============================================================================
// Schema Generator (for typed collections)
// =============================================================================

export {
  // Functions
  iceTypeToParquet,
  generateParquetSchema,
  generateMinimalSchema,
  schemaToColumnSources,
  validateSchemaTree,
  getSchemaColumnNames,
  schemaHasColumn,
  getRequiredColumns,
  getOptionalColumns,
} from './schema-generator'

export type {
  // Types
  ParquetType,
  SchemaField,
  SchemaTree,
  SchemaGeneratorOptions,
} from './schema-generator'
