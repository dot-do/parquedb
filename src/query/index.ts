/**
 * Query Module for ParqueDB
 *
 * Provides predicate pushdown, query execution, bloom filter support,
 * and update operator engine for efficient Parquet queries.
 */

// Filter evaluation
export {
  matchesFilter,
  createPredicate,
  matchesCondition,
  deepEqual,
  compareValues,
  getValueType,
} from './filter'

// Update operators engine
export {
  applyUpdate,
  getField,
  setField,
  unsetField,
  validateUpdate,
  type UpdateApplyOptions,
} from './update'

// Predicate pushdown and row group filtering
export {
  selectRowGroups,
  couldMatch,
  toPredicate,
  extractFilterFields,
  extractRowGroupStats,
  type RowGroupStats,
  type ColumnStats,
  type ParquetMetadata,
  type ParquetRowGroup,
  type ParquetColumnChunk,
  type ParquetColumnStatistics,
  type ParquetSchemaElement,
} from './predicate'

// Query executor
export {
  QueryExecutor,
  type QueryResult,
  type QueryStats,
  type QueryPlan,
  type ParquetReader,
  type BloomFilterReader,
  type AggregationStage,
  type AggregationOptions,
} from './executor'

// Bloom filter support
export {
  checkBloomFilter,
  bloomFilterMightContain,
  createBloomFilter,
  bloomFilterAdd,
  bloomFilterMerge,
  bloomFilterEstimateCount,
  serializeBloomFilter,
  deserializeBloomFilter,
  createBloomFilterIndex,
  bloomFilterIndexAddRow,
  bloomFilterIndexMightMatch,
  serializeBloomFilterIndex,
  deserializeBloomFilterIndex,
  type BloomFilter,
  type BloomFilterConfig,
  type BloomFilterHeader,
  type BloomFilterIndex,
} from './bloom'

// Query builder
export {
  QueryBuilder,
  type ComparisonOp,
  type StringOp,
  type ExistenceOp,
  type QueryOp,
} from './builder'
