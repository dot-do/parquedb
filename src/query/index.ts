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
  // Use DEFAULT_FILTER_CONFIG or pass config explicitly to filter functions
  DEFAULT_FILTER_CONFIG,
  type FilterConfig,
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

// Typed mode predicate pushdown
export {
  filterToPredicates,
  predicatesToQueryFilter,
  analyzeFilterForPushdown,
  extractNonPushableFilter,
  canFullyPushdown,
  getPredicateColumns,
  mergePredicates,
  hasPushableConditions,
  type ParquetPredicate,
  type PredicateOp,
  type HyparquetFilter,
  type PredicatePushdownResult,
} from './predicate-pushdown'

// Vector query text-to-embedding conversion
export {
  normalizeVectorFilter,
  normalizeVectorFilterBatch,
  extractVectorQuery,
  isTextVectorQuery,
  type NormalizedVectorQuery,
  type VectorFilterNormalizationResult,
} from './vector-query'

// Columnar aggregations (optimized, no row materialization)
export {
  parquetAggregate,
  ColumnarAggregator,
  type AggregationType,
  type AggregationOp,
  type AggregationSpec,
  type AggregationResult,
  type AggregationStats,
} from './columnar-aggregations'

// Shredded Variant predicate pushdown
export {
  // Context and helper functions
  ShreddedPushdownContext,
  buildShreddingProperties,
  extractShreddedFilterPaths,
  hasShreddedConditions,
  estimatePushdownEffectiveness,

  // Re-exports from @dotdo/iceberg
  extractVariantShredConfig,
  parseShredColumnsProperty,
  parseShredFieldsProperty,
  parseFieldTypesProperty,
  getShredFieldsKey,
  getFieldTypesKey,
  VARIANT_SHRED_COLUMNS_KEY,
  extractVariantFilterColumns,
  assignShreddedFieldIds,
  filterDataFiles,
  filterDataFilesWithStats,
  shouldSkipDataFile,
  createRangePredicate,
  evaluateRangePredicate,
  combinePredicatesAnd,
  combinePredicatesOr,

  // Types
  type ShreddedPushdownConfig,
  type FilterResult,
  type ShreddedPushdownOptions,
  type VariantShredPropertyConfig,
  type FilterStats,
  type RangePredicate,
  type PredicateResult,
} from './shredded-pushdown'
