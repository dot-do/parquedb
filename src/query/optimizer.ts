/**
 * Query Optimizer for ParqueDB
 *
 * Provides cost-based query optimization including:
 * - Predicate pushdown analysis
 * - Column pruning
 * - Index selection
 * - Join ordering (for future multi-collection queries)
 * - Cost estimation based on statistics
 *
 * The optimizer analyzes a query and produces an optimized QueryPlan
 * that minimizes I/O and CPU costs while maximizing the use of
 * Parquet's columnar format and available indexes.
 */

import type { Filter, FindOptions, Projection } from '../types'
import type { IndexManager, SelectedIndex } from '../indexes/manager'
import type { IndexDefinition } from '../indexes/types'
import {
  extractFilterFields,
  type RowGroupStats,
  selectRowGroups,
} from './predicate'
import {
  filterToPredicates,
  analyzeFilterForPushdown,
  type ParquetPredicate,
} from './predicate-pushdown'

// =============================================================================
// Cost Model Constants
// =============================================================================

/**
 * Cost constants for query operations
 * These are relative weights used for cost estimation
 */
export const COST_CONSTANTS = {
  /** Cost per row group scanned (I/O overhead) */
  ROW_GROUP_SCAN: 100,
  /** Cost per row read from Parquet */
  ROW_READ: 0.1,
  /** Cost per row filtered in memory */
  ROW_FILTER: 0.05,
  /** Cost per column read (I/O + decode) */
  COLUMN_READ: 10,
  /** Cost for sequential access vs random */
  SEQUENTIAL_FACTOR: 1,
  RANDOM_FACTOR: 5,
  /** Index lookup cost factors */
  FTS_INDEX_LOOKUP: 50,
  VECTOR_INDEX_LOOKUP: 100,
  /** Cost reduction factor when using bloom filter */
  BLOOM_FILTER_FACTOR: 0.3,
  /** Cost for sorting (per row) */
  SORT_PER_ROW: 0.2,
  /** Cost for projection (per field per row) */
  PROJECT_PER_FIELD: 0.01,
  // ==========================================================================
  // Vector Search Cost Model Constants
  // ==========================================================================
  /** Base cost for HNSW graph traversal per layer */
  HNSW_LAYER_TRAVERSAL: 5,
  /** Cost per distance computation (vector comparison) */
  VECTOR_DISTANCE_COMPUTE: 0.01,
  /** Cost multiplier for efSearch parameter (higher ef = more comparisons) */
  VECTOR_EF_FACTOR: 0.5,
  /** Cost for brute force scan per vector (used in pre-filter) */
  VECTOR_BRUTE_FORCE: 0.02,
  /** Overhead for hybrid search coordination */
  HYBRID_SEARCH_OVERHEAD: 20,
  /** Threshold for pre-filter efficiency (selectivity below this favors pre-filter) */
  HYBRID_PREFILTER_THRESHOLD: 0.3,
} as const

// =============================================================================
// Types
// =============================================================================

/**
 * Estimated cost for a query operation
 */
export interface QueryCost {
  /** Estimated I/O cost (reading from storage) */
  ioCost: number
  /** Estimated CPU cost (filtering, sorting, etc.) */
  cpuCost: number
  /** Total estimated cost */
  totalCost: number
  /** Estimated number of rows to scan */
  estimatedRowsScanned: number
  /** Estimated number of rows returned */
  estimatedRowsReturned: number
  /** Whether the estimate is precise or heuristic */
  isExact: boolean
}

/**
 * Index usage recommendation
 */
export interface IndexRecommendation {
  /** Index definition */
  index: IndexDefinition
  /** Type of index */
  type: 'fts' | 'vector'
  /** Field(s) the index covers */
  fields: string[]
  /** Estimated selectivity (0-1, lower is more selective) */
  selectivity: number
  /** Estimated cost reduction when using this index */
  costReduction: number
  /** Whether to use this index */
  recommended: boolean
  /** Reason for recommendation */
  reason: string
}

/**
 * Column pruning analysis
 */
export interface ColumnPruningResult {
  /** Columns needed for the filter */
  filterColumns: string[]
  /** Columns needed for projection */
  projectionColumns: string[]
  /** Columns needed for sorting */
  sortColumns: string[]
  /** All columns that must be read */
  requiredColumns: string[]
  /** Columns that can be pruned */
  prunedColumns: string[]
  /** Percentage of columns pruned */
  pruningRatio: number
}

/**
 * Predicate pushdown analysis
 */
export interface PredicatePushdownAnalysis {
  /** Predicates that can be pushed to Parquet level */
  pushedPredicates: ParquetPredicate[]
  /** Remaining filter for post-scan filtering */
  remainingFilter: Filter
  /** Estimated row groups that can be skipped */
  estimatedSkippedRowGroups: number
  /** Estimated rows eliminated by pushdown */
  estimatedRowsEliminated: number
  /** Columns used in pushed predicates */
  pushdownColumns: string[]
}

/**
 * Complete optimized query plan
 */
export interface OptimizedQueryPlan {
  /** Original filter */
  originalFilter: Filter
  /** Optimized filter (may be rewritten) */
  optimizedFilter: Filter
  /** Predicate pushdown analysis */
  predicatePushdown: PredicatePushdownAnalysis
  /** Column pruning analysis */
  columnPruning: ColumnPruningResult
  /** Index recommendation (if applicable) */
  indexRecommendation?: IndexRecommendation | undefined
  /** Vector search plan (if $vector query) */
  vectorSearchPlan?: VectorSearchPlan | undefined
  /** Estimated cost */
  estimatedCost: QueryCost
  /** Optimization suggestions */
  suggestions: OptimizationSuggestion[]
  /** Whether the query is optimal */
  isOptimal: boolean
  /** Execution strategy */
  strategy: ExecutionStrategy
}

/**
 * Optimization suggestion
 */
export interface OptimizationSuggestion {
  /** Suggestion type */
  type: 'create_index' | 'rewrite_filter' | 'add_projection' | 'use_limit' | 'add_sort_index'
  /** Description of the suggestion */
  description: string
  /** Estimated improvement factor */
  estimatedImprovement: number
  /** Priority (1-10, higher is more impactful) */
  priority: number
}

/**
 * Execution strategy
 */
export type ExecutionStrategy =
  | 'full_scan'
  | 'index_scan'
  | 'fts_search'
  | 'vector_search'
  | 'hybrid_search'

/**
 * Vector search plan details for explain output
 */
export interface VectorSearchPlan {
  /** Vector index name */
  indexName: string
  /** Field being searched */
  field: string
  /** Number of results requested */
  topK: number
  /** Minimum similarity score threshold */
  minScore?: number | undefined
  /** efSearch parameter for HNSW */
  efSearch: number
  /** Estimated index size (number of vectors) */
  estimatedIndexSize?: number | undefined
  /** Vector dimensions */
  dimensions?: number | undefined
  /** Distance metric used */
  metric?: 'cosine' | 'euclidean' | 'dot' | undefined
  /** Hybrid search details (if applicable) */
  hybridSearch?: {
    /** Strategy used */
    strategy: 'pre-filter' | 'post-filter' | 'auto'
    /** Actual strategy selected (if auto) */
    selectedStrategy?: 'pre-filter' | 'post-filter' | undefined
    /** Reason for strategy selection */
    strategyReason: string
    /** Estimated filter selectivity (0-1) */
    filterSelectivity?: number | undefined
    /** Estimated candidate count after pre-filter */
    preFilterCandidates?: number | undefined
    /** Over-fetch multiplier for post-filter */
    overFetchMultiplier?: number | undefined
  } | undefined
  /** Cost breakdown */
  costBreakdown: {
    /** Cost for HNSW traversal */
    hnswTraversalCost: number
    /** Cost for distance computations */
    distanceComputeCost: number
    /** Cost for hybrid coordination (if applicable) */
    hybridOverheadCost: number
    /** Total vector search cost */
    totalCost: number
  }
}

/**
 * Statistics for cost estimation
 */
export interface TableStatistics {
  /** Total number of rows */
  totalRows: number
  /** Number of row groups */
  rowGroupCount: number
  /** Average rows per row group */
  avgRowsPerGroup: number
  /** Column cardinality estimates */
  columnCardinality: Map<string, number>
  /** Column null counts */
  columnNullCounts: Map<string, number>
  /** Available indexes */
  indexes: IndexDefinition[]
  /** Row group statistics */
  rowGroupStats?: RowGroupStats[] | undefined
}

// =============================================================================
// Query Optimizer
// =============================================================================

/**
 * Query Optimizer for ParqueDB
 *
 * Analyzes queries and produces optimized execution plans
 */
export class QueryOptimizer {
  constructor(private indexManager?: IndexManager) {}

  /**
   * Set the index manager for index-aware optimization
   */
  setIndexManager(indexManager: IndexManager): void {
    this.indexManager = indexManager
  }

  /**
   * Optimize a query and produce an execution plan
   *
   * @param ns - Namespace (collection name)
   * @param filter - MongoDB-style filter
   * @param options - Find options
   * @param statistics - Table statistics for cost estimation
   * @returns Optimized query plan
   */
  async optimize<T>(
    ns: string,
    filter: Filter,
    options: FindOptions<T> = {},
    statistics?: TableStatistics
  ): Promise<OptimizedQueryPlan> {
    // 1. Analyze predicate pushdown
    const predicatePushdown = this.analyzePredicatePushdown(filter, statistics)

    // 2. Analyze column pruning
    const columnPruning = this.analyzeColumnPruning(filter, options)

    // 3. Analyze index usage
    const indexRecommendation = await this.analyzeIndexUsage(ns, filter, statistics)

    // 4. Determine execution strategy
    const strategy = this.determineStrategy(filter, indexRecommendation)

    // 5. Analyze vector search if applicable
    const vectorSearchPlan = this.analyzeVectorSearch(
      filter,
      indexRecommendation,
      statistics
    )

    // 6. Estimate cost
    const estimatedCost = this.estimateCost(
      filter,
      options,
      predicatePushdown,
      columnPruning,
      indexRecommendation,
      statistics,
      vectorSearchPlan
    )

    // 7. Generate optimization suggestions
    const suggestions = this.generateSuggestions(
      filter,
      options,
      predicatePushdown,
      columnPruning,
      indexRecommendation,
      statistics
    )

    // 8. Optimize filter (rewrite for better performance)
    const optimizedFilter = this.optimizeFilter(filter)

    // 9. Determine if query is optimal
    const isOptimal = suggestions.filter(s => s.priority >= 7).length === 0

    return {
      originalFilter: filter,
      optimizedFilter,
      predicatePushdown,
      columnPruning,
      indexRecommendation,
      vectorSearchPlan,
      estimatedCost,
      suggestions,
      isOptimal,
      strategy,
    }
  }

  /**
   * Analyze predicate pushdown opportunities
   */
  private analyzePredicatePushdown(
    filter: Filter,
    statistics?: TableStatistics
  ): PredicatePushdownAnalysis {
    // Get all fields in the filter
    const filterFields = extractFilterFields(filter)

    // Determine which columns are typed (can be pushed down)
    // If no statistics, assume all columns are pushable
    const typedColumns = new Set(filterFields)

    // Analyze pushdown
    const pushdownResult = analyzeFilterForPushdown(filter, typedColumns)

    // Estimate row groups and rows that can be skipped
    let estimatedSkippedRowGroups = 0
    let estimatedRowsEliminated = 0

    if (statistics?.rowGroupStats) {
      const selectedRowGroups = selectRowGroups(filter, statistics.rowGroupStats)
      estimatedSkippedRowGroups = statistics.rowGroupCount - selectedRowGroups.length

      // Estimate rows eliminated
      if (statistics.totalRows > 0 && statistics.rowGroupCount > 0) {
        const avgRowsPerGroup = statistics.totalRows / statistics.rowGroupCount
        estimatedRowsEliminated = estimatedSkippedRowGroups * avgRowsPerGroup
      }
    } else if (statistics) {
      // Heuristic: estimate 30% of row groups can be skipped with good predicates
      const pushableRatio = pushdownResult.pushdownPredicates.length > 0 ? 0.3 : 0
      estimatedSkippedRowGroups = Math.floor(statistics.rowGroupCount * pushableRatio)
      estimatedRowsEliminated = Math.floor(statistics.totalRows * pushableRatio)
    }

    return {
      pushedPredicates: pushdownResult.pushdownPredicates,
      remainingFilter: pushdownResult.remainingFilter,
      estimatedSkippedRowGroups,
      estimatedRowsEliminated,
      pushdownColumns: pushdownResult.pushdownColumns,
    }
  }

  /**
   * Analyze column pruning opportunities
   */
  private analyzeColumnPruning<T>(
    filter: Filter,
    options: FindOptions<T>
  ): ColumnPruningResult {
    // Extract columns needed for filtering
    const filterColumns = extractFilterFields(filter)

    // Extract columns needed for projection
    let projectionColumns: string[] = []
    if (options.project) {
      const projection = options.project as Projection
      projectionColumns = Object.keys(projection).filter(k => {
        const val = projection[k]
        return val === 1 || val === true
      })

      // If exclusion mode, we can't prune
      const hasExclusions = Object.values(projection).some(v => v === 0 || v === false)
      if (hasExclusions) {
        projectionColumns = [] // Will read all columns
      }
    }

    // Extract columns needed for sorting
    const sortColumns = options.sort ? Object.keys(options.sort) : []

    // Core columns always needed
    const coreColumns = ['$id', '$type', 'name', 'createdAt', 'updatedAt', 'deletedAt', 'version']

    // Combine all required columns
    const requiredColumnsSet = new Set([
      ...filterColumns,
      ...projectionColumns,
      ...sortColumns,
      ...coreColumns,
    ])
    const requiredColumns = Array.from(requiredColumnsSet)

    // For now, we don't know total columns, so can't compute pruned columns
    // This would require schema information
    const prunedColumns: string[] = []
    const pruningRatio = projectionColumns.length > 0
      ? 1 - (requiredColumns.length / Math.max(requiredColumns.length + 10, 20))
      : 0

    return {
      filterColumns,
      projectionColumns,
      sortColumns,
      requiredColumns,
      prunedColumns,
      pruningRatio,
    }
  }

  /**
   * Analyze index usage opportunities
   */
  private async analyzeIndexUsage(
    ns: string,
    filter: Filter,
    statistics?: TableStatistics
  ): Promise<IndexRecommendation | undefined> {
    if (!this.indexManager) {
      return undefined
    }

    // Check if there's an applicable index
    const selectedIndex = await this.indexManager.selectIndex(ns, filter)

    if (!selectedIndex) {
      return undefined
    }

    // Calculate selectivity and cost reduction
    const selectivity = this.estimateSelectivity(filter, selectedIndex, statistics)
    const costReduction = this.estimateIndexCostReduction(selectedIndex, selectivity, statistics)

    return {
      index: selectedIndex.index,
      type: selectedIndex.type,
      fields: selectedIndex.index.fields.map(f => f.path),
      selectivity,
      costReduction,
      recommended: costReduction > 0.2, // Recommend if >20% cost reduction
      reason: costReduction > 0.2
        ? `Index ${selectedIndex.index.name} can reduce query cost by ${(costReduction * 100).toFixed(0)}%`
        : `Index available but selectivity too low (${(selectivity * 100).toFixed(0)}%)`,
    }
  }

  /**
   * Analyze vector search parameters and build a vector search plan
   */
  private analyzeVectorSearch(
    filter: Filter,
    indexRecommendation?: IndexRecommendation,
    statistics?: TableStatistics
  ): VectorSearchPlan | undefined {
    if (!filter.$vector) {
      return undefined
    }

    const vq = filter.$vector as {
      // New format
      query?: number[] | string | undefined
      field?: string | undefined
      topK?: number | undefined
      minScore?: number | undefined
      efSearch?: number | undefined
      strategy?: 'pre-filter' | 'post-filter' | 'auto' | undefined
      // Legacy format
      $near?: number[] | undefined
      $k?: number | undefined
      $field?: string | undefined
      $minScore?: number | undefined
    }

    // Extract parameters from both new and legacy formats
    const queryVector = vq.query ?? vq.$near
    const field = vq.field ?? vq.$field ?? 'embedding'
    const topK = vq.topK ?? vq.$k ?? 10
    const minScore = vq.minScore ?? vq.$minScore
    const efSearch = vq.efSearch ?? Math.max(topK * 2, 50) // Default heuristic
    const strategy = vq.strategy ?? 'auto'

    // Get index info if available
    const indexName = indexRecommendation?.index?.name ?? `idx_${field}`
    const dimensions = typeof queryVector === 'object' && Array.isArray(queryVector)
      ? queryVector.length
      : undefined
    const metric = indexRecommendation?.index?.vectorOptions?.metric ?? 'cosine'

    // Estimate index size from statistics
    const estimatedIndexSize = statistics?.totalRows ?? 10000

    // Check if this is a hybrid search (has non-vector filters)
    const metadataFilterKeys = Object.keys(filter).filter(
      k => k !== '$vector' && k !== '$and' && k !== '$or' && k !== '$not'
    )
    const hasMetadataFilter = metadataFilterKeys.length > 0

    // Build hybrid search details if applicable
    let hybridSearch: VectorSearchPlan['hybridSearch'] | undefined

    if (hasMetadataFilter) {
      // Estimate filter selectivity based on available statistics
      const filterSelectivity = this.estimateFilterSelectivity(filter, statistics)

      // Determine strategy based on selectivity and index size
      let selectedStrategy: 'pre-filter' | 'post-filter'
      let strategyReason: string

      if (strategy === 'auto') {
        // Cost-based strategy selection
        const preFilterCost = this.estimatePreFilterCost(filterSelectivity, estimatedIndexSize, topK)
        const postFilterCost = this.estimatePostFilterCost(filterSelectivity, estimatedIndexSize, topK, efSearch)

        if (preFilterCost < postFilterCost) {
          selectedStrategy = 'pre-filter'
          strategyReason = `Pre-filter selected: filter selectivity ${(filterSelectivity * 100).toFixed(1)}% ` +
            `reduces candidate set significantly (estimated cost: ${preFilterCost.toFixed(0)} vs ${postFilterCost.toFixed(0)} for post-filter)`
        } else {
          selectedStrategy = 'post-filter'
          strategyReason = `Post-filter selected: filter selectivity ${(filterSelectivity * 100).toFixed(1)}% ` +
            `is too high for efficient pre-filtering (estimated cost: ${postFilterCost.toFixed(0)} vs ${preFilterCost.toFixed(0)} for pre-filter)`
        }
      } else {
        selectedStrategy = strategy
        strategyReason = `Strategy explicitly set to ${strategy}`
      }

      const preFilterCandidates = selectedStrategy === 'pre-filter'
        ? Math.floor(estimatedIndexSize * filterSelectivity)
        : undefined

      hybridSearch = {
        strategy,
        selectedStrategy,
        strategyReason,
        filterSelectivity,
        preFilterCandidates,
        overFetchMultiplier: selectedStrategy === 'post-filter' ? 3 : undefined,
      }
    }

    // Calculate cost breakdown
    const hnswTraversalCost = this.calculateHnswTraversalCost(estimatedIndexSize, efSearch)
    const distanceComputeCost = this.calculateDistanceComputeCost(
      hybridSearch?.selectedStrategy === 'pre-filter'
        ? (hybridSearch.preFilterCandidates ?? estimatedIndexSize)
        : estimatedIndexSize,
      efSearch,
      dimensions ?? 128
    )
    const hybridOverheadCost = hasMetadataFilter ? COST_CONSTANTS.HYBRID_SEARCH_OVERHEAD : 0

    return {
      indexName,
      field,
      topK,
      minScore,
      efSearch,
      estimatedIndexSize,
      dimensions,
      metric,
      hybridSearch,
      costBreakdown: {
        hnswTraversalCost,
        distanceComputeCost,
        hybridOverheadCost,
        totalCost: hnswTraversalCost + distanceComputeCost + hybridOverheadCost,
      },
    }
  }

  /**
   * Estimate filter selectivity (0-1, what fraction of rows pass the filter)
   */
  private estimateFilterSelectivity(filter: Filter, statistics?: TableStatistics): number {
    // Without statistics, use conservative estimate
    if (!statistics) {
      return 0.3 // Default 30% selectivity
    }

    let selectivity = 1.0

    for (const [key, value] of Object.entries(filter)) {
      if (key.startsWith('$')) continue

      // Get cardinality for this field if available
      const cardinality = statistics.columnCardinality.get(key)

      if (cardinality) {
        if (value === null || typeof value !== 'object') {
          // Equality: 1/cardinality
          selectivity *= 1 / cardinality
        } else if (typeof value === 'object' && value !== null) {
          const op = value as Record<string, unknown>
          if ('$eq' in op) {
            selectivity *= 1 / cardinality
          } else if ('$in' in op && Array.isArray(op.$in)) {
            selectivity *= op.$in.length / cardinality
          } else if ('$gt' in op || '$gte' in op || '$lt' in op || '$lte' in op) {
            // Range query: estimate ~33% of values
            selectivity *= 0.33
          } else if ('$ne' in op) {
            // Not equal: most values
            selectivity *= (cardinality - 1) / cardinality
          }
        }
      } else {
        // No cardinality info, use heuristic
        selectivity *= 0.3
      }
    }

    return Math.max(0.001, Math.min(1, selectivity))
  }

  /**
   * Estimate cost of pre-filter strategy
   */
  private estimatePreFilterCost(selectivity: number, indexSize: number, _topK: number): number {
    // Pre-filter: scan parquet for filter, then brute-force vector search on candidates
    const candidateCount = Math.floor(indexSize * selectivity)
    const parquetScanCost = indexSize * COST_CONSTANTS.ROW_READ * 0.1 // Fast filter scan
    const bruteForceVectorCost = candidateCount * COST_CONSTANTS.VECTOR_BRUTE_FORCE

    return parquetScanCost + bruteForceVectorCost + COST_CONSTANTS.HYBRID_SEARCH_OVERHEAD
  }

  /**
   * Estimate cost of post-filter strategy
   */
  private estimatePostFilterCost(
    selectivity: number,
    indexSize: number,
    topK: number,
    efSearch: number
  ): number {
    // Post-filter: HNSW search with over-fetching, then filter results
    const overFetchMultiplier = 3
    const hnswCost = this.calculateHnswTraversalCost(indexSize, efSearch)
    const fetchedCount = topK * overFetchMultiplier
    const filterCost = fetchedCount * COST_CONSTANTS.ROW_FILTER
    // Risk: we might not get enough results after filtering
    const resultRisk = selectivity < 0.3 ? 50 : 0 // Penalty for low selectivity

    return hnswCost + filterCost + resultRisk + COST_CONSTANTS.HYBRID_SEARCH_OVERHEAD
  }

  /**
   * Calculate HNSW graph traversal cost
   */
  private calculateHnswTraversalCost(indexSize: number, efSearch: number): number {
    // HNSW has O(log n) layers, each with efSearch comparisons
    const estimatedLayers = Math.max(1, Math.floor(Math.log2(indexSize) / 2))
    return estimatedLayers * COST_CONSTANTS.HNSW_LAYER_TRAVERSAL +
           efSearch * COST_CONSTANTS.VECTOR_EF_FACTOR
  }

  /**
   * Calculate distance computation cost
   */
  private calculateDistanceComputeCost(
    candidateCount: number,
    efSearch: number,
    dimensions: number
  ): number {
    // Each distance computation scales with vector dimensions
    const comparisons = Math.min(candidateCount, efSearch * 2) // HNSW limits comparisons
    const dimensionFactor = dimensions / 128 // Normalize to typical embedding size
    return comparisons * COST_CONSTANTS.VECTOR_DISTANCE_COMPUTE * dimensionFactor
  }

  /**
   * Determine the best execution strategy
   */
  private determineStrategy(
    filter: Filter,
    indexRecommendation?: IndexRecommendation
  ): ExecutionStrategy {
    // Check for special operators
    if (filter.$text) {
      return 'fts_search'
    }

    if (filter.$vector) {
      // Check if there are also regular filters (hybrid search)
      const hasRegularFilters = Object.keys(filter).some(
        k => !k.startsWith('$') || (k !== '$vector' && k !== '$and' && k !== '$or')
      )
      return hasRegularFilters ? 'hybrid_search' : 'vector_search'
    }

    // Check if index is recommended
    if (indexRecommendation?.recommended) {
      return 'index_scan'
    }

    return 'full_scan'
  }

  /**
   * Estimate query execution cost
   */
  private estimateCost<T>(
    _filter: Filter,
    options: FindOptions<T>,
    predicatePushdown: PredicatePushdownAnalysis,
    columnPruning: ColumnPruningResult,
    indexRecommendation?: IndexRecommendation,
    statistics?: TableStatistics,
    vectorSearchPlan?: VectorSearchPlan
  ): QueryCost {
    const totalRows = statistics?.totalRows ?? 10000
    const rowGroupCount = statistics?.rowGroupCount ?? 10
    const columnCount = columnPruning.requiredColumns.length

    // Calculate I/O cost
    let ioCost = 0

    // Row group scanning cost
    const rowGroupsToScan = rowGroupCount - predicatePushdown.estimatedSkippedRowGroups
    ioCost += rowGroupsToScan * COST_CONSTANTS.ROW_GROUP_SCAN

    // Column reading cost
    ioCost += columnCount * COST_CONSTANTS.COLUMN_READ * rowGroupsToScan

    // Adjust for index usage
    if (indexRecommendation?.recommended) {
      if (indexRecommendation.type === 'fts') {
        ioCost = COST_CONSTANTS.FTS_INDEX_LOOKUP + (ioCost * indexRecommendation.selectivity)
      } else if (indexRecommendation.type === 'vector') {
        // Use detailed vector cost model if available
        if (vectorSearchPlan) {
          ioCost = vectorSearchPlan.costBreakdown.totalCost
        } else {
          ioCost = COST_CONSTANTS.VECTOR_INDEX_LOOKUP + (ioCost * indexRecommendation.selectivity)
        }
      }
    } else if (vectorSearchPlan) {
      // Vector search without index recommendation (standalone $vector query)
      ioCost = vectorSearchPlan.costBreakdown.totalCost
    }

    // Calculate CPU cost
    let cpuCost = 0

    // Estimated rows to scan after pushdown
    const rowsToScan = totalRows - predicatePushdown.estimatedRowsEliminated
    cpuCost += rowsToScan * COST_CONSTANTS.ROW_READ

    // Filtering cost for remaining filter
    const remainingFilterFields = Object.keys(predicatePushdown.remainingFilter).length
    if (remainingFilterFields > 0) {
      cpuCost += rowsToScan * COST_CONSTANTS.ROW_FILTER * remainingFilterFields
    }

    // Sorting cost
    if (options.sort) {
      const sortFields = Object.keys(options.sort).length
      cpuCost += rowsToScan * COST_CONSTANTS.SORT_PER_ROW * Math.log2(Math.max(rowsToScan, 2)) * sortFields
    }

    // Projection cost
    if (options.project) {
      const projectFields = Object.keys(options.project).length
      cpuCost += rowsToScan * COST_CONSTANTS.PROJECT_PER_FIELD * projectFields
    }

    // Estimate rows returned
    let estimatedRowsReturned = rowsToScan
    if (indexRecommendation?.recommended) {
      estimatedRowsReturned = Math.floor(totalRows * indexRecommendation.selectivity)
    }
    if (options.limit) {
      estimatedRowsReturned = Math.min(estimatedRowsReturned, options.limit)
    }

    return {
      ioCost,
      cpuCost,
      totalCost: ioCost + cpuCost,
      estimatedRowsScanned: rowsToScan,
      estimatedRowsReturned,
      isExact: !!statistics?.rowGroupStats,
    }
  }

  /**
   * Generate optimization suggestions
   */
  private generateSuggestions<T>(
    _filter: Filter,
    options: FindOptions<T>,
    predicatePushdown: PredicatePushdownAnalysis,
    columnPruning: ColumnPruningResult,
    indexRecommendation?: IndexRecommendation,
    statistics?: TableStatistics
  ): OptimizationSuggestion[] {
    const suggestions: OptimizationSuggestion[] = []

    // Suggest creating an index if there's a high-selectivity filter without one
    if (!indexRecommendation && predicatePushdown.pushdownColumns.length > 0) {
      const primaryFilterField = predicatePushdown.pushdownColumns[0]
      if (primaryFilterField && !primaryFilterField.startsWith('$')) {
        suggestions.push({
          type: 'create_index',
          description: `Consider creating an index on '${primaryFilterField}' for faster queries`,
          estimatedImprovement: 0.5,
          priority: 6,
        })
      }
    }

    // Suggest using projection if reading all columns
    if (!options.project && columnPruning.filterColumns.length < 5) {
      suggestions.push({
        type: 'add_projection',
        description: 'Add a projection to read only required columns, reducing I/O',
        estimatedImprovement: 0.3,
        priority: 4,
      })
    }

    // Suggest adding a limit if scanning many rows
    if (!options.limit && statistics && statistics.totalRows > 1000) {
      suggestions.push({
        type: 'use_limit',
        description: 'Consider adding a limit to reduce memory usage for large result sets',
        estimatedImprovement: 0.2,
        priority: 3,
      })
    }

    // Suggest rewriting filter for better pushdown
    const remainingFilterKeys = Object.keys(predicatePushdown.remainingFilter)
      .filter(k => !k.startsWith('$'))
    if (remainingFilterKeys.length > 0) {
      const hasOrOperator = !!predicatePushdown.remainingFilter.$or

      if (hasOrOperator) {
        suggestions.push({
          type: 'rewrite_filter',
          description: 'Consider rewriting $or conditions as separate queries with UNION for better pushdown',
          estimatedImprovement: 0.4,
          priority: 5,
        })
      }

      // Check for regex that could be startsWith
      for (const key of remainingFilterKeys) {
        const value = predicatePushdown.remainingFilter[key]
        if (typeof value === 'object' && value !== null && '$regex' in value) {
          const regex = (value as { $regex: string | RegExp }).$regex
          const regexStr = typeof regex === 'string' ? regex : regex.source
          if (regexStr.startsWith('^') && !regexStr.includes('*') && !regexStr.includes('+')) {
            suggestions.push({
              type: 'rewrite_filter',
              description: `Consider using $startsWith instead of $regex for '${key}' for better performance`,
              estimatedImprovement: 0.2,
              priority: 4,
            })
          }
        }
      }
    }

    // Suggest sort index if sorting large datasets
    if (options.sort && statistics && statistics.totalRows > 10000) {
      const sortFields = Object.keys(options.sort)
      suggestions.push({
        type: 'add_sort_index',
        description: `Consider ensuring data is pre-sorted by [${sortFields.join(', ')}] in Parquet for efficient sorting`,
        estimatedImprovement: 0.3,
        priority: 5,
      })
    }

    // Sort suggestions by priority
    suggestions.sort((a, b) => b.priority - a.priority)

    return suggestions
  }

  /**
   * Optimize the filter for better performance
   */
  private optimizeFilter(filter: Filter): Filter {
    // Create a copy to avoid mutating the original
    const optimized = { ...filter }

    // Optimization 1: Flatten nested $and operators
    if (optimized.$and) {
      const flattenedConditions: Filter[] = []

      const flattenAnd = (conditions: Filter[]) => {
        for (const condition of conditions) {
          if (condition.$and) {
            flattenAnd(condition.$and)
          } else {
            flattenedConditions.push(condition)
          }
        }
      }

      flattenAnd(optimized.$and)
      optimized.$and = flattenedConditions
    }

    // Optimization 2: Move most selective filters first in $and
    // (This is a heuristic - equality > range > other)
    if (optimized.$and && optimized.$and.length > 1) {
      optimized.$and = [...optimized.$and].sort((a, b) => {
        const scoreA = this.getFilterSelectivityScore(a)
        const scoreB = this.getFilterSelectivityScore(b)
        return scoreA - scoreB // Lower score (more selective) first
      })
    }

    // Optimization 3: Convert single-element $and to flat filter
    if (optimized.$and && optimized.$and.length === 1) {
      const singleFilter = optimized.$and[0]!
      delete optimized.$and
      Object.assign(optimized, singleFilter)
    }

    return optimized
  }

  /**
   * Get a selectivity score for a filter (lower = more selective)
   */
  private getFilterSelectivityScore(filter: Filter): number {
    let score = 0

    for (const [key, value] of Object.entries(filter)) {
      if (key.startsWith('$')) continue

      if (value === null || typeof value !== 'object') {
        // Direct equality - most selective
        score += 1
      } else if ('$eq' in value) {
        score += 1
      } else if ('$in' in value) {
        const inArray = (value as { $in: unknown[] }).$in
        score += 2 + (inArray.length * 0.1)
      } else if ('$gt' in value || '$gte' in value || '$lt' in value || '$lte' in value) {
        score += 3
      } else if ('$ne' in value || '$nin' in value) {
        score += 5
      } else if ('$regex' in value || '$startsWith' in value || '$contains' in value) {
        score += 4
      } else {
        score += 6
      }
    }

    // Logical operators add complexity
    if (filter.$or) score += 10
    if (filter.$not) score += 8
    if (filter.$nor) score += 10

    return score
  }

  /**
   * Estimate selectivity for an index
   */
  private estimateSelectivity(
    _filter: Filter,
    selectedIndex: SelectedIndex,
    statistics?: TableStatistics
  ): number {
    // Default selectivity if no statistics
    if (!statistics) {
      // Heuristic based on index type
      if (selectedIndex.type === 'fts') {
        return 0.1 // FTS typically returns ~10% of documents
      }
      if (selectedIndex.type === 'vector') {
        // Vector search selectivity depends on k parameter
        const k = (selectedIndex.condition as { topK?: number | undefined; $k?: number | undefined })?.topK
          ?? (selectedIndex.condition as { topK?: number | undefined; $k?: number | undefined })?.$k
          ?? 10
        return Math.min(k / 1000, 0.1) // Assume 1000 documents min
      }
      return 0.3 // Default
    }

    // Calculate based on statistics
    if (selectedIndex.type === 'vector') {
      const k = (selectedIndex.condition as { topK?: number | undefined; $k?: number | undefined })?.topK
        ?? (selectedIndex.condition as { topK?: number | undefined; $k?: number | undefined })?.$k
        ?? 10
      return Math.min(k / statistics.totalRows, 1)
    }

    // For FTS, estimate based on typical search patterns
    if (selectedIndex.type === 'fts') {
      return 0.1
    }

    return 0.3
  }

  /**
   * Estimate cost reduction from using an index
   */
  private estimateIndexCostReduction(
    selectedIndex: SelectedIndex,
    selectivity: number,
    statistics?: TableStatistics
  ): number {
    // Base cost reduction from selectivity
    let reduction = 1 - selectivity

    // Adjust based on index type
    if (selectedIndex.type === 'vector') {
      // Vector indexes have overhead but are very selective
      reduction = Math.max(reduction - 0.1, 0)
    }

    if (selectedIndex.type === 'fts') {
      // FTS has moderate overhead
      reduction = Math.max(reduction - 0.05, 0)
    }

    // Adjust based on data size
    if (statistics && statistics.totalRows > 100000) {
      // Indexes are more beneficial for larger datasets
      reduction = Math.min(reduction * 1.2, 0.95)
    }

    return reduction
  }

  /**
   * Compare two query plans and return the better one
   */
  comparePlans(planA: OptimizedQueryPlan, planB: OptimizedQueryPlan): OptimizedQueryPlan {
    // Compare by total cost
    if (planA.estimatedCost.totalCost < planB.estimatedCost.totalCost) {
      return planA
    }
    return planB
  }

  /**
   * Explain a query plan in human-readable format
   */
  explainPlan(plan: OptimizedQueryPlan): string {
    const lines: string[] = [
      '=== Query Optimization Report ===',
      '',
      `Execution Strategy: ${plan.strategy}`,
      `Is Optimal: ${plan.isOptimal ? 'Yes' : 'No'}`,
      '',
      '--- Cost Estimate ---',
      `I/O Cost: ${plan.estimatedCost.ioCost.toFixed(2)}`,
      `CPU Cost: ${plan.estimatedCost.cpuCost.toFixed(2)}`,
      `Total Cost: ${plan.estimatedCost.totalCost.toFixed(2)}`,
      `Estimated Rows Scanned: ${plan.estimatedCost.estimatedRowsScanned}`,
      `Estimated Rows Returned: ${plan.estimatedCost.estimatedRowsReturned}`,
      '',
      '--- Predicate Pushdown ---',
      `Pushed Predicates: ${plan.predicatePushdown.pushedPredicates.length}`,
      `Estimated Row Groups Skipped: ${plan.predicatePushdown.estimatedSkippedRowGroups}`,
      `Estimated Rows Eliminated: ${plan.predicatePushdown.estimatedRowsEliminated}`,
    ]

    if (plan.predicatePushdown.pushedPredicates.length > 0) {
      lines.push('Pushed:')
      for (const pred of plan.predicatePushdown.pushedPredicates) {
        lines.push(`  - ${pred.column} ${pred.op} ${JSON.stringify(pred.value)}`)
      }
    }

    const remainingKeys = Object.keys(plan.predicatePushdown.remainingFilter)
    if (remainingKeys.length > 0) {
      lines.push('Remaining (post-scan):')
      lines.push(`  ${JSON.stringify(plan.predicatePushdown.remainingFilter)}`)
    }

    lines.push('')
    lines.push('--- Column Pruning ---')
    lines.push(`Required Columns: ${plan.columnPruning.requiredColumns.join(', ')}`)
    lines.push(`Pruning Ratio: ${(plan.columnPruning.pruningRatio * 100).toFixed(0)}%`)

    if (plan.indexRecommendation) {
      lines.push('')
      lines.push('--- Index Usage ---')
      lines.push(`Index: ${plan.indexRecommendation.index.name} (${plan.indexRecommendation.type})`)
      lines.push(`Fields: ${plan.indexRecommendation.fields.join(', ')}`)
      lines.push(`Recommended: ${plan.indexRecommendation.recommended ? 'Yes' : 'No'}`)
      lines.push(`Reason: ${plan.indexRecommendation.reason}`)
      lines.push(`Selectivity: ${(plan.indexRecommendation.selectivity * 100).toFixed(1)}%`)
      lines.push(`Cost Reduction: ${(plan.indexRecommendation.costReduction * 100).toFixed(0)}%`)
    }

    // Vector search plan details
    if (plan.vectorSearchPlan) {
      const vsp = plan.vectorSearchPlan
      lines.push('')
      lines.push('--- Vector Search Plan ---')
      lines.push(`Index: ${vsp.indexName}`)
      lines.push(`Field: ${vsp.field}`)
      lines.push(`Top K: ${vsp.topK}`)
      lines.push(`efSearch: ${vsp.efSearch}`)
      if (vsp.minScore !== undefined) {
        lines.push(`Min Score: ${vsp.minScore}`)
      }
      if (vsp.dimensions) {
        lines.push(`Dimensions: ${vsp.dimensions}`)
      }
      if (vsp.metric) {
        lines.push(`Distance Metric: ${vsp.metric}`)
      }
      if (vsp.estimatedIndexSize) {
        lines.push(`Estimated Index Size: ${vsp.estimatedIndexSize.toLocaleString()} vectors`)
      }

      // Hybrid search details
      if (vsp.hybridSearch) {
        lines.push('')
        lines.push('  Hybrid Search:')
        lines.push(`    Strategy: ${vsp.hybridSearch.strategy}`)
        if (vsp.hybridSearch.selectedStrategy && vsp.hybridSearch.strategy === 'auto') {
          lines.push(`    Selected: ${vsp.hybridSearch.selectedStrategy}`)
        }
        lines.push(`    Reason: ${vsp.hybridSearch.strategyReason}`)
        if (vsp.hybridSearch.filterSelectivity !== undefined) {
          lines.push(`    Filter Selectivity: ${(vsp.hybridSearch.filterSelectivity * 100).toFixed(1)}%`)
        }
        if (vsp.hybridSearch.preFilterCandidates !== undefined) {
          lines.push(`    Pre-filter Candidates: ${vsp.hybridSearch.preFilterCandidates.toLocaleString()}`)
        }
        if (vsp.hybridSearch.overFetchMultiplier !== undefined) {
          lines.push(`    Over-fetch Multiplier: ${vsp.hybridSearch.overFetchMultiplier}x`)
        }
      }

      // Cost breakdown
      lines.push('')
      lines.push('  Cost Breakdown:')
      lines.push(`    HNSW Traversal: ${vsp.costBreakdown.hnswTraversalCost.toFixed(2)}`)
      lines.push(`    Distance Compute: ${vsp.costBreakdown.distanceComputeCost.toFixed(2)}`)
      if (vsp.costBreakdown.hybridOverheadCost > 0) {
        lines.push(`    Hybrid Overhead: ${vsp.costBreakdown.hybridOverheadCost.toFixed(2)}`)
      }
      lines.push(`    Total Vector Cost: ${vsp.costBreakdown.totalCost.toFixed(2)}`)
    }

    if (plan.suggestions.length > 0) {
      lines.push('')
      lines.push('--- Suggestions ---')
      for (const suggestion of plan.suggestions) {
        lines.push(`[P${suggestion.priority}] ${suggestion.type}: ${suggestion.description}`)
        lines.push(`       Estimated Improvement: ${(suggestion.estimatedImprovement * 100).toFixed(0)}%`)
      }
    }

    return lines.join('\n')
  }
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Create a QueryOptimizer instance
 */
export function createQueryOptimizer(indexManager?: IndexManager): QueryOptimizer {
  return new QueryOptimizer(indexManager)
}

/**
 * Quick estimate of query cost without full optimization
 */
export function quickEstimateCost(
  filter: Filter,
  statistics: TableStatistics
): number {
  const predicates = filterToPredicates(filter)

  // Base cost from total rows
  let cost = statistics.totalRows * COST_CONSTANTS.ROW_READ

  // Reduce cost for pushable predicates
  if (predicates.length > 0) {
    cost *= 0.7 // Estimate 30% savings
  }

  // Increase cost for complex filters
  if (filter.$or) {
    cost *= 1.5
  }

  if (filter.$text) {
    cost += COST_CONSTANTS.FTS_INDEX_LOOKUP
    cost *= 0.3 // FTS is typically very selective
  }

  if (filter.$vector) {
    cost += COST_CONSTANTS.VECTOR_INDEX_LOOKUP
    cost *= 0.1 // Vector search returns limited results
  }

  return cost
}

/**
 * Check if a filter would benefit from an index
 */
export function wouldBenefitFromIndex(filter: Filter, statistics?: TableStatistics): boolean {
  // Always benefit if no index exists and data is large
  if (statistics && statistics.totalRows > 10000) {
    // Check for high-selectivity operators
    for (const [key, value] of Object.entries(filter)) {
      if (key.startsWith('$')) continue

      if (value === null || typeof value !== 'object') {
        return true // Equality - good for index
      }

      if (typeof value === 'object' && value !== null) {
        if ('$eq' in value || '$in' in value || '$gt' in value || '$gte' in value ||
            '$lt' in value || '$lte' in value) {
          return true
        }
      }
    }
  }

  // Special operators always benefit from indexes
  if (filter.$text || filter.$vector) {
    return true
  }

  return false
}

// =============================================================================
// Materialized View Routing
// =============================================================================

import type { MVDefinition } from '../materialized-views/types'
import type { StalenessState } from '../materialized-views/staleness'
import { logger } from '../utils/logger'

/**
 * Result of MV routing decision
 */
export interface MVRoutingResult {
  /** Whether the query can be satisfied by an MV */
  canUseMV: boolean

  /** The MV to use (if canUseMV is true) */
  mvName?: string | undefined

  /** The MV definition */
  mvDefinition?: MVDefinition | undefined

  /** Reason for the routing decision */
  reason: string

  /** Staleness state of the MV (if using an MV) */
  stalenessState?: StalenessState | undefined

  /** Whether the query needs additional filtering after MV */
  needsPostFilter: boolean

  /** Any additional filter to apply after MV results */
  postFilter?: Filter | undefined

  /** Estimated cost savings from using the MV (0-1) */
  costSavings: number
}

/**
 * MV metadata with staleness info for routing
 */
export interface MVRoutingMetadata {
  /** MV name */
  name: string

  /** MV definition */
  definition: MVDefinition

  /** Current staleness state */
  stalenessState: StalenessState

  /** Whether the MV is usable (fresh or within grace period) */
  usable: boolean

  /** Row count (for cost estimation) */
  rowCount?: number | undefined
}

/**
 * Provider interface for MV metadata
 */
export interface MVMetadataProvider {
  /**
   * Get all MVs that source from a given collection
   */
  getMVsForSource(source: string): Promise<MVRoutingMetadata[]>

  /**
   * Get MV metadata by name
   */
  getMVMetadata(name: string): Promise<MVRoutingMetadata | null>
}

/**
 * Materialized View Router
 *
 * Analyzes queries and determines if they can be satisfied by a materialized view
 * instead of querying the source collection directly.
 *
 * Routing criteria:
 * 1. MV must be fresh or within grace period
 * 2. MV filter must be a superset of or equal to query filter
 * 3. Query projection must be satisfiable by MV fields
 * 4. Query sort must be satisfiable by MV fields
 *
 * @example
 * ```typescript
 * const router = new MVRouter(metadataProvider)
 *
 * const result = await router.route('orders', { status: 'completed' })
 * if (result.canUseMV) {
 *   // Query MV instead of source
 *   const data = await queryMV(result.mvName)
 * }
 * ```
 */
export class MVRouter {
  constructor(private metadataProvider: MVMetadataProvider) {}

  /**
   * Route a query to an appropriate MV if possible
   *
   * @param sourceCollection - The source collection being queried
   * @param filter - The query filter
   * @param options - Query options (projection, sort, etc.)
   * @returns Routing decision
   */
  async route<T>(
    sourceCollection: string,
    filter: Filter = {},
    options: FindOptions<T> = {}
  ): Promise<MVRoutingResult> {
    // Get all MVs that source from this collection
    const mvs = await this.metadataProvider.getMVsForSource(sourceCollection)

    if (mvs.length === 0) {
      logger.debug(`[MVRouter] No MVs found for source: ${sourceCollection}`)
      return {
        canUseMV: false,
        reason: `No materialized views for collection: ${sourceCollection}`,
        needsPostFilter: false,
        costSavings: 0,
      }
    }

    logger.debug(`[MVRouter] Found ${mvs.length} MV(s) for source: ${sourceCollection}`)

    // Find the best matching MV
    let bestMatch: MVRoutingResult | null = null
    let bestScore = -1
    let firstRejectionReason: string | null = null

    for (const mv of mvs) {
      const result = this.evaluateMV(mv, filter, options)

      logger.debug(`[MVRouter] Evaluating MV "${mv.name}": canUse=${result.canUseMV}, score=${result.costSavings}`)

      if (result.canUseMV && result.costSavings > bestScore) {
        bestMatch = result
        bestScore = result.costSavings
      } else if (!result.canUseMV && !firstRejectionReason) {
        // Track the first rejection reason for better error messages
        firstRejectionReason = result.reason
      }
    }

    if (bestMatch) {
      logger.info(`[MVRouter] Selected MV "${bestMatch.mvName}" for query on "${sourceCollection}" (savings: ${(bestMatch.costSavings * 100).toFixed(0)}%)`)
      return bestMatch
    }

    // Return the first rejection reason if available, otherwise generic message
    const reason = firstRejectionReason || 'No suitable MV matches the query criteria'
    logger.debug(`[MVRouter] No suitable MV found for query on "${sourceCollection}": ${reason}`)
    return {
      canUseMV: false,
      reason,
      needsPostFilter: false,
      costSavings: 0,
    }
  }

  /**
   * Evaluate if a specific MV can satisfy a query
   */
  private evaluateMV<T>(
    mv: MVRoutingMetadata,
    filter: Filter,
    options: FindOptions<T>
  ): MVRoutingResult {
    const { definition, name, stalenessState, usable } = mv

    // Check 1: Is the MV usable (fresh or within grace period)?
    if (!usable) {
      return {
        canUseMV: false,
        reason: `MV "${name}" is stale and outside grace period`,
        needsPostFilter: false,
        costSavings: 0,
      }
    }

    // Check 2: Aggregation MVs can't satisfy regular queries
    if (definition.$groupBy || definition.$compute) {
      // Aggregation MVs have different schema, can only be queried directly
      return {
        canUseMV: false,
        reason: `MV "${name}" is an aggregation view (has $groupBy/$compute)`,
        needsPostFilter: false,
        costSavings: 0,
      }
    }

    // Check 3: Does the MV filter satisfy the query filter?
    const filterAnalysis = this.analyzeFilterCompatibility(definition.$filter, filter)
    if (!filterAnalysis.compatible) {
      return {
        canUseMV: false,
        reason: filterAnalysis.reason,
        needsPostFilter: false,
        costSavings: 0,
      }
    }

    // Check 4: Can the projection be satisfied?
    if (options.project) {
      const projectionAnalysis = this.analyzeProjectionCompatibility(definition, options.project)
      if (!projectionAnalysis.compatible) {
        return {
          canUseMV: false,
          reason: projectionAnalysis.reason,
          needsPostFilter: false,
          costSavings: 0,
        }
      }
    }

    // Check 5: Can the sort be satisfied?
    if (options.sort) {
      const sortAnalysis = this.analyzeSortCompatibility(definition, options.sort)
      if (!sortAnalysis.compatible) {
        return {
          canUseMV: false,
          reason: sortAnalysis.reason,
          needsPostFilter: false,
          costSavings: 0,
        }
      }
    }

    // Calculate cost savings estimate
    const costSavings = this.estimateCostSavings(mv, filter, filterAnalysis.needsPostFilter)

    return {
      canUseMV: true,
      mvName: name,
      mvDefinition: definition,
      reason: filterAnalysis.needsPostFilter
        ? `MV "${name}" can satisfy query with post-filtering`
        : `MV "${name}" fully satisfies query`,
      stalenessState,
      needsPostFilter: filterAnalysis.needsPostFilter,
      postFilter: filterAnalysis.postFilter,
      costSavings,
    }
  }

  /**
   * Analyze if an MV's filter is compatible with a query filter
   *
   * Compatibility rules:
   * - No MV filter: Query filter needs post-processing
   * - MV filter equals query filter: Fully compatible
   * - MV filter is subset of query filter: MV data contains all needed rows, need post-filter
   * - MV filter conflicts: Not compatible
   */
  private analyzeFilterCompatibility(
    mvFilter: Filter | undefined,
    queryFilter: Filter
  ): { compatible: boolean; needsPostFilter: boolean; postFilter?: Filter | undefined; reason: string } {
    // If no query filter, MV can be used (with any filter)
    if (!queryFilter || Object.keys(queryFilter).length === 0) {
      if (mvFilter && Object.keys(mvFilter).length > 0) {
        // MV has a filter but query doesn't - MV is a subset, which is fine
        return {
          compatible: true,
          needsPostFilter: false,
          reason: 'Query has no filter, MV provides filtered subset',
        }
      }
      return {
        compatible: true,
        needsPostFilter: false,
        reason: 'No filter required',
      }
    }

    // If no MV filter, all source data is in MV - need to post-filter
    if (!mvFilter || Object.keys(mvFilter).length === 0) {
      return {
        compatible: true,
        needsPostFilter: true,
        postFilter: queryFilter,
        reason: 'MV has all data, query filter will be applied post-read',
      }
    }

    // Check if MV filter is a subset of query filter (MV is more restrictive)
    // In this case, MV may not have all the data we need

    // Check for conflicts: if MV filters on a field with a different value
    for (const [field, mvCondition] of Object.entries(mvFilter)) {
      if (field.startsWith('$')) continue

      if (field in queryFilter) {
        const queryCondition = queryFilter[field]

        // Simple equality comparison
        if (this.filtersConflict(mvCondition, queryCondition)) {
          return {
            compatible: false,
            needsPostFilter: false,
            reason: `MV filter on "${field}" conflicts with query filter`,
          }
        }
      }
    }

    // Determine what post-filtering is needed
    const postFilterFields: Record<string, unknown> = {}
    let needsPostFilter = false

    for (const [field, condition] of Object.entries(queryFilter)) {
      if (field.startsWith('$')) {
        // Handle logical operators - need post-filter for these
        postFilterFields[field] = condition
        needsPostFilter = true
        continue
      }

      // If query has a field not in MV filter, need post-filter
      if (!(field in mvFilter)) {
        postFilterFields[field] = condition
        needsPostFilter = true
      } else {
        // Field is in MV filter - check if query condition is more complex
        const mvCondition = mvFilter[field]
        // If MV uses simple equality but query uses $in, need post-filter
        if (typeof mvCondition !== 'object' && typeof condition === 'object') {
          const qc = condition as Record<string, unknown>
          // If query uses $in that includes MV value, need post-filter to verify
          if ('$in' in qc && Array.isArray(qc.$in) && qc.$in.includes(mvCondition)) {
            postFilterFields[field] = condition
            needsPostFilter = true
          }
        }
      }
    }

    return {
      compatible: true,
      needsPostFilter,
      postFilter: needsPostFilter ? postFilterFields : undefined,
      reason: needsPostFilter
        ? 'MV filter is compatible, additional filtering needed'
        : 'MV filter matches query requirements',
    }
  }

  /**
   * Check if two filter conditions conflict
   */
  private filtersConflict(mvCondition: unknown, queryCondition: unknown): boolean {
    // Simple equality: different values = conflict
    if (typeof mvCondition !== 'object' && typeof queryCondition !== 'object') {
      return mvCondition !== queryCondition
    }

    // MV uses equality, query uses operators
    if (typeof mvCondition !== 'object' && typeof queryCondition === 'object') {
      const qc = queryCondition as Record<string, unknown>
      if ('$eq' in qc && qc.$eq !== mvCondition) return true
      if ('$ne' in qc && qc.$ne === mvCondition) return true
      if ('$in' in qc && Array.isArray(qc.$in) && !qc.$in.includes(mvCondition)) return true
      // $nin: if MV value is in the excluded list, conflict
      if ('$nin' in qc && Array.isArray(qc.$nin) && qc.$nin.includes(mvCondition)) return true
      // For range operators, MV value must be in range
      if ('$gt' in qc && typeof mvCondition === 'number' && mvCondition <= (qc.$gt as number)) return true
      if ('$gte' in qc && typeof mvCondition === 'number' && mvCondition < (qc.$gte as number)) return true
      if ('$lt' in qc && typeof mvCondition === 'number' && mvCondition >= (qc.$lt as number)) return true
      if ('$lte' in qc && typeof mvCondition === 'number' && mvCondition > (qc.$lte as number)) return true
    }

    // Both are objects - complex comparison (conservative: assume no conflict)
    return false
  }

  /**
   * Analyze if MV can satisfy the projection
   */
  private analyzeProjectionCompatibility(
    definition: MVDefinition,
    projection: unknown
  ): { compatible: boolean; reason: string } {
    // If MV has $select, check that all projected fields are available
    if (definition.$select) {
      const availableFields = new Set(Object.keys(definition.$select))
      // Always have core fields
      availableFields.add('$id')
      availableFields.add('$type')
      availableFields.add('name')

      const projectionObj = projection as Record<string, unknown>
      for (const field of Object.keys(projectionObj)) {
        if (projectionObj[field] === 1 || projectionObj[field] === true) {
          if (!availableFields.has(field)) {
            return {
              compatible: false,
              reason: `MV $select does not include field: ${field}`,
            }
          }
        }
      }
    }

    // If MV has $expand, the flattened fields are available
    // For simplicity, assume compatible (would need schema info for full check)
    return { compatible: true, reason: 'Projection can be satisfied' }
  }

  /**
   * Analyze if MV can satisfy the sort
   */
  private analyzeSortCompatibility(
    definition: MVDefinition,
    sort: unknown
  ): { compatible: boolean; reason: string } {
    // If MV has $select, check sort fields are included
    if (definition.$select) {
      const availableFields = new Set(Object.keys(definition.$select))
      // Always have core fields
      availableFields.add('$id')
      availableFields.add('createdAt')
      availableFields.add('updatedAt')

      const sortObj = sort as Record<string, unknown>
      for (const field of Object.keys(sortObj)) {
        if (!availableFields.has(field)) {
          return {
            compatible: false,
            reason: `MV does not have sort field: ${field}`,
          }
        }
      }
    }

    return { compatible: true, reason: 'Sort can be satisfied' }
  }

  /**
   * Extract field names from a filter
   */
  private _extractFilterFields(filter: Filter): Set<string> {
    const fields = new Set<string>()

    for (const key of Object.keys(filter)) {
      if (!key.startsWith('$')) {
        fields.add(key)
      } else if (key === '$and' || key === '$or') {
        const nested = filter[key] as Filter[]
        for (const f of nested) {
          const nestedFields = this._extractFilterFields(f)
          nestedFields.forEach(field => fields.add(field))
        }
      }
    }

    return fields
  }

  /**
   * Estimate cost savings from using an MV
   */
  private estimateCostSavings(
    mv: MVRoutingMetadata,
    _queryFilter: Filter,
    needsPostFilter: boolean
  ): number {
    let savings = 0.5 // Base savings from pre-materialized data

    // If MV has a filter and we don't need post-filtering, higher savings
    if (mv.definition.$filter && !needsPostFilter) {
      savings += 0.2
    }

    // If MV has expansions (denormalized), saves join costs
    if (mv.definition.$expand && mv.definition.$expand.length > 0) {
      savings += 0.15
    }

    // If MV is fresh, full savings; if stale, reduce
    if (mv.stalenessState === 'fresh') {
      // Full savings
    } else if (mv.stalenessState === 'stale') {
      savings -= 0.1 // Slight reduction for stale data
    }

    // If we need post-filtering, reduce savings
    if (needsPostFilter) {
      savings -= 0.15
    }

    return Math.max(0, Math.min(1, savings))
  }
}

/**
 * Create an MVRouter instance
 */
export function createMVRouter(metadataProvider: MVMetadataProvider): MVRouter {
  return new MVRouter(metadataProvider)
}

/**
 * In-memory MV metadata provider for testing
 */
export class InMemoryMVMetadataProvider implements MVMetadataProvider {
  private mvsBySource: Map<string, MVRoutingMetadata[]> = new Map()
  private mvsByName: Map<string, MVRoutingMetadata> = new Map()

  /**
   * Register an MV
   */
  registerMV(metadata: MVRoutingMetadata): void {
    const source = metadata.definition.$from
    if (!this.mvsBySource.has(source)) {
      this.mvsBySource.set(source, [])
    }
    this.mvsBySource.get(source)!.push(metadata)
    this.mvsByName.set(metadata.name, metadata)
  }

  /**
   * Clear all registered MVs
   */
  clear(): void {
    this.mvsBySource.clear()
    this.mvsByName.clear()
  }

  async getMVsForSource(source: string): Promise<MVRoutingMetadata[]> {
    return this.mvsBySource.get(source) ?? []
  }

  async getMVMetadata(name: string): Promise<MVRoutingMetadata | null> {
    return this.mvsByName.get(name) ?? null
  }
}

/**
 * Create an in-memory MV metadata provider for testing
 */
export function createInMemoryMVMetadataProvider(): InMemoryMVMetadataProvider {
  return new InMemoryMVMetadataProvider()
}
