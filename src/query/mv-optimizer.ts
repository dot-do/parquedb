/**
 * Materialized View Query Optimizer for ParqueDB
 *
 * Provides cost-based optimization for queries that could benefit from
 * materialized views. This module integrates with the main QueryOptimizer
 * to provide MV-aware query planning.
 *
 * Key capabilities:
 * - MV candidate detection: Identifies MVs that could satisfy a query
 * - Cost-based selection: Compares MV vs source table query costs
 * - Query rewriting: Transforms queries to use MVs when beneficial
 * - Staleness-aware decisions: Considers MV freshness in cost model
 *
 * @example
 * ```typescript
 * const mvOptimizer = new MVQueryOptimizer(mvRegistry, stalenessDetector)
 *
 * // Check if an MV can satisfy a query
 * const candidates = await mvOptimizer.findCandidateMVs('orders', filter)
 *
 * // Get optimized plan that may use MV
 * const plan = await mvOptimizer.optimizeWithMVs('orders', filter, options)
 * ```
 */

import type { Filter, FindOptions } from '../types'
import type { MVDefinition, MVMetadata, MVState } from '../materialized-views/types'
import type { StalenessMetrics, StalenessDetector, MVLineage } from '../materialized-views/staleness'
import type { QueryCost, TableStatistics } from './optimizer'
import { COST_CONSTANTS } from './optimizer'
import { extractFilterFields } from './predicate'
import { asViewName } from '../types/cast'

// =============================================================================
// Types
// =============================================================================

/**
 * A candidate materialized view for query optimization
 */
export interface MVCandidate {
  /** MV name */
  name: string

  /** MV definition */
  definition: MVDefinition

  /** MV metadata (if available) */
  metadata?: MVMetadata | undefined

  /** How well the MV matches the query (0-1, higher is better) */
  coverageScore: number

  /** Fields covered by the MV */
  coveredFields: string[]

  /** Fields not covered (require joining back to source) */
  uncoveredFields: string[]

  /** Whether the MV fully covers the query */
  isFullyCovered: boolean

  /** Staleness state of the MV */
  stalenessState?: MVState | undefined

  /** Staleness metrics (if available) */
  stalenessMetrics?: StalenessMetrics | undefined

  /** Estimated query cost using this MV */
  estimatedCost?: QueryCost | undefined

  /** Cost savings compared to source query (0-1) */
  costSavings?: number | undefined

  /** Whether the MV is recommended for this query */
  recommended: boolean

  /** Reason for recommendation (or not) */
  reason: string
}

/**
 * MV optimization result
 */
export interface MVOptimizationResult {
  /** Whether to use an MV */
  useMV: boolean

  /** Selected MV (if useMV is true) */
  selectedMV?: MVCandidate | undefined

  /** All candidate MVs considered */
  candidates: MVCandidate[]

  /** Rewritten filter for MV query (if useMV is true) */
  rewrittenFilter?: Filter | undefined

  /** Original filter */
  originalFilter: Filter

  /** Cost of querying the source table */
  sourceCost: QueryCost

  /** Cost of querying the selected MV (if applicable) */
  mvCost?: QueryCost | undefined

  /** Cost savings achieved (0-1) */
  costSavings: number

  /** Explanation of the optimization decision */
  explanation: string
}

/**
 * Configuration for MV-aware optimization
 */
export interface MVOptimizationConfig {
  /**
   * Minimum cost savings required to prefer MV over source (0-1)
   * @default 0.2 (20% savings)
   */
  minCostSavings: number

  /**
   * Whether to allow stale MV reads
   * @default true
   */
  allowStaleReads: boolean

  /**
   * Maximum staleness percentage allowed (0-100)
   * @default 50
   */
  maxStalenessPercent: number

  /**
   * Cost penalty factor for stale MVs (multiplier)
   * @default 1.5
   */
  stalenessCostPenalty: number

  /**
   * Minimum coverage score required to consider an MV (0-1)
   * @default 0.5
   */
  minCoverageScore: number

  /**
   * Whether to prefer aggregation MVs for aggregate queries
   * @default true
   */
  preferAggregationMVs: boolean

  /**
   * Maximum number of candidates to evaluate
   * @default 10
   */
  maxCandidates: number
}

/**
 * Default MV optimization configuration
 */
export const DEFAULT_MV_OPTIMIZATION_CONFIG: MVOptimizationConfig = {
  minCostSavings: 0.2,
  allowStaleReads: true,
  maxStalenessPercent: 50,
  stalenessCostPenalty: 1.5,
  minCoverageScore: 0.5,
  preferAggregationMVs: true,
  maxCandidates: 10,
}

/**
 * MV cost model constants
 */
export const MV_COST_CONSTANTS = {
  /** Base cost reduction for using pre-computed MV (vs computing on the fly) */
  MV_BASE_REDUCTION: 0.3,

  /** Additional reduction for aggregation MVs on aggregate queries */
  AGGREGATION_MV_REDUCTION: 0.5,

  /** Reduction for denormalized MVs (no joins needed) */
  DENORMALIZED_MV_REDUCTION: 0.2,

  /** Cost per uncovered field (requires join back to source) */
  UNCOVERED_FIELD_COST: 50,

  /** Cost penalty for stale data (per percentage of staleness) */
  STALENESS_COST_PER_PERCENT: 0.01,

  /** Fixed cost for MV metadata lookup */
  MV_LOOKUP_COST: 10,
} as const

// =============================================================================
// MV Registry Interface
// =============================================================================

/**
 * Interface for accessing materialized view definitions and metadata
 */
export interface MVRegistry {
  /**
   * Get all MV definitions
   */
  getAllMVs(): Map<string, MVDefinition>

  /**
   * Get MV definition by name
   */
  getMV(name: string): MVDefinition | undefined

  /**
   * Get MV metadata by name
   */
  getMVMetadata(name: string): Promise<MVMetadata | undefined>

  /**
   * Get MVs that have a specific source collection
   */
  getMVsBySource(source: string): Map<string, MVDefinition>

  /**
   * Get MV lineage for staleness detection
   */
  getMVLineage(name: string): Promise<MVLineage | undefined>
}

// =============================================================================
// MVQueryOptimizer Class
// =============================================================================

/**
 * Optimizes queries using materialized views when beneficial
 *
 * The optimizer performs the following steps:
 * 1. Find candidate MVs that could satisfy the query
 * 2. Calculate coverage score for each candidate
 * 3. Estimate query cost for each candidate vs source
 * 4. Consider staleness in cost model
 * 5. Select the best option (MV or source)
 * 6. Rewrite query if MV is selected
 */
export class MVQueryOptimizer {
  private registry: MVRegistry
  private stalenessDetector?: StalenessDetector | undefined
  private config: MVOptimizationConfig

  constructor(
    registry: MVRegistry,
    stalenessDetector?: StalenessDetector,
    config: Partial<MVOptimizationConfig> = {}
  ) {
    this.registry = registry
    this.stalenessDetector = stalenessDetector
    this.config = { ...DEFAULT_MV_OPTIMIZATION_CONFIG, ...config }
  }

  // ===========================================================================
  // Public API
  // ===========================================================================

  /**
   * Find candidate MVs that could satisfy a query
   */
  async findCandidateMVs(
    ns: string,
    filter: Filter,
    options: FindOptions<unknown> = {}
  ): Promise<MVCandidate[]> {
    // Get MVs that source from the target collection
    const mvsBySource = this.registry.getMVsBySource(ns)
    const candidates: MVCandidate[] = []

    for (const [name, definition] of Array.from(mvsBySource)) {
      const candidate = await this.evaluateCandidate(name, definition, filter, options)
      // Only include candidates that have sufficient coverage AND compatible filters
      const filterCompatible = this.isFilterCompatibleWithMV(filter, definition)
      if (candidate.coverageScore >= this.config.minCoverageScore && filterCompatible) {
        candidates.push(candidate)
      }
    }

    // Sort by coverage score (descending)
    candidates.sort((a, b) => b.coverageScore - a.coverageScore)

    // Limit candidates
    return candidates.slice(0, this.config.maxCandidates)
  }

  /**
   * Optimize a query, potentially using MVs
   */
  async optimize(
    ns: string,
    filter: Filter,
    options: FindOptions<unknown> = {},
    sourceStatistics?: TableStatistics
  ): Promise<MVOptimizationResult> {
    // Find candidate MVs
    const candidates = await this.findCandidateMVs(ns, filter, options)

    // Estimate source query cost
    const sourceCost = this.estimateSourceCost(filter, options, sourceStatistics)

    // If no candidates, return source-only result
    if (candidates.length === 0) {
      return {
        useMV: false,
        candidates: [],
        originalFilter: filter,
        sourceCost,
        costSavings: 0,
        explanation: 'No candidate materialized views found for this query',
      }
    }

    // Evaluate each candidate's cost
    for (const candidate of candidates) {
      const mvCost = await this.estimateMVCost(candidate, filter, options)
      candidate.estimatedCost = mvCost
      candidate.costSavings = Math.max(0, 1 - (mvCost.totalCost / sourceCost.totalCost))

      // Only update recommendation based on cost if the candidate was already viable
      // (don't overwrite staleness/filter rejections)
      if (candidate.recommended) {
        if (candidate.costSavings >= this.config.minCostSavings) {
          candidate.reason = `MV provides ${(candidate.costSavings * 100).toFixed(0)}% cost savings`
        } else {
          candidate.recommended = false
          candidate.reason = `Insufficient cost savings (${(candidate.costSavings * 100).toFixed(0)}% < ${(this.config.minCostSavings * 100).toFixed(0)}% threshold)`
        }
      }
    }

    // Select the best recommended candidate
    const recommendedCandidates = candidates.filter(c => c.recommended)
    const selectedMV = recommendedCandidates.length > 0
      ? recommendedCandidates.reduce((best, curr) =>
          (curr.costSavings ?? 0) > (best.costSavings ?? 0) ? curr : best
        )
      : undefined

    // Build result
    if (selectedMV) {
      const rewrittenFilter = this.rewriteFilterForMV(filter, selectedMV.definition)

      return {
        useMV: true,
        selectedMV,
        candidates,
        rewrittenFilter,
        originalFilter: filter,
        sourceCost,
        mvCost: selectedMV.estimatedCost,
        costSavings: selectedMV.costSavings ?? 0,
        explanation: `Using MV '${selectedMV.name}': ${selectedMV.reason}`,
      }
    }

    // No suitable MV found
    const bestCandidate = candidates[0]
    return {
      useMV: false,
      candidates,
      originalFilter: filter,
      sourceCost,
      costSavings: 0,
      explanation: bestCandidate
        ? `Best candidate MV '${bestCandidate.name}' rejected: ${bestCandidate.reason}`
        : 'No candidate MVs met the coverage threshold',
    }
  }

  /**
   * Update configuration
   */
  setConfig(config: Partial<MVOptimizationConfig>): void {
    this.config = { ...this.config, ...config }
  }

  /**
   * Get current configuration
   */
  getConfig(): MVOptimizationConfig {
    return { ...this.config }
  }

  // ===========================================================================
  // Internal Methods
  // ===========================================================================

  /**
   * Evaluate a single MV as a candidate for the query
   */
  private async evaluateCandidate(
    name: string,
    definition: MVDefinition,
    filter: Filter,
    options: FindOptions<unknown>
  ): Promise<MVCandidate> {
    // Extract fields needed by the query
    const filterFields = extractFilterFields(filter)
    const projectionFields = options.project
      ? Object.keys(options.project).filter(k => {
          const val = (options.project as Record<string, unknown>)[k]
          return val === 1 || val === true
        })
      : []
    const sortFields = options.sort ? Object.keys(options.sort) : []
    const allQueryFields = new Set([...filterFields, ...projectionFields, ...sortFields])

    // Get fields available in the MV
    const mvFields = this.getMVAvailableFields(definition)

    // Calculate coverage
    const coveredFields: string[] = []
    const uncoveredFields: string[] = []

    // Check if MV has wildcard (inherits all source fields)
    const hasWildcard = mvFields.has('*')

    for (const field of Array.from(allQueryFields)) {
      if (hasWildcard || mvFields.has(field) || this.isCoreField(field)) {
        coveredFields.push(field)
      } else {
        // Check for expanded field patterns (e.g., 'buyer_name' matches 'buyer_*')
        let isExpandedField = false
        for (const mvField of Array.from(mvFields)) {
          if (mvField.endsWith('_*')) {
            const prefix = mvField.slice(0, -1) // Remove the '*' to get 'buyer_'
            if (field.startsWith(prefix)) {
              isExpandedField = true
              break
            }
          }
        }
        if (isExpandedField) {
          coveredFields.push(field)
        } else {
          uncoveredFields.push(field)
        }
      }
    }

    const coverageScore = allQueryFields.size > 0
      ? coveredFields.length / allQueryFields.size
      : 1 // Empty query = full coverage

    const isFullyCovered = uncoveredFields.length === 0

    // Check filter compatibility
    const filterCompatible = this.isFilterCompatibleWithMV(filter, definition)

    // Get staleness info if detector is available
    let stalenessState: MVState | undefined
    let stalenessMetrics: StalenessMetrics | undefined

    if (this.stalenessDetector) {
      const lineage = await this.registry.getMVLineage(name)
      if (lineage) {
        stalenessMetrics = await this.stalenessDetector.getMetrics(
          asViewName(name),
          lineage,
          [definition.$from]
        )
        stalenessState = stalenessMetrics.state === 'fresh' ? 'fresh' : stalenessMetrics.state
      }
    }

    // Determine recommendation
    let recommended = isFullyCovered && filterCompatible
    let reason = ''

    if (!filterCompatible) {
      recommended = false
      reason = 'Filter is not compatible with MV definition'
    } else if (!isFullyCovered) {
      recommended = false
      reason = `Missing fields: ${uncoveredFields.join(', ')}`
    } else if (stalenessState === 'stale' && !this.config.allowStaleReads) {
      recommended = false
      reason = 'MV is stale and stale reads are disabled'
    } else if (stalenessMetrics && stalenessMetrics.stalenessPercent > this.config.maxStalenessPercent) {
      recommended = false
      reason = `MV staleness (${stalenessMetrics.stalenessPercent.toFixed(0)}%) exceeds threshold (${this.config.maxStalenessPercent}%)`
    } else {
      reason = 'MV is a viable candidate'
    }

    return {
      name,
      definition,
      coverageScore,
      coveredFields,
      uncoveredFields,
      isFullyCovered,
      stalenessState,
      stalenessMetrics,
      recommended,
      reason,
    }
  }

  /**
   * Get the set of fields available in an MV
   */
  private getMVAvailableFields(definition: MVDefinition): Set<string> {
    const fields = new Set<string>()

    // Add explicitly selected fields
    if (definition.$select) {
      for (const field of Object.keys(definition.$select)) {
        fields.add(field)
      }
    }

    // Add computed fields
    if (definition.$compute) {
      for (const field of Object.keys(definition.$compute)) {
        fields.add(field)
      }
    }

    // Add group by fields
    if (definition.$groupBy) {
      for (const spec of definition.$groupBy) {
        if (typeof spec === 'string') {
          fields.add(spec)
        } else {
          for (const alias of Object.keys(spec)) {
            fields.add(alias)
          }
        }
      }
    }

    // If no explicit projection, include source fields
    if (!definition.$select && !definition.$groupBy && !definition.$compute) {
      // MV inherits all source fields
      // For now, assume all fields are available
      // A more complete implementation would check the source schema
      fields.add('*') // Wildcard indicator
    }

    // Add expanded/flattened fields
    if (definition.$expand) {
      for (const relation of definition.$expand) {
        const alias = definition.$flatten?.[relation] ?? relation
        fields.add(`${alias}_*`) // Indicates expanded fields
      }
    }

    return fields
  }

  /**
   * Check if a field is a core entity field (always available)
   */
  private isCoreField(field: string): boolean {
    const coreFields = ['$id', '$type', 'name', 'createdAt', 'updatedAt', 'deletedAt', 'version']
    return coreFields.includes(field)
  }

  /**
   * Check if a filter is compatible with an MV's definition
   */
  private isFilterCompatibleWithMV(filter: Filter, definition: MVDefinition): boolean {
    // If MV has a filter, the query filter must be a subset
    if (definition.$filter) {
      // Simple check: if query has contradicting conditions, it's not compatible
      const mvFilter = definition.$filter

      for (const [field, queryValue] of Object.entries(filter)) {
        if (field.startsWith('$')) continue // Skip operators

        const mvFieldValue = mvFilter[field]
        if (mvFieldValue !== undefined) {
          // If both have equality conditions, they must match
          if (typeof queryValue !== 'object' && typeof mvFieldValue !== 'object') {
            if (queryValue !== mvFieldValue) {
              return false // Contradicting equality conditions
            }
          }
        }
      }
    }

    // If MV has $groupBy, only the grouped dimensions can be filtered
    if (definition.$groupBy) {
      const groupedFields = new Set<string>()
      for (const spec of definition.$groupBy) {
        if (typeof spec === 'string') {
          groupedFields.add(spec)
        } else {
          for (const alias of Object.keys(spec)) {
            groupedFields.add(alias)
          }
        }
      }

      // List of logical operators that should be skipped (not field names)
      const logicalOperators = new Set(['$and', '$or', '$not', '$nor', '$text', '$vector', '$geo'])

      for (const field of Object.keys(filter)) {
        // Skip logical operators, but NOT entity fields like $id, $type
        if (logicalOperators.has(field)) continue

        // For aggregation MVs, individual entity fields like $id cannot be queried
        // because individual records have been aggregated away
        if (field === '$id' || field === '$type') {
          return false
        }

        if (!groupedFields.has(field)) {
          // Filtering on a non-grouped field is not possible
          return false
        }
      }
    }

    return true
  }

  /**
   * Estimate the cost of querying the source table
   */
  private estimateSourceCost(
    filter: Filter,
    options: FindOptions<unknown>,
    statistics?: TableStatistics
  ): QueryCost {
    const totalRows = statistics?.totalRows ?? 10000
    const rowGroupCount = statistics?.rowGroupCount ?? 10
    const filterFields = extractFilterFields(filter)

    // Base I/O cost
    let ioCost = rowGroupCount * COST_CONSTANTS.ROW_GROUP_SCAN
    ioCost += filterFields.length * COST_CONSTANTS.COLUMN_READ * rowGroupCount

    // Base CPU cost
    let cpuCost = totalRows * COST_CONSTANTS.ROW_READ
    cpuCost += totalRows * COST_CONSTANTS.ROW_FILTER * Math.max(filterFields.length, 1)

    // Add sort cost
    if (options.sort) {
      const sortFields = Object.keys(options.sort).length
      cpuCost += totalRows * COST_CONSTANTS.SORT_PER_ROW * Math.log2(Math.max(totalRows, 2)) * sortFields
    }

    // Estimate rows returned
    let estimatedRowsReturned = totalRows
    if (filterFields.length > 0) {
      // Heuristic: each filter reduces rows by 50%
      estimatedRowsReturned = Math.max(1, totalRows * Math.pow(0.5, filterFields.length))
    }
    if (options.limit) {
      estimatedRowsReturned = Math.min(estimatedRowsReturned, options.limit)
    }

    return {
      ioCost,
      cpuCost,
      totalCost: ioCost + cpuCost,
      estimatedRowsScanned: totalRows,
      estimatedRowsReturned,
      isExact: false,
    }
  }

  /**
   * Estimate the cost of querying an MV
   */
  private async estimateMVCost(
    candidate: MVCandidate,
    filter: Filter,
    options: FindOptions<unknown>
  ): Promise<QueryCost> {
    const definition = candidate.definition

    // Get MV metadata for row count
    const metadata = await this.registry.getMVMetadata(candidate.name)
    const mvRowCount = metadata?.rowCount ?? 1000 // Default to smaller size

    // Base cost reduction for using MV
    let costReduction = MV_COST_CONSTANTS.MV_BASE_REDUCTION

    // Additional reduction for aggregation MVs
    if (definition.$groupBy || definition.$compute) {
      costReduction += MV_COST_CONSTANTS.AGGREGATION_MV_REDUCTION
    }

    // Additional reduction for denormalized MVs
    if (definition.$expand) {
      costReduction += MV_COST_CONSTANTS.DENORMALIZED_MV_REDUCTION
    }

    // Calculate base costs (smaller than source due to pre-computation)
    const rowGroupCount = Math.ceil(mvRowCount / 1000) // Assume ~1000 rows per row group
    const filterFields = extractFilterFields(filter)

    let ioCost = rowGroupCount * COST_CONSTANTS.ROW_GROUP_SCAN
    ioCost += filterFields.length * COST_CONSTANTS.COLUMN_READ * rowGroupCount
    ioCost += MV_COST_CONSTANTS.MV_LOOKUP_COST

    let cpuCost = mvRowCount * COST_CONSTANTS.ROW_READ
    cpuCost += mvRowCount * COST_CONSTANTS.ROW_FILTER * Math.max(filterFields.length, 1)

    // Add cost for uncovered fields
    if (candidate.uncoveredFields.length > 0) {
      ioCost += candidate.uncoveredFields.length * MV_COST_CONSTANTS.UNCOVERED_FIELD_COST
    }

    // Add staleness penalty
    if (candidate.stalenessMetrics && candidate.stalenessMetrics.state === 'stale') {
      const stalenessPenalty = 1 + (
        candidate.stalenessMetrics.stalenessPercent * MV_COST_CONSTANTS.STALENESS_COST_PER_PERCENT
      )
      ioCost *= stalenessPenalty
      cpuCost *= stalenessPenalty
    }

    // Apply cost reduction
    ioCost *= (1 - costReduction)
    cpuCost *= (1 - costReduction)

    // Add sort cost if needed
    if (options.sort) {
      const sortFields = Object.keys(options.sort).length
      cpuCost += mvRowCount * COST_CONSTANTS.SORT_PER_ROW * Math.log2(Math.max(mvRowCount, 2)) * sortFields
    }

    // Estimate rows returned
    let estimatedRowsReturned = mvRowCount
    if (filterFields.length > 0) {
      estimatedRowsReturned = Math.max(1, mvRowCount * Math.pow(0.5, filterFields.length))
    }
    if (options.limit) {
      estimatedRowsReturned = Math.min(estimatedRowsReturned, options.limit)
    }

    return {
      ioCost,
      cpuCost,
      totalCost: ioCost + cpuCost,
      estimatedRowsScanned: mvRowCount,
      estimatedRowsReturned,
      isExact: false,
    }
  }

  /**
   * Rewrite a filter to work with an MV
   */
  private rewriteFilterForMV(filter: Filter, definition: MVDefinition): Filter {
    const rewritten: Filter = {}

    for (const [key, value] of Object.entries(filter)) {
      // Handle field renaming from $flatten
      if (definition.$flatten) {
        let rewrittenKey = key
        for (const [from, to] of Object.entries(definition.$flatten)) {
          if (key.startsWith(`${from}.`) || key.startsWith(`${from}_`)) {
            rewrittenKey = key.replace(from, to)
            break
          }
        }
        rewritten[rewrittenKey] = value
      } else {
        rewritten[key] = value
      }
    }

    // If MV has a $filter, we can skip conditions that are already applied
    if (definition.$filter) {
      for (const [field, mvValue] of Object.entries(definition.$filter)) {
        if (field.startsWith('$')) continue

        const queryValue = rewritten[field]
        if (queryValue !== undefined) {
          // If the query has the same equality condition, we can remove it
          if (typeof queryValue !== 'object' && typeof mvValue !== 'object' && queryValue === mvValue) {
            delete rewritten[field]
          }
        }
      }
    }

    return rewritten
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create an MVQueryOptimizer instance
 */
export function createMVQueryOptimizer(
  registry: MVRegistry,
  stalenessDetector?: StalenessDetector,
  config?: Partial<MVOptimizationConfig>
): MVQueryOptimizer {
  return new MVQueryOptimizer(registry, stalenessDetector, config)
}

// =============================================================================
// In-Memory Registry (for testing)
// =============================================================================

/**
 * Simple in-memory MV registry for testing
 */
export class InMemoryMVRegistry implements MVRegistry {
  private mvs: Map<string, MVDefinition> = new Map()
  private metadata: Map<string, MVMetadata> = new Map()
  private lineages: Map<string, MVLineage> = new Map()

  /**
   * Register an MV
   */
  register(name: string, definition: MVDefinition, metadata?: MVMetadata): void {
    this.mvs.set(name, definition)
    if (metadata) {
      this.metadata.set(name, metadata)
    }
  }

  /**
   * Set lineage for an MV
   */
  setLineage(name: string, lineage: MVLineage): void {
    this.lineages.set(name, lineage)
  }

  /**
   * Clear all registrations
   */
  clear(): void {
    this.mvs.clear()
    this.metadata.clear()
    this.lineages.clear()
  }

  // MVRegistry implementation

  getAllMVs(): Map<string, MVDefinition> {
    return new Map(this.mvs)
  }

  getMV(name: string): MVDefinition | undefined {
    return this.mvs.get(name)
  }

  async getMVMetadata(name: string): Promise<MVMetadata | undefined> {
    return this.metadata.get(name)
  }

  getMVsBySource(source: string): Map<string, MVDefinition> {
    const result = new Map<string, MVDefinition>()
    for (const [name, def] of Array.from(this.mvs)) {
      if (def.$from === source) {
        result.set(name, def)
      }
    }
    return result
  }

  async getMVLineage(name: string): Promise<MVLineage | undefined> {
    return this.lineages.get(name)
  }
}

/**
 * Create an InMemoryMVRegistry instance
 */
export function createInMemoryMVRegistry(): InMemoryMVRegistry {
  return new InMemoryMVRegistry()
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Check if a query would benefit from using an MV
 *
 * Quick heuristic check without full cost analysis
 */
export function wouldBenefitFromMV(
  filter: Filter,
  mvDefinition: MVDefinition,
  queryOptions?: { hasAggregation?: boolean | undefined; hasJoins?: boolean | undefined }
): boolean {
  // Aggregation queries benefit from aggregation MVs
  if (queryOptions?.hasAggregation && (mvDefinition.$groupBy || mvDefinition.$compute)) {
    return true
  }

  // Queries with joins benefit from denormalized MVs
  if (queryOptions?.hasJoins && mvDefinition.$expand) {
    return true
  }

  // Queries with filters benefit from filtered MVs
  if (mvDefinition.$filter) {
    const mvFilterKeys = Object.keys(mvDefinition.$filter).filter(k => !k.startsWith('$'))
    const queryFilterKeys = Object.keys(filter).filter(k => !k.startsWith('$'))

    // If MV filter is a superset of query filter, beneficial
    if (queryFilterKeys.every(k => mvFilterKeys.includes(k))) {
      return true
    }
  }

  return false
}

/**
 * Explain why an MV was or wasn't selected
 */
export function explainMVSelection(result: MVOptimizationResult): string {
  const lines: string[] = [
    '=== MV Optimization Report ===',
    '',
    `Decision: ${result.useMV ? `Use MV '${result.selectedMV?.name}'` : 'Use source table'}`,
    `Cost Savings: ${(result.costSavings * 100).toFixed(1)}%`,
    '',
    '--- Source Table Cost ---',
    `I/O Cost: ${result.sourceCost.ioCost.toFixed(2)}`,
    `CPU Cost: ${result.sourceCost.cpuCost.toFixed(2)}`,
    `Total: ${result.sourceCost.totalCost.toFixed(2)}`,
    `Est. Rows: ${result.sourceCost.estimatedRowsReturned}`,
  ]

  if (result.mvCost) {
    lines.push(
      '',
      '--- Selected MV Cost ---',
      `MV: ${result.selectedMV?.name}`,
      `I/O Cost: ${result.mvCost.ioCost.toFixed(2)}`,
      `CPU Cost: ${result.mvCost.cpuCost.toFixed(2)}`,
      `Total: ${result.mvCost.totalCost.toFixed(2)}`,
      `Est. Rows: ${result.mvCost.estimatedRowsReturned}`,
    )
  }

  if (result.candidates.length > 0) {
    lines.push(
      '',
      '--- Candidates Evaluated ---',
    )
    for (const candidate of result.candidates) {
      lines.push(
        `${candidate.name}: coverage=${(candidate.coverageScore * 100).toFixed(0)}%, ` +
        `savings=${((candidate.costSavings ?? 0) * 100).toFixed(0)}%, ` +
        `recommended=${candidate.recommended}, ` +
        `reason="${candidate.reason}"`
      )
    }
  }

  lines.push(
    '',
    '--- Explanation ---',
    result.explanation,
  )

  return lines.join('\n')
}
