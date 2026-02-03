/**
 * Aggregation Pipeline Executor for ParqueDB
 *
 * Executes MongoDB-style aggregation pipelines on in-memory data.
 * Supports all standard pipeline stages and accumulator operators.
 */

import type { Filter } from '../types/filter'
import type {
  AggregationStage,
  AggregationOptions,
  GroupSpec,
  UnwindOptions,
  LookupOptions,
  Document,
  SumAccumulator,
} from './types'
import {
  isMatchStage,
  isGroupStage,
  isSortStage,
  isLimitStage,
  isSkipStage,
  isProjectStage,
  isUnwindStage,
  isLookupStage,
  isCountStage,
  isAddFieldsStage,
  isSetStage,
  isUnsetStage,
  isReplaceRootStage,
  isFacetStage,
  isBucketStage,
  isSampleStage,
  isSumAccumulator,
  isAvgAccumulator,
  isMinAccumulator,
  isMaxAccumulator,
  isFirstAccumulator,
  isLastAccumulator,
  isPushAccumulator,
  isAddToSetAccumulator,
  isCountAccumulator,
  isFieldRef,
} from './types'
import { matchesFilter } from '../query/filter'
import { getNestedValue, compareValues } from '../utils'
import type { IndexManager, SelectedIndex } from '../indexes/manager'
import type { FTSSearchResult } from '../indexes/types'

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Set value at a nested path using dot notation
 */
function setNestedValue(obj: Document, path: string, value: unknown): void {
  const parts = path.split('.')
  let current: Document = obj
  for (let i = 0; i < parts.length - 1; i++) {
    const part = parts[i]!
    if (!(part in current) || typeof current[part] !== 'object' || current[part] === null) {
      current[part] = {}
    }
    current = current[part] as Document
  }
  const lastPart = parts[parts.length - 1]
  if (lastPart !== undefined) {
    current[lastPart] = value
  }
}

/**
 * Compare two values for sorting with direction
 */
function compareValuesWithDirection(a: unknown, b: unknown, direction: 1 | -1): number {
  return direction * compareValues(a, b)
}

/**
 * Type for $cond object expression
 */
interface CondExpression {
  if: unknown
  then: unknown
  else: unknown
}

/**
 * Type guard for $cond object expression
 */
function isCondObject(value: unknown): value is CondExpression {
  return (
    typeof value === 'object' &&
    value !== null &&
    'if' in value &&
    'then' in value &&
    'else' in value
  )
}

/**
 * Evaluate a field reference (e.g., '$fieldName')
 */
function evaluateFieldRef(doc: Document, ref: unknown): unknown {
  if (isFieldRef(ref)) {
    return getNestedValue(doc, ref.slice(1))
  }
  return ref
}

// =============================================================================
// Stage Executors
// =============================================================================

/**
 * Execute $match stage
 */
function executeMatch(data: Document[], filter: Filter): Document[] {
  return data.filter(doc => matchesFilter(doc, filter))
}

/**
 * Execute $group stage
 */
function executeGroup(data: Document[], groupSpec: GroupSpec): Document[] {
  const groups = new Map<string, Document[]>()

  // Group documents by _id
  for (const doc of data) {
    let groupKey: unknown

    if (groupSpec._id === null) {
      groupKey = null
    } else if (isFieldRef(groupSpec._id)) {
      groupKey = getNestedValue(doc, groupSpec._id.slice(1))
    } else if (typeof groupSpec._id === 'object' && groupSpec._id !== null) {
      // Compound _id
      const compoundKey: Document = {}
      for (const [key, value] of Object.entries(groupSpec._id as Document)) {
        compoundKey[key] = evaluateFieldRef(doc, value)
      }
      groupKey = compoundKey
    } else {
      groupKey = groupSpec._id
    }

    const keyStr = JSON.stringify(groupKey)
    if (!groups.has(keyStr)) {
      groups.set(keyStr, [])
    }
    groups.get(keyStr)!.push(doc)
  }

  // Apply accumulators to each group
  return Array.from(groups.entries()).map(([keyStr, items]) => {
    const result: Document = { _id: JSON.parse(keyStr) as unknown }

    for (const [field, spec] of Object.entries(groupSpec)) {
      if (field === '_id') continue

      // $sum accumulator
      if (isSumAccumulator(spec)) {
        result[field] = executeSumAccumulator(items, spec)
      }
      // $avg accumulator
      else if (isAvgAccumulator(spec) && isFieldRef(spec.$avg)) {
        const fieldPath = spec.$avg.slice(1)
        const sum = items.reduce((s: number, item) => {
          const val = getNestedValue(item, fieldPath)
          return s + (typeof val === 'number' ? val : 0)
        }, 0)
        result[field] = items.length > 0 ? sum / items.length : 0
      }
      // $max accumulator
      else if (isMaxAccumulator(spec) && isFieldRef(spec.$max)) {
        const fieldPath = spec.$max.slice(1)
        result[field] = items.reduce((max: number | null, item) => {
          const val = getNestedValue(item, fieldPath)
          return typeof val === 'number' && (max === null || val > max) ? val : max
        }, null as number | null)
      }
      // $min accumulator
      else if (isMinAccumulator(spec) && isFieldRef(spec.$min)) {
        const fieldPath = spec.$min.slice(1)
        result[field] = items.reduce((min: number | null, item) => {
          const val = getNestedValue(item, fieldPath)
          return typeof val === 'number' && (min === null || val < min) ? val : min
        }, null as number | null)
      }
      // $first accumulator
      else if (isFirstAccumulator(spec) && isFieldRef(spec.$first)) {
        const fieldPath = spec.$first.slice(1)
        const firstItem = items[0]
        if (firstItem) {
          result[field] = getNestedValue(firstItem, fieldPath)
        }
      }
      // $last accumulator
      else if (isLastAccumulator(spec) && isFieldRef(spec.$last)) {
        const fieldPath = spec.$last.slice(1)
        const lastItem = items[items.length - 1]
        if (lastItem) {
          result[field] = getNestedValue(lastItem, fieldPath)
        }
      }
      // $push accumulator
      else if (isPushAccumulator(spec) && isFieldRef(spec.$push)) {
        const fieldPath = spec.$push.slice(1)
        result[field] = items.map(item => getNestedValue(item, fieldPath))
      }
      // $addToSet accumulator
      else if (isAddToSetAccumulator(spec) && isFieldRef(spec.$addToSet)) {
        const fieldPath = spec.$addToSet.slice(1)
        const values = items.map(item => getNestedValue(item, fieldPath))
        result[field] = [...new Set(values.map(v => JSON.stringify(v)))].map(s => JSON.parse(s) as unknown)
      }
      // $count accumulator (count non-null values)
      else if (isCountAccumulator(spec)) {
        result[field] = items.length
      }
    }

    return result
  })
}

/**
 * Execute $sum accumulator
 */
function executeSumAccumulator(items: Document[], spec: SumAccumulator): number {
  const sumValue = spec.$sum

  if (sumValue === 1) {
    return items.length
  } else if (typeof sumValue === 'number') {
    return items.length * sumValue
  } else if (isFieldRef(sumValue)) {
    const fieldPath = sumValue.slice(1)
    return items.reduce((sum: number, item) => {
      const val = getNestedValue(item, fieldPath)
      return sum + (typeof val === 'number' ? val : 0)
    }, 0)
  }

  return 0
}

/**
 * Execute $sort stage
 */
function executeSort(data: Document[], sortSpec: Record<string, 1 | -1>): Document[] {
  const sortEntries = Object.entries(sortSpec)

  return [...data].sort((a, b) => {
    for (const [field, direction] of sortEntries) {
      const aValue = getNestedValue(a, field)
      const bValue = getNestedValue(b, field)
      const cmp = compareValuesWithDirection(aValue, bValue, direction)
      if (cmp !== 0) return cmp
    }
    return 0
  })
}

/**
 * Execute $limit stage
 */
function executeLimit(data: Document[], limit: number): Document[] {
  return data.slice(0, limit)
}

/**
 * Execute $skip stage
 */
function executeSkip(data: Document[], skip: number): Document[] {
  return data.slice(skip)
}

/**
 * Execute $project stage
 */
function executeProject(data: Document[], projection: Record<string, unknown>): Document[] {
  return data.map(doc => {
    const result: Document = {}
    const isInclusion = Object.values(projection).some(v => v === 1 || v === true)

    if (isInclusion) {
      // Inclusion mode - only include specified fields
      for (const [key, value] of Object.entries(projection)) {
        if (value === 1 || value === true) {
          result[key] = doc[key]
        } else if (typeof value === 'object' && value !== null) {
          // Expression - evaluate it
          result[key] = evaluateProjectionExpression(doc, value)
        }
      }
    } else {
      // Exclusion mode - copy all fields except excluded ones
      for (const [key, value] of Object.entries(doc)) {
        if (!(key in projection) || projection[key] !== 0) {
          result[key] = value
        }
      }
    }

    return result
  })
}

/**
 * Evaluate a projection expression
 */
function evaluateProjectionExpression(doc: Document, expr: unknown): unknown {
  if (isFieldRef(expr)) {
    return getNestedValue(doc, expr.slice(1))
  }

  if (typeof expr === 'object' && expr !== null) {
    const exprObj = expr as Document

    // String operators
    if ('$strLenCP' in exprObj) {
      const val = evaluateFieldRef(doc, exprObj.$strLenCP)
      return typeof val === 'string' ? val.length : 0
    }

    if ('$concat' in exprObj && Array.isArray(exprObj.$concat)) {
      return exprObj.$concat
        .map(part => evaluateFieldRef(doc, part))
        .join('')
    }

    if ('$toUpper' in exprObj) {
      const val = evaluateFieldRef(doc, exprObj.$toUpper)
      return typeof val === 'string' ? val.toUpperCase() : val
    }

    if ('$toLower' in exprObj) {
      const val = evaluateFieldRef(doc, exprObj.$toLower)
      return typeof val === 'string' ? val.toLowerCase() : val
    }

    // Comparison operators
    if ('$gt' in exprObj && Array.isArray(exprObj.$gt)) {
      const [a, b] = exprObj.$gt.map(v => evaluateFieldRef(doc, v))
      return compareValues(a, b) > 0
    }

    if ('$gte' in exprObj && Array.isArray(exprObj.$gte)) {
      const [a, b] = exprObj.$gte.map(v => evaluateFieldRef(doc, v))
      return compareValues(a, b) >= 0
    }

    if ('$lt' in exprObj && Array.isArray(exprObj.$lt)) {
      const [a, b] = exprObj.$lt.map(v => evaluateFieldRef(doc, v))
      return compareValues(a, b) < 0
    }

    if ('$lte' in exprObj && Array.isArray(exprObj.$lte)) {
      const [a, b] = exprObj.$lte.map(v => evaluateFieldRef(doc, v))
      return compareValues(a, b) <= 0
    }

    if ('$eq' in exprObj && Array.isArray(exprObj.$eq)) {
      const [a, b] = exprObj.$eq.map(v => evaluateFieldRef(doc, v))
      return a === b
    }

    if ('$ne' in exprObj && Array.isArray(exprObj.$ne)) {
      const [a, b] = exprObj.$ne.map(v => evaluateFieldRef(doc, v))
      return a !== b
    }

    // Arithmetic operators
    if ('$add' in exprObj && Array.isArray(exprObj.$add)) {
      return exprObj.$add.reduce((sum: number, v) => {
        const val = evaluateFieldRef(doc, v)
        return sum + (typeof val === 'number' ? val : 0)
      }, 0)
    }

    if ('$subtract' in exprObj && Array.isArray(exprObj.$subtract)) {
      const [a, b] = exprObj.$subtract.map(v => {
        const val = evaluateFieldRef(doc, v)
        return typeof val === 'number' ? val : 0
      })
      return (a ?? 0) - (b ?? 0)
    }

    if ('$multiply' in exprObj && Array.isArray(exprObj.$multiply)) {
      return exprObj.$multiply.reduce((product: number, v) => {
        const val = evaluateFieldRef(doc, v)
        return product * (typeof val === 'number' ? val : 0)
      }, 1)
    }

    if ('$divide' in exprObj && Array.isArray(exprObj.$divide)) {
      const [a, b] = exprObj.$divide.map(v => {
        const val = evaluateFieldRef(doc, v)
        return typeof val === 'number' ? val : 0
      })
      return b !== undefined && b !== 0 ? (a ?? 0) / b : null
    }

    // Conditional operators
    if ('$cond' in exprObj) {
      const cond = exprObj.$cond
      if (Array.isArray(cond)) {
        const [condition, thenVal, elseVal] = cond as [unknown, unknown, unknown]
        const evaluated = evaluateProjectionExpression(doc, condition)
        return evaluated ? evaluateFieldRef(doc, thenVal) : evaluateFieldRef(doc, elseVal)
      } else if (isCondObject(cond)) {
        const evaluated = evaluateProjectionExpression(doc, cond.if)
        return evaluated ? evaluateFieldRef(doc, cond.then) : evaluateFieldRef(doc, cond.else)
      }
    }

    if ('$ifNull' in exprObj && Array.isArray(exprObj.$ifNull)) {
      const [value, replacement] = exprObj.$ifNull
      const evaluated = evaluateFieldRef(doc, value)
      return evaluated ?? evaluateFieldRef(doc, replacement)
    }
  }

  return expr
}

/**
 * Execute $unwind stage
 */
function executeUnwind(data: Document[], unwind: string | UnwindOptions): Document[] {
  const path = typeof unwind === 'string' ? unwind : unwind.path
  const preserveNull = typeof unwind === 'object' ? unwind.preserveNullAndEmptyArrays : false
  const includeArrayIndex = typeof unwind === 'object' ? unwind.includeArrayIndex : undefined
  const fieldPath = path.startsWith('$') ? path.slice(1) : path

  const newData: Document[] = []

  for (const doc of data) {
    const arr = getNestedValue(doc, fieldPath)

    if (Array.isArray(arr) && arr.length > 0) {
      for (let i = 0; i < arr.length; i++) {
        const newItem: Document = { ...doc }
        setNestedValue(newItem, fieldPath, arr[i])
        if (includeArrayIndex) {
          newItem[includeArrayIndex] = i
        }
        newData.push(newItem)
      }
    } else if (preserveNull) {
      const newItem: Document = { ...doc }
      if (includeArrayIndex) {
        newItem[includeArrayIndex] = null
      }
      newData.push(newItem)
    }
  }

  return newData
}

/**
 * Execute $lookup stage
 *
 * Note: This is a simplified implementation that requires the foreign collection
 * to be provided via the resolver option. Full implementation would need access
 * to the database instance.
 */
function executeLookup(
  data: Document[],
  lookup: LookupOptions,
  _resolver?: (collection: string) => Document[]
): Document[] {
  // Simplified implementation - just adds empty array
  // Full implementation would require database access
  return data.map(doc => ({
    ...doc,
    [lookup.as]: [],
  }))
}

/**
 * Execute $count stage
 */
function executeCount(data: Document[], fieldName: string): Document[] {
  return [{ [fieldName]: data.length }]
}

/**
 * Execute $addFields or $set stage
 */
function executeAddFields(data: Document[], fields: Record<string, unknown>): Document[] {
  return data.map(doc => {
    const result: Document = { ...doc }

    for (const [key, value] of Object.entries(fields)) {
      if (typeof value === 'object' && value !== null) {
        result[key] = evaluateProjectionExpression(doc, value)
      } else if (isFieldRef(value)) {
        result[key] = getNestedValue(doc, value.slice(1))
      } else {
        result[key] = value
      }
    }

    return result
  })
}

/**
 * Execute $unset stage
 */
function executeUnset(data: Document[], fields: string | string[]): Document[] {
  const fieldsToRemove = Array.isArray(fields) ? fields : [fields]

  return data.map(doc => {
    const result: Document = { ...doc }

    for (const field of fieldsToRemove) {
      delete result[field]
    }

    return result
  })
}

/**
 * Execute $replaceRoot stage
 */
function executeReplaceRoot(data: Document[], newRoot: unknown): Document[] {
  return data.map(doc => {
    const root = evaluateFieldRef(doc, newRoot)
    return typeof root === 'object' && root !== null ? (root as Document) : doc
  })
}

/**
 * Execute $sample stage
 */
function executeSample(data: Document[], size: number): Document[] {
  // Fisher-Yates shuffle and take first `size` elements
  const shuffled = [...data]
  for (let i = shuffled.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1))
    ;[shuffled[i], shuffled[j]] = [shuffled[j]!, shuffled[i]!]
  }
  return shuffled.slice(0, size)
}

/**
 * Bucket stage configuration
 */
interface BucketConfig {
  groupBy: string
  boundaries: unknown[]
  default?: string | undefined
  output?: Record<string, unknown> | undefined
}

/**
 * Execute $bucket stage
 */
function executeBucket(data: Document[], bucket: BucketConfig): Document[] {
  const { groupBy, boundaries, default: defaultBucket, output } = bucket
  const groups = new Map<string, Document[]>()

  // Initialize buckets
  for (let i = 0; i < boundaries.length - 1; i++) {
    groups.set(String(boundaries[i]), [])
  }
  if (defaultBucket) {
    groups.set(defaultBucket, [])
  }

  // Assign documents to buckets
  for (const doc of data) {
    const value = evaluateFieldRef(doc, groupBy)
    let assigned = false

    for (let i = 0; i < boundaries.length - 1; i++) {
      if (compareValues(value, boundaries[i]) >= 0 && compareValues(value, boundaries[i + 1]) < 0) {
        groups.get(String(boundaries[i]))!.push(doc)
        assigned = true
        break
      }
    }

    if (!assigned && defaultBucket) {
      groups.get(defaultBucket)!.push(doc)
    }
  }

  // Build results
  return Array.from(groups.entries())
    .filter(([, items]) => items.length > 0)
    .map(([key, items]) => {
      const result: Document = { _id: key === defaultBucket ? defaultBucket : Number(key) }

      if (output) {
        for (const [field, spec] of Object.entries(output)) {
          if (isSumAccumulator(spec)) {
            result[field] = executeSumAccumulator(items, spec)
          }
        }
      } else {
        result.count = items.length
      }

      return result
    })
}

// =============================================================================
// Main Executor
// =============================================================================

/**
 * Execute an aggregation pipeline on a dataset
 *
 * @param data - Initial dataset (items will be treated as Documents)
 * @param pipeline - Array of aggregation stages
 * @param options - Aggregation options
 * @returns Aggregation results
 */
export function executeAggregation<T = Document>(
  data: unknown[],
  pipeline: AggregationStage[],
  options?: AggregationOptions
): T[] {
  // Cast input data to Document[] - aggregation operates on object-like structures
  let result: Document[] = data as Document[]

  // Process each stage
  for (const stage of pipeline) {
    if (isMatchStage(stage)) {
      result = executeMatch(result, stage.$match)
    } else if (isGroupStage(stage)) {
      result = executeGroup(result, stage.$group)
    } else if (isSortStage(stage)) {
      result = executeSort(result, stage.$sort)
    } else if (isLimitStage(stage)) {
      result = executeLimit(result, stage.$limit)
    } else if (isSkipStage(stage)) {
      result = executeSkip(result, stage.$skip)
    } else if (isProjectStage(stage)) {
      result = executeProject(result, stage.$project)
    } else if (isUnwindStage(stage)) {
      result = executeUnwind(result, stage.$unwind)
    } else if (isLookupStage(stage)) {
      result = executeLookup(result, stage.$lookup)
    } else if (isCountStage(stage)) {
      result = executeCount(result, stage.$count)
    } else if (isAddFieldsStage(stage)) {
      result = executeAddFields(result, stage.$addFields)
    } else if (isSetStage(stage)) {
      result = executeAddFields(result, stage.$set)
    } else if (isUnsetStage(stage)) {
      result = executeUnset(result, stage.$unset)
    } else if (isReplaceRootStage(stage)) {
      result = executeReplaceRoot(result, stage.$replaceRoot.newRoot)
    } else if (isFacetStage(stage)) {
      // Execute each facet pipeline and combine results
      const facetResults: Document = {}
      for (const [name, facetPipeline] of Object.entries(stage.$facet)) {
        facetResults[name] = executeAggregation(result, facetPipeline, options)
      }
      result = [facetResults]
    } else if (isBucketStage(stage)) {
      result = executeBucket(result, stage.$bucket)
    } else if (isSampleStage(stage)) {
      result = executeSample(result, stage.$sample.size)
    }
  }

  return result as T[]
}

/**
 * Explain stage result
 */
interface ExplainStage {
  name: string
  inputCount: number
  outputCount: number
}

/**
 * Explain result
 */
interface ExplainResult {
  stages: ExplainStage[]
  totalDocuments: number
}

/**
 * AggregationExecutor class for stateful pipeline execution
 *
 * Provides a class-based interface for executing aggregation pipelines
 * with additional features like explain mode and cursor support.
 */
export class AggregationExecutor {
  private data: unknown[]
  private pipeline: AggregationStage[]
  private options: AggregationOptions

  constructor(data: unknown[], pipeline: AggregationStage[], options: AggregationOptions = {}) {
    this.data = data
    this.pipeline = pipeline
    this.options = options
  }

  /**
   * Execute the pipeline and return results
   */
  execute<T = Document>(): T[] {
    return executeAggregation<T>(this.data, this.pipeline, this.options)
  }

  /**
   * Execute the pipeline with explain mode
   */
  explain(): ExplainResult {
    const stages: ExplainStage[] = []
    let currentData = this.data as Document[]

    for (const stage of this.pipeline) {
      const inputCount = currentData.length
      const stageName = Object.keys(stage)[0]!

      // Execute stage
      currentData = executeAggregation(currentData, [stage], this.options)

      stages.push({
        name: stageName,
        inputCount,
        outputCount: currentData.length,
      })
    }

    return {
      stages,
      totalDocuments: currentData.length,
    }
  }

  /**
   * Add a stage to the pipeline
   */
  addStage(stage: AggregationStage): this {
    this.pipeline.push(stage)
    return this
  }

  /**
   * Get the current pipeline
   */
  getPipeline(): AggregationStage[] {
    return [...this.pipeline]
  }
}

// =============================================================================
// Index-Aware Aggregation
// =============================================================================

/**
 * Execute an aggregation pipeline with index support for $match stages.
 *
 * When the first stage is a $match and an indexManager is provided,
 * this function will attempt to use secondary indexes (hash, sst, fts, vector)
 * to efficiently filter the initial dataset before executing the remaining
 * pipeline stages.
 *
 * @param data - Initial dataset
 * @param pipeline - Array of aggregation stages
 * @param options - Aggregation options including optional indexManager and namespace
 * @returns Promise of aggregation results
 */
export async function executeAggregationWithIndex<T = Document>(
  data: Document[],
  pipeline: AggregationStage[],
  options?: AggregationOptions
): Promise<T[]> {
  // If no index manager or pipeline is empty, fall back to sync execution
  if (!options?.indexManager || pipeline.length === 0) {
    return executeAggregation<T>(data, pipeline, options)
  }

  const indexManager = options.indexManager
  const namespace = options.namespace ?? 'default'

  // Check if first stage is $match
  const firstStage = pipeline[0]
  if (firstStage === undefined || !isMatchStage(firstStage)) {
    // No $match as first stage, use sync execution
    return executeAggregation<T>(data, pipeline, options)
  }

  const filter = firstStage.$match

  // Try to select an applicable index
  const selectedIndex = await indexManager.selectIndex(namespace, filter)

  if (!selectedIndex) {
    // No applicable index found, use sync execution
    return executeAggregation<T>(data, pipeline, options)
  }

  // Execute index lookup and filter data
  const filteredData = await executeIndexedMatch(data, filter, selectedIndex, indexManager, namespace)

  // Execute remaining pipeline on filtered data
  // Skip the first $match stage since we already applied it via index
  const remainingPipeline = pipeline.slice(1)

  return executeAggregation<T>(filteredData, remainingPipeline, options)
}

/**
 * Execute a $match stage using an index and return filtered data.
 *
 * @param data - Full dataset to filter
 * @param filter - The $match filter
 * @param selectedIndex - The selected index to use
 * @param indexManager - The index manager for lookups
 * @param namespace - The namespace for index lookups
 * @returns Promise of filtered data
 */
async function executeIndexedMatch(
  data: Document[],
  filter: Filter,
  selectedIndex: SelectedIndex,
  indexManager: IndexManager,
  namespace: string
): Promise<Document[]> {
  let candidateDocIds: string[] = []

  // Execute index lookup based on type
  // NOTE: Hash and SST indexes removed - equality and range queries now use native parquet predicate pushdown
  switch (selectedIndex.type) {
    case 'fts': {
      const textCondition = filter.$text as { $search: string } | undefined
      if (textCondition?.$search) {
        const results = await indexManager.ftsSearch(namespace, textCondition.$search, {})
        candidateDocIds = results.map((r: FTSSearchResult) => r.docId)
      }
      break
    }

    case 'vector': {
      // Vector search is typically used with $vector operator, not $match
      // For now, fall through to regular filter
      break
    }
  }

  // If no candidate IDs from index, perform full filter
  if (candidateDocIds.length === 0 && selectedIndex.type !== 'vector') {
    // Index returned no results - could mean empty result or need full scan
    // Apply the filter to check for actual matches
    return data.filter(doc => matchesFilter(doc, filter))
  }

  // Filter data to candidate documents
  const candidateSet = new Set(candidateDocIds)
  let filtered = data.filter(doc => {
    const id = (doc as Record<string, unknown>).$id as string
    return candidateSet.has(id)
  })

  // Apply the full filter to handle any conditions not covered by the index
  // (e.g., compound filters where only one field is indexed)
  filtered = filtered.filter(doc => matchesFilter(doc, filter))

  return filtered
}

/**
 * Extract range query operators from a condition.
 * @internal Reserved for future range query optimization
 */
export function _extractRangeQuery(condition: unknown): {
  $gt?: unknown | undefined
  $gte?: unknown | undefined
  $lt?: unknown | undefined
  $lte?: unknown | undefined
} | null {
  if (typeof condition !== 'object' || condition === null) {
    return null
  }

  const obj = condition as Record<string, unknown>
  const range: { $gt?: unknown | undefined; $gte?: unknown | undefined; $lt?: unknown | undefined; $lte?: unknown | undefined } = {}

  if ('$gt' in obj) range.$gt = obj.$gt
  if ('$gte' in obj) range.$gte = obj.$gte
  if ('$lt' in obj) range.$lt = obj.$lt
  if ('$lte' in obj) range.$lte = obj.$lte

  if (Object.keys(range).length === 0) {
    return null
  }

  return range
}
