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
} from './types'
import { matchesFilter } from '../query/filter'
import { getNestedValue, compareValues } from '../utils'

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Set value at a nested path using dot notation
 */
function setNestedValue(obj: Record<string, unknown>, path: string, value: unknown): void {
  const parts = path.split('.')
  let current = obj
  for (let i = 0; i < parts.length - 1; i++) {
    const part = parts[i]!
    if (!(part in current) || typeof current[part] !== 'object' || current[part] === null) {
      current[part] = {}
    }
    current = current[part] as Record<string, unknown>
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
 * Evaluate a field reference (e.g., '$fieldName')
 */
function evaluateFieldRef(doc: Record<string, unknown>, ref: unknown): unknown {
  if (typeof ref === 'string' && ref.startsWith('$')) {
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
function executeMatch(data: unknown[], filter: Filter): unknown[] {
  return data.filter(doc => matchesFilter(doc, filter))
}

/**
 * Execute $group stage
 */
function executeGroup(data: unknown[], groupSpec: GroupSpec): unknown[] {
  const groups = new Map<string, unknown[]>()

  // Group documents by _id
  for (const item of data) {
    const doc = item as Record<string, unknown>
    let groupKey: unknown

    if (groupSpec._id === null) {
      groupKey = null
    } else if (typeof groupSpec._id === 'string' && groupSpec._id.startsWith('$')) {
      groupKey = getNestedValue(doc, groupSpec._id.slice(1))
    } else if (typeof groupSpec._id === 'object' && groupSpec._id !== null) {
      // Compound _id
      const compoundKey: Record<string, unknown> = {}
      for (const [key, value] of Object.entries(groupSpec._id as Record<string, unknown>)) {
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
    groups.get(keyStr)!.push(item)
  }

  // Apply accumulators to each group
  return Array.from(groups.entries()).map(([keyStr, items]) => {
    const result: Record<string, unknown> = { _id: JSON.parse(keyStr) }

    for (const [field, spec] of Object.entries(groupSpec)) {
      if (field === '_id') continue

      if (spec && typeof spec === 'object') {
        const specObj = spec as Record<string, unknown>

        // $sum accumulator
        if ('$sum' in specObj) {
          if (specObj.$sum === 1) {
            result[field] = items.length
          } else if (typeof specObj.$sum === 'number') {
            result[field] = items.length * specObj.$sum
          } else if (typeof specObj.$sum === 'string' && specObj.$sum.startsWith('$')) {
            result[field] = items.reduce((sum: number, item) => {
              const val = getNestedValue(item as Record<string, unknown>, (specObj.$sum as string).slice(1))
              return sum + (typeof val === 'number' ? val : 0)
            }, 0)
          }
        }

        // $avg accumulator
        else if ('$avg' in specObj && typeof specObj.$avg === 'string' && specObj.$avg.startsWith('$')) {
          const sum = items.reduce((s: number, item) => {
            const val = getNestedValue(item as Record<string, unknown>, (specObj.$avg as string).slice(1))
            return s + (typeof val === 'number' ? val : 0)
          }, 0)
          result[field] = items.length > 0 ? sum / items.length : 0
        }

        // $max accumulator
        else if ('$max' in specObj && typeof specObj.$max === 'string' && specObj.$max.startsWith('$')) {
          result[field] = items.reduce((max: number | null, item) => {
            const val = getNestedValue(item as Record<string, unknown>, (specObj.$max as string).slice(1))
            return typeof val === 'number' && (max === null || val > max) ? val : max
          }, null as number | null)
        }

        // $min accumulator
        else if ('$min' in specObj && typeof specObj.$min === 'string' && specObj.$min.startsWith('$')) {
          result[field] = items.reduce((min: number | null, item) => {
            const val = getNestedValue(item as Record<string, unknown>, (specObj.$min as string).slice(1))
            return typeof val === 'number' && (min === null || val < min) ? val : min
          }, null as number | null)
        }

        // $first accumulator
        else if ('$first' in specObj && typeof specObj.$first === 'string' && specObj.$first.startsWith('$')) {
          const firstItem = items[0]
          if (firstItem) {
            result[field] = getNestedValue(firstItem as Record<string, unknown>, (specObj.$first as string).slice(1))
          }
        }

        // $last accumulator
        else if ('$last' in specObj && typeof specObj.$last === 'string' && specObj.$last.startsWith('$')) {
          const lastItem = items[items.length - 1]
          if (lastItem) {
            result[field] = getNestedValue(lastItem as Record<string, unknown>, (specObj.$last as string).slice(1))
          }
        }

        // $push accumulator
        else if ('$push' in specObj && typeof specObj.$push === 'string' && specObj.$push.startsWith('$')) {
          result[field] = items.map(item =>
            getNestedValue(item as Record<string, unknown>, (specObj.$push as string).slice(1))
          )
        }

        // $addToSet accumulator
        else if ('$addToSet' in specObj && typeof specObj.$addToSet === 'string' && specObj.$addToSet.startsWith('$')) {
          const values = items.map(item =>
            getNestedValue(item as Record<string, unknown>, (specObj.$addToSet as string).slice(1))
          )
          result[field] = [...new Set(values.map(v => JSON.stringify(v)))].map(s => JSON.parse(s))
        }

        // $count accumulator (count non-null values)
        else if ('$count' in specObj) {
          result[field] = items.length
        }
      }
    }

    return result
  })
}

/**
 * Execute $sort stage
 */
function executeSort(data: unknown[], sortSpec: Record<string, 1 | -1>): unknown[] {
  const sortEntries = Object.entries(sortSpec)

  return [...data].sort((a, b) => {
    for (const [field, direction] of sortEntries) {
      const aValue = getNestedValue(a as Record<string, unknown>, field)
      const bValue = getNestedValue(b as Record<string, unknown>, field)
      const cmp = compareValuesWithDirection(aValue, bValue, direction)
      if (cmp !== 0) return cmp
    }
    return 0
  })
}

/**
 * Execute $limit stage
 */
function executeLimit(data: unknown[], limit: number): unknown[] {
  return data.slice(0, limit)
}

/**
 * Execute $skip stage
 */
function executeSkip(data: unknown[], skip: number): unknown[] {
  return data.slice(skip)
}

/**
 * Execute $project stage
 */
function executeProject(data: unknown[], projection: Record<string, unknown>): unknown[] {
  return data.map(item => {
    const doc = item as Record<string, unknown>
    const result: Record<string, unknown> = {}
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
function evaluateProjectionExpression(doc: Record<string, unknown>, expr: unknown): unknown {
  if (typeof expr === 'string' && expr.startsWith('$')) {
    return getNestedValue(doc, expr.slice(1))
  }

  if (typeof expr === 'object' && expr !== null) {
    const exprObj = expr as Record<string, unknown>

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
      return b !== 0 ? (a ?? 0) / b : null
    }

    // Conditional operators
    if ('$cond' in exprObj) {
      const cond = exprObj.$cond as { if: unknown; then: unknown; else: unknown } | unknown[]
      if (Array.isArray(cond)) {
        const [condition, thenVal, elseVal] = cond
        const evaluated = evaluateProjectionExpression(doc, condition)
        return evaluated ? evaluateFieldRef(doc, thenVal) : evaluateFieldRef(doc, elseVal)
      } else {
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
function executeUnwind(data: unknown[], unwind: string | UnwindOptions): unknown[] {
  const path = typeof unwind === 'string' ? unwind : unwind.path
  const preserveNull = typeof unwind === 'object' ? unwind.preserveNullAndEmptyArrays : false
  const includeArrayIndex = typeof unwind === 'object' ? unwind.includeArrayIndex : undefined
  const fieldPath = path.startsWith('$') ? path.slice(1) : path

  const newData: unknown[] = []

  for (const item of data) {
    const doc = item as Record<string, unknown>
    const arr = getNestedValue(doc, fieldPath)

    if (Array.isArray(arr) && arr.length > 0) {
      for (let i = 0; i < arr.length; i++) {
        const newItem = { ...doc }
        setNestedValue(newItem, fieldPath, arr[i])
        if (includeArrayIndex) {
          newItem[includeArrayIndex] = i
        }
        newData.push(newItem)
      }
    } else if (preserveNull) {
      const newItem = { ...doc }
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
  data: unknown[],
  lookup: LookupOptions,
  _resolver?: (collection: string) => unknown[]
): unknown[] {
  // Simplified implementation - just adds empty array
  // Full implementation would require database access
  return data.map(item => {
    const doc = item as Record<string, unknown>
    return {
      ...doc,
      [lookup.as]: [],
    }
  })
}

/**
 * Execute $count stage
 */
function executeCount(data: unknown[], fieldName: string): unknown[] {
  return [{ [fieldName]: data.length }]
}

/**
 * Execute $addFields or $set stage
 */
function executeAddFields(data: unknown[], fields: Record<string, unknown>): unknown[] {
  return data.map(item => {
    const doc = item as Record<string, unknown>
    const result = { ...doc }

    for (const [key, value] of Object.entries(fields)) {
      if (typeof value === 'object' && value !== null) {
        result[key] = evaluateProjectionExpression(doc, value)
      } else if (typeof value === 'string' && value.startsWith('$')) {
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
function executeUnset(data: unknown[], fields: string | string[]): unknown[] {
  const fieldsToRemove = Array.isArray(fields) ? fields : [fields]

  return data.map(item => {
    const doc = item as Record<string, unknown>
    const result = { ...doc }

    for (const field of fieldsToRemove) {
      delete result[field]
    }

    return result
  })
}

/**
 * Execute $replaceRoot stage
 */
function executeReplaceRoot(data: unknown[], newRoot: unknown): unknown[] {
  return data.map(item => {
    const doc = item as Record<string, unknown>
    const root = evaluateFieldRef(doc, newRoot)
    return typeof root === 'object' && root !== null ? root : doc
  })
}

/**
 * Execute $sample stage
 */
function executeSample(data: unknown[], size: number): unknown[] {
  // Fisher-Yates shuffle and take first `size` elements
  const shuffled = [...data]
  for (let i = shuffled.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1))
    ;[shuffled[i], shuffled[j]] = [shuffled[j]!, shuffled[i]!]
  }
  return shuffled.slice(0, size)
}

/**
 * Execute $bucket stage
 */
function executeBucket(
  data: unknown[],
  bucket: {
    groupBy: string
    boundaries: unknown[]
    default?: string
    output?: Record<string, unknown>
  }
): unknown[] {
  const { groupBy, boundaries, default: defaultBucket, output } = bucket
  const groups = new Map<string, unknown[]>()

  // Initialize buckets
  for (let i = 0; i < boundaries.length - 1; i++) {
    groups.set(String(boundaries[i]), [])
  }
  if (defaultBucket) {
    groups.set(defaultBucket, [])
  }

  // Assign documents to buckets
  for (const item of data) {
    const doc = item as Record<string, unknown>
    const value = evaluateFieldRef(doc, groupBy)
    let assigned = false

    for (let i = 0; i < boundaries.length - 1; i++) {
      if (compareValues(value, boundaries[i]) >= 0 && compareValues(value, boundaries[i + 1]) < 0) {
        groups.get(String(boundaries[i]))!.push(item)
        assigned = true
        break
      }
    }

    if (!assigned && defaultBucket) {
      groups.get(defaultBucket)!.push(item)
    }
  }

  // Build results
  return Array.from(groups.entries())
    .filter(([, items]) => items.length > 0)
    .map(([key, items]) => {
      const result: Record<string, unknown> = { _id: key === defaultBucket ? defaultBucket : Number(key) }

      if (output) {
        for (const [field, spec] of Object.entries(output)) {
          if (spec && typeof spec === 'object' && '$sum' in (spec as Record<string, unknown>)) {
            const sumSpec = (spec as Record<string, unknown>).$sum
            if (sumSpec === 1) {
              result[field] = items.length
            } else if (typeof sumSpec === 'string' && sumSpec.startsWith('$')) {
              result[field] = items.reduce((sum: number, item) => {
                const val = getNestedValue(item as Record<string, unknown>, sumSpec.slice(1))
                return sum + (typeof val === 'number' ? val : 0)
              }, 0)
            }
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
 * @param data - Initial dataset
 * @param pipeline - Array of aggregation stages
 * @param options - Aggregation options
 * @returns Aggregation results
 */
export function executeAggregation<T = unknown>(
  data: unknown[],
  pipeline: AggregationStage[],
  options?: AggregationOptions
): T[] {
  let result = [...data]

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
      const facetResults: Record<string, unknown[]> = {}
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
  execute<T = unknown>(): T[] {
    return executeAggregation<T>(this.data, this.pipeline, this.options)
  }

  /**
   * Execute the pipeline with explain mode
   */
  explain(): {
    stages: { name: string; inputCount: number; outputCount: number }[]
    totalDocuments: number
  } {
    const stages: { name: string; inputCount: number; outputCount: number }[] = []
    let currentData = [...this.data]

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
