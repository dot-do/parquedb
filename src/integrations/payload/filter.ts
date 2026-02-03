/**
 * Filter translation between Payload CMS and ParqueDB
 *
 * Payload uses operators like 'equals', 'not_equals', 'contains', etc.
 * ParqueDB uses MongoDB-style operators like $eq, $ne, $contains, etc.
 */

import type { Filter as ParqueDBFilter } from '../../types/filter'
import type { PayloadWhere, PayloadWhereField } from './types'

/**
 * Translate a Payload where clause to a ParqueDB filter
 *
 * @example
 * // Payload: { status: { equals: 'published' } }
 * // ParqueDB: { status: { $eq: 'published' } } or just { status: 'published' }
 *
 * @example
 * // Payload: { and: [{ status: { equals: 'published' } }, { featured: { equals: true } }] }
 * // ParqueDB: { $and: [{ status: 'published' }, { featured: true }] }
 */
export function translatePayloadFilter(where: PayloadWhere | undefined): ParqueDBFilter {
  if (!where) {
    return {}
  }

  const result: ParqueDBFilter = {}

  // Handle logical operators
  if (where.and && Array.isArray(where.and)) {
    result.$and = where.and.map(w => translatePayloadFilter(w))
  }

  if (where.or && Array.isArray(where.or)) {
    result.$or = where.or.map(w => translatePayloadFilter(w))
  }

  // Handle field conditions
  for (const [field, condition] of Object.entries(where)) {
    // Skip logical operators (already handled)
    if (field === 'and' || field === 'or') {
      continue
    }

    // Skip undefined/null conditions
    if (condition === undefined || condition === null) {
      continue
    }

    // Handle nested Where (recursive)
    if (Array.isArray(condition)) {
      // This is a nested and/or for this field (less common pattern)
      // In Payload this would be: { field: [{ equals: 'a' }, { equals: 'b' }] }
      // which means OR on those conditions
      result.$or = result.$or || []
      for (const nestedWhere of condition) {
        const translated = translatePayloadFilter({ [field]: nestedWhere } as PayloadWhere)
        result.$or.push(translated)
      }
      continue
    }

    // Handle field conditions
    const translatedField = translateFieldCondition(field, condition as PayloadWhereField)
    if (translatedField !== undefined) {
      result[field] = translatedField
    }
  }

  return result
}

/**
 * Translate a single field condition from Payload to ParqueDB format
 */
function translateFieldCondition(
  field: string,
  condition: PayloadWhereField
): unknown {
  // If the condition is a primitive value, treat it as equality
  if (typeof condition !== 'object' || condition === null) {
    return condition
  }

  // Handle each Payload operator
  const operators: Array<[keyof PayloadWhereField, string]> = [
    ['equals', '$eq'],
    ['not_equals', '$ne'],
    ['greater_than', '$gt'],
    ['greater_than_equal', '$gte'],
    ['less_than', '$lt'],
    ['less_than_equal', '$lte'],
    ['in', '$in'],
    ['not_in', '$nin'],
    ['all', '$all'],
    ['exists', '$exists'],
    ['contains', '$contains'],
    ['like', '$regex'],
    ['not_like', '$not'],
  ]

  // Check for simple equality shorthand
  if ('equals' in condition && Object.keys(condition).length === 1) {
    // Return the value directly for simple equality
    return condition.equals
  }

  const result: Record<string, unknown> = {}

  for (const [payloadOp, parquedbOp] of operators) {
    if (payloadOp in condition) {
      const value = condition[payloadOp]

      if (payloadOp === 'like') {
        // Convert SQL LIKE pattern to regex
        // % matches any sequence, _ matches single character
        result[parquedbOp] = convertLikeToRegex(value as string)
      } else if (payloadOp === 'not_like') {
        // Wrap in $not with regex
        result['$not'] = { $regex: convertLikeToRegex(value as string) }
      } else {
        result[parquedbOp] = value
      }
    }
  }

  // Handle geospatial operators (basic support)
  if ('within' in condition) {
    result['$geo'] = { $within: condition.within }
  }

  if ('intersects' in condition) {
    result['$geo'] = { $intersects: condition.intersects }
  }

  if ('near' in condition) {
    // Payload near format: { near: "lng,lat,maxDistance,minDistance" }
    // or { near: [lng, lat, maxDistance, minDistance] }
    const near = condition.near
    if (typeof near === 'string') {
      const [lng, lat, maxDistance, minDistance] = near.split(',').map(Number)
      result['$geo'] = {
        $near: { lng: lng!, lat: lat! },
        $maxDistance: maxDistance,
        $minDistance: minDistance,
      }
    } else if (Array.isArray(near)) {
      const [lng, lat, maxDistance, minDistance] = near as number[]
      result['$geo'] = {
        $near: { lng: lng!, lat: lat! },
        $maxDistance: maxDistance,
        $minDistance: minDistance,
      }
    }
  }

  // If we only have one operator and it's $eq, return the value directly
  if (Object.keys(result).length === 1 && '$eq' in result) {
    return result['$eq']
  }

  // If we have no operators, return undefined
  if (Object.keys(result).length === 0) {
    return undefined
  }

  return result
}

/**
 * Convert SQL LIKE pattern to JavaScript regex pattern
 *
 * @example
 * convertLikeToRegex('%test%') // '.*test.*'
 * convertLikeToRegex('test%') // '^test.*'
 * convertLikeToRegex('%test') // '.*test$'
 * convertLikeToRegex('te_t') // '^te.t$'
 */
export function convertLikeToRegex(pattern: string): string {
  // Escape regex special characters except % and _
  let regex = pattern.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')

  // Convert SQL wildcards to regex
  regex = regex
    .replace(/%/g, '.*')  // % matches any sequence
    .replace(/_/g, '.')   // _ matches single character

  // Add anchors if pattern doesn't start/end with wildcard
  if (!pattern.startsWith('%')) {
    regex = '^' + regex
  }
  if (!pattern.endsWith('%')) {
    regex = regex + '$'
  }

  return regex
}

/**
 * Translate a Payload sort specification to ParqueDB format
 *
 * Payload supports:
 * - String: 'field' (asc) or '-field' (desc)
 * - Array: ['field', '-field2']
 *
 * ParqueDB uses:
 * - { field: 1 } for ascending
 * - { field: -1 } for descending
 */
export function translatePayloadSort(sort: string | string[] | undefined): Record<string, 1 | -1> | undefined {
  if (!sort) {
    return undefined
  }

  const result: Record<string, 1 | -1> = {}

  const sortFields = Array.isArray(sort) ? sort : [sort]

  for (const field of sortFields) {
    if (field.startsWith('-')) {
      result[field.slice(1)] = -1
    } else {
      result[field] = 1
    }
  }

  return Object.keys(result).length > 0 ? result : undefined
}

/**
 * Build a filter for finding a document by ID
 */
export function buildIdFilter(id: string | number): ParqueDBFilter {
  const idString = String(id)
  // Check if this is a full entity ID (ns/id format)
  if (idString.includes('/')) {
    return { $id: idString }
  }
  // Otherwise filter by local id
  return { id: idString }
}

/**
 * Combine multiple filters with AND logic
 */
export function combineFilters(...filters: (ParqueDBFilter | undefined)[]): ParqueDBFilter {
  const validFilters = filters.filter((f): f is ParqueDBFilter => f !== undefined && Object.keys(f).length > 0)

  if (validFilters.length === 0) {
    return {}
  }

  if (validFilters.length === 1) {
    return validFilters[0]!
  }

  return { $and: validFilters }
}
