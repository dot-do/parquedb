/**
 * Custom Vitest Matchers for ParqueDB
 *
 * Extend Vitest's expect with domain-specific assertions.
 */

import type { ExpectationResult } from 'vitest'
import type { Entity, EntityId, EntityRef, AuditFields, Filter } from '../src/types'
import { isFieldOperator } from '../src/types/filter'

// =============================================================================
// Type Guards
// =============================================================================

function isEntityId(value: unknown): value is EntityId {
  return typeof value === 'string' && /^[^/]+\/[^/]+/.test(value)
}

function hasAuditFields(value: unknown): value is AuditFields {
  if (typeof value !== 'object' || value === null) return false
  const obj = value as Record<string, unknown>
  return (
    obj.createdAt instanceof Date &&
    typeof obj.createdBy === 'string' &&
    obj.updatedAt instanceof Date &&
    typeof obj.updatedBy === 'string' &&
    typeof obj.version === 'number'
  )
}

function isEntityRef(value: unknown): value is EntityRef {
  if (typeof value !== 'object' || value === null) return false
  const obj = value as Record<string, unknown>
  return isEntityId(obj.$id) && typeof obj.$type === 'string' && typeof obj.name === 'string'
}

function isValidEntity(value: unknown): value is Entity {
  return isEntityRef(value) && hasAuditFields(value)
}

// =============================================================================
// Filter Matching
// =============================================================================

function matchesFilter(entity: Entity, filter: Filter): boolean {
  for (const [key, condition] of Object.entries(filter)) {
    // Skip undefined conditions
    if (condition === undefined) continue

    // Handle logical operators
    if (key === '$and' && Array.isArray(condition)) {
      if (!condition.every((f) => matchesFilter(entity, f as Filter))) {
        return false
      }
      continue
    }

    if (key === '$or' && Array.isArray(condition)) {
      if (!condition.some((f) => matchesFilter(entity, f as Filter))) {
        return false
      }
      continue
    }

    if (key === '$not' && typeof condition === 'object') {
      if (matchesFilter(entity, condition as Filter)) {
        return false
      }
      continue
    }

    if (key === '$nor' && Array.isArray(condition)) {
      if (condition.some((f) => matchesFilter(entity, f as Filter))) {
        return false
      }
      continue
    }

    // Skip special operators for now
    if (key.startsWith('$')) continue

    // Get field value from entity (supports nested paths)
    const fieldValue = getFieldValue(entity, key)

    // Check if condition is an operator or direct value
    if (isFieldOperator(condition)) {
      if (!matchesOperator(fieldValue, condition)) {
        return false
      }
    } else {
      // Direct equality
      if (fieldValue !== condition) {
        return false
      }
    }
  }

  return true
}

function getFieldValue(obj: Record<string, unknown>, path: string): unknown {
  const parts = path.split('.')
  let current: unknown = obj

  for (const part of parts) {
    if (current === null || current === undefined) return undefined
    if (typeof current !== 'object') return undefined
    current = (current as Record<string, unknown>)[part]
  }

  return current
}

function matchesOperator(value: unknown, operator: Record<string, unknown>): boolean {
  for (const [op, opValue] of Object.entries(operator)) {
    switch (op) {
      case '$eq':
        if (value !== opValue) return false
        break
      case '$ne':
        if (value === opValue) return false
        break
      case '$gt':
        if (typeof value !== 'number' || typeof opValue !== 'number' || value <= opValue) return false
        break
      case '$gte':
        if (typeof value !== 'number' || typeof opValue !== 'number' || value < opValue) return false
        break
      case '$lt':
        if (typeof value !== 'number' || typeof opValue !== 'number' || value >= opValue) return false
        break
      case '$lte':
        if (typeof value !== 'number' || typeof opValue !== 'number' || value > opValue) return false
        break
      case '$in':
        if (!Array.isArray(opValue) || !opValue.includes(value)) return false
        break
      case '$nin':
        if (!Array.isArray(opValue) || opValue.includes(value)) return false
        break
      case '$exists':
        if (opValue === true && value === undefined) return false
        if (opValue === false && value !== undefined) return false
        break
      case '$regex': {
        const regex = opValue instanceof RegExp ? opValue : new RegExp(opValue as string, operator.$options as string)
        if (typeof value !== 'string' || !regex.test(value)) return false
        break
      }
      case '$startsWith':
        if (typeof value !== 'string' || !value.startsWith(opValue as string)) return false
        break
      case '$endsWith':
        if (typeof value !== 'string' || !value.endsWith(opValue as string)) return false
        break
      case '$contains':
        if (typeof value !== 'string' || !value.includes(opValue as string)) return false
        break
    }
  }
  return true
}

// =============================================================================
// Custom Matchers
// =============================================================================

export const parquedbMatchers = {
  /**
   * Assert that a value is a valid ParqueDB entity
   */
  toBeValidEntity(received: unknown): ExpectationResult {
    const pass = isValidEntity(received)

    if (pass) {
      return {
        message: () => `expected ${JSON.stringify(received)} not to be a valid entity`,
        pass: true,
      }
    }

    const missing: string[] = []
    if (typeof received !== 'object' || received === null) {
      missing.push('must be an object')
    } else {
      const obj = received as Record<string, unknown>
      if (!isEntityId(obj.$id)) missing.push('$id (valid EntityId)')
      if (typeof obj.$type !== 'string') missing.push('$type (string)')
      if (typeof obj.name !== 'string') missing.push('name (string)')
      if (!(obj.createdAt instanceof Date)) missing.push('createdAt (Date)')
      if (typeof obj.createdBy !== 'string') missing.push('createdBy (EntityId)')
      if (!(obj.updatedAt instanceof Date)) missing.push('updatedAt (Date)')
      if (typeof obj.updatedBy !== 'string') missing.push('updatedBy (EntityId)')
      if (typeof obj.version !== 'number') missing.push('version (number)')
    }

    return {
      message: () => `expected value to be a valid entity, missing: ${missing.join(', ')}`,
      pass: false,
    }
  },

  /**
   * Assert that an entity matches a filter
   */
  toMatchFilter(received: unknown, filter: Filter): ExpectationResult {
    if (!isValidEntity(received)) {
      return {
        message: () => `expected a valid entity, got ${typeof received}`,
        pass: false,
      }
    }

    const pass = matchesFilter(received, filter)

    return {
      message: () =>
        pass
          ? `expected entity not to match filter ${JSON.stringify(filter)}`
          : `expected entity to match filter ${JSON.stringify(filter)}`,
      pass,
    }
  },

  /**
   * Assert that a value has all required audit fields
   */
  toHaveAuditFields(received: unknown): ExpectationResult {
    const pass = hasAuditFields(received)

    if (pass) {
      return {
        message: () => `expected ${JSON.stringify(received)} not to have audit fields`,
        pass: true,
      }
    }

    const missing: string[] = []
    if (typeof received !== 'object' || received === null) {
      missing.push('must be an object')
    } else {
      const obj = received as Record<string, unknown>
      if (!(obj.createdAt instanceof Date)) missing.push('createdAt (Date)')
      if (typeof obj.createdBy !== 'string') missing.push('createdBy (EntityId)')
      if (!(obj.updatedAt instanceof Date)) missing.push('updatedAt (Date)')
      if (typeof obj.updatedBy !== 'string') missing.push('updatedBy (EntityId)')
      if (typeof obj.version !== 'number') missing.push('version (number)')
    }

    return {
      message: () => `expected value to have audit fields, missing: ${missing.join(', ')}`,
      pass: false,
    }
  },

  /**
   * Assert that a value is a valid EntityId (ns/id format)
   */
  toBeEntityId(received: unknown): ExpectationResult {
    const pass = isEntityId(received)

    return {
      message: () =>
        pass
          ? `expected ${received} not to be a valid EntityId`
          : `expected ${received} to be a valid EntityId (format: namespace/id)`,
      pass,
    }
  },
}

// =============================================================================
// Type Declarations for TypeScript
// =============================================================================

declare module 'vitest' {
  interface Assertion<T = unknown> {
    toBeValidEntity(): void
    toMatchFilter(filter: Filter): void
    toHaveAuditFields(): void
    toBeEntityId(): void
  }

  interface AsymmetricMatchersContaining {
    toBeValidEntity(): void
    toMatchFilter(filter: Filter): void
    toHaveAuditFields(): void
    toBeEntityId(): void
  }
}
