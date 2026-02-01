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

// NOTE: Main parquedbMatchers object defined below after Parquet validation helpers

// =============================================================================
// Parquet File Validation
// =============================================================================

/** Magic bytes for Parquet files: "PAR1" */
const PARQUET_MAGIC = new Uint8Array([0x50, 0x41, 0x52, 0x31])

/** Magic bytes for ParqueDB bloom filter: "PQBF" */
const BLOOM_FILTER_MAGIC = new Uint8Array([0x50, 0x51, 0x42, 0x46])

/**
 * Check if bytes match the Parquet magic number
 */
function hasParquetMagic(data: Uint8Array): boolean {
  if (data.length < 4) return false
  return (
    data[0] === PARQUET_MAGIC[0] &&
    data[1] === PARQUET_MAGIC[1] &&
    data[2] === PARQUET_MAGIC[2] &&
    data[3] === PARQUET_MAGIC[3]
  )
}

/**
 * Check if bytes end with Parquet magic number (footer check)
 */
function hasParquetFooter(data: Uint8Array): boolean {
  if (data.length < 8) return false
  const offset = data.length - 4
  return (
    data[offset] === PARQUET_MAGIC[0] &&
    data[offset + 1] === PARQUET_MAGIC[1] &&
    data[offset + 2] === PARQUET_MAGIC[2] &&
    data[offset + 3] === PARQUET_MAGIC[3]
  )
}

/**
 * Check if bytes have bloom filter magic
 */
function hasBloomFilterMagic(data: Uint8Array): boolean {
  if (data.length < 16) return false
  return (
    data[0] === BLOOM_FILTER_MAGIC[0] &&
    data[1] === BLOOM_FILTER_MAGIC[1] &&
    data[2] === BLOOM_FILTER_MAGIC[2] &&
    data[3] === BLOOM_FILTER_MAGIC[3]
  )
}

// =============================================================================
// Index Structure Types
// =============================================================================

import type { IndexType, IndexDefinition } from '../src/indexes/types'
import type { Event, EventOp, Relationship, Variant } from '../src/types/entity'

/**
 * Parsed index structure for validation
 */
interface ParsedIndexStructure {
  type: IndexType
  version?: number
  entryCount?: number
  hasKeyHash?: boolean
  isValid: boolean
  error?: string
}

/**
 * Parse and validate an index buffer structure
 */
function parseIndexStructure(data: Uint8Array, expectedType: IndexType): ParsedIndexStructure {
  if (data.length < 6) {
    return { type: expectedType, isValid: false, error: 'Buffer too small for index header' }
  }

  // Check for bloom filter (16-byte header with PQBF magic)
  if (expectedType === 'bloom') {
    if (!hasBloomFilterMagic(data)) {
      return { type: 'bloom', isValid: false, error: 'Missing PQBF magic bytes' }
    }
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
    const version = view.getUint16(4, false)
    const numHashFunctions = view.getUint16(6, false)
    const filterSize = view.getUint32(8, false)
    const numRowGroups = view.getUint16(12, false)

    if (version !== 1) {
      return { type: 'bloom', isValid: false, error: `Unsupported bloom version: ${version}` }
    }

    if (numHashFunctions < 1 || numHashFunctions > 10) {
      return { type: 'bloom', isValid: false, error: `Invalid hash function count: ${numHashFunctions}` }
    }

    const expectedSize = 16 + filterSize + numRowGroups * 4096
    if (data.length < expectedSize) {
      return { type: 'bloom', isValid: false, error: `Buffer size ${data.length} < expected ${expectedSize}` }
    }

    return {
      type: 'bloom',
      version,
      entryCount: numRowGroups,
      isValid: true,
    }
  }

  // Check for hash/sst indexes (6-byte header: version + flags + entryCount)
  if (expectedType === 'hash' || expectedType === 'sst') {
    const version = data[0]
    const flags = data[1]
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
    const entryCount = view.getUint32(2, false)
    const hasKeyHash = (flags & 0x01) !== 0

    if (version < 1 || version > 3) {
      return { type: expectedType, isValid: false, error: `Unsupported index version: ${version}` }
    }

    return {
      type: expectedType,
      version,
      entryCount,
      hasKeyHash,
      isValid: true,
    }
  }

  // FTS indexes have their own structure
  if (expectedType === 'fts') {
    // FTS indexes use JSON header format
    try {
      const headerLengthView = new DataView(data.buffer, data.byteOffset, 4)
      const headerLength = headerLengthView.getUint32(0, true)
      if (headerLength > data.length - 4) {
        return { type: 'fts', isValid: false, error: 'FTS header length exceeds buffer' }
      }

      const headerBytes = data.slice(4, 4 + headerLength)
      const headerJson = new TextDecoder().decode(headerBytes)
      const header = JSON.parse(headerJson)

      if (!header.version) {
        return { type: 'fts', isValid: false, error: 'Missing version in FTS header' }
      }

      return {
        type: 'fts',
        version: header.version,
        isValid: true,
      }
    } catch (e) {
      return { type: 'fts', isValid: false, error: `Failed to parse FTS header: ${e}` }
    }
  }

  return { type: expectedType, isValid: false, error: `Unknown index type: ${expectedType}` }
}

// =============================================================================
// New Custom Matchers
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

  // ===========================================================================
  // New Matchers for Domain Types
  // ===========================================================================

  /**
   * Assert that a Uint8Array is a valid Parquet file
   *
   * Checks:
   * - Has "PAR1" magic bytes at start
   * - Has "PAR1" magic bytes at end (footer)
   * - Minimum valid size (at least 8 bytes for header + footer)
   *
   * @example
   * expect(parquetData).toBeValidParquetFile()
   */
  toBeValidParquetFile(received: unknown): ExpectationResult {
    if (!(received instanceof Uint8Array)) {
      return {
        message: () => `expected a Uint8Array, got ${typeof received}`,
        pass: false,
      }
    }

    const errors: string[] = []

    if (received.length < 8) {
      errors.push(`file too small: ${received.length} bytes (minimum 8)`)
    }

    if (!hasParquetMagic(received)) {
      errors.push('missing PAR1 magic bytes at start')
    }

    if (!hasParquetFooter(received)) {
      errors.push('missing PAR1 magic bytes at end (footer)')
    }

    const pass = errors.length === 0

    return {
      message: () =>
        pass
          ? `expected data not to be a valid Parquet file`
          : `expected valid Parquet file, errors: ${errors.join(', ')}`,
      pass,
    }
  },

  /**
   * Assert that an entity has a relationship with a specific predicate and target
   *
   * @param predicate - The relationship predicate name (e.g., "author", "categories")
   * @param target - Expected target EntityId or partial match
   *
   * @example
   * expect(post).toHaveRelationship('author', 'users/alice')
   * expect(post).toHaveRelationship('categories', /^categories\//)
   */
  toHaveRelationship(
    received: unknown,
    predicate: string,
    target?: string | RegExp
  ): ExpectationResult {
    if (typeof received !== 'object' || received === null) {
      return {
        message: () => `expected an object, got ${typeof received}`,
        pass: false,
      }
    }

    const entity = received as Record<string, unknown>
    const relValue = entity[predicate]

    if (relValue === undefined || relValue === null) {
      return {
        message: () => `expected entity to have relationship '${predicate}', but it was not found`,
        pass: false,
      }
    }

    // Handle RelLink/RelSet (object with displayName: entityId entries)
    if (typeof relValue !== 'object') {
      return {
        message: () => `expected '${predicate}' to be a relationship object, got ${typeof relValue}`,
        pass: false,
      }
    }

    const relObj = relValue as Record<string, unknown>
    const entityIds = Object.entries(relObj)
      .filter(([key]) => !key.startsWith('$'))
      .map(([, value]) => value as string)

    if (entityIds.length === 0) {
      return {
        message: () => `relationship '${predicate}' exists but has no linked entities`,
        pass: false,
      }
    }

    // If no target specified, just check that relationship exists with values
    if (target === undefined) {
      return {
        message: () => `expected entity not to have relationship '${predicate}'`,
        pass: true,
      }
    }

    // Check if target matches any of the entity IDs
    const matchFound = entityIds.some((entityId) => {
      if (target instanceof RegExp) {
        return target.test(entityId)
      }
      return entityId === target
    })

    return {
      message: () =>
        matchFound
          ? `expected entity not to have relationship '${predicate}' with target matching ${target}`
          : `expected entity to have relationship '${predicate}' with target matching ${target}, found: ${entityIds.join(', ')}`,
      pass: matchFound,
    }
  },

  /**
   * Assert that an event matches expected type and data
   *
   * @param expectedOp - Expected operation type ('CREATE', 'UPDATE', 'DELETE')
   * @param expectedData - Partial match for event data fields
   *
   * @example
   * expect(event).toMatchEvent('CREATE', { target: 'users:alice' })
   * expect(event).toMatchEvent('UPDATE', { op: 'UPDATE', actor: 'users:admin' })
   */
  toMatchEvent(
    received: unknown,
    expectedOp: EventOp,
    expectedData?: Partial<Event>
  ): ExpectationResult {
    if (typeof received !== 'object' || received === null) {
      return {
        message: () => `expected an Event object, got ${typeof received}`,
        pass: false,
      }
    }

    const event = received as Record<string, unknown>
    const errors: string[] = []

    // Check required Event fields
    if (typeof event.id !== 'string') {
      errors.push('missing or invalid id (should be string)')
    }

    if (typeof event.ts !== 'number') {
      errors.push('missing or invalid ts (should be number)')
    }

    if (event.op !== expectedOp) {
      errors.push(`expected op '${expectedOp}', got '${event.op}'`)
    }

    if (typeof event.target !== 'string') {
      errors.push('missing or invalid target (should be string)')
    }

    // Validate based on operation type
    if (expectedOp === 'CREATE' && event.before !== undefined) {
      errors.push('CREATE event should not have "before" field')
    }

    if (expectedOp === 'DELETE' && event.after !== undefined) {
      errors.push('DELETE event should not have "after" field')
    }

    // Check expected data matches
    if (expectedData) {
      for (const [key, expectedValue] of Object.entries(expectedData)) {
        const actualValue = event[key]
        if (actualValue !== expectedValue) {
          if (typeof expectedValue === 'object' && expectedValue !== null) {
            // Deep comparison for objects
            if (JSON.stringify(actualValue) !== JSON.stringify(expectedValue)) {
              errors.push(`${key}: expected ${JSON.stringify(expectedValue)}, got ${JSON.stringify(actualValue)}`)
            }
          } else {
            errors.push(`${key}: expected ${expectedValue}, got ${actualValue}`)
          }
        }
      }
    }

    const pass = errors.length === 0

    return {
      message: () =>
        pass
          ? `expected event not to match ${expectedOp} event`
          : `expected valid ${expectedOp} event, errors: ${errors.join(', ')}`,
      pass,
    }
  },

  /**
   * Assert that a buffer contains a valid index structure
   *
   * @param indexType - Expected index type ('hash', 'sst', 'fts', 'bloom')
   *
   * @example
   * expect(indexBuffer).toBeValidIndex('hash')
   * expect(bloomData).toBeValidIndex('bloom')
   */
  toBeValidIndex(received: unknown, indexType: IndexType): ExpectationResult {
    if (!(received instanceof Uint8Array)) {
      return {
        message: () => `expected a Uint8Array, got ${typeof received}`,
        pass: false,
      }
    }

    const result = parseIndexStructure(received, indexType)

    return {
      message: () =>
        result.isValid
          ? `expected data not to be a valid ${indexType} index`
          : `expected valid ${indexType} index, error: ${result.error}`,
      pass: result.isValid,
    }
  },

  /**
   * Assert that Parquet metadata has a specific number of row groups
   *
   * @param expectedCount - Expected number of row groups
   *
   * @example
   * expect(metadata).toHaveRowGroups(5)
   * expect(metadata.rowGroups).toHaveLength(5) // alternative
   */
  toHaveRowGroups(received: unknown, expectedCount: number): ExpectationResult {
    if (typeof received !== 'object' || received === null) {
      return {
        message: () => `expected a Parquet metadata object, got ${typeof received}`,
        pass: false,
      }
    }

    const metadata = received as Record<string, unknown>

    // Check for rowGroups array
    if (!Array.isArray(metadata.rowGroups)) {
      return {
        message: () => `expected object to have rowGroups array, got ${typeof metadata.rowGroups}`,
        pass: false,
      }
    }

    const actualCount = metadata.rowGroups.length
    const pass = actualCount === expectedCount

    return {
      message: () =>
        pass
          ? `expected not to have ${expectedCount} row groups`
          : `expected ${expectedCount} row groups, got ${actualCount}`,
      pass,
    }
  },

  /**
   * Assert that Parquet column metadata uses a specific compression codec
   *
   * @param codec - Expected compression codec name
   *
   * @example
   * expect(columnMeta).toBeCompressedWith('SNAPPY')
   * expect(columnMeta).toBeCompressedWith('ZSTD')
   */
  toBeCompressedWith(
    received: unknown,
    codec: 'UNCOMPRESSED' | 'SNAPPY' | 'GZIP' | 'LZO' | 'BROTLI' | 'LZ4' | 'ZSTD'
  ): ExpectationResult {
    if (typeof received !== 'object' || received === null) {
      return {
        message: () => `expected a column metadata object, got ${typeof received}`,
        pass: false,
      }
    }

    const columnMeta = received as Record<string, unknown>

    // Handle both column chunk metadata and Parquet metadata with columns array
    let actualCodec: string | undefined

    if (typeof columnMeta.codec === 'string') {
      // Direct column chunk metadata
      actualCodec = columnMeta.codec
    } else if (Array.isArray(columnMeta.columns)) {
      // Row group with columns array
      const firstColumn = columnMeta.columns[0] as Record<string, unknown> | undefined
      if (firstColumn && typeof firstColumn.codec === 'string') {
        actualCodec = firstColumn.codec
      }
    } else if (Array.isArray(columnMeta.rowGroups)) {
      // Full metadata with row groups
      const firstRowGroup = columnMeta.rowGroups[0] as Record<string, unknown> | undefined
      if (firstRowGroup && Array.isArray(firstRowGroup.columns)) {
        const firstColumn = firstRowGroup.columns[0] as Record<string, unknown> | undefined
        if (firstColumn && typeof firstColumn.codec === 'string') {
          actualCodec = firstColumn.codec
        }
      }
    }

    if (actualCodec === undefined) {
      return {
        message: () => `could not find compression codec in metadata structure`,
        pass: false,
      }
    }

    const pass = actualCodec.toUpperCase() === codec.toUpperCase()

    return {
      message: () =>
        pass
          ? `expected compression codec not to be ${codec}`
          : `expected compression codec ${codec}, got ${actualCodec}`,
      pass,
    }
  },
}

// =============================================================================
// Type Declarations for TypeScript
// =============================================================================

declare module 'vitest' {
  interface Assertion<T = unknown> {
    // Existing matchers
    toBeValidEntity(): void
    toMatchFilter(filter: Filter): void
    toHaveAuditFields(): void
    toBeEntityId(): void
    // New matchers
    toBeValidParquetFile(): void
    toHaveRelationship(predicate: string, target?: string | RegExp): void
    toMatchEvent(expectedOp: EventOp, expectedData?: Partial<Event>): void
    toBeValidIndex(indexType: IndexType): void
    toHaveRowGroups(expectedCount: number): void
    toBeCompressedWith(codec: 'UNCOMPRESSED' | 'SNAPPY' | 'GZIP' | 'LZO' | 'BROTLI' | 'LZ4' | 'ZSTD'): void
  }

  interface AsymmetricMatchersContaining {
    // Existing matchers
    toBeValidEntity(): void
    toMatchFilter(filter: Filter): void
    toHaveAuditFields(): void
    toBeEntityId(): void
    // New matchers
    toBeValidParquetFile(): void
    toHaveRelationship(predicate: string, target?: string | RegExp): void
    toMatchEvent(expectedOp: EventOp, expectedData?: Partial<Event>): void
    toBeValidIndex(indexType: IndexType): void
    toHaveRowGroups(expectedCount: number): void
    toBeCompressedWith(codec: 'UNCOMPRESSED' | 'SNAPPY' | 'GZIP' | 'LZO' | 'BROTLI' | 'LZ4' | 'ZSTD'): void
  }
}
