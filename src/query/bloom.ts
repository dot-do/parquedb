/**
 * Bloom Filter Integration for Parquet
 *
 * Bloom filters provide probabilistic set membership testing:
 * - Returns false: Value is DEFINITELY NOT present
 * - Returns true: Value MIGHT be present (false positives possible)
 *
 * Parquet files can include bloom filters per column per row group,
 * enabling efficient filtering of equality predicates.
 */

import type { StorageBackend } from '../types/storage'
import type { Filter } from '../types/filter'
import { logger } from '../utils/logger'

// =============================================================================
// Bloom Filter Types
// =============================================================================

/**
 * Bloom filter configuration
 */
export interface BloomFilterConfig {
  /** Number of bits in the filter (m) */
  numBits: number
  /** Number of hash functions (k) */
  numHashes: number
  /** False positive probability (default: 0.01 = 1%) */
  falsePositiveRate: number
}

/**
 * Bloom filter header as stored in Parquet
 */
export interface BloomFilterHeader {
  /** Offset to the bloom filter data in the file */
  offset: number
  /** Length of the bloom filter data */
  length: number
  /** Number of bits in the filter */
  numBits: number
  /** Number of hash functions */
  numHashes: number
  /** Hash algorithm used (typically xxHash) */
  hashAlgorithm: 'xxhash' | 'murmur3'
}

/**
 * Bloom filter data structure
 */
export interface BloomFilter {
  /** Bit array (as Uint8Array for efficient storage) */
  bits: Uint8Array
  /** Number of hash functions */
  numHashes: number
  /** Number of bits (m) */
  numBits: number
}

// =============================================================================
// Bloom Filter Operations
// =============================================================================

/**
 * Check bloom filter for value existence
 *
 * @param storage - Storage backend
 * @param ns - Namespace (collection name)
 * @param field - Field name to check
 * @param value - Value to look for
 * @returns false = definitely not present, true = might be present
 */
export async function checkBloomFilter(
  storage: StorageBackend,
  ns: string,
  field: string,
  value: unknown
): Promise<boolean> {
  // Get the bloom filter path
  const bloomPath = `indexes/bloom/${ns}.bloom`

  // Check if bloom filter exists
  const exists = await storage.exists(bloomPath)
  if (!exists) {
    // No bloom filter, assume value might exist
    return true
  }

  try {
    // Read bloom filter data
    const data = await storage.read(bloomPath)

    // Parse bloom filter
    const filter = parseBloomFilter(data)

    // Get the field's bloom filter (bloom file may contain multiple field filters)
    const fieldFilter = getFieldBloomFilter(filter, field)
    if (!fieldFilter) {
      // No bloom filter for this field
      return true
    }

    // Check if value might exist
    return bloomFilterMightContain(fieldFilter, value)
  } catch (error: unknown) {
    // Error reading bloom filter - conservatively assume value might exist
    logger.debug('Bloom filter check failed, assuming value might exist', error)
    return true
  }
}

/**
 * Check if a value might exist in a bloom filter
 *
 * @param filter - Bloom filter
 * @param value - Value to check
 * @returns false = definitely not present, true = might be present
 */
export function bloomFilterMightContain(filter: BloomFilter, value: unknown): boolean {
  // Convert value to bytes for hashing
  const bytes = valueToBytes(value)

  // Calculate hash values
  const hashes = calculateHashes(bytes, filter.numHashes, filter.numBits)

  // Check all hash positions
  for (const hash of hashes) {
    const byteIndex = Math.floor(hash / 8)
    const bitIndex = hash % 8

    if (byteIndex >= filter.bits.length) {
      return true // Out of bounds, assume might contain
    }

    if ((filter.bits[byteIndex]! & (1 << bitIndex)) === 0) {
      return false // Bit not set, definitely not present
    }
  }

  return true // All bits set, might be present
}

/**
 * Create a new bloom filter
 *
 * @param expectedItems - Expected number of items
 * @param falsePositiveRate - Desired false positive rate (0-1)
 * @returns Empty bloom filter
 */
export function createBloomFilter(
  expectedItems: number,
  falsePositiveRate: number = 0.01
): BloomFilter {
  // Calculate optimal number of bits (m)
  // m = -(n * ln(p)) / (ln(2)^2)
  const numBits = Math.ceil(
    -(expectedItems * Math.log(falsePositiveRate)) / (Math.log(2) ** 2)
  )

  // Calculate optimal number of hash functions (k)
  // k = (m/n) * ln(2)
  const numHashes = Math.max(1, Math.round((numBits / expectedItems) * Math.log(2)))

  // Round up to nearest byte
  const numBytes = Math.ceil(numBits / 8)

  return {
    bits: new Uint8Array(numBytes),
    numHashes,
    numBits,
  }
}

/**
 * Add a value to a bloom filter
 *
 * @param filter - Bloom filter to modify
 * @param value - Value to add
 */
export function bloomFilterAdd(filter: BloomFilter, value: unknown): void {
  const bytes = valueToBytes(value)
  const hashes = calculateHashes(bytes, filter.numHashes, filter.numBits)

  for (const hash of hashes) {
    const byteIndex = Math.floor(hash / 8)
    const bitIndex = hash % 8

    if (byteIndex < filter.bits.length) {
      filter.bits[byteIndex]! |= 1 << bitIndex
    }
  }
}

/**
 * Merge two bloom filters
 * Both filters must have the same configuration
 *
 * @param a - First bloom filter
 * @param b - Second bloom filter
 * @returns Merged bloom filter
 */
export function bloomFilterMerge(a: BloomFilter, b: BloomFilter): BloomFilter {
  if (a.numBits !== b.numBits || a.numHashes !== b.numHashes) {
    throw new Error('Cannot merge bloom filters with different configurations')
  }

  const merged = new Uint8Array(a.bits.length)
  for (let i = 0; i < a.bits.length; i++) {
    merged[i] = a.bits[i]! | b.bits[i]!
  }

  return {
    bits: merged,
    numHashes: a.numHashes,
    numBits: a.numBits,
  }
}

/**
 * Estimate the number of items in a bloom filter
 *
 * @param filter - Bloom filter
 * @returns Estimated number of items
 */
export function bloomFilterEstimateCount(filter: BloomFilter): number {
  // Count set bits
  let setBits = 0
  for (let i = 0; i < filter.bits.length; i++) {
    setBits += popcount(filter.bits[i] ?? 0)
  }

  // Estimate using formula: n = -m * ln(1 - X/m) / k
  // where X is the number of set bits
  const m = filter.numBits
  const k = filter.numHashes
  const ratio = setBits / m

  if (ratio >= 1) {
    return Infinity // Filter is saturated
  }

  return Math.round(-m * Math.log(1 - ratio) / k)
}

/**
 * Serialize bloom filter to bytes
 *
 * @param filter - Bloom filter
 * @returns Serialized bytes
 */
export function serializeBloomFilter(filter: BloomFilter): Uint8Array {
  // Header: 4 bytes numBits + 4 bytes numHashes + bits
  const header = new ArrayBuffer(8)
  const view = new DataView(header)
  view.setUint32(0, filter.numBits, true) // little-endian
  view.setUint32(4, filter.numHashes, true)

  const result = new Uint8Array(8 + filter.bits.length)
  result.set(new Uint8Array(header), 0)
  result.set(filter.bits, 8)

  return result
}

/**
 * Deserialize bloom filter from bytes
 *
 * @param data - Serialized bytes
 * @returns Bloom filter
 */
export function deserializeBloomFilter(data: Uint8Array): BloomFilter {
  if (data.length < 8) {
    throw new Error('Invalid bloom filter data: too short')
  }

  const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
  const numBits = view.getUint32(0, true)
  const numHashes = view.getUint32(4, true)
  const bits = data.slice(8)

  return { bits, numHashes, numBits }
}

// =============================================================================
// Multi-Field Bloom Filter Storage
// =============================================================================

/**
 * Multi-field bloom filter index
 * Stores bloom filters for multiple fields in a single file
 */
export interface BloomFilterIndex {
  /** Version of the index format */
  version: number
  /** Bloom filters by field name */
  filters: Map<string, BloomFilter>
  /** Metadata */
  metadata: {
    createdAt: Date
    rowCount: number
    fields: string[]
  }
}

/**
 * Create a new bloom filter index for multiple fields
 *
 * @param fields - Field names to create filters for
 * @param expectedRows - Expected number of rows
 * @returns Empty bloom filter index
 */
export function createBloomFilterIndex(
  fields: string[],
  expectedRows: number
): BloomFilterIndex {
  const filters = new Map<string, BloomFilter>()

  for (const field of fields) {
    filters.set(field, createBloomFilter(expectedRows))
  }

  return {
    version: 1,
    filters,
    metadata: {
      createdAt: new Date(),
      rowCount: 0,
      fields,
    },
  }
}

/**
 * Add a row to the bloom filter index
 *
 * @param index - Bloom filter index
 * @param row - Row data
 */
export function bloomFilterIndexAddRow(
  index: BloomFilterIndex,
  row: Record<string, unknown>
): void {
  const entries = Array.from(index.filters.entries())
  for (const [field, filter] of entries) {
    const value = getNestedValue(row, field)
    if (value !== undefined && value !== null) {
      bloomFilterAdd(filter, value)
    }
  }
  index.metadata.rowCount++
}

/**
 * Check if a filter could match any rows in the index
 *
 * @param index - Bloom filter index
 * @param filter - MongoDB-style filter
 * @returns false = definitely no matches, true = might have matches
 */
export function bloomFilterIndexMightMatch(
  index: BloomFilterIndex,
  filter: Filter
): boolean {
  // Extract equality conditions
  for (const [field, value] of Object.entries(filter)) {
    if (field.startsWith('$')) continue

    const bloomFilter = index.filters.get(field)
    if (!bloomFilter) continue

    // Check direct equality
    if (value !== null && typeof value !== 'object') {
      if (!bloomFilterMightContain(bloomFilter, value)) {
        return false
      }
    }

    // Check $eq operator
    if (typeof value === 'object' && value !== null && '$eq' in (value as Record<string, unknown>)) {
      const eqValue = (value as { $eq: unknown }).$eq
      if (!bloomFilterMightContain(bloomFilter, eqValue)) {
        return false
      }
    }

    // Check $in operator (at least one value must might-exist)
    if (typeof value === 'object' && value !== null && '$in' in (value as Record<string, unknown>)) {
      const inValues = (value as { $in: unknown[] }).$in
      let anyMightExist = false
      for (const v of inValues) {
        if (bloomFilterMightContain(bloomFilter, v)) {
          anyMightExist = true
          break
        }
      }
      if (!anyMightExist) {
        return false
      }
    }
  }

  return true
}

/**
 * Serialize bloom filter index to bytes
 */
export function serializeBloomFilterIndex(index: BloomFilterIndex): Uint8Array {
  // Simple JSON header + binary filters
  const header = {
    version: index.version,
    metadata: {
      createdAt: index.metadata.createdAt.toISOString(),
      rowCount: index.metadata.rowCount,
      fields: index.metadata.fields,
    },
    filterOffsets: {} as Record<string, { offset: number; length: number }>,
  }

  // Calculate filter offsets
  let offset = 0
  const filterBuffers: Uint8Array[] = []
  const entries = Array.from(index.filters.entries())

  for (const [field, filter] of entries) {
    const serialized = serializeBloomFilter(filter)
    header.filterOffsets[field] = { offset, length: serialized.length }
    filterBuffers.push(serialized)
    offset += serialized.length
  }

  // Serialize header as JSON
  const headerJson = JSON.stringify(header)
  const headerBytes = new TextEncoder().encode(headerJson)
  const headerLengthBytes = new Uint8Array(4)
  new DataView(headerLengthBytes.buffer).setUint32(0, headerBytes.length, true)

  // Combine all parts
  const totalLength = 4 + headerBytes.length + filterBuffers.reduce((sum, b) => sum + b.length, 0)
  const result = new Uint8Array(totalLength)

  let pos = 0
  result.set(headerLengthBytes, pos)
  pos += 4
  result.set(headerBytes, pos)
  pos += headerBytes.length
  for (const buf of filterBuffers) {
    result.set(buf, pos)
    pos += buf.length
  }

  return result
}

/**
 * Deserialize bloom filter index from bytes
 */
export function deserializeBloomFilterIndex(data: Uint8Array): BloomFilterIndex {
  // Read header length
  const headerLength = new DataView(data.buffer, data.byteOffset, 4).getUint32(0, true)

  // Read header JSON
  const headerBytes = data.slice(4, 4 + headerLength)
  const headerJson = new TextDecoder().decode(headerBytes)
  let header: {
    version: number
    metadata: {
      createdAt: string
      rowCount: number
      fields: string[]
    }
    filterOffsets: Record<string, { offset: number; length: number }>
  }
  try {
    header = JSON.parse(headerJson) as typeof header
  } catch {
    throw new Error('Invalid bloom filter index: not valid JSON header')
  }

  // Read filters
  const filterDataStart = 4 + headerLength
  const filters = new Map<string, BloomFilter>()

  for (const [field, { offset, length }] of Object.entries(header.filterOffsets)) {
    const filterData = data.slice(filterDataStart + offset, filterDataStart + offset + length)
    filters.set(field, deserializeBloomFilter(filterData))
  }

  return {
    version: header.version,
    filters,
    metadata: {
      createdAt: new Date(header.metadata.createdAt),
      rowCount: header.metadata.rowCount,
      fields: header.metadata.fields,
    },
  }
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Convert a value to bytes for hashing
 */
function valueToBytes(value: unknown): Uint8Array {
  if (value === null || value === undefined) {
    return new Uint8Array([0])
  }

  if (typeof value === 'string') {
    return new TextEncoder().encode(value)
  }

  if (typeof value === 'number') {
    const buffer = new ArrayBuffer(8)
    new DataView(buffer).setFloat64(0, value, true)
    return new Uint8Array(buffer)
  }

  if (typeof value === 'boolean') {
    return new Uint8Array([value ? 1 : 0])
  }

  if (value instanceof Date) {
    const buffer = new ArrayBuffer(8)
    new DataView(buffer).setBigInt64(0, BigInt(value.getTime()), true)
    return new Uint8Array(buffer)
  }

  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength)
  }

  // Convert objects/arrays to JSON
  return new TextEncoder().encode(JSON.stringify(value))
}

/**
 * Calculate hash values using double hashing technique
 * h(i) = h1 + i * h2 (mod m)
 *
 * Uses simple hash functions - in production, use xxHash or murmur3
 */
function calculateHashes(data: Uint8Array, numHashes: number, numBits: number): number[] {
  const h1 = simpleHash(data, 0x9747b28c)
  const h2 = simpleHash(data, 0xc4ceb9fe)

  const hashes: number[] = []
  for (let i = 0; i < numHashes; i++) {
    // Ensure positive modulo
    const hash = (((h1 + i * h2) % numBits) + numBits) % numBits
    hashes.push(hash)
  }

  return hashes
}

/**
 * Simple hash function (FNV-1a variant)
 */
function simpleHash(data: Uint8Array, seed: number): number {
  let hash = seed

  for (let i = 0; i < data.length; i++) {
    hash ^= data[i]!
    hash = Math.imul(hash, 0x01000193)
  }

  // Ensure 32-bit positive integer
  return hash >>> 0
}

/**
 * Count set bits in a byte (population count)
 */
function popcount(b: number): number {
  let count = 0
  let byte = b
  while (byte) {
    count += byte & 1
    byte >>= 1
  }
  return count
}

/**
 * Get nested value from object using dot notation
 */
function getNestedValue(obj: Record<string, unknown>, path: string): unknown {
  const parts = path.split('.')
  let current: unknown = obj

  for (const part of parts) {
    if (current === null || current === undefined) return undefined
    if (typeof current !== 'object') return undefined
    current = (current as Record<string, unknown>)[part]
  }

  return current
}

/**
 * Parse bloom filter from Parquet bloom filter format
 * This handles the multi-field bloom filter file format
 */
function parseBloomFilter(data: Uint8Array): BloomFilterIndex {
  return deserializeBloomFilterIndex(data)
}

/**
 * Get bloom filter for a specific field
 */
function getFieldBloomFilter(index: BloomFilterIndex, field: string): BloomFilter | null {
  return index.filters.get(field) ?? null
}
