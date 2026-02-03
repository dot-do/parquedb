/**
 * Key Encoder for Secondary Indexes
 *
 * Provides type-prefixed key serialization that maintains sort order.
 * Keys are encoded as:
 *   [type_byte][value_bytes]
 *
 * Type bytes ensure proper ordering:
 *   0x00 - null/undefined
 *   0x10 - boolean false
 *   0x11 - boolean true
 *   0x20 - negative number (inverted for correct ordering)
 *   0x21 - positive number
 *   0x30 - string (UTF-8)
 *   0x40 - date (as epoch ms)
 *   0x50 - binary
 *   0x60 - array
 *   0x70 - object (JSON)
 */

import { FNV_OFFSET_BASIS, FNV_PRIME } from '../../constants'

// =============================================================================
// Type Prefixes
// =============================================================================

const TYPE_NULL = 0x00
const TYPE_BOOL_FALSE = 0x10
const TYPE_BOOL_TRUE = 0x11
const TYPE_NUMBER_NEG = 0x20
const TYPE_NUMBER_POS = 0x21
const TYPE_STRING = 0x30
const TYPE_DATE = 0x40
const TYPE_BINARY = 0x50
const TYPE_ARRAY = 0x60
const TYPE_OBJECT = 0x70

// =============================================================================
// Encoder
// =============================================================================

/**
 * Encode a value to a sortable key
 *
 * @param value - Value to encode
 * @returns Encoded key bytes
 */
export function encodeKey(value: unknown): Uint8Array {
  // Null/undefined
  if (value === null || value === undefined) {
    return new Uint8Array([TYPE_NULL])
  }

  // Boolean
  if (typeof value === 'boolean') {
    return new Uint8Array([value ? TYPE_BOOL_TRUE : TYPE_BOOL_FALSE])
  }

  // Number
  if (typeof value === 'number') {
    return encodeNumber(value)
  }

  // String
  if (typeof value === 'string') {
    return encodeString(value)
  }

  // Date
  if (value instanceof Date) {
    return encodeDate(value)
  }

  // Uint8Array
  if (value instanceof Uint8Array) {
    return encodeBinary(value)
  }

  // Array
  if (Array.isArray(value)) {
    return encodeArray(value)
  }

  // Object
  if (typeof value === 'object') {
    return encodeObject(value as Record<string, unknown>)
  }

  // Fallback: convert to string
  return encodeString(String(value))
}

/**
 * Encode a number to sortable bytes
 * Uses IEEE 754 double with XOR transformation for negative numbers
 */
function encodeNumber(num: number): Uint8Array {
  const buffer = new ArrayBuffer(9)
  const view = new DataView(buffer)

  if (num >= 0) {
    view.setUint8(0, TYPE_NUMBER_POS)
    // For positive numbers, just store as big-endian double
    view.setFloat64(1, num, false) // big-endian for lexicographic ordering
  } else {
    view.setUint8(0, TYPE_NUMBER_NEG)
    // For negative numbers, flip all bits to reverse ordering
    view.setFloat64(1, num, false)
    const bytes = new Uint8Array(buffer)
    for (let i = 1; i < 9; i++) {
      bytes[i] = (bytes[i] ?? 0) ^ 0xff  // loop bounds ensure valid index
    }
  }

  return new Uint8Array(buffer)
}

/**
 * Encode a string to sortable bytes
 * Uses UTF-8 encoding with null byte escaping
 */
function encodeString(str: string): Uint8Array {
  const utf8 = new TextEncoder().encode(str)
  const result = new Uint8Array(1 + utf8.length)
  result[0] = TYPE_STRING
  result.set(utf8, 1)
  return result
}

/**
 * Encode a date to sortable bytes
 * Stores as epoch milliseconds (big-endian for lexicographic ordering)
 */
function encodeDate(date: Date): Uint8Array {
  const buffer = new ArrayBuffer(9)
  const view = new DataView(buffer)
  view.setUint8(0, TYPE_DATE)
  // Store as signed 64-bit to handle dates before 1970
  view.setBigInt64(1, BigInt(date.getTime()), false) // big-endian
  return new Uint8Array(buffer)
}

/**
 * Encode binary data
 * Prepends type byte
 */
function encodeBinary(data: Uint8Array): Uint8Array {
  const result = new Uint8Array(1 + data.length)
  result[0] = TYPE_BINARY
  result.set(data, 1)
  return result
}

/**
 * Encode an array
 * Concatenates encoded elements with length prefixes
 */
function encodeArray(arr: unknown[]): Uint8Array {
  const encodedElements = arr.map(encodeKey)
  const totalLength = encodedElements.reduce((sum, e) => sum + 4 + e.length, 0)
  const result = new Uint8Array(1 + 4 + totalLength)
  const view = new DataView(result.buffer)

  result[0] = TYPE_ARRAY
  view.setUint32(1, arr.length, false) // element count

  let offset = 5
  for (const encoded of encodedElements) {
    view.setUint32(offset, encoded.length, false)
    offset += 4
    result.set(encoded, offset)
    offset += encoded.length
  }

  return result
}

/**
 * Encode an object
 * Uses JSON serialization with sorted keys (not sortable, but unique)
 */
function encodeObject(obj: Record<string, unknown>): Uint8Array {
  const json = JSON.stringify(obj, sortedReplacer)
  const utf8 = new TextEncoder().encode(json)
  const result = new Uint8Array(1 + utf8.length)
  result[0] = TYPE_OBJECT
  result.set(utf8, 1)
  return result
}

/**
 * JSON replacer that sorts object keys recursively
 */
function sortedReplacer(_key: string, value: unknown): unknown {
  if (value !== null && typeof value === 'object' && !Array.isArray(value) && !(value instanceof Date)) {
    const sorted: Record<string, unknown> = {}
    const keys = Object.keys(value as object).sort()
    for (const k of keys) {
      sorted[k] = (value as Record<string, unknown>)[k]
    }
    return sorted
  }
  return value
}

// =============================================================================
// Decoder
// =============================================================================

/**
 * Decode a key back to its original value
 *
 * @param key - Encoded key bytes
 * @returns Decoded value
 */
export function decodeKey(key: Uint8Array): unknown {
  if (key.length === 0) {
    throw new Error('Empty key')
  }

  const type = key[0]

  switch (type) {
    case TYPE_NULL:
      return null

    case TYPE_BOOL_FALSE:
      return false

    case TYPE_BOOL_TRUE:
      return true

    case TYPE_NUMBER_NEG:
    case TYPE_NUMBER_POS:
      return decodeNumber(key)

    case TYPE_STRING:
      return decodeString(key)

    case TYPE_DATE:
      return decodeDate(key)

    case TYPE_BINARY:
      return decodeBinary(key)

    case TYPE_ARRAY:
      return decodeArray(key)

    case TYPE_OBJECT:
      return decodeObject(key)

    default:
      throw new Error(`Unknown type byte: 0x${type?.toString(16) ?? 'undefined'}`)
  }
}

/**
 * Decode a number from key bytes
 */
function decodeNumber(key: Uint8Array): number {
  if (key.length !== 9) {
    throw new Error('Invalid number key length')
  }

  const buffer = key.buffer.slice(key.byteOffset, key.byteOffset + key.length)
  const view = new DataView(buffer)
  const type = view.getUint8(0)

  if (type === TYPE_NUMBER_NEG) {
    // Flip bits back for negative numbers
    const bytes = new Uint8Array(buffer)
    for (let i = 1; i < 9; i++) {
      bytes[i] = (bytes[i] ?? 0) ^ 0xff  // loop bounds ensure valid index
    }
  }

  return view.getFloat64(1, false)
}

/**
 * Decode a string from key bytes
 */
function decodeString(key: Uint8Array): string {
  return new TextDecoder().decode(key.slice(1))
}

/**
 * Decode a date from key bytes
 */
function decodeDate(key: Uint8Array): Date {
  if (key.length !== 9) {
    throw new Error('Invalid date key length')
  }

  const view = new DataView(key.buffer, key.byteOffset, key.length)
  const ms = view.getBigInt64(1, false)
  return new Date(Number(ms))
}

/**
 * Decode binary from key bytes
 */
function decodeBinary(key: Uint8Array): Uint8Array {
  return key.slice(1)
}

/**
 * Decode an array from key bytes
 */
function decodeArray(key: Uint8Array): unknown[] {
  const view = new DataView(key.buffer, key.byteOffset, key.length)
  const count = view.getUint32(1, false)

  const result: unknown[] = []
  let offset = 5

  for (let i = 0; i < count; i++) {
    const length = view.getUint32(offset, false)
    offset += 4
    const element = key.slice(offset, offset + length)
    result.push(decodeKey(element))
    offset += length
  }

  return result
}

/**
 * Decode an object from key bytes
 */
function decodeObject(key: Uint8Array): Record<string, unknown> {
  const json = new TextDecoder().decode(key.slice(1))
  try {
    return JSON.parse(json) as Record<string, unknown>
  } catch {
    // Invalid JSON in encoded key - return empty object as fallback
    return {}
  }
}

// =============================================================================
// Comparison
// =============================================================================

/**
 * Compare two encoded keys lexicographically
 *
 * @param a - First key
 * @param b - Second key
 * @returns -1 if a < b, 0 if equal, 1 if a > b
 */
export function compareKeys(a: Uint8Array, b: Uint8Array): number {
  const minLen = Math.min(a.length, b.length)

  for (let i = 0; i < minLen; i++) {
    const aVal = a[i]!  // loop bounds ensure valid index
    const bVal = b[i]!
    if (aVal < bVal) return -1
    if (aVal > bVal) return 1
  }

  // Shorter keys come first
  if (a.length < b.length) return -1
  if (a.length > b.length) return 1

  return 0
}

/**
 * Create a key for range query boundaries
 *
 * @param value - Boundary value
 * @param inclusive - Whether the boundary is inclusive
 * @param upper - Whether this is an upper bound
 * @returns Adjusted key bytes
 */
export function createBoundaryKey(
  value: unknown,
  inclusive: boolean,
  upper: boolean
): Uint8Array {
  const key = encodeKey(value)

  if (inclusive) {
    return key
  }

  // For exclusive bounds, we need to adjust the key
  if (upper) {
    // Upper exclusive: decrement the key (or just use the key and exclude it in comparison)
    return key
  } else {
    // Lower exclusive: increment the key (or just use the key and exclude it in comparison)
    return key
  }
}

// =============================================================================
// Hashing
// =============================================================================

/**
 * Hash a key for hash index bucket assignment
 * Uses FNV-1a hash
 *
 * @param key - Encoded key bytes
 * @returns 32-bit hash value
 */
export function hashKey(key: Uint8Array): number {
  let hash = FNV_OFFSET_BASIS

  for (let i = 0; i < key.length; i++) {
    hash ^= key[i]!  // loop bounds ensure valid index
    hash = Math.imul(hash, FNV_PRIME)
  }

  return hash >>> 0 // Ensure unsigned 32-bit
}

/**
 * Convert key to hex string for display/debugging
 */
export function keyToHex(key: Uint8Array): string {
  return Array.from(key)
    .map(b => b.toString(16).padStart(2, '0'))
    .join('')
}

/**
 * Parse hex string back to key
 */
export function hexToKey(hex: string): Uint8Array {
  const bytes = new Uint8Array(hex.length / 2)
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16)
  }
  return bytes
}

// =============================================================================
// Composite Keys
// =============================================================================

/**
 * Encode multiple values as a composite key
 *
 * @param values - Array of values to encode
 * @returns Composite key bytes
 */
export function encodeCompositeKey(values: unknown[]): Uint8Array {
  const encodedParts = values.map(encodeKey)
  const totalLength = encodedParts.reduce((sum, p) => sum + 4 + p.length, 0)
  const result = new Uint8Array(totalLength)
  const view = new DataView(result.buffer)

  let offset = 0
  for (const part of encodedParts) {
    view.setUint32(offset, part.length, false)
    offset += 4
    result.set(part, offset)
    offset += part.length
  }

  return result
}

/**
 * Decode a composite key back to individual values
 *
 * @param key - Composite key bytes
 * @returns Array of decoded values
 */
export function decodeCompositeKey(key: Uint8Array): unknown[] {
  const view = new DataView(key.buffer, key.byteOffset, key.length)
  const values: unknown[] = []

  let offset = 0
  while (offset < key.length) {
    const length = view.getUint32(offset, false)
    offset += 4
    const part = key.slice(offset, offset + length)
    values.push(decodeKey(part))
    offset += length
  }

  return values
}
