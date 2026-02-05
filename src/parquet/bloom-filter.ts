/**
 * Parquet Split Block Bloom Filter Implementation
 *
 * Implements the Split Block Bloom Filter (SBBF) format as specified in the
 * Apache Parquet format specification. Uses xxHash64 for hashing values.
 *
 * @see https://parquet.apache.org/docs/file-format/bloomfilter/
 * @see https://github.com/apache/parquet-format/blob/master/BloomFilter.md
 */

import type { BloomFilterReader } from '../query/executor'

// =============================================================================
// Constants
// =============================================================================

/**
 * SALT constants used by the Split Block Bloom Filter.
 * These are 8 odd unsigned 32-bit integers used for bit masking.
 */
const SALT = new Uint32Array([
  0x47b6137b,
  0x44974d91,
  0x8824ad5b,
  0xa2b7289d,
  0x705495c7,
  0x2df1424b,
  0x9efc4947,
  0x5c6bfb31,
])

/**
 * Block size in bytes (256 bits = 32 bytes)
 */
const BLOCK_SIZE_BYTES = 32

/**
 * Number of 32-bit words per block
 */
const WORDS_PER_BLOCK = 8

// =============================================================================
// xxHash64 Implementation
// =============================================================================

/**
 * xxHash64 prime constants
 */
const PRIME64_1 = 0x9e3779b185ebca87n
const PRIME64_2 = 0xc2b2ae3d27d4eb4fn
const PRIME64_3 = 0x165667b19e3779f9n
const PRIME64_4 = 0x85ebca77c2b2ae63n
const PRIME64_5 = 0x27d4eb2f165667c5n

/**
 * Rotate left for 64-bit BigInt
 */
function rotl64(x: bigint, r: number): bigint {
  return ((x << BigInt(r)) | (x >> BigInt(64 - r))) & 0xffffffffffffffffn
}

/**
 * Read a 64-bit little-endian value from a Uint8Array
 */
function readLE64(data: Uint8Array, offset: number): bigint {
  let result = 0n
  for (let i = 0; i < 8; i++) {
    result |= BigInt(data[offset + i] ?? 0) << BigInt(i * 8)
  }
  return result
}

/**
 * Read a 32-bit little-endian value from a Uint8Array
 */
function readLE32(data: Uint8Array, offset: number): bigint {
  let result = 0n
  for (let i = 0; i < 4; i++) {
    result |= BigInt(data[offset + i] ?? 0) << BigInt(i * 8)
  }
  return result
}

/**
 * xxHash64 round function
 */
function xxh64Round(acc: bigint, input: bigint): bigint {
  acc = (acc + input * PRIME64_2) & 0xffffffffffffffffn
  acc = rotl64(acc, 31)
  acc = (acc * PRIME64_1) & 0xffffffffffffffffn
  return acc
}

/**
 * xxHash64 merge round function
 */
function xxh64MergeRound(acc: bigint, val: bigint): bigint {
  val = xxh64Round(0n, val)
  acc = (acc ^ val) & 0xffffffffffffffffn
  acc = (acc * PRIME64_1 + PRIME64_4) & 0xffffffffffffffffn
  return acc
}

/**
 * xxHash64 avalanche function (finalization)
 */
function xxh64Avalanche(h64: bigint): bigint {
  h64 = (h64 ^ (h64 >> 33n)) & 0xffffffffffffffffn
  h64 = (h64 * PRIME64_2) & 0xffffffffffffffffn
  h64 = (h64 ^ (h64 >> 29n)) & 0xffffffffffffffffn
  h64 = (h64 * PRIME64_3) & 0xffffffffffffffffn
  h64 = (h64 ^ (h64 >> 32n)) & 0xffffffffffffffffn
  return h64
}

/**
 * Compute xxHash64 of a byte array with seed 0.
 * Follows xxHash specification version 0.1.1.
 *
 * @param data - Input bytes to hash
 * @returns 64-bit hash as BigInt
 */
export function xxHash64(data: Uint8Array): bigint {
  const len = data.length
  let h64: bigint

  if (len >= 32) {
    // Initialize accumulators
    let v1 = (0n + PRIME64_1 + PRIME64_2) & 0xffffffffffffffffn
    let v2 = PRIME64_2
    let v3 = 0n
    let v4 = (0n - PRIME64_1) & 0xffffffffffffffffn

    // Process 32-byte chunks
    let offset = 0
    const limit = len - 32

    while (offset <= limit) {
      v1 = xxh64Round(v1, readLE64(data, offset))
      offset += 8
      v2 = xxh64Round(v2, readLE64(data, offset))
      offset += 8
      v3 = xxh64Round(v3, readLE64(data, offset))
      offset += 8
      v4 = xxh64Round(v4, readLE64(data, offset))
      offset += 8
    }

    // Merge accumulators
    h64 = rotl64(v1, 1) + rotl64(v2, 7) + rotl64(v3, 12) + rotl64(v4, 18)
    h64 = h64 & 0xffffffffffffffffn
    h64 = xxh64MergeRound(h64, v1)
    h64 = xxh64MergeRound(h64, v2)
    h64 = xxh64MergeRound(h64, v3)
    h64 = xxh64MergeRound(h64, v4)

    // Process remaining bytes
    h64 = (h64 + BigInt(len)) & 0xffffffffffffffffn

    while (offset + 8 <= len) {
      const k1 = xxh64Round(0n, readLE64(data, offset))
      h64 = (h64 ^ k1) & 0xffffffffffffffffn
      h64 = (rotl64(h64, 27) * PRIME64_1 + PRIME64_4) & 0xffffffffffffffffn
      offset += 8
    }

    while (offset + 4 <= len) {
      h64 = (h64 ^ (readLE32(data, offset) * PRIME64_1)) & 0xffffffffffffffffn
      h64 = (rotl64(h64, 23) * PRIME64_2 + PRIME64_3) & 0xffffffffffffffffn
      offset += 4
    }

    while (offset < len) {
      h64 = (h64 ^ (BigInt(data[offset]!) * PRIME64_5)) & 0xffffffffffffffffn
      h64 = (rotl64(h64, 11) * PRIME64_1) & 0xffffffffffffffffn
      offset++
    }
  } else {
    // Small input (< 32 bytes)
    h64 = (PRIME64_5 + BigInt(len)) & 0xffffffffffffffffn

    let offset = 0

    while (offset + 8 <= len) {
      const k1 = xxh64Round(0n, readLE64(data, offset))
      h64 = (h64 ^ k1) & 0xffffffffffffffffn
      h64 = (rotl64(h64, 27) * PRIME64_1 + PRIME64_4) & 0xffffffffffffffffn
      offset += 8
    }

    while (offset + 4 <= len) {
      h64 = (h64 ^ (readLE32(data, offset) * PRIME64_1)) & 0xffffffffffffffffn
      h64 = (rotl64(h64, 23) * PRIME64_2 + PRIME64_3) & 0xffffffffffffffffn
      offset += 4
    }

    while (offset < len) {
      h64 = (h64 ^ (BigInt(data[offset]!) * PRIME64_5)) & 0xffffffffffffffffn
      h64 = (rotl64(h64, 11) * PRIME64_1) & 0xffffffffffffffffn
      offset++
    }
  }

  return xxh64Avalanche(h64)
}

// =============================================================================
// Split Block Bloom Filter
// =============================================================================

/**
 * Compute the mask for a block insert/check operation.
 * For each of the 8 words, this determines which bit to set/check.
 *
 * @param h - 32-bit hash value (lower 32 bits of xxHash64 result)
 * @returns Array of 8 bit masks, one per word
 */
function blockMask(h: number): Uint32Array {
  const mask = new Uint32Array(WORDS_PER_BLOCK)

  for (let i = 0; i < WORDS_PER_BLOCK; i++) {
    // Multiply h by SALT[i] and take lower 32 bits
    // Then right-shift by 27 to get bit index (0-31)
    // Use Math.imul for 32-bit multiplication
    const product = Math.imul(h, SALT[i]!) >>> 0
    const bitIndex = product >>> 27
    mask[i] = 1 << bitIndex
  }

  return mask
}

/**
 * Check if a value might be in a specific block.
 *
 * @param block - 256-bit block (8 x 32-bit words)
 * @param h - 32-bit hash value
 * @returns true if value might exist, false if definitely not present
 */
function blockCheck(block: Uint32Array, h: number): boolean {
  const mask = blockMask(h)

  for (let i = 0; i < WORDS_PER_BLOCK; i++) {
    // Check if the required bit is set in the block
    if ((block[i]! & mask[i]!) === 0) {
      return false // Definitely not present
    }
  }

  return true // Might be present
}

/**
 * Parquet Split Block Bloom Filter reader.
 *
 * Implements the BloomFilterReader interface for reading Parquet bloom filters.
 * The filter data is stored as raw bytes containing multiple 256-bit blocks.
 */
export class ParquetBloomFilter implements BloomFilterReader {
  private blocks: Uint32Array
  private numBlocks: number

  /**
   * Create a ParquetBloomFilter from raw filter bytes.
   *
   * @param data - Raw bloom filter data (multiple of 32 bytes)
   */
  constructor(data: Uint8Array) {
    // Validate size is multiple of block size
    if (data.byteLength % BLOCK_SIZE_BYTES !== 0) {
      throw new Error(
        `Invalid bloom filter size: ${data.byteLength} bytes (must be multiple of ${BLOCK_SIZE_BYTES})`
      )
    }

    this.numBlocks = data.byteLength / BLOCK_SIZE_BYTES

    // Convert to Uint32Array for efficient word access
    // Need to ensure proper alignment by copying to a new buffer
    const buffer = new ArrayBuffer(data.byteLength)
    new Uint8Array(buffer).set(data)
    this.blocks = new Uint32Array(buffer)
  }

  /**
   * Check if a value might exist in the bloom filter.
   *
   * @param value - Value to check (will be hashed with xxHash64)
   * @returns false if value is definitely not present, true if it might be present
   */
  mightContain(value: unknown): boolean {
    // Convert value to bytes
    const bytes = this.valueToBytes(value)

    // Compute xxHash64
    const hash = xxHash64(bytes)

    // Split hash into upper and lower 32 bits
    const upper = Number((hash >> 32n) & 0xffffffffn)
    const lower = Number(hash & 0xffffffffn)

    // Select block using upper 32 bits
    // blockIndex = (upper * numBlocks) >> 32
    // Using BigInt multiplication to avoid overflow
    const blockIndex = Number(
      (BigInt(upper) * BigInt(this.numBlocks)) >> 32n
    )

    // Get the block (8 words starting at blockIndex * 8)
    const blockOffset = blockIndex * WORDS_PER_BLOCK
    const block = this.blocks.subarray(blockOffset, blockOffset + WORDS_PER_BLOCK)

    // Check if value might be in this block
    return blockCheck(block, lower)
  }

  /**
   * Get the number of blocks in this filter
   */
  get blockCount(): number {
    return this.numBlocks
  }

  /**
   * Get the size of the filter in bytes
   */
  get sizeBytes(): number {
    return this.numBlocks * BLOCK_SIZE_BYTES
  }

  /**
   * Convert a value to bytes for hashing.
   * Follows Parquet type-specific serialization rules.
   */
  private valueToBytes(value: unknown): Uint8Array {
    if (value === null || value === undefined) {
      return new Uint8Array(0)
    }

    if (typeof value === 'string') {
      return new TextEncoder().encode(value)
    }

    if (typeof value === 'number') {
      // For floating point numbers, use IEEE 754 double representation
      if (!Number.isInteger(value)) {
        const buffer = new ArrayBuffer(8)
        new DataView(buffer).setFloat64(0, value, true) // little-endian
        return new Uint8Array(buffer)
      }

      // For integers, use the minimum bytes needed (little-endian)
      // Parquet typically uses INT32 or INT64
      if (value >= -2147483648 && value <= 2147483647) {
        const buffer = new ArrayBuffer(4)
        new DataView(buffer).setInt32(0, value, true)
        return new Uint8Array(buffer)
      }

      // Large integers: use 64-bit
      const buffer = new ArrayBuffer(8)
      const view = new DataView(buffer)
      // Handle as two 32-bit parts for large numbers
      const low = value & 0xffffffff
      const high = Math.floor(value / 0x100000000) & 0xffffffff
      view.setUint32(0, low >>> 0, true)
      view.setInt32(4, high, true)
      return new Uint8Array(buffer)
    }

    if (typeof value === 'bigint') {
      // 64-bit integer (little-endian)
      const buffer = new ArrayBuffer(8)
      const view = new DataView(buffer)
      view.setBigInt64(0, value, true)
      return new Uint8Array(buffer)
    }

    if (typeof value === 'boolean') {
      return new Uint8Array([value ? 1 : 0])
    }

    if (value instanceof Date) {
      // Use milliseconds timestamp as 64-bit integer
      const buffer = new ArrayBuffer(8)
      new DataView(buffer).setBigInt64(0, BigInt(value.getTime()), true)
      return new Uint8Array(buffer)
    }

    if (ArrayBuffer.isView(value)) {
      return new Uint8Array(value.buffer, value.byteOffset, value.byteLength)
    }

    if (value instanceof ArrayBuffer) {
      return new Uint8Array(value)
    }

    // For objects/arrays, serialize as JSON string
    return new TextEncoder().encode(JSON.stringify(value))
  }
}

// =============================================================================
// Bloom Filter Header Parsing
// =============================================================================

/**
 * Bloom filter algorithm types
 */
export type BloomFilterAlgorithm = 'SPLIT_BLOCK'

/**
 * Bloom filter hash function types
 */
export type BloomFilterHash = 'XXHASH'

/**
 * Bloom filter compression types
 */
export type BloomFilterCompression = 'UNCOMPRESSED'

/**
 * Parsed bloom filter header from Parquet file
 */
export interface BloomFilterHeader {
  /** Size of the bloom filter bitset in bytes */
  numBytes: number
  /** Algorithm used (currently only SPLIT_BLOCK) */
  algorithm: BloomFilterAlgorithm
  /** Hash function used (currently only XXHASH) */
  hash: BloomFilterHash
  /** Compression used (currently only UNCOMPRESSED) */
  compression: BloomFilterCompression
}

/**
 * Parse a bloom filter header from Thrift-encoded bytes.
 *
 * The header structure in Thrift is:
 * - field 1: numBytes (i32)
 * - field 2: algorithm (struct)
 * - field 3: hash (struct)
 * - field 4: compression (struct)
 *
 * @param data - Raw bytes containing the Thrift-encoded header
 * @returns Parsed header and offset to data
 */
export function parseBloomFilterHeader(
  data: Uint8Array
): { header: BloomFilterHeader; dataOffset: number } {
  let offset = 0
  let numBytes = 0
  let algorithm: BloomFilterAlgorithm = 'SPLIT_BLOCK'
  let hash: BloomFilterHash = 'XXHASH'
  let compression: BloomFilterCompression = 'UNCOMPRESSED'

  // Simple Thrift compact protocol parsing
  while (offset < data.length) {
    const fieldHeader = data[offset++]
    if (fieldHeader === undefined) break

    // Check for STOP (0x00)
    if (fieldHeader === 0) break

    const fieldType = fieldHeader & 0x0f
    const fieldDelta = (fieldHeader >> 4) & 0x0f

    // If field delta is 0, read field ID as varint
    let fieldId: number
    if (fieldDelta === 0) {
      // Read zigzag-encoded varint for field ID
      const { value, bytesRead } = readVarint(data, offset)
      fieldId = zigzagDecode(value)
      offset += bytesRead
    } else {
      fieldId = fieldDelta // Field ID is the delta for small IDs
    }

    // Parse based on field type and ID
    switch (fieldId) {
      case 1: // numBytes (i32)
        if (fieldType === 5) {
          // i32 type
          const { value, bytesRead } = readVarint(data, offset)
          numBytes = zigzagDecode(value)
          offset += bytesRead
        }
        break

      case 2: // algorithm (struct with single field)
      case 3: // hash (struct with single field)
      case 4: // compression (struct with single field)
        // These are union structs with a single empty struct inside
        // Skip the struct content (read until STOP byte)
        if (fieldType === 12) {
          // struct type
          offset = skipStruct(data, offset)
        }
        break

      default:
        // Skip unknown fields
        offset = skipField(data, offset, fieldType)
    }
  }

  return {
    header: {
      numBytes,
      algorithm,
      hash,
      compression,
    },
    dataOffset: offset,
  }
}

/**
 * Read a varint from the data
 */
function readVarint(data: Uint8Array, offset: number): { value: number; bytesRead: number } {
  let result = 0
  let shift = 0
  let bytesRead = 0

  while (offset + bytesRead < data.length) {
    const byte = data[offset + bytesRead]!
    bytesRead++

    result |= (byte & 0x7f) << shift
    shift += 7

    if ((byte & 0x80) === 0) {
      break
    }
  }

  return { value: result, bytesRead }
}

/**
 * Decode a zigzag-encoded integer
 */
function zigzagDecode(n: number): number {
  return (n >>> 1) ^ -(n & 1)
}

/**
 * Skip a Thrift struct (read until STOP byte)
 */
function skipStruct(data: Uint8Array, offset: number): number {
  while (offset < data.length) {
    const fieldHeader = data[offset++]
    if (fieldHeader === undefined || fieldHeader === 0) break

    const fieldType = fieldHeader & 0x0f
    const fieldDelta = (fieldHeader >> 4) & 0x0f

    // If field delta is 0, read field ID
    if (fieldDelta === 0) {
      const { bytesRead } = readVarint(data, offset)
      offset += bytesRead
    }

    // Skip the field value
    offset = skipField(data, offset, fieldType)
  }

  return offset
}

/**
 * Skip a Thrift field based on its type
 */
function skipField(data: Uint8Array, offset: number, fieldType: number): number {
  switch (fieldType) {
    case 1: // BOOL_TRUE
    case 2: // BOOL_FALSE
      return offset

    case 3: // BYTE
      return offset + 1

    case 4: // I16
    case 5: // I32
    case 6: // I64
    case 8: // DOUBLE
      // Read varint
      const { bytesRead } = readVarint(data, offset)
      return offset + bytesRead

    case 7: // BINARY/STRING
      // Length-prefixed
      const { value: len, bytesRead: lenBytes } = readVarint(data, offset)
      return offset + lenBytes + len

    case 9: // LIST
    case 10: // SET
      // element type (1 byte) + size varint + elements
      const listHeader = data[offset++]
      const elemType = listHeader !== undefined ? listHeader & 0x0f : 0
      const sizeInHeader = listHeader !== undefined ? (listHeader >> 4) & 0x0f : 0
      let listSize = sizeInHeader
      if (sizeInHeader === 15) {
        const { value, bytesRead: sb } = readVarint(data, offset)
        listSize = value
        offset += sb
      }
      for (let i = 0; i < listSize; i++) {
        offset = skipField(data, offset, elemType)
      }
      return offset

    case 11: // MAP
      // key type, value type, size, entries
      const mapHeader = data[offset++]
      if (mapHeader === 0) return offset
      const { value: mapSize, bytesRead: msb } = readVarint(data, offset)
      offset += msb
      const keyType = mapHeader !== undefined ? (mapHeader >> 4) & 0x0f : 0
      const valType = mapHeader !== undefined ? mapHeader & 0x0f : 0
      for (let i = 0; i < mapSize; i++) {
        offset = skipField(data, offset, keyType)
        offset = skipField(data, offset, valType)
      }
      return offset

    case 12: // STRUCT
      return skipStruct(data, offset)

    default:
      return offset
  }
}

// =============================================================================
// Exports
// =============================================================================

export type { BloomFilterReader }
