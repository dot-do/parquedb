/**
 * Bloom Filter Implementation for ParqueDB
 *
 * A space-efficient probabilistic data structure that provides quick existence checks
 * with a configurable false positive rate. This implementation uses MurmurHash3 for
 * optimal hash distribution.
 *
 * File format (.bloom):
 *   Header (16 bytes):
 *     - magic: "PQBF" (4 bytes)
 *     - version: 1 (2 bytes)
 *     - numHashFunctions: 3 (2 bytes)
 *     - filterSize: bytes (4 bytes)
 *     - numRowGroups: (2 bytes)
 *     - reserved: (2 bytes)
 *   Value Bloom Filter (filterSize bytes):
 *     - Bit array for value existence
 *   Row Group Bloom Filters (numRowGroups * 4096 bytes):
 *     - One 4KB bloom filter per row group
 */

// =============================================================================
// Constants
// =============================================================================

const MAGIC = new Uint8Array([0x50, 0x51, 0x42, 0x46]) // "PQBF"
const VERSION = 1
const HEADER_SIZE = 16
const ROW_GROUP_BLOOM_SIZE = 4096 // 4KB per row group

// Default parameters optimized for ~1% false positive rate
const DEFAULT_NUM_HASH_FUNCTIONS = 3
const DEFAULT_VALUE_BLOOM_SIZE = 131072 // 128KB

// =============================================================================
// MurmurHash3 Implementation
// =============================================================================

/**
 * MurmurHash3 32-bit implementation
 * Provides excellent distribution for bloom filter hashing
 */
function murmurHash3(key: Uint8Array, seed: number): number {
  const c1 = 0xcc9e2d51
  const c2 = 0x1b873593
  const r1 = 15
  const r2 = 13
  const m = 5
  const n = 0xe6546b64

  let hash = seed
  const len = key.length
  const numBlocks = Math.floor(len / 4)

  // Process 4-byte blocks
  for (let i = 0; i < numBlocks; i++) {
    let k =
      key[i * 4] |
      (key[i * 4 + 1] << 8) |
      (key[i * 4 + 2] << 16) |
      (key[i * 4 + 3] << 24)

    k = Math.imul(k, c1)
    k = (k << r1) | (k >>> (32 - r1))
    k = Math.imul(k, c2)

    hash ^= k
    hash = (hash << r2) | (hash >>> (32 - r2))
    hash = Math.imul(hash, m) + n
  }

  // Process remaining bytes (intentional fallthrough pattern rewritten as if/else)
  const tail = len - numBlocks * 4
  let k1 = 0
  if (tail >= 3) {
    k1 ^= key[numBlocks * 4 + 2] << 16
  }
  if (tail >= 2) {
    k1 ^= key[numBlocks * 4 + 1] << 8
  }
  if (tail >= 1) {
    k1 ^= key[numBlocks * 4]
    k1 = Math.imul(k1, c1)
    k1 = (k1 << r1) | (k1 >>> (32 - r1))
    k1 = Math.imul(k1, c2)
    hash ^= k1
  }

  // Finalization
  hash ^= len
  hash ^= hash >>> 16
  hash = Math.imul(hash, 0x85ebca6b)
  hash ^= hash >>> 13
  hash = Math.imul(hash, 0xc2b2ae35)
  hash ^= hash >>> 16

  return hash >>> 0
}

/**
 * Generate multiple hash values using double hashing technique
 * h(i) = h1 + i * h2
 */
function getHashes(
  key: Uint8Array,
  numHashes: number,
  filterBits: number
): number[] {
  const h1 = murmurHash3(key, 0)
  const h2 = murmurHash3(key, h1)

  const hashes: number[] = []
  for (let i = 0; i < numHashes; i++) {
    // Use double hashing: h(i) = h1 + i * h2
    const hash = ((h1 + i * h2) >>> 0) % filterBits
    hashes.push(hash)
  }

  return hashes
}

// =============================================================================
// BloomFilter Class
// =============================================================================

/**
 * Basic bloom filter for value existence checks
 */
export class BloomFilter {
  private bits: Uint8Array
  private numHashFunctions: number
  private numBits: number

  constructor(sizeBytes: number, numHashFunctions: number = DEFAULT_NUM_HASH_FUNCTIONS) {
    this.bits = new Uint8Array(sizeBytes)
    this.numHashFunctions = numHashFunctions
    this.numBits = sizeBytes * 8
  }

  /**
   * Add a value to the filter
   */
  add(value: string | number): void {
    const key = this.valueToBytes(value)
    const hashes = getHashes(key, this.numHashFunctions, this.numBits)

    for (const hash of hashes) {
      const byteIndex = Math.floor(hash / 8)
      const bitIndex = hash % 8
      this.bits[byteIndex] |= 1 << bitIndex
    }
  }

  /**
   * Add raw bytes to the filter
   */
  addBytes(key: Uint8Array): void {
    const hashes = getHashes(key, this.numHashFunctions, this.numBits)

    for (const hash of hashes) {
      const byteIndex = Math.floor(hash / 8)
      const bitIndex = hash % 8
      this.bits[byteIndex] |= 1 << bitIndex
    }
  }

  /**
   * Check if a value might be in the filter
   * @returns true if the value might exist, false if it definitely does not
   */
  mightContain(value: string | number): boolean {
    const key = this.valueToBytes(value)
    const hashes = getHashes(key, this.numHashFunctions, this.numBits)

    for (const hash of hashes) {
      const byteIndex = Math.floor(hash / 8)
      const bitIndex = hash % 8
      if ((this.bits[byteIndex] & (1 << bitIndex)) === 0) {
        return false
      }
    }

    return true
  }

  /**
   * Check if raw bytes might be in the filter
   */
  mightContainBytes(key: Uint8Array): boolean {
    const hashes = getHashes(key, this.numHashFunctions, this.numBits)

    for (const hash of hashes) {
      const byteIndex = Math.floor(hash / 8)
      const bitIndex = hash % 8
      if ((this.bits[byteIndex] & (1 << bitIndex)) === 0) {
        return false
      }
    }

    return true
  }

  /**
   * Get the underlying bit array
   */
  toBuffer(): Uint8Array {
    return this.bits
  }

  /**
   * Create a bloom filter from an existing buffer
   */
  static fromBuffer(
    buffer: Uint8Array,
    numHashFunctions: number = DEFAULT_NUM_HASH_FUNCTIONS
  ): BloomFilter {
    const filter = new BloomFilter(buffer.length, numHashFunctions)
    filter.bits.set(buffer)
    return filter
  }

  /**
   * Get the size in bytes
   */
  get sizeBytes(): number {
    return this.bits.length
  }

  /**
   * Clear all bits
   */
  clear(): void {
    this.bits.fill(0)
  }

  /**
   * Estimate the number of items in the filter
   * Uses the formula: -m * ln(1 - X/m) / k
   * where m = bits, k = hash functions, X = bits set
   */
  estimateCount(): number {
    let bitsSet = 0
    for (let i = 0; i < this.bits.length; i++) {
      bitsSet += this.popCount(this.bits[i])
    }

    if (bitsSet === 0) return 0
    if (bitsSet >= this.numBits) return Infinity

    return Math.round(
      (-this.numBits * Math.log(1 - bitsSet / this.numBits)) /
        this.numHashFunctions
    )
  }

  /**
   * Get estimated false positive rate
   */
  estimateFPR(): number {
    const count = this.estimateCount()
    if (count === 0) return 0
    if (count === Infinity) return 1

    // FPR = (1 - e^(-kn/m))^k
    const k = this.numHashFunctions
    const m = this.numBits
    const exponent = -k * count / m
    return Math.pow(1 - Math.exp(exponent), k)
  }

  private valueToBytes(value: string | number): Uint8Array {
    if (typeof value === 'number') {
      const buffer = new ArrayBuffer(8)
      const view = new DataView(buffer)
      view.setFloat64(0, value, false)
      return new Uint8Array(buffer)
    }
    return new TextEncoder().encode(String(value))
  }

  private popCount(n: number): number {
    let count = 0
    while (n) {
      count += n & 1
      n >>>= 1
    }
    return count
  }
}

// =============================================================================
// IndexBloomFilter Class
// =============================================================================

/**
 * Two-level bloom filter for index pre-filtering:
 * 1. Value bloom: "Does value X exist anywhere in this index?"
 * 2. Row group blooms: "Which row groups might contain value X?"
 */
export class IndexBloomFilter {
  private valueBloom: BloomFilter
  private rowGroupBlooms: BloomFilter[]
  private numRowGroups: number

  constructor(
    numRowGroups: number,
    valueBloomSize: number = DEFAULT_VALUE_BLOOM_SIZE,
    numHashFunctions: number = DEFAULT_NUM_HASH_FUNCTIONS
  ) {
    this.numRowGroups = numRowGroups
    this.valueBloom = new BloomFilter(valueBloomSize, numHashFunctions)
    this.rowGroupBlooms = []

    for (let i = 0; i < numRowGroups; i++) {
      this.rowGroupBlooms.push(new BloomFilter(ROW_GROUP_BLOOM_SIZE, numHashFunctions))
    }
  }

  /**
   * Add an entry to both value and row group bloom filters
   */
  addEntry(value: unknown, rowGroup: number): void {
    const key = this.encodeValue(value)

    // Add to global value bloom
    this.valueBloom.addBytes(key)

    // Add to row group specific bloom
    if (rowGroup >= 0 && rowGroup < this.rowGroupBlooms.length) {
      this.rowGroupBlooms[rowGroup].addBytes(key)
    }
  }

  /**
   * Check if a value might exist in the index
   * @returns true if value might exist, false if it definitely does not
   */
  mightContain(value: unknown): boolean {
    const key = this.encodeValue(value)
    return this.valueBloom.mightContainBytes(key)
  }

  /**
   * Get which row groups might contain the value
   * @returns Array of row group indices that might contain the value
   */
  getMatchingRowGroups(value: unknown): number[] {
    const key = this.encodeValue(value)

    // First check global bloom - if not present, no row groups match
    if (!this.valueBloom.mightContainBytes(key)) {
      return []
    }

    // Check each row group bloom
    const matches: number[] = []
    for (let i = 0; i < this.rowGroupBlooms.length; i++) {
      if (this.rowGroupBlooms[i].mightContainBytes(key)) {
        matches.push(i)
      }
    }

    return matches
  }

  /**
   * Serialize the bloom filter to bytes
   */
  toBuffer(): Uint8Array {
    const valueBloomData = this.valueBloom.toBuffer()
    const rowGroupBloomSize = ROW_GROUP_BLOOM_SIZE * this.numRowGroups

    const totalSize = HEADER_SIZE + valueBloomData.length + rowGroupBloomSize
    const buffer = new ArrayBuffer(totalSize)
    const view = new DataView(buffer)
    const bytes = new Uint8Array(buffer)

    let offset = 0

    // Header
    bytes.set(MAGIC, offset)
    offset += 4

    view.setUint16(offset, VERSION, false)
    offset += 2

    view.setUint16(offset, DEFAULT_NUM_HASH_FUNCTIONS, false)
    offset += 2

    view.setUint32(offset, valueBloomData.length, false)
    offset += 4

    view.setUint16(offset, this.numRowGroups, false)
    offset += 2

    view.setUint16(offset, 0, false) // reserved
    offset += 2

    // Value bloom filter
    bytes.set(valueBloomData, offset)
    offset += valueBloomData.length

    // Row group bloom filters
    for (let i = 0; i < this.numRowGroups; i++) {
      bytes.set(this.rowGroupBlooms[i].toBuffer(), offset)
      offset += ROW_GROUP_BLOOM_SIZE
    }

    return bytes
  }

  /**
   * Deserialize a bloom filter from bytes
   */
  static fromBuffer(buffer: Uint8Array): IndexBloomFilter {
    if (buffer.length < HEADER_SIZE) {
      throw new Error('Invalid bloom filter: buffer too small')
    }

    const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength)
    let offset = 0

    // Verify magic
    for (let i = 0; i < 4; i++) {
      if (buffer[offset + i] !== MAGIC[i]) {
        throw new Error('Invalid bloom filter: bad magic')
      }
    }
    offset += 4

    // Read header
    const version = view.getUint16(offset, false)
    offset += 2
    if (version !== VERSION) {
      throw new Error(`Unsupported bloom filter version: ${version}`)
    }

    const numHashFunctions = view.getUint16(offset, false)
    offset += 2

    const filterSize = view.getUint32(offset, false)
    offset += 4

    const numRowGroups = view.getUint16(offset, false)
    offset += 2

    // Skip reserved
    offset += 2

    // Create filter
    const filter = new IndexBloomFilter(numRowGroups, filterSize, numHashFunctions)

    // Read value bloom
    const valueBloomData = buffer.slice(offset, offset + filterSize)
    filter.valueBloom = BloomFilter.fromBuffer(valueBloomData, numHashFunctions)
    offset += filterSize

    // Read row group blooms
    for (let i = 0; i < numRowGroups; i++) {
      const rgBloomData = buffer.slice(offset, offset + ROW_GROUP_BLOOM_SIZE)
      filter.rowGroupBlooms[i] = BloomFilter.fromBuffer(rgBloomData, numHashFunctions)
      offset += ROW_GROUP_BLOOM_SIZE
    }

    return filter
  }

  /**
   * Get statistics about the bloom filter
   */
  getStats(): {
    totalSizeBytes: number
    valueBloomSizeBytes: number
    rowGroupBloomSizeBytes: number
    numRowGroups: number
    estimatedValueCount: number
    estimatedFPR: number
  } {
    return {
      totalSizeBytes: HEADER_SIZE + this.valueBloom.sizeBytes + ROW_GROUP_BLOOM_SIZE * this.numRowGroups,
      valueBloomSizeBytes: this.valueBloom.sizeBytes,
      rowGroupBloomSizeBytes: ROW_GROUP_BLOOM_SIZE * this.numRowGroups,
      numRowGroups: this.numRowGroups,
      estimatedValueCount: this.valueBloom.estimateCount(),
      estimatedFPR: this.valueBloom.estimateFPR(),
    }
  }

  private encodeValue(value: unknown): Uint8Array {
    if (value === null || value === undefined) {
      return new Uint8Array([0])
    }

    if (typeof value === 'number') {
      const buffer = new ArrayBuffer(8)
      const view = new DataView(buffer)
      view.setFloat64(0, value, false)
      return new Uint8Array(buffer)
    }

    if (typeof value === 'boolean') {
      return new Uint8Array([value ? 1 : 0])
    }

    if (typeof value === 'string') {
      return new TextEncoder().encode(value)
    }

    // For objects/arrays, use JSON
    return new TextEncoder().encode(JSON.stringify(value))
  }
}

// =============================================================================
// Optimal Parameters Calculator
// =============================================================================

/**
 * Calculate optimal bloom filter parameters
 */
export function calculateOptimalParams(
  expectedItems: number,
  falsePositiveRate: number = 0.01
): { sizeBytes: number; numHashFunctions: number } {
  // Optimal bits: m = -n * ln(p) / (ln(2))^2
  const m = Math.ceil(
    (-expectedItems * Math.log(falsePositiveRate)) / Math.pow(Math.log(2), 2)
  )

  // Optimal hash functions: k = (m/n) * ln(2)
  const k = Math.round((m / expectedItems) * Math.log(2))

  return {
    sizeBytes: Math.ceil(m / 8),
    numHashFunctions: Math.max(1, Math.min(k, 10)), // Cap at 10 hash functions
  }
}

/**
 * Estimate false positive rate for given parameters
 */
export function estimateFalsePositiveRate(
  sizeBytes: number,
  numHashFunctions: number,
  expectedItems: number
): number {
  const m = sizeBytes * 8
  const k = numHashFunctions
  const n = expectedItems

  // FPR = (1 - e^(-kn/m))^k
  return Math.pow(1 - Math.exp((-k * n) / m), k)
}
