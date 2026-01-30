/**
 * ParqueDB Bloom Filter and Probabilistic Index Implementation
 *
 * This module provides bloom filter implementations for efficient existence
 * checks in ParqueDB's graph-first architecture.
 */

import type { Node, Edge, Namespace, EntityId, RelationType } from './graph-schemas'

// ============================================================================
// Bloom Filter Core Types
// ============================================================================

/**
 * Bloom filter configuration
 */
export interface BloomFilterConfig {
  /** Expected number of elements */
  expectedElements: number

  /** Target false positive rate (0.0 - 1.0) */
  falsePositiveRate: number

  /** Hash function to use */
  hashFunction?: 'murmur3' | 'xxhash64'
}

/**
 * Bloom filter statistics
 */
export interface BloomFilterStats {
  /** Number of bits in the filter */
  bits: number

  /** Number of hash functions */
  hashFunctions: number

  /** Number of elements inserted */
  elements: number

  /** Current estimated false positive rate */
  currentFPR: number

  /** Size in bytes */
  sizeBytes: number
}

/**
 * Filter scope for hierarchical bloom filters
 */
export type FilterScope =
  | { type: 'global' }
  | { type: 'namespace'; ns: Namespace }
  | { type: 'file'; fileId: string }
  | { type: 'row_group'; fileId: string; rowGroupIndex: number }

/**
 * Key type for bloom filter entries
 */
export type BloomKeyType =
  | 'entity'        // ns|type|id
  | 'edge_exact'    // ns|from_id|rel_type|to_id
  | 'edge_outgoing' // ns|from_id|rel_type
  | 'edge_incoming' // ns|to_id|rel_type
  | 'custom'

// ============================================================================
// Standard Bloom Filter Implementation
// ============================================================================

/**
 * Standard Bloom Filter implementation using double hashing
 */
export class BloomFilter {
  private bits: Uint8Array
  private readonly k: number // Number of hash functions
  private readonly m: number // Number of bits
  private n: number = 0 // Number of elements inserted

  constructor(config: BloomFilterConfig) {
    const { expectedElements, falsePositiveRate } = config

    // Calculate optimal parameters
    // m = -n * ln(p) / (ln(2)^2)
    this.m = Math.ceil((-expectedElements * Math.log(falsePositiveRate)) / (Math.LN2 * Math.LN2))

    // k = (m/n) * ln(2)
    this.k = Math.round((this.m / expectedElements) * Math.LN2)

    // Allocate bit array
    this.bits = new Uint8Array(Math.ceil(this.m / 8))
  }

  /**
   * Insert an element into the bloom filter
   */
  insert(key: string): void {
    const hashes = this.computeHashes(key)

    for (const hash of hashes) {
      const bitIndex = hash % this.m
      const byteIndex = Math.floor(bitIndex / 8)
      const bitOffset = bitIndex % 8
      this.bits[byteIndex] |= 1 << bitOffset
    }

    this.n++
  }

  /**
   * Insert using pre-computed hash (for efficiency)
   */
  insertHash(hash: bigint): void {
    const hashes = this.hashesToPositions(hash)

    for (const pos of hashes) {
      const bitIndex = pos % this.m
      const byteIndex = Math.floor(bitIndex / 8)
      const bitOffset = bitIndex % 8
      this.bits[byteIndex] |= 1 << bitOffset
    }

    this.n++
  }

  /**
   * Check if an element might be in the set
   *
   * @returns false = definitely not in set
   * @returns true = probably in set (may be false positive)
   */
  contains(key: string): boolean {
    const hashes = this.computeHashes(key)

    for (const hash of hashes) {
      const bitIndex = hash % this.m
      const byteIndex = Math.floor(bitIndex / 8)
      const bitOffset = bitIndex % 8

      if ((this.bits[byteIndex] & (1 << bitOffset)) === 0) {
        return false
      }
    }

    return true
  }

  /**
   * Check using pre-computed hash
   */
  containsHash(hash: bigint): boolean {
    const positions = this.hashesToPositions(hash)

    for (const pos of positions) {
      const bitIndex = pos % this.m
      const byteIndex = Math.floor(bitIndex / 8)
      const bitOffset = bitIndex % 8

      if ((this.bits[byteIndex] & (1 << bitOffset)) === 0) {
        return false
      }
    }

    return true
  }

  /**
   * Compute k hash values using double hashing: h(i) = h1 + i * h2
   */
  private computeHashes(key: string): number[] {
    const h1 = murmur3_32(key, 0)
    const h2 = murmur3_32(key, h1)

    const hashes: number[] = []
    for (let i = 0; i < this.k; i++) {
      hashes.push(Math.abs((h1 + i * h2) | 0))
    }

    return hashes
  }

  /**
   * Convert a 64-bit hash to k positions using double hashing
   */
  private hashesToPositions(hash: bigint): number[] {
    const h1 = Number(hash & 0xffffffffn)
    const h2 = Number((hash >> 32n) & 0xffffffffn)

    const positions: number[] = []
    for (let i = 0; i < this.k; i++) {
      positions.push(Math.abs((h1 + i * h2) | 0))
    }

    return positions
  }

  /**
   * Serialize bloom filter to bytes
   */
  serialize(): Uint8Array {
    const header = new Uint8Array(16)
    const view = new DataView(header.buffer)

    view.setUint32(0, this.m, true)
    view.setUint32(4, this.k, true)
    view.setUint32(8, this.n, true)
    view.setUint32(12, 0, true) // Reserved

    const result = new Uint8Array(16 + this.bits.length)
    result.set(header)
    result.set(this.bits, 16)

    return result
  }

  /**
   * Deserialize bloom filter from bytes
   */
  static deserialize(data: Uint8Array): BloomFilter {
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength)

    const m = view.getUint32(0, true)
    const k = view.getUint32(4, true)
    const n = view.getUint32(8, true)

    // Create filter with dummy config, then override
    const filter = Object.create(BloomFilter.prototype) as BloomFilter
    ;(filter as any).m = m
    ;(filter as any).k = k
    ;(filter as any).n = n
    ;(filter as any).bits = data.slice(16)

    return filter
  }

  /**
   * Get bloom filter statistics
   */
  get stats(): BloomFilterStats {
    return {
      bits: this.m,
      hashFunctions: this.k,
      elements: this.n,
      currentFPR: Math.pow(1 - Math.exp((-this.k * this.n) / this.m), this.k),
      sizeBytes: this.bits.length,
    }
  }

  /**
   * Get number of elements inserted
   */
  get count(): number {
    return this.n
  }

  /**
   * Get size in bytes
   */
  get sizeBytes(): number {
    return this.bits.length
  }

  /**
   * Merge another bloom filter into this one (union)
   * Both filters must have same m and k values
   */
  merge(other: BloomFilter): void {
    if (this.m !== other.m || this.k !== other.k) {
      throw new Error('Cannot merge bloom filters with different parameters')
    }

    for (let i = 0; i < this.bits.length; i++) {
      this.bits[i] |= other.bits[i]
    }

    // Note: n becomes an estimate after merge
    this.n += other.n
  }

  /**
   * Clear the bloom filter
   */
  clear(): void {
    this.bits.fill(0)
    this.n = 0
  }
}

// ============================================================================
// Counting Bloom Filter (Supports Deletion)
// ============================================================================

/**
 * Counting Bloom Filter with 4-bit counters
 * Supports deletion but uses 4x more space than standard bloom filter
 */
export class CountingBloomFilter {
  private counters: Uint8Array // 2 counters per byte (4-bit each)
  private readonly k: number
  private readonly m: number
  private n: number = 0
  private overflow: Set<number> = new Set()

  constructor(config: BloomFilterConfig) {
    const { expectedElements, falsePositiveRate } = config

    this.m = Math.ceil((-expectedElements * Math.log(falsePositiveRate)) / (Math.LN2 * Math.LN2))
    this.k = Math.round((this.m / expectedElements) * Math.LN2)

    // 4 bits per counter = 2 counters per byte
    this.counters = new Uint8Array(Math.ceil(this.m / 2))
  }

  private getCounter(index: number): number {
    if (this.overflow.has(index)) {
      return 16 // Saturated
    }

    const byteIndex = Math.floor(index / 2)
    const isHighNibble = index % 2 === 1
    const byte = this.counters[byteIndex]

    return isHighNibble ? byte >> 4 : byte & 0x0f
  }

  private setCounter(index: number, value: number): void {
    if (value > 15) {
      this.overflow.add(index)
      return
    }

    const byteIndex = Math.floor(index / 2)
    const isHighNibble = index % 2 === 1

    if (isHighNibble) {
      this.counters[byteIndex] = (this.counters[byteIndex] & 0x0f) | (value << 4)
    } else {
      this.counters[byteIndex] = (this.counters[byteIndex] & 0xf0) | value
    }
  }

  /**
   * Insert an element
   */
  insert(key: string): void {
    const hashes = this.computeHashes(key)

    for (const hash of hashes) {
      const index = hash % this.m
      const current = this.getCounter(index)
      this.setCounter(index, current + 1)
    }

    this.n++
  }

  /**
   * Delete an element
   * @returns true if element was present (counters decremented)
   * @returns false if element was not present
   */
  delete(key: string): boolean {
    const hashes = this.computeHashes(key)

    // First verify all counters are > 0
    for (const hash of hashes) {
      const index = hash % this.m
      if (this.getCounter(index) === 0) {
        return false
      }
    }

    // Decrement all counters
    for (const hash of hashes) {
      const index = hash % this.m
      if (!this.overflow.has(index)) {
        const current = this.getCounter(index)
        this.setCounter(index, current - 1)
      }
    }

    this.n--
    return true
  }

  /**
   * Check if element might be in the set
   */
  contains(key: string): boolean {
    const hashes = this.computeHashes(key)

    for (const hash of hashes) {
      const index = hash % this.m
      if (this.getCounter(index) === 0) {
        return false
      }
    }

    return true
  }

  private computeHashes(key: string): number[] {
    const h1 = murmur3_32(key, 0)
    const h2 = murmur3_32(key, h1)

    const hashes: number[] = []
    for (let i = 0; i < this.k; i++) {
      hashes.push(Math.abs((h1 + i * h2) | 0))
    }

    return hashes
  }

  /**
   * Serialize counting bloom filter
   */
  serialize(): Uint8Array {
    const overflowArray = Array.from(this.overflow)
    const overflowBytes = new Uint8Array(overflowArray.length * 4)
    const overflowView = new DataView(overflowBytes.buffer)

    for (let i = 0; i < overflowArray.length; i++) {
      overflowView.setUint32(i * 4, overflowArray[i], true)
    }

    const header = new Uint8Array(20)
    const view = new DataView(header.buffer)

    view.setUint32(0, this.m, true)
    view.setUint32(4, this.k, true)
    view.setUint32(8, this.n, true)
    view.setUint32(12, overflowArray.length, true)
    view.setUint32(16, this.counters.length, true)

    const result = new Uint8Array(20 + overflowBytes.length + this.counters.length)
    result.set(header)
    result.set(overflowBytes, 20)
    result.set(this.counters, 20 + overflowBytes.length)

    return result
  }

  static deserialize(data: Uint8Array): CountingBloomFilter {
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength)

    const m = view.getUint32(0, true)
    const k = view.getUint32(4, true)
    const n = view.getUint32(8, true)
    const overflowCount = view.getUint32(12, true)
    const countersLength = view.getUint32(16, true)

    const filter = Object.create(CountingBloomFilter.prototype) as CountingBloomFilter
    ;(filter as any).m = m
    ;(filter as any).k = k
    ;(filter as any).n = n

    // Read overflow set
    const overflow = new Set<number>()
    for (let i = 0; i < overflowCount; i++) {
      overflow.add(view.getUint32(20 + i * 4, true))
    }
    ;(filter as any).overflow = overflow

    // Read counters
    const countersOffset = 20 + overflowCount * 4
    ;(filter as any).counters = data.slice(countersOffset, countersOffset + countersLength)

    return filter
  }

  get count(): number {
    return this.n
  }

  get sizeBytes(): number {
    return this.counters.length + this.overflow.size * 4
  }
}

// ============================================================================
// Blocked Bloom Filter (Cache Efficient)
// ============================================================================

/**
 * Blocked Bloom Filter for cache-efficient lookups
 * All hash positions for a key fall within a single 64-byte cache line
 */
export class BlockedBloomFilter {
  private blocks: Uint8Array
  private readonly blockCount: number
  private readonly k: number
  private n: number = 0

  // 64 bytes = 512 bits per block (cache line size)
  private readonly BLOCK_SIZE = 64
  private readonly BLOCK_BITS = 512

  constructor(config: BloomFilterConfig) {
    const { expectedElements, falsePositiveRate } = config

    const totalBits = Math.ceil((-expectedElements * Math.log(falsePositiveRate)) / (Math.LN2 * Math.LN2))

    this.blockCount = Math.ceil(totalBits / this.BLOCK_BITS)
    this.k = Math.round(-Math.log2(falsePositiveRate))
    this.blocks = new Uint8Array(this.blockCount * this.BLOCK_SIZE)
  }

  private selectBlock(key: string): number {
    return murmur3_32(key, 0) % this.blockCount
  }

  private hashesWithinBlock(key: string, blockIndex: number): number[] {
    const seed = blockIndex * 31
    const h1 = murmur3_32(key, seed) % this.BLOCK_BITS
    const h2 = murmur3_32(key, seed + 1) % this.BLOCK_BITS

    const hashes: number[] = []
    for (let i = 0; i < this.k; i++) {
      hashes.push((h1 + i * h2) % this.BLOCK_BITS)
    }

    return hashes
  }

  /**
   * Insert an element
   */
  insert(key: string): void {
    const blockIndex = this.selectBlock(key)
    const blockOffset = blockIndex * this.BLOCK_SIZE
    const hashes = this.hashesWithinBlock(key, blockIndex)

    for (const bitPos of hashes) {
      const byteIndex = Math.floor(bitPos / 8)
      const bitOffset = bitPos % 8
      this.blocks[blockOffset + byteIndex] |= 1 << bitOffset
    }

    this.n++
  }

  /**
   * Check if element might be in the set
   * Only accesses a single cache line
   */
  contains(key: string): boolean {
    const blockIndex = this.selectBlock(key)
    const blockOffset = blockIndex * this.BLOCK_SIZE
    const hashes = this.hashesWithinBlock(key, blockIndex)

    for (const bitPos of hashes) {
      const byteIndex = Math.floor(bitPos / 8)
      const bitOffset = bitPos % 8

      if ((this.blocks[blockOffset + byteIndex] & (1 << bitOffset)) === 0) {
        return false
      }
    }

    return true
  }

  /**
   * Batch lookup - more efficient than individual lookups
   */
  containsBatch(keys: string[]): boolean[] {
    return keys.map((key) => this.contains(key))
  }

  serialize(): Uint8Array {
    const header = new Uint8Array(16)
    const view = new DataView(header.buffer)

    view.setUint32(0, this.blockCount, true)
    view.setUint32(4, this.k, true)
    view.setUint32(8, this.n, true)
    view.setUint32(12, 0, true) // Reserved

    const result = new Uint8Array(16 + this.blocks.length)
    result.set(header)
    result.set(this.blocks, 16)

    return result
  }

  static deserialize(data: Uint8Array): BlockedBloomFilter {
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength)

    const blockCount = view.getUint32(0, true)
    const k = view.getUint32(4, true)
    const n = view.getUint32(8, true)

    const filter = Object.create(BlockedBloomFilter.prototype) as BlockedBloomFilter
    ;(filter as any).blockCount = blockCount
    ;(filter as any).k = k
    ;(filter as any).n = n
    ;(filter as any).BLOCK_SIZE = 64
    ;(filter as any).BLOCK_BITS = 512
    ;(filter as any).blocks = data.slice(16)

    return filter
  }

  get count(): number {
    return this.n
  }

  get sizeBytes(): number {
    return this.blocks.length
  }
}

// ============================================================================
// Cuckoo Filter (Alternative with Delete Support)
// ============================================================================

/**
 * Cuckoo Filter - space efficient with native delete support
 * Better than counting bloom filter for most use cases
 */
export class CuckooFilter {
  private buckets: Uint16Array // 16-bit fingerprints
  private readonly bucketCount: number
  private readonly ENTRIES_PER_BUCKET = 4
  private readonly MAX_KICKS = 500
  private n: number = 0

  constructor(capacity: number) {
    this.bucketCount = Math.ceil(capacity / (this.ENTRIES_PER_BUCKET * 0.95))
    this.buckets = new Uint16Array(this.bucketCount * this.ENTRIES_PER_BUCKET)
  }

  private fingerprint(key: string): number {
    // 16-bit fingerprint (non-zero)
    const hash = murmur3_32(key, 0)
    const fp = (hash & 0xffff) || 1
    return fp
  }

  private index1(key: string): number {
    return murmur3_32(key, 1) % this.bucketCount
  }

  private index2(i1: number, fp: number): number {
    // i2 = i1 XOR hash(fingerprint)
    return (i1 ^ (murmur3_32(fp.toString(), 2) % this.bucketCount)) % this.bucketCount
  }

  private insertIntoBucket(bucketIndex: number, fp: number): boolean {
    const start = bucketIndex * this.ENTRIES_PER_BUCKET

    for (let i = 0; i < this.ENTRIES_PER_BUCKET; i++) {
      if (this.buckets[start + i] === 0) {
        this.buckets[start + i] = fp
        return true
      }
    }

    return false
  }

  private bucketContains(bucketIndex: number, fp: number): boolean {
    const start = bucketIndex * this.ENTRIES_PER_BUCKET

    for (let i = 0; i < this.ENTRIES_PER_BUCKET; i++) {
      if (this.buckets[start + i] === fp) {
        return true
      }
    }

    return false
  }

  private deleteFromBucket(bucketIndex: number, fp: number): boolean {
    const start = bucketIndex * this.ENTRIES_PER_BUCKET

    for (let i = 0; i < this.ENTRIES_PER_BUCKET; i++) {
      if (this.buckets[start + i] === fp) {
        this.buckets[start + i] = 0
        return true
      }
    }

    return false
  }

  /**
   * Insert an element
   * @returns true if successful, false if filter is full
   */
  insert(key: string): boolean {
    const fp = this.fingerprint(key)
    const i1 = this.index1(key)
    const i2 = this.index2(i1, fp)

    // Try to insert in bucket i1
    if (this.insertIntoBucket(i1, fp)) {
      this.n++
      return true
    }

    // Try to insert in bucket i2
    if (this.insertIntoBucket(i2, fp)) {
      this.n++
      return true
    }

    // Must relocate existing items
    let currentIndex = Math.random() < 0.5 ? i1 : i2
    let currentFp = fp

    for (let kick = 0; kick < this.MAX_KICKS; kick++) {
      // Swap with random entry in bucket
      const entryIndex = Math.floor(Math.random() * this.ENTRIES_PER_BUCKET)
      const bucketStart = currentIndex * this.ENTRIES_PER_BUCKET

      const evictedFp = this.buckets[bucketStart + entryIndex]
      this.buckets[bucketStart + entryIndex] = currentFp
      currentFp = evictedFp

      // Find alternate bucket for evicted item
      currentIndex = this.index2(currentIndex, currentFp)

      if (this.insertIntoBucket(currentIndex, currentFp)) {
        this.n++
        return true
      }
    }

    // Filter is full
    return false
  }

  /**
   * Check if element might be in the set
   */
  contains(key: string): boolean {
    const fp = this.fingerprint(key)
    const i1 = this.index1(key)
    const i2 = this.index2(i1, fp)

    return this.bucketContains(i1, fp) || this.bucketContains(i2, fp)
  }

  /**
   * Delete an element
   * @returns true if element was found and deleted
   */
  delete(key: string): boolean {
    const fp = this.fingerprint(key)
    const i1 = this.index1(key)
    const i2 = this.index2(i1, fp)

    if (this.deleteFromBucket(i1, fp)) {
      this.n--
      return true
    }

    if (this.deleteFromBucket(i2, fp)) {
      this.n--
      return true
    }

    return false
  }

  serialize(): Uint8Array {
    const header = new Uint8Array(12)
    const view = new DataView(header.buffer)

    view.setUint32(0, this.bucketCount, true)
    view.setUint32(4, this.n, true)
    view.setUint32(8, 0, true) // Reserved

    const bucketBytes = new Uint8Array(this.buckets.buffer)
    const result = new Uint8Array(12 + bucketBytes.length)
    result.set(header)
    result.set(bucketBytes, 12)

    return result
  }

  static deserialize(data: Uint8Array): CuckooFilter {
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength)

    const bucketCount = view.getUint32(0, true)
    const n = view.getUint32(4, true)

    const filter = Object.create(CuckooFilter.prototype) as CuckooFilter
    ;(filter as any).bucketCount = bucketCount
    ;(filter as any).n = n
    ;(filter as any).ENTRIES_PER_BUCKET = 4
    ;(filter as any).MAX_KICKS = 500

    // Copy bucket data
    const bucketsData = data.slice(12)
    ;(filter as any).buckets = new Uint16Array(bucketsData.buffer, bucketsData.byteOffset, bucketsData.byteLength / 2)

    return filter
  }

  get count(): number {
    return this.n
  }

  get sizeBytes(): number {
    return this.buckets.byteLength
  }

  get loadFactor(): number {
    return this.n / (this.bucketCount * this.ENTRIES_PER_BUCKET)
  }
}

// ============================================================================
// Compound Key Bloom Filter
// ============================================================================

/**
 * Bloom filter specialized for compound keys (ns + type + id)
 */
export class CompoundKeyBloom {
  private bloom: BloomFilter
  private readonly delimiter = '|'

  constructor(config: BloomFilterConfig) {
    this.bloom = new BloomFilter(config)
  }

  /**
   * Insert an entity key: ns|type|id
   */
  insertEntity(ns: Namespace, type: string, id: EntityId): void {
    const key = `${ns}${this.delimiter}${type}${this.delimiter}${id}`
    this.bloom.insert(key)
  }

  /**
   * Check if entity might exist
   */
  containsEntity(ns: Namespace, type: string, id: EntityId): boolean {
    const key = `${ns}${this.delimiter}${type}${this.delimiter}${id}`
    return this.bloom.contains(key)
  }

  /**
   * Insert an edge key: ns|from_id|rel_type|to_id
   */
  insertEdge(ns: Namespace, fromId: EntityId, relType: RelationType, toId: EntityId): void {
    const key = `${ns}${this.delimiter}${fromId}${this.delimiter}${relType}${this.delimiter}${toId}`
    this.bloom.insert(key)
  }

  /**
   * Check if edge might exist
   */
  containsEdge(ns: Namespace, fromId: EntityId, relType: RelationType, toId: EntityId): boolean {
    const key = `${ns}${this.delimiter}${fromId}${this.delimiter}${relType}${this.delimiter}${toId}`
    return this.bloom.contains(key)
  }

  serialize(): Uint8Array {
    return this.bloom.serialize()
  }

  static deserialize(data: Uint8Array): CompoundKeyBloom {
    const filter = Object.create(CompoundKeyBloom.prototype) as CompoundKeyBloom
    ;(filter as any).bloom = BloomFilter.deserialize(data)
    ;(filter as any).delimiter = '|'
    return filter
  }

  get stats(): BloomFilterStats {
    return this.bloom.stats
  }
}

// ============================================================================
// Edge Existence Filter
// ============================================================================

/**
 * Specialized filter for edge existence queries
 * Maintains separate filters for exact, outgoing, and incoming patterns
 */
export class EdgeExistenceFilter {
  private exact: BloomFilter
  private outgoing: BloomFilter
  private incoming: BloomFilter
  private readonly delimiter = '|'

  constructor(expectedEdges: number, fpr: number = 0.001) {
    // Exact edges need all edges
    this.exact = new BloomFilter({
      expectedElements: expectedEdges,
      falsePositiveRate: fpr,
    })

    // Outgoing/incoming are sparser (unique combinations)
    // Estimate: 10% of edges are unique outgoing patterns
    this.outgoing = new BloomFilter({
      expectedElements: expectedEdges * 0.1,
      falsePositiveRate: fpr,
    })

    this.incoming = new BloomFilter({
      expectedElements: expectedEdges * 0.1,
      falsePositiveRate: fpr,
    })
  }

  /**
   * Insert an edge
   */
  insertEdge(edge: Edge): void {
    const d = this.delimiter

    // Exact edge key
    const exactKey = `${edge.ns}${d}${edge.from_id}${d}${edge.rel_type}${d}${edge.to_id}`
    this.exact.insert(exactKey)

    // Outgoing pattern
    const outKey = `${edge.ns}${d}${edge.from_id}${d}${edge.rel_type}`
    this.outgoing.insert(outKey)

    // Incoming pattern
    const inKey = `${edge.ns}${d}${edge.to_id}${d}${edge.rel_type}`
    this.incoming.insert(inKey)

    // For bidirectional edges, also add reverse
    if (edge.bidirectional) {
      const reverseExact = `${edge.ns}${d}${edge.to_id}${d}${edge.rel_type}${d}${edge.from_id}`
      this.exact.insert(reverseExact)

      const reverseOut = `${edge.ns}${d}${edge.to_id}${d}${edge.rel_type}`
      this.outgoing.insert(reverseOut)

      const reverseIn = `${edge.ns}${d}${edge.from_id}${d}${edge.rel_type}`
      this.incoming.insert(reverseIn)
    }
  }

  /**
   * Check if exact edge might exist
   */
  hasEdge(ns: Namespace, fromId: EntityId, relType: RelationType, toId: EntityId): boolean {
    const key = `${ns}${this.delimiter}${fromId}${this.delimiter}${relType}${this.delimiter}${toId}`
    return this.exact.contains(key)
  }

  /**
   * Check if node has any outgoing edges of type
   */
  hasOutgoing(ns: Namespace, fromId: EntityId, relType: RelationType): boolean {
    const key = `${ns}${this.delimiter}${fromId}${this.delimiter}${relType}`
    return this.outgoing.contains(key)
  }

  /**
   * Check if node has any incoming edges of type
   */
  hasIncoming(ns: Namespace, toId: EntityId, relType: RelationType): boolean {
    const key = `${ns}${this.delimiter}${toId}${this.delimiter}${relType}`
    return this.incoming.contains(key)
  }

  serialize(): Uint8Array {
    const exactData = this.exact.serialize()
    const outgoingData = this.outgoing.serialize()
    const incomingData = this.incoming.serialize()

    const header = new Uint8Array(12)
    const view = new DataView(header.buffer)

    view.setUint32(0, exactData.length, true)
    view.setUint32(4, outgoingData.length, true)
    view.setUint32(8, incomingData.length, true)

    const result = new Uint8Array(12 + exactData.length + outgoingData.length + incomingData.length)
    result.set(header)
    result.set(exactData, 12)
    result.set(outgoingData, 12 + exactData.length)
    result.set(incomingData, 12 + exactData.length + outgoingData.length)

    return result
  }

  static deserialize(data: Uint8Array): EdgeExistenceFilter {
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength)

    const exactLen = view.getUint32(0, true)
    const outgoingLen = view.getUint32(4, true)
    const incomingLen = view.getUint32(8, true)

    const filter = Object.create(EdgeExistenceFilter.prototype) as EdgeExistenceFilter
    ;(filter as any).delimiter = '|'
    ;(filter as any).exact = BloomFilter.deserialize(data.slice(12, 12 + exactLen))
    ;(filter as any).outgoing = BloomFilter.deserialize(data.slice(12 + exactLen, 12 + exactLen + outgoingLen))
    ;(filter as any).incoming = BloomFilter.deserialize(data.slice(12 + exactLen + outgoingLen))

    return filter
  }

  get stats(): {
    exact: BloomFilterStats
    outgoing: BloomFilterStats
    incoming: BloomFilterStats
  } {
    return {
      exact: this.exact.stats,
      outgoing: this.outgoing.stats,
      incoming: this.incoming.stats,
    }
  }
}

// ============================================================================
// Hierarchical Bloom Filter Manager
// ============================================================================

/**
 * Manages bloom filters across the storage hierarchy
 */
export class BloomFilterManager {
  private globalEntityBloom: BloomFilter
  private globalEdgeFilter: EdgeExistenceFilter
  private fileBlooms: Map<string, BloomFilter> = new Map()
  private rowGroupBlooms: Map<string, BloomFilter[]> = new Map()

  constructor(config: {
    expectedEntities: number
    expectedEdges: number
    entityFPR?: number
    edgeFPR?: number
  }) {
    this.globalEntityBloom = new BloomFilter({
      expectedElements: config.expectedEntities,
      falsePositiveRate: config.entityFPR ?? 0.01,
    })

    this.globalEdgeFilter = new EdgeExistenceFilter(config.expectedEdges, config.edgeFPR ?? 0.001)
  }

  /**
   * Check if entity might exist (global check)
   */
  maybeHasEntity(ns: Namespace, type: string, id: EntityId): boolean {
    const key = `${ns}|${type}|${id}`
    return this.globalEntityBloom.contains(key)
  }

  /**
   * Check if edge might exist (global check)
   */
  maybeHasEdge(ns: Namespace, fromId: EntityId, relType: RelationType, toId: EntityId): boolean {
    return this.globalEdgeFilter.hasEdge(ns, fromId, relType, toId)
  }

  /**
   * Check if node might have outgoing edges of type
   */
  maybeHasOutgoing(ns: Namespace, fromId: EntityId, relType: RelationType): boolean {
    return this.globalEdgeFilter.hasOutgoing(ns, fromId, relType)
  }

  /**
   * Check if node might have incoming edges of type
   */
  maybeHasIncoming(ns: Namespace, toId: EntityId, relType: RelationType): boolean {
    return this.globalEdgeFilter.hasIncoming(ns, toId, relType)
  }

  /**
   * Get candidate files for an entity lookup
   */
  getCandidateFiles(ns: Namespace, type: string, id: EntityId): string[] {
    const key = `${ns}|${type}|${id}`
    const candidates: string[] = []

    for (const [fileId, bloom] of this.fileBlooms) {
      if (bloom.contains(key)) {
        candidates.push(fileId)
      }
    }

    return candidates
  }

  /**
   * Get candidate row groups within a file
   */
  getCandidateRowGroups(fileId: string, ns: Namespace, type: string, id: EntityId): number[] {
    const key = `${ns}|${type}|${id}`
    const rgBlooms = this.rowGroupBlooms.get(fileId)

    if (!rgBlooms) {
      return [] // Return empty to indicate "check all"
    }

    const candidates: number[] = []
    for (let i = 0; i < rgBlooms.length; i++) {
      if (rgBlooms[i].contains(key)) {
        candidates.push(i)
      }
    }

    return candidates
  }

  /**
   * Insert entity into hierarchy
   */
  insertEntity(node: Node, fileId: string, rowGroupIndex: number): void {
    const key = `${node.ns}|${node.type}|${node.id}`

    // Global
    this.globalEntityBloom.insert(key)

    // File-level
    let fileBloom = this.fileBlooms.get(fileId)
    if (!fileBloom) {
      fileBloom = new BloomFilter({ expectedElements: 100000, falsePositiveRate: 0.01 })
      this.fileBlooms.set(fileId, fileBloom)
    }
    fileBloom.insert(key)

    // Row group
    let rgBlooms = this.rowGroupBlooms.get(fileId)
    if (!rgBlooms) {
      rgBlooms = []
      this.rowGroupBlooms.set(fileId, rgBlooms)
    }
    while (rgBlooms.length <= rowGroupIndex) {
      rgBlooms.push(new BloomFilter({ expectedElements: 10000, falsePositiveRate: 0.01 }))
    }
    rgBlooms[rowGroupIndex].insert(key)
  }

  /**
   * Insert edge
   */
  insertEdge(edge: Edge): void {
    this.globalEdgeFilter.insertEdge(edge)
  }

  /**
   * Get file bloom filter
   */
  getFileBloom(fileId: string): BloomFilter | undefined {
    return this.fileBlooms.get(fileId)
  }

  /**
   * Get row group bloom filters for a file
   */
  getRowGroupBlooms(fileId: string): BloomFilter[] | undefined {
    return this.rowGroupBlooms.get(fileId)
  }

  /**
   * Get statistics
   */
  get stats(): {
    global: BloomFilterStats
    edge: { exact: BloomFilterStats; outgoing: BloomFilterStats; incoming: BloomFilterStats }
    files: number
    rowGroups: number
  } {
    let totalRowGroups = 0
    for (const rgs of this.rowGroupBlooms.values()) {
      totalRowGroups += rgs.length
    }

    return {
      global: this.globalEntityBloom.stats,
      edge: this.globalEdgeFilter.stats,
      files: this.fileBlooms.size,
      rowGroups: totalRowGroups,
    }
  }
}

// ============================================================================
// Bloom Filter File Format
// ============================================================================

/**
 * Bloom filter file header
 */
export interface BloomFileHeader {
  magic: 'PQBL'
  version: number
  filterType: number // 1=standard, 2=counting, 3=cuckoo, 4=blocked
  compression: number // 0=none, 1=zstd, 2=lz4
  filterCount: number
}

/**
 * Bloom filter entry in file
 */
export interface BloomFileEntry {
  scope: FilterScope
  keyType: BloomKeyType
  elementCount: number
  falsePositiveRate: number
  filterData: Uint8Array
}

/**
 * Serialize bloom filters to file format
 */
export function serializeBloomFile(
  entries: BloomFileEntry[],
  options: { compression?: 'none' | 'zstd' | 'lz4' } = {}
): Uint8Array {
  const compression = options.compression ?? 'none'
  const compressionByte = compression === 'zstd' ? 1 : compression === 'lz4' ? 2 : 0

  // Calculate total size
  let totalSize = 16 // Header

  for (const entry of entries) {
    totalSize += 32 // Entry header
    totalSize += entry.filterData.length
  }

  const result = new Uint8Array(totalSize)
  const view = new DataView(result.buffer)

  // Write header
  result.set([0x50, 0x51, 0x42, 0x4c]) // "PQBL"
  view.setUint16(4, 1, true) // Version
  view.setUint8(6, 1) // Filter type (standard)
  view.setUint8(7, compressionByte)
  view.setUint32(8, entries.length, true)
  // Bytes 12-15 reserved

  let offset = 16

  for (const entry of entries) {
    // Entry header (32 bytes)
    view.setUint8(offset, scopeToType(entry.scope))
    view.setUint8(offset + 1, keyTypeToNumber(entry.keyType))
    view.setUint32(offset + 2, entry.elementCount, true)
    view.setFloat32(offset + 6, entry.falsePositiveRate, true)
    view.setUint32(offset + 10, entry.filterData.length, true)

    // Scope metadata (depends on scope type)
    if (entry.scope.type === 'file') {
      const fileIdBytes = new TextEncoder().encode(entry.scope.fileId)
      view.setUint16(offset + 14, fileIdBytes.length, true)
      result.set(fileIdBytes.slice(0, 14), offset + 16) // Max 14 bytes
    } else if (entry.scope.type === 'row_group') {
      const fileIdBytes = new TextEncoder().encode(entry.scope.fileId)
      view.setUint16(offset + 14, fileIdBytes.length, true)
      view.setUint16(offset + 16, entry.scope.rowGroupIndex, true)
      result.set(fileIdBytes.slice(0, 12), offset + 18) // Max 12 bytes
    }

    offset += 32

    // Filter data
    result.set(entry.filterData, offset)
    offset += entry.filterData.length
  }

  return result
}

/**
 * Parse bloom filter file
 */
export function parseBloomFile(data: Uint8Array): {
  header: BloomFileHeader
  entries: BloomFileEntry[]
} {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength)

  // Read header
  const magic = String.fromCharCode(data[0], data[1], data[2], data[3])
  if (magic !== 'PQBL') {
    throw new Error('Invalid bloom filter file: bad magic')
  }

  const header: BloomFileHeader = {
    magic: 'PQBL',
    version: view.getUint16(4, true),
    filterType: view.getUint8(6),
    compression: view.getUint8(7),
    filterCount: view.getUint32(8, true),
  }

  const entries: BloomFileEntry[] = []
  let offset = 16

  for (let i = 0; i < header.filterCount; i++) {
    const scopeType = view.getUint8(offset)
    const keyType = view.getUint8(offset + 1)
    const elementCount = view.getUint32(offset + 2, true)
    const fpr = view.getFloat32(offset + 6, true)
    const filterLength = view.getUint32(offset + 10, true)

    // Parse scope
    let scope: FilterScope
    if (scopeType === 0) {
      scope = { type: 'global' }
    } else if (scopeType === 1) {
      const fileIdLen = view.getUint16(offset + 14, true)
      const fileId = new TextDecoder().decode(data.slice(offset + 16, offset + 16 + Math.min(fileIdLen, 14)))
      scope = { type: 'file', fileId }
    } else if (scopeType === 2) {
      const fileIdLen = view.getUint16(offset + 14, true)
      const rowGroupIndex = view.getUint16(offset + 16, true)
      const fileId = new TextDecoder().decode(data.slice(offset + 18, offset + 18 + Math.min(fileIdLen, 12)))
      scope = { type: 'row_group', fileId, rowGroupIndex }
    } else {
      scope = { type: 'global' }
    }

    offset += 32

    const filterData = data.slice(offset, offset + filterLength)
    offset += filterLength

    entries.push({
      scope,
      keyType: numberToKeyType(keyType),
      elementCount,
      falsePositiveRate: fpr,
      filterData,
    })
  }

  return { header, entries }
}

// ============================================================================
// Query Optimizer Integration
// ============================================================================

/**
 * Query plan for bloom-optimized lookups
 */
export interface BloomQueryPlan {
  type: 'empty' | 'scan' | 'index'
  reason?: string
  files?: { fileId: string; rowGroups: number[] }[]
  estimatedRows?: number
  bloomFiltered?: boolean
}

/**
 * Optimize point lookup using bloom filters
 */
export function optimizePointLookup(
  manager: BloomFilterManager,
  ns: Namespace,
  type: string,
  id: EntityId
): BloomQueryPlan {
  // Step 1: Check global bloom
  if (!manager.maybeHasEntity(ns, type, id)) {
    return { type: 'empty', reason: 'Global bloom negative', bloomFiltered: true }
  }

  // Step 2: Find candidate files
  const candidateFiles = manager.getCandidateFiles(ns, type, id)

  if (candidateFiles.length === 0) {
    return { type: 'empty', reason: 'No candidate files', bloomFiltered: true }
  }

  // Step 3: Find candidate row groups within files
  const files: { fileId: string; rowGroups: number[] }[] = []

  for (const fileId of candidateFiles) {
    const rowGroups = manager.getCandidateRowGroups(fileId, ns, type, id)

    // Empty array means check all row groups
    if (rowGroups.length === 0) {
      const rgBlooms = manager.getRowGroupBlooms(fileId)
      if (rgBlooms) {
        files.push({ fileId, rowGroups: Array.from({ length: rgBlooms.length }, (_, i) => i) })
      } else {
        files.push({ fileId, rowGroups: [] }) // Will scan all
      }
    } else {
      files.push({ fileId, rowGroups })
    }
  }

  if (files.length === 0) {
    return { type: 'empty', reason: 'All files filtered by bloom', bloomFiltered: true }
  }

  // Estimate rows based on row group count
  let estimatedRows = 0
  for (const file of files) {
    estimatedRows += file.rowGroups.length * 10000 // Assume 10K rows per RG
  }

  return {
    type: 'scan',
    files,
    estimatedRows,
    bloomFiltered: true,
  }
}

/**
 * Optimize edge existence check
 */
export function optimizeEdgeExists(
  manager: BloomFilterManager,
  ns: Namespace,
  fromId: EntityId,
  relType: RelationType,
  toId: EntityId
): BloomQueryPlan {
  if (!manager.maybeHasEdge(ns, fromId, relType, toId)) {
    return { type: 'empty', reason: 'Edge bloom negative', bloomFiltered: true }
  }

  // Edge might exist - need to scan
  return {
    type: 'scan',
    reason: 'Edge bloom positive - scan required',
    bloomFiltered: true,
  }
}

/**
 * Optimize graph traversal
 */
export function optimizeTraversal(
  manager: BloomFilterManager,
  ns: Namespace,
  startId: EntityId,
  relType: RelationType
): { shouldTraverse: boolean; reason: string } {
  if (!manager.maybeHasOutgoing(ns, startId, relType)) {
    return { shouldTraverse: false, reason: 'No outgoing edges (bloom negative)' }
  }

  return { shouldTraverse: true, reason: 'Outgoing edges may exist' }
}

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * MurmurHash3 32-bit implementation
 */
function murmur3_32(key: string, seed: number): number {
  let h1 = seed
  const c1 = 0xcc9e2d51
  const c2 = 0x1b873593

  const encoder = new TextEncoder()
  const data = encoder.encode(key)
  const len = data.length
  const nblocks = Math.floor(len / 4)

  // Body
  for (let i = 0; i < nblocks; i++) {
    let k1 = (data[i * 4] | (data[i * 4 + 1] << 8) | (data[i * 4 + 2] << 16) | (data[i * 4 + 3] << 24)) >>> 0

    k1 = Math.imul(k1, c1)
    k1 = (k1 << 15) | (k1 >>> 17)
    k1 = Math.imul(k1, c2)

    h1 ^= k1
    h1 = (h1 << 13) | (h1 >>> 19)
    h1 = Math.imul(h1, 5) + 0xe6546b64
  }

  // Tail
  let k1 = 0
  const tail = len & 3
  if (tail >= 3) k1 ^= data[nblocks * 4 + 2] << 16
  if (tail >= 2) k1 ^= data[nblocks * 4 + 1] << 8
  if (tail >= 1) {
    k1 ^= data[nblocks * 4]
    k1 = Math.imul(k1, c1)
    k1 = (k1 << 15) | (k1 >>> 17)
    k1 = Math.imul(k1, c2)
    h1 ^= k1
  }

  // Finalization
  h1 ^= len
  h1 ^= h1 >>> 16
  h1 = Math.imul(h1, 0x85ebca6b)
  h1 ^= h1 >>> 13
  h1 = Math.imul(h1, 0xc2b2ae35)
  h1 ^= h1 >>> 16

  return h1 >>> 0
}

/**
 * Calculate optimal bloom filter parameters
 */
export function calculateBloomParams(expectedElements: number, falsePositiveRate: number): {
  bits: number
  hashFunctions: number
  sizeBytes: number
  bitsPerElement: number
} {
  const m = Math.ceil((-expectedElements * Math.log(falsePositiveRate)) / (Math.LN2 * Math.LN2))
  const k = Math.round((m / expectedElements) * Math.LN2)

  return {
    bits: m,
    hashFunctions: k,
    sizeBytes: Math.ceil(m / 8),
    bitsPerElement: m / expectedElements,
  }
}

function scopeToType(scope: FilterScope): number {
  switch (scope.type) {
    case 'global':
      return 0
    case 'namespace':
      return 1
    case 'file':
      return 2
    case 'row_group':
      return 3
    default:
      return 0
  }
}

function keyTypeToNumber(keyType: BloomKeyType): number {
  switch (keyType) {
    case 'entity':
      return 1
    case 'edge_exact':
      return 2
    case 'edge_outgoing':
      return 3
    case 'edge_incoming':
      return 4
    case 'custom':
      return 5
    default:
      return 1
  }
}

function numberToKeyType(n: number): BloomKeyType {
  switch (n) {
    case 1:
      return 'entity'
    case 2:
      return 'edge_exact'
    case 3:
      return 'edge_outgoing'
    case 4:
      return 'edge_incoming'
    case 5:
      return 'custom'
    default:
      return 'entity'
  }
}

// ============================================================================
// Default Configurations
// ============================================================================

/**
 * Recommended bloom filter configurations for different workloads
 */
export const BLOOM_CONFIGS = {
  /** Small dataset: < 1M entities */
  small: {
    expectedEntities: 1_000_000,
    expectedEdges: 10_000_000,
    entityFPR: 0.01,
    edgeFPR: 0.001,
  },

  /** Medium dataset: 1M - 10M entities */
  medium: {
    expectedEntities: 10_000_000,
    expectedEdges: 100_000_000,
    entityFPR: 0.01,
    edgeFPR: 0.001,
  },

  /** Large dataset: > 10M entities */
  large: {
    expectedEntities: 100_000_000,
    expectedEdges: 1_000_000_000,
    entityFPR: 0.01,
    edgeFPR: 0.001,
  },

  /** Per-file bloom */
  file: {
    expectedElements: 100_000,
    falsePositiveRate: 0.01,
  },

  /** Per-row-group bloom */
  rowGroup: {
    expectedElements: 10_000,
    falsePositiveRate: 0.01,
  },
} as const
