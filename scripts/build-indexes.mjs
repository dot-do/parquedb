#!/usr/bin/env node
/**
 * Build Secondary Indexes for ParqueDB
 *
 * Builds hash, SST, and FTS indexes from local Parquet data files.
 * Supports multi-file datasets with per-collection index definitions.
 * Generates bloom filters for fast pre-filtering.
 *
 * Usage:
 *   node scripts/build-indexes.mjs --dataset imdb-1m --collection titles --field $index_titleType --type hash
 *   node scripts/build-indexes.mjs --dataset imdb-1m --collection titles --field $index_startYear --type sst
 *   node scripts/build-indexes.mjs --dataset imdb-1m --collection titles --field name --type fts
 *   node scripts/build-indexes.mjs --dataset onet-full --collection occupations --all  # Build all indexes for collection
 *   node scripts/build-indexes.mjs --dataset imdb-1m --all  # Build all indexes for all collections in dataset
 *   node scripts/build-indexes.mjs --dataset imdb-1m --all --compact  # Use compact encoding (v3 format)
 *   node scripts/build-indexes.mjs --dataset imdb-1m --all --compact --bloom  # Include bloom filters
 */

import { readFileSync, writeFileSync, mkdirSync, existsSync, readdirSync, unlinkSync, rmSync } from 'fs'
import { join, dirname } from 'path'
import { parquetRead, parquetMetadataAsync } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'

// =============================================================================
// Configuration
// =============================================================================

const DATA_DIR = 'data-v3'  // Local data directory
const OUTPUT_DIR = 'data-v3'  // Output to same directory structure

// Sharding threshold - indexes larger than this will be sharded
const SHARD_THRESHOLD_BYTES = 5 * 1024 * 1024  // 5MB

// Bloom filter configuration
const BLOOM_FILTER_MAGIC = new Uint8Array([0x50, 0x51, 0x42, 0x46]) // "PQBF"
const BLOOM_FILTER_VERSION = 1
const BLOOM_FILTER_HEADER_SIZE = 16
const BLOOM_VALUE_FILTER_SIZE = 131072 // 128KB for global value bloom
const BLOOM_ROW_GROUP_SIZE = 4096 // 4KB per row group bloom
const BLOOM_NUM_HASH_FUNCTIONS = 3 // Optimized for ~1% FPR

// Dataset files mapping - lists all parquet files per dataset
const DATASET_FILES = {
  'imdb': ['titles', 'people', 'cast'],  // ~100K titles - fits in Worker limits
  'imdb-1m': ['titles', 'people', 'cast'],
  'onet-full': [
    'occupations',
    'skills',
    'abilities',
    'knowledge',
    'occupation-skills',
    'occupation-abilities',
    'occupation-knowledge',
  ],
  'unspsc-full': ['commodities', 'classes', 'families', 'segments'],
}

// Index definitions per dataset and collection
const INDEX_DEFINITIONS = {
  'imdb': {
    titles: [
      { name: 'titleType', field: '$index_titleType', type: 'hash' },
      { name: 'startYear', field: '$index_startYear', type: 'sst' },
      { name: 'averageRating', field: '$index_averageRating', type: 'sst' },
      { name: 'numVotes', field: '$index_numVotes', type: 'sst' },
      { name: 'name', field: 'name', type: 'fts' },
    ],
    people: [
      { name: 'birthYear', field: '$index_birthYear', type: 'sst' },
      { name: 'name', field: 'name', type: 'fts' },
    ],
    cast: [
      { name: 'category', field: '$index_category', type: 'hash' },
    ],
  },
  'imdb-1m': {
    titles: [
      { name: 'titleType', field: '$index_titleType', type: 'hash' },
      { name: 'startYear', field: '$index_startYear', type: 'sst' },
      { name: 'averageRating', field: '$index_averageRating', type: 'sst' },
      { name: 'numVotes', field: '$index_numVotes', type: 'sst' },
      { name: 'name', field: 'name', type: 'fts' },
    ],
    people: [
      { name: 'birthYear', field: '$index_birthYear', type: 'sst' },
      { name: 'name', field: 'name', type: 'fts' },
    ],
    cast: [
      { name: 'category', field: '$index_category', type: 'hash' },
    ],
  },
  'onet-full': {
    occupations: [
      { name: 'jobZone', field: '$index_jobZone', type: 'hash' },
      { name: 'socCode', field: '$index_socCode', type: 'sst' },
      { name: 'title', field: 'name', type: 'fts' },
    ],
    skills: [
      { name: 'name', field: 'name', type: 'fts' },
    ],
    abilities: [
      { name: 'name', field: 'name', type: 'fts' },
    ],
    knowledge: [
      { name: 'name', field: 'name', type: 'fts' },
    ],
    'occupation-skills': [
      { name: 'dataValue', field: '$index_dataValue', type: 'sst' },
      { name: 'scaleId', field: '$index_scaleId', type: 'hash' },
    ],
    'occupation-abilities': [
      { name: 'dataValue', field: '$index_dataValue', type: 'sst' },
      { name: 'scaleId', field: '$index_scaleId', type: 'hash' },
    ],
    'occupation-knowledge': [
      { name: 'dataValue', field: '$index_dataValue', type: 'sst' },
      { name: 'scaleId', field: '$index_scaleId', type: 'hash' },
    ],
  },
  'unspsc-full': {
    commodities: [
      { name: 'segmentCode', field: '$index_segmentCode', type: 'hash' },
      { name: 'code', field: '$index_code', type: 'sst' },
      { name: 'title', field: 'name', type: 'fts' },
    ],
    classes: [
      { name: 'segmentCode', field: '$index_segmentCode', type: 'hash' },
      { name: 'code', field: '$index_code', type: 'sst' },
      { name: 'title', field: 'name', type: 'fts' },
    ],
    families: [
      { name: 'segmentCode', field: '$index_segmentCode', type: 'hash' },
      { name: 'code', field: '$index_code', type: 'sst' },
      { name: 'title', field: 'name', type: 'fts' },
    ],
    segments: [
      { name: 'code', field: '$index_code', type: 'sst' },
      { name: 'title', field: 'name', type: 'fts' },
    ],
  },
}

// =============================================================================
// Index Building
// =============================================================================

/**
 * Build a hash index from Parquet data
 * @param {Array} rows - The data rows
 * @param {string} field - Field to index
 * @param {number[]} rowGroupBoundaries - Cumulative row counts per row group [end1, end2, ...]
 * @param {string} idField - ID field name
 */
function buildHashIndex(rows, field, rowGroupBoundaries = [], idField = '$id') {
  console.log(`  Building hash index for ${field}...`)
  const buckets = new Map()
  let entryCount = 0

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i]
    const value = getFieldValue(row, field)
    const docId = getFieldValue(row, idField) || `row_${i}`

    if (value === undefined || value === null) continue

    // Hash the value
    const key = encodeKey(value)
    const hash = hashKey(key)

    if (!buckets.has(hash)) {
      buckets.set(hash, [])
    }

    // Calculate row group from boundaries
    const rowGroup = getRowGroup(i, rowGroupBoundaries)
    const rowOffset = getRowOffset(i, rowGroup, rowGroupBoundaries)

    buckets.get(hash).push({
      key,
      docId: String(docId),
      rowGroup,
      rowOffset,
    })
    entryCount++
  }

  console.log(`  Created ${entryCount} entries in ${buckets.size} buckets across ${rowGroupBoundaries.length || 1} row groups`)
  return serializeHashIndex(buckets, entryCount)
}

/**
 * Build an SST index from Parquet data
 * @param {Array} rows - The data rows
 * @param {string} field - Field to index
 * @param {number[]} rowGroupBoundaries - Cumulative row counts per row group [end1, end2, ...]
 * @param {string} idField - ID field name
 */
function buildSSTIndex(rows, field, rowGroupBoundaries = [], idField = '$id') {
  console.log(`  Building SST index for ${field}...`)
  const entries = []

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i]
    const value = getFieldValue(row, field)
    const docId = getFieldValue(row, idField) || `row_${i}`

    if (value === undefined || value === null) continue

    // Calculate row group from boundaries
    const rowGroup = getRowGroup(i, rowGroupBoundaries)
    const rowOffset = getRowOffset(i, rowGroup, rowGroupBoundaries)

    entries.push({
      key: encodeKey(value),
      docId: String(docId),
      rowGroup,
      rowOffset,
    })
  }

  // Sort by key
  entries.sort((a, b) => compareKeys(a.key, b.key))

  console.log(`  Created ${entries.length} sorted entries across ${rowGroupBoundaries.length || 1} row groups`)
  return serializeSSTIndex(entries)
}

/**
 * Build an FTS index from Parquet data
 */
function buildFTSIndex(rows, field, idField = '$id') {
  console.log(`  Building FTS index for ${field}...`)
  const index = new Map()
  const docStats = new Map()
  const corpusStats = {
    documentCount: 0,
    avgDocLength: 0,
    documentFrequency: new Map(),
  }

  let totalLength = 0

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i]
    const text = getFieldValue(row, field)
    const docId = getFieldValue(row, idField) || `row_${i}`

    if (typeof text !== 'string' || !text) continue

    // Tokenize
    const tokens = tokenize(text)
    const termFreqs = new Map()

    for (let pos = 0; pos < tokens.length; pos++) {
      const term = tokens[pos]
      if (!termFreqs.has(term)) {
        termFreqs.set(term, { freq: 0, positions: [] })
      }
      termFreqs.get(term).freq++
      termFreqs.get(term).positions.push(pos)
    }

    // Add to index
    const termsInDoc = new Set()
    for (const [term, { freq, positions }] of termFreqs) {
      if (!index.has(term)) {
        index.set(term, [])
      }
      index.get(term).push({
        docId: String(docId),
        field,
        frequency: freq,
        positions,
      })

      if (!termsInDoc.has(term)) {
        termsInDoc.add(term)
        const df = corpusStats.documentFrequency.get(term) || 0
        corpusStats.documentFrequency.set(term, df + 1)
      }
    }

    // Document stats
    docStats.set(String(docId), {
      docId: String(docId),
      fieldLengths: new Map([[field, tokens.length]]),
      totalLength: tokens.length,
    })

    corpusStats.documentCount++
    totalLength += tokens.length
  }

  corpusStats.avgDocLength = totalLength / Math.max(corpusStats.documentCount, 1)

  console.log(`  Indexed ${corpusStats.documentCount} documents, ${index.size} unique terms`)
  return serializeFTSIndex(index, docStats, corpusStats)
}

// =============================================================================
// Sharded Index Building
// =============================================================================

/**
 * Build a sharded hash index from Parquet data
 * Groups entries by value into separate shard files
 *
 * @param {Array} rows - The data rows
 * @param {string} field - Field to index
 * @param {number[]} rowGroupBoundaries - Cumulative row counts per row group
 * @param {string} idField - ID field name
 * @param {boolean} useCompact - Use compact v3 encoding
 * @returns {Object} Sharding result with entries grouped by value
 */
function buildShardedHashIndex(rows, field, rowGroupBoundaries = [], idField = '$id', useCompact = false) {
  console.log(`  Building sharded hash index for ${field}...`)

  // Group entries by their string value
  const shards = new Map()  // value string -> entries[]
  let totalEntries = 0

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i]
    const value = getFieldValue(row, field)
    const docId = getFieldValue(row, idField) || `row_${i}`

    if (value === undefined || value === null) continue

    // Use the string representation of the value as the shard name
    const shardName = sanitizeShardName(String(value))
    const key = encodeKey(value)

    if (!shards.has(shardName)) {
      shards.set(shardName, {
        name: shardName,
        originalValue: value,
        entries: [],
      })
    }

    const rowGroup = getRowGroup(i, rowGroupBoundaries)
    const rowOffset = getRowOffset(i, rowGroup, rowGroupBoundaries)

    shards.get(shardName).entries.push({
      key,
      docId: String(docId),
      rowGroup,
      rowOffset,
    })
    totalEntries++
  }

  console.log(`  Found ${shards.size} unique values, ${totalEntries} total entries`)

  // Serialize each shard
  const shardResults = []
  for (const [shardName, shard] of shards) {
    // Use compact encoding if enabled (no key hash since shards group by value)
    const serialized = useCompact
      ? serializeCompactShard(shard.entries)
      : serializeHashShard(shard.entries)
    shardResults.push({
      name: shardName,
      originalValue: shard.originalValue,
      data: serialized,
      entryCount: shard.entries.length,
      sizeBytes: serialized.length,
    })
  }

  return {
    type: 'hash',
    sharding: 'by-value',
    compact: useCompact,
    shards: shardResults,
    totalEntries,
    rowGroups: rowGroupBoundaries.length || 1,
  }
}

/**
 * Build a sharded SST index from Parquet data
 * Groups entries by value range (e.g., by first character or numeric range)
 *
 * @param {Array} rows - The data rows
 * @param {string} field - Field to index
 * @param {number[]} rowGroupBoundaries - Cumulative row counts per row group
 * @param {string} idField - ID field name
 * @param {boolean} useCompact - Use compact v3 encoding
 * @returns {Object} Sharding result with entries grouped by range
 */
function buildShardedSSTIndex(rows, field, rowGroupBoundaries = [], idField = '$id', useCompact = false) {
  console.log(`  Building sharded SST index for ${field}...`)

  // Collect all entries with their values
  const entries = []

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i]
    const value = getFieldValue(row, field)
    const docId = getFieldValue(row, idField) || `row_${i}`

    if (value === undefined || value === null) continue

    const rowGroup = getRowGroup(i, rowGroupBoundaries)
    const rowOffset = getRowOffset(i, rowGroup, rowGroupBoundaries)

    entries.push({
      value,
      key: encodeKey(value),
      docId: String(docId),
      rowGroup,
      rowOffset,
    })
  }

  // Sort by key
  entries.sort((a, b) => compareKeys(a.key, b.key))

  // Determine sharding strategy based on value type
  const firstValue = entries[0]?.value
  const shards = new Map()

  if (typeof firstValue === 'number') {
    // Numeric: shard by range (50-year spans for years, or order of magnitude)
    // Find min/max without spread operator to avoid stack overflow on large arrays
    let min = entries[0].value
    let max = entries[0].value
    for (const entry of entries) {
      if (entry.value < min) min = entry.value
      if (entry.value > max) max = entry.value
    }

    // Calculate range size to aim for shards under target size
    // Each entry is ~36 bytes (key:9 + docId:~27 + rowGroup:4 + rowOffset:4)
    const avgEntrySize = 36
    const targetShardSize = SHARD_THRESHOLD_BYTES * 0.8  // 80% of threshold
    const entriesPerShard = Math.floor(targetShardSize / avgEntrySize)

    // Estimate entries per unit range based on distribution
    const range = max - min
    const entriesPerUnit = entries.length / Math.max(range, 1)

    // Calculate range size to achieve target shard size
    let rangeSize = Math.max(0.001, entriesPerShard / entriesPerUnit)

    // Round to nice boundaries depending on the value range
    if (range < 20) {
      // Small ranges (like ratings 1-10): use 0.5, 1, or 2 unit chunks
      if (rangeSize < 0.75) rangeSize = 0.5
      else if (rangeSize < 1.5) rangeSize = 1
      else if (rangeSize < 3) rangeSize = 2
      else rangeSize = 5
    } else if (range <= 200) {
      // Year-like ranges: round to 5, 10, 20, or 25 year chunks
      if (rangeSize < 8) rangeSize = 5
      else if (rangeSize < 15) rangeSize = 10
      else if (rangeSize < 22) rangeSize = 20
      else rangeSize = 25
    } else {
      // For larger ranges: round to power of 10
      rangeSize = Math.pow(10, Math.floor(Math.log10(rangeSize)))
    }

    console.log(`    Range ${min}-${max}, using ${rangeSize} unit ranges (target: ~${entriesPerShard} entries/shard)`)

    for (const entry of entries) {
      const rangeStart = Math.floor(entry.value / rangeSize) * rangeSize
      const rangeEnd = rangeStart + rangeSize
      const shardName = `range-${rangeStart}-${rangeEnd}`

      if (!shards.has(shardName)) {
        shards.set(shardName, {
          name: shardName,
          rangeStart,
          rangeEnd,
          entries: [],
        })
      }

      shards.get(shardName).entries.push({
        key: entry.key,
        docId: entry.docId,
        rowGroup: entry.rowGroup,
        rowOffset: entry.rowOffset,
      })
    }
  } else if (typeof firstValue === 'string') {
    // String: shard by first character (or first 2 chars if too few buckets)
    for (const entry of entries) {
      const prefix = entry.value.charAt(0).toLowerCase() || '_'

      if (!shards.has(prefix)) {
        shards.set(prefix, {
          name: prefix,
          prefix,
          entries: [],
        })
      }

      shards.get(prefix).entries.push({
        key: entry.key,
        docId: entry.docId,
        rowGroup: entry.rowGroup,
        rowOffset: entry.rowOffset,
      })
    }
  } else {
    // Default: single shard
    shards.set('all', {
      name: 'all',
      entries: entries.map(e => ({
        key: e.key,
        docId: e.docId,
        rowGroup: e.rowGroup,
        rowOffset: e.rowOffset,
      })),
    })
  }

  console.log(`  Created ${shards.size} shards, ${entries.length} total entries`)

  // Serialize each shard (already sorted within each shard due to overall sort)
  const shardResults = []
  for (const [, shard] of shards) {
    // Use compact encoding if enabled (no key hash since shards group by key prefix/range)
    const serialized = useCompact
      ? serializeCompactShard(shard.entries)
      : serializeSSTShard(shard.entries)
    shardResults.push({
      name: shard.name,
      rangeStart: shard.rangeStart,
      rangeEnd: shard.rangeEnd,
      prefix: shard.prefix,
      data: serialized,
      entryCount: shard.entries.length,
      sizeBytes: serialized.length,
    })
  }

  return {
    type: 'sst',
    sharding: typeof firstValue === 'number' ? 'by-range' : 'by-prefix',
    compact: useCompact,
    shards: shardResults,
    totalEntries: entries.length,
    rowGroups: rowGroupBoundaries.length || 1,
  }
}

/**
 * Sanitize a value for use as a shard name (filename)
 */
function sanitizeShardName(name) {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9-_]/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '')
    .substring(0, 50) || 'default'
}

/**
 * Serialize entries for a hash index shard
 */
function serializeHashShard(entries) {
  let size = 1 + 4  // version + entry count
  for (const entry of entries) {
    size += 2 + entry.key.length + 2 + entry.docId.length + 4 + 4
  }

  const buffer = new ArrayBuffer(size)
  const view = new DataView(buffer)
  const bytes = new Uint8Array(buffer)
  const encoder = new TextEncoder()

  let offset = 0
  view.setUint8(offset, 2)  // version 2 = sharded format
  offset += 1
  view.setUint32(offset, entries.length, false)
  offset += 4

  for (const entry of entries) {
    view.setUint16(offset, entry.key.length, false)
    offset += 2
    bytes.set(entry.key, offset)
    offset += entry.key.length

    const docIdBytes = encoder.encode(entry.docId)
    view.setUint16(offset, docIdBytes.length, false)
    offset += 2
    bytes.set(docIdBytes, offset)
    offset += docIdBytes.length

    view.setUint32(offset, entry.rowGroup, false)
    offset += 4
    view.setUint32(offset, entry.rowOffset, false)
    offset += 4
  }

  return bytes
}

/**
 * Serialize entries for an SST index shard
 */
function serializeSSTShard(entries) {
  // Same format as regular SST - entries are already sorted
  return serializeHashShard(entries)  // Same binary format
}

/**
 * Calculate estimated size for determining if sharding is needed
 */
function estimateIndexSize(entryCount, avgKeyLen = 10, avgDocIdLen = 26) {
  // Format: version(1) + count(4) + entries * (keyLen(2) + key + docIdLen(2) + docId + rowGroup(4) + rowOffset(4))
  return 5 + entryCount * (2 + avgKeyLen + 2 + avgDocIdLen + 8)
}

// =============================================================================
// Compact Encoding (Version 3)
// =============================================================================

// Format version constants
const FORMAT_VERSION_1 = 0x01  // Original format with full key
const FORMAT_VERSION_2 = 0x02  // Sharded format (still full key)
const FORMAT_VERSION_3 = 0x03  // Compact format with varint encoding

/**
 * Write a variable-length unsigned integer (LEB128-style)
 * @param {Uint8Array} buffer
 * @param {number} offset
 * @param {number} value
 * @returns {number} bytes written
 */
function writeVarint(buffer, offset, value) {
  let bytesWritten = 0
  let v = value

  while (v >= 0x80) {
    buffer[offset + bytesWritten] = (v & 0x7f) | 0x80
    v >>>= 7
    bytesWritten++
  }

  buffer[offset + bytesWritten] = v
  bytesWritten++

  return bytesWritten
}

/**
 * Calculate varint size
 */
function varintSize(value) {
  if (value < 0x80) return 1
  if (value < 0x4000) return 2
  if (value < 0x200000) return 3
  if (value < 0x10000000) return 4
  return 5
}

/**
 * Serialize entries in compact format
 * Format: [version:u8][flags:u8][entryCount:u32][entries...]
 * Entry: [rowGroup:u16][rowOffset:varint][docIdLen:u8][docId:bytes]
 *
 * This format is ~3x smaller than v1/v2 by:
 * - Removing key from each entry (sharded indexes group by key implicitly)
 * - Using 2-byte rowGroup instead of 4-byte
 * - Using varint for rowOffset
 * - Using 1-byte docIdLen instead of 2-byte
 *
 * @param {Array} entries
 * @param {boolean} includeKeyHash - Include 4-byte key hash for non-sharded indexes
 * @returns {Uint8Array}
 */
function serializeCompactIndex(entries, includeKeyHash = false) {
  const encoder = new TextEncoder()

  // Calculate total size
  let totalSize = 6  // version(1) + flags(1) + entryCount(4)

  for (const entry of entries) {
    const docIdBytes = encoder.encode(entry.docId)
    if (includeKeyHash) {
      totalSize += 4  // keyHash
    }
    totalSize += 2  // rowGroup (u16)
    totalSize += varintSize(entry.rowOffset)  // rowOffset (varint)
    totalSize += 1  // docIdLen (u8)
    totalSize += docIdBytes.length  // docId
  }

  const buffer = new Uint8Array(totalSize)
  const view = new DataView(buffer.buffer)
  let offset = 0

  // Header
  buffer[offset++] = FORMAT_VERSION_3
  buffer[offset++] = includeKeyHash ? 0x01 : 0x00  // flags: bit 0 = hasKeyHash
  view.setUint32(offset, entries.length, false)
  offset += 4

  // Entries
  for (const entry of entries) {
    // Optional key hash
    if (includeKeyHash) {
      view.setUint32(offset, hashKey(entry.key), false)
      offset += 4
    }

    // Row group (2 bytes)
    view.setUint16(offset, entry.rowGroup, false)
    offset += 2

    // Row offset (varint)
    offset += writeVarint(buffer, offset, entry.rowOffset)

    // Doc ID (1-byte length + bytes)
    const docIdBytes = encoder.encode(entry.docId)
    if (docIdBytes.length > 255) {
      throw new Error(`Doc ID too long: ${docIdBytes.length} bytes (max 255)`)
    }
    buffer[offset++] = docIdBytes.length
    buffer.set(docIdBytes, offset)
    offset += docIdBytes.length
  }

  return buffer.slice(0, offset)
}

/**
 * Serialize entries in compact format for sharded indexes (no key hash)
 */
function serializeCompactShard(entries) {
  return serializeCompactIndex(entries, false)
}

/**
 * Serialize entries in compact format for non-sharded indexes (with key hash)
 */
function serializeCompactNonShardedIndex(entries) {
  return serializeCompactIndex(entries, true)
}

// =============================================================================
// Bloom Filter Building
// =============================================================================

/**
 * MurmurHash3 32-bit for bloom filter hashing
 */
function murmurHash3ForBloom(key, seed) {
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

  // Process remaining bytes
  const tail = len - numBlocks * 4
  let k1 = 0
  switch (tail) {
    case 3:
      k1 ^= key[numBlocks * 4 + 2] << 16
    // fallthrough
    case 2:
      k1 ^= key[numBlocks * 4 + 1] << 8
    // fallthrough
    case 1:
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
 */
function getBloomHashes(key, numHashes, filterBits) {
  const h1 = murmurHash3ForBloom(key, 0)
  const h2 = murmurHash3ForBloom(key, h1)

  const hashes = []
  for (let i = 0; i < numHashes; i++) {
    const hash = ((h1 + i * h2) >>> 0) % filterBits
    hashes.push(hash)
  }

  return hashes
}

/**
 * Encode a value to bytes for bloom filter hashing
 */
function encodeValueForBloom(value) {
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

/**
 * Calculate optimal bloom filter size for a given number of expected items
 * Uses the formula: m = -n * ln(p) / (ln(2))^2
 * Where m = bits, n = expected items, p = false positive rate
 */
function calculateOptimalBloomSize(expectedItems, fpr = 0.01) {
  const m = Math.ceil((-expectedItems * Math.log(fpr)) / Math.pow(Math.log(2), 2))
  // Round up to nearest 256 bytes for alignment
  const bytes = Math.ceil(m / 8 / 256) * 256
  // Clamp between 256 bytes (minimum) and 128KB (maximum)
  return Math.min(Math.max(bytes, 256), BLOOM_VALUE_FILTER_SIZE)
}

/**
 * Build a bloom filter from index data
 * Creates a two-level bloom filter:
 * 1. Value bloom - "Does value X exist anywhere in this index?"
 * 2. Row group blooms - "Which row groups might contain value X?"
 *
 * @param {Array} rows - The data rows
 * @param {string} field - Field to index
 * @param {number[]} rowGroupBoundaries - Cumulative row counts per row group
 * @returns {Uint8Array} Serialized bloom filter
 */
function buildBloomFilter(rows, field, rowGroupBoundaries = []) {
  console.log(`  Building bloom filter for ${field}...`)

  // First pass: count unique values to optimize bloom filter size
  const uniqueValues = new Set()
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i]
    const value = getFieldValue(row, field)
    if (value !== undefined && value !== null) {
      uniqueValues.add(String(value))
    }
  }

  const numRowGroups = rowGroupBoundaries.length || 1

  // Calculate optimal value bloom size based on unique values
  const valueFilterSize = calculateOptimalBloomSize(uniqueValues.size, 0.01)
  const valueFilterBits = valueFilterSize * 8

  // For row group blooms, estimate unique values per row group
  // Use smaller bloom size for low cardinality fields
  const avgValuesPerRG = Math.ceil(uniqueValues.size / numRowGroups) * 2 // 2x buffer
  const rgBloomSize = calculateOptimalBloomSize(avgValuesPerRG, 0.05)
  const rgFilterBits = rgBloomSize * 8

  console.log(`  Unique values: ${uniqueValues.size}, value bloom: ${(valueFilterSize / 1024).toFixed(1)}KB, RG bloom: ${(rgBloomSize / 1024).toFixed(1)}KB`)

  // Initialize bloom filter arrays
  const valueBloom = new Uint8Array(valueFilterSize)
  const rowGroupBlooms = []
  for (let i = 0; i < numRowGroups; i++) {
    rowGroupBlooms.push(new Uint8Array(rgBloomSize))
  }

  // Add entries to bloom filters (second pass)
  let entryCount = 0

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i]
    const value = getFieldValue(row, field)

    if (value === undefined || value === null) continue

    const key = encodeValueForBloom(value)
    entryCount++

    // Add to value bloom
    const valueHashes = getBloomHashes(key, BLOOM_NUM_HASH_FUNCTIONS, valueFilterBits)
    for (const hash of valueHashes) {
      const byteIndex = Math.floor(hash / 8)
      const bitIndex = hash % 8
      valueBloom[byteIndex] |= 1 << bitIndex
    }

    // Add to row group bloom
    const rowGroup = getRowGroup(i, rowGroupBoundaries)
    const rgHashes = getBloomHashes(key, BLOOM_NUM_HASH_FUNCTIONS, rgFilterBits)
    for (const hash of rgHashes) {
      const byteIndex = Math.floor(hash / 8)
      const bitIndex = hash % 8
      rowGroupBlooms[rowGroup][byteIndex] |= 1 << bitIndex
    }
  }

  console.log(`  Added ${entryCount} entries (${uniqueValues.size} unique values) to bloom filter`)
  console.log(`  Row group blooms: ${numRowGroups} x ${(rgBloomSize / 1024).toFixed(1)}KB`)

  // Serialize bloom filter
  // Format: [header:16][valueBloom][rowGroupBlooms...]
  const totalSize = BLOOM_FILTER_HEADER_SIZE + valueFilterSize + (rgBloomSize * numRowGroups)
  const buffer = new ArrayBuffer(totalSize)
  const view = new DataView(buffer)
  const bytes = new Uint8Array(buffer)

  let offset = 0

  // Header
  bytes.set(BLOOM_FILTER_MAGIC, offset)
  offset += 4

  view.setUint16(offset, BLOOM_FILTER_VERSION, false)
  offset += 2

  view.setUint16(offset, BLOOM_NUM_HASH_FUNCTIONS, false)
  offset += 2

  view.setUint32(offset, valueFilterSize, false)
  offset += 4

  view.setUint16(offset, numRowGroups, false)
  offset += 2

  view.setUint16(offset, 0, false) // reserved
  offset += 2

  // Value bloom filter
  bytes.set(valueBloom, offset)
  offset += valueFilterSize

  // Row group bloom filters
  for (let i = 0; i < numRowGroups; i++) {
    bytes.set(rowGroupBlooms[i], offset)
    offset += rgBloomSize
  }

  return bytes
}

// =============================================================================
// Key Encoding
// =============================================================================

// Type prefixes matching src/indexes/secondary/key-encoder.ts
const TYPE_NULL = 0x00
const TYPE_BOOL_FALSE = 0x10
const TYPE_BOOL_TRUE = 0x11
const TYPE_NUMBER_NEG = 0x20
const TYPE_NUMBER_POS = 0x21
const TYPE_STRING = 0x30
const TYPE_OBJECT = 0x70

function encodeKey(value) {
  const encoder = new TextEncoder()

  // Null/undefined
  if (value === null || value === undefined) {
    return new Uint8Array([TYPE_NULL])
  }

  // Boolean
  if (typeof value === 'boolean') {
    return new Uint8Array([value ? TYPE_BOOL_TRUE : TYPE_BOOL_FALSE])
  }

  // Number - encode with proper sign handling
  if (typeof value === 'number') {
    const buffer = new ArrayBuffer(9)
    const view = new DataView(buffer)
    const bytes = new Uint8Array(buffer)

    if (value < 0) {
      view.setUint8(0, TYPE_NUMBER_NEG)
      // Invert bits for correct sort order of negative numbers
      view.setFloat64(1, value, false)
      for (let i = 1; i < 9; i++) {
        bytes[i] = bytes[i] ^ 0xff
      }
    } else {
      view.setUint8(0, TYPE_NUMBER_POS)
      view.setFloat64(1, value, false)
    }
    return bytes
  }

  // String
  if (typeof value === 'string') {
    const strBytes = encoder.encode(value)
    const result = new Uint8Array(strBytes.length + 1)
    result[0] = TYPE_STRING
    result.set(strBytes, 1)
    return result
  }

  // Default: JSON encode as object
  const json = encoder.encode(JSON.stringify(value))
  const result = new Uint8Array(json.length + 1)
  result[0] = TYPE_OBJECT
  result.set(json, 1)
  return result
}

function hashKey(key) {
  // FNV-1a hash
  let hash = 2166136261
  for (let i = 0; i < key.length; i++) {
    hash ^= key[i]
    hash = (hash * 16777619) >>> 0
  }
  return hash
}

function compareKeys(a, b) {
  const len = Math.min(a.length, b.length)
  for (let i = 0; i < len; i++) {
    if (a[i] < b[i]) return -1
    if (a[i] > b[i]) return 1
  }
  return a.length - b.length
}

// =============================================================================
// Tokenization with Porter Stemming
// =============================================================================

const STOP_WORDS = new Set([
  'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from',
  'has', 'he', 'in', 'is', 'it', 'its', 'of', 'on', 'or', 'that',
  'the', 'to', 'was', 'were', 'will', 'with'
])

function tokenize(text) {
  return text
    .toLowerCase()
    .replace(/[^\w\s]/g, ' ')
    .split(/\s+/)
    .filter(word => word.length >= 2 && word.length <= 50 && !STOP_WORDS.has(word))
    .map(word => porterStem(word))  // Apply stemming to match search tokenizer
}

// =============================================================================
// Porter Stemmer (matching src/indexes/fts/tokenizer.ts)
// =============================================================================

function porterStem(word) {
  if (word.length <= 2) return word

  let stem = word
  // Step 1a
  if (stem.endsWith('sses')) stem = stem.slice(0, -2)
  else if (stem.endsWith('ies')) stem = stem.slice(0, -2)
  else if (!stem.endsWith('ss') && stem.endsWith('s')) stem = stem.slice(0, -1)

  // Step 1b
  if (stem.endsWith('eed')) {
    if (measureConsonants(stem.slice(0, -3)) > 0) stem = stem.slice(0, -1)
  } else if (stem.endsWith('ed')) {
    const prefix = stem.slice(0, -2)
    if (hasVowel(prefix)) stem = step1bPostProcess(prefix)
  } else if (stem.endsWith('ing')) {
    const prefix = stem.slice(0, -3)
    if (hasVowel(prefix)) stem = step1bPostProcess(prefix)
  }

  // Step 1c
  if (stem.endsWith('y')) {
    const prefix = stem.slice(0, -1)
    if (hasVowel(prefix)) stem = prefix + 'i'
  }

  stem = step2(stem)
  stem = step3(stem)
  stem = step4(stem)

  // Step 5a
  if (stem.endsWith('e')) {
    const prefix = stem.slice(0, -1)
    const m = measureConsonants(prefix)
    if (m > 1 || (m === 1 && !endsWithCVC(prefix))) stem = prefix
  }

  // Step 5b
  if (stem.length > 1 && stem.endsWith('ll') && measureConsonants(stem.slice(0, -1)) > 1) {
    stem = stem.slice(0, -1)
  }

  return stem
}

function step1bPostProcess(stem) {
  if (stem.endsWith('at') || stem.endsWith('bl') || stem.endsWith('iz')) return stem + 'e'
  if (stem.length >= 2) {
    const last = stem[stem.length - 1]
    const secondLast = stem[stem.length - 2]
    if (last === secondLast && isConsonant(stem, stem.length - 1) && !['l', 's', 'z'].includes(last)) {
      return stem.slice(0, -1)
    }
  }
  if (endsWithCVC(stem) && measureConsonants(stem) === 1) return stem + 'e'
  return stem
}

function step2(stem) {
  const suffixes = {
    'ational': 'ate', 'tional': 'tion', 'enci': 'ence', 'anci': 'ance',
    'izer': 'ize', 'abli': 'able', 'alli': 'al', 'entli': 'ent',
    'eli': 'e', 'ousli': 'ous', 'ization': 'ize', 'ation': 'ate',
    'ator': 'ate', 'alism': 'al', 'iveness': 'ive', 'fulness': 'ful',
    'ousness': 'ous', 'aliti': 'al', 'iviti': 'ive', 'biliti': 'ble',
  }
  for (const [suffix, replacement] of Object.entries(suffixes)) {
    if (stem.endsWith(suffix)) {
      const prefix = stem.slice(0, -suffix.length)
      if (measureConsonants(prefix) > 0) return prefix + replacement
    }
  }
  return stem
}

function step3(stem) {
  const suffixes = { 'icate': 'ic', 'ative': '', 'alize': 'al', 'iciti': 'ic', 'ical': 'ic', 'ful': '', 'ness': '' }
  for (const [suffix, replacement] of Object.entries(suffixes)) {
    if (stem.endsWith(suffix)) {
      const prefix = stem.slice(0, -suffix.length)
      if (measureConsonants(prefix) > 0) return prefix + replacement
    }
  }
  return stem
}

function step4(stem) {
  const suffixes = ['al', 'ance', 'ence', 'er', 'ic', 'able', 'ible', 'ant', 'ement', 'ment', 'ent', 'ion', 'ou', 'ism', 'ate', 'iti', 'ous', 'ive', 'ize']
  for (const suffix of suffixes) {
    if (stem.endsWith(suffix)) {
      const prefix = stem.slice(0, -suffix.length)
      if (suffix === 'ion') {
        if ((prefix.endsWith('s') || prefix.endsWith('t')) && measureConsonants(prefix) > 1) return prefix
      } else if (measureConsonants(prefix) > 1) return prefix
    }
  }
  return stem
}

function isVowel(word, index) {
  const c = word[index]
  if ('aeiou'.includes(c)) return true
  if (c === 'y' && index > 0 && !isVowel(word, index - 1)) return true
  return false
}

function isConsonant(word, index) { return !isVowel(word, index) }

function hasVowel(word) {
  for (let i = 0; i < word.length; i++) if (isVowel(word, i)) return true
  return false
}

function measureConsonants(word) {
  let m = 0, i = 0
  while (i < word.length && isConsonant(word, i)) i++
  while (i < word.length) {
    while (i < word.length && isVowel(word, i)) i++
    if (i >= word.length) break
    while (i < word.length && isConsonant(word, i)) i++
    m++
  }
  return m
}

function endsWithCVC(word) {
  const len = word.length
  if (len < 3) return false
  if (isConsonant(word, len - 3) && isVowel(word, len - 2) && isConsonant(word, len - 1)) {
    return !['w', 'x', 'y'].includes(word[len - 1])
  }
  return false
}

// =============================================================================
// Serialization
// =============================================================================

function serializeHashIndex(buckets, entryCount) {
  // Calculate size
  let size = 1 + 4 // version + entry count
  for (const bucket of buckets.values()) {
    for (const entry of bucket) {
      size += 2 + entry.key.length + 2 + entry.docId.length + 4 + 4
    }
  }

  const buffer = new ArrayBuffer(size)
  const view = new DataView(buffer)
  const bytes = new Uint8Array(buffer)
  const encoder = new TextEncoder()

  let offset = 0
  view.setUint8(offset, 1) // version
  offset += 1
  view.setUint32(offset, entryCount, false)
  offset += 4

  for (const bucket of buckets.values()) {
    for (const entry of bucket) {
      view.setUint16(offset, entry.key.length, false)
      offset += 2
      bytes.set(entry.key, offset)
      offset += entry.key.length

      const docIdBytes = encoder.encode(entry.docId)
      view.setUint16(offset, docIdBytes.length, false)
      offset += 2
      bytes.set(docIdBytes, offset)
      offset += docIdBytes.length

      view.setUint32(offset, entry.rowGroup, false)
      offset += 4
      view.setUint32(offset, entry.rowOffset, false)
      offset += 4
    }
  }

  return bytes
}

function serializeSSTIndex(entries) {
  let size = 1 + 4 // version + entry count
  for (const entry of entries) {
    size += 2 + entry.key.length + 2 + entry.docId.length + 4 + 4
  }

  const buffer = new ArrayBuffer(size)
  const view = new DataView(buffer)
  const bytes = new Uint8Array(buffer)
  const encoder = new TextEncoder()

  let offset = 0
  view.setUint8(offset, 1) // version
  offset += 1
  view.setUint32(offset, entries.length, false)
  offset += 4

  for (const entry of entries) {
    view.setUint16(offset, entry.key.length, false)
    offset += 2
    bytes.set(entry.key, offset)
    offset += entry.key.length

    const docIdBytes = encoder.encode(entry.docId)
    view.setUint16(offset, docIdBytes.length, false)
    offset += 2
    bytes.set(docIdBytes, offset)
    offset += docIdBytes.length

    view.setUint32(offset, entry.rowGroup, false)
    offset += 4
    view.setUint32(offset, entry.rowOffset, false)
    offset += 4
  }

  return bytes
}

function serializeFTSIndex(index, docStats, corpusStats) {
  const data = {
    version: 1,
    index: Array.from(index.entries()),
    docStats: Array.from(docStats.entries()).map(([id, stats]) => ({
      docId: id,
      fieldLengths: Array.from(stats.fieldLengths.entries()),
      totalLength: stats.totalLength,
    })),
    corpusStats: {
      documentCount: corpusStats.documentCount,
      avgDocLength: corpusStats.avgDocLength,
      documentFrequency: Array.from(corpusStats.documentFrequency.entries()),
    },
  }

  return new TextEncoder().encode(JSON.stringify(data))
}

// =============================================================================
// Utilities
// =============================================================================

function getFieldValue(obj, path) {
  const parts = path.split('.')
  let current = obj
  for (const part of parts) {
    if (current === null || current === undefined) return undefined
    current = current[part]
  }
  return current
}

/**
 * Calculate which row group a row index belongs to
 * @param {number} rowIndex - Global row index
 * @param {number[]} boundaries - Cumulative row counts [end1, end2, ...]
 * @returns {number} Row group index
 */
function getRowGroup(rowIndex, boundaries) {
  if (!boundaries || boundaries.length === 0) return 0
  for (let rg = 0; rg < boundaries.length; rg++) {
    if (rowIndex < boundaries[rg]) {
      return rg
    }
  }
  return boundaries.length - 1
}

/**
 * Calculate the row offset within a row group
 * @param {number} rowIndex - Global row index
 * @param {number} rowGroup - Row group index
 * @param {number[]} boundaries - Cumulative row counts [end1, end2, ...]
 * @returns {number} Row offset within the row group
 */
function getRowOffset(rowIndex, rowGroup, boundaries) {
  if (!boundaries || boundaries.length === 0) return rowIndex
  const groupStart = rowGroup > 0 ? boundaries[rowGroup - 1] : 0
  return rowIndex - groupStart
}

async function readParquetFile(path) {
  console.log(`Reading ${path}...`)
  const nodeBuffer = readFileSync(path)
  const buffer = nodeBuffer.buffer.slice(nodeBuffer.byteOffset, nodeBuffer.byteOffset + nodeBuffer.byteLength)

  // Get metadata to extract row group boundaries
  const metadata = await parquetMetadataAsync(buffer)
  const rowGroupBoundaries = []
  let cumulativeRows = 0

  if (metadata.row_groups) {
    for (const rg of metadata.row_groups) {
      cumulativeRows += Number(rg.num_rows)
      rowGroupBoundaries.push(cumulativeRows)
    }
    console.log(`  Found ${metadata.row_groups.length} row groups: [${metadata.row_groups.map(rg => Number(rg.num_rows)).join(', ')}] rows each`)
  }

  const rows = []

  await parquetRead({
    rowFormat: 'object',
    file: buffer,
    compressors,
    onComplete: (data) => {
      for (let i = 0; i < data.length; i += 10000) { const chunk = data.slice(i, i + 10000); rows.push(...chunk); }
    },
  })

  console.log(`  Loaded ${rows.length} rows total`)
  return { rows, rowGroupBoundaries }
}

function ensureDir(path) {
  const dir = dirname(path)
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true })
  }
}

/**
 * Build indexes for a single collection
 * @param {string} dataset - Dataset name
 * @param {string} collection - Collection name
 * @param {Array} indexesToBuild - Index definitions to build
 * @param {boolean} useCompact - Use compact v3 encoding
 * @param {boolean} buildBloomFilters - Build bloom filters for pre-filtering
 */
async function buildCollectionIndexes(dataset, collection, indexesToBuild, useCompact = false, buildBloomFilters = false) {
  // Read data file for this collection
  const dataPath = join(DATA_DIR, dataset, `${collection}.parquet`)
  if (!existsSync(dataPath)) {
    console.error(`Data file not found: ${dataPath}`)
    return null
  }

  const { rows, rowGroupBoundaries } = await readParquetFile(dataPath)

  // Unpack data column if present
  const unpackedRows = rows.map(row => {
    if (row.data) {
      const data = typeof row.data === 'string' ? JSON.parse(row.data) : row.data
      return { ...row, ...data }
    }
    return row
  })

  const results = []

  // Build each index
  for (const def of indexesToBuild) {
    console.log(`\nBuilding ${def.type} index: ${collection}/${def.name}`)

    // Estimate size to determine if sharding is needed
    const estimatedSize = estimateIndexSize(unpackedRows.length)
    const shouldShard = estimatedSize > SHARD_THRESHOLD_BYTES && (def.type === 'hash' || def.type === 'sst')

    if (shouldShard) {
      console.log(`  Estimated size ${(estimatedSize / 1024 / 1024).toFixed(1)}MB > ${SHARD_THRESHOLD_BYTES / 1024 / 1024}MB threshold, using sharded format`)

      // Build sharded index
      let shardResult
      if (def.type === 'hash') {
        shardResult = buildShardedHashIndex(unpackedRows, def.field, rowGroupBoundaries, '$id', useCompact)
      } else {
        shardResult = buildShardedSSTIndex(unpackedRows, def.field, rowGroupBoundaries, '$id', useCompact)
      }

      // Write shards to directory (clean up old shards first)
      const shardDir = join(OUTPUT_DIR, dataset, collection, 'indexes', 'secondary', def.name)
      if (existsSync(shardDir)) {
        // Remove entire directory and recreate
        rmSync(shardDir, { recursive: true })
      }
      mkdirSync(shardDir, { recursive: true })

      let totalSizeBytes = 0
      const shardManifest = {
        version: useCompact ? 3 : 2,
        type: shardResult.type,
        field: def.field,
        sharding: shardResult.sharding,
        compact: useCompact,
        shards: [],
        totalEntries: shardResult.totalEntries,
        rowGroups: shardResult.rowGroups,
      }

      for (const shard of shardResult.shards) {
        const shardPath = join(shardDir, `${shard.name}.shard.idx`)
        writeFileSync(shardPath, shard.data)
        totalSizeBytes += shard.sizeBytes

        const shardInfo = {
          name: shard.name,
          path: `${shard.name}.shard.idx`,
          entryCount: shard.entryCount,
          sizeBytes: shard.sizeBytes,
        }

        // Include range info for SST shards
        if (shard.rangeStart !== undefined) {
          shardInfo.rangeStart = shard.rangeStart
          shardInfo.rangeEnd = shard.rangeEnd
        }
        if (shard.prefix !== undefined) {
          shardInfo.prefix = shard.prefix
        }
        if (shard.originalValue !== undefined) {
          shardInfo.value = shard.originalValue
        }

        shardManifest.shards.push(shardInfo)
      }

      // Build bloom filter if requested
      let bloomInfo = null
      if (buildBloomFilters && (def.type === 'hash' || def.type === 'sst')) {
        const bloomData = buildBloomFilter(unpackedRows, def.field, rowGroupBoundaries)
        const bloomPath = join(shardDir, '_bloom.bin')
        writeFileSync(bloomPath, bloomData)
        console.log(`  Wrote bloom filter: ${bloomPath} (${(bloomData.length / 1024).toFixed(1)}KB)`)

        bloomInfo = {
          bloomPath: '_bloom.bin',
          bloomSizeBytes: bloomData.length,
        }
        shardManifest.bloomPath = bloomInfo.bloomPath
        shardManifest.bloomSizeBytes = bloomInfo.bloomSizeBytes
      }

      // Write manifest
      const manifestPath = join(shardDir, '_manifest.json')
      writeFileSync(manifestPath, JSON.stringify(shardManifest, null, 2))

      console.log(`  Wrote ${shardResult.shards.length} shards to ${shardDir}`)
      console.log(`  Total size: ${(totalSizeBytes / 1024 / 1024).toFixed(2)}MB (largest shard: ${(Math.max(...shardResult.shards.map(s => s.sizeBytes)) / 1024 / 1024).toFixed(2)}MB)`)

      const indexResult = {
        name: def.name,
        type: def.type,
        field: def.field,
        sharded: true,
        compact: useCompact,
        manifestPath: `indexes/secondary/${def.name}/_manifest.json`,
        sizeBytes: totalSizeBytes,
        shardCount: shardResult.shards.length,
        entryCount: shardResult.totalEntries,
        rowGroups: rowGroupBoundaries.length || 1,
        updatedAt: new Date().toISOString(),
      }
      if (bloomInfo) {
        indexResult.bloomPath = `indexes/secondary/${def.name}/${bloomInfo.bloomPath}`
        indexResult.bloomSizeBytes = bloomInfo.bloomSizeBytes
      }
      results.push(indexResult)
    } else {
      // Build non-sharded index (original logic)
      let indexData
      let outputPath

      switch (def.type) {
        case 'hash':
          indexData = buildHashIndex(unpackedRows, def.field, rowGroupBoundaries)
          outputPath = join(OUTPUT_DIR, dataset, collection, 'indexes', 'secondary', `${def.name}.hash.idx`)
          break
        case 'sst':
          indexData = buildSSTIndex(unpackedRows, def.field, rowGroupBoundaries)
          outputPath = join(OUTPUT_DIR, dataset, collection, 'indexes', 'secondary', `${def.name}.sst.idx`)
          break
        case 'fts':
          indexData = buildFTSIndex(unpackedRows, def.field)
          outputPath = join(OUTPUT_DIR, dataset, collection, 'indexes', 'fts', `${def.name}.fts.json`)
          break
        default:
          console.error(`Unknown index type: ${def.type}`)
          continue
      }

      // Write index file
      ensureDir(outputPath)
      writeFileSync(outputPath, indexData)
      console.log(`  Wrote ${outputPath} (${indexData.length} bytes)`)

      results.push({
        name: def.name,
        type: def.type,
        field: def.field,
        path: outputPath.replace(`${OUTPUT_DIR}/${dataset}/${collection}/`, ''),
        sizeBytes: indexData.length,
        entryCount: unpackedRows.length,
        rowGroups: rowGroupBoundaries.length || 1,
        updatedAt: new Date().toISOString(),
      })
    }
  }

  return results
}

// =============================================================================
// Main
// =============================================================================

async function main() {
  const args = process.argv.slice(2)

  // Parse arguments
  let dataset = null
  let collection = null
  let field = null
  let type = null
  let buildAll = false
  let useCompact = false
  let buildBloom = false

  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--dataset' && args[i + 1]) {
      dataset = args[++i]
    } else if (args[i] === '--collection' && args[i + 1]) {
      collection = args[++i]
    } else if (args[i] === '--field' && args[i + 1]) {
      field = args[++i]
    } else if (args[i] === '--type' && args[i + 1]) {
      type = args[++i]
    } else if (args[i] === '--all') {
      buildAll = true
    } else if (args[i] === '--compact') {
      useCompact = true
    } else if (args[i] === '--bloom') {
      buildBloom = true
    }
  }

  if (useCompact) {
    console.log('Using compact encoding (v3 format) for ~3x smaller indexes')
  }
  if (buildBloom) {
    console.log('Building bloom filters for fast pre-filtering')
  }

  if (!dataset) {
    console.error('Usage: node scripts/build-indexes.mjs --dataset <name> [--collection <name>] [--field <field> --type <type>] [--all]')
    console.error('\nAvailable datasets:')
    for (const [d, collections] of Object.entries(DATASET_FILES)) {
      console.error(`  ${d}:`)
      for (const c of collections) {
        console.error(`    - ${c}`)
      }
    }
    process.exit(1)
  }

  const datasetDefs = INDEX_DEFINITIONS[dataset]
  if (!datasetDefs) {
    console.error(`Unknown dataset: ${dataset}`)
    console.error('\nAvailable datasets:', Object.keys(INDEX_DEFINITIONS).join(', '))
    process.exit(1)
  }

  const datasetFiles = DATASET_FILES[dataset]
  if (!datasetFiles) {
    console.error(`No files defined for dataset: ${dataset}`)
    process.exit(1)
  }

  // Determine which collections to process
  const collectionsToProcess = collection
    ? [collection]
    : (buildAll ? datasetFiles : [])

  if (collectionsToProcess.length === 0) {
    console.error('Please specify --collection <name> or use --all to build all collections')
    console.error('\nAvailable collections for this dataset:')
    for (const c of datasetFiles) {
      console.error(`  ${c}`)
    }
    process.exit(1)
  }

  // Global catalog for the dataset
  const globalCatalog = {
    version: 1,
    dataset,
    collections: {},
    updatedAt: new Date().toISOString(),
  }

  let totalIndexesBuilt = 0

  for (const col of collectionsToProcess) {
    const collectionDefs = datasetDefs[col]
    if (!collectionDefs) {
      console.warn(`\nNo index definitions for collection: ${col}, skipping...`)
      continue
    }

    // Determine which indexes to build for this collection
    const indexesToBuild = buildAll
      ? collectionDefs
      : collectionDefs.filter(d => d.field === field && d.type === type)

    if (indexesToBuild.length === 0) {
      if (!buildAll) {
        console.error(`No matching index definition found for collection=${col} field=${field} type=${type}`)
        console.error('\nAvailable indexes for this collection:')
        for (const d of collectionDefs) {
          console.error(`  --field ${d.field} --type ${d.type}`)
        }
      }
      continue
    }

    console.log(`\n${'='.repeat(60)}`)
    console.log(`Processing collection: ${col}`)
    console.log(`${'='.repeat(60)}`)

    const results = await buildCollectionIndexes(dataset, col, indexesToBuild, useCompact, buildBloom)

    if (results && results.length > 0) {
      // Write collection-level catalog
      const collectionCatalog = {
        version: 1,
        collection: col,
        indexes: results,
        updatedAt: new Date().toISOString(),
      }

      const catalogPath = join(OUTPUT_DIR, dataset, col, 'indexes', '_catalog.json')
      ensureDir(catalogPath)
      writeFileSync(catalogPath, JSON.stringify(collectionCatalog, null, 2))
      console.log(`\nWrote collection catalog: ${catalogPath}`)

      // Add to global catalog
      globalCatalog.collections[col] = {
        indexes: results,
        catalogPath: `${col}/indexes/_catalog.json`,
      }

      totalIndexesBuilt += results.length
    }
  }

  // Write global dataset catalog
  if (totalIndexesBuilt > 0) {
    const globalCatalogPath = join(OUTPUT_DIR, dataset, '_catalog.json')
    ensureDir(globalCatalogPath)
    writeFileSync(globalCatalogPath, JSON.stringify(globalCatalog, null, 2))
    console.log(`\nWrote global catalog: ${globalCatalogPath}`)
  }

  console.log(`\n${'='.repeat(60)}`)
  console.log(`Done! Built ${totalIndexesBuilt} index(es) across ${collectionsToProcess.length} collection(s).`)
  console.log(`${'='.repeat(60)}`)
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
