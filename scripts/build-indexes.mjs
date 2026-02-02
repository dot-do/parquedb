#!/usr/bin/env node
/**
 * Build FTS and Bloom Filter Indexes for ParqueDB
 *
 * Builds full-text search (FTS) indexes and bloom filters from local Parquet data files.
 * Supports multi-file datasets with per-collection index definitions.
 *
 * Usage:
 *   node scripts/build-indexes.mjs --dataset imdb-1m --collection titles --field name --type fts
 *   node scripts/build-indexes.mjs --dataset onet-full --collection occupations --all  # Build all FTS indexes for collection
 *   node scripts/build-indexes.mjs --dataset imdb-1m --all  # Build all FTS indexes for all collections in dataset
 *   node scripts/build-indexes.mjs --dataset imdb-1m --all --bloom  # Include bloom filters
 */

import { readFileSync, writeFileSync, mkdirSync, existsSync } from 'fs'
import { join, dirname } from 'path'
import { parquetRead, parquetMetadataAsync } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'

// =============================================================================
// Configuration
// =============================================================================

const DATA_DIR = 'data-v3'  // Local data directory
const OUTPUT_DIR = 'data-v3'  // Output to same directory structure

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

// Index definitions per dataset and collection (FTS only)
const INDEX_DEFINITIONS = {
  'imdb': {
    titles: [
      { name: 'name', field: 'name', type: 'fts' },
    ],
    people: [
      { name: 'name', field: 'name', type: 'fts' },
    ],
  },
  'imdb-1m': {
    titles: [
      { name: 'name', field: 'name', type: 'fts' },
    ],
    people: [
      { name: 'name', field: 'name', type: 'fts' },
    ],
  },
  'onet-full': {
    occupations: [
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
  },
  'unspsc-full': {
    commodities: [
      { name: 'title', field: 'name', type: 'fts' },
    ],
    classes: [
      { name: 'title', field: 'name', type: 'fts' },
    ],
    families: [
      { name: 'title', field: 'name', type: 'fts' },
    ],
    segments: [
      { name: 'title', field: 'name', type: 'fts' },
    ],
  },
}

// =============================================================================
// Index Building
// =============================================================================

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
 * @param {boolean} buildBloomFilters - Build bloom filters for pre-filtering
 */
async function buildCollectionIndexes(dataset, collection, indexesToBuild, buildBloomFilters = false) {
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

    if (def.type !== 'fts') {
      console.error(`  Unsupported index type: ${def.type} (only 'fts' is supported)`)
      continue
    }

    // Build FTS index
    const indexData = buildFTSIndex(unpackedRows, def.field)
    const outputPath = join(OUTPUT_DIR, dataset, collection, 'indexes', 'fts', `${def.name}.fts.json`)

    // Write index file
    ensureDir(outputPath)
    writeFileSync(outputPath, indexData)
    console.log(`  Wrote ${outputPath} (${indexData.length} bytes)`)

    // Build bloom filter if requested
    let bloomInfo = null
    if (buildBloomFilters) {
      const bloomData = buildBloomFilter(unpackedRows, def.field, rowGroupBoundaries)
      const bloomDir = join(OUTPUT_DIR, dataset, collection, 'indexes', 'fts')
      const bloomPath = join(bloomDir, `${def.name}.bloom.bin`)
      ensureDir(bloomPath)
      writeFileSync(bloomPath, bloomData)
      console.log(`  Wrote bloom filter: ${bloomPath} (${(bloomData.length / 1024).toFixed(1)}KB)`)

      bloomInfo = {
        bloomPath: `indexes/fts/${def.name}.bloom.bin`,
        bloomSizeBytes: bloomData.length,
      }
    }

    const result = {
      name: def.name,
      type: def.type,
      field: def.field,
      path: outputPath.replace(`${OUTPUT_DIR}/${dataset}/${collection}/`, ''),
      sizeBytes: indexData.length,
      entryCount: unpackedRows.length,
      rowGroups: rowGroupBoundaries.length || 1,
      updatedAt: new Date().toISOString(),
    }

    if (bloomInfo) {
      result.bloomPath = bloomInfo.bloomPath
      result.bloomSizeBytes = bloomInfo.bloomSizeBytes
    }

    results.push(result)
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
    } else if (args[i] === '--bloom') {
      buildBloom = true
    }
  }

  if (buildBloom) {
    console.log('Building bloom filters for fast pre-filtering')
  }

  if (!dataset) {
    console.error('Usage: node scripts/build-indexes.mjs --dataset <name> [--collection <name>] [--field <field> --type fts] [--all] [--bloom]')
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

    const results = await buildCollectionIndexes(dataset, col, indexesToBuild, buildBloom)

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
