#!/usr/bin/env node
/**
 * Build Secondary Indexes for ParqueDB
 *
 * Builds hash, SST, and FTS indexes from local Parquet data files.
 *
 * Usage:
 *   node scripts/build-indexes.mjs --dataset imdb-1m --field $index_titleType --type hash
 *   node scripts/build-indexes.mjs --dataset imdb-1m --field $index_startYear --type sst
 *   node scripts/build-indexes.mjs --dataset imdb-1m --field name --type fts
 *   node scripts/build-indexes.mjs --dataset imdb-1m --all  # Build all indexes for dataset
 */

import { readFileSync, writeFileSync, mkdirSync, existsSync, readdirSync } from 'fs'
import { join, dirname } from 'path'
import { parquetRead } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'

// =============================================================================
// Configuration
// =============================================================================

const DATA_DIR = 'data-v3'  // Local data directory
const OUTPUT_DIR = 'data-v3'  // Output to same directory structure

// Index definitions per dataset
const INDEX_DEFINITIONS = {
  'imdb-1m': [
    { name: 'titleType', field: '$index_titleType', type: 'hash' },
    { name: 'startYear', field: '$index_startYear', type: 'sst' },
    { name: 'averageRating', field: '$index_averageRating', type: 'sst' },
    { name: 'numVotes', field: '$index_numVotes', type: 'sst' },
    { name: 'name', field: 'name', type: 'fts' },
  ],
  'onet-full': [
    { name: 'jobZone', field: '$index_jobZone', type: 'hash' },
    { name: 'socCode', field: '$index_socCode', type: 'sst' },
    { name: 'dataValue', field: '$index_dataValue', type: 'sst' },
    { name: 'scaleId', field: '$index_scaleId', type: 'hash' },
    { name: 'title', field: 'title', type: 'fts' },
  ],
  'unspsc-full': [
    { name: 'segmentCode', field: '$index_segmentCode', type: 'hash' },
    { name: 'code', field: '$index_code', type: 'sst' },
    { name: 'title', field: 'title', type: 'fts' },
  ],
}

// =============================================================================
// Index Building
// =============================================================================

/**
 * Build a hash index from Parquet data
 */
function buildHashIndex(rows, field, idField = '$id') {
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

    buckets.get(hash).push({
      key,
      docId: String(docId),
      rowGroup: 0,
      rowOffset: i,
    })
    entryCount++
  }

  console.log(`  Created ${entryCount} entries in ${buckets.size} buckets`)
  return serializeHashIndex(buckets, entryCount)
}

/**
 * Build an SST index from Parquet data
 */
function buildSSTIndex(rows, field, idField = '$id') {
  console.log(`  Building SST index for ${field}...`)
  const entries = []

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i]
    const value = getFieldValue(row, field)
    const docId = getFieldValue(row, idField) || `row_${i}`

    if (value === undefined || value === null) continue

    entries.push({
      key: encodeKey(value),
      docId: String(docId),
      rowGroup: 0,
      rowOffset: i,
    })
  }

  // Sort by key
  entries.sort((a, b) => compareKeys(a.key, b.key))

  console.log(`  Created ${entries.length} sorted entries`)
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
// Key Encoding
// =============================================================================

function encodeKey(value) {
  const encoder = new TextEncoder()

  if (typeof value === 'string') {
    const bytes = encoder.encode(value)
    const result = new Uint8Array(bytes.length + 1)
    result[0] = 1 // String type marker
    result.set(bytes, 1)
    return result
  }

  if (typeof value === 'number') {
    const buffer = new ArrayBuffer(9)
    const view = new DataView(buffer)
    view.setUint8(0, 2) // Number type marker
    view.setFloat64(1, value, false)
    return new Uint8Array(buffer)
  }

  if (typeof value === 'boolean') {
    return new Uint8Array([3, value ? 1 : 0])
  }

  // Default: JSON encode
  const json = encoder.encode(JSON.stringify(value))
  const result = new Uint8Array(json.length + 1)
  result[0] = 0 // JSON type marker
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
// Tokenization
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

async function readParquetFile(path) {
  console.log(`Reading ${path}...`)
  const buffer = readFileSync(path)
  const rows = []

  await parquetRead({
    file: buffer,
    compressors,
    onComplete: (data) => {
      rows.push(...data)
    },
  })

  console.log(`  Loaded ${rows.length} rows`)
  return rows
}

function ensureDir(path) {
  const dir = dirname(path)
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true })
  }
}

// =============================================================================
// Main
// =============================================================================

async function main() {
  const args = process.argv.slice(2)

  // Parse arguments
  let dataset = null
  let field = null
  let type = null
  let buildAll = false

  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--dataset' && args[i + 1]) {
      dataset = args[++i]
    } else if (args[i] === '--field' && args[i + 1]) {
      field = args[++i]
    } else if (args[i] === '--type' && args[i + 1]) {
      type = args[++i]
    } else if (args[i] === '--all') {
      buildAll = true
    }
  }

  if (!dataset) {
    console.error('Usage: node scripts/build-indexes.mjs --dataset <name> [--field <field> --type <type>] [--all]')
    console.error('\nAvailable datasets:')
    for (const d of Object.keys(INDEX_DEFINITIONS)) {
      console.error(`  ${d}`)
    }
    process.exit(1)
  }

  const definitions = INDEX_DEFINITIONS[dataset]
  if (!definitions) {
    console.error(`Unknown dataset: ${dataset}`)
    process.exit(1)
  }

  // Read data file
  const dataPath = join(DATA_DIR, dataset, 'data.parquet')
  if (!existsSync(dataPath)) {
    console.error(`Data file not found: ${dataPath}`)
    process.exit(1)
  }

  const rows = await readParquetFile(dataPath)

  // Unpack data column if present
  const unpackedRows = rows.map(row => {
    if (row.data) {
      const data = typeof row.data === 'string' ? JSON.parse(row.data) : row.data
      return { ...row, ...data }
    }
    return row
  })

  // Determine which indexes to build
  const indexesToBuild = buildAll
    ? definitions
    : definitions.filter(d => d.field === field && d.type === type)

  if (indexesToBuild.length === 0) {
    console.error(`No matching index definition found for field=${field} type=${type}`)
    console.error('\nAvailable indexes for this dataset:')
    for (const d of definitions) {
      console.error(`  --field ${d.field} --type ${d.type}`)
    }
    process.exit(1)
  }

  // Build catalog
  const catalog = {
    version: 1,
    indexes: [],
  }

  // Build each index
  for (const def of indexesToBuild) {
    console.log(`\nBuilding ${def.type} index: ${def.name}`)

    let indexData
    let outputPath

    switch (def.type) {
      case 'hash':
        indexData = buildHashIndex(unpackedRows, def.field)
        outputPath = join(OUTPUT_DIR, dataset, 'indexes', 'secondary', `${def.name}.hash.idx`)
        break
      case 'sst':
        indexData = buildSSTIndex(unpackedRows, def.field)
        outputPath = join(OUTPUT_DIR, dataset, 'indexes', 'secondary', `${def.name}.sst.idx`)
        break
      case 'fts':
        indexData = buildFTSIndex(unpackedRows, def.field)
        outputPath = join(OUTPUT_DIR, dataset, 'indexes', 'fts', `${def.name}.fts.json`)
        break
      default:
        console.error(`Unknown index type: ${def.type}`)
        continue
    }

    // Write index file
    ensureDir(outputPath)
    writeFileSync(outputPath, indexData)
    console.log(`  Wrote ${outputPath} (${indexData.length} bytes)`)

    // Add to catalog
    catalog.indexes.push({
      name: def.name,
      type: def.type,
      field: def.field,
      path: outputPath.replace(`${OUTPUT_DIR}/${dataset}/`, ''),
      sizeBytes: indexData.length,
      entryCount: unpackedRows.length,
      updatedAt: new Date().toISOString(),
    })
  }

  // Write catalog
  const catalogPath = join(OUTPUT_DIR, dataset, 'indexes', '_catalog.json')
  ensureDir(catalogPath)
  writeFileSync(catalogPath, JSON.stringify(catalog, null, 2))
  console.log(`\nWrote catalog: ${catalogPath}`)
  console.log(`\nDone! Built ${indexesToBuild.length} index(es).`)
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
