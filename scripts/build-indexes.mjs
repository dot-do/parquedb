#!/usr/bin/env node
/**
 * Build Secondary Indexes for ParqueDB
 *
 * Builds hash, SST, and FTS indexes from local Parquet data files.
 * Supports multi-file datasets with per-collection index definitions.
 *
 * Usage:
 *   node scripts/build-indexes.mjs --dataset imdb-1m --collection titles --field $index_titleType --type hash
 *   node scripts/build-indexes.mjs --dataset imdb-1m --collection titles --field $index_startYear --type sst
 *   node scripts/build-indexes.mjs --dataset imdb-1m --collection titles --field name --type fts
 *   node scripts/build-indexes.mjs --dataset onet-full --collection occupations --all  # Build all indexes for collection
 *   node scripts/build-indexes.mjs --dataset imdb-1m --all  # Build all indexes for all collections in dataset
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

// Dataset files mapping - lists all parquet files per dataset
const DATASET_FILES = {
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
  const nodeBuffer = readFileSync(path)
  const buffer = nodeBuffer.buffer.slice(nodeBuffer.byteOffset, nodeBuffer.byteOffset + nodeBuffer.byteLength)
  const rows = []

  await parquetRead({
    rowFormat: 'object',
    file: buffer,
    compressors,
    onComplete: (data) => {
      for (let i = 0; i < data.length; i += 10000) { const chunk = data.slice(i, i + 10000); rows.push(...chunk); }
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

/**
 * Build indexes for a single collection
 */
async function buildCollectionIndexes(dataset, collection, indexesToBuild) {
  // Read data file for this collection
  const dataPath = join(DATA_DIR, dataset, `${collection}.parquet`)
  if (!existsSync(dataPath)) {
    console.error(`Data file not found: ${dataPath}`)
    return null
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

  const results = []

  // Build each index
  for (const def of indexesToBuild) {
    console.log(`\nBuilding ${def.type} index: ${collection}/${def.name}`)

    let indexData
    let outputPath

    switch (def.type) {
      case 'hash':
        indexData = buildHashIndex(unpackedRows, def.field)
        outputPath = join(OUTPUT_DIR, dataset, collection, 'indexes', 'secondary', `${def.name}.hash.idx`)
        break
      case 'sst':
        indexData = buildSSTIndex(unpackedRows, def.field)
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
      updatedAt: new Date().toISOString(),
    })
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
    }
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

    const results = await buildCollectionIndexes(dataset, col, indexesToBuild)

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
