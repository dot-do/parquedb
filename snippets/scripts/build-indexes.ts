#!/usr/bin/env tsx
/**
 * Build Pre-Computed Indexes for Snippet-Optimized Search (v2)
 *
 * This script generates optimized indexes for sub-5ms CPU queries:
 *
 * v1 structure (kept for fallback):
 * /indexes/{dataset}/
 *   ├── inverted.json       # Full inverted index
 *
 * v2 structure (new, optimized):
 * /indexes/{dataset}/
 *   ├── meta.json           # Dataset metadata (version: 2)
 *   ├── terms/              # Sharded term indexes (~20KB each vs 500KB full)
 *   │   ├── a.json          # Terms starting with 'a'
 *   │   ├── b.json          # Terms starting with 'b'
 *   │   └── ...
 *   ├── hash-{field}.json   # Hash indexes for exact lookups
 *   └── docs-{N}.json       # Document shards (500 docs each)
 *
 * Key optimizations:
 * - Sharded term indexes: Only load ~20KB instead of full 500KB index
 * - Smaller shards: 500 docs/shard for faster JSON.parse
 * - No prefix indexes: Reduces size, exact term matching only
 */

import { readFile, writeFile, mkdir, rm } from 'fs/promises'
import { existsSync } from 'fs'
import { join } from 'path'

// =============================================================================
// Configuration
// =============================================================================

const SHARD_SIZE = 500 // Documents per shard
const MIN_TERM_LENGTH = 2 // Minimum term length to index
const INDEX_VERSION = 2 // v2 indexes

interface DatasetConfig {
  sourceFile: string
  textFields: string[] // Fields for inverted index
  hashFields: string[] // Fields for exact lookup
  idField: string // Primary ID field
}

const DATASETS: Record<string, DatasetConfig> = {
  onet: {
    sourceFile: 'onet-occupations.json',
    textFields: ['title', 'description'],
    hashFields: ['code'],
    idField: 'code',
  },
  unspsc: {
    sourceFile: 'unspsc.json',
    textFields: ['commodityTitle', 'classTitle', 'familyTitle', 'segmentTitle'],
    hashFields: ['commodityCode', 'classCode', 'familyCode', 'segmentCode'],
    idField: 'commodityCode',
  },
  imdb: {
    sourceFile: 'imdb-titles.json',
    textFields: ['primaryTitle', 'originalTitle'],
    hashFields: ['tconst', 'titleType', 'startYear'],
    idField: 'tconst',
  },
}

// =============================================================================
// Tokenization
// =============================================================================

/**
 * Tokenize text into searchable terms
 */
function tokenize(text: string): string[] {
  if (!text || typeof text !== 'string') return []

  const terms = text
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter((t) => t.length >= MIN_TERM_LENGTH)

  return [...new Set(terms)]
}

/**
 * Get shard key for a term (first letter, or '0' for digits)
 */
function getTermShardKey(term: string): string {
  const first = term[0] || ''
  return /[0-9]/.test(first) ? '0' : first
}

// =============================================================================
// Types
// =============================================================================

interface InvertedIndex {
  [term: string]: number[]
}

interface HashIndex {
  [value: string]: number[]
}

interface DatasetMeta {
  name: string
  version: number
  totalDocs: number
  shardSize: number
  shardCount: number
  textFields: string[]
  hashFields: string[]
  idField: string
  termShards: string[] // List of term shard keys (a, b, c, ..., 0)
  buildTime: string
}

// =============================================================================
// Index Building
// =============================================================================

async function buildDatasetIndexes(
  name: string,
  config: DatasetConfig,
  dataDir: string,
  outputDir: string
): Promise<void> {
  console.log(`\n${'='.repeat(60)}`)
  console.log(`Building v2 indexes for: ${name}`)
  console.log(`${'='.repeat(60)}`)

  // Load source data
  const sourcePath = join(dataDir, config.sourceFile)
  console.log(`Loading: ${sourcePath}`)

  const sourceData = JSON.parse(await readFile(sourcePath, 'utf-8')) as Record<string, unknown>[]
  console.log(`Loaded ${sourceData.length} documents`)

  // Create output directories
  const datasetOutputDir = join(outputDir, name)
  const termsDir = join(datasetOutputDir, 'terms')

  if (existsSync(termsDir)) {
    await rm(termsDir, { recursive: true })
  }
  await mkdir(termsDir, { recursive: true })

  // Build sharded inverted indexes
  console.log(`\nBuilding sharded term indexes for fields: ${config.textFields.join(', ')}`)
  const termShards = new Map<string, InvertedIndex>()
  const fullInverted: InvertedIndex = {} // Also keep full index for v1 fallback

  for (let i = 0; i < sourceData.length; i++) {
    const doc = sourceData[i]!

    for (const field of config.textFields) {
      const value = doc[field]
      if (!value) continue

      // Handle string fields
      if (typeof value === 'string') {
        const terms = tokenize(value)
        for (const term of terms) {
          // Add to sharded index
          const shardKey = getTermShardKey(term)
          if (!termShards.has(shardKey)) {
            termShards.set(shardKey, {})
          }
          const shard = termShards.get(shardKey)!
          if (!shard[term]) shard[term] = []
          if (!shard[term].includes(i)) shard[term].push(i)

          // Also add to full index (v1 fallback)
          if (!fullInverted[term]) fullInverted[term] = []
          if (!fullInverted[term].includes(i)) fullInverted[term].push(i)
        }
      }

      // Handle array fields (like genres)
      if (Array.isArray(value)) {
        for (const item of value) {
          if (typeof item === 'string') {
            const terms = tokenize(item)
            for (const term of terms) {
              const shardKey = getTermShardKey(term)
              if (!termShards.has(shardKey)) {
                termShards.set(shardKey, {})
              }
              const shard = termShards.get(shardKey)!
              if (!shard[term]) shard[term] = []
              if (!shard[term].includes(i)) shard[term].push(i)

              if (!fullInverted[term]) fullInverted[term] = []
              if (!fullInverted[term].includes(i)) fullInverted[term].push(i)
            }
          }
        }
      }
    }
  }

  // Write sharded term indexes
  console.log(`\nWriting ${termShards.size} term shards:`)
  const shardKeys: string[] = []
  let totalTermShardSize = 0

  for (const [shardKey, index] of termShards) {
    const shardPath = join(termsDir, `${shardKey}.json`)
    const content = JSON.stringify(index)
    await writeFile(shardPath, content)
    shardKeys.push(shardKey)
    totalTermShardSize += content.length
    console.log(
      `  ${shardKey}.json: ${Object.keys(index).length} terms, ${(content.length / 1024).toFixed(1)} KB`
    )
  }

  console.log(`Total term shards: ${(totalTermShardSize / 1024).toFixed(1)} KB`)

  // Write full inverted index (v1 fallback)
  const fullInvertedPath = join(datasetOutputDir, 'inverted.json')
  const fullContent = JSON.stringify(fullInverted)
  await writeFile(fullInvertedPath, fullContent)
  console.log(`\nWrote v1 fallback inverted.json: ${(fullContent.length / 1024).toFixed(1)} KB`)

  // Build hash indexes
  const hashIndexes: Record<string, HashIndex> = {}

  for (const field of config.hashFields) {
    console.log(`Building hash index for field: ${field}`)
    const index: HashIndex = {}

    for (let i = 0; i < sourceData.length; i++) {
      const doc = sourceData[i]!
      const value = doc[field]
      if (value === undefined || value === null) continue

      const key = String(value)
      if (!index[key]) index[key] = []
      index[key].push(i)
    }

    hashIndexes[field] = index
    console.log(`  Indexed ${Object.keys(index).length} unique values`)
  }

  // Write hash indexes
  for (const [field, index] of Object.entries(hashIndexes)) {
    const hashPath = join(datasetOutputDir, `hash-${field}.json`)
    const content = JSON.stringify(index)
    await writeFile(hashPath, content)
    console.log(`Wrote hash-${field}.json: ${(content.length / 1024).toFixed(1)} KB`)
  }

  // Create document shards
  console.log(`\nCreating document shards (${SHARD_SIZE} docs each)`)
  const shardCount = Math.ceil(sourceData.length / SHARD_SIZE)

  for (let shard = 0; shard < shardCount; shard++) {
    const start = shard * SHARD_SIZE
    const end = Math.min(start + SHARD_SIZE, sourceData.length)
    const shardDocs = sourceData.slice(start, end)

    const shardPath = join(datasetOutputDir, `docs-${shard}.json`)
    await writeFile(shardPath, JSON.stringify(shardDocs))
    console.log(`  Shard ${shard}: rows ${start}-${end - 1} (${shardDocs.length} docs)`)
  }

  // Write metadata (v2)
  const meta: DatasetMeta = {
    name,
    version: INDEX_VERSION,
    totalDocs: sourceData.length,
    shardSize: SHARD_SIZE,
    shardCount,
    textFields: config.textFields,
    hashFields: config.hashFields,
    idField: config.idField,
    termShards: shardKeys.sort(),
    buildTime: new Date().toISOString(),
  }

  await writeFile(join(datasetOutputDir, 'meta.json'), JSON.stringify(meta, null, 2))
  console.log(`\nWrote metadata (v${INDEX_VERSION})`)

  // Summary
  console.log(`\n--- ${name} Summary ---`)
  console.log(`  Documents: ${sourceData.length}`)
  console.log(`  Doc shards: ${shardCount}`)
  console.log(`  Term shards: ${termShards.size}`)
  console.log(`  Unique terms: ${Object.keys(fullInverted).length}`)
  console.log(`  v1 index size: ${(fullContent.length / 1024).toFixed(1)} KB`)
  console.log(`  v2 max shard: ${(Math.max(...[...termShards.values()].map((s) => JSON.stringify(s).length)) / 1024).toFixed(1)} KB`)
}

// =============================================================================
// Main
// =============================================================================

async function main() {
  const args = process.argv.slice(2)

  // Default paths
  let dataDir = process.env.DATA_DIR || './data'
  let outputDir = process.env.OUTPUT_DIR || './indexes'

  // Parse args
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--data' && args[i + 1]) {
      dataDir = args[i + 1]!
      i++
    } else if (args[i] === '--output' && args[i + 1]) {
      outputDir = args[i + 1]!
      i++
    }
  }

  console.log('Pre-Computed Index Builder (v2)')
  console.log('================================')
  console.log(`Data directory: ${dataDir}`)
  console.log(`Output directory: ${outputDir}`)
  console.log(`Index version: ${INDEX_VERSION}`)

  // Create output directory
  if (!existsSync(outputDir)) {
    await mkdir(outputDir, { recursive: true })
  }

  // Build indexes for each dataset
  for (const [name, config] of Object.entries(DATASETS)) {
    try {
      await buildDatasetIndexes(name, config, dataDir, outputDir)
    } catch (error) {
      console.error(`Error building ${name}:`, error)
    }
  }

  console.log('\n' + '='.repeat(60))
  console.log('Index building complete (v2)!')
  console.log('='.repeat(60))
  console.log('\nTo upload indexes to R2:')
  console.log('  wrangler r2 object put parquedb-search-data/indexes/ --local-folder=./indexes --remote')
}

main().catch(console.error)
