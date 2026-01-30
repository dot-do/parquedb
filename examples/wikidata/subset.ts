#!/usr/bin/env node
/**
 * Wikidata Subset Extractor
 *
 * Extracts specific entity types from the Wikidata dump.
 * Useful for creating focused datasets (all humans, all cities, etc.)
 * without processing the entire 100GB+ dump.
 *
 * Usage:
 *   npx tsx examples/wikidata/subset.ts <dump-file> <output-dir> --type <Q-IDs>
 *
 * Options:
 *   --type <ids>         Comma-separated Q-IDs to extract (e.g., Q5,Q515)
 *   --type-name <names>  Predefined type names (human,city,country,film,book)
 *   --include-related    Also include entities referenced by extracted items
 *   --related-depth <n>  Depth of related entity traversal (default: 1)
 *   --batch-size <n>     Items per batch (default: 10000)
 *   --max-items <n>      Maximum items to extract
 *   --verbose            Enable verbose logging
 *
 * Examples:
 *   # Extract all humans
 *   npx tsx subset.ts dump.json.gz ./humans-db --type Q5
 *
 *   # Extract all cities and countries
 *   npx tsx subset.ts dump.json.gz ./locations-db --type Q515,Q6256
 *
 *   # Use predefined type names
 *   npx tsx subset.ts dump.json.gz ./movies-db --type-name film
 *
 *   # Extract humans with related entities (birthplaces, etc.)
 *   npx tsx subset.ts dump.json.gz ./humans-full-db --type Q5 --include-related
 */

import { createReadStream, existsSync, mkdirSync, writeFileSync, readFileSync } from 'node:fs'
import { createGunzip, createBrotliDecompress } from 'node:zlib'
import { Transform, Writable, pipeline } from 'node:stream'
import { promisify } from 'node:util'
import { basename, join } from 'node:path'
import {
  WikidataEntity,
  Claim,
  ENTITY_TYPES,
  extractPrimaryType,
  entityToItemRecord,
  entityToPropertyRecord,
  claimToRecord,
  claimToEdgeRecord,
  getEnglishLabel,
  ITEMS_PARQUET_SCHEMA,
  PROPERTIES_PARQUET_SCHEMA,
  CLAIMS_PARQUET_SCHEMA,
  wikidataSchema,
} from './schema.js'

const pipelineAsync = promisify(pipeline)

// =============================================================================
// Configuration
// =============================================================================

interface SubsetConfig {
  dumpFile: string
  outputDir: string
  targetTypes: Set<string>
  includeRelated: boolean
  relatedDepth: number
  batchSize: number
  maxItems: number | null
  verbose: boolean
}

/** Predefined type name mappings */
const TYPE_NAME_MAP: Record<string, string[]> = {
  human: ['Q5'],
  person: ['Q5'],
  city: ['Q515'],
  country: ['Q6256'],
  location: ['Q515', 'Q6256', 'Q7275', 'Q5107'],
  film: ['Q11424'],
  movie: ['Q11424'],
  book: ['Q571'],
  album: ['Q482994'],
  song: ['Q7366'],
  game: ['Q7889'],
  videogame: ['Q7889'],
  company: ['Q4830453'],
  organization: ['Q43229', 'Q4830453'],
  university: ['Q3918'],
  scientific: ['Q13442814'],
  article: ['Q13442814'],
  taxon: ['Q16521'],
  species: ['Q16521'],
}

function parseArgs(): SubsetConfig {
  const args = process.argv.slice(2)

  if (args.length < 2) {
    console.error('Usage: npx tsx subset.ts <dump-file> <output-dir> --type <Q-IDs>')
    console.error('')
    console.error('Options:')
    console.error('  --type <ids>         Comma-separated Q-IDs (e.g., Q5,Q515)')
    console.error('  --type-name <names>  Predefined names (human,city,country,film,book)')
    console.error('  --include-related    Include referenced entities')
    console.error('  --related-depth <n>  Related entity depth (default: 1)')
    console.error('  --batch-size <n>     Items per batch (default: 10000)')
    console.error('  --max-items <n>      Maximum items to extract')
    console.error('  --verbose            Enable verbose logging')
    console.error('')
    console.error('Predefined type names:')
    for (const [name, types] of Object.entries(TYPE_NAME_MAP)) {
      console.error(`  ${name.padEnd(15)} -> ${types.join(', ')}`)
    }
    process.exit(1)
  }

  const config: SubsetConfig = {
    dumpFile: args[0],
    outputDir: args[1],
    targetTypes: new Set(),
    includeRelated: false,
    relatedDepth: 1,
    batchSize: 10000,
    maxItems: null,
    verbose: false,
  }

  for (let i = 2; i < args.length; i++) {
    switch (args[i]) {
      case '--type':
        const types = args[++i].split(',').map(t => t.trim())
        for (const t of types) {
          config.targetTypes.add(t)
        }
        break
      case '--type-name':
        const names = args[++i].split(',').map(n => n.trim().toLowerCase())
        for (const name of names) {
          const mapped = TYPE_NAME_MAP[name]
          if (!mapped) {
            console.error(`Unknown type name: ${name}`)
            console.error(`Available: ${Object.keys(TYPE_NAME_MAP).join(', ')}`)
            process.exit(1)
          }
          for (const t of mapped) {
            config.targetTypes.add(t)
          }
        }
        break
      case '--include-related':
        config.includeRelated = true
        break
      case '--related-depth':
        config.relatedDepth = parseInt(args[++i], 10)
        break
      case '--batch-size':
        config.batchSize = parseInt(args[++i], 10)
        break
      case '--max-items':
        config.maxItems = parseInt(args[++i], 10)
        break
      case '--verbose':
        config.verbose = true
        break
    }
  }

  if (config.targetTypes.size === 0) {
    console.error('Error: No target types specified. Use --type or --type-name')
    process.exit(1)
  }

  return config
}

// =============================================================================
// Entity Filter
// =============================================================================

/**
 * Checks if an entity matches the target types based on P31 (instance of)
 */
function matchesTargetType(entity: WikidataEntity, targetTypes: Set<string>): boolean {
  if (entity.type !== 'item') return false

  const claims = entity.claims
  if (!claims) return false

  // Check P31 (instance of) claims
  const instanceOfClaims = claims['P31']
  if (!instanceOfClaims) return false

  for (const claim of instanceOfClaims) {
    const value = claim.mainsnak.datavalue
    if (value?.type === 'wikibase-entityid') {
      const typeId = value.value.id
      if (targetTypes.has(typeId)) {
        return true
      }
    }
  }

  // Also check P31 values transitively via P279 (subclass of)
  // This requires pre-computed type hierarchy, skip for now
  return false
}

/**
 * Extracts entity references from claims
 */
function extractReferencedEntities(entity: WikidataEntity): Set<string> {
  const refs = new Set<string>()

  if (!entity.claims) return refs

  for (const claims of Object.values(entity.claims)) {
    for (const claim of claims) {
      // Main value
      const value = claim.mainsnak.datavalue
      if (value?.type === 'wikibase-entityid') {
        refs.add(value.value.id)
      }

      // Qualifiers
      if (claim.qualifiers) {
        for (const qualSnaks of Object.values(claim.qualifiers)) {
          for (const snak of qualSnaks) {
            const qualValue = snak.datavalue
            if (qualValue?.type === 'wikibase-entityid') {
              refs.add(qualValue.value.id)
            }
          }
        }
      }
    }
  }

  return refs
}

// =============================================================================
// Two-Pass Extraction
// =============================================================================

/**
 * Pass 1: Scan dump to identify matching entity IDs
 */
class EntityScanner extends Writable {
  private config: SubsetConfig
  private matchedIds: Set<string> = new Set()
  private relatedIds: Set<string> = new Set()
  private processedCount = 0
  private matchedCount = 0

  constructor(config: SubsetConfig) {
    super({ objectMode: true })
    this.config = config
  }

  _write(entity: WikidataEntity, _encoding: string, callback: (error?: Error | null) => void): void {
    this.processedCount++

    // Check max items limit
    if (this.config.maxItems && this.matchedCount >= this.config.maxItems) {
      return callback()
    }

    // Check if entity matches target types
    if (matchesTargetType(entity, this.config.targetTypes)) {
      this.matchedIds.add(entity.id)
      this.matchedCount++

      // Track related entities if requested
      if (this.config.includeRelated) {
        const refs = extractReferencedEntities(entity)
        for (const ref of Array.from(refs)) {
          this.relatedIds.add(ref)
        }
      }
    }

    // Log progress
    if (this.config.verbose && this.processedCount % 1000000 === 0) {
      console.log(
        `Scanned ${(this.processedCount / 1000000).toFixed(1)}M entities, ` +
        `matched ${this.matchedCount.toLocaleString()}, ` +
        `related ${this.relatedIds.size.toLocaleString()}`
      )
    }

    callback()
  }

  getMatchedIds(): Set<string> {
    return this.matchedIds
  }

  getRelatedIds(): Set<string> {
    return this.relatedIds
  }

  getStats(): { processed: number; matched: number; related: number } {
    return {
      processed: this.processedCount,
      matched: this.matchedIds.size,
      related: this.relatedIds.size,
    }
  }
}

/**
 * Pass 2: Extract matched entities
 */
class EntityExtractor extends Writable {
  private config: SubsetConfig
  private targetIds: Set<string>
  private itemBuffer: Record<string, unknown>[] = []
  private propertyBuffer: Record<string, unknown>[] = []
  private claimBuffer: Record<string, unknown>[] = []
  private edgeBuffer: Record<string, unknown>[] = []
  private fileCounter = 0
  private stats = {
    extracted: 0,
    items: 0,
    properties: 0,
    claims: 0,
    edges: 0,
  }

  constructor(config: SubsetConfig, targetIds: Set<string>) {
    super({ objectMode: true })
    this.config = config
    this.targetIds = targetIds
  }

  _write(entity: WikidataEntity, _encoding: string, callback: (error?: Error | null) => void): void {
    // Skip non-matching entities
    if (!this.targetIds.has(entity.id)) {
      return callback()
    }

    this.extractEntity(entity)
    this.stats.extracted++

    // Flush buffers if needed
    if (this.itemBuffer.length >= this.config.batchSize) {
      this.flushBuffers()
        .then(() => callback())
        .catch(callback)
    } else {
      callback()
    }
  }

  _final(callback: (error?: Error | null) => void): void {
    this.flushBuffers(true)
      .then(() => callback())
      .catch(callback)
  }

  private extractEntity(entity: WikidataEntity): void {
    if (entity.type === 'item') {
      const record = entityToItemRecord(entity)
      this.itemBuffer.push(record)
      this.stats.items++

      // Extract claims
      if (entity.claims) {
        for (const [propertyId, claims] of Object.entries(entity.claims)) {
          for (const claim of claims) {
            const claimRecord = claimToRecord(entity.id, propertyId, claim)
            this.claimBuffer.push(claimRecord)
            this.stats.claims++

            const edgeRecord = claimToEdgeRecord(entity.id, propertyId, claim)
            if (edgeRecord) {
              this.edgeBuffer.push(edgeRecord)
              this.stats.edges++
            }
          }
        }
      }
    } else if (entity.type === 'property') {
      const record = entityToPropertyRecord(entity)
      this.propertyBuffer.push(record)
      this.stats.properties++
    }

    // Log progress
    if (this.config.verbose && this.stats.extracted % 100000 === 0) {
      console.log(
        `Extracted ${this.stats.extracted.toLocaleString()} entities, ` +
        `${this.stats.claims.toLocaleString()} claims`
      )
    }
  }

  private async flushBuffers(final = false): Promise<void> {
    const threshold = final ? 0 : this.config.batchSize

    if (this.itemBuffer.length > threshold) {
      await this.writeFile('items', this.itemBuffer)
      this.itemBuffer = []
    }

    if (this.propertyBuffer.length > threshold) {
      await this.writeFile('properties', this.propertyBuffer)
      this.propertyBuffer = []
    }

    if (this.claimBuffer.length > threshold) {
      await this.writeFile('claims', this.claimBuffer)
      this.claimBuffer = []
    }

    if (this.edgeBuffer.length > threshold) {
      await this.writeFile('edges', this.edgeBuffer)
      this.edgeBuffer = []
    }
  }

  private async writeFile(type: string, records: Record<string, unknown>[]): Promise<void> {
    const dir = join(this.config.outputDir, 'data', type)
    mkdirSync(dir, { recursive: true })

    const filename = `data.${String(this.fileCounter++).padStart(4, '0')}.ndjson`
    const filepath = join(dir, filename)

    // Write as NDJSON (replace with Parquet in production)
    const ndjson = records.map(r => JSON.stringify(r)).join('\n')
    writeFileSync(filepath, ndjson)

    if (this.config.verbose) {
      console.log(`  Wrote ${records.length} ${type} to ${filename}`)
    }
  }

  getStats(): typeof this.stats {
    return this.stats
  }
}

// =============================================================================
// Streaming JSON Parser
// =============================================================================

class WikidataParser extends Transform {
  private buffer = ''
  private entityCount = 0

  constructor() {
    super({ objectMode: true })
  }

  _transform(chunk: Buffer, _encoding: string, callback: (error?: Error | null) => void): void {
    this.buffer += chunk.toString('utf8')

    let newlineIndex: number
    while ((newlineIndex = this.buffer.indexOf('\n')) !== -1) {
      const line = this.buffer.slice(0, newlineIndex).trim()
      this.buffer = this.buffer.slice(newlineIndex + 1)

      if (line === '[' || line === ']' || line === '') {
        continue
      }

      const json = line.endsWith(',') ? line.slice(0, -1) : line

      try {
        const entity = JSON.parse(json) as WikidataEntity
        this.entityCount++
        this.push(entity)
      } catch {
        // Skip parse errors
      }
    }

    callback()
  }

  _flush(callback: (error?: Error | null) => void): void {
    const line = this.buffer.trim()
    if (line && line !== '[' && line !== ']') {
      const json = line.endsWith(',') ? line.slice(0, -1) : line
      try {
        const entity = JSON.parse(json) as WikidataEntity
        this.push(entity)
      } catch {
        // Ignore
      }
    }
    callback()
  }
}

// =============================================================================
// Main
// =============================================================================

function getDecompressor(filename: string): Transform | null {
  const ext = filename.toLowerCase()
  if (ext.endsWith('.gz')) {
    return createGunzip()
  }
  if (ext.endsWith('.br')) {
    return createBrotliDecompress()
  }
  if (ext.endsWith('.bz2')) {
    console.error('Error: bz2 not supported. Decompress first with: bunzip2 -k file.json.bz2')
    process.exit(1)
  }
  return null
}

function initOutputDir(outputDir: string, config: SubsetConfig): void {
  const dirs = [
    '_meta',
    'data/items',
    'data/properties',
    'data/claims',
    'data/edges',
  ]

  for (const dir of dirs) {
    mkdirSync(join(outputDir, dir), { recursive: true })
  }

  const manifest = {
    version: 1,
    created: new Date().toISOString(),
    source: 'wikidata-subset',
    targetTypes: Array.from(config.targetTypes),
    includeRelated: config.includeRelated,
    relatedDepth: config.relatedDepth,
  }
  writeFileSync(join(outputDir, '_meta/manifest.json'), JSON.stringify(manifest, null, 2))
  writeFileSync(join(outputDir, '_meta/schema.json'), JSON.stringify(wikidataSchema, null, 2))
}

async function runPass(
  dumpFile: string,
  processor: Writable,
  passName: string,
  verbose: boolean
): Promise<void> {
  const inputStream = createReadStream(dumpFile, {
    highWaterMark: 64 * 1024 * 1024,
  })

  const decompressor = getDecompressor(basename(dumpFile))
  const parser = new WikidataParser()

  if (verbose) {
    console.log(`Starting ${passName}...`)
  }

  if (decompressor) {
    await pipelineAsync(inputStream, decompressor, parser, processor)
  } else {
    await pipelineAsync(inputStream, parser, processor)
  }
}

async function main(): Promise<void> {
  const config = parseArgs()

  console.log('Wikidata Subset Extractor')
  console.log('='.repeat(60))
  console.log(`Input:            ${config.dumpFile}`)
  console.log(`Output:           ${config.outputDir}`)
  console.log(`Target types:     ${Array.from(config.targetTypes).join(', ')}`)
  console.log(`Include related:  ${config.includeRelated}`)
  if (config.includeRelated) {
    console.log(`Related depth:    ${config.relatedDepth}`)
  }
  console.log(`Batch size:       ${config.batchSize.toLocaleString()}`)
  if (config.maxItems) {
    console.log(`Max items:        ${config.maxItems.toLocaleString()}`)
  }
  console.log('='.repeat(60))
  console.log('')

  if (!existsSync(config.dumpFile)) {
    console.error(`Error: Input file not found: ${config.dumpFile}`)
    process.exit(1)
  }

  initOutputDir(config.outputDir, config)

  const startTime = Date.now()

  // Pass 1: Scan for matching entities
  console.log('Pass 1: Scanning for matching entities...')
  const scanner = new EntityScanner(config)
  await runPass(config.dumpFile, scanner, 'scan pass', config.verbose)

  const scanStats = scanner.getStats()
  console.log('')
  console.log(`Scan complete:`)
  console.log(`  Processed:  ${scanStats.processed.toLocaleString()} entities`)
  console.log(`  Matched:    ${scanStats.matched.toLocaleString()} entities`)
  console.log(`  Related:    ${scanStats.related.toLocaleString()} entities`)
  console.log('')

  // Combine matched and related IDs
  const targetIds = new Set(scanner.getMatchedIds())
  if (config.includeRelated) {
    for (const id of Array.from(scanner.getRelatedIds())) {
      targetIds.add(id)
    }
  }

  console.log(`Total to extract: ${targetIds.size.toLocaleString()} entities`)
  console.log('')

  // Save target IDs for potential resume
  writeFileSync(
    join(config.outputDir, '_meta/target-ids.json'),
    JSON.stringify(Array.from(targetIds))
  )

  // Pass 2: Extract matching entities
  console.log('Pass 2: Extracting entities...')
  const extractor = new EntityExtractor(config, targetIds)
  await runPass(config.dumpFile, extractor, 'extract pass', config.verbose)

  const extractStats = extractor.getStats()
  const elapsed = (Date.now() - startTime) / 1000
  const minutes = Math.floor(elapsed / 60)
  const seconds = Math.round(elapsed % 60)

  console.log('')
  console.log('='.repeat(60))
  console.log('Extraction Complete')
  console.log('='.repeat(60))
  console.log(`Time elapsed:   ${minutes}m ${seconds}s`)
  console.log(`Extracted:      ${extractStats.extracted.toLocaleString()} entities`)
  console.log(`  Items:        ${extractStats.items.toLocaleString()}`)
  console.log(`  Properties:   ${extractStats.properties.toLocaleString()}`)
  console.log(`Claims:         ${extractStats.claims.toLocaleString()}`)
  console.log(`Edges:          ${extractStats.edges.toLocaleString()}`)
  console.log('='.repeat(60))
}

main().catch(console.error)

export { EntityScanner, EntityExtractor, SubsetConfig }
