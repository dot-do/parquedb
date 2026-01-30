#!/usr/bin/env node
/**
 * Wikidata Full Dump Loader
 *
 * Streams the Wikidata JSON dump into ParqueDB with memory-efficient processing.
 * Handles 100GB+ dumps with ~256MB memory using streaming JSON parsing and
 * backpressure-aware batch writing.
 *
 * Usage:
 *   npx tsx examples/wikidata/load.ts <dump-file> <output-dir> [options]
 *
 * Options:
 *   --batch-size <n>    Items per batch (default: 10000)
 *   --max-items <n>     Maximum items to process (for testing)
 *   --skip-claims       Skip claim extraction (items only)
 *   --skip-edges        Skip edge extraction
 *   --verbose           Enable verbose logging
 */

import { createReadStream, existsSync } from 'node:fs'
import { createBrotliDecompress, createGunzip } from 'node:zlib'
import { Transform, Writable, pipeline } from 'node:stream'
import { promisify } from 'node:util'
import { basename } from 'node:path'

import { ParqueDB, FsBackend } from '../../src'
import type { EntityId, CreateInput } from '../../src/types'
import {
  WikidataEntity,
  Claim,
  extractPrimaryType,
  getCategoryForType,
  getEnglishLabel,
  getEnglishDescription,
  wikidataSchema,
  COMMON_PROPERTIES,
} from './schema.js'

const pipelineAsync = promisify(pipeline)

// =============================================================================
// Configuration
// =============================================================================

interface LoaderConfig {
  dumpFile: string
  outputDir: string
  batchSize: number
  maxItems: number | null
  skipClaims: boolean
  skipEdges: boolean
  verbose: boolean
}

function parseArgs(): LoaderConfig {
  const args = process.argv.slice(2)

  if (args.length < 2) {
    console.error('Usage: npx tsx load.ts <dump-file> <output-dir> [options]')
    console.error('')
    console.error('Options:')
    console.error('  --batch-size <n>    Items per batch (default: 10000)')
    console.error('  --max-items <n>     Maximum items to process (for testing)')
    console.error('  --skip-claims       Skip claim extraction')
    console.error('  --skip-edges        Skip edge extraction')
    console.error('  --verbose           Enable verbose logging')
    process.exit(1)
  }

  const config: LoaderConfig = {
    dumpFile: args[0],
    outputDir: args[1],
    batchSize: 10000,
    maxItems: null,
    skipClaims: false,
    skipEdges: false,
    verbose: false,
  }

  for (let i = 2; i < args.length; i++) {
    switch (args[i]) {
      case '--batch-size':
        config.batchSize = parseInt(args[++i], 10)
        break
      case '--max-items':
        config.maxItems = parseInt(args[++i], 10)
        break
      case '--skip-claims':
        config.skipClaims = true
        break
      case '--skip-edges':
        config.skipEdges = true
        break
      case '--verbose':
        config.verbose = true
        break
    }
  }

  return config
}

// =============================================================================
// Streaming JSON Parser
// =============================================================================

/**
 * Transform stream that parses JSON entities from the Wikidata dump.
 * The dump is a JSON array with one entity per line:
 * [
 * {"id":"Q1",...},
 * {"id":"Q2",...},
 * ]
 */
class WikidataParser extends Transform {
  private buffer = ''
  private entityCount = 0
  private maxItems: number | null

  constructor(maxItems: number | null = null) {
    super({ objectMode: true })
    this.maxItems = maxItems
  }

  _transform(chunk: Buffer, _encoding: string, callback: (error?: Error | null) => void): void {
    this.buffer += chunk.toString('utf8')

    // Process complete lines
    let newlineIndex: number
    while ((newlineIndex = this.buffer.indexOf('\n')) !== -1) {
      const line = this.buffer.slice(0, newlineIndex).trim()
      this.buffer = this.buffer.slice(newlineIndex + 1)

      // Skip array brackets and empty lines
      if (line === '[' || line === ']' || line === '') {
        continue
      }

      // Check max items limit
      if (this.maxItems !== null && this.entityCount >= this.maxItems) {
        this.push(null)
        return callback()
      }

      // Remove trailing comma if present
      const json = line.endsWith(',') ? line.slice(0, -1) : line

      try {
        const entity = JSON.parse(json) as WikidataEntity
        this.entityCount++
        this.push(entity)
      } catch (error) {
        // Log parse error but continue processing
        if (json.length > 0 && json !== '[' && json !== ']') {
          console.error(`Parse error at entity ${this.entityCount}: ${(error as Error).message}`)
        }
      }
    }

    callback()
  }

  _flush(callback: (error?: Error | null) => void): void {
    // Process any remaining buffer content
    const line = this.buffer.trim()
    if (line && line !== '[' && line !== ']') {
      const json = line.endsWith(',') ? line.slice(0, -1) : line
      try {
        const entity = JSON.parse(json) as WikidataEntity
        this.push(entity)
      } catch {
        // Ignore final parse errors
      }
    }
    callback()
  }

  getEntityCount(): number {
    return this.entityCount
  }
}

// =============================================================================
// Batch Accumulator using ParqueDB
// =============================================================================

interface BatchBuffers {
  items: CreateInput[]
  properties: CreateInput[]
  claims: CreateInput[]
}

/**
 * Accumulates entities into batches and writes them to ParqueDB.
 */
class BatchAccumulator extends Writable {
  private config: LoaderConfig
  private db: ParqueDB
  private buffers: BatchBuffers
  private stats: LoaderStats
  // Property label cache for relationship names
  private propertyLabels = new Map<string, string>()

  constructor(config: LoaderConfig, db: ParqueDB, stats: LoaderStats) {
    super({ objectMode: true })
    this.config = config
    this.db = db
    this.stats = stats
    this.buffers = {
      items: [],
      properties: [],
      claims: [],
    }
  }

  _write(entity: WikidataEntity, _encoding: string, callback: (error?: Error | null) => void): void {
    try {
      this.processEntity(entity)

      // Check if any buffer needs flushing
      if (this.shouldFlush()) {
        this.flushBuffers()
          .then(() => callback())
          .catch(callback)
      } else {
        callback()
      }
    } catch (error) {
      callback(error as Error)
    }
  }

  _final(callback: (error?: Error | null) => void): void {
    // Flush remaining buffers
    this.flushBuffers(true)
      .then(() => callback())
      .catch(callback)
  }

  private processEntity(entity: WikidataEntity): void {
    if (entity.type === 'item') {
      this.processItem(entity)
    } else if (entity.type === 'property') {
      this.processProperty(entity)
    }
    // Skip lexemes for now

    this.stats.entitiesProcessed++

    // Log progress
    if (this.config.verbose && this.stats.entitiesProcessed % 100000 === 0) {
      this.logProgress()
    }
  }

  private processItem(entity: WikidataEntity): void {
    const primaryType = extractPrimaryType(entity.claims)
    const category = getCategoryForType(primaryType)
    const labelEn = getEnglishLabel(entity.labels)
    const descriptionEn = getEnglishDescription(entity.descriptions)

    // Create item entity for ParqueDB
    const itemRecord: CreateInput = {
      $id: `items/${entity.id}`,
      $type: 'wikidata:Item',
      name: labelEn ?? entity.id,
      // Data fields
      wikidataId: entity.id,
      itemType: category,
      labelEn,
      descriptionEn,
      labels: entity.labels ?? {},
      descriptions: entity.descriptions ?? {},
      aliases: entity.aliases ?? {},
      sitelinks: entity.sitelinks ?? {},
      lastRevision: entity.lastrevid ?? null,
      modified: entity.modified ? new Date(entity.modified) : null,
    }

    this.buffers.items.push(itemRecord)
    this.stats.itemsProcessed++

    // Process claims (statements about this item)
    if (!this.config.skipClaims && entity.claims) {
      for (const [propertyId, claims] of Object.entries(entity.claims)) {
        for (const claim of claims) {
          this.processClaim(entity.id, propertyId, claim)
        }
      }
    }
  }

  private processProperty(entity: WikidataEntity): void {
    const labelEn = getEnglishLabel(entity.labels)
    const descriptionEn = getEnglishDescription(entity.descriptions)

    // Cache property label for relationship naming
    if (labelEn) {
      this.propertyLabels.set(entity.id, labelEn)
    }

    // Create property entity for ParqueDB
    const propertyRecord: CreateInput = {
      $id: `properties/${entity.id}`,
      $type: 'wikidata:Property',
      name: labelEn ?? entity.id,
      // Data fields
      wikidataId: entity.id,
      datatype: entity.datatype ?? 'unknown',
      labelEn,
      descriptionEn,
      labels: entity.labels ?? {},
      descriptions: entity.descriptions ?? {},
      aliases: entity.aliases ?? {},
      lastRevision: entity.lastrevid ?? null,
      modified: entity.modified ? new Date(entity.modified) : null,
    }

    this.buffers.properties.push(propertyRecord)
    this.stats.propertiesProcessed++
  }

  private processClaim(
    subjectId: string,
    propertyId: string,
    claim: Claim
  ): void {
    const value = claim.mainsnak.datavalue

    // Determine if this is a relationship (entity reference)
    let objectId: string | null = null
    let valueData: unknown = null

    if (value?.type === 'wikibase-entityid') {
      objectId = value.value.id
    } else if (value) {
      valueData = value
    }

    // Get a readable predicate name for the property
    const predicateName = this.getPredicateName(propertyId)

    // Create claim entity for ParqueDB
    // For entity references, we use $link to create relationships
    const claimRecord: CreateInput = {
      $id: `claims/${claim.id}`,
      $type: 'wikidata:Claim',
      name: `${subjectId} ${predicateName} ${objectId || 'value'}`,
      // Data fields
      claimId: claim.id,
      subjectId,
      propertyId,
      objectId,
      rank: claim.rank,
      snaktype: claim.mainsnak.snaktype,
      datatype: claim.mainsnak.datatype ?? 'unknown',
      value: valueData,
      qualifiers: claim.qualifiers ?? null,
      references: claim.references ?? null,
    }

    this.buffers.claims.push(claimRecord)
    this.stats.claimsProcessed++

    // Track edge count for entity references
    if (objectId && !this.config.skipEdges) {
      this.stats.edgesProcessed++
    }
  }

  /**
   * Get a human-readable predicate name for a property ID
   */
  private getPredicateName(propertyId: string): string {
    // Check cache first
    const cached = this.propertyLabels.get(propertyId)
    if (cached) return this.toCamelCase(cached)

    // Use common property names
    const commonNames: Record<string, string> = {
      [COMMON_PROPERTIES.instanceOf]: 'instanceOf',
      [COMMON_PROPERTIES.subclassOf]: 'subclassOf',
      [COMMON_PROPERTIES.country]: 'country',
      [COMMON_PROPERTIES.locatedIn]: 'locatedIn',
      [COMMON_PROPERTIES.coordinate]: 'coordinate',
      [COMMON_PROPERTIES.capital]: 'capital',
      [COMMON_PROPERTIES.dateOfBirth]: 'dateOfBirth',
      [COMMON_PROPERTIES.dateOfDeath]: 'dateOfDeath',
      [COMMON_PROPERTIES.inception]: 'inception',
      [COMMON_PROPERTIES.dissolved]: 'dissolved',
      [COMMON_PROPERTIES.publicationDate]: 'publicationDate',
      [COMMON_PROPERTIES.occupation]: 'occupation',
      [COMMON_PROPERTIES.employer]: 'employer',
      [COMMON_PROPERTIES.educatedAt]: 'educatedAt',
      [COMMON_PROPERTIES.citizenship]: 'citizenship',
      [COMMON_PROPERTIES.placeOfBirth]: 'placeOfBirth',
      [COMMON_PROPERTIES.placeOfDeath]: 'placeOfDeath',
      [COMMON_PROPERTIES.spouse]: 'spouse',
      [COMMON_PROPERTIES.child]: 'child',
      [COMMON_PROPERTIES.father]: 'father',
      [COMMON_PROPERTIES.mother]: 'mother',
      [COMMON_PROPERTIES.author]: 'author',
      [COMMON_PROPERTIES.director]: 'director',
      [COMMON_PROPERTIES.performer]: 'performer',
      [COMMON_PROPERTIES.genre]: 'genre',
      [COMMON_PROPERTIES.notableWork]: 'notableWork',
      [COMMON_PROPERTIES.imdbId]: 'imdbId',
      [COMMON_PROPERTIES.isbnCode]: 'isbnCode',
      [COMMON_PROPERTIES.doi]: 'doi',
      [COMMON_PROPERTIES.image]: 'image',
      [COMMON_PROPERTIES.logo]: 'logo',
    }

    return commonNames[propertyId] || propertyId
  }

  /**
   * Convert a label to camelCase for use as predicate name
   */
  private toCamelCase(str: string): string {
    return str
      .toLowerCase()
      .replace(/[^a-z0-9]+(.)/g, (_, char) => char.toUpperCase())
      .replace(/^(.)/, (char) => char.toLowerCase())
  }

  private shouldFlush(): boolean {
    return (
      this.buffers.items.length >= this.config.batchSize ||
      this.buffers.properties.length >= this.config.batchSize ||
      this.buffers.claims.length >= this.config.batchSize
    )
  }

  private async flushBuffers(final = false): Promise<void> {
    const flushThreshold = final ? 0 : this.config.batchSize

    // Flush items
    if (this.buffers.items.length > flushThreshold) {
      await this.writeEntities('items', this.buffers.items)
      this.buffers.items = []
    }

    // Flush properties
    if (this.buffers.properties.length > flushThreshold) {
      await this.writeEntities('properties', this.buffers.properties)
      this.buffers.properties = []
    }

    // Flush claims with relationships
    if (this.buffers.claims.length > flushThreshold) {
      await this.writeClaimsWithRelationships(this.buffers.claims)
      this.buffers.claims = []
    }
  }

  /**
   * Write entities to a collection
   */
  private async writeEntities(collectionName: string, records: CreateInput[]): Promise<void> {
    const collection = this.db.collection(collectionName)

    for (const record of records) {
      try {
        await collection.create(record)
      } catch (error) {
        // Log but continue - may be duplicate
        if (this.config.verbose) {
          console.error(`Error creating ${collectionName} entity: ${(error as Error).message}`)
        }
      }
    }

    this.stats.filesWritten++

    if (this.config.verbose) {
      console.log(`  Wrote ${records.length} ${collectionName} to ParqueDB`)
    }
  }

  /**
   * Write claims with $link relationships for entity references
   */
  private async writeClaimsWithRelationships(records: CreateInput[]): Promise<void> {
    const claimsCollection = this.db.collection('claims')

    for (const record of records) {
      try {
        // Create the base claim entity
        const claimData: CreateInput = {
          $id: record.$id,
          $type: record.$type as string,
          name: record.name as string,
          claimId: record.claimId,
          subjectId: record.subjectId,
          propertyId: record.propertyId,
          objectId: record.objectId,
          rank: record.rank,
          snaktype: record.snaktype,
          datatype: record.datatype,
          value: record.value,
          qualifiers: record.qualifiers,
          references: record.references,
        }

        // Add relationships using $link-style inline references
        // These establish the graph structure in ParqueDB
        const subjectId = record.subjectId as string
        const propertyId = record.propertyId as string
        const objectId = record.objectId as string | null

        // Link to subject item
        if (subjectId) {
          const subjectRef = `items/${subjectId}` as EntityId
          claimData.subject = { [subjectId]: subjectRef }
        }

        // Link to property
        if (propertyId) {
          const propertyRef = `properties/${propertyId}` as EntityId
          claimData.property = { [propertyId]: propertyRef }
        }

        // Link to object item (if this is an entity reference claim)
        if (objectId && !this.config.skipEdges) {
          const objectRef = `items/${objectId}` as EntityId
          claimData.object = { [objectId]: objectRef }
        }

        await claimsCollection.create(claimData)
      } catch (error) {
        if (this.config.verbose) {
          console.error(`Error creating claim: ${(error as Error).message}`)
        }
      }
    }

    this.stats.filesWritten++

    if (this.config.verbose) {
      console.log(`  Wrote ${records.length} claims with relationships to ParqueDB`)
    }
  }

  private logProgress(): void {
    const elapsed = (Date.now() - this.stats.startTime) / 1000
    const rate = this.stats.entitiesProcessed / elapsed

    console.log(
      `Progress: ${this.stats.entitiesProcessed.toLocaleString()} entities ` +
      `(${Math.round(rate).toLocaleString()}/s), ` +
      `${this.stats.itemsProcessed.toLocaleString()} items, ` +
      `${this.stats.claimsProcessed.toLocaleString()} claims, ` +
      `${this.stats.filesWritten} batches`
    )
  }
}

// =============================================================================
// Statistics
// =============================================================================

interface LoaderStats {
  startTime: number
  entitiesProcessed: number
  itemsProcessed: number
  propertiesProcessed: number
  claimsProcessed: number
  edgesProcessed: number
  filesWritten: number
  bytesWritten: number
}

function createStats(): LoaderStats {
  return {
    startTime: Date.now(),
    entitiesProcessed: 0,
    itemsProcessed: 0,
    propertiesProcessed: 0,
    claimsProcessed: 0,
    edgesProcessed: 0,
    filesWritten: 0,
    bytesWritten: 0,
  }
}

function printStats(stats: LoaderStats): void {
  const elapsed = (Date.now() - stats.startTime) / 1000
  const minutes = Math.floor(elapsed / 60)
  const seconds = Math.round(elapsed % 60)

  console.log('')
  console.log('='.repeat(60))
  console.log('Load Complete')
  console.log('='.repeat(60))
  console.log(`Time elapsed:     ${minutes}m ${seconds}s`)
  console.log(`Entities:         ${stats.entitiesProcessed.toLocaleString()}`)
  console.log(`  Items:          ${stats.itemsProcessed.toLocaleString()}`)
  console.log(`  Properties:     ${stats.propertiesProcessed.toLocaleString()}`)
  console.log(`Claims:           ${stats.claimsProcessed.toLocaleString()}`)
  console.log(`Edges:            ${stats.edgesProcessed.toLocaleString()}`)
  console.log(`Batches written:  ${stats.filesWritten}`)
  console.log(`Throughput:       ${Math.round(stats.entitiesProcessed / elapsed).toLocaleString()} entities/s`)
  console.log('='.repeat(60))
}

// =============================================================================
// Main Loader
// =============================================================================

/**
 * Get decompression stream based on file extension
 */
function getDecompressor(filename: string): Transform | null {
  const ext = filename.toLowerCase()
  if (ext.endsWith('.bz2')) {
    // Note: bz2 decompression requires external library like 'unbzip2-stream'
    // For now, recommend using gzip or pre-decompressed files
    console.error('Error: bz2 decompression not built-in. Use gzip or decompress first:')
    console.error('  bunzip2 -k latest-all.json.bz2')
    process.exit(1)
  }
  if (ext.endsWith('.gz')) {
    return createGunzip()
  }
  if (ext.endsWith('.br')) {
    return createBrotliDecompress()
  }
  return null
}

/**
 * Initialize ParqueDB with schema
 */
function initParqueDB(outputDir: string): ParqueDB {
  const storage = new FsBackend(outputDir)

  const db = new ParqueDB({
    storage,
    schema: wikidataSchema,
  })

  return db
}

/**
 * Main entry point
 */
async function main(): Promise<void> {
  const config = parseArgs()

  console.log('Wikidata Loader')
  console.log('='.repeat(60))
  console.log(`Input:        ${config.dumpFile}`)
  console.log(`Output:       ${config.outputDir}`)
  console.log(`Batch size:   ${config.batchSize.toLocaleString()}`)
  if (config.maxItems) {
    console.log(`Max items:    ${config.maxItems.toLocaleString()}`)
  }
  console.log(`Skip claims:  ${config.skipClaims}`)
  console.log(`Skip edges:   ${config.skipEdges}`)
  console.log('='.repeat(60))
  console.log('')

  // Validate input file
  if (!existsSync(config.dumpFile)) {
    console.error(`Error: Input file not found: ${config.dumpFile}`)
    process.exit(1)
  }

  // Initialize ParqueDB
  console.log('Initializing ParqueDB...')
  const db = initParqueDB(config.outputDir)

  // Create stats tracker
  const stats = createStats()

  // Build pipeline
  const inputStream = createReadStream(config.dumpFile, {
    highWaterMark: 64 * 1024 * 1024, // 64MB read buffer
  })

  const decompressor = getDecompressor(basename(config.dumpFile))
  const parser = new WikidataParser(config.maxItems)
  const accumulator = new BatchAccumulator(config, db, stats)

  console.log('Starting load...')
  console.log('')

  try {
    if (decompressor) {
      await pipelineAsync(inputStream, decompressor, parser, accumulator)
    } else {
      await pipelineAsync(inputStream, parser, accumulator)
    }

    printStats(stats)
  } catch (error) {
    console.error('Load failed:', error)
    process.exit(1)
  }
}

// Run if executed directly
main().catch(console.error)

export { WikidataParser, BatchAccumulator, LoaderConfig, LoaderStats }
