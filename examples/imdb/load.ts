/**
 * IMDB Dataset Streaming Loader for ParqueDB
 *
 * Downloads and processes IMDB TSV files (~1.2GB compressed, ~3GB+ uncompressed)
 * and stores them using ParqueDB's proper API with FsBackend.
 *
 * Usage:
 *   npx tsx examples/imdb/load.ts --output ./data/imdb --max-rows 10000
 *
 * Data source: https://datasets.imdbws.com/
 *
 * Data model:
 * - movies collection: titles from title.basics.tsv
 * - people collection: people from name.basics.tsv
 * - ratings collection: ratings from title.ratings.tsv (linked to movies)
 * - Relationships: movie hasCast person, movie hasRating rating
 */

import { createGunzip } from 'node:zlib'
import { Readable, Transform } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { promises as fs } from 'node:fs'

import { ParqueDB, FsBackend } from '../../src'
import type { CreateInput, EntityId } from '../../src/types'
import { imdbSchema } from './schema'

import {
  parseTitleBasics,
  parseNameBasics,
  parseTitleRatings,
  parseTitlePrincipals,
  parseImdbArray,
  IMDB_CONFIG,
} from './schema'

import type { Title, Person, Rating, Principal } from './schema'

// =============================================================================
// IMDB Dataset URLs
// =============================================================================

const IMDB_BASE_URL = 'https://datasets.imdbws.com'

export const IMDB_DATASETS = {
  titleBasics: `${IMDB_BASE_URL}/title.basics.tsv.gz`,
  titleRatings: `${IMDB_BASE_URL}/title.ratings.tsv.gz`,
  titlePrincipals: `${IMDB_BASE_URL}/title.principals.tsv.gz`,
  nameBasics: `${IMDB_BASE_URL}/name.basics.tsv.gz`,
} as const

// =============================================================================
// Types
// =============================================================================

export interface LoadImdbOptions {
  /** Enable verbose logging */
  verbose?: boolean
  /** Maximum rows to process per dataset (for testing) */
  maxRows?: number
  /** Batch size for database operations */
  batchSize?: number
}

export interface DatasetStats {
  name: string
  rowsProcessed: number
  bytesDownloaded: number
  durationMs: number
  rowsPerSecond: number
}

export interface LoadImdbStats {
  datasets: DatasetStats[]
  totalRowsProcessed: number
  totalBytesDownloaded: number
  totalDurationMs: number
  startTime: Date
  endTime: Date
  compressionRatio: number
  storageSize: number
}

// =============================================================================
// TSV Streaming Parser
// =============================================================================

/** TSV row with string values */
export type TSVRow = Record<string, string>

/**
 * Transform stream that parses TSV lines into objects
 */
export class TSVParser extends Transform {
  private buffer = ''
  private headers: string[] | null = null
  private lineCount = 0
  private maxRows: number
  private reachedMax = false

  constructor(maxRows: number = Infinity) {
    super({ objectMode: true })
    this.maxRows = maxRows
  }

  _transform(
    chunk: Buffer,
    _encoding: BufferEncoding,
    callback: (error?: Error | null) => void
  ): void {
    if (this.reachedMax) {
      callback()
      return
    }

    this.buffer += chunk.toString('utf-8')

    const lines = this.buffer.split('\n')
    // Keep the last incomplete line in the buffer
    this.buffer = lines.pop() || ''

    for (const line of lines) {
      if (line.trim() === '') continue

      // First line is headers
      if (this.headers === null) {
        this.headers = line.split('\t')
        continue
      }

      this.lineCount++

      // Check if we've reached max rows
      if (this.lineCount > this.maxRows) {
        this.reachedMax = true
        callback()
        return
      }

      // Parse row
      const values = line.split('\t')
      const row: Record<string, string> = {}

      for (let i = 0; i < this.headers.length; i++) {
        row[this.headers[i]] = values[i] || ''
      }

      this.push(row)
    }

    callback()
  }

  _flush(callback: (error?: Error | null) => void): void {
    // Process remaining buffer
    if (this.buffer.trim() !== '' && this.headers !== null && !this.reachedMax) {
      if (this.lineCount < this.maxRows) {
        const values = this.buffer.split('\t')
        const row: Record<string, string> = {}

        for (let i = 0; i < this.headers.length; i++) {
          row[this.headers[i]] = values[i] || ''
        }

        this.push(row)
      }
    }

    callback()
  }

  get processedLines(): number {
    return this.lineCount
  }
}

// =============================================================================
// Download Stream with Progress Tracking
// =============================================================================

interface DownloadResult {
  stream: Readable
  contentLength: number | null
}

async function downloadStream(url: string, verbose: boolean): Promise<DownloadResult> {
  if (verbose) {
    console.log(`  Downloading: ${url}`)
  }

  const response = await fetch(url)
  if (!response.ok || !response.body) {
    throw new Error(`Failed to fetch ${url}: ${response.statusText}`)
  }

  const contentLength = response.headers.get('content-length')

  return {
    stream: Readable.fromWeb(response.body as unknown as import('node:stream/web').ReadableStream),
    contentLength: contentLength ? parseInt(contentLength, 10) : null,
  }
}

// =============================================================================
// Counting Transform Stream
// =============================================================================

class ByteCounter extends Transform {
  public bytes = 0

  _transform(chunk: Buffer, _encoding: BufferEncoding, callback: (error?: Error | null) => void) {
    this.bytes += chunk.length
    this.push(chunk)
    callback()
  }
}

// =============================================================================
// Category to Relationship Type Mapping
// =============================================================================

function categoryToRelType(category: string): string {
  switch (category) {
    case 'actor':
    case 'actress':
    case 'self':
      return 'hasCast'
    case 'director':
      return 'hasDirector'
    case 'writer':
      return 'hasWriter'
    case 'producer':
      return 'hasProducer'
    case 'composer':
      return 'hasComposer'
    case 'cinematographer':
      return 'hasCinematographer'
    case 'editor':
      return 'hasEditor'
    default:
      return 'hasCrew'
  }
}

// =============================================================================
// Dataset Loaders using ParqueDB API
// =============================================================================

/**
 * Load ratings into memory for enrichment
 */
async function loadRatingsMap(
  options: LoadImdbOptions
): Promise<{ ratings: Map<string, Rating>; bytes: number }> {
  const { verbose, maxRows } = options
  const ratings = new Map<string, Rating>()

  if (verbose) console.log('\n[1/4] Loading ratings into memory for enrichment...')

  const { stream } = await downloadStream(IMDB_DATASETS.titleRatings, !!verbose)
  const counter = new ByteCounter()
  const parser = new TSVParser(maxRows)

  await pipeline(stream, counter, createGunzip(), parser, async function* (source) {
    for await (const row of source as AsyncIterable<TSVRow>) {
      const rating = parseTitleRatings(row as { tconst: string; averageRating: string; numVotes: string })
      ratings.set(rating.titleId, rating)
    }
  })

  if (verbose) {
    console.log(`  Loaded ${ratings.size.toLocaleString()} ratings`)
    console.log(`  Downloaded: ${(counter.bytes / 1024 / 1024).toFixed(2)} MB`)
  }

  return { ratings, bytes: counter.bytes }
}

/**
 * Load movies (titles) using ParqueDB createMany
 */
async function loadMovies(
  db: ParqueDB,
  ratings: Map<string, Rating>,
  options: LoadImdbOptions
): Promise<DatasetStats> {
  const { verbose, maxRows, batchSize = 1000 } = options
  const startTime = Date.now()
  let rowsProcessed = 0

  if (verbose) console.log('\n[2/4] Processing movies...')

  const { stream } = await downloadStream(IMDB_DATASETS.titleBasics, !!verbose)
  const counter = new ByteCounter()
  const parser = new TSVParser(maxRows)

  const moviesCollection = db.collection('movies')
  let batch: CreateInput<Title & { averageRating?: number; numVotes?: number }>[] = []

  await pipeline(stream, counter, createGunzip(), parser, async function* (source) {
    for await (const row of source as AsyncIterable<TSVRow>) {
      const title = parseTitleBasics(row as {
        tconst: string
        titleType: string
        primaryTitle: string
        originalTitle: string
        isAdult: string
        startYear: string
        endYear: string
        runtimeMinutes: string
        genres: string
      })
      rowsProcessed++

      // Enrich with rating
      const rating = ratings.get(title.id)

      batch.push({
        $id: `movies/${title.id}` as EntityId,
        $type: 'imdb:Title',
        name: title.primaryTitle,
        primaryTitle: title.primaryTitle,
        originalTitle: title.originalTitle,
        type: title.type,
        isAdult: title.isAdult,
        startYear: title.startYear,
        endYear: title.endYear,
        runtimeMinutes: title.runtimeMinutes,
        genres: title.genres,
        averageRating: rating?.averageRating,
        numVotes: rating?.numVotes,
      } as CreateInput<Title & { averageRating?: number; numVotes?: number }>)

      // Flush batch
      if (batch.length >= batchSize) {
        for (const item of batch) {
          await moviesCollection.create(item)
        }
        batch = []

        if (verbose && rowsProcessed % 100000 === 0) {
          console.log(`  Processed ${rowsProcessed.toLocaleString()} movies...`)
        }
      }
    }
  })

  // Flush remaining batch
  for (const item of batch) {
    await moviesCollection.create(item)
  }

  const durationMs = Date.now() - startTime

  if (verbose) {
    console.log(`  Total: ${rowsProcessed.toLocaleString()} movies`)
    console.log(`  Downloaded: ${(counter.bytes / 1024 / 1024).toFixed(2)} MB`)
    console.log(`  Duration: ${(durationMs / 1000).toFixed(1)}s`)
  }

  return {
    name: 'movies',
    rowsProcessed,
    bytesDownloaded: counter.bytes,
    durationMs,
    rowsPerSecond: durationMs > 0 ? Math.round((rowsProcessed / durationMs) * 1000) : 0,
  }
}

/**
 * Load people using ParqueDB createMany
 */
async function loadPeople(
  db: ParqueDB,
  options: LoadImdbOptions
): Promise<DatasetStats> {
  const { verbose, maxRows, batchSize = 1000 } = options
  const startTime = Date.now()
  let rowsProcessed = 0

  if (verbose) console.log('\n[3/4] Processing people...')

  const { stream } = await downloadStream(IMDB_DATASETS.nameBasics, !!verbose)
  const counter = new ByteCounter()
  const parser = new TSVParser(maxRows)

  const peopleCollection = db.collection('people')
  let batch: CreateInput<Person>[] = []

  await pipeline(stream, counter, createGunzip(), parser, async function* (source) {
    for await (const row of source as AsyncIterable<TSVRow>) {
      const typedRow = row as {
        nconst: string
        primaryName: string
        birthYear: string
        deathYear: string
        primaryProfession: string
        knownForTitles: string
      }
      const person = parseNameBasics(typedRow)
      rowsProcessed++

      batch.push({
        $id: `people/${person.id}` as EntityId,
        $type: 'imdb:Person',
        name: person.name,
        birthYear: person.birthYear,
        deathYear: person.deathYear,
        professions: person.professions,
        knownFor: parseImdbArray(typedRow.knownForTitles),
      } as CreateInput<Person>)

      // Flush batch
      if (batch.length >= batchSize) {
        for (const item of batch) {
          await peopleCollection.create(item)
        }
        batch = []

        if (verbose && rowsProcessed % 100000 === 0) {
          console.log(`  Processed ${rowsProcessed.toLocaleString()} people...`)
        }
      }
    }
  })

  // Flush remaining batch
  for (const item of batch) {
    await peopleCollection.create(item)
  }

  const durationMs = Date.now() - startTime

  if (verbose) {
    console.log(`  Total: ${rowsProcessed.toLocaleString()} people`)
    console.log(`  Downloaded: ${(counter.bytes / 1024 / 1024).toFixed(2)} MB`)
    console.log(`  Duration: ${(durationMs / 1000).toFixed(1)}s`)
  }

  return {
    name: 'people',
    rowsProcessed,
    bytesDownloaded: counter.bytes,
    durationMs,
    rowsPerSecond: durationMs > 0 ? Math.round((rowsProcessed / durationMs) * 1000) : 0,
  }
}

/**
 * Load ratings as separate entities and create relationships
 */
async function loadRatings(
  db: ParqueDB,
  ratings: Map<string, Rating>,
  options: LoadImdbOptions
): Promise<DatasetStats> {
  const { verbose, batchSize = 1000 } = options
  const startTime = Date.now()
  let rowsProcessed = 0

  if (verbose) console.log('\n[4/5] Creating rating entities and linking to movies...')

  const ratingsCollection = db.collection('ratings')
  const moviesCollection = db.collection('movies')
  let batch: Array<{ rating: CreateInput<Rating>; titleId: string }> = []

  for (const [titleId, rating] of ratings) {
    rowsProcessed++

    batch.push({
      rating: {
        $id: `ratings/${titleId}` as EntityId,
        $type: 'imdb:Rating',
        name: `Rating for ${titleId}`,
        titleId: rating.titleId,
        averageRating: rating.averageRating,
        numVotes: rating.numVotes,
      } as CreateInput<Rating>,
      titleId,
    })

    // Flush batch
    if (batch.length >= batchSize) {
      for (const item of batch) {
        // Create rating entity
        const ratingEntity = await ratingsCollection.create(item.rating)

        // Link movie -> rating using $link
        try {
          await moviesCollection.update(item.titleId, {
            $link: { hasRating: ratingEntity.$id },
          })
        } catch {
          // Movie may not exist if we limited rows
        }
      }
      batch = []

      if (verbose && rowsProcessed % 100000 === 0) {
        console.log(`  Processed ${rowsProcessed.toLocaleString()} ratings...`)
      }
    }
  }

  // Flush remaining batch
  for (const item of batch) {
    const ratingEntity = await ratingsCollection.create(item.rating)
    try {
      await moviesCollection.update(item.titleId, {
        $link: { hasRating: ratingEntity.$id },
      })
    } catch {
      // Movie may not exist
    }
  }

  const durationMs = Date.now() - startTime

  if (verbose) {
    console.log(`  Total: ${rowsProcessed.toLocaleString()} ratings`)
    console.log(`  Duration: ${(durationMs / 1000).toFixed(1)}s`)
  }

  return {
    name: 'ratings',
    rowsProcessed,
    bytesDownloaded: 0, // Already counted in initial load
    durationMs,
    rowsPerSecond: durationMs > 0 ? Math.round((rowsProcessed / durationMs) * 1000) : 0,
  }
}

/**
 * Load principals (cast/crew) and create relationships
 */
async function loadPrincipals(
  db: ParqueDB,
  options: LoadImdbOptions
): Promise<DatasetStats> {
  const { verbose, maxRows, batchSize = 1000 } = options
  const startTime = Date.now()
  let rowsProcessed = 0
  let relationshipsCreated = 0

  if (verbose) console.log('\n[5/5] Processing principals and creating relationships...')

  const { stream } = await downloadStream(IMDB_DATASETS.titlePrincipals, !!verbose)
  const counter = new ByteCounter()
  const parser = new TSVParser(maxRows)

  const moviesCollection = db.collection('movies')
  let batch: Array<{ titleId: string; personId: string; predicate: string }>[] = []

  await pipeline(stream, counter, createGunzip(), parser, async function* (source) {
    for await (const row of source as AsyncIterable<TSVRow>) {
      const principal = parseTitlePrincipals(row as {
        tconst: string
        ordering: string
        nconst: string
        category: string
        job: string
        characters: string
      })
      rowsProcessed++

      const predicate = categoryToRelType(principal.category)
      const personEntityId = `people/${principal.personId}` as EntityId

      batch.push({
        titleId: principal.titleId,
        personId: principal.personId,
        predicate,
      })

      // Flush batch
      if (batch.length >= batchSize) {
        for (const item of batch) {
          try {
            await moviesCollection.update(item.titleId, {
              $link: { [item.predicate]: `people/${item.personId}` as EntityId },
            })
            relationshipsCreated++
          } catch {
            // Movie or person may not exist if we limited rows
          }
        }
        batch = []

        if (verbose && rowsProcessed % 500000 === 0) {
          console.log(`  Processed ${rowsProcessed.toLocaleString()} principals, created ${relationshipsCreated.toLocaleString()} relationships...`)
        }
      }
    }
  })

  // Flush remaining batch
  for (const item of batch) {
    try {
      await moviesCollection.update(item.titleId, {
        $link: { [item.predicate]: `people/${item.personId}` as EntityId },
      })
      relationshipsCreated++
    } catch {
      // Movie or person may not exist
    }
  }

  const durationMs = Date.now() - startTime

  if (verbose) {
    console.log(`  Total: ${rowsProcessed.toLocaleString()} principals`)
    console.log(`  Relationships created: ${relationshipsCreated.toLocaleString()}`)
    console.log(`  Downloaded: ${(counter.bytes / 1024 / 1024).toFixed(2)} MB`)
    console.log(`  Duration: ${(durationMs / 1000).toFixed(1)}s`)
  }

  return {
    name: 'principals',
    rowsProcessed,
    bytesDownloaded: counter.bytes,
    durationMs,
    rowsPerSecond: durationMs > 0 ? Math.round((rowsProcessed / durationMs) * 1000) : 0,
  }
}

/**
 * Calculate storage size by traversing output directory
 */
async function calculateStorageSize(outputPath: string): Promise<number> {
  let totalSize = 0

  async function traverse(dir: string): Promise<void> {
    try {
      const entries = await fs.readdir(dir, { withFileTypes: true })
      for (const entry of entries) {
        const fullPath = `${dir}/${entry.name}`
        if (entry.isDirectory()) {
          await traverse(fullPath)
        } else {
          const stat = await fs.stat(fullPath)
          totalSize += stat.size
        }
      }
    } catch {
      // Directory may not exist yet
    }
  }

  await traverse(outputPath)
  return totalSize
}

// =============================================================================
// Main Loader Function
// =============================================================================

/**
 * Load IMDB datasets using ParqueDB API
 *
 * @param outputPath - Directory for ParqueDB storage
 * @param options - Loader options
 * @returns Detailed statistics about the load process
 *
 * @example
 * ```typescript
 * const stats = await loadImdb('./data/imdb', { verbose: true, maxRows: 10000 })
 * console.log(`Loaded ${stats.totalRowsProcessed} rows`)
 * console.log(`Compression ratio: ${stats.compressionRatio.toFixed(2)}x`)
 * ```
 */
export async function loadImdb(
  outputPath: string,
  options: LoadImdbOptions = {}
): Promise<LoadImdbStats> {
  const { verbose, maxRows } = options
  const startTime = new Date()
  const datasets: DatasetStats[] = []

  console.log('========================================')
  console.log('  IMDB Dataset Loader for ParqueDB')
  console.log('========================================')
  console.log()
  console.log(`Output path: ${outputPath}`)
  if (maxRows) {
    console.log(`Max rows per dataset: ${maxRows.toLocaleString()}`)
  }
  console.log()

  // Ensure output directory exists
  await fs.mkdir(outputPath, { recursive: true })

  // Initialize ParqueDB with FsBackend
  const storage = new FsBackend(outputPath)
  const db = new ParqueDB({
    storage,
    schema: imdbSchema,
  })

  // Step 1: Load ratings into memory (for enrichment)
  const { ratings, bytes: ratingsBytes } = await loadRatingsMap(options)

  // Step 2: Load movies with embedded ratings
  const moviesStats = await loadMovies(db, ratings, options)
  datasets.push(moviesStats)

  // Step 3: Load people
  const peopleStats = await loadPeople(db, options)
  datasets.push(peopleStats)

  // Step 4: Create rating entities and link to movies
  const ratingsStats = await loadRatings(db, ratings, options)
  datasets.push(ratingsStats)

  // Step 5: Load principals and create relationships
  const principalsStats = await loadPrincipals(db, options)
  datasets.push(principalsStats)

  const endTime = new Date()

  // Calculate totals
  const totalRowsProcessed = datasets.reduce((sum, d) => sum + d.rowsProcessed, 0)
  const totalBytesDownloaded = datasets.reduce((sum, d) => sum + d.bytesDownloaded, 0) + ratingsBytes
  const totalDurationMs = endTime.getTime() - startTime.getTime()

  // Calculate storage size
  const storageSize = await calculateStorageSize(outputPath)
  const compressionRatio = totalBytesDownloaded > 0 ? totalBytesDownloaded / storageSize : 1

  const stats: LoadImdbStats = {
    datasets,
    totalRowsProcessed,
    totalBytesDownloaded,
    totalDurationMs,
    startTime,
    endTime,
    compressionRatio,
    storageSize,
  }

  // Print summary
  console.log('\n========================================')
  console.log('  Summary')
  console.log('========================================')
  console.log()
  console.log('Dataset           Rows        Downloaded    Duration    Rows/sec')
  console.log('-'.repeat(70))
  for (const d of datasets) {
    const name = d.name.padEnd(16)
    const rows = d.rowsProcessed.toLocaleString().padStart(10)
    const downloaded = `${(d.bytesDownloaded / 1024 / 1024).toFixed(1)} MB`.padStart(12)
    const duration = `${(d.durationMs / 1000).toFixed(1)}s`.padStart(10)
    const rowsPerSec = d.rowsPerSecond.toLocaleString().padStart(10)
    console.log(`${name} ${rows} ${downloaded} ${duration} ${rowsPerSec}`)
  }
  console.log('-'.repeat(70))
  console.log(`${'TOTAL'.padEnd(16)} ${totalRowsProcessed.toLocaleString().padStart(10)} ${`${(totalBytesDownloaded / 1024 / 1024).toFixed(1)} MB`.padStart(12)}`)
  console.log()
  console.log(`Duration: ${(totalDurationMs / 1000 / 60).toFixed(1)} minutes`)
  console.log(`Throughput: ${Math.round(totalRowsProcessed / (totalDurationMs / 1000)).toLocaleString()} rows/sec`)
  console.log()
  console.log('Storage:')
  console.log(`  Downloaded (compressed): ${(totalBytesDownloaded / 1024 / 1024).toFixed(2)} MB`)
  console.log(`  Stored (Parquet): ${(storageSize / 1024 / 1024).toFixed(2)} MB`)
  console.log(`  Compression ratio: ${compressionRatio.toFixed(2)}x`)
  console.log()
  console.log('Data files written to:')
  console.log(`  ${outputPath}/data/movies/data.parquet`)
  console.log(`  ${outputPath}/data/people/data.parquet`)
  console.log(`  ${outputPath}/data/ratings/data.parquet`)
  console.log(`  ${outputPath}/rels/forward/*.parquet`)
  console.log(`  ${outputPath}/rels/reverse/*.parquet`)
  console.log()

  return stats
}

// =============================================================================
// CLI Entry Point
// =============================================================================

async function main() {
  const args = process.argv.slice(2)

  // Parse arguments
  const outputArg = args.find(a => a.startsWith('--output='))
  const maxRowsArg = args.find(a => a.startsWith('--max-rows='))
  const verboseArg = args.includes('--verbose') || args.includes('-v')
  const helpArg = args.includes('--help') || args.includes('-h')

  if (helpArg) {
    console.log(`
IMDB Dataset Loader for ParqueDB

Usage:
  npx tsx examples/imdb/load.ts [options]

Options:
  --output=<path>     Output directory for ParqueDB storage (required)
  --max-rows=<num>    Maximum rows per dataset (for testing)
  --verbose, -v       Enable verbose logging
  --help, -h          Show this help message

Examples:
  # Load full dataset
  npx tsx examples/imdb/load.ts --output=./data/imdb --verbose

  # Load test subset (10k rows per dataset)
  npx tsx examples/imdb/load.ts --output=./data/imdb-test --max-rows=10000 --verbose

Data source: https://datasets.imdbws.com/

Data model:
  - movies collection: titles from title.basics.tsv
  - people collection: people from name.basics.tsv
  - ratings collection: linked to movies via hasRating
  - Relationships: hasCast, hasDirector, hasWriter, etc.
`)
    process.exit(0)
  }

  if (!outputArg) {
    console.error('Error: --output=<path> is required')
    console.error('Run with --help for usage information')
    process.exit(1)
  }

  const outputPath = outputArg.split('=')[1]
  const maxRows = maxRowsArg ? parseInt(maxRowsArg.split('=')[1], 10) : undefined

  try {
    await loadImdb(outputPath, {
      verbose: verboseArg,
      maxRows,
    })
  } catch (error) {
    console.error('Error loading IMDB data:', error)
    process.exit(1)
  }
}

// Run if executed directly
if (typeof process !== 'undefined' && process.argv[1]?.endsWith('load.ts')) {
  main().catch(console.error)
}
