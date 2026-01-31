/**
 * FsBackend Disk I/O Benchmarks for ParqueDB
 *
 * Tests real disk I/O performance using actual Parquet files from data/imdb/.
 * Measures cold read latency (fresh file handles) and warm read latency (cached).
 *
 * Test Patterns:
 * - Point lookup by ID
 * - Range scan with filter
 * - Full collection scan
 * - Column projection
 * - Row group selection
 * - Relationship traversal
 *
 * Reports: p50, p95, p99 latencies
 *
 * To clear OS cache on macOS before cold read benchmarks, run:
 *   sudo purge
 *
 * Run with: npx vitest bench tests/benchmarks/fs-backend.bench.ts --reporter=verbose
 */

import { describe, bench, beforeAll, afterEach } from 'vitest'
import { join } from 'node:path'
import { FsBackend } from '../../src/storage/FsBackend'
import { ParquetReader, initializeAsyncBuffer } from '../../src/parquet/reader'
import {
  datasetExists,
  getDataStats,
  loadTestData,
  calculateStats,
  formatStats,
  Timer,
  startTimer,
  type BenchmarkStats,
} from './setup'

// =============================================================================
// Configuration
// =============================================================================

const DATA_DIR = join(process.cwd(), 'data')
const IMDB_DIR = join(DATA_DIR, 'imdb')
const ONET_DIR = join(DATA_DIR, 'onet')

// Test files - IMDB has larger files better for disk I/O testing
const IMDB_FILES = {
  titleBasics: 'title.basics.parquet',       // ~37MB, largest
  nameBasics: 'name.basics.parquet',         // ~35MB
  titleRatings: 'title.ratings.parquet',     // ~7.5MB
  titlePrincipals: 'title.principals.parquet', // ~14MB
  titleCrew: 'title.crew.parquet',           // ~17MB
}

const ONET_FILES = {
  skills: 'Skills.parquet',
  occupationData: 'Occupation Data.parquet',
  knowledge: 'Knowledge.parquet',
}

// =============================================================================
// Helpers
// =============================================================================

/**
 * Try to clear OS file cache on macOS using purge command
 * Requires sudo privileges - will silently fail if not available
 * Note: Must run `sudo purge` manually before cold read benchmarks for true cold reads
 */
function tryPurgeCache(): boolean {
  // child_process not available in all environments (e.g., Workers)
  // The purge command must be run manually before benchmarks
  // Run: sudo purge
  return false
}

/**
 * Statistics collector for manual benchmarks
 */
class LatencyCollector {
  private samples: number[] = []
  private name: string

  constructor(name: string) {
    this.name = name
  }

  addSample(durationMs: number): void {
    this.samples.push(durationMs)
  }

  getStats(): BenchmarkStats {
    return calculateStats(this.name, this.samples)
  }

  reset(): void {
    this.samples = []
  }

  get count(): number {
    return this.samples.length
  }
}

// =============================================================================
// Real Data Availability Check
// =============================================================================

let imdbAvailable = false
let onetAvailable = false
let imdbStorage: FsBackend
let onetStorage: FsBackend
let imdbReader: ParquetReader
let onetReader: ParquetReader

// =============================================================================
// Benchmark Suite
// =============================================================================

describe('FsBackend Disk I/O Benchmarks', () => {
  beforeAll(async () => {
    // Check if real data is available
    imdbAvailable = await datasetExists('imdb')
    onetAvailable = await datasetExists('onet')

    console.log('\n' + '='.repeat(70))
    console.log('FsBackend Disk I/O Benchmarks')
    console.log('='.repeat(70))
    console.log(`IMDB dataset: ${imdbAvailable ? 'Available' : 'NOT AVAILABLE - run npm run load:imdb'}`)
    console.log(`O*NET dataset: ${onetAvailable ? 'Available' : 'NOT AVAILABLE - run npm run load:onet'}`)
    console.log('='.repeat(70) + '\n')

    if (imdbAvailable) {
      imdbStorage = new FsBackend(IMDB_DIR)
      imdbReader = new ParquetReader({ storage: imdbStorage })
      const stats = await getDataStats('imdb')
      console.log(`IMDB: ${stats.fileCount} files, ${stats.totalSizeFormatted} total`)
    }

    if (onetAvailable) {
      onetStorage = new FsBackend(ONET_DIR)
      onetReader = new ParquetReader({ storage: onetStorage })
      const stats = await getDataStats('onet')
      console.log(`O*NET: ${stats.fileCount} files, ${stats.totalSizeFormatted} total`)
    }

    if (!imdbAvailable && !onetAvailable) {
      console.warn('\nNo test data available. Please run:')
      console.warn('  npm run load:imdb')
      console.warn('  npm run load:onet')
      console.warn('\nBenchmarks will be skipped.\n')
    }
  })

  // ===========================================================================
  // Cold Read Benchmarks (No Cache)
  // ===========================================================================

  describe('Cold Read Latency', () => {
    let cachePurged = false

    beforeAll(() => {
      if (!imdbAvailable) {
        console.log('Skipping cold read benchmarks - IMDB data not available')
        return
      }
      // Try to clear OS cache for true cold reads
      cachePurged = tryPurgeCache()
      if (cachePurged) {
        console.log('OS cache cleared with purge command for cold read benchmarks')
      } else {
        console.log('Note: Could not clear OS cache (requires sudo). Cold reads may be partially cached.')
      }
    })

    bench('cold read - title.ratings.parquet (7.5MB)', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      // Create fresh reader and storage to avoid internal caching
      const freshStorage = new FsBackend(IMDB_DIR)
      const freshReader = new ParquetReader({ storage: freshStorage })
      const rows = await freshReader.read(IMDB_FILES.titleRatings, { limit: 100 })
      if (rows.length === 0) throw new Error('No rows returned')
    }, {
      iterations: 20,
      warmupIterations: 2,
    })

    bench('cold read - title.basics.parquet (37MB)', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const freshStorage = new FsBackend(IMDB_DIR)
      const freshReader = new ParquetReader({ storage: freshStorage })
      const rows = await freshReader.read(IMDB_FILES.titleBasics, { limit: 100 })
      if (rows.length === 0) throw new Error('No rows returned')
    }, {
      iterations: 10,
      warmupIterations: 1,
    })

    bench('cold read metadata only - title.basics.parquet', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const freshStorage = new FsBackend(IMDB_DIR)
      const freshReader = new ParquetReader({ storage: freshStorage })
      const meta = await freshReader.readMetadata(IMDB_FILES.titleBasics)
      if (!meta) throw new Error('No metadata returned')
    }, {
      iterations: 20,
      warmupIterations: 2,
    })

    bench('cold read full file - title.ratings.parquet (7.5MB, ~1.4M rows)', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const freshStorage = new FsBackend(IMDB_DIR)
      const freshReader = new ParquetReader({ storage: freshStorage })
      const rows = await freshReader.read(IMDB_FILES.titleRatings)
      if (rows.length === 0) throw new Error('No rows returned')
    }, {
      iterations: 5,
      warmupIterations: 1,
    })
  })

  // ===========================================================================
  // Warm Read Benchmarks (After Cache)
  // ===========================================================================

  describe('Warm Read Latency', () => {
    beforeAll(async () => {
      if (!imdbAvailable) {
        console.log('Skipping warm read benchmarks - IMDB data not available')
        return
      }
      // Prime the cache by reading files once
      await imdbReader.read(IMDB_FILES.titleRatings, { limit: 10 })
      await imdbReader.read(IMDB_FILES.titleBasics, { limit: 10 })
      await imdbReader.readMetadata(IMDB_FILES.titleBasics)
    })

    bench('warm read - title.ratings.parquet (7.5MB)', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const rows = await imdbReader.read(IMDB_FILES.titleRatings, { limit: 100 })
      if (rows.length === 0) throw new Error('No rows returned')
    }, {
      iterations: 50,
      warmupIterations: 5,
    })

    bench('warm read - title.basics.parquet (37MB)', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const rows = await imdbReader.read(IMDB_FILES.titleBasics, { limit: 100 })
      if (rows.length === 0) throw new Error('No rows returned')
    }, {
      iterations: 30,
      warmupIterations: 5,
    })

    bench('warm read metadata - title.basics.parquet', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const meta = await imdbReader.readMetadata(IMDB_FILES.titleBasics)
      if (!meta) throw new Error('No metadata returned')
    }, {
      iterations: 100,
      warmupIterations: 10,
    })

    bench('warm read all records - title.ratings.parquet', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const rows = await imdbReader.read(IMDB_FILES.titleRatings)
      if (rows.length === 0) throw new Error('No rows returned')
    }, {
      iterations: 10,
      warmupIterations: 2,
    })
  })

  // ===========================================================================
  // Point Lookup by ID Pattern
  // ===========================================================================

  describe('Point Lookup by ID', () => {
    // Simulates looking up a specific record by ID
    // In practice, this requires scanning or an index

    bench('lookup single row with limit=1', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const rows = await imdbReader.read(IMDB_FILES.titleRatings, {
        limit: 1,
        columns: ['tconst', 'averageRating', 'numVotes'],
      })
      if (rows.length === 0) throw new Error('No rows returned')
    }, {
      iterations: 100,
      warmupIterations: 10,
    })

    bench('lookup with column projection (3 cols)', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const rows = await imdbReader.read(IMDB_FILES.titleBasics, {
        limit: 1,
        columns: ['tconst', 'primaryTitle', 'startYear'],
      })
      if (rows.length === 0) throw new Error('No rows returned')
    }, {
      iterations: 100,
      warmupIterations: 10,
    })

    bench('lookup with filter simulation (first 100, filter client-side)', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const rows = await imdbReader.read(IMDB_FILES.titleRatings, {
        limit: 100,
        columns: ['tconst', 'averageRating', 'numVotes'],
      })
      // Simulate finding a specific ID
      const found = rows.find((r: any) => r.tconst === 'tt0000001')
      if (rows.length === 0) throw new Error('No rows returned')
    }, {
      iterations: 50,
      warmupIterations: 5,
    })
  })

  // ===========================================================================
  // Range Scan with Filter
  // ===========================================================================

  describe('Range Scan with Filter', () => {
    bench('range scan - filter averageRating > 8.0', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const rows = await imdbReader.read(IMDB_FILES.titleRatings, {
        filter: { column: 'averageRating', op: 'gt', value: 8.0 },
        limit: 1000,
      })
      // Allow empty results for restrictive filters
    }, {
      iterations: 30,
      warmupIterations: 5,
    })

    bench('range scan - filter numVotes > 100000', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const rows = await imdbReader.read(IMDB_FILES.titleRatings, {
        filter: { column: 'numVotes', op: 'gt', value: 100000 },
        limit: 1000,
      })
    }, {
      iterations: 30,
      warmupIterations: 5,
    })

    bench('range scan - filter startYear >= 2020 (title.basics)', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const rows = await imdbReader.read(IMDB_FILES.titleBasics, {
        filter: { column: 'startYear', op: 'gte', value: 2020 },
        limit: 1000,
        columns: ['tconst', 'primaryTitle', 'startYear', 'genres'],
      })
    }, {
      iterations: 20,
      warmupIterations: 3,
    })

    bench('range scan - filter isAdult = 0 (title.basics)', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const rows = await imdbReader.read(IMDB_FILES.titleBasics, {
        filter: { column: 'isAdult', op: 'eq', value: 0 },
        limit: 1000,
        columns: ['tconst', 'primaryTitle', 'isAdult'],
      })
    }, {
      iterations: 20,
      warmupIterations: 3,
    })
  })

  // ===========================================================================
  // Full Collection Scan
  // ===========================================================================

  describe('Full Collection Scan', () => {
    bench('full scan - title.ratings.parquet (~1.4M rows)', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const rows = await imdbReader.read(IMDB_FILES.titleRatings)
      if (rows.length === 0) throw new Error('No rows returned')
    }, {
      iterations: 5,
      warmupIterations: 1,
    })

    bench('full scan with projection - title.ratings.parquet', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const rows = await imdbReader.read(IMDB_FILES.titleRatings, {
        columns: ['tconst', 'averageRating'],
      })
      if (rows.length === 0) throw new Error('No rows returned')
    }, {
      iterations: 5,
      warmupIterations: 1,
    })

    bench('full scan - O*NET Skills.parquet', async () => {
      if (!onetAvailable) throw new Error('O*NET data not available')
      const rows = await onetReader.read(ONET_FILES.skills)
      if (rows.length === 0) throw new Error('No rows returned')
    }, {
      iterations: 20,
      warmupIterations: 5,
    })

    bench('full scan - O*NET Knowledge.parquet', async () => {
      if (!onetAvailable) throw new Error('O*NET data not available')
      const rows = await onetReader.read(ONET_FILES.knowledge)
      if (rows.length === 0) throw new Error('No rows returned')
    }, {
      iterations: 20,
      warmupIterations: 5,
    })
  })

  // ===========================================================================
  // Column Projection Performance
  // ===========================================================================

  describe('Column Projection', () => {
    bench('read 1 column from title.basics', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const rows = await imdbReader.read(IMDB_FILES.titleBasics, {
        columns: ['tconst'],
        limit: 10000,
      })
      if (rows.length === 0) throw new Error('No rows returned')
    }, {
      iterations: 20,
      warmupIterations: 3,
    })

    bench('read 3 columns from title.basics', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const rows = await imdbReader.read(IMDB_FILES.titleBasics, {
        columns: ['tconst', 'primaryTitle', 'startYear'],
        limit: 10000,
      })
      if (rows.length === 0) throw new Error('No rows returned')
    }, {
      iterations: 20,
      warmupIterations: 3,
    })

    bench('read 5 columns from title.basics', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const rows = await imdbReader.read(IMDB_FILES.titleBasics, {
        columns: ['tconst', 'titleType', 'primaryTitle', 'startYear', 'genres'],
        limit: 10000,
      })
      if (rows.length === 0) throw new Error('No rows returned')
    }, {
      iterations: 20,
      warmupIterations: 3,
    })

    bench('read all columns from title.basics', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const rows = await imdbReader.read(IMDB_FILES.titleBasics, {
        limit: 10000,
      })
      if (rows.length === 0) throw new Error('No rows returned')
    }, {
      iterations: 20,
      warmupIterations: 3,
    })
  })

  // ===========================================================================
  // Row Group Selection
  // ===========================================================================

  describe('Row Group Selection', () => {
    bench('read first row group only', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const rows = await imdbReader.read(IMDB_FILES.titleBasics, {
        rowGroups: [0],
      })
      if (rows.length === 0) throw new Error('No rows returned')
    }, {
      iterations: 20,
      warmupIterations: 3,
    })

    bench('read row groups 0 and 1', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const rows = await imdbReader.read(IMDB_FILES.titleBasics, {
        rowGroups: [0, 1],
      })
      if (rows.length === 0) throw new Error('No rows returned')
    }, {
      iterations: 15,
      warmupIterations: 3,
    })

    bench('read specific row groups [0, 2, 4]', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const rows = await imdbReader.read(IMDB_FILES.titleBasics, {
        rowGroups: [0, 2, 4],
      })
      if (rows.length === 0) throw new Error('No rows returned')
    }, {
      iterations: 15,
      warmupIterations: 3,
    })
  })

  // ===========================================================================
  // Streaming Read Performance
  // ===========================================================================

  describe('Streaming Read', () => {
    bench('stream first 1000 rows', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      let count = 0
      for await (const row of imdbReader.stream(IMDB_FILES.titleRatings)) {
        count++
        if (count >= 1000) break
      }
      if (count === 0) throw new Error('No rows streamed')
    }, {
      iterations: 30,
      warmupIterations: 5,
    })

    bench('stream first 10000 rows', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      let count = 0
      for await (const row of imdbReader.stream(IMDB_FILES.titleRatings)) {
        count++
        if (count >= 10000) break
      }
      if (count === 0) throw new Error('No rows streamed')
    }, {
      iterations: 20,
      warmupIterations: 3,
    })

    bench('stream with filter', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      let count = 0
      for await (const row of imdbReader.stream(IMDB_FILES.titleRatings, {
        filter: { column: 'averageRating', op: 'gte', value: 9.0 },
      })) {
        count++
        if (count >= 100) break
      }
    }, {
      iterations: 30,
      warmupIterations: 5,
    })

    bench('stream with column projection', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      let count = 0
      for await (const row of imdbReader.stream(IMDB_FILES.titleRatings, {
        columns: ['tconst', 'averageRating'],
      })) {
        count++
        if (count >= 1000) break
      }
      if (count === 0) throw new Error('No rows streamed')
    }, {
      iterations: 30,
      warmupIterations: 5,
    })
  })

  // ===========================================================================
  // Byte Range Read Performance
  // ===========================================================================

  describe('Byte Range Reads', () => {
    bench('read first 1KB', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const data = await imdbStorage.readRange(IMDB_FILES.titleBasics, 0, 1024)
      if (data.length === 0) throw new Error('No data returned')
    }, {
      iterations: 100,
      warmupIterations: 10,
    })

    bench('read first 64KB', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const data = await imdbStorage.readRange(IMDB_FILES.titleBasics, 0, 65536)
      if (data.length === 0) throw new Error('No data returned')
    }, {
      iterations: 100,
      warmupIterations: 10,
    })

    bench('read first 1MB', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const data = await imdbStorage.readRange(IMDB_FILES.titleBasics, 0, 1024 * 1024)
      if (data.length === 0) throw new Error('No data returned')
    }, {
      iterations: 50,
      warmupIterations: 5,
    })

    bench('read middle 64KB', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      // Read from middle of the file
      const data = await imdbStorage.readRange(IMDB_FILES.titleBasics, 18 * 1024 * 1024, 18 * 1024 * 1024 + 65536)
      if (data.length === 0) throw new Error('No data returned')
    }, {
      iterations: 100,
      warmupIterations: 10,
    })

    bench('read last 8KB (footer)', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const stat = await imdbStorage.stat(IMDB_FILES.titleBasics)
      if (!stat) throw new Error('File not found')
      const data = await imdbStorage.readRange(IMDB_FILES.titleBasics, stat.size - 8192, stat.size)
      if (data.length === 0) throw new Error('No data returned')
    }, {
      iterations: 100,
      warmupIterations: 10,
    })
  })

  // ===========================================================================
  // Concurrent Read Performance
  // ===========================================================================

  describe('Concurrent Reads', () => {
    bench('2 concurrent reads', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const results = await Promise.all([
        imdbReader.read(IMDB_FILES.titleRatings, { limit: 1000 }),
        imdbReader.read(IMDB_FILES.titleBasics, { limit: 1000 }),
      ])
      if (results[0].length === 0 || results[1].length === 0) throw new Error('No rows returned')
    }, {
      iterations: 20,
      warmupIterations: 3,
    })

    bench('4 concurrent reads', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const results = await Promise.all([
        imdbReader.read(IMDB_FILES.titleRatings, { limit: 500 }),
        imdbReader.read(IMDB_FILES.titleBasics, { limit: 500 }),
        imdbReader.read(IMDB_FILES.titleCrew, { limit: 500 }),
        imdbReader.read(IMDB_FILES.titlePrincipals, { limit: 500 }),
      ])
      if (results.some(r => r.length === 0)) throw new Error('No rows returned')
    }, {
      iterations: 15,
      warmupIterations: 3,
    })

    bench('8 concurrent metadata reads', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const results = await Promise.all([
        imdbReader.readMetadata(IMDB_FILES.titleRatings),
        imdbReader.readMetadata(IMDB_FILES.titleBasics),
        imdbReader.readMetadata(IMDB_FILES.titleCrew),
        imdbReader.readMetadata(IMDB_FILES.titlePrincipals),
        imdbReader.readMetadata(IMDB_FILES.nameBasics),
        imdbReader.readMetadata(IMDB_FILES.titleRatings),
        imdbReader.readMetadata(IMDB_FILES.titleBasics),
        imdbReader.readMetadata(IMDB_FILES.titleCrew),
      ])
      if (results.some(r => !r)) throw new Error('No metadata returned')
    }, {
      iterations: 20,
      warmupIterations: 3,
    })
  })

  // ===========================================================================
  // Relationship Traversal Simulation
  // ===========================================================================

  describe('Relationship Traversal Pattern', () => {
    // Simulates traversing from title -> principals -> names
    // This is a common pattern in graph-like queries

    bench('traverse: title -> principals (join pattern)', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      // Step 1: Get some titles
      const titles = await imdbReader.read<any>(IMDB_FILES.titleBasics, {
        limit: 10,
        columns: ['tconst', 'primaryTitle'],
      })

      // Step 2: Get principals for those titles (simulated via limit)
      const principals = await imdbReader.read<any>(IMDB_FILES.titlePrincipals, {
        limit: 100,
        columns: ['tconst', 'nconst', 'category'],
      })

      // Client-side join
      const titleIds = new Set(titles.map((t: any) => t.tconst))
      const matched = principals.filter((p: any) => titleIds.has(p.tconst))
      if (titles.length === 0) throw new Error('No titles returned')
    }, {
      iterations: 30,
      warmupIterations: 5,
    })

    bench('traverse: title -> ratings (lookup pattern)', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      // Step 1: Get titles
      const titles = await imdbReader.read<any>(IMDB_FILES.titleBasics, {
        limit: 50,
        columns: ['tconst', 'primaryTitle', 'startYear'],
      })

      // Step 2: Get ratings
      const ratings = await imdbReader.read<any>(IMDB_FILES.titleRatings, {
        limit: 1000,
        columns: ['tconst', 'averageRating', 'numVotes'],
      })

      // Client-side join to get ratings for titles
      const ratingMap = new Map(ratings.map((r: any) => [r.tconst, r]))
      const joined = titles.map((t: any) => ({
        ...t,
        rating: ratingMap.get(t.tconst),
      }))
      if (titles.length === 0) throw new Error('No titles returned')
    }, {
      iterations: 30,
      warmupIterations: 5,
    })

    bench('traverse: multi-hop (title -> principals -> names)', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      // Step 1: Get a title
      const titles = await imdbReader.read<any>(IMDB_FILES.titleBasics, {
        limit: 5,
        columns: ['tconst', 'primaryTitle'],
      })

      // Step 2: Get principals for that title
      const principals = await imdbReader.read<any>(IMDB_FILES.titlePrincipals, {
        limit: 200,
        columns: ['tconst', 'nconst', 'category', 'job'],
      })

      // Step 3: Get name info
      const names = await imdbReader.read<any>(IMDB_FILES.nameBasics, {
        limit: 500,
        columns: ['nconst', 'primaryName', 'birthYear'],
      })

      // Client-side multi-hop join
      const titleIds = new Set(titles.map((t: any) => t.tconst))
      const relevantPrincipals = principals.filter((p: any) => titleIds.has(p.tconst))
      const nameIds = new Set(relevantPrincipals.map((p: any) => p.nconst))
      const matchedNames = names.filter((n: any) => nameIds.has(n.nconst))
      if (titles.length === 0) throw new Error('No titles returned')
    }, {
      iterations: 20,
      warmupIterations: 3,
    })

    bench('traverse: parallel relationship fetch (title -> [principals, ratings, crew])', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      // Step 1: Get a title
      const titles = await imdbReader.read<any>(IMDB_FILES.titleBasics, {
        limit: 10,
        columns: ['tconst', 'primaryTitle', 'titleType'],
      })
      const titleIds = new Set(titles.map((t: any) => t.tconst))

      // Step 2: Fetch all related entities in parallel
      const [principals, ratings, crew] = await Promise.all([
        imdbReader.read<any>(IMDB_FILES.titlePrincipals, {
          limit: 500,
          columns: ['tconst', 'nconst', 'category', 'ordering'],
        }),
        imdbReader.read<any>(IMDB_FILES.titleRatings, {
          limit: 500,
          columns: ['tconst', 'averageRating', 'numVotes'],
        }),
        imdbReader.read<any>(IMDB_FILES.titleCrew, {
          limit: 500,
          columns: ['tconst', 'directors', 'writers'],
        }),
      ])

      // Build result with all relationships
      const results = titles.map((t: any) => ({
        ...t,
        principals: principals.filter((p: any) => p.tconst === t.tconst),
        rating: ratings.find((r: any) => r.tconst === t.tconst),
        crew: crew.find((c: any) => c.tconst === t.tconst),
      }))
      if (results.length === 0) throw new Error('No results returned')
    }, {
      iterations: 20,
      warmupIterations: 3,
    })

    bench('traverse: deep graph (movie -> actors -> other movies)', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      // Step 1: Get starting title
      const titles = await imdbReader.read<any>(IMDB_FILES.titleBasics, {
        limit: 1,
        columns: ['tconst', 'primaryTitle'],
      })
      const startTitle = titles[0]

      // Step 2: Get principals (actors) for this title
      const principals = await imdbReader.read<any>(IMDB_FILES.titlePrincipals, {
        limit: 1000,
        columns: ['tconst', 'nconst', 'category'],
      })
      const actorIds = new Set(
        principals
          .filter((p: any) => p.tconst === startTitle.tconst && (p.category === 'actor' || p.category === 'actress'))
          .map((p: any) => p.nconst)
      )

      // Step 3: Get names of these actors
      const names = await imdbReader.read<any>(IMDB_FILES.nameBasics, {
        limit: 1000,
        columns: ['nconst', 'primaryName'],
      })
      const actorNames = names.filter((n: any) => actorIds.has(n.nconst))

      // Step 4: Find other movies these actors appeared in (simulated)
      const otherMovies = principals
        .filter((p: any) => actorIds.has(p.nconst) && p.tconst !== startTitle.tconst)
        .slice(0, 20)

      if (titles.length === 0) throw new Error('No titles returned')
    }, {
      iterations: 15,
      warmupIterations: 3,
    })
  })

  // ===========================================================================
  // Storage Backend Operations
  // ===========================================================================

  describe('Storage Backend Operations', () => {
    bench('stat file', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const stat = await imdbStorage.stat(IMDB_FILES.titleBasics)
      if (!stat) throw new Error('File not found')
    }, {
      iterations: 100,
      warmupIterations: 10,
    })

    bench('exists check', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const exists = await imdbStorage.exists(IMDB_FILES.titleBasics)
      if (!exists) throw new Error('File should exist')
    }, {
      iterations: 100,
      warmupIterations: 10,
    })

    bench('list directory', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const list = await imdbStorage.list('')
      if (list.files.length === 0) throw new Error('No files found')
    }, {
      iterations: 50,
      warmupIterations: 5,
    })

    bench('list with pattern', async () => {
      if (!imdbAvailable) throw new Error('IMDB data not available')
      const list = await imdbStorage.list('', { pattern: '*.parquet' })
      if (list.files.length === 0) throw new Error('No files found')
    }, {
      iterations: 50,
      warmupIterations: 5,
    })
  })
})

// =============================================================================
// Manual Latency Report (run after vitest bench completes)
// =============================================================================

/**
 * Generate a detailed latency report with p50, p95, p99
 * This function can be called manually to get more detailed statistics
 */
export async function generateLatencyReport(): Promise<void> {
  console.log('\n' + '='.repeat(70))
  console.log('DETAILED LATENCY REPORT')
  console.log('='.repeat(70))

  const imdbExists = await datasetExists('imdb')
  if (!imdbExists) {
    console.log('IMDB data not available. Run: npm run load:imdb')
    return
  }

  const storage = new FsBackend(IMDB_DIR)
  const reader = new ParquetReader({ storage })

  const tests = [
    {
      name: 'Cold read metadata (title.basics)',
      iterations: 20,
      fn: async () => {
        const s = new FsBackend(IMDB_DIR)
        const r = new ParquetReader({ storage: s })
        await r.readMetadata(IMDB_FILES.titleBasics)
      },
    },
    {
      name: 'Warm read metadata (title.basics)',
      iterations: 50,
      fn: async () => reader.readMetadata(IMDB_FILES.titleBasics),
    },
    {
      name: 'Point lookup (limit=1)',
      iterations: 100,
      fn: async () => reader.read(IMDB_FILES.titleRatings, { limit: 1 }),
    },
    {
      name: 'Range scan (1000 rows)',
      iterations: 50,
      fn: async () => reader.read(IMDB_FILES.titleRatings, { limit: 1000 }),
    },
    {
      name: 'Full scan (title.ratings)',
      iterations: 5,
      fn: async () => reader.read(IMDB_FILES.titleRatings),
    },
  ]

  for (const test of tests) {
    const collector = new LatencyCollector(test.name)

    // Warmup
    for (let i = 0; i < 3; i++) {
      await test.fn()
    }

    // Collect samples
    for (let i = 0; i < test.iterations; i++) {
      const timer = startTimer()
      await test.fn()
      collector.addSample(timer.stop().elapsed())
    }

    const stats = collector.getStats()
    console.log(`\n${test.name}:`)
    console.log(`  p50: ${stats.median.toFixed(3)}ms`)
    console.log(`  p95: ${stats.p95.toFixed(3)}ms`)
    console.log(`  p99: ${stats.p99.toFixed(3)}ms`)
    console.log(`  min: ${stats.min.toFixed(3)}ms`)
    console.log(`  max: ${stats.max.toFixed(3)}ms`)
    console.log(`  ops/sec: ${stats.opsPerSecond.toFixed(2)}`)
  }

  console.log('\n' + '='.repeat(70))
}
