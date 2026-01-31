/**
 * Standalone FsBackend Disk I/O Benchmark Script
 *
 * This script measures real disk I/O performance using FsBackend and ParquetReader
 * with actual Parquet files from data/imdb/ and data/onet/.
 *
 * Unlike vitest bench, this script works independently and provides detailed
 * p50, p95, p99 latency measurements.
 *
 * Run with: npx tsx tests/benchmarks/run-fs-benchmark.ts
 *
 * For true cold reads, clear OS file cache first:
 *   macOS: sudo purge
 *   Linux: echo 3 | sudo tee /proc/sys/vm/drop_caches
 */

import { FsBackend } from '../../src/storage/FsBackend'
import { ParquetReader } from '../../src/parquet/reader'
import { join } from 'node:path'
import { stat } from 'node:fs/promises'

// =============================================================================
// Configuration
// =============================================================================

const DATA_DIR = join(process.cwd(), 'data')
const IMDB_DIR = join(DATA_DIR, 'imdb')
const ONET_DIR = join(DATA_DIR, 'onet')

const IMDB_FILES = {
  titleBasics: 'title.basics.parquet',
  nameBasics: 'name.basics.parquet',
  titleRatings: 'title.ratings.parquet',
  titlePrincipals: 'title.principals.parquet',
  titleCrew: 'title.crew.parquet',
}

const ONET_FILES = {
  skills: 'Skills.parquet',
  occupationData: 'Occupation Data.parquet',
  knowledge: 'Knowledge.parquet',
}

// =============================================================================
// Statistics Helpers
// =============================================================================

interface BenchmarkStats {
  name: string
  iterations: number
  samples: number[]
  mean: number
  median: number
  min: number
  max: number
  stdDev: number
  p50: number
  p95: number
  p99: number
  opsPerSecond: number
}

function calculateStats(name: string, samples: number[]): BenchmarkStats {
  const sorted = [...samples].sort((a, b) => a - b)
  const n = samples.length
  const sum = samples.reduce((a, b) => a + b, 0)
  const mean = sum / n

  // Standard deviation
  const variance = samples.reduce((acc, s) => acc + Math.pow(s - mean, 2), 0) / n
  const stdDev = Math.sqrt(variance)

  // Percentiles
  const percentile = (p: number) => {
    const idx = Math.ceil((p / 100) * n) - 1
    return sorted[Math.max(0, Math.min(idx, n - 1))]
  }

  return {
    name,
    iterations: n,
    samples,
    mean,
    median: percentile(50),
    min: sorted[0],
    max: sorted[n - 1],
    stdDev,
    p50: percentile(50),
    p95: percentile(95),
    p99: percentile(99),
    opsPerSecond: (n / sum) * 1000,
  }
}

function formatStats(stats: BenchmarkStats): string {
  return `
${stats.name}:
  Iterations: ${stats.iterations}
  p50 (median): ${stats.p50.toFixed(3)}ms
  p95: ${stats.p95.toFixed(3)}ms
  p99: ${stats.p99.toFixed(3)}ms
  Mean: ${stats.mean.toFixed(3)}ms
  Min: ${stats.min.toFixed(3)}ms
  Max: ${stats.max.toFixed(3)}ms
  Std Dev: ${stats.stdDev.toFixed(3)}ms
  Ops/sec: ${stats.opsPerSecond.toFixed(2)}`
}

// =============================================================================
// Benchmark Runner
// =============================================================================

async function runBenchmark(
  name: string,
  fn: () => Promise<void>,
  options: { iterations?: number; warmup?: number } = {}
): Promise<BenchmarkStats> {
  const { iterations = 20, warmup = 3 } = options
  const samples: number[] = []

  // Warmup
  for (let i = 0; i < warmup; i++) {
    await fn()
  }

  // Measure
  for (let i = 0; i < iterations; i++) {
    const start = performance.now()
    await fn()
    samples.push(performance.now() - start)
  }

  return calculateStats(name, samples)
}

// =============================================================================
// Check Data Availability
// =============================================================================

async function checkDataExists(dir: string): Promise<boolean> {
  try {
    const s = await stat(dir)
    return s.isDirectory()
  } catch {
    return false
  }
}

// =============================================================================
// Main Benchmark Suite
// =============================================================================

async function main() {
  console.log('='.repeat(70))
  console.log('FsBackend Disk I/O Benchmarks')
  console.log('='.repeat(70))

  // Check data availability
  const imdbAvailable = await checkDataExists(IMDB_DIR)
  const onetAvailable = await checkDataExists(ONET_DIR)

  console.log(`\nIMDB dataset: ${imdbAvailable ? 'Available' : 'NOT AVAILABLE'}`)
  console.log(`O*NET dataset: ${onetAvailable ? 'Available' : 'NOT AVAILABLE'}`)

  if (!imdbAvailable && !onetAvailable) {
    console.error('\nNo test data available. Please run:')
    console.error('  npm run load:imdb')
    console.error('  npm run load:onet')
    process.exit(1)
  }

  const results: BenchmarkStats[] = []

  // IMDB Benchmarks
  if (imdbAvailable) {
    const imdbStorage = new FsBackend(IMDB_DIR)
    const imdbReader = new ParquetReader({ storage: imdbStorage })

    console.log('\n' + '-'.repeat(70))
    console.log('Cold Read Latency (fresh reader/storage per read)')
    console.log('-'.repeat(70))

    // Cold read - title.ratings (7.5MB)
    results.push(await runBenchmark(
      'Cold read - title.ratings.parquet (7.5MB)',
      async () => {
        const freshStorage = new FsBackend(IMDB_DIR)
        const freshReader = new ParquetReader({ storage: freshStorage })
        const rows = await freshReader.read(IMDB_FILES.titleRatings, { limit: 100 })
        if (rows.length === 0) throw new Error('No rows')
      },
      { iterations: 20, warmup: 2 }
    ))
    console.log(formatStats(results[results.length - 1]))

    // Cold read - title.basics (37MB)
    results.push(await runBenchmark(
      'Cold read - title.basics.parquet (37MB)',
      async () => {
        const freshStorage = new FsBackend(IMDB_DIR)
        const freshReader = new ParquetReader({ storage: freshStorage })
        const rows = await freshReader.read(IMDB_FILES.titleBasics, { limit: 100 })
        if (rows.length === 0) throw new Error('No rows')
      },
      { iterations: 10, warmup: 1 }
    ))
    console.log(formatStats(results[results.length - 1]))

    // Cold read metadata only
    results.push(await runBenchmark(
      'Cold read metadata - title.basics.parquet',
      async () => {
        const freshStorage = new FsBackend(IMDB_DIR)
        const freshReader = new ParquetReader({ storage: freshStorage })
        const meta = await freshReader.readMetadata(IMDB_FILES.titleBasics)
        if (!meta) throw new Error('No metadata')
      },
      { iterations: 20, warmup: 2 }
    ))
    console.log(formatStats(results[results.length - 1]))

    console.log('\n' + '-'.repeat(70))
    console.log('Warm Read Latency (cached reader/storage)')
    console.log('-'.repeat(70))

    // Prime cache
    await imdbReader.read(IMDB_FILES.titleRatings, { limit: 10 })
    await imdbReader.read(IMDB_FILES.titleBasics, { limit: 10 })

    // Warm read - title.ratings
    results.push(await runBenchmark(
      'Warm read - title.ratings.parquet (limit 100)',
      async () => {
        const rows = await imdbReader.read(IMDB_FILES.titleRatings, { limit: 100 })
        if (rows.length === 0) throw new Error('No rows')
      },
      { iterations: 50, warmup: 5 }
    ))
    console.log(formatStats(results[results.length - 1]))

    // Warm read - title.basics
    results.push(await runBenchmark(
      'Warm read - title.basics.parquet (limit 100)',
      async () => {
        const rows = await imdbReader.read(IMDB_FILES.titleBasics, { limit: 100 })
        if (rows.length === 0) throw new Error('No rows')
      },
      { iterations: 30, warmup: 5 }
    ))
    console.log(formatStats(results[results.length - 1]))

    // Warm read metadata
    results.push(await runBenchmark(
      'Warm read metadata - title.basics.parquet',
      async () => {
        const meta = await imdbReader.readMetadata(IMDB_FILES.titleBasics)
        if (!meta) throw new Error('No metadata')
      },
      { iterations: 100, warmup: 10 }
    ))
    console.log(formatStats(results[results.length - 1]))

    // Warm read all records - title.ratings (smaller file)
    results.push(await runBenchmark(
      'Warm read ALL records - title.ratings.parquet (~1.4M rows)',
      async () => {
        const rows = await imdbReader.read(IMDB_FILES.titleRatings)
        if (rows.length === 0) throw new Error('No rows')
      },
      { iterations: 5, warmup: 1 }
    ))
    console.log(formatStats(results[results.length - 1]))

    console.log('\n' + '-'.repeat(70))
    console.log('Point Lookup by ID')
    console.log('-'.repeat(70))

    // Lookup single row
    results.push(await runBenchmark(
      'Point lookup (limit=1)',
      async () => {
        const rows = await imdbReader.read(IMDB_FILES.titleRatings, {
          limit: 1,
          columns: ['tconst', 'averageRating', 'numVotes'],
        })
        if (rows.length === 0) throw new Error('No rows')
      },
      { iterations: 100, warmup: 10 }
    ))
    console.log(formatStats(results[results.length - 1]))

    // Lookup with projection
    results.push(await runBenchmark(
      'Point lookup with projection (3 cols)',
      async () => {
        const rows = await imdbReader.read(IMDB_FILES.titleBasics, {
          limit: 1,
          columns: ['tconst', 'primaryTitle', 'startYear'],
        })
        if (rows.length === 0) throw new Error('No rows')
      },
      { iterations: 100, warmup: 10 }
    ))
    console.log(formatStats(results[results.length - 1]))

    console.log('\n' + '-'.repeat(70))
    console.log('Range Scan with Filter')
    console.log('-'.repeat(70))

    // Range scan - filter
    results.push(await runBenchmark(
      'Range scan - filter averageRating > 8.0 (limit 1000)',
      async () => {
        const rows = await imdbReader.read(IMDB_FILES.titleRatings, {
          filter: { column: 'averageRating', op: 'gt', value: 8.0 },
          limit: 1000,
        })
      },
      { iterations: 30, warmup: 5 }
    ))
    console.log(formatStats(results[results.length - 1]))

    // Range scan - filter numVotes
    results.push(await runBenchmark(
      'Range scan - filter numVotes > 100000 (limit 1000)',
      async () => {
        const rows = await imdbReader.read(IMDB_FILES.titleRatings, {
          filter: { column: 'numVotes', op: 'gt', value: 100000 },
          limit: 1000,
        })
      },
      { iterations: 30, warmup: 5 }
    ))
    console.log(formatStats(results[results.length - 1]))

    console.log('\n' + '-'.repeat(70))
    console.log('Full Collection Scan')
    console.log('-'.repeat(70))

    // Full scan - title.ratings
    results.push(await runBenchmark(
      'Full scan - title.ratings.parquet (~1.4M rows)',
      async () => {
        const rows = await imdbReader.read(IMDB_FILES.titleRatings)
        if (rows.length === 0) throw new Error('No rows')
      },
      { iterations: 5, warmup: 1 }
    ))
    console.log(formatStats(results[results.length - 1]))

    // Full scan with projection
    results.push(await runBenchmark(
      'Full scan with projection (2 cols) - title.ratings.parquet',
      async () => {
        const rows = await imdbReader.read(IMDB_FILES.titleRatings, {
          columns: ['tconst', 'averageRating'],
        })
        if (rows.length === 0) throw new Error('No rows')
      },
      { iterations: 5, warmup: 1 }
    ))
    console.log(formatStats(results[results.length - 1]))

    console.log('\n' + '-'.repeat(70))
    console.log('Column Projection Performance')
    console.log('-'.repeat(70))

    // Read 1 column
    results.push(await runBenchmark(
      'Read 1 column - title.basics (10000 rows)',
      async () => {
        const rows = await imdbReader.read(IMDB_FILES.titleBasics, {
          columns: ['tconst'],
          limit: 10000,
        })
        if (rows.length === 0) throw new Error('No rows')
      },
      { iterations: 20, warmup: 3 }
    ))
    console.log(formatStats(results[results.length - 1]))

    // Read 3 columns
    results.push(await runBenchmark(
      'Read 3 columns - title.basics (10000 rows)',
      async () => {
        const rows = await imdbReader.read(IMDB_FILES.titleBasics, {
          columns: ['tconst', 'primaryTitle', 'startYear'],
          limit: 10000,
        })
        if (rows.length === 0) throw new Error('No rows')
      },
      { iterations: 20, warmup: 3 }
    ))
    console.log(formatStats(results[results.length - 1]))

    // Read 5 columns
    results.push(await runBenchmark(
      'Read 5 columns - title.basics (10000 rows)',
      async () => {
        const rows = await imdbReader.read(IMDB_FILES.titleBasics, {
          columns: ['tconst', 'titleType', 'primaryTitle', 'startYear', 'genres'],
          limit: 10000,
        })
        if (rows.length === 0) throw new Error('No rows')
      },
      { iterations: 20, warmup: 3 }
    ))
    console.log(formatStats(results[results.length - 1]))

    // Read all columns
    results.push(await runBenchmark(
      'Read all columns - title.basics (10000 rows)',
      async () => {
        const rows = await imdbReader.read(IMDB_FILES.titleBasics, {
          limit: 10000,
        })
        if (rows.length === 0) throw new Error('No rows')
      },
      { iterations: 20, warmup: 3 }
    ))
    console.log(formatStats(results[results.length - 1]))

    console.log('\n' + '-'.repeat(70))
    console.log('Row Group Selection')
    console.log('-'.repeat(70))

    // Read first row group only
    results.push(await runBenchmark(
      'Read first row group only - title.basics',
      async () => {
        const rows = await imdbReader.read(IMDB_FILES.titleBasics, {
          rowGroups: [0],
        })
        if (rows.length === 0) throw new Error('No rows')
      },
      { iterations: 15, warmup: 3 }
    ))
    console.log(formatStats(results[results.length - 1]))

    console.log('\n' + '-'.repeat(70))
    console.log('Streaming Read')
    console.log('-'.repeat(70))

    // Stream first 1000 rows
    results.push(await runBenchmark(
      'Stream first 1000 rows - title.ratings',
      async () => {
        let count = 0
        for await (const row of imdbReader.stream(IMDB_FILES.titleRatings)) {
          count++
          if (count >= 1000) break
        }
        if (count === 0) throw new Error('No rows')
      },
      { iterations: 30, warmup: 5 }
    ))
    console.log(formatStats(results[results.length - 1]))

    // Stream first 10000 rows
    results.push(await runBenchmark(
      'Stream first 10000 rows - title.ratings',
      async () => {
        let count = 0
        for await (const row of imdbReader.stream(IMDB_FILES.titleRatings)) {
          count++
          if (count >= 10000) break
        }
        if (count === 0) throw new Error('No rows')
      },
      { iterations: 20, warmup: 3 }
    ))
    console.log(formatStats(results[results.length - 1]))

    console.log('\n' + '-'.repeat(70))
    console.log('Byte Range Reads (Storage Backend)')
    console.log('-'.repeat(70))

    // Read first 1KB
    results.push(await runBenchmark(
      'Read first 1KB - title.basics',
      async () => {
        const data = await imdbStorage.readRange(IMDB_FILES.titleBasics, 0, 1024)
        if (data.length === 0) throw new Error('No data')
      },
      { iterations: 100, warmup: 10 }
    ))
    console.log(formatStats(results[results.length - 1]))

    // Read first 64KB
    results.push(await runBenchmark(
      'Read first 64KB - title.basics',
      async () => {
        const data = await imdbStorage.readRange(IMDB_FILES.titleBasics, 0, 65536)
        if (data.length === 0) throw new Error('No data')
      },
      { iterations: 100, warmup: 10 }
    ))
    console.log(formatStats(results[results.length - 1]))

    // Read first 1MB
    results.push(await runBenchmark(
      'Read first 1MB - title.basics',
      async () => {
        const data = await imdbStorage.readRange(IMDB_FILES.titleBasics, 0, 1024 * 1024)
        if (data.length === 0) throw new Error('No data')
      },
      { iterations: 50, warmup: 5 }
    ))
    console.log(formatStats(results[results.length - 1]))

    // Read last 8KB (footer)
    results.push(await runBenchmark(
      'Read last 8KB (footer) - title.basics',
      async () => {
        const fileStat = await imdbStorage.stat(IMDB_FILES.titleBasics)
        if (!fileStat) throw new Error('File not found')
        const data = await imdbStorage.readRange(IMDB_FILES.titleBasics, fileStat.size - 8192, fileStat.size)
        if (data.length === 0) throw new Error('No data')
      },
      { iterations: 100, warmup: 10 }
    ))
    console.log(formatStats(results[results.length - 1]))

    console.log('\n' + '-'.repeat(70))
    console.log('Concurrent Reads')
    console.log('-'.repeat(70))

    // 2 concurrent reads
    results.push(await runBenchmark(
      '2 concurrent reads',
      async () => {
        const [a, b] = await Promise.all([
          imdbReader.read(IMDB_FILES.titleRatings, { limit: 1000 }),
          imdbReader.read(IMDB_FILES.titleBasics, { limit: 1000 }),
        ])
        if (a.length === 0 || b.length === 0) throw new Error('No rows')
      },
      { iterations: 20, warmup: 3 }
    ))
    console.log(formatStats(results[results.length - 1]))

    // 4 concurrent reads
    results.push(await runBenchmark(
      '4 concurrent reads',
      async () => {
        const res = await Promise.all([
          imdbReader.read(IMDB_FILES.titleRatings, { limit: 500 }),
          imdbReader.read(IMDB_FILES.titleBasics, { limit: 500 }),
          imdbReader.read(IMDB_FILES.titleCrew, { limit: 500 }),
          imdbReader.read(IMDB_FILES.titlePrincipals, { limit: 500 }),
        ])
        if (res.some(r => r.length === 0)) throw new Error('No rows')
      },
      { iterations: 15, warmup: 3 }
    ))
    console.log(formatStats(results[results.length - 1]))

    console.log('\n' + '-'.repeat(70))
    console.log('Relationship Traversal Pattern')
    console.log('-'.repeat(70))

    // Traverse: title -> principals (join pattern)
    results.push(await runBenchmark(
      'Traverse: title -> principals (join pattern)',
      async () => {
        const titles = await imdbReader.read<any>(IMDB_FILES.titleBasics, {
          limit: 10,
          columns: ['tconst', 'primaryTitle'],
        })
        const principals = await imdbReader.read<any>(IMDB_FILES.titlePrincipals, {
          limit: 100,
          columns: ['tconst', 'nconst', 'category'],
        })
        const titleIds = new Set(titles.map((t: any) => t.tconst))
        const matched = principals.filter((p: any) => titleIds.has(p.tconst))
        if (titles.length === 0) throw new Error('No titles')
      },
      { iterations: 30, warmup: 5 }
    ))
    console.log(formatStats(results[results.length - 1]))

    // Multi-hop traversal
    results.push(await runBenchmark(
      'Traverse: multi-hop (title -> principals -> names)',
      async () => {
        const titles = await imdbReader.read<any>(IMDB_FILES.titleBasics, {
          limit: 5,
          columns: ['tconst', 'primaryTitle'],
        })
        const principals = await imdbReader.read<any>(IMDB_FILES.titlePrincipals, {
          limit: 200,
          columns: ['tconst', 'nconst', 'category'],
        })
        const names = await imdbReader.read<any>(IMDB_FILES.nameBasics, {
          limit: 500,
          columns: ['nconst', 'primaryName', 'birthYear'],
        })
        const titleIds = new Set(titles.map((t: any) => t.tconst))
        const relevantPrincipals = principals.filter((p: any) => titleIds.has(p.tconst))
        const nameIds = new Set(relevantPrincipals.map((p: any) => p.nconst))
        const matchedNames = names.filter((n: any) => nameIds.has(n.nconst))
        if (titles.length === 0) throw new Error('No titles')
      },
      { iterations: 20, warmup: 3 }
    ))
    console.log(formatStats(results[results.length - 1]))

    console.log('\n' + '-'.repeat(70))
    console.log('Storage Backend Operations')
    console.log('-'.repeat(70))

    // Stat file
    results.push(await runBenchmark(
      'stat() file',
      async () => {
        const s = await imdbStorage.stat(IMDB_FILES.titleBasics)
        if (!s) throw new Error('File not found')
      },
      { iterations: 100, warmup: 10 }
    ))
    console.log(formatStats(results[results.length - 1]))

    // Exists check
    results.push(await runBenchmark(
      'exists() check',
      async () => {
        const exists = await imdbStorage.exists(IMDB_FILES.titleBasics)
        if (!exists) throw new Error('File not found')
      },
      { iterations: 100, warmup: 10 }
    ))
    console.log(formatStats(results[results.length - 1]))

    // List directory
    results.push(await runBenchmark(
      'list() directory',
      async () => {
        const list = await imdbStorage.list('')
        if (list.files.length === 0) throw new Error('No files')
      },
      { iterations: 50, warmup: 5 }
    ))
    console.log(formatStats(results[results.length - 1]))
  }

  // O*NET Benchmarks
  if (onetAvailable) {
    const onetStorage = new FsBackend(ONET_DIR)
    const onetReader = new ParquetReader({ storage: onetStorage })

    console.log('\n' + '-'.repeat(70))
    console.log('O*NET Dataset Tests')
    console.log('-'.repeat(70))

    // Full scan - Skills.parquet
    results.push(await runBenchmark(
      'Full scan - O*NET Skills.parquet',
      async () => {
        const rows = await onetReader.read(ONET_FILES.skills)
        if (rows.length === 0) throw new Error('No rows')
      },
      { iterations: 20, warmup: 5 }
    ))
    console.log(formatStats(results[results.length - 1]))

    // Full scan - Knowledge.parquet
    results.push(await runBenchmark(
      'Full scan - O*NET Knowledge.parquet',
      async () => {
        const rows = await onetReader.read(ONET_FILES.knowledge)
        if (rows.length === 0) throw new Error('No rows')
      },
      { iterations: 20, warmup: 5 }
    ))
    console.log(formatStats(results[results.length - 1]))
  }

  // Print summary
  console.log('\n' + '='.repeat(70))
  console.log('SUMMARY')
  console.log('='.repeat(70))
  console.log('\n' + 'Benchmark'.padEnd(50) + 'p50 (ms)'.padStart(12) + 'p95 (ms)'.padStart(12) + 'p99 (ms)'.padStart(12))
  console.log('-'.repeat(86))

  for (const r of results) {
    const name = r.name.length > 48 ? r.name.slice(0, 48) + '..' : r.name
    console.log(`${name.padEnd(50)} ${r.p50.toFixed(3).padStart(12)} ${r.p95.toFixed(3).padStart(12)} ${r.p99.toFixed(3).padStart(12)}`)
  }

  console.log('\n' + '='.repeat(70))
  console.log('Benchmark complete!')
  console.log('='.repeat(70))
}

main().catch(console.error)
