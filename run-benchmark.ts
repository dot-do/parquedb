/**
 * Standalone benchmark runner for FsBackend disk I/O
 *
 * Run with: npx tsx run-benchmark.ts
 */

import { join } from 'node:path'
import { FsBackend } from './src/storage/FsBackend'
import { ParquetReader } from './src/parquet/reader'

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
  knowledge: 'Knowledge.parquet',
}

// =============================================================================
// Benchmark Utilities
// =============================================================================

interface BenchResult {
  name: string
  samples: number[]
  p50: number
  p95: number
  p99: number
  min: number
  max: number
  mean: number
  opsPerSec: number
}

function percentile(samples: number[], p: number): number {
  const sorted = [...samples].sort((a, b) => a - b)
  const index = Math.ceil((p / 100) * sorted.length) - 1
  return sorted[Math.max(0, Math.min(index, sorted.length - 1))]
}

async function runBenchmark(
  name: string,
  fn: () => Promise<void>,
  options: { iterations: number; warmup: number } = { iterations: 20, warmup: 5 }
): Promise<BenchResult> {
  const samples: number[] = []

  // Warmup
  for (let i = 0; i < options.warmup; i++) {
    await fn()
  }

  // Run benchmark
  for (let i = 0; i < options.iterations; i++) {
    const start = performance.now()
    await fn()
    samples.push(performance.now() - start)
  }

  const sum = samples.reduce((a, b) => a + b, 0)
  const mean = sum / samples.length

  return {
    name,
    samples,
    p50: percentile(samples, 50),
    p95: percentile(samples, 95),
    p99: percentile(samples, 99),
    min: Math.min(...samples),
    max: Math.max(...samples),
    mean,
    opsPerSec: 1000 / mean,
  }
}

function formatResult(r: BenchResult): string {
  return `${r.name}
  p50:     ${r.p50.toFixed(2)}ms
  p95:     ${r.p95.toFixed(2)}ms
  p99:     ${r.p99.toFixed(2)}ms
  min:     ${r.min.toFixed(2)}ms
  max:     ${r.max.toFixed(2)}ms
  mean:    ${r.mean.toFixed(2)}ms
  ops/sec: ${r.opsPerSec.toFixed(2)}`
}

// =============================================================================
// Main
// =============================================================================

async function main() {
  console.log('='.repeat(70))
  console.log('ParqueDB FsBackend Disk I/O Benchmarks')
  console.log('='.repeat(70))

  // Check data availability
  const imdbStorage = new FsBackend(IMDB_DIR)
  const onetStorage = new FsBackend(ONET_DIR)

  let imdbAvailable = false
  let onetAvailable = false

  try {
    await imdbStorage.stat(IMDB_FILES.titleRatings)
    imdbAvailable = true
    console.log('IMDB dataset: Available')
  } catch {
    console.log('IMDB dataset: NOT AVAILABLE')
  }

  try {
    await onetStorage.stat(ONET_FILES.skills)
    onetAvailable = true
    console.log('O*NET dataset: Available')
  } catch {
    console.log('O*NET dataset: NOT AVAILABLE')
  }

  if (!imdbAvailable) {
    console.log('\nNo IMDB data available. Exiting.')
    return
  }

  const imdbReader = new ParquetReader({ storage: imdbStorage })
  const results: BenchResult[] = []

  console.log('\n' + '='.repeat(70))
  console.log('Cold Read Latency')
  console.log('Note: Run `sudo purge` before this to clear OS cache')
  console.log('='.repeat(70) + '\n')

  results.push(await runBenchmark(
    'Cold read - title.ratings.parquet (7.5MB, limit=100)',
    async () => {
      const freshStorage = new FsBackend(IMDB_DIR)
      const freshReader = new ParquetReader({ storage: freshStorage })
      await freshReader.read(IMDB_FILES.titleRatings, { limit: 100 })
    },
    { iterations: 20, warmup: 2 }
  ))
  console.log(formatResult(results[results.length - 1]) + '\n')

  results.push(await runBenchmark(
    'Cold read - title.basics.parquet (37MB, limit=100)',
    async () => {
      const freshStorage = new FsBackend(IMDB_DIR)
      const freshReader = new ParquetReader({ storage: freshStorage })
      await freshReader.read(IMDB_FILES.titleBasics, { limit: 100 })
    },
    { iterations: 10, warmup: 1 }
  ))
  console.log(formatResult(results[results.length - 1]) + '\n')

  results.push(await runBenchmark(
    'Cold read metadata - title.basics.parquet',
    async () => {
      const freshStorage = new FsBackend(IMDB_DIR)
      const freshReader = new ParquetReader({ storage: freshStorage })
      await freshReader.readMetadata(IMDB_FILES.titleBasics)
    },
    { iterations: 20, warmup: 2 }
  ))
  console.log(formatResult(results[results.length - 1]) + '\n')

  console.log('\n' + '='.repeat(70))
  console.log('Warm Read Latency (cached)')
  console.log('='.repeat(70) + '\n')

  // Prime the cache
  await imdbReader.read(IMDB_FILES.titleRatings, { limit: 10 })
  await imdbReader.read(IMDB_FILES.titleBasics, { limit: 10 })

  results.push(await runBenchmark(
    'Warm read - title.ratings.parquet (7.5MB, limit=100)',
    async () => {
      await imdbReader.read(IMDB_FILES.titleRatings, { limit: 100 })
    },
    { iterations: 50, warmup: 5 }
  ))
  console.log(formatResult(results[results.length - 1]) + '\n')

  results.push(await runBenchmark(
    'Warm read - title.basics.parquet (37MB, limit=100)',
    async () => {
      await imdbReader.read(IMDB_FILES.titleBasics, { limit: 100 })
    },
    { iterations: 30, warmup: 5 }
  ))
  console.log(formatResult(results[results.length - 1]) + '\n')

  results.push(await runBenchmark(
    'Warm read metadata - title.basics.parquet',
    async () => {
      await imdbReader.readMetadata(IMDB_FILES.titleBasics)
    },
    { iterations: 100, warmup: 10 }
  ))
  console.log(formatResult(results[results.length - 1]) + '\n')

  results.push(await runBenchmark(
    'Warm full scan - title.ratings.parquet (~1.4M rows)',
    async () => {
      await imdbReader.read(IMDB_FILES.titleRatings)
    },
    { iterations: 5, warmup: 1 }
  ))
  console.log(formatResult(results[results.length - 1]) + '\n')

  console.log('\n' + '='.repeat(70))
  console.log('Point Lookup by ID')
  console.log('='.repeat(70) + '\n')

  results.push(await runBenchmark(
    'Point lookup (limit=1)',
    async () => {
      await imdbReader.read(IMDB_FILES.titleRatings, { limit: 1 })
    },
    { iterations: 100, warmup: 10 }
  ))
  console.log(formatResult(results[results.length - 1]) + '\n')

  results.push(await runBenchmark(
    'Point lookup with projection (3 cols, limit=1)',
    async () => {
      await imdbReader.read(IMDB_FILES.titleBasics, {
        limit: 1,
        columns: ['tconst', 'primaryTitle', 'startYear'],
      })
    },
    { iterations: 100, warmup: 10 }
  ))
  console.log(formatResult(results[results.length - 1]) + '\n')

  console.log('\n' + '='.repeat(70))
  console.log('Range Scan with Filter')
  console.log('='.repeat(70) + '\n')

  results.push(await runBenchmark(
    'Range scan - filter averageRating > 8.0 (limit=1000)',
    async () => {
      await imdbReader.read(IMDB_FILES.titleRatings, {
        filter: { column: 'averageRating', op: 'gt', value: 8.0 },
        limit: 1000,
      })
    },
    { iterations: 30, warmup: 5 }
  ))
  console.log(formatResult(results[results.length - 1]) + '\n')

  results.push(await runBenchmark(
    'Range scan - filter numVotes > 100000 (limit=1000)',
    async () => {
      await imdbReader.read(IMDB_FILES.titleRatings, {
        filter: { column: 'numVotes', op: 'gt', value: 100000 },
        limit: 1000,
      })
    },
    { iterations: 30, warmup: 5 }
  ))
  console.log(formatResult(results[results.length - 1]) + '\n')

  console.log('\n' + '='.repeat(70))
  console.log('Full Collection Scan')
  console.log('='.repeat(70) + '\n')

  results.push(await runBenchmark(
    'Full scan - title.ratings.parquet (~1.4M rows)',
    async () => {
      await imdbReader.read(IMDB_FILES.titleRatings)
    },
    { iterations: 5, warmup: 1 }
  ))
  console.log(formatResult(results[results.length - 1]) + '\n')

  results.push(await runBenchmark(
    'Full scan with projection (2 cols) - title.ratings.parquet',
    async () => {
      await imdbReader.read(IMDB_FILES.titleRatings, {
        columns: ['tconst', 'averageRating'],
      })
    },
    { iterations: 5, warmup: 1 }
  ))
  console.log(formatResult(results[results.length - 1]) + '\n')

  console.log('\n' + '='.repeat(70))
  console.log('Relationship Traversal')
  console.log('='.repeat(70) + '\n')

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
      principals.filter((p: any) => titleIds.has(p.tconst))
    },
    { iterations: 30, warmup: 5 }
  ))
  console.log(formatResult(results[results.length - 1]) + '\n')

  results.push(await runBenchmark(
    'Traverse: title -> ratings (lookup pattern)',
    async () => {
      const titles = await imdbReader.read<any>(IMDB_FILES.titleBasics, {
        limit: 50,
        columns: ['tconst', 'primaryTitle', 'startYear'],
      })
      const ratings = await imdbReader.read<any>(IMDB_FILES.titleRatings, {
        limit: 1000,
        columns: ['tconst', 'averageRating', 'numVotes'],
      })
      const ratingMap = new Map(ratings.map((r: any) => [r.tconst, r]))
      titles.map((t: any) => ({ ...t, rating: ratingMap.get(t.tconst) }))
    },
    { iterations: 30, warmup: 5 }
  ))
  console.log(formatResult(results[results.length - 1]) + '\n')

  results.push(await runBenchmark(
    'Traverse: multi-hop (title -> principals -> names)',
    async () => {
      const titles = await imdbReader.read<any>(IMDB_FILES.titleBasics, {
        limit: 5,
        columns: ['tconst', 'primaryTitle'],
      })
      const principals = await imdbReader.read<any>(IMDB_FILES.titlePrincipals, {
        limit: 200,
        columns: ['tconst', 'nconst', 'category', 'job'],
      })
      const names = await imdbReader.read<any>(IMDB_FILES.nameBasics, {
        limit: 500,
        columns: ['nconst', 'primaryName', 'birthYear'],
      })
      const titleIds = new Set(titles.map((t: any) => t.tconst))
      const relevantPrincipals = principals.filter((p: any) => titleIds.has(p.tconst))
      const nameIds = new Set(relevantPrincipals.map((p: any) => p.nconst))
      names.filter((n: any) => nameIds.has(n.nconst))
    },
    { iterations: 20, warmup: 3 }
  ))
  console.log(formatResult(results[results.length - 1]) + '\n')

  results.push(await runBenchmark(
    'Traverse: parallel fetch (title -> [principals, ratings, crew])',
    async () => {
      const titles = await imdbReader.read<any>(IMDB_FILES.titleBasics, {
        limit: 10,
        columns: ['tconst', 'primaryTitle', 'titleType'],
      })
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
      titles.map((t: any) => ({
        ...t,
        principals: principals.filter((p: any) => p.tconst === t.tconst),
        rating: ratings.find((r: any) => r.tconst === t.tconst),
        crew: crew.find((c: any) => c.tconst === t.tconst),
      }))
    },
    { iterations: 20, warmup: 3 }
  ))
  console.log(formatResult(results[results.length - 1]) + '\n')

  console.log('\n' + '='.repeat(70))
  console.log('Byte Range Reads')
  console.log('='.repeat(70) + '\n')

  results.push(await runBenchmark(
    'Read first 1KB',
    async () => {
      await imdbStorage.readRange(IMDB_FILES.titleBasics, 0, 1024)
    },
    { iterations: 100, warmup: 10 }
  ))
  console.log(formatResult(results[results.length - 1]) + '\n')

  results.push(await runBenchmark(
    'Read first 64KB',
    async () => {
      await imdbStorage.readRange(IMDB_FILES.titleBasics, 0, 65536)
    },
    { iterations: 100, warmup: 10 }
  ))
  console.log(formatResult(results[results.length - 1]) + '\n')

  results.push(await runBenchmark(
    'Read first 1MB',
    async () => {
      await imdbStorage.readRange(IMDB_FILES.titleBasics, 0, 1024 * 1024)
    },
    { iterations: 50, warmup: 5 }
  ))
  console.log(formatResult(results[results.length - 1]) + '\n')

  results.push(await runBenchmark(
    'Read middle 64KB',
    async () => {
      await imdbStorage.readRange(IMDB_FILES.titleBasics, 18 * 1024 * 1024, 18 * 1024 * 1024 + 65536)
    },
    { iterations: 100, warmup: 10 }
  ))
  console.log(formatResult(results[results.length - 1]) + '\n')

  const stat = await imdbStorage.stat(IMDB_FILES.titleBasics)
  if (stat) {
    results.push(await runBenchmark(
      'Read last 8KB (footer)',
      async () => {
        await imdbStorage.readRange(IMDB_FILES.titleBasics, stat.size - 8192, stat.size)
      },
      { iterations: 100, warmup: 10 }
    ))
    console.log(formatResult(results[results.length - 1]) + '\n')
  }

  console.log('\n' + '='.repeat(70))
  console.log('Storage Backend Operations')
  console.log('='.repeat(70) + '\n')

  results.push(await runBenchmark(
    'stat file',
    async () => {
      await imdbStorage.stat(IMDB_FILES.titleBasics)
    },
    { iterations: 100, warmup: 10 }
  ))
  console.log(formatResult(results[results.length - 1]) + '\n')

  results.push(await runBenchmark(
    'exists check',
    async () => {
      await imdbStorage.exists(IMDB_FILES.titleBasics)
    },
    { iterations: 100, warmup: 10 }
  ))
  console.log(formatResult(results[results.length - 1]) + '\n')

  results.push(await runBenchmark(
    'list directory',
    async () => {
      await imdbStorage.list('')
    },
    { iterations: 50, warmup: 5 }
  ))
  console.log(formatResult(results[results.length - 1]) + '\n')

  // Summary table
  console.log('\n' + '='.repeat(70))
  console.log('Summary Table')
  console.log('='.repeat(70))
  console.log()
  console.log('| Benchmark | p50 | p95 | p99 | ops/sec |')
  console.log('|-----------|-----|-----|-----|---------|')
  for (const r of results) {
    const name = r.name.length > 50 ? r.name.slice(0, 47) + '...' : r.name
    console.log(`| ${name.padEnd(50)} | ${r.p50.toFixed(1).padStart(7)}ms | ${r.p95.toFixed(1).padStart(7)}ms | ${r.p99.toFixed(1).padStart(7)}ms | ${r.opsPerSec.toFixed(1).padStart(8)} |`)
  }
  console.log()
}

main().catch(console.error)
