/**
 * Comprehensive Dataset Backend Benchmark
 *
 * Tests real-world datasets (IMDB, O*NET, UNSPSC, Wiktionary, Wikidata)
 * across all three table formats (Native Parquet, Apache Iceberg, Delta Lake).
 *
 * Backend Evolution: Just like schema evolution, backend evolution is automatic.
 * Use `?migrate=true` to auto-convert native data to Iceberg/Delta formats.
 *
 * Endpoints:
 * - GET /benchmark/datasets/backends - Full benchmark
 * - GET /benchmark/datasets/backends?dataset=imdb - Specific dataset
 * - GET /benchmark/datasets/backends?backend=iceberg - Specific backend
 * - GET /benchmark/datasets/backends?migrate=true - Auto-migrate native → Iceberg/Delta
 *
 * Query params:
 * - dataset: imdb|imdb-1m|onet|onet-full|unspsc|unspsc-full|wiktionary|wikidata|all
 * - backend: native|iceberg|delta|all
 * - iterations: number of iterations per query (default: 3)
 * - maxQueries: max queries per dataset (default: 5)
 * - migrate: true to auto-convert native data to Iceberg/Delta (default: false)
 */

import { parquetQuery } from 'hyparquet'
import { compressors } from '../parquet/compressors'
import { logger } from '../utils/logger'
import { R2Backend } from '../storage/R2Backend'
import {
  migrateBackend,
} from '../backends'

// =============================================================================
// Types
// =============================================================================

type R2Bucket = {
  get(key: string, options?: { range?: { offset: number; length: number } | undefined }): Promise<{ arrayBuffer(): Promise<ArrayBuffer> } | null>
  head(key: string): Promise<{ size: number } | null>
  list(options?: { prefix?: string | undefined; limit?: number | undefined; cursor?: string | undefined }): Promise<{ objects: { key: string; size: number }[]; truncated: boolean; cursor?: string | undefined }>
}

interface DatasetBackendConfig {
  datasets: string[]
  backends: ('native' | 'iceberg' | 'delta')[]
  iterations: number
  maxQueries: number
  /** Auto-migrate native data to Iceberg/Delta if not present */
  autoMigrate: boolean
}

interface LatencyStats {
  p50: number
  p95: number
  p99: number
  mean: number
  min: number
  max: number
}

interface QueryResult {
  query: string
  description: string
  latency: LatencyStats
  bytesRead: number
  rowsReturned: number
  rowsScanned: number
}

interface BackendDatasetResult {
  backend: string
  format: string
  available: boolean
  queries: QueryResult[]
  totalBytesRead: number
  totalTimeMs: number
  error?: string | undefined
}

interface DatasetResult {
  dataset: string
  size: string
  backends: BackendDatasetResult[]
  comparison?: {
    fastestBackend: string
    speedup: string
  } | undefined
}

interface BenchmarkResult {
  config: DatasetBackendConfig
  results: DatasetResult[]
  summary: {
    datasetsCount: number
    backendsCount: number
    queriesExecuted: number
    avgLatencyByBackend: Record<string, number>
    recommendation: string
  }
  metadata: {
    timestamp: string
    colo?: string | undefined
    durationMs: number
  }
}

// =============================================================================
// Constants
// =============================================================================

const DATASET_PREFIX = 'benchmark-data'

// Dataset configurations with paths for each backend
const DATASETS: Record<string, {
  name: string
  size: string
  collections: string[]
  queries: QueryDefinition[]
  native: { prefix: string }
  iceberg?: { prefix: string; metadataPath: string } | undefined
  delta?: { prefix: string; logPath: string } | undefined
}> = {
  'imdb': {
    name: 'IMDB 100K',
    size: '~10MB',
    collections: ['titles', 'people', 'cast'],
    queries: [
      { name: 'Full scan titles', file: 'titles.parquet', columns: ['$id', 'name'] },
      { name: 'Filter by titleType', file: 'titles.parquet', filter: { $index_titleType: 'movie' }, columns: ['$id', 'name'] },
      { name: 'Filter by year (2000-2010)', file: 'titles.parquet', filter: { $index_startYear: { $gte: 2000, $lte: 2010 } }, columns: ['$id', 'name'] },
      { name: 'High rating (>8.0)', file: 'titles.parquet', filter: { $index_averageRating: { $gt: 8.0 } }, columns: ['$id', 'name'] },
      { name: 'People - actors', file: 'people.parquet', filter: { $index_primaryProfession: 'actor' }, columns: ['$id', 'name'] },
    ],
    native: { prefix: `${DATASET_PREFIX}/imdb` },
    iceberg: { prefix: `${DATASET_PREFIX}/imdb-iceberg`, metadataPath: 'metadata/v1.metadata.json' },
    delta: { prefix: `${DATASET_PREFIX}/imdb-delta`, logPath: '_delta_log' },
  },
  'imdb-1m': {
    name: 'IMDB 1M',
    size: '~50MB',
    collections: ['titles', 'people', 'cast'],
    queries: [
      { name: 'Full scan titles (1M)', file: 'titles.parquet', columns: ['$id', 'name'] },
      { name: 'Filter by titleType', file: 'titles.parquet', filter: { $index_titleType: 'movie' }, columns: ['$id', 'name'] },
      { name: 'Year range (2015-2020)', file: 'titles.parquet', filter: { $index_startYear: { $gte: 2015, $lte: 2020 } }, columns: ['$id', 'name'] },
      { name: 'High votes (>10K)', file: 'titles.parquet', filter: { $index_numVotes: { $gte: 10000 } }, columns: ['$id', 'name'] },
      { name: 'Compound (movie + rating >7.5)', file: 'titles.parquet', filter: { $index_titleType: 'movie', $index_averageRating: { $gt: 7.5 } }, columns: ['$id', 'name'] },
    ],
    native: { prefix: `${DATASET_PREFIX}/imdb-1m` },
    iceberg: { prefix: `${DATASET_PREFIX}/imdb-1m-iceberg`, metadataPath: 'metadata/v1.metadata.json' },
    delta: { prefix: `${DATASET_PREFIX}/imdb-1m-delta`, logPath: '_delta_log' },
  },
  'onet': {
    name: 'O*NET Sample',
    size: '~100KB',
    collections: ['occupations', 'skills', 'occupation-skills'],
    queries: [
      { name: 'Full scan occupations', file: 'occupations.parquet', columns: ['$id', 'name'] },
      { name: 'Filter by jobZone (4)', file: 'occupations.parquet', filter: { $index_jobZone: 4 }, columns: ['$id', 'name'] },
      { name: 'Skills by category', file: 'skills.parquet', filter: { $index_category: 'Basic Skills' }, columns: ['$id', 'name'] },
    ],
    native: { prefix: `${DATASET_PREFIX}/onet` },
    iceberg: { prefix: `${DATASET_PREFIX}/onet-iceberg`, metadataPath: 'metadata/v1.metadata.json' },
    delta: { prefix: `${DATASET_PREFIX}/onet-delta`, logPath: '_delta_log' },
  },
  'onet-full': {
    name: 'O*NET Full (1050 occupations)',
    size: '~10MB',
    collections: ['occupations', 'skills', 'abilities', 'knowledge', 'occupation-skills'],
    queries: [
      { name: 'Full scan occupations', file: 'occupations.parquet', columns: ['$id', 'name'] },
      { name: 'Filter by jobZone (4)', file: 'occupations.parquet', filter: { $index_jobZone: 4 }, columns: ['$id', 'name'] },
      { name: 'Computer occupations (15-)', file: 'occupations.parquet', filter: { $index_majorGroup: '15' }, columns: ['$id', 'name'] },
      { name: 'Full scan skills (73K)', file: 'occupation-skills.parquet', columns: ['$id', '$index_socCode'] },
      { name: 'High importance skills (>=4.0)', file: 'occupation-skills.parquet', filter: { $index_dataValue: { $gte: 4.0 } }, columns: ['$id', '$index_socCode'] },
    ],
    native: { prefix: `${DATASET_PREFIX}/onet-full` },
    iceberg: { prefix: `${DATASET_PREFIX}/onet-full-iceberg`, metadataPath: 'metadata/v1.metadata.json' },
    delta: { prefix: `${DATASET_PREFIX}/onet-full-delta`, logPath: '_delta_log' },
  },
  'unspsc': {
    name: 'UNSPSC Sample',
    size: '~15KB',
    collections: ['segments', 'families', 'classes', 'commodities'],
    queries: [
      { name: 'Full scan commodities', file: 'commodities.parquet', columns: ['$id', 'name'] },
      { name: 'Filter by segment (IT)', file: 'commodities.parquet', filter: { $index_segmentCode: '43' }, columns: ['$id', 'name'] },
      { name: 'Code prefix (4310)', file: 'commodities.parquet', filter: { $index_code: { $gte: '43100000', $lt: '43110000' } }, columns: ['$id', 'name'] },
    ],
    native: { prefix: `${DATASET_PREFIX}/unspsc` },
    iceberg: { prefix: `${DATASET_PREFIX}/unspsc-iceberg`, metadataPath: 'metadata/v1.metadata.json' },
    delta: { prefix: `${DATASET_PREFIX}/unspsc-delta`, logPath: '_delta_log' },
  },
  'unspsc-full': {
    name: 'UNSPSC Full (70K commodities)',
    size: '~50MB',
    collections: ['segments', 'families', 'classes', 'commodities'],
    queries: [
      { name: 'Full scan commodities (70K)', file: 'commodities.parquet', columns: ['$id', 'name'] },
      { name: 'Filter by segment (IT - 43)', file: 'commodities.parquet', filter: { $index_segmentCode: '43' }, columns: ['$id', 'name'] },
      { name: 'Code prefix (431001)', file: 'commodities.parquet', filter: { $index_code: { $gte: '43100100', $lt: '43100200' } }, columns: ['$id', 'name'] },
      { name: 'Family filter (4311)', file: 'commodities.parquet', filter: { $index_familyCode: '4311' }, columns: ['$id', 'name'] },
      { name: 'Class filter (431115)', file: 'commodities.parquet', filter: { $index_classCode: '431115' }, columns: ['$id', 'name'] },
    ],
    native: { prefix: `${DATASET_PREFIX}/unspsc-full` },
    iceberg: { prefix: `${DATASET_PREFIX}/unspsc-full-iceberg`, metadataPath: 'metadata/v1.metadata.json' },
    delta: { prefix: `${DATASET_PREFIX}/unspsc-full-delta`, logPath: '_delta_log' },
  },
  'wiktionary': {
    name: 'Wiktionary (English)',
    size: '~500MB',
    collections: ['words', 'definitions', 'pronunciations', 'translations'],
    queries: [
      { name: 'Full scan words', file: 'words.parquet', columns: ['$id', 'name'] },
      { name: 'Filter by part of speech', file: 'words.parquet', filter: { $index_partOfSpeech: 'noun' }, columns: ['$id', 'name'] },
      { name: 'Filter by language', file: 'words.parquet', filter: { $index_language: 'en' }, columns: ['$id', 'name'] },
      { name: 'Full scan definitions', file: 'definitions.parquet', columns: ['$id', 'text'] },
      { name: 'Words with pronunciations', file: 'pronunciations.parquet', filter: { $index_hasIPA: true }, columns: ['$id', 'ipa'] },
    ],
    native: { prefix: `${DATASET_PREFIX}/wiktionary` },
    iceberg: { prefix: `${DATASET_PREFIX}/wiktionary-iceberg`, metadataPath: 'metadata/v1.metadata.json' },
    delta: { prefix: `${DATASET_PREFIX}/wiktionary-delta`, logPath: '_delta_log' },
  },
  'wikidata': {
    name: 'Wikidata (Items)',
    size: '~1GB+',
    collections: ['items', 'properties', 'claims'],
    queries: [
      { name: 'Full scan items sample', file: 'items.parquet', columns: ['$id', 'label'] },
      { name: 'Filter by type (human)', file: 'items.parquet', filter: { $index_instanceOf: 'Q5' }, columns: ['$id', 'label'] },
      { name: 'Filter by country', file: 'items.parquet', filter: { $index_country: 'Q30' }, columns: ['$id', 'label'] },
      { name: 'Properties scan', file: 'properties.parquet', columns: ['$id', 'label'] },
      { name: 'Claims by property', file: 'claims.parquet', filter: { $index_property: 'P31' }, columns: ['$id', 'value'] },
    ],
    native: { prefix: `${DATASET_PREFIX}/wikidata` },
    iceberg: { prefix: `${DATASET_PREFIX}/wikidata-iceberg`, metadataPath: 'metadata/v1.metadata.json' },
    delta: { prefix: `${DATASET_PREFIX}/wikidata-delta`, logPath: '_delta_log' },
  },
}

interface QueryDefinition {
  name: string
  file: string
  columns: string[]
  filter?: Record<string, unknown> | undefined
}

// =============================================================================
// Utility Functions
// =============================================================================

function calculateStats(samples: number[]): LatencyStats {
  if (samples.length === 0) {
    return { p50: 0, p95: 0, p99: 0, mean: 0, min: 0, max: 0 }
  }

  const sorted = [...samples].sort((a, b) => a - b)
  const n = samples.length
  const sum = sorted.reduce((a, b) => a + b, 0)

  const percentile = (p: number) => {
    const idx = Math.ceil((p / 100) * n) - 1
    return sorted[Math.max(0, Math.min(idx, n - 1))] ?? 0
  }

  return {
    p50: Math.round(percentile(50)),
    p95: Math.round(percentile(95)),
    p99: Math.round(percentile(99)),
    mean: Math.round(sum / n),
    min: Math.round(sorted[0] ?? 0),
    max: Math.round(sorted[n - 1] ?? 0),
  }
}

function createTrackedR2File(bucket: R2Bucket, key: string, size: number) {
  const file = {
    byteLength: size,
    bytesRead: 0,
    slice: async (start: number, end: number): Promise<ArrayBuffer> => {
      const obj = await bucket.get(key, { range: { offset: start, length: end - start } })
      if (!obj) throw new Error(`Object not found: ${key}`)
      file.bytesRead += (end - start)
      return obj.arrayBuffer()
    },
  }
  return file
}

async function checkDatasetAvailable(bucket: R2Bucket, prefix: string, file: string): Promise<{ available: boolean; size?: number | undefined }> {
  try {
    const path = `${prefix}/${file}`
    const head = await bucket.head(path)
    return { available: head !== null, size: head?.size }
  } catch {
    return { available: false }
  }
}

// =============================================================================
// Auto-Migration Support
// =============================================================================

interface MigrationResult {
  success: boolean
  migrated: string[]
  errors: string[]
  durationMs: number
}

/**
 * Auto-migrate native data to Iceberg and/or Delta formats
 * This implements backend evolution - seamlessly converting data when switching formats.
 */
async function autoMigrateDatasets(
  bucket: R2Bucket,
  datasets: string[],
  targetBackends: ('iceberg' | 'delta')[],
  datasetsConfig: typeof DATASETS
): Promise<MigrationResult> {
  const startTime = performance.now()
  const migrated: string[] = []
  const errors: string[] = []

  // Create StorageBackend from R2Bucket
  const storage = new R2Backend(bucket as unknown as import('../storage/types/r2').R2Bucket)

  for (const datasetId of datasets) {
    const dataset = datasetsConfig[datasetId]
    if (!dataset) continue

    // Check if native data exists
    const nativeExists = await checkDatasetAvailable(bucket, dataset.native.prefix, dataset.queries[0]?.file ?? 'data.parquet')
    if (!nativeExists.available) {
      logger.debug(`No native data found for ${datasetId}, skipping migration`)
      continue
    }

    for (const targetBackend of targetBackends) {
      const targetConfig = targetBackend === 'iceberg' ? dataset.iceberg : dataset.delta

      if (!targetConfig) {
        logger.debug(`No ${targetBackend} config for ${datasetId}`)
        continue
      }

      // Check if target format already exists
      const metadataPath = targetBackend === 'iceberg'
        ? `${targetConfig.prefix}/${(targetConfig as { metadataPath: string }).metadataPath}`
        : `${targetConfig.prefix}/${(targetConfig as { logPath: string }).logPath}/00000000000000000000.json`

      const targetExists = await bucket.head(metadataPath)

      if (targetExists) {
        logger.debug(`${targetBackend} data already exists for ${datasetId}`)
        continue
      }

      // Perform migration
      logger.info(`Auto-migrating ${datasetId} from native to ${targetBackend}`)

      try {
        // Map dataset collections to namespaces
        const namespaces = dataset.collections.map(c => `${datasetId}/${c}`)

        const result = await migrateBackend({
          storage,
          from: 'native',
          to: targetBackend,
          namespaces,
          onProgress: (progress) => {
            logger.debug(`Migration progress: ${progress.namespace} - ${progress.entitiesMigrated}/${progress.totalEntities}`)
          },
        })

        if (result.success) {
          migrated.push(`${datasetId} → ${targetBackend}`)
        } else {
          errors.push(...result.errors)
        }
      } catch (err) {
        const errorMsg = `Failed to migrate ${datasetId} to ${targetBackend}: ${err instanceof Error ? err.message : 'Unknown error'}`
        logger.error(errorMsg)
        errors.push(errorMsg)
      }
    }
  }

  return {
    success: errors.length === 0,
    migrated,
    errors,
    durationMs: Math.round(performance.now() - startTime),
  }
}

// =============================================================================
// Native Parquet Benchmark
// =============================================================================

async function benchmarkNativeQueries(
  bucket: R2Bucket,
  _datasetId: string,
  dataset: typeof DATASETS[string],
  config: DatasetBackendConfig
): Promise<BackendDatasetResult> {
  const startTime = performance.now()
  const queries: QueryResult[] = []
  let totalBytesRead = 0

  // Check if native data exists
  const firstQuery = dataset.queries[0]
  if (!firstQuery) {
    return {
      backend: 'native',
      format: 'Parquet',
      available: false,
      queries: [],
      totalBytesRead: 0,
      totalTimeMs: 0,
      error: 'No queries defined',
    }
  }

  const check = await checkDatasetAvailable(bucket, dataset.native.prefix, firstQuery.file)
  if (!check.available) {
    return {
      backend: 'native',
      format: 'Parquet',
      available: false,
      queries: [],
      totalBytesRead: 0,
      totalTimeMs: performance.now() - startTime,
      error: `Dataset not found at ${dataset.native.prefix}/${firstQuery.file}`,
    }
  }

  const queriesToRun = dataset.queries.slice(0, config.maxQueries)

  for (const queryDef of queriesToRun) {
    const filePath = `${dataset.native.prefix}/${queryDef.file}`
    const head = await bucket.head(filePath)

    if (!head) {
      queries.push({
        query: queryDef.name,
        description: `File not found: ${queryDef.file}`,
        latency: { p50: 0, p95: 0, p99: 0, mean: 0, min: 0, max: 0 },
        bytesRead: 0,
        rowsReturned: 0,
        rowsScanned: 0,
      })
      continue
    }

    const latencies: number[] = []
    let bytesRead = 0
    let rowsReturned = 0

    for (let i = 0; i < config.iterations; i++) {
      const file = createTrackedR2File(bucket, filePath, head.size)
      const queryStart = performance.now()

      try {
        const rows = await parquetQuery({
          file,
          columns: queryDef.columns,
          filter: queryDef.filter,
          compressors,
        })

        latencies.push(performance.now() - queryStart)
        bytesRead = file.bytesRead
        rowsReturned = rows.length
      } catch (err) {
        logger.error(`Query error: ${queryDef.name}`, { error: err })
        latencies.push(performance.now() - queryStart)
      }
    }

    totalBytesRead += bytesRead
    queries.push({
      query: queryDef.name,
      description: queryDef.filter ? `Filter: ${JSON.stringify(queryDef.filter)}` : 'Full scan',
      latency: calculateStats(latencies),
      bytesRead,
      rowsReturned,
      rowsScanned: rowsReturned, // Native scans all matching rows
    })
  }

  return {
    backend: 'native',
    format: 'Parquet',
    available: true,
    queries,
    totalBytesRead,
    totalTimeMs: Math.round(performance.now() - startTime),
  }
}

// =============================================================================
// Apache Iceberg Benchmark
// =============================================================================

async function benchmarkIcebergQueries(
  bucket: R2Bucket,
  _datasetId: string,
  dataset: typeof DATASETS[string],
  config: DatasetBackendConfig
): Promise<BackendDatasetResult> {
  const startTime = performance.now()

  if (!dataset.iceberg) {
    return {
      backend: 'iceberg',
      format: 'Apache Iceberg',
      available: false,
      queries: [],
      totalBytesRead: 0,
      totalTimeMs: 0,
      error: 'Iceberg format not configured for this dataset',
    }
  }

  // Check if Iceberg metadata exists
  const metadataPath = `${dataset.iceberg.prefix}/${dataset.iceberg.metadataPath}`
  const metadataCheck = await bucket.head(metadataPath)

  if (!metadataCheck) {
    return {
      backend: 'iceberg',
      format: 'Apache Iceberg',
      available: false,
      queries: [],
      totalBytesRead: 0,
      totalTimeMs: performance.now() - startTime,
      error: `Iceberg metadata not found at ${metadataPath}. Run dataset conversion first.`,
    }
  }

  // Parse Iceberg metadata to find data files
  const queries: QueryResult[] = []
  let totalBytesRead = 0

  try {
    // Read metadata
    const metadataObj = await bucket.get(metadataPath)
    if (!metadataObj) throw new Error('Failed to read metadata')

    const metadataBytes = await metadataObj.arrayBuffer()
    const metadata = JSON.parse(new TextDecoder().decode(metadataBytes))
    totalBytesRead += metadataBytes.byteLength

    // Find current snapshot
    const currentSnapshotId = metadata['current-snapshot-id']
    const snapshot = metadata.snapshots?.find((s: { 'snapshot-id': number }) => s['snapshot-id'] === currentSnapshotId)

    if (!snapshot) {
      return {
        backend: 'iceberg',
        format: 'Apache Iceberg',
        available: false,
        queries: [],
        totalBytesRead,
        totalTimeMs: performance.now() - startTime,
        error: 'No current snapshot found in Iceberg metadata',
      }
    }

    // Read manifest list
    const manifestListPath = `${dataset.iceberg.prefix}/${snapshot['manifest-list']}`
    const manifestListObj = await bucket.get(manifestListPath)
    if (!manifestListObj) throw new Error('Failed to read manifest list')

    const manifestListBytes = await manifestListObj.arrayBuffer()
    totalBytesRead += manifestListBytes.byteLength

    // For now, just report that Iceberg is available but queries need implementation
    // Real implementation would parse manifests and query data files
    const queriesToRun = dataset.queries.slice(0, config.maxQueries)

    for (const queryDef of queriesToRun) {
      queries.push({
        query: queryDef.name,
        description: 'Iceberg query (metadata overhead included)',
        latency: { p50: 0, p95: 0, p99: 0, mean: 0, min: 0, max: 0 },
        bytesRead: totalBytesRead,
        rowsReturned: 0,
        rowsScanned: 0,
      })
    }

    return {
      backend: 'iceberg',
      format: 'Apache Iceberg',
      available: true,
      queries,
      totalBytesRead,
      totalTimeMs: Math.round(performance.now() - startTime),
      error: 'Iceberg query execution pending - metadata found',
    }
  } catch (err) {
    return {
      backend: 'iceberg',
      format: 'Apache Iceberg',
      available: false,
      queries: [],
      totalBytesRead,
      totalTimeMs: performance.now() - startTime,
      error: `Iceberg error: ${err instanceof Error ? err.message : 'Unknown error'}`,
    }
  }
}

// =============================================================================
// Delta Lake Benchmark
// =============================================================================

async function benchmarkDeltaQueries(
  bucket: R2Bucket,
  _datasetId: string,
  dataset: typeof DATASETS[string],
  config: DatasetBackendConfig
): Promise<BackendDatasetResult> {
  const startTime = performance.now()

  if (!dataset.delta) {
    return {
      backend: 'delta',
      format: 'Delta Lake',
      available: false,
      queries: [],
      totalBytesRead: 0,
      totalTimeMs: 0,
      error: 'Delta format not configured for this dataset',
    }
  }

  // Check if Delta log exists
  const logPath = `${dataset.delta.prefix}/${dataset.delta.logPath}/00000000000000000000.json`
  const logCheck = await bucket.head(logPath)

  if (!logCheck) {
    return {
      backend: 'delta',
      format: 'Delta Lake',
      available: false,
      queries: [],
      totalBytesRead: 0,
      totalTimeMs: performance.now() - startTime,
      error: `Delta log not found at ${logPath}. Run dataset conversion first.`,
    }
  }

  const queries: QueryResult[] = []
  let totalBytesRead = 0

  try {
    // Read Delta log
    const logObj = await bucket.get(logPath)
    if (!logObj) throw new Error('Failed to read Delta log')

    const logBytes = await logObj.arrayBuffer()
    totalBytesRead += logBytes.byteLength

    // Parse commit log
    const logText = new TextDecoder().decode(logBytes)

    // For now, report that Delta is available
    const queriesToRun = dataset.queries.slice(0, config.maxQueries)

    for (const queryDef of queriesToRun) {
      queries.push({
        query: queryDef.name,
        description: 'Delta query (commit log overhead included)',
        latency: { p50: 0, p95: 0, p99: 0, mean: 0, min: 0, max: 0 },
        bytesRead: totalBytesRead,
        rowsReturned: 0,
        rowsScanned: 0,
      })
    }

    return {
      backend: 'delta',
      format: 'Delta Lake',
      available: true,
      queries,
      totalBytesRead,
      totalTimeMs: Math.round(performance.now() - startTime),
      error: 'Delta query execution pending - log found',
    }
  } catch (err) {
    return {
      backend: 'delta',
      format: 'Delta Lake',
      available: false,
      queries: [],
      totalBytesRead,
      totalTimeMs: performance.now() - startTime,
      error: `Delta error: ${err instanceof Error ? err.message : 'Unknown error'}`,
    }
  }
}

// =============================================================================
// Main Benchmark Runner
// =============================================================================

async function runDatasetBackendBenchmark(
  bucket: R2Bucket,
  config: DatasetBackendConfig
): Promise<BenchmarkResult & { migration?: MigrationResult | undefined }> {
  const startTime = performance.now()
  const results: DatasetResult[] = []
  let migrationResult: MigrationResult | undefined

  // Auto-migrate if requested
  if (config.autoMigrate) {
    const targetBackends = config.backends.filter(b => b !== 'native') as ('iceberg' | 'delta')[]
    if (targetBackends.length > 0) {
      logger.info(`Auto-migration enabled for backends: ${targetBackends.join(', ')}`)
      migrationResult = await autoMigrateDatasets(bucket, config.datasets, targetBackends, DATASETS)
      logger.info(`Migration complete: ${migrationResult.migrated.length} datasets migrated in ${migrationResult.durationMs}ms`)
    }
  }

  for (const datasetId of config.datasets) {
    const dataset = DATASETS[datasetId]
    if (!dataset) {
      results.push({
        dataset: datasetId,
        size: 'unknown',
        backends: [{
          backend: 'native',
          format: 'Parquet',
          available: false,
          queries: [],
          totalBytesRead: 0,
          totalTimeMs: 0,
          error: `Unknown dataset: ${datasetId}`,
        }],
      })
      continue
    }

    const backendResults: BackendDatasetResult[] = []

    for (const backend of config.backends) {
      if (backend === 'native') {
        backendResults.push(await benchmarkNativeQueries(bucket, datasetId, dataset, config))
      } else if (backend === 'iceberg') {
        backendResults.push(await benchmarkIcebergQueries(bucket, datasetId, dataset, config))
      } else if (backend === 'delta') {
        backendResults.push(await benchmarkDeltaQueries(bucket, datasetId, dataset, config))
      }
    }

    // Calculate comparison
    const availableBackends = backendResults.filter(b => b.available && b.queries.length > 0)
    let comparison: DatasetResult['comparison']

    if (availableBackends.length > 1) {
      const avgLatencies = availableBackends.map(b => ({
        backend: b.backend,
        avgLatency: b.queries.reduce((sum, q) => sum + q.latency.p50, 0) / b.queries.length,
      }))
      avgLatencies.sort((a, b) => a.avgLatency - b.avgLatency)

      if (avgLatencies.length >= 2 && avgLatencies[0] && avgLatencies[1]) {
        const fastest = avgLatencies[0]
        const slowest = avgLatencies[avgLatencies.length - 1]
        if (slowest && fastest.avgLatency > 0) {
          comparison = {
            fastestBackend: fastest.backend,
            speedup: `${(slowest.avgLatency / fastest.avgLatency).toFixed(1)}x faster than ${slowest.backend}`,
          }
        }
      }
    }

    results.push({
      dataset: datasetId,
      size: dataset.size,
      backends: backendResults,
      comparison,
    })
  }

  // Calculate summary
  const avgLatencyByBackend: Record<string, number> = {}
  let totalQueries = 0

  for (const result of results) {
    for (const backend of result.backends) {
      if (backend.available) {
        const backendName = backend.backend
        const latencies = backend.queries.map(q => q.latency.p50).filter(l => l > 0)
        if (latencies.length > 0) {
          const avg = latencies.reduce((a, b) => a + b, 0) / latencies.length
          avgLatencyByBackend[backendName] = (avgLatencyByBackend[backendName] || 0) + avg
          totalQueries += backend.queries.length
        }
      }
    }
  }

  // Average across all datasets
  for (const backend of Object.keys(avgLatencyByBackend)) {
    avgLatencyByBackend[backend] = Math.round(avgLatencyByBackend[backend]! / results.length)
  }

  // Determine recommendation
  let recommendation = 'Native Parquet recommended for best performance'
  const sortedBackends = Object.entries(avgLatencyByBackend).sort((a, b) => a[1] - b[1])
  if (sortedBackends.length > 0 && sortedBackends[0]) {
    const [fastestBackend] = sortedBackends[0]
    if (fastestBackend === 'iceberg') {
      recommendation = 'Iceberg recommended - provides time-travel with competitive performance'
    } else if (fastestBackend === 'delta') {
      recommendation = 'Delta Lake recommended - provides ACID transactions with competitive performance'
    }
  }

  return {
    config,
    results,
    summary: {
      datasetsCount: results.length,
      backendsCount: config.backends.length,
      queriesExecuted: totalQueries,
      avgLatencyByBackend,
      recommendation,
    },
    metadata: {
      timestamp: new Date().toISOString(),
      durationMs: Math.round(performance.now() - startTime),
    },
    ...(migrationResult && { migration: migrationResult }),
  }
}

// =============================================================================
// HTTP Handler
// =============================================================================

export async function handleDatasetBackendsBenchmarkRequest(
  request: Request,
  bucket: R2Bucket
): Promise<Response> {
  const url = new URL(request.url)
  const params = url.searchParams

  // Parse config
  const datasetParam = params.get('dataset') || 'all'
  const backendParam = params.get('backend') || 'all'
  const iterations = Math.min(parseInt(params.get('iterations') || '3'), 10)
  const maxQueries = Math.min(parseInt(params.get('maxQueries') || '5'), 10)
  const autoMigrate = params.get('migrate') === 'true'

  // Determine datasets to test
  const availableDatasets = Object.keys(DATASETS)
  const datasets = datasetParam === 'all'
    ? availableDatasets
    : datasetParam.split(',').filter(d => availableDatasets.includes(d))

  // Determine backends to test
  const allBackends: ('native' | 'iceberg' | 'delta')[] = ['native', 'iceberg', 'delta']
  const backends = backendParam === 'all'
    ? allBackends
    : backendParam.split(',').filter(b => allBackends.includes(b as typeof allBackends[number])) as typeof allBackends

  const config: DatasetBackendConfig = {
    datasets,
    backends,
    iterations,
    maxQueries,
    autoMigrate,
  }

  try {
    const result = await runDatasetBackendBenchmark(bucket, config)

    // Add colo info if available
    const cf = (request as Request & { cf?: { colo?: string | undefined } | undefined }).cf
    if (cf?.colo) {
      result.metadata.colo = cf.colo
    }

    return new Response(JSON.stringify(result, null, 2), {
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'no-cache',
        'Access-Control-Allow-Origin': '*',
      },
    })
  } catch (err) {
    logger.error('Dataset backends benchmark error', { error: err })
    return new Response(JSON.stringify({
      error: err instanceof Error ? err.message : 'Unknown error',
      config,
    }), {
      status: 500,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
      },
    })
  }
}

// Export available datasets for documentation
export { DATASETS }
