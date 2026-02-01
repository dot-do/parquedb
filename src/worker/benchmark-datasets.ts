/**
 * Dataset Benchmark Module
 *
 * Real-world benchmarks against actual dataset files in R2.
 * Tests real R2 network I/O performance with various query patterns.
 */

import { parquetQuery } from 'hyparquet'
import { compressors } from '../parquet/compressors'
import { logger } from '../utils/logger'

// =============================================================================
// Types
// =============================================================================

export interface DatasetBenchmarkConfig {
  /** Max iterations per query */
  iterations: number
  /** Max queries to run per dataset */
  maxQueries: number
  /** Warmup iterations (not counted) */
  warmupIterations: number
  /** Specific dataset to test (or 'all' for all datasets) */
  dataset: string
}

export interface DatasetBenchmarkResult {
  config: DatasetBenchmarkConfig
  datasets: DatasetQueryResult[]
  summary: BenchmarkSummary
  totalTimeMs: number
}

interface DatasetQueryResult {
  dataset: string
  files: FileInfo[]
  queries: QueryBenchmark[]
  totalBytesRead: number
  totalTimeMs: number
}

interface FileInfo {
  name: string
  path: string
  size: number
  exists: boolean
}

interface QueryBenchmark {
  name: string
  description: string
  file: string
  metrics: QueryMetrics
}

interface QueryMetrics {
  /** Median latency in ms */
  medianMs: number
  /** 95th percentile latency */
  p95Ms: number
  /** Min latency */
  minMs: number
  /** Max latency */
  maxMs: number
  /** Bytes read from R2 */
  bytesRead: number
  /** Rows scanned */
  rowsScanned: number
  /** Rows returned after filter */
  rowsReturned: number
  /** All latency measurements */
  latencies: number[]
}

interface BenchmarkSummary {
  /** Total datasets tested */
  datasetsCount: number
  /** Total queries executed */
  queriesExecuted: number
  /** Total bytes read from R2 */
  totalBytesRead: number
  /** Average latency across all queries */
  avgLatencyMs: number
  /** Fastest query */
  fastestQuery: { name: string; dataset: string; latencyMs: number }
  /** Slowest query */
  slowestQuery: { name: string; dataset: string; latencyMs: number }
}

// =============================================================================
// R2 File Wrapper with Byte Tracking
// =============================================================================

interface TrackedFile {
  byteLength: number
  bytesRead: number
  slice: (start: number, end: number) => Promise<ArrayBuffer>
}

function createTrackedR2File(bucket: R2Bucket, key: string, size: number): TrackedFile {
  const file: TrackedFile = {
    byteLength: size,
    bytesRead: 0,
    slice: async (start: number, end: number): Promise<ArrayBuffer> => {
      const range = { offset: start, length: end - start }
      const obj = await bucket.get(key, { range })
      if (!obj) throw new Error(`Object not found: ${key}`)
      file.bytesRead += end - start
      return obj.arrayBuffer()
    },
  }
  return file
}

// =============================================================================
// Query Definitions by Dataset
// =============================================================================

interface QueryDefinition {
  name: string
  description: string
  file: string
  filter?: Record<string, unknown>
  columns?: string[]
}

const DATASET_QUERIES: Record<string, QueryDefinition[]> = {
  // IMDB dataset queries (100k sample)
  imdb: [
    {
      name: 'Full scan titles',
      description: 'Read all titles without filter',
      file: 'titles.parquet',
      columns: ['$id', 'name'],
    },
    {
      name: 'Filter by titleType (movie)',
      description: 'Select only movies using index column',
      file: 'titles.parquet',
      filter: { $index_titleType: 'movie' },
      columns: ['$id', 'name', '$index_titleType'],
    },
    {
      name: 'Filter by year range (2000-2010)',
      description: 'Numeric range filter on startYear',
      file: 'titles.parquet',
      filter: { $index_startYear: { $gte: 2000, $lte: 2010 } },
      columns: ['$id', 'name', '$index_startYear'],
    },
    {
      name: 'High rating filter (>8.0)',
      description: 'Filter by averageRating threshold',
      file: 'titles.parquet',
      filter: { $index_averageRating: { $gt: 8.0 } },
      columns: ['$id', 'name', '$index_averageRating'],
    },
    {
      name: 'People by profession (actor)',
      description: 'Filter people by profession',
      file: 'people.parquet',
      filter: { $index_primaryProfession: 'actor' },
      columns: ['$id', 'name', '$index_primaryProfession'],
    },
    {
      name: 'Cast by category (director)',
      description: 'Filter cast entries by category',
      file: 'cast.parquet',
      filter: { $index_category: 'director' },
      columns: ['$id', '$index_tconst', '$index_nconst', '$index_category'],
    },
  ],

  // IMDB 1M dataset (larger scale test)
  'imdb-1m': [
    {
      name: 'Full scan titles (1M)',
      description: 'Read 1M titles without filter - measures full scan I/O',
      file: 'titles.parquet',
      columns: ['$id', 'name'],
    },
    {
      name: 'Filter by titleType (movie)',
      description: 'Select only movies from 1M titles',
      file: 'titles.parquet',
      filter: { $index_titleType: 'movie' },
      columns: ['$id', 'name', '$index_titleType'],
    },
    {
      name: 'Filter by year range (2015-2020)',
      description: 'Recent movies - high selectivity filter',
      file: 'titles.parquet',
      filter: { $index_startYear: { $gte: 2015, $lte: 2020 } },
      columns: ['$id', 'name', '$index_startYear'],
    },
    {
      name: 'High vote count filter',
      description: 'Filter by numVotes >= 10000 (popular content)',
      file: 'titles.parquet',
      filter: { $index_numVotes: { $gte: 10000 } },
      columns: ['$id', 'name', '$index_numVotes'],
    },
    {
      name: 'Compound filter (movie + high rating)',
      description: 'Movies with rating > 7.5',
      file: 'titles.parquet',
      filter: { $index_titleType: 'movie', $index_averageRating: { $gt: 7.5 } },
      columns: ['$id', 'name', '$index_titleType', '$index_averageRating'],
    },
    {
      name: 'Cast full scan (1M)',
      description: 'Full scan of cast relationships',
      file: 'cast.parquet',
      columns: ['$id', '$index_tconst', '$index_category'],
    },
    {
      name: 'Cast by category (actor)',
      description: 'Filter cast by actor category',
      file: 'cast.parquet',
      filter: { $index_category: 'actor' },
      columns: ['$id', '$index_tconst', '$index_nconst', '$index_category'],
    },
  ],

  // O*NET dataset queries
  onet: [
    {
      name: 'Full scan occupations',
      description: 'Read all occupations',
      file: 'occupations.parquet',
      columns: ['$id', 'name'],
    },
    {
      name: 'Filter by jobZone (4)',
      description: 'Filter occupations requiring bachelors degree',
      file: 'occupations.parquet',
      filter: { $index_jobZone: 4 },
      columns: ['$id', 'name', '$index_jobZone'],
    },
    {
      name: 'Skills by category',
      description: 'Filter skills by category',
      file: 'skills.parquet',
      filter: { $index_category: 'Basic Skills' },
      columns: ['$id', 'name', '$index_category'],
    },
    {
      name: 'High importance skills',
      description: 'Filter occupation-skills by importance >= 4.0',
      file: 'occupation-skills.parquet',
      filter: { $index_importance: { $gte: 4.0 } },
      columns: ['$id', '$index_socCode', '$index_elementId', '$index_importance'],
    },
    {
      name: 'SOC code prefix filter',
      description: 'Filter by SOC code range (15-xxxx)',
      file: 'occupation-skills.parquet',
      filter: { $index_socCode: { $gte: '15-0000.00', $lt: '16-0000.00' } },
      columns: ['$id', '$index_socCode'],
    },
  ],

  // O*NET Full dataset (larger scale)
  'onet-full': [
    {
      name: 'Full scan occupations',
      description: 'Read all 1050 occupations',
      file: 'occupations.parquet',
      columns: ['$id', 'name'],
    },
    {
      name: 'Filter by jobZone (4)',
      description: 'Filter occupations requiring bachelors degree',
      file: 'occupations.parquet',
      filter: { $index_jobZone: 4 },
      columns: ['$id', 'name', '$index_jobZone'],
    },
    {
      name: 'Filter by major group (15-)',
      description: 'Computer occupations (SOC 15-xxxx)',
      file: 'occupations.parquet',
      filter: { $index_majorGroup: '15' },
      columns: ['$id', 'name', '$index_majorGroup'],
    },
    {
      name: 'Full scan occupation-skills (73K)',
      description: 'Full scan of all occupation-skill ratings',
      file: 'occupation-skills.parquet',
      columns: ['$id', '$index_socCode', '$index_elementId'],
    },
    {
      name: 'High dataValue skills (>= 4.0)',
      description: 'Filter occupation-skills by importance rating',
      file: 'occupation-skills.parquet',
      filter: { $index_dataValue: { $gte: 4.0 } },
      columns: ['$id', '$index_socCode', '$index_dataValue'],
    },
    {
      name: 'Filter by scaleId (IM)',
      description: 'Importance scale ratings only',
      file: 'occupation-skills.parquet',
      filter: { $index_scaleId: 'IM' },
      columns: ['$id', '$index_socCode', '$index_scaleId'],
    },
    {
      name: 'SOC code prefix filter (15-)',
      description: 'Computer occupation skills',
      file: 'occupation-skills.parquet',
      filter: { $index_socCode: { $gte: '15-0000.00', $lt: '16-0000.00' } },
      columns: ['$id', '$index_socCode', '$index_elementId'],
    },
  ],

  // UNSPSC dataset queries
  unspsc: [
    {
      name: 'Full scan commodities',
      description: 'Read all commodities',
      file: 'commodities.parquet',
      columns: ['$id', 'name'],
    },
    {
      name: 'Filter by segment (IT)',
      description: 'Filter commodities in IT segment (43)',
      file: 'commodities.parquet',
      filter: { $index_segmentCode: '43' },
      columns: ['$id', 'name', '$index_segmentCode'],
    },
    {
      name: 'Code prefix lookup',
      description: 'Filter by code prefix (4310)',
      file: 'commodities.parquet',
      filter: { $index_code: { $gte: '43100000', $lt: '43110000' } },
      columns: ['$id', '$index_code', 'name'],
    },
    {
      name: 'Family filter',
      description: 'Filter by family code',
      file: 'commodities.parquet',
      filter: { $index_familyCode: '4310' },
      columns: ['$id', '$index_familyCode', 'name'],
    },
  ],

  // UNSPSC Full dataset (70K commodities)
  'unspsc-full': [
    {
      name: 'Full scan commodities (70K)',
      description: 'Read all 70K commodities',
      file: 'commodities.parquet',
      columns: ['$id', 'name'],
    },
    {
      name: 'Filter by segment (IT - 43)',
      description: 'Filter IT segment commodities',
      file: 'commodities.parquet',
      filter: { $index_segmentCode: '43' },
      columns: ['$id', 'name', '$index_segmentCode'],
    },
    {
      name: 'Filter by segment (Office - 44)',
      description: 'Filter Office Equipment segment',
      file: 'commodities.parquet',
      filter: { $index_segmentCode: '44' },
      columns: ['$id', 'name', '$index_segmentCode'],
    },
    {
      name: 'Code range lookup',
      description: 'Filter by code range (4310xxxx)',
      file: 'commodities.parquet',
      filter: { $index_code: { $gte: '43100000', $lt: '43200000' } },
      columns: ['$id', '$index_code', 'name'],
    },
    {
      name: 'Family filter',
      description: 'Filter by family code',
      file: 'commodities.parquet',
      filter: { $index_familyCode: '4310' },
      columns: ['$id', '$index_familyCode', 'name'],
    },
    {
      name: 'Class filter',
      description: 'Filter by class code',
      file: 'commodities.parquet',
      filter: { $index_classCode: '431000' },
      columns: ['$id', '$index_classCode', 'name'],
    },
  ],
}

// =============================================================================
// Benchmark Runner
// =============================================================================

export async function runDatasetBenchmark(
  bucket: R2Bucket,
  config: DatasetBenchmarkConfig = {
    iterations: 2,
    maxQueries: 3,
    warmupIterations: 0,
    dataset: 'imdb',
  }
): Promise<DatasetBenchmarkResult> {
  const startTime = performance.now()
  const results: DatasetQueryResult[] = []

  let totalBytesRead = 0
  let allLatencies: { name: string; dataset: string; latencyMs: number }[] = []

  // Filter datasets based on config
  const datasetsToTest =
    config.dataset === 'all'
      ? Object.entries(DATASET_QUERIES)
      : Object.entries(DATASET_QUERIES).filter(([name]) => name === config.dataset)

  if (datasetsToTest.length === 0) {
    throw new Error(
      `Dataset '${config.dataset}' not found. Available: ${Object.keys(DATASET_QUERIES).join(', ')}`
    )
  }

  // Process each dataset
  for (const [dataset, queries] of datasetsToTest) {
    const datasetStartTime = performance.now()
    const datasetResult: DatasetQueryResult = {
      dataset,
      files: [],
      queries: [],
      totalBytesRead: 0,
      totalTimeMs: 0,
    }

    // Check which files exist
    const prefix = `benchmark-data/${dataset}/`
    const fileList = await bucket.list({ prefix, limit: 100 })

    const fileMap = new Map<string, R2Object>()
    for (const obj of fileList.objects) {
      const fileName = obj.key.replace(prefix, '')
      fileMap.set(fileName, obj)
      datasetResult.files.push({
        name: fileName,
        path: obj.key,
        size: obj.size,
        exists: true,
      })
    }

    if (fileList.objects.length === 0) {
      logger.info(`No files found for dataset: ${dataset}`)
      continue
    }

    // Run queries for this dataset
    const datasetQueries = queries.slice(0, config.maxQueries)

    for (const queryDef of datasetQueries) {
      const fileKey = `${prefix}${queryDef.file}`
      const fileInfo = fileMap.get(queryDef.file)

      if (!fileInfo) {
        logger.info(`File not found: ${fileKey}`)
        continue
      }

      // Run benchmark iterations
      const latencies: number[] = []
      let lastRowsScanned = 0
      let lastRowsReturned = 0
      let lastBytesRead = 0

      // Warmup iterations
      for (let i = 0; i < config.warmupIterations; i++) {
        const file = createTrackedR2File(bucket, fileKey, fileInfo.size)
        try {
          await parquetQuery({
            file,
            columns: queryDef.columns,
            filter: queryDef.filter,
            compressors,
          })
        } catch (e) {
          logger.debug(`Warmup error for ${queryDef.name}: ${e}`)
        }
      }

      // Measured iterations
      for (let i = 0; i < config.iterations; i++) {
        const file = createTrackedR2File(bucket, fileKey, fileInfo.size)

        const iterStart = performance.now()
        try {
          const rows = await parquetQuery({
            file,
            columns: queryDef.columns,
            filter: queryDef.filter,
            compressors,
          })
          const iterEnd = performance.now()

          latencies.push(iterEnd - iterStart)
          lastRowsScanned = rows.length
          lastRowsReturned = rows.length
          lastBytesRead = file.bytesRead
        } catch (e) {
          logger.debug(`Query error for ${queryDef.name}: ${e}`)
          latencies.push(-1)
        }
      }

      // Calculate metrics
      const validLatencies = latencies.filter((l) => l >= 0).sort((a, b) => a - b)

      if (validLatencies.length === 0) {
        continue
      }

      const metrics: QueryMetrics = {
        medianMs: validLatencies[Math.floor(validLatencies.length / 2)] ?? 0,
        p95Ms: validLatencies[Math.floor(validLatencies.length * 0.95)] ?? validLatencies[validLatencies.length - 1] ?? 0,
        minMs: Math.min(...validLatencies),
        maxMs: Math.max(...validLatencies),
        bytesRead: lastBytesRead,
        rowsScanned: lastRowsScanned,
        rowsReturned: lastRowsReturned,
        latencies: validLatencies,
      }

      datasetResult.queries.push({
        name: queryDef.name,
        description: queryDef.description,
        file: queryDef.file,
        metrics,
      })

      datasetResult.totalBytesRead += lastBytesRead
      allLatencies.push({
        name: queryDef.name,
        dataset,
        latencyMs: metrics.medianMs,
      })
    }

    datasetResult.totalTimeMs = performance.now() - datasetStartTime
    totalBytesRead += datasetResult.totalBytesRead
    results.push(datasetResult)
  }

  // Build summary
  const sortedLatencies = allLatencies.sort((a, b) => a.latencyMs - b.latencyMs)
  const avgLatency =
    allLatencies.length > 0 ? allLatencies.reduce((sum, l) => sum + l.latencyMs, 0) / allLatencies.length : 0

  const summary: BenchmarkSummary = {
    datasetsCount: results.length,
    queriesExecuted: allLatencies.length,
    totalBytesRead,
    avgLatencyMs: Math.round(avgLatency * 10) / 10,
    fastestQuery: sortedLatencies[0] || { name: 'N/A', dataset: 'N/A', latencyMs: 0 },
    slowestQuery: sortedLatencies[sortedLatencies.length - 1] || { name: 'N/A', dataset: 'N/A', latencyMs: 0 },
  }

  return {
    config,
    datasets: results,
    summary,
    totalTimeMs: Math.round(performance.now() - startTime),
  }
}

// =============================================================================
// HTTP Handler
// =============================================================================

export async function handleDatasetBenchmarkRequest(request: Request, bucket: R2Bucket): Promise<Response> {
  const url = new URL(request.url)
  const startTime = performance.now()

  // Parse config from query params
  // Lower defaults to avoid R2 API rate limits (1000 subrequests per worker invocation)
  const iterations = Math.min(parseInt(url.searchParams.get('iterations') || '2'), 5)
  const maxQueries = Math.min(parseInt(url.searchParams.get('maxQueries') || '3'), 10)
  const warmupIterations = Math.min(parseInt(url.searchParams.get('warmup') || '0'), 2)
  const dataset = url.searchParams.get('dataset') || 'imdb'

  // Check if data exists
  const checkPrefix = 'benchmark-data/'
  const checkResult = await bucket.list({ prefix: checkPrefix, limit: 1 })

  if (checkResult.objects.length === 0) {
    return Response.json(
      {
        error: true,
        message: 'No benchmark data found in R2',
        help: 'Run "node scripts/upload-benchmark-data.mjs" to upload the data-v3 files to R2',
        expectedPrefix: checkPrefix,
      },
      { status: 404 }
    )
  }

  try {
    const result = await runDatasetBenchmark(bucket, {
      iterations,
      maxQueries,
      warmupIterations,
      dataset,
    })

    const totalTime = Math.round(performance.now() - startTime)

    // Format results for readability
    const formattedDatasets = result.datasets.map((ds) => ({
      dataset: ds.dataset,
      files: ds.files.map((f) => ({
        name: f.name,
        size: `${(f.size / 1024).toFixed(1)} KB`,
      })),
      queries: ds.queries.map((q) => ({
        name: q.name,
        description: q.description,
        file: q.file,
        latency: {
          median: `${q.metrics.medianMs.toFixed(1)}ms`,
          p95: `${q.metrics.p95Ms.toFixed(1)}ms`,
          min: `${q.metrics.minMs.toFixed(1)}ms`,
          max: `${q.metrics.maxMs.toFixed(1)}ms`,
        },
        io: {
          bytesRead: `${(q.metrics.bytesRead / 1024).toFixed(1)} KB`,
          rowsReturned: q.metrics.rowsReturned,
        },
      })),
      totalBytesRead: `${(ds.totalBytesRead / 1024).toFixed(1)} KB`,
      totalTimeMs: Math.round(ds.totalTimeMs),
    }))

    return Response.json(
      {
        benchmark: 'Dataset R2 I/O Performance',
        description: 'Real-world R2 I/O measurements against actual dataset files',
        totalTimeMs: totalTime,
        config: result.config,
        datasets: formattedDatasets,
        summary: {
          ...result.summary,
          totalBytesRead: `${(result.summary.totalBytesRead / 1024 / 1024).toFixed(2)} MB`,
          avgLatency: `${result.summary.avgLatencyMs.toFixed(1)}ms`,
          fastestQuery: {
            ...result.summary.fastestQuery,
            latency: `${result.summary.fastestQuery.latencyMs.toFixed(1)}ms`,
          },
          slowestQuery: {
            ...result.summary.slowestQuery,
            latency: `${result.summary.slowestQuery.latencyMs.toFixed(1)}ms`,
          },
        },
        interpretation: {
          purpose: 'Measures actual R2 network I/O with real Parquet files',
          queryTypes: 'Tests full scans, equality filters, range filters, and predicate pushdown',
          metrics: 'Reports latency (median/p95), bytes read, and rows returned',
          recommendation: 'Compare predicate pushdown queries vs full scans to measure I/O savings',
        },
        availableDatasets: Object.keys(DATASET_QUERIES),
        usage: {
          singleDataset: '/benchmark-datasets?dataset=imdb',
          allDatasets: '/benchmark-datasets?dataset=all&maxQueries=2&iterations=1',
          customIterations: '/benchmark-datasets?dataset=onet-full&iterations=3&maxQueries=5',
        },
      },
      {
        headers: {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': '*',
          'Server-Timing': `total;dur=${totalTime}`,
        },
      }
    )
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error)
    const stack = error instanceof Error ? error.stack : undefined
    return Response.json(
      {
        error: true,
        message,
        stack,
      },
      { status: 500 }
    )
  }
}
