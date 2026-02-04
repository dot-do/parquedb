/**
 * Runtime Configuration for Cross-Platform Benchmarks
 *
 * Defines which storage backends, datasets, and configurations are supported
 * across different JavaScript runtimes (Browser, Worker, Node.js).
 *
 * Used by:
 * - Storage benchmark runners
 * - CI workflow configurations
 * - Dataset loading scripts
 */

// =============================================================================
// Types
// =============================================================================

/**
 * Supported runtime environments
 */
export type RuntimeName = 'browser' | 'worker' | 'node'

/**
 * Supported storage backends
 */
export type BackendName = 'cdn' | 'r2' | 'fs' | 'iceberg' | 'delta'

/**
 * Supported benchmark datasets
 */
export type DatasetName = 'imdb' | 'onet' | 'unspsc' | 'blog' | 'ecommerce'

/**
 * Fetch implementation available in each runtime
 */
export type FetchImplementation = 'native' | 'node-fetch' | 'undici'

/**
 * Configuration for a specific runtime environment
 */
export interface RuntimeConfig {
  /** Runtime identifier */
  name: RuntimeName
  /** Storage backends supported in this runtime */
  supportedBackends: BackendName[]
  /** Datasets that can be used in this runtime */
  supportedDatasets: DatasetName[]
  /** Which fetch implementation is used */
  fetchImplementation: FetchImplementation
  /** Human-readable notes about limitations or features */
  notes: string
}

/**
 * Size information for a dataset
 */
export interface DatasetSize {
  /** Approximate size in bytes */
  sizeBytes: number
  /** If true, only available via CDN (not bundled locally) */
  webOnly: boolean
  /** Human-readable description */
  description?: string
}

/**
 * Valid combination of runtime, backend, and datasets
 */
export interface RuntimeBackendCombination {
  runtime: RuntimeName
  backend: BackendName
  datasets: DatasetName[]
}

// =============================================================================
// Runtime Configurations
// =============================================================================

/**
 * Runtime configurations for all supported environments
 */
export const runtimeConfigs: RuntimeConfig[] = [
  {
    name: 'browser',
    supportedBackends: ['cdn'],  // Browser can ONLY use public CDN
    supportedDatasets: ['imdb', 'onet', 'unspsc', 'blog', 'ecommerce'],
    fetchImplementation: 'native',
    notes: 'fetch() with Range headers via CDN. No direct R2/FS access.',
  },
  {
    name: 'worker',
    supportedBackends: ['cdn', 'r2', 'iceberg', 'delta'],
    supportedDatasets: ['imdb', 'onet', 'unspsc', 'blog', 'ecommerce'],
    fetchImplementation: 'native',
    notes: 'Full access to R2 bindings, Iceberg/Delta formats. No FS.',
  },
  {
    name: 'node',
    supportedBackends: ['cdn', 'fs', 'r2'],
    supportedDatasets: ['onet', 'unspsc', 'blog', 'ecommerce'],  // Large datasets web-only
    fetchImplementation: 'undici',
    notes: 'Local FS for small datasets, CDN/R2 for reads. No IMDB (too large).',
  },
]

// =============================================================================
// Dataset Sizes
// =============================================================================

/**
 * Size constraints and metadata for each dataset
 */
export const datasetSizes: Record<DatasetName, DatasetSize> = {
  imdb: {
    sizeBytes: 500_000_000,  // ~500MB
    webOnly: true,
    description: 'IMDB movie database - large dataset, web/CDN only',
  },
  onet: {
    sizeBytes: 10_000_000,   // ~10MB
    webOnly: false,
    description: 'O*NET occupational database - medium dataset',
  },
  unspsc: {
    sizeBytes: 5_000_000,    // ~5MB
    webOnly: false,
    description: 'UNSPSC product classification - small dataset',
  },
  blog: {
    sizeBytes: 1_000_000,    // ~1MB
    webOnly: false,
    description: 'Blog posts/comments - synthetic test data',
  },
  ecommerce: {
    sizeBytes: 2_000_000,    // ~2MB
    webOnly: false,
    description: 'E-commerce products/orders - synthetic test data',
  },
}

// =============================================================================
// URL/Path Configuration
// =============================================================================

/**
 * Base URLs for CDN access
 */
export const CDN_CONFIG = {
  /** Production CDN base URL */
  baseUrl: 'https://cdn.parque.db',
  /** Development/staging CDN URL (R2 public bucket) */
  devUrl: process.env.CDN_R2_DEV_URL ?? 'https://pub-parquedb.r2.dev',
  /** Whether to use dev URL */
  useDev: process.env.NODE_ENV !== 'production',
} as const

/**
 * R2 configuration (requires credentials)
 */
export const R2_CONFIG = {
  /** R2 endpoint URL */
  endpoint: process.env.R2_URL ?? '',
  /** R2 bucket name */
  bucket: process.env.R2_BUCKET ?? 'parquedb',
  /** Access key ID */
  accessKeyId: process.env.R2_ACCESS_KEY_ID ?? '',
  /** Secret access key */
  secretAccessKey: process.env.R2_SECRET_ACCESS_KEY ?? '',
  /** Whether R2 credentials are configured */
  get isConfigured(): boolean {
    return Boolean(this.endpoint && this.accessKeyId && this.secretAccessKey)
  },
} as const

/**
 * Local filesystem configuration
 */
export const FS_CONFIG = {
  /** Base data directory */
  baseDir: process.env.PARQUEDB_DATA_DIR ?? './data',
} as const

// =============================================================================
// URL/Path Builders
// =============================================================================

/**
 * Get CDN URL for a dataset collection
 *
 * @param dataset - Dataset name
 * @param collection - Collection name within the dataset
 * @returns Full CDN URL to the Parquet file
 *
 * @example
 * getCdnUrl('imdb', 'movies')
 * // => 'https://cdn.parque.db/imdb/movies.parquet'
 */
export function getCdnUrl(dataset: DatasetName, collection: string): string {
  const baseUrl = CDN_CONFIG.useDev ? CDN_CONFIG.devUrl : CDN_CONFIG.baseUrl
  return `${baseUrl}/${dataset}/${collection}.parquet`
}

/**
 * Get R2 object key for a dataset collection
 *
 * @param dataset - Dataset name
 * @param collection - Collection name within the dataset
 * @returns R2 object key
 *
 * @example
 * getR2Key('imdb', 'movies')
 * // => 'datasets/imdb/movies.parquet'
 */
export function getR2Key(dataset: DatasetName, collection: string): string {
  return `datasets/${dataset}/${collection}.parquet`
}

/**
 * Get R2 URL for a dataset collection (S3-compatible)
 *
 * @param dataset - Dataset name
 * @param collection - Collection name within the dataset
 * @returns Full R2 URL (requires credentials to access)
 *
 * @example
 * getR2Url('imdb', 'movies')
 * // => 'https://account.r2.cloudflarestorage.com/parquedb/datasets/imdb/movies.parquet'
 */
export function getR2Url(dataset: DatasetName, collection: string): string {
  if (!R2_CONFIG.endpoint) {
    throw new Error('R2 endpoint not configured. Set R2_URL environment variable.')
  }
  return `${R2_CONFIG.endpoint}/${R2_CONFIG.bucket}/${getR2Key(dataset, collection)}`
}

/**
 * Get local filesystem path for a dataset collection
 *
 * @param dataset - Dataset name
 * @param collection - Collection name within the dataset
 * @returns Local file path
 *
 * @example
 * getFsPath('onet', 'occupations')
 * // => './data/onet/occupations.parquet'
 */
export function getFsPath(dataset: DatasetName, collection: string): string {
  return `${FS_CONFIG.baseDir}/${dataset}/${collection}.parquet`
}

// =============================================================================
// Validation Helpers
// =============================================================================

/**
 * Get runtime configuration by name
 *
 * @param runtime - Runtime name
 * @returns Runtime configuration or undefined if not found
 */
export function getRuntimeConfig(runtime: RuntimeName): RuntimeConfig | undefined {
  return runtimeConfigs.find(config => config.name === runtime)
}

/**
 * Check if a runtime supports a specific backend
 *
 * @param runtime - Runtime name
 * @param backend - Backend name
 * @returns True if the combination is valid
 */
export function supportsBackend(runtime: RuntimeName, backend: BackendName): boolean {
  const config = getRuntimeConfig(runtime)
  return config?.supportedBackends.includes(backend) ?? false
}

/**
 * Check if a runtime supports a specific dataset
 *
 * @param runtime - Runtime name
 * @param dataset - Dataset name
 * @returns True if the runtime can use this dataset
 */
export function supportsDataset(runtime: RuntimeName, dataset: DatasetName): boolean {
  const config = getRuntimeConfig(runtime)
  return config?.supportedDatasets.includes(dataset) ?? false
}

/**
 * Check if a specific combination of runtime, backend, and dataset is valid
 *
 * @param runtime - Runtime name
 * @param backend - Backend name
 * @param dataset - Dataset name
 * @returns True if all three can be used together
 *
 * @example
 * isValidCombination('browser', 'cdn', 'imdb')  // true
 * isValidCombination('browser', 'r2', 'imdb')   // false - browser can't use R2
 * isValidCombination('node', 'fs', 'imdb')      // false - imdb too large for local
 */
export function isValidCombination(
  runtime: RuntimeName,
  backend: BackendName,
  dataset: DatasetName
): boolean {
  const config = getRuntimeConfig(runtime)
  if (!config) return false

  // Check backend support
  if (!config.supportedBackends.includes(backend)) return false

  // Check dataset support
  if (!config.supportedDatasets.includes(dataset)) return false

  // Additional validation: FS backend can't use web-only datasets
  if (backend === 'fs' && datasetSizes[dataset].webOnly) return false

  return true
}

/**
 * Get all valid runtime/backend/dataset combinations
 *
 * @returns Array of all valid combinations
 *
 * @example
 * const combinations = getValidCombinations()
 * // [
 * //   { runtime: 'browser', backend: 'cdn', datasets: ['imdb', 'onet', ...] },
 * //   { runtime: 'worker', backend: 'cdn', datasets: ['imdb', 'onet', ...] },
 * //   { runtime: 'worker', backend: 'r2', datasets: ['imdb', 'onet', ...] },
 * //   ...
 * // ]
 */
export function getValidCombinations(): RuntimeBackendCombination[] {
  const combinations: RuntimeBackendCombination[] = []

  for (const config of runtimeConfigs) {
    for (const backend of config.supportedBackends) {
      // Filter datasets that work with this runtime/backend combo
      const validDatasets = config.supportedDatasets.filter(dataset => {
        // FS backend can't use web-only datasets
        if (backend === 'fs' && datasetSizes[dataset].webOnly) return false
        return true
      })

      if (validDatasets.length > 0) {
        combinations.push({
          runtime: config.name,
          backend,
          datasets: validDatasets,
        })
      }
    }
  }

  return combinations
}

/**
 * Get valid datasets for a specific runtime and backend
 *
 * @param runtime - Runtime name
 * @param backend - Backend name
 * @returns Array of valid dataset names
 */
export function getValidDatasets(runtime: RuntimeName, backend: BackendName): DatasetName[] {
  const config = getRuntimeConfig(runtime)
  if (!config || !config.supportedBackends.includes(backend)) {
    return []
  }

  return config.supportedDatasets.filter(dataset => {
    if (backend === 'fs' && datasetSizes[dataset].webOnly) return false
    return true
  })
}

/**
 * Get valid backends for a specific runtime
 *
 * @param runtime - Runtime name
 * @returns Array of valid backend names
 */
export function getValidBackends(runtime: RuntimeName): BackendName[] {
  const config = getRuntimeConfig(runtime)
  return config?.supportedBackends ?? []
}

// =============================================================================
// Runtime Detection
// =============================================================================

/**
 * Detect the current runtime environment
 *
 * @returns Detected runtime name
 */
export function detectRuntime(): RuntimeName {
  // Check for Cloudflare Workers
  if (typeof globalThis.caches !== 'undefined' &&
      typeof (globalThis as Record<string, unknown>).HTMLRewriter !== 'undefined') {
    return 'worker'
  }

  // Check for browser
  if (typeof window !== 'undefined' && typeof document !== 'undefined') {
    return 'browser'
  }

  // Default to Node.js
  return 'node'
}

/**
 * Get configuration for the current runtime
 *
 * @returns Runtime configuration for the detected environment
 */
export function getCurrentRuntimeConfig(): RuntimeConfig {
  const runtime = detectRuntime()
  const config = getRuntimeConfig(runtime)
  if (!config) {
    throw new Error(`No configuration found for runtime: ${runtime}`)
  }
  return config
}

// =============================================================================
// Benchmark Configuration Helpers
// =============================================================================

/**
 * Configuration for a benchmark run
 */
export interface BenchmarkRunConfig {
  runtime: RuntimeName
  backends: BackendName[]
  datasets: DatasetName[]
  iterations: number
  warmup: number
}

/**
 * Generate benchmark configuration for a specific runtime
 *
 * @param runtime - Target runtime
 * @param options - Override options
 * @returns Complete benchmark configuration
 */
export function getBenchmarkConfig(
  runtime: RuntimeName,
  options: Partial<BenchmarkRunConfig> = {}
): BenchmarkRunConfig {
  const config = getRuntimeConfig(runtime)
  if (!config) {
    throw new Error(`Unknown runtime: ${runtime}`)
  }

  return {
    runtime,
    backends: options.backends ?? config.supportedBackends,
    datasets: options.datasets ?? config.supportedDatasets,
    iterations: options.iterations ?? 10,
    warmup: options.warmup ?? 2,
  }
}

/**
 * Filter datasets by size threshold
 *
 * @param maxSizeBytes - Maximum dataset size in bytes
 * @returns Array of dataset names within the size limit
 */
export function getDatasetsBySize(maxSizeBytes: number): DatasetName[] {
  return (Object.entries(datasetSizes) as [DatasetName, DatasetSize][])
    .filter(([_, size]) => size.sizeBytes <= maxSizeBytes)
    .map(([name]) => name)
}

/**
 * Get total size of specified datasets
 *
 * @param datasets - Array of dataset names
 * @returns Total size in bytes
 */
export function getTotalDatasetSize(datasets: DatasetName[]): number {
  return datasets.reduce((total, dataset) => {
    return total + (datasetSizes[dataset]?.sizeBytes ?? 0)
  }, 0)
}

// =============================================================================
// Summary Helpers
// =============================================================================

/**
 * Get a summary of all runtime configurations
 *
 * @returns Human-readable summary string
 */
export function getRuntimeSummary(): string {
  const lines: string[] = [
    '=== Runtime Configuration Summary ===',
    '',
  ]

  for (const config of runtimeConfigs) {
    lines.push(`${config.name.toUpperCase()}:`)
    lines.push(`  Backends: ${config.supportedBackends.join(', ')}`)
    lines.push(`  Datasets: ${config.supportedDatasets.join(', ')}`)
    lines.push(`  Fetch: ${config.fetchImplementation}`)
    lines.push(`  Notes: ${config.notes}`)
    lines.push('')
  }

  return lines.join('\n')
}

/**
 * Get a summary of dataset sizes
 *
 * @returns Human-readable summary string
 */
export function getDatasetSummary(): string {
  const lines: string[] = [
    '=== Dataset Size Summary ===',
    '',
  ]

  const formatBytes = (bytes: number): string => {
    if (bytes >= 1_000_000_000) return `${(bytes / 1_000_000_000).toFixed(1)} GB`
    if (bytes >= 1_000_000) return `${(bytes / 1_000_000).toFixed(1)} MB`
    if (bytes >= 1_000) return `${(bytes / 1_000).toFixed(1)} KB`
    return `${bytes} B`
  }

  for (const [name, size] of Object.entries(datasetSizes) as [DatasetName, DatasetSize][]) {
    const webOnlyFlag = size.webOnly ? ' [WEB ONLY]' : ''
    lines.push(`${name}: ${formatBytes(size.sizeBytes)}${webOnlyFlag}`)
    if (size.description) {
      lines.push(`  ${size.description}`)
    }
  }

  return lines.join('\n')
}
