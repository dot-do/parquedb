/**
 * E2E Benchmark Baseline Store
 *
 * Stores and retrieves benchmark results from R2 for:
 * - Baseline comparisons (detect regressions)
 * - Historical trend analysis
 * - Result persistence across CI runs
 *
 * Storage layout in R2 bucket:
 *   benchmarks/baselines/{env}/latest.json - Current baseline
 *   benchmarks/results/{env}/{year}/{month}/{day}/run-{id}.json - Historical results
 */

import type {
  StoredBenchmarkResult,
  E2EBenchmarkSuiteResult,
} from './types'

// =============================================================================
// Types
// =============================================================================

/** Configuration for baseline store */
export interface BaselineStoreConfig {
  /** R2 bucket binding (for Workers) */
  bucket?: R2Bucket | undefined
  /** Base URL for R2 operations via HTTP (for Node.js) */
  r2BaseUrl?: string | undefined
  /** Wrangler credentials for R2 access */
  accountId?: string | undefined
  /** R2 bucket name */
  bucketName?: string | undefined
}

/** Result of a save operation */
export interface SaveResult {
  success: boolean
  path: string
  error?: string | undefined
}

/** Options for listing results */
export interface ListOptions {
  /** Environment to list results for */
  environment: string
  /** Number of days to look back */
  days?: number | undefined
  /** Maximum results to return */
  limit?: number | undefined
}

// =============================================================================
// Baseline Store Interface
// =============================================================================

export interface BaselineStore {
  /** Save a benchmark result */
  saveResult(result: StoredBenchmarkResult): Promise<SaveResult>
  /** Get the current baseline for an environment */
  getBaseline(environment: string): Promise<StoredBenchmarkResult | null>
  /** Set the baseline for an environment */
  setBaseline(result: StoredBenchmarkResult): Promise<SaveResult>
  /** List historical results */
  listResults(options: ListOptions): Promise<StoredBenchmarkResult[]>
}

// =============================================================================
// Path Helpers
// =============================================================================

function getBaselinePath(environment: string): string {
  return `benchmarks/baselines/${environment}/latest.json`
}

function getResultPath(result: StoredBenchmarkResult): string {
  const date = new Date(result.timestamp)
  const year = date.getUTCFullYear()
  const month = String(date.getUTCMonth() + 1).padStart(2, '0')
  const day = String(date.getUTCDate()).padStart(2, '0')
  return `benchmarks/results/${result.environment}/${year}/${month}/${day}/${result.runId}.json`
}

function getResultsPrefix(environment: string, date: Date): string {
  const year = date.getUTCFullYear()
  const month = String(date.getUTCMonth() + 1).padStart(2, '0')
  const day = String(date.getUTCDate()).padStart(2, '0')
  return `benchmarks/results/${environment}/${year}/${month}/${day}/`
}

// =============================================================================
// R2 Baseline Store (for Workers)
// =============================================================================

/**
 * Baseline store using Cloudflare R2 bucket binding
 */
export class R2BaselineStore implements BaselineStore {
  private bucket: R2Bucket

  constructor(bucket: R2Bucket) {
    this.bucket = bucket
  }

  async saveResult(result: StoredBenchmarkResult): Promise<SaveResult> {
    const path = getResultPath(result)
    try {
      await this.bucket.put(path, JSON.stringify(result, null, 2), {
        httpMetadata: { contentType: 'application/json' },
        customMetadata: {
          runId: result.runId,
          environment: result.environment,
          commitSha: result.commitSha ?? '',
          branch: result.branch ?? '',
        },
      })
      return { success: true, path }
    } catch (error) {
      return {
        success: false,
        path,
        error: error instanceof Error ? error.message : 'Unknown error',
      }
    }
  }

  async getBaseline(environment: string): Promise<StoredBenchmarkResult | null> {
    const path = getBaselinePath(environment)
    try {
      const object = await this.bucket.get(path)
      if (!object) return null
      const text = await object.text()
      return JSON.parse(text) as StoredBenchmarkResult
    } catch {
      return null
    }
  }

  async setBaseline(result: StoredBenchmarkResult): Promise<SaveResult> {
    const path = getBaselinePath(result.environment)
    try {
      await this.bucket.put(path, JSON.stringify(result, null, 2), {
        httpMetadata: { contentType: 'application/json' },
        customMetadata: {
          runId: result.runId,
          environment: result.environment,
          updatedAt: new Date().toISOString(),
        },
      })
      return { success: true, path }
    } catch (error) {
      return {
        success: false,
        path,
        error: error instanceof Error ? error.message : 'Unknown error',
      }
    }
  }

  async listResults(options: ListOptions): Promise<StoredBenchmarkResult[]> {
    const { environment, days = 7, limit = 100 } = options
    const results: StoredBenchmarkResult[] = []

    // List results for each day
    const now = new Date()
    for (let i = 0; i < days && results.length < limit; i++) {
      const date = new Date(now)
      date.setUTCDate(date.getUTCDate() - i)
      const prefix = getResultsPrefix(environment, date)

      const listed = await this.bucket.list({ prefix, limit: limit - results.length })
      for (const object of listed.objects) {
        try {
          const obj = await this.bucket.get(object.key)
          if (obj) {
            const text = await obj.text()
            results.push(JSON.parse(text) as StoredBenchmarkResult)
          }
        } catch {
          // Skip invalid objects
        }
      }
    }

    // Sort by timestamp descending
    return results.sort((a, b) =>
      new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime()
    )
  }
}

// =============================================================================
// File-based Baseline Store (for local testing / Node.js)
// =============================================================================

/**
 * Baseline store using local filesystem
 * Used for local development and testing
 */
export class FileBaselineStore implements BaselineStore {
  private basePath: string

  constructor(basePath: string) {
    this.basePath = basePath
  }

  async saveResult(result: StoredBenchmarkResult): Promise<SaveResult> {
    const path = `${this.basePath}/${getResultPath(result)}`
    try {
      const fs = await import('fs/promises')
      const dir = path.substring(0, path.lastIndexOf('/'))
      await fs.mkdir(dir, { recursive: true })
      await fs.writeFile(path, JSON.stringify(result, null, 2))
      return { success: true, path }
    } catch (error) {
      return {
        success: false,
        path,
        error: error instanceof Error ? error.message : 'Unknown error',
      }
    }
  }

  async getBaseline(environment: string): Promise<StoredBenchmarkResult | null> {
    const path = `${this.basePath}/${getBaselinePath(environment)}`
    try {
      const fs = await import('fs/promises')
      const text = await fs.readFile(path, 'utf-8')
      return JSON.parse(text) as StoredBenchmarkResult
    } catch {
      return null
    }
  }

  async setBaseline(result: StoredBenchmarkResult): Promise<SaveResult> {
    const path = `${this.basePath}/${getBaselinePath(result.environment)}`
    try {
      const fs = await import('fs/promises')
      const dir = path.substring(0, path.lastIndexOf('/'))
      await fs.mkdir(dir, { recursive: true })
      await fs.writeFile(path, JSON.stringify(result, null, 2))
      return { success: true, path }
    } catch (error) {
      return {
        success: false,
        path,
        error: error instanceof Error ? error.message : 'Unknown error',
      }
    }
  }

  async listResults(options: ListOptions): Promise<StoredBenchmarkResult[]> {
    const { environment, days = 7, limit = 100 } = options
    const results: StoredBenchmarkResult[] = []

    try {
      const fs = await import('fs/promises')
      const path = await import('path')

      const now = new Date()
      for (let i = 0; i < days && results.length < limit; i++) {
        const date = new Date(now)
        date.setUTCDate(date.getUTCDate() - i)
        const prefix = getResultsPrefix(environment, date)
        const dirPath = `${this.basePath}/${prefix}`

        try {
          const files = await fs.readdir(dirPath)
          for (const file of files) {
            if (!file.endsWith('.json')) continue
            try {
              const text = await fs.readFile(path.join(dirPath, file), 'utf-8')
              results.push(JSON.parse(text) as StoredBenchmarkResult)
              if (results.length >= limit) break
            } catch {
              // Skip invalid files
            }
          }
        } catch {
          // Directory doesn't exist
        }
      }
    } catch {
      // fs module not available
    }

    return results.sort((a, b) =>
      new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime()
    )
  }
}

// =============================================================================
// URL-based Baseline Store (for fetching from R2 via HTTP)
// =============================================================================

/**
 * Baseline store that fetches baselines via HTTP
 * Used when running benchmarks from CI without R2 binding
 */
export class HttpBaselineStore implements BaselineStore {
  private baseUrl: string
  private headers: Record<string, string>

  constructor(baseUrl: string, headers: Record<string, string> = {}) {
    this.baseUrl = baseUrl.replace(/\/$/, '')
    this.headers = headers
  }

  async saveResult(_result: StoredBenchmarkResult): Promise<SaveResult> {
    // HTTP store is read-only for fetching baselines
    // Saving is done via wrangler r2 commands in CI
    return {
      success: false,
      path: '',
      error: 'HttpBaselineStore is read-only. Use wrangler r2 to upload results.',
    }
  }

  async getBaseline(environment: string): Promise<StoredBenchmarkResult | null> {
    const path = getBaselinePath(environment)
    const url = `${this.baseUrl}/${path}`

    try {
      const response = await fetch(url, { headers: this.headers })
      if (!response.ok) return null
      return (await response.json()) as StoredBenchmarkResult
    } catch {
      return null
    }
  }

  async setBaseline(_result: StoredBenchmarkResult): Promise<SaveResult> {
    return {
      success: false,
      path: '',
      error: 'HttpBaselineStore is read-only. Use wrangler r2 to upload baselines.',
    }
  }

  async listResults(_options: ListOptions): Promise<StoredBenchmarkResult[]> {
    // Listing not supported via HTTP
    return []
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a baseline store from configuration
 */
export function createBaselineStore(config: BaselineStoreConfig): BaselineStore {
  if (config.bucket) {
    return new R2BaselineStore(config.bucket)
  }

  if (config.r2BaseUrl) {
    return new HttpBaselineStore(config.r2BaseUrl)
  }

  // Default to file-based store for local testing
  return new FileBaselineStore('./tests/e2e/benchmarks/.results')
}

/**
 * Create a stored benchmark result from suite result
 */
export function createStoredResult(
  suiteResult: E2EBenchmarkSuiteResult,
  metadata: {
    runId: string
    environment: string
    commitSha?: string | undefined
    branch?: string | undefined
    tag?: string | undefined
  }
): StoredBenchmarkResult {
  return {
    runId: metadata.runId,
    commitSha: metadata.commitSha,
    branch: metadata.branch,
    tag: metadata.tag,
    environment: metadata.environment,
    results: suiteResult,
    timestamp: new Date().toISOString(),
  }
}
