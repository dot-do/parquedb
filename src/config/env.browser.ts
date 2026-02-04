/**
 * Environment Detection (Browser Build)
 *
 * Browser-safe environment module that excludes cloudflare:workers import.
 * Workers env must be passed explicitly via setWorkersEnv() in browser builds.
 */

import type { Env } from '../types/worker'
import { detectRuntime } from './runtime'

// Module-scoped cache for Workers env
let _workersEnv: Env | null = null
let _workersEnvLoaded = false

/**
 * Load Cloudflare Workers environment bindings (browser stub)
 *
 * In browser builds, cloudflare:workers is not available.
 * Use setWorkersEnv() to manually provide the env in Workers context.
 */
export async function loadWorkersEnv(): Promise<Env | null> {
  if (_workersEnvLoaded) return _workersEnv

  // In browser context, we can't dynamically import cloudflare:workers
  // Return cached env if set, otherwise null
  _workersEnvLoaded = true
  return _workersEnv
}

/**
 * Get cached Workers env (must call setWorkersEnv first)
 */
export function getWorkersEnv(): Env | null {
  return _workersEnv
}

/**
 * Set Workers env manually (required in browser/bundled builds)
 */
export function setWorkersEnv(env: Env): void {
  _workersEnv = env
  _workersEnvLoaded = true
}

/**
 * Available bindings detected from environment
 */
export interface AvailableBindings {
  r2?: R2Bucket
  kv?: KVNamespace
  d1?: D1Database
  do?: DurableObjectNamespace
  ai?: Ai
  vectorize?: VectorizeIndex
}

/**
 * Detect available Cloudflare bindings from env
 */
export function detectBindings(env?: Env): AvailableBindings {
  const source = env ?? _workersEnv
  if (!source) return {}

  const bindings: AvailableBindings = {}

  // Check for common binding patterns
  for (const [, value] of Object.entries(source)) {
    if (!value || typeof value !== 'object') continue

    // R2 bucket detection
    if ('put' in value && 'get' in value && 'delete' in value && 'list' in value) {
      bindings.r2 = value as R2Bucket
    }
    // KV namespace detection
    else if ('get' in value && 'put' in value && 'delete' in value && !('list' in value)) {
      bindings.kv = value as KVNamespace
    }
    // D1 database detection
    else if ('prepare' in value && 'exec' in value && 'batch' in value) {
      bindings.d1 = value as D1Database
    }
    // Durable Object detection
    else if ('get' in value && 'idFromName' in value) {
      bindings.do = value as DurableObjectNamespace
    }
    // AI detection
    else if ('run' in value) {
      bindings.ai = value as Ai
    }
    // Vectorize detection
    else if ('insert' in value && 'query' in value) {
      bindings.vectorize = value as VectorizeIndex
    }
  }

  return bindings
}

/**
 * Storage paths configuration
 */
export interface StoragePaths {
  dataDir: string
  configPath?: string
  cacheDir?: string
}

/**
 * Detect storage paths from environment/cwd
 */
export function detectStoragePaths(): StoragePaths {
  const runtime = detectRuntime()

  // Default paths for different runtimes
  if (runtime === 'node') {
    const dataDir = process.env.PARQUEDB_DATA_DIR || process.env.DATA_DIR || './data'
    const configPath = process.env.PARQUEDB_CONFIG_PATH || undefined
    const cacheDir = process.env.PARQUEDB_CACHE_DIR || undefined
    const paths: StoragePaths = { dataDir }
    if (configPath) paths.configPath = configPath
    if (cacheDir) paths.cacheDir = cacheDir
    return paths
  }

  // For browser/workers, use defaults
  return {
    dataDir: './data',
  }
}

/**
 * Ensure data directory exists (no-op in browser)
 */
export async function ensureDataDir(_path: string): Promise<void> {
  // No-op in browser - filesystem not available
  return
}
