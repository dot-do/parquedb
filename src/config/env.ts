/**
 * Environment Detection
 *
 * Detects available bindings and environment variables across runtimes.
 * For Workers, uses `import { env } from 'cloudflare:workers'`.
 * For Node.js, detects filesystem paths and process.env.
 */

import type { Env } from '../types/worker'
import { detectRuntime } from './runtime'

// Module-scoped cache for Workers env
let _workersEnv: Env | null = null
let _workersEnvLoaded = false

/**
 * Load Cloudflare Workers environment bindings
 *
 * Uses dynamic import of 'cloudflare:workers' module.
 * Returns null if not in Workers environment.
 */
export async function loadWorkersEnv(): Promise<Env | null> {
  if (_workersEnvLoaded) return _workersEnv

  const runtime = detectRuntime()
  if (runtime !== 'cloudflare-workers') {
    _workersEnvLoaded = true
    return null
  }

  try {
    // Dynamic import - only available in Workers
    const { env } = await import('cloudflare:workers')
    _workersEnv = env as Env
    _workersEnvLoaded = true
    return _workersEnv
  } catch {
    _workersEnvLoaded = true
    return null
  }
}

/**
 * Get cached Workers env (must call loadWorkersEnv first)
 */
export function getWorkersEnv(): Env | null {
  return _workersEnv
}

/**
 * Set Workers env manually (useful for testing or explicit passing)
 */
export function setWorkersEnv(env: Env): void {
  _workersEnv = env
  _workersEnvLoaded = true
}

/**
 * Available bindings detected from environment
 */
export interface AvailableBindings {
  hasR2: boolean
  hasCDNBucket: boolean
  hasDurableObjects: boolean
  hasAI: boolean
  r2BucketName?: string | undefined
}

/**
 * Detect available bindings from Workers env
 */
export function detectBindings(env?: Env | null): AvailableBindings {
  const e = env ?? _workersEnv

  if (!e) {
    return {
      hasR2: false,
      hasCDNBucket: false,
      hasDurableObjects: false,
      hasAI: false,
    }
  }

  return {
    hasR2: !!e.BUCKET,
    hasCDNBucket: !!e.CDN_BUCKET,
    hasDurableObjects: !!e.PARQUEDB,
    hasAI: !!e.AI,
  }
}

/**
 * Storage path detection for Node.js/Bun/Deno
 */
export interface StoragePaths {
  projectRoot: string
  dataDir: string | null
  configFile: string | null
}

/**
 * Detect storage paths for server environments
 *
 * Looks for data directories in order:
 * 1. .parquedb/
 * 2. .db/
 * 3. data/
 */
export async function detectStoragePaths(): Promise<StoragePaths | null> {
  const runtime = detectRuntime()

  if (runtime !== 'node' && runtime !== 'bun' && runtime !== 'deno') {
    return null
  }

  try {
    let cwd: string

    if (runtime === 'deno') {
      cwd = globalThis.Deno!.cwd()
      // Deno fs detection uses Deno.stat and Deno.readDir which are not yet implemented here
      // For now, return minimal info - Deno users should explicitly configure paths
      return {
        projectRoot: cwd,
        dataDir: null,
        configFile: null,
      }
    }

    // Node.js or Bun - import Node types
    cwd = process.cwd()
    const fs = await import('node:fs/promises')
    const path = await import('node:path')

    const candidates = ['.parquedb', '.db', 'data']
    let dataDir: string | null = null

    for (const dir of candidates) {
      const fullPath = path.join(cwd, dir)
      try {
        const stat = await fs.stat(fullPath)
        if (stat.isDirectory()) {
          dataDir = fullPath
          break
        }
      } catch {
        // Directory doesn't exist, continue
      }
    }

    // Look for config file
    const configCandidates = [
      'parquedb.config.ts',
      'parquedb.config.js',
      'parquedb.config.mjs',
    ]
    let configFile: string | null = null

    for (const file of configCandidates) {
      const fullPath = path.join(cwd, file)
      try {
        await fs.access(fullPath)
        configFile = fullPath
        break
      } catch {
        // File doesn't exist, continue
      }
    }

    return {
      projectRoot: cwd,
      dataDir,
      configFile,
    }
  } catch {
    return null
  }
}

/**
 * Create default data directory if none exists
 */
export async function ensureDataDir(
  preferredName: string = '.parquedb'
): Promise<string | null> {
  const runtime = detectRuntime()

  if (runtime !== 'node' && runtime !== 'bun') {
    return null
  }

  try {
    const fs = await import('node:fs/promises')
    const path = await import('node:path')

    const dataDir = path.join(process.cwd(), preferredName)
    await fs.mkdir(dataDir, { recursive: true })
    return dataDir
  } catch {
    return null
  }
}
