/**
 * Runtime Detection
 *
 * Reliably detects the current JavaScript runtime environment.
 */

export type Runtime =
  | 'cloudflare-workers'
  | 'node'
  | 'bun'
  | 'deno'
  | 'browser'
  | 'unknown'

export interface RuntimeInfo {
  runtime: Runtime
  version?: string | undefined
  features: {
    hasFileSystem: boolean
    hasIndexedDB: boolean
    hasR2: boolean
    hasDurableObjects: boolean
  }
}

/**
 * Detect the current runtime environment
 *
 * Detection order matters - most specific first:
 * 1. Cloudflare Workers (check navigator.userAgent)
 * 2. Bun (has Bun global)
 * 3. Deno (has Deno global)
 * 4. Node.js (has process.versions.node, after Bun check)
 * 5. Browser (has window)
 */
export function detectRuntime(): Runtime {
  // Cloudflare Workers - most reliable check
  if (
    typeof navigator !== 'undefined' &&
    navigator.userAgent === 'Cloudflare-Workers'
  ) {
    return 'cloudflare-workers'
  }

  // Bun - check before Node since Bun has process.versions too
  if (typeof globalThis !== 'undefined' && 'Bun' in globalThis) {
    return 'bun'
  }

  // Deno
  if (typeof globalThis !== 'undefined' && 'Deno' in globalThis) {
    return 'deno'
  }

  // Node.js - check after Bun/Deno
  if (
    typeof process !== 'undefined' &&
    process.versions &&
    'node' in process.versions
  ) {
    return 'node'
  }

  // Browser
  if (typeof window !== 'undefined' && typeof document !== 'undefined') {
    return 'browser'
  }

  return 'unknown'
}

/**
 * Get detailed runtime information including available features
 */
export function getRuntimeInfo(): RuntimeInfo {
  const runtime = detectRuntime()

  const info: RuntimeInfo = {
    runtime,
    features: {
      hasFileSystem: false,
      hasIndexedDB: false,
      hasR2: false,
      hasDurableObjects: false,
    },
  }

  switch (runtime) {
    case 'node':
      info.version = process.versions.node
      info.features.hasFileSystem = true
      break

    case 'bun':
      info.version = globalThis.Bun?.version
      info.features.hasFileSystem = true
      break

    case 'deno':
      info.version = globalThis.Deno?.version?.deno
      info.features.hasFileSystem = true
      break

    case 'browser':
      info.features.hasIndexedDB = typeof indexedDB !== 'undefined'
      break

    case 'cloudflare-workers':
      // R2/DO detection happens separately via env
      break
  }

  return info
}

/**
 * Check if we're in a server-side environment (can access filesystem)
 */
export function isServer(): boolean {
  const runtime = detectRuntime()
  return runtime === 'node' || runtime === 'bun' || runtime === 'deno'
}

/**
 * Check if we're in Cloudflare Workers
 */
export function isWorkers(): boolean {
  return detectRuntime() === 'cloudflare-workers'
}

/**
 * Check if we're in a browser
 */
export function isBrowser(): boolean {
  return detectRuntime() === 'browser'
}

/**
 * Get an environment variable in a platform-agnostic way.
 *
 * Supports:
 * - Node.js (process.env)
 * - Bun (process.env, Bun.env)
 * - Deno (Deno.env.get)
 * - Cloudflare Workers (returns undefined - no env vars at runtime)
 * - Browser (returns undefined)
 *
 * @param name - The environment variable name
 * @returns The value or undefined if not found/not available
 */
export function getEnv(name: string): string | undefined {
  // Deno
  if (typeof globalThis !== 'undefined' && 'Deno' in globalThis) {
    try {
      const denoGlobal = globalThis as unknown as { Deno: { env: { get: (key: string) => string | undefined } } }
      return denoGlobal.Deno.env.get(name)
    } catch {
      // Deno.env.get may throw if permission denied
      return undefined
    }
  }

  // Bun has Bun.env and also process.env
  if (typeof globalThis !== 'undefined' && 'Bun' in globalThis) {
    const bunGlobal = globalThis as unknown as { Bun: { env?: Record<string, string | undefined> } }
    const bunEnv = bunGlobal.Bun.env
    if (bunEnv) {
      return bunEnv[name]
    }
  }

  // Node.js (and Bun fallback)
  if (typeof process !== 'undefined' && process.env) {
    return process.env[name]
  }

  // Cloudflare Workers and Browser don't have env vars at runtime
  return undefined
}

/**
 * Check if running in production mode.
 *
 * Checks multiple environment variables in order:
 * 1. NODE_ENV === 'production'
 * 2. ENVIRONMENT === 'production'
 * 3. CF_ENVIRONMENT === 'production'
 *
 * For Cloudflare Workers where env vars aren't available at runtime,
 * defaults to true (safer to omit debug info in production).
 *
 * @returns true if in production, false otherwise
 */
export function isProduction(): boolean {
  const runtime = detectRuntime()

  // In Workers, we can't access env vars at runtime without bindings
  // Default to production-like behavior (safer)
  if (runtime === 'cloudflare-workers') {
    return true
  }

  const nodeEnv = getEnv('NODE_ENV')
  if (nodeEnv) {
    return nodeEnv === 'production'
  }

  const env = getEnv('ENVIRONMENT')
  if (env) {
    return env === 'production' || env === 'prod'
  }

  const cfEnv = getEnv('CF_ENVIRONMENT')
  if (cfEnv) {
    return cfEnv === 'production' || cfEnv === 'prod'
  }

  // Default to non-production if no env vars set
  return false
}

/**
 * Check if running in development mode.
 *
 * @returns true if in development, false otherwise
 */
export function isDevelopment(): boolean {
  const runtime = detectRuntime()

  // In Workers, default to false (production-like)
  if (runtime === 'cloudflare-workers') {
    return false
  }

  const nodeEnv = getEnv('NODE_ENV')
  if (nodeEnv) {
    return nodeEnv === 'development' || nodeEnv === 'dev'
  }

  const env = getEnv('ENVIRONMENT')
  if (env) {
    return env === 'development' || env === 'dev'
  }

  const cfEnv = getEnv('CF_ENVIRONMENT')
  if (cfEnv) {
    return cfEnv === 'development' || cfEnv === 'dev'
  }

  // Default to development if no env vars set (better for local dev)
  return true
}

/**
 * Check if running in test mode.
 *
 * @returns true if in test mode, false otherwise
 */
export function isTest(): boolean {
  const nodeEnv = getEnv('NODE_ENV')
  if (nodeEnv) {
    return nodeEnv === 'test' || nodeEnv === 'testing'
  }

  const env = getEnv('ENVIRONMENT')
  if (env) {
    return env === 'test' || env === 'testing'
  }

  return false
}
