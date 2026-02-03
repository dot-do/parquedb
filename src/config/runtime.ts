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
