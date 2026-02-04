/**
 * Vitest Test Setup
 *
 * This file is loaded before all tests run.
 * It sets up global test utilities, custom matchers, and environment detection.
 */

import { beforeAll, afterEach, describe, expect } from 'vitest'
import { parquedbMatchers } from './matchers'
import { config } from 'dotenv'

// Load environment variables from .env file
config()

// Register custom matchers
expect.extend(parquedbMatchers)

// Global test utilities
declare global {
  // eslint-disable-next-line @typescript-eslint/no-namespace
  namespace Vi {
    interface Assertion {
      // Existing matchers
      toBeValidEntity(): void
      toMatchFilter(filter: import('../src/types').Filter): void
      toHaveAuditFields(): void
      toBeEntityId(): void
      // New matchers
      toBeValidParquetFile(): void
      toHaveRelationship(predicate: string, target?: string | RegExp): void
      toMatchEvent(expectedOp: import('../src/types/entity').EventOp, expectedData?: Partial<import('../src/types/entity').Event>): void
      toBeValidIndex(indexType: import('../src/indexes/types').IndexType): void
      toHaveRowGroups(expectedCount: number): void
      toBeCompressedWith(codec: 'UNCOMPRESSED' | 'SNAPPY' | 'GZIP' | 'LZO' | 'BROTLI' | 'LZ4' | 'ZSTD'): void
    }
  }
}

// =============================================================================
// Environment Detection
// =============================================================================

/**
 * Detect if running in Node.js environment
 */
export const isNode = typeof process !== 'undefined'
  && process.versions?.node !== undefined
  && typeof window === 'undefined'

/**
 * Detect if running in browser environment
 */
export const isBrowser = typeof window !== 'undefined'
  && typeof document !== 'undefined'

/**
 * Detect if running in Cloudflare Workers environment
 */
export const isWorkers = typeof caches !== 'undefined'
  && !isBrowser
  && typeof ServiceWorkerGlobalScope === 'undefined'
  && typeof HTMLElement === 'undefined'

/**
 * Get the current environment name
 */
export function getEnvironment(): 'node' | 'browser' | 'workers' | 'unknown' {
  if (isNode) return 'node'
  if (isBrowser) return 'browser'
  if (isWorkers) return 'workers'
  return 'unknown'
}

// =============================================================================
// Test Helpers for Environment-Specific Tests
// =============================================================================

/**
 * Check if a test should be skipped in a specific environment
 *
 * @example
 * ```ts
 * it.skipIf(shouldSkipInEnvironment('workers'))('uses fs module', () => {
 *   // This test won't run in Workers
 * })
 * ```
 */
export function shouldSkipInEnvironment(env: 'node' | 'browser' | 'workers'): boolean {
  const current = getEnvironment()
  return current === env
}

/**
 * Check if a test should run only in specific environments
 *
 * @example
 * ```ts
 * it.skipIf(!shouldRunInEnvironment('browser'))('uses DOM API', () => {
 *   // This test only runs in browser
 * })
 * ```
 */
export function shouldRunInEnvironment(...envs: Array<'node' | 'browser' | 'workers'>): boolean {
  const current = getEnvironment()
  return envs.includes(current)
}

/**
 * Create a describe block that only runs in specific environments
 */
export function describeForEnvironment(
  envs: Array<'node' | 'browser' | 'workers'>,
  name: string,
  fn: () => void
): void {
  const current = getEnvironment()
  if (envs.includes(current)) {
    describe(name, fn)
  } else {
    describe.skip(name, fn)
  }
}

// =============================================================================
// Type Guards for Environment Assertions
// =============================================================================

/**
 * Assert that the current environment is Node.js
 */
export function assertNode(): asserts globalThis is typeof globalThis & {
  process: NodeJS.Process
} {
  if (!isNode) {
    throw new Error('This code requires Node.js environment')
  }
}

/**
 * Assert that the current environment is a browser
 */
export function assertBrowser(): asserts globalThis is typeof globalThis & Window {
  if (!isBrowser) {
    throw new Error('This code requires browser environment')
  }
}

/**
 * Assert that the current environment is Cloudflare Workers
 */
export function assertWorkers(): asserts globalThis is typeof globalThis & {
  caches: CacheStorage
} {
  if (!isWorkers) {
    throw new Error('This code requires Cloudflare Workers environment')
  }
}

// =============================================================================
// General Test Utilities
// =============================================================================

/**
 * Create a delay promise (works in all environments)
 * @deprecated Prefer using vi.useFakeTimers() and vi.advanceTimersByTime() for deterministic timing in tests
 */
export function delay(_ms: number): Promise<void> {
  // In test environments, prefer fake timers over real delays
  // This function is kept for backwards compatibility but does nothing
  return Promise.resolve()
}

/**
 * Generate random bytes using crypto API (works in all environments)
 */
export function randomBytes(length: number): Uint8Array {
  const bytes = new Uint8Array(length)
  if (typeof crypto !== 'undefined' && crypto.getRandomValues) {
    crypto.getRandomValues(bytes)
  } else {
    // Fallback for older Node.js versions
    for (let i = 0; i < length; i++) {
      bytes[i] = Math.floor(Math.random() * 256)
    }
  }
  return bytes
}

/**
 * Generate a unique test ID
 */
export function uniqueTestId(): string {
  const bytes = randomBytes(8)
  return Array.from(bytes)
    .map(b => b.toString(16).padStart(2, '0'))
    .join('')
}

/**
 * Cleanup helper for async resources
 */
export async function cleanupAfterTest(cleanup: () => Promise<void>): Promise<void> {
  try {
    await cleanup()
  } catch {
    // Ignore cleanup errors in tests
  }
}

/**
 * Robust temp directory cleanup with retries
 * Handles ENOTEMPTY errors that occur when files are still being written
 */
export async function cleanupTempDir(tempDir: string, maxRetries = 3): Promise<void> {
  const { rm } = await import('node:fs/promises')

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      await rm(tempDir, { recursive: true, force: true, maxRetries: 3 })
      return
    } catch (error: unknown) {
      const isEnotempty = error instanceof Error &&
        (error.message.includes('ENOTEMPTY') || (error as NodeJS.ErrnoException).code === 'ENOTEMPTY')
      const isEnoent = error instanceof Error &&
        (error.message.includes('ENOENT') || (error as NodeJS.ErrnoException).code === 'ENOENT')

      if (isEnoent) {
        // Directory already removed
        return
      }

      if (isEnotempty && attempt < maxRetries - 1) {
        // Wait a bit and retry
        await new Promise(resolve => setTimeout(resolve, 50 * (attempt + 1)))
        continue
      }

      // On final attempt, just ignore the error - temp dirs will be cleaned up eventually
      if (attempt === maxRetries - 1) {
        return
      }
      throw error
    }
  }
}

/**
 * Create a test context with automatic cleanup
 */
export function useTestContext<T>(
  setup: () => Promise<T>,
  teardown: (ctx: T) => Promise<void>
): { getContext: () => T } {
  let context: T | undefined

  beforeAll(async () => {
    context = await setup()
  })

  afterEach(async () => {
    if (context) {
      await teardown(context)
    }
  })

  return {
    getContext: () => {
      if (!context) {
        throw new Error('Test context not initialized')
      }
      return context
    }
  }
}

// =============================================================================
// Global Test Setup
// =============================================================================

beforeAll(() => {
  const env = getEnvironment()
  console.log(`\n[ParqueDB Tests] Running in ${env} environment\n`)
})
