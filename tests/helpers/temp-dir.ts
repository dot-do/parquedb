/**
 * Test Temp Directory Utilities
 *
 * Provides isolated temp directories for tests with proper async cleanup.
 * This module solves the race condition where temp directories are deleted
 * before async operations (like flushEvents) complete.
 *
 * Key features:
 * - Each test gets a unique temp directory
 * - Proper async disposal of ParqueDB instances before cleanup
 * - Retry logic for cleanup to handle lingering file handles
 * - Isolated state between tests
 *
 * Usage:
 * ```typescript
 * import { createTestContext, type TestContext } from '../helpers/temp-dir'
 *
 * describe('MyTest', () => {
 *   let ctx: TestContext
 *
 *   beforeEach(async () => {
 *     ctx = await createTestContext()
 *   })
 *
 *   afterEach(async () => {
 *     await ctx.cleanup()
 *   })
 *
 *   it('should work', async () => {
 *     const db = new ParqueDB({ storage: ctx.storage })
 *     // ... test code ...
 *     await ctx.disposeDb(db)  // Properly dispose before cleanup
 *   })
 * })
 * ```
 */

import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { FsBackend } from '../../src/storage/FsBackend'
import type { StorageBackend } from '../../src/types/storage'

// =============================================================================
// Types
// =============================================================================

/**
 * Interface for objects with disposeAsync method (like ParqueDB)
 */
export interface AsyncDisposable {
  disposeAsync(): Promise<void>
}

/**
 * Interface for objects with dispose method
 */
export interface SyncDisposable {
  dispose(): void
}

/**
 * Options for creating a test context
 */
export interface TestContextOptions {
  /** Prefix for the temp directory name */
  prefix?: string
  /** Maximum cleanup retries (default: 5) */
  maxCleanupRetries?: number
  /** Delay between cleanup retries in ms (default: 100) */
  cleanupRetryDelay?: number
}

/**
 * Test context providing isolated temp directory and storage
 */
export interface TestContext {
  /** The unique temp directory path */
  tempDir: string
  /** FsBackend configured with the temp directory */
  storage: FsBackend
  /** Register an async-disposable resource for cleanup */
  disposeDb(db: AsyncDisposable): Promise<void>
  /** Register a sync-disposable resource */
  disposeSync(resource: SyncDisposable): void
  /** Cleanup all resources and temp directory */
  cleanup(): Promise<void>
  /** Check if cleanup has been called */
  isCleanedUp: boolean
}

// =============================================================================
// Global Tracking
// =============================================================================

/**
 * Track all active test contexts for emergency cleanup
 */
const activeContexts: Set<TestContext> = new Set()

/**
 * Get count of active test contexts (for debugging)
 */
export function getActiveContextCount(): number {
  return activeContexts.size
}

// =============================================================================
// Implementation
// =============================================================================

/**
 * Generate a unique temp directory prefix
 */
function generatePrefix(basePrefix: string): string {
  const timestamp = Date.now()
  const random = Math.random().toString(36).slice(2, 10)
  return `${basePrefix}${timestamp}-${random}`
}

/**
 * Robust cleanup of a temp directory with retries
 */
async function cleanupTempDirWithRetries(
  tempDir: string,
  maxRetries: number,
  retryDelay: number
): Promise<void> {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      await rm(tempDir, { recursive: true, force: true, maxRetries: 3 })
      return
    } catch (error: unknown) {
      const isEnoent = error instanceof Error &&
        ((error as NodeJS.ErrnoException).code === 'ENOENT' ||
         error.message.includes('ENOENT'))

      // Directory already gone, we're done
      if (isEnoent) {
        return
      }

      const isEnotempty = error instanceof Error &&
        ((error as NodeJS.ErrnoException).code === 'ENOTEMPTY' ||
         error.message.includes('ENOTEMPTY'))

      const isEbusy = error instanceof Error &&
        ((error as NodeJS.ErrnoException).code === 'EBUSY' ||
         error.message.includes('EBUSY'))

      // Retriable errors - wait and try again
      if ((isEnotempty || isEbusy) && attempt < maxRetries - 1) {
        await new Promise(resolve => setTimeout(resolve, retryDelay * (attempt + 1)))
        continue
      }

      // Final attempt - just ignore the error
      // The OS will clean up temp directories eventually
      if (attempt === maxRetries - 1) {
        return
      }

      throw error
    }
  }
}

/**
 * Create an isolated test context with a unique temp directory
 *
 * @param options - Configuration options
 * @returns Test context with temp directory and storage
 *
 * @example
 * ```typescript
 * const ctx = await createTestContext()
 * const db = new ParqueDB({ storage: ctx.storage })
 *
 * // ... do test work ...
 *
 * await ctx.disposeDb(db)  // Wait for pending flushes
 * await ctx.cleanup()       // Clean up temp directory
 * ```
 */
export async function createTestContext(options: TestContextOptions = {}): Promise<TestContext> {
  const {
    prefix = 'parquedb-test-',
    maxCleanupRetries = 5,
    cleanupRetryDelay = 100,
  } = options

  // Create unique temp directory
  const uniquePrefix = generatePrefix(prefix)
  const tempDir = await mkdtemp(join(tmpdir(), uniquePrefix))

  // Create storage backend
  const storage = new FsBackend(tempDir)

  // Track resources to dispose
  const asyncDisposables: AsyncDisposable[] = []
  const syncDisposables: SyncDisposable[] = []
  let isCleanedUp = false

  const context: TestContext = {
    tempDir,
    storage,

    async disposeDb(db: AsyncDisposable): Promise<void> {
      if (isCleanedUp) {
        console.warn('TestContext: disposeDb called after cleanup')
        return
      }
      try {
        await db.disposeAsync()
      } catch {
        // Ignore disposal errors
      }
      // Remove from tracking
      const idx = asyncDisposables.indexOf(db)
      if (idx >= 0) {
        asyncDisposables.splice(idx, 1)
      }
    },

    disposeSync(resource: SyncDisposable): void {
      if (isCleanedUp) {
        console.warn('TestContext: disposeSync called after cleanup')
        return
      }
      try {
        resource.dispose()
      } catch {
        // Ignore disposal errors
      }
      // Remove from tracking
      const idx = syncDisposables.indexOf(resource)
      if (idx >= 0) {
        syncDisposables.splice(idx, 1)
      }
    },

    async cleanup(): Promise<void> {
      if (isCleanedUp) {
        return
      }
      isCleanedUp = true

      // First dispose all async resources (like ParqueDB)
      // These need to wait for pending flushes
      for (const disposable of asyncDisposables) {
        try {
          await disposable.disposeAsync()
        } catch {
          // Ignore disposal errors during cleanup
        }
      }
      asyncDisposables.length = 0

      // Then dispose sync resources
      for (const disposable of syncDisposables) {
        try {
          disposable.dispose()
        } catch {
          // Ignore disposal errors during cleanup
        }
      }
      syncDisposables.length = 0

      // Finally clean up the temp directory
      await cleanupTempDirWithRetries(tempDir, maxCleanupRetries, cleanupRetryDelay)

      // Remove from active tracking
      activeContexts.delete(context)
    },

    get isCleanedUp(): boolean {
      return isCleanedUp
    },
  }

  // Track for emergency cleanup
  activeContexts.add(context)

  return context
}

/**
 * Create multiple isolated test contexts
 *
 * @param count - Number of contexts to create
 * @param options - Configuration options
 * @returns Array of test contexts
 */
export async function createMultipleTestContexts(
  count: number,
  options?: TestContextOptions
): Promise<TestContext[]> {
  const promises = Array.from({ length: count }, () => createTestContext(options))
  return Promise.all(promises)
}

/**
 * Cleanup all active test contexts
 *
 * Call this in a global afterAll hook to ensure all temp directories
 * are cleaned up even if individual tests fail to call cleanup.
 *
 * @example
 * ```typescript
 * // In vitest.setup.ts or a global afterAll
 * afterAll(async () => {
 *   await cleanupAllTestContexts()
 * })
 * ```
 */
export async function cleanupAllTestContexts(): Promise<void> {
  const contexts = [...activeContexts]
  await Promise.all(contexts.map(ctx => ctx.cleanup()))
}

// =============================================================================
// Convenience Functions
// =============================================================================

/**
 * Create a simple isolated temp directory without the full context
 *
 * Use this when you don't need ParqueDB instance tracking.
 * Remember to call cleanupTempDir manually in afterEach.
 *
 * @param prefix - Prefix for the temp directory name
 * @returns Path to the created temp directory
 */
export async function createIsolatedTempDir(prefix = 'parquedb-test-'): Promise<string> {
  const uniquePrefix = generatePrefix(prefix)
  return mkdtemp(join(tmpdir(), uniquePrefix))
}

/**
 * Cleanup a temp directory with retry logic
 *
 * @param tempDir - Path to the temp directory
 */
export async function cleanupTempDir(
  tempDir: string,
  options: { maxRetries?: number; retryDelay?: number } = {}
): Promise<void> {
  const { maxRetries = 5, retryDelay = 100 } = options
  await cleanupTempDirWithRetries(tempDir, maxRetries, retryDelay)
}

// =============================================================================
// Storage Factory
// =============================================================================

/**
 * Create an FsBackend with an isolated temp directory
 *
 * This is a simpler API when you just need a storage backend.
 * The returned object includes the cleanup function.
 *
 * @param prefix - Prefix for the temp directory name
 * @returns Object with storage backend and cleanup function
 *
 * @example
 * ```typescript
 * const { storage, tempDir, cleanup } = await createIsolatedStorage()
 * const db = new ParqueDB({ storage })
 * // ... test ...
 * await db.disposeAsync()
 * await cleanup()
 * ```
 */
export async function createIsolatedStorage(
  prefix = 'parquedb-test-'
): Promise<{
  storage: FsBackend
  tempDir: string
  cleanup: () => Promise<void>
}> {
  const tempDir = await createIsolatedTempDir(prefix)
  const storage = new FsBackend(tempDir)

  return {
    storage,
    tempDir,
    cleanup: () => cleanupTempDir(tempDir),
  }
}

// =============================================================================
// Re-export from setup for backwards compatibility
// =============================================================================

export { cleanupTempDir as robustCleanupTempDir } from '../setup'
