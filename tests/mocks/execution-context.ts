/**
 * ExecutionContext Mock Factory
 *
 * Provides mock implementations of Cloudflare Workers ExecutionContext for testing.
 */

import { vi, type Mock } from 'vitest'

// =============================================================================
// Types
// =============================================================================

/**
 * Cloudflare Workers ExecutionContext interface
 */
export interface ExecutionContext {
  waitUntil(promise: Promise<unknown>): void
  passThroughOnException(): void
}

/**
 * Mock ExecutionContext with vi.fn() methods for assertions
 */
export interface MockExecutionContext extends ExecutionContext {
  waitUntil: Mock<[Promise<unknown>], void>
  passThroughOnException: Mock<[], void>

  // Test helpers
  _pendingPromises: Promise<unknown>[]
  _waitForPending: () => Promise<void>
  _passThroughEnabled: boolean
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a mock ExecutionContext
 *
 * @returns Mock ExecutionContext instance
 *
 * @example
 * ```typescript
 * const ctx = createMockExecutionContext()
 *
 * // In handler
 * ctx.waitUntil(asyncOperation())
 *
 * // In test - wait for all waitUntil promises
 * await ctx._waitForPending()
 *
 * // Assert waitUntil was called
 * expect(ctx.waitUntil).toHaveBeenCalled()
 * ```
 */
export function createMockExecutionContext(): MockExecutionContext {
  const pendingPromises: Promise<unknown>[] = []
  let passThroughEnabled = false

  return {
    _pendingPromises: pendingPromises,
    _passThroughEnabled: passThroughEnabled,

    _waitForPending: async () => {
      await Promise.all(pendingPromises)
    },

    waitUntil: vi.fn((promise: Promise<unknown>): void => {
      pendingPromises.push(promise)
    }),

    passThroughOnException: vi.fn((): void => {
      passThroughEnabled = true
    }),
  }
}

/**
 * Create a minimal ExecutionContext that ignores all calls
 *
 * @returns Minimal ExecutionContext stub
 */
export function createNoopExecutionContext(): ExecutionContext {
  return {
    waitUntil: () => {},
    passThroughOnException: () => {},
  }
}

/**
 * Create an ExecutionContext that tracks promises for testing
 *
 * @returns ExecutionContext with promise tracking
 *
 * @example
 * ```typescript
 * const ctx = createTrackingExecutionContext()
 *
 * handler(request, env, ctx)
 *
 * // Wait for all background work to complete
 * await ctx.flush()
 *
 * // Check how many background tasks were scheduled
 * expect(ctx.pendingCount).toBe(3)
 * ```
 */
export function createTrackingExecutionContext(): ExecutionContext & {
  pendingCount: number
  flush: () => Promise<void>
  promises: Promise<unknown>[]
} {
  const promises: Promise<unknown>[] = []

  return {
    promises,

    get pendingCount() {
      return promises.length
    },

    flush: async () => {
      await Promise.all(promises)
    },

    waitUntil: (promise: Promise<unknown>) => {
      promises.push(promise)
    },

    passThroughOnException: () => {},
  }
}
