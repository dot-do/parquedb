/**
 * RpcPromise - Chainable Promise for RPC operations
 *
 * Implements the capnweb pattern for chaining method calls
 * without awaiting intermediate results. The entire chain
 * is sent to the server in a single request.
 *
 * @see https://github.com/nicholascelestin/capnweb
 */

import type { RpcPromiseMarker } from '../types/integrations'
import type { RpcService } from './collection'

// =============================================================================
// Types
// =============================================================================

/**
 * A single step in the RPC chain
 */
export interface RpcChainStep {
  /** Method name */
  method: string
  /** Method arguments */
  args: unknown[]
}

/**
 * Full RPC chain to be executed
 */
export type RpcPromiseChain = RpcChainStep[]

/**
 * Internal state for RpcPromise
 */
interface RpcPromiseState<T> {
  /** The client to execute the chain */
  client: RpcService
  /** The chain of operations */
  chain: RpcPromiseChain
  /** Resolve function (set when promise is awaited) */
  resolve?: (value: T | PromiseLike<T>) => void
  /** Reject function (set when promise is awaited) */
  reject?: (reason: unknown) => void
  /** Whether execution has started */
  started: boolean
  /** Cached result (if already executed) */
  result?: T
  /** Cached error (if execution failed) */
  error?: unknown
}

// =============================================================================
// RpcPromise Factory
// =============================================================================

/**
 * Create an RpcPromise that chains method calls
 *
 * The promise defers execution until awaited, allowing
 * multiple operations to be chained. When the promise
 * is finally awaited, the entire chain is sent to the
 * server in a single RPC call.
 *
 * @example
 * ```typescript
 * // This sends ONE request with the full chain
 * const names = await db.Posts
 *   .find({ status: 'published' })
 *   .map(p => p.author)
 *   .map(a => a.name)
 *
 * // Without chaining, this would require 3 separate requests
 * ```
 *
 * @param client - RpcService instance
 * @param method - Initial method name
 * @param args - Method arguments
 * @returns RpcPromise with chaining support
 */
export function createRpcPromise<T>(
  client: RpcService,
  method: string,
  args: unknown[]
): RpcPromiseMarker<T> {
  // Initialize chain with the first operation
  const chain: RpcPromiseChain = [{ method, args }]

  // State for deferred execution
  const state: RpcPromiseState<T> = {
    client,
    chain,
    started: false,
  }

  // Create the base promise
  const promise = new Promise<T>((resolve, reject) => {
    state.resolve = resolve
    state.reject = reject

    // Defer execution to collect chain
    // This runs after the current microtask queue is empty,
    // allowing .map() calls to be added to the chain
    queueMicrotask(() => {
      if (!state.started) {
        state.started = true
        executeChain(state)
      }
    })
  })

  // Add RpcPromise marker and methods
  const rpcPromise = Object.assign(promise, {
    __rpcPromise: true as const,

    /**
     * Map over results on the server side
     *
     * The mapper function is serialized (via toString) and
     * sent to the server for execution. This avoids transferring
     * large datasets when only a subset of fields is needed.
     *
     * @example
     * ```typescript
     * // Extract titles (runs on server)
     * const titles = await posts.find().map(p => p.title)
     *
     * // Nested mapping
     * const authorNames = await posts
     *   .find()
     *   .map(p => p.author)
     *   .map(a => a.name)
     * ```
     *
     * @param fn - Mapping function (serialized to server)
     * @returns New RpcPromise for chaining
     */
    map<U>(
      fn: (value: T extends (infer E)[] ? E : T) => U | Promise<U>
    ): RpcPromiseMarker<T extends unknown[] ? U[] : U> {
      // Serialize the function
      const fnString = serializeFunction(fn)

      // Add map step to chain
      chain.push({ method: 'map', args: [fnString] })

      // Return the same promise (chain is modified in place)
      return rpcPromise as unknown as RpcPromiseMarker<T extends unknown[] ? U[] : U>
    },
  })

  return rpcPromise
}

// =============================================================================
// Chain Execution
// =============================================================================

/**
 * Execute the RPC chain
 *
 * Sends the entire chain to the server in a single request.
 * The server executes each step and returns the final result.
 */
async function executeChain<T>(state: RpcPromiseState<T>): Promise<void> {
  const { client, chain, resolve, reject } = state

  try {
    const response = await client.fetch('/rpc', {
      method: 'POST',
      body: JSON.stringify({ chain }),
    })

    if (!response.ok) {
      const errorText = await response.text()
      throw new RpcError(
        `RPC chain execution failed: ${errorText}`,
        response.status,
        chain
      )
    }

    const result = await response.json() as T
    state.result = result
    resolve?.(result)
  } catch (error: unknown) {
    state.error = error
    reject?.(error)
  }
}

// =============================================================================
// Function Serialization
// =============================================================================

/**
 * Serialized function representation
 */
export interface SerializedFunction {
  /** Function type */
  type: 'sync' | 'async'
  /** Function body as string */
  body: string
}

/**
 * Serialize a function to a string for transmission
 *
 * Extracts the function body and converts it to a form
 * that can be reconstructed on the server.
 *
 * @param fn - Function to serialize
 * @returns Serialized function string
 */
function serializeFunction(fn: Function): string {
  const fnString = fn.toString()

  // Handle arrow functions, regular functions, and methods
  // The server will use new Function() to reconstruct

  // Check if it's an async function
  const isAsync = fnString.startsWith('async')

  // Return the full function string
  // Server will wrap it appropriately
  return JSON.stringify({
    type: isAsync ? 'async' : 'sync',
    body: fnString,
  } satisfies SerializedFunction)
}

/**
 * Deserialize a function on the server side
 *
 * This is used by the server to reconstruct the mapping function.
 * SECURITY: Only use this with trusted input - executing arbitrary
 * code is dangerous.
 *
 * @param serialized - Serialized function string
 * @returns Reconstructed function
 */
export function deserializeFunction<T, U>(serialized: string): (value: T) => U {
  const { type, body } = JSON.parse(serialized) as SerializedFunction

  // Parse the function body to extract parameters and body
  // This handles arrow functions: (x) => x.foo or x => x.foo
  // And regular functions: function(x) { return x.foo }

  // For arrow functions: match (param) => body or param => body
  const arrowMatch = body.match(/^\s*(?:async\s+)?(?:\(([^)]*)\)|(\w+))\s*=>\s*([\s\S]+)$/)
  if (arrowMatch) {
    const params = arrowMatch[1] ?? arrowMatch[2] ?? ''
    const fnBody = arrowMatch[3]

    // Determine if body needs return statement
    const needsReturn = !fnBody.trim().startsWith('{')
    const actualBody = needsReturn ? `return ${fnBody}` : fnBody

    if (type === 'async') {
      return new Function(params, actualBody) as (value: T) => U
    }
    return new Function(params, actualBody) as (value: T) => U
  }

  // For regular functions: function name(params) { body }
  const funcMatch = body.match(/^\s*(?:async\s+)?function\s*\w*\s*\(([^)]*)\)\s*\{([\s\S]*)\}\s*$/)
  if (funcMatch) {
    const params = funcMatch[1] ?? ''
    const fnBody = funcMatch[2] ?? ''
    return new Function(params, fnBody) as (value: T) => U
  }

  throw new Error(`Unable to deserialize function: ${body}`)
}

// =============================================================================
// Error Types
// =============================================================================

/**
 * Error from RPC chain execution
 */
export class RpcError extends Error {
  constructor(
    message: string,
    public readonly status: number,
    public readonly chain: RpcPromiseChain
  ) {
    super(message)
    this.name = 'RpcError'
  }
}

// =============================================================================
// Utilities
// =============================================================================

/**
 * Check if a value is an RpcPromise
 */
export function isRpcPromise<T>(value: unknown): value is RpcPromiseMarker<T> {
  return (
    value !== null &&
    typeof value === 'object' &&
    '__rpcPromise' in value &&
    (value as { __rpcPromise: unknown }).__rpcPromise === true
  )
}

/**
 * Combine multiple RpcPromises into one request
 *
 * This is useful when you need to execute multiple chains
 * in parallel but want to minimize round trips.
 *
 * @example
 * ```typescript
 * const [posts, users] = await batchRpc(
 *   db.Posts.find({ status: 'published' }),
 *   db.Users.find({ active: true })
 * )
 * ```
 */
export async function batchRpc<T extends readonly RpcPromiseMarker<unknown>[]>(
  ...promises: T
): Promise<{ [K in keyof T]: T[K] extends RpcPromiseMarker<infer U> ? U : never }> {
  // For now, just await all promises in parallel
  // A more sophisticated implementation would batch
  // the chains into a single request
  return Promise.all(promises) as Promise<
    { [K in keyof T]: T[K] extends RpcPromiseMarker<infer U> ? U : never }
  >
}

/**
 * Create an already-resolved RpcPromise
 *
 * Useful for mocking or when you have a local value
 * that needs to be returned as an RpcPromise.
 */
export function resolvedRpcPromise<T>(value: T): RpcPromiseMarker<T> {
  const promise = Promise.resolve(value)

  return Object.assign(promise, {
    __rpcPromise: true as const,
    map<U>(
      fn: (v: T extends (infer E)[] ? E : T) => U | Promise<U>
    ): RpcPromiseMarker<T extends unknown[] ? U[] : U> {
      // For resolved promises, we can apply the map locally
      const mapped: Promise<unknown> = promise.then((result) => {
        if (Array.isArray(result)) {
          return Promise.all(result.map(fn as (item: unknown) => U | Promise<U>))
        }
        return (fn as (item: unknown) => U | Promise<U>)(result)
      })
      return resolvedRpcPromise(mapped) as RpcPromiseMarker<T extends unknown[] ? U[] : U>
    },
  })
}
