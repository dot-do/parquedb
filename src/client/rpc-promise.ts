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

/**
 * Service interface for RPC communication
 */
export interface RpcService {
  fetch(path: string, options?: { method?: string; body?: string; headers?: Record<string, string> }): Promise<Response>
}

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
 * Mapper type for serialized functions
 */
export type SerializedMapperType =
  | 'path'       // Safe property path: "author.name"
  | 'registered' // Pre-registered mapper by name
  | 'legacy'     // Legacy new Function() - deprecated, validates pattern

/**
 * Serialized mapper representation (v2 - secure)
 */
export interface SerializedMapper {
  /** Mapper type */
  mapperType: SerializedMapperType
  /** For 'path': the property path (e.g., "author.name") */
  path?: string
  /** For 'registered': the mapper name */
  name?: string
  /** For 'legacy': the function body (validated for safety) */
  body?: string
  /** Whether the original function was async */
  async?: boolean
}

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
// Mapper Registry
// =============================================================================

/**
 * Registry of pre-defined safe mapper functions
 *
 * Register mappers here for use across the application.
 * This is the safest way to use server-side mapping.
 *
 * @example
 * ```typescript
 * // Register a mapper
 * registerMapper('extractTitle', (entity) => entity.title)
 *
 * // Use it in queries
 * const titles = await posts.find().map('extractTitle')
 * ```
 */
const mapperRegistry = new Map<string, (value: unknown) => unknown>()

/**
 * Register a mapper function by name
 *
 * @param name - Unique name for the mapper
 * @param fn - The mapper function
 */
export function registerMapper<T, U>(name: string, fn: (value: T) => U): void {
  if (mapperRegistry.has(name)) {
    throw new Error(`Mapper '${name}' is already registered`)
  }
  mapperRegistry.set(name, fn as (value: unknown) => unknown)
}

/**
 * Get a registered mapper by name
 *
 * @param name - The mapper name
 * @returns The mapper function or undefined
 */
export function getRegisteredMapper(name: string): ((value: unknown) => unknown) | undefined {
  return mapperRegistry.get(name)
}

/**
 * Clear all registered mappers (for testing)
 */
export function clearMapperRegistry(): void {
  mapperRegistry.clear()
}

// =============================================================================
// Function Serialization (Secure)
// =============================================================================

/**
 * Serialized function representation (legacy - for backward compatibility)
 * @deprecated Use SerializedMapper instead
 */
export interface SerializedFunction {
  /** Function type */
  type: 'sync' | 'async'
  /** Function body as string */
  body: string
}

/**
 * Safe property path pattern
 * Matches: x.foo, x.foo.bar, x["foo"], x['foo'], x[0], etc.
 * Does NOT match: function calls, arithmetic, assignments, etc.
 */
const SAFE_PROPERTY_PATH_PATTERN = /^[\w$]+(?:\.[\w$]+|\[(?:\d+|['"][^'"]+['"])\])*$/

/**
 * Extract property path from a simple arrow function
 *
 * Handles: (x) => x.foo, x => x.foo.bar, (item) => item.author.name
 * Returns null for complex functions that can't be safely converted to paths
 */
function extractPropertyPath(fnString: string): string | null {
  // Match arrow function: (param) => body or param => body
  const arrowMatch = fnString.match(/^\s*(?:async\s+)?(?:\((\w+)\)|(\w+))\s*=>\s*([\s\S]+)$/)
  if (!arrowMatch) return null

  const param = arrowMatch[1] ?? arrowMatch[2]
  const body = arrowMatch[3]?.trim()
  if (!param || !body) return null

  // Check if body is just property access on the parameter
  // e.g., "x.name" or "x.author.name" or "x['foo']"
  if (!body.startsWith(param)) return null

  // Extract the path after the parameter
  const pathPart = body.slice(param.length)
  if (!pathPart) return '' // Just returning the parameter itself

  // Validate the path is safe (only property access)
  // Remove leading dot if present
  const path = pathPart.startsWith('.') ? pathPart.slice(1) : pathPart

  // For bracket notation at the start
  if (pathPart.startsWith('[')) {
    // Validate the entire path including brackets
    const fullPath = param + pathPart
    if (!SAFE_PROPERTY_PATH_PATTERN.test(fullPath)) return null
    return path
  }

  // For dot notation
  if (!SAFE_PROPERTY_PATH_PATTERN.test(param + '.' + path)) return null
  return path
}

/**
 * Serialize a function to a safe representation
 *
 * This function analyzes the input and produces a secure serialized form:
 * 1. If the function is simple property access, extracts the path
 * 2. If it matches a registered mapper, uses the name
 * 3. Otherwise, validates and uses legacy mode (with strict pattern checking)
 *
 * @param fn - Function to serialize
 * @returns Serialized mapper string
 */
function serializeFunction(fn: Function): string {
  const fnString = fn.toString()
  const isAsync = fnString.startsWith('async')

  // Try to extract as a safe property path
  const path = extractPropertyPath(fnString)
  if (path !== null) {
    return JSON.stringify({
      mapperType: 'path',
      path,
      async: isAsync,
    } satisfies SerializedMapper)
  }

  // For complex functions, use legacy mode with validation warning
  // The server will validate these strictly
  return JSON.stringify({
    mapperType: 'legacy',
    body: fnString,
    async: isAsync,
  } satisfies SerializedMapper)
}

/**
 * Get value at a property path from an object
 *
 * @param obj - The object to traverse
 * @param path - The property path (e.g., "author.name" or "items[0].title")
 * @returns The value at the path, or undefined if not found
 */
function getAtPath(obj: unknown, path: string): unknown {
  if (!path) return obj

  // Parse the path into segments
  const segments: (string | number)[] = []
  const pathRegex = /\.?([^.[]+)|\[(\d+|'[^']*'|"[^"]*")\]/g
  let match

  while ((match = pathRegex.exec(path)) !== null) {
    if (match[1] !== undefined) {
      segments.push(match[1])
    } else if (match[2] !== undefined) {
      // Remove quotes if present, or parse as number
      const segment = match[2]
      if (segment.startsWith("'") || segment.startsWith('"')) {
        segments.push(segment.slice(1, -1))
      } else {
        segments.push(parseInt(segment, 10))
      }
    }
  }

  // Traverse the object
  let current: unknown = obj
  for (const segment of segments) {
    if (current === null || current === undefined) return undefined
    if (typeof current !== 'object') return undefined
    current = (current as Record<string | number, unknown>)[segment]
  }

  return current
}

/**
 * Validate that a function body is safe for legacy deserialization
 *
 * Only allows very simple patterns:
 * - Property access: x.foo, x.foo.bar
 * - Object literals: { id: x.$id, name: x.name }
 * - Template literals with only property access: `${x.first} ${x.last}`
 * - Ternary with property access: x.active ? "yes" : "no"
 * - Array access: x.items[0]
 *
 * Does NOT allow:
 * - Function calls (except for safe built-ins like .slice())
 * - Assignments
 * - eval, Function, import
 * - Network access
 * - DOM manipulation
 */
function validateLegacyFunction(body: string): void {
  // Forbidden patterns that indicate potentially dangerous code
  const forbiddenPatterns = [
    /\beval\b/i,                    // eval()
    /\bFunction\b/,                 // new Function()
    /\bimport\b/,                   // dynamic import
    /\brequire\b/,                  // require()
    /\bprocess\b/,                  // process.env, etc.
    /\bglobal(?:This)?\b/,          // globalThis, global
    /\bwindow\b/,                   // window object
    /\bdocument\b/,                 // document object
    /\bfetch\b/,                    // fetch()
    /\bXMLHttpRequest\b/,           // XHR
    /\bWebSocket\b/,                // WebSocket
    /\bWorker\b/,                   // Worker
    /\bsetTimeout\b/,               // setTimeout
    /\bsetInterval\b/,              // setInterval
    /\bsetImmediate\b/,             // setImmediate
    /\bqueueMicrotask\b/,           // queueMicrotask
    /\bPromise\b/,                  // Promise constructor
    /\bReflect\b/,                  // Reflect API
    /\bProxy\b/,                    // Proxy
    /\bObject\.(?:assign|create|definePropert|getOwnPropertyDescriptor|setPrototypeOf)\b/, // Dangerous Object methods
    /\bArray\.from\b/,              // Array.from (can be used with iterators)
    /\.__proto__\b/,                // Prototype manipulation
    /\bconstructor\b/,              // Constructor access
    /\bprototype\b/,                // Prototype access
    /[=!]=\s*undefined|undefined\s*[=!]=/, // undefined comparison (could be redefined)
    /\bthis\b/,                     // this keyword (context dependent)
    /\barguments\b/,                // arguments object
    /\bawait\b/,                    // await (async context)
    /\byield\b/,                    // yield (generator)
    /\breturn\s+[^};\s]+\s*\(/,     // Return with function call (complex)
  ]

  for (const pattern of forbiddenPatterns) {
    if (pattern.test(body)) {
      throw new Error(
        `Unsafe function pattern detected. For security, use property paths ` +
        `(e.g., p => p.name) or register mappers with registerMapper().`
      )
    }
  }

  // Additional check: no complex function calls except safe ones
  // Allow: .slice(), .trim(), .toLowerCase(), .toUpperCase(), .toString()
  const safeMethodPattern = /\.(slice|trim|toLowerCase|toUpperCase|toString|valueOf|toFixed|toPrecision)\s*\(/g
  const functionCallPattern = /\w+\s*\(/g

  let match
  const functionCalls: string[] = []
  while ((match = functionCallPattern.exec(body)) !== null) {
    functionCalls.push(match[0])
  }

  // Remove safe method calls
  const safeMethods = new Set(['slice(', 'trim(', 'toLowerCase(', 'toUpperCase(', 'toString(', 'valueOf(', 'toFixed(', 'toPrecision('])
  const unsafeCalls = functionCalls.filter(call => {
    const methodName = call.trim()
    return !safeMethods.has(methodName)
  })

  // Arrow function itself is OK: (x) or x =>
  // Also OK: function(x)
  const filtered = unsafeCalls.filter(call => {
    return !/^(?:async\s+)?(?:function)?\s*\(/.test(call) && call !== '('
  })

  if (filtered.length > 0) {
    throw new Error(
      `Unsafe function calls detected: ${filtered.join(', ')}. ` +
      `For security, use property paths (e.g., p => p.name) or register mappers.`
    )
  }
}

/**
 * Deserialize a mapper function on the server side
 *
 * This is the secure version that handles different mapper types:
 * - 'path': Safe property path traversal (no code execution)
 * - 'registered': Lookup in pre-registered mapper functions
 * - 'legacy': Validated function body (strict pattern checking)
 *
 * @param serialized - Serialized mapper string
 * @returns Reconstructed function
 */
export function deserializeFunction<T, U>(serialized: string): (value: T) => U {
  const parsed = JSON.parse(serialized)

  // Handle new secure format
  if ('mapperType' in parsed) {
    const mapper = parsed as SerializedMapper

    switch (mapper.mapperType) {
      case 'path': {
        // Safe property path - no code execution
        const path = mapper.path ?? ''
        return ((value: T) => getAtPath(value, path)) as (value: T) => U
      }

      case 'registered': {
        // Lookup registered mapper
        const name = mapper.name
        if (!name) {
          throw new Error('Registered mapper requires a name')
        }
        const fn = getRegisteredMapper(name)
        if (!fn) {
          throw new Error(`Mapper '${name}' is not registered`)
        }
        return fn as (value: T) => U
      }

      case 'legacy': {
        // Legacy mode with validation
        const body = mapper.body
        if (!body) {
          throw new Error('Legacy mapper requires a body')
        }

        // Strict validation before using new Function()
        validateLegacyFunction(body)

        // Parse and execute (same as before, but validated)
        return deserializeLegacyFunction<T, U>(body)
      }

      default:
        throw new Error(`Unknown mapper type: ${(mapper as SerializedMapper).mapperType}`)
    }
  }

  // Handle legacy format (for backward compatibility)
  const legacy = parsed as SerializedFunction
  if ('type' in legacy && 'body' in legacy) {
    // Validate legacy function
    validateLegacyFunction(legacy.body)
    return deserializeLegacyFunction<T, U>(legacy.body)
  }

  throw new Error(`Invalid serialized function format`)
}

/**
 * Deserialize a legacy function body (after validation)
 *
 * @internal
 */
function deserializeLegacyFunction<T, U>(body: string): (value: T) => U {
  // For arrow functions: match (param) => body or param => body
  const arrowMatch = body.match(/^\s*(?:async\s+)?(?:\(([^)]*)\)|(\w+))\s*=>\s*([\s\S]+)$/)
  if (arrowMatch) {
    const params = arrowMatch[1] ?? arrowMatch[2] ?? ''
    const fnBody = arrowMatch[3]

    // Determine if body needs return statement
    const needsReturn = !fnBody.trim().startsWith('{')
    const actualBody = needsReturn ? `return ${fnBody}` : fnBody

    // eslint-disable-next-line @typescript-eslint/no-implied-eval
    return new Function(params, actualBody) as (value: T) => U
  }

  // For regular functions: function name(params) { body }
  const funcMatch = body.match(/^\s*(?:async\s+)?function\s*\w*\s*\(([^)]*)\)\s*\{([\s\S]*)\}\s*$/)
  if (funcMatch) {
    const params = funcMatch[1] ?? ''
    const fnBody = funcMatch[2] ?? ''
    // eslint-disable-next-line @typescript-eslint/no-implied-eval
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
    Object.setPrototypeOf(this, RpcError.prototype)
  }
  override readonly name = 'RpcError'
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
