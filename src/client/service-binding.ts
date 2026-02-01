/**
 * ServiceBindingAdapter - Adapter for Cloudflare Service Bindings
 *
 * Provides a unified interface for communicating with ParqueDB
 * whether through HTTP fetch or direct RPC via Service Bindings.
 */

import type { RpcPromiseChain } from './rpc-promise'

// =============================================================================
// Types
// =============================================================================

/**
 * Cloudflare Service type
 * Represents a binding to another Worker
 */
export interface Service {
  /** Execute a fetch request to the service */
  fetch(request: Request): Promise<Response>

  /** Connect to a socket (for Workers with WebSocket support) */
  connect?(address: string): unknown

  /** RPC methods are added dynamically based on the Worker's exports */
  [key: string]: unknown
}

/**
 * Result from a service binding RPC call
 */
export interface ServiceBindingResult<T> {
  /** Whether the call was successful */
  success: boolean
  /** Result data (if successful) */
  data?: T
  /** Error message (if failed) */
  error?: string
}

// =============================================================================
// ServiceBindingAdapter Class
// =============================================================================

/**
 * Adapter for Cloudflare Service Bindings
 *
 * Service Bindings allow Workers to call each other directly
 * without going through the network. This adapter provides
 * both HTTP-style and direct RPC-style access.
 *
 * @example
 * ```typescript
 * // In your Worker
 * export default {
 *   async fetch(request, env) {
 *     const adapter = new ServiceBindingAdapter(env.PARQUEDB)
 *
 *     // Use HTTP-style
 *     const response = await adapter.fetch('/rpc', {
 *       method: 'POST',
 *       body: JSON.stringify({ chain })
 *     })
 *
 *     // Or direct RPC (if the binding supports it)
 *     const result = await adapter.call('find', ['posts', { status: 'published' }])
 *   }
 * }
 * ```
 */
export class ServiceBindingAdapter {
  constructor(private binding: Service) {}

  // ===========================================================================
  // HTTP-style Methods
  // ===========================================================================

  /**
   * Execute a fetch request through the service binding
   *
   * @param path - Request path
   * @param init - Request init options
   * @returns Response from the service
   */
  async fetch(path: string, init?: RequestInit): Promise<Response> {
    // Create a request with a dummy URL (service bindings ignore the host)
    const url = new URL(path, 'https://service.local')
    const request = new Request(url.toString(), init)
    return this.binding.fetch(request)
  }

  // ===========================================================================
  // Direct RPC Methods
  // ===========================================================================

  /**
   * Call an RPC method directly on the service binding
   *
   * This uses Cloudflare's RPC support for service bindings,
   * which allows calling methods on the Worker's exported class.
   *
   * @param method - Method name to call
   * @param args - Method arguments
   * @returns Result from the method
   */
  async call<T>(method: string, args: unknown[]): Promise<T> {
    // Check if the binding supports direct RPC
    const bindingMethod = this.binding[method]
    if (typeof bindingMethod === 'function') {
      // Call the method directly
      return bindingMethod(...args) as T
    }

    // Fall back to HTTP-style call
    const response = await this.fetch('/rpc', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ method, args }),
    })

    if (!response.ok) {
      throw new Error(`RPC call failed: ${await response.text()}`)
    }

    return response.json()
  }

  /**
   * Execute an RPC chain through the service binding
   *
   * @param chain - Chain of RPC operations
   * @returns Result from the chain execution
   */
  async executeChain<T>(chain: RpcPromiseChain): Promise<T> {
    // Try direct RPC first
    const bindingExecuteChain = this.binding.executeChain
    if (typeof bindingExecuteChain === 'function') {
      return bindingExecuteChain(chain) as T
    }

    // Fall back to HTTP
    const response = await this.fetch('/rpc', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chain }),
    })

    if (!response.ok) {
      throw new Error(`Chain execution failed: ${await response.text()}`)
    }

    return response.json()
  }

  // ===========================================================================
  // Collection Methods
  // ===========================================================================

  /**
   * Find entities in a namespace
   */
  async find<T>(
    ns: string,
    filter?: Record<string, unknown>,
    options?: Record<string, unknown>
  ): Promise<T> {
    return this.call<T>('find', [ns, filter, options])
  }

  /**
   * Get a single entity
   */
  async get<T>(ns: string, id: string, options?: Record<string, unknown>): Promise<T | null> {
    return this.call<T | null>('get', [ns, id, options])
  }

  /**
   * Create an entity
   */
  async create<T>(
    ns: string,
    data: Record<string, unknown>,
    options?: Record<string, unknown>
  ): Promise<T> {
    return this.call<T>('create', [ns, data, options])
  }

  /**
   * Update an entity
   */
  async update<T>(
    ns: string,
    id: string,
    update: Record<string, unknown>,
    options?: Record<string, unknown>
  ): Promise<T | null> {
    return this.call<T | null>('update', [ns, id, update, options])
  }

  /**
   * Delete an entity
   */
  async delete(
    ns: string,
    id: string,
    options?: Record<string, unknown>
  ): Promise<{ deletedCount: number }> {
    return this.call<{ deletedCount: number }>('delete', [ns, id, options])
  }

  // ===========================================================================
  // Health Check
  // ===========================================================================

  /**
   * Check if the service is healthy
   */
  async health(): Promise<{ ok: boolean; version?: string }> {
    try {
      const response = await this.fetch('/health')
      if (response.ok) {
        return response.json()
      }
      return { ok: false }
    } catch {
      return { ok: false }
    }
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a ServiceBindingAdapter from an environment binding
 *
 * @example
 * ```typescript
 * // In your Worker
 * const adapter = createServiceAdapter(env.PARQUEDB)
 * const posts = await adapter.find('posts', { status: 'published' })
 * ```
 */
export function createServiceAdapter(binding: Service): ServiceBindingAdapter {
  return new ServiceBindingAdapter(binding)
}

/**
 * Check if a value is a service binding
 */
export function isServiceBinding(value: unknown): value is Service {
  return (
    value !== null &&
    typeof value === 'object' &&
    typeof (value as Service).fetch === 'function'
  )
}
