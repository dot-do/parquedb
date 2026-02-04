/**
 * CircuitBreakerBackend - Storage backend wrapper with circuit breaker protection
 *
 * Wraps any StorageBackend implementation to provide circuit breaker protection
 * for external service calls. This helps prevent cascading failures when external
 * services (R2, S3, remote APIs) are experiencing issues.
 *
 * Features:
 * - Per-operation type circuit breakers (reads, writes, deletes)
 * - Configurable failure thresholds and recovery timeouts
 * - Metrics collection for monitoring
 * - Graceful degradation with fallback support
 *
 * @example
 * ```typescript
 * import { R2Backend } from './R2Backend'
 * import { CircuitBreakerBackend, withCircuitBreaker } from './CircuitBreakerBackend'
 *
 * const r2 = new R2Backend(bucket)
 *
 * // Wrap with default settings
 * const protected = withCircuitBreaker(r2)
 *
 * // Or with custom configuration
 * const protected = new CircuitBreakerBackend(r2, {
 *   failureThreshold: 3,
 *   resetTimeoutMs: 15000,
 * })
 * ```
 */

import type {
  StorageBackend,
  FileStat,
  ListOptions,
  ListResult,
  WriteOptions,
  WriteResult,
  RmdirOptions,
} from '../types/storage'
import {
  CircuitBreaker,
  CircuitState,
  CircuitOpenError,
  type CircuitBreakerOptions,
  type CircuitBreakerMetrics,
} from './CircuitBreaker'

// =============================================================================
// Types
// =============================================================================

/**
 * Configuration options for CircuitBreakerBackend
 */
export interface CircuitBreakerBackendOptions extends CircuitBreakerOptions {
  /**
   * Use separate circuit breakers for read and write operations
   * When true, read failures won't affect write availability and vice versa
   * @default true
   */
  separateReadWriteCircuits?: boolean | undefined

  /**
   * Lightweight operations (exists, stat) bypass the circuit breaker
   * These operations are typically cheap and don't stress the backend
   * @default true
   */
  bypassLightweightOps?: boolean | undefined

  /**
   * Optional fallback backend for reads when circuit is open
   * Useful for implementing read replicas or cached fallbacks
   */
  fallbackBackend?: StorageBackend | undefined
}

/**
 * Aggregated metrics for all circuit breakers in the backend
 */
export interface CircuitBreakerBackendMetrics {
  /** Read circuit breaker metrics */
  read: CircuitBreakerMetrics
  /** Write circuit breaker metrics */
  write: CircuitBreakerMetrics
  /** Whether fallback is being used */
  usingFallback: boolean
  /** Backend type */
  backendType: string
}

// =============================================================================
// CircuitBreakerBackend Implementation
// =============================================================================

/**
 * Storage backend wrapper with circuit breaker protection
 */
export class CircuitBreakerBackend implements StorageBackend {
  readonly type: string

  private readonly backend: StorageBackend
  private readonly readCircuit: CircuitBreaker
  private readonly writeCircuit: CircuitBreaker
  private readonly separateCircuits: boolean
  private readonly bypassLightweight: boolean
  private readonly fallback?: StorageBackend | undefined

  constructor(backend: StorageBackend, options: CircuitBreakerBackendOptions = {}) {
    this.backend = backend
    this.type = `circuit-breaker:${backend.type}`
    this.separateCircuits = options.separateReadWriteCircuits ?? true
    this.bypassLightweight = options.bypassLightweightOps ?? true
    this.fallback = options.fallbackBackend

    // Create circuit breakers
    const baseOptions: CircuitBreakerOptions = {
      failureThreshold: options.failureThreshold,
      successThreshold: options.successThreshold,
      resetTimeoutMs: options.resetTimeoutMs,
      failureWindowMs: options.failureWindowMs,
      isFailure: options.isFailure,
    }

    this.readCircuit = new CircuitBreaker({
      ...baseOptions,
      name: `${backend.type}:read`,
      onStateChange: options.onStateChange,
    })

    if (this.separateCircuits) {
      this.writeCircuit = new CircuitBreaker({
        ...baseOptions,
        name: `${backend.type}:write`,
        onStateChange: options.onStateChange,
      })
    } else {
      // Use same circuit for both
      this.writeCircuit = this.readCircuit
    }
  }

  /**
   * Get the underlying backend
   */
  get inner(): StorageBackend {
    return this.backend
  }

  /**
   * Get aggregated metrics for all circuit breakers
   */
  getMetrics(): CircuitBreakerBackendMetrics {
    return {
      read: this.readCircuit.getMetrics(),
      write: this.writeCircuit.getMetrics(),
      usingFallback: this.readCircuit.getState() === CircuitState.OPEN && !!this.fallback,
      backendType: this.backend.type,
    }
  }

  /**
   * Reset all circuit breakers
   */
  reset(): void {
    this.readCircuit.reset()
    if (this.separateCircuits) {
      this.writeCircuit.reset()
    }
  }

  // ===========================================================================
  // Read Operations
  // ===========================================================================

  async read(path: string): Promise<Uint8Array> {
    try {
      return await this.readCircuit.execute(() => this.backend.read(path))
    } catch (error) {
      if (error instanceof CircuitOpenError && this.fallback) {
        return this.fallback.read(path)
      }
      throw error
    }
  }

  async readRange(path: string, start: number, end: number): Promise<Uint8Array> {
    try {
      return await this.readCircuit.execute(() => this.backend.readRange(path, start, end))
    } catch (error) {
      if (error instanceof CircuitOpenError && this.fallback) {
        return this.fallback.readRange(path, start, end)
      }
      throw error
    }
  }

  async exists(path: string): Promise<boolean> {
    if (this.bypassLightweight) {
      return this.backend.exists(path)
    }
    try {
      return await this.readCircuit.execute(() => this.backend.exists(path))
    } catch (error) {
      if (error instanceof CircuitOpenError && this.fallback) {
        return this.fallback.exists(path)
      }
      throw error
    }
  }

  async stat(path: string): Promise<FileStat | null> {
    if (this.bypassLightweight) {
      return this.backend.stat(path)
    }
    try {
      return await this.readCircuit.execute(() => this.backend.stat(path))
    } catch (error) {
      if (error instanceof CircuitOpenError && this.fallback) {
        return this.fallback.stat(path)
      }
      throw error
    }
  }

  async list(prefix: string, options?: ListOptions): Promise<ListResult> {
    try {
      return await this.readCircuit.execute(() => this.backend.list(prefix, options))
    } catch (error) {
      if (error instanceof CircuitOpenError && this.fallback) {
        return this.fallback.list(prefix, options)
      }
      throw error
    }
  }

  // ===========================================================================
  // Write Operations
  // ===========================================================================

  async write(path: string, data: Uint8Array, options?: WriteOptions): Promise<WriteResult> {
    return this.writeCircuit.execute(() => this.backend.write(path, data, options))
  }

  async writeAtomic(path: string, data: Uint8Array, options?: WriteOptions): Promise<WriteResult> {
    return this.writeCircuit.execute(() => this.backend.writeAtomic(path, data, options))
  }

  async append(path: string, data: Uint8Array): Promise<void> {
    return this.writeCircuit.execute(() => this.backend.append(path, data))
  }

  async delete(path: string): Promise<boolean> {
    return this.writeCircuit.execute(() => this.backend.delete(path))
  }

  async deletePrefix(prefix: string): Promise<number> {
    return this.writeCircuit.execute(() => this.backend.deletePrefix(prefix))
  }

  // ===========================================================================
  // Directory Operations
  // ===========================================================================

  async mkdir(path: string): Promise<void> {
    if (this.bypassLightweight) {
      return this.backend.mkdir(path)
    }
    return this.writeCircuit.execute(() => this.backend.mkdir(path))
  }

  async rmdir(path: string, options?: RmdirOptions): Promise<void> {
    return this.writeCircuit.execute(() => this.backend.rmdir(path, options))
  }

  // ===========================================================================
  // Atomic Operations
  // ===========================================================================

  async writeConditional(
    path: string,
    data: Uint8Array,
    expectedVersion: string | null,
    options?: WriteOptions
  ): Promise<WriteResult> {
    return this.writeCircuit.execute(() =>
      this.backend.writeConditional(path, data, expectedVersion, options)
    )
  }

  async copy(source: string, dest: string): Promise<void> {
    return this.writeCircuit.execute(() => this.backend.copy(source, dest))
  }

  async move(source: string, dest: string): Promise<void> {
    return this.writeCircuit.execute(() => this.backend.move(source, dest))
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Wrap a storage backend with circuit breaker protection
 *
 * @param backend - Storage backend to wrap
 * @param options - Circuit breaker configuration
 * @returns Protected storage backend
 *
 * @example
 * ```typescript
 * const r2 = new R2Backend(bucket)
 * const protected = withCircuitBreaker(r2)
 *
 * // With custom options
 * const protected = withCircuitBreaker(r2, {
 *   failureThreshold: 3,
 *   resetTimeoutMs: 10000,
 * })
 * ```
 */
export function withCircuitBreaker(
  backend: StorageBackend,
  options?: CircuitBreakerBackendOptions
): CircuitBreakerBackend {
  // Don't double-wrap
  if (backend instanceof CircuitBreakerBackend) {
    return backend
  }
  return new CircuitBreakerBackend(backend, options)
}

/**
 * Create a circuit breaker backend with read fallback
 *
 * When the primary backend's read circuit opens, reads automatically
 * fall back to the secondary backend. Useful for read replicas or caches.
 *
 * @param primary - Primary storage backend
 * @param fallback - Fallback backend for reads
 * @param options - Circuit breaker configuration
 *
 * @example
 * ```typescript
 * const primary = new R2Backend(bucket)
 * const cache = new MemoryBackend()
 *
 * const resilient = withReadFallback(primary, cache)
 * ```
 */
export function withReadFallback(
  primary: StorageBackend,
  fallback: StorageBackend,
  options?: Omit<CircuitBreakerBackendOptions, 'fallbackBackend'>
): CircuitBreakerBackend {
  return new CircuitBreakerBackend(primary, {
    ...options,
    fallbackBackend: fallback,
  })
}

// =============================================================================
// Re-exports
// =============================================================================

export {
  CircuitBreaker,
  CircuitState,
  CircuitOpenError,
  type CircuitBreakerOptions,
  type CircuitBreakerMetrics,
} from './CircuitBreaker'
