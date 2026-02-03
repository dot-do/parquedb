/**
 * CircuitBreakerBackend Tests
 *
 * Tests for the storage backend wrapper with circuit breaker protection.
 * Verifies that operations are properly protected and fallback behavior works.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import {
  CircuitBreakerBackend,
  withCircuitBreaker,
  withReadFallback,
  CircuitState,
  CircuitOpenError,
} from '../../../src/storage/CircuitBreakerBackend'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import type { StorageBackend } from '../../../src/types/storage'

// =============================================================================
// Helpers
// =============================================================================

function textToBytes(text: string): Uint8Array {
  return new TextEncoder().encode(text)
}

function bytesToText(bytes: Uint8Array): string {
  return new TextDecoder().decode(bytes)
}

/**
 * Advance fake timers by specified milliseconds and flush pending promises
 */
async function advanceTime(ms: number): Promise<void> {
  vi.advanceTimersByTime(ms)
  // Flush any pending promise resolutions
  await vi.runOnlyPendingTimersAsync()
}

/**
 * Create a backend that fails on specific operations
 */
function createFailingBackend(
  operations: Array<'read' | 'write' | 'list' | 'delete' | 'all'>
): MemoryBackend {
  const backend = new MemoryBackend()
  const shouldFail = (op: string) =>
    operations.includes('all') || operations.includes(op as 'read' | 'write' | 'list' | 'delete')

  if (shouldFail('read')) {
    backend.read = async () => {
      throw new Error('Read failed')
    }
  }
  if (shouldFail('write')) {
    backend.write = async () => {
      throw new Error('Write failed')
    }
  }
  if (shouldFail('list')) {
    backend.list = async () => {
      throw new Error('List failed')
    }
  }
  if (shouldFail('delete')) {
    backend.delete = async () => {
      throw new Error('Delete failed')
    }
  }

  return backend
}

// =============================================================================
// Test Suite
// =============================================================================

describe('CircuitBreakerBackend', () => {
  let inner: MemoryBackend
  let wrapped: CircuitBreakerBackend

  beforeEach(() => {
    vi.useFakeTimers()
    inner = new MemoryBackend()
    wrapped = new CircuitBreakerBackend(inner, {
      failureThreshold: 3,
      successThreshold: 2,
      resetTimeoutMs: 100,
      separateReadWriteCircuits: true,
    })
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  // ===========================================================================
  // Constructor and Type
  // ===========================================================================

  describe('constructor and type', () => {
    it('should set type to circuit-breaker:{inner.type}', () => {
      expect(wrapped.type).toBe('circuit-breaker:memory')
    })

    it('should expose the inner backend via .inner', () => {
      expect(wrapped.inner).toBe(inner)
    })
  })

  // ===========================================================================
  // Normal Operation (CLOSED state)
  // ===========================================================================

  describe('normal operation', () => {
    it('should pass through read operations', async () => {
      await inner.write('test.txt', textToBytes('hello'))
      const result = await wrapped.read('test.txt')
      expect(bytesToText(result)).toBe('hello')
    })

    it('should pass through write operations', async () => {
      const result = await wrapped.write('test.txt', textToBytes('hello'))
      expect(result.size).toBe(5)

      const data = await inner.read('test.txt')
      expect(bytesToText(data)).toBe('hello')
    })

    it('should pass through list operations', async () => {
      await inner.write('a.txt', textToBytes('a'))
      await inner.write('b.txt', textToBytes('b'))

      const result = await wrapped.list('')
      expect(result.files).toHaveLength(2)
    })

    it('should pass through delete operations', async () => {
      await inner.write('test.txt', textToBytes('data'))
      const deleted = await wrapped.delete('test.txt')
      expect(deleted).toBe(true)
      expect(await inner.exists('test.txt')).toBe(false)
    })

    it('should pass through stat operations (bypassing circuit)', async () => {
      await inner.write('test.txt', textToBytes('data'))
      const stat = await wrapped.stat('test.txt')
      expect(stat).not.toBeNull()
      expect(stat!.size).toBe(4)
    })

    it('should pass through exists operations (bypassing circuit)', async () => {
      await inner.write('test.txt', textToBytes('data'))
      expect(await wrapped.exists('test.txt')).toBe(true)
      expect(await wrapped.exists('nope.txt')).toBe(false)
    })
  })

  // ===========================================================================
  // Circuit Opening on Failures
  // ===========================================================================

  describe('circuit opening on failures', () => {
    it('should open read circuit after threshold failures', async () => {
      const failing = createFailingBackend(['read'])
      const protectedBackend = new CircuitBreakerBackend(failing, {
        failureThreshold: 3,
        resetTimeoutMs: 100,
      })

      // Trigger failures
      for (let i = 0; i < 3; i++) {
        try {
          await protectedBackend.read('test.txt')
        } catch {
          // Expected
        }
      }

      // Next read should fail fast
      await expect(protectedBackend.read('test.txt')).rejects.toThrow(CircuitOpenError)

      const metrics = protectedBackend.getMetrics()
      expect(metrics.read.state).toBe(CircuitState.OPEN)
    })

    it('should open write circuit after threshold failures', async () => {
      const failing = createFailingBackend(['write'])
      const protectedBackend = new CircuitBreakerBackend(failing, {
        failureThreshold: 3,
        resetTimeoutMs: 100,
      })

      // Trigger failures
      for (let i = 0; i < 3; i++) {
        try {
          await protectedBackend.write('test.txt', textToBytes('data'))
        } catch {
          // Expected
        }
      }

      // Next write should fail fast
      await expect(protectedBackend.write('test.txt', textToBytes('data'))).rejects.toThrow(CircuitOpenError)

      const metrics = protectedBackend.getMetrics()
      expect(metrics.write.state).toBe(CircuitState.OPEN)
    })
  })

  // ===========================================================================
  // Separate Read/Write Circuits
  // ===========================================================================

  describe('separate read/write circuits', () => {
    it('should not affect write circuit when read circuit opens', async () => {
      const failing = createFailingBackend(['read'])
      const protectedBackend = new CircuitBreakerBackend(failing, {
        failureThreshold: 2,
        resetTimeoutMs: 100,
        separateReadWriteCircuits: true,
      })

      // Open the read circuit
      for (let i = 0; i < 2; i++) {
        try {
          await protectedBackend.read('test.txt')
        } catch {
          // Expected
        }
      }

      // Read should fail fast
      await expect(protectedBackend.read('test.txt')).rejects.toThrow(CircuitOpenError)

      // Write should still work
      const result = await protectedBackend.write('test.txt', textToBytes('data'))
      expect(result.size).toBe(4)
    })

    it('should not affect read circuit when write circuit opens', async () => {
      const failing = createFailingBackend(['write'])
      const protectedBackend = new CircuitBreakerBackend(failing, {
        failureThreshold: 2,
        resetTimeoutMs: 100,
        separateReadWriteCircuits: true,
      })

      // Write some data first
      const inner = new MemoryBackend()
      await inner.write('test.txt', textToBytes('hello'))

      // Replace the inner backend to have data to read
      const combined = Object.assign({}, failing)
      combined.read = inner.read.bind(inner)
      combined.exists = inner.exists.bind(inner)

      const protectedCombined = new CircuitBreakerBackend(combined as StorageBackend, {
        failureThreshold: 2,
        resetTimeoutMs: 100,
        separateReadWriteCircuits: true,
      })

      // Open the write circuit
      for (let i = 0; i < 2; i++) {
        try {
          await protectedCombined.write('test.txt', textToBytes('data'))
        } catch {
          // Expected
        }
      }

      // Write should fail fast
      await expect(protectedCombined.write('test.txt', textToBytes('data'))).rejects.toThrow(CircuitOpenError)

      // Read should still work
      const data = await protectedCombined.read('test.txt')
      expect(bytesToText(data)).toBe('hello')
    })

    it('should share circuit when separateReadWriteCircuits is false', async () => {
      const failing = createFailingBackend(['read'])
      const protectedBackend = new CircuitBreakerBackend(failing, {
        failureThreshold: 2,
        resetTimeoutMs: 100,
        separateReadWriteCircuits: false,
      })

      // Open the circuit via reads
      for (let i = 0; i < 2; i++) {
        try {
          await protectedBackend.read('test.txt')
        } catch {
          // Expected
        }
      }

      // Both read and write should fail fast
      await expect(protectedBackend.read('test.txt')).rejects.toThrow(CircuitOpenError)
      await expect(protectedBackend.write('test.txt', textToBytes('data'))).rejects.toThrow(CircuitOpenError)
    })
  })

  // ===========================================================================
  // Fallback Backend
  // ===========================================================================

  describe('fallback backend', () => {
    it('should use fallback for reads when circuit is open', async () => {
      const primary = createFailingBackend(['read'])
      const fallback = new MemoryBackend()
      await fallback.write('test.txt', textToBytes('fallback data'))

      const protectedBackend = withReadFallback(primary, fallback, {
        failureThreshold: 2,
        resetTimeoutMs: 100,
      })

      // Open the circuit
      for (let i = 0; i < 2; i++) {
        try {
          await protectedBackend.read('test.txt')
        } catch {
          // Expected
        }
      }

      // Should read from fallback
      const data = await protectedBackend.read('test.txt')
      expect(bytesToText(data)).toBe('fallback data')

      const metrics = protectedBackend.getMetrics()
      expect(metrics.usingFallback).toBe(true)
    })

    it('should use fallback for readRange when circuit is open', async () => {
      const primary = createFailingBackend(['read'])
      primary.readRange = async () => {
        throw new Error('readRange failed')
      }
      const fallback = new MemoryBackend()
      await fallback.write('test.txt', textToBytes('fallback data'))

      const protectedBackend = withReadFallback(primary, fallback, {
        failureThreshold: 2,
        resetTimeoutMs: 100,
      })

      // Open the circuit
      for (let i = 0; i < 2; i++) {
        try {
          await protectedBackend.readRange('test.txt', 0, 5)
        } catch {
          // Expected
        }
      }

      // Should read from fallback
      const data = await protectedBackend.readRange('test.txt', 0, 8)
      expect(bytesToText(data)).toBe('fallback')
    })

    it('should use fallback for list when circuit is open', async () => {
      const primary = createFailingBackend(['list'])
      const fallback = new MemoryBackend()
      await fallback.write('a.txt', textToBytes('a'))
      await fallback.write('b.txt', textToBytes('b'))

      const protectedBackend = withReadFallback(primary, fallback, {
        failureThreshold: 2,
        resetTimeoutMs: 100,
      })

      // Open the circuit
      for (let i = 0; i < 2; i++) {
        try {
          await protectedBackend.list('')
        } catch {
          // Expected
        }
      }

      // Should list from fallback
      const result = await protectedBackend.list('')
      expect(result.files).toHaveLength(2)
    })

    it('should not use fallback for writes', async () => {
      const primary = createFailingBackend(['write'])
      const fallback = new MemoryBackend()

      const protectedBackend = withReadFallback(primary, fallback, {
        failureThreshold: 2,
        resetTimeoutMs: 100,
      })

      // Open the circuit
      for (let i = 0; i < 2; i++) {
        try {
          await protectedBackend.write('test.txt', textToBytes('data'))
        } catch {
          // Expected
        }
      }

      // Write should still fail (no fallback for writes)
      await expect(protectedBackend.write('test.txt', textToBytes('data'))).rejects.toThrow(CircuitOpenError)
    })
  })

  // ===========================================================================
  // Lightweight Operations Bypass
  // ===========================================================================

  describe('lightweight operations bypass', () => {
    it('should bypass circuit for exists by default', async () => {
      const failing = createFailingBackend(['read'])
      const protectedBackend = new CircuitBreakerBackend(failing, {
        failureThreshold: 2,
        resetTimeoutMs: 100,
        bypassLightweightOps: true,
      })

      // Open the read circuit
      for (let i = 0; i < 2; i++) {
        try {
          await protectedBackend.read('test.txt')
        } catch {
          // Expected
        }
      }

      // exists should still work (bypasses circuit)
      const exists = await protectedBackend.exists('test.txt')
      expect(exists).toBe(false) // File doesn't exist in failing backend
    })

    it('should bypass circuit for stat by default', async () => {
      const failing = createFailingBackend(['read'])
      const protectedBackend = new CircuitBreakerBackend(failing, {
        failureThreshold: 2,
        resetTimeoutMs: 100,
        bypassLightweightOps: true,
      })

      // Open the read circuit
      for (let i = 0; i < 2; i++) {
        try {
          await protectedBackend.read('test.txt')
        } catch {
          // Expected
        }
      }

      // stat should still work (bypasses circuit)
      const stat = await protectedBackend.stat('test.txt')
      expect(stat).toBeNull()
    })

    it('should not bypass when bypassLightweightOps is false', async () => {
      const failing = createFailingBackend(['read'])
      failing.exists = async () => {
        throw new Error('exists failed')
      }

      const protectedBackend = new CircuitBreakerBackend(failing, {
        failureThreshold: 2,
        resetTimeoutMs: 100,
        bypassLightweightOps: false,
      })

      // Open the read circuit
      for (let i = 0; i < 2; i++) {
        try {
          await protectedBackend.exists('test.txt')
        } catch {
          // Expected
        }
      }

      // exists should fail fast (circuit is open)
      await expect(protectedBackend.exists('test.txt')).rejects.toThrow(CircuitOpenError)
    })
  })

  // ===========================================================================
  // All Operations
  // ===========================================================================

  describe('all write operations', () => {
    it('should protect writeAtomic', async () => {
      const failing = createFailingBackend(['write'])
      failing.writeAtomic = async () => {
        throw new Error('writeAtomic failed')
      }

      const protectedBackend = new CircuitBreakerBackend(failing, {
        failureThreshold: 2,
        resetTimeoutMs: 100,
      })

      for (let i = 0; i < 2; i++) {
        try {
          await protectedBackend.writeAtomic('test.txt', textToBytes('data'))
        } catch {
          // Expected
        }
      }

      await expect(protectedBackend.writeAtomic('test.txt', textToBytes('data'))).rejects.toThrow(CircuitOpenError)
    })

    it('should protect append', async () => {
      const failing = createFailingBackend(['write'])
      failing.append = async () => {
        throw new Error('append failed')
      }

      const protectedBackend = new CircuitBreakerBackend(failing, {
        failureThreshold: 2,
        resetTimeoutMs: 100,
      })

      for (let i = 0; i < 2; i++) {
        try {
          await protectedBackend.append('test.txt', textToBytes('data'))
        } catch {
          // Expected
        }
      }

      await expect(protectedBackend.append('test.txt', textToBytes('data'))).rejects.toThrow(CircuitOpenError)
    })

    it('should protect deletePrefix', async () => {
      const failing = createFailingBackend(['delete'])
      failing.deletePrefix = async () => {
        throw new Error('deletePrefix failed')
      }

      const protectedBackend = new CircuitBreakerBackend(failing, {
        failureThreshold: 2,
        resetTimeoutMs: 100,
      })

      for (let i = 0; i < 2; i++) {
        try {
          await protectedBackend.deletePrefix('prefix/')
        } catch {
          // Expected
        }
      }

      await expect(protectedBackend.deletePrefix('prefix/')).rejects.toThrow(CircuitOpenError)
    })

    it('should protect writeConditional', async () => {
      const failing = createFailingBackend(['write'])
      failing.writeConditional = async () => {
        throw new Error('writeConditional failed')
      }

      const protectedBackend = new CircuitBreakerBackend(failing, {
        failureThreshold: 2,
        resetTimeoutMs: 100,
      })

      for (let i = 0; i < 2; i++) {
        try {
          await protectedBackend.writeConditional('test.txt', textToBytes('data'), null)
        } catch {
          // Expected
        }
      }

      await expect(protectedBackend.writeConditional('test.txt', textToBytes('data'), null)).rejects.toThrow(CircuitOpenError)
    })

    it('should protect copy', async () => {
      const failing = createFailingBackend(['write'])
      failing.copy = async () => {
        throw new Error('copy failed')
      }

      const protectedBackend = new CircuitBreakerBackend(failing, {
        failureThreshold: 2,
        resetTimeoutMs: 100,
      })

      for (let i = 0; i < 2; i++) {
        try {
          await protectedBackend.copy('src.txt', 'dst.txt')
        } catch {
          // Expected
        }
      }

      await expect(protectedBackend.copy('src.txt', 'dst.txt')).rejects.toThrow(CircuitOpenError)
    })

    it('should protect move', async () => {
      const failing = createFailingBackend(['write'])
      failing.move = async () => {
        throw new Error('move failed')
      }

      const protectedBackend = new CircuitBreakerBackend(failing, {
        failureThreshold: 2,
        resetTimeoutMs: 100,
      })

      for (let i = 0; i < 2; i++) {
        try {
          await protectedBackend.move('src.txt', 'dst.txt')
        } catch {
          // Expected
        }
      }

      await expect(protectedBackend.move('src.txt', 'dst.txt')).rejects.toThrow(CircuitOpenError)
    })

    it('should protect rmdir', async () => {
      const failing = createFailingBackend(['delete'])
      failing.rmdir = async () => {
        throw new Error('rmdir failed')
      }

      const protectedBackend = new CircuitBreakerBackend(failing, {
        failureThreshold: 2,
        resetTimeoutMs: 100,
      })

      for (let i = 0; i < 2; i++) {
        try {
          await protectedBackend.rmdir('dir')
        } catch {
          // Expected
        }
      }

      await expect(protectedBackend.rmdir('dir')).rejects.toThrow(CircuitOpenError)
    })
  })

  // ===========================================================================
  // Metrics and Reset
  // ===========================================================================

  describe('metrics and reset', () => {
    it('should provide metrics for both circuits', async () => {
      await wrapped.write('test.txt', textToBytes('data'))
      await wrapped.read('test.txt')

      const metrics = wrapped.getMetrics()

      expect(metrics.read.totalRequests).toBe(1)
      expect(metrics.read.totalSuccesses).toBe(1)
      expect(metrics.write.totalRequests).toBe(1)
      expect(metrics.write.totalSuccesses).toBe(1)
      expect(metrics.backendType).toBe('memory')
      expect(metrics.usingFallback).toBe(false)
    })

    it('should reset all circuits', async () => {
      const failing = createFailingBackend(['all'])
      const protectedBackend = new CircuitBreakerBackend(failing, {
        failureThreshold: 2,
        resetTimeoutMs: 100,
      })

      // Open both circuits
      for (let i = 0; i < 2; i++) {
        try {
          await protectedBackend.read('test.txt')
        } catch {
          // Expected
        }
        try {
          await protectedBackend.write('test.txt', textToBytes('data'))
        } catch {
          // Expected
        }
      }

      const beforeReset = protectedBackend.getMetrics()
      expect(beforeReset.read.state).toBe(CircuitState.OPEN)
      expect(beforeReset.write.state).toBe(CircuitState.OPEN)

      protectedBackend.reset()

      const afterReset = protectedBackend.getMetrics()
      expect(afterReset.read.state).toBe(CircuitState.CLOSED)
      expect(afterReset.write.state).toBe(CircuitState.CLOSED)
    })
  })

  // ===========================================================================
  // withCircuitBreaker Helper
  // ===========================================================================

  describe('withCircuitBreaker()', () => {
    it('should wrap a backend in CircuitBreakerBackend', () => {
      const backend = new MemoryBackend()
      const wrapped = withCircuitBreaker(backend)

      expect(wrapped).toBeInstanceOf(CircuitBreakerBackend)
      expect(wrapped.inner).toBe(backend)
    })

    it('should not double-wrap', () => {
      const backend = new MemoryBackend()
      const wrapped = withCircuitBreaker(backend)
      const doubleWrapped = withCircuitBreaker(wrapped)

      expect(doubleWrapped).toBe(wrapped)
    })

    it('should accept custom options', async () => {
      const failing = createFailingBackend(['read'])
      const wrapped = withCircuitBreaker(failing, {
        failureThreshold: 1,
        resetTimeoutMs: 50,
      })

      // Single failure should open circuit
      try {
        await wrapped.read('test.txt')
      } catch {
        // Expected
      }

      await expect(wrapped.read('test.txt')).rejects.toThrow(CircuitOpenError)
    })
  })

  // ===========================================================================
  // Recovery
  // ===========================================================================

  describe('recovery', () => {
    it('should recover after timeout and successful operations', async () => {
      const callCount = { read: 0 }
      const sometimesFailingBackend = new MemoryBackend()
      await sometimesFailingBackend.write('test.txt', textToBytes('data'))

      const originalRead = sometimesFailingBackend.read.bind(sometimesFailingBackend)
      sometimesFailingBackend.read = async (path: string) => {
        callCount.read++
        // Fail first 3 calls
        if (callCount.read <= 3) {
          throw new Error('Read failed')
        }
        return originalRead(path)
      }

      const protectedBackend = new CircuitBreakerBackend(sometimesFailingBackend, {
        failureThreshold: 3,
        successThreshold: 2,
        resetTimeoutMs: 50,
      })

      // Trigger 3 failures to open circuit
      for (let i = 0; i < 3; i++) {
        try {
          await protectedBackend.read('test.txt')
        } catch {
          // Expected
        }
      }

      expect(protectedBackend.getMetrics().read.state).toBe(CircuitState.OPEN)

      // Wait for recovery timeout using fake timers
      await advanceTime(60)

      // Circuit should now allow test requests (HALF_OPEN)
      // Next reads will succeed (callCount > 3)
      const data1 = await protectedBackend.read('test.txt')
      expect(bytesToText(data1)).toBe('data')

      const data2 = await protectedBackend.read('test.txt')
      expect(bytesToText(data2)).toBe('data')

      // Circuit should be closed now
      expect(protectedBackend.getMetrics().read.state).toBe(CircuitState.CLOSED)
    })
  })
})
