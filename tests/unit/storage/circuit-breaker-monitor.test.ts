/**
 * CircuitBreakerMonitor Tests
 *
 * Tests for the centralized circuit breaker monitoring system.
 * Verifies health status aggregation, event tracking, and notifications.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  CircuitBreakerMonitor,
  createCircuitBreakerMonitor,
  globalCircuitBreakerMonitor,
} from '../../../src/storage/CircuitBreakerMonitor'
import { CircuitBreakerBackend } from '../../../src/storage/CircuitBreakerBackend'
import { CircuitState } from '../../../src/storage/CircuitBreaker'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'

// =============================================================================
// Helpers
// =============================================================================

function textToBytes(text: string): Uint8Array {
  return new TextEncoder().encode(text)
}

function createFailingBackend(): MemoryBackend {
  const backend = new MemoryBackend()
  backend.read = async () => {
    throw new Error('Read failed')
  }
  backend.write = async () => {
    throw new Error('Write failed')
  }
  return backend
}

async function triggerCircuitOpen(backend: CircuitBreakerBackend, failures: number = 3): Promise<void> {
  for (let i = 0; i < failures; i++) {
    try {
      await backend.read('test.txt')
    } catch {
      // Expected
    }
  }
}

// =============================================================================
// Test Suite
// =============================================================================

describe('CircuitBreakerMonitor', () => {
  let monitor: CircuitBreakerMonitor

  beforeEach(() => {
    monitor = new CircuitBreakerMonitor()
  })

  // ===========================================================================
  // Registration
  // ===========================================================================

  describe('backend registration', () => {
    it('should register a backend', () => {
      const backend = new CircuitBreakerBackend(new MemoryBackend())
      monitor.register('test', backend)

      expect(monitor.getRegisteredBackends()).toContain('test')
    })

    it('should unregister a backend', () => {
      const backend = new CircuitBreakerBackend(new MemoryBackend())
      monitor.register('test', backend)
      monitor.unregister('test')

      expect(monitor.getRegisteredBackends()).not.toContain('test')
    })

    it('should list all registered backends', () => {
      monitor.register('r2', new CircuitBreakerBackend(new MemoryBackend()))
      monitor.register('s3', new CircuitBreakerBackend(new MemoryBackend()))
      monitor.register('remote', new CircuitBreakerBackend(new MemoryBackend()))

      const backends = monitor.getRegisteredBackends()
      expect(backends).toHaveLength(3)
      expect(backends).toContain('r2')
      expect(backends).toContain('s3')
      expect(backends).toContain('remote')
    })
  })

  // ===========================================================================
  // Health Status
  // ===========================================================================

  describe('health status', () => {
    it('should report healthy when all circuits are closed', () => {
      const backend = new CircuitBreakerBackend(new MemoryBackend())
      monitor.register('test', backend)

      const status = monitor.getHealthStatus()

      expect(status.healthy).toBe(true)
      expect(status.openCount).toBe(0)
      expect(status.closedCount).toBeGreaterThan(0)
    })

    it('should report unhealthy when a circuit is open', async () => {
      const failing = createFailingBackend()
      const backend = new CircuitBreakerBackend(failing, {
        failureThreshold: 3,
        resetTimeoutMs: 1000,
      })
      monitor.register('test', backend)

      await triggerCircuitOpen(backend)

      const status = monitor.getHealthStatus()

      expect(status.healthy).toBe(false)
      expect(status.openCount).toBeGreaterThan(0)
    })

    it('should track individual circuit health', async () => {
      const backend = new CircuitBreakerBackend(new MemoryBackend())
      monitor.register('primary', backend)

      const status = monitor.getHealthStatus()
      const readHealth = status.circuits['primary:read']

      expect(readHealth).toBeDefined()
      expect(readHealth.state).toBe(CircuitState.CLOSED)
      expect(readHealth.isHealthy).toBe(true)
    })

    it('should calculate success rate', async () => {
      const memory = new MemoryBackend()
      await memory.write('test.txt', textToBytes('data'))

      const backend = new CircuitBreakerBackend(memory, {
        failureThreshold: 10,
      })
      monitor.register('test', backend)

      // Perform some successful reads
      await backend.read('test.txt')
      await backend.read('test.txt')
      await backend.read('test.txt')

      const status = monitor.getHealthStatus()
      const health = status.circuits['test:read']

      expect(health.totalRequests).toBe(3)
      expect(health.successRate).toBe(1)
    })

    it('should check for open circuits', async () => {
      const backend = new CircuitBreakerBackend(createFailingBackend(), {
        failureThreshold: 2,
      })
      monitor.register('test', backend)

      expect(monitor.hasOpenCircuits()).toBe(false)

      await triggerCircuitOpen(backend, 2)

      expect(monitor.hasOpenCircuits()).toBe(true)
    })
  })

  // ===========================================================================
  // State Change Tracking
  // ===========================================================================

  describe('state change tracking', () => {
    it('should track state changes via handler', async () => {
      const failing = createFailingBackend()
      const handler = monitor.createStateChangeHandler('test')

      const backend = new CircuitBreakerBackend(failing, {
        failureThreshold: 2,
        onStateChange: handler,
      })
      monitor.register('test', backend)

      await triggerCircuitOpen(backend, 2)

      const events = monitor.getRecentEvents()
      expect(events.length).toBeGreaterThan(0)
      expect(events[events.length - 1].toState).toBe(CircuitState.OPEN)
    })

    it('should include reason in state change events', async () => {
      const handler = monitor.createStateChangeHandler('test')

      // Simulate state change
      handler(CircuitState.CLOSED, CircuitState.OPEN, 'read')

      const events = monitor.getRecentEvents()
      expect(events[0].reason).toBe('Failure threshold exceeded')
    })

    it('should limit history size', () => {
      const smallMonitor = new CircuitBreakerMonitor({ maxHistorySize: 5 })
      const handler = smallMonitor.createStateChangeHandler('test')

      // Add more events than the limit
      for (let i = 0; i < 10; i++) {
        handler(CircuitState.CLOSED, CircuitState.OPEN, `circuit-${i}`)
      }

      const events = smallMonitor.getRecentEvents()
      expect(events).toHaveLength(5)
    })

    it('should filter events by backend name', () => {
      const r2Handler = monitor.createStateChangeHandler('r2')
      const s3Handler = monitor.createStateChangeHandler('s3')

      r2Handler(CircuitState.CLOSED, CircuitState.OPEN, 'read')
      s3Handler(CircuitState.CLOSED, CircuitState.OPEN, 'read')
      r2Handler(CircuitState.OPEN, CircuitState.HALF_OPEN, 'read')

      const r2Events = monitor.getBackendEvents('r2')
      expect(r2Events).toHaveLength(2)
      expect(r2Events.every(e => e.name.startsWith('r2'))).toBe(true)
    })

    it('should clear history', () => {
      const handler = monitor.createStateChangeHandler('test')
      handler(CircuitState.CLOSED, CircuitState.OPEN, 'read')

      expect(monitor.getRecentEvents()).toHaveLength(1)

      monitor.clearHistory()

      expect(monitor.getRecentEvents()).toHaveLength(0)
    })
  })

  // ===========================================================================
  // State Change Notifications
  // ===========================================================================

  describe('state change notifications', () => {
    it('should notify subscribers of state changes', async () => {
      const callback = vi.fn()
      monitor.onStateChange(callback)

      const handler = monitor.createStateChangeHandler('test')
      handler(CircuitState.CLOSED, CircuitState.OPEN, 'read')

      expect(callback).toHaveBeenCalledTimes(1)
      expect(callback).toHaveBeenCalledWith(expect.objectContaining({
        name: 'test:read',
        fromState: CircuitState.CLOSED,
        toState: CircuitState.OPEN,
      }))
    })

    it('should allow unsubscribing', () => {
      const callback = vi.fn()
      const unsubscribe = monitor.onStateChange(callback)

      const handler = monitor.createStateChangeHandler('test')
      handler(CircuitState.CLOSED, CircuitState.OPEN, 'read')

      expect(callback).toHaveBeenCalledTimes(1)

      unsubscribe()
      handler(CircuitState.OPEN, CircuitState.HALF_OPEN, 'read')

      expect(callback).toHaveBeenCalledTimes(1) // Still 1
    })

    it('should not throw if callback throws', () => {
      const badCallback = vi.fn(() => {
        throw new Error('Callback error')
      })
      monitor.onStateChange(badCallback)

      const handler = monitor.createStateChangeHandler('test')

      // Should not throw
      expect(() => {
        handler(CircuitState.CLOSED, CircuitState.OPEN, 'read')
      }).not.toThrow()
    })
  })

  // ===========================================================================
  // Bulk Operations
  // ===========================================================================

  describe('bulk operations', () => {
    it('should reset all circuit breakers', async () => {
      const backend1 = new CircuitBreakerBackend(createFailingBackend(), { failureThreshold: 2 })
      const backend2 = new CircuitBreakerBackend(createFailingBackend(), { failureThreshold: 2 })

      monitor.register('backend1', backend1)
      monitor.register('backend2', backend2)

      await triggerCircuitOpen(backend1, 2)
      await triggerCircuitOpen(backend2, 2)

      expect(monitor.hasOpenCircuits()).toBe(true)

      monitor.resetAll()

      expect(monitor.hasOpenCircuits()).toBe(false)
    })

    it('should aggregate metrics across backends', async () => {
      const memory1 = new MemoryBackend()
      const memory2 = new MemoryBackend()
      await memory1.write('test.txt', textToBytes('data'))
      await memory2.write('test.txt', textToBytes('data'))

      const backend1 = new CircuitBreakerBackend(memory1)
      const backend2 = new CircuitBreakerBackend(memory2)

      monitor.register('backend1', backend1)
      monitor.register('backend2', backend2)

      // Perform operations on both
      await backend1.read('test.txt')
      await backend1.read('test.txt')
      await backend2.read('test.txt')

      const metrics = monitor.getAggregatedMetrics()

      expect(metrics.totalRequests).toBe(3)
      expect(metrics.totalSuccesses).toBe(3)
      expect(metrics.overallSuccessRate).toBe(1)
    })
  })

  // ===========================================================================
  // Factory and Global Instance
  // ===========================================================================

  describe('factory and global instance', () => {
    it('should create monitor with factory function', () => {
      const monitor = createCircuitBreakerMonitor({ maxHistorySize: 50 })
      expect(monitor).toBeInstanceOf(CircuitBreakerMonitor)
    })

    it('should have global instance available', () => {
      expect(globalCircuitBreakerMonitor).toBeInstanceOf(CircuitBreakerMonitor)
    })
  })

  // ===========================================================================
  // Backend Health
  // ===========================================================================

  describe('getBackendHealth', () => {
    it('should return null for unregistered backend', () => {
      const health = monitor.getBackendHealth('nonexistent')
      expect(health).toBeNull()
    })

    it('should return health for registered backend', () => {
      const backend = new CircuitBreakerBackend(new MemoryBackend())
      monitor.register('test', backend)

      const health = monitor.getBackendHealth('test')

      expect(health).not.toBeNull()
      expect(health!.name).toBe('test')
      expect(health!.state).toBe(CircuitState.CLOSED)
    })
  })
})

// =============================================================================
// Reason Determination
// =============================================================================

describe('State Change Reasons', () => {
  it('should identify failure threshold exceeded', () => {
    const monitor = new CircuitBreakerMonitor()
    const handler = monitor.createStateChangeHandler('test')

    handler(CircuitState.CLOSED, CircuitState.OPEN, 'read')

    const events = monitor.getRecentEvents()
    expect(events[0].reason).toBe('Failure threshold exceeded')
  })

  it('should identify recovery timeout elapsed', () => {
    const monitor = new CircuitBreakerMonitor()
    const handler = monitor.createStateChangeHandler('test')

    handler(CircuitState.OPEN, CircuitState.HALF_OPEN, 'read')

    const events = monitor.getRecentEvents()
    expect(events[0].reason).toBe('Recovery timeout elapsed')
  })

  it('should identify successful recovery', () => {
    const monitor = new CircuitBreakerMonitor()
    const handler = monitor.createStateChangeHandler('test')

    handler(CircuitState.HALF_OPEN, CircuitState.CLOSED, 'read')

    const events = monitor.getRecentEvents()
    expect(events[0].reason).toBe('Recovery successful')
  })

  it('should identify failed recovery', () => {
    const monitor = new CircuitBreakerMonitor()
    const handler = monitor.createStateChangeHandler('test')

    handler(CircuitState.HALF_OPEN, CircuitState.OPEN, 'read')

    const events = monitor.getRecentEvents()
    expect(events[0].reason).toBe('Recovery failed')
  })
})
