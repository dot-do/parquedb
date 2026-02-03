/**
 * BackpressureManager Tests
 *
 * Tests for rate limiting, circuit breaker, and backpressure
 * mechanisms in the compaction queue consumer.
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import {
  BackpressureManager,
  createDefaultBackpressureManager,
  createTestBackpressureManager,
  createConservativeBackpressureManager,
  type BackpressureConfig,
} from '../../../src/workflows/backpressure'

// =============================================================================
// Token Bucket Rate Limiting Tests
// =============================================================================

describe('BackpressureManager - Token Bucket Rate Limiting', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('should allow dispatches within rate limit', () => {
    const manager = new BackpressureManager({
      dispatchesPerSecond: 10,
      rateLimitWindowMs: 1000,
    })

    // Should allow 10 dispatches
    for (let i = 0; i < 10; i++) {
      expect(manager.canDispatch()).toBe(true)
      expect(manager.isRateLimited()).toBe(false)
    }
  })

  it('should block dispatches when rate limit exceeded', async () => {
    const manager = new BackpressureManager({
      dispatchesPerSecond: 5,
      rateLimitWindowMs: 1000,
    })

    // Consume all tokens
    for (let i = 0; i < 5; i++) {
      await manager.executeWorkflowCreate(async () => ({ id: `workflow-${i}` }))
    }

    // Should be rate limited now
    expect(manager.isRateLimited()).toBe(true)
    expect(manager.canDispatch()).toBe(false)
  })

  it('should refill tokens after window expires', async () => {
    const manager = new BackpressureManager({
      dispatchesPerSecond: 5,
      rateLimitWindowMs: 1000,
    })

    // Consume all tokens
    for (let i = 0; i < 5; i++) {
      await manager.executeWorkflowCreate(async () => ({ id: `workflow-${i}` }))
    }

    expect(manager.isRateLimited()).toBe(true)

    // Advance time past the window
    vi.advanceTimersByTime(1100)

    // Tokens should be refilled
    expect(manager.isRateLimited()).toBe(false)
    expect(manager.canDispatch()).toBe(true)
  })

  it('should use sliding window for rate limiting', async () => {
    const manager = new BackpressureManager({
      dispatchesPerSecond: 10,
      rateLimitWindowMs: 1000,
    })

    // Consume 5 tokens at t=0
    for (let i = 0; i < 5; i++) {
      await manager.executeWorkflowCreate(async () => ({ id: `workflow-${i}` }))
    }

    // Advance 500ms
    vi.advanceTimersByTime(500)

    // Consume 5 more tokens at t=500
    for (let i = 0; i < 5; i++) {
      await manager.executeWorkflowCreate(async () => ({ id: `workflow-${i}` }))
    }

    // Should be rate limited (10 tokens used in last 1000ms)
    expect(manager.isRateLimited()).toBe(true)

    // Advance 600ms (to t=1100) - first 5 tokens expire
    vi.advanceTimersByTime(600)

    // Now only 5 tokens are in window, should have 5 available
    expect(manager.isRateLimited()).toBe(false)
    const status = manager.getStatus()
    expect(status.tokensRemaining).toBe(5)
  })

  it('should return rate_limited reason when skipped', async () => {
    const manager = new BackpressureManager({
      dispatchesPerSecond: 2,
      rateLimitWindowMs: 1000,
    })

    // Consume all tokens
    await manager.executeWorkflowCreate(async () => ({ id: 'w1' }))
    await manager.executeWorkflowCreate(async () => ({ id: 'w2' }))

    // Next should be skipped
    const result = await manager.executeWorkflowCreate(async () => ({ id: 'w3' }))

    expect(result.success).toBe(false)
    expect(result.skipped).toBe(true)
    expect(result.reason).toBe('rate_limited')
  })

  it('should calculate delay until next token', async () => {
    const manager = new BackpressureManager({
      dispatchesPerSecond: 2,
      rateLimitWindowMs: 1000,
    })

    // Consume all tokens
    await manager.executeWorkflowCreate(async () => ({ id: 'w1' }))
    await manager.executeWorkflowCreate(async () => ({ id: 'w2' }))

    const delay = manager.getDelayUntilToken()
    expect(delay).toBeGreaterThan(0)
    expect(delay).toBeLessThanOrEqual(1000)
  })
})

// =============================================================================
// Circuit Breaker Tests
// =============================================================================

describe('BackpressureManager - Circuit Breaker', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('should open circuit after consecutive failures', async () => {
    const manager = new BackpressureManager({
      dispatchesPerSecond: 100,
      circuitBreakerThreshold: 3,
      circuitBreakerBaseDelayMs: 1000,
    })

    // Record 3 failures
    for (let i = 0; i < 3; i++) {
      await manager.executeWorkflowCreate(async () => {
        throw new Error('Workflow creation failed')
      })
    }

    expect(manager.isCircuitOpen()).toBe(true)
  })

  it('should reject requests when circuit is open', async () => {
    const manager = new BackpressureManager({
      dispatchesPerSecond: 100,
      circuitBreakerThreshold: 2,
      circuitBreakerBaseDelayMs: 5000,
    })

    // Open the circuit
    for (let i = 0; i < 2; i++) {
      await manager.executeWorkflowCreate(async () => {
        throw new Error('Fail')
      })
    }

    // Should be rejected with circuit_open reason
    const result = await manager.executeWorkflowCreate(async () => ({ id: 'test' }))

    expect(result.success).toBe(false)
    expect(result.skipped).toBe(true)
    expect(result.reason).toBe('circuit_open')
  })

  it('should close circuit after successful request in half-open state', async () => {
    const manager = new BackpressureManager({
      dispatchesPerSecond: 100,
      circuitBreakerThreshold: 2,
      circuitBreakerBaseDelayMs: 1000,
    })

    // Open the circuit
    for (let i = 0; i < 2; i++) {
      await manager.executeWorkflowCreate(async () => {
        throw new Error('Fail')
      })
    }

    expect(manager.isCircuitOpen()).toBe(true)

    // Wait for circuit to transition to half-open (backoff multiplier starts at 1, doubles on each failure cycle)
    // After first failure cycle, delay = 1000 * 2 = 2000ms
    vi.advanceTimersByTime(2100)

    // Circuit should allow a test request (half-open state)
    expect(manager.isCircuitOpen()).toBe(false)

    // Successful request should close the circuit
    const result = await manager.executeWorkflowCreate(async () => ({ id: 'success' }))

    expect(result.success).toBe(true)
    expect(manager.isCircuitOpen()).toBe(false)

    // Verify circuit is fully closed by making more requests
    for (let i = 0; i < 5; i++) {
      const r = await manager.executeWorkflowCreate(async () => ({ id: `w${i}` }))
      expect(r.success).toBe(true)
    }
  })

  it('should reopen circuit on failure in half-open state', async () => {
    const manager = new BackpressureManager({
      dispatchesPerSecond: 100,
      circuitBreakerThreshold: 2,
      circuitBreakerBaseDelayMs: 1000,
    })

    // Open the circuit
    for (let i = 0; i < 2; i++) {
      await manager.executeWorkflowCreate(async () => {
        throw new Error('Fail')
      })
    }

    // Wait for half-open
    vi.advanceTimersByTime(1100)

    // Fail in half-open state
    await manager.executeWorkflowCreate(async () => {
      throw new Error('Still failing')
    })

    // Circuit should be open again
    expect(manager.isCircuitOpen()).toBe(true)
  })

  it('should use exponential backoff for repeated failures', async () => {
    const manager = new BackpressureManager({
      dispatchesPerSecond: 100,
      circuitBreakerThreshold: 2,
      circuitBreakerBaseDelayMs: 1000,
      circuitBreakerMaxDelayMs: 10000,
    })

    // First failure cycle - backoff multiplier goes to 2
    for (let i = 0; i < 2; i++) {
      await manager.executeWorkflowCreate(async () => { throw new Error('Fail') })
    }

    const firstResetTime = manager.getCircuitResetTime()
    // First reset: baseDelay (1000) * multiplier (2) = 2000
    expect(firstResetTime).toBeLessThanOrEqual(2000)
    expect(firstResetTime).toBeGreaterThan(0)

    // Wait for half-open
    vi.advanceTimersByTime(2100)

    // Fail again in half-open - backoff multiplier doubles to 4
    await manager.executeWorkflowCreate(async () => { throw new Error('Fail again') })

    // Backoff should be doubled: baseDelay (1000) * multiplier (4) = 4000
    const secondResetTime = manager.getCircuitResetTime()
    expect(secondResetTime).toBeLessThanOrEqual(4000)
    expect(secondResetTime).toBeGreaterThan(2000)
  })

  it('should respect max delay for exponential backoff', async () => {
    const manager = new BackpressureManager({
      dispatchesPerSecond: 100,
      circuitBreakerThreshold: 1,
      circuitBreakerBaseDelayMs: 1000,
      circuitBreakerMaxDelayMs: 3000,
    })

    // Multiple failure cycles
    for (let cycle = 0; cycle < 5; cycle++) {
      await manager.executeWorkflowCreate(async () => { throw new Error('Fail') })
      vi.advanceTimersByTime(5000)
    }

    // Reset time should not exceed max
    const resetTime = manager.getCircuitResetTime()
    expect(resetTime).toBeLessThanOrEqual(3000)
  })

  it('should reset consecutive failures on success', async () => {
    const manager = new BackpressureManager({
      dispatchesPerSecond: 100,
      circuitBreakerThreshold: 3,
    })

    // 2 failures (not enough to open)
    await manager.executeWorkflowCreate(async () => { throw new Error('Fail') })
    await manager.executeWorkflowCreate(async () => { throw new Error('Fail') })

    // 1 success resets counter
    await manager.executeWorkflowCreate(async () => ({ id: 'success' }))

    // 2 more failures should not open circuit
    await manager.executeWorkflowCreate(async () => { throw new Error('Fail') })
    await manager.executeWorkflowCreate(async () => { throw new Error('Fail') })

    expect(manager.isCircuitOpen()).toBe(false)
  })
})

// =============================================================================
// Backpressure Signal Tests
// =============================================================================

describe('BackpressureManager - Backpressure Signal', () => {
  it('should activate backpressure when threshold exceeded', () => {
    const manager = new BackpressureManager({
      dispatchesPerSecond: 100,
      backpressureThreshold: 10,
    })

    expect(manager.isBackpressureActive()).toBe(false)

    manager.updateBackpressureFromStatus(15)

    expect(manager.isBackpressureActive()).toBe(true)
  })

  it('should deactivate backpressure when below threshold', () => {
    const manager = new BackpressureManager({
      dispatchesPerSecond: 100,
      backpressureThreshold: 10,
    })

    manager.updateBackpressureFromStatus(15)
    expect(manager.isBackpressureActive()).toBe(true)

    manager.updateBackpressureFromStatus(5)
    expect(manager.isBackpressureActive()).toBe(false)
  })

  it('should block dispatches when backpressure active', async () => {
    const manager = new BackpressureManager({
      dispatchesPerSecond: 100,
      backpressureThreshold: 10,
    })

    manager.setBackpressureActive(true)

    expect(manager.canDispatch()).toBe(false)

    const result = await manager.executeWorkflowCreate(async () => ({ id: 'test' }))

    expect(result.success).toBe(false)
    expect(result.skipped).toBe(true)
    expect(result.reason).toBe('backpressure')
  })

  it('should allow high-priority namespaces during backpressure', async () => {
    const manager = new BackpressureManager({
      dispatchesPerSecond: 100,
      backpressureThreshold: 10,
      highPriorityNamespaces: ['critical', 'audit'],
    })

    manager.setBackpressureActive(true)

    // Regular namespace should be blocked
    expect(manager.canDispatch('users')).toBe(false)

    // High-priority namespace should be allowed
    expect(manager.canDispatch('critical')).toBe(true)
    expect(manager.canDispatch('audit')).toBe(true)

    const result = await manager.executeWorkflowCreate(
      async () => ({ id: 'critical-workflow' }),
      'critical'
    )

    expect(result.success).toBe(true)
  })
})

// =============================================================================
// Adaptive Batch Sizing Tests
// =============================================================================

describe('BackpressureManager - Adaptive Batch Sizing', () => {
  it('should return all namespaces when not under backpressure', () => {
    const manager = new BackpressureManager({
      dispatchesPerSecond: 100,
      backpressureThreshold: 50,
      highPriorityNamespaces: ['critical'],
    })

    const namespaces = ['users', 'posts', 'critical', 'comments']
    const filtered = manager.filterNamespacesForProcessing(namespaces)

    expect(filtered).toEqual(namespaces)
  })

  it('should filter to high-priority only under backpressure', () => {
    const manager = new BackpressureManager({
      dispatchesPerSecond: 100,
      backpressureThreshold: 50,
      highPriorityNamespaces: ['critical', 'audit'],
    })

    manager.setBackpressureActive(true)

    const namespaces = ['users', 'posts', 'critical', 'comments', 'audit']
    const filtered = manager.filterNamespacesForProcessing(namespaces)

    expect(filtered).toEqual(['critical', 'audit'])
  })

  it('should return empty array if no high-priority under backpressure', () => {
    const manager = new BackpressureManager({
      dispatchesPerSecond: 100,
      backpressureThreshold: 50,
      highPriorityNamespaces: ['critical'],
    })

    manager.setBackpressureActive(true)

    const namespaces = ['users', 'posts', 'comments']
    const filtered = manager.filterNamespacesForProcessing(namespaces)

    expect(filtered).toEqual([])
  })

  it('should correctly identify high-priority namespaces', () => {
    const manager = new BackpressureManager({
      highPriorityNamespaces: ['critical', 'audit', 'billing'],
    })

    expect(manager.isHighPriority('critical')).toBe(true)
    expect(manager.isHighPriority('audit')).toBe(true)
    expect(manager.isHighPriority('billing')).toBe(true)
    expect(manager.isHighPriority('users')).toBe(false)
    expect(manager.isHighPriority('posts')).toBe(false)
  })
})

// =============================================================================
// Status Reporting Tests
// =============================================================================

describe('BackpressureManager - Status Reporting', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('should report accurate status', async () => {
    const manager = new BackpressureManager({
      dispatchesPerSecond: 10,
      rateLimitWindowMs: 1000,
      backpressureThreshold: 50,
      circuitBreakerThreshold: 5,
    })

    // Consume some tokens
    for (let i = 0; i < 3; i++) {
      await manager.executeWorkflowCreate(async () => ({ id: `w${i}` }))
    }

    // Record some failures
    for (let i = 0; i < 2; i++) {
      await manager.executeWorkflowCreate(async () => { throw new Error('Fail') })
    }

    const status = manager.getStatus()

    expect(status.tokensRemaining).toBe(5) // 10 - 5 used
    expect(status.dispatchesInWindow).toBe(5)
    expect(status.consecutiveFailures).toBe(2)
    expect(status.circuitBreakerOpen).toBe(false)
    expect(status.backpressureSignalActive).toBe(false)
  })

  it('should show circuit breaker status', async () => {
    const manager = new BackpressureManager({
      dispatchesPerSecond: 100,
      circuitBreakerThreshold: 2,
      circuitBreakerBaseDelayMs: 5000,
    })

    // Open circuit - backoff multiplier goes to 2
    for (let i = 0; i < 2; i++) {
      await manager.executeWorkflowCreate(async () => { throw new Error('Fail') })
    }

    const status = manager.getStatus()

    expect(status.circuitBreakerOpen).toBe(true)
    expect(status.circuitBreakerResetMs).toBeGreaterThan(0)
    // Reset time = baseDelay (5000) * multiplier (2) = 10000
    expect(status.circuitBreakerResetMs).toBeLessThanOrEqual(10000)
  })

  it('should show backpressure status', () => {
    const manager = new BackpressureManager({
      backpressureThreshold: 10,
    })

    manager.updateBackpressureFromStatus(15)

    const status = manager.getStatus()
    expect(status.backpressureSignalActive).toBe(true)
  })
})

// =============================================================================
// Reset and Factory Tests
// =============================================================================

describe('BackpressureManager - Reset and Factory', () => {
  it('should reset all state', async () => {
    const manager = new BackpressureManager({
      dispatchesPerSecond: 5,
      circuitBreakerThreshold: 2,
    })

    // Consume tokens
    for (let i = 0; i < 5; i++) {
      await manager.executeWorkflowCreate(async () => ({ id: `w${i}` }))
    }

    // Create failures
    for (let i = 0; i < 2; i++) {
      await manager.executeWorkflowCreate(async () => { throw new Error('Fail') })
    }

    // Set backpressure
    manager.setBackpressureActive(true)

    // Reset
    manager.reset()

    const status = manager.getStatus()
    expect(status.tokensRemaining).toBe(5)
    expect(status.consecutiveFailures).toBe(0)
    expect(status.circuitBreakerOpen).toBe(false)
    expect(status.backpressureSignalActive).toBe(false)
    expect(status.dispatchesInWindow).toBe(0)
  })

  it('should create default manager with correct settings', () => {
    const manager = createDefaultBackpressureManager()
    const status = manager.getStatus()

    expect(status.tokensRemaining).toBe(100) // default dispatchesPerSecond
  })

  it('should create test manager with aggressive settings', () => {
    const manager = createTestBackpressureManager()
    const status = manager.getStatus()

    expect(status.tokensRemaining).toBe(10) // lower for testing
  })

  it('should create conservative manager for high-load', () => {
    const manager = createConservativeBackpressureManager()
    const status = manager.getStatus()

    expect(status.tokensRemaining).toBe(50) // lower for conservative
  })
})

// =============================================================================
// Integration Tests
// =============================================================================

describe('BackpressureManager - Integration', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('should handle mixed success and failure workflow', async () => {
    const manager = new BackpressureManager({
      dispatchesPerSecond: 100,
      circuitBreakerThreshold: 3,
      circuitBreakerBaseDelayMs: 1000,
    })

    // Simulate realistic workflow pattern
    const results: Array<{ success: boolean; skipped?: boolean; reason?: string }> = []

    // 5 successful workflows
    for (let i = 0; i < 5; i++) {
      const result = await manager.executeWorkflowCreate(async () => ({ id: `success-${i}` }))
      results.push(result)
    }

    // 2 failures
    for (let i = 0; i < 2; i++) {
      const result = await manager.executeWorkflowCreate(async () => { throw new Error('Fail') })
      results.push(result)
    }

    // 1 success (resets consecutive failures)
    const resetResult = await manager.executeWorkflowCreate(async () => ({ id: 'reset' }))
    results.push(resetResult)

    // Circuit should still be closed
    expect(manager.isCircuitOpen()).toBe(false)

    // Count successes and failures
    const successes = results.filter(r => r.success).length
    const failures = results.filter(r => !r.success && !r.skipped).length

    expect(successes).toBe(6)
    expect(failures).toBe(2)
  })

  it('should handle extreme load with all protections', async () => {
    const manager = new BackpressureManager({
      dispatchesPerSecond: 10,
      rateLimitWindowMs: 1000,
      backpressureThreshold: 5,
      circuitBreakerThreshold: 3,
      circuitBreakerBaseDelayMs: 1000,
    })

    const results = {
      success: 0,
      rateLimited: 0,
      circuitOpen: 0,
      backpressure: 0,
      error: 0,
    }

    // Simulate 50 rapid workflow attempts
    for (let i = 0; i < 50; i++) {
      const result = await manager.executeWorkflowCreate(async () => {
        // 30% failure rate
        if (Math.random() < 0.3) {
          throw new Error('Random failure')
        }
        return { id: `workflow-${i}` }
      })

      if (result.success) {
        results.success++
      } else if (result.skipped) {
        if (result.reason === 'rate_limited') results.rateLimited++
        else if (result.reason === 'circuit_open') results.circuitOpen++
        else if (result.reason === 'backpressure') results.backpressure++
      } else {
        results.error++
      }
    }

    // Should have hit rate limits since we only allow 10/sec
    expect(results.rateLimited).toBeGreaterThan(0)

    // Total should add up
    const total = Object.values(results).reduce((a, b) => a + b, 0)
    expect(total).toBe(50)
  })

  it('should respect all guards in canDispatch', async () => {
    const manager = new BackpressureManager({
      dispatchesPerSecond: 5,
      backpressureThreshold: 5,
      circuitBreakerThreshold: 2,
      highPriorityNamespaces: ['critical'],
    })

    // Initially all should pass for critical namespace
    expect(manager.canDispatch('critical')).toBe(true)
    expect(manager.canDispatch('users')).toBe(true)

    // Activate backpressure - only critical should pass
    manager.setBackpressureActive(true)
    expect(manager.canDispatch('critical')).toBe(true)
    expect(manager.canDispatch('users')).toBe(false)

    // Open circuit - nothing should pass
    for (let i = 0; i < 2; i++) {
      await manager.executeWorkflowCreate(async () => { throw new Error('Fail') }, 'critical')
    }
    expect(manager.canDispatch('critical')).toBe(false)
    expect(manager.canDispatch('users')).toBe(false)
  })
})
