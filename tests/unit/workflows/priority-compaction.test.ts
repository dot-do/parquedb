/**
 * Priority-based Compaction Scheduling Tests
 *
 * Tests for priority-based compaction scheduling feature:
 * - Namespace priority configuration (P0-P3)
 * - Priority-aware scheduling (different maxWaitTimeMs per priority)
 * - Backpressure behavior (skip P3/P2 under load)
 * - Dashboard integration (queue depth by priority)
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'

import {
  TestableCompactionStateDO,
  MockDurableObjectState,
  createUpdateRequest,
  createUpdate,
  type StoredState,
  type WindowReadyEntry,
} from './__helpers__/testable-compaction-state-do'

// Priority levels
type NamespacePriority = 0 | 1 | 2 | 3

// Priority-specific max wait times (in milliseconds)
const PRIORITY_WAIT_TIMES: Record<NamespacePriority, number> = {
  0: 1 * 60 * 1000,    // P0 (critical): 1 minute
  1: 5 * 60 * 1000,    // P1 (high): 5 minutes
  2: 15 * 60 * 1000,   // P2 (medium): 15 minutes
  3: 60 * 60 * 1000,   // P3 (background): 1 hour
}

// Backpressure thresholds
const BACKPRESSURE_THRESHOLD = 10  // Windows pending before backpressure kicks in
const SEVERE_BACKPRESSURE_THRESHOLD = 20  // Windows pending before severe backpressure

// =============================================================================
// Namespace Priority Configuration Tests
// =============================================================================

describe('Priority-based Compaction - Configuration', () => {
  let state: MockDurableObjectState
  let compactionDO: TestableCompactionStateDO

  beforeEach(() => {
    state = new MockDurableObjectState()
    compactionDO = new TestableCompactionStateDO(state)
  })

  describe('namespace priority configuration', () => {
    it('should store namespace priority in state', async () => {
      const request = new Request('http://internal/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ priority: 0 }),
      })

      const response = await compactionDO.fetch(request)
      expect(response.status).toBe(200)

      // Verify priority is stored
      const statusResponse = await compactionDO.fetch(new Request('http://internal/status'))
      const status = await statusResponse.json() as { priority: NamespacePriority }
      expect(status.priority).toBe(0)
    })

    it('should default to P2 (medium) priority for new namespaces', async () => {
      const statusResponse = await compactionDO.fetch(new Request('http://internal/status'))
      const status = await statusResponse.json() as { priority: NamespacePriority }
      expect(status.priority).toBe(2)
    })

    it('should validate priority is between 0 and 3', async () => {
      const request = new Request('http://internal/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ priority: 5 }),
      })

      const response = await compactionDO.fetch(request)
      expect(response.status).toBe(400)
    })

    it('should persist priority across restarts', async () => {
      // Set priority
      await compactionDO.fetch(new Request('http://internal/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ priority: 1 }),
      }))

      // Simulate restart with new DO instance
      const newDO = new TestableCompactionStateDO(state)
      const statusResponse = await newDO.fetch(new Request('http://internal/status'))
      const status = await statusResponse.json() as { priority: NamespacePriority }
      expect(status.priority).toBe(1)
    })
  })
})

// =============================================================================
// Priority-Aware Scheduling Tests
// =============================================================================

describe('Priority-based Compaction - Scheduling', () => {
  let state: MockDurableObjectState
  let compactionDO: TestableCompactionStateDO

  beforeEach(() => {
    state = new MockDurableObjectState()
    compactionDO = new TestableCompactionStateDO(state)
  })

  describe('priority-aware max wait time', () => {
    it('should use 1 minute max wait for P0 namespaces', async () => {
      // Configure as P0
      await compactionDO.fetch(new Request('http://internal/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ priority: 0 }),
      }))

      // Create window 2 minutes old (past P0 threshold of 1 minute)
      const windowEnd = Date.now() - (2 * 60 * 1000) // 2 minutes ago
      const windowStart = windowEnd - (60 * 60 * 1000) // 1 hour window

      const response = await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: Array.from({ length: 15 }, (_, i) =>
            createUpdate({ timestamp: windowStart + i * 1000, file: `file${i}.parquet` })
          ),
        })),
      }))

      const body = await response.json() as { windowsReady: WindowReadyEntry[] }
      expect(body.windowsReady.length).toBe(1)
    })

    it('should use 5 minute max wait for P1 namespaces', async () => {
      // Configure as P1
      await compactionDO.fetch(new Request('http://internal/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ priority: 1 }),
      }))

      // Verify that P1 has the correct effective max wait time
      const statusResponse = await compactionDO.fetch(new Request('http://internal/status'))
      const status = await statusResponse.json() as { effectiveMaxWaitTimeMs: number }
      expect(status.effectiveMaxWaitTimeMs).toBe(5 * 60 * 1000) // 5 minutes
    })

    it('should use 1 hour max wait for P3 namespaces', async () => {
      // Configure as P3
      await compactionDO.fetch(new Request('http://internal/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ priority: 3 }),
      }))

      // Verify that P3 has the correct effective max wait time
      const statusResponse = await compactionDO.fetch(new Request('http://internal/status'))
      const status = await statusResponse.json() as { effectiveMaxWaitTimeMs: number }
      expect(status.effectiveMaxWaitTimeMs).toBe(60 * 60 * 1000) // 1 hour
    })
  })

  describe('priority in window response', () => {
    it('should include priority in ready window response', async () => {
      await compactionDO.fetch(new Request('http://internal/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ priority: 0 }),
      }))

      const oldTimestamp = Date.now() - (3600000 + 400000)
      const response = await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: Array.from({ length: 15 }, (_, i) =>
            createUpdate({ timestamp: oldTimestamp, file: `file${i}.parquet` })
          ),
        })),
      }))

      const body = await response.json() as { windowsReady: Array<WindowReadyEntry & { priority: NamespacePriority }> }
      expect(body.windowsReady[0].priority).toBe(0)
    })
  })
})

// =============================================================================
// Backpressure Behavior Tests
// =============================================================================

describe('Priority-based Compaction - Backpressure', () => {
  let state: MockDurableObjectState
  let compactionDO: TestableCompactionStateDO

  beforeEach(() => {
    state = new MockDurableObjectState()
    compactionDO = new TestableCompactionStateDO(state)
  })

  describe('backpressure detection', () => {
    it('should report backpressure level in status', async () => {
      // Create many pending windows to trigger backpressure
      const preloadedState: StoredState = {
        namespace: 'test',
        windows: {},
        knownWriters: ['writer1'],
        writerLastSeen: { writer1: Date.now() },
        priority: 2,
      }

      // Add 15 pending windows to trigger backpressure
      for (let i = 0; i < 15; i++) {
        const windowStart = Date.now() - (i + 2) * 3600000
        preloadedState.windows[String(windowStart)] = {
          windowStart,
          windowEnd: windowStart + 3600000,
          filesByWriter: { writer1: [`file${i}.parquet`] },
          writers: ['writer1'],
          lastActivityAt: Date.now() - 3600000,
          totalSize: 1024,
          processingStatus: { state: 'pending' },
        }
      }

      state.setData('compactionState', preloadedState)
      const newDO = new TestableCompactionStateDO(state)

      const statusResponse = await newDO.fetch(new Request('http://internal/status'))
      const status = await statusResponse.json() as { backpressure: 'none' | 'normal' | 'severe' }
      expect(status.backpressure).toBe('normal')
    })

    it('should report severe backpressure when windows exceed threshold', async () => {
      const preloadedState: StoredState = {
        namespace: 'test',
        windows: {},
        knownWriters: ['writer1'],
        writerLastSeen: { writer1: Date.now() },
        priority: 2,
      }

      // Add 25 pending windows for severe backpressure
      for (let i = 0; i < 25; i++) {
        const windowStart = Date.now() - (i + 2) * 3600000
        preloadedState.windows[String(windowStart)] = {
          windowStart,
          windowEnd: windowStart + 3600000,
          filesByWriter: { writer1: [`file${i}.parquet`] },
          writers: ['writer1'],
          lastActivityAt: Date.now() - 3600000,
          totalSize: 1024,
          processingStatus: { state: 'pending' },
        }
      }

      state.setData('compactionState', preloadedState)
      const newDO = new TestableCompactionStateDO(state)

      const statusResponse = await newDO.fetch(new Request('http://internal/status'))
      const status = await statusResponse.json() as { backpressure: 'none' | 'normal' | 'severe' }
      expect(status.backpressure).toBe('severe')
    })
  })

  describe('backpressure-aware skipping', () => {
    it('should skip P3 namespaces under normal backpressure', async () => {
      // Configure as P3
      await compactionDO.fetch(new Request('http://internal/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ priority: 3 }),
      }))

      // Set backpressure flag
      await compactionDO.fetch(new Request('http://internal/set-backpressure', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ level: 'normal' }),
      }))

      const oldTimestamp = Date.now() - (2 * 3600000) // 2 hours ago
      const response = await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: Array.from({ length: 15 }, (_, i) =>
            createUpdate({ timestamp: oldTimestamp, file: `file${i}.parquet` })
          ),
        })),
      }))

      const body = await response.json() as { windowsReady: WindowReadyEntry[]; skippedDueToBackpressure: boolean }
      expect(body.windowsReady.length).toBe(0)
      expect(body.skippedDueToBackpressure).toBe(true)
    })

    it('should skip P2 namespaces under severe backpressure', async () => {
      // Configure as P2
      await compactionDO.fetch(new Request('http://internal/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ priority: 2 }),
      }))

      // Set severe backpressure
      await compactionDO.fetch(new Request('http://internal/set-backpressure', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ level: 'severe' }),
      }))

      const oldTimestamp = Date.now() - (2 * 3600000)
      const response = await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: Array.from({ length: 15 }, (_, i) =>
            createUpdate({ timestamp: oldTimestamp, file: `file${i}.parquet` })
          ),
        })),
      }))

      const body = await response.json() as { windowsReady: WindowReadyEntry[]; skippedDueToBackpressure: boolean }
      expect(body.windowsReady.length).toBe(0)
      expect(body.skippedDueToBackpressure).toBe(true)
    })

    it('should always process P0 namespaces regardless of backpressure', async () => {
      // Configure as P0
      await compactionDO.fetch(new Request('http://internal/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ priority: 0 }),
      }))

      // Set severe backpressure
      await compactionDO.fetch(new Request('http://internal/set-backpressure', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ level: 'severe' }),
      }))

      const oldTimestamp = Date.now() - (3600000 + 400000)
      const response = await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: Array.from({ length: 15 }, (_, i) =>
            createUpdate({ timestamp: oldTimestamp, file: `file${i}.parquet` })
          ),
        })),
      }))

      const body = await response.json() as { windowsReady: WindowReadyEntry[] }
      expect(body.windowsReady.length).toBe(1)
    })

    it('should always process P1 namespaces under normal backpressure', async () => {
      // Configure as P1
      await compactionDO.fetch(new Request('http://internal/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ priority: 1 }),
      }))

      // Set normal backpressure
      await compactionDO.fetch(new Request('http://internal/set-backpressure', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ level: 'normal' }),
      }))

      const oldTimestamp = Date.now() - (3600000 + 400000)
      const response = await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: Array.from({ length: 15 }, (_, i) =>
            createUpdate({ timestamp: oldTimestamp, file: `file${i}.parquet` })
          ),
        })),
      }))

      const body = await response.json() as { windowsReady: WindowReadyEntry[] }
      expect(body.windowsReady.length).toBe(1)
    })
  })
})

// =============================================================================
// Dashboard Integration Tests
// =============================================================================

describe('Priority-based Compaction - Dashboard', () => {
  let state: MockDurableObjectState
  let compactionDO: TestableCompactionStateDO

  beforeEach(() => {
    state = new MockDurableObjectState()
    compactionDO = new TestableCompactionStateDO(state)
  })

  describe('priority-aware status', () => {
    it('should include priority in status response', async () => {
      await compactionDO.fetch(new Request('http://internal/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ priority: 1 }),
      }))

      const statusResponse = await compactionDO.fetch(new Request('http://internal/status'))
      const status = await statusResponse.json() as { priority: NamespacePriority }
      expect(status.priority).toBe(1)
    })

    it('should include queue metrics by priority in status', async () => {
      const statusResponse = await compactionDO.fetch(new Request('http://internal/status'))
      const status = await statusResponse.json() as {
        queueMetrics: {
          pendingWindows: number
          processingWindows: number
          dispatchedWindows: number
        }
      }

      expect(status.queueMetrics).toBeDefined()
      expect(status.queueMetrics.pendingWindows).toBeDefined()
      expect(status.queueMetrics.processingWindows).toBeDefined()
      expect(status.queueMetrics.dispatchedWindows).toBeDefined()
    })

    it('should include effective max wait time based on priority', async () => {
      await compactionDO.fetch(new Request('http://internal/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ priority: 0 }),
      }))

      const statusResponse = await compactionDO.fetch(new Request('http://internal/status'))
      const status = await statusResponse.json() as { effectiveMaxWaitTimeMs: number }
      expect(status.effectiveMaxWaitTimeMs).toBe(60 * 1000) // 1 minute for P0
    })
  })
})

// =============================================================================
// Health Check Integration Tests
// =============================================================================

describe('Priority-based Compaction - Health Checks', () => {
  let state: MockDurableObjectState
  let compactionDO: TestableCompactionStateDO

  beforeEach(() => {
    state = new MockDurableObjectState()
    compactionDO = new TestableCompactionStateDO(state)
  })

  describe('priority-aware health evaluation', () => {
    it('should alert earlier for P0 namespaces falling behind', async () => {
      // Configure as P0
      await compactionDO.fetch(new Request('http://internal/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ priority: 0 }),
      }))

      // Add a window that's old enough to be concerning for P0
      const twoMinutesAgo = Date.now() - (2 * 60 * 1000)
      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: Array.from({ length: 15 }, (_, i) =>
            createUpdate({ timestamp: twoMinutesAgo - 3600000, file: `file${i}.parquet` })
          ),
        })),
      }))

      const statusResponse = await compactionDO.fetch(new Request('http://internal/status'))
      const status = await statusResponse.json() as { health: { status: string; issues: string[] } }

      // P0 should show degraded status if oldest window is > 1 minute old
      expect(status.health.status).toBe('degraded')
    })

    it('should have relaxed thresholds for P3 namespaces', async () => {
      // Configure as P3
      await compactionDO.fetch(new Request('http://internal/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ priority: 3 }),
      }))

      // Add a window that's 30 minutes old (would be concerning for P0/P1 but fine for P3)
      const thirtyMinutesAgo = Date.now() - (30 * 60 * 1000)
      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: Array.from({ length: 15 }, (_, i) =>
            createUpdate({ timestamp: thirtyMinutesAgo - 3600000, file: `file${i}.parquet` })
          ),
        })),
      }))

      const statusResponse = await compactionDO.fetch(new Request('http://internal/status'))
      const status = await statusResponse.json() as { health: { status: string } }

      // P3 should still be healthy with 30 minute old windows
      expect(status.health.status).toBe('healthy')
    })
  })
})
