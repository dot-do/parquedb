/**
 * CompactionStateDO Tests
 *
 * Tests for the Durable Object that tracks compaction state.
 * Covers:
 * - /update endpoint
 * - /status endpoint
 * - State persistence
 * - Error handling
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'

// Import shared test utilities
import {
  TestableCompactionStateDO,
  MockDurableObjectState,
  createUpdateRequest,
  createUpdate,
  type StoredState,
  type WindowReadyEntry,
} from './__helpers__/testable-compaction-state-do'

// =============================================================================
// /update Endpoint Tests
// =============================================================================

describe('CompactionStateDO - /update Endpoint', () => {
  let state: MockDurableObjectState
  let compactionDO: TestableCompactionStateDO

  beforeEach(() => {
    state = new MockDurableObjectState()
    compactionDO = new TestableCompactionStateDO(state)
  })

  describe('basic update handling', () => {
    it('should return 200 for valid POST request', async () => {
      const request = new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: [createUpdate()],
        })),
      })

      const response = await compactionDO.fetch(request)

      expect(response.status).toBe(200)
    })

    it('should return windowsReady array in response', async () => {
      const request = new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: [createUpdate()],
        })),
      })

      const response = await compactionDO.fetch(request)
      const body = await response.json() as { windowsReady: WindowReadyEntry[] }

      expect(body).toHaveProperty('windowsReady')
      expect(Array.isArray(body.windowsReady)).toBe(true)
    })

    it('should set namespace from first update', async () => {
      const request = new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          namespace: 'posts',
          updates: [createUpdate({ namespace: 'posts' })],
        })),
      })

      await compactionDO.fetch(request)

      expect(compactionDO.getNamespace()).toBe('posts')
    })
  })

  describe('writer tracking', () => {
    it('should track writers from updates', async () => {
      const request = new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: [
            createUpdate({ writerId: 'writer1' }),
            createUpdate({ writerId: 'writer2' }),
            createUpdate({ writerId: 'writer3' }),
          ],
        })),
      })

      await compactionDO.fetch(request)

      const knownWriters = compactionDO.getKnownWriters()
      expect(knownWriters).toContain('writer1')
      expect(knownWriters).toContain('writer2')
      expect(knownWriters).toContain('writer3')
    })

    it('should not duplicate writers', async () => {
      const request = new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: [
            createUpdate({ writerId: 'writer1' }),
            createUpdate({ writerId: 'writer1' }),
            createUpdate({ writerId: 'writer1' }),
          ],
        })),
      })

      await compactionDO.fetch(request)

      const knownWriters = compactionDO.getKnownWriters()
      expect(knownWriters).toHaveLength(1)
    })
  })

  describe('window creation', () => {
    it('should create window for new timestamp range', async () => {
      const request = new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: [createUpdate({ timestamp: 1700001234000 })],
        })),
      })

      await compactionDO.fetch(request)

      expect(compactionDO.getWindowCount()).toBe(1)
    })

    it('should group files into same window for same hour', async () => {
      const request = new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: [
            createUpdate({ timestamp: 1700000000000, file: 'file1.parquet' }),
            createUpdate({ timestamp: 1700001000000, file: 'file2.parquet' }),
            createUpdate({ timestamp: 1700002000000, file: 'file3.parquet' }),
          ],
        })),
      })

      await compactionDO.fetch(request)

      expect(compactionDO.getWindowCount()).toBe(1)
    })

    it('should create separate windows for different hours', async () => {
      const request = new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: [
            createUpdate({ timestamp: 1700000000000, file: 'file1.parquet' }),
            createUpdate({ timestamp: 1700003600000, file: 'file2.parquet' }), // +1 hour
          ],
        })),
      })

      await compactionDO.fetch(request)

      expect(compactionDO.getWindowCount()).toBe(2)
    })
  })

  describe('window readiness detection', () => {
    it('should not return windows that are too recent', async () => {
      const now = Date.now()
      const request = new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: Array.from({ length: 15 }, (_, i) =>
            createUpdate({ timestamp: now, file: `file${i}.parquet` })
          ),
        })),
      })

      const response = await compactionDO.fetch(request)
      const body = await response.json() as { windowsReady: WindowReadyEntry[] }

      expect(body.windowsReady).toHaveLength(0)
    })

    it('should return ready windows that have enough time and files', async () => {
      const oldTimestamp = Date.now() - (3600000 + 400000) // More than window + wait time ago
      const request = new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: Array.from({ length: 15 }, (_, i) =>
            createUpdate({ timestamp: oldTimestamp, file: `file${i}.parquet` })
          ),
        })),
      })

      const response = await compactionDO.fetch(request)
      const body = await response.json() as { windowsReady: WindowReadyEntry[] }

      expect(body.windowsReady).toHaveLength(1)
      expect(body.windowsReady[0].files).toHaveLength(15)
    })

    it('should not return windows below minimum file threshold', async () => {
      const oldTimestamp = Date.now() - (3600000 + 400000)
      const request = new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: Array.from({ length: 5 }, (_, i) =>
            createUpdate({ timestamp: oldTimestamp, file: `file${i}.parquet` })
          ),
          config: {
            windowSizeMs: 3600000,
            minFilesToCompact: 10, // Requires 10 but only has 5
            maxWaitTimeMs: 300000,
            targetFormat: 'native',
          },
        })),
      })

      const response = await compactionDO.fetch(request)
      const body = await response.json() as { windowsReady: WindowReadyEntry[] }

      expect(body.windowsReady).toHaveLength(0)
    })

    it('should mark ready windows as processing (not delete them)', async () => {
      const oldTimestamp = Date.now() - (3600000 + 400000)
      const windowStart = Math.floor(oldTimestamp / 3600000) * 3600000
      const windowKey = String(windowStart)

      const request = new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: Array.from({ length: 15 }, (_, i) =>
            createUpdate({ timestamp: oldTimestamp, file: `file${i}.parquet` })
          ),
        })),
      })

      await compactionDO.fetch(request)

      // Window should still exist but be in processing state (two-phase commit)
      expect(compactionDO.getWindowCount()).toBe(1)
      const status = compactionDO.getWindowProcessingStatus(windowKey)
      expect(status?.state).toBe('processing')
    })

    it('should include sorted file list in ready window', async () => {
      const oldTimestamp = Date.now() - (3600000 + 400000)
      const request = new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: [
            createUpdate({ timestamp: oldTimestamp, file: 'c.parquet' }),
            createUpdate({ timestamp: oldTimestamp, file: 'a.parquet' }),
            createUpdate({ timestamp: oldTimestamp, file: 'b.parquet' }),
            ...Array.from({ length: 10 }, (_, i) =>
              createUpdate({ timestamp: oldTimestamp, file: `d${i}.parquet` })
            ),
          ],
        })),
      })

      const response = await compactionDO.fetch(request)
      const body = await response.json() as { windowsReady: WindowReadyEntry[] }

      // Files should be sorted
      const files = body.windowsReady[0].files
      const sortedFiles = [...files].sort()
      expect(files).toEqual(sortedFiles)
    })
  })

  describe('multiple updates', () => {
    it('should accumulate state across multiple update calls', async () => {
      // First update
      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: [createUpdate({ writerId: 'writer1', file: 'file1.parquet' })],
        })),
      }))

      // Second update
      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: [createUpdate({ writerId: 'writer2', file: 'file2.parquet' })],
        })),
      }))

      const knownWriters = compactionDO.getKnownWriters()
      expect(knownWriters).toContain('writer1')
      expect(knownWriters).toContain('writer2')
    })
  })
})

// =============================================================================
// /status Endpoint Tests
// =============================================================================

describe('CompactionStateDO - /status Endpoint', () => {
  let state: MockDurableObjectState
  let compactionDO: TestableCompactionStateDO

  beforeEach(() => {
    state = new MockDurableObjectState()
    compactionDO = new TestableCompactionStateDO(state)
  })

  describe('status response format', () => {
    it('should return 200 for GET request', async () => {
      const response = await compactionDO.fetch(new Request('http://internal/status'))

      expect(response.status).toBe(200)
    })

    it('should return JSON content-type', async () => {
      const response = await compactionDO.fetch(new Request('http://internal/status'))

      expect(response.headers.get('Content-Type')).toBe('application/json')
    })

    it('should include all status fields', async () => {
      const response = await compactionDO.fetch(new Request('http://internal/status'))
      const body = await response.json() as Record<string, unknown>

      expect(body).toHaveProperty('namespace')
      expect(body).toHaveProperty('activeWindows')
      expect(body).toHaveProperty('knownWriters')
      expect(body).toHaveProperty('activeWriters')
      expect(body).toHaveProperty('windows')
    })
  })

  describe('status content', () => {
    it('should report zero windows for empty state', async () => {
      const response = await compactionDO.fetch(new Request('http://internal/status'))
      const body = await response.json() as { activeWindows: number }

      expect(body.activeWindows).toBe(0)
    })

    it('should report correct window count', async () => {
      // Add some windows
      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: [
            createUpdate({ timestamp: 1700000000000 }),
            createUpdate({ timestamp: 1700003600000 }), // Different window
          ],
        })),
      }))

      const response = await compactionDO.fetch(new Request('http://internal/status'))
      const body = await response.json() as { activeWindows: number }

      expect(body.activeWindows).toBe(2)
    })

    it('should list known writers', async () => {
      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: [
            createUpdate({ writerId: 'writer-alpha' }),
            createUpdate({ writerId: 'writer-beta' }),
          ],
        })),
      }))

      const response = await compactionDO.fetch(new Request('http://internal/status'))
      const body = await response.json() as { knownWriters: string[] }

      expect(body.knownWriters).toContain('writer-alpha')
      expect(body.knownWriters).toContain('writer-beta')
    })

    it('should include window details', async () => {
      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: [
            createUpdate({ timestamp: 1700000000000, size: 1024 }),
            createUpdate({ timestamp: 1700000000000, size: 2048 }),
          ],
        })),
      }))

      const response = await compactionDO.fetch(new Request('http://internal/status'))
      const body = await response.json() as {
        windows: Array<{
          key: string
          windowStart: string
          windowEnd: string
          writers: string[]
          fileCount: number
          totalSize: number
        }>
      }

      expect(body.windows).toHaveLength(1)
      expect(body.windows[0].fileCount).toBe(2)
      expect(body.windows[0].totalSize).toBe(3072)
    })
  })
})

// =============================================================================
// State Persistence Tests
// =============================================================================

describe('CompactionStateDO - State Persistence', () => {
  let state: MockDurableObjectState
  let compactionDO: TestableCompactionStateDO

  beforeEach(() => {
    state = new MockDurableObjectState()
    compactionDO = new TestableCompactionStateDO(state)
  })

  describe('saving state', () => {
    it('should persist state after update', async () => {
      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: [createUpdate({ writerId: 'writer1' })],
        })),
      }))

      const stored = state.getData('compactionState') as StoredState

      expect(stored).toBeDefined()
      expect(stored.knownWriters).toContain('writer1')
    })

    it('should persist windows correctly', async () => {
      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: [
            createUpdate({
              timestamp: 1700001234000,
              file: 'test-file.parquet',
            }),
          ],
        })),
      }))

      const stored = state.getData('compactionState') as StoredState

      expect(Object.keys(stored.windows)).toHaveLength(1)
      const windowKey = Object.keys(stored.windows)[0]!
      expect(stored.windows[windowKey].filesByWriter['writer1']).toContain('test-file.parquet')
    })

    it('should persist namespace', async () => {
      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          namespace: 'test-namespace',
          updates: [createUpdate()],
        })),
      }))

      const stored = state.getData('compactionState') as StoredState

      expect(stored.namespace).toBe('test-namespace')
    })
  })

  describe('loading state', () => {
    it('should restore state from storage', async () => {
      // Pre-populate storage
      const preloadedState: StoredState = {
        namespace: 'preloaded',
        windows: {
          '1700000000000': {
            windowStart: 1700000000000,
            windowEnd: 1700003600000,
            filesByWriter: { 'writer1': ['preloaded-file.parquet'] },
            writers: ['writer1'],
            lastActivityAt: 1700001234000,
            totalSize: 1024,
          },
        },
        knownWriters: ['writer1', 'writer2'],
        writerLastSeen: {
          'writer1': Date.now(),
          'writer2': Date.now() - 1000,
        },
      }

      state.setData('compactionState', preloadedState)

      // Create new DO instance (simulates restart)
      const newDO = new TestableCompactionStateDO(state)

      const response = await newDO.fetch(new Request('http://internal/status'))
      const body = await response.json() as {
        namespace: string
        activeWindows: number
        knownWriters: string[]
      }

      expect(body.namespace).toBe('preloaded')
      expect(body.activeWindows).toBe(1)
      expect(body.knownWriters).toContain('writer1')
      expect(body.knownWriters).toContain('writer2')
    })

    it('should handle missing storage gracefully', async () => {
      // Don't pre-populate storage

      const response = await compactionDO.fetch(new Request('http://internal/status'))
      const body = await response.json() as { activeWindows: number }

      expect(body.activeWindows).toBe(0)
    })
  })

  describe('state consistency', () => {
    it('should maintain consistency across multiple operations', async () => {
      // Operation 1
      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: [createUpdate({ writerId: 'writer1', file: 'file1.parquet' })],
        })),
      }))

      // Operation 2
      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: [createUpdate({ writerId: 'writer2', file: 'file2.parquet' })],
        })),
      }))

      // Simulate restart
      const newDO = new TestableCompactionStateDO(state)

      const response = await newDO.fetch(new Request('http://internal/status'))
      const body = await response.json() as { knownWriters: string[] }

      expect(body.knownWriters).toContain('writer1')
      expect(body.knownWriters).toContain('writer2')
    })
  })
})

// =============================================================================
// Error Handling Tests
// =============================================================================

describe('CompactionStateDO - Error Handling', () => {
  let state: MockDurableObjectState
  let compactionDO: TestableCompactionStateDO

  beforeEach(() => {
    state = new MockDurableObjectState()
    compactionDO = new TestableCompactionStateDO(state)
  })

  describe('routing errors', () => {
    it('should return 404 for unknown paths', async () => {
      const response = await compactionDO.fetch(
        new Request('http://internal/unknown')
      )

      expect(response.status).toBe(404)
    })

    it('should return 404 for unknown nested paths', async () => {
      const response = await compactionDO.fetch(
        new Request('http://internal/api/v1/status')
      )

      expect(response.status).toBe(404)
    })
  })

  describe('request validation', () => {
    it('should throw error for invalid JSON in update', async () => {
      const request = new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: 'not valid json',
      })

      await expect(compactionDO.fetch(request)).rejects.toThrow()
    })

    it('should handle empty updates array', async () => {
      const request = new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: [],
        })),
      })

      const response = await compactionDO.fetch(request)
      expect(response.status).toBe(200)

      const body = await response.json() as { windowsReady: WindowReadyEntry[] }
      expect(body.windowsReady).toHaveLength(0)
    })
  })

  describe('method validation', () => {
    it('should handle GET request to /update (implied not POST)', async () => {
      // /update requires POST - GET should fail routing
      const response = await compactionDO.fetch(
        new Request('http://internal/update', { method: 'GET' })
      )

      expect(response.status).toBe(404)
    })
  })
})

// =============================================================================
// Edge Cases Tests
// =============================================================================

describe('CompactionStateDO - Edge Cases', () => {
  let state: MockDurableObjectState
  let compactionDO: TestableCompactionStateDO

  beforeEach(() => {
    state = new MockDurableObjectState()
    compactionDO = new TestableCompactionStateDO(state)
  })

  describe('timestamp boundaries', () => {
    it('should handle files at exact window boundaries', async () => {
      const windowSizeMs = 3600000
      const exactBoundary = Math.floor(Date.now() / windowSizeMs) * windowSizeMs - (2 * windowSizeMs)

      const request = new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: [
            createUpdate({ timestamp: exactBoundary, file: 'boundary.parquet' }),
          ],
        })),
      })

      const response = await compactionDO.fetch(request)
      expect(response.status).toBe(200)
    })
  })

  describe('concurrent writers', () => {
    it('should handle files from many writers in same window', async () => {
      const timestamp = Date.now() - (3600000 + 400000)
      const updates = Array.from({ length: 20 }, (_, i) =>
        createUpdate({
          writerId: `writer${i}`,
          timestamp,
          file: `writer${i}-file.parquet`,
        })
      )

      const request = new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({ updates })),
      })

      const response = await compactionDO.fetch(request)
      const body = await response.json() as { windowsReady: WindowReadyEntry[] }

      expect(body.windowsReady).toHaveLength(1)
      expect(body.windowsReady[0].writers).toHaveLength(20)
    })
  })

  describe('large file counts', () => {
    it('should handle many files per writer', async () => {
      const timestamp = Date.now() - (3600000 + 400000)
      const updates = Array.from({ length: 100 }, (_, i) =>
        createUpdate({
          timestamp,
          file: `file${i}.parquet`,
        })
      )

      const request = new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({ updates })),
      })

      const response = await compactionDO.fetch(request)
      const body = await response.json() as { windowsReady: WindowReadyEntry[] }

      expect(body.windowsReady).toHaveLength(1)
      expect(body.windowsReady[0].files).toHaveLength(100)
    })
  })

  describe('size tracking', () => {
    it('should accumulate file sizes correctly', async () => {
      const timestamp = Date.now()
      const updates = [
        createUpdate({ timestamp, file: 'file1.parquet', size: 1000 }),
        createUpdate({ timestamp, file: 'file2.parquet', size: 2000 }),
        createUpdate({ timestamp, file: 'file3.parquet', size: 3000 }),
      ]

      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({ updates })),
      }))

      const statusResponse = await compactionDO.fetch(
        new Request('http://internal/status')
      )
      const status = await statusResponse.json() as {
        windows: Array<{ totalSize: number }>
      }

      expect(status.windows[0].totalSize).toBe(6000)
    })
  })
})

// =============================================================================
// Two-Phase Commit Tests
// =============================================================================

describe('CompactionStateDO - Two-Phase Commit', () => {
  let state: MockDurableObjectState
  let compactionDO: TestableCompactionStateDO

  beforeEach(() => {
    state = new MockDurableObjectState()
    compactionDO = new TestableCompactionStateDO(state)
  })

  describe('/confirm-dispatch endpoint', () => {
    it('should mark window as dispatched', async () => {
      // First create a ready window
      const oldTimestamp = Date.now() - (3600000 + 400000)
      const windowStart = Math.floor(oldTimestamp / 3600000) * 3600000
      const windowKey = String(windowStart)

      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: Array.from({ length: 15 }, (_, i) =>
            createUpdate({ timestamp: oldTimestamp, file: `file${i}.parquet` })
          ),
        })),
      }))

      // Confirm dispatch
      const response = await compactionDO.fetch(new Request('http://internal/confirm-dispatch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ windowKey, workflowId: 'workflow-123' }),
      }))

      expect(response.status).toBe(200)
      const body = await response.json() as { success: boolean }
      expect(body.success).toBe(true)

      // Check status
      const status = compactionDO.getWindowProcessingStatus(windowKey)
      expect(status?.state).toBe('dispatched')
      if (status?.state === 'dispatched') {
        expect(status.workflowId).toBe('workflow-123')
      }
    })

    it('should return 404 for non-existent window', async () => {
      const response = await compactionDO.fetch(new Request('http://internal/confirm-dispatch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ windowKey: 'nonexistent', workflowId: 'workflow-123' }),
      }))

      expect(response.status).toBe(404)
    })

    it('should return 409 if window is not in processing state', async () => {
      // Create a window but don't let it become ready
      const timestamp = Date.now()
      const windowStart = Math.floor(timestamp / 3600000) * 3600000
      const windowKey = String(windowStart)

      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: [createUpdate({ timestamp })],
        })),
      }))

      // Try to confirm dispatch on pending window
      const response = await compactionDO.fetch(new Request('http://internal/confirm-dispatch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ windowKey, workflowId: 'workflow-123' }),
      }))

      expect(response.status).toBe(409)
    })
  })

  describe('/rollback-processing endpoint', () => {
    it('should reset window to pending state', async () => {
      // Create a ready window (goes to processing)
      const oldTimestamp = Date.now() - (3600000 + 400000)
      const windowStart = Math.floor(oldTimestamp / 3600000) * 3600000
      const windowKey = String(windowStart)

      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: Array.from({ length: 15 }, (_, i) =>
            createUpdate({ timestamp: oldTimestamp, file: `file${i}.parquet` })
          ),
        })),
      }))

      // Rollback processing
      const response = await compactionDO.fetch(new Request('http://internal/rollback-processing', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ windowKey }),
      }))

      expect(response.status).toBe(200)
      const body = await response.json() as { success: boolean }
      expect(body.success).toBe(true)

      // Check status is back to pending
      const status = compactionDO.getWindowProcessingStatus(windowKey)
      expect(status?.state).toBe('pending')
    })

    it('should return 404 for non-existent window', async () => {
      const response = await compactionDO.fetch(new Request('http://internal/rollback-processing', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ windowKey: 'nonexistent' }),
      }))

      expect(response.status).toBe(404)
    })
  })

  describe('/workflow-complete endpoint', () => {
    it('should delete window on successful completion', async () => {
      // Create a ready window and confirm dispatch
      const oldTimestamp = Date.now() - (3600000 + 400000)
      const windowStart = Math.floor(oldTimestamp / 3600000) * 3600000
      const windowKey = String(windowStart)

      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: Array.from({ length: 15 }, (_, i) =>
            createUpdate({ timestamp: oldTimestamp, file: `file${i}.parquet` })
          ),
        })),
      }))

      await compactionDO.fetch(new Request('http://internal/confirm-dispatch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ windowKey, workflowId: 'workflow-123' }),
      }))

      // Complete workflow successfully
      const response = await compactionDO.fetch(new Request('http://internal/workflow-complete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ windowKey, workflowId: 'workflow-123', success: true }),
      }))

      expect(response.status).toBe(200)
      expect(compactionDO.getWindowCount()).toBe(0)
    })

    it('should reset window to pending on failed completion', async () => {
      // Create a ready window and confirm dispatch
      const oldTimestamp = Date.now() - (3600000 + 400000)
      const windowStart = Math.floor(oldTimestamp / 3600000) * 3600000
      const windowKey = String(windowStart)

      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: Array.from({ length: 15 }, (_, i) =>
            createUpdate({ timestamp: oldTimestamp, file: `file${i}.parquet` })
          ),
        })),
      }))

      await compactionDO.fetch(new Request('http://internal/confirm-dispatch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ windowKey, workflowId: 'workflow-123' }),
      }))

      // Complete workflow with failure
      const response = await compactionDO.fetch(new Request('http://internal/workflow-complete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ windowKey, workflowId: 'workflow-123', success: false }),
      }))

      expect(response.status).toBe(200)
      expect(compactionDO.getWindowCount()).toBe(1)
      expect(compactionDO.getWindowProcessingStatus(windowKey)?.state).toBe('pending')
    })

    it('should reject mismatched workflow ID', async () => {
      // Create a ready window and confirm dispatch
      const oldTimestamp = Date.now() - (3600000 + 400000)
      const windowStart = Math.floor(oldTimestamp / 3600000) * 3600000
      const windowKey = String(windowStart)

      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: Array.from({ length: 15 }, (_, i) =>
            createUpdate({ timestamp: oldTimestamp, file: `file${i}.parquet` })
          ),
        })),
      }))

      await compactionDO.fetch(new Request('http://internal/confirm-dispatch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ windowKey, workflowId: 'workflow-123' }),
      }))

      // Try to complete with wrong workflow ID
      const response = await compactionDO.fetch(new Request('http://internal/workflow-complete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ windowKey, workflowId: 'wrong-workflow', success: true }),
      }))

      expect(response.status).toBe(409)
    })

    it('should handle already-deleted window gracefully', async () => {
      const response = await compactionDO.fetch(new Request('http://internal/workflow-complete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ windowKey: 'nonexistent', workflowId: 'workflow-123', success: true }),
      }))

      expect(response.status).toBe(200)
      const body = await response.json() as { success: boolean; alreadyDeleted: boolean }
      expect(body.success).toBe(true)
      expect(body.alreadyDeleted).toBe(true)
    })
  })

  describe('stuck window cleanup', () => {
    it('should NOT automatically reset stuck processing windows (deprecated behavior)', async () => {
      // NOTE: Automatic stuck window cleanup was deprecated to prevent race conditions.
      // The cleanupStuckProcessingWindows method is now a no-op.
      // Proper crash recovery is via /get-stuck-windows endpoint which includes
      // the provisional workflow ID for status checking before deciding to confirm or rollback.
      //
      // This test verifies that stuck windows are NOT automatically reset,
      // preserving them for proper recovery via the /get-stuck-windows endpoint.

      // Pre-populate storage with a stuck processing window
      const oldWindowStart = Date.now() - (2 * 3600000) // 2 hours ago
      const windowKey = String(oldWindowStart)
      const stuckStartedAt = Date.now() - (10 * 60 * 1000) // 10 minutes ago (past 5 min timeout)

      const preloadedState: StoredState = {
        namespace: 'users',
        windows: {
          [windowKey]: {
            windowStart: oldWindowStart,
            windowEnd: oldWindowStart + 3600000,
            filesByWriter: { 'writer1': ['stuck-file.parquet'] },
            writers: ['writer1'],
            lastActivityAt: stuckStartedAt,
            totalSize: 1024,
            processingStatus: { state: 'processing', startedAt: stuckStartedAt },
          },
        },
        knownWriters: ['writer1'],
        writerLastSeen: { 'writer1': stuckStartedAt },
      }

      state.setData('compactionState', preloadedState)

      // Create new DO (simulates restart)
      const newDO = new TestableCompactionStateDO(state)

      // Send any update
      await newDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: [createUpdate({ timestamp: Date.now() })],
        })),
      }))

      // Stuck window should STILL be in processing state (no automatic cleanup)
      // Recovery must happen via /get-stuck-windows endpoint and workflow status check
      expect(newDO.getWindowProcessingStatus(windowKey)?.state).toBe('processing')
    })
  })

  describe('window state transitions', () => {
    it('should not return processing windows as ready again', async () => {
      // Create a ready window
      const oldTimestamp = Date.now() - (3600000 + 400000)
      const windowStart = Math.floor(oldTimestamp / 3600000) * 3600000
      const windowKey = String(windowStart)

      const firstResponse = await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: Array.from({ length: 15 }, (_, i) =>
            createUpdate({ timestamp: oldTimestamp, file: `file${i}.parquet` })
          ),
        })),
      }))

      const firstBody = await firstResponse.json() as { windowsReady: WindowReadyEntry[] }
      expect(firstBody.windowsReady).toHaveLength(1)

      // Send another update - processing window should not be returned again
      const secondResponse = await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: [createUpdate({ timestamp: Date.now() })],
        })),
      }))

      const secondBody = await secondResponse.json() as { windowsReady: WindowReadyEntry[] }
      // The processing window should not appear again
      const processingWindowReady = secondBody.windowsReady.find(w => w.windowKey === windowKey)
      expect(processingWindowReady).toBeUndefined()
    })

    it('should not add new files to processing windows', async () => {
      // Create a ready window
      const oldTimestamp = Date.now() - (3600000 + 400000)
      const windowStart = Math.floor(oldTimestamp / 3600000) * 3600000
      const windowKey = String(windowStart)

      const firstResponse = await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: Array.from({ length: 15 }, (_, i) =>
            createUpdate({ timestamp: oldTimestamp, file: `file${i}.parquet` })
          ),
        })),
      }))

      const firstBody = await firstResponse.json() as { windowsReady: WindowReadyEntry[] }
      const originalFileCount = firstBody.windowsReady[0].files.length

      // Try to add more files to the same window
      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: Array.from({ length: 5 }, (_, i) =>
            createUpdate({ timestamp: oldTimestamp, file: `extra-file${i}.parquet` })
          ),
        })),
      }))

      // Check that status shows same file count (files not added)
      const statusResponse = await compactionDO.fetch(new Request('http://internal/status'))
      const status = await statusResponse.json() as {
        windows: Array<{ key: string; fileCount: number }>
      }

      const windowStatus = status.windows.find(w => w.key === windowKey)
      expect(windowStatus?.fileCount).toBe(originalFileCount)
    })

    it('should include windowKey in ready window response', async () => {
      const oldTimestamp = Date.now() - (3600000 + 400000)
      const windowStart = Math.floor(oldTimestamp / 3600000) * 3600000
      const expectedWindowKey = String(windowStart)

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
      expect(body.windowsReady[0].windowKey).toBe(expectedWindowKey)
    })
  })
})

// =============================================================================
// Per-Window Storage Tests (128KB Limit Fix)
// =============================================================================

describe('CompactionStateDO - Per-Window Storage (128KB Limit Fix)', () => {
  let state: MockDurableObjectState
  let compactionDO: TestableCompactionStateDO

  beforeEach(() => {
    state = new MockDurableObjectState()
    compactionDO = new TestableCompactionStateDO(state)
  })

  describe('storage key structure', () => {
    it('should store metadata in separate key from windows', async () => {
      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          namespace: 'users',
          updates: [createUpdate({ timestamp: 1700001234000 })],
        })),
      }))

      // Verify metadata is stored separately
      const metadata = state.getData('metadata') as { namespace: string; knownWriters: string[] }
      expect(metadata).toBeDefined()
      expect(metadata.namespace).toBe('users')
      expect(metadata.knownWriters).toContain('writer1')
    })

    it('should store each window in its own key', async () => {
      const timestamp = 1700001234000
      const windowStart = Math.floor(timestamp / 3600000) * 3600000

      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: [createUpdate({ timestamp })],
        })),
      }))

      // Verify window is stored with its own key
      const windowData = state.getData(`window:${windowStart}`)
      expect(windowData).toBeDefined()
    })

    it('should store multiple windows in separate keys', async () => {
      const timestamp1 = 1700000000000
      const timestamp2 = 1700003600000 // +1 hour
      const windowStart1 = Math.floor(timestamp1 / 3600000) * 3600000
      const windowStart2 = Math.floor(timestamp2 / 3600000) * 3600000

      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: [
            createUpdate({ timestamp: timestamp1, file: 'file1.parquet' }),
            createUpdate({ timestamp: timestamp2, file: 'file2.parquet' }),
          ],
        })),
      }))

      // Verify each window has its own key
      expect(state.getData(`window:${windowStart1}`)).toBeDefined()
      expect(state.getData(`window:${windowStart2}`)).toBeDefined()
    })

    it('should delete window key when workflow completes successfully', async () => {
      const oldTimestamp = Date.now() - (3600000 + 400000)
      const windowStart = Math.floor(oldTimestamp / 3600000) * 3600000
      const windowKey = String(windowStart)

      // Create a ready window and confirm dispatch
      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({
          updates: Array.from({ length: 15 }, (_, i) =>
            createUpdate({ timestamp: oldTimestamp, file: `file${i}.parquet` })
          ),
        })),
      }))

      await compactionDO.fetch(new Request('http://internal/confirm-dispatch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ windowKey, workflowId: 'workflow-123' }),
      }))

      // Complete workflow successfully
      await compactionDO.fetch(new Request('http://internal/workflow-complete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ windowKey, workflowId: 'workflow-123', success: true }),
      }))

      // Window key should be deleted
      expect(state.getData(`window:${windowStart}`)).toBeUndefined()
    })
  })

  describe('storage size limits', () => {
    it('should handle large number of files without exceeding per-key limit', async () => {
      // Each file path is ~50 bytes, 128KB / 50 = ~2600 files max per window
      // Test with 1000 files per window to stay well under limit
      const timestamp = Date.now() - (3600000 + 400000)
      const updates = Array.from({ length: 1000 }, (_, i) =>
        createUpdate({
          timestamp,
          file: `data/users/${timestamp}-writer1-${i}.parquet`,
        })
      )

      const response = await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({ updates })),
      }))

      expect(response.status).toBe(200)
      const body = await response.json() as { windowsReady: WindowReadyEntry[] }
      expect(body.windowsReady).toHaveLength(1)
      expect(body.windowsReady[0].files).toHaveLength(1000)
    })

    it('should handle many windows without exceeding total storage', async () => {
      // Create 50 windows with 20 files each
      // Each window stored separately, so no single key is too large
      const baseTimestamp = Date.now() - (100 * 3600000) // 100 hours ago
      const updates: ReturnType<typeof createUpdate>[] = []

      for (let hour = 0; hour < 50; hour++) {
        const timestamp = baseTimestamp + (hour * 3600000) + 400000 // Add offset to make windows ready
        for (let file = 0; file < 20; file++) {
          updates.push(createUpdate({
            timestamp,
            file: `data/users/${timestamp}-writer1-${file}.parquet`,
          }))
        }
      }

      const response = await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createUpdateRequest({ updates })),
      }))

      expect(response.status).toBe(200)
      const body = await response.json() as { windowsReady: WindowReadyEntry[] }
      // Some windows may be ready depending on timing
      expect(compactionDO.getWindowCount()).toBeGreaterThanOrEqual(50)
    })
  })

  describe('state restoration from per-window keys', () => {
    it('should restore state from per-window storage keys', async () => {
      const windowStart = 1700000000000

      // Pre-populate with new per-window storage format
      state.setData('metadata', {
        namespace: 'preloaded',
        knownWriters: ['writer1', 'writer2'],
        writerLastSeen: {
          'writer1': Date.now(),
          'writer2': Date.now() - 1000,
        },
        priority: 1,
      })

      state.setData(`window:${windowStart}`, {
        windowStart,
        windowEnd: windowStart + 3600000,
        filesByWriter: { 'writer1': ['preloaded-file.parquet'] },
        writers: ['writer1'],
        lastActivityAt: 1700001234000,
        totalSize: 1024,
        processingStatus: { state: 'pending' },
      })

      // Create new DO instance (simulates restart)
      const newDO = new TestableCompactionStateDO(state)

      const response = await newDO.fetch(new Request('http://internal/status'))
      const body = await response.json() as {
        namespace: string
        activeWindows: number
        knownWriters: string[]
      }

      expect(body.namespace).toBe('preloaded')
      expect(body.activeWindows).toBe(1)
      expect(body.knownWriters).toContain('writer1')
      expect(body.knownWriters).toContain('writer2')
    })

    it('should handle mixed old and new storage format (migration)', async () => {
      // Pre-populate with old format for backwards compatibility during migration
      const preloadedState: StoredState = {
        namespace: 'legacy',
        windows: {
          '1700000000000': {
            windowStart: 1700000000000,
            windowEnd: 1700003600000,
            filesByWriter: { 'writer1': ['legacy-file.parquet'] },
            writers: ['writer1'],
            lastActivityAt: 1700001234000,
            totalSize: 1024,
          },
        },
        knownWriters: ['writer1'],
        writerLastSeen: { 'writer1': Date.now() },
      }

      state.setData('compactionState', preloadedState)

      // Create new DO instance (simulates restart)
      const newDO = new TestableCompactionStateDO(state)

      const response = await newDO.fetch(new Request('http://internal/status'))
      const body = await response.json() as {
        namespace: string
        activeWindows: number
      }

      // Should still work with old format
      expect(body.namespace).toBe('legacy')
      expect(body.activeWindows).toBe(1)
    })
  })
})
