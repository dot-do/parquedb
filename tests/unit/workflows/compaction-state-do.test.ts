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

// =============================================================================
// Types
// =============================================================================

interface WindowState {
  windowStart: number
  windowEnd: number
  filesByWriter: Map<string, string[]>
  writers: Set<string>
  lastActivityAt: number
  totalSize: number
}

interface StoredWindowState {
  windowStart: number
  windowEnd: number
  filesByWriter: Record<string, string[]>
  writers: string[]
  lastActivityAt: number
  totalSize: number
}

interface StoredState {
  namespace: string
  windows: Record<string, StoredWindowState>
  knownWriters: string[]
  writerLastSeen: Record<string, number>
}

interface UpdateRequest {
  namespace: string
  updates: Array<{
    namespace: string
    writerId: string
    file: string
    timestamp: number
    size: number
  }>
  config: {
    windowSizeMs: number
    minFilesToCompact: number
    maxWaitTimeMs: number
    targetFormat: string
  }
}

interface WindowReadyEntry {
  namespace: string
  windowStart: number
  windowEnd: number
  files: string[]
  writers: string[]
}

// =============================================================================
// Mock Classes
// =============================================================================

class MockDurableObjectStorage {
  private data: Map<string, unknown> = new Map()

  async get<T>(key: string): Promise<T | undefined> {
    return this.data.get(key) as T | undefined
  }

  async put<T>(key: string, value: T): Promise<void> {
    this.data.set(key, value)
  }

  async delete(key: string): Promise<boolean> {
    return this.data.delete(key)
  }

  async list(): Promise<Map<string, unknown>> {
    return new Map(this.data)
  }

  // Test helpers
  clear(): void {
    this.data.clear()
  }

  getData(key: string): unknown {
    return this.data.get(key)
  }

  setData(key: string, value: unknown): void {
    this.data.set(key, value)
  }
}

class MockDurableObjectState {
  storage: MockDurableObjectStorage

  constructor() {
    this.storage = new MockDurableObjectStorage()
  }

  clear(): void {
    this.storage.clear()
  }

  getData(key: string): unknown {
    return this.storage.getData(key)
  }

  setData(key: string, value: unknown): void {
    this.storage.setData(key, value)
  }
}

/**
 * Testable CompactionStateDO implementation
 * Mirrors production implementation for testing
 */
class TestableCompactionStateDO {
  private state: MockDurableObjectState
  private namespace: string = ''
  private windows: Map<string, WindowState> = new Map()
  private knownWriters: Set<string> = new Set()
  private writerLastSeen: Map<string, number> = new Map()
  private initialized = false

  private static WRITER_INACTIVE_THRESHOLD_MS = 30 * 60 * 1000

  constructor(state: MockDurableObjectState) {
    this.state = state
  }

  private async ensureInitialized(): Promise<void> {
    if (this.initialized) return

    const stored = await this.state.storage.get<StoredState>('compactionState')
    if (stored) {
      this.namespace = stored.namespace ?? ''
      for (const [key, sw] of Object.entries(stored.windows)) {
        this.windows.set(key, {
          windowStart: sw.windowStart,
          windowEnd: sw.windowEnd,
          filesByWriter: new Map(Object.entries(sw.filesByWriter)),
          writers: new Set(sw.writers),
          lastActivityAt: sw.lastActivityAt,
          totalSize: sw.totalSize,
        })
      }
      this.knownWriters = new Set(stored.knownWriters)
      this.writerLastSeen = new Map(Object.entries(stored.writerLastSeen))
    }

    this.initialized = true
  }

  private async saveState(): Promise<void> {
    const stored: StoredState = {
      namespace: this.namespace,
      windows: {},
      knownWriters: Array.from(this.knownWriters),
      writerLastSeen: Object.fromEntries(this.writerLastSeen),
    }

    for (const [key, window] of this.windows) {
      stored.windows[key] = {
        windowStart: window.windowStart,
        windowEnd: window.windowEnd,
        filesByWriter: Object.fromEntries(window.filesByWriter),
        writers: Array.from(window.writers),
        lastActivityAt: window.lastActivityAt,
        totalSize: window.totalSize,
      }
    }

    await this.state.storage.put('compactionState', stored)
  }

  async fetch(request: Request): Promise<Response> {
    await this.ensureInitialized()
    const url = new URL(request.url)

    if (url.pathname === '/update' && request.method === 'POST') {
      return this.handleUpdate(request)
    }

    if (url.pathname === '/status') {
      return this.handleStatus()
    }

    return new Response('Not Found', { status: 404 })
  }

  private async handleUpdate(request: Request): Promise<Response> {
    const body = await request.json() as UpdateRequest

    const { namespace, updates, config } = body
    const now = Date.now()
    const windowsReady: WindowReadyEntry[] = []

    // Set namespace on first update
    if (!this.namespace) {
      this.namespace = namespace
    }

    // Process updates
    for (const update of updates) {
      const { writerId, file, timestamp, size } = update

      this.knownWriters.add(writerId)
      this.writerLastSeen.set(writerId, now)

      const windowStart = Math.floor(timestamp / config.windowSizeMs) * config.windowSizeMs
      const windowEnd = windowStart + config.windowSizeMs
      const windowKey = String(windowStart)

      let window = this.windows.get(windowKey)
      if (!window) {
        window = {
          windowStart,
          windowEnd,
          filesByWriter: new Map(),
          writers: new Set(),
          lastActivityAt: now,
          totalSize: 0,
        }
        this.windows.set(windowKey, window)
      }

      const writerFiles = window.filesByWriter.get(writerId) ?? []
      writerFiles.push(file)
      window.filesByWriter.set(writerId, writerFiles)
      window.writers.add(writerId)
      window.lastActivityAt = now
      window.totalSize += size
    }

    // Check for windows ready for compaction
    const activeWriters = this.getActiveWriters(now)

    for (const [windowKey, window] of this.windows) {
      if (now < window.windowEnd + config.maxWaitTimeMs) continue

      let totalFiles = 0
      for (const files of window.filesByWriter.values()) {
        totalFiles += files.length
      }

      if (totalFiles < config.minFilesToCompact) continue

      const missingWriters = activeWriters.filter(w => !window.writers.has(w))
      const waitedLongEnough = (now - window.lastActivityAt) > config.maxWaitTimeMs

      if (missingWriters.length === 0 || waitedLongEnough) {
        const allFiles: string[] = []
        for (const files of window.filesByWriter.values()) {
          allFiles.push(...files)
        }

        windowsReady.push({
          namespace: this.namespace,
          windowStart: window.windowStart,
          windowEnd: window.windowEnd,
          files: allFiles.sort(),
          writers: Array.from(window.writers),
        })

        this.windows.delete(windowKey)
      }
    }

    await this.saveState()

    return new Response(JSON.stringify({ windowsReady }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  private handleStatus(): Response {
    const status = {
      namespace: this.namespace,
      activeWindows: this.windows.size,
      knownWriters: Array.from(this.knownWriters),
      activeWriters: this.getActiveWriters(Date.now()),
      windows: Array.from(this.windows.entries()).map(([key, w]) => ({
        key,
        windowStart: new Date(w.windowStart).toISOString(),
        windowEnd: new Date(w.windowEnd).toISOString(),
        writers: Array.from(w.writers),
        fileCount: Array.from(w.filesByWriter.values()).reduce((sum, f) => sum + f.length, 0),
        totalSize: w.totalSize,
      })),
    }

    return new Response(JSON.stringify(status, null, 2), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  private getActiveWriters(now: number): string[] {
    const active: string[] = []
    for (const [writerId, lastSeen] of this.writerLastSeen) {
      if (now - lastSeen < TestableCompactionStateDO.WRITER_INACTIVE_THRESHOLD_MS) {
        active.push(writerId)
      }
    }
    return active
  }

  // Test helpers
  getWindowCount(): number {
    return this.windows.size
  }

  getKnownWriters(): string[] {
    return Array.from(this.knownWriters)
  }

  getNamespace(): string {
    return this.namespace
  }
}

// =============================================================================
// Helper Functions
// =============================================================================

function createUpdateRequest(overrides: Partial<UpdateRequest> = {}): UpdateRequest {
  return {
    namespace: 'users',
    updates: [],
    config: {
      windowSizeMs: 3600000, // 1 hour
      minFilesToCompact: 10,
      maxWaitTimeMs: 300000, // 5 minutes
      targetFormat: 'native',
    },
    ...overrides,
  }
}

function createUpdate(overrides: Partial<UpdateRequest['updates'][0]> = {}) {
  return {
    namespace: 'users',
    writerId: 'writer1',
    file: 'data/users/1700001234-writer1-0.parquet',
    timestamp: 1700001234000,
    size: 1024,
    ...overrides,
  }
}

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

    it('should remove ready windows from tracking', async () => {
      const oldTimestamp = Date.now() - (3600000 + 400000)
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

      // Window should be removed after being marked ready
      expect(compactionDO.getWindowCount()).toBe(0)
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
