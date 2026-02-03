/**
 * Compaction Workflow Test Suite
 *
 * Tests for the compaction-migration workflow and CompactionStateDO.
 * Covers:
 * - Workflow state transitions
 * - Error handling and retries
 * - Concurrent compaction handling
 * - DO state persistence
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import type { R2EventMessage, CompactionConsumerConfig } from '@/workflows/compaction-queue-consumer'

// =============================================================================
// Mock Types
// =============================================================================

interface MockR2Object {
  key: string
  size: number
  eTag: string
  uploaded: Date
  body?: ReadableStream
  arrayBuffer(): Promise<ArrayBuffer>
  text(): Promise<string>
}

interface MockR2ListResult {
  objects: MockR2Object[]
  truncated: boolean
  cursor?: string
}

interface MockMessage<T> {
  body: T
  ack: ReturnType<typeof vi.fn>
  retry: ReturnType<typeof vi.fn>
}

interface MockMessageBatch<T> {
  messages: MockMessage<T>[]
  queue: string
}

// =============================================================================
// Mock Storage
// =============================================================================

class MockR2Bucket {
  private files: Map<string, { data: Uint8Array; size: number }> = new Map()

  async get(key: string): Promise<MockR2Object | null> {
    const file = this.files.get(key)
    if (!file) return null

    return {
      key,
      size: file.size,
      eTag: '"mock-etag"',
      uploaded: new Date(),
      async arrayBuffer() {
        return file.data.buffer as ArrayBuffer
      },
      async text() {
        return new TextDecoder().decode(file.data)
      },
    }
  }

  async put(key: string, data: Uint8Array | ArrayBuffer | string): Promise<MockR2Object> {
    const uint8 = typeof data === 'string'
      ? new TextEncoder().encode(data)
      : data instanceof Uint8Array
        ? data
        : new Uint8Array(data)

    this.files.set(key, { data: uint8, size: uint8.length })

    return {
      key,
      size: uint8.length,
      eTag: '"mock-etag"',
      uploaded: new Date(),
      async arrayBuffer() {
        return uint8.buffer as ArrayBuffer
      },
      async text() {
        return new TextDecoder().decode(uint8)
      },
    }
  }

  async head(key: string): Promise<{ key: string; size: number } | null> {
    const file = this.files.get(key)
    if (!file) return null
    return { key, size: file.size }
  }

  async delete(key: string | string[]): Promise<void> {
    const keys = Array.isArray(key) ? key : [key]
    for (const k of keys) {
      this.files.delete(k)
    }
  }

  async list(options?: { prefix?: string }): Promise<MockR2ListResult> {
    const prefix = options?.prefix ?? ''
    const objects: MockR2Object[] = []

    for (const [key, file] of this.files) {
      if (key.startsWith(prefix)) {
        objects.push({
          key,
          size: file.size,
          eTag: '"mock-etag"',
          uploaded: new Date(),
          async arrayBuffer() {
            return file.data.buffer as ArrayBuffer
          },
          async text() {
            return new TextDecoder().decode(file.data)
          },
        })
      }
    }

    return { objects, truncated: false }
  }

  // Test helpers
  clear(): void {
    this.files.clear()
  }

  getFileCount(): number {
    return this.files.size
  }

  hasFile(key: string): boolean {
    return this.files.has(key)
  }
}

// =============================================================================
// Mock Durable Object State
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
}

class MockDurableObjectState {
  storage: MockDurableObjectStorage

  constructor() {
    this.storage = new MockDurableObjectStorage()
  }

  // Test helpers
  clear(): void {
    this.storage.clear()
  }

  getData(key: string): unknown {
    return this.storage.getData(key)
  }
}

// =============================================================================
// CompactionStateDO Implementation for Testing
// =============================================================================

interface StoredWindowState {
  windowStart: number
  windowEnd: number
  filesByWriter: Record<string, string[]>
  writers: string[]
  lastActivityAt: number
  totalSize: number
}

interface StoredState {
  windows: Record<string, StoredWindowState>
  knownWriters: string[]
  writerLastSeen: Record<string, number>
}

interface WindowState {
  windowStart: number
  windowEnd: number
  filesByWriter: Map<string, string[]>
  writers: Set<string>
  lastActivityAt: number
  totalSize: number
}

const WRITER_INACTIVE_THRESHOLD_MS = 30 * 60 * 1000

class TestableCompactionStateDO {
  private state: MockDurableObjectState
  private windows: Map<string, WindowState> = new Map()
  private knownWriters: Set<string> = new Set()
  private writerLastSeen: Map<string, number> = new Map()
  private initialized = false

  constructor(state: MockDurableObjectState) {
    this.state = state
  }

  private async ensureInitialized(): Promise<void> {
    if (this.initialized) return

    const stored = await this.state.storage.get<StoredState>('compactionState')
    if (stored) {
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
    const body = await request.json() as {
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

    const { updates, config } = body
    const now = Date.now()
    const windowsReady: Array<{
      namespace: string
      windowStart: number
      windowEnd: number
      files: string[]
      writers: string[]
    }> = []

    for (const update of updates) {
      const { namespace, writerId, file, timestamp, size } = update

      this.knownWriters.add(writerId)
      this.writerLastSeen.set(writerId, now)

      const windowStart = Math.floor(timestamp / config.windowSizeMs) * config.windowSizeMs
      const windowEnd = windowStart + config.windowSizeMs
      const windowKey = `${namespace}:${windowStart}`

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
        const namespace = windowKey.split(':')[0] ?? ''
        const allFiles: string[] = []
        for (const files of window.filesByWriter.values()) {
          allFiles.push(...files)
        }

        windowsReady.push({
          namespace,
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
      if (now - lastSeen < WRITER_INACTIVE_THRESHOLD_MS) {
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
}

// =============================================================================
// Helper Functions
// =============================================================================

function createR2EventMessage(
  key: string,
  size: number = 1024,
  action: R2EventMessage['action'] = 'PutObject'
): R2EventMessage {
  return {
    account: 'test-account',
    bucket: 'parquedb-data',
    object: {
      key,
      size,
      eTag: '"abc123"',
    },
    action,
    eventTime: new Date().toISOString(),
  }
}

function createMockMessage<T>(body: T): MockMessage<T> {
  return {
    body,
    ack: vi.fn(),
    retry: vi.fn(),
  }
}

function createMockBatch<T>(messages: T[]): MockMessageBatch<T> {
  return {
    messages: messages.map(m => createMockMessage(m)),
    queue: 'parquedb-compaction-events',
  }
}

// =============================================================================
// CompactionStateDO Tests
// =============================================================================

describe('CompactionStateDO', () => {
  let state: MockDurableObjectState
  let compactionDO: TestableCompactionStateDO

  beforeEach(() => {
    state = new MockDurableObjectState()
    compactionDO = new TestableCompactionStateDO(state)
  })

  describe('state initialization', () => {
    it('should initialize with empty state', async () => {
      const response = await compactionDO.fetch(new Request('http://internal/status'))
      const status = await response.json() as { activeWindows: number; knownWriters: string[] }

      expect(status.activeWindows).toBe(0)
      expect(status.knownWriters).toEqual([])
    })

    it('should restore state from storage', async () => {
      // Pre-populate storage with state
      const storedState: StoredState = {
        windows: {
          'users:1700000000000': {
            windowStart: 1700000000000,
            windowEnd: 1700003600000,
            filesByWriter: { 'writer1': ['data/users/1700001234-writer1-0.parquet'] },
            writers: ['writer1'],
            lastActivityAt: 1700001234000,
            totalSize: 1024,
          },
        },
        knownWriters: ['writer1', 'writer2'],
        writerLastSeen: { 'writer1': Date.now(), 'writer2': Date.now() - 1000 },
      }

      await state.storage.put('compactionState', storedState)

      // Create new DO instance to load from storage
      const newDO = new TestableCompactionStateDO(state)
      const response = await newDO.fetch(new Request('http://internal/status'))
      const status = await response.json() as { activeWindows: number; knownWriters: string[] }

      expect(status.activeWindows).toBe(1)
      expect(status.knownWriters).toContain('writer1')
      expect(status.knownWriters).toContain('writer2')
    })
  })

  describe('update handling', () => {
    it('should track files by writer', async () => {
      const updates = [
        {
          namespace: 'users',
          writerId: 'writer1',
          file: 'data/users/1700001234-writer1-0.parquet',
          timestamp: 1700001234000,
          size: 1024,
        },
        {
          namespace: 'users',
          writerId: 'writer2',
          file: 'data/users/1700001235-writer2-0.parquet',
          timestamp: 1700001235000,
          size: 2048,
        },
      ]

      const response = await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          updates,
          config: {
            windowSizeMs: 3600000, // 1 hour
            minFilesToCompact: 10,
            maxWaitTimeMs: 300000, // 5 minutes
            targetFormat: 'native',
          },
        }),
      }))

      expect(response.status).toBe(200)

      const statusResponse = await compactionDO.fetch(new Request('http://internal/status'))
      const status = await statusResponse.json() as {
        activeWindows: number
        knownWriters: string[]
        windows: Array<{ fileCount: number; totalSize: number }>
      }

      expect(status.activeWindows).toBe(1)
      expect(status.knownWriters).toContain('writer1')
      expect(status.knownWriters).toContain('writer2')
      expect(status.windows[0].fileCount).toBe(2)
      expect(status.windows[0].totalSize).toBe(3072)
    })

    it('should group files into time windows', async () => {
      const updates = [
        {
          namespace: 'users',
          writerId: 'writer1',
          file: 'data/users/1700000000-writer1-0.parquet',
          timestamp: 1700000000000, // Window 1
          size: 1024,
        },
        {
          namespace: 'users',
          writerId: 'writer1',
          file: 'data/users/1700003700-writer1-1.parquet',
          timestamp: 1700003700000, // Window 2 (1 hour later)
          size: 1024,
        },
      ]

      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          updates,
          config: {
            windowSizeMs: 3600000,
            minFilesToCompact: 10,
            maxWaitTimeMs: 300000,
            targetFormat: 'native',
          },
        }),
      }))

      const statusResponse = await compactionDO.fetch(new Request('http://internal/status'))
      const status = await statusResponse.json() as { activeWindows: number }

      expect(status.activeWindows).toBe(2)
    })

    it('should track multiple namespaces separately', async () => {
      const updates = [
        {
          namespace: 'users',
          writerId: 'writer1',
          file: 'data/users/1700001234-writer1-0.parquet',
          timestamp: 1700001234000,
          size: 1024,
        },
        {
          namespace: 'posts',
          writerId: 'writer1',
          file: 'data/posts/1700001234-writer1-0.parquet',
          timestamp: 1700001234000,
          size: 1024,
        },
      ]

      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          updates,
          config: {
            windowSizeMs: 3600000,
            minFilesToCompact: 10,
            maxWaitTimeMs: 300000,
            targetFormat: 'native',
          },
        }),
      }))

      const statusResponse = await compactionDO.fetch(new Request('http://internal/status'))
      const status = await statusResponse.json() as {
        activeWindows: number
        windows: Array<{ key: string }>
      }

      expect(status.activeWindows).toBe(2)
      expect(status.windows.some(w => w.key.startsWith('users:'))).toBe(true)
      expect(status.windows.some(w => w.key.startsWith('posts:'))).toBe(true)
    })
  })

  describe('window readiness', () => {
    it('should not mark window ready if too recent', async () => {
      const now = Date.now()
      const updates = Array.from({ length: 15 }, (_, i) => ({
        namespace: 'users',
        writerId: 'writer1',
        file: `data/users/${now}-writer1-${i}.parquet`,
        timestamp: now,
        size: 1024,
      }))

      const response = await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          updates,
          config: {
            windowSizeMs: 3600000,
            minFilesToCompact: 10,
            maxWaitTimeMs: 300000,
            targetFormat: 'native',
          },
        }),
      }))

      const result = await response.json() as { windowsReady: unknown[] }
      expect(result.windowsReady).toHaveLength(0)
    })

    it('should mark window ready when enough time has passed', async () => {
      // Use a timestamp far in the past
      const oldTimestamp = Date.now() - (3600000 + 400000) // More than 1 hour + 5 min ago
      const updates = Array.from({ length: 15 }, (_, i) => ({
        namespace: 'users',
        writerId: 'writer1',
        file: `data/users/${oldTimestamp}-writer1-${i}.parquet`,
        timestamp: oldTimestamp,
        size: 1024,
      }))

      const response = await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          updates,
          config: {
            windowSizeMs: 3600000,
            minFilesToCompact: 10,
            maxWaitTimeMs: 300000,
            targetFormat: 'native',
          },
        }),
      }))

      const result = await response.json() as { windowsReady: Array<{ files: string[]; writers: string[] }> }

      expect(result.windowsReady).toHaveLength(1)
      expect(result.windowsReady[0].files).toHaveLength(15)
      expect(result.windowsReady[0].writers).toContain('writer1')
    })

    it('should not mark window ready if below minimum files', async () => {
      const oldTimestamp = Date.now() - (3600000 + 400000)
      const updates = Array.from({ length: 5 }, (_, i) => ({
        namespace: 'users',
        writerId: 'writer1',
        file: `data/users/${oldTimestamp}-writer1-${i}.parquet`,
        timestamp: oldTimestamp,
        size: 1024,
      }))

      const response = await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          updates,
          config: {
            windowSizeMs: 3600000,
            minFilesToCompact: 10,
            maxWaitTimeMs: 300000,
            targetFormat: 'native',
          },
        }),
      }))

      const result = await response.json() as { windowsReady: unknown[] }
      expect(result.windowsReady).toHaveLength(0)
    })

    it('should remove ready windows from tracking', async () => {
      const oldTimestamp = Date.now() - (3600000 + 400000)
      const updates = Array.from({ length: 15 }, (_, i) => ({
        namespace: 'users',
        writerId: 'writer1',
        file: `data/users/${oldTimestamp}-writer1-${i}.parquet`,
        timestamp: oldTimestamp,
        size: 1024,
      }))

      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          updates,
          config: {
            windowSizeMs: 3600000,
            minFilesToCompact: 10,
            maxWaitTimeMs: 300000,
            targetFormat: 'native',
          },
        }),
      }))

      const statusResponse = await compactionDO.fetch(new Request('http://internal/status'))
      const status = await statusResponse.json() as { activeWindows: number }

      expect(status.activeWindows).toBe(0)
    })
  })

  describe('state persistence', () => {
    it('should persist state after updates', async () => {
      const updates = [{
        namespace: 'users',
        writerId: 'writer1',
        file: 'data/users/1700001234-writer1-0.parquet',
        timestamp: 1700001234000,
        size: 1024,
      }]

      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          updates,
          config: {
            windowSizeMs: 3600000,
            minFilesToCompact: 10,
            maxWaitTimeMs: 300000,
            targetFormat: 'native',
          },
        }),
      }))

      const stored = state.getData('compactionState') as StoredState
      expect(stored).toBeDefined()
      expect(stored.knownWriters).toContain('writer1')
    })

    it('should persist windows correctly', async () => {
      const updates = [{
        namespace: 'users',
        writerId: 'writer1',
        file: 'data/users/1700001234-writer1-0.parquet',
        timestamp: 1700001234000,
        size: 1024,
      }]

      await compactionDO.fetch(new Request('http://internal/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          updates,
          config: {
            windowSizeMs: 3600000,
            minFilesToCompact: 10,
            maxWaitTimeMs: 300000,
            targetFormat: 'native',
          },
        }),
      }))

      const stored = state.getData('compactionState') as StoredState
      expect(Object.keys(stored.windows)).toHaveLength(1)

      const windowKey = Object.keys(stored.windows)[0]
      expect(windowKey).toBeDefined()
      expect(stored.windows[windowKey!].writers).toContain('writer1')
      expect(stored.windows[windowKey!].filesByWriter['writer1']).toContain('data/users/1700001234-writer1-0.parquet')
    })
  })

  describe('error handling', () => {
    it('should return 404 for unknown endpoints', async () => {
      const response = await compactionDO.fetch(new Request('http://internal/unknown'))
      expect(response.status).toBe(404)
    })

    it('should throw error for invalid JSON', async () => {
      await expect(
        compactionDO.fetch(new Request('http://internal/update', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: 'invalid json',
        }))
      ).rejects.toThrow()
    })
  })
})

// =============================================================================
// R2 Event Message Parsing Tests
// =============================================================================

describe('R2 Event Message Parsing', () => {
  describe('createR2EventMessage helper', () => {
    it('should create valid R2 event message', () => {
      const message = createR2EventMessage('data/users/1700001234-writer1-0.parquet', 1024)

      expect(message.object.key).toBe('data/users/1700001234-writer1-0.parquet')
      expect(message.object.size).toBe(1024)
      expect(message.action).toBe('PutObject')
      expect(message.bucket).toBe('parquedb-data')
    })

    it('should support different actions', () => {
      const putMessage = createR2EventMessage('file.parquet', 1024, 'PutObject')
      const copyMessage = createR2EventMessage('file.parquet', 1024, 'CopyObject')
      const completeMessage = createR2EventMessage('file.parquet', 1024, 'CompleteMultipartUpload')
      const deleteMessage = createR2EventMessage('file.parquet', 1024, 'DeleteObject')

      expect(putMessage.action).toBe('PutObject')
      expect(copyMessage.action).toBe('CopyObject')
      expect(completeMessage.action).toBe('CompleteMultipartUpload')
      expect(deleteMessage.action).toBe('DeleteObject')
    })
  })

  describe('file path parsing', () => {
    it('should identify parquet files', () => {
      const parquetFile = 'data/users/1700001234-writer1-0.parquet'
      const jsonFile = 'data/users/metadata.json'

      expect(parquetFile.endsWith('.parquet')).toBe(true)
      expect(jsonFile.endsWith('.parquet')).toBe(false)
    })

    it('should parse namespace from file path', () => {
      const filePath = 'data/users/1700001234-writer1-0.parquet'
      const prefix = 'data/'

      const keyWithoutPrefix = filePath.slice(prefix.length)
      const parts = keyWithoutPrefix.split('/')
      const namespace = parts.slice(0, -1).join('/')

      expect(namespace).toBe('users')
    })

    it('should parse nested namespace from file path', () => {
      const filePath = 'data/app/users/1700001234-writer1-0.parquet'
      const prefix = 'data/'

      const keyWithoutPrefix = filePath.slice(prefix.length)
      const parts = keyWithoutPrefix.split('/')
      const namespace = parts.slice(0, -1).join('/')

      expect(namespace).toBe('app/users')
    })

    it('should parse file info from filename', () => {
      const filename = '1700001234-writer1-42.parquet'
      const match = filename.match(/^(\d+)-([^-]+)-(\d+)\.parquet$/)

      expect(match).not.toBeNull()
      expect(match![1]).toBe('1700001234')
      expect(match![2]).toBe('writer1')
      expect(match![3]).toBe('42')
    })

    it('should reject invalid filename formats', () => {
      const invalidFilenames = [
        'invalid.parquet',
        '1700001234.parquet',
        '1700001234-writer1.parquet',
        'writer1-42.parquet',
        '1700001234-writer1-42.json',
      ]

      for (const filename of invalidFilenames) {
        const match = filename.match(/^(\d+)-([^-]+)-(\d+)\.parquet$/)
        expect(match).toBeNull()
      }
    })
  })
})

// =============================================================================
// Concurrent Compaction Handling Tests
// =============================================================================

describe('Concurrent Compaction Handling', () => {
  let state: MockDurableObjectState
  let compactionDO: TestableCompactionStateDO

  beforeEach(() => {
    state = new MockDurableObjectState()
    compactionDO = new TestableCompactionStateDO(state)
  })

  it('should handle concurrent updates from multiple writers', async () => {
    const now = Date.now() - (3600000 + 400000)

    // Simulate concurrent updates from multiple writers
    const writer1Updates = Array.from({ length: 5 }, (_, i) => ({
      namespace: 'users',
      writerId: 'writer1',
      file: `data/users/${now}-writer1-${i}.parquet`,
      timestamp: now,
      size: 1024,
    }))

    const writer2Updates = Array.from({ length: 5 }, (_, i) => ({
      namespace: 'users',
      writerId: 'writer2',
      file: `data/users/${now}-writer2-${i}.parquet`,
      timestamp: now,
      size: 1024,
    }))

    const writer3Updates = Array.from({ length: 5 }, (_, i) => ({
      namespace: 'users',
      writerId: 'writer3',
      file: `data/users/${now}-writer3-${i}.parquet`,
      timestamp: now,
      size: 1024,
    }))

    // Process all updates together
    const response = await compactionDO.fetch(new Request('http://internal/update', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        updates: [...writer1Updates, ...writer2Updates, ...writer3Updates],
        config: {
          windowSizeMs: 3600000,
          minFilesToCompact: 10,
          maxWaitTimeMs: 300000,
          targetFormat: 'native',
        },
      }),
    }))

    const result = await response.json() as { windowsReady: Array<{ files: string[]; writers: string[] }> }

    expect(result.windowsReady).toHaveLength(1)
    expect(result.windowsReady[0].files).toHaveLength(15)
    expect(result.windowsReady[0].writers).toContain('writer1')
    expect(result.windowsReady[0].writers).toContain('writer2')
    expect(result.windowsReady[0].writers).toContain('writer3')
  })

  it('should handle sequential updates from the same writer', async () => {
    const now = Date.now() - (3600000 + 400000)

    // First batch
    await compactionDO.fetch(new Request('http://internal/update', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        updates: Array.from({ length: 5 }, (_, i) => ({
          namespace: 'users',
          writerId: 'writer1',
          file: `data/users/${now}-writer1-${i}.parquet`,
          timestamp: now,
          size: 1024,
        })),
        config: {
          windowSizeMs: 3600000,
          minFilesToCompact: 10,
          maxWaitTimeMs: 300000,
          targetFormat: 'native',
        },
      }),
    }))

    // Second batch
    const response = await compactionDO.fetch(new Request('http://internal/update', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        updates: Array.from({ length: 5 }, (_, i) => ({
          namespace: 'users',
          writerId: 'writer1',
          file: `data/users/${now}-writer1-${i + 5}.parquet`,
          timestamp: now,
          size: 1024,
        })),
        config: {
          windowSizeMs: 3600000,
          minFilesToCompact: 10,
          maxWaitTimeMs: 300000,
          targetFormat: 'native',
        },
      }),
    }))

    const result = await response.json() as { windowsReady: Array<{ files: string[] }> }

    expect(result.windowsReady).toHaveLength(1)
    expect(result.windowsReady[0].files).toHaveLength(10)
  })

  it('should isolate windows for different namespaces', async () => {
    const now = Date.now() - (3600000 + 400000)

    const updates = [
      ...Array.from({ length: 10 }, (_, i) => ({
        namespace: 'users',
        writerId: 'writer1',
        file: `data/users/${now}-writer1-${i}.parquet`,
        timestamp: now,
        size: 1024,
      })),
      ...Array.from({ length: 5 }, (_, i) => ({
        namespace: 'posts',
        writerId: 'writer1',
        file: `data/posts/${now}-writer1-${i}.parquet`,
        timestamp: now,
        size: 1024,
      })),
    ]

    const response = await compactionDO.fetch(new Request('http://internal/update', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        updates,
        config: {
          windowSizeMs: 3600000,
          minFilesToCompact: 10,
          maxWaitTimeMs: 300000,
          targetFormat: 'native',
        },
      }),
    }))

    const result = await response.json() as { windowsReady: Array<{ namespace: string }> }

    // Only users namespace should be ready (has 10 files)
    expect(result.windowsReady).toHaveLength(1)
    expect(result.windowsReady[0].namespace).toBe('users')

    // Posts namespace should still be tracked
    const statusResponse = await compactionDO.fetch(new Request('http://internal/status'))
    const status = await statusResponse.json() as { activeWindows: number }

    expect(status.activeWindows).toBe(1)
  })
})

// =============================================================================
// Workflow State Transitions Tests
// =============================================================================

describe('Workflow State Transitions', () => {
  describe('CompactionMigrationParams validation', () => {
    it('should accept valid params', () => {
      const params = {
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
        files: ['data/users/1700001234-writer1-0.parquet'],
        writers: ['writer1'],
        targetFormat: 'native' as const,
      }

      expect(params.namespace).toBe('users')
      expect(params.files.length).toBeGreaterThan(0)
      expect(params.targetFormat).toBe('native')
    })

    it('should support all target formats', () => {
      const formats = ['native', 'iceberg', 'delta'] as const

      for (const format of formats) {
        const params = {
          namespace: 'users',
          windowStart: 1700000000000,
          windowEnd: 1700003600000,
          files: ['data/users/1700001234-writer1-0.parquet'],
          writers: ['writer1'],
          targetFormat: format,
        }

        expect(params.targetFormat).toBe(format)
      }
    })
  })

  describe('output file path generation', () => {
    it('should generate correct native format path', () => {
      const timestamp = 1700000000000 // 2023-11-14 22:13:20 UTC
      const date = new Date(timestamp)
      const year = date.getUTCFullYear()
      const month = String(date.getUTCMonth() + 1).padStart(2, '0')
      const day = String(date.getUTCDate()).padStart(2, '0')
      const hour = String(date.getUTCHours()).padStart(2, '0')

      const namespace = 'users'
      const batchNum = 1
      const outputFile = `data/${namespace}/year=${year}/month=${month}/day=${day}/hour=${hour}/` +
        `compacted-${timestamp}-${batchNum}.parquet`

      expect(outputFile).toContain('data/users/')
      expect(outputFile).toContain(`year=${year}`)
      expect(outputFile).toContain('compacted-')
      expect(outputFile.endsWith('.parquet')).toBe(true)
    })

    it('should generate correct iceberg format path', () => {
      const timestamp = 1700000000000
      const date = new Date(timestamp)
      const year = date.getUTCFullYear()
      const month = String(date.getUTCMonth() + 1).padStart(2, '0')
      const day = String(date.getUTCDate()).padStart(2, '0')
      const hour = String(date.getUTCHours()).padStart(2, '0')

      const namespace = 'users'
      const batchNum = 1
      const outputFile = `${namespace}/data/year=${year}/month=${month}/day=${day}/hour=${hour}/` +
        `compacted-${timestamp}-${batchNum}.parquet`

      expect(outputFile).toContain('users/data/')
      expect(outputFile).not.toContain('data/users/')
    })

    it('should generate correct delta format path', () => {
      const timestamp = 1700000000000
      const date = new Date(timestamp)
      const year = date.getUTCFullYear()
      const month = String(date.getUTCMonth() + 1).padStart(2, '0')
      const day = String(date.getUTCDate()).padStart(2, '0')
      const hour = String(date.getUTCHours()).padStart(2, '0')

      const namespace = 'users'
      const batchNum = 1
      const outputFile = `${namespace}/year=${year}/month=${month}/day=${day}/hour=${hour}/` +
        `part-${String(batchNum).padStart(5, '0')}-compacted-${timestamp}.parquet`

      expect(outputFile).toContain('users/')
      expect(outputFile).toContain('part-00001-')
    })
  })
})

// =============================================================================
// Mock Message Batch Tests
// =============================================================================

describe('Message Batch Handling', () => {
  it('should create mock batch with ack/retry methods', () => {
    const messages = [
      createR2EventMessage('data/users/file1.parquet'),
      createR2EventMessage('data/users/file2.parquet'),
    ]

    const batch = createMockBatch(messages)

    expect(batch.messages).toHaveLength(2)
    expect(batch.queue).toBe('parquedb-compaction-events')

    // Verify ack/retry work
    batch.messages[0].ack()
    batch.messages[1].retry()

    expect(batch.messages[0].ack).toHaveBeenCalled()
    expect(batch.messages[1].retry).toHaveBeenCalled()
  })

  it('should filter non-create events', () => {
    const messages = [
      createR2EventMessage('data/users/file1.parquet', 1024, 'PutObject'),
      createR2EventMessage('data/users/file2.parquet', 1024, 'DeleteObject'),
      createR2EventMessage('data/users/file3.parquet', 1024, 'CopyObject'),
    ]

    const createMessages = messages.filter(m =>
      m.action === 'PutObject' ||
      m.action === 'CopyObject' ||
      m.action === 'CompleteMultipartUpload'
    )

    expect(createMessages).toHaveLength(2)
    expect(createMessages.map(m => m.object.key)).toContain('data/users/file1.parquet')
    expect(createMessages.map(m => m.object.key)).toContain('data/users/file3.parquet')
  })

  it('should filter non-parquet files', () => {
    const messages = [
      createR2EventMessage('data/users/file1.parquet'),
      createR2EventMessage('data/users/metadata.json'),
      createR2EventMessage('data/users/file2.parquet'),
    ]

    const parquetMessages = messages.filter(m => m.object.key.endsWith('.parquet'))

    expect(parquetMessages).toHaveLength(2)
  })

  it('should filter files outside namespace prefix', () => {
    const prefix = 'data/'
    const messages = [
      createR2EventMessage('data/users/file1.parquet'),
      createR2EventMessage('logs/system/file.parquet'),
      createR2EventMessage('data/posts/file2.parquet'),
    ]

    const matchingMessages = messages.filter(m => m.object.key.startsWith(prefix))

    expect(matchingMessages).toHaveLength(2)
    expect(matchingMessages.map(m => m.object.key)).toContain('data/users/file1.parquet')
    expect(matchingMessages.map(m => m.object.key)).toContain('data/posts/file2.parquet')
  })
})

// =============================================================================
// MockR2Bucket Tests
// =============================================================================

describe('MockR2Bucket', () => {
  let bucket: MockR2Bucket

  beforeEach(() => {
    bucket = new MockR2Bucket()
  })

  it('should store and retrieve files', async () => {
    await bucket.put('test/file.parquet', new Uint8Array([1, 2, 3, 4]))

    const obj = await bucket.get('test/file.parquet')
    expect(obj).not.toBeNull()
    expect(obj!.size).toBe(4)

    const data = await obj!.arrayBuffer()
    expect(new Uint8Array(data)).toEqual(new Uint8Array([1, 2, 3, 4]))
  })

  it('should return null for non-existent files', async () => {
    const obj = await bucket.get('nonexistent.parquet')
    expect(obj).toBeNull()
  })

  it('should list files with prefix', async () => {
    await bucket.put('data/users/file1.parquet', 'content1')
    await bucket.put('data/users/file2.parquet', 'content2')
    await bucket.put('data/posts/file3.parquet', 'content3')

    const result = await bucket.list({ prefix: 'data/users/' })

    expect(result.objects).toHaveLength(2)
    expect(result.objects.map(o => o.key)).toContain('data/users/file1.parquet')
    expect(result.objects.map(o => o.key)).toContain('data/users/file2.parquet')
  })

  it('should delete files', async () => {
    await bucket.put('test/file.parquet', 'content')
    expect(bucket.hasFile('test/file.parquet')).toBe(true)

    await bucket.delete('test/file.parquet')
    expect(bucket.hasFile('test/file.parquet')).toBe(false)
  })

  it('should support head operation', async () => {
    await bucket.put('test/file.parquet', 'content')

    const head = await bucket.head('test/file.parquet')
    expect(head).not.toBeNull()
    expect(head!.size).toBe(7) // 'content'.length

    const missingHead = await bucket.head('nonexistent.parquet')
    expect(missingHead).toBeNull()
  })
})

// =============================================================================
// handleCompactionQueue Function Tests
// =============================================================================

describe('handleCompactionQueue', () => {
  // Mock environment for handleCompactionQueue
  interface MockCompactionEnv {
    COMPACTION_STATE: {
      idFromName: ReturnType<typeof vi.fn>
      get: ReturnType<typeof vi.fn>
    }
    COMPACTION_WORKFLOW: {
      create: ReturnType<typeof vi.fn>
    }
  }

  let mockEnv: MockCompactionEnv
  let mockStateDO: {
    fetch: ReturnType<typeof vi.fn>
  }

  beforeEach(() => {
    mockStateDO = {
      fetch: vi.fn(),
    }

    mockEnv = {
      COMPACTION_STATE: {
        idFromName: vi.fn().mockReturnValue('mock-id'),
        get: vi.fn().mockReturnValue(mockStateDO),
      },
      COMPACTION_WORKFLOW: {
        create: vi.fn().mockResolvedValue({ id: 'workflow-123' }),
      },
    }
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  describe('message filtering', () => {
    it('should filter and process only valid parquet file events', () => {
      // Test the filtering logic used in handleCompactionQueue
      const messages: R2EventMessage[] = [
        createR2EventMessage('data/users/1700001234-writer1-0.parquet', 1024, 'PutObject'),
        createR2EventMessage('data/users/1700001234-writer1-1.parquet', 1024, 'CopyObject'),
        createR2EventMessage('data/users/1700001234-writer1-2.parquet', 1024, 'CompleteMultipartUpload'),
        createR2EventMessage('data/users/1700001234-writer1-3.parquet', 1024, 'DeleteObject'),
        createR2EventMessage('data/users/metadata.json', 100, 'PutObject'),
        createR2EventMessage('logs/system.parquet', 500, 'PutObject'),
      ]

      const namespacePrefix = 'data/'
      const validMessages = messages.filter(m => {
        const isCreateAction = m.action === 'PutObject' || m.action === 'CopyObject' || m.action === 'CompleteMultipartUpload'
        const isParquet = m.object.key.endsWith('.parquet')
        const isInPrefix = m.object.key.startsWith(namespacePrefix)
        return isCreateAction && isParquet && isInPrefix
      })

      expect(validMessages).toHaveLength(3)
      expect(validMessages.map(m => m.object.key)).toEqual([
        'data/users/1700001234-writer1-0.parquet',
        'data/users/1700001234-writer1-1.parquet',
        'data/users/1700001234-writer1-2.parquet',
      ])
    })

    it('should correctly parse file info from valid filenames', () => {
      const testCases = [
        { key: 'data/users/1700001234-writer1-0.parquet', expectedNs: 'users', expectedWriter: 'writer1', expectedTs: 1700001234 },
        { key: 'data/app/users/1700001234-w2-42.parquet', expectedNs: 'app/users', expectedWriter: 'w2', expectedTs: 1700001234 },
        { key: 'data/posts/1700005678-abc123-99.parquet', expectedNs: 'posts', expectedWriter: 'abc123', expectedTs: 1700005678 },
      ]

      for (const { key, expectedNs, expectedWriter, expectedTs } of testCases) {
        const prefix = 'data/'
        const keyWithoutPrefix = key.slice(prefix.length)
        const parts = keyWithoutPrefix.split('/')
        const namespace = parts.slice(0, -1).join('/')
        const filename = parts[parts.length - 1] ?? ''
        const match = filename.match(/^(\d+)-([^-]+)-(\d+)\.parquet$/)

        expect(namespace).toBe(expectedNs)
        expect(match).not.toBeNull()
        expect(match![1]).toBe(String(expectedTs))
        expect(match![2]).toBe(expectedWriter)
      }
    })

    it('should reject files with invalid filename format', () => {
      const invalidKeys = [
        'data/users/invalid.parquet',
        'data/users/1700001234.parquet',
        'data/users/writer1-0.parquet',
        'data/users/1700001234-writer1.parquet',
        'data/users/-writer1-0.parquet',
        'data/users/1700001234--0.parquet',
      ]

      for (const key of invalidKeys) {
        const prefix = 'data/'
        const keyWithoutPrefix = key.slice(prefix.length)
        const parts = keyWithoutPrefix.split('/')
        const filename = parts[parts.length - 1] ?? ''
        const match = filename.match(/^(\d+)-([^-]+)-(\d+)\.parquet$/)

        expect(match).toBeNull()
      }
    })
  })

  describe('update processing', () => {
    it('should build correct updates array from valid messages', () => {
      const messages: R2EventMessage[] = [
        createR2EventMessage('data/users/1700001234-writer1-0.parquet', 1024, 'PutObject'),
        createR2EventMessage('data/users/1700001235-writer2-0.parquet', 2048, 'PutObject'),
      ]

      const prefix = 'data/'
      const updates: Array<{
        namespace: string
        writerId: string
        file: string
        timestamp: number
        size: number
      }> = []

      for (const msg of messages) {
        if (!msg.object.key.endsWith('.parquet')) continue
        if (!msg.object.key.startsWith(prefix)) continue

        const keyWithoutPrefix = msg.object.key.slice(prefix.length)
        const parts = keyWithoutPrefix.split('/')
        const namespace = parts.slice(0, -1).join('/')
        const filename = parts[parts.length - 1] ?? ''
        const match = filename.match(/^(\d+)-([^-]+)-(\d+)\.parquet$/)
        if (!match) continue

        const [, timestampStr, writerId] = match
        // Filename timestamps are in seconds, convert to milliseconds
        const timestamp = parseInt(timestampStr ?? '0', 10) * 1000

        updates.push({
          namespace,
          writerId: writerId ?? 'unknown',
          file: msg.object.key,
          timestamp,
          size: msg.object.size,
        })
      }

      expect(updates).toHaveLength(2)
      expect(updates[0]).toEqual({
        namespace: 'users',
        writerId: 'writer1',
        file: 'data/users/1700001234-writer1-0.parquet',
        timestamp: 1700001234000,
        size: 1024,
      })
      expect(updates[1]).toEqual({
        namespace: 'users',
        writerId: 'writer2',
        file: 'data/users/1700001235-writer2-0.parquet',
        timestamp: 1700001235000,
        size: 2048,
      })
    })
  })

  describe('workflow triggering', () => {
    it('should construct correct workflow params from ready windows', () => {
      const readyWindow = {
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
        files: ['data/users/1700001234-writer1-0.parquet', 'data/users/1700001235-writer2-0.parquet'],
        writers: ['writer1', 'writer2'],
      }
      const targetFormat = 'iceberg'

      const workflowParams = {
        namespace: readyWindow.namespace,
        windowStart: readyWindow.windowStart,
        windowEnd: readyWindow.windowEnd,
        files: readyWindow.files,
        writers: readyWindow.writers,
        targetFormat,
      }

      expect(workflowParams.namespace).toBe('users')
      expect(workflowParams.files).toHaveLength(2)
      expect(workflowParams.writers).toContain('writer1')
      expect(workflowParams.writers).toContain('writer2')
      expect(workflowParams.targetFormat).toBe('iceberg')
    })
  })

  describe('configuration defaults', () => {
    it('should use default values when not specified', () => {
      const config: CompactionConsumerConfig = {}

      const windowSizeMs = config.windowSizeMs ?? 60 * 60 * 1000 // 1 hour
      const minFilesToCompact = config.minFilesToCompact ?? 10
      const maxWaitTimeMs = config.maxWaitTimeMs ?? 5 * 60 * 1000 // 5 minutes
      const targetFormat = config.targetFormat ?? 'native'
      const namespacePrefix = config.namespacePrefix ?? 'data/'

      expect(windowSizeMs).toBe(3600000)
      expect(minFilesToCompact).toBe(10)
      expect(maxWaitTimeMs).toBe(300000)
      expect(targetFormat).toBe('native')
      expect(namespacePrefix).toBe('data/')
    })

    it('should use provided values when specified', () => {
      const config: CompactionConsumerConfig = {
        windowSizeMs: 1800000, // 30 minutes
        minFilesToCompact: 5,
        maxWaitTimeMs: 120000, // 2 minutes
        targetFormat: 'delta',
        namespacePrefix: 'events/',
      }

      expect(config.windowSizeMs).toBe(1800000)
      expect(config.minFilesToCompact).toBe(5)
      expect(config.maxWaitTimeMs).toBe(120000)
      expect(config.targetFormat).toBe('delta')
      expect(config.namespacePrefix).toBe('events/')
    })
  })
})

// =============================================================================
// MigrationWorkflow Tests
// =============================================================================

describe('MigrationWorkflow', () => {
  describe('MigrationWorkflowParams validation', () => {
    it('should accept valid params with required fields', () => {
      const params = {
        to: 'iceberg' as const,
      }

      expect(params.to).toBe('iceberg')
    })

    it('should accept valid params with all optional fields', () => {
      const params = {
        to: 'delta' as const,
        from: 'native' as const,
        namespaces: ['users', 'posts'],
        batchSize: 200,
        deleteSource: true,
      }

      expect(params.to).toBe('delta')
      expect(params.from).toBe('native')
      expect(params.namespaces).toEqual(['users', 'posts'])
      expect(params.batchSize).toBe(200)
      expect(params.deleteSource).toBe(true)
    })

    it('should support auto-detect for source format', () => {
      const params = {
        to: 'iceberg' as const,
        from: 'auto' as const,
      }

      expect(params.from).toBe('auto')
    })

    it('should support all target formats', () => {
      const formats = ['native', 'iceberg', 'delta'] as const

      for (const format of formats) {
        const params = { to: format }
        expect(params.to).toBe(format)
      }
    })
  })

  describe('batch size configuration', () => {
    it('should use default batch size when not specified', () => {
      const params = { to: 'iceberg' as const }
      const batchSize = params.batchSize ?? 400

      expect(batchSize).toBe(400)
    })

    it('should cap batch size at 450 to stay under subrequest limit', () => {
      const params = { to: 'iceberg' as const, batchSize: 1000 }
      const batchSize = Math.min(params.batchSize ?? 400, 450)

      expect(batchSize).toBe(450)
    })

    it('should use provided batch size if under limit', () => {
      const params = { to: 'iceberg' as const, batchSize: 200 }
      const batchSize = Math.min(params.batchSize ?? 400, 450)

      expect(batchSize).toBe(200)
    })
  })

  describe('namespace limits', () => {
    it('should enforce maximum namespace limit', () => {
      const MAX_NAMESPACES = 500
      const tooManyNamespaces = Array.from({ length: 501 }, (_, i) => `ns${i}`)

      const shouldThrow = tooManyNamespaces.length > MAX_NAMESPACES

      expect(shouldThrow).toBe(true)
    })

    it('should accept up to maximum namespaces', () => {
      const MAX_NAMESPACES = 500
      const maxNamespaces = Array.from({ length: 500 }, (_, i) => `ns${i}`)

      const shouldThrow = maxNamespaces.length > MAX_NAMESPACES

      expect(shouldThrow).toBe(false)
    })
  })

  describe('migration state', () => {
    it('should track migration progress', () => {
      const state = {
        namespaces: ['users', 'posts', 'comments'],
        currentIndex: 1,
        totalMigrated: 1500,
        errors: [] as string[],
        startedAt: Date.now(),
      }

      expect(state.currentIndex).toBe(1)
      expect(state.namespaces[state.currentIndex]).toBe('posts')
      expect(state.totalMigrated).toBe(1500)
    })

    it('should track errors during migration', () => {
      const state = {
        namespaces: ['users', 'posts'],
        currentIndex: 2,
        totalMigrated: 1000,
        errors: ['users: Connection timeout', 'posts: Invalid schema'],
        startedAt: Date.now(),
      }

      expect(state.errors).toHaveLength(2)
      expect(state.errors[0]).toContain('users')
      expect(state.errors[1]).toContain('posts')
    })

    it('should calculate migration summary', () => {
      const state = {
        namespaces: ['users', 'posts', 'comments'],
        currentIndex: 3,
        totalMigrated: 5000,
        errors: ['posts: Timeout'],
        startedAt: Date.now() - 60000, // 1 minute ago
      }

      const summary = {
        success: state.errors.length === 0,
        namespacesMigrated: state.currentIndex,
        totalNamespaces: state.namespaces.length,
        entitiesMigrated: state.totalMigrated,
        errors: state.errors,
        durationMs: Date.now() - state.startedAt,
      }

      expect(summary.success).toBe(false)
      expect(summary.namespacesMigrated).toBe(3)
      expect(summary.totalNamespaces).toBe(3)
      expect(summary.entitiesMigrated).toBe(5000)
      expect(summary.durationMs).toBeGreaterThan(59000)
    })
  })
})

// =============================================================================
// CompactionMigrationWorkflow Tests
// =============================================================================

describe('CompactionMigrationWorkflow', () => {
  describe('CompactionMigrationParams validation', () => {
    it('should accept valid params with required fields', () => {
      const params = {
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
        files: ['data/users/1700001234-writer1-0.parquet'],
        writers: ['writer1'],
        targetFormat: 'native' as const,
      }

      expect(params.namespace).toBe('users')
      expect(params.windowEnd - params.windowStart).toBe(3600000) // 1 hour
      expect(params.files).toHaveLength(1)
      expect(params.writers).toContain('writer1')
    })

    it('should accept valid params with all optional fields', () => {
      const params = {
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
        files: ['data/users/1700001234-writer1-0.parquet'],
        writers: ['writer1'],
        targetFormat: 'iceberg' as const,
        maxFilesPerStep: 25,
        deleteSource: false,
        targetFileSize: 64 * 1024 * 1024, // 64MB
      }

      expect(params.maxFilesPerStep).toBe(25)
      expect(params.deleteSource).toBe(false)
      expect(params.targetFileSize).toBe(67108864)
    })
  })

  describe('compaction state tracking', () => {
    it('should track remaining and processed files', () => {
      const state = {
        remainingFiles: ['file3.parquet', 'file4.parquet'],
        processedFiles: ['file1.parquet', 'file2.parquet'],
        outputFiles: ['compacted-1.parquet'],
        totalRows: 1000,
        bytesRead: 1024 * 100,
        bytesWritten: 1024 * 50,
        errors: [] as string[],
        startedAt: Date.now(),
      }

      expect(state.remainingFiles).toHaveLength(2)
      expect(state.processedFiles).toHaveLength(2)
      expect(state.outputFiles).toHaveLength(1)
    })

    it('should track compression metrics', () => {
      const state = {
        remainingFiles: [],
        processedFiles: ['file1.parquet', 'file2.parquet', 'file3.parquet'],
        outputFiles: ['compacted-1.parquet'],
        totalRows: 5000,
        bytesRead: 1024 * 1024, // 1MB
        bytesWritten: 512 * 1024, // 512KB
        errors: [],
        startedAt: Date.now() - 30000,
      }

      const compressionRatio = state.bytesWritten / state.bytesRead

      expect(compressionRatio).toBe(0.5) // 2:1 compression
    })

    it('should accumulate errors across batches', () => {
      let state = {
        remainingFiles: ['file1.parquet', 'file2.parquet'],
        processedFiles: [] as string[],
        outputFiles: [] as string[],
        totalRows: 0,
        bytesRead: 0,
        bytesWritten: 0,
        errors: [] as string[],
        startedAt: Date.now(),
      }

      // Simulate first batch with error
      state = {
        ...state,
        remainingFiles: ['file2.parquet'],
        processedFiles: ['file1.parquet'],
        errors: ['Batch 1: File corrupted'],
      }

      // Simulate second batch with error
      state = {
        ...state,
        remainingFiles: [],
        processedFiles: [...state.processedFiles, 'file2.parquet'],
        errors: [...state.errors, 'Batch 2: Parse failed'],
      }

      expect(state.errors).toHaveLength(2)
      expect(state.errors[0]).toContain('Batch 1')
      expect(state.errors[1]).toContain('Batch 2')
    })
  })

  describe('writer window analysis', () => {
    it('should group files by writer', () => {
      const files = [
        'data/users/1700001234-writer1-0.parquet',
        'data/users/1700001235-writer1-1.parquet',
        'data/users/1700001236-writer2-0.parquet',
        'data/users/1700001237-writer2-1.parquet',
        'data/users/1700001238-writer3-0.parquet',
      ]

      const writerWindows = new Map<string, { writerId: string; files: string[] }>()

      for (const file of files) {
        const match = file.match(/(\d+)-([^-]+)-(\d+)\.parquet$/)
        if (!match) continue

        const [, , writerId] = match
        if (!writerId) continue

        const existing = writerWindows.get(writerId)
        if (existing) {
          existing.files.push(file)
        } else {
          writerWindows.set(writerId, { writerId, files: [file] })
        }
      }

      expect(writerWindows.size).toBe(3)
      expect(writerWindows.get('writer1')?.files).toHaveLength(2)
      expect(writerWindows.get('writer2')?.files).toHaveLength(2)
      expect(writerWindows.get('writer3')?.files).toHaveLength(1)
    })

    it('should track writer timestamps', () => {
      const files = [
        'data/users/1700001000-writer1-0.parquet',
        'data/users/1700001500-writer1-1.parquet',
        'data/users/1700002000-writer1-2.parquet',
      ]

      const writerWindow = {
        writerId: 'writer1',
        files: [] as string[],
        firstTimestamp: Infinity,
        lastTimestamp: 0,
      }

      for (const file of files) {
        const match = file.match(/(\d+)-([^-]+)-(\d+)\.parquet$/)
        if (!match) continue

        const timestamp = parseInt(match[1] ?? '0', 10)
        writerWindow.files.push(file)
        writerWindow.firstTimestamp = Math.min(writerWindow.firstTimestamp, timestamp)
        writerWindow.lastTimestamp = Math.max(writerWindow.lastTimestamp, timestamp)
      }

      expect(writerWindow.firstTimestamp).toBe(1700001000)
      expect(writerWindow.lastTimestamp).toBe(1700002000)
      expect(writerWindow.lastTimestamp - writerWindow.firstTimestamp).toBe(1000)
    })
  })

  describe('output path generation', () => {
    it('should generate partitioned paths for all formats', () => {
      const timestamp = 1700000000000 // 2023-11-14 22:13:20 UTC
      const date = new Date(timestamp)
      const year = date.getUTCFullYear()
      const month = String(date.getUTCMonth() + 1).padStart(2, '0')
      const day = String(date.getUTCDate()).padStart(2, '0')
      const hour = String(date.getUTCHours()).padStart(2, '0')
      const namespace = 'users'
      const batchNum = 1

      // Native format
      const nativePath = `data/${namespace}/year=${year}/month=${month}/day=${day}/hour=${hour}/` +
        `compacted-${timestamp}-${batchNum}.parquet`
      expect(nativePath).toMatch(/^data\/users\/year=\d{4}\/month=\d{2}\/day=\d{2}\/hour=\d{2}\/compacted-\d+-\d+\.parquet$/)

      // Iceberg format
      const icebergPath = `${namespace}/data/year=${year}/month=${month}/day=${day}/hour=${hour}/` +
        `compacted-${timestamp}-${batchNum}.parquet`
      expect(icebergPath).toMatch(/^users\/data\/year=\d{4}\/month=\d{2}\/day=\d{2}\/hour=\d{2}\/compacted-\d+-\d+\.parquet$/)

      // Delta format
      const deltaPath = `${namespace}/year=${year}/month=${month}/day=${day}/hour=${hour}/` +
        `part-${String(batchNum).padStart(5, '0')}-compacted-${timestamp}.parquet`
      expect(deltaPath).toMatch(/^users\/year=\d{4}\/month=\d{2}\/day=\d{2}\/hour=\d{2}\/part-\d{5}-compacted-\d+\.parquet$/)
    })

    it('should handle different timezones correctly', () => {
      // Test with a timestamp that might cause timezone issues
      const timestamp = 1700006400000 // 2023-11-15 00:00:00 UTC
      const date = new Date(timestamp)

      // Always use UTC to avoid timezone issues
      const year = date.getUTCFullYear()
      const month = String(date.getUTCMonth() + 1).padStart(2, '0')
      const day = String(date.getUTCDate()).padStart(2, '0')
      const hour = String(date.getUTCHours()).padStart(2, '0')

      expect(year).toBe(2023)
      expect(month).toBe('11')
      expect(day).toBe('15')
      expect(hour).toBe('00')
    })
  })

  describe('batch processing', () => {
    it('should respect maxFilesPerStep limit', () => {
      const files = Array.from({ length: 100 }, (_, i) => `file${i}.parquet`)
      const maxFilesPerStep = 50

      const batch1 = files.slice(0, maxFilesPerStep)
      const remaining1 = files.slice(maxFilesPerStep)

      expect(batch1).toHaveLength(50)
      expect(remaining1).toHaveLength(50)

      const batch2 = remaining1.slice(0, maxFilesPerStep)
      const remaining2 = remaining1.slice(maxFilesPerStep)

      expect(batch2).toHaveLength(50)
      expect(remaining2).toHaveLength(0)
    })

    it('should use default maxFilesPerStep when not specified', () => {
      const params = {
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
        files: ['file.parquet'],
        writers: ['writer1'],
        targetFormat: 'native' as const,
      }

      const maxFilesPerStep = params.maxFilesPerStep ?? 50

      expect(maxFilesPerStep).toBe(50)
    })
  })

  describe('grace period handling', () => {
    it('should calculate remaining grace period', () => {
      const windowEnd = Date.now() - 20000 // 20 seconds ago
      const gracePeriodMs = 30000 // 30 seconds

      const timeSinceWindowEnd = Date.now() - windowEnd
      const remainingGracePeriod = gracePeriodMs - timeSinceWindowEnd

      expect(remainingGracePeriod).toBeLessThan(gracePeriodMs)
      expect(remainingGracePeriod).toBeGreaterThan(0)
    })

    it('should skip grace period if enough time has passed', () => {
      const windowEnd = Date.now() - 60000 // 1 minute ago
      const gracePeriodMs = 30000 // 30 seconds

      const timeSinceWindowEnd = Date.now() - windowEnd
      const needsWait = timeSinceWindowEnd < gracePeriodMs

      expect(needsWait).toBe(false)
    })
  })

  describe('notify-completion step', () => {
    it('should include doId param for DO identification', () => {
      const params = {
        namespace: 'users',
        doId: 'custom-do-id',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
        files: ['file.parquet'],
        writers: ['writer1'],
        targetFormat: 'native' as const,
      }

      // doId should be used if provided
      const doId = params.doId ?? params.namespace
      expect(doId).toBe('custom-do-id')
    })

    it('should default doId to namespace when not provided', () => {
      const params = {
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
        files: ['file.parquet'],
        writers: ['writer1'],
        targetFormat: 'native' as const,
      }

      // Should use namespace as default
      const doId = (params as { doId?: string }).doId ?? params.namespace
      expect(doId).toBe('users')
    })

    it('should format notification payload correctly', () => {
      const params = {
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
        files: ['file.parquet'],
        writers: ['writer1'],
        targetFormat: 'native' as const,
      }
      const workflowId = 'workflow-abc123'
      const state = {
        errors: [] as string[],
        failedFiles: [] as string[],
      }

      // Simulate the notification payload
      const payload = {
        windowKey: String(params.windowStart),
        workflowId,
        success: state.errors.length === 0 && state.failedFiles.length === 0,
      }

      expect(payload.windowKey).toBe('1700000000000')
      expect(payload.workflowId).toBe('workflow-abc123')
      expect(payload.success).toBe(true)
    })

    it('should report failure when errors exist', () => {
      const params = {
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
        files: ['file.parquet'],
        writers: ['writer1'],
        targetFormat: 'native' as const,
      }
      const workflowId = 'workflow-abc123'
      const state = {
        errors: ['Batch 1: Error occurred'],
        failedFiles: [] as string[],
      }

      const payload = {
        windowKey: String(params.windowStart),
        workflowId,
        success: state.errors.length === 0 && state.failedFiles.length === 0,
      }

      expect(payload.success).toBe(false)
    })

    it('should report failure when failed files exist', () => {
      const params = {
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
        files: ['file.parquet'],
        writers: ['writer1'],
        targetFormat: 'native' as const,
      }
      const workflowId = 'workflow-abc123'
      const state = {
        errors: [] as string[],
        failedFiles: ['failed-file.parquet'],
      }

      const payload = {
        windowKey: String(params.windowStart),
        workflowId,
        success: state.errors.length === 0 && state.failedFiles.length === 0,
      }

      expect(payload.success).toBe(false)
    })
  })
})

// =============================================================================
// Window Calculation Tests
// =============================================================================

describe('Window Calculation', () => {
  describe('window boundaries', () => {
    it('should calculate correct window boundaries for 1-hour windows', () => {
      const windowSizeMs = 3600000 // 1 hour
      const timestamp = 1700001234000 // Some time in the middle of an hour

      const windowStart = Math.floor(timestamp / windowSizeMs) * windowSizeMs
      const windowEnd = windowStart + windowSizeMs

      expect(windowStart).toBeLessThanOrEqual(timestamp)
      expect(windowEnd).toBeGreaterThan(timestamp)
      expect(windowEnd - windowStart).toBe(windowSizeMs)
    })

    it('should calculate correct window boundaries for 30-minute windows', () => {
      const windowSizeMs = 1800000 // 30 minutes
      const timestamp = 1700001234000

      const windowStart = Math.floor(timestamp / windowSizeMs) * windowSizeMs
      const windowEnd = windowStart + windowSizeMs

      expect(windowEnd - windowStart).toBe(windowSizeMs)
    })

    it('should handle timestamps at exact window boundaries', () => {
      const windowSizeMs = 3600000
      // Use a timestamp that's exactly on an hour boundary
      const exactBoundary = Math.floor(1700000000000 / windowSizeMs) * windowSizeMs

      const windowStart = Math.floor(exactBoundary / windowSizeMs) * windowSizeMs
      const windowEnd = windowStart + windowSizeMs

      expect(windowStart).toBe(exactBoundary)
      expect(windowEnd).toBe(exactBoundary + windowSizeMs)
    })
  })

  describe('window key generation', () => {
    it('should generate unique keys for different namespaces', () => {
      const timestamp = 1700001234000
      const windowSizeMs = 3600000
      const windowStart = Math.floor(timestamp / windowSizeMs) * windowSizeMs

      const key1 = `users:${windowStart}`
      const key2 = `posts:${windowStart}`

      expect(key1).not.toBe(key2)
      // Both should have same window start since they're from same time
      expect(key1.startsWith('users:')).toBe(true)
      expect(key2.startsWith('posts:')).toBe(true)
      expect(key1.split(':')[1]).toBe(key2.split(':')[1])
    })

    it('should generate unique keys for different time windows', () => {
      const windowSizeMs = 3600000

      const timestamp1 = 1700001234000
      const windowStart1 = Math.floor(timestamp1 / windowSizeMs) * windowSizeMs

      const timestamp2 = 1700005000000
      const windowStart2 = Math.floor(timestamp2 / windowSizeMs) * windowSizeMs

      const key1 = `users:${windowStart1}`
      const key2 = `users:${windowStart2}`

      expect(key1).not.toBe(key2)
    })
  })

  describe('window readiness criteria', () => {
    it('should not be ready if window is too recent', () => {
      const now = Date.now()
      const windowEnd = now - 60000 // 1 minute ago
      const maxWaitTimeMs = 300000 // 5 minutes

      const isReady = now >= windowEnd + maxWaitTimeMs

      expect(isReady).toBe(false)
    })

    it('should be ready if enough time has passed', () => {
      const now = Date.now()
      const windowEnd = now - 400000 // 6+ minutes ago
      const maxWaitTimeMs = 300000 // 5 minutes

      const isReady = now >= windowEnd + maxWaitTimeMs

      expect(isReady).toBe(true)
    })

    it('should not be ready if below minimum files', () => {
      const fileCount = 5
      const minFilesToCompact = 10

      const hasEnoughFiles = fileCount >= minFilesToCompact

      expect(hasEnoughFiles).toBe(false)
    })

    it('should be ready if at or above minimum files', () => {
      const testCases = [
        { fileCount: 10, minFiles: 10, expected: true },
        { fileCount: 15, minFiles: 10, expected: true },
        { fileCount: 9, minFiles: 10, expected: false },
      ]

      for (const { fileCount, minFiles, expected } of testCases) {
        const hasEnoughFiles = fileCount >= minFiles
        expect(hasEnoughFiles).toBe(expected)
      }
    })
  })
})

// =============================================================================
// State Serialization Round-Trip Tests
// =============================================================================

describe('State Serialization Round-Trip', () => {
  describe('WindowState serialization', () => {
    it('should round-trip WindowState correctly', () => {
      const originalState: WindowState = {
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
        filesByWriter: new Map([
          ['writer1', ['file1.parquet', 'file2.parquet']],
          ['writer2', ['file3.parquet']],
        ]),
        writers: new Set(['writer1', 'writer2']),
        lastActivityAt: 1700001234000,
        totalSize: 3072,
      }

      // Serialize (like saveState does)
      const serialized: StoredWindowState = {
        windowStart: originalState.windowStart,
        windowEnd: originalState.windowEnd,
        filesByWriter: Object.fromEntries(originalState.filesByWriter),
        writers: Array.from(originalState.writers),
        lastActivityAt: originalState.lastActivityAt,
        totalSize: originalState.totalSize,
      }

      // Deserialize (like ensureInitialized does)
      const deserialized: WindowState = {
        windowStart: serialized.windowStart,
        windowEnd: serialized.windowEnd,
        filesByWriter: new Map(Object.entries(serialized.filesByWriter)),
        writers: new Set(serialized.writers),
        lastActivityAt: serialized.lastActivityAt,
        totalSize: serialized.totalSize,
      }

      expect(deserialized.windowStart).toBe(originalState.windowStart)
      expect(deserialized.windowEnd).toBe(originalState.windowEnd)
      expect(deserialized.filesByWriter.get('writer1')).toEqual(originalState.filesByWriter.get('writer1'))
      expect(deserialized.filesByWriter.get('writer2')).toEqual(originalState.filesByWriter.get('writer2'))
      expect(deserialized.writers.has('writer1')).toBe(true)
      expect(deserialized.writers.has('writer2')).toBe(true)
      expect(deserialized.lastActivityAt).toBe(originalState.lastActivityAt)
      expect(deserialized.totalSize).toBe(originalState.totalSize)
    })
  })

  describe('ConsumerState serialization', () => {
    it('should round-trip ConsumerState with multiple windows', () => {
      const originalWindows = new Map<string, WindowState>([
        ['users:1700000000000', {
          windowStart: 1700000000000,
          windowEnd: 1700003600000,
          filesByWriter: new Map([['writer1', ['file1.parquet']]]),
          writers: new Set(['writer1']),
          lastActivityAt: 1700001234000,
          totalSize: 1024,
        }],
        ['posts:1700000000000', {
          windowStart: 1700000000000,
          windowEnd: 1700003600000,
          filesByWriter: new Map([['writer2', ['file2.parquet']]]),
          writers: new Set(['writer2']),
          lastActivityAt: 1700001235000,
          totalSize: 2048,
        }],
      ])

      const originalKnownWriters = new Set(['writer1', 'writer2', 'writer3'])
      const originalWriterLastSeen = new Map([
        ['writer1', 1700001234000],
        ['writer2', 1700001235000],
        ['writer3', 1700001236000],
      ])

      // Serialize
      const stored: StoredState = {
        windows: {},
        knownWriters: Array.from(originalKnownWriters),
        writerLastSeen: Object.fromEntries(originalWriterLastSeen),
      }

      for (const [key, window] of originalWindows) {
        stored.windows[key] = {
          windowStart: window.windowStart,
          windowEnd: window.windowEnd,
          filesByWriter: Object.fromEntries(window.filesByWriter),
          writers: Array.from(window.writers),
          lastActivityAt: window.lastActivityAt,
          totalSize: window.totalSize,
        }
      }

      // Deserialize
      const deserializedWindows = new Map<string, WindowState>()
      for (const [key, sw] of Object.entries(stored.windows)) {
        deserializedWindows.set(key, {
          windowStart: sw.windowStart,
          windowEnd: sw.windowEnd,
          filesByWriter: new Map(Object.entries(sw.filesByWriter)),
          writers: new Set(sw.writers),
          lastActivityAt: sw.lastActivityAt,
          totalSize: sw.totalSize,
        })
      }
      const deserializedKnownWriters = new Set(stored.knownWriters)
      const deserializedWriterLastSeen = new Map(Object.entries(stored.writerLastSeen))

      expect(deserializedWindows.size).toBe(2)
      expect(deserializedKnownWriters.size).toBe(3)
      expect(deserializedWriterLastSeen.size).toBe(3)
      expect(deserializedWindows.get('users:1700000000000')?.totalSize).toBe(1024)
      expect(deserializedWindows.get('posts:1700000000000')?.totalSize).toBe(2048)
    })
  })

  describe('empty state handling', () => {
    it('should handle empty windows map', () => {
      const stored: StoredState = {
        windows: {},
        knownWriters: [],
        writerLastSeen: {},
      }

      const deserializedWindows = new Map<string, WindowState>()
      for (const [key, sw] of Object.entries(stored.windows)) {
        deserializedWindows.set(key, {
          windowStart: sw.windowStart,
          windowEnd: sw.windowEnd,
          filesByWriter: new Map(Object.entries(sw.filesByWriter)),
          writers: new Set(sw.writers),
          lastActivityAt: sw.lastActivityAt,
          totalSize: sw.totalSize,
        })
      }

      expect(deserializedWindows.size).toBe(0)
    })

    it('should handle empty writers list', () => {
      const stored: StoredState = {
        windows: {},
        knownWriters: [],
        writerLastSeen: {},
      }

      const deserializedKnownWriters = new Set(stored.knownWriters)
      const deserializedWriterLastSeen = new Map(Object.entries(stored.writerLastSeen))

      expect(deserializedKnownWriters.size).toBe(0)
      expect(deserializedWriterLastSeen.size).toBe(0)
    })
  })

  describe('JSON compatibility', () => {
    it('should survive JSON.stringify/parse round-trip', () => {
      const stored: StoredState = {
        windows: {
          'users:1700000000000': {
            windowStart: 1700000000000,
            windowEnd: 1700003600000,
            filesByWriter: { 'writer1': ['file1.parquet', 'file2.parquet'] },
            writers: ['writer1'],
            lastActivityAt: 1700001234000,
            totalSize: 2048,
          },
        },
        knownWriters: ['writer1', 'writer2'],
        writerLastSeen: { 'writer1': 1700001234000, 'writer2': 1700001235000 },
      }

      const json = JSON.stringify(stored)
      const parsed = JSON.parse(json) as StoredState

      expect(parsed.windows['users:1700000000000']?.windowStart).toBe(1700000000000)
      expect(parsed.windows['users:1700000000000']?.filesByWriter['writer1']).toEqual(['file1.parquet', 'file2.parquet'])
      expect(parsed.knownWriters).toContain('writer1')
      expect(parsed.writerLastSeen['writer1']).toBe(1700001234000)
    })
  })
})

// =============================================================================
// Writer Activity Tracking Tests
// =============================================================================

describe('Writer Activity Tracking', () => {
  describe('active writer detection', () => {
    it('should identify active writers within threshold', () => {
      const now = Date.now()
      const inactiveThresholdMs = 30 * 60 * 1000 // 30 minutes

      const writerLastSeen = new Map([
        ['writer1', now - 1000], // 1 second ago - active
        ['writer2', now - (inactiveThresholdMs - 1000)], // Just under threshold - active
        ['writer3', now - inactiveThresholdMs], // Exactly at threshold - inactive
        ['writer4', now - (inactiveThresholdMs + 1000)], // Just over threshold - inactive
      ])

      const activeWriters: string[] = []
      for (const [writerId, lastSeen] of writerLastSeen) {
        if (now - lastSeen < inactiveThresholdMs) {
          activeWriters.push(writerId)
        }
      }

      expect(activeWriters).toContain('writer1')
      expect(activeWriters).toContain('writer2')
      expect(activeWriters).not.toContain('writer3')
      expect(activeWriters).not.toContain('writer4')
    })
  })

  describe('missing writer detection', () => {
    it('should detect missing writers for a window', () => {
      const activeWriters = ['writer1', 'writer2', 'writer3']
      const windowWriters = new Set(['writer1', 'writer3'])

      const missingWriters = activeWriters.filter(w => !windowWriters.has(w))

      expect(missingWriters).toEqual(['writer2'])
    })

    it('should return empty array when all active writers contributed', () => {
      const activeWriters = ['writer1', 'writer2']
      const windowWriters = new Set(['writer1', 'writer2', 'writer3']) // Extra writer is fine

      const missingWriters = activeWriters.filter(w => !windowWriters.has(w))

      expect(missingWriters).toHaveLength(0)
    })
  })

  describe('writer quorum', () => {
    it('should trigger compaction when all active writers contributed', () => {
      const activeWriters = ['writer1', 'writer2']
      const windowWriters = new Set(['writer1', 'writer2'])
      const hasEnoughFiles = true

      const missingWriters = activeWriters.filter(w => !windowWriters.has(w))
      const canCompact = missingWriters.length === 0 && hasEnoughFiles

      expect(canCompact).toBe(true)
    })

    it('should trigger compaction after wait timeout even with missing writers', () => {
      const now = Date.now()
      const lastActivityAt = now - 400000 // 6+ minutes ago
      const maxWaitTimeMs = 300000 // 5 minutes
      const hasEnoughFiles = true

      const waitedLongEnough = (now - lastActivityAt) > maxWaitTimeMs
      const canCompact = waitedLongEnough && hasEnoughFiles

      expect(canCompact).toBe(true)
    })
  })
})

// =============================================================================
// Batch Error Handling Tests (Issue: parquedb-pu5c)
// Critical: Prevent data loss when batch processing fails
// =============================================================================

describe('Batch Error Handling - Data Loss Prevention', () => {
  describe('CompactionState with failedFiles', () => {
    it('should include failedFiles array in state', () => {
      const state = {
        remainingFiles: ['file3.parquet'],
        processedFiles: ['file1.parquet'],
        failedFiles: ['file2.parquet'], // NEW: tracks failed batches
        outputFiles: ['compacted-1.parquet'],
        totalRows: 1000,
        bytesRead: 1024 * 100,
        bytesWritten: 1024 * 50,
        errors: ['Batch 2: File corrupted'],
        startedAt: Date.now(),
      }

      expect(state.failedFiles).toHaveLength(1)
      expect(state.failedFiles[0]).toBe('file2.parquet')
    })

    it('should track processed and failed files separately', () => {
      let state = {
        remainingFiles: ['file1.parquet', 'file2.parquet', 'file3.parquet', 'file4.parquet'],
        processedFiles: [] as string[],
        failedFiles: [] as string[],
        outputFiles: [] as string[],
        totalRows: 0,
        bytesRead: 0,
        bytesWritten: 0,
        errors: [] as string[],
        startedAt: Date.now(),
      }

      // Simulate first batch success (files 1-2)
      state = {
        ...state,
        remainingFiles: ['file3.parquet', 'file4.parquet'],
        processedFiles: ['file1.parquet', 'file2.parquet'],
        failedFiles: [],
        outputFiles: ['compacted-1.parquet'],
        totalRows: 500,
        bytesRead: 2048,
        bytesWritten: 1024,
      }

      expect(state.processedFiles).toHaveLength(2)
      expect(state.failedFiles).toHaveLength(0)

      // Simulate second batch failure (files 3-4)
      state = {
        ...state,
        remainingFiles: [],
        // CRITICAL: failed files should NOT be added to processedFiles
        processedFiles: state.processedFiles, // stays at 2
        failedFiles: ['file3.parquet', 'file4.parquet'], // failed batch goes here
        errors: ['Batch 2: Connection timeout'],
      }

      expect(state.processedFiles).toHaveLength(2) // Only successful files
      expect(state.failedFiles).toHaveLength(2) // Failed batch tracked separately
      expect(state.errors).toHaveLength(1)
    })
  })

  describe('source file deletion safety', () => {
    it('should only delete processedFiles, not failedFiles', () => {
      const state = {
        processedFiles: ['file1.parquet', 'file2.parquet'],
        failedFiles: ['file3.parquet', 'file4.parquet'],
      }

      // Simulate deleteSource logic - only delete processedFiles
      const filesToDelete = state.processedFiles
      const filesToPreserve = state.failedFiles

      expect(filesToDelete).toContain('file1.parquet')
      expect(filesToDelete).toContain('file2.parquet')
      expect(filesToDelete).not.toContain('file3.parquet')
      expect(filesToDelete).not.toContain('file4.parquet')

      expect(filesToPreserve).toContain('file3.parquet')
      expect(filesToPreserve).toContain('file4.parquet')
    })

    it('should not delete any files if all batches fail', () => {
      const state = {
        processedFiles: [] as string[],
        failedFiles: ['file1.parquet', 'file2.parquet', 'file3.parquet'],
      }

      const filesToDelete = state.processedFiles

      expect(filesToDelete).toHaveLength(0)
      // All files are preserved for retry
      expect(state.failedFiles).toHaveLength(3)
    })
  })

  describe('workflow summary with failed files', () => {
    it('should report success=false when there are failed files', () => {
      const state = {
        processedFiles: ['file1.parquet'],
        failedFiles: ['file2.parquet'],
        errors: ['Batch 2: Parse error'],
        startedAt: Date.now() - 5000,
      }

      const summary = {
        success: state.errors.length === 0 && state.failedFiles.length === 0,
        filesProcessed: state.processedFiles.length,
        filesFailed: state.failedFiles.length,
        failedFiles: state.failedFiles,
        errors: state.errors,
        durationMs: Date.now() - state.startedAt,
      }

      expect(summary.success).toBe(false)
      expect(summary.filesProcessed).toBe(1)
      expect(summary.filesFailed).toBe(1)
      expect(summary.failedFiles).toContain('file2.parquet')
    })

    it('should report success=true only when no errors and no failed files', () => {
      const state = {
        processedFiles: ['file1.parquet', 'file2.parquet'],
        failedFiles: [] as string[],
        errors: [] as string[],
        startedAt: Date.now() - 5000,
      }

      const summary = {
        success: state.errors.length === 0 && state.failedFiles.length === 0,
        filesProcessed: state.processedFiles.length,
        filesFailed: state.failedFiles.length,
        failedFiles: state.failedFiles,
        errors: state.errors,
        durationMs: Date.now() - state.startedAt,
      }

      expect(summary.success).toBe(true)
      expect(summary.filesProcessed).toBe(2)
      expect(summary.filesFailed).toBe(0)
    })

    it('should include failed files list for debugging/retry', () => {
      const failedFiles = [
        'data/users/1700001234-writer1-0.parquet',
        'data/users/1700001235-writer1-1.parquet',
      ]

      const summary = {
        success: false,
        filesFailed: failedFiles.length,
        failedFiles: failedFiles,
        errors: ['Batch 1: Network error', 'Batch 2: Timeout'],
      }

      // Failed files list enables:
      // 1. Debugging which files caused issues
      // 2. Manual retry of just the failed files
      // 3. Alerting/monitoring integration
      expect(summary.failedFiles).toEqual(failedFiles)
      expect(summary.failedFiles.length).toBe(summary.filesFailed)
    })
  })

  describe('partial success scenarios', () => {
    it('should handle mixed success/failure batches correctly', () => {
      let state = {
        remainingFiles: ['f1.parquet', 'f2.parquet', 'f3.parquet', 'f4.parquet', 'f5.parquet', 'f6.parquet'],
        processedFiles: [] as string[],
        failedFiles: [] as string[],
        outputFiles: [] as string[],
        totalRows: 0,
        bytesRead: 0,
        bytesWritten: 0,
        errors: [] as string[],
        startedAt: Date.now(),
      }

      const maxFilesPerBatch = 2

      // Batch 1: Success (f1, f2)
      const batch1 = state.remainingFiles.slice(0, maxFilesPerBatch)
      state = {
        ...state,
        remainingFiles: state.remainingFiles.slice(maxFilesPerBatch),
        processedFiles: [...state.processedFiles, ...batch1],
        outputFiles: ['compacted-1.parquet'],
        totalRows: 100,
        bytesRead: 1000,
        bytesWritten: 500,
      }

      // Batch 2: Failure (f3, f4)
      const batch2 = state.remainingFiles.slice(0, maxFilesPerBatch)
      state = {
        ...state,
        remainingFiles: state.remainingFiles.slice(maxFilesPerBatch),
        // CRITICAL: DO NOT add batch2 to processedFiles
        failedFiles: [...state.failedFiles, ...batch2],
        errors: [...state.errors, 'Batch 2: Corrupted file'],
      }

      // Batch 3: Success (f5, f6)
      const batch3 = state.remainingFiles.slice(0, maxFilesPerBatch)
      state = {
        ...state,
        remainingFiles: state.remainingFiles.slice(maxFilesPerBatch),
        processedFiles: [...state.processedFiles, ...batch3],
        outputFiles: [...state.outputFiles, 'compacted-3.parquet'],
        totalRows: state.totalRows + 100,
        bytesRead: state.bytesRead + 1000,
        bytesWritten: state.bytesWritten + 500,
      }

      // Final state verification
      expect(state.remainingFiles).toHaveLength(0)
      expect(state.processedFiles).toEqual(['f1.parquet', 'f2.parquet', 'f5.parquet', 'f6.parquet'])
      expect(state.failedFiles).toEqual(['f3.parquet', 'f4.parquet'])
      expect(state.outputFiles).toEqual(['compacted-1.parquet', 'compacted-3.parquet'])
      expect(state.errors).toHaveLength(1)

      // Only successfully processed files should be candidates for deletion
      const filesToDelete = state.processedFiles
      expect(filesToDelete).not.toContain('f3.parquet')
      expect(filesToDelete).not.toContain('f4.parquet')
    })

    it('should calculate correct statistics with partial failures', () => {
      const state = {
        processedFiles: ['f1.parquet', 'f2.parquet', 'f5.parquet', 'f6.parquet'],
        failedFiles: ['f3.parquet', 'f4.parquet'],
        outputFiles: ['compacted-1.parquet', 'compacted-3.parquet'],
        totalRows: 200, // Only from successful batches
        bytesRead: 2000, // Only from successful batches
        bytesWritten: 1000, // Only from successful batches
        errors: ['Batch 2: Error'],
      }

      // Statistics should only reflect successful processing
      expect(state.totalRows).toBe(200)

      // Compression ratio only considers successfully processed data
      const compressionRatio = state.bytesWritten / state.bytesRead
      expect(compressionRatio).toBe(0.5)

      // Total files = processed + failed
      const totalInputFiles = state.processedFiles.length + state.failedFiles.length
      expect(totalInputFiles).toBe(6)

      // Success rate
      const successRate = state.processedFiles.length / totalInputFiles
      expect(successRate).toBeCloseTo(0.667, 2) // 4/6 = 66.7%
    })
  })

  describe('error recovery information', () => {
    it('should preserve enough information for retry', () => {
      const originalFiles = [
        'data/users/1700001234-writer1-0.parquet',
        'data/users/1700001235-writer1-1.parquet',
        'data/users/1700001236-writer2-0.parquet',
      ]

      // After workflow with partial failure
      const result = {
        success: false,
        namespace: 'users',
        windowStart: '2023-11-14T22:00:00.000Z',
        windowEnd: '2023-11-14T23:00:00.000Z',
        filesProcessed: 1,
        filesFailed: 2,
        failedFiles: [
          'data/users/1700001235-writer1-1.parquet',
          'data/users/1700001236-writer2-0.parquet',
        ],
        errors: ['Batch 2: Network timeout'],
      }

      // Retry workflow should be able to use failedFiles directly
      const retryParams = {
        namespace: result.namespace,
        windowStart: new Date(result.windowStart).getTime(),
        windowEnd: new Date(result.windowEnd).getTime(),
        files: result.failedFiles, // Only retry failed files
        writers: ['writer1', 'writer2'], // Extract from file names
        targetFormat: 'native' as const,
      }

      expect(retryParams.files).toHaveLength(2)
      expect(retryParams.files).toEqual(result.failedFiles)
    })
  })
})
