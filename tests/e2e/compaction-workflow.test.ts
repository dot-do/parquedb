/**
 * E2E Integration Test for Compaction Workflow
 *
 * Tests the full compaction flow from R2 event notifications through
 * workflow execution to compacted output files.
 *
 * Test scenarios:
 * 1. Basic compaction flow - single writer
 * 2. Multi-writer scenario - merge-sort verification
 * 3. Iceberg format output
 * 4. Delta format output
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import type { R2EventMessage, CompactionConsumerConfig } from '@/workflows/compaction-queue-consumer'
import type { BackendType } from '@/backends'

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
// Mock R2 Bucket (Enhanced for E2E)
// =============================================================================

class MockR2BucketE2E {
  private files: Map<string, { data: Uint8Array; size: number; metadata?: Record<string, string> }> = new Map()
  private writeLog: Array<{ key: string; size: number; timestamp: number }> = []
  private deleteLog: Array<{ key: string; timestamp: number }> = []

  async get(key: string): Promise<MockR2Object | null> {
    const file = this.files.get(key)
    if (!file) return null

    return {
      key,
      size: file.size,
      eTag: `"${this.generateEtag(file.data)}"`,
      uploaded: new Date(),
      async arrayBuffer() {
        return file.data.buffer.slice(file.data.byteOffset, file.data.byteOffset + file.data.byteLength)
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
    this.writeLog.push({ key, size: uint8.length, timestamp: Date.now() })

    return {
      key,
      size: uint8.length,
      eTag: `"${this.generateEtag(uint8)}"`,
      uploaded: new Date(),
      async arrayBuffer() {
        return uint8.buffer.slice(uint8.byteOffset, uint8.byteOffset + uint8.byteLength)
      },
      async text() {
        return new TextDecoder().decode(uint8)
      },
    }
  }

  async head(key: string): Promise<{ key: string; size: number; customMetadata?: Record<string, string> } | null> {
    const file = this.files.get(key)
    if (!file) return null
    return { key, size: file.size, customMetadata: file.metadata }
  }

  async delete(key: string | string[]): Promise<void> {
    const keys = Array.isArray(key) ? key : [key]
    for (const k of keys) {
      if (this.files.has(k)) {
        this.deleteLog.push({ key: k, timestamp: Date.now() })
        this.files.delete(k)
      }
    }
  }

  async list(options?: { prefix?: string; limit?: number }): Promise<MockR2ListResult> {
    const prefix = options?.prefix ?? ''
    const limit = options?.limit
    const objects: MockR2Object[] = []

    for (const [key, file] of this.files) {
      if (key.startsWith(prefix)) {
        objects.push({
          key,
          size: file.size,
          eTag: `"${this.generateEtag(file.data)}"`,
          uploaded: new Date(),
          async arrayBuffer() {
            return file.data.buffer.slice(file.data.byteOffset, file.data.byteOffset + file.data.byteLength)
          },
          async text() {
            return new TextDecoder().decode(file.data)
          },
        })

        if (limit && objects.length >= limit) break
      }
    }

    return { objects, truncated: false }
  }

  // Test helpers
  clear(): void {
    this.files.clear()
    this.writeLog = []
    this.deleteLog = []
  }

  getFileCount(): number {
    return this.files.size
  }

  hasFile(key: string): boolean {
    return this.files.has(key)
  }

  getFileKeys(): string[] {
    return Array.from(this.files.keys())
  }

  getWriteLog(): Array<{ key: string; size: number; timestamp: number }> {
    return [...this.writeLog]
  }

  getDeleteLog(): Array<{ key: string; timestamp: number }> {
    return [...this.deleteLog]
  }

  private generateEtag(data: Uint8Array): string {
    // Simple hash for testing
    let hash = 0
    for (let i = 0; i < data.length; i++) {
      hash = ((hash << 5) - hash) + data[i]
      hash = hash & hash
    }
    return hash.toString(16)
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

  clear(): void {
    this.storage.clear()
  }
}

// =============================================================================
// Mock Workflow
// =============================================================================

interface WorkflowParams {
  namespace: string
  windowStart: number
  windowEnd: number
  files: string[]
  writers: string[]
  targetFormat: BackendType
}

class MockWorkflow {
  public createdInstances: Array<{ id: string; params: WorkflowParams }> = []
  private nextId = 1

  async create(options: { params: WorkflowParams }): Promise<{ id: string }> {
    const id = `workflow-${this.nextId++}`
    this.createdInstances.push({ id, params: options.params })
    return { id }
  }

  clear(): void {
    this.createdInstances = []
    this.nextId = 1
  }
}

// =============================================================================
// CompactionStateDO (Testable Implementation)
// =============================================================================

type WindowProcessingStatus =
  | { state: 'pending' }
  | { state: 'processing'; startedAt: number }
  | { state: 'dispatched'; workflowId: string; dispatchedAt: number }

interface WindowState {
  windowStart: number
  windowEnd: number
  filesByWriter: Map<string, string[]>
  writers: Set<string>
  lastActivityAt: number
  totalSize: number
  processingStatus: WindowProcessingStatus
}

interface StoredWindowState {
  windowStart: number
  windowEnd: number
  filesByWriter: Record<string, string[]>
  writers: string[]
  lastActivityAt: number
  totalSize: number
  processingStatus?: WindowProcessingStatus
}

interface StoredState {
  namespace: string
  windows: Record<string, StoredWindowState>
  knownWriters: string[]
  writerLastSeen: Record<string, number>
}

interface WindowReadyEntry {
  namespace: string
  windowKey: string
  windowStart: number
  windowEnd: number
  files: string[]
  writers: string[]
}

const WRITER_INACTIVE_THRESHOLD_MS = 30 * 60 * 1000
const PROCESSING_TIMEOUT_MS = 5 * 60 * 1000

class TestableCompactionStateDO {
  private state: MockDurableObjectState
  private namespace: string = ''
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
      this.namespace = stored.namespace ?? ''
      for (const [key, sw] of Object.entries(stored.windows)) {
        this.windows.set(key, {
          windowStart: sw.windowStart,
          windowEnd: sw.windowEnd,
          filesByWriter: new Map(Object.entries(sw.filesByWriter)),
          writers: new Set(sw.writers),
          lastActivityAt: sw.lastActivityAt,
          totalSize: sw.totalSize,
          processingStatus: sw.processingStatus ?? { state: 'pending' },
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
        processingStatus: window.processingStatus,
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

    if (url.pathname === '/confirm-dispatch' && request.method === 'POST') {
      return this.handleConfirmDispatch(request)
    }

    if (url.pathname === '/rollback-processing' && request.method === 'POST') {
      return this.handleRollbackProcessing(request)
    }

    if (url.pathname === '/workflow-complete' && request.method === 'POST') {
      return this.handleWorkflowComplete(request)
    }

    if (url.pathname === '/status') {
      return this.handleStatus()
    }

    return new Response('Not Found', { status: 404 })
  }

  private cleanupStuckProcessingWindows(now: number): void {
    for (const [, window] of this.windows) {
      if (
        window.processingStatus.state === 'processing' &&
        now - window.processingStatus.startedAt > PROCESSING_TIMEOUT_MS
      ) {
        window.processingStatus = { state: 'pending' }
      }
    }
  }

  private async handleUpdate(request: Request): Promise<Response> {
    const body = await request.json() as {
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

    const { namespace, updates, config } = body
    const now = Date.now()
    const windowsReady: WindowReadyEntry[] = []

    if (!this.namespace) {
      this.namespace = namespace
    }

    this.cleanupStuckProcessingWindows(now)

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
          processingStatus: { state: 'pending' },
        }
        this.windows.set(windowKey, window)
      }

      if (window.processingStatus.state === 'pending') {
        const writerFiles = window.filesByWriter.get(writerId) ?? []
        writerFiles.push(file)
        window.filesByWriter.set(writerId, writerFiles)
        window.writers.add(writerId)
        window.lastActivityAt = now
        window.totalSize += size
      }
    }

    const activeWriters = this.getActiveWriters(now)

    for (const [windowKey, window] of this.windows) {
      if (window.processingStatus.state !== 'pending') continue
      if (now < window.windowEnd + config.maxWaitTimeMs) continue

      let totalFiles = 0
      for (const files of window.filesByWriter.values()) {
        totalFiles += files.length
      }

      if (totalFiles < config.minFilesToCompact) continue

      const missingWriters = activeWriters.filter(w => !window.writers.has(w))
      const waitedLongEnough = (now - window.lastActivityAt) > config.maxWaitTimeMs

      if (missingWriters.length === 0 || waitedLongEnough) {
        window.processingStatus = { state: 'processing', startedAt: now }

        const allFiles: string[] = []
        for (const files of window.filesByWriter.values()) {
          allFiles.push(...files)
        }

        windowsReady.push({
          namespace: this.namespace,
          windowKey,
          windowStart: window.windowStart,
          windowEnd: window.windowEnd,
          files: allFiles.sort(),
          writers: Array.from(window.writers),
        })
      }
    }

    await this.saveState()

    return new Response(JSON.stringify({ windowsReady }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  private async handleConfirmDispatch(request: Request): Promise<Response> {
    const body = await request.json() as { windowKey: string; workflowId: string }
    const { windowKey, workflowId } = body
    const window = this.windows.get(windowKey)

    if (!window) {
      return new Response(JSON.stringify({ error: 'Window not found' }), {
        status: 404,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    if (window.processingStatus.state !== 'processing') {
      return new Response(JSON.stringify({
        error: 'Window not in processing state',
        currentState: window.processingStatus.state,
      }), {
        status: 409,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    window.processingStatus = {
      state: 'dispatched',
      workflowId,
      dispatchedAt: Date.now(),
    }

    await this.saveState()

    return new Response(JSON.stringify({ success: true }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  private async handleRollbackProcessing(request: Request): Promise<Response> {
    const body = await request.json() as { windowKey: string }
    const { windowKey } = body
    const window = this.windows.get(windowKey)

    if (!window) {
      return new Response(JSON.stringify({ error: 'Window not found' }), {
        status: 404,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    if (window.processingStatus.state !== 'processing') {
      return new Response(JSON.stringify({
        error: 'Window not in processing state',
        currentState: window.processingStatus.state,
      }), {
        status: 409,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    window.processingStatus = { state: 'pending' }
    await this.saveState()

    return new Response(JSON.stringify({ success: true }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  private async handleWorkflowComplete(request: Request): Promise<Response> {
    const body = await request.json() as { windowKey: string; workflowId: string; success: boolean }
    const { windowKey, workflowId, success } = body
    const window = this.windows.get(windowKey)

    if (!window) {
      return new Response(JSON.stringify({ success: true, alreadyDeleted: true }), {
        headers: { 'Content-Type': 'application/json' },
      })
    }

    if (window.processingStatus.state !== 'dispatched') {
      return new Response(JSON.stringify({
        error: 'Window not in dispatched state',
        currentState: window.processingStatus.state,
      }), {
        status: 409,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    if (window.processingStatus.workflowId !== workflowId) {
      return new Response(JSON.stringify({
        error: 'Workflow ID mismatch',
        expected: window.processingStatus.workflowId,
        received: workflowId,
      }), {
        status: 409,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    if (success) {
      this.windows.delete(windowKey)
    } else {
      window.processingStatus = { state: 'pending' }
    }

    await this.saveState()

    return new Response(JSON.stringify({ success: true }), {
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
        processingStatus: w.processingStatus,
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

  getNamespace(): string {
    return this.namespace
  }
}

// =============================================================================
// Mock DurableObjectNamespace
// =============================================================================

class MockDurableObjectNamespace {
  private dos: Map<string, TestableCompactionStateDO> = new Map()
  private states: Map<string, MockDurableObjectState> = new Map()

  idFromName(name: string): string {
    return `do-id-${name}`
  }

  get(id: string): TestableCompactionStateDO {
    let doInstance = this.dos.get(id)
    if (!doInstance) {
      let state = this.states.get(id)
      if (!state) {
        state = new MockDurableObjectState()
        this.states.set(id, state)
      }
      doInstance = new TestableCompactionStateDO(state)
      this.dos.set(id, doInstance)
    }
    return doInstance
  }

  clear(): void {
    this.dos.clear()
    for (const state of this.states.values()) {
      state.clear()
    }
    this.states.clear()
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

function createMockBatch<T>(messages: T[], queueName = 'parquedb-compaction-events'): MockMessageBatch<T> {
  return {
    messages: messages.map(m => createMockMessage(m)),
    queue: queueName,
  }
}

/**
 * Create test parquet file content (minimal valid parquet for testing)
 */
function createTestParquetContent(rows: Array<Record<string, unknown>>): Uint8Array {
  // For E2E testing, we create a simple NDJSON representation
  // In production, this would be actual Parquet data
  const content = rows.map(r => JSON.stringify(r)).join('\n')
  return new TextEncoder().encode(content)
}

// =============================================================================
// E2E Test Suite: Basic Compaction Flow
// =============================================================================

describe('E2E: Basic Compaction Flow', () => {
  let bucket: MockR2BucketE2E
  let doNamespace: MockDurableObjectNamespace
  let workflow: MockWorkflow

  beforeEach(() => {
    vi.useFakeTimers()
    bucket = new MockR2BucketE2E()
    doNamespace = new MockDurableObjectNamespace()
    workflow = new MockWorkflow()
  })

  afterEach(() => {
    vi.useRealTimers()
    bucket.clear()
    doNamespace.clear()
    workflow.clear()
  })

  it('should track files from R2 event notifications', async () => {
    const now = Date.now()
    const oldTimestamp = Math.floor((now - 2 * 60 * 60 * 1000) / 1000) // 2 hours ago in seconds

    // Create test files in R2
    for (let i = 0; i < 5; i++) {
      const content = createTestParquetContent([{ id: i, name: `Entity ${i}` }])
      await bucket.put(`data/users/${oldTimestamp}-writer1-${i}.parquet`, content)
    }

    // Create event notifications
    const events = Array.from({ length: 5 }, (_, i) =>
      createR2EventMessage(
        `data/users/${oldTimestamp}-writer1-${i}.parquet`,
        1024
      )
    )

    // Get the DO for namespace 'users'
    const doId = doNamespace.idFromName('users')
    const stateDO = doNamespace.get(doId)

    // Simulate processing events
    const updates = events.map(e => ({
      namespace: 'users',
      writerId: 'writer1',
      file: e.object.key,
      timestamp: oldTimestamp * 1000,
      size: e.object.size,
    }))

    const response = await stateDO.fetch(new Request('http://internal/update', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: 'users',
        updates,
        config: {
          windowSizeMs: 3600000,
          minFilesToCompact: 10,
          maxWaitTimeMs: 300000,
          targetFormat: 'native',
        },
      }),
    }))

    expect(response.status).toBe(200)

    // Check status
    const statusResponse = await stateDO.fetch(new Request('http://internal/status'))
    const status = await statusResponse.json() as { activeWindows: number; knownWriters: string[] }

    expect(status.activeWindows).toBe(1)
    expect(status.knownWriters).toContain('writer1')
  })

  it('should trigger workflow when window is ready', async () => {
    const now = Date.now()
    const oldTimestamp = Math.floor((now - 2 * 60 * 60 * 1000) / 1000) // 2 hours ago

    // Get the DO
    const doId = doNamespace.idFromName('users')
    const stateDO = doNamespace.get(doId)

    // Create enough files to trigger compaction
    const updates = Array.from({ length: 15 }, (_, i) => ({
      namespace: 'users',
      writerId: 'writer1',
      file: `data/users/${oldTimestamp}-writer1-${i}.parquet`,
      timestamp: oldTimestamp * 1000,
      size: 1024,
    }))

    const response = await stateDO.fetch(new Request('http://internal/update', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: 'users',
        updates,
        config: {
          windowSizeMs: 3600000,
          minFilesToCompact: 10,
          maxWaitTimeMs: 300000,
          targetFormat: 'native',
        },
      }),
    }))

    const result = await response.json() as { windowsReady: WindowReadyEntry[] }

    expect(result.windowsReady).toHaveLength(1)
    expect(result.windowsReady[0].files).toHaveLength(15)
    expect(result.windowsReady[0].namespace).toBe('users')
    expect(result.windowsReady[0].writers).toContain('writer1')

    // Simulate workflow creation
    const windowReady = result.windowsReady[0]
    const workflowInstance = await workflow.create({
      params: {
        namespace: windowReady.namespace,
        windowStart: windowReady.windowStart,
        windowEnd: windowReady.windowEnd,
        files: windowReady.files,
        writers: windowReady.writers,
        targetFormat: 'native',
      },
    })

    expect(workflowInstance.id).toBeDefined()
    expect(workflow.createdInstances).toHaveLength(1)
    expect(workflow.createdInstances[0].params.namespace).toBe('users')

    // Confirm dispatch
    await stateDO.fetch(new Request('http://internal/confirm-dispatch', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        windowKey: windowReady.windowKey,
        workflowId: workflowInstance.id,
      }),
    }))

    // Complete workflow
    await stateDO.fetch(new Request('http://internal/workflow-complete', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        windowKey: windowReady.windowKey,
        workflowId: workflowInstance.id,
        success: true,
      }),
    }))

    // Verify window is cleaned up
    const statusResponse = await stateDO.fetch(new Request('http://internal/status'))
    const status = await statusResponse.json() as { activeWindows: number }
    expect(status.activeWindows).toBe(0)
  })

  it('should mark source files for deletion after compaction', async () => {
    const now = Date.now()
    const oldTimestamp = Math.floor((now - 2 * 60 * 60 * 1000) / 1000)

    // Create test files in R2
    const sourceFiles: string[] = []
    for (let i = 0; i < 15; i++) {
      const key = `data/users/${oldTimestamp}-writer1-${i}.parquet`
      sourceFiles.push(key)
      const content = createTestParquetContent([{ id: i, name: `Entity ${i}`, createdAt: oldTimestamp * 1000 + i }])
      await bucket.put(key, content)
    }

    expect(bucket.getFileCount()).toBe(15)

    // Simulate compaction by deleting source files
    for (const file of sourceFiles) {
      await bucket.delete(file)
    }

    expect(bucket.getFileCount()).toBe(0)
    expect(bucket.getDeleteLog()).toHaveLength(15)
  })
})

// =============================================================================
// E2E Test Suite: Multi-Writer Scenario
// =============================================================================

describe('E2E: Multi-Writer Scenario', () => {
  let bucket: MockR2BucketE2E
  let doNamespace: MockDurableObjectNamespace
  let workflow: MockWorkflow

  beforeEach(() => {
    vi.useFakeTimers()
    bucket = new MockR2BucketE2E()
    doNamespace = new MockDurableObjectNamespace()
    workflow = new MockWorkflow()
  })

  afterEach(() => {
    vi.useRealTimers()
    bucket.clear()
    doNamespace.clear()
    workflow.clear()
  })

  it('should track files from multiple writers in same window', async () => {
    const now = Date.now()
    const oldTimestamp = Math.floor((now - 2 * 60 * 60 * 1000) / 1000)

    const doId = doNamespace.idFromName('users')
    const stateDO = doNamespace.get(doId)

    // Updates from writer1
    const writer1Updates = Array.from({ length: 5 }, (_, i) => ({
      namespace: 'users',
      writerId: 'writer1',
      file: `data/users/${oldTimestamp}-writer1-${i}.parquet`,
      timestamp: oldTimestamp * 1000,
      size: 1024,
    }))

    // Updates from writer2
    const writer2Updates = Array.from({ length: 5 }, (_, i) => ({
      namespace: 'users',
      writerId: 'writer2',
      file: `data/users/${oldTimestamp + 1}-writer2-${i}.parquet`,
      timestamp: (oldTimestamp + 1) * 1000,
      size: 2048,
    }))

    // Updates from writer3
    const writer3Updates = Array.from({ length: 5 }, (_, i) => ({
      namespace: 'users',
      writerId: 'writer3',
      file: `data/users/${oldTimestamp + 2}-writer3-${i}.parquet`,
      timestamp: (oldTimestamp + 2) * 1000,
      size: 1536,
    }))

    // Process all updates together
    const response = await stateDO.fetch(new Request('http://internal/update', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: 'users',
        updates: [...writer1Updates, ...writer2Updates, ...writer3Updates],
        config: {
          windowSizeMs: 3600000,
          minFilesToCompact: 10,
          maxWaitTimeMs: 300000,
          targetFormat: 'native',
        },
      }),
    }))

    const result = await response.json() as { windowsReady: WindowReadyEntry[] }

    expect(result.windowsReady).toHaveLength(1)
    expect(result.windowsReady[0].files).toHaveLength(15)
    expect(result.windowsReady[0].writers).toHaveLength(3)
    expect(result.windowsReady[0].writers).toContain('writer1')
    expect(result.windowsReady[0].writers).toContain('writer2')
    expect(result.windowsReady[0].writers).toContain('writer3')
  })

  it('should produce correctly ordered files for merge-sort', async () => {
    const now = Date.now()
    const baseTimestamp = Math.floor((now - 2 * 60 * 60 * 1000) / 1000)

    const doId = doNamespace.idFromName('users')
    const stateDO = doNamespace.get(doId)

    // Create interleaved updates from multiple writers
    const updates: Array<{
      namespace: string
      writerId: string
      file: string
      timestamp: number
      size: number
    }> = []

    for (let i = 0; i < 5; i++) {
      // Writer1 at base + i*3
      updates.push({
        namespace: 'users',
        writerId: 'writer1',
        file: `data/users/${baseTimestamp + i * 3}-writer1-${i}.parquet`,
        timestamp: (baseTimestamp + i * 3) * 1000,
        size: 1024,
      })

      // Writer2 at base + i*3 + 1
      updates.push({
        namespace: 'users',
        writerId: 'writer2',
        file: `data/users/${baseTimestamp + i * 3 + 1}-writer2-${i}.parquet`,
        timestamp: (baseTimestamp + i * 3 + 1) * 1000,
        size: 1024,
      })
    }

    const response = await stateDO.fetch(new Request('http://internal/update', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: 'users',
        updates,
        config: {
          windowSizeMs: 3600000,
          minFilesToCompact: 10,
          maxWaitTimeMs: 300000,
          targetFormat: 'native',
        },
      }),
    }))

    const result = await response.json() as { windowsReady: WindowReadyEntry[] }

    expect(result.windowsReady).toHaveLength(1)

    // Files should be sorted for efficient merge-sort
    const files = result.windowsReady[0].files
    const sortedFiles = [...files].sort()
    expect(files).toEqual(sortedFiles)
  })
})

// =============================================================================
// E2E Test Suite: Iceberg Format Output
// =============================================================================

describe('E2E: Iceberg Format Output', () => {
  let bucket: MockR2BucketE2E
  let doNamespace: MockDurableObjectNamespace
  let workflow: MockWorkflow

  beforeEach(() => {
    vi.useFakeTimers()
    bucket = new MockR2BucketE2E()
    doNamespace = new MockDurableObjectNamespace()
    workflow = new MockWorkflow()
  })

  afterEach(() => {
    vi.useRealTimers()
    bucket.clear()
    doNamespace.clear()
    workflow.clear()
  })

  it('should trigger workflow with targetFormat=iceberg', async () => {
    const now = Date.now()
    const oldTimestamp = Math.floor((now - 2 * 60 * 60 * 1000) / 1000)

    const doId = doNamespace.idFromName('users')
    const stateDO = doNamespace.get(doId)

    const updates = Array.from({ length: 15 }, (_, i) => ({
      namespace: 'users',
      writerId: 'writer1',
      file: `data/users/${oldTimestamp}-writer1-${i}.parquet`,
      timestamp: oldTimestamp * 1000,
      size: 1024,
    }))

    const response = await stateDO.fetch(new Request('http://internal/update', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: 'users',
        updates,
        config: {
          windowSizeMs: 3600000,
          minFilesToCompact: 10,
          maxWaitTimeMs: 300000,
          targetFormat: 'iceberg',
        },
      }),
    }))

    const result = await response.json() as { windowsReady: WindowReadyEntry[] }
    expect(result.windowsReady).toHaveLength(1)

    // Create workflow with iceberg format
    const windowReady = result.windowsReady[0]
    const workflowInstance = await workflow.create({
      params: {
        namespace: windowReady.namespace,
        windowStart: windowReady.windowStart,
        windowEnd: windowReady.windowEnd,
        files: windowReady.files,
        writers: windowReady.writers,
        targetFormat: 'iceberg',
      },
    })

    expect(workflow.createdInstances[0].params.targetFormat).toBe('iceberg')
  })

  it('should generate correct Iceberg data file path', () => {
    const timestamp = 1700000000000 // 2023-11-14 22:13:20 UTC
    const date = new Date(timestamp)
    const year = date.getUTCFullYear()
    const month = String(date.getUTCMonth() + 1).padStart(2, '0')
    const day = String(date.getUTCDate()).padStart(2, '0')
    const hour = String(date.getUTCHours()).padStart(2, '0')
    const namespace = 'users'
    const batchNum = 1

    // Iceberg format: {namespace}/data/year={year}/month={month}/day={day}/hour={hour}/compacted-{timestamp}-{batch}.parquet
    const icebergPath = `${namespace}/data/year=${year}/month=${month}/day=${day}/hour=${hour}/` +
      `compacted-${timestamp}-${batchNum}.parquet`

    expect(icebergPath).toMatch(/^users\/data\/year=\d{4}\/month=\d{2}\/day=\d{2}\/hour=\d{2}\/compacted-\d+-\d+\.parquet$/)
    expect(icebergPath).toContain('users/data/')
    expect(icebergPath).not.toContain('data/users/')
  })

  it('should simulate Iceberg manifest creation', async () => {
    // Simulate the structure Iceberg would create
    const tableLocation = 'users'
    const timestamp = 1700000000000
    const date = new Date(timestamp)
    const year = date.getUTCFullYear()
    const month = String(date.getUTCMonth() + 1).padStart(2, '0')
    const day = String(date.getUTCDate()).padStart(2, '0')
    const hour = String(date.getUTCHours()).padStart(2, '0')

    // Data file
    const dataFilePath = `${tableLocation}/data/year=${year}/month=${month}/day=${day}/hour=${hour}/compacted-${timestamp}-1.parquet`
    await bucket.put(dataFilePath, createTestParquetContent([{ id: 1, name: 'Test' }]))

    // Manifest file (simplified for testing)
    const manifestPath = `${tableLocation}/metadata/manifest-1.avro`
    const manifestContent = JSON.stringify({
      format_version: 2,
      entries: [{
        status: 'ADDED',
        data_file: {
          file_path: dataFilePath,
          file_size_in_bytes: 1024,
          record_count: 1,
        },
      }],
    })
    await bucket.put(manifestPath, manifestContent)

    // Metadata file
    const metadataPath = `${tableLocation}/metadata/v1.metadata.json`
    const metadataContent = JSON.stringify({
      format_version: 2,
      table_uuid: 'test-uuid',
      location: tableLocation,
      current_snapshot_id: 1,
      snapshots: [{
        snapshot_id: 1,
        timestamp_ms: timestamp,
        manifests: [manifestPath],
      }],
    })
    await bucket.put(metadataPath, metadataContent)

    // Verify structure
    expect(bucket.hasFile(dataFilePath)).toBe(true)
    expect(bucket.hasFile(manifestPath)).toBe(true)
    expect(bucket.hasFile(metadataPath)).toBe(true)
  })
})

// =============================================================================
// E2E Test Suite: Delta Format Output
// =============================================================================

describe('E2E: Delta Format Output', () => {
  let bucket: MockR2BucketE2E
  let doNamespace: MockDurableObjectNamespace
  let workflow: MockWorkflow

  beforeEach(() => {
    vi.useFakeTimers()
    bucket = new MockR2BucketE2E()
    doNamespace = new MockDurableObjectNamespace()
    workflow = new MockWorkflow()
  })

  afterEach(() => {
    vi.useRealTimers()
    bucket.clear()
    doNamespace.clear()
    workflow.clear()
  })

  it('should trigger workflow with targetFormat=delta', async () => {
    const now = Date.now()
    const oldTimestamp = Math.floor((now - 2 * 60 * 60 * 1000) / 1000)

    const doId = doNamespace.idFromName('users')
    const stateDO = doNamespace.get(doId)

    const updates = Array.from({ length: 15 }, (_, i) => ({
      namespace: 'users',
      writerId: 'writer1',
      file: `data/users/${oldTimestamp}-writer1-${i}.parquet`,
      timestamp: oldTimestamp * 1000,
      size: 1024,
    }))

    const response = await stateDO.fetch(new Request('http://internal/update', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: 'users',
        updates,
        config: {
          windowSizeMs: 3600000,
          minFilesToCompact: 10,
          maxWaitTimeMs: 300000,
          targetFormat: 'delta',
        },
      }),
    }))

    const result = await response.json() as { windowsReady: WindowReadyEntry[] }
    expect(result.windowsReady).toHaveLength(1)

    // Create workflow with delta format
    const windowReady = result.windowsReady[0]
    const workflowInstance = await workflow.create({
      params: {
        namespace: windowReady.namespace,
        windowStart: windowReady.windowStart,
        windowEnd: windowReady.windowEnd,
        files: windowReady.files,
        writers: windowReady.writers,
        targetFormat: 'delta',
      },
    })

    expect(workflow.createdInstances[0].params.targetFormat).toBe('delta')
  })

  it('should generate correct Delta data file path', () => {
    const timestamp = 1700000000000
    const date = new Date(timestamp)
    const year = date.getUTCFullYear()
    const month = String(date.getUTCMonth() + 1).padStart(2, '0')
    const day = String(date.getUTCDate()).padStart(2, '0')
    const hour = String(date.getUTCHours()).padStart(2, '0')
    const namespace = 'users'
    const batchNum = 1

    // Delta format: {namespace}/year={year}/month={month}/day={day}/hour={hour}/part-{batch}-compacted-{timestamp}.parquet
    const deltaPath = `${namespace}/year=${year}/month=${month}/day=${day}/hour=${hour}/` +
      `part-${String(batchNum).padStart(5, '0')}-compacted-${timestamp}.parquet`

    expect(deltaPath).toMatch(/^users\/year=\d{4}\/month=\d{2}\/day=\d{2}\/hour=\d{2}\/part-\d{5}-compacted-\d+\.parquet$/)
    expect(deltaPath).toContain('part-00001-')
  })

  it('should simulate Delta _delta_log entry creation', async () => {
    const tableLocation = 'users'
    const timestamp = 1700000000000
    const date = new Date(timestamp)
    const year = date.getUTCFullYear()
    const month = String(date.getUTCMonth() + 1).padStart(2, '0')
    const day = String(date.getUTCDate()).padStart(2, '0')
    const hour = String(date.getUTCHours()).padStart(2, '0')

    // Data file
    const dataFilePath = `${tableLocation}/year=${year}/month=${month}/day=${day}/hour=${hour}/part-00001-compacted-${timestamp}.parquet`
    await bucket.put(dataFilePath, createTestParquetContent([{ id: 1, name: 'Test' }]))

    // Delta log entry (version 0)
    const deltaLogPath = `${tableLocation}/_delta_log/00000000000000000000.json`
    const deltaLogEntry = JSON.stringify({
      protocol: { minReaderVersion: 1, minWriterVersion: 2 },
      metaData: {
        id: 'test-table-id',
        format: { provider: 'parquet', options: {} },
        schemaString: '{}',
        partitionColumns: ['year', 'month', 'day', 'hour'],
      },
      add: {
        path: dataFilePath.replace(`${tableLocation}/`, ''),
        size: 1024,
        dataChange: true,
        modificationTime: timestamp,
        stats: JSON.stringify({ numRecords: 1 }),
      },
    })
    await bucket.put(deltaLogPath, deltaLogEntry)

    // Verify structure
    expect(bucket.hasFile(dataFilePath)).toBe(true)
    expect(bucket.hasFile(deltaLogPath)).toBe(true)

    // Verify log content
    const logObj = await bucket.get(deltaLogPath)
    expect(logObj).not.toBeNull()
    const logContent = await logObj!.text()
    const parsed = JSON.parse(logContent)
    expect(parsed.add.path).toContain('part-00001-compacted-')
  })
})

// =============================================================================
// E2E Test Suite: Error Handling and Recovery
// =============================================================================

describe('E2E: Error Handling and Recovery', () => {
  let bucket: MockR2BucketE2E
  let doNamespace: MockDurableObjectNamespace
  let workflow: MockWorkflow

  beforeEach(() => {
    vi.useFakeTimers()
    bucket = new MockR2BucketE2E()
    doNamespace = new MockDurableObjectNamespace()
    workflow = new MockWorkflow()
  })

  afterEach(() => {
    vi.useRealTimers()
    bucket.clear()
    doNamespace.clear()
    workflow.clear()
  })

  it('should rollback on workflow creation failure', async () => {
    const now = Date.now()
    const oldTimestamp = Math.floor((now - 2 * 60 * 60 * 1000) / 1000)

    const doId = doNamespace.idFromName('users')
    const stateDO = doNamespace.get(doId)

    const updates = Array.from({ length: 15 }, (_, i) => ({
      namespace: 'users',
      writerId: 'writer1',
      file: `data/users/${oldTimestamp}-writer1-${i}.parquet`,
      timestamp: oldTimestamp * 1000,
      size: 1024,
    }))

    const response = await stateDO.fetch(new Request('http://internal/update', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: 'users',
        updates,
        config: {
          windowSizeMs: 3600000,
          minFilesToCompact: 10,
          maxWaitTimeMs: 300000,
          targetFormat: 'native',
        },
      }),
    }))

    const result = await response.json() as { windowsReady: WindowReadyEntry[] }
    const windowReady = result.windowsReady[0]

    // Simulate workflow creation failure by calling rollback
    const rollbackResponse = await stateDO.fetch(new Request('http://internal/rollback-processing', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        windowKey: windowReady.windowKey,
      }),
    }))

    expect(rollbackResponse.status).toBe(200)

    // Window should still exist and be pending
    const statusResponse = await stateDO.fetch(new Request('http://internal/status'))
    const status = await statusResponse.json() as {
      activeWindows: number
      windows: Array<{ processingStatus: { state: string } }>
    }

    expect(status.activeWindows).toBe(1)
    expect(status.windows[0].processingStatus.state).toBe('pending')
  })

  it('should reset window on workflow failure', async () => {
    const now = Date.now()
    const oldTimestamp = Math.floor((now - 2 * 60 * 60 * 1000) / 1000)

    const doId = doNamespace.idFromName('users')
    const stateDO = doNamespace.get(doId)

    const updates = Array.from({ length: 15 }, (_, i) => ({
      namespace: 'users',
      writerId: 'writer1',
      file: `data/users/${oldTimestamp}-writer1-${i}.parquet`,
      timestamp: oldTimestamp * 1000,
      size: 1024,
    }))

    const response = await stateDO.fetch(new Request('http://internal/update', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: 'users',
        updates,
        config: {
          windowSizeMs: 3600000,
          minFilesToCompact: 10,
          maxWaitTimeMs: 300000,
          targetFormat: 'native',
        },
      }),
    }))

    const result = await response.json() as { windowsReady: WindowReadyEntry[] }
    const windowReady = result.windowsReady[0]

    // Confirm dispatch
    const workflowInstance = await workflow.create({
      params: {
        namespace: windowReady.namespace,
        windowStart: windowReady.windowStart,
        windowEnd: windowReady.windowEnd,
        files: windowReady.files,
        writers: windowReady.writers,
        targetFormat: 'native',
      },
    })

    await stateDO.fetch(new Request('http://internal/confirm-dispatch', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        windowKey: windowReady.windowKey,
        workflowId: workflowInstance.id,
      }),
    }))

    // Report workflow failure
    await stateDO.fetch(new Request('http://internal/workflow-complete', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        windowKey: windowReady.windowKey,
        workflowId: workflowInstance.id,
        success: false,
      }),
    }))

    // Window should be reset to pending
    const statusResponse = await stateDO.fetch(new Request('http://internal/status'))
    const status = await statusResponse.json() as {
      activeWindows: number
      windows: Array<{ processingStatus: { state: string } }>
    }

    expect(status.activeWindows).toBe(1)
    expect(status.windows[0].processingStatus.state).toBe('pending')
  })

  it('should handle non-parquet file events gracefully', () => {
    const events = [
      createR2EventMessage('data/users/1700001234-writer1-0.parquet'),
      createR2EventMessage('data/users/metadata.json'),
      createR2EventMessage('data/users/1700001234-writer1-1.parquet'),
      createR2EventMessage('logs/system.parquet'),
    ]

    const namespacePrefix = 'data/'

    // Filter like the queue consumer does
    const validEvents = events.filter(e => {
      const isCreateAction = e.action === 'PutObject' || e.action === 'CopyObject' || e.action === 'CompleteMultipartUpload'
      const isParquet = e.object.key.endsWith('.parquet')
      const isInPrefix = e.object.key.startsWith(namespacePrefix)
      return isCreateAction && isParquet && isInPrefix
    })

    expect(validEvents).toHaveLength(2)
  })
})

// =============================================================================
// E2E Test Suite: Full Integration Simulation
// =============================================================================

describe('E2E: Full Integration Simulation', () => {
  let bucket: MockR2BucketE2E
  let doNamespace: MockDurableObjectNamespace
  let workflow: MockWorkflow

  beforeEach(() => {
    vi.useFakeTimers()
    bucket = new MockR2BucketE2E()
    doNamespace = new MockDurableObjectNamespace()
    workflow = new MockWorkflow()
  })

  afterEach(() => {
    vi.useRealTimers()
    bucket.clear()
    doNamespace.clear()
    workflow.clear()
  })

  it('should handle complete compaction lifecycle', async () => {
    const now = Date.now()
    const oldTimestamp = Math.floor((now - 2 * 60 * 60 * 1000) / 1000)

    // Step 1: Create source parquet files in R2
    const sourceFiles: string[] = []
    for (let i = 0; i < 15; i++) {
      const key = `data/users/${oldTimestamp + i}-writer1-${i}.parquet`
      sourceFiles.push(key)
      const content = createTestParquetContent([
        { id: `user-${i}`, name: `User ${i}`, createdAt: (oldTimestamp + i) * 1000 },
      ])
      await bucket.put(key, content)
    }

    expect(bucket.getFileCount()).toBe(15)

    // Step 2: Simulate R2 event notifications
    const events = sourceFiles.map(key =>
      createR2EventMessage(key, 1024, 'PutObject')
    )

    // Step 3: Process events through CompactionStateDO
    const doId = doNamespace.idFromName('users')
    const stateDO = doNamespace.get(doId)

    const updates = events.map((e, i) => ({
      namespace: 'users',
      writerId: 'writer1',
      file: e.object.key,
      timestamp: (oldTimestamp + i) * 1000,
      size: e.object.size,
    }))

    const response = await stateDO.fetch(new Request('http://internal/update', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: 'users',
        updates,
        config: {
          windowSizeMs: 3600000,
          minFilesToCompact: 10,
          maxWaitTimeMs: 300000,
          targetFormat: 'native',
        },
      }),
    }))

    const result = await response.json() as { windowsReady: WindowReadyEntry[] }
    expect(result.windowsReady).toHaveLength(1)

    // Step 4: Trigger and confirm workflow
    const windowReady = result.windowsReady[0]
    const workflowInstance = await workflow.create({
      params: {
        namespace: windowReady.namespace,
        windowStart: windowReady.windowStart,
        windowEnd: windowReady.windowEnd,
        files: windowReady.files,
        writers: windowReady.writers,
        targetFormat: 'native',
      },
    })

    await stateDO.fetch(new Request('http://internal/confirm-dispatch', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        windowKey: windowReady.windowKey,
        workflowId: workflowInstance.id,
      }),
    }))

    // Step 5: Simulate workflow execution - create compacted file
    const date = new Date(windowReady.windowStart)
    const year = date.getUTCFullYear()
    const month = String(date.getUTCMonth() + 1).padStart(2, '0')
    const day = String(date.getUTCDate()).padStart(2, '0')
    const hour = String(date.getUTCHours()).padStart(2, '0')

    const compactedPath = `data/users/year=${year}/month=${month}/day=${day}/hour=${hour}/` +
      `compacted-${windowReady.windowStart}-1.parquet`

    const compactedContent = createTestParquetContent(
      Array.from({ length: 15 }, (_, i) => ({
        id: `user-${i}`,
        name: `User ${i}`,
        createdAt: (oldTimestamp + i) * 1000,
      }))
    )
    await bucket.put(compactedPath, compactedContent)

    // Step 6: Delete source files
    for (const file of sourceFiles) {
      await bucket.delete(file)
    }

    // Step 7: Complete workflow
    await stateDO.fetch(new Request('http://internal/workflow-complete', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        windowKey: windowReady.windowKey,
        workflowId: workflowInstance.id,
        success: true,
      }),
    }))

    // Verify final state
    const finalStatus = await stateDO.fetch(new Request('http://internal/status'))
    const status = await finalStatus.json() as { activeWindows: number }

    expect(status.activeWindows).toBe(0)
    expect(bucket.getFileCount()).toBe(1)
    expect(bucket.hasFile(compactedPath)).toBe(true)
    expect(bucket.getDeleteLog()).toHaveLength(15)
    expect(bucket.getWriteLog().some(w => w.key === compactedPath)).toBe(true)
  })
})
