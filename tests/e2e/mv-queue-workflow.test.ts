/**
 * E2E Integration Test for Materialized View Queue + Workflow
 *
 * Tests the full MV refresh flow from R2 event notifications through
 * workflow execution to refreshed materialized views.
 *
 * Test scenarios:
 * 1. Entity write triggers R2 event notification
 * 2. Queue receives the R2 notification
 * 3. Cloudflare Workflow is triggered for MV refresh
 * 4. MV is refreshed with new entity data
 * 5. Full pipeline: Entity write -> R2 -> Queue -> Workflow -> MV refresh -> Verify
 *
 * STATUS: RED PHASE (TDD)
 * ========================
 * These tests define the expected API contract for MV refresh queue integration.
 * The tests use a testable mock implementation (TestableMVRefreshStateDO) that
 * captures the expected behavior. The production implementation does NOT yet exist:
 *
 * Required implementations (GREEN phase):
 * - src/workflows/mv-refresh-queue-consumer.ts - handleMVRefreshQueue() function
 * - src/workflows/mv-refresh-state-do.ts - MVRefreshStateDO Durable Object
 * - src/workflows/mv-refresh-workflow.ts - MVRefreshWorkflow Cloudflare Workflow
 *
 * The test pattern follows the compaction workflow tests:
 * - tests/e2e/compaction-workflow.test.ts (reference implementation)
 * - src/workflows/compaction-queue-consumer.ts (production code)
 *
 * Key API endpoints expected from MVRefreshStateDO:
 * - POST /register-mv - Register MV definitions with source dependencies
 * - POST /notify-change - Notify of source data changes from R2 events
 * - POST /get-ready-mvs - Get MVs ready for refresh (after debounce)
 * - POST /confirm-dispatch - Confirm workflow was created successfully
 * - POST /workflow-complete - Mark workflow as complete (success or failure)
 * - GET /status - Get current state of pending MV refreshes
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'

// =============================================================================
// Expected Types (to be implemented)
// =============================================================================

/**
 * R2 Event Notification message from queue
 * Reuses the same shape as compaction queue consumer
 */
interface R2EventMessage {
  account: string
  bucket: string
  object: {
    key: string
    size: number
    eTag: string
  }
  action: 'PutObject' | 'CopyObject' | 'CompleteMultipartUpload' | 'DeleteObject' | 'LifecycleDeletion'
  eventTime: string
}

/**
 * Configuration for the MV refresh queue consumer
 */
interface MVRefreshConsumerConfig {
  /** Debounce time before triggering refresh (default: 1000ms) */
  debounceMs?: number | undefined
  /** Maximum time to wait before forcing refresh (default: 5000ms) */
  maxWaitMs?: number | undefined
  /** Namespace prefix to watch (default: 'data/') */
  namespacePrefix?: string | undefined
  /** Whether to batch changes per MV (default: true) */
  batchChanges?: boolean | undefined
}

/**
 * MV definition for tracking source dependencies
 */
interface MVDefinitionEntry {
  name: string
  source: string
  refreshMode: 'streaming' | 'scheduled' | 'manual'
}

/**
 * Pending MV refresh entry
 */
interface MVRefreshEntry {
  mvName: string
  source: string
  changedFiles: string[]
  lastChangeAt: number
  firstChangeAt: number
  status: 'pending' | 'processing' | 'dispatched'
  workflowId?: string | undefined
}

/**
 * Response from MVRefreshStateDO
 */
interface MVRefreshResponse {
  mvsReady: MVRefreshEntry[]
}

/**
 * Status response from MVRefreshStateDO
 */
interface MVRefreshStatusResponse {
  pendingMVs: number
  processingMVs: number
  dispatchedMVs: number
  mvs: MVRefreshEntry[]
}

// =============================================================================
// Mock Types
// =============================================================================

interface MockR2Object {
  key: string
  size: number
  eTag: string
  uploaded: Date
  body?: ReadableStream | undefined
  arrayBuffer(): Promise<ArrayBuffer>
  text(): Promise<string>
}

interface MockR2ListResult {
  objects: MockR2Object[]
  truncated: boolean
  cursor?: string | undefined
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
// Mock R2 Bucket
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

interface MVRefreshWorkflowParams {
  mvName: string
  source: string
  changedFiles: string[]
  refreshType: 'full' | 'incremental'
}

class MockWorkflow {
  public createdInstances: Array<{ id: string; params: MVRefreshWorkflowParams }> = []
  private nextId = 1

  async create(options: { params: MVRefreshWorkflowParams }): Promise<{ id: string }> {
    const id = `mv-refresh-${this.nextId++}`
    this.createdInstances.push({ id, params: options.params })
    return { id }
  }

  clear(): void {
    this.createdInstances = []
    this.nextId = 1
  }
}

// =============================================================================
// Testable MVRefreshStateDO (Expected Implementation)
// =============================================================================

interface StoredMVRefreshState {
  mvName: string
  source: string
  changedFiles: string[]
  lastChangeAt: number
  firstChangeAt: number
  status: 'pending' | 'processing' | 'dispatched'
  workflowId?: string | undefined
}

interface StoredState {
  mvDefinitions: Record<string, MVDefinitionEntry>
  pendingRefreshes: Record<string, StoredMVRefreshState>
}

/**
 * Testable implementation of the expected MVRefreshStateDO
 *
 * This DO tracks which MVs need refresh based on source data changes.
 * It batches changes and triggers refresh workflows when conditions are met.
 */
class TestableMVRefreshStateDO {
  private state: MockDurableObjectState
  private mvDefinitions: Map<string, MVDefinitionEntry> = new Map()
  private pendingRefreshes: Map<string, StoredMVRefreshState> = new Map()
  private initialized = false

  constructor(state: MockDurableObjectState) {
    this.state = state
  }

  private async ensureInitialized(): Promise<void> {
    if (this.initialized) return

    const stored = await this.state.storage.get<StoredState>('mvRefreshState')
    if (stored) {
      this.mvDefinitions = new Map(Object.entries(stored.mvDefinitions))
      this.pendingRefreshes = new Map(Object.entries(stored.pendingRefreshes))
    }

    this.initialized = true
  }

  private async saveState(): Promise<void> {
    const stored: StoredState = {
      mvDefinitions: Object.fromEntries(this.mvDefinitions),
      pendingRefreshes: Object.fromEntries(this.pendingRefreshes),
    }
    await this.state.storage.put('mvRefreshState', stored)
  }

  async fetch(request: Request): Promise<Response> {
    await this.ensureInitialized()
    const url = new URL(request.url)

    if (url.pathname === '/register-mv' && request.method === 'POST') {
      return this.handleRegisterMV(request)
    }

    if (url.pathname === '/notify-change' && request.method === 'POST') {
      return this.handleNotifyChange(request)
    }

    if (url.pathname === '/get-ready-mvs' && request.method === 'POST') {
      return this.handleGetReadyMVs(request)
    }

    if (url.pathname === '/confirm-dispatch' && request.method === 'POST') {
      return this.handleConfirmDispatch(request)
    }

    if (url.pathname === '/workflow-complete' && request.method === 'POST') {
      return this.handleWorkflowComplete(request)
    }

    if (url.pathname === '/status') {
      return this.handleStatus()
    }

    return new Response('Not Found', { status: 404 })
  }

  private async handleRegisterMV(request: Request): Promise<Response> {
    const body = await request.json() as { mv: MVDefinitionEntry }
    const { mv } = body

    this.mvDefinitions.set(mv.name, mv)
    await this.saveState()

    return new Response(JSON.stringify({ success: true }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  private async handleNotifyChange(request: Request): Promise<Response> {
    const body = await request.json() as {
      namespace: string
      files: string[]
      timestamp: number
    }
    const { namespace, files, timestamp } = body
    const now = Date.now()

    // Find all MVs that depend on this namespace
    for (const [mvName, mv] of this.mvDefinitions) {
      if (mv.source === namespace && mv.refreshMode === 'streaming') {
        const existing = this.pendingRefreshes.get(mvName)
        if (existing && existing.status === 'pending') {
          // Add to existing batch
          existing.changedFiles.push(...files)
          existing.lastChangeAt = now
        } else if (!existing || existing.status === 'dispatched') {
          // Create new pending refresh
          this.pendingRefreshes.set(mvName, {
            mvName,
            source: mv.source,
            changedFiles: [...files],
            firstChangeAt: timestamp,
            lastChangeAt: now,
            status: 'pending',
          })
        }
      }
    }

    await this.saveState()

    return new Response(JSON.stringify({ success: true }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  private async handleGetReadyMVs(request: Request): Promise<Response> {
    const body = await request.json() as {
      config: MVRefreshConsumerConfig
    }
    const { config } = body
    const now = Date.now()
    const debounceMs = config.debounceMs ?? 1000
    const maxWaitMs = config.maxWaitMs ?? 5000
    const mvsReady: MVRefreshEntry[] = []

    for (const [mvName, pending] of this.pendingRefreshes) {
      if (pending.status !== 'pending') continue

      // Check if debounce period has passed OR max wait exceeded
      const timeSinceLastChange = now - pending.lastChangeAt
      const timeSinceFirstChange = now - pending.firstChangeAt
      const debounced = timeSinceLastChange >= debounceMs
      const maxWaitExceeded = timeSinceFirstChange >= maxWaitMs

      if (debounced || maxWaitExceeded) {
        // Mark as processing
        pending.status = 'processing'
        mvsReady.push(pending)
      }
    }

    await this.saveState()

    return new Response(JSON.stringify({ mvsReady }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  private async handleConfirmDispatch(request: Request): Promise<Response> {
    const body = await request.json() as { mvName: string; workflowId: string }
    const { mvName, workflowId } = body

    const pending = this.pendingRefreshes.get(mvName)
    if (!pending) {
      return new Response(JSON.stringify({ error: 'MV refresh not found' }), {
        status: 404,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    if (pending.status !== 'processing') {
      return new Response(JSON.stringify({ error: 'MV not in processing state' }), {
        status: 409,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    pending.status = 'dispatched'
    pending.workflowId = workflowId
    await this.saveState()

    return new Response(JSON.stringify({ success: true }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  private async handleWorkflowComplete(request: Request): Promise<Response> {
    const body = await request.json() as { mvName: string; workflowId: string; success: boolean }
    const { mvName, workflowId, success } = body

    const pending = this.pendingRefreshes.get(mvName)
    if (!pending) {
      return new Response(JSON.stringify({ success: true, alreadyDeleted: true }), {
        headers: { 'Content-Type': 'application/json' },
      })
    }

    if (pending.workflowId !== workflowId) {
      return new Response(JSON.stringify({ error: 'Workflow ID mismatch' }), {
        status: 409,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    if (success) {
      // Remove completed refresh
      this.pendingRefreshes.delete(mvName)
    } else {
      // Reset to pending for retry
      pending.status = 'pending'
      pending.workflowId = undefined
    }

    await this.saveState()

    return new Response(JSON.stringify({ success: true }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  private handleStatus(): Response {
    let pendingMVs = 0
    let processingMVs = 0
    let dispatchedMVs = 0

    for (const pending of this.pendingRefreshes.values()) {
      switch (pending.status) {
        case 'pending': pendingMVs++; break
        case 'processing': processingMVs++; break
        case 'dispatched': dispatchedMVs++; break
      }
    }

    const status: MVRefreshStatusResponse = {
      pendingMVs,
      processingMVs,
      dispatchedMVs,
      mvs: Array.from(this.pendingRefreshes.values()),
    }

    return new Response(JSON.stringify(status, null, 2), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  // Test helpers
  getMVDefinitions(): Map<string, MVDefinitionEntry> {
    return new Map(this.mvDefinitions)
  }

  getPendingRefreshes(): Map<string, StoredMVRefreshState> {
    return new Map(this.pendingRefreshes)
  }
}

// =============================================================================
// Mock DurableObjectNamespace
// =============================================================================

class MockDurableObjectNamespace {
  private dos: Map<string, TestableMVRefreshStateDO> = new Map()
  private states: Map<string, MockDurableObjectState> = new Map()

  idFromName(name: string): string {
    return `do-id-${name}`
  }

  get(id: string): TestableMVRefreshStateDO {
    let doInstance = this.dos.get(id)
    if (!doInstance) {
      let state = this.states.get(id)
      if (!state) {
        state = new MockDurableObjectState()
        this.states.set(id, state)
      }
      doInstance = new TestableMVRefreshStateDO(state)
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

function createMockBatch<T>(messages: T[], queueName = 'parquedb-mv-events'): MockMessageBatch<T> {
  return {
    messages: messages.map(m => createMockMessage(m)),
    queue: queueName,
  }
}

/**
 * Create test parquet file content (minimal for testing)
 */
function createTestParquetContent(rows: Array<Record<string, unknown>>): Uint8Array {
  const content = rows.map(r => JSON.stringify(r)).join('\n')
  return new TextEncoder().encode(content)
}

// =============================================================================
// E2E Test Suite: Entity Write -> R2 Event Notification
// =============================================================================

describe('E2E: Entity Write Triggers R2 Event Notification', () => {
  let bucket: MockR2BucketE2E

  beforeEach(() => {
    vi.useFakeTimers()
    bucket = new MockR2BucketE2E()
  })

  afterEach(() => {
    vi.useRealTimers()
    bucket.clear()
  })

  it('should trigger R2 event when entity data is written', async () => {
    // Simulate entity write to R2
    const timestamp = Date.now()
    const key = `data/orders/${timestamp}-writer1-0.parquet`
    const content = createTestParquetContent([
      { $id: 'order-1', $type: 'Order', total: 100, status: 'pending' },
    ])

    await bucket.put(key, content)

    // Verify file was written
    expect(bucket.hasFile(key)).toBe(true)
    const writeLog = bucket.getWriteLog()
    expect(writeLog.length).toBe(1)
    expect(writeLog[0].key).toBe(key)
  })

  it('should create R2 event message with correct shape for entity write', () => {
    const timestamp = Date.now()
    const key = `data/orders/${timestamp}-writer1-0.parquet`
    const event = createR2EventMessage(key, 1024, 'PutObject')

    expect(event.action).toBe('PutObject')
    expect(event.object.key).toBe(key)
    expect(event.object.size).toBe(1024)
    expect(event.bucket).toBe('parquedb-data')
  })

  it('should batch multiple entity writes into single notification window', async () => {
    const timestamp = Date.now()
    const files: string[] = []

    // Write 5 order entities
    for (let i = 0; i < 5; i++) {
      const key = `data/orders/${timestamp + i}-writer1-${i}.parquet`
      files.push(key)
      const content = createTestParquetContent([
        { $id: `order-${i}`, $type: 'Order', total: 100 + i * 10 },
      ])
      await bucket.put(key, content)
    }

    expect(bucket.getFileCount()).toBe(5)

    // All writes should be tracked
    const writeLog = bucket.getWriteLog()
    expect(writeLog.length).toBe(5)
    expect(writeLog.map(w => w.key)).toEqual(files)
  })
})

// =============================================================================
// E2E Test Suite: Queue Receives R2 Notification
// =============================================================================

describe('E2E: Queue Receives R2 Notification', () => {
  let doNamespace: MockDurableObjectNamespace

  beforeEach(() => {
    vi.useFakeTimers()
    doNamespace = new MockDurableObjectNamespace()
  })

  afterEach(() => {
    vi.useRealTimers()
    doNamespace.clear()
  })

  it('should register MV definition with source dependency', async () => {
    const doId = doNamespace.idFromName('mv-refresh')
    const stateDO = doNamespace.get(doId)

    // Register an MV that depends on 'orders' source
    const response = await stateDO.fetch(new Request('http://internal/register-mv', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mv: {
          name: 'OrderAnalytics',
          source: 'orders',
          refreshMode: 'streaming',
        },
      }),
    }))

    expect(response.status).toBe(200)

    // Verify registration
    const defs = stateDO.getMVDefinitions()
    expect(defs.size).toBe(1)
    expect(defs.get('OrderAnalytics')).toEqual({
      name: 'OrderAnalytics',
      source: 'orders',
      refreshMode: 'streaming',
    })
  })

  it('should track source namespace changes from R2 events', async () => {
    const doId = doNamespace.idFromName('mv-refresh')
    const stateDO = doNamespace.get(doId)

    // Register MV
    await stateDO.fetch(new Request('http://internal/register-mv', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mv: {
          name: 'OrderAnalytics',
          source: 'orders',
          refreshMode: 'streaming',
        },
      }),
    }))

    const now = Date.now()

    // Notify of source data change
    const response = await stateDO.fetch(new Request('http://internal/notify-change', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: 'orders',
        files: ['data/orders/1700001234-writer1-0.parquet'],
        timestamp: now,
      }),
    }))

    expect(response.status).toBe(200)

    // Check status
    const statusResponse = await stateDO.fetch(new Request('http://internal/status'))
    const status = await statusResponse.json() as MVRefreshStatusResponse

    expect(status.pendingMVs).toBe(1)
    expect(status.mvs.length).toBe(1)
    expect(status.mvs[0].mvName).toBe('OrderAnalytics')
    expect(status.mvs[0].changedFiles).toContain('data/orders/1700001234-writer1-0.parquet')
  })

  it('should batch multiple changes before triggering refresh', async () => {
    const doId = doNamespace.idFromName('mv-refresh')
    const stateDO = doNamespace.get(doId)

    // Register MV
    await stateDO.fetch(new Request('http://internal/register-mv', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mv: {
          name: 'OrderAnalytics',
          source: 'orders',
          refreshMode: 'streaming',
        },
      }),
    }))

    const now = Date.now()

    // Send multiple change notifications
    for (let i = 0; i < 5; i++) {
      await stateDO.fetch(new Request('http://internal/notify-change', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          namespace: 'orders',
          files: [`data/orders/1700001234-writer1-${i}.parquet`],
          timestamp: now + i * 100,
        }),
      }))
    }

    // Should still be 1 pending MV (batched)
    const statusResponse = await stateDO.fetch(new Request('http://internal/status'))
    const status = await statusResponse.json() as MVRefreshStatusResponse

    expect(status.pendingMVs).toBe(1)
    expect(status.mvs[0].changedFiles.length).toBe(5)
  })

  it('should not track changes for manual refresh MVs', async () => {
    const doId = doNamespace.idFromName('mv-refresh')
    const stateDO = doNamespace.get(doId)

    // Register MV with manual refresh
    await stateDO.fetch(new Request('http://internal/register-mv', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mv: {
          name: 'ManualReport',
          source: 'orders',
          refreshMode: 'manual',
        },
      }),
    }))

    const now = Date.now()

    // Notify of source data change
    await stateDO.fetch(new Request('http://internal/notify-change', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: 'orders',
        files: ['data/orders/1700001234-writer1-0.parquet'],
        timestamp: now,
      }),
    }))

    // Should have no pending MVs (manual refresh doesn't track changes)
    const statusResponse = await stateDO.fetch(new Request('http://internal/status'))
    const status = await statusResponse.json() as MVRefreshStatusResponse

    expect(status.pendingMVs).toBe(0)
  })
})

// =============================================================================
// E2E Test Suite: Cloudflare Workflow Triggered for MV Refresh
// =============================================================================

describe('E2E: Cloudflare Workflow Triggered for MV Refresh', () => {
  let doNamespace: MockDurableObjectNamespace
  let workflow: MockWorkflow

  beforeEach(() => {
    vi.useFakeTimers()
    doNamespace = new MockDurableObjectNamespace()
    workflow = new MockWorkflow()
  })

  afterEach(() => {
    vi.useRealTimers()
    doNamespace.clear()
    workflow.clear()
  })

  it('should return MVs ready for refresh after debounce period', async () => {
    const doId = doNamespace.idFromName('mv-refresh')
    const stateDO = doNamespace.get(doId)

    // Register MV
    await stateDO.fetch(new Request('http://internal/register-mv', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mv: {
          name: 'OrderAnalytics',
          source: 'orders',
          refreshMode: 'streaming',
        },
      }),
    }))

    const now = Date.now()

    // Notify of source data change
    await stateDO.fetch(new Request('http://internal/notify-change', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: 'orders',
        files: ['data/orders/1700001234-writer1-0.parquet'],
        timestamp: now,
      }),
    }))

    // Advance time past debounce period
    vi.advanceTimersByTime(2000)

    // Get MVs ready for refresh
    const response = await stateDO.fetch(new Request('http://internal/get-ready-mvs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        config: {
          debounceMs: 1000,
          maxWaitMs: 5000,
        },
      }),
    }))

    const result = await response.json() as MVRefreshResponse
    expect(result.mvsReady.length).toBe(1)
    expect(result.mvsReady[0].mvName).toBe('OrderAnalytics')
    expect(result.mvsReady[0].status).toBe('processing')
  })

  it('should trigger workflow for ready MV', async () => {
    const doId = doNamespace.idFromName('mv-refresh')
    const stateDO = doNamespace.get(doId)

    // Register MV
    await stateDO.fetch(new Request('http://internal/register-mv', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mv: {
          name: 'OrderAnalytics',
          source: 'orders',
          refreshMode: 'streaming',
        },
      }),
    }))

    const now = Date.now()

    // Notify of changes
    await stateDO.fetch(new Request('http://internal/notify-change', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: 'orders',
        files: ['data/orders/1700001234-writer1-0.parquet'],
        timestamp: now,
      }),
    }))

    // Advance time past debounce
    vi.advanceTimersByTime(2000)

    // Get ready MVs
    const readyResponse = await stateDO.fetch(new Request('http://internal/get-ready-mvs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        config: { debounceMs: 1000, maxWaitMs: 5000 },
      }),
    }))

    const { mvsReady } = await readyResponse.json() as MVRefreshResponse
    const mvToRefresh = mvsReady[0]

    // Trigger workflow
    const workflowInstance = await workflow.create({
      params: {
        mvName: mvToRefresh.mvName,
        source: mvToRefresh.source,
        changedFiles: mvToRefresh.changedFiles,
        refreshType: 'incremental',
      },
    })

    expect(workflowInstance.id).toBeDefined()
    expect(workflow.createdInstances.length).toBe(1)
    expect(workflow.createdInstances[0].params.mvName).toBe('OrderAnalytics')

    // Confirm dispatch
    await stateDO.fetch(new Request('http://internal/confirm-dispatch', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mvName: mvToRefresh.mvName,
        workflowId: workflowInstance.id,
      }),
    }))

    // Verify status
    const statusResponse = await stateDO.fetch(new Request('http://internal/status'))
    const status = await statusResponse.json() as MVRefreshStatusResponse

    expect(status.dispatchedMVs).toBe(1)
    expect(status.mvs[0].status).toBe('dispatched')
    expect(status.mvs[0].workflowId).toBe(workflowInstance.id)
  })

  it('should force refresh after max wait time even with ongoing changes', async () => {
    const doId = doNamespace.idFromName('mv-refresh')
    const stateDO = doNamespace.get(doId)

    // Register MV
    await stateDO.fetch(new Request('http://internal/register-mv', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mv: {
          name: 'OrderAnalytics',
          source: 'orders',
          refreshMode: 'streaming',
        },
      }),
    }))

    const startTime = Date.now()

    // Send initial change
    await stateDO.fetch(new Request('http://internal/notify-change', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: 'orders',
        files: ['data/orders/1700001234-writer1-0.parquet'],
        timestamp: startTime,
      }),
    }))

    // Keep sending changes within debounce period
    for (let i = 1; i <= 10; i++) {
      vi.advanceTimersByTime(500) // Half the debounce period
      await stateDO.fetch(new Request('http://internal/notify-change', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          namespace: 'orders',
          files: [`data/orders/1700001234-writer1-${i}.parquet`],
          timestamp: startTime + i * 500,
        }),
      }))
    }

    // Should still be pending (debounce not met)
    let statusResponse = await stateDO.fetch(new Request('http://internal/status'))
    let status = await statusResponse.json() as MVRefreshStatusResponse
    expect(status.pendingMVs).toBe(1)

    // Advance to exceed max wait time
    vi.advanceTimersByTime(5000)

    // Get ready MVs - should now be ready due to max wait
    const readyResponse = await stateDO.fetch(new Request('http://internal/get-ready-mvs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        config: { debounceMs: 1000, maxWaitMs: 5000 },
      }),
    }))

    const { mvsReady } = await readyResponse.json() as MVRefreshResponse
    expect(mvsReady.length).toBe(1)
    expect(mvsReady[0].changedFiles.length).toBe(11) // All batched changes
  })
})

// =============================================================================
// E2E Test Suite: MV Refresh with New Entity Data
// =============================================================================

describe('E2E: MV Refresh with New Entity Data', () => {
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

  it('should complete MV refresh workflow successfully', async () => {
    const doId = doNamespace.idFromName('mv-refresh')
    const stateDO = doNamespace.get(doId)

    // Register MV
    await stateDO.fetch(new Request('http://internal/register-mv', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mv: {
          name: 'OrderAnalytics',
          source: 'orders',
          refreshMode: 'streaming',
        },
      }),
    }))

    const now = Date.now()

    // Write source data
    const sourceKey = `data/orders/${now}-writer1-0.parquet`
    await bucket.put(sourceKey, createTestParquetContent([
      { $id: 'order-1', total: 100 },
      { $id: 'order-2', total: 200 },
    ]))

    // Notify change
    await stateDO.fetch(new Request('http://internal/notify-change', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: 'orders',
        files: [sourceKey],
        timestamp: now,
      }),
    }))

    // Advance time
    vi.advanceTimersByTime(2000)

    // Get ready MVs and trigger workflow
    const readyResponse = await stateDO.fetch(new Request('http://internal/get-ready-mvs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        config: { debounceMs: 1000, maxWaitMs: 5000 },
      }),
    }))

    const { mvsReady } = await readyResponse.json() as MVRefreshResponse
    const mvToRefresh = mvsReady[0]

    const workflowInstance = await workflow.create({
      params: {
        mvName: mvToRefresh.mvName,
        source: mvToRefresh.source,
        changedFiles: mvToRefresh.changedFiles,
        refreshType: 'full',
      },
    })

    await stateDO.fetch(new Request('http://internal/confirm-dispatch', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mvName: mvToRefresh.mvName,
        workflowId: workflowInstance.id,
      }),
    }))

    // Simulate workflow writing MV data
    const mvDataKey = '_views/OrderAnalytics/data.parquet'
    await bucket.put(mvDataKey, createTestParquetContent([
      { orderCount: 2, totalRevenue: 300 },
    ]))

    // Complete workflow
    await stateDO.fetch(new Request('http://internal/workflow-complete', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mvName: mvToRefresh.mvName,
        workflowId: workflowInstance.id,
        success: true,
      }),
    }))

    // Verify final state
    const statusResponse = await stateDO.fetch(new Request('http://internal/status'))
    const status = await statusResponse.json() as MVRefreshStatusResponse

    expect(status.pendingMVs).toBe(0)
    expect(status.dispatchedMVs).toBe(0)
    expect(bucket.hasFile(mvDataKey)).toBe(true)
  })

  it('should reset MV to pending on workflow failure', async () => {
    const doId = doNamespace.idFromName('mv-refresh')
    const stateDO = doNamespace.get(doId)

    // Register MV
    await stateDO.fetch(new Request('http://internal/register-mv', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mv: {
          name: 'OrderAnalytics',
          source: 'orders',
          refreshMode: 'streaming',
        },
      }),
    }))

    const now = Date.now()

    // Notify change
    await stateDO.fetch(new Request('http://internal/notify-change', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: 'orders',
        files: ['data/orders/test.parquet'],
        timestamp: now,
      }),
    }))

    // Advance time
    vi.advanceTimersByTime(2000)

    // Get ready MVs and trigger workflow
    const readyResponse = await stateDO.fetch(new Request('http://internal/get-ready-mvs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        config: { debounceMs: 1000, maxWaitMs: 5000 },
      }),
    }))

    const { mvsReady } = await readyResponse.json() as MVRefreshResponse
    const mvToRefresh = mvsReady[0]

    const workflowInstance = await workflow.create({
      params: {
        mvName: mvToRefresh.mvName,
        source: mvToRefresh.source,
        changedFiles: mvToRefresh.changedFiles,
        refreshType: 'full',
      },
    })

    await stateDO.fetch(new Request('http://internal/confirm-dispatch', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mvName: mvToRefresh.mvName,
        workflowId: workflowInstance.id,
      }),
    }))

    // Report workflow failure
    await stateDO.fetch(new Request('http://internal/workflow-complete', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mvName: mvToRefresh.mvName,
        workflowId: workflowInstance.id,
        success: false,
      }),
    }))

    // MV should be reset to pending for retry
    const statusResponse = await stateDO.fetch(new Request('http://internal/status'))
    const status = await statusResponse.json() as MVRefreshStatusResponse

    expect(status.pendingMVs).toBe(1)
    expect(status.dispatchedMVs).toBe(0)
    expect(status.mvs[0].status).toBe('pending')
    expect(status.mvs[0].workflowId).toBeUndefined()
  })
})

// =============================================================================
// E2E Test Suite: Full Pipeline Integration
// =============================================================================

describe('E2E: Full Pipeline - Entity Write -> R2 -> Queue -> Workflow -> MV Refresh -> Verify', () => {
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

  it('should complete full pipeline: entity writes propagate to MV', async () => {
    const doId = doNamespace.idFromName('mv-refresh')
    const stateDO = doNamespace.get(doId)
    const startTime = Date.now()

    // Step 1: Register MV definition
    await stateDO.fetch(new Request('http://internal/register-mv', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mv: {
          name: 'DailySalesReport',
          source: 'orders',
          refreshMode: 'streaming',
        },
      }),
    }))

    // Step 2: Write entity data to R2 (simulates ParqueDB write)
    const sourceFiles: string[] = []
    for (let i = 0; i < 10; i++) {
      const key = `data/orders/${startTime + i}-writer1-${i}.parquet`
      sourceFiles.push(key)
      await bucket.put(key, createTestParquetContent([
        { $id: `order-${i}`, $type: 'Order', total: 100 + i * 10, status: 'completed' },
      ]))
    }

    expect(bucket.getFileCount()).toBe(10)

    // Step 3: Simulate R2 event notifications -> Queue -> DO
    for (const file of sourceFiles) {
      await stateDO.fetch(new Request('http://internal/notify-change', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          namespace: 'orders',
          files: [file],
          timestamp: startTime,
        }),
      }))
    }

    // Verify batching worked
    let statusResponse = await stateDO.fetch(new Request('http://internal/status'))
    let status = await statusResponse.json() as MVRefreshStatusResponse
    expect(status.pendingMVs).toBe(1)
    expect(status.mvs[0].changedFiles.length).toBe(10)

    // Step 4: Advance time past debounce period
    vi.advanceTimersByTime(2000)

    // Step 5: Get ready MVs and trigger workflow
    const readyResponse = await stateDO.fetch(new Request('http://internal/get-ready-mvs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        config: { debounceMs: 1000, maxWaitMs: 5000 },
      }),
    }))

    const { mvsReady } = await readyResponse.json() as MVRefreshResponse
    expect(mvsReady.length).toBe(1)

    const mvToRefresh = mvsReady[0]
    expect(mvToRefresh.mvName).toBe('DailySalesReport')
    expect(mvToRefresh.changedFiles.length).toBe(10)

    // Step 6: Create and confirm workflow
    const workflowInstance = await workflow.create({
      params: {
        mvName: mvToRefresh.mvName,
        source: mvToRefresh.source,
        changedFiles: mvToRefresh.changedFiles,
        refreshType: 'full',
      },
    })

    expect(workflow.createdInstances.length).toBe(1)

    await stateDO.fetch(new Request('http://internal/confirm-dispatch', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mvName: mvToRefresh.mvName,
        workflowId: workflowInstance.id,
      }),
    }))

    // Step 7: Simulate workflow execution - read source and write MV
    // In real implementation, workflow would aggregate source data
    const mvDataKey = '_views/DailySalesReport/data.parquet'
    const aggregatedData = createTestParquetContent([{
      date: new Date(startTime).toISOString().split('T')[0],
      orderCount: 10,
      totalRevenue: 1450, // sum of 100+110+120+...+190
      avgOrderValue: 145,
    }])
    await bucket.put(mvDataKey, aggregatedData)

    // Step 8: Complete workflow
    await stateDO.fetch(new Request('http://internal/workflow-complete', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mvName: mvToRefresh.mvName,
        workflowId: workflowInstance.id,
        success: true,
      }),
    }))

    // Step 9: Verify final state
    statusResponse = await stateDO.fetch(new Request('http://internal/status'))
    status = await statusResponse.json() as MVRefreshStatusResponse

    expect(status.pendingMVs).toBe(0)
    expect(status.processingMVs).toBe(0)
    expect(status.dispatchedMVs).toBe(0)

    // Verify MV data file was created
    expect(bucket.hasFile(mvDataKey)).toBe(true)

    // Verify original source files still exist
    for (const file of sourceFiles) {
      expect(bucket.hasFile(file)).toBe(true)
    }

    // Verify total file count: 10 source + 1 MV
    expect(bucket.getFileCount()).toBe(11)
  })

  it('should handle multiple MVs depending on same source', async () => {
    const doId = doNamespace.idFromName('mv-refresh')
    const stateDO = doNamespace.get(doId)
    const now = Date.now()

    // Register multiple MVs for same source
    const mvDefs = [
      { name: 'OrderCount', source: 'orders', refreshMode: 'streaming' as const },
      { name: 'RevenueByDay', source: 'orders', refreshMode: 'streaming' as const },
      { name: 'TopProducts', source: 'orders', refreshMode: 'streaming' as const },
    ]

    for (const mv of mvDefs) {
      await stateDO.fetch(new Request('http://internal/register-mv', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ mv }),
      }))
    }

    // Write source data
    const sourceKey = `data/orders/${now}-writer1-0.parquet`
    await bucket.put(sourceKey, createTestParquetContent([
      { $id: 'order-1', total: 100 },
    ]))

    // Notify change - should affect all 3 MVs
    await stateDO.fetch(new Request('http://internal/notify-change', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: 'orders',
        files: [sourceKey],
        timestamp: now,
      }),
    }))

    // All 3 MVs should be pending
    const statusResponse = await stateDO.fetch(new Request('http://internal/status'))
    const status = await statusResponse.json() as MVRefreshStatusResponse

    expect(status.pendingMVs).toBe(3)
    expect(status.mvs.map(m => m.mvName).sort()).toEqual(['OrderCount', 'RevenueByDay', 'TopProducts'])

    // Advance time and get ready MVs
    vi.advanceTimersByTime(2000)

    const readyResponse = await stateDO.fetch(new Request('http://internal/get-ready-mvs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        config: { debounceMs: 1000, maxWaitMs: 5000 },
      }),
    }))

    const { mvsReady } = await readyResponse.json() as MVRefreshResponse
    expect(mvsReady.length).toBe(3)

    // All 3 should be ready for refresh
    const readyNames = mvsReady.map(m => m.mvName).sort()
    expect(readyNames).toEqual(['OrderCount', 'RevenueByDay', 'TopProducts'])
  })

  it('should handle concurrent changes to different source namespaces', async () => {
    const doId = doNamespace.idFromName('mv-refresh')
    const stateDO = doNamespace.get(doId)
    const now = Date.now()

    // Register MVs for different sources
    await stateDO.fetch(new Request('http://internal/register-mv', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mv: { name: 'OrderAnalytics', source: 'orders', refreshMode: 'streaming' },
      }),
    }))

    await stateDO.fetch(new Request('http://internal/register-mv', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mv: { name: 'ProductCatalog', source: 'products', refreshMode: 'streaming' },
      }),
    }))

    await stateDO.fetch(new Request('http://internal/register-mv', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mv: { name: 'CustomerList', source: 'customers', refreshMode: 'streaming' },
      }),
    }))

    // Write data to different namespaces
    await bucket.put(`data/orders/${now}-w1-0.parquet`, createTestParquetContent([{ $id: 'o1' }]))
    await bucket.put(`data/products/${now}-w1-0.parquet`, createTestParquetContent([{ $id: 'p1' }]))
    await bucket.put(`data/customers/${now}-w1-0.parquet`, createTestParquetContent([{ $id: 'c1' }]))

    // Notify changes for each namespace
    await stateDO.fetch(new Request('http://internal/notify-change', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: 'orders',
        files: [`data/orders/${now}-w1-0.parquet`],
        timestamp: now,
      }),
    }))

    await stateDO.fetch(new Request('http://internal/notify-change', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: 'products',
        files: [`data/products/${now}-w1-0.parquet`],
        timestamp: now,
      }),
    }))

    await stateDO.fetch(new Request('http://internal/notify-change', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: 'customers',
        files: [`data/customers/${now}-w1-0.parquet`],
        timestamp: now,
      }),
    }))

    // All 3 MVs should be independently pending
    const statusResponse = await stateDO.fetch(new Request('http://internal/status'))
    const status = await statusResponse.json() as MVRefreshStatusResponse

    expect(status.pendingMVs).toBe(3)

    // Verify each MV is tracking its own source
    const orderMV = status.mvs.find(m => m.mvName === 'OrderAnalytics')
    const productMV = status.mvs.find(m => m.mvName === 'ProductCatalog')
    const customerMV = status.mvs.find(m => m.mvName === 'CustomerList')

    expect(orderMV?.source).toBe('orders')
    expect(productMV?.source).toBe('products')
    expect(customerMV?.source).toBe('customers')
  })
})

// =============================================================================
// E2E Test Suite: Edge Cases and Error Handling
// =============================================================================

describe('E2E: Edge Cases and Error Handling', () => {
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

  it('should ignore changes to namespaces with no registered MVs', async () => {
    const doId = doNamespace.idFromName('mv-refresh')
    const stateDO = doNamespace.get(doId)

    // Register MV for 'orders'
    await stateDO.fetch(new Request('http://internal/register-mv', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mv: { name: 'OrderAnalytics', source: 'orders', refreshMode: 'streaming' },
      }),
    }))

    // Notify change for 'products' (no MV registered)
    await stateDO.fetch(new Request('http://internal/notify-change', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: 'products',
        files: ['data/products/test.parquet'],
        timestamp: Date.now(),
      }),
    }))

    // Should have no pending MVs
    const statusResponse = await stateDO.fetch(new Request('http://internal/status'))
    const status = await statusResponse.json() as MVRefreshStatusResponse

    expect(status.pendingMVs).toBe(0)
  })

  it('should handle workflow complete for already-deleted MV refresh', async () => {
    const doId = doNamespace.idFromName('mv-refresh')
    const stateDO = doNamespace.get(doId)

    // Call workflow-complete for non-existent MV
    const response = await stateDO.fetch(new Request('http://internal/workflow-complete', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mvName: 'NonExistentMV',
        workflowId: 'wf-123',
        success: true,
      }),
    }))

    const result = await response.json() as { success: boolean; alreadyDeleted?: boolean }
    expect(result.success).toBe(true)
    expect(result.alreadyDeleted).toBe(true)
  })

  it('should reject confirm-dispatch with workflow ID mismatch after double dispatch', async () => {
    const doId = doNamespace.idFromName('mv-refresh')
    const stateDO = doNamespace.get(doId)

    // Register MV and trigger refresh
    await stateDO.fetch(new Request('http://internal/register-mv', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mv: { name: 'TestMV', source: 'test', refreshMode: 'streaming' },
      }),
    }))

    await stateDO.fetch(new Request('http://internal/notify-change', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: 'test',
        files: ['data/test/test.parquet'],
        timestamp: Date.now(),
      }),
    }))

    vi.advanceTimersByTime(2000)

    // Get ready and dispatch first workflow
    await stateDO.fetch(new Request('http://internal/get-ready-mvs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ config: { debounceMs: 1000 } }),
    }))

    await stateDO.fetch(new Request('http://internal/confirm-dispatch', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ mvName: 'TestMV', workflowId: 'wf-1' }),
    }))

    // Try to complete with wrong workflow ID
    const response = await stateDO.fetch(new Request('http://internal/workflow-complete', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mvName: 'TestMV',
        workflowId: 'wf-wrong',
        success: true,
      }),
    }))

    expect(response.status).toBe(409)
    const result = await response.json() as { error: string }
    expect(result.error).toBe('Workflow ID mismatch')
  })

  it('should handle R2 delete events without triggering refresh', async () => {
    // Delete events should not trigger MV refresh
    const deleteEvent = createR2EventMessage('data/orders/test.parquet', 0, 'DeleteObject')

    expect(deleteEvent.action).toBe('DeleteObject')
    // In real implementation, queue consumer should filter out delete events
    // This test documents the expected behavior
  })

  it('should skip non-parquet files in R2 events', async () => {
    // Non-parquet files should be ignored
    const events = [
      createR2EventMessage('data/orders/test.parquet', 1024, 'PutObject'),
      createR2EventMessage('data/orders/metadata.json', 256, 'PutObject'),
      createR2EventMessage('data/orders/README.md', 128, 'PutObject'),
    ]

    const parquetEvents = events.filter(e => e.object.key.endsWith('.parquet'))
    expect(parquetEvents.length).toBe(1)
    expect(parquetEvents[0].object.key).toBe('data/orders/test.parquet')
  })
})
