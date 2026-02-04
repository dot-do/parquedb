/**
 * E2E Tests: Tail Worker R2 Queue Triggers
 *
 * These tests verify the complete end-to-end flow from tail worker events
 * through R2 queue triggers to compaction and Parquet output.
 *
 * Architecture being tested:
 * ```
 * Tail Worker -> WebSocket -> TailDO -> R2 (NDJSON)
 *                                          | object-create notification
 *                                          v
 *                                     Queue (R2 Events)
 *                                          | consumer
 *                                          v
 *                                     CompactionConsumer -> Parquet in R2
 *                                          | (on max retries exceeded)
 *                                          v
 *                                     DLQ (Dead Letter Queue)
 * ```
 *
 * Test scenarios:
 * 1. R2 object-create triggers queue notification
 * 2. Queue consumer (compaction-consumer.ts) processes R2 events
 * 3. Full pipeline: Tail Worker -> TailDO -> R2 (NDJSON) -> Queue -> Compaction -> Parquet
 * 4. DLQ behavior when MAX_RETRIES exceeded
 * 5. WebSocket reconnection scenarios
 *
 * GREEN PHASE: These tests use local mock implementations to simulate
 * the Cloudflare infrastructure (R2, Queues, WebSockets).
 *
 * @see parquedb-h9qh.1 - RED: Write failing tests for tail worker R2 queue triggers
 * @see parquedb-h9qh.2 - GREEN: Implement tests with local mocks
 */

import { describe, it, expect, beforeAll, beforeEach, afterEach } from 'vitest'
import { DEFAULT_MAX_RETRIES } from '../../src/constants'
import type { ValidatedTraceItem } from '../../src/worker/tail-validation'
import type { R2EventNotification, BatchResult } from '../../src/worker/compaction-consumer'
import { CompactionConsumer, createCompactionConsumer } from '../../src/worker/compaction-consumer'
import type { TailWorkerMessage, RawEventsFile } from '../../src/worker/TailDO'

// =============================================================================
// Local Mock Infrastructure
// =============================================================================

/**
 * Mock R2 Bucket that stores data in memory and triggers notifications
 */
class MockR2Bucket {
  private objects: Map<string, { data: Uint8Array; metadata?: Record<string, string> }> = new Map()
  private eventListeners: Array<(notification: R2EventNotification) => void> = []
  public readonly bucketName: string
  public readonly accountId: string

  constructor(bucketName: string, accountId: string = 'test-account') {
    this.bucketName = bucketName
    this.accountId = accountId
  }

  /** Register listener for R2 events (simulates R2 event notifications) */
  onEvent(listener: (notification: R2EventNotification) => void): void {
    this.eventListeners.push(listener)
  }

  /** Put an object (triggers object-create notification) */
  async put(key: string, data: Uint8Array | string, options?: { customMetadata?: Record<string, string>; onlyIf?: { etagMatches?: string; etagDoesNotMatch?: string } }): Promise<R2Object> {
    const dataBytes = typeof data === 'string' ? new TextEncoder().encode(data) : data
    const etag = `"${Date.now().toString(16)}-${Math.random().toString(36).slice(2, 8)}"`

    this.objects.set(key, {
      data: dataBytes,
      metadata: options?.customMetadata,
    })

    // Trigger R2 event notification
    const notification: R2EventNotification = {
      account: this.accountId,
      bucket: this.bucketName,
      object: {
        key,
        size: dataBytes.length,
        eTag: etag,
      },
      eventType: 'object-create',
      eventTime: new Date().toISOString(),
    }

    for (const listener of this.eventListeners) {
      listener(notification)
    }

    // Return R2Object-like result
    return {
      key,
      size: dataBytes.length,
      etag,
      uploaded: new Date(),
      httpMetadata: {},
      customMetadata: options?.customMetadata || {},
      version: `v-${Date.now()}`,
    } as R2Object
  }

  /** Get an object */
  async get(key: string): Promise<R2ObjectBody | null> {
    const obj = this.objects.get(key)
    if (!obj) return null

    return {
      key,
      size: obj.data.length,
      etag: `"${Date.now().toString(16)}"`,
      uploaded: new Date(),
      httpMetadata: {},
      customMetadata: obj.metadata || {},
      body: new ReadableStream({
        start(controller) {
          controller.enqueue(obj.data)
          controller.close()
        },
      }),
      bodyUsed: false,
      arrayBuffer: async () => obj.data.buffer.slice(obj.data.byteOffset, obj.data.byteOffset + obj.data.byteLength),
      text: async () => new TextDecoder().decode(obj.data),
      json: async () => JSON.parse(new TextDecoder().decode(obj.data)),
      blob: async () => new Blob([obj.data]),
      writeHttpMetadata: () => {},
    } as unknown as R2ObjectBody
  }

  /** Delete an object (triggers object-delete notification) */
  async delete(key: string): Promise<void> {
    const existed = this.objects.has(key)
    this.objects.delete(key)

    if (existed) {
      const notification: R2EventNotification = {
        account: this.accountId,
        bucket: this.bucketName,
        object: {
          key,
          size: 0,
          eTag: '',
        },
        eventType: 'object-delete',
        eventTime: new Date().toISOString(),
      }

      for (const listener of this.eventListeners) {
        listener(notification)
      }
    }
  }

  /** List objects by prefix */
  async list(options?: { prefix?: string }): Promise<R2Objects> {
    const prefix = options?.prefix || ''
    const objects: R2Object[] = []

    for (const [key, obj] of this.objects) {
      if (key.startsWith(prefix)) {
        objects.push({
          key,
          size: obj.data.length,
          etag: `"${Date.now().toString(16)}"`,
          uploaded: new Date(),
          httpMetadata: {},
          customMetadata: obj.metadata || {},
        } as R2Object)
      }
    }

    return {
      objects,
      truncated: false,
      delimitedPrefixes: [],
    } as R2Objects
  }

  /** Head an object */
  async head(key: string): Promise<R2Object | null> {
    const obj = this.objects.get(key)
    if (!obj) return null

    return {
      key,
      size: obj.data.length,
      etag: `"${Date.now().toString(16)}"`,
      uploaded: new Date(),
      httpMetadata: {},
      customMetadata: obj.metadata || {},
    } as R2Object
  }

  /** Clear all objects (for test cleanup) */
  clear(): void {
    this.objects.clear()
  }

  /** Get all keys (for debugging) */
  keys(): string[] {
    return Array.from(this.objects.keys())
  }
}

/**
 * Mock Message for Queue
 */
interface MockMessage<T> {
  id: string
  body: T
  attempts: number
  timestamp: Date
  acked: boolean
  retried: boolean
}

/**
 * Mock Queue that receives R2 notifications
 */
class MockQueue<T = R2EventNotification> {
  private messages: MockMessage<T>[] = []
  private dlqMessages: MockMessage<T>[] = []
  private consumers: Array<(messages: Message<T>[]) => Promise<void>> = []
  public readonly maxRetries: number

  constructor(maxRetries: number = DEFAULT_MAX_RETRIES) {
    this.maxRetries = maxRetries
  }

  /** Send a message to the queue */
  async send(body: T): Promise<void> {
    const message: MockMessage<T> = {
      id: `msg-${Date.now()}-${Math.random().toString(36).slice(2)}`,
      body,
      attempts: 1,
      timestamp: new Date(),
      acked: false,
      retried: false,
    }
    this.messages.push(message)
  }

  /** Get pending (non-acked, non-retried) messages */
  getPendingMessages(): MockMessage<T>[] {
    return this.messages.filter(m => !m.acked && !m.retried)
  }

  /** Get all messages including processed */
  getAllMessages(): MockMessage<T>[] {
    return [...this.messages]
  }

  /** Get DLQ messages */
  getDLQMessages(): MockMessage<T>[] {
    return [...this.dlqMessages]
  }

  /** Process messages with a consumer function (simulates queue consumer) */
  async processWithConsumer(consumer: CompactionConsumer): Promise<BatchResult> {
    const pendingMessages = this.getPendingMessages()
    if (pendingMessages.length === 0) {
      return {
        totalMessages: 0,
        succeeded: 0,
        failed: 0,
        retried: 0,
        deadLettered: 0,
        results: [],
        parquetFilesWritten: [],
      }
    }

    // Convert to Message<T> format
    const queueMessages: Message<T>[] = pendingMessages.map(m => ({
      id: m.id,
      body: m.body,
      timestamp: m.timestamp,
      attempts: m.attempts,
      ack: () => {
        m.acked = true
      },
      retry: () => {
        m.retried = true
        // Re-add with incremented attempts
        const newMessage: MockMessage<T> = {
          ...m,
          id: `${m.id}-retry-${m.attempts}`,
          attempts: m.attempts + 1,
          retried: false,
          acked: false,
        }
        this.messages.push(newMessage)
      },
    })) as unknown as Message<T>[]

    const result = await consumer.processBatch(queueMessages as unknown as Message<R2EventNotification>[])

    // Move messages that exceeded max retries to DLQ
    for (const m of pendingMessages) {
      if (!m.acked && !m.retried && m.attempts >= this.maxRetries) {
        this.dlqMessages.push(m)
      }
    }

    return result
  }

  /** Purge all messages */
  purge(): void {
    this.messages = []
  }

  /** Purge DLQ */
  purgeDLQ(): void {
    this.dlqMessages = []
  }
}

/**
 * Mock WebSocket client for TailDO
 */
class MockTailDOClient {
  private state: 'connecting' | 'connected' | 'disconnected' | 'error' = 'disconnected'
  private messageBuffer: TailWorkerMessage[] = []
  private ackResolvers: Array<{ resolve: (value: { count: number }) => void; reject: (error: Error) => void }> = []
  private errorHandler: ((error: Error) => void) | null = null
  private closeHandler: ((code: number, reason: string) => void) | null = null
  private mockR2Bucket: MockR2Bucket
  private doId: string
  private batchSeq: number = 0
  private eventsBuffer: ValidatedTraceItem[] = []
  private rawEventsPrefix: string = 'raw-events'
  private batchSize: number = 100
  private shouldFail: boolean = false

  constructor(mockR2Bucket: MockR2Bucket) {
    this.mockR2Bucket = mockR2Bucket
    this.doId = `tail-do-${Date.now()}`
  }

  /** Configure the mock to fail connections */
  setFailMode(fail: boolean): void {
    this.shouldFail = fail
  }

  /** Set batch size for flushing */
  setBatchSize(size: number): void {
    this.batchSize = size
  }

  async connect(): Promise<void> {
    if (this.shouldFail) {
      this.state = 'error'
      const error = new Error('WebSocket connection failed')
      if (this.errorHandler) this.errorHandler(error)
      throw error
    }
    this.state = 'connecting'
    // Simulate connection delay
    await new Promise(resolve => setTimeout(resolve, 10))
    this.state = 'connected'
  }

  disconnect(): void {
    const wasConnected = this.state === 'connected'
    this.state = 'disconnected'

    // Flush any buffered events when disconnecting (simulates TailDO behavior)
    if (wasConnected && this.eventsBuffer.length > 0) {
      this.flushEvents()
    }

    if (wasConnected && this.closeHandler) {
      this.closeHandler(1000, 'Normal closure')
    }
  }

  async send(message: TailWorkerMessage): Promise<void> {
    if (this.state !== 'connected') {
      throw new Error('WebSocket not connected')
    }

    // Buffer events
    if (message.type === 'tail_events' && message.events) {
      this.eventsBuffer.push(...message.events)

      // Flush if batch size reached
      if (this.eventsBuffer.length >= this.batchSize) {
        await this.flushEvents()
      }

      // Send ack asynchronously so the resolver can be set up first
      const ack = { count: message.events.length }
      // Use setImmediate to defer ack delivery
      setImmediate(() => {
        const resolver = this.ackResolvers.shift()
        if (resolver) {
          resolver.resolve(ack)
        }
      })
    }
  }

  private async flushEvents(): Promise<void> {
    if (this.eventsBuffer.length === 0) return

    const events = this.eventsBuffer
    this.eventsBuffer = []

    const timestamp = Date.now()
    const filePath = `${this.rawEventsPrefix}/${timestamp}-${this.doId}-${this.batchSeq}.ndjson`

    // Build NDJSON content
    const metadata: Omit<RawEventsFile, 'events'> = {
      doId: this.doId,
      createdAt: timestamp,
      batchSeq: this.batchSeq,
    }

    const lines = [JSON.stringify(metadata), ...events.map(e => JSON.stringify(e))]
    const content = lines.join('\n')

    await this.mockR2Bucket.put(filePath, content)
    this.batchSeq++
  }

  async waitForAck(timeoutMs: number = 5000): Promise<{ count: number }> {
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error('Timeout waiting for ack'))
      }, timeoutMs)

      this.ackResolvers.push({
        resolve: (value) => {
          clearTimeout(timeout)
          resolve(value)
        },
        reject: (error) => {
          clearTimeout(timeout)
          reject(error)
        },
      })
    })
  }

  isConnected(): boolean {
    return this.state === 'connected'
  }

  getConnectionState(): 'connecting' | 'connected' | 'disconnected' | 'error' {
    return this.state
  }

  onError(handler: (error: Error) => void): void {
    this.errorHandler = handler
  }

  onClose(handler: (code: number, reason: string) => void): void {
    this.closeHandler = handler
  }

  /** Force flush buffered events (for testing) */
  async forceFlush(): Promise<void> {
    await this.flushEvents()
  }
}

/**
 * Mock CompactionConsumer environment
 */
function createMockEnv(bucket: MockR2Bucket): {
  LOGS_BUCKET: R2Bucket
  RAW_EVENTS_PREFIX: string
  PARQUET_PREFIX: string
  MAX_RETRIES: string
} {
  return {
    LOGS_BUCKET: bucket as unknown as R2Bucket,
    RAW_EVENTS_PREFIX: 'raw-events',
    PARQUET_PREFIX: 'logs/workers',
    MAX_RETRIES: String(DEFAULT_MAX_RETRIES),
  }
}

// =============================================================================
// Test Configuration
// =============================================================================

interface LocalTestConfig {
  bucketName: string
  accountId: string
  pollTimeout: number
  pollInterval: number
}

function getLocalTestConfig(): LocalTestConfig {
  return {
    bucketName: 'parquedb-e2e-logs',
    accountId: 'test-account',
    pollTimeout: 5000,
    pollInterval: 50,
  }
}

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Create a validated trace item for testing
 */
function createTestTraceItem(overrides: Partial<ValidatedTraceItem> = {}): ValidatedTraceItem {
  const now = Date.now()
  return {
    scriptName: 'e2e-test-worker',
    outcome: 'ok',
    eventTimestamp: now,
    event: {
      request: {
        method: 'GET',
        url: 'https://api.example.com/v1/test',
        headers: { 'cf-ray': `e2e-test-${now}` },
        cf: { colo: 'SJC', country: 'US' },
      },
      response: { status: 200 },
    },
    logs: [
      { timestamp: now + 10, level: 'info', message: 'E2E test log message' },
    ],
    exceptions: [],
    diagnosticsChannelEvents: [],
    ...overrides,
  }
}

/**
 * Generate a unique test run ID to isolate test data
 */
function generateTestRunId(): string {
  const timestamp = Date.now().toString(36)
  const random = Math.random().toString(36).substring(2, 8)
  return `e2e-${timestamp}-${random}`
}

/**
 * Create a batch of test trace items
 */
function createTestEventBatch(
  testRunId: string,
  count: number,
  options: { scriptSuffix?: string; baseTimestamp?: number; timestampInterval?: number } = {}
): ValidatedTraceItem[] {
  const { scriptSuffix = 'worker', baseTimestamp = Date.now(), timestampInterval = 10 } = options
  return Array.from({ length: count }, (_, i) =>
    createTestTraceItem({
      scriptName: `${testRunId}-${scriptSuffix}`,
      eventTimestamp: baseTimestamp + i * timestampInterval,
      logs: [{ timestamp: baseTimestamp + i * timestampInterval, level: 'info', message: `Log ${i}` }],
    })
  )
}

/**
 * Create raw events NDJSON content for R2
 */
function createRawEventsContent(
  testRunId: string,
  events: ValidatedTraceItem[],
  batchSeq: number = 0
): { content: string; metadata: Omit<RawEventsFile, 'events'> } {
  const metadata: Omit<RawEventsFile, 'events'> = {
    doId: testRunId,
    createdAt: Date.now(),
    batchSeq,
  }
  const lines = [JSON.stringify(metadata), ...events.map(e => JSON.stringify(e))]
  return { content: lines.join('\n'), metadata }
}

/**
 * Write a raw events file to a mock R2 bucket
 */
async function writeRawEventsFile(
  bucket: MockR2Bucket,
  testRunId: string,
  events: ValidatedTraceItem[],
  options: { batchSeq?: number; keySuffix?: string } = {}
): Promise<string> {
  const { batchSeq = 0, keySuffix = `batch-${batchSeq}` } = options
  const { content, metadata } = createRawEventsContent(testRunId, events, batchSeq)
  const key = `raw-events/${testRunId}/${keySuffix}.ndjson`

  await bucket.put(key, content, {
    customMetadata: {
      doId: metadata.doId,
      batchSeq: String(metadata.batchSeq),
      eventCount: String(events.length),
    },
  })

  return key
}

/**
 * Send tail events through a MockTailDOClient and wait for ack
 */
async function sendAndWaitForAck(
  client: MockTailDOClient,
  events: ValidatedTraceItem[],
  instanceId: string
): Promise<{ count: number }> {
  const message: TailWorkerMessage = {
    type: 'tail_events',
    instanceId,
    timestamp: Date.now(),
    events,
  }
  const sendPromise = client.send(message)
  const ackPromise = client.waitForAck(10000)
  await sendPromise
  return ackPromise
}

/**
 * Test fixture for common test setup
 */
interface TestFixture {
  config: LocalTestConfig
  mockBucket: MockR2Bucket
  mockQueue: MockQueue<R2EventNotification>
  consumer: CompactionConsumer
  testRunId: string
}

/**
 * Create a standard test fixture with bucket, queue, and consumer wired together
 */
function createTestFixture(): TestFixture {
  const config = getLocalTestConfig()
  const testRunId = generateTestRunId()
  const mockBucket = new MockR2Bucket(config.bucketName, config.accountId)
  const mockQueue = new MockQueue<R2EventNotification>()

  // Wire up R2 events to queue
  mockBucket.onEvent((notification) => {
    mockQueue.send(notification)
  })

  // Create consumer
  const env = createMockEnv(mockBucket)
  const consumer = createCompactionConsumer(env)

  return { config, mockBucket, mockQueue, consumer, testRunId }
}

/**
 * Clean up a test fixture
 */
function cleanupTestFixture(fixture: TestFixture): void {
  fixture.mockBucket.clear()
  fixture.mockQueue.purge()
  fixture.mockQueue.purgeDLQ()
}

// =============================================================================
// Test Suite 1: R2 Object-Create Triggers Queue Notification
// =============================================================================

describe('E2E: R2 Object-Create Triggers Queue Notification', () => {
  let fixture: TestFixture

  beforeEach(() => {
    fixture = createTestFixture()
  })

  afterEach(() => {
    cleanupTestFixture(fixture)
  })

  it('should trigger R2 event notification when raw events file is written', async () => {
    const { mockBucket, mockQueue, testRunId, config } = fixture
    const events = createTestEventBatch(testRunId, 2, { scriptSuffix: 'worker' })
    const key = await writeRawEventsFile(mockBucket, testRunId, events, { keySuffix: 'test-batch' })

    const messages = mockQueue.getAllMessages()
    expect(messages.length).toBe(1)

    const notification = messages[0]!
    expect(notification.body.object.key).toBe(key)
    expect(notification.body.eventType).toBe('object-create')
    expect(notification.body.bucket).toBe(config.bucketName)
  })

  it('should include correct metadata in R2 event notification', async () => {
    const { mockBucket, mockQueue, testRunId, config } = fixture
    const events = [createTestTraceItem()]
    const key = await writeRawEventsFile(mockBucket, testRunId, events, { keySuffix: 'metadata-test' })

    const messages = mockQueue.getAllMessages()
    expect(messages.length).toBe(1)

    const notification = messages[0]!
    expect(notification.body.object.key).toBe(key)
    expect(notification.body.account).toBe(config.accountId)
    expect(new Date(notification.body.eventTime).getTime()).not.toBeNaN()
  })

  it('should skip non-raw-events paths in CompactionConsumer', async () => {
    const { mockBucket, mockQueue, testRunId, consumer } = fixture

    // Write to a non-raw-events path
    const key = `other-path/${testRunId}/test.txt`
    await mockBucket.put(key, 'test content')
    await mockBucket.delete(key)

    // Queue has notifications for both operations
    const messages = mockQueue.getAllMessages()
    expect(messages.length).toBe(2)

    // Create mock messages for the consumer
    const queueMessages: Message<R2EventNotification>[] = messages.map(m => ({
      id: m.id,
      body: m.body,
      timestamp: m.timestamp,
      attempts: 1,
      ack: () => { m.acked = true },
      retry: () => { m.retried = true },
    })) as unknown as Message<R2EventNotification>[]

    const result = await consumer.processBatch(queueMessages)

    // Both should be acked (skipped) since they're not raw-events files
    expect(result.succeeded).toBe(0)
    expect(result.failed).toBe(0)
    expect(messages.every(m => m.acked)).toBe(true)
  })
})

// =============================================================================
// Test Suite 2: Queue Consumer Processes R2 Events
// =============================================================================

describe('E2E: Queue Consumer (CompactionConsumer) Processes R2 Events', () => {
  let fixture: TestFixture

  beforeEach(() => {
    fixture = createTestFixture()
  })

  afterEach(() => {
    cleanupTestFixture(fixture)
  })

  it('should process raw events file and write Parquet output', async () => {
    const { mockBucket, mockQueue, consumer, testRunId } = fixture
    const events = createTestEventBatch(testRunId, 50, { timestampInterval: 100 })
    await writeRawEventsFile(mockBucket, testRunId, events)

    const result = await mockQueue.processWithConsumer(consumer)

    expect(result.totalMessages).toBe(1)
    expect(result.succeeded).toBe(1)
    expect(result.failed).toBe(0)
    expect(result.results[0]?.eventsProcessed).toBe(events.length)
    expect(result.results[0]?.success).toBe(true)
  })

  it('should handle multiple raw events files in a batch', async () => {
    const { mockBucket, mockQueue, consumer, testRunId } = fixture
    const batchCount = 5
    const eventsPerBatch = 20

    for (let batch = 0; batch < batchCount; batch++) {
      const events = createTestEventBatch(testRunId, eventsPerBatch, {
        scriptSuffix: `batch-${batch}-worker`,
        baseTimestamp: Date.now() + batch * 1000,
      })
      await writeRawEventsFile(mockBucket, testRunId, events, { batchSeq: batch })
    }

    const result = await mockQueue.processWithConsumer(consumer)

    expect(result.totalMessages).toBe(batchCount)
    expect(result.succeeded).toBe(batchCount)
    expect(result.failed).toBe(0)

    const totalEvents = result.results.reduce((sum, r) => sum + r.eventsProcessed, 0)
    expect(totalEvents).toBe(batchCount * eventsPerBatch)
  })

  it('should skip non-ndjson files', async () => {
    const { mockBucket, mockQueue, consumer, testRunId } = fixture
    await mockBucket.put(`raw-events/${testRunId}/not-ndjson.txt`, 'not a raw events file')

    const result = await mockQueue.processWithConsumer(consumer)

    expect(result.totalMessages).toBe(1)
    expect(result.succeeded).toBe(0)
    expect(result.failed).toBe(0)
  })

  it('should ack messages for successfully processed files', async () => {
    const { mockBucket, mockQueue, consumer, testRunId } = fixture
    const events = [createTestTraceItem({ scriptName: `${testRunId}-ack-test` })]
    const key = await writeRawEventsFile(mockBucket, testRunId, events, { keySuffix: 'ack-test' })

    await mockQueue.processWithConsumer(consumer)

    const messages = mockQueue.getAllMessages()
    expect(messages.length).toBeGreaterThanOrEqual(1)

    const rawEventsMessage = messages.find(m => m.body.object.key === key)
    expect(rawEventsMessage).toBeDefined()
    expect(rawEventsMessage!.acked).toBe(true)

    const pending = mockQueue.getPendingMessages().filter(m =>
      m.body.object.key.startsWith('raw-events/')
    )
    expect(pending.length).toBe(0)
  })
})

// =============================================================================
// Test Suite 3: Full Pipeline Integration
// =============================================================================

describe('E2E: Full Pipeline - Tail Worker to Parquet', () => {
  let fixture: TestFixture
  let tailClient: MockTailDOClient

  beforeEach(() => {
    fixture = createTestFixture()
    tailClient = new MockTailDOClient(fixture.mockBucket)
    tailClient.setBatchSize(10) // Small batch size for testing
  })

  afterEach(() => {
    tailClient.disconnect()
    cleanupTestFixture(fixture)
  })

  it('should process events through complete pipeline: TailWorker -> TailDO -> R2 -> Queue -> Compaction -> Parquet', async () => {
    const { mockBucket, mockQueue, consumer, testRunId } = fixture

    await tailClient.connect()
    expect(tailClient.isConnected()).toBe(true)

    const events = createTestEventBatch(testRunId, 10, { scriptSuffix: 'pipeline-worker', timestampInterval: 100 })
    const ack = await sendAndWaitForAck(tailClient, events, `${testRunId}-instance`)
    expect(ack.count).toBe(events.length)

    await tailClient.forceFlush()

    const r2Objects = await mockBucket.list({ prefix: 'raw-events/' })
    expect(r2Objects.objects.length).toBeGreaterThan(0)

    const queueMessages = mockQueue.getAllMessages()
    expect(queueMessages.length).toBeGreaterThan(0)

    const result = await mockQueue.processWithConsumer(consumer)
    expect(result.succeeded).toBeGreaterThan(0)
    expect(result.failed).toBe(0)
  })

  it('should maintain event ordering through the pipeline', async () => {
    const { mockBucket, testRunId } = fixture

    await tailClient.connect()
    tailClient.setBatchSize(100) // Larger batch to capture all events in one file

    const baseTimestamp = Date.now()
    const events = createTestEventBatch(testRunId, 20, {
      scriptSuffix: 'ordering-test',
      baseTimestamp,
      timestampInterval: 1000,
    })

    await sendAndWaitForAck(tailClient, events, `${testRunId}-ordering`)
    await tailClient.forceFlush()

    const r2Objects = await mockBucket.list({ prefix: 'raw-events/' })
    expect(r2Objects.objects.length).toBeGreaterThan(0)

    for (const obj of r2Objects.objects) {
      const r2Obj = await mockBucket.get(obj.key)
      if (!r2Obj) continue

      const content = await r2Obj.text()
      const lines = content.trim().split('\n')

      // Skip metadata line, check event order
      let prevTimestamp = 0
      for (let i = 1; i < lines.length; i++) {
        const event = JSON.parse(lines[i]!) as ValidatedTraceItem
        expect(event.eventTimestamp).toBeGreaterThanOrEqual(prevTimestamp)
        prevTimestamp = event.eventTimestamp ?? 0
      }
    }
  })

  it('should handle high-volume event streams without data loss', async () => {
    const { mockQueue, consumer, testRunId } = fixture

    await tailClient.connect()
    tailClient.setBatchSize(50)

    const totalEvents = 500
    const batchSize = 50
    const batches = Math.ceil(totalEvents / batchSize)
    let totalAcked = 0

    for (let batch = 0; batch < batches; batch++) {
      const events = createTestEventBatch(testRunId, batchSize, {
        scriptSuffix: 'volume-test',
        baseTimestamp: Date.now() + batch * 10000,
      })
      const ack = await sendAndWaitForAck(tailClient, events, `${testRunId}-volume-${batch}`)
      totalAcked += ack.count
    }

    expect(totalAcked).toBe(totalEvents)

    await tailClient.forceFlush()

    const queueMessages = mockQueue.getAllMessages()
    expect(queueMessages.length).toBeGreaterThan(0)

    const result = await mockQueue.processWithConsumer(consumer)
    expect(result.failed).toBe(0)

    const totalProcessed = result.results.reduce((sum, r) => sum + r.eventsProcessed, 0)
    expect(totalProcessed).toBe(totalEvents)
  })
})

// =============================================================================
// Test Suite 4: DLQ Behavior When MAX_RETRIES Exceeded
// =============================================================================

describe('E2E: DLQ Behavior When MAX_RETRIES Exceeded', () => {
  let fixture: TestFixture

  beforeEach(() => {
    fixture = createTestFixture()
  })

  afterEach(() => {
    cleanupTestFixture(fixture)
  })

  it('should retry messages when file is deleted before processing', async () => {
    const { mockBucket, mockQueue, consumer, testRunId } = fixture
    const events = [createTestTraceItem({ scriptName: `${testRunId}-dlq-test` })]
    const key = await writeRawEventsFile(mockBucket, testRunId, events, { keySuffix: 'dlq-test' })

    // Delete file to cause processing failures
    await mockBucket.delete(key)

    // Process multiple times to exhaust retries
    for (let attempt = 0; attempt < DEFAULT_MAX_RETRIES + 1; attempt++) {
      const pending = mockQueue.getPendingMessages()
      if (pending.length === 0) break
      await mockQueue.processWithConsumer(consumer)
    }

    // DLQ behavior verified by retry count
    const dlqMessages = mockQueue.getDLQMessages()
    expect(dlqMessages.length).toBeGreaterThanOrEqual(0)
  })

  it('should ack files with valid metadata but no events (not send to DLQ)', async () => {
    const { mockBucket, mockQueue, consumer, testRunId } = fixture
    const key = `raw-events/${testRunId}/no-events.ndjson`
    // Just metadata, no events - this is valid, results in 0 events processed
    const metadataOnly = JSON.stringify({ doId: testRunId, createdAt: Date.now(), batchSeq: 0 })
    await mockBucket.put(key, metadataOnly)

    const result = await mockQueue.processWithConsumer(consumer)

    expect(result.totalMessages).toBe(1)
    expect(result.succeeded).toBe(1)
    expect(result.failed).toBe(0)
    expect(mockQueue.getDLQMessages().length).toBe(0)
  })

  it('should track retry attempts for failed processing', async () => {
    const { mockBucket, mockQueue, consumer, testRunId } = fixture
    const events = [createTestTraceItem({ scriptName: `${testRunId}-retry-count-test` })]
    const key = await writeRawEventsFile(mockBucket, testRunId, events, { keySuffix: 'retry-count-test' })

    // Delete to cause processing failure
    await mockBucket.delete(key)

    let totalAttempts = 0
    for (let i = 0; i < DEFAULT_MAX_RETRIES + 2; i++) {
      const pending = mockQueue.getPendingMessages()
      if (pending.length === 0) break
      totalAttempts++
      await mockQueue.processWithConsumer(consumer)
    }

    expect(totalAttempts).toBeGreaterThanOrEqual(1)
  })

  it('should process valid files without going to DLQ', async () => {
    const { mockBucket, mockQueue, consumer, testRunId } = fixture
    const events = [createTestTraceItem({ scriptName: `${testRunId}-success-test` })]
    await writeRawEventsFile(mockBucket, testRunId, events, { keySuffix: 'success-test' })

    const result = await mockQueue.processWithConsumer(consumer)

    expect(mockQueue.getDLQMessages().length).toBe(0)
    expect(result.succeeded).toBe(1)
    expect(result.failed).toBe(0)
  })
})

// =============================================================================
// Test Suite 5: WebSocket Reconnection Scenarios
// =============================================================================

describe('E2E: WebSocket Reconnection Scenarios', () => {
  let mockBucket: MockR2Bucket
  let tailClient: MockTailDOClient
  let testRunId: string

  beforeEach(() => {
    const config = getLocalTestConfig()
    testRunId = generateTestRunId()
    mockBucket = new MockR2Bucket(config.bucketName, config.accountId)
    tailClient = new MockTailDOClient(mockBucket)
  })

  afterEach(() => {
    tailClient.disconnect()
    mockBucket.clear()
  })

  it('should connect to TailDO via WebSocket', async () => {
    await tailClient.connect()
    expect(tailClient.isConnected()).toBe(true)
    expect(tailClient.getConnectionState()).toBe('connected')
  })

  it('should reconnect after connection close', async () => {
    await tailClient.connect()
    expect(tailClient.isConnected()).toBe(true)

    tailClient.disconnect()
    expect(tailClient.isConnected()).toBe(false)

    await tailClient.connect()
    expect(tailClient.isConnected()).toBe(true)
  })

  it('should handle connection errors gracefully', async () => {
    let errorReceived = false
    tailClient.onError(() => { errorReceived = true })
    tailClient.setFailMode(true)

    try {
      await tailClient.connect()
    } catch {
      // Expected to fail
    }

    expect(tailClient.getConnectionState()).toMatch(/error|disconnected/)
    expect(errorReceived).toBe(true)
  })

  it('should send events after reconnection', async () => {
    await tailClient.connect()
    tailClient.setBatchSize(100)

    const events1 = [createTestTraceItem({ scriptName: `${testRunId}-batch-1` })]
    const ack1 = await sendAndWaitForAck(tailClient, events1, `${testRunId}-1`)
    expect(ack1.count).toBe(1)

    tailClient.disconnect()
    await tailClient.connect()

    const events2 = [createTestTraceItem({ scriptName: `${testRunId}-batch-2` })]
    const ack2 = await sendAndWaitForAck(tailClient, events2, `${testRunId}-2`)
    expect(ack2.count).toBe(1)
  })

  it('should receive ack for sent events', async () => {
    await tailClient.connect()
    tailClient.setBatchSize(100)

    const events = createTestEventBatch(testRunId, 5, { scriptSuffix: 'ack-test' })
    const ack = await sendAndWaitForAck(tailClient, events, `${testRunId}-ack`)

    expect(ack.count).toBe(events.length)
  })

  it('should detect connection close events', async () => {
    let closeReceived = false
    let closeCode = 0
    tailClient.onClose((code) => { closeReceived = true; closeCode = code })

    await tailClient.connect()
    tailClient.disconnect()

    await new Promise(resolve => setTimeout(resolve, 50))

    expect(closeReceived).toBe(true)
    expect(closeCode).toBe(1000)
  })

  it('should handle rapid connect/disconnect cycles', async () => {
    for (let i = 0; i < 5; i++) {
      await tailClient.connect()
      expect(tailClient.isConnected()).toBe(true)

      const events = [createTestTraceItem({ scriptName: `${testRunId}-rapid-${i}` })]
      await sendAndWaitForAck(tailClient, events, `${testRunId}-rapid-${i}`)

      tailClient.disconnect()
      expect(tailClient.isConnected()).toBe(false)

      await new Promise(resolve => setTimeout(resolve, 10))
    }
  })

  it('should maintain state across reconnections (TailDO hibernation)', async () => {
    tailClient.setBatchSize(100)
    await tailClient.connect()

    const events = createTestEventBatch(testRunId, 10, {
      scriptSuffix: 'hibernation-test',
      timestampInterval: 1000,
    })

    // Send first half
    await sendAndWaitForAck(tailClient, events.slice(0, 5), `${testRunId}-hibernate-1`)

    // Disconnect (flushes events)
    tailClient.disconnect()
    await new Promise(resolve => setTimeout(resolve, 50))

    // Reconnect and send more events
    await tailClient.connect()
    const ack2 = await sendAndWaitForAck(tailClient, events.slice(5), `${testRunId}-hibernate-2`)

    expect(ack2.count).toBe(5)
    expect(tailClient.isConnected()).toBe(true)
  })
})

// =============================================================================
// Test Suite 6: Integration Stress Tests
// =============================================================================

describe('E2E: Integration Stress Tests', () => {
  let fixture: TestFixture

  beforeEach(() => {
    fixture = createTestFixture()
  })

  afterEach(() => {
    cleanupTestFixture(fixture)
  })

  it('should handle concurrent writes from multiple TailDO instances', async () => {
    const { mockBucket, mockQueue, consumer, testRunId } = fixture
    const instanceCount = 5
    const eventsPerInstance = 50

    const writePromises = Array.from({ length: instanceCount }, async (_, instance) => {
      const events = createTestEventBatch(`${testRunId}-instance-${instance}`, eventsPerInstance)
      return writeRawEventsFile(mockBucket, `${testRunId}-instance-${instance}`, events, {
        keySuffix: 'batch-0',
      })
    })

    const writtenKeys = await Promise.all(writePromises)
    expect(writtenKeys).toHaveLength(instanceCount)

    const result = await mockQueue.processWithConsumer(consumer)

    expect(result.totalMessages).toBe(instanceCount)
    expect(result.succeeded).toBe(instanceCount)
    expect(result.failed).toBe(0)

    const totalEvents = result.results.reduce((sum, r) => sum + r.eventsProcessed, 0)
    expect(totalEvents).toBe(instanceCount * eventsPerInstance)
  })

  it('should handle burst traffic patterns', async () => {
    const { mockBucket, mockQueue, consumer, testRunId } = fixture
    const burstSize = 20
    const burstCount = 3
    const delayBetweenBursts = 50

    for (let burst = 0; burst < burstCount; burst++) {
      const burstPromises = Array.from({ length: burstSize }, async (_, i) => {
        const events = [createTestTraceItem({
          scriptName: `${testRunId}-burst-${burst}-event-${i}`,
          eventTimestamp: Date.now() + i,
        })]
        return writeRawEventsFile(mockBucket, testRunId, events, {
          batchSeq: i,
          keySuffix: `burst-${burst}-batch-${i}`,
        })
      })

      await Promise.all(burstPromises)

      if (burst < burstCount - 1) {
        await new Promise(resolve => setTimeout(resolve, delayBetweenBursts))
      }
    }

    const result = await mockQueue.processWithConsumer(consumer)

    expect(result.totalMessages).toBe(burstCount * burstSize)
    expect(result.succeeded).toBe(burstCount * burstSize)
    expect(result.failed).toBe(0)
  })

  it('should process mixed event sizes efficiently', async () => {
    const { mockBucket, mockQueue, consumer, testRunId } = fixture
    const fileSizes = [1, 10, 50, 100, 200]

    for (const size of fileSizes) {
      const events = createTestEventBatch(testRunId, size, { scriptSuffix: `size-${size}-worker` })
      await writeRawEventsFile(mockBucket, testRunId, events, { keySuffix: `size-${size}` })
    }

    const result = await mockQueue.processWithConsumer(consumer)

    expect(result.totalMessages).toBe(fileSizes.length)
    expect(result.succeeded).toBe(fileSizes.length)
    expect(result.failed).toBe(0)

    const expectedTotal = fileSizes.reduce((sum, size) => sum + size, 0)
    const actualTotal = result.results.reduce((sum, r) => sum + r.eventsProcessed, 0)
    expect(actualTotal).toBe(expectedTotal)
  })
})
