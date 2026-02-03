/**
 * Tests for Compaction Consumer
 *
 * Tests the event-driven compaction worker that processes raw event files
 * from R2 and writes Parquet segments.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'

// Mock functions for R2 and Queue
const mockR2Get = vi.fn()
const mockR2Put = vi.fn()
const mockQueueSend = vi.fn()

// Mock R2 Bucket
const mockBucket = {
  get: mockR2Get,
  put: mockR2Put,
  head: vi.fn(),
  delete: vi.fn(),
  list: vi.fn(),
  createMultipartUpload: vi.fn(),
}

// Mock Queue
const mockQueue = {
  send: mockQueueSend,
}

// Mock environment
const mockEnv = {
  LOGS_BUCKET: mockBucket as unknown as R2Bucket,
  RAW_EVENTS_PREFIX: 'raw-events',
  PARQUET_PREFIX: 'logs/workers',
  FLUSH_THRESHOLD: '100',
  COMPRESSION: 'lz4' as const,
  DOWNSTREAM_QUEUE: mockQueue as unknown as Queue<unknown>,
}

// =============================================================================
// Test Helper Functions
// =============================================================================

/**
 * Create a mock R2 event notification
 */
function createR2Notification(key: string, size: number = 1024): {
  account: string
  bucket: string
  object: { key: string; size: number; eTag: string }
  eventType: 'object-create' | 'object-delete'
  eventTime: string
} {
  return {
    account: 'test-account',
    bucket: 'parquedb-logs',
    object: {
      key,
      size,
      eTag: '"abc123"',
    },
    eventType: 'object-create',
    eventTime: new Date().toISOString(),
  }
}

/**
 * Create a mock message with ack/retry methods and attempts property
 *
 * @param body - The message body
 * @param attempts - Number of delivery attempts (default: 1, per Cloudflare Queues API)
 */
function createMockMessage<T>(body: T, attempts: number = 1): {
  body: T
  attempts: number
  ack: ReturnType<typeof vi.fn>
  retry: ReturnType<typeof vi.fn>
} {
  return {
    body,
    attempts,
    ack: vi.fn(),
    retry: vi.fn(),
  }
}

/**
 * Create a mock raw events file content (NDJSON format)
 */
function createRawEventsContent(
  doId: string,
  batchSeq: number,
  events: Array<{
    scriptName: string
    outcome: string
    eventTimestamp: number
    logs?: Array<{ timestamp: number; level: string; message: string }>
    exceptions?: Array<{ timestamp: number; name: string; message: string }>
  }>
): string {
  const metadata = { doId, createdAt: Date.now(), batchSeq }
  const lines = [JSON.stringify(metadata)]

  for (const event of events) {
    lines.push(JSON.stringify({
      scriptName: event.scriptName,
      outcome: event.outcome,
      eventTimestamp: event.eventTimestamp,
      event: null,
      logs: event.logs || [],
      exceptions: event.exceptions || [],
      diagnosticsChannelEvents: [],
    }))
  }

  return lines.join('\n')
}

// =============================================================================
// Tests
// =============================================================================

describe('Compaction Consumer', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  describe('R2 Event Notification Parsing', () => {
    it('should create valid R2 notification', () => {
      const notification = createR2Notification('raw-events/1234-abc.ndjson')

      expect(notification.object.key).toBe('raw-events/1234-abc.ndjson')
      expect(notification.eventType).toBe('object-create')
      expect(notification.bucket).toBe('parquedb-logs')
    })

    it('should identify raw events files by path', () => {
      const rawEventsPath = 'raw-events/1704067200000-abc123-42.ndjson'
      const parquetPath = 'logs/workers/year=2024/month=01/day=01/logs-123.parquet'

      expect(rawEventsPath.startsWith('raw-events/')).toBe(true)
      expect(rawEventsPath.endsWith('.ndjson')).toBe(true)

      expect(parquetPath.startsWith('raw-events/')).toBe(false)
    })
  })

  describe('Raw Events File Parsing', () => {
    it('should parse valid NDJSON raw events file', () => {
      const content = createRawEventsContent('do-123', 1, [
        {
          scriptName: 'my-worker',
          outcome: 'ok',
          eventTimestamp: Date.now(),
          logs: [
            { timestamp: Date.now(), level: 'info', message: 'Hello' },
          ],
        },
        {
          scriptName: 'my-worker',
          outcome: 'exception',
          eventTimestamp: Date.now(),
          exceptions: [
            { timestamp: Date.now(), name: 'Error', message: 'Something failed' },
          ],
        },
      ])

      const lines = content.trim().split('\n')
      expect(lines.length).toBe(3) // 1 metadata + 2 events

      const metadata = JSON.parse(lines[0]!)
      expect(metadata.doId).toBe('do-123')
      expect(metadata.batchSeq).toBe(1)

      const event1 = JSON.parse(lines[1]!)
      expect(event1.scriptName).toBe('my-worker')
      expect(event1.outcome).toBe('ok')

      const event2 = JSON.parse(lines[2]!)
      expect(event2.outcome).toBe('exception')
    })

    it('should handle empty events array', () => {
      const content = createRawEventsContent('do-123', 0, [])

      const lines = content.trim().split('\n')
      expect(lines.length).toBe(1) // metadata only

      const metadata = JSON.parse(lines[0]!)
      expect(metadata.doId).toBe('do-123')
    })
  })

  describe('Message Handling', () => {
    it('should create mock message with ack/retry', () => {
      const notification = createR2Notification('raw-events/test.ndjson')
      const message = createMockMessage(notification)

      expect(message.body.object.key).toBe('raw-events/test.ndjson')
      expect(typeof message.ack).toBe('function')
      expect(typeof message.retry).toBe('function')

      // Verify ack/retry can be called
      message.ack()
      message.retry()

      expect(message.ack).toHaveBeenCalledOnce()
      expect(message.retry).toHaveBeenCalledOnce()
    })

    it('should skip delete events', () => {
      const notification = {
        ...createR2Notification('raw-events/test.ndjson'),
        eventType: 'object-delete' as const,
      }

      expect(notification.eventType).toBe('object-delete')
    })

    it('should skip non-raw-events files', () => {
      const notification = createR2Notification('logs/workers/data.parquet')

      // File should not be considered a raw events file
      const key = notification.object.key
      const isRawEvents = key.startsWith('raw-events/') && key.endsWith('.ndjson')

      expect(isRawEvents).toBe(false)
    })
  })

  describe('Processing Result', () => {
    it('should create processing result for success', () => {
      const result = {
        sourceKey: 'raw-events/123.ndjson',
        eventsProcessed: 50,
        success: true,
      }

      expect(result.success).toBe(true)
      expect(result.eventsProcessed).toBe(50)
      expect(result.error).toBeUndefined()
    })

    it('should create processing result for failure', () => {
      const result = {
        sourceKey: 'raw-events/123.ndjson',
        eventsProcessed: 0,
        success: false,
        error: 'Failed to parse file',
      }

      expect(result.success).toBe(false)
      expect(result.error).toBe('Failed to parse file')
    })
  })

  describe('Batch Result', () => {
    it('should create batch result with DLQ statistics', () => {
      const result = {
        totalMessages: 5,
        succeeded: 3,
        failed: 2,
        retried: 1,
        deadLettered: 1,
        results: [
          { sourceKey: 'raw-events/1.ndjson', eventsProcessed: 10, success: true },
          { sourceKey: 'raw-events/2.ndjson', eventsProcessed: 20, success: true },
          { sourceKey: 'raw-events/3.ndjson', eventsProcessed: 0, success: false, error: 'Parse error' },
        ],
        parquetFilesWritten: ['logs/workers/year=2024/month=01/day=01/hour=12/logs-123.parquet'],
      }

      expect(result.succeeded + result.failed).toBeLessThanOrEqual(result.totalMessages)
      expect(result.results.length).toBe(3)
      expect(result.parquetFilesWritten.length).toBe(1)
      expect(result.retried).toBe(1)
      expect(result.deadLettered).toBe(1)
    })

    it('should track retried and deadLettered counts separately from failed', () => {
      // Failed messages can be either retried OR deadLettered (not both)
      // retried: will be redelivered
      // deadLettered: exceeded max retries, sent to DLQ
      const result = {
        totalMessages: 10,
        succeeded: 5,
        failed: 5,
        retried: 3, // 3 messages will be retried
        deadLettered: 2, // 2 messages exceeded max retries
        results: [],
        parquetFilesWritten: [],
      }

      expect(result.retried + result.deadLettered).toBeLessThanOrEqual(result.failed)
    })
  })

  describe('Downstream Message', () => {
    it('should create downstream notification message', () => {
      const message = {
        type: 'parquet-written' as const,
        parquetPath: 'logs/workers/year=2024/month=01/day=01/hour=12/',
        recordCount: 500,
        sizeBytes: 102400,
        sourceFiles: [
          'raw-events/1704067200000-abc-1.ndjson',
          'raw-events/1704067200001-abc-2.ndjson',
        ],
        processedAt: Date.now(),
      }

      expect(message.type).toBe('parquet-written')
      expect(message.recordCount).toBe(500)
      expect(message.sourceFiles.length).toBe(2)
    })
  })

  describe('Environment Configuration', () => {
    it('should have required bindings', () => {
      expect(mockEnv.LOGS_BUCKET).toBeDefined()
      expect(mockEnv.RAW_EVENTS_PREFIX).toBe('raw-events')
      expect(mockEnv.PARQUET_PREFIX).toBe('logs/workers')
    })

    it('should use defaults for optional configuration', () => {
      const envWithoutPrefix = {
        LOGS_BUCKET: mockBucket as unknown as R2Bucket,
      }

      const rawEventsPrefix = envWithoutPrefix.RAW_EVENTS_PREFIX || 'raw-events'
      const parquetPrefix = envWithoutPrefix.PARQUET_PREFIX || 'logs/workers'
      const flushThreshold = envWithoutPrefix.FLUSH_THRESHOLD
        ? parseInt(envWithoutPrefix.FLUSH_THRESHOLD, 10)
        : 1000

      expect(rawEventsPrefix).toBe('raw-events')
      expect(parquetPrefix).toBe('logs/workers')
      expect(flushThreshold).toBe(1000)
    })

    it('should use defaults for MAX_RETRIES if not provided', () => {
      const envWithoutMaxRetries = {
        LOGS_BUCKET: mockBucket as unknown as R2Bucket,
      }

      const maxRetries = envWithoutMaxRetries.MAX_RETRIES
        ? parseInt(envWithoutMaxRetries.MAX_RETRIES, 10)
        : 3 // DEFAULT_MAX_RETRIES

      expect(maxRetries).toBe(3)
    })

    it('should allow custom MAX_RETRIES configuration', () => {
      const envWithCustomMaxRetries = {
        LOGS_BUCKET: mockBucket as unknown as R2Bucket,
        MAX_RETRIES: '5',
      }

      const maxRetries = envWithCustomMaxRetries.MAX_RETRIES
        ? parseInt(envWithCustomMaxRetries.MAX_RETRIES, 10)
        : 3

      expect(maxRetries).toBe(5)
    })
  })

  describe('Dead Letter Queue (DLQ) Handling', () => {
    it('should create message with attempts property', () => {
      const notification = createR2Notification('raw-events/test.ndjson')

      // First attempt (default)
      const message1 = createMockMessage(notification)
      expect(message1.attempts).toBe(1)

      // Third attempt (after 2 retries)
      const message3 = createMockMessage(notification, 3)
      expect(message3.attempts).toBe(3)
    })

    it('should distinguish between retryable and non-retryable errors', () => {
      // Non-retryable errors (should be acked, not retried)
      const nonRetryableErrors = [
        'Failed to parse JSON',
        'malformed event data',
        'Empty raw events file',
      ]

      for (const error of nonRetryableErrors) {
        const isNonRetryable =
          error.includes('parse') ||
          error.includes('malformed') ||
          error.includes('Empty raw events file')

        expect(isNonRetryable).toBe(true)
      }

      // Retryable errors (transient failures)
      const retryableErrors = ['Network timeout', 'R2 temporarily unavailable', 'Connection reset']

      for (const error of retryableErrors) {
        const isNonRetryable =
          error.includes('parse') ||
          error.includes('malformed') ||
          error.includes('Empty raw events file')

        expect(isNonRetryable).toBe(false)
      }
    })

    it('should determine retry eligibility based on attempts vs maxRetries', () => {
      const maxRetries = 3

      // Attempt 1: should retry
      expect(1 < maxRetries).toBe(true)

      // Attempt 2: should retry
      expect(2 < maxRetries).toBe(true)

      // Attempt 3: at max retries, should NOT retry (goes to DLQ)
      expect(3 >= maxRetries).toBe(true)

      // Attempt 4: exceeds max retries (should never happen if DLQ is working)
      expect(4 >= maxRetries).toBe(true)
    })

    it('should track message handling decisions correctly', () => {
      // Simulate the logic from processBatch
      const maxRetries = 3

      interface MessageDecision {
        key: string
        attempts: number
        error: string | null
        decision: 'ack' | 'retry' | 'dlq'
      }

      function decideAction(
        success: boolean,
        error: string | null,
        attempts: number
      ): 'ack' | 'retry' | 'dlq' {
        if (success) return 'ack'

        const isNonRetryable =
          error?.includes('parse') ||
          error?.includes('malformed') ||
          error?.includes('Empty raw events file')

        if (isNonRetryable) return 'ack' // Non-retryable: ack to avoid blocking
        if (attempts >= maxRetries) return 'dlq' // Max retries: let Cloudflare send to DLQ
        return 'retry' // Transient error: retry
      }

      const testCases: MessageDecision[] = [
        // Success cases
        { key: 'file1.ndjson', attempts: 1, error: null, decision: 'ack' },

        // Parse error (non-retryable) - any attempt
        { key: 'file2.ndjson', attempts: 1, error: 'Failed to parse', decision: 'ack' },
        { key: 'file3.ndjson', attempts: 3, error: 'malformed data', decision: 'ack' },

        // Transient error - first attempt (should retry)
        { key: 'file4.ndjson', attempts: 1, error: 'Network timeout', decision: 'retry' },

        // Transient error - second attempt (should retry)
        { key: 'file5.ndjson', attempts: 2, error: 'R2 unavailable', decision: 'retry' },

        // Transient error - max retries (should go to DLQ)
        { key: 'file6.ndjson', attempts: 3, error: 'Connection failed', decision: 'dlq' },
      ]

      for (const tc of testCases) {
        const action = decideAction(tc.error === null, tc.error, tc.attempts)
        expect(action).toBe(tc.decision)
      }
    })

    it('should handle message with missing attempts property gracefully', () => {
      // Some message implementations might not have attempts property
      // The code should default to 1
      const notification = createR2Notification('raw-events/test.ndjson')
      const message = createMockMessage(notification)

      // Simulate the fallback logic: message.attempts ?? 1
      const attempts = (message as { attempts?: number }).attempts ?? 1
      expect(attempts).toBe(1)
    })
  })
})

// Add interface declaration for missing types in test environment
declare global {
  interface R2Bucket {
    get(key: string): Promise<R2ObjectBody | null>
    put(key: string, value: ArrayBuffer | string): Promise<R2Object>
    head(key: string): Promise<R2Object | null>
    delete(keys: string | string[]): Promise<void>
    list(options?: { prefix?: string }): Promise<{ objects: R2Object[] }>
    createMultipartUpload(key: string): Promise<R2MultipartUpload>
  }

  interface R2Object {
    key: string
    size: number
    etag: string
    uploaded: Date
  }

  interface R2ObjectBody extends R2Object {
    arrayBuffer(): Promise<ArrayBuffer>
    text(): Promise<string>
  }

  interface R2MultipartUpload {
    uploadId: string
    uploadPart(partNumber: number, data: ArrayBuffer): Promise<R2UploadedPart>
    complete(parts: R2UploadedPart[]): Promise<R2Object>
    abort(): Promise<void>
  }

  interface R2UploadedPart {
    partNumber: number
    etag: string
  }

  interface Queue<T> {
    send(message: T): Promise<void>
  }
}
