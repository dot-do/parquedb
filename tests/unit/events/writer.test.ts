/**
 * EventWriter Test Suite
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { EventWriter, createEventWriter, createTimedEventWriter } from '../../../src/events/writer'
import type { Event } from '../../../src/types'
import type { EventBatch } from '../../../src/events/types'

// =============================================================================
// Helper Functions
// =============================================================================

function createTestEvent(overrides: Partial<Event> = {}): Event {
  return {
    id: `evt_${Date.now()}_${Math.random().toString(36).slice(2)}`,
    ts: Date.now(),
    op: 'CREATE',
    target: 'posts:test123',
    after: { title: 'Test', content: 'Content' },
    ...overrides,
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

// =============================================================================
// Tests
// =============================================================================

describe('EventWriter', () => {
  let writer: EventWriter

  beforeEach(() => {
    writer = new EventWriter()
  })

  afterEach(() => {
    writer.stopTimer()
  })

  describe('basic operations', () => {
    it('buffers events without flushing by default', async () => {
      const event = createTestEvent()
      await writer.write(event)

      expect(writer.hasEvents()).toBe(true)
      expect(writer.getStats().bufferedEvents).toBe(1)
    })

    it('returns buffer contents', async () => {
      const event1 = createTestEvent()
      const event2 = createTestEvent()

      await writer.write(event1)
      await writer.write(event2)

      const buffer = writer.getBuffer()
      expect(buffer).toHaveLength(2)
      expect(buffer[0]).toEqual(event1)
      expect(buffer[1]).toEqual(event2)
    })

    it('writes multiple events at once', async () => {
      const events = [createTestEvent(), createTestEvent(), createTestEvent()]
      await writer.writeMany(events)

      expect(writer.getStats().bufferedEvents).toBe(3)
    })

    it('tracks min/max timestamps', async () => {
      const event1 = createTestEvent({ ts: 1000 })
      const event2 = createTestEvent({ ts: 3000 })
      const event3 = createTestEvent({ ts: 2000 })

      await writer.write(event1)
      await writer.write(event2)
      await writer.write(event3)

      // We can't directly access min/max, but we can check via flush
      let capturedBatch: EventBatch | null = null
      writer.onFlush(async (batch) => {
        capturedBatch = batch
      })

      await writer.flush()

      expect(capturedBatch).not.toBeNull()
      expect(capturedBatch!.minTs).toBe(1000)
      expect(capturedBatch!.maxTs).toBe(3000)
    })
  })

  describe('flush handlers', () => {
    it('calls flush handler when flushing', async () => {
      const handler = vi.fn()
      writer.onFlush(handler)

      await writer.write(createTestEvent())
      await writer.flush()

      expect(handler).toHaveBeenCalledTimes(1)
      expect(handler).toHaveBeenCalledWith(
        expect.objectContaining({
          events: expect.any(Array),
          count: 1,
          minTs: expect.any(Number),
          maxTs: expect.any(Number),
        })
      )
    })

    it('calls multiple flush handlers in parallel', async () => {
      const handler1 = vi.fn()
      const handler2 = vi.fn()

      writer.onFlush(handler1)
      writer.onFlush(handler2)

      await writer.write(createTestEvent())
      await writer.flush()

      expect(handler1).toHaveBeenCalledTimes(1)
      expect(handler2).toHaveBeenCalledTimes(1)
    })

    it('removes flush handlers', async () => {
      const handler = vi.fn()
      writer.onFlush(handler)
      writer.offFlush(handler)

      await writer.write(createTestEvent())
      await writer.flush()

      expect(handler).not.toHaveBeenCalled()
    })

    it('does not call handler on empty flush', async () => {
      const handler = vi.fn()
      writer.onFlush(handler)

      await writer.flush()

      expect(handler).not.toHaveBeenCalled()
    })
  })

  describe('threshold-based flushing', () => {
    it('flushes when maxBufferSize is reached', async () => {
      const smallWriter = new EventWriter({ maxBufferSize: 3 })
      const handler = vi.fn()
      smallWriter.onFlush(handler)

      await smallWriter.write(createTestEvent())
      await smallWriter.write(createTestEvent())
      expect(handler).not.toHaveBeenCalled()

      await smallWriter.write(createTestEvent())
      expect(handler).toHaveBeenCalledTimes(1)
      expect(smallWriter.getStats().bufferedEvents).toBe(0)
    })

    it('flushes when maxBufferBytes is reached', async () => {
      const smallWriter = new EventWriter({ maxBufferBytes: 100 })
      const handler = vi.fn()
      smallWriter.onFlush(handler)

      // Create events with large content to exceed byte threshold
      const largeEvent = createTestEvent({
        after: { content: 'x'.repeat(200) },
      })

      await smallWriter.write(largeEvent)
      expect(handler).toHaveBeenCalledTimes(1)
    })
  })

  describe('statistics', () => {
    it('tracks total events written', async () => {
      writer.onFlush(async () => {})

      await writer.write(createTestEvent())
      await writer.write(createTestEvent())
      await writer.flush()

      await writer.write(createTestEvent())
      await writer.flush()

      const stats = writer.getStats()
      expect(stats.totalEventsWritten).toBe(3)
    })

    it('tracks total flushes', async () => {
      writer.onFlush(async () => {})

      await writer.write(createTestEvent())
      await writer.flush()
      await writer.write(createTestEvent())
      await writer.flush()

      const stats = writer.getStats()
      expect(stats.totalFlushes).toBe(2)
    })

    it('tracks last flush time', async () => {
      writer.onFlush(async () => {})

      const before = Date.now()
      await writer.write(createTestEvent())
      await writer.flush()
      const after = Date.now()

      const stats = writer.getStats()
      expect(stats.lastFlushAt).toBeGreaterThanOrEqual(before)
      expect(stats.lastFlushAt).toBeLessThanOrEqual(after)
    })

    it('resets buffer after flush', async () => {
      writer.onFlush(async () => {})

      await writer.write(createTestEvent())
      await writer.write(createTestEvent())
      expect(writer.getStats().bufferedEvents).toBe(2)

      await writer.flush()
      expect(writer.getStats().bufferedEvents).toBe(0)
      expect(writer.hasEvents()).toBe(false)
    })
  })

  describe('timed flushing', () => {
    it('flushes periodically when timer is started', async () => {
      const fastWriter = new EventWriter({ flushIntervalMs: 50 })
      const handler = vi.fn()
      fastWriter.onFlush(handler)
      fastWriter.startTimer()

      await fastWriter.write(createTestEvent())
      expect(handler).not.toHaveBeenCalled()

      await sleep(100)
      expect(handler).toHaveBeenCalledTimes(1)

      fastWriter.stopTimer()
    })

    it('stops timer on close', async () => {
      const fastWriter = new EventWriter({ flushIntervalMs: 50 })
      const handler = vi.fn()
      fastWriter.onFlush(handler)
      fastWriter.startTimer()

      await fastWriter.write(createTestEvent())
      await fastWriter.close()

      expect(handler).toHaveBeenCalledTimes(1) // final flush on close

      // Timer should be stopped, no more flushes
      await fastWriter.write(createTestEvent())
      await sleep(100)
      expect(handler).toHaveBeenCalledTimes(1) // still only 1
    })
  })

  describe('factory functions', () => {
    it('createEventWriter creates a writer', () => {
      const w = createEventWriter({ maxBufferSize: 500 })
      expect(w).toBeInstanceOf(EventWriter)
    })

    it('createTimedEventWriter starts timer automatically', async () => {
      const fastWriter = createTimedEventWriter({ flushIntervalMs: 50 })
      const handler = vi.fn()
      fastWriter.onFlush(handler)

      await fastWriter.write(createTestEvent())
      await sleep(100)

      expect(handler).toHaveBeenCalled()
      fastWriter.stopTimer()
    })
  })

  describe('concurrent flush handling', () => {
    it('waits for in-progress flush before starting new one', async () => {
      const slowHandler = vi.fn(async () => {
        await sleep(50)
      })
      writer.onFlush(slowHandler)

      await writer.write(createTestEvent())

      // Start flush without awaiting
      const flush1 = writer.flush()
      const flush2 = writer.flush()

      await Promise.all([flush1, flush2])

      // Should only flush once since buffer is empty after first flush
      expect(slowHandler).toHaveBeenCalledTimes(1)
    })
  })
})
