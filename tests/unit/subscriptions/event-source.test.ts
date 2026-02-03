/**
 * Event Source Test Suite
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import {
  InMemoryEventSource,
  EventWriterSource,
  PollingEventSource,
  createInMemoryEventSource,
  createEventWriterSource,
  createPollingEventSource,
} from '@/subscriptions'
import type { Event } from '@/types'

// =============================================================================
// Helper Functions
// =============================================================================

function createTestEvent(overrides: Partial<Event> = {}): Event {
  return {
    id: `evt_${Date.now()}_${Math.random().toString(36).slice(2)}`,
    ts: Date.now(),
    op: 'CREATE',
    target: 'posts:post1',
    after: { title: 'Test Post' },
    ...overrides,
  }
}

// =============================================================================
// InMemoryEventSource Tests
// =============================================================================

describe('InMemoryEventSource', () => {
  let source: InMemoryEventSource

  beforeEach(() => {
    source = createInMemoryEventSource()
  })

  afterEach(async () => {
    await source.stop()
  })

  it('registers event handlers', () => {
    const handler = vi.fn()
    const unsubscribe = source.onEvent(handler)

    expect(typeof unsubscribe).toBe('function')
  })

  it('does not emit events when not started', () => {
    const handler = vi.fn()
    source.onEvent(handler)

    source.emit(createTestEvent())

    expect(handler).not.toHaveBeenCalled()
  })

  it('emits events when started', async () => {
    const handler = vi.fn()
    source.onEvent(handler)

    await source.start()

    const event = createTestEvent()
    source.emit(event)

    expect(handler).toHaveBeenCalledWith(event)
  })

  it('emits to multiple handlers', async () => {
    const handler1 = vi.fn()
    const handler2 = vi.fn()

    source.onEvent(handler1)
    source.onEvent(handler2)

    await source.start()

    const event = createTestEvent()
    source.emit(event)

    expect(handler1).toHaveBeenCalledWith(event)
    expect(handler2).toHaveBeenCalledWith(event)
  })

  it('stops emitting to unsubscribed handlers', async () => {
    const handler = vi.fn()
    const unsubscribe = source.onEvent(handler)

    await source.start()

    source.emit(createTestEvent())
    expect(handler).toHaveBeenCalledTimes(1)

    unsubscribe()

    source.emit(createTestEvent())
    expect(handler).toHaveBeenCalledTimes(1) // Still 1
  })

  it('emits multiple events', async () => {
    const handler = vi.fn()
    source.onEvent(handler)

    await source.start()

    const events = [
      createTestEvent({ id: 'evt1' }),
      createTestEvent({ id: 'evt2' }),
      createTestEvent({ id: 'evt3' }),
    ]

    source.emitMany(events)

    expect(handler).toHaveBeenCalledTimes(3)
  })

  it('reports started state', async () => {
    expect(source.isStarted()).toBe(false)

    await source.start()
    expect(source.isStarted()).toBe(true)

    await source.stop()
    expect(source.isStarted()).toBe(false)
  })

  it('handles handler errors gracefully', async () => {
    const errorHandler = vi.fn(() => {
      throw new Error('Handler error')
    })
    const normalHandler = vi.fn()

    source.onEvent(errorHandler)
    source.onEvent(normalHandler)

    await source.start()

    // Should not throw
    expect(() => source.emit(createTestEvent())).not.toThrow()

    // Normal handler should still be called
    expect(normalHandler).toHaveBeenCalled()
  })
})

// =============================================================================
// EventWriterSource Tests
// =============================================================================

describe('EventWriterSource', () => {
  let source: EventWriterSource

  beforeEach(() => {
    source = createEventWriterSource()
  })

  afterEach(async () => {
    await source.stop()
  })

  it('dispatches events when started', async () => {
    const handler = vi.fn()
    source.onEvent(handler)

    await source.start()

    const event = createTestEvent()
    source.handleEvent(event)

    expect(handler).toHaveBeenCalledWith(event)
  })

  it('buffers events when not started', async () => {
    const handler = vi.fn()
    source.onEvent(handler)

    const event = createTestEvent()
    source.handleEvent(event)

    expect(handler).not.toHaveBeenCalled()
    expect(source.getBufferSize()).toBe(1)
  })

  it('clears buffer on start without replay', async () => {
    const handler = vi.fn()
    source.onEvent(handler)

    source.handleEvent(createTestEvent())
    source.handleEvent(createTestEvent())

    await source.start()

    expect(handler).not.toHaveBeenCalled()
    expect(source.getBufferSize()).toBe(0)
  })

  it('replays buffered events on start when configured', async () => {
    const source = createEventWriterSource({ replayOnStart: true })
    const handler = vi.fn()
    source.onEvent(handler)

    source.handleEvent(createTestEvent({ id: 'evt1' }))
    source.handleEvent(createTestEvent({ id: 'evt2' }))

    await source.start()

    expect(handler).toHaveBeenCalledTimes(2)
  })

  it('respects max buffer size', () => {
    const source = createEventWriterSource({ maxBufferSize: 2 })

    source.handleEvent(createTestEvent({ id: 'evt1' }))
    source.handleEvent(createTestEvent({ id: 'evt2' }))
    source.handleEvent(createTestEvent({ id: 'evt3' })) // Should be dropped

    expect(source.getBufferSize()).toBe(2)
  })

  it('handles multiple events at once', async () => {
    const handler = vi.fn()
    source.onEvent(handler)

    await source.start()

    const events = [
      createTestEvent({ id: 'evt1' }),
      createTestEvent({ id: 'evt2' }),
    ]

    source.handleEvents(events)

    expect(handler).toHaveBeenCalledTimes(2)
  })
})

// =============================================================================
// PollingEventSource Tests
// =============================================================================

describe('PollingEventSource', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('polls for events on start', async () => {
    const events = [
      createTestEvent({ id: 'evt1' }),
      createTestEvent({ id: 'evt2' }),
    ]

    const fetcher = vi.fn().mockResolvedValue(events)
    const source = createPollingEventSource(fetcher, { intervalMs: 100 })

    const handler = vi.fn()
    source.onEvent(handler)

    await source.start()

    // Initial poll happens on start, no need to wait
    expect(fetcher).toHaveBeenCalled()
    expect(handler).toHaveBeenCalledTimes(2)

    await source.stop()
  })

  it('passes last event ID to fetcher', async () => {
    const events1 = [createTestEvent({ id: 'evt1' })]
    const events2 = [createTestEvent({ id: 'evt2' })]

    let callCount = 0
    const fetcher = vi.fn().mockImplementation(() => {
      callCount++
      return Promise.resolve(callCount === 1 ? events1 : events2)
    })

    const source = createPollingEventSource(fetcher, { intervalMs: 50 })

    const handler = vi.fn()
    source.onEvent(handler)

    await source.start()

    // Advance time past the interval to trigger second poll
    await vi.advanceTimersByTimeAsync(60)

    // First call should have no lastEventId
    expect(fetcher.mock.calls[0][0]).toBeUndefined()

    // Second call should have 'evt1' as lastEventId
    expect(fetcher.mock.calls[1][0]).toBe('evt1')

    await source.stop()
  })

  it('starts from specified event ID', async () => {
    const fetcher = vi.fn().mockResolvedValue([])
    const source = createPollingEventSource(fetcher, {
      intervalMs: 100,
      startAfter: 'evt_initial',
    })

    await source.start()

    expect(fetcher).toHaveBeenCalledWith('evt_initial')

    await source.stop()
  })

  it('tracks last event ID', async () => {
    const events = [
      createTestEvent({ id: 'evt_a' }),
      createTestEvent({ id: 'evt_c' }),
      createTestEvent({ id: 'evt_b' }), // Out of order
    ]

    const fetcher = vi.fn().mockResolvedValueOnce(events).mockResolvedValue([])
    const source = createPollingEventSource(fetcher, { intervalMs: 100 })

    await source.start()

    // Initial poll happens on start
    // Should be 'evt_c' (highest alphabetically)
    expect(source.getLastEventId()).toBe('evt_c')

    await source.stop()
  })

  it('stops polling on stop', async () => {
    const fetcher = vi.fn().mockResolvedValue([])
    const source = createPollingEventSource(fetcher, { intervalMs: 50 })

    await source.start()
    await source.stop()

    const callCountAtStop = fetcher.mock.calls.length

    // Advance time past interval
    await vi.advanceTimersByTimeAsync(100)

    // Should not have polled again
    expect(fetcher.mock.calls.length).toBe(callCountAtStop)
  })

  it('handles fetch errors gracefully', async () => {
    const fetcher = vi.fn()
      .mockRejectedValueOnce(new Error('Network error'))
      .mockResolvedValue([createTestEvent()])

    const source = createPollingEventSource(fetcher, { intervalMs: 50 })

    const handler = vi.fn()
    source.onEvent(handler)

    // Should not throw
    await expect(source.start()).resolves.not.toThrow()

    // Advance time to trigger retry poll
    await vi.advanceTimersByTimeAsync(60)

    // Should have recovered and delivered events
    expect(handler).toHaveBeenCalled()

    await source.stop()
  })

  it('reports started state', async () => {
    const fetcher = vi.fn().mockResolvedValue([])
    const source = createPollingEventSource(fetcher, { intervalMs: 100 })

    expect(source.isStarted()).toBe(false)

    await source.start()
    expect(source.isStarted()).toBe(true)

    await source.stop()
    expect(source.isStarted()).toBe(false)
  })
})

// =============================================================================
// Factory Function Tests
// =============================================================================

describe('factory functions', () => {
  it('createInMemoryEventSource creates source', () => {
    const source = createInMemoryEventSource()
    expect(source).toBeInstanceOf(InMemoryEventSource)
  })

  it('createEventWriterSource creates source', () => {
    const source = createEventWriterSource()
    expect(source).toBeInstanceOf(EventWriterSource)
  })

  it('createPollingEventSource creates source', () => {
    const fetcher = vi.fn().mockResolvedValue([])
    const source = createPollingEventSource(fetcher)
    expect(source).toBeInstanceOf(PollingEventSource)
  })
})
