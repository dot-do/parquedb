/**
 * EventReplayer Test Suite
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  EventReplayer,
  InMemoryEventSource,
  BatchEventSource,
  createEventReplayer,
  createInMemoryEventSource,
  createBatchEventSource,
} from '../../../src/events/replay'
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
    target: 'users:user1',
    after: { name: 'Test User' },
    ...overrides,
  }
}

// =============================================================================
// InMemoryEventSource Tests
// =============================================================================

describe('InMemoryEventSource', () => {
  let source: InMemoryEventSource

  beforeEach(() => {
    source = new InMemoryEventSource()
  })

  describe('basic operations', () => {
    it('starts empty', async () => {
      const events = await source.getEventsForTarget('users:user1')
      expect(events).toHaveLength(0)
    })

    it('adds and retrieves events', async () => {
      const event = createTestEvent({ target: 'users:user1' })
      source.addEvent(event)

      const events = await source.getEventsForTarget('users:user1')
      expect(events).toHaveLength(1)
      expect(events[0].id).toBe(event.id)
    })

    it('adds multiple events at once', async () => {
      const events = [
        createTestEvent({ target: 'users:user1', ts: 1000 }),
        createTestEvent({ target: 'users:user1', ts: 2000 }),
        createTestEvent({ target: 'users:user2', ts: 3000 }),
      ]
      source.addEvents(events)

      const user1Events = await source.getEventsForTarget('users:user1')
      expect(user1Events).toHaveLength(2)

      const user2Events = await source.getEventsForTarget('users:user2')
      expect(user2Events).toHaveLength(1)
    })

    it('filters by target', async () => {
      source.addEvents([
        createTestEvent({ target: 'users:user1' }),
        createTestEvent({ target: 'users:user2' }),
        createTestEvent({ target: 'posts:post1' }),
      ])

      const userEvents = await source.getEventsForTarget('users:user1')
      expect(userEvents).toHaveLength(1)
      expect(userEvents[0].target).toBe('users:user1')
    })

    it('clears all events', async () => {
      source.addEvents([
        createTestEvent({ target: 'users:user1' }),
        createTestEvent({ target: 'users:user2' }),
      ])

      source.clear()

      const events = source.getAllEvents()
      expect(events).toHaveLength(0)
    })
  })

  describe('time range queries', () => {
    beforeEach(() => {
      source.addEvents([
        createTestEvent({ target: 'users:user1', ts: 1000 }),
        createTestEvent({ target: 'users:user1', ts: 2000 }),
        createTestEvent({ target: 'users:user1', ts: 3000 }),
        createTestEvent({ target: 'users:user1', ts: 4000 }),
      ])
    })

    it('filters by minTs', async () => {
      const events = await source.getEventsForTarget('users:user1', 2500)
      expect(events).toHaveLength(2)
      expect(events.every(e => e.ts >= 2500)).toBe(true)
    })

    it('filters by maxTs', async () => {
      const events = await source.getEventsForTarget('users:user1', undefined, 2500)
      expect(events).toHaveLength(2)
      expect(events.every(e => e.ts <= 2500)).toBe(true)
    })

    it('filters by both minTs and maxTs', async () => {
      const events = await source.getEventsForTarget('users:user1', 1500, 3500)
      expect(events).toHaveLength(2)
      expect(events.every(e => e.ts >= 1500 && e.ts <= 3500)).toBe(true)
    })

    it('gets events in range across targets', async () => {
      source.addEvent(createTestEvent({ target: 'users:user2', ts: 2500 }))

      const events = await source.getEventsInRange(2000, 3000)
      expect(events).toHaveLength(3) // user1@2000, user2@2500, user1@3000
    })
  })
})

// =============================================================================
// BatchEventSource Tests
// =============================================================================

describe('BatchEventSource', () => {
  it('reads events from batches', async () => {
    const batches: EventBatch[] = [
      {
        events: [
          createTestEvent({ target: 'users:user1', ts: 1000 }),
          createTestEvent({ target: 'users:user1', ts: 2000 }),
        ],
        minTs: 1000,
        maxTs: 2000,
        count: 2,
      },
      {
        events: [
          createTestEvent({ target: 'users:user1', ts: 3000 }),
        ],
        minTs: 3000,
        maxTs: 3000,
        count: 1,
      },
    ]

    const source = new BatchEventSource(async () => batches)
    const events = await source.getEventsForTarget('users:user1')

    expect(events).toHaveLength(3)
  })

  it('filters by target', async () => {
    const batches: EventBatch[] = [
      {
        events: [
          createTestEvent({ target: 'users:user1', ts: 1000 }),
          createTestEvent({ target: 'users:user2', ts: 2000 }),
        ],
        minTs: 1000,
        maxTs: 2000,
        count: 2,
      },
    ]

    const source = new BatchEventSource(async () => batches)
    const events = await source.getEventsForTarget('users:user1')

    expect(events).toHaveLength(1)
    expect(events[0].target).toBe('users:user1')
  })

  it('filters by time range', async () => {
    const batches: EventBatch[] = [
      {
        events: [
          createTestEvent({ target: 'users:user1', ts: 1000 }),
          createTestEvent({ target: 'users:user1', ts: 2000 }),
          createTestEvent({ target: 'users:user1', ts: 3000 }),
        ],
        minTs: 1000,
        maxTs: 3000,
        count: 3,
      },
    ]

    const source = new BatchEventSource(async () => batches)
    const events = await source.getEventsForTarget('users:user1', 1500, 2500)

    expect(events).toHaveLength(1)
    expect(events[0].ts).toBe(2000)
  })

  it('passes time range to getBatches', async () => {
    let capturedMinTs: number | undefined
    let capturedMaxTs: number | undefined

    const source = new BatchEventSource(async (minTs, maxTs) => {
      capturedMinTs = minTs
      capturedMaxTs = maxTs
      return []
    })

    await source.getEventsForTarget('users:user1', 1000, 2000)

    expect(capturedMinTs).toBe(1000)
    expect(capturedMaxTs).toBe(2000)
  })

  it('gets events in range', async () => {
    const batches: EventBatch[] = [
      {
        events: [
          createTestEvent({ target: 'users:user1', ts: 1000 }),
          createTestEvent({ target: 'users:user2', ts: 2000 }),
          createTestEvent({ target: 'posts:post1', ts: 3000 }),
        ],
        minTs: 1000,
        maxTs: 3000,
        count: 3,
      },
    ]

    const source = new BatchEventSource(async () => batches)
    const events = await source.getEventsInRange(1500, 2500)

    expect(events).toHaveLength(1)
    expect(events[0].ts).toBe(2000)
  })
})

// =============================================================================
// EventReplayer Tests
// =============================================================================

describe('EventReplayer', () => {
  let source: InMemoryEventSource
  let replayer: EventReplayer

  beforeEach(() => {
    source = new InMemoryEventSource()
    replayer = new EventReplayer(source)
  })

  describe('replayEntity - basic operations', () => {
    it('returns not existed for entity with no events', async () => {
      const result = await replayer.replayEntity('users:user1', { at: 5000 })

      expect(result.existed).toBe(false)
      expect(result.state).toBeNull()
      expect(result.eventsReplayed).toBe(0)
    })

    it('returns not existed for target time before first event', async () => {
      source.addEvent(createTestEvent({
        target: 'users:user1',
        ts: 2000,
        op: 'CREATE',
        after: { name: 'Test' },
      }))

      const result = await replayer.replayEntity('users:user1', { at: 1000 })

      expect(result.existed).toBe(false)
      expect(result.state).toBeNull()
    })

    it('replays CREATE event', async () => {
      source.addEvent(createTestEvent({
        target: 'users:user1',
        ts: 1000,
        op: 'CREATE',
        after: { name: 'Test User', email: 'test@example.com' },
      }))

      const result = await replayer.replayEntity('users:user1', { at: 2000 })

      expect(result.existed).toBe(true)
      expect(result.state).toEqual({ name: 'Test User', email: 'test@example.com' })
      expect(result.eventsReplayed).toBe(1)
    })

    it('replays UPDATE event', async () => {
      source.addEvents([
        createTestEvent({
          target: 'users:user1',
          ts: 1000,
          op: 'CREATE',
          after: { name: 'Original Name' },
        }),
        createTestEvent({
          target: 'users:user1',
          ts: 2000,
          op: 'UPDATE',
          before: { name: 'Original Name' },
          after: { name: 'Updated Name' },
        }),
      ])

      const result = await replayer.replayEntity('users:user1', { at: 3000 })

      expect(result.existed).toBe(true)
      expect(result.state).toEqual({ name: 'Updated Name' })
      expect(result.eventsReplayed).toBe(2)
    })

    it('replays DELETE event', async () => {
      source.addEvents([
        createTestEvent({
          target: 'users:user1',
          ts: 1000,
          op: 'CREATE',
          after: { name: 'Test User' },
        }),
        createTestEvent({
          target: 'users:user1',
          ts: 2000,
          op: 'DELETE',
          before: { name: 'Test User' },
        }),
      ])

      const result = await replayer.replayEntity('users:user1', { at: 3000 })

      expect(result.existed).toBe(false)
      expect(result.state).toBeNull()
      expect(result.eventsReplayed).toBe(2)
    })
  })

  describe('replayEntity - time-travel', () => {
    beforeEach(() => {
      source.addEvents([
        createTestEvent({
          id: 'evt1',
          target: 'users:user1',
          ts: 1000,
          op: 'CREATE',
          after: { name: 'V1', version: 1 },
        }),
        createTestEvent({
          id: 'evt2',
          target: 'users:user1',
          ts: 2000,
          op: 'UPDATE',
          before: { name: 'V1', version: 1 },
          after: { name: 'V2', version: 2 },
        }),
        createTestEvent({
          id: 'evt3',
          target: 'users:user1',
          ts: 3000,
          op: 'UPDATE',
          before: { name: 'V2', version: 2 },
          after: { name: 'V3', version: 3 },
        }),
        createTestEvent({
          id: 'evt4',
          target: 'users:user1',
          ts: 4000,
          op: 'UPDATE',
          before: { name: 'V3', version: 3 },
          after: { name: 'V4', version: 4 },
        }),
      ])
    })

    it('returns state at exact event timestamp', async () => {
      const result = await replayer.replayEntity('users:user1', { at: 2000 })

      expect(result.state).toEqual({ name: 'V2', version: 2 })
    })

    it('returns state between events', async () => {
      const result = await replayer.replayEntity('users:user1', { at: 2500 })

      expect(result.state).toEqual({ name: 'V2', version: 2 })
    })

    it('returns initial state at creation time', async () => {
      const result = await replayer.replayEntity('users:user1', { at: 1000 })

      expect(result.state).toEqual({ name: 'V1', version: 1 })
    })

    it('returns latest state after all events', async () => {
      const result = await replayer.replayEntity('users:user1', { at: 10000 })

      expect(result.state).toEqual({ name: 'V4', version: 4 })
    })
  })

  describe('replayEntity - backward replay', () => {
    beforeEach(() => {
      source.addEvents([
        createTestEvent({
          id: 'evt1',
          target: 'users:user1',
          ts: 1000,
          op: 'CREATE',
          after: { name: 'V1' },
        }),
        createTestEvent({
          id: 'evt2',
          target: 'users:user1',
          ts: 2000,
          op: 'UPDATE',
          before: { name: 'V1' },
          after: { name: 'V2' },
        }),
        createTestEvent({
          id: 'evt3',
          target: 'users:user1',
          ts: 3000,
          op: 'UPDATE',
          before: { name: 'V2' },
          after: { name: 'V3' },
        }),
      ])
    })

    it('replays backward from known state', async () => {
      const result = await replayer.replayEntity('users:user1', {
        at: 1500,
        currentState: { name: 'V3' },
        currentTs: 3000,
      })

      expect(result.state).toEqual({ name: 'V1' })
      expect(result.eventsReplayed).toBe(2) // Undid evt2 and evt3
    })

    it('undoes CREATE to get null state', async () => {
      const result = await replayer.replayEntity('users:user1', {
        at: 500,
        currentState: { name: 'V1' },
        currentTs: 1000,
      })

      expect(result.existed).toBe(false)
      expect(result.state).toBeNull()
    })

    it('undoes DELETE to restore state', async () => {
      source.addEvent(createTestEvent({
        id: 'evt4',
        target: 'users:user1',
        ts: 4000,
        op: 'DELETE',
        before: { name: 'V3' },
      }))

      const result = await replayer.replayEntity('users:user1', {
        at: 3500,
        currentState: null,
        currentTs: 4000,
      })

      expect(result.existed).toBe(true)
      expect(result.state).toEqual({ name: 'V3' })
    })
  })

  describe('replayEntities - batch replay', () => {
    beforeEach(() => {
      source.addEvents([
        createTestEvent({
          target: 'users:user1',
          ts: 1000,
          op: 'CREATE',
          after: { name: 'User 1' },
        }),
        createTestEvent({
          target: 'users:user2',
          ts: 1500,
          op: 'CREATE',
          after: { name: 'User 2' },
        }),
        createTestEvent({
          target: 'posts:post1',
          ts: 2000,
          op: 'CREATE',
          after: { title: 'Post 1' },
        }),
      ])
    })

    it('replays multiple entities at once', async () => {
      const results = await replayer.replayEntities(
        ['users:user1', 'users:user2', 'posts:post1'],
        { at: 3000 }
      )

      expect(results.size).toBe(3)
      expect(results.get('users:user1')?.state).toEqual({ name: 'User 1' })
      expect(results.get('users:user2')?.state).toEqual({ name: 'User 2' })
      expect(results.get('posts:post1')?.state).toEqual({ title: 'Post 1' })
    })

    it('returns partial results for mixed existence', async () => {
      const results = await replayer.replayEntities(
        ['users:user1', 'users:user2', 'users:user3'],
        { at: 1200 }
      )

      expect(results.get('users:user1')?.existed).toBe(true)
      expect(results.get('users:user2')?.existed).toBe(false) // Created at 1500
      expect(results.get('users:user3')?.existed).toBe(false) // Never existed
    })

    it('uses current states for batch backward replay', async () => {
      source.addEvents([
        createTestEvent({
          target: 'users:user1',
          ts: 2000,
          op: 'UPDATE',
          before: { name: 'User 1' },
          after: { name: 'User 1 Updated' },
        }),
        createTestEvent({
          target: 'users:user2',
          ts: 2500,
          op: 'UPDATE',
          before: { name: 'User 2' },
          after: { name: 'User 2 Updated' },
        }),
      ])

      const currentStates = new Map([
        ['users:user1', { name: 'User 1 Updated' }],
        ['users:user2', { name: 'User 2 Updated' }],
      ])

      const results = await replayer.replayEntities(
        ['users:user1', 'users:user2'],
        { at: 1800, currentStates, currentTs: 3000 }
      )

      expect(results.get('users:user1')?.state).toEqual({ name: 'User 1' })
      expect(results.get('users:user2')?.state).toEqual({ name: 'User 2' })
    })
  })

  describe('getStateHistory', () => {
    beforeEach(() => {
      source.addEvents([
        createTestEvent({
          id: 'evt1',
          target: 'users:user1',
          ts: 1000,
          op: 'CREATE',
          after: { name: 'Created' },
        }),
        createTestEvent({
          id: 'evt2',
          target: 'users:user1',
          ts: 2000,
          op: 'UPDATE',
          before: { name: 'Created' },
          after: { name: 'Updated' },
        }),
        createTestEvent({
          id: 'evt3',
          target: 'users:user1',
          ts: 3000,
          op: 'DELETE',
          before: { name: 'Updated' },
        }),
      ])
    })

    it('returns full history', async () => {
      const history = await replayer.getStateHistory('users:user1')

      expect(history).toHaveLength(3)

      expect(history[0]).toEqual({
        ts: 1000,
        state: { name: 'Created' },
        op: 'CREATE',
      })

      expect(history[1]).toEqual({
        ts: 2000,
        state: { name: 'Updated' },
        op: 'UPDATE',
      })

      expect(history[2]).toEqual({
        ts: 3000,
        state: null,
        op: 'DELETE',
      })
    })

    it('filters by time range', async () => {
      const history = await replayer.getStateHistory('users:user1', {
        minTs: 1500,
        maxTs: 2500,
      })

      expect(history).toHaveLength(1)
      expect(history[0].ts).toBe(2000)
    })

    it('returns empty for entity with no events', async () => {
      const history = await replayer.getStateHistory('users:unknown')
      expect(history).toHaveLength(0)
    })
  })

  describe('event ordering', () => {
    it('handles events with same timestamp using ID tiebreaker', async () => {
      source.addEvents([
        createTestEvent({
          id: 'evt_a',
          target: 'users:user1',
          ts: 1000,
          op: 'CREATE',
          after: { value: 'first' },
        }),
        createTestEvent({
          id: 'evt_b',
          target: 'users:user1',
          ts: 1000,
          op: 'UPDATE',
          before: { value: 'first' },
          after: { value: 'second' },
        }),
      ])

      const result = await replayer.replayEntity('users:user1', { at: 2000 })

      // evt_a comes before evt_b alphabetically, so CREATE runs first, UPDATE runs last
      expect(result.state).toEqual({ value: 'second' })
    })

    it('replays events in chronological order', async () => {
      // Add events out of order
      source.addEvents([
        createTestEvent({
          id: 'evt3',
          target: 'users:user1',
          ts: 3000,
          op: 'UPDATE',
          before: { counter: 2 },
          after: { counter: 3 },
        }),
        createTestEvent({
          id: 'evt1',
          target: 'users:user1',
          ts: 1000,
          op: 'CREATE',
          after: { counter: 1 },
        }),
        createTestEvent({
          id: 'evt2',
          target: 'users:user1',
          ts: 2000,
          op: 'UPDATE',
          before: { counter: 1 },
          after: { counter: 2 },
        }),
      ])

      const result = await replayer.replayEntity('users:user1', { at: 4000 })

      expect(result.state).toEqual({ counter: 3 })
      expect(result.eventsReplayed).toBe(3)
    })
  })
})

// =============================================================================
// Factory Function Tests
// =============================================================================

describe('Factory functions', () => {
  it('createEventReplayer creates a replayer', () => {
    const source = new InMemoryEventSource()
    const replayer = createEventReplayer(source)

    expect(replayer).toBeInstanceOf(EventReplayer)
  })

  it('createInMemoryEventSource creates a source', () => {
    const source = createInMemoryEventSource()

    expect(source).toBeInstanceOf(InMemoryEventSource)
  })

  it('createBatchEventSource creates a source', () => {
    const source = createBatchEventSource(async () => [])

    expect(source).toBeInstanceOf(BatchEventSource)
  })
})
