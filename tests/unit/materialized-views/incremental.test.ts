/**
 * Incremental Refresh Test Suite
 *
 * Tests for the incremental refresh logic of materialized views.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  IncrementalRefresher,
  InMemoryMVStorage,
  createIncrementalRefresher,
  createInMemoryMVStorage,
  type IncrementalRefreshOptions,
} from '@/materialized-views/incremental'
import type { MVLineage } from '@/materialized-views/staleness'
import {
  createEmptyLineage,
  serializeLineage,
  deserializeLineage,
} from '@/materialized-views/staleness'
import { InMemoryEventSource } from '@/events/replay'
import { ManifestManager } from '@/events/manifest'
import type { SegmentStorage } from '@/events/segment'
import type { Event } from '@/types'
import type { ViewDefinition } from '@/materialized-views/types'
import { viewName } from '@/materialized-views/types'

// =============================================================================
// Mock Storage
// =============================================================================

class MockSegmentStorage implements SegmentStorage {
  private files: Map<string, Uint8Array> = new Map()

  async put(key: string, data: Uint8Array | ArrayBuffer): Promise<void> {
    const uint8 = data instanceof Uint8Array ? data : new Uint8Array(data)
    this.files.set(key, uint8)
  }

  async get(key: string): Promise<Uint8Array | null> {
    return this.files.get(key) ?? null
  }

  async head(key: string): Promise<boolean> {
    return this.files.has(key)
  }

  async delete(key: string): Promise<void> {
    this.files.delete(key)
  }

  async list(prefix: string): Promise<string[]> {
    return [...this.files.keys()].filter(k => k.startsWith(prefix))
  }

  clear(): void {
    this.files.clear()
  }
}

// =============================================================================
// Helpers
// =============================================================================

function createTestEvent(overrides: Partial<Event> = {}): Event {
  const id = `evt_${Date.now()}_${Math.random().toString(36).slice(2)}`
  return {
    id,
    ts: Date.now(),
    op: 'CREATE',
    target: 'users:user1',
    after: { name: 'Test User', age: 25 },
    ...overrides,
  }
}

function createTestView(overrides: Partial<ViewDefinition> = {}): ViewDefinition {
  return {
    name: viewName('TestView'),
    source: 'users',
    query: {
      filter: undefined,
      project: undefined,
      sort: undefined,
    },
    options: {
      refreshMode: 'streaming',
      refreshStrategy: 'incremental',
    },
    ...overrides,
  }
}

// =============================================================================
// MVLineage Tests
// =============================================================================

describe('MVLineage', () => {
  describe('createEmptyLineage', () => {
    it('creates empty lineage with default values', () => {
      const lineage = createEmptyLineage()

      expect(lineage.lastEventIds?.size).toBe(0)
      expect(lineage.sourceSnapshots?.size).toBe(0)
      expect(lineage.lastRefreshTime).toBe(0)
      expect(lineage.lastEventCount).toBe(0)
      // MVLineage uses viewName and definitionVersionId
      expect(lineage.sourceVersions.size).toBe(0)
    })
  })

  describe('serializeLineage / deserializeLineage', () => {
    it('round-trips lineage through serialization', () => {
      const original: MVLineage = {
        viewName: 'testView' as MVLineage['viewName'],
        sourceVersions: new Map(),
        definitionVersionId: 'ver_789',
        lastRefreshTime: 1000000,
        lastRefreshDurationMs: 100,
        lastRefreshRecordCount: 50,
        lastRefreshType: 'incremental',
        lastEventIds: new Map([['users', 'evt_123']]),
        sourceSnapshots: new Map([['users', 'snap_456']]),
        lastEventCount: 50,
      }

      const serialized = serializeLineage(original)
      const deserialized = deserializeLineage(serialized)

      expect(deserialized.lastEventIds?.get('users')).toBe('evt_123')
      expect(deserialized.sourceSnapshots?.get('users')).toBe('snap_456')
      expect(deserialized.definitionVersionId).toBe('ver_789')
      expect(deserialized.lastRefreshTime).toBe(1000000)
      expect(deserialized.lastEventCount).toBe(50)
      expect(deserialized.lastRefreshType).toBe('incremental')
    })
  })
})

// =============================================================================
// InMemoryMVStorage Tests
// =============================================================================

describe('InMemoryMVStorage', () => {
  let storage: InMemoryMVStorage

  beforeEach(() => {
    storage = createInMemoryMVStorage()
  })

  describe('append', () => {
    it('appends rows to storage', async () => {
      await storage.append([{ name: 'Alice' }, { name: 'Bob' }])

      const rows = await storage.readAll()
      expect(rows).toHaveLength(2)
      expect(rows[0]).toEqual({ name: 'Alice' })
      expect(rows[1]).toEqual({ name: 'Bob' })
    })

    it('accumulates across multiple appends', async () => {
      await storage.append([{ name: 'Alice' }])
      await storage.append([{ name: 'Bob' }])

      const rows = await storage.readAll()
      expect(rows).toHaveLength(2)
    })
  })

  describe('replace', () => {
    it('replaces all rows', async () => {
      await storage.append([{ name: 'Alice' }, { name: 'Bob' }])
      await storage.replace([{ name: 'Charlie' }])

      const rows = await storage.readAll()
      expect(rows).toHaveLength(1)
      expect(rows[0]).toEqual({ name: 'Charlie' })
    })
  })

  describe('update', () => {
    it('updates matching rows', async () => {
      await storage.append([
        { $id: 'user1', name: 'Alice' },
        { $id: 'user2', name: 'Bob' },
      ])

      const updated = await storage.update({ $id: 'user1' }, { name: 'Alice Updated' })

      expect(updated).toBe(1)
      const rows = await storage.readAll()
      expect(rows.find(r => r.$id === 'user1')?.name).toBe('Alice Updated')
      expect(rows.find(r => r.$id === 'user2')?.name).toBe('Bob')
    })
  })

  describe('delete', () => {
    it('deletes matching rows', async () => {
      await storage.append([
        { $id: 'user1', name: 'Alice' },
        { $id: 'user2', name: 'Bob' },
      ])

      const deleted = await storage.delete({ $id: 'user1' })

      expect(deleted).toBe(1)
      const rows = await storage.readAll()
      expect(rows).toHaveLength(1)
      expect(rows[0]?.$id).toBe('user2')
    })
  })

  describe('count', () => {
    it('returns row count', async () => {
      await storage.append([{ name: 'Alice' }, { name: 'Bob' }])

      const count = await storage.count()
      expect(count).toBe(2)
    })
  })

  describe('clear', () => {
    it('removes all rows', () => {
      storage.append([{ name: 'Alice' }])
      storage.clear()

      expect(storage.getRows()).toHaveLength(0)
    })
  })
})

// =============================================================================
// IncrementalRefresher Tests
// =============================================================================

describe('IncrementalRefresher', () => {
  let eventSource: InMemoryEventSource
  let mvStorage: InMemoryMVStorage
  let segmentStorage: MockSegmentStorage
  let manifest: ManifestManager
  let refresher: IncrementalRefresher

  beforeEach(async () => {
    eventSource = new InMemoryEventSource()
    mvStorage = new InMemoryMVStorage()
    segmentStorage = new MockSegmentStorage()
    manifest = new ManifestManager(segmentStorage, { dataset: 'test' })
    await manifest.load()

    refresher = createIncrementalRefresher({
      eventSource,
      storage: mvStorage,
      manifest,
      segmentStorage,
    })
  })

  describe('canRefreshIncrementally', () => {
    it('returns true for empty delta', async () => {
      const view = createTestView()
      const lineage = createEmptyLineage()

      const result = await refresher.canRefreshIncrementally(view, lineage)

      expect(result.canIncremental).toBe(true)
    })

    it('returns false when view is configured for full refresh', async () => {
      const view = createTestView({
        options: { refreshStrategy: 'full' },
      })
      const lineage = createEmptyLineage()

      const result = await refresher.canRefreshIncrementally(view, lineage)

      expect(result.canIncremental).toBe(false)
      expect(result.reason).toContain('full refresh only')
    })

    it('returns false when delta contains DELETE operations', async () => {
      const view = createTestView()
      const lineage = createEmptyLineage()
      lineage.lastRefreshTime = Date.now() - 1000

      eventSource.addEvent(createTestEvent({
        target: 'users:user1',
        op: 'DELETE',
        before: { name: 'Alice' },
      }))

      const result = await refresher.canRefreshIncrementally(view, lineage)

      expect(result.canIncremental).toBe(false)
      expect(result.reason).toContain('DELETE')
    })

    it('returns false for unsupported aggregation stages', async () => {
      const view = createTestView({
        query: {
          pipeline: [
            { $lookup: { from: 'orders', localField: 'userId', foreignField: 'userId', as: 'orders' } },
          ],
        },
      })
      const lineage = createEmptyLineage()

      const result = await refresher.canRefreshIncrementally(view, lineage)

      expect(result.canIncremental).toBe(false)
      expect(result.reason).toContain('$lookup')
    })
  })

  describe('incrementalRefresh', () => {
    it('returns early when no new events', async () => {
      const view = createTestView()
      const lineage = createEmptyLineage()
      lineage.lastRefreshTime = Date.now() + 1000 // Future time, no events after

      const result = await refresher.incrementalRefresh(view, lineage)

      expect(result.success).toBe(true)
      expect(result.eventsProcessed).toBe(0)
      expect(result.wasFullRefresh).toBe(false)
    })

    it('processes CREATE events', async () => {
      const view = createTestView()
      const lineage = createEmptyLineage()
      lineage.lastRefreshTime = Date.now() - 1000

      eventSource.addEvent(createTestEvent({
        target: 'users:user1',
        op: 'CREATE',
        after: { name: 'Alice', age: 25 },
        ts: Date.now(),
      }))

      const result = await refresher.incrementalRefresh(view, lineage)

      expect(result.success).toBe(true)
      expect(result.eventsProcessed).toBe(1)
      expect(result.rowsAdded).toBe(1)
      expect(result.wasFullRefresh).toBe(false)

      const rows = await mvStorage.readAll()
      expect(rows).toHaveLength(1)
      expect(rows[0]?.name).toBe('Alice')
    })

    it('applies filter to CREATE events', async () => {
      const view = createTestView({
        query: {
          filter: { age: { $gte: 30 } },
        },
      })
      const lineage = createEmptyLineage()
      lineage.lastRefreshTime = Date.now() - 1000

      // This should be filtered out (age < 30)
      eventSource.addEvent(createTestEvent({
        target: 'users:user1',
        op: 'CREATE',
        after: { name: 'Young', age: 25 },
        ts: Date.now(),
      }))

      // This should be included (age >= 30)
      eventSource.addEvent(createTestEvent({
        target: 'users:user2',
        op: 'CREATE',
        after: { name: 'Older', age: 35 },
        ts: Date.now(),
      }))

      const result = await refresher.incrementalRefresh(view, lineage)

      expect(result.rowsAdded).toBe(1)
      const rows = await mvStorage.readAll()
      expect(rows[0]?.name).toBe('Older')
    })

    it('applies projection to CREATE events', async () => {
      const view = createTestView({
        query: {
          project: { name: 1 },
        },
      })
      const lineage = createEmptyLineage()
      lineage.lastRefreshTime = Date.now() - 1000

      eventSource.addEvent(createTestEvent({
        target: 'users:user1',
        op: 'CREATE',
        after: { name: 'Alice', age: 25, secret: 'password' },
        ts: Date.now(),
      }))

      await refresher.incrementalRefresh(view, lineage)

      const rows = await mvStorage.readAll()
      expect(rows[0]).toEqual({ name: 'Alice' })
      expect(rows[0]).not.toHaveProperty('age')
      expect(rows[0]).not.toHaveProperty('secret')
    })

    it('updates lineage after processing', async () => {
      const view = createTestView()
      const lineage = createEmptyLineage()
      lineage.lastRefreshTime = Date.now() - 1000

      const event = createTestEvent({
        target: 'users:user1',
        op: 'CREATE',
        after: { name: 'Alice' },
        ts: Date.now(),
      })
      eventSource.addEvent(event)

      const result = await refresher.incrementalRefresh(view, lineage)

      expect(result.lineage.lastEventIds.get('users')).toBe(event.id)
      expect(result.lineage.lastRefreshTime).toBeGreaterThan(lineage.lastRefreshTime)
      expect(result.lineage.lastEventCount).toBe(1)
    })

    it('respects maxEvents option', async () => {
      const view = createTestView()
      const lineage = createEmptyLineage()
      const baseTime = Date.now()
      lineage.lastRefreshTime = baseTime - 2000

      // Add 10 events with timestamps in the valid range
      for (let i = 0; i < 10; i++) {
        eventSource.addEvent(createTestEvent({
          target: `users:user${i}`,
          op: 'CREATE',
          after: { name: `User ${i}` },
          ts: baseTime - 1000 + i, // Within the range of lastRefreshTime to now
        }))
      }

      const options: IncrementalRefreshOptions = { maxEvents: 3 }
      const result = await refresher.incrementalRefresh(view, lineage, options)

      expect(result.eventsProcessed).toBe(3)
    })

    it('reports progress via callback', async () => {
      const view = createTestView()
      const lineage = createEmptyLineage()
      const baseTime = Date.now()
      lineage.lastRefreshTime = baseTime - 2000

      for (let i = 0; i < 5; i++) {
        eventSource.addEvent(createTestEvent({
          target: `users:user${i}`,
          op: 'CREATE',
          after: { name: `User ${i}` },
          ts: baseTime - 1000 + i, // Within the range of lastRefreshTime to now
        }))
      }

      const progress: Array<{ processed: number; total: number }> = []
      const options: IncrementalRefreshOptions = {
        onProgress: (processed, total) => progress.push({ processed, total }),
      }

      await refresher.incrementalRefresh(view, lineage, options)

      expect(progress).toHaveLength(5)
      expect(progress[0]).toEqual({ processed: 1, total: 5 })
      expect(progress[4]).toEqual({ processed: 5, total: 5 })
    })
  })

  describe('fullRefresh', () => {
    it('recomputes entire view from events', async () => {
      const view = createTestView()
      const lineage = createEmptyLineage()

      // Add some events
      eventSource.addEvent(createTestEvent({
        target: 'users:user1',
        op: 'CREATE',
        after: { name: 'Alice' },
        ts: 1000,
      }))
      eventSource.addEvent(createTestEvent({
        target: 'users:user2',
        op: 'CREATE',
        after: { name: 'Bob' },
        ts: 2000,
      }))

      const result = await refresher.fullRefresh(view, lineage)

      expect(result.success).toBe(true)
      expect(result.wasFullRefresh).toBe(true)
      expect(result.rowsAdded).toBe(2)

      const rows = await mvStorage.readAll()
      expect(rows).toHaveLength(2)
    })

    it('excludes deleted entities', async () => {
      const view = createTestView()
      const lineage = createEmptyLineage()

      eventSource.addEvent(createTestEvent({
        target: 'users:user1',
        op: 'CREATE',
        after: { name: 'Alice' },
        ts: 1000,
      }))
      eventSource.addEvent(createTestEvent({
        target: 'users:user1',
        op: 'DELETE',
        before: { name: 'Alice' },
        ts: 2000,
      }))
      eventSource.addEvent(createTestEvent({
        target: 'users:user2',
        op: 'CREATE',
        after: { name: 'Bob' },
        ts: 3000,
      }))

      const result = await refresher.fullRefresh(view, lineage)

      expect(result.rowsAdded).toBe(1)
      const rows = await mvStorage.readAll()
      expect(rows).toHaveLength(1)
      expect(rows[0]?.name).toBe('Bob')
    })

    it('applies filter during full refresh', async () => {
      const view = createTestView({
        query: {
          filter: { status: 'active' },
        },
      })
      const lineage = createEmptyLineage()

      eventSource.addEvent(createTestEvent({
        target: 'users:user1',
        op: 'CREATE',
        after: { name: 'Active', status: 'active' },
        ts: 1000,
      }))
      eventSource.addEvent(createTestEvent({
        target: 'users:user2',
        op: 'CREATE',
        after: { name: 'Inactive', status: 'inactive' },
        ts: 2000,
      }))

      const result = await refresher.fullRefresh(view, lineage)

      expect(result.rowsAdded).toBe(1)
      const rows = await mvStorage.readAll()
      expect(rows[0]?.name).toBe('Active')
    })

    it('executes aggregation pipeline', async () => {
      const view = createTestView({
        query: {
          pipeline: [
            { $group: { _id: '$status', count: { $count: '*' } } },
          ],
        },
      })
      const lineage = createEmptyLineage()

      eventSource.addEvent(createTestEvent({
        target: 'users:user1',
        op: 'CREATE',
        after: { name: 'Alice', status: 'active' },
        ts: 1000,
      }))
      eventSource.addEvent(createTestEvent({
        target: 'users:user2',
        op: 'CREATE',
        after: { name: 'Bob', status: 'active' },
        ts: 2000,
      }))
      eventSource.addEvent(createTestEvent({
        target: 'users:user3',
        op: 'CREATE',
        after: { name: 'Charlie', status: 'inactive' },
        ts: 3000,
      }))

      await refresher.fullRefresh(view, lineage)

      const rows = await mvStorage.readAll()
      expect(rows).toHaveLength(2)

      const activeGroup = rows.find(r => r._id === 'active')
      const inactiveGroup = rows.find(r => r._id === 'inactive')

      expect(activeGroup?.count).toBe(2)
      expect(inactiveGroup?.count).toBe(1)
    })

    it('creates fresh lineage', async () => {
      const view = createTestView()
      const lineage = createEmptyLineage()

      const event = createTestEvent({
        target: 'users:user1',
        op: 'CREATE',
        after: { name: 'Alice' },
        ts: Date.now(),
      })
      eventSource.addEvent(event)

      const result = await refresher.fullRefresh(view, lineage)

      expect(result.lineage.lastEventIds?.get('users')).toBe(event.id)
      expect(result.lineage.definitionVersionId).toBeTruthy()
      expect(result.lineage.definitionVersionId).not.toBe(lineage.definitionVersionId)
    })
  })

  describe('refresh (automatic mode selection)', () => {
    it('uses incremental when possible', async () => {
      const view = createTestView()
      const lineage = createEmptyLineage()
      lineage.lastRefreshTime = Date.now() - 1000

      eventSource.addEvent(createTestEvent({
        target: 'users:user1',
        op: 'CREATE',
        after: { name: 'Alice' },
        ts: Date.now(),
      }))

      const result = await refresher.refresh(view, lineage)

      expect(result.success).toBe(true)
      expect(result.wasFullRefresh).toBe(false)
    })

    it('falls back to full when incremental not possible', async () => {
      const view = createTestView()
      const lineage = createEmptyLineage()
      lineage.lastRefreshTime = Date.now() - 1000

      eventSource.addEvent(createTestEvent({
        target: 'users:user1',
        op: 'DELETE',
        before: { name: 'Alice' },
        ts: Date.now(),
      }))

      const result = await refresher.refresh(view, lineage)

      expect(result.success).toBe(true)
      expect(result.wasFullRefresh).toBe(true)
    })

    it('respects forceFullRefresh option', async () => {
      const view = createTestView()
      const lineage = createEmptyLineage()
      lineage.lastRefreshTime = Date.now() - 1000

      eventSource.addEvent(createTestEvent({
        target: 'users:user1',
        op: 'CREATE',
        after: { name: 'Alice' },
        ts: Date.now(),
      }))

      const result = await refresher.refresh(view, lineage, { forceFullRefresh: true })

      expect(result.wasFullRefresh).toBe(true)
    })

    it('handles errors gracefully', async () => {
      // Create a view with an invalid configuration
      const view = createTestView({
        source: 'nonexistent',
      })
      const lineage = createEmptyLineage()

      // The refresh should still succeed (no events for nonexistent source)
      const result = await refresher.refresh(view, lineage)

      expect(result.success).toBe(true)
    })
  })

  describe('aggregation operations', () => {
    it('handles $sum aggregation', async () => {
      const view = createTestView({
        query: {
          pipeline: [
            { $group: { _id: null, total: { $sum: '$amount' } } },
          ],
        },
      })
      const lineage = createEmptyLineage()

      eventSource.addEvent(createTestEvent({
        target: 'users:user1',
        op: 'CREATE',
        after: { name: 'Alice', amount: 100 },
      }))
      eventSource.addEvent(createTestEvent({
        target: 'users:user2',
        op: 'CREATE',
        after: { name: 'Bob', amount: 50 },
      }))

      await refresher.fullRefresh(view, lineage)

      const rows = await mvStorage.readAll()
      expect(rows[0]?.total).toBe(150)
    })

    it('handles $min and $max aggregation', async () => {
      const view = createTestView({
        query: {
          pipeline: [
            { $group: { _id: null, minAge: { $min: '$age' }, maxAge: { $max: '$age' } } },
          ],
        },
      })
      const lineage = createEmptyLineage()

      eventSource.addEvent(createTestEvent({
        target: 'users:user1',
        op: 'CREATE',
        after: { age: 25 },
      }))
      eventSource.addEvent(createTestEvent({
        target: 'users:user2',
        op: 'CREATE',
        after: { age: 35 },
      }))
      eventSource.addEvent(createTestEvent({
        target: 'users:user3',
        op: 'CREATE',
        after: { age: 30 },
      }))

      await refresher.fullRefresh(view, lineage)

      const rows = await mvStorage.readAll()
      expect(rows[0]?.minAge).toBe(25)
      expect(rows[0]?.maxAge).toBe(35)
    })

    it('handles $avg aggregation', async () => {
      const view = createTestView({
        query: {
          pipeline: [
            { $group: { _id: null, avgAge: { $avg: '$age' } } },
          ],
        },
      })
      const lineage = createEmptyLineage()

      eventSource.addEvent(createTestEvent({
        target: 'users:user1',
        op: 'CREATE',
        after: { age: 20 },
      }))
      eventSource.addEvent(createTestEvent({
        target: 'users:user2',
        op: 'CREATE',
        after: { age: 30 },
      }))
      eventSource.addEvent(createTestEvent({
        target: 'users:user3',
        op: 'CREATE',
        after: { age: 40 },
      }))

      await refresher.fullRefresh(view, lineage)

      const rows = await mvStorage.readAll()
      expect(rows[0]?.avgAge).toBe(30)
    })

    it('handles $match pipeline stage', async () => {
      const view = createTestView({
        query: {
          pipeline: [
            { $match: { status: 'active' } },
            { $group: { _id: null, count: { $count: '*' } } },
          ],
        },
      })
      const lineage = createEmptyLineage()

      eventSource.addEvent(createTestEvent({
        target: 'users:user1',
        op: 'CREATE',
        after: { status: 'active' },
      }))
      eventSource.addEvent(createTestEvent({
        target: 'users:user2',
        op: 'CREATE',
        after: { status: 'inactive' },
      }))
      eventSource.addEvent(createTestEvent({
        target: 'users:user3',
        op: 'CREATE',
        after: { status: 'active' },
      }))

      await refresher.fullRefresh(view, lineage)

      const rows = await mvStorage.readAll()
      expect(rows[0]?.count).toBe(2)
    })

    it('handles $sort pipeline stage', async () => {
      const view = createTestView({
        query: {
          pipeline: [
            { $sort: { age: -1 } },
          ],
        },
      })
      const lineage = createEmptyLineage()

      eventSource.addEvent(createTestEvent({
        target: 'users:user1',
        op: 'CREATE',
        after: { name: 'Young', age: 25 },
      }))
      eventSource.addEvent(createTestEvent({
        target: 'users:user2',
        op: 'CREATE',
        after: { name: 'Old', age: 45 },
      }))
      eventSource.addEvent(createTestEvent({
        target: 'users:user3',
        op: 'CREATE',
        after: { name: 'Middle', age: 35 },
      }))

      await refresher.fullRefresh(view, lineage)

      const rows = await mvStorage.readAll()
      expect(rows[0]?.name).toBe('Old')
      expect(rows[1]?.name).toBe('Middle')
      expect(rows[2]?.name).toBe('Young')
    })

    it('handles $limit pipeline stage', async () => {
      const view = createTestView({
        query: {
          pipeline: [
            { $limit: 2 },
          ],
        },
      })
      const lineage = createEmptyLineage()

      for (let i = 0; i < 5; i++) {
        eventSource.addEvent(createTestEvent({
          target: `users:user${i}`,
          op: 'CREATE',
          after: { name: `User ${i}` },
        }))
      }

      await refresher.fullRefresh(view, lineage)

      const rows = await mvStorage.readAll()
      expect(rows).toHaveLength(2)
    })

    it('handles $skip pipeline stage', async () => {
      const view = createTestView({
        query: {
          pipeline: [
            { $skip: 2 },
          ],
        },
      })
      const lineage = createEmptyLineage()

      for (let i = 0; i < 5; i++) {
        eventSource.addEvent(createTestEvent({
          target: `users:user${i}`,
          op: 'CREATE',
          after: { name: `User ${i}` },
        }))
      }

      await refresher.fullRefresh(view, lineage)

      const rows = await mvStorage.readAll()
      expect(rows).toHaveLength(3)
    })
  })

  describe('filter operators', () => {
    it('handles $eq operator', async () => {
      const view = createTestView({
        query: { filter: { status: { $eq: 'active' } } },
      })
      const lineage = createEmptyLineage()
      lineage.lastRefreshTime = Date.now() - 1000

      eventSource.addEvent(createTestEvent({
        target: 'users:user1',
        op: 'CREATE',
        after: { status: 'active' },
        ts: Date.now(),
      }))
      eventSource.addEvent(createTestEvent({
        target: 'users:user2',
        op: 'CREATE',
        after: { status: 'inactive' },
        ts: Date.now(),
      }))

      await refresher.incrementalRefresh(view, lineage)

      const rows = await mvStorage.readAll()
      expect(rows).toHaveLength(1)
    })

    it('handles $ne operator', async () => {
      const view = createTestView({
        query: { filter: { status: { $ne: 'deleted' } } },
      })
      const lineage = createEmptyLineage()
      lineage.lastRefreshTime = Date.now() - 1000

      eventSource.addEvent(createTestEvent({
        target: 'users:user1',
        op: 'CREATE',
        after: { status: 'active' },
        ts: Date.now(),
      }))
      eventSource.addEvent(createTestEvent({
        target: 'users:user2',
        op: 'CREATE',
        after: { status: 'deleted' },
        ts: Date.now(),
      }))

      await refresher.incrementalRefresh(view, lineage)

      const rows = await mvStorage.readAll()
      expect(rows).toHaveLength(1)
    })

    it('handles $in operator', async () => {
      const view = createTestView({
        query: { filter: { status: { $in: ['active', 'pending'] } } },
      })
      const lineage = createEmptyLineage()
      lineage.lastRefreshTime = Date.now() - 1000

      eventSource.addEvent(createTestEvent({
        target: 'users:user1',
        op: 'CREATE',
        after: { status: 'active' },
        ts: Date.now(),
      }))
      eventSource.addEvent(createTestEvent({
        target: 'users:user2',
        op: 'CREATE',
        after: { status: 'deleted' },
        ts: Date.now(),
      }))
      eventSource.addEvent(createTestEvent({
        target: 'users:user3',
        op: 'CREATE',
        after: { status: 'pending' },
        ts: Date.now(),
      }))

      await refresher.incrementalRefresh(view, lineage)

      const rows = await mvStorage.readAll()
      expect(rows).toHaveLength(2)
    })

    it('handles $exists operator', async () => {
      const view = createTestView({
        query: { filter: { email: { $exists: true } } },
      })
      const lineage = createEmptyLineage()
      lineage.lastRefreshTime = Date.now() - 1000

      eventSource.addEvent(createTestEvent({
        target: 'users:user1',
        op: 'CREATE',
        after: { name: 'With Email', email: 'test@example.com' },
        ts: Date.now(),
      }))
      eventSource.addEvent(createTestEvent({
        target: 'users:user2',
        op: 'CREATE',
        after: { name: 'No Email' },
        ts: Date.now(),
      }))

      await refresher.incrementalRefresh(view, lineage)

      const rows = await mvStorage.readAll()
      expect(rows).toHaveLength(1)
      expect(rows[0]?.name).toBe('With Email')
    })
  })
})

// =============================================================================
// Factory Function Tests
// =============================================================================

describe('Factory functions', () => {
  it('createIncrementalRefresher creates a refresher', () => {
    const eventSource = new InMemoryEventSource()
    const storage = new InMemoryMVStorage()
    const segmentStorage = new MockSegmentStorage()
    const manifest = new ManifestManager(segmentStorage, { dataset: 'test' })

    const refresher = createIncrementalRefresher({
      eventSource,
      storage,
      manifest,
      segmentStorage,
    })

    expect(refresher).toBeInstanceOf(IncrementalRefresher)
  })

  it('createEmptyLineage creates empty lineage', () => {
    const lineage = createEmptyLineage()
    expect(lineage).toBeDefined()
    expect(lineage.lastEventIds).toBeInstanceOf(Map)
  })

  it('createInMemoryMVStorage creates storage', () => {
    const storage = createInMemoryMVStorage()
    expect(storage).toBeInstanceOf(InMemoryMVStorage)
  })
})
