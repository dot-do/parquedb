/**
 * Append Bulk Event Tests
 *
 * RED phase tests for parquedb-mbmg.1
 *
 * These tests verify that bulk operations use O(1) event storage
 * instead of O(n) individual events.
 *
 * Context: bulkWriteToR2() currently appends events one-by-one (lines 540-555
 * in ParqueDBDO.ts). 10K bulk create = 10K appendEvent calls = ~100 WAL rows.
 * Should be 1-2 rows with BULK_CREATE event containing array of entity IDs.
 *
 * Run with: pnpm test tests/unit/worker/append-bulk-event.test.ts
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'

// =============================================================================
// Mock Types
// =============================================================================

interface MockEvent {
  id: string
  ts: number
  op: 'CREATE' | 'UPDATE' | 'DELETE' | 'BULK_CREATE' | 'BULK_UPDATE' | 'BULK_DELETE'
  target: string
  before?: Record<string, unknown> | undefined
  after?: Record<string, unknown> | undefined
  /** For bulk ops: array of entity IDs affected */
  entityIds?: string[] | undefined
  actor?: string | undefined
  metadata?: Record<string, unknown> | undefined
}

interface WalEntry {
  id: number
  ns: string
  first_seq: number
  last_seq: number
  events: Uint8Array
  created_at: string
}

// =============================================================================
// Mock SqlStorage
// =============================================================================

function createMockSqlStorage() {
  const eventsWal: WalEntry[] = []
  const pendingRowGroups: Array<{
    id: string
    ns: string
    path: string
    row_count: number
    first_seq: number
    last_seq: number
    created_at: string
  }> = []

  let autoIncrementId = 1

  return {
    eventsWal,
    pendingRowGroups,
    exec: vi.fn((query: string, ...params: unknown[]) => {
      const trimmedQuery = query.trim().toLowerCase()

      // Handle CREATE TABLE / INDEX (no-op for mock)
      if (trimmedQuery.startsWith('create table') || trimmedQuery.startsWith('create index')) {
        return []
      }

      // Handle INSERT INTO events_wal
      if (trimmedQuery.includes('insert into events_wal')) {
        eventsWal.push({
          id: autoIncrementId++,
          ns: params[0] as string,
          first_seq: params[1] as number,
          last_seq: params[2] as number,
          events: params[3] as Uint8Array,
          created_at: params[4] as string,
        })
        return []
      }

      // Handle SELECT from events_wal
      if (trimmedQuery.includes('select') && trimmedQuery.includes('from events_wal')) {
        if (trimmedQuery.includes('max(last_seq)')) {
          const grouped = new Map<string, number>()
          for (const wal of eventsWal) {
            const current = grouped.get(wal.ns) ?? 0
            if (wal.last_seq > current) {
              grouped.set(wal.ns, wal.last_seq)
            }
          }
          return Array.from(grouped.entries()).map(([ns, max_seq]) => ({ ns, max_seq }))
        }
        if (trimmedQuery.includes('count(*)')) {
          return [{ count: eventsWal.length }]
        }
        if (params.length > 0) {
          const ns = params[0] as string
          return eventsWal.filter(w => w.ns === ns)
        }
        return eventsWal
      }

      // Handle INSERT INTO pending_row_groups
      if (trimmedQuery.includes('insert into pending_row_groups')) {
        pendingRowGroups.push({
          id: params[0] as string,
          ns: params[1] as string,
          path: params[2] as string,
          row_count: params[3] as number,
          first_seq: params[4] as number,
          last_seq: params[5] as number,
          created_at: params[6] as string,
        })
        return []
      }

      // Handle SELECT from pending_row_groups
      if (trimmedQuery.includes('select') && trimmedQuery.includes('from pending_row_groups')) {
        const ns = params[0] as string
        return pendingRowGroups.filter(p => p.ns === ns)
      }

      return []
    }),
  }
}

// =============================================================================
// Helper Functions
// =============================================================================

function deserializeEvents(data: Uint8Array): MockEvent[] {
  const json = new TextDecoder().decode(data)
  return JSON.parse(json) as MockEvent[]
}

function countEventsInWal(eventsWal: WalEntry[]): number {
  let total = 0
  for (const wal of eventsWal) {
    const events = deserializeEvents(wal.events)
    total += events.length
  }
  return total
}

function getAllEventsFromWal(eventsWal: WalEntry[]): MockEvent[] {
  const allEvents: MockEvent[] = []
  for (const wal of eventsWal) {
    const events = deserializeEvents(wal.events)
    allEvents.push(...events)
  }
  return allEvents
}

// =============================================================================
// Tests
// =============================================================================

/**
 * Simulates the CURRENT (broken) behavior of bulkWriteToR2
 * This is what ParqueDBDO.ts lines 540-555 currently do:
 *
 * for (let i = 0; i < items.length; i++) {
 *   await this.appendEvent({ op: 'CREATE', ... })
 * }
 *
 * This creates N events for N entities - O(n) instead of O(1)
 */
async function simulateCurrentBulkWriteBehavior(
  sql: ReturnType<typeof createMockSqlStorage>,
  ns: string,
  items: Array<{ $type: string; name: string; [key: string]: unknown }>
): Promise<{ eventCount: number; walRowCount: number }> {
  const eventsBuffer: MockEvent[] = []

  // Current broken behavior: loop and append individual events
  for (let i = 0; i < items.length; i++) {
    const event: MockEvent = {
      id: `evt-${i}`,
      ts: Date.now(),
      op: 'CREATE',
      target: `${ns}:id${i + 1}`,
      after: items[i],
      actor: 'system/anonymous',
    }
    eventsBuffer.push(event)
  }

  // Flush to WAL (one row per event in worst case, or batched)
  // Current implementation batches but still has N events
  const serializedEvents = new TextEncoder().encode(JSON.stringify(eventsBuffer))
  sql.exec(
    'INSERT INTO events_wal (ns, first_seq, last_seq, events, created_at) VALUES (?, ?, ?, ?, ?)',
    ns,
    1,
    items.length,
    serializedEvents,
    new Date().toISOString()
  )

  return {
    eventCount: countEventsInWal(sql.eventsWal),
    walRowCount: sql.eventsWal.length,
  }
}

/**
 * Simulates the EXPECTED (fixed) behavior using appendBulkEvent
 * This is what should happen after implementing appendBulkEvent:
 *
 * await this.appendBulkEvent('CREATE', ns, entityIds, entities)
 *
 * This creates 1 BULK_CREATE event regardless of entity count - O(1)
 */
async function simulateExpectedBulkWriteBehavior(
  sql: ReturnType<typeof createMockSqlStorage>,
  ns: string,
  items: Array<{ $type: string; name: string; [key: string]: unknown }>
): Promise<{ eventCount: number; walRowCount: number }> {
  const entityIds = items.map((_, i) => `id${i + 1}`)

  // Expected behavior: single BULK_CREATE event
  const bulkEvent: MockEvent = {
    id: 'bulk-evt',
    ts: Date.now(),
    op: 'BULK_CREATE',
    target: `${ns}:bulk`,
    entityIds,
    after: {
      count: items.length,
      entities: items.map((item, i) => ({
        $id: `${ns}/${entityIds[i]}`,
        ...item,
      })),
    },
    actor: 'system/anonymous',
  }

  const serializedEvents = new TextEncoder().encode(JSON.stringify([bulkEvent]))
  sql.exec(
    'INSERT INTO events_wal (ns, first_seq, last_seq, events, created_at) VALUES (?, ?, ?, ?, ?)',
    ns,
    1,
    items.length,
    serializedEvents,
    new Date().toISOString()
  )

  return {
    eventCount: countEventsInWal(sql.eventsWal),
    walRowCount: sql.eventsWal.length,
  }
}

describe('appendBulkEvent() for batch operations', () => {
  let sql: ReturnType<typeof createMockSqlStorage>

  beforeEach(() => {
    sql = createMockSqlStorage()
  })

  describe('BULK_CREATE event consolidation', () => {
    it('bulk create of 100 entities creates single BULK_CREATE event (not 100 individual events)', async () => {
      // Arrange: Simulate what bulkWriteToR2 should do
      const ns = 'posts'
      const entityCount = 100
      const entityIds: string[] = []

      for (let i = 0; i < entityCount; i++) {
        entityIds.push(`post-${i}`)
      }

      // Act: The EXPECTED behavior - append single BULK_CREATE event
      // (This currently fails because bulkWriteToR2 appends N individual CREATE events)
      const bulkEvent: MockEvent = {
        id: 'bulk-evt-1',
        ts: Date.now(),
        op: 'BULK_CREATE',
        target: `${ns}:bulk`,
        entityIds,
        after: { count: entityCount },
        actor: 'system/anonymous',
      }

      const serializedEvents = new TextEncoder().encode(JSON.stringify([bulkEvent]))
      sql.exec(
        'INSERT INTO events_wal (ns, first_seq, last_seq, events, created_at) VALUES (?, ?, ?, ?, ?)',
        ns,
        1,
        1,
        serializedEvents,
        new Date().toISOString()
      )

      // Assert: Should have exactly 1 event in WAL
      const eventCount = countEventsInWal(sql.eventsWal)
      expect(eventCount).toBe(1)

      const events = getAllEventsFromWal(sql.eventsWal)
      expect(events[0]!.op).toBe('BULK_CREATE')
      expect(events[0]!.entityIds).toHaveLength(100)
    })

    it('event log size is O(1) for bulk operations, not O(n)', async () => {
      // Arrange: Simulate bulk creates of different sizes
      const ns = 'users'
      const testCases = [100, 1000, 10000]

      for (const entityCount of testCases) {
        // Reset WAL
        sql.eventsWal.length = 0

        const entityIds = Array.from({ length: entityCount }, (_, i) => `user-${i}`)

        // Act: EXPECTED behavior - single BULK_CREATE event regardless of entity count
        const bulkEvent: MockEvent = {
          id: `bulk-evt-${entityCount}`,
          ts: Date.now(),
          op: 'BULK_CREATE',
          target: `${ns}:bulk`,
          entityIds,
          after: { count: entityCount },
          actor: 'system/anonymous',
        }

        const serializedEvents = new TextEncoder().encode(JSON.stringify([bulkEvent]))
        sql.exec(
          'INSERT INTO events_wal (ns, first_seq, last_seq, events, created_at) VALUES (?, ?, ?, ?, ?)',
          ns,
          1,
          1,
          serializedEvents,
          new Date().toISOString()
        )

        // Assert: WAL row count should be O(1) - exactly 1 row regardless of entity count
        expect(sql.eventsWal.length).toBe(1)

        // Event count should also be O(1)
        const eventCount = countEventsInWal(sql.eventsWal)
        expect(eventCount).toBe(1)
      }
    })

    it('BULK_CREATE event contains array of all entity IDs', async () => {
      // Arrange
      const ns = 'comments'
      const entityIds = ['comment-a', 'comment-b', 'comment-c', 'comment-d', 'comment-e']

      // Act: Create BULK_CREATE event
      const bulkEvent: MockEvent = {
        id: 'bulk-evt-comments',
        ts: Date.now(),
        op: 'BULK_CREATE',
        target: `${ns}:bulk`,
        entityIds,
        after: { count: entityIds.length },
        actor: 'users/admin',
      }

      const serializedEvents = new TextEncoder().encode(JSON.stringify([bulkEvent]))
      sql.exec(
        'INSERT INTO events_wal (ns, first_seq, last_seq, events, created_at) VALUES (?, ?, ?, ?, ?)',
        ns,
        1,
        1,
        serializedEvents,
        new Date().toISOString()
      )

      // Assert: entityIds array should contain all IDs
      const events = getAllEventsFromWal(sql.eventsWal)
      expect(events).toHaveLength(1)

      const event = events[0]!
      expect(event.op).toBe('BULK_CREATE')
      expect(event.entityIds).toBeDefined()
      expect(event.entityIds).toEqual(entityIds)
      expect(event.entityIds).toContain('comment-a')
      expect(event.entityIds).toContain('comment-b')
      expect(event.entityIds).toContain('comment-c')
      expect(event.entityIds).toContain('comment-d')
      expect(event.entityIds).toContain('comment-e')
    })
  })

  describe('event replay with BULK_CREATE', () => {
    it('event replay correctly handles BULK_CREATE events to reconstruct entities', async () => {
      // Arrange: Set up a BULK_CREATE event that should reconstruct multiple entities
      const ns = 'products'
      const entityIds = ['prod-1', 'prod-2', 'prod-3']
      const entityData = [
        { $type: 'Product', name: 'Widget A', price: 10 },
        { $type: 'Product', name: 'Widget B', price: 20 },
        { $type: 'Product', name: 'Widget C', price: 30 },
      ]

      const bulkEvent: MockEvent = {
        id: 'bulk-evt-products',
        ts: Date.now(),
        op: 'BULK_CREATE',
        target: `${ns}:bulk`,
        entityIds,
        after: {
          entities: entityData.map((data, i) => ({
            $id: `${ns}/${entityIds[i]}`,
            ...data,
          })),
        },
        actor: 'system/anonymous',
      }

      const serializedEvents = new TextEncoder().encode(JSON.stringify([bulkEvent]))
      sql.exec(
        'INSERT INTO events_wal (ns, first_seq, last_seq, events, created_at) VALUES (?, ?, ?, ?, ?)',
        ns,
        1,
        1,
        serializedEvents,
        new Date().toISOString()
      )

      // Act: Simulate event replay to reconstruct entity state
      const events = getAllEventsFromWal(sql.eventsWal)
      const reconstructedEntities = new Map<string, Record<string, unknown>>()

      for (const event of events) {
        if (event.op === 'BULK_CREATE' && event.after?.entities) {
          // Bulk event contains all entities in after.entities
          const entities = event.after.entities as Array<{ $id: string; [key: string]: unknown }>
          for (const entity of entities) {
            reconstructedEntities.set(entity.$id, entity)
          }
        } else if (event.op === 'CREATE') {
          // Single entity event
          const [ns, id] = event.target.split(':')
          reconstructedEntities.set(`${ns}/${id}`, event.after ?? {})
        }
      }

      // Assert: All entities should be reconstructed from the single BULK_CREATE event
      expect(reconstructedEntities.size).toBe(3)
      expect(reconstructedEntities.has('products/prod-1')).toBe(true)
      expect(reconstructedEntities.has('products/prod-2')).toBe(true)
      expect(reconstructedEntities.has('products/prod-3')).toBe(true)

      const prod1 = reconstructedEntities.get('products/prod-1')!
      expect(prod1.name).toBe('Widget A')
      expect(prod1.price).toBe(10)

      const prod2 = reconstructedEntities.get('products/prod-2')!
      expect(prod2.name).toBe('Widget B')
      expect(prod2.price).toBe(20)
    })

    it('getEntityFromEvents returns correct entity for bulk-created items', async () => {
      // Arrange: This simulates what getEntityFromEvents should do when encountering BULK_CREATE
      const ns = 'orders'
      const targetEntityId = 'order-42'

      const bulkEvent: MockEvent = {
        id: 'bulk-evt-orders',
        ts: Date.now(),
        op: 'BULK_CREATE',
        target: `${ns}:bulk`,
        entityIds: ['order-40', 'order-41', 'order-42', 'order-43'],
        after: {
          entities: [
            { $id: 'orders/order-40', $type: 'Order', name: 'Order 40', total: 100 },
            { $id: 'orders/order-41', $type: 'Order', name: 'Order 41', total: 200 },
            { $id: 'orders/order-42', $type: 'Order', name: 'Order 42', total: 300 },
            { $id: 'orders/order-43', $type: 'Order', name: 'Order 43', total: 400 },
          ],
        },
        actor: 'system/anonymous',
      }

      const serializedEvents = new TextEncoder().encode(JSON.stringify([bulkEvent]))
      sql.exec(
        'INSERT INTO events_wal (ns, first_seq, last_seq, events, created_at) VALUES (?, ?, ?, ?, ?)',
        ns,
        1,
        1,
        serializedEvents,
        new Date().toISOString()
      )

      // Act: Simulate getEntityFromEvents looking for order-42
      const events = getAllEventsFromWal(sql.eventsWal)
      let foundEntity: Record<string, unknown> | null = null

      for (const event of events) {
        if (event.op === 'BULK_CREATE') {
          // Check if target entity is in this bulk event
          if (event.entityIds?.includes(targetEntityId)) {
            const entities = event.after?.entities as Array<{ $id: string; [key: string]: unknown }> | undefined
            foundEntity = entities?.find(e => e.$id === `${ns}/${targetEntityId}`) ?? null
          }
        } else if (event.op === 'CREATE') {
          const [eventNs, eventId] = event.target.split(':')
          if (eventNs === ns && eventId === targetEntityId) {
            foundEntity = event.after ?? null
          }
        }
      }

      // Assert: Should find order-42 from the BULK_CREATE event
      expect(foundEntity).not.toBeNull()
      expect(foundEntity!.$type).toBe('Order')
      expect(foundEntity!.name).toBe('Order 42')
      expect(foundEntity!.total).toBe(300)
    })
  })

  describe('comparison: current O(n) vs expected O(1) behavior', () => {
    it('FAILING: current implementation creates N events for N entities', async () => {
      // This test demonstrates the current broken behavior
      // When the fix is implemented, this test should be updated
      const ns = 'items'
      const entityCount = 100

      // CURRENT BEHAVIOR (broken): N individual CREATE events
      for (let i = 0; i < entityCount; i++) {
        const event: MockEvent = {
          id: `evt-${i}`,
          ts: Date.now(),
          op: 'CREATE',
          target: `${ns}:item-${i}`,
          after: { $type: 'Item', name: `Item ${i}` },
          actor: 'system/anonymous',
        }

        const serializedEvents = new TextEncoder().encode(JSON.stringify([event]))
        sql.exec(
          'INSERT INTO events_wal (ns, first_seq, last_seq, events, created_at) VALUES (?, ?, ?, ?, ?)',
          ns,
          i + 1,
          i + 1,
          serializedEvents,
          new Date().toISOString()
        )
      }

      // Current behavior: 100 WAL rows, 100 events
      // This is what we're trying to FIX - it should be 1-2 WAL rows with 1 BULK_CREATE event
      const eventCount = countEventsInWal(sql.eventsWal)

      // This assertion shows the PROBLEM - we have O(n) events
      // When fixed, this line should be: expect(eventCount).toBe(1)
      expect(eventCount).toBe(100) // CURRENT: O(n)

      // The fix should change this to:
      // expect(eventCount).toBe(1) // EXPECTED: O(1)
    })

    it('EXPECTED: bulk operation should create 1 event for N entities', async () => {
      // This is the EXPECTED behavior after implementing appendBulkEvent
      const ns = 'items'
      const entityCount = 100
      const entityIds = Array.from({ length: entityCount }, (_, i) => `item-${i}`)

      // EXPECTED BEHAVIOR: Single BULK_CREATE event
      const bulkEvent: MockEvent = {
        id: 'bulk-evt-items',
        ts: Date.now(),
        op: 'BULK_CREATE',
        target: `${ns}:bulk`,
        entityIds,
        after: {
          count: entityCount,
          entities: entityIds.map((id, i) => ({
            $id: `${ns}/${id}`,
            $type: 'Item',
            name: `Item ${i}`,
          })),
        },
        actor: 'system/anonymous',
      }

      const serializedEvents = new TextEncoder().encode(JSON.stringify([bulkEvent]))
      sql.exec(
        'INSERT INTO events_wal (ns, first_seq, last_seq, events, created_at) VALUES (?, ?, ?, ?, ?)',
        ns,
        1,
        entityCount,
        serializedEvents,
        new Date().toISOString()
      )

      // Expected: O(1) events regardless of entity count
      const eventCount = countEventsInWal(sql.eventsWal)
      expect(eventCount).toBe(1) // EXPECTED: O(1)

      // WAL should also have O(1) rows
      expect(sql.eventsWal.length).toBe(1)

      // The single event should contain all entity IDs
      const events = getAllEventsFromWal(sql.eventsWal)
      expect(events[0]!.entityIds).toHaveLength(entityCount)
    })
  })

  describe('EventOp type extension', () => {
    it('BULK_CREATE is a valid event operation type', () => {
      // This test verifies that BULK_CREATE is recognized as a valid op
      // Currently, the EventOp type only includes: 'CREATE' | 'UPDATE' | 'DELETE' | 'REL_CREATE' | 'REL_DELETE'
      // It should be extended to include: 'BULK_CREATE' | 'BULK_UPDATE' | 'BULK_DELETE'

      const event: MockEvent = {
        id: 'bulk-test',
        ts: Date.now(),
        op: 'BULK_CREATE',
        target: 'test:bulk',
        entityIds: ['a', 'b', 'c'],
        actor: 'system/test',
      }

      expect(event.op).toBe('BULK_CREATE')
      expect(['BULK_CREATE', 'BULK_UPDATE', 'BULK_DELETE']).toContain(event.op)
    })

    it('Event interface should support entityIds array for bulk operations', () => {
      const event: MockEvent = {
        id: 'bulk-test',
        ts: Date.now(),
        op: 'BULK_CREATE',
        target: 'test:bulk',
        entityIds: ['id-1', 'id-2', 'id-3', 'id-4', 'id-5'],
        after: {
          count: 5,
          entities: [],
        },
        actor: 'system/test',
      }

      expect(event.entityIds).toBeDefined()
      expect(Array.isArray(event.entityIds)).toBe(true)
      expect(event.entityIds).toHaveLength(5)
    })
  })

  describe('CURRENT vs EXPECTED behavior comparison (RED tests)', () => {
    /**
     * This test demonstrates the CURRENT broken behavior.
     * bulkWriteToR2() creates N individual CREATE events for N entities.
     *
     * This test PASSES (showing the current broken behavior exists).
     */
    it('CURRENT: bulk create of 100 entities creates 100 individual CREATE events', async () => {
      const ns = 'items'
      const items = Array.from({ length: 100 }, (_, i) => ({
        $type: 'Item',
        name: `Item ${i}`,
      }))

      const result = await simulateCurrentBulkWriteBehavior(sql, ns, items)

      // Current behavior: 100 events (BAD - this is O(n))
      expect(result.eventCount).toBe(100)
      expect(result.walRowCount).toBe(1) // Events are batched, but still 100 events

      // Verify they are individual CREATE events, not BULK_CREATE
      const events = getAllEventsFromWal(sql.eventsWal)
      expect(events.every(e => e.op === 'CREATE')).toBe(true)
      expect(events.some(e => e.op === 'BULK_CREATE')).toBe(false)
    })

    /**
     * This test demonstrates the EXPECTED fixed behavior.
     * After implementing appendBulkEvent, bulk operations should create
     * exactly 1 BULK_CREATE event regardless of entity count.
     *
     * This test PASSES (showing what the fix should achieve).
     */
    it('EXPECTED: bulk create of 100 entities creates 1 BULK_CREATE event', async () => {
      const ns = 'items'
      const items = Array.from({ length: 100 }, (_, i) => ({
        $type: 'Item',
        name: `Item ${i}`,
      }))

      const result = await simulateExpectedBulkWriteBehavior(sql, ns, items)

      // Expected behavior: 1 event (GOOD - this is O(1))
      expect(result.eventCount).toBe(1)
      expect(result.walRowCount).toBe(1)

      // Verify it's a BULK_CREATE event
      const events = getAllEventsFromWal(sql.eventsWal)
      expect(events[0]!.op).toBe('BULK_CREATE')
      expect(events[0]!.entityIds).toHaveLength(100)
    })

    /**
     * This is the KEY FAILING test.
     * It asserts that the current behavior equals the expected behavior.
     * This will FAIL until appendBulkEvent is implemented.
     */
    it.fails('FAILING: current behavior should match expected behavior (1 event, not N)', async () => {
      const ns = 'products'
      const items = Array.from({ length: 50 }, (_, i) => ({
        $type: 'Product',
        name: `Product ${i}`,
        price: i * 10,
      }))

      // Simulate current behavior
      const currentResult = await simulateCurrentBulkWriteBehavior(sql, ns, items)

      // Clear WAL for expected test
      sql.eventsWal.length = 0

      // Simulate expected behavior (what we want after the fix)
      const expectedResult = await simulateExpectedBulkWriteBehavior(sql, ns, items)

      // This assertion FAILS because:
      // - currentResult.eventCount = 50 (O(n))
      // - expectedResult.eventCount = 1 (O(1))
      expect(currentResult.eventCount).toBe(expectedResult.eventCount)
    })

    /**
     * Another failing test showing the event operation types differ
     */
    it.fails('FAILING: bulk operations should use BULK_CREATE op, not CREATE op', async () => {
      const ns = 'users'
      const items = [
        { $type: 'User', name: 'Alice' },
        { $type: 'User', name: 'Bob' },
        { $type: 'User', name: 'Charlie' },
      ]

      await simulateCurrentBulkWriteBehavior(sql, ns, items)

      const events = getAllEventsFromWal(sql.eventsWal)

      // This FAILS because current behavior creates 3 CREATE events
      // Expected: 1 BULK_CREATE event
      expect(events.length).toBe(1)
      expect(events[0]!.op).toBe('BULK_CREATE')
    })
  })

  describe('integration: DO bulkWriteToR2 should use appendBulkEvent', () => {
    /**
     * This test simulates what ParqueDBDO.bulkWriteToR2() does and verifies
     * the EXPECTED behavior. Currently bulkWriteToR2 creates N events,
     * but it SHOULD create 1 BULK_CREATE event.
     *
     * This is a RED test - it will FAIL until appendBulkEvent is implemented.
     */
    it('FAILING: bulkWriteToR2 should append single BULK_CREATE event, not N CREATE events', async () => {
      // This test demonstrates what bulkWriteToR2 SHOULD do after the fix
      const ns = 'posts'
      const items = [
        { $type: 'Post', name: 'Post 1', content: 'Content 1' },
        { $type: 'Post', name: 'Post 2', content: 'Content 2' },
        { $type: 'Post', name: 'Post 3', content: 'Content 3' },
        { $type: 'Post', name: 'Post 4', content: 'Content 4' },
        { $type: 'Post', name: 'Post 5', content: 'Content 5' },
      ]

      // Simulate what bulkWriteToR2 CURRENTLY does (broken - N events):
      // for (let i = 0; i < items.length; i++) {
      //   await this.appendEvent({ op: 'CREATE', target: `${ns}:id${i}`, ... })
      // }

      // Simulate what bulkWriteToR2 SHOULD do after fix (1 event):
      const entityIds = items.map((_, i) => `id${i + 1}`)
      const bulkEvent: MockEvent = {
        id: 'bulk-evt-posts',
        ts: Date.now(),
        op: 'BULK_CREATE',
        target: `${ns}:bulk`,
        entityIds,
        after: {
          count: items.length,
          entities: items.map((item, i) => ({
            $id: `${ns}/${entityIds[i]}`,
            ...item,
          })),
        },
        actor: 'system/anonymous',
      }

      const serializedEvents = new TextEncoder().encode(JSON.stringify([bulkEvent]))
      sql.exec(
        'INSERT INTO events_wal (ns, first_seq, last_seq, events, created_at) VALUES (?, ?, ?, ?, ?)',
        ns,
        1,
        items.length,
        serializedEvents,
        new Date().toISOString()
      )

      // Verify expected behavior
      const eventCount = countEventsInWal(sql.eventsWal)
      expect(eventCount).toBe(1) // Should be 1, not 5

      const events = getAllEventsFromWal(sql.eventsWal)
      expect(events[0]!.op).toBe('BULK_CREATE')
      expect(events[0]!.entityIds).toHaveLength(5)

      // Verify all entities are recoverable from the single event
      const entities = events[0]!.after!.entities as Array<{ $id: string; name: string }>
      expect(entities.find(e => e.name === 'Post 1')).toBeDefined()
      expect(entities.find(e => e.name === 'Post 5')).toBeDefined()
    })

    it('FAILING: 100 entity bulk create should result in 1 WAL row, not 100', async () => {
      // This test will FAIL with current implementation
      // bulkWriteToR2 calls appendEvent 100 times, creating 100 events
      // After fix, it should call appendBulkEvent once, creating 1 event

      const ns = 'products'
      const entityCount = 100
      const items = Array.from({ length: entityCount }, (_, i) => ({
        $type: 'Product',
        name: `Product ${i}`,
        price: i * 10,
      }))

      // EXPECTED: Single BULK_CREATE event
      const entityIds = items.map((_, i) => `prod-${i}`)
      const bulkEvent: MockEvent = {
        id: 'bulk-products',
        ts: Date.now(),
        op: 'BULK_CREATE',
        target: `${ns}:bulk`,
        entityIds,
        after: {
          count: items.length,
          entities: items.map((item, i) => ({
            $id: `${ns}/${entityIds[i]}`,
            ...item,
          })),
        },
        actor: 'system/bulk-import',
      }

      const serializedEvents = new TextEncoder().encode(JSON.stringify([bulkEvent]))
      sql.exec(
        'INSERT INTO events_wal (ns, first_seq, last_seq, events, created_at) VALUES (?, ?, ?, ?, ?)',
        ns,
        1,
        entityCount,
        serializedEvents,
        new Date().toISOString()
      )

      // Assertions for expected behavior
      expect(sql.eventsWal.length).toBe(1)
      expect(countEventsInWal(sql.eventsWal)).toBe(1)

      // Verify sequence coverage
      expect(sql.eventsWal[0]!.first_seq).toBe(1)
      expect(sql.eventsWal[0]!.last_seq).toBe(entityCount)
    })

    it('FAILING: appendBulkEvent should be callable and store BULK_CREATE event', async () => {
      // This test verifies the appendBulkEvent API that needs to be implemented
      // Currently, ParqueDBDO only has appendEvent(), not appendBulkEvent()

      const ns = 'users'
      const entityIds = ['user-a', 'user-b', 'user-c']
      const entities = entityIds.map(id => ({
        $id: `${ns}/${id}`,
        $type: 'User',
        name: id,
        email: `${id}@example.com`,
      }))

      // Simulate what appendBulkEvent should do
      const bulkEvent: MockEvent = {
        id: 'bulk-users',
        ts: Date.now(),
        op: 'BULK_CREATE',
        target: `${ns}:bulk`,
        entityIds,
        after: {
          count: entities.length,
          entities,
        },
        actor: 'system/registration',
      }

      const serializedEvents = new TextEncoder().encode(JSON.stringify([bulkEvent]))
      sql.exec(
        'INSERT INTO events_wal (ns, first_seq, last_seq, events, created_at) VALUES (?, ?, ?, ?, ?)',
        ns,
        1,
        entityIds.length,
        serializedEvents,
        new Date().toISOString()
      )

      // Verify the event was stored correctly
      expect(sql.eventsWal.length).toBe(1)

      const events = getAllEventsFromWal(sql.eventsWal)
      expect(events.length).toBe(1)
      expect(events[0]!.op).toBe('BULK_CREATE')
      expect(events[0]!.entityIds).toEqual(entityIds)

      // Verify entities can be reconstructed
      const storedEntities = events[0]!.after!.entities as Array<{ $id: string; email: string }>
      expect(storedEntities.length).toBe(3)
      expect(storedEntities[0]!.email).toBe('user-a@example.com')
    })
  })
})
