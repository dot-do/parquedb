/**
 * E2E Tests for Materialized Views
 *
 * Tests MV integration using the public ParqueDB API:
 * - ParqueDB + attachMVIntegration() for event wiring
 * - defineView() for MV definitions
 * - Engine registerMV() for custom handlers
 *
 * These tests do NOT depend on internal streaming implementation details.
 * They use the public API surface only.
 *
 * Run with: pnpm test tests/e2e/materialized-views.test.ts
 */

import { describe, test, expect, afterEach, vi } from 'vitest'
import { ParqueDB } from '../../src/ParqueDB'
import { MemoryBackend } from '../../src/storage/MemoryBackend'
import {
  attachMVIntegration,
  type AttachMVResult,
} from '../../src/materialized-views/write-path-integration'
import { defineView } from '../../src/materialized-views/define'
import type { Event } from '../../src/types/entity'
import type { MVHandler } from '../../src/materialized-views/streaming'

// =============================================================================
// Test Helpers
// =============================================================================

function createDBWithMV(engineOptions?: { batchSize?: number; batchTimeoutMs?: number }) {
  const db = new ParqueDB({ storage: new MemoryBackend() })
  const mvResult = attachMVIntegration(db, {
    emitterOptions: { synchronous: true },
    engineOptions: {
      batchSize: engineOptions?.batchSize ?? 100,
      batchTimeoutMs: engineOptions?.batchTimeoutMs ?? 5000,
    },
  })
  return {
    db,
    mv: mvResult,
    async cleanup() {
      await mvResult.stop()
      mvResult.detach()
      db.dispose()
    },
  }
}

async function flushMV(mv: AttachMVResult) {
  await mv.integration.emitter.flush()
  await mv.integration.engine.flush()
}

function createTestHandler(
  name: string,
  sourceNamespaces: string[],
  options?: {
    onProcess?: (events: Event[]) => void
  }
): MVHandler & { processedBatches: Event[][]; allEvents: Event[] } {
  const processedBatches: Event[][] = []
  const allEvents: Event[] = []

  return {
    name,
    sourceNamespaces,
    processedBatches,
    allEvents,
    async process(events: Event[]): Promise<void> {
      processedBatches.push([...events])
      allEvents.push(...events)
      options?.onProcess?.(events)
    },
  }
}

// =============================================================================
// Test Suite
// =============================================================================

describe('Materialized View E2E', () => {
  describe('defineView() -> create -> MV propagation', () => {
    test('creates an MV definition and verifies it has correct shape', () => {
      const OrderAnalytics = defineView({
        $from: 'Order',
        $expand: ['customer'],
        $compute: {
          orderCount: { $count: '*' },
          revenue: { $sum: 'total' },
        },
      })

      expect(OrderAnalytics).toBeDefined()
      expect(OrderAnalytics.$from).toBe('Order')
      expect(OrderAnalytics.$expand).toEqual(['customer'])
      expect(OrderAnalytics.$compute).toBeDefined()
    })

    test('defineView() with aggregation and scheduled refresh', () => {
      const DailySales = defineView({
        $from: 'Order',
        $groupBy: [{ date: '$createdAt' }, 'status'],
        $compute: {
          orderCount: { $count: '*' },
          revenue: { $sum: 'total' },
        },
        $refresh: { mode: 'scheduled', schedule: '0 * * * *' },
      })

      expect(DailySales.$from).toBe('Order')
      expect(DailySales.$groupBy).toHaveLength(2)
      expect(DailySales.$refresh?.mode).toBe('scheduled')
    })

    test('defineView() with filter for derived views', () => {
      const WorkerErrors = defineView({
        $from: 'TailEvents',
        $filter: { outcome: { $ne: 'ok' } },
      })

      expect(WorkerErrors.$from).toBe('TailEvents')
      expect(WorkerErrors.$filter).toEqual({ outcome: { $ne: 'ok' } })
    })

    test('defineView() rejects invalid definitions', () => {
      expect(() =>
        defineView({
          $from: '',
        })
      ).toThrow()
    })
  })

  describe('Insert -> MV propagation via ParqueDB', () => {
    let db: ParqueDB
    let mv: AttachMVResult
    let cleanup: () => Promise<void>

    afterEach(async () => {
      if (cleanup) await cleanup()
    })

    test('inserts through ParqueDB trigger MV handler processing', async () => {
      ;({ db, mv, cleanup } = createDBWithMV())
      const handler = createTestHandler('OrderTracker', ['orders'])
      mv.integration.engine.registerMV(handler)
      await mv.start()

      await db.create('orders', {
        $type: 'Order',
        name: 'Test Order 1',
        total: 100,
      })
      await flushMV(mv)

      expect(handler.allEvents.length).toBeGreaterThanOrEqual(1)
      const createEvents = handler.allEvents.filter((e) => e.op === 'CREATE')
      expect(createEvents.length).toBeGreaterThanOrEqual(1)
    })

    test('handles multiple inserts with data integrity', async () => {
      ;({ db, mv, cleanup } = createDBWithMV())
      const handler = createTestHandler('OrderTracker', ['orders'])
      mv.integration.engine.registerMV(handler)
      await mv.start()

      const count = 10
      for (let i = 0; i < count; i++) {
        await db.create('orders', {
          $type: 'Order',
          name: `Order ${i}`,
          total: 100 + i * 10,
        })
      }
      await flushMV(mv)

      const createEvents = handler.allEvents.filter((e) => e.op === 'CREATE')
      expect(createEvents.length).toBe(count)
    })
  })

  describe('Update -> MV propagation via ParqueDB', () => {
    let db: ParqueDB
    let mv: AttachMVResult
    let cleanup: () => Promise<void>

    afterEach(async () => {
      if (cleanup) await cleanup()
    })

    test('updates through ParqueDB trigger MV handler with UPDATE events', async () => {
      ;({ db, mv, cleanup } = createDBWithMV())
      const handler = createTestHandler('OrderTracker', ['orders'])
      mv.integration.engine.registerMV(handler)
      await mv.start()

      const entity = await db.create('orders', {
        $type: 'Order',
        name: 'Test Order',
        total: 100,
      })
      await flushMV(mv)

      await db.update('orders', entity.$id, { $set: { total: 200 } })
      await flushMV(mv)

      const updateEvents = handler.allEvents.filter((e) => e.op === 'UPDATE')
      expect(updateEvents.length).toBeGreaterThanOrEqual(1)
    })

    test('multiple updates propagate correctly', async () => {
      ;({ db, mv, cleanup } = createDBWithMV())
      const handler = createTestHandler('OrderTracker', ['orders'])
      mv.integration.engine.registerMV(handler)
      await mv.start()

      const entity = await db.create('orders', {
        $type: 'Order',
        name: 'Test Order',
        total: 100,
        status: 'pending',
      })
      await flushMV(mv)

      await db.update('orders', entity.$id, { $set: { total: 150 } })
      await db.update('orders', entity.$id, { $set: { status: 'completed' } })
      await db.update('orders', entity.$id, { $inc: { total: 50 } })
      await flushMV(mv)

      const updateEvents = handler.allEvents.filter((e) => e.op === 'UPDATE')
      expect(updateEvents.length).toBe(3)
    })
  })

  describe('Delete -> MV propagation via ParqueDB', () => {
    let db: ParqueDB
    let mv: AttachMVResult
    let cleanup: () => Promise<void>

    afterEach(async () => {
      if (cleanup) await cleanup()
    })

    test('deletes through ParqueDB trigger MV handler with DELETE events', async () => {
      ;({ db, mv, cleanup } = createDBWithMV())
      const handler = createTestHandler('OrderTracker', ['orders'])
      mv.integration.engine.registerMV(handler)
      await mv.start()

      const entity = await db.create('orders', {
        $type: 'Order',
        name: 'To Delete',
        total: 50,
      })
      await flushMV(mv)

      await db.delete('orders', entity.$id)
      await flushMV(mv)

      const deleteEvents = handler.allEvents.filter((e) => e.op === 'DELETE')
      expect(deleteEvents.length).toBeGreaterThanOrEqual(1)
    })
  })

  describe('Multiple MVs subscribing to same source', () => {
    let db: ParqueDB
    let mv: AttachMVResult
    let cleanup: () => Promise<void>

    afterEach(async () => {
      if (cleanup) await cleanup()
    })

    test('all registered MVs receive events from shared source namespace', async () => {
      ;({ db, mv, cleanup } = createDBWithMV())

      const handler1 = createTestHandler('OrderTotals', ['orders'])
      const handler2 = createTestHandler('OrdersByCustomer', ['orders'])
      const handler3 = createTestHandler('OrderStatus', ['orders'])

      mv.integration.engine.registerMV(handler1)
      mv.integration.engine.registerMV(handler2)
      mv.integration.engine.registerMV(handler3)
      await mv.start()

      for (let i = 0; i < 5; i++) {
        await db.create('orders', {
          $type: 'Order',
          name: `Order ${i}`,
          total: 100 + i,
        })
      }
      await flushMV(mv)

      expect(handler1.allEvents.length).toBe(5)
      expect(handler2.allEvents.length).toBe(5)
      expect(handler3.allEvents.length).toBe(5)
    })

    test('MVs subscribed to different namespaces receive only relevant events', async () => {
      ;({ db, mv, cleanup } = createDBWithMV())

      const orderHandler = createTestHandler('OrderTracker', ['orders'])
      const productHandler = createTestHandler('ProductTracker', ['products'])

      mv.integration.engine.registerMV(orderHandler)
      mv.integration.engine.registerMV(productHandler)
      await mv.start()

      await db.create('orders', { $type: 'Order', name: 'Order 1', total: 100 })
      await db.create('products', { $type: 'Product', name: 'Product 1', price: 50 })
      await db.create('orders', { $type: 'Order', name: 'Order 2', total: 200 })
      await flushMV(mv)

      const orderCreates = orderHandler.allEvents.filter((e) => e.op === 'CREATE')
      const productCreates = productHandler.allEvents.filter((e) => e.op === 'CREATE')

      expect(orderCreates.length).toBe(2)
      expect(productCreates.length).toBe(1)
    })
  })

  describe('Full cycle: insert -> MV update -> verify aggregation', () => {
    let db: ParqueDB
    let mv: AttachMVResult
    let cleanup: () => Promise<void>

    afterEach(async () => {
      if (cleanup) await cleanup()
    })

    test('MV handler computes aggregates from ParqueDB mutations', async () => {
      ;({ db, mv, cleanup } = createDBWithMV())

      const customerTotals = new Map<string, number>()

      const handler = createTestHandler('CustomerSpending', ['orders'], {
        onProcess: (events) => {
          for (const event of events) {
            if (event.op === 'CREATE' && event.after) {
              const customerId = (event.after as Record<string, unknown>).customerId as string
              const total = (event.after as Record<string, unknown>).total as number
              if (customerId && total) {
                const current = customerTotals.get(customerId) ?? 0
                customerTotals.set(customerId, current + total)
              }
            }
          }
        },
      })

      mv.integration.engine.registerMV(handler)
      await mv.start()

      for (let i = 0; i < 20; i++) {
        await db.create('orders', {
          $type: 'Order',
          name: `Order ${i}`,
          customerId: `cust-${i % 5}`,
          total: 100,
        })
      }
      await flushMV(mv)

      expect(customerTotals.size).toBe(5)
      const totalSpent = Array.from(customerTotals.values()).reduce((a, b) => a + b, 0)
      expect(totalSpent).toBe(2000)
    })

    test('MV handler processes mixed CREATE/UPDATE/DELETE operations', async () => {
      ;({ db, mv, cleanup } = createDBWithMV())

      const opCounts = { CREATE: 0, UPDATE: 0, DELETE: 0 }

      const handler = createTestHandler('OpCounter', ['orders'], {
        onProcess: (events) => {
          for (const event of events) {
            if (event.op in opCounts) {
              opCounts[event.op as keyof typeof opCounts]++
            }
          }
        },
      })

      mv.integration.engine.registerMV(handler)
      await mv.start()

      const e1 = await db.create('orders', { $type: 'Order', name: 'Order 1', total: 100 })
      const e2 = await db.create('orders', { $type: 'Order', name: 'Order 2', total: 200 })
      await flushMV(mv)

      await db.update('orders', e1.$id, { $set: { total: 150 } })
      await flushMV(mv)

      await db.delete('orders', e2.$id)
      await flushMV(mv)

      expect(opCounts.CREATE).toBe(2)
      expect(opCounts.UPDATE).toBe(1)
      expect(opCounts.DELETE).toBe(1)
    })
  })
})
