/**
 * Tests for ParqueDB + Materialized Views Integration
 *
 * These tests verify that the ParqueDB write path properly emits events
 * to the MV streaming engine when configured with onEvent callback.
 */

import { describe, it, expect, afterEach, vi } from 'vitest'
import type { Event } from '@/types/entity'
import { ParqueDB } from '@/ParqueDB'
import { MemoryBackend } from '@/storage/MemoryBackend'
import { attachMVIntegration } from '@/materialized-views/write-path-integration'

// =============================================================================
// ParqueDB onEvent Config Tests
// =============================================================================

describe('ParqueDB onEvent integration', () => {
  let db: ParqueDB | null = null

  afterEach(() => {
    if (db) {
      db.dispose()
      db = null
    }
  })

  it('calls onEvent callback when events are recorded', async () => {
    const receivedEvents: Event[] = []

    db = new ParqueDB({
      storage: new MemoryBackend(),
      onEvent: (event) => {
        receivedEvents.push(event)
      },
    })

    // Create an entity - should trigger onEvent
    await db.create('orders', {
      $type: 'Order',
      name: 'Test Order',
      total: 100,
    })

    // The event should have been emitted (may be async, give it a tick)
    await vi.waitFor(() => {
      expect(receivedEvents.length).toBeGreaterThanOrEqual(1)
    })

    const createEvent = receivedEvents.find(e => e.op === 'CREATE')
    expect(createEvent).toBeDefined()
    expect(createEvent!.target).toContain('orders:')
  })

  it('setEventCallback can be used to set callback after construction', async () => {
    const receivedEvents: Event[] = []

    db = new ParqueDB({
      storage: new MemoryBackend(),
    })

    // Initially no callback
    expect(db.getEventCallback()).toBeNull()

    // Set callback
    db.setEventCallback((event) => {
      receivedEvents.push(event)
    })

    expect(db.getEventCallback()).not.toBeNull()

    // Create an entity
    await db.create('products', {
      $type: 'Product',
      name: 'Test Product',
      price: 50,
    })

    await vi.waitFor(() => {
      expect(receivedEvents.length).toBeGreaterThanOrEqual(1)
    })

    // Clear callback
    db.setEventCallback(null)
    expect(db.getEventCallback()).toBeNull()
  })

  it('end-to-end MV integration with ParqueDB', async () => {
    db = new ParqueDB({
      storage: new MemoryBackend(),
    })

    const processedEvents: Event[] = []
    const { integration, start, stop, detach } = attachMVIntegration(db)

    integration.engine.registerMV({
      name: 'OrderAnalytics',
      sourceNamespaces: ['orders'],
      async process(events) {
        processedEvents.push(...events)
      },
    })

    await start()

    // Create orders - should trigger MV updates
    await db.create('orders', { $type: 'Order', name: 'Order 1', total: 100 })
    await db.create('orders', { $type: 'Order', name: 'Order 2', total: 200 })

    // Wait for async processing
    await integration.emitter.flush()
    await integration.engine.flush()

    expect(processedEvents.length).toBe(2)
    expect(processedEvents.every(e => e.op === 'CREATE')).toBe(true)
    expect(processedEvents.every(e => e.target.startsWith('orders:'))).toBe(true)

    await stop()
    detach()
  })

  it('MV integration handles UPDATE events', async () => {
    db = new ParqueDB({
      storage: new MemoryBackend(),
    })

    const processedEvents: Event[] = []
    const { integration, start, stop, detach } = attachMVIntegration(db)

    integration.engine.registerMV({
      name: 'OrderTracker',
      sourceNamespaces: ['orders'],
      async process(events) {
        processedEvents.push(...events)
      },
    })

    await start()

    // Create and update
    const order = await db.create('orders', { $type: 'Order', name: 'Order 1', total: 100 })
    await db.update('orders', order.$id as string, { $set: { total: 150 } })

    await integration.emitter.flush()
    await integration.engine.flush()

    expect(processedEvents.length).toBe(2)
    expect(processedEvents[0].op).toBe('CREATE')
    expect(processedEvents[1].op).toBe('UPDATE')

    await stop()
    detach()
  })

  it('MV integration handles DELETE events', async () => {
    db = new ParqueDB({
      storage: new MemoryBackend(),
    })

    const processedEvents: Event[] = []
    const { integration, start, stop, detach } = attachMVIntegration(db)

    integration.engine.registerMV({
      name: 'OrderTracker',
      sourceNamespaces: ['orders'],
      async process(events) {
        processedEvents.push(...events)
      },
    })

    await start()

    // Create and delete
    const order = await db.create('orders', { $type: 'Order', name: 'Order 1', total: 100 })
    await db.delete('orders', order.$id as string)

    await integration.emitter.flush()
    await integration.engine.flush()

    expect(processedEvents.length).toBe(2)
    expect(processedEvents[0].op).toBe('CREATE')
    expect(processedEvents[1].op).toBe('DELETE')

    await stop()
    detach()
  })
})
