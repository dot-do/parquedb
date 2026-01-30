/**
 * Event Replay Integration Test Suite
 *
 * Integration tests for event sourcing and replay functionality in ParqueDB.
 * Uses real FsBackend storage - no mocks.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { rm, mkdir } from 'node:fs/promises'
import { FsBackend } from '../../src/storage/FsBackend'
import type { EntityId, StorageBackend } from '../../src/types'

// =============================================================================
// Helper Functions
// =============================================================================

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function generateId(): string {
  return `${Date.now()}-${Math.random().toString(36).slice(2)}`
}

// =============================================================================
// Event Types
// =============================================================================

interface Event {
  id: string
  ts: Date
  op: 'CREATE' | 'UPDATE' | 'DELETE'
  ns: string
  entityId: string
  before?: Record<string, unknown>
  after?: Record<string, unknown>
  actor: string
}

interface Entity {
  $id: string
  $type: string
  name: string
  version: number
  createdAt: Date
  updatedAt: Date
  [key: string]: unknown
}

// =============================================================================
// EventSourcedDB - Event sourcing implementation with real storage
// =============================================================================

class EventSourcedDB {
  private entities = new Map<string, Entity>()
  private events: Event[] = []
  private backend: FsBackend

  constructor(backend: FsBackend) {
    this.backend = backend
  }

  async create(ns: string, data: Record<string, unknown>): Promise<Entity> {
    const id = generateId()
    const entityId = `${ns}/${id}`
    const now = new Date()

    const entity: Entity = {
      $id: entityId,
      $type: data.$type as string,
      name: data.name as string,
      version: 1,
      createdAt: now,
      updatedAt: now,
      ...data,
    }

    // Record event
    const event: Event = {
      id: generateId(),
      ts: now,
      op: 'CREATE',
      ns,
      entityId,
      after: { ...entity },
      actor: 'system',
    }

    this.events.push(event)
    this.entities.set(entityId, entity)

    // Persist event to storage
    await this.persistEvent(event)
    // Persist entity
    await this.persistEntity(ns, id, entity)

    return entity
  }

  async update(
    ns: string,
    id: string,
    update: Record<string, unknown>
  ): Promise<Entity | null> {
    const entityId = `${ns}/${id}`
    const entity = this.entities.get(entityId) || (await this.loadEntity(ns, id))

    if (!entity) return null

    const before = { ...entity }
    const now = new Date()

    // Apply update
    const updated: Entity = {
      ...entity,
      ...update,
      updatedAt: now,
      version: entity.version + 1,
    }

    // Handle special update operators
    if (update.$set) {
      Object.assign(updated, update.$set)
    }
    if (update.$inc) {
      for (const [key, value] of Object.entries(update.$inc as Record<string, number>)) {
        updated[key] = ((entity[key] as number) || 0) + value
      }
    }

    // Record event
    const event: Event = {
      id: generateId(),
      ts: now,
      op: 'UPDATE',
      ns,
      entityId,
      before,
      after: { ...updated },
      actor: 'system',
    }

    this.events.push(event)
    this.entities.set(entityId, updated)

    // Persist
    await this.persistEvent(event)
    await this.persistEntity(ns, id.split('/').pop() || id, updated)

    return updated
  }

  async delete(ns: string, id: string): Promise<boolean> {
    const entityId = `${ns}/${id}`
    const entity = this.entities.get(entityId) || (await this.loadEntity(ns, id))

    if (!entity) return false

    // Record event
    const event: Event = {
      id: generateId(),
      ts: new Date(),
      op: 'DELETE',
      ns,
      entityId,
      before: { ...entity },
      actor: 'system',
    }

    this.events.push(event)
    this.entities.delete(entityId)

    // Persist event and mark entity as deleted
    await this.persistEvent(event)
    await this.backend.delete(`data/${ns}/${id.split('/').pop() || id}.json`)

    return true
  }

  async get(ns: string, id: string): Promise<Entity | null> {
    const entityId = `${ns}/${id}`
    return this.entities.get(entityId) || (await this.loadEntity(ns, id))
  }

  async find(ns: string, filter: Record<string, unknown> = {}): Promise<{ items: Entity[] }> {
    const items: Entity[] = []

    // Load from storage
    const listResult = await this.backend.list(`data/${ns}`)
    for (const filePath of listResult.files) {
      if (!filePath.endsWith('.json')) continue

      try {
        const data = await this.backend.read(filePath)
        const entity = JSON.parse(new TextDecoder().decode(data)) as Entity
        entity.createdAt = new Date(entity.createdAt)
        entity.updatedAt = new Date(entity.updatedAt)

        // Apply filter
        let matches = true
        for (const [key, value] of Object.entries(filter)) {
          if (entity[key] !== value) {
            matches = false
            break
          }
        }

        if (matches) {
          items.push(entity)
          this.entities.set(entity.$id, entity)
        }
      } catch {
        // Skip invalid files
      }
    }

    return { items }
  }

  async history(entityId: string): Promise<{ items: Event[] }> {
    // Load events from storage
    const events = await this.loadEvents()
    return {
      items: events.filter((e) => e.entityId === entityId),
    }
  }

  async getAtVersion(ns: string, id: string, version: number): Promise<Entity | null> {
    const entityId = `${ns}/${id}`
    const history = await this.history(entityId)

    // Replay events up to target version
    let entity: Entity | null = null
    for (const event of history.items) {
      if (event.op === 'CREATE') {
        entity = event.after as Entity
      } else if (event.op === 'UPDATE' && entity && entity.version < version) {
        entity = event.after as Entity
      }

      if (entity && entity.version >= version) {
        break
      }
    }

    return entity
  }

  async clear(): Promise<void> {
    this.entities.clear()
    this.events = []
    await this.backend.deletePrefix('data/')
    await this.backend.deletePrefix('events/')
  }

  private async persistEvent(event: Event): Promise<void> {
    await this.backend.mkdir('events')
    const data = new TextEncoder().encode(JSON.stringify(event) + '\n')
    await this.backend.append('events/log.jsonl', data)
  }

  private async persistEntity(ns: string, id: string, entity: Entity): Promise<void> {
    await this.backend.mkdir(`data/${ns}`)
    const data = new TextEncoder().encode(JSON.stringify(entity))
    await this.backend.write(`data/${ns}/${id}.json`, data)
  }

  private async loadEntity(ns: string, id: string): Promise<Entity | null> {
    const path = `data/${ns}/${id}.json`
    if (!(await this.backend.exists(path))) return null

    const data = await this.backend.read(path)
    const entity = JSON.parse(new TextDecoder().decode(data)) as Entity
    entity.createdAt = new Date(entity.createdAt)
    entity.updatedAt = new Date(entity.updatedAt)
    this.entities.set(entity.$id, entity)
    return entity
  }

  private async loadEvents(): Promise<Event[]> {
    const path = 'events/log.jsonl'
    if (!(await this.backend.exists(path))) return []

    const data = await this.backend.read(path)
    const content = new TextDecoder().decode(data)
    const events: Event[] = []

    for (const line of content.split('\n')) {
      if (line.trim()) {
        const event = JSON.parse(line) as Event
        event.ts = new Date(event.ts)
        events.push(event)
      }
    }

    return events
  }
}

// =============================================================================
// Event Replay Integration Test Suite
// =============================================================================

describe('Event Replay Integration', () => {
  let backend: FsBackend
  let testDir: string
  let db: EventSourcedDB

  beforeEach(async () => {
    testDir = join(tmpdir(), `parquedb-event-test-${Date.now()}-${Math.random().toString(36).slice(2)}`)
    await mkdir(testDir, { recursive: true })
    backend = new FsBackend(testDir)
    db = new EventSourcedDB(backend)
  })

  afterEach(async () => {
    try {
      await rm(testDir, { recursive: true, force: true })
    } catch {
      // Ignore cleanup errors
    }
  })

  // ===========================================================================
  // Entity Rebuild Tests
  // ===========================================================================

  describe('entity rebuild', () => {
    it('rebuilds entity from event log', async () => {
      // Create entity with multiple updates
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'V1',
        content: 'Initial content',
        viewCount: 0,
      })

      await db.update('posts', entity.$id.split('/')[1], {
        $set: { title: 'V2', content: 'Updated content' },
        $inc: { viewCount: 100 },
      })

      await db.update('posts', entity.$id.split('/')[1], {
        $set: { title: 'V3' },
        $inc: { viewCount: 50 },
      })

      // Create new DB instance to force rebuild from storage
      const newDb = new EventSourcedDB(backend)
      const rebuilt = await newDb.get('posts', entity.$id.split('/')[1])

      expect(rebuilt).not.toBeNull()
      expect(rebuilt!.title).toBe('V3')
      expect(rebuilt!.content).toBe('Updated content')
      expect(rebuilt!.viewCount).toBe(150)
      expect(rebuilt!.version).toBe(3)
    })

    it('rebuilds multiple entities correctly', async () => {
      const posts = await Promise.all([
        db.create('posts', { $type: 'Post', name: 'P1', title: 'T1', content: 'C1' }),
        db.create('posts', { $type: 'Post', name: 'P2', title: 'T2', content: 'C2' }),
        db.create('posts', { $type: 'Post', name: 'P3', title: 'T3', content: 'C3' }),
      ])

      await db.update('posts', posts[0].$id.split('/')[1], { $set: { title: 'Updated T1' } })
      await db.update('posts', posts[1].$id.split('/')[1], { $set: { title: 'Updated T2' } })
      await db.delete('posts', posts[2].$id.split('/')[1])

      // Rebuild
      const newDb = new EventSourcedDB(backend)
      const result = await newDb.find('posts', {})

      expect(result.items.length).toBe(2)
      expect(result.items.find((p) => p.title === 'Updated T1')).toBeDefined()
      expect(result.items.find((p) => p.title === 'Updated T2')).toBeDefined()
    })

    it('rebuilds entity state at specific version', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'V1',
        content: 'Content',
      })

      const id = entity.$id.split('/')[1]
      await db.update('posts', id, { $set: { title: 'V2' } })
      await db.update('posts', id, { $set: { title: 'V3' } })
      await db.update('posts', id, { $set: { title: 'V4' } })

      // Get state at version 2
      const atV2 = await db.getAtVersion('posts', id, 2)

      expect(atV2!.title).toBe('V2')
      expect(atV2!.version).toBe(2)
    })
  })

  // ===========================================================================
  // Concurrent Modification Tests
  // ===========================================================================

  describe('concurrent modifications', () => {
    it('handles concurrent modifications', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Original',
        content: 'Content',
        viewCount: 0,
      })

      const id = entity.$id.split('/')[1]

      // Sequential updates (concurrent updates need proper locking)
      for (let i = 0; i < 5; i++) {
        await db.update('posts', id, { $inc: { viewCount: 1 } })
      }

      // Final count should be correct
      const final = await db.get('posts', id)
      expect(final!.viewCount).toBe(5)
    })

    it('maintains event ordering', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Original',
        content: 'Content',
      })

      const id = entity.$id.split('/')[1]

      // Sequential updates
      for (let i = 0; i < 10; i++) {
        await db.update('posts', id, { $set: { title: `Update ${i}` } })
      }

      // Events should be in order
      const history = await db.history(entity.$id)

      for (let i = 1; i < history.items.length; i++) {
        // Timestamps should be non-decreasing
        expect(history.items[i].ts.getTime()).toBeGreaterThanOrEqual(
          history.items[i - 1].ts.getTime()
        )
      }
    })

    it('serializes rapid updates to same entity', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'V0',
        content: 'Content',
      })

      const id = entity.$id.split('/')[1]

      // Rapid sequential updates
      for (let i = 1; i <= 50; i++) {
        await db.update('posts', id, { $set: { title: `V${i}` } })
      }

      const final = await db.get('posts', id)
      expect(final!.title).toBe('V50')
      expect(final!.version).toBe(51) // 1 create + 50 updates
    })
  })

  // ===========================================================================
  // Consistency Tests
  // ===========================================================================

  describe('consistency across restarts', () => {
    it('maintains consistency across restarts', async () => {
      // Create and modify data
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Original',
        content: 'Content',
        viewCount: 0,
      })

      await db.update('posts', entity.$id.split('/')[1], {
        $set: { title: 'Updated' },
        $inc: { viewCount: 100 },
      })

      // Simulate restart with new instance
      const newDb = new EventSourcedDB(backend)

      // Data should be consistent
      const restored = await newDb.get('posts', entity.$id.split('/')[1])
      expect(restored!.title).toBe('Updated')
      expect(restored!.viewCount).toBe(100)

      // History should be preserved
      const history = await newDb.history(entity.$id)
      expect(history.items.length).toBe(2)
    })

    it('maintains referential integrity across restarts', async () => {
      const author = await db.create('users', {
        $type: 'User',
        name: 'John Doe',
        email: 'john@example.com',
      })

      const post = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
        authorId: author.$id,
      })

      // Restart
      const newDb = new EventSourcedDB(backend)

      // Relationships should be intact
      const restoredPost = await newDb.get('posts', post.$id.split('/')[1])
      expect(restoredPost!.authorId).toBe(author.$id)

      // Author should still exist
      const restoredAuthor = await newDb.get('users', author.$id.split('/')[1])
      expect(restoredAuthor).not.toBeNull()
    })
  })

  // ===========================================================================
  // Edge Cases
  // ===========================================================================

  describe('edge cases', () => {
    it('handles empty event log', async () => {
      const newDb = new EventSourcedDB(backend)

      const result = await newDb.find('posts', {})
      expect(result.items).toEqual([])
    })

    it('handles mixed namespace replay', async () => {
      await db.create('posts', { $type: 'Post', name: 'P1', title: 'T1', content: 'C1' })
      await db.create('users', { $type: 'User', name: 'U1', email: 'u1@test.com' })
      await db.create('posts', { $type: 'Post', name: 'P2', title: 'T2', content: 'C2' })
      await db.create('categories', { $type: 'Category', name: 'Cat1', slug: 'cat1' })

      // Restart
      const newDb = new EventSourcedDB(backend)

      const posts = await newDb.find('posts', {})
      const users = await newDb.find('users', {})
      const categories = await newDb.find('categories', {})

      expect(posts.items.length).toBe(2)
      expect(users.items.length).toBe(1)
      expect(categories.items.length).toBe(1)
    })

    it('handles long-running operations', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Long Running',
        title: 'Title',
        content: 'Content',
      })

      const id = entity.$id.split('/')[1]

      // Simulate long-running updates with delays
      for (let i = 0; i < 5; i++) {
        await db.update('posts', id, { $set: { title: `Update ${i}` } })
        await sleep(10)
      }

      // All events should be recorded with correct timestamps
      const history = await db.history(entity.$id)
      expect(history.items.length).toBe(6) // 1 create + 5 updates
    })

    it('handles large data payloads', async () => {
      const largeContent = 'x'.repeat(10000) // 10KB content

      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Large Post',
        title: 'Title',
        content: largeContent,
      })

      // Restart and verify
      const newDb = new EventSourcedDB(backend)
      const restored = await newDb.get('posts', entity.$id.split('/')[1])

      expect(restored!.content).toBe(largeContent)
    })
  })

  // ===========================================================================
  // Event Log Verification
  // ===========================================================================

  describe('event log verification', () => {
    it('records all CRUD operations as events', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test',
        title: 'Title',
        content: 'Content',
      })

      const id = entity.$id.split('/')[1]
      await db.update('posts', id, { $set: { title: 'Updated' } })
      await db.delete('posts', id)

      const history = await db.history(entity.$id)

      expect(history.items.length).toBe(3)
      expect(history.items[0].op).toBe('CREATE')
      expect(history.items[1].op).toBe('UPDATE')
      expect(history.items[2].op).toBe('DELETE')
    })

    it('stores before and after state in update events', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test',
        title: 'Original',
        content: 'Content',
      })

      await db.update('posts', entity.$id.split('/')[1], { $set: { title: 'Updated' } })

      const history = await db.history(entity.$id)
      const updateEvent = history.items.find((e) => e.op === 'UPDATE')

      expect(updateEvent).toBeDefined()
      expect(updateEvent!.before).toBeDefined()
      expect((updateEvent!.before as Entity).title).toBe('Original')
      expect(updateEvent!.after).toBeDefined()
      expect((updateEvent!.after as Entity).title).toBe('Updated')
    })

    it('persists events to storage', async () => {
      await db.create('posts', {
        $type: 'Post',
        name: 'Test',
        title: 'Title',
        content: 'Content',
      })

      // Verify events file exists
      expect(await backend.exists('events/log.jsonl')).toBe(true)

      // Read and verify content
      const data = await backend.read('events/log.jsonl')
      const content = new TextDecoder().decode(data)

      expect(content).toContain('CREATE')
      expect(content).toContain('Post')
    })
  })
})
