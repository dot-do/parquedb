/**
 * SegmentWriter Test Suite
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  SegmentWriter,
  createSegmentWriter,
  createSegmentFlushHandler,
  createR2Adapter,
} from '../../../src/events/segment'
import type { SegmentStorage } from '../../../src/events/segment'
import type { Event } from '../../../src/types'
import type { EventBatch } from '../../../src/events/types'

// =============================================================================
// Mock Storage Implementation
// =============================================================================

class MockStorage implements SegmentStorage {
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

  // Test helpers
  getFileCount(): number {
    return this.files.size
  }

  clear(): void {
    this.files.clear()
  }
}

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

function createTestBatch(count: number = 3, minTs: number = 1000, maxTs: number = 3000): EventBatch {
  const events: Event[] = []
  for (let i = 0; i < count; i++) {
    const ts = minTs + Math.floor(i * ((maxTs - minTs) / Math.max(count - 1, 1)))
    events.push(createTestEvent({ ts }))
  }
  return {
    events,
    minTs,
    maxTs,
    count,
    sizeBytes: 500,
  }
}

// =============================================================================
// Tests
// =============================================================================

describe('SegmentWriter', () => {
  let storage: MockStorage
  let writer: SegmentWriter

  beforeEach(() => {
    storage = new MockStorage()
    writer = new SegmentWriter(storage, { dataset: 'test-app' })
  })

  describe('write operations', () => {
    it('writes a segment and returns metadata', async () => {
      const batch = createTestBatch()
      const segment = await writer.writeSegment(batch)

      expect(segment.seq).toBe(1)
      expect(segment.path).toBe('test-app/events/seg-0001.parquet')
      expect(segment.minTs).toBe(batch.minTs)
      expect(segment.maxTs).toBe(batch.maxTs)
      expect(segment.count).toBe(batch.count)
      expect(segment.sizeBytes).toBeGreaterThan(0)
      expect(segment.createdAt).toBeGreaterThan(0)
    })

    it('increments sequence numbers', async () => {
      const seg1 = await writer.writeSegment(createTestBatch())
      const seg2 = await writer.writeSegment(createTestBatch())
      const seg3 = await writer.writeSegment(createTestBatch())

      expect(seg1.seq).toBe(1)
      expect(seg2.seq).toBe(2)
      expect(seg3.seq).toBe(3)

      expect(seg1.path).toContain('seg-0001')
      expect(seg2.path).toContain('seg-0002')
      expect(seg3.path).toContain('seg-0003')
    })

    it('writes multiple batches as one segment', async () => {
      const batch1 = createTestBatch(2, 1000, 2000)
      const batch2 = createTestBatch(3, 2500, 3500)

      const segment = await writer.writeSegmentFromBatches([batch1, batch2])

      expect(segment.count).toBe(5)
      expect(segment.minTs).toBe(1000)
      expect(segment.maxTs).toBe(3500)
    })
  })

  describe('read operations', () => {
    it('reads a segment back', async () => {
      const originalBatch = createTestBatch(3, 1000, 3000)
      const segment = await writer.writeSegment(originalBatch)

      const readBatch = await writer.readSegment(segment)

      expect(readBatch).not.toBeNull()
      expect(readBatch!.events).toHaveLength(3)
      expect(readBatch!.minTs).toBe(1000)
      expect(readBatch!.maxTs).toBe(3000)
    })

    it('reads segment by sequence number', async () => {
      const originalBatch = createTestBatch(3, 1000, 3000)
      await writer.writeSegment(originalBatch)

      const readBatch = await writer.readSegmentBySeq(1)

      expect(readBatch).not.toBeNull()
      expect(readBatch!.events).toHaveLength(3)
    })

    it('returns null for non-existent segment', async () => {
      const result = await writer.readSegmentBySeq(999)
      expect(result).toBeNull()
    })

    it('preserves event data through serialization', async () => {
      const originalEvent = createTestEvent({
        ts: 12345,
        op: 'UPDATE',
        target: 'users:abc123',
        before: { name: 'Old Name' },
        after: { name: 'New Name' },
        actor: 'admin',
      })

      const batch: EventBatch = {
        events: [originalEvent],
        minTs: 12345,
        maxTs: 12345,
        count: 1,
      }

      const segment = await writer.writeSegment(batch)
      const readBatch = await writer.readSegment(segment)

      expect(readBatch).not.toBeNull()
      const readEvent = readBatch!.events[0]
      expect(readEvent.id).toBe(originalEvent.id)
      expect(readEvent.ts).toBe(originalEvent.ts)
      expect(readEvent.op).toBe(originalEvent.op)
      expect(readEvent.target).toBe(originalEvent.target)
      expect(readEvent.before).toEqual(originalEvent.before)
      expect(readEvent.after).toEqual(originalEvent.after)
      expect(readEvent.actor).toBe(originalEvent.actor)
    })
  })

  describe('delete operations', () => {
    it('deletes a segment', async () => {
      const segment = await writer.writeSegment(createTestBatch())
      expect(storage.getFileCount()).toBe(1)

      await writer.deleteSegment(segment)
      expect(storage.getFileCount()).toBe(0)
    })

    it('deletes multiple segments by sequence', async () => {
      await writer.writeSegment(createTestBatch())
      await writer.writeSegment(createTestBatch())
      await writer.writeSegment(createTestBatch())
      expect(storage.getFileCount()).toBe(3)

      await writer.deleteSegments([1, 3])
      expect(storage.getFileCount()).toBe(1)
    })
  })

  describe('listing and checking', () => {
    it('lists all segments', async () => {
      await writer.writeSegment(createTestBatch())
      await writer.writeSegment(createTestBatch())

      const paths = await writer.listSegments()
      expect(paths).toHaveLength(2)
      expect(paths).toContain('test-app/events/seg-0001.parquet')
      expect(paths).toContain('test-app/events/seg-0002.parquet')
    })

    it('checks if segment exists', async () => {
      await writer.writeSegment(createTestBatch())

      expect(await writer.segmentExists(1)).toBe(true)
      expect(await writer.segmentExists(2)).toBe(false)
    })
  })

  describe('sequence management', () => {
    it('allows setting next sequence number', async () => {
      writer.setNextSeq(100)

      const segment = await writer.writeSegment(createTestBatch())
      expect(segment.seq).toBe(100)
      expect(segment.path).toContain('seg-0100')
    })

    it('gets current next sequence number', () => {
      expect(writer.getNextSeq()).toBe(1)

      writer.setNextSeq(50)
      expect(writer.getNextSeq()).toBe(50)
    })
  })

  describe('path helpers', () => {
    it('generates correct segment paths', () => {
      expect(writer.getSegmentPath(1)).toBe('test-app/events/seg-0001.parquet')
      expect(writer.getSegmentPath(42)).toBe('test-app/events/seg-0042.parquet')
      expect(writer.getSegmentPath(9999)).toBe('test-app/events/seg-9999.parquet')
    })

    it('parses segment paths', () => {
      expect(writer.parseSegmentPath('test-app/events/seg-0001.parquet')).toBe(1)
      expect(writer.parseSegmentPath('test-app/events/seg-0042.parquet')).toBe(42)
      expect(writer.parseSegmentPath('invalid-path')).toBeNull()
    })
  })

  describe('factory functions', () => {
    it('createSegmentWriter creates a writer', () => {
      const w = createSegmentWriter(storage, { dataset: 'my-app' })
      expect(w).toBeInstanceOf(SegmentWriter)
    })

    it('createSegmentFlushHandler creates a flush handler', async () => {
      const handler = createSegmentFlushHandler(writer)
      const batch = createTestBatch()

      const segment = await handler(batch)

      expect(segment.seq).toBe(1)
      expect(storage.getFileCount()).toBe(1)
    })
  })

  describe('R2 adapter', () => {
    it('creates an adapter from R2-like bucket', async () => {
      // Mock R2 bucket
      const mockBucket = {
        files: new Map<string, Uint8Array>(),

        async put(key: string, value: ArrayBuffer | Uint8Array) {
          const data = value instanceof Uint8Array ? value : new Uint8Array(value)
          this.files.set(key, data)
        },

        async get(key: string) {
          const data = this.files.get(key)
          if (!data) return null
          return {
            async arrayBuffer() {
              return data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength)
            },
          }
        },

        async head(key: string) {
          return this.files.has(key) ? {} : null
        },

        async delete(key: string) {
          this.files.delete(key)
        },

        async list(options: { prefix: string }) {
          const keys = [...this.files.keys()].filter(k => k.startsWith(options.prefix))
          return { objects: keys.map(key => ({ key })) }
        },
      }

      const adapter = createR2Adapter(mockBucket)
      const r2Writer = new SegmentWriter(adapter, { dataset: 'r2-test' })

      const segment = await r2Writer.writeSegment(createTestBatch())
      expect(segment.seq).toBe(1)

      const readBack = await r2Writer.readSegment(segment)
      expect(readBack).not.toBeNull()
      expect(readBack!.events).toHaveLength(3)
    })
  })
})
