/**
 * ManifestManager Test Suite
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { ManifestManager, createManifestManager } from '@/events/manifest'
import type { SegmentStorage } from '@/events/segment'
import type { EventSegment } from '@/events/types'

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

  clear(): void {
    this.files.clear()
  }
}

// =============================================================================
// Helper Functions
// =============================================================================

function createTestSegment(overrides: Partial<EventSegment> = {}): EventSegment {
  return {
    seq: 1,
    path: 'test-app/events/seg-0001.parquet',
    minTs: 1000,
    maxTs: 2000,
    count: 10,
    sizeBytes: 1024,
    createdAt: Date.now(),
    ...overrides,
  }
}

// =============================================================================
// Tests
// =============================================================================

describe('ManifestManager', () => {
  let storage: MockStorage
  let manager: ManifestManager

  beforeEach(() => {
    storage = new MockStorage()
    manager = new ManifestManager(storage, { dataset: 'test-app' })
  })

  describe('load and save', () => {
    it('creates empty manifest if none exists', async () => {
      const manifest = await manager.load()

      expect(manifest.version).toBe(1)
      expect(manifest.dataset).toBe('test-app')
      expect(manifest.segments).toEqual([])
      expect(manifest.nextSeq).toBe(1)
      expect(manifest.totalEvents).toBe(0)
    })

    it('loads existing manifest', async () => {
      // Pre-populate storage
      const existingManifest = {
        version: 1,
        dataset: 'test-app',
        segments: [createTestSegment()],
        nextSeq: 2,
        totalEvents: 10,
        updatedAt: 12345,
      }
      const data = new TextEncoder().encode(JSON.stringify(existingManifest))
      await storage.put('test-app/events/_manifest.json', data)

      const manifest = await manager.load()

      expect(manifest.segments).toHaveLength(1)
      expect(manifest.nextSeq).toBe(2)
      expect(manifest.totalEvents).toBe(10)
    })

    it('saves manifest to storage', async () => {
      await manager.load()
      await manager.addSegment(createTestSegment())
      await manager.save()

      // Clear cache and reload
      manager.clearCache()
      const manifest = await manager.load()

      expect(manifest.segments).toHaveLength(1)
    })

    it('tracks dirty state', async () => {
      await manager.load()
      expect(manager.isDirty()).toBe(false)

      await manager.addSegment(createTestSegment())
      expect(manager.isDirty()).toBe(true)

      await manager.save()
      expect(manager.isDirty()).toBe(false)
    })

    it('saveIfDirty only saves when dirty', async () => {
      await manager.load()
      await manager.saveIfDirty() // Should not throw

      await manager.addSegment(createTestSegment())
      await manager.saveIfDirty()
      expect(manager.isDirty()).toBe(false)
    })
  })

  describe('segment management', () => {
    it('adds segments in order by minTs', async () => {
      await manager.load()

      await manager.addSegment(createTestSegment({ seq: 3, minTs: 3000, maxTs: 4000 }))
      await manager.addSegment(createTestSegment({ seq: 1, minTs: 1000, maxTs: 2000 }))
      await manager.addSegment(createTestSegment({ seq: 2, minTs: 2000, maxTs: 3000 }))

      const segments = await manager.getSegments()
      expect(segments.map(s => s.seq)).toEqual([1, 2, 3])
    })

    it('updates total event count when adding segments', async () => {
      await manager.load()

      await manager.addSegment(createTestSegment({ count: 10 }))
      await manager.addSegment(createTestSegment({ seq: 2, count: 20 }))

      const summary = await manager.getSummary()
      expect(summary.totalEvents).toBe(30)
    })

    it('removes a segment', async () => {
      await manager.load()
      await manager.addSegment(createTestSegment({ seq: 1, count: 10 }))
      await manager.addSegment(createTestSegment({ seq: 2, count: 20 }))

      const removed = await manager.removeSegment(1)

      expect(removed).not.toBeNull()
      expect(removed!.seq).toBe(1)

      const segments = await manager.getSegments()
      expect(segments).toHaveLength(1)
      expect(segments[0].seq).toBe(2)

      const summary = await manager.getSummary()
      expect(summary.totalEvents).toBe(20)
    })

    it('removes multiple segments', async () => {
      await manager.load()
      await manager.addSegment(createTestSegment({ seq: 1 }))
      await manager.addSegment(createTestSegment({ seq: 2 }))
      await manager.addSegment(createTestSegment({ seq: 3 }))

      const removed = await manager.removeSegments([1, 3])

      expect(removed).toHaveLength(2)
      const segments = await manager.getSegments()
      expect(segments).toHaveLength(1)
      expect(segments[0].seq).toBe(2)
    })

    it('gets segment by seq', async () => {
      await manager.load()
      await manager.addSegment(createTestSegment({ seq: 1 }))
      await manager.addSegment(createTestSegment({ seq: 2 }))

      const segment = await manager.getSegment(2)
      expect(segment).not.toBeNull()
      expect(segment!.seq).toBe(2)

      const notFound = await manager.getSegment(999)
      expect(notFound).toBeNull()
    })
  })

  describe('time range queries', () => {
    beforeEach(async () => {
      await manager.load()
      await manager.addSegment(createTestSegment({ seq: 1, minTs: 1000, maxTs: 2000 }))
      await manager.addSegment(createTestSegment({ seq: 2, minTs: 2500, maxTs: 3500 }))
      await manager.addSegment(createTestSegment({ seq: 3, minTs: 4000, maxTs: 5000 }))
    })

    it('gets segments in time range', async () => {
      // Should get seg 1 and 2
      const segments = await manager.getSegmentsInRange(1500, 3000)
      expect(segments.map(s => s.seq)).toEqual([1, 2])
    })

    it('gets segments after timestamp', async () => {
      // Should get seg 2 and 3 (their maxTs >= 2500)
      const segments = await manager.getSegmentsAfter(2500)
      expect(segments.map(s => s.seq)).toEqual([2, 3])
    })

    it('gets segments before timestamp', async () => {
      // Should get seg 1 (its maxTs < 2500)
      const segments = await manager.getSegmentsBefore(2500)
      expect(segments.map(s => s.seq)).toEqual([1])
    })

    it('returns empty for non-overlapping range', async () => {
      const segments = await manager.getSegmentsInRange(6000, 7000)
      expect(segments).toHaveLength(0)
    })
  })

  describe('compaction', () => {
    it('sets and gets compaction watermark', async () => {
      await manager.load()

      await manager.setCompactedThrough(5000)
      const watermark = await manager.getCompactedThrough()

      expect(watermark).toBe(5000)
    })

    it('gets compactable segments', async () => {
      await manager.load()
      await manager.addSegment(createTestSegment({ seq: 1, minTs: 1000, maxTs: 2000 }))
      await manager.addSegment(createTestSegment({ seq: 2, minTs: 2500, maxTs: 3500 }))
      await manager.addSegment(createTestSegment({ seq: 3, minTs: 4000, maxTs: 5000 }))

      await manager.setCompactedThrough(3500)

      const compactable = await manager.getCompactableSegments()
      expect(compactable.map(s => s.seq)).toEqual([1, 2])
    })

    it('returns empty if no watermark set', async () => {
      await manager.load()
      await manager.addSegment(createTestSegment())

      const compactable = await manager.getCompactableSegments()
      expect(compactable).toHaveLength(0)
    })
  })

  describe('sequence management', () => {
    it('gets next sequence number', async () => {
      await manager.load()
      expect(await manager.getNextSeq()).toBe(1)
    })

    it('reserves sequence number', async () => {
      await manager.load()

      const seq1 = await manager.reserveSeq()
      const seq2 = await manager.reserveSeq()

      expect(seq1).toBe(1)
      expect(seq2).toBe(2)
      expect(await manager.getNextSeq()).toBe(3)
    })

    it('updates nextSeq when adding segment with higher seq', async () => {
      await manager.load()

      await manager.addSegment(createTestSegment({ seq: 10 }))

      expect(await manager.getNextSeq()).toBe(11)
    })
  })

  describe('statistics', () => {
    it('calculates summary correctly', async () => {
      await manager.load()
      await manager.addSegment(createTestSegment({
        seq: 1, minTs: 1000, maxTs: 2000, count: 10, sizeBytes: 1024
      }))
      await manager.addSegment(createTestSegment({
        seq: 2, minTs: 3000, maxTs: 4000, count: 20, sizeBytes: 2048
      }))
      await manager.setCompactedThrough(2500)

      const summary = await manager.getSummary()

      expect(summary.segmentCount).toBe(2)
      expect(summary.totalEvents).toBe(30)
      expect(summary.minTs).toBe(1000)
      expect(summary.maxTs).toBe(4000)
      expect(summary.compactedThrough).toBe(2500)
      expect(summary.totalSizeBytes).toBe(3072)
    })

    it('handles empty manifest', async () => {
      await manager.load()

      const summary = await manager.getSummary()

      expect(summary.segmentCount).toBe(0)
      expect(summary.totalEvents).toBe(0)
      expect(summary.minTs).toBeNull()
      expect(summary.maxTs).toBeNull()
      expect(summary.totalSizeBytes).toBe(0)
    })
  })

  describe('factory function', () => {
    it('creates a manager', () => {
      const m = createManifestManager(storage, { dataset: 'my-app' })
      expect(m).toBeInstanceOf(ManifestManager)
    })
  })
})
