/**
 * Event Archival Test Suite
 *
 * Archival moves old event segments to cold storage (archive path) for
 * long-term retention. Unlike compaction, archival preserves the original
 * events without replaying them.
 *
 * Archive structure:
 *   events/archive/{year}/{month}/seg-{seq}.parquet
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  EventArchiver,
  createEventArchiver,
  type ArchiverOptions,
  type ArchivalResult,
  type ArchivalPolicy,
} from '@/events/archival'
import { ManifestManager } from '@/events/manifest'
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

  // Test helpers
  getFileCount(): number {
    return this.files.size
  }

  getAllKeys(): string[] {
    return [...this.files.keys()]
  }

  clear(): void {
    this.files.clear()
  }

  // Helper to write segment data
  writeSegmentData(path: string, data: string): void {
    this.files.set(path, new TextEncoder().encode(data))
  }
}

// =============================================================================
// Helper Functions
// =============================================================================

function createTestSegment(overrides: Partial<EventSegment> = {}): EventSegment {
  const ts = overrides.minTs ?? Date.now()
  return {
    seq: 1,
    path: 'test-app/events/seg-0001.parquet',
    minTs: ts,
    maxTs: ts + 1000,
    count: 10,
    sizeBytes: 1024,
    createdAt: ts,
    ...overrides,
  }
}

function daysAgo(days: number): number {
  return Date.now() - days * 24 * 60 * 60 * 1000
}

// =============================================================================
// EventArchiver Tests
// =============================================================================

describe('EventArchiver', () => {
  let storage: MockStorage
  let manifest: ManifestManager
  let archiver: EventArchiver

  beforeEach(async () => {
    storage = new MockStorage()
    manifest = new ManifestManager(storage, { dataset: 'test-app' })
    await manifest.load()

    archiver = new EventArchiver({
      dataset: 'test-app',
      storage,
      manifest,
    })
  })

  describe('constructor', () => {
    it('creates an archiver with default policy', () => {
      expect(archiver).toBeInstanceOf(EventArchiver)
    })

    it('accepts custom archival policy', () => {
      const customArchiver = new EventArchiver({
        dataset: 'test-app',
        storage,
        manifest,
        policy: {
          retentionDays: 90,
          archiveAfterDays: 7,
        },
      })
      expect(customArchiver).toBeInstanceOf(EventArchiver)
    })
  })

  describe('getArchivableSegments', () => {
    it('returns empty array when no segments exist', async () => {
      const segments = await archiver.getArchivableSegments()
      expect(segments).toEqual([])
    })

    it('returns segments older than archiveAfterDays', async () => {
      // Segment from 10 days ago
      const oldSegment = createTestSegment({
        seq: 1,
        path: 'test-app/events/seg-0001.parquet',
        minTs: daysAgo(10),
        maxTs: daysAgo(10) + 1000,
      })

      // Segment from 1 day ago
      const recentSegment = createTestSegment({
        seq: 2,
        path: 'test-app/events/seg-0002.parquet',
        minTs: daysAgo(1),
        maxTs: daysAgo(1) + 1000,
      })

      await manifest.addSegment(oldSegment)
      await manifest.addSegment(recentSegment)

      // Default archiveAfterDays is 7
      const archivable = await archiver.getArchivableSegments()

      expect(archivable).toHaveLength(1)
      expect(archivable[0].seq).toBe(1)
    })

    it('respects custom archiveAfterDays', async () => {
      const customArchiver = new EventArchiver({
        dataset: 'test-app',
        storage,
        manifest,
        policy: { archiveAfterDays: 30 },
      })

      const segment = createTestSegment({
        seq: 1,
        path: 'test-app/events/seg-0001.parquet',
        minTs: daysAgo(15),
        maxTs: daysAgo(15) + 1000,
      })

      await manifest.addSegment(segment)

      // 15 days is less than 30 days threshold
      const archivable = await customArchiver.getArchivableSegments()
      expect(archivable).toHaveLength(0)
    })
  })

  describe('getArchivePath', () => {
    it('generates archive path with year/month structure', () => {
      // January 15, 2024
      const ts = new Date('2024-01-15T12:00:00Z').getTime()
      const segment = createTestSegment({
        seq: 42,
        path: 'test-app/events/seg-0042.parquet',
        minTs: ts,
      })

      const archivePath = archiver.getArchivePath(segment)

      expect(archivePath).toBe('test-app/events/archive/2024/01/seg-0042.parquet')
    })

    it('zero-pads month correctly', () => {
      // September (month 09)
      const ts = new Date('2024-09-01T12:00:00Z').getTime()
      const segment = createTestSegment({
        seq: 1,
        path: 'test-app/events/seg-0001.parquet',
        minTs: ts,
      })

      const archivePath = archiver.getArchivePath(segment)

      expect(archivePath).toContain('/2024/09/')
    })
  })

  describe('archive', () => {
    it('returns empty result when no archivable segments', async () => {
      const result = await archiver.archive()

      expect(result.segmentsArchived).toBe(0)
      expect(result.segmentsFailed).toBe(0)
      expect(result.archivedPaths).toEqual([])
    })

    it('moves old segments to archive path', async () => {
      const oldTs = daysAgo(10)
      const segment = createTestSegment({
        seq: 1,
        path: 'test-app/events/seg-0001.parquet',
        minTs: oldTs,
        maxTs: oldTs + 1000,
      })

      // Write segment data
      storage.writeSegmentData(segment.path, '{"id":"evt1","ts":1000}')
      await manifest.addSegment(segment)

      const result = await archiver.archive()

      expect(result.segmentsArchived).toBe(1)
      expect(result.archivedPaths).toHaveLength(1)

      // Original should be deleted
      expect(await storage.head(segment.path)).toBe(false)

      // Archive should exist
      const archivePath = archiver.getArchivePath(segment)
      expect(await storage.head(archivePath)).toBe(true)
    })

    it('preserves segment data during archival', async () => {
      const oldTs = daysAgo(10)
      const segment = createTestSegment({
        seq: 1,
        path: 'test-app/events/seg-0001.parquet',
        minTs: oldTs,
        maxTs: oldTs + 1000,
      })

      const originalData = '{"id":"evt1","ts":1000}\n{"id":"evt2","ts":2000}'
      storage.writeSegmentData(segment.path, originalData)
      await manifest.addSegment(segment)

      await archiver.archive()

      const archivePath = archiver.getArchivePath(segment)
      const archivedData = await storage.get(archivePath)

      expect(new TextDecoder().decode(archivedData!)).toBe(originalData)
    })

    it('removes archived segments from manifest', async () => {
      const oldTs = daysAgo(10)
      const segment = createTestSegment({
        seq: 1,
        path: 'test-app/events/seg-0001.parquet',
        minTs: oldTs,
        maxTs: oldTs + 1000,
      })

      storage.writeSegmentData(segment.path, '{}')
      await manifest.addSegment(segment)

      expect((await manifest.getSegments()).length).toBe(1)

      await archiver.archive()

      // Segment should be removed from manifest
      expect((await manifest.getSegments()).length).toBe(0)
    })

    it('archives multiple segments in order', async () => {
      const segments = [
        createTestSegment({
          seq: 1,
          path: 'test-app/events/seg-0001.parquet',
          minTs: daysAgo(30),
          maxTs: daysAgo(30) + 1000,
        }),
        createTestSegment({
          seq: 2,
          path: 'test-app/events/seg-0002.parquet',
          minTs: daysAgo(20),
          maxTs: daysAgo(20) + 1000,
        }),
        createTestSegment({
          seq: 3,
          path: 'test-app/events/seg-0003.parquet',
          minTs: daysAgo(10),
          maxTs: daysAgo(10) + 1000,
        }),
      ]

      for (const seg of segments) {
        storage.writeSegmentData(seg.path, `{"seq":${seg.seq}}`)
        await manifest.addSegment(seg)
      }

      const result = await archiver.archive()

      expect(result.segmentsArchived).toBe(3)
      expect(result.archivedPaths).toHaveLength(3)
    })

    it('continues archiving after individual segment failure', async () => {
      const segments = [
        createTestSegment({
          seq: 1,
          path: 'test-app/events/seg-0001.parquet',
          minTs: daysAgo(20),
        }),
        createTestSegment({
          seq: 2,
          path: 'test-app/events/seg-0002.parquet',
          minTs: daysAgo(10),
        }),
      ]

      // Only write data for segment 2 (segment 1 will fail to read)
      storage.writeSegmentData(segments[1].path, '{}')

      for (const seg of segments) {
        await manifest.addSegment(seg)
      }

      const result = await archiver.archive()

      expect(result.segmentsArchived).toBe(1)
      expect(result.segmentsFailed).toBe(1)
      expect(result.errors).toHaveLength(1)
    })

    it('respects maxSegments limit', async () => {
      const segments = [
        createTestSegment({
          seq: 1,
          path: 'test-app/events/seg-0001.parquet',
          minTs: daysAgo(30),
        }),
        createTestSegment({
          seq: 2,
          path: 'test-app/events/seg-0002.parquet',
          minTs: daysAgo(20),
        }),
        createTestSegment({
          seq: 3,
          path: 'test-app/events/seg-0003.parquet',
          minTs: daysAgo(10),
        }),
      ]

      for (const seg of segments) {
        storage.writeSegmentData(seg.path, '{}')
        await manifest.addSegment(seg)
      }

      const result = await archiver.archive({ maxSegments: 2 })

      expect(result.segmentsArchived).toBe(2)
      // Remaining segment should still be in manifest
      expect((await manifest.getSegments()).length).toBe(1)
    })

    it('uses dryRun mode without modifying storage', async () => {
      const oldTs = daysAgo(10)
      const segment = createTestSegment({
        seq: 1,
        path: 'test-app/events/seg-0001.parquet',
        minTs: oldTs,
      })

      storage.writeSegmentData(segment.path, '{}')
      await manifest.addSegment(segment)

      const result = await archiver.archive({ dryRun: true })

      expect(result.segmentsArchived).toBe(1)
      expect(result.dryRun).toBe(true)

      // Original should still exist
      expect(await storage.head(segment.path)).toBe(true)
      // Archive should NOT exist
      const archivePath = archiver.getArchivePath(segment)
      expect(await storage.head(archivePath)).toBe(false)
      // Manifest should still have segment
      expect((await manifest.getSegments()).length).toBe(1)
    })
  })

  describe('listArchived', () => {
    it('returns empty array when no archived segments', async () => {
      const archived = await archiver.listArchived()
      expect(archived).toEqual([])
    })

    it('lists all archived segment paths', async () => {
      // Simulate archived segments
      storage.writeSegmentData('test-app/events/archive/2024/01/seg-0001.parquet', '{}')
      storage.writeSegmentData('test-app/events/archive/2024/02/seg-0002.parquet', '{}')
      storage.writeSegmentData('test-app/events/archive/2024/02/seg-0003.parquet', '{}')

      const archived = await archiver.listArchived()

      expect(archived).toHaveLength(3)
      expect(archived).toContain('test-app/events/archive/2024/01/seg-0001.parquet')
      expect(archived).toContain('test-app/events/archive/2024/02/seg-0002.parquet')
      expect(archived).toContain('test-app/events/archive/2024/02/seg-0003.parquet')
    })

    it('filters by year', async () => {
      storage.writeSegmentData('test-app/events/archive/2023/12/seg-0001.parquet', '{}')
      storage.writeSegmentData('test-app/events/archive/2024/01/seg-0002.parquet', '{}')

      const archived = await archiver.listArchived({ year: 2024 })

      expect(archived).toHaveLength(1)
      expect(archived[0]).toContain('/2024/')
    })

    it('filters by year and month', async () => {
      storage.writeSegmentData('test-app/events/archive/2024/01/seg-0001.parquet', '{}')
      storage.writeSegmentData('test-app/events/archive/2024/02/seg-0002.parquet', '{}')

      const archived = await archiver.listArchived({ year: 2024, month: 1 })

      expect(archived).toHaveLength(1)
      expect(archived[0]).toContain('/2024/01/')
    })
  })

  describe('restore', () => {
    it('restores archived segment to active path', async () => {
      const archivePath = 'test-app/events/archive/2024/01/seg-0001.parquet'
      const activePath = 'test-app/events/seg-0001.parquet'
      const originalData = '{"id":"evt1"}'

      storage.writeSegmentData(archivePath, originalData)

      await archiver.restore(archivePath)

      // Active path should exist
      expect(await storage.head(activePath)).toBe(true)
      // Data should be preserved
      const restoredData = await storage.get(activePath)
      expect(new TextDecoder().decode(restoredData!)).toBe(originalData)
    })

    it('removes segment from archive after restore', async () => {
      const archivePath = 'test-app/events/archive/2024/01/seg-0001.parquet'
      storage.writeSegmentData(archivePath, '{}')

      await archiver.restore(archivePath)

      expect(await storage.head(archivePath)).toBe(false)
    })

    it('adds restored segment back to manifest', async () => {
      const archivePath = 'test-app/events/archive/2024/01/seg-0001.parquet'
      const originalData = '{"id":"evt1","ts":1000}\n{"id":"evt2","ts":2000}'
      storage.writeSegmentData(archivePath, originalData)

      await archiver.restore(archivePath)

      const segments = await manifest.getSegments()
      expect(segments.length).toBe(1)
      expect(segments[0].seq).toBe(1)
    })

    it('throws error for non-existent archive', async () => {
      await expect(
        archiver.restore('test-app/events/archive/2024/01/seg-9999.parquet')
      ).rejects.toThrow(/not found/i)
    })

    it('throws error for invalid archive path', async () => {
      await expect(
        archiver.restore('test-app/events/seg-0001.parquet')
      ).rejects.toThrow(/invalid archive path/i)
    })
  })

  describe('purgeOldArchives', () => {
    it('deletes archives older than retention period', async () => {
      // Use 30-day retention
      const customArchiver = new EventArchiver({
        dataset: 'test-app',
        storage,
        manifest,
        policy: { retentionDays: 30 },
      })

      // Archive from 60 days ago (should be purged)
      const oldDate = new Date(daysAgo(60))
      const oldPath = `test-app/events/archive/${oldDate.getUTCFullYear()}/${String(oldDate.getUTCMonth() + 1).padStart(2, '0')}/seg-0001.parquet`

      // Archive from 10 days ago (should be kept)
      const recentDate = new Date(daysAgo(10))
      const recentPath = `test-app/events/archive/${recentDate.getUTCFullYear()}/${String(recentDate.getUTCMonth() + 1).padStart(2, '0')}/seg-0002.parquet`

      storage.writeSegmentData(oldPath, '{}')
      storage.writeSegmentData(recentPath, '{}')

      const result = await customArchiver.purgeOldArchives()

      expect(result.purgedCount).toBe(1)
      expect(await storage.head(oldPath)).toBe(false)
      expect(await storage.head(recentPath)).toBe(true)
    })

    it('returns zero when nothing to purge', async () => {
      const result = await archiver.purgeOldArchives()

      expect(result.purgedCount).toBe(0)
    })

    it('respects dryRun mode', async () => {
      const customArchiver = new EventArchiver({
        dataset: 'test-app',
        storage,
        manifest,
        policy: { retentionDays: 30 },
      })

      const oldDate = new Date(daysAgo(60))
      const oldPath = `test-app/events/archive/${oldDate.getUTCFullYear()}/${String(oldDate.getUTCMonth() + 1).padStart(2, '0')}/seg-0001.parquet`
      storage.writeSegmentData(oldPath, '{}')

      const result = await customArchiver.purgeOldArchives({ dryRun: true })

      expect(result.purgedCount).toBe(1)
      expect(result.dryRun).toBe(true)
      // File should still exist
      expect(await storage.head(oldPath)).toBe(true)
    })
  })
})

// =============================================================================
// Factory Function Tests
// =============================================================================

describe('createEventArchiver', () => {
  it('creates an archiver instance', async () => {
    const storage = new MockStorage()
    const manifest = new ManifestManager(storage, { dataset: 'test' })

    const archiver = createEventArchiver({
      dataset: 'test',
      storage,
      manifest,
    })

    expect(archiver).toBeInstanceOf(EventArchiver)
  })
})
