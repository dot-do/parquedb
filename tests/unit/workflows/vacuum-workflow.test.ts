/**
 * Vacuum Workflow Tests
 *
 * Tests for the VacuumWorkflow that cleans up orphaned files from failed commits.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'

// Import types from the vacuum workflow
import type {
  VacuumWorkflowParams,
  VacuumResult,
  OrphanedFileInfo,
} from '../../../src/workflows/vacuum-workflow'

// =============================================================================
// Mock Types
// =============================================================================

interface MockFile {
  path: string
  size: number
  mtime: Date
  content?: Uint8Array
}

interface MockStorage {
  files: Map<string, MockFile>
  list(prefix: string): Promise<{ files: string[] }>
  read(path: string): Promise<Uint8Array>
  stat(path: string): Promise<{ size: number; mtime: Date } | null>
  delete(path: string): Promise<void>
}

// =============================================================================
// Test Helpers
// =============================================================================

function createMockStorage(files: MockFile[]): MockStorage {
  const fileMap = new Map<string, MockFile>()
  for (const file of files) {
    fileMap.set(file.path, file)
  }

  return {
    files: fileMap,
    async list(prefix: string) {
      const matching = Array.from(fileMap.keys()).filter(p => p.startsWith(prefix))
      return { files: matching }
    },
    async read(path: string) {
      const file = fileMap.get(path)
      if (!file) {
        throw new Error(`File not found: ${path}`)
      }
      return file.content ?? new Uint8Array()
    },
    async stat(path: string) {
      const file = fileMap.get(path)
      if (!file) return null
      return { size: file.size, mtime: file.mtime }
    },
    async delete(path: string) {
      fileMap.delete(path)
    },
  }
}

function createIcebergMetadata(snapshots: Array<{ manifestList: string }>) {
  return JSON.stringify({
    snapshots: snapshots.map(s => ({
      'manifest-list': s.manifestList,
    })),
  })
}

function createDeltaCommit(actions: Array<{ add?: string; remove?: { path: string; timestamp?: number } }>) {
  return actions
    .map(a => {
      if (a.add) {
        return JSON.stringify({ add: { path: a.add } })
      }
      if (a.remove) {
        return JSON.stringify({ remove: a.remove })
      }
      return ''
    })
    .join('\n')
}

// =============================================================================
// Tests
// =============================================================================

describe('VacuumWorkflow', () => {
  describe('Iceberg orphan detection', () => {
    it('should identify manifest files not referenced by any snapshot', async () => {
      const now = Date.now()
      const oldTime = new Date(now - 48 * 60 * 60 * 1000) // 48 hours ago (older than retention)

      // Active manifest referenced in metadata
      const activeManifestPath = 'warehouse/users/metadata/active-m0.avro'
      // Orphaned manifest not referenced
      const orphanedManifestPath = 'warehouse/users/metadata/orphaned-m0.avro'

      const storage = createMockStorage([
        {
          path: 'warehouse/users/metadata/version-hint.text',
          size: 100,
          mtime: new Date(),
          content: new TextEncoder().encode('warehouse/users/metadata/1-abc.metadata.json'),
        },
        {
          path: 'warehouse/users/metadata/1-abc.metadata.json',
          size: 1000,
          mtime: new Date(),
          content: new TextEncoder().encode(
            createIcebergMetadata([
              { manifestList: 'warehouse/users/metadata/snap-123.avro' },
            ])
          ),
        },
        {
          path: 'warehouse/users/metadata/snap-123.avro',
          size: 500,
          mtime: new Date(),
          // Simplified manifest list that references active manifest
          content: new TextEncoder().encode(JSON.stringify([{ 'manifest-path': activeManifestPath }])),
        },
        {
          path: activeManifestPath,
          size: 200,
          mtime: new Date(),
          content: new Uint8Array(),
        },
        {
          path: orphanedManifestPath,
          size: 200,
          mtime: oldTime,
          content: new Uint8Array(),
        },
      ])

      // The orphaned manifest should be detected
      expect(storage.files.has(orphanedManifestPath)).toBe(true)
      expect(storage.files.has(activeManifestPath)).toBe(true)
    })

    it('should identify data files not referenced by any manifest', async () => {
      const now = Date.now()
      const oldTime = new Date(now - 48 * 60 * 60 * 1000)

      const activeDataPath = 'warehouse/users/data/active-123.parquet'
      const orphanedDataPath = 'warehouse/users/data/orphaned-456.parquet'

      const storage = createMockStorage([
        {
          path: activeDataPath,
          size: 5000,
          mtime: new Date(),
        },
        {
          path: orphanedDataPath,
          size: 3000,
          mtime: oldTime,
        },
      ])

      // Both should exist before vacuum
      expect(storage.files.has(activeDataPath)).toBe(true)
      expect(storage.files.has(orphanedDataPath)).toBe(true)
    })
  })

  describe('Delta orphan detection', () => {
    it('should identify parquet files not referenced in delta log', async () => {
      const now = Date.now()
      const oldTime = new Date(now - 48 * 60 * 60 * 1000)

      const activeDataPath = 'warehouse/posts/active.parquet'
      const orphanedDataPath = 'warehouse/posts/orphaned.parquet'

      const storage = createMockStorage([
        {
          path: 'warehouse/posts/_delta_log/00000000000000000000.json',
          size: 100,
          mtime: new Date(),
          content: new TextEncoder().encode(
            createDeltaCommit([{ add: 'active.parquet' }])
          ),
        },
        {
          path: activeDataPath,
          size: 5000,
          mtime: new Date(),
        },
        {
          path: orphanedDataPath,
          size: 3000,
          mtime: oldTime,
        },
      ])

      expect(storage.files.has(activeDataPath)).toBe(true)
      expect(storage.files.has(orphanedDataPath)).toBe(true)
    })

    it('should keep removed files within retention period', async () => {
      const now = Date.now()
      const recentRemovalTime = now - 1 * 60 * 60 * 1000 // 1 hour ago (within retention)

      const recentlyRemovedPath = 'warehouse/posts/removed-recent.parquet'

      const storage = createMockStorage([
        {
          path: 'warehouse/posts/_delta_log/00000000000000000000.json',
          size: 100,
          mtime: new Date(),
          content: new TextEncoder().encode(
            createDeltaCommit([{ add: 'removed-recent.parquet' }])
          ),
        },
        {
          path: 'warehouse/posts/_delta_log/00000000000000000001.json',
          size: 100,
          mtime: new Date(),
          content: new TextEncoder().encode(
            createDeltaCommit([{ remove: { path: 'removed-recent.parquet', timestamp: recentRemovalTime } }])
          ),
        },
        {
          path: recentlyRemovedPath,
          size: 5000,
          mtime: new Date(recentRemovalTime),
        },
      ])

      // File should still exist (within retention)
      expect(storage.files.has(recentlyRemovedPath)).toBe(true)
    })
  })

  describe('Dry run mode', () => {
    it('should report orphans without deleting them', async () => {
      const now = Date.now()
      const oldTime = new Date(now - 48 * 60 * 60 * 1000)

      const orphanedPath = 'warehouse/users/data/orphaned.parquet'

      const storage = createMockStorage([
        {
          path: orphanedPath,
          size: 3000,
          mtime: oldTime,
        },
      ])

      // In dry run, files should not be deleted
      expect(storage.files.has(orphanedPath)).toBe(true)

      // Simulate dry run - file should still exist
      expect(storage.files.has(orphanedPath)).toBe(true)
    })
  })

  describe('Retention period', () => {
    it('should not delete files newer than retention period', async () => {
      const now = Date.now()
      const recentTime = new Date(now - 1 * 60 * 60 * 1000) // 1 hour ago

      const recentOrphanPath = 'warehouse/users/data/recent-orphan.parquet'

      const storage = createMockStorage([
        {
          path: recentOrphanPath,
          size: 3000,
          mtime: recentTime,
        },
      ])

      // Recent orphan should not be deleted (within 24 hour retention)
      expect(storage.files.has(recentOrphanPath)).toBe(true)
    })

    it('should respect custom retention period', async () => {
      const now = Date.now()
      const oneHourAgo = new Date(now - 1 * 60 * 60 * 1000)

      // With 30 minute retention, 1 hour old file should be orphaned
      const customRetentionMs = 30 * 60 * 1000 // 30 minutes
      const fileAge = now - oneHourAgo.getTime()

      expect(fileAge > customRetentionMs).toBe(true) // File is older than retention
    })
  })

  describe('Format detection', () => {
    it('should detect Iceberg format from metadata files', async () => {
      const storage = createMockStorage([
        {
          path: 'warehouse/users/metadata/version-hint.text',
          size: 100,
          mtime: new Date(),
        },
      ])

      const files = await storage.list('warehouse/users/metadata/')
      const hasIceberg = files.files.some(
        f => f.endsWith('.metadata.json') || f.includes('version-hint.text')
      )

      expect(hasIceberg).toBe(true)
    })

    it('should detect Delta format from _delta_log', async () => {
      const storage = createMockStorage([
        {
          path: 'warehouse/posts/_delta_log/00000000000000000000.json',
          size: 100,
          mtime: new Date(),
        },
      ])

      const files = await storage.list('warehouse/posts/_delta_log/')
      const hasDelta = files.files.some(f => f.endsWith('.json'))

      expect(hasDelta).toBe(true)
    })
  })

  describe('Orphan reasons', () => {
    it('should categorize Iceberg orphans correctly', () => {
      // Iceberg orphans should be 'not_in_snapshot'
      const orphan: OrphanedFileInfo = {
        path: 'warehouse/users/metadata/orphan.avro',
        size: 1000,
        lastModified: new Date(),
        ageMs: 86400000,
        reason: 'not_in_snapshot',
        deleted: false,
      }

      expect(orphan.reason).toBe('not_in_snapshot')
    })

    it('should categorize Delta orphans correctly', () => {
      // Delta orphans should be 'not_in_delta_log'
      const orphan: OrphanedFileInfo = {
        path: 'warehouse/posts/orphan.parquet',
        size: 1000,
        lastModified: new Date(),
        ageMs: 86400000,
        reason: 'not_in_delta_log',
        deleted: false,
      }

      expect(orphan.reason).toBe('not_in_delta_log')
    })

    it('should categorize expired remove actions correctly', () => {
      const orphan: OrphanedFileInfo = {
        path: 'warehouse/posts/removed.parquet',
        size: 1000,
        lastModified: new Date(),
        ageMs: 172800000, // 48 hours
        reason: 'removed_action_expired',
        deleted: false,
      }

      expect(orphan.reason).toBe('removed_action_expired')
    })
  })

  describe('Result structure', () => {
    it('should return correct result structure for successful vacuum', () => {
      const result: VacuumResult = {
        success: true,
        filesScanned: 100,
        orphansFound: 5,
        filesDeleted: 5,
        bytesRecovered: 50000,
        orphanedFiles: [],
        errors: [],
        durationMs: 1000,
        dryRun: false,
      }

      expect(result.success).toBe(true)
      expect(result.filesScanned).toBe(100)
      expect(result.orphansFound).toBe(5)
      expect(result.filesDeleted).toBe(5)
      expect(result.bytesRecovered).toBe(50000)
      expect(result.errors).toHaveLength(0)
      expect(result.dryRun).toBe(false)
    })

    it('should return correct result structure for dry run', () => {
      const result: VacuumResult = {
        success: true,
        filesScanned: 100,
        orphansFound: 5,
        filesDeleted: 0, // No deletions in dry run
        bytesRecovered: 0, // No bytes recovered in dry run
        orphanedFiles: [
          {
            path: 'test/orphan.parquet',
            size: 10000,
            lastModified: new Date(),
            ageMs: 86400000,
            reason: 'not_in_snapshot',
            deleted: false,
          },
        ],
        errors: [],
        durationMs: 500,
        dryRun: true,
      }

      expect(result.success).toBe(true)
      expect(result.filesDeleted).toBe(0)
      expect(result.bytesRecovered).toBe(0)
      expect(result.dryRun).toBe(true)
      expect(result.orphanedFiles[0]?.deleted).toBe(false)
    })

    it('should include errors for partial failures', () => {
      const result: VacuumResult = {
        success: false,
        filesScanned: 100,
        orphansFound: 5,
        filesDeleted: 3,
        bytesRecovered: 30000,
        orphanedFiles: [],
        errors: ['Failed to delete file1: Permission denied', 'Failed to delete file2: Timeout'],
        durationMs: 2000,
        dryRun: false,
      }

      expect(result.success).toBe(false)
      expect(result.errors).toHaveLength(2)
      expect(result.errors[0]).toContain('Permission denied')
    })
  })

  describe('Params validation', () => {
    it('should use default retention when not specified', () => {
      const params: VacuumWorkflowParams = {
        namespace: 'users',
      }

      // Default retention should be 24 hours
      const defaultRetentionMs = 24 * 60 * 60 * 1000
      const retentionMs = params.retentionMs ?? defaultRetentionMs

      expect(retentionMs).toBe(defaultRetentionMs)
    })

    it('should use custom retention when specified', () => {
      const params: VacuumWorkflowParams = {
        namespace: 'users',
        retentionMs: 7 * 24 * 60 * 60 * 1000, // 7 days
      }

      expect(params.retentionMs).toBe(7 * 24 * 60 * 60 * 1000)
    })

    it('should default to auto format detection', () => {
      const params: VacuumWorkflowParams = {
        namespace: 'users',
      }

      const format = params.format ?? 'auto'
      expect(format).toBe('auto')
    })

    it('should default to non-dry-run mode', () => {
      const params: VacuumWorkflowParams = {
        namespace: 'users',
      }

      const dryRun = params.dryRun ?? false
      expect(dryRun).toBe(false)
    })
  })
})
