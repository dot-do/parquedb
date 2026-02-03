/**
 * Sync Manifest Tests
 *
 * Tests for manifest creation, diffing, and conflict resolution.
 */

import { describe, it, expect } from 'vitest'
import {
  type SyncManifest,
  type SyncFileEntry,
  createManifest,
  diffManifests,
  resolveConflicts,
  updateManifestFile,
  removeManifestFile,
} from '../../../src/sync/manifest'

describe('Sync Manifest', () => {
  describe('createManifest', () => {
    it('should create a manifest with required fields', () => {
      const manifest = createManifest('db-123', 'my-database')

      expect(manifest.version).toBe(1)
      expect(manifest.databaseId).toBe('db-123')
      expect(manifest.name).toBe('my-database')
      expect(manifest.visibility).toBe('private') // default
      expect(manifest.files).toEqual({})
      expect(manifest.lastSyncedAt).toBeDefined()
    })

    it('should accept optional fields', () => {
      const manifest = createManifest('db-123', 'my-database', {
        owner: 'username',
        slug: 'my-dataset',
        visibility: 'public',
        metadata: { description: 'Test database' },
      })

      expect(manifest.owner).toBe('username')
      expect(manifest.slug).toBe('my-dataset')
      expect(manifest.visibility).toBe('public')
      expect(manifest.metadata).toEqual({ description: 'Test database' })
    })
  })

  describe('diffManifests', () => {
    const createFile = (path: string, hash: string, modifiedAt: string = '2024-01-01T00:00:00Z'): SyncFileEntry => ({
      path,
      size: 1000,
      hash,
      hashAlgorithm: 'sha256',
      modifiedAt,
    })

    it('should identify files to upload (local only)', () => {
      const local = createManifest('db', 'test')
      local.files = {
        'data/posts/data.parquet': createFile('data/posts/data.parquet', 'abc123'),
      }

      const remote = createManifest('db', 'test')
      remote.files = {}

      const diff = diffManifests(local, remote)

      expect(diff.toUpload).toHaveLength(1)
      expect(diff.toUpload[0]?.path).toBe('data/posts/data.parquet')
      expect(diff.toDownload).toHaveLength(0)
      expect(diff.conflicts).toHaveLength(0)
    })

    it('should identify files to download (remote only)', () => {
      const local = createManifest('db', 'test')
      local.files = {}

      const remote = createManifest('db', 'test')
      remote.files = {
        'data/users/data.parquet': createFile('data/users/data.parquet', 'def456'),
      }

      const diff = diffManifests(local, remote)

      expect(diff.toUpload).toHaveLength(0)
      expect(diff.toDownload).toHaveLength(1)
      expect(diff.toDownload[0]?.path).toBe('data/users/data.parquet')
      expect(diff.conflicts).toHaveLength(0)
    })

    it('should identify unchanged files', () => {
      const local = createManifest('db', 'test')
      local.files = {
        'data/posts/data.parquet': createFile('data/posts/data.parquet', 'same-hash'),
      }

      const remote = createManifest('db', 'test')
      remote.files = {
        'data/posts/data.parquet': createFile('data/posts/data.parquet', 'same-hash'),
      }

      const diff = diffManifests(local, remote)

      expect(diff.toUpload).toHaveLength(0)
      expect(diff.toDownload).toHaveLength(0)
      expect(diff.conflicts).toHaveLength(0)
      expect(diff.unchanged).toContain('data/posts/data.parquet')
    })

    it('should identify conflicts (different hashes)', () => {
      const local = createManifest('db', 'test')
      local.files = {
        'data/posts/data.parquet': createFile('data/posts/data.parquet', 'local-hash', '2024-01-02T00:00:00Z'),
      }

      const remote = createManifest('db', 'test')
      remote.files = {
        'data/posts/data.parquet': createFile('data/posts/data.parquet', 'remote-hash', '2024-01-01T00:00:00Z'),
      }

      const diff = diffManifests(local, remote)

      expect(diff.conflicts).toHaveLength(1)
      expect(diff.conflicts[0]?.path).toBe('data/posts/data.parquet')
      expect(diff.conflicts[0]?.suggestedResolution).toBe('keep-local') // local is newer
    })

    it('should suggest keep-remote when remote is newer', () => {
      const local = createManifest('db', 'test')
      local.files = {
        'data/posts/data.parquet': createFile('data/posts/data.parquet', 'local-hash', '2024-01-01T00:00:00Z'),
      }

      const remote = createManifest('db', 'test')
      remote.files = {
        'data/posts/data.parquet': createFile('data/posts/data.parquet', 'remote-hash', '2024-01-02T00:00:00Z'),
      }

      const diff = diffManifests(local, remote)

      expect(diff.conflicts[0]?.suggestedResolution).toBe('keep-remote')
    })

    it('should handle null manifests', () => {
      const local = createManifest('db', 'test')
      local.files = {
        'data/posts/data.parquet': createFile('data/posts/data.parquet', 'abc123'),
      }

      // Local exists, remote doesn't
      const diff1 = diffManifests(local, null)
      expect(diff1.toUpload).toHaveLength(1)
      expect(diff1.toDownload).toHaveLength(0)

      // Remote exists, local doesn't
      const diff2 = diffManifests(null, local)
      expect(diff2.toUpload).toHaveLength(0)
      expect(diff2.toDownload).toHaveLength(1)

      // Neither exists
      const diff3 = diffManifests(null, null)
      expect(diff3.toUpload).toHaveLength(0)
      expect(diff3.toDownload).toHaveLength(0)
    })
  })

  describe('resolveConflicts', () => {
    const createConflict = (localTime: string, remoteTime: string) => ({
      path: 'data/test.parquet',
      local: {
        path: 'data/test.parquet',
        size: 1000,
        hash: 'local-hash',
        hashAlgorithm: 'sha256' as const,
        modifiedAt: localTime,
      },
      remote: {
        path: 'data/test.parquet',
        size: 1000,
        hash: 'remote-hash',
        hashAlgorithm: 'sha256' as const,
        modifiedAt: remoteTime,
      },
      suggestedResolution: 'manual' as const,
    })

    it('should resolve conflicts with local-wins strategy', () => {
      const conflicts = [createConflict('2024-01-01T00:00:00Z', '2024-01-02T00:00:00Z')]
      const { upload, download, manual } = resolveConflicts(conflicts, 'local-wins')

      expect(upload).toHaveLength(1)
      expect(download).toHaveLength(0)
      expect(manual).toHaveLength(0)
    })

    it('should resolve conflicts with remote-wins strategy', () => {
      const conflicts = [createConflict('2024-01-02T00:00:00Z', '2024-01-01T00:00:00Z')]
      const { upload, download, manual } = resolveConflicts(conflicts, 'remote-wins')

      expect(upload).toHaveLength(0)
      expect(download).toHaveLength(1)
      expect(manual).toHaveLength(0)
    })

    it('should resolve conflicts with newest strategy (local newer)', () => {
      const conflicts = [createConflict('2024-01-02T00:00:00Z', '2024-01-01T00:00:00Z')]
      const { upload, download, manual } = resolveConflicts(conflicts, 'newest')

      expect(upload).toHaveLength(1)
      expect(download).toHaveLength(0)
    })

    it('should resolve conflicts with newest strategy (remote newer)', () => {
      const conflicts = [createConflict('2024-01-01T00:00:00Z', '2024-01-02T00:00:00Z')]
      const { upload, download, manual } = resolveConflicts(conflicts, 'newest')

      expect(upload).toHaveLength(0)
      expect(download).toHaveLength(1)
    })

    it('should leave conflicts for manual strategy', () => {
      const conflicts = [createConflict('2024-01-01T00:00:00Z', '2024-01-02T00:00:00Z')]
      const { upload, download, manual } = resolveConflicts(conflicts, 'manual')

      expect(upload).toHaveLength(0)
      expect(download).toHaveLength(0)
      expect(manual).toHaveLength(1)
    })
  })

  describe('updateManifestFile', () => {
    it('should add new file to manifest', () => {
      const manifest = createManifest('db', 'test')
      const file: SyncFileEntry = {
        path: 'data/new.parquet',
        size: 1000,
        hash: 'abc123',
        hashAlgorithm: 'sha256',
        modifiedAt: '2024-01-01T00:00:00Z',
      }

      const updated = updateManifestFile(manifest, file)

      expect(updated.files['data/new.parquet']).toBeDefined()
      expect(updated.files['data/new.parquet']?.hash).toBe('abc123')
    })

    it('should update existing file in manifest', () => {
      const manifest = createManifest('db', 'test')
      manifest.files = {
        'data/existing.parquet': {
          path: 'data/existing.parquet',
          size: 1000,
          hash: 'old-hash',
          hashAlgorithm: 'sha256',
          modifiedAt: '2024-01-01T00:00:00Z',
        },
      }

      const updatedFile: SyncFileEntry = {
        path: 'data/existing.parquet',
        size: 2000,
        hash: 'new-hash',
        hashAlgorithm: 'sha256',
        modifiedAt: '2024-01-02T00:00:00Z',
      }

      const updated = updateManifestFile(manifest, updatedFile)

      expect(updated.files['data/existing.parquet']?.hash).toBe('new-hash')
      expect(updated.files['data/existing.parquet']?.size).toBe(2000)
    })
  })

  describe('removeManifestFile', () => {
    it('should remove file from manifest', () => {
      const manifest = createManifest('db', 'test')
      manifest.files = {
        'data/file1.parquet': {
          path: 'data/file1.parquet',
          size: 1000,
          hash: 'abc',
          hashAlgorithm: 'sha256',
          modifiedAt: '2024-01-01T00:00:00Z',
        },
        'data/file2.parquet': {
          path: 'data/file2.parquet',
          size: 2000,
          hash: 'def',
          hashAlgorithm: 'sha256',
          modifiedAt: '2024-01-01T00:00:00Z',
        },
      }

      const updated = removeManifestFile(manifest, 'data/file1.parquet')

      expect(updated.files['data/file1.parquet']).toBeUndefined()
      expect(updated.files['data/file2.parquet']).toBeDefined()
    })

    it('should handle removing non-existent file', () => {
      const manifest = createManifest('db', 'test')
      manifest.files = {
        'data/existing.parquet': {
          path: 'data/existing.parquet',
          size: 1000,
          hash: 'abc',
          hashAlgorithm: 'sha256',
          modifiedAt: '2024-01-01T00:00:00Z',
        },
      }

      const updated = removeManifestFile(manifest, 'data/non-existent.parquet')

      expect(updated.files['data/existing.parquet']).toBeDefined()
      expect(Object.keys(updated.files)).toHaveLength(1)
    })
  })
})
