/**
 * SyncEngine Integration Tests
 *
 * Tests for push/pull/sync operations using MemoryBackend
 * for both local and remote storage.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import { SyncEngine, createSyncEngine, type SyncEngineOptions } from '../../../src/sync/engine'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import type { SyncProgress, SyncManifest } from '../../../src/sync/manifest'
import type { StorageBackend } from '../../../src/types/storage'

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Create a SyncEngine with fresh MemoryBackend instances
 */
function createTestEngine(options?: Partial<SyncEngineOptions>): {
  engine: SyncEngine
  local: MemoryBackend
  remote: MemoryBackend
} {
  const local = new MemoryBackend()
  const remote = new MemoryBackend()

  const engine = createSyncEngine({
    local,
    remote,
    databaseId: 'test-db-123',
    name: 'test-database',
    owner: 'test-user',
    ...options,
  })

  return { engine, local, remote }
}

/**
 * Populate a backend with test files
 */
async function populateBackend(
  backend: StorageBackend,
  files: Record<string, string | Uint8Array>,
  options?: { mtime?: Date }
): Promise<void> {
  for (const [path, content] of Object.entries(files)) {
    const data = typeof content === 'string'
      ? new TextEncoder().encode(content)
      : content
    await backend.write(path, data, { mtime: options?.mtime })
  }
}

/**
 * Create a manifest and save it to a backend
 */
async function saveManifest(
  backend: StorageBackend,
  manifest: SyncManifest
): Promise<void> {
  const data = new TextEncoder().encode(JSON.stringify(manifest, null, 2))
  await backend.write('_meta/manifest.json', data)
}

/**
 * Create a test manifest with files
 */
function createTestManifest(
  files: Record<string, { hash: string; size: number; modifiedAt?: string }>
): SyncManifest {
  const fileEntries: SyncManifest['files'] = {}

  for (const [path, { hash, size, modifiedAt }] of Object.entries(files)) {
    fileEntries[path] = {
      path,
      size,
      hash,
      hashAlgorithm: 'sha256',
      modifiedAt: modifiedAt ?? new Date().toISOString(),
    }
  }

  return {
    version: 1,
    databaseId: 'test-db-123',
    name: 'test-database',
    visibility: 'private',
    lastSyncedAt: new Date().toISOString(),
    files: fileEntries,
  }
}

// =============================================================================
// Tests
// =============================================================================

describe('SyncEngine', () => {
  describe('push()', () => {
    it('should upload new files to remote', async () => {
      const { engine, local, remote } = createTestEngine()

      // Create a local file
      await populateBackend(local, {
        'data/posts/data.parquet': 'local file content',
      })

      // Push to remote
      const result = await engine.push()

      // Verify the file was uploaded
      expect(result.success).toBe(true)
      expect(result.uploaded).toContain('data/posts/data.parquet')
      expect(result.errors).toHaveLength(0)

      // Verify file exists on remote
      const remoteExists = await remote.exists('data/posts/data.parquet')
      expect(remoteExists).toBe(true)

      // Verify content matches
      const remoteContent = await remote.read('data/posts/data.parquet')
      expect(new TextDecoder().decode(remoteContent)).toBe('local file content')
    })

    it('should not upload unchanged files', async () => {
      const { engine, local, remote } = createTestEngine()

      const content = 'same content on both sides'

      // Create identical files on both sides
      await populateBackend(local, {
        'data/posts/data.parquet': content,
      })
      await populateBackend(remote, {
        'data/posts/data.parquet': content,
      })

      // First push to establish baseline
      const firstResult = await engine.push()
      expect(firstResult.success).toBe(true)
      expect(firstResult.uploaded).toContain('data/posts/data.parquet')

      // Second push should find nothing to upload
      const secondResult = await engine.push()

      expect(secondResult.success).toBe(true)
      expect(secondResult.uploaded).toHaveLength(0)
    })

    it('should upload changed files', async () => {
      const { engine, local, remote } = createTestEngine()

      // Create different content locally and remotely
      await populateBackend(local, {
        'data/posts/data.parquet': 'new local content',
      })
      await populateBackend(remote, {
        'data/posts/data.parquet': 'old remote content',
      })

      // Create manifests with different hashes
      const localManifest = createTestManifest({
        'data/posts/data.parquet': {
          hash: 'new-local-hash',
          size: 100,
          modifiedAt: '2024-02-01T00:00:00Z', // Newer
        },
      })

      const remoteManifest = createTestManifest({
        'data/posts/data.parquet': {
          hash: 'old-remote-hash',
          size: 100,
          modifiedAt: '2024-01-01T00:00:00Z', // Older
        },
      })

      await saveManifest(local, localManifest)
      await saveManifest(remote, remoteManifest)

      // Push with local-wins strategy
      const result = await engine.push({ conflictStrategy: 'local-wins' })

      expect(result.success).toBe(true)
      expect(result.uploaded).toContain('data/posts/data.parquet')

      // Verify remote was updated
      const remoteContent = await remote.read('data/posts/data.parquet')
      expect(new TextDecoder().decode(remoteContent)).toBe('new local content')
    })

    it('should return diff without changes in dry run mode', async () => {
      const { engine, local, remote } = createTestEngine()

      // Create a local file
      await populateBackend(local, {
        'data/posts/data.parquet': 'local file content',
      })

      // Dry run push
      const result = await engine.push({ dryRun: true })

      // Should report files to upload
      expect(result.success).toBe(true)
      expect(result.uploaded).toContain('data/posts/data.parquet')

      // But remote should NOT have the file
      const remoteExists = await remote.exists('data/posts/data.parquet')
      expect(remoteExists).toBe(false)
    })

    it('should upload multiple new files', async () => {
      const { engine, local, remote } = createTestEngine()

      // Create multiple local files
      await populateBackend(local, {
        'data/posts/data.parquet': 'posts content',
        'data/users/data.parquet': 'users content',
        'data/comments/data.parquet': 'comments content',
      })

      // Push to remote
      const result = await engine.push()

      expect(result.success).toBe(true)
      expect(result.uploaded).toHaveLength(3)
      expect(result.uploaded).toContain('data/posts/data.parquet')
      expect(result.uploaded).toContain('data/users/data.parquet')
      expect(result.uploaded).toContain('data/comments/data.parquet')
    })
  })

  describe('pull()', () => {
    it('should download new remote files', async () => {
      const { engine, local, remote } = createTestEngine()

      // Create a remote file and manifest
      await populateBackend(remote, {
        'data/posts/data.parquet': 'remote file content',
      })

      const remoteManifest = createTestManifest({
        'data/posts/data.parquet': {
          hash: 'remote-hash',
          size: 100,
        },
      })
      await saveManifest(remote, remoteManifest)

      // Pull from remote
      const result = await engine.pull()

      // Verify the file was downloaded
      expect(result.success).toBe(true)
      expect(result.downloaded).toContain('data/posts/data.parquet')
      expect(result.errors).toHaveLength(0)

      // Verify file exists locally
      const localExists = await local.exists('data/posts/data.parquet')
      expect(localExists).toBe(true)

      // Verify content matches
      const localContent = await local.read('data/posts/data.parquet')
      expect(new TextDecoder().decode(localContent)).toBe('remote file content')
    })

    it('should return error when remote manifest is missing', async () => {
      const { engine } = createTestEngine()

      // Pull without any remote manifest
      const result = await engine.pull()

      expect(result.success).toBe(false)
      expect(result.errors).toHaveLength(1)
      expect(result.errors[0].message).toBe('Remote manifest not found')
    })

    it('should handle dry run mode', async () => {
      const { engine, local, remote } = createTestEngine()

      // Create a remote file and manifest
      await populateBackend(remote, {
        'data/posts/data.parquet': 'remote file content',
      })

      const remoteManifest = createTestManifest({
        'data/posts/data.parquet': {
          hash: 'remote-hash',
          size: 100,
        },
      })
      await saveManifest(remote, remoteManifest)

      // Dry run pull
      const result = await engine.pull({ dryRun: true })

      // Should report files to download
      expect(result.success).toBe(true)
      expect(result.downloaded).toContain('data/posts/data.parquet')

      // But local should NOT have the file
      const localExists = await local.exists('data/posts/data.parquet')
      expect(localExists).toBe(false)
    })

    it('should download multiple files', async () => {
      const { engine, local, remote } = createTestEngine()

      // Create multiple remote files
      await populateBackend(remote, {
        'data/posts/data.parquet': 'posts content',
        'data/users/data.parquet': 'users content',
      })

      const remoteManifest = createTestManifest({
        'data/posts/data.parquet': { hash: 'hash1', size: 100 },
        'data/users/data.parquet': { hash: 'hash2', size: 100 },
      })
      await saveManifest(remote, remoteManifest)

      // Pull from remote
      const result = await engine.pull()

      expect(result.success).toBe(true)
      expect(result.downloaded).toHaveLength(2)
      expect(result.downloaded).toContain('data/posts/data.parquet')
      expect(result.downloaded).toContain('data/users/data.parquet')
    })
  })

  describe('sync()', () => {
    it('should handle bidirectional changes', async () => {
      const { engine, local, remote } = createTestEngine()

      // Local has a file remote doesn't have
      await populateBackend(local, {
        'data/local-only/data.parquet': 'local only content',
      })

      // Remote has a file local doesn't have
      await populateBackend(remote, {
        'data/remote-only/data.parquet': 'remote only content',
      })

      // Set up manifests
      const localManifest = createTestManifest({
        'data/local-only/data.parquet': { hash: 'local-hash', size: 100 },
      })

      const remoteManifest = createTestManifest({
        'data/remote-only/data.parquet': { hash: 'remote-hash', size: 100 },
      })

      await saveManifest(local, localManifest)
      await saveManifest(remote, remoteManifest)

      // Sync
      const result = await engine.sync()

      expect(result.success).toBe(true)
      expect(result.uploaded).toContain('data/local-only/data.parquet')
      expect(result.downloaded).toContain('data/remote-only/data.parquet')

      // Verify both sides have both files
      expect(await local.exists('data/remote-only/data.parquet')).toBe(true)
      expect(await remote.exists('data/local-only/data.parquet')).toBe(true)
    })

    it('should handle dry run mode', async () => {
      const { engine, local, remote } = createTestEngine()

      // Set up files on each side
      await populateBackend(local, {
        'data/local-only/data.parquet': 'local only content',
      })
      await populateBackend(remote, {
        'data/remote-only/data.parquet': 'remote only content',
      })

      const remoteManifest = createTestManifest({
        'data/remote-only/data.parquet': { hash: 'remote-hash', size: 100 },
      })
      await saveManifest(remote, remoteManifest)

      // Dry run sync
      const result = await engine.sync({ dryRun: true })

      // Should report what would happen
      expect(result.success).toBe(true)
      expect(result.uploaded).toContain('data/local-only/data.parquet')
      expect(result.downloaded).toContain('data/remote-only/data.parquet')

      // But no actual changes
      expect(await local.exists('data/remote-only/data.parquet')).toBe(false)
      expect(await remote.exists('data/local-only/data.parquet')).toBe(false)
    })
  })

  describe('status()', () => {
    it('should return correct diff for synced state', async () => {
      const { engine, local, remote } = createTestEngine()

      const content = 'same content'

      // Create identical files on both sides
      await populateBackend(local, {
        'data/posts/data.parquet': content,
      })
      await populateBackend(remote, {
        'data/posts/data.parquet': content,
      })

      // Push first to sync
      const pushResult = await engine.push()
      expect(pushResult.success).toBe(true)

      // Now check status - should be synced
      const status = await engine.status()

      expect(status.isSynced).toBe(true)
      expect(status.diff.toUpload).toHaveLength(0)
      expect(status.diff.toDownload).toHaveLength(0)
      expect(status.diff.conflicts).toHaveLength(0)
    })

    it('should return correct diff for unsynced state', async () => {
      const { engine, local, remote } = createTestEngine()

      // Local has file, remote doesn't
      await populateBackend(local, {
        'data/posts/data.parquet': 'local content',
      })

      const localManifest = createTestManifest({
        'data/posts/data.parquet': { hash: 'local-hash', size: 100 },
      })
      await saveManifest(local, localManifest)

      // Remote has different file
      await populateBackend(remote, {
        'data/users/data.parquet': 'remote content',
      })

      const remoteManifest = createTestManifest({
        'data/users/data.parquet': { hash: 'remote-hash', size: 100 },
      })
      await saveManifest(remote, remoteManifest)

      const status = await engine.status()

      expect(status.isSynced).toBe(false)
      expect(status.diff.toUpload).toHaveLength(1)
      expect(status.diff.toUpload[0].path).toBe('data/posts/data.parquet')
      expect(status.diff.toDownload).toHaveLength(1)
      expect(status.diff.toDownload[0].path).toBe('data/users/data.parquet')
    })

    it('should detect conflicts', async () => {
      const { engine, local, remote } = createTestEngine()

      // Same file with different content
      await populateBackend(local, {
        'data/posts/data.parquet': 'local content',
      })
      await populateBackend(remote, {
        'data/posts/data.parquet': 'remote content',
      })

      const localManifest = createTestManifest({
        'data/posts/data.parquet': {
          hash: 'local-hash',
          size: 100,
          modifiedAt: '2024-01-01T00:00:00Z',
        },
      })

      const remoteManifest = createTestManifest({
        'data/posts/data.parquet': {
          hash: 'remote-hash',
          size: 100,
          modifiedAt: '2024-01-02T00:00:00Z',
        },
      })

      await saveManifest(local, localManifest)
      await saveManifest(remote, remoteManifest)

      const status = await engine.status()

      expect(status.isSynced).toBe(false)
      expect(status.diff.conflicts).toHaveLength(1)
      expect(status.diff.conflicts[0].path).toBe('data/posts/data.parquet')
    })
  })

  describe('conflict resolution strategies', () => {
    let testSetup: {
      engine: SyncEngine
      local: MemoryBackend
      remote: MemoryBackend
    }

    beforeEach(async () => {
      testSetup = createTestEngine()
      const { local, remote } = testSetup

      // Set up a conflict scenario
      await populateBackend(local, {
        'data/posts/data.parquet': 'local version',
      })
      await populateBackend(remote, {
        'data/posts/data.parquet': 'remote version',
      })

      const localManifest = createTestManifest({
        'data/posts/data.parquet': {
          hash: 'local-hash',
          size: 100,
          modifiedAt: '2024-01-02T00:00:00Z', // Newer
        },
      })

      const remoteManifest = createTestManifest({
        'data/posts/data.parquet': {
          hash: 'remote-hash',
          size: 100,
          modifiedAt: '2024-01-01T00:00:00Z', // Older
        },
      })

      await saveManifest(local, localManifest)
      await saveManifest(remote, remoteManifest)
    })

    it('should resolve with local-wins strategy', async () => {
      const { engine, remote } = testSetup

      const result = await engine.push({ conflictStrategy: 'local-wins' })

      expect(result.success).toBe(true)
      expect(result.uploaded).toContain('data/posts/data.parquet')

      // Remote should have local content
      const content = await remote.read('data/posts/data.parquet')
      expect(new TextDecoder().decode(content)).toBe('local version')
    })

    it('should resolve with remote-wins strategy', async () => {
      const { engine, local } = testSetup

      const result = await engine.pull({ conflictStrategy: 'remote-wins' })

      expect(result.success).toBe(true)
      expect(result.downloaded).toContain('data/posts/data.parquet')

      // Local should have remote content
      const content = await local.read('data/posts/data.parquet')
      expect(new TextDecoder().decode(content)).toBe('remote version')
    })

    it('should resolve with newest strategy (local is newer)', async () => {
      const { engine, remote } = testSetup

      const result = await engine.sync({ conflictStrategy: 'newest' })

      expect(result.success).toBe(true)
      // Local is newer (2024-01-02), so it should be uploaded
      expect(result.uploaded).toContain('data/posts/data.parquet')

      // Remote should have local content
      const content = await remote.read('data/posts/data.parquet')
      expect(new TextDecoder().decode(content)).toBe('local version')
    })

    it('should resolve with newest strategy (remote is newer)', async () => {
      // Create fresh engine for this test to avoid beforeEach conflicts
      const { engine, local, remote } = createTestEngine()

      // Set up a conflict scenario where remote is newer
      // Use explicit mtimes so that file stats match manifest timestamps
      const localMtime = new Date('2024-01-01T00:00:00Z')
      const remoteMtime = new Date('2024-01-03T00:00:00Z')

      await populateBackend(local, {
        'data/posts/data.parquet': 'local version',
      }, { mtime: localMtime })
      await populateBackend(remote, {
        'data/posts/data.parquet': 'remote version',
      }, { mtime: remoteMtime })

      const localManifest = createTestManifest({
        'data/posts/data.parquet': {
          hash: 'local-hash',
          size: 100,
          modifiedAt: localMtime.toISOString(), // Older
        },
      })

      const remoteManifest = createTestManifest({
        'data/posts/data.parquet': {
          hash: 'remote-hash',
          size: 100,
          modifiedAt: remoteMtime.toISOString(), // Newer
        },
      })

      await saveManifest(local, localManifest)
      await saveManifest(remote, remoteManifest)

      const result = await engine.sync({ conflictStrategy: 'newest' })

      expect(result.success).toBe(true)
      // Remote is newer (2024-01-03), so it should be downloaded
      expect(result.downloaded).toContain('data/posts/data.parquet')

      // Local should have remote content
      const content = await local.read('data/posts/data.parquet')
      expect(new TextDecoder().decode(content)).toBe('remote version')
    })

    it('should leave conflicts pending with manual strategy', async () => {
      const { engine } = testSetup

      const result = await engine.sync({ conflictStrategy: 'manual' })

      expect(result.success).toBe(true)
      expect(result.conflictsPending).toHaveLength(1)
      expect(result.conflictsPending[0].path).toBe('data/posts/data.parquet')
      expect(result.uploaded).not.toContain('data/posts/data.parquet')
      expect(result.downloaded).not.toContain('data/posts/data.parquet')
    })
  })

  describe('progress callback', () => {
    it('should call progress callback during push', async () => {
      const progressEvents: SyncProgress[] = []
      const onProgress = vi.fn((progress: SyncProgress) => {
        progressEvents.push({ ...progress })
      })

      const { engine, local } = createTestEngine({ onProgress })

      // Create files to upload
      await populateBackend(local, {
        'data/posts/data.parquet': 'content 1',
        'data/users/data.parquet': 'content 2',
      })

      await engine.push()

      expect(onProgress).toHaveBeenCalled()
      expect(progressEvents.some(p => p.operation === 'uploading')).toBe(true)
      expect(progressEvents.some(p => p.currentFile === 'data/posts/data.parquet')).toBe(true)
    })

    it('should call progress callback during pull', async () => {
      const progressEvents: SyncProgress[] = []
      const onProgress = vi.fn((progress: SyncProgress) => {
        progressEvents.push({ ...progress })
      })

      const { engine, remote } = createTestEngine({ onProgress })

      // Create files on remote
      await populateBackend(remote, {
        'data/posts/data.parquet': 'content 1',
      })

      const remoteManifest = createTestManifest({
        'data/posts/data.parquet': { hash: 'hash1', size: 100 },
      })
      await saveManifest(remote, remoteManifest)

      await engine.pull()

      expect(onProgress).toHaveBeenCalled()
      expect(progressEvents.some(p => p.operation === 'downloading')).toBe(true)
    })

    it('should report progress with file counts', async () => {
      const progressEvents: SyncProgress[] = []
      const onProgress = (progress: SyncProgress) => {
        progressEvents.push({ ...progress })
      }

      const { engine, local } = createTestEngine({ onProgress })

      // Create multiple files
      await populateBackend(local, {
        'data/file1.parquet': 'content 1',
        'data/file2.parquet': 'content 2',
        'data/file3.parquet': 'content 3',
      })

      await engine.push()

      // Should have progress events with increasing processed count
      const uploadEvents = progressEvents.filter(p => p.operation === 'uploading')
      expect(uploadEvents.length).toBeGreaterThan(0)

      // Verify total matches number of files
      const firstEvent = uploadEvents[0]
      expect(firstEvent.total).toBe(3)
    })
  })

  describe('error handling', () => {
    it('should handle upload errors gracefully', async () => {
      const { engine, local, remote } = createTestEngine()

      // Create a file locally
      await populateBackend(local, {
        'data/posts/data.parquet': 'local content',
      })

      // Mock remote write to fail during upload
      vi.spyOn(remote, 'write').mockRejectedValueOnce(new Error('Simulated upload error'))

      const result = await engine.push()

      // Should have errors for the failed file (either during upload or manifest save)
      // Since write is mocked to fail once, either the file upload or manifest save will fail
      expect(result.errors.length).toBeGreaterThan(0)
      expect(result.errors.some(e => e.operation === 'upload')).toBe(true)

      vi.restoreAllMocks()
    })

    it('should continue after single file upload error', async () => {
      const { engine, local, remote } = createTestEngine()

      // Create files locally
      await populateBackend(local, {
        'data/posts/data.parquet': 'posts content',
        'data/users/data.parquet': 'users content',
      })

      // Mock remote write to fail for specific file
      const originalWrite = remote.write.bind(remote)
      vi.spyOn(remote, 'write').mockImplementation(async (path: string, data: Uint8Array, options?: any) => {
        if (path === 'data/posts/data.parquet') {
          throw new Error('Simulated upload error')
        }
        return originalWrite(path, data, options)
      })

      const result = await engine.push()

      // Should have error for the failed file
      expect(result.errors.some(e => e.path === 'data/posts/data.parquet')).toBe(true)
      expect(result.errors.some(e => e.operation === 'upload')).toBe(true)

      // But other file should have been uploaded
      expect(result.uploaded).toContain('data/users/data.parquet')

      vi.restoreAllMocks()
    })

    it('should handle download errors gracefully', async () => {
      const { engine, local, remote } = createTestEngine()

      // Create files on remote
      await populateBackend(remote, {
        'data/posts/data.parquet': 'remote content',
      })

      const remoteManifest = createTestManifest({
        'data/posts/data.parquet': { hash: 'hash1', size: 100 },
      })
      await saveManifest(remote, remoteManifest)

      // Mock local write to fail during download
      const originalWrite = local.write.bind(local)
      vi.spyOn(local, 'write').mockImplementation(async (path: string, data: Uint8Array, options?: any) => {
        if (path === 'data/posts/data.parquet') {
          throw new Error('Simulated write error')
        }
        return originalWrite(path, data, options)
      })

      const result = await engine.pull()

      // Should have errors for the failed download
      expect(result.errors.length).toBeGreaterThan(0)
      expect(result.errors.some(e => e.operation === 'download')).toBe(true)

      vi.restoreAllMocks()
    })

    it('should continue after single file download error', async () => {
      const { engine, local, remote } = createTestEngine()

      // Create files on remote
      await populateBackend(remote, {
        'data/posts/data.parquet': 'posts content',
        'data/users/data.parquet': 'users content',
      })

      const remoteManifest = createTestManifest({
        'data/posts/data.parquet': { hash: 'hash1', size: 100 },
        'data/users/data.parquet': { hash: 'hash2', size: 100 },
      })
      await saveManifest(remote, remoteManifest)

      // Mock local write to fail for specific file
      const originalWrite = local.write.bind(local)
      vi.spyOn(local, 'write').mockImplementation(async (path: string, data: Uint8Array, options?: any) => {
        if (path === 'data/posts/data.parquet') {
          throw new Error('Simulated write error')
        }
        return originalWrite(path, data, options)
      })

      const result = await engine.pull()

      // Should have error for the failed file
      expect(result.errors.some(e => e.path === 'data/posts/data.parquet')).toBe(true)

      // But other file should have been downloaded
      expect(result.downloaded).toContain('data/users/data.parquet')

      vi.restoreAllMocks()
    })
  })

  describe('manifest management', () => {
    it('should save manifest after successful push', async () => {
      const { engine, local, remote } = createTestEngine()

      await populateBackend(local, {
        'data/posts/data.parquet': 'content',
      })

      await engine.push()

      // Both local and remote should have manifests
      expect(await local.exists('_meta/manifest.json')).toBe(true)
      expect(await remote.exists('_meta/manifest.json')).toBe(true)
    })

    it('should save manifest after successful pull', async () => {
      const { engine, local, remote } = createTestEngine()

      await populateBackend(remote, {
        'data/posts/data.parquet': 'content',
      })

      const remoteManifest = createTestManifest({
        'data/posts/data.parquet': { hash: 'hash1', size: 100 },
      })
      await saveManifest(remote, remoteManifest)

      await engine.pull()

      // Local should have manifest
      expect(await local.exists('_meta/manifest.json')).toBe(true)
    })

    it('should update lastSyncedAt after sync', async () => {
      const { engine, local, remote } = createTestEngine()

      await populateBackend(local, {
        'data/posts/data.parquet': 'content',
      })

      const beforeSync = new Date().toISOString()

      // Small delay to ensure timestamp difference
      await new Promise(resolve => setTimeout(resolve, 10))

      const result = await engine.push()

      expect(result.manifest.lastSyncedAt).toBeDefined()
      expect(new Date(result.manifest.lastSyncedAt).getTime())
        .toBeGreaterThanOrEqual(new Date(beforeSync).getTime())
    })

    it('should ignore manifest file in file listing', async () => {
      const { engine, local, remote } = createTestEngine()

      // Create a manifest and a data file
      await populateBackend(local, {
        '_meta/manifest.json': '{"version": 1}',
        'data/posts/data.parquet': 'content',
      })

      const result = await engine.push()

      // Should only upload the data file, not the manifest itself
      expect(result.uploaded).toContain('data/posts/data.parquet')
      expect(result.uploaded).not.toContain('_meta/manifest.json')
    })
  })

  describe('edge cases', () => {
    it('should handle empty local storage on push', async () => {
      const { engine } = createTestEngine()

      const result = await engine.push()

      expect(result.success).toBe(true)
      expect(result.uploaded).toHaveLength(0)
      expect(result.errors).toHaveLength(0)
    })

    it('should handle empty remote storage on pull (with manifest)', async () => {
      const { engine, remote } = createTestEngine()

      // Create empty manifest on remote
      const remoteManifest = createTestManifest({})
      await saveManifest(remote, remoteManifest)

      const result = await engine.pull()

      expect(result.success).toBe(true)
      expect(result.downloaded).toHaveLength(0)
      expect(result.errors).toHaveLength(0)
    })

    it('should handle special characters in file paths', async () => {
      const { engine, local, remote } = createTestEngine()

      await populateBackend(local, {
        'data/my-db/file-1.parquet': 'content with dash',
        'data/my_db/file_2.parquet': 'content with underscore',
      })

      const result = await engine.push()

      expect(result.success).toBe(true)
      expect(result.uploaded).toContain('data/my-db/file-1.parquet')
      expect(result.uploaded).toContain('data/my_db/file_2.parquet')
    })

    it('should handle binary file content', async () => {
      const { engine, local, remote } = createTestEngine()

      // Create binary content (simulating parquet file)
      const binaryContent = new Uint8Array([
        0x50, 0x41, 0x52, 0x31, // PAR1 magic
        0x00, 0x01, 0x02, 0x03,
        0xFF, 0xFE, 0xFD, 0xFC,
      ])

      await local.write('data/binary.parquet', binaryContent)

      const result = await engine.push()

      expect(result.success).toBe(true)
      expect(result.uploaded).toContain('data/binary.parquet')

      // Verify binary content was preserved
      const remoteContent = await remote.read('data/binary.parquet')
      expect(remoteContent).toEqual(binaryContent)
    })

    it('should skip .git and other ignored paths', async () => {
      const { engine, local } = createTestEngine()

      await populateBackend(local, {
        '.git/objects/abc123': 'git object',
        '.DS_Store': 'mac file',
        'node_modules/package/index.js': 'node module',
        'data/posts/data.parquet': 'actual data',
      })

      const result = await engine.push()

      expect(result.success).toBe(true)
      expect(result.uploaded).toContain('data/posts/data.parquet')
      expect(result.uploaded).not.toContain('.git/objects/abc123')
      expect(result.uploaded).not.toContain('.DS_Store')
      expect(result.uploaded).not.toContain('node_modules/package/index.js')
    })

    it('should skip lock files in _meta/locks', async () => {
      const { engine, local } = createTestEngine()

      // First create some data and do a push
      await populateBackend(local, {
        'data/posts/data.parquet': 'actual data',
      })

      const result = await engine.push()

      expect(result.success).toBe(true)
      expect(result.uploaded).toContain('data/posts/data.parquet')

      // Verify lock files are created in _meta/locks but not uploaded
      // The lock files should exist locally but not be in the uploaded list
      expect(result.uploaded.filter(f => f.startsWith('_meta/locks/'))).toHaveLength(0)
    })
  })

  describe('timeout and lock handling', () => {
    it('should acquire lock before push operation', async () => {
      const { engine, local } = createTestEngine()

      await populateBackend(local, {
        'data/posts/data.parquet': 'content',
      })

      // First push should succeed
      const result = await engine.push()
      expect(result.success).toBe(true)

      // Lock should be released after operation completes
      // (subsequent push should also succeed)
      const result2 = await engine.push()
      expect(result2.success).toBe(true)
    })

    it('should throw TimeoutError when operation times out', async () => {
      const local = new MemoryBackend()
      const remote = new MemoryBackend()

      // Create a slow remote backend
      const originalWrite = remote.write.bind(remote)
      vi.spyOn(remote, 'write').mockImplementation(async (path, data, options) => {
        // Simulate slow write (100ms)
        await new Promise(resolve => setTimeout(resolve, 100))
        return originalWrite(path, data, options)
      })

      const engine = createSyncEngine({
        local,
        remote,
        databaseId: 'test-db',
        name: 'test',
        timeout: 50, // Very short timeout
      })

      await populateBackend(local, {
        'data/posts/data.parquet': 'content',
      })

      // Should timeout before the slow write completes
      await expect(engine.push()).rejects.toThrow(/timed out/)

      vi.restoreAllMocks()
    })

    it('should use custom timeout from options', async () => {
      const local = new MemoryBackend()
      const remote = new MemoryBackend()

      const engine = createSyncEngine({
        local,
        remote,
        databaseId: 'test-db',
        name: 'test',
        timeout: 60000, // 60 second timeout
      })

      await populateBackend(local, {
        'data/posts/data.parquet': 'content',
      })

      // Should complete successfully with generous timeout
      const result = await engine.push()
      expect(result.success).toBe(true)
    })

    it('should release lock even when operation fails', async () => {
      const local = new MemoryBackend()
      const remote = new MemoryBackend()

      const engine = createSyncEngine({
        local,
        remote,
        databaseId: 'test-db',
        name: 'test',
      })

      await populateBackend(local, {
        'data/posts/data.parquet': 'content',
      })

      // Make remote write fail for data files only (not lock files or manifest)
      const originalWrite = remote.write.bind(remote)
      const writeSpy = vi.spyOn(remote, 'write').mockImplementation(async (path, data, options) => {
        if (path.startsWith('data/')) {
          throw new Error('Write failed')
        }
        return originalWrite(path, data, options)
      })

      // First push will have errors but should release lock
      const result = await engine.push()
      expect(result.errors.length).toBeGreaterThan(0)
      expect(result.errors.some(e => e.operation === 'upload')).toBe(true)

      // Restore mock before second push
      writeSpy.mockRestore()

      // Second push should be able to acquire lock (proves first released it)
      // This should succeed since the mock is restored
      const result2 = await engine.push()
      expect(result2.success).toBe(true)
    })
  })
})
