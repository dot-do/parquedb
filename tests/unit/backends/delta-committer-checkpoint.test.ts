/**
 * Tests for DeltaCommitter checkpoint functionality
 *
 * Verifies that the DeltaCommitter properly creates checkpoint files:
 * - After every N commits (configurable, default 10)
 * - Checkpoint is Parquet file containing all active add actions
 * - _last_checkpoint file points to latest checkpoint
 * - Checkpoint content follows Delta Lake spec
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import {
  DeltaCommitter,
  createDeltaCommitter,
} from '../../../src/backends/delta-commit'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import { readParquet } from '../../../src/parquet/reader'

/** Helper to parse JSON-encoded action columns */
function parseActionColumn<T>(value: string | null | undefined): T | null {
  if (value === null || value === undefined) return null
  try {
    return JSON.parse(value) as T
  } catch {
    return null
  }
}

describe('DeltaCommitter Checkpoint', () => {
  let storage: MemoryBackend
  let committer: DeltaCommitter

  beforeEach(async () => {
    storage = new MemoryBackend()
    committer = createDeltaCommitter({
      storage,
      tableLocation: 'test-table',
      checkpointInterval: 10, // Create checkpoint every 10 commits
    })
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  describe('Checkpoint creation', () => {
    it('should create checkpoint after checkpointInterval commits', async () => {
      // Create commits (need 11 total to trigger checkpoint at version 10)
      for (let i = 0; i < 11; i++) {
        await committer.commitDataFile({
          path: `file${i}.parquet`,
          size: 1000 * (i + 1),
        })
      }

      // Checkpoint should exist at version 10
      const checkpointExists = await storage.exists(
        'test-table/_delta_log/00000000000000000010.checkpoint.parquet'
      )
      expect(checkpointExists).toBe(true)

      // _last_checkpoint should exist and point to version 10
      const lastCheckpointExists = await storage.exists(
        'test-table/_delta_log/_last_checkpoint'
      )
      expect(lastCheckpointExists).toBe(true)
    })

    it('should not create checkpoint before reaching interval', async () => {
      // Create 9 commits (not enough for checkpoint)
      for (let i = 0; i < 9; i++) {
        await committer.commitDataFile({
          path: `file${i}.parquet`,
          size: 1000,
        })
      }

      // No checkpoint should exist
      const files = await storage.list('test-table/_delta_log/')
      const checkpointFiles = files.files.filter(f => f.includes('.checkpoint.parquet'))
      expect(checkpointFiles.length).toBe(0)
    })

    it('should create multiple checkpoints at interval boundaries', async () => {
      // Create 21 commits to trigger checkpoints at version 10 and 20
      for (let i = 0; i < 21; i++) {
        await committer.commitDataFile({
          path: `file${i}.parquet`,
          size: 1000,
        })
      }

      // Both checkpoints should exist
      const checkpoint10Exists = await storage.exists(
        'test-table/_delta_log/00000000000000000010.checkpoint.parquet'
      )
      const checkpoint20Exists = await storage.exists(
        'test-table/_delta_log/00000000000000000020.checkpoint.parquet'
      )

      expect(checkpoint10Exists).toBe(true)
      expect(checkpoint20Exists).toBe(true)

      // _last_checkpoint should point to version 20
      const lastCheckpointData = await storage.read('test-table/_delta_log/_last_checkpoint')
      const lastCheckpoint = JSON.parse(new TextDecoder().decode(lastCheckpointData))
      expect(lastCheckpoint.version).toBe(20)
    })

    it('should respect custom checkpointInterval', async () => {
      const customCommitter = createDeltaCommitter({
        storage,
        tableLocation: 'custom-table',
        checkpointInterval: 5,
      })

      // Create 6 commits to trigger checkpoint at version 5
      for (let i = 0; i < 6; i++) {
        await customCommitter.commitDataFile({
          path: `file${i}.parquet`,
          size: 1000,
        })
      }

      const checkpointExists = await storage.exists(
        'custom-table/_delta_log/00000000000000000005.checkpoint.parquet'
      )
      expect(checkpointExists).toBe(true)
    })

    it('should disable checkpoints when interval is 0', async () => {
      // Create a committer with checkpointInterval = 0 (disabled)
      const noCheckpointCommitter = createDeltaCommitter({
        storage, // Use the shared storage from beforeEach
        tableLocation: 'no-cp-table',
        checkpointInterval: 0,
      })

      // Create commits at checkpoint boundary (10) and beyond
      // This should NOT create a checkpoint because interval is 0
      for (let i = 0; i < 11; i++) {
        await noCheckpointCommitter.commitDataFile({
          path: `file${i}.parquet`,
          size: 1000,
        })
      }

      // No checkpoint should exist even at version 10
      const checkpointExists = await storage.exists(
        'no-cp-table/_delta_log/00000000000000000010.checkpoint.parquet'
      )
      expect(checkpointExists).toBe(false)

      // _last_checkpoint should not exist either
      const lastCheckpointExists = await storage.exists(
        'no-cp-table/_delta_log/_last_checkpoint'
      )
      expect(lastCheckpointExists).toBe(false)
    })
  })

  describe('maybeCreateCheckpoint', () => {
    it('should allow explicit checkpoint creation with force option', async () => {
      // Create a few commits
      for (let i = 0; i < 5; i++) {
        await committer.commitDataFile({
          path: `file${i}.parquet`,
          size: 1000,
        })
      }

      // Explicitly create checkpoint with force
      const result = await committer.maybeCreateCheckpoint({ force: true })

      expect(result.created).toBe(true)
      // First commit creates table at version 0, then 4 more commits = version 4
      expect(result.version).toBe(4)

      const checkpointExists = await storage.exists(
        'test-table/_delta_log/00000000000000000004.checkpoint.parquet'
      )
      expect(checkpointExists).toBe(true)
    })

    it('should return false when no commits exist', async () => {
      const emptyCommitter = createDeltaCommitter({
        storage,
        tableLocation: 'empty-table',
      })

      const result = await emptyCommitter.maybeCreateCheckpoint()

      expect(result.created).toBe(false)
      expect(result.version).toBe(-1)
    })

    it('should allow forcing checkpoint at any version', async () => {
      // Create commits
      for (let i = 0; i < 3; i++) {
        await committer.commitDataFile({
          path: `file${i}.parquet`,
          size: 1000,
        })
      }

      // Force checkpoint creation
      const result = await committer.maybeCreateCheckpoint({ force: true })

      expect(result.created).toBe(true)
      // First commit creates table at version 0, then 2 more commits = version 2
      expect(result.version).toBe(2)
    })
  })

  describe('Checkpoint content', () => {
    it('should contain all active add actions', async () => {
      // Create commits to trigger checkpoint
      for (let i = 0; i < 11; i++) {
        await committer.commitDataFile({
          path: `file${i}.parquet`,
          size: 1000 * (i + 1),
        })
      }

      // Read checkpoint and verify content
      const rows = await readParquet<{
        add?: string | null
        remove?: string | null
        protocol?: string | null
        metaData?: string | null
      }>(storage, 'test-table/_delta_log/00000000000000000010.checkpoint.parquet')

      // Should have protocol, metaData, and add actions
      const addRows = rows.filter(r => r.add !== null && r.add !== undefined)
      expect(addRows.length).toBe(11) // 11 data files

      // Verify each add action has the expected structure
      for (let i = 0; i < 11; i++) {
        const addAction = parseActionColumn<{
          path: string
          size: number
          modificationTime: number
          dataChange: boolean
        }>(addRows[i]!.add)

        expect(addAction).not.toBeNull()
        expect(addAction!.path).toBe(`file${i}.parquet`)
        expect(addAction!.size).toBe(1000 * (i + 1))
      }
    })

    it('should exclude removed files from checkpoint', async () => {
      // First, create some data files via direct storage write + commit
      // (simulating normal delta operations)
      await committer.ensureTable() // version 0

      // Add 5 files (versions 1-5)
      for (let i = 0; i < 5; i++) {
        await committer.commitDataFile({
          path: `file${i}.parquet`,
          size: 1000,
        })
      }

      // Commit a remove action for file0 and file1 (version 6)
      const currentVersion = await committer.getCurrentVersion()
      await committer.commitRemoveFiles(
        ['file0.parquet', 'file1.parquet'],
        currentVersion
      )

      // Add more files (versions 7-11)
      // This should trigger checkpoint at version 10
      for (let i = 5; i < 10; i++) {
        await committer.commitDataFile({
          path: `file${i}.parquet`,
          size: 1000,
        })
      }

      // Checkpoint should have been auto-created at version 10
      // Read checkpoint
      const rows = await readParquet<{
        add?: string | null
      }>(storage, 'test-table/_delta_log/00000000000000000010.checkpoint.parquet')

      const addRows = rows.filter(r => r.add !== null && r.add !== undefined)

      // Version 10 state:
      // - versions 1-5: 5 files added (file0-file4)
      // - version 6: file0 and file1 removed (3 remaining: file2, file3, file4)
      // - versions 7-10: 4 files added (file5, file6, file7, file8)
      // Total: 7 active files at version 10
      expect(addRows.length).toBe(7)

      // file0 and file1 should not be in checkpoint
      const paths = addRows.map(r => {
        const action = parseActionColumn<{ path: string }>(r.add)
        return action?.path
      })
      expect(paths).not.toContain('file0.parquet')
      expect(paths).not.toContain('file1.parquet')
    })

    it('should include protocol and metadata in checkpoint', async () => {
      // Create commits to trigger checkpoint
      for (let i = 0; i < 11; i++) {
        await committer.commitDataFile({
          path: `file${i}.parquet`,
          size: 1000,
        })
      }

      const rows = await readParquet<{
        protocol?: string | null
        metaData?: string | null
      }>(storage, 'test-table/_delta_log/00000000000000000010.checkpoint.parquet')

      // Should have exactly one protocol row
      const protocolRows = rows.filter(r => r.protocol !== null && r.protocol !== undefined)
      expect(protocolRows.length).toBe(1)

      const protocol = parseActionColumn<{
        minReaderVersion: number
        minWriterVersion: number
      }>(protocolRows[0]!.protocol)
      expect(protocol!.minReaderVersion).toBeGreaterThanOrEqual(1)
      expect(protocol!.minWriterVersion).toBeGreaterThanOrEqual(1)

      // Should have exactly one metaData row
      const metaDataRows = rows.filter(r => r.metaData !== null && r.metaData !== undefined)
      expect(metaDataRows.length).toBe(1)
    })
  })

  describe('_last_checkpoint format', () => {
    it('should contain valid JSON with version and size', async () => {
      // Create commits to trigger checkpoint
      for (let i = 0; i < 11; i++) {
        await committer.commitDataFile({
          path: `file${i}.parquet`,
          size: 1000,
        })
      }

      const lastCheckpointData = await storage.read('test-table/_delta_log/_last_checkpoint')
      const lastCheckpoint = JSON.parse(new TextDecoder().decode(lastCheckpointData))

      expect(typeof lastCheckpoint.version).toBe('number')
      expect(typeof lastCheckpoint.size).toBe('number')
      expect(lastCheckpoint.version).toBe(10)
      expect(lastCheckpoint.size).toBeGreaterThan(0)
    })

    it('should match the checkpoint file that exists', async () => {
      // Create commits to trigger checkpoint
      for (let i = 0; i < 11; i++) {
        await committer.commitDataFile({
          path: `file${i}.parquet`,
          size: 1000,
        })
      }

      const lastCheckpointData = await storage.read('test-table/_delta_log/_last_checkpoint')
      const lastCheckpoint = JSON.parse(new TextDecoder().decode(lastCheckpointData))

      // The checkpoint file should exist
      const checkpointPath = `test-table/_delta_log/${lastCheckpoint.version.toString().padStart(20, '0')}.checkpoint.parquet`
      const exists = await storage.exists(checkpointPath)
      expect(exists).toBe(true)

      // The size should match number of actions
      const rows = await readParquet(storage, checkpointPath)
      expect(rows.length).toBe(lastCheckpoint.size)
    })
  })

  describe('Reader optimization', () => {
    it('getCurrentVersion should use checkpoint for faster version lookup', async () => {
      // Create commits to trigger checkpoint at version 10
      // First commit creates table at version 0, so we need 11 commits total
      for (let i = 0; i < 11; i++) {
        await committer.commitDataFile({
          path: `file${i}.parquet`,
          size: 1000,
        })
      }
      // Now we're at version 10 (0-10 = 11 commits)

      // Create a few more commits after checkpoint (versions 11, 12, 13)
      for (let i = 11; i < 14; i++) {
        await committer.commitDataFile({
          path: `file${i}.parquet`,
          size: 1000,
        })
      }

      // Create a new committer to test reading
      const readCommitter = createDeltaCommitter({
        storage,
        tableLocation: 'test-table',
      })

      // It should correctly determine version 13 using checkpoint
      // (11 + 3 = 14 commits total, versions 0-13)
      const version = await readCommitter.getCurrentVersion()
      expect(version).toBe(13)
    })
  })

  describe('Integration with compaction workflow', () => {
    it('should trigger checkpoint after compaction commits reach threshold', async () => {
      const compactionCommitter = createDeltaCommitter({
        storage,
        tableLocation: 'compaction-table',
        checkpointInterval: 5,
      })

      // Simulate compaction workflow committing files
      for (let i = 0; i < 6; i++) {
        await compactionCommitter.commitDataFiles([
          { path: `compacted-${i}.parquet`, size: 50000, dataChange: false },
        ])
      }

      // Checkpoint should exist
      const checkpointExists = await storage.exists(
        'compaction-table/_delta_log/00000000000000000005.checkpoint.parquet'
      )
      expect(checkpointExists).toBe(true)
    })
  })
})
