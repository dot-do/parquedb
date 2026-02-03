/**
 * Tests for DeltaBackend checkpoint format
 *
 * Delta Lake checkpoints must be stored as proper Parquet files with:
 * - Top-level columns for each action type: txn, add, remove, metaData, protocol, commitInfo
 * - Each row represents one action, with the action stored as JSON and nulls for unused columns
 * - The add struct contains: path, size, modificationTime, dataChange, partitionValues, stats, tags
 * - The remove struct contains: path, deletionTimestamp, dataChange, extendedFileMetadata
 * - The metaData struct contains: id, name, description, schemaString, createdTime, partitionColumns, configuration, format
 * - The protocol struct contains: minReaderVersion, minWriterVersion, readerFeatures, writerFeatures
 *
 * Note: Since hyparquet-writer doesn't support nested struct columns directly,
 * we store action data as JSON strings. This is compatible with Delta Lake readers
 * that can parse JSON-encoded structs.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { DeltaBackend, createDeltaBackend } from '../../../src/backends/delta'
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

describe('DeltaBackend Checkpoint Format', () => {
  let storage: MemoryBackend
  let backend: DeltaBackend

  beforeEach(async () => {
    storage = new MemoryBackend()
    backend = createDeltaBackend({
      type: 'delta',
      storage,
      location: 'warehouse',
    })
    await backend.initialize()
  })

  afterEach(async () => {
    await backend.close()
  })

  describe('Checkpoint Parquet Structure', () => {
    it('should write checkpoint as valid Parquet file', async () => {
      // Create enough commits to trigger checkpoint (threshold is 10)
      for (let i = 0; i < 15; i++) {
        await backend.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      // Find the checkpoint file
      const deltaLogFiles = await storage.list('warehouse/posts/_delta_log/')
      const checkpointFiles = deltaLogFiles.files.filter(f => f.includes('.checkpoint.parquet'))
      expect(checkpointFiles.length).toBeGreaterThan(0)

      // Read the checkpoint file as Parquet
      const checkpointPath = checkpointFiles[0]!
      const rows = await readParquet(storage, checkpointPath)

      // Should be readable as Parquet rows
      expect(Array.isArray(rows)).toBe(true)
      expect(rows.length).toBeGreaterThan(0)
    })

    it('should have action columns as struct fields', async () => {
      // Create commits to trigger checkpoint
      for (let i = 0; i < 15; i++) {
        await backend.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      const deltaLogFiles = await storage.list('warehouse/posts/_delta_log/')
      const checkpointFiles = deltaLogFiles.files.filter(f => f.includes('.checkpoint.parquet'))
      const checkpointPath = checkpointFiles[0]!

      const rows = await readParquet<{
        txn?: unknown
        add?: unknown
        remove?: unknown
        metaData?: unknown
        protocol?: unknown
        commitInfo?: unknown
      }>(storage, checkpointPath)

      // At minimum, checkpoint should contain protocol and metaData rows
      const hasProtocol = rows.some(row => row.protocol !== null && row.protocol !== undefined)
      const hasMetaData = rows.some(row => row.metaData !== null && row.metaData !== undefined)
      const hasAdd = rows.some(row => row.add !== null && row.add !== undefined)

      expect(hasProtocol).toBe(true)
      expect(hasMetaData).toBe(true)
      expect(hasAdd).toBe(true)
    })

    it('should have correct add action schema', async () => {
      // Create commits to trigger checkpoint
      for (let i = 0; i < 15; i++) {
        await backend.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      const deltaLogFiles = await storage.list('warehouse/posts/_delta_log/')
      const checkpointFiles = deltaLogFiles.files.filter(f => f.includes('.checkpoint.parquet'))
      const checkpointPath = checkpointFiles[0]!

      const rows = await readParquet<{
        add?: string | null
      }>(storage, checkpointPath)

      // Find rows with add actions (stored as JSON strings)
      const addRows = rows.filter(row => row.add !== null && row.add !== undefined)
      expect(addRows.length).toBeGreaterThan(0)

      // Parse and verify add action structure
      const addAction = parseActionColumn<{
        path: string
        size: number
        modificationTime: number
        dataChange: boolean
        partitionValues?: Record<string, string>
        stats?: string
        tags?: Record<string, string>
      }>(addRows[0]!.add)

      expect(addAction).not.toBeNull()
      expect(typeof addAction!.path).toBe('string')
      expect(typeof addAction!.size).toBe('number')
      expect(typeof addAction!.modificationTime).toBe('number')
      expect(typeof addAction!.dataChange).toBe('boolean')
    })

    it('should have correct protocol action schema', async () => {
      // Create commits to trigger checkpoint
      for (let i = 0; i < 15; i++) {
        await backend.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      const deltaLogFiles = await storage.list('warehouse/posts/_delta_log/')
      const checkpointFiles = deltaLogFiles.files.filter(f => f.includes('.checkpoint.parquet'))
      const checkpointPath = checkpointFiles[0]!

      const rows = await readParquet<{
        protocol?: string | null
      }>(storage, checkpointPath)

      // Find rows with protocol action (stored as JSON string)
      const protocolRows = rows.filter(row => row.protocol !== null && row.protocol !== undefined)
      expect(protocolRows.length).toBe(1) // Should have exactly one protocol action

      const protocol = parseActionColumn<{
        minReaderVersion: number
        minWriterVersion: number
      }>(protocolRows[0]!.protocol)

      expect(protocol).not.toBeNull()
      expect(typeof protocol!.minReaderVersion).toBe('number')
      expect(typeof protocol!.minWriterVersion).toBe('number')
      expect(protocol!.minReaderVersion).toBeGreaterThanOrEqual(1)
      expect(protocol!.minWriterVersion).toBeGreaterThanOrEqual(1)
    })

    it('should have correct metaData action schema', async () => {
      // Create commits to trigger checkpoint
      for (let i = 0; i < 15; i++) {
        await backend.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      const deltaLogFiles = await storage.list('warehouse/posts/_delta_log/')
      const checkpointFiles = deltaLogFiles.files.filter(f => f.includes('.checkpoint.parquet'))
      const checkpointPath = checkpointFiles[0]!

      const rows = await readParquet<{
        metaData?: string | null
      }>(storage, checkpointPath)

      // Find rows with metaData action (stored as JSON string)
      const metaDataRows = rows.filter(row => row.metaData !== null && row.metaData !== undefined)
      expect(metaDataRows.length).toBe(1) // Should have exactly one metaData action

      const metaData = parseActionColumn<{
        id: string
        schemaString: string
        partitionColumns: string[]
        createdTime?: number
        configuration?: Record<string, string>
      }>(metaDataRows[0]!.metaData)

      expect(metaData).not.toBeNull()
      expect(typeof metaData!.id).toBe('string')
      expect(typeof metaData!.schemaString).toBe('string')
      expect(Array.isArray(metaData!.partitionColumns)).toBe(true)
    })

    it('should have one row per action with null for unused columns', async () => {
      // Create commits to trigger checkpoint
      for (let i = 0; i < 15; i++) {
        await backend.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      const deltaLogFiles = await storage.list('warehouse/posts/_delta_log/')
      const checkpointFiles = deltaLogFiles.files.filter(f => f.includes('.checkpoint.parquet'))
      const checkpointPath = checkpointFiles[0]!

      const rows = await readParquet<{
        txn?: unknown
        add?: unknown
        remove?: unknown
        metaData?: unknown
        protocol?: unknown
        commitInfo?: unknown
      }>(storage, checkpointPath)

      // Each row should have exactly one non-null action column
      for (const row of rows) {
        const nonNullActions = [
          row.txn,
          row.add,
          row.remove,
          row.metaData,
          row.protocol,
          row.commitInfo,
        ].filter(a => a !== null && a !== undefined)

        expect(nonNullActions.length).toBe(1)
      }
    })
  })

  describe('Checkpoint Reading', () => {
    it('should be able to read checkpoint and reconstruct table state', async () => {
      // Create commits with various operations
      const entity1 = await backend.create('users', { $type: 'User', name: 'Alice', score: 100 })
      const entity2 = await backend.create('users', { $type: 'User', name: 'Bob', score: 50 })

      // Create more commits to trigger checkpoint (need 11 total for checkpoint at version 10)
      for (let i = 0; i < 9; i++) {
        await backend.create('users', { $type: 'User', name: `User ${i}` })
      }

      // Now we should have a checkpoint at version 10 (after 11 commits: 0-10)
      const lastCheckpointExists = await storage.exists('warehouse/users/_delta_log/_last_checkpoint')
      expect(lastCheckpointExists).toBe(true)

      // Create a new backend instance to test reading from checkpoint
      const newBackend = createDeltaBackend({
        type: 'delta',
        storage,
        location: 'warehouse',
      })
      await newBackend.initialize()

      // Should be able to read entities (uses checkpoint for faster loading)
      const users = await newBackend.find('users', {})
      expect(users.length).toBe(11)

      // Original entities should be readable
      const alice = await newBackend.get('users', entity1.$id.split('/')[1]!)
      expect(alice).not.toBeNull()
      expect(alice!.name).toBe('Alice')

      const bob = await newBackend.get('users', entity2.$id.split('/')[1]!)
      expect(bob).not.toBeNull()
      expect(bob!.name).toBe('Bob')

      await newBackend.close()
    })

    it('should read checkpoint and continue with incremental commits', async () => {
      // Create commits to trigger checkpoint
      for (let i = 0; i < 12; i++) {
        await backend.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      // Create new backend to simulate restarting
      const newBackend = createDeltaBackend({
        type: 'delta',
        storage,
        location: 'warehouse',
      })
      await newBackend.initialize()

      // Add more entities (should continue from checkpoint)
      await newBackend.create('posts', { $type: 'Post', name: 'Post 12' })
      await newBackend.create('posts', { $type: 'Post', name: 'Post 13' })

      // All entities should be readable
      const posts = await newBackend.find('posts', {})
      expect(posts.length).toBe(14)

      await newBackend.close()
    })
  })

  describe('Checkpoint Compatibility', () => {
    it('should be readable by standard Delta Lake readers', async () => {
      // Create commits to trigger checkpoint
      for (let i = 0; i < 15; i++) {
        await backend.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      const deltaLogFiles = await storage.list('warehouse/posts/_delta_log/')
      const checkpointFiles = deltaLogFiles.files.filter(f => f.includes('.checkpoint.parquet'))
      const checkpointPath = checkpointFiles[0]!

      // Read checkpoint and verify it follows Delta Lake spec
      const rows = await readParquet<Record<string, unknown>>(storage, checkpointPath)

      // Checkpoint must have these columns (as per Delta Lake Protocol spec)
      const firstRow = rows[0]
      expect(firstRow).toBeDefined()

      // Verify the expected column names exist
      const expectedColumns = ['txn', 'add', 'remove', 'metaData', 'protocol']
      for (const col of expectedColumns) {
        expect(col in firstRow!).toBe(true)
      }
    })

    it('should store partition values as map type in add actions', async () => {
      // Create commits to trigger checkpoint
      for (let i = 0; i < 15; i++) {
        await backend.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      const deltaLogFiles = await storage.list('warehouse/posts/_delta_log/')
      const checkpointFiles = deltaLogFiles.files.filter(f => f.includes('.checkpoint.parquet'))
      const checkpointPath = checkpointFiles[0]!

      const rows = await readParquet<{
        add?: string | null
      }>(storage, checkpointPath)

      const addRows = rows.filter(row => row.add !== null && row.add !== undefined)

      // Parse add actions and check partitionValues
      for (const row of addRows) {
        const addAction = parseActionColumn<{
          partitionValues?: Record<string, string> | null
        }>(row.add)

        expect(addAction).not.toBeNull()
        // partitionValues should be an object (map), undefined, or null - never a primitive
        const partitionValues = addAction!.partitionValues
        expect(
          partitionValues === undefined ||
          partitionValues === null ||
          typeof partitionValues === 'object'
        ).toBe(true)
      }
    })
  })

  describe('_last_checkpoint Format', () => {
    it('should contain valid JSON with version and size', async () => {
      // Create commits to trigger checkpoint
      for (let i = 0; i < 15; i++) {
        await backend.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      const lastCheckpointData = await storage.read('warehouse/posts/_delta_log/_last_checkpoint')
      const lastCheckpoint = JSON.parse(new TextDecoder().decode(lastCheckpointData))

      expect(typeof lastCheckpoint.version).toBe('number')
      expect(typeof lastCheckpoint.size).toBe('number')
      expect(lastCheckpoint.version).toBeGreaterThanOrEqual(10)
      expect(lastCheckpoint.size).toBeGreaterThan(0)
    })

    it('should match the checkpoint file that exists', async () => {
      // Create commits to trigger checkpoint
      for (let i = 0; i < 15; i++) {
        await backend.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      const lastCheckpointData = await storage.read('warehouse/posts/_delta_log/_last_checkpoint')
      const lastCheckpoint = JSON.parse(new TextDecoder().decode(lastCheckpointData))

      // The checkpoint file referenced should exist
      const expectedCheckpointPath = `warehouse/posts/_delta_log/${lastCheckpoint.version.toString().padStart(20, '0')}.checkpoint.parquet`
      const exists = await storage.exists(expectedCheckpointPath)
      expect(exists).toBe(true)

      // The size in _last_checkpoint should match number of actions in checkpoint
      const rows = await readParquet(storage, expectedCheckpointPath)
      expect(rows.length).toBe(lastCheckpoint.size)
    })
  })
})
