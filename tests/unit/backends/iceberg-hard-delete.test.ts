/**
 * Tests for IcebergBackend hard delete operations
 *
 * Tests the equality delete file implementation for hard deletes.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { IcebergBackend, createIcebergBackend } from '../../../src/backends/iceberg'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import type { Entity, EntityId } from '../../../src/types/entity'

describe('IcebergBackend hard delete', () => {
  let storage: MemoryBackend
  let backend: IcebergBackend

  beforeEach(async () => {
    storage = new MemoryBackend()
    backend = createIcebergBackend({
      type: 'iceberg',
      storage,
      warehouse: 'warehouse',
      database: 'testdb',
    })
    await backend.initialize()
  })

  afterEach(async () => {
    await backend.close()
  })

  describe('hardDelete()', () => {
    it('should hard delete a single entity', async () => {
      // Create an entity
      const entity = await backend.create('users', {
        $type: 'User',
        name: 'Alice',
        email: 'alice@example.com',
      })

      // Verify entity exists
      const foundBefore = await backend.get('users', entity.$id.split('/')[1]!)
      expect(foundBefore).not.toBeNull()
      expect(foundBefore!.name).toBe('Alice')

      // Hard delete the entity
      const result = await backend.delete('users', entity.$id.split('/')[1]!, { hard: true })
      expect(result.deletedCount).toBe(1)

      // Verify entity is gone (even with includeDeleted)
      const foundAfter = await backend.find('users', {}, { includeDeleted: true })
      expect(foundAfter).toHaveLength(0)
    })

    it('should hard delete multiple entities', async () => {
      // Create multiple entities
      const entities = await backend.bulkCreate('users', [
        { $type: 'User', name: 'Alice' },
        { $type: 'User', name: 'Bob' },
        { $type: 'User', name: 'Charlie' },
      ])

      // Verify all entities exist
      const foundBefore = await backend.find('users', {})
      expect(foundBefore).toHaveLength(3)

      // Hard delete two entities
      const result1 = await backend.delete('users', entities[0]!.$id.split('/')[1]!, { hard: true })
      const result2 = await backend.delete('users', entities[1]!.$id.split('/')[1]!, { hard: true })
      expect(result1.deletedCount).toBe(1)
      expect(result2.deletedCount).toBe(1)

      // Verify only one entity remains
      const foundAfter = await backend.find('users', {})
      expect(foundAfter).toHaveLength(1)
      expect(foundAfter[0]!.name).toBe('Charlie')
    })

    it('should create delete snapshot with delete operation', async () => {
      // Create an entity
      const entity = await backend.create('users', {
        $type: 'User',
        name: 'Alice',
      })

      const snapshotsBefore = await backend.listSnapshots('users')

      // Hard delete the entity
      await backend.delete('users', entity.$id.split('/')[1]!, { hard: true })

      const snapshotsAfter = await backend.listSnapshots('users')
      expect(snapshotsAfter.length).toBe(snapshotsBefore.length + 1)

      // Verify the latest snapshot is a delete operation
      const latestSnapshot = snapshotsAfter[snapshotsAfter.length - 1]!
      expect(latestSnapshot.operation).toBe('delete')
    })

    it('should create equality delete file in data directory', async () => {
      // Create an entity
      const entity = await backend.create('users', {
        $type: 'User',
        name: 'Alice',
      })

      // Hard delete the entity
      await backend.delete('users', entity.$id.split('/')[1]!, { hard: true })

      // Check for delete file in data directory
      const dataFiles = await storage.list('warehouse/testdb/users/data/')
      const deleteFiles = dataFiles.files.filter(f => f.includes('-delete.parquet'))
      expect(deleteFiles.length).toBeGreaterThan(0)
    })

    it('should create delete manifest in metadata directory', async () => {
      // Create an entity
      const entity = await backend.create('users', {
        $type: 'User',
        name: 'Alice',
      })

      const manifestsBefore = await storage.list('warehouse/testdb/users/metadata/')
      const avroFilesBefore = manifestsBefore.files.filter(f => f.endsWith('.avro'))

      // Hard delete the entity
      await backend.delete('users', entity.$id.split('/')[1]!, { hard: true })

      const manifestsAfter = await storage.list('warehouse/testdb/users/metadata/')
      const avroFilesAfter = manifestsAfter.files.filter(f => f.endsWith('.avro'))

      // Should have more avro files (manifest + manifest list)
      expect(avroFilesAfter.length).toBeGreaterThan(avroFilesBefore.length)
    })

    it('should return 0 deletedCount for non-existent entity', async () => {
      const result = await backend.delete('users', 'nonexistent', { hard: true })
      expect(result.deletedCount).toBe(0)
    })

    it('should handle hard delete after soft delete', async () => {
      // Create an entity
      const entity = await backend.create('users', {
        $type: 'User',
        name: 'Alice',
      })

      // Soft delete first
      await backend.delete('users', entity.$id.split('/')[1]!)

      // Verify it's soft deleted
      const foundSoftDeleted = await backend.find('users', {}, { includeDeleted: true })
      expect(foundSoftDeleted).toHaveLength(1)
      expect(foundSoftDeleted[0]!.deletedAt).toBeInstanceOf(Date)

      // Hard delete
      await backend.delete('users', entity.$id.split('/')[1]!, { hard: true })

      // Verify it's completely gone
      const foundAfterHardDelete = await backend.find('users', {}, { includeDeleted: true })
      expect(foundAfterHardDelete).toHaveLength(0)
    })

    it('should not affect other entities', async () => {
      // Create multiple entities
      const alice = await backend.create('users', {
        $type: 'User',
        name: 'Alice',
        role: 'admin',
      })

      const bob = await backend.create('users', {
        $type: 'User',
        name: 'Bob',
        role: 'user',
      })

      // Hard delete Alice
      await backend.delete('users', alice.$id.split('/')[1]!, { hard: true })

      // Verify Bob is still there with all data intact
      const found = await backend.get('users', bob.$id.split('/')[1]!)
      expect(found).not.toBeNull()
      expect(found!.name).toBe('Bob')
      expect(found!.role).toBe('user')
    })

    it('should work with bulkDelete and hard option', async () => {
      // Create multiple entities
      const entities = await backend.bulkCreate('users', [
        { $type: 'User', name: 'Alice' },
        { $type: 'User', name: 'Bob' },
        { $type: 'User', name: 'Charlie' },
      ])

      // Bulk hard delete using $in filter
      const ids = entities.map(e => e.$id)
      const result = await backend.bulkDelete('users', { $id: { $in: ids } }, { hard: true })
      expect(result.deletedCount).toBe(3)

      // Verify all are gone
      const foundAfter = await backend.find('users', {}, { includeDeleted: true })
      expect(foundAfter).toHaveLength(0)
    })
  })

  describe('time travel with hard deletes', () => {
    it('should still see hard-deleted entities in previous snapshots', async () => {
      // Create an entity
      const entity = await backend.create('users', {
        $type: 'User',
        name: 'Alice',
      })

      // Get snapshot info before delete
      const snapshots = await backend.listSnapshots('users')
      const snapshotBeforeDelete = snapshots[snapshots.length - 1]!

      // Hard delete the entity
      await backend.delete('users', entity.$id.split('/')[1]!, { hard: true })

      // Verify entity is gone in current state
      const currentEntities = await backend.find('users', {})
      expect(currentEntities).toHaveLength(0)

      // Use time travel to view previous snapshot
      const snapshotBackend = await backend.snapshot('users', snapshotBeforeDelete.id)
      const historicalEntities = await snapshotBackend.find('users', {})
      expect(historicalEntities).toHaveLength(1)
      expect(historicalEntities[0]!.name).toBe('Alice')
    })
  })
})
