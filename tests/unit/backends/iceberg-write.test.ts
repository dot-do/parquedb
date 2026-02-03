/**
 * Tests for IcebergBackend write operations (appendEntities)
 *
 * RED phase: These tests should fail until appendEntities is implemented
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { IcebergBackend, createIcebergBackend } from '../../../src/backends/iceberg'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import type { Entity, EntityId } from '../../../src/types/entity'

describe('IcebergBackend', () => {
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

  describe('create()', () => {
    it('should create an entity and return it with generated $id', async () => {
      const entity = await backend.create('users', {
        $type: 'User',
        name: 'Alice',
        email: 'alice@example.com',
      })

      expect(entity.$id).toMatch(/^users\//)
      expect(entity.$type).toBe('User')
      expect(entity.name).toBe('Alice')
      expect(entity.email).toBe('alice@example.com')
      expect(entity.version).toBe(1)
      expect(entity.createdAt).toBeInstanceOf(Date)
      expect(entity.updatedAt).toBeInstanceOf(Date)
    })

    it('should persist entity to Iceberg table', async () => {
      const created = await backend.create('users', {
        $type: 'User',
        name: 'Bob',
      })

      // Should be able to read it back
      const found = await backend.get('users', created.$id.split('/')[1]!)
      expect(found).not.toBeNull()
      expect(found!.$id).toBe(created.$id)
      expect(found!.name).toBe('Bob')
    })

    it('should create Parquet file in correct Iceberg location', async () => {
      await backend.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Hello World',
      })

      // Check that data file was created in Iceberg structure
      const files = await storage.list('warehouse/testdb/posts/data/')
      expect(files.files.length).toBeGreaterThan(0)
      expect(files.files[0]).toMatch(/\.parquet$/)
    })

    it('should create/update table metadata', async () => {
      await backend.create('posts', {
        $type: 'Post',
        name: 'Test Post',
      })

      // Should have metadata files
      const metadataFiles = await storage.list('warehouse/testdb/posts/metadata/')
      expect(metadataFiles.files.length).toBeGreaterThan(0)

      // Should have version-hint.txt
      const versionHintExists = await storage.exists('warehouse/testdb/posts/metadata/version-hint.txt')
      expect(versionHintExists).toBe(true)
    })

    it('should create snapshot with correct summary', async () => {
      await backend.create('posts', {
        $type: 'Post',
        name: 'Post 1',
      })

      const snapshots = await backend.listSnapshots('posts')
      expect(snapshots.length).toBeGreaterThan(0)

      const latest = snapshots[snapshots.length - 1]!
      expect(latest.operation).toBe('append')
      expect(latest.recordCount).toBe(1)
    })
  })

  describe('bulkCreate()', () => {
    it('should create multiple entities in single Parquet file', async () => {
      const entities = await backend.bulkCreate('users', [
        { $type: 'User', name: 'Alice' },
        { $type: 'User', name: 'Bob' },
        { $type: 'User', name: 'Charlie' },
      ])

      expect(entities).toHaveLength(3)
      expect(entities[0]!.name).toBe('Alice')
      expect(entities[1]!.name).toBe('Bob')
      expect(entities[2]!.name).toBe('Charlie')

      // All should have unique IDs
      const ids = entities.map(e => e.$id)
      expect(new Set(ids).size).toBe(3)
    })

    it('should be readable after bulk create', async () => {
      await backend.bulkCreate('users', [
        { $type: 'User', name: 'Alice', role: 'admin' },
        { $type: 'User', name: 'Bob', role: 'user' },
      ])

      const all = await backend.find('users', {})
      expect(all).toHaveLength(2)

      const admins = await backend.find('users', { role: 'admin' })
      expect(admins).toHaveLength(1)
      expect(admins[0]!.name).toBe('Alice')
    })

    it('should update snapshot record count correctly', async () => {
      await backend.bulkCreate('posts', [
        { $type: 'Post', name: 'Post 1' },
        { $type: 'Post', name: 'Post 2' },
        { $type: 'Post', name: 'Post 3' },
      ])

      const stats = await backend.stats('posts')
      expect(stats.recordCount).toBe(3)
    })
  })

  describe('update()', () => {
    it('should update entity and increment version', async () => {
      const created = await backend.create('users', {
        $type: 'User',
        name: 'Alice',
        score: 100,
      })

      const updated = await backend.update('users', created.$id.split('/')[1]!, {
        $set: { score: 150 },
      })

      expect(updated.score).toBe(150)
      expect(updated.version).toBe(2)
      expect(updated.updatedAt.getTime()).toBeGreaterThan(created.updatedAt.getTime())
    })

    it('should persist update to Iceberg', async () => {
      const created = await backend.create('users', {
        $type: 'User',
        name: 'Alice',
        active: true,
      })

      await backend.update('users', created.$id.split('/')[1]!, {
        $set: { active: false },
      })

      const found = await backend.get('users', created.$id.split('/')[1]!)
      expect(found!.active).toBe(false)
    })

    it('should create new snapshot for update', async () => {
      const created = await backend.create('users', {
        $type: 'User',
        name: 'Alice',
      })

      const snapshotsBefore = await backend.listSnapshots('users')

      await backend.update('users', created.$id.split('/')[1]!, {
        $set: { name: 'Alicia' },
      })

      const snapshotsAfter = await backend.listSnapshots('users')
      expect(snapshotsAfter.length).toBeGreaterThan(snapshotsBefore.length)
    })
  })

  describe('delete()', () => {
    it('should soft delete by default', async () => {
      const created = await backend.create('users', {
        $type: 'User',
        name: 'Alice',
      })

      const result = await backend.delete('users', created.$id.split('/')[1]!)
      expect(result.deletedCount).toBe(1)

      // Should not appear in normal find
      const found = await backend.find('users', {})
      expect(found).toHaveLength(0)

      // Should appear with includeDeleted
      const foundWithDeleted = await backend.find('users', {}, { includeDeleted: true })
      expect(foundWithDeleted).toHaveLength(1)
      expect(foundWithDeleted[0]!.deletedAt).toBeInstanceOf(Date)
    })

    it('should return 0 for non-existent entity', async () => {
      const result = await backend.delete('users', 'nonexistent')
      expect(result.deletedCount).toBe(0)
    })
  })

  describe('Variant encoding', () => {
    it('should handle nested objects in $data', async () => {
      const entity = await backend.create('posts', {
        $type: 'Post',
        name: 'Test',
        metadata: {
          tags: ['tech', 'database'],
          stats: { views: 100, likes: 50 },
        },
      })

      const found = await backend.get('posts', entity.$id.split('/')[1]!)
      expect(found!.metadata).toEqual({
        tags: ['tech', 'database'],
        stats: { views: 100, likes: 50 },
      })
    })

    it('should handle arrays', async () => {
      const entity = await backend.create('posts', {
        $type: 'Post',
        name: 'Test',
        tags: ['a', 'b', 'c'],
        numbers: [1, 2, 3],
      })

      const found = await backend.get('posts', entity.$id.split('/')[1]!)
      expect(found!.tags).toEqual(['a', 'b', 'c'])
      expect(found!.numbers).toEqual([1, 2, 3])
    })

    it('should handle null values', async () => {
      const entity = await backend.create('users', {
        $type: 'User',
        name: 'Test',
        middleName: null,
        bio: undefined,
      })

      const found = await backend.get('users', entity.$id.split('/')[1]!)
      expect(found!.middleName).toBeNull()
      // undefined should not be present
      expect('bio' in found!).toBe(false)
    })

    it('should handle dates', async () => {
      const birthDate = new Date('1990-01-15')
      const entity = await backend.create('users', {
        $type: 'User',
        name: 'Test',
        birthDate,
      })

      const found = await backend.get('users', entity.$id.split('/')[1]!)
      expect(found!.birthDate).toEqual(birthDate)
    })
  })

  describe('Schema', () => {
    it('should auto-create table with default entity schema', async () => {
      await backend.create('newcollection', {
        $type: 'Thing',
        name: 'Test',
      })

      const schema = await backend.getSchema('newcollection')
      expect(schema).not.toBeNull()
      expect(schema!.fields.some(f => f.name === '$id')).toBe(true)
      expect(schema!.fields.some(f => f.name === '$type')).toBe(true)
      expect(schema!.fields.some(f => f.name === 'name')).toBe(true)
      expect(schema!.fields.some(f => f.name === 'version')).toBe(true)
    })

    it('should list namespaces after creating tables', async () => {
      await backend.create('users', { $type: 'User', name: 'Alice' })
      await backend.create('posts', { $type: 'Post', name: 'Post 1' })

      const namespaces = await backend.listNamespaces()
      expect(namespaces).toContain('users')
      expect(namespaces).toContain('posts')
    })
  })
})
