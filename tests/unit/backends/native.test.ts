/**
 * Tests for NativeBackend
 *
 * Tests the simple Parquet-based storage backend without Iceberg/Delta overhead.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { NativeBackend, createNativeBackend } from '../../../src/backends/native'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import type { EntityBackend } from '../../../src/backends/types'
import { ReadOnlyError } from '../../../src/backends/types'

describe('NativeBackend', () => {
  let storage: MemoryBackend
  let backend: EntityBackend

  beforeEach(async () => {
    storage = new MemoryBackend()
    backend = createNativeBackend({
      type: 'native',
      storage,
      location: 'warehouse',
    })
    await backend.initialize()
  })

  afterEach(async () => {
    await backend.close()
  })

  // ===========================================================================
  // Metadata
  // ===========================================================================

  describe('metadata', () => {
    it('should have correct type', () => {
      expect(backend.type).toBe('native')
    })

    it('should not support time travel', () => {
      expect(backend.supportsTimeTravel).toBe(false)
    })

    it('should not support schema evolution', () => {
      expect(backend.supportsSchemaEvolution).toBe(false)
    })

    it('should not be read-only by default', () => {
      expect(backend.readOnly).toBe(false)
    })
  })

  // ===========================================================================
  // CRUD Operations
  // ===========================================================================

  describe('create', () => {
    it('should create an entity', async () => {
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

    it('should persist entities to storage', async () => {
      await backend.create('users', {
        $type: 'User',
        name: 'Alice',
      })

      // Close and reopen to verify persistence
      await backend.close()

      const newBackend = createNativeBackend({
        type: 'native',
        storage,
        location: 'warehouse',
      })
      await newBackend.initialize()

      const users = await newBackend.find('users', {})
      expect(users).toHaveLength(1)
      expect(users[0]!.name).toBe('Alice')

      await newBackend.close()
    })
  })

  describe('get', () => {
    it('should get an entity by ID', async () => {
      const created = await backend.create('users', {
        $type: 'User',
        name: 'Alice',
      })

      const id = created.$id.split('/')[1]!
      const found = await backend.get('users', id)

      expect(found).not.toBeNull()
      expect(found!.$id).toBe(created.$id)
      expect(found!.name).toBe('Alice')
    })

    it('should return null for non-existent entity', async () => {
      const found = await backend.get('users', 'nonexistent')
      expect(found).toBeNull()
    })
  })

  describe('find', () => {
    beforeEach(async () => {
      await backend.bulkCreate('users', [
        { $type: 'User', name: 'Alice', age: 30 },
        { $type: 'User', name: 'Bob', age: 25 },
        { $type: 'User', name: 'Charlie', age: 35 },
      ])
    })

    it('should find all entities', async () => {
      const users = await backend.find('users', {})
      expect(users).toHaveLength(3)
    })

    it('should filter by field equality', async () => {
      const users = await backend.find('users', { name: 'Alice' })
      expect(users).toHaveLength(1)
      expect(users[0]!.name).toBe('Alice')
    })

    it('should filter by comparison operators', async () => {
      const users = await backend.find('users', { age: { $gte: 30 } })
      expect(users).toHaveLength(2)
    })

    it('should sort results', async () => {
      const users = await backend.find('users', {}, { sort: { age: 1 } })
      expect(users[0]!.name).toBe('Bob')
      expect(users[1]!.name).toBe('Alice')
      expect(users[2]!.name).toBe('Charlie')
    })

    it('should apply skip and limit', async () => {
      const users = await backend.find('users', {}, {
        sort: { name: 1 },
        skip: 1,
        limit: 1,
      })
      expect(users).toHaveLength(1)
      expect(users[0]!.name).toBe('Bob')
    })
  })

  describe('update', () => {
    it('should update an entity', async () => {
      const created = await backend.create('users', {
        $type: 'User',
        name: 'Alice',
        score: 100,
      })

      const id = created.$id.split('/')[1]!
      const updated = await backend.update('users', id, {
        $set: { score: 200 },
      })

      expect(updated.score).toBe(200)
      expect(updated.version).toBe(2)
    })

    it('should support upsert', async () => {
      const result = await backend.update('users', 'newuser', {
        $set: { email: 'new@example.com' },
      }, { upsert: true })

      expect(result.$id).toBe('users/newuser')
      expect(result.email).toBe('new@example.com')
    })

    it('should throw error for non-existent entity without upsert', async () => {
      await expect(
        backend.update('users', 'nonexistent', { $set: { name: 'Test' } })
      ).rejects.toThrow('Entity not found')
    })
  })

  describe('delete', () => {
    it('should soft delete an entity', async () => {
      const created = await backend.create('users', {
        $type: 'User',
        name: 'Alice',
      })

      const id = created.$id.split('/')[1]!
      const result = await backend.delete('users', id)

      expect(result.deletedCount).toBe(1)

      // Should not appear in normal find
      const users = await backend.find('users', {})
      expect(users).toHaveLength(0)

      // Should appear with includeDeleted
      const allUsers = await backend.find('users', {}, { includeDeleted: true })
      expect(allUsers).toHaveLength(1)
      expect(allUsers[0]!.deletedAt).toBeInstanceOf(Date)
    })

    it('should hard delete an entity', async () => {
      const created = await backend.create('users', {
        $type: 'User',
        name: 'Alice',
      })

      const id = created.$id.split('/')[1]!
      await backend.delete('users', id, { hard: true })

      // Should not appear even with includeDeleted
      const users = await backend.find('users', {}, { includeDeleted: true })
      expect(users).toHaveLength(0)
    })

    it('should return 0 for non-existent entity', async () => {
      const result = await backend.delete('users', 'nonexistent')
      expect(result.deletedCount).toBe(0)
    })
  })

  // ===========================================================================
  // Batch Operations
  // ===========================================================================

  describe('bulkCreate', () => {
    it('should create multiple entities', async () => {
      const entities = await backend.bulkCreate('users', [
        { $type: 'User', name: 'Alice' },
        { $type: 'User', name: 'Bob' },
        { $type: 'User', name: 'Charlie' },
      ])

      expect(entities).toHaveLength(3)
      expect(entities[0]!.name).toBe('Alice')
      expect(entities[1]!.name).toBe('Bob')
      expect(entities[2]!.name).toBe('Charlie')

      const users = await backend.find('users', {})
      expect(users).toHaveLength(3)
    })
  })

  describe('bulkUpdate', () => {
    it('should update multiple entities', async () => {
      await backend.bulkCreate('users', [
        { $type: 'User', name: 'Alice', status: 'active' },
        { $type: 'User', name: 'Bob', status: 'active' },
        { $type: 'User', name: 'Charlie', status: 'inactive' },
      ])

      const result = await backend.bulkUpdate('users', { status: 'active' }, {
        $set: { verified: true },
      })

      expect(result.matchedCount).toBe(2)
      expect(result.modifiedCount).toBe(2)

      const verified = await backend.find('users', { verified: true })
      expect(verified).toHaveLength(2)
    })
  })

  describe('bulkDelete', () => {
    it('should delete multiple entities', async () => {
      await backend.bulkCreate('users', [
        { $type: 'User', name: 'Alice', role: 'admin' },
        { $type: 'User', name: 'Bob', role: 'user' },
        { $type: 'User', name: 'Charlie', role: 'user' },
      ])

      const result = await backend.bulkDelete('users', { role: 'user' })

      expect(result.deletedCount).toBe(2)

      const users = await backend.find('users', {})
      expect(users).toHaveLength(1)
      expect(users[0]!.name).toBe('Alice')
    })
  })

  // ===========================================================================
  // Schema and Namespace
  // ===========================================================================

  describe('getSchema', () => {
    it('should return null for non-existent namespace', async () => {
      const schema = await backend.getSchema('nonexistent')
      expect(schema).toBeNull()
    })

    it('should return schema for existing namespace', async () => {
      await backend.create('users', {
        $type: 'User',
        name: 'Alice',
      })

      const schema = await backend.getSchema('users')
      expect(schema).not.toBeNull()
      expect(schema!.name).toBe('users')
      expect(schema!.fields).toBeInstanceOf(Array)
    })
  })

  describe('listNamespaces', () => {
    it('should return empty array when no data', async () => {
      const namespaces = await backend.listNamespaces()
      expect(namespaces).toEqual([])
    })

    it('should list all namespaces', async () => {
      await backend.create('users', { $type: 'User', name: 'Alice' })
      await backend.create('posts', { $type: 'Post', name: 'Hello' })
      await backend.create('comments', { $type: 'Comment', name: 'Nice!' })

      const namespaces = await backend.listNamespaces()
      expect(namespaces).toHaveLength(3)
      expect(namespaces.sort()).toEqual(['comments', 'posts', 'users'])
    })
  })

  // ===========================================================================
  // Stats
  // ===========================================================================

  describe('stats', () => {
    it('should return stats for a namespace', async () => {
      await backend.bulkCreate('users', [
        { $type: 'User', name: 'Alice' },
        { $type: 'User', name: 'Bob' },
        { $type: 'User', name: 'Charlie' },
      ])

      const stats = await backend.stats!('users')
      expect(stats.recordCount).toBe(3)
      expect(stats.fileCount).toBe(1)
      expect(stats.totalBytes).toBeGreaterThan(0)
    })

    it('should return zero stats for non-existent namespace', async () => {
      const stats = await backend.stats!('nonexistent')
      expect(stats.recordCount).toBe(0)
      expect(stats.fileCount).toBe(0)
      expect(stats.totalBytes).toBe(0)
    })
  })

  // ===========================================================================
  // Read-Only Mode
  // ===========================================================================

  describe('read-only mode', () => {
    let readOnlyBackend: EntityBackend

    beforeEach(async () => {
      // Create some test data first
      await backend.create('users', {
        $type: 'User',
        name: 'Alice',
      })

      // Create read-only backend
      readOnlyBackend = createNativeBackend({
        type: 'native',
        storage,
        location: 'warehouse',
        readOnly: true,
      })
      await readOnlyBackend.initialize()
    })

    afterEach(async () => {
      await readOnlyBackend.close()
    })

    it('should allow read operations', async () => {
      const users = await readOnlyBackend.find('users', {})
      expect(users).toHaveLength(1)
    })

    it('should throw ReadOnlyError on create', async () => {
      await expect(
        readOnlyBackend.create('users', { $type: 'User', name: 'Bob' })
      ).rejects.toThrow(ReadOnlyError)
    })

    it('should throw ReadOnlyError on update', async () => {
      const users = await readOnlyBackend.find('users', {})
      const id = users[0]!.$id.split('/')[1]!

      await expect(
        readOnlyBackend.update('users', id, { $set: { name: 'Alicia' } })
      ).rejects.toThrow(ReadOnlyError)
    })

    it('should throw ReadOnlyError on delete', async () => {
      const users = await readOnlyBackend.find('users', {})
      const id = users[0]!.$id.split('/')[1]!

      await expect(
        readOnlyBackend.delete('users', id)
      ).rejects.toThrow(ReadOnlyError)
    })

    it('should throw ReadOnlyError on bulkCreate', async () => {
      await expect(
        readOnlyBackend.bulkCreate('users', [{ $type: 'User', name: 'Bob' }])
      ).rejects.toThrow(ReadOnlyError)
    })

    it('should throw ReadOnlyError on bulkUpdate', async () => {
      await expect(
        readOnlyBackend.bulkUpdate('users', {}, { $set: { verified: true } })
      ).rejects.toThrow(ReadOnlyError)
    })

    it('should throw ReadOnlyError on bulkDelete', async () => {
      await expect(
        readOnlyBackend.bulkDelete('users', {})
      ).rejects.toThrow(ReadOnlyError)
    })
  })

  // ===========================================================================
  // Lifecycle
  // ===========================================================================

  describe('lifecycle', () => {
    it('should be idempotent for multiple initialize calls', async () => {
      await backend.initialize()
      await backend.initialize()

      const entity = await backend.create('test', {
        $type: 'Test',
        name: 'Lifecycle Test',
      })
      expect(entity).toBeDefined()
    })

    it('should clear cache on close', async () => {
      await backend.create('users', {
        $type: 'User',
        name: 'Alice',
      })

      await backend.close()
      await backend.initialize()

      // Should still be able to read data (from disk)
      const users = await backend.find('users', {})
      expect(users).toHaveLength(1)
    })
  })

  // ===========================================================================
  // Update Operators
  // ===========================================================================

  describe('update operators', () => {
    it('should support $set', async () => {
      const created = await backend.create('users', {
        $type: 'User',
        name: 'Alice',
        score: 100,
      })

      const id = created.$id.split('/')[1]!
      const updated = await backend.update('users', id, {
        $set: { score: 200, level: 'gold' },
      })

      expect(updated.score).toBe(200)
      expect(updated.level).toBe('gold')
    })

    it('should support $unset', async () => {
      const created = await backend.create('users', {
        $type: 'User',
        name: 'Alice',
        tempField: 'to-be-removed',
      })

      const id = created.$id.split('/')[1]!
      const updated = await backend.update('users', id, {
        $unset: { tempField: true },
      })

      expect('tempField' in updated).toBe(false)
    })

    it('should support $inc', async () => {
      const created = await backend.create('users', {
        $type: 'User',
        name: 'Alice',
        points: 100,
      })

      const id = created.$id.split('/')[1]!
      const updated = await backend.update('users', id, {
        $inc: { points: 50 },
      })

      expect(updated.points).toBe(150)
    })
  })
})

describe('createNativeBackend factory', () => {
  it('should create a NativeBackend instance', () => {
    const storage = new MemoryBackend()
    const backend = createNativeBackend({
      type: 'native',
      storage,
      location: 'warehouse',
    })

    expect(backend).toBeInstanceOf(NativeBackend)
  })
})
