/**
 * Tests for DeltaBackend write operations
 *
 * RED phase: These tests should fail until DeltaBackend is implemented
 *
 * Delta Lake specifics:
 * - Transaction log in `_delta_log/` directory
 * - Commit files: `00000000000000000000.json`, `00000000000000000001.json`, etc.
 * - Each commit contains add/remove actions for Parquet files
 * - `_last_checkpoint` file for optimization
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { DeltaBackend, createDeltaBackend } from '../../../src/backends/delta'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import type { Entity, EntityId } from '../../../src/types/entity'

describe('DeltaBackend', () => {
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

    it('should persist entity to Delta table', async () => {
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

    it('should create Parquet file in correct Delta Lake location', async () => {
      await backend.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Hello World',
      })

      // Check that data file was created in Delta Lake structure
      // Delta Lake stores data files directly in the table directory (not in data/)
      const files = await storage.list('warehouse/posts/')
      const parquetFiles = files.files.filter(f => f.endsWith('.parquet'))
      expect(parquetFiles.length).toBeGreaterThan(0)
    })

    it('should create _delta_log directory with commit file', async () => {
      await backend.create('posts', {
        $type: 'Post',
        name: 'Test Post',
      })

      // Should have _delta_log directory with commit file
      const deltaLogFiles = await storage.list('warehouse/posts/_delta_log/')
      expect(deltaLogFiles.files.length).toBeGreaterThan(0)

      // First commit should be 00000000000000000000.json
      expect(deltaLogFiles.files.some(f => f.includes('00000000000000000000.json'))).toBe(true)
    })

    it('should create valid Delta commit JSON with add action', async () => {
      await backend.create('posts', {
        $type: 'Post',
        name: 'Test Post',
      })

      // Read the commit file
      const commitData = await storage.read('warehouse/posts/_delta_log/00000000000000000000.json')
      const commitText = new TextDecoder().decode(commitData)

      // Delta log files are newline-delimited JSON (each line is a separate action)
      const lines = commitText.trim().split('\n')
      const actions = lines.map(line => JSON.parse(line))

      // Should have protocol and metadata actions first, then add action
      const addAction = actions.find((a: Record<string, unknown>) => 'add' in a)
      expect(addAction).toBeDefined()
      expect(addAction.add.path).toMatch(/\.parquet$/)
      expect(addAction.add.size).toBeGreaterThan(0)
    })

    it('should create protocol and metaData actions in first commit', async () => {
      await backend.create('posts', {
        $type: 'Post',
        name: 'Test Post',
      })

      const commitData = await storage.read('warehouse/posts/_delta_log/00000000000000000000.json')
      const commitText = new TextDecoder().decode(commitData)
      const lines = commitText.trim().split('\n')
      const actions = lines.map(line => JSON.parse(line))

      // First commit should have protocol action
      const protocolAction = actions.find((a: Record<string, unknown>) => 'protocol' in a)
      expect(protocolAction).toBeDefined()
      expect(protocolAction.protocol.minReaderVersion).toBeGreaterThanOrEqual(1)
      expect(protocolAction.protocol.minWriterVersion).toBeGreaterThanOrEqual(1)

      // First commit should have metaData action
      const metaDataAction = actions.find((a: Record<string, unknown>) => 'metaData' in a)
      expect(metaDataAction).toBeDefined()
      expect(metaDataAction.metaData.schemaString).toBeDefined()
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

    it('should create single commit for bulk operation', async () => {
      await backend.bulkCreate('posts', [
        { $type: 'Post', name: 'Post 1' },
        { $type: 'Post', name: 'Post 2' },
        { $type: 'Post', name: 'Post 3' },
      ])

      // Should have exactly one commit file (00000000000000000000.json)
      const deltaLogFiles = await storage.list('warehouse/posts/_delta_log/')
      const commitFiles = deltaLogFiles.files.filter(f => f.endsWith('.json') && !f.includes('checkpoint'))
      expect(commitFiles).toHaveLength(1)
    })

    it('should update stats correctly for bulk create', async () => {
      await backend.bulkCreate('posts', [
        { $type: 'Post', name: 'Post 1' },
        { $type: 'Post', name: 'Post 2' },
        { $type: 'Post', name: 'Post 3' },
      ])

      const stats = await backend.stats('posts')
      expect(stats.recordCount).toBe(3)
    })
  })

  describe('get()/find()', () => {
    it('should find entity by ID', async () => {
      const created = await backend.create('users', {
        $type: 'User',
        name: 'Alice',
      })

      const found = await backend.get('users', created.$id.split('/')[1]!)
      expect(found).not.toBeNull()
      expect(found!.$id).toBe(created.$id)
      expect(found!.name).toBe('Alice')
    })

    it('should return null for non-existent entity', async () => {
      const found = await backend.get('users', 'nonexistent')
      expect(found).toBeNull()
    })

    it('should find entities with equality filter', async () => {
      await backend.bulkCreate('users', [
        { $type: 'User', name: 'Alice', role: 'admin' },
        { $type: 'User', name: 'Bob', role: 'user' },
        { $type: 'User', name: 'Charlie', role: 'user' },
      ])

      const users = await backend.find('users', { role: 'user' })
      expect(users).toHaveLength(2)
      expect(users.map(u => u.name).sort()).toEqual(['Bob', 'Charlie'])
    })

    it('should find entities with comparison operators', async () => {
      await backend.bulkCreate('users', [
        { $type: 'User', name: 'Alice', score: 100 },
        { $type: 'User', name: 'Bob', score: 50 },
        { $type: 'User', name: 'Charlie', score: 75 },
      ])

      const highScorers = await backend.find('users', { score: { $gte: 75 } })
      expect(highScorers).toHaveLength(2)
      expect(highScorers.map(u => u.name).sort()).toEqual(['Alice', 'Charlie'])
    })

    it('should find entities with $in operator', async () => {
      await backend.bulkCreate('users', [
        { $type: 'User', name: 'Alice', role: 'admin' },
        { $type: 'User', name: 'Bob', role: 'user' },
        { $type: 'User', name: 'Charlie', role: 'moderator' },
      ])

      const selected = await backend.find('users', { role: { $in: ['admin', 'moderator'] } })
      expect(selected).toHaveLength(2)
      expect(selected.map(u => u.name).sort()).toEqual(['Alice', 'Charlie'])
    })

    it('should support limit and skip options', async () => {
      await backend.bulkCreate('users', [
        { $type: 'User', name: 'Alice' },
        { $type: 'User', name: 'Bob' },
        { $type: 'User', name: 'Charlie' },
        { $type: 'User', name: 'Dave' },
      ])

      const limited = await backend.find('users', {}, { limit: 2 })
      expect(limited).toHaveLength(2)

      const skipped = await backend.find('users', {}, { skip: 2, limit: 2 })
      expect(skipped).toHaveLength(2)
    })

    it('should support sorting', async () => {
      await backend.bulkCreate('users', [
        { $type: 'User', name: 'Charlie', score: 75 },
        { $type: 'User', name: 'Alice', score: 100 },
        { $type: 'User', name: 'Bob', score: 50 },
      ])

      const sortedByScore = await backend.find('users', {}, { sort: { score: -1 } })
      expect(sortedByScore.map(u => u.name)).toEqual(['Alice', 'Charlie', 'Bob'])

      const sortedByName = await backend.find('users', {}, { sort: { name: 1 } })
      expect(sortedByName.map(u => u.name)).toEqual(['Alice', 'Bob', 'Charlie'])
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

    it('should persist update to Delta Lake', async () => {
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

    it('should create new commit for update', async () => {
      const created = await backend.create('users', {
        $type: 'User',
        name: 'Alice',
      })

      const commitsBefore = await storage.list('warehouse/users/_delta_log/')
      const commitCountBefore = commitsBefore.files.filter(f => f.endsWith('.json') && !f.includes('checkpoint')).length

      await backend.update('users', created.$id.split('/')[1]!, {
        $set: { name: 'Alicia' },
      })

      const commitsAfter = await storage.list('warehouse/users/_delta_log/')
      const commitCountAfter = commitsAfter.files.filter(f => f.endsWith('.json') && !f.includes('checkpoint')).length

      expect(commitCountAfter).toBeGreaterThan(commitCountBefore)
    })

    it('should support $inc operator', async () => {
      const created = await backend.create('users', {
        $type: 'User',
        name: 'Alice',
        score: 100,
      })

      const updated = await backend.update('users', created.$id.split('/')[1]!, {
        $inc: { score: 10 },
      })

      expect(updated.score).toBe(110)
    })

    it('should support $unset operator', async () => {
      const created = await backend.create('users', {
        $type: 'User',
        name: 'Alice',
        nickname: 'Ali',
      })

      const updated = await backend.update('users', created.$id.split('/')[1]!, {
        $unset: { nickname: true },
      })

      expect('nickname' in updated).toBe(false)
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

    it('should hard delete when option is set', async () => {
      const created = await backend.create('users', {
        $type: 'User',
        name: 'Alice',
      })

      await backend.delete('users', created.$id.split('/')[1]!, { hard: true })

      // Should not appear even with includeDeleted
      const foundWithDeleted = await backend.find('users', {}, { includeDeleted: true })
      expect(foundWithDeleted).toHaveLength(0)
    })

    it('should create remove action in commit for hard delete', async () => {
      const created = await backend.create('users', {
        $type: 'User',
        name: 'Alice',
      })

      await backend.delete('users', created.$id.split('/')[1]!, { hard: true })

      // Get the latest commit file
      const deltaLogFiles = await storage.list('warehouse/users/_delta_log/')
      const commitFiles = deltaLogFiles.files
        .filter(f => f.endsWith('.json') && !f.includes('checkpoint'))
        .sort()
      const latestCommitPath = commitFiles[commitFiles.length - 1]!

      const commitData = await storage.read(latestCommitPath)
      const commitText = new TextDecoder().decode(commitData)
      const lines = commitText.trim().split('\n')
      const actions = lines.map(line => JSON.parse(line))

      // Should have remove action
      const removeAction = actions.find((a: Record<string, unknown>) => 'remove' in a)
      expect(removeAction).toBeDefined()
    })
  })

  describe('Delta Log Structure', () => {
    it('should use zero-padded 20-digit commit filenames', async () => {
      await backend.create('posts', { $type: 'Post', name: 'Post 1' })
      await backend.create('posts', { $type: 'Post', name: 'Post 2' })
      await backend.create('posts', { $type: 'Post', name: 'Post 3' })

      const deltaLogFiles = await storage.list('warehouse/posts/_delta_log/')
      const commitFiles = deltaLogFiles.files.filter(f => f.endsWith('.json') && !f.includes('checkpoint'))

      // Should have commit files with proper naming
      expect(commitFiles.some(f => f.includes('00000000000000000000.json'))).toBe(true)
      expect(commitFiles.some(f => f.includes('00000000000000000001.json'))).toBe(true)
      expect(commitFiles.some(f => f.includes('00000000000000000002.json'))).toBe(true)
    })

    it('should include commitInfo action in each commit', async () => {
      await backend.create('posts', { $type: 'Post', name: 'Test Post' })

      const commitData = await storage.read('warehouse/posts/_delta_log/00000000000000000000.json')
      const commitText = new TextDecoder().decode(commitData)
      const lines = commitText.trim().split('\n')
      const actions = lines.map(line => JSON.parse(line))

      const commitInfoAction = actions.find((a: Record<string, unknown>) => 'commitInfo' in a)
      expect(commitInfoAction).toBeDefined()
      expect(commitInfoAction.commitInfo.timestamp).toBeDefined()
      expect(commitInfoAction.commitInfo.operation).toBeDefined()
    })

    it('should store add action with required fields', async () => {
      await backend.create('posts', { $type: 'Post', name: 'Test Post' })

      const commitData = await storage.read('warehouse/posts/_delta_log/00000000000000000000.json')
      const commitText = new TextDecoder().decode(commitData)
      const lines = commitText.trim().split('\n')
      const actions = lines.map(line => JSON.parse(line))

      const addAction = actions.find((a: Record<string, unknown>) => 'add' in a)
      expect(addAction.add.path).toBeDefined()
      expect(addAction.add.size).toBeGreaterThan(0)
      expect(addAction.add.modificationTime).toBeDefined()
      expect(addAction.add.dataChange).toBe(true)
    })
  })

  describe('Time Travel', () => {
    it('should support querying at specific version', async () => {
      const created = await backend.create('users', {
        $type: 'User',
        name: 'Alice',
        score: 100,
      })

      await backend.update('users', created.$id.split('/')[1]!, {
        $set: { score: 150 },
      })

      // Query at version 0 should show original value
      const snapshotBackend = await backend.snapshot('users', 0)
      const oldEntities = await snapshotBackend.find('users', {})
      expect(oldEntities).toHaveLength(1)
      expect(oldEntities[0]!.score).toBe(100)

      // Current version should show updated value
      const currentEntities = await backend.find('users', {})
      expect(currentEntities[0]!.score).toBe(150)
    })

    it('should support querying at specific timestamp', async () => {
      const created = await backend.create('users', {
        $type: 'User',
        name: 'Alice',
      })

      const creationTime = new Date()

      // Wait a bit and update
      await new Promise(resolve => setTimeout(resolve, 10))

      await backend.update('users', created.$id.split('/')[1]!, {
        $set: { name: 'Alicia' },
      })

      // Query at creation time should show original name
      const snapshotBackend = await backend.snapshot('users', creationTime)
      const oldEntities = await snapshotBackend.find('users', {})
      expect(oldEntities[0]!.name).toBe('Alice')
    })

    it('should list available versions/snapshots', async () => {
      await backend.create('users', { $type: 'User', name: 'Alice' })
      await backend.create('users', { $type: 'User', name: 'Bob' })

      const snapshots = await backend.listSnapshots('users')
      expect(snapshots.length).toBeGreaterThanOrEqual(2)

      // Versions should be sequential
      const versions = snapshots.map(s => s.id as number).sort((a, b) => a - b)
      expect(versions[0]).toBe(0)
      expect(versions[1]).toBe(1)
    })

    it('should have read-only snapshot backend', async () => {
      await backend.create('users', { $type: 'User', name: 'Alice' })

      const snapshotBackend = await backend.snapshot('users', 0)
      expect(snapshotBackend.readOnly).toBe(true)

      // Write operations should throw
      await expect(
        snapshotBackend.create('users', { $type: 'User', name: 'Bob' })
      ).rejects.toThrow(/read.only/i)
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

    it('should handle empty objects and arrays', async () => {
      const entity = await backend.create('posts', {
        $type: 'Post',
        name: 'Test',
        emptyObj: {},
        emptyArr: [],
      })

      const found = await backend.get('posts', entity.$id.split('/')[1]!)
      expect(found!.emptyObj).toEqual({})
      expect(found!.emptyArr).toEqual([])
    })

    it('should handle deeply nested structures', async () => {
      const entity = await backend.create('posts', {
        $type: 'Post',
        name: 'Test',
        deep: {
          level1: {
            level2: {
              level3: {
                value: 'deep value',
              },
            },
          },
        },
      })

      const found = await backend.get('posts', entity.$id.split('/')[1]!)
      expect(found!.deep).toEqual({
        level1: {
          level2: {
            level3: {
              value: 'deep value',
            },
          },
        },
      })
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

    it('should store schema in metaData action', async () => {
      await backend.create('posts', { $type: 'Post', name: 'Test' })

      const commitData = await storage.read('warehouse/posts/_delta_log/00000000000000000000.json')
      const commitText = new TextDecoder().decode(commitData)
      const lines = commitText.trim().split('\n')
      const actions = lines.map(line => JSON.parse(line))

      const metaDataAction = actions.find((a: Record<string, unknown>) => 'metaData' in a)
      expect(metaDataAction.metaData.schemaString).toBeDefined()

      // Schema should be valid JSON
      const schemaObj = JSON.parse(metaDataAction.metaData.schemaString)
      expect(schemaObj.type).toBe('struct')
      expect(schemaObj.fields).toBeInstanceOf(Array)
    })
  })

  describe('Checkpoint', () => {
    it('should create _last_checkpoint file after threshold', async () => {
      // Create enough commits to trigger checkpoint (usually every 10 commits)
      for (let i = 0; i < 15; i++) {
        await backend.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      // Check for _last_checkpoint file
      const lastCheckpointExists = await storage.exists('warehouse/posts/_delta_log/_last_checkpoint')
      expect(lastCheckpointExists).toBe(true)
    })

    it('should store checkpoint version in _last_checkpoint', async () => {
      // Create enough commits to trigger checkpoint
      for (let i = 0; i < 15; i++) {
        await backend.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      const lastCheckpointData = await storage.read('warehouse/posts/_delta_log/_last_checkpoint')
      const lastCheckpoint = JSON.parse(new TextDecoder().decode(lastCheckpointData))

      expect(lastCheckpoint.version).toBeGreaterThanOrEqual(10)
      expect(lastCheckpoint.size).toBeDefined()
    })

    it('should create checkpoint parquet file', async () => {
      // Create enough commits to trigger checkpoint
      for (let i = 0; i < 15; i++) {
        await backend.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      const deltaLogFiles = await storage.list('warehouse/posts/_delta_log/')
      const checkpointFiles = deltaLogFiles.files.filter(f => f.includes('.checkpoint.parquet'))

      expect(checkpointFiles.length).toBeGreaterThan(0)
    })
  })

  describe('Concurrent Operations', () => {
    it('should handle concurrent creates correctly', async () => {
      const promises = Array.from({ length: 10 }, (_, i) =>
        backend.create('posts', { $type: 'Post', name: `Post ${i}` })
      )

      const results = await Promise.all(promises)
      expect(results).toHaveLength(10)

      // All should have unique IDs
      const ids = results.map(e => e.$id)
      expect(new Set(ids).size).toBe(10)

      // All should be readable
      const all = await backend.find('posts', {})
      expect(all).toHaveLength(10)
    })

    it('should maintain sequential commit versions', async () => {
      await backend.create('posts', { $type: 'Post', name: 'Post 1' })
      await backend.create('posts', { $type: 'Post', name: 'Post 2' })
      await backend.create('posts', { $type: 'Post', name: 'Post 3' })

      const snapshots = await backend.listSnapshots('posts')
      const versions = snapshots.map(s => s.id as number).sort((a, b) => a - b)

      // Versions should be 0, 1, 2
      expect(versions).toEqual([0, 1, 2])
    })
  })

  describe('Backend Properties', () => {
    it('should report correct backend type', () => {
      expect(backend.type).toBe('delta')
    })

    it('should support time travel', () => {
      expect(backend.supportsTimeTravel).toBe(true)
    })

    it('should support schema evolution', () => {
      expect(backend.supportsSchemaEvolution).toBe(true)
    })

    it('should not be read-only by default', () => {
      expect(backend.readOnly).toBe(false)
    })
  })

  describe('Error Handling', () => {
    it('should throw on update of non-existent entity', async () => {
      await expect(
        backend.update('users', 'nonexistent', { $set: { name: 'Test' } })
      ).rejects.toThrow(/not found/i)
    })

    it('should throw when read-only backend attempts write', async () => {
      const readOnlyBackend = createDeltaBackend({
        type: 'delta',
        storage,
        location: 'warehouse',
        readOnly: true,
      })
      await readOnlyBackend.initialize()

      await expect(
        readOnlyBackend.create('users', { $type: 'User', name: 'Alice' })
      ).rejects.toThrow(/read.only/i)

      await readOnlyBackend.close()
    })
  })
})
