/**
 * IcebergBackend Integration Tests
 *
 * RED phase: These tests validate IcebergBackend functionality
 * Tests may fail until GREEN phase implements fixes.
 *
 * Test scenarios:
 * 1. Basic CRUD operations with proper Iceberg metadata
 * 2. Snapshot management and time-travel queries
 * 3. Schema evolution and field additions
 * 4. Compaction & vacuum operations
 * 5. Error handling and edge cases
 * 6. Data integrity and concurrent operations
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { IcebergBackend, createIcebergBackend } from '../../../src/backends/iceberg'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import type { Entity, EntityId } from '../../../src/types/entity'
import type { EntitySchema } from '../../../src/backends/types'

describe('IcebergBackend Integration', () => {
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

  // ===========================================================================
  // Full CRUD Cycle Tests
  // ===========================================================================

  describe('Full CRUD Cycle', () => {
    it('should complete a full entity lifecycle with persistence verification', async () => {
      // CREATE
      const created = await backend.create('users', {
        $type: 'User',
        name: 'Alice Johnson',
        email: 'alice@example.com',
        role: 'admin',
      })

      expect(created.$id).toMatch(/^users\//)
      expect(created.$type).toBe('User')
      expect(created.name).toBe('Alice Johnson')
      expect(created.email).toBe('alice@example.com')
      expect(created.version).toBe(1)

      // READ - Verify persistence
      const read = await backend.get('users', created.$id.split('/')[1]!)
      expect(read).not.toBeNull()
      expect(read!.$id).toBe(created.$id)
      expect(read!.name).toBe('Alice Johnson')
      expect(read!.email).toBe('alice@example.com')

      // UPDATE
      const updated = await backend.update('users', created.$id.split('/')[1]!, {
        $set: { role: 'superadmin', email: 'alice.j@example.com' },
      })

      expect(updated.role).toBe('superadmin')
      expect(updated.email).toBe('alice.j@example.com')
      expect(updated.version).toBe(2)

      // Verify update persisted
      const readAfterUpdate = await backend.get('users', created.$id.split('/')[1]!)
      expect(readAfterUpdate!.role).toBe('superadmin')
      expect(readAfterUpdate!.version).toBe(2)

      // DELETE (soft)
      const deleteResult = await backend.delete('users', created.$id.split('/')[1]!)
      expect(deleteResult.deletedCount).toBe(1)

      // Verify soft delete - should not appear in normal queries
      const afterDelete = await backend.find('users', {})
      expect(afterDelete).toHaveLength(0)

      // Verify soft delete - should appear with includeDeleted
      const withDeleted = await backend.find('users', {}, { includeDeleted: true })
      expect(withDeleted).toHaveLength(1)
      expect(withDeleted[0]!.deletedAt).toBeInstanceOf(Date)
    })

    it('should maintain data integrity across multiple operations', async () => {
      // Create multiple entities
      const entity1 = await backend.create('items', {
        $type: 'Item',
        name: 'Item 1',
        value: 100,
      })

      const entity2 = await backend.create('items', {
        $type: 'Item',
        name: 'Item 2',
        value: 200,
      })

      // Update one entity
      await backend.update('items', entity1.$id.split('/')[1]!, {
        $set: { value: 150 },
      })

      // Verify both entities exist with correct values
      const all = await backend.find('items', {})
      expect(all).toHaveLength(2)

      const item1 = all.find(e => e.$id === entity1.$id)
      const item2 = all.find(e => e.$id === entity2.$id)

      expect(item1!.value).toBe(150)
      expect(item2!.value).toBe(200)
    })

    it('should handle create-read cycle with complex nested data', async () => {
      const created = await backend.create('documents', {
        $type: 'Document',
        name: 'Complex Doc',
        metadata: {
          author: { name: 'John', email: 'john@example.com' },
          tags: ['tech', 'database', 'parquet'],
          stats: { views: 1000, likes: 50, shares: 10 },
          history: [
            { action: 'created', timestamp: '2024-01-01' },
            { action: 'edited', timestamp: '2024-01-02' },
          ],
        },
        settings: {
          public: true,
          allowComments: false,
        },
      })

      const read = await backend.get('documents', created.$id.split('/')[1]!)

      expect(read).not.toBeNull()
      expect(read!.metadata).toEqual({
        author: { name: 'John', email: 'john@example.com' },
        tags: ['tech', 'database', 'parquet'],
        stats: { views: 1000, likes: 50, shares: 10 },
        history: [
          { action: 'created', timestamp: '2024-01-01' },
          { action: 'edited', timestamp: '2024-01-02' },
        ],
      })
      expect(read!.settings).toEqual({
        public: true,
        allowComments: false,
      })
    })
  })

  // ===========================================================================
  // Time Travel Tests
  // ===========================================================================

  describe('Time Travel Queries', () => {
    it('should query data at specific snapshot', async () => {
      // Create initial entity
      const entity = await backend.create('users', {
        $type: 'User',
        name: 'Alice',
        score: 100,
      })

      // Get snapshot after first create
      const snapshotsAfterCreate = await backend.listSnapshots('users')
      const firstSnapshotId = snapshotsAfterCreate[snapshotsAfterCreate.length - 1]!.id

      // Update the entity
      await backend.update('users', entity.$id.split('/')[1]!, {
        $set: { score: 200, name: 'Alice Updated' },
      })

      // Current data should show updated values
      const current = await backend.get('users', entity.$id.split('/')[1]!)
      expect(current!.score).toBe(200)
      expect(current!.name).toBe('Alice Updated')

      // Query at first snapshot should show original values
      const historicalBackend = await backend.snapshot('users', firstSnapshotId as number)
      const historical = await historicalBackend.get('users', entity.$id.split('/')[1]!)

      expect(historical!.score).toBe(100)
      expect(historical!.name).toBe('Alice')
    })

    it('should query data at specific timestamp', async () => {
      // Create initial entity
      await backend.create('items', {
        $type: 'Item',
        name: 'Time Travel Item',
        value: 10,
      })

      // Record timestamp after first operation
      const timestampAfterCreate = new Date()

      // Wait a bit and update
      await new Promise(resolve => setTimeout(resolve, 10))

      const entities = await backend.find('items', {})
      const entity = entities[0]!

      await backend.update('items', entity.$id.split('/')[1]!, {
        $set: { value: 20 },
      })

      // Query at timestamp should return original value
      const historicalBackend = await backend.snapshot('items', timestampAfterCreate)
      const historical = await historicalBackend.find('items', {})

      expect(historical).toHaveLength(1)
      expect(historical[0]!.value).toBe(10)
    })

    it('should not find entities that did not exist at historical snapshot', async () => {
      // Create first entity
      await backend.create('items', {
        $type: 'Item',
        name: 'First Item',
      })

      const snapshotsAfterFirst = await backend.listSnapshots('items')
      const firstSnapshotId = snapshotsAfterFirst[snapshotsAfterFirst.length - 1]!.id

      // Create second entity
      await backend.create('items', {
        $type: 'Item',
        name: 'Second Item',
      })

      // Current should have 2 items
      const current = await backend.find('items', {})
      expect(current).toHaveLength(2)

      // Historical should have only 1 item
      const historicalBackend = await backend.snapshot('items', firstSnapshotId as number)
      const historical = await historicalBackend.find('items', {})

      expect(historical).toHaveLength(1)
      expect(historical[0]!.name).toBe('First Item')
    })

    it('should throw error for non-existent snapshot', async () => {
      await backend.create('items', {
        $type: 'Item',
        name: 'Test',
      })

      await expect(
        backend.snapshot('items', 999999999)
      ).rejects.toThrow(/not found/)
    })
  })

  // ===========================================================================
  // Multiple Snapshots Tests
  // ===========================================================================

  describe('Multiple Snapshots', () => {
    it('should create new snapshot for each write operation', async () => {
      // Initial state - no snapshots
      const initialSnapshots = await backend.listSnapshots('users')
      expect(initialSnapshots).toHaveLength(0)

      // Create entity - should create snapshot
      const entity = await backend.create('users', {
        $type: 'User',
        name: 'Alice',
      })

      const snapshotsAfterCreate = await backend.listSnapshots('users')
      expect(snapshotsAfterCreate.length).toBe(1)

      // Update entity - should create another snapshot
      await backend.update('users', entity.$id.split('/')[1]!, {
        $set: { name: 'Alice Updated' },
      })

      const snapshotsAfterUpdate = await backend.listSnapshots('users')
      expect(snapshotsAfterUpdate.length).toBe(2)

      // Delete entity - should create another snapshot
      await backend.delete('users', entity.$id.split('/')[1]!)

      const snapshotsAfterDelete = await backend.listSnapshots('users')
      expect(snapshotsAfterDelete.length).toBe(3)
    })

    it('should track snapshot history with correct operations', async () => {
      const entity = await backend.create('items', {
        $type: 'Item',
        name: 'Tracked Item',
        count: 0,
      })

      // Multiple updates
      await backend.update('items', entity.$id.split('/')[1]!, { $set: { count: 1 } })
      await backend.update('items', entity.$id.split('/')[1]!, { $set: { count: 2 } })
      await backend.update('items', entity.$id.split('/')[1]!, { $set: { count: 3 } })

      const snapshots = await backend.listSnapshots('items')

      expect(snapshots.length).toBe(4) // 1 create + 3 updates
      expect(snapshots[0]!.operation).toBe('append')
      expect(snapshots[0]!.recordCount).toBe(1)
    })

    it('should preserve snapshot timestamps in chronological order', async () => {
      await backend.create('items', { $type: 'Item', name: 'Item 1' })
      await new Promise(resolve => setTimeout(resolve, 5))
      await backend.create('items', { $type: 'Item', name: 'Item 2' })
      await new Promise(resolve => setTimeout(resolve, 5))
      await backend.create('items', { $type: 'Item', name: 'Item 3' })

      const snapshots = await backend.listSnapshots('items')

      expect(snapshots.length).toBe(3)

      // Verify chronological order
      for (let i = 1; i < snapshots.length; i++) {
        expect(snapshots[i]!.timestamp.getTime()).toBeGreaterThanOrEqual(
          snapshots[i - 1]!.timestamp.getTime()
        )
      }
    })

    it('should query any historical snapshot in the chain', async () => {
      const entity = await backend.create('counters', {
        $type: 'Counter',
        name: 'Test Counter',
        value: 0,
      })

      // Create series of updates
      for (let i = 1; i <= 5; i++) {
        await backend.update('counters', entity.$id.split('/')[1]!, {
          $set: { value: i * 10 },
        })
      }

      const snapshots = await backend.listSnapshots('counters')
      expect(snapshots.length).toBe(6) // 1 create + 5 updates

      // Query snapshot after creation (value = 0)
      const snapshot0 = await backend.snapshot('counters', snapshots[0]!.id as number)
      const data0 = await snapshot0.get('counters', entity.$id.split('/')[1]!)
      expect(data0!.value).toBe(0)

      // Query snapshot after 3rd update (value = 30)
      const snapshot3 = await backend.snapshot('counters', snapshots[3]!.id as number)
      const data3 = await snapshot3.get('counters', entity.$id.split('/')[1]!)
      expect(data3!.value).toBe(30)

      // Query latest (value = 50)
      const current = await backend.get('counters', entity.$id.split('/')[1]!)
      expect(current!.value).toBe(50)
    })
  })

  // ===========================================================================
  // Schema Operations Tests
  // ===========================================================================

  describe('Schema Operations', () => {
    it('should auto-create schema when first entity is created', async () => {
      // No schema before any entities
      const schemaBefore = await backend.getSchema('newcollection')
      expect(schemaBefore).toBeNull()

      // Create entity
      await backend.create('newcollection', {
        $type: 'Thing',
        name: 'First Thing',
        customField: 'value',
      })

      // Schema should exist now
      const schemaAfter = await backend.getSchema('newcollection')
      expect(schemaAfter).not.toBeNull()
      expect(schemaAfter!.name).toBe('newcollection')
    })

    it('should include standard entity fields in schema', async () => {
      await backend.create('items', {
        $type: 'Item',
        name: 'Schema Test',
      })

      const schema = await backend.getSchema('items')

      expect(schema).not.toBeNull()

      const fieldNames = schema!.fields.map(f => f.name)
      expect(fieldNames).toContain('$id')
      expect(fieldNames).toContain('$type')
      expect(fieldNames).toContain('name')
      expect(fieldNames).toContain('createdAt')
      expect(fieldNames).toContain('updatedAt')
      expect(fieldNames).toContain('version')
    })

    it('should list all namespaces after creating multiple tables', async () => {
      await backend.create('users', { $type: 'User', name: 'Alice' })
      await backend.create('posts', { $type: 'Post', name: 'Post 1' })
      await backend.create('comments', { $type: 'Comment', name: 'Comment 1' })
      await backend.create('tags', { $type: 'Tag', name: 'Tag 1' })

      const namespaces = await backend.listNamespaces()

      expect(namespaces).toContain('users')
      expect(namespaces).toContain('posts')
      expect(namespaces).toContain('comments')
      expect(namespaces).toContain('tags')
      expect(namespaces.length).toBe(4)
    })

    it('should return empty array for listNamespaces when no tables exist', async () => {
      const namespaces = await backend.listNamespaces()
      expect(namespaces).toEqual([])
    })

    it('should return null schema for non-existent namespace', async () => {
      const schema = await backend.getSchema('nonexistent')
      expect(schema).toBeNull()
    })
  })

  // ===========================================================================
  // Bulk Operations Tests
  // ===========================================================================

  describe('Bulk Operations', () => {
    describe('bulkCreate', () => {
      it('should create multiple entities atomically', async () => {
        const entities = await backend.bulkCreate('users', [
          { $type: 'User', name: 'Alice', role: 'admin' },
          { $type: 'User', name: 'Bob', role: 'user' },
          { $type: 'User', name: 'Charlie', role: 'user' },
          { $type: 'User', name: 'Diana', role: 'moderator' },
        ])

        expect(entities).toHaveLength(4)

        // All should have unique IDs
        const ids = entities.map(e => e.$id)
        expect(new Set(ids).size).toBe(4)

        // Verify all persisted
        const all = await backend.find('users', {})
        expect(all).toHaveLength(4)
      })

      it('should create single snapshot for bulk operation', async () => {
        await backend.bulkCreate('items', [
          { $type: 'Item', name: 'Item 1' },
          { $type: 'Item', name: 'Item 2' },
          { $type: 'Item', name: 'Item 3' },
        ])

        const snapshots = await backend.listSnapshots('items')

        // Should be a single snapshot, not 3
        expect(snapshots.length).toBe(1)
        expect(snapshots[0]!.recordCount).toBe(3)
      })

      it('should preserve bulk created data after read', async () => {
        const input = [
          { $type: 'Product', name: 'Widget A', price: 10.99, quantity: 100 },
          { $type: 'Product', name: 'Widget B', price: 25.50, quantity: 50 },
          { $type: 'Product', name: 'Widget C', price: 5.00, quantity: 200 },
        ]

        await backend.bulkCreate('products', input)

        const products = await backend.find('products', {})

        expect(products).toHaveLength(3)

        const widgetA = products.find(p => p.name === 'Widget A')
        expect(widgetA!.price).toBe(10.99)
        expect(widgetA!.quantity).toBe(100)
      })
    })

    describe('bulkUpdate', () => {
      beforeEach(async () => {
        await backend.bulkCreate('items', [
          { $type: 'Item', name: 'Item 1', status: 'pending', value: 10 },
          { $type: 'Item', name: 'Item 2', status: 'pending', value: 20 },
          { $type: 'Item', name: 'Item 3', status: 'active', value: 30 },
          { $type: 'Item', name: 'Item 4', status: 'pending', value: 40 },
        ])
      })

      it('should update all matching entities', async () => {
        const result = await backend.bulkUpdate(
          'items',
          { status: 'pending' },
          { $set: { status: 'processed' } }
        )

        expect(result.matchedCount).toBe(3)
        expect(result.modifiedCount).toBe(3)

        // Verify updates
        const processed = await backend.find('items', { status: 'processed' })
        expect(processed).toHaveLength(3)

        const pending = await backend.find('items', { status: 'pending' })
        expect(pending).toHaveLength(0)
      })

      it('should increment version for all updated entities', async () => {
        await backend.bulkUpdate(
          'items',
          { status: 'pending' },
          { $set: { status: 'done' } }
        )

        const updated = await backend.find('items', { status: 'done' })

        for (const entity of updated) {
          expect(entity.version).toBe(2)
        }
      })

      it('should return zero counts when no entities match', async () => {
        const result = await backend.bulkUpdate(
          'items',
          { status: 'nonexistent' },
          { $set: { value: 999 } }
        )

        expect(result.matchedCount).toBe(0)
        expect(result.modifiedCount).toBe(0)
      })
    })

    describe('bulkDelete', () => {
      beforeEach(async () => {
        await backend.bulkCreate('items', [
          { $type: 'Item', name: 'Keep 1', category: 'keep' },
          { $type: 'Item', name: 'Delete 1', category: 'delete' },
          { $type: 'Item', name: 'Keep 2', category: 'keep' },
          { $type: 'Item', name: 'Delete 2', category: 'delete' },
          { $type: 'Item', name: 'Delete 3', category: 'delete' },
        ])
      })

      it('should soft delete all matching entities', async () => {
        const result = await backend.bulkDelete('items', { category: 'delete' })

        expect(result.deletedCount).toBe(3)

        // Verify deletions
        const remaining = await backend.find('items', {})
        expect(remaining).toHaveLength(2)

        const deleted = await backend.find('items', {}, { includeDeleted: true })
        expect(deleted).toHaveLength(5)
      })

      it('should return zero when no entities match', async () => {
        const result = await backend.bulkDelete('items', { category: 'nonexistent' })

        expect(result.deletedCount).toBe(0)

        const all = await backend.find('items', {})
        expect(all).toHaveLength(5)
      })

      it('should preserve non-matching entities', async () => {
        await backend.bulkDelete('items', { category: 'delete' })

        const kept = await backend.find('items', { category: 'keep' })
        expect(kept).toHaveLength(2)
        expect(kept.map(k => k.name).sort()).toEqual(['Keep 1', 'Keep 2'])
      })
    })
  })

  // ===========================================================================
  // Stats and Maintenance Tests
  // ===========================================================================

  describe('Stats and Maintenance', () => {
    describe('stats()', () => {
      it('should return zero stats for empty namespace', async () => {
        const stats = await backend.stats('empty')

        expect(stats.recordCount).toBe(0)
        expect(stats.fileCount).toBe(0)
        expect(stats.snapshotCount).toBe(0)
      })

      it('should track record count correctly', async () => {
        await backend.bulkCreate('items', [
          { $type: 'Item', name: 'Item 1' },
          { $type: 'Item', name: 'Item 2' },
          { $type: 'Item', name: 'Item 3' },
        ])

        const stats = await backend.stats('items')

        expect(stats.recordCount).toBe(3)
        expect(stats.snapshotCount).toBe(1)
      })

      it('should update stats after operations', async () => {
        // Create some entities
        await backend.create('items', { $type: 'Item', name: 'Item 1' })
        await backend.create('items', { $type: 'Item', name: 'Item 2' })

        let stats = await backend.stats('items')
        expect(stats.recordCount).toBe(2)
        expect(stats.snapshotCount).toBe(2)

        // Create more
        await backend.create('items', { $type: 'Item', name: 'Item 3' })

        stats = await backend.stats('items')
        expect(stats.recordCount).toBe(3)
        expect(stats.snapshotCount).toBe(3)
      })

      it('should track file count', async () => {
        // Each operation creates a new file
        await backend.create('items', { $type: 'Item', name: 'Item 1' })
        await backend.create('items', { $type: 'Item', name: 'Item 2' })
        await backend.create('items', { $type: 'Item', name: 'Item 3' })

        const stats = await backend.stats('items')

        expect(stats.fileCount).toBeGreaterThan(0)
      })

      it('should include lastModified timestamp', async () => {
        const beforeCreate = Date.now()

        await backend.create('items', { $type: 'Item', name: 'Test' })

        const stats = await backend.stats('items')

        expect(stats.lastModified).toBeInstanceOf(Date)
        expect(stats.lastModified!.getTime()).toBeGreaterThanOrEqual(beforeCreate)
      })
    })

    describe('listSnapshots()', () => {
      it('should return empty array for non-existent namespace', async () => {
        const snapshots = await backend.listSnapshots('nonexistent')
        expect(snapshots).toEqual([])
      })

      it('should include snapshot metadata', async () => {
        await backend.bulkCreate('items', [
          { $type: 'Item', name: 'Item 1' },
          { $type: 'Item', name: 'Item 2' },
        ])

        const snapshots = await backend.listSnapshots('items')

        expect(snapshots.length).toBe(1)
        expect(snapshots[0]).toMatchObject({
          id: expect.any(Number),
          timestamp: expect.any(Date),
          operation: 'append',
          recordCount: 2,
        })
      })

      it('should list all snapshots in order', async () => {
        await backend.create('items', { $type: 'Item', name: 'Item 1' })
        await backend.create('items', { $type: 'Item', name: 'Item 2' })
        await backend.create('items', { $type: 'Item', name: 'Item 3' })

        const snapshots = await backend.listSnapshots('items')

        expect(snapshots.length).toBe(3)

        // Verify order
        for (let i = 1; i < snapshots.length; i++) {
          expect(snapshots[i]!.id).toBeGreaterThan(snapshots[i - 1]!.id as number)
        }
      })
    })
  })

  // ===========================================================================
  // Iceberg Metadata Integrity Tests
  // ===========================================================================

  describe('Iceberg Metadata Integrity', () => {
    it('should create valid Iceberg table structure', async () => {
      await backend.create('items', {
        $type: 'Item',
        name: 'Test',
      })

      // Verify metadata directory exists
      const metadataFiles = await storage.list('warehouse/testdb/items/metadata/')
      expect(metadataFiles.files.length).toBeGreaterThan(0)

      // Verify version-hint.text exists
      const versionHintExists = await storage.exists(
        'warehouse/testdb/items/metadata/version-hint.text'
      )
      expect(versionHintExists).toBe(true)
    })

    it('should create valid data files in Parquet format', async () => {
      await backend.create('items', {
        $type: 'Item',
        name: 'Test',
      })

      const dataFiles = await storage.list('warehouse/testdb/items/data/')

      expect(dataFiles.files.length).toBeGreaterThan(0)
      expect(dataFiles.files[0]).toMatch(/\.parquet$/)
    })

    it('should maintain manifest list for each snapshot', async () => {
      await backend.create('items', { $type: 'Item', name: 'Item 1' })
      await backend.create('items', { $type: 'Item', name: 'Item 2' })

      const snapshots = await backend.listSnapshots('items')

      // Each snapshot should have a manifest list
      expect(snapshots.length).toBe(2)

      // Verify manifest list files exist
      const metadataFiles = await storage.list('warehouse/testdb/items/metadata/')
      const snapFiles = metadataFiles.files.filter(f => f.includes('snap-'))

      expect(snapFiles.length).toBe(2)
    })

    it('should update metadata version correctly', async () => {
      await backend.create('items', { $type: 'Item', name: 'Item 1' })

      // Read version hint
      const versionHint1 = await storage.read(
        'warehouse/testdb/items/metadata/version-hint.text'
      )
      const versionPath1 = new TextDecoder().decode(versionHint1)

      await backend.create('items', { $type: 'Item', name: 'Item 2' })

      const versionHint2 = await storage.read(
        'warehouse/testdb/items/metadata/version-hint.text'
      )
      const versionPath2 = new TextDecoder().decode(versionHint2)

      // Version path should have changed
      expect(versionPath2).not.toBe(versionPath1)
    })

    it('should preserve parent snapshot reference', async () => {
      await backend.create('items', { $type: 'Item', name: 'Item 1' })
      await backend.create('items', { $type: 'Item', name: 'Item 2' })
      await backend.create('items', { $type: 'Item', name: 'Item 3' })

      const snapshots = await backend.listSnapshots('items')

      expect(snapshots.length).toBe(3)

      // Read metadata to verify parent references
      const versionHint = await storage.read(
        'warehouse/testdb/items/metadata/version-hint.text'
      )
      const metadataPath = new TextDecoder().decode(versionHint)
      const metadataContent = await storage.read(metadataPath)
      const metadata = JSON.parse(new TextDecoder().decode(metadataContent))

      // Should have snapshot-log tracking the history
      expect(metadata['snapshot-log'].length).toBe(3)
    })
  })

  // ===========================================================================
  // Error Handling and Edge Cases
  // ===========================================================================

  describe('Error Handling', () => {
    it('should handle update of non-existent entity', async () => {
      await expect(
        backend.update('items', 'nonexistent', { $set: { name: 'Test' } })
      ).rejects.toThrow(/not found/)
    })

    it('should return 0 for delete of non-existent entity', async () => {
      const result = await backend.delete('items', 'nonexistent')
      expect(result.deletedCount).toBe(0)
    })

    it('should return null for get of non-existent entity', async () => {
      const entity = await backend.get('items', 'nonexistent')
      expect(entity).toBeNull()
    })

    it('should throw on write operations when readOnly', async () => {
      const readOnlyBackend = createIcebergBackend({
        type: 'iceberg',
        storage,
        warehouse: 'warehouse',
        database: 'testdb',
        readOnly: true,
      })
      await readOnlyBackend.initialize()

      await expect(
        readOnlyBackend.create('items', { $type: 'Item', name: 'Test' })
      ).rejects.toThrow(/read-only/)

      await readOnlyBackend.close()
    })
  })

  // ===========================================================================
  // Concurrent Operations Tests
  // ===========================================================================

  describe('Concurrent Operations', () => {
    it('should handle concurrent creates to same namespace', async () => {
      const createPromises = Array.from({ length: 10 }, (_, i) =>
        backend.create('items', {
          $type: 'Item',
          name: `Concurrent Item ${i}`,
          index: i,
        })
      )

      const results = await Promise.all(createPromises)

      expect(results).toHaveLength(10)

      // All IDs should be unique
      const ids = results.map(r => r.$id)
      expect(new Set(ids).size).toBe(10)

      // All should be persisted
      const all = await backend.find('items', {})
      expect(all).toHaveLength(10)
    })

    it('should handle concurrent operations across namespaces', async () => {
      const operations = [
        backend.create('users', { $type: 'User', name: 'User 1' }),
        backend.create('posts', { $type: 'Post', name: 'Post 1' }),
        backend.create('comments', { $type: 'Comment', name: 'Comment 1' }),
        backend.create('users', { $type: 'User', name: 'User 2' }),
        backend.create('posts', { $type: 'Post', name: 'Post 2' }),
      ]

      await Promise.all(operations)

      const users = await backend.find('users', {})
      const posts = await backend.find('posts', {})
      const comments = await backend.find('comments', {})

      expect(users).toHaveLength(2)
      expect(posts).toHaveLength(2)
      expect(comments).toHaveLength(1)
    })
  })
})
