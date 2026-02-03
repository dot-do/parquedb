/**
 * Tests for BaseEntityBackend shared functionality
 *
 * These tests verify the shared functionality extracted into BaseEntityBackend,
 * including update operators, sorting, filtering, and pagination helpers.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { IcebergBackend, createIcebergBackend } from '../../../src/backends/iceberg'
import { DeltaBackend, createDeltaBackend } from '../../../src/backends/delta'
import { NativeBackend, createNativeBackend } from '../../../src/backends/native'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import type { EntityBackend } from '../../../src/backends/types'
import { ReadOnlyError } from '../../../src/backends/types'

/**
 * Test suite that runs against Iceberg, Delta, and Native backends
 * to verify BaseEntityBackend shared functionality works correctly in all.
 */
describe('BaseEntityBackend shared functionality', () => {
  // Run tests for all backend types
  const backendConfigs = [
    {
      name: 'IcebergBackend',
      createBackend: (storage: MemoryBackend) =>
        createIcebergBackend({
          type: 'iceberg',
          storage,
          warehouse: 'warehouse',
          database: 'testdb',
        }),
    },
    {
      name: 'DeltaBackend',
      createBackend: (storage: MemoryBackend) =>
        createDeltaBackend({
          type: 'delta',
          storage,
          location: 'warehouse',
        }),
    },
    {
      name: 'NativeBackend',
      createBackend: (storage: MemoryBackend) =>
        createNativeBackend({
          type: 'native',
          storage,
          location: 'warehouse',
        }),
    },
  ]

  for (const config of backendConfigs) {
    describe(`${config.name}`, () => {
      let storage: MemoryBackend
      let backend: EntityBackend

      beforeEach(async () => {
        storage = new MemoryBackend()
        backend = config.createBackend(storage)
        await backend.initialize()
      })

      afterEach(async () => {
        await backend.close()
      })

      // =======================================================================
      // Update Operators ($set, $unset, $inc)
      // =======================================================================

      describe('applyUpdate - $set operator', () => {
        it('should set new field values', async () => {
          const created = await backend.create('users', {
            $type: 'User',
            name: 'Alice',
            score: 100,
          })

          const updated = await backend.update('users', created.$id.split('/')[1]!, {
            $set: { score: 200, level: 'gold' },
          })

          expect(updated.score).toBe(200)
          expect(updated.level).toBe('gold')
        })

        it('should overwrite existing fields', async () => {
          const created = await backend.create('users', {
            $type: 'User',
            name: 'Alice',
            status: 'active',
          })

          const updated = await backend.update('users', created.$id.split('/')[1]!, {
            $set: { status: 'inactive' },
          })

          expect(updated.status).toBe('inactive')
        })
      })

      describe('applyUpdate - $unset operator', () => {
        it('should remove specified fields', async () => {
          const created = await backend.create('users', {
            $type: 'User',
            name: 'Alice',
            tempField: 'to-be-removed',
          })

          const updated = await backend.update('users', created.$id.split('/')[1]!, {
            $unset: { tempField: true },
          })

          expect('tempField' in updated).toBe(false)
        })

        it('should handle multiple unset operations', async () => {
          const created = await backend.create('users', {
            $type: 'User',
            name: 'Alice',
            field1: 'a',
            field2: 'b',
            field3: 'c',
          })

          const updated = await backend.update('users', created.$id.split('/')[1]!, {
            $unset: { field1: true, field3: true },
          })

          expect('field1' in updated).toBe(false)
          expect(updated.field2).toBe('b')
          expect('field3' in updated).toBe(false)
        })
      })

      describe('applyUpdate - $inc operator', () => {
        it('should increment numeric fields', async () => {
          const created = await backend.create('users', {
            $type: 'User',
            name: 'Alice',
            points: 100,
          })

          const updated = await backend.update('users', created.$id.split('/')[1]!, {
            $inc: { points: 50 },
          })

          expect(updated.points).toBe(150)
        })

        it('should handle negative increments', async () => {
          const created = await backend.create('users', {
            $type: 'User',
            name: 'Alice',
            balance: 1000,
          })

          const updated = await backend.update('users', created.$id.split('/')[1]!, {
            $inc: { balance: -100 },
          })

          expect(updated.balance).toBe(900)
        })

        it('should handle multiple increments', async () => {
          const created = await backend.create('users', {
            $type: 'User',
            name: 'Alice',
            xp: 100,
            gold: 500,
          })

          const updated = await backend.update('users', created.$id.split('/')[1]!, {
            $inc: { xp: 25, gold: -50 },
          })

          expect(updated.xp).toBe(125)
          expect(updated.gold).toBe(450)
        })

        it('should not affect non-numeric fields', async () => {
          const created = await backend.create('users', {
            $type: 'User',
            name: 'Alice',
            label: 'test',
          })

          const updated = await backend.update('users', created.$id.split('/')[1]!, {
            $inc: { label: 10 },
          })

          // String field should not change
          expect(updated.label).toBe('test')
        })
      })

      describe('combined update operators', () => {
        it('should handle $set and $inc together', async () => {
          const created = await backend.create('users', {
            $type: 'User',
            name: 'Alice',
            level: 1,
            title: 'Novice',
          })

          const updated = await backend.update('users', created.$id.split('/')[1]!, {
            $set: { title: 'Expert' },
            $inc: { level: 5 },
          })

          expect(updated.title).toBe('Expert')
          expect(updated.level).toBe(6)
        })

        it('should handle all operators together', async () => {
          const created = await backend.create('users', {
            $type: 'User',
            name: 'Alice',
            score: 100,
            oldField: 'delete-me',
          })

          const updated = await backend.update('users', created.$id.split('/')[1]!, {
            $set: { name: 'Alicia' },
            $inc: { score: 10 },
            $unset: { oldField: true },
          })

          expect(updated.name).toBe('Alicia')
          expect(updated.score).toBe(110)
          expect('oldField' in updated).toBe(false)
        })
      })

      // =======================================================================
      // Sorting
      // =======================================================================

      describe('sortEntities', () => {
        beforeEach(async () => {
          // Create test data
          await backend.bulkCreate('items', [
            { $type: 'Item', name: 'Charlie', score: 200, createdDate: new Date('2024-01-03') },
            { $type: 'Item', name: 'Alice', score: 300, createdDate: new Date('2024-01-01') },
            { $type: 'Item', name: 'Bob', score: 100, createdDate: new Date('2024-01-02') },
          ])
        })

        it('should sort by string field ascending', async () => {
          const results = await backend.find('items', {}, { sort: { name: 1 } })

          expect(results[0]!.name).toBe('Alice')
          expect(results[1]!.name).toBe('Bob')
          expect(results[2]!.name).toBe('Charlie')
        })

        it('should sort by string field descending', async () => {
          const results = await backend.find('items', {}, { sort: { name: -1 } })

          expect(results[0]!.name).toBe('Charlie')
          expect(results[1]!.name).toBe('Bob')
          expect(results[2]!.name).toBe('Alice')
        })

        it('should sort by numeric field ascending', async () => {
          const results = await backend.find('items', {}, { sort: { score: 1 } })

          expect(results[0]!.score).toBe(100)
          expect(results[1]!.score).toBe(200)
          expect(results[2]!.score).toBe(300)
        })

        it('should sort by numeric field descending', async () => {
          const results = await backend.find('items', {}, { sort: { score: -1 } })

          expect(results[0]!.score).toBe(300)
          expect(results[1]!.score).toBe(200)
          expect(results[2]!.score).toBe(100)
        })

        it('should handle null values in sort (nulls last)', async () => {
          await backend.create('items', {
            $type: 'Item',
            name: 'Diana',
            score: null,
          })

          const results = await backend.find('items', {}, { sort: { score: 1 } })

          // Diana with null score should be last
          expect(results[results.length - 1]!.name).toBe('Diana')
        })

        it('should sort by multiple fields', async () => {
          // Add items with same score
          await backend.bulkCreate('products', [
            { $type: 'Product', name: 'A', category: 'cat1', price: 100 },
            { $type: 'Product', name: 'B', category: 'cat2', price: 100 },
            { $type: 'Product', name: 'C', category: 'cat1', price: 50 },
          ])

          const results = await backend.find('products', {}, {
            sort: { price: 1, category: 1 },
          })

          // First by price ascending, then by category
          expect(results[0]!.name).toBe('C')  // price: 50
          expect(results[1]!.category).toBe('cat1')  // price: 100, cat1
          expect(results[2]!.category).toBe('cat2')  // price: 100, cat2
        })
      })

      // =======================================================================
      // Pagination (skip/limit)
      // =======================================================================

      describe('applyPagination', () => {
        beforeEach(async () => {
          // Create 10 items
          const items = Array.from({ length: 10 }, (_, i) => ({
            $type: 'Item',
            name: `Item ${i + 1}`,
            order: i + 1,
          }))
          await backend.bulkCreate('pages', items)
        })

        it('should apply limit', async () => {
          const results = await backend.find('pages', {}, {
            limit: 3,
            sort: { order: 1 },
          })

          expect(results).toHaveLength(3)
          expect(results[0]!.name).toBe('Item 1')
          expect(results[2]!.name).toBe('Item 3')
        })

        it('should apply skip', async () => {
          const results = await backend.find('pages', {}, {
            skip: 5,
            sort: { order: 1 },
          })

          expect(results).toHaveLength(5)
          expect(results[0]!.name).toBe('Item 6')
        })

        it('should apply skip and limit together', async () => {
          const results = await backend.find('pages', {}, {
            skip: 2,
            limit: 3,
            sort: { order: 1 },
          })

          expect(results).toHaveLength(3)
          expect(results[0]!.name).toBe('Item 3')
          expect(results[1]!.name).toBe('Item 4')
          expect(results[2]!.name).toBe('Item 5')
        })

        it('should handle skip beyond total count', async () => {
          const results = await backend.find('pages', {}, {
            skip: 100,
            sort: { order: 1 },
          })

          expect(results).toHaveLength(0)
        })
      })

      // =======================================================================
      // Read-Only Mode
      // =======================================================================

      describe('read-only mode', () => {
        let readOnlyBackend: EntityBackend

        beforeEach(async () => {
          // Create some test data first
          await backend.create('users', {
            $type: 'User',
            name: 'Alice',
          })

          // Create read-only backend pointing to same storage
          if (config.name === 'IcebergBackend') {
            readOnlyBackend = createIcebergBackend({
              type: 'iceberg',
              storage,
              warehouse: 'warehouse',
              database: 'testdb',
              readOnly: true,
            })
          } else if (config.name === 'DeltaBackend') {
            readOnlyBackend = createDeltaBackend({
              type: 'delta',
              storage,
              location: 'warehouse',
              readOnly: true,
            })
          } else {
            readOnlyBackend = createNativeBackend({
              type: 'native',
              storage,
              location: 'warehouse',
              readOnly: true,
            })
          }
          await readOnlyBackend.initialize()
        })

        afterEach(async () => {
          await readOnlyBackend.close()
        })

        it('should allow read operations', async () => {
          const results = await readOnlyBackend.find('users', {})
          expect(results).toHaveLength(1)
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
      })

      // =======================================================================
      // Default Entity Creation (for upsert)
      // =======================================================================

      describe('createDefaultEntity (upsert)', () => {
        it('should create entity on upsert when not found', async () => {
          const result = await backend.update('users', 'newid123', {
            $set: { email: 'new@example.com', active: true },
          }, { upsert: true })

          expect(result.$id).toBe('users/newid123')
          expect(result.$type).toBe('unknown')  // default type
          expect(result.email).toBe('new@example.com')
          expect(result.active).toBe(true)
          expect(result.version).toBe(1)
        })

        it('should have correct audit fields on upsert', async () => {
          const before = new Date()

          const result = await backend.update('users', 'upserttest', {
            $set: { status: 'created' },
          }, { upsert: true })

          const after = new Date()

          expect(result.createdAt.getTime()).toBeGreaterThanOrEqual(before.getTime())
          expect(result.createdAt.getTime()).toBeLessThanOrEqual(after.getTime())
          expect(result.createdBy).toMatch(/^system\//)
        })
      })

      // =======================================================================
      // Lifecycle
      // =======================================================================

      describe('lifecycle', () => {
        it('should be idempotent for multiple initialize calls', async () => {
          // Initialize again should not throw
          await backend.initialize()
          await backend.initialize()

          // Backend should still work
          const entity = await backend.create('test', {
            $type: 'Test',
            name: 'Lifecycle Test',
          })
          expect(entity).toBeDefined()
        })

        it('should reset state on close', async () => {
          await backend.create('users', {
            $type: 'User',
            name: 'Test',
          })

          await backend.close()

          // After close, need to reinitialize
          await backend.initialize()

          // Should still be able to read data
          const results = await backend.find('users', {})
          expect(results).toHaveLength(1)
        })
      })
    })
  }
})
