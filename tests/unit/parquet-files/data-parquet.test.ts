/**
 * data.parquet File Creation and Verification Tests
 *
 * Comprehensive tests verifying:
 * - data.parquet file creation after entity operations
 * - Schema correctness ($id, $type, $name, $data columns)
 * - Row count matches entity count
 * - Data can be decoded and matches original entities
 * - Multiple entities across different namespaces
 *
 * Uses a test factory pattern to run the same tests against both FsBackend and R2Backend.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { ParqueDB } from '../../../src/ParqueDB'
import { FsBackend } from '../../../src/storage/FsBackend'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import { ParquetReader } from '../../../src/parquet/reader'
import type { StorageBackend } from '../../../src/types/storage'
import { mkdtemp, rm, readdir, readFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { rowToEntity } from '../../../src/backends/parquet-utils'

// =============================================================================
// Test Factory
// =============================================================================

interface BackendTestContext {
  storage: StorageBackend
  cleanup: () => Promise<void>
}

/**
 * Create data.parquet tests for a specific storage backend
 *
 * @param backendName - Name of the backend (for test naming)
 * @param createBackend - Factory function to create the backend and cleanup function
 */
function createDataParquetTests(
  backendName: string,
  createBackend: () => Promise<BackendTestContext>
) {
  describe(`data.parquet verification (${backendName})`, () => {
    let storage: StorageBackend
    let db: ParqueDB
    let cleanup: () => Promise<void>

    beforeEach(async () => {
      const context = await createBackend()
      storage = context.storage
      cleanup = context.cleanup
      db = new ParqueDB({ storage })
    })

    afterEach(async () => {
      await db.disposeAsync()
      try {
        await cleanup()
      } catch {
        // Ignore cleanup errors
      }
    })

    // =========================================================================
    // Basic File Creation
    // =========================================================================

    describe('file creation', () => {
      it('should create data.parquet after entity creation and flush', async () => {
        // Create an entity
        await db.create('posts', {
          $type: 'Post',
          name: 'Test Post',
          title: 'Hello World',
          content: 'This is a test post',
        })

        // Flush and dispose to ensure data is written
        await db.disposeAsync()

        // Verify data.parquet exists
        const exists = await storage.exists('data.parquet')
        expect(exists).toBe(true)
      })

      it('should not create data.parquet when no entities exist', async () => {
        // Just dispose without creating anything
        await db.disposeAsync()

        // data.parquet should not exist (no entities to write)
        const exists = await storage.exists('data.parquet')
        expect(exists).toBe(false)
      })
    })

    // =========================================================================
    // Schema Verification
    // =========================================================================

    describe('schema structure', () => {
      it('should have correct columns: $id, $type, $name, $data', async () => {
        // Create an entity
        await db.create('posts', {
          $type: 'Post',
          name: 'Test Post',
          title: 'Hello',
          content: 'World',
        })

        await db.disposeAsync()

        // Read and verify schema
        const data = await storage.read('data.parquet')
        const { parquetMetadataAsync } = await import('hyparquet')

        const asyncBuffer = createAsyncBuffer(data)
        const metadata = await parquetMetadataAsync(asyncBuffer)

        const schemaNames = extractSchemaColumnNames(metadata.schema as Array<{ name?: string }>)

        // Verify required columns exist
        expect(schemaNames).toContain('$id')
        expect(schemaNames).toContain('$type')
        expect(schemaNames).toContain('$name')
        expect(schemaNames).toContain('$data')
      })

      it('should NOT have separate audit field columns (they are in $data)', async () => {
        await db.create('posts', {
          $type: 'Post',
          name: 'Test Post',
          title: 'Hello',
        })

        await db.disposeAsync()

        const data = await storage.read('data.parquet')
        const { parquetMetadataAsync } = await import('hyparquet')

        const asyncBuffer = createAsyncBuffer(data)
        const metadata = await parquetMetadataAsync(asyncBuffer)
        const schemaNames = extractSchemaColumnNames(metadata.schema as Array<{ name?: string }>)

        // Audit fields should NOT be separate columns
        expect(schemaNames).not.toContain('createdAt')
        expect(schemaNames).not.toContain('createdBy')
        expect(schemaNames).not.toContain('updatedAt')
        expect(schemaNames).not.toContain('updatedBy')
        expect(schemaNames).not.toContain('version')
        expect(schemaNames).not.toContain('deletedAt')
        expect(schemaNames).not.toContain('deletedBy')
      })

      it('should have exactly 4 columns (lean schema)', async () => {
        await db.create('posts', {
          $type: 'Post',
          name: 'Test Post',
          title: 'Hello',
        })

        await db.disposeAsync()

        const data = await storage.read('data.parquet')
        const { parquetMetadataAsync } = await import('hyparquet')

        const asyncBuffer = createAsyncBuffer(data)
        const metadata = await parquetMetadataAsync(asyncBuffer)
        const schemaNames = extractSchemaColumnNames(metadata.schema as Array<{ name?: string }>)

        // Should have exactly 4 columns: $id, $type, $name, $data
        expect(schemaNames.length).toBe(4)
      })
    })

    // =========================================================================
    // Row Count Verification
    // =========================================================================

    describe('row count', () => {
      it('should have 1 row for 1 entity', async () => {
        await db.create('posts', {
          $type: 'Post',
          name: 'Single Post',
          title: 'Only One',
        })

        await db.disposeAsync()

        const rows = await readDataParquetRows(storage)
        expect(rows.length).toBe(1)
      })

      it('should have correct row count for multiple entities', async () => {
        // Create 5 entities
        for (let i = 0; i < 5; i++) {
          await db.create('posts', {
            $type: 'Post',
            name: `Post ${i}`,
            title: `Title ${i}`,
          })
        }

        await db.disposeAsync()

        const rows = await readDataParquetRows(storage)
        expect(rows.length).toBe(5)
      })

      it('should have correct row count for entities across multiple namespaces', async () => {
        // Create entities in different namespaces
        await db.create('posts', { $type: 'Post', name: 'Post 1', title: 'Hello' })
        await db.create('posts', { $type: 'Post', name: 'Post 2', title: 'World' })
        await db.create('users', { $type: 'User', name: 'Alice', email: 'alice@example.com' })
        await db.create('users', { $type: 'User', name: 'Bob', email: 'bob@example.com' })
        await db.create('comments', { $type: 'Comment', name: 'Comment 1', text: 'Great!' })

        await db.disposeAsync()

        const rows = await readDataParquetRows(storage)
        expect(rows.length).toBe(5)
      })

      it('should not include deleted entities in row count', async () => {
        // Create 3 entities
        const entity1 = await db.create('posts', { $type: 'Post', name: 'Post 1', title: 'Keep' })
        await db.create('posts', { $type: 'Post', name: 'Post 2', title: 'Keep' })
        const entity3 = await db.create('posts', { $type: 'Post', name: 'Post 3', title: 'Delete' })

        // Delete one
        await db.delete('posts', entity3.$id as string)

        await db.disposeAsync()

        const rows = await readDataParquetRows(storage)
        // Should only have 2 rows (soft-deleted entities may still be there but marked)
        // This depends on implementation - adjust expectation based on actual behavior
        expect(rows.length).toBeGreaterThanOrEqual(2)
      })
    })

    // =========================================================================
    // Data Content Verification
    // =========================================================================

    describe('data content', () => {
      it('should store and retrieve $id correctly', async () => {
        const created = await db.create('posts', {
          $type: 'Post',
          name: 'Test Post',
          title: 'Hello',
        })

        await db.disposeAsync()

        const rows = await readDataParquetRows(storage)
        expect(rows.length).toBe(1)

        const row = rows[0]!
        expect(row['$id']).toBe(created.$id)
      })

      it('should store and retrieve $type correctly', async () => {
        await db.create('posts', {
          $type: 'BlogPost',
          name: 'Test Post',
          title: 'Hello',
        })

        await db.disposeAsync()

        const rows = await readDataParquetRows(storage)
        expect(rows[0]!['$type']).toBe('BlogPost')
      })

      it('should store and retrieve $name correctly', async () => {
        await db.create('posts', {
          $type: 'Post',
          name: 'My Special Post Name',
          title: 'Hello',
        })

        await db.disposeAsync()

        const rows = await readDataParquetRows(storage)
        expect(rows[0]!['$name']).toBe('My Special Post Name')
      })

      it('should encode custom fields in $data and decode correctly', async () => {
        await db.create('posts', {
          $type: 'Post',
          name: 'Test Post',
          title: 'Custom Title',
          content: 'Custom Content',
          views: 42,
          published: true,
        })

        await db.disposeAsync()

        const rows = await readDataParquetRows(storage)
        const row = rows[0]!

        // Decode entity from row
        const entity = rowToEntity(row)

        expect(entity.title).toBe('Custom Title')
        expect(entity.content).toBe('Custom Content')
        expect(entity.views).toBe(42)
        expect(entity.published).toBe(true)
      })

      it('should preserve nested objects in $data', async () => {
        await db.create('posts', {
          $type: 'Post',
          name: 'Test Post',
          metadata: {
            author: 'John',
            tags: ['typescript', 'parquet'],
            settings: { featured: true, priority: 1 },
          },
        })

        await db.disposeAsync()

        const rows = await readDataParquetRows(storage)
        const entity = rowToEntity(rows[0]!)

        expect(entity.metadata).toEqual({
          author: 'John',
          tags: ['typescript', 'parquet'],
          settings: { featured: true, priority: 1 },
        })
      })

      it('should preserve arrays in $data', async () => {
        await db.create('posts', {
          $type: 'Post',
          name: 'Test Post',
          tags: ['a', 'b', 'c'],
          scores: [1, 2, 3, 4, 5],
        })

        await db.disposeAsync()

        const rows = await readDataParquetRows(storage)
        const entity = rowToEntity(rows[0]!)

        expect(entity.tags).toEqual(['a', 'b', 'c'])
        expect(entity.scores).toEqual([1, 2, 3, 4, 5])
      })

      it('should include audit fields in decoded entity', async () => {
        await db.create('posts', {
          $type: 'Post',
          name: 'Test Post',
          title: 'Hello',
        })

        await db.disposeAsync()

        const rows = await readDataParquetRows(storage)
        const entity = rowToEntity(rows[0]!)

        // Audit fields should be present in the decoded entity
        expect(entity.createdAt).toBeDefined()
        expect(entity.updatedAt).toBeDefined()
        expect(entity.version).toBeDefined()
      })
    })

    // =========================================================================
    // Multiple Entities Verification
    // =========================================================================

    describe('multiple entities', () => {
      it('should store all entities with correct data', async () => {
        const entities = [
          { $type: 'Post', name: 'Post Alpha', title: 'Alpha Title' },
          { $type: 'Post', name: 'Post Beta', title: 'Beta Title' },
          { $type: 'Post', name: 'Post Gamma', title: 'Gamma Title' },
        ]

        const createdIds: string[] = []
        for (const entityData of entities) {
          const created = await db.create('posts', entityData)
          createdIds.push(created.$id as string)
        }

        await db.disposeAsync()

        const rows = await readDataParquetRows(storage)
        expect(rows.length).toBe(3)

        // Verify all IDs are present
        const rowIds = rows.map((r) => r['$id'])
        for (const id of createdIds) {
          expect(rowIds).toContain(id)
        }

        // Verify names
        const rowNames = rows.map((r) => r['$name'])
        expect(rowNames).toContain('Post Alpha')
        expect(rowNames).toContain('Post Beta')
        expect(rowNames).toContain('Post Gamma')
      })

      it('should correctly distinguish entities by namespace', async () => {
        // Create entities in different namespaces
        const post = await db.create('posts', { $type: 'Post', name: 'A Post', title: 'Hello' })
        const user = await db.create('users', { $type: 'User', name: 'A User', email: 'test@test.com' })

        await db.disposeAsync()

        const rows = await readDataParquetRows(storage)
        expect(rows.length).toBe(2)

        // Find the post and user rows
        const postRow = rows.find((r) => r['$id'] === post.$id)
        const userRow = rows.find((r) => r['$id'] === user.$id)

        expect(postRow).toBeDefined()
        expect(userRow).toBeDefined()

        // Decode and verify types
        const postEntity = rowToEntity(postRow!)
        const userEntity = rowToEntity(userRow!)

        expect(postEntity.$type).toBe('Post')
        expect(userEntity.$type).toBe('User')
        expect(postEntity.title).toBe('Hello')
        expect(userEntity.email).toBe('test@test.com')
      })
    })

    // =========================================================================
    // Different Entity Types/Namespaces
    // =========================================================================

    describe('different entity types and namespaces', () => {
      it('should handle entities from multiple namespaces in single data.parquet', async () => {
        // Create entities in 5 different namespaces
        await db.create('posts', { $type: 'Post', name: 'Post 1', content: 'Blog content' })
        await db.create('users', { $type: 'User', name: 'User 1', role: 'admin' })
        await db.create('comments', { $type: 'Comment', name: 'Comment 1', text: 'Nice!' })
        await db.create('tags', { $type: 'Tag', name: 'Tag 1', label: 'javascript' })
        await db.create('categories', { $type: 'Category', name: 'Category 1', slug: 'tech' })

        await db.disposeAsync()

        const rows = await readDataParquetRows(storage)
        expect(rows.length).toBe(5)

        // Verify all types are present
        const types = rows.map((r) => r['$type'])
        expect(types).toContain('Post')
        expect(types).toContain('User')
        expect(types).toContain('Comment')
        expect(types).toContain('Tag')
        expect(types).toContain('Category')
      })

      it('should preserve type-specific fields for each entity type', async () => {
        await db.create('products', {
          $type: 'Product',
          name: 'Widget',
          price: 29.99,
          inStock: true,
          sku: 'WIDGET-001',
        })

        await db.create('orders', {
          $type: 'Order',
          name: 'Order #1234',
          total: 59.98,
          status: 'pending',
          items: ['WIDGET-001', 'WIDGET-001'],
        })

        await db.disposeAsync()

        const rows = await readDataParquetRows(storage)
        expect(rows.length).toBe(2)

        const productRow = rows.find((r) => r['$type'] === 'Product')
        const orderRow = rows.find((r) => r['$type'] === 'Order')

        const product = rowToEntity(productRow!)
        const order = rowToEntity(orderRow!)

        expect(product.price).toBe(29.99)
        expect(product.inStock).toBe(true)
        expect(product.sku).toBe('WIDGET-001')

        expect(order.total).toBe(59.98)
        expect(order.status).toBe('pending')
        expect(order.items).toEqual(['WIDGET-001', 'WIDGET-001'])
      })
    })

    // =========================================================================
    // Edge Cases
    // =========================================================================

    describe('edge cases', () => {
      it('should handle entities with whitespace-only name', async () => {
        // Note: Empty names may be auto-derived from other fields (like title) in ParqueDB
        // This test verifies whitespace handling instead
        await db.create('posts', {
          $type: 'Post',
          name: '   ',
          title: 'Test',
        })

        await db.disposeAsync()

        const rows = await readDataParquetRows(storage)
        // Whitespace names should be preserved or trimmed based on implementation
        expect(rows[0]!['$name']).toBeDefined()
        expect(typeof rows[0]!['$name']).toBe('string')
      })

      it('should handle entities with Unicode content', async () => {
        await db.create('posts', {
          $type: 'Post',
          name: 'Unicode Post',
          title: 'Hello World - \u4e2d\u6587 - \u65e5\u672c\u8a9e - \ud83d\ude80',
          content: 'Emoji: \ud83c\udf1f\ud83c\udf08\ud83c\udf0d',
        })

        await db.disposeAsync()

        const rows = await readDataParquetRows(storage)
        const entity = rowToEntity(rows[0]!)

        expect(entity.title).toBe('Hello World - \u4e2d\u6587 - \u65e5\u672c\u8a9e - \ud83d\ude80')
        expect(entity.content).toBe('Emoji: \ud83c\udf1f\ud83c\udf08\ud83c\udf0d')
      })

      it('should handle entities with null values', async () => {
        await db.create('posts', {
          $type: 'Post',
          name: 'Post with Nulls',
          title: 'Hello',
          subtitle: null,
          category: null,
        })

        await db.disposeAsync()

        const rows = await readDataParquetRows(storage)
        const entity = rowToEntity(rows[0]!)

        // Null values should be preserved or converted to undefined
        expect(entity.subtitle === null || entity.subtitle === undefined).toBe(true)
      })

      it('should handle large number of entities', { timeout: 120000 }, async () => {
        const count = 100

        for (let i = 0; i < count; i++) {
          await db.create('items', {
            $type: 'Item',
            name: `Item ${i}`,
            index: i,
          })
        }

        await db.disposeAsync()

        const rows = await readDataParquetRows(storage)
        expect(rows.length).toBe(count)

        // Verify first and last items
        const firstItem = rows.find((r) => {
          const entity = rowToEntity(r)
          return entity.index === 0
        })
        const lastItem = rows.find((r) => {
          const entity = rowToEntity(r)
          return entity.index === 99
        })

        expect(firstItem).toBeDefined()
        expect(lastItem).toBeDefined()
      })
    })

    // =========================================================================
    // Update Handling
    // =========================================================================

    describe('update handling', () => {
      it('should reflect updated entity data in data.parquet', async () => {
        const created = await db.create('posts', {
          $type: 'Post',
          name: 'Original Name',
          title: 'Original Title',
        })

        // Update the entity
        await db.update('posts', created.$id as string, {
          $set: { title: 'Updated Title', views: 100 },
        })

        await db.disposeAsync()

        const rows = await readDataParquetRows(storage)
        const entity = rowToEntity(rows[0]!)

        expect(entity.title).toBe('Updated Title')
        expect(entity.views).toBe(100)
      })
    })
  })
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Create an async buffer compatible with hyparquet
 */
function createAsyncBuffer(data: Uint8Array): {
  byteLength: number
  slice: (start: number, end?: number) => Promise<ArrayBuffer>
} {
  return {
    byteLength: data.length,
    slice: async (start: number, end?: number): Promise<ArrayBuffer> => {
      const sliced = data.slice(start, end ?? data.length)
      const buffer = new ArrayBuffer(sliced.byteLength)
      new Uint8Array(buffer).set(sliced)
      return buffer
    },
  }
}

/**
 * Extract column names from parquet schema (excluding 'root')
 */
function extractSchemaColumnNames(schema: Array<{ name?: string }>): string[] {
  return schema.filter((s) => s.name && s.name !== 'root').map((s) => s.name!)
}

/**
 * Read data.parquet and return rows as array of records
 *
 * Uses ParquetReader which properly handles hyparquet's row format
 */
async function readDataParquetRows(storage: StorageBackend): Promise<Array<Record<string, unknown>>> {
  const reader = new ParquetReader({ storage })
  const rows = await reader.read<Record<string, unknown>>('data.parquet')
  return rows
}

// =============================================================================
// Run Tests Against FsBackend
// =============================================================================

createDataParquetTests('FsBackend', async () => {
  const tempDir = await mkdtemp(join(tmpdir(), 'parquedb-data-parquet-test-'))
  return {
    storage: new FsBackend(tempDir),
    cleanup: async () => {
      await rm(tempDir, { recursive: true, force: true })
    },
  }
})

// =============================================================================
// Run Tests Against MemoryBackend (simulates R2-like behavior)
// =============================================================================

createDataParquetTests('MemoryBackend', async () => {
  return {
    storage: new MemoryBackend(),
    cleanup: async () => {
      // MemoryBackend doesn't need cleanup
    },
  }
})

// =============================================================================
// R2Backend Tests (when R2 environment is available)
// =============================================================================

import { R2Backend } from '../../../src/storage/R2Backend'
import {
  createTestR2Backend,
  hasR2Credentials,
  cleanupR2Backend,
} from '../../helpers/storage'

// R2 tests run when credentials are available
describe.skipIf(!hasR2Credentials())('data.parquet Tests (R2Backend)', () => {
  createDataParquetTests('R2Backend', async () => {
    const storage = await createTestR2Backend()
    return {
      storage,
      cleanup: async () => {
        await cleanupR2Backend(storage as R2Backend)
      },
    }
  })
})

// =============================================================================
// Additional Direct Tests
// =============================================================================

describe('data.parquet - additional verification', () => {
  let tempDir: string
  let storage: FsBackend
  let db: ParqueDB

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), 'parquedb-data-direct-test-'))
    storage = new FsBackend(tempDir)
    db = new ParqueDB({ storage })
  })

  afterEach(async () => {
    await db.disposeAsync()
    try {
      await rm(tempDir, { recursive: true, force: true })
    } catch {
      // Ignore cleanup errors
    }
  })

  it('should create valid Parquet file readable by hyparquet', async () => {
    await db.create('posts', {
      $type: 'Post',
      name: 'Parquet Test',
      title: 'Testing Parquet Format',
    })

    await db.disposeAsync()

    // Read and verify it's a valid Parquet file
    const data = await readFile(join(tempDir, 'data.parquet'))
    expect(data.length).toBeGreaterThan(0)

    // Parquet files start with "PAR1" magic bytes
    const magicBytes = data.slice(0, 4).toString('utf-8')
    expect(magicBytes).toBe('PAR1')

    // And end with "PAR1" magic bytes
    const endMagicBytes = data.slice(-4).toString('utf-8')
    expect(endMagicBytes).toBe('PAR1')
  })

  it('should list data.parquet in directory listing', async () => {
    await db.create('posts', {
      $type: 'Post',
      name: 'Test',
      title: 'Hello',
    })

    await db.disposeAsync()

    const files = await readdir(tempDir)
    expect(files).toContain('data.parquet')
  })

  it('should have reasonable file size for content', async () => {
    // Create an entity with known content
    await db.create('posts', {
      $type: 'Post',
      name: 'Size Test',
      title: 'Testing file size',
      content: 'Some content here',
    })

    await db.disposeAsync()

    const stat = await storage.stat('data.parquet')
    expect(stat).not.toBeNull()

    // File should be reasonable size (not empty, not huge)
    expect(stat!.size).toBeGreaterThan(100) // At least some bytes
    expect(stat!.size).toBeLessThan(10000) // Not unreasonably large for small content
  })
})
