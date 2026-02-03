/**
 * Typed Parquet Writer Tests
 *
 * Tests for typed entity writing functionality including:
 * - Writing entities with typed schema
 * - $data column containing full JSON
 * - $data excluded when disabled
 * - All field types serialize correctly
 * - Null/undefined handling
 * - Audit and soft delete columns
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { ParquetWriter, writeTypedParquet } from '@/parquet/writer'
import { ParquetReader } from '@/parquet/reader'
import { MemoryBackend } from '@/storage/MemoryBackend'
import type { TypeDefinition } from '@/types/schema'
import type { Entity } from '@/types/entity'

// =============================================================================
// Test Fixtures
// =============================================================================

const PostSchema: TypeDefinition = {
  title: 'string!',
  content: 'text',
  views: 'int',
  rating: 'float',
  published: 'boolean',
  publishedAt: 'datetime',
  tags: 'string[]',
  metadata: 'json',
}

interface PostEntity extends Entity {
  title: string
  content?: string
  views?: number
  rating?: number
  published?: boolean
  publishedAt?: Date | number
  tags?: string[]
  metadata?: Record<string, unknown>
}

const createTestPost = (id: string, data: Partial<PostEntity> = {}): PostEntity => ({
  $id: `posts/${id}` as PostEntity['$id'],
  $type: 'Post',
  name: data.title ?? `Post ${id}`,
  title: data.title ?? `Post ${id}`,
  content: data.content ?? 'Test content',
  views: data.views ?? 0,
  rating: data.rating ?? 0.0,
  published: data.published ?? false,
  publishedAt: data.publishedAt ?? new Date('2024-01-15T10:00:00Z'),
  tags: data.tags ?? [],
  metadata: data.metadata ?? {},
  createdAt: new Date('2024-01-01T00:00:00Z'),
  createdBy: 'users/admin' as PostEntity['$id'],
  updatedAt: new Date('2024-01-01T00:00:00Z'),
  updatedBy: 'users/admin' as PostEntity['$id'],
  version: 1,
})

// =============================================================================
// ParquetWriter.writeTypedEntities Tests
// =============================================================================

describe('ParquetWriter.writeTypedEntities', () => {
  let storage: MemoryBackend
  let writer: ParquetWriter
  let reader: ParquetReader

  beforeEach(() => {
    storage = new MemoryBackend()
    writer = new ParquetWriter(storage)
    reader = new ParquetReader({ storage })
  })

  // ===========================================================================
  // Basic Write Tests
  // ===========================================================================

  describe('basic writes', () => {
    it('should write entities with typed schema', async () => {
      const posts = [
        createTestPost('1', { title: 'First Post', views: 100 }),
        createTestPost('2', { title: 'Second Post', views: 200 }),
        createTestPost('3', { title: 'Third Post', views: 300 }),
      ]

      const result = await writer.writeTypedEntities('data/posts.parquet', posts, {
        schema: PostSchema,
      })

      expect(result.rowCount).toBe(3)
      expect(await storage.exists('data/posts.parquet')).toBe(true)
    })

    it('should return correct WriteResult', async () => {
      const posts = [createTestPost('1')]

      const result = await writer.writeTypedEntities('data/posts.parquet', posts, {
        schema: PostSchema,
      })

      expect(result.rowCount).toBe(1)
      expect(result.rowGroupCount).toBeGreaterThan(0)
      expect(result.columns).toBeDefined()
      expect(Array.isArray(result.columns)).toBe(true)
      expect(result.size).toBeGreaterThan(0)
    })

    it('should handle empty entity array', async () => {
      const result = await writer.writeTypedEntities('data/empty.parquet', [], {
        schema: PostSchema,
      })

      expect(result.rowCount).toBe(0)
      expect(result.rowGroupCount).toBe(0)
      expect(await storage.exists('data/empty.parquet')).toBe(true)
    })

    it('should include all schema columns in result', async () => {
      const posts = [createTestPost('1')]

      const result = await writer.writeTypedEntities('data/posts.parquet', posts, {
        schema: PostSchema,
      })

      // System columns
      expect(result.columns).toContain('$id')
      expect(result.columns).toContain('$type')
      expect(result.columns).toContain('$data')

      // User-defined columns
      expect(result.columns).toContain('title')
      expect(result.columns).toContain('content')
      expect(result.columns).toContain('views')

      // Audit columns
      expect(result.columns).toContain('createdAt')
      expect(result.columns).toContain('updatedAt')
      expect(result.columns).toContain('version')
    })
  })

  // ===========================================================================
  // $data Variant Column Tests
  // ===========================================================================

  describe('$data variant column', () => {
    it('should include $data column by default', async () => {
      const posts = [createTestPost('1', { title: 'Test Post' })]

      const result = await writer.writeTypedEntities('data/posts.parquet', posts, {
        schema: PostSchema,
      })

      expect(result.columns).toContain('$data')
    })

    it('should write $data containing full entity as JSON', async () => {
      const posts = [
        createTestPost('1', { title: 'Test Post', views: 42, published: true }),
      ]

      await writer.writeTypedEntities('data/posts.parquet', posts, {
        schema: PostSchema,
      })

      const readData = await reader.read<Record<string, unknown>>('data/posts.parquet')

      expect(readData).toHaveLength(1)
      expect(readData[0].$data).toBeDefined()

      // Parse the JSON to verify contents
      const dataJson = JSON.parse(readData[0].$data as string)
      expect(dataJson.$id).toBe('posts/1')
      expect(dataJson.$type).toBe('Post')
      expect(dataJson.title).toBe('Test Post')
      expect(dataJson.views).toBe(42)
      expect(dataJson.published).toBe(true)
    })

    it('should exclude $data when includeDataVariant is false', async () => {
      const posts = [createTestPost('1')]

      const result = await writer.writeTypedEntities('data/posts.parquet', posts, {
        schema: PostSchema,
        includeDataVariant: false,
      })

      expect(result.columns).not.toContain('$data')
    })

    it('should still write data when $data is disabled', async () => {
      const posts = [
        createTestPost('1', { title: 'Test Post', views: 100 }),
      ]

      await writer.writeTypedEntities('data/posts.parquet', posts, {
        schema: PostSchema,
        includeDataVariant: false,
      })

      const readData = await reader.read<Record<string, unknown>>('data/posts.parquet')

      expect(readData).toHaveLength(1)
      expect(readData[0].$id).toBe('posts/1')
      expect(readData[0].title).toBe('Test Post')
      expect(readData[0].views).toBe(100)
      expect(readData[0].$data).toBeUndefined()
    })
  })

  // ===========================================================================
  // Field Type Serialization Tests
  // ===========================================================================

  describe('field type serialization', () => {
    it('should serialize string fields correctly', async () => {
      const posts = [
        createTestPost('1', { title: 'Hello World', content: 'Test content here' }),
      ]

      await writer.writeTypedEntities('data/posts.parquet', posts, {
        schema: PostSchema,
      })

      const readData = await reader.read<Record<string, unknown>>('data/posts.parquet')

      expect(readData[0].title).toBe('Hello World')
      expect(readData[0].content).toBe('Test content here')
    })

    it('should serialize integer fields correctly', async () => {
      const posts = [
        createTestPost('1', { views: 12345 }),
        createTestPost('2', { views: 0 }),
        createTestPost('3', { views: -100 }),
      ]

      await writer.writeTypedEntities('data/posts.parquet', posts, {
        schema: PostSchema,
      })

      const readData = await reader.read<Record<string, unknown>>('data/posts.parquet')

      expect(readData[0].views).toBe(12345)
      expect(readData[1].views).toBe(0)
      expect(readData[2].views).toBe(-100)
    })

    it('should serialize float fields correctly', async () => {
      const posts = [
        createTestPost('1', { rating: 4.5 }),
        createTestPost('2', { rating: 0.0 }),
        createTestPost('3', { rating: -1.25 }),
      ]

      await writer.writeTypedEntities('data/posts.parquet', posts, {
        schema: PostSchema,
      })

      const readData = await reader.read<Record<string, unknown>>('data/posts.parquet')

      expect(readData[0].rating).toBeCloseTo(4.5)
      expect(readData[1].rating).toBeCloseTo(0.0)
      expect(readData[2].rating).toBeCloseTo(-1.25)
    })

    it('should serialize boolean fields correctly', async () => {
      const posts = [
        createTestPost('1', { published: true }),
        createTestPost('2', { published: false }),
      ]

      await writer.writeTypedEntities('data/posts.parquet', posts, {
        schema: PostSchema,
      })

      const readData = await reader.read<Record<string, unknown>>('data/posts.parquet')

      expect(readData[0].published).toBe(true)
      expect(readData[1].published).toBe(false)
    })

    it('should serialize datetime fields correctly', async () => {
      const date = new Date('2024-06-15T14:30:00Z')
      const posts = [createTestPost('1', { publishedAt: date })]

      await writer.writeTypedEntities('data/posts.parquet', posts, {
        schema: PostSchema,
      })

      const readData = await reader.read<Record<string, unknown>>('data/posts.parquet')

      // DateTime may be returned as timestamp (ms), Date object, or ISO string
      const publishedAt = readData[0].publishedAt
      if (typeof publishedAt === 'number') {
        expect(publishedAt).toBe(date.getTime())
      } else if (publishedAt instanceof Date) {
        expect(publishedAt.getTime()).toBe(date.getTime())
      } else if (typeof publishedAt === 'string') {
        expect(new Date(publishedAt).getTime()).toBe(date.getTime())
      } else {
        // Fail with meaningful message if unexpected type
        expect(publishedAt).toBeDefined()
      }
    })

    it('should serialize array fields as JSON', async () => {
      const posts = [
        createTestPost('1', { tags: ['javascript', 'nodejs', 'parquet'] }),
      ]

      await writer.writeTypedEntities('data/posts.parquet', posts, {
        schema: PostSchema,
      })

      const readData = await reader.read<Record<string, unknown>>('data/posts.parquet')

      // Arrays are stored as JSON strings
      const tags = typeof readData[0].tags === 'string'
        ? JSON.parse(readData[0].tags)
        : readData[0].tags

      expect(tags).toEqual(['javascript', 'nodejs', 'parquet'])
    })

    it('should serialize json fields correctly', async () => {
      const posts = [
        createTestPost('1', { metadata: { key: 'value', nested: { a: 1 } } }),
      ]

      await writer.writeTypedEntities('data/posts.parquet', posts, {
        schema: PostSchema,
      })

      const readData = await reader.read<Record<string, unknown>>('data/posts.parquet')

      const metadata = typeof readData[0].metadata === 'string'
        ? JSON.parse(readData[0].metadata)
        : readData[0].metadata

      expect(metadata).toEqual({ key: 'value', nested: { a: 1 } })
    })
  })

  // ===========================================================================
  // Null/Undefined Handling Tests
  // ===========================================================================

  describe('null and undefined handling', () => {
    it('should handle null values in optional fields', async () => {
      const post: PostEntity = {
        ...createTestPost('1'),
        content: undefined,
        views: undefined,
        rating: undefined,
      }

      await writer.writeTypedEntities('data/posts.parquet', [post], {
        schema: PostSchema,
      })

      const readData = await reader.read<Record<string, unknown>>('data/posts.parquet')

      expect(readData[0].content).toBeNull()
      expect(readData[0].views).toBeNull()
      expect(readData[0].rating).toBeNull()
    })

    it('should handle entities with some missing fields', async () => {
      const posts = [
        createTestPost('1', { title: 'Full Post', content: 'Content', views: 100 }),
        { ...createTestPost('2'), content: undefined, views: undefined },
      ]

      await writer.writeTypedEntities('data/posts.parquet', posts, {
        schema: PostSchema,
      })

      const readData = await reader.read<Record<string, unknown>>('data/posts.parquet')

      expect(readData[0].content).toBe('Content')
      expect(readData[0].views).toBe(100)
      expect(readData[1].content).toBeNull()
      expect(readData[1].views).toBeNull()
    })

    it('should handle empty string vs null', async () => {
      const posts = [
        createTestPost('1', { content: '' }),
        { ...createTestPost('2'), content: undefined },
      ]

      await writer.writeTypedEntities('data/posts.parquet', posts, {
        schema: PostSchema,
      })

      const readData = await reader.read<Record<string, unknown>>('data/posts.parquet')

      expect(readData[0].content).toBe('')
      expect(readData[1].content).toBeNull()
    })

    it('should handle empty arrays', async () => {
      const posts = [createTestPost('1', { tags: [] })]

      await writer.writeTypedEntities('data/posts.parquet', posts, {
        schema: PostSchema,
      })

      const readData = await reader.read<Record<string, unknown>>('data/posts.parquet')

      const tags = typeof readData[0].tags === 'string'
        ? JSON.parse(readData[0].tags)
        : readData[0].tags

      expect(tags).toEqual([])
    })
  })

  // ===========================================================================
  // Audit Column Tests
  // ===========================================================================

  describe('audit columns', () => {
    it('should include audit columns by default', async () => {
      const posts = [createTestPost('1')]

      const result = await writer.writeTypedEntities('data/posts.parquet', posts, {
        schema: PostSchema,
      })

      expect(result.columns).toContain('createdAt')
      expect(result.columns).toContain('createdBy')
      expect(result.columns).toContain('updatedAt')
      expect(result.columns).toContain('updatedBy')
      expect(result.columns).toContain('version')
    })

    it('should exclude audit columns when disabled', async () => {
      const posts = [createTestPost('1')]

      const result = await writer.writeTypedEntities('data/posts.parquet', posts, {
        schema: PostSchema,
        includeAuditColumns: false,
      })

      expect(result.columns).not.toContain('createdAt')
      expect(result.columns).not.toContain('createdBy')
      expect(result.columns).not.toContain('updatedAt')
      expect(result.columns).not.toContain('updatedBy')
      expect(result.columns).not.toContain('version')
    })

    it('should write correct audit field values', async () => {
      const post = createTestPost('1')
      post.createdBy = 'users/creator' as PostEntity['$id']
      post.updatedBy = 'users/updater' as PostEntity['$id']
      post.version = 5

      await writer.writeTypedEntities('data/posts.parquet', [post], {
        schema: PostSchema,
      })

      const readData = await reader.read<Record<string, unknown>>('data/posts.parquet')

      expect(readData[0].createdBy).toBe('users/creator')
      expect(readData[0].updatedBy).toBe('users/updater')
      expect(readData[0].version).toBe(5)
    })
  })

  // ===========================================================================
  // Soft Delete Column Tests
  // ===========================================================================

  describe('soft delete columns', () => {
    it('should include soft delete columns by default', async () => {
      const posts = [createTestPost('1')]

      const result = await writer.writeTypedEntities('data/posts.parquet', posts, {
        schema: PostSchema,
      })

      expect(result.columns).toContain('deletedAt')
      expect(result.columns).toContain('deletedBy')
    })

    it('should exclude soft delete columns when disabled', async () => {
      const posts = [createTestPost('1')]

      const result = await writer.writeTypedEntities('data/posts.parquet', posts, {
        schema: PostSchema,
        includeSoftDeleteColumns: false,
      })

      expect(result.columns).not.toContain('deletedAt')
      expect(result.columns).not.toContain('deletedBy')
    })
  })

  // ===========================================================================
  // Writer Options Tests
  // ===========================================================================

  describe('writer options', () => {
    it('should respect compression option', async () => {
      const posts = [createTestPost('1')]

      const result = await writer.writeTypedEntities('data/posts.parquet', posts, {
        schema: PostSchema,
        compression: 'snappy',
      })

      expect(result.size).toBeGreaterThan(0)
    })

    it('should respect rowGroupSize option', async () => {
      const posts = Array.from({ length: 100 }, (_, i) =>
        createTestPost(`${i + 1}`, { title: `Post ${i + 1}` })
      )

      const result = await writer.writeTypedEntities('data/posts.parquet', posts, {
        schema: PostSchema,
        rowGroupSize: 20,
      })

      expect(result.rowGroupCount).toBe(5)
    })

    it('should include custom metadata', async () => {
      const posts = [createTestPost('1')]

      await writer.writeTypedEntities('data/posts.parquet', posts, {
        schema: PostSchema,
        metadata: { author: 'test-user', version: '1.0' },
      })

      const metadata = await reader.readMetadata('data/posts.parquet')

      if (metadata.keyValueMetadata) {
        const authorMeta = metadata.keyValueMetadata.find((kv) => kv.key === 'author')
        expect(authorMeta?.value).toBe('test-user')
      }
    })
  })
})

// =============================================================================
// writeTypedParquet Standalone Function Tests
// =============================================================================

describe('writeTypedParquet', () => {
  let storage: MemoryBackend
  let reader: ParquetReader

  beforeEach(() => {
    storage = new MemoryBackend()
    reader = new ParquetReader({ storage })
  })

  it('should write typed entities using convenience function', async () => {
    const posts = [
      createTestPost('1', { title: 'First Post' }),
      createTestPost('2', { title: 'Second Post' }),
    ]

    const result = await writeTypedParquet(storage, 'data/posts.parquet', posts, {
      schema: PostSchema,
    })

    expect(result.rowCount).toBe(2)
    expect(await storage.exists('data/posts.parquet')).toBe(true)
  })

  it('should read back written data correctly', async () => {
    const posts = [
      createTestPost('1', { title: 'Test Post', views: 42, published: true }),
    ]

    await writeTypedParquet(storage, 'data/posts.parquet', posts, {
      schema: PostSchema,
    })

    const readData = await reader.read<Record<string, unknown>>('data/posts.parquet')

    expect(readData).toHaveLength(1)
    expect(readData[0].$id).toBe('posts/1')
    expect(readData[0].title).toBe('Test Post')
    expect(readData[0].views).toBe(42)
    expect(readData[0].published).toBe(true)
  })

  it('should accept all typed write options', async () => {
    const posts = [createTestPost('1')]

    const result = await writeTypedParquet(storage, 'data/posts.parquet', posts, {
      schema: PostSchema,
      includeDataVariant: true,
      includeAuditColumns: true,
      includeSoftDeleteColumns: true,
      compression: 'lz4',
      rowGroupSize: 50,
    })

    expect(result.rowCount).toBe(1)
    expect(result.columns).toContain('$data')
    expect(result.columns).toContain('createdAt')
    expect(result.columns).toContain('deletedAt')
  })
})

// =============================================================================
// writeTypedEntitiesBuffer Tests
// =============================================================================

describe('ParquetWriter.writeTypedEntitiesBuffer', () => {
  let storage: MemoryBackend
  let writer: ParquetWriter

  beforeEach(() => {
    storage = new MemoryBackend()
    writer = new ParquetWriter(storage)
  })

  it('should return Uint8Array buffer', async () => {
    const posts = [createTestPost('1')]

    const buffer = await writer.writeTypedEntitiesBuffer(posts, {
      schema: PostSchema,
    })

    expect(buffer).toBeInstanceOf(Uint8Array)
    expect(buffer.length).toBeGreaterThan(0)
  })

  it('should produce valid Parquet data', async () => {
    const posts = [
      createTestPost('1', { title: 'Buffer Test', views: 99 }),
    ]

    const buffer = await writer.writeTypedEntitiesBuffer(posts, {
      schema: PostSchema,
    })

    // Write buffer to storage and read it back
    await storage.writeAtomic('data/buffer-test.parquet', buffer)

    const reader = new ParquetReader({ storage })
    const readData = await reader.read<Record<string, unknown>>('data/buffer-test.parquet')

    expect(readData).toHaveLength(1)
    expect(readData[0].title).toBe('Buffer Test')
    expect(readData[0].views).toBe(99)
  })

  it('should handle empty entities', async () => {
    const buffer = await writer.writeTypedEntitiesBuffer([], {
      schema: PostSchema,
    })

    expect(buffer).toBeInstanceOf(Uint8Array)
    expect(buffer.length).toBeGreaterThan(0)
  })
})

// =============================================================================
// Complex Schema Tests
// =============================================================================

describe('complex typed schemas', () => {
  let storage: MemoryBackend
  let writer: ParquetWriter
  let reader: ParquetReader

  beforeEach(() => {
    storage = new MemoryBackend()
    writer = new ParquetWriter(storage)
    reader = new ParquetReader({ storage })
  })

  it('should handle schema with all supported field types', async () => {
    const ComplexSchema: TypeDefinition = {
      name: 'string!',
      description: 'text',
      count: 'int',
      price: 'float',
      rating: 'double',
      active: 'boolean',
      birthDate: 'date',
      createdDate: 'datetime',
      uuid: 'uuid',
      email: 'email',
      website: 'url',
      data: 'json',
      tags: 'string[]',
    }

    interface ComplexEntity extends Entity {
      description?: string
      count?: number
      price?: number
      rating?: number
      active?: boolean
      birthDate?: string
      createdDate?: Date | number
      uuid?: string
      email?: string
      website?: string
      data?: Record<string, unknown>
      tags?: string[]
    }

    const entity: ComplexEntity = {
      $id: 'items/1' as ComplexEntity['$id'],
      $type: 'ComplexItem',
      name: 'Test Item',
      description: 'A test item',
      count: 42,
      price: 19.99,
      rating: 4.5,
      active: true,
      birthDate: '2024-01-15',
      createdDate: new Date('2024-01-15T10:00:00Z'),
      uuid: '550e8400-e29b-41d4-a716-446655440000',
      email: 'test@example.com',
      website: 'https://example.com',
      data: { nested: { key: 'value' } },
      tags: ['tag1', 'tag2'],
      createdAt: new Date(),
      createdBy: 'users/admin' as ComplexEntity['$id'],
      updatedAt: new Date(),
      updatedBy: 'users/admin' as ComplexEntity['$id'],
      version: 1,
    }

    const result = await writer.writeTypedEntities('data/complex.parquet', [entity], {
      schema: ComplexSchema,
    })

    expect(result.rowCount).toBe(1)

    const readData = await reader.read<Record<string, unknown>>('data/complex.parquet')

    expect(readData[0].name).toBe('Test Item')
    expect(readData[0].description).toBe('A test item')
    expect(readData[0].count).toBe(42)
    expect(readData[0].active).toBe(true)
    expect(readData[0].uuid).toBe('550e8400-e29b-41d4-a716-446655440000')
    expect(readData[0].email).toBe('test@example.com')
  })

  it('should handle schema with object field definitions', async () => {
    const SchemaWithObjects: TypeDefinition = {
      status: { type: 'string', required: true },
      priority: { type: 'int', required: false },
      percentage: { type: 'float', required: false },
    }

    interface StatusEntity extends Entity {
      status: string
      priority?: number
      percentage?: number
    }

    const entity: StatusEntity = {
      $id: 'status/1' as StatusEntity['$id'],
      $type: 'Status',
      name: 'Test Status',
      status: 'active',
      priority: 5,
      percentage: 75.5,
      createdAt: new Date(),
      createdBy: 'users/admin' as StatusEntity['$id'],
      updatedAt: new Date(),
      updatedBy: 'users/admin' as StatusEntity['$id'],
      version: 1,
    }

    const result = await writer.writeTypedEntities('data/status.parquet', [entity], {
      schema: SchemaWithObjects,
    })

    expect(result.columns).toContain('status')
    expect(result.columns).toContain('priority')
    expect(result.columns).toContain('percentage')

    const readData = await reader.read<Record<string, unknown>>('data/status.parquet')

    expect(readData[0].status).toBe('active')
    expect(readData[0].priority).toBe(5)
    expect(readData[0].percentage).toBeCloseTo(75.5)
  })

  it('should skip relationship definitions in schema', async () => {
    const SchemaWithRels: TypeDefinition = {
      title: 'string!',
      author: '-> User.posts',
      comments: '<- Comment.post[]',
    }

    const entity: Entity = {
      $id: 'posts/1' as Entity['$id'],
      $type: 'Post',
      name: 'Test Post',
      title: 'My Post',
      createdAt: new Date(),
      createdBy: 'users/admin' as Entity['$id'],
      updatedAt: new Date(),
      updatedBy: 'users/admin' as Entity['$id'],
      version: 1,
    }

    const result = await writer.writeTypedEntities('data/with-rels.parquet', [entity], {
      schema: SchemaWithRels,
    })

    expect(result.columns).toContain('title')
    expect(result.columns).not.toContain('author')
    expect(result.columns).not.toContain('comments')
  })
})
