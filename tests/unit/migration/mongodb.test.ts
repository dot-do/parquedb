/**
 * Tests for MongoDB BSON import utilities
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import * as fs from 'fs/promises'
import * as path from 'path'
import * as os from 'os'
import { ParqueDB } from '../../../src/ParqueDB'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import { importFromMongodb } from '../../../src/migration/mongodb'
import { convertBsonValue } from '../../../src/migration/utils'

describe('importFromMongodb', () => {
  let db: ParqueDB
  let tempDir: string

  beforeEach(async () => {
    db = new ParqueDB({ storage: new MemoryBackend() })
    tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'parquedb-mongo-test-'))
  })

  afterEach(async () => {
    db.dispose()
    await fs.rm(tempDir, { recursive: true, force: true })
  })

  describe('JSON array format', () => {
    it('imports documents from mongoexport --jsonArray output', async () => {
      const jsonPath = path.join(tempDir, 'users.json')
      const data = [
        {
          _id: { $oid: '507f1f77bcf86cd799439011' },
          name: 'Alice',
          email: 'alice@example.com',
        },
        {
          _id: { $oid: '507f1f77bcf86cd799439012' },
          name: 'Bob',
          email: 'bob@example.com',
        },
      ]
      await fs.writeFile(jsonPath, JSON.stringify(data))

      const result = await importFromMongodb(db, 'users', jsonPath)

      expect(result.imported).toBe(2)

      const users = await db.collection('users').find()
      expect(users.items).toHaveLength(2)
      expect(users.items[0]?.name).toBe('Alice')
    })

    it('converts MongoDB ObjectIds to strings', async () => {
      const jsonPath = path.join(tempDir, 'docs.json')
      const data = [
        {
          _id: { $oid: '507f1f77bcf86cd799439011' },
          name: 'Document 1',
          authorId: { $oid: '507f1f77bcf86cd799439100' },
        },
      ]
      await fs.writeFile(jsonPath, JSON.stringify(data))

      await importFromMongodb(db, 'docs', jsonPath)

      const docs = await db.collection('docs').find()
      expect(typeof docs.items[0]?.authorId).toBe('string')
      expect(docs.items[0]?.authorId).toBe('507f1f77bcf86cd799439100')
    })

    it('converts MongoDB dates', async () => {
      const jsonPath = path.join(tempDir, 'events.json')
      const data = [
        {
          _id: { $oid: '507f1f77bcf86cd799439011' },
          name: 'Event 1',
          createdAt: { $date: '2024-01-15T10:30:00Z' },
        },
        {
          _id: { $oid: '507f1f77bcf86cd799439012' },
          name: 'Event 2',
          createdAt: { $date: { $numberLong: '1705315800000' } },
        },
      ]
      await fs.writeFile(jsonPath, JSON.stringify(data))

      await importFromMongodb(db, 'events', jsonPath)

      const events = await db.collection('events').find()
      expect(events.items[0]?.createdAt).toBeInstanceOf(Date)
    })

    it('stores MongoDB _id as mongoId when preserveMongoId is false', async () => {
      const jsonPath = path.join(tempDir, 'docs.json')
      const data = [
        {
          _id: { $oid: '507f1f77bcf86cd799439011' },
          name: 'Document 1',
        },
      ]
      await fs.writeFile(jsonPath, JSON.stringify(data))

      await importFromMongodb(db, 'docs', jsonPath, { preserveMongoId: false })

      const docs = await db.collection('docs').find()
      expect(docs.items[0]?.mongoId).toBe('507f1f77bcf86cd799439011')
      expect(docs.items[0]?._id).toBeUndefined()
    })

    it('preserves MongoDB _id when preserveMongoId is true', async () => {
      const jsonPath = path.join(tempDir, 'docs.json')
      const data = [
        {
          _id: { $oid: '507f1f77bcf86cd799439011' },
          name: 'Document 1',
        },
      ]
      await fs.writeFile(jsonPath, JSON.stringify(data))

      await importFromMongodb(db, 'docs', jsonPath, { preserveMongoId: true })

      const docs = await db.collection('docs').find()
      expect(docs.items[0]?._id).toBe('507f1f77bcf86cd799439011')
    })
  })

  describe('JSONL format', () => {
    it('imports documents from mongoexport JSONL output', async () => {
      const jsonlPath = path.join(tempDir, 'users.json')
      const lines = [
        JSON.stringify({ _id: { $oid: '507f1f77bcf86cd799439011' }, name: 'Alice' }),
        JSON.stringify({ _id: { $oid: '507f1f77bcf86cd799439012' }, name: 'Bob' }),
        JSON.stringify({ _id: { $oid: '507f1f77bcf86cd799439013' }, name: 'Charlie' }),
      ]
      await fs.writeFile(jsonlPath, lines.join('\n'))

      const result = await importFromMongodb(db, 'users', jsonlPath)

      expect(result.imported).toBe(3)
    })

    it('handles invalid JSON lines', async () => {
      const jsonlPath = path.join(tempDir, 'mixed.json')
      const lines = [
        JSON.stringify({ _id: { $oid: '507f1f77bcf86cd799439011' }, name: 'Good' }),
        '{ invalid json }',
        JSON.stringify({ _id: { $oid: '507f1f77bcf86cd799439012' }, name: 'Also Good' }),
      ]
      await fs.writeFile(jsonlPath, lines.join('\n'))

      const result = await importFromMongodb(db, 'users', jsonlPath)

      expect(result.imported).toBe(2)
      expect(result.failed).toBe(1)
      expect(result.errors).toHaveLength(1)
    })
  })

  describe('BSON extended JSON types', () => {
    it('converts $numberLong', async () => {
      const jsonPath = path.join(tempDir, 'numbers.json')
      const data = [
        {
          _id: { $oid: '507f1f77bcf86cd799439011' },
          name: 'Big Number',
          bigValue: { $numberLong: '9223372036854775807' },
        },
      ]
      await fs.writeFile(jsonPath, JSON.stringify(data))

      await importFromMongodb(db, 'numbers', jsonPath)

      const docs = await db.collection('numbers').find()
      expect(typeof docs.items[0]?.bigValue).toBe('number')
    })

    it('converts $numberDecimal', async () => {
      const jsonPath = path.join(tempDir, 'decimals.json')
      const data = [
        {
          _id: { $oid: '507f1f77bcf86cd799439011' },
          name: 'Decimal Value',
          price: { $numberDecimal: '19.99' },
        },
      ]
      await fs.writeFile(jsonPath, JSON.stringify(data))

      await importFromMongodb(db, 'items', jsonPath)

      const items = await db.collection('items').find()
      expect(items.items[0]?.price).toBe(19.99)
    })

    it('converts nested documents with BSON types', async () => {
      const jsonPath = path.join(tempDir, 'nested.json')
      const data = [
        {
          _id: { $oid: '507f1f77bcf86cd799439011' },
          name: 'Nested Doc',
          meta: {
            authorId: { $oid: '507f1f77bcf86cd799439100' },
            createdAt: { $date: '2024-01-15T10:30:00Z' },
            stats: {
              views: { $numberLong: '1000000' },
            },
          },
        },
      ]
      await fs.writeFile(jsonPath, JSON.stringify(data))

      await importFromMongodb(db, 'docs', jsonPath)

      const docs = await db.collection('docs').find()
      const meta = docs.items[0]?.meta as Record<string, unknown>
      expect(meta.authorId).toBe('507f1f77bcf86cd799439100')
      expect(meta.createdAt).toBeInstanceOf(Date)
      expect((meta.stats as Record<string, unknown>).views).toBe(1000000)
    })

    it('converts arrays with BSON types', async () => {
      const jsonPath = path.join(tempDir, 'arrays.json')
      const data = [
        {
          _id: { $oid: '507f1f77bcf86cd799439011' },
          name: 'With Array',
          comments: [
            {
              authorId: { $oid: '507f1f77bcf86cd799439100' },
              text: 'First comment',
            },
            {
              authorId: { $oid: '507f1f77bcf86cd799439101' },
              text: 'Second comment',
            },
          ],
        },
      ]
      await fs.writeFile(jsonPath, JSON.stringify(data))

      await importFromMongodb(db, 'posts', jsonPath)

      const posts = await db.collection('posts').find()
      const comments = posts.items[0]?.comments as Array<Record<string, unknown>>
      expect(comments[0].authorId).toBe('507f1f77bcf86cd799439100')
    })
  })

  describe('name field inference', () => {
    it('uses nameField option', async () => {
      const jsonPath = path.join(tempDir, 'products.json')
      const data = [
        {
          _id: { $oid: '507f1f77bcf86cd799439011' },
          title: 'Product Title',
          sku: 'PRD-001',
        },
      ]
      await fs.writeFile(jsonPath, JSON.stringify(data))

      await importFromMongodb(db, 'products', jsonPath, { nameField: 'title' })

      const products = await db.collection('products').find()
      expect(products.items[0]?.name).toBe('Product Title')
    })

    it('auto-detects name from common fields', async () => {
      const jsonPath = path.join(tempDir, 'docs.json')
      const data = [
        {
          _id: { $oid: '507f1f77bcf86cd799439011' },
          title: 'Doc Title',
        },
      ]
      await fs.writeFile(jsonPath, JSON.stringify(data))

      await importFromMongodb(db, 'docs', jsonPath)

      const docs = await db.collection('docs').find()
      expect(docs.items[0]?.name).toBe('Doc Title')
    })

    it('uses _id as name when no name fields found', async () => {
      const jsonPath = path.join(tempDir, 'items.json')
      const data = [
        {
          _id: { $oid: '507f1f77bcf86cd799439011' },
          value: 100,
        },
      ]
      await fs.writeFile(jsonPath, JSON.stringify(data))

      await importFromMongodb(db, 'items', jsonPath)

      const items = await db.collection('items').find()
      expect(items.items[0]?.name).toBe('507f1f77bcf86cd799439011')
    })
  })

  describe('transform function', () => {
    it('applies transform to documents', async () => {
      const jsonPath = path.join(tempDir, 'users.json')
      const data = [
        {
          _id: { $oid: '507f1f77bcf86cd799439011' },
          name: 'Alice',
          status: 'active',
        },
      ]
      await fs.writeFile(jsonPath, JSON.stringify(data))

      await importFromMongodb(db, 'users', jsonPath, {
        transform: (doc) => {
          const user = doc as { status: string }
          return {
            ...user,
            isActive: user.status === 'active',
          }
        },
      })

      const users = await db.collection('users').find()
      expect(users.items[0]?.isActive).toBe(true)
    })

    it('skips documents filtered by transform', async () => {
      const jsonPath = path.join(tempDir, 'users.json')
      const data = [
        { _id: { $oid: '507f1f77bcf86cd799439011' }, name: 'Keep', status: 'active' },
        { _id: { $oid: '507f1f77bcf86cd799439012' }, name: 'Skip', status: 'deleted' },
      ]
      await fs.writeFile(jsonPath, JSON.stringify(data))

      const result = await importFromMongodb(db, 'users', jsonPath, {
        transform: (doc) => {
          const user = doc as { status: string }
          return user.status === 'deleted' ? null : doc
        },
      })

      expect(result.imported).toBe(1)
      expect(result.skipped).toBe(1)
    })
  })

  describe('entity type', () => {
    it('uses entityType option', async () => {
      const jsonPath = path.join(tempDir, 'items.json')
      const data = [
        { _id: { $oid: '507f1f77bcf86cd799439011' }, name: 'Item 1' },
      ]
      await fs.writeFile(jsonPath, JSON.stringify(data))

      await importFromMongodb(db, 'items', jsonPath, { entityType: 'Product' })

      const items = await db.collection('items').find()
      expect(items.items[0]?.$type).toBe('Product')
    })

    it('infers entity type from namespace', async () => {
      const jsonPath = path.join(tempDir, 'blog-posts.json')
      const data = [
        { _id: { $oid: '507f1f77bcf86cd799439011' }, name: 'Post 1' },
      ]
      await fs.writeFile(jsonPath, JSON.stringify(data))

      await importFromMongodb(db, 'blog-posts', jsonPath)

      const posts = await db.collection('blog-posts').find()
      expect(posts.items[0]?.$type).toBe('BlogPost')
    })
  })

  describe('error handling', () => {
    it('throws error for non-existent file', async () => {
      await expect(
        importFromMongodb(db, 'items', '/nonexistent/file.json')
      ).rejects.toThrow('File not found')
    })

    it('throws error for invalid JSON array', async () => {
      const jsonPath = path.join(tempDir, 'invalid.json')
      await fs.writeFile(jsonPath, '[{ invalid }]')

      await expect(
        importFromMongodb(db, 'items', jsonPath)
      ).rejects.toThrow('Invalid JSON')
    })
  })

  describe('progress reporting', () => {
    it('reports progress during import', async () => {
      const jsonPath = path.join(tempDir, 'large.json')
      const data = Array.from({ length: 50 }, (_, i) => ({
        _id: { $oid: `507f1f77bcf86cd79943901${i.toString().padStart(2, '0')}` },
        name: `Item ${i}`,
      }))
      await fs.writeFile(jsonPath, JSON.stringify(data))

      const progressCalls: number[] = []
      await importFromMongodb(db, 'items', jsonPath, {
        batchSize: 10,
        onProgress: (count) => progressCalls.push(count),
      })

      expect(progressCalls.length).toBeGreaterThan(0)
    })
  })
})

describe('convertBsonValue', () => {
  it('converts ObjectId', () => {
    const result = convertBsonValue({ $oid: '507f1f77bcf86cd799439011' })
    expect(result).toBe('507f1f77bcf86cd799439011')
  })

  it('converts date string', () => {
    const result = convertBsonValue({ $date: '2024-01-15T10:30:00Z' })
    expect(result).toBeInstanceOf(Date)
  })

  it('converts date number', () => {
    const result = convertBsonValue({ $date: 1705315800000 })
    expect(result).toBeInstanceOf(Date)
  })

  it('converts date numberLong', () => {
    const result = convertBsonValue({ $date: { $numberLong: '1705315800000' } })
    expect(result).toBeInstanceOf(Date)
  })

  it('converts numberLong', () => {
    const result = convertBsonValue({ $numberLong: '9223372036854775807' })
    expect(typeof result).toBe('number')
  })

  it('converts numberDecimal', () => {
    const result = convertBsonValue({ $numberDecimal: '19.99' })
    expect(result).toBe(19.99)
  })

  it('converts numberInt', () => {
    const result = convertBsonValue({ $numberInt: '42' })
    expect(result).toBe(42)
  })

  it('converts numberDouble', () => {
    const result = convertBsonValue({ $numberDouble: '3.14159' })
    expect(result).toBeCloseTo(3.14159)
  })

  it('converts UUID', () => {
    const result = convertBsonValue({ $uuid: '550e8400-e29b-41d4-a716-446655440000' })
    expect(result).toBe('550e8400-e29b-41d4-a716-446655440000')
  })

  it('preserves regular values', () => {
    expect(convertBsonValue('string')).toBe('string')
    expect(convertBsonValue(42)).toBe(42)
    expect(convertBsonValue(true)).toBe(true)
    expect(convertBsonValue(null)).toBe(null)
  })

  it('recursively converts arrays', () => {
    const result = convertBsonValue([
      { $oid: '507f1f77bcf86cd799439011' },
      { $date: '2024-01-15T10:30:00Z' },
    ])
    expect(Array.isArray(result)).toBe(true)
    expect((result as unknown[])[0]).toBe('507f1f77bcf86cd799439011')
    expect((result as unknown[])[1]).toBeInstanceOf(Date)
  })

  it('recursively converts nested objects', () => {
    const result = convertBsonValue({
      user: {
        id: { $oid: '507f1f77bcf86cd799439011' },
        createdAt: { $date: '2024-01-15T10:30:00Z' },
      },
    })
    const obj = result as Record<string, Record<string, unknown>>
    expect(obj.user.id).toBe('507f1f77bcf86cd799439011')
    expect(obj.user.createdAt).toBeInstanceOf(Date)
  })
})
