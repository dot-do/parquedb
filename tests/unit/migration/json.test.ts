/**
 * Tests for JSON/JSONL import utilities
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import * as fs from 'fs/promises'
import * as path from 'path'
import * as os from 'os'
import { ParqueDB } from '../../../src/ParqueDB'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import { importFromJson, importFromJsonl } from '../../../src/migration/json'

describe('importFromJson', () => {
  let db: ParqueDB
  let tempDir: string

  beforeEach(async () => {
    db = new ParqueDB({ storage: new MemoryBackend() })
    tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'parquedb-json-test-'))
  })

  afterEach(async () => {
    db.dispose()
    await fs.rm(tempDir, { recursive: true, force: true })
  })

  describe('JSON array files', () => {
    it('imports documents from JSON array', async () => {
      const jsonPath = path.join(tempDir, 'users.json')
      const data = [
        { name: 'Alice', email: 'alice@example.com', age: 30 },
        { name: 'Bob', email: 'bob@example.com', age: 25 },
        { name: 'Charlie', email: 'charlie@example.com', age: 35 },
      ]
      await fs.writeFile(jsonPath, JSON.stringify(data))

      const result = await importFromJson(db, 'users', jsonPath)

      expect(result.imported).toBe(3)
      expect(result.skipped).toBe(0)
      expect(result.failed).toBe(0)
      expect(result.errors).toHaveLength(0)

      const users = await db.collection('users').find()
      expect(users.items).toHaveLength(3)
      expect(users.items.map(u => u.name).sort()).toEqual(['Alice', 'Bob', 'Charlie'])
    })

    it('infers entity type from namespace', async () => {
      const jsonPath = path.join(tempDir, 'blog-posts.json')
      const data = [{ name: 'First Post', content: 'Hello World' }]
      await fs.writeFile(jsonPath, JSON.stringify(data))

      await importFromJson(db, 'blog-posts', jsonPath)

      const posts = await db.collection('blog-posts').find()
      expect(posts.items[0]?.$type).toBe('BlogPost')
    })

    it('uses custom entity type when provided', async () => {
      const jsonPath = path.join(tempDir, 'items.json')
      const data = [{ name: 'Item 1' }]
      await fs.writeFile(jsonPath, JSON.stringify(data))

      await importFromJson(db, 'items', jsonPath, { entityType: 'Product' })

      const items = await db.collection('items').find()
      expect(items.items[0]?.$type).toBe('Product')
    })

    it('applies transform function', async () => {
      const jsonPath = path.join(tempDir, 'products.json')
      const data = [
        { title: 'Product 1', price: 10 },
        { title: 'Product 2', price: 20 },
      ]
      await fs.writeFile(jsonPath, JSON.stringify(data))

      const result = await importFromJson(db, 'products', jsonPath, {
        transform: (doc) => {
          const product = doc as { title: string; price: number }
          return {
            ...product,
            name: product.title,
            $type: 'Product',
            priceInCents: product.price * 100,
          }
        },
      })

      expect(result.imported).toBe(2)

      const products = await db.collection('products').find()
      expect(products.items[0]?.priceInCents).toBe(1000)
    })

    it('skips documents filtered by transform', async () => {
      const jsonPath = path.join(tempDir, 'items.json')
      const data = [
        { name: 'Keep 1', status: 'active' },
        { name: 'Skip 1', status: 'inactive' },
        { name: 'Keep 2', status: 'active' },
      ]
      await fs.writeFile(jsonPath, JSON.stringify(data))

      const result = await importFromJson(db, 'items', jsonPath, {
        transform: (doc) => {
          const item = doc as { status: string }
          if (item.status === 'inactive') return null
          return doc
        },
      })

      expect(result.imported).toBe(2)
      expect(result.skipped).toBe(1)

      const items = await db.collection('items').find()
      expect(items.items).toHaveLength(2)
    })

    it('reports progress during import', async () => {
      const jsonPath = path.join(tempDir, 'large.json')
      const data = Array.from({ length: 50 }, (_, i) => ({
        name: `Item ${i}`,
        index: i,
      }))
      await fs.writeFile(jsonPath, JSON.stringify(data))

      const progressCalls: number[] = []
      await importFromJson(db, 'items', jsonPath, {
        batchSize: 10,
        onProgress: (count) => progressCalls.push(count),
      })

      expect(progressCalls.length).toBeGreaterThan(0)
      expect(progressCalls[progressCalls.length - 1]).toBe(50)
    })
  })

  describe('JSON object with nested array', () => {
    it('imports from specified array path', async () => {
      const jsonPath = path.join(tempDir, 'response.json')
      const data = {
        status: 'success',
        data: {
          items: [
            { name: 'Item 1' },
            { name: 'Item 2' },
          ],
        },
      }
      await fs.writeFile(jsonPath, JSON.stringify(data))

      const result = await importFromJson(db, 'items', jsonPath, {
        arrayPath: 'data.items',
      })

      expect(result.imported).toBe(2)
    })

    it('auto-detects single array in object', async () => {
      const jsonPath = path.join(tempDir, 'simple.json')
      const data = {
        items: [{ name: 'Item 1' }, { name: 'Item 2' }],
      }
      await fs.writeFile(jsonPath, JSON.stringify(data))

      const result = await importFromJson(db, 'items', jsonPath)

      expect(result.imported).toBe(2)
    })

    it('throws error for multiple arrays without arrayPath', async () => {
      const jsonPath = path.join(tempDir, 'multi.json')
      const data = {
        items: [{ name: 'Item 1' }],
        categories: [{ name: 'Cat 1' }],
      }
      await fs.writeFile(jsonPath, JSON.stringify(data))

      await expect(importFromJson(db, 'items', jsonPath)).rejects.toThrow(
        'Multiple arrays found'
      )
    })

    it('throws error for invalid array path', async () => {
      const jsonPath = path.join(tempDir, 'data.json')
      const data = { items: { nested: 'not an array' } }
      await fs.writeFile(jsonPath, JSON.stringify(data))

      await expect(
        importFromJson(db, 'items', jsonPath, { arrayPath: 'items.nested' })
      ).rejects.toThrow("does not contain an array")
    })
  })

  describe('error handling', () => {
    it('throws error for non-existent file', async () => {
      await expect(
        importFromJson(db, 'items', '/nonexistent/file.json')
      ).rejects.toThrow('File not found')
    })

    it('throws error for invalid JSON', async () => {
      const jsonPath = path.join(tempDir, 'invalid.json')
      await fs.writeFile(jsonPath, '{ invalid json }')

      await expect(importFromJson(db, 'items', jsonPath)).rejects.toThrow(
        'Invalid JSON'
      )
    })

    it('records errors for transform failures', async () => {
      const jsonPath = path.join(tempDir, 'items.json')
      const data = [
        { name: 'Good', value: 10 },
        { name: 'Bad', value: null },
      ]
      await fs.writeFile(jsonPath, JSON.stringify(data))

      const result = await importFromJson(db, 'items', jsonPath, {
        transform: (doc) => {
          const item = doc as { value: number | null }
          if (item.value === null) {
            throw new Error('Value is required')
          }
          return doc
        },
      })

      expect(result.imported).toBe(1)
      expect(result.failed).toBe(1)
      expect(result.errors).toHaveLength(1)
      expect(result.errors[0]?.message).toContain('Transform failed')
    })
  })
})

describe('importFromJsonl', () => {
  let db: ParqueDB
  let tempDir: string

  beforeEach(async () => {
    db = new ParqueDB({ storage: new MemoryBackend() })
    tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'parquedb-jsonl-test-'))
  })

  afterEach(async () => {
    db.dispose()
    await fs.rm(tempDir, { recursive: true, force: true })
  })

  it('imports documents from JSONL file', async () => {
    const jsonlPath = path.join(tempDir, 'events.jsonl')
    const lines = [
      JSON.stringify({ name: 'Event 1', timestamp: '2024-01-01' }),
      JSON.stringify({ name: 'Event 2', timestamp: '2024-01-02' }),
      JSON.stringify({ name: 'Event 3', timestamp: '2024-01-03' }),
    ]
    await fs.writeFile(jsonlPath, lines.join('\n'))

    const result = await importFromJsonl(db, 'events', jsonlPath)

    expect(result.imported).toBe(3)
    expect(result.failed).toBe(0)
  })

  it('handles empty lines', async () => {
    const jsonlPath = path.join(tempDir, 'events.jsonl')
    const lines = [
      JSON.stringify({ name: 'Event 1' }),
      '',
      JSON.stringify({ name: 'Event 2' }),
      '   ',
      JSON.stringify({ name: 'Event 3' }),
    ]
    await fs.writeFile(jsonlPath, lines.join('\n'))

    const result = await importFromJsonl(db, 'events', jsonlPath)

    expect(result.imported).toBe(3)
  })

  it('records errors for invalid JSON lines', async () => {
    const jsonlPath = path.join(tempDir, 'mixed.jsonl')
    const lines = [
      JSON.stringify({ name: 'Good 1' }),
      '{ invalid }',
      JSON.stringify({ name: 'Good 2' }),
    ]
    await fs.writeFile(jsonlPath, lines.join('\n'))

    const result = await importFromJsonl(db, 'events', jsonlPath)

    expect(result.imported).toBe(2)
    expect(result.failed).toBe(1)
    expect(result.errors[0]?.message).toContain('Invalid JSON at line 2')
  })

  it('applies transform to each line', async () => {
    const jsonlPath = path.join(tempDir, 'items.jsonl')
    const lines = [
      JSON.stringify({ title: 'Item 1', count: 5 }),
      JSON.stringify({ title: 'Item 2', count: 10 }),
    ]
    await fs.writeFile(jsonlPath, lines.join('\n'))

    await importFromJsonl(db, 'items', jsonlPath, {
      transform: (doc) => {
        const item = doc as { title: string; count: number }
        return {
          name: item.title,
          $type: 'Item',
          doubled: item.count * 2,
        }
      },
    })

    const items = await db.collection('items').find()
    expect(items.items[0]?.doubled).toBe(10)
    expect(items.items[1]?.doubled).toBe(20)
  })

  it('streams large files efficiently', async () => {
    const jsonlPath = path.join(tempDir, 'large.jsonl')
    const lines = Array.from({ length: 1000 }, (_, i) =>
      JSON.stringify({ name: `Item ${i}`, index: i })
    )
    await fs.writeFile(jsonlPath, lines.join('\n'))

    const result = await importFromJsonl(db, 'items', jsonlPath, {
      batchSize: 100,
    })

    expect(result.imported).toBe(1000)
    expect(result.duration).toBeLessThan(30000) // Should complete in reasonable time
  })
})
