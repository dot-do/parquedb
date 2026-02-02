/**
 * Tests for streaming migration utilities
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import * as fs from 'fs/promises'
import * as path from 'path'
import * as os from 'os'
import { ParqueDB } from '../../../src/ParqueDB'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import {
  streamFromJsonl,
  streamFromJson,
  streamFromCsv,
  streamFromMongodbJsonl,
  importFromMongodb,
} from '../../../src/migration'

describe('streamFromJsonl', () => {
  let tempDir: string

  beforeEach(async () => {
    tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'parquedb-stream-jsonl-'))
  })

  afterEach(async () => {
    await fs.rm(tempDir, { recursive: true, force: true })
  })

  it('streams documents from JSONL file', async () => {
    const jsonlPath = path.join(tempDir, 'data.jsonl')
    const lines = [
      JSON.stringify({ name: 'Item 1', value: 10 }),
      JSON.stringify({ name: 'Item 2', value: 20 }),
      JSON.stringify({ name: 'Item 3', value: 30 }),
    ]
    await fs.writeFile(jsonlPath, lines.join('\n'))

    const docs: Record<string, unknown>[] = []
    for await (const { document, error } of streamFromJsonl(jsonlPath)) {
      expect(error).toBeUndefined()
      docs.push(document)
    }

    expect(docs).toHaveLength(3)
    expect(docs[0]?.name).toBe('Item 1')
    expect(docs[2]?.value).toBe(30)
  })

  it('yields line numbers', async () => {
    const jsonlPath = path.join(tempDir, 'data.jsonl')
    const lines = [
      JSON.stringify({ name: 'Item 1' }),
      '',
      JSON.stringify({ name: 'Item 2' }),
      '   ',
      JSON.stringify({ name: 'Item 3' }),
    ]
    await fs.writeFile(jsonlPath, lines.join('\n'))

    const lineNumbers: number[] = []
    for await (const { lineNumber } of streamFromJsonl(jsonlPath)) {
      lineNumbers.push(lineNumber)
    }

    expect(lineNumbers).toEqual([1, 3, 5])
  })

  it('yields errors for invalid JSON', async () => {
    const jsonlPath = path.join(tempDir, 'data.jsonl')
    const lines = [
      JSON.stringify({ name: 'Good 1' }),
      '{ invalid json }',
      JSON.stringify({ name: 'Good 2' }),
    ]
    await fs.writeFile(jsonlPath, lines.join('\n'))

    const results: Array<{ error?: string; lineNumber: number }> = []
    for await (const { error, lineNumber } of streamFromJsonl(jsonlPath)) {
      results.push({ error, lineNumber })
    }

    expect(results).toHaveLength(3)
    expect(results[0]?.error).toBeUndefined()
    expect(results[1]?.error).toContain('Invalid JSON')
    expect(results[1]?.lineNumber).toBe(2)
    expect(results[2]?.error).toBeUndefined()
  })

  it('skips errors when skipErrors is true', async () => {
    const jsonlPath = path.join(tempDir, 'data.jsonl')
    const lines = [
      JSON.stringify({ name: 'Good 1' }),
      '{ invalid }',
      JSON.stringify({ name: 'Good 2' }),
    ]
    await fs.writeFile(jsonlPath, lines.join('\n'))

    const docs: Record<string, unknown>[] = []
    for await (const { document } of streamFromJsonl(jsonlPath, { skipErrors: true })) {
      docs.push(document)
    }

    expect(docs).toHaveLength(2)
  })

  it('applies transform function', async () => {
    const jsonlPath = path.join(tempDir, 'data.jsonl')
    const lines = [
      JSON.stringify({ value: 10 }),
      JSON.stringify({ value: 20 }),
    ]
    await fs.writeFile(jsonlPath, lines.join('\n'))

    const docs: Record<string, unknown>[] = []
    for await (const { document } of streamFromJsonl(jsonlPath, {
      transform: (doc) => {
        const item = doc as { value: number }
        return { ...item, doubled: item.value * 2 }
      },
    })) {
      docs.push(document)
    }

    expect(docs[0]?.doubled).toBe(20)
    expect(docs[1]?.doubled).toBe(40)
  })

  it('skips documents when transform returns null', async () => {
    const jsonlPath = path.join(tempDir, 'data.jsonl')
    const lines = [
      JSON.stringify({ name: 'Keep', status: 'active' }),
      JSON.stringify({ name: 'Skip', status: 'inactive' }),
      JSON.stringify({ name: 'Also Keep', status: 'active' }),
    ]
    await fs.writeFile(jsonlPath, lines.join('\n'))

    const docs: Record<string, unknown>[] = []
    for await (const { document } of streamFromJsonl(jsonlPath, {
      transform: (doc) => {
        const item = doc as { status: string }
        return item.status === 'active' ? doc : null
      },
    })) {
      docs.push(document)
    }

    expect(docs).toHaveLength(2)
  })

  it('throws error for non-existent file', async () => {
    await expect(async () => {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      for await (const _ of streamFromJsonl('/nonexistent/file.jsonl')) {
        // Should not reach here
      }
    }).rejects.toThrow('File not found')
  })

  it('handles large files efficiently', async () => {
    const jsonlPath = path.join(tempDir, 'large.jsonl')
    const count = 10000
    const lines = Array.from({ length: count }, (_, i) =>
      JSON.stringify({ name: `Item ${i}`, index: i, data: 'x'.repeat(100) })
    )
    await fs.writeFile(jsonlPath, lines.join('\n'))

    let processed = 0
    const startMemory = process.memoryUsage().heapUsed

    for await (const { document } of streamFromJsonl(jsonlPath)) {
      if (document) processed++
    }

    const endMemory = process.memoryUsage().heapUsed
    const memoryIncrease = endMemory - startMemory

    expect(processed).toBe(count)
    // Memory increase should be relatively small (streaming should not load all data)
    // This is a rough heuristic - the file is ~2MB, memory increase should be < 50MB
    expect(memoryIncrease).toBeLessThan(50 * 1024 * 1024)
  })
})

describe('streamFromJson', () => {
  let tempDir: string

  beforeEach(async () => {
    tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'parquedb-stream-json-'))
  })

  afterEach(async () => {
    await fs.rm(tempDir, { recursive: true, force: true })
  })

  it('streams documents from JSON array', async () => {
    const jsonPath = path.join(tempDir, 'data.json')
    const data = [
      { name: 'Item 1', value: 10 },
      { name: 'Item 2', value: 20 },
      { name: 'Item 3', value: 30 },
    ]
    await fs.writeFile(jsonPath, JSON.stringify(data))

    const docs: Record<string, unknown>[] = []
    for await (const { document } of streamFromJson(jsonPath)) {
      docs.push(document)
    }

    expect(docs).toHaveLength(3)
    expect(docs[0]?.name).toBe('Item 1')
  })

  it('streams from nested array path', async () => {
    const jsonPath = path.join(tempDir, 'data.json')
    const data = {
      meta: { total: 2 },
      data: {
        items: [
          { name: 'Item 1' },
          { name: 'Item 2' },
        ],
      },
    }
    await fs.writeFile(jsonPath, JSON.stringify(data))

    const docs: Record<string, unknown>[] = []
    for await (const { document } of streamFromJson(jsonPath, { arrayPath: 'data.items' })) {
      docs.push(document)
    }

    expect(docs).toHaveLength(2)
  })

  it('applies transform function', async () => {
    const jsonPath = path.join(tempDir, 'data.json')
    const data = [{ value: 10 }, { value: 20 }]
    await fs.writeFile(jsonPath, JSON.stringify(data))

    const docs: Record<string, unknown>[] = []
    for await (const { document } of streamFromJson(jsonPath, {
      transform: (doc) => {
        const item = doc as { value: number }
        return { ...item, doubled: item.value * 2 }
      },
    })) {
      docs.push(document)
    }

    expect(docs[0]?.doubled).toBe(20)
  })
})

describe('streamFromCsv', () => {
  let tempDir: string

  beforeEach(async () => {
    tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'parquedb-stream-csv-'))
  })

  afterEach(async () => {
    await fs.rm(tempDir, { recursive: true, force: true })
  })

  it('streams rows from CSV file', async () => {
    const csvPath = path.join(tempDir, 'data.csv')
    const csv = `name,value,active
Item 1,10,true
Item 2,20,false
Item 3,30,true`
    await fs.writeFile(csvPath, csv)

    const docs: Record<string, unknown>[] = []
    for await (const { document } of streamFromCsv(csvPath)) {
      docs.push(document)
    }

    expect(docs).toHaveLength(3)
    expect(docs[0]?.name).toBe('Item 1')
    expect(docs[0]?.value).toBe(10)
    expect(docs[0]?.active).toBe(true)
  })

  it('yields line numbers (accounting for header)', async () => {
    const csvPath = path.join(tempDir, 'data.csv')
    const csv = `name,value
Item 1,10
Item 2,20`
    await fs.writeFile(csvPath, csv)

    const lineNumbers: number[] = []
    for await (const { lineNumber } of streamFromCsv(csvPath)) {
      lineNumbers.push(lineNumber)
    }

    // Line 1 is header, so data starts at line 2
    expect(lineNumbers).toEqual([2, 3])
  })

  it('uses custom delimiter', async () => {
    const csvPath = path.join(tempDir, 'data.csv')
    const csv = `name;value;status
Item 1;100;active
Item 2;200;inactive`
    await fs.writeFile(csvPath, csv)

    const docs: Record<string, unknown>[] = []
    for await (const { document } of streamFromCsv(csvPath, { delimiter: ';' })) {
      docs.push(document)
    }

    expect(docs).toHaveLength(2)
    expect(docs[0]?.value).toBe(100)
  })

  it('uses provided headers array', async () => {
    const csvPath = path.join(tempDir, 'data.csv')
    const csv = `Item 1,100,active
Item 2,200,inactive`
    await fs.writeFile(csvPath, csv)

    const docs: Record<string, unknown>[] = []
    for await (const { document } of streamFromCsv(csvPath, {
      headers: ['productName', 'quantity', 'status'],
    })) {
      docs.push(document)
    }

    expect(docs).toHaveLength(2)
    expect(docs[0]?.productName).toBe('Item 1')
    expect(docs[0]?.quantity).toBe(100)
  })

  it('applies column type mappings', async () => {
    const csvPath = path.join(tempDir, 'data.csv')
    const csv = `id,name,price
123,Widget,10.99
456,Gadget,25.50`
    await fs.writeFile(csvPath, csv)

    const docs: Record<string, unknown>[] = []
    for await (const { document } of streamFromCsv(csvPath, {
      columnTypes: { id: 'string' },
    })) {
      docs.push(document)
    }

    expect(typeof docs[0]?.id).toBe('string')
    expect(docs[0]?.id).toBe('123')
  })

  it('skips empty lines', async () => {
    const csvPath = path.join(tempDir, 'data.csv')
    const csv = `name,value
Item 1,10

Item 2,20

Item 3,30`
    await fs.writeFile(csvPath, csv)

    const docs: Record<string, unknown>[] = []
    for await (const { document } of streamFromCsv(csvPath)) {
      docs.push(document)
    }

    expect(docs).toHaveLength(3)
  })

  it('applies transform function', async () => {
    const csvPath = path.join(tempDir, 'data.csv')
    const csv = `title,price
Widget,10.99
Gadget,25.50`
    await fs.writeFile(csvPath, csv)

    const docs: Record<string, unknown>[] = []
    for await (const { document } of streamFromCsv(csvPath, {
      transform: (doc) => {
        const row = doc as { title: string; price: number }
        return {
          ...row,
          name: row.title,
          priceInCents: Math.round(row.price * 100),
        }
      },
    })) {
      docs.push(document)
    }

    expect(docs[0]?.name).toBe('Widget')
    expect(docs[0]?.priceInCents).toBe(1099)
  })

  it('handles large files efficiently', async () => {
    const csvPath = path.join(tempDir, 'large.csv')
    const header = 'name,index,data'
    const rows = Array.from({ length: 10000 }, (_, i) => `Item ${i},${i},${'x'.repeat(100)}`)
    await fs.writeFile(csvPath, [header, ...rows].join('\n'))

    let processed = 0
    for await (const { document } of streamFromCsv(csvPath)) {
      if (document) processed++
    }

    expect(processed).toBe(10000)
  })
})

describe('streamFromMongodbJsonl', () => {
  let tempDir: string

  beforeEach(async () => {
    tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'parquedb-stream-mongodb-'))
  })

  afterEach(async () => {
    await fs.rm(tempDir, { recursive: true, force: true })
  })

  it('streams and converts MongoDB Extended JSON', async () => {
    const jsonlPath = path.join(tempDir, 'data.jsonl')
    const lines = [
      JSON.stringify({ _id: { $oid: '507f1f77bcf86cd799439011' }, name: 'Item 1' }),
      JSON.stringify({ _id: { $oid: '507f1f77bcf86cd799439012' }, name: 'Item 2' }),
    ]
    await fs.writeFile(jsonlPath, lines.join('\n'))

    const docs: Record<string, unknown>[] = []
    for await (const { document } of streamFromMongodbJsonl(jsonlPath)) {
      docs.push(document)
    }

    expect(docs).toHaveLength(2)
    // ObjectId should be converted to string
    expect(docs[0]?._id).toBe('507f1f77bcf86cd799439011')
  })

  it('converts MongoDB dates', async () => {
    const jsonlPath = path.join(tempDir, 'data.jsonl')
    const lines = [
      JSON.stringify({
        name: 'Event',
        createdAt: { $date: '2024-01-15T10:30:00Z' },
      }),
    ]
    await fs.writeFile(jsonlPath, lines.join('\n'))

    const docs: Record<string, unknown>[] = []
    for await (const { document } of streamFromMongodbJsonl(jsonlPath)) {
      docs.push(document)
    }

    expect(docs[0]?.createdAt).toBeInstanceOf(Date)
  })

  it('converts MongoDB number types', async () => {
    const jsonlPath = path.join(tempDir, 'data.jsonl')
    const lines = [
      JSON.stringify({
        name: 'Numbers',
        longValue: { $numberLong: '9223372036854775807' },
        decimalValue: { $numberDecimal: '123.456' },
      }),
    ]
    await fs.writeFile(jsonlPath, lines.join('\n'))

    const docs: Record<string, unknown>[] = []
    for await (const { document } of streamFromMongodbJsonl(jsonlPath)) {
      docs.push(document)
    }

    expect(typeof docs[0]?.longValue).toBe('number')
    expect(typeof docs[0]?.decimalValue).toBe('number')
  })

  it('applies transform after BSON conversion', async () => {
    const jsonlPath = path.join(tempDir, 'data.jsonl')
    const lines = [
      JSON.stringify({ _id: { $oid: '507f1f77bcf86cd799439011' }, value: 10 }),
    ]
    await fs.writeFile(jsonlPath, lines.join('\n'))

    const docs: Record<string, unknown>[] = []
    for await (const { document } of streamFromMongodbJsonl(jsonlPath, {
      transform: (doc) => {
        const item = doc as { _id: string; value: number }
        return { ...item, mongoId: item._id, doubled: item.value * 2 }
      },
    })) {
      docs.push(document)
    }

    expect(docs[0]?.mongoId).toBe('507f1f77bcf86cd799439011')
    expect(docs[0]?.doubled).toBe(20)
  })
})

describe('importFromMongodb with streaming option', () => {
  let db: ParqueDB
  let tempDir: string

  beforeEach(async () => {
    db = new ParqueDB({ storage: new MemoryBackend() })
    tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'parquedb-mongodb-stream-'))
  })

  afterEach(async () => {
    db.dispose()
    await fs.rm(tempDir, { recursive: true, force: true })
  })

  it('imports with streaming enabled for JSONL files', async () => {
    const jsonlPath = path.join(tempDir, 'data.jsonl')
    const lines = [
      JSON.stringify({ _id: { $oid: '507f1f77bcf86cd799439011' }, name: 'Item 1' }),
      JSON.stringify({ _id: { $oid: '507f1f77bcf86cd799439012' }, name: 'Item 2' }),
      JSON.stringify({ _id: { $oid: '507f1f77bcf86cd799439013' }, name: 'Item 3' }),
    ]
    await fs.writeFile(jsonlPath, lines.join('\n'))

    const result = await importFromMongodb(db, 'items', jsonlPath, { streaming: true })

    expect(result.imported).toBe(3)
    expect(result.failed).toBe(0)

    const items = await db.collection('items').find()
    expect(items.items).toHaveLength(3)
  })

  it('reports progress during streaming import', async () => {
    const jsonlPath = path.join(tempDir, 'data.jsonl')
    const lines = Array.from({ length: 50 }, (_, i) =>
      JSON.stringify({ name: `Item ${i}`, index: i })
    )
    await fs.writeFile(jsonlPath, lines.join('\n'))

    const progressCalls: number[] = []
    await importFromMongodb(db, 'items', jsonlPath, {
      streaming: true,
      batchSize: 10,
      onProgress: (count) => progressCalls.push(count),
    })

    expect(progressCalls.length).toBeGreaterThan(0)
    expect(progressCalls[progressCalls.length - 1]).toBe(50)
  })

  it('handles errors during streaming import', async () => {
    const jsonlPath = path.join(tempDir, 'data.jsonl')
    const lines = [
      JSON.stringify({ name: 'Good 1' }),
      '{ invalid json }',
      JSON.stringify({ name: 'Good 2' }),
    ]
    await fs.writeFile(jsonlPath, lines.join('\n'))

    const result = await importFromMongodb(db, 'items', jsonlPath, { streaming: true })

    expect(result.imported).toBe(2)
    expect(result.failed).toBe(1)
    expect(result.errors).toHaveLength(1)
    expect(result.errors[0]?.message).toContain('Invalid JSON')
  })

  it('applies BSON conversion during streaming import', async () => {
    const jsonlPath = path.join(tempDir, 'data.jsonl')
    const lines = [
      JSON.stringify({
        _id: { $oid: '507f1f77bcf86cd799439011' },
        name: 'Item 1',
        createdAt: { $date: '2024-01-15T10:30:00Z' },
      }),
    ]
    await fs.writeFile(jsonlPath, lines.join('\n'))

    await importFromMongodb(db, 'items', jsonlPath, { streaming: true })

    const items = await db.collection('items').find()
    // ObjectId stored in mongoId field
    expect(items.items[0]?.mongoId).toBe('507f1f77bcf86cd799439011')
    // Date should be converted
    expect(items.items[0]?.createdAt).toBeInstanceOf(Date)
  })
})
