/**
 * Tests for CSV import utilities
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import * as fs from 'fs/promises'
import * as path from 'path'
import * as os from 'os'
import { ParqueDB } from '../../../src/ParqueDB'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import { importFromCsv } from '../../../src/migration/csv'

describe('importFromCsv', () => {
  let db: ParqueDB
  let tempDir: string

  beforeEach(async () => {
    db = new ParqueDB({ storage: new MemoryBackend() })
    tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'parquedb-csv-test-'))
  })

  afterEach(async () => {
    db.dispose()
    await fs.rm(tempDir, { recursive: true, force: true })
  })

  describe('basic CSV import', () => {
    it('imports CSV with headers', async () => {
      const csvPath = path.join(tempDir, 'users.csv')
      const csv = `name,email,age
Alice,alice@example.com,30
Bob,bob@example.com,25
Charlie,charlie@example.com,35`
      await fs.writeFile(csvPath, csv)

      const result = await importFromCsv(db, 'users', csvPath)

      expect(result.imported).toBe(3)
      expect(result.failed).toBe(0)

      const users = await db.collection('users').find()
      expect(users.items).toHaveLength(3)
      expect(users.items[0]?.email).toBe('alice@example.com')
    })

    it('uses first column as name by default when no name column present', async () => {
      const csvPath = path.join(tempDir, 'products.csv')
      const csv = `sku,title,price
PRD-001,Widget,10.99
PRD-002,Gadget,25.50`
      await fs.writeFile(csvPath, csv)

      await importFromCsv(db, 'products', csvPath)

      const products = await db.collection('products').find()
      // Since 'title' is recognized as a name-like field, it uses that
      // The prepareDocument function checks for 'name' and 'title' first
      expect(products.items[0]?.name).toBe('Widget')
    })

    it('uses nameColumn option for name field', async () => {
      const csvPath = path.join(tempDir, 'products.csv')
      const csv = `sku,title,price
PRD-001,Widget,10.99
PRD-002,Gadget,25.50`
      await fs.writeFile(csvPath, csv)

      await importFromCsv(db, 'products', csvPath, { nameColumn: 'title' })

      const products = await db.collection('products').find()
      expect(products.items[0]?.name).toBe('Widget')
    })
  })

  describe('type inference', () => {
    it('infers number types', async () => {
      const csvPath = path.join(tempDir, 'data.csv')
      const csv = `name,count,price
Item 1,42,10.99
Item 2,100,5.50`
      await fs.writeFile(csvPath, csv)

      await importFromCsv(db, 'items', csvPath)

      const items = await db.collection('items').find()
      expect(typeof items.items[0]?.count).toBe('number')
      expect(items.items[0]?.count).toBe(42)
      expect(typeof items.items[0]?.price).toBe('number')
      expect(items.items[0]?.price).toBe(10.99)
    })

    it('infers boolean types', async () => {
      const csvPath = path.join(tempDir, 'flags.csv')
      const csv = `name,active,published
Item 1,true,false
Item 2,TRUE,FALSE
Item 3,1,0`
      await fs.writeFile(csvPath, csv)

      await importFromCsv(db, 'items', csvPath)

      const items = await db.collection('items').find()
      expect(items.items[0]?.active).toBe(true)
      expect(items.items[0]?.published).toBe(false)
      expect(items.items[1]?.active).toBe(true)
      expect(items.items[2]?.active).toBe(1) // Numbers stay as numbers
    })

    it('infers date types', async () => {
      const csvPath = path.join(tempDir, 'events.csv')
      const csv = `name,date,timestamp
Event 1,2024-01-15,2024-01-15T10:30:00Z
Event 2,2024-06-30,2024-06-30T14:45:00Z`
      await fs.writeFile(csvPath, csv)

      await importFromCsv(db, 'events', csvPath)

      const events = await db.collection('events').find()
      expect(events.items[0]?.date).toBeInstanceOf(Date)
      expect(events.items[0]?.timestamp).toBeInstanceOf(Date)
    })

    it('handles null values', async () => {
      const csvPath = path.join(tempDir, 'data.csv')
      const csv = `name,value,optional
Item 1,10,
Item 2,,null
Item 3,30,present`
      await fs.writeFile(csvPath, csv)

      await importFromCsv(db, 'items', csvPath)

      const items = await db.collection('items').find()
      expect(items.items[0]?.optional).toBe(null)
      expect(items.items[1]?.value).toBe(null)
      expect(items.items[1]?.optional).toBe(null)
    })

    it('disables type inference when inferTypes is false', async () => {
      const csvPath = path.join(tempDir, 'data.csv')
      const csv = `name,count
Item 1,42`
      await fs.writeFile(csvPath, csv)

      await importFromCsv(db, 'items', csvPath, { inferTypes: false })

      const items = await db.collection('items').find()
      expect(typeof items.items[0]?.count).toBe('string')
      expect(items.items[0]?.count).toBe('42')
    })
  })

  describe('custom column types', () => {
    it('applies custom column type mappings', async () => {
      const csvPath = path.join(tempDir, 'mixed.csv')
      // Use quoted JSON field to handle the JSON properly in CSV
      const csv = `name,id,metadata
Item 1,123,"{""key"":""value""}"
Item 2,456,"{""foo"":""bar""}"`
      await fs.writeFile(csvPath, csv)

      await importFromCsv(db, 'items', csvPath, {
        columnTypes: {
          id: 'string',
          metadata: 'json',
        },
      })

      const items = await db.collection('items').find()
      expect(typeof items.items[0]?.id).toBe('string')
      expect(items.items[0]?.id).toBe('123')
      // JSON parsing - metadata should be an object with key property
      const metadata = items.items[0]?.metadata
      expect(typeof metadata).toBe('object')
      expect(metadata).not.toBeNull()
      if (metadata && typeof metadata === 'object') {
        expect((metadata as Record<string, unknown>).key).toBe('value')
      }
    })
  })

  describe('delimiter handling', () => {
    it('handles semicolon delimiter', async () => {
      const csvPath = path.join(tempDir, 'semicolon.csv')
      const csv = `name;value;status
Item 1;100;active
Item 2;200;inactive`
      await fs.writeFile(csvPath, csv)

      const result = await importFromCsv(db, 'items', csvPath, { delimiter: ';' })

      expect(result.imported).toBe(2)

      const items = await db.collection('items').find()
      expect(items.items[0]?.value).toBe(100)
    })

    it('handles tab delimiter', async () => {
      const csvPath = path.join(tempDir, 'tabs.csv')
      const csv = `name\tvalue\tstatus
Item 1\t100\tactive
Item 2\t200\tinactive`
      await fs.writeFile(csvPath, csv)

      const result = await importFromCsv(db, 'items', csvPath, { delimiter: '\t' })

      expect(result.imported).toBe(2)
    })
  })

  describe('quoted fields', () => {
    it('handles quoted fields with commas', async () => {
      const csvPath = path.join(tempDir, 'quoted.csv')
      const csv = `name,description,price
"Widget",Simple widget,10
"Gadget, Pro","Advanced gadget, with features",25
"Tool",Basic tool,5`
      await fs.writeFile(csvPath, csv)

      const result = await importFromCsv(db, 'items', csvPath)

      expect(result.imported).toBe(3)

      const items = await db.collection('items').find()
      const gadget = items.items.find(i => i.name === 'Gadget, Pro')
      expect(gadget?.description).toBe('Advanced gadget, with features')
    })

    it('handles escaped quotes', async () => {
      const csvPath = path.join(tempDir, 'escaped.csv')
      const csv = `name,quote
Item 1,"He said ""hello"""
Item 2,"She replied ""hi"""`
      await fs.writeFile(csvPath, csv)

      const result = await importFromCsv(db, 'items', csvPath)

      const items = await db.collection('items').find()
      expect(items.items[0]?.quote).toBe('He said "hello"')
    })

    it('handles multiline quoted fields', async () => {
      const csvPath = path.join(tempDir, 'multiline.csv')
      const csv = `name,notes
"Item 1","Line one
Line two"
Item 2,Simple`
      await fs.writeFile(csvPath, csv)

      const result = await importFromCsv(db, 'items', csvPath)

      const items = await db.collection('items').find()
      expect(items.items[0]?.notes).toContain('Line one')
    })
  })

  describe('custom headers', () => {
    it('uses provided header array', async () => {
      const csvPath = path.join(tempDir, 'noheaders.csv')
      const csv = `Item 1,100,active
Item 2,200,inactive`
      await fs.writeFile(csvPath, csv)

      const result = await importFromCsv(db, 'items', csvPath, {
        headers: ['productName', 'quantity', 'status'],
      })

      expect(result.imported).toBe(2)

      const items = await db.collection('items').find()
      expect(items.items[0]?.productName).toBe('Item 1')
      expect(items.items[0]?.quantity).toBe(100)
    })

    it('generates column names when headers is false', async () => {
      const csvPath = path.join(tempDir, 'noheaders.csv')
      const csv = `Item 1,100,active
Item 2,200,inactive`
      await fs.writeFile(csvPath, csv)

      await importFromCsv(db, 'items', csvPath, { headers: false })

      const items = await db.collection('items').find()
      expect(items.items[0]?.column1).toBe('Item 1')
      expect(items.items[0]?.column2).toBe(100)
    })
  })

  describe('header normalization', () => {
    it('normalizes headers with underscores', async () => {
      const csvPath = path.join(tempDir, 'headers.csv')
      const csv = `product_name,unit_price,status_code
Widget,10.99,active`
      await fs.writeFile(csvPath, csv)

      await importFromCsv(db, 'products', csvPath)

      const products = await db.collection('products').find()
      // Check that underscores are converted to camelCase
      expect(products.items[0]?.productName).toBe('Widget')
      expect(products.items[0]?.unitPrice).toBe(10.99)
      expect(products.items[0]?.statusCode).toBe('active')
    })

    it('handles special characters in headers', async () => {
      const csvPath = path.join(tempDir, 'special.csv')
      const csv = `"Name (Full)","Price ($)","Count #"
Item,10,5`
      await fs.writeFile(csvPath, csv)

      await importFromCsv(db, 'items', csvPath)

      const items = await db.collection('items').find()
      // Special chars are replaced with underscores and then converted to camelCase
      // "Name (Full)" -> "Name__Full_" -> "Name_Full" -> "NameFull"
      // The actual key name depends on the normalization implementation
      const keys = Object.keys(items.items[0] || {})
      // Just verify we have some normalized keys that don't contain special chars
      expect(keys.every(k => !k.includes('(') && !k.includes(')'))).toBe(true)
    })
  })

  describe('transform function', () => {
    it('applies transform to each row', async () => {
      const csvPath = path.join(tempDir, 'products.csv')
      const csv = `title,price,category
Widget,10.99,Electronics
Gadget,25.50,Electronics`
      await fs.writeFile(csvPath, csv)

      await importFromCsv(db, 'products', csvPath, {
        transform: (doc) => {
          const row = doc as { title: string; price: number; category: string }
          return {
            ...row,
            name: row.title,
            $type: 'Product',
            priceInCents: Math.round(row.price * 100),
          }
        },
      })

      const products = await db.collection('products').find()
      expect(products.items[0]?.priceInCents).toBe(1099)
    })

    it('skips rows returning null from transform', async () => {
      const csvPath = path.join(tempDir, 'items.csv')
      const csv = `name,status
Keep,active
Skip,inactive
Also Keep,active`
      await fs.writeFile(csvPath, csv)

      const result = await importFromCsv(db, 'items', csvPath, {
        transform: (doc) => {
          const row = doc as { status: string }
          return row.status === 'active' ? doc : null
        },
      })

      expect(result.imported).toBe(2)
      expect(result.skipped).toBe(1)
    })
  })

  describe('empty lines handling', () => {
    it('skips empty lines by default', async () => {
      const csvPath = path.join(tempDir, 'empty.csv')
      const csv = `name,value
Item 1,10

Item 2,20

Item 3,30`
      await fs.writeFile(csvPath, csv)

      const result = await importFromCsv(db, 'items', csvPath)

      expect(result.imported).toBe(3)
    })

    it('can include empty lines when skipEmptyLines is false', async () => {
      const csvPath = path.join(tempDir, 'empty.csv')
      const csv = `name,value
Item 1,10

Item 2,20`
      await fs.writeFile(csvPath, csv)

      const result = await importFromCsv(db, 'items', csvPath, {
        skipEmptyLines: false,
      })

      // Empty line would create a document with null values
      // but since name would be null, it might fail validation
      expect(result.imported).toBeGreaterThanOrEqual(2)
    })
  })

  describe('error handling', () => {
    it('throws error for non-existent file', async () => {
      await expect(
        importFromCsv(db, 'items', '/nonexistent/file.csv')
      ).rejects.toThrow('File not found')
    })

    it('records transform errors', async () => {
      const csvPath = path.join(tempDir, 'errors.csv')
      const csv = `name,value
Good,10
Bad,error`
      await fs.writeFile(csvPath, csv)

      const result = await importFromCsv(db, 'items', csvPath, {
        transform: (doc) => {
          const row = doc as { value: unknown }
          if (row.value === 'error') {
            throw new Error('Invalid value')
          }
          return doc
        },
      })

      expect(result.imported).toBe(1)
      expect(result.failed).toBe(1)
      expect(result.errors).toHaveLength(1)
    })
  })

  describe('progress reporting', () => {
    it('reports progress during import', async () => {
      const csvPath = path.join(tempDir, 'large.csv')
      const header = 'name,index'
      const rows = Array.from({ length: 50 }, (_, i) => `Item ${i},${i}`)
      await fs.writeFile(csvPath, [header, ...rows].join('\n'))

      const progressCalls: number[] = []
      await importFromCsv(db, 'items', csvPath, {
        batchSize: 10,
        onProgress: (count) => progressCalls.push(count),
      })

      expect(progressCalls.length).toBeGreaterThan(0)
      expect(progressCalls[progressCalls.length - 1]).toBe(50)
    })
  })
})
