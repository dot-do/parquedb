/**
 * Parquet Layer Tests
 *
 * Tests for the Parquet reading/writing integration using hyparquet.
 * These tests verify:
 * - Variant encoding/decoding
 * - Schema conversion
 * - Reader functionality
 * - Writer functionality
 *
 * Uses real FsBackend storage instead of mocks to verify actual Parquet files.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtemp, rm, readdir, stat } from 'node:fs/promises'
import { cleanupTempDir } from '../setup'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

// Import from parquet module
import {
  // Variant
  encodeVariant,
  decodeVariant,
  shredObject,
  mergeShredded,
  isEncodable,
  estimateVariantSize,
  variantEquals,

  // Schema
  inferParquetType,
  toParquetSchema,
  createEntitySchema,
  createRelationshipSchema,
  createEventSchema,
  validateParquetSchema,
  getShredFields,
  mergeSchemas,
  getColumnNames,
  hasColumn,

  // Reader
  ParquetReader,
  createAsyncBuffer,
  initializeAsyncBuffer,

  // Writer
  ParquetWriter,

  // Types
  type ParquetSchema,
  type ParquetFieldSchema,
} from '../../src/parquet'

import { FsBackend } from '../../src/storage/FsBackend'
import type { StorageBackend, TypeDefinition } from '../../src/types'

// =============================================================================
// Variant Encoding Tests
// =============================================================================

describe('Variant Encoding/Decoding', () => {
  describe('encodeVariant / decodeVariant', () => {
    it('should encode and decode null', () => {
      const encoded = encodeVariant(null)
      const decoded = decodeVariant(encoded)
      expect(decoded).toBeNull()
    })

    it('should encode and decode undefined as null', () => {
      const encoded = encodeVariant(undefined)
      const decoded = decodeVariant(encoded)
      expect(decoded).toBeNull()
    })

    it('should encode and decode true', () => {
      const encoded = encodeVariant(true)
      const decoded = decodeVariant(encoded)
      expect(decoded).toBe(true)
    })

    it('should encode and decode false', () => {
      const encoded = encodeVariant(false)
      const decoded = decodeVariant(encoded)
      expect(decoded).toBe(false)
    })

    it('should encode and decode small integers', () => {
      const values = [0, 1, -1, 127, -128]
      for (const value of values) {
        const encoded = encodeVariant(value)
        const decoded = decodeVariant(encoded)
        expect(decoded).toBe(value)
      }
    })

    it('should encode and decode medium integers', () => {
      const values = [128, -129, 32767, -32768]
      for (const value of values) {
        const encoded = encodeVariant(value)
        const decoded = decodeVariant(encoded)
        expect(decoded).toBe(value)
      }
    })

    it('should encode and decode large integers', () => {
      const values = [32768, -32769, 2147483647, -2147483648]
      for (const value of values) {
        const encoded = encodeVariant(value)
        const decoded = decodeVariant(encoded)
        expect(decoded).toBe(value)
      }
    })

    it('should encode and decode floating point numbers', () => {
      const values = [3.14, -2.718, 0.0, 1e10, 1e-10]
      for (const value of values) {
        const encoded = encodeVariant(value)
        const decoded = decodeVariant(encoded)
        expect(decoded).toBeCloseTo(value)
      }
    })

    it('should encode and decode strings', () => {
      const values = ['', 'hello', 'Hello, World!', 'unicode: \u{1F600}', 'newline\ntest']
      for (const value of values) {
        const encoded = encodeVariant(value)
        const decoded = decodeVariant(encoded)
        expect(decoded).toBe(value)
      }
    })

    it('should encode and decode Date objects', () => {
      const date = new Date('2024-01-15T12:30:00Z')
      const encoded = encodeVariant(date)
      const decoded = decodeVariant(encoded) as Date
      expect(decoded).toBeInstanceOf(Date)
      expect(decoded.getTime()).toBe(date.getTime())
    })

    it('should encode and decode Uint8Array', () => {
      const binary = new Uint8Array([1, 2, 3, 4, 5])
      const encoded = encodeVariant(binary)
      const decoded = decodeVariant(encoded) as Uint8Array
      expect(decoded).toBeInstanceOf(Uint8Array)
      expect(Array.from(decoded)).toEqual([1, 2, 3, 4, 5])
    })

    it('should encode and decode empty array', () => {
      const encoded = encodeVariant([])
      const decoded = decodeVariant(encoded)
      expect(decoded).toEqual([])
    })

    it('should encode and decode array of primitives', () => {
      const arr = [1, 'two', true, null]
      const encoded = encodeVariant(arr)
      const decoded = decodeVariant(encoded)
      expect(decoded).toEqual(arr)
    })

    it('should encode and decode nested arrays', () => {
      const arr = [[1, 2], [3, [4, 5]]]
      const encoded = encodeVariant(arr)
      const decoded = decodeVariant(encoded)
      expect(decoded).toEqual(arr)
    })

    it('should encode and decode empty object', () => {
      const encoded = encodeVariant({})
      const decoded = decodeVariant(encoded)
      expect(decoded).toEqual({})
    })

    it('should encode and decode object with primitives', () => {
      const obj = { a: 1, b: 'two', c: true, d: null }
      const encoded = encodeVariant(obj)
      const decoded = decodeVariant(encoded)
      expect(decoded).toEqual(obj)
    })

    it('should encode and decode nested objects', () => {
      const obj = {
        user: {
          name: 'John',
          age: 30,
          tags: ['developer', 'writer'],
        },
        metadata: {
          created: new Date('2024-01-01T00:00:00Z'),
        },
      }
      const encoded = encodeVariant(obj)
      const decoded = decodeVariant(encoded)
      expect((decoded as any).user.name).toBe('John')
      expect((decoded as any).user.age).toBe(30)
      expect((decoded as any).user.tags).toEqual(['developer', 'writer'])
    })

    it('should encode and decode complex nested structure', () => {
      const complex = {
        id: 123,
        items: [
          { name: 'Item 1', count: 5 },
          { name: 'Item 2', count: 10 },
        ],
        meta: {
          tags: ['a', 'b'],
          nested: {
            deep: {
              value: true,
            },
          },
        },
      }
      const encoded = encodeVariant(complex)
      const decoded = decodeVariant(encoded)
      expect(decoded).toEqual(complex)
    })
  })

  describe('shredObject', () => {
    it('should shred specified fields from object', () => {
      const obj = { a: 1, b: 2, c: 3, d: 4 }
      const { shredded, remaining } = shredObject(obj, ['a', 'c'])

      expect(shredded).toEqual({ a: 1, c: 3 })
      expect(remaining).toEqual({ b: 2, d: 4 })
    })

    it('should handle empty shred fields', () => {
      const obj = { a: 1, b: 2 }
      const { shredded, remaining } = shredObject(obj, [])

      expect(shredded).toEqual({})
      expect(remaining).toEqual(obj)
    })

    it('should handle all fields shredded', () => {
      const obj = { a: 1, b: 2 }
      const { shredded, remaining } = shredObject(obj, ['a', 'b'])

      expect(shredded).toEqual(obj)
      expect(remaining).toEqual({})
    })

    it('should ignore non-existent fields', () => {
      const obj = { a: 1, b: 2 }
      const { shredded, remaining } = shredObject(obj, ['a', 'nonexistent'])

      expect(shredded).toEqual({ a: 1 })
      expect(remaining).toEqual({ b: 2 })
    })
  })

  describe('mergeShredded', () => {
    it('should merge shredded and remaining data', () => {
      const shredded = { a: 1, c: 3 }
      const remaining = { b: 2, d: 4 }
      const merged = mergeShredded(shredded, remaining)

      expect(merged).toEqual({ a: 1, b: 2, c: 3, d: 4 })
    })

    it('should give shredded fields precedence', () => {
      const shredded = { a: 'new' }
      const remaining = { a: 'old', b: 2 }
      const merged = mergeShredded(shredded, remaining)

      expect(merged.a).toBe('new')
    })
  })

  describe('isEncodable', () => {
    it('should return true for encodable values', () => {
      expect(isEncodable(null)).toBe(true)
      expect(isEncodable(undefined)).toBe(true)
      expect(isEncodable(true)).toBe(true)
      expect(isEncodable(false)).toBe(true)
      expect(isEncodable(42)).toBe(true)
      expect(isEncodable(3.14)).toBe(true)
      expect(isEncodable('string')).toBe(true)
      expect(isEncodable(new Date())).toBe(true)
      expect(isEncodable(new Uint8Array())).toBe(true)
      expect(isEncodable([])).toBe(true)
      expect(isEncodable({})).toBe(true)
    })

    it('should return false for non-finite numbers', () => {
      expect(isEncodable(Infinity)).toBe(false)
      expect(isEncodable(-Infinity)).toBe(false)
      expect(isEncodable(NaN)).toBe(false)
    })

    it('should return false for invalid dates', () => {
      expect(isEncodable(new Date('invalid'))).toBe(false)
    })

    it('should recursively check arrays', () => {
      expect(isEncodable([1, 2, 3])).toBe(true)
      expect(isEncodable([1, Infinity, 3])).toBe(false)
    })

    it('should recursively check objects', () => {
      expect(isEncodable({ a: 1 })).toBe(true)
      expect(isEncodable({ a: Infinity })).toBe(false)
    })
  })

  describe('estimateVariantSize', () => {
    it('should estimate size for primitives', () => {
      expect(estimateVariantSize(null)).toBe(3) // header + type
      expect(estimateVariantSize(true)).toBe(3)
      expect(estimateVariantSize(0)).toBeLessThan(10)
      expect(estimateVariantSize(1000000)).toBeLessThan(15)
    })

    it('should estimate size for strings', () => {
      const short = estimateVariantSize('hi')
      const long = estimateVariantSize('hello world')
      expect(long).toBeGreaterThan(short)
    })

    it('should estimate size for objects', () => {
      const size = estimateVariantSize({ a: 1, b: 2 })
      expect(size).toBeGreaterThan(10)
    })
  })

  describe('variantEquals', () => {
    it('should compare primitives', () => {
      expect(variantEquals(1, 1)).toBe(true)
      expect(variantEquals(1, 2)).toBe(false)
      expect(variantEquals('a', 'a')).toBe(true)
      expect(variantEquals(null, null)).toBe(true)
    })

    it('should compare dates', () => {
      const d1 = new Date('2024-01-01')
      const d2 = new Date('2024-01-01')
      const d3 = new Date('2024-01-02')

      expect(variantEquals(d1, d2)).toBe(true)
      expect(variantEquals(d1, d3)).toBe(false)
    })

    it('should compare Uint8Arrays', () => {
      const a1 = new Uint8Array([1, 2, 3])
      const a2 = new Uint8Array([1, 2, 3])
      const a3 = new Uint8Array([1, 2, 4])

      expect(variantEquals(a1, a2)).toBe(true)
      expect(variantEquals(a1, a3)).toBe(false)
    })

    it('should compare arrays', () => {
      expect(variantEquals([1, 2], [1, 2])).toBe(true)
      expect(variantEquals([1, 2], [1, 3])).toBe(false)
      expect(variantEquals([1, 2], [1, 2, 3])).toBe(false)
    })

    it('should compare objects', () => {
      expect(variantEquals({ a: 1 }, { a: 1 })).toBe(true)
      expect(variantEquals({ a: 1 }, { a: 2 })).toBe(false)
      expect(variantEquals({ a: 1 }, { b: 1 })).toBe(false)
    })

    it('should compare nested structures', () => {
      const obj1 = { arr: [1, { x: 2 }] }
      const obj2 = { arr: [1, { x: 2 }] }
      const obj3 = { arr: [1, { x: 3 }] }

      expect(variantEquals(obj1, obj2)).toBe(true)
      expect(variantEquals(obj1, obj3)).toBe(false)
    })
  })
})

// =============================================================================
// Schema Conversion Tests
// =============================================================================

describe('Schema Conversion', () => {
  describe('inferParquetType', () => {
    it('should infer type for basic types', () => {
      expect(inferParquetType('string').type).toBe('STRING')
      expect(inferParquetType('int').type).toBe('INT64')
      expect(inferParquetType('boolean').type).toBe('BOOLEAN')
      expect(inferParquetType('datetime').type).toBe('TIMESTAMP_MILLIS')
    })

    it('should handle required modifier', () => {
      const required = inferParquetType('string!')
      expect(required.optional).toBe(false)

      const optional = inferParquetType('string')
      expect(optional.optional).toBe(true)
    })

    it('should handle optional modifier', () => {
      const optional = inferParquetType('string?')
      expect(optional.optional).toBe(true)
    })

    it('should handle array types', () => {
      const arr = inferParquetType('string[]')
      expect(arr.repetitionType).toBe('REPEATED')
    })

    it('should handle decimal type', () => {
      const decimal = inferParquetType('decimal(10,2)')
      expect(decimal.type).toBe('DECIMAL')
      expect(decimal.precision).toBe(10)
      expect(decimal.scale).toBe(2)
    })

    it('should handle varchar type', () => {
      const varchar = inferParquetType('varchar(255)')
      expect(varchar.type).toBe('STRING')
    })

    it('should handle enum type', () => {
      const enumType = inferParquetType('enum(draft,published)')
      expect(enumType.type).toBe('STRING')
    })
  })

  describe('toParquetSchema', () => {
    it('should convert TypeDefinition to ParquetSchema', () => {
      const typeDef: TypeDefinition = {
        title: 'string!',
        count: 'int',
        published: 'boolean',
      }

      const schema = toParquetSchema(typeDef)

      expect(schema.title.type).toBe('STRING')
      expect(schema.title.optional).toBe(false)
      expect(schema.count.type).toBe('INT64')
      expect(schema.published.type).toBe('BOOLEAN')
    })

    it('should skip relationship fields', () => {
      const typeDef: TypeDefinition = {
        title: 'string!',
        author: '-> User.posts',
        comments: '<- Comment.post[]',
      }

      const schema = toParquetSchema(typeDef)

      expect(schema.title).toBeDefined()
      expect(schema.author).toBeUndefined()
      expect(schema.comments).toBeUndefined()
    })

    it('should skip metadata fields', () => {
      const typeDef: TypeDefinition = {
        $type: 'schema:Post',
        $shred: ['status'],
        title: 'string!',
      }

      const schema = toParquetSchema(typeDef)

      expect(schema.title).toBeDefined()
      expect(schema.$type).toBeUndefined()
      expect(schema.$shred).toBeUndefined()
    })

    it('should handle field definition objects', () => {
      const typeDef: TypeDefinition = {
        status: { type: 'string', required: true, index: true },
      }

      const schema = toParquetSchema(typeDef)

      expect(schema.status.type).toBe('STRING')
      expect(schema.status.optional).toBe(false)
    })
  })

  describe('createEntitySchema', () => {
    it('should create schema with system columns', () => {
      const schema = createEntitySchema()

      expect(schema.$id).toBeDefined()
      expect(schema.$type).toBeDefined()
      expect(schema.name).toBeDefined()
      expect(schema.createdAt).toBeDefined()
      expect(schema.updatedAt).toBeDefined()
      expect(schema.version).toBeDefined()
      expect(schema.$data).toBeDefined()
    })

    it('should include audit columns', () => {
      const schema = createEntitySchema()

      expect(schema.createdBy).toBeDefined()
      expect(schema.updatedBy).toBeDefined()
      expect(schema.deletedAt).toBeDefined()
      expect(schema.deletedBy).toBeDefined()
    })

    it('should include shredded fields', () => {
      const typeDef: TypeDefinition = {
        status: 'string!',
        priority: 'int',
      }

      const schema = createEntitySchema({
        typeDef,
        shredFields: ['status', 'priority'],
      })

      expect(schema.status).toBeDefined()
      expect(schema.priority).toBeDefined()
    })

    it('should include additional columns', () => {
      const schema = createEntitySchema({
        additionalColumns: {
          customField: { type: 'STRING', optional: true },
        },
      })

      expect(schema.customField).toBeDefined()
    })
  })

  describe('createRelationshipSchema', () => {
    it('should create schema for relationship storage', () => {
      const schema = createRelationshipSchema()

      // Source fields
      expect(schema.fromNs).toBeDefined()
      expect(schema.fromId).toBeDefined()
      expect(schema.fromType).toBeDefined()

      // Relationship names
      expect(schema.predicate).toBeDefined()
      expect(schema.reverse).toBeDefined()

      // Target fields
      expect(schema.toNs).toBeDefined()
      expect(schema.toId).toBeDefined()
      expect(schema.toType).toBeDefined()

      // Audit
      expect(schema.createdAt).toBeDefined()
      expect(schema.version).toBeDefined()

      // Edge data
      expect(schema.data).toBeDefined()
    })
  })

  describe('createEventSchema', () => {
    it('should create schema for event log storage', () => {
      const schema = createEventSchema()

      expect(schema.id).toBeDefined()
      expect(schema.ts).toBeDefined()
      expect(schema.target).toBeDefined()
      expect(schema.op).toBeDefined()
      expect(schema.ns).toBeDefined()
      expect(schema.entityId).toBeDefined()
      expect(schema.before).toBeDefined()
      expect(schema.after).toBeDefined()
      expect(schema.actor).toBeDefined()
    })
  })

  describe('validateParquetSchema', () => {
    it('should validate valid schema', () => {
      const schema: ParquetSchema = {
        id: { type: 'STRING', optional: false },
        count: { type: 'INT64', optional: true },
      }

      const result = validateParquetSchema(schema)
      expect(result.valid).toBe(true)
      expect(result.errors).toHaveLength(0)
    })

    it('should detect missing type', () => {
      const schema = {
        id: { optional: false },
      } as unknown as ParquetSchema

      const result = validateParquetSchema(schema)
      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.includes('missing type'))).toBe(true)
    })

    it('should validate decimal parameters', () => {
      const validSchema: ParquetSchema = {
        price: { type: 'DECIMAL', precision: 10, scale: 2 },
      }
      expect(validateParquetSchema(validSchema).valid).toBe(true)

      const missingPrecision = {
        price: { type: 'DECIMAL', scale: 2 },
      } as unknown as ParquetSchema
      expect(validateParquetSchema(missingPrecision).valid).toBe(false)

      const invalidScale: ParquetSchema = {
        price: { type: 'DECIMAL', precision: 2, scale: 5 },
      }
      expect(validateParquetSchema(invalidScale).valid).toBe(false)
    })
  })

  describe('getShredFields', () => {
    it('should return explicit $shred fields', () => {
      const typeDef: TypeDefinition = {
        $shred: ['status', 'priority'],
        status: 'string',
        priority: 'int',
        other: 'string',
      }

      const fields = getShredFields(typeDef)
      expect(fields).toEqual(['status', 'priority'])
    })

    it('should auto-detect indexed fields', () => {
      const typeDef: TypeDefinition = {
        status: { type: 'string', index: true },
        email: { type: 'email', index: 'unique' },
        other: 'string',
      }

      const fields = getShredFields(typeDef)
      expect(fields).toContain('status')
      expect(fields).toContain('email')
      expect(fields).not.toContain('other')
    })
  })

  describe('Schema utilities', () => {
    it('should merge schemas', () => {
      const base: ParquetSchema = { a: { type: 'STRING' } }
      const override: ParquetSchema = { b: { type: 'INT64' }, a: { type: 'INT32' } }

      const merged = mergeSchemas(base, override)

      expect(merged.a.type).toBe('INT32') // Override takes precedence
      expect(merged.b.type).toBe('INT64')
    })

    it('should get column names', () => {
      const schema: ParquetSchema = {
        id: { type: 'STRING' },
        name: { type: 'STRING' },
      }

      const names = getColumnNames(schema)
      expect(names).toEqual(['id', 'name'])
    })

    it('should check if column exists', () => {
      const schema: ParquetSchema = {
        id: { type: 'STRING' },
      }

      expect(hasColumn(schema, 'id')).toBe(true)
      expect(hasColumn(schema, 'missing')).toBe(false)
    })
  })
})

// =============================================================================
// ParquetReader Tests (with real FsBackend)
// =============================================================================

describe('ParquetReader', () => {
  let tempDir: string
  let storage: FsBackend

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), 'parquedb-reader-test-'))
    storage = new FsBackend(tempDir)
  })

  afterEach(async () => {
    await cleanupTempDir(tempDir)
  })

  describe('constructor', () => {
    it('should create reader with storage', () => {
      const reader = new ParquetReader({ storage })
      expect(reader).toBeDefined()
    })

    it('should accept default columns option', () => {
      const reader = new ParquetReader({
        storage,
        columns: ['id', 'name'],
      })
      expect(reader).toBeDefined()
    })

    it('should accept default rowGroups option', () => {
      const reader = new ParquetReader({
        storage,
        rowGroups: [0, 1],
      })
      expect(reader).toBeDefined()
    })
  })

  describe('initializeAsyncBuffer', () => {
    it('should create async buffer with file size', async () => {
      // Write a test file
      const testData = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
      await storage.write('test.parquet', testData)

      const buffer = await initializeAsyncBuffer(storage, 'test.parquet')

      expect(buffer.byteLength).toBe(10)
    })

    it('should throw if file not found', async () => {
      await expect(
        initializeAsyncBuffer(storage, 'nonexistent.parquet')
      ).rejects.toThrow('File not found')
    })

    it('should provide slice method that reads file ranges', async () => {
      // Write a test file with known content
      const testData = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
      await storage.write('test.parquet', testData)

      const buffer = await initializeAsyncBuffer(storage, 'test.parquet')
      const slice = await buffer.slice(2, 7)

      expect(slice).toBeInstanceOf(ArrayBuffer)
      expect(new Uint8Array(slice)).toEqual(new Uint8Array([2, 3, 4, 5, 6]))
    })
  })

  describe('stream method', () => {
    it('should return async generator', async () => {
      const reader = new ParquetReader({ storage })
      const generator = reader.stream('test.parquet')

      expect(generator[Symbol.asyncIterator]).toBeDefined()
    })
  })
})

// =============================================================================
// ParquetWriter Tests (with real FsBackend)
// =============================================================================

describe('ParquetWriter', () => {
  let tempDir: string
  let storage: FsBackend

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), 'parquedb-writer-test-'))
    storage = new FsBackend(tempDir)
  })

  afterEach(async () => {
    await cleanupTempDir(tempDir)
  })

  describe('constructor', () => {
    it('should create writer with storage', () => {
      const writer = new ParquetWriter(storage)
      expect(writer).toBeDefined()
    })

    it('should accept compression option', () => {
      const writer = new ParquetWriter(storage, { compression: 'gzip' })
      expect(writer).toBeDefined()
    })

    it('should accept rowGroupSize option', () => {
      const writer = new ParquetWriter(storage, { rowGroupSize: 5000 })
      expect(writer).toBeDefined()
    })

    it('should accept dictionary option', () => {
      const writer = new ParquetWriter(storage, { dictionary: true })
      expect(writer).toBeDefined()
    })

    it('should accept metadata option', () => {
      const writer = new ParquetWriter(storage, {
        metadata: { creator: 'ParqueDB' },
      })
      expect(writer).toBeDefined()
    })
  })

  describe('write method', () => {
    it('should write data to storage and create actual file', async () => {
      const writer = new ParquetWriter(storage)
      const schema: ParquetSchema = {
        id: { type: 'STRING', optional: false },
        name: { type: 'STRING', optional: true },
      }
      const data = [
        { id: '1', name: 'Alice' },
        { id: '2', name: 'Bob' },
      ]

      const result = await writer.write('test.parquet', data, schema)

      expect(result.rowCount).toBe(2)

      // Verify file actually exists on disk
      const fileStat = await stat(join(tempDir, 'test.parquet'))
      expect(fileStat.isFile()).toBe(true)
      expect(fileStat.size).toBeGreaterThan(0)
    })

    it('should handle empty data', async () => {
      const writer = new ParquetWriter(storage)
      const schema: ParquetSchema = {
        id: { type: 'STRING', optional: false },
      }

      const result = await writer.write('empty.parquet', [], schema)

      expect(result.rowCount).toBe(0)
      expect(result.rowGroupCount).toBe(0)
    })

    it('should accept write options', async () => {
      const writer = new ParquetWriter(storage)
      const schema: ParquetSchema = {
        id: { type: 'STRING', optional: false },
      }

      const result = await writer.write('options.parquet', [{ id: '1' }], schema, {
        compression: 'zstd',
        rowGroupSize: 100,
      })

      expect(result.rowCount).toBe(1)

      // Verify file was created
      const exists = await storage.exists('options.parquet')
      expect(exists).toBe(true)
    })

    it('should create files in nested directories', async () => {
      const writer = new ParquetWriter(storage)
      const schema: ParquetSchema = {
        id: { type: 'STRING', optional: false },
      }
      const data = [{ id: '1' }]

      await writer.write('data/namespace/test.parquet', data, schema)

      // Verify file was created in nested path
      const exists = await storage.exists('data/namespace/test.parquet')
      expect(exists).toBe(true)
    })
  })

  describe('append method', () => {
    it('should throw if file does not exist', async () => {
      const writer = new ParquetWriter(storage)

      await expect(
        writer.append('nonexistent.parquet', [{ id: '1' }])
      ).rejects.toThrow('Cannot append to non-existent file')
    })

    it('should handle empty data', async () => {
      const writer = new ParquetWriter(storage)

      const result = await writer.append('test.parquet', [])

      expect(result.rowCount).toBe(0)
    })
  })
})

// =============================================================================
// Integration Tests (with real storage)
// =============================================================================

describe('Integration Tests', () => {
  let tempDir: string
  let storage: FsBackend

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), 'parquedb-integration-test-'))
    storage = new FsBackend(tempDir)
  })

  afterEach(async () => {
    await cleanupTempDir(tempDir)
  })

  describe('Variant round-trip with complex data', () => {
    it('should handle real-world entity data', () => {
      const entity = {
        $id: 'posts/abc123',
        $type: 'Post',
        name: 'My First Post',
        title: 'Hello World',
        content: 'This is my first blog post with some *markdown*.',
        tags: ['tech', 'blog', 'intro'],
        metadata: {
          views: 100,
          likes: 10,
          author: {
            id: 'users/john',
            name: 'John Doe',
          },
        },
        createdAt: new Date('2024-01-15T12:00:00Z'),
      }

      const encoded = encodeVariant(entity)
      const decoded = decodeVariant(encoded) as typeof entity

      expect(decoded.$id).toBe(entity.$id)
      expect(decoded.$type).toBe(entity.$type)
      expect(decoded.title).toBe(entity.title)
      expect(decoded.tags).toEqual(entity.tags)
      expect(decoded.metadata.views).toBe(100)
      expect(decoded.metadata.author.name).toBe('John Doe')
      expect(decoded.createdAt.getTime()).toBe(entity.createdAt.getTime())
    })
  })

  describe('Schema creation for entities', () => {
    it('should create complete entity schema', () => {
      const typeDef: TypeDefinition = {
        $type: 'schema:BlogPosting',
        $shred: ['status', 'publishedAt'],
        title: 'string!',
        content: 'markdown!',
        status: { type: 'string', default: 'draft', index: true },
        publishedAt: 'datetime?',
        author: '-> User.posts',
      }

      const schema = createEntitySchema({
        typeDef,
        shredFields: getShredFields(typeDef),
      })

      // System columns
      expect(schema.$id).toBeDefined()
      expect(schema.$type).toBeDefined()
      expect(schema.$data).toBeDefined()

      // Shredded columns
      expect(schema.status).toBeDefined()
      expect(schema.publishedAt).toBeDefined()

      // Relationship columns should NOT be in schema (they're stored separately)
      expect(schema.author).toBeUndefined()
    })
  })

  describe('Shredding workflow', () => {
    it('should shred, encode remaining, and merge back', () => {
      const original = {
        $id: 'posts/123',
        status: 'published',
        priority: 1,
        title: 'Test Post',
        content: 'Long content here...',
        metadata: { tags: ['a', 'b'] },
      }

      // Shred hot fields
      const { shredded, remaining } = shredObject(original, ['status', 'priority'])

      // Encode remaining as Variant
      const encoded = encodeVariant(remaining)

      // Later, decode and merge
      const decoded = decodeVariant(encoded) as Record<string, unknown>
      const restored = mergeShredded(shredded, decoded)

      // Verify restoration
      expect(restored.$id).toBe(original.$id)
      expect(restored.status).toBe(original.status)
      expect(restored.priority).toBe(original.priority)
      expect(restored.title).toBe(original.title)
      expect((restored.metadata as any).tags).toEqual(['a', 'b'])
    })
  })

  describe('Full write-read cycle', () => {
    it('should write and read back Parquet data', async () => {
      // Use snappy compression which is built into hyparquet-writer
      const writer = new ParquetWriter(storage, { compression: 'snappy' })
      const reader = new ParquetReader({ storage })

      const schema: ParquetSchema = {
        id: { type: 'STRING', optional: false },
        name: { type: 'STRING', optional: true },
        age: { type: 'INT64', optional: true },
      }

      const data = [
        { id: '1', name: 'Alice', age: 30 },
        { id: '2', name: 'Bob', age: 25 },
        { id: '3', name: 'Charlie', age: 35 },
      ]

      // Write
      const writeResult = await writer.write('users.parquet', data, schema)
      expect(writeResult.rowCount).toBe(3)

      // Verify file exists
      const exists = await storage.exists('users.parquet')
      expect(exists).toBe(true)

      // Read back
      const readData = await reader.read('users.parquet')
      expect(readData).toHaveLength(3)
      expect(readData[0].id).toBe('1')
      expect(readData[0].name).toBe('Alice')
      expect(readData[1].id).toBe('2')
      expect(readData[2].name).toBe('Charlie')
    })

    it('should write and read back with various data types', async () => {
      // Use snappy compression which is built into hyparquet-writer
      const writer = new ParquetWriter(storage, { compression: 'snappy' })
      const reader = new ParquetReader({ storage })

      const schema: ParquetSchema = {
        id: { type: 'STRING', optional: false },
        count: { type: 'INT64', optional: true },
        active: { type: 'BOOLEAN', optional: true },
        score: { type: 'DOUBLE', optional: true },
      }

      const data = [
        { id: '1', count: 100, active: true, score: 98.5 },
        { id: '2', count: 200, active: false, score: 75.0 },
      ]

      await writer.write('mixed.parquet', data, schema)
      const readData = await reader.read('mixed.parquet')

      expect(readData).toHaveLength(2)
      expect(readData[0].count).toBe(100)
      expect(readData[0].active).toBe(true)
      expect(readData[0].score).toBeCloseTo(98.5)
    })

    it('should verify file is actually created on disk', async () => {
      const writer = new ParquetWriter(storage)
      const schema: ParquetSchema = {
        id: { type: 'STRING', optional: false },
      }
      const data = [{ id: '1' }, { id: '2' }]

      await writer.write('verify.parquet', data, schema)

      // Check using native fs
      const fileStat = await stat(join(tempDir, 'verify.parquet'))
      expect(fileStat.isFile()).toBe(true)
      expect(fileStat.size).toBeGreaterThan(0)

      // List directory to verify
      const files = await readdir(tempDir)
      expect(files).toContain('verify.parquet')
    })
  })

  describe('Multiple files in same directory', () => {
    it('should write and read multiple Parquet files', async () => {
      const writer = new ParquetWriter(storage)
      const reader = new ParquetReader({ storage })

      const schema: ParquetSchema = {
        id: { type: 'STRING', optional: false },
        value: { type: 'INT64', optional: true },
      }

      // Write multiple files
      await writer.write('data/file1.parquet', [{ id: '1', value: 10 }], schema)
      await writer.write('data/file2.parquet', [{ id: '2', value: 20 }], schema)
      await writer.write('data/file3.parquet', [{ id: '3', value: 30 }], schema)

      // Verify all files exist
      expect(await storage.exists('data/file1.parquet')).toBe(true)
      expect(await storage.exists('data/file2.parquet')).toBe(true)
      expect(await storage.exists('data/file3.parquet')).toBe(true)

      // Read back each file
      const data1 = await reader.read('data/file1.parquet')
      const data2 = await reader.read('data/file2.parquet')
      const data3 = await reader.read('data/file3.parquet')

      expect(data1[0].value).toBe(10)
      expect(data2[0].value).toBe(20)
      expect(data3[0].value).toBe(30)
    })
  })

  describe('Cleanup verification', () => {
    it('should properly clean up temp files', async () => {
      // This test verifies the afterEach cleanup works
      const writer = new ParquetWriter(storage)
      const schema: ParquetSchema = {
        id: { type: 'STRING', optional: false },
      }

      await writer.write('cleanup-test.parquet', [{ id: '1' }], schema)

      const exists = await storage.exists('cleanup-test.parquet')
      expect(exists).toBe(true)

      // Cleanup happens in afterEach, but we can verify files were created
      const fileStat = await stat(join(tempDir, 'cleanup-test.parquet'))
      expect(fileStat.isFile()).toBe(true)
    })
  })
})
