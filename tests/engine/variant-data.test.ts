/**
 * VARIANT $data Column Tests
 *
 * Tests for the VARIANT-encoded $data column in the MergeTree engine.
 * The $data column stores all non-system entity fields as a Parquet VARIANT,
 * replacing the previous JSON string blob approach. This enables:
 * - Automatic columnar compression of semi-structured data
 * - Future predicate pushdown via shredded fields
 * - Native decoding by analytics tools (no opaque string)
 *
 * Tests follow Red-Green-Refactor: these are written first, then implementation.
 */

import { describe, it, expect } from 'vitest'
import {
  encodeDataToParquet,
} from '@/engine/parquet-encoders'
import { parseDataField } from '@/engine/parquet-data-utils'
import { makeLine, decodeParquet, toNumber } from './helpers'

// =============================================================================
// Helpers
// =============================================================================

/** Verify a buffer starts with PAR1 magic bytes */
function expectParquetMagic(buffer: ArrayBuffer): void {
  const view = new Uint8Array(buffer)
  expect(view[0]).toBe(0x50) // P
  expect(view[1]).toBe(0x41) // A
  expect(view[2]).toBe(0x52) // R
  expect(view[3]).toBe(0x31) // 1
}

/**
 * Extract the user data fields from a decoded Parquet row.
 * Handles VARIANT binary, JSON object, and legacy JSON string formats.
 * Uses the production parseDataField for consistency.
 */
function extractDataFields(row: Record<string, unknown>): Record<string, unknown> {
  return parseDataField(row.$data)
}

// =============================================================================
// 1. Roundtrip Test
// =============================================================================

describe('VARIANT $data: roundtrip encoding/decoding', () => {
  it('should encode entities and decode back with all fields preserved', async () => {
    const data = [
      makeLine({ $id: 'u1', $ts: 1000, name: 'Alice', age: 25, email: 'alice@example.com' }),
      makeLine({ $id: 'u2', $ts: 2000, name: 'Bob', age: 30, email: 'bob@example.com' }),
    ]

    const buffer = await encodeDataToParquet(data)
    expectParquetMagic(buffer)

    const rows = await decodeParquet(buffer)
    expect(rows).toHaveLength(2)

    // Rows are sorted by $id
    expect(rows[0].$id).toBe('u1')
    expect(rows[1].$id).toBe('u2')

    // System fields preserved
    expect(rows[0].$op).toBe('c')
    expect(rows[0].$v).toBe(1)
    expect(toNumber(rows[0].$ts)).toBe(1000)

    // Data fields preserved via $data VARIANT
    const u1Data = extractDataFields(rows[0])
    expect(u1Data.name).toBe('Alice')
    expect(u1Data.age).toBe(25)
    expect(u1Data.email).toBe('alice@example.com')

    const u2Data = extractDataFields(rows[1])
    expect(u2Data.name).toBe('Bob')
    expect(u2Data.age).toBe(30)
    expect(u2Data.email).toBe('bob@example.com')
  })

  it('should produce valid Parquet with PAR1 magic bytes', async () => {
    const data = [makeLine({ $id: 'u1', $ts: 1000, name: 'Test' })]
    const buffer = await encodeDataToParquet(data)

    expect(buffer).toBeInstanceOf(ArrayBuffer)
    expect(buffer.byteLength).toBeGreaterThan(0)
    expectParquetMagic(buffer)
  })

  it('should exclude system fields ($id, $op, $v, $ts) from $data', async () => {
    const data = [
      makeLine({
        $id: 'u1',
        $ts: 1000,
        customField: 'yes',
        anotherField: 42,
      }),
    ]

    const buffer = await encodeDataToParquet(data)
    const rows = await decodeParquet(buffer)
    const dataFields = extractDataFields(rows[0])

    // System fields should NOT appear in $data
    expect(dataFields.$id).toBeUndefined()
    expect(dataFields.$op).toBeUndefined()
    expect(dataFields.$v).toBeUndefined()
    expect(dataFields.$ts).toBeUndefined()

    // Custom fields should appear in $data
    expect(dataFields.customField).toBe('yes')
    expect(dataFields.anotherField).toBe(42)
  })
})

// =============================================================================
// 2. $data is VARIANT (not string)
// =============================================================================

describe('VARIANT $data: column type verification', () => {
  it('should store $data as a VARIANT object (not a JSON string)', async () => {
    const data = [makeLine({ $id: 'u1', $ts: 1000, name: 'Alice' })]
    const buffer = await encodeDataToParquet(data)
    const rows = await decodeParquet(buffer)

    // hyparquet reads VARIANT columns as a struct with metadata and value
    // sub-columns (both Uint8Array). The $data field should be an object
    // with these binary sub-columns, NOT a JSON string.
    expect(typeof rows[0].$data).toBe('object')
    expect(rows[0].$data).not.toBeNull()

    const rawVariant = rows[0].$data as Record<string, unknown>
    // VARIANT struct has metadata (dictionary) and value (encoded data)
    expect(rawVariant.metadata).toBeInstanceOf(Uint8Array)
    expect(rawVariant.value).toBeInstanceOf(Uint8Array)

    // When decoded through parseDataField, it should yield the original data
    const decoded = extractDataFields(rows[0])
    expect(decoded.name).toBe('Alice')
  })
})

// =============================================================================
// 3. Nested Data Test
// =============================================================================

describe('VARIANT $data: nested data', () => {
  it('should preserve nested objects through VARIANT encoding', async () => {
    const data = [
      makeLine({
        $id: 'u1',
        $ts: 1000,
        name: 'Alice',
        address: { street: '123 Main St', city: 'Springfield', zip: '62701' },
        metadata: { role: 'admin', permissions: { read: true, write: true } },
      }),
    ]

    const buffer = await encodeDataToParquet(data)
    const rows = await decodeParquet(buffer)
    const dataFields = extractDataFields(rows[0])

    expect(dataFields.name).toBe('Alice')
    expect(dataFields.address).toEqual({ street: '123 Main St', city: 'Springfield', zip: '62701' })
    expect(dataFields.metadata).toEqual({ role: 'admin', permissions: { read: true, write: true } })
  })

  it('should preserve arrays through VARIANT encoding', async () => {
    const data = [
      makeLine({
        $id: 'u1',
        $ts: 1000,
        tags: ['admin', 'user', 'moderator'],
        scores: [85, 90, 78, 92],
      }),
    ]

    const buffer = await encodeDataToParquet(data)
    const rows = await decodeParquet(buffer)
    const dataFields = extractDataFields(rows[0])

    expect(dataFields.tags).toEqual(['admin', 'user', 'moderator'])
    expect(dataFields.scores).toEqual([85, 90, 78, 92])
  })

  it('should preserve mixed arrays and nested objects', async () => {
    const data = [
      makeLine({
        $id: 'u1',
        $ts: 1000,
        items: [
          { name: 'item1', price: 10.5 },
          { name: 'item2', price: 20.0 },
        ],
        matrix: [[1, 2], [3, 4]],
      }),
    ]

    const buffer = await encodeDataToParquet(data)
    const rows = await decodeParquet(buffer)
    const dataFields = extractDataFields(rows[0])

    expect(dataFields.items).toEqual([
      { name: 'item1', price: 10.5 },
      { name: 'item2', price: 20.0 },
    ])
    expect(dataFields.matrix).toEqual([[1, 2], [3, 4]])
  })
})

// =============================================================================
// 4. Empty Data Test
// =============================================================================

describe('VARIANT $data: empty data', () => {
  it('should handle entity with no user fields (just system fields)', async () => {
    const data = [makeLine({ $id: 'u1', $ts: 1000 })]

    const buffer = await encodeDataToParquet(data)
    const rows = await decodeParquet(buffer)

    expect(rows).toHaveLength(1)
    expect(rows[0].$id).toBe('u1')

    const dataFields = extractDataFields(rows[0])
    expect(Object.keys(dataFields)).toHaveLength(0)
  })

  it('should handle empty input array', async () => {
    const buffer = await encodeDataToParquet([])
    expectParquetMagic(buffer)

    const rows = await decodeParquet(buffer)
    expect(rows).toHaveLength(0)
  })
})

// =============================================================================
// 5. Mixed Types Test
// =============================================================================

describe('VARIANT $data: mixed types', () => {
  it('should preserve strings, numbers, booleans, nulls, arrays, objects', async () => {
    const data = [
      makeLine({
        $id: 'u1',
        $ts: 1000,
        stringField: 'hello world',
        intField: 42,
        floatField: 3.14,
        boolTrue: true,
        boolFalse: false,
        nullField: null,
        arrayField: [1, 'two', true, null],
        objectField: { nested: 'value', count: 99 },
      }),
    ]

    const buffer = await encodeDataToParquet(data)
    const rows = await decodeParquet(buffer)
    const dataFields = extractDataFields(rows[0])

    expect(dataFields.stringField).toBe('hello world')
    expect(dataFields.intField).toBe(42)
    expect(dataFields.floatField).toBeCloseTo(3.14)
    expect(dataFields.boolTrue).toBe(true)
    expect(dataFields.boolFalse).toBe(false)
    expect(dataFields.nullField).toBeNull()
    expect(dataFields.arrayField).toEqual([1, 'two', true, null])
    expect(dataFields.objectField).toEqual({ nested: 'value', count: 99 })
  })

  it('should preserve zero values correctly', async () => {
    const data = [
      makeLine({
        $id: 'u1',
        $ts: 1000,
        zero: 0,
        emptyString: '',
        emptyArray: [],
        emptyObject: {},
      }),
    ]

    const buffer = await encodeDataToParquet(data)
    const rows = await decodeParquet(buffer)
    const dataFields = extractDataFields(rows[0])

    expect(dataFields.zero).toBe(0)
    expect(dataFields.emptyString).toBe('')
    expect(dataFields.emptyArray).toEqual([])
    expect(dataFields.emptyObject).toEqual({})
  })
})

// =============================================================================
// 6. Delete Operation Test
// =============================================================================

describe('VARIANT $data: delete operations', () => {
  it('should handle delete markers with empty VARIANT data', async () => {
    const data = [
      makeLine({ $id: 'u1', $op: 'd', $v: 2, $ts: 3000 }),
    ]

    const buffer = await encodeDataToParquet(data)
    const rows = await decodeParquet(buffer)

    expect(rows).toHaveLength(1)
    expect(rows[0].$id).toBe('u1')
    expect(rows[0].$op).toBe('d')
    expect(toNumber(rows[0].$v)).toBe(2)

    const dataFields = extractDataFields(rows[0])
    expect(Object.keys(dataFields)).toHaveLength(0)
  })

  it('should handle mix of creates, updates, and deletes', async () => {
    const data = [
      makeLine({ $id: 'u1', $ts: 1000, name: 'Alice', age: 25 }),
      makeLine({ $id: 'u2', $op: 'u', $v: 2, $ts: 2000, name: 'Bob Updated' }),
      makeLine({ $id: 'u3', $op: 'd', $v: 3, $ts: 3000 }),
    ]

    const buffer = await encodeDataToParquet(data)
    const rows = await decodeParquet(buffer)

    expect(rows).toHaveLength(3)

    // Create
    const u1Data = extractDataFields(rows[0])
    expect(u1Data.name).toBe('Alice')
    expect(u1Data.age).toBe(25)

    // Update
    const u2Data = extractDataFields(rows[1])
    expect(u2Data.name).toBe('Bob Updated')

    // Delete (empty data)
    const u3Data = extractDataFields(rows[2])
    expect(Object.keys(u3Data)).toHaveLength(0)
  })
})

// =============================================================================
// 7. Backward Compatibility Test
// =============================================================================

describe('VARIANT $data: backward compatibility', () => {
  it('should handle old JSON string $data format in the reader', async () => {
    // This test verifies that the read path can handle BOTH:
    // 1. New VARIANT $data (object) -- from new encoder
    // 2. Old JSON string $data -- from legacy files
    // We test the reader logic by importing it directly

    const { parseDataField } = await import('@/engine/parquet-data-utils')

    // New format: object
    const objResult = parseDataField({ name: 'Alice', age: 25 })
    expect(objResult).toEqual({ name: 'Alice', age: 25 })

    // Old format: JSON string
    const strResult = parseDataField('{"name":"Bob","age":30}')
    expect(strResult).toEqual({ name: 'Bob', age: 30 })

    // Null/empty
    const nullResult = parseDataField(null)
    expect(nullResult).toEqual({})

    const undefinedResult = parseDataField(undefined)
    expect(undefinedResult).toEqual({})

    const emptyResult = parseDataField('')
    expect(emptyResult).toEqual({})
  })
})

// =============================================================================
// 8. Timestamp Precision
// =============================================================================

describe('VARIANT $data: timestamp precision', () => {
  it('should preserve large timestamps as DOUBLE', async () => {
    const ts = 1738857600000 // Large timestamp
    const data = [makeLine({ $id: 'u1', $ts: ts })]

    const buffer = await encodeDataToParquet(data)
    const rows = await decodeParquet(buffer)

    expect(toNumber(rows[0].$ts)).toBe(ts)
  })

  it('should NOT introduce BigInt anywhere', async () => {
    const data = [
      makeLine({ $id: 'u1', $ts: 1738857600000, count: 42 }),
    ]

    const buffer = await encodeDataToParquet(data)
    const rows = await decodeParquet(buffer)

    // $ts should be number, not bigint
    expect(typeof rows[0].$ts).not.toBe('bigint')
    // $v should be number, not bigint
    expect(typeof rows[0].$v).not.toBe('bigint')
  })
})

// =============================================================================
// 9. Integration with ParquetStorageAdapter
// =============================================================================

describe('VARIANT $data: ParquetStorageAdapter integration', () => {
  it('should roundtrip entities through writeData + readData', async () => {
    const { ParquetStorageAdapter } = await import('@/engine/parquet-adapter')
    const { mkdtemp, rm } = await import('node:fs/promises')
    const { join } = await import('node:path')
    const { tmpdir } = await import('node:os')

    const tmpDir = await mkdtemp(join(tmpdir(), 'parquedb-variant-'))
    const adapter = new ParquetStorageAdapter()
    const filePath = join(tmpDir, 'test-data.parquet')

    const entities = [
      makeLine({ $id: 'u1', $ts: 1000, name: 'Alice', nested: { a: 1 }, tags: ['x'] }),
      makeLine({ $id: 'u2', $ts: 2000, name: 'Bob', count: 42 }),
      makeLine({ $id: 'u3', $op: 'd', $v: 2, $ts: 3000 }),
    ]

    try {
      await adapter.writeData(filePath, entities)
      const result = await adapter.readData(filePath)

      expect(result).toHaveLength(3)

      // Sorted by $id in encoder
      expect(result[0].$id).toBe('u1')
      expect(result[0].name).toBe('Alice')
      expect(result[0].nested).toEqual({ a: 1 })
      expect(result[0].tags).toEqual(['x'])

      expect(result[1].$id).toBe('u2')
      expect(result[1].name).toBe('Bob')
      expect(result[1].count).toBe(42)

      expect(result[2].$id).toBe('u3')
      expect(result[2].$op).toBe('d')
    } finally {
      await rm(tmpDir, { recursive: true, force: true })
    }
  })
})
