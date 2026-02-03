/**
 * Variant Shredding Tests
 *
 * Tests for Parquet VARIANT shredding functionality as implemented in hyparquet-writer.
 * Shredding extracts specific fields from Variant values into typed Parquet columns,
 * enabling predicate pushdown and statistics-based row group skipping.
 *
 * @see /docs/architecture/variant-shredding.md
 * @see https://parquet.apache.org/docs/file-format/types/variantshredding/
 */

import { describe, it, expect } from 'vitest'
import {
  createShreddedVariantColumn,
  getStatisticsPaths,
  mapFilterPathToStats,
  encodeVariant,
} from 'hyparquet-writer'
import {
  determineShredFields,
  DEFAULT_SHRED_CONFIG,
} from '@/types/integrations'
import type { TypeDefinition } from '@/types/schema'
import {
  ShreddedPushdownContext,
  buildShreddingProperties,
  extractShreddedFilterPaths,
  hasShreddedConditions,
  estimatePushdownEffectiveness,
} from '@/query/shredded-pushdown'

// =============================================================================
// Test Data
// =============================================================================

const TEST_OBJECTS = [
  { titleType: 'movie', genre: 'action', year: 2020, rating: 8.5, title: 'Action Movie' },
  { titleType: 'movie', genre: 'drama', year: 2021, rating: 9.0, title: 'Drama Movie' },
  { titleType: 'series', genre: 'comedy', year: 2019, rating: 7.5, title: 'Comedy Series' },
  { titleType: 'short', genre: 'horror', year: 2022, rating: 6.0, title: 'Horror Short' },
  { titleType: 'movie', genre: 'action', year: 2023, rating: 8.0, title: 'Action Movie 2' },
]

const SHRED_FIELDS = ['titleType', 'genre', 'year']

// =============================================================================
// createShreddedVariantColumn Tests
// =============================================================================

describe('createShreddedVariantColumn', () => {
  describe('schema generation', () => {
    it('should create correct schema structure for shredded variant', () => {
      const { schema } = createShreddedVariantColumn('$index', TEST_OBJECTS, SHRED_FIELDS)

      // Root VARIANT group
      expect(schema[0]).toMatchObject({
        name: '$index',
        repetition_type: 'OPTIONAL',
        num_children: 3,
      })
      expect(schema[0].logical_type).toEqual({ type: 'VARIANT' })

      // metadata (required binary)
      expect(schema[1]).toMatchObject({
        name: 'metadata',
        type: 'BYTE_ARRAY',
        repetition_type: 'REQUIRED',
      })

      // value (optional binary)
      expect(schema[2]).toMatchObject({
        name: 'value',
        type: 'BYTE_ARRAY',
        repetition_type: 'OPTIONAL',
      })

      // typed_value group
      expect(schema[3]).toMatchObject({
        name: 'typed_value',
        repetition_type: 'OPTIONAL',
        num_children: SHRED_FIELDS.length,
      })
    })

    it('should create field groups with value and typed_value subcolumns', () => {
      const { schema } = createShreddedVariantColumn('$index', TEST_OBJECTS, SHRED_FIELDS)

      // After root (1) + metadata (1) + value (1) + typed_value group (1) = index 4
      // Each field has: field group (1) + value (1) + typed_value (1) = 3 elements

      // titleType field group (index 4)
      expect(schema[4]).toMatchObject({
        name: 'titleType',
        repetition_type: 'OPTIONAL',
        num_children: 2,
      })

      // titleType.value (index 5)
      expect(schema[5]).toMatchObject({
        name: 'value',
        type: 'BYTE_ARRAY',
        repetition_type: 'OPTIONAL',
      })

      // titleType.typed_value (index 6)
      expect(schema[6]).toMatchObject({
        name: 'typed_value',
        repetition_type: 'OPTIONAL',
      })
    })

    it('should handle required (non-nullable) variant column', () => {
      const { schema } = createShreddedVariantColumn(
        '$index',
        TEST_OBJECTS,
        SHRED_FIELDS,
        { nullable: false }
      )

      expect(schema[0].repetition_type).toBe('REQUIRED')
    })
  })

  describe('type detection', () => {
    it('should detect string type for string fields', () => {
      const { schema } = createShreddedVariantColumn(
        '$index',
        [{ name: 'test' }],
        ['name']
      )

      // Find the typed_value column for 'name'
      const typedValue = schema.find(
        (s) => s.name === 'typed_value' && s.type === 'BYTE_ARRAY'
      )
      expect(typedValue).toBeDefined()
      expect(typedValue?.converted_type).toBe('UTF8')
    })

    it('should detect INT32 type for small integers', () => {
      const { schema } = createShreddedVariantColumn(
        '$index',
        [{ count: 42 }],
        ['count']
      )

      const typedValue = schema.find(
        (s) => s.name === 'typed_value' && s.type === 'INT32'
      )
      expect(typedValue).toBeDefined()
    })

    it('should detect INT64 type for large integers', () => {
      const { schema } = createShreddedVariantColumn(
        '$index',
        [{ bigNum: 9007199254740993 }], // > MAX_SAFE_INTEGER
        ['bigNum']
      )

      const typedValue = schema.find(
        (s) => s.name === 'typed_value' && s.type === 'INT64'
      )
      expect(typedValue).toBeDefined()
    })

    it('should detect DOUBLE type for floating point numbers', () => {
      const { schema } = createShreddedVariantColumn(
        '$index',
        [{ rating: 8.5 }],
        ['rating']
      )

      const typedValue = schema.find(
        (s) => s.name === 'typed_value' && s.type === 'DOUBLE'
      )
      expect(typedValue).toBeDefined()
    })

    it('should detect BOOLEAN type for boolean values', () => {
      const { schema } = createShreddedVariantColumn(
        '$index',
        [{ active: true }],
        ['active']
      )

      const typedValue = schema.find(
        (s) => s.name === 'typed_value' && s.type === 'BOOLEAN'
      )
      expect(typedValue).toBeDefined()
    })

    it('should detect TIMESTAMP type for Date values', () => {
      const { schema } = createShreddedVariantColumn(
        '$index',
        [{ createdAt: new Date() }],
        ['createdAt']
      )

      const typedValue = schema.find(
        (s) => s.name === 'typed_value' && s.type === 'INT64'
      )
      expect(typedValue).toBeDefined()
      expect(typedValue?.converted_type).toBe('TIMESTAMP_MILLIS')
    })

    it('should fall back to UTF8 for mixed types', () => {
      const { schema } = createShreddedVariantColumn(
        '$index',
        [{ mixed: 'string' }, { mixed: 123 }],
        ['mixed']
      )

      const typedValue = schema.find(
        (s) => s.name === 'typed_value' && s.type === 'BYTE_ARRAY'
      )
      expect(typedValue).toBeDefined()
      expect(typedValue?.converted_type).toBe('UTF8')
    })

    it('should use fieldTypes override', () => {
      const { schema } = createShreddedVariantColumn(
        '$index',
        [{ year: 2024 }],
        ['year'],
        { fieldTypes: { year: 'INT64' } }
      )

      const typedValue = schema.find(
        (s) => s.name === 'typed_value' && s.type === 'INT64'
      )
      expect(typedValue).toBeDefined()
    })
  })

  describe('column data extraction', () => {
    it('should extract metadata for each row', () => {
      const { columnData } = createShreddedVariantColumn('$index', TEST_OBJECTS, SHRED_FIELDS)

      const metadata = columnData.get('$index.metadata')
      expect(metadata).toHaveLength(TEST_OBJECTS.length)
      // All should be Uint8Array
      for (const m of metadata!) {
        expect(m).toBeInstanceOf(Uint8Array)
      }
    })

    it('should extract remaining data (non-shredded fields) to value column', () => {
      const { columnData } = createShreddedVariantColumn('$index', TEST_OBJECTS, SHRED_FIELDS)

      const values = columnData.get('$index.value')
      expect(values).toHaveLength(TEST_OBJECTS.length)
      // Each row has 'rating' and 'title' which are not shredded
      for (const v of values!) {
        expect(v).toBeInstanceOf(Uint8Array)
      }
    })

    it('should set value to null when all fields are shredded', () => {
      const objects = [
        { titleType: 'movie', genre: 'action' },
        { titleType: 'series', genre: 'drama' },
      ]
      const { columnData } = createShreddedVariantColumn(
        '$index',
        objects,
        ['titleType', 'genre']
      )

      const values = columnData.get('$index.value')
      expect(values).toHaveLength(2)
      for (const v of values!) {
        expect(v).toBeNull()
      }
    })

    it('should extract shredded field values to typed_value columns', () => {
      const { columnData } = createShreddedVariantColumn('$index', TEST_OBJECTS, SHRED_FIELDS)

      // Check titleType values
      const titleTypes = columnData.get('$index.typed_value.titleType.typed_value')
      expect(titleTypes).toEqual(['movie', 'movie', 'series', 'short', 'movie'])

      // Check year values
      const years = columnData.get('$index.typed_value.year.typed_value')
      expect(years).toEqual([2020, 2021, 2019, 2022, 2023])
    })

    it('should set value subcolumn to null for shredded fields', () => {
      const { columnData } = createShreddedVariantColumn('$index', TEST_OBJECTS, SHRED_FIELDS)

      // The 'value' subcolumn of each shredded field should be null
      for (const field of SHRED_FIELDS) {
        const values = columnData.get(`$index.typed_value.${field}.value`)
        expect(values).toHaveLength(TEST_OBJECTS.length)
        for (const v of values!) {
          expect(v).toBeNull()
        }
      }
    })

    it('should handle null values in objects', () => {
      const objects = [
        { titleType: 'movie', year: 2020 },
        { titleType: null, year: null },
        { titleType: 'series', year: 2021 },
      ]
      const { columnData } = createShreddedVariantColumn('$index', objects, ['titleType', 'year'])

      const titleTypes = columnData.get('$index.typed_value.titleType.typed_value')
      expect(titleTypes).toEqual(['movie', null, 'series'])

      const years = columnData.get('$index.typed_value.year.typed_value')
      expect(years).toEqual([2020, null, 2021])
    })

    it('should handle undefined values in objects', () => {
      const objects = [
        { titleType: 'movie', year: 2020 },
        { titleType: 'series' }, // year is undefined
      ]
      const { columnData } = createShreddedVariantColumn('$index', objects, ['titleType', 'year'])

      const years = columnData.get('$index.typed_value.year.typed_value')
      expect(years).toEqual([2020, null])
    })

    it('should handle null input objects', () => {
      const objects = [
        { titleType: 'movie', year: 2020 },
        null,
        { titleType: 'series', year: 2021 },
      ]
      const { columnData } = createShreddedVariantColumn('$index', objects, ['titleType', 'year'])

      const titleTypes = columnData.get('$index.typed_value.titleType.typed_value')
      expect(titleTypes).toEqual(['movie', null, 'series'])
    })
  })

  describe('shredPaths', () => {
    it('should return correct statistics paths for shredded fields', () => {
      const { shredPaths } = createShreddedVariantColumn('$index', TEST_OBJECTS, SHRED_FIELDS)

      expect(shredPaths).toEqual([
        '$index.typed_value.titleType.typed_value',
        '$index.typed_value.genre.typed_value',
        '$index.typed_value.year.typed_value',
      ])
    })
  })
})

// =============================================================================
// getStatisticsPaths Tests
// =============================================================================

describe('getStatisticsPaths', () => {
  it('should return paths for all shredded fields', () => {
    const paths = getStatisticsPaths('$index', ['titleType', 'year', 'genre'])

    expect(paths).toEqual([
      '$index.typed_value.titleType.typed_value',
      '$index.typed_value.year.typed_value',
      '$index.typed_value.genre.typed_value',
    ])
  })

  it('should handle empty shred fields', () => {
    const paths = getStatisticsPaths('$data', [])
    expect(paths).toEqual([])
  })

  it('should handle single shred field', () => {
    const paths = getStatisticsPaths('$index', ['status'])
    expect(paths).toEqual(['$index.typed_value.status.typed_value'])
  })

  it('should work with non-$ column names', () => {
    const paths = getStatisticsPaths('metadata', ['type', 'version'])
    expect(paths).toEqual([
      'metadata.typed_value.type.typed_value',
      'metadata.typed_value.version.typed_value',
    ])
  })
})

// =============================================================================
// mapFilterPathToStats Tests
// =============================================================================

describe('mapFilterPathToStats', () => {
  it('should map shredded field filter to statistics path', () => {
    const result = mapFilterPathToStats('$index.titleType', '$index', ['titleType', 'year'])
    expect(result).toBe('$index.typed_value.titleType.typed_value')
  })

  it('should return null for non-shredded field', () => {
    const result = mapFilterPathToStats('$index.rating', '$index', ['titleType', 'year'])
    expect(result).toBeNull()
  })

  it('should return null for wrong column prefix', () => {
    const result = mapFilterPathToStats('$data.titleType', '$index', ['titleType', 'year'])
    expect(result).toBeNull()
  })

  it('should return null for filter without dot notation', () => {
    const result = mapFilterPathToStats('titleType', '$index', ['titleType'])
    expect(result).toBeNull()
  })

  it('should handle nested filter paths (first level only)', () => {
    // Filter path '$index.titleType.subfield' should map using 'titleType' as the field
    const result = mapFilterPathToStats('$index.titleType.subfield', '$index', ['titleType'])
    expect(result).toBe('$index.typed_value.titleType.typed_value')
  })

  it('should work with various column names', () => {
    const result = mapFilterPathToStats('data.status', 'data', ['status', 'priority'])
    expect(result).toBe('data.typed_value.status.typed_value')
  })
})

// =============================================================================
// encodeVariant Tests
// =============================================================================

describe('encodeVariant', () => {
  it('should encode simple object to metadata and value', () => {
    const obj = { name: 'test', count: 42 }
    const { metadata, value } = encodeVariant(obj)

    expect(metadata).toBeInstanceOf(Uint8Array)
    expect(value).toBeInstanceOf(Uint8Array)
  })

  it('should encode empty object', () => {
    const { metadata, value } = encodeVariant({})

    expect(metadata).toBeInstanceOf(Uint8Array)
    expect(value).toBeInstanceOf(Uint8Array)
  })

  it('should encode null value', () => {
    const { metadata, value } = encodeVariant(null)

    expect(metadata).toBeInstanceOf(Uint8Array)
    expect(value).toBeInstanceOf(Uint8Array)
  })

  it('should encode nested objects', () => {
    const obj = {
      level1: {
        level2: {
          value: 'deep',
        },
      },
    }
    const { metadata, value } = encodeVariant(obj)

    expect(metadata).toBeInstanceOf(Uint8Array)
    expect(value).toBeInstanceOf(Uint8Array)
  })

  it('should encode arrays', () => {
    const obj = { tags: ['a', 'b', 'c'], numbers: [1, 2, 3] }
    const { metadata, value } = encodeVariant(obj)

    expect(metadata).toBeInstanceOf(Uint8Array)
    expect(value).toBeInstanceOf(Uint8Array)
  })

  it('should encode various primitive types', () => {
    const obj = {
      string: 'hello',
      int: 42,
      float: 3.14,
      bool: true,
      null: null,
      bigint: BigInt(9007199254740993),
      date: new Date('2024-01-01'),
    }
    const { metadata, value } = encodeVariant(obj)

    expect(metadata).toBeInstanceOf(Uint8Array)
    expect(value).toBeInstanceOf(Uint8Array)
  })
})

// =============================================================================
// Column Data Structure Tests
// =============================================================================

describe('Variant Shredding Column Data Structure', () => {
  it('should produce column data with correct paths', () => {
    const { columnData } = createShreddedVariantColumn(
      '$index',
      TEST_OBJECTS,
      SHRED_FIELDS
    )

    // Check all expected paths exist
    expect(columnData.has('$index.metadata')).toBe(true)
    expect(columnData.has('$index.value')).toBe(true)
    expect(columnData.has('$index.typed_value.titleType.value')).toBe(true)
    expect(columnData.has('$index.typed_value.titleType.typed_value')).toBe(true)
    expect(columnData.has('$index.typed_value.genre.value')).toBe(true)
    expect(columnData.has('$index.typed_value.genre.typed_value')).toBe(true)
    expect(columnData.has('$index.typed_value.year.value')).toBe(true)
    expect(columnData.has('$index.typed_value.year.typed_value')).toBe(true)
  })

  it('should have consistent row counts across all columns', () => {
    const { columnData } = createShreddedVariantColumn(
      '$index',
      TEST_OBJECTS,
      SHRED_FIELDS
    )

    const rowCount = TEST_OBJECTS.length
    for (const [path, data] of columnData) {
      expect(data.length).toBe(rowCount)
    }
  })

  it('should produce valid metadata binary for each row', () => {
    const { columnData } = createShreddedVariantColumn(
      '$index',
      TEST_OBJECTS,
      SHRED_FIELDS
    )

    const metadata = columnData.get('$index.metadata')!
    for (const m of metadata) {
      expect(m).toBeInstanceOf(Uint8Array)
      expect(m.length).toBeGreaterThan(0)
    }
  })

  it('should extract shredded values correctly', () => {
    const objects = [
      { type: 'A', count: 1 },
      { type: 'B', count: 2 },
      { type: 'C', count: 3 },
    ]
    const { columnData } = createShreddedVariantColumn(
      'data',
      objects,
      ['type', 'count']
    )

    expect(columnData.get('data.typed_value.type.typed_value')).toEqual(['A', 'B', 'C'])
    expect(columnData.get('data.typed_value.count.typed_value')).toEqual([1, 2, 3])
  })

  it('should handle sparse data correctly', () => {
    const objects = [
      { type: 'A', count: 1 },
      { type: 'B' },  // missing count
      { count: 3 },   // missing type
      {},             // missing both
    ]
    const { columnData } = createShreddedVariantColumn(
      'data',
      objects,
      ['type', 'count']
    )

    expect(columnData.get('data.typed_value.type.typed_value')).toEqual(['A', 'B', null, null])
    expect(columnData.get('data.typed_value.count.typed_value')).toEqual([1, null, 3, null])
  })

  it('should set value subcolumns to null for typed shredded fields', () => {
    const { columnData } = createShreddedVariantColumn(
      '$index',
      TEST_OBJECTS,
      SHRED_FIELDS
    )

    // The 'value' subcolumn of each shredded field should always be null
    // (because we use typed_value instead)
    for (const field of SHRED_FIELDS) {
      const values = columnData.get(`$index.typed_value.${field}.value`)!
      expect(values.every(v => v === null)).toBe(true)
    }
  })

  it('should encode remaining (non-shredded) fields in value column', () => {
    const objects = [
      { shredded: 'A', remaining1: 'X', remaining2: 123 },
      { shredded: 'B', remaining1: 'Y', remaining2: 456 },
    ]
    const { columnData } = createShreddedVariantColumn(
      'data',
      objects,
      ['shredded']
    )

    const values = columnData.get('data.value')!

    // Both rows have remaining data, so values should not be null
    for (const v of values) {
      expect(v).toBeInstanceOf(Uint8Array)
      expect(v.length).toBeGreaterThan(0)
    }
  })
})

// =============================================================================
// Edge Cases
// =============================================================================

describe('Edge Cases', () => {
  describe('empty inputs', () => {
    it('should handle empty object array', () => {
      const { schema, columnData, shredPaths } = createShreddedVariantColumn(
        '$index',
        [],
        ['titleType', 'year']
      )

      expect(schema.length).toBeGreaterThan(0)
      expect(columnData.get('$index.metadata')).toEqual([])
      expect(shredPaths.length).toBe(2)
    })

    it('should handle empty shred fields', () => {
      const { schema, columnData, shredPaths } = createShreddedVariantColumn(
        '$index',
        TEST_OBJECTS,
        []
      )

      // Should still have root + metadata + value + typed_value group (empty)
      expect(schema.length).toBe(4)
      expect(shredPaths).toEqual([])
    })
  })

  describe('special values', () => {
    it('should handle objects with special characters in keys', () => {
      const objects = [
        { 'key-with-dash': 'value1', 'key.with.dot': 'value2' },
      ]
      const { columnData } = createShreddedVariantColumn(
        '$index',
        objects,
        ['key-with-dash']
      )

      const values = columnData.get('$index.typed_value.key-with-dash.typed_value')
      expect(values).toEqual(['value1'])
    })

    it('should handle unicode strings', () => {
      const objects = [
        { title: '\u4e2d\u6587', genre: '\u00e9\u00e0\u00fc' },
        { title: 'normal', genre: '\ud83c\udf89' },
      ]
      const { columnData } = createShreddedVariantColumn(
        '$index',
        objects,
        ['title', 'genre']
      )

      const titles = columnData.get('$index.typed_value.title.typed_value')
      expect(titles).toEqual(['\u4e2d\u6587', 'normal'])
    })

    it('should handle very large objects', () => {
      const largeObject = {
        titleType: 'movie',
        data: 'x'.repeat(10000),
      }
      const { columnData } = createShreddedVariantColumn(
        '$index',
        [largeObject],
        ['titleType']
      )

      const titleTypes = columnData.get('$index.typed_value.titleType.typed_value')
      expect(titleTypes).toEqual(['movie'])

      // Non-shredded data should be in value column
      const values = columnData.get('$index.value')
      expect(values![0]).toBeInstanceOf(Uint8Array)
    })
  })

  describe('type edge cases', () => {
    it('should handle Date objects', () => {
      const now = new Date()
      const objects = [{ createdAt: now }]
      const { columnData } = createShreddedVariantColumn(
        '$index',
        objects,
        ['createdAt']
      )

      const values = columnData.get('$index.typed_value.createdAt.typed_value')
      // Should be converted to BigInt milliseconds
      expect(typeof values![0]).toBe('bigint')
      expect(values![0]).toBe(BigInt(now.getTime()))
    })

    it('should handle nested objects as JSON strings', () => {
      const objects = [
        { metadata: { nested: true, value: 123 } },
      ]
      const { columnData } = createShreddedVariantColumn(
        '$index',
        objects,
        ['metadata']
      )

      const values = columnData.get('$index.typed_value.metadata.typed_value')
      // Nested objects are JSON stringified
      expect(typeof values![0]).toBe('string')
      expect(JSON.parse(values![0] as string)).toEqual({ nested: true, value: 123 })
    })

    it('should handle array values as JSON strings', () => {
      const objects = [
        { tags: ['a', 'b', 'c'] },
      ]
      const { columnData } = createShreddedVariantColumn(
        '$index',
        objects,
        ['tags']
      )

      const values = columnData.get('$index.typed_value.tags.typed_value')
      expect(typeof values![0]).toBe('string')
      expect(JSON.parse(values![0] as string)).toEqual(['a', 'b', 'c'])
    })

    it('should handle zero values correctly', () => {
      const objects = [
        { count: 0, rating: 0.0 },
      ]
      const { columnData } = createShreddedVariantColumn(
        '$index',
        objects,
        ['count', 'rating']
      )

      const counts = columnData.get('$index.typed_value.count.typed_value')
      expect(counts).toEqual([0])

      const ratings = columnData.get('$index.typed_value.rating.typed_value')
      expect(ratings).toEqual([0.0])
    })

    it('should handle boolean false correctly', () => {
      const objects = [
        { active: false },
        { active: true },
      ]
      const { columnData } = createShreddedVariantColumn(
        '$index',
        objects,
        ['active']
      )

      const values = columnData.get('$index.typed_value.active.typed_value')
      expect(values).toEqual([false, true])
    })

    it('should handle empty strings', () => {
      const objects = [
        { title: '' },
        { title: 'non-empty' },
      ]
      const { columnData } = createShreddedVariantColumn(
        '$index',
        objects,
        ['title']
      )

      const values = columnData.get('$index.typed_value.title.typed_value')
      expect(values).toEqual(['', 'non-empty'])
    })
  })
})

// =============================================================================
// Performance Characteristics
// =============================================================================

describe('Performance Characteristics', () => {
  it('should handle large number of rows', () => {
    const count = 1000
    const objects = Array.from({ length: count }, (_, i) => ({
      titleType: i % 2 === 0 ? 'movie' : 'series',
      year: 2000 + (i % 25),
      id: `item-${i}`,
    }))

    const { columnData, shredPaths } = createShreddedVariantColumn(
      '$index',
      objects,
      ['titleType', 'year']
    )

    // Verify all rows were processed
    const titleTypes = columnData.get('$index.typed_value.titleType.typed_value')
    expect(titleTypes).toHaveLength(count)

    // Verify correct distribution
    const movies = titleTypes!.filter((t) => t === 'movie')
    const series = titleTypes!.filter((t) => t === 'series')
    expect(movies.length).toBe(500)
    expect(series.length).toBe(500)
  })

  it('should handle many shred fields', () => {
    const fields = Array.from({ length: 20 }, (_, i) => `field${i}`)
    const objects = [
      Object.fromEntries(fields.map((f, i) => [f, `value${i}`])),
    ]

    const { schema, columnData, shredPaths } = createShreddedVariantColumn(
      '$index',
      objects,
      fields
    )

    expect(shredPaths).toHaveLength(20)

    // All fields should be shredded
    for (const field of fields) {
      const values = columnData.get(`$index.typed_value.${field}.typed_value`)
      expect(values).toHaveLength(1)
    }
  })
})

// =============================================================================
// Nested Object Shredding Tests
// =============================================================================

describe('Nested Object Shredding', () => {
  describe('single-level nesting', () => {
    it('should shred top-level fields from objects with nested data', () => {
      const objects = [
        {
          status: 'active',
          metadata: { version: 1, author: 'user1' },
          tags: ['a', 'b'],
        },
        {
          status: 'inactive',
          metadata: { version: 2, author: 'user2' },
          tags: ['c'],
        },
      ]
      const { columnData } = createShreddedVariantColumn('$data', objects, ['status'])

      // Status should be shredded as string
      const statusValues = columnData.get('$data.typed_value.status.typed_value')
      expect(statusValues).toEqual(['active', 'inactive'])

      // Remaining data (metadata, tags) should be in value column
      const values = columnData.get('$data.value')
      expect(values![0]).toBeInstanceOf(Uint8Array)
      expect(values![1]).toBeInstanceOf(Uint8Array)
    })

    it('should handle nested objects when shredding a nested field value', () => {
      const objects = [
        { config: { enabled: true, level: 5 }, name: 'test1' },
        { config: { enabled: false, level: 10 }, name: 'test2' },
      ]
      // Shred the entire 'config' nested object (serialized as JSON)
      const { columnData } = createShreddedVariantColumn('$data', objects, ['config'])

      const configValues = columnData.get('$data.typed_value.config.typed_value')
      // Nested objects are serialized as JSON strings
      expect(typeof configValues![0]).toBe('string')
      expect(JSON.parse(configValues![0] as string)).toEqual({ enabled: true, level: 5 })
      expect(JSON.parse(configValues![1] as string)).toEqual({ enabled: false, level: 10 })
    })

    it('should preserve remaining nested fields in value column', () => {
      const objects = [
        {
          id: 'item1',
          price: 100,
          details: {
            color: 'red',
            size: 'large',
            dimensions: { width: 10, height: 20 },
          },
        },
      ]
      const { columnData } = createShreddedVariantColumn('$data', objects, ['id', 'price'])

      // Check shredded fields
      expect(columnData.get('$data.typed_value.id.typed_value')).toEqual(['item1'])
      expect(columnData.get('$data.typed_value.price.typed_value')).toEqual([100])

      // Value column should contain the nested 'details' object
      const values = columnData.get('$data.value')
      expect(values![0]).toBeInstanceOf(Uint8Array)
    })
  })

  describe('deeply nested objects', () => {
    it('should handle objects with multiple levels of nesting', () => {
      const objects = [
        {
          level1: {
            level2: {
              level3: {
                value: 'deep',
                count: 42,
              },
            },
          },
          status: 'active',
        },
      ]
      const { columnData } = createShreddedVariantColumn('$data', objects, ['status'])

      expect(columnData.get('$data.typed_value.status.typed_value')).toEqual(['active'])
      // Deep nesting is preserved in value column
      const values = columnData.get('$data.value')
      expect(values![0]).toBeInstanceOf(Uint8Array)
    })

    it('should handle shredding nested object as JSON string', () => {
      const deepNested = {
        a: { b: { c: { d: { e: 'deep' } } } },
      }
      const objects = [
        { deep: deepNested, id: 'test' },
      ]
      const { columnData } = createShreddedVariantColumn('$data', objects, ['deep'])

      const deepValues = columnData.get('$data.typed_value.deep.typed_value')
      expect(typeof deepValues![0]).toBe('string')
      expect(JSON.parse(deepValues![0] as string)).toEqual(deepNested)
    })
  })

  describe('nested objects with arrays', () => {
    it('should handle objects containing arrays at various levels', () => {
      const objects = [
        {
          tags: ['web', 'api', 'backend'],
          config: {
            features: ['auth', 'logging'],
            nested: {
              items: [1, 2, 3],
            },
          },
          status: 'running',
        },
      ]
      const { columnData } = createShreddedVariantColumn('$data', objects, ['status', 'tags'])

      expect(columnData.get('$data.typed_value.status.typed_value')).toEqual(['running'])
      // Arrays are serialized as JSON
      const tagsValues = columnData.get('$data.typed_value.tags.typed_value')
      expect(JSON.parse(tagsValues![0] as string)).toEqual(['web', 'api', 'backend'])
    })

    it('should handle array of objects', () => {
      const objects = [
        {
          items: [
            { name: 'item1', price: 10 },
            { name: 'item2', price: 20 },
          ],
          total: 30,
        },
      ]
      const { columnData } = createShreddedVariantColumn('$data', objects, ['total', 'items'])

      expect(columnData.get('$data.typed_value.total.typed_value')).toEqual([30])
      const itemsValues = columnData.get('$data.typed_value.items.typed_value')
      expect(JSON.parse(itemsValues![0] as string)).toEqual([
        { name: 'item1', price: 10 },
        { name: 'item2', price: 20 },
      ])
    })
  })
})

// =============================================================================
// Array Shredding Tests
// =============================================================================

describe('Array Shredding', () => {
  describe('primitive arrays', () => {
    it('should shred array of strings as JSON', () => {
      const objects = [
        { tags: ['javascript', 'typescript', 'node'], count: 3 },
        { tags: ['python', 'django'], count: 2 },
        { tags: [], count: 0 },
      ]
      const { columnData } = createShreddedVariantColumn('$data', objects, ['tags', 'count'])

      const tagsValues = columnData.get('$data.typed_value.tags.typed_value')
      expect(JSON.parse(tagsValues![0] as string)).toEqual(['javascript', 'typescript', 'node'])
      expect(JSON.parse(tagsValues![1] as string)).toEqual(['python', 'django'])
      expect(JSON.parse(tagsValues![2] as string)).toEqual([])

      expect(columnData.get('$data.typed_value.count.typed_value')).toEqual([3, 2, 0])
    })

    it('should shred array of numbers as JSON', () => {
      const objects = [
        { scores: [85, 90, 78], average: 84.3 },
        { scores: [100], average: 100 },
      ]
      const { columnData } = createShreddedVariantColumn('$data', objects, ['scores'])

      const scoresValues = columnData.get('$data.typed_value.scores.typed_value')
      expect(JSON.parse(scoresValues![0] as string)).toEqual([85, 90, 78])
      expect(JSON.parse(scoresValues![1] as string)).toEqual([100])
    })

    it('should handle mixed-type arrays', () => {
      const objects = [
        { mixed: [1, 'two', true, null, { nested: 'value' }] },
      ]
      const { columnData } = createShreddedVariantColumn('$data', objects, ['mixed'])

      const mixedValues = columnData.get('$data.typed_value.mixed.typed_value')
      expect(JSON.parse(mixedValues![0] as string)).toEqual([1, 'two', true, null, { nested: 'value' }])
    })
  })

  describe('nested arrays', () => {
    it('should handle 2D arrays', () => {
      const objects = [
        { matrix: [[1, 2, 3], [4, 5, 6], [7, 8, 9]] },
        { matrix: [[10, 20], [30, 40]] },
      ]
      const { columnData } = createShreddedVariantColumn('$data', objects, ['matrix'])

      const matrixValues = columnData.get('$data.typed_value.matrix.typed_value')
      expect(JSON.parse(matrixValues![0] as string)).toEqual([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
      expect(JSON.parse(matrixValues![1] as string)).toEqual([[10, 20], [30, 40]])
    })

    it('should handle array of objects with nested arrays', () => {
      const objects = [
        {
          users: [
            { name: 'Alice', roles: ['admin', 'user'] },
            { name: 'Bob', roles: ['user'] },
          ],
        },
      ]
      const { columnData } = createShreddedVariantColumn('$data', objects, ['users'])

      const usersValues = columnData.get('$data.typed_value.users.typed_value')
      expect(JSON.parse(usersValues![0] as string)).toEqual([
        { name: 'Alice', roles: ['admin', 'user'] },
        { name: 'Bob', roles: ['user'] },
      ])
    })
  })

  describe('empty and sparse arrays', () => {
    it('should handle empty arrays', () => {
      const objects = [
        { items: [], status: 'empty' },
        { items: [1, 2, 3], status: 'populated' },
      ]
      const { columnData } = createShreddedVariantColumn('$data', objects, ['items', 'status'])

      const itemsValues = columnData.get('$data.typed_value.items.typed_value')
      expect(JSON.parse(itemsValues![0] as string)).toEqual([])
      expect(JSON.parse(itemsValues![1] as string)).toEqual([1, 2, 3])
    })

    it('should handle undefined/null array elements', () => {
      const objects = [
        { arr: [1, null, 3, undefined, 5] },
      ]
      const { columnData } = createShreddedVariantColumn('$data', objects, ['arr'])

      const arrValues = columnData.get('$data.typed_value.arr.typed_value')
      // undefined becomes null in JSON
      expect(JSON.parse(arrValues![0] as string)).toEqual([1, null, 3, null, 5])
    })

    it('should handle missing array fields', () => {
      const objects = [
        { arr: [1, 2, 3], name: 'with-arr' },
        { name: 'without-arr' }, // arr is missing
      ]
      const { columnData } = createShreddedVariantColumn('$data', objects, ['arr', 'name'])

      const arrValues = columnData.get('$data.typed_value.arr.typed_value')
      expect(JSON.parse(arrValues![0] as string)).toEqual([1, 2, 3])
      expect(arrValues![1]).toBeNull()
    })
  })
})

// =============================================================================
// Auto-Detection of Hot Fields Tests
// =============================================================================

describe('Auto-Detection of Hot Fields', () => {
  describe('determineShredFields function', () => {
    it('should auto-detect enum fields for shredding', () => {
      const typeDef: TypeDefinition = {
        $name: 'Task',
        status: 'enum:pending,active,completed',
        priority: 'enum:low,medium,high',
        description: 'text',
      }

      const shredFields = determineShredFields(typeDef)

      expect(shredFields).toContain('status')
      expect(shredFields).toContain('priority')
      expect(shredFields).not.toContain('description')
    })

    it('should auto-detect boolean fields for shredding', () => {
      const typeDef: TypeDefinition = {
        $name: 'User',
        isActive: 'boolean',
        isVerified: 'boolean',
        bio: 'text',
      }

      const shredFields = determineShredFields(typeDef)

      expect(shredFields).toContain('isActive')
      expect(shredFields).toContain('isVerified')
      expect(shredFields).not.toContain('bio')
    })

    it('should auto-detect date/timestamp fields for shredding', () => {
      const typeDef: TypeDefinition = {
        $name: 'Event',
        createdAt: 'datetime',
        updatedAt: 'timestamp',
        eventDate: 'date',
        title: 'string',
      }

      const shredFields = determineShredFields(typeDef)

      expect(shredFields).toContain('createdAt')
      expect(shredFields).toContain('updatedAt')
      expect(shredFields).toContain('eventDate')
      expect(shredFields).not.toContain('title')
    })

    it('should auto-detect numeric fields for shredding', () => {
      const typeDef: TypeDefinition = {
        $name: 'Product',
        price: 'float',
        quantity: 'int',
        sku: 'string',
      }

      const shredFields = determineShredFields(typeDef)

      expect(shredFields).toContain('price')
      expect(shredFields).toContain('quantity')
      expect(shredFields).not.toContain('sku')
    })

    it('should include explicitly marked shred fields via $shred', () => {
      const typeDef: TypeDefinition = {
        $name: 'Document',
        $shred: ['category', 'priority'],
        category: 'string',
        priority: 'string',
        content: 'text',
      }

      const shredFields = determineShredFields(typeDef)

      expect(shredFields).toContain('category')
      expect(shredFields).toContain('priority')
    })

    it('should include indexed fields for shredding', () => {
      const typeDef: TypeDefinition = {
        $name: 'Article',
        authorId: 'string index',
        slug: { type: 'string', index: true },
        content: 'text',
      }

      const shredFields = determineShredFields(typeDef)

      expect(shredFields).toContain('authorId')
      expect(shredFields).toContain('slug')
    })

    it('should respect always-shred fields in config', () => {
      const typeDef: TypeDefinition = {
        $name: 'Log',
        level: 'string',
        message: 'text',
      }

      const customConfig = {
        ...DEFAULT_SHRED_CONFIG,
        always: ['level', 'timestamp'],
      }

      const shredFields = determineShredFields(typeDef, customConfig)

      expect(shredFields).toContain('level')
      expect(shredFields).toContain('timestamp')
    })

    it('should disable auto-detection when auto is false', () => {
      const typeDef: TypeDefinition = {
        $name: 'Config',
        enabled: 'boolean',
        count: 'int',
        name: 'string',
      }

      const customConfig = {
        always: [],
        auto: false,
        shredTypes: [],
      }

      const shredFields = determineShredFields(typeDef, customConfig)

      expect(shredFields).toEqual([])
    })

    it('should handle custom shred types', () => {
      const typeDef: TypeDefinition = {
        $name: 'Custom',
        customField: 'mytype',
        regularField: 'string',
      }

      const customConfig = {
        ...DEFAULT_SHRED_CONFIG,
        shredTypes: ['mytype'],
      }

      const shredFields = determineShredFields(typeDef, customConfig)

      expect(shredFields).toContain('customField')
      expect(shredFields).not.toContain('regularField')
    })

    it('should combine explicit $shred with auto-detection', () => {
      const typeDef: TypeDefinition = {
        $name: 'Combined',
        $shred: ['customField'],
        customField: 'string',
        status: 'enum:a,b,c',
        description: 'text',
      }

      const shredFields = determineShredFields(typeDef)

      expect(shredFields).toContain('customField')
      expect(shredFields).toContain('status')
      expect(shredFields).not.toContain('description')
    })
  })

  describe('type detection from values', () => {
    it('should auto-detect string type from values', () => {
      const { schema } = createShreddedVariantColumn(
        '$data',
        [{ field: 'value1' }, { field: 'value2' }],
        ['field']
      )

      const typedValue = schema.find(
        (s) => s.name === 'typed_value' && s.type === 'BYTE_ARRAY'
      )
      expect(typedValue?.converted_type).toBe('UTF8')
    })

    it('should auto-detect integer type from values', () => {
      const { schema } = createShreddedVariantColumn(
        '$data',
        [{ count: 10 }, { count: 20 }, { count: 30 }],
        ['count']
      )

      const typedValue = schema.find(
        (s) => s.name === 'typed_value' && s.type === 'INT32'
      )
      expect(typedValue).toBeDefined()
    })

    it('should auto-detect boolean type from values', () => {
      const { schema } = createShreddedVariantColumn(
        '$data',
        [{ active: true }, { active: false }],
        ['active']
      )

      const typedValue = schema.find(
        (s) => s.name === 'typed_value' && s.type === 'BOOLEAN'
      )
      expect(typedValue).toBeDefined()
    })

    it('should fall back to UTF8 for mixed types', () => {
      const { schema } = createShreddedVariantColumn(
        '$data',
        [{ value: 'string' }, { value: 123 }, { value: true }],
        ['value']
      )

      const typedValue = schema.find(
        (s) => s.name === 'typed_value' && s.type === 'BYTE_ARRAY'
      )
      expect(typedValue?.converted_type).toBe('UTF8')
    })

    it('should respect fieldTypes override', () => {
      const { schema } = createShreddedVariantColumn(
        '$data',
        [{ year: 2024 }], // Would normally be INT32
        ['year'],
        { fieldTypes: { year: 'INT64' } }
      )

      const typedValue = schema.find(
        (s) => s.name === 'typed_value' && s.type === 'INT64'
      )
      expect(typedValue).toBeDefined()
    })
  })
})

// =============================================================================
// Predicate Pushdown Tests
// =============================================================================

describe('Predicate Pushdown with Shredded Fields', () => {
  const testConfigs = [
    {
      columnName: '$data',
      fields: ['year', 'rating', 'status', 'category'],
      fieldTypes: { year: 'int', rating: 'double', status: 'string', category: 'string' },
    },
    {
      columnName: '$index',
      fields: ['titleType', 'genre', 'isActive'],
      fieldTypes: { titleType: 'string', genre: 'string', isActive: 'boolean' },
    },
  ]

  describe('filter transformation for shredded paths', () => {
    it('should identify equality filters on shredded fields', () => {
      const context = ShreddedPushdownContext.fromConfig(testConfigs)
      const filter = { '$data.status': 'active' }

      expect(context.isShreddedField('$data.status')).toBe(true)
      const transformed = context.transformFilter(filter)
      expect(transformed).toBeDefined()
    })

    it('should identify range filters on shredded fields', () => {
      const context = ShreddedPushdownContext.fromConfig(testConfigs)
      const filter = {
        '$data.year': { $gte: 2020, $lte: 2025 },
        '$data.rating': { $gt: 8.0 },
      }

      expect(context.isShreddedField('$data.year')).toBe(true)
      expect(context.isShreddedField('$data.rating')).toBe(true)
      const transformed = context.transformFilter(filter)
      expect(transformed).toBeDefined()
    })

    it('should handle $in filters on shredded fields', () => {
      const context = ShreddedPushdownContext.fromConfig(testConfigs)
      const filter = {
        '$data.category': { $in: ['movies', 'tv', 'games'] },
      }

      expect(context.isShreddedField('$data.category')).toBe(true)
    })

    it('should pass through non-shredded filters unchanged', () => {
      const context = ShreddedPushdownContext.fromConfig(testConfigs)
      const filter = { name: 'test', $type: 'Post' }

      const transformed = context.transformFilter(filter)
      expect(transformed).toEqual(filter)
    })
  })

  describe('filter extraction', () => {
    it('should extract shredded paths from simple filter', () => {
      const context = ShreddedPushdownContext.fromConfig(testConfigs)
      const filter = {
        '$data.year': { $gte: 2020 },
        '$index.titleType': 'movie',
      }

      const result = context.extractFilterColumns(filter)
      expect(result.shreddedPaths).toContain('$data.year')
      expect(result.shreddedPaths).toContain('$index.titleType')
    })

    it('should extract shredded paths from $and filter', () => {
      const context = ShreddedPushdownContext.fromConfig(testConfigs)
      const filter = {
        $and: [
          { '$data.year': { $gte: 2020 } },
          { '$data.rating': { $gt: 7.0 } },
          { '$index.titleType': 'movie' },
        ],
      }

      const paths = extractShreddedFilterPaths(filter, context)
      expect(paths).toContain('$data.year')
      expect(paths).toContain('$data.rating')
      expect(paths).toContain('$index.titleType')
    })

    it('should extract shredded paths from $or filter', () => {
      const context = ShreddedPushdownContext.fromConfig(testConfigs)
      const filter = {
        $or: [
          { '$data.status': 'active' },
          { '$data.status': 'pending' },
        ],
      }

      const paths = extractShreddedFilterPaths(filter, context)
      expect(paths).toContain('$data.status')
    })

    it('should handle nested logical operators', () => {
      const context = ShreddedPushdownContext.fromConfig(testConfigs)
      const filter = {
        $and: [
          {
            $or: [
              { '$data.year': { $lt: 2000 } },
              { '$data.year': { $gt: 2020 } },
            ],
          },
          { '$index.titleType': 'movie' },
        ],
      }

      const paths = extractShreddedFilterPaths(filter, context)
      expect(paths).toContain('$data.year')
      expect(paths).toContain('$index.titleType')
    })
  })

  describe('pushdown effectiveness estimation', () => {
    it('should calculate 100% effectiveness for all-shredded filters', () => {
      const context = ShreddedPushdownContext.fromConfig(testConfigs)
      const filter = {
        '$data.year': { $gte: 2020 },
        '$data.rating': { $gt: 8.0 },
        '$index.titleType': 'movie',
      }

      const result = estimatePushdownEffectiveness(filter, context)

      expect(result.totalConditions).toBe(3)
      expect(result.shreddedConditions).toBe(3)
      expect(result.effectiveness).toBe(1.0)
      expect(result.isEffective).toBe(true)
    })

    it('should calculate partial effectiveness for mixed filters', () => {
      const context = ShreddedPushdownContext.fromConfig(testConfigs)
      const filter = {
        '$data.year': { $gte: 2020 },
        name: 'Test',
        title: { $regex: 'hello' },
      }

      const result = estimatePushdownEffectiveness(filter, context)

      expect(result.totalConditions).toBe(3)
      expect(result.shreddedConditions).toBe(1)
      expect(result.effectiveness).toBeCloseTo(0.333, 2)
      expect(result.isEffective).toBe(false)
    })

    it('should return 0 effectiveness for no shredded conditions', () => {
      const context = ShreddedPushdownContext.fromConfig(testConfigs)
      const filter = {
        name: 'Test',
        $type: 'Post',
      }

      const result = estimatePushdownEffectiveness(filter, context)

      expect(result.shreddedConditions).toBe(0)
      expect(result.effectiveness).toBe(0)
      expect(result.isEffective).toBe(false)
    })
  })

  describe('hasShreddedConditions', () => {
    it('should return true when filter has shredded conditions', () => {
      const context = ShreddedPushdownContext.fromConfig(testConfigs)

      expect(hasShreddedConditions({ '$data.year': 2020 }, context)).toBe(true)
      expect(hasShreddedConditions({ '$index.genre': 'action' }, context)).toBe(true)
    })

    it('should return false when filter has no shredded conditions', () => {
      const context = ShreddedPushdownContext.fromConfig(testConfigs)

      expect(hasShreddedConditions({ name: 'test' }, context)).toBe(false)
      expect(hasShreddedConditions({ $type: 'Post' }, context)).toBe(false)
    })

    it('should return false for empty context', () => {
      const emptyContext = ShreddedPushdownContext.empty()

      expect(hasShreddedConditions({ '$data.year': 2020 }, emptyContext)).toBe(false)
    })
  })

  describe('range predicate creation', () => {
    it('should create range predicate for equality', () => {
      const context = ShreddedPushdownContext.fromConfig(testConfigs)
      const pred = context.createRangePredicate('$eq', 2020)

      expect(pred.lowerInclusive).toBe(2020)
      expect(pred.upperInclusive).toBe(2020)
    })

    it('should create range predicate for greater than', () => {
      const context = ShreddedPushdownContext.fromConfig(testConfigs)
      const pred = context.createRangePredicate('$gt', 2020)

      expect(pred.lowerExclusive).toBe(2020)
      expect(pred.upperExclusive).toBeUndefined()
    })

    it('should create range predicate for less than or equal', () => {
      const context = ShreddedPushdownContext.fromConfig(testConfigs)
      const pred = context.createRangePredicate('$lte', 2020)

      expect(pred.upperInclusive).toBe(2020)
      expect(pred.lowerInclusive).toBeUndefined()
    })

    it('should create range predicate for $in with points', () => {
      const context = ShreddedPushdownContext.fromConfig(testConfigs)
      const pred = context.createRangePredicate('$in', [2018, 2019, 2020])

      expect(pred.points).toEqual([2018, 2019, 2020])
    })
  })

  describe('predicate combination', () => {
    it('should combine predicates with AND logic', () => {
      const context = ShreddedPushdownContext.fromConfig(testConfigs)
      const p1 = context.createRangePredicate('$gte', 2020)
      const p2 = context.createRangePredicate('$lte', 2025)

      const combined = context.combinePredicatesAnd([p1, p2])

      expect(combined).not.toBeNull()
      expect(combined!.lowerInclusive).toBe(2020)
      expect(combined!.upperInclusive).toBe(2025)
    })

    it('should combine predicates with OR logic', () => {
      const context = ShreddedPushdownContext.fromConfig(testConfigs)
      const p1 = context.createRangePredicate('$eq', 10)
      const p2 = context.createRangePredicate('$eq', 20)

      const combined = context.combinePredicatesOr([p1, p2])

      expect(combined).toHaveLength(2)
    })

    it('should return null for impossible AND combination', () => {
      const context = ShreddedPushdownContext.fromConfig(testConfigs)
      const p1 = context.createRangePredicate('$gt', 2025) // > 2025
      const p2 = context.createRangePredicate('$lt', 2020) // < 2020

      const combined = context.combinePredicatesAnd([p1, p2])

      // Impossible range (> 2025 AND < 2020) should return null
      expect(combined).toBeNull()
    })
  })

  describe('configuration roundtrip', () => {
    it('should preserve shredding config through properties', () => {
      const props = buildShreddingProperties(testConfigs)
      const context = ShreddedPushdownContext.fromTableProperties(props)

      expect(context.hasShredding).toBe(true)
      expect(context.shreddedColumns).toContain('$data')
      expect(context.shreddedColumns).toContain('$index')

      for (const config of testConfigs) {
        for (const field of config.fields) {
          expect(context.isShreddedField(`${config.columnName}.${field}`)).toBe(true)
        }
      }
    })
  })
})
