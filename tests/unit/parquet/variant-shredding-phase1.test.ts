/**
 * Variant Shredding Phase 1 Tests
 *
 * Tests for Phase 1 of Variant shredding implementation:
 * - $type field shredding from $data Variant column
 * - Predicate pushdown for $type field
 * - Integration with hyparquet reader
 */

import { describe, it, expect } from 'vitest'
import {
  prepareShreddedVariantData,
  getDataShredFields,
  mapFilterToStatisticsPath,
  transformFilterForShredding,
  canPushdownWithShredding,
  createShreddedPredicate,
  shouldSkipRowGroup,
  DEFAULT_DATA_SHRED_FIELDS,
  type ShreddedVariantReadConfig,
} from '@/parquet/variant-shredding'
import type { Entity } from '@/types/entity'
import type { TypeDefinition } from '@/types/schema'

// =============================================================================
// Test Data
// =============================================================================

const testEntities: Entity[] = [
  {
    $id: 'posts/1',
    $type: 'Post',
    name: 'First Post',
    title: 'Hello World',
    status: 'published',
    views: 100,
    createdAt: new Date('2024-01-01'),
    createdBy: 'users/1',
    updatedAt: new Date('2024-01-02'),
    updatedBy: 'users/1',
    version: 1,
  },
  {
    $id: 'posts/2',
    $type: 'Post',
    name: 'Second Post',
    title: 'Another Post',
    status: 'draft',
    views: 50,
    createdAt: new Date('2024-01-03'),
    createdBy: 'users/2',
    updatedAt: new Date('2024-01-04'),
    updatedBy: 'users/2',
    version: 1,
  },
  {
    $id: 'comments/1',
    $type: 'Comment',
    name: 'First Comment',
    content: 'Great post!',
    postId: 'posts/1',
    createdAt: new Date('2024-01-05'),
    createdBy: 'users/3',
    updatedAt: new Date('2024-01-05'),
    updatedBy: 'users/3',
    version: 1,
  },
]

const testTypeDef: TypeDefinition = {
  $name: 'Post',
  title: 'string!',
  status: 'enum:draft,published,archived',
  views: 'int',
  content: 'text',
}

// =============================================================================
// DEFAULT_DATA_SHRED_FIELDS Tests
// =============================================================================

describe('DEFAULT_DATA_SHRED_FIELDS', () => {
  it('should include $type field', () => {
    expect(DEFAULT_DATA_SHRED_FIELDS).toContain('$type')
  })
})

// =============================================================================
// getDataShredFields Tests
// =============================================================================

describe('getDataShredFields', () => {
  it('should always include $type', () => {
    const fields = getDataShredFields()
    expect(fields).toContain('$type')
  })

  it('should include $type with no type definition', () => {
    const fields = getDataShredFields(undefined)
    expect(fields).toContain('$type')
  })

  it('should include explicitly marked shred fields from type definition', () => {
    const typeDef: TypeDefinition = {
      $name: 'Task',
      $shred: ['status', 'priority'],
      status: 'string',
      priority: 'int',
      description: 'text',
    }
    const fields = getDataShredFields(typeDef)
    expect(fields).toContain('$type')
    expect(fields).toContain('status')
    expect(fields).toContain('priority')
  })

  it('should auto-detect enum fields for shredding', () => {
    const typeDef: TypeDefinition = {
      $name: 'Order',
      status: 'enum:pending,shipped,delivered',
      notes: 'text',
    }
    const fields = getDataShredFields(typeDef)
    expect(fields).toContain('status')
    expect(fields).not.toContain('notes')
  })

  it('should auto-detect boolean fields for shredding', () => {
    const typeDef: TypeDefinition = {
      $name: 'User',
      isActive: 'boolean',
      bio: 'text',
    }
    const fields = getDataShredFields(typeDef)
    expect(fields).toContain('isActive')
  })

  it('should auto-detect date/timestamp fields for shredding', () => {
    const typeDef: TypeDefinition = {
      $name: 'Event',
      eventDate: 'date',
      startTime: 'datetime',
      title: 'string',
    }
    const fields = getDataShredFields(typeDef)
    expect(fields).toContain('eventDate')
    expect(fields).toContain('startTime')
  })

  it('should auto-detect indexed fields for shredding', () => {
    const typeDef: TypeDefinition = {
      $name: 'Article',
      slug: { type: 'string', index: true },
      content: 'text',
    }
    const fields = getDataShredFields(typeDef)
    expect(fields).toContain('slug')
  })
})

// =============================================================================
// prepareShreddedVariantData Tests
// =============================================================================

describe('prepareShreddedVariantData', () => {
  it('should prepare shredded Variant data with $type field', async () => {
    const result = await prepareShreddedVariantData(testEntities)

    expect(result.schema).toBeDefined()
    expect(result.columnData).toBeInstanceOf(Map)
    expect(result.statisticsPaths).toBeDefined()
  })

  it('should include $type in shredded column data', async () => {
    const result = await prepareShreddedVariantData(testEntities, ['$type'])

    // Should have typed_value column for $type
    const typePath = '$data.typed_value.$type.typed_value'
    expect(result.columnData.has(typePath)).toBe(true)

    const typeValues = result.columnData.get(typePath)
    expect(typeValues).toHaveLength(testEntities.length)
    expect(typeValues).toEqual(['Post', 'Post', 'Comment'])
  })

  it('should include metadata column', async () => {
    const result = await prepareShreddedVariantData(testEntities, ['$type'])

    const metadataPath = '$data.metadata'
    expect(result.columnData.has(metadataPath)).toBe(true)

    const metadata = result.columnData.get(metadataPath)
    expect(metadata).toHaveLength(testEntities.length)
    // All metadata values should be Uint8Array
    for (const m of metadata!) {
      expect(m).toBeInstanceOf(Uint8Array)
    }
  })

  it('should include remaining data in value column', async () => {
    const result = await prepareShreddedVariantData(testEntities, ['$type'])

    const valuePath = '$data.value'
    expect(result.columnData.has(valuePath)).toBe(true)

    const values = result.columnData.get(valuePath)
    expect(values).toHaveLength(testEntities.length)
  })

  it('should provide statistics paths for shredded fields', async () => {
    const result = await prepareShreddedVariantData(testEntities, ['$type', 'status'])

    expect(result.statisticsPaths).toContain('$data.typed_value.$type.typed_value')
    expect(result.statisticsPaths).toContain('$data.typed_value.status.typed_value')
  })

  it('should handle custom column name', async () => {
    const result = await prepareShreddedVariantData(testEntities, ['$type'], '$index')

    expect(result.columnData.has('$index.metadata')).toBe(true)
    expect(result.columnData.has('$index.typed_value.$type.typed_value')).toBe(true)
  })

  it('should handle empty entities array', async () => {
    const result = await prepareShreddedVariantData([], ['$type'])

    expect(result.schema).toBeDefined()
    expect(result.columnData.get('$data.metadata')).toEqual([])
  })

  it('should handle entities with missing $type', async () => {
    const entities: Entity[] = [
      {
        $id: 'test/1',
        $type: undefined as unknown as string,
        name: 'No Type',
        createdAt: new Date(),
        createdBy: 'users/1',
        updatedAt: new Date(),
        updatedBy: 'users/1',
        version: 1,
      },
    ]
    const result = await prepareShreddedVariantData(entities, ['$type'])

    const typeValues = result.columnData.get('$data.typed_value.$type.typed_value')
    expect(typeValues).toHaveLength(1)
    expect(typeValues![0]).toBeNull()
  })
})

// =============================================================================
// mapFilterToStatisticsPath Tests
// =============================================================================

describe('mapFilterToStatisticsPath', () => {
  const config: ShreddedVariantReadConfig = {
    columnName: '$data',
    shredFields: ['$type', 'status', 'priority'],
  }

  it('should map $data.$type to statistics path', () => {
    const result = mapFilterToStatisticsPath('$data.$type', config)
    expect(result).toBe('$data.typed_value.$type.typed_value')
  })

  it('should map $data.status to statistics path', () => {
    const result = mapFilterToStatisticsPath('$data.status', config)
    expect(result).toBe('$data.typed_value.status.typed_value')
  })

  it('should return null for non-shredded fields', () => {
    const result = mapFilterToStatisticsPath('$data.title', config)
    expect(result).toBeNull()
  })

  it('should return null for different column name', () => {
    const result = mapFilterToStatisticsPath('$index.$type', config)
    expect(result).toBeNull()
  })

  it('should return null for non-dot-notation paths', () => {
    const result = mapFilterToStatisticsPath('$type', config)
    expect(result).toBeNull()
  })

  it('should handle nested paths (first level only)', () => {
    const result = mapFilterToStatisticsPath('$data.$type.nested', config)
    expect(result).toBe('$data.typed_value.$type.typed_value')
  })
})

// =============================================================================
// transformFilterForShredding Tests
// =============================================================================

describe('transformFilterForShredding', () => {
  const configs: ShreddedVariantReadConfig[] = [
    { columnName: '$data', shredFields: ['$type', 'status'] },
  ]

  it('should transform $data.$type filter path', () => {
    const filter = { '$data.$type': 'Post' }
    const result = transformFilterForShredding(filter, configs)
    expect(result).toEqual({ '$data.typed_value.$type.typed_value': 'Post' })
  })

  it('should transform multiple shredded field paths', () => {
    const filter = {
      '$data.$type': 'Post',
      '$data.status': 'published',
    }
    const result = transformFilterForShredding(filter, configs)
    expect(result).toEqual({
      '$data.typed_value.$type.typed_value': 'Post',
      '$data.typed_value.status.typed_value': 'published',
    })
  })

  it('should pass through non-shredded paths unchanged', () => {
    const filter = {
      '$data.$type': 'Post',
      name: 'Test',
      $id: 'posts/1',
    }
    const result = transformFilterForShredding(filter, configs)
    expect(result).toEqual({
      '$data.typed_value.$type.typed_value': 'Post',
      name: 'Test',
      $id: 'posts/1',
    })
  })

  it('should handle operator objects as values', () => {
    const filter = { '$data.$type': { $in: ['Post', 'Comment'] } }
    const result = transformFilterForShredding(filter, configs)
    expect(result).toEqual({
      '$data.typed_value.$type.typed_value': { $in: ['Post', 'Comment'] },
    })
  })

  it('should transform filters inside $and', () => {
    const filter = {
      $and: [
        { '$data.$type': 'Post' },
        { '$data.status': 'published' },
      ],
    }
    const result = transformFilterForShredding(filter, configs)
    expect(result).toEqual({
      $and: [
        { '$data.typed_value.$type.typed_value': 'Post' },
        { '$data.typed_value.status.typed_value': 'published' },
      ],
    })
  })

  it('should transform filters inside $or', () => {
    const filter = {
      $or: [
        { '$data.$type': 'Post' },
        { '$data.$type': 'Comment' },
      ],
    }
    const result = transformFilterForShredding(filter, configs)
    expect(result).toEqual({
      $or: [
        { '$data.typed_value.$type.typed_value': 'Post' },
        { '$data.typed_value.$type.typed_value': 'Comment' },
      ],
    })
  })

  it('should transform filters inside $not', () => {
    const filter = {
      $not: { '$data.$type': 'Draft' },
    }
    const result = transformFilterForShredding(filter, configs)
    expect(result).toEqual({
      $not: { '$data.typed_value.$type.typed_value': 'Draft' },
    })
  })

  it('should handle nested logical operators', () => {
    const filter = {
      $and: [
        {
          $or: [
            { '$data.$type': 'Post' },
            { '$data.$type': 'Article' },
          ],
        },
        { '$data.status': 'published' },
      ],
    }
    const result = transformFilterForShredding(filter, configs)
    expect(result).toEqual({
      $and: [
        {
          $or: [
            { '$data.typed_value.$type.typed_value': 'Post' },
            { '$data.typed_value.$type.typed_value': 'Article' },
          ],
        },
        { '$data.typed_value.status.typed_value': 'published' },
      ],
    })
  })

  it('should handle empty filter', () => {
    const result = transformFilterForShredding({}, configs)
    expect(result).toEqual({})
  })

  it('should handle empty configs', () => {
    const filter = { '$data.$type': 'Post' }
    const result = transformFilterForShredding(filter, [])
    expect(result).toEqual({ '$data.$type': 'Post' })
  })
})

// =============================================================================
// canPushdownWithShredding Tests
// =============================================================================

describe('canPushdownWithShredding', () => {
  const configs: ShreddedVariantReadConfig[] = [
    { columnName: '$data', shredFields: ['$type', 'status'] },
  ]

  it('should return true for $data.$type filter', () => {
    const filter = { '$data.$type': 'Post' }
    expect(canPushdownWithShredding(filter, configs)).toBe(true)
  })

  it('should return true for $data.status filter', () => {
    const filter = { '$data.status': 'published' }
    expect(canPushdownWithShredding(filter, configs)).toBe(true)
  })

  it('should return false for non-shredded field', () => {
    const filter = { '$data.title': 'Test' }
    expect(canPushdownWithShredding(filter, configs)).toBe(false)
  })

  it('should return false for top-level fields', () => {
    const filter = { name: 'Test', $id: 'posts/1' }
    expect(canPushdownWithShredding(filter, configs)).toBe(false)
  })

  it('should return true when any condition in $and targets shredded field', () => {
    const filter = {
      $and: [
        { name: 'Test' },
        { '$data.$type': 'Post' },
      ],
    }
    expect(canPushdownWithShredding(filter, configs)).toBe(true)
  })

  it('should return true when any condition in $or targets shredded field', () => {
    const filter = {
      $or: [
        { name: 'Test' },
        { '$data.$type': 'Post' },
      ],
    }
    expect(canPushdownWithShredding(filter, configs)).toBe(true)
  })

  it('should return true when $not contains shredded field', () => {
    const filter = {
      $not: { '$data.$type': 'Draft' },
    }
    expect(canPushdownWithShredding(filter, configs)).toBe(true)
  })

  it('should return false for empty filter', () => {
    expect(canPushdownWithShredding({}, configs)).toBe(false)
  })

  it('should return false for empty configs', () => {
    const filter = { '$data.$type': 'Post' }
    expect(canPushdownWithShredding(filter, [])).toBe(false)
  })
})

// =============================================================================
// createShreddedPredicate Tests
// =============================================================================

describe('createShreddedPredicate', () => {
  describe('direct value (equality)', () => {
    it('should return true when value is within range', () => {
      const pred = createShreddedPredicate('Post')
      expect(pred('Comment', 'User')).toBe(true) // 'Post' is between 'Comment' and 'User'
    })

    it('should return true when value equals min', () => {
      const pred = createShreddedPredicate('Post')
      expect(pred('Post', 'User')).toBe(true)
    })

    it('should return true when value equals max', () => {
      const pred = createShreddedPredicate('Post')
      expect(pred('Comment', 'Post')).toBe(true)
    })

    it('should return false when value is below range', () => {
      const pred = createShreddedPredicate('Article')
      expect(pred('Comment', 'User')).toBe(false) // 'Article' < 'Comment'
    })

    it('should return false when value is above range', () => {
      const pred = createShreddedPredicate('Zebra')
      expect(pred('Article', 'Post')).toBe(false) // 'Zebra' > 'Post'
    })
  })

  describe('$eq operator', () => {
    it('should check if value is within range', () => {
      const pred = createShreddedPredicate({ $eq: 'Post' })
      expect(pred('Comment', 'User')).toBe(true)
      expect(pred('User', 'Zebra')).toBe(false)
    })
  })

  describe('$in operator', () => {
    it('should return true if any value in array is within range', () => {
      const pred = createShreddedPredicate({ $in: ['Post', 'Comment'] })
      expect(pred('Article', 'Draft')).toBe(true) // 'Comment' is in range
    })

    it('should return false if no value in array is within range', () => {
      const pred = createShreddedPredicate({ $in: ['Post', 'User'] })
      expect(pred('Article', 'Draft')).toBe(false)
    })

    it('should return false for empty $in array', () => {
      const pred = createShreddedPredicate({ $in: [] })
      expect(pred('Article', 'Zebra')).toBe(false)
    })
  })

  describe('$gt operator', () => {
    it('should return true when max > value', () => {
      const pred = createShreddedPredicate({ $gt: 50 })
      expect(pred(0, 100)).toBe(true)
    })

    it('should return false when max <= value', () => {
      const pred = createShreddedPredicate({ $gt: 50 })
      expect(pred(0, 50)).toBe(false)
      expect(pred(0, 49)).toBe(false)
    })
  })

  describe('$gte operator', () => {
    it('should return true when max >= value', () => {
      const pred = createShreddedPredicate({ $gte: 50 })
      expect(pred(0, 100)).toBe(true)
      expect(pred(0, 50)).toBe(true)
    })

    it('should return false when max < value', () => {
      const pred = createShreddedPredicate({ $gte: 50 })
      expect(pred(0, 49)).toBe(false)
    })
  })

  describe('$lt operator', () => {
    it('should return true when min < value', () => {
      const pred = createShreddedPredicate({ $lt: 50 })
      expect(pred(0, 100)).toBe(true)
    })

    it('should return false when min >= value', () => {
      const pred = createShreddedPredicate({ $lt: 50 })
      expect(pred(50, 100)).toBe(false)
      expect(pred(51, 100)).toBe(false)
    })
  })

  describe('$lte operator', () => {
    it('should return true when min <= value', () => {
      const pred = createShreddedPredicate({ $lte: 50 })
      expect(pred(0, 100)).toBe(true)
      expect(pred(50, 100)).toBe(true)
    })

    it('should return false when min > value', () => {
      const pred = createShreddedPredicate({ $lte: 50 })
      expect(pred(51, 100)).toBe(false)
    })
  })

  describe('combined operators', () => {
    it('should handle $gte and $lte together', () => {
      const pred = createShreddedPredicate({ $gte: 20, $lte: 80 })
      expect(pred(10, 90)).toBe(true) // overlap
      expect(pred(0, 19)).toBe(false) // max < gte
      expect(pred(81, 100)).toBe(false) // min > lte
    })

    it('should handle $gt and $lt together', () => {
      const pred = createShreddedPredicate({ $gt: 20, $lt: 80 })
      expect(pred(10, 90)).toBe(true)
      expect(pred(0, 20)).toBe(false) // max <= gt
      expect(pred(80, 100)).toBe(false) // min >= lt
    })
  })

  describe('null condition', () => {
    it('should handle null as direct value', () => {
      const pred = createShreddedPredicate(null)
      // null is treated as direct comparison
      expect(pred).toBeDefined()
    })
  })
})

// =============================================================================
// shouldSkipRowGroup Tests
// =============================================================================

describe('shouldSkipRowGroup', () => {
  it('should return true when filter value is outside statistics range', () => {
    const filter = { '$data.typed_value.$type.typed_value': 'Zebra' }
    const statistics = new Map([
      ['$data.typed_value.$type.typed_value', { min: 'Article', max: 'Post' }],
    ])

    expect(shouldSkipRowGroup(filter, statistics)).toBe(true)
  })

  it('should return false when filter value is within statistics range', () => {
    const filter = { '$data.typed_value.$type.typed_value': 'Post' }
    const statistics = new Map([
      ['$data.typed_value.$type.typed_value', { min: 'Article', max: 'User' }],
    ])

    expect(shouldSkipRowGroup(filter, statistics)).toBe(false)
  })

  it('should return false when no statistics are available', () => {
    const filter = { '$data.typed_value.$type.typed_value': 'Post' }
    const statistics = new Map<string, { min: unknown; max: unknown }>()

    expect(shouldSkipRowGroup(filter, statistics)).toBe(false)
  })

  it('should skip logical operators', () => {
    const filter = { $and: [{ '$data.typed_value.$type.typed_value': 'Post' }] }
    const statistics = new Map([
      ['$data.typed_value.$type.typed_value', { min: 'User', max: 'Zebra' }],
    ])

    // $and is skipped, so the predicate inside is not evaluated at top level
    expect(shouldSkipRowGroup(filter, statistics)).toBe(false)
  })

  it('should handle multiple filter conditions', () => {
    const filter = {
      '$data.typed_value.$type.typed_value': 'Post',
      '$data.typed_value.status.typed_value': 'published',
    }
    const statistics = new Map([
      ['$data.typed_value.$type.typed_value', { min: 'Article', max: 'User' }],
      ['$data.typed_value.status.typed_value', { min: 'active', max: 'draft' }],
    ])

    // 'published' is outside 'active'-'draft' range
    expect(shouldSkipRowGroup(filter, statistics)).toBe(true)
  })

  it('should return false when all conditions might match', () => {
    const filter = {
      '$data.typed_value.$type.typed_value': 'Post',
      '$data.typed_value.status.typed_value': 'draft',
    }
    const statistics = new Map([
      ['$data.typed_value.$type.typed_value', { min: 'Article', max: 'User' }],
      ['$data.typed_value.status.typed_value', { min: 'active', max: 'published' }],
    ])

    expect(shouldSkipRowGroup(filter, statistics)).toBe(false)
  })

  it('should handle $in operator in filter', () => {
    const filter = {
      '$data.typed_value.$type.typed_value': { $in: ['Zebra', 'Yaml'] },
    }
    const statistics = new Map([
      ['$data.typed_value.$type.typed_value', { min: 'Article', max: 'Post' }],
    ])

    // Neither 'Zebra' nor 'Yaml' is in 'Article'-'Post' range
    expect(shouldSkipRowGroup(filter, statistics)).toBe(true)
  })

  it('should handle range operators in filter', () => {
    const filter = {
      '$data.typed_value.views.typed_value': { $gt: 1000 },
    }
    const statistics = new Map([
      ['$data.typed_value.views.typed_value', { min: 0, max: 100 }],
    ])

    // max=100 is not > 1000
    expect(shouldSkipRowGroup(filter, statistics)).toBe(true)
  })
})
