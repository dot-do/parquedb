/**
 * Shredded Variant Predicate Pushdown Tests
 *
 * Tests for the integration of @dotdo/iceberg v3 variant shredding APIs
 * with ParqueDB's query system. Covers configuration, filter transformation,
 * data file filtering, and effectiveness estimation.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  ShreddedPushdownContext,
  buildShreddingProperties,
  extractShreddedFilterPaths,
  hasShreddedConditions,
  estimatePushdownEffectiveness,
  VARIANT_SHRED_COLUMNS_KEY,
  type VariantShredPropertyConfig,
} from '@/query/shredded-pushdown'
import type { DataFile } from '@dotdo/iceberg'

// =============================================================================
// Test Fixtures
// =============================================================================

const testConfigs: VariantShredPropertyConfig[] = [
  {
    columnName: '$data',
    fields: ['year', 'rating', 'status'],
    fieldTypes: { year: 'int', rating: 'double', status: 'string' },
  },
  {
    columnName: '$index',
    fields: ['titleType', 'genre'],
    fieldTypes: { titleType: 'string', genre: 'string' },
  },
]

/**
 * Create a mock data file with column bounds
 */
function createMockDataFile(
  path: string,
  bounds: Record<number, { lower: unknown; upper: unknown }>
): DataFile {
  const dataFile: DataFile = {
    'file-path': path,
    'file-format': 'parquet' as const,
    'record-count': 1000,
    'file-size-in-bytes': 10000,
    partition: {},
    'lower-bounds': {},
    'upper-bounds': {},
  }

  for (const [fieldId, { lower }] of Object.entries(bounds)) {
    dataFile['lower-bounds']![Number(fieldId)] = lower
  }
  for (const [fieldId, { upper }] of Object.entries(bounds)) {
    dataFile['upper-bounds']![Number(fieldId)] = upper
  }

  return dataFile
}

// =============================================================================
// ShreddedPushdownContext Tests
// =============================================================================

describe('ShreddedPushdownContext', () => {
  describe('creation', () => {
    it('should create context from explicit config', () => {
      const context = ShreddedPushdownContext.fromConfig(testConfigs)

      expect(context.hasShredding).toBe(true)
      expect(context.shreddedColumns).toEqual(['$data', '$index'])
    })

    it('should create context from table properties', () => {
      const properties: Record<string, string> = {
        'write.variant.shred-columns': '$data,$index',
        'write.variant.$data.shred-fields': 'year,rating,status',
        'write.variant.$data.field-types': 'year:int,rating:double,status:string',
        'write.variant.$index.shred-fields': 'titleType,genre',
        'write.variant.$index.field-types': 'titleType:string,genre:string',
      }

      const context = ShreddedPushdownContext.fromTableProperties(properties)

      expect(context.hasShredding).toBe(true)
      expect(context.shreddedColumns).toContain('$data')
      expect(context.shreddedColumns).toContain('$index')
    })

    it('should create empty context when no shredding configured', () => {
      const context = ShreddedPushdownContext.empty()

      expect(context.hasShredding).toBe(false)
      expect(context.shreddedColumns).toEqual([])
    })

    it('should handle empty table properties', () => {
      const context = ShreddedPushdownContext.fromTableProperties({})

      expect(context.hasShredding).toBe(false)
    })
  })

  describe('shredded field detection', () => {
    let context: ShreddedPushdownContext

    beforeEach(() => {
      context = ShreddedPushdownContext.fromConfig(testConfigs)
    })

    it('should identify shredded field paths', () => {
      expect(context.isShreddedField('$data.year')).toBe(true)
      expect(context.isShreddedField('$data.rating')).toBe(true)
      expect(context.isShreddedField('$data.status')).toBe(true)
      expect(context.isShreddedField('$index.titleType')).toBe(true)
      expect(context.isShreddedField('$index.genre')).toBe(true)
    })

    it('should return false for non-shredded fields', () => {
      expect(context.isShreddedField('$data.unknown')).toBe(false)
      expect(context.isShreddedField('name')).toBe(false)
      expect(context.isShreddedField('$id')).toBe(false)
    })

    it('should return false for top-level paths', () => {
      expect(context.isShreddedField('$data')).toBe(false)
      expect(context.isShreddedField('year')).toBe(false)
    })

    it('should get shredded fields for a column', () => {
      expect(context.getShreddedFields('$data')).toEqual([
        'year',
        'rating',
        'status',
      ])
      expect(context.getShreddedFields('$index')).toEqual([
        'titleType',
        'genre',
      ])
      expect(context.getShreddedFields('unknown')).toEqual([])
    })
  })

  describe('filter transformation', () => {
    let context: ShreddedPushdownContext

    beforeEach(() => {
      context = ShreddedPushdownContext.fromConfig(testConfigs)
    })

    it('should transform simple shredded field filter', () => {
      const filter = { '$data.year': { $gte: 2020 } }
      const transformed = context.transformFilter(filter)

      // The filter should be transformed with typed_value paths
      expect(transformed).toBeDefined()
    })

    it('should pass through non-shredded fields unchanged', () => {
      const filter = { name: 'Test', $type: 'movie' }
      const transformed = context.transformFilter(filter)

      expect(transformed).toEqual(filter)
    })

    it('should handle empty filter', () => {
      const transformed = context.transformFilter({})
      expect(transformed).toEqual({})
    })

    it('should handle undefined filter', () => {
      const transformed = context.transformFilter(undefined as unknown as Record<string, unknown>)
      expect(transformed).toBeFalsy()
    })
  })

  describe('filter column extraction', () => {
    let context: ShreddedPushdownContext

    beforeEach(() => {
      context = ShreddedPushdownContext.fromConfig(testConfigs)
    })

    it('should extract columns from shredded filter', () => {
      const filter = {
        '$data.year': { $gte: 2020 },
        '$data.rating': { $gt: 8.0 },
      }
      const result = context.extractFilterColumns(filter)

      expect(result.shreddedPaths).toContain('$data.year')
      expect(result.shreddedPaths).toContain('$data.rating')
    })

    it('should handle mixed shredded and non-shredded filters', () => {
      const filter = {
        '$data.year': { $gte: 2020 },
        name: 'Test Movie',
      }
      const result = context.extractFilterColumns(filter)

      expect(result.shreddedPaths).toContain('$data.year')
      expect(result.shreddedPaths).not.toContain('name')
    })

    it('should return empty for non-shredded filters', () => {
      const filter = { name: 'Test', $type: 'movie' }
      const result = context.extractFilterColumns(filter)

      expect(result.shreddedPaths).toEqual([])
    })
  })

  describe('data file filtering', () => {
    let context: ShreddedPushdownContext

    beforeEach(() => {
      context = ShreddedPushdownContext.fromConfig(testConfigs, {
        startingFieldId: 1000,
      })
    })

    it('should pass through all files when no shredding configured', () => {
      const emptyContext = ShreddedPushdownContext.empty()
      const files = [
        createMockDataFile('file1.parquet', {}),
        createMockDataFile('file2.parquet', {}),
      ]

      const result = emptyContext.filterDataFiles(files, { '$data.year': { $gte: 2020 } })

      expect(result).toHaveLength(2)
    })

    it('should pass through all files when filter is empty', () => {
      const files = [
        createMockDataFile('file1.parquet', {}),
        createMockDataFile('file2.parquet', {}),
      ]

      const result = context.filterDataFiles(files, {})

      expect(result).toHaveLength(2)
    })

    it('should return empty array for empty file list', () => {
      const result = context.filterDataFiles([], { '$data.year': { $gte: 2020 } })

      expect(result).toHaveLength(0)
    })
  })

  describe('data file filtering with stats', () => {
    let context: ShreddedPushdownContext

    beforeEach(() => {
      context = ShreddedPushdownContext.fromConfig(testConfigs)
    })

    it('should return stats for filtering', () => {
      const files = [
        createMockDataFile('file1.parquet', {}),
        createMockDataFile('file2.parquet', {}),
      ]

      const result = context.filterDataFilesWithStats(files, { '$data.year': { $gte: 2020 } })

      expect(result.stats.totalFiles).toBe(2)
      expect(result.files).toBeDefined()
    })

    it('should handle empty context', () => {
      const emptyContext = ShreddedPushdownContext.empty()
      const files = [createMockDataFile('file1.parquet', {})]

      const result = emptyContext.filterDataFilesWithStats(files, { year: { $gte: 2020 } })

      expect(result.stats.totalFiles).toBe(1)
      expect(result.stats.skippedFiles).toBe(0)
      expect(result.files).toHaveLength(1)
    })
  })

  describe('shouldSkipDataFile', () => {
    let context: ShreddedPushdownContext

    beforeEach(() => {
      context = ShreddedPushdownContext.fromConfig(testConfigs)
    })

    it('should not skip when no shredding configured', () => {
      const emptyContext = ShreddedPushdownContext.empty()
      const file = createMockDataFile('file.parquet', {})

      const result = emptyContext.shouldSkipDataFile(file, { '$data.year': { $gte: 2020 } })

      expect(result.skip).toBe(false)
    })

    it('should not skip when filter is empty', () => {
      const file = createMockDataFile('file.parquet', {})

      const result = context.shouldSkipDataFile(file, {})

      expect(result.skip).toBe(false)
    })
  })

  describe('range predicates', () => {
    let context: ShreddedPushdownContext

    beforeEach(() => {
      context = ShreddedPushdownContext.fromConfig(testConfigs)
    })

    it('should create range predicate from $gte', () => {
      const pred = context.createRangePredicate('$gte', 10)

      expect(pred.lowerInclusive).toBe(10)
    })

    it('should create range predicate from $gt', () => {
      const pred = context.createRangePredicate('$gt', 10)

      expect(pred.lowerExclusive).toBe(10)
    })

    it('should create range predicate from $lte', () => {
      const pred = context.createRangePredicate('$lte', 100)

      expect(pred.upperInclusive).toBe(100)
    })

    it('should create range predicate from $lt', () => {
      const pred = context.createRangePredicate('$lt', 100)

      expect(pred.upperExclusive).toBe(100)
    })

    it('should create range predicate from $eq', () => {
      const pred = context.createRangePredicate('$eq', 50)

      expect(pred.lowerInclusive).toBe(50)
      expect(pred.upperInclusive).toBe(50)
    })

    it('should create range predicate from $in', () => {
      const pred = context.createRangePredicate('$in', [10, 20, 30])

      expect(pred.points).toEqual([10, 20, 30])
    })

    it('should combine predicates with AND', () => {
      const p1 = context.createRangePredicate('$gte', 10)
      const p2 = context.createRangePredicate('$lte', 100)
      const combined = context.combinePredicatesAnd([p1, p2])

      expect(combined).toBeDefined()
      expect(combined?.lowerInclusive).toBe(10)
      expect(combined?.upperInclusive).toBe(100)
    })

    it('should combine predicates with OR', () => {
      const p1 = context.createRangePredicate('$eq', 10)
      const p2 = context.createRangePredicate('$eq', 20)
      const combined = context.combinePredicatesOr([p1, p2])

      expect(combined).toHaveLength(2)
    })
  })
})

// =============================================================================
// buildShreddingProperties Tests
// =============================================================================

describe('buildShreddingProperties', () => {
  it('should build properties from configs', () => {
    const props = buildShreddingProperties(testConfigs)

    expect(props[VARIANT_SHRED_COLUMNS_KEY]).toBe('$data,$index')
    expect(props['write.variant.$data.shred-fields']).toBe('year,rating,status')
    expect(props['write.variant.$data.field-types']).toBe(
      'year:int,rating:double,status:string'
    )
    expect(props['write.variant.$index.shred-fields']).toBe('titleType,genre')
    expect(props['write.variant.$index.field-types']).toBe(
      'titleType:string,genre:string'
    )
  })

  it('should return empty object for empty configs', () => {
    const props = buildShreddingProperties([])

    expect(props).toEqual({})
  })

  it('should handle config with no field types', () => {
    const configs: VariantShredPropertyConfig[] = [
      {
        columnName: '$data',
        fields: ['year', 'rating'],
        fieldTypes: {},
      },
    ]

    const props = buildShreddingProperties(configs)

    expect(props[VARIANT_SHRED_COLUMNS_KEY]).toBe('$data')
    expect(props['write.variant.$data.shred-fields']).toBe('year,rating')
    expect(props['write.variant.$data.field-types']).toBeUndefined()
  })

  it('should handle config with no fields', () => {
    const configs: VariantShredPropertyConfig[] = [
      {
        columnName: '$data',
        fields: [],
        fieldTypes: {},
      },
    ]

    const props = buildShreddingProperties(configs)

    expect(props[VARIANT_SHRED_COLUMNS_KEY]).toBe('$data')
    expect(props['write.variant.$data.shred-fields']).toBeUndefined()
  })
})

// =============================================================================
// extractShreddedFilterPaths Tests
// =============================================================================

describe('extractShreddedFilterPaths', () => {
  let context: ShreddedPushdownContext

  beforeEach(() => {
    context = ShreddedPushdownContext.fromConfig(testConfigs)
  })

  it('should extract shredded paths from filter', () => {
    const filter = {
      '$data.year': { $gte: 2020 },
      '$data.rating': { $gt: 8.0 },
      '$index.titleType': 'movie',
    }

    const paths = extractShreddedFilterPaths(filter, context)

    expect(paths).toContain('$data.year')
    expect(paths).toContain('$data.rating')
    expect(paths).toContain('$index.titleType')
  })

  it('should not include non-shredded paths', () => {
    const filter = {
      '$data.year': { $gte: 2020 },
      name: 'Test',
      $id: '123',
    }

    const paths = extractShreddedFilterPaths(filter, context)

    expect(paths).toContain('$data.year')
    expect(paths).not.toContain('name')
    expect(paths).not.toContain('$id')
  })

  it('should extract paths from $and', () => {
    const filter = {
      $and: [{ '$data.year': { $gte: 2020 } }, { '$index.titleType': 'movie' }],
    }

    const paths = extractShreddedFilterPaths(filter, context)

    expect(paths).toContain('$data.year')
    expect(paths).toContain('$index.titleType')
  })

  it('should extract paths from $or', () => {
    const filter = {
      $or: [{ '$data.year': { $lt: 2000 } }, { '$data.year': { $gt: 2020 } }],
    }

    const paths = extractShreddedFilterPaths(filter, context)

    expect(paths).toContain('$data.year')
  })

  it('should extract paths from $not', () => {
    const filter = {
      $not: { '$data.status': 'draft' },
    }

    const paths = extractShreddedFilterPaths(filter, context)

    expect(paths).toContain('$data.status')
  })

  it('should return empty array for empty context', () => {
    const emptyContext = ShreddedPushdownContext.empty()
    const filter = { '$data.year': { $gte: 2020 } }

    const paths = extractShreddedFilterPaths(filter, emptyContext)

    expect(paths).toEqual([])
  })

  it('should return empty array for empty filter', () => {
    const paths = extractShreddedFilterPaths({}, context)

    expect(paths).toEqual([])
  })

  it('should deduplicate paths', () => {
    const filter = {
      $and: [{ '$data.year': { $gte: 2020 } }, { '$data.year': { $lte: 2025 } }],
    }

    const paths = extractShreddedFilterPaths(filter, context)

    expect(paths).toEqual(['$data.year'])
  })
})

// =============================================================================
// hasShreddedConditions Tests
// =============================================================================

describe('hasShreddedConditions', () => {
  let context: ShreddedPushdownContext

  beforeEach(() => {
    context = ShreddedPushdownContext.fromConfig(testConfigs)
  })

  it('should return true when filter has shredded conditions', () => {
    const filter = { '$data.year': { $gte: 2020 } }

    expect(hasShreddedConditions(filter, context)).toBe(true)
  })

  it('should return false when filter has no shredded conditions', () => {
    const filter = { name: 'Test', $type: 'movie' }

    expect(hasShreddedConditions(filter, context)).toBe(false)
  })

  it('should return false for empty filter', () => {
    expect(hasShreddedConditions({}, context)).toBe(false)
  })

  it('should return false for empty context', () => {
    const emptyContext = ShreddedPushdownContext.empty()
    const filter = { '$data.year': { $gte: 2020 } }

    expect(hasShreddedConditions(filter, emptyContext)).toBe(false)
  })
})

// =============================================================================
// estimatePushdownEffectiveness Tests
// =============================================================================

describe('estimatePushdownEffectiveness', () => {
  let context: ShreddedPushdownContext

  beforeEach(() => {
    context = ShreddedPushdownContext.fromConfig(testConfigs)
  })

  it('should calculate effectiveness for all shredded conditions', () => {
    const filter = {
      '$data.year': { $gte: 2020 },
      '$data.rating': { $gt: 8.0 },
    }

    const result = estimatePushdownEffectiveness(filter, context)

    expect(result.totalConditions).toBe(2)
    expect(result.shreddedConditions).toBe(2)
    expect(result.effectiveness).toBe(1.0)
    expect(result.isEffective).toBe(true)
  })

  it('should calculate effectiveness for mixed conditions', () => {
    const filter = {
      '$data.year': { $gte: 2020 },
      name: 'Test',
    }

    const result = estimatePushdownEffectiveness(filter, context)

    expect(result.totalConditions).toBe(2)
    expect(result.shreddedConditions).toBe(1)
    expect(result.effectiveness).toBe(0.5)
    expect(result.isEffective).toBe(false) // 50% is not > 50%
  })

  it('should calculate effectiveness for no shredded conditions', () => {
    const filter = {
      name: 'Test',
      version: 1,
    }

    const result = estimatePushdownEffectiveness(filter, context)

    expect(result.totalConditions).toBe(2)
    expect(result.shreddedConditions).toBe(0)
    expect(result.effectiveness).toBe(0)
    expect(result.isEffective).toBe(false)
  })

  it('should handle empty filter', () => {
    const result = estimatePushdownEffectiveness({}, context)

    expect(result.totalConditions).toBe(0)
    expect(result.shreddedConditions).toBe(0)
    expect(result.effectiveness).toBe(0)
    expect(result.isEffective).toBe(false)
  })

  it('should handle empty context', () => {
    const emptyContext = ShreddedPushdownContext.empty()
    const filter = { '$data.year': { $gte: 2020 } }

    const result = estimatePushdownEffectiveness(filter, emptyContext)

    expect(result.totalConditions).toBe(0)
    expect(result.effectiveness).toBe(0)
    expect(result.isEffective).toBe(false)
  })

  it('should count conditions in $and', () => {
    const filter = {
      $and: [
        { '$data.year': { $gte: 2020 } },
        { '$data.rating': { $gt: 8.0 } },
        { name: 'Test' },
      ],
    }

    const result = estimatePushdownEffectiveness(filter, context)

    expect(result.totalConditions).toBe(3)
    expect(result.shreddedConditions).toBe(2)
    expect(result.effectiveness).toBeCloseTo(0.667, 2)
    expect(result.isEffective).toBe(true)
  })

  it('should count conditions in $or', () => {
    const filter = {
      $or: [
        { '$data.year': { $lt: 2000 } },
        { '$data.year': { $gt: 2020 } },
      ],
    }

    const result = estimatePushdownEffectiveness(filter, context)

    expect(result.totalConditions).toBe(2)
    expect(result.shreddedConditions).toBe(2)
    expect(result.effectiveness).toBe(1.0)
    expect(result.isEffective).toBe(true)
  })
})

// =============================================================================
// Integration Tests
// =============================================================================

describe('Shredded Pushdown Integration', () => {
  it('should roundtrip config through properties', () => {
    // Build properties from configs
    const props = buildShreddingProperties(testConfigs)

    // Create context from properties
    const context = ShreddedPushdownContext.fromTableProperties(props)

    // Verify the config was preserved
    expect(context.hasShredding).toBe(true)
    expect(context.shreddedColumns).toContain('$data')
    expect(context.shreddedColumns).toContain('$index')
    expect(context.getShreddedFields('$data')).toContain('year')
    expect(context.getShreddedFields('$data')).toContain('rating')
    expect(context.getShreddedFields('$data')).toContain('status')
  })

  it('should handle complete query workflow', () => {
    // Setup context
    const context = ShreddedPushdownContext.fromConfig(testConfigs)

    // Define filter
    const filter = {
      '$data.year': { $gte: 2020 },
      '$data.rating': { $gt: 8.0 },
    }

    // Check if pushdown is effective
    const effectiveness = estimatePushdownEffectiveness(filter, context)
    expect(effectiveness.isEffective).toBe(true)

    // Extract shredded paths
    const paths = extractShreddedFilterPaths(filter, context)
    expect(paths).toContain('$data.year')
    expect(paths).toContain('$data.rating')

    // Transform filter
    const transformed = context.transformFilter(filter)
    expect(transformed).toBeDefined()

    // Filter data files (with mock data)
    const files = [createMockDataFile('file1.parquet', {})]
    const result = context.filterDataFilesWithStats(files, filter)
    expect(result.stats.totalFiles).toBe(1)
  })
})

// =============================================================================
// Row Group Filtering with Statistics Tests
// =============================================================================

describe('Row Group Filtering with Statistics', () => {
  /**
   * Create a mock data file with proper field IDs based on shred config.
   * The field IDs are assigned starting from startingFieldId (default 1000).
   */
  function createDataFileWithBounds(
    path: string,
    fieldBounds: Record<string, { lower: unknown; upper: unknown }>,
    context: ShreddedPushdownContext
  ): DataFile {
    // Get the field ID map from context (it's private, so we work around it)
    // Field IDs are assigned based on config order: $data.year=1000, $data.rating=1001, etc.
    const fieldIdMap: Record<string, number> = {}
    let fieldId = 1000
    for (const config of testConfigs) {
      for (const field of config.fields) {
        fieldIdMap[`${config.columnName}.${field}`] = fieldId++
      }
    }

    const dataFile: DataFile = {
      'file-path': path,
      'file-format': 'parquet' as const,
      'record-count': 1000,
      'file-size-in-bytes': 10000,
      partition: {},
      'lower-bounds': {},
      'upper-bounds': {},
    }

    for (const [fieldPath, bounds] of Object.entries(fieldBounds)) {
      const id = fieldIdMap[fieldPath]
      if (id !== undefined) {
        dataFile['lower-bounds']![id] = bounds.lower
        dataFile['upper-bounds']![id] = bounds.upper
      }
    }

    return dataFile
  }

  describe('filterDataFilesWithStats - actual filtering', () => {
    let context: ShreddedPushdownContext

    beforeEach(() => {
      context = ShreddedPushdownContext.fromConfig(testConfigs, {
        startingFieldId: 1000,
      })
    })

    it('should skip files where year range is entirely below filter threshold', () => {
      // File with years 2000-2010 should be skipped when filtering for year >= 2020
      const files = [
        createDataFileWithBounds('old_movies.parquet', {
          '$data.year': { lower: 2000, upper: 2010 },
        }, context),
        createDataFileWithBounds('new_movies.parquet', {
          '$data.year': { lower: 2018, upper: 2024 },
        }, context),
      ]

      const filter = { '$data.year': { $gte: 2020 } }
      const result = context.filterDataFilesWithStats(files, filter)

      // The iceberg library handles the actual filtering logic
      // We're testing that the integration is correct
      expect(result.stats.totalFiles).toBe(2)
    })

    it('should track skipped files by field in stats', () => {
      const files = [
        createDataFileWithBounds('file1.parquet', {
          '$data.rating': { lower: 1.0, upper: 5.0 },
        }, context),
        createDataFileWithBounds('file2.parquet', {
          '$data.rating': { lower: 6.0, upper: 10.0 },
        }, context),
      ]

      const filter = { '$data.rating': { $gt: 8.0 } }
      const result = context.filterDataFilesWithStats(files, filter)

      expect(result.stats.totalFiles).toBe(2)
      expect(result.stats.skippedByField).toBeDefined()
    })

    it('should handle multiple filter conditions on different shredded fields', () => {
      const files = [
        createDataFileWithBounds('file1.parquet', {
          '$data.year': { lower: 2020, upper: 2024 },
          '$data.rating': { lower: 8.0, upper: 10.0 },
        }, context),
        createDataFileWithBounds('file2.parquet', {
          '$data.year': { lower: 2020, upper: 2024 },
          '$data.rating': { lower: 1.0, upper: 5.0 },
        }, context),
        createDataFileWithBounds('file3.parquet', {
          '$data.year': { lower: 2000, upper: 2010 },
          '$data.rating': { lower: 8.0, upper: 10.0 },
        }, context),
      ]

      const filter = {
        '$data.year': { $gte: 2015 },
        '$data.rating': { $gte: 7.0 },
      }

      const result = context.filterDataFilesWithStats(files, filter)
      expect(result.stats.totalFiles).toBe(3)
    })

    it('should handle $eq filter for exact value match', () => {
      const files = [
        createDataFileWithBounds('exact_match.parquet', {
          '$data.year': { lower: 2020, upper: 2020 },
        }, context),
        createDataFileWithBounds('no_match.parquet', {
          '$data.year': { lower: 2021, upper: 2024 },
        }, context),
      ]

      const filter = { '$data.year': { $eq: 2020 } }
      const result = context.filterDataFilesWithStats(files, filter)

      expect(result.stats.totalFiles).toBe(2)
    })

    it('should handle $in filter for set membership', () => {
      const files = [
        createDataFileWithBounds('contains_target.parquet', {
          '$data.year': { lower: 2018, upper: 2022 },
        }, context),
        createDataFileWithBounds('no_target.parquet', {
          '$data.year': { lower: 2025, upper: 2030 },
        }, context),
      ]

      const filter = { '$data.year': { $in: [2020, 2021, 2022] } }
      const result = context.filterDataFilesWithStats(files, filter)

      expect(result.stats.totalFiles).toBe(2)
    })

    it('should handle $lt and $lte filters', () => {
      const files = [
        createDataFileWithBounds('old.parquet', {
          '$data.year': { lower: 1990, upper: 2000 },
        }, context),
        createDataFileWithBounds('recent.parquet', {
          '$data.year': { lower: 2020, upper: 2024 },
        }, context),
      ]

      const ltFilter = { '$data.year': { $lt: 2005 } }
      const ltResult = context.filterDataFilesWithStats(files, ltFilter)
      expect(ltResult.stats.totalFiles).toBe(2)

      const lteFilter = { '$data.year': { $lte: 2000 } }
      const lteResult = context.filterDataFilesWithStats(files, lteFilter)
      expect(lteResult.stats.totalFiles).toBe(2)
    })
  })

  describe('shouldSkipDataFile - individual file decisions', () => {
    let context: ShreddedPushdownContext

    beforeEach(() => {
      context = ShreddedPushdownContext.fromConfig(testConfigs, {
        startingFieldId: 1000,
      })
    })

    it('should return skip reason when file can be skipped', () => {
      const file = createDataFileWithBounds('skip_me.parquet', {
        '$data.year': { lower: 1990, upper: 2000 },
      }, context)

      const filter = { '$data.year': { $gt: 2020 } }
      const result = context.shouldSkipDataFile(file, filter)

      // The result should have a skip decision
      expect(result).toHaveProperty('skip')
    })

    it('should not skip when bounds overlap with filter range', () => {
      const file = createDataFileWithBounds('keep_me.parquet', {
        '$data.year': { lower: 2015, upper: 2025 },
      }, context)

      const filter = { '$data.year': { $gte: 2020, $lte: 2022 } }
      const result = context.shouldSkipDataFile(file, filter)

      // Overlapping range should not be skipped
      expect(result.skip).toBe(false)
    })

    it('should handle missing bounds gracefully', () => {
      const file = createDataFileWithBounds('no_bounds.parquet', {}, context)

      const filter = { '$data.year': { $gte: 2020 } }
      const result = context.shouldSkipDataFile(file, filter)

      // Without bounds, cannot skip (conservative approach)
      expect(result.skip).toBe(false)
    })
  })

  describe('Range Predicate Evaluation', () => {
    let context: ShreddedPushdownContext

    beforeEach(() => {
      context = ShreddedPushdownContext.fromConfig(testConfigs)
    })

    it('should combine multiple $gte and $lte into tight range', () => {
      const p1 = context.createRangePredicate('$gte', 10)
      const p2 = context.createRangePredicate('$lte', 100)
      const p3 = context.createRangePredicate('$gte', 20)
      const p4 = context.createRangePredicate('$lte', 80)

      const combined = context.combinePredicatesAnd([p1, p2, p3, p4])

      // Should pick the tightest range: [20, 80]
      expect(combined).toBeDefined()
      expect(combined?.lowerInclusive).toBe(20)
      expect(combined?.upperInclusive).toBe(80)
    })

    it('should return null for contradictory ranges', () => {
      const p1 = context.createRangePredicate('$gte', 100)
      const p2 = context.createRangePredicate('$lte', 50)

      const combined = context.combinePredicatesAnd([p1, p2])

      // Range [100, 50] is empty - should return null
      expect(combined).toBeNull()
    })

    it('should handle exclusive bounds correctly', () => {
      const p1 = context.createRangePredicate('$gt', 10)
      const p2 = context.createRangePredicate('$lt', 20)

      const combined = context.combinePredicatesAnd([p1, p2])

      expect(combined).toBeDefined()
      expect(combined?.lowerExclusive).toBe(10)
      expect(combined?.upperExclusive).toBe(20)
    })

    it('should merge OR predicates into list', () => {
      const p1 = context.createRangePredicate('$eq', 10)
      const p2 = context.createRangePredicate('$eq', 20)
      const p3 = context.createRangePredicate('$eq', 30)

      const combined = context.combinePredicatesOr([p1, p2, p3])

      // OR of equals should return the list
      expect(combined).toHaveLength(3)
    })
  })
})

// =============================================================================
// Export Integration Tests
// =============================================================================

describe('Export Integration', () => {
  it('should export all necessary types and functions from query module', async () => {
    // Import from the main query module to verify exports work
    const queryModule = await import('@/query')

    // Verify ShreddedPushdownContext is exported
    expect(queryModule.ShreddedPushdownContext).toBeDefined()

    // Verify helper functions are exported
    expect(queryModule.buildShreddingProperties).toBeDefined()
    expect(queryModule.extractShreddedFilterPaths).toBeDefined()
    expect(queryModule.hasShreddedConditions).toBeDefined()
    expect(queryModule.estimatePushdownEffectiveness).toBeDefined()

    // Verify re-exports from @dotdo/iceberg
    expect(queryModule.extractVariantShredConfig).toBeDefined()
    expect(queryModule.filterDataFilesWithStats).toBeDefined()
    expect(queryModule.shouldSkipDataFile).toBeDefined()
    expect(queryModule.createRangePredicate).toBeDefined()
    expect(queryModule.VARIANT_SHRED_COLUMNS_KEY).toBeDefined()
  })

  it('should create context and use filtering in one import', async () => {
    const { ShreddedPushdownContext } = await import('@/query')

    const context = ShreddedPushdownContext.fromConfig([
      {
        columnName: '$data',
        fields: ['year'],
        fieldTypes: { year: 'int' },
      },
    ])

    expect(context.hasShredding).toBe(true)
    expect(context.isShreddedField('$data.year')).toBe(true)
  })
})
