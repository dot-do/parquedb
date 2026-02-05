/**
 * Tests for $data variant column read optimization
 *
 * When using $data variant schema (full row stored as JSON in one column),
 * we should only read $id and $data columns, not reassemble from many columns.
 *
 * @see parquedb-1so4: Optimize $data variant reads for SELECT *
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { MemoryBackend } from '@/storage/MemoryBackend'
import { ParquetReader } from '@/parquet/reader'
import { ParquetWriter } from '@/parquet/writer'
import type { ParquetSchema } from '@/parquet/types'
import {
  detectDataVariantSchema,
  getDataVariantColumns,
  reconstructEntityFromDataVariant,
} from '@/parquet/data-variant'

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Create a test Parquet file with $id + $data variant schema
 */
async function createDataVariantParquet(
  storage: MemoryBackend,
  path: string,
  entities: Array<{ $id: string; [key: string]: unknown }>
): Promise<void> {
  // Create rows with $id and $data columns
  const rows = entities.map((entity) => ({
    $id: entity.$id,
    $data: JSON.stringify(entity),
  }))

  const schema: ParquetSchema = {
    $id: { type: 'STRING', optional: false },
    $data: { type: 'STRING', optional: false },
  }

  const writer = new ParquetWriter(storage)
  await writer.write(path, rows, schema)
}

/**
 * Create a test Parquet file with multi-column schema (no $data)
 */
async function createMultiColumnParquet(
  storage: MemoryBackend,
  path: string,
  entities: Array<{ $id: string; name: string; age: number; email: string }>
): Promise<void> {
  const schema: ParquetSchema = {
    $id: { type: 'STRING', optional: false },
    name: { type: 'STRING', optional: false },
    age: { type: 'INT64', optional: false },
    email: { type: 'STRING', optional: true },
  }

  const writer = new ParquetWriter(storage)
  await writer.write(path, entities, schema)
}

/**
 * Create a test Parquet file with hybrid schema ($id + $data + shredded columns)
 */
async function createHybridParquet(
  storage: MemoryBackend,
  path: string,
  entities: Array<{ $id: string; status: string; [key: string]: unknown }>
): Promise<void> {
  const rows = entities.map((entity) => ({
    $id: entity.$id,
    $data: JSON.stringify(entity),
    status: entity.status, // Shredded column for predicate pushdown
  }))

  const schema: ParquetSchema = {
    $id: { type: 'STRING', optional: false },
    $data: { type: 'STRING', optional: false },
    status: { type: 'STRING', optional: false },
  }

  const writer = new ParquetWriter(storage)
  await writer.write(path, rows, schema)
}

// =============================================================================
// Schema Detection Tests
// =============================================================================

describe('$data Variant Schema Detection', () => {
  describe('detectDataVariantSchema', () => {
    it('should detect schema with $id and $data columns', () => {
      const schema = {
        $id: { type: 'STRING' as const, optional: false },
        $data: { type: 'STRING' as const, optional: false },
      }

      expect(detectDataVariantSchema(schema)).toBe(true)
    })

    it('should detect hybrid schema with $id, $data, and additional columns', () => {
      const schema = {
        $id: { type: 'STRING' as const, optional: false },
        $data: { type: 'STRING' as const, optional: false },
        status: { type: 'STRING' as const, optional: false },
        $index_category: { type: 'STRING' as const, optional: true },
      }

      expect(detectDataVariantSchema(schema)).toBe(true)
    })

    it('should return false for multi-column schema without $data', () => {
      const schema = {
        $id: { type: 'STRING' as const, optional: false },
        name: { type: 'STRING' as const, optional: false },
        age: { type: 'INT64' as const, optional: false },
      }

      expect(detectDataVariantSchema(schema)).toBe(false)
    })

    it('should return false for schema with $data but no $id', () => {
      const schema = {
        id: { type: 'STRING' as const, optional: false },
        $data: { type: 'STRING' as const, optional: false },
      }

      expect(detectDataVariantSchema(schema)).toBe(false)
    })

    it('should return false for empty schema', () => {
      expect(detectDataVariantSchema({})).toBe(false)
    })
  })

  describe('getDataVariantColumns', () => {
    it('should return only $id and $data for basic variant schema', () => {
      const schema = {
        $id: { type: 'STRING' as const, optional: false },
        $data: { type: 'STRING' as const, optional: false },
      }

      const columns = getDataVariantColumns(schema)
      expect(columns).toEqual(['$id', '$data'])
    })

    it('should return $id, $data and filter columns for hybrid schema', () => {
      const schema = {
        $id: { type: 'STRING' as const, optional: false },
        $data: { type: 'STRING' as const, optional: false },
        status: { type: 'STRING' as const, optional: false },
        $index_category: { type: 'STRING' as const, optional: true },
      }

      // For SELECT *, we only need $id and $data
      const columns = getDataVariantColumns(schema)
      expect(columns).toEqual(['$id', '$data'])
    })

    it('should return null for non-variant schema', () => {
      const schema = {
        $id: { type: 'STRING' as const, optional: false },
        name: { type: 'STRING' as const, optional: false },
      }

      const columns = getDataVariantColumns(schema)
      expect(columns).toBeNull()
    })

    it('should include additional requested columns', () => {
      const schema = {
        $id: { type: 'STRING' as const, optional: false },
        $data: { type: 'STRING' as const, optional: false },
        status: { type: 'STRING' as const, optional: false },
      }

      // When filtering by status, we need that column too
      const columns = getDataVariantColumns(schema, ['status'])
      expect(columns).toEqual(['$id', '$data', 'status'])
    })
  })
})

// =============================================================================
// Entity Reconstruction Tests
// =============================================================================

describe('Entity Reconstruction from $data', () => {
  describe('reconstructEntityFromDataVariant', () => {
    it('should reconstruct entity from $data JSON string', () => {
      const row = {
        $id: 'user/123',
        $data: JSON.stringify({
          $id: 'user/123',
          name: 'Alice',
          age: 30,
          email: 'alice@example.com',
        }),
      }

      const entity = reconstructEntityFromDataVariant(row)

      expect(entity).toEqual({
        $id: 'user/123',
        name: 'Alice',
        age: 30,
        email: 'alice@example.com',
      })
    })

    it('should use $id from row if not in $data', () => {
      const row = {
        $id: 'user/123',
        $data: JSON.stringify({
          name: 'Alice',
          age: 30,
        }),
      }

      const entity = reconstructEntityFromDataVariant(row)

      expect(entity.$id).toBe('user/123')
      expect(entity.name).toBe('Alice')
    })

    it('should handle $data as object (already parsed)', () => {
      const row = {
        $id: 'user/123',
        $data: {
          $id: 'user/123',
          name: 'Alice',
          age: 30,
        },
      }

      const entity = reconstructEntityFromDataVariant(row)

      expect(entity).toEqual({
        $id: 'user/123',
        name: 'Alice',
        age: 30,
      })
    })

    it('should handle invalid JSON in $data gracefully', () => {
      const row = {
        $id: 'user/123',
        $data: 'not valid json',
      }

      const entity = reconstructEntityFromDataVariant(row)

      // Falls back to returning the row with $id
      expect(entity.$id).toBe('user/123')
    })

    it('should handle null $data', () => {
      const row = {
        $id: 'user/123',
        $data: null,
      }

      const entity = reconstructEntityFromDataVariant(row)

      expect(entity.$id).toBe('user/123')
    })

    it('should preserve row $id over $data $id if different', () => {
      // Row $id should take precedence (it's the indexed column)
      const row = {
        $id: 'user/123',
        $data: JSON.stringify({
          $id: 'user/456', // Different ID in data
          name: 'Alice',
        }),
      }

      const entity = reconstructEntityFromDataVariant(row)

      // Row $id takes precedence
      expect(entity.$id).toBe('user/123')
      expect(entity.name).toBe('Alice')
    })
  })
})

// =============================================================================
// Integration Tests with ParquetReader
// =============================================================================

describe('ParquetReader $data Variant Optimization', () => {
  let storage: MemoryBackend
  let reader: ParquetReader

  beforeEach(() => {
    storage = new MemoryBackend()
    reader = new ParquetReader({ storage })
  })

  it('should read entities from $data variant schema', async () => {
    const entities = [
      { $id: 'user/1', name: 'Alice', age: 30, role: 'admin' },
      { $id: 'user/2', name: 'Bob', age: 25, role: 'user' },
      { $id: 'user/3', name: 'Charlie', age: 35, role: 'user' },
    ]

    await createDataVariantParquet(storage, 'test/data.parquet', entities)

    const result = await reader.read<{ $id: string; name: string; age: number; role: string }>(
      'test/data.parquet'
    )

    // Should have all rows
    expect(result).toHaveLength(3)

    // Raw rows have $id and $data columns
    expect(result[0]).toHaveProperty('$id', 'user/1')
    expect(result[0]).toHaveProperty('$data')
  })

  it('should correctly reconstruct entities when using column projection', async () => {
    const entities = [
      { $id: 'user/1', name: 'Alice', age: 30 },
      { $id: 'user/2', name: 'Bob', age: 25 },
    ]

    await createDataVariantParquet(storage, 'test/data.parquet', entities)

    // Read only $id and $data columns
    const result = await reader.read<{ $id: string; $data: string }>(
      'test/data.parquet',
      { columns: ['$id', '$data'] }
    )

    expect(result).toHaveLength(2)
    expect(result[0]?.$id).toBe('user/1')
    expect(result[0]?.$data).toBeDefined()

    // Parse $data to verify content
    const parsed = JSON.parse(result[0]?.$data ?? '{}')
    expect(parsed.name).toBe('Alice')
    expect(parsed.age).toBe(30)
  })

  it('should read hybrid schema with shredded columns', async () => {
    const entities = [
      { $id: 'post/1', status: 'published', title: 'Hello World', content: 'Test content' },
      { $id: 'post/2', status: 'draft', title: 'Draft Post', content: 'Work in progress' },
    ]

    await createHybridParquet(storage, 'test/data.parquet', entities)

    const result = await reader.read<{ $id: string; $data: string; status: string }>(
      'test/data.parquet'
    )

    expect(result).toHaveLength(2)
    expect(result[0]).toHaveProperty('$id', 'post/1')
    expect(result[0]).toHaveProperty('status', 'published')
    expect(result[0]).toHaveProperty('$data')
  })

  it('should support filtering by shredded column in hybrid schema', async () => {
    const entities = [
      { $id: 'post/1', status: 'published', title: 'Published Post' },
      { $id: 'post/2', status: 'draft', title: 'Draft Post' },
      { $id: 'post/3', status: 'published', title: 'Another Published' },
    ]

    await createHybridParquet(storage, 'test/data.parquet', entities)

    // Read with filter on shredded column
    const result = await reader.read<{ $id: string; $data: string; status: string }>(
      'test/data.parquet',
      {
        filter: {
          column: 'status',
          op: 'eq',
          value: 'published',
        },
      }
    )

    expect(result).toHaveLength(2)
    expect(result.every(r => r.status === 'published')).toBe(true)
  })
})

// =============================================================================
// Performance Comparison Tests
// =============================================================================

describe('$data Variant Read Performance', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  it('should demonstrate column projection reduces data read', async () => {
    // Create a wide table with many columns
    const entities = Array.from({ length: 100 }, (_, i) => ({
      $id: `user/${i}`,
      name: `User ${i}`,
      email: `user${i}@example.com`,
      age: 20 + (i % 50),
      address: `${i} Main Street`,
      city: `City ${i % 10}`,
      country: 'USA',
      phone: `555-${String(i).padStart(4, '0')}`,
      department: `Dept ${i % 5}`,
      salary: 50000 + (i * 1000),
    }))

    await createDataVariantParquet(storage, 'test/wide.parquet', entities)

    const reader = new ParquetReader({ storage })

    // Read all columns
    const fullRead = await reader.read('test/wide.parquet')
    expect(fullRead).toHaveLength(100)

    // Read only $id and $data columns (optimized)
    const projectedRead = await reader.read('test/wide.parquet', {
      columns: ['$id', '$data'],
    })
    expect(projectedRead).toHaveLength(100)

    // Both should have the same data available (via $data)
    const fullFirst = fullRead[0] as { $id: string; $data: string }
    const projectedFirst = projectedRead[0] as { $id: string; $data: string }

    expect(fullFirst.$id).toBe(projectedFirst.$id)
    expect(fullFirst.$data).toBe(projectedFirst.$data)
  })

  it('benchmark: $data variant read vs multi-column reassembly', async () => {
    // Skip in CI - only run locally for benchmarking
    if (process.env.CI) {
      return
    }

    const entityCount = 1000
    const entities = Array.from({ length: entityCount }, (_, i) => ({
      $id: `user/${i}`,
      name: `User ${i}`,
      email: `user${i}@example.com`,
      age: 20 + (i % 50),
    }))

    // Create variant schema file
    await createDataVariantParquet(storage, 'test/variant.parquet', entities)

    // Create multi-column schema file
    await createMultiColumnParquet(storage, 'test/multi.parquet', entities as any)

    const reader = new ParquetReader({ storage })

    // Benchmark variant read (only 2 columns)
    const variantStart = performance.now()
    const variantResult = await reader.read('test/variant.parquet', {
      columns: ['$id', '$data'],
    })
    const variantTime = performance.now() - variantStart

    // Benchmark multi-column read (all columns)
    const multiStart = performance.now()
    const multiResult = await reader.read('test/multi.parquet')
    const multiTime = performance.now() - multiStart

    expect(variantResult).toHaveLength(entityCount)
    expect(multiResult).toHaveLength(entityCount)

    // Log benchmark results
    console.log(`Variant schema read (${entityCount} rows): ${variantTime.toFixed(2)}ms`)
    console.log(`Multi-column schema read (${entityCount} rows): ${multiTime.toFixed(2)}ms`)

    // Variant read should generally be faster due to fewer columns
    // (but this isn't always guaranteed due to JSON parsing overhead)
  })
})
