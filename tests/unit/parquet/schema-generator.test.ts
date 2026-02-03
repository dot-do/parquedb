/**
 * Parquet Schema Generator Tests
 *
 * Tests for the ParquetSchemaGenerator that converts IceType/ParqueDB
 * schema definitions to Parquet column schemas (SchemaTree format).
 *
 * Covers:
 * - Basic type mapping (string, int, float, bool, date, datetime, uuid, json)
 * - Required vs optional fields
 * - $data variant inclusion/exclusion
 * - Array types (stored as JSON)
 * - System columns always present
 * - Audit columns configuration
 * - Schema validation
 */

import { describe, it, expect } from 'vitest'
import {
  iceTypeToParquet,
  generateParquetSchema,
  generateMinimalSchema,
  schemaToColumnSources,
  validateSchemaTree,
  getSchemaColumnNames,
  schemaHasColumn,
  getRequiredColumns,
  getOptionalColumns,
  type SchemaTree,
  type ParquetType,
} from '@/parquet/schema-generator'
import type { TypeDefinition } from '@/types/schema'

// =============================================================================
// Type Mapping Tests
// =============================================================================

describe('iceTypeToParquet', () => {
  describe('string types', () => {
    it('should map string to STRING', () => {
      expect(iceTypeToParquet('string')).toBe('STRING')
    })

    it('should map text to STRING', () => {
      expect(iceTypeToParquet('text')).toBe('STRING')
    })

    it('should map markdown to STRING', () => {
      expect(iceTypeToParquet('markdown')).toBe('STRING')
    })

    it('should map email to STRING', () => {
      expect(iceTypeToParquet('email')).toBe('STRING')
    })

    it('should map url to STRING', () => {
      expect(iceTypeToParquet('url')).toBe('STRING')
    })

    it('should map uuid to STRING', () => {
      expect(iceTypeToParquet('uuid')).toBe('STRING')
    })

    it('should be case-insensitive', () => {
      expect(iceTypeToParquet('STRING')).toBe('STRING')
      expect(iceTypeToParquet('String')).toBe('STRING')
    })
  })

  describe('numeric types', () => {
    it('should map int to INT32', () => {
      expect(iceTypeToParquet('int')).toBe('INT32')
    })

    it('should map integer to INT32', () => {
      expect(iceTypeToParquet('integer')).toBe('INT32')
    })

    it('should map float to DOUBLE', () => {
      expect(iceTypeToParquet('float')).toBe('DOUBLE')
    })

    it('should map double to DOUBLE', () => {
      expect(iceTypeToParquet('double')).toBe('DOUBLE')
    })

    it('should map number to DOUBLE', () => {
      expect(iceTypeToParquet('number')).toBe('DOUBLE')
    })
  })

  describe('boolean type', () => {
    it('should map bool to BOOLEAN', () => {
      expect(iceTypeToParquet('bool')).toBe('BOOLEAN')
    })

    it('should map boolean to BOOLEAN', () => {
      expect(iceTypeToParquet('boolean')).toBe('BOOLEAN')
    })
  })

  describe('date/time types', () => {
    it('should map date to STRING (ISO format)', () => {
      expect(iceTypeToParquet('date')).toBe('STRING')
    })

    it('should map datetime to TIMESTAMP', () => {
      expect(iceTypeToParquet('datetime')).toBe('TIMESTAMP')
    })

    it('should map timestamp to TIMESTAMP', () => {
      expect(iceTypeToParquet('timestamp')).toBe('TIMESTAMP')
    })
  })

  describe('binary/json types', () => {
    it('should map json to JSON', () => {
      expect(iceTypeToParquet('json')).toBe('JSON')
    })

    it('should map binary to BYTE_ARRAY', () => {
      expect(iceTypeToParquet('binary')).toBe('BYTE_ARRAY')
    })
  })

  describe('parametric types', () => {
    it('should map decimal(p,s) to DOUBLE', () => {
      expect(iceTypeToParquet('decimal(10,2)')).toBe('DOUBLE')
      expect(iceTypeToParquet('decimal(18,4)')).toBe('DOUBLE')
    })

    it('should map varchar(n) to STRING', () => {
      expect(iceTypeToParquet('varchar(255)')).toBe('STRING')
      expect(iceTypeToParquet('varchar(50)')).toBe('STRING')
    })

    it('should map char(n) to STRING', () => {
      expect(iceTypeToParquet('char(36)')).toBe('STRING')
      expect(iceTypeToParquet('char(10)')).toBe('STRING')
    })

    it('should map vector(n) to BYTE_ARRAY', () => {
      expect(iceTypeToParquet('vector(1536)')).toBe('BYTE_ARRAY')
      expect(iceTypeToParquet('vector(768)')).toBe('BYTE_ARRAY')
    })

    it('should map enum(...) to STRING', () => {
      expect(iceTypeToParquet('enum(draft,published,archived)')).toBe('STRING')
      expect(iceTypeToParquet('enum(a,b,c)')).toBe('STRING')
    })
  })

  describe('unknown types', () => {
    it('should default to JSON for unknown types', () => {
      expect(iceTypeToParquet('customType')).toBe('JSON')
      expect(iceTypeToParquet('unknownType')).toBe('JSON')
    })
  })
})

// =============================================================================
// Schema Generation Tests
// =============================================================================

describe('generateParquetSchema', () => {
  describe('system columns', () => {
    it('should always include $id column', () => {
      const schema = generateParquetSchema({})
      expect(schema.$id).toEqual({ type: 'STRING', optional: false })
    })

    it('should always include $type column', () => {
      const schema = generateParquetSchema({})
      expect(schema.$type).toEqual({ type: 'STRING', optional: false })
    })

    it('should have $id as required', () => {
      const schema = generateParquetSchema({})
      expect(schema.$id.optional).toBe(false)
    })

    it('should have $type as required', () => {
      const schema = generateParquetSchema({})
      expect(schema.$type.optional).toBe(false)
    })
  })

  describe('$data variant', () => {
    it('should include $data by default', () => {
      const schema = generateParquetSchema({})
      expect(schema.$data).toEqual({ type: 'JSON', optional: true })
    })

    it('should allow excluding $data variant', () => {
      const schema = generateParquetSchema({}, { includeDataVariant: false })
      expect(schema.$data).toBeUndefined()
    })

    it('should have $data as optional', () => {
      const schema = generateParquetSchema({})
      expect(schema.$data.optional).toBe(true)
    })
  })

  describe('audit columns', () => {
    it('should include audit columns by default', () => {
      const schema = generateParquetSchema({})

      expect(schema.createdAt).toEqual({ type: 'TIMESTAMP', optional: false })
      expect(schema.createdBy).toEqual({ type: 'STRING', optional: false })
      expect(schema.updatedAt).toEqual({ type: 'TIMESTAMP', optional: false })
      expect(schema.updatedBy).toEqual({ type: 'STRING', optional: false })
      expect(schema.version).toEqual({ type: 'INT32', optional: false })
    })

    it('should allow excluding audit columns', () => {
      const schema = generateParquetSchema({}, { includeAuditColumns: false })

      expect(schema.createdAt).toBeUndefined()
      expect(schema.createdBy).toBeUndefined()
      expect(schema.updatedAt).toBeUndefined()
      expect(schema.updatedBy).toBeUndefined()
      expect(schema.version).toBeUndefined()
    })
  })

  describe('soft delete columns', () => {
    it('should include soft delete columns by default', () => {
      const schema = generateParquetSchema({})

      expect(schema.deletedAt).toEqual({ type: 'TIMESTAMP', optional: true })
      expect(schema.deletedBy).toEqual({ type: 'STRING', optional: true })
    })

    it('should allow excluding soft delete columns', () => {
      const schema = generateParquetSchema({}, { includeSoftDeleteColumns: false })

      expect(schema.deletedAt).toBeUndefined()
      expect(schema.deletedBy).toBeUndefined()
    })

    it('should have soft delete columns as optional', () => {
      const schema = generateParquetSchema({})

      expect(schema.deletedAt.optional).toBe(true)
      expect(schema.deletedBy.optional).toBe(true)
    })
  })

  describe('basic type mapping', () => {
    it('should map string field', () => {
      const typeDef: TypeDefinition = { name: 'string' }
      const schema = generateParquetSchema(typeDef)
      expect(schema.name).toEqual({ type: 'STRING', optional: true })
    })

    it('should map int field', () => {
      const typeDef: TypeDefinition = { count: 'int' }
      const schema = generateParquetSchema(typeDef)
      expect(schema.count).toEqual({ type: 'INT32', optional: true })
    })

    it('should map float field', () => {
      const typeDef: TypeDefinition = { price: 'float' }
      const schema = generateParquetSchema(typeDef)
      expect(schema.price).toEqual({ type: 'DOUBLE', optional: true })
    })

    it('should map boolean field', () => {
      const typeDef: TypeDefinition = { active: 'boolean' }
      const schema = generateParquetSchema(typeDef)
      expect(schema.active).toEqual({ type: 'BOOLEAN', optional: true })
    })

    it('should map date field', () => {
      const typeDef: TypeDefinition = { birthDate: 'date' }
      const schema = generateParquetSchema(typeDef)
      expect(schema.birthDate).toEqual({ type: 'STRING', optional: true })
    })

    it('should map datetime field', () => {
      const typeDef: TypeDefinition = { publishedAt: 'datetime' }
      const schema = generateParquetSchema(typeDef)
      expect(schema.publishedAt).toEqual({ type: 'TIMESTAMP', optional: true })
    })

    it('should map uuid field', () => {
      const typeDef: TypeDefinition = { externalId: 'uuid' }
      const schema = generateParquetSchema(typeDef)
      expect(schema.externalId).toEqual({ type: 'STRING', optional: true })
    })

    it('should map json field', () => {
      const typeDef: TypeDefinition = { metadata: 'json' }
      const schema = generateParquetSchema(typeDef)
      expect(schema.metadata).toEqual({ type: 'JSON', optional: true })
    })
  })

  describe('required vs optional fields', () => {
    it('should handle required string field (string!)', () => {
      const typeDef: TypeDefinition = { title: 'string!' }
      const schema = generateParquetSchema(typeDef)
      expect(schema.title).toEqual({ type: 'STRING', optional: false })
    })

    it('should handle optional string field (string?)', () => {
      const typeDef: TypeDefinition = { subtitle: 'string?' }
      const schema = generateParquetSchema(typeDef)
      expect(schema.subtitle).toEqual({ type: 'STRING', optional: true })
    })

    it('should handle required int field', () => {
      const typeDef: TypeDefinition = { count: 'int!' }
      const schema = generateParquetSchema(typeDef)
      expect(schema.count).toEqual({ type: 'INT32', optional: false })
    })

    it('should handle required boolean field', () => {
      const typeDef: TypeDefinition = { active: 'boolean!' }
      const schema = generateParquetSchema(typeDef)
      expect(schema.active).toEqual({ type: 'BOOLEAN', optional: false })
    })

    it('should default to optional when no modifier', () => {
      const typeDef: TypeDefinition = { name: 'string' }
      const schema = generateParquetSchema(typeDef)
      expect(schema.name.optional).toBe(true)
    })
  })

  describe('array types', () => {
    it('should map string[] to JSON', () => {
      const typeDef: TypeDefinition = { tags: 'string[]' }
      const schema = generateParquetSchema(typeDef)
      expect(schema.tags).toEqual({ type: 'JSON', optional: true })
    })

    it('should map int[] to JSON', () => {
      const typeDef: TypeDefinition = { scores: 'int[]' }
      const schema = generateParquetSchema(typeDef)
      expect(schema.scores).toEqual({ type: 'JSON', optional: true })
    })

    it('should handle required array (string[]!)', () => {
      const typeDef: TypeDefinition = { tags: 'string[]!' }
      const schema = generateParquetSchema(typeDef)
      expect(schema.tags).toEqual({ type: 'JSON', optional: false })
    })
  })

  describe('skipping special fields', () => {
    it('should skip metadata fields starting with $', () => {
      const typeDef: TypeDefinition = {
        $type: 'schema:Post',
        $shred: ['status'],
        $description: 'A blog post',
        title: 'string!',
      }
      const schema = generateParquetSchema(typeDef)

      expect(schema.title).toBeDefined()
      // These should not be user columns (system columns are added separately)
      expect(Object.keys(schema).filter(k => k.startsWith('$'))).toEqual(['$id', '$type', '$data'])
    })

    it('should skip relationship definitions', () => {
      const typeDef: TypeDefinition = {
        title: 'string!',
        author: '-> User.posts',
        comments: '<- Comment.post[]',
        similar: '~> Post',
      }
      const schema = generateParquetSchema(typeDef)

      expect(schema.title).toBeDefined()
      expect(schema.author).toBeUndefined()
      expect(schema.comments).toBeUndefined()
      expect(schema.similar).toBeUndefined()
    })

    it('should skip $indexes array', () => {
      const typeDef: TypeDefinition = {
        title: 'string!',
        $indexes: [{ fields: ['title'], unique: true }],
      }
      const schema = generateParquetSchema(typeDef)

      expect(schema.title).toBeDefined()
      expect(schema.$indexes).toBeUndefined()
    })
  })

  describe('object field definitions', () => {
    it('should handle object field with type', () => {
      const typeDef: TypeDefinition = {
        status: { type: 'string', required: true },
      }
      const schema = generateParquetSchema(typeDef)

      expect(schema.status).toEqual({ type: 'STRING', optional: false })
    })

    it('should respect required: false in object definition', () => {
      const typeDef: TypeDefinition = {
        status: { type: 'string', required: false },
      }
      const schema = generateParquetSchema(typeDef)

      expect(schema.status).toEqual({ type: 'STRING', optional: true })
    })

    it('should handle object field with index', () => {
      const typeDef: TypeDefinition = {
        email: { type: 'email', required: true, index: 'unique' },
      }
      const schema = generateParquetSchema(typeDef)

      expect(schema.email).toEqual({ type: 'STRING', optional: false })
    })
  })

  describe('complex type definitions', () => {
    it('should handle a complete type definition', () => {
      const Post: TypeDefinition = {
        $type: 'schema:BlogPosting',
        $shred: ['status', 'publishedAt'],

        title: 'string!',
        content: 'markdown!',
        excerpt: 'text',
        slug: 'string!',
        status: { type: 'string', required: true, default: 'draft' },
        publishedAt: 'datetime?',
        views: 'int',
        rating: 'float',
        featured: 'boolean',
        tags: 'string[]',
        metadata: 'json',

        author: '-> User.posts',
        categories: '-> Category.posts[]',
        comments: '<- Comment.post[]',
      }

      const schema = generateParquetSchema(Post)

      // System columns
      expect(schema.$id).toBeDefined()
      expect(schema.$type).toBeDefined()
      expect(schema.$data).toBeDefined()

      // User fields
      expect(schema.title).toEqual({ type: 'STRING', optional: false })
      expect(schema.content).toEqual({ type: 'STRING', optional: false })
      expect(schema.excerpt).toEqual({ type: 'STRING', optional: true })
      expect(schema.slug).toEqual({ type: 'STRING', optional: false })
      expect(schema.status).toEqual({ type: 'STRING', optional: false })
      expect(schema.publishedAt).toEqual({ type: 'TIMESTAMP', optional: true })
      expect(schema.views).toEqual({ type: 'INT32', optional: true })
      expect(schema.rating).toEqual({ type: 'DOUBLE', optional: true })
      expect(schema.featured).toEqual({ type: 'BOOLEAN', optional: true })
      expect(schema.tags).toEqual({ type: 'JSON', optional: true })
      expect(schema.metadata).toEqual({ type: 'JSON', optional: true })

      // Relationships should not be included
      expect(schema.author).toBeUndefined()
      expect(schema.categories).toBeUndefined()
      expect(schema.comments).toBeUndefined()

      // Audit columns
      expect(schema.createdAt).toBeDefined()
      expect(schema.updatedAt).toBeDefined()
      expect(schema.version).toBeDefined()
    })
  })
})

// =============================================================================
// Minimal Schema Tests
// =============================================================================

describe('generateMinimalSchema', () => {
  it('should generate schema with only system columns', () => {
    const schema = generateMinimalSchema()

    expect(schema.$id).toBeDefined()
    expect(schema.$type).toBeDefined()
    expect(schema.$data).toBeDefined()
  })

  it('should include audit columns by default', () => {
    const schema = generateMinimalSchema()

    expect(schema.createdAt).toBeDefined()
    expect(schema.updatedAt).toBeDefined()
    expect(schema.version).toBeDefined()
  })

  it('should respect options', () => {
    const schema = generateMinimalSchema({
      includeDataVariant: false,
      includeAuditColumns: false,
      includeSoftDeleteColumns: false,
    })

    expect(schema.$data).toBeUndefined()
    expect(schema.createdAt).toBeUndefined()
    expect(schema.deletedAt).toBeUndefined()
  })
})

// =============================================================================
// Schema Conversion Tests
// =============================================================================

describe('schemaToColumnSources', () => {
  it('should convert schema to column sources', () => {
    const schema: SchemaTree = {
      $id: { type: 'STRING', optional: false },
      name: { type: 'STRING', optional: true },
    }
    const data = {
      $id: ['1', '2', '3'],
      name: ['Alice', 'Bob', 'Charlie'],
    }

    const sources = schemaToColumnSources(schema, data)

    expect(sources).toHaveLength(2)
    expect(sources[0]).toEqual({
      name: '$id',
      data: ['1', '2', '3'],
      type: 'STRING',
      nullable: false,
    })
    expect(sources[1]).toEqual({
      name: 'name',
      data: ['Alice', 'Bob', 'Charlie'],
      type: 'STRING',
      nullable: true,
    })
  })

  it('should handle missing data columns with empty arrays', () => {
    const schema: SchemaTree = {
      $id: { type: 'STRING', optional: false },
      name: { type: 'STRING', optional: true },
    }
    const data = {
      $id: ['1', '2'],
      // name is missing
    }

    const sources = schemaToColumnSources(schema, data)

    expect(sources).toHaveLength(2)
    expect(sources[1].data).toEqual([])
  })
})

// =============================================================================
// Schema Validation Tests
// =============================================================================

describe('validateSchemaTree', () => {
  it('should validate correct schema', () => {
    const schema = generateParquetSchema({})
    const result = validateSchemaTree(schema)

    expect(result.valid).toBe(true)
    expect(result.errors).toHaveLength(0)
  })

  it('should detect missing $id column', () => {
    const schema: SchemaTree = {
      $type: { type: 'STRING', optional: false },
    }
    const result = validateSchemaTree(schema)

    expect(result.valid).toBe(false)
    expect(result.errors).toContain('Missing required system column: $id')
  })

  it('should detect missing $type column', () => {
    const schema: SchemaTree = {
      $id: { type: 'STRING', optional: false },
    }
    const result = validateSchemaTree(schema)

    expect(result.valid).toBe(false)
    expect(result.errors).toContain('Missing required system column: $type')
  })

  it('should detect missing type in field', () => {
    const schema = {
      $id: { type: 'STRING', optional: false },
      $type: { type: 'STRING', optional: false },
      badField: { optional: true } as unknown as { type: ParquetType; optional: boolean },
    }
    const result = validateSchemaTree(schema)

    expect(result.valid).toBe(false)
    expect(result.errors.some(e => e.includes('badField') && e.includes('missing type'))).toBe(true)
  })

  it('should detect invalid optional value', () => {
    const schema = {
      $id: { type: 'STRING', optional: false },
      $type: { type: 'STRING', optional: false },
      badField: { type: 'STRING' as ParquetType, optional: 'yes' as unknown as boolean },
    }
    const result = validateSchemaTree(schema)

    expect(result.valid).toBe(false)
    expect(result.errors.some(e => e.includes('badField') && e.includes('optional'))).toBe(true)
  })
})

// =============================================================================
// Utility Functions Tests
// =============================================================================

describe('getSchemaColumnNames', () => {
  it('should return all column names', () => {
    const schema = generateParquetSchema({
      title: 'string!',
      count: 'int',
    })

    const names = getSchemaColumnNames(schema)

    expect(names).toContain('$id')
    expect(names).toContain('$type')
    expect(names).toContain('title')
    expect(names).toContain('count')
    expect(names).toContain('createdAt')
  })

  it('should return empty array for empty schema', () => {
    const schema: SchemaTree = {}
    const names = getSchemaColumnNames(schema)

    expect(names).toEqual([])
  })
})

describe('schemaHasColumn', () => {
  it('should return true for existing column', () => {
    const schema = generateParquetSchema({ title: 'string!' })

    expect(schemaHasColumn(schema, '$id')).toBe(true)
    expect(schemaHasColumn(schema, 'title')).toBe(true)
    expect(schemaHasColumn(schema, 'createdAt')).toBe(true)
  })

  it('should return false for non-existing column', () => {
    const schema = generateParquetSchema({ title: 'string!' })

    expect(schemaHasColumn(schema, 'nonexistent')).toBe(false)
    expect(schemaHasColumn(schema, 'missing')).toBe(false)
  })
})

describe('getRequiredColumns', () => {
  it('should return only required columns', () => {
    const typeDef: TypeDefinition = {
      title: 'string!',
      content: 'text!',
      subtitle: 'string?',
    }
    const schema = generateParquetSchema(typeDef)

    const required = getRequiredColumns(schema)

    expect(required).toContain('$id')
    expect(required).toContain('$type')
    expect(required).toContain('title')
    expect(required).toContain('content')
    expect(required).toContain('createdAt')
    expect(required).not.toContain('subtitle')
    expect(required).not.toContain('$data')
  })
})

describe('getOptionalColumns', () => {
  it('should return only optional columns', () => {
    const typeDef: TypeDefinition = {
      title: 'string!',
      subtitle: 'string?',
      tags: 'string[]',
    }
    const schema = generateParquetSchema(typeDef)

    const optional = getOptionalColumns(schema)

    expect(optional).toContain('subtitle')
    expect(optional).toContain('tags')
    expect(optional).toContain('$data')
    expect(optional).toContain('deletedAt')
    expect(optional).not.toContain('$id')
    expect(optional).not.toContain('title')
    expect(optional).not.toContain('createdAt')
  })
})
