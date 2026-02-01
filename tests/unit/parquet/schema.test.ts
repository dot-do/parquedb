/**
 * Parquet Schema Tests
 *
 * Tests for schema conversion utilities that transform ParqueDB schema
 * definitions to Parquet schemas. Covers type mapping, entity schema
 * creation, validation, and utility functions.
 */

import { describe, it, expect } from 'vitest'
import {
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
} from '@/parquet/schema'
import type { ParquetSchema } from '@/parquet/types'
import type { TypeDefinition } from '@/types/schema'

// =============================================================================
// Type Inference Tests
// =============================================================================

describe('Parquet Type Inference', () => {
  describe('inferParquetType', () => {
    // =========================================================================
    // String Types
    // =========================================================================

    describe('string types', () => {
      it('should infer STRING for basic string type', () => {
        const result = inferParquetType('string')
        expect(result.type).toBe('STRING')
        expect(result.optional).toBe(true)
      })

      it('should infer STRING for required string type', () => {
        const result = inferParquetType('string!')
        expect(result.type).toBe('STRING')
        expect(result.optional).toBe(false)
      })

      it('should infer STRING for optional string type', () => {
        const result = inferParquetType('string?')
        expect(result.type).toBe('STRING')
        expect(result.optional).toBe(true)
      })

      it('should infer STRING for text type', () => {
        const result = inferParquetType('text')
        expect(result.type).toBe('STRING')
      })

      it('should infer STRING for markdown type', () => {
        const result = inferParquetType('markdown')
        expect(result.type).toBe('STRING')
      })

      it('should infer STRING for email type', () => {
        const result = inferParquetType('email')
        expect(result.type).toBe('STRING')
      })

      it('should infer STRING for url type', () => {
        const result = inferParquetType('url')
        expect(result.type).toBe('STRING')
      })

      it('should infer STRING for uuid type', () => {
        const result = inferParquetType('uuid')
        expect(result.type).toBe('STRING')
      })
    })

    // =========================================================================
    // Numeric Types
    // =========================================================================

    describe('numeric types', () => {
      it('should infer DOUBLE for number type', () => {
        const result = inferParquetType('number')
        expect(result.type).toBe('DOUBLE')
      })

      it('should infer INT64 for int type', () => {
        const result = inferParquetType('int')
        expect(result.type).toBe('INT64')
      })

      it('should infer DOUBLE for float type', () => {
        const result = inferParquetType('float')
        expect(result.type).toBe('DOUBLE')
      })

      it('should infer DOUBLE for double type', () => {
        const result = inferParquetType('double')
        expect(result.type).toBe('DOUBLE')
      })

      it('should handle required numeric types', () => {
        const result = inferParquetType('int!')
        expect(result.type).toBe('INT64')
        expect(result.optional).toBe(false)
      })
    })

    // =========================================================================
    // Boolean Type
    // =========================================================================

    describe('boolean type', () => {
      it('should infer BOOLEAN for boolean type', () => {
        const result = inferParquetType('boolean')
        expect(result.type).toBe('BOOLEAN')
      })

      it('should handle required boolean', () => {
        const result = inferParquetType('boolean!')
        expect(result.type).toBe('BOOLEAN')
        expect(result.optional).toBe(false)
      })
    })

    // =========================================================================
    // Date/Time Types
    // =========================================================================

    describe('date/time types', () => {
      it('should infer DATE for date type', () => {
        const result = inferParquetType('date')
        expect(result.type).toBe('DATE')
      })

      it('should infer TIMESTAMP_MILLIS for datetime type', () => {
        const result = inferParquetType('datetime')
        expect(result.type).toBe('TIMESTAMP_MILLIS')
      })

      it('should infer TIMESTAMP_MILLIS for timestamp type', () => {
        const result = inferParquetType('timestamp')
        expect(result.type).toBe('TIMESTAMP_MILLIS')
      })
    })

    // =========================================================================
    // Binary Types
    // =========================================================================

    describe('binary types', () => {
      it('should infer BYTE_ARRAY for json type', () => {
        const result = inferParquetType('json')
        expect(result.type).toBe('BYTE_ARRAY')
      })

      it('should infer BYTE_ARRAY for binary type', () => {
        const result = inferParquetType('binary')
        expect(result.type).toBe('BYTE_ARRAY')
      })
    })

    // =========================================================================
    // Parametric Types
    // =========================================================================

    describe('parametric types', () => {
      it('should infer DECIMAL for decimal type with parameters', () => {
        const result = inferParquetType('decimal(10,2)')
        expect(result.type).toBe('DECIMAL')
        expect(result.precision).toBe(10)
        expect(result.scale).toBe(2)
      })

      it('should use default DECIMAL precision and scale if not specified', () => {
        const result = inferParquetType('decimal')
        expect(result.type).toBe('DECIMAL')
        expect(result.precision).toBe(18)
        expect(result.scale).toBe(2)
      })

      it('should infer STRING for varchar type', () => {
        const result = inferParquetType('varchar(255)')
        expect(result.type).toBe('STRING')
      })

      it('should infer STRING for char type', () => {
        const result = inferParquetType('char(10)')
        expect(result.type).toBe('STRING')
      })

      it('should infer BYTE_ARRAY for vector type', () => {
        const result = inferParquetType('vector(1536)')
        expect(result.type).toBe('BYTE_ARRAY')
      })

      it('should infer STRING for enum type', () => {
        const result = inferParquetType('enum(draft,published,archived)')
        expect(result.type).toBe('STRING')
      })
    })

    // =========================================================================
    // Array Types
    // =========================================================================

    describe('array types', () => {
      it('should set REPEATED repetitionType for arrays', () => {
        const result = inferParquetType('string[]')
        expect(result.type).toBe('BYTE_ARRAY')
        expect(result.repetitionType).toBe('REPEATED')
      })

      it('should handle required arrays', () => {
        const result = inferParquetType('string[]!')
        expect(result.optional).toBe(false)
      })
    })

    // =========================================================================
    // Unknown Types
    // =========================================================================

    describe('unknown types', () => {
      it('should default to BYTE_ARRAY for unknown types', () => {
        const result = inferParquetType('customType')
        expect(result.type).toBe('BYTE_ARRAY')
        expect(result.optional).toBe(true)
      })
    })
  })
})

// =============================================================================
// Schema Conversion Tests
// =============================================================================

describe('Schema Conversion', () => {
  describe('toParquetSchema', () => {
    it('should convert simple type definition', () => {
      const typeDef: TypeDefinition = {
        name: 'string!',
        age: 'int',
        active: 'boolean',
      }

      const schema = toParquetSchema(typeDef)

      expect(schema.name.type).toBe('STRING')
      expect(schema.name.optional).toBe(false)
      expect(schema.age.type).toBe('INT64')
      expect(schema.active.type).toBe('BOOLEAN')
    })

    it('should skip metadata fields starting with $', () => {
      const typeDef: TypeDefinition = {
        $type: 'User',
        $shred: ['status'],
        name: 'string',
      }

      const schema = toParquetSchema(typeDef)

      expect(schema.$type).toBeUndefined()
      expect(schema.$shred).toBeUndefined()
      expect(schema.name).toBeDefined()
    })

    it('should skip relationship definitions', () => {
      const typeDef: TypeDefinition = {
        name: 'string',
        author: '-> User.posts',
        comments: '<- Comment.post',
      }

      const schema = toParquetSchema(typeDef)

      expect(schema.name).toBeDefined()
      expect(schema.author).toBeUndefined()
      expect(schema.comments).toBeUndefined()
    })

    it('should handle object field definitions', () => {
      const typeDef: TypeDefinition = {
        name: { type: 'string', required: true },
        age: { type: 'int', required: false },
      }

      const schema = toParquetSchema(typeDef)

      expect(schema.name.type).toBe('STRING')
      expect(schema.name.optional).toBe(false)
      expect(schema.age.type).toBe('INT64')
      expect(schema.age.optional).toBe(true)
    })

    it('should return empty schema for empty type definition', () => {
      const schema = toParquetSchema({})
      expect(Object.keys(schema)).toHaveLength(0)
    })

    it('should skip non-field definitions', () => {
      const typeDef: TypeDefinition = {
        name: 'string',
        invalid: null as unknown as string, // Invalid field
        alsoInvalid: 123 as unknown as string, // Invalid field
      }

      const schema = toParquetSchema(typeDef)

      expect(schema.name).toBeDefined()
      expect(Object.keys(schema)).toHaveLength(1)
    })
  })
})

// =============================================================================
// Entity Schema Tests
// =============================================================================

describe('Entity Schema Creation', () => {
  describe('createEntitySchema', () => {
    it('should create schema with all system columns', () => {
      const schema = createEntitySchema()

      // System columns
      expect(schema.$id.type).toBe('STRING')
      expect(schema.$id.optional).toBe(false)
      expect(schema.$type.type).toBe('STRING')
      expect(schema.name.type).toBe('STRING')
      expect(schema.name.optional).toBe(false)

      // Audit columns
      expect(schema.createdAt.type).toBe('TIMESTAMP_MILLIS')
      expect(schema.createdBy.type).toBe('STRING')
      expect(schema.updatedAt.type).toBe('TIMESTAMP_MILLIS')
      expect(schema.updatedBy.type).toBe('STRING')
      expect(schema.deletedAt.type).toBe('TIMESTAMP_MILLIS')
      expect(schema.deletedAt.optional).toBe(true)
      expect(schema.deletedBy.type).toBe('STRING')
      expect(schema.deletedBy.optional).toBe(true)
      expect(schema.version.type).toBe('INT32')

      // Data column
      expect(schema.$data.type).toBe('BYTE_ARRAY')
    })

    it('should add shredded columns from type definition', () => {
      const typeDef: TypeDefinition = {
        status: 'string',
        priority: 'int',
        content: 'text',
      }

      const schema = createEntitySchema({
        typeDef,
        shredFields: ['status', 'priority'],
      })

      expect(schema.status.type).toBe('STRING')
      expect(schema.priority.type).toBe('INT64')
      expect(schema.content).toBeUndefined() // Not shredded
    })

    it('should handle shred fields not in type definition', () => {
      const typeDef: TypeDefinition = {
        status: 'string',
      }

      const schema = createEntitySchema({
        typeDef,
        shredFields: ['status', 'nonexistent'],
      })

      expect(schema.status).toBeDefined()
      // nonexistent should be silently ignored
    })

    it('should add additional columns', () => {
      const schema = createEntitySchema({
        additionalColumns: {
          customField: { type: 'STRING', optional: true },
          anotherField: { type: 'INT64', optional: false },
        },
      })

      expect(schema.customField.type).toBe('STRING')
      expect(schema.anotherField.type).toBe('INT64')
      expect(schema.anotherField.optional).toBe(false)
    })

    it('should not include relationships in shredded columns', () => {
      const typeDef: TypeDefinition = {
        status: 'string',
        author: '-> User.posts',
      }

      const schema = createEntitySchema({
        typeDef,
        shredFields: ['status', 'author'],
      })

      expect(schema.status).toBeDefined()
      expect(schema.author).toBeUndefined()
    })

    it('should handle object field definitions for shredding', () => {
      const typeDef: TypeDefinition = {
        status: { type: 'string', required: true },
        count: { type: 'int', required: false },
      }

      const schema = createEntitySchema({
        typeDef,
        shredFields: ['status', 'count'],
      })

      expect(schema.status.type).toBe('STRING')
      expect(schema.status.optional).toBe(false)
      expect(schema.count.type).toBe('INT64')
      expect(schema.count.optional).toBe(true)
    })
  })

  describe('createRelationshipSchema', () => {
    it('should create schema for relationship storage', () => {
      const schema = createRelationshipSchema()

      // Source entity
      expect(schema.fromNs.type).toBe('STRING')
      expect(schema.fromId.type).toBe('STRING')
      expect(schema.fromType.type).toBe('STRING')
      expect(schema.fromName.type).toBe('STRING')

      // Relationship names
      expect(schema.predicate.type).toBe('STRING')
      expect(schema.predicate.optional).toBe(false)
      expect(schema.reverse.type).toBe('STRING')
      expect(schema.reverse.optional).toBe(false)

      // Target entity
      expect(schema.toNs.type).toBe('STRING')
      expect(schema.toId.type).toBe('STRING')
      expect(schema.toType.type).toBe('STRING')
      expect(schema.toName.type).toBe('STRING')

      // Audit columns
      expect(schema.createdAt.type).toBe('TIMESTAMP_MILLIS')
      expect(schema.createdBy.type).toBe('STRING')
      expect(schema.deletedAt.type).toBe('TIMESTAMP_MILLIS')
      expect(schema.deletedAt.optional).toBe(true)
      expect(schema.version.type).toBe('INT32')

      // Edge properties
      expect(schema.data.type).toBe('BYTE_ARRAY')
      expect(schema.data.optional).toBe(true)
    })
  })

  describe('createEventSchema', () => {
    it('should create schema for event log storage', () => {
      const schema = createEventSchema()

      // Event identity
      expect(schema.id.type).toBe('STRING')
      expect(schema.id.optional).toBe(false)
      expect(schema.ts.type).toBe('TIMESTAMP_MILLIS')
      expect(schema.ts.optional).toBe(false)

      // Target info
      expect(schema.target.type).toBe('STRING')
      expect(schema.op.type).toBe('STRING')

      // Entity reference
      expect(schema.ns.type).toBe('STRING')
      expect(schema.entityId.type).toBe('STRING')

      // State snapshots
      expect(schema.before.type).toBe('BYTE_ARRAY')
      expect(schema.before.optional).toBe(true)
      expect(schema.after.type).toBe('BYTE_ARRAY')
      expect(schema.after.optional).toBe(true)

      // Audit
      expect(schema.actor.type).toBe('STRING')
      expect(schema.metadata.type).toBe('BYTE_ARRAY')
    })
  })
})

// =============================================================================
// Schema Validation Tests
// =============================================================================

describe('Schema Validation', () => {
  describe('validateParquetSchema', () => {
    it('should validate correct schema', () => {
      const schema: ParquetSchema = {
        id: { type: 'STRING', optional: false },
        name: { type: 'STRING', optional: true },
        count: { type: 'INT64', optional: true },
      }

      const result = validateParquetSchema(schema)

      expect(result.valid).toBe(true)
      expect(result.errors).toHaveLength(0)
    })

    it('should detect missing type', () => {
      const schema: ParquetSchema = {
        id: { type: '' as 'STRING', optional: false },
      }

      const result = validateParquetSchema(schema)

      expect(result.valid).toBe(false)
      expect(result.errors.length).toBeGreaterThan(0)
      expect(result.errors[0]).toContain('missing type')
    })

    it('should validate DECIMAL requires precision', () => {
      const schema: ParquetSchema = {
        amount: { type: 'DECIMAL', scale: 2 },
      }

      const result = validateParquetSchema(schema)

      expect(result.valid).toBe(false)
      expect(result.errors.some((e) => e.includes('precision'))).toBe(true)
    })

    it('should validate DECIMAL requires scale', () => {
      const schema: ParquetSchema = {
        amount: { type: 'DECIMAL', precision: 10 },
      }

      const result = validateParquetSchema(schema)

      expect(result.valid).toBe(false)
      expect(result.errors.some((e) => e.includes('scale'))).toBe(true)
    })

    it('should validate DECIMAL scale cannot exceed precision', () => {
      const schema: ParquetSchema = {
        amount: { type: 'DECIMAL', precision: 5, scale: 10 },
      }

      const result = validateParquetSchema(schema)

      expect(result.valid).toBe(false)
      expect(result.errors.some((e) => e.includes('scale cannot be greater than precision'))).toBe(true)
    })

    it('should validate FIXED_LEN_BYTE_ARRAY requires typeLength', () => {
      const schema: ParquetSchema = {
        hash: { type: 'FIXED_LEN_BYTE_ARRAY' },
      }

      const result = validateParquetSchema(schema)

      expect(result.valid).toBe(false)
      expect(result.errors.some((e) => e.includes('typeLength'))).toBe(true)
    })

    it('should validate FIXED_LEN_BYTE_ARRAY typeLength must be positive', () => {
      const schema: ParquetSchema = {
        hash: { type: 'FIXED_LEN_BYTE_ARRAY', typeLength: 0 },
      }

      const result = validateParquetSchema(schema)

      expect(result.valid).toBe(false)
      expect(result.errors.some((e) => e.includes('typeLength'))).toBe(true)
    })

    it('should pass valid DECIMAL schema', () => {
      const schema: ParquetSchema = {
        amount: { type: 'DECIMAL', precision: 10, scale: 2 },
      }

      const result = validateParquetSchema(schema)

      expect(result.valid).toBe(true)
    })

    it('should pass valid FIXED_LEN_BYTE_ARRAY schema', () => {
      const schema: ParquetSchema = {
        hash: { type: 'FIXED_LEN_BYTE_ARRAY', typeLength: 32 },
      }

      const result = validateParquetSchema(schema)

      expect(result.valid).toBe(true)
    })
  })
})

// =============================================================================
// Utility Functions Tests
// =============================================================================

describe('Schema Utility Functions', () => {
  describe('getShredFields', () => {
    it('should return explicit $shred fields', () => {
      const typeDef: TypeDefinition = {
        $shred: ['status', 'priority'],
        status: 'string',
        priority: 'int',
        content: 'text',
      }

      const fields = getShredFields(typeDef)

      expect(fields).toEqual(['status', 'priority'])
    })

    it('should auto-detect indexed fields', () => {
      const typeDef: TypeDefinition = {
        status: { type: 'string', index: true },
        priority: { type: 'int', index: true },
        content: 'text',
      }

      const fields = getShredFields(typeDef)

      expect(fields).toContain('status')
      expect(fields).toContain('priority')
      expect(fields).not.toContain('content')
    })

    it('should return empty array when no shred fields', () => {
      const typeDef: TypeDefinition = {
        name: 'string',
        age: 'int',
      }

      const fields = getShredFields(typeDef)

      expect(fields).toEqual([])
    })

    it('should ignore metadata fields in auto-detection', () => {
      const typeDef: TypeDefinition = {
        $type: 'User',
        status: { type: 'string', index: true },
      }

      const fields = getShredFields(typeDef)

      expect(fields).toEqual(['status'])
      expect(fields).not.toContain('$type')
    })

    it('should not include string field definitions in auto-detection', () => {
      const typeDef: TypeDefinition = {
        status: 'string#', // This would be indexed via index modifier
        content: 'text',
      }

      const fields = getShredFields(typeDef)

      // String definitions don't have index property in object form
      expect(fields).toEqual([])
    })
  })

  describe('mergeSchemas', () => {
    it('should merge two schemas', () => {
      const base: ParquetSchema = {
        id: { type: 'STRING', optional: false },
        name: { type: 'STRING', optional: true },
      }
      const override: ParquetSchema = {
        count: { type: 'INT64', optional: true },
      }

      const merged = mergeSchemas(base, override)

      expect(merged.id).toEqual(base.id)
      expect(merged.name).toEqual(base.name)
      expect(merged.count).toEqual(override.count)
    })

    it('should override fields with same name', () => {
      const base: ParquetSchema = {
        field: { type: 'STRING', optional: true },
      }
      const override: ParquetSchema = {
        field: { type: 'INT64', optional: false },
      }

      const merged = mergeSchemas(base, override)

      expect(merged.field.type).toBe('INT64')
      expect(merged.field.optional).toBe(false)
    })

    it('should handle empty base schema', () => {
      const override: ParquetSchema = {
        field: { type: 'STRING', optional: true },
      }

      const merged = mergeSchemas({}, override)

      expect(merged).toEqual(override)
    })

    it('should handle empty override schema', () => {
      const base: ParquetSchema = {
        field: { type: 'STRING', optional: true },
      }

      const merged = mergeSchemas(base, {})

      expect(merged).toEqual(base)
    })
  })

  describe('getColumnNames', () => {
    it('should return all column names', () => {
      const schema: ParquetSchema = {
        id: { type: 'STRING' },
        name: { type: 'STRING' },
        count: { type: 'INT64' },
      }

      const names = getColumnNames(schema)

      expect(names).toContain('id')
      expect(names).toContain('name')
      expect(names).toContain('count')
      expect(names).toHaveLength(3)
    })

    it('should return empty array for empty schema', () => {
      const names = getColumnNames({})

      expect(names).toEqual([])
    })
  })

  describe('hasColumn', () => {
    it('should return true for existing column', () => {
      const schema: ParquetSchema = {
        id: { type: 'STRING' },
        name: { type: 'STRING' },
      }

      expect(hasColumn(schema, 'id')).toBe(true)
      expect(hasColumn(schema, 'name')).toBe(true)
    })

    it('should return false for non-existing column', () => {
      const schema: ParquetSchema = {
        id: { type: 'STRING' },
      }

      expect(hasColumn(schema, 'name')).toBe(false)
      expect(hasColumn(schema, 'nonexistent')).toBe(false)
    })

    it('should handle empty schema', () => {
      expect(hasColumn({}, 'any')).toBe(false)
    })
  })
})
