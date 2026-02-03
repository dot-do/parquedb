/**
 * Comprehensive tests for all TypeDefinition variations in ParqueDB schemas
 *
 * This file tests all variations of:
 * - Meta fields ($type, $ns, $shred, $description, $abstract, $extends, $indexes, $visibility)
 * - Primitive types (string, text, markdown, number, int, float, double, boolean, date, datetime, timestamp, uuid, email, url, json, binary)
 * - Parametric types (decimal, varchar, char, vector, enum)
 * - Type modifiers (!, ?, [], []!, defaults)
 * - Index modifiers (#, ##, #fts, #vec, #hash)
 * - Relationships (forward ->, backward <-, fuzzy ~>, <~)
 * - Field definition objects
 */

import { describe, it, expect } from 'vitest'
import { parseFieldType, parseRelation, isRelationString } from '../../../src/types/schema'
import {
  validateSchema,
  validateTypeDefinition,
  validateRelationshipTargets,
  parseSchema,
  isValidFieldType,
  isValidRelationString,
} from '../../../src/schema/parser'
import { SchemaValidator } from '../../../src/schema/validator'
import type { Schema, TypeDefinition, FieldDefinition, IndexDefinition } from '../../../src/types/schema'

describe('TypeDefinition Meta Fields', () => {
  describe('$type - JSON-LD type URI', () => {
    it('accepts valid $type URIs', () => {
      const schema: Schema = {
        User: {
          $type: 'schema:Person',
          name: 'string!',
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
    })

    it('accepts full URIs', () => {
      const schema: Schema = {
        User: {
          $type: 'https://schema.org/Person',
          name: 'string!',
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
    })

    it('parses $type correctly', () => {
      const schema: Schema = {
        User: {
          $type: 'schema:Person',
          name: 'string!',
        },
      }

      const parsed = parseSchema(schema)
      expect(parsed.getType('User')?.typeUri).toBe('schema:Person')
    })
  })

  describe('$ns - Default namespace', () => {
    it('accepts $ns namespace', () => {
      const schema: Schema = {
        User: {
          $ns: 'https://example.com/users',
          name: 'string!',
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
    })

    it('parses $ns correctly', () => {
      const schema: Schema = {
        User: {
          $ns: 'https://example.com/users',
          name: 'string!',
        },
      }

      const parsed = parseSchema(schema)
      expect(parsed.getType('User')?.namespace).toBe('https://example.com/users')
    })
  })

  describe('$shred - Fields to shred from Variant', () => {
    it('accepts $shred array of field names', () => {
      const schema: Schema = {
        User: {
          $shred: ['status', 'createdAt'],
          name: 'string!',
          status: 'string',
          createdAt: 'datetime',
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
    })

    it('parses $shred correctly', () => {
      const schema: Schema = {
        User: {
          $shred: ['status', 'createdAt'],
          name: 'string!',
          status: 'string',
          createdAt: 'datetime',
        },
      }

      const parsed = parseSchema(schema)
      expect(parsed.getType('User')?.shredFields).toEqual(['status', 'createdAt'])
    })

    it('handles empty $shred array', () => {
      const schema: Schema = {
        User: {
          $shred: [],
          name: 'string!',
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
    })
  })

  describe('$description - Type description', () => {
    it('accepts $description string', () => {
      const schema: Schema = {
        User: {
          $description: 'A user in the system',
          name: 'string!',
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
    })
  })

  describe('$abstract - Abstract type marker', () => {
    it('accepts $abstract boolean', () => {
      const schema: Schema = {
        BaseEntity: {
          $abstract: true,
          id: 'uuid!',
          createdAt: 'datetime!',
        },
        User: {
          name: 'string!',
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
    })

    it('parses $abstract correctly', () => {
      const schema: Schema = {
        BaseEntity: {
          $abstract: true,
          id: 'uuid!',
        },
      }

      const parsed = parseSchema(schema)
      expect(parsed.getType('BaseEntity')?.isAbstract).toBe(true)
    })
  })

  describe('$extends - Type inheritance', () => {
    it('accepts $extends string', () => {
      const schema: Schema = {
        BaseEntity: {
          id: 'uuid!',
        },
        User: {
          $extends: 'BaseEntity',
          name: 'string!',
        },
      }

      const result = validateSchema(schema, { checkRelationships: false })
      expect(result.valid).toBe(true)
    })

    it('parses $extends correctly', () => {
      const schema: Schema = {
        BaseEntity: {
          id: 'uuid!',
        },
        User: {
          $extends: 'BaseEntity',
          name: 'string!',
        },
      }

      const parsed = parseSchema(schema)
      expect(parsed.getType('User')?.extends).toBe('BaseEntity')
    })
  })

  describe('$indexes - Compound index definitions', () => {
    it('accepts $indexes array', () => {
      const indexes: IndexDefinition[] = [
        { fields: ['lastName', 'firstName'], name: 'name_idx' },
        { fields: [{ field: 'createdAt', direction: -1 }], unique: false },
      ]

      const schema: Schema = {
        User: {
          $indexes: indexes,
          firstName: 'string!',
          lastName: 'string!',
          createdAt: 'datetime!',
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
    })

    it('parses $indexes correctly', () => {
      const indexes: IndexDefinition[] = [
        { fields: ['lastName', 'firstName'], name: 'name_idx', unique: true },
      ]

      const schema: Schema = {
        User: {
          $indexes: indexes,
          firstName: 'string!',
          lastName: 'string!',
        },
      }

      const parsed = parseSchema(schema)
      expect(parsed.getType('User')?.indexes).toHaveLength(1)
      expect(parsed.getType('User')?.indexes[0]?.name).toBe('name_idx')
    })

    it('accepts indexes with TTL', () => {
      const indexes: IndexDefinition[] = [
        { fields: ['expiresAt'], expireAfterSeconds: 3600 },
      ]

      const schema: Schema = {
        Session: {
          $indexes: indexes,
          token: 'string!',
          expiresAt: 'datetime!',
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
    })

    it('accepts indexes with partial filter', () => {
      const indexes: IndexDefinition[] = [
        {
          fields: ['email'],
          unique: true,
          sparse: true,
          partialFilterExpression: { status: 'active' },
        },
      ]

      const schema: Schema = {
        User: {
          $indexes: indexes,
          email: 'email',
          status: 'string',
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
    })
  })

  describe('$visibility - Collection visibility', () => {
    it('accepts $visibility public', () => {
      const schema: Schema = {
        PublicPost: {
          $visibility: 'public',
          title: 'string!',
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
    })

    it('accepts $visibility unlisted', () => {
      const schema: Schema = {
        Draft: {
          $visibility: 'unlisted',
          content: 'text!',
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
    })

    it('accepts $visibility private', () => {
      const schema: Schema = {
        SecretData: {
          $visibility: 'private',
          secret: 'string!',
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
    })
  })

  describe('Unknown meta fields', () => {
    it('rejects unknown $-prefixed fields', () => {
      const schema: Schema = {
        User: {
          $customMeta: 'invalid',
          name: 'string!',
        } as any,
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.code === 'RESERVED_FIELD_NAME')).toBe(true)
    })
  })
})

describe('TypeDefinition Primitive Types', () => {
  const primitiveTypes = [
    'string',
    'text',
    'markdown',
    'number',
    'int',
    'float',
    'double',
    'boolean',
    'date',
    'datetime',
    'timestamp',
    'uuid',
    'email',
    'url',
    'json',
    'binary',
  ]

  describe('All primitive types are valid', () => {
    for (const type of primitiveTypes) {
      it(`validates ${type} type`, () => {
        expect(isValidFieldType(type)).toBe(true)
      })

      it(`parses ${type} type`, () => {
        const result = parseFieldType(type)
        expect(result.type).toBe(type)
        expect(result.required).toBe(false)
        expect(result.isArray).toBe(false)
      })

      it(`${type} works in schema`, () => {
        const schema: Schema = {
          TestType: {
            field: type,
          },
        }

        const result = validateSchema(schema)
        expect(result.valid).toBe(true)
      })
    }
  })

  describe('Runtime validation for primitive types', () => {
    it('validates string type', () => {
      const validator = new SchemaValidator({
        Test: { field: 'string!' },
      }, { mode: 'permissive' })

      expect(validator.validate('Test', { field: 'hello' }).valid).toBe(true)
      expect(validator.validate('Test', { field: 123 }).valid).toBe(false)
    })

    it('validates text type', () => {
      const validator = new SchemaValidator({
        Test: { field: 'text!' },
      }, { mode: 'permissive' })

      expect(validator.validate('Test', { field: 'long text content' }).valid).toBe(true)
      expect(validator.validate('Test', { field: {} }).valid).toBe(false)
    })

    it('validates markdown type', () => {
      const validator = new SchemaValidator({
        Test: { field: 'markdown!' },
      }, { mode: 'permissive' })

      expect(validator.validate('Test', { field: '# Heading\n\nParagraph' }).valid).toBe(true)
    })

    it('validates number type', () => {
      const validator = new SchemaValidator({
        Test: { field: 'number!' },
      }, { mode: 'permissive' })

      expect(validator.validate('Test', { field: 42 }).valid).toBe(true)
      expect(validator.validate('Test', { field: 3.14 }).valid).toBe(true)
      expect(validator.validate('Test', { field: '42' }).valid).toBe(false)
    })

    it('validates int type', () => {
      const validator = new SchemaValidator({
        Test: { field: 'int!' },
      }, { mode: 'permissive' })

      expect(validator.validate('Test', { field: 42 }).valid).toBe(true)
      expect(validator.validate('Test', { field: 3.14 }).valid).toBe(false)
    })

    it('validates float type', () => {
      const validator = new SchemaValidator({
        Test: { field: 'float!' },
      }, { mode: 'permissive' })

      expect(validator.validate('Test', { field: 3.14 }).valid).toBe(true)
      expect(validator.validate('Test', { field: 42 }).valid).toBe(true) // int is valid float
    })

    it('validates double type', () => {
      const validator = new SchemaValidator({
        Test: { field: 'double!' },
      }, { mode: 'permissive' })

      expect(validator.validate('Test', { field: 1.7976931348623157e308 }).valid).toBe(true)
    })

    it('validates boolean type', () => {
      const validator = new SchemaValidator({
        Test: { field: 'boolean!' },
      }, { mode: 'permissive' })

      expect(validator.validate('Test', { field: true }).valid).toBe(true)
      expect(validator.validate('Test', { field: false }).valid).toBe(true)
      expect(validator.validate('Test', { field: 1 }).valid).toBe(false)
      expect(validator.validate('Test', { field: 'true' }).valid).toBe(false)
    })

    it('validates date type', () => {
      const validator = new SchemaValidator({
        Test: { field: 'date!' },
      }, { mode: 'permissive' })

      expect(validator.validate('Test', { field: '2024-01-15' }).valid).toBe(true)
      expect(validator.validate('Test', { field: new Date() }).valid).toBe(true)
    })

    it('validates datetime type', () => {
      const validator = new SchemaValidator({
        Test: { field: 'datetime!' },
      }, { mode: 'permissive' })

      expect(validator.validate('Test', { field: '2024-01-15T10:30:00Z' }).valid).toBe(true)
      expect(validator.validate('Test', { field: new Date() }).valid).toBe(true)
    })

    it('validates timestamp type', () => {
      const validator = new SchemaValidator({
        Test: { field: 'timestamp!' },
      }, { mode: 'permissive' })

      expect(validator.validate('Test', { field: '2024-01-15T10:30:00.000Z' }).valid).toBe(true)
    })

    it('validates uuid type', () => {
      const validator = new SchemaValidator({
        Test: { field: 'uuid!' },
      }, { mode: 'permissive' })

      expect(validator.validate('Test', { field: '550e8400-e29b-41d4-a716-446655440000' }).valid).toBe(true)
      expect(validator.validate('Test', { field: 'not-a-uuid' }).valid).toBe(false)
    })

    it('validates email type', () => {
      const validator = new SchemaValidator({
        Test: { field: 'email!' },
      }, { mode: 'permissive' })

      expect(validator.validate('Test', { field: 'test@example.com' }).valid).toBe(true)
      expect(validator.validate('Test', { field: 'invalid-email' }).valid).toBe(false)
    })

    it('validates url type', () => {
      const validator = new SchemaValidator({
        Test: { field: 'url!' },
      }, { mode: 'permissive' })

      expect(validator.validate('Test', { field: 'https://example.com' }).valid).toBe(true)
      expect(validator.validate('Test', { field: 'not-a-url' }).valid).toBe(false)
    })

    it('validates json type', () => {
      const validator = new SchemaValidator({
        Test: { field: 'json!' },
      }, { mode: 'permissive' })

      expect(validator.validate('Test', { field: { any: 'object' } }).valid).toBe(true)
      expect(validator.validate('Test', { field: [1, 2, 3] }).valid).toBe(true)
      expect(validator.validate('Test', { field: 'string' }).valid).toBe(true)
      expect(validator.validate('Test', { field: 42 }).valid).toBe(true)
    })

    it('validates binary type', () => {
      const validator = new SchemaValidator({
        Test: { field: 'binary!' },
      }, { mode: 'permissive' })

      expect(validator.validate('Test', { field: new Uint8Array([1, 2, 3]) }).valid).toBe(true)
      expect(validator.validate('Test', { field: 'base64string' }).valid).toBe(true)
    })
  })
})

describe('TypeDefinition Parametric Types', () => {
  describe('decimal(precision,scale)', () => {
    it('validates decimal type syntax', () => {
      expect(isValidFieldType('decimal(10,2)')).toBe(true)
      expect(isValidFieldType('decimal(18,4)')).toBe(true)
    })

    it('rejects invalid decimal syntax', () => {
      expect(isValidFieldType('decimal(10)')).toBe(false)
      expect(isValidFieldType('decimal()')).toBe(false)
      expect(isValidFieldType('decimal')).toBe(false)
    })

    it('parses decimal type', () => {
      const result = parseFieldType('decimal(10,2)')
      expect(result.type).toBe('decimal(10,2)')
    })

    it('validates decimal at runtime', () => {
      const validator = new SchemaValidator({
        Test: { price: 'decimal(10,2)' },
      }, { mode: 'permissive' })

      expect(validator.validate('Test', { price: 99.99 }).valid).toBe(true)
      expect(validator.validate('Test', { price: '99.99' }).valid).toBe(true)
    })
  })

  describe('varchar(n)', () => {
    it('validates varchar type syntax', () => {
      expect(isValidFieldType('varchar(255)')).toBe(true)
      expect(isValidFieldType('varchar(50)')).toBe(true)
    })

    it('rejects invalid varchar syntax', () => {
      expect(isValidFieldType('varchar()')).toBe(false)
      expect(isValidFieldType('varchar')).toBe(false)
    })

    it('validates varchar length at runtime', () => {
      const validator = new SchemaValidator({
        Test: { code: 'varchar(5)' },
      }, { mode: 'permissive' })

      expect(validator.validate('Test', { code: 'ABCDE' }).valid).toBe(true)
      expect(validator.validate('Test', { code: 'ABCDEF' }).valid).toBe(false)
    })
  })

  describe('char(n)', () => {
    it('validates char type syntax', () => {
      expect(isValidFieldType('char(36)')).toBe(true)
      expect(isValidFieldType('char(1)')).toBe(true)
    })

    it('validates char length at runtime', () => {
      const validator = new SchemaValidator({
        Test: { code: 'char(3)' },
      }, { mode: 'permissive' })

      expect(validator.validate('Test', { code: 'ABC' }).valid).toBe(true)
      expect(validator.validate('Test', { code: 'ABCD' }).valid).toBe(false)
    })
  })

  describe('vector(n)', () => {
    it('validates vector type syntax', () => {
      expect(isValidFieldType('vector(1536)')).toBe(true)
      expect(isValidFieldType('vector(3)')).toBe(true)
    })

    it('rejects invalid vector syntax', () => {
      expect(isValidFieldType('vector()')).toBe(false)
      expect(isValidFieldType('vector')).toBe(false)
    })

    it('validates vector dimensions at runtime', () => {
      const validator = new SchemaValidator({
        Test: { embedding: 'vector(3)' },
      }, { mode: 'permissive' })

      expect(validator.validate('Test', { embedding: [1.0, 2.0, 3.0] }).valid).toBe(true)
      expect(validator.validate('Test', { embedding: [1.0, 2.0] }).valid).toBe(false)
    })

    it('validates vector element types', () => {
      const validator = new SchemaValidator({
        Test: { embedding: 'vector(3)' },
      }, { mode: 'permissive' })

      expect(validator.validate('Test', { embedding: [1.0, 'two', 3.0] }).valid).toBe(false)
    })
  })

  describe('enum(values)', () => {
    it('validates enum type syntax', () => {
      expect(isValidFieldType('enum(draft,published,archived)')).toBe(true)
      expect(isValidFieldType('enum(a,b,c)')).toBe(true)
    })

    it('rejects empty enum', () => {
      expect(isValidFieldType('enum()')).toBe(false)
    })

    it('validates enum values at runtime', () => {
      const validator = new SchemaValidator({
        Test: { status: 'enum(draft,published,archived)' },
      }, { mode: 'permissive' })

      expect(validator.validate('Test', { status: 'draft' }).valid).toBe(true)
      expect(validator.validate('Test', { status: 'published' }).valid).toBe(true)
      expect(validator.validate('Test', { status: 'invalid' }).valid).toBe(false)
    })
  })
})

describe('TypeDefinition Type Modifiers', () => {
  describe('Required modifier (!)', () => {
    it('parses required modifier', () => {
      const result = parseFieldType('string!')
      expect(result.type).toBe('string')
      expect(result.required).toBe(true)
    })

    it('validates required fields', () => {
      const validator = new SchemaValidator({
        Test: { name: 'string!' },
      }, { mode: 'permissive' })

      expect(validator.validate('Test', { name: 'Alice' }).valid).toBe(true)
      expect(validator.validate('Test', {}).valid).toBe(false)
      expect(validator.validate('Test', { name: null }).valid).toBe(false)
    })
  })

  describe('Optional modifier (?)', () => {
    it('parses optional modifier', () => {
      const result = parseFieldType('string?')
      expect(result.type).toBe('string')
      expect(result.required).toBe(false)
    })

    it('validates optional fields', () => {
      const validator = new SchemaValidator({
        Test: { bio: 'string?' },
      }, { mode: 'permissive' })

      expect(validator.validate('Test', { bio: 'Hello' }).valid).toBe(true)
      expect(validator.validate('Test', {}).valid).toBe(true)
      expect(validator.validate('Test', { bio: null }).valid).toBe(true)
    })
  })

  describe('Array modifier ([])', () => {
    it('parses array modifier', () => {
      const result = parseFieldType('string[]')
      expect(result.type).toBe('string')
      expect(result.isArray).toBe(true)
      expect(result.required).toBe(false)
    })

    it('validates array fields', () => {
      const validator = new SchemaValidator({
        Test: { tags: 'string[]' },
      }, { mode: 'permissive' })

      expect(validator.validate('Test', { tags: ['a', 'b', 'c'] }).valid).toBe(true)
      expect(validator.validate('Test', { tags: [] }).valid).toBe(true)
      expect(validator.validate('Test', { tags: 'not-an-array' }).valid).toBe(false)
    })

    it('validates array element types', () => {
      const validator = new SchemaValidator({
        Test: { numbers: 'int[]' },
      }, { mode: 'permissive' })

      expect(validator.validate('Test', { numbers: [1, 2, 3] }).valid).toBe(true)
      expect(validator.validate('Test', { numbers: [1, 'two', 3] }).valid).toBe(false)
    })
  })

  describe('Required array modifier ([]!)', () => {
    it('parses required array modifier', () => {
      const result = parseFieldType('string[]!')
      expect(result.type).toBe('string')
      expect(result.isArray).toBe(true)
      expect(result.required).toBe(true)
    })

    it('validates required array fields', () => {
      const validator = new SchemaValidator({
        Test: { tags: 'string[]!' },
      }, { mode: 'permissive' })

      expect(validator.validate('Test', { tags: ['a'] }).valid).toBe(true)
      expect(validator.validate('Test', {}).valid).toBe(false)
    })
  })

  describe('Default values (= value)', () => {
    it('parses default string value', () => {
      const result = parseFieldType('string = "draft"')
      expect(result.type).toBe('string')
      expect(result.default).toBe('"draft"')
    })

    it('parses default number value', () => {
      const result = parseFieldType('int = 0')
      expect(result.type).toBe('int')
      expect(result.default).toBe('0')
    })

    it('allows missing required fields with defaults', () => {
      const validator = new SchemaValidator({
        Test: { status: 'string! = "draft"' },
      }, { mode: 'permissive' })

      // Required field with default should pass validation when missing
      expect(validator.validate('Test', {}).valid).toBe(true)
    })

    it('validates provided values even with defaults', () => {
      const validator = new SchemaValidator({
        Test: { count: 'int = 0' },
      }, { mode: 'permissive' })

      expect(validator.validate('Test', { count: 5 }).valid).toBe(true)
      expect(validator.validate('Test', { count: 'five' }).valid).toBe(false)
    })
  })

  describe('Modifier combinations', () => {
    it('rejects double required (!!) ', () => {
      expect(isValidFieldType('string!!')).toBe(false)
    })

    it('rejects conflicting modifiers (!?)', () => {
      expect(isValidFieldType('string!?')).toBe(false)
      expect(isValidFieldType('string?!')).toBe(false)
    })

    it('accepts parametric types with modifiers', () => {
      expect(isValidFieldType('varchar(255)!')).toBe(true)
      expect(isValidFieldType('decimal(10,2)?')).toBe(true)
      expect(isValidFieldType('vector(1536)[]')).toBe(true)
      expect(isValidFieldType('enum(a,b,c)!')).toBe(true)
    })
  })
})

describe('TypeDefinition Index Modifiers', () => {
  describe('Basic index (#)', () => {
    it('parses index modifier', () => {
      const result = parseFieldType('string#')
      expect(result.type).toBe('string')
      expect(result.index).toBe(true)
    })

    it('parses indexed + required (#!)', () => {
      const result = parseFieldType('string#!')
      expect(result.type).toBe('string')
      expect(result.index).toBe(true)
      expect(result.required).toBe(true)
    })

    it('works in schema', () => {
      const schema: Schema = {
        User: {
          status: 'string#',
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
    })
  })

  describe('Unique index (##)', () => {
    it('parses unique index modifier', () => {
      const result = parseFieldType('email##')
      expect(result.type).toBe('email')
      expect(result.index).toBe('unique')
    })

    it('parses unique + required (##!)', () => {
      const result = parseFieldType('email##!')
      expect(result.type).toBe('email')
      expect(result.index).toBe('unique')
      expect(result.required).toBe(true)
    })
  })

  describe('Full-text search index (#fts)', () => {
    it('parses FTS index modifier', () => {
      const result = parseFieldType('text#fts')
      expect(result.type).toBe('text')
      expect(result.index).toBe('fts')
    })

    it('parses FTS + required (#fts!)', () => {
      const result = parseFieldType('text#fts!')
      expect(result.type).toBe('text')
      expect(result.index).toBe('fts')
      expect(result.required).toBe(true)
    })
  })

  describe('Vector index (#vec)', () => {
    it('parses vector index modifier', () => {
      const result = parseFieldType('vector(1536)#vec')
      expect(result.type).toBe('vector(1536)')
      expect(result.index).toBe('vector')
    })
  })

  describe('Hash index (#hash)', () => {
    it('parses hash index modifier', () => {
      const result = parseFieldType('string#hash')
      expect(result.type).toBe('string')
      expect(result.index).toBe('hash')
    })
  })
})

describe('TypeDefinition Relationships', () => {
  describe('Forward relations (->)', () => {
    it('parses forward relation', () => {
      const result = parseRelation('-> User.posts')
      expect(result).toMatchObject({
        toType: 'User',
        reverse: 'posts',
        direction: 'forward',
        mode: 'exact',
        isArray: false,
      })
    })

    it('parses forward relation array (->[])', () => {
      const result = parseRelation('-> Category.posts[]')
      expect(result).toMatchObject({
        toType: 'Category',
        reverse: 'posts',
        isArray: true,
      })
    })

    it('validates forward relation string', () => {
      expect(isValidRelationString('-> User.posts')).toBe(true)
      expect(isValidRelationString('-> User.posts[]')).toBe(true)
    })

    it('rejects invalid forward relations', () => {
      expect(isValidRelationString('-> User')).toBe(false) // missing field
      // Note: ->User.posts is accepted (space is optional in the implementation)
      expect(isValidRelationString('->User.posts')).toBe(true)
      expect(isValidRelationString('')).toBe(false) // empty string
      expect(isValidRelationString('User.posts')).toBe(false) // missing arrow
    })

    it('works in schema with matching reverse', () => {
      const schema: Schema = {
        Post: {
          title: 'string!',
          author: '-> User.posts',
        },
        User: {
          name: 'string!',
          posts: '<- Post.author[]',
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
    })
  })

  describe('Backward relations (<-)', () => {
    it('parses backward relation', () => {
      const result = parseRelation('<- Comment.post')
      expect(result).toMatchObject({
        fromType: 'Comment',
        fromField: 'post',
        direction: 'backward',
        mode: 'exact',
        isArray: false,
      })
    })

    it('parses backward relation array (<-[])', () => {
      const result = parseRelation('<- Comment.post[]')
      expect(result).toMatchObject({
        fromType: 'Comment',
        fromField: 'post',
        isArray: true,
      })
    })

    it('validates backward relation string', () => {
      expect(isValidRelationString('<- Comment.post')).toBe(true)
      expect(isValidRelationString('<- Comment.post[]')).toBe(true)
    })
  })

  describe('Fuzzy forward relations (~>)', () => {
    it('parses fuzzy forward without field', () => {
      const result = parseRelation('~> Topic')
      expect(result).toMatchObject({
        toType: 'Topic',
        direction: 'forward',
        mode: 'fuzzy',
      })
    })

    it('parses fuzzy forward with field', () => {
      const result = parseRelation('~> Topic.related')
      expect(result).toMatchObject({
        toType: 'Topic',
        reverse: 'related',
        direction: 'forward',
        mode: 'fuzzy',
      })
    })

    it('parses fuzzy forward array', () => {
      const result = parseRelation('~> Topic[]')
      expect(result).toMatchObject({
        toType: 'Topic',
        isArray: true,
        mode: 'fuzzy',
      })
    })

    it('validates fuzzy forward relation string', () => {
      expect(isValidRelationString('~> Topic')).toBe(true)
      expect(isValidRelationString('~> Topic.related')).toBe(true)
      expect(isValidRelationString('~> Topic[]')).toBe(true)
    })
  })

  describe('Fuzzy backward relations (<~)', () => {
    it('parses fuzzy backward', () => {
      const result = parseRelation('<~ Source')
      expect(result).toMatchObject({
        fromType: 'Source',
        direction: 'backward',
        mode: 'fuzzy',
      })
    })

    it('parses fuzzy backward with field', () => {
      const result = parseRelation('<~ Source.related')
      expect(result).toMatchObject({
        fromType: 'Source',
        fromField: 'related',
        mode: 'fuzzy',
      })
    })

    it('validates fuzzy backward relation string', () => {
      expect(isValidRelationString('<~ Source')).toBe(true)
      expect(isValidRelationString('<~ Source.related')).toBe(true)
    })
  })

  describe('Relationship validation', () => {
    it('detects missing target type', () => {
      const schema: Schema = {
        Post: {
          title: 'string!',
          author: '-> NonExistent.posts',
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.code === 'MISSING_TARGET_TYPE')).toBe(true)
    })

    it('detects missing reverse field', () => {
      const schema: Schema = {
        Post: {
          title: 'string!',
          author: '-> User.posts',
        },
        User: {
          name: 'string!',
          // missing posts field
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.code === 'MISSING_REVERSE_FIELD')).toBe(true)
    })

    it('detects relationship mismatch when reverse does not point back', () => {
      // The implementation allows forward-forward pairs that point to each other.
      // A mismatch occurs when the reverse field points to a different field.
      const schema: Schema = {
        Post: {
          title: 'string!',
          author: '-> User.posts',
        },
        User: {
          name: 'string!',
          posts: '-> Post.title', // points to wrong field - should point to 'author'
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.code === 'RELATIONSHIP_MISMATCH')).toBe(true)
    })

    it('accepts bidirectional forward-forward relationships that point to each other', () => {
      // Two forward relations that point to each other are valid
      const schema: Schema = {
        Post: {
          title: 'string!',
          author: '-> User.posts',
        },
        User: {
          name: 'string!',
          posts: '-> Post.author',
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
    })
  })

  describe('Runtime relationship validation', () => {
    it('validates relationship string format', () => {
      const validator = new SchemaValidator({
        Post: {
          title: 'string!',
          author: '-> User.posts',
        },
        User: {
          name: 'string!',
          posts: '<- Post.author[]',
        },
      }, { mode: 'permissive' })

      expect(validator.validate('Post', {
        title: 'Hello',
        author: 'users/alice',
      }).valid).toBe(true)

      expect(validator.validate('Post', {
        title: 'Hello',
        author: 'invalid-id',
      }).valid).toBe(false)
    })

    it('validates relationship object format', () => {
      const validator = new SchemaValidator({
        Post: {
          title: 'string!',
          author: '-> User.posts',
        },
        User: {
          name: 'string!',
          posts: '<- Post.author[]',
        },
      }, { mode: 'permissive' })

      expect(validator.validate('Post', {
        title: 'Hello',
        author: { Alice: 'users/alice' },
      }).valid).toBe(true)
    })

    it('validates array relationships', () => {
      const validator = new SchemaValidator({
        Post: {
          title: 'string!',
          categories: '-> Category.posts[]',
        },
        Category: {
          name: 'string!',
          posts: '<- Post.categories[]',
        },
      }, { mode: 'permissive' })

      expect(validator.validate('Post', {
        title: 'Hello',
        categories: ['categories/tech', 'categories/news'],
      }).valid).toBe(true)
    })
  })
})

describe('TypeDefinition Field Definition Objects', () => {
  describe('Basic FieldDefinition', () => {
    it('accepts type property', () => {
      const schema: Schema = {
        User: {
          name: { type: 'string!' },
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
    })

    it('requires type property', () => {
      const schema: Schema = {
        User: {
          name: { required: true } as any, // missing type
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.code === 'MISSING_TYPE')).toBe(true)
    })
  })

  describe('FieldDefinition with required', () => {
    it('accepts required: true', () => {
      const schema: Schema = {
        User: {
          name: { type: 'string', required: true },
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(true)

      const parsed = parseSchema(schema)
      expect(parsed.getType('User')?.fields.get('name')?.required).toBe(true)
    })

    it('accepts required: false', () => {
      const schema: Schema = {
        User: {
          bio: { type: 'string', required: false },
        },
      }

      const parsed = parseSchema(schema)
      expect(parsed.getType('User')?.fields.get('bio')?.required).toBe(false)
    })
  })

  describe('FieldDefinition with default', () => {
    it('accepts default value', () => {
      const schema: Schema = {
        User: {
          role: { type: 'string', default: 'user' },
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(true)

      const parsed = parseSchema(schema)
      expect(parsed.getType('User')?.fields.get('role')?.default).toBe('user')
    })
  })

  describe('FieldDefinition with index', () => {
    it('accepts index: true', () => {
      const schema: Schema = {
        User: {
          status: { type: 'string', index: true },
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(true)

      const parsed = parseSchema(schema)
      expect(parsed.getType('User')?.fields.get('status')?.index).toBe(true)
    })

    it('accepts index: "unique"', () => {
      const schema: Schema = {
        User: {
          email: { type: 'email', index: 'unique' },
        },
      }

      const parsed = parseSchema(schema)
      expect(parsed.getType('User')?.fields.get('email')?.index).toBe('unique')
    })

    it('accepts index: "fts"', () => {
      const schema: Schema = {
        Article: {
          content: { type: 'text', index: 'fts' },
        },
      }

      const parsed = parseSchema(schema)
      expect(parsed.getType('Article')?.fields.get('content')?.index).toBe('fts')
    })

    it('accepts index: "vector"', () => {
      const schema: Schema = {
        Document: {
          embedding: { type: 'vector(1536)', index: 'vector' },
        },
      }

      const parsed = parseSchema(schema)
      expect(parsed.getType('Document')?.fields.get('embedding')?.index).toBe('vector')
    })

    it('accepts index: "hash"', () => {
      const schema: Schema = {
        User: {
          apiKey: { type: 'string', index: 'hash' },
        },
      }

      const parsed = parseSchema(schema)
      expect(parsed.getType('User')?.fields.get('apiKey')?.index).toBe('hash')
    })

    it('rejects invalid index type', () => {
      const schema: Schema = {
        User: {
          field: { type: 'string', index: 'invalid' as any },
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.code === 'INVALID_INDEX_TYPE')).toBe(true)
    })
  })

  describe('FieldDefinition with description', () => {
    it('accepts description', () => {
      const schema: Schema = {
        User: {
          email: { type: 'email!', description: 'Primary email address' },
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
    })
  })

  describe('FieldDefinition with validation constraints', () => {
    it('accepts pattern for strings', () => {
      const fieldDef: FieldDefinition = {
        type: 'string',
        pattern: '^[A-Z]{2}\\d{4}$',
      }

      const schema: Schema = {
        Product: {
          sku: fieldDef,
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
    })

    it('accepts min/max for numbers', () => {
      const fieldDef: FieldDefinition = {
        type: 'int',
        min: 0,
        max: 100,
      }

      const schema: Schema = {
        Review: {
          rating: fieldDef,
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
    })

    it('accepts minLength/maxLength for strings', () => {
      const fieldDef: FieldDefinition = {
        type: 'string',
        minLength: 3,
        maxLength: 50,
      }

      const schema: Schema = {
        User: {
          username: fieldDef,
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
    })

    it('accepts enum array for allowed values', () => {
      const fieldDef: FieldDefinition = {
        type: 'string',
        enum: ['draft', 'published', 'archived'],
      }

      const schema: Schema = {
        Post: {
          status: fieldDef,
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
    })
  })

  describe('FieldDefinition with vector options', () => {
    it('accepts dimensions', () => {
      const fieldDef: FieldDefinition = {
        type: 'vector(1536)',
        dimensions: 1536,
      }

      const schema: Schema = {
        Document: {
          embedding: fieldDef,
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
    })

    it('accepts metric', () => {
      const fieldDef: FieldDefinition = {
        type: 'vector(1536)',
        metric: 'cosine',
      }

      const schema: Schema = {
        Document: {
          embedding: fieldDef,
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
    })
  })

  describe('FieldDefinition with FTS options', () => {
    it('accepts ftsOptions', () => {
      const fieldDef: FieldDefinition = {
        type: 'text',
        index: 'fts',
        ftsOptions: {
          language: 'english',
          weight: 2,
        },
      }

      const schema: Schema = {
        Article: {
          title: fieldDef,
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
    })
  })

  describe('FieldDefinition with meta', () => {
    it('accepts custom metadata', () => {
      const fieldDef: FieldDefinition = {
        type: 'string',
        meta: {
          displayName: 'Full Name',
          order: 1,
        },
      }

      const schema: Schema = {
        User: {
          name: fieldDef,
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
    })
  })
})

describe('Complete TypeDefinition examples', () => {
  it('validates a comprehensive User type', () => {
    const schema: Schema = {
      User: {
        $type: 'schema:Person',
        $ns: 'https://example.com/users',
        $shred: ['status', 'role', 'createdAt'],
        $description: 'A user in the system',
        $indexes: [
          { fields: ['lastName', 'firstName'], name: 'name_idx' },
        ],
        $visibility: 'private',

        // Required string fields
        firstName: 'string!',
        lastName: 'string!',
        email: 'email##!', // unique indexed, required

        // Optional fields with various types
        bio: 'text?',
        age: 'int?',
        website: 'url?',
        avatar: 'binary?',

        // Indexed fields
        status: 'string#', // indexed
        role: 'enum(admin,user,guest) = "user"', // with default

        // Arrays
        tags: 'string[]',
        favoriteNumbers: 'int[]',

        // Dates
        createdAt: 'datetime!',
        updatedAt: 'datetime?',
        lastLoginAt: 'timestamp?',

        // Complex field definitions
        settings: {
          type: 'json',
          description: 'User preferences',
          default: {},
        },
        profile: {
          type: 'json',
          required: false,
        },

        // Relationships
        posts: '<- Post.author[]',
        followers: '<- Follow.follower[]',
        following: '<- Follow.following[]',
      },

      Post: {
        title: 'string!',
        content: 'markdown!',
        author: '-> User.posts',
      },

      Follow: {
        follower: '-> User.followers',
        following: '-> User.following',
        createdAt: 'datetime!',
      },
    }

    const result = validateSchema(schema)
    expect(result.valid).toBe(true)

    const parsed = parseSchema(schema)
    const userType = parsed.getType('User')
    expect(userType).toBeDefined()
    expect(userType?.typeUri).toBe('schema:Person')
    expect(userType?.namespace).toBe('https://example.com/users')
    expect(userType?.shredFields).toEqual(['status', 'role', 'createdAt'])
    expect(userType?.indexes).toHaveLength(1)
  })

  it('validates an abstract base type with inheritance', () => {
    const schema: Schema = {
      BaseEntity: {
        $abstract: true,
        id: 'uuid!',
        createdAt: 'datetime!',
        updatedAt: 'datetime?',
        createdBy: 'string?',
      },

      User: {
        $extends: 'BaseEntity',
        name: 'string!',
        email: 'email!',
      },

      Post: {
        $extends: 'BaseEntity',
        title: 'string!',
        content: 'text!',
      },
    }

    const result = validateSchema(schema, { checkRelationships: false })
    expect(result.valid).toBe(true)

    const parsed = parseSchema(schema)
    expect(parsed.getType('BaseEntity')?.isAbstract).toBe(true)
    expect(parsed.getType('User')?.extends).toBe('BaseEntity')
    expect(parsed.getType('Post')?.extends).toBe('BaseEntity')
  })

  it('validates a type with all parametric types', () => {
    const schema: Schema = {
      Product: {
        name: 'string!',
        sku: 'char(10)!',
        description: 'varchar(1000)?',
        price: 'decimal(10,2)!',
        status: 'enum(draft,active,discontinued)!',
        embedding: 'vector(1536)?',
      },
    }

    const result = validateSchema(schema)
    expect(result.valid).toBe(true)
  })

  it('validates a type with all index modifiers', () => {
    const schema: Schema = {
      SearchableDocument: {
        slug: 'string##!', // unique index
        category: 'string#', // regular index
        content: 'text#fts', // FTS index
        embedding: 'vector(1536)#vec', // vector index
        apiKey: 'string#hash', // hash index
      },
    }

    const result = validateSchema(schema)
    expect(result.valid).toBe(true)
  })
})
