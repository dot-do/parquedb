/**
 * Schema Parser Tests
 *
 * Tests for parseFieldType, parseRelation, isRelationString, and schema validation.
 */

import { describe, it, expect } from 'vitest'
import {
  parseFieldType,
  parseRelation,
  isRelationString,
  validateSchema,
  validateTypeDefinition,
  validateRelationshipTargets,
  parseSchema,
  isValidFieldType,
  isValidRelationString,
} from '../../src/parser'
import type { Schema, TypeDefinition } from '../../src/types/schema'

// =============================================================================
// parseFieldType Tests
// =============================================================================

describe('parseFieldType', () => {
  describe('basic types', () => {
    it('should parse string type', () => {
      const result = parseFieldType('string')
      expect(result.type).toBe('string')
      expect(result.required).toBe(false)
      expect(result.isArray).toBe(false)
      expect(result.default).toBeUndefined()
    })

    it('should parse int type', () => {
      const result = parseFieldType('int')
      expect(result.type).toBe('int')
      expect(result.required).toBe(false)
      expect(result.isArray).toBe(false)
    })

    it('should parse boolean type', () => {
      const result = parseFieldType('boolean')
      expect(result.type).toBe('boolean')
      expect(result.required).toBe(false)
      expect(result.isArray).toBe(false)
    })

    it('should parse number type', () => {
      const result = parseFieldType('number')
      expect(result.type).toBe('number')
    })

    it('should parse float type', () => {
      const result = parseFieldType('float')
      expect(result.type).toBe('float')
    })

    it('should parse double type', () => {
      const result = parseFieldType('double')
      expect(result.type).toBe('double')
    })

    it('should parse date type', () => {
      const result = parseFieldType('date')
      expect(result.type).toBe('date')
    })

    it('should parse datetime type', () => {
      const result = parseFieldType('datetime')
      expect(result.type).toBe('datetime')
    })

    it('should parse timestamp type', () => {
      const result = parseFieldType('timestamp')
      expect(result.type).toBe('timestamp')
    })

    it('should parse uuid type', () => {
      const result = parseFieldType('uuid')
      expect(result.type).toBe('uuid')
    })

    it('should parse email type', () => {
      const result = parseFieldType('email')
      expect(result.type).toBe('email')
    })

    it('should parse url type', () => {
      const result = parseFieldType('url')
      expect(result.type).toBe('url')
    })

    it('should parse json type', () => {
      const result = parseFieldType('json')
      expect(result.type).toBe('json')
    })

    it('should parse binary type', () => {
      const result = parseFieldType('binary')
      expect(result.type).toBe('binary')
    })

    it('should parse text type', () => {
      const result = parseFieldType('text')
      expect(result.type).toBe('text')
    })

    it('should parse markdown type', () => {
      const result = parseFieldType('markdown')
      expect(result.type).toBe('markdown')
    })
  })

  describe('required modifier (!)', () => {
    it('should parse string! as required', () => {
      const result = parseFieldType('string!')
      expect(result.type).toBe('string')
      expect(result.required).toBe(true)
      expect(result.isArray).toBe(false)
    })

    it('should parse int! as required', () => {
      const result = parseFieldType('int!')
      expect(result.type).toBe('int')
      expect(result.required).toBe(true)
    })

    it('should parse boolean! as required', () => {
      const result = parseFieldType('boolean!')
      expect(result.type).toBe('boolean')
      expect(result.required).toBe(true)
    })

    it('should parse uuid! as required', () => {
      const result = parseFieldType('uuid!')
      expect(result.type).toBe('uuid')
      expect(result.required).toBe(true)
    })

    it('should parse datetime! as required', () => {
      const result = parseFieldType('datetime!')
      expect(result.type).toBe('datetime')
      expect(result.required).toBe(true)
    })
  })

  describe('optional modifier (?)', () => {
    it('should parse string? as optional', () => {
      const result = parseFieldType('string?')
      expect(result.type).toBe('string')
      expect(result.required).toBe(false)
      expect(result.isArray).toBe(false)
    })

    it('should parse int? as optional', () => {
      const result = parseFieldType('int?')
      expect(result.type).toBe('int')
      expect(result.required).toBe(false)
    })

    it('should parse datetime? as optional', () => {
      const result = parseFieldType('datetime?')
      expect(result.type).toBe('datetime')
      expect(result.required).toBe(false)
    })
  })

  describe('array modifier ([])', () => {
    it('should parse string[] as array', () => {
      const result = parseFieldType('string[]')
      expect(result.type).toBe('string')
      expect(result.required).toBe(false)
      expect(result.isArray).toBe(true)
    })

    it('should parse int[] as array', () => {
      const result = parseFieldType('int[]')
      expect(result.type).toBe('int')
      expect(result.isArray).toBe(true)
    })

    it('should parse uuid[] as array', () => {
      const result = parseFieldType('uuid[]')
      expect(result.type).toBe('uuid')
      expect(result.isArray).toBe(true)
    })
  })

  describe('required array modifier ([]!)', () => {
    it('should parse string[]! as required array', () => {
      const result = parseFieldType('string[]!')
      expect(result.type).toBe('string')
      expect(result.required).toBe(true)
      expect(result.isArray).toBe(true)
    })

    it('should parse int[]! as required array', () => {
      const result = parseFieldType('int[]!')
      expect(result.type).toBe('int')
      expect(result.required).toBe(true)
      expect(result.isArray).toBe(true)
    })

    it('should parse uuid[]! as required array', () => {
      const result = parseFieldType('uuid[]!')
      expect(result.type).toBe('uuid')
      expect(result.required).toBe(true)
      expect(result.isArray).toBe(true)
    })
  })

  describe('default values', () => {
    it('should parse string with quoted default', () => {
      const result = parseFieldType('string = "default"')
      expect(result.type).toBe('string')
      expect(result.default).toBe('"default"')
    })

    it('should parse string with single-quoted default', () => {
      const result = parseFieldType("string = 'default'")
      expect(result.type).toBe('string')
      expect(result.default).toBe("'default'")
    })

    it('should parse int with numeric default', () => {
      const result = parseFieldType('int = 0')
      expect(result.type).toBe('int')
      expect(result.default).toBe('0')
    })

    it('should parse int with negative default', () => {
      const result = parseFieldType('int = -1')
      expect(result.type).toBe('int')
      expect(result.default).toBe('-1')
    })

    it('should parse boolean with true default', () => {
      const result = parseFieldType('boolean = true')
      expect(result.type).toBe('boolean')
      expect(result.default).toBe('true')
    })

    it('should parse boolean with false default', () => {
      const result = parseFieldType('boolean = false')
      expect(result.type).toBe('boolean')
      expect(result.default).toBe('false')
    })

    it('should parse float with decimal default', () => {
      const result = parseFieldType('float = 3.14')
      expect(result.type).toBe('float')
      expect(result.default).toBe('3.14')
    })

    it('should preserve spacing in default values', () => {
      const result = parseFieldType('string = "hello world"')
      expect(result.default).toBe('"hello world"')
    })

    it('should handle default with required modifier', () => {
      const result = parseFieldType('string! = "default"')
      expect(result.type).toBe('string')
      expect(result.required).toBe(true)
      expect(result.default).toBe('"default"')
    })
  })

  describe('parametric types', () => {
    it('should parse decimal(10,2)', () => {
      const result = parseFieldType('decimal(10,2)')
      expect(result.type).toBe('decimal(10,2)')
      expect(result.required).toBe(false)
      expect(result.isArray).toBe(false)
    })

    it('should parse decimal(18,4)', () => {
      const result = parseFieldType('decimal(18,4)')
      expect(result.type).toBe('decimal(18,4)')
    })

    it('should parse varchar(255)', () => {
      const result = parseFieldType('varchar(255)')
      expect(result.type).toBe('varchar(255)')
    })

    it('should parse varchar(50)', () => {
      const result = parseFieldType('varchar(50)')
      expect(result.type).toBe('varchar(50)')
    })

    it('should parse char(36)', () => {
      const result = parseFieldType('char(36)')
      expect(result.type).toBe('char(36)')
    })

    it('should parse vector(1536)', () => {
      const result = parseFieldType('vector(1536)')
      expect(result.type).toBe('vector(1536)')
    })

    it('should parse vector(768)', () => {
      const result = parseFieldType('vector(768)')
      expect(result.type).toBe('vector(768)')
    })

    it('should parse vector(3072)', () => {
      const result = parseFieldType('vector(3072)')
      expect(result.type).toBe('vector(3072)')
    })

    it('should parse enum(a,b,c)', () => {
      const result = parseFieldType('enum(a,b,c)')
      expect(result.type).toBe('enum(a,b,c)')
    })

    it('should parse enum(draft,published,archived)', () => {
      const result = parseFieldType('enum(draft,published,archived)')
      expect(result.type).toBe('enum(draft,published,archived)')
    })

    it('should parse enum with spaces in values', () => {
      const result = parseFieldType('enum(in_progress,done,cancelled)')
      expect(result.type).toBe('enum(in_progress,done,cancelled)')
    })
  })

  describe('parametric types with modifiers', () => {
    it('should parse decimal(10,2)!', () => {
      const result = parseFieldType('decimal(10,2)!')
      expect(result.type).toBe('decimal(10,2)')
      expect(result.required).toBe(true)
    })

    it('should parse varchar(255)!', () => {
      const result = parseFieldType('varchar(255)!')
      expect(result.type).toBe('varchar(255)')
      expect(result.required).toBe(true)
    })

    it('should parse vector(1536)?', () => {
      const result = parseFieldType('vector(1536)?')
      expect(result.type).toBe('vector(1536)')
      expect(result.required).toBe(false)
    })

    it('should parse enum(a,b,c)[]', () => {
      const result = parseFieldType('enum(a,b,c)[]')
      expect(result.type).toBe('enum(a,b,c)')
      expect(result.isArray).toBe(true)
    })

    it('should parse vector(1536)[]!', () => {
      const result = parseFieldType('vector(1536)[]!')
      expect(result.type).toBe('vector(1536)')
      expect(result.required).toBe(true)
      expect(result.isArray).toBe(true)
    })

    it('should parse decimal(10,2) = 0.00', () => {
      const result = parseFieldType('decimal(10,2) = 0.00')
      expect(result.type).toBe('decimal(10,2)')
      expect(result.default).toBe('0.00')
    })

    it('should parse enum(a,b,c)! = a', () => {
      const result = parseFieldType('enum(a,b,c)! = a')
      expect(result.type).toBe('enum(a,b,c)')
      expect(result.required).toBe(true)
      expect(result.default).toBe('a')
    })
  })

  describe('edge cases', () => {
    it('should handle extra whitespace', () => {
      const result = parseFieldType('  string  ')
      expect(result.type).toBe('string')
    })

    it('should handle whitespace around equals sign', () => {
      const result = parseFieldType('int   =   42')
      expect(result.type).toBe('int')
      expect(result.default).toBe('42')
    })

    it('should handle empty string input', () => {
      const result = parseFieldType('')
      expect(result.type).toBe('')
    })
  })
})

// =============================================================================
// parseRelation Tests
// =============================================================================

describe('parseRelation', () => {
  describe('forward relations (->)', () => {
    it('should parse -> User.posts', () => {
      const result = parseRelation('-> User.posts')
      expect(result).not.toBeNull()
      expect(result!.toType).toBe('User')
      expect(result!.reverse).toBe('posts')
      expect(result!.isArray).toBe(false)
      expect(result!.direction).toBe('forward')
      expect(result!.mode).toBe('exact')
    })

    it('should parse -> User.posts[]', () => {
      const result = parseRelation('-> User.posts[]')
      expect(result).not.toBeNull()
      expect(result!.toType).toBe('User')
      expect(result!.reverse).toBe('posts')
      expect(result!.isArray).toBe(true)
      expect(result!.direction).toBe('forward')
    })

    it('should parse -> Category.items', () => {
      const result = parseRelation('-> Category.items')
      expect(result).not.toBeNull()
      expect(result!.toType).toBe('Category')
      expect(result!.reverse).toBe('items')
    })

    it('should parse -> Comment.author', () => {
      const result = parseRelation('-> Comment.author')
      expect(result).not.toBeNull()
      expect(result!.toType).toBe('Comment')
      expect(result!.reverse).toBe('author')
    })

    it('should handle extra whitespace after arrow', () => {
      const result = parseRelation('->  User.posts')
      expect(result).not.toBeNull()
      expect(result!.toType).toBe('User')
    })

    it('should handle no whitespace after arrow', () => {
      const result = parseRelation('->User.posts')
      expect(result).not.toBeNull()
      expect(result!.toType).toBe('User')
    })
  })

  describe('backward relations (<-)', () => {
    it('should parse <- Comment.post', () => {
      const result = parseRelation('<- Comment.post')
      expect(result).not.toBeNull()
      expect(result!.fromType).toBe('Comment')
      expect(result!.fromField).toBe('post')
      expect(result!.isArray).toBe(false)
      expect(result!.direction).toBe('backward')
      expect(result!.mode).toBe('exact')
    })

    it('should parse <- Comment.post[]', () => {
      const result = parseRelation('<- Comment.post[]')
      expect(result).not.toBeNull()
      expect(result!.fromType).toBe('Comment')
      expect(result!.fromField).toBe('post')
      expect(result!.isArray).toBe(true)
      expect(result!.direction).toBe('backward')
    })

    it('should parse <- Post.author', () => {
      const result = parseRelation('<- Post.author')
      expect(result).not.toBeNull()
      expect(result!.fromType).toBe('Post')
      expect(result!.fromField).toBe('author')
    })

    it('should parse <- Like.target[]', () => {
      const result = parseRelation('<- Like.target[]')
      expect(result).not.toBeNull()
      expect(result!.fromType).toBe('Like')
      expect(result!.fromField).toBe('target')
      expect(result!.isArray).toBe(true)
    })
  })

  describe('fuzzy forward relations (~>)', () => {
    it('should parse ~> Topic', () => {
      const result = parseRelation('~> Topic')
      expect(result).not.toBeNull()
      expect(result!.toType).toBe('Topic')
      expect(result!.reverse).toBe('')
      expect(result!.isArray).toBe(false)
      expect(result!.direction).toBe('forward')
      expect(result!.mode).toBe('fuzzy')
    })

    it('should parse ~> Topic.interests', () => {
      const result = parseRelation('~> Topic.interests')
      expect(result).not.toBeNull()
      expect(result!.toType).toBe('Topic')
      expect(result!.reverse).toBe('interests')
      expect(result!.direction).toBe('forward')
      expect(result!.mode).toBe('fuzzy')
    })

    it('should parse ~> Category[]', () => {
      const result = parseRelation('~> Category[]')
      expect(result).not.toBeNull()
      expect(result!.toType).toBe('Category')
      expect(result!.isArray).toBe(true)
      expect(result!.mode).toBe('fuzzy')
    })

    it('should parse ~> Tag.articles[]', () => {
      const result = parseRelation('~> Tag.articles[]')
      expect(result).not.toBeNull()
      expect(result!.toType).toBe('Tag')
      expect(result!.reverse).toBe('articles')
      expect(result!.isArray).toBe(true)
    })
  })

  describe('fuzzy backward relations (<~)', () => {
    it('should parse <~ Source', () => {
      const result = parseRelation('<~ Source')
      expect(result).not.toBeNull()
      expect(result!.fromType).toBe('Source')
      expect(result!.fromField).toBe('')
      expect(result!.isArray).toBe(false)
      expect(result!.direction).toBe('backward')
      expect(result!.mode).toBe('fuzzy')
    })

    it('should parse <~ Source.items', () => {
      const result = parseRelation('<~ Source.items')
      expect(result).not.toBeNull()
      expect(result!.fromType).toBe('Source')
      expect(result!.fromField).toBe('items')
      expect(result!.mode).toBe('fuzzy')
    })

    it('should parse <~ Reference[]', () => {
      const result = parseRelation('<~ Reference[]')
      expect(result).not.toBeNull()
      expect(result!.fromType).toBe('Reference')
      expect(result!.isArray).toBe(true)
    })

    it('should parse <~ Citation.target[]', () => {
      const result = parseRelation('<~ Citation.target[]')
      expect(result).not.toBeNull()
      expect(result!.fromType).toBe('Citation')
      expect(result!.fromField).toBe('target')
      expect(result!.isArray).toBe(true)
    })
  })

  describe('invalid relations', () => {
    it('should return null for plain string', () => {
      const result = parseRelation('string')
      expect(result).toBeNull()
    })

    it('should return null for type with modifier', () => {
      const result = parseRelation('string!')
      expect(result).toBeNull()
    })

    it('should return null for empty string', () => {
      const result = parseRelation('')
      expect(result).toBeNull()
    })

    it('should return null for malformed forward relation', () => {
      const result = parseRelation('-> User')
      expect(result).toBeNull()
    })

    it('should return null for malformed backward relation', () => {
      const result = parseRelation('<- Comment')
      expect(result).toBeNull()
    })

    it('should return null for wrong arrow direction', () => {
      const result = parseRelation('-< User.posts')
      expect(result).toBeNull()
    })

    it('should return null for missing target', () => {
      const result = parseRelation('->')
      expect(result).toBeNull()
    })
  })
})

// =============================================================================
// isRelationString Tests
// =============================================================================

describe('isRelationString', () => {
  describe('should return true for relation strings', () => {
    it('should detect forward relation', () => {
      expect(isRelationString('-> User.posts')).toBe(true)
    })

    it('should detect forward relation with array', () => {
      expect(isRelationString('-> User.posts[]')).toBe(true)
    })

    it('should detect backward relation', () => {
      expect(isRelationString('<- Comment.post')).toBe(true)
    })

    it('should detect backward relation with array', () => {
      expect(isRelationString('<- Comment.post[]')).toBe(true)
    })

    it('should detect fuzzy forward relation', () => {
      expect(isRelationString('~> Topic')).toBe(true)
    })

    it('should detect fuzzy forward relation with field', () => {
      expect(isRelationString('~> Topic.interests')).toBe(true)
    })

    it('should detect fuzzy backward relation', () => {
      expect(isRelationString('<~ Source')).toBe(true)
    })

    it('should detect fuzzy backward relation with field', () => {
      expect(isRelationString('<~ Source.items')).toBe(true)
    })
  })

  describe('should return false for non-relation strings', () => {
    it('should reject plain type string', () => {
      expect(isRelationString('string')).toBe(false)
    })

    it('should reject required type', () => {
      expect(isRelationString('string!')).toBe(false)
    })

    it('should reject optional type', () => {
      expect(isRelationString('string?')).toBe(false)
    })

    it('should reject array type', () => {
      expect(isRelationString('string[]')).toBe(false)
    })

    it('should reject parametric type', () => {
      expect(isRelationString('vector(1536)')).toBe(false)
    })

    it('should reject type with default', () => {
      expect(isRelationString('string = "default"')).toBe(false)
    })

    it('should reject empty string', () => {
      expect(isRelationString('')).toBe(false)
    })

    it('should reject number-like string', () => {
      expect(isRelationString('123')).toBe(false)
    })

    it('should reject partial arrow', () => {
      expect(isRelationString('>')).toBe(false)
    })

    it('should reject single dash', () => {
      expect(isRelationString('-')).toBe(false)
    })
  })
})

// =============================================================================
// Schema Validation Tests
// =============================================================================

describe('validateSchema', () => {
  describe('valid schemas', () => {
    it('should validate a simple schema', () => {
      const schema: Schema = {
        User: {
          name: 'string!',
          email: 'email!',
        },
      }
      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
      expect(result.errors).toHaveLength(0)
    })

    it('should validate schema with multiple types', () => {
      const schema: Schema = {
        User: {
          name: 'string!',
          email: 'email!',
        },
        Post: {
          title: 'string!',
          content: 'text!',
        },
      }
      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
    })

    it('should validate schema with relationships', () => {
      const schema: Schema = {
        User: {
          name: 'string!',
          posts: '<- Post.author[]',
        },
        Post: {
          title: 'string!',
          author: '-> User.posts',
        },
      }
      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
    })

    it('should validate schema with metadata fields', () => {
      const schema: Schema = {
        User: {
          $type: 'schema:Person',
          $ns: 'https://example.com/users',
          $description: 'A user in the system',
          name: 'string!',
        },
      }
      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
    })

    it('should validate schema with indexes', () => {
      const schema: Schema = {
        User: {
          $indexes: [
            { fields: ['email'], unique: true },
            { fields: ['createdAt'] },
          ],
          email: 'email!',
          createdAt: 'datetime!',
        },
      }
      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
    })

    it('should validate schema with abstract types', () => {
      const schema: Schema = {
        Entity: {
          $abstract: true,
          id: 'uuid!',
          createdAt: 'datetime!',
        },
        User: {
          $extends: 'Entity',
          name: 'string!',
        },
      }
      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
    })
  })

  describe('invalid schemas', () => {
    it('should reject empty schema', () => {
      const schema: Schema = {}
      const result = validateSchema(schema)
      expect(result.valid).toBe(false)
      expect(result.errors.length).toBeGreaterThan(0)
    })

    it('should reject schema with empty type definition', () => {
      const schema: Schema = {
        User: {},
      }
      const result = validateSchema(schema)
      expect(result.valid).toBe(false)
    })

    it('should reject invalid field type', () => {
      const schema: Schema = {
        User: {
          name: 'invalidtype!' as any,
        },
      }
      const result = validateSchema(schema)
      expect(result.valid).toBe(false)
      expect(result.errors.some((e) => e.path.includes('name'))).toBe(true)
    })

    it('should reject type name starting with lowercase', () => {
      const schema: Schema = {
        user: {
          name: 'string!',
        },
      }
      const result = validateSchema(schema)
      expect(result.valid).toBe(false)
    })

    it('should reject type name with invalid characters', () => {
      const schema: Schema = {
        'User-Profile': {
          name: 'string!',
        },
      }
      const result = validateSchema(schema)
      expect(result.valid).toBe(false)
    })

    it('should reject field name starting with $ (reserved)', () => {
      const schema: Schema = {
        User: {
          $customField: 'string!', // $-prefixed fields are reserved for metadata
          name: 'string!',
        },
      }
      const result = validateSchema(schema)
      expect(result.valid).toBe(false)
    })
  })
})

describe('validateTypeDefinition', () => {
  describe('valid type definitions', () => {
    it('should validate type with string fields', () => {
      const typeDef: TypeDefinition = {
        name: 'string!',
        description: 'text?',
      }
      const result = validateTypeDefinition('User', typeDef)
      expect(result.valid).toBe(true)
    })

    it('should validate type with various field types', () => {
      const typeDef: TypeDefinition = {
        name: 'string!',
        age: 'int?',
        score: 'float',
        isActive: 'boolean = true',
        tags: 'string[]',
        metadata: 'json?',
      }
      const result = validateTypeDefinition('Profile', typeDef)
      expect(result.valid).toBe(true)
    })

    it('should validate type with parametric types', () => {
      const typeDef: TypeDefinition = {
        price: 'decimal(10,2)!',
        status: 'enum(active,inactive,pending)!',
        embedding: 'vector(1536)?',
      }
      const result = validateTypeDefinition('Product', typeDef)
      expect(result.valid).toBe(true)
    })

    it('should validate type with field definition objects', () => {
      const typeDef: TypeDefinition = {
        email: {
          type: 'email!',
          index: 'unique',
          description: 'User email address',
        },
      }
      const result = validateTypeDefinition('User', typeDef)
      expect(result.valid).toBe(true)
    })
  })

  describe('invalid type definitions', () => {
    it('should reject null type definition', () => {
      const result = validateTypeDefinition('User', null as any)
      expect(result.valid).toBe(false)
    })

    it('should reject non-object type definition', () => {
      const result = validateTypeDefinition('User', 'string' as any)
      expect(result.valid).toBe(false)
    })

    it('should reject invalid index type in field definition', () => {
      const typeDef: TypeDefinition = {
        email: {
          type: 'email!',
          index: 'invalid' as any,
        },
      }
      const result = validateTypeDefinition('User', typeDef)
      expect(result.valid).toBe(false)
    })
  })
})

describe('validateRelationshipTargets', () => {
  describe('valid relationships', () => {
    it('should validate when all relationship targets exist', () => {
      const schema: Schema = {
        User: {
          name: 'string!',
          posts: '<- Post.author[]',
        },
        Post: {
          title: 'string!',
          author: '-> User.posts',
        },
      }
      const result = validateRelationshipTargets(schema)
      expect(result.valid).toBe(true)
    })

    it('should validate bidirectional relationships', () => {
      const schema: Schema = {
        User: {
          name: 'string!',
          friends: '-> User.friends[]',
        },
      }
      const result = validateRelationshipTargets(schema)
      expect(result.valid).toBe(true)
    })

    it('should validate fuzzy relationships', () => {
      const schema: Schema = {
        Article: {
          title: 'string!',
          topics: '~> Topic[]',
        },
        Topic: {
          name: 'string!',
          articles: '<~ Article.topics[]',
        },
      }
      const result = validateRelationshipTargets(schema)
      expect(result.valid).toBe(true)
    })
  })

  describe('invalid relationships', () => {
    it('should reject relationship to non-existent type', () => {
      const schema: Schema = {
        Post: {
          title: 'string!',
          author: '-> NonExistent.posts',
        },
      }
      const result = validateRelationshipTargets(schema)
      expect(result.valid).toBe(false)
      expect(result.errors.some((e) => e.message.includes('NonExistent'))).toBe(true)
    })

    it('should reject relationship with non-existent reverse field', () => {
      const schema: Schema = {
        User: {
          name: 'string!',
        },
        Post: {
          title: 'string!',
          author: '-> User.nonExistentField',
        },
      }
      const result = validateRelationshipTargets(schema)
      expect(result.valid).toBe(false)
    })

    it('should reject mismatched relationship pairs', () => {
      const schema: Schema = {
        User: {
          name: 'string!',
          posts: '<- Post.creator[]', // references Post.creator
        },
        Post: {
          title: 'string!',
          author: '-> User.posts', // references User.posts
        },
      }
      const result = validateRelationshipTargets(schema)
      expect(result.valid).toBe(false)
    })
  })
})

// =============================================================================
// parseSchema Tests
// =============================================================================

describe('parseSchema', () => {
  it('should parse a simple schema', () => {
    const schema: Schema = {
      User: {
        name: 'string!',
        email: 'email!',
      },
    }
    const parsed = parseSchema(schema)
    expect(parsed.types.size).toBe(1)
    expect(parsed.getType('User')).toBeDefined()
  })

  it('should parse field types correctly', () => {
    const schema: Schema = {
      User: {
        name: 'string!',
        age: 'int?',
        tags: 'string[]',
      },
    }
    const parsed = parseSchema(schema)
    const userType = parsed.getType('User')!

    const nameField = userType.fields.get('name')!
    expect(nameField.type).toBe('string')
    expect(nameField.required).toBe(true)

    const ageField = userType.fields.get('age')!
    expect(ageField.type).toBe('int')
    expect(ageField.required).toBe(false)

    const tagsField = userType.fields.get('tags')!
    expect(tagsField.type).toBe('string')
    expect(tagsField.isArray).toBe(true)
  })

  it('should parse relationships', () => {
    const schema: Schema = {
      User: {
        name: 'string!',
        posts: '<- Post.author[]',
      },
      Post: {
        title: 'string!',
        author: '-> User.posts',
      },
    }
    const parsed = parseSchema(schema)
    const relationships = parsed.getRelationships()
    expect(relationships.length).toBeGreaterThan(0)
  })

  it('should parse metadata fields', () => {
    const schema: Schema = {
      User: {
        $type: 'schema:Person',
        $ns: 'https://example.com/users',
        $shred: ['status', 'createdAt'],
        $abstract: false,
        name: 'string!',
        status: 'string!',
        createdAt: 'datetime!',
      },
    }
    const parsed = parseSchema(schema)
    const userType = parsed.getType('User')!

    expect(userType.typeUri).toBe('schema:Person')
    expect(userType.namespace).toBe('https://example.com/users')
    expect(userType.shredFields).toEqual(['status', 'createdAt'])
    expect(userType.isAbstract).toBe(false)
  })

  it('should parse extends relationship', () => {
    const schema: Schema = {
      Entity: {
        $abstract: true,
        id: 'uuid!',
      },
      User: {
        $extends: 'Entity',
        name: 'string!',
      },
    }
    const parsed = parseSchema(schema)
    const userType = parsed.getType('User')!
    expect(userType.extends).toBe('Entity')
  })

  it('should return undefined for non-existent type', () => {
    const schema: Schema = {
      User: {
        name: 'string!',
      },
    }
    const parsed = parseSchema(schema)
    expect(parsed.getType('NonExistent')).toBeUndefined()
  })
})

// =============================================================================
// isValidFieldType Tests
// =============================================================================

describe('isValidFieldType', () => {
  describe('valid field types', () => {
    it('should accept primitive types', () => {
      expect(isValidFieldType('string')).toBe(true)
      expect(isValidFieldType('int')).toBe(true)
      expect(isValidFieldType('boolean')).toBe(true)
      expect(isValidFieldType('float')).toBe(true)
      expect(isValidFieldType('double')).toBe(true)
      expect(isValidFieldType('date')).toBe(true)
      expect(isValidFieldType('datetime')).toBe(true)
      expect(isValidFieldType('timestamp')).toBe(true)
      expect(isValidFieldType('uuid')).toBe(true)
      expect(isValidFieldType('email')).toBe(true)
      expect(isValidFieldType('url')).toBe(true)
      expect(isValidFieldType('json')).toBe(true)
      expect(isValidFieldType('binary')).toBe(true)
      expect(isValidFieldType('text')).toBe(true)
      expect(isValidFieldType('markdown')).toBe(true)
      expect(isValidFieldType('number')).toBe(true)
    })

    it('should accept types with modifiers', () => {
      expect(isValidFieldType('string!')).toBe(true)
      expect(isValidFieldType('string?')).toBe(true)
      expect(isValidFieldType('string[]')).toBe(true)
      expect(isValidFieldType('string[]!')).toBe(true)
    })

    it('should accept parametric types', () => {
      expect(isValidFieldType('decimal(10,2)')).toBe(true)
      expect(isValidFieldType('varchar(255)')).toBe(true)
      expect(isValidFieldType('char(36)')).toBe(true)
      expect(isValidFieldType('vector(1536)')).toBe(true)
      expect(isValidFieldType('enum(a,b,c)')).toBe(true)
    })

    it('should accept parametric types with modifiers', () => {
      expect(isValidFieldType('decimal(10,2)!')).toBe(true)
      expect(isValidFieldType('vector(1536)?')).toBe(true)
      expect(isValidFieldType('enum(a,b,c)[]')).toBe(true)
    })

    it('should accept types with defaults', () => {
      expect(isValidFieldType('string = "default"')).toBe(true)
      expect(isValidFieldType('int = 0')).toBe(true)
      expect(isValidFieldType('boolean = false')).toBe(true)
    })
  })

  describe('invalid field types', () => {
    it('should reject unknown types', () => {
      expect(isValidFieldType('unknowntype')).toBe(false)
      expect(isValidFieldType('foo')).toBe(false)
      expect(isValidFieldType('bar!')).toBe(false)
    })

    it('should reject invalid parametric syntax', () => {
      expect(isValidFieldType('decimal()')).toBe(false)
      expect(isValidFieldType('decimal(10)')).toBe(false) // decimal needs 2 params
      expect(isValidFieldType('varchar()')).toBe(false)
      expect(isValidFieldType('vector()')).toBe(false)
      expect(isValidFieldType('enum()')).toBe(false)
    })

    it('should reject invalid modifier combinations', () => {
      expect(isValidFieldType('string!?')).toBe(false)
      expect(isValidFieldType('string?!')).toBe(false)
    })

    it('should reject relation strings', () => {
      expect(isValidFieldType('-> User.posts')).toBe(false)
      expect(isValidFieldType('<- Comment.post')).toBe(false)
    })

    it('should reject empty string', () => {
      expect(isValidFieldType('')).toBe(false)
    })
  })
})

// =============================================================================
// isValidRelationString Tests
// =============================================================================

describe('isValidRelationString', () => {
  describe('valid relation strings', () => {
    it('should accept forward relations', () => {
      expect(isValidRelationString('-> User.posts')).toBe(true)
      expect(isValidRelationString('-> User.posts[]')).toBe(true)
    })

    it('should accept backward relations', () => {
      expect(isValidRelationString('<- Comment.post')).toBe(true)
      expect(isValidRelationString('<- Comment.post[]')).toBe(true)
    })

    it('should accept fuzzy relations', () => {
      expect(isValidRelationString('~> Topic')).toBe(true)
      expect(isValidRelationString('~> Topic.interests')).toBe(true)
      expect(isValidRelationString('<~ Source')).toBe(true)
      expect(isValidRelationString('<~ Source.items')).toBe(true)
    })
  })

  describe('invalid relation strings', () => {
    it('should reject malformed forward relations', () => {
      expect(isValidRelationString('-> User')).toBe(false) // missing field
      expect(isValidRelationString('->')).toBe(false)
      expect(isValidRelationString('-> .posts')).toBe(false)
    })

    it('should reject malformed backward relations', () => {
      expect(isValidRelationString('<- Comment')).toBe(false) // missing field
      expect(isValidRelationString('<-')).toBe(false)
      expect(isValidRelationString('<- .post')).toBe(false)
    })

    it('should reject field types', () => {
      expect(isValidRelationString('string')).toBe(false)
      expect(isValidRelationString('string!')).toBe(false)
      expect(isValidRelationString('int[]')).toBe(false)
    })

    it('should reject empty string', () => {
      expect(isValidRelationString('')).toBe(false)
    })
  })
})
