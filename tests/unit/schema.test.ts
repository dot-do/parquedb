import { describe, it, expect } from 'vitest'
import { parseFieldType, parseRelation, isRelationString } from '../../src/types/schema'
import {
  validateSchema,
  validateTypeDefinition,
  validateRelationshipTargets,
  validateEntityCoreFields,
  validateEntityFull,
  parseSchema,
  inferSchema,
  inferSchemaFromCollections,
  inferredToTypeDefinition,
  isValidFieldType,
  isValidRelationString,
  parseNestedField,
} from '../../src/schema/parser'
import type { Schema } from '../../src/types/schema'

describe('Schema Parsing', () => {
  describe('parseFieldType', () => {
    it('parses basic types', () => {
      expect(parseFieldType('string')).toEqual({
        type: 'string',
        required: false,
        isArray: false,
        index: undefined,
        default: undefined,
      })
    })

    it('parses required modifier (!)', () => {
      expect(parseFieldType('string!')).toEqual({
        type: 'string',
        required: true,
        isArray: false,
        index: undefined,
        default: undefined,
      })
    })

    it('parses optional modifier (?)', () => {
      expect(parseFieldType('string?')).toEqual({
        type: 'string',
        required: false,
        isArray: false,
        index: undefined,
        default: undefined,
      })
    })

    it('parses array modifier ([])', () => {
      expect(parseFieldType('string[]')).toEqual({
        type: 'string',
        required: false,
        isArray: true,
        index: undefined,
        default: undefined,
      })
    })

    it('parses required array ([]!)', () => {
      expect(parseFieldType('string[]!')).toEqual({
        type: 'string',
        required: true,
        isArray: true,
        index: undefined,
        default: undefined,
      })
    })

    // Index modifiers (#)
    it('parses index modifier (#)', () => {
      expect(parseFieldType('string#')).toEqual({
        type: 'string',
        required: false,
        isArray: false,
        index: true,
        default: undefined,
      })
    })

    it('parses indexed + required (#!)', () => {
      expect(parseFieldType('string#!')).toEqual({
        type: 'string',
        required: true,
        isArray: false,
        index: true,
        default: undefined,
      })
    })

    it('parses unique index (##)', () => {
      expect(parseFieldType('email##')).toEqual({
        type: 'email',
        required: false,
        isArray: false,
        index: 'unique',
        default: undefined,
      })
    })

    it('parses unique + required (##!)', () => {
      expect(parseFieldType('email##!')).toEqual({
        type: 'email',
        required: true,
        isArray: false,
        index: 'unique',
        default: undefined,
      })
    })

    it('parses FTS index (#fts)', () => {
      expect(parseFieldType('text#fts')).toEqual({
        type: 'text',
        required: false,
        isArray: false,
        index: 'fts',
        default: undefined,
      })
    })

    it('parses vector index (#vec)', () => {
      expect(parseFieldType('vector(1536)#vec')).toEqual({
        type: 'vector(1536)',
        required: false,
        isArray: false,
        index: 'vector',
        default: undefined,
      })
    })

    it('parses hash index (#hash)', () => {
      expect(parseFieldType('string#hash')).toEqual({
        type: 'string',
        required: false,
        isArray: false,
        index: 'hash',
        default: undefined,
      })
    })

    it('parses parametric types with index', () => {
      expect(parseFieldType('decimal(10,2)#')).toEqual({
        type: 'decimal(10,2)',
        required: false,
        isArray: false,
        index: true,
        default: undefined,
      })
    })

    it('parses default values', () => {
      expect(parseFieldType('string = "draft"')).toEqual({
        type: 'string',
        required: false,
        isArray: false,
        index: undefined,
        default: '"draft"',
      })
    })
  })

  describe('parseRelation', () => {
    it('parses forward relation', () => {
      const result = parseRelation('-> User.posts')
      expect(result).toMatchObject({
        toType: 'User',
        reverse: 'posts',
        isArray: false,
        direction: 'forward',
        mode: 'exact',
      })
    })

    it('parses forward relation array', () => {
      const result = parseRelation('-> Category.posts[]')
      expect(result).toMatchObject({
        toType: 'Category',
        reverse: 'posts',
        isArray: true,
        direction: 'forward',
        mode: 'exact',
      })
    })

    it('parses backward relation', () => {
      const result = parseRelation('<- Comment.post')
      expect(result).toMatchObject({
        fromType: 'Comment',
        fromField: 'post',
        isArray: false,
        direction: 'backward',
        mode: 'exact',
      })
    })

    it('parses fuzzy forward relation', () => {
      const result = parseRelation('~> Topic')
      expect(result).toMatchObject({
        toType: 'Topic',
        direction: 'forward',
        mode: 'fuzzy',
      })
    })
  })

  describe('isRelationString', () => {
    it('identifies forward relations', () => {
      expect(isRelationString('-> User.posts')).toBe(true)
    })

    it('identifies backward relations', () => {
      expect(isRelationString('<- Comment.post')).toBe(true)
    })

    it('identifies fuzzy relations', () => {
      expect(isRelationString('~> Topic')).toBe(true)
      expect(isRelationString('<~ Source')).toBe(true)
    })

    it('rejects non-relations', () => {
      expect(isRelationString('string!')).toBe(false)
      expect(isRelationString('User')).toBe(false)
    })
  })
})

describe('Schema Validation', () => {
  describe('validateSchema', () => {
    it('validates a valid schema', () => {
      const schema: Schema = {
        User: {
          name: 'string!',
          email: 'email!',
          posts: '<- Post.author[]',
        },
        Post: {
          title: 'string!',
          content: 'text',
          author: '-> User.posts',
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
      expect(result.errors).toHaveLength(0)
    })

    it('rejects empty schema', () => {
      const result = validateSchema({})
      expect(result.valid).toBe(false)
      expect(result.errors[0].code).toBe('EMPTY_SCHEMA')
    })

    it('rejects invalid type names', () => {
      const schema: Schema = {
        user: { name: 'string!' }, // lowercase
        '123Type': { name: 'string!' }, // starts with number
      }

      const result = validateSchema(schema, { checkRelationships: false })
      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.code === 'INVALID_TYPE_NAME')).toBe(true)
    })

    it('rejects empty types', () => {
      const schema: Schema = {
        User: { $type: 'schema:Person' }, // no fields, only metadata
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(false)
      expect(result.errors[0].code).toBe('EMPTY_TYPE')
    })

    it('rejects invalid field types', () => {
      const schema: Schema = {
        User: {
          name: 'invalidtype!',
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(false)
      expect(result.errors[0].code).toBe('INVALID_FIELD_TYPE')
    })

    it('validates relationship targets', () => {
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
  })

  describe('validateTypeDefinition', () => {
    it('validates a valid type definition', () => {
      const result = validateTypeDefinition('User', {
        name: 'string!',
        age: 'int?',
      })
      expect(result.valid).toBe(true)
    })

    it('rejects unknown metadata fields', () => {
      const result = validateTypeDefinition('User', {
        name: 'string!',
        $unknown: 'value',
      } as any)
      expect(result.valid).toBe(false)
      expect(result.errors[0].code).toBe('RESERVED_FIELD_NAME')
    })
  })

  describe('validateRelationshipTargets', () => {
    it('validates matching relationship pairs', () => {
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

    it('detects missing reverse fields', () => {
      const schema: Schema = {
        User: {
          name: 'string!',
        },
        Post: {
          title: 'string!',
          author: '-> User.posts',
        },
      }

      const result = validateRelationshipTargets(schema)
      expect(result.valid).toBe(false)
      expect(result.errors[0].code).toBe('MISSING_REVERSE_FIELD')
    })
  })
})

describe('isValidFieldType', () => {
  it('validates primitive types', () => {
    expect(isValidFieldType('string')).toBe(true)
    expect(isValidFieldType('number')).toBe(true)
    expect(isValidFieldType('boolean')).toBe(true)
    expect(isValidFieldType('date')).toBe(true)
    expect(isValidFieldType('json')).toBe(true)
  })

  it('validates types with modifiers', () => {
    expect(isValidFieldType('string!')).toBe(true)
    expect(isValidFieldType('string?')).toBe(true)
    expect(isValidFieldType('string[]')).toBe(true)
    expect(isValidFieldType('string[]!')).toBe(true)
  })

  it('validates parametric types', () => {
    expect(isValidFieldType('decimal(10,2)')).toBe(true)
    expect(isValidFieldType('varchar(255)')).toBe(true)
    expect(isValidFieldType('vector(1536)')).toBe(true)
    expect(isValidFieldType('enum(draft,published)')).toBe(true)
  })

  it('rejects invalid types', () => {
    expect(isValidFieldType('invalidtype')).toBe(false)
    expect(isValidFieldType('string!!')).toBe(false)
    expect(isValidFieldType('string!?')).toBe(false)
    expect(isValidFieldType('')).toBe(false)
  })

  it('rejects relation strings', () => {
    expect(isValidFieldType('-> User.posts')).toBe(false)
    expect(isValidFieldType('<- Comment.post')).toBe(false)
  })
})

describe('isValidRelationString', () => {
  it('validates forward relations', () => {
    expect(isValidRelationString('-> User.posts')).toBe(true)
    expect(isValidRelationString('-> User.posts[]')).toBe(true)
  })

  it('validates backward relations', () => {
    expect(isValidRelationString('<- Comment.post')).toBe(true)
    expect(isValidRelationString('<- Comment.post[]')).toBe(true)
  })

  it('validates fuzzy relations', () => {
    expect(isValidRelationString('~> Topic')).toBe(true)
    expect(isValidRelationString('~> Topic.related')).toBe(true)
    expect(isValidRelationString('<~ Source')).toBe(true)
  })

  it('rejects invalid relations', () => {
    expect(isValidRelationString('-> User')).toBe(false) // Missing field
    expect(isValidRelationString('string!')).toBe(false)
  })
})

describe('parseSchema', () => {
  it('parses a complete schema', () => {
    const schema: Schema = {
      User: {
        $type: 'schema:Person',
        name: 'string!',
        email: { type: 'email!', index: 'unique' },
        posts: '<- Post.author[]',
      },
      Post: {
        title: 'string!',
        content: 'markdown',
        author: '-> User.posts',
      },
    }

    const parsed = parseSchema(schema)

    expect(parsed.types.size).toBe(2)
    expect(parsed.getType('User')).toBeDefined()
    expect(parsed.getType('Post')).toBeDefined()

    const user = parsed.getType('User')!
    expect(user.typeUri).toBe('schema:Person')
    expect(user.fields.get('name')?.required).toBe(true)
    expect(user.fields.get('email')?.index).toBe('unique')

    const post = parsed.getType('Post')!
    expect(post.fields.get('author')?.isRelation).toBe(true)
    expect(post.fields.get('author')?.targetType).toBe('User')
  })

  it('extracts relationships', () => {
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

    expect(relationships.length).toBe(2)
    expect(relationships.some(r =>
      r.fromType === 'Post' && r.toType === 'User' && r.direction === 'forward'
    )).toBe(true)
  })

  it('validates entities against schema', () => {
    const schema: Schema = {
      User: {
        name: 'string!',
        age: 'int?',
      },
    }

    const parsed = parseSchema(schema)

    // Valid entity
    const validResult = parsed.validate('User', { name: 'Alice', age: 30 })
    expect(validResult.valid).toBe(true)

    // Missing required field
    const invalidResult = parsed.validate('User', { age: 30 })
    expect(invalidResult.valid).toBe(false)
    expect(invalidResult.errors[0].code).toBe('REQUIRED')
  })
})

describe('Schema Inference', () => {
  describe('inferSchema', () => {
    it('infers types from documents', () => {
      const docs = [
        { name: 'Alice', age: 30, email: 'alice@example.com' },
        { name: 'Bob', age: 25, email: 'bob@example.com' },
      ]

      const schema = inferSchema(docs)

      expect(schema.name.type).toBe('string')
      expect(schema.name.required).toBe(true)
      expect(schema.age.type).toBe('int')
      expect(schema.email.type).toBe('email')
    })

    it('detects optional fields', () => {
      const docs = [
        { name: 'Alice', email: 'alice@example.com' },
        { name: 'Bob' }, // no email
      ]

      const schema = inferSchema(docs)

      expect(schema.name.required).toBe(true)
      expect(schema.email.required).toBe(false)
    })

    it('detects arrays', () => {
      const docs = [
        { tags: ['a', 'b', 'c'] },
        { tags: ['x', 'y'] },
      ]

      const schema = inferSchema(docs)

      expect(schema.tags.isArray).toBe(true)
      expect(schema.tags.type).toBe('string')
    })

    it('detects datetime strings', () => {
      const docs = [
        { createdAt: '2024-01-15T10:30:00Z' },
      ]

      const schema = inferSchema(docs)

      expect(schema.createdAt.type).toBe('datetime')
    })

    it('detects UUID strings', () => {
      const docs = [
        { id: '550e8400-e29b-41d4-a716-446655440000' },
      ]

      const schema = inferSchema(docs)

      expect(schema.id.type).toBe('uuid')
    })

    it('detects URL strings', () => {
      const docs = [
        { website: 'https://example.com' },
      ]

      const schema = inferSchema(docs)

      expect(schema.website.type).toBe('url')
    })

    it('infers nested object schemas', () => {
      const docs = [
        { address: { city: 'NYC', zip: '10001' } },
        { address: { city: 'LA', zip: '90001' } },
      ]

      const schema = inferSchema(docs, { inferNested: true })

      expect(schema.address.type).toBe('object')
      expect(schema.address.nested).toBeDefined()
      expect(schema.address.nested!.city.type).toBe('string')
    })

    it('handles empty documents array', () => {
      const schema = inferSchema([])
      expect(Object.keys(schema)).toHaveLength(0)
    })

    it('respects sampleSize option', () => {
      const docs = Array.from({ length: 200 }, (_, i) => ({
        id: i,
        name: `Item ${i}`,
      }))

      // Only sample first 10
      const schema = inferSchema(docs, { sampleSize: 10, detectRequired: true })

      expect(schema.id).toBeDefined()
      expect(schema.name).toBeDefined()
    })
  })

  describe('inferredToTypeDefinition', () => {
    it('converts inferred schema to type definition', () => {
      const inferred = {
        name: { type: 'string', required: true, isArray: false },
        tags: { type: 'string', required: false, isArray: true },
        count: { type: 'int', required: true, isArray: false },
      }

      const typeDef = inferredToTypeDefinition('Item', inferred)

      expect(typeDef.name).toBe('string!')
      expect(typeDef.tags).toBe('string[]')
      expect(typeDef.count).toBe('int!')
    })
  })

  describe('inferSchemaFromCollections', () => {
    it('infers schema from multiple collections', () => {
      const collections = {
        users: [
          { name: 'Alice', email: 'alice@example.com' },
        ],
        posts: [
          { title: 'Hello', views: 100 },
        ],
      }

      const schema = inferSchemaFromCollections(collections)

      expect(schema.Users).toBeDefined()
      expect(schema.Posts).toBeDefined()
      expect(schema.Users.name).toBe('string!')
      expect(schema.Posts.views).toBe('int!')
    })
  })
})

describe('Entity Core Fields Validation', () => {
  describe('validateEntityCoreFields', () => {
    it('validates entity with all core fields', () => {
      const entity = {
        $id: 'users/alice',
        $type: 'User',
        name: 'Alice',
      }

      const result = validateEntityCoreFields(entity)
      expect(result.valid).toBe(true)
    })

    it('rejects entity missing $id', () => {
      const entity = {
        $type: 'User',
        name: 'Alice',
      }

      const result = validateEntityCoreFields(entity)
      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.path === '$id')).toBe(true)
    })

    it('rejects entity missing $type', () => {
      const entity = {
        $id: 'users/alice',
        name: 'Alice',
      }

      const result = validateEntityCoreFields(entity)
      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.path === '$type')).toBe(true)
    })

    it('rejects entity missing name', () => {
      const entity = {
        $id: 'users/alice',
        $type: 'User',
      }

      const result = validateEntityCoreFields(entity)
      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.path === 'name')).toBe(true)
    })

    it('rejects non-string core fields', () => {
      const entity = {
        $id: 123,
        $type: true,
        name: ['Alice'],
      }

      const result = validateEntityCoreFields(entity)
      expect(result.valid).toBe(false)
      expect(result.errors.length).toBe(3)
    })

    it('rejects non-object entities', () => {
      const result = validateEntityCoreFields('not an object')
      expect(result.valid).toBe(false)
      expect(result.errors[0].code).toBe('INVALID_TYPE')
    })
  })

  describe('validateEntityFull', () => {
    it('validates entity against schema with core fields', () => {
      const schema: Schema = {
        User: {
          email: 'email!',
          age: 'int?',
        },
      }

      const parsed = parseSchema(schema)

      const entity = {
        $id: 'users/alice',
        $type: 'User',
        name: 'Alice',
        email: 'alice@example.com',
        age: 30,
      }

      const result = validateEntityFull(parsed, 'User', entity)
      expect(result.valid).toBe(true)
    })

    it('fails when core fields are missing', () => {
      const schema: Schema = {
        User: {
          email: 'email!',
        },
      }

      const parsed = parseSchema(schema)

      const entity = {
        email: 'alice@example.com',
      }

      const result = validateEntityFull(parsed, 'User', entity)
      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.path === '$id')).toBe(true)
    })

    it('can skip core field validation', () => {
      const schema: Schema = {
        User: {
          email: 'email!',
        },
      }

      const parsed = parseSchema(schema)

      const entity = {
        email: 'alice@example.com',
      }

      const result = validateEntityFull(parsed, 'User', entity, { validateCoreFields: false })
      expect(result.valid).toBe(true)
    })
  })
})

describe('Nested Schema Support', () => {
  describe('parseNestedField', () => {
    it('parses simple field strings', () => {
      const result = parseNestedField('name', 'string!')
      expect(result.type).toBe('string')
      expect(result.required).toBe(true)
    })

    it('parses nested object definitions', () => {
      const result = parseNestedField('address', {
        type: 'json',
        properties: {
          city: 'string!',
          zip: 'string',
        },
      })

      expect(result.type).toBe('json')
      expect(result.properties).toBeDefined()
      expect(result.properties!.get('city')?.required).toBe(true)
      expect(result.properties!.get('zip')?.required).toBe(false)
    })

    it('parses relation fields', () => {
      const result = parseNestedField('author', '-> User.posts')
      expect(result.isRelation).toBe(true)
      expect(result.targetType).toBe('User')
    })
  })
})

// =============================================================================
// Schema Evolution Tests
// =============================================================================
// These tests cover complex schema evolution scenarios including:
// - Adding new required fields with defaults
// - Removing fields (soft delete)
// - Renaming fields
// - Type changes (string to number)
// - Nested schema changes

describe('Schema Evolution', () => {
  describe('Adding new required fields with defaults', () => {
    it('validates new schema with required field and default', () => {
      const schemaV2: Schema = {
        User: {
          name: 'string!',
          email: 'email!',
          status: 'string! = "active"', // New required field with default
        },
      }

      const result = validateSchema(schemaV2, { checkRelationships: false })
      expect(result.valid).toBe(true)
    })

    it('parses required field with default value', () => {
      const parsed = parseFieldType('string! = "active"')
      expect(parsed.type).toBe('string')
      expect(parsed.required).toBe(true)
      expect(parsed.default).toBe('"active"')
    })

    it('validates entity against schema with required field default', () => {
      const schema: Schema = {
        User: {
          name: 'string!',
          role: 'string! = "user"', // Required with default
        },
      }

      const parsed = parseSchema(schema)

      // Entity without role should fail (required field)
      const invalidResult = parsed.validate('User', { name: 'Alice' })
      expect(invalidResult.valid).toBe(false)
      expect(invalidResult.errors[0]?.code).toBe('REQUIRED')

      // Entity with role should pass
      const validResult = parsed.validate('User', { name: 'Alice', role: 'admin' })
      expect(validResult.valid).toBe(true)
    })

    it('validates adding multiple required fields with defaults', () => {
      const schema: Schema = {
        Post: {
          title: 'string!',
          status: 'string! = "draft"',
          visibility: 'string! = "public"',
          priority: 'int! = "0"',
        },
      }

      const result = validateSchema(schema, { checkRelationships: false })
      expect(result.valid).toBe(true)

      const parsed = parseSchema(schema)
      const statusField = parsed.getType('Post')?.fields.get('status')
      expect(statusField?.default).toBe('draft')
    })

    it('validates adding required indexed field with default', () => {
      const schema: Schema = {
        User: {
          email: 'email!',
          status: { type: 'string!', index: true, default: 'pending' },
        },
      }

      const result = validateSchema(schema, { checkRelationships: false })
      expect(result.valid).toBe(true)

      const parsed = parseSchema(schema)
      const statusField = parsed.getType('User')?.fields.get('status')
      expect(statusField?.default).toBe('pending')
      expect(statusField?.index).toBe(true)
    })
  })

  describe('Removing fields (soft delete semantics)', () => {
    it('validates schema after field removal', () => {
      // Original schema had 'age' field, evolved schema removes it
      const evolvedSchema: Schema = {
        User: {
          name: 'string!',
          email: 'email!',
          // age field removed
        },
      }

      const result = validateSchema(evolvedSchema, { checkRelationships: false })
      expect(result.valid).toBe(true)
    })

    it('validates schema removing optional field', () => {
      const originalSchema: Schema = {
        User: {
          name: 'string!',
          email: 'email!',
          bio: 'text?', // Optional field
        },
      }

      const evolvedSchema: Schema = {
        User: {
          name: 'string!',
          email: 'email!',
          // bio removed - this is safe for existing data
        },
      }

      expect(validateSchema(originalSchema, { checkRelationships: false }).valid).toBe(true)
      expect(validateSchema(evolvedSchema, { checkRelationships: false }).valid).toBe(true)
    })

    it('validates schema after removing indexed field', () => {
      const evolvedSchema: Schema = {
        User: {
          name: 'string!',
          // email field with index was removed
        },
      }

      const result = validateSchema(evolvedSchema, { checkRelationships: false })
      expect(result.valid).toBe(true)
    })

    it('validates entities with extra fields not in schema', () => {
      const schema: Schema = {
        User: {
          name: 'string!',
        },
      }

      const parsed = parseSchema(schema)

      // Entity with extra field (from old schema) should still validate
      // ParqueDB uses flexible schema - extra fields are allowed
      const result = parsed.validate('User', { name: 'Alice', legacyField: 'value' })
      expect(result.valid).toBe(true)
    })
  })

  describe('Renaming fields', () => {
    it('validates schema with renamed field', () => {
      // Original: { fullName: 'string!' }
      // Evolved: { displayName: 'string!' }
      const evolvedSchema: Schema = {
        User: {
          displayName: 'string!', // Renamed from fullName
          email: 'email!',
        },
      }

      const result = validateSchema(evolvedSchema, { checkRelationships: false })
      expect(result.valid).toBe(true)
    })

    it('validates entity against schema after field rename', () => {
      const schema: Schema = {
        User: {
          displayName: 'string!', // Was 'fullName'
          email: 'email!',
        },
      }

      const parsed = parseSchema(schema)

      // Entity with new field name should pass
      const validResult = parsed.validate('User', {
        displayName: 'Alice',
        email: 'alice@example.com',
      })
      expect(validResult.valid).toBe(true)

      // Entity with old field name should fail (missing required displayName)
      const invalidResult = parsed.validate('User', {
        fullName: 'Alice', // Old field name
        email: 'alice@example.com',
      })
      expect(invalidResult.valid).toBe(false)
    })

    it('validates renaming relationship field', () => {
      const evolvedSchema: Schema = {
        User: {
          name: 'string!',
          authoredPosts: '<- Post.creator[]', // Renamed from 'posts'
        },
        Post: {
          title: 'string!',
          creator: '-> User.authoredPosts', // Renamed from 'author'
        },
      }

      const result = validateSchema(evolvedSchema)
      expect(result.valid).toBe(true)
    })
  })

  describe('Type changes (string to number, etc.)', () => {
    it('validates schema with changed field type', () => {
      // Original: { age: 'string' }
      // Evolved: { age: 'int' }
      const evolvedSchema: Schema = {
        User: {
          name: 'string!',
          age: 'int?', // Changed from string to int
        },
      }

      const result = validateSchema(evolvedSchema, { checkRelationships: false })
      expect(result.valid).toBe(true)
    })

    it('validates entity with new type after schema change', () => {
      const schema: Schema = {
        User: {
          name: 'string!',
          score: 'number!', // Changed from string to number
        },
      }

      const parsed = parseSchema(schema)

      // Entity with numeric score should pass
      const validResult = parsed.validate('User', {
        name: 'Alice',
        score: 100,
      })
      expect(validResult.valid).toBe(true)

      // Entity with string score should fail
      const invalidResult = parsed.validate('User', {
        name: 'Alice',
        score: '100', // String instead of number
      })
      expect(invalidResult.valid).toBe(false)
      expect(invalidResult.errors[0]?.code).toBe('TYPE_MISMATCH')
    })

    it('validates boolean to string type change', () => {
      const schema: Schema = {
        Feature: {
          name: 'string!',
          enabled: 'string?', // Changed from boolean to string (e.g., "yes"/"no"/"partial")
        },
      }

      const parsed = parseSchema(schema)

      const result = parsed.validate('Feature', {
        name: 'Dark Mode',
        enabled: 'partial',
      })
      expect(result.valid).toBe(true)
    })

    it('validates array to non-array type change', () => {
      // Changing from array to single value
      const schema: Schema = {
        User: {
          name: 'string!',
          primaryTag: 'string?', // Changed from 'tags: string[]'
        },
      }

      const parsed = parseSchema(schema)

      const result = parsed.validate('User', {
        name: 'Alice',
        primaryTag: 'developer',
      })
      expect(result.valid).toBe(true)
    })

    it('validates type widening (int to number)', () => {
      const schema: Schema = {
        Metric: {
          name: 'string!',
          value: 'number!', // Widened from 'int'
        },
      }

      const parsed = parseSchema(schema)

      // Both integers and floats should work
      expect(parsed.validate('Metric', { name: 'Count', value: 42 }).valid).toBe(true)
      expect(parsed.validate('Metric', { name: 'Rate', value: 3.14 }).valid).toBe(true)
    })

    it('validates enum type change (adding values)', () => {
      const schema: Schema = {
        Task: {
          title: 'string!',
          status: 'enum(todo,in_progress,review,done)!', // Added 'review' status
        },
      }

      const parsed = parseSchema(schema)

      const result = parsed.validate('Task', {
        title: 'Fix bug',
        status: 'review',
      })
      expect(result.valid).toBe(true)
    })
  })

  describe('Nested schema changes', () => {
    it('validates schema with nested object structure', () => {
      const schema: Schema = {
        User: {
          name: 'string!',
          address: {
            type: 'json',
            properties: {
              street: 'string!',
              city: 'string!',
              zip: 'string!',
              country: 'string! = "US"',
            },
          },
        },
      }

      const result = validateSchema(schema, { checkRelationships: false })
      expect(result.valid).toBe(true)
    })

    it('parses nested field with property additions', () => {
      const result = parseNestedField('settings', {
        type: 'json',
        properties: {
          theme: 'string = "light"',
          notifications: 'boolean = "true"',
          language: 'string = "en"', // New nested property
        },
      })

      expect(result.type).toBe('json')
      expect(result.properties).toBeDefined()
      expect(result.properties!.size).toBe(3)
      expect(result.properties!.get('theme')?.default).toBe('light')
      expect(result.properties!.get('language')?.default).toBe('en')
    })

    it('validates nested schema with type changes', () => {
      const schema: Schema = {
        Product: {
          name: 'string!',
          metadata: {
            type: 'json',
            properties: {
              weight: 'number!', // Changed from 'string' to 'number'
              dimensions: {
                type: 'json',
                properties: {
                  width: 'number!',
                  height: 'number!',
                  depth: 'number?', // New optional nested field
                },
              },
            },
          },
        },
      }

      const result = validateSchema(schema, { checkRelationships: false })
      expect(result.valid).toBe(true)
    })

    it('validates deeply nested schema evolution', () => {
      const schema: Schema = {
        Organization: {
          name: 'string!',
          config: {
            type: 'json',
            properties: {
              billing: {
                type: 'json',
                properties: {
                  plan: 'string!',
                  seats: 'int!',
                  features: {
                    type: 'json',
                    properties: {
                      analytics: 'boolean = "false"',
                      api: 'boolean = "true"',
                      sso: 'boolean = "false"', // New deeply nested property
                    },
                  },
                },
              },
            },
          },
        },
      }

      const result = validateSchema(schema, { checkRelationships: false })
      expect(result.valid).toBe(true)
    })

    it('validates removing nested properties', () => {
      // Original nested schema had more properties
      const evolvedSchema: Schema = {
        User: {
          name: 'string!',
          preferences: {
            type: 'json',
            properties: {
              theme: 'string?',
              // 'font' and 'size' properties removed
            },
          },
        },
      }

      const result = validateSchema(evolvedSchema, { checkRelationships: false })
      expect(result.valid).toBe(true)
    })

    it('validates converting flat fields to nested structure', () => {
      // Evolution: flat fields -> nested object
      // Before: { street: 'string', city: 'string', zip: 'string' }
      // After: { address: { street: 'string', city: 'string', zip: 'string' } }
      const evolvedSchema: Schema = {
        User: {
          name: 'string!',
          address: {
            type: 'json',
            properties: {
              street: 'string?',
              city: 'string?',
              zip: 'string?',
            },
          },
        },
      }

      const result = validateSchema(evolvedSchema, { checkRelationships: false })
      expect(result.valid).toBe(true)
    })

    it('validates nested array schema changes', () => {
      const schema: Schema = {
        Order: {
          id: 'string!',
          items: {
            type: 'json',
            properties: {
              products: 'json[]', // Array of nested objects
              total: 'number!',
              discount: 'number = "0"', // New nested field with default
            },
          },
        },
      }

      const result = validateSchema(schema, { checkRelationships: false })
      expect(result.valid).toBe(true)
    })
  })

  describe('Schema evolution edge cases', () => {
    it('handles multiple simultaneous changes', () => {
      // Schema with multiple changes at once:
      // - Added required field with default
      // - Removed field
      // - Changed field type
      // - Added nested structure
      const evolvedSchema: Schema = {
        User: {
          name: 'string!',
          email: 'email!',
          // removed: age (was 'int?')
          score: 'number!', // changed from 'string'
          status: 'string! = "active"', // new required with default
          settings: {
            type: 'json',
            properties: {
              theme: 'string = "light"',
              notifications: 'boolean = "true"',
            },
          },
        },
      }

      const result = validateSchema(evolvedSchema, { checkRelationships: false })
      expect(result.valid).toBe(true)
    })

    it('validates schema with optional to required change plus default', () => {
      // Changing optional field to required with default
      const schema: Schema = {
        User: {
          name: 'string!',
          verified: 'boolean! = "false"', // Was 'boolean?', now required with default
        },
      }

      const result = validateSchema(schema, { checkRelationships: false })
      expect(result.valid).toBe(true)

      const parsed = parseSchema(schema)
      const verifiedField = parsed.getType('User')?.fields.get('verified')
      expect(verifiedField?.required).toBe(true)
      // Default is stored as string '"false"' in the type definition and parsed to 'false' string
      expect(verifiedField?.default).toBe('false')
    })

    it('validates relationship changes during evolution', () => {
      // Evolution that changes relationship cardinality
      const schema: Schema = {
        User: {
          name: 'string!',
          mainProject: '-> Project.owner', // Changed from 'projects: -> Project.members[]'
        },
        Project: {
          title: 'string!',
          owner: '<- User.mainProject', // Changed from 'members: <- User.projects[]'
        },
      }

      const result = validateSchema(schema)
      expect(result.valid).toBe(true)
    })

    it('validates index changes during schema evolution', () => {
      // Adding/removing/changing indexes
      const schema: Schema = {
        User: {
          name: 'string!',
          email: { type: 'email!', index: 'unique' }, // New unique index
          username: { type: 'string!', index: true }, // Changed from no index
          // legacyCode index was removed
        },
      }

      const result = validateSchema(schema, { checkRelationships: false })
      expect(result.valid).toBe(true)
    })

    it('validates parametric type changes', () => {
      const schema: Schema = {
        Transaction: {
          id: 'string!',
          amount: 'decimal(18,4)!', // Changed from 'decimal(10,2)'
          currency: 'varchar(10)!', // Changed from 'varchar(3)'
        },
      }

      const result = validateSchema(schema, { checkRelationships: false })
      expect(result.valid).toBe(true)

      expect(isValidFieldType('decimal(18,4)')).toBe(true)
      expect(isValidFieldType('varchar(10)')).toBe(true)
    })
  })
})
