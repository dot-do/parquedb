/**
 * Tests for IceType integration
 *
 * IceType is a schema definition package that supports:
 * - Field types with required/optional
 * - Default values
 * - Relationships with operators
 * - FTS indexes ($fts)
 * - Vector indexes ($vector)
 * - Compound indexes ($index)
 * - Partitioning hints ($partitionBy)
 *
 * Updated for @icetype/core@0.2.0
 *
 * @see https://github.com/primitives-org/icetype
 */

import { describe, it, expect } from 'vitest'
import type {
  IceTypeSchema,
  FieldDefinition as IceTypeFieldDefinition,
  RelationDefinition,
  SchemaDirectives,
  FieldModifier,
  RelationOperator,
  VectorDirective,
  IndexDirective,
} from '@icetype/core'
import {
  fromIceType,
  type IceTypeParsedSchema,
} from '../../src/types/integrations'
import type { Schema, TypeDefinition, FieldDefinition, IndexDefinition } from '../../src/types/schema'

// =============================================================================
// Test Helpers - Mock IceType Schema Builders for new @icetype/core@0.2.0 API
// =============================================================================

/**
 * Create a mock IceTypeFieldDefinition (new @icetype/core structure)
 */
function createField(
  name: string,
  type: string,
  options: {
    required?: boolean
    default?: unknown
    relation?: {
      operator: RelationOperator
      targetType: string
      inverse?: string
      threshold?: number  // For fuzzy relationships
    }
    isArray?: boolean
    description?: string
    validation?: {
      min?: number
      max?: number
      minLength?: number
      maxLength?: number
      pattern?: string
    }
  } = {}
): IceTypeFieldDefinition & { description?: string; validation?: unknown } {
  const modifier: FieldModifier = options.required ? '!' : ''

  return {
    name,
    type,
    modifier,
    isArray: options.isArray ?? false,
    isOptional: !options.required,
    isUnique: false,
    isIndexed: false,
    defaultValue: options.default,
    relation: options.relation ? {
      operator: options.relation.operator,
      targetType: options.relation.targetType,
      inverse: options.relation.inverse,
      // Pass through threshold for fuzzy relationships
      ...(options.relation.threshold !== undefined ? { threshold: options.relation.threshold } : {}),
    } as RelationDefinition & { threshold?: number } : undefined,
    // Extended properties for tests
    description: options.description,
    validation: options.validation,
  }
}

/**
 * Create a mock IceTypeSchema (new @icetype/core structure)
 */
function createIceTypeSchema(
  fields: (IceTypeFieldDefinition & { description?: string; validation?: unknown })[],
  options: {
    $type?: string
    $partitionBy?: string[]
    $index?: (string[] | { fields: string[]; unique?: boolean; sparse?: boolean; name?: string })[]
    $fts?: (string | { field: string; language?: string; weight?: number })[]
    $vector?: VectorDirective[]
    $unique?: string[][]
    $description?: string
  } = {}
): IceTypeSchema & {
  $type?: string
  $partitionBy?: string[]
  $index?: (string[] | { fields: string[]; unique?: boolean; sparse?: boolean; name?: string })[]
  $fts?: (string | { field: string; language?: string; weight?: number })[]
  $unique?: string[][]
  $description?: string
} {
  const fieldMap = new Map<string, IceTypeFieldDefinition>()
  for (const field of fields) {
    fieldMap.set(field.name, field)
  }

  // Build directives in the new format
  const directives: SchemaDirectives = {}
  if (options.$partitionBy && options.$partitionBy.length > 0) {
    directives.partitionBy = options.$partitionBy
  }
  if (options.$index && options.$index.length > 0) {
    directives.index = options.$index.map((idx) => {
      if (Array.isArray(idx)) {
        return { fields: idx }
      }
      return idx as IndexDirective
    })
  }
  if (options.$fts && options.$fts.length > 0) {
    directives.fts = options.$fts.map(f => typeof f === 'string' ? f : f.field)
  }
  if (options.$vector && options.$vector.length > 0) {
    directives.vector = options.$vector
  }

  // Build relations map
  const relations = new Map<string, RelationDefinition>()
  for (const field of fields) {
    if (field.relation) {
      relations.set(field.name, field.relation)
    }
  }

  return {
    name: '', // Will be set when added to parsed schema
    fields: fieldMap,
    directives,
    relations,
    version: 1,
    createdAt: Date.now(),
    updatedAt: Date.now(),
    // Pass through for legacy compatibility and extended tests
    $type: options.$type,
    $partitionBy: options.$partitionBy,
    $index: options.$index,
    $fts: options.$fts,
    $unique: options.$unique,
    $description: options.$description,
  }
}

/**
 * Create a mock IceTypeParsedSchema (legacy wrapper for compatibility)
 */
function createIceTypeParsedSchema(
  schemas: Record<string, ReturnType<typeof createIceTypeSchema>>
): IceTypeParsedSchema {
  const schemaMap = new Map<string, IceTypeSchema>()
  for (const [name, schema] of Object.entries(schemas)) {
    // Set the name on each schema
    (schema as any).name = name
    schemaMap.set(name, schema as IceTypeSchema)
  }

  return {
    schemas: schemaMap,
    getSchema(name: string) {
      return schemaMap.get(name)
    },
  }
}

// =============================================================================
// Test Suite
// =============================================================================

describe('fromIceType', () => {
  // ---------------------------------------------------------------------------
  // 1. Convert basic entity with scalar fields
  // ---------------------------------------------------------------------------
  describe('basic entity conversion', () => {
    it('should convert a simple entity with string fields', () => {
      const iceSchema = createIceTypeParsedSchema({
        User: createIceTypeSchema([
          createField('id', 'uuid'),
          createField('name', 'string'),
          createField('email', 'email'),
        ]),
      })

      const result = fromIceType(iceSchema)

      expect(result).toHaveProperty('User')
      expect(result.User).toHaveProperty('id')
      expect(result.User).toHaveProperty('name')
      expect(result.User).toHaveProperty('email')
    })

    it('should convert numeric field types correctly', () => {
      const iceSchema = createIceTypeParsedSchema({
        Product: createIceTypeSchema([
          createField('price', 'decimal'),
          createField('quantity', 'int'),
          createField('rating', 'float'),
        ]),
      })

      const result = fromIceType(iceSchema)

      expect(result.Product.price).toBe('decimal')
      expect(result.Product.quantity).toBe('int')
      expect(result.Product.rating).toBe('float')
    })

    it('should convert date/time field types correctly', () => {
      const iceSchema = createIceTypeParsedSchema({
        Event: createIceTypeSchema([
          createField('date', 'date'),
          createField('startTime', 'datetime'),
          createField('createdAt', 'timestamp'),
        ]),
      })

      const result = fromIceType(iceSchema)

      expect(result.Event.date).toBe('date')
      expect(result.Event.startTime).toBe('datetime')
      expect(result.Event.createdAt).toBe('timestamp')
    })

    it('should convert boolean fields correctly', () => {
      const iceSchema = createIceTypeParsedSchema({
        Settings: createIceTypeSchema([
          createField('isActive', 'boolean'),
          createField('notifications', 'boolean'),
        ]),
      })

      const result = fromIceType(iceSchema)

      expect(result.Settings.isActive).toBe('boolean')
      expect(result.Settings.notifications).toBe('boolean')
    })

    it('should convert multiple entities', () => {
      const iceSchema = createIceTypeParsedSchema({
        User: createIceTypeSchema([createField('name', 'string')]),
        Post: createIceTypeSchema([createField('title', 'string')]),
        Comment: createIceTypeSchema([createField('text', 'string')]),
      })

      const result = fromIceType(iceSchema)

      expect(Object.keys(result)).toHaveLength(3)
      expect(result).toHaveProperty('User')
      expect(result).toHaveProperty('Post')
      expect(result).toHaveProperty('Comment')
    })

    it('should handle text and markdown types', () => {
      const iceSchema = createIceTypeParsedSchema({
        Article: createIceTypeSchema([
          createField('summary', 'text'),
          createField('content', 'markdown'),
        ]),
      })

      const result = fromIceType(iceSchema)

      expect(result.Article.summary).toBe('text')
      expect(result.Article.content).toBe('markdown')
    })

    it('should handle json and binary types', () => {
      const iceSchema = createIceTypeParsedSchema({
        Document: createIceTypeSchema([
          createField('metadata', 'json'),
          createField('content', 'binary'),
        ]),
      })

      const result = fromIceType(iceSchema)

      expect(result.Document.metadata).toBe('json')
      expect(result.Document.content).toBe('binary')
    })

    // RED PHASE: Test for array field types - NOT YET IMPLEMENTED
    it('should convert array field types with [] suffix', () => {
      const iceSchema = createIceTypeParsedSchema({
        User: createIceTypeSchema([
          createField('tags', 'string', { isArray: true }),
          createField('scores', 'int', { isArray: true }),
        ]),
      })

      const result = fromIceType(iceSchema)

      // Should produce string[] and int[] in the output
      expect(result.User.tags).toBe('string[]')
      expect(result.User.scores).toBe('int[]')
    })

    // RED PHASE: Test for required array types
    it('should convert required array field types', () => {
      const iceSchema = createIceTypeParsedSchema({
        User: createIceTypeSchema([
          createField('roles', 'string', { required: true, isArray: true }),
        ]),
      })

      const result = fromIceType(iceSchema)

      expect(result.User.roles).toBe('string[]!')
    })

    // RED PHASE: Test for parametric types
    it('should preserve parametric types like varchar(255)', () => {
      const iceSchema = createIceTypeParsedSchema({
        User: createIceTypeSchema([
          createField('name', 'varchar(100)'),
          createField('balance', 'decimal(10,2)'),
        ]),
      })

      const result = fromIceType(iceSchema)

      expect(result.User.name).toBe('varchar(100)')
      expect(result.User.balance).toBe('decimal(10,2)')
    })

    // RED PHASE: Test for enum types
    it('should convert enum types correctly', () => {
      const iceSchema = createIceTypeParsedSchema({
        Post: createIceTypeSchema([
          createField('status', 'enum(draft,published,archived)'),
        ]),
      })

      const result = fromIceType(iceSchema)

      expect(result.Post.status).toBe('enum(draft,published,archived)')
    })
  })

  // ---------------------------------------------------------------------------
  // 2. Handle required vs optional fields
  // ---------------------------------------------------------------------------
  describe('required vs optional fields', () => {
    it('should mark required fields with ! modifier', () => {
      const iceSchema = createIceTypeParsedSchema({
        User: createIceTypeSchema([
          createField('id', 'uuid', { required: true }),
          createField('email', 'email', { required: true }),
        ]),
      })

      const result = fromIceType(iceSchema)

      expect(result.User.id).toBe('uuid!')
      expect(result.User.email).toBe('email!')
    })

    it('should leave optional fields without modifier', () => {
      const iceSchema = createIceTypeParsedSchema({
        User: createIceTypeSchema([
          createField('bio', 'string', { required: false }),
          createField('avatar', 'url', { required: false }),
        ]),
      })

      const result = fromIceType(iceSchema)

      expect(result.User.bio).toBe('string')
      expect(result.User.avatar).toBe('url')
    })

    it('should handle mixed required and optional fields', () => {
      const iceSchema = createIceTypeParsedSchema({
        User: createIceTypeSchema([
          createField('id', 'uuid', { required: true }),
          createField('name', 'string', { required: true }),
          createField('bio', 'text', { required: false }),
          createField('website', 'url', { required: false }),
        ]),
      })

      const result = fromIceType(iceSchema)

      expect(result.User.id).toBe('uuid!')
      expect(result.User.name).toBe('string!')
      expect(result.User.bio).toBe('text')
      expect(result.User.website).toBe('url')
    })

    it('should default to optional when required is not specified', () => {
      const iceSchema = createIceTypeParsedSchema({
        User: createIceTypeSchema([createField('nickname', 'string')]),
      })

      const result = fromIceType(iceSchema)

      // Should not have ! modifier
      expect(result.User.nickname).toBe('string')
      expect(result.User.nickname).not.toContain('!')
    })
  })

  // ---------------------------------------------------------------------------
  // 3. Preserve default values
  // ---------------------------------------------------------------------------
  describe('default values', () => {
    it('should preserve string default values', () => {
      const iceSchema = createIceTypeParsedSchema({
        Post: createIceTypeSchema([
          createField('status', 'string', { default: 'draft' }),
        ]),
      })

      const result = fromIceType(iceSchema)

      expect(result.Post.status).toBe('string = "draft"')
    })

    it('should preserve numeric default values', () => {
      const iceSchema = createIceTypeParsedSchema({
        Product: createIceTypeSchema([
          createField('quantity', 'int', { default: 0 }),
          createField('price', 'float', { default: 9.99 }),
        ]),
      })

      const result = fromIceType(iceSchema)

      expect(result.Product.quantity).toBe('int = 0')
      expect(result.Product.price).toBe('float = 9.99')
    })

    it('should preserve boolean default values', () => {
      const iceSchema = createIceTypeParsedSchema({
        Settings: createIceTypeSchema([
          createField('isActive', 'boolean', { default: true }),
          createField('notifications', 'boolean', { default: false }),
        ]),
      })

      const result = fromIceType(iceSchema)

      expect(result.Settings.isActive).toBe('boolean = true')
      expect(result.Settings.notifications).toBe('boolean = false')
    })

    it('should preserve null default value', () => {
      const iceSchema = createIceTypeParsedSchema({
        User: createIceTypeSchema([
          createField('deletedAt', 'datetime', { default: null }),
        ]),
      })

      const result = fromIceType(iceSchema)

      expect(result.User.deletedAt).toBe('datetime = null')
    })

    it('should preserve array default values', () => {
      const iceSchema = createIceTypeParsedSchema({
        User: createIceTypeSchema([
          createField('tags', 'string', { default: [] }),
          createField('roles', 'string', { default: ['user'] }),
        ]),
      })

      const result = fromIceType(iceSchema)

      expect(result.User.tags).toBe('string = []')
      expect(result.User.roles).toBe('string = ["user"]')
    })

    it('should preserve object default values', () => {
      const iceSchema = createIceTypeParsedSchema({
        User: createIceTypeSchema([
          createField('preferences', 'json', {
            default: { theme: 'dark', notifications: true },
          }),
        ]),
      })

      const result = fromIceType(iceSchema)

      expect(result.User.preferences).toBe(
        'json = {"theme":"dark","notifications":true}'
      )
    })

    it('should handle required fields with default values', () => {
      const iceSchema = createIceTypeParsedSchema({
        Post: createIceTypeSchema([
          createField('status', 'string', { required: true, default: 'draft' }),
        ]),
      })

      const result = fromIceType(iceSchema)

      expect(result.Post.status).toBe('string! = "draft"')
    })
  })

  // ---------------------------------------------------------------------------
  // 4. Convert relationship fields
  // ---------------------------------------------------------------------------
  describe('relationship fields', () => {
    it('should convert forward relationship with ->', () => {
      const iceSchema = createIceTypeParsedSchema({
        Post: createIceTypeSchema([
          createField('author', 'User', {
            relation: {
              operator: '->',
              targetType: 'User',
              inverse: 'posts',
            },
          }),
        ]),
      })

      const result = fromIceType(iceSchema)

      expect(result.Post.author).toBe('-> User.posts')
    })

    it('should convert backward relationship with <-', () => {
      const iceSchema = createIceTypeParsedSchema({
        User: createIceTypeSchema([
          createField('posts', 'Post', {
            relation: {
              operator: '<-',
              targetType: 'Post',
              inverse: 'author',
            },
          }),
        ]),
      })

      const result = fromIceType(iceSchema)

      expect(result.User.posts).toBe('<- Post.author')
    })

    it('should convert fuzzy forward relationship with ~>', () => {
      const iceSchema = createIceTypeParsedSchema({
        User: createIceTypeSchema([
          createField('interests', 'Topic', {
            relation: {
              operator: '~>',
              targetType: 'Topic',
              inverse: 'users',
            },
          }),
        ]),
      })

      const result = fromIceType(iceSchema)

      expect(result.User.interests).toBe('~> Topic.users')
    })

    it('should convert fuzzy backward relationship with <~', () => {
      const iceSchema = createIceTypeParsedSchema({
        Topic: createIceTypeSchema([
          createField('relatedUsers', 'User', {
            relation: {
              operator: '<~',
              targetType: 'User',
              inverse: 'topics',
            },
          }),
        ]),
      })

      const result = fromIceType(iceSchema)

      expect(result.Topic.relatedUsers).toBe('<~ User.topics')
    })

    it('should use field name as inverse when not specified', () => {
      const iceSchema = createIceTypeParsedSchema({
        Post: createIceTypeSchema([
          createField('author', 'User', {
            relation: {
              operator: '->',
              targetType: 'User',
              // inverse not specified
            },
          }),
        ]),
      })

      const result = fromIceType(iceSchema)

      expect(result.Post.author).toBe('-> User.author')
    })

    it('should handle multiple relationships in one entity', () => {
      const iceSchema = createIceTypeParsedSchema({
        Comment: createIceTypeSchema([
          createField('author', 'User', {
            relation: {
              operator: '->',
              targetType: 'User',
              inverse: 'comments',
            },
          }),
          createField('post', 'Post', {
            relation: {
              operator: '->',
              targetType: 'Post',
              inverse: 'comments',
            },
          }),
        ]),
      })

      const result = fromIceType(iceSchema)

      expect(result.Comment.author).toBe('-> User.comments')
      expect(result.Comment.post).toBe('-> Post.comments')
    })

    it('should handle self-referential relationships', () => {
      const iceSchema = createIceTypeParsedSchema({
        User: createIceTypeSchema([
          createField('friends', 'User', {
            relation: {
              operator: '~>',
              targetType: 'User',
              inverse: 'friends',
            },
          }),
        ]),
      })

      const result = fromIceType(iceSchema)

      expect(result.User.friends).toBe('~> User.friends')
    })

    // RED PHASE: Array relationships with [] suffix
    it('should handle array relationships with [] suffix', () => {
      const iceSchema = createIceTypeParsedSchema({
        User: createIceTypeSchema([
          createField('posts', 'Post', {
            isArray: true,
            relation: {
              operator: '<-',
              targetType: 'Post',
              inverse: 'author',
            },
          }),
        ]),
      })

      const result = fromIceType(iceSchema)

      expect(result.User.posts).toBe('<- Post.author[]')
    })

    // RED PHASE: Relationship with threshold for fuzzy matching
    it('should preserve threshold for fuzzy relationships', () => {
      const iceSchema = createIceTypeParsedSchema({
        User: createIceTypeSchema([
          createField('similar', 'User', {
            relation: {
              operator: '~>',
              targetType: 'User',
              inverse: 'similar',
              threshold: 0.8,
            },
          }),
        ]),
      })

      const result = fromIceType(iceSchema)
      const field = result.User.similar

      // Should be converted to an object with threshold
      expect(typeof field).toBe('object')
      expect((field as FieldDefinition).type).toContain('~> User.similar')
    })
  })

  // ---------------------------------------------------------------------------
  // 5. Handle $fts array - mark fields for FTS indexing
  // ---------------------------------------------------------------------------
  describe('$fts (Full-Text Search) indexing', () => {
    it('should mark single field for FTS indexing', () => {
      const iceSchema = createIceTypeParsedSchema({
        Post: createIceTypeSchema(
          [
            createField('title', 'string', { required: true }),
            createField('content', 'text'),
          ],
          { $fts: ['title'] }
        ),
      })

      const result = fromIceType(iceSchema)

      expect(result.Post.title).toEqual({
        type: 'string!',
        index: 'fts',
      })
    })

    it('should mark multiple fields for FTS indexing', () => {
      const iceSchema = createIceTypeParsedSchema({
        Post: createIceTypeSchema(
          [
            createField('title', 'string', { required: true }),
            createField('content', 'text'),
            createField('summary', 'text'),
          ],
          { $fts: ['title', 'content', 'summary'] }
        ),
      })

      const result = fromIceType(iceSchema)

      expect(result.Post.title).toEqual({ type: 'string!', index: 'fts' })
      expect(result.Post.content).toEqual({ type: 'text', index: 'fts' })
      expect(result.Post.summary).toEqual({ type: 'text', index: 'fts' })
    })

    it('should preserve non-FTS fields as strings', () => {
      const iceSchema = createIceTypeParsedSchema({
        Post: createIceTypeSchema(
          [
            createField('title', 'string', { required: true }),
            createField('slug', 'string'),
            createField('content', 'text'),
          ],
          { $fts: ['title'] }
        ),
      })

      const result = fromIceType(iceSchema)

      expect(result.Post.title).toEqual({ type: 'string!', index: 'fts' })
      expect(result.Post.slug).toBe('string')
      expect(result.Post.content).toBe('text')
    })

    it('should handle empty $fts array', () => {
      const iceSchema = createIceTypeParsedSchema({
        Post: createIceTypeSchema(
          [createField('title', 'string', { required: true })],
          { $fts: [] }
        ),
      })

      const result = fromIceType(iceSchema)

      expect(result.Post.title).toBe('string!')
    })

    it('should handle $fts with field that has default value', () => {
      const iceSchema = createIceTypeParsedSchema({
        Post: createIceTypeSchema(
          [createField('status', 'string', { default: 'draft' })],
          { $fts: ['status'] }
        ),
      })

      const result = fromIceType(iceSchema)

      expect(result.Post.status).toEqual({
        type: 'string = "draft"',
        index: 'fts',
      })
    })

    // RED PHASE: FTS with language configuration
    it('should support FTS with language configuration', () => {
      const iceSchema = createIceTypeParsedSchema({
        Post: createIceTypeSchema(
          [createField('content', 'text')],
          {
            // @ts-expect-error - extended $fts format not in type yet
            $fts: [{ field: 'content', language: 'english', weight: 1.0 }],
          }
        ),
      })

      const result = fromIceType(iceSchema)

      expect(result.Post.content).toEqual({
        type: 'text',
        index: 'fts',
        ftsOptions: { language: 'english', weight: 1.0 },
      })
    })
  })

  // ---------------------------------------------------------------------------
  // 6. Handle $vector array - mark fields for vector indexing
  // ---------------------------------------------------------------------------
  describe('$vector indexing', () => {
    it('should create vector field with dimensions', () => {
      const iceSchema = createIceTypeParsedSchema({
        Document: createIceTypeSchema([createField('title', 'string')], {
          $vector: [{ field: 'embedding', dimensions: 1536 }],
        }),
      })

      const result = fromIceType(iceSchema)

      expect(result.Document.embedding).toEqual({
        type: 'vector(1536)',
        index: 'vector',
        dimensions: 1536,
      })
    })

    it('should handle multiple vector fields', () => {
      const iceSchema = createIceTypeParsedSchema({
        Document: createIceTypeSchema([createField('title', 'string')], {
          $vector: [
            { field: 'titleEmbedding', dimensions: 768 },
            { field: 'contentEmbedding', dimensions: 1536 },
          ],
        }),
      })

      const result = fromIceType(iceSchema)

      expect(result.Document.titleEmbedding).toEqual({
        type: 'vector(768)',
        index: 'vector',
        dimensions: 768,
      })
      expect(result.Document.contentEmbedding).toEqual({
        type: 'vector(1536)',
        index: 'vector',
        dimensions: 1536,
      })
    })

    it('should handle different vector dimensions', () => {
      const iceSchema = createIceTypeParsedSchema({
        Embeddings: createIceTypeSchema([], {
          $vector: [
            { field: 'small', dimensions: 384 },
            { field: 'medium', dimensions: 768 },
            { field: 'large', dimensions: 3072 },
          ],
        }),
      })

      const result = fromIceType(iceSchema)

      expect((result.Embeddings.small as FieldDefinition).dimensions).toBe(384)
      expect((result.Embeddings.medium as FieldDefinition).dimensions).toBe(768)
      expect((result.Embeddings.large as FieldDefinition).dimensions).toBe(3072)
    })

    it('should handle empty $vector array', () => {
      const iceSchema = createIceTypeParsedSchema({
        Document: createIceTypeSchema(
          [createField('title', 'string', { required: true })],
          { $vector: [] }
        ),
      })

      const result = fromIceType(iceSchema)

      expect(result.Document.title).toBe('string!')
      expect(result.Document).not.toHaveProperty('embedding')
    })

    it('should coexist with regular fields', () => {
      const iceSchema = createIceTypeParsedSchema({
        Document: createIceTypeSchema(
          [
            createField('title', 'string', { required: true }),
            createField('content', 'text'),
          ],
          { $vector: [{ field: 'embedding', dimensions: 1536 }] }
        ),
      })

      const result = fromIceType(iceSchema)

      expect(result.Document.title).toBe('string!')
      expect(result.Document.content).toBe('text')
      expect(result.Document.embedding).toEqual({
        type: 'vector(1536)',
        index: 'vector',
        dimensions: 1536,
      })
    })

    // RED PHASE: Vector with metric specification
    it('should preserve vector metric type', () => {
      const iceSchema = createIceTypeParsedSchema({
        Document: createIceTypeSchema([], {
          $vector: [{ field: 'embedding', dimensions: 1536, metric: 'cosine' }],
        }),
      })

      const result = fromIceType(iceSchema)

      expect(result.Document.embedding).toEqual({
        type: 'vector(1536)',
        index: 'vector',
        dimensions: 1536,
        metric: 'cosine',
      })
    })

    // RED PHASE: Vector with euclidean distance
    it('should support euclidean distance metric', () => {
      const iceSchema = createIceTypeParsedSchema({
        Document: createIceTypeSchema([], {
          $vector: [{ field: 'embedding', dimensions: 768, metric: 'euclidean' }],
        }),
      })

      const result = fromIceType(iceSchema)

      expect((result.Document.embedding as FieldDefinition).metric).toBe('euclidean')
    })
  })

  // ---------------------------------------------------------------------------
  // 7. Handle $index - create compound indexes
  // ---------------------------------------------------------------------------
  describe('$index (compound indexes)', () => {
    it('should create single compound index', () => {
      const iceSchema = createIceTypeParsedSchema({
        Post: createIceTypeSchema(
          [
            createField('authorId', 'uuid'),
            createField('createdAt', 'datetime'),
          ],
          { $index: [['authorId', 'createdAt']] }
        ),
      })

      const result = fromIceType(iceSchema)

      expect(result.Post.$indexes).toBeDefined()
      expect(result.Post.$indexes).toHaveLength(1)
      expect(result.Post.$indexes![0]).toEqual({
        name: 'idx_Post_0',
        fields: [{ field: 'authorId' }, { field: 'createdAt' }],
      })
    })

    it('should create multiple compound indexes', () => {
      const iceSchema = createIceTypeParsedSchema({
        Post: createIceTypeSchema(
          [
            createField('authorId', 'uuid'),
            createField('categoryId', 'uuid'),
            createField('createdAt', 'datetime'),
            createField('status', 'string'),
          ],
          {
            $index: [
              ['authorId', 'createdAt'],
              ['categoryId', 'status'],
            ],
          }
        ),
      })

      const result = fromIceType(iceSchema)

      expect(result.Post.$indexes).toHaveLength(2)
      expect(result.Post.$indexes![0]).toEqual({
        name: 'idx_Post_0',
        fields: [{ field: 'authorId' }, { field: 'createdAt' }],
      })
      expect(result.Post.$indexes![1]).toEqual({
        name: 'idx_Post_1',
        fields: [{ field: 'categoryId' }, { field: 'status' }],
      })
    })

    it('should create single-field index', () => {
      const iceSchema = createIceTypeParsedSchema({
        User: createIceTypeSchema([createField('email', 'email')], {
          $index: [['email']],
        }),
      })

      const result = fromIceType(iceSchema)

      expect(result.User.$indexes).toHaveLength(1)
      expect(result.User.$indexes![0].fields).toEqual([{ field: 'email' }])
    })

    it('should handle empty $index array', () => {
      const iceSchema = createIceTypeParsedSchema({
        User: createIceTypeSchema([createField('name', 'string')], {
          $index: [],
        }),
      })

      const result = fromIceType(iceSchema)

      // Should either not have $indexes or have empty array
      expect(result.User.$indexes ?? []).toHaveLength(0)
    })

    it('should handle three-field compound index', () => {
      const iceSchema = createIceTypeParsedSchema({
        Order: createIceTypeSchema(
          [
            createField('customerId', 'uuid'),
            createField('status', 'string'),
            createField('createdAt', 'datetime'),
          ],
          { $index: [['customerId', 'status', 'createdAt']] }
        ),
      })

      const result = fromIceType(iceSchema)

      expect(result.Order.$indexes![0].fields).toHaveLength(3)
    })

    // RED PHASE: Index with unique constraint
    it('should support unique index option', () => {
      const iceSchema = createIceTypeParsedSchema({
        User: createIceTypeSchema([createField('email', 'email')], {
          $index: [{ fields: ['email'], unique: true }] as any,
        }),
      })

      const result = fromIceType(iceSchema)

      expect(result.User.$indexes![0]).toEqual({
        name: expect.any(String),
        fields: [{ field: 'email' }],
        unique: true,
      })
    })

    // RED PHASE: Index with custom name
    it('should support custom index names', () => {
      const iceSchema = createIceTypeParsedSchema({
        User: createIceTypeSchema([createField('email', 'email')], {
          $index: [{ fields: ['email'], name: 'unique_email_idx' }] as any,
        }),
      })

      const result = fromIceType(iceSchema)

      expect(result.User.$indexes![0].name).toBe('unique_email_idx')
    })

    // RED PHASE: Index with sort direction
    it('should support index sort direction', () => {
      const iceSchema = createIceTypeParsedSchema({
        Post: createIceTypeSchema(
          [createField('createdAt', 'datetime')],
          {
            $index: [{ fields: [{ field: 'createdAt', direction: -1 }] }] as any,
          }
        ),
      })

      const result = fromIceType(iceSchema)

      expect(result.Post.$indexes![0].fields).toEqual([
        { field: 'createdAt', direction: -1 },
      ])
    })

    // RED PHASE: Sparse index
    it('should support sparse index option', () => {
      const iceSchema = createIceTypeParsedSchema({
        User: createIceTypeSchema([createField('phone', 'string')], {
          $index: [{ fields: ['phone'], sparse: true }] as any,
        }),
      })

      const result = fromIceType(iceSchema)

      expect(result.User.$indexes![0]).toMatchObject({
        fields: [{ field: 'phone' }],
        sparse: true,
      })
    })
  })

  // ---------------------------------------------------------------------------
  // 8. Handle $partitionBy - convert to $shred
  // ---------------------------------------------------------------------------
  describe('$partitionBy to $shred conversion', () => {
    it('should convert single partition field to $shred', () => {
      const iceSchema = createIceTypeParsedSchema({
        Event: createIceTypeSchema(
          [
            createField('tenantId', 'uuid'),
            createField('data', 'json'),
          ],
          { $partitionBy: ['tenantId'] }
        ),
      })

      const result = fromIceType(iceSchema)

      expect(result.Event.$shred).toEqual(['tenantId'])
    })

    it('should convert multiple partition fields to $shred', () => {
      const iceSchema = createIceTypeParsedSchema({
        Event: createIceTypeSchema(
          [
            createField('tenantId', 'uuid'),
            createField('year', 'int'),
            createField('month', 'int'),
          ],
          { $partitionBy: ['tenantId', 'year', 'month'] }
        ),
      })

      const result = fromIceType(iceSchema)

      expect(result.Event.$shred).toEqual(['tenantId', 'year', 'month'])
    })

    it('should handle empty $partitionBy array', () => {
      const iceSchema = createIceTypeParsedSchema({
        Event: createIceTypeSchema([createField('data', 'json')], {
          $partitionBy: [],
        }),
      })

      const result = fromIceType(iceSchema)

      // Should either not have $shred or have empty array
      expect(result.Event.$shred ?? []).toHaveLength(0)
    })

    it('should preserve $partitionBy order in $shred', () => {
      const iceSchema = createIceTypeParsedSchema({
        TimeSeries: createIceTypeSchema(
          [
            createField('date', 'date'),
            createField('hour', 'int'),
            createField('metric', 'string'),
          ],
          { $partitionBy: ['date', 'hour', 'metric'] }
        ),
      })

      const result = fromIceType(iceSchema)

      expect(result.TimeSeries.$shred).toEqual(['date', 'hour', 'metric'])
      expect(result.TimeSeries.$shred![0]).toBe('date')
      expect(result.TimeSeries.$shred![1]).toBe('hour')
      expect(result.TimeSeries.$shred![2]).toBe('metric')
    })
  })

  // ---------------------------------------------------------------------------
  // 9. Preserve $type metadata
  // ---------------------------------------------------------------------------
  describe('$type metadata preservation', () => {
    it('should preserve $type as schema.org type', () => {
      const iceSchema = createIceTypeParsedSchema({
        Person: createIceTypeSchema([createField('name', 'string')], {
          $type: 'schema:Person',
        }),
      })

      const result = fromIceType(iceSchema)

      expect(result.Person.$type).toBe('schema:Person')
    })

    it('should preserve $type as full URI', () => {
      const iceSchema = createIceTypeParsedSchema({
        Article: createIceTypeSchema([createField('title', 'string')], {
          $type: 'https://schema.org/Article',
        }),
      })

      const result = fromIceType(iceSchema)

      expect(result.Article.$type).toBe('https://schema.org/Article')
    })

    it('should handle entities without $type', () => {
      const iceSchema = createIceTypeParsedSchema({
        InternalEntity: createIceTypeSchema([createField('data', 'json')]),
      })

      const result = fromIceType(iceSchema)

      expect(result.InternalEntity.$type).toBeUndefined()
    })

    it('should preserve multiple entity $types', () => {
      const iceSchema = createIceTypeParsedSchema({
        Person: createIceTypeSchema([createField('name', 'string')], {
          $type: 'schema:Person',
        }),
        Organization: createIceTypeSchema([createField('name', 'string')], {
          $type: 'schema:Organization',
        }),
      })

      const result = fromIceType(iceSchema)

      expect(result.Person.$type).toBe('schema:Person')
      expect(result.Organization.$type).toBe('schema:Organization')
    })

    it('should preserve custom $type values', () => {
      const iceSchema = createIceTypeParsedSchema({
        CustomEntity: createIceTypeSchema([createField('data', 'json')], {
          $type: 'myapp:CustomEntity/v2',
        }),
      })

      const result = fromIceType(iceSchema)

      expect(result.CustomEntity.$type).toBe('myapp:CustomEntity/v2')
    })

    // RED PHASE: Preserve $description metadata
    it('should preserve $description metadata', () => {
      const iceSchema = createIceTypeParsedSchema({
        User: createIceTypeSchema([createField('name', 'string')], {
          $type: 'schema:Person',
          $description: 'A user in the system',
        }),
      })

      const result = fromIceType(iceSchema)

      expect(result.User.$description).toBe('A user in the system')
    })
  })

  // ---------------------------------------------------------------------------
  // 10. Error handling for invalid schemas
  // ---------------------------------------------------------------------------
  describe('error handling', () => {
    it('should handle empty schema gracefully', () => {
      const iceSchema = createIceTypeParsedSchema({})

      const result = fromIceType(iceSchema)

      expect(result).toEqual({})
    })

    it('should handle entity with no fields', () => {
      const iceSchema = createIceTypeParsedSchema({
        EmptyEntity: createIceTypeSchema([]),
      })

      const result = fromIceType(iceSchema)

      expect(result).toHaveProperty('EmptyEntity')
      // Should have no field properties (only potential metadata)
    })

    it('should throw error for null schema input', () => {
      expect(() => {
        fromIceType(null as unknown as IceTypeParsedSchema)
      }).toThrow()
    })

    it('should throw error for undefined schema input', () => {
      expect(() => {
        fromIceType(undefined as unknown as IceTypeParsedSchema)
      }).toThrow()
    })

    it('should throw error for schema without schemas map', () => {
      const invalidSchema = {
        getSchema: () => undefined,
      } as unknown as IceTypeParsedSchema

      expect(() => {
        fromIceType(invalidSchema)
      }).toThrow()
    })

    it('should handle $fts referencing non-existent field gracefully', () => {
      const iceSchema = createIceTypeParsedSchema({
        Post: createIceTypeSchema([createField('title', 'string')], {
          $fts: ['nonExistentField'],
        }),
      })

      // Should either ignore the non-existent field or throw a clear error
      const result = fromIceType(iceSchema)

      // The non-existent field should not be added to the result
      expect(result.Post).not.toHaveProperty('nonExistentField')
    })

    it('should handle $index referencing non-existent fields', () => {
      const iceSchema = createIceTypeParsedSchema({
        Post: createIceTypeSchema([createField('title', 'string')], {
          $index: [['nonExistentField1', 'nonExistentField2']],
        }),
      })

      // Should still create the index (validation happens elsewhere)
      const result = fromIceType(iceSchema)

      expect(result.Post.$indexes).toBeDefined()
    })

    it('should handle invalid relationship operator gracefully', () => {
      const iceSchema = createIceTypeParsedSchema({
        Post: createIceTypeSchema([
          createField('author', 'User', {
            relation: {
              operator: '>>>' as any, // Invalid operator
              targetType: 'User',
              inverse: 'posts',
            },
          }),
        ]),
      })

      // Should either use the operator as-is or throw an error
      expect(() => fromIceType(iceSchema)).not.toThrow()
    })

    it('should handle field with undefined type', () => {
      const iceSchema = createIceTypeParsedSchema({
        Entity: createIceTypeSchema([
          createField('field', undefined as unknown as string),
        ]),
      })

      // Should handle gracefully
      expect(() => fromIceType(iceSchema)).not.toThrow()
    })

    // RED PHASE: Throw descriptive error for circular references
    it('should detect and warn about circular relationship references', () => {
      const iceSchema = createIceTypeParsedSchema({
        Node: createIceTypeSchema([
          createField('parent', 'Node', {
            relation: {
              operator: '->',
              targetType: 'Node',
              inverse: 'children',
            },
          }),
          createField('children', 'Node', {
            isArray: true,
            relation: {
              operator: '<-',
              targetType: 'Node',
              inverse: 'parent',
            },
          }),
        ]),
      })

      const result = fromIceType(iceSchema)

      // Should still work but might include metadata about circular refs
      expect(result.Node.parent).toBeDefined()
      expect(result.Node.children).toBeDefined()
    })

    // RED PHASE: Validate vector dimensions are positive
    it('should throw error for invalid vector dimensions', () => {
      const iceSchema = createIceTypeParsedSchema({
        Document: createIceTypeSchema([], {
          $vector: [{ field: 'embedding', dimensions: -1 }],
        }),
      })

      expect(() => fromIceType(iceSchema)).toThrow(/dimensions/i)
    })

    // RED PHASE: Validate vector dimensions are integers
    it('should throw error for non-integer vector dimensions', () => {
      const iceSchema = createIceTypeParsedSchema({
        Document: createIceTypeSchema([], {
          $vector: [{ field: 'embedding', dimensions: 1536.5 }],
        }),
      })

      expect(() => fromIceType(iceSchema)).toThrow(/dimensions/i)
    })
  })

  // ---------------------------------------------------------------------------
  // 11. Handle $unique constraints
  // ---------------------------------------------------------------------------
  describe('$unique constraints', () => {
    // RED PHASE: Convert $unique to unique indexes
    it('should convert $unique to unique indexes', () => {
      const iceSchema = createIceTypeParsedSchema({
        User: createIceTypeSchema([createField('email', 'email')], {
          $unique: [['email']],
        }),
      })

      const result = fromIceType(iceSchema)

      expect(result.User.$indexes).toBeDefined()
      expect(result.User.$indexes!.some(idx => idx.unique)).toBe(true)
    })

    // RED PHASE: Compound unique constraint
    it('should convert compound $unique to unique indexes', () => {
      const iceSchema = createIceTypeParsedSchema({
        Membership: createIceTypeSchema([
          createField('userId', 'uuid'),
          createField('orgId', 'uuid'),
        ], {
          $unique: [['userId', 'orgId']],
        }),
      })

      const result = fromIceType(iceSchema)

      expect(result.Membership.$indexes).toBeDefined()
      const uniqueIdx = result.Membership.$indexes!.find(idx => idx.unique)
      expect(uniqueIdx).toBeDefined()
      expect(uniqueIdx!.fields).toHaveLength(2)
    })
  })

  // ---------------------------------------------------------------------------
  // 12. Field-level metadata
  // ---------------------------------------------------------------------------
  describe('field-level metadata', () => {
    // RED PHASE: Preserve field descriptions
    it('should preserve field descriptions', () => {
      const iceSchema = createIceTypeParsedSchema({
        User: createIceTypeSchema([
          createField('email', 'email', {
            required: true,
            description: 'Primary email address for the user',
          }),
        ]),
      })

      const result = fromIceType(iceSchema)

      expect(typeof result.User.email).toBe('object')
      expect((result.User.email as FieldDefinition).description).toBe(
        'Primary email address for the user'
      )
    })

    // RED PHASE: Preserve validation rules
    it('should preserve field validation rules', () => {
      const iceSchema = createIceTypeParsedSchema({
        Product: createIceTypeSchema([
          createField('price', 'float', {
            required: true,
            validation: { min: 0, max: 999999 },
          }),
        ]),
      })

      const result = fromIceType(iceSchema)

      expect(typeof result.Product.price).toBe('object')
      expect((result.Product.price as FieldDefinition).min).toBe(0)
      expect((result.Product.price as FieldDefinition).max).toBe(999999)
    })

    // RED PHASE: Preserve string length constraints
    it('should preserve string length constraints', () => {
      const iceSchema = createIceTypeParsedSchema({
        User: createIceTypeSchema([
          createField('username', 'string', {
            required: true,
            validation: { minLength: 3, maxLength: 50 },
          }),
        ]),
      })

      const result = fromIceType(iceSchema)

      expect(typeof result.User.username).toBe('object')
      expect((result.User.username as FieldDefinition).minLength).toBe(3)
      expect((result.User.username as FieldDefinition).maxLength).toBe(50)
    })

    // RED PHASE: Preserve regex pattern
    it('should preserve regex pattern validation', () => {
      const iceSchema = createIceTypeParsedSchema({
        User: createIceTypeSchema([
          createField('username', 'string', {
            required: true,
            validation: { pattern: '^[a-z0-9_]+$' },
          }),
        ]),
      })

      const result = fromIceType(iceSchema)

      expect(typeof result.User.username).toBe('object')
      expect((result.User.username as FieldDefinition).pattern).toBe('^[a-z0-9_]+$')
    })
  })

  // ---------------------------------------------------------------------------
  // Complex/Integration tests
  // ---------------------------------------------------------------------------
  describe('complex schema conversion', () => {
    it('should convert a complete blog schema', () => {
      const iceSchema = createIceTypeParsedSchema({
        User: createIceTypeSchema(
          [
            createField('id', 'uuid', { required: true }),
            createField('email', 'email', { required: true }),
            createField('name', 'string', { required: true }),
            createField('bio', 'text'),
            createField('createdAt', 'timestamp', { required: true }),
          ],
          {
            $type: 'schema:Person',
            $index: [['email']],
            $fts: ['name', 'bio'],
          }
        ),
        Post: createIceTypeSchema(
          [
            createField('id', 'uuid', { required: true }),
            createField('title', 'string', { required: true }),
            createField('content', 'markdown', { required: true }),
            createField('status', 'string', { default: 'draft' }),
            createField('publishedAt', 'datetime'),
            createField('author', 'User', {
              relation: { operator: '->', targetType: 'User', inverse: 'posts' },
            }),
          ],
          {
            $type: 'schema:BlogPosting',
            $partitionBy: ['status'],
            $fts: ['title', 'content'],
            $vector: [{ field: 'embedding', dimensions: 1536 }],
          }
        ),
        Comment: createIceTypeSchema(
          [
            createField('id', 'uuid', { required: true }),
            createField('text', 'text', { required: true }),
            createField('createdAt', 'timestamp', { required: true }),
            createField('author', 'User', {
              relation: { operator: '->', targetType: 'User', inverse: 'comments' },
            }),
            createField('post', 'Post', {
              relation: { operator: '->', targetType: 'Post', inverse: 'comments' },
            }),
          ],
          {
            $index: [['post', 'createdAt']],
          }
        ),
      })

      const result = fromIceType(iceSchema)

      // User assertions
      expect(result.User.$type).toBe('schema:Person')
      expect(result.User.$indexes).toBeDefined()
      expect(result.User.name).toEqual({ type: 'string!', index: 'fts' })
      expect(result.User.bio).toEqual({ type: 'text', index: 'fts' })

      // Post assertions
      expect(result.Post.$type).toBe('schema:BlogPosting')
      expect(result.Post.$shred).toEqual(['status'])
      expect(result.Post.title).toEqual({ type: 'string!', index: 'fts' })
      expect(result.Post.content).toEqual({ type: 'markdown!', index: 'fts' })
      expect(result.Post.embedding).toEqual({
        type: 'vector(1536)',
        index: 'vector',
        dimensions: 1536,
      })
      expect(result.Post.author).toBe('-> User.posts')

      // Comment assertions
      expect(result.Comment.$indexes).toHaveLength(1)
      expect(result.Comment.author).toBe('-> User.comments')
      expect(result.Comment.post).toBe('-> Post.comments')
    })

    it('should handle all features combined on single entity', () => {
      const iceSchema = createIceTypeParsedSchema({
        ComplexEntity: createIceTypeSchema(
          [
            createField('id', 'uuid', { required: true }),
            createField('name', 'string', { required: true }),
            createField('description', 'text'),
            createField('status', 'string', { default: 'active' }),
            createField('count', 'int', { default: 0 }),
            createField('owner', 'User', {
              relation: { operator: '->', targetType: 'User', inverse: 'entities' },
            }),
          ],
          {
            $type: 'app:ComplexEntity',
            $partitionBy: ['status'],
            $fts: ['name', 'description'],
            $vector: [{ field: 'embedding', dimensions: 768 }],
            $index: [
              ['owner', 'status'],
              ['status', 'count'],
            ],
          }
        ),
      })

      const result = fromIceType(iceSchema)

      // All features should be present
      expect(result.ComplexEntity.$type).toBe('app:ComplexEntity')
      expect(result.ComplexEntity.$shred).toEqual(['status'])
      expect(result.ComplexEntity.$indexes).toHaveLength(2)
      expect(result.ComplexEntity.name).toEqual({ type: 'string!', index: 'fts' })
      expect(result.ComplexEntity.description).toEqual({ type: 'text', index: 'fts' })
      expect(result.ComplexEntity.embedding).toEqual({
        type: 'vector(768)',
        index: 'vector',
        dimensions: 768,
      })
      expect(result.ComplexEntity.owner).toBe('-> User.entities')
    })

    // RED PHASE: E-commerce schema with all advanced features
    it('should convert an e-commerce schema with all advanced features', () => {
      const iceSchema = createIceTypeParsedSchema({
        Product: createIceTypeSchema(
          [
            createField('id', 'uuid', { required: true }),
            createField('sku', 'string', { required: true }),
            createField('name', 'string', { required: true }),
            createField('description', 'markdown'),
            createField('price', 'decimal(10,2)', { required: true }),
            createField('stock', 'int', { default: 0 }),
            createField('tags', 'string', { isArray: true }),
            createField('category', 'Category', {
              relation: { operator: '->', targetType: 'Category', inverse: 'products' },
            }),
          ],
          {
            $type: 'schema:Product',
            $unique: [['sku']],
            $fts: ['name', 'description'],
            $vector: [
              { field: 'embedding', dimensions: 1536, metric: 'cosine' },
            ],
            $index: [
              { fields: ['category', 'price'], name: 'products_by_category_price' } as any,
            ],
          }
        ),
        Category: createIceTypeSchema(
          [
            createField('id', 'uuid', { required: true }),
            createField('name', 'string', { required: true }),
            createField('slug', 'string', { required: true }),
            createField('parent', 'Category', {
              relation: { operator: '->', targetType: 'Category', inverse: 'children' },
            }),
            createField('children', 'Category', {
              isArray: true,
              relation: { operator: '<-', targetType: 'Category', inverse: 'parent' },
            }),
          ],
          {
            $type: 'schema:Category',
            $unique: [['slug']],
          }
        ),
      })

      const result = fromIceType(iceSchema)

      // Product should have array fields
      expect(result.Product.tags).toBe('string[]')

      // Product should have unique SKU index
      const skuIdx = result.Product.$indexes?.find(idx =>
        idx.fields.some(f => typeof f === 'object' && f.field === 'sku')
      )
      expect(skuIdx?.unique).toBe(true)

      // Product should have named index
      const namedIdx = result.Product.$indexes?.find(idx =>
        idx.name === 'products_by_category_price'
      )
      expect(namedIdx).toBeDefined()

      // Product embedding should have metric
      expect((result.Product.embedding as FieldDefinition).metric).toBe('cosine')

      // Category should have array relationship
      expect(result.Category.children).toBe('<- Category.parent[]')
    })
  })
})
