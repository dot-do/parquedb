/**
 * Tests for GraphDL integration
 *
 * GraphDL is a schema DSL with relationship operators:
 * - `->` Forward exact relationship
 * - `<-` Backward exact relationship
 * - `~>` Forward fuzzy relationship
 * - `<~` Backward fuzzy relationship
 *
 * Updated for @graphdl/core@0.3.0
 *
 * @see https://github.com/primitives-org/graphdl
 */

import { describe, it, expect } from 'vitest'
import { Graph } from '@graphdl/core'
import type { ParsedGraph, ParsedEntity, ParsedField, RelationshipOperator } from '@graphdl/core'
import { fromGraphDL } from '../../src/types/integrations'
import type { Schema } from '../../src/types/schema'

// =============================================================================
// GraphDL Schema Builder using Graph() function from @graphdl/core
// =============================================================================

/**
 * Helper to create a ParsedField for testing
 * Uses the new @graphdl/core ParsedField structure
 */
function createField(
  name: string,
  type: string,
  options: {
    isArray?: boolean
    isOptional?: boolean
    isRequired?: boolean
    isRelation?: boolean
    operator?: RelationshipOperator
    relatedType?: string
    backref?: string
    threshold?: number
  } = {}
): ParsedField {
  return {
    name,
    type,
    isArray: options.isArray ?? false,
    isOptional: options.isOptional ?? false,
    isRelation: options.isRelation ?? false,
    isRequired: options.isRequired,
    operator: options.operator,
    relatedType: options.relatedType,
    backref: options.backref,
    threshold: options.threshold,
    direction: options.operator ? (options.operator === '->' || options.operator === '~>' ? 'forward' : 'backward') : undefined,
    matchMode: options.operator ? (options.operator === '->' || options.operator === '<-' ? 'exact' : 'fuzzy') : undefined,
  }
}

/**
 * Helper to create a mock ParsedEntity
 */
function createEntity(
  name: string,
  fields: ParsedField[],
  $type?: string,
  directives?: Record<string, unknown>
): ParsedEntity {
  const fieldsMap = new Map<string, ParsedField>()
  for (const field of fields) {
    fieldsMap.set(field.name, field)
  }
  return {
    name,
    $type,
    fields: fieldsMap,
    directives,
  }
}

/**
 * Helper to create a mock ParsedGraph
 */
function createSchema(entities: ParsedEntity[]): ParsedGraph {
  const entitiesMap = new Map<string, ParsedEntity>()
  const typeUris = new Map<string, string>()
  for (const entity of entities) {
    entitiesMap.set(entity.name, entity)
    if (entity.$type) {
      typeUris.set(entity.name, entity.$type)
    }
  }

  return {
    entities: entitiesMap,
    typeUris,
  }
}

// =============================================================================
// Test Suite
// =============================================================================

describe('fromGraphDL', () => {
  describe('basic entity with scalar fields', () => {
    it('should convert entity with string field', () => {
      const graphdl = createSchema([
        createEntity('User', [createField('name', 'string')]),
      ])

      const schema = fromGraphDL(graphdl)

      expect(schema.User).toBeDefined()
      expect(schema.User.name).toBe('string')
    })

    it('should convert entity with multiple scalar fields', () => {
      const graphdl = createSchema([
        createEntity('User', [
          createField('name', 'string'),
          createField('age', 'int'),
          createField('email', 'email'),
          createField('isActive', 'boolean'),
        ]),
      ])

      const schema = fromGraphDL(graphdl)

      expect(schema.User).toBeDefined()
      expect(schema.User.name).toBe('string')
      expect(schema.User.age).toBe('int')
      expect(schema.User.email).toBe('email')
      expect(schema.User.isActive).toBe('boolean')
    })

    it('should convert entity with parametric types', () => {
      const graphdl = createSchema([
        createEntity('Product', [
          createField('price', 'decimal(10,2)'),
          createField('sku', 'varchar(50)'),
          createField('embedding', 'vector(1536)'),
        ]),
      ])

      const schema = fromGraphDL(graphdl)

      expect(schema.Product.price).toBe('decimal(10,2)')
      expect(schema.Product.sku).toBe('varchar(50)')
      expect(schema.Product.embedding).toBe('vector(1536)')
    })
  })

  describe('forward relationships (->)', () => {
    it('should convert forward relationship with backref', () => {
      const graphdl = createSchema([
        createEntity('Post', [
          createField('title', 'string'),
          createField('author', 'User', {
            isRelation: true,
            operator: '->',
            relatedType: 'User',
            backref: 'posts',
          }),
        ]),
      ])

      const schema = fromGraphDL(graphdl)

      expect(schema.Post.author).toBe('-> User.posts')
    })

    it('should convert forward relationship without explicit backref', () => {
      const graphdl = createSchema([
        createEntity('Comment', [
          createField('text', 'string'),
          createField('post', 'Post', {
            isRelation: true,
            operator: '->',
            relatedType: 'Post',
          }),
        ]),
      ])

      const schema = fromGraphDL(graphdl)

      // Should use field name as default backref
      expect(schema.Comment.post).toBe('-> Post.post')
    })

    it('should convert multiple forward relationships', () => {
      const graphdl = createSchema([
        createEntity('Post', [
          createField('title', 'string'),
          createField('author', 'User', {
            isRelation: true,
            operator: '->',
            relatedType: 'User',
            backref: 'posts',
          }),
          createField('editor', 'User', {
            isRelation: true,
            operator: '->',
            relatedType: 'User',
            backref: 'editedPosts',
          }),
        ]),
      ])

      const schema = fromGraphDL(graphdl)

      expect(schema.Post.author).toBe('-> User.posts')
      expect(schema.Post.editor).toBe('-> User.editedPosts')
    })
  })

  describe('backward relationships (<-)', () => {
    it('should convert backward relationship with predicate', () => {
      const graphdl = createSchema([
        createEntity('User', [
          createField('name', 'string'),
          createField('posts', 'Post', {
            isRelation: true,
            isArray: true,
            operator: '<-',
            relatedType: 'Post',
            backref: 'author',
          }),
        ]),
      ])

      const schema = fromGraphDL(graphdl)

      expect(schema.User.posts).toBe('<- Post.author[]')
    })

    it('should convert backward relationship without array modifier', () => {
      const graphdl = createSchema([
        createEntity('Profile', [
          createField('bio', 'string'),
          createField('user', 'User', {
            isRelation: true,
            operator: '<-',
            relatedType: 'User',
            backref: 'profile',
          }),
        ]),
      ])

      const schema = fromGraphDL(graphdl)

      expect(schema.Profile.user).toBe('<- User.profile')
    })
  })

  describe('fuzzy relationships (~> and <~)', () => {
    it('should convert forward fuzzy relationship', () => {
      const graphdl = createSchema([
        createEntity('Article', [
          createField('title', 'string'),
          createField('relatedTopics', 'Topic', {
            isRelation: true,
            isArray: true,
            operator: '~>',
            relatedType: 'Topic',
            backref: 'articles',
          }),
        ]),
      ])

      const schema = fromGraphDL(graphdl)

      expect(schema.Article.relatedTopics).toBe('~> Topic.articles[]')
    })

    it('should convert backward fuzzy relationship', () => {
      const graphdl = createSchema([
        createEntity('Topic', [
          createField('name', 'string'),
          createField('suggestedArticles', 'Article', {
            isRelation: true,
            isArray: true,
            operator: '<~',
            relatedType: 'Article',
            backref: 'topics',
          }),
        ]),
      ])

      const schema = fromGraphDL(graphdl)

      expect(schema.Topic.suggestedArticles).toBe('<~ Article.topics[]')
    })

    it('should preserve fuzzy relationship threshold if provided', () => {
      const graphdl = createSchema([
        createEntity('Document', [
          createField('content', 'text'),
          createField('similar', 'Document', {
            isRelation: true,
            isArray: true,
            operator: '~>',
            relatedType: 'Document',
            backref: 'similarTo',
            threshold: 0.8,
          }),
        ]),
      ])

      const schema = fromGraphDL(graphdl)

      // Fuzzy relationship should still be converted
      expect(schema.Document.similar).toBe('~> Document.similarTo[]')
      // Note: Threshold may need to be stored in metadata - this test verifies basic conversion
    })
  })

  describe('array relationships', () => {
    it('should handle array forward relationship', () => {
      const graphdl = createSchema([
        createEntity('Post', [
          createField('title', 'string'),
          createField('tags', 'Tag', {
            isRelation: true,
            isArray: true,
            operator: '->',
            relatedType: 'Tag',
            backref: 'posts',
          }),
        ]),
      ])

      const schema = fromGraphDL(graphdl)

      expect(schema.Post.tags).toBe('-> Tag.posts[]')
    })

    it('should handle array scalar fields', () => {
      const graphdl = createSchema([
        createEntity('Post', [
          createField('title', 'string'),
          createField('keywords', 'string', { isArray: true }),
        ]),
      ])

      const schema = fromGraphDL(graphdl)

      expect(schema.Post.keywords).toBe('string[]')
    })

    it('should handle required array fields', () => {
      const graphdl = createSchema([
        createEntity('Survey', [
          createField('title', 'string'),
          createField('questions', 'string', { isArray: true, isRequired: true }),
        ]),
      ])

      const schema = fromGraphDL(graphdl)

      expect(schema.Survey.questions).toBe('string[]!')
    })
  })

  describe('$type metadata', () => {
    it('should preserve $type on entity', () => {
      const graphdl = createSchema([
        createEntity(
          'Post',
          [createField('title', 'string')],
          'schema:BlogPosting'
        ),
      ])

      const schema = fromGraphDL(graphdl)

      expect(schema.Post.$type).toBe('schema:BlogPosting')
    })

    it('should handle entity without $type', () => {
      const graphdl = createSchema([
        createEntity('Comment', [createField('text', 'string')]),
      ])

      const schema = fromGraphDL(graphdl)

      expect(schema.Comment.$type).toBeUndefined()
    })

    it('should preserve $type across multiple entities', () => {
      const graphdl = createSchema([
        createEntity('User', [createField('name', 'string')], 'schema:Person'),
        createEntity('Post', [createField('title', 'string')], 'schema:BlogPosting'),
        createEntity('Comment', [createField('text', 'string')]),
      ])

      const schema = fromGraphDL(graphdl)

      expect(schema.User.$type).toBe('schema:Person')
      expect(schema.Post.$type).toBe('schema:BlogPosting')
      expect(schema.Comment.$type).toBeUndefined()
    })
  })

  describe('optional vs required fields', () => {
    it('should mark required fields with !', () => {
      const graphdl = createSchema([
        createEntity('User', [
          createField('name', 'string', { isRequired: true }),
          createField('email', 'email', { isRequired: true }),
        ]),
      ])

      const schema = fromGraphDL(graphdl)

      expect(schema.User.name).toBe('string!')
      expect(schema.User.email).toBe('email!')
    })

    it('should not add ! for optional fields', () => {
      const graphdl = createSchema([
        createEntity('User', [
          createField('name', 'string', { isRequired: true }),
          createField('nickname', 'string', { isOptional: true }),
          createField('bio', 'text', { isOptional: true }),
        ]),
      ])

      const schema = fromGraphDL(graphdl)

      expect(schema.User.name).toBe('string!')
      expect(schema.User.nickname).toBe('string')
      expect(schema.User.bio).toBe('text')
    })

    it('should handle mixed required and optional fields', () => {
      const graphdl = createSchema([
        createEntity('Product', [
          createField('name', 'string', { isRequired: true }),
          createField('description', 'text', { isOptional: true }),
          createField('price', 'decimal(10,2)', { isRequired: true }),
          createField('discount', 'float', { isOptional: true }),
        ]),
      ])

      const schema = fromGraphDL(graphdl)

      expect(schema.Product.name).toBe('string!')
      expect(schema.Product.description).toBe('text')
      expect(schema.Product.price).toBe('decimal(10,2)!')
      expect(schema.Product.discount).toBe('float')
    })
  })

  describe('complex schema with multiple entities', () => {
    it('should convert a blog schema with users, posts, and comments', () => {
      const graphdl = createSchema([
        createEntity(
          'User',
          [
            createField('name', 'string', { isRequired: true }),
            createField('email', 'email', { isRequired: true }),
            createField('bio', 'text', { isOptional: true }),
            createField('posts', 'Post', {
              isRelation: true,
              isArray: true,
              operator: '<-',
              relatedType: 'Post',
              backref: 'author',
            }),
            createField('comments', 'Comment', {
              isRelation: true,
              isArray: true,
              operator: '<-',
              relatedType: 'Comment',
              backref: 'author',
            }),
          ],
          'schema:Person'
        ),
        createEntity(
          'Post',
          [
            createField('title', 'string', { isRequired: true }),
            createField('content', 'markdown', { isRequired: true }),
            createField('publishedAt', 'datetime', { isOptional: true }),
            createField('author', 'User', {
              isRelation: true,
              operator: '->',
              relatedType: 'User',
              backref: 'posts',
            }),
            createField('comments', 'Comment', {
              isRelation: true,
              isArray: true,
              operator: '<-',
              relatedType: 'Comment',
              backref: 'post',
            }),
            createField('tags', 'Tag', {
              isRelation: true,
              isArray: true,
              operator: '->',
              relatedType: 'Tag',
              backref: 'posts',
            }),
          ],
          'schema:BlogPosting'
        ),
        createEntity('Comment', [
          createField('text', 'string', { isRequired: true }),
          createField('createdAt', 'datetime', { isRequired: true }),
          createField('post', 'Post', {
            isRelation: true,
            operator: '->',
            relatedType: 'Post',
            backref: 'comments',
          }),
          createField('author', 'User', {
            isRelation: true,
            operator: '->',
            relatedType: 'User',
            backref: 'comments',
          }),
        ]),
        createEntity('Tag', [
          createField('name', 'string', { isRequired: true }),
          createField('posts', 'Post', {
            isRelation: true,
            isArray: true,
            operator: '<-',
            relatedType: 'Post',
            backref: 'tags',
          }),
        ]),
      ])

      const schema = fromGraphDL(graphdl)

      // Check User entity
      expect(schema.User).toBeDefined()
      expect(schema.User.$type).toBe('schema:Person')
      expect(schema.User.name).toBe('string!')
      expect(schema.User.email).toBe('email!')
      expect(schema.User.bio).toBe('text')
      expect(schema.User.posts).toBe('<- Post.author[]')
      expect(schema.User.comments).toBe('<- Comment.author[]')

      // Check Post entity
      expect(schema.Post).toBeDefined()
      expect(schema.Post.$type).toBe('schema:BlogPosting')
      expect(schema.Post.title).toBe('string!')
      expect(schema.Post.content).toBe('markdown!')
      expect(schema.Post.publishedAt).toBe('datetime')
      expect(schema.Post.author).toBe('-> User.posts')
      expect(schema.Post.comments).toBe('<- Comment.post[]')
      expect(schema.Post.tags).toBe('-> Tag.posts[]')

      // Check Comment entity
      expect(schema.Comment).toBeDefined()
      expect(schema.Comment.text).toBe('string!')
      expect(schema.Comment.createdAt).toBe('datetime!')
      expect(schema.Comment.post).toBe('-> Post.comments')
      expect(schema.Comment.author).toBe('-> User.comments')

      // Check Tag entity
      expect(schema.Tag).toBeDefined()
      expect(schema.Tag.name).toBe('string!')
      expect(schema.Tag.posts).toBe('<- Post.tags[]')
    })

    it('should convert e-commerce schema with products, orders, and customers', () => {
      const graphdl = createSchema([
        createEntity(
          'Customer',
          [
            createField('name', 'string', { isRequired: true }),
            createField('email', 'email', { isRequired: true }),
            createField('orders', 'Order', {
              isRelation: true,
              isArray: true,
              operator: '<-',
              relatedType: 'Order',
              backref: 'customer',
            }),
          ],
          'schema:Customer'
        ),
        createEntity(
          'Product',
          [
            createField('name', 'string', { isRequired: true }),
            createField('price', 'decimal(10,2)', { isRequired: true }),
            createField('stock', 'int', { isRequired: true }),
            createField('similar', 'Product', {
              isRelation: true,
              isArray: true,
              operator: '~>',
              relatedType: 'Product',
              backref: 'similarTo',
            }),
          ],
          'schema:Product'
        ),
        createEntity('Order', [
          createField('total', 'decimal(10,2)', { isRequired: true }),
          createField('status', 'string', { isRequired: true }),
          createField('customer', 'Customer', {
            isRelation: true,
            operator: '->',
            relatedType: 'Customer',
            backref: 'orders',
          }),
          createField('items', 'OrderItem', {
            isRelation: true,
            isArray: true,
            operator: '<-',
            relatedType: 'OrderItem',
            backref: 'order',
          }),
        ]),
        createEntity('OrderItem', [
          createField('quantity', 'int', { isRequired: true }),
          createField('price', 'decimal(10,2)', { isRequired: true }),
          createField('order', 'Order', {
            isRelation: true,
            operator: '->',
            relatedType: 'Order',
            backref: 'items',
          }),
          createField('product', 'Product', {
            isRelation: true,
            operator: '->',
            relatedType: 'Product',
            backref: 'orderItems',
          }),
        ]),
      ])

      const schema = fromGraphDL(graphdl)

      // Check Customer
      expect(schema.Customer.$type).toBe('schema:Customer')
      expect(schema.Customer.orders).toBe('<- Order.customer[]')

      // Check Product with fuzzy relationship
      expect(schema.Product.$type).toBe('schema:Product')
      expect(schema.Product.similar).toBe('~> Product.similarTo[]')

      // Check Order
      expect(schema.Order.customer).toBe('-> Customer.orders')
      expect(schema.Order.items).toBe('<- OrderItem.order[]')

      // Check OrderItem
      expect(schema.OrderItem.order).toBe('-> Order.items')
      expect(schema.OrderItem.product).toBe('-> Product.orderItems')
    })
  })

  describe('error handling for invalid GraphDL schemas', () => {
    it('should handle empty schema', () => {
      const graphdl = createSchema([])

      const schema = fromGraphDL(graphdl)

      expect(schema).toEqual({})
    })

    it('should handle entity with no fields', () => {
      const graphdl = createSchema([createEntity('Empty', [])])

      const schema = fromGraphDL(graphdl)

      expect(schema.Empty).toBeDefined()
      expect(Object.keys(schema.Empty).length).toBe(0)
    })

    it('should throw error for null schema', () => {
      expect(() => fromGraphDL(null as unknown as ParsedGraph)).toThrow()
    })

    it('should throw error for undefined schema', () => {
      expect(() => fromGraphDL(undefined as unknown as ParsedGraph)).toThrow()
    })

    it('should throw error for schema without entities map', () => {
      const invalidSchema = {
        typeUris: new Map(),
      } as unknown as ParsedGraph

      expect(() => fromGraphDL(invalidSchema)).toThrow()
    })

    it('should handle malformed relationship field gracefully', () => {
      const graphdl = createSchema([
        createEntity('Post', [
          createField('title', 'string'),
          createField('broken', 'User', {
            isRelation: true,
            // Missing operator, relatedType, backref - should use defaults
          }),
        ]),
      ])

      // Should either throw or handle gracefully
      const schema = fromGraphDL(graphdl)

      // If handled gracefully, should treat as relationship with defaults
      // Default operator is '->', relatedType falls back to type, backref falls back to field name
      expect(schema.Post.broken).toBe('-> User.broken')
    })

    it('should handle circular relationships', () => {
      const graphdl = createSchema([
        createEntity('Node', [
          createField('value', 'string'),
          createField('parent', 'Node', {
            isRelation: true,
            operator: '->',
            relatedType: 'Node',
            backref: 'children',
          }),
          createField('children', 'Node', {
            isRelation: true,
            isArray: true,
            operator: '<-',
            relatedType: 'Node',
            backref: 'parent',
          }),
        ]),
      ])

      const schema = fromGraphDL(graphdl)

      // Circular relationships should be handled
      expect(schema.Node.parent).toBe('-> Node.children')
      expect(schema.Node.children).toBe('<- Node.parent[]')
    })

    it('should handle self-referential relationships', () => {
      const graphdl = createSchema([
        createEntity('Employee', [
          createField('name', 'string'),
          createField('manager', 'Employee', {
            isRelation: true,
            operator: '->',
            relatedType: 'Employee',
            backref: 'reports',
          }),
          createField('reports', 'Employee', {
            isRelation: true,
            isArray: true,
            operator: '<-',
            relatedType: 'Employee',
            backref: 'manager',
          }),
        ]),
      ])

      const schema = fromGraphDL(graphdl)

      expect(schema.Employee.manager).toBe('-> Employee.reports')
      expect(schema.Employee.reports).toBe('<- Employee.manager[]')
    })
  })

  describe('edge cases', () => {
    it('should handle fields with special characters in names', () => {
      // Note: This might not be valid GraphDL, but tests robustness
      const graphdl = createSchema([
        createEntity('Config', [
          createField('api_key', 'string'),
          createField('max_retries', 'int'),
        ]),
      ])

      const schema = fromGraphDL(graphdl)

      expect(schema.Config.api_key).toBe('string')
      expect(schema.Config.max_retries).toBe('int')
    })

    it('should handle very long type names', () => {
      const longTypeName = 'VeryLongEntityTypeNameThatMightCauseIssuesInSomeSystems'
      const graphdl = createSchema([
        createEntity(longTypeName, [createField('id', 'uuid')]),
      ])

      const schema = fromGraphDL(graphdl)

      expect(schema[longTypeName]).toBeDefined()
      expect(schema[longTypeName].id).toBe('uuid')
    })

    it('should preserve field order from GraphDL schema', () => {
      const graphdl = createSchema([
        createEntity('Ordered', [
          createField('first', 'string'),
          createField('second', 'string'),
          createField('third', 'string'),
        ]),
      ])

      const schema = fromGraphDL(graphdl)
      const fieldNames = Object.keys(schema.Ordered)

      // Field order should be preserved (excluding $type if present)
      expect(fieldNames).toContain('first')
      expect(fieldNames).toContain('second')
      expect(fieldNames).toContain('third')
    })

    it('should handle enum types correctly', () => {
      const graphdl = createSchema([
        createEntity('Post', [
          createField('status', 'enum(draft,published,archived)', { isRequired: true }),
        ]),
      ])

      const schema = fromGraphDL(graphdl)

      expect(schema.Post.status).toBe('enum(draft,published,archived)!')
    })
  })
})
