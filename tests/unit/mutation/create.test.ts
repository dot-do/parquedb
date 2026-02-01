/**
 * Create Operation Tests
 *
 * Tests for the create operation module.
 */

import { describe, it, expect } from 'vitest'
import {
  executeCreate,
  validateCreateInput,
  applySchemaDefaults,
  validateNamespace,
  normalizeNamespace,
  createMutationContext,
  MutationOperationError,
  MutationErrorCodes,
} from '../../../src/mutation'
import type { Schema, EntityId } from '../../../src/types'

// =============================================================================
// Namespace Validation Tests
// =============================================================================

describe('validateNamespace', () => {
  it('accepts valid namespaces', () => {
    expect(() => validateNamespace('posts')).not.toThrow()
    expect(() => validateNamespace('user-profiles')).not.toThrow()
    expect(() => validateNamespace('MyCollection')).not.toThrow()
  })

  it('rejects empty namespaces', () => {
    expect(() => validateNamespace('')).toThrow()
    expect(() => validateNamespace(null as any)).toThrow()
    expect(() => validateNamespace(undefined as any)).toThrow()
  })

  it('rejects namespaces with slashes', () => {
    expect(() => validateNamespace('posts/data')).toThrow(/cannot contain/)
  })

  it('rejects namespaces starting with underscore', () => {
    expect(() => validateNamespace('_internal')).toThrow(/cannot start with underscore/)
  })

  it('rejects namespaces starting with dollar sign', () => {
    expect(() => validateNamespace('$system')).toThrow(/cannot start with dollar sign/)
  })
})

describe('normalizeNamespace', () => {
  it('converts to lowercase', () => {
    expect(normalizeNamespace('Posts')).toBe('posts')
    expect(normalizeNamespace('BlogPosts')).toBe('blogposts')
    expect(normalizeNamespace('UserProfiles')).toBe('userprofiles')
  })

  it('handles already lowercase', () => {
    expect(normalizeNamespace('posts')).toBe('posts')
  })
})

// =============================================================================
// Create Input Validation Tests
// =============================================================================

describe('validateCreateInput', () => {
  const context = createMutationContext('posts', {
    actor: 'users/test' as EntityId,
    skipValidation: false,
  })

  it('accepts valid input', () => {
    expect(() =>
      validateCreateInput({ $type: 'Post', name: 'Test Post' }, context)
    ).not.toThrow()
  })

  it('throws MutationOperationError for missing $type', () => {
    try {
      validateCreateInput({ name: 'No Type' } as any, context)
      expect.fail('Should have thrown')
    } catch (error) {
      expect(error).toBeInstanceOf(MutationOperationError)
      expect((error as MutationOperationError).code).toBe(MutationErrorCodes.VALIDATION_FAILED)
    }
  })

  it('throws MutationOperationError for missing name', () => {
    try {
      validateCreateInput({ $type: 'Post' } as any, context)
      expect.fail('Should have thrown')
    } catch (error) {
      expect(error).toBeInstanceOf(MutationOperationError)
      expect((error as MutationOperationError).code).toBe(MutationErrorCodes.VALIDATION_FAILED)
    }
  })

  it('throws for non-string $type', () => {
    expect(() =>
      validateCreateInput({ $type: 123, name: 'Test' } as any, context)
    ).toThrow()
  })

  it('throws for non-string name', () => {
    expect(() =>
      validateCreateInput({ $type: 'Post', name: 123 } as any, context)
    ).toThrow()
  })
})

// =============================================================================
// Schema Defaults Tests
// =============================================================================

describe('applySchemaDefaults', () => {
  const schema: Schema = {
    Post: {
      $ns: 'posts',
      title: 'string!',
      status: 'string = "draft"',
      viewCount: 'number = 0',
      featured: 'boolean = false',
    },
    Article: {
      $ns: 'articles',
      title: 'string!',
      metadata: {
        type: 'object',
        default: { views: 0, likes: 0 },
      },
    },
  }

  it('applies string defaults', () => {
    const result = applySchemaDefaults(
      { $type: 'Post', name: 'Test' },
      schema
    )
    expect(result.status).toBe('draft')
  })

  it('applies number defaults', () => {
    const result = applySchemaDefaults(
      { $type: 'Post', name: 'Test' },
      schema
    )
    expect(result.viewCount).toBe(0)
  })

  it('applies boolean defaults', () => {
    const result = applySchemaDefaults(
      { $type: 'Post', name: 'Test' },
      schema
    )
    expect(result.featured).toBe(false)
  })

  it('applies object defaults', () => {
    const result = applySchemaDefaults(
      { $type: 'Article', name: 'Test' },
      schema
    )
    expect(result.metadata).toEqual({ views: 0, likes: 0 })
  })

  it('does not override existing values', () => {
    const result = applySchemaDefaults(
      { $type: 'Post', name: 'Test', status: 'published' },
      schema
    )
    expect(result.status).toBe('published')
  })

  it('returns input unchanged for unknown type', () => {
    const input = { $type: 'Unknown', name: 'Test', custom: 'value' }
    const result = applySchemaDefaults(input, schema)
    expect(result).toEqual(input)
  })

  it('returns input unchanged for missing $type', () => {
    const input = { name: 'Test' } as any
    const result = applySchemaDefaults(input, schema)
    expect(result).toEqual(input)
  })
})

// =============================================================================
// Execute Create Tests
// =============================================================================

describe('executeCreate', () => {
  const context = createMutationContext('posts', {
    actor: 'users/creator' as EntityId,
    timestamp: new Date('2024-01-01T00:00:00Z'),
  })

  it('creates entity with all required fields', () => {
    const result = executeCreate(context, {
      $type: 'Post',
      name: 'Test Post',
      title: 'Hello World',
    })

    expect(result.entity.$id).toMatch(/^posts\//)
    expect(result.entity.$type).toBe('Post')
    expect(result.entity.name).toBe('Test Post')
    expect(result.entity.title).toBe('Hello World')
    expect(result.entity.version).toBe(1)
    expect(result.entity.createdAt).toEqual(context.timestamp)
    expect(result.entity.createdBy).toBe('users/creator')
    expect(result.entity.updatedAt).toEqual(context.timestamp)
    expect(result.entity.updatedBy).toBe('users/creator')
  })

  it('generates entityId', () => {
    const result = executeCreate(context, {
      $type: 'Post',
      name: 'Test Post',
    })

    expect(result.entityId).toBe(result.entity.$id)
    expect(result.entityId).toMatch(/^posts\//)
  })

  it('generates CREATE event', () => {
    const result = executeCreate(context, {
      $type: 'Post',
      name: 'Test Post',
    })

    expect(result.events).toHaveLength(1)
    expect(result.events[0].op).toBe('CREATE')
    expect(result.events[0].target).toMatch(/^posts:/)
    expect(result.events[0].before).toBeNull()
    expect(result.events[0].after).toBeDefined()
    expect(result.events[0].actor).toBe('users/creator')
  })

  it('uses custom ID generator', () => {
    let counter = 0
    const result = executeCreate(context, {
      $type: 'Post',
      name: 'Test Post',
    }, {
      generateId: () => `custom-${++counter}`,
    })

    expect(result.entity.$id).toBe('posts/custom-1')
  })

  it('applies schema defaults', () => {
    const schema: Schema = {
      Post: {
        $ns: 'posts',
        status: 'string = "draft"',
      },
    }

    const result = executeCreate(context, {
      $type: 'Post',
      name: 'Test Post',
    }, { schema })

    expect(result.entity.status).toBe('draft')
  })

  it('preserves additional data fields', () => {
    const result = executeCreate(context, {
      $type: 'Post',
      name: 'Test Post',
      title: 'Hello',
      content: 'World',
      tags: ['a', 'b'],
      metadata: { views: 100 },
    })

    expect(result.entity.title).toBe('Hello')
    expect(result.entity.content).toBe('World')
    expect(result.entity.tags).toEqual(['a', 'b'])
    expect(result.entity.metadata).toEqual({ views: 100 })
  })

  it('skips validation when configured', () => {
    const skipContext = createMutationContext('posts', {
      actor: 'users/test' as EntityId,
      skipValidation: true,
    })

    // Should not throw even with missing required fields
    expect(() =>
      executeCreate(skipContext, {
        $type: 'Post',
        // Missing name
      } as any)
    ).not.toThrow()
  })
})
