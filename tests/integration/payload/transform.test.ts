/**
 * Tests for Payload CMS document transformation
 */

import { describe, it, expect } from 'vitest'
import {
  toParqueDBInput,
  toParqueDBUpdate,
  toPayloadDoc,
  toPayloadDocs,
  extractLocalId,
  buildEntityId,
  buildPaginationInfo,
} from '../../../src/integrations/payload/transform'
import type { Entity, EntityId } from '../../../src/types'

describe('toParqueDBInput', () => {
  it('creates input with $type and name', () => {
    const result = toParqueDBInput(
      { title: 'Hello World', content: 'Test content' },
      { collection: 'posts' }
    )

    expect(result.$type).toBe('Posts')
    expect(result.name).toBe('Hello World')
    expect(result.title).toBe('Hello World')
    expect(result.content).toBe('Test content')
  })

  it('derives name from title field', () => {
    const result = toParqueDBInput(
      { title: 'My Post' },
      { collection: 'posts' }
    )
    expect(result.name).toBe('My Post')
  })

  it('derives name from name field', () => {
    const result = toParqueDBInput(
      { name: 'John Doe', email: 'john@example.com' },
      { collection: 'users' }
    )
    expect(result.name).toBe('John Doe')
  })

  it('derives name from email field as fallback', () => {
    const result = toParqueDBInput(
      { email: 'john@example.com' },
      { collection: 'users' }
    )
    expect(result.name).toBe('john@example.com')
  })

  it('excludes Payload internal fields', () => {
    const result = toParqueDBInput(
      {
        id: '123',
        title: 'Test',
        createdAt: '2024-01-01T00:00:00.000Z',
        updatedAt: '2024-01-01T00:00:00.000Z',
      },
      { collection: 'posts' }
    )

    expect(result).not.toHaveProperty('id')
    expect(result).not.toHaveProperty('createdAt')
    expect(result).not.toHaveProperty('updatedAt')
    expect(result.title).toBe('Test')
  })

  it('preserves _status field for drafts', () => {
    const result = toParqueDBInput(
      { title: 'Draft Post', _status: 'draft' },
      { collection: 'posts' }
    )

    expect(result._status).toBe('draft')
  })

  it('converts date strings to Date objects', () => {
    const result = toParqueDBInput(
      { title: 'Test', publishedAt: '2024-01-01T00:00:00.000Z' },
      { collection: 'posts' }
    )

    expect(result.publishedAt).toBeInstanceOf(Date)
  })
})

describe('toParqueDBUpdate', () => {
  it('creates $set operations for fields', () => {
    const result = toParqueDBUpdate(
      { title: 'Updated Title', content: 'New content' },
      { collection: 'posts' }
    )

    expect(result.$set).toEqual({
      title: 'Updated Title',
      content: 'New content',
    })
  })

  it('excludes id and audit fields from updates', () => {
    const result = toParqueDBUpdate(
      {
        id: '123',
        title: 'Updated',
        createdAt: '2024-01-01',
        updatedAt: '2024-01-02',
      },
      { collection: 'posts' }
    )

    expect(result.$set).toEqual({ title: 'Updated' })
  })
})

describe('toPayloadDoc', () => {
  const mockEntity: Entity = {
    $id: 'posts/abc123' as EntityId,
    $type: 'Post',
    name: 'Test Post',
    title: 'Test Post',
    content: 'This is content',
    createdAt: new Date('2024-01-01T00:00:00.000Z'),
    createdBy: 'users/admin' as EntityId,
    updatedAt: new Date('2024-01-02T00:00:00.000Z'),
    updatedBy: 'users/admin' as EntityId,
    version: 1,
  }

  it('extracts local ID from $id', () => {
    const result = toPayloadDoc(mockEntity, { collection: 'posts' })
    expect(result?.id).toBe('abc123')
  })

  it('converts dates to ISO strings', () => {
    const result = toPayloadDoc(mockEntity, { collection: 'posts' })
    expect(result?.createdAt).toBe('2024-01-01T00:00:00.000Z')
    expect(result?.updatedAt).toBe('2024-01-02T00:00:00.000Z')
  })

  it('excludes ParqueDB internal fields', () => {
    const result = toPayloadDoc(mockEntity, { collection: 'posts' })
    expect(result).not.toHaveProperty('$id')
    expect(result).not.toHaveProperty('$type')
    expect(result).not.toHaveProperty('createdBy')
    expect(result).not.toHaveProperty('updatedBy')
    expect(result).not.toHaveProperty('version')
  })

  it('includes data fields', () => {
    const result = toPayloadDoc(mockEntity, { collection: 'posts' })
    expect(result?.title).toBe('Test Post')
    expect(result?.content).toBe('This is content')
  })

  it('handles null input', () => {
    expect(toPayloadDoc(null, { collection: 'posts' })).toBeNull()
    expect(toPayloadDoc(undefined, { collection: 'posts' })).toBeNull()
  })

  it('applies field selection', () => {
    const result = toPayloadDoc(mockEntity, {
      collection: 'posts',
      select: { title: true },
    })
    expect(result?.title).toBe('Test Post')
    expect(result).not.toHaveProperty('content')
  })
})

describe('toPayloadDocs', () => {
  it('transforms array of entities', () => {
    const entities: Entity[] = [
      {
        $id: 'posts/1' as EntityId,
        $type: 'Post',
        name: 'Post 1',
        title: 'Post 1',
        createdAt: new Date(),
        createdBy: 'system/test' as EntityId,
        updatedAt: new Date(),
        updatedBy: 'system/test' as EntityId,
        version: 1,
      },
      {
        $id: 'posts/2' as EntityId,
        $type: 'Post',
        name: 'Post 2',
        title: 'Post 2',
        createdAt: new Date(),
        createdBy: 'system/test' as EntityId,
        updatedAt: new Date(),
        updatedBy: 'system/test' as EntityId,
        version: 1,
      },
    ]

    const result = toPayloadDocs(entities, { collection: 'posts' })

    expect(result).toHaveLength(2)
    expect(result[0]?.id).toBe('1')
    expect(result[1]?.id).toBe('2')
  })
})

describe('extractLocalId', () => {
  it('extracts ID from ns/id format', () => {
    expect(extractLocalId('posts/abc123' as EntityId)).toBe('abc123')
    expect(extractLocalId('users/user-1' as EntityId)).toBe('user-1')
  })

  it('returns original if no slash', () => {
    expect(extractLocalId('abc123')).toBe('abc123')
  })

  it('handles IDs with slashes', () => {
    expect(extractLocalId('posts/path/to/id' as EntityId)).toBe('path/to/id')
  })
})

describe('buildEntityId', () => {
  it('creates ns/id format', () => {
    const result = buildEntityId('posts', 'abc123')
    expect(result).toBe('posts/abc123')
  })
})

describe('buildPaginationInfo', () => {
  it('calculates pagination correctly', () => {
    const result = buildPaginationInfo(100, 10, 1)

    expect(result.totalDocs).toBe(100)
    expect(result.totalPages).toBe(10)
    expect(result.page).toBe(1)
    expect(result.limit).toBe(10)
    expect(result.hasNextPage).toBe(true)
    expect(result.hasPrevPage).toBe(false)
    expect(result.nextPage).toBe(2)
    expect(result.prevPage).toBeNull()
    expect(result.pagingCounter).toBe(1)
  })

  it('handles last page', () => {
    const result = buildPaginationInfo(100, 10, 10)

    expect(result.hasNextPage).toBe(false)
    expect(result.hasPrevPage).toBe(true)
    expect(result.nextPage).toBeNull()
    expect(result.prevPage).toBe(9)
    expect(result.pagingCounter).toBe(91)
  })

  it('handles middle page', () => {
    const result = buildPaginationInfo(100, 10, 5)

    expect(result.hasNextPage).toBe(true)
    expect(result.hasPrevPage).toBe(true)
    expect(result.nextPage).toBe(6)
    expect(result.prevPage).toBe(4)
    expect(result.pagingCounter).toBe(41)
  })

  it('handles single page', () => {
    const result = buildPaginationInfo(5, 10, 1)

    expect(result.totalPages).toBe(1)
    expect(result.hasNextPage).toBe(false)
    expect(result.hasPrevPage).toBe(false)
  })

  it('handles empty results', () => {
    const result = buildPaginationInfo(0, 10, 1)

    expect(result.totalDocs).toBe(0)
    expect(result.totalPages).toBe(1)
    expect(result.hasNextPage).toBe(false)
    expect(result.hasPrevPage).toBe(false)
  })
})
