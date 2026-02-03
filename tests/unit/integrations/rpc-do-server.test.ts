/**
 * Tests for ParqueDB DurableRPC Server Wrapper
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  ParqueDBRPCWrapper,
  createParqueDBRPCWrapper,
  type ParqueDBDOInterface,
} from '../../../src/integrations/rpc-do'
import type { Entity, Relationship, CreateInput, UpdateInput } from '../../../src/types'

// =============================================================================
// Mock ParqueDBDO
// =============================================================================

function createMockParqueDBDO(): ParqueDBDOInterface & { mockData: Map<string, Entity> } {
  const mockData = new Map<string, Entity>()
  const mockRelationships: Relationship[] = []

  return {
    mockData,

    get: vi.fn(async (ns: string, id: string): Promise<Entity | null> => {
      const key = `${ns}/${id}`
      return mockData.get(key) ?? null
    }),

    create: vi.fn(async (ns: string, data: CreateInput): Promise<Entity> => {
      const id = `${Date.now()}`
      const entity: Entity = {
        $id: `${ns}/${id}` as any,
        $type: data.$type ?? 'Unknown',
        name: data.name ?? 'Unnamed',
        createdAt: new Date(),
        createdBy: 'system/anonymous' as any,
        updatedAt: new Date(),
        updatedBy: 'system/anonymous' as any,
        version: 1,
        ...data,
      }
      mockData.set(`${ns}/${id}`, entity)
      return entity
    }),

    createMany: vi.fn(async (ns: string, items: CreateInput[]): Promise<Entity[]> => {
      const entities: Entity[] = []
      for (const item of items) {
        const id = `${Date.now()}-${Math.random().toString(36).slice(2)}`
        const entity: Entity = {
          $id: `${ns}/${id}` as any,
          $type: item.$type ?? 'Unknown',
          name: item.name ?? 'Unnamed',
          createdAt: new Date(),
          createdBy: 'system/anonymous' as any,
          updatedAt: new Date(),
          updatedBy: 'system/anonymous' as any,
          version: 1,
          ...item,
        }
        mockData.set(`${ns}/${id}`, entity)
        entities.push(entity)
      }
      return entities
    }),

    update: vi.fn(async (ns: string, id: string, update: UpdateInput): Promise<Entity> => {
      const key = `${ns}/${id}`
      const existing = mockData.get(key)
      if (!existing) {
        throw new Error(`Entity ${key} not found`)
      }
      const updated: Entity = {
        ...existing,
        ...update.$set,
        updatedAt: new Date(),
        version: existing.version + 1,
      }
      mockData.set(key, updated)
      return updated
    }),

    delete: vi.fn(async (ns: string, id: string): Promise<boolean> => {
      const key = `${ns}/${id}`
      return mockData.delete(key)
    }),

    link: vi.fn(async (_fromId: string, _predicate: string, _toId: string): Promise<void> => {
      // No-op for testing
    }),

    unlink: vi.fn(async (_fromId: string, _predicate: string, _toId: string): Promise<void> => {
      // No-op for testing
    }),

    getRelationships: vi.fn(async (): Promise<Relationship[]> => {
      return mockRelationships
    }),
  }
}

// =============================================================================
// Tests
// =============================================================================

describe('ParqueDBRPCWrapper', () => {
  let mockDO: ReturnType<typeof createMockParqueDBDO>
  let wrapper: ParqueDBRPCWrapper

  beforeEach(() => {
    mockDO = createMockParqueDBDO()
    wrapper = new ParqueDBRPCWrapper(mockDO)
  })

  describe('createParqueDBRPCWrapper factory', () => {
    it('should create a wrapper instance', () => {
      const wrapper = createParqueDBRPCWrapper(mockDO)
      expect(wrapper).toBeInstanceOf(ParqueDBRPCWrapper)
    })

    it('should accept configuration options', () => {
      const wrapper = createParqueDBRPCWrapper(mockDO, {
        defaultActor: 'test-actor',
        debug: true,
      })
      expect(wrapper).toBeInstanceOf(ParqueDBRPCWrapper)
    })
  })

  describe('entity operations', () => {
    it('should get entity by namespace and id', async () => {
      // Pre-populate data
      const entity: Entity = {
        $id: 'posts/123' as any,
        $type: 'Post',
        name: 'Test Post',
        createdAt: new Date(),
        createdBy: 'system/anonymous' as any,
        updatedAt: new Date(),
        updatedBy: 'system/anonymous' as any,
        version: 1,
      }
      mockDO.mockData.set('posts/123', entity)

      const result = await wrapper.get('posts', '123')

      expect(result).toEqual(entity)
      expect(mockDO.get).toHaveBeenCalledWith('posts', '123')
    })

    it('should return null for non-existent entity', async () => {
      const result = await wrapper.get('posts', 'not-found')

      expect(result).toBeNull()
    })

    it('should create entity', async () => {
      const result = await wrapper.create('posts', {
        $type: 'Post',
        name: 'New Post',
        content: 'Hello World',
      })

      expect(result.$type).toBe('Post')
      expect(result.name).toBe('New Post')
      expect(mockDO.create).toHaveBeenCalledWith(
        'posts',
        expect.objectContaining({
          $type: 'Post',
          name: 'New Post',
        }),
        expect.any(Object)
      )
    })

    it('should create entity with custom actor', async () => {
      const result = await wrapper.create('posts', {
        $type: 'Post',
        name: 'New Post',
      }, { actor: 'users/admin' })

      expect(result).toBeDefined()
      expect(mockDO.create).toHaveBeenCalledWith(
        'posts',
        expect.any(Object),
        expect.objectContaining({ actor: 'users/admin' })
      )
    })

    it('should create many entities', async () => {
      const result = await wrapper.createMany('posts', [
        { $type: 'Post', name: 'Post 1' },
        { $type: 'Post', name: 'Post 2' },
      ])

      expect(result).toHaveLength(2)
      expect(mockDO.createMany).toHaveBeenCalledWith(
        'posts',
        expect.arrayContaining([
          expect.objectContaining({ name: 'Post 1' }),
          expect.objectContaining({ name: 'Post 2' }),
        ]),
        expect.any(Object)
      )
    })

    it('should update entity', async () => {
      // Pre-populate data
      const entity: Entity = {
        $id: 'posts/123' as any,
        $type: 'Post',
        name: 'Original',
        createdAt: new Date(),
        createdBy: 'system/anonymous' as any,
        updatedAt: new Date(),
        updatedBy: 'system/anonymous' as any,
        version: 1,
      }
      mockDO.mockData.set('posts/123', entity)

      const result = await wrapper.update('posts', '123', {
        $set: { name: 'Updated' },
      })

      expect(result.name).toBe('Updated')
      expect(result.version).toBe(2)
    })

    it('should update entity with optimistic concurrency', async () => {
      // Pre-populate data
      const entity: Entity = {
        $id: 'posts/123' as any,
        $type: 'Post',
        name: 'Original',
        createdAt: new Date(),
        createdBy: 'system/anonymous' as any,
        updatedAt: new Date(),
        updatedBy: 'system/anonymous' as any,
        version: 1,
      }
      mockDO.mockData.set('posts/123', entity)

      await wrapper.update('posts', '123', {
        $set: { name: 'Updated' },
      }, { expectedVersion: 1 })

      expect(mockDO.update).toHaveBeenCalledWith(
        'posts',
        '123',
        expect.any(Object),
        expect.objectContaining({ expectedVersion: 1 })
      )
    })

    it('should delete entity', async () => {
      // Pre-populate data
      mockDO.mockData.set('posts/123', {} as Entity)

      const result = await wrapper.delete('posts', '123')

      expect(result).toBe(true)
      expect(mockDO.delete).toHaveBeenCalledWith(
        'posts',
        '123',
        expect.any(Object)
      )
    })

    it('should hard delete entity', async () => {
      mockDO.mockData.set('posts/123', {} as Entity)

      await wrapper.delete('posts', '123', { hard: true })

      expect(mockDO.delete).toHaveBeenCalledWith(
        'posts',
        '123',
        expect.objectContaining({ hard: true })
      )
    })
  })

  describe('relationship operations', () => {
    it('should link entities', async () => {
      await wrapper.link('posts/123', 'author', 'users/456')

      expect(mockDO.link).toHaveBeenCalledWith(
        'posts/123',
        'author',
        'users/456',
        expect.any(Object)
      )
    })

    it('should link with match mode and similarity', async () => {
      await wrapper.link('posts/123', 'related', 'posts/456', {
        matchMode: 'fuzzy',
        similarity: 0.85,
      })

      expect(mockDO.link).toHaveBeenCalledWith(
        'posts/123',
        'related',
        'posts/456',
        expect.objectContaining({
          matchMode: 'fuzzy',
          similarity: 0.85,
        })
      )
    })

    it('should unlink entities', async () => {
      await wrapper.unlink('posts/123', 'author', 'users/456')

      expect(mockDO.unlink).toHaveBeenCalledWith(
        'posts/123',
        'author',
        'users/456',
        expect.any(Object)
      )
    })

    it('should get relationships', async () => {
      const result = await wrapper.getRelationships('posts', '123', 'author', 'outbound')

      expect(result).toBeInstanceOf(Array)
      expect(mockDO.getRelationships).toHaveBeenCalledWith(
        'posts',
        '123',
        'author',
        'outbound'
      )
    })
  })

  describe('batch operations', () => {
    it('should batch get entities', async () => {
      // Pre-populate data
      for (const id of ['1', '2', '3']) {
        mockDO.mockData.set(`posts/${id}`, {
          $id: `posts/${id}` as any,
          $type: 'Post',
          name: `Post ${id}`,
        } as Entity)
      }

      const result = await wrapper.batchGet('posts', ['1', '2', '3'])

      expect(result).toHaveLength(3)
      expect(result[0]?.$id).toBe('posts/1')
      expect(result[1]?.$id).toBe('posts/2')
      expect(result[2]?.$id).toBe('posts/3')
    })

    it('should handle not-found in batch get', async () => {
      mockDO.mockData.set('posts/1', {
        $id: 'posts/1' as any,
        $type: 'Post',
        name: 'Post 1',
      } as Entity)

      const result = await wrapper.batchGet('posts', ['1', 'not-found'])

      expect(result).toHaveLength(2)
      expect(result[0]?.$id).toBe('posts/1')
      expect(result[1]).toBeNull()
    })

    it('should batch get related entities', async () => {
      const result = await wrapper.batchGetRelated([
        { type: 'posts', id: '1', relation: 'author' },
        { type: 'posts', id: '2', relation: 'author' },
      ])

      expect(result).toHaveLength(2)
      expect(result[0]).toHaveProperty('items')
      expect(result[1]).toHaveProperty('items')
    })
  })

  describe('collection operations', () => {
    it('should get collection proxy', () => {
      const collection = wrapper.collection('posts')

      expect(collection).toBeDefined()
      expect(collection.find).toBeDefined()
      expect(collection.findOne).toBeDefined()
      expect(collection.get).toBeDefined()
      expect(collection.create).toBeDefined()
      expect(collection.update).toBeDefined()
      expect(collection.delete).toBeDefined()
      expect(collection.count).toBeDefined()
      expect(collection.exists).toBeDefined()
      expect(collection.getRelated).toBeDefined()
    })

    it('should cache collection proxies', () => {
      const collection1 = wrapper.collection('posts')
      const collection2 = wrapper.collection('posts')

      expect(collection1).toBe(collection2)
    })

    it('should get entity via collection', async () => {
      const entity: Entity = {
        $id: 'posts/123' as any,
        $type: 'Post',
        name: 'Test Post',
        createdAt: new Date(),
        createdBy: 'system/anonymous' as any,
        updatedAt: new Date(),
        updatedBy: 'system/anonymous' as any,
        version: 1,
      }
      mockDO.mockData.set('posts/123', entity)

      const collection = wrapper.collection('posts')
      const result = await collection.get('123')

      expect(result).toEqual(entity)
    })

    it('should create entity via collection', async () => {
      const collection = wrapper.collection('posts')
      const result = await collection.create({
        $type: 'Post',
        name: 'New Post',
      })

      expect(result.$type).toBe('Post')
      expect(result.name).toBe('New Post')
    })

    it('should update entity via collection', async () => {
      const entity: Entity = {
        $id: 'posts/123' as any,
        $type: 'Post',
        name: 'Original',
        createdAt: new Date(),
        createdBy: 'system/anonymous' as any,
        updatedAt: new Date(),
        updatedBy: 'system/anonymous' as any,
        version: 1,
      }
      mockDO.mockData.set('posts/123', entity)

      const collection = wrapper.collection('posts')
      const result = await collection.update('123', { $set: { name: 'Updated' } })

      expect(result?.name).toBe('Updated')
    })

    it('should return null when updating non-existent entity', async () => {
      const collection = wrapper.collection('posts')
      const result = await collection.update('not-found', { $set: { name: 'Updated' } })

      expect(result).toBeNull()
    })

    it('should delete entity via collection', async () => {
      mockDO.mockData.set('posts/123', {} as Entity)

      const collection = wrapper.collection('posts')
      const result = await collection.delete('123')

      expect(result.deleted).toBe(true)
    })

    it('should check entity existence via collection', async () => {
      mockDO.mockData.set('posts/123', {} as Entity)

      const collection = wrapper.collection('posts')

      expect(await collection.exists('123')).toBe(true)
      expect(await collection.exists('not-found')).toBe(false)
    })
  })

  describe('schema introspection', () => {
    it('should return schema', () => {
      const schema = wrapper.getSchema()

      expect(schema).toBeDefined()
      expect(schema.collections).toBeDefined()
      expect(schema.methods).toBeDefined()
    })

    it('should include all methods in schema', () => {
      const schema = wrapper.getSchema()

      expect(schema.methods.get).toBeDefined()
      expect(schema.methods.create).toBeDefined()
      expect(schema.methods.createMany).toBeDefined()
      expect(schema.methods.update).toBeDefined()
      expect(schema.methods.delete).toBeDefined()
      expect(schema.methods.link).toBeDefined()
      expect(schema.methods.unlink).toBeDefined()
      expect(schema.methods.getRelationships).toBeDefined()
      expect(schema.methods.batchGet).toBeDefined()
      expect(schema.methods.batchGetRelated).toBeDefined()
    })

    it('should include method metadata', () => {
      const schema = wrapper.getSchema()

      expect(schema.methods.get.params).toEqual(['ns', 'id'])
      expect(schema.methods.get.returns).toBe('Entity | null')

      expect(schema.methods.create.params).toEqual(['ns', 'data', 'options?'])
      expect(schema.methods.create.returns).toBe('Entity')
    })
  })

  describe('configuration', () => {
    it('should use default actor from config', async () => {
      const wrapper = new ParqueDBRPCWrapper(mockDO, {
        defaultActor: 'users/admin',
      })

      await wrapper.create('posts', { $type: 'Post', name: 'Test' })

      expect(mockDO.create).toHaveBeenCalledWith(
        'posts',
        expect.any(Object),
        expect.objectContaining({ actor: 'users/admin' })
      )
    })

    it('should override default actor with explicit actor', async () => {
      const wrapper = new ParqueDBRPCWrapper(mockDO, {
        defaultActor: 'users/admin',
      })

      await wrapper.create('posts', { $type: 'Post', name: 'Test' }, {
        actor: 'users/other',
      })

      expect(mockDO.create).toHaveBeenCalledWith(
        'posts',
        expect.any(Object),
        expect.objectContaining({ actor: 'users/other' })
      )
    })
  })
})
