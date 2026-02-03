/**
 * ai-database Package Type Compatibility Tests
 *
 * This test verifies that the ParqueDB adapter is compatible with the
 * ai-database package interfaces and can be used as a provider.
 *
 * Key tests:
 * 1. Type compatibility with DBProvider interface
 * 2. Usage with ai-database's setProvider() function
 * 3. Semantic parity - same operations produce compatible results
 *
 * Note: The ai-database package and ParqueDB use similar but not identical
 * type definitions. This test verifies semantic compatibility rather than
 * strict type assignability for result types (like SemanticSearchResult).
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'

// Import actual types from ai-database package
import {
  type DBProvider as AIDBProvider,
  type ListOptions as AIListOptions,
  type SearchOptions as AISearchOptions,
  setProvider,
} from 'ai-database'

// Import ParqueDB implementation
import { ParqueDB } from '../../src/ParqueDB'
import { MemoryBackend } from '../../src/storage/MemoryBackend'
import {
  ParqueDBAdapter,
  createParqueDBProvider,
  type DBProviderExtended as ParqueDBProviderExtended,
} from '../../src/integrations/ai-database'
import type { Schema } from '../../src/types'

// =============================================================================
// Test Schemas
// =============================================================================

const TestSchema: Schema = {
  User: {
    $type: 'schema:Person',
    $ns: 'users',
    name: 'string!',
    email: 'email?',
    age: 'int?',
  },
  Post: {
    $type: 'schema:BlogPosting',
    $ns: 'posts',
    name: 'string!',
    title: 'string!',
    content: 'text?',
    author: '-> User.posts',
  },
}

// =============================================================================
// Type Compatibility Tests (Compile-Time Verification)
// =============================================================================

describe('ai-database Type Compatibility', () => {
  let parquedb: ParqueDB
  let adapter: ParqueDBProviderExtended

  beforeEach(async () => {
    parquedb = new ParqueDB({
      storage: new MemoryBackend(),
      schema: TestSchema,
    })
    adapter = createParqueDBProvider(parquedb)
  })

  afterEach(() => {
    parquedb.dispose()
  })

  describe('DBProvider Interface Compatibility', () => {
    it('should be assignable to ai-database DBProvider type', () => {
      // This is a compile-time type check - if this compiles, types are compatible
      const aiProvider: AIDBProvider = adapter

      // Verify all required methods exist
      expect(typeof aiProvider.get).toBe('function')
      expect(typeof aiProvider.list).toBe('function')
      expect(typeof aiProvider.search).toBe('function')
      expect(typeof aiProvider.create).toBe('function')
      expect(typeof aiProvider.update).toBe('function')
      expect(typeof aiProvider.delete).toBe('function')
      expect(typeof aiProvider.related).toBe('function')
      expect(typeof aiProvider.relate).toBe('function')
      expect(typeof aiProvider.unrelate).toBe('function')
    })

    it('should have extended provider methods', () => {
      // Verify extended methods exist on the ParqueDB adapter
      expect(typeof adapter.setEmbeddingsConfig).toBe('function')
      expect(typeof adapter.semanticSearch).toBe('function')
      expect(typeof adapter.hybridSearch).toBe('function')
      expect(typeof adapter.on).toBe('function')
      expect(typeof adapter.emit).toBe('function')
      expect(typeof adapter.listEvents).toBe('function')
      expect(typeof adapter.createAction).toBe('function')
      expect(typeof adapter.getArtifact).toBe('function')
    })
  })

  describe('setProvider() Integration', () => {
    it('should be usable with ai-database setProvider()', () => {
      // This should not throw - setProvider accepts DBProvider
      setProvider(adapter)

      // Provider was accepted without type errors
      expect(true).toBe(true)
    })
  })

  describe('CRUD Operations Semantic Parity', () => {
    it('should create entities with correct return type', async () => {
      const aiProvider: AIDBProvider = adapter

      const user = await aiProvider.create('User', undefined, {
        name: 'Alice',
        email: 'alice@example.com',
        age: 30,
      })

      expect(user.$id).toBeDefined()
      expect(user.$type).toBe('User')
      expect(user.name).toBe('Alice')
    })

    it('should get entities', async () => {
      const aiProvider: AIDBProvider = adapter

      const created = await aiProvider.create('User', undefined, {
        name: 'Bob',
        email: 'bob@example.com',
      })

      const retrieved = await aiProvider.get('User', created.$id as string)

      expect(retrieved).not.toBeNull()
      expect(retrieved!.$id).toBe(created.$id)
    })

    it('should list entities with ListOptions', async () => {
      const aiProvider: AIDBProvider = adapter

      await aiProvider.create('User', undefined, { name: 'Alice', age: 25 })
      await aiProvider.create('User', undefined, { name: 'Bob', age: 30 })

      const options: AIListOptions = {
        where: { age: { $gt: 28 } },
        limit: 2,
      }

      const users = await aiProvider.list('User', options)

      expect(users.length).toBe(1)
      expect(users[0]!.name).toBe('Bob')
    })

    it('should update entities', async () => {
      const aiProvider: AIDBProvider = adapter

      const user = await aiProvider.create('User', undefined, {
        name: 'Alice',
        age: 25,
      })

      const updated = await aiProvider.update('User', user.$id as string, {
        age: 26,
      })

      expect(updated.age).toBe(26)
    })

    it('should delete entities', async () => {
      const aiProvider: AIDBProvider = adapter

      const user = await aiProvider.create('User', undefined, { name: 'ToDelete' })

      const deleted = await aiProvider.delete('User', user.$id as string)
      expect(deleted).toBe(true)

      const retrieved = await aiProvider.get('User', user.$id as string)
      expect(retrieved).toBeNull()
    })
  })

  describe('Relationship Operations', () => {
    it('should create and query relationships', async () => {
      const aiProvider: AIDBProvider = adapter

      const user = await aiProvider.create('User', undefined, { name: 'Author' })
      const post = await aiProvider.create('Post', undefined, {
        title: 'My Post',
        content: 'Content here',
      })

      await aiProvider.relate(
        'Post',
        post.$id as string,
        'author',
        'User',
        user.$id as string,
        { matchMode: 'exact' }
      )

      const authors = await aiProvider.related('Post', post.$id as string, 'author')

      expect(authors.length).toBe(1)
      expect(authors[0]!.name).toBe('Author')
    })

    it('should remove relationships', async () => {
      const aiProvider: AIDBProvider = adapter

      const user = await aiProvider.create('User', undefined, { name: 'Author' })
      const post = await aiProvider.create('Post', undefined, { title: 'Post' })

      await aiProvider.relate('Post', post.$id as string, 'author', 'User', user.$id as string)

      let authors = await aiProvider.related('Post', post.$id as string, 'author')
      expect(authors.length).toBe(1)

      await aiProvider.unrelate('Post', post.$id as string, 'author', 'User', user.$id as string)

      authors = await aiProvider.related('Post', post.$id as string, 'author')
      expect(authors.length).toBe(0)
    })
  })

  describe('Extended API', () => {
    it('should emit events', async () => {
      const event = await adapter.emit({
        actor: 'user/123',
        event: 'user.created',
      })

      expect(event.id).toBeDefined()
      expect(event.event).toBe('user.created')
    })

    it('should create and manage actions', async () => {
      const action = await adapter.createAction({
        actor: 'system',
        action: 'process',
      })

      expect(action.id).toBeDefined()
      expect(action.status).toBe('pending')

      const updated = await adapter.updateAction(action.id, {
        status: 'active',
      })

      expect(updated.status).toBe('active')
    })

    it('should manage artifacts', async () => {
      const url = 'https://example.com/doc.pdf'

      await adapter.setArtifact(url, 'extraction', {
        content: { text: 'Content' },
        sourceHash: 'abc123',
      })

      const artifact = await adapter.getArtifact(url, 'extraction')

      expect(artifact).not.toBeNull()
      expect(artifact!.sourceHash).toBe('abc123')
    })
  })

  describe('Factory Function', () => {
    it('createParqueDBProvider should return ai-database compatible provider', () => {
      const provider = createParqueDBProvider(parquedb)

      const asAIProvider: AIDBProvider = provider

      expect(asAIProvider).toBeDefined()
      expect(typeof asAIProvider.get).toBe('function')
    })
  })

  describe('Entity Structure Compatibility', () => {
    it('should produce entities with ai-database expected structure', async () => {
      const aiProvider: AIDBProvider = adapter

      const entity = await aiProvider.create('User', undefined, {
        name: 'Test User',
        email: 'test@example.com',
      })

      // Verify entity has standard ai-database entity structure
      expect(entity).toHaveProperty('$id')
      expect(entity).toHaveProperty('$type')
      expect(entity).toHaveProperty('name')

      // List should return array of entities
      const list = await aiProvider.list('User')
      expect(Array.isArray(list)).toBe(true)

      // Each entity in list should have same structure
      for (const item of list) {
        expect(item).toHaveProperty('$id')
        expect(item).toHaveProperty('$type')
      }
    })
  })
})
