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

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'

// Import actual types from ai-database package
import {
  type DBProvider as AIDBProvider,
  type DBEvent as AIDBEvent,
  type DBAction as AIDBAction,
  type DBArtifact as AIArtifact,
  type ListOptions as AIListOptions,
  type SearchOptions as AISearchOptions,
  type EmbeddingsConfig as AIEmbeddingsConfig,
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
    bio: 'text?',
  },
  Post: {
    $type: 'schema:BlogPosting',
    $ns: 'posts',
    name: 'string!',
    title: 'string!',
    content: 'text?',
    published: 'boolean?',
    author: '-> User.posts',
  },
  Article: {
    $type: 'schema:Article',
    $ns: 'articles',
    name: 'string!',
    title: 'string!',
    content: 'text?',
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
      expect(typeof adapter.replayEvents).toBe('function')
      expect(typeof adapter.createAction).toBe('function')
      expect(typeof adapter.getAction).toBe('function')
      expect(typeof adapter.updateAction).toBe('function')
      expect(typeof adapter.listActions).toBe('function')
      expect(typeof adapter.retryAction).toBe('function')
      expect(typeof adapter.cancelAction).toBe('function')
      expect(typeof adapter.getArtifact).toBe('function')
      expect(typeof adapter.setArtifact).toBe('function')
      expect(typeof adapter.deleteArtifact).toBe('function')
      expect(typeof adapter.listArtifacts).toBe('function')
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

      // Verify return structure matches ai-database expectations
      expect(user.$id).toBeDefined()
      expect(user.$type).toBe('User')
      expect(user.name).toBe('Alice')
      expect(user.email).toBe('alice@example.com')
      expect(user.age).toBe(30)
    })

    it('should get entities with correct return type', async () => {
      const aiProvider: AIDBProvider = adapter

      const created = await aiProvider.create('User', undefined, {
        name: 'Bob',
        email: 'bob@example.com',
      })

      const retrieved = await aiProvider.get('User', created.$id as string)

      expect(retrieved).not.toBeNull()
      expect(retrieved!.$id).toBe(created.$id)
      expect(retrieved!.name).toBe('Bob')
    })

    it('should list entities with ListOptions', async () => {
      const aiProvider: AIDBProvider = adapter

      await aiProvider.create('User', undefined, { name: 'Alice', age: 25 })
      await aiProvider.create('User', undefined, { name: 'Bob', age: 30 })
      await aiProvider.create('User', undefined, { name: 'Charlie', age: 35 })

      // Use ai-database ListOptions type
      const options: AIListOptions = {
        where: { age: { $gt: 28 } },
        orderBy: 'age',
        order: 'asc',
        limit: 2,
      }

      const users = await aiProvider.list('User', options)

      expect(users.length).toBe(2)
      expect(users[0]!.name).toBe('Bob')
      expect(users[1]!.name).toBe('Charlie')
    })

    it('should search entities with SearchOptions', async () => {
      const aiProvider: AIDBProvider = adapter

      await aiProvider.create('Article', undefined, {
        title: 'TypeScript Tutorial',
        content: 'Learn TypeScript basics',
      })
      await aiProvider.create('Article', undefined, {
        title: 'React Guide',
        content: 'Master React development',
      })

      const options: AISearchOptions = {
        limit: 10,
      }

      const results = await aiProvider.search('Article', 'TypeScript', options)

      expect(Array.isArray(results)).toBe(true)
    })

    it('should update entities correctly', async () => {
      const aiProvider: AIDBProvider = adapter

      const user = await aiProvider.create('User', undefined, {
        name: 'Alice',
        age: 25,
      })

      const updated = await aiProvider.update('User', user.$id as string, {
        age: 26,
      })

      expect(updated.age).toBe(26)
      expect(updated.name).toBe('Alice') // Unchanged fields preserved
    })

    it('should delete entities correctly', async () => {
      const aiProvider: AIDBProvider = adapter

      const user = await aiProvider.create('User', undefined, { name: 'ToDelete' })

      const deleted = await aiProvider.delete('User', user.$id as string)
      expect(deleted).toBe(true)

      const retrieved = await aiProvider.get('User', user.$id as string)
      expect(retrieved).toBeNull()
    })
  })

  describe('Relationship Operations Semantic Parity', () => {
    it('should create and query relationships', async () => {
      const aiProvider: AIDBProvider = adapter

      const user = await aiProvider.create('User', undefined, { name: 'Author' })
      const post = await aiProvider.create('Post', undefined, {
        title: 'My Post',
        content: 'Content here',
      })

      // Create relationship with metadata (ai-database signature)
      await aiProvider.relate(
        'Post',
        post.$id as string,
        'author',
        'User',
        user.$id as string,
        { matchMode: 'exact', similarity: 1.0 }
      )

      // Query related entities
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

  describe('Extended API Semantic Parity', () => {
    describe('Embeddings Configuration', () => {
      it('should accept embeddings configuration', () => {
        // The ParqueDB adapter accepts a similar but not identical config structure
        adapter.setEmbeddingsConfig({
          fields: {
            Article: ['title', 'content'],
          },
        })

        expect(adapter.getEmbeddingsConfig()).not.toBeNull()
      })
    })

    describe('Events API', () => {
      it('should emit events with correct return structure', async () => {
        const event = await adapter.emit({
          actor: 'user/123',
          event: 'user.created',
          object: 'users/456',
          objectData: { name: 'Test' },
        })

        // Verify return structure has expected properties
        expect(event.id).toBeDefined()
        expect(event.actor).toBe('user/123')
        expect(event.event).toBe('user.created')
        expect(event.timestamp).toBeInstanceOf(Date)
      })

      it('should list events', async () => {
        await adapter.emit({ actor: 'system', event: 'test.event' })

        const events = await adapter.listEvents({ event: 'test.event' })

        expect(events.length).toBeGreaterThanOrEqual(1)
        expect(events[0]!.event).toBe('test.event')
      })

      it('should subscribe to events with pattern matching', async () => {
        const received: Array<{ event: string }> = []

        const unsubscribe = adapter.on('test.*', (event) => {
          received.push(event)
        })

        await adapter.emit({ actor: 'system', event: 'test.one' })
        await adapter.emit({ actor: 'system', event: 'test.two' })
        await adapter.emit({ actor: 'system', event: 'other.event' })

        expect(received.length).toBe(2)

        unsubscribe()
      })
    })

    describe('Actions API', () => {
      it('should create actions with correct return structure', async () => {
        const action = await adapter.createAction({
          actor: 'system',
          action: 'process',
          total: 100,
        })

        expect(action.id).toBeDefined()
        expect(action.actor).toBe('system')
        expect(action.action).toBe('process')
        expect(action.status).toBe('pending')
        expect(action.activity).toBe('processing') // Verb conjugation
      })

      it('should update action status', async () => {
        const action = await adapter.createAction({
          actor: 'system',
          action: 'export',
        })

        const updated = await adapter.updateAction(action.id, {
          status: 'active',
          progress: 50,
        })

        expect(updated.status).toBe('active')
        expect(updated.progress).toBe(50)
        expect(updated.startedAt).toBeDefined()
      })

      it('should list actions with filters', async () => {
        await adapter.createAction({ actor: 'alice', action: 'import' })
        await adapter.createAction({ actor: 'bob', action: 'export' })

        const actions = await adapter.listActions({ actor: 'alice' })

        expect(actions.every(a => a.actor === 'alice')).toBe(true)
      })

      it('should retry failed actions', async () => {
        const action = await adapter.createAction({
          actor: 'system',
          action: 'sync',
        })

        await adapter.updateAction(action.id, {
          status: 'failed',
          error: 'Connection error',
        })

        const retried = await adapter.retryAction(action.id)

        expect(retried.status).toBe('pending')
        expect(retried.error).toBeUndefined()
      })

      it('should cancel pending actions', async () => {
        const action = await adapter.createAction({
          actor: 'system',
          action: 'process',
        })

        await adapter.cancelAction(action.id)

        const cancelled = await adapter.getAction(action.id)
        expect(cancelled!.status).toBe('cancelled')
      })
    })

    describe('Artifacts API', () => {
      it('should store and retrieve artifacts', async () => {
        const url = 'https://example.com/doc.pdf'
        const type = 'extraction'

        await adapter.setArtifact(url, type, {
          content: { text: 'Extracted content', pages: 5 },
          sourceHash: 'abc123',
          metadata: { model: 'pdf-parser' },
        })

        const artifact = await adapter.getArtifact(url, type)

        expect(artifact).not.toBeNull()
        expect(artifact!.url).toBe(url)
        expect(artifact!.type).toBe(type)
        expect(artifact!.sourceHash).toBe('abc123')
        expect((artifact!.content as Record<string, unknown>).text).toBe('Extracted content')
      })

      it('should list artifacts for a URL', async () => {
        const url = 'https://example.com/multi.pdf'

        await adapter.setArtifact(url, 'text', { content: 'Text', sourceHash: 'a' })
        await adapter.setArtifact(url, 'summary', { content: 'Summary', sourceHash: 'b' })

        const artifacts = await adapter.listArtifacts(url)

        expect(artifacts.length).toBe(2)
        expect(artifacts.map(a => a.type).sort()).toEqual(['summary', 'text'])
      })

      it('should delete artifacts', async () => {
        const url = 'https://example.com/temp.txt'

        await adapter.setArtifact(url, 'parsed', { content: 'Content', sourceHash: 'x' })
        await adapter.deleteArtifact(url, 'parsed')

        const artifact = await adapter.getArtifact(url, 'parsed')
        expect(artifact).toBeNull()
      })
    })

    describe('Semantic Search (with mock provider)', () => {
      it('should perform semantic search with correct result structure', async () => {
        const mockEmbeddingProvider = {
          embed: vi.fn().mockResolvedValue([0.1, 0.2, 0.3]),
          embedBatch: vi.fn().mockResolvedValue([[0.1, 0.2, 0.3]]),
          dimensions: 3,
          model: 'mock-model',
        }

        const adapterWithEmbeddings = new ParqueDBAdapter(parquedb, {
          embeddingProvider: mockEmbeddingProvider,
        })

        adapterWithEmbeddings.setEmbeddingsConfig({
          fields: { Article: ['title', 'content'] },
        })

        await adapterWithEmbeddings.create('Article', undefined, {
          title: 'Machine Learning Guide',
          content: 'Neural networks and deep learning',
        })

        const results = await adapterWithEmbeddings.semanticSearch('Article', 'AI concepts', { limit: 5 })

        expect(Array.isArray(results)).toBe(true)
        // ParqueDB uses $id, $type, $score format
        for (const result of results) {
          expect(typeof result.$id).toBe('string')
          expect(typeof result.$type).toBe('string')
          expect(typeof result.$score).toBe('number')
        }
      })

      it('should perform hybrid search with correct result structure', async () => {
        const mockEmbeddingProvider = {
          embed: vi.fn().mockResolvedValue([0.1, 0.2, 0.3]),
          embedBatch: vi.fn().mockResolvedValue([[0.1, 0.2, 0.3]]),
          dimensions: 3,
          model: 'mock-model',
        }

        const adapterWithEmbeddings = new ParqueDBAdapter(parquedb, {
          embeddingProvider: mockEmbeddingProvider,
        })

        adapterWithEmbeddings.setEmbeddingsConfig({
          fields: { Article: ['title', 'content'] },
        })

        await adapterWithEmbeddings.create('Article', undefined, {
          title: 'Deep Learning Tutorial',
          content: 'Understanding neural networks',
        })

        const results = await adapterWithEmbeddings.hybridSearch('Article', 'deep learning', {
          limit: 5,
          ftsWeight: 0.5,
          semanticWeight: 0.5,
        })

        expect(Array.isArray(results)).toBe(true)
        // ParqueDB uses $rrfScore, $ftsRank, $semanticRank format
        for (const result of results) {
          expect(typeof result.$id).toBe('string')
          expect(typeof result.$type).toBe('string')
          expect(typeof result.$rrfScore).toBe('number')
          expect(typeof result.$ftsRank).toBe('number')
          expect(typeof result.$semanticRank).toBe('number')
        }
      })
    })
  })

  describe('Factory Function', () => {
    it('createParqueDBProvider should return ai-database compatible provider', () => {
      const provider = createParqueDBProvider(parquedb)

      // Should be assignable to ai-database DBProvider type
      const asAIProvider: AIDBProvider = provider

      expect(asAIProvider).toBeDefined()
      expect(typeof asAIProvider.get).toBe('function')
      expect(typeof asAIProvider.list).toBe('function')
      expect(typeof asAIProvider.create).toBe('function')
    })
  })

  describe('Interoperability with ai-database MemoryProvider', () => {
    it('should produce results with same structure as ai-database operations expect', async () => {
      const aiProvider: AIDBProvider = adapter

      // Create entity
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
      expect(list.length).toBeGreaterThan(0)

      // Each entity in list should have same structure
      for (const item of list) {
        expect(item).toHaveProperty('$id')
        expect(item).toHaveProperty('$type')
      }

      // Get should return single entity or null
      const retrieved = await aiProvider.get('User', entity.$id as string)
      expect(retrieved).not.toBeNull()
      expect(retrieved).toHaveProperty('$id', entity.$id)

      // Update should return updated entity
      const updated = await aiProvider.update('User', entity.$id as string, { name: 'Updated' })
      expect(updated).toHaveProperty('$id', entity.$id)
      expect(updated.name).toBe('Updated')

      // Delete should return boolean
      const deleted = await aiProvider.delete('User', entity.$id as string)
      expect(typeof deleted).toBe('boolean')
    })
  })
})
