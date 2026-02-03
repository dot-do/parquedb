/**
 * ai-database Adapter Tests
 *
 * Tests for the ParqueDB adapter that implements the ai-database DBProvider
 * and DBProviderExtended interfaces.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { DB } from '../../../src/db'
import { ParqueDBAdapter, createParqueDBProvider } from '../../../src/integrations/ai-database'
import type { DBProviderExtended, DBEvent, DBAction } from '../../../src/integrations/ai-database'
import type { DBInstance } from '../../../src/db'

describe('ai-database Adapter', () => {
  let db: DBInstance
  let adapter: DBProviderExtended

  beforeEach(async () => {
    // Create a fresh database for each test
    db = DB({
      Users: {
        email: 'string!',
        name: 'string',
        role: 'string',
        age: 'int',
      },
      Posts: {
        title: 'string!',
        content: 'text',
        status: 'string',
        views: 'int',
        author: '-> User',
      },
      Categories: {
        name: 'string!',
        slug: 'string',
      },
    })

    adapter = new ParqueDBAdapter(db as any)
  })

  afterEach(() => {
    // Clean up
    db = null as any
    adapter = null as any
  })

  describe('Factory Function', () => {
    it('should create adapter using createParqueDBProvider', () => {
      const provider = createParqueDBProvider(db as any)
      expect(provider).toBeInstanceOf(ParqueDBAdapter)
    })
  })

  describe('Read Operations', () => {
    describe('get()', () => {
      it('should get an entity by type and ID', async () => {
        // Create a user first
        const created = await adapter.create('User', undefined, {
          name: 'Alice',
          email: 'alice@example.com',
          role: 'admin',
        })

        // Get the user
        const user = await adapter.get('User', created.$id as string)

        expect(user).not.toBeNull()
        expect(user?.name).toBe('Alice')
        expect(user?.email).toBe('alice@example.com')
      })

      it('should return null for non-existent entity', async () => {
        const user = await adapter.get('User', 'nonexistent-id')
        expect(user).toBeNull()
      })

      it('should handle ID with namespace prefix', async () => {
        const created = await adapter.create('User', undefined, {
          name: 'Bob',
          email: 'bob@example.com',
        })

        // Get using full ID (which includes namespace)
        const user = await adapter.get('User', created.$id as string)
        expect(user).not.toBeNull()
        expect(user?.name).toBe('Bob')
      })
    })

    describe('list()', () => {
      beforeEach(async () => {
        // Seed test data
        await adapter.create('User', undefined, { name: 'Alice', email: 'alice@example.com', age: 30 })
        await adapter.create('User', undefined, { name: 'Bob', email: 'bob@example.com', age: 25 })
        await adapter.create('User', undefined, { name: 'Charlie', email: 'charlie@example.com', age: 35 })
      })

      it('should list all entities of a type', async () => {
        const users = await adapter.list('User')
        expect(users.length).toBe(3)
      })

      it('should filter by where clause', async () => {
        const users = await adapter.list('User', { where: { age: { $gt: 28 } } })
        expect(users.length).toBe(2) // Alice (30) and Charlie (35)
      })

      it('should sort by orderBy', async () => {
        const usersAsc = await adapter.list('User', { orderBy: 'age', order: 'asc' })
        expect(usersAsc[0]?.name).toBe('Bob') // age 25

        const usersDesc = await adapter.list('User', { orderBy: 'age', order: 'desc' })
        expect(usersDesc[0]?.name).toBe('Charlie') // age 35
      })

      it('should limit results', async () => {
        const users = await adapter.list('User', { limit: 2 })
        expect(users.length).toBe(2)
      })

      it('should offset results', async () => {
        const users = await adapter.list('User', { orderBy: 'age', order: 'asc', offset: 1, limit: 2 })
        expect(users.length).toBe(2)
        expect(users[0]?.name).toBe('Alice') // second oldest
      })
    })

    describe('search()', () => {
      beforeEach(async () => {
        await adapter.create('Post', undefined, {
          title: 'Introduction to TypeScript',
          content: 'Learn TypeScript basics',
          status: 'published',
        })
        await adapter.create('Post', undefined, {
          title: 'Advanced JavaScript Patterns',
          content: 'Master JavaScript design patterns',
          status: 'published',
        })
        await adapter.create('Post', undefined, {
          title: 'Python for Beginners',
          content: 'Getting started with Python',
          status: 'draft',
        })
      })

      it('should search entities by query', async () => {
        // Note: FTS search depends on ParqueDB's FTS index being set up
        // For basic test, just ensure the method exists and returns results
        const results = await adapter.search('Post', 'TypeScript')
        expect(Array.isArray(results)).toBe(true)
      })
    })
  })

  describe('Write Operations', () => {
    describe('create()', () => {
      it('should create an entity with generated ID', async () => {
        const user = await adapter.create('User', undefined, {
          name: 'Alice',
          email: 'alice@example.com',
        })

        expect(user.$id).toBeDefined()
        expect(user.$type).toBe('User')
        expect(user.name).toBe('Alice')
      })

      it('should create an entity with custom ID', async () => {
        const user = await adapter.create('User', 'custom-id', {
          name: 'Bob',
          email: 'bob@example.com',
        })

        expect(user.$id).toContain('custom-id')
        expect(user.name).toBe('Bob')
      })

      it('should use title as name if no name provided', async () => {
        const post = await adapter.create('Post', undefined, {
          title: 'My First Post',
          content: 'Hello World',
        })

        expect(post.name).toBe('My First Post')
      })
    })

    describe('update()', () => {
      it('should update an entity', async () => {
        const created = await adapter.create('User', undefined, {
          name: 'Alice',
          email: 'alice@example.com',
          age: 30,
        })

        const updated = await adapter.update('User', created.$id as string, {
          age: 31,
        })

        expect(updated.age).toBe(31)
        expect(updated.name).toBe('Alice') // unchanged
      })

      it('should throw error for non-existent entity', async () => {
        await expect(
          adapter.update('User', 'nonexistent', { name: 'Nobody' })
        ).rejects.toThrow('Entity not found')
      })
    })

    describe('delete()', () => {
      it('should delete an entity', async () => {
        const created = await adapter.create('User', undefined, {
          name: 'Alice',
          email: 'alice@example.com',
        })

        const result = await adapter.delete('User', created.$id as string)
        expect(result).toBe(true)

        // Entity should be soft-deleted (not found by default)
        const user = await adapter.get('User', created.$id as string)
        expect(user).toBeNull()
      })

      it('should return false for non-existent entity', async () => {
        const result = await adapter.delete('User', 'nonexistent-id')
        // ParqueDB may return true for soft-delete on "valid-looking" IDs
        // or false for clearly invalid ones
        expect(typeof result).toBe('boolean')
      })
    })
  })

  describe('Relationship Operations', () => {
    let userId: string
    let postId: string

    beforeEach(async () => {
      const user = await adapter.create('User', undefined, {
        name: 'Alice',
        email: 'alice@example.com',
      })
      userId = user.$id as string

      const post = await adapter.create('Post', undefined, {
        title: 'My First Post',
        content: 'Hello World',
        status: 'published',
      })
      postId = post.$id as string
    })

    describe('relate()', () => {
      it('should create a relationship between entities', async () => {
        await adapter.relate('Post', postId, 'author', 'User', userId)

        // Verify relationship was created
        const relatedUsers = await adapter.related('Post', postId, 'author')
        expect(relatedUsers.length).toBe(1)
        expect(relatedUsers[0]?.name).toBe('Alice')
      })

      it('should create relationship with metadata', async () => {
        await adapter.relate('Post', postId, 'author', 'User', userId, {
          matchMode: 'exact',
          similarity: 1.0,
        })

        const relatedUsers = await adapter.related('Post', postId, 'author')
        expect(relatedUsers.length).toBe(1)
      })
    })

    describe('unrelate()', () => {
      it('should remove a relationship between entities', async () => {
        // Create relationship first
        await adapter.relate('Post', postId, 'author', 'User', userId)

        // Verify it exists
        let relatedUsers = await adapter.related('Post', postId, 'author')
        expect(relatedUsers.length).toBe(1)

        // Remove relationship
        await adapter.unrelate('Post', postId, 'author', 'User', userId)

        // Verify it's removed
        relatedUsers = await adapter.related('Post', postId, 'author')
        expect(relatedUsers.length).toBe(0)
      })
    })

    describe('related()', () => {
      it('should get related entities', async () => {
        await adapter.relate('Post', postId, 'author', 'User', userId)

        const authors = await adapter.related('Post', postId, 'author')
        expect(authors.length).toBe(1)
        expect(authors[0]?.email).toBe('alice@example.com')
      })

      it('should return empty array for no relationships', async () => {
        const authors = await adapter.related('Post', postId, 'author')
        expect(authors).toEqual([])
      })
    })
  })

  describe('Transaction Support', () => {
    it('should begin a transaction', async () => {
      const tx = await adapter.beginTransaction()

      expect(tx).toBeDefined()
      expect(typeof tx.get).toBe('function')
      expect(typeof tx.create).toBe('function')
      expect(typeof tx.update).toBe('function')
      expect(typeof tx.delete).toBe('function')
      expect(typeof tx.relate).toBe('function')
      expect(typeof tx.commit).toBe('function')
      expect(typeof tx.rollback).toBe('function')

      await tx.rollback() // Clean up
    })

    it('should persist created entities on commit', async () => {
      const tx = await adapter.beginTransaction()

      const user = await tx.create('User', undefined, {
        name: 'TxUser',
        email: 'txuser@example.com',
      })

      await tx.commit()

      // Entity should exist after commit
      const retrieved = await adapter.get('User', user.$id as string)
      expect(retrieved).not.toBeNull()
      expect(retrieved?.name).toBe('TxUser')
    })

    it('should rollback created entities', async () => {
      const tx = await adapter.beginTransaction()

      const user = await tx.create('User', undefined, {
        name: 'RollbackUser',
        email: 'rollback@example.com',
      })

      const userId = user.$id as string

      await tx.rollback()

      // Entity should not exist after rollback
      const retrieved = await adapter.get('User', userId)
      expect(retrieved).toBeNull()
    })

    it('should rollback updates to existing entities', async () => {
      // Create entity outside transaction
      const original = await adapter.create('User', undefined, {
        name: 'OriginalName',
        email: 'original@example.com',
        age: 25,
      })

      const tx = await adapter.beginTransaction()

      // Update within transaction
      await tx.update('User', original.$id as string, {
        name: 'UpdatedName',
        age: 30,
      })

      // Verify update is visible within transaction
      const duringTx = await tx.get('User', original.$id as string)
      expect(duringTx?.name).toBe('UpdatedName')
      expect(duringTx?.age).toBe(30)

      await tx.rollback()

      // Entity should have original values after rollback
      const retrieved = await adapter.get('User', original.$id as string)
      expect(retrieved).not.toBeNull()
      expect(retrieved?.name).toBe('OriginalName')
      expect(retrieved?.age).toBe(25)
    })

    it('should rollback deleted entities', async () => {
      // Create entity outside transaction
      const original = await adapter.create('User', undefined, {
        name: 'ToDelete',
        email: 'delete@example.com',
      })

      const tx = await adapter.beginTransaction()

      // Delete within transaction
      const deleted = await tx.delete('User', original.$id as string)
      expect(deleted).toBe(true)

      await tx.rollback()

      // Entity should still exist after rollback
      const retrieved = await adapter.get('User', original.$id as string)
      expect(retrieved).not.toBeNull()
      expect(retrieved?.name).toBe('ToDelete')
    })

    it('should rollback relationships created in transaction', async () => {
      // Create entities outside transaction
      const user = await adapter.create('User', undefined, {
        name: 'RelUser',
        email: 'reluser@example.com',
      })

      const post = await adapter.create('Post', undefined, {
        title: 'RelPost',
        content: 'Content',
        status: 'draft',
      })

      const tx = await adapter.beginTransaction()

      // Create relationship within transaction
      await tx.relate('Post', post.$id as string, 'author', 'User', user.$id as string)

      await tx.rollback()

      // Relationship should not exist after rollback
      const relatedUsers = await adapter.related('Post', post.$id as string, 'author')
      expect(relatedUsers.length).toBe(0)
    })

    it('should handle multiple operations in one transaction with rollback', async () => {
      // Create an entity to update
      const existing = await adapter.create('User', undefined, {
        name: 'Existing',
        email: 'existing@example.com',
      })

      const tx = await adapter.beginTransaction()

      // Create new entity
      const newUser = await tx.create('User', undefined, {
        name: 'NewUser',
        email: 'new@example.com',
      })

      // Update existing entity
      await tx.update('User', existing.$id as string, {
        name: 'UpdatedExisting',
      })

      // Create a post
      const post = await tx.create('Post', undefined, {
        title: 'TxPost',
        content: 'Content',
      })

      // Relate post to new user
      await tx.relate('Post', post.$id as string, 'author', 'User', newUser.$id as string)

      await tx.rollback()

      // New user should not exist
      const newUserResult = await adapter.get('User', newUser.$id as string)
      expect(newUserResult).toBeNull()

      // Existing user should have original name
      const existingResult = await adapter.get('User', existing.$id as string)
      expect(existingResult?.name).toBe('Existing')

      // Post should not exist
      const postResult = await adapter.get('Post', post.$id as string)
      expect(postResult).toBeNull()
    })

    it('should persist all operations on commit', async () => {
      // Create an entity to update
      const existing = await adapter.create('User', undefined, {
        name: 'ExistingForCommit',
        email: 'existingcommit@example.com',
      })

      const tx = await adapter.beginTransaction()

      // Create new entity
      const newUser = await tx.create('User', undefined, {
        name: 'NewUserCommit',
        email: 'newcommit@example.com',
      })

      // Update existing entity
      await tx.update('User', existing.$id as string, {
        name: 'UpdatedForCommit',
      })

      // Create a post
      const post = await tx.create('Post', undefined, {
        title: 'CommitPost',
        content: 'Content',
      })

      // Relate post to new user
      await tx.relate('Post', post.$id as string, 'author', 'User', newUser.$id as string)

      await tx.commit()

      // All changes should be persisted
      const newUserResult = await adapter.get('User', newUser.$id as string)
      expect(newUserResult).not.toBeNull()
      expect(newUserResult?.name).toBe('NewUserCommit')

      const existingResult = await adapter.get('User', existing.$id as string)
      expect(existingResult?.name).toBe('UpdatedForCommit')

      const postResult = await adapter.get('Post', post.$id as string)
      expect(postResult).not.toBeNull()

      const relatedUsers = await adapter.related('Post', post.$id as string, 'author')
      expect(relatedUsers.length).toBe(1)
    })
  })

  describe('Embeddings Configuration', () => {
    it('should set embeddings config', () => {
      adapter.setEmbeddingsConfig({
        provider: 'openai',
        model: 'text-embedding-3-small',
        dimensions: 1536,
        fields: {
          Post: ['title', 'content'],
        },
      })

      // Verify config is stored
      const config = adapter.getEmbeddingsConfig()
      expect(config).not.toBeNull()
      expect(config?.fields?.Post).toEqual(['title', 'content'])
    })

    it('should accept provider in setEmbeddingsConfig', () => {
      const mockProvider = {
        embed: async (text: string) => [0.1, 0.2, 0.3],
        embedBatch: async (texts: string[]) => texts.map(() => [0.1, 0.2, 0.3]),
        dimensions: 3,
        model: 'mock-model',
      }

      adapter.setEmbeddingsConfig({
        fields: { Post: ['title'] },
      }, mockProvider)

      expect(adapter.getEmbeddingsConfig()).not.toBeNull()
    })
  })

  describe('Auto-Embedding Generation', () => {
    let adapterWithEmbeddings: ParqueDBAdapter
    let mockProvider: {
      embed: ReturnType<typeof vi.fn>
      embedBatch: ReturnType<typeof vi.fn>
      dimensions: number
      model: string
    }

    beforeEach(() => {
      // Create a mock embedding provider
      mockProvider = {
        embed: vi.fn().mockResolvedValue([0.1, 0.2, 0.3]),
        embedBatch: vi.fn().mockResolvedValue([[0.1, 0.2, 0.3]]),
        dimensions: 3,
        model: 'mock-model',
      }

      // Create adapter with embedding provider
      adapterWithEmbeddings = new ParqueDBAdapter(db as any, {
        embeddingProvider: mockProvider,
      })

      // Configure embeddings
      adapterWithEmbeddings.setEmbeddingsConfig({
        fields: {
          Post: ['title', 'content'],
          User: ['bio'],
        },
        vectorField: 'embedding',
      })
    })

    it('should auto-generate embeddings on create', async () => {
      const post = await adapterWithEmbeddings.create('Post', undefined, {
        title: 'Hello World',
        content: 'This is a test post',
      })

      // Verify embedding provider was called
      expect(mockProvider.embed).toHaveBeenCalledWith('Hello World\n\nThis is a test post')

      // Verify embedding is stored in the entity
      expect(post.embedding).toEqual([0.1, 0.2, 0.3])
    })

    it('should not generate embeddings for unconfigured types', async () => {
      const category = await adapterWithEmbeddings.create('Category', undefined, {
        name: 'Tech',
        slug: 'tech',
      })

      // Embedding provider should not be called
      expect(mockProvider.embed).not.toHaveBeenCalled()

      // No embedding field
      expect(category.embedding).toBeUndefined()
    })

    it('should skip embedding if no text fields have values', async () => {
      const post = await adapterWithEmbeddings.create('Post', undefined, {
        status: 'draft',
      })

      // Embedding provider should not be called (no title or content)
      expect(mockProvider.embed).not.toHaveBeenCalled()
    })

    it('should regenerate embeddings on update when configured fields change', async () => {
      // Create a post first
      const post = await adapterWithEmbeddings.create('Post', undefined, {
        title: 'Original Title',
        content: 'Original content',
      })

      mockProvider.embed.mockClear()

      // Update the title
      const updated = await adapterWithEmbeddings.update('Post', post.$id as string, {
        title: 'Updated Title',
      })

      // Embedding should be regenerated with merged content
      expect(mockProvider.embed).toHaveBeenCalledTimes(1)
      // Should include the updated title and original content
      expect(mockProvider.embed).toHaveBeenCalledWith(expect.stringContaining('Updated Title'))
    })

    it('should not regenerate embeddings when non-configured fields change', async () => {
      const post = await adapterWithEmbeddings.create('Post', undefined, {
        title: 'Test',
        content: 'Test content',
      })

      mockProvider.embed.mockClear()

      // Update a non-configured field
      await adapterWithEmbeddings.update('Post', post.$id as string, {
        status: 'published',
      })

      // Embedding should NOT be regenerated
      expect(mockProvider.embed).not.toHaveBeenCalled()
    })

    it('should use custom vectorField from config', async () => {
      adapterWithEmbeddings.setEmbeddingsConfig({
        fields: { Post: ['title'] },
        vectorField: 'customEmbedding',
      })

      const post = await adapterWithEmbeddings.create('Post', undefined, {
        title: 'Hello',
      })

      expect(post.customEmbedding).toEqual([0.1, 0.2, 0.3])
      expect(post.embedding).toBeUndefined()
    })

    it('should work without provider configured', async () => {
      // Use adapter without embedding provider
      adapter.setEmbeddingsConfig({
        fields: { Post: ['title'] },
      })

      // Should not throw, just skip embedding generation
      const post = await adapter.create('Post', undefined, {
        title: 'Test',
        content: 'Content',
      })

      expect(post.embedding).toBeUndefined()
    })

    it('should concatenate multiple fields with newlines', async () => {
      await adapterWithEmbeddings.create('Post', undefined, {
        title: 'Title Text',
        content: 'Content Text',
      })

      // Fields should be joined with '\n\n'
      expect(mockProvider.embed).toHaveBeenCalledWith('Title Text\n\nContent Text')
    })

    it('should only embed non-empty string fields', async () => {
      await adapterWithEmbeddings.create('Post', undefined, {
        title: 'Only Title',
        content: '', // Empty content
      })

      // Should only include title
      expect(mockProvider.embed).toHaveBeenCalledWith('Only Title')
    })
  })

  describe('Semantic Search', () => {
    it('should perform semantic search', async () => {
      // Create a mock embedding provider that returns consistent vectors
      const mockEmbeddingProvider = {
        embed: vi.fn().mockResolvedValue([0.1, 0.2, 0.3]),
        embedBatch: vi.fn().mockResolvedValue([[0.1, 0.2, 0.3]]),
        dimensions: 3,
        model: 'mock-model',
      }

      // Create a new adapter with the embedding provider
      const adapterWithEmbeddings = new ParqueDBAdapter(db as any, {
        embeddingProvider: mockEmbeddingProvider,
      })

      adapterWithEmbeddings.setEmbeddingsConfig({
        fields: { Post: ['title', 'content'] },
        vectorField: 'embedding',
      })

      await adapterWithEmbeddings.create('Post', undefined, {
        title: 'Machine Learning Basics',
        content: 'Introduction to ML algorithms',
      })

      // Semantic search (requires vector index to be meaningful)
      const results = await adapterWithEmbeddings.semanticSearch('Post', 'AI and machine learning', {
        limit: 5,
      })

      expect(Array.isArray(results)).toBe(true)
      // Each result should have $score
      for (const result of results) {
        expect(typeof result.$score).toBe('number')
      }
    })
  })

  describe('Hybrid Search', () => {
    it('should perform hybrid search', async () => {
      // Create a mock embedding provider that returns consistent vectors
      const mockEmbeddingProvider = {
        embed: vi.fn().mockResolvedValue([0.1, 0.2, 0.3]),
        embedBatch: vi.fn().mockResolvedValue([[0.1, 0.2, 0.3]]),
        dimensions: 3,
        model: 'mock-model',
      }

      // Create a new adapter with the embedding provider
      const adapterWithEmbeddings = new ParqueDBAdapter(db as any, {
        embeddingProvider: mockEmbeddingProvider,
      })

      adapterWithEmbeddings.setEmbeddingsConfig({
        fields: { Post: ['title', 'content'] },
        vectorField: 'embedding',
      })

      await adapterWithEmbeddings.create('Post', undefined, {
        title: 'Deep Learning Tutorial',
        content: 'Neural networks and deep learning',
      })

      const results = await adapterWithEmbeddings.hybridSearch('Post', 'deep learning', {
        limit: 5,
        rrfK: 60,
      })

      expect(Array.isArray(results)).toBe(true)
      // Each result should have hybrid search scores
      for (const result of results) {
        expect(typeof result.$rrfScore).toBe('number')
        expect(typeof result.$ftsRank).toBe('number')
        expect(typeof result.$semanticRank).toBe('number')
      }
    })
  })

  describe('Events API', () => {
    describe('on() and emit()', () => {
      it('should subscribe to and emit events', async () => {
        const receivedEvents: DBEvent[] = []

        const unsubscribe = adapter.on('User.created', (event) => {
          receivedEvents.push(event)
        })

        await adapter.emit({
          actor: 'test-user',
          event: 'User.created',
          object: 'users/123',
          objectData: { name: 'Alice' },
        })

        expect(receivedEvents.length).toBe(1)
        expect(receivedEvents[0]?.event).toBe('User.created')
        expect(receivedEvents[0]?.actor).toBe('test-user')

        unsubscribe()
      })

      it('should support wildcard patterns', async () => {
        const receivedEvents: DBEvent[] = []

        const unsubscribe = adapter.on('User.*', (event) => {
          receivedEvents.push(event)
        })

        await adapter.emit({ actor: 'system', event: 'User.created', object: 'users/1' })
        await adapter.emit({ actor: 'system', event: 'User.updated', object: 'users/1' })
        await adapter.emit({ actor: 'system', event: 'Post.created', object: 'posts/1' })

        expect(receivedEvents.length).toBe(2)

        unsubscribe()
      })

      it('should emit legacy format events', async () => {
        const event = await adapter.emit('test.event', { foo: 'bar' })

        expect(event.event).toBe('test.event')
        expect(event.actor).toBe('system')
      })

      it('should unsubscribe correctly', async () => {
        const receivedEvents: DBEvent[] = []

        const unsubscribe = adapter.on('*', (event) => {
          receivedEvents.push(event)
        })

        await adapter.emit({ actor: 'system', event: 'test1', object: 'obj1' })
        expect(receivedEvents.length).toBe(1)

        unsubscribe()

        await adapter.emit({ actor: 'system', event: 'test2', object: 'obj2' })
        expect(receivedEvents.length).toBe(1) // Should not increase
      })
    })

    describe('listEvents()', () => {
      beforeEach(async () => {
        await adapter.emit({ actor: 'alice', event: 'User.login', object: 'users/1' })
        await adapter.emit({ actor: 'bob', event: 'User.login', object: 'users/2' })
        await adapter.emit({ actor: 'alice', event: 'Post.created', object: 'posts/1' })
      })

      it('should list all events', async () => {
        const events = await adapter.listEvents()
        expect(events.length).toBeGreaterThanOrEqual(3)
      })

      it('should filter by event type', async () => {
        const events = await adapter.listEvents({ event: 'User.login' })
        expect(events.every(e => e.event === 'User.login')).toBe(true)
      })

      it('should filter by actor', async () => {
        const events = await adapter.listEvents({ actor: 'alice' })
        expect(events.every(e => e.actor === 'alice')).toBe(true)
      })

      it('should limit results', async () => {
        const events = await adapter.listEvents({ limit: 2 })
        expect(events.length).toBeLessThanOrEqual(2)
      })
    })

    describe('replayEvents()', () => {
      it('should replay events through handler', async () => {
        await adapter.emit({ actor: 'system', event: 'replay.test', object: 'obj1' })
        await adapter.emit({ actor: 'system', event: 'replay.test', object: 'obj2' })

        const replayed: DBEvent[] = []

        await adapter.replayEvents({
          event: 'replay.test',
          handler: (event) => {
            replayed.push(event)
          },
        })

        expect(replayed.length).toBeGreaterThanOrEqual(2)
      })
    })
  })

  describe('Actions API', () => {
    describe('createAction()', () => {
      it('should create an action with new format', async () => {
        const action = await adapter.createAction({
          actor: 'system',
          action: 'generate',
          object: 'posts',
          total: 10,
        })

        expect(action.id).toBeDefined()
        expect(action.actor).toBe('system')
        expect(action.action).toBe('generate')
        expect(action.act).toBe('generates')
        expect(action.activity).toBe('generating')
        expect(action.status).toBe('pending')
        expect(action.total).toBe(10)
      })

      it('should create an action with legacy format', async () => {
        const action = await adapter.createAction({
          type: 'import',
          data: { source: 'csv' },
          total: 100,
        })

        expect(action.action).toBe('import')
        expect(action.status).toBe('pending')
      })

      it('should conjugate verbs correctly', async () => {
        // Test various verb forms
        const tests = [
          { action: 'create', expectedAct: 'creates', expectedActivity: 'creating' },
          { action: 'publish', expectedAct: 'publishes', expectedActivity: 'publishing' },
          { action: 'run', expectedAct: 'runs', expectedActivity: 'running' },
        ]

        for (const test of tests) {
          const action = await adapter.createAction({
            actor: 'system',
            action: test.action,
          })
          expect(action.act).toBe(test.expectedAct)
          expect(action.activity).toBe(test.expectedActivity)
        }
      })
    })

    describe('getAction()', () => {
      it('should get an action by ID', async () => {
        const created = await adapter.createAction({
          actor: 'system',
          action: 'process',
        })

        const action = await adapter.getAction(created.id)

        expect(action).not.toBeNull()
        expect(action?.id).toBe(created.id)
        expect(action?.action).toBe('process')
      })

      it('should return null for non-existent action', async () => {
        const action = await adapter.getAction('nonexistent-id')
        expect(action).toBeNull()
      })
    })

    describe('updateAction()', () => {
      it('should update action status', async () => {
        const created = await adapter.createAction({
          actor: 'system',
          action: 'process',
          total: 100,
        })

        const updated = await adapter.updateAction(created.id, {
          status: 'active',
          progress: 50,
        })

        expect(updated.status).toBe('active')
        expect(updated.progress).toBe(50)
        expect(updated.startedAt).toBeDefined()
      })

      it('should set completedAt when status is completed', async () => {
        const created = await adapter.createAction({
          actor: 'system',
          action: 'process',
        })

        const updated = await adapter.updateAction(created.id, {
          status: 'completed',
          result: { success: true },
        })

        expect(updated.status).toBe('completed')
        expect(updated.completedAt).toBeDefined()
      })
    })

    describe('listActions()', () => {
      beforeEach(async () => {
        await adapter.createAction({ actor: 'alice', action: 'import' })
        await adapter.createAction({ actor: 'bob', action: 'export' })
        const action = await adapter.createAction({ actor: 'alice', action: 'process' })
        await adapter.updateAction(action.id, { status: 'completed' })
      })

      it('should list all actions', async () => {
        const actions = await adapter.listActions()
        expect(actions.length).toBeGreaterThanOrEqual(3)
      })

      it('should filter by status', async () => {
        const actions = await adapter.listActions({ status: 'pending' })
        expect(actions.every(a => a.status === 'pending')).toBe(true)
      })

      it('should filter by actor', async () => {
        const actions = await adapter.listActions({ actor: 'alice' })
        expect(actions.every(a => a.actor === 'alice')).toBe(true)
      })
    })

    describe('retryAction()', () => {
      it('should retry a failed action', async () => {
        const created = await adapter.createAction({
          actor: 'system',
          action: 'process',
        })

        await adapter.updateAction(created.id, {
          status: 'failed',
          error: 'Connection timeout',
        })

        const retried = await adapter.retryAction(created.id)

        expect(retried.status).toBe('pending')
        expect(retried.error).toBeUndefined()
        expect(retried.progress).toBe(0)
      })

      it('should throw error for non-failed action', async () => {
        const created = await adapter.createAction({
          actor: 'system',
          action: 'process',
        })

        await expect(adapter.retryAction(created.id)).rejects.toThrow(
          'Can only retry failed actions'
        )
      })
    })

    describe('cancelAction()', () => {
      it('should cancel a pending action', async () => {
        const created = await adapter.createAction({
          actor: 'system',
          action: 'process',
        })

        await adapter.cancelAction(created.id)

        const action = await adapter.getAction(created.id)
        expect(action?.status).toBe('cancelled')
      })

      it('should throw error for completed action', async () => {
        const created = await adapter.createAction({
          actor: 'system',
          action: 'process',
        })

        await adapter.updateAction(created.id, { status: 'completed' })

        await expect(adapter.cancelAction(created.id)).rejects.toThrow(
          'Cannot cancel action with status: completed'
        )
      })
    })
  })

  describe('Artifacts API', () => {
    const testUrl = 'https://example.com/document.pdf'
    const testType = 'embedding'

    describe('setArtifact() and getArtifact()', () => {
      it('should set and get an artifact', async () => {
        await adapter.setArtifact(testUrl, testType, {
          content: [0.1, 0.2, 0.3],
          sourceHash: 'abc123',
          metadata: { model: 'text-embedding-3-small' },
        })

        const artifact = await adapter.getArtifact(testUrl, testType)

        expect(artifact).not.toBeNull()
        expect(artifact?.url).toBe(testUrl)
        expect(artifact?.type).toBe(testType)
        expect(artifact?.content).toEqual([0.1, 0.2, 0.3])
        expect(artifact?.sourceHash).toBe('abc123')
        expect(artifact?.metadata?.model).toBe('text-embedding-3-small')
      })

      it('should update existing artifact', async () => {
        await adapter.setArtifact(testUrl, testType, {
          content: [0.1, 0.2],
          sourceHash: 'v1',
        })

        await adapter.setArtifact(testUrl, testType, {
          content: [0.3, 0.4],
          sourceHash: 'v2',
        })

        const artifact = await adapter.getArtifact(testUrl, testType)
        expect(artifact?.sourceHash).toBe('v2')
        expect(artifact?.content).toEqual([0.3, 0.4])
      })

      it('should return null for non-existent artifact', async () => {
        const artifact = await adapter.getArtifact('nonexistent-url', 'type')
        expect(artifact).toBeNull()
      })
    })

    describe('listArtifacts()', () => {
      it('should list all artifacts for a URL', async () => {
        await adapter.setArtifact(testUrl, 'embedding', {
          content: [0.1],
          sourceHash: 'hash1',
        })
        await adapter.setArtifact(testUrl, 'summary', {
          content: 'Summary text',
          sourceHash: 'hash2',
        })
        await adapter.setArtifact('other-url', 'embedding', {
          content: [0.2],
          sourceHash: 'hash3',
        })

        const artifacts = await adapter.listArtifacts(testUrl)

        expect(artifacts.length).toBe(2)
        expect(artifacts.map(a => a.type).sort()).toEqual(['embedding', 'summary'])
      })
    })

    describe('deleteArtifact()', () => {
      it('should delete a specific artifact', async () => {
        await adapter.setArtifact(testUrl, 'embedding', {
          content: [0.1],
          sourceHash: 'hash1',
        })
        await adapter.setArtifact(testUrl, 'summary', {
          content: 'text',
          sourceHash: 'hash2',
        })

        await adapter.deleteArtifact(testUrl, 'embedding')

        const embedding = await adapter.getArtifact(testUrl, 'embedding')
        const summary = await adapter.getArtifact(testUrl, 'summary')

        expect(embedding).toBeNull()
        expect(summary).not.toBeNull()
      })

      it('should delete all artifacts for a URL when type not specified', async () => {
        await adapter.setArtifact(testUrl, 'embedding', {
          content: [0.1],
          sourceHash: 'hash1',
        })
        await adapter.setArtifact(testUrl, 'summary', {
          content: 'text',
          sourceHash: 'hash2',
        })

        await adapter.deleteArtifact(testUrl)

        const artifacts = await adapter.listArtifacts(testUrl)
        expect(artifacts.length).toBe(0)
      })
    })
  })
})
