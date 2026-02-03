/**
 * Tests for ai-database DBProvider Interface Adapter
 *
 * Tests the ParqueDB adapter for ai-database's DBProvider and DBProviderExtended interfaces,
 * covering CRUD operations, relationships, search, events, actions, and artifacts.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import {
  ParqueDBAdapter,
  createParqueDBProvider,
  type DBProvider,
  type DBProviderExtended,
  type DBEvent,
  type DBAction,
  type DBArtifact,
} from '../../../src/integrations/ai-database'
import { ParqueDB } from '../../../src/ParqueDB'

describe('ParqueDBAdapter', () => {
  let db: ParqueDB
  let adapter: ParqueDBAdapter

  beforeEach(async () => {
    const storage = new MemoryBackend()
    db = new ParqueDB({ storage })
    adapter = new ParqueDBAdapter(db)
  })

  describe('factory function', () => {
    it('should create adapter using createParqueDBProvider', () => {
      const storage = new MemoryBackend()
      const db = new ParqueDB({ storage })
      const provider = createParqueDBProvider(db)

      expect(provider).toBeInstanceOf(ParqueDBAdapter)
    })

    it('should return DBProviderExtended interface', () => {
      const storage = new MemoryBackend()
      const db = new ParqueDB({ storage })
      const provider: DBProviderExtended = createParqueDBProvider(db)

      // Verify all DBProvider methods exist
      expect(typeof provider.get).toBe('function')
      expect(typeof provider.list).toBe('function')
      expect(typeof provider.search).toBe('function')
      expect(typeof provider.create).toBe('function')
      expect(typeof provider.update).toBe('function')
      expect(typeof provider.delete).toBe('function')
      expect(typeof provider.related).toBe('function')
      expect(typeof provider.relate).toBe('function')
      expect(typeof provider.unrelate).toBe('function')

      // Verify DBProviderExtended methods exist
      expect(typeof provider.setEmbeddingsConfig).toBe('function')
      expect(typeof provider.semanticSearch).toBe('function')
      expect(typeof provider.hybridSearch).toBe('function')
      expect(typeof provider.on).toBe('function')
      expect(typeof provider.emit).toBe('function')
      expect(typeof provider.listEvents).toBe('function')
      expect(typeof provider.replayEvents).toBe('function')
      expect(typeof provider.createAction).toBe('function')
      expect(typeof provider.getAction).toBe('function')
      expect(typeof provider.updateAction).toBe('function')
      expect(typeof provider.listActions).toBe('function')
      expect(typeof provider.retryAction).toBe('function')
      expect(typeof provider.cancelAction).toBe('function')
      expect(typeof provider.getArtifact).toBe('function')
      expect(typeof provider.setArtifact).toBe('function')
      expect(typeof provider.deleteArtifact).toBe('function')
      expect(typeof provider.listArtifacts).toBe('function')
    })
  })

  describe('CRUD operations', () => {
    describe('create', () => {
      it('should create entity with auto-generated ID', async () => {
        const result = await adapter.create('User', undefined, {
          name: 'Alice',
          email: 'alice@example.com',
        })

        expect(result.$id).toBeDefined()
        expect(result.$type).toBe('User')
        expect(result.name).toBe('Alice')
        expect(result.email).toBe('alice@example.com')
      })

      it('should create entity with provided ID', async () => {
        const result = await adapter.create('User', 'user-123', {
          name: 'Bob',
          email: 'bob@example.com',
        })

        expect(result.$id).toContain('user-123')
        expect(result.name).toBe('Bob')
      })

      it('should pluralize type name for namespace', async () => {
        const user = await adapter.create('User', 'test-1', { name: 'Test' })
        expect(user.$id).toMatch(/^users\//)

        const post = await adapter.create('Post', 'test-2', { title: 'Hello' })
        expect(post.$id).toMatch(/^posts\//)
      })

      it('should handle types already ending in s', async () => {
        const status = await adapter.create('Status', 'active', { label: 'Active' })
        expect(status.$id).toMatch(/^status\//)
      })
    })

    describe('get', () => {
      it('should get entity by type and ID', async () => {
        await adapter.create('User', 'alice', { name: 'Alice', email: 'alice@example.com' })

        const result = await adapter.get('User', 'users/alice')

        expect(result).not.toBeNull()
        expect(result!.name).toBe('Alice')
        expect(result!.email).toBe('alice@example.com')
      })

      it('should return null for non-existent entity', async () => {
        const result = await adapter.get('User', 'nonexistent')

        expect(result).toBeNull()
      })

      it('should strip namespace from ID', async () => {
        await adapter.create('User', 'bob', { name: 'Bob' })

        // Both should work
        const result1 = await adapter.get('User', 'users/bob')
        const result2 = await adapter.get('User', 'bob')

        expect(result1).not.toBeNull()
        expect(result2).not.toBeNull()
      })
    })

    describe('list', () => {
      beforeEach(async () => {
        await adapter.create('User', 'alice', { name: 'Alice', age: 30, role: 'admin' })
        await adapter.create('User', 'bob', { name: 'Bob', age: 25, role: 'user' })
        await adapter.create('User', 'charlie', { name: 'Charlie', age: 35, role: 'admin' })
      })

      it('should list all entities of a type', async () => {
        const users = await adapter.list('User')

        expect(users).toHaveLength(3)
      })

      it('should filter entities with where clause', async () => {
        const admins = await adapter.list('User', { where: { role: 'admin' } })

        expect(admins).toHaveLength(2)
        expect(admins.every(u => u.role === 'admin')).toBe(true)
      })

      it('should support limit option', async () => {
        const users = await adapter.list('User', { limit: 2 })

        expect(users).toHaveLength(2)
      })

      it('should support offset option', async () => {
        const allUsers = await adapter.list('User', { orderBy: 'name' })
        const offsetUsers = await adapter.list('User', { orderBy: 'name', offset: 1 })

        expect(offsetUsers).toHaveLength(2)
      })

      it('should support orderBy with ascending order', async () => {
        const users = await adapter.list('User', { orderBy: 'age', order: 'asc' })

        expect(users[0]!.name).toBe('Bob') // age 25
        expect(users[2]!.name).toBe('Charlie') // age 35
      })

      it('should support orderBy with descending order', async () => {
        const users = await adapter.list('User', { orderBy: 'age', order: 'desc' })

        expect(users[0]!.name).toBe('Charlie') // age 35
        expect(users[2]!.name).toBe('Bob') // age 25
      })
    })

    describe('update', () => {
      it('should update existing entity', async () => {
        await adapter.create('User', 'alice', { name: 'Alice', email: 'alice@old.com' })

        const updated = await adapter.update('User', 'users/alice', { email: 'alice@new.com' })

        expect(updated.email).toBe('alice@new.com')
        expect(updated.name).toBe('Alice') // unchanged
      })

      it('should throw for non-existent entity', async () => {
        await expect(
          adapter.update('User', 'nonexistent', { name: 'Test' })
        ).rejects.toThrow('Entity not found')
      })
    })

    describe('delete', () => {
      it('should delete existing entity', async () => {
        await adapter.create('User', 'alice', { name: 'Alice' })

        const result = await adapter.delete('User', 'users/alice')

        expect(result).toBe(true)

        const check = await adapter.get('User', 'users/alice')
        expect(check).toBeNull()
      })

      it('should return false for non-existent entity', async () => {
        const result = await adapter.delete('User', 'nonexistent')

        expect(result).toBe(false)
      })
    })
  })

  describe('relationship operations', () => {
    beforeEach(async () => {
      await adapter.create('User', 'alice', { name: 'Alice' })
      await adapter.create('Post', 'post-1', { title: 'First Post' })
      await adapter.create('Post', 'post-2', { title: 'Second Post' })
    })

    describe('relate', () => {
      it('should create relationship between entities', async () => {
        await adapter.relate('User', 'users/alice', 'posts', 'Post', 'posts/post-1')

        const posts = await adapter.related('User', 'users/alice', 'posts')
        expect(posts).toHaveLength(1)
        expect(posts[0]!.title).toBe('First Post')
      })

      it('should support relationship metadata', async () => {
        await adapter.relate('User', 'users/alice', 'posts', 'Post', 'posts/post-1', {
          matchMode: 'exact',
          similarity: 0.95,
        })

        // Relationship created successfully
        const posts = await adapter.related('User', 'users/alice', 'posts')
        expect(posts).toHaveLength(1)
      })
    })

    describe('related', () => {
      it('should get related entities', async () => {
        await adapter.relate('User', 'users/alice', 'posts', 'Post', 'posts/post-1')
        await adapter.relate('User', 'users/alice', 'posts', 'Post', 'posts/post-2')

        const posts = await adapter.related('User', 'users/alice', 'posts')

        expect(posts).toHaveLength(2)
      })

      it('should return empty array for no relationships', async () => {
        const posts = await adapter.related('User', 'users/alice', 'comments')

        expect(posts).toHaveLength(0)
      })
    })

    describe('unrelate', () => {
      it('should remove relationship between entities', async () => {
        await adapter.relate('User', 'users/alice', 'posts', 'Post', 'posts/post-1')
        await adapter.relate('User', 'users/alice', 'posts', 'Post', 'posts/post-2')

        await adapter.unrelate('User', 'users/alice', 'posts', 'Post', 'posts/post-1')

        const posts = await adapter.related('User', 'users/alice', 'posts')
        expect(posts).toHaveLength(1)
        expect(posts[0]!.title).toBe('Second Post')
      })
    })
  })

  describe('search operations', () => {
    beforeEach(async () => {
      await adapter.create('Article', 'article-1', {
        title: 'Introduction to TypeScript',
        content: 'TypeScript is a typed superset of JavaScript',
        category: 'programming',
      })
      await adapter.create('Article', 'article-2', {
        title: 'Advanced React Patterns',
        content: 'Learn advanced patterns for building React applications',
        category: 'programming',
      })
      await adapter.create('Article', 'article-3', {
        title: 'Cooking with JavaScript',
        content: 'How to build recipe applications using JavaScript',
        category: 'cooking',
      })
    })

    describe('search (FTS)', () => {
      it('should search using full-text search', async () => {
        const results = await adapter.search('Article', 'TypeScript')

        expect(results.length).toBeGreaterThan(0)
      })

      it('should respect limit option', async () => {
        const results = await adapter.search('Article', 'JavaScript', { limit: 1 })

        expect(results.length).toBeLessThanOrEqual(1)
      })

      it('should combine search with where filter', async () => {
        const results = await adapter.search('Article', 'JavaScript', {
          where: { category: 'programming' },
        })

        // Should only find programming articles mentioning JavaScript
        expect(results.every(r => r.category === 'programming')).toBe(true)
      })
    })
  })

  describe('embeddings configuration', () => {
    it('should set embeddings config', () => {
      adapter.setEmbeddingsConfig({
        provider: 'openai',
        model: 'text-embedding-3-small',
        dimensions: 1536,
        fields: {
          Article: ['title', 'content'],
          User: ['bio'],
        },
      })

      // Config set successfully (no error thrown)
      expect(true).toBe(true)
    })
  })

  describe('events API', () => {
    describe('emit', () => {
      it('should emit event with CreateEventOptions', async () => {
        const event = await adapter.emit({
          actor: 'user-123',
          event: 'user.created',
          object: 'users/new-user',
          objectData: { name: 'New User' },
        })

        expect(event.id).toBeDefined()
        expect(event.actor).toBe('user-123')
        expect(event.event).toBe('user.created')
        expect(event.object).toBe('users/new-user')
        expect(event.timestamp).toBeInstanceOf(Date)
      })

      it('should emit event with legacy format (type, data)', async () => {
        const event = await adapter.emit('user.updated', { userId: '123' })

        expect(event.event).toBe('user.updated')
        expect(event.actor).toBe('system')
        expect(event.objectData).toEqual({ userId: '123' })
      })
    })

    describe('on', () => {
      it('should subscribe to events and receive them', async () => {
        const receivedEvents: DBEvent[] = []

        adapter.on('user.created', event => {
          receivedEvents.push(event)
        })

        await adapter.emit({
          actor: 'test',
          event: 'user.created',
        })

        expect(receivedEvents).toHaveLength(1)
        expect(receivedEvents[0]!.event).toBe('user.created')
      })

      it('should return unsubscribe function', async () => {
        const receivedEvents: DBEvent[] = []

        const unsubscribe = adapter.on('user.created', event => {
          receivedEvents.push(event)
        })

        await adapter.emit({ actor: 'test', event: 'user.created' })
        expect(receivedEvents).toHaveLength(1)

        unsubscribe()

        await adapter.emit({ actor: 'test', event: 'user.created' })
        expect(receivedEvents).toHaveLength(1) // No new events
      })

      it('should support wildcard pattern', async () => {
        const receivedEvents: DBEvent[] = []

        adapter.on('*', event => {
          receivedEvents.push(event)
        })

        await adapter.emit({ actor: 'test', event: 'user.created' })
        await adapter.emit({ actor: 'test', event: 'post.created' })

        expect(receivedEvents).toHaveLength(2)
      })

      it('should support prefix wildcard pattern', async () => {
        const receivedEvents: DBEvent[] = []

        adapter.on('user.*', event => {
          receivedEvents.push(event)
        })

        await adapter.emit({ actor: 'test', event: 'user.created' })
        await adapter.emit({ actor: 'test', event: 'user.updated' })
        await adapter.emit({ actor: 'test', event: 'post.created' })

        expect(receivedEvents).toHaveLength(2)
      })
    })

    describe('listEvents', () => {
      beforeEach(async () => {
        await adapter.emit({ actor: 'user-1', event: 'user.login', object: 'session-1' })
        await adapter.emit({ actor: 'user-2', event: 'user.login', object: 'session-2' })
        await adapter.emit({ actor: 'user-1', event: 'post.created', object: 'posts/1' })
      })

      it('should list all events', async () => {
        const events = await adapter.listEvents()

        expect(events).toHaveLength(3)
      })

      it('should filter by event type', async () => {
        const events = await adapter.listEvents({ event: 'user.login' })

        expect(events).toHaveLength(2)
        expect(events.every(e => e.event === 'user.login')).toBe(true)
      })

      it('should filter by actor', async () => {
        const events = await adapter.listEvents({ actor: 'user-1' })

        expect(events).toHaveLength(2)
        expect(events.every(e => e.actor === 'user-1')).toBe(true)
      })

      it('should support limit', async () => {
        const events = await adapter.listEvents({ limit: 2 })

        expect(events).toHaveLength(2)
      })
    })

    describe('replayEvents', () => {
      it('should replay events through handler', async () => {
        await adapter.emit({ actor: 'user-1', event: 'action.start' })
        await adapter.emit({ actor: 'user-1', event: 'action.complete' })

        const replayed: DBEvent[] = []

        await adapter.replayEvents({
          actor: 'user-1',
          handler: event => {
            replayed.push(event)
          },
        })

        expect(replayed).toHaveLength(2)
      })
    })
  })

  describe('actions API', () => {
    describe('createAction', () => {
      it('should create action with CreateActionOptions', async () => {
        const action = await adapter.createAction({
          actor: 'user-123',
          action: 'process',
          object: 'data/batch-1',
          total: 100,
        })

        expect(action.id).toBeDefined()
        expect(action.actor).toBe('user-123')
        expect(action.action).toBe('process')
        expect(action.status).toBe('pending')
        expect(action.total).toBe(100)
        expect(action.createdAt).toBeInstanceOf(Date)
      })

      it('should create action with legacy format', async () => {
        const action = await adapter.createAction({
          type: 'generate',
          data: { prompt: 'Hello world' },
          total: 50,
        })

        expect(action.action).toBe('generate')
        expect(action.actor).toBe('system')
        expect(action.status).toBe('pending')
      })

      it('should conjugate verb forms', async () => {
        const action = await adapter.createAction({
          actor: 'test',
          action: 'process',
        })

        expect(action.act).toBe('processes')
        expect(action.activity).toBe('processing')
      })
    })

    describe('getAction', () => {
      it('should get action by ID', async () => {
        const created = await adapter.createAction({
          actor: 'test',
          action: 'generate',
        })

        const retrieved = await adapter.getAction(created.id)

        expect(retrieved).not.toBeNull()
        expect(retrieved!.id).toBe(created.id)
      })

      it('should return null for non-existent action', async () => {
        const result = await adapter.getAction('nonexistent')

        expect(result).toBeNull()
      })
    })

    describe('updateAction', () => {
      it('should update action status', async () => {
        const action = await adapter.createAction({
          actor: 'test',
          action: 'process',
        })

        const updated = await adapter.updateAction(action.id, {
          status: 'active',
        })

        expect(updated.status).toBe('active')
        expect(updated.startedAt).toBeInstanceOf(Date)
      })

      it('should update action progress', async () => {
        const action = await adapter.createAction({
          actor: 'test',
          action: 'process',
          total: 100,
        })

        const updated = await adapter.updateAction(action.id, {
          progress: 50,
        })

        expect(updated.progress).toBe(50)
      })

      it('should set completedAt when completing', async () => {
        const action = await adapter.createAction({
          actor: 'test',
          action: 'process',
        })

        const updated = await adapter.updateAction(action.id, {
          status: 'completed',
          result: { success: true },
        })

        expect(updated.status).toBe('completed')
        expect(updated.completedAt).toBeInstanceOf(Date)
        expect(updated.result).toEqual({ success: true })
      })

      it('should set completedAt when failing', async () => {
        const action = await adapter.createAction({
          actor: 'test',
          action: 'process',
        })

        const updated = await adapter.updateAction(action.id, {
          status: 'failed',
          error: 'Something went wrong',
        })

        expect(updated.status).toBe('failed')
        expect(updated.completedAt).toBeInstanceOf(Date)
        expect(updated.error).toBe('Something went wrong')
      })
    })

    describe('listActions', () => {
      beforeEach(async () => {
        await adapter.createAction({ actor: 'user-1', action: 'process' })
        const action2 = await adapter.createAction({ actor: 'user-2', action: 'generate' })
        await adapter.updateAction(action2.id, { status: 'completed' })
        await adapter.createAction({ actor: 'user-1', action: 'export' })
      })

      it('should list all actions', async () => {
        const actions = await adapter.listActions()

        expect(actions).toHaveLength(3)
      })

      it('should filter by status', async () => {
        const pending = await adapter.listActions({ status: 'pending' })

        expect(pending).toHaveLength(2)
      })

      it('should filter by actor', async () => {
        const user1Actions = await adapter.listActions({ actor: 'user-1' })

        expect(user1Actions).toHaveLength(2)
      })

      it('should filter by action type', async () => {
        const processActions = await adapter.listActions({ action: 'process' })

        expect(processActions).toHaveLength(1)
      })
    })

    describe('retryAction', () => {
      it('should retry failed action', async () => {
        const action = await adapter.createAction({
          actor: 'test',
          action: 'process',
        })

        await adapter.updateAction(action.id, {
          status: 'failed',
          error: 'Network error',
        })

        const retried = await adapter.retryAction(action.id)

        expect(retried.status).toBe('pending')
        expect(retried.error).toBeUndefined()
        expect(retried.progress).toBe(0)
      })

      it('should throw for non-failed action', async () => {
        const action = await adapter.createAction({
          actor: 'test',
          action: 'process',
        })

        await expect(adapter.retryAction(action.id)).rejects.toThrow('Can only retry failed actions')
      })

      it('should throw for non-existent action', async () => {
        await expect(adapter.retryAction('nonexistent')).rejects.toThrow('Action not found')
      })
    })

    describe('cancelAction', () => {
      it('should cancel pending action', async () => {
        const action = await adapter.createAction({
          actor: 'test',
          action: 'process',
        })

        await adapter.cancelAction(action.id)

        const updated = await adapter.getAction(action.id)
        expect(updated!.status).toBe('cancelled')
      })

      it('should throw for completed action', async () => {
        const action = await adapter.createAction({
          actor: 'test',
          action: 'process',
        })

        await adapter.updateAction(action.id, { status: 'completed' })

        await expect(adapter.cancelAction(action.id)).rejects.toThrow('Cannot cancel action with status: completed')
      })

      it('should throw for non-existent action', async () => {
        await expect(adapter.cancelAction('nonexistent')).rejects.toThrow('Action not found')
      })
    })
  })

  describe('artifacts API', () => {
    describe('setArtifact', () => {
      it('should create new artifact', async () => {
        await adapter.setArtifact('https://example.com/doc', 'markdown', {
          content: '# Hello World',
          sourceHash: 'abc123',
        })

        const artifact = await adapter.getArtifact('https://example.com/doc', 'markdown')

        expect(artifact).not.toBeNull()
        expect(artifact!.content).toBe('# Hello World')
        expect(artifact!.sourceHash).toBe('abc123')
      })

      it('should update existing artifact', async () => {
        await adapter.setArtifact('https://example.com/doc', 'markdown', {
          content: 'v1',
          sourceHash: 'hash1',
        })

        await adapter.setArtifact('https://example.com/doc', 'markdown', {
          content: 'v2',
          sourceHash: 'hash2',
        })

        const artifact = await adapter.getArtifact('https://example.com/doc', 'markdown')

        expect(artifact!.content).toBe('v2')
        expect(artifact!.sourceHash).toBe('hash2')
      })

      it('should store metadata', async () => {
        await adapter.setArtifact('https://example.com/doc', 'pdf', {
          content: { pages: 10 },
          sourceHash: 'xyz789',
          metadata: { author: 'Alice', version: 2 },
        })

        const artifact = await adapter.getArtifact('https://example.com/doc', 'pdf')

        expect(artifact!.metadata).toEqual({ author: 'Alice', version: 2 })
      })
    })

    describe('getArtifact', () => {
      it('should get artifact by URL and type', async () => {
        await adapter.setArtifact('https://example.com/doc', 'markdown', {
          content: 'Test content',
          sourceHash: 'test-hash',
        })

        const artifact = await adapter.getArtifact('https://example.com/doc', 'markdown')

        expect(artifact).not.toBeNull()
        expect(artifact!.url).toBe('https://example.com/doc')
        expect(artifact!.type).toBe('markdown')
      })

      it('should return null for non-existent artifact', async () => {
        const result = await adapter.getArtifact('https://nonexistent.com', 'markdown')

        expect(result).toBeNull()
      })

      it('should differentiate by type', async () => {
        await adapter.setArtifact('https://example.com/doc', 'markdown', {
          content: 'Markdown content',
          sourceHash: 'md-hash',
        })
        await adapter.setArtifact('https://example.com/doc', 'html', {
          content: '<p>HTML content</p>',
          sourceHash: 'html-hash',
        })

        const markdown = await adapter.getArtifact('https://example.com/doc', 'markdown')
        const html = await adapter.getArtifact('https://example.com/doc', 'html')

        expect(markdown!.content).toBe('Markdown content')
        expect(html!.content).toBe('<p>HTML content</p>')
      })
    })

    describe('deleteArtifact', () => {
      it('should delete specific artifact type', async () => {
        await adapter.setArtifact('https://example.com/doc', 'markdown', {
          content: 'Test',
          sourceHash: 'hash',
        })

        await adapter.deleteArtifact('https://example.com/doc', 'markdown')

        const result = await adapter.getArtifact('https://example.com/doc', 'markdown')
        expect(result).toBeNull()
      })

      it('should delete all artifacts for URL when type not specified', async () => {
        await adapter.setArtifact('https://example.com/doc', 'markdown', {
          content: 'MD',
          sourceHash: 'hash1',
        })
        await adapter.setArtifact('https://example.com/doc', 'html', {
          content: 'HTML',
          sourceHash: 'hash2',
        })

        await adapter.deleteArtifact('https://example.com/doc')

        const artifacts = await adapter.listArtifacts('https://example.com/doc')
        expect(artifacts).toHaveLength(0)
      })
    })

    describe('listArtifacts', () => {
      beforeEach(async () => {
        await adapter.setArtifact('https://example.com/doc1', 'markdown', {
          content: 'Doc 1',
          sourceHash: 'hash1',
        })
        await adapter.setArtifact('https://example.com/doc1', 'pdf', {
          content: 'Doc 1 PDF',
          sourceHash: 'hash2',
        })
        await adapter.setArtifact('https://example.com/doc2', 'markdown', {
          content: 'Doc 2',
          sourceHash: 'hash3',
        })
      })

      it('should list all artifacts for a URL', async () => {
        const artifacts = await adapter.listArtifacts('https://example.com/doc1')

        expect(artifacts).toHaveLength(2)
        expect(artifacts.map(a => a.type).sort()).toEqual(['markdown', 'pdf'])
      })

      it('should return empty array for URL with no artifacts', async () => {
        const artifacts = await adapter.listArtifacts('https://nonexistent.com')

        expect(artifacts).toHaveLength(0)
      })
    })
  })

  describe('transactions', () => {
    describe('beginTransaction', () => {
      it('should create transaction object', async () => {
        const tx = await adapter.beginTransaction()

        expect(tx).toBeDefined()
        expect(typeof tx.get).toBe('function')
        expect(typeof tx.create).toBe('function')
        expect(typeof tx.update).toBe('function')
        expect(typeof tx.delete).toBe('function')
        expect(typeof tx.relate).toBe('function')
        expect(typeof tx.commit).toBe('function')
        expect(typeof tx.rollback).toBe('function')
      })

      it('should perform operations in transaction', async () => {
        const tx = await adapter.beginTransaction()

        await tx.create('User', 'tx-user', { name: 'Transaction User' })
        await tx.commit()

        const user = await adapter.get('User', 'users/tx-user')
        expect(user).not.toBeNull()
      })
    })
  })

  describe('semantic search', () => {
    it('should perform semantic search', async () => {
      await adapter.create('Document', 'doc-1', {
        content: 'This document is about machine learning and AI',
      })

      // Note: This will use placeholder scoring since no real embeddings
      const results = await adapter.semanticSearch('Document', 'artificial intelligence')

      expect(Array.isArray(results)).toBe(true)
    })

    it('should respect limit option', async () => {
      await adapter.create('Document', 'doc-1', { content: 'Content 1' })
      await adapter.create('Document', 'doc-2', { content: 'Content 2' })
      await adapter.create('Document', 'doc-3', { content: 'Content 3' })

      const results = await adapter.semanticSearch('Document', 'content', { limit: 2 })

      expect(results.length).toBeLessThanOrEqual(2)
    })
  })

  describe('hybrid search', () => {
    it('should perform hybrid search combining FTS and semantic', async () => {
      await adapter.create('Article', 'art-1', {
        title: 'Introduction to TypeScript',
        content: 'TypeScript extends JavaScript with static typing',
      })

      const results = await adapter.hybridSearch('Article', 'TypeScript programming')

      expect(Array.isArray(results)).toBe(true)
      if (results.length > 0) {
        expect(results[0]!.$rrfScore).toBeDefined()
        expect(results[0]!.$ftsRank).toBeDefined()
        expect(results[0]!.$semanticRank).toBeDefined()
      }
    })

    it('should respect RRF parameters', async () => {
      await adapter.create('Article', 'art-1', {
        title: 'Test Article',
        content: 'Test content',
      })

      const results = await adapter.hybridSearch('Article', 'test', {
        rrfK: 30,
        ftsWeight: 0.7,
        semanticWeight: 0.3,
      })

      expect(Array.isArray(results)).toBe(true)
    })
  })
})
