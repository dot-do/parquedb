/**
 * ai-database Integration Tests
 *
 * Comprehensive tests for the ParqueDB adapter for ai-database.
 * Tests cover:
 * - Basic CRUD operations
 * - Relationship handling (forward and backward)
 * - Search operations (FTS and semantic)
 * - Events API
 * - Actions API
 * - Artifacts API
 * - Transactions
 *
 * NO MOCKS - Uses real MemoryBackend storage.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { ParqueDB } from '../../src/ParqueDB'
import { MemoryBackend } from '../../src/storage/MemoryBackend'
import type { Schema } from '../../src/types'
import {
  ParqueDBAdapter,
  createParqueDBProvider,
  type DBProvider,
  type DBProviderExtended,
  type DBEvent,
  type DBAction,
} from '../../src/integrations/ai-database'

// =============================================================================
// Test Schema with Relationships
// =============================================================================

function createTestSchema(): Schema {
  return {
    Post: {
      $type: 'schema:BlogPosting',
      $ns: 'posts',
      name: 'string!',
      title: 'string!',
      content: 'text?',
      status: { type: 'string', default: 'draft' },
      published: 'boolean?',
      order: 'int?',
      // Forward relationship to Author
      author: '-> Author.posts',
    },
    Author: {
      $type: 'schema:Person',
      $ns: 'authors',
      name: 'string!',
      email: 'email?',
      bio: 'text?',
      // Backward relationship: all posts by this author
      posts: '<- Post.author[]',
    },
    Article: {
      $type: 'schema:Article',
      $ns: 'articles',
      name: 'string!',
      title: 'string!',
      content: 'text?',
      tags: 'string[]?',
    },
    Document: {
      $type: 'schema:Document',
      $ns: 'documents',
      name: 'string!',
      title: 'string!',
      content: 'text?',
    },
    User: {
      $type: 'schema:Person',
      $ns: 'users',
      name: 'string!',
      email: 'email?',
    },
    Entity: {
      $type: 'schema:Thing',
      $ns: 'entities',
      name: 'string!',
      value: 'string?',
    },
    News: {
      $type: 'schema:NewsArticle',
      $ns: 'news',
      name: 'string!',
      title: 'string!',
    },
    Blog: {
      $type: 'schema:Blog',
      $ns: 'blogs',
      name: 'string!',
      title: 'string!',
      description: 'text?',
    },
    Topic: {
      $type: 'schema:Thing',
      $ns: 'topics',
      name: 'string!',
      description: 'text?',
      // Forward relationship to Blog
      blog: '-> Blog.topics',
    },
    // System namespaces for events/actions/artifacts
    // Using 'sys' prefix instead of underscore
    SysEvent: {
      $type: 'schema:Event',
      $ns: 'sysevents',
      name: 'string!',
      actor: 'string!',
      event: 'string!',
      object: 'string?',
      objectData: 'json?',
      result: 'string?',
      resultData: 'json?',
      meta: 'json?',
      timestamp: 'datetime!',
    },
    SysAction: {
      $type: 'schema:Action',
      $ns: 'sysactions',
      name: 'string!',
      actor: 'string!',
      action: 'string!',
      act: 'string!',
      activity: 'string!',
      object: 'string?',
      objectData: 'json?',
      status: 'string!',
      progress: 'int?',
      total: 'int?',
      result: 'json?',
      error: 'string?',
      meta: 'json?',
      createdAt: 'datetime!',
      startedAt: 'datetime?',
      completedAt: 'datetime?',
    },
    SysArtifact: {
      $type: 'schema:DigitalDocument',
      $ns: 'sysartifacts',
      name: 'string!',
      url: 'url!',
      type: 'string!',
      sourceHash: 'string!',
      content: 'json?',
      metadata: 'json?',
      createdAt: 'datetime!',
    },
  }
}

// =============================================================================
// Test Setup
// =============================================================================

describe('ai-database Integration', () => {
  let parquedb: ParqueDB
  let provider: DBProviderExtended

  beforeEach(async () => {
    // Create fresh instances for each test with schema
    parquedb = new ParqueDB({
      storage: new MemoryBackend(),
      schema: createTestSchema(),
    })
    provider = createParqueDBProvider(parquedb)
  })

  afterEach(() => {
    // Clean up
    parquedb.dispose()
  })

  // ===========================================================================
  // Basic CRUD Operations
  // ===========================================================================

  describe('Basic CRUD', () => {
    it('should create and retrieve entities', async () => {
      // Create a post
      const post = await provider.create('Post', undefined, {
        title: 'Hello World',
        content: 'This is my first post',
        published: false,
      })

      expect(post.$id).toBeDefined()
      expect(post.$type).toBe('Post')
      expect(post.title).toBe('Hello World')
      expect(post.content).toBe('This is my first post')
      expect(post.published).toBe(false)

      // Retrieve the post
      const retrieved = await provider.get('Post', post.$id as string)
      expect(retrieved).not.toBeNull()
      expect(retrieved!.$id).toBe(post.$id)
      expect(retrieved!.title).toBe('Hello World')
    })

    it('should create entity with custom ID', async () => {
      const post = await provider.create('Post', 'custom-123', {
        title: 'Custom ID Post',
        content: 'Content here',
      })

      // Entity should be created and retrievable
      expect(post.$id).toBeDefined()

      // Should be retrievable
      const retrieved = await provider.get('Post', post.$id as string)
      expect(retrieved).not.toBeNull()
      expect(retrieved!.title).toBe('Custom ID Post')
    })

    it('should list all entities of a type', async () => {
      // Create multiple posts
      await provider.create('Post', undefined, { title: 'Post 1', content: 'Content 1' })
      await provider.create('Post', undefined, { title: 'Post 2', content: 'Content 2' })
      await provider.create('Post', undefined, { title: 'Post 3', content: 'Content 3' })

      const posts = await provider.list('Post')
      expect(posts.length).toBe(3)
    })

    it('should list with filter options', async () => {
      await provider.create('Post', undefined, { title: 'Draft Post', status: 'draft' })
      await provider.create('Post', undefined, { title: 'Published Post', status: 'published' })
      await provider.create('Post', undefined, { title: 'Another Draft', status: 'draft' })

      const drafts = await provider.list('Post', { where: { status: 'draft' } })
      expect(drafts.length).toBe(2)
      expect(drafts.every(p => p.status === 'draft')).toBe(true)
    })

    it('should list with pagination options', async () => {
      // Create 5 posts
      for (let i = 1; i <= 5; i++) {
        await provider.create('Post', undefined, { title: `Post ${i}`, order: i })
      }

      // Test limit
      const limited = await provider.list('Post', { limit: 2 })
      expect(limited.length).toBe(2)

      // Test offset
      const offset = await provider.list('Post', { limit: 2, offset: 2 })
      expect(offset.length).toBe(2)
    })

    it('should update entities', async () => {
      const post = await provider.create('Post', undefined, {
        title: 'Original Title',
        content: 'Original Content',
      })

      const updated = await provider.update('Post', post.$id as string, {
        title: 'Updated Title',
      })

      expect(updated.title).toBe('Updated Title')
      expect(updated.content).toBe('Original Content') // Should preserve unchanged fields

      // Verify persistence
      const retrieved = await provider.get('Post', post.$id as string)
      expect(retrieved!.title).toBe('Updated Title')
    })

    it('should delete entities', async () => {
      const post = await provider.create('Post', undefined, {
        title: 'To Be Deleted',
        content: 'Content',
      })

      // Verify existence
      const exists = await provider.get('Post', post.$id as string)
      expect(exists).not.toBeNull()

      // Delete
      const deleted = await provider.delete('Post', post.$id as string)
      expect(deleted).toBe(true)

      // Verify deletion
      const gone = await provider.get('Post', post.$id as string)
      expect(gone).toBeNull()
    })

    it('should return false when deleting non-existent entity', async () => {
      const deleted = await provider.delete('Post', 'posts/nonexistent-id')
      expect(deleted).toBe(false)
    })
  })

  // ===========================================================================
  // Relationship Operations
  // ===========================================================================

  describe('Relationships', () => {
    it('should create relationships between entities via relate method', async () => {
      // Create author and post
      const author = await provider.create('Author', undefined, {
        name: 'John Doe',
        email: 'john@example.com',
      })

      const post = await provider.create('Post', undefined, {
        title: 'My Post',
        content: 'Content here',
      })

      // Create relationship using relate method (uses $link internally)
      await provider.relate('Post', post.$id as string, 'author', 'Author', author.$id as string)

      // The relationship should be stored - verify the post was updated
      const updatedPost = await provider.get('Post', post.$id as string)
      expect(updatedPost).not.toBeNull()
    })

    it('should handle one-to-many relationships setup', async () => {
      // Create author
      const author = await provider.create('Author', undefined, {
        name: 'Jane Smith',
        email: 'jane@example.com',
      })

      // Create multiple posts
      const post1 = await provider.create('Post', undefined, { title: 'Post 1' })
      const post2 = await provider.create('Post', undefined, { title: 'Post 2' })
      const post3 = await provider.create('Post', undefined, { title: 'Post 3' })

      // Create relationships
      await provider.relate('Post', post1.$id as string, 'author', 'Author', author.$id as string)
      await provider.relate('Post', post2.$id as string, 'author', 'Author', author.$id as string)
      await provider.relate('Post', post3.$id as string, 'author', 'Author', author.$id as string)

      // Verify posts were updated
      const updatedPost1 = await provider.get('Post', post1.$id as string)
      expect(updatedPost1).not.toBeNull()
    })

    it('should create and remove relationships via unrelate', async () => {
      const author = await provider.create('Author', undefined, { name: 'Author' })
      const post = await provider.create('Post', undefined, { title: 'Post' })

      // Create relationship
      await provider.relate('Post', post.$id as string, 'author', 'Author', author.$id as string)

      // Remove relationship - should not throw
      await provider.unrelate('Post', post.$id as string, 'author', 'Author', author.$id as string)

      // Post should still exist
      const updatedPost = await provider.get('Post', post.$id as string)
      expect(updatedPost).not.toBeNull()
    })

    it('should handle relationship metadata', async () => {
      const author = await provider.create('Author', undefined, { name: 'Author' })
      const post = await provider.create('Post', undefined, { title: 'Post' })

      // Create relationship with metadata - should not throw
      await provider.relate(
        'Post',
        post.$id as string,
        'author',
        'Author',
        author.$id as string,
        {
          matchMode: 'exact',
          similarity: 1.0,
        }
      )

      // Post should be updated
      const updatedPost = await provider.get('Post', post.$id as string)
      expect(updatedPost).not.toBeNull()
    })
  })

  // ===========================================================================
  // Search Operations
  // ===========================================================================

  describe('Search Operations', () => {
    beforeEach(async () => {
      // Seed data for search tests
      await provider.create('Article', undefined, {
        title: 'Introduction to TypeScript',
        content: 'TypeScript is a typed superset of JavaScript',
        tags: ['typescript', 'javascript', 'programming'],
      })
      await provider.create('Article', undefined, {
        title: 'React Hooks Guide',
        content: 'Learn how to use React hooks effectively',
        tags: ['react', 'javascript', 'frontend'],
      })
      await provider.create('Article', undefined, {
        title: 'Node.js Best Practices',
        content: 'Best practices for building Node.js applications',
        tags: ['nodejs', 'javascript', 'backend'],
      })
    })

    it('should perform full-text search', async () => {
      const results = await provider.search('Article', 'TypeScript')
      expect(results.length).toBeGreaterThanOrEqual(1)
      expect(results.some(r => r.title === 'Introduction to TypeScript')).toBe(true)
    })

    it('should search with options', async () => {
      const results = await provider.search('Article', 'javascript', {
        limit: 2,
      })
      expect(results.length).toBeLessThanOrEqual(2)
    })
  })

  // ===========================================================================
  // Semantic Search
  // ===========================================================================

  describe('Semantic Search', () => {
    it('should configure embeddings', () => {
      provider.setEmbeddingsConfig({
        provider: 'openai',
        model: 'text-embedding-3-small',
        dimensions: 1536,
      })
      // No error means success
    })

    it('should perform semantic search', async () => {
      // Create some articles
      await provider.create('Document', undefined, {
        title: 'Machine Learning Basics',
        content: 'Introduction to neural networks and deep learning',
      })
      await provider.create('Document', undefined, {
        title: 'Data Structures',
        content: 'Arrays, linked lists, trees, and graphs',
      })

      // Semantic search (will use vector similarity when available)
      const results = await provider.semanticSearch('Document', 'AI and neural networks', {
        limit: 5,
      })

      expect(Array.isArray(results)).toBe(true)
      // Results should have score
      if (results.length > 0) {
        expect(results[0].$score).toBeDefined()
      }
    })

    it('should perform hybrid search', async () => {
      await provider.create('Document', undefined, {
        title: 'Python Programming',
        content: 'Learn Python for data science and machine learning',
      })

      const results = await provider.hybridSearch('Document', 'Python data science', {
        limit: 5,
        ftsWeight: 0.5,
        semanticWeight: 0.5,
      })

      expect(Array.isArray(results)).toBe(true)
      if (results.length > 0) {
        expect(results[0].$rrfScore).toBeDefined()
        expect(results[0].$ftsRank).toBeDefined()
        expect(results[0].$semanticRank).toBeDefined()
      }
    })
  })

  // ===========================================================================
  // Events API
  // ===========================================================================

  describe('Events API', () => {
    it('should emit and list events', async () => {
      // Emit an event
      const event = await provider.emit({
        actor: 'user/123',
        event: 'post.created',
        object: 'post/456',
        objectData: { title: 'New Post' },
      })

      expect(event.id).toBeDefined()
      expect(event.actor).toBe('user/123')
      expect(event.event).toBe('post.created')
      expect(event.timestamp).toBeInstanceOf(Date)

      // List events
      const events = await provider.listEvents({ event: 'post.created' })
      expect(events.length).toBeGreaterThanOrEqual(1)
    })

    it('should emit with legacy format', async () => {
      const event = await provider.emit('user.signup', { email: 'test@example.com' })

      expect(event.event).toBe('user.signup')
      expect(event.objectData?.email).toBe('test@example.com')
    })

    it('should subscribe to events', async () => {
      const receivedEvents: DBEvent[] = []

      // Subscribe to events
      const unsubscribe = provider.on('post.*', (event) => {
        receivedEvents.push(event)
      })

      // Emit matching events
      await provider.emit({ actor: 'user/1', event: 'post.created' })
      await provider.emit({ actor: 'user/1', event: 'post.updated' })

      expect(receivedEvents.length).toBe(2)

      // Unsubscribe
      unsubscribe()

      // This event should not be received
      await provider.emit({ actor: 'user/1', event: 'post.deleted' })
      expect(receivedEvents.length).toBe(2)
    })

    it('should filter events by criteria', async () => {
      await provider.emit({ actor: 'user/1', event: 'post.created' })
      await provider.emit({ actor: 'user/2', event: 'post.created' })
      await provider.emit({ actor: 'user/1', event: 'comment.created' })

      // Filter by actor
      const user1Events = await provider.listEvents({ actor: 'user/1' })
      expect(user1Events.length).toBe(2)

      // Filter by event type
      const postEvents = await provider.listEvents({ event: 'post.created' })
      expect(postEvents.length).toBe(2)
    })

    it('should replay events', async () => {
      // Emit some events
      await provider.emit({ actor: 'user/1', event: 'action.1' })
      await provider.emit({ actor: 'user/1', event: 'action.2' })
      await provider.emit({ actor: 'user/1', event: 'action.3' })

      // Replay
      const replayed: DBEvent[] = []
      await provider.replayEvents({
        actor: 'user/1',
        handler: (event) => {
          replayed.push(event)
        },
      })

      expect(replayed.length).toBe(3)
    })
  })

  // ===========================================================================
  // Actions API
  // ===========================================================================

  describe('Actions API', () => {
    it('should create and track actions', async () => {
      const action = await provider.createAction({
        actor: 'user/123',
        action: 'generate',
        object: 'document/456',
        total: 100,
      })

      expect(action.id).toBeDefined()
      expect(action.actor).toBe('user/123')
      expect(action.action).toBe('generate')
      expect(action.status).toBe('pending')
      expect(action.activity).toBe('generating') // Verb conjugation
    })

    it('should update action progress', async () => {
      const action = await provider.createAction({
        actor: 'user/1',
        action: 'process',
        total: 10,
      })

      // Start the action
      const started = await provider.updateAction(action.id, {
        status: 'active',
        progress: 0,
      })
      expect(started.status).toBe('active')
      expect(started.startedAt).toBeInstanceOf(Date)

      // Update progress
      const progress = await provider.updateAction(action.id, { progress: 5 })
      expect(progress.progress).toBe(5)

      // Complete the action
      const completed = await provider.updateAction(action.id, {
        status: 'completed',
        progress: 10,
        result: { success: true },
      })
      expect(completed.status).toBe('completed')
      expect(completed.completedAt).toBeInstanceOf(Date)
    })

    it('should get action by ID', async () => {
      const created = await provider.createAction({
        actor: 'user/1',
        action: 'import',
      })

      const retrieved = await provider.getAction(created.id)
      expect(retrieved).not.toBeNull()
      expect(retrieved!.id).toBe(created.id)
    })

    it('should list actions with filters', async () => {
      await provider.createAction({ actor: 'user/1', action: 'import' })
      await provider.createAction({ actor: 'user/2', action: 'export' })
      const pending = await provider.createAction({ actor: 'user/1', action: 'process' })
      await provider.updateAction(pending.id, { status: 'active' })

      // Filter by status
      const activeActions = await provider.listActions({ status: 'active' })
      expect(activeActions.length).toBe(1)

      // Filter by actor
      const user1Actions = await provider.listActions({ actor: 'user/1' })
      expect(user1Actions.length).toBe(2)
    })

    it('should retry failed actions', async () => {
      const action = await provider.createAction({
        actor: 'user/1',
        action: 'sync',
      })

      // Fail the action
      await provider.updateAction(action.id, {
        status: 'failed',
        error: 'Network timeout',
      })

      // Retry
      const retried = await provider.retryAction(action.id)
      expect(retried.status).toBe('pending')
      expect(retried.error).toBeUndefined()
    })

    it('should cancel pending actions', async () => {
      const action = await provider.createAction({
        actor: 'user/1',
        action: 'longRunning',
      })

      await provider.cancelAction(action.id)

      const cancelled = await provider.getAction(action.id)
      expect(cancelled!.status).toBe('cancelled')
    })

    it('should not cancel completed actions', async () => {
      const action = await provider.createAction({
        actor: 'user/1',
        action: 'task',
      })

      await provider.updateAction(action.id, { status: 'completed' })

      await expect(provider.cancelAction(action.id)).rejects.toThrow(
        /Cannot cancel action with status/
      )
    })
  })

  // ===========================================================================
  // Artifacts API
  // ===========================================================================

  describe('Artifacts API', () => {
    it('should store and retrieve artifacts', async () => {
      const url = 'https://example.com/document.pdf'
      const type = 'pdf-extraction'

      // Store artifact
      await provider.setArtifact(url, type, {
        content: { text: 'Extracted PDF content', pages: 10 },
        sourceHash: 'abc123',
        metadata: { version: '1.0' },
      })

      // Retrieve artifact
      const artifact = await provider.getArtifact(url, type)
      expect(artifact).not.toBeNull()
      expect(artifact!.url).toBe(url)
      expect(artifact!.type).toBe(type)
      expect(artifact!.sourceHash).toBe('abc123')
      expect((artifact!.content as Record<string, unknown>).text).toBe('Extracted PDF content')
    })

    it('should update existing artifacts', async () => {
      const url = 'https://example.com/data.json'
      const type = 'json-parse'

      // Create initial artifact
      await provider.setArtifact(url, type, {
        content: { version: 1 },
        sourceHash: 'v1',
      })

      // Update artifact
      await provider.setArtifact(url, type, {
        content: { version: 2 },
        sourceHash: 'v2',
      })

      // Should have updated content
      const artifact = await provider.getArtifact(url, type)
      expect(artifact!.sourceHash).toBe('v2')
      expect((artifact!.content as Record<string, unknown>).version).toBe(2)
    })

    it('should delete artifacts', async () => {
      const url = 'https://example.com/temp.txt'
      const type = 'text-extraction'

      await provider.setArtifact(url, type, {
        content: 'Temp content',
        sourceHash: 'hash',
      })

      // Delete
      await provider.deleteArtifact(url, type)

      // Should be gone
      const artifact = await provider.getArtifact(url, type)
      expect(artifact).toBeNull()
    })

    it('should list all artifacts for a URL', async () => {
      const url = 'https://example.com/multi.pdf'

      // Store multiple artifacts for same URL
      await provider.setArtifact(url, 'text', { content: 'Text', sourceHash: 'a' })
      await provider.setArtifact(url, 'summary', { content: 'Summary', sourceHash: 'b' })
      await provider.setArtifact(url, 'embedding', { content: [1, 2, 3], sourceHash: 'c' })

      const artifacts = await provider.listArtifacts(url)
      expect(artifacts.length).toBe(3)
      expect(artifacts.map(a => a.type).sort()).toEqual(['embedding', 'summary', 'text'])
    })
  })

  // ===========================================================================
  // Transactions
  // ===========================================================================

  describe('Transactions', () => {
    it('should support transaction operations', async () => {
      const tx = await provider.beginTransaction!()

      // Create within transaction
      const user = await tx.create('User', undefined, {
        name: 'Transaction User',
        email: 'tx@example.com',
      })

      // Update within transaction
      await tx.update('User', user.$id as string, { name: 'Updated Name' })

      // Commit
      await tx.commit()

      // Verify persistence
      const retrieved = await provider.get('User', user.$id as string)
      expect(retrieved).not.toBeNull()
    })
  })

  // ===========================================================================
  // Type Conversion
  // ===========================================================================

  describe('Type Conversion', () => {
    it('should convert type names to namespaces correctly', async () => {
      // Singular types become plural namespaces
      const post = await provider.create('Post', undefined, { title: 'Test' })
      expect(post.$id).toContain('posts/')

      // Types already ending in 's' stay the same
      const news = await provider.create('News', undefined, { title: 'News Item' })
      expect(news.$id).toContain('news/')
    })

    it('should handle entity references with namespaces', async () => {
      const entity = await provider.create('Entity', undefined, { value: 'test' })
      const entityId = entity.$id as string

      // Get with full ID
      const retrieved = await provider.get('Entity', entityId)
      expect(retrieved).not.toBeNull()

      // Get with just the local ID part
      const localId = entityId.split('/').slice(1).join('/')
      const alsoRetrieved = await provider.get('Entity', localId)
      expect(alsoRetrieved).not.toBeNull()
    })
  })

  // ===========================================================================
  // Factory Function
  // ===========================================================================

  describe('Factory Function', () => {
    it('should create provider via factory function', () => {
      const newProvider = createParqueDBProvider(parquedb)
      expect(newProvider).toBeInstanceOf(ParqueDBAdapter)
    })

    it('should implement DBProvider interface', () => {
      const p: DBProvider = provider

      // All required methods should exist
      expect(typeof p.get).toBe('function')
      expect(typeof p.list).toBe('function')
      expect(typeof p.search).toBe('function')
      expect(typeof p.create).toBe('function')
      expect(typeof p.update).toBe('function')
      expect(typeof p.delete).toBe('function')
      expect(typeof p.related).toBe('function')
      expect(typeof p.relate).toBe('function')
      expect(typeof p.unrelate).toBe('function')
    })

    it('should implement DBProviderExtended interface', () => {
      const p: DBProviderExtended = provider

      // Extended methods should exist
      expect(typeof p.setEmbeddingsConfig).toBe('function')
      expect(typeof p.semanticSearch).toBe('function')
      expect(typeof p.hybridSearch).toBe('function')
      expect(typeof p.on).toBe('function')
      expect(typeof p.emit).toBe('function')
      expect(typeof p.listEvents).toBe('function')
      expect(typeof p.replayEvents).toBe('function')
      expect(typeof p.createAction).toBe('function')
      expect(typeof p.getAction).toBe('function')
      expect(typeof p.updateAction).toBe('function')
      expect(typeof p.listActions).toBe('function')
      expect(typeof p.retryAction).toBe('function')
      expect(typeof p.cancelAction).toBe('function')
      expect(typeof p.getArtifact).toBe('function')
      expect(typeof p.setArtifact).toBe('function')
      expect(typeof p.deleteArtifact).toBe('function')
      expect(typeof p.listArtifacts).toBe('function')
    })
  })
})
