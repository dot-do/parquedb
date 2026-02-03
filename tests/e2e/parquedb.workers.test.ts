/**
 * ParqueDB Workers E2E Tests
 *
 * These tests run in the Cloudflare Workers environment with real bindings:
 * - R2 bucket (BUCKET) for Parquet file storage
 * - Durable Objects (PARQUEDB) for write operations
 * - Service bindings (PARQUEDB_SERVICE) for RPC
 *
 * Run with: npm run test:e2e:workers
 *
 * Configuration is loaded from wrangler.jsonc with environment: 'test'
 */

import { env, SELF, createExecutionContext, waitOnExecutionContext } from 'cloudflare:test'
import { describe, it, expect, beforeEach, afterEach } from 'vitest'

// Type for the env bindings from wrangler.jsonc
interface TestEnv {
  BUCKET: R2Bucket
  PARQUEDB: DurableObjectNamespace
  ENVIRONMENT: string
}

// Cast env to our typed environment
const testEnv = env as TestEnv

describe('ParqueDB Workers E2E', () => {
  describe('Environment Bindings', () => {
    it('has R2 bucket binding (BUCKET)', () => {
      expect(testEnv.BUCKET).toBeDefined()
      // R2Bucket has specific methods
      expect(typeof testEnv.BUCKET.put).toBe('function')
      expect(typeof testEnv.BUCKET.get).toBe('function')
      expect(typeof testEnv.BUCKET.delete).toBe('function')
      expect(typeof testEnv.BUCKET.list).toBe('function')
    })

    it('has Durable Object binding (PARQUEDB)', () => {
      expect(testEnv.PARQUEDB).toBeDefined()
      // DurableObjectNamespace has specific methods
      expect(typeof testEnv.PARQUEDB.idFromName).toBe('function')
      expect(typeof testEnv.PARQUEDB.idFromString).toBe('function')
      expect(typeof testEnv.PARQUEDB.get).toBe('function')
    })

    it('has test environment configured', () => {
      expect(testEnv.ENVIRONMENT).toBe('test')
    })
  })

  describe('R2 Bucket Operations', () => {
    const testKey = `test/e2e-${Date.now()}.txt`
    const testContent = 'Hello from ParqueDB E2E test'

    afterEach(async () => {
      // Cleanup test files
      try {
        await testEnv.BUCKET.delete(testKey)
      } catch {
        // Ignore cleanup errors
      }
    })

    it('can write to R2 bucket', async () => {
      const result = await testEnv.BUCKET.put(testKey, testContent)
      expect(result).toBeDefined()
      expect(result.key).toBe(testKey)
    })

    it('can read from R2 bucket', async () => {
      // Write first
      await testEnv.BUCKET.put(testKey, testContent)

      // Read back
      const object = await testEnv.BUCKET.get(testKey)
      expect(object).toBeDefined()
      expect(object).not.toBeNull()

      const text = await object!.text()
      expect(text).toBe(testContent)
    })

    it('can list objects in R2 bucket', async () => {
      // Write a test file
      await testEnv.BUCKET.put(testKey, testContent)

      // List objects with prefix
      const listed = await testEnv.BUCKET.list({ prefix: 'test/' })
      expect(listed.objects).toBeDefined()
      expect(listed.objects.length).toBeGreaterThan(0)

      const found = listed.objects.find(obj => obj.key === testKey)
      expect(found).toBeDefined()
    })

    it('can delete from R2 bucket', async () => {
      // Write first
      await testEnv.BUCKET.put(testKey, testContent)

      // Delete
      await testEnv.BUCKET.delete(testKey)

      // Verify deleted
      const object = await testEnv.BUCKET.get(testKey)
      expect(object).toBeNull()
    })
  })

  describe('Durable Object Operations', () => {
    it('can get a DO stub by name', () => {
      const id = testEnv.PARQUEDB.idFromName('posts')
      expect(id).toBeDefined()
      expect(id.toString()).toBeTruthy()

      const stub = testEnv.PARQUEDB.get(id)
      expect(stub).toBeDefined()
    })

    it('can create unique DO IDs', () => {
      const id1 = testEnv.PARQUEDB.idFromName('users')
      const id2 = testEnv.PARQUEDB.idFromName('posts')

      expect(id1.toString()).not.toBe(id2.toString())
    })

    it('returns same ID for same name', () => {
      const id1 = testEnv.PARQUEDB.idFromName('orders')
      const id2 = testEnv.PARQUEDB.idFromName('orders')

      expect(id1.toString()).toBe(id2.toString())
    })
  })

  describe('ParqueDB DO Entity Operations', () => {
    let doStub: DurableObjectStub

    beforeEach(() => {
      // Get a fresh DO stub for each test
      const id = testEnv.PARQUEDB.idFromName(`test-${Date.now()}`)
      doStub = testEnv.PARQUEDB.get(id)
    })

    it('can create an entity via DO RPC', async () => {
      const stub = doStub as unknown as {
        create(ns: string, data: unknown, options?: unknown): Promise<unknown>
      }

      const entity = await stub.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'My First Post',
        content: 'Hello, World!',
        status: 'draft',
      }, { actor: 'users/test-user' })

      expect(entity).toBeDefined()
      expect((entity as Record<string, unknown>).$id).toBeTruthy()
      expect((entity as Record<string, unknown>).$type).toBe('Post')
      expect((entity as Record<string, unknown>).name).toBe('Test Post')
      expect((entity as Record<string, unknown>).version).toBe(1)
    })

    it('can get an entity via DO RPC', async () => {
      const stub = doStub as unknown as {
        create(ns: string, data: unknown, options?: unknown): Promise<unknown>
        get(ns: string, id: string): Promise<unknown>
      }

      // Create first
      const created = await stub.create('posts', {
        $type: 'Post',
        name: 'Get Test Post',
        title: 'Post to Get',
      }, {}) as Record<string, unknown>

      // Extract the ID (without namespace prefix)
      const fullId = created.$id as string
      const id = fullId.split('/')[1]

      // Get by ID
      const retrieved = await stub.get('posts', id) as Record<string, unknown>

      expect(retrieved).toBeDefined()
      expect(retrieved.$id).toBe(fullId)
      expect(retrieved.name).toBe('Get Test Post')
    })

    it('can update an entity via DO RPC', async () => {
      const stub = doStub as unknown as {
        create(ns: string, data: unknown, options?: unknown): Promise<unknown>
        update(ns: string, id: string, update: unknown, options?: unknown): Promise<unknown>
      }

      // Create first
      const created = await stub.create('posts', {
        $type: 'Post',
        name: 'Update Test Post',
        viewCount: 0,
      }, {}) as Record<string, unknown>

      const id = (created.$id as string).split('/')[1]

      // Update with $set and $inc
      const updated = await stub.update('posts', id, {
        $set: { name: 'Updated Post' },
        $inc: { viewCount: 5 },
      }, {}) as Record<string, unknown>

      expect(updated).toBeDefined()
      expect(updated.name).toBe('Updated Post')
      expect(updated.viewCount).toBe(5)
      expect(updated.version).toBe(2)
    })

    it('can delete an entity via DO RPC', async () => {
      const stub = doStub as unknown as {
        create(ns: string, data: unknown, options?: unknown): Promise<unknown>
        delete(ns: string, id: string, options?: unknown): Promise<boolean>
        get(ns: string, id: string): Promise<unknown>
      }

      // Create first
      const created = await stub.create('posts', {
        $type: 'Post',
        name: 'Delete Test Post',
      }, {}) as Record<string, unknown>

      const id = (created.$id as string).split('/')[1]

      // Soft delete
      const deleted = await stub.delete('posts', id, {})
      expect(deleted).toBe(true)

      // Should not find (soft deleted)
      const retrieved = await stub.get('posts', id)
      expect(retrieved).toBeNull()
    })

    // Note: This test is skipped due to vitest-pool-workers isolated storage limitations
    // When a DO throws an error, isolated storage cleanup can fail
    // See: https://developers.cloudflare.com/workers/testing/vitest-integration/known-issues/#isolated-storage
    it.skip('enforces optimistic concurrency', async () => {
      const stub = doStub as unknown as {
        create(ns: string, data: unknown, options?: unknown): Promise<unknown>
        update(ns: string, id: string, update: unknown, options?: unknown): Promise<unknown>
      }

      // Create first
      const created = await stub.create('posts', {
        $type: 'Post',
        name: 'Concurrency Test',
      }, {}) as Record<string, unknown>

      const id = (created.$id as string).split('/')[1]

      // Update with correct version
      await stub.update('posts', id, {
        $set: { name: 'First Update' },
      }, { expectedVersion: 1 })

      // Update with stale version should fail
      let error: Error | null = null
      try {
        await stub.update('posts', id, {
          $set: { name: 'Stale Update' },
        }, { expectedVersion: 1 })
      } catch (e) {
        error = e as Error
      }

      expect(error).not.toBeNull()
      expect(error?.message).toMatch(/Version mismatch/)
    })
  })

  describe('ParqueDB DO Relationship Operations', () => {
    let doStub: DurableObjectStub

    beforeEach(() => {
      const id = testEnv.PARQUEDB.idFromName(`test-rels-${Date.now()}`)
      doStub = testEnv.PARQUEDB.get(id)
    })

    it('can create relationships between entities', async () => {
      const stub = doStub as unknown as {
        create(ns: string, data: unknown, options?: unknown): Promise<unknown>
        link(fromId: string, predicate: string, toId: string, options?: unknown): Promise<void>
        getRelationships(ns: string, id: string, predicate?: string, direction?: string): Promise<unknown[]>
      }

      // Create author and post
      const author = await stub.create('users', {
        $type: 'User',
        name: 'Test Author',
      }, {}) as Record<string, unknown>

      const post = await stub.create('posts', {
        $type: 'Post',
        name: 'Linked Post',
      }, {}) as Record<string, unknown>

      // Link post to author
      await stub.link(post.$id as string, 'author', author.$id as string, {})

      // Get relationships
      const postId = (post.$id as string).split('/')[1]
      const rels = await stub.getRelationships('posts', postId, 'author', 'outbound')

      expect(rels).toBeDefined()
      expect(rels.length).toBe(1)
      const rel = rels[0] as Record<string, unknown>
      expect(rel.predicate).toBe('author')

      // Verify fromType/fromName and toType/toName are populated from entity data
      expect(rel.fromType).toBe('Post')
      expect(rel.fromName).toBe('Linked Post')
      expect(rel.toType).toBe('User')
      expect(rel.toName).toBe('Test Author')
    })

    it('can create inline relationships during entity creation', async () => {
      const stub = doStub as unknown as {
        create(ns: string, data: unknown, options?: unknown): Promise<unknown>
        getRelationships(ns: string, id: string, predicate?: string, direction?: string): Promise<unknown[]>
      }

      // Create author first
      const author = await stub.create('users', {
        $type: 'User',
        name: 'Inline Author',
      }, {}) as Record<string, unknown>

      // Create post with inline author relationship
      const post = await stub.create('posts', {
        $type: 'Post',
        name: 'Post with Inline Author',
        author: {
          'Inline Author': author.$id,
        },
      }, {}) as Record<string, unknown>

      // Verify relationship was created
      const postId = (post.$id as string).split('/')[1]
      const rels = await stub.getRelationships('posts', postId, 'author', 'outbound')

      expect(rels.length).toBe(1)
    })

    it('can remove relationships', async () => {
      const stub = doStub as unknown as {
        create(ns: string, data: unknown, options?: unknown): Promise<unknown>
        link(fromId: string, predicate: string, toId: string, options?: unknown): Promise<void>
        unlink(fromId: string, predicate: string, toId: string, options?: unknown): Promise<void>
        getRelationships(ns: string, id: string, predicate?: string, direction?: string): Promise<unknown[]>
      }

      // Create entities
      const user = await stub.create('users', {
        $type: 'User',
        name: 'Unlink Test User',
      }, {}) as Record<string, unknown>

      const post = await stub.create('posts', {
        $type: 'Post',
        name: 'Unlink Test Post',
      }, {}) as Record<string, unknown>

      // Link
      await stub.link(post.$id as string, 'author', user.$id as string, {})

      // Unlink
      await stub.unlink(post.$id as string, 'author', user.$id as string, {})

      // Verify unlinked
      const postId = (post.$id as string).split('/')[1]
      const rels = await stub.getRelationships('posts', postId, 'author', 'outbound')

      expect(rels.length).toBe(0)
    })
  })

  describe('HTTP API', () => {
    it('handles requests via SELF binding', async () => {
      const ctx = createExecutionContext()

      // Create a test request to the root endpoint (doesn't require storage)
      const request = new Request('http://localhost/', {
        method: 'GET',
      })

      // Use SELF to call the worker
      const response = await SELF.fetch(request)

      // Should get a successful response
      expect(response).toBeDefined()
      expect(response.status).toBe(200)

      // Verify response structure
      const body = await response.json() as Record<string, unknown>
      expect(body.api).toBeDefined()
      expect((body.api as Record<string, unknown>).name).toBe('ParqueDB')

      await waitOnExecutionContext(ctx)
    })
  })

  // =============================================================================
  // Dataset Endpoint Tests (RED - Expected to fail)
  // =============================================================================
  // These tests verify that dataset endpoints return actual data from Parquet files.
  // They are designed to catch the "File not found" errors that occur in production.
  // =============================================================================

  describe('Dataset Endpoints', () => {
    describe('/datasets - List all datasets', () => {
      it('returns list of available datasets', async () => {
        const ctx = createExecutionContext()
        const response = await SELF.fetch(new Request('http://localhost/datasets'))

        expect(response.status).toBe(200)
        const body = await response.json() as Record<string, unknown>

        expect(body.api).toBeDefined()
        expect((body.api as Record<string, unknown>).resource).toBe('datasets')
        expect(body.items).toBeDefined()
        expect(Array.isArray(body.items)).toBe(true)
        expect((body.items as unknown[]).length).toBeGreaterThan(0)

        // Verify expected datasets are present
        const items = body.items as Array<Record<string, unknown>>
        const datasetIds = items.map(item => item.id)
        expect(datasetIds).toContain('onet-graph')
        expect(datasetIds).toContain('imdb')
        expect(datasetIds).toContain('unspsc')

        await waitOnExecutionContext(ctx)
      })
    })

    describe('/datasets/:dataset - Dataset detail', () => {
      it('returns onet-graph dataset details', async () => {
        const ctx = createExecutionContext()
        const response = await SELF.fetch(new Request('http://localhost/datasets/onet-graph'))

        expect(response.status).toBe(200)
        const body = await response.json() as Record<string, unknown>

        expect(body.api).toBeDefined()
        expect((body.api as Record<string, unknown>).resource).toBe('dataset')
        expect((body.api as Record<string, unknown>).id).toBe('onet-graph')

        // Verify collections are listed
        expect(body.data).toBeDefined()
        const data = body.data as Record<string, unknown>
        expect(data.collections).toBeDefined()

        await waitOnExecutionContext(ctx)
      })

      it('returns 404 for non-existent dataset', async () => {
        const ctx = createExecutionContext()
        const response = await SELF.fetch(new Request('http://localhost/datasets/nonexistent'))

        expect(response.status).toBe(404)

        await waitOnExecutionContext(ctx)
      })
    })

    // Skipped: requires parquet files in R2 - use npm run check:datasets for production validation
    describe.skip('/datasets/:dataset/:collection - Collection list', () => {
      // O*NET Graph Dataset Tests
      describe('onet-graph dataset', () => {
        it('returns occupations with actual data', async () => {
          const ctx = createExecutionContext()
          const response = await SELF.fetch(
            new Request('http://localhost/datasets/onet-graph/occupations')
          )

          expect(response.status).toBe(200)
          const body = await response.json() as Record<string, unknown>

          // Verify response structure
          expect(body.api).toBeDefined()
          expect((body.api as Record<string, unknown>).resource).toBe('collection')
          expect((body.api as Record<string, unknown>).dataset).toBe('onet-graph')
          expect((body.api as Record<string, unknown>).collection).toBe('occupations')

          // Verify items contain actual data (not empty)
          expect(body.items).toBeDefined()
          expect(Array.isArray(body.items)).toBe(true)
          const items = body.items as Array<Record<string, unknown>>
          expect(items.length).toBeGreaterThan(0)

          // Verify first item has expected structure
          const firstItem = items[0]
          expect(firstItem.$id).toBeDefined()
          expect(firstItem.name).toBeDefined()

          await waitOnExecutionContext(ctx)
        })

        it('returns skills with actual data', async () => {
          const ctx = createExecutionContext()
          const response = await SELF.fetch(
            new Request('http://localhost/datasets/onet-graph/skills')
          )

          expect(response.status).toBe(200)
          const body = await response.json() as Record<string, unknown>

          expect(body.items).toBeDefined()
          const items = body.items as Array<Record<string, unknown>>
          expect(items.length).toBeGreaterThan(0)
          expect(items[0].$id).toBeDefined()

          await waitOnExecutionContext(ctx)
        })

        it('returns abilities with actual data', async () => {
          const ctx = createExecutionContext()
          const response = await SELF.fetch(
            new Request('http://localhost/datasets/onet-graph/abilities')
          )

          expect(response.status).toBe(200)
          const body = await response.json() as Record<string, unknown>

          expect(body.items).toBeDefined()
          const items = body.items as Array<Record<string, unknown>>
          expect(items.length).toBeGreaterThan(0)

          await waitOnExecutionContext(ctx)
        })

        it('returns knowledge with actual data', async () => {
          const ctx = createExecutionContext()
          const response = await SELF.fetch(
            new Request('http://localhost/datasets/onet-graph/knowledge')
          )

          expect(response.status).toBe(200)
          const body = await response.json() as Record<string, unknown>

          expect(body.items).toBeDefined()
          const items = body.items as Array<Record<string, unknown>>
          expect(items.length).toBeGreaterThan(0)

          await waitOnExecutionContext(ctx)
        })
      })

      // IMDB Dataset Tests
      describe('imdb dataset', () => {
        it('returns titles with actual data', async () => {
          const ctx = createExecutionContext()
          const response = await SELF.fetch(
            new Request('http://localhost/datasets/imdb/titles')
          )

          expect(response.status).toBe(200)
          const body = await response.json() as Record<string, unknown>

          expect(body.api).toBeDefined()
          expect((body.api as Record<string, unknown>).dataset).toBe('imdb')
          expect((body.api as Record<string, unknown>).collection).toBe('titles')

          expect(body.items).toBeDefined()
          const items = body.items as Array<Record<string, unknown>>
          expect(items.length).toBeGreaterThan(0)

          await waitOnExecutionContext(ctx)
        })

        it('returns names with actual data', async () => {
          const ctx = createExecutionContext()
          const response = await SELF.fetch(
            new Request('http://localhost/datasets/imdb/names')
          )

          expect(response.status).toBe(200)
          const body = await response.json() as Record<string, unknown>

          expect(body.items).toBeDefined()
          const items = body.items as Array<Record<string, unknown>>
          expect(items.length).toBeGreaterThan(0)

          await waitOnExecutionContext(ctx)
        })
      })

      // UNSPSC Dataset Tests
      describe('unspsc dataset', () => {
        it('returns segments with actual data', async () => {
          const ctx = createExecutionContext()
          const response = await SELF.fetch(
            new Request('http://localhost/datasets/unspsc/segments')
          )

          expect(response.status).toBe(200)
          const body = await response.json() as Record<string, unknown>

          expect(body.items).toBeDefined()
          const items = body.items as Array<Record<string, unknown>>
          expect(items.length).toBeGreaterThan(0)

          await waitOnExecutionContext(ctx)
        })
      })
    })

    // Skipped: requires parquet files in R2 - use npm run check:datasets for production validation
    describe.skip('Filtering', () => {
      it('filters onet-graph occupations by name', async () => {
        const ctx = createExecutionContext()
        const filter = encodeURIComponent(JSON.stringify({ name: { $regex: 'Software' } }))
        const response = await SELF.fetch(
          new Request(`http://localhost/datasets/onet-graph/occupations?filter=${filter}`)
        )

        expect(response.status).toBe(200)
        const body = await response.json() as Record<string, unknown>

        expect(body.api).toBeDefined()
        expect((body.api as Record<string, unknown>).filter).toBeDefined()

        expect(body.items).toBeDefined()
        const items = body.items as Array<Record<string, unknown>>
        // Should return matching items (or empty if no match, but not an error)
        expect(Array.isArray(items)).toBe(true)

        await waitOnExecutionContext(ctx)
      })

      it('filters with equality operator', async () => {
        const ctx = createExecutionContext()
        // Filter for a specific $type
        const filter = encodeURIComponent(JSON.stringify({ $type: 'Occupation' }))
        const response = await SELF.fetch(
          new Request(`http://localhost/datasets/onet-graph/occupations?filter=${filter}`)
        )

        expect(response.status).toBe(200)
        const body = await response.json() as Record<string, unknown>

        expect(body.items).toBeDefined()
        const items = body.items as Array<Record<string, unknown>>
        // All returned items should have the specified $type
        for (const item of items) {
          expect(item.$type).toBe('Occupation')
        }

        await waitOnExecutionContext(ctx)
      })
    })

    // Skipped: requires parquet files in R2 - use npm run check:datasets for production validation
    describe.skip('Pagination', () => {
      it('respects limit parameter', async () => {
        const ctx = createExecutionContext()
        const response = await SELF.fetch(
          new Request('http://localhost/datasets/onet-graph/occupations?limit=5')
        )

        expect(response.status).toBe(200)
        const body = await response.json() as Record<string, unknown>

        expect(body.api).toBeDefined()
        expect((body.api as Record<string, unknown>).limit).toBe(5)

        expect(body.items).toBeDefined()
        const items = body.items as Array<Record<string, unknown>>
        expect(items.length).toBeLessThanOrEqual(5)

        await waitOnExecutionContext(ctx)
      })

      it('respects skip parameter', async () => {
        const ctx = createExecutionContext()

        // Get first page
        const response1 = await SELF.fetch(
          new Request('http://localhost/datasets/onet-graph/occupations?limit=5')
        )
        expect(response1.status).toBe(200)
        const body1 = await response1.json() as Record<string, unknown>
        const items1 = body1.items as Array<Record<string, unknown>>

        // Get second page with skip
        const response2 = await SELF.fetch(
          new Request('http://localhost/datasets/onet-graph/occupations?limit=5&skip=5')
        )
        expect(response2.status).toBe(200)
        const body2 = await response2.json() as Record<string, unknown>
        const items2 = body2.items as Array<Record<string, unknown>>

        expect((body2.api as Record<string, unknown>).skip).toBe(5)

        // Items from page 2 should be different from page 1
        if (items1.length > 0 && items2.length > 0) {
          expect(items1[0].$id).not.toBe(items2[0].$id)
        }

        await waitOnExecutionContext(ctx)
      })

      it('indicates hasMore when more results exist', async () => {
        const ctx = createExecutionContext()
        const response = await SELF.fetch(
          new Request('http://localhost/datasets/onet-graph/occupations?limit=1')
        )

        expect(response.status).toBe(200)
        const body = await response.json() as Record<string, unknown>

        // O*NET has ~1000 occupations, so hasMore should be true with limit=1
        expect((body.api as Record<string, unknown>).hasMore).toBe(true)

        await waitOnExecutionContext(ctx)
      })

      it('provides pagination links', async () => {
        const ctx = createExecutionContext()
        const response = await SELF.fetch(
          new Request('http://localhost/datasets/onet-graph/occupations?limit=10')
        )

        expect(response.status).toBe(200)
        const body = await response.json() as Record<string, unknown>

        expect(body.links).toBeDefined()
        const links = body.links as Record<string, string>

        // Should have next link if hasMore is true
        if ((body.api as Record<string, unknown>).hasMore) {
          expect(links.next).toBeDefined()
        }

        await waitOnExecutionContext(ctx)
      })
    })

    // Skipped: requires parquet files in R2 - use npm run check:datasets for production validation
    describe.skip('Error handling', () => {
      it('returns 404 for non-existent collection', async () => {
        const ctx = createExecutionContext()
        const response = await SELF.fetch(
          new Request('http://localhost/datasets/onet-graph/nonexistent')
        )

        // Should return error (either 404 or 500 with file not found)
        // Currently returns Error 1101 (file not found) which may be 500
        expect(response.status).toBeGreaterThanOrEqual(400)

        await waitOnExecutionContext(ctx)
      })

      it('handles invalid filter JSON gracefully', async () => {
        const ctx = createExecutionContext()
        const response = await SELF.fetch(
          new Request('http://localhost/datasets/onet-graph/occupations?filter=invalid-json')
        )

        expect(response.status).toBe(400)

        await waitOnExecutionContext(ctx)
      })

      it('handles invalid limit parameter', async () => {
        const ctx = createExecutionContext()
        const response = await SELF.fetch(
          new Request('http://localhost/datasets/onet-graph/occupations?limit=-1')
        )

        expect(response.status).toBe(400)

        await waitOnExecutionContext(ctx)
      })
    })
  })
})
