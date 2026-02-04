/**
 * ParqueDB Worker E2E Tests
 *
 * Tests the complete write path through Durable Objects using RPC.
 * Uses vitest-pool-workers for realistic Cloudflare Workers environment.
 *
 * NOTE: These tests require the Cloudflare Workers test environment (vitest-pool-workers).
 * Run with: npm run test:e2e:workers
 * They will be skipped when running in Node.js environment.
 */

import { describe, it, expect } from 'vitest'
import { ParqueDBClient } from '../../src/client/ParqueDBClient'

// =============================================================================
// ParqueDBClient Tests (work in Node.js without Workers environment)
// =============================================================================

describe('ParqueDBClient', () => {
  // Note: In a real test environment, we would have a service binding
  // For now, we test the client interface structure

  describe('Collection Access', () => {
    it('provides proxy-based collection access', () => {
      // Create a mock service stub
      const mockStub = {
        find: async () => ({ items: [], hasMore: false }),
        get: async () => null,
        create: async (ns: string, data: any) => ({
          $id: `${ns}/test-id`,
          $type: data.$type,
          name: data.name,
          createdAt: new Date(),
          updatedAt: new Date(),
          createdBy: 'system/test',
          updatedBy: 'system/test',
          version: 1,
          ...data,
        }),
        update: async () => ({}),
        delete: async () => ({ deletedCount: 1 }),
        createMany: async () => [],
        deleteMany: async () => ({ deletedCount: 0 }),
        count: async () => 0,
        exists: async () => false,
        getRelationships: async () => [],
        link: async () => {},
        unlink: async () => {},
        flush: async () => {},
        getFlushStatus: async () => ({ unflushedCount: 0 }),
      } as any

      const client = new ParqueDBClient(mockStub)

      // Access Posts collection
      expect(client.Posts).toBeDefined()
      expect(client.Posts.namespace).toBe('posts')

      // Access Users collection
      expect(client.Users).toBeDefined()
      expect(client.Users.namespace).toBe('users')

      // Explicit collection access
      const custom = client.collection('custom')
      expect(custom.namespace).toBe('custom')
    })

    it('collection operations delegate to stub', async () => {
      let createCalled = false
      let createArgs: any[] = []

      const mockStub = {
        find: async () => ({ items: [], hasMore: false }),
        get: async () => null,
        create: async (ns: string, data: any, options: any) => {
          createCalled = true
          createArgs = [ns, data, options]
          return {
            $id: `${ns}/new-id`,
            $type: data.$type,
            name: data.name,
            createdAt: new Date(),
            updatedAt: new Date(),
            createdBy: options?.actor || 'system/test',
            updatedBy: options?.actor || 'system/test',
            version: 1,
          }
        },
        update: async () => ({}),
        delete: async () => ({ deletedCount: 1 }),
        createMany: async () => [],
        deleteMany: async () => ({ deletedCount: 0 }),
        count: async () => 0,
        exists: async () => false,
        getRelationships: async () => [],
        link: async () => {},
        unlink: async () => {},
        flush: async () => {},
        getFlushStatus: async () => ({ unflushedCount: 0 }),
      } as any

      const client = new ParqueDBClient(mockStub, { actor: 'users/test-user' })

      const post = await client.Posts.create({
        $type: 'Post',
        name: 'Test Post',
      })

      expect(createCalled).toBe(true)
      expect(createArgs[0]).toBe('posts')
      expect(createArgs[1].$type).toBe('Post')
      expect(createArgs[2].actor).toBe('users/test-user')
      expect(post.$id).toBe('posts/new-id')
    })
  })

  describe('Typed Collections', () => {
    it('supports typed collection access', async () => {
      interface PostData {
        title: string
        content: string
        status: 'draft' | 'published'
      }

      const mockStub = {
        find: async () => ({
          items: [
            {
              $id: 'posts/1',
              $type: 'Post',
              name: 'Test',
              title: 'Test Post',
              content: 'Content',
              status: 'published',
              createdAt: new Date(),
              updatedAt: new Date(),
              createdBy: 'system/test',
              updatedBy: 'system/test',
              version: 1,
            },
          ],
          hasMore: false,
        }),
        get: async () => null,
        create: async () => ({}),
        update: async () => ({}),
        delete: async () => ({ deletedCount: 1 }),
        createMany: async () => [],
        deleteMany: async () => ({ deletedCount: 0 }),
        count: async () => 1,
        exists: async () => true,
        getRelationships: async () => [],
        link: async () => {},
        unlink: async () => {},
        flush: async () => {},
        getFlushStatus: async () => ({ unflushedCount: 0 }),
      } as any

      const client = new ParqueDBClient(mockStub)
      const posts = client.collection<PostData>('posts')

      const result = await posts.find({ status: 'published' })

      expect(result.items.length).toBe(1)
      // TypeScript should recognize these as typed
      expect(result.items[0].title).toBe('Test Post')
      expect(result.items[0].status).toBe('published')
    })
  })
})

// =============================================================================
// Workers-Only Tests
// =============================================================================
//
// Workers-specific tests have been moved to dedicated test files that run
// with vitest-pool-workers:
// - tests/e2e/parquedb.workers.test.ts - Complete DO and HTTP API tests
// - tests/e2e/worker/do-bulk.workers.test.ts - Bulk operation tests
// - tests/e2e/worker/do-sqlite-persistence.workers.test.ts - Persistence tests
//
// Run with: npm run test:e2e:workers
