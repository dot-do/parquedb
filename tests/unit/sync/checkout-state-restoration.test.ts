/**
 * Checkout State Restoration Tests
 *
 * Tests for the checkout command's state reconstruction functionality.
 * Verifies that checkout properly restores:
 * - Collection data files
 * - Relationship indexes
 * - Event log position
 *
 * Also tests safety features:
 * - Uncommitted changes detection
 * - Atomic operations (rollback on failure)
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { BranchManager, createBranchManager } from '../../../src/sync/branch-manager'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import { createRefManager } from '../../../src/sync/refs'
import { createCommit, saveCommit, loadCommit, type DatabaseState } from '../../../src/sync/commit'
import {
  storeObject,
  loadObject,
  snapshotState,
  reconstructState,
  checkUncommittedChanges,
} from '../../../src/sync/state-store'

describe('checkout-state-restoration', () => {
  let storage: MemoryBackend
  let branchManager: BranchManager

  beforeEach(() => {
    storage = new MemoryBackend()
    branchManager = createBranchManager({ storage })
  })

  /**
   * Helper to create a commit with actual data stored in object store
   */
  async function createCommitWithData(
    message: string,
    data: {
      collections?: Record<string, { data: string; schema?: string }>
      relationships?: { forward?: Record<string, string>; reverse?: Record<string, string> }
    },
    parents: string[] = []
  ): Promise<string> {
    const collections: DatabaseState['collections'] = {}

    // Store collection data in object store
    for (const [ns, colData] of Object.entries(data.collections || {})) {
      const dataBytes = new TextEncoder().encode(colData.data)
      const dataHash = await storeObject(storage, dataBytes)

      let schemaHash = ''
      if (colData.schema) {
        const schemaBytes = new TextEncoder().encode(colData.schema)
        schemaHash = await storeObject(storage, schemaBytes)
      }

      collections[ns] = {
        dataHash,
        schemaHash,
        rowCount: 1,
      }

      // Also write the actual data file (as it would be in a real scenario)
      await storage.write(`data/${ns}/data.parquet`, dataBytes)
      if (colData.schema) {
        await storage.write(`data/${ns}/schema.json`, new TextEncoder().encode(colData.schema))
      }
    }

    // Store relationship data
    let forwardHash = ''
    let reverseHash = ''

    if (data.relationships?.forward) {
      const forwardManifest: Record<string, string> = {}
      for (const [ns, content] of Object.entries(data.relationships.forward)) {
        const bytes = new TextEncoder().encode(content)
        const hash = await storeObject(storage, bytes)
        const path = `rels/forward/${ns}.parquet`
        forwardManifest[path] = hash
        await storage.write(path, bytes)
      }
      const manifestBytes = new TextEncoder().encode(JSON.stringify(forwardManifest, Object.keys(forwardManifest).sort()))
      forwardHash = await storeObject(storage, manifestBytes)
    } else {
      forwardHash = await storeObject(storage, new TextEncoder().encode('{}'))
    }

    if (data.relationships?.reverse) {
      const reverseManifest: Record<string, string> = {}
      for (const [ns, content] of Object.entries(data.relationships.reverse)) {
        const bytes = new TextEncoder().encode(content)
        const hash = await storeObject(storage, bytes)
        const path = `rels/reverse/${ns}.parquet`
        reverseManifest[path] = hash
        await storage.write(path, bytes)
      }
      const manifestBytes = new TextEncoder().encode(JSON.stringify(reverseManifest, Object.keys(reverseManifest).sort()))
      reverseHash = await storeObject(storage, manifestBytes)
    } else {
      reverseHash = await storeObject(storage, new TextEncoder().encode('{}'))
    }

    const commit = await createCommit(
      {
        collections,
        relationships: {
          forwardHash,
          reverseHash,
        },
        eventLogPosition: {
          segmentId: 'initial',
          offset: 0,
        },
      },
      {
        message,
        author: 'test-author',
        parents,
      }
    )

    await saveCommit(storage, commit)
    return commit.hash
  }

  describe('storeObject and loadObject', () => {
    it('should store and retrieve objects by hash', async () => {
      const data = new TextEncoder().encode('test data content')
      const hash = await storeObject(storage, data)

      expect(hash).toMatch(/^[a-f0-9]{64}$/) // SHA256 hex string

      const retrieved = await loadObject(storage, hash)
      expect(new TextDecoder().decode(retrieved)).toBe('test data content')
    })

    it('should deduplicate identical content', async () => {
      const data = new TextEncoder().encode('duplicate content')

      const hash1 = await storeObject(storage, data)
      const hash2 = await storeObject(storage, data)

      expect(hash1).toBe(hash2)
    })

    it('should throw when loading non-existent object', async () => {
      await expect(loadObject(storage, 'nonexistenthash')).rejects.toThrow('Object not found')
    })
  })

  describe('checkout with state reconstruction', () => {
    it('should restore collection data when switching branches', async () => {
      // Create first commit with collection data
      const commit1 = await createCommitWithData('Initial commit', {
        collections: {
          posts: { data: 'initial posts data' },
        },
      })

      const refManager = createRefManager(storage)
      await refManager.setHead('main')
      await refManager.updateRef('main', commit1)

      // Create a second branch with different data
      // Note: We create the commit but NOT the actual files yet - the files
      // will be created when we checkout the branch
      const featurePostsData = new TextEncoder().encode('feature posts data')
      const featureUsersData = new TextEncoder().encode('feature users data')
      const featurePostsHash = await storeObject(storage, featurePostsData)
      const featureUsersHash = await storeObject(storage, featureUsersData)

      const commit2 = await createCommit(
        {
          collections: {
            posts: {
              dataHash: featurePostsHash,
              schemaHash: '',
              rowCount: 1,
            },
            users: {
              dataHash: featureUsersHash,
              schemaHash: '',
              rowCount: 1,
            },
          },
          relationships: {
            forwardHash: await storeObject(storage, new TextEncoder().encode('{}')),
            reverseHash: await storeObject(storage, new TextEncoder().encode('{}')),
          },
          eventLogPosition: {
            segmentId: 'initial',
            offset: 0,
          },
        },
        {
          message: 'Feature commit',
          author: 'test-author',
          parents: [commit1],
        }
      )
      await saveCommit(storage, commit2)
      await refManager.updateRef('feature', commit2.hash)

      // Checkout feature branch - this should restore the feature data
      await branchManager.checkout('feature')

      // Verify data was restored
      const postsData = await storage.read('data/posts/data.parquet')
      expect(new TextDecoder().decode(postsData)).toBe('feature posts data')

      const usersData = await storage.read('data/users/data.parquet')
      expect(new TextDecoder().decode(usersData)).toBe('feature users data')

      // Checkout back to main
      await branchManager.checkout('main')

      // Verify original data was restored
      const mainPostsData = await storage.read('data/posts/data.parquet')
      expect(new TextDecoder().decode(mainPostsData)).toBe('initial posts data')

      // Users collection should not exist on main branch
      const usersExists = await storage.exists('data/users/data.parquet')
      expect(usersExists).toBe(false)
    })

    it('should restore relationship indexes', async () => {
      // Create commit with relationships
      const commit1 = await createCommitWithData('With relationships', {
        collections: {
          posts: { data: 'posts data' },
        },
        relationships: {
          forward: { posts: 'forward relationship data' },
          reverse: { posts: 'reverse relationship data' },
        },
      })

      const refManager = createRefManager(storage)
      await refManager.setHead('main')
      await refManager.updateRef('main', commit1)

      // Create branch without relationships
      await branchManager.create('no-rels')
      const commit2 = await createCommitWithData(
        'No relationships',
        {
          collections: {
            posts: { data: 'posts data v2' },
          },
        },
        [commit1]
      )
      await refManager.updateRef('no-rels', commit2)

      // Delete relationships manually to simulate the change
      await storage.delete('rels/forward/posts.parquet')
      await storage.delete('rels/reverse/posts.parquet')

      // Checkout back to main should restore relationships
      await branchManager.checkout('main')

      const forwardData = await storage.read('rels/forward/posts.parquet')
      expect(new TextDecoder().decode(forwardData)).toBe('forward relationship data')

      const reverseData = await storage.read('rels/reverse/posts.parquet')
      expect(new TextDecoder().decode(reverseData)).toBe('reverse relationship data')
    })
  })

  describe('uncommitted changes detection', () => {
    it('should detect modified collection data', async () => {
      // Create initial commit
      const commit1 = await createCommitWithData('Initial', {
        collections: {
          posts: { data: 'original data' },
        },
      })

      const refManager = createRefManager(storage)
      await refManager.setHead('main')
      await refManager.updateRef('main', commit1)

      // Modify the data file
      await storage.write('data/posts/data.parquet', new TextEncoder().encode('modified data'))

      // Check for uncommitted changes
      const result = await branchManager.hasUncommittedChanges()

      expect(result.hasChanges).toBe(true)
      expect(result.changedCollections).toContain('posts')
    })

    it('should detect new collections', async () => {
      const commit1 = await createCommitWithData('Initial', {
        collections: {
          posts: { data: 'posts data' },
        },
      })

      const refManager = createRefManager(storage)
      await refManager.setHead('main')
      await refManager.updateRef('main', commit1)

      // Add a new collection
      await storage.mkdir('data/users')
      await storage.write('data/users/data.parquet', new TextEncoder().encode('new users data'))

      const result = await branchManager.hasUncommittedChanges()

      expect(result.hasChanges).toBe(true)
      expect(result.changedCollections).toContain('users')
    })

    it('should block checkout with uncommitted changes unless forced', async () => {
      const commit1 = await createCommitWithData('Initial', {
        collections: {
          posts: { data: 'original data' },
        },
      })

      const refManager = createRefManager(storage)
      await refManager.setHead('main')
      await refManager.updateRef('main', commit1)

      // Create another branch
      await branchManager.create('feature')
      const commit2 = await createCommitWithData(
        'Feature',
        {
          collections: {
            posts: { data: 'feature data' },
          },
        },
        [commit1]
      )
      await refManager.updateRef('feature', commit2)

      // Modify data on main
      await storage.write('data/posts/data.parquet', new TextEncoder().encode('modified data'))

      // Try to checkout feature - should fail
      await expect(branchManager.checkout('feature')).rejects.toThrow('uncommitted changes')

      // Force checkout should succeed
      await expect(branchManager.checkout('feature', { force: true })).resolves.not.toThrow()
    })
  })

  describe('atomic operations', () => {
    it('should not leave partial state on reconstruction failure', async () => {
      const commit1 = await createCommitWithData('Initial', {
        collections: {
          posts: { data: 'posts data' },
          users: { data: 'users data' },
        },
      })

      const refManager = createRefManager(storage)
      await refManager.setHead('main')
      await refManager.updateRef('main', commit1)

      // Create a commit that references a non-existent object
      // This simulates a corrupted commit or missing object
      const brokenCommit = await createCommit(
        {
          collections: {
            posts: {
              dataHash: 'nonexistent_hash_that_will_fail',
              schemaHash: '',
              rowCount: 1,
            },
          },
          relationships: {
            forwardHash: '',
            reverseHash: '',
          },
          eventLogPosition: {
            segmentId: 'initial',
            offset: 0,
          },
        },
        {
          message: 'Broken commit',
          author: 'test',
          parents: [commit1],
        }
      )
      await saveCommit(storage, brokenCommit)
      await refManager.updateRef('broken', brokenCommit.hash)

      // Store original data for comparison
      const originalPostsData = await storage.read('data/posts/data.parquet')

      // Try to checkout broken branch - should fail
      await expect(branchManager.checkout('broken', { force: true })).rejects.toThrow()

      // Verify original data is still intact (rollback worked)
      const currentPostsData = await storage.read('data/posts/data.parquet')
      expect(new TextDecoder().decode(currentPostsData)).toBe(new TextDecoder().decode(originalPostsData))
    })
  })

  describe('create and checkout integration', () => {
    it('should create branch and checkout with state from current HEAD', async () => {
      const commit1 = await createCommitWithData('Initial', {
        collections: {
          posts: { data: 'initial posts' },
        },
      })

      const refManager = createRefManager(storage)
      await refManager.setHead('main')
      await refManager.updateRef('main', commit1)

      // Create and checkout new branch
      await branchManager.checkout('feature', { create: true })

      // Verify we're on the new branch
      const current = await branchManager.current()
      expect(current).toBe('feature')

      // Verify data is still accessible
      const postsData = await storage.read('data/posts/data.parquet')
      expect(new TextDecoder().decode(postsData)).toBe('initial posts')
    })
  })
})
