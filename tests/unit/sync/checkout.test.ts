import { describe, it, expect, beforeEach } from 'vitest'
import { BranchManager, createBranchManager } from '../../../src/sync/branch-manager'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import { createRefManager } from '../../../src/sync/refs'
import { createCommit, saveCommit, type DatabaseState } from '../../../src/sync/commit'
import {
  computeObjectHash,
  type ObjectStore,
  createObjectStore,
} from '../../../src/sync/object-store'
import { storeObject } from '../../../src/sync/state-store'
import { StoragePaths } from '../../../src/types/storage'

describe('checkout state reconstruction', () => {
  let storage: MemoryBackend
  let branchManager: BranchManager
  let objectStore: ObjectStore

  beforeEach(() => {
    storage = new MemoryBackend()
    branchManager = createBranchManager({ storage })
    objectStore = createObjectStore(storage)
  })

  /**
   * Helper to create sample parquet data
   */
  function createSampleData(content: string): Uint8Array {
    return new TextEncoder().encode(content)
  }

  /**
   * Helper to create a full database state with objects stored
   *
   * Note: The state-store uses a manifest-based format for relationships.
   * The manifest is a JSON object mapping file paths to content hashes.
   */
  async function createDatabaseStateWithObjects(
    collections: Record<string, string>,
    relationships?: { forward: string; reverse: string }
  ): Promise<DatabaseState> {
    const collectionStates: DatabaseState['collections'] = {}

    // Store collection data objects
    for (const [name, content] of Object.entries(collections)) {
      const data = createSampleData(content)
      const dataHash = await objectStore.save(data)

      // For schema, we'll use a simple JSON
      const schemaData = createSampleData(JSON.stringify({ collection: name }))
      const schemaHash = await objectStore.save(schemaData)

      collectionStates[name] = {
        dataHash,
        schemaHash,
        rowCount: content.length, // Simple placeholder for row count
      }
    }

    // Store relationship objects using the manifest format expected by state-store
    // The state-store stores relationships as a manifest JSON that maps file paths to content hashes
    const forwardData = createSampleData(relationships?.forward ?? 'forward-rels')
    const reverseData = createSampleData(relationships?.reverse ?? 'reverse-rels')

    // Store the actual relationship data
    const forwardFileHash = await objectStore.save(forwardData)
    const reverseFileHash = await objectStore.save(reverseData)

    // Create manifests (JSON mapping file path to content hash)
    const forwardManifest: Record<string, string> = {
      'rels/forward/default.parquet': forwardFileHash,
    }
    const reverseManifest: Record<string, string> = {
      'rels/reverse/default.parquet': reverseFileHash,
    }

    // Store manifests and get their hashes (this is what goes in the commit state)
    const forwardManifestData = createSampleData(JSON.stringify(forwardManifest, Object.keys(forwardManifest).sort()))
    const reverseManifestData = createSampleData(JSON.stringify(reverseManifest, Object.keys(reverseManifest).sort()))

    const forwardHash = await objectStore.save(forwardManifestData)
    const reverseHash = await objectStore.save(reverseManifestData)

    return {
      collections: collectionStates,
      relationships: {
        forwardHash,
        reverseHash,
      },
      eventLogPosition: {
        segmentId: 'segment-0',
        offset: 0,
      },
    }
  }

  /**
   * Helper to create and save a commit
   */
  async function createAndSaveCommit(
    state: DatabaseState,
    message: string,
    parents: string[] = []
  ): Promise<string> {
    const commit = await createCommit(state, {
      message,
      author: 'test-author',
      parents,
    })
    await saveCommit(storage, commit)
    return commit.hash
  }

  /**
   * Helper to write current working state files
   */
  async function writeWorkingState(
    collections: Record<string, string>,
    relationships?: { forward: string; reverse: string }
  ): Promise<void> {
    for (const [name, content] of Object.entries(collections)) {
      const dataPath = StoragePaths.data(name)
      await storage.write(dataPath, createSampleData(content))
    }

    // Write relationship files
    await storage.write(
      'rels/forward/default.parquet',
      createSampleData(relationships?.forward ?? 'forward-rels')
    )
    await storage.write(
      'rels/reverse/default.parquet',
      createSampleData(relationships?.reverse ?? 'reverse-rels')
    )
  }

  describe('object-store', () => {
    it('should compute deterministic hash for data', async () => {
      const data = createSampleData('hello world')
      const hash1 = computeObjectHash(data)
      const hash2 = computeObjectHash(data)

      expect(hash1).toBe(hash2)
      expect(hash1).toHaveLength(64) // SHA256 hex length
    })

    it('should save and load object by hash', async () => {
      const data = createSampleData('test content')
      const hash = await objectStore.save(data)

      const loaded = await objectStore.load(hash)
      expect(new TextDecoder().decode(loaded)).toBe('test content')
    })

    it('should not duplicate objects with same content', async () => {
      const data = createSampleData('duplicate content')
      const hash1 = await objectStore.save(data)
      const hash2 = await objectStore.save(data)

      expect(hash1).toBe(hash2)

      // Verify only one file exists
      const exists1 = await storage.exists(`_meta/objects/${hash1.slice(0, 2)}/${hash1}`)
      expect(exists1).toBe(true)
    })

    it('should store objects in content-addressed directory structure', async () => {
      const data = createSampleData('structured storage test')
      const hash = await objectStore.save(data)

      // Objects stored as _meta/objects/{first-2-chars}/{full-hash}
      const expectedPath = `_meta/objects/${hash.slice(0, 2)}/${hash}`
      const exists = await storage.exists(expectedPath)
      expect(exists).toBe(true)
    })

    it('should throw when loading non-existent object', async () => {
      await expect(objectStore.load('nonexistent-hash')).rejects.toThrow()
    })

    it('should check if object exists', async () => {
      const data = createSampleData('check existence')
      const hash = await objectStore.save(data)

      expect(await objectStore.exists(hash)).toBe(true)
      expect(await objectStore.exists('nonexistent')).toBe(false)
    })
  })

  describe('checkout with state reconstruction', () => {
    it('should restore collection data when checking out a branch', async () => {
      // Setup: Create initial state on main
      const mainState = await createDatabaseStateWithObjects({
        users: 'main-users-data',
        posts: 'main-posts-data',
      })
      const mainCommit = await createAndSaveCommit(mainState, 'Initial commit on main')

      const refManager = createRefManager(storage)
      await refManager.setHead('main')
      await refManager.updateRef('main', mainCommit)

      // Write current working state matching main
      await writeWorkingState({
        users: 'main-users-data',
        posts: 'main-posts-data',
      })

      // Create feature branch with different data
      const featureState = await createDatabaseStateWithObjects({
        users: 'feature-users-data',
        posts: 'feature-posts-data',
      })
      const featureCommit = await createAndSaveCommit(featureState, 'Feature commit', [mainCommit])
      await refManager.updateRef('feature', featureCommit)

      // Checkout feature branch
      await branchManager.checkout('feature')

      // Verify working state was restored
      const usersData = await storage.read(StoragePaths.data('users'))
      const postsData = await storage.read(StoragePaths.data('posts'))

      expect(new TextDecoder().decode(usersData)).toBe('feature-users-data')
      expect(new TextDecoder().decode(postsData)).toBe('feature-posts-data')
    })

    it('should restore relationship files when checking out', async () => {
      // Setup main with specific relationships
      const mainState = await createDatabaseStateWithObjects(
        { users: 'users-data' },
        { forward: 'main-forward-rels', reverse: 'main-reverse-rels' }
      )
      const mainCommit = await createAndSaveCommit(mainState, 'Main commit')

      const refManager = createRefManager(storage)
      await refManager.setHead('main')
      await refManager.updateRef('main', mainCommit)

      await writeWorkingState(
        { users: 'users-data' },
        { forward: 'main-forward-rels', reverse: 'main-reverse-rels' }
      )

      // Create feature with different relationships
      const featureState = await createDatabaseStateWithObjects(
        { users: 'users-data' },
        { forward: 'feature-forward-rels', reverse: 'feature-reverse-rels' }
      )
      const featureCommit = await createAndSaveCommit(featureState, 'Feature commit', [mainCommit])
      await refManager.updateRef('feature', featureCommit)

      // Checkout feature
      await branchManager.checkout('feature')

      // Verify relationships were restored
      const forwardData = await storage.read('rels/forward/default.parquet')
      const reverseData = await storage.read('rels/reverse/default.parquet')

      expect(new TextDecoder().decode(forwardData)).toBe('feature-forward-rels')
      expect(new TextDecoder().decode(reverseData)).toBe('feature-reverse-rels')
    })

    it('should switch back and forth between branches', async () => {
      // Create main state
      const mainState = await createDatabaseStateWithObjects({ users: 'main-users' })
      const mainCommit = await createAndSaveCommit(mainState, 'Main commit')

      const refManager = createRefManager(storage)
      await refManager.setHead('main')
      await refManager.updateRef('main', mainCommit)
      await writeWorkingState({ users: 'main-users' })

      // Create feature state
      const featureState = await createDatabaseStateWithObjects({ users: 'feature-users' })
      const featureCommit = await createAndSaveCommit(featureState, 'Feature commit', [mainCommit])
      await refManager.updateRef('feature', featureCommit)

      // Switch to feature
      await branchManager.checkout('feature')
      let usersData = await storage.read(StoragePaths.data('users'))
      expect(new TextDecoder().decode(usersData)).toBe('feature-users')

      // Switch back to main
      await branchManager.checkout('main')
      usersData = await storage.read(StoragePaths.data('users'))
      expect(new TextDecoder().decode(usersData)).toBe('main-users')

      // Switch to feature again
      await branchManager.checkout('feature')
      usersData = await storage.read(StoragePaths.data('users'))
      expect(new TextDecoder().decode(usersData)).toBe('feature-users')
    })

    it('should handle branches with different collections', async () => {
      // Main has users and posts
      const mainState = await createDatabaseStateWithObjects({
        users: 'main-users',
        posts: 'main-posts',
      })
      const mainCommit = await createAndSaveCommit(mainState, 'Main commit')

      const refManager = createRefManager(storage)
      await refManager.setHead('main')
      await refManager.updateRef('main', mainCommit)
      await writeWorkingState({ users: 'main-users', posts: 'main-posts' })

      // Feature adds comments collection and modifies users
      const featureState = await createDatabaseStateWithObjects({
        users: 'feature-users',
        posts: 'main-posts',
        comments: 'feature-comments',
      })
      const featureCommit = await createAndSaveCommit(featureState, 'Feature commit', [mainCommit])
      await refManager.updateRef('feature', featureCommit)

      // Checkout feature
      await branchManager.checkout('feature')

      // Verify all collections
      const usersData = await storage.read(StoragePaths.data('users'))
      const postsData = await storage.read(StoragePaths.data('posts'))
      const commentsData = await storage.read(StoragePaths.data('comments'))

      expect(new TextDecoder().decode(usersData)).toBe('feature-users')
      expect(new TextDecoder().decode(postsData)).toBe('main-posts')
      expect(new TextDecoder().decode(commentsData)).toBe('feature-comments')

      // Checkout main - comments should be removed
      await branchManager.checkout('main')

      const mainUsersData = await storage.read(StoragePaths.data('users'))
      expect(new TextDecoder().decode(mainUsersData)).toBe('main-users')

      // Comments collection should not exist in main
      const commentsExists = await storage.exists(StoragePaths.data('comments'))
      expect(commentsExists).toBe(false)
    })

    it('should preserve event log position in checkout', async () => {
      const state1: DatabaseState = {
        collections: {},
        relationships: {
          forwardHash: await objectStore.save(createSampleData('forward')),
          reverseHash: await objectStore.save(createSampleData('reverse')),
        },
        eventLogPosition: {
          segmentId: 'segment-1',
          offset: 100,
        },
      }
      const commit1 = await createAndSaveCommit(state1, 'Commit 1')

      const state2: DatabaseState = {
        collections: {},
        relationships: {
          forwardHash: await objectStore.save(createSampleData('forward')),
          reverseHash: await objectStore.save(createSampleData('reverse')),
        },
        eventLogPosition: {
          segmentId: 'segment-2',
          offset: 250,
        },
      }
      const commit2 = await createAndSaveCommit(state2, 'Commit 2', [commit1])

      const refManager = createRefManager(storage)
      await refManager.setHead('main')
      await refManager.updateRef('main', commit1)
      await refManager.updateRef('feature', commit2)

      // Checkout feature and verify event log position is available
      await branchManager.checkout('feature')

      // The restored state should be accessible via the branch manager
      const currentState = await branchManager.getCurrentState()
      expect(currentState?.eventLogPosition).toEqual({
        segmentId: 'segment-2',
        offset: 250,
      })
    })

    it('should create new branch at current HEAD and preserve state', async () => {
      // Setup main
      const mainState = await createDatabaseStateWithObjects({ users: 'main-users' })
      const mainCommit = await createAndSaveCommit(mainState, 'Main commit')

      const refManager = createRefManager(storage)
      await refManager.setHead('main')
      await refManager.updateRef('main', mainCommit)
      await writeWorkingState({ users: 'main-users' })

      // Create and checkout new branch
      await branchManager.checkout('new-feature', { create: true })

      // Verify state is preserved (same as main)
      const usersData = await storage.read(StoragePaths.data('users'))
      expect(new TextDecoder().decode(usersData)).toBe('main-users')

      // Verify branch points to same commit
      const newBranchCommit = await refManager.resolveRef('new-feature')
      expect(newBranchCommit).toBe(mainCommit)
    })
  })

  describe('error handling', () => {
    it('should throw when object is missing from store', async () => {
      // Create state referencing a non-existent object
      const commit = await createCommit(
        {
          collections: {
            users: {
              dataHash: 'nonexistent-hash',
              schemaHash: 'nonexistent-schema',
              rowCount: 10,
            },
          },
          relationships: {
            forwardHash: 'nonexistent-forward',
            reverseHash: 'nonexistent-reverse',
          },
          eventLogPosition: { segmentId: 'seg-0', offset: 0 },
        },
        { message: 'Bad commit' }
      )
      await saveCommit(storage, commit)

      const refManager = createRefManager(storage)
      await refManager.updateRef('bad-branch', commit.hash)

      await expect(branchManager.checkout('bad-branch')).rejects.toThrow(
        /object not found|missing object/i
      )
    })

    it('should throw descriptive error when branch commit is missing', async () => {
      const refManager = createRefManager(storage)
      await refManager.updateRef('orphan', 'missing-commit-hash')

      await expect(branchManager.checkout('orphan')).rejects.toThrow(/commit.*not found/i)
    })
  })
})
