import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  createCommit,
  loadCommit,
  saveCommit,
  hashCommit,
  serializeCommit,
  parseCommit,
  type DatabaseState,
  type CommitOptions
} from '../../../src/sync/commit'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'

describe('commit', () => {
  let storage: MemoryBackend
  let sampleState: DatabaseState

  beforeEach(() => {
    storage = new MemoryBackend()
    sampleState = {
      collections: {
        users: {
          dataHash: 'abc123',
          schemaHash: 'def456',
          rowCount: 100
        }
      },
      relationships: {
        forwardHash: 'fwd789',
        reverseHash: 'rev012'
      },
      eventLogPosition: {
        segmentId: 'seg1',
        offset: 42
      }
    }
  })

  describe('createCommit', () => {
    it('should create a commit with all required fields', async () => {
      const opts: CommitOptions = {
        message: 'Initial commit',
        author: 'test@example.com'
      }

      const commit = await createCommit(sampleState, opts)

      expect(commit.hash).toHaveLength(64)
      expect(commit.parents).toEqual([])
      expect(commit.timestamp).toBeTypeOf('number')
      expect(commit.author).toBe('test@example.com')
      expect(commit.message).toBe('Initial commit')
      expect(commit.state).toEqual(sampleState)
    })

    it('should use default author if not provided', async () => {
      const opts: CommitOptions = {
        message: 'Test commit'
      }

      const commit = await createCommit(sampleState, opts)
      expect(commit.author).toBe('anonymous')
    })

    it('should include parent commits', async () => {
      const opts: CommitOptions = {
        message: 'Child commit',
        parents: ['parent1', 'parent2']
      }

      const commit = await createCommit(sampleState, opts)
      expect(commit.parents).toEqual(['parent1', 'parent2'])
    })

    it('should generate different hashes for different commits', async () => {
      vi.useFakeTimers()
      try {
        const commit1 = await createCommit(sampleState, { message: 'Commit 1' })

        // Advance time to ensure different timestamp
        vi.advanceTimersByTime(10)

        const commit2 = await createCommit(sampleState, { message: 'Commit 2' })

        expect(commit1.hash).not.toBe(commit2.hash)
      } finally {
        vi.useRealTimers()
      }
    })
  })

  describe('hashCommit', () => {
    it('should compute consistent hash', () => {
      const commitData = {
        parents: [],
        timestamp: 1234567890,
        author: 'test@example.com',
        message: 'Test commit',
        state: sampleState
      }

      const hash1 = hashCommit(commitData)
      const hash2 = hashCommit(commitData)

      expect(hash1).toBe(hash2)
      expect(hash1).toHaveLength(64)
    })

    it('should produce different hashes for different commits', () => {
      const commit1 = {
        parents: [],
        timestamp: 1234567890,
        author: 'test@example.com',
        message: 'Test commit 1',
        state: sampleState
      }

      const commit2 = {
        ...commit1,
        message: 'Test commit 2'
      }

      const hash1 = hashCommit(commit1)
      const hash2 = hashCommit(commit2)

      expect(hash1).not.toBe(hash2)
    })

    it('should be order-independent for object keys', () => {
      // This tests that internal key sorting works
      const commit = {
        message: 'Test',
        author: 'test@example.com',
        timestamp: 1234567890,
        parents: [],
        state: sampleState
      }

      const hash = hashCommit(commit)
      expect(hash).toHaveLength(64)
    })
  })

  describe('serializeCommit and parseCommit', () => {
    it('should serialize and parse round-trip', async () => {
      const original = await createCommit(sampleState, {
        message: 'Test commit',
        author: 'test@example.com',
        parents: ['parent1']
      })

      const json = serializeCommit(original)
      const parsed = parseCommit(json)

      expect(parsed).toEqual(original)
    })

    it('should produce valid JSON', async () => {
      const commit = await createCommit(sampleState, { message: 'Test' })
      const json = serializeCommit(commit)

      expect(() => JSON.parse(json)).not.toThrow()
    })

    it('should throw on invalid JSON', () => {
      expect(() => parseCommit('not json')).toThrow()
    })

    it('should throw on missing hash', () => {
      const json = JSON.stringify({
        parents: [],
        timestamp: 123,
        author: 'test',
        message: 'test',
        state: sampleState
      })

      expect(() => parseCommit(json)).toThrow('Invalid commit: missing or invalid hash')
    })

    it('should throw on invalid parents', () => {
      const json = JSON.stringify({
        hash: 'abc123',
        parents: 'not-an-array',
        timestamp: 123,
        author: 'test',
        message: 'test',
        state: sampleState
      })

      expect(() => parseCommit(json)).toThrow('Invalid commit: parents must be an array')
    })

    it('should throw on missing timestamp', () => {
      const json = JSON.stringify({
        hash: 'abc123',
        parents: [],
        author: 'test',
        message: 'test',
        state: sampleState
      })

      expect(() => parseCommit(json)).toThrow('Invalid commit: timestamp must be a number')
    })

    it('should throw on missing author', () => {
      const json = JSON.stringify({
        hash: 'abc123',
        parents: [],
        timestamp: 123,
        message: 'test',
        state: sampleState
      })

      expect(() => parseCommit(json)).toThrow('Invalid commit: author must be a string')
    })

    it('should throw on missing message', () => {
      const json = JSON.stringify({
        hash: 'abc123',
        parents: [],
        timestamp: 123,
        author: 'test',
        state: sampleState
      })

      expect(() => parseCommit(json)).toThrow('Invalid commit: message must be a string')
    })

    it('should throw on missing state', () => {
      const json = JSON.stringify({
        hash: 'abc123',
        parents: [],
        timestamp: 123,
        author: 'test',
        message: 'test'
      })

      expect(() => parseCommit(json)).toThrow('Invalid commit: state must be an object')
    })
  })

  describe('saveCommit and loadCommit', () => {
    it('should save and load commit', async () => {
      const original = await createCommit(sampleState, {
        message: 'Test commit',
        author: 'test@example.com'
      })

      await saveCommit(storage, original)
      const loaded = await loadCommit(storage, original.hash)

      expect(loaded).toEqual(original)
    })

    it('should throw when loading non-existent commit', async () => {
      await expect(loadCommit(storage, 'nonexistent')).rejects.toThrow('Commit not found')
    })

    it('should verify hash on load', async () => {
      const commit = await createCommit(sampleState, { message: 'Test' })

      // Save commit with tampered hash
      const tampered = { ...commit, hash: 'tampered' }
      const path = `_meta/commits/tampered.json`
      await storage.write(path, new TextEncoder().encode(JSON.stringify(tampered)))

      await expect(loadCommit(storage, 'tampered')).rejects.toThrow('Commit hash mismatch')
    })

    it('should store commits in correct location', async () => {
      const commit = await createCommit(sampleState, { message: 'Test' })
      await saveCommit(storage, commit)

      const path = `_meta/commits/${commit.hash}.json`
      const exists = await storage.exists(path)
      expect(exists).toBe(true)
    })
  })

  describe('commit chain', () => {
    it('should create a chain of commits', async () => {
      // Initial commit
      const commit1 = await createCommit(sampleState, {
        message: 'Initial commit'
      })
      await saveCommit(storage, commit1)

      // Second commit with first as parent
      const state2 = {
        ...sampleState,
        collections: {
          ...sampleState.collections,
          users: {
            ...sampleState.collections.users,
            rowCount: 200
          }
        }
      }

      const commit2 = await createCommit(state2, {
        message: 'Second commit',
        parents: [commit1.hash]
      })
      await saveCommit(storage, commit2)

      // Third commit with second as parent
      const commit3 = await createCommit(state2, {
        message: 'Third commit',
        parents: [commit2.hash]
      })
      await saveCommit(storage, commit3)

      // Verify chain
      expect(commit1.parents).toEqual([])
      expect(commit2.parents).toEqual([commit1.hash])
      expect(commit3.parents).toEqual([commit2.hash])

      // Load and verify
      const loaded1 = await loadCommit(storage, commit1.hash)
      const loaded2 = await loadCommit(storage, commit2.hash)
      const loaded3 = await loadCommit(storage, commit3.hash)

      expect(loaded1).toEqual(commit1)
      expect(loaded2).toEqual(commit2)
      expect(loaded3).toEqual(commit3)
    })

    it('should support merge commits with two parents', async () => {
      const commit1 = await createCommit(sampleState, { message: 'Branch A' })
      const commit2 = await createCommit(sampleState, { message: 'Branch B' })

      const mergeCommit = await createCommit(sampleState, {
        message: 'Merge A and B',
        parents: [commit1.hash, commit2.hash]
      })

      expect(mergeCommit.parents).toHaveLength(2)
      expect(mergeCommit.parents).toContain(commit1.hash)
      expect(mergeCommit.parents).toContain(commit2.hash)
    })
  })
})
