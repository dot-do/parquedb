import { describe, it, expect, beforeEach } from 'vitest'
import { RefManager, createRefManager } from '../../../src/sync/refs'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'

describe('refs', () => {
  let storage: MemoryBackend
  let refManager: RefManager

  beforeEach(() => {
    storage = new MemoryBackend()
    refManager = createRefManager(storage)
  })

  describe('createRefManager', () => {
    it('should create a RefManager instance', () => {
      expect(refManager).toBeInstanceOf(RefManager)
    })
  })

  describe('resolveRef', () => {
    it('should resolve a branch ref', async () => {
      const hash = 'abc123'
      await storage.write('_meta/refs/heads/main', new TextEncoder().encode(hash))

      const resolved = await refManager.resolveRef('refs/heads/main')
      expect(resolved).toBe(hash)
    })

    it('should resolve short branch name', async () => {
      const hash = 'abc123'
      await storage.write('_meta/refs/heads/main', new TextEncoder().encode(hash))

      const resolved = await refManager.resolveRef('main')
      expect(resolved).toBe(hash)
    })

    it('should resolve a tag ref', async () => {
      const hash = 'def456'
      await storage.write('_meta/refs/tags/v1.0.0', new TextEncoder().encode(hash))

      const resolved = await refManager.resolveRef('refs/tags/v1.0.0')
      expect(resolved).toBe(hash)
    })

    it('should resolve short tag name', async () => {
      const hash = 'def456'
      await storage.write('_meta/refs/tags/v1.0.0', new TextEncoder().encode(hash))

      const resolved = await refManager.resolveRef('v1.0.0')
      expect(resolved).toBe(hash)
    })

    it('should return null for non-existent ref', async () => {
      const resolved = await refManager.resolveRef('nonexistent')
      expect(resolved).toBeNull()
    })

    it('should resolve HEAD when pointing to branch', async () => {
      const hash = 'abc123'
      await storage.write('_meta/HEAD', new TextEncoder().encode('refs/heads/main'))
      await storage.write('_meta/refs/heads/main', new TextEncoder().encode(hash))

      const resolved = await refManager.resolveRef('HEAD')
      expect(resolved).toBe(hash)
    })

    it('should resolve HEAD when detached', async () => {
      const hash = 'abc123'
      await storage.write('_meta/HEAD', new TextEncoder().encode(hash))

      const resolved = await refManager.resolveRef('HEAD')
      expect(resolved).toBe(hash)
    })

    it('should handle recursive ref resolution', async () => {
      const hash = 'abc123'
      // This shouldn't happen in practice, but test the recursion
      await storage.write('_meta/refs/heads/alias', new TextEncoder().encode('refs/heads/main'))
      await storage.write('_meta/refs/heads/main', new TextEncoder().encode(hash))

      const resolved = await refManager.resolveRef('refs/heads/alias')
      expect(resolved).toBe(hash)
    })
  })

  describe('updateRef', () => {
    it('should update a branch ref', async () => {
      const hash = 'abc123'
      await refManager.updateRef('refs/heads/main', hash)

      const data = await storage.read('_meta/refs/heads/main')
      const content = new TextDecoder().decode(data)
      expect(content).toBe(hash)
    })

    it('should update a tag ref', async () => {
      const hash = 'def456'
      await refManager.updateRef('refs/tags/v1.0.0', hash)

      const data = await storage.read('_meta/refs/tags/v1.0.0')
      const content = new TextDecoder().decode(data)
      expect(content).toBe(hash)
    })

    it('should update using short branch name', async () => {
      const hash = 'abc123'
      await refManager.updateRef('main', hash)

      const data = await storage.read('_meta/refs/heads/main')
      const content = new TextDecoder().decode(data)
      expect(content).toBe(hash)
    })

    it('should update using short tag name', async () => {
      const hash = 'def456'
      await refManager.updateRef('v1.0.0', hash)

      const data = await storage.read('_meta/refs/tags/v1.0.0')
      const content = new TextDecoder().decode(data)
      expect(content).toBe(hash)
    })

    it('should overwrite existing ref', async () => {
      await refManager.updateRef('main', 'old-hash')
      await refManager.updateRef('main', 'new-hash')

      const resolved = await refManager.resolveRef('main')
      expect(resolved).toBe('new-hash')
    })

    it('should throw when trying to update HEAD directly', async () => {
      await expect(refManager.updateRef('HEAD', 'abc123')).rejects.toThrow(
        'Cannot directly update HEAD'
      )
    })
  })

  describe('deleteRef', () => {
    it('should delete a branch ref', async () => {
      await refManager.updateRef('main', 'abc123')
      await refManager.deleteRef('main')

      const resolved = await refManager.resolveRef('main')
      expect(resolved).toBeNull()
    })

    it('should delete a tag ref', async () => {
      await refManager.updateRef('v1.0.0', 'abc123')
      await refManager.deleteRef('v1.0.0')

      const resolved = await refManager.resolveRef('v1.0.0')
      expect(resolved).toBeNull()
    })

    it('should throw when deleting non-existent ref', async () => {
      await expect(refManager.deleteRef('nonexistent')).rejects.toThrow('Ref not found')
    })

    it('should throw when trying to delete HEAD', async () => {
      await expect(refManager.deleteRef('HEAD')).rejects.toThrow('Cannot delete HEAD')
    })
  })

  describe('listRefs', () => {
    beforeEach(async () => {
      // Create some test refs
      await refManager.updateRef('main', 'hash1')
      await refManager.updateRef('develop', 'hash2')
      await refManager.updateRef('v1.0.0', 'hash3')
    })

    it('should list all heads', async () => {
      const heads = await refManager.listRefs('heads')
      expect(heads).toContain('refs/heads/main')
      expect(heads).not.toContain('refs/tags/v1.0.0')
    })

    it('should list all tags', async () => {
      const tags = await refManager.listRefs('tags')
      expect(tags).toContain('refs/tags/v1.0.0')
      expect(tags).not.toContain('refs/heads/main')
    })

    it('should list all refs when type not specified', async () => {
      const all = await refManager.listRefs()
      expect(all.length).toBeGreaterThanOrEqual(2)
      expect(all.some(ref => ref.startsWith('refs/heads/'))).toBe(true)
    })

    it('should return empty array when no refs exist', async () => {
      const storage2 = new MemoryBackend()
      const refManager2 = createRefManager(storage2)
      const refs = await refManager2.listRefs()
      expect(refs).toEqual([])
    })
  })

  describe('getHead', () => {
    it('should return default when HEAD does not exist', async () => {
      const head = await refManager.getHead()
      expect(head).toEqual({ type: 'branch', ref: 'main' })
    })

    it('should return branch when HEAD points to branch', async () => {
      await storage.write('_meta/HEAD', new TextEncoder().encode('refs/heads/develop'))

      const head = await refManager.getHead()
      expect(head).toEqual({ type: 'branch', ref: 'develop' })
    })

    it('should return detached when HEAD points to commit', async () => {
      const hash = 'abc123'
      await storage.write('_meta/HEAD', new TextEncoder().encode(hash))

      const head = await refManager.getHead()
      expect(head).toEqual({ type: 'detached', ref: hash })
    })

    it('should handle HEAD with whitespace', async () => {
      await storage.write('_meta/HEAD', new TextEncoder().encode('refs/heads/main\n'))

      const head = await refManager.getHead()
      expect(head).toEqual({ type: 'branch', ref: 'main' })
    })
  })

  describe('setHead', () => {
    it('should set HEAD to a branch', async () => {
      await refManager.setHead('main')

      const data = await storage.read('_meta/HEAD')
      const content = new TextDecoder().decode(data)
      expect(content).toBe('refs/heads/main')
    })

    it('should change HEAD to different branch', async () => {
      await refManager.setHead('main')
      await refManager.setHead('develop')

      const head = await refManager.getHead()
      expect(head).toEqual({ type: 'branch', ref: 'develop' })
    })
  })

  describe('detachHead', () => {
    it('should detach HEAD to a commit', async () => {
      const hash = 'abc123'
      await refManager.detachHead(hash)

      const data = await storage.read('_meta/HEAD')
      const content = new TextDecoder().decode(data)
      expect(content).toBe(hash)
    })

    it('should change from branch to detached', async () => {
      await refManager.setHead('main')
      await refManager.detachHead('abc123')

      const head = await refManager.getHead()
      expect(head).toEqual({ type: 'detached', ref: 'abc123' })
    })
  })

  describe('integration scenarios', () => {
    it('should support typical workflow: create branch, commit, switch', async () => {
      // Initial commit on main
      const commit1 = 'hash1'
      await refManager.setHead('main')
      await refManager.updateRef('main', commit1)

      // Verify HEAD points to main
      let head = await refManager.getHead()
      expect(head).toEqual({ type: 'branch', ref: 'main' })

      // Verify main points to commit
      let resolved = await refManager.resolveRef('HEAD')
      expect(resolved).toBe(commit1)

      // Create develop branch
      await refManager.updateRef('develop', commit1)

      // Switch to develop
      await refManager.setHead('develop')
      head = await refManager.getHead()
      expect(head).toEqual({ type: 'branch', ref: 'develop' })

      // Make commit on develop
      const commit2 = 'hash2'
      await refManager.updateRef('develop', commit2)

      // Verify develop points to new commit
      resolved = await refManager.resolveRef('HEAD')
      expect(resolved).toBe(commit2)

      // Verify main still points to old commit
      resolved = await refManager.resolveRef('main')
      expect(resolved).toBe(commit1)
    })

    it('should support detached HEAD workflow', async () => {
      // Setup initial state
      await refManager.setHead('main')
      await refManager.updateRef('main', 'hash1')
      await refManager.updateRef('main', 'hash2')

      // Checkout old commit (detach HEAD)
      await refManager.detachHead('hash1')

      const head = await refManager.getHead()
      expect(head).toEqual({ type: 'detached', ref: 'hash1' })

      // Re-attach to branch
      await refManager.setHead('main')
      const resolved = await refManager.resolveRef('HEAD')
      expect(resolved).toBe('hash2')
    })

    it('should support tagging workflow', async () => {
      // Make commits
      await refManager.setHead('main')
      await refManager.updateRef('main', 'hash1')

      // Create release tag
      await refManager.updateRef('v1.0.0', 'hash1')

      // Continue development
      await refManager.updateRef('main', 'hash2')

      // Tag should still point to old commit
      const tagResolved = await refManager.resolveRef('v1.0.0')
      expect(tagResolved).toBe('hash1')

      const mainResolved = await refManager.resolveRef('main')
      expect(mainResolved).toBe('hash2')
    })
  })
})
