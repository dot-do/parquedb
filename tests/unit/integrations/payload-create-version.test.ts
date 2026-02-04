/**
 * Unit tests for Payload create version operations
 * Verifies the N+1 query fix: uses updateMany instead of individual updates
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { createVersion, createGlobalVersion } from '../../../src/integrations/payload/operations/create'
import type { ResolvedAdapterConfig } from '../../../src/integrations/payload/types'
import type { EntityId } from '../../../src/types'

// Mock collection with updateMany
function createMockCollection() {
  return {
    updateMany: vi.fn().mockResolvedValue({ matchedCount: 0, modifiedCount: 0 }),
  }
}

// Mock db
function createMockDB() {
  const mockCollection = createMockCollection()

  return {
    db: {
      find: vi.fn().mockResolvedValue({ items: [] }),
      update: vi.fn(),
      create: vi.fn().mockResolvedValue({
        $id: 'posts_versions/ver-001',
        $type: 'PostsVersion',
        name: 'Version of parent-123',
        createdAt: new Date('2026-01-01'),
        updatedAt: new Date('2026-01-01'),
        version: 1,
      }),
      collection: vi.fn().mockReturnValue(mockCollection),
    },
    mockCollection,
  }
}

const defaultConfig: ResolvedAdapterConfig = {
  storage: {} as any,
  migrationCollection: 'payload_migrations',
  globalsCollection: 'payload_globals',
  versionsSuffix: '_versions',
  defaultActor: 'system/payload' as EntityId,
  debug: false,
}

describe('createVersion - N+1 fix', () => {
  it('calls collection.updateMany instead of individual updates', async () => {
    const { db, mockCollection } = createMockDB()

    await createVersion(db as any, defaultConfig, {
      collection: 'posts',
      parent: 'parent-123',
      versionData: { title: 'Version 1' },
    })

    // Should use collection().updateMany() for batch update
    expect(db.collection).toHaveBeenCalledWith('posts_versions')
    expect(mockCollection.updateMany).toHaveBeenCalledWith(
      { parent: 'parent-123', latest: true },
      { $set: { latest: false } },
      { actor: 'system/payload' }
    )

    // Should NOT use individual db.update calls
    expect(db.update).not.toHaveBeenCalled()
  })

  it('does not use db.find + db.update N+1 pattern', async () => {
    const { db, mockCollection } = createMockDB()

    // Even if there were existing versions, we should not call db.find for versions
    await createVersion(db as any, defaultConfig, {
      collection: 'posts',
      parent: 'parent-123',
      versionData: { title: 'Version 2' },
    })

    // db.find should NOT be called (the old N+1 code called db.find first)
    expect(db.find).not.toHaveBeenCalled()

    // updateMany handles the find+update in one call
    expect(mockCollection.updateMany).toHaveBeenCalledTimes(1)
  })

  it('creates the version entity after marking old versions', async () => {
    const { db, mockCollection } = createMockDB()

    const result = await createVersion(db as any, defaultConfig, {
      collection: 'posts',
      parent: 'parent-123',
      versionData: { title: 'New Version' },
    })

    // Should create the new version
    expect(db.create).toHaveBeenCalledWith(
      'posts_versions',
      expect.objectContaining({
        $type: 'PostsVersion',
      }),
      { actor: 'system/payload' }
    )

    expect(result.latest).toBe(true)
    expect(result.parent).toBe('parent-123')
    expect(result.version).toEqual({ title: 'New Version' })
  })
})

describe('createGlobalVersion - N+1 fix', () => {
  it('calls collection.updateMany instead of individual updates', async () => {
    const { db, mockCollection } = createMockDB()

    // Override create to return global version entity
    db.create.mockResolvedValue({
      $id: 'payload_globals_versions/ver-001',
      $type: 'GlobalVersion',
      name: 'Global version of settings',
      createdAt: new Date('2026-01-01'),
      updatedAt: new Date('2026-01-01'),
      version: 1,
    })

    await createGlobalVersion(db as any, defaultConfig, {
      slug: 'settings',
      parent: 'global-123',
      versionData: { siteName: 'My Site' },
    })

    // Should use collection().updateMany() for batch update
    expect(db.collection).toHaveBeenCalledWith('payload_globals_versions')
    expect(mockCollection.updateMany).toHaveBeenCalledWith(
      { globalSlug: 'settings', latest: true },
      { $set: { latest: false } },
      { actor: 'system/payload' }
    )

    // Should NOT use individual db.update calls
    expect(db.update).not.toHaveBeenCalled()
  })

  it('does not use db.find + db.update N+1 pattern', async () => {
    const { db, mockCollection } = createMockDB()

    db.create.mockResolvedValue({
      $id: 'payload_globals_versions/ver-001',
      $type: 'GlobalVersion',
      name: 'Global version of settings',
      createdAt: new Date('2026-01-01'),
      updatedAt: new Date('2026-01-01'),
      version: 1,
    })

    await createGlobalVersion(db as any, defaultConfig, {
      slug: 'settings',
      parent: 'global-123',
      versionData: { siteName: 'Updated Site' },
    })

    // db.find should NOT be called
    expect(db.find).not.toHaveBeenCalled()

    // updateMany handles it in one call
    expect(mockCollection.updateMany).toHaveBeenCalledTimes(1)
  })
})
