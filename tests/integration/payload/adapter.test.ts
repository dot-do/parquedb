/**
 * Tests for Payload CMS adapter factory and configuration
 */

import { describe, it, expect } from 'vitest'
import { parquedbAdapter, PayloadAdapter } from '../../../src/integrations/payload'
import { MemoryBackend } from '../../../src/storage'
import type { EntityId } from '../../../src/types'

describe('parquedbAdapter factory', () => {
  it('returns adapter configuration object', () => {
    const storage = new MemoryBackend()
    const result = parquedbAdapter({ storage })

    expect(result.name).toBe('parquedb')
    expect(result.defaultIDType).toBe('text')
    expect(result.allowIDOnCreate).toBe(true)
    expect(typeof result.init).toBe('function')
  })

  it('init creates PayloadAdapter instance', () => {
    const storage = new MemoryBackend()
    const adapterConfig = parquedbAdapter({ storage })

    const adapter = adapterConfig.init({ payload: {} })

    expect(adapter).toBeInstanceOf(PayloadAdapter)
    expect(adapter.payload).toEqual({})
  })
})

describe('PayloadAdapter', () => {
  describe('configuration', () => {
    it('uses default configuration values', () => {
      const adapter = new PayloadAdapter({
        storage: new MemoryBackend(),
      })

      const config = adapter.getConfig()

      expect(config.migrationCollection).toBe('payload_migrations')
      expect(config.globalsCollection).toBe('payload_globals')
      expect(config.versionsSuffix).toBe('_versions')
      expect(config.defaultActor).toBe('system/payload')
      expect(config.debug).toBe(false)
    })

    it('accepts custom configuration values', () => {
      const adapter = new PayloadAdapter({
        storage: new MemoryBackend(),
        migrationCollection: 'my_migrations',
        globalsCollection: 'my_globals',
        versionsSuffix: '_history',
        defaultActor: 'custom/actor' as EntityId,
        debug: true,
      })

      const config = adapter.getConfig()

      expect(config.migrationCollection).toBe('my_migrations')
      expect(config.globalsCollection).toBe('my_globals')
      expect(config.versionsSuffix).toBe('_history')
      expect(config.defaultActor).toBe('custom/actor')
      expect(config.debug).toBe(true)
    })
  })

  describe('properties', () => {
    it('has correct static properties', () => {
      const adapter = new PayloadAdapter({
        storage: new MemoryBackend(),
      })

      expect(adapter.name).toBe('parquedb')
      expect(adapter.packageName).toBe('parquedb')
      expect(adapter.defaultIDType).toBe('text')
      expect(adapter.allowIDOnCreate).toBe(true)
      expect(adapter.bulkOperationsSingleTransaction).toBe(false)
      expect(adapter.migrationDir).toBe('./migrations')
    })
  })

  describe('lifecycle', () => {
    it('init completes without error', async () => {
      const adapter = new PayloadAdapter({
        storage: new MemoryBackend(),
      })

      await expect(adapter.init()).resolves.not.toThrow()
    })

    it('connect completes without error', async () => {
      const adapter = new PayloadAdapter({
        storage: new MemoryBackend(),
      })

      await expect(adapter.connect()).resolves.not.toThrow()
    })

    it('destroy completes without error', async () => {
      const adapter = new PayloadAdapter({
        storage: new MemoryBackend(),
      })

      await expect(adapter.destroy()).resolves.not.toThrow()
    })
  })

  describe('getDB', () => {
    it('returns ParqueDB instance', () => {
      const adapter = new PayloadAdapter({
        storage: new MemoryBackend(),
      })

      const db = adapter.getDB()
      expect(db).toBeDefined()
    })
  })

  describe('migration methods', () => {
    it('migrate completes without error', async () => {
      const adapter = new PayloadAdapter({
        storage: new MemoryBackend(),
      })

      await expect(adapter.migrate()).resolves.not.toThrow()
    })

    it('migrateDown completes without error', async () => {
      const adapter = new PayloadAdapter({
        storage: new MemoryBackend(),
      })

      await expect(adapter.migrateDown()).resolves.not.toThrow()
    })

    it('migrateFresh completes without error', async () => {
      const adapter = new PayloadAdapter({
        storage: new MemoryBackend(),
      })

      await expect(adapter.migrateFresh({})).resolves.not.toThrow()
    })

    it('migrateRefresh completes without error', async () => {
      const adapter = new PayloadAdapter({
        storage: new MemoryBackend(),
      })

      await expect(adapter.migrateRefresh()).resolves.not.toThrow()
    })

    it('migrateReset completes without error', async () => {
      const adapter = new PayloadAdapter({
        storage: new MemoryBackend(),
      })

      await expect(adapter.migrateReset()).resolves.not.toThrow()
    })
  })

  describe('findDistinct', () => {
    it('returns distinct values for a field', async () => {
      const adapter = new PayloadAdapter({
        storage: new MemoryBackend(),
      })

      await adapter.create({
        collection: 'posts',
        data: { title: 'Post 1', category: 'tech' },
      })
      await adapter.create({
        collection: 'posts',
        data: { title: 'Post 2', category: 'news' },
      })
      await adapter.create({
        collection: 'posts',
        data: { title: 'Post 3', category: 'tech' },
      })

      const distinct = await adapter.findDistinct({
        collection: 'posts',
        field: 'category',
      })

      expect(distinct).toHaveLength(2)
      expect(distinct).toContain('tech')
      expect(distinct).toContain('news')
    })
  })

  describe('updateJobs', () => {
    it('updates job documents', async () => {
      const adapter = new PayloadAdapter({
        storage: new MemoryBackend(),
      })

      await expect(
        adapter.updateJobs({
          input: [
            { id: 'job-1', data: { status: 'completed' } },
            { id: 'job-2', data: { status: 'processing' } },
          ],
        })
      ).resolves.not.toThrow()
    })
  })
})

describe('PayloadAdapter error handling', () => {
  it('handles missing document in updateOne gracefully', async () => {
    const adapter = new PayloadAdapter({
      storage: new MemoryBackend(),
    })

    const result = await adapter.updateOne({
      collection: 'posts',
      id: 'nonexistent',
      data: { title: 'Test' },
    })

    expect(result).toBeNull()
  })

  it('handles missing global in findGlobal gracefully', async () => {
    const adapter = new PayloadAdapter({
      storage: new MemoryBackend(),
    })

    const result = await adapter.findGlobal({
      slug: 'nonexistent',
    })

    expect(result).toBeNull()
  })
})
