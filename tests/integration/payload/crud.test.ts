/**
 * Tests for Payload CMS adapter CRUD operations
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { PayloadAdapter } from '../../../src/integrations/payload/adapter'
import { MemoryBackend } from '../../../src/storage'

describe('PayloadAdapter CRUD', () => {
  let adapter: PayloadAdapter

  beforeEach(() => {
    adapter = new PayloadAdapter({
      storage: new MemoryBackend(),
      debug: false,
    })
  })

  afterEach(async () => {
    // Cleanup adapter resources
    if (adapter && typeof adapter.destroy === 'function') {
      await adapter.destroy()
    }
  })

  describe('create', () => {
    it('creates a document and returns it', async () => {
      const doc = await adapter.create({
        collection: 'posts',
        data: {
          title: 'Hello World',
          content: 'This is my first post',
          status: 'published',
        },
      })

      expect(doc).toBeDefined()
      expect(doc.id).toBeDefined()
      expect(doc.title).toBe('Hello World')
      expect(doc.content).toBe('This is my first post')
      expect(doc.status).toBe('published')
    })

    it('creates a draft document', async () => {
      const doc = await adapter.create({
        collection: 'posts',
        data: { title: 'Draft Post' },
        draft: true,
      })

      expect(doc._status).toBe('draft')
    })
  })

  describe('find', () => {
    beforeEach(async () => {
      // Create test documents
      await adapter.create({
        collection: 'posts',
        data: { title: 'Post 1', views: 100, status: 'published' },
      })
      await adapter.create({
        collection: 'posts',
        data: { title: 'Post 2', views: 200, status: 'published' },
      })
      await adapter.create({
        collection: 'posts',
        data: { title: 'Post 3', views: 50, status: 'draft' },
        draft: true,
      })
    })

    it('finds all documents', async () => {
      const result = await adapter.find({ collection: 'posts' })

      expect(result.docs).toHaveLength(3)
      expect(result.totalDocs).toBe(3)
    })

    it('finds documents with filter', async () => {
      const result = await adapter.find({
        collection: 'posts',
        where: { status: { equals: 'published' } },
      })

      expect(result.docs).toHaveLength(2)
    })

    it('finds documents with comparison filter', async () => {
      const result = await adapter.find({
        collection: 'posts',
        where: { views: { greater_than: 75 } },
      })

      expect(result.docs).toHaveLength(2)
    })

    it('respects pagination', async () => {
      const result = await adapter.find({
        collection: 'posts',
        limit: 2,
        page: 1,
      })

      expect(result.docs).toHaveLength(2)
      expect(result.hasNextPage).toBe(true)
      expect(result.totalPages).toBe(2)
    })

    it('sorts results', async () => {
      const result = await adapter.find({
        collection: 'posts',
        sort: '-views',
      })

      expect(result.docs[0]?.views).toBe(200)
      expect(result.docs[1]?.views).toBe(100)
    })
  })

  describe('findOne', () => {
    it('finds a single document', async () => {
      const created = await adapter.create({
        collection: 'posts',
        data: { title: 'Unique Post', slug: 'unique-post' },
      })

      const found = await adapter.findOne({
        collection: 'posts',
        where: { slug: { equals: 'unique-post' } },
      })

      expect(found).toBeDefined()
      expect(found?.title).toBe('Unique Post')
    })

    it('returns null when not found', async () => {
      const found = await adapter.findOne({
        collection: 'posts',
        where: { slug: { equals: 'nonexistent' } },
      })

      expect(found).toBeNull()
    })
  })

  describe('updateOne', () => {
    it('updates a document', async () => {
      const created = await adapter.create({
        collection: 'posts',
        data: { title: 'Original Title', views: 0 },
      })

      const updated = await adapter.updateOne({
        collection: 'posts',
        id: created.id as string,
        data: { title: 'Updated Title', views: 100 },
      })

      expect(updated).toBeDefined()
      expect(updated?.title).toBe('Updated Title')
      expect(updated?.views).toBe(100)
    })

    it('returns null for non-existent document', async () => {
      const updated = await adapter.updateOne({
        collection: 'posts',
        id: 'nonexistent',
        data: { title: 'Test' },
      })

      expect(updated).toBeNull()
    })
  })

  describe('updateMany', () => {
    beforeEach(async () => {
      await adapter.create({
        collection: 'posts',
        data: { title: 'Post 1', status: 'draft' },
      })
      await adapter.create({
        collection: 'posts',
        data: { title: 'Post 2', status: 'draft' },
      })
    })

    it('updates multiple documents', async () => {
      const result = await adapter.updateMany({
        collection: 'posts',
        where: { status: { equals: 'draft' } },
        data: { status: 'published' },
      })

      expect(result.docs).toHaveLength(2)
      expect(result.errors).toHaveLength(0)

      // Verify updates
      const published = await adapter.find({
        collection: 'posts',
        where: { status: { equals: 'published' } },
      })
      expect(published.docs).toHaveLength(2)
    })
  })

  describe('deleteOne', () => {
    it('deletes a document', async () => {
      const created = await adapter.create({
        collection: 'posts',
        data: { title: 'To Delete' },
      })

      const result = await adapter.deleteOne({
        collection: 'posts',
        id: created.id as string,
      })

      expect(result.docs).toHaveLength(1)
      expect(result.errors).toHaveLength(0)

      // Verify deletion
      const found = await adapter.findOne({
        collection: 'posts',
        where: { id: { equals: created.id } },
      })
      expect(found).toBeNull()
    })
  })

  describe('deleteMany', () => {
    beforeEach(async () => {
      await adapter.create({
        collection: 'posts',
        data: { title: 'Keep', category: 'important' },
      })
      await adapter.create({
        collection: 'posts',
        data: { title: 'Delete 1', category: 'temp' },
      })
      await adapter.create({
        collection: 'posts',
        data: { title: 'Delete 2', category: 'temp' },
      })
    })

    it('deletes multiple documents', async () => {
      const result = await adapter.deleteMany({
        collection: 'posts',
        where: { category: { equals: 'temp' } },
      })

      expect(result.docs).toHaveLength(2)
      expect(result.errors).toHaveLength(0)

      // Verify remaining
      const remaining = await adapter.find({ collection: 'posts' })
      expect(remaining.docs).toHaveLength(1)
      expect(remaining.docs[0]?.title).toBe('Keep')
    })
  })

  describe('count', () => {
    beforeEach(async () => {
      await adapter.create({
        collection: 'posts',
        data: { title: 'Post 1', status: 'published' },
      })
      await adapter.create({
        collection: 'posts',
        data: { title: 'Post 2', status: 'published' },
      })
      await adapter.create({
        collection: 'posts',
        data: { title: 'Post 3', status: 'draft' },
      })
    })

    it('counts all documents', async () => {
      const count = await adapter.count({ collection: 'posts' })
      expect(count).toBe(3)
    })

    it('counts with filter', async () => {
      const count = await adapter.count({
        collection: 'posts',
        where: { status: { equals: 'published' } },
      })
      expect(count).toBe(2)
    })
  })

  describe('upsert', () => {
    it('creates when document does not exist', async () => {
      const result = await adapter.upsert({
        collection: 'posts',
        where: { slug: { equals: 'new-post' } },
        data: { title: 'New Post', slug: 'new-post' },
      })

      expect(result).toBeDefined()
      expect(result?.title).toBe('New Post')
    })

    it('updates when document exists', async () => {
      await adapter.create({
        collection: 'posts',
        data: { title: 'Original', slug: 'existing-post' },
      })

      const result = await adapter.upsert({
        collection: 'posts',
        where: { slug: { equals: 'existing-post' } },
        data: { title: 'Updated', slug: 'existing-post' },
      })

      expect(result?.title).toBe('Updated')

      // Should still be only one document
      const all = await adapter.find({
        collection: 'posts',
        where: { slug: { equals: 'existing-post' } },
      })
      expect(all.docs).toHaveLength(1)
    })
  })

  describe('queryDrafts', () => {
    beforeEach(async () => {
      await adapter.create({
        collection: 'posts',
        data: { title: 'Published Post' },
      })
      await adapter.create({
        collection: 'posts',
        data: { title: 'Draft 1' },
        draft: true,
      })
      await adapter.create({
        collection: 'posts',
        data: { title: 'Draft 2' },
        draft: true,
      })
    })

    it('returns only draft documents', async () => {
      const result = await adapter.queryDrafts({
        collection: 'posts',
      })

      expect(result.docs).toHaveLength(2)
      for (const doc of result.docs) {
        expect(doc._status).toBe('draft')
      }
    })
  })
})

describe('PayloadAdapter Transactions', () => {
  let adapter: PayloadAdapter

  beforeEach(() => {
    adapter = new PayloadAdapter({
      storage: new MemoryBackend(),
    })
  })

  it('begins and commits transaction', async () => {
    const txId = await adapter.beginTransaction()
    expect(txId).toBeDefined()

    await adapter.commitTransaction(txId)
  })

  it('begins and rolls back transaction', async () => {
    const txId = await adapter.beginTransaction()
    expect(txId).toBeDefined()

    await adapter.rollbackTransaction(txId)
  })
})

describe('PayloadAdapter Globals', () => {
  let adapter: PayloadAdapter

  beforeEach(() => {
    adapter = new PayloadAdapter({
      storage: new MemoryBackend(),
    })
  })

  it('creates a global', async () => {
    const global = await adapter.createGlobal({
      slug: 'settings',
      data: { siteName: 'My Site', theme: 'dark' },
    })

    expect(global).toBeDefined()
    expect(global.siteName).toBe('My Site')
    expect(global.theme).toBe('dark')
  })

  it('finds a global', async () => {
    await adapter.createGlobal({
      slug: 'settings',
      data: { siteName: 'My Site' },
    })

    const found = await adapter.findGlobal({
      slug: 'settings',
    })

    expect(found).toBeDefined()
    expect(found?.siteName).toBe('My Site')
  })

  it('updates a global', async () => {
    await adapter.createGlobal({
      slug: 'settings',
      data: { siteName: 'Original' },
    })

    const updated = await adapter.updateGlobal({
      slug: 'settings',
      data: { siteName: 'Updated' },
    })

    expect(updated?.siteName).toBe('Updated')
  })
})

describe('PayloadAdapter Versions', () => {
  let adapter: PayloadAdapter

  beforeEach(() => {
    adapter = new PayloadAdapter({
      storage: new MemoryBackend(),
    })
  })

  it('creates a version', async () => {
    const doc = await adapter.create({
      collection: 'posts',
      data: { title: 'Original' },
    })

    const version = await adapter.createVersion({
      collection: 'posts',
      parent: doc.id as string,
      versionData: { title: 'Version 1' },
    })

    expect(version).toBeDefined()
    expect(version.parent).toBe(doc.id)
    expect(version.latest).toBe(true)
  })

  it('finds versions', async () => {
    const doc = await adapter.create({
      collection: 'posts',
      data: { title: 'Original' },
    })

    await adapter.createVersion({
      collection: 'posts',
      parent: doc.id as string,
      versionData: { title: 'Version 1' },
    })

    await adapter.createVersion({
      collection: 'posts',
      parent: doc.id as string,
      versionData: { title: 'Version 2' },
    })

    const versions = await adapter.findVersions({
      collection: 'posts',
    })

    expect(versions.docs).toHaveLength(2)
  })

  it('marks only latest version as latest', async () => {
    const doc = await adapter.create({
      collection: 'posts',
      data: { title: 'Original' },
    })

    await adapter.createVersion({
      collection: 'posts',
      parent: doc.id as string,
      versionData: { title: 'Version 1' },
    })

    await adapter.createVersion({
      collection: 'posts',
      parent: doc.id as string,
      versionData: { title: 'Version 2' },
    })

    const versions = await adapter.findVersions({
      collection: 'posts',
      where: { parent: { equals: doc.id } },
    })

    const latestVersions = versions.docs.filter((v: any) => v.latest)
    expect(latestVersions).toHaveLength(1)
    // The version data is stored in the 'version' field as an object
    const latestVersion = latestVersions[0] as any
    expect(latestVersion.version).toBeDefined()
    expect(latestVersion.version.title).toBe('Version 2')
  })
})
