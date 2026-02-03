/**
 * Tests for src/studio/metadata.ts
 *
 * Tests UI metadata management for collections.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  createDefaultMetadata,
  generateCollectionMetadata,
  mergeMetadata,
  updateCollectionMetadata,
  updateFieldMetadata,
  validateMetadata,
} from '../../../src/studio/metadata'
import type {
  StudioMetadata,
  DiscoveredCollection,
  DiscoveredField,
  CollectionUIMetadata,
} from '../../../src/studio/types'

describe('metadata', () => {
  describe('createDefaultMetadata', () => {
    it('creates metadata with correct version', () => {
      const result = createDefaultMetadata()

      expect(result.version).toBe('1.0')
    })

    it('creates metadata with empty collections', () => {
      const result = createDefaultMetadata()

      expect(result.collections).toEqual({})
    })
  })

  describe('generateCollectionMetadata', () => {
    const mockFields: DiscoveredField[] = [
      { name: '$id', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
      { name: 'name', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
      { name: 'email', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: true, isArray: false },
      { name: 'createdAt', parquetType: 'INT64', payloadType: 'date', optional: false, isArray: false },
      { name: 'updatedAt', parquetType: 'INT64', payloadType: 'date', optional: false, isArray: false },
    ]

    const mockCollection: DiscoveredCollection = {
      slug: 'users',
      label: 'Users',
      path: '.db/users/data.parquet',
      rowCount: 100,
      fileSize: 1024,
      fields: mockFields,
      isParqueDB: true,
    }

    it('generates label and labelSingular', () => {
      const result = generateCollectionMetadata(mockCollection)

      expect(result.label).toBe('Users')
      expect(result.labelSingular).toBe('User')
    })

    it('sets useAsTitle to name field', () => {
      const result = generateCollectionMetadata(mockCollection)

      expect(result.admin?.useAsTitle).toBe('name')
    })

    it('generates default columns', () => {
      const result = generateCollectionMetadata(mockCollection)

      expect(result.admin?.defaultColumns).toContain('$id')
      expect(result.admin?.defaultColumns).toContain('name')
    })

    it('generates field metadata for ParqueDB fields', () => {
      const result = generateCollectionMetadata(mockCollection)

      expect(result.fields?.['$id']?.label).toBe('ID')
      expect(result.fields?.['$id']?.readOnly).toBe(true)
      expect(result.fields?.['$id']?.admin?.position).toBe('sidebar')
    })

    it('generates field metadata for createdAt', () => {
      const result = generateCollectionMetadata(mockCollection)

      expect(result.fields?.['createdAt']?.label).toBe('Created')
      expect(result.fields?.['createdAt']?.readOnly).toBe(true)
    })

    it('generates field metadata for updatedAt', () => {
      const result = generateCollectionMetadata(mockCollection)

      expect(result.fields?.['updatedAt']?.label).toBe('Updated')
      expect(result.fields?.['updatedAt']?.readOnly).toBe(true)
    })

    it('generates field metadata for email field', () => {
      const result = generateCollectionMetadata(mockCollection)

      expect(result.fields?.['email']?.description).toBe('Email address')
    })

    it('generates field metadata for status field', () => {
      const fieldsWithStatus: DiscoveredField[] = [
        ...mockFields,
        { name: 'status', parquetType: 'BYTE_ARRAY', payloadType: 'select', optional: false, isArray: false },
      ]

      const collectionWithStatus: DiscoveredCollection = {
        ...mockCollection,
        fields: fieldsWithStatus,
      }

      const result = generateCollectionMetadata(collectionWithStatus)

      expect(result.fields?.['status']?.options).toBeDefined()
      expect(result.fields?.['status']?.options).toContainEqual({ label: 'Draft', value: 'draft' })
    })

    it('generates field metadata for description field', () => {
      const fieldsWithDescription: DiscoveredField[] = [
        ...mockFields,
        { name: 'description', parquetType: 'BYTE_ARRAY', payloadType: 'textarea', optional: true, isArray: false },
      ]

      const collectionWithDescription: DiscoveredCollection = {
        ...mockCollection,
        fields: fieldsWithDescription,
      }

      const result = generateCollectionMetadata(collectionWithDescription)

      expect(result.fields?.['description']?.description).toBe('A detailed description')
    })

    it('generates field metadata for password field', () => {
      const fieldsWithPassword: DiscoveredField[] = [
        ...mockFields,
        { name: 'password', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
      ]

      const collectionWithPassword: DiscoveredCollection = {
        ...mockCollection,
        fields: fieldsWithPassword,
      }

      const result = generateCollectionMetadata(collectionWithPassword)

      expect(result.fields?.['password']?.hideInList).toBe(true)
    })

    it('generates field metadata for URL fields', () => {
      const fieldsWithUrl: DiscoveredField[] = [
        ...mockFields,
        { name: 'websiteUrl', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: true, isArray: false },
      ]

      const collectionWithUrl: DiscoveredCollection = {
        ...mockCollection,
        fields: fieldsWithUrl,
      }

      const result = generateCollectionMetadata(collectionWithUrl)

      expect(result.fields?.['websiteUrl']?.description).toBe('Enter a valid URL')
    })

    it('does not generate metadata for non-ParqueDB files', () => {
      const nonParqueDBCollection: DiscoveredCollection = {
        ...mockCollection,
        isParqueDB: false,
        fields: [
          { name: 'id', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
          { name: 'createdAt', parquetType: 'INT64', payloadType: 'date', optional: false, isArray: false },
        ],
      }

      const result = generateCollectionMetadata(nonParqueDBCollection)

      // createdAt should not have special metadata for non-ParqueDB files
      expect(result.fields?.['createdAt']?.readOnly).toBeUndefined()
    })
  })

  describe('mergeMetadata', () => {
    const mockCollection: DiscoveredCollection = {
      slug: 'posts',
      label: 'Posts',
      path: '.db/posts/data.parquet',
      rowCount: 50,
      fileSize: 512,
      fields: [
        { name: '$id', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
        { name: 'title', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
      ],
      isParqueDB: true,
    }

    it('adds new collection to empty metadata', () => {
      const existing = createDefaultMetadata()

      const result = mergeMetadata(existing, [mockCollection])

      expect(result.collections['posts']).toBeDefined()
      expect(result.collections['posts'].label).toBe('Posts')
    })

    it('preserves existing collection customizations', () => {
      const existing: StudioMetadata = {
        version: '1.0',
        collections: {
          posts: {
            label: 'Blog Posts',
            description: 'All blog posts',
          },
        },
      }

      const result = mergeMetadata(existing, [mockCollection])

      expect(result.collections['posts'].label).toBe('Blog Posts')
      expect(result.collections['posts'].description).toBe('All blog posts')
    })

    it('adds metadata for new fields', () => {
      const existing: StudioMetadata = {
        version: '1.0',
        collections: {
          posts: {
            label: 'Posts',
            fields: {
              '$id': { label: 'ID' },
            },
          },
        },
      }

      const collectionWithNewField: DiscoveredCollection = {
        ...mockCollection,
        fields: [
          ...mockCollection.fields,
          { name: 'status', parquetType: 'BYTE_ARRAY', payloadType: 'select', optional: false, isArray: false },
        ],
      }

      const result = mergeMetadata(existing, [collectionWithNewField])

      expect(result.collections['posts'].fields?.['status']).toBeDefined()
      expect(result.collections['posts'].fields?.['$id']?.label).toBe('ID')
    })

    it('handles multiple collections', () => {
      const existing = createDefaultMetadata()

      const collections: DiscoveredCollection[] = [
        mockCollection,
        {
          slug: 'users',
          label: 'Users',
          path: '.db/users/data.parquet',
          rowCount: 100,
          fileSize: 1024,
          fields: [
            { name: '$id', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
            { name: 'name', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
          ],
          isParqueDB: true,
        },
      ]

      const result = mergeMetadata(existing, collections)

      expect(Object.keys(result.collections)).toHaveLength(2)
      expect(result.collections['posts']).toBeDefined()
      expect(result.collections['users']).toBeDefined()
    })

    it('does not overwrite existing field metadata', () => {
      const existing: StudioMetadata = {
        version: '1.0',
        collections: {
          posts: {
            fields: {
              title: { label: 'Post Title', description: 'Custom description' },
            },
          },
        },
      }

      const result = mergeMetadata(existing, [mockCollection])

      expect(result.collections['posts'].fields?.['title']?.label).toBe('Post Title')
      expect(result.collections['posts'].fields?.['title']?.description).toBe('Custom description')
    })
  })

  describe('updateCollectionMetadata', () => {
    it('updates collection label', () => {
      const metadata: StudioMetadata = {
        version: '1.0',
        collections: {
          posts: { label: 'Posts' },
        },
      }

      const result = updateCollectionMetadata(metadata, 'posts', { label: 'Blog Posts' })

      expect(result.collections['posts'].label).toBe('Blog Posts')
    })

    it('preserves existing fields', () => {
      const metadata: StudioMetadata = {
        version: '1.0',
        collections: {
          posts: {
            label: 'Posts',
            description: 'All posts',
          },
        },
      }

      const result = updateCollectionMetadata(metadata, 'posts', { label: 'Blog Posts' })

      expect(result.collections['posts'].label).toBe('Blog Posts')
      expect(result.collections['posts'].description).toBe('All posts')
    })

    it('creates collection if it does not exist', () => {
      const metadata: StudioMetadata = {
        version: '1.0',
        collections: {},
      }

      const result = updateCollectionMetadata(metadata, 'posts', { label: 'Posts' })

      expect(result.collections['posts'].label).toBe('Posts')
    })

    it('does not mutate original metadata', () => {
      const metadata: StudioMetadata = {
        version: '1.0',
        collections: {
          posts: { label: 'Posts' },
        },
      }

      const result = updateCollectionMetadata(metadata, 'posts', { label: 'Blog Posts' })

      expect(metadata.collections['posts'].label).toBe('Posts')
      expect(result.collections['posts'].label).toBe('Blog Posts')
    })
  })

  describe('updateFieldMetadata', () => {
    it('updates field label', () => {
      const metadata: StudioMetadata = {
        version: '1.0',
        collections: {
          posts: {
            fields: {
              title: { label: 'Title' },
            },
          },
        },
      }

      const result = updateFieldMetadata(metadata, 'posts', 'title', { label: 'Post Title' })

      expect(result.collections['posts'].fields?.['title']?.label).toBe('Post Title')
    })

    it('preserves existing field properties', () => {
      const metadata: StudioMetadata = {
        version: '1.0',
        collections: {
          posts: {
            fields: {
              title: { label: 'Title', description: 'The title', readOnly: true },
            },
          },
        },
      }

      const result = updateFieldMetadata(metadata, 'posts', 'title', { label: 'Post Title' })

      expect(result.collections['posts'].fields?.['title']?.label).toBe('Post Title')
      expect(result.collections['posts'].fields?.['title']?.description).toBe('The title')
      expect(result.collections['posts'].fields?.['title']?.readOnly).toBe(true)
    })

    it('creates field if it does not exist', () => {
      const metadata: StudioMetadata = {
        version: '1.0',
        collections: {
          posts: {},
        },
      }

      const result = updateFieldMetadata(metadata, 'posts', 'title', { label: 'Title' })

      expect(result.collections['posts'].fields?.['title']?.label).toBe('Title')
    })

    it('creates collection and field if neither exist', () => {
      const metadata: StudioMetadata = {
        version: '1.0',
        collections: {},
      }

      const result = updateFieldMetadata(metadata, 'posts', 'title', { label: 'Title' })

      expect(result.collections['posts'].fields?.['title']?.label).toBe('Title')
    })

    it('does not mutate original metadata', () => {
      const metadata: StudioMetadata = {
        version: '1.0',
        collections: {
          posts: {
            fields: {
              title: { label: 'Title' },
            },
          },
        },
      }

      const result = updateFieldMetadata(metadata, 'posts', 'title', { label: 'Post Title' })

      expect(metadata.collections['posts'].fields?.['title']?.label).toBe('Title')
      expect(result.collections['posts'].fields?.['title']?.label).toBe('Post Title')
    })
  })

  describe('validateMetadata', () => {
    const mockCollections: DiscoveredCollection[] = [
      {
        slug: 'posts',
        label: 'Posts',
        path: '.db/posts/data.parquet',
        rowCount: 50,
        fileSize: 512,
        fields: [
          { name: '$id', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
          { name: 'title', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
          { name: 'status', parquetType: 'BYTE_ARRAY', payloadType: 'select', optional: false, isArray: false },
        ],
        isParqueDB: true,
      },
    ]

    it('returns valid for correct metadata', () => {
      const metadata: StudioMetadata = {
        version: '1.0',
        collections: {
          posts: {
            admin: {
              useAsTitle: 'title',
              defaultColumns: ['$id', 'title', 'status'],
            },
            fields: {
              title: { label: 'Title' },
            },
          },
        },
      }

      const result = validateMetadata(metadata, mockCollections)

      expect(result.valid).toBe(true)
      expect(result.warnings).toHaveLength(0)
    })

    it('warns about metadata for non-existent collection', () => {
      const metadata: StudioMetadata = {
        version: '1.0',
        collections: {
          posts: {},
          nonexistent: {},
        },
      }

      const result = validateMetadata(metadata, mockCollections)

      expect(result.valid).toBe(false)
      expect(result.warnings).toContain('Metadata for unknown collection: nonexistent')
    })

    it('warns about useAsTitle referencing unknown field', () => {
      const metadata: StudioMetadata = {
        version: '1.0',
        collections: {
          posts: {
            admin: {
              useAsTitle: 'nonexistent',
            },
          },
        },
      }

      const result = validateMetadata(metadata, mockCollections)

      expect(result.valid).toBe(false)
      expect(result.warnings.some((w) => w.includes('useAsTitle references unknown field'))).toBe(true)
    })

    it('warns about defaultColumns referencing unknown field', () => {
      const metadata: StudioMetadata = {
        version: '1.0',
        collections: {
          posts: {
            admin: {
              defaultColumns: ['title', 'nonexistent'],
            },
          },
        },
      }

      const result = validateMetadata(metadata, mockCollections)

      expect(result.valid).toBe(false)
      expect(result.warnings.some((w) => w.includes('defaultColumns references unknown field'))).toBe(true)
    })

    it('warns about field metadata for unknown field', () => {
      const metadata: StudioMetadata = {
        version: '1.0',
        collections: {
          posts: {
            fields: {
              nonexistent: { label: 'Does Not Exist' },
            },
          },
        },
      }

      const result = validateMetadata(metadata, mockCollections)

      expect(result.valid).toBe(false)
      expect(result.warnings.some((w) => w.includes('field metadata for unknown field'))).toBe(true)
    })

    it('returns valid for empty metadata', () => {
      const metadata = createDefaultMetadata()

      const result = validateMetadata(metadata, mockCollections)

      expect(result.valid).toBe(true)
      expect(result.warnings).toHaveLength(0)
    })

    it('returns valid for empty collections', () => {
      const metadata: StudioMetadata = {
        version: '1.0',
        collections: {
          posts: { label: 'Posts' },
        },
      }

      const result = validateMetadata(metadata, [])

      expect(result.valid).toBe(false)
      expect(result.warnings).toContain('Metadata for unknown collection: posts')
    })
  })
})
