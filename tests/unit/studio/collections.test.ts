/**
 * Tests for src/studio/collections.ts
 *
 * Tests Payload collection generation from discovered Parquet schemas.
 */

import { describe, it, expect } from 'vitest'
import {
  generateCollection,
  generateField,
  generateCollections,
  formatFieldLabel,
  inferRelationships,
} from '../../../src/studio/collections'
import type { DiscoveredCollection, DiscoveredField, CollectionUIMetadata } from '../../../src/studio/types'

describe('collections', () => {
  describe('generateField', () => {
    it('generates basic text field config', () => {
      const field: DiscoveredField = {
        name: 'username',
        parquetType: 'BYTE_ARRAY (UTF8)',
        payloadType: 'text',
        optional: false,
        isArray: false,
      }

      const result = generateField(field)

      expect(result.name).toBe('username')
      expect(result.type).toBe('text')
      expect(result.label).toBe('Username')
      expect(result.required).toBe(true)
    })

    it('generates number field with constraints from UI metadata', () => {
      const field: DiscoveredField = {
        name: 'age',
        parquetType: 'INT32',
        payloadType: 'number',
        optional: true,
        isArray: false,
      }

      const uiMetadata = {
        min: 0,
        max: 150,
      }

      const result = generateField(field, uiMetadata)

      expect(result.type).toBe('number')
      expect(result.min).toBe(0)
      expect(result.max).toBe(150)
      expect(result.required).toBe(false)
    })

    it('generates text field with length constraints', () => {
      const field: DiscoveredField = {
        name: 'bio',
        parquetType: 'BYTE_ARRAY (UTF8)',
        payloadType: 'textarea',
        optional: true,
        isArray: false,
      }

      const uiMetadata = {
        minLength: 10,
        maxLength: 500,
      }

      const result = generateField(field, uiMetadata)

      expect(result.minLength).toBe(10)
      expect(result.maxLength).toBe(500)
    })

    it('generates select field with options', () => {
      const field: DiscoveredField = {
        name: 'status',
        parquetType: 'BYTE_ARRAY (ENUM)',
        payloadType: 'select',
        optional: false,
        isArray: false,
      }

      const uiMetadata = {
        options: [
          { label: 'Draft', value: 'draft' },
          { label: 'Published', value: 'published' },
        ],
      }

      const result = generateField(field, uiMetadata)

      expect(result.type).toBe('select')
      expect(result.options).toEqual([
        { label: 'Draft', value: 'draft' },
        { label: 'Published', value: 'published' },
      ])
    })

    it('generates select field with default options when none provided', () => {
      const field: DiscoveredField = {
        name: 'category',
        parquetType: 'BYTE_ARRAY (ENUM)',
        payloadType: 'select',
        optional: false,
        isArray: false,
      }

      const result = generateField(field)

      expect(result.type).toBe('select')
      expect(result.options).toHaveLength(2)
    })

    it('generates relationship field', () => {
      const field: DiscoveredField = {
        name: 'author',
        parquetType: 'BYTE_ARRAY (UTF8)',
        payloadType: 'relationship',
        optional: false,
        isArray: false,
      }

      const uiMetadata = {
        relationTo: 'users',
        hasMany: false,
      }

      const result = generateField(field, uiMetadata)

      expect(result.type).toBe('relationship')
      expect(result.relationTo).toBe('users')
      expect(result.hasMany).toBe(false)
    })

    it('generates relationship field with hasMany for arrays', () => {
      const field: DiscoveredField = {
        name: 'tags',
        parquetType: 'BYTE_ARRAY (UTF8)',
        payloadType: 'relationship',
        optional: true,
        isArray: true,
      }

      const result = generateField(field)

      expect(result.hasMany).toBe(true)
    })

    it('generates array field with nested value field', () => {
      const field: DiscoveredField = {
        name: 'items',
        parquetType: 'LIST',
        payloadType: 'array',
        optional: true,
        isArray: true,
      }

      const result = generateField(field)

      expect(result.type).toBe('array')
      expect(result.fields).toHaveLength(1)
      expect(result.fields![0].name).toBe('value')
    })

    it('converts json field to code type', () => {
      const field: DiscoveredField = {
        name: 'metadata',
        parquetType: 'BYTE_ARRAY (JSON)',
        payloadType: 'json',
        optional: true,
        isArray: false,
      }

      const result = generateField(field)

      expect(result.type).toBe('code')
    })

    it('sets admin config from UI metadata', () => {
      const field: DiscoveredField = {
        name: 'createdAt',
        parquetType: 'INT64 (TIMESTAMP_MILLIS)',
        payloadType: 'date',
        optional: false,
        isArray: false,
      }

      const uiMetadata = {
        readOnly: true,
        description: 'When the record was created',
        admin: {
          position: 'sidebar' as const,
          width: '50%',
        },
      }

      const result = generateField(field, uiMetadata)

      expect(result.admin?.readOnly).toBe(true)
      expect(result.admin?.description).toBe('When the record was created')
      expect(result.admin?.position).toBe('sidebar')
      expect(result.admin?.width).toBe('50%')
    })

    it('sets admin hidden when hideInForm is true', () => {
      const field: DiscoveredField = {
        name: 'internalId',
        parquetType: 'BYTE_ARRAY (UTF8)',
        payloadType: 'text',
        optional: false,
        isArray: false,
      }

      const uiMetadata = {
        hideInForm: true,
      }

      const result = generateField(field, uiMetadata)

      expect(result.admin?.hidden).toBe(true)
    })

    it('respects readOnly option for all fields', () => {
      const field: DiscoveredField = {
        name: 'title',
        parquetType: 'BYTE_ARRAY (UTF8)',
        payloadType: 'text',
        optional: false,
        isArray: false,
      }

      const result = generateField(field, undefined, true)

      expect(result.admin?.readOnly).toBe(true)
    })
  })

  describe('generateCollection', () => {
    const mockFields: DiscoveredField[] = [
      { name: '$id', parquetType: 'BYTE_ARRAY (UTF8)', payloadType: 'text', optional: false, isArray: false },
      { name: '$type', parquetType: 'BYTE_ARRAY (UTF8)', payloadType: 'text', optional: false, isArray: false },
      { name: 'name', parquetType: 'BYTE_ARRAY (UTF8)', payloadType: 'text', optional: false, isArray: false },
      { name: 'email', parquetType: 'BYTE_ARRAY (UTF8)', payloadType: 'text', optional: true, isArray: false },
      { name: 'createdAt', parquetType: 'INT64 (TIMESTAMP_MILLIS)', payloadType: 'date', optional: false, isArray: false },
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

    it('generates basic collection config', () => {
      const result = generateCollection(mockCollection)

      expect(result.slug).toBe('users')
      expect(result.labels?.singular).toBe('User')
      expect(result.labels?.plural).toBe('Users')
      expect(result.timestamps).toBe(true)
    })

    it('filters out ParqueDB internal fields', () => {
      const result = generateCollection(mockCollection)

      const fieldNames = result.fields.map((f) => f.name)
      expect(fieldNames).not.toContain('$type')
      expect(fieldNames).toContain('$id')
      expect(fieldNames).toContain('name')
      expect(fieldNames).toContain('email')
    })

    it('uses UI metadata for labels', () => {
      const uiMetadata: CollectionUIMetadata = {
        label: 'Team Members',
        labelSingular: 'Team Member',
      }

      const result = generateCollection(mockCollection, uiMetadata)

      expect(result.labels?.singular).toBe('Team Member')
      expect(result.labels?.plural).toBe('Team Members')
    })

    it('sets admin config from UI metadata', () => {
      const uiMetadata: CollectionUIMetadata = {
        description: 'All user accounts',
        admin: {
          useAsTitle: 'email',
          defaultColumns: ['name', 'email', 'createdAt'],
          group: 'People',
          hidden: false,
          preview: true,
        },
      }

      const result = generateCollection(mockCollection, uiMetadata)

      expect(result.admin?.useAsTitle).toBe('email')
      expect(result.admin?.defaultColumns).toEqual(['name', 'email', 'createdAt'])
      expect(result.admin?.description).toBe('All user accounts')
      expect(result.admin?.group).toBe('People')
      expect(result.admin?.preview).toBe(true)
    })

    it('uses findTitleField when not specified in metadata', () => {
      const result = generateCollection(mockCollection)

      expect(result.admin?.useAsTitle).toBe('name')
    })

    it('generates read-only collection with access control', () => {
      const result = generateCollection(mockCollection, undefined, { readOnly: true })

      expect(result.access).toBeDefined()
      expect(typeof result.access?.create).toBe('function')
      expect(typeof result.access?.update).toBe('function')
      expect(typeof result.access?.delete).toBe('function')
      expect((result.access?.create as () => boolean)()).toBe(false)
      expect((result.access?.update as () => boolean)()).toBe(false)
      expect((result.access?.delete as () => boolean)()).toBe(false)
    })

    it('sets timestamps false for non-ParqueDB files', () => {
      const nonParqueDBCollection: DiscoveredCollection = {
        ...mockCollection,
        isParqueDB: false,
        fields: mockFields.filter((f) => !f.name.startsWith('$')),
      }

      const result = generateCollection(nonParqueDBCollection)

      expect(result.timestamps).toBe(false)
    })
  })

  describe('generateCollections', () => {
    const mockCollections: DiscoveredCollection[] = [
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
      {
        slug: 'posts',
        label: 'Posts',
        path: '.db/posts/data.parquet',
        rowCount: 500,
        fileSize: 2048,
        fields: [
          { name: '$id', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
          { name: 'title', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
        ],
        isParqueDB: true,
      },
    ]

    it('generates configs for all collections', () => {
      const result = generateCollections(mockCollections)

      expect(result).toHaveLength(2)
      expect(result[0].slug).toBe('users')
      expect(result[1].slug).toBe('posts')
    })

    it('applies metadata map to collections', () => {
      const metadataMap = {
        users: { label: 'Team Members' },
        posts: { label: 'Articles' },
      }

      const result = generateCollections(mockCollections, metadataMap)

      expect(result[0].labels?.plural).toBe('Team Members')
      expect(result[1].labels?.plural).toBe('Articles')
    })

    it('applies readOnly option to all collections', () => {
      const result = generateCollections(mockCollections, {}, { readOnly: true })

      expect(result[0].access).toBeDefined()
      expect(result[1].access).toBeDefined()
    })
  })

  describe('formatFieldLabel', () => {
    it('converts snake_case to Title Case', () => {
      expect(formatFieldLabel('user_name')).toBe('User Name')
      expect(formatFieldLabel('first_name')).toBe('First Name')
      expect(formatFieldLabel('created_at')).toBe('Created At')
    })

    it('converts camelCase to Title Case', () => {
      expect(formatFieldLabel('userName')).toBe('User Name')
      expect(formatFieldLabel('firstName')).toBe('First Name')
      expect(formatFieldLabel('createdAt')).toBe('Created At')
    })

    it('converts kebab-case to Title Case', () => {
      expect(formatFieldLabel('user-name')).toBe('User Name')
      expect(formatFieldLabel('first-name')).toBe('First Name')
    })

    it('handles $ prefix by removing it', () => {
      expect(formatFieldLabel('$id')).toBe('ID')
      expect(formatFieldLabel('$type')).toBe('Type')
      expect(formatFieldLabel('$data')).toBe('Data')
    })

    it('capitalizes common abbreviations', () => {
      expect(formatFieldLabel('userId')).toBe('User ID')
      expect(formatFieldLabel('api_url')).toBe('API URL')
      // Note: apiKey becomes 'Api Key' because the abbreviation replacement
      // only matches 'Api' as a word boundary, then 'Key' separately
      expect(formatFieldLabel('apiKey')).toBe('API Key')
    })

    it('handles single word fields', () => {
      expect(formatFieldLabel('name')).toBe('Name')
      expect(formatFieldLabel('email')).toBe('Email')
    })
  })

  describe('inferRelationships', () => {
    const mockCollections: DiscoveredCollection[] = [
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
      {
        slug: 'posts',
        label: 'Posts',
        path: '.db/posts/data.parquet',
        rowCount: 500,
        fileSize: 2048,
        fields: [
          { name: '$id', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
          { name: 'title', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
          { name: 'author', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
          { name: 'userId', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
        ],
        isParqueDB: true,
      },
      {
        slug: 'categories',
        label: 'Categories',
        path: '.db/categories/data.parquet',
        rowCount: 10,
        fileSize: 512,
        fields: [
          { name: '$id', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
          { name: 'name', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
          { name: 'parent', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: true, isArray: false },
        ],
        isParqueDB: true,
      },
      {
        slug: 'tags',
        label: 'Tags',
        path: '.db/tags/data.parquet',
        rowCount: 50,
        fileSize: 256,
        fields: [
          { name: '$id', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
          { name: 'name', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
        ],
        isParqueDB: true,
      },
    ]

    it('infers relationship from author field pattern', () => {
      const relationships = inferRelationships(mockCollections)

      expect(relationships.get('posts.author')).toBe('users')
    })

    it('infers relationship from userId field pattern', () => {
      const relationships = inferRelationships(mockCollections)

      expect(relationships.get('posts.userId')).toBe('users')
    })

    it('infers self-referential relationship from parent pattern', () => {
      const relationships = inferRelationships(mockCollections)

      expect(relationships.get('categories.parent')).toBe('categories')
    })

    it('returns empty map when no relationships found', () => {
      const simpleCollections: DiscoveredCollection[] = [
        {
          slug: 'settings',
          label: 'Settings',
          path: '.db/settings/data.parquet',
          rowCount: 1,
          fileSize: 128,
          fields: [
            { name: 'key', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
            { name: 'value', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
          ],
          isParqueDB: true,
        },
      ]

      const relationships = inferRelationships(simpleCollections)

      expect(relationships.size).toBe(0)
    })

    it('matches field name directly to collection slug', () => {
      const collectionsWithDirect: DiscoveredCollection[] = [
        ...mockCollections,
        {
          slug: 'comments',
          label: 'Comments',
          path: '.db/comments/data.parquet',
          rowCount: 1000,
          fileSize: 4096,
          fields: [
            { name: '$id', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
            { name: 'posts', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
          ],
          isParqueDB: true,
        },
      ]

      const relationships = inferRelationships(collectionsWithDirect)

      expect(relationships.get('comments.posts')).toBe('posts')
    })
  })
})
