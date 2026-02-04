/**
 * E2E Tests: Studio Schema Discovery
 *
 * Tests schema discovery utilities with REAL storage backends (no mocks):
 * - Schema discovery from storage
 * - Field extraction logic
 * - UI hint generation
 *
 * Note: These tests focus on the discovery utilities themselves.
 * Full Parquet file tests would require writing actual Parquet files.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import {
  createTestBackend,
  cleanupBackend,
  writeCorruptedParquetFile,
  writeTruncatedParquetFile,
  writeEmptyFile,
  type BackendType,
} from './setup'
import type { StorageBackend } from '../../../src/types/storage'
import {
  extractFields,
  schemaElementToField,
  slugToLabel,
  findTitleField,
  findDefaultColumns,
} from '../../../src/studio/discovery'
import type { DiscoveredField } from '../../../src/studio/types'
import type { SchemaElement } from 'hyparquet'

describe('E2E: Studio Schema Discovery', () => {
  const backends: BackendType[] = ['memory', 'fs']

  for (const backendType of backends) {
    describe(`with ${backendType} backend`, () => {
      let storage: StorageBackend

      beforeEach(async () => {
        storage = await createTestBackend(backendType)
      })

      afterEach(async () => {
        await cleanupBackend(storage)
      })

      // =========================================================================
      // Storage Operations for Discovery
      // =========================================================================

      describe('Storage Operations for Discovery', () => {
        it('lists files in data directory', async () => {
          await storage.write('.db/posts/data.parquet', new Uint8Array([1, 2, 3]))
          await storage.write('.db/users/data.parquet', new Uint8Array([4, 5, 6]))

          const result = await storage.list('.db/', { delimiter: '/' })

          expect(result.prefixes).toBeDefined()
          expect(result.prefixes!.length).toBeGreaterThanOrEqual(2)
        })

        it('finds parquet files by extension', async () => {
          await storage.write('data/file1.parquet', new Uint8Array([1]))
          await storage.write('data/file2.parquet', new Uint8Array([2]))
          await storage.write('data/file3.txt', new Uint8Array([3]))

          const result = await storage.list('data/', { pattern: '*.parquet' })

          // The pattern is applied to filenames, not paths
          expect(result.files.every(f => f.endsWith('.parquet'))).toBe(true)
        })

        it('handles nested data directories', async () => {
          await storage.write('.db/entities/posts/data.parquet', new Uint8Array([1]))
          await storage.write('.db/entities/users/data.parquet', new Uint8Array([2]))

          const result = await storage.list('.db/entities/', { delimiter: '/' })

          expect(result.prefixes).toBeDefined()
          expect(result.prefixes!.some(p => p.includes('posts'))).toBe(true)
          expect(result.prefixes!.some(p => p.includes('users'))).toBe(true)
        })

        it('returns empty list for non-existent directory', async () => {
          const result = await storage.list('nonexistent/', { delimiter: '/' })

          expect(result.files).toEqual([])
        })
      })

      // =========================================================================
      // Schema Element to Field Conversion
      // =========================================================================

      describe('Schema Element Conversion', () => {
        it('converts text field', () => {
          const element: SchemaElement = {
            name: 'username',
            type: 'BYTE_ARRAY',
            converted_type: 'UTF8',
            repetition_type: 'REQUIRED',
          }

          const field = schemaElementToField(element)

          expect(field).not.toBeNull()
          expect(field!.name).toBe('username')
          expect(field!.payloadType).toBe('text')
          expect(field!.optional).toBe(false)
        })

        it('converts number field', () => {
          const element: SchemaElement = {
            name: 'count',
            type: 'INT32',
            repetition_type: 'OPTIONAL',
          }

          const field = schemaElementToField(element)

          expect(field).not.toBeNull()
          expect(field!.name).toBe('count')
          expect(field!.payloadType).toBe('number')
          expect(field!.optional).toBe(true)
        })

        it('converts boolean field to checkbox', () => {
          const element: SchemaElement = {
            name: 'active',
            type: 'BOOLEAN',
            repetition_type: 'REQUIRED',
          }

          const field = schemaElementToField(element)

          expect(field!.payloadType).toBe('checkbox')
        })

        it('converts timestamp field to date', () => {
          const element: SchemaElement = {
            name: 'createdAt',
            type: 'INT64',
            converted_type: 'TIMESTAMP_MILLIS',
            repetition_type: 'REQUIRED',
          }

          const field = schemaElementToField(element)

          expect(field!.payloadType).toBe('date')
        })

        it('converts JSON field', () => {
          const element: SchemaElement = {
            name: 'metadata',
            type: 'BYTE_ARRAY',
            converted_type: 'JSON',
            repetition_type: 'OPTIONAL',
          }

          const field = schemaElementToField(element)

          expect(field!.payloadType).toBe('json')
        })

        it('converts ENUM field to select', () => {
          const element: SchemaElement = {
            name: 'status',
            type: 'BYTE_ARRAY',
            converted_type: 'ENUM',
            repetition_type: 'REQUIRED',
          }

          const field = schemaElementToField(element)

          expect(field!.payloadType).toBe('select')
        })

        it('converts LIST field to array', () => {
          const element: SchemaElement = {
            name: 'tags',
            type: 'BYTE_ARRAY',
            converted_type: 'LIST',
            repetition_type: 'OPTIONAL',
          }

          const field = schemaElementToField(element)

          expect(field!.payloadType).toBe('array')
        })

        it('converts repeated field to array', () => {
          const element: SchemaElement = {
            name: 'items',
            type: 'BYTE_ARRAY',
            converted_type: 'UTF8',
            repetition_type: 'REPEATED',
          }

          const field = schemaElementToField(element)

          expect(field!.isArray).toBe(true)
        })

        it('returns null for element without name', () => {
          const element: SchemaElement = {
            type: 'BYTE_ARRAY',
          }

          const field = schemaElementToField(element)

          expect(field).toBeNull()
        })

        it('includes type info for decimal fields', () => {
          const element: SchemaElement = {
            name: 'amount',
            type: 'FIXED_LEN_BYTE_ARRAY',
            converted_type: 'DECIMAL',
            precision: 18,
            scale: 2,
            type_length: 16,
            repetition_type: 'REQUIRED',
          }

          const field = schemaElementToField(element)

          expect(field!.typeInfo).toEqual({
            precision: 18,
            scale: 2,
            typeLength: 16,
          })
        })
      })

      // =========================================================================
      // Field Extraction
      // =========================================================================

      describe('Field Extraction', () => {
        it('extracts fields from schema, skipping root', () => {
          const schema: SchemaElement[] = [
            { name: 'root', num_children: 3 },
            { name: 'id', type: 'BYTE_ARRAY', converted_type: 'UTF8', repetition_type: 'REQUIRED' },
            { name: 'name', type: 'BYTE_ARRAY', converted_type: 'UTF8', repetition_type: 'REQUIRED' },
            { name: 'age', type: 'INT32', repetition_type: 'OPTIONAL' },
          ]

          const fields = extractFields(schema)

          expect(fields).toHaveLength(3)
          expect(fields.map(f => f.name)).toEqual(['id', 'name', 'age'])
        })

        it('skips nested struct elements', () => {
          const schema: SchemaElement[] = [
            { name: 'root', num_children: 2 },
            { name: 'id', type: 'BYTE_ARRAY', converted_type: 'UTF8', repetition_type: 'REQUIRED' },
            { name: 'address', num_children: 2 },
            { name: 'street', type: 'BYTE_ARRAY', converted_type: 'UTF8', repetition_type: 'REQUIRED' },
            { name: 'city', type: 'BYTE_ARRAY', converted_type: 'UTF8', repetition_type: 'REQUIRED' },
          ]

          const fields = extractFields(schema)

          expect(fields.map(f => f.name)).toEqual(['id', 'street', 'city'])
        })

        it('returns empty array for empty schema', () => {
          const fields = extractFields([])

          expect(fields).toEqual([])
        })
      })

      // =========================================================================
      // Slug to Label Conversion
      // =========================================================================

      describe('Slug to Label Conversion', () => {
        it('converts snake_case to Title Case', () => {
          expect(slugToLabel('user_profiles')).toBe('User Profiles')
          expect(slugToLabel('order_items')).toBe('Order Items')
        })

        it('converts camelCase to Title Case', () => {
          expect(slugToLabel('userProfiles')).toBe('User Profiles')
          expect(slugToLabel('orderItems')).toBe('Order Items')
        })

        it('converts kebab-case to Title Case', () => {
          expect(slugToLabel('user-profiles')).toBe('User Profiles')
          expect(slugToLabel('order-items')).toBe('Order Items')
        })

        it('handles single word', () => {
          expect(slugToLabel('users')).toBe('Users')
          expect(slugToLabel('posts')).toBe('Posts')
        })

        it('handles already title case', () => {
          expect(slugToLabel('Users')).toBe('Users')
        })
      })

      // =========================================================================
      // Title Field Detection
      // =========================================================================

      describe('Title Field Detection', () => {
        it('finds name field as title', () => {
          const fields: DiscoveredField[] = [
            { name: '$id', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
            { name: 'name', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
            { name: 'email', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
          ]

          expect(findTitleField(fields)).toBe('name')
        })

        it('finds title field when no name', () => {
          const fields: DiscoveredField[] = [
            { name: '$id', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
            { name: 'title', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
          ]

          expect(findTitleField(fields)).toBe('title')
        })

        it('prefers name over title', () => {
          const fields: DiscoveredField[] = [
            { name: 'title', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
            { name: 'name', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
          ]

          expect(findTitleField(fields)).toBe('name')
        })

        it('falls back to email', () => {
          const fields: DiscoveredField[] = [
            { name: '$id', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
            { name: 'email', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
          ]

          expect(findTitleField(fields)).toBe('email')
        })

        it('falls back to $id', () => {
          const fields: DiscoveredField[] = [
            { name: '$id', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
            { name: 'status', parquetType: 'BYTE_ARRAY', payloadType: 'select', optional: false, isArray: false },
          ]

          expect(findTitleField(fields)).toBe('$id')
        })

        it('returns id for empty fields', () => {
          expect(findTitleField([])).toBe('id')
        })
      })

      // =========================================================================
      // Default Columns
      // =========================================================================

      describe('Default Columns', () => {
        it('includes $id for ParqueDB files', () => {
          const fields: DiscoveredField[] = [
            { name: '$id', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
            { name: 'name', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
          ]

          const columns = findDefaultColumns(fields, true)

          expect(columns).toContain('$id')
        })

        it('includes title field', () => {
          const fields: DiscoveredField[] = [
            { name: '$id', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
            { name: 'name', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
          ]

          const columns = findDefaultColumns(fields, true)

          expect(columns).toContain('name')
        })

        it('limits to 5 columns', () => {
          const fields: DiscoveredField[] = [
            { name: '$id', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
            { name: 'name', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
            { name: 'status', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
            { name: 'type', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
            { name: 'category', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
            { name: 'createdAt', parquetType: 'INT64', payloadType: 'date', optional: false, isArray: false },
            { name: 'updatedAt', parquetType: 'INT64', payloadType: 'date', optional: false, isArray: false },
          ]

          const columns = findDefaultColumns(fields, true)

          expect(columns.length).toBeLessThanOrEqual(5)
        })

        it('excludes $ prefixed fields except $id', () => {
          const fields: DiscoveredField[] = [
            { name: '$id', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
            { name: '$type', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
            { name: '$data', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
            { name: 'title', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
          ]

          const columns = findDefaultColumns(fields, true)

          expect(columns).toContain('$id')
          expect(columns).not.toContain('$type')
          expect(columns).not.toContain('$data')
        })
      })

      // =========================================================================
      // Error File Handling
      // =========================================================================

      describe('Error File Handling', () => {
        it('creates corrupted parquet file', async () => {
          await writeCorruptedParquetFile(storage, 'corrupted.parquet')

          expect(await storage.exists('corrupted.parquet')).toBe(true)

          const content = await storage.read('corrupted.parquet')
          // Should not start with PAR1 magic bytes
          const magic = String.fromCharCode(...content.slice(0, 4))
          expect(magic).not.toBe('PAR1')
        })

        it('creates truncated parquet file', async () => {
          await writeTruncatedParquetFile(storage, 'truncated.parquet')

          expect(await storage.exists('truncated.parquet')).toBe(true)

          const content = await storage.read('truncated.parquet')
          // Should start with PAR1 but be too short
          const magic = String.fromCharCode(...content.slice(0, 4))
          expect(magic).toBe('PAR1')
          expect(content.length).toBeLessThan(20) // Too short for valid parquet
        })

        it('creates empty file', async () => {
          await writeEmptyFile(storage, 'empty.parquet')

          expect(await storage.exists('empty.parquet')).toBe(true)

          const content = await storage.read('empty.parquet')
          expect(content.length).toBe(0)
        })
      })
    })
  }
})
