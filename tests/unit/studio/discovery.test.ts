/**
 * Tests for src/studio/discovery.ts
 *
 * Tests Parquet schema discovery and field extraction.
 */

import { describe, it, expect, vi } from 'vitest'
import {
  extractFields,
  schemaElementToField,
  slugToLabel,
  findTitleField,
  findDefaultColumns,
} from '../../../src/studio/discovery'
import type { SchemaElement } from 'hyparquet'
import type { DiscoveredField } from '../../../src/studio/types'

describe('discovery', () => {
  describe('schemaElementToField', () => {
    it('converts text field', () => {
      const element: SchemaElement = {
        name: 'username',
        type: 'BYTE_ARRAY',
        converted_type: 'UTF8',
        repetition_type: 'REQUIRED',
      }

      const result = schemaElementToField(element)

      expect(result).toEqual({
        name: 'username',
        parquetType: 'BYTE_ARRAY (UTF8)',
        payloadType: 'text',
        optional: false,
        isArray: false,
        typeInfo: undefined,
      })
    })

    it('converts number field (INT32)', () => {
      const element: SchemaElement = {
        name: 'age',
        type: 'INT32',
        repetition_type: 'OPTIONAL',
      }

      const result = schemaElementToField(element)

      expect(result).toEqual({
        name: 'age',
        parquetType: 'INT32',
        payloadType: 'number',
        optional: true,
        isArray: false,
        typeInfo: undefined,
      })
    })

    it('converts number field (INT64)', () => {
      const element: SchemaElement = {
        name: 'bigNumber',
        type: 'INT64',
        repetition_type: 'REQUIRED',
      }

      const result = schemaElementToField(element)

      expect(result?.payloadType).toBe('number')
    })

    it('converts double field', () => {
      const element: SchemaElement = {
        name: 'price',
        type: 'DOUBLE',
        repetition_type: 'REQUIRED',
      }

      const result = schemaElementToField(element)

      expect(result?.payloadType).toBe('number')
    })

    it('converts boolean field to checkbox', () => {
      const element: SchemaElement = {
        name: 'active',
        type: 'BOOLEAN',
        repetition_type: 'REQUIRED',
      }

      const result = schemaElementToField(element)

      expect(result?.payloadType).toBe('checkbox')
    })

    it('converts timestamp field to date', () => {
      const element: SchemaElement = {
        name: 'createdAt',
        type: 'INT64',
        converted_type: 'TIMESTAMP_MILLIS',
        repetition_type: 'REQUIRED',
      }

      const result = schemaElementToField(element)

      expect(result?.payloadType).toBe('date')
      expect(result?.parquetType).toBe('INT64 (TIMESTAMP_MILLIS)')
    })

    it('converts date field', () => {
      const element: SchemaElement = {
        name: 'birthDate',
        type: 'INT32',
        converted_type: 'DATE',
        repetition_type: 'OPTIONAL',
      }

      const result = schemaElementToField(element)

      expect(result?.payloadType).toBe('date')
    })

    it('converts JSON field', () => {
      const element: SchemaElement = {
        name: 'metadata',
        type: 'BYTE_ARRAY',
        converted_type: 'JSON',
        repetition_type: 'OPTIONAL',
      }

      const result = schemaElementToField(element)

      expect(result?.payloadType).toBe('json')
    })

    it('converts ENUM field to select', () => {
      const element: SchemaElement = {
        name: 'status',
        type: 'BYTE_ARRAY',
        converted_type: 'ENUM',
        repetition_type: 'REQUIRED',
      }

      const result = schemaElementToField(element)

      expect(result?.payloadType).toBe('select')
    })

    it('converts LIST field to array', () => {
      const element: SchemaElement = {
        name: 'tags',
        type: 'BYTE_ARRAY',
        converted_type: 'LIST',
        repetition_type: 'OPTIONAL',
      }

      const result = schemaElementToField(element)

      expect(result?.payloadType).toBe('array')
    })

    it('converts repeated field to array', () => {
      const element: SchemaElement = {
        name: 'items',
        type: 'BYTE_ARRAY',
        converted_type: 'UTF8',
        repetition_type: 'REPEATED',
      }

      const result = schemaElementToField(element)

      expect(result?.payloadType).toBe('array')
      expect(result?.isArray).toBe(true)
    })

    it('handles optional fields', () => {
      const element: SchemaElement = {
        name: 'nickname',
        type: 'BYTE_ARRAY',
        converted_type: 'UTF8',
        repetition_type: 'OPTIONAL',
      }

      const result = schemaElementToField(element)

      expect(result?.optional).toBe(true)
    })

    it('handles required fields', () => {
      const element: SchemaElement = {
        name: 'id',
        type: 'BYTE_ARRAY',
        converted_type: 'UTF8',
        repetition_type: 'REQUIRED',
      }

      const result = schemaElementToField(element)

      expect(result?.optional).toBe(false)
    })

    it('returns null for element without name', () => {
      const element: SchemaElement = {
        type: 'BYTE_ARRAY',
      }

      const result = schemaElementToField(element)

      expect(result).toBeNull()
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

      const result = schemaElementToField(element)

      expect(result?.typeInfo).toEqual({
        precision: 18,
        scale: 2,
        typeLength: 16,
      })
    })

    it('uses logical type when converted type not available', () => {
      const element: SchemaElement = {
        name: 'timestamp',
        type: 'INT64',
        logical_type: { type: 'TIMESTAMP_MILLIS' },
        repetition_type: 'REQUIRED',
      }

      const result = schemaElementToField(element)

      expect(result?.payloadType).toBe('date')
    })

    it('falls back to text for unknown types', () => {
      const element: SchemaElement = {
        name: 'unknown',
        repetition_type: 'REQUIRED',
      }

      const result = schemaElementToField(element)

      expect(result?.payloadType).toBe('text')
      expect(result?.parquetType).toBe('UNKNOWN')
    })
  })

  describe('extractFields', () => {
    it('extracts fields from schema, skipping root element', () => {
      const schema: SchemaElement[] = [
        { name: 'root', num_children: 3 },
        { name: 'id', type: 'BYTE_ARRAY', converted_type: 'UTF8', repetition_type: 'REQUIRED' },
        { name: 'name', type: 'BYTE_ARRAY', converted_type: 'UTF8', repetition_type: 'REQUIRED' },
        { name: 'age', type: 'INT32', repetition_type: 'OPTIONAL' },
      ]

      const result = extractFields(schema)

      expect(result).toHaveLength(3)
      expect(result.map((f) => f.name)).toEqual(['id', 'name', 'age'])
    })

    it('skips nested struct elements', () => {
      const schema: SchemaElement[] = [
        { name: 'root', num_children: 2 },
        { name: 'id', type: 'BYTE_ARRAY', converted_type: 'UTF8', repetition_type: 'REQUIRED' },
        { name: 'address', num_children: 2 },
        { name: 'street', type: 'BYTE_ARRAY', converted_type: 'UTF8', repetition_type: 'REQUIRED' },
        { name: 'city', type: 'BYTE_ARRAY', converted_type: 'UTF8', repetition_type: 'REQUIRED' },
      ]

      const result = extractFields(schema)

      // Should include id, street, city but not address (which has children)
      expect(result.map((f) => f.name)).toEqual(['id', 'street', 'city'])
    })

    it('returns empty array for empty schema', () => {
      const result = extractFields([])

      expect(result).toEqual([])
    })

    it('filters out null results', () => {
      const schema: SchemaElement[] = [
        { name: 'root', num_children: 2 },
        { type: 'BYTE_ARRAY' }, // No name - should be filtered
        { name: 'valid', type: 'BYTE_ARRAY', converted_type: 'UTF8', repetition_type: 'REQUIRED' },
      ]

      const result = extractFields(schema)

      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('valid')
    })
  })

  describe('slugToLabel', () => {
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

    it('trims whitespace', () => {
      expect(slugToLabel(' users ')).toBe('Users')
    })
  })

  describe('findTitleField', () => {
    it('finds name field', () => {
      const fields: DiscoveredField[] = [
        { name: '$id', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
        { name: 'name', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
        { name: 'email', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
      ]

      expect(findTitleField(fields)).toBe('name')
    })

    it('finds title field', () => {
      const fields: DiscoveredField[] = [
        { name: '$id', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
        { name: 'title', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
        { name: 'content', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
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
        { name: 'password', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
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

    it('falls back to first text field', () => {
      const fields: DiscoveredField[] = [
        { name: 'count', parquetType: 'INT32', payloadType: 'number', optional: false, isArray: false },
        { name: 'description', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
      ]

      expect(findTitleField(fields)).toBe('description')
    })

    it('falls back to first field name', () => {
      const fields: DiscoveredField[] = [
        { name: 'count', parquetType: 'INT32', payloadType: 'number', optional: false, isArray: false },
        { name: 'active', parquetType: 'BOOLEAN', payloadType: 'checkbox', optional: false, isArray: false },
      ]

      expect(findTitleField(fields)).toBe('count')
    })

    it('returns id for empty fields', () => {
      expect(findTitleField([])).toBe('id')
    })

    it('is case-insensitive', () => {
      const fields: DiscoveredField[] = [
        { name: 'NAME', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
      ]

      expect(findTitleField(fields)).toBe('NAME')
    })
  })

  describe('findDefaultColumns', () => {
    it('includes $id for ParqueDB files', () => {
      const fields: DiscoveredField[] = [
        { name: '$id', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
        { name: 'name', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
      ]

      const result = findDefaultColumns(fields, true)

      expect(result[0]).toBe('$id')
    })

    it('includes title field', () => {
      const fields: DiscoveredField[] = [
        { name: '$id', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
        { name: 'name', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
      ]

      const result = findDefaultColumns(fields, true)

      expect(result).toContain('name')
    })

    it('includes status field', () => {
      const fields: DiscoveredField[] = [
        { name: '$id', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
        { name: 'name', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
        { name: 'status', parquetType: 'BYTE_ARRAY', payloadType: 'select', optional: false, isArray: false },
      ]

      const result = findDefaultColumns(fields, true)

      expect(result).toContain('status')
    })

    it('includes createdAt and updatedAt', () => {
      const fields: DiscoveredField[] = [
        { name: '$id', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
        { name: 'name', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
        { name: 'createdAt', parquetType: 'INT64', payloadType: 'date', optional: false, isArray: false },
        { name: 'updatedAt', parquetType: 'INT64', payloadType: 'date', optional: false, isArray: false },
      ]

      const result = findDefaultColumns(fields, true)

      expect(result).toContain('createdAt')
      expect(result).toContain('updatedAt')
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

      const result = findDefaultColumns(fields, true)

      expect(result.length).toBeLessThanOrEqual(5)
    })

    it('does not include $id for non-ParqueDB files', () => {
      const fields: DiscoveredField[] = [
        { name: 'id', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
        { name: 'name', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
      ]

      const result = findDefaultColumns(fields, false)

      expect(result).not.toContain('$id')
    })

    it('excludes $ prefixed fields when filling with text fields', () => {
      const fields: DiscoveredField[] = [
        { name: '$id', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
        { name: '$type', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
        { name: '$data', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
        { name: 'title', parquetType: 'BYTE_ARRAY', payloadType: 'text', optional: false, isArray: false },
      ]

      const result = findDefaultColumns(fields, true)

      // $type and $data should not be included in the columns
      expect(result).not.toContain('$type')
      expect(result).not.toContain('$data')
    })
  })
})
