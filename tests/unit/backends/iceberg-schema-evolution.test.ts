/**
 * Tests for IcebergBackend schema evolution
 *
 * Covers field additions, removals, renames, and type changes
 * via the setSchema() method.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { IcebergBackend, createIcebergBackend } from '../../../src/backends/iceberg'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import type { EntitySchema, SchemaField } from '../../../src/backends/types'

describe('IcebergBackend Schema Evolution', () => {
  let storage: MemoryBackend
  let backend: IcebergBackend

  beforeEach(async () => {
    storage = new MemoryBackend()
    backend = createIcebergBackend({
      type: 'iceberg',
      storage,
      warehouse: 'warehouse',
      database: 'testdb',
    })
    await backend.initialize()
  })

  afterEach(async () => {
    await backend.close()
  })

  function makeSchema(name: string, fields: SchemaField[], version?: number): EntitySchema {
    return {
      name,
      version,
      fields: [
        // Always include core entity fields
        { name: '$id', type: 'string', required: true },
        { name: '$type', type: 'string', required: true },
        { name: 'name', type: 'string', required: true },
        { name: 'createdAt', type: 'timestamp', required: true },
        { name: 'createdBy', type: 'string', required: true },
        { name: 'updatedAt', type: 'timestamp', required: true },
        { name: 'updatedBy', type: 'string', required: true },
        { name: 'deletedAt', type: 'timestamp', nullable: true },
        { name: 'deletedBy', type: 'string', nullable: true },
        { name: 'version', type: 'int', required: true },
        { name: '$data', type: 'binary', nullable: true },
        ...fields,
      ],
    }
  }

  describe('setSchema() - Create new table', () => {
    it('should create a table with initial schema', async () => {
      const schema = makeSchema('users', [
        { name: 'email', type: 'string', nullable: true },
        { name: 'age', type: 'int', nullable: true },
      ])

      await backend.setSchema('users', schema)

      const retrieved = await backend.getSchema('users')
      expect(retrieved).not.toBeNull()
      expect(retrieved!.fields.some(f => f.name === 'email')).toBe(true)
      expect(retrieved!.fields.some(f => f.name === 'age')).toBe(true)
    })
  })

  describe('setSchema() - Add fields', () => {
    it('should add a new optional field to existing schema', async () => {
      // Create initial schema
      const schema1 = makeSchema('users', [
        { name: 'email', type: 'string', nullable: true },
      ])
      await backend.setSchema('users', schema1)

      // Evolve: add 'age' field
      const schema2 = makeSchema('users', [
        { name: 'email', type: 'string', nullable: true },
        { name: 'age', type: 'int', nullable: true },
      ])
      await backend.setSchema('users', schema2)

      const retrieved = await backend.getSchema('users')
      expect(retrieved).not.toBeNull()
      expect(retrieved!.fields.some(f => f.name === 'age')).toBe(true)
    })

    it('should add multiple new fields', async () => {
      const schema1 = makeSchema('products', [])
      await backend.setSchema('products', schema1)

      const schema2 = makeSchema('products', [
        { name: 'price', type: 'double', nullable: true },
        { name: 'sku', type: 'string', nullable: true },
        { name: 'active', type: 'boolean', nullable: true },
      ])
      await backend.setSchema('products', schema2)

      const retrieved = await backend.getSchema('products')
      expect(retrieved).not.toBeNull()
      expect(retrieved!.fields.some(f => f.name === 'price')).toBe(true)
      expect(retrieved!.fields.some(f => f.name === 'sku')).toBe(true)
      expect(retrieved!.fields.some(f => f.name === 'active')).toBe(true)
    })
  })

  describe('setSchema() - Remove fields', () => {
    it('should remove a non-core field from schema', async () => {
      const schema1 = makeSchema('users', [
        { name: 'email', type: 'string', nullable: true },
        { name: 'legacy_field', type: 'string', nullable: true },
      ])
      await backend.setSchema('users', schema1)

      // Evolve: remove 'legacy_field'
      const schema2 = makeSchema('users', [
        { name: 'email', type: 'string', nullable: true },
      ])
      await backend.setSchema('users', schema2)

      const retrieved = await backend.getSchema('users')
      expect(retrieved).not.toBeNull()
      expect(retrieved!.fields.some(f => f.name === 'legacy_field')).toBe(false)
      expect(retrieved!.fields.some(f => f.name === 'email')).toBe(true)
    })

    it('should not remove core entity fields', async () => {
      const schema1 = makeSchema('users', [
        { name: 'email', type: 'string', nullable: true },
      ])
      await backend.setSchema('users', schema1)

      // Evolve: schema without core fields listed should NOT drop them
      const schema2: EntitySchema = {
        name: 'users',
        fields: [
          { name: 'email', type: 'string', nullable: true },
        ],
      }
      await backend.setSchema('users', schema2)

      const retrieved = await backend.getSchema('users')
      expect(retrieved).not.toBeNull()
      // Core fields should still be present
      expect(retrieved!.fields.some(f => f.name === '$id')).toBe(true)
      expect(retrieved!.fields.some(f => f.name === '$type')).toBe(true)
      expect(retrieved!.fields.some(f => f.name === 'name')).toBe(true)
      expect(retrieved!.fields.some(f => f.name === 'version')).toBe(true)
    })
  })

  describe('setSchema() - Rename fields', () => {
    it('should rename a field using renamedFrom', async () => {
      const schema1 = makeSchema('users', [
        { name: 'user_name', type: 'string', nullable: true },
      ])
      await backend.setSchema('users', schema1)

      // Evolve: rename 'user_name' to 'username'
      const schema2 = makeSchema('users', [
        { name: 'username', type: 'string', nullable: true, renamedFrom: 'user_name' },
      ])
      await backend.setSchema('users', schema2)

      const retrieved = await backend.getSchema('users')
      expect(retrieved).not.toBeNull()
      expect(retrieved!.fields.some(f => f.name === 'username')).toBe(true)
      expect(retrieved!.fields.some(f => f.name === 'user_name')).toBe(false)
    })
  })

  describe('setSchema() - Type changes', () => {
    it('should widen int to long', async () => {
      const schema1 = makeSchema('metrics', [
        { name: 'count', type: 'int', nullable: true },
      ])
      await backend.setSchema('metrics', schema1)

      // Evolve: promote int -> long
      const schema2 = makeSchema('metrics', [
        { name: 'count', type: 'long', nullable: true },
      ])
      await backend.setSchema('metrics', schema2)

      const retrieved = await backend.getSchema('metrics')
      expect(retrieved).not.toBeNull()
      const countField = retrieved!.fields.find(f => f.name === 'count')
      expect(countField).toBeDefined()
      expect(countField!.type).toBe('long')
    })

    it('should widen float to double', async () => {
      const schema1 = makeSchema('measurements', [
        { name: 'value', type: 'float', nullable: true },
      ])
      await backend.setSchema('measurements', schema1)

      // Evolve: promote float -> double
      const schema2 = makeSchema('measurements', [
        { name: 'value', type: 'double', nullable: true },
      ])
      await backend.setSchema('measurements', schema2)

      const retrieved = await backend.getSchema('measurements')
      expect(retrieved).not.toBeNull()
      const valueField = retrieved!.fields.find(f => f.name === 'value')
      expect(valueField).toBeDefined()
      expect(valueField!.type).toBe('double')
    })
  })

  describe('setSchema() - Combined operations', () => {
    it('should handle add + remove + rename in a single evolution', async () => {
      const schema1 = makeSchema('orders', [
        { name: 'total_price', type: 'double', nullable: true },
        { name: 'legacy_status', type: 'string', nullable: true },
        { name: 'customer_name', type: 'string', nullable: true },
      ])
      await backend.setSchema('orders', schema1)

      // Evolve: add 'currency', remove 'legacy_status', rename 'customer_name' -> 'buyer'
      const schema2 = makeSchema('orders', [
        { name: 'total_price', type: 'double', nullable: true },
        { name: 'buyer', type: 'string', nullable: true, renamedFrom: 'customer_name' },
        { name: 'currency', type: 'string', nullable: true },
      ])
      await backend.setSchema('orders', schema2)

      const retrieved = await backend.getSchema('orders')
      expect(retrieved).not.toBeNull()
      expect(retrieved!.fields.some(f => f.name === 'total_price')).toBe(true)
      expect(retrieved!.fields.some(f => f.name === 'buyer')).toBe(true)
      expect(retrieved!.fields.some(f => f.name === 'currency')).toBe(true)
      expect(retrieved!.fields.some(f => f.name === 'legacy_status')).toBe(false)
      expect(retrieved!.fields.some(f => f.name === 'customer_name')).toBe(false)
    })
  })

  describe('setSchema() - Data preservation after evolution', () => {
    it('should preserve existing data after adding a field', async () => {
      // Create entity first
      const entity = await backend.create('users', {
        $type: 'User',
        name: 'Alice',
        email: 'alice@example.com',
      })

      // Set schema with new field
      const schema = makeSchema('users', [
        { name: 'age', type: 'int', nullable: true },
      ])
      await backend.setSchema('users', schema)

      // Existing data should still be readable
      const found = await backend.get('users', entity.$id.split('/')[1]!)
      expect(found).not.toBeNull()
      expect(found!.name).toBe('Alice')
    })
  })

  describe('getSchema()', () => {
    it('should return null for non-existent table', async () => {
      const schema = await backend.getSchema('nonexistent')
      expect(schema).toBeNull()
    })

    it('should return the current schema after evolution', async () => {
      const schema1 = makeSchema('items', [
        { name: 'price', type: 'float', nullable: true },
      ])
      await backend.setSchema('items', schema1)

      const schema2 = makeSchema('items', [
        { name: 'price', type: 'double', nullable: true },
        { name: 'description', type: 'string', nullable: true },
      ])
      await backend.setSchema('items', schema2)

      const retrieved = await backend.getSchema('items')
      expect(retrieved).not.toBeNull()
      expect(retrieved!.fields.some(f => f.name === 'description')).toBe(true)
    })
  })
})
