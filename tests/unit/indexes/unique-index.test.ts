/**
 * Tests for Unique Index Constraints
 *
 * Unique indexes enforce that no two documents can have the same value
 * for the indexed field(s).
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { HashIndex } from '@/indexes/secondary/hash'
import { SSTIndex } from '@/indexes/secondary/sst'
import { MemoryBackend } from '@/storage/MemoryBackend'
import type { IndexDefinition } from '@/indexes/types'
import { UniqueConstraintError } from '@/indexes/errors'

describe('Unique Index Constraints', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  describe('HashIndex with unique constraint', () => {
    it('allows inserting unique values', () => {
      const definition: IndexDefinition = {
        name: 'idx_email',
        type: 'hash',
        fields: [{ path: 'email' }],
        unique: true,
      }

      const index = new HashIndex(storage, 'users', definition)

      // Should not throw
      index.insert('alice@example.com', 'user1', 0, 0)
      index.insert('bob@example.com', 'user2', 0, 1)
      index.insert('charlie@example.com', 'user3', 0, 2)

      expect(index.size).toBe(3)
    })

    it('throws UniqueConstraintError on duplicate value', () => {
      const definition: IndexDefinition = {
        name: 'idx_email',
        type: 'hash',
        fields: [{ path: 'email' }],
        unique: true,
      }

      const index = new HashIndex(storage, 'users', definition)

      index.insert('alice@example.com', 'user1', 0, 0)

      expect(() => {
        index.insert('alice@example.com', 'user2', 0, 1)
      }).toThrow(UniqueConstraintError)
    })

    it('includes field name and value in error message', () => {
      const definition: IndexDefinition = {
        name: 'idx_email',
        type: 'hash',
        fields: [{ path: 'email' }],
        unique: true,
      }

      const index = new HashIndex(storage, 'users', definition)

      index.insert('alice@example.com', 'user1', 0, 0)

      try {
        index.insert('alice@example.com', 'user2', 0, 1)
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(UniqueConstraintError)
        const e = error as UniqueConstraintError
        expect(e.indexName).toBe('idx_email')
        expect(e.value).toBe('alice@example.com')
      }
    })

    it('allows same docId to update its own value', () => {
      const definition: IndexDefinition = {
        name: 'idx_email',
        type: 'hash',
        fields: [{ path: 'email' }],
        unique: true,
      }

      const index = new HashIndex(storage, 'users', definition)

      index.insert('alice@example.com', 'user1', 0, 0)

      // Updating same document should be allowed
      // First remove old entry, then insert new
      index.remove('alice@example.com', 'user1')
      index.insert('alice.new@example.com', 'user1', 0, 0)

      expect(index.size).toBe(1)
      expect(index.lookup('alice.new@example.com').docIds).toContain('user1')
    })

    it('allows duplicate values when unique is false', () => {
      const definition: IndexDefinition = {
        name: 'idx_status',
        type: 'hash',
        fields: [{ path: 'status' }],
        unique: false,
      }

      const index = new HashIndex(storage, 'orders', definition)

      index.insert('pending', 'order1', 0, 0)
      index.insert('pending', 'order2', 0, 1) // Should not throw

      expect(index.size).toBe(2)
    })

    it('allows duplicate values when unique is not specified', () => {
      const definition: IndexDefinition = {
        name: 'idx_status',
        type: 'hash',
        fields: [{ path: 'status' }],
        // unique not specified, defaults to false
      }

      const index = new HashIndex(storage, 'orders', definition)

      index.insert('pending', 'order1', 0, 0)
      index.insert('pending', 'order2', 0, 1) // Should not throw

      expect(index.size).toBe(2)
    })

    it('enforces uniqueness on number values', () => {
      const definition: IndexDefinition = {
        name: 'idx_ssn',
        type: 'hash',
        fields: [{ path: 'ssn' }],
        unique: true,
      }

      const index = new HashIndex(storage, 'people', definition)

      index.insert(123456789, 'person1', 0, 0)

      expect(() => {
        index.insert(123456789, 'person2', 0, 1)
      }).toThrow(UniqueConstraintError)
    })

    it('enforces uniqueness on composite keys', () => {
      const definition: IndexDefinition = {
        name: 'idx_tenant_code',
        type: 'hash',
        fields: [{ path: 'tenantId' }, { path: 'code' }],
        unique: true,
      }

      const index = new HashIndex(storage, 'products', definition)

      // Different tenants can have same code
      index.insert(['tenant1', 'SKU001'], 'prod1', 0, 0)
      index.insert(['tenant2', 'SKU001'], 'prod2', 0, 1) // OK - different tenant

      // Same tenant cannot have same code twice
      expect(() => {
        index.insert(['tenant1', 'SKU001'], 'prod3', 0, 2)
      }).toThrow(UniqueConstraintError)
    })

    describe('sparse unique indexes', () => {
      it('allows multiple null values with sparse unique index', () => {
        const definition: IndexDefinition = {
          name: 'idx_phone',
          type: 'hash',
          fields: [{ path: 'phone' }],
          unique: true,
          sparse: true,
        }

        const index = new HashIndex(storage, 'users', definition)

        // Multiple nulls are allowed with sparse unique
        index.insert(null, 'user1', 0, 0)
        index.insert(null, 'user2', 0, 1)
        index.insert(undefined, 'user3', 0, 2)

        expect(index.size).toBe(3)
      })

      it('still enforces uniqueness for non-null values with sparse unique', () => {
        const definition: IndexDefinition = {
          name: 'idx_phone',
          type: 'hash',
          fields: [{ path: 'phone' }],
          unique: true,
          sparse: true,
        }

        const index = new HashIndex(storage, 'users', definition)

        index.insert('555-1234', 'user1', 0, 0)

        expect(() => {
          index.insert('555-1234', 'user2', 0, 1)
        }).toThrow(UniqueConstraintError)
      })

      it('does not allow duplicate nulls with non-sparse unique index', () => {
        const definition: IndexDefinition = {
          name: 'idx_phone',
          type: 'hash',
          fields: [{ path: 'phone' }],
          unique: true,
          sparse: false,
        }

        const index = new HashIndex(storage, 'users', definition)

        index.insert(null, 'user1', 0, 0)

        expect(() => {
          index.insert(null, 'user2', 0, 1)
        }).toThrow(UniqueConstraintError)
      })
    })

    describe('buildFromArray with unique constraint', () => {
      it('throws on duplicate values during build', () => {
        const definition: IndexDefinition = {
          name: 'idx_email',
          type: 'hash',
          fields: [{ path: 'email' }],
          unique: true,
        }

        const index = new HashIndex(storage, 'users', definition)

        const docs = [
          { doc: { email: 'alice@example.com' }, docId: 'user1', rowGroup: 0, rowOffset: 0 },
          { doc: { email: 'bob@example.com' }, docId: 'user2', rowGroup: 0, rowOffset: 1 },
          { doc: { email: 'alice@example.com' }, docId: 'user3', rowGroup: 0, rowOffset: 2 }, // duplicate
        ]

        expect(() => {
          index.buildFromArray(docs)
        }).toThrow(UniqueConstraintError)
      })

      it('builds successfully with all unique values', () => {
        const definition: IndexDefinition = {
          name: 'idx_email',
          type: 'hash',
          fields: [{ path: 'email' }],
          unique: true,
        }

        const index = new HashIndex(storage, 'users', definition)

        const docs = [
          { doc: { email: 'alice@example.com' }, docId: 'user1', rowGroup: 0, rowOffset: 0 },
          { doc: { email: 'bob@example.com' }, docId: 'user2', rowGroup: 0, rowOffset: 1 },
          { doc: { email: 'charlie@example.com' }, docId: 'user3', rowGroup: 0, rowOffset: 2 },
        ]

        index.buildFromArray(docs)

        expect(index.size).toBe(3)
      })
    })
  })

  describe('SSTIndex with unique constraint', () => {
    it('throws UniqueConstraintError on duplicate value', () => {
      const definition: IndexDefinition = {
        name: 'idx_slug',
        type: 'sst',
        fields: [{ path: 'slug' }],
        unique: true,
      }

      const index = new SSTIndex(storage, 'posts', definition)

      index.insert('hello-world', 'post1', 0, 0)

      expect(() => {
        index.insert('hello-world', 'post2', 0, 1)
      }).toThrow(UniqueConstraintError)
    })

    it('allows unique values', () => {
      const definition: IndexDefinition = {
        name: 'idx_slug',
        type: 'sst',
        fields: [{ path: 'slug' }],
        unique: true,
      }

      const index = new SSTIndex(storage, 'posts', definition)

      index.insert('hello-world', 'post1', 0, 0)
      index.insert('another-post', 'post2', 0, 1)
      index.insert('third-post', 'post3', 0, 2)

      expect(index.size).toBe(3)
    })

    it('enforces uniqueness on numeric keys', () => {
      const definition: IndexDefinition = {
        name: 'idx_order_number',
        type: 'sst',
        fields: [{ path: 'orderNumber' }],
        unique: true,
      }

      const index = new SSTIndex(storage, 'orders', definition)

      index.insert(1001, 'order1', 0, 0)

      expect(() => {
        index.insert(1001, 'order2', 0, 1)
      }).toThrow(UniqueConstraintError)
    })

    it('enforces uniqueness on composite keys', () => {
      const definition: IndexDefinition = {
        name: 'idx_year_seq',
        type: 'sst',
        fields: [{ path: 'year' }, { path: 'sequence' }],
        unique: true,
      }

      const index = new SSTIndex(storage, 'invoices', definition)

      // Different years can have same sequence
      index.insert([2024, 1], 'inv1', 0, 0)
      index.insert([2025, 1], 'inv2', 0, 1)

      // Same year and sequence is duplicate
      expect(() => {
        index.insert([2024, 1], 'inv3', 0, 2)
      }).toThrow(UniqueConstraintError)
    })

    describe('sparse unique indexes', () => {
      it('allows multiple null values with sparse unique SST index', () => {
        const definition: IndexDefinition = {
          name: 'idx_external_id',
          type: 'sst',
          fields: [{ path: 'externalId' }],
          unique: true,
          sparse: true,
        }

        const index = new SSTIndex(storage, 'items', definition)

        index.insert(null, 'item1', 0, 0)
        index.insert(null, 'item2', 0, 1)

        expect(index.size).toBe(2)
      })
    })
  })

  describe('checkUnique method', () => {
    it('returns true if value is unique', () => {
      const definition: IndexDefinition = {
        name: 'idx_email',
        type: 'hash',
        fields: [{ path: 'email' }],
        unique: true,
      }

      const index = new HashIndex(storage, 'users', definition)

      index.insert('alice@example.com', 'user1', 0, 0)

      expect(index.checkUnique('bob@example.com')).toBe(true)
    })

    it('returns false if value already exists', () => {
      const definition: IndexDefinition = {
        name: 'idx_email',
        type: 'hash',
        fields: [{ path: 'email' }],
        unique: true,
      }

      const index = new HashIndex(storage, 'users', definition)

      index.insert('alice@example.com', 'user1', 0, 0)

      expect(index.checkUnique('alice@example.com')).toBe(false)
    })

    it('returns true for null/undefined with sparse index', () => {
      const definition: IndexDefinition = {
        name: 'idx_phone',
        type: 'hash',
        fields: [{ path: 'phone' }],
        unique: true,
        sparse: true,
      }

      const index = new HashIndex(storage, 'users', definition)

      index.insert(null, 'user1', 0, 0)

      // With sparse, null is always allowed
      expect(index.checkUnique(null)).toBe(true)
      expect(index.checkUnique(undefined)).toBe(true)
    })

    it('returns false for null with non-sparse unique index when null exists', () => {
      const definition: IndexDefinition = {
        name: 'idx_phone',
        type: 'hash',
        fields: [{ path: 'phone' }],
        unique: true,
        sparse: false,
      }

      const index = new HashIndex(storage, 'users', definition)

      index.insert(null, 'user1', 0, 0)

      expect(index.checkUnique(null)).toBe(false)
    })

    it('optionally excludes a specific docId from uniqueness check', () => {
      const definition: IndexDefinition = {
        name: 'idx_email',
        type: 'hash',
        fields: [{ path: 'email' }],
        unique: true,
      }

      const index = new HashIndex(storage, 'users', definition)

      index.insert('alice@example.com', 'user1', 0, 0)

      // When updating user1's email to same value, should pass uniqueness check
      expect(index.checkUnique('alice@example.com', 'user1')).toBe(true)
      // But another document cannot use that email
      expect(index.checkUnique('alice@example.com', 'user2')).toBe(false)
    })
  })
})
