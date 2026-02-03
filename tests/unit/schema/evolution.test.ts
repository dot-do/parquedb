/**
 * Schema Evolution Tests
 *
 * Comprehensive tests for schema evolution functionality including:
 * 1. Adding new fields to existing collections
 * 2. Removing fields
 * 3. Changing field types (compatible and incompatible)
 * 4. Renaming fields (detected as remove + add)
 * 5. Schema versioning
 *
 * @see Issue: parquedb-1j5c - Add schema evolution tests
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  detectBreakingChanges,
  generateMigrationHints,
  isSafeToApply,
  categorizeChanges,
  type BreakingChange
} from '../../../src/sync/schema-evolution'
import {
  captureSchema,
  diffSchemas,
  type SchemaSnapshot,
  type SchemaChanges,
  type SchemaChange,
  type CollectionSchemaSnapshot,
  type SchemaFieldSnapshot
} from '../../../src/sync/schema-snapshot'
import type { ParqueDBConfig } from '../../../src/config/loader'

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Create a test schema snapshot with specified version
 */
function createSnapshot(
  collections: Record<string, CollectionSchemaSnapshot>,
  options: { version?: number } = {}
): SchemaSnapshot {
  return {
    hash: `hash-${Date.now()}-${Math.random().toString(36).slice(2)}`,
    configHash: `config-${Date.now()}`,
    capturedAt: Date.now(),
    collections
  }
}

/**
 * Create a test collection schema with version tracking
 * Hash is computed deterministically from fields for proper diff detection
 */
function createCollection(
  name: string,
  fields: SchemaFieldSnapshot[],
  version: number = 1
): CollectionSchemaSnapshot {
  // Create deterministic hash based on field content
  const fieldStr = JSON.stringify(fields.map(f => ({
    name: f.name,
    type: f.type,
    required: f.required,
    indexed: f.indexed,
    unique: f.unique,
    array: f.array
  })))
  const hash = `coll-${name}-${fieldStr.length}-${simpleHash(fieldStr)}`
  return {
    name,
    hash,
    version,
    fields
  }
}

/**
 * Simple hash function for deterministic test hashes
 */
function simpleHash(str: string): string {
  let hash = 0
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i)
    hash = ((hash << 5) - hash) + char
    hash = hash & hash // Convert to 32bit integer
  }
  return Math.abs(hash).toString(36)
}

/**
 * Create a test field with all options
 */
function createField(
  name: string,
  type: string,
  options: Partial<Omit<SchemaFieldSnapshot, 'name' | 'type'>> = {}
): SchemaFieldSnapshot {
  return {
    name,
    type,
    required: options.required ?? false,
    indexed: options.indexed ?? false,
    unique: options.unique ?? false,
    array: options.array ?? false,
    default: options.default,
    relationship: options.relationship
  }
}

/**
 * Create a mock SchemaChanges object for testing evolution functions
 */
function createSchemaChanges(
  changes: SchemaChange[],
  options: { compatible?: boolean; summary?: string } = {}
): SchemaChanges {
  const breakingChanges = changes.filter(c => c.breaking)
  return {
    changes,
    breakingChanges,
    compatible: options.compatible ?? breakingChanges.length === 0,
    summary: options.summary ?? (changes.length === 0 ? 'No schema changes' : `${changes.length} changes`)
  }
}

// =============================================================================
// 1. Adding New Fields to Existing Collections
// =============================================================================

describe('Schema Evolution: Adding New Fields', () => {
  describe('Adding nullable/optional fields', () => {
    it('should allow adding a single optional field without breaking changes', () => {
      const before = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true })
        ])
      })

      const after = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true }),
          createField('bio', 'string?', { required: false })
        ])
      })

      const changes = diffSchemas(before, after)

      expect(changes.compatible).toBe(true)
      expect(changes.breakingChanges).toHaveLength(0)

      const addField = changes.changes.find(c => c.type === 'ADD_FIELD')
      expect(addField).toBeDefined()
      expect(addField?.field).toBe('bio')
      expect(addField?.breaking).toBe(false)
    })

    it('should allow adding multiple optional fields at once', () => {
      const before = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true })
        ])
      })

      const after = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true }),
          createField('firstName', 'string?'),
          createField('lastName', 'string?'),
          createField('age', 'int?'),
          createField('website', 'url?')
        ])
      })

      const changes = diffSchemas(before, after)

      expect(changes.compatible).toBe(true)
      const addedFields = changes.changes.filter(c => c.type === 'ADD_FIELD')
      expect(addedFields).toHaveLength(4)
      expect(addedFields.every(f => !f.breaking)).toBe(true)
    })

    it('should allow adding fields with default values', () => {
      const before = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true })
        ])
      })

      const after = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true }),
          createField('status', 'string', { default: 'active' }),
          createField('role', 'string', { default: 'user' })
        ])
      })

      const changes = diffSchemas(before, after)

      expect(changes.compatible).toBe(true)
      const addedFields = changes.changes.filter(c => c.type === 'ADD_FIELD')
      expect(addedFields).toHaveLength(2)
    })

    it('should allow adding array fields', () => {
      const before = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true })
        ])
      })

      const after = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true }),
          createField('tags', 'string[]', { array: true }),
          createField('permissions', 'string[]', { array: true })
        ])
      })

      const changes = diffSchemas(before, after)

      expect(changes.compatible).toBe(true)
      const addedFields = changes.changes.filter(c => c.type === 'ADD_FIELD')
      expect(addedFields).toHaveLength(2)
    })
  })

  describe('Adding required fields', () => {
    it('should mark adding a required field as breaking', () => {
      const before = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true })
        ])
      })

      const after = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true }),
          createField('username', 'string!', { required: true })
        ])
      })

      const changes = diffSchemas(before, after)

      expect(changes.compatible).toBe(false)
      expect(changes.breakingChanges).toHaveLength(1)
      expect(changes.breakingChanges[0]?.type).toBe('ADD_FIELD')
      expect(changes.breakingChanges[0]?.field).toBe('username')
    })

    it('should detect severity for adding required field', () => {
      const changes = createSchemaChanges([
        {
          type: 'ADD_FIELD',
          collection: 'User',
          field: 'username',
          after: createField('username', 'string!', { required: true }),
          breaking: true,
          description: 'Added field: User.username (required - BREAKING)'
        }
      ], { compatible: false })

      const breaking = detectBreakingChanges(changes)
      expect(breaking).toHaveLength(1)
      expect(breaking[0]?.severity).toBe('high')
      expect(breaking[0]?.impact).toContain('required field')
    })

    it('should still mark required field with default as breaking', () => {
      // Even with a default in schema, existing data won't have the field
      const before = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true })
        ])
      })

      const after = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true }),
          createField('role', 'string!', { required: true, default: 'user' })
        ])
      })

      const changes = diffSchemas(before, after)

      expect(changes.compatible).toBe(false)
    })
  })

  describe('Adding indexed fields', () => {
    it('should allow adding an indexed optional field', () => {
      const before = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true })
        ])
      })

      const after = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true }),
          createField('username', 'string?', { indexed: true })
        ])
      })

      const changes = diffSchemas(before, after)

      expect(changes.compatible).toBe(true)
    })

    it('should allow adding a unique indexed field', () => {
      const before = createSnapshot({
        User: createCollection('User', [
          createField('id', 'string!', { required: true })
        ])
      })

      const after = createSnapshot({
        User: createCollection('User', [
          createField('id', 'string!', { required: true }),
          createField('slug', 'string?', { unique: true })
        ])
      })

      const changes = diffSchemas(before, after)

      expect(changes.compatible).toBe(true)
    })
  })

  describe('Adding relationship fields', () => {
    it('should allow adding optional relationship fields', () => {
      const before = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true })
        ])
      })

      const after = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true }),
          createField('profile', '-> Profile', {
            relationship: { target: 'Profile', direction: 'outbound' }
          })
        ])
      })

      const changes = diffSchemas(before, after)

      expect(changes.compatible).toBe(true)
      const addField = changes.changes.find(c => c.type === 'ADD_FIELD')
      expect(addField?.field).toBe('profile')
    })
  })
})

// =============================================================================
// 2. Removing Fields
// =============================================================================

describe('Schema Evolution: Removing Fields', () => {
  describe('Basic field removal', () => {
    it('should mark removing any field as breaking', () => {
      const before = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true }),
          createField('age', 'int?')
        ])
      })

      const after = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true })
        ])
      })

      const changes = diffSchemas(before, after)

      expect(changes.compatible).toBe(false)
      const removeField = changes.changes.find(c => c.type === 'REMOVE_FIELD')
      expect(removeField).toBeDefined()
      expect(removeField?.field).toBe('age')
      expect(removeField?.breaking).toBe(true)
    })

    it('should detect removing multiple fields', () => {
      const before = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true }),
          createField('age', 'int?'),
          createField('bio', 'text?'),
          createField('phone', 'string?')
        ])
      })

      const after = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true })
        ])
      })

      const changes = diffSchemas(before, after)

      expect(changes.compatible).toBe(false)
      const removedFields = changes.changes.filter(c => c.type === 'REMOVE_FIELD')
      expect(removedFields).toHaveLength(3)
      expect(removedFields.map(f => f.field).sort()).toEqual(['age', 'bio', 'phone'])
    })

    it('should mark removing a required field as breaking', () => {
      const before = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true }),
          createField('username', 'string!', { required: true })
        ])
      })

      const after = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true })
        ])
      })

      const changes = diffSchemas(before, after)

      expect(changes.compatible).toBe(false)
      const removeField = changes.breakingChanges.find(c => c.type === 'REMOVE_FIELD')
      expect(removeField?.field).toBe('username')
    })
  })

  describe('Field removal severity and migration hints', () => {
    it('should assign high severity to field removal', () => {
      const changes = createSchemaChanges([
        {
          type: 'REMOVE_FIELD',
          collection: 'User',
          field: 'age',
          before: createField('age', 'int?'),
          breaking: true,
          description: 'Removed field: User.age'
        }
      ], { compatible: false })

      const breaking = detectBreakingChanges(changes)
      expect(breaking).toHaveLength(1)
      expect(breaking[0]?.severity).toBe('high')
      expect(breaking[0]?.impact).toContain('Queries referencing')
    })

    it('should provide migration hints for field removal', () => {
      const changes = createSchemaChanges([
        {
          type: 'REMOVE_FIELD',
          collection: 'User',
          field: 'legacyField',
          breaking: true,
          description: 'Removed field: User.legacyField'
        }
      ], { compatible: false })

      const hints = generateMigrationHints(changes)
      const hintsText = hints.join('\n')

      expect(hintsText).toContain('BREAKING CHANGES')
      expect(hintsText).toContain('legacyField')
    })
  })

  describe('Removing indexed fields', () => {
    it('should mark removing an indexed field as breaking', () => {
      const before = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true }),
          createField('username', 'string!', { required: true, indexed: true })
        ])
      })

      const after = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true })
        ])
      })

      const changes = diffSchemas(before, after)

      expect(changes.compatible).toBe(false)
    })
  })
})

// =============================================================================
// 3. Changing Field Types (Compatible and Incompatible)
// =============================================================================

describe('Schema Evolution: Changing Field Types', () => {
  describe('Type widening (potentially compatible)', () => {
    it('should detect int to long type change', () => {
      const before = createSnapshot({
        Counter: createCollection('Counter', [
          createField('value', 'int')
        ])
      })

      const after = createSnapshot({
        Counter: createCollection('Counter', [
          createField('value', 'long')
        ])
      })

      const changes = diffSchemas(before, after)

      const typeChange = changes.changes.find(c => c.type === 'CHANGE_TYPE')
      expect(typeChange).toBeDefined()
      expect(typeChange?.before).toBe('int')
      expect(typeChange?.after).toBe('long')
      // Currently all type changes are marked as breaking
      expect(typeChange?.breaking).toBe(true)
    })

    it('should detect float to double type change', () => {
      const before = createSnapshot({
        Metric: createCollection('Metric', [
          createField('value', 'float')
        ])
      })

      const after = createSnapshot({
        Metric: createCollection('Metric', [
          createField('value', 'double')
        ])
      })

      const changes = diffSchemas(before, after)

      const typeChange = changes.changes.find(c => c.type === 'CHANGE_TYPE')
      expect(typeChange).toBeDefined()
      expect(typeChange?.before).toBe('float')
      expect(typeChange?.after).toBe('double')
    })

    it('should detect int32 to int64 type change', () => {
      const before = createSnapshot({
        Data: createCollection('Data', [
          createField('count', 'int32')
        ])
      })

      const after = createSnapshot({
        Data: createCollection('Data', [
          createField('count', 'int64')
        ])
      })

      const changes = diffSchemas(before, after)

      const typeChange = changes.changes.find(c => c.type === 'CHANGE_TYPE')
      expect(typeChange).toBeDefined()
    })
  })

  describe('Incompatible type changes', () => {
    it('should detect string to int as critical breaking', () => {
      const before = createSnapshot({
        User: createCollection('User', [
          createField('age', 'string')
        ])
      })

      const after = createSnapshot({
        User: createCollection('User', [
          createField('age', 'int')
        ])
      })

      const changes = diffSchemas(before, after)

      expect(changes.compatible).toBe(false)
      const typeChange = changes.changes.find(c => c.type === 'CHANGE_TYPE')
      expect(typeChange?.breaking).toBe(true)

      const breaking = detectBreakingChanges(changes)
      expect(breaking[0]?.severity).toBe('critical')
    })

    it('should detect int to string as critical breaking', () => {
      const before = createSnapshot({
        User: createCollection('User', [
          createField('id', 'int')
        ])
      })

      const after = createSnapshot({
        User: createCollection('User', [
          createField('id', 'string')
        ])
      })

      const changes = diffSchemas(before, after)

      const breaking = detectBreakingChanges(changes)
      expect(breaking).toHaveLength(1)
      expect(breaking[0]?.severity).toBe('critical')
    })

    it('should detect boolean to string as critical breaking', () => {
      const before = createSnapshot({
        User: createCollection('User', [
          createField('verified', 'boolean')
        ])
      })

      const after = createSnapshot({
        User: createCollection('User', [
          createField('verified', 'string')
        ])
      })

      const changes = diffSchemas(before, after)

      const breaking = detectBreakingChanges(changes)
      expect(breaking[0]?.severity).toBe('critical')
    })

    it('should detect array to non-array as breaking', () => {
      const before = createSnapshot({
        User: createCollection('User', [
          createField('tags', 'string[]', { array: true })
        ])
      })

      const after = createSnapshot({
        User: createCollection('User', [
          createField('tags', 'string', { array: false })
        ])
      })

      const changes = diffSchemas(before, after)

      const typeChange = changes.changes.find(c => c.type === 'CHANGE_TYPE')
      expect(typeChange?.breaking).toBe(true)
    })

    it('should detect non-array to array as breaking', () => {
      const before = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string')
        ])
      })

      const after = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string[]', { array: true })
        ])
      })

      const changes = diffSchemas(before, after)

      expect(changes.compatible).toBe(false)
    })
  })

  describe('Type change migration hints', () => {
    it('should provide detailed migration hints for type changes', () => {
      const changes = createSchemaChanges([
        {
          type: 'CHANGE_TYPE',
          collection: 'User',
          field: 'age',
          before: 'string',
          after: 'int',
          breaking: true,
          description: 'Changed type: User.age from string to int'
        }
      ], { compatible: false })

      const hints = generateMigrationHints(changes)
      const hintsText = hints.join('\n')

      expect(hintsText).toContain('BREAKING CHANGES')
      expect(hintsText).toContain('migration script')
      expect(hintsText).toContain('convertType')
    })
  })

  describe('Required/Optional changes', () => {
    it('should detect making optional field required as breaking', () => {
      const before = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true }),
          createField('phone', 'string?', { required: false })
        ])
      })

      const after = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true }),
          createField('phone', 'string!', { required: true })
        ])
      })

      const changes = diffSchemas(before, after)

      expect(changes.compatible).toBe(false)
      const reqChange = changes.changes.find(c => c.type === 'CHANGE_REQUIRED')
      expect(reqChange).toBeDefined()
      expect(reqChange?.before).toBe(false)
      expect(reqChange?.after).toBe(true)
      expect(reqChange?.breaking).toBe(true)
    })

    it('should detect making required field optional as non-breaking', () => {
      const before = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true }),
          createField('phone', 'string!', { required: true })
        ])
      })

      const after = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true }),
          createField('phone', 'string?', { required: false })
        ])
      })

      const changes = diffSchemas(before, after)

      const reqChange = changes.changes.find(c => c.type === 'CHANGE_REQUIRED')
      expect(reqChange).toBeDefined()
      expect(reqChange?.breaking).toBe(false)
    })

    it('should provide migration hint for making field required', () => {
      const changes = createSchemaChanges([
        {
          type: 'CHANGE_REQUIRED',
          collection: 'User',
          field: 'phone',
          before: false,
          after: true,
          breaking: true,
          description: 'Changed required: User.phone now required'
        }
      ], { compatible: false })

      const breaking = detectBreakingChanges(changes)
      expect(breaking[0]?.severity).toBe('high')
      expect(breaking[0]?.migrationHint).toContain('updateMany')
      expect(breaking[0]?.migrationHint).toContain('$exists')
    })
  })
})

// =============================================================================
// 4. Renaming Fields
// =============================================================================

describe('Schema Evolution: Renaming Fields', () => {
  describe('Basic rename detection', () => {
    it('should detect rename as remove + add operations', () => {
      const before = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true }),
          createField('fullName', 'string')
        ])
      })

      const after = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true }),
          createField('displayName', 'string')  // Renamed from fullName
        ])
      })

      const changes = diffSchemas(before, after)

      // Should detect as remove fullName + add displayName
      const removeField = changes.changes.find(c => c.type === 'REMOVE_FIELD')
      const addField = changes.changes.find(c => c.type === 'ADD_FIELD')

      expect(removeField).toBeDefined()
      expect(removeField?.field).toBe('fullName')
      expect(addField).toBeDefined()
      expect(addField?.field).toBe('displayName')

      // Remove is breaking, add is not (if optional)
      expect(removeField?.breaking).toBe(true)
      expect(addField?.breaking).toBe(false)
    })

    it('should mark rename of required field as fully breaking', () => {
      const before = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true }),
          createField('username', 'string!', { required: true })
        ])
      })

      const after = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true }),
          createField('handle', 'string!', { required: true })  // Renamed from username
        ])
      })

      const changes = diffSchemas(before, after)

      // Both remove and add should be breaking
      const breakingChanges = changes.changes.filter(c => c.breaking)
      expect(breakingChanges.length).toBe(2)
    })

    it('should detect rename with type change', () => {
      const before = createSnapshot({
        User: createCollection('User', [
          createField('age', 'string')
        ])
      })

      const after = createSnapshot({
        User: createCollection('User', [
          createField('birthYear', 'int')  // Renamed and type changed
        ])
      })

      const changes = diffSchemas(before, after)

      expect(changes.changes.some(c => c.type === 'REMOVE_FIELD' && c.field === 'age')).toBe(true)
      expect(changes.changes.some(c => c.type === 'ADD_FIELD' && c.field === 'birthYear')).toBe(true)
    })
  })

  describe('Multiple field renames', () => {
    it('should detect multiple renames in same collection', () => {
      const before = createSnapshot({
        User: createCollection('User', [
          createField('fname', 'string'),
          createField('lname', 'string')
        ])
      })

      const after = createSnapshot({
        User: createCollection('User', [
          createField('firstName', 'string'),
          createField('lastName', 'string')
        ])
      })

      const changes = diffSchemas(before, after)

      const removedFields = changes.changes.filter(c => c.type === 'REMOVE_FIELD')
      const addedFields = changes.changes.filter(c => c.type === 'ADD_FIELD')

      expect(removedFields).toHaveLength(2)
      expect(addedFields).toHaveLength(2)
      expect(removedFields.map(f => f.field).sort()).toEqual(['fname', 'lname'])
      expect(addedFields.map(f => f.field).sort()).toEqual(['firstName', 'lastName'])
    })
  })
})

// =============================================================================
// 5. Schema Versioning
// =============================================================================

describe('Schema Evolution: Schema Versioning', () => {
  describe('Version tracking in snapshots', () => {
    it('should capture schema with version information', async () => {
      const config: ParqueDBConfig = {
        schema: {
          User: {
            email: 'string!#',
            name: 'string'
          }
        }
      }

      const snapshot = await captureSchema(config)

      expect(snapshot.collections.User).toBeDefined()
      expect(snapshot.collections.User?.version).toBeDefined()
      expect(snapshot.collections.User?.version).toBeGreaterThan(0)
    })

    it('should track collection version independently', async () => {
      const config: ParqueDBConfig = {
        schema: {
          User: {
            email: 'string!'
          },
          Post: {
            title: 'string!'
          }
        }
      }

      const snapshot = await captureSchema(config)

      expect(snapshot.collections.User?.version).toBeDefined()
      expect(snapshot.collections.Post?.version).toBeDefined()
    })

    it('should generate unique collection hash for different schema versions', async () => {
      const configV1: ParqueDBConfig = {
        schema: {
          User: {
            email: 'string!'
          }
        }
      }

      const configV2: ParqueDBConfig = {
        schema: {
          User: {
            email: 'string!',
            name: 'string?'
          }
        }
      }

      const snapshot1 = await captureSchema(configV1)
      const snapshot2 = await captureSchema(configV2)

      // Collection hashes should differ when fields change
      // (This is the key comparison for schema evolution detection)
      expect(snapshot1.collections.User?.hash).not.toBe(snapshot2.collections.User?.hash)
      expect(snapshot1.collections.User?.fields.length).toBe(1)
      expect(snapshot2.collections.User?.fields.length).toBe(2)
    })
  })

  describe('Version evolution tracking', () => {
    it('should detect sequential schema evolution', () => {
      // Simulate v1 -> v2 -> v3 evolution
      const v1 = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!')
        ], 1)
      })

      const v2 = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!'),
          createField('name', 'string?')
        ], 2)
      })

      const v3 = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!'),
          createField('name', 'string!', { required: true }),
          createField('bio', 'text?')
        ], 3)
      })

      // v1 -> v2: safe (add optional field)
      const changes1to2 = diffSchemas(v1, v2)
      expect(changes1to2.compatible).toBe(true)

      // v2 -> v3: breaking (optional -> required)
      const changes2to3 = diffSchemas(v2, v3)
      expect(changes2to3.compatible).toBe(false)

      // v1 -> v3: breaking (add required field)
      const changes1to3 = diffSchemas(v1, v3)
      expect(changes1to3.compatible).toBe(false)
    })

    it('should track changes across multiple collections', () => {
      const v1 = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!')
        ], 1),
        Post: createCollection('Post', [
          createField('title', 'string!')
        ], 1)
      })

      const v2 = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!'),
          createField('name', 'string?')
        ], 2),
        Post: createCollection('Post', [
          createField('title', 'string!'),
          createField('content', 'text?')
        ], 2),
        Comment: createCollection('Comment', [
          createField('text', 'string!')
        ], 1)
      })

      const changes = diffSchemas(v1, v2)

      // Should detect changes in User, Post, and new Comment collection
      expect(changes.changes.some(c => c.collection === 'User')).toBe(true)
      expect(changes.changes.some(c => c.collection === 'Post')).toBe(true)
      expect(changes.changes.some(c => c.type === 'ADD_COLLECTION' && c.collection === 'Comment')).toBe(true)
    })
  })

  describe('Schema hash consistency', () => {
    it('should produce consistent hash for same schema', async () => {
      const config: ParqueDBConfig = {
        schema: {
          User: {
            email: 'string!',
            name: 'string'
          }
        }
      }

      const snapshot1 = await captureSchema(config)
      const snapshot2 = await captureSchema(config)

      // Config hashes should be the same (same content)
      expect(snapshot1.configHash).toBe(snapshot2.configHash)
    })

    it('should detect schema changes via hash comparison', () => {
      const v1 = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!')
        ])
      })

      const v2 = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!'),
          createField('name', 'string?')
        ])
      })

      expect(v1.collections.User?.hash).not.toBe(v2.collections.User?.hash)
    })
  })

  describe('Schema versioning best practices', () => {
    it('should mark collection as unchanged when no fields change', () => {
      const userFields = [
        createField('email', 'string!', { required: true })
      ]

      const v1 = createSnapshot({
        User: createCollection('User', userFields)
      })

      // Create v2 with identical fields but same hash
      const v2: SchemaSnapshot = {
        hash: 'different-snapshot-hash',
        configHash: 'different-config-hash',
        capturedAt: Date.now(),
        collections: {
          User: {
            name: 'User',
            hash: v1.collections.User!.hash, // Same hash = same schema
            version: 1,
            fields: userFields
          }
        }
      }

      const changes = diffSchemas(v1, v2)

      expect(changes.changes).toHaveLength(0)
      expect(changes.summary).toBe('No schema changes')
    })
  })
})

// =============================================================================
// Collection-Level Changes
// =============================================================================

describe('Schema Evolution: Collection-Level Changes', () => {
  describe('Adding collections', () => {
    it('should detect adding a new collection as non-breaking', () => {
      const before = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true })
        ])
      })

      const after = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true })
        ]),
        Post: createCollection('Post', [
          createField('title', 'string!', { required: true }),
          createField('content', 'text')
        ])
      })

      const changes = diffSchemas(before, after)

      expect(changes.compatible).toBe(true)
      const addCollection = changes.changes.find(c => c.type === 'ADD_COLLECTION')
      expect(addCollection).toBeDefined()
      expect(addCollection?.collection).toBe('Post')
      expect(addCollection?.breaking).toBe(false)
    })

    it('should provide helpful hints for added collections', () => {
      const changes = createSchemaChanges([
        {
          type: 'ADD_COLLECTION',
          collection: 'Comment',
          breaking: false,
          description: 'Added collection: Comment'
        }
      ])

      const hints = generateMigrationHints(changes)
      const hintsText = hints.join('\n')

      expect(hintsText).toContain('Non-breaking changes')
      expect(hintsText).toContain('No action required')
    })
  })

  describe('Dropping collections', () => {
    it('should detect removing a collection as critical breaking', () => {
      const before = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true })
        ]),
        Post: createCollection('Post', [
          createField('title', 'string!', { required: true })
        ])
      })

      const after = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true })
        ])
      })

      const changes = diffSchemas(before, after)

      expect(changes.compatible).toBe(false)
      const dropCollection = changes.changes.find(c => c.type === 'DROP_COLLECTION')
      expect(dropCollection).toBeDefined()
      expect(dropCollection?.collection).toBe('Post')
      expect(dropCollection?.breaking).toBe(true)

      const breaking = detectBreakingChanges(changes)
      expect(breaking[0]?.severity).toBe('critical')
      expect(breaking[0]?.impact).toContain('All data')
    })

    it('should provide migration hints for dropped collections', () => {
      const changes = createSchemaChanges([
        {
          type: 'DROP_COLLECTION',
          collection: 'LegacyData',
          breaking: true,
          description: 'Dropped collection: LegacyData'
        }
      ], { compatible: false })

      const breaking = detectBreakingChanges(changes)
      expect(breaking[0]?.migrationHint).toContain('export')
    })
  })
})

// =============================================================================
// Index Changes
// =============================================================================

describe('Schema Evolution: Index Changes', () => {
  it('should detect adding an index as non-breaking', () => {
    const before = createSnapshot({
      User: createCollection('User', [
        createField('email', 'string', { indexed: false })
      ])
    })

    const after = createSnapshot({
      User: createCollection('User', [
        createField('email', 'string', { indexed: true })
      ])
    })

    const changes = diffSchemas(before, after)

    expect(changes.compatible).toBe(true)
    const indexChange = changes.changes.find(c => c.type === 'ADD_INDEX')
    expect(indexChange).toBeDefined()
    expect(indexChange?.breaking).toBe(false)
  })

  it('should detect removing an index as non-breaking', () => {
    const before = createSnapshot({
      User: createCollection('User', [
        createField('email', 'string', { indexed: true })
      ])
    })

    const after = createSnapshot({
      User: createCollection('User', [
        createField('email', 'string', { indexed: false })
      ])
    })

    const changes = diffSchemas(before, after)

    expect(changes.compatible).toBe(true)
    const indexChange = changes.changes.find(c => c.type === 'REMOVE_INDEX')
    expect(indexChange).toBeDefined()
    expect(indexChange?.breaking).toBe(false)
  })

  it('should provide performance hint for adding index', () => {
    const changes = createSchemaChanges([
      {
        type: 'ADD_INDEX',
        collection: 'User',
        field: 'email',
        before: false,
        after: true,
        breaking: false,
        description: 'Added index: User.email'
      }
    ])

    const hints = generateMigrationHints(changes)
    const hintsText = hints.join('\n')

    expect(hintsText).toContain('improve query performance')
  })
})

// =============================================================================
// Utility Functions
// =============================================================================

describe('Schema Evolution: Utility Functions', () => {
  describe('isSafeToApply', () => {
    it('should return true for compatible changes', () => {
      const changes = createSchemaChanges([
        {
          type: 'ADD_FIELD',
          collection: 'User',
          field: 'bio',
          breaking: false,
          description: 'Added field: User.bio'
        }
      ])

      expect(isSafeToApply(changes)).toBe(true)
    })

    it('should return false for breaking changes', () => {
      const changes = createSchemaChanges([
        {
          type: 'REMOVE_FIELD',
          collection: 'User',
          field: 'email',
          breaking: true,
          description: 'Removed field: User.email'
        }
      ], { compatible: false })

      expect(isSafeToApply(changes)).toBe(false)
    })

    it('should return false even with mixed changes', () => {
      const changes = createSchemaChanges([
        {
          type: 'ADD_FIELD',
          collection: 'User',
          field: 'bio',
          breaking: false,
          description: 'Added field: User.bio'
        },
        {
          type: 'REMOVE_FIELD',
          collection: 'User',
          field: 'legacy',
          breaking: true,
          description: 'Removed field: User.legacy'
        }
      ], { compatible: false })

      expect(isSafeToApply(changes)).toBe(false)
    })
  })

  describe('categorizeChanges', () => {
    it('should categorize changes by type', () => {
      const changes = createSchemaChanges([
        { type: 'ADD_COLLECTION', collection: 'Post', breaking: false, description: 'Added Post' },
        { type: 'DROP_COLLECTION', collection: 'Legacy', breaking: true, description: 'Dropped Legacy' },
        { type: 'ADD_FIELD', collection: 'User', field: 'bio', breaking: false, description: 'Added bio' },
        { type: 'REMOVE_FIELD', collection: 'User', field: 'age', breaking: true, description: 'Removed age' },
        { type: 'ADD_INDEX', collection: 'User', field: 'email', breaking: false, description: 'Added index' },
        { type: 'CHANGE_TYPE', collection: 'User', field: 'score', breaking: true, description: 'Changed type' }
      ], { compatible: false })

      const categories = categorizeChanges(changes)

      expect(categories.get('Collections')).toHaveLength(2)
      expect(categories.get('Fields')).toHaveLength(2)
      expect(categories.get('Indexes')).toHaveLength(1)
      expect(categories.get('Type Changes')).toHaveLength(1)
    })
  })

  describe('generateMigrationHints', () => {
    it('should generate type generation hint for all changes', () => {
      const changes = createSchemaChanges([])

      const hints = generateMigrationHints(changes)

      expect(hints.join('\n')).toContain('parquedb types generate')
    })

    it('should generate workflow recommendations for breaking changes', () => {
      const changes = createSchemaChanges([
        {
          type: 'REMOVE_FIELD',
          collection: 'User',
          field: 'legacy',
          breaking: true,
          description: 'Removed field: User.legacy'
        }
      ], { compatible: false })

      const hints = generateMigrationHints(changes)
      const hintsText = hints.join('\n')

      expect(hintsText).toContain('Recommended workflow')
      expect(hintsText).toContain('backup')
    })
  })
})

// =============================================================================
// Edge Cases
// =============================================================================

describe('Schema Evolution: Edge Cases', () => {
  it('should handle empty schemas', () => {
    const before = createSnapshot({})
    const after = createSnapshot({})

    const changes = diffSchemas(before, after)

    expect(changes.changes).toHaveLength(0)
    expect(changes.compatible).toBe(true)
    expect(changes.summary).toBe('No schema changes')
  })

  it('should handle schema with no fields', () => {
    const before = createSnapshot({
      Empty: createCollection('Empty', [])
    })
    const after = createSnapshot({
      Empty: createCollection('Empty', [])
    })

    const changes = diffSchemas(before, after)

    expect(changes.changes).toHaveLength(0)
    expect(changes.compatible).toBe(true)
  })

  it('should handle multiple collections with mixed changes', () => {
    const before = createSnapshot({
      User: createCollection('User', [
        createField('email', 'string!', { required: true }),
        createField('age', 'int?'),
        createField('legacy', 'string')
      ]),
      Post: createCollection('Post', [
        createField('title', 'string!')
      ]),
      Comment: createCollection('Comment', [
        createField('text', 'string')
      ])
    })

    const after = createSnapshot({
      User: createCollection('User', [
        createField('email', 'string!', { required: true }),
        createField('bio', 'text?')  // Added bio, removed age and legacy
      ]),
      Post: createCollection('Post', [
        createField('title', 'string!'),
        createField('content', 'text?')  // Added content
      ])
      // Comment collection removed
    })

    const changes = diffSchemas(before, after)

    expect(changes.compatible).toBe(false)

    // Should detect: remove age, remove legacy, add bio, add content, drop Comment
    expect(changes.changes.some(c => c.type === 'REMOVE_FIELD' && c.field === 'age')).toBe(true)
    expect(changes.changes.some(c => c.type === 'REMOVE_FIELD' && c.field === 'legacy')).toBe(true)
    expect(changes.changes.some(c => c.type === 'ADD_FIELD' && c.field === 'bio')).toBe(true)
    expect(changes.changes.some(c => c.type === 'ADD_FIELD' && c.field === 'content')).toBe(true)
    expect(changes.changes.some(c => c.type === 'DROP_COLLECTION' && c.collection === 'Comment')).toBe(true)
  })

  it('should handle flexible collections in config', async () => {
    const config: ParqueDBConfig = {
      schema: {
        User: {
          email: 'string!'
        },
        Logs: 'flexible'
      }
    }

    const snapshot = await captureSchema(config)

    // Flexible collections should be skipped
    expect(snapshot.collections.User).toBeDefined()
    expect(snapshot.collections.Logs).toBeUndefined()
  })

  it('should handle relationship fields in schema', async () => {
    const config: ParqueDBConfig = {
      schema: {
        User: {
          email: 'string!',
          profile: '-> Profile'
        },
        Profile: {
          bio: 'text',
          user: '<- User.profile'
        }
      }
    }

    const snapshot = await captureSchema(config)

    const profileField = snapshot.collections.User?.fields.find(f => f.name === 'profile')
    expect(profileField?.relationship).toBeDefined()
    expect(profileField?.relationship?.target).toBe('Profile')
    expect(profileField?.relationship?.direction).toBe('outbound')

    const userField = snapshot.collections.Profile?.fields.find(f => f.name === 'user')
    expect(userField?.relationship).toBeDefined()
    expect(userField?.relationship?.target).toBe('User')
    expect(userField?.relationship?.direction).toBe('inbound')
  })
})
