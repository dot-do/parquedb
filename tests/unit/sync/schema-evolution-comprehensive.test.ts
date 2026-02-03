/**
 * Comprehensive Schema Evolution Tests
 *
 * Tests for schema evolution functionality including:
 * - Adding new columns (nullable and with defaults)
 * - Removing columns
 * - Renaming columns
 * - Type changes (compatible and incompatible)
 * - Schema evolution with existing data
 * - Cross-backend evolution (Iceberg, Delta)
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
 * Create a test schema snapshot
 */
function createSnapshot(
  collections: Record<string, CollectionSchemaSnapshot>
): SchemaSnapshot {
  return {
    hash: `hash-${Date.now()}`,
    configHash: `config-${Date.now()}`,
    capturedAt: Date.now(),
    collections
  }
}

/**
 * Create a test collection schema
 * Uses a content-based hash to ensure field changes are detected
 */
function createCollection(
  name: string,
  fields: SchemaFieldSnapshot[]
): CollectionSchemaSnapshot {
  // Generate a deterministic hash based on actual content
  // This mimics the real hashObject behavior in production
  const hash = JSON.stringify({ name, fields })
  return {
    name,
    hash,
    version: 1,
    fields
  }
}

/**
 * Create a test field
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

// =============================================================================
// Adding New Columns Tests
// =============================================================================

describe('Schema Evolution: Adding New Columns', () => {
  describe('Adding nullable columns', () => {
    it('should detect adding a nullable field as non-breaking', () => {
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

    it('should detect adding multiple nullable fields as non-breaking', () => {
      const before = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true })
        ])
      })

      const after = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true }),
          createField('bio', 'string?'),
          createField('age', 'int?'),
          createField('website', 'url?')
        ])
      })

      const changes = diffSchemas(before, after)

      expect(changes.compatible).toBe(true)
      const addedFields = changes.changes.filter(c => c.type === 'ADD_FIELD')
      expect(addedFields).toHaveLength(3)
      expect(addedFields.every(f => !f.breaking)).toBe(true)
    })

    it('should generate helpful hints for nullable field additions', () => {
      const changes: SchemaChanges = {
        changes: [
          {
            type: 'ADD_FIELD',
            collection: 'User',
            field: 'bio',
            after: createField('bio', 'string?'),
            breaking: false,
            description: 'Added field: User.bio'
          }
        ],
        breakingChanges: [],
        compatible: true,
        summary: '1 change'
      }

      const hints = generateMigrationHints(changes)

      expect(hints.join('\n')).toContain('Non-breaking changes')
      expect(hints.join('\n')).toContain('No action required')
    })
  })

  describe('Adding columns with defaults', () => {
    it('should detect adding a field with default as non-breaking', () => {
      const before = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true })
        ])
      })

      const after = createSnapshot({
        User: createCollection('User', [
          createField('email', 'string!', { required: true }),
          createField('status', 'string', { required: false, default: 'active' })
        ])
      })

      const changes = diffSchemas(before, after)

      expect(changes.compatible).toBe(true)
      const addField = changes.changes.find(c => c.type === 'ADD_FIELD' && c.field === 'status')
      expect(addField).toBeDefined()
      expect(addField?.breaking).toBe(false)
    })

    it('should detect adding a required field with default as potentially breaking', () => {
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

      // Adding a required field is breaking (even with default in schema)
      // because existing data may not have this field
      expect(changes.compatible).toBe(false)
      const addField = changes.changes.find(c => c.type === 'ADD_FIELD' && c.field === 'role')
      expect(addField?.breaking).toBe(true)
    })

    it('should provide migration hint for adding required field with default', () => {
      const changes: SchemaChanges = {
        changes: [
          {
            type: 'ADD_FIELD',
            collection: 'User',
            field: 'role',
            after: {
              name: 'role',
              type: 'string!',
              required: true,
              indexed: false,
              unique: false,
              array: false,
              default: 'user'
            } as SchemaFieldSnapshot,
            breaking: true,
            description: 'Added field: User.role (required - BREAKING)'
          }
        ],
        breakingChanges: [
          {
            type: 'ADD_FIELD',
            collection: 'User',
            field: 'role',
            after: {
              name: 'role',
              type: 'string!',
              required: true,
              indexed: false,
              unique: false,
              array: false,
              default: 'user'
            } as SchemaFieldSnapshot,
            breaking: true,
            description: 'Added field: User.role (required - BREAKING)'
          }
        ],
        compatible: false,
        summary: '1 breaking change'
      }

      const breaking = detectBreakingChanges(changes)
      expect(breaking).toHaveLength(1)
      expect(breaking[0]?.severity).toBe('high')
      expect(breaking[0]?.impact).toContain('required field')
    })
  })

  describe('Adding required columns without defaults', () => {
    it('should detect adding a required field without default as breaking', () => {
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
    })
  })
})

// =============================================================================
// Removing Columns Tests
// =============================================================================

describe('Schema Evolution: Removing Columns', () => {
  it('should detect removing a field as breaking', () => {
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

  it('should assign high severity to field removal', () => {
    const changes: SchemaChanges = {
      changes: [
        {
          type: 'REMOVE_FIELD',
          collection: 'User',
          field: 'age',
          before: createField('age', 'int?'),
          breaking: true,
          description: 'Removed field: User.age'
        }
      ],
      breakingChanges: [
        {
          type: 'REMOVE_FIELD',
          collection: 'User',
          field: 'age',
          before: createField('age', 'int?'),
          breaking: true,
          description: 'Removed field: User.age'
        }
      ],
      compatible: false,
      summary: '1 breaking change'
    }

    const breaking = detectBreakingChanges(changes)
    expect(breaking).toHaveLength(1)
    expect(breaking[0]?.severity).toBe('high')
    expect(breaking[0]?.impact).toContain('Queries referencing')
  })

  it('should provide migration hints for field removal', () => {
    const changes: SchemaChanges = {
      changes: [
        {
          type: 'REMOVE_FIELD',
          collection: 'User',
          field: 'age',
          breaking: true,
          description: 'Removed field: User.age'
        }
      ],
      breakingChanges: [
        {
          type: 'REMOVE_FIELD',
          collection: 'User',
          field: 'age',
          breaking: true,
          description: 'Removed field: User.age'
        }
      ],
      compatible: false,
      summary: '1 breaking change'
    }

    const hints = generateMigrationHints(changes)
    const hintsText = hints.join('\n')

    expect(hintsText).toContain('BREAKING CHANGES')
    expect(hintsText).toContain('age')
    expect(hintsText).toContain('migration')
  })
})

// =============================================================================
// Renaming Columns Tests
// =============================================================================

describe('Schema Evolution: Renaming Columns', () => {
  // Note: ParqueDB schema evolution doesn't have native "rename" detection
  // A rename appears as a remove + add operation
  it('should detect rename as remove + add (both breaking and non-breaking)', () => {
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
})

// =============================================================================
// Type Changes Tests
// =============================================================================

describe('Schema Evolution: Type Changes', () => {
  describe('Compatible type changes (type widening)', () => {
    // Note: ParqueDB currently treats all type changes as breaking
    // These tests document expected behavior for future "type widening" support
    it('should detect int to long type change', () => {
      const before = createSnapshot({
        User: createCollection('User', [
          createField('age', 'int', { required: false })
        ])
      })

      const after = createSnapshot({
        User: createCollection('User', [
          createField('age', 'long', { required: false })
        ])
      })

      const changes = diffSchemas(before, after)

      const typeChange = changes.changes.find(c => c.type === 'CHANGE_TYPE')
      expect(typeChange).toBeDefined()
      expect(typeChange?.before).toBe('int')
      expect(typeChange?.after).toBe('long')
      // Currently all type changes are breaking
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
        Counter: createCollection('Counter', [
          createField('count', 'int32')
        ])
      })

      const after = createSnapshot({
        Counter: createCollection('Counter', [
          createField('count', 'int64')
        ])
      })

      const changes = diffSchemas(before, after)

      const typeChange = changes.changes.find(c => c.type === 'CHANGE_TYPE')
      expect(typeChange).toBeDefined()
      expect(typeChange?.before).toBe('int32')
      expect(typeChange?.after).toBe('int64')
    })
  })

  describe('Incompatible type changes', () => {
    it('should detect string to int type change as critical breaking', () => {
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

    it('should detect int to string type change as critical breaking', () => {
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
      expect(breaking[0]?.impact).toContain('type changed')
    })

    it('should detect boolean to string type change as critical breaking', () => {
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

    it('should provide detailed migration hints for incompatible type changes', () => {
      const changes: SchemaChanges = {
        changes: [
          {
            type: 'CHANGE_TYPE',
            collection: 'User',
            field: 'age',
            before: 'string',
            after: 'int',
            breaking: true,
            description: 'Changed type: User.age from string to int'
          }
        ],
        breakingChanges: [
          {
            type: 'CHANGE_TYPE',
            collection: 'User',
            field: 'age',
            before: 'string',
            after: 'int',
            breaking: true,
            description: 'Changed type: User.age from string to int'
          }
        ],
        compatible: false,
        summary: '1 breaking change'
      }

      const hints = generateMigrationHints(changes)
      const hintsText = hints.join('\n')

      expect(hintsText).toContain('BREAKING CHANGES')
      expect(hintsText).toContain('migration script')
      expect(hintsText).toContain('convertType')
    })

    it('should detect changing array to non-array as breaking', () => {
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
      expect(typeChange).toBeDefined()
      expect(typeChange?.breaking).toBe(true)
    })
  })
})

// =============================================================================
// Required/Optional Changes Tests
// =============================================================================

describe('Schema Evolution: Required/Optional Changes', () => {
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

    // Making required optional is safe
    const reqChange = changes.changes.find(c => c.type === 'CHANGE_REQUIRED')
    expect(reqChange).toBeDefined()
    expect(reqChange?.breaking).toBe(false)
  })

  it('should provide migration hint for making field required', () => {
    const changes: SchemaChanges = {
      changes: [
        {
          type: 'CHANGE_REQUIRED',
          collection: 'User',
          field: 'phone',
          before: false,
          after: true,
          breaking: true,
          description: 'Changed required: User.phone now required'
        }
      ],
      breakingChanges: [
        {
          type: 'CHANGE_REQUIRED',
          collection: 'User',
          field: 'phone',
          before: false,
          after: true,
          breaking: true,
          description: 'Changed required: User.phone now required'
        }
      ],
      compatible: false,
      summary: '1 breaking change'
    }

    const breaking = detectBreakingChanges(changes)
    expect(breaking[0]?.severity).toBe('high')
    expect(breaking[0]?.migrationHint).toContain('updateMany')
    expect(breaking[0]?.migrationHint).toContain('$exists')
  })
})

// =============================================================================
// Index Changes Tests
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
    const changes: SchemaChanges = {
      changes: [
        {
          type: 'ADD_INDEX',
          collection: 'User',
          field: 'email',
          before: false,
          after: true,
          breaking: false,
          description: 'Added index: User.email'
        }
      ],
      breakingChanges: [],
      compatible: true,
      summary: '1 change'
    }

    const hints = generateMigrationHints(changes)
    const hintsText = hints.join('\n')

    expect(hintsText).toContain('improve query performance')
  })
})

// =============================================================================
// Collection-Level Changes Tests
// =============================================================================

describe('Schema Evolution: Collection-Level Changes', () => {
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
})

// =============================================================================
// Schema Evolution with Existing Data Tests
// =============================================================================

describe('Schema Evolution: With Existing Data', () => {
  it('should capture schema from config', async () => {
    const config: ParqueDBConfig = {
      schema: {
        User: {
          email: 'string!#',
          name: 'string',
          age: 'int?'
        }
      }
    }

    const snapshot = await captureSchema(config)

    expect(snapshot.collections.User).toBeDefined()
    expect(snapshot.collections.User?.fields).toHaveLength(3)

    const emailField = snapshot.collections.User?.fields.find(f => f.name === 'email')
    expect(emailField?.required).toBe(true)
    expect(emailField?.indexed).toBe(true)
  })

  it('should detect changes between two captured schemas', async () => {
    const configV1: ParqueDBConfig = {
      schema: {
        User: {
          email: 'string!',
          name: 'string'
        }
      }
    }

    const configV2: ParqueDBConfig = {
      schema: {
        User: {
          email: 'string!#',  // Added index
          name: 'string',
          bio: 'text?'       // New field
        }
      }
    }

    const snapshot1 = await captureSchema(configV1)
    const snapshot2 = await captureSchema(configV2)

    const changes = diffSchemas(snapshot1, snapshot2)

    expect(changes.compatible).toBe(true)
    expect(changes.changes.length).toBeGreaterThan(0)

    // Should detect the new bio field
    const addField = changes.changes.find(c => c.type === 'ADD_FIELD')
    expect(addField?.field).toBe('bio')

    // Should detect the index addition
    const addIndex = changes.changes.find(c => c.type === 'ADD_INDEX')
    expect(addIndex?.field).toBe('email')
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

  it('should handle relationship fields', async () => {
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

// =============================================================================
// Categorization and Summary Tests
// =============================================================================

describe('Schema Evolution: Categorization and Summary', () => {
  it('should categorize changes by type', () => {
    const changes: SchemaChanges = {
      changes: [
        { type: 'ADD_COLLECTION', collection: 'Post', breaking: false, description: 'Added Post' },
        { type: 'DROP_COLLECTION', collection: 'Legacy', breaking: true, description: 'Dropped Legacy' },
        { type: 'ADD_FIELD', collection: 'User', field: 'bio', breaking: false, description: 'Added bio' },
        { type: 'REMOVE_FIELD', collection: 'User', field: 'age', breaking: true, description: 'Removed age' },
        { type: 'ADD_INDEX', collection: 'User', field: 'email', breaking: false, description: 'Added index' },
        { type: 'CHANGE_TYPE', collection: 'User', field: 'score', breaking: true, description: 'Changed type' }
      ],
      breakingChanges: [
        { type: 'DROP_COLLECTION', collection: 'Legacy', breaking: true, description: 'Dropped Legacy' },
        { type: 'REMOVE_FIELD', collection: 'User', field: 'age', breaking: true, description: 'Removed age' },
        { type: 'CHANGE_TYPE', collection: 'User', field: 'score', breaking: true, description: 'Changed type' }
      ],
      compatible: false,
      summary: '3 breaking changes'
    }

    const categories = categorizeChanges(changes)

    expect(categories.get('Collections')).toHaveLength(2)
    expect(categories.get('Fields')).toHaveLength(2)
    expect(categories.get('Indexes')).toHaveLength(1)
    expect(categories.get('Type Changes')).toHaveLength(1)
  })

  it('should generate comprehensive summary', () => {
    const before = createSnapshot({
      User: createCollection('User', [
        createField('email', 'string!', { required: true }),
        createField('age', 'int?'),
        createField('legacy', 'string')
      ]),
      Legacy: createCollection('Legacy', [
        createField('data', 'string')
      ])
    })

    const after = createSnapshot({
      User: createCollection('User', [
        createField('email', 'string!#', { required: true, indexed: true }),
        createField('age', 'long'),  // Type change
        createField('bio', 'text?')  // New field
      ]),
      Post: createCollection('Post', [
        createField('title', 'string!')
      ])
    })

    const changes = diffSchemas(before, after)

    expect(changes.summary).toContain('breaking')
    expect(changes.compatible).toBe(false)
  })

  it('should check isSafeToApply correctly', () => {
    const safeChanges: SchemaChanges = {
      changes: [
        { type: 'ADD_FIELD', collection: 'User', field: 'bio', breaking: false, description: 'Added bio' }
      ],
      breakingChanges: [],
      compatible: true,
      summary: '1 change'
    }

    const unsafeChanges: SchemaChanges = {
      changes: [
        { type: 'REMOVE_FIELD', collection: 'User', field: 'age', breaking: true, description: 'Removed age' }
      ],
      breakingChanges: [
        { type: 'REMOVE_FIELD', collection: 'User', field: 'age', breaking: true, description: 'Removed age' }
      ],
      compatible: false,
      summary: '1 breaking change'
    }

    expect(isSafeToApply(safeChanges)).toBe(true)
    expect(isSafeToApply(unsafeChanges)).toBe(false)
  })
})

// =============================================================================
// Cross-Backend Evolution Tests
// =============================================================================

describe('Schema Evolution: Cross-Backend (Iceberg, Delta)', () => {
  // These tests verify that schema evolution concepts apply consistently
  // regardless of the underlying table format

  describe('Iceberg-compatible schema changes', () => {
    it('should detect Iceberg-safe field additions', () => {
      // Iceberg allows adding optional columns at any position
      const before = createSnapshot({
        Events: createCollection('Events', [
          createField('id', 'string!', { required: true }),
          createField('timestamp', 'timestamp!')
        ])
      })

      const after = createSnapshot({
        Events: createCollection('Events', [
          createField('id', 'string!', { required: true }),
          createField('timestamp', 'timestamp!'),
          createField('metadata', 'json?'),  // New optional column
          createField('source', 'string?')   // New optional column
        ])
      })

      const changes = diffSchemas(before, after)

      expect(changes.compatible).toBe(true)
      const addFields = changes.changes.filter(c => c.type === 'ADD_FIELD')
      expect(addFields).toHaveLength(2)
      expect(addFields.every(c => !c.breaking)).toBe(true)
    })

    it('should detect Iceberg type widening scenarios', () => {
      // Iceberg supports specific type promotions: int->long, float->double
      const before = createSnapshot({
        Metrics: createCollection('Metrics', [
          createField('count', 'int'),
          createField('rate', 'float')
        ])
      })

      const after = createSnapshot({
        Metrics: createCollection('Metrics', [
          createField('count', 'long'),   // Widened: int -> long
          createField('rate', 'double')   // Widened: float -> double
        ])
      })

      const changes = diffSchemas(before, after)

      const typeChanges = changes.changes.filter(c => c.type === 'CHANGE_TYPE')
      expect(typeChanges).toHaveLength(2)

      // Note: Currently ParqueDB marks all type changes as breaking
      // Future versions may detect Iceberg-safe promotions
    })

    it('should detect Iceberg-unsafe schema changes', () => {
      // Iceberg does NOT allow: removing columns, changing types incompatibly
      const before = createSnapshot({
        Data: createCollection('Data', [
          createField('id', 'string!', { required: true }),
          createField('value', 'int'),
          createField('label', 'string')
        ])
      })

      const after = createSnapshot({
        Data: createCollection('Data', [
          createField('id', 'string!', { required: true }),
          createField('value', 'string')  // Incompatible type change
          // label removed
        ])
      })

      const changes = diffSchemas(before, after)

      expect(changes.compatible).toBe(false)
      expect(changes.breakingChanges.length).toBeGreaterThan(0)

      // Should have both type change and field removal
      expect(changes.changes.some(c => c.type === 'CHANGE_TYPE')).toBe(true)
      expect(changes.changes.some(c => c.type === 'REMOVE_FIELD')).toBe(true)
    })
  })

  describe('Delta Lake-compatible schema changes', () => {
    it('should detect Delta-safe column additions', () => {
      // Delta Lake allows adding nullable columns
      const before = createSnapshot({
        Transactions: createCollection('Transactions', [
          createField('id', 'string!', { required: true }),
          createField('amount', 'decimal(18,2)!')
        ])
      })

      const after = createSnapshot({
        Transactions: createCollection('Transactions', [
          createField('id', 'string!', { required: true }),
          createField('amount', 'decimal(18,2)!'),
          createField('currency', 'string?'),
          createField('notes', 'text?')
        ])
      })

      const changes = diffSchemas(before, after)

      expect(changes.compatible).toBe(true)
      const addFields = changes.changes.filter(c => c.type === 'ADD_FIELD')
      expect(addFields).toHaveLength(2)
    })

    it('should detect Delta merge schema scenarios', () => {
      // Delta's mergeSchema allows adding new columns from new data
      const existingSchema = createSnapshot({
        Events: createCollection('Events', [
          createField('eventId', 'string!', { required: true }),
          createField('eventType', 'string!')
        ])
      })

      const newDataSchema = createSnapshot({
        Events: createCollection('Events', [
          createField('eventId', 'string!', { required: true }),
          createField('eventType', 'string!'),
          createField('userId', 'string?'),
          createField('sessionId', 'string?'),
          createField('properties', 'json?')
        ])
      })

      const changes = diffSchemas(existingSchema, newDataSchema)

      // New optional fields should be safe to merge
      expect(changes.compatible).toBe(true)
      expect(changes.changes.filter(c => c.type === 'ADD_FIELD')).toHaveLength(3)
    })

    it('should detect Delta overwriteSchema scenarios as breaking', () => {
      // overwriteSchema requires explicit flag because it can break readers
      const before = createSnapshot({
        Data: createCollection('Data', [
          createField('id', 'string!', { required: true }),
          createField('oldField', 'string!', { required: true })
        ])
      })

      const after = createSnapshot({
        Data: createCollection('Data', [
          createField('id', 'string!', { required: true }),
          createField('newField', 'int!', { required: true })  // Different field entirely
        ])
      })

      const changes = diffSchemas(before, after)

      expect(changes.compatible).toBe(false)
      // Old required field removed, new required field added
      expect(changes.breakingChanges.length).toBe(2)
    })
  })

  describe('Backend-agnostic evolution rules', () => {
    it('should consistently detect breaking changes across formats', () => {
      // These rules should apply regardless of backend
      const breakingScenarios = [
        {
          name: 'remove field',
          before: createSnapshot({
            T: createCollection('T', [
              createField('a', 'string'),
              createField('b', 'string')
            ])
          }),
          after: createSnapshot({
            T: createCollection('T', [
              createField('a', 'string')
            ])
          })
        },
        {
          name: 'incompatible type change',
          before: createSnapshot({
            T: createCollection('T', [
              createField('value', 'string')
            ])
          }),
          after: createSnapshot({
            T: createCollection('T', [
              createField('value', 'int')
            ])
          })
        },
        {
          name: 'optional to required',
          before: createSnapshot({
            T: createCollection('T', [
              createField('field', 'string?', { required: false })
            ])
          }),
          after: createSnapshot({
            T: createCollection('T', [
              createField('field', 'string!', { required: true })
            ])
          })
        },
        {
          name: 'drop collection',
          before: createSnapshot({
            T: createCollection('T', [createField('a', 'string')]),
            U: createCollection('U', [createField('b', 'string')])
          }),
          after: createSnapshot({
            T: createCollection('T', [createField('a', 'string')])
          })
        }
      ]

      for (const scenario of breakingScenarios) {
        const changes = diffSchemas(scenario.before, scenario.after)
        expect(changes.compatible).toBe(false)
      }
    })

    it('should consistently detect safe changes across formats', () => {
      const safeScenarios = [
        {
          name: 'add optional field',
          before: createSnapshot({
            T: createCollection('T', [createField('a', 'string')])
          }),
          after: createSnapshot({
            T: createCollection('T', [
              createField('a', 'string'),
              createField('b', 'string?')
            ])
          })
        },
        {
          name: 'add collection',
          before: createSnapshot({
            T: createCollection('T', [createField('a', 'string')])
          }),
          after: createSnapshot({
            T: createCollection('T', [createField('a', 'string')]),
            U: createCollection('U', [createField('b', 'string')])
          })
        },
        {
          name: 'add index',
          before: createSnapshot({
            T: createCollection('T', [
              createField('email', 'string', { indexed: false })
            ])
          }),
          after: createSnapshot({
            T: createCollection('T', [
              createField('email', 'string', { indexed: true })
            ])
          })
        },
        {
          name: 'required to optional',
          before: createSnapshot({
            T: createCollection('T', [
              createField('field', 'string!', { required: true })
            ])
          }),
          after: createSnapshot({
            T: createCollection('T', [
              createField('field', 'string?', { required: false })
            ])
          })
        }
      ]

      for (const scenario of safeScenarios) {
        const changes = diffSchemas(scenario.before, scenario.after)
        expect(changes.compatible).toBe(true)
      }
    })
  })
})

// =============================================================================
// Edge Cases and Error Handling Tests
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
        createField('age', 'int?')
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
        createField('bio', 'text?')  // Added bio, removed age
      ]),
      Post: createCollection('Post', [
        createField('title', 'string!'),
        createField('content', 'text?')  // Added content
      ])
      // Comment collection removed
    })

    const changes = diffSchemas(before, after)

    expect(changes.compatible).toBe(false)

    // Should detect: remove age, add bio, add content, drop Comment
    expect(changes.changes.some(c => c.type === 'REMOVE_FIELD' && c.field === 'age')).toBe(true)
    expect(changes.changes.some(c => c.type === 'ADD_FIELD' && c.field === 'bio')).toBe(true)
    expect(changes.changes.some(c => c.type === 'ADD_FIELD' && c.field === 'content')).toBe(true)
    expect(changes.changes.some(c => c.type === 'DROP_COLLECTION' && c.collection === 'Comment')).toBe(true)
  })

  it('should handle rapid sequential schema changes', () => {
    // Simulate v1 -> v2 -> v3 evolution
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

    const v3 = createSnapshot({
      User: createCollection('User', [
        createField('email', 'string!'),
        createField('name', 'string!', { required: true }),
        createField('bio', 'text?')
      ])
    })

    const changes1to2 = diffSchemas(v1, v2)
    const changes2to3 = diffSchemas(v2, v3)
    const changes1to3 = diffSchemas(v1, v3)

    // v1 -> v2: safe (add optional field)
    expect(changes1to2.compatible).toBe(true)

    // v2 -> v3: breaking (optional -> required)
    expect(changes2to3.compatible).toBe(false)

    // v1 -> v3: breaking (add required field)
    expect(changes1to3.compatible).toBe(false)
  })
})
