/**
 * Schema Evolution Tests
 *
 * Tests for detecting breaking changes and generating migration hints
 */

import { describe, it, expect } from 'vitest'
import { detectBreakingChanges, generateMigrationHints, isSafeToApply } from '../../../src/sync/schema-evolution'
import type { SchemaChanges } from '../../../src/sync/schema-snapshot'

describe('Schema Evolution', () => {
  describe('detectBreakingChanges', () => {
    it('should detect DROP_COLLECTION as critical', () => {
      const changes: SchemaChanges = {
        changes: [
          {
            type: 'DROP_COLLECTION',
            collection: 'User',
            breaking: true,
            description: 'Dropped collection: User'
          }
        ],
        breakingChanges: [
          {
            type: 'DROP_COLLECTION',
            collection: 'User',
            breaking: true,
            description: 'Dropped collection: User'
          }
        ],
        compatible: false,
        summary: '1 breaking change'
      }

      const breaking = detectBreakingChanges(changes)

      expect(breaking).toHaveLength(1)
      expect(breaking[0]?.severity).toBe('critical')
      expect(breaking[0]?.impact).toContain('All data')
      expect(breaking[0]?.migrationHint).toBeDefined()
    })

    it('should detect REMOVE_FIELD as high severity', () => {
      const changes: SchemaChanges = {
        changes: [
          {
            type: 'REMOVE_FIELD',
            collection: 'User',
            field: 'email',
            breaking: true,
            description: 'Removed field: User.email'
          }
        ],
        breakingChanges: [
          {
            type: 'REMOVE_FIELD',
            collection: 'User',
            field: 'email',
            breaking: true,
            description: 'Removed field: User.email'
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

    it('should detect CHANGE_TYPE as critical', () => {
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

      const breaking = detectBreakingChanges(changes)

      expect(breaking).toHaveLength(1)
      expect(breaking[0]?.severity).toBe('critical')
      expect(breaking[0]?.impact).toContain('type changed')
      expect(breaking[0]?.migrationHint).toContain('migration script')
    })

    it('should detect making field required as high severity', () => {
      const changes: SchemaChanges = {
        changes: [
          {
            type: 'CHANGE_REQUIRED',
            collection: 'User',
            field: 'email',
            before: false,
            after: true,
            breaking: true,
            description: 'Changed required: User.email now required'
          }
        ],
        breakingChanges: [
          {
            type: 'CHANGE_REQUIRED',
            collection: 'User',
            field: 'email',
            before: false,
            after: true,
            breaking: true,
            description: 'Changed required: User.email now required'
          }
        ],
        compatible: false,
        summary: '1 breaking change'
      }

      const breaking = detectBreakingChanges(changes)

      expect(breaking).toHaveLength(1)
      expect(breaking[0]?.severity).toBe('high')
      expect(breaking[0]?.impact).toContain('now required')
      expect(breaking[0]?.migrationHint).toContain('updateMany')
    })

    it('should detect adding required field as high severity', () => {
      const changes: SchemaChanges = {
        changes: [
          {
            type: 'ADD_FIELD',
            collection: 'User',
            field: 'username',
            after: {
              name: 'username',
              type: 'string!',
              required: true,
              indexed: false,
              unique: false,
              array: false
            },
            breaking: true,
            description: 'Added field: User.username (required - BREAKING)'
          }
        ],
        breakingChanges: [
          {
            type: 'ADD_FIELD',
            collection: 'User',
            field: 'username',
            after: {
              name: 'username',
              type: 'string!',
              required: true,
              indexed: false,
              unique: false,
              array: false
            },
            breaking: true,
            description: 'Added field: User.username (required - BREAKING)'
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

  describe('generateMigrationHints', () => {
    it('should generate hints for breaking changes', () => {
      const changes: SchemaChanges = {
        changes: [
          {
            type: 'REMOVE_FIELD',
            collection: 'User',
            field: 'email',
            breaking: true,
            description: 'Removed field: User.email'
          }
        ],
        breakingChanges: [
          {
            type: 'REMOVE_FIELD',
            collection: 'User',
            field: 'email',
            breaking: true,
            description: 'Removed field: User.email'
          }
        ],
        compatible: false,
        summary: '1 breaking change'
      }

      const hints = generateMigrationHints(changes)

      expect(hints.length).toBeGreaterThan(0)
      expect(hints.join('\n')).toContain('BREAKING CHANGES')
      expect(hints.join('\n')).toContain('Recommended workflow')
      expect(hints.join('\n')).toContain('parquedb types generate')
    })

    it('should include non-breaking changes', () => {
      const changes: SchemaChanges = {
        changes: [
          {
            type: 'ADD_FIELD',
            collection: 'User',
            field: 'bio',
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
      expect(hints.join('\n')).toContain('Added field: User.bio')
    })

    it('should provide type generation hint', () => {
      const changes: SchemaChanges = {
        changes: [],
        breakingChanges: [],
        compatible: true,
        summary: 'No changes'
      }

      const hints = generateMigrationHints(changes)

      expect(hints.join('\n')).toContain('parquedb types generate')
    })
  })

  describe('isSafeToApply', () => {
    it('should return true for compatible changes', () => {
      const changes: SchemaChanges = {
        changes: [
          {
            type: 'ADD_FIELD',
            collection: 'User',
            field: 'bio',
            breaking: false,
            description: 'Added field: User.bio'
          }
        ],
        breakingChanges: [],
        compatible: true,
        summary: '1 change'
      }

      expect(isSafeToApply(changes)).toBe(true)
    })

    it('should return false for breaking changes', () => {
      const changes: SchemaChanges = {
        changes: [
          {
            type: 'REMOVE_FIELD',
            collection: 'User',
            field: 'email',
            breaking: true,
            description: 'Removed field: User.email'
          }
        ],
        breakingChanges: [
          {
            type: 'REMOVE_FIELD',
            collection: 'User',
            field: 'email',
            breaking: true,
            description: 'Removed field: User.email'
          }
        ],
        compatible: false,
        summary: '1 breaking change'
      }

      expect(isSafeToApply(changes)).toBe(false)
    })
  })
})
