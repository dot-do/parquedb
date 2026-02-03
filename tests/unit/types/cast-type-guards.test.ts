/**
 * Type Guard Tests for AI Database Adapter Casts
 *
 * Tests for the type guard functions that validate entity structures
 * at runtime for the ai-database adapter.
 */

import { describe, it, expect } from 'vitest'
import {
  isRecord,
  isAIDBEventEntity,
  isAIDBActionEntity,
  isAIDBArtifactEntity,
  hasRelationshipField,
  getRelationshipField,
  getStringField,
  getNumberField,
  getDateField,
  getRecordField,
  assertAIDBEventEntity,
  assertAIDBActionEntity,
  assertAIDBArtifactEntity,
  // Entity name extraction
  getEntityName,
  getEntityNameOrDefault,
  hasNameField,
  hasTitleField,
  // Entity type field guards
  hasTypeField,
  getTypeField,
  getTypeFieldOrDefault,
  // Entity ID field guards
  hasIdField,
  getIdField,
  // Safe record casts
  safeAsRecord,
  asRecordOrEmpty,
  // Additional field guards
  getBooleanField,
  getArrayField,
  getStringArrayField,
} from '../../../src/types/cast'

describe('Type Guards', () => {
  describe('isRecord()', () => {
    it('should return true for plain objects', () => {
      expect(isRecord({})).toBe(true)
      expect(isRecord({ foo: 'bar' })).toBe(true)
      expect(isRecord({ nested: { object: true } })).toBe(true)
    })

    it('should return false for null', () => {
      expect(isRecord(null)).toBe(false)
    })

    it('should return false for undefined', () => {
      expect(isRecord(undefined)).toBe(false)
    })

    it('should return false for arrays', () => {
      expect(isRecord([])).toBe(false)
      expect(isRecord([1, 2, 3])).toBe(false)
    })

    it('should return false for primitives', () => {
      expect(isRecord('string')).toBe(false)
      expect(isRecord(123)).toBe(false)
      expect(isRecord(true)).toBe(false)
    })
  })

  describe('isAIDBEventEntity()', () => {
    it('should return true for valid event entity', () => {
      expect(isAIDBEventEntity({
        actor: 'user123',
        event: 'User.created',
        object: 'users/456',
        timestamp: new Date(),
      })).toBe(true)
    })

    it('should return true for empty object (all fields optional)', () => {
      expect(isAIDBEventEntity({})).toBe(true)
    })

    it('should return true for partial event entity', () => {
      expect(isAIDBEventEntity({
        actor: 'system',
        event: 'test.event',
      })).toBe(true)
    })

    it('should return false for non-object', () => {
      expect(isAIDBEventEntity(null)).toBe(false)
      expect(isAIDBEventEntity('string')).toBe(false)
      expect(isAIDBEventEntity(123)).toBe(false)
    })

    it('should return false for invalid actor type', () => {
      expect(isAIDBEventEntity({ actor: 123 })).toBe(false)
    })

    it('should return false for invalid event type', () => {
      expect(isAIDBEventEntity({ event: { nested: true } })).toBe(false)
    })

    it('should return false for invalid objectData type', () => {
      expect(isAIDBEventEntity({ objectData: 'not-an-object' })).toBe(false)
    })

    it('should accept timestamp as string (ISO date)', () => {
      expect(isAIDBEventEntity({
        timestamp: '2024-01-15T10:30:00Z',
      })).toBe(true)
    })
  })

  describe('isAIDBActionEntity()', () => {
    it('should return true for valid action entity', () => {
      expect(isAIDBActionEntity({
        actor: 'system',
        action: 'generate',
        act: 'generates',
        activity: 'generating',
        status: 'pending',
        progress: 50,
        total: 100,
        createdAt: new Date(),
      })).toBe(true)
    })

    it('should return true for empty object', () => {
      expect(isAIDBActionEntity({})).toBe(true)
    })

    it('should return false for invalid status', () => {
      expect(isAIDBActionEntity({ status: 'invalid-status' })).toBe(false)
    })

    it('should accept all valid statuses', () => {
      const validStatuses = ['pending', 'active', 'completed', 'failed', 'cancelled']
      for (const status of validStatuses) {
        expect(isAIDBActionEntity({ status })).toBe(true)
      }
    })

    it('should return false for invalid progress type', () => {
      expect(isAIDBActionEntity({ progress: 'fifty' })).toBe(false)
    })

    it('should return false for invalid total type', () => {
      expect(isAIDBActionEntity({ total: '100' })).toBe(false)
    })

    it('should return false for invalid result type', () => {
      expect(isAIDBActionEntity({ result: 'not-an-object' })).toBe(false)
    })
  })

  describe('isAIDBArtifactEntity()', () => {
    it('should return true for valid artifact entity', () => {
      expect(isAIDBArtifactEntity({
        url: 'https://example.com/doc.pdf',
        type: 'embedding',
        sourceHash: 'abc123',
        content: [0.1, 0.2, 0.3],
        metadata: { model: 'text-embedding-3-small' },
        createdAt: new Date(),
      })).toBe(true)
    })

    it('should return true for empty object', () => {
      expect(isAIDBArtifactEntity({})).toBe(true)
    })

    it('should return false for invalid url type', () => {
      expect(isAIDBArtifactEntity({ url: 123 })).toBe(false)
    })

    it('should return false for invalid sourceHash type', () => {
      expect(isAIDBArtifactEntity({ sourceHash: { hash: 'abc' } })).toBe(false)
    })

    it('should return false for invalid metadata type', () => {
      expect(isAIDBArtifactEntity({ metadata: 'not-an-object' })).toBe(false)
    })

    it('should accept any content type', () => {
      expect(isAIDBArtifactEntity({ content: 'string' })).toBe(true)
      expect(isAIDBArtifactEntity({ content: [1, 2, 3] })).toBe(true)
      expect(isAIDBArtifactEntity({ content: { nested: true } })).toBe(true)
      expect(isAIDBArtifactEntity({ content: null })).toBe(true)
    })
  })

  describe('hasRelationshipField()', () => {
    it('should return true for valid relationship field', () => {
      const entity = {
        $id: 'posts/123',
        author: { 'Alice': 'users/alice' },
      }
      expect(hasRelationshipField(entity, 'author')).toBe(true)
    })

    it('should return false for missing field', () => {
      const entity = { $id: 'posts/123' }
      expect(hasRelationshipField(entity, 'author')).toBe(false)
    })

    it('should return false for null field', () => {
      const entity = { author: null }
      expect(hasRelationshipField(entity, 'author')).toBe(false)
    })

    it('should return false for primitive field', () => {
      const entity = { author: 'users/alice' }
      expect(hasRelationshipField(entity, 'author')).toBe(false)
    })

    it('should return false for non-object value', () => {
      expect(hasRelationshipField(null, 'field')).toBe(false)
      expect(hasRelationshipField('string', 'field')).toBe(false)
    })
  })

  describe('getRelationshipField()', () => {
    it('should return relationship field value', () => {
      const entity = {
        author: { 'Alice': 'users/alice', 'Bob': 'users/bob' },
      }
      const result = getRelationshipField(entity, 'author')
      expect(result).toEqual({ 'Alice': 'users/alice', 'Bob': 'users/bob' })
    })

    it('should return undefined for missing field', () => {
      const entity = { $id: 'posts/123' }
      expect(getRelationshipField(entity, 'author')).toBeUndefined()
    })

    it('should return undefined for invalid field type', () => {
      const entity = { author: 'not-a-relationship' }
      expect(getRelationshipField(entity, 'author')).toBeUndefined()
    })
  })

  describe('getStringField()', () => {
    it('should return string value', () => {
      expect(getStringField({ name: 'Alice' }, 'name')).toBe('Alice')
    })

    it('should return undefined for missing field', () => {
      expect(getStringField({}, 'name')).toBeUndefined()
    })

    it('should return undefined for non-string field', () => {
      expect(getStringField({ name: 123 }, 'name')).toBeUndefined()
      expect(getStringField({ name: { nested: true } }, 'name')).toBeUndefined()
    })

    it('should return undefined for non-object', () => {
      expect(getStringField(null, 'name')).toBeUndefined()
      expect(getStringField('string', 'name')).toBeUndefined()
    })
  })

  describe('getNumberField()', () => {
    it('should return number value', () => {
      expect(getNumberField({ age: 30 }, 'age')).toBe(30)
      expect(getNumberField({ score: 0 }, 'score')).toBe(0)
      expect(getNumberField({ negative: -5 }, 'negative')).toBe(-5)
    })

    it('should return undefined for missing field', () => {
      expect(getNumberField({}, 'age')).toBeUndefined()
    })

    it('should return undefined for non-number field', () => {
      expect(getNumberField({ age: '30' }, 'age')).toBeUndefined()
      expect(getNumberField({ age: null }, 'age')).toBeUndefined()
    })
  })

  describe('getDateField()', () => {
    it('should return Date value', () => {
      const date = new Date('2024-01-15')
      expect(getDateField({ createdAt: date }, 'createdAt')).toEqual(date)
    })

    it('should parse ISO string to Date', () => {
      const result = getDateField({ createdAt: '2024-01-15T10:30:00Z' }, 'createdAt')
      expect(result).toBeInstanceOf(Date)
      expect(result?.toISOString()).toBe('2024-01-15T10:30:00.000Z')
    })

    it('should return undefined for invalid date string', () => {
      expect(getDateField({ createdAt: 'not-a-date' }, 'createdAt')).toBeUndefined()
    })

    it('should return undefined for missing field', () => {
      expect(getDateField({}, 'createdAt')).toBeUndefined()
    })

    it('should return undefined for non-date field', () => {
      expect(getDateField({ createdAt: 12345 }, 'createdAt')).toBeUndefined()
      expect(getDateField({ createdAt: null }, 'createdAt')).toBeUndefined()
    })
  })

  describe('getRecordField()', () => {
    it('should return record value', () => {
      const meta = { foo: 'bar', count: 5 }
      expect(getRecordField({ meta }, 'meta')).toEqual(meta)
    })

    it('should return undefined for missing field', () => {
      expect(getRecordField({}, 'meta')).toBeUndefined()
    })

    it('should return undefined for non-object field', () => {
      expect(getRecordField({ meta: 'string' }, 'meta')).toBeUndefined()
      expect(getRecordField({ meta: [1, 2] }, 'meta')).toBeUndefined()
    })

    it('should return undefined for null field', () => {
      expect(getRecordField({ meta: null }, 'meta')).toBeUndefined()
    })
  })

  describe('assertAIDBEventEntity()', () => {
    it('should return value for valid event entity', () => {
      const entity = { actor: 'system', event: 'test' }
      expect(assertAIDBEventEntity(entity)).toBe(entity)
    })

    it('should throw for invalid entity', () => {
      expect(() => assertAIDBEventEntity({ actor: 123 })).toThrow('Invalid AI Database Event entity')
    })

    it('should include context in error message', () => {
      expect(() => assertAIDBEventEntity({ actor: 123 }, 'listEvents')).toThrow('(listEvents)')
    })
  })

  describe('assertAIDBActionEntity()', () => {
    it('should return value for valid action entity', () => {
      const entity = { actor: 'system', action: 'test', status: 'pending' }
      expect(assertAIDBActionEntity(entity)).toBe(entity)
    })

    it('should throw for invalid entity', () => {
      expect(() => assertAIDBActionEntity({ status: 'invalid' })).toThrow('Invalid AI Database Action entity')
    })
  })

  describe('assertAIDBArtifactEntity()', () => {
    it('should return value for valid artifact entity', () => {
      const entity = { url: 'https://example.com', type: 'test' }
      expect(assertAIDBArtifactEntity(entity)).toBe(entity)
    })

    it('should throw for invalid entity', () => {
      expect(() => assertAIDBArtifactEntity({ url: 123 })).toThrow('Invalid AI Database Artifact entity')
    })
  })

  // ===========================================================================
  // Entity Name Extraction Type Guards
  // ===========================================================================

  describe('getEntityName()', () => {
    it('should return name field if present and non-empty', () => {
      expect(getEntityName({ name: 'Alice' })).toBe('Alice')
      expect(getEntityName({ name: 'Test', title: 'Other' })).toBe('Test')
    })

    it('should return title field if name is missing', () => {
      expect(getEntityName({ title: 'My Post' })).toBe('My Post')
    })

    it('should return title field if name is empty string', () => {
      expect(getEntityName({ name: '', title: 'Fallback Title' })).toBe('Fallback Title')
    })

    it('should return undefined if neither name nor title exists', () => {
      expect(getEntityName({})).toBeUndefined()
      expect(getEntityName({ description: 'Something' })).toBeUndefined()
    })

    it('should return undefined if name and title are wrong type', () => {
      expect(getEntityName({ name: 123 })).toBeUndefined()
      expect(getEntityName({ title: { nested: true } })).toBeUndefined()
      expect(getEntityName({ name: null, title: null })).toBeUndefined()
    })
  })

  describe('getEntityNameOrDefault()', () => {
    it('should return name when available', () => {
      expect(getEntityNameOrDefault({ name: 'Alice' }, 'default')).toBe('Alice')
    })

    it('should return title when name missing', () => {
      expect(getEntityNameOrDefault({ title: 'Post Title' }, 'default')).toBe('Post Title')
    })

    it('should return fallback when neither exists', () => {
      expect(getEntityNameOrDefault({}, 'fallback-id')).toBe('fallback-id')
      expect(getEntityNameOrDefault({ other: 'value' }, 'my-id')).toBe('my-id')
    })

    it('should return fallback for empty strings', () => {
      expect(getEntityNameOrDefault({ name: '', title: '' }, 'default')).toBe('default')
    })
  })

  describe('hasNameField()', () => {
    it('should return true for valid name field', () => {
      expect(hasNameField({ name: 'Alice' })).toBe(true)
      expect(hasNameField({ name: 'Test', other: 123 })).toBe(true)
    })

    it('should return false for missing name field', () => {
      expect(hasNameField({})).toBe(false)
      expect(hasNameField({ title: 'Post' })).toBe(false)
    })

    it('should return false for non-string name', () => {
      expect(hasNameField({ name: 123 })).toBe(false)
      expect(hasNameField({ name: null })).toBe(false)
      expect(hasNameField({ name: { nested: true } })).toBe(false)
    })

    it('should return false for non-objects', () => {
      expect(hasNameField(null)).toBe(false)
      expect(hasNameField('string')).toBe(false)
      expect(hasNameField(123)).toBe(false)
    })
  })

  describe('hasTitleField()', () => {
    it('should return true for valid title field', () => {
      expect(hasTitleField({ title: 'My Post' })).toBe(true)
      expect(hasTitleField({ title: 'Test', name: 'other' })).toBe(true)
    })

    it('should return false for missing title field', () => {
      expect(hasTitleField({})).toBe(false)
      expect(hasTitleField({ name: 'Alice' })).toBe(false)
    })

    it('should return false for non-string title', () => {
      expect(hasTitleField({ title: 123 })).toBe(false)
      expect(hasTitleField({ title: null })).toBe(false)
    })
  })

  // ===========================================================================
  // Entity Type Field Guards
  // ===========================================================================

  describe('hasTypeField()', () => {
    it('should return true for valid $type field', () => {
      expect(hasTypeField({ $type: 'User' })).toBe(true)
      expect(hasTypeField({ $type: 'Post', $id: 'posts/1' })).toBe(true)
    })

    it('should return false for missing $type field', () => {
      expect(hasTypeField({})).toBe(false)
      expect(hasTypeField({ type: 'User' })).toBe(false) // not $type
    })

    it('should return false for non-string $type', () => {
      expect(hasTypeField({ $type: 123 })).toBe(false)
      expect(hasTypeField({ $type: null })).toBe(false)
      expect(hasTypeField({ $type: { nested: true } })).toBe(false)
    })

    it('should return false for non-objects', () => {
      expect(hasTypeField(null)).toBe(false)
      expect(hasTypeField('string')).toBe(false)
    })
  })

  describe('getTypeField()', () => {
    it('should return $type value', () => {
      expect(getTypeField({ $type: 'User' })).toBe('User')
      expect(getTypeField({ $type: 'Post', other: 123 })).toBe('Post')
    })

    it('should return undefined for missing $type', () => {
      expect(getTypeField({})).toBeUndefined()
      expect(getTypeField({ type: 'User' })).toBeUndefined()
    })

    it('should return undefined for invalid $type', () => {
      expect(getTypeField({ $type: 123 })).toBeUndefined()
      expect(getTypeField(null)).toBeUndefined()
    })
  })

  describe('getTypeFieldOrDefault()', () => {
    it('should return $type when available', () => {
      expect(getTypeFieldOrDefault({ $type: 'User' }, 'unknown')).toBe('User')
    })

    it('should return fallback when $type missing', () => {
      expect(getTypeFieldOrDefault({}, 'unknown')).toBe('unknown')
      expect(getTypeFieldOrDefault({ type: 'User' }, 'fallback')).toBe('fallback')
    })

    it('should return fallback for invalid $type', () => {
      expect(getTypeFieldOrDefault({ $type: 123 }, 'unknown')).toBe('unknown')
    })
  })

  // ===========================================================================
  // Entity ID Field Guards
  // ===========================================================================

  describe('hasIdField()', () => {
    it('should return true for valid $id field', () => {
      expect(hasIdField({ $id: 'users/alice' })).toBe(true)
      expect(hasIdField({ $id: 'posts/123', $type: 'Post' })).toBe(true)
    })

    it('should return false for missing $id field', () => {
      expect(hasIdField({})).toBe(false)
      expect(hasIdField({ id: 'users/alice' })).toBe(false) // not $id
    })

    it('should return false for non-string $id', () => {
      expect(hasIdField({ $id: 123 })).toBe(false)
      expect(hasIdField({ $id: null })).toBe(false)
    })
  })

  describe('getIdField()', () => {
    it('should return $id value', () => {
      expect(getIdField({ $id: 'users/alice' })).toBe('users/alice')
    })

    it('should return undefined for missing $id', () => {
      expect(getIdField({})).toBeUndefined()
      expect(getIdField({ id: 'users/alice' })).toBeUndefined()
    })

    it('should return undefined for invalid $id', () => {
      expect(getIdField({ $id: 123 })).toBeUndefined()
      expect(getIdField(null)).toBeUndefined()
    })
  })

  // ===========================================================================
  // Safe Record Casts
  // ===========================================================================

  describe('safeAsRecord()', () => {
    it('should return record for valid objects', () => {
      const obj = { foo: 'bar' }
      expect(safeAsRecord(obj)).toBe(obj)
      expect(safeAsRecord({})).toEqual({})
    })

    it('should return undefined for non-objects', () => {
      expect(safeAsRecord(null)).toBeUndefined()
      expect(safeAsRecord('string')).toBeUndefined()
      expect(safeAsRecord(123)).toBeUndefined()
      expect(safeAsRecord([1, 2, 3])).toBeUndefined()
    })
  })

  describe('asRecordOrEmpty()', () => {
    it('should return record for valid objects', () => {
      const obj = { foo: 'bar' }
      expect(asRecordOrEmpty(obj)).toBe(obj)
    })

    it('should return empty object for non-objects', () => {
      expect(asRecordOrEmpty(null)).toEqual({})
      expect(asRecordOrEmpty('string')).toEqual({})
      expect(asRecordOrEmpty(123)).toEqual({})
      expect(asRecordOrEmpty([1, 2, 3])).toEqual({})
      expect(asRecordOrEmpty(undefined)).toEqual({})
    })
  })

  // ===========================================================================
  // Additional Field Guards
  // ===========================================================================

  describe('getBooleanField()', () => {
    it('should return boolean value', () => {
      expect(getBooleanField({ active: true }, 'active')).toBe(true)
      expect(getBooleanField({ enabled: false }, 'enabled')).toBe(false)
    })

    it('should return undefined for missing field', () => {
      expect(getBooleanField({}, 'active')).toBeUndefined()
    })

    it('should return undefined for non-boolean field', () => {
      expect(getBooleanField({ active: 1 }, 'active')).toBeUndefined()
      expect(getBooleanField({ active: 'true' }, 'active')).toBeUndefined()
      expect(getBooleanField({ active: null }, 'active')).toBeUndefined()
    })

    it('should return undefined for non-object', () => {
      expect(getBooleanField(null, 'active')).toBeUndefined()
      expect(getBooleanField('string', 'active')).toBeUndefined()
    })
  })

  describe('getArrayField()', () => {
    it('should return array value', () => {
      expect(getArrayField({ items: [1, 2, 3] }, 'items')).toEqual([1, 2, 3])
      expect(getArrayField({ tags: ['a', 'b'] }, 'tags')).toEqual(['a', 'b'])
      expect(getArrayField({ empty: [] }, 'empty')).toEqual([])
    })

    it('should return undefined for missing field', () => {
      expect(getArrayField({}, 'items')).toBeUndefined()
    })

    it('should return undefined for non-array field', () => {
      expect(getArrayField({ items: 'not-array' }, 'items')).toBeUndefined()
      expect(getArrayField({ items: { 0: 'a' } }, 'items')).toBeUndefined()
      expect(getArrayField({ items: null }, 'items')).toBeUndefined()
    })

    it('should return undefined for non-object', () => {
      expect(getArrayField(null, 'items')).toBeUndefined()
    })
  })

  describe('getStringArrayField()', () => {
    it('should return string array value', () => {
      expect(getStringArrayField({ tags: ['a', 'b', 'c'] }, 'tags')).toEqual(['a', 'b', 'c'])
      expect(getStringArrayField({ items: [] }, 'items')).toEqual([])
    })

    it('should return undefined for missing field', () => {
      expect(getStringArrayField({}, 'tags')).toBeUndefined()
    })

    it('should return undefined for mixed array', () => {
      expect(getStringArrayField({ tags: ['a', 1, 'b'] }, 'tags')).toBeUndefined()
      expect(getStringArrayField({ tags: [1, 2, 3] }, 'tags')).toBeUndefined()
    })

    it('should return undefined for non-array field', () => {
      expect(getStringArrayField({ tags: 'not-array' }, 'tags')).toBeUndefined()
    })

    it('should return undefined for non-object', () => {
      expect(getStringArrayField(null, 'tags')).toBeUndefined()
    })
  })
})
