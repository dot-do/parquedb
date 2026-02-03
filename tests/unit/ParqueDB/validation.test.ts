import { describe, it, expect } from 'vitest'
import {
  validateEntityId,
  validateLocalId,
  normalizeEntityId,
  toFullId,
} from '../../../src/ParqueDB/validation'
import type { EntityId } from '../../../src/types/entity'

describe('validateEntityId', () => {
  describe('valid full EntityIds', () => {
    it('should accept a simple EntityId', () => {
      expect(() => validateEntityId('users/user-123')).not.toThrow()
    })

    it('should accept an EntityId with ULID', () => {
      expect(() => validateEntityId('posts/01HZXYZ123ABC456DEF789')).not.toThrow()
    })

    it('should accept an EntityId with multiple slashes in localId', () => {
      expect(() => validateEntityId('files/path/to/file.txt')).not.toThrow()
    })

    it('should accept an EntityId typed as EntityId', () => {
      const id = 'posts/post-456' as EntityId
      expect(() => validateEntityId(id)).not.toThrow()
    })

    it('should accept special characters in localId', () => {
      expect(() => validateEntityId('events/2024-01-15T10:30:00.000Z')).not.toThrow()
    })
  })

  describe('invalid EntityIds', () => {
    it('should throw for null', () => {
      expect(() => validateEntityId(null as unknown as string)).toThrow(
        'Entity ID is required and must be a non-empty string'
      )
    })

    it('should throw for undefined', () => {
      expect(() => validateEntityId(undefined as unknown as string)).toThrow(
        'Entity ID is required and must be a non-empty string'
      )
    })

    it('should throw for empty string', () => {
      expect(() => validateEntityId('')).toThrow(
        'Entity ID is required and must be a non-empty string'
      )
    })

    it('should throw for non-string', () => {
      expect(() => validateEntityId(123 as unknown as string)).toThrow(
        'Entity ID is required and must be a non-empty string'
      )
    })

    it('should throw for ID without slash separator', () => {
      expect(() => validateEntityId('invalidid')).toThrow(
        'Entity ID must be in "namespace/id" format'
      )
    })

    it('should throw for ID starting with slash (empty namespace)', () => {
      expect(() => validateEntityId('/localId')).toThrow(
        'Entity ID namespace cannot be empty'
      )
    })

    it('should throw for ID ending with slash (empty localId)', () => {
      expect(() => validateEntityId('namespace/')).toThrow(
        'Entity ID local part cannot be empty'
      )
    })

    it('should throw for just a slash', () => {
      expect(() => validateEntityId('/')).toThrow(
        'Entity ID namespace cannot be empty'
      )
    })

    it('should throw for namespace starting with underscore', () => {
      expect(() => validateEntityId('_internal/id-123')).toThrow(
        'Entity ID namespace cannot start with underscore'
      )
    })

    it('should throw for namespace starting with dollar sign', () => {
      expect(() => validateEntityId('$system/id-123')).toThrow(
        'Entity ID namespace cannot start with dollar sign'
      )
    })
  })
})

describe('validateLocalId', () => {
  describe('valid local IDs', () => {
    it('should accept a simple local ID', () => {
      expect(() => validateLocalId('user-123')).not.toThrow()
    })

    it('should accept a ULID', () => {
      expect(() => validateLocalId('01HZXYZ123ABC456DEF789')).not.toThrow()
    })

    it('should accept a UUID', () => {
      expect(() => validateLocalId('550e8400-e29b-41d4-a716-446655440000')).not.toThrow()
    })

    it('should accept numeric IDs', () => {
      expect(() => validateLocalId('12345')).not.toThrow()
    })

    it('should accept IDs with special characters', () => {
      expect(() => validateLocalId('user@example.com')).not.toThrow()
      expect(() => validateLocalId('2024-01-15T10:30:00.000Z')).not.toThrow()
    })

    it('should accept IDs with slashes (path-like)', () => {
      expect(() => validateLocalId('path/to/file.txt')).not.toThrow()
    })
  })

  describe('invalid local IDs', () => {
    it('should throw for null', () => {
      expect(() => validateLocalId(null as unknown as string)).toThrow(
        'Local ID is required and must be a non-empty string'
      )
    })

    it('should throw for undefined', () => {
      expect(() => validateLocalId(undefined as unknown as string)).toThrow(
        'Local ID is required and must be a non-empty string'
      )
    })

    it('should throw for empty string', () => {
      expect(() => validateLocalId('')).toThrow(
        'Local ID is required and must be a non-empty string'
      )
    })

    it('should throw for non-string', () => {
      expect(() => validateLocalId(123 as unknown as string)).toThrow(
        'Local ID is required and must be a non-empty string'
      )
    })
  })
})

describe('normalizeEntityId', () => {
  describe('full EntityIds (with namespace)', () => {
    it('should return full EntityId unchanged', () => {
      expect(normalizeEntityId('posts', 'users/user-123')).toBe('users/user-123')
    })

    it('should return system EntityId unchanged', () => {
      expect(normalizeEntityId('posts', 'system/parquedb')).toBe('system/parquedb')
    })

    it('should return EntityId with path unchanged', () => {
      expect(normalizeEntityId('files', 'files/path/to/doc.txt')).toBe('files/path/to/doc.txt')
    })
  })

  describe('local IDs (without namespace)', () => {
    it('should prefix local ID with namespace', () => {
      expect(normalizeEntityId('users', 'user-123')).toBe('users/user-123')
    })

    it('should handle ULID local ID', () => {
      expect(normalizeEntityId('posts', '01HZXYZ123ABC456DEF789')).toBe('posts/01HZXYZ123ABC456DEF789')
    })

    it('should handle UUID local ID', () => {
      expect(normalizeEntityId('items', '550e8400-e29b-41d4-a716-446655440000')).toBe('items/550e8400-e29b-41d4-a716-446655440000')
    })

    it('should handle numeric local ID', () => {
      expect(normalizeEntityId('orders', '12345')).toBe('orders/12345')
    })
  })

  describe('edge cases', () => {
    it('should validate namespace', () => {
      expect(() => normalizeEntityId('', 'id-123')).toThrow(
        'Namespace is required and must be a non-empty string'
      )
    })

    it('should validate local ID when no slash', () => {
      expect(() => normalizeEntityId('users', '')).toThrow(
        'Local ID is required and must be a non-empty string'
      )
    })

    it('should validate full EntityId format', () => {
      // If ID has slash, it should be validated as a full EntityId
      expect(() => normalizeEntityId('posts', '/invalid')).toThrow(
        'Entity ID namespace cannot be empty'
      )
    })

    it('should handle namespace normalization', () => {
      // Namespaces should be lowercase
      expect(normalizeEntityId('Users', 'user-123')).toBe('users/user-123')
    })
  })
})

describe('toFullId', () => {
  describe('ID without namespace (local ID)', () => {
    it('should prefix local ID with namespace', () => {
      expect(toFullId('users', 'user-123')).toBe('users/user-123')
    })

    it('should handle ULID local ID', () => {
      expect(toFullId('posts', '01HZXYZ123ABC456DEF789')).toBe('posts/01HZXYZ123ABC456DEF789')
    })

    it('should handle numeric local ID', () => {
      expect(toFullId('orders', '12345')).toBe('orders/12345')
    })
  })

  describe('ID with namespace (full ID)', () => {
    it('should return full ID unchanged', () => {
      expect(toFullId('posts', 'users/user-123')).toBe('users/user-123')
    })

    it('should preserve the original namespace in full ID', () => {
      expect(toFullId('posts', 'system/parquedb')).toBe('system/parquedb')
    })

    it('should preserve IDs with multiple slashes', () => {
      expect(toFullId('files', 'files/path/to/doc.txt')).toBe('files/path/to/doc.txt')
    })
  })

  describe('difference from normalizeEntityId', () => {
    it('should NOT validate namespace (unlike normalizeEntityId)', () => {
      // toFullId does not validate, so empty namespace is allowed
      expect(toFullId('', 'id-123')).toBe('/id-123')
    })

    it('should NOT normalize namespace to lowercase (unlike normalizeEntityId)', () => {
      // toFullId preserves the original case
      expect(toFullId('Users', 'user-123')).toBe('Users/user-123')
    })

    it('should NOT validate local ID (unlike normalizeEntityId)', () => {
      // toFullId does not validate, so empty local ID is allowed
      expect(toFullId('users', '')).toBe('users/')
    })
  })
})
