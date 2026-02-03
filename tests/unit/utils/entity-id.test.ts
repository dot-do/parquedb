import { describe, it, expect } from 'vitest'
import { parseEntityId, tryParseEntityId, userActorId } from '../../../src/utils/entity-id'
import type { EntityId } from '../../../src/types/entity'

describe('parseEntityId', () => {
  describe('valid EntityIds', () => {
    it('should parse a simple EntityId', () => {
      const result = parseEntityId('users/user-123')
      expect(result.ns).toBe('users')
      expect(result.localId).toBe('user-123')
    })

    it('should parse an EntityId with multiple slashes in localId', () => {
      const result = parseEntityId('files/path/to/file.txt')
      expect(result.ns).toBe('files')
      expect(result.localId).toBe('path/to/file.txt')
    })

    it('should parse an EntityId typed as EntityId', () => {
      const id = 'posts/post-456' as EntityId
      const result = parseEntityId(id)
      expect(result.ns).toBe('posts')
      expect(result.localId).toBe('post-456')
    })

    it('should handle single-character namespace', () => {
      const result = parseEntityId('a/id')
      expect(result.ns).toBe('a')
      expect(result.localId).toBe('id')
    })

    it('should handle single-character localId', () => {
      const result = parseEntityId('namespace/1')
      expect(result.ns).toBe('namespace')
      expect(result.localId).toBe('1')
    })

    it('should handle special characters in localId', () => {
      const result = parseEntityId('events/2024-01-15T10:30:00.000Z')
      expect(result.ns).toBe('events')
      expect(result.localId).toBe('2024-01-15T10:30:00.000Z')
    })

    it('should handle numeric localId', () => {
      const result = parseEntityId('items/12345')
      expect(result.ns).toBe('items')
      expect(result.localId).toBe('12345')
    })
  })

  describe('invalid EntityIds', () => {
    it('should throw for an ID without a slash', () => {
      expect(() => parseEntityId('invalidid')).toThrow(
        'Invalid EntityId: "invalidid" (must contain \'/\' separator)'
      )
    })

    it('should throw for an ID starting with a slash (empty namespace)', () => {
      expect(() => parseEntityId('/localId')).toThrow(
        'Invalid EntityId: "/localId" (namespace cannot be empty)'
      )
    })

    it('should throw for an ID ending with a slash (empty localId)', () => {
      expect(() => parseEntityId('namespace/')).toThrow(
        'Invalid EntityId: "namespace/" (localId cannot be empty)'
      )
    })

    it('should throw for empty string', () => {
      expect(() => parseEntityId('')).toThrow(
        'Invalid EntityId: "" (must contain \'/\' separator)'
      )
    })

    it('should throw for just a slash', () => {
      expect(() => parseEntityId('/')).toThrow(
        'Invalid EntityId: "/" (namespace cannot be empty)'
      )
    })
  })
})

describe('tryParseEntityId', () => {
  describe('valid EntityIds', () => {
    it('should parse a simple EntityId', () => {
      const result = tryParseEntityId('users/user-123')
      expect(result).not.toBeNull()
      expect(result!.ns).toBe('users')
      expect(result!.localId).toBe('user-123')
    })

    it('should parse an EntityId with multiple slashes', () => {
      const result = tryParseEntityId('files/path/to/file.txt')
      expect(result).not.toBeNull()
      expect(result!.ns).toBe('files')
      expect(result!.localId).toBe('path/to/file.txt')
    })
  })

  describe('invalid EntityIds', () => {
    it('should return null for an ID without a slash', () => {
      expect(tryParseEntityId('invalidid')).toBeNull()
    })

    it('should return null for an ID starting with a slash', () => {
      expect(tryParseEntityId('/localId')).toBeNull()
    })

    it('should return null for an ID ending with a slash', () => {
      expect(tryParseEntityId('namespace/')).toBeNull()
    })

    it('should return null for empty string', () => {
      expect(tryParseEntityId('')).toBeNull()
    })

    it('should return null for just a slash', () => {
      expect(tryParseEntityId('/')).toBeNull()
    })

    it('should return null for non-string values', () => {
      expect(tryParseEntityId(null)).toBeNull()
      expect(tryParseEntityId(undefined)).toBeNull()
      expect(tryParseEntityId(123)).toBeNull()
      expect(tryParseEntityId({ ns: 'test', localId: 'id' })).toBeNull()
      expect(tryParseEntityId(['users', 'id'])).toBeNull()
    })
  })
})

describe('userActorId', () => {
  describe('creating user actor IDs', () => {
    it('should prefix raw user ID with users/', () => {
      const result = userActorId('user-123')
      expect(result).toBe('users/user-123')
    })

    it('should handle numeric-like user IDs', () => {
      const result = userActorId('12345')
      expect(result).toBe('users/12345')
    })

    it('should handle UUID-style user IDs', () => {
      const result = userActorId('550e8400-e29b-41d4-a716-446655440000')
      expect(result).toBe('users/550e8400-e29b-41d4-a716-446655440000')
    })

    it('should handle email-like user IDs', () => {
      const result = userActorId('user@example.com')
      expect(result).toBe('users/user@example.com')
    })
  })

  describe('preserving existing EntityIds', () => {
    it('should return users/ EntityId unchanged', () => {
      const result = userActorId('users/user-123')
      expect(result).toBe('users/user-123')
    })

    it('should return other namespace EntityIds unchanged', () => {
      const result = userActorId('admins/admin-1')
      expect(result).toBe('admins/admin-1')
    })

    it('should return system actor unchanged', () => {
      const result = userActorId('system/parquedb')
      expect(result).toBe('system/parquedb')
    })

    it('should preserve EntityIds with nested paths', () => {
      const result = userActorId('teams/org-1/team-5')
      expect(result).toBe('teams/org-1/team-5')
    })
  })

  describe('edge cases', () => {
    it('should handle empty string by prefixing', () => {
      const result = userActorId('')
      expect(result).toBe('users/')
    })

    it('should handle ID that starts with slash by returning as-is', () => {
      // Edge case: if someone passes '/id', it has a slash so returned as-is
      const result = userActorId('/id')
      expect(result).toBe('/id')
    })
  })
})
