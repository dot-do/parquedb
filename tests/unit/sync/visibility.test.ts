/**
 * Visibility Tests
 *
 * Tests for visibility types and access control logic.
 */

import { describe, it, expect } from 'vitest'
import {
  type Visibility,
  DEFAULT_VISIBILITY,
  VISIBILITY_VALUES,
  isValidVisibility,
  parseVisibility,
  allowsAnonymousRead,
  allowsDiscovery,
} from '../../../src/types/visibility'

describe('Visibility Types', () => {
  describe('DEFAULT_VISIBILITY', () => {
    it('should default to private', () => {
      expect(DEFAULT_VISIBILITY).toBe('private')
    })
  })

  describe('VISIBILITY_VALUES', () => {
    it('should contain all visibility levels', () => {
      expect(VISIBILITY_VALUES).toContain('public')
      expect(VISIBILITY_VALUES).toContain('unlisted')
      expect(VISIBILITY_VALUES).toContain('private')
      expect(VISIBILITY_VALUES).toHaveLength(3)
    })
  })

  describe('isValidVisibility', () => {
    it('should return true for valid visibility values', () => {
      expect(isValidVisibility('public')).toBe(true)
      expect(isValidVisibility('unlisted')).toBe(true)
      expect(isValidVisibility('private')).toBe(true)
    })

    it('should return false for invalid values', () => {
      expect(isValidVisibility('PUBLIC')).toBe(false) // case sensitive
      expect(isValidVisibility('internal')).toBe(false)
      expect(isValidVisibility('')).toBe(false)
      expect(isValidVisibility(null)).toBe(false)
      expect(isValidVisibility(undefined)).toBe(false)
      expect(isValidVisibility(123)).toBe(false)
    })
  })

  describe('parseVisibility', () => {
    it('should return valid visibility values as-is', () => {
      expect(parseVisibility('public')).toBe('public')
      expect(parseVisibility('unlisted')).toBe('unlisted')
      expect(parseVisibility('private')).toBe('private')
    })

    it('should return default for undefined', () => {
      expect(parseVisibility(undefined)).toBe(DEFAULT_VISIBILITY)
    })

    it('should return default for invalid values', () => {
      expect(parseVisibility('invalid')).toBe(DEFAULT_VISIBILITY)
      expect(parseVisibility('')).toBe(DEFAULT_VISIBILITY)
    })
  })

  describe('allowsAnonymousRead', () => {
    it('should allow anonymous read for public databases', () => {
      expect(allowsAnonymousRead('public')).toBe(true)
    })

    it('should allow anonymous read for unlisted databases', () => {
      expect(allowsAnonymousRead('unlisted')).toBe(true)
    })

    it('should NOT allow anonymous read for private databases', () => {
      expect(allowsAnonymousRead('private')).toBe(false)
    })
  })

  describe('allowsDiscovery', () => {
    it('should allow discovery for public databases', () => {
      expect(allowsDiscovery('public')).toBe(true)
    })

    it('should NOT allow discovery for unlisted databases', () => {
      expect(allowsDiscovery('unlisted')).toBe(false)
    })

    it('should NOT allow discovery for private databases', () => {
      expect(allowsDiscovery('private')).toBe(false)
    })
  })
})

describe('Visibility Matrix', () => {
  const matrix: { visibility: Visibility; discoverable: boolean; anonymousRead: boolean; requiresAuth: boolean }[] = [
    { visibility: 'public', discoverable: true, anonymousRead: true, requiresAuth: false },
    { visibility: 'unlisted', discoverable: false, anonymousRead: true, requiresAuth: false },
    { visibility: 'private', discoverable: false, anonymousRead: false, requiresAuth: true },
  ]

  it.each(matrix)(
    '$visibility: discoverable=$discoverable, anonymousRead=$anonymousRead, requiresAuth=$requiresAuth',
    ({ visibility, discoverable, anonymousRead, requiresAuth }) => {
      expect(allowsDiscovery(visibility)).toBe(discoverable)
      expect(allowsAnonymousRead(visibility)).toBe(anonymousRead)
      expect(!allowsAnonymousRead(visibility)).toBe(requiresAuth)
    }
  )
})
