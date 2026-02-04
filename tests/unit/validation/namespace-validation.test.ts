/**
 * Namespace Validation Tests (RED phase)
 *
 * Tests for comprehensive namespace validation in ParqueDB.
 * These tests verify that namespace input is properly validated for:
 * - Length limits (max 128 characters)
 * - Character restrictions (alphanumeric, underscore, hyphen only)
 * - Path traversal prevention (no ../, \, etc.)
 * - Reserved keyword rejection (system, admin, etc.)
 * - Empty namespace rejection
 *
 * @see parquedb-js6o - Missing namespace input validation limits
 * @see parquedb-js6o.1 - RED: Write failing tests for namespace validation
 */

import { describe, it, expect } from 'vitest'
import { validateNamespace } from '../../../src/ParqueDB/validation'
import { ValidationError } from '../../../src/ParqueDB/types'

// =============================================================================
// Namespace Length Validation
// =============================================================================

describe('Namespace Validation - Length Limits', () => {
  it('should accept namespace at exactly 128 characters', () => {
    const namespace = 'a'.repeat(128)
    expect(() => validateNamespace(namespace)).not.toThrow()
  })

  it('should reject namespace over 128 characters', () => {
    const namespace = 'a'.repeat(129)
    expect(() => validateNamespace(namespace)).toThrow(ValidationError)
    expect(() => validateNamespace(namespace)).toThrow(/length|too long|128/i)
  })

  it('should reject namespace of 200 characters', () => {
    const namespace = 'verylongnamespace'.repeat(20) // 340 chars
    expect(() => validateNamespace(namespace)).toThrow(ValidationError)
  })

  it('should reject namespace of 1000 characters', () => {
    const namespace = 'x'.repeat(1000)
    expect(() => validateNamespace(namespace)).toThrow(ValidationError)
  })
})

// =============================================================================
// Namespace Character Validation
// =============================================================================

describe('Namespace Validation - Character Restrictions', () => {
  describe('Path traversal characters', () => {
    it('should reject namespace containing ../', () => {
      expect(() => validateNamespace('../users')).toThrow(ValidationError)
    })

    it('should reject namespace containing ..\\', () => {
      expect(() => validateNamespace('..\\users')).toThrow(ValidationError)
    })

    it('should reject namespace containing backslash', () => {
      expect(() => validateNamespace('users\\data')).toThrow(ValidationError)
    })

    it('should reject namespace containing ./', () => {
      expect(() => validateNamespace('./users')).toThrow(ValidationError)
    })

    it('should reject namespace containing only ..', () => {
      expect(() => validateNamespace('..')).toThrow(ValidationError)
    })

    it('should reject namespace containing %2e%2e (URL encoded ..)', () => {
      expect(() => validateNamespace('%2e%2e/users')).toThrow(ValidationError)
    })
  })

  describe('Special characters', () => {
    it('should reject namespace containing spaces', () => {
      expect(() => validateNamespace('my users')).toThrow(ValidationError)
    })

    it('should reject namespace containing @', () => {
      expect(() => validateNamespace('users@domain')).toThrow(ValidationError)
    })

    it('should reject namespace containing #', () => {
      expect(() => validateNamespace('users#1')).toThrow(ValidationError)
    })

    it('should reject namespace containing !', () => {
      expect(() => validateNamespace('users!')).toThrow(ValidationError)
    })

    it('should reject namespace containing *', () => {
      expect(() => validateNamespace('users*')).toThrow(ValidationError)
    })

    it('should reject namespace containing ?', () => {
      expect(() => validateNamespace('users?')).toThrow(ValidationError)
    })

    it('should reject namespace containing <', () => {
      expect(() => validateNamespace('users<data')).toThrow(ValidationError)
    })

    it('should reject namespace containing >', () => {
      expect(() => validateNamespace('users>data')).toThrow(ValidationError)
    })

    it('should reject namespace containing |', () => {
      expect(() => validateNamespace('users|data')).toThrow(ValidationError)
    })

    it('should reject namespace containing :', () => {
      expect(() => validateNamespace('users:data')).toThrow(ValidationError)
    })

    it('should reject namespace containing "', () => {
      expect(() => validateNamespace('users"data')).toThrow(ValidationError)
    })

    it('should reject namespace containing single quote', () => {
      expect(() => validateNamespace("users'data")).toThrow(ValidationError)
    })

    it('should reject namespace containing semicolon', () => {
      expect(() => validateNamespace('users;data')).toThrow(ValidationError)
    })

    it('should reject namespace containing comma', () => {
      expect(() => validateNamespace('users,data')).toThrow(ValidationError)
    })

    it('should reject namespace containing equals sign', () => {
      expect(() => validateNamespace('users=data')).toThrow(ValidationError)
    })

    it('should reject namespace containing parentheses', () => {
      expect(() => validateNamespace('users(data)')).toThrow(ValidationError)
    })

    it('should reject namespace containing brackets', () => {
      expect(() => validateNamespace('users[0]')).toThrow(ValidationError)
    })

    it('should reject namespace containing curly braces', () => {
      expect(() => validateNamespace('users{data}')).toThrow(ValidationError)
    })

    it('should reject namespace containing backtick', () => {
      expect(() => validateNamespace('users`data')).toThrow(ValidationError)
    })

    it('should reject namespace containing tilde', () => {
      expect(() => validateNamespace('~users')).toThrow(ValidationError)
    })

    it('should reject namespace containing caret', () => {
      expect(() => validateNamespace('users^data')).toThrow(ValidationError)
    })

    it('should reject namespace containing ampersand', () => {
      expect(() => validateNamespace('users&data')).toThrow(ValidationError)
    })

    it('should reject namespace containing percent', () => {
      expect(() => validateNamespace('users%data')).toThrow(ValidationError)
    })
  })

  describe('Valid characters', () => {
    it('should accept namespace with lowercase letters only', () => {
      expect(() => validateNamespace('users')).not.toThrow()
    })

    it('should accept namespace with uppercase letters only', () => {
      expect(() => validateNamespace('USERS')).not.toThrow()
    })

    it('should accept namespace with mixed case letters', () => {
      expect(() => validateNamespace('MyUsers')).not.toThrow()
    })

    it('should accept namespace with numbers', () => {
      expect(() => validateNamespace('users123')).not.toThrow()
    })

    it('should accept namespace with underscore', () => {
      expect(() => validateNamespace('my_users')).not.toThrow()
    })

    it('should accept namespace with hyphen', () => {
      expect(() => validateNamespace('my-users')).not.toThrow()
    })

    it('should accept namespace with mix of valid characters', () => {
      expect(() => validateNamespace('My_Users-123')).not.toThrow()
    })

    it('should accept namespace starting with number', () => {
      expect(() => validateNamespace('123users')).not.toThrow()
    })
  })
})

// =============================================================================
// Reserved Keywords Validation
// =============================================================================

describe('Namespace Validation - Reserved Keywords', () => {
  const reservedKeywords = [
    'system',
    'admin',
    'root',
    'null',
    'undefined',
    'true',
    'false',
    'internal',
    '__proto__',
    'constructor',
    'prototype',
    'config',
    'settings',
    'metadata',
    '_internal',
    '_system',
  ]

  reservedKeywords.forEach((keyword) => {
    it(`should reject reserved keyword: ${keyword}`, () => {
      expect(() => validateNamespace(keyword)).toThrow(ValidationError)
      expect(() => validateNamespace(keyword)).toThrow(/reserved|not allowed|forbidden/i)
    })
  })

  it('should reject reserved keywords case-insensitively for system', () => {
    expect(() => validateNamespace('SYSTEM')).toThrow(ValidationError)
    expect(() => validateNamespace('System')).toThrow(ValidationError)
    expect(() => validateNamespace('sYsTeM')).toThrow(ValidationError)
  })

  it('should reject reserved keywords case-insensitively for admin', () => {
    expect(() => validateNamespace('ADMIN')).toThrow(ValidationError)
    expect(() => validateNamespace('Admin')).toThrow(ValidationError)
    expect(() => validateNamespace('aDmIn')).toThrow(ValidationError)
  })

  it('should allow keywords that contain reserved words as substrings', () => {
    // These should be allowed because they're not exact matches
    expect(() => validateNamespace('systems')).not.toThrow()
    expect(() => validateNamespace('admins')).not.toThrow()
    expect(() => validateNamespace('systemconfig')).not.toThrow()
    expect(() => validateNamespace('useradmin')).not.toThrow()
  })
})

// =============================================================================
// Empty Namespace Validation
// =============================================================================

describe('Namespace Validation - Empty Namespace', () => {
  it('should reject empty string namespace', () => {
    expect(() => validateNamespace('')).toThrow(ValidationError)
  })

  it('should reject whitespace-only namespace', () => {
    expect(() => validateNamespace('   ')).toThrow(ValidationError)
  })

  it('should reject tab-only namespace', () => {
    expect(() => validateNamespace('\t')).toThrow(ValidationError)
  })

  it('should reject newline-only namespace', () => {
    expect(() => validateNamespace('\n')).toThrow(ValidationError)
  })

  it('should reject namespace with only whitespace characters mixed', () => {
    expect(() => validateNamespace(' \t\n ')).toThrow(ValidationError)
  })
})

// =============================================================================
// Edge Cases
// =============================================================================

describe('Namespace Validation - Edge Cases', () => {
  it('should reject null-byte in namespace', () => {
    expect(() => validateNamespace('users\x00data')).toThrow(ValidationError)
  })

  it('should reject control characters in namespace', () => {
    expect(() => validateNamespace('users\x01data')).toThrow(ValidationError)
    expect(() => validateNamespace('users\x1Fdata')).toThrow(ValidationError)
  })

  it('should reject unicode special characters', () => {
    expect(() => validateNamespace('users\u200Bdata')).toThrow(ValidationError) // Zero-width space
    expect(() => validateNamespace('users\uFEFFdata')).toThrow(ValidationError) // BOM
  })

  it('should accept single character namespace', () => {
    expect(() => validateNamespace('a')).not.toThrow()
  })

  it('should handle namespace with leading/trailing whitespace (should trim or reject)', () => {
    // Should either reject or trim - but currently the namespace includes whitespace
    // which would fail other validations anyway
    expect(() => validateNamespace(' users ')).toThrow(ValidationError)
  })

  it('should reject namespace with only dots', () => {
    expect(() => validateNamespace('.')).toThrow(ValidationError)
    expect(() => validateNamespace('..')).toThrow(ValidationError)
    expect(() => validateNamespace('...')).toThrow(ValidationError)
  })
})

// =============================================================================
// Integration with existing validation
// =============================================================================

describe('Namespace Validation - Existing Behavior', () => {
  // These tests verify that existing validation rules are still in place

  it('should reject namespace containing forward slash (existing rule)', () => {
    expect(() => validateNamespace('users/data')).toThrow(ValidationError)
  })

  it('should reject namespace starting with underscore (existing rule)', () => {
    expect(() => validateNamespace('_users')).toThrow(ValidationError)
  })

  it('should reject namespace starting with dollar sign (existing rule)', () => {
    expect(() => validateNamespace('$users')).toThrow(ValidationError)
  })
})
