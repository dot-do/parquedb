/**
 * Tests for runtime schema validation
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  SchemaValidationError,
  SchemaValidator,
  createValidator,
  validate,
} from '../../../src/schema/validator'
import type { Schema } from '../../../src/types/schema'

describe('SchemaValidator', () => {
  const testSchema: Schema = {
    User: {
      name: 'string!',
      email: 'email!',
      age: 'int?',
      bio: 'text',
      isActive: 'boolean = true',
      tags: 'string[]',
      role: 'enum(admin,user,guest)',
      avatar: 'url?',
      id: 'uuid?',
    },
    Post: {
      title: 'string!',
      content: 'markdown!',
      views: 'int = 0',
      rating: 'float?',
      publishedAt: 'datetime?',
      author: '-> User.posts',
      categories: '-> Category.posts[]',
    },
    Category: {
      name: 'string!',
      description: 'text?',
      posts: '<- Post.categories[]',
    },
    Profile: {
      displayName: { type: 'string!', required: true },
      website: { type: 'url' },
      maxLen: 'varchar(100)',
      embedding: 'vector(3)',
    },
  }

  let validator: SchemaValidator

  beforeEach(() => {
    validator = new SchemaValidator(testSchema, { mode: 'permissive' })
  })

  describe('Basic validation', () => {
    it('validates a valid entity', () => {
      const result = validator.validate('User', {
        name: 'Alice',
        email: 'alice@example.com',
        age: 30,
      })

      expect(result.valid).toBe(true)
      expect(result.errors).toHaveLength(0)
    })

    it('detects missing required fields', () => {
      const result = validator.validate('User', {
        name: 'Alice',
        // missing email
      })

      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.path === 'email' && e.code === 'REQUIRED')).toBe(true)
    })

    it('validates string types', () => {
      const result = validator.validate('User', {
        name: 123, // should be string
        email: 'valid@example.com',
      })

      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.path === 'name' && e.code === 'TYPE_MISMATCH')).toBe(true)
    })

    it('validates integer types', () => {
      const result = validator.validate('User', {
        name: 'Alice',
        email: 'alice@example.com',
        age: 30.5, // should be int
      })

      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.path === 'age' && e.code === 'TYPE_MISMATCH')).toBe(true)
    })

    it('validates boolean types', () => {
      const result = validator.validate('User', {
        name: 'Alice',
        email: 'alice@example.com',
        isActive: 'yes', // should be boolean
      })

      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.path === 'isActive' && e.code === 'TYPE_MISMATCH')).toBe(true)
    })

    it('allows optional fields to be undefined', () => {
      const result = validator.validate('User', {
        name: 'Alice',
        email: 'alice@example.com',
        // age, bio, etc. are all optional
      })

      expect(result.valid).toBe(true)
    })

    it('allows null for optional fields', () => {
      const result = validator.validate('User', {
        name: 'Alice',
        email: 'alice@example.com',
        age: null,
      })

      expect(result.valid).toBe(true)
    })
  })

  describe('Array validation', () => {
    it('validates array fields', () => {
      const result = validator.validate('User', {
        name: 'Alice',
        email: 'alice@example.com',
        tags: ['typescript', 'nodejs'],
      })

      expect(result.valid).toBe(true)
    })

    it('detects non-array for array field', () => {
      const result = validator.validate('User', {
        name: 'Alice',
        email: 'alice@example.com',
        tags: 'typescript', // should be array
      })

      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.path === 'tags' && e.code === 'EXPECTED_ARRAY')).toBe(true)
    })

    it('validates array element types', () => {
      const result = validator.validate('User', {
        name: 'Alice',
        email: 'alice@example.com',
        tags: ['typescript', 123], // all elements should be strings
      })

      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.path === 'tags[1]' && e.code === 'TYPE_MISMATCH')).toBe(true)
    })
  })

  describe('Email validation', () => {
    it('validates email format', () => {
      const result = validator.validate('User', {
        name: 'Alice',
        email: 'invalid-email',
      })

      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.path === 'email' && e.code === 'INVALID_FORMAT')).toBe(true)
    })

    it('accepts valid emails', () => {
      const result = validator.validate('User', {
        name: 'Alice',
        email: 'alice@example.com',
      })

      expect(result.valid).toBe(true)
    })
  })

  describe('UUID validation', () => {
    it('validates UUID format', () => {
      const result = validator.validate('User', {
        name: 'Alice',
        email: 'alice@example.com',
        id: 'not-a-uuid',
      })

      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.path === 'id' && e.code === 'INVALID_FORMAT')).toBe(true)
    })

    it('accepts valid UUIDs', () => {
      const result = validator.validate('User', {
        name: 'Alice',
        email: 'alice@example.com',
        id: '550e8400-e29b-41d4-a716-446655440000',
      })

      expect(result.valid).toBe(true)
    })
  })

  describe('URL validation', () => {
    it('validates URL format', () => {
      const result = validator.validate('User', {
        name: 'Alice',
        email: 'alice@example.com',
        avatar: 'not-a-url',
      })

      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.path === 'avatar' && e.code === 'INVALID_FORMAT')).toBe(true)
    })

    it('accepts valid URLs', () => {
      const result = validator.validate('User', {
        name: 'Alice',
        email: 'alice@example.com',
        avatar: 'https://example.com/avatar.png',
      })

      expect(result.valid).toBe(true)
    })
  })

  describe('Enum validation', () => {
    it('validates enum values', () => {
      const result = validator.validate('User', {
        name: 'Alice',
        email: 'alice@example.com',
        role: 'superuser', // not in enum
      })

      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.path === 'role' && e.code === 'INVALID_ENUM')).toBe(true)
    })

    it('accepts valid enum values', () => {
      const result = validator.validate('User', {
        name: 'Alice',
        email: 'alice@example.com',
        role: 'admin',
      })

      expect(result.valid).toBe(true)
    })
  })

  describe('Date/datetime validation', () => {
    it('accepts Date objects', () => {
      const result = validator.validate('Post', {
        title: 'Hello',
        content: 'World',
        publishedAt: new Date(),
      })

      expect(result.valid).toBe(true)
    })

    it('accepts ISO date strings', () => {
      const result = validator.validate('Post', {
        title: 'Hello',
        content: 'World',
        publishedAt: '2024-01-15T10:30:00Z',
      })

      expect(result.valid).toBe(true)
    })

    it('rejects invalid date strings', () => {
      const result = validator.validate('Post', {
        title: 'Hello',
        content: 'World',
        publishedAt: 'not-a-date',
      })

      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.path === 'publishedAt' && e.code === 'INVALID_FORMAT')).toBe(true)
    })
  })

  describe('Float validation', () => {
    it('accepts float numbers', () => {
      const result = validator.validate('Post', {
        title: 'Hello',
        content: 'World',
        rating: 4.5,
      })

      expect(result.valid).toBe(true)
    })

    it('accepts integers for float fields', () => {
      const result = validator.validate('Post', {
        title: 'Hello',
        content: 'World',
        rating: 5,
      })

      expect(result.valid).toBe(true)
    })

    it('rejects non-numbers for float fields', () => {
      const result = validator.validate('Post', {
        title: 'Hello',
        content: 'World',
        rating: '4.5',
      })

      expect(result.valid).toBe(false)
    })
  })

  describe('Varchar/char length validation', () => {
    it('validates varchar max length', () => {
      const result = validator.validate('Profile', {
        displayName: 'Alice',
        maxLen: 'A'.repeat(101), // exceeds varchar(100)
      })

      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.path === 'maxLen' && e.code === 'MAX_LENGTH')).toBe(true)
    })

    it('accepts strings within length', () => {
      const result = validator.validate('Profile', {
        displayName: 'Alice',
        maxLen: 'A'.repeat(100), // exactly 100
      })

      expect(result.valid).toBe(true)
    })
  })

  describe('Vector validation', () => {
    it('validates vector dimension', () => {
      const result = validator.validate('Profile', {
        displayName: 'Alice',
        embedding: [1.0, 2.0], // should be 3 dimensions
      })

      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.path === 'embedding' && e.code === 'DIMENSION_MISMATCH')).toBe(true)
    })

    it('accepts correct vector dimensions', () => {
      const result = validator.validate('Profile', {
        displayName: 'Alice',
        embedding: [1.0, 2.0, 3.0],
      })

      expect(result.valid).toBe(true)
    })

    it('validates vector element types', () => {
      const result = validator.validate('Profile', {
        displayName: 'Alice',
        embedding: [1.0, 'two', 3.0],
      })

      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.path.includes('embedding') && e.code === 'TYPE_MISMATCH')).toBe(true)
    })
  })

  describe('Relationship validation', () => {
    it('validates relationship string ID format', () => {
      const result = validator.validate('Post', {
        title: 'Hello',
        content: 'World',
        author: 'invalid-id', // should be ns/id format
      })

      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.path === 'author' && e.code === 'INVALID_RELATION_FORMAT')).toBe(true)
    })

    it('accepts valid relationship string ID', () => {
      const result = validator.validate('Post', {
        title: 'Hello',
        content: 'World',
        author: 'users/alice',
      })

      expect(result.valid).toBe(true)
    })

    it('validates relationship object format', () => {
      const result = validator.validate('Post', {
        title: 'Hello',
        content: 'World',
        author: { Alice: 'users/alice' },
      })

      expect(result.valid).toBe(true)
    })

    it('detects invalid relationship object references', () => {
      const result = validator.validate('Post', {
        title: 'Hello',
        content: 'World',
        author: { Alice: 'invalid' },
      })

      expect(result.valid).toBe(false)
    })
  })
})

describe('SchemaValidationError', () => {
  const schema: Schema = {
    User: {
      name: 'string!',
      email: 'email!',
    },
  }

  it('throws in strict mode', () => {
    const validator = new SchemaValidator(schema, { mode: 'strict' })

    expect(() => {
      validator.validate('User', { name: 'Alice' })
    }).toThrow(SchemaValidationError)
  })

  it('provides error details', () => {
    const validator = new SchemaValidator(schema, { mode: 'strict' })

    try {
      validator.validate('User', { name: 'Alice' })
      expect.fail('Should have thrown')
    } catch (error) {
      expect(error).toBeInstanceOf(SchemaValidationError)
      const validationError = error as SchemaValidationError

      expect(validationError.typeName).toBe('User')
      expect(validationError.errors.length).toBeGreaterThan(0)
      expect(validationError.getSummary()).toContain('email')
    }
  })

  it('provides field-specific errors', () => {
    const validator = new SchemaValidator(schema, { mode: 'strict' })

    try {
      validator.validate('User', { name: 123, email: 'invalid' })
      expect.fail('Should have thrown')
    } catch (error) {
      const validationError = error as SchemaValidationError
      const fieldErrors = validationError.getFieldErrors()

      expect(fieldErrors.has('name')).toBe(true)
      expect(fieldErrors.has('email')).toBe(true)
    }
  })
})

describe('Validation modes', () => {
  const schema: Schema = {
    User: {
      name: 'string!',
      email: 'email!',
    },
  }

  it('permissive mode returns errors without throwing', () => {
    const validator = new SchemaValidator(schema, { mode: 'permissive' })

    const result = validator.validate('User', { name: 'Alice' })

    expect(result.valid).toBe(false)
    expect(result.errors.length).toBeGreaterThan(0)
  })

  it('warn mode logs and returns errors', () => {
    const consoleSpy = { warn: vi.fn() }
    vi.spyOn(console, 'warn').mockImplementation(consoleSpy.warn)

    const validator = new SchemaValidator(schema, { mode: 'warn' })

    const result = validator.validate('User', { name: 'Alice' })

    expect(result.valid).toBe(false)
    expect(consoleSpy.warn).toHaveBeenCalled()

    vi.restoreAllMocks()
  })
})

describe('Unknown fields', () => {
  const schema: Schema = {
    User: {
      name: 'string!',
    },
  }

  it('allows unknown fields by default', () => {
    const validator = new SchemaValidator(schema)

    const result = validator.validate('User', {
      name: 'Alice',
      unknownField: 'value',
    })

    expect(result.valid).toBe(true)
  })

  it('rejects unknown fields when configured', () => {
    const validator = new SchemaValidator(schema, {
      mode: 'permissive',
      allowUnknownFields: false,
    })

    const result = validator.validate('User', {
      name: 'Alice',
      unknownField: 'value',
    })

    expect(result.valid).toBe(false)
    expect(result.errors.some(e => e.path === 'unknownField' && e.code === 'UNKNOWN_FIELD')).toBe(true)
  })
})

describe('Factory functions', () => {
  const schema: Schema = {
    User: {
      name: 'string!',
    },
  }

  it('createValidator creates a validator', () => {
    const validator = createValidator(schema)
    expect(validator).toBeInstanceOf(SchemaValidator)
  })

  it('validate function works', () => {
    const result = validate(schema, 'User', { name: 'Alice' })
    expect(result.valid).toBe(true)
  })
})

describe('Types not in schema', () => {
  const schema: Schema = {
    User: {
      name: 'string!',
    },
  }

  it('passes validation for types not in schema', () => {
    const validator = new SchemaValidator(schema)

    // UnknownType is not in schema
    const result = validator.validate('UnknownType', { anything: 'goes' })

    expect(result.valid).toBe(true)
  })
})

describe('Required fields with defaults', () => {
  const schema: Schema = {
    Config: {
      setting: 'string! = "default"',
    },
  }

  it('allows missing required fields if they have defaults', () => {
    const validator = new SchemaValidator(schema)

    // Setting is required but has a default
    const result = validator.validate('Config', {})

    // This should pass because the field has a default
    // (the actual default application happens in ParqueDB.create)
    expect(result.valid).toBe(true)
  })
})
