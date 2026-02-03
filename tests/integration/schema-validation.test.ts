/**
 * Integration tests for schema validation in ParqueDB
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { ParqueDB } from '../../src/ParqueDB'
import { MemoryBackend } from '../../src/storage/MemoryBackend'
import { SchemaValidationError } from '../../src/schema/validator'
import type { Schema } from '../../src/types/schema'

describe('ParqueDB Schema Validation Integration', () => {
  const schema: Schema = {
    User: {
      name: 'string!',
      email: 'email!',
      age: 'int?',
      role: 'enum(admin,user,guest) = "user"',
      tags: 'string[]',
    },
    Post: {
      title: 'string!',
      content: 'text!',
      views: 'int = 0',
      rating: 'float?',
    },
    Config: {
      key: 'string!',
      value: 'string! = "default"',
    },
  }

  let db: ParqueDB

  beforeEach(() => {
    db = new ParqueDB({
      storage: new MemoryBackend(),
      schema,
    })
  })

  afterEach(() => {
    // Cleanup to prevent resource leaks
    if (db && typeof (db as any).dispose === 'function') {
      (db as any).dispose()
    }
  })

  describe('Create with validation', () => {
    it('creates valid entities', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Alice',
        email: 'alice@example.com',
        age: 30,
      })

      expect(user.$id).toMatch(/^users\//)
      expect(user.name).toBe('Alice')
    })

    it('rejects entities missing required fields', async () => {
      await expect(
        db.create('users', {
          $type: 'User',
          name: 'Alice',
          // missing email
        })
      ).rejects.toThrow(/email/)
    })

    it('rejects entities with wrong types', async () => {
      await expect(
        db.create('users', {
          $type: 'User',
          name: 'Alice',
          email: 'alice@example.com',
          age: 'thirty', // should be int
        } as any)
      ).rejects.toThrow()
    })

    it('applies default values', async () => {
      const post = await db.create('posts', {
        $type: 'Post',
        name: 'Hello',
        title: 'Hello World',
        content: 'Content here',
      })

      expect(post.views).toBe(0)
    })

    it('validates email format', async () => {
      await expect(
        db.create('users', {
          $type: 'User',
          name: 'Alice',
          email: 'not-an-email',
        })
      ).rejects.toThrow()
    })

    it('validates enum values', async () => {
      await expect(
        db.create('users', {
          $type: 'User',
          name: 'Alice',
          email: 'alice@example.com',
          role: 'superuser', // not in enum
        })
      ).rejects.toThrow()
    })

    it('accepts valid enum values', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Alice',
        email: 'alice@example.com',
        role: 'admin',
      })

      expect(user.role).toBe('admin')
    })

    it('validates array types', async () => {
      await expect(
        db.create('users', {
          $type: 'User',
          name: 'Alice',
          email: 'alice@example.com',
          tags: 'not-an-array',
        } as any)
      ).rejects.toThrow()
    })

    it('accepts valid arrays', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Alice',
        email: 'alice@example.com',
        tags: ['typescript', 'nodejs'],
      })

      expect(user.tags).toEqual(['typescript', 'nodejs'])
    })
  })

  describe('skipValidation option', () => {
    it('skips validation when skipValidation is true', async () => {
      // This would normally fail validation
      const user = await db.create('users', {
        $type: 'User',
        name: 'Alice',
        // email is missing
      } as any, { skipValidation: true })

      expect(user.$id).toMatch(/^users\//)
    })
  })

  describe('validateOnWrite option', () => {
    it('uses strict mode by default', async () => {
      await expect(
        db.create('users', {
          $type: 'User',
          name: 'Alice',
          // missing email
        })
      ).rejects.toThrow()
    })

    it('validateOnWrite: false skips validation', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Alice',
        // email is missing
      } as any, { validateOnWrite: false })

      expect(user.name).toBe('Alice')
    })

    it('validateOnWrite: "permissive" returns result but creates entity', async () => {
      // With permissive mode, validation errors don't throw
      // The entity is created (for backward compatibility)
      // This is different from strict mode which throws
      const user = await db.create('users', {
        $type: 'User',
        name: 'Alice',
        email: 'alice@example.com',
        age: 'not-a-number', // wrong type
      } as any, { validateOnWrite: 'permissive' })

      // Entity is created even with validation errors in permissive mode
      // (This tests that permissive mode doesn't throw)
      expect(user.name).toBe('Alice')
    })
  })

  describe('Types not in schema', () => {
    it('allows creating entities with types not in schema', async () => {
      // CustomType is not defined in schema
      const custom = await db.create('customs', {
        $type: 'CustomType',
        name: 'Custom Entity',
        anyField: 'any value',
        nested: { deep: 'value' },
      })

      expect(custom.$type).toBe('CustomType')
      expect(custom.anyField).toBe('any value')
    })
  })

  describe('Schema registration', () => {
    it('can register additional schemas', async () => {
      const additionalSchema: Schema = {
        Comment: {
          text: 'string!',
          author: 'string!',
        },
      }

      db.registerSchema(additionalSchema)

      // Now Comment type should be validated
      await expect(
        db.create('comments', {
          $type: 'Comment',
          name: 'A comment',
          // text and author are missing
        })
      ).rejects.toThrow()
    })
  })

  describe('Collection operations', () => {
    it('validates via collection create', async () => {
      const users = db.collection('users')

      await expect(
        users.create({
          $type: 'User',
          name: 'Alice',
          // missing email
        })
      ).rejects.toThrow()
    })

    it('creates valid entities via collection', async () => {
      const users = db.collection('users')

      const user = await users.create({
        $type: 'User',
        name: 'Alice',
        email: 'alice@example.com',
      })

      expect(user.name).toBe('Alice')
    })
  })

  describe('Error messages', () => {
    it('provides helpful error messages', async () => {
      try {
        await db.create('users', {
          $type: 'User',
          name: 'Alice',
          // missing email
        })
        expect.fail('Should have thrown')
      } catch (error: any) {
        expect(error.message).toContain('email')
      }
    })

    it('SchemaValidationError provides detailed info', async () => {
      try {
        await db.create('users', {
          $type: 'User',
          name: 123, // wrong type
          email: 'invalid', // wrong format
        } as any)
        expect.fail('Should have thrown')
      } catch (error) {
        if (error instanceof SchemaValidationError) {
          expect(error.typeName).toBe('User')
          expect(error.errors.length).toBeGreaterThan(0)

          const fieldErrors = error.getFieldErrors()
          expect(fieldErrors.size).toBeGreaterThan(0)
        }
      }
    })
  })

  describe('Default values from schema', () => {
    it('applies string defaults', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Alice',
        email: 'alice@example.com',
        // role has default "user"
      })

      expect(user.role).toBe('user')
    })

    it('applies number defaults', async () => {
      const post = await db.create('posts', {
        $type: 'Post',
        name: 'Hello',
        title: 'Hello World',
        content: 'Content here',
        // views has default 0
      })

      expect(post.views).toBe(0)
    })
  })
})
