import { describe, it, expect, beforeEach } from 'vitest'
import { DB, MemoryBackend } from '../../src'
import { ValidationError } from '../../src/ParqueDB/types'

describe('$id directive', () => {
  it('uses field value as entity ID when $id directive is set', async () => {
    const db = DB({
      User: {
        $id: 'email',
        email: 'string!#',
        name: 'string',
      }
    }, { storage: new MemoryBackend() })

    const user = await db.User.create({
      email: 'alice@example.com',
      name: 'Alice'
    })

    expect(user.$id).toBe('user/alice@example.com')

    db.dispose()
  })

  it('allows get() with short ID when $id directive is set', async () => {
    const db = DB({
      User: {
        $id: 'email',
        email: 'string!#',
        name: 'string',
      }
    }, { storage: new MemoryBackend() })

    await db.User.create({
      email: 'alice@example.com',
      name: 'Alice'
    })

    const found = await db.User.get('alice@example.com')
    expect(found?.name).toBe('Alice')

    db.dispose()
  })

  it('generates ULID when no $id directive is specified', async () => {
    const db = DB({
      Event: {
        type: 'string!'
      }
    }, { storage: new MemoryBackend() })

    const event = await db.Event.create({ type: 'click' })

    // Should be a ULID-based ID, not the type value
    expect(event.$id).toMatch(/^event\/[0-9a-z]+$/)
    expect(event.$id).not.toBe('event/click')

    db.dispose()
  })

  it('works with different field types as $id', async () => {
    const db = DB({
      Post: {
        $id: 'slug',
        slug: 'string!#',
        title: 'string!',
      }
    }, { storage: new MemoryBackend() })

    const post = await db.Post.create({
      slug: 'hello-world',
      title: 'Hello World'
    })

    expect(post.$id).toBe('post/hello-world')

    db.dispose()
  })

  describe('edge cases', () => {
    it('throws error when $id field value contains slash', async () => {
      const db = DB({
        Document: {
          $id: 'path',
          path: 'string!#',
          content: 'string',
        }
      }, { storage: new MemoryBackend() })

      // Slashes are not allowed in local IDs (they separate namespace from local ID)
      await expect(db.Document.create({
        path: 'folder/subfolder/file.txt',
        content: 'Hello'
      })).rejects.toThrow(/cannot contain '\/'/)

      db.dispose()
    })

    it('throws error when $id field value is empty string', async () => {
      const db = DB({
        User: {
          $id: 'email',
          email: 'string!#',
          name: 'string',
        }
      }, { storage: new MemoryBackend() })

      await expect(db.User.create({
        email: '',
        name: 'Anonymous'
      })).rejects.toThrow(/empty string/)

      db.dispose()
    })

    it('throws error when creating duplicate entity with same $id', async () => {
      const db = DB({
        User: {
          $id: 'email',
          email: 'string!#',
          name: 'string',
        }
      }, { storage: new MemoryBackend() })

      await db.User.create({
        email: 'alice@example.com',
        name: 'Alice'
      })

      // Try to create another user with the same email
      await expect(db.User.create({
        email: 'alice@example.com',
        name: 'Alice 2'
      })).rejects.toThrow(/already exists/)

      db.dispose()
    })

    it('throws error when $id directive references nonexistent field', () => {
      expect(() => {
        DB({
          User: {
            $id: 'nonexistent',
            email: 'string!#',
            name: 'string',
          }
        }, { storage: new MemoryBackend() })
      }).toThrow(/nonexistent field/)
    })

    it('allows creating entity after deleting one with same $id', async () => {
      const db = DB({
        User: {
          $id: 'email',
          email: 'string!#',
          name: 'string',
        }
      }, { storage: new MemoryBackend() })

      const user1 = await db.User.create({
        email: 'alice@example.com',
        name: 'Alice'
      })

      // Soft delete the user
      await db.User.delete(user1.$id)

      // Should be able to create a new user with same email
      const user2 = await db.User.create({
        email: 'alice@example.com',
        name: 'Alice Reborn'
      })

      expect(user2.$id).toBe('user/alice@example.com')
      expect(user2.name).toBe('Alice Reborn')

      db.dispose()
    })
  })
})
