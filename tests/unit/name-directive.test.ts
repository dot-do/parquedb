import { describe, it, expect } from 'vitest'
import { DB, MemoryBackend } from '../../src'

describe('$name directive', () => {
  it('uses field value as entity name when $name directive is set', async () => {
    const db = DB({
      User: {
        $id: 'email',
        $name: 'fullName',
        email: 'string!#',
        fullName: 'string!',
      }
    }, { storage: new MemoryBackend() })

    const user = await db.User.create({
      email: 'alice@example.com',
      fullName: 'Alice Smith'
    })

    expect(user.name).toBe('Alice Smith')

    db.dispose()
  })

  it('can use same field for both $id and $name', async () => {
    const db = DB({
      User: {
        $id: 'email',
        $name: 'email',
        email: 'string!#',
      }
    }, { storage: new MemoryBackend() })

    const user = await db.User.create({
      email: 'alice@example.com'
    })

    expect(user.$id).toBe('user/alice@example.com')
    expect(user.name).toBe('alice@example.com')

    db.dispose()
  })

  it('allows explicit name to override $name directive', async () => {
    const db = DB({
      User: {
        $id: 'email',
        $name: 'email',
        email: 'string!#',
      }
    }, { storage: new MemoryBackend() })

    const user = await db.User.create({
      email: 'alice@example.com',
      name: 'Alice'  // Explicit override
    })

    expect(user.name).toBe('Alice')

    db.dispose()
  })

  it('works with $name but without $id directive', async () => {
    const db = DB({
      Post: {
        $name: 'title',
        title: 'string!',
        content: 'text',
      }
    }, { storage: new MemoryBackend() })

    const post = await db.Post.create({
      title: 'Hello World',
      content: 'My first post'
    })

    expect(post.name).toBe('Hello World')
    // $id should still be a ULID
    expect(post.$id).toMatch(/^post\/[0-9a-z]+$/)

    db.dispose()
  })

  it('auto-generates name when no $name directive and no name provided', async () => {
    const db = DB({
      Event: {
        type: 'string!'
      }
    }, { storage: new MemoryBackend() })

    const event = await db.Event.create({ type: 'click' })

    // Should have some name (could be empty or auto-generated)
    expect(event.name).toBeDefined()

    db.dispose()
  })

  it('throws error if $name field does not exist in schema', async () => {
    expect(() => {
      DB({
        User: {
          $name: 'nonexistent',
          email: 'string!#',
        }
      }, { storage: new MemoryBackend() })
    }).toThrow(/nonexistent field/)
  })

  describe('edge cases', () => {
    it('falls back to ID when $name field value is empty string', async () => {
      const db = DB({
        User: {
          $id: 'email',
          $name: 'displayName',
          email: 'string!#',
          displayName: 'string',
        }
      }, { storage: new MemoryBackend() })

      const user = await db.User.create({
        email: 'alice@example.com',
        displayName: ''  // Empty string
      })

      // Empty string should fall back to the entity ID part
      expect(user.name).toBe('alice@example.com')

      db.dispose()
    })

    it('falls back to ID when $name field value is null', async () => {
      const db = DB({
        User: {
          $id: 'email',
          $name: 'displayName',
          email: 'string!#',
          displayName: 'string',
        }
      }, { storage: new MemoryBackend() })

      const user = await db.User.create({
        email: 'bob@example.com',
        displayName: null  // Null value
      } as any)  // Cast to any to allow null

      // Null should fall back to the entity ID part
      expect(user.name).toBe('bob@example.com')

      db.dispose()
    })

    it('falls back to ID when $name field value is undefined', async () => {
      const db = DB({
        User: {
          $id: 'email',
          $name: 'displayName',
          email: 'string!#',
          displayName: 'string',
        }
      }, { storage: new MemoryBackend() })

      const user = await db.User.create({
        email: 'carol@example.com',
        // displayName not provided (undefined)
      })

      // Undefined should fall back to the entity ID part
      expect(user.name).toBe('carol@example.com')

      db.dispose()
    })

    it('throws error when $name directive references another directive', async () => {
      expect(() => {
        DB({
          User: {
            $name: '$id',
            $id: 'email',
            email: 'string!#',
          }
        }, { storage: new MemoryBackend() })
      }).toThrow(/cannot reference another directive/)
    })
  })
})
