import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { ParqueEngine } from '@/engine/engine'

/**
 * ParqueEngine Error Path Tests (zou5.27)
 *
 * Tests edge cases and error conditions in the engine's write operations:
 * - update() on non-existent entity
 * - create() with duplicate $id (second create becomes version 1 again, overwrites in buffer)
 * - delete() on already-deleted entity
 * - delete() on non-existent entity
 *
 * These tests document the actual behavior of the engine under error conditions,
 * ensuring that it throws appropriate errors and maintains internal consistency.
 */

let engine: ParqueEngine
let dataDir: string

beforeEach(async () => {
  dataDir = await mkdtemp(join(tmpdir(), 'parquedb-error-test-'))
  engine = new ParqueEngine({ dataDir })
})

afterEach(async () => {
  await engine.close()
  await rm(dataDir, { recursive: true, force: true })
})

// =============================================================================
// update() on non-existent entity
// =============================================================================

describe('ParqueEngine error paths - update() on non-existent entity', () => {
  it('throws "Entity not found" when table has no entities', async () => {
    await expect(
      engine.update('users', 'ghost-id', { $set: { name: 'Ghost' } })
    ).rejects.toThrow('Entity not found: users/ghost-id')
  })

  it('throws "Entity not found" when table has entities but ID does not exist', async () => {
    await engine.create('users', { $id: 'real-user', name: 'Alice' })

    await expect(
      engine.update('users', 'nonexistent', { $set: { name: 'Nobody' } })
    ).rejects.toThrow('Entity not found: users/nonexistent')
  })

  it('throws "Entity not found" when table name does not exist at all', async () => {
    // No entities have ever been created in this table
    await expect(
      engine.update('unknown_table', 'some-id', { $set: { x: 1 } })
    ).rejects.toThrow('Entity not found: unknown_table/some-id')
  })

  it('does not modify the buffer when update throws', async () => {
    await engine.create('users', { $id: 'u1', name: 'Alice' })

    await expect(
      engine.update('users', 'missing', { $set: { name: 'Ghost' } })
    ).rejects.toThrow()

    // Buffer should still have only the original entity
    const buffer = engine.getBuffer('users')
    const existing = buffer.get('u1')
    expect(existing).toBeDefined()
    expect(existing!.name).toBe('Alice')
    expect(existing!.$v).toBe(1)

    // The missing entity should not be in the buffer
    expect(buffer.get('missing')).toBeUndefined()
  })
})

// =============================================================================
// create() with duplicate $id
// =============================================================================

describe('ParqueEngine error paths - create() with duplicate $id', () => {
  it('second create with same $id overwrites in buffer with $v=1', async () => {
    // The engine's create() does NOT check for existing entities.
    // It always writes $v=1. The second create will overwrite the first
    // in the in-memory buffer because TableBuffer.set() replaces by $id.
    const first = await engine.create('users', { $id: 'dup-1', name: 'Alice' })
    const second = await engine.create('users', { $id: 'dup-1', name: 'Bob' })

    expect(first.$id).toBe('dup-1')
    expect(first.$v).toBe(1)
    expect(first.name).toBe('Alice')

    expect(second.$id).toBe('dup-1')
    expect(second.$v).toBe(1)
    expect(second.name).toBe('Bob')

    // The buffer should have the second entity (last write wins)
    const buffered = engine.getBuffer('users').get('dup-1')
    expect(buffered).toBeDefined()
    expect(buffered!.name).toBe('Bob')
    expect(buffered!.$v).toBe(1)
  })

  it('get() returns the last created version after duplicate create', async () => {
    await engine.create('users', { $id: 'dup-2', name: 'First' })
    await engine.create('users', { $id: 'dup-2', name: 'Second' })

    const result = await engine.get('users', 'dup-2')
    expect(result).toBeDefined()
    expect(result!.name).toBe('Second')
    expect(result!.$v).toBe(1)
  })

  it('update after duplicate create operates on the last version', async () => {
    await engine.create('users', { $id: 'dup-3', name: 'First' })
    await engine.create('users', { $id: 'dup-3', name: 'Second' })

    const updated = await engine.update('users', 'dup-3', {
      $set: { name: 'Updated' },
    })

    // The update should bump the version from the buffer's current $v=1 to $v=2
    expect(updated.$id).toBe('dup-3')
    expect(updated.$v).toBe(2)
    expect(updated.name).toBe('Updated')
  })
})

// =============================================================================
// delete() on already-deleted entity
// =============================================================================

describe('ParqueEngine error paths - delete() on already-deleted entity', () => {
  it('throws "Entity not found" when deleting an already-deleted entity', async () => {
    const created = await engine.create('users', { $id: 'del-1', name: 'Alice' })
    await engine.delete('users', created.$id)

    // Second delete should throw because the entity is now a tombstone
    await expect(
      engine.delete('users', 'del-1')
    ).rejects.toThrow('Entity not found: users/del-1')
  })

  it('buffer still shows tombstone after failed second delete attempt', async () => {
    await engine.create('users', { $id: 'del-2', name: 'Bob' })
    await engine.delete('users', 'del-2')

    // Attempt second delete (should throw)
    await expect(engine.delete('users', 'del-2')).rejects.toThrow()

    // Buffer should still have the tombstone from the first delete
    const buffer = engine.getBuffer('users')
    expect(buffer.isTombstone('del-2')).toBe(true)
  })

  it('get() returns null for a deleted entity', async () => {
    await engine.create('users', { $id: 'del-3', name: 'Charlie' })
    await engine.delete('users', 'del-3')

    const result = await engine.get('users', 'del-3')
    expect(result).toBeNull()
  })

  it('update() on a deleted entity throws "Entity not found"', async () => {
    await engine.create('users', { $id: 'del-4', name: 'Diana' })
    await engine.delete('users', 'del-4')

    await expect(
      engine.update('users', 'del-4', { $set: { name: 'Revived' } })
    ).rejects.toThrow('Entity not found: users/del-4')
  })
})

// =============================================================================
// delete() on non-existent entity
// =============================================================================

describe('ParqueEngine error paths - delete() on non-existent entity', () => {
  it('throws "Entity not found" when entity never existed', async () => {
    await expect(
      engine.delete('users', 'never-created')
    ).rejects.toThrow('Entity not found: users/never-created')
  })

  it('throws "Entity not found" when table has no entities', async () => {
    // The table 'empty_table' has never had any data
    await expect(
      engine.delete('empty_table', 'some-id')
    ).rejects.toThrow('Entity not found: empty_table/some-id')
  })

  it('throws "Entity not found" when table exists but ID is wrong', async () => {
    await engine.create('users', { $id: 'real-user', name: 'Alice' })

    await expect(
      engine.delete('users', 'wrong-id')
    ).rejects.toThrow('Entity not found: users/wrong-id')
  })

  it('does not create a tombstone when delete throws on non-existent', async () => {
    await engine.create('users', { $id: 'real-user', name: 'Alice' })

    await expect(engine.delete('users', 'ghost')).rejects.toThrow()

    // The ghost entity should not exist in the buffer at all
    const buffer = engine.getBuffer('users')
    expect(buffer.get('ghost')).toBeUndefined()

    // The real user should be unaffected
    const real = buffer.get('real-user')
    expect(real).toBeDefined()
    expect(real!.name).toBe('Alice')
    expect(real!.$v).toBe(1)
  })
})
