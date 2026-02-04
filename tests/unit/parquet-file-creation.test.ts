/**
 * Parquet File Creation Test
 *
 * Verifies that ParqueDB creates Parquet files (not JSON) for data persistence.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { ParqueDB } from '../../src/ParqueDB'
import { FsBackend } from '../../src/storage/FsBackend'
import { mkdtemp, rm, readdir } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

describe('Parquet File Creation', () => {
  let tempDir: string
  let storage: FsBackend
  let db: ParqueDB

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), 'parquedb-parquet-test-'))
    storage = new FsBackend(tempDir)
    db = new ParqueDB({ storage })
  })

  afterEach(async () => {
    await db.disposeAsync()
    try {
      await rm(tempDir, { recursive: true, force: true })
    } catch {
      // Ignore cleanup errors
    }
  })

  it('should create events.parquet file (not JSON)', async () => {
    // Create an entity
    const entity = await db.create('posts', {
      $type: 'Post',
      name: 'Test Post',
      title: 'Hello World',
      content: 'This is a test',
    })

    expect(entity).toBeDefined()
    expect(entity.$id).toBeDefined()

    // Flush and dispose to ensure data is written
    await db.disposeAsync()

    // List files in the temp directory
    const files = await readdir(tempDir, { recursive: true })

    // Should have events.parquet file
    const hasEventsParquet = files.some((f) => f === 'events.parquet')
    expect(hasEventsParquet).toBe(true)

    // Should NOT have JSON event files (the old format)
    const hasJsonEvents = files.some(
      (f) => f.endsWith('.json') && f.includes('events/')
    )
    expect(hasJsonEvents).toBe(false)
  })

  it('should persist entities that can be recovered', async () => {
    // Create an entity
    const entity = await db.create('posts', {
      $type: 'Post',
      name: 'Persistent Post',
      title: 'Will persist',
      content: 'Content here',
    })

    const entityId = entity.$id

    // Flush and dispose
    await db.disposeAsync()

    // Create new DB instance with same storage
    const db2 = new ParqueDB({ storage })

    // Should be able to retrieve the entity
    const retrieved = await db2.get('posts', entityId)
    expect(retrieved).not.toBeNull()
    expect(retrieved?.name).toBe('Persistent Post')
    expect(retrieved?.title).toBe('Will persist')

    await db2.disposeAsync()
  })
})
