/**
 * Cross-Process Persistence Test
 *
 * Tests that data persisted by one ParqueDB instance can be read by another.
 * This simulates what happens when a user creates data, then later reads it
 * in a different process.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { ParqueDB } from '../../src/ParqueDB'
import { FsBackend } from '../../src/storage/FsBackend'
import { mkdtemp, rm, readdir, readFile, stat } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

describe('Cross-Process Persistence', () => {
  let tempDir: string
  let storage: FsBackend

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), 'parquedb-cross-process-'))
    storage = new FsBackend(tempDir)
  })

  afterEach(async () => {
    try {
      await rm(tempDir, { recursive: true, force: true })
    } catch {
      // Ignore cleanup errors
    }
  })

  it('should persist and retrieve entities via get() across instances', async () => {
    // Instance 1: Create entity
    const db1 = new ParqueDB({ storage })
    const entity = await db1.create('posts', {
      $type: 'Post',
      name: 'Test Post',
      title: 'Hello World',
      content: 'This is test content',
    })

    const entityId = entity.$id
    console.log('Created entity:', entityId)

    // Verify entity exists in instance 1
    const found1 = await db1.get('posts', entityId)
    expect(found1).not.toBeNull()
    expect(found1?.name).toBe('Test Post')

    // Flush and dispose
    await db1.disposeAsync()

    // List files to verify persistence
    const files = await readdir(tempDir, { recursive: true })
    console.log('Files after dispose:', files)

    // Verify events.parquet exists
    const hasEventsParquet = files.includes('events.parquet')
    expect(hasEventsParquet).toBe(true)

    // Instance 2: Read entity (simulates new process)
    const storage2 = new FsBackend(tempDir)
    const db2 = new ParqueDB({ storage: storage2 })

    // Try to get the entity
    const found2 = await db2.get('posts', entityId)
    console.log('Found via get() in new instance:', found2)

    expect(found2).not.toBeNull()
    expect(found2?.name).toBe('Test Post')
    expect(found2?.title).toBe('Hello World')

    await db2.disposeAsync()
  })

  it('should persist and retrieve entities via find() across instances', async () => {
    // Instance 1: Create entity
    const db1 = new ParqueDB({ storage })
    const entity = await db1.create('posts', {
      $type: 'Post',
      name: 'Findable Post',
      title: 'Can You Find Me',
      content: 'I hope so',
    })

    console.log('Created entity:', entity.$id)

    // Verify find works in instance 1
    const result1 = await db1.find('posts')
    expect(result1.items.length).toBe(1)
    expect(result1.items[0]?.name).toBe('Findable Post')

    // Flush and dispose
    await db1.disposeAsync()

    // Instance 2: Try to find the entity
    const storage2 = new FsBackend(tempDir)
    const db2 = new ParqueDB({ storage: storage2 })

    const result2 = await db2.find('posts')
    console.log('Found via find() in new instance:', result2)

    expect(result2.items.length).toBe(1)
    expect(result2.items[0]?.name).toBe('Findable Post')

    await db2.disposeAsync()
  })

  it('should persist and retrieve entities from MULTIPLE namespaces', async () => {
    // Verify clean state - no events.parquet should exist
    const eventsPathBefore = join(tempDir, 'events.parquet')
    let existsBefore = false
    try {
      await stat(eventsPathBefore)
      existsBefore = true
    } catch { existsBefore = false }
    console.log('events.parquet exists BEFORE test:', existsBefore, 'in', tempDir)
    expect(existsBefore).toBe(false)

    // Instance 1: Create entities in different namespaces
    const db1 = new ParqueDB({ storage })

    const industry1 = await db1.create('industry', {
      $type: 'Industry',
      name: 'Technology',
    })
    const industry2 = await db1.create('industry', {
      $type: 'Industry',
      name: 'Healthcare',
    })
    const icp1 = await db1.create('icp', {
      $type: 'ICP',
      name: 'SMB Tech',
    })
    const icp2 = await db1.create('icp', {
      $type: 'ICP',
      name: 'Enterprise',
    })

    console.log('Created:', {
      industries: [industry1.$id, industry2.$id],
      icps: [icp1.$id, icp2.$id],
    })

    // Verify all exist in instance 1
    const result1 = await db1.find('industry')
    expect(result1.items.length).toBe(2)
    const result2 = await db1.find('icp')
    expect(result2.items.length).toBe(2)

    // Flush and dispose
    await db1.disposeAsync()

    // Debug: Check what's in the events.parquet file
    const eventsPath = join(tempDir, 'events.parquet')
    const data = await readFile(eventsPath)
    console.log('events.parquet size after dispose:', data.length)

    const { parquetRead, parquetMetadataAsync } = await import('hyparquet')
    const { compressors } = await import('../../src/parquet/compression')

    const asyncBuffer = {
      byteLength: data.length,
      slice: async (start: number, end?: number): Promise<ArrayBuffer> => {
        const sliced = data.slice(start, end ?? data.length)
        const buffer = new ArrayBuffer(sliced.byteLength)
        new Uint8Array(buffer).set(sliced)
        return buffer
      },
    }

    let rows: unknown[][] = []
    await parquetRead({
      file: asyncBuffer,
      compressors,
      onComplete: (data: unknown[][]) => {
        rows = data
      },
    })
    console.log('Events in parquet:', rows.length)
    rows.forEach((row, i) => {
      console.log(`  Event ${i}: op=${row[2]}, target=${row[3]}`)
    })

    // Instance 2: Read all entities
    const storage2 = new FsBackend(tempDir)
    const db2 = new ParqueDB({ storage: storage2 })

    // Verify ALL entities from ALL namespaces are found
    const industries = await db2.find('industry')
    console.log('Industries found:', industries.items.length, industries.items.map((i) => i.name))
    expect(industries.items.length).toBe(2)
    expect(industries.items.map((i) => i.name).sort()).toEqual(['Healthcare', 'Technology'])

    const icps = await db2.find('icp')
    console.log('ICPs found:', icps.items.length, icps.items.map((i) => i.name))
    expect(icps.items.length).toBe(2)
    expect(icps.items.map((i) => i.name).sort()).toEqual(['Enterprise', 'SMB Tech'])

    await db2.disposeAsync()
  })

  it('should debug: read events.parquet directly', async () => {
    // Instance 1: Create entity
    const db1 = new ParqueDB({ storage })
    await db1.create('posts', {
      $type: 'Post',
      name: 'Debug Post',
      title: 'Debug Title',
      content: 'Debug Content',
    })

    await db1.disposeAsync()

    // Read events.parquet directly
    const eventsPath = join(tempDir, 'events.parquet')
    const data = await readFile(eventsPath)
    console.log('events.parquet size:', data.length)

    // Try to parse it with hyparquet
    const { parquetRead, parquetMetadataAsync } = await import('hyparquet')
    const { compressors } = await import('../../src/parquet/compression')

    const asyncBuffer = {
      byteLength: data.length,
      slice: async (start: number, end?: number): Promise<ArrayBuffer> => {
        const sliced = data.slice(start, end ?? data.length)
        const buffer = new ArrayBuffer(sliced.byteLength)
        new Uint8Array(buffer).set(sliced)
        return buffer
      },
    }

    const metadata = await parquetMetadataAsync(asyncBuffer)
    console.log('Parquet schema:', (metadata.schema as Array<{ name?: string }>).map(s => s.name))

    let rows: unknown[][] = []
    await parquetRead({
      file: asyncBuffer,
      compressors,
      onComplete: (data: unknown[][]) => {
        rows = data
      },
    })

    console.log('Rows count:', rows.length)
    if (rows.length > 0) {
      console.log('First row:', rows[0])
    }

    expect(rows.length).toBeGreaterThan(0)
  })
})
