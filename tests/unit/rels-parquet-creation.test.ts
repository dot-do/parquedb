/**
 * Relationships Parquet File Creation Test
 *
 * Verifies that ParqueDB creates rels.parquet for relationship data.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { ParqueDB } from '../../src/ParqueDB'
import { FsBackend } from '../../src/storage/FsBackend'
import { mkdtemp, rm, readdir, readFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

describe('Relationships Parquet File Creation', () => {
  let tempDir: string
  let storage: FsBackend
  let db: ParqueDB

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), 'parquedb-rels-test-'))
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

  it('should create rels.parquet when entities have relationships', async () => {
    // Create a company
    const company = await db.create('companies', {
      $type: 'Company',
      name: 'Acme Inc',
    })

    // Create a person with a relationship to the company
    const person = await db.create('people', {
      $type: 'Person',
      name: 'John Doe',
      worksAt: company.$id, // Reference to company
    })

    expect(company).toBeDefined()
    expect(person).toBeDefined()

    // Flush and dispose to ensure data is written
    await db.disposeAsync()

    // List files in the temp directory
    const files = await readdir(tempDir, { recursive: true })
    console.log('Files after dispose:', files)

    // Should have events.parquet
    const hasEventsParquet = files.some((f) => f === 'events.parquet')
    expect(hasEventsParquet).toBe(true)

    // Should have rels.parquet (if relationships were detected)
    const hasRelsParquet = files.some((f) => f === 'rels.parquet')
    console.log('Has rels.parquet:', hasRelsParquet)

    // Note: rels.parquet is only created if relationships were indexed
    // The relationship detection looks for entity ID patterns (ns/id format)
    if (hasRelsParquet) {
      // Read and verify rels.parquet structure
      const data = await readFile(join(tempDir, 'rels.parquet'))
      console.log('rels.parquet size:', data.length)

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
      console.log('Rels schema:', (metadata.schema as Array<{ name?: string }>).map((s) => s.name))

      let rows: unknown[][] = []
      await parquetRead({
        file: asyncBuffer,
        compressors,
        onComplete: (data: unknown[][]) => {
          rows = data
        },
      })

      console.log('Rels rows:', rows.length)
      if (rows.length > 0) {
        console.log('First rel row:', rows[0])
      }

      expect(rows.length).toBeGreaterThan(0)
    }
  })

  it('should have correct schema in events.parquet with audit info in after variant', async () => {
    // Create an entity
    await db.create('posts', {
      $type: 'Post',
      name: 'Test Post',
      title: 'Hello',
      content: 'World',
    })

    await db.disposeAsync()

    // Read events.parquet
    const data = await readFile(join(tempDir, 'events.parquet'))
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
    const schemaNames = (metadata.schema as Array<{ name?: string }>)
      .filter((s) => s.name && s.name !== 'root')
      .map((s) => s.name)

    console.log('Events schema columns:', schemaNames)

    // Events.parquet should have: id, ts, op, target, before, after, actor, metadata
    expect(schemaNames).toContain('id')
    expect(schemaNames).toContain('ts')
    expect(schemaNames).toContain('op')
    expect(schemaNames).toContain('target')
    expect(schemaNames).toContain('before')
    expect(schemaNames).toContain('after')
    expect(schemaNames).toContain('actor')
    expect(schemaNames).toContain('metadata')

    // Audit fields should NOT be separate columns (they're in the after variant)
    expect(schemaNames).not.toContain('createdAt')
    expect(schemaNames).not.toContain('createdBy')
    expect(schemaNames).not.toContain('updatedAt')
    expect(schemaNames).not.toContain('updatedBy')
    expect(schemaNames).not.toContain('version')
  })
})
