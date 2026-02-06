/**
 * BunParquetReader Test Suite
 *
 * Tests the Bun-optimized Parquet file reader. Since vitest runs on Node.js,
 * these tests exercise the Node.js fallback path. The tests verify that
 * readParquetFile returns a correct AsyncBuffer, handles missing files
 * gracefully, and integrates with hyparquet for full Parquet roundtrips.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtemp, rm, writeFile, stat } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { readParquetFile, isBunRuntime } from '@/engine/bun-reader'
import type { AsyncBuffer } from '@/engine/bun-reader'

// =============================================================================
// Test Setup
// =============================================================================

let tempDir: string

beforeEach(async () => {
  tempDir = await mkdtemp(join(tmpdir(), 'bun-reader-'))
})

afterEach(async () => {
  await rm(tempDir, { recursive: true, force: true })
})

/**
 * Helper: create a Parquet file with the given column data and return its path.
 */
async function createParquetFile(
  filename: string,
  columnData: Array<{ name: string; data: unknown[]; type?: string }>,
): Promise<string> {
  const { parquetWriteBuffer } = await import('hyparquet-writer')
  const buffer = parquetWriteBuffer({ columnData })
  const path = join(tempDir, filename)
  await writeFile(path, new Uint8Array(buffer))
  return path
}

// =============================================================================
// readParquetFile - Basic Behavior
// =============================================================================

describe('readParquetFile', () => {
  it('returns an AsyncBuffer for an existing file', async () => {
    const path = await createParquetFile('test.parquet', [
      { name: 'id', data: ['a', 'b', 'c'] },
      { name: 'value', data: [1, 2, 3] },
    ])

    const asyncBuffer = await readParquetFile(path)

    expect(asyncBuffer).not.toBeNull()
    expect(asyncBuffer!.byteLength).toBeGreaterThan(0)
    expect(typeof asyncBuffer!.slice).toBe('function')
  })

  it('returns null for a missing file', async () => {
    const result = await readParquetFile(join(tempDir, 'nonexistent.parquet'))
    expect(result).toBeNull()
  })

  it('returns null for a path that is a directory', async () => {
    const result = await readParquetFile(tempDir)
    // Reading a directory with fs.readFile throws, so we expect null
    expect(result).toBeNull()
  })
})

// =============================================================================
// AsyncBuffer Properties
// =============================================================================

describe('AsyncBuffer', () => {
  it('byteLength matches the actual file size', async () => {
    const path = await createParquetFile('size-check.parquet', [
      { name: 'col', data: ['hello', 'world'] },
    ])

    const asyncBuffer = await readParquetFile(path)
    const fileStats = await stat(path)

    expect(asyncBuffer).not.toBeNull()
    expect(asyncBuffer!.byteLength).toBe(fileStats.size)
  })

  it('slice() returns the correct byte range', async () => {
    const path = await createParquetFile('slice-range.parquet', [
      { name: 'x', data: [10, 20, 30, 40, 50] },
    ])

    const asyncBuffer = await readParquetFile(path)
    expect(asyncBuffer).not.toBeNull()

    // Read the first 4 bytes (should be PAR1 magic for Parquet)
    const header = await asyncBuffer!.slice(0, 4)
    expect(header.byteLength).toBe(4)

    const magic = new Uint8Array(header)
    // PAR1 magic bytes: 0x50, 0x41, 0x52, 0x31
    expect(magic[0]).toBe(0x50) // P
    expect(magic[1]).toBe(0x41) // A
    expect(magic[2]).toBe(0x52) // R
    expect(magic[3]).toBe(0x31) // 1
  })

  it('slice() with only start returns bytes to end of file', async () => {
    const path = await createParquetFile('slice-to-end.parquet', [
      { name: 'a', data: ['one', 'two'] },
    ])

    const asyncBuffer = await readParquetFile(path)
    expect(asyncBuffer).not.toBeNull()

    const totalLength = asyncBuffer!.byteLength
    const halfwayPoint = Math.floor(totalLength / 2)

    const sliceFromMiddle = await asyncBuffer!.slice(halfwayPoint)
    expect(sliceFromMiddle.byteLength).toBe(totalLength - halfwayPoint)
  })

  it('zero-length slice returns empty ArrayBuffer', async () => {
    const path = await createParquetFile('zero-slice.parquet', [
      { name: 'col', data: ['data'] },
    ])

    const asyncBuffer = await readParquetFile(path)
    expect(asyncBuffer).not.toBeNull()

    const emptySlice = await asyncBuffer!.slice(5, 5)
    expect(emptySlice.byteLength).toBe(0)
  })
})

// =============================================================================
// Parquet Roundtrip Integration
// =============================================================================

describe('Parquet roundtrip', () => {
  it('reads back simple string and number columns', async () => {
    const path = await createParquetFile('roundtrip-simple.parquet', [
      { name: 'id', data: ['a', 'b', 'c'] },
      { name: 'value', data: [1, 2, 3] },
    ])

    const asyncBuffer = await readParquetFile(path)
    expect(asyncBuffer).not.toBeNull()

    const { parquetReadObjects } = await import('hyparquet')
    const rows = await parquetReadObjects({ file: asyncBuffer! }) as Array<{ id: string; value: number }>

    expect(rows).toHaveLength(3)
    expect(rows[0]).toEqual({ id: 'a', value: 1 })
    expect(rows[1]).toEqual({ id: 'b', value: 2 })
    expect(rows[2]).toEqual({ id: 'c', value: 3 })
  })

  it('handles large files with 10K+ rows', async () => {
    const rowCount = 10_000
    const ids: string[] = []
    const values: number[] = []
    for (let i = 0; i < rowCount; i++) {
      ids.push(`row-${String(i).padStart(5, '0')}`)
      values.push(i * 10)
    }

    const path = await createParquetFile('large-file.parquet', [
      { name: 'id', data: ids },
      { name: 'value', data: values },
    ])

    const asyncBuffer = await readParquetFile(path)
    expect(asyncBuffer).not.toBeNull()
    // File should be non-trivial in size
    expect(asyncBuffer!.byteLength).toBeGreaterThan(1000)

    const { parquetReadObjects } = await import('hyparquet')
    const rows = await parquetReadObjects({ file: asyncBuffer! }) as Array<{ id: string; value: number }>

    expect(rows).toHaveLength(rowCount)
    expect(rows[0]).toEqual({ id: 'row-00000', value: 0 })
    expect(rows[rowCount - 1]).toEqual({ id: `row-${String(rowCount - 1).padStart(5, '0')}`, value: (rowCount - 1) * 10 })
  })

  it('preserves data through full write-read-decode cycle with multiple types', async () => {
    const path = await createParquetFile('roundtrip-types.parquet', [
      { name: 'name', data: ['Alice', 'Bob', 'Charlie'] },
      { name: 'age', data: [30, 25, 35] },
      { name: 'active', data: [true, false, true] },
    ])

    const asyncBuffer = await readParquetFile(path)
    expect(asyncBuffer).not.toBeNull()

    const { parquetReadObjects } = await import('hyparquet')
    const rows = await parquetReadObjects({ file: asyncBuffer! }) as Array<{
      name: string
      age: number
      active: boolean
    }>

    expect(rows).toHaveLength(3)
    expect(rows[0]).toEqual({ name: 'Alice', age: 30, active: true })
    expect(rows[1]).toEqual({ name: 'Bob', age: 25, active: false })
    expect(rows[2]).toEqual({ name: 'Charlie', age: 35, active: true })
  })
})

// =============================================================================
// Runtime Detection
// =============================================================================

describe('isBunRuntime', () => {
  it('returns false when running on Node.js (vitest)', () => {
    expect(isBunRuntime()).toBe(false)
  })
})
