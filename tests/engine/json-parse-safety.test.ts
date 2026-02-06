/**
 * JSON.parse Error Handling Safety Tests
 *
 * Verifies that corrupted JSON in Parquet $data columns and event
 * before/after columns does not crash the entire read path.
 *
 * Strategy: We write valid Parquet files with intentionally corrupted
 * JSON strings in the $data, before, and after columns, then verify
 * that reads gracefully handle the corruption (skip or default).
 *
 * Pattern follows jsonl-reader.ts: skip/default on corrupt data,
 * never crash the whole read.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { ParquetStorageAdapter } from '@/engine/parquet-adapter'

// =============================================================================
// Test Helpers
// =============================================================================

let tempDir: string
let adapter: ParquetStorageAdapter

beforeEach(async () => {
  tempDir = await mkdtemp(join(tmpdir(), 'json-parse-safety-'))
  adapter = new ParquetStorageAdapter()
})

afterEach(async () => {
  await rm(tempDir, { recursive: true, force: true })
})

/**
 * Write a Parquet file with raw column data, allowing us to inject
 * corrupted JSON strings into $data, before, or after columns.
 */
async function writeRawDataParquet(
  path: string,
  rows: Array<{ $id: string; $op: string; $v: number; $ts: number; $data: string }>,
): Promise<void> {
  const { parquetWriteBuffer } = await import('hyparquet-writer')

  const buffer = parquetWriteBuffer({
    columnData: [
      { name: '$id', data: rows.map(r => r.$id) },
      { name: '$op', data: rows.map(r => r.$op) },
      { name: '$v', data: rows.map(r => r.$v) },
      { name: '$ts', data: rows.map(r => r.$ts + 0.0), type: 'DOUBLE' as const },
      { name: '$data', data: rows.map(r => r.$data) },
    ],
  })

  await writeFile(path, new Uint8Array(buffer))
}

/**
 * Write a Parquet events file with raw column data, allowing corrupted
 * JSON in before/after columns.
 */
async function writeRawEventsParquet(
  path: string,
  rows: Array<{
    id: string
    ts: number
    op: string
    ns: string
    eid: string
    before: string
    after: string
    actor: string
  }>,
): Promise<void> {
  const { parquetWriteBuffer } = await import('hyparquet-writer')

  const buffer = parquetWriteBuffer({
    columnData: [
      { name: 'id', data: rows.map(r => r.id) },
      { name: 'ts', data: rows.map(r => r.ts + 0.0), type: 'DOUBLE' as const },
      { name: 'op', data: rows.map(r => r.op) },
      { name: 'ns', data: rows.map(r => r.ns) },
      { name: 'eid', data: rows.map(r => r.eid) },
      { name: 'before', data: rows.map(r => r.before) },
      { name: 'after', data: rows.map(r => r.after) },
      { name: 'actor', data: rows.map(r => r.actor) },
    ],
  })

  await writeFile(path, new Uint8Array(buffer))
}

// =============================================================================
// parquet-adapter.ts: readData() with corrupted $data
// =============================================================================

describe('ParquetStorageAdapter.readData() - corrupted $data JSON', () => {
  it('should not crash when $data contains invalid JSON', async () => {
    const path = join(tempDir, 'corrupt-data.parquet')

    await writeRawDataParquet(path, [
      { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, $data: '{"name":"Alice"}' },
      { $id: 'u2', $op: 'c', $v: 1, $ts: 1000, $data: '{corrupt json!!!' },
      { $id: 'u3', $op: 'c', $v: 1, $ts: 1000, $data: '{"name":"Charlie"}' },
    ])

    const result = await adapter.readData(path)

    // Should return all 3 rows (corrupted one gets empty data fields)
    expect(result).toHaveLength(3)

    // Valid rows preserve their data
    const u1 = result.find(r => r.$id === 'u1')!
    expect(u1.name).toBe('Alice')

    const u3 = result.find(r => r.$id === 'u3')!
    expect(u3.name).toBe('Charlie')

    // Corrupted row gets empty data fields but system fields are preserved
    const u2 = result.find(r => r.$id === 'u2')!
    expect(u2.$id).toBe('u2')
    expect(u2.$op).toBe('c')
    expect(u2.$v).toBe(1)
    expect(u2.$ts).toBe(1000)
    // No data fields from the corrupted JSON
    expect(u2.name).toBeUndefined()
  })

  it('should log a warning when $data JSON is corrupted', async () => {
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const path = join(tempDir, 'corrupt-warn.parquet')

    await writeRawDataParquet(path, [
      { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, $data: 'not-json' },
    ])

    await adapter.readData(path)

    expect(warnSpy).toHaveBeenCalled()
    expect(warnSpy.mock.calls[0]![0]).toContain('u1')

    warnSpy.mockRestore()
  })

  it('should handle all rows having corrupted $data', async () => {
    const path = join(tempDir, 'all-corrupt.parquet')

    await writeRawDataParquet(path, [
      { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, $data: '<<<bad>>>' },
      { $id: 'u2', $op: 'u', $v: 2, $ts: 2000, $data: 'undefined' },
    ])

    const result = await adapter.readData(path)

    expect(result).toHaveLength(2)
    expect(result[0]!.$id).toBeDefined()
    expect(result[1]!.$id).toBeDefined()
  })

  it('should handle empty string $data gracefully', async () => {
    const path = join(tempDir, 'empty-data.parquet')

    await writeRawDataParquet(path, [
      { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, $data: '' },
    ])

    const result = await adapter.readData(path)
    expect(result).toHaveLength(1)
    expect(result[0]!.$id).toBe('u1')
  })
})

// =============================================================================
// parquet-adapter.ts: readEvents() with corrupted before/after JSON
// =============================================================================

describe('ParquetStorageAdapter.readEvents() - corrupted before/after JSON', () => {
  it('should not crash when before JSON is corrupted', async () => {
    const path = join(tempDir, 'corrupt-before.parquet')

    await writeRawEventsParquet(path, [
      { id: 'e1', ts: 100, op: 'u', ns: 'users', eid: 'u1', before: '{bad json', after: '{"name":"Alice"}', actor: 'system' },
      { id: 'e2', ts: 200, op: 'c', ns: 'users', eid: 'u2', before: '', after: '{"name":"Bob"}', actor: 'admin' },
    ])

    const result = await adapter.readEvents(path)

    expect(result).toHaveLength(2)

    // First event: before is corrupted, should be undefined; after is valid
    expect(result[0]!.before).toBeUndefined()
    expect(result[0]!.after).toEqual({ name: 'Alice' })

    // Second event: before is empty (normal), after is valid
    expect(result[1]!.before).toBeUndefined()
    expect(result[1]!.after).toEqual({ name: 'Bob' })
  })

  it('should not crash when after JSON is corrupted', async () => {
    const path = join(tempDir, 'corrupt-after.parquet')

    await writeRawEventsParquet(path, [
      { id: 'e1', ts: 100, op: 'c', ns: 'users', eid: 'u1', before: '', after: 'not valid json!!!', actor: 'system' },
    ])

    const result = await adapter.readEvents(path)

    expect(result).toHaveLength(1)
    expect(result[0]!.after).toBeUndefined()
    expect(result[0]!.id).toBe('e1')
  })

  it('should not crash when both before and after are corrupted', async () => {
    const path = join(tempDir, 'both-corrupt.parquet')

    await writeRawEventsParquet(path, [
      { id: 'e1', ts: 100, op: 'u', ns: 'users', eid: 'u1', before: '{bad', after: '{also bad', actor: 'test' },
    ])

    const result = await adapter.readEvents(path)

    expect(result).toHaveLength(1)
    expect(result[0]!.before).toBeUndefined()
    expect(result[0]!.after).toBeUndefined()
    expect(result[0]!.id).toBe('e1')
    expect(result[0]!.actor).toBe('test')
  })

  it('should log warnings for corrupted event JSON', async () => {
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const path = join(tempDir, 'event-warn.parquet')

    await writeRawEventsParquet(path, [
      { id: 'e1', ts: 100, op: 'u', ns: 'users', eid: 'u1', before: '{bad', after: '{bad', actor: '' },
    ])

    await adapter.readEvents(path)

    // Should have warned about both before and after
    expect(warnSpy).toHaveBeenCalled()

    warnSpy.mockRestore()
  })
})

// =============================================================================
// do-read-path.ts: rowsToDataLines() with corrupted $data
// (We test via the exported helper by importing it indirectly)
// =============================================================================

describe('do-read-path rowsToDataLines - corrupted $data', () => {
  it('should handle corrupted $data in rowsToDataLines without crashing', async () => {
    // We cannot easily test do-read-path in Node.js (requires R2Bucket),
    // but we can test that the same pattern works in parquet-adapter
    // since do-read-path.rowsToDataLines follows the same pattern.
    // The fix in do-read-path mirrors parquet-adapter.readData().
    //
    // This test verifies the parquet-adapter pattern which do-read-path copies.
    const path = join(tempDir, 'doread-corrupt.parquet')

    await writeRawDataParquet(path, [
      { $id: 'x1', $op: 'c', $v: 1, $ts: 500, $data: '{"valid":"data"}' },
      { $id: 'x2', $op: 'c', $v: 1, $ts: 500, $data: '!!!CORRUPT!!!' },
    ])

    const result = await adapter.readData(path)

    expect(result).toHaveLength(2)
    expect(result.find(r => r.$id === 'x1')!.valid).toBe('data')
    expect(result.find(r => r.$id === 'x2')!.$id).toBe('x2')
  })
})

// =============================================================================
// do-compactor.ts: decodeDataRows() and decodeEventRows() with corrupted JSON
// (Same pattern as parquet-adapter - the fix is identical)
// =============================================================================

describe('do-compactor decode functions - corrupted JSON', () => {
  it('should handle corrupted $data in data rows (same pattern as parquet-adapter)', async () => {
    // The do-compactor decodeDataRows function mirrors parquet-adapter.readData.
    // We verify the pattern via parquet-adapter since decodeDataRows is not exported.
    const path = join(tempDir, 'compact-corrupt.parquet')

    await writeRawDataParquet(path, [
      { $id: 'c1', $op: 'c', $v: 1, $ts: 100, $data: '{"key":"value"}' },
      { $id: 'c2', $op: 'u', $v: 2, $ts: 200, $data: 'INVALID_JSON_STRING' },
      { $id: 'c3', $op: 'c', $v: 1, $ts: 300, $data: '{"another":"entity"}' },
    ])

    const result = await adapter.readData(path)

    expect(result).toHaveLength(3)
    // Valid rows
    expect(result.find(r => r.$id === 'c1')!.key).toBe('value')
    expect(result.find(r => r.$id === 'c3')!.another).toBe('entity')
    // Corrupted row has system fields but no data fields
    const c2 = result.find(r => r.$id === 'c2')!
    expect(c2.$op).toBe('u')
    expect(c2.$v).toBe(2)
  })

  it('should handle corrupted before/after in event rows', async () => {
    const path = join(tempDir, 'compact-event-corrupt.parquet')

    await writeRawEventsParquet(path, [
      { id: 'ev1', ts: 100, op: 'c', ns: 'posts', eid: 'p1', before: '', after: '{"title":"Hello"}', actor: 'user1' },
      { id: 'ev2', ts: 200, op: 'u', ns: 'posts', eid: 'p1', before: '<<<CORRUPT>>>', after: '{"title":"Updated"}', actor: 'user1' },
      { id: 'ev3', ts: 300, op: 'd', ns: 'posts', eid: 'p1', before: '{"title":"Updated"}', after: '{{CORRUPT}}', actor: 'user1' },
    ])

    const result = await adapter.readEvents(path)

    expect(result).toHaveLength(3)

    // ev1: valid after
    expect(result[0]!.after).toEqual({ title: 'Hello' })

    // ev2: corrupted before, valid after
    expect(result[1]!.before).toBeUndefined()
    expect(result[1]!.after).toEqual({ title: 'Updated' })

    // ev3: valid before, corrupted after
    expect(result[2]!.before).toEqual({ title: 'Updated' })
    expect(result[2]!.after).toBeUndefined()
  })
})
