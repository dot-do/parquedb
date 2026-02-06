/**
 * JsonlWriter Test Suite
 *
 * Tests the JsonlWriter write primitive including:
 * - Basic append and appendBatch operations
 * - Line/byte count tracking
 * - Closed-state guards
 * - Write queue error recovery (Bug fix: rejected promise no longer bricks queue)
 * - Concurrent write integrity
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { JsonlWriter } from '@/engine/jsonl-writer'
import { mkdtemp, rm, readFile, chmod } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'

describe('JsonlWriter', () => {
  let dir: string
  let writer: JsonlWriter

  beforeEach(async () => {
    dir = await mkdtemp(join(tmpdir(), 'jsonl-writer-'))
  })

  afterEach(async () => {
    try { await writer?.close() } catch {}
    await rm(dir, { recursive: true, force: true })
  })

  // ===========================================================================
  // Basic operations
  // ===========================================================================

  it('append writes a single JSON line', async () => {
    writer = new JsonlWriter(join(dir, 'test.jsonl'))
    await writer.append({ name: 'Alice' })
    await writer.close()
    const content = await readFile(join(dir, 'test.jsonl'), 'utf-8')
    expect(content.trim()).toBe('{"name":"Alice"}')
  })

  it('appendBatch writes multiple lines atomically', async () => {
    writer = new JsonlWriter(join(dir, 'test.jsonl'))
    await writer.appendBatch([{ a: 1 }, { b: 2 }, { c: 3 }])
    await writer.close()
    const lines = (await readFile(join(dir, 'test.jsonl'), 'utf-8')).trim().split('\n')
    expect(lines).toHaveLength(3)
  })

  // ===========================================================================
  // Tracking
  // ===========================================================================

  it('tracks lineCount and byteCount', async () => {
    writer = new JsonlWriter(join(dir, 'test.jsonl'))
    expect(writer.getLineCount()).toBe(0)
    expect(writer.getByteCount()).toBe(0)
    await writer.append({ name: 'Alice' })
    expect(writer.getLineCount()).toBe(1)
    expect(writer.getByteCount()).toBeGreaterThan(0)
  })

  it('getPath returns the file path', () => {
    const path = join(dir, 'test.jsonl')
    writer = new JsonlWriter(path)
    expect(writer.getPath()).toBe(path)
  })

  // ===========================================================================
  // Closed-state guards
  // ===========================================================================

  it('throws on append after close', async () => {
    writer = new JsonlWriter(join(dir, 'test.jsonl'))
    await writer.close()
    await expect(writer.append({ a: 1 })).rejects.toThrow('JsonlWriter is closed')
  })

  it('throws on appendBatch after close', async () => {
    writer = new JsonlWriter(join(dir, 'test.jsonl'))
    await writer.close()
    await expect(writer.appendBatch([{ a: 1 }])).rejects.toThrow('JsonlWriter is closed')
  })

  it('close is idempotent', async () => {
    writer = new JsonlWriter(join(dir, 'test.jsonl'))
    await writer.close()
    await writer.close() // Should not throw
  })

  // ===========================================================================
  // Flush
  // ===========================================================================

  it('flush drains the write queue', async () => {
    writer = new JsonlWriter(join(dir, 'test.jsonl'))
    await writer.append({ a: 1 })
    await writer.flush()
    const content = await readFile(join(dir, 'test.jsonl'), 'utf-8')
    expect(content.trim()).toBe('{"a":1}')
  })

  // ===========================================================================
  // Error recovery (Bug fix: rejected promise must not brick the queue)
  // ===========================================================================

  it('writer recovers after a failed write attempt', async () => {
    const filePath = join(dir, 'test.jsonl')
    writer = new JsonlWriter(filePath)

    // First write succeeds
    await writer.append({ first: true })

    // Force a failure by writing to a path inside a non-existent directory
    // We do this by temporarily replacing the internal path
    const originalPath = (writer as any).path
    ;(writer as any).path = join(dir, 'nonexistent', 'deep', 'file.jsonl')

    // This write should fail
    await expect(writer.append({ shouldFail: true })).rejects.toThrow()

    // Restore the original path
    ;(writer as any).path = originalPath

    // This write should SUCCEED - the queue must not be bricked
    await writer.append({ recovered: true })
    await writer.close()

    const content = await readFile(filePath, 'utf-8')
    const lines = content.trim().split('\n')
    expect(lines).toHaveLength(2)
    expect(JSON.parse(lines[0])).toEqual({ first: true })
    expect(JSON.parse(lines[1])).toEqual({ recovered: true })
  })

  it('appendBatch recovers after a failed batch write', async () => {
    const filePath = join(dir, 'test.jsonl')
    writer = new JsonlWriter(filePath)

    await writer.append({ before: true })

    // Force a batch failure
    const originalPath = (writer as any).path
    ;(writer as any).path = join(dir, 'nonexistent', 'deep', 'file.jsonl')
    await expect(writer.appendBatch([{ shouldFail: true }])).rejects.toThrow()
    ;(writer as any).path = originalPath

    // Subsequent batch should succeed
    await writer.appendBatch([{ recovered: 1 }, { recovered: 2 }])
    await writer.close()

    const content = await readFile(filePath, 'utf-8')
    const lines = content.trim().split('\n')
    expect(lines).toHaveLength(3)
    expect(JSON.parse(lines[0])).toEqual({ before: true })
    expect(JSON.parse(lines[1])).toEqual({ recovered: 1 })
    expect(JSON.parse(lines[2])).toEqual({ recovered: 2 })
  })

  // ===========================================================================
  // Concurrent writes
  // ===========================================================================

  it('concurrent appends maintain integrity', async () => {
    writer = new JsonlWriter(join(dir, 'test.jsonl'))
    const promises = Array.from({ length: 20 }, (_, i) =>
      writer.append({ index: i })
    )
    await Promise.all(promises)
    await writer.close()

    const lines = (await readFile(join(dir, 'test.jsonl'), 'utf-8')).trim().split('\n')
    expect(lines).toHaveLength(20)
    // Each line should be valid JSON
    for (const line of lines) {
      expect(() => JSON.parse(line)).not.toThrow()
    }
  })
})
