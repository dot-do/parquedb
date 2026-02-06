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

  it('getLineCount returns correct count after appends', async () => {
    writer = new JsonlWriter(join(dir, 'test.jsonl'))
    expect(writer.getLineCount()).toBe(0)
    await writer.append({ a: 1 })
    expect(writer.getLineCount()).toBe(1)
    await writer.append({ b: 2 })
    expect(writer.getLineCount()).toBe(2)
    await writer.appendBatch([{ c: 3 }, { d: 4 }, { e: 5 }])
    expect(writer.getLineCount()).toBe(5)
  })

  it('getByteCount returns correct byte count', async () => {
    writer = new JsonlWriter(join(dir, 'test.jsonl'))
    expect(writer.getByteCount()).toBe(0)

    const line = { name: 'Alice' }
    const expectedBytes = Buffer.byteLength(JSON.stringify(line) + '\n', 'utf-8')
    await writer.append(line)
    expect(writer.getByteCount()).toBe(expectedBytes)

    const batch = [{ x: 1 }, { y: 2 }]
    const batchData = batch.map(l => JSON.stringify(l) + '\n').join('')
    const batchBytes = Buffer.byteLength(batchData, 'utf-8')
    await writer.appendBatch(batch)
    expect(writer.getByteCount()).toBe(expectedBytes + batchBytes)
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

  it('throws on flush after close', async () => {
    writer = new JsonlWriter(join(dir, 'test.jsonl'))
    await writer.close()
    await expect(writer.flush()).rejects.toThrow('JsonlWriter is closed')
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

  it('concurrent appends are serialized (20 concurrent appends produce 20 valid JSON lines)', async () => {
    writer = new JsonlWriter(join(dir, 'test.jsonl'))
    const promises = Array.from({ length: 20 }, (_, i) =>
      writer.append({ index: i })
    )
    await Promise.all(promises)
    await writer.close()

    const lines = (await readFile(join(dir, 'test.jsonl'), 'utf-8')).trim().split('\n')
    expect(lines).toHaveLength(20)

    // Each line should be valid JSON
    const parsed = lines.map((line) => JSON.parse(line))
    // All 20 indices should be present (order may vary due to concurrency)
    const indices = parsed.map((obj: { index: number }) => obj.index).sort((a: number, b: number) => a - b)
    expect(indices).toEqual(Array.from({ length: 20 }, (_, i) => i))

    // lineCount should reflect all 20 writes
    expect(writer.getLineCount()).toBe(20)
  })

  // ===========================================================================
  // Concurrent write stress tests (zou5.28)
  // ===========================================================================

  it('50 concurrent append() calls produce exactly 50 valid JSONL lines with no interleaving', async () => {
    const filePath = join(dir, 'stress-append.jsonl')
    writer = new JsonlWriter(filePath)

    // Fire 50 append() calls concurrently
    const promises = Array.from({ length: 50 }, (_, i) =>
      writer.append({ $id: `entity-${i}`, $type: 'test', value: `data-${i}` })
    )
    await Promise.all(promises)
    await writer.close()

    const raw = await readFile(filePath, 'utf-8')
    const lines = raw.trim().split('\n')

    // Exactly 50 lines
    expect(lines).toHaveLength(50)

    // Every line must be valid JSON (no partial writes or interleaving)
    const parsed = lines.map((line, idx) => {
      let obj: any
      try {
        obj = JSON.parse(line)
      } catch (e) {
        throw new Error(`Line ${idx} is not valid JSON: "${line}"`)
      }
      return obj
    })

    // Every entity $id appears exactly once
    const ids = parsed.map((obj: any) => obj.$id)
    const uniqueIds = new Set(ids)
    expect(uniqueIds.size).toBe(50)

    // All expected IDs are present
    const expectedIds = new Set(Array.from({ length: 50 }, (_, i) => `entity-${i}`))
    expect(uniqueIds).toEqual(expectedIds)

    // Internal counters match
    expect(writer.getLineCount()).toBe(50)
    expect(writer.getByteCount()).toBeGreaterThan(0)
  })

  it('5 concurrent appendBatch() calls with 10 lines each produce exactly 50 valid JSONL lines', async () => {
    const filePath = join(dir, 'stress-batch.jsonl')
    writer = new JsonlWriter(filePath)

    // Fire 5 appendBatch() calls concurrently, each with 10 lines
    const promises = Array.from({ length: 5 }, (_, batchIdx) => {
      const batch = Array.from({ length: 10 }, (_, lineIdx) => ({
        $id: `batch-${batchIdx}-line-${lineIdx}`,
        $type: 'test',
        batchIdx,
        lineIdx,
      }))
      return writer.appendBatch(batch)
    })
    await Promise.all(promises)
    await writer.close()

    const raw = await readFile(filePath, 'utf-8')
    const lines = raw.trim().split('\n')

    // Exactly 50 lines
    expect(lines).toHaveLength(50)

    // Every line must be valid JSON
    const parsed = lines.map((line, idx) => {
      let obj: any
      try {
        obj = JSON.parse(line)
      } catch (e) {
        throw new Error(`Line ${idx} is not valid JSON: "${line}"`)
      }
      return obj
    })

    // Every $id appears exactly once
    const ids = parsed.map((obj: any) => obj.$id)
    const uniqueIds = new Set(ids)
    expect(uniqueIds.size).toBe(50)

    // All expected IDs are present
    const expectedIds = new Set(
      Array.from({ length: 5 }, (_, b) =>
        Array.from({ length: 10 }, (_, l) => `batch-${b}-line-${l}`)
      ).flat()
    )
    expect(uniqueIds).toEqual(expectedIds)

    // Each batch's 10 lines must appear consecutively (batch atomicity)
    // Since appendBatch writes all lines in a single appendFile call,
    // a batch's lines should not be interleaved with another batch's lines.
    for (let batchIdx = 0; batchIdx < 5; batchIdx++) {
      const batchLinePositions = parsed
        .map((obj: any, pos: number) => (obj.batchIdx === batchIdx ? pos : -1))
        .filter((pos: number) => pos !== -1)

      expect(batchLinePositions).toHaveLength(10)

      // Positions should be contiguous (consecutive)
      for (let i = 1; i < batchLinePositions.length; i++) {
        expect(batchLinePositions[i]).toBe(batchLinePositions[i - 1] + 1)
      }
    }

    // Internal counters match
    expect(writer.getLineCount()).toBe(50)
  })

  it('mixed concurrent append() and appendBatch() produces correct total lines with no corruption', async () => {
    const filePath = join(dir, 'stress-mixed.jsonl')
    writer = new JsonlWriter(filePath)

    // 30 individual appends + 4 batches of 5 = 30 + 20 = 50 total lines
    const appendPromises = Array.from({ length: 30 }, (_, i) =>
      writer.append({ $id: `single-${i}`, $type: 'single', index: i })
    )

    const batchPromises = Array.from({ length: 4 }, (_, batchIdx) => {
      const batch = Array.from({ length: 5 }, (_, lineIdx) => ({
        $id: `batch-${batchIdx}-${lineIdx}`,
        $type: 'batch',
        batchIdx,
        lineIdx,
      }))
      return writer.appendBatch(batch)
    })

    // Fire all writes concurrently (mix of append and appendBatch)
    await Promise.all([...appendPromises, ...batchPromises])
    await writer.close()

    const raw = await readFile(filePath, 'utf-8')
    const lines = raw.trim().split('\n')

    // Total line count: 30 singles + 4 batches * 5 = 50
    expect(lines).toHaveLength(50)

    // Every line must be valid JSON -- no partial writes or interleaving
    const parsed = lines.map((line, idx) => {
      let obj: any
      try {
        obj = JSON.parse(line)
      } catch (e) {
        throw new Error(`Line ${idx} is not valid JSON: "${line}"`)
      }
      return obj
    })

    // Every $id appears exactly once
    const ids = parsed.map((obj: any) => obj.$id)
    const uniqueIds = new Set(ids)
    expect(uniqueIds.size).toBe(50)

    // Verify all single IDs present
    for (let i = 0; i < 30; i++) {
      expect(uniqueIds.has(`single-${i}`)).toBe(true)
    }

    // Verify all batch IDs present
    for (let b = 0; b < 4; b++) {
      for (let l = 0; l < 5; l++) {
        expect(uniqueIds.has(`batch-${b}-${l}`)).toBe(true)
      }
    }

    // Batch lines must still be contiguous (batch atomicity preserved under mixed load)
    for (let batchIdx = 0; batchIdx < 4; batchIdx++) {
      const batchLinePositions = parsed
        .map((obj: any, pos: number) => (obj.$type === 'batch' && obj.batchIdx === batchIdx ? pos : -1))
        .filter((pos: number) => pos !== -1)

      expect(batchLinePositions).toHaveLength(5)

      // Positions should be contiguous
      for (let i = 1; i < batchLinePositions.length; i++) {
        expect(batchLinePositions[i]).toBe(batchLinePositions[i - 1] + 1)
      }
    }

    // Internal counters match
    expect(writer.getLineCount()).toBe(50)
    expect(writer.getByteCount()).toBeGreaterThan(0)
  })

  it('no partial writes: every line has correct structure after concurrent stress', async () => {
    const filePath = join(dir, 'stress-integrity.jsonl')
    writer = new JsonlWriter(filePath)

    // Write entities with large-ish payloads to increase chance of partial write detection
    const promises = Array.from({ length: 50 }, (_, i) =>
      writer.append({
        $id: `integrity-${i}`,
        $type: 'document',
        payload: 'x'.repeat(200), // 200 char payload to make lines non-trivial
        index: i,
        ts: Date.now(),
      })
    )
    await Promise.all(promises)
    await writer.close()

    const raw = await readFile(filePath, 'utf-8')

    // File must end with newline (no truncated last line)
    expect(raw.endsWith('\n')).toBe(true)

    const lines = raw.trim().split('\n')
    expect(lines).toHaveLength(50)

    // Parse every line and validate structure
    const parsed = lines.map((line, idx) => {
      let obj: any
      try {
        obj = JSON.parse(line)
      } catch (e) {
        throw new Error(`Line ${idx} is corrupted (invalid JSON): "${line.substring(0, 80)}..."`)
      }

      // Verify structural integrity of each record
      expect(obj).toHaveProperty('$id')
      expect(obj).toHaveProperty('$type', 'document')
      expect(obj).toHaveProperty('payload')
      expect(obj.payload).toHaveLength(200)
      expect(obj).toHaveProperty('index')
      expect(typeof obj.index).toBe('number')

      return obj
    })

    // All IDs unique and present
    const ids = new Set(parsed.map((obj: any) => obj.$id))
    expect(ids.size).toBe(50)
  })

  it('byteCount is accurate after concurrent mixed writes', async () => {
    const filePath = join(dir, 'stress-bytes.jsonl')
    writer = new JsonlWriter(filePath)

    const appendPromises = Array.from({ length: 25 }, (_, i) =>
      writer.append({ $id: `a-${i}`, v: i })
    )
    const batchPromises = Array.from({ length: 5 }, (_, b) =>
      writer.appendBatch(
        Array.from({ length: 5 }, (_, l) => ({ $id: `b-${b}-${l}`, v: b * 5 + l }))
      )
    )

    await Promise.all([...appendPromises, ...batchPromises])
    await writer.close()

    // Total lines: 25 + 5*5 = 50
    expect(writer.getLineCount()).toBe(50)

    // byteCount should match the actual file size
    const raw = await readFile(filePath, 'utf-8')
    const actualBytes = Buffer.byteLength(raw, 'utf-8')
    expect(writer.getByteCount()).toBe(actualBytes)
  })
})
