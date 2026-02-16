/**
 * JsonlWriter Test Suite
 *
 * Tests for the JSONL append-only write primitive used by ParqueDB's MergeTree engine.
 * Each mutation is serialized as a single JSON line appended to a file.
 *
 * TDD: RED phase -- these tests are written first, before implementation.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { JsonlWriter } from '@/engine/jsonl-writer'

// =============================================================================
// Helper Functions
// =============================================================================

let tempDir: string

beforeEach(async () => {
  tempDir = await mkdtemp(join(tmpdir(), 'parquedb-jsonl-test-'))
})

afterEach(async () => {
  try {
    await rm(tempDir, { recursive: true, force: true })
  } catch {
    // Ignore cleanup errors
  }
})

/**
 * Read a JSONL file and parse each line back into an object.
 */
async function readJsonlFile(filePath: string): Promise<Record<string, unknown>[]> {
  const content = await readFile(filePath, 'utf-8')
  if (content.length === 0) return []
  return content
    .split('\n')
    .filter((line) => line.length > 0)
    .map((line) => JSON.parse(line) as Record<string, unknown>)
}

// =============================================================================
// Tests
// =============================================================================

describe('JsonlWriter', () => {
  // ---------------------------------------------------------------------------
  // Construction
  // ---------------------------------------------------------------------------

  describe('construction', () => {
    it('creates a writer with a file path', () => {
      const filePath = join(tempDir, 'test.jsonl')
      const writer = new JsonlWriter(filePath)
      expect(writer).toBeInstanceOf(JsonlWriter)
    })
  })

  // ---------------------------------------------------------------------------
  // append()
  // ---------------------------------------------------------------------------

  describe('append()', () => {
    it('writes one JSON line to file', async () => {
      const filePath = join(tempDir, 'test.jsonl')
      const writer = new JsonlWriter(filePath)

      await writer.append({ id: '1', name: 'Alice' })
      await writer.close()

      const lines = await readJsonlFile(filePath)
      expect(lines).toHaveLength(1)
      expect(lines[0]).toEqual({ id: '1', name: 'Alice' })
    })

    it('creates the file if it does not exist', async () => {
      const filePath = join(tempDir, 'nonexistent.jsonl')
      const writer = new JsonlWriter(filePath)

      await writer.append({ created: true })
      await writer.close()

      const content = await readFile(filePath, 'utf-8')
      expect(content).toBeTruthy()

      const lines = await readJsonlFile(filePath)
      expect(lines).toHaveLength(1)
      expect(lines[0]).toEqual({ created: true })
    })

    it('appends multiple lines from multiple append calls', async () => {
      const filePath = join(tempDir, 'multi.jsonl')
      const writer = new JsonlWriter(filePath)

      await writer.append({ seq: 1 })
      await writer.append({ seq: 2 })
      await writer.append({ seq: 3 })
      await writer.close()

      const lines = await readJsonlFile(filePath)
      expect(lines).toHaveLength(3)
      expect(lines[0]).toEqual({ seq: 1 })
      expect(lines[1]).toEqual({ seq: 2 })
      expect(lines[2]).toEqual({ seq: 3 })
    })

    it('each line ends with a newline (no empty lines except trailing)', async () => {
      const filePath = join(tempDir, 'newlines.jsonl')
      const writer = new JsonlWriter(filePath)

      await writer.append({ a: 1 })
      await writer.append({ b: 2 })
      await writer.close()

      const content = await readFile(filePath, 'utf-8')

      // File should end with a newline
      expect(content.endsWith('\n')).toBe(true)

      // Split by newline: last element should be empty (trailing newline), all others non-empty
      const parts = content.split('\n')
      expect(parts[parts.length - 1]).toBe('')
      for (let i = 0; i < parts.length - 1; i++) {
        expect(parts[i].length).toBeGreaterThan(0)
      }
    })
  })

  // ---------------------------------------------------------------------------
  // appendBatch()
  // ---------------------------------------------------------------------------

  describe('appendBatch()', () => {
    it('writes multiple lines in one call', async () => {
      const filePath = join(tempDir, 'batch.jsonl')
      const writer = new JsonlWriter(filePath)

      await writer.appendBatch([
        { id: 'a', value: 10 },
        { id: 'b', value: 20 },
        { id: 'c', value: 30 },
      ])
      await writer.close()

      const lines = await readJsonlFile(filePath)
      expect(lines).toHaveLength(3)
      expect(lines[0]).toEqual({ id: 'a', value: 10 })
      expect(lines[1]).toEqual({ id: 'b', value: 20 })
      expect(lines[2]).toEqual({ id: 'c', value: 30 })
    })

    it('writes all lines atomically (single write call)', async () => {
      const filePath = join(tempDir, 'atomic.jsonl')
      const writer = new JsonlWriter(filePath)

      // Write a batch -- all lines should appear together
      const batch = Array.from({ length: 100 }, (_, i) => ({ idx: i }))
      await writer.appendBatch(batch)
      await writer.close()

      const lines = await readJsonlFile(filePath)
      expect(lines).toHaveLength(100)

      // Verify order is preserved
      for (let i = 0; i < 100; i++) {
        expect(lines[i]).toEqual({ idx: i })
      }
    })
  })

  // ---------------------------------------------------------------------------
  // flush()
  // ---------------------------------------------------------------------------

  describe('flush()', () => {
    it('forces buffered writes to disk', async () => {
      const filePath = join(tempDir, 'flush.jsonl')
      const writer = new JsonlWriter(filePath)

      await writer.append({ flushed: true })
      await writer.flush()

      // After flush, data should be readable from disk
      const lines = await readJsonlFile(filePath)
      expect(lines).toHaveLength(1)
      expect(lines[0]).toEqual({ flushed: true })

      await writer.close()
    })
  })

  // ---------------------------------------------------------------------------
  // close()
  // ---------------------------------------------------------------------------

  describe('close()', () => {
    it('flushes and releases the file handle', async () => {
      const filePath = join(tempDir, 'close.jsonl')
      const writer = new JsonlWriter(filePath)

      await writer.append({ before: 'close' })
      await writer.close()

      // After close, data should be readable
      const lines = await readJsonlFile(filePath)
      expect(lines).toHaveLength(1)
      expect(lines[0]).toEqual({ before: 'close' })
    })
  })

  // ---------------------------------------------------------------------------
  // getPath()
  // ---------------------------------------------------------------------------

  describe('getPath()', () => {
    it('returns the file path', () => {
      const filePath = join(tempDir, 'path-check.jsonl')
      const writer = new JsonlWriter(filePath)
      expect(writer.getPath()).toBe(filePath)
    })
  })

  // ---------------------------------------------------------------------------
  // getLineCount()
  // ---------------------------------------------------------------------------

  describe('getLineCount()', () => {
    it('starts at 0', () => {
      const filePath = join(tempDir, 'count.jsonl')
      const writer = new JsonlWriter(filePath)
      expect(writer.getLineCount()).toBe(0)
    })

    it('tracks total lines written since construction', async () => {
      const filePath = join(tempDir, 'count.jsonl')
      const writer = new JsonlWriter(filePath)

      await writer.append({ a: 1 })
      expect(writer.getLineCount()).toBe(1)

      await writer.append({ b: 2 })
      expect(writer.getLineCount()).toBe(2)

      await writer.appendBatch([{ c: 3 }, { d: 4 }, { e: 5 }])
      expect(writer.getLineCount()).toBe(5)

      await writer.close()
    })
  })

  // ---------------------------------------------------------------------------
  // getByteCount()
  // ---------------------------------------------------------------------------

  describe('getByteCount()', () => {
    it('starts at 0', () => {
      const filePath = join(tempDir, 'bytes.jsonl')
      const writer = new JsonlWriter(filePath)
      expect(writer.getByteCount()).toBe(0)
    })

    it('tracks total bytes written for compaction threshold checks', async () => {
      const filePath = join(tempDir, 'bytes.jsonl')
      const writer = new JsonlWriter(filePath)

      const line = { key: 'value' }
      const expectedBytes = Buffer.byteLength(JSON.stringify(line) + '\n', 'utf-8')

      await writer.append(line)
      expect(writer.getByteCount()).toBe(expectedBytes)

      await writer.append(line)
      expect(writer.getByteCount()).toBe(expectedBytes * 2)

      await writer.close()
    })
  })

  // ---------------------------------------------------------------------------
  // Concurrent appends
  // ---------------------------------------------------------------------------

  describe('concurrent appends', () => {
    it('multiple async appends do not interleave lines', async () => {
      const filePath = join(tempDir, 'concurrent.jsonl')
      const writer = new JsonlWriter(filePath)

      // Fire off many concurrent appends
      const promises = Array.from({ length: 50 }, (_, i) =>
        writer.append({ idx: i, data: `line-${i}` }),
      )
      await Promise.all(promises)
      await writer.close()

      const lines = await readJsonlFile(filePath)
      expect(lines).toHaveLength(50)

      // Each line should be a valid, complete JSON object (no interleaving)
      for (const line of lines) {
        expect(line).toHaveProperty('idx')
        expect(line).toHaveProperty('data')
        expect(typeof line.idx).toBe('number')
        expect(typeof line.data).toBe('string')
      }

      // All 50 indices should be present (order may vary due to concurrency scheduling)
      const indices = new Set(lines.map((l) => l.idx))
      expect(indices.size).toBe(50)
    })
  })

  // ---------------------------------------------------------------------------
  // Large line
  // ---------------------------------------------------------------------------

  describe('large line', () => {
    it('handles a line with a large JSON object (100KB+)', async () => {
      const filePath = join(tempDir, 'large.jsonl')
      const writer = new JsonlWriter(filePath)

      // Create a large object > 100KB
      const largeValue = 'x'.repeat(100 * 1024)
      const largeObj = { id: 'large', payload: largeValue }

      await writer.append(largeObj)
      await writer.close()

      const lines = await readJsonlFile(filePath)
      expect(lines).toHaveLength(1)
      expect(lines[0]).toEqual(largeObj)
      expect((lines[0].payload as string).length).toBe(100 * 1024)
    })
  })

  // ---------------------------------------------------------------------------
  // Unicode
  // ---------------------------------------------------------------------------

  describe('unicode', () => {
    it('handles unicode content correctly', async () => {
      const filePath = join(tempDir, 'unicode.jsonl')
      const writer = new JsonlWriter(filePath)

      const unicodeLine = {
        emoji: '\u{1F680}\u{1F30D}\u{2728}',
        japanese: '\u3053\u3093\u306B\u3061\u306F',
        chinese: '\u4F60\u597D\u4E16\u754C',
        arabic: '\u0645\u0631\u062D\u0628\u0627',
        mixed: 'Hello \u4E16\u754C \u{1F44B}',
      }

      await writer.append(unicodeLine)
      await writer.close()

      const lines = await readJsonlFile(filePath)
      expect(lines).toHaveLength(1)
      expect(lines[0]).toEqual(unicodeLine)
    })

    it('tracks byte count correctly for multi-byte characters', async () => {
      const filePath = join(tempDir, 'unicode-bytes.jsonl')
      const writer = new JsonlWriter(filePath)

      const line = { text: '\u{1F680}' } // Rocket emoji
      const serialized = JSON.stringify(line) + '\n'
      const expectedBytes = Buffer.byteLength(serialized, 'utf-8')

      await writer.append(line)
      expect(writer.getByteCount()).toBe(expectedBytes)

      await writer.close()
    })
  })

  // ---------------------------------------------------------------------------
  // After close
  // ---------------------------------------------------------------------------

  describe('after close', () => {
    it('append after close throws an error', async () => {
      const filePath = join(tempDir, 'closed.jsonl')
      const writer = new JsonlWriter(filePath)

      await writer.append({ before: 'close' })
      await writer.close()

      await expect(writer.append({ after: 'close' })).rejects.toThrow()
    })

    it('appendBatch after close throws an error', async () => {
      const filePath = join(tempDir, 'closed-batch.jsonl')
      const writer = new JsonlWriter(filePath)

      await writer.close()

      await expect(writer.appendBatch([{ after: 'close' }])).rejects.toThrow()
    })

    it('flush after close throws an error', async () => {
      const filePath = join(tempDir, 'closed-flush.jsonl')
      const writer = new JsonlWriter(filePath)

      await writer.close()

      await expect(writer.flush()).rejects.toThrow()
    })

    it('close is idempotent (calling close twice does not throw)', async () => {
      const filePath = join(tempDir, 'double-close.jsonl')
      const writer = new JsonlWriter(filePath)

      await writer.append({ data: 1 })
      await writer.close()

      // Second close should not throw
      await expect(writer.close()).resolves.toBeUndefined()
    })
  })
})
