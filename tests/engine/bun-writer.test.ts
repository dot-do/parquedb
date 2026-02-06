/**
 * BunJsonlWriter Test Suite
 *
 * Tests the Node.js fallback path of BunJsonlWriter (since vitest runs on Node.js).
 * Verifies that the writer correctly appends JSONL lines using fs.appendFile,
 * tracks line counts, and produces output compatible with the JSONL reader.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtemp, rm, readFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { BunJsonlWriter } from '@/engine/bun-writer'
import { replay } from '@/engine/jsonl-reader'

// =============================================================================
// Test Setup
// =============================================================================

let tempDir: string

beforeEach(async () => {
  tempDir = await mkdtemp(join(tmpdir(), 'bun-writer-'))
})

afterEach(async () => {
  await rm(tempDir, { recursive: true, force: true })
})

// =============================================================================
// Constructor & Initialization
// =============================================================================

describe('BunJsonlWriter', () => {
  describe('constructor', () => {
    it('creates writer with the given path', () => {
      const path = join(tempDir, 'test.jsonl')
      const writer = new BunJsonlWriter(path)
      expect(writer).toBeInstanceOf(BunJsonlWriter)
    })
  })

  describe('init', () => {
    it('succeeds on Node.js (no-op fallback)', async () => {
      const path = join(tempDir, 'test.jsonl')
      const writer = new BunJsonlWriter(path)
      await expect(writer.init()).resolves.toBeUndefined()
    })
  })

  // ===========================================================================
  // append()
  // ===========================================================================

  describe('append', () => {
    it('writes a single line to file', async () => {
      const path = join(tempDir, 'test.jsonl')
      const writer = new BunJsonlWriter(path)
      await writer.init()

      await writer.append({ $op: 'c', $id: 'abc', name: 'Alice' })
      await writer.close()

      const content = await readFile(path, 'utf-8')
      expect(content.trim()).toBe('{"$op":"c","$id":"abc","name":"Alice"}')
    })

    it('appends multiple lines sequentially', async () => {
      const path = join(tempDir, 'test.jsonl')
      const writer = new BunJsonlWriter(path)
      await writer.init()

      await writer.append({ id: 1, value: 'first' })
      await writer.append({ id: 2, value: 'second' })
      await writer.append({ id: 3, value: 'third' })
      await writer.close()

      const content = await readFile(path, 'utf-8')
      const lines = content.trim().split('\n')
      expect(lines).toHaveLength(3)
      expect(JSON.parse(lines[0])).toEqual({ id: 1, value: 'first' })
      expect(JSON.parse(lines[1])).toEqual({ id: 2, value: 'second' })
      expect(JSON.parse(lines[2])).toEqual({ id: 3, value: 'third' })
    })
  })

  // ===========================================================================
  // appendBatch()
  // ===========================================================================

  describe('appendBatch', () => {
    it('writes multiple lines at once', async () => {
      const path = join(tempDir, 'test.jsonl')
      const writer = new BunJsonlWriter(path)
      await writer.init()

      await writer.appendBatch([
        { id: 1, value: 'a' },
        { id: 2, value: 'b' },
        { id: 3, value: 'c' },
      ])
      await writer.close()

      const content = await readFile(path, 'utf-8')
      const lines = content.trim().split('\n')
      expect(lines).toHaveLength(3)
      expect(JSON.parse(lines[0])).toEqual({ id: 1, value: 'a' })
      expect(JSON.parse(lines[1])).toEqual({ id: 2, value: 'b' })
      expect(JSON.parse(lines[2])).toEqual({ id: 3, value: 'c' })
    })

    it('is a no-op with empty array', async () => {
      const path = join(tempDir, 'test.jsonl')
      const writer = new BunJsonlWriter(path)
      await writer.init()

      await writer.appendBatch([])
      await writer.close()

      // File should not exist since nothing was written
      await expect(readFile(path, 'utf-8')).rejects.toThrow()
    })
  })

  // ===========================================================================
  // flush() and close()
  // ===========================================================================

  describe('flush', () => {
    it('succeeds on Node.js fallback (no-op)', async () => {
      const path = join(tempDir, 'test.jsonl')
      const writer = new BunJsonlWriter(path)
      await writer.init()

      await writer.append({ id: 1 })
      await expect(writer.flush()).resolves.toBeUndefined()
      await writer.close()
    })
  })

  describe('close', () => {
    it('succeeds and can be called safely', async () => {
      const path = join(tempDir, 'test.jsonl')
      const writer = new BunJsonlWriter(path)
      await writer.init()

      await writer.append({ id: 1 })
      await expect(writer.close()).resolves.toBeUndefined()
    })
  })

  // ===========================================================================
  // count and usesBunFileSink
  // ===========================================================================

  describe('count', () => {
    it('tracks total lines written via append and appendBatch', async () => {
      const path = join(tempDir, 'test.jsonl')
      const writer = new BunJsonlWriter(path)
      await writer.init()

      expect(writer.count).toBe(0)

      await writer.append({ id: 1 })
      expect(writer.count).toBe(1)

      await writer.append({ id: 2 })
      expect(writer.count).toBe(2)

      await writer.appendBatch([{ id: 3 }, { id: 4 }, { id: 5 }])
      expect(writer.count).toBe(5)

      await writer.close()
    })
  })

  describe('usesBunFileSink', () => {
    it('returns false on Node.js', async () => {
      const path = join(tempDir, 'test.jsonl')
      const writer = new BunJsonlWriter(path)
      await writer.init()

      expect(writer.usesBunFileSink).toBe(false)
      await writer.close()
    })
  })

  // ===========================================================================
  // JSONL validity and reader integration
  // ===========================================================================

  describe('JSONL validity', () => {
    it('produces valid JSONL that can be parsed line-by-line', async () => {
      const path = join(tempDir, 'test.jsonl')
      const writer = new BunJsonlWriter(path)
      await writer.init()

      const records = [
        { $op: 'c', $id: 'u1', name: 'Alice', tags: ['admin', 'user'] },
        { $op: 'u', $id: 'u1', name: 'Alice B', nested: { deep: true } },
        { $op: 'c', $id: 'u2', name: 'Bob', score: 42.5 },
      ]

      for (const rec of records) {
        await writer.append(rec)
      }
      await writer.close()

      const content = await readFile(path, 'utf-8')
      const lines = content.trim().split('\n')
      expect(lines).toHaveLength(3)

      // Every line must parse to valid JSON matching the original record
      for (let i = 0; i < lines.length; i++) {
        const parsed = JSON.parse(lines[i])
        expect(parsed).toEqual(records[i])
      }
    })
  })

  describe('integration with replay()', () => {
    it('writes data that replay() reads back correctly', async () => {
      const path = join(tempDir, 'test.jsonl')
      const writer = new BunJsonlWriter(path)
      await writer.init()

      const records = [
        { $op: 'c', $id: 'p1', title: 'First Post', $v: 1 },
        { $op: 'u', $id: 'p1', title: 'First Post (edited)', $v: 2 },
        { $op: 'c', $id: 'p2', title: 'Second Post', $v: 1 },
      ]

      await writer.appendBatch(records)
      await writer.close()

      // Use the JSONL reader to replay
      const replayed = await replay(path)
      expect(replayed).toHaveLength(3)
      expect(replayed[0]).toEqual(records[0])
      expect(replayed[1]).toEqual(records[1])
      expect(replayed[2]).toEqual(records[2])
    })
  })
})
