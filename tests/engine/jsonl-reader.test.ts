/**
 * JsonlReader Test Suite
 *
 * Tests for JSONL file reading functions used by the MergeTree engine.
 * These functions replay lines from .jsonl files for startup buffer rebuild
 * and compaction of rotated files.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { mkdtemp, writeFile, rm, mkdir } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { replay, replayInto, replayRange, lineCount } from '@/engine/jsonl-reader'

// =============================================================================
// Test Fixtures
// =============================================================================

let tempDir: string

beforeEach(async () => {
  tempDir = await mkdtemp(join(tmpdir(), 'jsonl-reader-test-'))
})

afterEach(async () => {
  await rm(tempDir, { recursive: true, force: true })
})

/** Helper to write a JSONL file from an array of objects */
async function writeJsonl(filename: string, lines: unknown[]): Promise<string> {
  const filePath = join(tempDir, filename)
  const content = lines.map(line => JSON.stringify(line)).join('\n')
  await writeFile(filePath, content, 'utf-8')
  return filePath
}

/** Helper to write raw string content as a file */
async function writeRaw(filename: string, content: string): Promise<string> {
  const filePath = join(tempDir, filename)
  await writeFile(filePath, content, 'utf-8')
  return filePath
}

// =============================================================================
// replay() Tests
// =============================================================================

describe('JsonlReader', () => {
  describe('replay(path)', () => {
    it('reads all lines and returns array of parsed objects', async () => {
      const data = [
        { $id: '1', name: 'Alice', $ts: 1000 },
        { $id: '2', name: 'Bob', $ts: 2000 },
        { $id: '3', name: 'Charlie', $ts: 3000 },
      ]
      const filePath = await writeJsonl('data.jsonl', data)

      const result = await replay(filePath)

      expect(result).toHaveLength(3)
      expect(result[0]).toEqual({ $id: '1', name: 'Alice', $ts: 1000 })
      expect(result[1]).toEqual({ $id: '2', name: 'Bob', $ts: 2000 })
      expect(result[2]).toEqual({ $id: '3', name: 'Charlie', $ts: 3000 })
    })

    it('roundtrip: write lines manually, read back with replay() — data matches', async () => {
      const original = [
        { op: 'CREATE', target: 'posts:abc', after: { title: 'Hello' }, ts: 1000 },
        { op: 'UPDATE', target: 'posts:abc', before: { title: 'Hello' }, after: { title: 'World' }, ts: 2000 },
        { op: 'DELETE', target: 'posts:abc', before: { title: 'World' }, ts: 3000 },
      ]
      const filePath = await writeJsonl('events.jsonl', original)

      const result = await replay(filePath)

      expect(result).toEqual(original)
    })

    it('returns empty array for empty file', async () => {
      const filePath = await writeRaw('empty.jsonl', '')

      const result = await replay(filePath)

      expect(result).toEqual([])
    })

    it('returns empty array for missing file (no error thrown)', async () => {
      const filePath = join(tempDir, 'nonexistent.jsonl')

      const result = await replay(filePath)

      expect(result).toEqual([])
    })

    it('skips empty lines between entries', async () => {
      const content = [
        JSON.stringify({ $id: '1', name: 'Alice' }),
        '',
        '',
        JSON.stringify({ $id: '2', name: 'Bob' }),
        '',
        JSON.stringify({ $id: '3', name: 'Charlie' }),
        '',
      ].join('\n')
      const filePath = await writeRaw('sparse.jsonl', content)

      const result = await replay(filePath)

      expect(result).toHaveLength(3)
      expect(result[0]).toEqual({ $id: '1', name: 'Alice' })
      expect(result[1]).toEqual({ $id: '2', name: 'Bob' })
      expect(result[2]).toEqual({ $id: '3', name: 'Charlie' })
    })

    it('logs warning and skips corrupted line, returns valid lines', async () => {
      const content = [
        JSON.stringify({ $id: '1', name: 'Alice' }),
        'this is not valid json{{{',
        JSON.stringify({ $id: '2', name: 'Bob' }),
      ].join('\n')
      const filePath = await writeRaw('corrupted.jsonl', content)

      const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})

      const result = await replay(filePath)

      expect(result).toHaveLength(2)
      expect(result[0]).toEqual({ $id: '1', name: 'Alice' })
      expect(result[1]).toEqual({ $id: '2', name: 'Bob' })
      expect(warnSpy).toHaveBeenCalled()

      warnSpy.mockRestore()
    })

    it('preserves order: lines returned in file order', async () => {
      const data = Array.from({ length: 20 }, (_, i) => ({
        $id: String(i),
        seq: i,
      }))
      const filePath = await writeJsonl('ordered.jsonl', data)

      const result = await replay(filePath)

      expect(result).toHaveLength(20)
      for (let i = 0; i < 20; i++) {
        expect(result[i]).toEqual({ $id: String(i), seq: i })
      }
    })
  })

  // =============================================================================
  // replayInto() Tests
  // =============================================================================

  describe('replayInto(path, callback)', () => {
    it('calls callback for each parsed line and returns line count', async () => {
      const data = [
        { $id: '1', name: 'Alice' },
        { $id: '2', name: 'Bob' },
        { $id: '3', name: 'Charlie' },
      ]
      const filePath = await writeJsonl('data.jsonl', data)

      const collected: Record<string, unknown>[] = []
      const count = await replayInto(filePath, (line) => {
        collected.push(line as Record<string, unknown>)
      })

      expect(count).toBe(3)
      expect(collected).toHaveLength(3)
      expect(collected[0]).toEqual({ $id: '1', name: 'Alice' })
      expect(collected[1]).toEqual({ $id: '2', name: 'Bob' })
      expect(collected[2]).toEqual({ $id: '3', name: 'Charlie' })
    })

    it('returns 0 for missing file', async () => {
      const filePath = join(tempDir, 'nonexistent.jsonl')

      const collected: unknown[] = []
      const count = await replayInto(filePath, (line) => {
        collected.push(line)
      })

      expect(count).toBe(0)
      expect(collected).toHaveLength(0)
    })

    it('skips empty lines and corrupted lines', async () => {
      const content = [
        JSON.stringify({ $id: '1' }),
        '',
        'bad json',
        JSON.stringify({ $id: '2' }),
      ].join('\n')
      const filePath = await writeRaw('mixed.jsonl', content)

      const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})

      const collected: Record<string, unknown>[] = []
      const count = await replayInto(filePath, (line) => {
        collected.push(line as Record<string, unknown>)
      })

      expect(count).toBe(2)
      expect(collected).toHaveLength(2)
      expect(warnSpy).toHaveBeenCalled()

      warnSpy.mockRestore()
    })
  })

  // =============================================================================
  // replayRange() Tests
  // =============================================================================

  describe('replayRange(path, fromTs, toTs)', () => {
    it('filters DataLine by $ts field within range', async () => {
      const data = [
        { $id: '1', name: 'Alice', $ts: 1000 },
        { $id: '2', name: 'Bob', $ts: 2000 },
        { $id: '3', name: 'Charlie', $ts: 3000 },
        { $id: '4', name: 'Diana', $ts: 4000 },
      ]
      const filePath = await writeJsonl('data.jsonl', data)

      const result = await replayRange(filePath, 1500, 3500)

      expect(result).toHaveLength(2)
      expect(result[0]).toEqual({ $id: '2', name: 'Bob', $ts: 2000 })
      expect(result[1]).toEqual({ $id: '3', name: 'Charlie', $ts: 3000 })
    })

    it('filters EventLine by ts field within range', async () => {
      const data = [
        { op: 'CREATE', target: 'posts:1', ts: 1000 },
        { op: 'UPDATE', target: 'posts:1', ts: 2000 },
        { op: 'DELETE', target: 'posts:1', ts: 3000 },
      ]
      const filePath = await writeJsonl('events.jsonl', data)

      const result = await replayRange(filePath, 1500, 2500)

      expect(result).toHaveLength(1)
      expect(result[0]).toEqual({ op: 'UPDATE', target: 'posts:1', ts: 2000 })
    })

    it('includes lines without a timestamp field (no filtering)', async () => {
      const data = [
        { $id: '1', $ts: 1000 },
        { $id: '2' }, // no timestamp field
        { $id: '3', $ts: 3000 },
      ]
      const filePath = await writeJsonl('mixed.jsonl', data)

      const result = await replayRange(filePath, 1500, 3500)

      // $id: '1' is out of range ($ts: 1000 < 1500)
      // $id: '2' has no timestamp, so it is included
      // $id: '3' is in range ($ts: 3000)
      expect(result).toHaveLength(2)
      expect(result[0]).toEqual({ $id: '2' })
      expect(result[1]).toEqual({ $id: '3', $ts: 3000 })
    })

    it('returns empty array for missing file', async () => {
      const filePath = join(tempDir, 'nonexistent.jsonl')

      const result = await replayRange(filePath, 0, Infinity)

      expect(result).toEqual([])
    })

    it('includes boundaries (inclusive range)', async () => {
      const data = [
        { $id: '1', $ts: 1000 },
        { $id: '2', $ts: 2000 },
        { $id: '3', $ts: 3000 },
      ]
      const filePath = await writeJsonl('bounds.jsonl', data)

      const result = await replayRange(filePath, 1000, 3000)

      expect(result).toHaveLength(3)
    })
  })

  // =============================================================================
  // lineCount() Tests
  // =============================================================================

  describe('lineCount(path)', () => {
    it('returns number of non-empty lines without full JSON parse', async () => {
      const data = [
        { $id: '1', name: 'Alice' },
        { $id: '2', name: 'Bob' },
        { $id: '3', name: 'Charlie' },
      ]
      const filePath = await writeJsonl('data.jsonl', data)

      const count = await lineCount(filePath)

      expect(count).toBe(3)
    })

    it('returns 0 for missing file', async () => {
      const filePath = join(tempDir, 'nonexistent.jsonl')

      const count = await lineCount(filePath)

      expect(count).toBe(0)
    })

    it('returns 0 for empty file', async () => {
      const filePath = await writeRaw('empty.jsonl', '')

      const count = await lineCount(filePath)

      expect(count).toBe(0)
    })

    it('does not count empty lines', async () => {
      const content = [
        JSON.stringify({ $id: '1' }),
        '',
        '',
        JSON.stringify({ $id: '2' }),
        '',
      ].join('\n')
      const filePath = await writeRaw('sparse.jsonl', content)

      const count = await lineCount(filePath)

      expect(count).toBe(2)
    })
  })

  // =============================================================================
  // Edge Cases
  // =============================================================================

  describe('edge cases', () => {
    it('handles file with 10,000 lines correctly', async () => {
      const data = Array.from({ length: 10_000 }, (_, i) => ({
        $id: String(i),
        value: i,
        $ts: 1000 + i,
      }))
      const filePath = await writeJsonl('large.jsonl', data)

      const result = await replay(filePath)

      expect(result).toHaveLength(10_000)
      expect(result[0]).toEqual({ $id: '0', value: 0, $ts: 1000 })
      expect(result[9_999]).toEqual({ $id: '9999', value: 9999, $ts: 10999 })
    })

    it('handles unicode content in JSON values', async () => {
      const data = [
        { $id: '1', name: '\u00e9\u00e0\u00fc\u00f1', emoji: '\u2764\ufe0f\ud83d\ude80\ud83c\udf0d' },
        { $id: '2', name: '\u4f60\u597d\u4e16\u754c', japanese: '\u3053\u3093\u306b\u3061\u306f' },
        { $id: '3', name: '\u041f\u0440\u0438\u0432\u0435\u0442', arabic: '\u0645\u0631\u062d\u0628\u0627' },
      ]
      const filePath = await writeJsonl('unicode.jsonl', data)

      const result = await replay(filePath)

      expect(result).toHaveLength(3)
      expect(result[0]).toEqual(data[0])
      expect(result[1]).toEqual(data[1])
      expect(result[2]).toEqual(data[2])
    })

    it('handles deeply nested JSON objects', async () => {
      const nested = {
        $id: '1',
        level1: {
          level2: {
            level3: {
              level4: {
                level5: {
                  value: 'deep',
                  array: [1, [2, [3, [4, [5]]]]],
                },
              },
            },
          },
        },
      }
      const filePath = await writeJsonl('nested.jsonl', [nested])

      const result = await replay(filePath)

      expect(result).toHaveLength(1)
      expect(result[0]).toEqual(nested)
    })
  })
})
