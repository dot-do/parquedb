/**
 * Relationship Compaction Test Suite
 *
 * Tests for compacting rels.jsonl into rels.parquet (JSON stand-in for tests).
 * The compactor merges relationship mutations from JSONL with existing relationships
 * using ReplacingMergeTree semantics:
 *
 * - Dedup key is composite `f:p:t` (from + predicate + to)
 * - Links ($op='l') add/replace relationships
 * - Unlinks ($op='u') are tombstones that remove relationships
 * - Output is sorted by (f, p, t) for deterministic results
 * - Atomic file operations: rotate -> compact -> rename -> cleanup
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtemp, writeFile, readFile, rm, stat, access } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import {
  compactRelationships,
  shouldCompact,
  type RelStorageAdapter,
} from '@/engine/compactor-rels'
import type { RelLine } from '@/engine/types'

// =============================================================================
// Storage adapter for testing (JSON files instead of Parquet)
// =============================================================================

async function readRels(path: string): Promise<RelLine[]> {
  try {
    return JSON.parse(await readFile(path, 'utf-8'))
  } catch {
    return []
  }
}

async function writeRels(path: string, data: RelLine[]): Promise<void> {
  await writeFile(path, JSON.stringify(data))
}

const testStorage: RelStorageAdapter = {
  readRels,
  writeRels,
}

// =============================================================================
// Test Fixtures
// =============================================================================

let tempDir: string

beforeEach(async () => {
  tempDir = await mkdtemp(join(tmpdir(), 'compactor-rels-test-'))
})

afterEach(async () => {
  await rm(tempDir, { recursive: true, force: true })
})

/** Helper to write a JSONL file from an array of objects */
async function writeJsonl(filename: string, lines: RelLine[]): Promise<string> {
  const filePath = join(tempDir, filename)
  const content = lines.map(line => JSON.stringify(line)).join('\n')
  await writeFile(filePath, content, 'utf-8')
  return filePath
}

/** Helper to write a JSON "parquet" file */
async function writeParquet(filename: string, data: RelLine[]): Promise<string> {
  const filePath = join(tempDir, filename)
  await writeFile(filePath, JSON.stringify(data), 'utf-8')
  return filePath
}

/** Helper to check if a file exists */
async function fileExists(filePath: string): Promise<boolean> {
  try {
    await access(filePath)
    return true
  } catch {
    return false
  }
}

/** Helper to make a link RelLine */
function makeLink(f: string, p: string, r: string, t: string, ts = 1000): RelLine {
  return { $op: 'l', $ts: ts, f, p, r, t }
}

/** Helper to make an unlink RelLine */
function makeUnlink(f: string, p: string, r: string, t: string, ts = 2000): RelLine {
  return { $op: 'u', $ts: ts, f, p, r, t }
}

// =============================================================================
// First compaction (no existing parquet file)
// =============================================================================

describe('compactRelationships', () => {
  describe('first compaction (no existing parquet)', () => {
    it('1. 3 links in JSONL, no existing file -> output has 3 relationships', async () => {
      await writeJsonl('rels.jsonl', [
        makeLink('user1', 'posts', 'author', 'post1'),
        makeLink('user1', 'posts', 'author', 'post2'),
        makeLink('user2', 'posts', 'author', 'post3'),
      ])

      const count = await compactRelationships(tempDir, testStorage)

      expect(count).toBe(3)
      const output = await readRels(join(tempDir, 'rels.parquet'))
      expect(output).toHaveLength(3)
      // All should be links
      expect(output.every(r => r.$op === 'l')).toBe(true)
    })

    it('2. output sorted by composite key (f, p, t)', async () => {
      await writeJsonl('rels.jsonl', [
        makeLink('z-user', 'posts', 'author', 'post1'),
        makeLink('a-user', 'posts', 'author', 'post1'),
        makeLink('m-user', 'follows', 'followers', 'a-user'),
      ])

      await compactRelationships(tempDir, testStorage)

      const output = await readRels(join(tempDir, 'rels.parquet'))
      expect(output).toHaveLength(3)
      // Should be sorted by (f, p, t)
      expect(output[0].f).toBe('a-user')
      expect(output[1].f).toBe('m-user')
      expect(output[2].f).toBe('z-user')
    })

    it('3. cleanup: .compacting file removed after compaction', async () => {
      await writeJsonl('rels.jsonl', [
        makeLink('user1', 'posts', 'author', 'post1'),
      ])

      await compactRelationships(tempDir, testStorage)

      const compactingExists = await fileExists(join(tempDir, 'rels.jsonl.compacting'))
      expect(compactingExists).toBe(false)
    })
  })

  // ===========================================================================
  // Merge with existing parquet
  // ===========================================================================

  describe('merge with existing parquet', () => {
    it('4. 3 rels in existing + 2 new links in JSONL -> 5 relationships', async () => {
      await writeParquet('rels.parquet', [
        makeLink('user1', 'posts', 'author', 'post1', 500),
        makeLink('user1', 'posts', 'author', 'post2', 500),
        makeLink('user2', 'posts', 'author', 'post3', 500),
      ])
      await writeJsonl('rels.jsonl', [
        makeLink('user3', 'posts', 'author', 'post4', 1000),
        makeLink('user4', 'follows', 'followers', 'user1', 1000),
      ])

      const count = await compactRelationships(tempDir, testStorage)

      expect(count).toBe(5)
      const output = await readRels(join(tempDir, 'rels.parquet'))
      expect(output).toHaveLength(5)
    })

    it('5. existing rel + updated link (same f:p:t, newer $ts) -> uses JSONL version', async () => {
      await writeParquet('rels.parquet', [
        makeLink('user1', 'posts', 'author', 'post1', 500),
      ])
      await writeJsonl('rels.jsonl', [
        makeLink('user1', 'posts', 'writtenBy', 'post1', 1000), // same f:p:t but different r, newer $ts
      ])

      const count = await compactRelationships(tempDir, testStorage)

      expect(count).toBe(1)
      const output = await readRels(join(tempDir, 'rels.parquet'))
      expect(output).toHaveLength(1)
      // JSONL version wins (newer $ts)
      expect(output[0].r).toBe('writtenBy')
      expect(output[0].$ts).toBe(1000)
    })
  })

  // ===========================================================================
  // Unlink (tombstone) handling
  // ===========================================================================

  describe('unlink (tombstone) handling', () => {
    it('6. existing link + unlink in JSONL -> relationship removed from output', async () => {
      await writeParquet('rels.parquet', [
        makeLink('user1', 'posts', 'author', 'post1', 500),
        makeLink('user1', 'posts', 'author', 'post2', 500),
      ])
      await writeJsonl('rels.jsonl', [
        makeUnlink('user1', 'posts', 'author', 'post1', 1000),
      ])

      const count = await compactRelationships(tempDir, testStorage)

      expect(count).toBe(1)
      const output = await readRels(join(tempDir, 'rels.parquet'))
      expect(output).toHaveLength(1)
      expect(output[0].t).toBe('post2')
    })

    it('7. multiple unlinks: all corresponding links removed', async () => {
      await writeParquet('rels.parquet', [
        makeLink('user1', 'posts', 'author', 'post1', 500),
        makeLink('user1', 'posts', 'author', 'post2', 500),
        makeLink('user1', 'posts', 'author', 'post3', 500),
      ])
      await writeJsonl('rels.jsonl', [
        makeUnlink('user1', 'posts', 'author', 'post1', 1000),
        makeUnlink('user1', 'posts', 'author', 'post3', 1000),
      ])

      const count = await compactRelationships(tempDir, testStorage)

      expect(count).toBe(1)
      const output = await readRels(join(tempDir, 'rels.parquet'))
      expect(output).toHaveLength(1)
      expect(output[0].t).toBe('post2')
    })

    it('8. unlink for non-existent relationship: ignored (not in output)', async () => {
      await writeParquet('rels.parquet', [
        makeLink('user1', 'posts', 'author', 'post1', 500),
      ])
      await writeJsonl('rels.jsonl', [
        makeUnlink('user99', 'posts', 'author', 'post99', 1000),
      ])

      const count = await compactRelationships(tempDir, testStorage)

      expect(count).toBe(1)
      const output = await readRels(join(tempDir, 'rels.parquet'))
      expect(output).toHaveLength(1)
      expect(output[0].f).toBe('user1')
    })
  })

  // ===========================================================================
  // Edge cases
  // ===========================================================================

  describe('edge cases', () => {
    it('9. empty JSONL -> returns null (nothing to compact)', async () => {
      // No rels.jsonl file at all
      const count = await compactRelationships(tempDir, testStorage)

      expect(count).toBeNull()
    })

    it('10. link then unlink same relationship in JSONL -> not in output', async () => {
      await writeJsonl('rels.jsonl', [
        makeLink('user1', 'posts', 'author', 'post1', 1000),
        makeUnlink('user1', 'posts', 'author', 'post1', 2000),
      ])

      const count = await compactRelationships(tempDir, testStorage)

      expect(count).toBe(0)
      const output = await readRels(join(tempDir, 'rels.parquet'))
      expect(output).toHaveLength(0)
    })

    it('11. different predicates between same entities are independent', async () => {
      await writeJsonl('rels.jsonl', [
        makeLink('user1', 'posts', 'author', 'post1', 1000),
        makeLink('user1', 'likes', 'likedBy', 'post1', 1000),
        makeUnlink('user1', 'posts', 'author', 'post1', 2000), // unlink only 'posts'
      ])

      const count = await compactRelationships(tempDir, testStorage)

      expect(count).toBe(1)
      const output = await readRels(join(tempDir, 'rels.parquet'))
      expect(output).toHaveLength(1)
      expect(output[0].p).toBe('likes')
      expect(output[0].r).toBe('likedBy')
    })

    it('12. large: 500 existing + 50 new links -> correct merge', async () => {
      const existing: RelLine[] = []
      for (let i = 0; i < 500; i++) {
        existing.push(makeLink(`user${i}`, 'posts', 'author', `post${i}`, 500))
      }
      await writeParquet('rels.parquet', existing)

      const newLinks: RelLine[] = []
      for (let i = 500; i < 550; i++) {
        newLinks.push(makeLink(`user${i}`, 'posts', 'author', `post${i}`, 1000))
      }
      await writeJsonl('rels.jsonl', newLinks)

      const count = await compactRelationships(tempDir, testStorage)

      expect(count).toBe(550)
      const output = await readRels(join(tempDir, 'rels.parquet'))
      expect(output).toHaveLength(550)
    })
  })

  // ===========================================================================
  // shouldCompact
  // ===========================================================================

  describe('shouldCompact', () => {
    it('13. returns true when JSONL exceeds line threshold', async () => {
      // Create a JSONL file with many lines
      const lines: RelLine[] = []
      for (let i = 0; i < 100; i++) {
        lines.push(makeLink(`user${i}`, 'posts', 'author', `post${i}`))
      }
      await writeJsonl('rels.jsonl', lines)

      const result = await shouldCompact(join(tempDir, 'rels.jsonl'), { lineThreshold: 50 })

      expect(result).toBe(true)
    })

    it('14. returns false when JSONL is below threshold', async () => {
      const lines: RelLine[] = [
        makeLink('user1', 'posts', 'author', 'post1'),
      ]
      await writeJsonl('rels.jsonl', lines)

      const result = await shouldCompact(join(tempDir, 'rels.jsonl'), { lineThreshold: 50 })

      expect(result).toBe(false)
    })

    it('15. returns false when JSONL does not exist', async () => {
      const result = await shouldCompact(join(tempDir, 'rels.jsonl'), { lineThreshold: 50 })

      expect(result).toBe(false)
    })

    it('16. returns true when JSONL exceeds byte threshold', async () => {
      const lines: RelLine[] = []
      for (let i = 0; i < 50; i++) {
        lines.push(makeLink(`user${i}`, 'posts', 'author', `post${i}`))
      }
      await writeJsonl('rels.jsonl', lines)

      // Get the actual file size and set threshold below it
      const info = await stat(join(tempDir, 'rels.jsonl'))
      const result = await shouldCompact(join(tempDir, 'rels.jsonl'), {
        lineThreshold: 10000,  // very high line threshold
        byteThreshold: Math.floor(info.size / 2), // byte threshold below actual size
      })

      expect(result).toBe(true)
    })
  })
})
