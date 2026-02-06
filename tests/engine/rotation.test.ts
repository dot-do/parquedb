/**
 * JSONL File Rotation Test Suite
 *
 * Tests for atomic JSONL file rotation used by the MergeTree engine
 * during compaction. The rotation sequence:
 *   1. Rename table.jsonl -> table.jsonl.compacting
 *   2. Create fresh empty table.jsonl
 *   3. Compaction reads from table.jsonl.compacting
 *   4. After compaction, delete table.jsonl.compacting
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtemp, writeFile, readFile, rm, stat, access } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { rotate, cleanup, needsRecovery, getCompactingPath } from '@/engine/rotation'

// =============================================================================
// Test Fixtures
// =============================================================================

let tempDir: string

beforeEach(async () => {
  tempDir = await mkdtemp(join(tmpdir(), 'rotation-test-'))
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

/** Helper to check if a file exists */
async function fileExists(filePath: string): Promise<boolean> {
  try {
    await access(filePath)
    return true
  } catch {
    return false
  }
}

// =============================================================================
// rotate() Tests
// =============================================================================

describe('JSONL File Rotation', () => {
  describe('rotate(basePath)', () => {
    it('renames file.jsonl to file.jsonl.compacting', async () => {
      const basePath = await writeJsonl('table.jsonl', [
        { $id: '1', name: 'Alice' },
        { $id: '2', name: 'Bob' },
      ])

      await rotate(basePath)

      const compactingExists = await fileExists(basePath + '.compacting')
      expect(compactingExists).toBe(true)
    })

    it('creates a fresh empty file.jsonl after rename', async () => {
      const basePath = await writeJsonl('table.jsonl', [
        { $id: '1', name: 'Alice' },
      ])

      await rotate(basePath)

      const baseExists = await fileExists(basePath)
      expect(baseExists).toBe(true)
    })

    it('new file.jsonl is empty (0 bytes)', async () => {
      const basePath = await writeJsonl('table.jsonl', [
        { $id: '1', name: 'Alice' },
        { $id: '2', name: 'Bob' },
      ])

      await rotate(basePath)

      const info = await stat(basePath)
      expect(info.size).toBe(0)
    })

    it('file.jsonl.compacting contains the original content', async () => {
      const lines = [
        { $id: '1', name: 'Alice' },
        { $id: '2', name: 'Bob' },
        { $id: '3', name: 'Charlie' },
      ]
      const basePath = await writeJsonl('table.jsonl', lines)
      const originalContent = await readFile(basePath, 'utf-8')

      await rotate(basePath)

      const compactingContent = await readFile(basePath + '.compacting', 'utf-8')
      expect(compactingContent).toBe(originalContent)
    })

    it('returns the path to the .compacting file', async () => {
      const basePath = await writeJsonl('table.jsonl', [
        { $id: '1', name: 'Alice' },
      ])

      const result = await rotate(basePath)

      expect(result).toBe(basePath + '.compacting')
    })

    it('returns null if file.jsonl does not exist', async () => {
      const basePath = join(tempDir, 'nonexistent.jsonl')

      const result = await rotate(basePath)

      expect(result).toBeNull()
    })

    it('returns null if file.jsonl.compacting already exists (compaction in progress)', async () => {
      const basePath = await writeJsonl('table.jsonl', [
        { $id: '1', name: 'Alice' },
      ])
      // Simulate an in-progress compaction
      await writeFile(basePath + '.compacting', 'old compaction data', 'utf-8')

      const result = await rotate(basePath)

      expect(result).toBeNull()
    })
  })

  // =============================================================================
  // cleanup() Tests
  // =============================================================================

  describe('cleanup(compactingPath)', () => {
    it('deletes the .compacting file', async () => {
      const compactingPath = join(tempDir, 'table.jsonl.compacting')
      await writeFile(compactingPath, 'compaction data', 'utf-8')

      await cleanup(compactingPath)

      const exists = await fileExists(compactingPath)
      expect(exists).toBe(false)
    })

    it('is a no-op on non-existent file (no error)', async () => {
      const compactingPath = join(tempDir, 'nonexistent.jsonl.compacting')

      // Should not throw
      await expect(cleanup(compactingPath)).resolves.toBeUndefined()
    })
  })

  // =============================================================================
  // Recovery Tests
  // =============================================================================

  describe('needsRecovery(basePath)', () => {
    it('returns true if .compacting file exists', async () => {
      const basePath = join(tempDir, 'table.jsonl')
      await writeFile(basePath + '.compacting', 'leftover data', 'utf-8')

      const result = await needsRecovery(basePath)

      expect(result).toBe(true)
    })

    it('returns false if no .compacting file', async () => {
      const basePath = join(tempDir, 'table.jsonl')

      const result = await needsRecovery(basePath)

      expect(result).toBe(false)
    })
  })

  describe('getCompactingPath(basePath)', () => {
    it('returns basePath + .compacting', () => {
      const basePath = '/data/tables/users.jsonl'

      const result = getCompactingPath(basePath)

      expect(result).toBe('/data/tables/users.jsonl.compacting')
    })
  })

  // =============================================================================
  // Edge Cases
  // =============================================================================

  describe('edge cases', () => {
    it('rotates an empty file successfully', async () => {
      const basePath = join(tempDir, 'empty.jsonl')
      await writeFile(basePath, '', 'utf-8')

      const result = await rotate(basePath)

      expect(result).toBe(basePath + '.compacting')
      const compactingContent = await readFile(basePath + '.compacting', 'utf-8')
      expect(compactingContent).toBe('')
      const freshInfo = await stat(basePath)
      expect(freshInfo.size).toBe(0)
    })

    it('rotates a large file (10K lines): content preserved in .compacting', async () => {
      const lines = Array.from({ length: 10_000 }, (_, i) => ({
        $id: String(i),
        value: i,
        $ts: 1000 + i,
      }))
      const basePath = await writeJsonl('large.jsonl', lines)
      const originalContent = await readFile(basePath, 'utf-8')

      const result = await rotate(basePath)

      expect(result).toBe(basePath + '.compacting')
      const compactingContent = await readFile(basePath + '.compacting', 'utf-8')
      expect(compactingContent).toBe(originalContent)
      // Verify line count preserved
      const compactingLines = compactingContent.split('\n').filter(l => l.trim() !== '')
      expect(compactingLines).toHaveLength(10_000)
    })

    it('after rotate + cleanup, only fresh .jsonl remains', async () => {
      const basePath = await writeJsonl('table.jsonl', [
        { $id: '1', name: 'Alice' },
        { $id: '2', name: 'Bob' },
      ])

      const compactingPath = await rotate(basePath)
      expect(compactingPath).not.toBeNull()

      await cleanup(compactingPath!)

      const baseExists = await fileExists(basePath)
      const compactingExists = await fileExists(basePath + '.compacting')
      expect(baseExists).toBe(true)
      expect(compactingExists).toBe(false)

      // Fresh file should be empty
      const info = await stat(basePath)
      expect(info.size).toBe(0)
    })
  })
})
