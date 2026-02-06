/**
 * Data Table Compactor Test Suite
 *
 * Tests the compaction process that merges JSONL buffer files into Parquet
 * (or JSON for testing) data files using ReplacingMergeTree semantics:
 *
 * 1. Rotate the JSONL file (table.jsonl -> table.jsonl.compacting)
 * 2. Read existing table.parquet (if any)
 * 3. Read table.jsonl.compacting
 * 4. Merge: deduplicate by $id, latest $v wins, tombstones remove entities
 * 5. Write new table.parquet.tmp sorted by $id
 * 6. Atomic swap: rename .tmp -> .parquet
 * 7. Delete .jsonl.compacting
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { mkdtemp, writeFile, readFile, rm, stat, access } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { compactDataTable, shouldCompact } from '@/engine/compactor'
import type { StorageAdapter, CompactOptions } from '@/engine/compactor'
import type { DataLine } from '@/engine/types'

// =============================================================================
// Mock the rotation module (being built in parallel)
// =============================================================================

vi.mock('@/engine/rotation', () => ({
  rotate: vi.fn(),
  cleanup: vi.fn(),
  getCompactingPath: vi.fn((basePath: string) => basePath + '.compacting'),
}))

import { rotate, cleanup, getCompactingPath } from '@/engine/rotation'

// =============================================================================
// Test Helpers
// =============================================================================

let tempDir: string

beforeEach(async () => {
  tempDir = await mkdtemp(join(tmpdir(), 'compactor-data-test-'))
  vi.clearAllMocks()
})

afterEach(async () => {
  await rm(tempDir, { recursive: true, force: true })
})

/** Helper to create a DataLine with sensible defaults */
function makeLine(overrides: Partial<DataLine> & { $id: string }): DataLine {
  return {
    $op: 'c',
    $v: 1,
    $ts: Date.now(),
    ...overrides,
  }
}

/** Simple JSON-based read/write for testing (stand-in for Parquet) */
async function readJsonFile(path: string): Promise<DataLine[]> {
  try {
    const content = await readFile(path, 'utf-8')
    return JSON.parse(content)
  } catch {
    return []
  }
}

async function writeJsonFile(path: string, data: DataLine[]): Promise<void> {
  await writeFile(path, JSON.stringify(data))
}

/** Create a StorageAdapter backed by JSON files */
function createJsonStorageAdapter(): StorageAdapter {
  return {
    readData: readJsonFile,
    writeData: writeJsonFile,
  }
}

/** Write JSONL lines to a file (simulates the buffer file) */
async function writeJsonlFile(path: string, lines: DataLine[]): Promise<void> {
  const content = lines.map(l => JSON.stringify(l)).join('\n') + '\n'
  await writeFile(path, content, 'utf-8')
}

/** Check if a file exists */
async function fileExists(path: string): Promise<boolean> {
  try {
    await access(path)
    return true
  } catch {
    return false
  }
}

// =============================================================================
// Tests
// =============================================================================

describe('compactDataTable', () => {
  const storage = createJsonStorageAdapter()

  // ===========================================================================
  // First compaction (no existing Parquet)
  // ===========================================================================
  describe('first compaction (no existing data file)', () => {
    it('1. compacts 3 entities in JSONL into a new output file', async () => {
      const jsonlPath = join(tempDir, 'users.jsonl')
      const compactingPath = jsonlPath + '.compacting'
      const dataPath = join(tempDir, 'users.parquet')

      const entities = [
        makeLine({ $id: 'u1', name: 'Alice' }),
        makeLine({ $id: 'u2', name: 'Bob' }),
        makeLine({ $id: 'u3', name: 'Charlie' }),
      ]

      // Write the compacting file (rotation already happened)
      await writeJsonlFile(compactingPath, entities)

      // Mock rotation: return the compacting path
      vi.mocked(rotate).mockResolvedValue(compactingPath)
      vi.mocked(cleanup).mockResolvedValue(undefined)
      vi.mocked(getCompactingPath).mockReturnValue(compactingPath)

      const count = await compactDataTable(tempDir, 'users', storage)

      expect(count).toBe(3)

      // Verify the output file was written
      const output = await readJsonFile(dataPath)
      expect(output).toHaveLength(3)
    })

    it('2. output is sorted by $id', async () => {
      const jsonlPath = join(tempDir, 'users.jsonl')
      const compactingPath = jsonlPath + '.compacting'
      const dataPath = join(tempDir, 'users.parquet')

      const entities = [
        makeLine({ $id: 'charlie', name: 'Charlie' }),
        makeLine({ $id: 'alice', name: 'Alice' }),
        makeLine({ $id: 'bob', name: 'Bob' }),
      ]

      await writeJsonlFile(compactingPath, entities)

      vi.mocked(rotate).mockResolvedValue(compactingPath)
      vi.mocked(cleanup).mockResolvedValue(undefined)
      vi.mocked(getCompactingPath).mockReturnValue(compactingPath)

      await compactDataTable(tempDir, 'users', storage)

      const output = await readJsonFile(dataPath)
      expect(output.map(e => e.$id)).toEqual(['alice', 'bob', 'charlie'])
    })

    it('3. JSONL.compacting file is cleaned up after compaction', async () => {
      const jsonlPath = join(tempDir, 'users.jsonl')
      const compactingPath = jsonlPath + '.compacting'

      const entities = [makeLine({ $id: 'u1', name: 'Alice' })]
      await writeJsonlFile(compactingPath, entities)

      vi.mocked(rotate).mockResolvedValue(compactingPath)
      vi.mocked(cleanup).mockResolvedValue(undefined)
      vi.mocked(getCompactingPath).mockReturnValue(compactingPath)

      await compactDataTable(tempDir, 'users', storage)

      // Verify cleanup was called with the compacting path
      expect(cleanup).toHaveBeenCalledWith(compactingPath)
    })
  })

  // ===========================================================================
  // Compaction with existing data
  // ===========================================================================
  describe('compaction with existing data', () => {
    it('5. 3 entities in data file + 2 new entities in JSONL -> output has 5 entities', async () => {
      const jsonlPath = join(tempDir, 'users.jsonl')
      const compactingPath = jsonlPath + '.compacting'
      const dataPath = join(tempDir, 'users.parquet')

      // Write existing data file
      const existing = [
        makeLine({ $id: 'u1', name: 'Alice' }),
        makeLine({ $id: 'u2', name: 'Bob' }),
        makeLine({ $id: 'u3', name: 'Charlie' }),
      ]
      await writeJsonFile(dataPath, existing)

      // Write JSONL with new entities
      const newEntities = [
        makeLine({ $id: 'u4', name: 'Diana' }),
        makeLine({ $id: 'u5', name: 'Eve' }),
      ]
      await writeJsonlFile(compactingPath, newEntities)

      vi.mocked(rotate).mockResolvedValue(compactingPath)
      vi.mocked(cleanup).mockResolvedValue(undefined)
      vi.mocked(getCompactingPath).mockReturnValue(compactingPath)

      const count = await compactDataTable(tempDir, 'users', storage)

      expect(count).toBe(5)
      const output = await readJsonFile(dataPath)
      expect(output).toHaveLength(5)
    })

    it('6. 3 entities in data file + 1 update in JSONL -> output has 3 entities', async () => {
      const jsonlPath = join(tempDir, 'users.jsonl')
      const compactingPath = jsonlPath + '.compacting'
      const dataPath = join(tempDir, 'users.parquet')

      const existing = [
        makeLine({ $id: 'u1', $v: 1, name: 'Alice' }),
        makeLine({ $id: 'u2', $v: 1, name: 'Bob' }),
        makeLine({ $id: 'u3', $v: 1, name: 'Charlie' }),
      ]
      await writeJsonFile(dataPath, existing)

      const updates = [
        makeLine({ $id: 'u2', $v: 2, $op: 'u', name: 'Bob Updated' }),
      ]
      await writeJsonlFile(compactingPath, updates)

      vi.mocked(rotate).mockResolvedValue(compactingPath)
      vi.mocked(cleanup).mockResolvedValue(undefined)
      vi.mocked(getCompactingPath).mockReturnValue(compactingPath)

      const count = await compactDataTable(tempDir, 'users', storage)

      expect(count).toBe(3)
      const output = await readJsonFile(dataPath)
      expect(output).toHaveLength(3)
    })

    it('7. updated entity has the JSONL version (higher $v)', async () => {
      const jsonlPath = join(tempDir, 'users.jsonl')
      const compactingPath = jsonlPath + '.compacting'
      const dataPath = join(tempDir, 'users.parquet')

      const existing = [
        makeLine({ $id: 'u1', $v: 1, name: 'Alice' }),
        makeLine({ $id: 'u2', $v: 1, name: 'Bob' }),
      ]
      await writeJsonFile(dataPath, existing)

      const updates = [
        makeLine({ $id: 'u1', $v: 2, $op: 'u', name: 'Alice Updated' }),
      ]
      await writeJsonlFile(compactingPath, updates)

      vi.mocked(rotate).mockResolvedValue(compactingPath)
      vi.mocked(cleanup).mockResolvedValue(undefined)
      vi.mocked(getCompactingPath).mockReturnValue(compactingPath)

      await compactDataTable(tempDir, 'users', storage)

      const output = await readJsonFile(dataPath)
      const alice = output.find(e => e.$id === 'u1')!
      expect(alice.name).toBe('Alice Updated')
      expect(alice.$v).toBe(2)
    })
  })

  // ===========================================================================
  // Tombstone handling
  // ===========================================================================
  describe('tombstone handling', () => {
    it('8. entity in data file + delete in JSONL -> entity removed from output', async () => {
      const jsonlPath = join(tempDir, 'users.jsonl')
      const compactingPath = jsonlPath + '.compacting'
      const dataPath = join(tempDir, 'users.parquet')

      const existing = [
        makeLine({ $id: 'u1', $v: 1, name: 'Alice' }),
        makeLine({ $id: 'u2', $v: 1, name: 'Bob' }),
      ]
      await writeJsonFile(dataPath, existing)

      const deletes = [
        makeLine({ $id: 'u1', $v: 2, $op: 'd' }),
      ]
      await writeJsonlFile(compactingPath, deletes)

      vi.mocked(rotate).mockResolvedValue(compactingPath)
      vi.mocked(cleanup).mockResolvedValue(undefined)
      vi.mocked(getCompactingPath).mockReturnValue(compactingPath)

      const count = await compactDataTable(tempDir, 'users', storage)

      expect(count).toBe(1)
      const output = await readJsonFile(dataPath)
      expect(output).toHaveLength(1)
      expect(output[0].$id).toBe('u2')
    })

    it('9. multiple deletes: only non-deleted entities remain', async () => {
      const jsonlPath = join(tempDir, 'users.jsonl')
      const compactingPath = jsonlPath + '.compacting'
      const dataPath = join(tempDir, 'users.parquet')

      const existing = [
        makeLine({ $id: 'u1', $v: 1, name: 'Alice' }),
        makeLine({ $id: 'u2', $v: 1, name: 'Bob' }),
        makeLine({ $id: 'u3', $v: 1, name: 'Charlie' }),
        makeLine({ $id: 'u4', $v: 1, name: 'Diana' }),
      ]
      await writeJsonFile(dataPath, existing)

      const deletes = [
        makeLine({ $id: 'u1', $v: 2, $op: 'd' }),
        makeLine({ $id: 'u3', $v: 2, $op: 'd' }),
      ]
      await writeJsonlFile(compactingPath, deletes)

      vi.mocked(rotate).mockResolvedValue(compactingPath)
      vi.mocked(cleanup).mockResolvedValue(undefined)
      vi.mocked(getCompactingPath).mockReturnValue(compactingPath)

      const count = await compactDataTable(tempDir, 'users', storage)

      expect(count).toBe(2)
      const output = await readJsonFile(dataPath)
      expect(output).toHaveLength(2)
      expect(output.map(e => e.$id)).toEqual(['u2', 'u4'])
    })
  })

  // ===========================================================================
  // Edge cases
  // ===========================================================================
  describe('edge cases', () => {
    it('10. empty JSONL (nothing to compact) -> rotation returns null, no compaction needed', async () => {
      // rotation returns null when there is nothing to rotate
      vi.mocked(rotate).mockResolvedValue(null)
      vi.mocked(getCompactingPath).mockReturnValue(join(tempDir, 'users.jsonl.compacting'))

      const count = await compactDataTable(tempDir, 'users', storage)

      expect(count).toBeNull()
      // cleanup should NOT be called when rotation returns null
      expect(cleanup).not.toHaveBeenCalled()
    })

    it('11. large compaction: 1000 entities in data file + 100 new in JSONL -> correct merge', async () => {
      const jsonlPath = join(tempDir, 'users.jsonl')
      const compactingPath = jsonlPath + '.compacting'
      const dataPath = join(tempDir, 'users.parquet')

      const existing: DataLine[] = []
      for (let i = 0; i < 1000; i++) {
        existing.push(makeLine({
          $id: `u-${String(i).padStart(4, '0')}`,
          $v: 1,
          name: `User-${i}`,
        }))
      }
      await writeJsonFile(dataPath, existing)

      const newEntities: DataLine[] = []
      for (let i = 0; i < 100; i++) {
        newEntities.push(makeLine({
          $id: `n-${String(i).padStart(4, '0')}`,
          $v: 1,
          name: `New-${i}`,
        }))
      }
      await writeJsonlFile(compactingPath, newEntities)

      vi.mocked(rotate).mockResolvedValue(compactingPath)
      vi.mocked(cleanup).mockResolvedValue(undefined)
      vi.mocked(getCompactingPath).mockReturnValue(compactingPath)

      const count = await compactDataTable(tempDir, 'users', storage)

      expect(count).toBe(1100)
      const output = await readJsonFile(dataPath)
      expect(output).toHaveLength(1100)
    })

    it('12. entity created and deleted in same JSONL -> not in output', async () => {
      const jsonlPath = join(tempDir, 'users.jsonl')
      const compactingPath = jsonlPath + '.compacting'
      const dataPath = join(tempDir, 'users.parquet')

      // No existing data file
      // JSONL has create then delete for same entity
      const lines = [
        makeLine({ $id: 'u1', $v: 1, $op: 'c', name: 'Alice' }),
        makeLine({ $id: 'u1', $v: 2, $op: 'd' }),
        makeLine({ $id: 'u2', $v: 1, $op: 'c', name: 'Bob' }),
      ]
      await writeJsonlFile(compactingPath, lines)

      vi.mocked(rotate).mockResolvedValue(compactingPath)
      vi.mocked(cleanup).mockResolvedValue(undefined)
      vi.mocked(getCompactingPath).mockReturnValue(compactingPath)

      const count = await compactDataTable(tempDir, 'users', storage)

      expect(count).toBe(1)
      const output = await readJsonFile(dataPath)
      expect(output).toHaveLength(1)
      expect(output[0].$id).toBe('u2')
    })
  })

  // ===========================================================================
  // Atomic swap
  // ===========================================================================
  describe('atomic swap', () => {
    it('13. output is written to .tmp first, then renamed (verify .tmp does not exist after)', async () => {
      const jsonlPath = join(tempDir, 'users.jsonl')
      const compactingPath = jsonlPath + '.compacting'
      const dataPath = join(tempDir, 'users.parquet')
      const tmpPath = dataPath + '.tmp'

      const entities = [
        makeLine({ $id: 'u1', name: 'Alice' }),
      ]
      await writeJsonlFile(compactingPath, entities)

      vi.mocked(rotate).mockResolvedValue(compactingPath)
      vi.mocked(cleanup).mockResolvedValue(undefined)
      vi.mocked(getCompactingPath).mockReturnValue(compactingPath)

      await compactDataTable(tempDir, 'users', storage)

      // .tmp file should NOT exist (it was renamed to .parquet)
      expect(await fileExists(tmpPath)).toBe(false)

      // .parquet file should exist
      expect(await fileExists(dataPath)).toBe(true)

      // Verify the data is accessible
      const output = await readJsonFile(dataPath)
      expect(output).toHaveLength(1)
      expect(output[0].$id).toBe('u1')
    })
  })
})

// =============================================================================
// shouldCompact
// =============================================================================

describe('shouldCompact', () => {
  it('14. returns true if JSONL exceeds byte threshold', async () => {
    const jsonlPath = join(tempDir, 'users.jsonl')

    // Write enough data to exceed 1024 bytes
    const lines: DataLine[] = []
    for (let i = 0; i < 50; i++) {
      lines.push(makeLine({ $id: `u-${i}`, name: `User with a somewhat long name ${i}` }))
    }
    await writeJsonlFile(jsonlPath, lines)

    const result = await shouldCompact(jsonlPath, { maxBytes: 1024 })
    expect(result).toBe(true)
  })

  it('15. returns false if JSONL is under byte threshold', async () => {
    const jsonlPath = join(tempDir, 'users.jsonl')

    // Write a small amount of data
    const lines = [makeLine({ $id: 'u1', name: 'Alice' })]
    await writeJsonlFile(jsonlPath, lines)

    const result = await shouldCompact(jsonlPath, { maxBytes: 1024 * 1024 })
    expect(result).toBe(false)
  })

  it('16. returns true if line count exceeds threshold', async () => {
    const jsonlPath = join(tempDir, 'users.jsonl')

    const lines: DataLine[] = []
    for (let i = 0; i < 150; i++) {
      lines.push(makeLine({ $id: `u-${i}`, name: `User-${i}` }))
    }
    await writeJsonlFile(jsonlPath, lines)

    const result = await shouldCompact(jsonlPath, { maxLines: 100 })
    expect(result).toBe(true)
  })

  it('returns false if line count is under threshold', async () => {
    const jsonlPath = join(tempDir, 'users.jsonl')

    const lines: DataLine[] = []
    for (let i = 0; i < 10; i++) {
      lines.push(makeLine({ $id: `u-${i}`, name: `User-${i}` }))
    }
    await writeJsonlFile(jsonlPath, lines)

    const result = await shouldCompact(jsonlPath, { maxLines: 100 })
    expect(result).toBe(false)
  })

  it('returns false if JSONL file does not exist', async () => {
    const jsonlPath = join(tempDir, 'nonexistent.jsonl')

    const result = await shouldCompact(jsonlPath, { maxBytes: 1024 })
    expect(result).toBe(false)
  })

  it('returns false if no thresholds are specified', async () => {
    const jsonlPath = join(tempDir, 'users.jsonl')
    const lines = [makeLine({ $id: 'u1', name: 'Alice' })]
    await writeJsonlFile(jsonlPath, lines)

    const result = await shouldCompact(jsonlPath, {})
    expect(result).toBe(false)
  })
})
