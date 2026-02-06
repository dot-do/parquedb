/**
 * Compaction Failure Recovery Test Suite
 *
 * Tests three recovery improvements for the MergeTree engine:
 *
 * 1. compactDataTable() in compactor.ts: Must have try/catch like
 *    compactor-rels.ts and compactor-events.ts, preserving the .compacting
 *    file on failure for recovery.
 *
 * 2. hybridCompactData() in storage-adapters.ts: Must use a temp file +
 *    rename for atomicity instead of writing directly to the final path,
 *    so partial writes don't corrupt the data file.
 *
 * 3. engine.init(): Must clean up orphaned .tmp files left behind by
 *    interrupted compactions.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { mkdtemp, writeFile, readFile, rm, access, readdir } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { compactDataTable } from '@/engine/compactor'
import type { StorageAdapter } from '@/engine/compactor'
import { hybridCompactData } from '@/engine/storage-adapters'
import { MemoryStorageAdapter } from '@/engine/storage-adapters'
import type { FullStorageAdapter } from '@/engine/storage-adapters'
import { ParqueEngine } from '@/engine/engine'
import type { DataLine } from '@/engine/types'

// =============================================================================
// Helpers
// =============================================================================

let tempDir: string

beforeEach(async () => {
  tempDir = await mkdtemp(join(tmpdir(), 'compaction-recovery-test-'))
})

afterEach(async () => {
  await rm(tempDir, { recursive: true, force: true })
})

/** Check if a file exists */
async function fileExists(path: string): Promise<boolean> {
  try {
    await access(path)
    return true
  } catch {
    return false
  }
}

/** Write JSONL lines to a file */
async function writeJsonlFile(path: string, lines: Record<string, unknown>[]): Promise<void> {
  const content = lines.map(l => JSON.stringify(l)).join('\n') + '\n'
  await writeFile(path, content, 'utf-8')
}

/** Helper to create a DataLine with defaults */
function makeLine(overrides: Partial<DataLine> & { $id: string }): DataLine {
  return {
    $op: 'c',
    $v: 1,
    $ts: Date.now(),
    ...overrides,
  }
}

// =============================================================================
// 1. compactDataTable() try/catch: preserves .compacting file on failure
// =============================================================================

describe('compactDataTable - try/catch failure recovery', () => {
  // Use real rotation for these tests (no mocking)
  // We simulate the scenario where rotation succeeds but a later step fails.

  it('preserves .compacting file when storage.readData() throws', async () => {
    const jsonlPath = join(tempDir, 'users.jsonl')

    // Write a real JSONL file so rotation can work
    await writeJsonlFile(jsonlPath, [
      makeLine({ $id: 'u1', name: 'Alice' }),
    ])

    const failingStorage: StorageAdapter = {
      readData: async () => {
        throw new Error('R2 GET failed: bucket unavailable')
      },
      writeData: async () => {},
    }

    // compactDataTable should throw (not swallow the error)
    await expect(
      compactDataTable(tempDir, 'users', failingStorage),
    ).rejects.toThrow('R2 GET failed: bucket unavailable')

    // The .compacting file should still exist for recovery
    const compactingPath = jsonlPath + '.compacting'
    expect(await fileExists(compactingPath)).toBe(true)
  })

  it('preserves .compacting file when storage.writeData() throws', async () => {
    const jsonlPath = join(tempDir, 'users.jsonl')

    // Write a real JSONL file so rotation can work
    await writeJsonlFile(jsonlPath, [
      makeLine({ $id: 'u1', name: 'Alice' }),
    ])

    const failingStorage: StorageAdapter = {
      readData: async () => [],
      writeData: async () => {
        throw new Error('Disk full')
      },
    }

    await expect(
      compactDataTable(tempDir, 'users', failingStorage),
    ).rejects.toThrow('Disk full')

    // The .compacting file should still exist for recovery
    const compactingPath = jsonlPath + '.compacting'
    expect(await fileExists(compactingPath)).toBe(true)
  })

  it('preserves .compacting file when rename() fails (after writeData)', async () => {
    const jsonlPath = join(tempDir, 'users.jsonl')

    // Write a real JSONL file
    await writeJsonlFile(jsonlPath, [
      makeLine({ $id: 'u1', name: 'Alice' }),
    ])

    // Track calls to verify writeData was called but we fail on rename
    let writeDataCalled = false

    const storage: StorageAdapter = {
      readData: async () => [],
      writeData: async () => {
        writeDataCalled = true
        // Write succeeds, but we'll simulate a rename failure by
        // not actually writing to the .tmp path (so rename will fail
        // because the path doesn't exist on disk for non-MemoryStorageAdapter).
        // Actually, compactDataTable uses its own rename, so this test
        // just validates the try/catch around the entire operation.
      },
    }

    // The writeData writes to .tmp, then rename .tmp -> .parquet
    // Since the StorageAdapter.writeData is a mock that doesn't actually
    // write to disk, the rename() call will fail with ENOENT
    await expect(
      compactDataTable(tempDir, 'users', storage),
    ).rejects.toThrow()

    expect(writeDataCalled).toBe(true)

    // The .compacting file should still exist for recovery
    const compactingPath = jsonlPath + '.compacting'
    expect(await fileExists(compactingPath)).toBe(true)
  })

  it('still cleans up .compacting file on success', async () => {
    const jsonlPath = join(tempDir, 'users.jsonl')
    const dataPath = join(tempDir, 'users.parquet')

    // Write a real JSONL file
    await writeJsonlFile(jsonlPath, [
      makeLine({ $id: 'u1', name: 'Alice' }),
    ])

    // Use a simple JSON-based storage adapter that actually writes to disk
    const storage: StorageAdapter = {
      readData: async (path: string) => {
        try {
          return JSON.parse(await readFile(path, 'utf-8'))
        } catch {
          return []
        }
      },
      writeData: async (path: string, data: DataLine[]) => {
        await writeFile(path, JSON.stringify(data))
      },
    }

    const count = await compactDataTable(tempDir, 'users', storage)

    expect(count).toBe(1)

    // The .compacting file should be cleaned up on success
    const compactingPath = jsonlPath + '.compacting'
    expect(await fileExists(compactingPath)).toBe(false)

    // Data file should exist
    expect(await fileExists(dataPath)).toBe(true)
  })
})

// =============================================================================
// 2. hybridCompactData() atomicity: uses tmp file + rename
// =============================================================================

describe('hybridCompactData - atomic write with tmp file', () => {
  it('writes to a .tmp path first, not directly to the final path', async () => {
    const jsonlPath = join(tempDir, 'users.jsonl')
    const dataPath = join(tempDir, 'users.parquet')
    const tmpPath = dataPath + '.tmp'

    // Write a JSONL file with data to compact
    await writeJsonlFile(jsonlPath, [
      makeLine({ $id: 'u1', name: 'Alice' }),
    ])

    // Track which paths are written to
    const writtenPaths: string[] = []

    const trackingStorage: FullStorageAdapter = {
      readData: async () => [],
      writeData: async (path: string, data: DataLine[]) => {
        writtenPaths.push(path)
        await writeFile(path, JSON.stringify(data))
      },
      readRels: async () => [],
      writeRels: async () => {},
      readEvents: async () => [],
      writeEvents: async () => {},
    }

    await hybridCompactData(tempDir, 'users', trackingStorage)

    // hybridCompactData should write to .tmp first for atomicity
    // Currently it writes directly to the final path -- this test should fail
    expect(writtenPaths).toContain(tmpPath)
    expect(writtenPaths).not.toContain(dataPath)
  })

  it('does not leave a corrupted data file when writeData throws', async () => {
    const jsonlPath = join(tempDir, 'users.jsonl')
    const dataPath = join(tempDir, 'users.parquet')

    // Write some existing data at the final path
    await writeFile(dataPath, JSON.stringify([
      makeLine({ $id: 'u1', name: 'Alice' }),
    ]))

    // Write a JSONL file with new data
    await writeJsonlFile(jsonlPath, [
      makeLine({ $id: 'u2', name: 'Bob' }),
    ])

    // Create a storage adapter that fails on write
    const failingStorage: FullStorageAdapter = {
      readData: async (path: string) => {
        try {
          return JSON.parse(await readFile(path, 'utf-8'))
        } catch {
          return []
        }
      },
      writeData: async () => {
        throw new Error('Network timeout during write')
      },
      readRels: async () => [],
      writeRels: async () => {},
      readEvents: async () => [],
      writeEvents: async () => {},
    }

    await expect(
      hybridCompactData(tempDir, 'users', failingStorage),
    ).rejects.toThrow('Network timeout during write')

    // The original data file should still be intact (not corrupted)
    // With atomic writes via tmp+rename, a write failure to .tmp
    // leaves the original file untouched.
    const existingData = JSON.parse(await readFile(dataPath, 'utf-8'))
    expect(existingData).toHaveLength(1)
    expect(existingData[0].$id).toBe('u1')
  })

  it('preserves .compacting file on failure for recovery', async () => {
    const jsonlPath = join(tempDir, 'users.jsonl')

    // Write a JSONL file
    await writeJsonlFile(jsonlPath, [
      makeLine({ $id: 'u1', name: 'Alice' }),
    ])

    const failingStorage: FullStorageAdapter = {
      readData: async () => [],
      writeData: async () => {
        throw new Error('Write failed')
      },
      readRels: async () => [],
      writeRels: async () => {},
      readEvents: async () => [],
      writeEvents: async () => {},
    }

    await expect(
      hybridCompactData(tempDir, 'users', failingStorage),
    ).rejects.toThrow('Write failed')

    // The .compacting file should be preserved for recovery
    const compactingPath = jsonlPath + '.compacting'
    expect(await fileExists(compactingPath)).toBe(true)
  })

  it('does not leave .tmp files on disk after successful compaction', async () => {
    const jsonlPath = join(tempDir, 'users.jsonl')

    await writeJsonlFile(jsonlPath, [
      makeLine({ $id: 'u1', name: 'Alice' }),
    ])

    const memStorage = new MemoryStorageAdapter()

    const count = await hybridCompactData(tempDir, 'users', memStorage)
    expect(count).toBe(1)

    // No .tmp files should remain in the temp directory
    const files = await readdir(tempDir)
    const tmpFiles = files.filter(f => f.endsWith('.tmp'))
    expect(tmpFiles).toHaveLength(0)
  })
})

// =============================================================================
// 3. engine.init() cleans up orphaned .tmp files
// =============================================================================

describe('ParqueEngine.init() - .tmp file cleanup', () => {
  it('removes orphaned .tmp files left by interrupted compactions', async () => {
    const tmpFilePath = join(tempDir, 'users.parquet.tmp')

    // Simulate an interrupted compaction that left a .tmp file
    await writeFile(tmpFilePath, JSON.stringify([
      makeLine({ $id: 'u1', name: 'Alice' }),
    ]))

    // Also create a real JSONL file so the table is discovered
    await writeJsonlFile(join(tempDir, 'users.jsonl'), [
      makeLine({ $id: 'u1', name: 'Alice' }),
    ])

    const engine = new ParqueEngine({ dataDir: tempDir })
    await engine.init()

    // The .tmp file should have been cleaned up during init
    expect(await fileExists(tmpFilePath)).toBe(false)

    await engine.close()
  })

  it('removes .tmp files even when no matching .jsonl exists', async () => {
    const tmpFilePath = join(tempDir, 'orphaned.parquet.tmp')

    // Just a .tmp file with no corresponding table
    await writeFile(tmpFilePath, 'partial data')

    const engine = new ParqueEngine({ dataDir: tempDir })
    await engine.init()

    // The orphaned .tmp file should be cleaned up
    expect(await fileExists(tmpFilePath)).toBe(false)

    await engine.close()
  })

  it('does not remove non-.tmp files during cleanup', async () => {
    // Create a real JSONL file and a parquet file
    const jsonlPath = join(tempDir, 'users.jsonl')
    const parquetPath = join(tempDir, 'users.parquet')

    await writeJsonlFile(jsonlPath, [
      makeLine({ $id: 'u1', name: 'Alice' }),
    ])

    // Write a "parquet" file (using JSON for test simplicity)
    // The ParquetStorageAdapter may fail on this, so we just check the
    // .jsonl file is preserved. The important thing is that regular files
    // are not removed.
    await writeFile(parquetPath, 'not-actually-parquet-but-should-not-be-deleted')

    // Also create a .tmp file that should be cleaned up
    const tmpFilePath = join(tempDir, 'something.parquet.tmp')
    await writeFile(tmpFilePath, 'orphaned tmp data')

    const engine = new ParqueEngine({ dataDir: tempDir })

    // init() may throw when trying to read the fake parquet file,
    // but we mainly care that .tmp cleanup happens and .jsonl is preserved
    try {
      await engine.init()
    } catch {
      // Ignore errors from reading fake parquet files
    }

    // The .tmp file should be cleaned up
    expect(await fileExists(tmpFilePath)).toBe(false)

    // The .jsonl and .parquet files should NOT be removed
    expect(await fileExists(jsonlPath)).toBe(true)

    await engine.close()
  })
})
