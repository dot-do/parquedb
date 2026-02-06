/**
 * LocalStorageAdapter Error Handling Tests
 *
 * Verifies that LocalStorageAdapter only treats ENOENT (missing file) as "empty"
 * and re-throws all other errors (corrupt files, permission errors, etc.)
 * instead of silently swallowing them.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { LocalStorageAdapter } from '@/engine/storage-adapters'

let tmpDir: string
let adapter: LocalStorageAdapter

beforeEach(async () => {
  tmpDir = await mkdtemp(join(tmpdir(), 'storage-adapters-test-'))
  adapter = new LocalStorageAdapter()
})

afterEach(async () => {
  await rm(tmpDir, { recursive: true, force: true })
})

// =============================================================================
// readData error handling
// =============================================================================

describe('LocalStorageAdapter.readData error handling', () => {
  it('returns [] for missing file (ENOENT)', async () => {
    const result = await adapter.readData(join(tmpDir, 'nonexistent.json'))
    expect(result).toEqual([])
  })

  it('throws for corrupt JSON file', async () => {
    const corruptPath = join(tmpDir, 'corrupt-data.json')
    await writeFile(corruptPath, 'not valid json{{{')
    await expect(adapter.readData(corruptPath)).rejects.toThrow()
  })
})

// =============================================================================
// readRels error handling
// =============================================================================

describe('LocalStorageAdapter.readRels error handling', () => {
  it('returns [] for missing file (ENOENT)', async () => {
    const result = await adapter.readRels(join(tmpDir, 'nonexistent-rels.json'))
    expect(result).toEqual([])
  })

  it('throws for corrupt JSON file', async () => {
    const corruptPath = join(tmpDir, 'corrupt-rels.json')
    await writeFile(corruptPath, '{{invalid json!!!}')
    await expect(adapter.readRels(corruptPath)).rejects.toThrow()
  })
})

// =============================================================================
// readEvents error handling
// =============================================================================

describe('LocalStorageAdapter.readEvents error handling', () => {
  it('returns [] for missing file (ENOENT)', async () => {
    const result = await adapter.readEvents(join(tmpDir, 'nonexistent-events.json'))
    expect(result).toEqual([])
  })

  it('throws for corrupt JSON file', async () => {
    const corruptPath = join(tmpDir, 'corrupt-events.json')
    await writeFile(corruptPath, 'this is definitely not json')
    await expect(adapter.readEvents(corruptPath)).rejects.toThrow()
  })
})
