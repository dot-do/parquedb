/**
 * Storage Backend Benchmarks for ParqueDB
 *
 * Compares performance across storage backends:
 * - MemoryBackend vs FsBackend
 * - Read latency
 * - Write throughput
 * - List/scan performance
 */

import { describe, bench, beforeAll, beforeEach, afterAll } from 'vitest'
import { MemoryBackend } from '../../src/storage/MemoryBackend'
import { FsBackend } from '../../src/storage/FsBackend'
import type { StorageBackend } from '../../src/types/storage'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { promises as fs } from 'node:fs'
import {
  randomString,
  randomInt,
  randomElement,
  getMemoryUsage,
  formatBytes,
  Timer,
  startTimer,
} from './setup'

// =============================================================================
// Test Data Generators
// =============================================================================

function generateSmallData(): Uint8Array {
  // ~100 bytes
  const content = JSON.stringify({
    id: randomString(10),
    value: randomInt(0, 1000),
    timestamp: Date.now(),
  })
  return new TextEncoder().encode(content)
}

function generateMediumData(): Uint8Array {
  // ~1KB
  const content = JSON.stringify({
    id: randomString(10),
    title: randomString(100),
    description: randomString(500),
    tags: Array.from({ length: 10 }, () => randomString(10)),
    metadata: {
      created: Date.now(),
      modified: Date.now(),
      version: randomInt(1, 100),
    },
  })
  return new TextEncoder().encode(content)
}

function generateLargeData(): Uint8Array {
  // ~10KB
  const content = JSON.stringify({
    id: randomString(10),
    title: randomString(100),
    content: randomString(8000),
    tags: Array.from({ length: 50 }, () => randomString(20)),
    metadata: {
      created: Date.now(),
      modified: Date.now(),
      version: randomInt(1, 100),
      history: Array.from({ length: 10 }, () => ({
        timestamp: Date.now(),
        action: randomString(20),
      })),
    },
  })
  return new TextEncoder().encode(content)
}

function generateBinaryData(size: number): Uint8Array {
  const data = new Uint8Array(size)
  for (let i = 0; i < size; i++) {
    data[i] = randomInt(0, 255)
  }
  return data
}

// =============================================================================
// Storage Backend Benchmarks
// =============================================================================

describe('Storage Backend Benchmarks', () => {
  // ===========================================================================
  // MemoryBackend Benchmarks
  // ===========================================================================

  describe('MemoryBackend', () => {
    let memory: MemoryBackend
    const seededFiles: string[] = []

    beforeAll(async () => {
      memory = new MemoryBackend()

      // Seed test data
      for (let i = 0; i < 1000; i++) {
        const path = `data/entity-${i.toString().padStart(6, '0')}.json`
        await memory.write(path, generateMediumData())
        seededFiles.push(path)
      }

      // Create some larger files
      for (let i = 0; i < 100; i++) {
        const path = `data/large-${i.toString().padStart(4, '0')}.json`
        await memory.write(path, generateLargeData())
        seededFiles.push(path)
      }
    })

    describe('Write Operations', () => {
      bench('[Memory] write small file (~100B)', async () => {
        const path = `test/small-${Date.now()}-${randomInt(0, 10000)}.json`
        await memory.write(path, generateSmallData())
      })

      bench('[Memory] write medium file (~1KB)', async () => {
        const path = `test/medium-${Date.now()}-${randomInt(0, 10000)}.json`
        await memory.write(path, generateMediumData())
      })

      bench('[Memory] write large file (~10KB)', async () => {
        const path = `test/large-${Date.now()}-${randomInt(0, 10000)}.json`
        await memory.write(path, generateLargeData())
      })

      bench('[Memory] write binary file (100KB)', async () => {
        const path = `test/binary-${Date.now()}-${randomInt(0, 10000)}.bin`
        await memory.write(path, generateBinaryData(100 * 1024))
      })

      bench('[Memory] overwrite existing file', async () => {
        const path = randomElement(seededFiles)
        await memory.write(path, generateMediumData())
      })

      bench('[Memory] batch write 10 files', async () => {
        for (let i = 0; i < 10; i++) {
          const path = `test/batch-${Date.now()}-${i}.json`
          await memory.write(path, generateMediumData())
        }
      })

      bench('[Memory] batch write 50 files', async () => {
        for (let i = 0; i < 50; i++) {
          const path = `test/batch-${Date.now()}-${i}.json`
          await memory.write(path, generateSmallData())
        }
      })

      bench('[Memory] parallel write 10 files', async () => {
        const ops = Array.from({ length: 10 }, (_, i) => {
          const path = `test/parallel-${Date.now()}-${i}.json`
          return memory.write(path, generateMediumData())
        })
        await Promise.all(ops)
      })
    })

    describe('Read Operations', () => {
      bench('[Memory] read small file', async () => {
        const path = randomElement(seededFiles.filter(f => f.includes('entity-')))
        await memory.read(path)
      })

      bench('[Memory] read large file', async () => {
        const path = randomElement(seededFiles.filter(f => f.includes('large-')))
        await memory.read(path)
      })

      bench('[Memory] read with range (first 100 bytes)', async () => {
        const path = randomElement(seededFiles)
        await memory.readRange(path, 0, 100)
      })

      bench('[Memory] read with range (middle 500 bytes)', async () => {
        const path = randomElement(seededFiles.filter(f => f.includes('large-')))
        await memory.readRange(path, 1000, 1500)
      })

      bench('[Memory] read with range (last 200 bytes)', async () => {
        const path = randomElement(seededFiles.filter(f => f.includes('large-')))
        const stat = await memory.stat(path)
        if (stat) {
          await memory.readRange(path, stat.size - 200, stat.size)
        }
      })

      bench('[Memory] batch read 10 files', async () => {
        for (let i = 0; i < 10; i++) {
          const path = randomElement(seededFiles)
          await memory.read(path)
        }
      })

      bench('[Memory] parallel read 10 files', async () => {
        const ops = Array.from({ length: 10 }, () => {
          const path = randomElement(seededFiles)
          return memory.read(path)
        })
        await Promise.all(ops)
      })

      bench('[Memory] parallel read 50 files', async () => {
        const ops = Array.from({ length: 50 }, () => {
          const path = randomElement(seededFiles)
          return memory.read(path)
        })
        await Promise.all(ops)
      })
    })

    describe('List/Scan Operations', () => {
      bench('[Memory] list all files (1000+)', async () => {
        await memory.list('data/')
      })

      bench('[Memory] list with limit (20)', async () => {
        await memory.list('data/', { limit: 20 })
      })

      bench('[Memory] list with limit (100)', async () => {
        await memory.list('data/', { limit: 100 })
      })

      bench('[Memory] list with pattern filter', async () => {
        await memory.list('data/', { pattern: 'entity-*' })
      })

      bench('[Memory] list with delimiter', async () => {
        await memory.list('', { delimiter: '/' })
      })

      bench('[Memory] list paginated (3 pages of 20)', async () => {
        let cursor: string | undefined
        for (let i = 0; i < 3; i++) {
          const result = await memory.list('data/', { limit: 20, cursor })
          cursor = result.cursor
          if (!result.hasMore) break
        }
      })

      bench('[Memory] list with metadata', async () => {
        await memory.list('data/', { limit: 50, includeMetadata: true })
      })
    })

    describe('Metadata Operations', () => {
      bench('[Memory] exists check (found)', async () => {
        const path = randomElement(seededFiles)
        await memory.exists(path)
      })

      bench('[Memory] exists check (not found)', async () => {
        await memory.exists('nonexistent/path/file.json')
      })

      bench('[Memory] stat file', async () => {
        const path = randomElement(seededFiles)
        await memory.stat(path)
      })

      bench('[Memory] batch exists check (10 files)', async () => {
        for (let i = 0; i < 10; i++) {
          const path = i % 2 === 0 ? randomElement(seededFiles) : `nonexistent-${i}.json`
          await memory.exists(path)
        }
      })
    })

    describe('Delete Operations', () => {
      bench('[Memory] delete single file', async () => {
        const path = `delete-test/file-${Date.now()}.json`
        await memory.write(path, generateSmallData())
        await memory.delete(path)
      })

      bench('[Memory] delete with prefix (10 files)', async () => {
        const prefix = `delete-prefix-${Date.now()}`
        for (let i = 0; i < 10; i++) {
          await memory.write(`${prefix}/file-${i}.json`, generateSmallData())
        }
        await memory.deletePrefix(prefix)
      })
    })
  })

  // ===========================================================================
  // FsBackend Benchmarks
  // ===========================================================================

  describe('FsBackend', () => {
    let fsBackend: FsBackend
    let testDir: string
    const seededFiles: string[] = []

    beforeAll(async () => {
      // Create temp directory for tests
      testDir = join(tmpdir(), `parquedb-bench-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`)
      await fs.mkdir(testDir, { recursive: true })
      fsBackend = new FsBackend(testDir)

      // Seed test data
      for (let i = 0; i < 500; i++) {
        const path = `data/entity-${i.toString().padStart(6, '0')}.json`
        await fsBackend.write(path, generateMediumData())
        seededFiles.push(path)
      }

      // Create some larger files
      for (let i = 0; i < 50; i++) {
        const path = `data/large-${i.toString().padStart(4, '0')}.json`
        await fsBackend.write(path, generateLargeData())
        seededFiles.push(path)
      }
    }, 60000)

    afterAll(async () => {
      // Cleanup
      try {
        await fs.rm(testDir, { recursive: true, force: true })
      } catch {
        // Ignore cleanup errors
      }
    })

    describe('Write Operations', () => {
      bench('[Fs] write small file (~100B)', async () => {
        const path = `test/small-${Date.now()}-${randomInt(0, 10000)}.json`
        await fsBackend.write(path, generateSmallData())
      })

      bench('[Fs] write medium file (~1KB)', async () => {
        const path = `test/medium-${Date.now()}-${randomInt(0, 10000)}.json`
        await fsBackend.write(path, generateMediumData())
      })

      bench('[Fs] write large file (~10KB)', async () => {
        const path = `test/large-${Date.now()}-${randomInt(0, 10000)}.json`
        await fsBackend.write(path, generateLargeData())
      })

      bench('[Fs] write binary file (100KB)', async () => {
        const path = `test/binary-${Date.now()}-${randomInt(0, 10000)}.bin`
        await fsBackend.write(path, generateBinaryData(100 * 1024))
      })

      bench('[Fs] atomic write medium file', async () => {
        const path = `test/atomic-${Date.now()}-${randomInt(0, 10000)}.json`
        await fsBackend.writeAtomic(path, generateMediumData())
      })

      bench('[Fs] overwrite existing file', async () => {
        const path = randomElement(seededFiles)
        await fsBackend.write(path, generateMediumData())
      })

      bench('[Fs] batch write 10 files', async () => {
        for (let i = 0; i < 10; i++) {
          const path = `test/batch-${Date.now()}-${i}.json`
          await fsBackend.write(path, generateMediumData())
        }
      })

      bench('[Fs] parallel write 10 files', async () => {
        const ops = Array.from({ length: 10 }, (_, i) => {
          const path = `test/parallel-${Date.now()}-${i}.json`
          return fsBackend.write(path, generateMediumData())
        })
        await Promise.all(ops)
      })

      bench('[Fs] append to file', async () => {
        const path = `test/append-${Date.now()}.log`
        await fsBackend.write(path, generateSmallData())
        for (let i = 0; i < 10; i++) {
          await fsBackend.append(path, generateSmallData())
        }
      })
    })

    describe('Read Operations', () => {
      bench('[Fs] read small file', async () => {
        const path = randomElement(seededFiles.filter(f => f.includes('entity-')))
        await fsBackend.read(path)
      })

      bench('[Fs] read large file', async () => {
        const path = randomElement(seededFiles.filter(f => f.includes('large-')))
        await fsBackend.read(path)
      })

      bench('[Fs] read with range (first 100 bytes)', async () => {
        const path = randomElement(seededFiles)
        await fsBackend.readRange(path, 0, 100)
      })

      bench('[Fs] read with range (middle 500 bytes)', async () => {
        const path = randomElement(seededFiles.filter(f => f.includes('large-')))
        await fsBackend.readRange(path, 1000, 1500)
      })

      bench('[Fs] batch read 10 files', async () => {
        for (let i = 0; i < 10; i++) {
          const path = randomElement(seededFiles)
          await fsBackend.read(path)
        }
      })

      bench('[Fs] parallel read 10 files', async () => {
        const ops = Array.from({ length: 10 }, () => {
          const path = randomElement(seededFiles)
          return fsBackend.read(path)
        })
        await Promise.all(ops)
      })

      bench('[Fs] parallel read 50 files', async () => {
        const ops = Array.from({ length: 50 }, () => {
          const path = randomElement(seededFiles)
          return fsBackend.read(path)
        })
        await Promise.all(ops)
      })
    })

    describe('List/Scan Operations', () => {
      bench('[Fs] list all files (500+)', async () => {
        await fsBackend.list('data/')
      })

      bench('[Fs] list with limit (20)', async () => {
        await fsBackend.list('data/', { limit: 20 })
      })

      bench('[Fs] list with pattern filter', async () => {
        await fsBackend.list('data/', { pattern: 'entity-*' })
      })

      bench('[Fs] list with delimiter', async () => {
        await fsBackend.list('', { delimiter: '/' })
      })

      bench('[Fs] list paginated (3 pages of 20)', async () => {
        let cursor: string | undefined
        for (let i = 0; i < 3; i++) {
          const result = await fsBackend.list('data/', { limit: 20, cursor })
          cursor = result.cursor
          if (!result.hasMore) break
        }
      })

      bench('[Fs] list with metadata', async () => {
        await fsBackend.list('data/', { limit: 50, includeMetadata: true })
      })
    })

    describe('Metadata Operations', () => {
      bench('[Fs] exists check (found)', async () => {
        const path = randomElement(seededFiles)
        await fsBackend.exists(path)
      })

      bench('[Fs] exists check (not found)', async () => {
        await fsBackend.exists('nonexistent/path/file.json')
      })

      bench('[Fs] stat file', async () => {
        const path = randomElement(seededFiles)
        await fsBackend.stat(path)
      })
    })

    describe('Delete Operations', () => {
      bench('[Fs] delete single file', async () => {
        const path = `delete-test/file-${Date.now()}.json`
        await fsBackend.write(path, generateSmallData())
        await fsBackend.delete(path)
      })

      bench('[Fs] delete with prefix (10 files)', async () => {
        const prefix = `delete-prefix-${Date.now()}`
        for (let i = 0; i < 10; i++) {
          await fsBackend.write(`${prefix}/file-${i}.json`, generateSmallData())
        }
        await fsBackend.deletePrefix(prefix)
      })
    })

    describe('Directory Operations', () => {
      bench('[Fs] mkdir (single level)', async () => {
        const path = `new-dir-${Date.now()}`
        await fsBackend.mkdir(path)
      })

      bench('[Fs] mkdir (nested)', async () => {
        const path = `nested/${Date.now()}/deep/path`
        await fsBackend.mkdir(path)
      })

      bench('[Fs] rmdir (empty)', async () => {
        const path = `rmdir-test-${Date.now()}`
        await fsBackend.mkdir(path)
        await fsBackend.rmdir(path)
      })

      bench('[Fs] rmdir recursive (with 10 files)', async () => {
        const path = `rmdir-recursive-${Date.now()}`
        for (let i = 0; i < 10; i++) {
          await fsBackend.write(`${path}/file-${i}.json`, generateSmallData())
        }
        await fsBackend.rmdir(path, { recursive: true })
      })
    })

    describe('Copy/Move Operations', () => {
      bench('[Fs] copy file', async () => {
        const source = randomElement(seededFiles)
        const dest = `copy-test/copied-${Date.now()}.json`
        await fsBackend.copy(source, dest)
      })

      bench('[Fs] move file', async () => {
        const source = `move-source-${Date.now()}.json`
        const dest = `move-dest-${Date.now()}.json`
        await fsBackend.write(source, generateMediumData())
        await fsBackend.move(source, dest)
      })
    })
  })

  // ===========================================================================
  // Backend Comparison
  // ===========================================================================

  describe('Backend Comparison', () => {
    let memory: MemoryBackend
    let fsBackend: FsBackend
    let testDir: string

    beforeAll(async () => {
      memory = new MemoryBackend()

      testDir = join(tmpdir(), `parquedb-compare-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`)
      await fs.mkdir(testDir, { recursive: true })
      fsBackend = new FsBackend(testDir)

      // Seed both with identical data
      for (let i = 0; i < 200; i++) {
        const path = `compare/entity-${i.toString().padStart(4, '0')}.json`
        const data = generateMediumData()
        await memory.write(path, data)
        await fsBackend.write(path, data)
      }
    }, 30000)

    afterAll(async () => {
      try {
        await fs.rm(testDir, { recursive: true, force: true })
      } catch {
        // Ignore cleanup errors
      }
    })

    // Direct comparisons
    bench('[Compare] Memory: write 1KB', async () => {
      const path = `bench/write-${Date.now()}.json`
      await memory.write(path, generateMediumData())
    })

    bench('[Compare] Fs: write 1KB', async () => {
      const path = `bench/write-${Date.now()}.json`
      await fsBackend.write(path, generateMediumData())
    })

    bench('[Compare] Memory: read 1KB', async () => {
      await memory.read('compare/entity-0100.json')
    })

    bench('[Compare] Fs: read 1KB', async () => {
      await fsBackend.read('compare/entity-0100.json')
    })

    bench('[Compare] Memory: list 200 files', async () => {
      await memory.list('compare/')
    })

    bench('[Compare] Fs: list 200 files', async () => {
      await fsBackend.list('compare/')
    })

    bench('[Compare] Memory: 10 parallel reads', async () => {
      const ops = Array.from({ length: 10 }, (_, i) =>
        memory.read(`compare/entity-${(i * 10).toString().padStart(4, '0')}.json`)
      )
      await Promise.all(ops)
    })

    bench('[Compare] Fs: 10 parallel reads', async () => {
      const ops = Array.from({ length: 10 }, (_, i) =>
        fsBackend.read(`compare/entity-${(i * 10).toString().padStart(4, '0')}.json`)
      )
      await Promise.all(ops)
    })

    bench('[Compare] Memory: 10 parallel writes', async () => {
      const ops = Array.from({ length: 10 }, (_, i) =>
        memory.write(`bench/parallel-mem-${Date.now()}-${i}.json`, generateMediumData())
      )
      await Promise.all(ops)
    })

    bench('[Compare] Fs: 10 parallel writes', async () => {
      const ops = Array.from({ length: 10 }, (_, i) =>
        fsBackend.write(`bench/parallel-fs-${Date.now()}-${i}.json`, generateMediumData())
      )
      await Promise.all(ops)
    })

    bench('[Compare] Memory: mixed workload (read/write)', async () => {
      for (let i = 0; i < 20; i++) {
        if (i % 4 === 0) {
          await memory.write(`bench/mixed-mem-${Date.now()}-${i}.json`, generateMediumData())
        } else {
          await memory.read(`compare/entity-${(i * 5).toString().padStart(4, '0')}.json`)
        }
      }
    })

    bench('[Compare] Fs: mixed workload (read/write)', async () => {
      for (let i = 0; i < 20; i++) {
        if (i % 4 === 0) {
          await fsBackend.write(`bench/mixed-fs-${Date.now()}-${i}.json`, generateMediumData())
        } else {
          await fsBackend.read(`compare/entity-${(i * 5).toString().padStart(4, '0')}.json`)
        }
      }
    })
  })

  // ===========================================================================
  // Throughput Tests
  // ===========================================================================

  describe('Throughput Tests', () => {
    let memory: MemoryBackend
    let fsBackend: FsBackend
    let testDir: string

    beforeAll(async () => {
      memory = new MemoryBackend()
      testDir = join(tmpdir(), `parquedb-throughput-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`)
      await fs.mkdir(testDir, { recursive: true })
      fsBackend = new FsBackend(testDir)
    })

    afterAll(async () => {
      try {
        await fs.rm(testDir, { recursive: true, force: true })
      } catch {
        // Ignore cleanup errors
      }
    })

    bench('[Throughput] Memory: 100 writes/iteration', async () => {
      for (let i = 0; i < 100; i++) {
        await memory.write(`throughput/file-${i}.json`, generateSmallData())
      }
    }, { iterations: 10 })

    bench('[Throughput] Memory: 100 reads/iteration', async () => {
      // Pre-seed
      for (let i = 0; i < 100; i++) {
        await memory.write(`throughput-read/file-${i}.json`, generateSmallData())
      }

      // Read
      for (let i = 0; i < 100; i++) {
        await memory.read(`throughput-read/file-${i}.json`)
      }
    }, { iterations: 10 })

    bench('[Throughput] Fs: 50 writes/iteration', async () => {
      for (let i = 0; i < 50; i++) {
        await fsBackend.write(`throughput/file-${Date.now()}-${i}.json`, generateSmallData())
      }
    }, { iterations: 5 })

    bench('[Throughput] Fs: 50 reads/iteration', async () => {
      // Pre-seed
      const paths: string[] = []
      for (let i = 0; i < 50; i++) {
        const path = `throughput-read/file-${i}.json`
        await fsBackend.write(path, generateSmallData())
        paths.push(path)
      }

      // Read
      for (const path of paths) {
        await fsBackend.read(path)
      }
    }, { iterations: 5 })
  })

  // ===========================================================================
  // Latency Percentiles (Manual Tracking)
  // ===========================================================================

  describe('Latency Distribution', () => {
    let memory: MemoryBackend
    const paths: string[] = []

    beforeAll(async () => {
      memory = new MemoryBackend()

      // Seed data
      for (let i = 0; i < 1000; i++) {
        const path = `latency/file-${i.toString().padStart(4, '0')}.json`
        await memory.write(path, generateMediumData())
        paths.push(path)
      }
    })

    bench('[Latency] Memory read distribution (1000 reads)', async () => {
      for (let i = 0; i < 1000; i++) {
        const path = paths[i % paths.length]
        await memory.read(path)
      }
    }, { iterations: 5 })

    bench('[Latency] Memory write distribution (100 writes)', async () => {
      for (let i = 0; i < 100; i++) {
        await memory.write(`latency/new-${Date.now()}-${i}.json`, generateMediumData())
      }
    }, { iterations: 5 })

    bench('[Latency] Memory list distribution (100 lists)', async () => {
      for (let i = 0; i < 100; i++) {
        await memory.list('latency/', { limit: 20 })
      }
    }, { iterations: 5 })
  })
})
