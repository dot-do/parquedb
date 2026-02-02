/**
 * Custom Matchers Example Usage Tests
 *
 * Demonstrates how to use the custom ParqueDB matchers for testing
 * domain-specific types like Parquet files, events, relationships, and indexes.
 */

import { describe, it, expect } from 'vitest'
import type { Event, EventOp } from '../../src/types/entity'
import type { IndexType } from '../../src/indexes/types'

// =============================================================================
// toBeValidParquetFile Examples
// =============================================================================

describe('toBeValidParquetFile', () => {
  it('validates a properly formatted Parquet file', () => {
    // PAR1 header + some content + PAR1 footer
    const validParquet = new Uint8Array([
      0x50, 0x41, 0x52, 0x31, // PAR1 header
      0x00, 0x00, 0x00, 0x00, // placeholder content
      0x50, 0x41, 0x52, 0x31, // PAR1 footer
    ])

    expect(validParquet).toBeValidParquetFile()
  })

  it('fails for data without PAR1 magic', () => {
    const invalidData = new Uint8Array([0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07])

    expect(() => expect(invalidData).toBeValidParquetFile()).toThrow()
  })

  it('fails for data that is too small', () => {
    const tooSmall = new Uint8Array([0x50, 0x41, 0x52, 0x31])

    expect(() => expect(tooSmall).toBeValidParquetFile()).toThrow()
  })

  it('fails for data missing footer', () => {
    const noFooter = new Uint8Array([
      0x50, 0x41, 0x52, 0x31, // PAR1 header
      0x00, 0x00, 0x00, 0x00, // content
    ])

    expect(() => expect(noFooter).toBeValidParquetFile()).toThrow()
  })
})

// =============================================================================
// toHaveRelationship Examples
// =============================================================================

describe('toHaveRelationship', () => {
  it('validates an entity has a relationship predicate', () => {
    const post = {
      $id: 'posts/post-1',
      $type: 'Post',
      name: 'My Post',
      author: { Alice: 'users/alice' },
    }

    expect(post).toHaveRelationship('author')
  })

  it('validates relationship with specific target', () => {
    const post = {
      $id: 'posts/post-1',
      $type: 'Post',
      name: 'My Post',
      author: { Alice: 'users/alice' },
    }

    expect(post).toHaveRelationship('author', 'users/alice')
  })

  it('validates relationship with regex target', () => {
    const post = {
      $id: 'posts/post-1',
      $type: 'Post',
      name: 'My Post',
      categories: {
        Tech: 'categories/tech',
        Database: 'categories/database',
      },
    }

    expect(post).toHaveRelationship('categories', /^categories\//)
  })

  it('fails when relationship does not exist', () => {
    const post = {
      $id: 'posts/post-1',
      $type: 'Post',
      name: 'My Post',
    }

    expect(() => expect(post).toHaveRelationship('author')).toThrow()
  })

  it('fails when target does not match', () => {
    const post = {
      $id: 'posts/post-1',
      $type: 'Post',
      name: 'My Post',
      author: { Alice: 'users/alice' },
    }

    expect(() => expect(post).toHaveRelationship('author', 'users/bob')).toThrow()
  })
})

// =============================================================================
// toMatchEvent Examples
// =============================================================================

describe('toMatchEvent', () => {
  it('validates a CREATE event', () => {
    const createEvent: Event = {
      id: '01ABCDEF12345678',
      ts: Date.now(),
      op: 'CREATE',
      target: 'users:alice',
      after: { name: 'Alice', email: 'alice@example.com' },
    }

    expect(createEvent).toMatchEvent('CREATE')
  })

  it('validates an UPDATE event with before and after', () => {
    const updateEvent: Event = {
      id: '01ABCDEF12345679',
      ts: Date.now(),
      op: 'UPDATE',
      target: 'users:alice',
      before: { name: 'Alice', email: 'alice@example.com' },
      after: { name: 'Alice Smith', email: 'alice@example.com' },
      actor: 'users:admin',
    }

    expect(updateEvent).toMatchEvent('UPDATE')
  })

  it('validates a DELETE event', () => {
    const deleteEvent: Event = {
      id: '01ABCDEF12345680',
      ts: Date.now(),
      op: 'DELETE',
      target: 'users:bob',
      before: { name: 'Bob', email: 'bob@example.com' },
    }

    expect(deleteEvent).toMatchEvent('DELETE')
  })

  it('validates event with expected data', () => {
    const event: Event = {
      id: '01ABCDEF12345678',
      ts: Date.now(),
      op: 'CREATE',
      target: 'users:alice',
      actor: 'users:admin',
      after: { name: 'Alice' },
    }

    expect(event).toMatchEvent('CREATE', { target: 'users:alice', actor: 'users:admin' })
  })

  it('fails when operation type does not match', () => {
    const event: Event = {
      id: '01ABCDEF12345678',
      ts: Date.now(),
      op: 'CREATE',
      target: 'users:alice',
      after: {},
    }

    expect(() => expect(event).toMatchEvent('UPDATE')).toThrow()
  })

  it('fails when CREATE event has before field', () => {
    const invalidCreate = {
      id: '01ABCDEF12345678',
      ts: Date.now(),
      op: 'CREATE',
      target: 'users:alice',
      before: { shouldNotExist: true }, // Invalid for CREATE
      after: { name: 'Alice' },
    }

    expect(() => expect(invalidCreate).toMatchEvent('CREATE')).toThrow()
  })
})

// =============================================================================
// toBeValidIndex Examples
// =============================================================================

describe('toBeValidIndex', () => {
  it('validates a bloom filter index', () => {
    // Minimal valid bloom filter: 16-byte header + value bloom + row group bloom
    const filterSize = 16 // Small filter for testing
    const numRowGroups = 1
    const totalSize = 16 + filterSize + numRowGroups * 4096

    const bloomIndex = new Uint8Array(totalSize)
    const view = new DataView(bloomIndex.buffer)

    // Header: PQBF magic
    bloomIndex[0] = 0x50 // P
    bloomIndex[1] = 0x51 // Q
    bloomIndex[2] = 0x42 // B
    bloomIndex[3] = 0x46 // F

    // Version (2 bytes, big-endian)
    view.setUint16(4, 1, false)

    // Num hash functions (2 bytes)
    view.setUint16(6, 3, false)

    // Filter size (4 bytes)
    view.setUint32(8, filterSize, false)

    // Num row groups (2 bytes)
    view.setUint16(12, numRowGroups, false)

    // Reserved (2 bytes)
    view.setUint16(14, 0, false)

    expect(bloomIndex).toBeValidIndex('bloom')
  })

  it('validates a hash index', () => {
    // Minimal hash index header: version + flags + entry count
    const hashIndex = new Uint8Array(6)
    const view = new DataView(hashIndex.buffer)

    hashIndex[0] = 0x03 // version 3
    hashIndex[1] = 0x00 // flags (no key hash)
    view.setUint32(2, 0, false) // entry count

    expect(hashIndex).toBeValidIndex('hash')
  })

  // NOTE: SST index test removed - range queries now use native parquet predicate pushdown

  it('fails for invalid bloom filter magic', () => {
    const invalidBloom = new Uint8Array(4096 + 16)
    invalidBloom[0] = 0x00 // Wrong magic

    expect(() => expect(invalidBloom).toBeValidIndex('bloom')).toThrow()
  })

  it('fails for unsupported index version', () => {
    const oldVersion = new Uint8Array(6)
    oldVersion[0] = 0x99 // Unsupported version

    expect(() => expect(oldVersion).toBeValidIndex('hash')).toThrow()
  })
})

// =============================================================================
// toHaveRowGroups Examples
// =============================================================================

describe('toHaveRowGroups', () => {
  it('validates Parquet metadata row group count', () => {
    const metadata = {
      version: 1,
      schema: [],
      numRows: 1000,
      rowGroups: [
        { numRows: 500, totalByteSize: 1024, columns: [] },
        { numRows: 500, totalByteSize: 1024, columns: [] },
      ],
    }

    expect(metadata).toHaveRowGroups(2)
  })

  it('validates empty row groups', () => {
    const emptyMetadata = {
      version: 1,
      schema: [],
      numRows: 0,
      rowGroups: [],
    }

    expect(emptyMetadata).toHaveRowGroups(0)
  })

  it('fails when row group count does not match', () => {
    const metadata = {
      rowGroups: [{ numRows: 100, columns: [] }],
    }

    expect(() => expect(metadata).toHaveRowGroups(5)).toThrow()
  })

  it('fails for objects without rowGroups array', () => {
    const noRowGroups = { version: 1, schema: [] }

    expect(() => expect(noRowGroups).toHaveRowGroups(1)).toThrow()
  })
})

// =============================================================================
// toBeCompressedWith Examples
// =============================================================================

describe('toBeCompressedWith', () => {
  it('validates column chunk compression codec', () => {
    const columnMeta = {
      pathInSchema: ['name'],
      codec: 'SNAPPY',
      totalCompressedSize: 1024,
      totalUncompressedSize: 2048,
      numValues: 100,
      encodings: ['PLAIN'],
    }

    expect(columnMeta).toBeCompressedWith('SNAPPY')
  })

  it('validates row group with columns', () => {
    const rowGroup = {
      numRows: 100,
      columns: [
        { codec: 'ZSTD', pathInSchema: ['id'] },
        { codec: 'ZSTD', pathInSchema: ['name'] },
      ],
    }

    expect(rowGroup).toBeCompressedWith('ZSTD')
  })

  it('validates full metadata structure', () => {
    const metadata = {
      version: 1,
      rowGroups: [
        {
          numRows: 100,
          columns: [
            { codec: 'GZIP', pathInSchema: ['data'] },
          ],
        },
      ],
    }

    expect(metadata).toBeCompressedWith('GZIP')
  })

  it('handles case-insensitive codec comparison', () => {
    const columnMeta = { codec: 'snappy' }

    expect(columnMeta).toBeCompressedWith('SNAPPY')
  })

  it('fails when codec does not match', () => {
    const columnMeta = { codec: 'SNAPPY' }

    expect(() => expect(columnMeta).toBeCompressedWith('ZSTD')).toThrow()
  })

  it('fails for UNCOMPRESSED check', () => {
    const columnMeta = { codec: 'SNAPPY' }

    expect(() => expect(columnMeta).toBeCompressedWith('UNCOMPRESSED')).toThrow()
  })
})
