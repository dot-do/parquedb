/**
 * R2 Event Message Parsing Tests
 *
 * Tests for parsing R2 event notification messages received from Cloudflare Queues.
 * Covers:
 * - Action type detection (PutObject, CopyObject, DeleteObject, etc.)
 * - File path parsing ({timestamp}-{writerId}-{seq}.parquet)
 * - Timestamp conversion (seconds to milliseconds)
 * - Namespace extraction
 */

import { describe, it, expect } from 'vitest'
import type { R2EventMessage } from '@/workflows/compaction-queue-consumer'

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Create a test R2 event message
 */
function createR2EventMessage(
  key: string,
  size: number = 1024,
  action: R2EventMessage['action'] = 'PutObject'
): R2EventMessage {
  return {
    account: 'test-account',
    bucket: 'parquedb-data',
    object: {
      key,
      size,
      eTag: '"abc123"',
    },
    action,
    eventTime: new Date().toISOString(),
  }
}

/**
 * Parse file info from R2 event message key
 * Mirrors the parsing logic in handleCompactionQueue
 */
function parseFileInfo(key: string, prefix: string = 'data/') {
  if (!key.startsWith(prefix)) {
    return null
  }
  if (!key.endsWith('.parquet')) {
    return null
  }

  const keyWithoutPrefix = key.slice(prefix.length)
  const parts = keyWithoutPrefix.split('/')
  const namespace = parts.slice(0, -1).join('/')
  const filename = parts[parts.length - 1] ?? ''

  const match = filename.match(/^(\d+)-([^-]+)-(\d+)\.parquet$/)
  if (!match) {
    return null
  }

  const [, timestampStr, writerId, seqStr] = match
  // Filename timestamps are in seconds, convert to milliseconds
  const timestamp = parseInt(timestampStr ?? '0', 10) * 1000
  const seq = parseInt(seqStr ?? '0', 10)

  return {
    namespace,
    writerId: writerId ?? 'unknown',
    timestamp,
    seq,
    filename,
  }
}

/**
 * Check if action is a create action (puts new data into R2)
 */
function isCreateAction(action: R2EventMessage['action']): boolean {
  return (
    action === 'PutObject' ||
    action === 'CopyObject' ||
    action === 'CompleteMultipartUpload'
  )
}

// =============================================================================
// Action Type Detection Tests
// =============================================================================

describe('R2 Event Message - Action Type Detection', () => {
  describe('create actions', () => {
    it('should recognize PutObject as create action', () => {
      const message = createR2EventMessage('data/users/file.parquet', 1024, 'PutObject')
      expect(isCreateAction(message.action)).toBe(true)
    })

    it('should recognize CopyObject as create action', () => {
      const message = createR2EventMessage('data/users/file.parquet', 1024, 'CopyObject')
      expect(isCreateAction(message.action)).toBe(true)
    })

    it('should recognize CompleteMultipartUpload as create action', () => {
      const message = createR2EventMessage('data/users/file.parquet', 1024, 'CompleteMultipartUpload')
      expect(isCreateAction(message.action)).toBe(true)
    })
  })

  describe('non-create actions', () => {
    it('should not recognize DeleteObject as create action', () => {
      const message = createR2EventMessage('data/users/file.parquet', 1024, 'DeleteObject')
      expect(isCreateAction(message.action)).toBe(false)
    })

    it('should not recognize LifecycleDeletion as create action', () => {
      const message = createR2EventMessage('data/users/file.parquet', 1024, 'LifecycleDeletion')
      expect(isCreateAction(message.action)).toBe(false)
    })
  })

  describe('all action types', () => {
    const allActions: R2EventMessage['action'][] = [
      'PutObject',
      'CopyObject',
      'CompleteMultipartUpload',
      'DeleteObject',
      'LifecycleDeletion',
    ]

    it('should handle all defined action types', () => {
      for (const action of allActions) {
        const message = createR2EventMessage('data/users/file.parquet', 1024, action)
        expect(message.action).toBe(action)
        // Just verify it doesn't throw
        expect(typeof isCreateAction(message.action)).toBe('boolean')
      }
    })
  })
})

// =============================================================================
// File Path Parsing Tests
// =============================================================================

describe('R2 Event Message - File Path Parsing', () => {
  describe('valid parquet file paths', () => {
    it('should parse simple namespace path', () => {
      const result = parseFileInfo('data/users/1700001234-writer1-0.parquet')

      expect(result).not.toBeNull()
      expect(result!.namespace).toBe('users')
      expect(result!.writerId).toBe('writer1')
      expect(result!.timestamp).toBe(1700001234000) // seconds -> milliseconds
      expect(result!.seq).toBe(0)
    })

    it('should parse nested namespace path', () => {
      const result = parseFileInfo('data/app/users/1700001234-writer1-0.parquet')

      expect(result).not.toBeNull()
      expect(result!.namespace).toBe('app/users')
      expect(result!.writerId).toBe('writer1')
    })

    it('should parse deeply nested namespace path', () => {
      const result = parseFileInfo('data/org/team/project/1700001234-writer1-0.parquet')

      expect(result).not.toBeNull()
      expect(result!.namespace).toBe('org/team/project')
    })

    it('should parse writer ID with numbers', () => {
      const result = parseFileInfo('data/users/1700001234-writer123-5.parquet')

      expect(result).not.toBeNull()
      expect(result!.writerId).toBe('writer123')
    })

    it('should parse writer ID with alphanumeric characters', () => {
      const result = parseFileInfo('data/users/1700001234-abc123def-99.parquet')

      expect(result).not.toBeNull()
      expect(result!.writerId).toBe('abc123def')
    })

    it('should parse high sequence numbers', () => {
      const result = parseFileInfo('data/users/1700001234-writer1-99999.parquet')

      expect(result).not.toBeNull()
      expect(result!.seq).toBe(99999)
    })

    it('should parse large timestamps', () => {
      // Timestamp for year 2050
      const result = parseFileInfo('data/users/2524608000-writer1-0.parquet')

      expect(result).not.toBeNull()
      expect(result!.timestamp).toBe(2524608000000)
    })
  })

  describe('file path with custom prefix', () => {
    it('should parse with default data/ prefix', () => {
      const result = parseFileInfo('data/users/1700001234-writer1-0.parquet', 'data/')

      expect(result).not.toBeNull()
      expect(result!.namespace).toBe('users')
    })

    it('should parse with custom prefix', () => {
      const result = parseFileInfo('events/users/1700001234-writer1-0.parquet', 'events/')

      expect(result).not.toBeNull()
      expect(result!.namespace).toBe('users')
    })

    it('should return null for non-matching prefix', () => {
      const result = parseFileInfo('logs/users/1700001234-writer1-0.parquet', 'data/')

      expect(result).toBeNull()
    })
  })

  describe('invalid file paths', () => {
    it('should return null for non-parquet files', () => {
      expect(parseFileInfo('data/users/metadata.json')).toBeNull()
      expect(parseFileInfo('data/users/config.yaml')).toBeNull()
      expect(parseFileInfo('data/users/readme.txt')).toBeNull()
    })

    it('should return null for invalid filename format - no dashes', () => {
      expect(parseFileInfo('data/users/invalid.parquet')).toBeNull()
    })

    it('should return null for invalid filename format - missing sequence', () => {
      expect(parseFileInfo('data/users/1700001234-writer1.parquet')).toBeNull()
    })

    it('should return null for invalid filename format - missing writer', () => {
      expect(parseFileInfo('data/users/1700001234-0.parquet')).toBeNull()
    })

    it('should return null for invalid filename format - missing timestamp', () => {
      expect(parseFileInfo('data/users/writer1-0.parquet')).toBeNull()
    })

    it('should return null for invalid filename format - non-numeric timestamp', () => {
      expect(parseFileInfo('data/users/abc-writer1-0.parquet')).toBeNull()
    })

    it('should return null for invalid filename format - non-numeric sequence', () => {
      expect(parseFileInfo('data/users/1700001234-writer1-abc.parquet')).toBeNull()
    })

    it('should return null for files outside prefix', () => {
      expect(parseFileInfo('logs/system.parquet', 'data/')).toBeNull()
      expect(parseFileInfo('backup/1700001234-writer1-0.parquet', 'data/')).toBeNull()
    })

    it('should return null for empty writer ID', () => {
      expect(parseFileInfo('data/users/1700001234--0.parquet')).toBeNull()
    })

    it('should handle root level files with empty namespace', () => {
      // File directly in data/ with no namespace subdirectory
      // Note: This parses successfully but with empty namespace
      const result = parseFileInfo('data/1700001234-writer1-0.parquet')
      expect(result).not.toBeNull()
      expect(result!.namespace).toBe('')
    })
  })

  describe('edge cases', () => {
    it('should handle namespace with dashes', () => {
      const result = parseFileInfo('data/user-data/1700001234-writer1-0.parquet')

      expect(result).not.toBeNull()
      expect(result!.namespace).toBe('user-data')
    })

    it('should handle namespace with underscores', () => {
      const result = parseFileInfo('data/user_data/1700001234-writer1-0.parquet')

      expect(result).not.toBeNull()
      expect(result!.namespace).toBe('user_data')
    })

    it('should handle minimum valid timestamp (0)', () => {
      const result = parseFileInfo('data/users/0-writer1-0.parquet')

      expect(result).not.toBeNull()
      expect(result!.timestamp).toBe(0)
    })

    it('should handle sequence number 0', () => {
      const result = parseFileInfo('data/users/1700001234-writer1-0.parquet')

      expect(result).not.toBeNull()
      expect(result!.seq).toBe(0)
    })
  })
})

// =============================================================================
// Timestamp Conversion Tests
// =============================================================================

describe('R2 Event Message - Timestamp Conversion', () => {
  describe('seconds to milliseconds conversion', () => {
    it('should convert timestamp from seconds to milliseconds', () => {
      const result = parseFileInfo('data/users/1700001234-writer1-0.parquet')

      expect(result).not.toBeNull()
      // Original: 1700001234 seconds
      // Expected: 1700001234000 milliseconds
      expect(result!.timestamp).toBe(1700001234000)
    })

    it('should handle zero timestamp', () => {
      const result = parseFileInfo('data/users/0-writer1-0.parquet')

      expect(result).not.toBeNull()
      expect(result!.timestamp).toBe(0)
    })

    it('should handle Unix epoch timestamp', () => {
      // 1 second after Unix epoch
      const result = parseFileInfo('data/users/1-writer1-0.parquet')

      expect(result).not.toBeNull()
      expect(result!.timestamp).toBe(1000)
    })

    it('should preserve millisecond precision after conversion', () => {
      const testCases = [
        { seconds: 1700000000, expectedMs: 1700000000000 },
        { seconds: 1700001234, expectedMs: 1700001234000 },
        { seconds: 2000000000, expectedMs: 2000000000000 },
        { seconds: 1234567890, expectedMs: 1234567890000 },
      ]

      for (const { seconds, expectedMs } of testCases) {
        const result = parseFileInfo(`data/users/${seconds}-writer1-0.parquet`)
        expect(result).not.toBeNull()
        expect(result!.timestamp).toBe(expectedMs)
      }
    })
  })

  describe('timestamp validity', () => {
    it('should parse timestamps that represent valid dates', () => {
      // 2023-11-14 22:13:20 UTC
      const result = parseFileInfo('data/users/1700000000-writer1-0.parquet')

      expect(result).not.toBeNull()
      const date = new Date(result!.timestamp)
      expect(date.getUTCFullYear()).toBe(2023)
      expect(date.getUTCMonth()).toBe(10) // November (0-indexed)
    })

    it('should parse future timestamps (year 2050)', () => {
      // 2050-01-01 00:00:00 UTC = 2524608000 seconds
      const result = parseFileInfo('data/users/2524608000-writer1-0.parquet')

      expect(result).not.toBeNull()
      const date = new Date(result!.timestamp)
      expect(date.getUTCFullYear()).toBe(2050)
    })
  })
})

// =============================================================================
// Namespace Extraction Tests
// =============================================================================

describe('R2 Event Message - Namespace Extraction', () => {
  describe('single-level namespaces', () => {
    it('should extract "users" namespace', () => {
      const result = parseFileInfo('data/users/1700001234-writer1-0.parquet')
      expect(result!.namespace).toBe('users')
    })

    it('should extract "posts" namespace', () => {
      const result = parseFileInfo('data/posts/1700001234-writer1-0.parquet')
      expect(result!.namespace).toBe('posts')
    })

    it('should extract "events" namespace', () => {
      const result = parseFileInfo('data/events/1700001234-writer1-0.parquet')
      expect(result!.namespace).toBe('events')
    })
  })

  describe('multi-level namespaces', () => {
    it('should extract two-level namespace', () => {
      const result = parseFileInfo('data/app/users/1700001234-writer1-0.parquet')
      expect(result!.namespace).toBe('app/users')
    })

    it('should extract three-level namespace', () => {
      const result = parseFileInfo('data/org/team/users/1700001234-writer1-0.parquet')
      expect(result!.namespace).toBe('org/team/users')
    })

    it('should extract four-level namespace', () => {
      const result = parseFileInfo('data/org/team/project/users/1700001234-writer1-0.parquet')
      expect(result!.namespace).toBe('org/team/project/users')
    })
  })

  describe('namespace with special characters', () => {
    it('should preserve dashes in namespace', () => {
      const result = parseFileInfo('data/my-app/user-data/1700001234-writer1-0.parquet')
      expect(result!.namespace).toBe('my-app/user-data')
    })

    it('should preserve underscores in namespace', () => {
      const result = parseFileInfo('data/my_app/user_data/1700001234-writer1-0.parquet')
      expect(result!.namespace).toBe('my_app/user_data')
    })

    it('should preserve numbers in namespace', () => {
      const result = parseFileInfo('data/app2/users123/1700001234-writer1-0.parquet')
      expect(result!.namespace).toBe('app2/users123')
    })
  })

  describe('namespace isolation', () => {
    it('should treat different namespaces as distinct', () => {
      const result1 = parseFileInfo('data/users/1700001234-writer1-0.parquet')
      const result2 = parseFileInfo('data/posts/1700001234-writer1-0.parquet')

      expect(result1!.namespace).not.toBe(result2!.namespace)
      expect(result1!.namespace).toBe('users')
      expect(result2!.namespace).toBe('posts')
    })

    it('should treat nested namespaces as distinct from parent', () => {
      const result1 = parseFileInfo('data/users/1700001234-writer1-0.parquet')
      const result2 = parseFileInfo('data/users/archived/1700001234-writer1-0.parquet')

      expect(result1!.namespace).not.toBe(result2!.namespace)
      expect(result1!.namespace).toBe('users')
      expect(result2!.namespace).toBe('users/archived')
    })
  })
})

// =============================================================================
// Message Batch Processing Tests
// =============================================================================

describe('R2 Event Message - Batch Processing', () => {
  describe('filtering messages', () => {
    it('should filter out delete events', () => {
      const messages = [
        createR2EventMessage('data/users/1700001234-writer1-0.parquet', 1024, 'PutObject'),
        createR2EventMessage('data/users/1700001235-writer1-1.parquet', 1024, 'DeleteObject'),
        createR2EventMessage('data/users/1700001236-writer1-2.parquet', 1024, 'PutObject'),
      ]

      const createMessages = messages.filter(m => isCreateAction(m.action))

      expect(createMessages).toHaveLength(2)
    })

    it('should filter out non-parquet files', () => {
      const messages = [
        createR2EventMessage('data/users/1700001234-writer1-0.parquet', 1024),
        createR2EventMessage('data/users/metadata.json', 100),
        createR2EventMessage('data/users/1700001235-writer1-1.parquet', 1024),
      ]

      const parquetMessages = messages.filter(m => m.object.key.endsWith('.parquet'))

      expect(parquetMessages).toHaveLength(2)
    })

    it('should filter out files with invalid filename format', () => {
      const messages = [
        createR2EventMessage('data/users/1700001234-writer1-0.parquet', 1024),
        createR2EventMessage('data/users/invalid.parquet', 1024),
        createR2EventMessage('data/users/1700001235-writer1-1.parquet', 1024),
      ]

      const validMessages = messages.filter(m => parseFileInfo(m.object.key) !== null)

      expect(validMessages).toHaveLength(2)
    })

    it('should filter out files outside namespace prefix', () => {
      const prefix = 'data/'
      const messages = [
        createR2EventMessage('data/users/1700001234-writer1-0.parquet', 1024),
        createR2EventMessage('logs/system/1700001234-writer1-0.parquet', 1024),
        createR2EventMessage('data/posts/1700001235-writer1-1.parquet', 1024),
      ]

      const prefixMessages = messages.filter(m => m.object.key.startsWith(prefix))

      expect(prefixMessages).toHaveLength(2)
    })

    it('should apply all filters correctly', () => {
      const prefix = 'data/'
      const messages = [
        // Valid
        createR2EventMessage('data/users/1700001234-writer1-0.parquet', 1024, 'PutObject'),
        // Invalid: delete action
        createR2EventMessage('data/users/1700001235-writer1-1.parquet', 1024, 'DeleteObject'),
        // Invalid: not parquet
        createR2EventMessage('data/users/metadata.json', 100, 'PutObject'),
        // Invalid: wrong prefix
        createR2EventMessage('logs/system/1700001234-writer1-0.parquet', 1024, 'PutObject'),
        // Invalid: bad filename format
        createR2EventMessage('data/users/invalid.parquet', 1024, 'PutObject'),
        // Valid
        createR2EventMessage('data/posts/1700001236-writer2-0.parquet', 2048, 'CopyObject'),
      ]

      const validMessages = messages.filter(m => {
        if (!isCreateAction(m.action)) return false
        if (!m.object.key.endsWith('.parquet')) return false
        if (!m.object.key.startsWith(prefix)) return false
        if (!parseFileInfo(m.object.key, prefix)) return false
        return true
      })

      expect(validMessages).toHaveLength(2)
      expect(validMessages[0].object.key).toBe('data/users/1700001234-writer1-0.parquet')
      expect(validMessages[1].object.key).toBe('data/posts/1700001236-writer2-0.parquet')
    })
  })

  describe('grouping messages by namespace', () => {
    it('should group messages by namespace', () => {
      const messages = [
        createR2EventMessage('data/users/1700001234-writer1-0.parquet', 1024),
        createR2EventMessage('data/posts/1700001235-writer1-0.parquet', 1024),
        createR2EventMessage('data/users/1700001236-writer2-0.parquet', 1024),
        createR2EventMessage('data/posts/1700001237-writer2-1.parquet', 1024),
        createR2EventMessage('data/users/1700001238-writer1-1.parquet', 1024),
      ]

      const byNamespace = new Map<string, R2EventMessage[]>()

      for (const msg of messages) {
        const info = parseFileInfo(msg.object.key)
        if (!info) continue

        const existing = byNamespace.get(info.namespace) ?? []
        existing.push(msg)
        byNamespace.set(info.namespace, existing)
      }

      expect(byNamespace.size).toBe(2)
      expect(byNamespace.get('users')).toHaveLength(3)
      expect(byNamespace.get('posts')).toHaveLength(2)
    })
  })
})

// =============================================================================
// Event Message Structure Tests
// =============================================================================

describe('R2 Event Message - Message Structure', () => {
  describe('message fields', () => {
    it('should have all required fields', () => {
      const message = createR2EventMessage('data/users/1700001234-writer1-0.parquet')

      expect(message).toHaveProperty('account')
      expect(message).toHaveProperty('bucket')
      expect(message).toHaveProperty('object')
      expect(message).toHaveProperty('action')
      expect(message).toHaveProperty('eventTime')
    })

    it('should have valid object fields', () => {
      const message = createR2EventMessage('data/users/1700001234-writer1-0.parquet', 2048)

      expect(message.object).toHaveProperty('key')
      expect(message.object).toHaveProperty('size')
      expect(message.object).toHaveProperty('eTag')
      expect(message.object.key).toBe('data/users/1700001234-writer1-0.parquet')
      expect(message.object.size).toBe(2048)
    })

    it('should have valid eventTime as ISO string', () => {
      const message = createR2EventMessage('data/users/1700001234-writer1-0.parquet')

      // Should be parseable as a date
      const date = new Date(message.eventTime)
      expect(date.getTime()).not.toBeNaN()
    })
  })

  describe('eTag format', () => {
    it('should have quoted eTag', () => {
      const message = createR2EventMessage('data/users/1700001234-writer1-0.parquet')

      // eTags are typically quoted strings
      expect(message.object.eTag).toMatch(/^".*"$/)
    })
  })
})
