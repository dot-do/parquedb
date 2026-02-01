/**
 * Transaction Log Utilities Test Suite
 *
 * Tests for the Delta Lake-compatible transaction log utilities.
 * Covers serialization, version formatting, action validation, and type guards.
 */

import { describe, it, expect } from 'vitest'
import {
  // Serialization
  serializeAction,
  parseAction,
  serializeCommit,
  parseCommit,

  // Version handling
  formatVersion,
  parseVersionFromFilename,
  getLogFilePath,
  getCheckpointPath,

  // Validation
  validateAction,

  // Type guards
  isAddAction,
  isRemoveAction,
  isMetadataAction,
  isProtocolAction,
  isCommitInfoAction,

  // Stats
  parseStats,
  encodeStats,

  // Action creation
  createAddAction,
  createRemoveAction,

  // Types
  type AddAction,
  type RemoveAction,
  type MetadataAction,
  type ProtocolAction,
  type CommitInfoAction,
  type LogAction,
  type FileStats,
} from '../../../src/delta-utils/transaction-log'

// =============================================================================
// SERIALIZATION TESTS
// =============================================================================

describe('Transaction Log Serialization', () => {
  describe('serializeAction', () => {
    it('serializes add action to JSON', () => {
      const action: AddAction = {
        add: {
          path: 'data/part-0001.parquet',
          size: 1024,
          modificationTime: 1700000000000,
          dataChange: true,
        },
      }

      const json = serializeAction(action)
      const parsed = JSON.parse(json)

      expect(parsed.add.path).toBe('data/part-0001.parquet')
      expect(parsed.add.size).toBe(1024)
      expect(parsed.add.modificationTime).toBe(1700000000000)
      expect(parsed.add.dataChange).toBe(true)
    })

    it('serializes remove action to JSON', () => {
      const action: RemoveAction = {
        remove: {
          path: 'data/part-0001.parquet',
          deletionTimestamp: 1700000000000,
          dataChange: true,
        },
      }

      const json = serializeAction(action)
      const parsed = JSON.parse(json)

      expect(parsed.remove.path).toBe('data/part-0001.parquet')
      expect(parsed.remove.deletionTimestamp).toBe(1700000000000)
    })

    it('serializes metadata action to JSON', () => {
      const action: MetadataAction = {
        metaData: {
          id: 'table-123',
          name: 'test_table',
          description: 'A test table',
          format: { provider: 'parquet' },
          schemaString: '{}',
          partitionColumns: ['date'],
        },
      }

      const json = serializeAction(action)
      const parsed = JSON.parse(json)

      expect(parsed.metaData.id).toBe('table-123')
      expect(parsed.metaData.name).toBe('test_table')
    })

    it('serializes protocol action to JSON', () => {
      const action: ProtocolAction = {
        protocol: {
          minReaderVersion: 1,
          minWriterVersion: 2,
        },
      }

      const json = serializeAction(action)
      const parsed = JSON.parse(json)

      expect(parsed.protocol.minReaderVersion).toBe(1)
      expect(parsed.protocol.minWriterVersion).toBe(2)
    })

    it('serializes commitInfo action to JSON', () => {
      const action: CommitInfoAction = {
        commitInfo: {
          timestamp: 1700000000000,
          operation: 'WRITE',
          operationParameters: { mode: 'Append' },
          readVersion: 5,
        },
      }

      const json = serializeAction(action)
      const parsed = JSON.parse(json)

      expect(parsed.commitInfo.operation).toBe('WRITE')
      expect(parsed.commitInfo.readVersion).toBe(5)
    })
  })

  describe('parseAction', () => {
    it('parses add action from JSON', () => {
      const json = '{"add":{"path":"data/file.parquet","size":100,"modificationTime":1000,"dataChange":true}}'
      const action = parseAction(json)

      expect(isAddAction(action)).toBe(true)
      if (isAddAction(action)) {
        expect(action.add.path).toBe('data/file.parquet')
      }
    })

    it('parses remove action from JSON', () => {
      const json = '{"remove":{"path":"data/file.parquet","deletionTimestamp":1000,"dataChange":true}}'
      const action = parseAction(json)

      expect(isRemoveAction(action)).toBe(true)
    })

    it('throws on empty string', () => {
      expect(() => parseAction('')).toThrow('Cannot parse empty JSON string')
    })

    it('throws on whitespace only', () => {
      expect(() => parseAction('   ')).toThrow('Cannot parse empty JSON string')
    })

    it('throws on invalid JSON', () => {
      expect(() => parseAction('not json')).toThrow()
    })

    it('throws on array input', () => {
      expect(() => parseAction('[1, 2, 3]')).toThrow('Action must be a JSON object')
    })

    it('throws on primitive input', () => {
      expect(() => parseAction('42')).toThrow('Action must be a JSON object')
      expect(() => parseAction('"string"')).toThrow('Action must be a JSON object')
      expect(() => parseAction('null')).toThrow('Action must be a JSON object')
    })

    it('throws on unrecognized action type', () => {
      expect(() => parseAction('{"unknown":{}}')).toThrow('JSON must contain a recognized action type')
    })
  })

  describe('serializeCommit', () => {
    it('serializes multiple actions to NDJSON', () => {
      const actions: LogAction[] = [
        {
          protocol: { minReaderVersion: 1, minWriterVersion: 2 },
        },
        {
          add: {
            path: 'data/file.parquet',
            size: 100,
            modificationTime: 1000,
            dataChange: true,
          },
        },
      ]

      const ndjson = serializeCommit(actions)
      const lines = ndjson.split('\n')

      expect(lines.length).toBe(2)
      expect(JSON.parse(lines[0]).protocol).toBeDefined()
      expect(JSON.parse(lines[1]).add).toBeDefined()
    })

    it('handles empty array', () => {
      const ndjson = serializeCommit([])
      expect(ndjson).toBe('')
    })

    it('handles single action', () => {
      const actions: LogAction[] = [
        { protocol: { minReaderVersion: 1, minWriterVersion: 2 } },
      ]

      const ndjson = serializeCommit(actions)
      expect(ndjson.split('\n').length).toBe(1)
    })
  })

  describe('parseCommit', () => {
    it('parses NDJSON to actions', () => {
      const ndjson = '{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}\n{"add":{"path":"data/file.parquet","size":100,"modificationTime":1000,"dataChange":true}}'

      const actions = parseCommit(ndjson)

      expect(actions.length).toBe(2)
      expect(isProtocolAction(actions[0])).toBe(true)
      expect(isAddAction(actions[1])).toBe(true)
    })

    it('handles CRLF line endings', () => {
      const ndjson = '{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}\r\n{"add":{"path":"file.parquet","size":100,"modificationTime":1000,"dataChange":true}}'

      const actions = parseCommit(ndjson)
      expect(actions.length).toBe(2)
    })

    it('ignores empty lines', () => {
      const ndjson = '{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}\n\n{"add":{"path":"file.parquet","size":100,"modificationTime":1000,"dataChange":true}}\n'

      const actions = parseCommit(ndjson)
      expect(actions.length).toBe(2)
    })

    it('handles empty input', () => {
      const actions = parseCommit('')
      expect(actions).toEqual([])
    })
  })
})

// =============================================================================
// VERSION FORMATTING TESTS
// =============================================================================

describe('Transaction Log Version Formatting', () => {
  describe('formatVersion', () => {
    it('formats version 0', () => {
      expect(formatVersion(0)).toBe('00000000000000000000')
    })

    it('formats single digit version', () => {
      expect(formatVersion(5)).toBe('00000000000000000005')
    })

    it('formats large version', () => {
      expect(formatVersion(12345678901234)).toBe('00000012345678901234')
    })

    it('handles bigint', () => {
      expect(formatVersion(1000000000000000n)).toBe('00001000000000000000')
    })

    it('throws on negative number', () => {
      expect(() => formatVersion(-1)).toThrow('Version number cannot be negative')
    })

    it('throws on version exceeding 20 digits', () => {
      expect(() => formatVersion(BigInt('123456789012345678901'))).toThrow('Version number exceeds 20 digits')
    })
  })

  describe('parseVersionFromFilename', () => {
    it('parses version from valid filename', () => {
      expect(parseVersionFromFilename('00000000000000000042.json')).toBe(42)
    })

    it('parses version from full path', () => {
      expect(parseVersionFromFilename('_delta_log/00000000000000000123.json')).toBe(123)
    })

    it('parses version 0', () => {
      expect(parseVersionFromFilename('00000000000000000000.json')).toBe(0)
    })

    it('throws on invalid format - wrong extension', () => {
      expect(() => parseVersionFromFilename('00000000000000000042.parquet')).toThrow('Invalid log file name format')
    })

    it('throws on invalid format - wrong digit count', () => {
      expect(() => parseVersionFromFilename('0000000000000000042.json')).toThrow('Invalid log file name format')
    })

    it('throws on invalid format - no digits', () => {
      expect(() => parseVersionFromFilename('commit.json')).toThrow('Invalid log file name format')
    })
  })

  describe('getLogFilePath', () => {
    it('returns log file path for table', () => {
      expect(getLogFilePath('my_table', 0)).toBe('my_table/_delta_log/00000000000000000000.json')
    })

    it('handles trailing slash in table path', () => {
      expect(getLogFilePath('my_table/', 5)).toBe('my_table/_delta_log/00000000000000000005.json')
    })

    it('handles empty table path', () => {
      expect(getLogFilePath('', 0)).toBe('_delta_log/00000000000000000000.json')
    })

    it('handles nested table path', () => {
      expect(getLogFilePath('data/tables/orders', 10)).toBe('data/tables/orders/_delta_log/00000000000000000010.json')
    })
  })

  describe('getCheckpointPath', () => {
    it('returns checkpoint path', () => {
      expect(getCheckpointPath('my_table', 10)).toBe('my_table/_delta_log/00000000000000000010.checkpoint.parquet')
    })

    it('handles trailing slash', () => {
      expect(getCheckpointPath('my_table/', 10)).toBe('my_table/_delta_log/00000000000000000010.checkpoint.parquet')
    })
  })
})

// =============================================================================
// ACTION VALIDATION TESTS
// =============================================================================

describe('Transaction Log Action Validation', () => {
  describe('validateAction - add', () => {
    it('validates valid add action', () => {
      const action: AddAction = {
        add: {
          path: 'data/file.parquet',
          size: 100,
          modificationTime: 1000,
          dataChange: true,
        },
      }

      const result = validateAction(action)
      expect(result.valid).toBe(true)
      expect(result.errors).toEqual([])
    })

    it('fails on empty path', () => {
      const action: AddAction = {
        add: {
          path: '',
          size: 100,
          modificationTime: 1000,
          dataChange: true,
        },
      }

      const result = validateAction(action)
      expect(result.valid).toBe(false)
      expect(result.errors).toContain('add.path must not be empty')
    })

    it('fails on negative size', () => {
      const action: AddAction = {
        add: {
          path: 'file.parquet',
          size: -1,
          modificationTime: 1000,
          dataChange: true,
        },
      }

      const result = validateAction(action)
      expect(result.valid).toBe(false)
      expect(result.errors).toContain('add.size must be non-negative')
    })

    it('fails on negative modificationTime', () => {
      const action: AddAction = {
        add: {
          path: 'file.parquet',
          size: 100,
          modificationTime: -1,
          dataChange: true,
        },
      }

      const result = validateAction(action)
      expect(result.valid).toBe(false)
      expect(result.errors).toContain('add.modificationTime must be non-negative')
    })

    it('fails on invalid stats JSON', () => {
      const action: AddAction = {
        add: {
          path: 'file.parquet',
          size: 100,
          modificationTime: 1000,
          dataChange: true,
          stats: 'not valid json',
        },
      }

      const result = validateAction(action)
      expect(result.valid).toBe(false)
      expect(result.errors).toContain('add.stats must be valid JSON')
    })

    it('passes with valid stats JSON', () => {
      const action: AddAction = {
        add: {
          path: 'file.parquet',
          size: 100,
          modificationTime: 1000,
          dataChange: true,
          stats: '{"numRecords":100}',
        },
      }

      const result = validateAction(action)
      expect(result.valid).toBe(true)
    })
  })

  describe('validateAction - remove', () => {
    it('validates valid remove action', () => {
      const action: RemoveAction = {
        remove: {
          path: 'data/file.parquet',
          deletionTimestamp: 1000,
          dataChange: true,
        },
      }

      const result = validateAction(action)
      expect(result.valid).toBe(true)
    })

    it('fails on empty path', () => {
      const action: RemoveAction = {
        remove: {
          path: '',
          deletionTimestamp: 1000,
          dataChange: true,
        },
      }

      const result = validateAction(action)
      expect(result.valid).toBe(false)
      expect(result.errors).toContain('remove.path must not be empty')
    })

    it('fails on negative deletionTimestamp', () => {
      const action: RemoveAction = {
        remove: {
          path: 'file.parquet',
          deletionTimestamp: -1,
          dataChange: true,
        },
      }

      const result = validateAction(action)
      expect(result.valid).toBe(false)
      expect(result.errors).toContain('remove.deletionTimestamp must be non-negative')
    })
  })

  describe('validateAction - metaData', () => {
    it('validates valid metadata action', () => {
      const action: MetadataAction = {
        metaData: {
          id: 'table-123',
          format: { provider: 'parquet' },
          schemaString: '{"type":"struct"}',
          partitionColumns: [],
        },
      }

      const result = validateAction(action)
      expect(result.valid).toBe(true)
    })

    it('fails on empty id', () => {
      const action: MetadataAction = {
        metaData: {
          id: '',
          format: { provider: 'parquet' },
          schemaString: '{}',
          partitionColumns: [],
        },
      }

      const result = validateAction(action)
      expect(result.valid).toBe(false)
      expect(result.errors).toContain('metaData.id must not be empty')
    })

    it('fails on empty provider', () => {
      const action: MetadataAction = {
        metaData: {
          id: 'table-123',
          format: { provider: '' },
          schemaString: '{}',
          partitionColumns: [],
        },
      }

      const result = validateAction(action)
      expect(result.valid).toBe(false)
      expect(result.errors).toContain('metaData.format.provider must not be empty')
    })

    it('fails on invalid schemaString JSON', () => {
      const action: MetadataAction = {
        metaData: {
          id: 'table-123',
          format: { provider: 'parquet' },
          schemaString: 'not json',
          partitionColumns: [],
        },
      }

      const result = validateAction(action)
      expect(result.valid).toBe(false)
      expect(result.errors).toContain('metaData.schemaString must be valid JSON')
    })
  })

  describe('validateAction - protocol', () => {
    it('validates valid protocol action', () => {
      const action: ProtocolAction = {
        protocol: {
          minReaderVersion: 1,
          minWriterVersion: 2,
        },
      }

      const result = validateAction(action)
      expect(result.valid).toBe(true)
    })

    it('fails on minReaderVersion < 1', () => {
      const action: ProtocolAction = {
        protocol: {
          minReaderVersion: 0,
          minWriterVersion: 1,
        },
      }

      const result = validateAction(action)
      expect(result.valid).toBe(false)
      expect(result.errors).toContain('protocol.minReaderVersion must be at least 1')
    })

    it('fails on minWriterVersion < 1', () => {
      const action: ProtocolAction = {
        protocol: {
          minReaderVersion: 1,
          minWriterVersion: 0,
        },
      }

      const result = validateAction(action)
      expect(result.valid).toBe(false)
      expect(result.errors).toContain('protocol.minWriterVersion must be at least 1')
    })

    it('fails on non-integer versions', () => {
      const action: ProtocolAction = {
        protocol: {
          minReaderVersion: 1.5,
          minWriterVersion: 2.5,
        },
      }

      const result = validateAction(action)
      expect(result.valid).toBe(false)
      expect(result.errors).toContain('protocol.minReaderVersion must be an integer')
      expect(result.errors).toContain('protocol.minWriterVersion must be an integer')
    })
  })

  describe('validateAction - commitInfo', () => {
    it('validates valid commitInfo action', () => {
      const action: CommitInfoAction = {
        commitInfo: {
          timestamp: 1700000000000,
          operation: 'WRITE',
        },
      }

      const result = validateAction(action)
      expect(result.valid).toBe(true)
    })

    it('fails on negative timestamp', () => {
      const action: CommitInfoAction = {
        commitInfo: {
          timestamp: -1,
          operation: 'WRITE',
        },
      }

      const result = validateAction(action)
      expect(result.valid).toBe(false)
      expect(result.errors).toContain('commitInfo.timestamp must be non-negative')
    })

    it('fails on empty operation', () => {
      const action: CommitInfoAction = {
        commitInfo: {
          timestamp: 1000,
          operation: '',
        },
      }

      const result = validateAction(action)
      expect(result.valid).toBe(false)
      expect(result.errors).toContain('commitInfo.operation must not be empty')
    })

    it('fails on negative readVersion', () => {
      const action: CommitInfoAction = {
        commitInfo: {
          timestamp: 1000,
          operation: 'WRITE',
          readVersion: -1,
        },
      }

      const result = validateAction(action)
      expect(result.valid).toBe(false)
      expect(result.errors).toContain('commitInfo.readVersion must be non-negative')
    })
  })
})

// =============================================================================
// TYPE GUARD TESTS
// =============================================================================

describe('Transaction Log Type Guards', () => {
  describe('isAddAction', () => {
    it('returns true for add action', () => {
      const action: LogAction = {
        add: { path: 'file.parquet', size: 100, modificationTime: 1000, dataChange: true },
      }
      expect(isAddAction(action)).toBe(true)
    })

    it('returns false for remove action', () => {
      const action: LogAction = {
        remove: { path: 'file.parquet', deletionTimestamp: 1000, dataChange: true },
      }
      expect(isAddAction(action)).toBe(false)
    })

    it('returns false for null', () => {
      expect(isAddAction(null as unknown as LogAction)).toBe(false)
    })

    it('returns false for non-object', () => {
      expect(isAddAction('string' as unknown as LogAction)).toBe(false)
    })

    it('returns false when add is not an object', () => {
      const action = { add: 'not an object' } as unknown as LogAction
      expect(isAddAction(action)).toBe(false)
    })
  })

  describe('isRemoveAction', () => {
    it('returns true for remove action', () => {
      const action: LogAction = {
        remove: { path: 'file.parquet', deletionTimestamp: 1000, dataChange: true },
      }
      expect(isRemoveAction(action)).toBe(true)
    })

    it('returns false for add action', () => {
      const action: LogAction = {
        add: { path: 'file.parquet', size: 100, modificationTime: 1000, dataChange: true },
      }
      expect(isRemoveAction(action)).toBe(false)
    })

    it('returns false when remove is not an object', () => {
      const action = { remove: null } as unknown as LogAction
      expect(isRemoveAction(action)).toBe(false)
    })
  })

  describe('isMetadataAction', () => {
    it('returns true for metadata action', () => {
      const action: LogAction = {
        metaData: { id: 'table', format: { provider: 'parquet' }, schemaString: '{}', partitionColumns: [] },
      }
      expect(isMetadataAction(action)).toBe(true)
    })

    it('returns false for other actions', () => {
      const action: LogAction = {
        protocol: { minReaderVersion: 1, minWriterVersion: 2 },
      }
      expect(isMetadataAction(action)).toBe(false)
    })
  })

  describe('isProtocolAction', () => {
    it('returns true for protocol action', () => {
      const action: LogAction = {
        protocol: { minReaderVersion: 1, minWriterVersion: 2 },
      }
      expect(isProtocolAction(action)).toBe(true)
    })

    it('returns false for other actions', () => {
      const action: LogAction = {
        commitInfo: { timestamp: 1000, operation: 'WRITE' },
      }
      expect(isProtocolAction(action)).toBe(false)
    })
  })

  describe('isCommitInfoAction', () => {
    it('returns true for commitInfo action', () => {
      const action: LogAction = {
        commitInfo: { timestamp: 1000, operation: 'WRITE' },
      }
      expect(isCommitInfoAction(action)).toBe(true)
    })

    it('returns false for other actions', () => {
      const action: LogAction = {
        add: { path: 'file.parquet', size: 100, modificationTime: 1000, dataChange: true },
      }
      expect(isCommitInfoAction(action)).toBe(false)
    })
  })
})

// =============================================================================
// STATS PARSING TESTS
// =============================================================================

describe('Transaction Log Stats', () => {
  describe('parseStats', () => {
    it('parses valid stats JSON', () => {
      const stats: FileStats = parseStats('{"numRecords":100,"minValues":{"id":1},"maxValues":{"id":100},"nullCount":{"id":0}}')

      expect(stats.numRecords).toBe(100)
      expect(stats.minValues.id).toBe(1)
      expect(stats.maxValues.id).toBe(100)
      expect(stats.nullCount.id).toBe(0)
    })

    it('throws on missing numRecords', () => {
      expect(() => parseStats('{"minValues":{}}')).toThrow('numRecords is required')
    })

    it('throws on null numRecords', () => {
      expect(() => parseStats('{"numRecords":null}')).toThrow('numRecords is required')
    })

    it('throws on invalid JSON', () => {
      expect(() => parseStats('not json')).toThrow()
    })
  })

  describe('encodeStats', () => {
    it('encodes stats to JSON', () => {
      const stats: FileStats = {
        numRecords: 50,
        minValues: { name: 'Alice' },
        maxValues: { name: 'Zoe' },
        nullCount: { name: 5 },
      }

      const json = encodeStats(stats)
      const parsed = JSON.parse(json)

      expect(parsed.numRecords).toBe(50)
      expect(parsed.minValues.name).toBe('Alice')
    })
  })

  describe('parseStats + encodeStats roundtrip', () => {
    it('roundtrips stats correctly', () => {
      const original: FileStats = {
        numRecords: 1000,
        minValues: { id: 1, date: '2024-01-01' },
        maxValues: { id: 1000, date: '2024-12-31' },
        nullCount: { id: 0, date: 10 },
      }

      const encoded = encodeStats(original)
      const decoded = parseStats(encoded)

      expect(decoded).toEqual(original)
    })
  })
})

// =============================================================================
// ACTION CREATION TESTS
// =============================================================================

describe('Transaction Log Action Creation', () => {
  describe('createAddAction', () => {
    it('creates valid add action', () => {
      const action = createAddAction({
        path: 'data/part-0001.parquet',
        size: 1024,
        modificationTime: 1700000000000,
        dataChange: true,
      })

      expect(action.add.path).toBe('data/part-0001.parquet')
      expect(action.add.size).toBe(1024)
      expect(action.add.modificationTime).toBe(1700000000000)
      expect(action.add.dataChange).toBe(true)
    })

    it('includes optional partition values', () => {
      const action = createAddAction({
        path: 'data/date=2024-01-01/part-0001.parquet',
        size: 1024,
        modificationTime: 1000,
        dataChange: true,
        partitionValues: { date: '2024-01-01' },
      })

      expect(action.add.partitionValues).toEqual({ date: '2024-01-01' })
    })

    it('includes optional stats', () => {
      const action = createAddAction({
        path: 'data/part-0001.parquet',
        size: 1024,
        modificationTime: 1000,
        dataChange: true,
        stats: {
          numRecords: 100,
          minValues: { id: 1 },
          maxValues: { id: 100 },
          nullCount: { id: 0 },
        },
      })

      expect(action.add.stats).toBeDefined()
      const parsedStats = parseStats(action.add.stats!)
      expect(parsedStats.numRecords).toBe(100)
    })

    it('includes optional tags', () => {
      const action = createAddAction({
        path: 'data/part-0001.parquet',
        size: 1024,
        modificationTime: 1000,
        dataChange: true,
        tags: { INSERTION_TIME: '1700000000000000' },
      })

      expect(action.add.tags).toEqual({ INSERTION_TIME: '1700000000000000' })
    })

    it('throws on absolute path', () => {
      expect(() =>
        createAddAction({
          path: '/absolute/path.parquet',
          size: 1024,
          modificationTime: 1000,
          dataChange: true,
        })
      ).toThrow('path must be relative')
    })

    it('throws on parent directory traversal', () => {
      expect(() =>
        createAddAction({
          path: 'data/../../../etc/passwd',
          size: 1024,
          modificationTime: 1000,
          dataChange: true,
        })
      ).toThrow('path cannot contain parent directory traversal')
    })

    it('throws on path starting with ./', () => {
      expect(() =>
        createAddAction({
          path: './data/file.parquet',
          size: 1024,
          modificationTime: 1000,
          dataChange: true,
        })
      ).toThrow('path should not start with ./')
    })

    it('throws on non-integer size', () => {
      expect(() =>
        createAddAction({
          path: 'data/file.parquet',
          size: 1024.5,
          modificationTime: 1000,
          dataChange: true,
        })
      ).toThrow('size must be an integer')
    })

    it('throws on non-integer modificationTime', () => {
      expect(() =>
        createAddAction({
          path: 'data/file.parquet',
          size: 1024,
          modificationTime: 1000.5,
          dataChange: true,
        })
      ).toThrow('modificationTime must be an integer')
    })

    it('throws on negative stats numRecords', () => {
      expect(() =>
        createAddAction({
          path: 'data/file.parquet',
          size: 1024,
          modificationTime: 1000,
          dataChange: true,
          stats: {
            numRecords: -1,
            minValues: {},
            maxValues: {},
            nullCount: {},
          },
        })
      ).toThrow('numRecords must be non-negative')
    })

    it('throws on negative nullCount', () => {
      expect(() =>
        createAddAction({
          path: 'data/file.parquet',
          size: 1024,
          modificationTime: 1000,
          dataChange: true,
          stats: {
            numRecords: 100,
            minValues: {},
            maxValues: {},
            nullCount: { id: -5 },
          },
        })
      ).toThrow('nullCount values must be non-negative')
    })

    it('throws when nullCount exceeds numRecords', () => {
      expect(() =>
        createAddAction({
          path: 'data/file.parquet',
          size: 1024,
          modificationTime: 1000,
          dataChange: true,
          stats: {
            numRecords: 100,
            minValues: {},
            maxValues: {},
            nullCount: { id: 150 },
          },
        })
      ).toThrow('nullCount cannot exceed numRecords')
    })
  })

  describe('createRemoveAction', () => {
    it('creates valid remove action', () => {
      const action = createRemoveAction({
        path: 'data/part-0001.parquet',
        deletionTimestamp: 1700000000000,
        dataChange: true,
      })

      expect(action.remove.path).toBe('data/part-0001.parquet')
      expect(action.remove.deletionTimestamp).toBe(1700000000000)
      expect(action.remove.dataChange).toBe(true)
    })

    it('includes optional partition values', () => {
      const action = createRemoveAction({
        path: 'data/date=2024-01-01/part-0001.parquet',
        deletionTimestamp: 1000,
        dataChange: true,
        partitionValues: { date: '2024-01-01' },
      })

      expect(action.remove.partitionValues).toEqual({ date: '2024-01-01' })
    })

    it('includes optional extended file metadata flag', () => {
      const action = createRemoveAction({
        path: 'data/part-0001.parquet',
        deletionTimestamp: 1000,
        dataChange: true,
        extendedFileMetadata: true,
      })

      expect(action.remove.extendedFileMetadata).toBe(true)
    })

    it('includes optional size', () => {
      const action = createRemoveAction({
        path: 'data/part-0001.parquet',
        deletionTimestamp: 1000,
        dataChange: true,
        size: 2048,
      })

      expect(action.remove.size).toBe(2048)
    })
  })
})
