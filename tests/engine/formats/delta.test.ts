/**
 * Delta Lake Table Format Test Suite
 *
 * Tests the DeltaFormat class which produces Delta Lake-compatible
 * transaction logs and Parquet data files for the MergeTree engine.
 *
 * Verifies:
 * - Schema generation (Delta struct format)
 * - Initial transaction (protocol + metaData)
 * - Data transactions (add + remove actions, Parquet encoding)
 * - Transaction log serialization (JSON Lines)
 * - Version management and path generation
 */

import { describe, it, expect } from 'vitest'
import { DeltaFormat } from '@/engine/formats/delta'
import type { DeltaAction, DeltaTransaction } from '@/engine/formats/delta'

// =============================================================================
// Helpers
// =============================================================================

/** Decode a Parquet ArrayBuffer into rows using hyparquet */
async function decodeParquet(buffer: ArrayBuffer): Promise<Array<Record<string, unknown>>> {
  const { parquetReadObjects } = await import('hyparquet')
  const asyncBuffer = {
    byteLength: buffer.byteLength,
    slice: async (start: number, end?: number) => buffer.slice(start, end ?? buffer.byteLength),
  }
  return parquetReadObjects({ file: asyncBuffer }) as Promise<Array<Record<string, unknown>>>
}

/** Create a DeltaFormat instance with default test config */
function createDelta(overrides?: { basePath?: string; tableName?: string }): DeltaFormat {
  return new DeltaFormat({
    basePath: overrides?.basePath ?? 'tables/users',
    tableName: overrides?.tableName ?? 'users',
  })
}

/** Helper to create test data entries */
function makeData(
  id: string,
  overrides?: Partial<{ $op: string; $v: number; $ts: number; [key: string]: unknown }>,
) {
  return {
    $id: id,
    $op: 'c' as string,
    $v: 1,
    $ts: 1000,
    ...overrides,
  }
}

/** Type guard for protocol action */
function isProtocol(action: DeltaAction): action is { protocol: { minReaderVersion: number; minWriterVersion: number } } {
  return 'protocol' in action
}

/** Type guard for metaData action */
function isMetaData(action: DeltaAction): action is { metaData: { id: string; name: string; format: { provider: 'parquet'; options: Record<string, string> }; schemaString: string; partitionColumns: string[]; configuration: Record<string, string>; createdTime: number } } {
  return 'metaData' in action
}

/** Type guard for add action */
function isAdd(action: DeltaAction): action is { add: { path: string; size: number; partitionValues: Record<string, string>; modificationTime: number; dataChange: boolean; stats: string } } {
  return 'add' in action
}

/** Type guard for remove action */
function isRemove(action: DeltaAction): action is { remove: { path: string; deletionTimestamp: number; dataChange: boolean } } {
  return 'remove' in action
}

// =============================================================================
// Schema Tests
// =============================================================================

describe('DeltaFormat schema', () => {
  it('1. getSchemaString returns valid Delta schema JSON', () => {
    const delta = createDelta()
    const schemaString = delta.getSchemaString()

    const schema = JSON.parse(schemaString)
    expect(schema.type).toBe('struct')
    expect(schema.fields).toBeInstanceOf(Array)
    expect(schema.fields).toHaveLength(5)

    // Verify field names and types
    const fieldNames = schema.fields.map((f: { name: string }) => f.name)
    expect(fieldNames).toEqual(['$id', '$op', '$v', '$ts', '$data'])

    // Verify $id field
    const idField = schema.fields[0]
    expect(idField.type).toBe('string')
    expect(idField.nullable).toBe(false)
    expect(idField.metadata).toEqual({})

    // Verify $v field is integer
    const vField = schema.fields[2]
    expect(vField.type).toBe('integer')

    // Verify $ts field is double
    const tsField = schema.fields[3]
    expect(tsField.type).toBe('double')

    // Verify $data is nullable
    const dataField = schema.fields[4]
    expect(dataField.nullable).toBe(true)
  })
})

// =============================================================================
// Initial Transaction Tests
// =============================================================================

describe('DeltaFormat createInitialTransaction', () => {
  it('2. createInitialTransaction has version 0', () => {
    const delta = createDelta()
    const tx = delta.createInitialTransaction()

    expect(tx.version).toBe(0)
  })

  it('3. createInitialTransaction has protocol action', () => {
    const delta = createDelta()
    const tx = delta.createInitialTransaction()

    const protocolAction = tx.actions.find(isProtocol)
    expect(protocolAction).toBeDefined()
    expect(protocolAction!.protocol.minReaderVersion).toBe(1)
    expect(protocolAction!.protocol.minWriterVersion).toBe(2)
  })

  it('4. createInitialTransaction has metaData action with correct schema', () => {
    const delta = createDelta({ tableName: 'orders' })
    const tx = delta.createInitialTransaction()

    const metaDataAction = tx.actions.find(isMetaData)
    expect(metaDataAction).toBeDefined()

    const md = metaDataAction!.metaData
    expect(md.name).toBe('orders')
    expect(md.format.provider).toBe('parquet')
    expect(md.format.options).toEqual({})
    expect(md.partitionColumns).toEqual([])
    expect(md.configuration).toEqual({})
    expect(md.createdTime).toBeGreaterThan(0)

    // Verify schemaString is valid and matches getSchemaString
    const schema = JSON.parse(md.schemaString)
    expect(schema.type).toBe('struct')
    expect(schema.fields).toHaveLength(5)

    // Verify ID is a valid UUID
    expect(md.id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/)
  })
})

// =============================================================================
// Data Transaction Tests
// =============================================================================

describe('DeltaFormat createDataTransaction', () => {
  it('5. createDataTransaction increments version', async () => {
    const delta = createDelta()
    delta.createInitialTransaction() // version 0

    const result1 = await delta.createDataTransaction([makeData('u1')])
    expect(result1.transaction.version).toBe(1)
    expect(delta.version).toBe(1)

    const result2 = await delta.createDataTransaction([makeData('u2')])
    expect(result2.transaction.version).toBe(2)
    expect(delta.version).toBe(2)
  })

  it('6. createDataTransaction encodes data to valid Parquet', async () => {
    const delta = createDelta()
    delta.createInitialTransaction()

    const data = [
      makeData('u1', { $ts: 1000, name: 'Alice', email: 'alice@example.com' }),
      makeData('u2', { $ts: 2000, name: 'Bob' }),
    ]

    const result = await delta.createDataTransaction(data)

    // dataBuffer is a valid Parquet file
    expect(result.dataBuffer).toBeInstanceOf(ArrayBuffer)
    expect(result.dataBuffer.byteLength).toBeGreaterThan(0)

    // Verify PAR1 magic bytes
    const view = new Uint8Array(result.dataBuffer)
    expect(view[0]).toBe(0x50) // P
    expect(view[1]).toBe(0x41) // A
    expect(view[2]).toBe(0x52) // R
    expect(view[3]).toBe(0x31) // 1

    // Decode and verify contents
    const rows = await decodeParquet(result.dataBuffer)
    expect(rows).toHaveLength(2)

    // Data is sorted by $id from encodeDataToParquet
    expect(rows[0].$id).toBe('u1')
    expect(rows[1].$id).toBe('u2')
  })

  it('7. createDataTransaction generates add action with stats', async () => {
    const delta = createDelta()
    delta.createInitialTransaction()

    const data = [
      makeData('u3', { $ts: 3000 }),
      makeData('u1', { $ts: 1000 }),
      makeData('u2', { $ts: 2000 }),
    ]

    const result = await delta.createDataTransaction(data)
    const addAction = result.transaction.actions.find(isAdd)

    expect(addAction).toBeDefined()

    const add = addAction!.add
    expect(add.path).toMatch(/^data\/part-00000-[0-9a-f-]+\.parquet$/)
    expect(add.path).toBe(result.dataPath)
    expect(add.size).toBe(result.dataBuffer.byteLength)
    expect(add.partitionValues).toEqual({})
    expect(add.modificationTime).toBeGreaterThan(0)
    expect(add.dataChange).toBe(true)

    // Verify stats JSON
    const stats = JSON.parse(add.stats)
    expect(stats.numRecords).toBe(3)
    expect(stats.minValues.$id).toBe('u1')
    expect(stats.maxValues.$id).toBe('u3')
    expect(stats.nullCount).toEqual({ $id: 0, $op: 0, $v: 0, $ts: 0, $data: 0 })
  })

  it('8. createDataTransaction generates remove actions for previous files', async () => {
    const delta = createDelta()
    delta.createInitialTransaction()

    const previousPaths = [
      'data/part-00000-old-uuid-1.parquet',
      'data/part-00000-old-uuid-2.parquet',
    ]

    const result = await delta.createDataTransaction(
      [makeData('u1')],
      previousPaths,
    )

    const removeActions = result.transaction.actions.filter(isRemove)
    expect(removeActions).toHaveLength(2)

    expect(removeActions[0].remove.path).toBe('data/part-00000-old-uuid-1.parquet')
    expect(removeActions[0].remove.dataChange).toBe(true)
    expect(removeActions[0].remove.deletionTimestamp).toBeGreaterThan(0)

    expect(removeActions[1].remove.path).toBe('data/part-00000-old-uuid-2.parquet')
    expect(removeActions[1].remove.dataChange).toBe(true)

    // Also has an add action for the new file
    const addAction = result.transaction.actions.find(isAdd)
    expect(addAction).toBeDefined()
  })

  it('handles empty data array without crashing', async () => {
    const delta = createDelta()
    delta.createInitialTransaction() // version 0

    // Should not throw - this is the core regression test for parquedb-zou5.2
    const result = await delta.createDataTransaction([])
    expect(result).toBeDefined()
    expect(result.transaction).toBeDefined()
    expect(result.transaction.actions).toBeDefined()

    // Version should still increment
    expect(result.transaction.version).toBe(1)
    expect(delta.version).toBe(1)

    // The add action should have numRecords: 0 with empty min/max
    const addAction = result.transaction.actions.find(isAdd)
    expect(addAction).toBeDefined()
    if (addAction) {
      const stats = JSON.parse(addAction.add.stats)
      expect(stats.numRecords).toBe(0)
      expect(stats.minValues).toEqual({})
      expect(stats.maxValues).toEqual({})
    }

    // dataBuffer should be a valid (possibly empty-content) Parquet file
    expect(result.dataBuffer).toBeInstanceOf(ArrayBuffer)
    expect(result.dataBuffer.byteLength).toBeGreaterThan(0)

    // dataPath should still be a valid path
    expect(result.dataPath).toMatch(/^data\/part-00000-[0-9a-f-]+\.parquet$/)
  })

  it('handles empty data array with previousDataPaths', async () => {
    const delta = createDelta()
    delta.createInitialTransaction()

    const previousPaths = ['data/part-00000-old-uuid.parquet']

    // Should not crash even with empty data and previous paths to remove
    const result = await delta.createDataTransaction([], previousPaths)
    expect(result).toBeDefined()

    // Should have a remove action for the previous file
    const removeActions = result.transaction.actions.filter(isRemove)
    expect(removeActions).toHaveLength(1)
    expect(removeActions[0].remove.path).toBe('data/part-00000-old-uuid.parquet')

    // Should still have an add action for the (empty) new file
    const addAction = result.transaction.actions.find(isAdd)
    expect(addAction).toBeDefined()
    if (addAction) {
      const stats = JSON.parse(addAction.add.stats)
      expect(stats.numRecords).toBe(0)
    }
  })
})

// =============================================================================
// Serialization Tests
// =============================================================================

describe('DeltaFormat serializeTransaction', () => {
  it('9. serializeTransaction produces valid JSON Lines format', () => {
    const delta = createDelta()
    const tx = delta.createInitialTransaction()

    const serialized = delta.serializeTransaction(tx)

    // Should end with newline
    expect(serialized.endsWith('\n')).toBe(true)

    // Split into lines (last element after split is empty due to trailing newline)
    const lines = serialized.trim().split('\n')
    expect(lines).toHaveLength(2) // protocol + metaData

    // Each line is valid JSON
    const action1 = JSON.parse(lines[0])
    const action2 = JSON.parse(lines[1])

    // First line is protocol
    expect(action1.protocol).toBeDefined()
    expect(action1.protocol.minReaderVersion).toBe(1)
    expect(action1.protocol.minWriterVersion).toBe(2)

    // Second line is metaData
    expect(action2.metaData).toBeDefined()
    expect(action2.metaData.format.provider).toBe('parquet')
  })
})

// =============================================================================
// Version and Path Tests
// =============================================================================

describe('DeltaFormat formatVersion', () => {
  it('10. formatVersion pads to 20 digits', () => {
    const delta = createDelta()

    expect(delta.formatVersion(0)).toBe('00000000000000000000')
    expect(delta.formatVersion(1)).toBe('00000000000000000001')
    expect(delta.formatVersion(42)).toBe('00000000000000000042')
    expect(delta.formatVersion(12345)).toBe('00000000000000012345')
    expect(delta.formatVersion(99999999999999999)).toBe('00100000000000000000') // JS number precision
  })
})

describe('DeltaFormat getLogPath', () => {
  it('11. getLogPath returns correct Delta log path', () => {
    const delta = createDelta({ basePath: 'tables/users' })

    expect(delta.getLogPath(0)).toBe('tables/users/_delta_log/00000000000000000000.json')
    expect(delta.getLogPath(1)).toBe('tables/users/_delta_log/00000000000000000001.json')
    expect(delta.getLogPath(42)).toBe('tables/users/_delta_log/00000000000000000042.json')
  })
})

// =============================================================================
// Multi-Transaction Tests
// =============================================================================

describe('DeltaFormat multi-transaction', () => {
  it('12. multiple transactions create incrementing versions', async () => {
    const delta = createDelta()

    // Version 0: initial
    const tx0 = delta.createInitialTransaction()
    expect(tx0.version).toBe(0)
    expect(delta.version).toBe(0)

    // Version 1: first data
    const result1 = await delta.createDataTransaction([
      makeData('u1', { $ts: 1000, name: 'Alice' }),
    ])
    expect(result1.transaction.version).toBe(1)
    expect(delta.version).toBe(1)

    // Version 2: second data (replacing first)
    const result2 = await delta.createDataTransaction(
      [
        makeData('u1', { $ts: 2000, name: 'Alice Updated' }),
        makeData('u2', { $ts: 2000, name: 'Bob' }),
      ],
      [result1.dataPath],
    )
    expect(result2.transaction.version).toBe(2)
    expect(delta.version).toBe(2)

    // Version 2 should have a remove for the version 1 data path
    const removeActions = result2.transaction.actions.filter(isRemove)
    expect(removeActions).toHaveLength(1)
    expect(removeActions[0].remove.path).toBe(result1.dataPath)

    // Version 2 should have an add for new data
    const addActions = result2.transaction.actions.filter(isAdd)
    expect(addActions).toHaveLength(1)
    expect(addActions[0].add.path).toBe(result2.dataPath)

    // Decode version 2 data - should have 2 records
    const rows = await decodeParquet(result2.dataBuffer)
    expect(rows).toHaveLength(2)

    // Log paths are correctly constructed
    expect(delta.getLogPath(0)).toContain('00000000000000000000.json')
    expect(delta.getLogPath(1)).toContain('00000000000000000001.json')
    expect(delta.getLogPath(2)).toContain('00000000000000000002.json')

    // Serialize each transaction and verify they're valid JSON Lines
    for (const tx of [tx0, result1.transaction, result2.transaction]) {
      const serialized = delta.serializeTransaction(tx)
      const lines = serialized.trim().split('\n')
      for (const line of lines) {
        expect(() => JSON.parse(line)).not.toThrow()
      }
    }
  })
})
