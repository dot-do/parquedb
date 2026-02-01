/**
 * Change Data Capture (CDC) Utilities Test Suite
 *
 * Tests for the CDC producer, consumer, and conversion utilities.
 * Covers record creation, filtering, and Delta Lake interoperability.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  // Classes
  CDCProducer,
  CDCConsumer,

  // Conversion utilities
  cdcOpToDeltaChangeType,
  deltaChangeTypeToCDCOp,
  cdcRecordToDeltaRecords,
  deltaCDCRecordToCDCRecord,

  // Types
  type CDCRecord,
  type CDCSource,
  type DeltaCDCRecord,
} from '../../../src/delta-utils/cdc'

// =============================================================================
// CDC PRODUCER TESTS
// =============================================================================

describe('CDCProducer', () => {
  interface TestEntity {
    id: string
    name: string
    email: string
  }

  let producer: CDCProducer<TestEntity>

  beforeEach(() => {
    producer = new CDCProducer<TestEntity>({
      source: {
        database: 'test_db',
        collection: 'users',
      },
    })
  })

  describe('constructor', () => {
    it('sets default system to parquedb', () => {
      const record = producer.create('user-1', { id: 'user-1', name: 'Test', email: 'test@example.com' })
      // We need to await since emit is async
    })

    it('allows custom system', () => {
      const customProducer = new CDCProducer<TestEntity>({
        system: 'deltalake',
        source: {
          database: 'test_db',
          collection: 'users',
        },
      })

      expect(customProducer).toBeDefined()
    })
  })

  describe('create', () => {
    it('emits create record with null before', async () => {
      const entity: TestEntity = { id: 'user-1', name: 'Alice', email: 'alice@example.com' }
      const record = await producer.create('user-1', entity)

      expect(record._id).toBe('user-1')
      expect(record._op).toBe('c')
      expect(record._before).toBeNull()
      expect(record._after).toEqual(entity)
    })

    it('increments sequence number', async () => {
      const entity: TestEntity = { id: 'user-1', name: 'Alice', email: 'alice@example.com' }

      const record1 = await producer.create('user-1', entity)
      const record2 = await producer.create('user-2', { ...entity, id: 'user-2' })

      expect(record1._seq).toBe(0n)
      expect(record2._seq).toBe(1n)
    })

    it('includes timestamp in nanoseconds', async () => {
      const before = BigInt(Date.now()) * 1_000_000n
      const record = await producer.create('user-1', { id: 'user-1', name: 'Alice', email: 'alice@example.com' })
      const after = BigInt(Date.now()) * 1_000_000n

      expect(record._ts).toBeGreaterThanOrEqual(before)
      expect(record._ts).toBeLessThanOrEqual(after)
    })

    it('includes source metadata', async () => {
      const record = await producer.create('user-1', { id: 'user-1', name: 'Alice', email: 'alice@example.com' })

      expect(record._source.system).toBe('parquedb')
      expect(record._source.database).toBe('test_db')
      expect(record._source.collection).toBe('users')
    })

    it('includes transaction ID when provided', async () => {
      const record = await producer.create('user-1', { id: 'user-1', name: 'Alice', email: 'alice@example.com' }, 'txn-123')

      expect(record._txn).toBe('txn-123')
    })
  })

  describe('update', () => {
    it('emits update record with before and after', async () => {
      const before: TestEntity = { id: 'user-1', name: 'Alice', email: 'alice@example.com' }
      const after: TestEntity = { id: 'user-1', name: 'Alice Smith', email: 'alice.smith@example.com' }

      const record = await producer.update('user-1', before, after)

      expect(record._id).toBe('user-1')
      expect(record._op).toBe('u')
      expect(record._before).toEqual(before)
      expect(record._after).toEqual(after)
    })

    it('includes transaction ID when provided', async () => {
      const before: TestEntity = { id: 'user-1', name: 'Alice', email: 'alice@example.com' }
      const after: TestEntity = { id: 'user-1', name: 'Alice Smith', email: 'alice.smith@example.com' }

      const record = await producer.update('user-1', before, after, 'txn-456')

      expect(record._txn).toBe('txn-456')
    })
  })

  describe('delete', () => {
    it('emits delete record with before and null after', async () => {
      const before: TestEntity = { id: 'user-1', name: 'Alice', email: 'alice@example.com' }

      const record = await producer.delete('user-1', before)

      expect(record._id).toBe('user-1')
      expect(record._op).toBe('d')
      expect(record._before).toEqual(before)
      expect(record._after).toBeNull()
    })

    it('includes transaction ID when provided', async () => {
      const before: TestEntity = { id: 'user-1', name: 'Alice', email: 'alice@example.com' }

      const record = await producer.delete('user-1', before, 'txn-789')

      expect(record._txn).toBe('txn-789')
    })
  })

  describe('snapshot', () => {
    it('emits snapshot records for bulk data', async () => {
      const entities = [
        { id: 'user-1', data: { id: 'user-1', name: 'Alice', email: 'alice@example.com' } },
        { id: 'user-2', data: { id: 'user-2', name: 'Bob', email: 'bob@example.com' } },
        { id: 'user-3', data: { id: 'user-3', name: 'Charlie', email: 'charlie@example.com' } },
      ]

      const records = await producer.snapshot(entities)

      expect(records.length).toBe(3)
      expect(records.every(r => r._op === 'r')).toBe(true)
      expect(records.every(r => r._before === null)).toBe(true)
      expect(records[0]._after?.id).toBe('user-1')
      expect(records[1]._after?.id).toBe('user-2')
      expect(records[2]._after?.id).toBe('user-3')
    })

    it('handles empty snapshot', async () => {
      const records = await producer.snapshot([])

      expect(records).toEqual([])
    })
  })

  describe('getSequence', () => {
    it('returns current sequence number', async () => {
      expect(producer.getSequence()).toBe(0n)

      await producer.create('user-1', { id: 'user-1', name: 'Alice', email: 'alice@example.com' })
      expect(producer.getSequence()).toBe(1n)

      await producer.create('user-2', { id: 'user-2', name: 'Bob', email: 'bob@example.com' })
      expect(producer.getSequence()).toBe(2n)
    })
  })

  describe('resetSequence', () => {
    it('resets sequence to 0 by default', async () => {
      await producer.create('user-1', { id: 'user-1', name: 'Alice', email: 'alice@example.com' })
      await producer.create('user-2', { id: 'user-2', name: 'Bob', email: 'bob@example.com' })

      producer.resetSequence()

      expect(producer.getSequence()).toBe(0n)
    })

    it('resets sequence to specified value', async () => {
      producer.resetSequence(100n)

      expect(producer.getSequence()).toBe(100n)

      const record = await producer.create('user-1', { id: 'user-1', name: 'Alice', email: 'alice@example.com' })
      expect(record._seq).toBe(100n)
      expect(producer.getSequence()).toBe(101n)
    })
  })
})

// =============================================================================
// CDC CONSUMER TESTS
// =============================================================================

describe('CDCConsumer', () => {
  interface TestEntity {
    id: string
    name: string
  }

  const createTestRecord = (overrides: Partial<CDCRecord<TestEntity>> = {}): CDCRecord<TestEntity> => ({
    _id: 'test-1',
    _seq: 0n,
    _op: 'c',
    _before: null,
    _after: { id: 'test-1', name: 'Test' },
    _ts: BigInt(Date.now()) * 1_000_000n,
    _source: { system: 'parquedb' },
    ...overrides,
  })

  describe('constructor', () => {
    it('creates consumer with default options', () => {
      const consumer = new CDCConsumer<TestEntity>()
      expect(consumer.getPosition()).toBe(0n)
    })

    it('creates consumer with fromSeq option', () => {
      const consumer = new CDCConsumer<TestEntity>({ fromSeq: 100n })
      expect(consumer.getPosition()).toBe(100n)
    })
  })

  describe('subscribe', () => {
    it('adds handler that receives records', async () => {
      const consumer = new CDCConsumer<TestEntity>()
      const received: CDCRecord<TestEntity>[] = []

      consumer.subscribe(async (record) => {
        received.push(record)
      })

      const record = createTestRecord()
      await consumer.process(record)

      expect(received.length).toBe(1)
      expect(received[0]._id).toBe('test-1')
    })

    it('returns unsubscribe function', async () => {
      const consumer = new CDCConsumer<TestEntity>()
      const received: CDCRecord<TestEntity>[] = []

      const unsubscribe = consumer.subscribe(async (record) => {
        received.push(record)
      })

      await consumer.process(createTestRecord({ _seq: 0n }))
      expect(received.length).toBe(1)

      unsubscribe()

      await consumer.process(createTestRecord({ _seq: 1n }))
      expect(received.length).toBe(1) // Still 1, handler was unsubscribed
    })

    it('supports multiple handlers', async () => {
      const consumer = new CDCConsumer<TestEntity>()
      const received1: CDCRecord<TestEntity>[] = []
      const received2: CDCRecord<TestEntity>[] = []

      consumer.subscribe(async (record) => {
        received1.push(record)
      })
      consumer.subscribe(async (record) => {
        received2.push(record)
      })

      await consumer.process(createTestRecord())

      expect(received1.length).toBe(1)
      expect(received2.length).toBe(1)
    })
  })

  describe('process', () => {
    it('filters by operation types', async () => {
      const consumer = new CDCConsumer<TestEntity>({
        operations: ['c', 'd'],
      })
      const received: CDCRecord<TestEntity>[] = []

      consumer.subscribe(async (record) => {
        received.push(record)
      })

      await consumer.process(createTestRecord({ _op: 'c', _seq: 0n }))
      await consumer.process(createTestRecord({ _op: 'u', _seq: 1n }))
      await consumer.process(createTestRecord({ _op: 'd', _seq: 2n }))
      await consumer.process(createTestRecord({ _op: 'r', _seq: 3n }))

      expect(received.length).toBe(2)
      expect(received.map(r => r._op)).toEqual(['c', 'd'])
    })

    it('filters by sequence number', async () => {
      const consumer = new CDCConsumer<TestEntity>({ fromSeq: 5n })
      const received: CDCRecord<TestEntity>[] = []

      consumer.subscribe(async (record) => {
        received.push(record)
      })

      await consumer.process(createTestRecord({ _seq: 3n }))
      await consumer.process(createTestRecord({ _seq: 5n }))
      await consumer.process(createTestRecord({ _seq: 7n }))

      expect(received.length).toBe(2)
      expect(received.map(r => r._seq)).toEqual([5n, 7n])
    })

    it('filters by timestamp', async () => {
      const now = Date.now()
      const consumer = new CDCConsumer<TestEntity>({
        fromTimestamp: new Date(now),
      })
      const received: CDCRecord<TestEntity>[] = []

      consumer.subscribe(async (record) => {
        received.push(record)
      })

      await consumer.process(createTestRecord({
        _seq: 0n,
        _ts: BigInt(now - 1000) * 1_000_000n, // 1 second before
      }))
      await consumer.process(createTestRecord({
        _seq: 1n,
        _ts: BigInt(now + 1000) * 1_000_000n, // 1 second after
      }))

      expect(received.length).toBe(1)
      expect(received[0]._seq).toBe(1n)
    })

    it('updates position after processing', async () => {
      const consumer = new CDCConsumer<TestEntity>()

      await consumer.process(createTestRecord({ _seq: 5n }))
      expect(consumer.getPosition()).toBe(6n)

      await consumer.process(createTestRecord({ _seq: 10n }))
      expect(consumer.getPosition()).toBe(11n)
    })
  })

  describe('seekTo', () => {
    it('sets position to specified sequence', async () => {
      const consumer = new CDCConsumer<TestEntity>()
      const received: CDCRecord<TestEntity>[] = []

      consumer.subscribe(async (record) => {
        received.push(record)
      })

      // Process some records
      await consumer.process(createTestRecord({ _seq: 0n }))
      await consumer.process(createTestRecord({ _seq: 1n }))
      expect(consumer.getPosition()).toBe(2n)

      // Seek back
      consumer.seekTo(0n)
      expect(consumer.getPosition()).toBe(0n)

      // Now can process from beginning again
      await consumer.process(createTestRecord({ _seq: 0n }))
      expect(received.length).toBe(3)
    })
  })

  describe('seekToTimestamp', () => {
    it('sets timestamp filter', async () => {
      const consumer = new CDCConsumer<TestEntity>()
      const received: CDCRecord<TestEntity>[] = []

      consumer.subscribe(async (record) => {
        received.push(record)
      })

      const cutoff = new Date()
      consumer.seekToTimestamp(cutoff)

      // Record before cutoff should be filtered
      await consumer.process(createTestRecord({
        _seq: 0n,
        _ts: BigInt(cutoff.getTime() - 1000) * 1_000_000n,
      }))

      // Record after cutoff should pass
      await consumer.process(createTestRecord({
        _seq: 1n,
        _ts: BigInt(cutoff.getTime() + 1000) * 1_000_000n,
      }))

      expect(received.length).toBe(1)
    })
  })
})

// =============================================================================
// CDC CONVERSION UTILITIES TESTS
// =============================================================================

describe('CDC Conversion Utilities', () => {
  describe('cdcOpToDeltaChangeType', () => {
    it('converts create to insert', () => {
      expect(cdcOpToDeltaChangeType('c')).toBe('insert')
    })

    it('converts snapshot to insert', () => {
      expect(cdcOpToDeltaChangeType('r')).toBe('insert')
    })

    it('converts update to update_postimage by default', () => {
      expect(cdcOpToDeltaChangeType('u')).toBe('update_postimage')
    })

    it('converts update to update_preimage when isPreimage is true', () => {
      expect(cdcOpToDeltaChangeType('u', true)).toBe('update_preimage')
    })

    it('converts delete to delete', () => {
      expect(cdcOpToDeltaChangeType('d')).toBe('delete')
    })

    it('defaults to insert for unknown operation', () => {
      expect(cdcOpToDeltaChangeType('x' as any)).toBe('insert')
    })
  })

  describe('deltaChangeTypeToCDCOp', () => {
    it('converts insert to create', () => {
      expect(deltaChangeTypeToCDCOp('insert')).toBe('c')
    })

    it('converts update_preimage to update', () => {
      expect(deltaChangeTypeToCDCOp('update_preimage')).toBe('u')
    })

    it('converts update_postimage to update', () => {
      expect(deltaChangeTypeToCDCOp('update_postimage')).toBe('u')
    })

    it('converts delete to delete', () => {
      expect(deltaChangeTypeToCDCOp('delete')).toBe('d')
    })

    it('defaults to create for unknown type', () => {
      expect(deltaChangeTypeToCDCOp('unknown' as any)).toBe('c')
    })
  })

  describe('cdcRecordToDeltaRecords', () => {
    const source: CDCSource = { system: 'parquedb', database: 'test', collection: 'users' }
    const commitVersion = 1n
    const commitTimestamp = new Date('2024-01-15T10:00:00Z')

    it('converts create record to single insert', () => {
      const cdcRecord: CDCRecord<{ name: string }> = {
        _id: 'user-1',
        _seq: 0n,
        _op: 'c',
        _before: null,
        _after: { name: 'Alice' },
        _ts: 1000n,
        _source: source,
      }

      const deltaRecords = cdcRecordToDeltaRecords(cdcRecord, commitVersion, commitTimestamp)

      expect(deltaRecords.length).toBe(1)
      expect(deltaRecords[0]._change_type).toBe('insert')
      expect(deltaRecords[0].data).toEqual({ name: 'Alice' })
      expect(deltaRecords[0]._commit_version).toBe(1n)
      expect(deltaRecords[0]._commit_timestamp).toEqual(commitTimestamp)
    })

    it('converts update record to preimage and postimage', () => {
      const cdcRecord: CDCRecord<{ name: string }> = {
        _id: 'user-1',
        _seq: 0n,
        _op: 'u',
        _before: { name: 'Alice' },
        _after: { name: 'Alice Smith' },
        _ts: 1000n,
        _source: source,
      }

      const deltaRecords = cdcRecordToDeltaRecords(cdcRecord, commitVersion, commitTimestamp)

      expect(deltaRecords.length).toBe(2)
      expect(deltaRecords[0]._change_type).toBe('update_preimage')
      expect(deltaRecords[0].data).toEqual({ name: 'Alice' })
      expect(deltaRecords[1]._change_type).toBe('update_postimage')
      expect(deltaRecords[1].data).toEqual({ name: 'Alice Smith' })
    })

    it('handles update with null before', () => {
      const cdcRecord: CDCRecord<{ name: string }> = {
        _id: 'user-1',
        _seq: 0n,
        _op: 'u',
        _before: null,
        _after: { name: 'Alice Smith' },
        _ts: 1000n,
        _source: source,
      }

      const deltaRecords = cdcRecordToDeltaRecords(cdcRecord, commitVersion, commitTimestamp)

      expect(deltaRecords.length).toBe(1)
      expect(deltaRecords[0]._change_type).toBe('update_postimage')
    })

    it('handles update with null after', () => {
      const cdcRecord: CDCRecord<{ name: string }> = {
        _id: 'user-1',
        _seq: 0n,
        _op: 'u',
        _before: { name: 'Alice' },
        _after: null,
        _ts: 1000n,
        _source: source,
      }

      const deltaRecords = cdcRecordToDeltaRecords(cdcRecord, commitVersion, commitTimestamp)

      expect(deltaRecords.length).toBe(1)
      expect(deltaRecords[0]._change_type).toBe('update_preimage')
    })

    it('converts delete record to single delete', () => {
      const cdcRecord: CDCRecord<{ name: string }> = {
        _id: 'user-1',
        _seq: 0n,
        _op: 'd',
        _before: { name: 'Alice' },
        _after: null,
        _ts: 1000n,
        _source: source,
      }

      const deltaRecords = cdcRecordToDeltaRecords(cdcRecord, commitVersion, commitTimestamp)

      expect(deltaRecords.length).toBe(1)
      expect(deltaRecords[0]._change_type).toBe('delete')
      expect(deltaRecords[0].data).toEqual({ name: 'Alice' })
    })

    it('converts snapshot record to insert', () => {
      const cdcRecord: CDCRecord<{ name: string }> = {
        _id: 'user-1',
        _seq: 0n,
        _op: 'r',
        _before: null,
        _after: { name: 'Alice' },
        _ts: 1000n,
        _source: source,
      }

      const deltaRecords = cdcRecordToDeltaRecords(cdcRecord, commitVersion, commitTimestamp)

      expect(deltaRecords.length).toBe(1)
      expect(deltaRecords[0]._change_type).toBe('insert')
    })

    it('returns empty array when both before and after are null for non-update', () => {
      const cdcRecord: CDCRecord<{ name: string }> = {
        _id: 'user-1',
        _seq: 0n,
        _op: 'c',
        _before: null,
        _after: null,
        _ts: 1000n,
        _source: source,
      }

      const deltaRecords = cdcRecordToDeltaRecords(cdcRecord, commitVersion, commitTimestamp)

      expect(deltaRecords.length).toBe(0)
    })
  })

  describe('deltaCDCRecordToCDCRecord', () => {
    const source: CDCSource = { system: 'deltalake', database: 'test', collection: 'users' }

    it('converts insert to create', () => {
      const deltaRecord: DeltaCDCRecord<{ name: string }> = {
        _change_type: 'insert',
        _commit_version: 1n,
        _commit_timestamp: new Date('2024-01-15T10:00:00Z'),
        data: { name: 'Alice' },
      }

      const cdcRecord = deltaCDCRecordToCDCRecord(deltaRecord, 'user-1', 0n, source)

      expect(cdcRecord._id).toBe('user-1')
      expect(cdcRecord._seq).toBe(0n)
      expect(cdcRecord._op).toBe('c')
      expect(cdcRecord._before).toBeNull()
      expect(cdcRecord._after).toEqual({ name: 'Alice' })
      expect(cdcRecord._source).toEqual(source)
    })

    it('converts delete to delete', () => {
      const deltaRecord: DeltaCDCRecord<{ name: string }> = {
        _change_type: 'delete',
        _commit_version: 1n,
        _commit_timestamp: new Date('2024-01-15T10:00:00Z'),
        data: { name: 'Alice' },
      }

      const cdcRecord = deltaCDCRecordToCDCRecord(deltaRecord, 'user-1', 0n, source)

      expect(cdcRecord._op).toBe('d')
      expect(cdcRecord._before).toEqual({ name: 'Alice' })
      expect(cdcRecord._after).toBeNull()
    })

    it('converts update_preimage to update with before', () => {
      const deltaRecord: DeltaCDCRecord<{ name: string }> = {
        _change_type: 'update_preimage',
        _commit_version: 1n,
        _commit_timestamp: new Date('2024-01-15T10:00:00Z'),
        data: { name: 'Alice' },
      }

      const cdcRecord = deltaCDCRecordToCDCRecord(deltaRecord, 'user-1', 0n, source)

      expect(cdcRecord._op).toBe('u')
      expect(cdcRecord._before).toEqual({ name: 'Alice' })
      expect(cdcRecord._after).toBeNull()
    })

    it('converts update_postimage to update with after', () => {
      const deltaRecord: DeltaCDCRecord<{ name: string }> = {
        _change_type: 'update_postimage',
        _commit_version: 1n,
        _commit_timestamp: new Date('2024-01-15T10:00:00Z'),
        data: { name: 'Alice Smith' },
      }

      const cdcRecord = deltaCDCRecordToCDCRecord(deltaRecord, 'user-1', 0n, source)

      expect(cdcRecord._op).toBe('u')
      expect(cdcRecord._before).toBeNull()
      expect(cdcRecord._after).toEqual({ name: 'Alice Smith' })
    })

    it('includes preimage when provided for update_postimage', () => {
      const deltaRecord: DeltaCDCRecord<{ name: string }> = {
        _change_type: 'update_postimage',
        _commit_version: 1n,
        _commit_timestamp: new Date('2024-01-15T10:00:00Z'),
        data: { name: 'Alice Smith' },
      }

      const preimage = { name: 'Alice' }
      const cdcRecord = deltaCDCRecordToCDCRecord(deltaRecord, 'user-1', 0n, source, preimage)

      expect(cdcRecord._before).toEqual({ name: 'Alice' })
      expect(cdcRecord._after).toEqual({ name: 'Alice Smith' })
    })

    it('converts timestamp correctly', () => {
      const timestamp = new Date('2024-01-15T10:00:00Z')
      const deltaRecord: DeltaCDCRecord<{ name: string }> = {
        _change_type: 'insert',
        _commit_version: 1n,
        _commit_timestamp: timestamp,
        data: { name: 'Alice' },
      }

      const cdcRecord = deltaCDCRecordToCDCRecord(deltaRecord, 'user-1', 0n, source)

      // Should convert to nanoseconds
      expect(cdcRecord._ts).toBe(BigInt(timestamp.getTime()) * 1_000_000n)
    })
  })
})

// =============================================================================
// EDGE CASES AND ERROR HANDLING
// =============================================================================

describe('CDC Edge Cases', () => {
  describe('CDCProducer with complex data', () => {
    it('handles nested objects', async () => {
      interface NestedEntity {
        id: string
        profile: {
          name: string
          address: {
            city: string
            country: string
          }
        }
      }

      const producer = new CDCProducer<NestedEntity>({
        source: { collection: 'users' },
      })

      const entity: NestedEntity = {
        id: 'user-1',
        profile: {
          name: 'Alice',
          address: {
            city: 'New York',
            country: 'USA',
          },
        },
      }

      const record = await producer.create('user-1', entity)

      expect(record._after).toEqual(entity)
      expect(record._after?.profile.address.city).toBe('New York')
    })

    it('handles arrays', async () => {
      interface EntityWithArray {
        id: string
        tags: string[]
      }

      const producer = new CDCProducer<EntityWithArray>({
        source: { collection: 'items' },
      })

      const entity: EntityWithArray = {
        id: 'item-1',
        tags: ['featured', 'new', 'sale'],
      }

      const record = await producer.create('item-1', entity)

      expect(record._after?.tags).toEqual(['featured', 'new', 'sale'])
    })

    it('handles null values in data', async () => {
      interface EntityWithNull {
        id: string
        optional: string | null
      }

      const producer = new CDCProducer<EntityWithNull>({
        source: { collection: 'items' },
      })

      const entity: EntityWithNull = {
        id: 'item-1',
        optional: null,
      }

      const record = await producer.create('item-1', entity)

      expect(record._after?.optional).toBeNull()
    })
  })

  describe('CDCConsumer edge cases', () => {
    it('handles concurrent subscriptions', async () => {
      const consumer = new CDCConsumer<{ id: string }>()
      const results: number[] = []

      // Add multiple handlers
      for (let i = 0; i < 5; i++) {
        const idx = i
        consumer.subscribe(async () => {
          results.push(idx)
        })
      }

      await consumer.process({
        _id: 'test',
        _seq: 0n,
        _op: 'c',
        _before: null,
        _after: { id: 'test' },
        _ts: 1000n,
        _source: { system: 'parquedb' },
      })

      expect(results.length).toBe(5)
      expect(results).toEqual([0, 1, 2, 3, 4])
    })

    it('handles rapid unsubscribe/resubscribe', async () => {
      const consumer = new CDCConsumer<{ id: string }>()
      const results: string[] = []

      const unsubscribe1 = consumer.subscribe(async () => {
        results.push('handler1')
      })

      const record = {
        _id: 'test',
        _seq: 0n,
        _op: 'c' as const,
        _before: null,
        _after: { id: 'test' },
        _ts: 1000n,
        _source: { system: 'parquedb' as const },
      }

      await consumer.process(record)
      expect(results).toEqual(['handler1'])

      unsubscribe1()

      const unsubscribe2 = consumer.subscribe(async () => {
        results.push('handler2')
      })

      consumer.seekTo(0n) // Reset position to process same record
      await consumer.process(record)
      expect(results).toEqual(['handler1', 'handler2'])

      unsubscribe2()
    })
  })
})
