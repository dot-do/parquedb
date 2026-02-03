/**
 * Change Data Capture (CDC) Utilities
 *
 * Unified CDC record format that can be used by both ParqueDB and Delta Lake.
 * Enables cross-system interoperability and standard CDC consumers.
 *
 * Operations:
 * - 'c' (create): A new record was inserted
 * - 'u' (update): An existing record was modified
 * - 'd' (delete): A record was removed
 * - 'r' (read/snapshot): Bulk snapshot record
 */

// =============================================================================
// CDC TYPES
// =============================================================================

/**
 * CDC operation type
 */
export type CDCOperation = 'c' | 'u' | 'd' | 'r'

/**
 * CDC record structure
 * Generic over the document type T
 */
export interface CDCRecord<T = unknown> {
  /** Entity ID (document ID, message key, row PK) */
  _id: string

  /** Sequence number (LSN, Kafka offset, oplog ts) */
  _seq: bigint

  /** Operation type */
  _op: CDCOperation

  /** Previous state (null for create) */
  _before: T | null

  /** New state (null for delete) */
  _after: T | null

  /** Timestamp in nanoseconds */
  _ts: bigint

  /** Source metadata */
  _source: CDCSource

  /** Transaction ID (for exactly-once) */
  _txn?: string | undefined
}

/**
 * Source metadata for CDC records
 */
export interface CDCSource {
  /** Source system */
  system: 'parquedb' | 'deltalake' | 'mongolake' | 'kafkalake' | 'postgres' | 'mysql' | 'debezium'

  /** Database name */
  database?: string | undefined

  /** Collection/table/topic name */
  collection?: string | undefined

  /** Partition/shard ID */
  partition?: number | undefined

  /** Server ID (for multi-master) */
  serverId?: string | undefined
}

// =============================================================================
// DELTA LAKE CDC EXTENSIONS
// =============================================================================

/**
 * Delta Lake specific CDC change types
 * These match the Delta Lake CDC specification
 */
export type DeltaCDCChangeType =
  | 'insert'
  | 'update_preimage'
  | 'update_postimage'
  | 'delete'

/**
 * Delta Lake CDC Record structure
 * Contains the standard CDC metadata columns
 */
export interface DeltaCDCRecord<T = Record<string, unknown>> {
  /** The change type: insert, update_preimage, update_postimage, delete */
  _change_type: DeltaCDCChangeType
  /** The commit version that produced this change */
  _commit_version: bigint
  /** The timestamp of the commit */
  _commit_timestamp: Date
  /** The actual row data */
  data: T
}

/**
 * CDC Configuration for a table
 */
export interface CDCConfig {
  /** Whether CDC is enabled for this table */
  enabled: boolean
  /** Retention period for CDC files in milliseconds */
  retentionMs?: number | undefined
}

// =============================================================================
// CDC PRODUCER
// =============================================================================

/**
 * Options for creating a CDC producer
 */
export interface CDCProducerOptions {
  source: Omit<CDCSource, 'system'>
  system?: CDCSource['system'] | undefined
}

/**
 * CDC Producer for emitting change records
 */
export class CDCProducer<T = unknown> {
  private seq: bigint = 0n
  private source: CDCSource

  constructor(options: CDCProducerOptions) {
    this.source = {
      system: options.system ?? 'parquedb',
      ...options.source,
    }
  }

  /**
   * Emit a CDC record
   */
  async emit(
    op: CDCOperation,
    id: string,
    before: T | null,
    after: T | null,
    txn?: string
  ): Promise<CDCRecord<T>> {
    const record: CDCRecord<T> = {
      _id: id,
      _seq: this.seq++,
      _op: op,
      _before: before,
      _after: after,
      _ts: BigInt(Date.now()) * 1_000_000n, // nanoseconds
      _source: this.source,
    }
    if (txn) record._txn = txn

    return record
  }

  /**
   * Emit a create record
   */
  async create(id: string, data: T, txn?: string): Promise<CDCRecord<T>> {
    return this.emit('c', id, null, data, txn)
  }

  /**
   * Emit an update record
   */
  async update(id: string, before: T, after: T, txn?: string): Promise<CDCRecord<T>> {
    return this.emit('u', id, before, after, txn)
  }

  /**
   * Emit a delete record
   */
  async delete(id: string, before: T, txn?: string): Promise<CDCRecord<T>> {
    return this.emit('d', id, before, null, txn)
  }

  /**
   * Emit snapshot records (bulk 'r' operations)
   */
  async snapshot(records: Array<{ id: string; data: T }>): Promise<CDCRecord<T>[]> {
    return Promise.all(
      records.map(({ id, data }) => this.emit('r', id, null, data))
    )
  }

  /**
   * Get the current sequence number
   */
  getSequence(): bigint {
    return this.seq
  }

  /**
   * Reset the sequence counter (useful for testing)
   */
  resetSequence(seq: bigint = 0n): void {
    this.seq = seq
  }
}

// =============================================================================
// CDC CONSUMER
// =============================================================================

/**
 * Options for creating a CDC consumer
 */
export interface CDCConsumerOptions {
  /** Starting sequence number */
  fromSeq?: bigint | undefined

  /** Starting timestamp */
  fromTimestamp?: Date | undefined

  /** Filter by operation types */
  operations?: CDCOperation[] | undefined
}

/**
 * CDC Consumer for processing change records
 */
export class CDCConsumer<T = unknown> {
  private handlers: Array<(record: CDCRecord<T>) => Promise<void>> = []
  private position: bigint = 0n
  private options: CDCConsumerOptions

  constructor(options: CDCConsumerOptions = {}) {
    this.options = options
    if (options.fromSeq !== undefined) {
      this.position = options.fromSeq
    }
  }

  /**
   * Subscribe to CDC records
   */
  subscribe(handler: (record: CDCRecord<T>) => Promise<void>): () => void {
    this.handlers.push(handler)
    return () => {
      const idx = this.handlers.indexOf(handler)
      if (idx >= 0) this.handlers.splice(idx, 1)
    }
  }

  /**
   * Process a record
   */
  async process(record: CDCRecord<T>): Promise<void> {
    // Filter by operations
    if (this.options.operations && !this.options.operations.includes(record._op)) {
      return
    }

    // Filter by sequence
    if (record._seq < this.position) {
      return
    }

    // Filter by timestamp
    if (this.options.fromTimestamp) {
      const tsMs = Number(record._ts / 1_000_000n)
      if (tsMs < this.options.fromTimestamp.getTime()) {
        return
      }
    }

    // Notify handlers
    await Promise.all(this.handlers.map(h => h(record)))
    this.position = record._seq + 1n
  }

  /**
   * Seek to a specific position
   */
  seekTo(seq: bigint): void {
    this.position = seq
  }

  /**
   * Seek to a timestamp
   */
  seekToTimestamp(ts: Date): void {
    this.options.fromTimestamp = ts
  }

  /**
   * Get current position
   */
  getPosition(): bigint {
    return this.position
  }
}

// =============================================================================
// CDC UTILITIES
// =============================================================================

/**
 * Convert a CDC operation to Delta Lake change type
 */
export function cdcOpToDeltaChangeType(
  op: CDCOperation,
  isPreimage: boolean = false
): DeltaCDCChangeType {
  switch (op) {
    case 'c':
    case 'r':
      return 'insert'
    case 'u':
      return isPreimage ? 'update_preimage' : 'update_postimage'
    case 'd':
      return 'delete'
    default:
      return 'insert'
  }
}

/**
 * Convert Delta Lake change type to CDC operation
 */
export function deltaChangeTypeToCDCOp(changeType: DeltaCDCChangeType): CDCOperation {
  switch (changeType) {
    case 'insert':
      return 'c'
    case 'update_preimage':
    case 'update_postimage':
      return 'u'
    case 'delete':
      return 'd'
    default:
      return 'c'
  }
}

/**
 * Convert a CDCRecord to DeltaCDCRecords
 * Note: Updates produce two records (preimage and postimage)
 */
export function cdcRecordToDeltaRecords<T extends Record<string, unknown>>(
  record: CDCRecord<T>,
  commitVersion: bigint,
  commitTimestamp: Date
): DeltaCDCRecord<T>[] {
  const results: DeltaCDCRecord<T>[] = []

  if (record._op === 'u') {
    // Update produces preimage and postimage
    if (record._before !== null) {
      results.push({
        _change_type: 'update_preimage',
        _commit_version: commitVersion,
        _commit_timestamp: commitTimestamp,
        data: record._before as T,
      })
    }
    if (record._after !== null) {
      results.push({
        _change_type: 'update_postimage',
        _commit_version: commitVersion,
        _commit_timestamp: commitTimestamp,
        data: record._after as T,
      })
    }
  } else {
    // Create, delete, or snapshot
    const data = record._after ?? record._before
    if (data !== null) {
      results.push({
        _change_type: cdcOpToDeltaChangeType(record._op),
        _commit_version: commitVersion,
        _commit_timestamp: commitTimestamp,
        data: data as T,
      })
    }
  }

  return results
}

/**
 * Convert DeltaCDCRecord to CDCRecord
 */
export function deltaCDCRecordToCDCRecord<T>(
  deltaRecord: DeltaCDCRecord<T>,
  id: string,
  seq: bigint,
  source: CDCSource,
  preimage?: T
): CDCRecord<T> {
  const op = deltaChangeTypeToCDCOp(deltaRecord._change_type)

  return {
    _id: id,
    _seq: seq,
    _op: op,
    _before: deltaRecord._change_type === 'delete' || deltaRecord._change_type === 'update_preimage'
      ? deltaRecord.data
      : preimage ?? null,
    _after: deltaRecord._change_type === 'insert' || deltaRecord._change_type === 'update_postimage'
      ? deltaRecord.data
      : null,
    _ts: BigInt(deltaRecord._commit_timestamp.getTime()) * 1_000_000n,
    _source: source,
  }
}
