/**
 * Delta Lake Table Format for ParqueDB MergeTree Engine
 *
 * Writes compacted data as Delta Lake-compatible tables with:
 * - Transaction log in _delta_log/ directory
 * - Standard Parquet data files
 * - Compatible with Spark, Databricks, DuckDB readers
 *
 * Delta Lake Format Overview:
 *   table/
 *   +-- _delta_log/
 *   |   +-- 00000000000000000000.json   (protocol + metaData)
 *   |   +-- 00000000000000000001.json   (add/remove actions)
 *   +-- data/
 *       +-- part-00000-{uuid}.parquet
 *
 * Each JSON file in _delta_log/ uses JSON Lines format (one action per line).
 */

import { encodeDataToParquet } from '../parquet-encoders'

// =============================================================================
// Configuration
// =============================================================================

export interface DeltaConfig {
  /** Base path for the table (e.g., 'tables/users') */
  basePath: string
  /** Table name */
  tableName: string
}

// =============================================================================
// Delta Lake Action Types
// =============================================================================

export interface DeltaProtocol {
  minReaderVersion: number
  minWriterVersion: number
}

export interface DeltaMetaData {
  id: string
  name: string
  format: { provider: 'parquet'; options: Record<string, string> }
  schemaString: string
  partitionColumns: string[]
  configuration: Record<string, string>
  createdTime: number
}

export interface DeltaAdd {
  path: string
  size: number
  partitionValues: Record<string, string>
  modificationTime: number
  dataChange: boolean
  stats: string // JSON string with numRecords, minValues, maxValues, nullCount
}

export interface DeltaRemove {
  path: string
  deletionTimestamp: number
  dataChange: boolean
}

export type DeltaAction =
  | { protocol: DeltaProtocol }
  | { metaData: DeltaMetaData }
  | { add: DeltaAdd }
  | { remove: DeltaRemove }

export interface DeltaTransaction {
  version: number
  actions: DeltaAction[]
}

// =============================================================================
// DeltaFormat
// =============================================================================

export class DeltaFormat {
  private config: DeltaConfig
  private currentVersion = -1

  constructor(config: DeltaConfig) {
    this.config = config
  }

  /** Generate the default schema for MergeTree data tables as Delta schema JSON */
  getSchemaString(): string {
    return JSON.stringify({
      type: 'struct',
      fields: [
        { name: '$id', type: 'string', nullable: false, metadata: {} },
        { name: '$op', type: 'string', nullable: false, metadata: {} },
        { name: '$v', type: 'integer', nullable: false, metadata: {} },
        { name: '$ts', type: 'double', nullable: false, metadata: {} },
        { name: '$data', type: 'string', nullable: true, metadata: {} },
      ],
    })
  }

  /** Create the initial transaction (version 0) with protocol + metaData actions */
  createInitialTransaction(): DeltaTransaction {
    this.currentVersion = 0

    const protocol: DeltaAction = {
      protocol: {
        minReaderVersion: 1,
        minWriterVersion: 2,
      },
    }

    const metaData: DeltaAction = {
      metaData: {
        id: crypto.randomUUID(),
        name: this.config.tableName,
        format: { provider: 'parquet', options: {} },
        schemaString: this.getSchemaString(),
        partitionColumns: [],
        configuration: {},
        createdTime: Date.now(),
      },
    }

    return {
      version: 0,
      actions: [protocol, metaData],
    }
  }

  /**
   * Create a data transaction from compacted data.
   * Returns the transaction log entry and the data file artifacts.
   *
   * @param data - Compacted data to write
   * @param previousDataPaths - Paths of data files from previous version (to generate remove actions)
   */
  async createDataTransaction(
    data: Array<{ $id: string; $op: string; $v: number; $ts: number; [key: string]: unknown }>,
    previousDataPaths?: string[],
  ): Promise<{
    transaction: DeltaTransaction
    dataPath: string
    dataBuffer: ArrayBuffer
  }> {
    this.currentVersion++

    // Encode data to Parquet
    const dataBuffer = await encodeDataToParquet(data)

    // Generate data file path
    const uuid = crypto.randomUUID()
    const dataPath = `data/part-00000-${uuid}.parquet`

    // Calculate stats from data
    const stats = data.length > 0
      ? {
          numRecords: data.length,
          minValues: { $id: data.reduce((min, d) => (d.$id < min ? d.$id : min), data[0].$id) },
          maxValues: { $id: data.reduce((max, d) => (d.$id > max ? d.$id : max), data[0].$id) },
          nullCount: { $id: 0, $op: 0, $v: 0, $ts: 0, $data: 0 },
        }
      : { numRecords: 0, minValues: {}, maxValues: {} }

    const now = Date.now()

    // Build actions
    const actions: DeltaAction[] = []

    // Remove actions for previous data files
    if (previousDataPaths && previousDataPaths.length > 0) {
      for (const path of previousDataPaths) {
        actions.push({
          remove: {
            path,
            deletionTimestamp: now,
            dataChange: true,
          },
        })
      }
    }

    // Add action for new data file
    actions.push({
      add: {
        path: dataPath,
        size: dataBuffer.byteLength,
        partitionValues: {},
        modificationTime: now,
        dataChange: true,
        stats: JSON.stringify(stats),
      },
    })

    return {
      transaction: {
        version: this.currentVersion,
        actions,
      },
      dataPath,
      dataBuffer,
    }
  }

  /** Format a transaction version number as a zero-padded 20-digit string */
  formatVersion(version: number): string {
    return String(version).padStart(20, '0')
  }

  /** Get the log path for a given version */
  getLogPath(version: number): string {
    return `${this.config.basePath}/_delta_log/${this.formatVersion(version)}.json`
  }

  /** Serialize a transaction to Delta log format (one JSON action per line) */
  serializeTransaction(transaction: DeltaTransaction): string {
    return transaction.actions.map((a) => JSON.stringify(a)).join('\n') + '\n'
  }

  /** Get the current transaction version */
  get version(): number {
    return this.currentVersion
  }
}
