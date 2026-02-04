/**
 * Delta Lake Utilities
 *
 * Shared utilities for working with Delta Lake format.
 * Used by both the DeltaBackend and the compaction workflow.
 */

// =============================================================================
// Types
// =============================================================================

/** Delta Lake protocol action */
export interface ProtocolAction {
  protocol: {
    minReaderVersion: number
    minWriterVersion: number
  }
}

/** Delta Lake metadata action */
export interface MetadataAction {
  metaData: {
    id: string
    name?: string | undefined
    schemaString: string
    partitionColumns: string[]
    configuration?: Record<string, string> | undefined
    createdTime?: number | undefined
  }
}

/** Delta Lake add action */
export interface AddAction {
  add: {
    path: string
    size: number
    modificationTime: number
    dataChange: boolean
    stats?: string | undefined
    partitionValues?: Record<string, string> | undefined
    tags?: Record<string, string> | undefined
  }
}

/** Delta Lake remove action */
export interface RemoveAction {
  remove: {
    path: string
    deletionTimestamp?: number | undefined
    dataChange: boolean
    extendedFileMetadata?: boolean | undefined
    partitionValues?: Record<string, string> | undefined
  }
}

/** Delta Lake commit info action */
export interface CommitInfoAction {
  commitInfo: {
    timestamp: number
    operation: string
    operationParameters?: Record<string, unknown> | undefined
    readVersion?: number | undefined
    isolationLevel?: string | undefined
    isBlindAppend?: boolean | undefined
    operationMetrics?: Record<string, string> | undefined
    engineInfo?: string | undefined
  }
}

/** Union of all action types */
export type LogAction = ProtocolAction | MetadataAction | AddAction | RemoveAction | CommitInfoAction

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Format version number as zero-padded 20-digit string
 */
export function formatVersion(version: number): string {
  return version.toString().padStart(20, '0')
}

/**
 * Serialize commit actions to Delta log format (NDJSON)
 */
export function serializeCommit(actions: LogAction[]): string {
  return actions.map(a => JSON.stringify(a)).join('\n')
}

/**
 * Create a protocol action
 */
export function createProtocolAction(
  minReaderVersion: number = 1,
  minWriterVersion: number = 2
): ProtocolAction {
  return {
    protocol: {
      minReaderVersion,
      minWriterVersion,
    },
  }
}

/**
 * Create a metadata action with the default entity schema
 */
export function createMetadataAction(tableId: string): MetadataAction {
  return {
    metaData: {
      id: tableId,
      schemaString: JSON.stringify({
        type: 'struct',
        fields: [
          { name: '$id', type: 'string', nullable: false },
          { name: '$type', type: 'string', nullable: false },
          { name: 'name', type: 'string', nullable: false },
          { name: 'createdAt', type: 'string', nullable: false },
          { name: 'createdBy', type: 'string', nullable: false },
          { name: 'updatedAt', type: 'string', nullable: false },
          { name: 'updatedBy', type: 'string', nullable: false },
          { name: 'deletedAt', type: 'string', nullable: true },
          { name: 'deletedBy', type: 'string', nullable: true },
          { name: 'version', type: 'integer', nullable: false },
          { name: '$data', type: 'string', nullable: true },
        ],
      }),
      partitionColumns: [],
      createdTime: Date.now(),
    },
  }
}

/**
 * Create an add action for a data file
 */
export function createAddAction(
  path: string,
  size: number,
  dataChange: boolean = true
): AddAction {
  return {
    add: {
      path,
      size,
      modificationTime: Date.now(),
      dataChange,
    },
  }
}

/**
 * Create a remove action for a data file
 */
export function createRemoveAction(
  path: string,
  dataChange: boolean = true
): RemoveAction {
  return {
    remove: {
      path,
      deletionTimestamp: Date.now(),
      dataChange,
    },
  }
}

/**
 * Create a commit info action
 */
export function createCommitInfoAction(
  operation: string,
  readVersion?: number,
  isBlindAppend: boolean = true,
  operationParameters?: Record<string, unknown>
): CommitInfoAction {
  return {
    commitInfo: {
      timestamp: Date.now(),
      operation,
      operationParameters,
      readVersion,
      isBlindAppend,
    },
  }
}

/**
 * Generate a UUID
 */
export function generateUUID(): string {
  const bytes = new Uint8Array(16)
  if (typeof crypto !== 'undefined' && crypto.getRandomValues) {
    crypto.getRandomValues(bytes)
  } else {
    for (let i = 0; i < 16; i++) {
      bytes[i] = Math.floor(Math.random() * 256)
    }
  }

  // Set version 4 and variant
  bytes[6] = (bytes[6]! & 0x0f) | 0x40
  bytes[8] = (bytes[8]! & 0x3f) | 0x80

  const hex = Array.from(bytes).map(b => b.toString(16).padStart(2, '0')).join('')
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`
}
