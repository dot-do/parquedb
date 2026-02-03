/**
 * Transaction Log Utilities
 *
 * Common utilities for managing transaction logs in both ParqueDB and Delta Lake.
 * Provides NDJSON serialization, version formatting, and action validation.
 *
 * This module enables:
 * - ACID transactions through log-structured storage
 * - Time-travel by replaying commits
 * - Concurrent writers through optimistic concurrency control
 */

// =============================================================================
// TYPES
// =============================================================================

/**
 * Base action interface - all actions have one property identifying the type
 */
export interface BaseAction {
  [key: string]: unknown
}

/**
 * Add file action - records a new data file
 */
export interface AddAction {
  add: {
    path: string
    size: number
    modificationTime: number
    dataChange: boolean
    partitionValues?: Record<string, string> | undefined
    stats?: string | undefined // JSON encoded statistics
    tags?: Record<string, string> | undefined
  }
}

/**
 * Remove file action - marks a file for deletion
 */
export interface RemoveAction {
  remove: {
    path: string
    deletionTimestamp: number
    dataChange: boolean
    partitionValues?: Record<string, string> | undefined
    extendedFileMetadata?: boolean | undefined
    size?: number | undefined
  }
}

/**
 * Metadata action - table-level metadata
 */
export interface MetadataAction {
  metaData: {
    id: string
    name?: string | undefined
    description?: string | undefined
    format: { provider: string; options?: Record<string, string> | undefined }
    schemaString: string
    partitionColumns: string[]
    configuration?: Record<string, string> | undefined
    createdTime?: number | undefined
  }
}

/**
 * Protocol action - version requirements
 */
export interface ProtocolAction {
  protocol: {
    minReaderVersion: number
    minWriterVersion: number
  }
}

/**
 * Commit info action - metadata about the commit
 */
export interface CommitInfoAction {
  commitInfo: {
    timestamp: number
    operation: string
    operationParameters?: Record<string, string> | undefined
    readVersion?: number | undefined
    isolationLevel?: string | undefined
    isBlindAppend?: boolean | undefined
  }
}

/**
 * Union of all action types
 */
export type LogAction =
  | AddAction
  | RemoveAction
  | MetadataAction
  | ProtocolAction
  | CommitInfoAction

/**
 * A commit is a collection of actions at a specific version
 */
export interface Commit {
  version: number
  timestamp: number
  actions: LogAction[]
}

/**
 * A snapshot represents the state of a table at a specific version
 */
export interface Snapshot {
  version: number
  files: AddAction['add'][]
  metadata?: MetadataAction['metaData'] | undefined
  protocol?: ProtocolAction['protocol'] | undefined
}

/**
 * File statistics for predicate pushdown
 */
export interface FileStats {
  numRecords: number
  minValues: Record<string, unknown>
  maxValues: Record<string, unknown>
  nullCount: Record<string, number>
}

// =============================================================================
// SERIALIZATION
// =============================================================================

/**
 * Serialize a single action to JSON
 */
export function serializeAction(action: LogAction): string {
  return JSON.stringify(action)
}

/**
 * Parse a single action from JSON
 */
export function parseAction(json: string): LogAction {
  if (!json || json.trim() === '') {
    throw new Error('Cannot parse empty JSON string')
  }

  let parsed
  try {
    parsed = JSON.parse(json)
  } catch {
    throw new Error('Invalid action: not valid JSON')
  }

  // Validate that it's an object
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
    throw new Error('Action must be a JSON object')
  }

  // Validate that it has a recognized action type
  const hasValidAction =
    'add' in parsed ||
    'remove' in parsed ||
    'metaData' in parsed ||
    'protocol' in parsed ||
    'commitInfo' in parsed

  if (!hasValidAction) {
    throw new Error('JSON must contain a recognized action type')
  }

  return parsed as LogAction
}

/**
 * Serialize multiple actions to NDJSON format
 */
export function serializeCommit(actions: LogAction[]): string {
  return actions.map(action => serializeAction(action)).join('\n')
}

/**
 * Parse NDJSON content to actions
 */
export function parseCommit(content: string): LogAction[] {
  const lines = content.split(/\r?\n/).filter(line => line.trim() !== '')
  return lines.map(line => parseAction(line))
}

// =============================================================================
// VERSION FORMATTING
// =============================================================================

/**
 * Format version number as 20-digit zero-padded string
 */
export function formatVersion(version: number | bigint): string {
  const versionStr = version.toString()

  // Check for negative numbers
  if (versionStr.startsWith('-')) {
    throw new Error('Version number cannot be negative')
  }

  // Check if exceeds 20 digits
  if (versionStr.length > 20) {
    throw new Error('Version number exceeds 20 digits')
  }

  return versionStr.padStart(20, '0')
}

/**
 * Parse version number from filename
 */
export function parseVersionFromFilename(filename: string): number {
  // Extract just the filename if a full path is provided
  const basename = filename.split('/').pop() || filename

  // Match 20-digit number followed by .json
  const match = basename.match(/^(\d{20})\.json$/)

  if (!match || !match[1]) {
    throw new Error('Invalid log file name format')
  }

  return parseInt(match[1], 10)
}

/**
 * Get the log file path for a version
 */
export function getLogFilePath(tablePath: string, version: number): string {
  // Remove trailing slash if present
  const cleanPath = tablePath.endsWith('/') ? tablePath.slice(0, -1) : tablePath
  const baseDir = cleanPath ? `${cleanPath}/_delta_log` : '_delta_log'
  return `${baseDir}/${formatVersion(version)}.json`
}

/**
 * Get the checkpoint file path for a version
 */
export function getCheckpointPath(tablePath: string, version: number): string {
  const cleanPath = tablePath.endsWith('/') ? tablePath.slice(0, -1) : tablePath
  const baseDir = cleanPath ? `${cleanPath}/_delta_log` : '_delta_log'
  return `${baseDir}/${formatVersion(version)}.checkpoint.parquet`
}

// =============================================================================
// ACTION VALIDATION
// =============================================================================

/**
 * Validation result
 */
export interface ValidationResult {
  valid: boolean
  errors: string[]
}

/**
 * Validate an action structure
 */
export function validateAction(action: LogAction): ValidationResult {
  const errors: string[] = []

  if ('add' in action) {
    const add = action.add

    if (!add.path || add.path === '') {
      errors.push('add.path must not be empty')
    }

    if (add.size < 0) {
      errors.push('add.size must be non-negative')
    }

    if (add.modificationTime < 0) {
      errors.push('add.modificationTime must be non-negative')
    }

    if (add.stats !== undefined) {
      try {
        JSON.parse(add.stats)
      } catch {
        // Intentionally ignored: JSON.parse failure means stats is not valid JSON
        errors.push('add.stats must be valid JSON')
      }
    }
  } else if ('remove' in action) {
    const remove = action.remove

    if (!remove.path || remove.path === '') {
      errors.push('remove.path must not be empty')
    }

    if (remove.deletionTimestamp < 0) {
      errors.push('remove.deletionTimestamp must be non-negative')
    }
  } else if ('metaData' in action) {
    const metaData = action.metaData

    if (!metaData.id || metaData.id === '') {
      errors.push('metaData.id must not be empty')
    }

    if (!metaData.format.provider || metaData.format.provider === '') {
      errors.push('metaData.format.provider must not be empty')
    }

    if (metaData.schemaString !== undefined) {
      try {
        JSON.parse(metaData.schemaString)
      } catch {
        // Intentionally ignored: JSON.parse failure means schemaString is not valid JSON
        errors.push('metaData.schemaString must be valid JSON')
      }
    }
  } else if ('protocol' in action) {
    const protocol = action.protocol

    if (protocol.minReaderVersion < 1) {
      errors.push('protocol.minReaderVersion must be at least 1')
    }

    if (protocol.minWriterVersion < 1) {
      errors.push('protocol.minWriterVersion must be at least 1')
    }

    if (!Number.isInteger(protocol.minReaderVersion)) {
      errors.push('protocol.minReaderVersion must be an integer')
    }

    if (!Number.isInteger(protocol.minWriterVersion)) {
      errors.push('protocol.minWriterVersion must be an integer')
    }
  } else if ('commitInfo' in action) {
    const commitInfo = action.commitInfo

    if (commitInfo.timestamp < 0) {
      errors.push('commitInfo.timestamp must be non-negative')
    }

    if (!commitInfo.operation || commitInfo.operation === '') {
      errors.push('commitInfo.operation must not be empty')
    }

    if (commitInfo.readVersion !== undefined && commitInfo.readVersion < 0) {
      errors.push('commitInfo.readVersion must be non-negative')
    }
  }

  return {
    valid: errors.length === 0,
    errors,
  }
}

// =============================================================================
// TYPE GUARDS
// =============================================================================

/**
 * Type guard for AddAction
 */
export function isAddAction(action: LogAction): action is AddAction {
  if (!action || typeof action !== 'object') return false
  if (!('add' in action)) return false
  if (!action.add || typeof action.add !== 'object') return false
  return true
}

/**
 * Type guard for RemoveAction
 */
export function isRemoveAction(action: LogAction): action is RemoveAction {
  if (!action || typeof action !== 'object') return false
  if (!('remove' in action)) return false
  if (!action.remove || typeof action.remove !== 'object') return false
  return true
}

/**
 * Type guard for MetadataAction
 */
export function isMetadataAction(action: LogAction): action is MetadataAction {
  if (!action || typeof action !== 'object') return false
  if (!('metaData' in action)) return false
  return true
}

/**
 * Type guard for ProtocolAction
 */
export function isProtocolAction(action: LogAction): action is ProtocolAction {
  if (!action || typeof action !== 'object') return false
  if (!('protocol' in action)) return false
  return true
}

/**
 * Type guard for CommitInfoAction
 */
export function isCommitInfoAction(action: LogAction): action is CommitInfoAction {
  if (!action || typeof action !== 'object') return false
  if (!('commitInfo' in action)) return false
  return true
}

// =============================================================================
// STATS PARSING
// =============================================================================

/**
 * Parse stats JSON string from AddAction
 */
export function parseStats(statsJson: string): FileStats {
  let parsed
  try {
    parsed = JSON.parse(statsJson)
  } catch {
    throw new Error('Invalid stats: not valid JSON')
  }

  if (parsed.numRecords === undefined || parsed.numRecords === null) {
    throw new Error('numRecords is required')
  }

  return parsed as FileStats
}

/**
 * Encode stats object to JSON string for AddAction
 */
export function encodeStats(stats: FileStats): string {
  return JSON.stringify(stats)
}

// =============================================================================
// ACTION CREATION HELPERS
// =============================================================================

/**
 * Create an AddAction with validation
 */
export function createAddAction(params: {
  path: string
  size: number
  modificationTime: number
  dataChange: boolean
  partitionValues?: Record<string, string> | undefined
  stats?: FileStats | undefined
  tags?: Record<string, string> | undefined
}): AddAction {
  // Validate path
  if (params.path.startsWith('/')) {
    throw new Error('path must be relative')
  }
  if (params.path.includes('../')) {
    throw new Error('path cannot contain parent directory traversal')
  }
  if (params.path.startsWith('./')) {
    throw new Error('path should not start with ./')
  }

  // Validate size
  if (!Number.isInteger(params.size)) {
    throw new Error('size must be an integer')
  }
  if (params.size > Number.MAX_SAFE_INTEGER) {
    throw new Error('size exceeds maximum safe integer')
  }

  // Validate modificationTime
  if (!Number.isInteger(params.modificationTime)) {
    throw new Error('modificationTime must be an integer')
  }

  // Validate stats if provided
  if (params.stats) {
    if (params.stats.numRecords < 0) {
      throw new Error('numRecords must be non-negative')
    }
    for (const [, value] of Object.entries(params.stats.nullCount)) {
      if (value < 0) {
        throw new Error('nullCount values must be non-negative')
      }
      if (value > params.stats.numRecords) {
        throw new Error('nullCount cannot exceed numRecords')
      }
    }
  }

  const action: AddAction = {
    add: {
      path: params.path,
      size: params.size,
      modificationTime: params.modificationTime,
      dataChange: params.dataChange,
    },
  }

  if (params.partitionValues !== undefined) {
    action.add.partitionValues = params.partitionValues
  }

  if (params.stats !== undefined) {
    action.add.stats = encodeStats(params.stats)
  }

  if (params.tags !== undefined) {
    action.add.tags = params.tags
  }

  return action
}

/**
 * Create a RemoveAction with validation
 */
export function createRemoveAction(params: {
  path: string
  deletionTimestamp: number
  dataChange: boolean
  partitionValues?: Record<string, string> | undefined
  extendedFileMetadata?: boolean | undefined
  size?: number | undefined
}): RemoveAction {
  const action: RemoveAction = {
    remove: {
      path: params.path,
      deletionTimestamp: params.deletionTimestamp,
      dataChange: params.dataChange,
    },
  }

  if (params.partitionValues !== undefined) {
    action.remove.partitionValues = params.partitionValues
  }

  if (params.extendedFileMetadata !== undefined) {
    action.remove.extendedFileMetadata = params.extendedFileMetadata
  }

  if (params.size !== undefined) {
    action.remove.size = params.size
  }

  return action
}
