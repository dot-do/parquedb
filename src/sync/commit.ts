import type { StorageBackend } from '../types/storage'
import { hashObject } from './hash'

// Re-export types from shared types file for backward compatibility
export type {
  SchemaSnapshot,
  CollectionState,
  RelationshipState,
  EventLogPosition,
  CommitState,
  DatabaseCommit,
  CommitOptions,
  DatabaseState,
} from './types'

// Import types for use in this module
import type {
  SchemaSnapshot,
  CommitState,
  DatabaseCommit,
  CommitOptions,
  DatabaseState,
} from './types'

/**
 * Create a new commit from database state
 * @param state Current database state
 * @param opts Commit options (message, author, parents)
 * @returns DatabaseCommit with computed hash
 */
export async function createCommit(
  state: DatabaseState,
  opts: CommitOptions
): Promise<DatabaseCommit> {
  const commitWithoutHash = {
    parents: opts.parents || [],
    timestamp: Date.now(),
    author: opts.author || 'anonymous',
    message: opts.message,
    state
  }

  const hash = hashCommit(commitWithoutHash)

  return {
    hash,
    ...commitWithoutHash
  }
}

/**
 * Create a new commit from database state with schema snapshot
 *
 * This function embeds the schema snapshot directly in the commit state,
 * enabling strongly-typed time travel queries and schema evolution tracking.
 *
 * @param state Current database state (without schema)
 * @param schema Schema snapshot to embed
 * @param opts Commit options (message, author, parents)
 * @returns DatabaseCommit with embedded schema and computed hash
 */
export async function createCommitWithSchema(
  state: Omit<DatabaseState, 'schema'>,
  schema: SchemaSnapshot,
  opts: CommitOptions
): Promise<DatabaseCommit> {
  // Clone schema and add commitHash after we compute the hash
  // We need to compute the hash first, then set commitHash
  const commitWithoutHash = {
    parents: opts.parents || [],
    timestamp: Date.now(),
    author: opts.author || 'anonymous',
    message: opts.message,
    state: {
      ...state,
      schema: {
        ...schema,
        commitHash: '' // Placeholder - will be set after hash computation
      }
    }
  }

  // Compute hash with placeholder commitHash
  const hash = hashCommit(commitWithoutHash)

  // Now set the actual commitHash
  const stateWithSchema: CommitState = {
    ...state,
    schema: {
      ...schema,
      commitHash: hash
    }
  }

  return {
    hash,
    parents: commitWithoutHash.parents,
    timestamp: commitWithoutHash.timestamp,
    author: commitWithoutHash.author,
    message: commitWithoutHash.message,
    state: stateWithSchema
  }
}

/**
 * Load a commit from storage by hash
 * @param storage StorageBackend to read from
 * @param hash Commit hash
 * @returns DatabaseCommit
 * @throws If commit not found or invalid
 */
export async function loadCommit(
  storage: StorageBackend,
  hash: string
): Promise<DatabaseCommit> {
  const path = `_meta/commits/${hash}.json`
  const exists = await storage.exists(path)

  if (!exists) {
    throw new Error(`Commit not found: ${hash}`)
  }

  const data = await storage.read(path)
  const commit = parseCommit(new TextDecoder().decode(data))

  // Verify hash matches
  const computedHash = hashCommit({
    parents: commit.parents,
    timestamp: commit.timestamp,
    author: commit.author,
    message: commit.message,
    state: commit.state
  })

  if (computedHash !== commit.hash) {
    throw new Error(`Commit hash mismatch: expected ${commit.hash}, got ${computedHash}`)
  }

  return commit
}

/**
 * Save a commit to storage
 * @param storage StorageBackend to write to
 * @param commit DatabaseCommit to save
 */
export async function saveCommit(
  storage: StorageBackend,
  commit: DatabaseCommit
): Promise<void> {
  const path = `_meta/commits/${commit.hash}.json`
  const json = serializeCommit(commit)
  await storage.write(path, new TextEncoder().encode(json))
}

/**
 * Compute hash of a commit (excluding the hash field itself)
 * @param commit Commit data without hash
 * @returns SHA256 hash
 */
export function hashCommit(commit: Omit<DatabaseCommit, 'hash'>): string {
  // Sort keys for deterministic ordering
  const normalized = {
    author: commit.author,
    message: commit.message,
    parents: commit.parents,
    state: commit.state,
    timestamp: commit.timestamp
  }

  return hashObject(normalized)
}

/**
 * Serialize commit to JSON string
 * @param commit DatabaseCommit
 * @returns JSON string with sorted keys
 */
export function serializeCommit(commit: DatabaseCommit): string {
  return JSON.stringify(commit, null, 2)
}

/**
 * Parse commit from JSON string
 * @param json JSON string
 * @returns DatabaseCommit
 * @throws If JSON is invalid
 */
export function parseCommit(json: string): DatabaseCommit {
  let commit
  try {
    commit = JSON.parse(json)
  } catch {
    throw new Error('Invalid commit: not valid JSON')
  }

  // Validate required fields
  if (!commit.hash || typeof commit.hash !== 'string') {
    throw new Error('Invalid commit: missing or invalid hash')
  }

  if (!Array.isArray(commit.parents)) {
    throw new Error('Invalid commit: parents must be an array')
  }

  if (typeof commit.timestamp !== 'number') {
    throw new Error('Invalid commit: timestamp must be a number')
  }

  if (typeof commit.author !== 'string') {
    throw new Error('Invalid commit: author must be a string')
  }

  if (typeof commit.message !== 'string') {
    throw new Error('Invalid commit: message must be a string')
  }

  if (!commit.state || typeof commit.state !== 'object') {
    throw new Error('Invalid commit: state must be an object')
  }

  return commit
}
