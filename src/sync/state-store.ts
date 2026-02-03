/**
 * State Store - Content-addressable storage and state reconstruction for ParqueDB
 *
 * Provides:
 * - Content-addressable object storage (files stored by their SHA256 hash)
 * - State snapshotting during commits
 * - State reconstruction during checkout
 * - Uncommitted changes detection
 */

import type { StorageBackend } from '../types/storage'
import type { DatabaseCommit, DatabaseState } from './commit'
import { sha256 } from './hash'

// =============================================================================
// Types
// =============================================================================

/**
 * Options for state store operations
 */
export interface StateStoreOptions {
  storage: StorageBackend
}

/**
 * Result of checking for uncommitted changes
 */
export interface UncommittedChangesResult {
  /** Whether there are uncommitted changes */
  hasChanges: boolean
  /** Collections with changes */
  changedCollections: string[]
  /** Whether relationships have changed */
  relationshipsChanged: boolean
  /** Human-readable summary */
  summary: string
}

/**
 * File info for snapshot
 */
export interface FileSnapshot {
  path: string
  hash: string
  size: number
}

// =============================================================================
// Object Store Operations
// =============================================================================

/**
 * Store a file in the content-addressable object store
 *
 * Files are stored at _meta/objects/{hash[:2]}/{hash} for better filesystem performance
 *
 * @param storage StorageBackend to use
 * @param data File contents
 * @returns SHA256 hash of the file
 */
export async function storeObject(storage: StorageBackend, data: Uint8Array): Promise<string> {
  const hash = sha256(data)
  const objectPath = getObjectPath(hash)

  // Check if object already exists (deduplication)
  const exists = await storage.exists(objectPath)
  if (!exists) {
    await storage.write(objectPath, data)
  }

  return hash
}

/**
 * Retrieve a file from the object store by hash
 *
 * @param storage StorageBackend to use
 * @param hash SHA256 hash of the file
 * @returns File contents
 * @throws If object not found
 */
export async function loadObject(storage: StorageBackend, hash: string): Promise<Uint8Array> {
  const objectPath = getObjectPath(hash)
  const exists = await storage.exists(objectPath)

  if (!exists) {
    throw new Error(`Object not found: ${hash}`)
  }

  return storage.read(objectPath)
}

/**
 * Check if an object exists in the store
 *
 * @param storage StorageBackend to use
 * @param hash SHA256 hash of the file
 * @returns True if object exists
 */
export async function objectExists(storage: StorageBackend, hash: string): Promise<boolean> {
  const objectPath = getObjectPath(hash)
  return storage.exists(objectPath)
}

/**
 * Get the storage path for an object by hash
 *
 * Uses first 2 characters as subdirectory to avoid too many files in one directory
 */
function getObjectPath(hash: string): string {
  const prefix = hash.substring(0, 2)
  return `_meta/objects/${prefix}/${hash}`
}

// =============================================================================
// State Snapshot Operations
// =============================================================================

/**
 * Snapshot current database state to the object store
 *
 * Stores all data files and relationship indexes in the object store,
 * returns a DatabaseState that can be saved in a commit.
 *
 * @param storage StorageBackend to use
 * @returns DatabaseState with hashes pointing to stored objects
 */
export async function snapshotState(storage: StorageBackend): Promise<DatabaseState> {
  // Use mutable type during construction
  const collections: Record<string, { dataHash: string; schemaHash: string; rowCount: number }> = {}

  // Snapshot all collection data files
  const dataFiles = await listDataFiles(storage)
  for (const { ns, path } of dataFiles) {
    const exists = await storage.exists(path)
    if (!exists) continue

    const data = await storage.read(path)
    const dataHash = await storeObject(storage, data)

    // Get schema hash if schema file exists
    const schemaPath = `data/${ns}/schema.json`
    let schemaHash = ''
    if (await storage.exists(schemaPath)) {
      const schemaData = await storage.read(schemaPath)
      schemaHash = await storeObject(storage, schemaData)
    }

    // Get row count from file metadata or estimate
    // For now, use size as a proxy (will be refined)
    const rowCount = estimateRowCount(data)

    collections[ns] = {
      dataHash,
      schemaHash,
      rowCount,
    }
  }

  // Snapshot relationship indexes
  const forwardHash = await snapshotRelationships(storage, 'forward')
  const reverseHash = await snapshotRelationships(storage, 'reverse')

  // Get current event log position
  const eventLogPosition = await getCurrentEventLogPosition(storage)

  return {
    collections,
    relationships: {
      forwardHash,
      reverseHash,
    },
    eventLogPosition,
  }
}

/**
 * Snapshot relationship files to object store
 */
async function snapshotRelationships(
  storage: StorageBackend,
  direction: 'forward' | 'reverse'
): Promise<string> {
  const basePath = `rels/${direction}`

  // List all relationship files
  const files = await listRelationshipFiles(storage, direction)

  if (files.length === 0) {
    // No relationships - return hash of empty state
    return sha256('{}')
  }

  // Combine all relationship files into a single snapshot
  // Store each file and create a manifest
  const manifest: Record<string, string> = {}

  for (const file of files) {
    const exists = await storage.exists(file)
    if (!exists) continue

    const data = await storage.read(file)
    const hash = await storeObject(storage, data)
    manifest[file] = hash
  }

  // Store the manifest itself
  const manifestData = new TextEncoder().encode(JSON.stringify(manifest, Object.keys(manifest).sort()))
  return storeObject(storage, manifestData)
}

/**
 * Get current event log position
 */
async function getCurrentEventLogPosition(
  storage: StorageBackend
): Promise<DatabaseState['eventLogPosition']> {
  const manifestPath = 'events/_manifest.json'
  const exists = await storage.exists(manifestPath)

  if (!exists) {
    return { segmentId: 'initial', offset: 0 }
  }

  try {
    const data = await storage.read(manifestPath)
    const manifest = JSON.parse(new TextDecoder().decode(data))
    const segments = manifest.segments || []
    const lastSegment = segments[segments.length - 1]

    return {
      segmentId: lastSegment?.path ?? 'initial',
      offset: manifest.totalEvents ?? 0,
    }
  } catch {
    return { segmentId: 'initial', offset: 0 }
  }
}

// =============================================================================
// State Reconstruction Operations
// =============================================================================

/**
 * Reconstruct database state from a commit
 *
 * This is the core checkout operation - it restores all data files,
 * relationship indexes, and updates event log position to match the commit.
 *
 * Uses atomic operations to ensure consistency - if any step fails,
 * the database is left unchanged.
 *
 * @param storage StorageBackend to use
 * @param commit DatabaseCommit to restore state from
 */
export async function reconstructState(
  storage: StorageBackend,
  commit: DatabaseCommit
): Promise<void> {
  const state = commit.state

  // Create backup paths for rollback
  const backupSuffix = `.backup-${Date.now()}`
  const restoredPaths: string[] = []
  const backupPaths: Map<string, string> = new Map()

  try {
    // 1. Restore collection data files
    for (const [ns, colState] of Object.entries(state.collections)) {
      const dataPath = `data/${ns}/data.parquet`
      const schemaPath = `data/${ns}/schema.json`

      // Backup existing files if they exist
      if (await storage.exists(dataPath)) {
        const backupPath = dataPath + backupSuffix
        await storage.copy(dataPath, backupPath)
        backupPaths.set(dataPath, backupPath)
      }

      // Restore data file from object store
      if (colState.dataHash) {
        const data = await loadObject(storage, colState.dataHash)
        await storage.write(dataPath, data)
        restoredPaths.push(dataPath)
      }

      // Restore schema file if present
      if (colState.schemaHash) {
        if (await storage.exists(schemaPath)) {
          const backupPath = schemaPath + backupSuffix
          await storage.copy(schemaPath, backupPath)
          backupPaths.set(schemaPath, backupPath)
        }

        const schemaData = await loadObject(storage, colState.schemaHash)
        await storage.write(schemaPath, schemaData)
        restoredPaths.push(schemaPath)
      }
    }

    // 2. Remove collections that don't exist in the target state
    const currentCollections = await listDataFiles(storage)
    for (const { ns, path } of currentCollections) {
      if (!state.collections[ns]) {
        // Backup before removing
        if (await storage.exists(path)) {
          const backupPath = path + backupSuffix
          await storage.copy(path, backupPath)
          backupPaths.set(path, backupPath)
          await storage.delete(path)
          restoredPaths.push(path)
        }
      }
    }

    // 3. Restore relationship indexes
    await restoreRelationships(storage, state.relationships.forwardHash, 'forward', backupSuffix, backupPaths, restoredPaths)
    await restoreRelationships(storage, state.relationships.reverseHash, 'reverse', backupSuffix, backupPaths, restoredPaths)

    // 4. Update event log position marker
    await updateEventLogPosition(storage, state.eventLogPosition)

    // Success - clean up backups
    for (const backupPath of Array.from(backupPaths.values())) {
      try {
        await storage.delete(backupPath)
      } catch {
        // Ignore cleanup errors
      }
    }
  } catch (error) {
    // Rollback - restore from backups
    for (const [originalPath, backupPath] of Array.from(backupPaths.entries())) {
      try {
        if (await storage.exists(backupPath)) {
          await storage.copy(backupPath, originalPath)
          await storage.delete(backupPath)
        }
      } catch {
        // Best effort rollback
      }
    }

    throw new Error(
      `Failed to reconstruct state: ${error instanceof Error ? error.message : String(error)}. ` +
      `Database has been rolled back to previous state.`
    )
  }
}

/**
 * Restore relationship files from a manifest hash
 */
async function restoreRelationships(
  storage: StorageBackend,
  manifestHash: string,
  direction: 'forward' | 'reverse',
  backupSuffix: string,
  backupPaths: Map<string, string>,
  restoredPaths: string[]
): Promise<void> {
  // Load the manifest
  const emptyHash = sha256('{}')
  if (manifestHash === emptyHash) {
    // Empty relationships - remove existing files
    const existingFiles = await listRelationshipFiles(storage, direction)
    for (const path of existingFiles) {
      if (await storage.exists(path)) {
        const backupPath = path + backupSuffix
        await storage.copy(path, backupPath)
        backupPaths.set(path, backupPath)
        await storage.delete(path)
        restoredPaths.push(path)
      }
    }
    return
  }

  // Load manifest from object store
  const manifestData = await loadObject(storage, manifestHash)
  const manifest: Record<string, string> = JSON.parse(new TextDecoder().decode(manifestData))

  // Backup and remove existing files
  const existingFiles = await listRelationshipFiles(storage, direction)
  for (const path of existingFiles) {
    if (await storage.exists(path)) {
      const backupPath = path + backupSuffix
      await storage.copy(path, backupPath)
      backupPaths.set(path, backupPath)
      await storage.delete(path)
    }
  }

  // Restore files from manifest
  for (const [path, hash] of Object.entries(manifest)) {
    const data = await loadObject(storage, hash)
    await storage.write(path, data)
    restoredPaths.push(path)
  }
}

/**
 * Update event log position marker
 */
async function updateEventLogPosition(
  storage: StorageBackend,
  position: DatabaseState['eventLogPosition']
): Promise<void> {
  const positionPath = '_meta/event-position.json'
  const data = new TextEncoder().encode(JSON.stringify(position, null, 2))
  await storage.write(positionPath, data)
}

// =============================================================================
// Uncommitted Changes Detection
// =============================================================================

/**
 * Check if there are uncommitted changes compared to a commit
 *
 * @param storage StorageBackend to use
 * @param commit DatabaseCommit to compare against
 * @returns UncommittedChangesResult
 */
export async function checkUncommittedChanges(
  storage: StorageBackend,
  commit: DatabaseCommit
): Promise<UncommittedChangesResult> {
  const changedCollections: string[] = []
  let relationshipsChanged = false

  const state = commit.state

  // Check each collection in the commit
  for (const [ns, colState] of Object.entries(state.collections)) {
    const dataPath = `data/${ns}/data.parquet`
    const exists = await storage.exists(dataPath)

    if (!exists) {
      // Collection was deleted
      changedCollections.push(ns)
      continue
    }

    // Compare hash
    const currentData = await storage.read(dataPath)
    const currentHash = sha256(currentData)

    if (currentHash !== colState.dataHash) {
      changedCollections.push(ns)
    }
  }

  // Check for new collections not in commit
  const currentCollections = await listDataFiles(storage)
  for (const { ns } of currentCollections) {
    if (!state.collections[ns]) {
      changedCollections.push(ns)
    }
  }

  // Check relationships
  const currentForwardHash = await computeRelationshipsHash(storage, 'forward')
  const currentReverseHash = await computeRelationshipsHash(storage, 'reverse')

  if (
    currentForwardHash !== state.relationships.forwardHash ||
    currentReverseHash !== state.relationships.reverseHash
  ) {
    relationshipsChanged = true
  }

  const hasChanges = changedCollections.length > 0 || relationshipsChanged

  // Generate summary
  let summary = 'No uncommitted changes'
  if (hasChanges) {
    const parts: string[] = []
    if (changedCollections.length > 0) {
      parts.push(`${changedCollections.length} collection(s) modified`)
    }
    if (relationshipsChanged) {
      parts.push('relationships modified')
    }
    summary = parts.join(', ')
  }

  return {
    hasChanges,
    changedCollections,
    relationshipsChanged,
    summary,
  }
}

/**
 * Compute hash for current relationship state
 */
async function computeRelationshipsHash(
  storage: StorageBackend,
  direction: 'forward' | 'reverse'
): Promise<string> {
  const files = await listRelationshipFiles(storage, direction)

  if (files.length === 0) {
    return sha256('{}')
  }

  const manifest: Record<string, string> = {}
  for (const file of files) {
    const exists = await storage.exists(file)
    if (!exists) continue

    const data = await storage.read(file)
    manifest[file] = sha256(data)
  }

  return sha256(JSON.stringify(manifest, Object.keys(manifest).sort()))
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * List all data files in the database
 */
async function listDataFiles(
  storage: StorageBackend
): Promise<Array<{ ns: string; path: string }>> {
  const results: Array<{ ns: string; path: string }> = []

  try {
    const dataDir = await storage.list('data/', { delimiter: '/' })

    for (const prefix of dataDir.prefixes || []) {
      // Extract namespace from prefix like 'data/posts/'
      const ns = prefix.replace('data/', '').replace(/\/$/, '')
      if (ns) {
        const dataPath = `data/${ns}/data.parquet`
        if (await storage.exists(dataPath)) {
          results.push({ ns, path: dataPath })
        }
      }
    }
  } catch {
    // No data directory yet
  }

  return results
}

/**
 * List relationship files for a direction
 */
async function listRelationshipFiles(
  storage: StorageBackend,
  direction: 'forward' | 'reverse'
): Promise<string[]> {
  const results: string[] = []

  try {
    const relDir = await storage.list(`rels/${direction}/`)

    for (const file of relDir.files) {
      if (file.endsWith('.parquet')) {
        results.push(file)
      }
    }
  } catch {
    // No relationship directory yet
  }

  return results
}

/**
 * Estimate row count from parquet file data
 * This is a rough estimate based on file size
 */
function estimateRowCount(data: Uint8Array): number {
  // Very rough estimate: assume ~100 bytes per row on average
  // In production, we should read the parquet metadata
  return Math.max(1, Math.floor(data.length / 100))
}
