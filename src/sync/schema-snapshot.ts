/**
 * Schema Snapshot - Capture and track schema evolution over time
 *
 * Enables strongly-typed time travel queries by maintaining schema state
 * at each commit point.
 */

import type { ParqueDBConfig } from '../config/loader'
import type { CollectionSchemaWithLayout } from '../db'
import type { StorageBackend } from '../types/storage'
import { sha256, hashObject } from './hash'

// Re-export types from shared types file for backward compatibility
export type {
  SchemaFieldRelationship,
  SchemaFieldSnapshot,
  CollectionSchemaOptions,
  CollectionSchemaSnapshot,
  SchemaSnapshot,
} from './types'

// Import types for use in this module
import type {
  SchemaFieldRelationship,
  SchemaFieldSnapshot,
  CollectionSchemaSnapshot,
  SchemaSnapshot,
  DatabaseCommit,
} from './types'

/**
 * Capture current schema from config
 *
 * @param config ParqueDB configuration with schema
 * @returns SchemaSnapshot of current state
 */
export async function captureSchema(config: ParqueDBConfig): Promise<SchemaSnapshot> {
  if (!config.schema) {
    // No schema - return empty snapshot
    return {
      hash: sha256('{}'),
      configHash: sha256('{}'),
      collections: {},
      capturedAt: Date.now()
    }
  }

  const collections: Record<string, CollectionSchemaSnapshot> = {}
  let collectionVersion = 1 // Simple incrementing version

  for (const [name, collectionSchema] of Object.entries(config.schema)) {
    if (collectionSchema === 'flexible') {
      // Skip flexible collections - they don't have a defined schema
      continue
    }

    const fields = extractFields(collectionSchema as CollectionSchemaWithLayout)
    const collectionHash = hashObject({ name, fields })

    collections[name] = {
      name,
      hash: collectionHash,
      fields,
      version: collectionVersion++,
      options: (collectionSchema as CollectionSchemaWithLayout).$options
    }
  }

  const schemaHash = hashObject({ collections })
  const configHash = hashObject(config)

  return {
    hash: schemaHash,
    configHash,
    collections,
    capturedAt: Date.now()
  }
}

/**
 * Extract field definitions from collection schema
 */
function extractFields(schema: CollectionSchemaWithLayout): SchemaFieldSnapshot[] {
  const fields: SchemaFieldSnapshot[] = []

  for (const [fieldName, fieldDef] of Object.entries(schema)) {
    // Skip $-prefixed config keys
    if (fieldName.startsWith('$')) continue
    if (typeof fieldDef !== 'string') continue

    const field = parseFieldDefinition(fieldName, fieldDef)
    fields.push(field)
  }

  return fields
}

/**
 * Parse a field definition string into structured metadata
 *
 * Examples:
 * - 'string!' → required string
 * - 'int?' → optional integer
 * - 'string!#' → required + indexed string
 * - 'string!@' → required + unique string
 * - '-> User' → relationship to User
 * - '<- Post.author' → reverse relationship
 * - 'string[]' → array of strings
 */
function parseFieldDefinition(name: string, def: string): SchemaFieldSnapshot {
  // Parse relationship if present
  let relationship: SchemaFieldRelationship | undefined
  if (def.includes('->')) {
    const match = def.match(/\->\s*([^\s.[\]]+)(?:\.([^\s[\]]+))?/)
    if (match) {
      relationship = {
        target: match[1] ?? 'unknown',
        reverse: match[2],
        direction: 'outbound'
      }
    }
  } else if (def.includes('<-')) {
    const match = def.match(/\<-\s*([^\s.[\]]+)(?:\.([^\s[\]]+))?/)
    if (match) {
      relationship = {
        target: match[1] ?? 'unknown',
        reverse: match[2],
        direction: 'inbound'
      }
    }
  }

  return {
    name,
    type: def,
    required: def.includes('!'),
    indexed: def.includes('#'),
    unique: def.includes('@'),
    array: def.includes('[]'),
    relationship
  }
}

/**
 * Extract base type from a type definition string
 * Removes modifiers like !, ?, #, @, []
 *
 * Examples:
 * - 'string!' → 'string'
 * - 'string!#' → 'string'
 * - 'int?' → 'int'
 * - 'string[]' → 'string[]' (array is part of the type)
 */
function extractBaseType(typeDef: string): string {
  // Remove required (!), optional (?), indexed (#), unique (@) modifiers
  // Keep array notation ([]) as it's part of the structural type
  return typeDef.replace(/[!?#@]/g, '')
}

/**
 * Load schema snapshot from a commit
 *
 * @param storage StorageBackend to read from
 * @param commitHash Commit hash to load schema from
 * @returns SchemaSnapshot at that commit
 * @throws If commit not found or no schema data
 */
export async function loadSchemaAtCommit(
  storage: StorageBackend,
  commitHash: string
): Promise<SchemaSnapshot> {
  // Load the commit directly to avoid circular dependency with commit.ts
  const commitPath = `_meta/commits/${commitHash}.json`
  const commitExists = await storage.exists(commitPath)

  if (commitExists) {
    const commitData = await storage.read(commitPath)
    let commit: DatabaseCommit
    try {
      commit = JSON.parse(new TextDecoder().decode(commitData))
    } catch {
      throw new Error(`Invalid commit JSON: ${commitHash}`)
    }

    // Check if commit has schema in state
    // The state may have an optional schema field in newer commits
    interface CommitStateWithSchema {
      schema?: SchemaSnapshot | undefined
    }
    const state = commit.state as typeof commit.state & CommitStateWithSchema
    if (state.schema) {
      return state.schema
    }
  }

  // Legacy commit without schema or commit not found - try to load from separate snapshot file
  const snapshotPath = `_meta/schemas/${commitHash}.json`
  const exists = await storage.exists(snapshotPath)

  if (!exists) {
    throw new Error(`Schema snapshot not found for commit: ${commitHash}`)
  }

  const data = await storage.read(snapshotPath)
  let snapshot
  try {
    snapshot = JSON.parse(new TextDecoder().decode(data))
  } catch {
    throw new Error(`Invalid schema snapshot JSON for commit: ${commitHash}`)
  }

  return snapshot as SchemaSnapshot
}

/**
 * Save schema snapshot to storage
 *
 * Stores in both the commit object and as a separate file for backward compatibility
 *
 * @param storage StorageBackend to write to
 * @param snapshot SchemaSnapshot to save
 */
export async function saveSchemaSnapshot(
  storage: StorageBackend,
  snapshot: SchemaSnapshot
): Promise<void> {
  if (!snapshot.commitHash) {
    throw new Error('Cannot save schema snapshot without commitHash')
  }

  const path = `_meta/schemas/${snapshot.commitHash}.json`
  const json = JSON.stringify(snapshot, null, 2)
  await storage.write(path, new TextEncoder().encode(json))
}

/**
 * Compare two schema snapshots
 *
 * @param before Earlier schema state
 * @param after Later schema state
 * @returns SchemaChanges describing differences
 */
export function diffSchemas(
  before: SchemaSnapshot,
  after: SchemaSnapshot
): SchemaChanges {
  const changes: SchemaChange[] = []

  const beforeCollections = new Set(Object.keys(before.collections))
  const afterCollections = new Set(Object.keys(after.collections))

  // Find added collections
  for (const name of afterCollections) {
    if (!beforeCollections.has(name)) {
      changes.push({
        type: 'ADD_COLLECTION',
        collection: name,
        after: after.collections[name],
        breaking: false,
        description: `Added collection: ${name}`
      })
    }
  }

  // Find removed collections
  for (const name of beforeCollections) {
    if (!afterCollections.has(name)) {
      changes.push({
        type: 'DROP_COLLECTION',
        collection: name,
        before: before.collections[name],
        breaking: true,
        description: `Dropped collection: ${name}`
      })
    }
  }

  // Find modified collections
  for (const name of afterCollections) {
    if (beforeCollections.has(name)) {
      const beforeColl = before.collections[name]!
      const afterColl = after.collections[name]!

      if (beforeColl.hash !== afterColl.hash) {
        // Collection changed - compare fields
        const fieldChanges = diffCollectionFields(name, beforeColl, afterColl)
        changes.push(...fieldChanges)
      }
    }
  }

  // Determine if changes are compatible
  const breakingChanges = changes.filter(c => c.breaking)
  const compatible = breakingChanges.length === 0

  // Generate summary
  const summary = generateChangeSummary(changes)

  return {
    changes,
    breakingChanges,
    compatible,
    summary
  }
}

/**
 * Compare fields between two collection versions
 */
function diffCollectionFields(
  collectionName: string,
  before: CollectionSchemaSnapshot,
  after: CollectionSchemaSnapshot
): SchemaChange[] {
  const changes: SchemaChange[] = []

  const beforeFields = new Map(before.fields.map(f => [f.name, f]))
  const afterFields = new Map(after.fields.map(f => [f.name, f]))

  // Find added fields
  for (const [name, field] of afterFields) {
    if (!beforeFields.has(name)) {
      changes.push({
        type: 'ADD_FIELD',
        collection: collectionName,
        field: name,
        after: field,
        breaking: field.required, // Adding required field is breaking
        description: `Added field: ${collectionName}.${name}${field.required ? ' (required - BREAKING)' : ''}`
      })
    }
  }

  // Find removed fields
  for (const [name, field] of beforeFields) {
    if (!afterFields.has(name)) {
      changes.push({
        type: 'REMOVE_FIELD',
        collection: collectionName,
        field: name,
        before: field,
        breaking: true,
        description: `Removed field: ${collectionName}.${name}`
      })
    }
  }

  // Find modified fields
  for (const [name, afterField] of afterFields) {
    const beforeField = beforeFields.get(name)
    if (!beforeField) continue

    // Type change - compare base types without modifiers (!, ?, #, @, [])
    const beforeBaseType = extractBaseType(beforeField.type)
    const afterBaseType = extractBaseType(afterField.type)
    if (beforeBaseType !== afterBaseType) {
      changes.push({
        type: 'CHANGE_TYPE',
        collection: collectionName,
        field: name,
        before: beforeBaseType,
        after: afterBaseType,
        breaking: true,
        description: `Changed type: ${collectionName}.${name} from ${beforeBaseType} to ${afterBaseType}`
      })
    }

    // Required change
    if (beforeField.required !== afterField.required) {
      changes.push({
        type: 'CHANGE_REQUIRED',
        collection: collectionName,
        field: name,
        before: beforeField.required,
        after: afterField.required,
        breaking: afterField.required, // Making required is breaking
        description: `Changed required: ${collectionName}.${name} ${afterField.required ? 'now required' : 'now optional'}`
      })
    }

    // Index changes
    if (beforeField.indexed !== afterField.indexed) {
      const type = afterField.indexed ? 'ADD_INDEX' : 'REMOVE_INDEX'
      changes.push({
        type,
        collection: collectionName,
        field: name,
        before: beforeField.indexed,
        after: afterField.indexed,
        breaking: false,
        description: `${afterField.indexed ? 'Added' : 'Removed'} index: ${collectionName}.${name}`
      })
    }
  }

  return changes
}

/**
 * Generate human-readable summary of changes
 */
function generateChangeSummary(changes: SchemaChange[]): string {
  if (changes.length === 0) {
    return 'No schema changes'
  }

  const lines: string[] = []
  const breakingCount = changes.filter(c => c.breaking).length

  if (breakingCount > 0) {
    lines.push(`⚠️  ${breakingCount} breaking change${breakingCount > 1 ? 's' : ''}`)
  }

  const addedCollections = changes.filter(c => c.type === 'ADD_COLLECTION').length
  const removedCollections = changes.filter(c => c.type === 'DROP_COLLECTION').length
  const addedFields = changes.filter(c => c.type === 'ADD_FIELD').length
  const removedFields = changes.filter(c => c.type === 'REMOVE_FIELD').length
  const modifiedFields = changes.filter(c => c.type === 'CHANGE_TYPE' || c.type === 'CHANGE_REQUIRED').length

  if (addedCollections > 0) lines.push(`+ ${addedCollections} collection${addedCollections > 1 ? 's' : ''}`)
  if (removedCollections > 0) lines.push(`- ${removedCollections} collection${removedCollections > 1 ? 's' : ''}`)
  if (addedFields > 0) lines.push(`+ ${addedFields} field${addedFields > 1 ? 's' : ''}`)
  if (removedFields > 0) lines.push(`- ${removedFields} field${removedFields > 1 ? 's' : ''}`)
  if (modifiedFields > 0) lines.push(`~ ${modifiedFields} field${modifiedFields > 1 ? 's' : ''} modified`)

  return lines.join(', ')
}

/**
 * Schema change types
 */
export type SchemaChangeType =
  | 'ADD_COLLECTION'
  | 'DROP_COLLECTION'
  | 'ADD_FIELD'
  | 'REMOVE_FIELD'
  | 'MODIFY_FIELD'
  | 'CHANGE_TYPE'
  | 'CHANGE_REQUIRED'
  | 'ADD_INDEX'
  | 'REMOVE_INDEX'

/**
 * A single schema change
 */
export interface SchemaChange {
  readonly type: SchemaChangeType
  readonly collection: string
  readonly field?: string | undefined
  readonly before?: unknown | undefined
  readonly after?: unknown | undefined
  readonly breaking: boolean
  readonly description: string
}

/**
 * Set of schema changes with metadata
 */
export interface SchemaChanges {
  readonly changes: readonly SchemaChange[]
  readonly breakingChanges: readonly SchemaChange[]
  readonly compatible: boolean
  readonly summary: string
}
